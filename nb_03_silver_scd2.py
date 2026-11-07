# Fabric Notebook: notebooks/nb_03_silver_scd2.py
# Flatten + dedupe bronze -> silver.<resource> with SCD Type 2 via Delta MERGE.

# %% PARAMETERS
resource_name = "Patient"
ingest_date   = "2026-11-03"     # process this bronze partition

# %% Imports
from pyspark.sql import functions as F, Window
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

src_table = f"bronze.{resource_name.lower()}"
tgt_table = f"silver.{resource_name.lower()}"

# %% Column mappings per resource (name -> Spark SQL expression on the `resource` struct)
REF_ID = lambda path: f"regexp_extract({path}.reference, '([^/]+)$', 1)"   # "Patient/123" -> "123"

MAPPINGS = {
    "Patient": {
        "gender":      "resource.gender",
        "birth_date":  "resource.birthDate",
        "family_name": "resource.name[0].family",
        "given_name":  "resource.name[0].given[0]",
        "city":        "resource.address[0].city",
        "state":       "resource.address[0].state",
        "country":     "resource.address[0].country",
    },
    "Encounter": {
        "status":      "resource.status",
        "class_code":  "resource.class.code",
        "patient_id":  REF_ID("resource.subject"),
        "period_start": "resource.period.start",
        "period_end":   "resource.period.end",
    },
    "Observation": {
        "status":      "resource.status",
        "code":        "resource.code.coding[0].code",
        "code_display": "resource.code.coding[0].display",
        "value_number": "resource.valueQuantity.value",
        "value_unit":   "resource.valueQuantity.unit",
        "patient_id":   REF_ID("resource.subject"),
        "encounter_id": REF_ID("resource.encounter"),
        "effective_datetime": "resource.effectiveDateTime",
    },
    "Condition": {
        "clinical_status": "resource.clinicalStatus.coding[0].code",
        "code":         "resource.code.coding[0].code",
        "code_display": "resource.code.coding[0].display",
        "patient_id":   REF_ID("resource.subject"),
        "encounter_id": REF_ID("resource.encounter"),
        "onset_datetime": "resource.onsetDateTime",
        "recorded_date":  "resource.recordedDate",
    },
}
mapping = MAPPINGS[resource_name]

# %% Flatten (a missing field in a given day's data becomes NULL instead of failing)
bronze = spark.table(src_table).filter(f"ingest_date = '{ingest_date}'")

def safe_expr(df, expr):
    try:
        df.select(F.expr(expr)).schema          # triggers analysis only
        return F.expr(expr)
    except AnalysisException:
        return F.lit(None).cast("string")

flat = bronze.select(
    F.col("resource_id").alias("id"),
    *[safe_expr(bronze, e).alias(n) for n, e in mapping.items()],
    F.col("source_last_updated").cast("timestamp").alias("source_last_updated"),
    "extraction_timestamp", "api_url_or_params", "ingest_date",
)

# %% Clean + dedupe: latest version per id inside this batch
flat = flat.filter("id IS NOT NULL")
w = Window.partitionBy("id").orderBy(F.col("source_last_updated").desc_nulls_last(),
                                     F.col("extraction_timestamp").desc())
dedup = flat.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

# %% Hash of business columns -> detects change
biz_cols = list(mapping.keys())
dedup = dedup.withColumn(
    "hash_diff",
    F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in biz_cols]), 256))

load_ts = F.current_timestamp()
new_rows = (dedup
    .withColumn("effective_from", load_ts)
    .withColumn("effective_to", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True)))

# %% First load: create the table
if not spark.catalog.tableExists(tgt_table):
    new_rows.write.format("delta").saveAsTable(tgt_table)
else:
    # %% SCD2 merge
    tgt = DeltaTable.forName(spark, tgt_table)
    current = (tgt.toDF().filter("is_current = true")
               .select(F.col("id").alias("t_id"), F.col("hash_diff").alias("t_hash")))

    candidates = (new_rows.join(current, new_rows.id == current.t_id, "left")
                  .filter("t_hash IS NULL OR t_hash <> hash_diff"))
    changed = candidates.filter("t_hash IS NOT NULL").drop("t_id", "t_hash")
    inserts = candidates.drop("t_id", "t_hash")

    # changed rows appear twice: once to close the old version, once to insert the new one
    stage = (changed.withColumn("merge_key", F.col("id"))
             .unionByName(inserts.withColumn("merge_key", F.lit(None).cast("string"))))

    (tgt.alias("t").merge(stage.alias("s"), "t.id = s.merge_key AND t.is_current = true")
        .whenMatchedUpdate(set={"is_current": F.lit(False), "effective_to": F.col("s.effective_from")})
        .whenNotMatchedInsertAll()
        .execute())

# %% Quick check
spark.sql(f"""
SELECT is_current, count(*) AS rows, count(DISTINCT id) AS ids
FROM {tgt_table} GROUP BY is_current
""").show()
