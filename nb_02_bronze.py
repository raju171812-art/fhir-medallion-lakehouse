# Fabric Notebook: 03_bronze_processing.py
# Reads raw JSON pages for one resource + date and appends to bronze.<resource>.
# Keeps the full nested FHIR resource (no flattening) + metadata columns.

# %% PARAMETERS
resource_name = "Patient"
ingest_date   = "2026-11-03"     # same value as from_date used in ingestion
run_id        = ""

# %% Imports
import uuid
from pyspark.sql import functions as F

run_id = run_id or str(uuid.uuid4())
raw_path = f"Files/raw/{resource_name}/ingest_date={ingest_date}/*.json"
target   = f"bronze.{resource_name.lower()}"

# %% Read raw Bundles (one JSON document per file)
raw = (spark.read.option("multiLine", True).json(raw_path)
       .withColumn("source_file", F.col("_metadata.file_path")))

# %% Explode entries; api_url comes from the Bundle's own "self" link
bronze = (raw
    .withColumn("api_url_or_params",
                F.expr("filter(link, x -> x.relation = 'self')[0].url"))
    .withColumn("entry", F.explode("entry"))
    .select(
        F.col("entry.resource").alias("resource"),
        F.col("entry.resource.id").alias("resource_id"),
        F.col("entry.resource.meta.lastUpdated").alias("source_last_updated"),
        F.col("api_url_or_params"),
        F.col("source_file"),
    )
    .withColumn("extraction_timestamp", F.current_timestamp())
    .withColumn("ingest_date", F.lit(ingest_date))
    .withColumn("run_id", F.lit(run_id))
)

# %% Idempotent append: remove this date's rows if re-run, then append
if spark.catalog.tableExists(target):
    spark.sql(f"DELETE FROM {target} WHERE ingest_date = '{ingest_date}'")

(bronze.write.format("delta").mode("append")
    .option("mergeSchema", "true")          # FHIR schemas vary between pages/days
    .partitionBy("ingest_date")
    .saveAsTable(target))

n = spark.table(target).filter(f"ingest_date = '{ingest_date}'").count()
spark.createDataFrame(
    [(run_id, resource_name, "bronze", 0, None, None, None, n, raw_path, None, "SUCCESS", None)],
    "run_id string, resource_name string, layer string, page_no int, api_url string, "
    "api_call_time timestamp, http_status int, rows_fetched int, raw_file_path string, "
    "data_saved_time timestamp, status string, error_message string"
).withColumn("data_saved_time", F.current_timestamp()) \
 .write.mode("append").saveAsTable("meta.pipeline_log")
print(f"{target}: {n} rows for {ingest_date}")
