# Fabric Notebook: notebooks/nb_00_setup_meta.py
# Run ONCE. Creates schemas, metadata tables and seeds the resource config.
# Attach the default Lakehouse (lh_fhir). Use a schema-enabled Lakehouse;
# if yours is not schema-enabled, replace "layer.table" names with "layer_table".

# %% Schemas
for s in ["meta", "bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s}")

# %% Config table (no hardcoding anywhere else)
spark.sql("""
CREATE TABLE IF NOT EXISTS meta.config_resources (
    order_id     INT,
    resource_name STRING,
    endpoint     STRING,
    page_size    INT,
    key_column   STRING,
    is_active    BOOLEAN
) USING DELTA
""")

spark.sql("DELETE FROM meta.config_resources")
spark.sql("""
INSERT INTO meta.config_resources VALUES
 (1, 'Patient',     'https://hapi.fhir.org/baseR4', 100, 'id', true),
 (2, 'Encounter',   'https://hapi.fhir.org/baseR4', 100, 'id', true),
 (3, 'Observation', 'https://hapi.fhir.org/baseR4', 100, 'id', true),
 (4, 'Condition',   'https://hapi.fhir.org/baseR4', 100, 'id', true)
""")

# %% Watermark table
spark.sql("""
CREATE TABLE IF NOT EXISTS meta.watermark (
    resource_name        STRING,
    last_loaded_to_date  STRING,
    updated_at           TIMESTAMP
) USING DELTA
""")

# %% Pipeline log: one row per API call
spark.sql("""
CREATE TABLE IF NOT EXISTS meta.pipeline_log (
    run_id            STRING,
    resource_name     STRING,
    layer             STRING,
    page_no           INT,
    api_url           STRING,
    api_call_time     TIMESTAMP,
    http_status       INT,
    rows_fetched      INT,
    raw_file_path     STRING,
    data_saved_time   TIMESTAMP,
    status            STRING,
    error_message     STRING
) USING DELTA
""")

display(spark.table("meta.config_resources"))
