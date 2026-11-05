# Fabric Notebook: nb_04_gold
# Builds reporting tables/views from the CURRENT rows of silver.
# Created as Delta tables so the Lakehouse SQL endpoint / Power BI can read them.

# %% dim_patient (current version only; use silver.patient for full history)
spark.sql("""
CREATE OR REPLACE TABLE gold.dim_patient AS
SELECT id AS patient_id, gender, birth_date,
       family_name, given_name, city, state, country,
       effective_from
FROM silver.patient
WHERE is_current = true
""")

# %% dim_date (built from the dates present in the data)
spark.sql("""
CREATE OR REPLACE TABLE gold.dim_date AS
SELECT d AS date_key, year(d) AS year, month(d) AS month,
       date_format(d, 'MMMM') AS month_name, dayofweek(d) AS day_of_week
FROM (
  SELECT explode(sequence(
      (SELECT min(to_date(period_start)) FROM silver.encounter WHERE is_current),
      (SELECT max(to_date(period_start)) FROM silver.encounter WHERE is_current),
      interval 1 day)) AS d
)
""")

# %% Facts
spark.sql("""
CREATE OR REPLACE TABLE gold.fact_encounter AS
SELECT id AS encounter_id, patient_id, status, class_code,
       to_date(period_start) AS start_date, period_start, period_end
FROM silver.encounter WHERE is_current = true
""")

spark.sql("""
CREATE OR REPLACE TABLE gold.fact_observation AS
SELECT id AS observation_id, patient_id, encounter_id, code, code_display,
       value_number, value_unit, to_date(effective_datetime) AS effective_date
FROM silver.observation WHERE is_current = true
""")

spark.sql("""
CREATE OR REPLACE TABLE gold.fact_condition AS
SELECT id AS condition_id, patient_id, encounter_id, code, code_display,
       clinical_status, to_date(onset_datetime) AS onset_date
FROM silver.condition WHERE is_current = true
""")

# %% Reporting views
spark.sql("""
CREATE OR REPLACE VIEW gold.vw_encounters_per_patient AS
SELECT p.patient_id, p.gender, count(e.encounter_id) AS encounter_count
FROM gold.dim_patient p LEFT JOIN gold.fact_encounter e USING (patient_id)
GROUP BY p.patient_id, p.gender
""")

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_top_conditions AS
SELECT code, code_display, count(*) AS cases, count(DISTINCT patient_id) AS patients
FROM gold.fact_condition GROUP BY code, code_display
""")

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_observation_summary AS
SELECT code, code_display, value_unit, count(*) AS readings,
       round(avg(value_number), 2) AS avg_value
FROM gold.fact_observation GROUP BY code, code_display, value_unit
""")

display(spark.sql("SELECT * FROM gold.vw_top_conditions ORDER BY cases DESC LIMIT 10"))
