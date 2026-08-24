# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Monitoring and demo

# COMMAND ----------

from pyspark.sql import functions as F

DEFAULTS = {
    "catalog": "main", "schema": "insurance_claims", "landing_schema": "insurance_claims",
    "volume": "claim_events", "checkpoint_root": "_checkpoints",
}
for name, default in DEFAULTS.items():
    dbutils.widgets.text(name, default)
config = {name: dbutils.widgets.get(name) for name in DEFAULTS}
table_root = f"`{config['catalog']}`.`{config['schema']}`"

# COMMAND ----------

tables = ["bronze_claim_events", "silver_claim_events", "quarantine_claim_events", "latest_claim_state"]
counts = [(name, spark.table(f"{table_root}.`{name}`").count()) for name in tables]
display(spark.createDataFrame(counts, "table_name string, record_count long"))

# COMMAND ----------

display(
    spark.table(f"{table_root}.`quarantine_claim_events`")
    .groupBy("validation_error").count().orderBy(F.desc("count"))
)

# COMMAND ----------

display(
    spark.table(f"{table_root}.`latest_claim_state`")
    .select("claim_id", "policy_id", "status", "claim_amount", "event_time", "sequence_number")
    .orderBy(F.desc("event_time")).limit(20)
)
