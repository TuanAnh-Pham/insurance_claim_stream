# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Set up catalog objects

# COMMAND ----------

DEFAULTS = {
    "catalog": "main",
    "schema": "insurance_claims",
    "landing_schema": "insurance_claims",
    "volume": "claim_events",
    "checkpoint_root": "_checkpoints",
}
for name, default in DEFAULTS.items():
    dbutils.widgets.text(name, default)
config = {name: dbutils.widgets.get(name) for name in DEFAULTS}


def ident(value):
    """Quote a user-configured SQL identifier."""
    return f"`{value.replace('`', '``')}`"


catalog = ident(config["catalog"])
schema = ident(config["schema"])
landing_schema = ident(config["landing_schema"])
volume = ident(config["volume"])

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{landing_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{landing_schema}.{volume}")

# COMMAND ----------

table_root = f"{catalog}.{schema}"
spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {table_root}.bronze_claim_events (
      raw_payload STRING, source_file STRING, file_modification_time TIMESTAMP,
      ingested_at TIMESTAMP
    ) USING DELTA"""
)
spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {table_root}.silver_claim_events (
      event_id STRING, claim_id STRING, policy_id STRING, event_type STRING,
      event_time TIMESTAMP, sequence_number BIGINT, claim_amount DECIMAL(18,2),
      status STRING, source STRING, source_file STRING, ingested_at TIMESTAMP
    ) USING DELTA"""
)
spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {table_root}.quarantine_claim_events (
      raw_payload STRING, source_file STRING, ingested_at TIMESTAMP,
      validation_error STRING, quarantined_at TIMESTAMP
    ) USING DELTA"""
)
spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {table_root}.latest_claim_state (
      event_id STRING, claim_id STRING, policy_id STRING, event_type STRING,
      event_time TIMESTAMP, sequence_number BIGINT, claim_amount DECIMAL(18,2),
      status STRING, source STRING, source_file STRING, ingested_at TIMESTAMP
    ) USING DELTA"""
)
