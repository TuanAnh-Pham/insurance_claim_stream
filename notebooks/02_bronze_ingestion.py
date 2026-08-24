# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze ingestion with Auto Loader

# COMMAND ----------

from pyspark.sql import functions as F

DEFAULTS = {
    "catalog": "main", "schema": "insurance_claims", "landing_schema": "insurance_claims",
    "volume": "claim_events", "checkpoint_root": "_checkpoints",
}
for name, default in DEFAULTS.items():
    dbutils.widgets.text(name, default)
config = {name: dbutils.widgets.get(name) for name in DEFAULTS}

volume_root = f"/Volumes/{config['catalog']}/{config['landing_schema']}/{config['volume']}"
source_path = f"{volume_root}/incoming"
schema_path = f"{volume_root}/{config['checkpoint_root']}/bronze-schema"
checkpoint_path = f"{volume_root}/{config['checkpoint_root']}/bronze"
target = f"`{config['catalog']}`.`{config['schema']}`.`bronze_claim_events`"

# COMMAND ----------

bronze = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "text")
    .option("cloudFiles.schemaLocation", schema_path)
    .load(source_path)
    .select(
        F.col("value").alias("raw_payload"),
        F.col("_metadata.file_path").alias("source_file"),
        F.col("_metadata.file_modification_time").alias("file_modification_time"),
        F.current_timestamp().alias("ingested_at"),
    )
)
(
    bronze.writeStream.option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(target)
    .awaitTermination()
)
