# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Validate and deduplicate silver events

# COMMAND ----------

import sys
from pathlib import Path
from pyspark.sql import functions as F

sys.path.append(str(Path.cwd().parent / "src"))
from insurance_claim_stream.schemas import CLAIM_EVENT_SCHEMA
from insurance_claim_stream.transformations import deduplicate_events
from insurance_claim_stream.validation import with_validation

DEFAULTS = {
    "catalog": "main", "schema": "insurance_claims", "landing_schema": "insurance_claims",
    "volume": "claim_events", "checkpoint_root": "_checkpoints",
}
for name, default in DEFAULTS.items():
    dbutils.widgets.text(name, default)
config = {name: dbutils.widgets.get(name) for name in DEFAULTS}

table_root = f"`{config['catalog']}`.`{config['schema']}`"
volume_root = f"/Volumes/{config['catalog']}/{config['landing_schema']}/{config['volume']}"

# COMMAND ----------

def process_batch(batch, batch_id):
    parsed = batch.withColumn("event", F.from_json("raw_payload", CLAIM_EVENT_SCHEMA))
    expanded = parsed.select("raw_payload", "source_file", "ingested_at", "event.*")
    checked = with_validation(expanded)

    invalid = checked.where(~F.col("is_valid")).select(
        "raw_payload", "source_file", "ingested_at", "validation_error",
        F.current_timestamp().alias("quarantined_at"),
    )
    invalid.write.mode("append").saveAsTable(f"{table_root}.`quarantine_claim_events`")

    valid = checked.where("is_valid").drop("raw_payload", "validation_error", "is_valid")
    deduplicate_events(valid).write.mode("append").saveAsTable(f"{table_root}.`silver_claim_events`")


query = (
    spark.readStream.option("skipChangeCommits", "true").table(f"{table_root}.`bronze_claim_events`")
    .writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", f"{volume_root}/{config['checkpoint_root']}/silver")
    .trigger(availableNow=True).start()
)
query.awaitTermination()
