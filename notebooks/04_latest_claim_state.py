# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Materialize latest claim state

# COMMAND ----------

import sys
from pathlib import Path

sys.path.append(str(Path.cwd().parent / "src"))
from insurance_claim_stream.state import merge_latest_claim_state
from insurance_claim_stream.transformations import latest_claim_events

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

def update_state(batch, batch_id):
    merge_latest_claim_state(spark, latest_claim_events(batch), f"{table_root}.`latest_claim_state`")


query = (
    spark.readStream.option("skipChangeCommits", "true").table(f"{table_root}.`silver_claim_events`")
    .writeStream.foreachBatch(update_state)
    .option("checkpointLocation", f"{volume_root}/{config['checkpoint_root']}/latest-state")
    .trigger(availableNow=True).start()
)
query.awaitTermination()
