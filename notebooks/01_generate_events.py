# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Generate synthetic claim events

# COMMAND ----------

import json
import random
import uuid
from datetime import datetime, timezone

DEFAULTS = {
    "catalog": "main", "schema": "insurance_claims", "landing_schema": "insurance_claims",
    "volume": "claim_events", "checkpoint_root": "_checkpoints", "claim_count": "25",
}
for name, default in DEFAULTS.items():
    dbutils.widgets.text(name, default)
config = {name: dbutils.widgets.get(name) for name in DEFAULTS}

landing_path = f"/Volumes/{config['catalog']}/{config['landing_schema']}/{config['volume']}/incoming"
claim_count = int(config["claim_count"])

# COMMAND ----------

now = datetime.now(timezone.utc).isoformat()
events = []
for index in range(claim_count):
    claim_id = f"CLM-{uuid.uuid4().hex[:12].upper()}"
    amount = round(random.uniform(100, 25_000), 2)
    events.append({
        "event_id": str(uuid.uuid4()), "claim_id": claim_id, "policy_id": f"POL-{index:06d}",
        "event_type": "CLAIM_OPENED", "event_time": now, "sequence_number": 1,
        "claim_amount": amount, "status": "OPEN", "source": "demo-generator",
    })

# Include one retry and one invalid record to make deduplication and quarantine visible.
if events:
    events.append(dict(events[0]))
events.append({
    "event_id": str(uuid.uuid4()), "claim_id": f"CLM-{uuid.uuid4().hex[:12].upper()}",
    "policy_id": "POL-INVALID", "event_type": "UNKNOWN", "event_time": now,
    "sequence_number": 1, "claim_amount": -10, "status": "OPEN", "source": "demo-generator",
})

batch_id = uuid.uuid4().hex
dbutils.fs.mkdirs(landing_path)
dbutils.fs.put(f"{landing_path}/batch-{batch_id}.json", "\n".join(json.dumps(event) for event in events), True)
print(f"Wrote {len(events)} records to batch {batch_id}")
