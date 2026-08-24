# Architecture

## Data flow

```text
synthetic producer -> Unity Catalog volume -> bronze Delta table
                                              |
                                              v
                              validated silver + quarantine
                                              |
                                              v
                                  latest claim state
```

The producer emits one JSON object per claim event. Auto Loader tracks discovered files in a checkpoint and appends the raw payload plus ingestion metadata to bronze. Silver parses the payload with an explicit schema, annotates business-rule failures, quarantines invalid rows, and deterministically removes duplicate event identifiers. The state step ranks events by sequence, event time, and event ID before applying an idempotent Delta merge.

## Design decisions

- **Replayable bronze:** raw JSON and file metadata remain available for audit and reprocessing.
- **Event identity:** `event_id` identifies retries; `claim_id` groups the lifecycle.
- **Ordering:** producer sequence is authoritative, with event time and event ID as deterministic tie breakers.
- **Incrementality:** independent checkpoints isolate each streaming stage.
- **Configuration:** widgets supply all environment-specific object names and paths.
- **Quality:** invalid data is retained in a quarantine table rather than silently dropped.

For a production deployment, replace synthetic generation with an event bus or managed file delivery, configure table access controls, add expectations and alerting, and define retention and recovery objectives.
