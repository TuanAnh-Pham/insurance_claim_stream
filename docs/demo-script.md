# Demo script

1. Open `00_setup` and show the centralized widgets, then run it to provision the objects.
2. Run `01_generate_events` with 25 claims and explain that duplicate and invalid rows are deliberately injected.
3. Run `02_bronze_ingestion`; show that bronze preserves the original JSON and source filename.
4. Run `03_silver_processing`; compare valid silver rows with the quarantine table.
5. Run `04_latest_claim_state`; point out that each claim now has exactly one row.
6. Run `01_generate_events` again with another batch, followed by notebooks 02 through 04. Explain checkpointed discovery and idempotent merge behavior.
7. Run `05_monitoring_and_demo` and use its summary queries to discuss throughput, quality, and current business state.

Suggested talking points are auditability, late/retried event handling, explicit business validation, deterministic ordering, and the separation between event history and query-friendly state.
