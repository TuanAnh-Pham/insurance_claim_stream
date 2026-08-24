# Insurance Claim Stream

Reference Databricks implementation of a medallion pipeline for synthetic insurance claim events. The project demonstrates append-only bronze ingestion, validated and deduplicated silver events, and a latest-state claim table maintained from event time and sequence number.

## Repository layout

- `notebooks/00_setup.py` creates the configured catalog, schemas, volume, and Delta tables.
- `notebooks/01_generate_events.py` writes synthetic JSON events to the landing volume.
- `notebooks/02_bronze_ingestion.py` incrementally ingests files with Auto Loader.
- `notebooks/03_silver_processing.py` validates and deduplicates bronze events.
- `notebooks/04_latest_claim_state.py` merges the newest event for each claim into a current-state table.
- `notebooks/05_monitoring_and_demo.py` displays quality and operational metrics.
- `src/insurance_claim_stream` contains reusable schemas and transformations.
- `resources/` contains the optional Databricks Asset Bundle job definition.

## Configuration

Every notebook defines widgets for `catalog`, `schema`, `landing_schema`, `volume`, and `checkpoint_root`. Bundle jobs pass those values as task parameters. Change target-specific values in `databricks.yml`; no workspace paths or catalog names are spread through pipeline logic.

The defaults use `main.insurance_claims` and a `claim_events` volume. A Unity Catalog-enabled workspace and a cluster/runtime supporting Auto Loader and Delta Lake are required. The setup notebook must be run by an identity permitted to create catalogs, schemas, volumes, and tables. If catalog creation is centrally managed, pre-create the catalog and remove that statement from the setup notebook.

## Run interactively

1. Import the repository into a Databricks workspace or connect it with Databricks Repos.
2. Run the numbered notebooks in order, overriding widgets as appropriate.
3. Re-run generation through latest state to observe incremental processing.
4. Run the monitoring notebook for table counts, invalid-record counts, and recent claims.

The streaming notebooks use `availableNow` triggers, making them suitable for repeatable demos and scheduled jobs while preserving checkpointed incremental behavior.

## Deploy as a Databricks Asset Bundle (optional)

Install and authenticate the Databricks CLI, then validate and deploy:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run insurance_claim_stream -t dev
```

Set `DATABRICKS_HOST` (or select an authenticated CLI profile) and override bundle variables with `--var="catalog=...,schema=..."`. The included job uses an existing cluster because workspace policies vary; pass its ID through `--var="cluster_id=..."`.

## Local development

The reusable package targets PySpark on Databricks. Run local tests in an environment with Java, PySpark, and pytest:

```bash
PYTHONPATH=src pytest
```
