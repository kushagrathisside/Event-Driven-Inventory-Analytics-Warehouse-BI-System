# Functional Requirements Checklist

Assessment date: 2026-04-27 UTC

This checklist is based on the current repository structure, the canonical `src/medwarehouse` package, supporting SQL/Docker/Airflow assets, and the live workspace state.

## Status legend

- Complete: implemented in the current codebase
- Partial: implemented only in part, or present in code but not fully runtime-verified
- Not started: no meaningful implementation found

## Current progress summary

- Inventory domain: mostly implemented end to end in code
- Procurement domain: producer only
- Sales domain: producer only
- Runtime verification today: partial, because Docker services are stopped and the analytics warehouse is not reachable
- Test confidence: basic to moderate, because CLI/config/contracts/platform smoke tests pass, but Spark and warehouse flows do not have end-to-end automated tests

## Functional checklist

| Area | Functional requirement | Status | Progress notes | Evidence |
| --- | --- | --- | --- | --- |
| Platform foundation | Provide one canonical command surface for producers, Spark jobs, warehouse actions, orchestration, and platform UI | Complete | The CLI exposes all major operational commands through `python -m medwarehouse`. | `src/medwarehouse/cli.py` |
| Configuration | Load runtime configuration from environment variables instead of hardcoded values | Complete | Kafka, Spark, paths, and database connections are centrally configured. | `src/medwarehouse/config.py` |
| Inventory contracts | Define a shared inventory event schema and validation rules | Complete | Inventory event contract, deterministic IDs, and validation logic are implemented. | `src/medwarehouse/contracts/inventory.py` |
| Inventory producer | Produce deterministic inventory events to Kafka for repeatable local runs | Complete | Inventory producer is implemented and wired into the CLI. | `src/medwarehouse/producers/inventory.py` |
| Bronze ingestion | Continuously ingest Kafka inventory events into immutable Bronze parquet | Complete | Structured Streaming Bronze ingestion is implemented. Local Bronze parquet already exists in `data/bronze/inventory_events`. | `src/medwarehouse/spark/jobs/inventory_bronze.py` |
| Silver transformation | Parse, validate, normalize, deduplicate, and quarantine bad inventory events | Complete | Silver job builds validation errors, dedupe logic, and quarantine output. Local Silver parquet exists; quarantine path exists with no parquet files at the moment. | `src/medwarehouse/spark/jobs/inventory_silver.py` |
| Warehouse staging | Stage Silver inventory events into PostgreSQL | Complete | Silver-to-JDBC staging is implemented for `analytics.stg_inventory_events`. | `src/medwarehouse/spark/jobs/inventory_stage.py` |
| Warehouse bootstrap | Create schemas, roles, FDW access, dimensions, staging tables, fact table, views, and grants | Complete | Bootstrap applies the full warehouse SQL stack in order. | `src/medwarehouse/warehouse/inventory.py`, `sql/warehouse/` |
| Master data integration | Mirror operational master data into analytics through FDW | Complete | FDW server, user mappings, and foreign table imports are defined. | `sql/warehouse/02_fdw_master_access.sql` |
| Dimensions | Maintain SCD Type-2 supplier, product, and warehouse dimensions | Complete | Refresh functions for all three dimensions are defined. | `sql/warehouse/03_inventory_dimensions.sql` |
| Fact model | Store inventory events in an append-only partitioned fact table | Complete | Staging table, partitioned fact table, and partition helpers are implemented. | `sql/warehouse/04_inventory_fact_and_staging.sql` |
| Fact loading | Load staged inventory events into the fact table with event-level deduplication | Complete | Fact load skips already-loaded `event_id` values. | `sql/warehouse/05_inventory_loads_and_views.sql` |
| Semantic layer | Expose BI-safe balance and snapshot views | Complete | Current-dimension, balance, and snapshot views are defined. | `sql/warehouse/05_inventory_loads_and_views.sql` |
| Quality checks | Run data-quality and reconciliation checks before considering Gold healthy | Complete | SQL quality checks exist and are called from the warehouse orchestration layer. | `src/medwarehouse/warehouse/inventory.py`, `sql/warehouse/05_inventory_loads_and_views.sql` |
| Orchestration | Provide an orchestrated Gold build flow from Silver validation through quality checks | Complete | CLI orchestration and an Airflow DAG exist for the inventory Gold pipeline. | `src/medwarehouse/cli.py`, `airflow/dags/inventory_gold_pipeline.py` |
| Control plane | Provide a local UI/API to inspect status and trigger jobs | Complete | Flask app, job controller, infra controls, and status snapshot logic are implemented. | `src/medwarehouse/platform/` |
| Infrastructure automation | Start/stop PostgreSQL, Zookeeper, Kafka, and optional Airflow locally | Complete | Shell scripts and Docker Compose cover local stack lifecycle. | `scripts/start_services.sh`, `scripts/stop_services.sh`, `docker-compose.yml` |
| Procurement domain | Support procurement events beyond simple sample production | Partial | A procurement producer exists, but the project explicitly marks downstream pipeline work as not implemented yet. | `src/medwarehouse/producers/procurement.py`, `src/medwarehouse/platform/catalog.py` |
| Sales domain | Support sales events beyond simple sample production | Partial | A sales producer exists, but the project explicitly marks downstream pipeline work as not implemented yet. | `src/medwarehouse/producers/sales.py`, `src/medwarehouse/platform/catalog.py` |
| Runtime observability | Report artifact presence, infrastructure health, warehouse reachability, role visibility, and DAG availability | Complete | The status layer inspects artifacts, Docker state, warehouse metadata, and DAG presence. | `src/medwarehouse/platform/status.py` |
| Automated verification | Provide automated tests for core project behavior | Partial | Basic tests pass for CLI, config, contracts, SQL templating, and platform smoke checks. Spark transformations, JDBC staging, and warehouse SQL flows are not covered by automated end-to-end tests. | `tests/` |

## Live workspace evidence

As of 2026-04-27 UTC:

- `data/bronze/inventory_events` exists with 176 parquet files
- `data/silver/inventory_events` exists with 16 parquet files
- `data/quarantine/inventory_events` exists but currently has 0 parquet files
- `airflow/dags/inventory_gold_pipeline.py` exists
- Docker Compose is available locally, but all tracked services are currently stopped
- The analytics warehouse is not reachable in the current session because infrastructure is stopped
- `PYTHONPATH=src venv/bin/python -m unittest discover -s tests -v` passes with 9 tests

## Progress evaluation

### Strongly implemented now

- The inventory pipeline is the clear MVP and is the most mature part of the repo.
- The project already has a coherent path from Kafka event generation to Bronze, Silver, warehouse staging, fact loading, semantic views, Airflow orchestration, and a local control plane.
- The SQL warehouse layer is more than a placeholder; it includes dimensions, fact partitioning, semantic views, grants, and quality checks.

### Partially complete

- Procurement and sales are present only as sample event producers. They are not yet modeled as full Bronze/Silver/warehouse pipelines.
- Operational verification is incomplete today because the local stack is down, so the warehouse-backed parts are structurally implemented but not currently live-validated.
- Automated test coverage is still shallow around the most important data-path logic: Spark transformations, JDBC loads, Airflow task execution, and warehouse stored procedures.

### Structural notes

- The canonical implementation is under `src/medwarehouse`, but older prototype folders still exist under `src/producer` and `src/spark/jobs`.
- Legacy local Gold parquet outputs are still present under `data/gold/`, even though the newer architecture positions PostgreSQL facts and semantic views as the canonical Gold layer.

## Overall assessment

- Inventory MVP readiness: high in code structure, medium in live verification
- Cross-domain warehouse readiness: low to medium
- Overall project maturity: good prototype / near-MVP for inventory, not yet a fully completed multi-domain warehouse

## Recommended next milestones

1. Bring the local infrastructure up and run a fresh warehouse-backed inventory flow to confirm live end-to-end behavior.
2. Add automated tests for Spark Silver logic, staging writes, and warehouse load/quality-check behavior.
3. Decide whether procurement and sales should become first-class pipelines or remain future scope.
4. Remove or archive legacy prototype paths and stale Gold parquet outputs so the repo reflects the canonical architecture more clearly.
