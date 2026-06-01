# Medical Warehouse

Event-driven inventory analytics warehouse for a medical/pharmaceutical operation. Ingests inventory business events from Kafka, processes them through a Bronze → Silver → Gold pipeline using PySpark, and materialises a queryable analytical warehouse in PostgreSQL.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Flow](#data-flow)
- [Repository Layout](#repository-layout)
- [Tech Stack](#tech-stack)
- [Infrastructure](#infrastructure)
- [CLI Reference](#cli-reference)
- [Warehouse Schema](#warehouse-schema)
- [Security Model](#security-model)
- [Configuration](#configuration)
- [Quick Start](#quick-start)
- [Control Plane](#control-plane)
- [Airflow DAG](#airflow-dag)
- [Tests](#tests)

---

## Overview

The system tracks four inventory event types — `STOCK_RECEIVED`, `STOCK_SOLD`, `STOCK_ADJUSTED`, `STOCK_EXPIRED` — published to Kafka by upstream producers. Each event carries a typed payload (product, warehouse, batch, quantity delta, expiry date, etc.). The warehouse accumulates these events into a fact table and exposes two BI-facing views: `v_inventory_balance` (running totals by batch) and `v_inventory_snapshot` (enriched, non-zero balances only).

---

## Architecture

```
┌──────────────────────┐       Kafka topics         ┌────────────────────┐
│  Upstream Producers  │──── inventory_events ──────▶│  Bronze (Parquet)  │
│  (or local sample)   │──── procurement_events      │  Raw Kafka payload │
│                      │──── sales_events             └────────┬───────────┘
└──────────────────────┘                                       │ Spark Structured
                                                               │ Streaming
                                                    ┌──────────▼───────────┐
                                                    │  Silver (Parquet)    │
                                                    │  Validated + Deduped │
                                                    │  + Quarantine        │
                                                    └──────────┬───────────┘
                                                               │ Spark Batch (JDBC)
                                                    ┌──────────▼───────────┐
                                                    │  analytics.stg_*     │
                                                    │  (PostgreSQL staging)│
                                                    └──────────┬───────────┘
                                                               │ SQL procedures
                                              ┌────────────────▼────────────────┐
                                              │  analytics schema (PostgreSQL)  │
                                              │  dim_product / dim_supplier /   │
                                              │  dim_warehouse  (SCD Type 2)    │
                                              │  fact_inventory_events          │
                                              │  v_inventory_balance            │
                                              │  v_inventory_snapshot           │
                                              └─────────────────────────────────┘
```

Two PostgreSQL databases:

| Database | Purpose |
|---|---|
| `medwarehouse_master` | Operational source (products, suppliers, warehouse locations) |
| `medwarehouse_analytics` | Analytical warehouse; exposes `master` tables via PostgreSQL FDW |

---

## Data Flow

Each step corresponds to a CLI command or Airflow task:

| Step | Command | What it does |
|---|---|---|
| 1 | `produce inventory` | Emits deterministic sample events to `inventory_events` Kafka topic |
| 2 | `spark inventory-bronze` | Spark Structured Streaming reads Kafka → appends raw payloads to Bronze Parquet |
| 3 | `spark inventory-silver` | Spark batch reads Bronze → validates, deduplicates, routes bad records to Quarantine → writes Silver Parquet |
| 4 | `spark stage-inventory-events` | Spark batch reads Silver → truncate-and-load into `analytics.stg_inventory_events` via JDBC |
| 5 | `warehouse refresh-dimensions` | Calls `analytics.refresh_inventory_dimensions()` — SCD Type 2 upsert from FDW-backed `master.*` tables |
| 6 | `warehouse load-facts` | Calls `analytics.load_inventory_event_facts()` — idempotent insert from staging to `fact_inventory_events`, skips already-loaded event IDs |
| 7 | `warehouse refresh-views` | Calls `analytics.refresh_inventory_semantic_layer()` — recreates all BI-facing views |
| 8 | `warehouse quality-checks` | Calls `analytics.run_inventory_quality_checks()` — raises on any failing assertion |

Steps 4–8 are composed into `orchestration build-gold` and into the Airflow DAG `inventory_gold_pipeline`.

### Silver validation rules

The Silver job enforces these rules per event; failures go to the Quarantine dataset:

- Top-level fields (`event_id`, `event_type`, `event_time`, `producer`) must be non-null
- `event_type` must be one of `STOCK_RECEIVED`, `STOCK_SOLD`, `STOCK_ADJUSTED`, `STOCK_EXPIRED`
- `product_id`, `warehouse_id`, `batch_number`, `quantity_delta` must be non-null
- `expiry_date` required for `STOCK_RECEIVED`, `STOCK_SOLD`, `STOCK_EXPIRED`
- `STOCK_RECEIVED` quantity must be positive; `STOCK_SOLD` and `STOCK_EXPIRED` must be negative
- `STOCK_ADJUSTED` quantity must be non-zero and must carry an `adjustment_reason`
- `STOCK_SOLD` must carry a `sale_id`
- Duplicate `event_id` values are quarantined (lowest-offset/latest-timestamp wins)

### Quality checks

Seven assertions run at the end of every Gold build:

| Check | What it catches |
|---|---|
| `staging_null_business_keys` | Staging rows missing `product_id`, `warehouse_id`, or `batch_number` |
| `staging_duplicate_event_ids` | Duplicate `event_id` values still in staging |
| `staged_events_not_loaded` | Staging rows that did not make it into the fact table |
| `fact_null_dimension_keys` | Fact rows with null `product_sk` or `warehouse_sk` |
| `fact_orphan_product_dimension` | Fact rows whose `product_sk` no longer exists in `dim_product` |
| `fact_orphan_warehouse_dimension` | Fact rows whose `warehouse_sk` no longer exists in `dim_warehouse` |
| `fact_rows_in_default_partition` | Fact rows that fell into the catch-all partition (missing time-range partition) |

---

## Repository Layout

```
medical-warehouse/
├── src/
│   └── medwarehouse/           # Canonical Python package (PYTHONPATH=src)
│       ├── __main__.py         # Entry point: python -m medwarehouse
│       ├── cli.py              # Argument parser and command dispatch
│       ├── config.py           # All settings loaded from env vars (lru_cache)
│       ├── logging.py          # Structured logging setup
│       ├── contracts/
│       │   └── inventory.py    # Event envelope, validation rules, sample generator
│       ├── producers/          # Kafka producers (inventory, procurement, sales)
│       ├── spark/
│       │   ├── session.py      # Shared SparkSession builder
│       │   └── jobs/
│       │       ├── inventory_bronze.py   # Streaming ingest → Parquet
│       │       ├── inventory_silver.py   # Validate + dedupe → Silver/Quarantine
│       │       └── inventory_stage.py    # Silver → PostgreSQL staging (JDBC)
│       ├── warehouse/
│       │   ├── inventory.py    # Python wrappers for SQL procedures
│       │   ├── postgres.py     # psycopg2 connection helpers
│       │   └── sql_runner.py   # Template-variable SQL execution
│       └── platform/           # Flask control plane (port 8787)
│           ├── api/            # Blueprint: REST API + HTML page routes
│           ├── control/        # Job runner and infra action handlers
│           ├── probes/         # Health probes (infra, artifacts, warehouse, jobs, pipeline)
│           ├── services/       # StatusService (TTL cache), AlertService
│           └── ui/             # Jinja2 templates + static assets
├── sql/
│   └── warehouse/
│       ├── 01_roles_and_schemas.sql        # Roles, schemas, FDW extension
│       ├── 02_fdw_master_access.sql        # Foreign server + user mapping
│       ├── 03_inventory_dimensions.sql     # SCD Type 2 dim tables + refresh functions
│       ├── 04_inventory_fact_and_staging.sql  # Staging table, fact table, partition helpers
│       └── 05_inventory_loads_and_views.sql   # Load function, semantic views, quality checks, grants
├── airflow/
│   └── dags/
│       └── inventory_gold_pipeline.py     # DAG: validate_silver → refresh_dims → stage → load → views → checks
├── schemas/
│   └── kafka/
│       ├── inventory_events.json
│       ├── procurement_events.json
│       └── sales_events.json
├── docker/
│   └── airflow/Dockerfile
├── scripts/
│   ├── start_services.sh       # Start Docker Compose stack (optional --with-airflow)
│   ├── stop_services.sh
│   ├── setup_local_stack.sh
│   └── run_inventory_flow.sh   # End-to-end local run helper
├── tests/
│   ├── test_cli.py
│   ├── test_config.py
│   ├── test_contracts.py
│   ├── test_platform.py
│   └── test_sql_runner.py
├── data/                       # Runtime output (Bronze, Silver, Quarantine Parquet; checkpoints)
├── jars/
│   └── postgresql-42.7.3.jar  # JDBC driver for Spark → PostgreSQL writes
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3 |
| Stream processing | PySpark 3.5.1 (Structured Streaming + batch) |
| Message broker | Apache Kafka (Confluent Platform 7.5.0) |
| Coordination | Apache ZooKeeper 7.5.0 |
| Analytical store | PostgreSQL 16 with `postgres_fdw` |
| DB driver (Python) | psycopg2-binary 2.9.9 |
| DB driver (Spark) | postgresql JDBC 42.7.3 |
| Kafka client | confluent-kafka 2.4.0 |
| Orchestration | Apache Airflow 2.8.1 (LocalExecutor) |
| Control plane | Flask 2.2.5 + Jinja2 |
| Containerisation | Docker Compose |

---

## Infrastructure

Core services (always required):

| Service | Image | Port |
|---|---|---|
| PostgreSQL | `postgres:16` | 5432 |
| ZooKeeper | `confluentinc/cp-zookeeper:7.5.0` | 2181 |
| Kafka | `confluentinc/cp-kafka:7.5.0` | 9092 |

Optional Airflow services (started with `--with-airflow`):

| Service | Port |
|---|---|
| Airflow webserver | 8080 |
| Airflow scheduler | — |

All services propagate `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY` from `.env` for corporate network environments.

---

## CLI Reference

All commands go through `python -m medwarehouse [--log-level LEVEL] <command>`.

### `produce`

```
python -m medwarehouse produce {inventory|procurement|sales} [--max-events N] [--dry-run]
```

Emits deterministic sample events to the corresponding Kafka topic. The inventory producer generates a repeating sequence of `STOCK_RECEIVED → STOCK_ADJUSTED → STOCK_SOLD → STOCK_RECEIVED → STOCK_EXPIRED` events with configurable seed and start time for reproducibility.

### `spark`

```
python -m medwarehouse spark inventory-bronze [--starting-offsets {earliest|latest}]
python -m medwarehouse spark inventory-silver
python -m medwarehouse spark stage-inventory-events
```

- `inventory-bronze` — long-running Structured Streaming job; leave running in a dedicated terminal.
- `inventory-silver` — batch job; reads Bronze, writes Silver + Quarantine Parquet (overwrite mode).
- `stage-inventory-events` — batch job; reads Silver, truncates and reloads `analytics.stg_inventory_events` via JDBC.

### `warehouse`

```
python -m medwarehouse warehouse bootstrap
python -m medwarehouse warehouse refresh-dimensions
python -m medwarehouse warehouse load-facts
python -m medwarehouse warehouse refresh-views
python -m medwarehouse warehouse quality-checks
python -m medwarehouse warehouse inventory-gold
```

- `bootstrap` — runs all SQL files in `sql/warehouse/` in lexicographic order, substituting template variables from config.
- `inventory-gold` — shortcut that runs `refresh-dimensions → load-facts → refresh-views → quality-checks`.

### `orchestration`

```
python -m medwarehouse orchestration validate-silver
python -m medwarehouse orchestration stage-events
python -m medwarehouse orchestration build-gold
```

- `build-gold` — full post-Silver pipeline: validate Silver exists → stage events → build Gold.

### `platform`

```
python -m medwarehouse platform serve [--host HOST] [--port PORT] [--debug]
```

Starts the Flask control plane (default `127.0.0.1:8787`).

---

## Warehouse Schema

### Dimensions (SCD Type 2)

| Table | Natural key | Tracked attributes |
|---|---|---|
| `analytics.dim_product` | `product_id` | `supplier_id`, `brand_name`, `generic_name`, `form_factor`, `hsn_code` |
| `analytics.dim_supplier` | `supplier_id` | `supplier_name`, `gstin` |
| `analytics.dim_warehouse` | `warehouse_id` | `warehouse_name`, `temperature_range` |

All dimensions carry `valid_from`, `valid_to`, `is_current`, `created_at`, `updated_at`. Source data is read from `master.*` tables via PostgreSQL FDW.

### Staging

`analytics.stg_inventory_events` — truncated and reloaded by each Spark staging run. Indexed on `event_id` and `event_time`.

### Fact table

`analytics.fact_inventory_events` — append-only, partitioned by `event_time` (monthly range partitions created on demand). Surrogate keys `product_sk`, `warehouse_sk`, `supplier_sk` link to current dimension rows at load time. Idempotent: `NOT EXISTS` guard on `event_id` prevents double-loads.

### BI views

| View | Description |
|---|---|
| `v_inventory_balance` | Running `SUM(quantity_delta)` grouped by product, warehouse, supplier, batch, expiry |
| `v_inventory_snapshot` | `v_inventory_balance` joined to current dimension views; excludes zero-balance batches |
| `v_dim_product_current` | Current rows from `dim_product` |
| `v_dim_supplier_current` | Current rows from `dim_supplier` |
| `v_dim_warehouse_current` | Current rows from `dim_warehouse` |

---

## Security Model

| Role | Permissions |
|---|---|
| `spark_writer` | `SELECT/INSERT/DELETE/TRUNCATE` on `stg_inventory_events`; `EXECUTE` on all warehouse procedures |
| `analytics_reader` | `SELECT` on all five BI views only |
| `fdw_reader` | FDW-backed read path into `master` schema inside the analytics database |

All warehouse procedures use `SECURITY DEFINER` so callers need only `EXECUTE` permission, not direct table access.

---

## Configuration

Copy `.env.example` to `.env`. All settings are prefixed `MW_`.

| Variable | Default | Description |
|---|---|---|
| `MW_ENV` | `local` | Environment name |
| `MW_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap address |
| `MW_KAFKA_INVENTORY_TOPIC` | `inventory_events` | Inventory Kafka topic |
| `MW_MASTER_DB_*` | `localhost:5432/medwarehouse_master` | Operational source database |
| `MW_ANALYTICS_ADMIN_DB_*` | `localhost:5432/medwarehouse_analytics` | Analytics DB (admin user) |
| `MW_ANALYTICS_WRITER_DB_*` | same host / `spark_writer` | Analytics DB (Spark write user) |
| `MW_ANALYTICS_READER_DB_*` | same host / `analytics_reader` | Analytics DB (BI read user) |
| `MW_ANALYTICS_FDW_READER_USER` | `fdw_reader` | FDW role inside analytics DB |
| `MW_SPARK_MASTER` | `local[*]` | Spark master URL |
| `MW_PRODUCER_MAX_EVENTS` | `10` | Default event cap for producers |
| `MW_SAMPLE_SEED` | `medical-warehouse-local` | Determinism seed for sample generator |
| `MW_SAMPLE_START_TIME` | `2026-01-01T08:00:00+00:00` | First event timestamp for sample data |
| `MW_SAMPLE_PRODUCT_ID` | `P-LOCAL-CALPOL-500` | Product ID used in local sample events |
| `MW_SAMPLE_CURRENCY` | `INR` | Currency for sample events |

---

## Quick Start

```bash
# 1. Copy config
cp .env.example .env

# 2. Start core infrastructure
./scripts/start_services.sh
# or with Airflow:
./scripts/start_services.sh --with-airflow

# 3. Set Python path
export PYTHONPATH=src

# 4. Bootstrap the analytics warehouse (runs all SQL migrations)
python -m medwarehouse --log-level INFO warehouse bootstrap

# 5. Start Bronze ingestion (keep running in a dedicated terminal)
python -m medwarehouse spark inventory-bronze

# 6. Emit sample events (in a second terminal)
python -m medwarehouse produce inventory --max-events 10

# 7. Run the full Gold pipeline
python -m medwarehouse spark inventory-silver
python -m medwarehouse orchestration build-gold

# 8. Start the control plane
python -m medwarehouse platform serve --host 127.0.0.1 --port 8787
# Open http://127.0.0.1:8787
```

---

## Control Plane

The Flask application at `http://127.0.0.1:8787` provides operator visibility and basic control:

| Page | Path | Purpose |
|---|---|---|
| Dashboard | `/` | Aggregated health across all subsystems |
| Pipeline | `/pipeline` | Bronze → Warehouse freshness and consistency gaps |
| Warehouse | `/warehouse` | Reachability, row counts, view state, quality-check results |
| Artifacts | `/artifacts` | Bronze, Silver, and Quarantine filesystem statistics |
| Infrastructure | `/infrastructure` | Docker service health; Start/Stop infrastructure buttons |
| Jobs | `/jobs` | Trigger and monitor predefined pipeline commands |
| Runbook | `/runbook` | In-app operator guide for diagnosing and recovering failures |

The REST API at `/api/status` (and sub-paths `/pipeline`, `/infra`, `/alerts`) mirrors the page data as JSON. Append `?refresh=hard` to bypass the TTL cache.

---

## Airflow DAG

`inventory_gold_pipeline` (located at `airflow/dags/inventory_gold_pipeline.py`):

```
validate_silver_availability
        ↓
refresh_inventory_dimensions
        ↓
stage_inventory_events
        ↓
load_inventory_event_facts
        ↓
refresh_inventory_semantic_views
        ↓
inventory_quality_checks
```

The DAG is manually triggered (`schedule=None`), has `max_active_runs=1`, and shares the same Python callables as the CLI's `orchestration build-gold` path.

---

## Tests

```bash
export PYTHONPATH=src
pytest tests/
```

| File | Covers |
|---|---|
| `test_config.py` | Settings loading and defaults |
| `test_contracts.py` | Event envelope building and validation rules |
| `test_cli.py` | CLI argument parsing |
| `test_sql_runner.py` | Template-variable substitution in SQL execution |
| `test_platform.py` | Control plane routes and status service |
