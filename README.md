# Medical Warehouse — Event-Driven Inventory Analytics

Event-driven inventory analytics warehouse for a medical/pharmaceutical operation. Operator-submitted stock movement events travel through Apache Kafka into a Bronze → Silver → Gold Spark pipeline, land in a queryable analytical warehouse in PostgreSQL, and are served to Power BI and a REST API consumed by warehouse staff.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Flow](#data-flow)
- [Repository Layout](#repository-layout)
- [Tech Stack](#tech-stack)
- [Infrastructure](#infrastructure)
- [Configuration](#configuration)
- [Quick Start](#quick-start)
- [CLI Reference](#cli-reference)
- [Operator Webapp](#operator-webapp)
- [Monitoring Platform](#monitoring-platform)
- [Warehouse Schema](#warehouse-schema)
- [Security Model](#security-model)
- [Airflow DAGs](#airflow-dags)
- [Tests](#tests)

---

## Overview

The system tracks four inventory event types — `STOCK_RECEIVED`, `STOCK_SOLD`, `STOCK_ADJUSTED`, `STOCK_EXPIRED` — alongside procurement and sales events, across three independently-orchestrated Kafka → Parquet → PostgreSQL pipelines.

Inventory quantities are **never stored as a running balance**. Every stock movement is appended as an immutable event; the current balance is derived by summing all deltas for a given product–warehouse–batch combination. A physical snapshot table (`fact_inventory_balance`) is maintained after each pipeline run so Power BI can avoid re-summing millions of events on each query.

Nine analytical domains are served through semantic views in PostgreSQL: inventory, procurement, sales, prescriptions/compliance, supplier management, financial (AP/AR), customer analytics, audit, and staff performance.

Two independent Flask services complement the data pipeline:

| Service | Port | Audience | Purpose |
|---|---|---|---|
| **Operator Webapp** | 8080 | Warehouse staff | Stock entry, purchase order management, reorder policy configuration |
| **Monitoring Platform** | 8787 | Engineers / DevOps | Operational dashboard, pipeline health probes, job runner |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  Operator Webapp (port 8080)                                     │
│  POST /api/v1/stock/*  •  reorder policies  •  purchase orders   │
└────────────────────┬─────────────────────────────────────────────┘
                     │ writes OLTP + emits Kafka events
                     ▼
┌──────────────────────────────────────────────────────────────────┐
│  medwarehouse_master (PostgreSQL OLTP)                           │
│  products  •  suppliers  •  warehouse_locations                  │
│  inventory_lots  •  goods_receipts  •  sales  •  prescriptions   │
└────────┬───────────────────────────────────────────────────────┬─┘
         │ FDW (read-only)                Kafka topics           │
         │                  ┌─────────────────────────────────┐  │
         │                  │  inventory_events               │  │
         │                  │  procurement_events             │◀─┘
         │                  │  sales_events                   │
         │                  └──────────────┬──────────────────┘
         │                                 │ PySpark Structured Streaming
         │                                 ▼
         │                  ┌──────────────────────────────────┐
         │                  │  Bronze Layer (Parquet)          │
         │                  │  Immutable raw JSON, append-only │
         │                  └──────────────┬───────────────────┘
         │                                 │ PySpark batch
         │                                 ▼
         │                  ┌──────────────────────────────────┐
         │                  │  Silver Layer (Parquet)          │
         │                  │  Validated + deduplicated        │
         │                  │  Quarantine for failed records   │
         │                  └──────────────┬───────────────────┘
         │                                 │ PySpark JDBC write
         │                                 ▼
         └──────────────────▶ medwarehouse_analytics (PostgreSQL)
                               stg_*  →  fact_*  →  dim_* (SCD-2)
                               fact_inventory_balance
                               pending_purchase_orders
                               30+ semantic views
                                        │
                                        │ analytics_reader role
                                        ▼
                             Power BI  •  Monitoring Platform (8787)
```

---

## Data Flow

Each step maps to a CLI command or an Airflow DAG task:

| Step | Command | What it does |
|---|---|---|
| 1 | `produce inventory` | Emit sample events to the `inventory_events` Kafka topic |
| 2 | `spark inventory-bronze` | Structured Streaming: Kafka → Bronze Parquet (long-running) |
| 3 | `spark inventory-silver` | Batch: Bronze → validated Silver Parquet + Quarantine |
| 4 | `spark stage-inventory-events` | Batch JDBC write: Silver → `analytics.stg_inventory_events` |
| 5 | `warehouse refresh-dimensions` | SCD-2 upsert from FDW-backed `master.*` dimension tables |
| 6 | `warehouse load-facts` | Idempotent insert from staging → `fact_inventory_events` |
| 7 | `warehouse refresh-views` | Recreate all semantic BI views |
| 8 | `warehouse quality-checks` | Seven integrity assertions; raises on any failure |
| 9 | `warehouse refresh-balance` | MERGE `v_inventory_balance` → `fact_inventory_balance` snapshot |
| 10 | `warehouse check-reorders` | Insert `DRAFT` purchase orders for stock below reorder threshold |

Steps 4–10 are composed into `orchestration build-gold` and into the Airflow DAG `inventory_gold_pipeline`.

### Silver validation rules

The following violations route records to `data/quarantine/<domain>_events/`:

- Top-level fields (`event_id`, `event_type`, `event_time`, `producer`) must be non-null
- `event_type` must be a recognised value for the domain
- `product_id`, `warehouse_id`, `batch_number`, `quantity_delta` must be non-null
- `expiry_date` required for `STOCK_RECEIVED`, `STOCK_SOLD`, `STOCK_EXPIRED`
- `STOCK_RECEIVED` quantity must be positive; `STOCK_SOLD` / `STOCK_EXPIRED` must be negative
- `STOCK_ADJUSTED` must be non-zero and carry a `reason`
- `STOCK_SOLD` must carry a `sale_id`
- Duplicate `event_id` values are quarantined (lowest-offset record wins)

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
.
├── src/
│   ├── medwarehouse/                   # Core pipeline package (python -m medwarehouse)
│   │   ├── __main__.py                 # Entry point
│   │   ├── cli.py                      # CLI argument parser and command dispatch
│   │   ├── config.py                   # All settings from env vars (lru_cache singleton)
│   │   ├── logging.py                  # Structured logging setup
│   │   ├── contracts/
│   │   │   ├── _utils.py               # Shared: deterministic_uuid(), to_utc_iso()
│   │   │   ├── inventory.py            # STOCK_* event schema + validator + sample generator
│   │   │   ├── procurement.py          # PO_* event schema + validator
│   │   │   └── sales.py                # SALE_* event schema + validator
│   │   ├── producers/
│   │   │   ├── common.py               # emit_events() + run_producer() shared utilities
│   │   │   ├── inventory.py            # Inventory sample producer
│   │   │   ├── procurement.py          # Procurement sample producer
│   │   │   └── sales.py                # Sales sample producer
│   │   ├── spark/
│   │   │   ├── session.py              # SparkSession builder
│   │   │   └── jobs/
│   │   │       ├── _bronze.py          # Shared: Kafka → Parquet streaming logic
│   │   │       ├── _silver.py          # Shared: validate + deduplicate + quarantine
│   │   │       ├── _stage.py           # Shared: Silver → PostgreSQL JDBC write
│   │   │       ├── inventory_bronze.py / _silver.py / _stage.py
│   │   │       ├── procurement_bronze.py / _silver.py / _stage.py
│   │   │       └── sales_bronze.py / _silver.py / _stage.py
│   │   ├── warehouse/
│   │   │   ├── _common.py              # Shared: validate_silver_ready(), call_warehouse_function()
│   │   │   ├── postgres.py             # psycopg2 connection context manager
│   │   │   ├── sql_runner.py           # Template-variable SQL execution
│   │   │   ├── inventory.py            # Python wrappers for inventory stored procedures
│   │   │   ├── procurement.py          # Python wrappers for procurement stored procedures
│   │   │   └── sales.py                # Python wrappers for sales stored procedures
│   │   └── platform/                   # Monitoring dashboard (Flask, port 8787)
│   │       ├── api/                    # Blueprint: REST API + HTML page routes
│   │       ├── auth.py                 # API key middleware (hmac.compare_digest)
│   │       ├── cache.py                # TTLCache with stampede protection
│   │       ├── control/                # Job runner, infra actions, job catalog (SQLite-backed)
│   │       ├── probes/                 # Health probes (infra, warehouse, pipeline, artifacts, jobs, airflow)
│   │       ├── services/               # StatusService, AlertEngine
│   │       └── ui/                     # Jinja2 templates + static assets
│   └── medwarehouse_webapp/            # Operator data-entry webapp (Flask, port 8080)
│       ├── __main__.py                 # Entry point: python -m medwarehouse_webapp
│       ├── app.py                      # Flask application factory
│       ├── db.py                       # DB connection helpers
│       ├── api/
│       │   ├── stock.py                # Blueprint: stock movement endpoints
│       │   ├── inventory.py            # Blueprint: reference data reads
│       │   ├── reorder.py              # Blueprint: reorder policy CRUD
│       │   └── orders.py               # Blueprint: purchase order lifecycle
│       └── services/
│           ├── stock_service.py        # Validate → write OLTP → emit Kafka → trigger pipeline
│           ├── reorder_service.py      # Reorder policy CRUD against analytics DB
│           ├── order_service.py        # PO state machine + supplier email dispatch
│           └── idempotency.py          # In-memory idempotency key store with background TTL eviction
├── sql/warehouse/                      # SQL bootstrap files; executed in numeric order
│   ├── 01_roles_and_schemas.sql        # Roles, schemas, postgres_fdw extension
│   ├── 02_fdw_master_access.sql        # Foreign server + user mappings (16 tables)
│   ├── 03_inventory_dimensions.sql     # SCD-2 dimension tables + refresh functions
│   ├── 04_inventory_fact_and_staging.sql
│   ├── 05_inventory_loads_and_views.sql
│   ├── 06_reorder_and_balance.sql      # fact_inventory_balance, reorder_policies, pending_purchase_orders
│   ├── 07_reorder_functions.sql        # refresh_inventory_balance(), check_reorder_thresholds()
│   ├── 08_procurement_warehouse.sql    # Procurement staging, facts, views, quality checks
│   ├── 09_sales_warehouse.sql          # Sales staging, facts, views + dim_customer
│   └── 10_analytical_domains.sql       # Cross-domain views (compliance, financial, audit, staff)
├── airflow/dags/
│   ├── inventory_gold_pipeline.py      # 8 tasks (includes balance refresh + reorder check)
│   ├── procurement_gold_pipeline.py    # 6 tasks
│   └── sales_gold_pipeline.py          # 6 tasks
├── schemas/kafka/
│   ├── inventory_events.json
│   ├── procurement_events.json
│   └── sales_events.json
├── docker/
│   ├── airflow/Dockerfile              # Airflow image with PySpark + project dependencies
│   └── postgres/init/                  # DB creation scripts + master data restore
├── scripts/
│   ├── lib/common.sh                   # Shared bash utilities sourced by all scripts
│   ├── start_services.sh               # Start PostgreSQL + Kafka (optionally + Airflow)
│   ├── stop_services.sh
│   ├── setup_local_stack.sh
│   ├── run_inventory_flow.sh           # End-to-end inventory pipeline convenience script
│   ├── run_procurement_flow.sh
│   └── run_sales_flow.sh
├── tests/
│   ├── test_cli.py                     # CLI argument parsing smoke tests
│   ├── test_config.py                  # Settings loading and credential validation
│   ├── test_contracts.py               # Inventory event building and validation rules
│   ├── test_domain_contracts.py        # Procurement and sales contract validation
│   ├── test_platform.py                # StatusService, ControlPlaneStore, Flask routes
│   ├── test_sql_runner.py              # SQL template rendering
│   ├── test_warehouse_functions.py     # Warehouse wrappers, auth middleware, credential warnings
│   └── test_webapp_services.py         # Stock service, IdempotencyStore, TTLCache stampede, order state machine
├── data/                               # Runtime output (Bronze/Silver/Quarantine Parquet; SQLite store)
├── docs/                               # Project documentation
├── jars/
│   └── postgresql-42.7.3.jar           # JDBC driver for Spark → PostgreSQL
├── docker-compose.yml
├── pyproject.toml
├── requirements.txt
├── requirements-dev.txt
└── .env.example
```

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Language | Python | 3.11+ |
| Stream processing | PySpark (Structured Streaming + batch) | 3.5.1 |
| Message broker | Apache Kafka (Confluent Platform) | 7.5.0 |
| Coordination | Apache ZooKeeper | 7.5.0 |
| Analytical store | PostgreSQL with `postgres_fdw` | 16 |
| DB driver (Python) | psycopg2-binary | 2.9.9 |
| DB driver (Spark) | PostgreSQL JDBC | 42.7.3 |
| Kafka client | confluent-kafka | 2.4.0 |
| Orchestration | Apache Airflow (LocalExecutor) | 2.8.1 |
| Monitoring + Webapp | Flask + Jinja2 | 2.x |
| Containerisation | Docker Compose | v2 |

---

## Infrastructure

Core services (always required):

| Service | Image | Port | Role |
|---|---|---|---|
| `postgres` | `postgres:16` | 5432 | Hosts both `medwarehouse_master` and `medwarehouse_analytics` |
| `zookeeper` | `confluentinc/cp-zookeeper:7.5.0` | 2181 | Kafka coordination |
| `kafka` | `confluentinc/cp-kafka:7.5.0` | 9092 | Event bus (external); 29092 (internal Docker network) |

Optional Airflow services (started with `--with-airflow`):

| Service | Port | Role |
|---|---|---|
| `airflow-webserver` | **8090** | DAG management UI — remapped from the default 8080 to avoid conflict with the operator webapp |
| `airflow-scheduler` | — | DAG scheduling backend |

All services have `healthcheck` definitions and `restart: unless-stopped`. Kafka and the Airflow application services use `depends_on: condition: service_healthy` so containers do not race on startup.

All services propagate `HTTP_PROXY` / `HTTPS_PROXY` / `NO_PROXY` from `.env` for corporate proxy environments.

---

## Configuration

Copy `.env.example` to `.env`. All application settings are prefixed `MW_`.

| Variable | Default | Description |
|---|---|---|
| `MW_ENV` | `local` | Environment name; non-`local`/`dev` environments enforce strong credentials |
| `MW_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap address |
| `MW_KAFKA_INVENTORY_TOPIC` | `inventory_events` | Inventory Kafka topic name |
| `MW_MASTER_DB_*` | `localhost:5432/medwarehouse_master` | OLTP source database |
| `MW_ANALYTICS_ADMIN_DB_*` | `localhost:5432/medwarehouse_analytics` | Analytics DB (admin role) |
| `MW_ANALYTICS_WRITER_DB_*` | same host / `spark_writer` | Analytics DB (Spark write role) |
| `MW_ANALYTICS_READER_DB_*` | same host / `analytics_reader` | Analytics DB (BI read role) |
| `MW_ANALYTICS_FDW_READER_USER` | `fdw_reader` | FDW user inside the analytics DB |
| `MW_SPARK_MASTER` | `local[*]` | Spark master URL |
| `MW_PRODUCER_MAX_EVENTS` | `10` | Default event cap for sample producers |
| `MW_SAMPLE_SEED` | `medical-warehouse-local` | Determinism seed for reproducible sample data |
| `MW_SAMPLE_START_TIME` | `2026-01-01T08:00:00+00:00` | First event timestamp for sample data |
| `MW_WEBAPP_API_KEY` | (empty) | Operator webapp API key — empty disables auth (dev only) |
| `MW_PLATFORM_API_KEY` | (empty) | Monitoring platform API key — empty disables auth (dev only) |
| `MW_PLATFORM_SECRET_KEY` | (random per-process) | Flask session secret for flash messages |
| `MW_PLATFORM_HOST` | `127.0.0.1` | Platform host used by the webapp for pipeline trigger calls |
| `MW_PLATFORM_PORT` | `8787` | Platform port |
| `MW_SMTP_HOST` | (empty) | SMTP server for supplier PO emails — empty logs instead of sending |
| `AIRFLOW_ADMIN_PASSWORD` | `admin` | Airflow admin password; set before first `docker compose up` |

---

## Quick Start

```bash
# 1. Copy and configure environment
cp .env.example .env

# 2. Start core infrastructure (PostgreSQL + Kafka)
./scripts/start_services.sh
# To also start Airflow (UI at http://localhost:8090):
./scripts/start_services.sh --with-airflow

# 3. Set Python path
export PYTHONPATH=src

# 4. Bootstrap the analytics warehouse (run once — applies all 10 SQL files)
python -m medwarehouse --log-level INFO warehouse bootstrap

# 5. Start Bronze ingestion in a dedicated terminal (long-running stream)
python -m medwarehouse spark inventory-bronze

# 6. Emit sample inventory events (second terminal)
python -m medwarehouse produce inventory --max-events 20

# 7. Run the full Gold pipeline
python -m medwarehouse spark inventory-silver
python -m medwarehouse orchestration build-gold

# 8. Start the monitoring platform
python -m medwarehouse platform serve --host 127.0.0.1 --port 8787
# Browse to http://127.0.0.1:8787

# 9. Start the operator webapp (separate terminal)
python -m medwarehouse_webapp --host 127.0.0.1 --port 8080
# Health check: curl http://127.0.0.1:8080/health
```

Or use the convenience script for the full inventory pipeline end-to-end:

```bash
./scripts/run_inventory_flow.sh 20
```

---

## CLI Reference

All commands use the form `python -m medwarehouse [--log-level LEVEL] <group> <subcommand> [options]`.

### `produce`

```
python -m medwarehouse produce {inventory|procurement|sales} [--max-events N] [--dry-run]
```

Emits deterministic sample events to the corresponding Kafka topic. The inventory producer cycles `STOCK_RECEIVED → STOCK_ADJUSTED → STOCK_SOLD → STOCK_RECEIVED → STOCK_EXPIRED` with a configurable seed and start time for reproducibility. `--dry-run` logs events without publishing to Kafka.

### `spark`

```
python -m medwarehouse spark inventory-bronze [--starting-offsets {earliest|latest}]
python -m medwarehouse spark inventory-silver
python -m medwarehouse spark stage-inventory-events

python -m medwarehouse spark procurement-bronze [--starting-offsets {earliest|latest}]
python -m medwarehouse spark procurement-silver
python -m medwarehouse spark stage-procurement-events

python -m medwarehouse spark sales-bronze [--starting-offsets {earliest|latest}]
python -m medwarehouse spark sales-silver
python -m medwarehouse spark stage-sales-events
```

`*-bronze` jobs are long-running Structured Streaming processes. All others are batch jobs.

### `warehouse`

```
python -m medwarehouse warehouse bootstrap
python -m medwarehouse warehouse refresh-dimensions
python -m medwarehouse warehouse load-facts
python -m medwarehouse warehouse refresh-views
python -m medwarehouse warehouse quality-checks
python -m medwarehouse warehouse refresh-balance
python -m medwarehouse warehouse check-reorders
python -m medwarehouse warehouse inventory-gold
```

| Subcommand | Description |
|---|---|
| `bootstrap` | Runs all SQL files in `sql/warehouse/` in numeric order |
| `refresh-balance` | MERGEs `v_inventory_balance` into the physical `fact_inventory_balance` snapshot |
| `check-reorders` | Compares `fact_inventory_balance` against `reorder_policies`; inserts `DRAFT` purchase orders |
| `inventory-gold` | Shortcut: `refresh-dimensions → load-facts → refresh-views → quality-checks` |

Procurement and sales equivalents: `refresh-procurement-dimensions`, `load-procurement-facts`, `procurement-quality-checks`, `build-procurement-gold`; and equivalently for sales.

### `orchestration`

```
python -m medwarehouse orchestration validate-silver
python -m medwarehouse orchestration stage-events
python -m medwarehouse orchestration build-gold
python -m medwarehouse orchestration build-procurement-gold
python -m medwarehouse orchestration build-sales-gold
```

`build-gold` — full post-Silver inventory pipeline: validate → stage → refresh-dims → load-facts → refresh-views → quality-checks → refresh-balance → check-reorders.

### `platform`

```
python -m medwarehouse platform serve [--host HOST] [--port PORT] [--debug]
```

Starts the Flask monitoring platform (default `127.0.0.1:8787`).

---

## Operator Webapp

**Entry point:** `python -m medwarehouse_webapp [--host HOST] [--port PORT]`

The operator webapp (port 8080) is a REST API for warehouse staff. It shares the master and analytics PostgreSQL databases with the pipeline but runs as a completely independent Flask process.

### Stock entry

Each POST endpoint: validates the request body → writes to `inventory_lots` in the OLTP database → emits a Kafka event → fires an async non-blocking trigger to `POST /api/jobs/build_gold/start` on the monitoring platform.

| Method | Endpoint | Emits | Description |
|---|---|---|---|
| POST | `/api/v1/stock/received` | `STOCK_RECEIVED` | Record goods receipt into a warehouse lot |
| POST | `/api/v1/stock/sold` | `STOCK_SOLD` | Record outbound sale; validates sufficient stock |
| POST | `/api/v1/stock/adjusted` | `STOCK_ADJUSTED` | Manual stock correction |
| POST | `/api/v1/stock/expired` | `STOCK_EXPIRED` | Write off an expired batch |
| GET | `/api/v1/stock/movements` | — | Recent movement history (default limit 50) |

### Idempotency

All stock endpoints accept an optional `Idempotency-Key: <uuid>` header. A repeated request carrying the same key within 24 hours returns the cached response without re-executing the operation or re-emitting to Kafka. This protects against duplicate events from client retries after network timeouts.

### Reference data

`GET /api/v1/{products|suppliers|warehouses|inventory|inventory/risk}`

### Reorder automation

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/v1/reorder-policies` | List all active reorder policies |
| POST | `/api/v1/reorder-policies` | Create a policy (reorder_point, reorder_quantity, lead_time_days) |
| PUT | `/api/v1/reorder-policies/{id}` | Update thresholds |
| DELETE | `/api/v1/reorder-policies/{id}` | Deactivate (sets `active = FALSE`) |

After each Gold pipeline run, `check_reorder_thresholds()` compares `fact_inventory_balance` against active `reorder_policies` and inserts `DRAFT` purchase orders for any product below its threshold.

### Purchase order lifecycle

`DRAFT → APPROVED → SENT → RECEIVED / CANCELLED`

| Method | Endpoint | Action |
|---|---|---|
| GET | `/api/v1/orders` | List purchase orders with optional status filter |
| POST | `/api/v1/orders/{po_id}/approve` | Approve a DRAFT order |
| POST | `/api/v1/orders/{po_id}/send` | Mark as SENT and dispatch supplier email (if SMTP configured) |
| POST | `/api/v1/orders/{po_id}/cancel` | Cancel a DRAFT or APPROVED order |
| POST | `/api/v1/orders/{po_id}/receive` | Mark as RECEIVED and emit a `STOCK_RECEIVED` Kafka event |

Full request and response schemas: [docs/api-reference.md](docs/api-reference.md)

---

## Monitoring Platform

**Entry point:** `python -m medwarehouse platform serve` (default port 8787)

| Page | Path | Purpose |
|---|---|---|
| Dashboard | `/` | Aggregated health across all subsystems; active alert summary |
| Pipeline | `/pipeline` | Bronze → Warehouse freshness metrics and consistency gaps |
| Warehouse | `/warehouse` | DB reachability, table/view presence, row counts |
| Artifacts | `/artifacts` | Bronze, Silver, and Quarantine filesystem statistics |
| Infrastructure | `/infrastructure` | Docker service health; Start/Stop infrastructure buttons |
| Jobs | `/jobs` | Trigger and monitor predefined pipeline commands |
| Runbook | `/runbook` | In-app operator guide for diagnosing and recovering failures |

The REST API at `/api/status` (and sub-paths `/pipeline`, `/infra`, `/alerts`) mirrors page data as JSON. Append `?refresh=hard` to bypass the 60-second TTL cache.

Available `job_id` values for `POST /api/jobs/{job_id}/start`:

```
warehouse_bootstrap  inventory_producer  inventory_bronze  inventory_silver
inventory_stage  refresh_dimensions  load_inventory_facts  refresh_views
quality_checks  build_gold  refresh_balance  check_reorders
procurement_producer  procurement_bronze  procurement_silver  build_procurement_gold
sales_producer  sales_bronze  sales_silver  build_sales_gold
```

---

## Warehouse Schema

### Dimensions (SCD Type 2)

All dimension tables carry `valid_from`, `valid_to`, `is_current`. Source data flows in from `master.*` via FDW. The `v_dim_*_current` views filter `WHERE is_current = TRUE`.

| Table | Natural key | Tracked attributes |
|---|---|---|
| `analytics.dim_product` | `product_id` | `supplier_id`, `brand_name`, `generic_name`, `form_factor`, `hsn_code` |
| `analytics.dim_supplier` | `supplier_id` | `supplier_name`, `gstin` |
| `analytics.dim_warehouse` | `warehouse_id` | `warehouse_name`, `temperature_range` |
| `analytics.dim_customer` | `customer_id` | `customer_type` |

### Fact tables (range-partitioned by month)

| Table | Domain | Contents |
|---|---|---|
| `analytics.fact_inventory_events` | Inventory | Append-only; one row per stock event |
| `analytics.fact_procurement_events` | Procurement | Append-only; one row per PO lifecycle event |
| `analytics.fact_sales_events` | Sales | Append-only; includes computed `line_revenue` |

### Operational tables

| Table | Description |
|---|---|
| `analytics.fact_inventory_balance` | Physical inventory snapshot per product+warehouse+batch; refreshed after each Gold run |
| `analytics.reorder_policies` | Operator-configured reorder thresholds (`reorder_point`, `reorder_quantity`, `lead_time_days`) |
| `analytics.pending_purchase_orders` | Auto-generated DRAFT POs awaiting operator approval and dispatch |

### Key BI views

| View | Description |
|---|---|
| `v_inventory_snapshot` | Non-zero stock balances enriched with product and warehouse names |
| `v_reorder_risk` | Products at or approaching their reorder threshold with risk status |
| `v_purchase_order_status` | Purchase orders with product and supplier names |
| `v_po_lifecycle` | PO progression with full lifecycle timestamps and computed lead time |
| `v_supplier_performance` | Fulfillment rate, fill rate, and average lead time per supplier |
| `v_revenue_summary` | Daily net revenue by product, warehouse, and customer type |
| `v_controlled_substance_register` | Dispensing register for Schedule H1/X/NDPS drugs (regulatory requirement) |
| `v_supplier_license_expiry` | License status per supplier: VALID / WARNING / CRITICAL / EXPIRED |
| `v_audit_activity` | Audit log enriched with username and role; `password_hash` excluded |

Full schema reference: [docs/data-dictionary.md](docs/data-dictionary.md)

---

## Security Model

### Database roles

| Role | Permissions |
|---|---|
| `spark_writer` | SELECT/INSERT/TRUNCATE on staging; INSERT on facts; EXECUTE on all warehouse procedures |
| `analytics_reader` | SELECT on all analytics tables and views |
| `fdw_reader` | SELECT on `medwarehouse_master` tables via FDW only |

All warehouse procedures use `SECURITY DEFINER` — callers need only `EXECUTE` privilege, not direct table access.

### API authentication

Both services authenticate via static API key. Keys are compared using `hmac.compare_digest` to prevent timing-based extraction.

```
X-API-Key: <key>
# or
Authorization: Bearer <key>
```

Set `MW_WEBAPP_API_KEY` and `MW_PLATFORM_API_KEY` in `.env`. Leaving either variable empty disables authentication for that service (acceptable in `local` development only).

### Credential enforcement

`get_settings()` calls `_warn_weak_credentials()` at startup. In `local`/`dev` environments, weak default passwords (e.g. `postgres`) log a `WARNING`. In all other environments they raise `RuntimeError` immediately, preventing deployment with insecure defaults.

Full security model: [docs/security.md](docs/security.md)

---

## Airflow DAGs

Three DAGs with `schedule=None` (manual trigger only) and `max_active_runs=1`. Access the Airflow UI at **`http://localhost:8090`** (port remapped from the default 8080 to avoid conflict with the operator webapp).

### `inventory_gold_pipeline` — 8 tasks

```
validate_silver_availability
        ↓
stage_inventory_events
        ↓
refresh_inventory_dimensions
        ↓
load_inventory_event_facts
        ↓
refresh_inventory_semantic_views
        ↓
inventory_quality_checks
        ↓
refresh_inventory_balance
        ↓
check_reorder_thresholds
```

### `procurement_gold_pipeline` — 6 tasks

`validate_silver → stage → refresh_dims → load_facts → refresh_views → quality_checks`

### `sales_gold_pipeline` — 6 tasks

`validate_silver → stage → refresh_dims → load_facts → refresh_views → quality_checks`

All DAGs share the same Python callables as the corresponding `orchestration build-*` CLI commands.

---

## Tests

```bash
export PYTHONPATH=src
pytest tests/ -q
# 82 tests in ~0.6 s — no live infrastructure required
```

| File | Coverage |
|---|---|
| `test_cli.py` | CLI argument parsing and dry-run producer |
| `test_config.py` | Settings construction and credential validation |
| `test_contracts.py` | Inventory event building and all validation rules |
| `test_domain_contracts.py` | Procurement and sales contract validation |
| `test_platform.py` | StatusService, ControlPlaneStore, ControlPlaneService, Flask routes |
| `test_sql_runner.py` | SQL template-variable rendering |
| `test_warehouse_functions.py` | Warehouse Python wrappers, auth middleware, credential warnings |
| `test_webapp_services.py` | Stock service (no `::uuid` casts), IdempotencyStore (TTL eviction + thread safety), TTLCache stampede protection, order service state machine, reorder SET clause construction |

All tests run without live infrastructure. Integration tests that require a PostgreSQL connection are decorated `@pytest.mark.integration` and activated with `MW_INTEGRATION_TEST=1`.
