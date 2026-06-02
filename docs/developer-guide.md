# Developer Guide

## Table of Contents

1. [System Purpose and Context](#1-system-purpose-and-context)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Repository Layout](#3-repository-layout)
4. [Configuration System](#4-configuration-system)
5. [Data Flow: End-to-End](#5-data-flow-end-to-end)
6. [Layer 1 — Event Producers](#6-layer-1--event-producers)
7. [Layer 2 — Apache Kafka](#7-layer-2--apache-kafka)
8. [Layer 3 — Spark Bronze and Silver Pipelines](#8-layer-3--spark-bronze-and-silver-pipelines)
9. [Layer 4 — PostgreSQL Analytical Warehouse](#9-layer-4--postgresql-analytical-warehouse)
10. [Layer 5 — Monitoring Platform and Operator Webapp](#10-layer-5--monitoring-platform-and-operator-webapp)
11. [The Three Business Domains](#11-the-three-business-domains)
12. [Nine Analytical Domains in the Database](#12-nine-analytical-domains-in-the-database)
13. [The Control Plane](#13-the-control-plane)
14. [Alert System](#14-alert-system)
15. [Airflow Orchestration](#15-airflow-orchestration)
16. [Infrastructure and Docker Compose](#16-infrastructure-and-docker-compose)
17. [Testing Strategy](#17-testing-strategy)
18. [Key Design Decisions and Trade-offs](#18-key-design-decisions-and-trade-offs)
19. [Important Nuances and Gotchas](#19-important-nuances-and-gotchas)
20. [Common Developer Workflows](#20-common-developer-workflows)
21. [Extending the System: Adding a New Domain](#21-extending-the-system-adding-a-new-domain)

---

## 1. System Purpose and Context

This repository implements a **medical/pharmaceutical inventory analytics warehouse**. A warehouse operator records stock movements (goods received, sold, adjusted, expired) via a REST API. Those events travel through a streaming pipeline and land in a read-optimised PostgreSQL schema where Power BI reports on inventory levels, reorder risk, procurement lifecycle, and sales velocity.

The system is event-sourced at its heart: inventory quantities are **never stored as a current balance**. Every stock movement is appended as an immutable event, and the current balance is derived by summing all events for a given product–warehouse–batch combination. This means you can reconstruct the state at any point in history by replaying events up to that timestamp.

The full stack involves six technologies working together:

| Technology | Role |
|---|---|
| **Python / Flask** | Event producers, operator webapp REST API, monitoring dashboard |
| **Apache Kafka** | Durable event bus — decouples producers from consumers |
| **Apache Spark (PySpark)** | Streaming ingestion (Bronze), transformation (Silver), batch load (Gold) |
| **PostgreSQL** | Analytical warehouse with FDW, SCD-2 dimensions, partitioned facts |
| **Apache Airflow** | DAG-based orchestration of the Gold pipeline on demand |
| **Docker Compose** | Local development infrastructure |

---

## 2. High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  OLTP Layer (medwarehouse_master DB)                             │
│  inventory_lots · goods_receipts · sales · purchase_orders      │
│  Written by: operator webapp (port 8080)                         │
└────────────────────────────┬─────────────────────────────────────┘
                             │ writes + emits Kafka events
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│  Kafka Topics                                                    │
│  inventory_events · procurement_events · sales_events           │
└────────────────────────────┬─────────────────────────────────────┘
                             │ Spark Structured Streaming
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│  Bronze Layer  (local Parquet, append-only, raw JSON preserved)  │
│  data/bronze/{inventory,procurement,sales}_events/               │
└────────────────────────────┬─────────────────────────────────────┘
                             │ batch Spark read + write
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│  Silver Layer  (validated, normalised, deduplicated Parquet)     │
│  data/silver/{inventory,procurement,sales}_events/               │
│  data/quarantine/  ← records that failed validation              │
└────────────────────────────┬─────────────────────────────────────┘
                             │ batch Spark JDBC write
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│  Gold / Warehouse Layer  (medwarehouse_analytics DB)             │
│  stg_*  →  fact_*  →  dim_*  →  v_*  (semantic views)          │
│  fact_inventory_balance  (physical snapshot for Power BI)        │
│  pending_purchase_orders (auto-created by reorder automation)    │
└────────────────────────────┬─────────────────────────────────────┘
                             │ FDW cross-DB queries + direct reads
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│  Consumption Layer                                               │
│  Power BI ← analytics_reader role                               │
│  Monitoring dashboard (port 8787)                               │
└──────────────────────────────────────────────────────────────────┘
```

The two Flask applications are independent processes:

- **Platform / Monitoring dashboard** (`python -m medwarehouse platform serve`, port 8787) — read-only operational view of the pipeline. Used by engineers.
- **Operator webapp** (`python -m medwarehouse_webapp`, port 8080) — data entry and purchase order management. Used by warehouse staff.

---

## 3. Repository Layout

```
.
├── airflow/dags/          # Airflow DAG definitions (one per domain)
├── data/                  # Runtime data (gitignored except .gitignore markers)
│   ├── bronze/            # Raw Parquet from Kafka
│   ├── silver/            # Validated Parquet
│   ├── quarantine/        # Failed-validation records
│   ├── gold/              # Legacy: pre-FDW gold outputs (no longer primary path)
│   └── control_plane/     # SQLite database for job run history
├── docker/
│   ├── airflow/Dockerfile # Airflow image with PySpark + project deps
│   └── postgres/init/     # DB creation + master data restore scripts
├── docs/                  # All project documentation
├── jars/                  # PostgreSQL JDBC driver for PySpark
├── schemas/kafka/         # JSON Schema for each Kafka event type
├── scripts/               # Shell scripts for local development
│   └── lib/common.sh      # Shared bash library sourced by all scripts
├── sql/warehouse/         # Ordered SQL files for warehouse bootstrap
├── src/
│   ├── medwarehouse/      # Core package: pipeline, platform, CLI
│   │   ├── cli.py         # Entry point: python -m medwarehouse <command>
│   │   ├── config.py      # All configuration via environment variables
│   │   ├── contracts/     # Event schemas and validation logic
│   │   ├── platform/      # Monitoring dashboard (Flask app + probes + alerts)
│   │   ├── producers/     # Kafka event generators (sample + live data)
│   │   ├── spark/         # PySpark Bronze/Silver/Stage jobs
│   │   └── warehouse/     # Python wrappers around PostgreSQL stored procedures
│   └── medwarehouse_webapp/  # Operator data-entry webapp (separate Flask app)
│       ├── api/           # REST API blueprints
│       └── services/      # Business logic (stock, orders, reorder policies)
└── tests/                 # Unit tests (no live DB or Kafka required)
```

---

## 4. Configuration System

**File:** `src/medwarehouse/config.py`

All configuration is read from environment variables at startup and assembled into a frozen `AppSettings` dataclass. There is no YAML, no INI file — everything goes through env vars.

### Why environment variables?

The system targets Docker Compose locally and can trivially be adapted for Kubernetes or any container orchestration. Environment variables are the standard 12-factor config mechanism. Hardcoded config files require file mounts, templating, and update ceremonies; env vars just work.

### The `get_settings()` singleton

```python
@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    ...
```

`get_settings()` is called once per process. `lru_cache(maxsize=1)` makes it a singleton. **This has an important implication for tests**: you must call `get_settings.cache_clear()` in `setUp`/`tearDown` when patching `os.environ`, otherwise the cached settings from a previous test bleed through.

### Frozen dataclasses

`AppSettings` and all its sub-settings are `@dataclass(frozen=True)`. This prevents accidental mutation after construction. Immutability is especially important for settings shared across threads (the monitoring platform is multi-threaded).

### Credential validation

`_warn_weak_credentials()` is called at the end of `get_settings()`. In `local`/`dev` environments, weak passwords like `"postgres"` log a `WARNING`. In any other environment they raise `RuntimeError` immediately — this prevents accidentally deploying with default credentials.

### Key environment variables

| Variable | Purpose | Default |
|---|---|---|
| `MW_ENV` | Environment name (affects credential validation) | `local` |
| `MW_MASTER_DB_*` | OLTP database (medwarehouse_master) | localhost:5432 |
| `MW_ANALYTICS_ADMIN_DB_*` | Analytics DB (admin role) | localhost:5432 |
| `MW_ANALYTICS_WRITER_DB_*` | Analytics DB (spark_writer role) | same host |
| `MW_ANALYTICS_READER_DB_*` | Analytics DB (analytics_reader role) | same host |
| `MW_KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address | localhost:9092 |
| `MW_PLATFORM_PORT` | Platform service port (used by webapp for pipeline trigger) | 8787 |
| `MW_WEBAPP_API_KEY` | API key for operator webapp | blank = no auth |
| `MW_PLATFORM_API_KEY` | API key for monitoring platform | blank = no auth |
| `MW_PLATFORM_SECRET_KEY` | Flask session secret for flash messages | random per-process |

---

## 5. Data Flow: End-to-End

A full inventory stock receipt follows this path:

1. **Operator posts** `POST /api/v1/stock/received` to the webapp (port 8080).
2. Webapp writes a row to `inventory_lots` in the OLTP database (`medwarehouse_master`).
3. Webapp builds an `InventoryEvent` dict using `contracts.inventory.build_inventory_event()` and publishes it to the `inventory_events` Kafka topic.
4. **Bronze Spark job** (`spark inventory-bronze`) reads from Kafka as a stream and writes each raw JSON payload to Parquet in `data/bronze/inventory_events/`.
5. **Silver Spark job** (`spark inventory-silver`) reads Bronze Parquet, parses JSON, validates fields, deduplicates on `event_id`, writes clean events to `data/silver/inventory_events/` and bad records to `data/quarantine/inventory_events/`.
6. **Stage job** (`spark stage-inventory-events`) reads Silver Parquet and bulk-inserts into `analytics.stg_inventory_events` via JDBC.
7. **Warehouse Gold jobs** (called by Airflow DAG or directly):
   - `refresh_inventory_dimensions()` — upserts SCD-2 dimension rows.
   - `load_inventory_event_facts()` — moves staging events into `fact_inventory_events`, de-duplicating on `event_id`.
   - `refresh_inventory_semantic_views()` — refreshes materialised views.
   - `run_inventory_quality_checks()` — asserts referential integrity.
   - `refresh_inventory_balance()` — MERGEs current balances into `fact_inventory_balance`.
   - `check_reorder_thresholds()` — inserts draft `pending_purchase_orders` for depleted stock.
8. **Power BI** queries `analytics.v_inventory_snapshot`, `analytics.fact_inventory_balance`, and `analytics.v_reorder_risk` via the `analytics_reader` role.

---

## 6. Layer 1 — Event Producers

**Location:** `src/medwarehouse/producers/`

There are two distinct producer paths:

### Sample data producers
`inventory.py`, `procurement.py`, `sales.py` — generate deterministic synthetic events for development and testing. They are seeded from `SampleDataSettings` in config, so the same run always produces the same event IDs (important for deduplication tests). Run via:
```
python -m medwarehouse produce inventory --max-events 20
```

### Live webapp producer
The operator webapp's `stock_service.py` calls `producers.common.emit_events()` directly after validating and writing each stock movement to the OLTP DB. This is the production code path.

### `emit_events()` in `producers/common.py`

All producers funnel through `emit_events()`. It accepts an iterable of event dicts and handles both real Kafka publishing and `dry_run` mode (which logs instead of producing). The `confluent_kafka` import is lazy (inside the function body), which means the module is importable in test environments without Kafka installed.

---

## 7. Layer 2 — Apache Kafka

**Topics:** `inventory_events`, `procurement_events`, `sales_events`

Kafka is used as a durable, ordered, replay-capable event bus. The core reason for Kafka rather than writing directly to the DB is **decoupling**: the OLTP write (which must be fast and synchronous) is separated from the analytics pipeline (which can lag, catch up, and be re-run without loss).

### Event schema

Each event follows this structure:

```json
{
  "event_id": "uuid",
  "event_type": "STOCK_RECEIVED",
  "event_time": "2026-01-15T08:30:00+00:00",
  "producer": "webapp",
  "schema_version": 1,
  "payload": {
    "product_id": "...",
    "warehouse_id": "...",
    "batch_number": "...",
    "quantity_delta": 100,
    ...
  }
}
```

The `event_id` is deterministic (seeded UUID-v5) for sample data, and a timestamp-seeded UUID-v5 for webapp-originated events. This means replaying the same operation twice produces the same `event_id`, which allows the warehouse to deduplicate cleanly on the `ON CONFLICT DO NOTHING` clause in `load_inventory_event_facts()`.

### Schema files

`schemas/kafka/{topic}.json` — JSON Schema definitions used for documentation and future schema registry integration.

---

## 8. Layer 3 — Spark Bronze and Silver Pipelines

**Location:** `src/medwarehouse/spark/`

### Shared utilities

Three base modules (`_bronze.py`, `_silver.py`, `_stage.py`) contain the shared logic. Each domain's job (e.g., `inventory_bronze.py`) is a thin wrapper that passes domain-specific parameters.

### Bronze — `_bronze.py`

Reads from Kafka using Spark Structured Streaming with `readStream`. Writes each Kafka message as a raw row with:
- `raw_event` — the original JSON string (preserved for forensics)
- `kafka_key`, `source_topic`, `source_partition`, `source_offset`, `source_kafka_timestamp` — provenance metadata

**Why preserve raw JSON?** If the Silver schema changes or a bug is found, you can re-derive Silver from Bronze without re-reading Kafka.

Bronze jobs run as **long-running streaming processes**. They are started in the background by the flow scripts and must be stopped after producers have flushed. The scripts use a `wait_for_parquet` function with a 90-second timeout to detect when Bronze has written output, then send SIGTERM.

### Silver — `_silver.py`

Reads Bronze Parquet in batch mode. For each domain:
1. Parses the `raw_event` JSON column using the domain's event schema.
2. Adds `validation_errors` — an array of string codes for each failing rule.
3. Splits into `silver` (zero errors) and `quarantine` (one or more errors).

The quarantine split happens in `split_silver_quarantine()`:
```python
silver     = df.filter(F.size("validation_errors") == 0)
quarantine = df.filter(F.size("validation_errors") > 0)
```

Silver records are also deduplicated on `dedupe_key` (= `event_id` if present, else SHA-256 of raw event).

**Why write quarantine?** Validation errors in streaming pipelines are silent by default. Writing failed records to a named quarantine path makes them inspectable and recoverable — an operator can fix the upstream issue and re-process quarantined records.

### Stage — `_stage.py`

Reads Silver Parquet and uses Spark's JDBC writer to insert into `analytics.stg_*_events`. The staging table acts as a buffer — it is truncated before each load (via `TRUNCATE` in the stored procedure), so the fact load is always idempotent.

**Why staging?** Direct Silver-to-fact inserts with JDBC would require the Spark executor to resolve foreign keys (product_sk, warehouse_sk) at row level. Instead, the staging insert is a bulk append, and the `load_*_event_facts()` stored procedure joins staging against dimensions in PostgreSQL — much faster, and keeps complex join logic in SQL where it belongs.

---

## 9. Layer 4 — PostgreSQL Analytical Warehouse

**Database:** `medwarehouse_analytics`

The analytics schema is built by running the SQL files in `sql/warehouse/` in numeric order. `bootstrap_warehouse()` does this.

### SQL file order

| File | Contents |
|---|---|
| `01_roles_and_schemas.sql` | Creates roles (`fdw_reader`, `spark_writer`, `analytics_reader`), schemas (`master`, `analytics`) |
| `02_fdw_master_access.sql` | Creates the `postgres_fdw` extension, foreign server, user mappings, and imports the master DB tables as foreign tables under the `master` schema |
| `03_inventory_dimensions.sql` | SCD-2 dimension tables: `dim_product`, `dim_supplier`, `dim_warehouse`, `dim_customer` |
| `04_inventory_fact_and_staging.sql` | Staging table + range-partitioned `fact_inventory_events` |
| `05_inventory_loads_and_views.sql` | Stored procedures (`load_inventory_event_facts`, `refresh_inventory_dimensions`, etc.) and semantic views |
| `06_reorder_and_balance.sql` | `fact_inventory_balance`, `reorder_policies`, `pending_purchase_orders` tables |
| `07_reorder_functions.sql` | `refresh_inventory_balance()`, `check_reorder_thresholds()` stored procedures |
| `08_procurement_warehouse.sql` | Procurement staging, facts, views |
| `09_sales_warehouse.sql` | Sales staging, facts, views |
| `10_analytical_domains.sql` | Cross-domain views: supplier license expiry, controlled substance register, revenue summary |

### SCD Type-2 dimensions

Dimension tables like `dim_product` use Slowly Changing Dimension Type-2: each change creates a new row with a new `*_sk` surrogate key, with `valid_from` / `valid_to` / `is_current` flags. The `v_dim_*_current` views filter `WHERE is_current = TRUE` for convenience.

**Why SCD-2?** Point-in-time correctness. If a product's name changed on 1 March, a January sale must report with the January name. The fact table stores the surrogate key at the time of loading, so historical queries are automatically correct.

### Foreign Data Wrapper (FDW)

`postgres_fdw` maps selected tables from `medwarehouse_master` into the `master` schema of `medwarehouse_analytics`. This means the analytics DB can JOIN its fact tables against live OLTP reference data without ETL duplication.

**Trade-off:** FDW queries incur a network round-trip to the master DB. For large analytical queries this is a bottleneck. The current design accepts this because the FDW tables are used primarily by stored procedures (not by direct Power BI queries), and the physical dimension tables cache the critical attributes.

### Range-partitioned fact tables

`fact_inventory_events` is partitioned by `event_time` with monthly partitions. `ensure_inventory_fact_partitions()` is called before each load to create missing partitions automatically.

**Why partitioning?** Power BI date-range filters get partition pruning — queries for "last 30 days" skip all older partitions without a full scan. This is important at scale.

### Roles and access control

Three application roles with least-privilege grants:

| Role | Can do |
|---|---|
| `fdw_reader` | SELECT on master DB tables via FDW only |
| `spark_writer` | INSERT on staging, SELECT on dimensions, EXECUTE stored procedures |
| `analytics_reader` | SELECT on all analytics tables and views |

Power BI connects as `analytics_reader`. Spark jobs connect as `spark_writer`. Stored procedures use `SECURITY DEFINER` so they run with the definer's privileges (postgres superuser), not the caller's.

---

## 10. Layer 5 — Monitoring Platform and Operator Webapp

### Monitoring platform (`medwarehouse` package, port 8787)

**Entry point:** `python -m medwarehouse platform serve`

The platform is a Flask application structured around **probes** — classes that each collect a specific slice of system state.

| Probe | What it collects |
|---|---|
| `InfraProbe` | Docker Compose service status via `docker compose ps` |
| `WarehouseProbe` | PostgreSQL reachability, table/view presence, row counts |
| `PipelineProbe` | Parquet file counts and timestamps for all Bronze/Silver/Quarantine paths, plus warehouse counts |
| `ArtifactProbe` | Per-domain filesystem artifact inventory |
| `JobProbe` | Run history from the SQLite control plane store |
| `AirflowProbe` | DAG existence and last-run state for all three DAGs |

All six probes are collected in **parallel** via `ThreadPoolExecutor` in `StatusService._collect_probes()`. `WarehouseProbe` and `PipelineProbe` both need the same warehouse DB query; `StatusService` pre-fetches this once and shares the result with both to avoid a double roundtrip.

Results are assembled into a full status dict, then passed through the alert engine. The full status is cached with a 60-second TTL. The `TTLCache` implementation includes stampede protection via a per-key `threading.Event` — the first thread to encounter a cache miss claims the build slot; subsequent concurrent threads wait on the event rather than each launching their own expensive build.

### Operator webapp (`medwarehouse_webapp` package, port 8080)

**Entry point:** `python -m medwarehouse_webapp`

REST API only (no HTML frontend — that is a separate project). The four stock endpoints each:
1. Validate the request body against `contracts.inventory` rules.
2. Write to the OLTP `inventory_lots` table in the master DB.
3. Emit a Kafka event via `producers.common.emit_events()`.
4. Fire a non-blocking async trigger to `POST /api/jobs/build_gold/start` on the platform service so the Gold pipeline runs.

**Idempotency:** Every POST endpoint supports an `Idempotency-Key` header. If the same key is seen twice within 24 hours, the cached response is returned without re-executing. This protects against client retries after timeouts. The `IdempotencyStore` is an in-memory dict with a background eviction thread. In production, replace it with Redis.

**Reorder automation:** After Gold pipeline execution, `check_reorder_thresholds()` compares `fact_inventory_balance` against `reorder_policies` and inserts `DRAFT` purchase orders for any product below its threshold. Operators review draft POs at `GET /api/v1/orders`, then approve → send → receive. The `send` step dispatches a supplier email via SMTP if `MW_SMTP_HOST` is configured. The `receive` step emits a new `STOCK_RECEIVED` event, closing the loop.

---

## 11. The Three Business Domains

Each domain maps to a set of Kafka events, Spark jobs, warehouse tables, and an Airflow DAG.

### Inventory

The primary domain. Tracks stock movements in a warehouse:
- `STOCK_RECEIVED` — goods arrive, quantity goes up
- `STOCK_SOLD` — goods dispatched to a customer, quantity goes down
- `STOCK_ADJUSTED` — manual correction (damaged goods, audit discrepancy)
- `STOCK_EXPIRED` — batch expired, written off

Current balance = sum of all `quantity_delta` values for a product–warehouse–batch.

### Procurement

Tracks the purchase order lifecycle from a supplier's perspective:
- `PO_CREATED` — a purchase order is raised
- `PO_APPROVED` — internally authorised
- `PO_SENT` — transmitted to supplier
- `PO_RECEIVED` — goods physically received (triggers an inventory receipt)
- `PO_CANCELLED` — order withdrawn

### Sales

Tracks outbound customer sales:
- `SALE_CREATED` — sale confirmed
- `SALE_CANCELLED` — reversed
- `SALE_RETURNED` — customer return

---

## 12. Nine Analytical Domains in the Database

The AI-generated master database (`meddata_full_20250916_175921.dump`) contains nine business domains covering a realistic pharmaceutical operation. These are exposed through the FDW and semantic views:

| Domain | Key Tables / Views | Business Question Answered |
|---|---|---|
| **Inventory** | `fact_inventory_events`, `v_inventory_snapshot`, `fact_inventory_balance` | What stock do we hold right now, and how has it moved? |
| **Procurement** | `fact_procurement_events`, `v_po_lifecycle`, `v_supplier_performance` | Which POs are open, and how do suppliers perform on lead time? |
| **Sales** | `fact_sales_events`, `v_revenue_summary` | What is our revenue by product and time period? |
| **Compliance** | `v_controlled_substance_register`, `v_supplier_license_expiry` | Are controlled substances properly tracked? Are supplier licences current? |
| **Supplier** | `dim_supplier`, `v_dim_supplier_current` | Supplier master data with SCD-2 history |
| **Product** | `dim_product`, `v_dim_product_current` | Product catalogue with pricing and classification |
| **Warehouse** | `dim_warehouse`, `v_dim_warehouse_current` | Warehouse locations and temperature capabilities |
| **Customer** | `dim_customer` | Dispensary and hospital customers |
| **Financial** | `v_revenue_summary`, reorder cost estimates in `v_reorder_risk` | Revenue vs. cost of goods |

---

## 13. The Control Plane

**Location:** `src/medwarehouse/platform/control/`

The control plane is the system that allows the monitoring dashboard to start, stop, and track pipeline jobs. It has three components:

### `ControlPlaneStore` (SQLite)

SQLite in WAL mode is used because the control plane is a local tool running on a developer's machine — there is no need for a full PostgreSQL deployment just for job tracking. WAL mode allows concurrent readers while a single writer commits.

Per-thread connections via `threading.local()` eliminate the overhead of opening a connection per method call. The schema is two tables: `runs` (one row per job execution) and `run_logs` (one row per log line, foreign-keyed to `runs` with `ON DELETE CASCADE`). Runs older than 30 days are pruned at startup to prevent unbounded growth.

### `ProcessSupervisor`

Launches pipeline commands as subprocesses with `start_new_session=True`, which puts each job in its own process group. This allows sending SIGTERM/SIGKILL to the entire group (preventing orphaned child processes if Spark spawns workers).

Stop sequence: SIGTERM → wait up to 10 seconds → SIGKILL. A single watcher thread handles both natural exit detection and SIGKILL escalation (earlier designs used two separate threads, creating a race condition on the final store update — merged into one to eliminate the race).

On startup, `_reconcile_runs()` is called to handle the case where the control plane was restarted while a job was running. Active runs without a live PID are marked `failed`; active runs with a live PID are marked `orphaned` (so operators can decide what to do).

### `ControlPlaneService`

Public API used by Flask routes: `start_job(job_id)`, `stop_job(job_id)`, `run_infra_action("start"|"stop")`. All operations are protected by a `threading.Lock` to prevent double-starts from concurrent HTTP requests.

### `JobSpec` catalog

`catalog.py` contains the complete list of predefined jobs as frozen `JobSpec` dataclasses. A module-level dict provides O(1) lookup. The `command` field is the argv suffix after `python -m medwarehouse`, so jobs are just CLI commands — no magic.

---

## 14. Alert System

**Location:** `src/medwarehouse/platform/services/alerts/`

The alert system is a rules engine: a list of `AlertRule` instances that each receive the full metrics dict and return an `Alert` or `None`.

### Split into submodules

The rules are split by concern:

| Module | Rules |
|---|---|
| `inventory.py` | Freshness, volume anomaly, quarantine volume, pipeline consistency |
| `infra.py` | Warehouse reachability, Kafka/PostgreSQL service status |
| `airflow.py` | DAG failure, DAG staleness |
| `compliance.py` | Supplier licence expiry, controlled substance compliance |

### `AlertEngine`

Manages state transitions: an alert transitions from absent → `ACTIVE` → `RESOLVED` as conditions appear and clear. Recent resolved alerts are kept in a deque for history. The engine evaluates rules against the current metrics snapshot on every status refresh.

### `VolumeBaselineStore`

`VolumeAnomalyRule` detects unusual drops in pipeline volume by comparing the current count against a rolling average of the last N observations (default 6). The baseline is accumulated in `VolumeBaselineStore`, which lives in `_engine.py` as the canonical definition. `inventory.py` imports it from there — there is only one class, not two.

---

## 15. Airflow Orchestration

**Location:** `airflow/dags/`

Three DAGs, one per domain. All DAGs have `schedule=None` (manual trigger only) and `max_active_runs=1` (prevents concurrent runs that would conflict on staging tables).

### Inventory DAG task chain

```
validate_silver → stage_events → refresh_dimensions → load_facts
  → refresh_views → quality_checks → refresh_balance → check_reorders
```

The quality checks step calls `run_inventory_quality_checks()` which raises `RuntimeError` if any check fails — this causes the DAG task to fail and stops the chain, preventing bad data from propagating.

### Why Airflow?

Airflow is used as a scheduler/orchestrator rather than running the Python functions directly. This provides: retry logic, task-level success/failure tracking, dependency enforcement between tasks, and the Airflow UI for historical run inspection.

**Trade-off:** Airflow adds significant infrastructure weight (a PostgreSQL metadata DB, webserver, scheduler, worker). For this local deployment the same pipeline can be run via the CLI (`python -m medwarehouse orchestration build-gold`) or from the monitoring dashboard. Airflow becomes important when running on a schedule or in a shared environment.

---

## 16. Infrastructure and Docker Compose

### Service topology

| Service | Port | Purpose |
|---|---|---|
| `postgres` | 5432 | Both `medwarehouse_master` and `medwarehouse_analytics` databases |
| `zookeeper` | 2181 | Kafka coordination (deprecated in newer Kafka but present in Confluent 7.5) |
| `kafka` | 9092 (external), 29092 (internal) | Event bus |
| `airflow-webserver` | **8090** | Airflow UI (remapped from default 8080 to avoid conflict with webapp) |
| `airflow-scheduler` | — | Airflow scheduling backend |

**Port 8090 for Airflow:** The standard Airflow port is 8080. The operator webapp also runs on 8080. They cannot both bind to the same host port, so Airflow is remapped to 8090 in this deployment.

### Health checks and `depends_on` conditions

PostgreSQL and Kafka both have `healthcheck` blocks. The Airflow services use `condition: service_healthy` and `condition: service_completed_successfully` so they do not start until the dependencies are actually ready (not just started).

This is important because Kafka takes 15–30 seconds to elect a leader after the container starts. Without health checks, `airflow-init` would fail trying to connect to Kafka before it's ready.

### Restart policies

Core services (postgres, kafka, zookeeper, and Airflow application services) have `restart: unless-stopped`. This means they survive host reboots and `docker daemon` restarts without manual intervention.

### Database initialisation

`docker/postgres/init/` contains two SQL files and a shell script that run on first container startup (PostgreSQL's `docker-entrypoint-initdb.d` mechanism):
1. `00-create-databases.sql` — creates `medwarehouse_master`, `medwarehouse_analytics`, and `airflow` databases.
2. `01-create-airflow-user.sql` — creates the Airflow PostgreSQL user.
3. `02-restore-master.sh` — restores `meddata_full_20250916_175921.dump` into `medwarehouse_master`.

The master database restore is the AI-generated sample dataset. It is restored once at container creation and not re-run.

---

## 17. Testing Strategy

### Philosophy

All tests in `tests/` run without a live database, Kafka cluster, or Spark session. Heavy dependencies (`psycopg2`, `confluent_kafka`, `pyspark`) are imported **lazily** — inside function bodies rather than at module level. This means `import medwarehouse.warehouse.inventory` succeeds in a minimal test environment without those packages installed, and tests can mock the DB layer cleanly.

**Critical pattern:** `__init__.py` files throughout the package are deliberately empty (no eager imports). This prevents a test that imports `medwarehouse.warehouse` from pulling in psycopg2 transitively.

### Test files

| File | What it tests |
|---|---|
| `test_cli.py` | CLI entry point smoke test (dry-run inventory producer) |
| `test_config.py` | Settings construction, weak credential detection, API key env vars |
| `test_contracts.py` | Inventory event building and validation rules |
| `test_domain_contracts.py` | Procurement and sales contract validation |
| `test_platform.py` | StatusService, ControlPlaneStore, ControlPlaneService, Flask route layer |
| `test_sql_runner.py` | SQL template rendering |
| `test_warehouse_functions.py` | Warehouse Python wrappers (mocking the DB), credential warnings, auth middleware |
| `test_webapp_services.py` | Stock service (writes + events), order service state machine, reorder service SET clause, IdempotencyStore, TTLCache stampede protection |

### Running tests

```bash
pytest tests/ -q               # all 82 unit tests
pytest tests/ -k "idempotency"  # subset by keyword
MW_INTEGRATION_TEST=1 pytest tests/test_warehouse_functions.py -m integration  # live DB
```

### Mock targets

When mocking the DB layer, mock the `fetch_rows` or `connect` function at the point of use, not at the import site:

```python
# Correct: mock at usage site
patch("medwarehouse.warehouse._common.fetch_rows")

# Wrong: won't intercept calls that imported the name already
patch("medwarehouse.warehouse.inventory.fetch_rows")
```

---

## 18. Key Design Decisions and Trade-offs

### Event sourcing for inventory

**Decision:** Never store a `current_quantity` column that gets updated in place. Instead, derive it from the sum of event deltas.

**Why:** Append-only event logs are auditable, replayable, and correct under concurrent writes. A running balance stored as a column is prone to race conditions under concurrent updates (two SOLD events for the same lot running simultaneously would both read the same balance and both decrement from it, losing one update).

**Trade-off:** Querying current balance requires summing events, which is slower than a point lookup. This is mitigated by `fact_inventory_balance` — a physical snapshot maintained by `refresh_inventory_balance()` that Power BI reads instead of the derived view.

### Dual-database architecture

**Decision:** OLTP data lives in `medwarehouse_master`; analytics live in `medwarehouse_analytics`. They communicate via FDW.

**Why:** Analytical queries (aggregations, window functions, cross-joins) compete with OLTP writes for resources. Separating them onto different schemas/connections means a slow Power BI query cannot lock an inventory_lots row that an operator needs to update.

**Trade-off:** FDW adds latency and a single point of coupling between the two databases. If the master DB is unreachable, the analytics stored procedures that query FDW tables will fail.

### Local Parquet as the intermediate store

**Decision:** Bronze and Silver are local Parquet files, not database tables or Kafka streams.

**Why:** Parquet is self-describing, columnar, and cheap to re-read. Storing as files means the Silver transformation can be re-run any number of times against the same Bronze data without re-reading Kafka. This is important for recovery: if a Silver job has a bug, fix the code, delete the Silver directory, and re-run Silver from Bronze.

**Trade-off:** This architecture works only when Bronze and Silver jobs run on the same machine. In a distributed deployment, a shared filesystem (S3, HDFS) would replace local paths.

### SQLite for the control plane

**Decision:** The control plane store (job run history) uses SQLite rather than PostgreSQL.

**Why:** The monitoring platform is a local developer tool. Adding a dependency on the analytics database would mean the monitoring platform cannot start if the analytics DB is down — defeating its purpose as a tool to diagnose infrastructure problems. SQLite is self-contained, requires no setup, and its WAL mode handles the modest multi-reader/single-writer concurrency this application needs.

### Deterministic event IDs

**Decision:** Sample data event IDs are UUID-v5 seeded from a string (`seed + event_name`). Webapp event IDs are UUID-v5 seeded from `seed + operation + timestamp`.

**Why:** The warehouse uses `ON CONFLICT (event_id) DO NOTHING` to deduplicate. For this to work reliably on retries, the same logical operation must produce the same `event_id`. UUID-v5 is deterministic given the same inputs, so re-submitting a failed request (with an `Idempotency-Key`) will produce an event that the database safely ignores as a duplicate.

### Separate monitoring and operator apps

**Decision:** Two completely independent Flask applications.

**Why:** Their security models differ. The monitoring platform shows sensitive operational data and should be accessible only to engineers; the operator webapp handles data entry and should be accessible to warehouse staff. Running them as separate processes means separate API keys, separate logs, and independent restart/scaling.

---

## 19. Important Nuances and Gotchas

### Product and warehouse IDs are TEXT, not UUID

The OLTP schema uses TEXT columns for `product_id` and `location_id`. Sample values look like `"P-LOCAL-CALPOL-500"`. Do not add `::uuid` PostgreSQL casts in any SQL query — they will fail with `invalid input syntax for type uuid` for non-UUID strings.

### `get_settings.cache_clear()` in tests

The `get_settings()` singleton caches on first call. Any test that patches `os.environ` must call `get_settings.cache_clear()` in both `setUp` and `tearDown`, otherwise patched environment variables bleed through into subsequent tests that assume clean settings.

### Bronze lifecycle in scripts

The Bronze Spark job is a long-running streaming process. Flow scripts manage its lifecycle:
1. Start Bronze in the background (`&`).
2. Set a `trap ... EXIT` to kill it on script exit.
3. Produce events.
4. Wait for Parquet output with `wait_for_parquet` (polling with 90-second timeout).
5. Stop Bronze cleanly before running Silver.

If you abort a flow script mid-run, the EXIT trap handles cleanup. But if the script is killed with SIGKILL (`kill -9`), the trap does not run — you will need to manually kill the Bronze PID.

### Airflow `--if-not-exists` on user create

The `airflow users create` command in `airflow-init` uses `--if-not-exists` (available in Airflow 2.8+, which is the minimum version). This prevents the init container from failing on `docker compose up` re-runs when the user already exists.

### Parquet overwrite in Silver

Silver writes with `mode("overwrite")`. This means running Silver twice on the same Bronze data is idempotent — the second run replaces the first. This is intentional (Silver is derived from Bronze; it is not authoritative and can be regenerated at any time). The warehouse Gold pipeline reads Silver once; after that, Silver can be safely deleted to save disk space.

### `SECURITY DEFINER` on stored procedures

Stored procedures in the analytics schema use `SECURITY DEFINER`, which means they execute with the privileges of their creator (postgres superuser) regardless of which role calls them. This allows `spark_writer` to call `load_inventory_event_facts()` even though `spark_writer` does not have direct INSERT privileges on `fact_inventory_events` (which is owned by postgres). Keep this in mind when debugging permission errors — the effective user inside a `SECURITY DEFINER` function is the definer, not the caller.

### `ProducerSettings` vs `SampleDataSettings`

These were deliberately separated during a refactor. `ProducerSettings` has only three fields: `interval_seconds`, `max_events`, `dry_run` — runtime controls. `SampleDataSettings` has all the seed values used to generate repeatable test data. Do not add sample-data-specific fields to `ProducerSettings`; they belong in `SampleDataSettings`.

### `VolumeBaselineStore` has one canonical location

`VolumeBaselineStore` is defined in `alerts/_engine.py`. `alerts/inventory.py` imports it from there. The class must not be re-defined in `inventory.py` — both modules share the same class so that `isinstance` checks and baseline stores passed between them are consistent.

### Thread safety of the TTLCache

`TTLCache` uses a `threading.Event` per cache key to prevent concurrent builds (thundering herd). The design guarantees exactly one builder per concurrent miss. However, `invalidate()` does not wait for an in-progress build to finish — if a force-refresh races with a cache write, the freshly built value might be evicted immediately. This is acceptable for a monitoring dashboard (the next request will just rebuild), but it would be problematic for a high-write-rate cache.

---

## 20. Common Developer Workflows

### First-time setup

```bash
cd Event-Driven-Inventory-Analytics-Warehouse-BI-System
cp .env.example .env     # review and adjust if needed
./scripts/setup_local_stack.sh
```

This script checks prerequisites, creates a venv, starts Docker services, and bootstraps the warehouse schema.

### Run the inventory pipeline end-to-end

```bash
./scripts/run_inventory_flow.sh
```

This handles: Kafka topic creation → Bronze start → event production → wait for Parquet → Bronze stop → Silver → Stage → Gold (dimensions + facts + views + quality checks + balance refresh + reorder check).

### Start the monitoring dashboard

```bash
source .venv/bin/activate
python -m medwarehouse platform serve --port 8787
```

Open `http://localhost:8787`.

### Start the operator webapp

```bash
source .venv/bin/activate
python -m medwarehouse_webapp --port 8080
```

### Run tests

```bash
source .venv/bin/activate
pip install -e ".[dev]"
pytest tests/ -q
```

### Bootstrap or re-bootstrap the warehouse schema

```bash
python -m medwarehouse warehouse bootstrap
```

Runs all SQL files in `sql/warehouse/` in order. Safe to re-run (`CREATE OR REPLACE` and `IF NOT EXISTS` are used throughout).

### Inspect quarantined records

```bash
python -c "
import pandas as pd
df = pd.read_parquet('data/quarantine/inventory_events/')
print(df[['event_id','event_type','validation_errors']].head(20))
"
```

### Trigger the Gold pipeline manually (without Airflow)

```bash
python -m medwarehouse orchestration build-gold
```

---

## 21. Extending the System: Adding a New Domain

To add a new domain (e.g., `returns`):

1. **Define the event contract** — Add `src/medwarehouse/contracts/returns.py` following the same pattern as `inventory.py`: define event types, required fields, build function, and validate function.

2. **Add a Kafka topic** — Add `MW_KAFKA_RETURNS_TOPIC=returns_events` to `.env.example` and `config.py` (`KafkaSettings`).

3. **Create Spark jobs** — Add `returns_bronze.py`, `returns_silver.py`, `returns_stage.py` in `src/medwarehouse/spark/jobs/`, each delegating to the shared `_bronze.py`, `_silver.py`, `_stage.py` utilities with domain-specific schema and validation.

4. **Add path settings** — Add `bronze_returns_path`, `silver_returns_path`, etc. to `PathSettings` in `config.py`.

5. **Create a producer** — Add `src/medwarehouse/producers/returns.py` using `run_producer()` from `common.py`.

6. **Write SQL** — Add `11_returns_warehouse.sql` with staging table, fact table, stored procedures, and semantic views.

7. **Add warehouse Python wrappers** — Add `src/medwarehouse/warehouse/returns.py` with `validate_returns_silver_ready()`, `load_returns_event_facts()`, etc.

8. **Register CLI commands** — In `cli.py`, add entries to the `spark`, `warehouse`, and `orchestration` subparsers, and update `_warehouse_dispatch`.

9. **Create an Airflow DAG** — Add `airflow/dags/returns_gold_pipeline.py` following the same task chain as the inventory DAG.

10. **Register jobs in the catalog** — Add `JobSpec` entries to `platform/control/catalog.py`.

11. **Update probes** — Add the returns paths to `ArtifactProbe` and `PipelineProbe`.

12. **Add freshness alert rules** — Register a `FreshnessAlertRule` for returns bronze and silver in `build_default_alert_engine()`.

13. **Add flow script** — Create `scripts/run_returns_flow.sh` sourcing `lib/common.sh`.

14. **Write tests** — Cover the contract validation, the warehouse wrapper, and at minimum a smoke test for the producer.

The pattern is deliberately consistent across domains so that adding a new one is mechanical, not creative.
