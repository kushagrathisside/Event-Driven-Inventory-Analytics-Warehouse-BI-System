# Development Guide

Everything needed to set up a local development environment, run tests, and understand the codebase structure.

---

## Repository Structure

```
medwarehouse/
├── src/
│   ├── medwarehouse/               # Main Python package
│   │   ├── config.py               # All settings loaded from environment
│   │   ├── logging.py              # Logging setup
│   │   ├── cli.py                  # CLI entrypoint: python -m medwarehouse <command>
│   │   ├── contracts/              # Event schemas and validation
│   │   │   ├── inventory.py        # STOCK_* event contract
│   │   │   ├── procurement.py      # PO_* event contract
│   │   │   └── sales.py            # SALE_* event contract
│   │   ├── producers/              # Kafka event producers
│   │   │   ├── common.py           # emit_events() + Kafka producer
│   │   │   ├── inventory.py
│   │   │   ├── procurement.py
│   │   │   └── sales.py
│   │   ├── spark/
│   │   │   ├── session.py          # build_spark_session()
│   │   │   └── jobs/
│   │   │       ├── inventory_bronze.py    # Kafka → Bronze Parquet
│   │   │       ├── inventory_silver.py    # Bronze → Silver Parquet
│   │   │       ├── inventory_stage.py     # Silver → stg_inventory_events
│   │   │       ├── procurement_bronze.py
│   │   │       ├── procurement_silver.py
│   │   │       ├── procurement_stage.py
│   │   │       ├── sales_bronze.py
│   │   │       ├── sales_silver.py
│   │   │       └── sales_stage.py
│   │   ├── warehouse/
│   │   │   ├── postgres.py         # psycopg2 connection context manager
│   │   │   ├── sql_runner.py       # execute_sql_file, fetch_rows, execute_statement
│   │   │   ├── inventory.py        # Inventory warehouse functions
│   │   │   ├── procurement.py      # Procurement warehouse functions
│   │   │   └── sales.py            # Sales warehouse functions
│   │   └── platform/               # Flask monitoring app (port 8787)
│   │       ├── api/                # Flask blueprints
│   │       ├── auth.py             # API key middleware
│   │       ├── control/            # Job runner (SQLite-backed)
│   │       ├── models/             # Dataclasses
│   │       ├── probes/             # Data collection probes
│   │       ├── services/           # StatusService, AlertEngine
│   │       └── ui/                 # Jinja2 templates + static files
│   └── medwarehouse_webapp/        # Operator webapp (port 8080)
│       ├── app.py                  # Flask factory
│       ├── db.py                   # DB connection helpers
│       ├── api/                    # REST API blueprints
│       └── services/               # Business logic services
├── sql/warehouse/                  # SQL bootstrap files (run in order)
│   ├── 01_roles_and_schemas.sql
│   ├── 02_fdw_master_access.sql
│   ├── 03_inventory_dimensions.sql
│   ├── 04_inventory_fact_and_staging.sql
│   ├── 05_inventory_loads_and_views.sql
│   ├── 06_reorder_and_balance.sql
│   ├── 07_reorder_functions.sql
│   ├── 08_procurement_warehouse.sql
│   ├── 09_sales_warehouse.sql
│   └── 10_analytical_domains.sql
├── schemas/kafka/                  # Kafka topic schemas (documentation)
│   ├── inventory_events.json
│   ├── procurement_events.json
│   └── sales_events.json
├── airflow/dags/                   # Airflow DAG definitions
│   ├── inventory_gold_pipeline.py
│   ├── procurement_gold_pipeline.py
│   └── sales_gold_pipeline.py
├── tests/                          # Test suite
├── docs/                           # Documentation
├── jars/                           # PostgreSQL JDBC driver
├── scripts/                        # Shell scripts for local dev
├── pyproject.toml                  # Package definition + optional dependencies
├── requirements.txt                # Full lockfile for Docker/Airflow
└── .env.example                    # Environment template
```

---

## Quick Setup

```bash
# 1. Clone the repository
git clone <repo-url>
cd Event-Driven-Inventory-Analytics-Warehouse-BI-System

# 2. Create virtual environment
python -m venv .venv
source .venv/bin/activate

# 3. Install core dependencies
pip install flask sqlparse psycopg2-binary confluent-kafka pyspark pytest

# 4. Install in editable mode (optional, for IDE support)
pip install -e ".[spark,dev]"

# 5. Set PYTHONPATH
export PYTHONPATH="$PWD/src"

# 6. Run tests (no infrastructure needed)
pytest tests/
```

---

## Running Tests

The test suite is split into tiers:

### Unit tests (no infrastructure required)
```bash
pytest tests/ -v
```

These tests cover:
- Event contract validation (`test_contracts.py`, `test_domain_contracts.py`)
- Configuration loading and credential hardening (`test_config.py`, `test_warehouse_functions.py`)
- SQL template rendering (`test_sql_runner.py`)
- Warehouse function call patterns (`test_warehouse_functions.py`)
- Platform UI and API routes (`test_platform.py`)
- CLI argument parsing (`test_cli.py`)
- API key authentication (`test_warehouse_functions.py::TestWebappAuth`)

### Integration tests (requires live PostgreSQL)
```bash
MW_INTEGRATION_TEST=1 pytest tests/ -m integration -v
```

These tests connect to the actual database and verify stored procedure behaviour.

---

## Key Design Patterns

### Configuration
All settings come from environment variables via `src/medwarehouse/config.py`. The `get_settings()` function is `@lru_cache` — it reads env vars once per process and is cleared in tests via `get_settings.cache_clear()`. Weak credentials log a warning in `local`/`dev` environments and raise `RuntimeError` in all others.

### CLI command structure
```
python -m medwarehouse <group> <subcommand> [options]

Groups: produce | spark | warehouse | orchestration | platform
```

Heavy imports (pyspark, psycopg2, confluent_kafka) are deferred to inside command handlers, so `import medwarehouse.cli` succeeds without those packages installed.

### Event contract pattern
Each domain has a `contracts/<domain>.py` file that defines:
- `build_<domain>_event(...)` — constructs a valid event dict
- `validate_<domain>_event_dict(event)` — returns a list of error strings
- `generate_<domain>_sample_events(...)` — deterministic sample data for testing

The Silver Spark job enforces the same rules as the contract validator, applied at scale.

### Adding a new domain
1. Define the Kafka schema in `schemas/kafka/<domain>_events.json`
2. Create `src/medwarehouse/contracts/<domain>.py`
3. Update `src/medwarehouse/producers/<domain>.py`
4. Create Bronze/Silver/Stage Spark jobs following the existing pattern
5. Create `sql/warehouse/NN_<domain>_warehouse.sql`
6. Create `src/medwarehouse/warehouse/<domain>.py`
7. Add new paths to `PathSettings` in `config.py`
8. Add CLI subcommands to `cli.py`
9. Add `JobSpec` entries to `platform/control/catalog.py`
10. Create `airflow/dags/<domain>_gold_pipeline.py`
11. Add tests to `tests/test_domain_contracts.py`

### Database access
- All DB access goes through `medwarehouse.warehouse.postgres.connect(connection)` — a context manager that handles rollback on exception and always closes the connection.
- SQL template substitution uses `render_sql_template(template, context)` which replaces `{{KEY}}` placeholders.
- Multi-statement SQL files are split using `sqlparse.split()` before execution.

### API key authentication
Both Flask apps use `medwarehouse.platform.auth.register_api_key_auth(app, api_key, service=...)`. The `/health` endpoint and `/static/*` paths are always exempt. Keys are read from `MW_WEBAPP_API_KEY` and `MW_PLATFORM_API_KEY` environment variables.

---

## Common Development Tasks

### Add a new warehouse SQL object
Add SQL to the appropriate `sql/warehouse/NN_*.sql` file, then re-run `python -m medwarehouse warehouse bootstrap` to apply.

### Add a new alert rule
1. Create a class in `src/medwarehouse/platform/services/alerts.py` inheriting `AlertRule`
2. Implement `evaluate(self, metrics: dict) -> Alert | None`
3. Add an instance to the `rules` list in `build_default_alert_engine()`

### Add a new monitoring probe
1. Create a class in `src/medwarehouse/platform/probes/` inheriting `Probe`
2. Implement `_collect(self) -> dict`
3. Register it in `StatusService.__init__._probes` in `services/status.py`

### Debug a Silver quarantine
Check the quarantine Parquet:
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").getOrCreate()
df = spark.read.parquet("data/quarantine/inventory_events/")
df.select("event_type", "quarantine_reasons").show(truncate=False)
```

### Reset the warehouse
```bash
# Drop and recreate analytics schema
psql -U postgres -d medwarehouse_analytics -c "DROP SCHEMA analytics CASCADE; CREATE SCHEMA analytics;"
python -m medwarehouse warehouse bootstrap
```
