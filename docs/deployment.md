# Deployment Guide

This guide covers deploying the full medwarehouse stack locally and to a shared server.

---

## Prerequisites

| Requirement | Minimum Version | Notes |
|---|---|---|
| Docker | 24.x | Required for all infrastructure services |
| Docker Compose | 2.x | Included with Docker Desktop |
| Python | 3.11 | 3.12 and 3.13 supported |
| Java JRE | 17 | Required by PySpark. Install `openjdk-17-jre-headless` |
| Git | 2.x | |

**Java installation (Ubuntu/Debian):**
```bash
sudo apt-get install -y openjdk-17-jre-headless
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

---

## Environment Configuration

Copy `.env.example` to `.env` and configure all values:

```bash
cp .env.example .env
```

**Mandatory settings to change before any shared deployment:**

| Variable | Description | How to generate |
|---|---|---|
| `MW_MASTER_DB_PASSWORD` | Master DB password | `openssl rand -hex 16` |
| `MW_ANALYTICS_ADMIN_DB_PASSWORD` | Analytics admin password | `openssl rand -hex 16` |
| `MW_ANALYTICS_WRITER_DB_PASSWORD` | Spark writer password | `openssl rand -hex 16` |
| `MW_ANALYTICS_READER_DB_PASSWORD` | BI reader password | `openssl rand -hex 16` |
| `MW_ANALYTICS_FDW_READER_PASSWORD` | FDW password | `openssl rand -hex 16` |
| `MW_WEBAPP_API_KEY` | Webapp auth key | `openssl rand -hex 32` |
| `MW_PLATFORM_API_KEY` | Monitoring platform auth key | `openssl rand -hex 32` |

> **Security note:** The system raises a `RuntimeError` at startup if any password matches a known weak default in any environment except `local` or `dev`. Set `MW_ENV` to a value other than `local` / `dev` in all shared deployments.

---

## Infrastructure Startup

### Start core services (PostgreSQL + Kafka)

```bash
./scripts/start_services.sh
```

This script:
1. Starts PostgreSQL, Zookeeper, and Kafka via Docker Compose
2. Waits for Kafka to be ready
3. Creates the three Kafka topics: `inventory_events`, `procurement_events`, `sales_events`

### Start with Airflow

```bash
./scripts/start_services.sh --with-airflow
```

Airflow initialises its database, creates an admin user (credentials set via `AIRFLOW_ADMIN_PASSWORD` in `.env`, defaulting to `admin`), and starts the webserver and scheduler. The Airflow webserver binds to host port **8090** (not the default 8080) to avoid conflict with the operator webapp on port 8080.

### Stop all services

```bash
./scripts/stop_services.sh
```

---

## Python Environment Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt       # Full lockfile (for Airflow container parity)
# OR for local dev:
pip install -e ".[spark,dev]"         # Uses pyproject.toml
```

Set `PYTHONPATH` so the medwarehouse package is discoverable:

```bash
export PYTHONPATH="$PWD/src"
```

Or source the `.env` file (it does not set PYTHONPATH; the scripts set it automatically):

```bash
set -a && source .env && set +a
```

---

## Warehouse Bootstrap

Run once after starting PostgreSQL to create all schemas, roles, FDW, dimensions, facts, views, and functions:

```bash
python -m medwarehouse warehouse bootstrap
```

This executes all SQL files in `sql/warehouse/` in lexicographic order (01 → 10).

> **Important:** The bootstrap SQL uses template substitution for role names and passwords. Ensure all `MW_*` environment variables are set before running bootstrap.

---

## Running the Inventory Pipeline (Step-by-Step)

```bash
# 1. Produce sample inventory events to Kafka
python -m medwarehouse produce inventory --max-events 20

# 2. In a separate terminal — start the Bronze stream (runs continuously)
python -m medwarehouse spark inventory-bronze

# 3. After events land in Bronze, run Silver transformation
python -m medwarehouse spark inventory-silver

# 4. Build Gold (stage → load facts → refresh views → quality checks → balance → reorder check)
python -m medwarehouse orchestration build-gold
```

Or use the convenience script:

```bash
./scripts/run_inventory_flow.sh 20    # produces 20 events and runs the full pipeline
```

---

## Running the Procurement Pipeline

```bash
python -m medwarehouse produce procurement --max-events 15
python -m medwarehouse spark procurement-bronze   # separate terminal, long-running
python -m medwarehouse spark procurement-silver
python -m medwarehouse orchestration build-procurement-gold
```

## Running the Sales Pipeline

```bash
python -m medwarehouse produce sales --max-events 20
python -m medwarehouse spark sales-bronze          # separate terminal, long-running
python -m medwarehouse spark sales-silver
python -m medwarehouse orchestration build-sales-gold
```

---

## Starting the Operator Webapp

```bash
python -m medwarehouse_webapp --host 0.0.0.0 --port 8080
```

If `MW_WEBAPP_API_KEY` is set in `.env`, all API calls require:
```
X-API-Key: <your key>
```
or
```
Authorization: Bearer <your key>
```

The `/health` endpoint is always accessible without authentication.

---

## Starting the Monitoring Platform

```bash
python -m medwarehouse platform serve --host 0.0.0.0 --port 8787
```

Accessible at `http://localhost:8787`. All pages and API endpoints require `MW_PLATFORM_API_KEY` if set.

---

## Running via Airflow

Three DAGs are available after Airflow starts:

| DAG ID | Trigger | Description |
|---|---|---|
| `inventory_gold_pipeline` | Manual | Full inventory Gold pipeline (8 tasks, includes balance refresh + reorder check) |
| `procurement_gold_pipeline` | Manual | Full procurement Gold pipeline (6 tasks) |
| `sales_gold_pipeline` | Manual | Full sales Gold pipeline (6 tasks) |

Access Airflow UI at **`http://localhost:8090`** (credentials: `admin` / value of `AIRFLOW_ADMIN_PASSWORD`, default `admin`).

---

## Port Reference

| Service | Port | Notes |
|---|---|---|
| PostgreSQL | 5432 | Hosts both `medwarehouse_master` and `medwarehouse_analytics` |
| Kafka | 9092 | External listener (localhost) |
| Kafka internal | 29092 | Docker network listener |
| ZooKeeper | 2181 | Kafka coordination |
| Airflow webserver | **8090** | DAG management UI (remapped from default 8080 — see note below) |
| Monitoring platform | 8787 | Operational monitoring dashboard |
| Operator webapp | 8080 | Data entry and PO management |

> **Port note:** The Airflow webserver is mapped to host port **8090** in `docker-compose.yml` to avoid conflict with the operator webapp, which runs on port 8080. Do not change the operator webapp to 8090 unless you also change the Airflow mapping.

---

## Environment Variables Reference

See `.env.example` for the complete annotated list. Key variables:

| Variable | Default | Description |
|---|---|---|
| `MW_ENV` | `local` | Environment name. Non-local environments enforce strong credentials. |
| `MW_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `MW_MASTER_DB_*` | localhost/5432/postgres | Master database connection |
| `MW_ANALYTICS_ADMIN_DB_*` | localhost/5432/postgres | Analytics admin connection |
| `MW_ANALYTICS_WRITER_DB_*` | — | Spark writer connection |
| `MW_ANALYTICS_READER_DB_*` | — | BI reader connection |
| `MW_WEBAPP_API_KEY` | (empty) | Webapp auth key. Empty = no auth (dev only). |
| `MW_PLATFORM_API_KEY` | (empty) | Platform auth key. Empty = no auth (dev only). |
| `MW_SMTP_HOST` | (empty) | SMTP server for supplier emails. Empty = log only. |
| `JAVA_HOME` | — | Required for PySpark. Must point to Java 17 installation. |

---

## Upgrading

1. Pull the latest code
2. Run `pip install -r requirements.txt` to pick up any new Python dependencies
3. Run `python -m medwarehouse warehouse bootstrap` to apply new SQL files
4. Restart all running services

---

## Health Checks

```bash
# Platform health
curl http://localhost:8787/health

# Webapp health
curl http://localhost:8080/health

# Warehouse quality checks
python -m medwarehouse warehouse quality-checks
python -m medwarehouse warehouse procurement-quality-checks
python -m medwarehouse warehouse sales-quality-checks

# Pipeline status
curl http://localhost:8787/api/status/pipeline
```
