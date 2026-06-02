# Runbook

Operational guide for running, monitoring, and recovering all three domain pipelines.

---

## Quick Start (all three pipelines)

```bash
# 1. Configure environment
cp .env.example .env && $EDITOR .env

# 2. Start infrastructure
./scripts/start_services.sh

# 3. Bootstrap analytics warehouse (run once)
export PYTHONPATH=src
python -m medwarehouse warehouse bootstrap

# 4. Start monitoring platform
python -m medwarehouse platform serve --host 0.0.0.0 --port 8787

# 5. Start operator webapp
python -m medwarehouse_webapp --host 0.0.0.0 --port 8080
```

---

## Local Environment

Set environment variables by sourcing `.env`:
```bash
set -a && source .env && set +a
export PYTHONPATH=src
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

---

## Inventory Pipeline

```bash
# Produce sample events
python -m medwarehouse produce inventory --max-events 20

# Start Bronze stream (long-running, separate terminal)
python -m medwarehouse spark inventory-bronze

# After events land in Bronze:
python -m medwarehouse spark inventory-silver
python -m medwarehouse orchestration build-gold

# Or use the convenience script:
./scripts/run_inventory_flow.sh 20
```

**build-gold** runs: validate-silver → stage-inventory-events → refresh-dimensions → load-facts → refresh-views → quality-checks → refresh-balance → check-reorders

---

## Procurement Pipeline

```bash
python -m medwarehouse produce procurement --max-events 15

# Start Bronze stream (separate terminal)
python -m medwarehouse spark procurement-bronze

python -m medwarehouse spark procurement-silver
python -m medwarehouse orchestration build-procurement-gold

# Or use:
./scripts/run_procurement_flow.sh 15
```

**build-procurement-gold** runs: validate-silver → stage-procurement-events → refresh-dimensions → load-facts → refresh-views → quality-checks

---

## Sales Pipeline

```bash
python -m medwarehouse produce sales --max-events 20

# Start Bronze stream (separate terminal)
python -m medwarehouse spark sales-bronze

python -m medwarehouse spark sales-silver
python -m medwarehouse orchestration build-sales-gold

# Or use:
./scripts/run_sales_flow.sh 20
```

---

## Airflow

Three DAGs are available (`schedule=None` — trigger manually):

```
http://localhost:8080
DAGs: inventory_gold_pipeline | procurement_gold_pipeline | sales_gold_pipeline
Default credentials: admin / admin (change in production)
```

---

## Bootstrap Replay

If the warehouse schema is corrupted or needs to be rebuilt:

```bash
# Drop and recreate analytics schema (DESTRUCTIVE — all analytics data lost)
psql -U postgres -d medwarehouse_analytics -c "DROP SCHEMA analytics CASCADE; CREATE SCHEMA analytics;"
python -m medwarehouse warehouse bootstrap
```

---

## Quality Checks

```bash
python -m medwarehouse warehouse quality-checks
python -m medwarehouse warehouse procurement-quality-checks
python -m medwarehouse warehouse sales-quality-checks
```

Each command raises on any failing assertion and exits non-zero.

---

## Reorder Automation

```bash
# Refresh the physical inventory snapshot
python -m medwarehouse warehouse refresh-balance

# Check stock against reorder policies → creates DRAFT purchase orders
python -m medwarehouse warehouse check-reorders

# View draft orders in the webapp or query directly:
# SELECT * FROM analytics.v_purchase_order_status WHERE status = 'DRAFT';
```

---

## Silver Quarantine Investigation

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").getOrCreate()

# Inventory
df = spark.read.parquet("data/quarantine/inventory_events/")
df.select("event_type", "quarantine_reasons").show(truncate=False)

# Procurement
df = spark.read.parquet("data/quarantine/procurement_events/")
df.select("event_type", "quarantine_reasons").show(truncate=False)
```

---

## Recovery: Bronze Stream Stuck

1. Kill the stuck streaming job via `POST /api/jobs/inventory_bronze/stop` or Ctrl+C
2. Delete checkpoints: `rm -rf data/checkpoints/inventory_events/`
3. Restart Bronze: `python -m medwarehouse spark inventory-bronze --starting-offsets latest`

---

## Infrastructure

```bash
./scripts/start_services.sh            # Start PostgreSQL + Kafka
./scripts/start_services.sh --with-airflow  # Include Airflow
./scripts/stop_services.sh             # Stop all services
```

Health check:
```bash
curl http://localhost:8787/health   # Monitoring platform
curl http://localhost:8080/health   # Operator webapp
```

---

## Compliance Checks

```sql
-- Controlled substance violations (schedule H1/X/NDPS dispensed without prescription)
SELECT * FROM analytics.v_prescription_compliance_violations;

-- Supplier drug licenses expiring soon
SELECT * FROM analytics.v_supplier_license_expiry WHERE license_status IN ('EXPIRED','CRITICAL','WARNING');

-- Sales of recalled/banned products
SELECT * FROM analytics.v_recalled_product_sales;
```
