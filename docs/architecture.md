# Architecture

## System Overview

MedWarehouse is an event-driven analytical warehouse for a medical/pharmaceutical operation. Business events produced by an operator webapp or sample producers flow through a three-stage Spark pipeline (Bronze → Silver → Gold) into a PostgreSQL analytical warehouse. Nine analytical domains are served to Power BI and to the operator webapp via a REST API.

---

## Layer Map

```
┌──────────────────────────────────────────────────────────────────┐
│  Operator Webapp (port 8080)                                     │
│  Stock entry, reorder policies, purchase order management        │
└────────────────────┬─────────────────────────────────────────────┘
                     │ writes to master DB + emits Kafka events
                     │ triggers pipeline via POST /api/jobs/build_*/start
                     ▼
┌──────────────────────────────────────────────────────────────────┐
│  medwarehouse_master (PostgreSQL OLTP)                           │
│  products, suppliers, warehouse_locations, inventory_lots,       │
│  stock_adjustments, purchase_orders, goods_receipts, sales,      │
│  prescriptions, customers, users, payments, audit_logs           │
└──────────┬───────────────────────────────────────────────────────┘
           │ FDW (read-only) → master schema in analytics DB
           │
┌──────────▼───────────────────────────────────────────────────────┐
│  Kafka Topics                                                    │
│  inventory_events  •  procurement_events  •  sales_events       │
└──────────┬───────────────────────────────────────────────────────┘
           │ PySpark Structured Streaming
           ▼
┌──────────────────────────────────────────────────────────────────┐
│  Bronze Layer (Parquet, immutable, append-only)                  │
│  data/bronze/{inventory,procurement,sales}_events/               │
└──────────┬───────────────────────────────────────────────────────┘
           │ PySpark batch (validate, deduplicate, quarantine)
           ▼
┌──────────────────────────────────────────────────────────────────┐
│  Silver Layer (Parquet, validated + deduped events)              │
│  data/silver/{inventory,procurement,sales}_events/               │
│  data/quarantine/{inventory,procurement,sales}_events/           │
└──────────┬───────────────────────────────────────────────────────┘
           │ PySpark JDBC write (truncate-and-load)
           ▼
┌──────────────────────────────────────────────────────────────────┐
│  medwarehouse_analytics (PostgreSQL)                             │
│                                                                  │
│  Staging: stg_inventory_events / stg_procurement_events /        │
│           stg_sales_events                                       │
│                                                                  │
│  Dimensions (SCD Type 2):                                        │
│    dim_product  dim_supplier  dim_warehouse  dim_customer        │
│                                                                  │
│  Facts (partitioned by event_time):                              │
│    fact_inventory_events   fact_procurement_events               │
│    fact_sales_events                                             │
│                                                                  │
│  Operational: fact_inventory_balance  reorder_policies           │
│               pending_purchase_orders                            │
│                                                                  │
│  Semantic views (inventory, procurement, sales, compliance ...)  │
│  See data-dictionary.md for full view listing.                   │
└──────────┬───────────────────────────────────────────────────────┘
           │ analytics_reader role
           ▼
┌──────────────────────────────────────────────────────────────────┐
│  Power BI  /  Monitoring Platform (port 8787)                    │
└──────────────────────────────────────────────────────────────────┘
```

---

## Kafka Domain Pipelines

Three complete Bronze → Silver → Gold pipelines, each independently orchestrated:

| Domain | Topic | Event Types | Fact Table |
|---|---|---|---|
| Inventory | `inventory_events` | STOCK_RECEIVED, STOCK_SOLD, STOCK_ADJUSTED, STOCK_EXPIRED | `fact_inventory_events` |
| Procurement | `procurement_events` | PO_CREATED, PO_APPROVED, PO_RECEIVED, PO_CANCELLED | `fact_procurement_events` |
| Sales | `sales_events` | SALE_CREATED, SALE_CANCELLED | `fact_sales_events` |

Each domain has: a Kafka schema, a Python contract, a Kafka producer, three Spark jobs (Bronze/Silver/Stage), a warehouse module, and an Airflow DAG.

---

## Analytical Domains (FDW-backed)

Six additional domains read directly from the OLTP database via PostgreSQL FDW. No Spark pipeline.

| Domain | Key Views |
|---|---|
| Prescriptions / Compliance | v_controlled_substance_register, v_prescription_compliance_violations |
| Supplier Management | v_supplier_license_expiry, v_supplier_directory |
| Financial / AP-AR | v_accounts_payable, v_daily_collections |
| Customer Analytics | v_customer_summary |
| Audit / Compliance | v_audit_activity, v_recalled_product_sales |
| Staff Performance | v_staff_performance, v_inactive_user_accounts |

---

## Security Model

Four PostgreSQL roles enforcing least-privilege. API key auth (`X-API-Key` header) on both Flask services. See `docs/security.md` for full details.

---

## Package Structure

```
src/medwarehouse/
├── contracts/_utils.py           # Shared: deterministic_uuid, to_utc_iso
├── producers/common.py           # Shared: emit_events(), run_producer()
├── spark/jobs/_bronze.py         # Shared: run_bronze_ingestion()
│              _silver.py         # Shared: split_silver_quarantine()
│              _stage.py          # Shared: write_to_staging()
└── warehouse/_common.py          # Shared: validate_silver_ready(), call_warehouse_function()

src/medwarehouse_webapp/           # Flask operator webapp (port 8080)
```

---

## Orchestration

Three Airflow DAGs (`schedule=None`). All can also be run via CLI or the monitoring platform job runner.

**inventory_gold_pipeline** (8 tasks): validate → stage → dims → facts → views → quality → balance → reorders

**procurement_gold_pipeline** (6 tasks): validate → stage → dims → facts → views → quality

**sales_gold_pipeline** (6 tasks): validate → stage → dims → facts → views → quality
