# Functional Requirements Checklist

Assessment date: 2026-06-06

---

## Status legend

- **Complete** — implemented and tested
- **Partial** — implemented structurally; integration tests against a live DB are not yet automated
- **Not started** — no meaningful implementation

---

## Summary

| Domain | Pipeline | Analytics Views | Alerts | Tests |
|---|---|---|---|---|
| Inventory | Complete | Complete | Complete | Unit tests pass |
| Procurement | Complete | Complete | Freshness alerts | Unit tests pass |
| Sales | Complete | Complete | Freshness alerts | Unit tests pass |
| Prescriptions | FDW views | Complete | Compliance alert | — |
| Supplier | FDW views | Complete | License expiry alert | — |
| Financial | FDW views | Complete | — | — |
| Customer | FDW views | Complete | — | — |
| Audit/Compliance | FDW views | Complete | — | — |
| Staff Performance | FDW views | Complete | — | — |

Automated test count: **82 unit tests passing** across 8 test files. Integration tests (requiring a live PostgreSQL connection) are not yet automated.

---

## Functional Checklist

| Area | Requirement | Status | Notes |
|---|---|---|---|
| Platform foundation | Single CLI entrypoint for all operations | Complete | `python -m medwarehouse <group> <cmd>` |
| Configuration | All settings from MW_* env vars | Complete | `config.py`, weak credential detection in non-local envs |
| Security | API key auth on both Flask services | Complete | `platform/auth.py`, MW_WEBAPP_API_KEY + MW_PLATFORM_API_KEY |
| Inventory contracts | Event schema + validation | Complete | `contracts/inventory.py`, shared utils in `contracts/_utils.py` |
| Procurement contracts | Event schema + validation (4 types) | Complete | `contracts/procurement.py` |
| Sales contracts | Event schema + validation (2 types) | Complete | `contracts/sales.py` |
| Inventory producer | Deterministic sample events to Kafka | Complete | All 4 event types |
| Procurement producer | Full lifecycle events to Kafka | Complete | All 4 event types: CREATED→APPROVED→RECEIVED, with cancellations |
| Sales producer | Sales + cancellation events to Kafka | Complete | Both types, all 3 customer types, realistic price tiers |
| Bronze ingestion | Kafka → Parquet (all 3 domains) | Complete | Shared `_bronze.py` utility |
| Silver transformation | Validate, deduplicate, quarantine (all 3 domains) | Complete | Shared `_silver.py` deduplication logic |
| Warehouse staging | Silver → PostgreSQL staging (all 3 domains) | Complete | Shared `_stage.py` JDBC writer |
| Warehouse bootstrap | Schemas, roles, FDW, dims, facts, views, grants | Complete | `sql/warehouse/01-10_*.sql` applied in order |
| FDW integration | Master DB read-only via FDW (16 tables) | Complete | User mappings for all 4 roles |
| Dimensions | SCD Type-2 product, supplier, warehouse, customer | Complete | `sql/warehouse/03_inventory_dimensions.sql`, `09_sales_warehouse.sql` |
| Inventory fact | Append-only partitioned fact table | Complete | `fact_inventory_events` |
| Procurement fact | Append-only partitioned fact table | Complete | `fact_procurement_events` |
| Sales fact | Append-only partitioned fact table with line_revenue | Complete | `fact_sales_events` |
| Inventory semantic views | v_inventory_balance, v_inventory_snapshot | Complete | |
| Procurement semantic views | v_po_lifecycle, v_supplier_performance, v_po_aging | Complete | |
| Sales semantic views | v_revenue_summary, v_sales_velocity, v_returns_analysis | Complete | |
| Reorder automation | Snapshot table + policy-based PO creation | Complete | `fact_inventory_balance`, `reorder_policies`, `check_reorder_thresholds()` |
| Purchase order management | DRAFT → APPROVED → SENT → RECEIVED lifecycle | Complete | `pending_purchase_orders` + webapp API |
| Prescription compliance views | v_controlled_substance_register, violations | Complete | FDW-backed |
| Supplier license monitoring | v_supplier_license_expiry, CRITICAL/WARNING/EXPIRED | Complete | FDW-backed + alert rule |
| Financial analytics | v_accounts_payable, v_daily_collections | Complete | FDW-backed |
| Customer analytics | v_customer_summary | Complete | FDW-backed |
| Audit analytics | v_audit_activity, v_recalled_product_sales | Complete | FDW-backed |
| Staff performance | v_staff_performance, v_inactive_user_accounts | Complete | FDW-backed |
| Quality checks | Data integrity assertions (all 3 domains) | Complete | 7 + 4 + 5 checks across inventory/procurement/sales |
| Inventory Airflow DAG | 8-task orchestrated pipeline | Complete | `inventory_gold_pipeline.py` |
| Procurement Airflow DAG | 6-task orchestrated pipeline | Complete | `procurement_gold_pipeline.py` |
| Sales Airflow DAG | 6-task orchestrated pipeline | Complete | `sales_gold_pipeline.py` |
| Monitoring platform | Status dashboard, job runner, alerts UI | Complete | `platform/` at port 8787 |
| Monitoring: procurement/sales probes | Pipeline freshness + artifact tracking | Complete | `PipelineProbe`, `ArtifactProbe` cover all 3 domains |
| Alert engine | Freshness, volume, compliance, infrastructure alerts | Complete | 14 alert rules across inventory, procurement, sales, compliance |
| Operator webapp REST API | Stock entry, inventory lookup, reorder, PO management | Complete | `medwarehouse_webapp/` at port 8080 |
| Operator webapp frontend | HTML/CSS/JS UI | **Not started** | REST API is complete; frontend to be built separately |
| Shell scripts | Local flow scripts for all 3 domains | Complete | `scripts/run_*_flow.sh` |
| Documentation | architecture, data-dict, API reference, deployment, security, 9 domain docs | Complete | `docs/` |
| Unit tests | Contract validation, config, platform, auth, warehouse functions, webapp services | Complete | 82 tests across 8 files |
| Integration tests | Live DB stored procedure tests | **Not started** | Requires `MW_INTEGRATION_TEST=1` and live PostgreSQL |
| CI/CD | Automated test pipeline | **Not started** | No GitHub Actions configured |

---

## Remaining Work

1. **Operator webapp frontend** — HTML/CSS/JS for the 6 operator pages. REST API is complete and documented in `docs/api-reference.md`.
2. **Integration tests** — Tests that exercise stored procedures against a live PostgreSQL instance. Framework is in place (`MW_INTEGRATION_TEST=1`).
3. **CI/CD** — GitHub Actions or equivalent to run tests on every push.
4. **Power BI connection** — Step-by-step guide for connecting Power BI to `analytics_reader` and importing recommended views.
