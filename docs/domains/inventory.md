# Domain: Inventory Management

## Purpose
Track all physical stock movements for every product, warehouse, and batch in the system. This is the system of record for current stock levels and the full movement history.

## Status
**Fully implemented** — end-to-end from Kafka to Power BI.

## Event Types
| Event | Trigger | Quantity |
|---|---|---|
| `STOCK_RECEIVED` | Goods received from supplier | Positive |
| `STOCK_SOLD` | Units sold/dispensed to customer | Negative |
| `STOCK_ADJUSTED` | Manual correction (audit, damage, system) | Any non-zero |
| `STOCK_EXPIRED` | Expired units removed from stock | Negative |

## Pipeline
```
Kafka: inventory_events
  → Bronze Parquet: data/bronze/inventory_events/
  → Silver Parquet: data/silver/inventory_events/  (validated, deduplicated)
  → Quarantine:     data/quarantine/inventory_events/  (failed validation)
  → stg_inventory_events
  → fact_inventory_events  (partitioned by event_time)
  → fact_inventory_balance (snapshot)
  → v_inventory_snapshot   (BI view)
```

## Validation Rules (Silver)
- `event_id`, `event_type`, `event_time`, `producer` required
- `event_type` must be one of the 4 supported types
- `product_id`, `warehouse_id`, `batch_number`, `quantity_delta` required
- `expiry_date` required for STOCK_RECEIVED, STOCK_SOLD, STOCK_EXPIRED
- `STOCK_RECEIVED`: quantity > 0
- `STOCK_SOLD`, `STOCK_EXPIRED`: quantity < 0
- `STOCK_ADJUSTED`: quantity ≠ 0, `reason` required
- `STOCK_SOLD`: `sale_id` required
- Duplicate `event_id` values quarantined

## Key Analytics Objects
- `v_inventory_snapshot` — current stock by product+warehouse+batch with names
- `v_inventory_balance` — derived balance (SUM of deltas)
- `fact_inventory_balance` — physical snapshot (fast queries)
- `v_reorder_risk` — products below threshold

## Quality Checks
7 checks: null business keys, duplicate event IDs, staged events not loaded, null dimension keys, orphan product/warehouse dimension, rows in default partition.

## CLI Commands
```bash
python -m medwarehouse produce inventory [--max-events N]
python -m medwarehouse spark inventory-bronze
python -m medwarehouse spark inventory-silver
python -m medwarehouse orchestration build-gold
python -m medwarehouse warehouse refresh-balance
python -m medwarehouse warehouse check-reorders
```

## Airflow DAG
`inventory_gold_pipeline` — 8 tasks: validate_silver → stage → refresh_dimensions → load_facts → refresh_views → quality_checks → refresh_balance → check_reorders
