# Domain: Sales & Revenue

## Purpose
Track every sale and cancellation to provide real-time revenue visibility, demand forecasting, and staff performance measurement.

## Status
**Fully implemented** — end-to-end from Kafka to analytics views.

## Event Types
| Event | Trigger | Required Fields |
|---|---|---|
| `SALE_CREATED` | Sale completed | `sale_id`, `product_id`, `warehouse_id`, `quantity`, `unit_price`, `currency`, `customer_type` |
| `SALE_CANCELLED` | Sale reversed | `sale_id`, `product_id`, `warehouse_id`, `quantity`, `original_event_id` |

`customer_type` must be: `RETAIL`, `HOSPITAL`, or `DISTRIBUTOR`.
`currency` must be: `INR`, `USD`, `EUR`, or `GBP`.
`quantity` must be positive for both event types.

## Pipeline
```
Kafka: sales_events
  → Bronze Parquet: data/bronze/sales_events/
  → Silver Parquet: data/silver/sales_events/
  → Quarantine:     data/quarantine/sales_events/
  → stg_sales_events
  → fact_sales_events  (partitioned by event_time, includes computed line_revenue)
  → v_revenue_summary
  → v_sales_velocity
  → v_returns_analysis
```

`line_revenue` is computed during fact load: `+quantity × unit_price` for SALE_CREATED, `-quantity × unit_price` for SALE_CANCELLED.

## Key Analytics Objects
- `v_revenue_summary` — daily net revenue by product, warehouse, currency, and customer type
- `v_sales_velocity` — daily units with 7-day and 30-day rolling averages per product+warehouse
- `v_returns_analysis` — cancellation rate and return volume per product

## Quality Checks
5 checks: null sale_id, duplicate event IDs, staged events not loaded, negative revenue on SALE_CREATED, rows in default partition.

## Power BI Usage
Connect to `v_revenue_summary` for revenue dashboards. Use `v_sales_velocity` for demand forecasting and reorder trigger tuning. All views include product and warehouse names — no joins needed in Power BI.

## CLI Commands
```bash
python -m medwarehouse produce sales [--max-events N]
python -m medwarehouse spark sales-bronze
python -m medwarehouse spark sales-silver
python -m medwarehouse orchestration build-sales-gold
```

## Airflow DAG
`sales_gold_pipeline` — 6 tasks: validate_silver → stage → refresh_dimensions → load_facts → refresh_views → quality_checks
