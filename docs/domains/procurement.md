# Domain: Procurement Lifecycle

## Purpose
Track the full lifecycle of purchase orders from creation through approval, receipt, and cancellation. Enables supplier performance analysis and procurement KPIs.

## Status
**Fully implemented** — end-to-end from Kafka to analytics views.

## Event Types
| Event | Trigger | Required Fields |
|---|---|---|
| `PO_CREATED` | Purchase order raised | `po_id`, `product_id`, `ordered_quantity`, `supplier_id`, `trigger_reason` |
| `PO_APPROVED` | Manager approves PO | `po_id`, `approved_by` |
| `PO_RECEIVED` | Goods received against PO | `po_id`, `product_id`, `received_quantity`, `warehouse_id` |
| `PO_CANCELLED` | PO cancelled before receipt | `po_id`, `reason` |

`trigger_reason` must be `AUTO_REORDER` or `MANUAL`.

## PO Lifecycle State Machine
```
PO_CREATED → PO_APPROVED → PO_RECEIVED  (fulfilled)
PO_CREATED → PO_CANCELLED               (cancelled)
```

## Pipeline
```
Kafka: procurement_events
  → Bronze Parquet: data/bronze/procurement_events/
  → Silver Parquet: data/silver/procurement_events/
  → Quarantine:     data/quarantine/procurement_events/
  → stg_procurement_events
  → fact_procurement_events  (partitioned by event_time)
  → v_po_lifecycle
  → v_supplier_performance
  → v_po_aging
```

## Key Analytics Objects
- `v_po_lifecycle` — latest status per PO with all lifecycle timestamps and computed `actual_lead_time_days`
- `v_supplier_performance` — per-supplier KPIs: fulfillment rate, fill rate, average lead time
- `v_po_aging` — open POs (PENDING/APPROVED) ordered by age

## Quality Checks
4 checks: null po_id, duplicate event IDs, staged events not loaded, rows in default partition.

## Integration with Reorder System
When the automated reorder service (`check_reorder_thresholds`) creates a DRAFT purchase order in `pending_purchase_orders`, the operator reviews and approves it in the webapp. On approval and send, the operator records receipt via `POST /api/v1/orders/{po_id}/receive` — which emits a `STOCK_RECEIVED` inventory event and closes the procurement loop.

## CLI Commands
```bash
python -m medwarehouse produce procurement [--max-events N]
python -m medwarehouse spark procurement-bronze
python -m medwarehouse spark procurement-silver
python -m medwarehouse orchestration build-procurement-gold
```

## Airflow DAG
`procurement_gold_pipeline` — 6 tasks: validate_silver → stage → refresh_dimensions → load_facts → refresh_views → quality_checks
