# Domain: Operations & Staff Performance

## Purpose
Monitor staff productivity, detect operational anomalies (excessive adjustments, after-hours activity), and maintain account hygiene (inactive accounts).

## Status
**Implemented as analytics views** — data read from master DB via FDW.

## Source Tables (via FDW)
- `master.users` — staff accounts with roles and last login
- `master.sales` — cross-reference via `sales.user_id`
- `master.stock_adjustments` — cross-reference via `adjusted_by`
- `master.goods_receipts` — cross-reference via `received_by`
- `master.audit_logs` — activity timeline per user

## Key Analytics Objects

### `v_staff_performance`
Per-user summary of all operational activity:
- `completed_sales` — count of completed sales processed
- `total_revenue` — gross revenue from completed sales
- `returns_processed` — count of returned sales
- `stock_adjustments_made` — number of manual stock adjustments
- `grns_received` — number of GRNs processed
- `last_login` — most recent login timestamp

### `v_inactive_user_accounts`
Active accounts with no login for more than 90 days. These are a security risk and should be suspended.
Fields: `user_id`, `username`, `role`, `last_login`, `days_since_login`

## Operational Anomaly Patterns
Using `v_staff_performance`:
- High `stock_adjustments_made` relative to peers → investigate for shrinkage or errors
- High `returns_processed` rate → investigate customer complaint patterns or fraud

Using `v_audit_activity` (compliance domain):
- Filter `user_id` + `timestamp` outside 08:00–20:00 for after-hours activity review
- Filter `entity = 'users'` + `action = 'UPDATE'` for role change detection

## Power BI Usage
`v_staff_performance` for operations management dashboards. `v_inactive_user_accounts` for IT security reporting. Set up automated alerts using `v_inactive_user_accounts` row count.

## Privacy Note
Staff performance data is sensitive HR information. Access should be restricted to managers and administrators. Apply PostgreSQL row-level security if per-manager scoping is needed in the future.
