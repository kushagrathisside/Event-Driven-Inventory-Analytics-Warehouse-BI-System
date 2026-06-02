# Domain: Customer Analytics

## Purpose
Understand customer purchasing behaviour, identify high-value customers, and track demographic patterns relevant to pharmaceutical dispensing (age groups, prescription frequency, government ID verification).

## Status
**Implemented as analytics views** — data read from master DB via FDW.

## Source Tables (via FDW)
- `master.customers` — customer master with PII fields
- `master.sales` — cross-reference for purchase history
- `master.prescriptions` — cross-reference for prescription history

## Key Analytics Objects

### `v_customer_summary`
Per-customer aggregation:
- `lifetime_value` — total spend on completed sales
- `total_purchases` — count of completed sales
- `first_purchase_date` / `last_purchase_date`
- `total_prescriptions` — number of prescriptions linked to this customer
- `age_years` — computed from `date_of_birth`
- `id_verified` — whether `government_id` is populated (required for Schedule H1/X/NDPS)

## Privacy Considerations
The `v_customer_summary` view exposes PII including `full_name`, `phone`, `email`, and `government_id`. Grant the `analytics_reader` role only to users who are authorised to view customer personal data.

The `password_hash` field from `users` is never exposed through any analytics view.

## Segmentation
Customer types tracked in `fact_sales_events.customer_type`:
- `RETAIL` — individual retail buyers
- `HOSPITAL` — institutional bulk buyers
- `DISTRIBUTOR` — wholesale distributors

Power BI can segment revenue and units by customer type without any joins.

## Power BI Usage
`v_customer_summary` for customer lifetime value and loyalty analysis. Cross-join with `v_revenue_summary` on `customer_type` for segment-level revenue breakdown.
