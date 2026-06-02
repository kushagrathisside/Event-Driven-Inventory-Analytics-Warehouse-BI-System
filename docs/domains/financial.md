# Domain: Financial Analytics

## Purpose
Provide accounts payable visibility (outstanding supplier payments), accounts receivable insight (customer collections), and payment method breakdowns for cash flow management.

## Status
**Implemented as analytics views** — data read from master DB via FDW.

## Source Tables (via FDW)
- `master.payments` — all payment records (customer and supplier)
- `master.purchase_orders` + `master.purchase_order_items` — PO values
- `master.sales` — invoice values

## Key Analytics Objects

### `v_accounts_payable`
Outstanding amounts owed to suppliers per purchase order.
- Shows `po_total_amount`, `amount_paid`, `outstanding_amount`, and `age_days`
- Filtered to POs with outstanding balance > 0
- Ordered by outstanding amount descending

### `v_payment_method_breakdown`
Monthly payment volume by method (Cash, UPI, Credit Card, etc.) and payer type (customer/supplier).

### `v_daily_collections`
Daily customer payment totals by method. Used for cash position monitoring and reconciliation.

## GST / Tax Analytics
Revenue-side GST can be computed from `fact_sales_events.line_revenue` combined with `master.sale_items.tax_percentage_at_sale`. The `products.hsn_code` links to the applicable tax slab for GST filing.

## Power BI Usage
- `v_accounts_payable` for AP aging report
- `v_payment_method_breakdown` for cash flow trend analysis
- `v_daily_collections` for daily cash reconciliation

## Note on Completeness
This domain surfaces financial data for visibility only. The source of truth for all financial transactions remains the OLTP system. The analytics views are read-only and do not replace the accounting system.
