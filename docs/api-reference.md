# Operator Webapp — API Reference

Base URL: `http://localhost:8080`

All endpoints require `X-API-Key: <key>` or `Authorization: Bearer <key>` unless `MW_WEBAPP_API_KEY` is unset.

The `/health` endpoint is always accessible.

> **ID types:** `product_id`, `warehouse_id`, `supplier_id`, `customer_id` are **text strings** (business keys), not UUID type. In the sample dataset they take the form `P-LOCAL-CALPOL-500` / `W-LOCAL-RECEIVING-01`. In a production OLTP deployment they may be UUID-format strings; do not apply PostgreSQL `::uuid` casts.

---

## Health

### `GET /health`
Returns service status. No authentication required.

**Response 200:**
```json
{ "status": "ok", "service": "medwarehouse-webapp" }
```

---

## Stock Entry (`/api/v1/stock`)

### `POST /api/v1/stock/received`
Record goods received into a warehouse lot and emit a `STOCK_RECEIVED` Kafka event.

**Request body:**
```json
{
  "product_id": "string",
  "warehouse_id": "string",
  "batch_number": "string",
  "expiry_date": "YYYY-MM-DD",
  "quantity": 200,
  "supplier_id": "string",          // optional
  "cost_price_per_unit": 85.50,     // optional
  "sale_price_per_unit": 120.00     // optional
}
```

**Response 201:**
```json
{ "event_id": "uuid", "event_type": "STOCK_RECEIVED", "status": "accepted" }
```

**Response 400:** Missing required field.
**Response 422:** Contract validation failure (e.g. quantity ≤ 0).

---

### `POST /api/v1/stock/sold`
Record a stock sale, reduce lot quantity, and emit `STOCK_SOLD`.

**Request body:**
```json
{
  "product_id": "string",
  "warehouse_id": "string",
  "batch_number": "string",
  "expiry_date": "YYYY-MM-DD",
  "quantity": 24,
  "sale_id": "string"
}
```

**Response 201:** `{ "event_id": ..., "event_type": "STOCK_SOLD", "status": "accepted" }`
**Response 422:** Insufficient stock or lot not found.

---

### `POST /api/v1/stock/adjusted`
Record a manual stock adjustment and emit `STOCK_ADJUSTED`.

**Request body:**
```json
{
  "product_id": "string",
  "warehouse_id": "string",
  "batch_number": "string",
  "quantity_delta": -5,
  "reason": "AUDIT"
}
```
`reason` must be one of: `AUDIT`, `DAMAGE`, `SYSTEM_CORRECTION`

**Response 201:** `{ "event_id": ..., "event_type": "STOCK_ADJUSTED", "status": "accepted" }`
**Response 422:** Lot not found or quantity_delta is zero.

---

### `POST /api/v1/stock/expired`
Record expired stock removal and emit `STOCK_EXPIRED`.

**Request body:**
```json
{
  "product_id": "string",
  "warehouse_id": "string",
  "batch_number": "string",
  "expiry_date": "YYYY-MM-DD",
  "quantity": 5
}
```

**Response 201:** `{ "event_id": ..., "event_type": "STOCK_EXPIRED", "status": "accepted" }`

---

### `GET /api/v1/stock/movements`
List recent stock movement events from the analytics staging table.

**Query params:**
- `limit` (int, default 50, max 500)

**Response 200:** Array of event objects with fields: `event_id`, `event_type`, `event_time`, `product_id`, `warehouse_id`, `batch_number`, `quantity_delta`, `supplier_id`, `sale_id`, `adjustment_reason`

---

## Reference Data (`/api/v1`)

### `GET /api/v1/products`
List all active products from the master database.

**Response 200:** Array of `{ product_id, brand_name, generic_name, form_factor, hsn_code, supplier_id }`

---

### `GET /api/v1/suppliers`
List all active suppliers.

**Response 200:** Array of `{ supplier_id, name, gstin }`

---

### `GET /api/v1/warehouses`
List all active warehouse locations.

**Response 200:** Array of `{ warehouse_id, warehouse_name, temperature_range }`

---

### `GET /api/v1/inventory`
Current stock levels from `analytics.v_inventory_snapshot` (non-zero balances only).

**Response 200:** Array of `{ product_id, brand_name, generic_name, warehouse_id, warehouse_name, batch_number, expiry_date, current_quantity, last_event_time }`

---

### `GET /api/v1/inventory/risk`
Products at or approaching reorder threshold, ordered by severity.

**Response 200:** Array of `{ product_id, brand_name, warehouse_id, warehouse_name, batch_number, expiry_date, current_quantity, reorder_point, quantity_gap, risk_status, lead_time_days }`

`risk_status` values: `OUT_OF_STOCK`, `BELOW_THRESHOLD`, `APPROACHING_THRESHOLD`, `HEALTHY`

---

## Reorder Policies (`/api/v1/reorder-policies`)

### `GET /api/v1/reorder-policies`
List all active reorder policies.

**Response 200:** Array of `{ id, product_id, warehouse_id, reorder_point, reorder_quantity, preferred_supplier_id, lead_time_days, active, created_at, updated_at }`

---

### `POST /api/v1/reorder-policies`
Create a reorder policy. Replaces any existing active policy for the same product+warehouse.

**Request body:**
```json
{
  "product_id": "string",
  "warehouse_id": "string",
  "reorder_point": 50,
  "reorder_quantity": 200,
  "preferred_supplier_id": "string",  // optional
  "lead_time_days": 7                 // optional, default 7
}
```

**Response 201:** `{ "id": 42, "status": "created" }`

---

### `PUT /api/v1/reorder-policies/{id}`
Update an existing policy's thresholds.

**Request body:** Any subset of `{ reorder_point, reorder_quantity, preferred_supplier_id, lead_time_days }`

**Response 200:** `{ "id": 42, "status": "updated" }`

---

### `DELETE /api/v1/reorder-policies/{id}`
Deactivate a policy (sets `active = FALSE`).

**Response 200:** `{ "id": 42, "status": "deactivated" }`

---

## Purchase Orders (`/api/v1/orders`)

Auto-generated purchase orders flow: `DRAFT → APPROVED → SENT → RECEIVED / CANCELLED`

### `GET /api/v1/orders`
List purchase orders with enriched product and supplier names.

**Query params:**
- `status` (string, optional) — filter by status: `DRAFT`, `APPROVED`, `SENT`, `RECEIVED`, `CANCELLED`

**Response 200:** Array from `v_purchase_order_status`

---

### `POST /api/v1/orders/{po_id}/approve`
Approve a DRAFT purchase order.

**Request body:**
```json
{ "approved_by": "manager_username" }
```

**Response 200:** `{ "po_id": "...", "status": "approved" }`
**Response 200 (not found):** `{ "po_id": "...", "status": "not_found_or_wrong_state" }`

---

### `POST /api/v1/orders/{po_id}/send`
Mark an APPROVED order as SENT and dispatch supplier email (if SMTP is configured).

**Response 200:** `{ "po_id": "...", "status": "sent" }`

---

### `POST /api/v1/orders/{po_id}/cancel`
Cancel a DRAFT or APPROVED order.

**Response 200:** `{ "po_id": "...", "status": "cancelled" }`

---

### `POST /api/v1/orders/{po_id}/receive`
Mark a SENT order as RECEIVED and emit a `STOCK_RECEIVED` Kafka event to update inventory.

**Response 200:** `{ "po_id": "...", "status": "received", "stock_event_id": "uuid" }`

---

## Monitoring Platform API Reference

Base URL: `http://localhost:8787/api`

All endpoints support `?refresh=hard` to bypass the 60-second status cache.

| Endpoint | Description |
|---|---|
| `GET /api/status` | Legacy snapshot: environment, jobs, infra, alerts |
| `GET /api/status/full` | Complete status including all probe metrics |
| `GET /api/status/pipeline` | Pipeline freshness metrics and alerts |
| `GET /api/status/infra` | Infrastructure service status and alerts |
| `GET /api/status/alerts` | Active and recent alert list |
| `POST /api/jobs/{job_id}/start` | Start a registered pipeline job |
| `POST /api/jobs/{job_id}/stop` | Stop a running long-running job |
| `POST /api/infra/start` | Start infrastructure (docker-compose up) |
| `POST /api/infra/stop` | Stop infrastructure (docker-compose down) |

Available `job_id` values: `warehouse_bootstrap`, `inventory_producer`, `inventory_bronze`, `inventory_silver`, `inventory_stage`, `refresh_dimensions`, `load_inventory_facts`, `refresh_views`, `quality_checks`, `build_gold`, `refresh_balance`, `check_reorders`, `procurement_producer`, `procurement_bronze`, `procurement_silver`, `build_procurement_gold`, `sales_producer`, `sales_bronze`, `sales_silver`, `build_sales_gold`
