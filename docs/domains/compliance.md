# Domain: Compliance & Audit

## Purpose
Provide immutable audit trails, detect regulatory violations, and identify security risks such as recalled product sales and inactive user accounts.

## Status
**Implemented as analytics views** — data read from master DB via FDW.

## Source Tables (via FDW)
- `master.audit_logs` — all entity change records
- `master.users` — user accounts with roles
- `master.products` — `drug_schedule` and `status` fields
- `master.sales` + `master.sale_items` — for product-level compliance checks

## Key Analytics Objects

### `v_audit_activity`
Audit log enriched with username and role. Excludes `password_hash`.
Covers: INSERT, UPDATE, DELETE, LOGIN, LOGOUT events with `old_values` and `new_values` (JSONB).

### `v_recalled_product_sales`
All sales where the product `status` is `recalled` or `banned`. Any rows here are a compliance and patient safety issue requiring immediate action.

### `v_prescription_compliance_violations`
(Defined in prescriptions domain, surfaced here for compliance reporting.)
Schedule H1/X/NDPS drugs dispensed without a linked prescription.

## Alert Rules

### `ControlledSubstanceComplianceRule`
Fires CRITICAL if `v_prescription_compliance_violations` contains any rows.

### `SupplierLicenseExpiryRule`
Fires CRITICAL/WARNING based on license expiry proximity.

## Regulatory Obligations
- **Dispensing register**: `v_controlled_substance_register` must be made available for inspection on demand
- **Audit trail**: `v_audit_activity` satisfies the requirement for a change log of all entity modifications
- **Recalled products**: `v_recalled_product_sales` must be reviewed whenever a product recall is issued

## Security Use Cases
- `v_inactive_user_accounts` — active accounts with no login for > 90 days should be suspended
- `v_audit_activity` filtered by `action = 'role_change'` detects privilege escalation events
- `v_audit_activity` filtered by `timestamp` outside business hours detects after-hours activity

## Power BI Usage
`v_audit_activity` for compliance reporting and security reviews. `v_recalled_product_sales` for product recall impact assessment. `v_prescription_compliance_violations` for regulatory dashboard.
