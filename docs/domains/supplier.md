# Domain: Supplier Management

## Purpose
Monitor active suppliers, track drug license validity, and maintain a directory of supplier contacts and addresses. Drug license expiry is a critical compliance item — procurement from an unlicensed supplier is illegal under the Drugs and Cosmetics Act.

## Status
**Implemented as analytics views** — data read from master DB via FDW.

## Source Tables (via FDW)
- `master.suppliers` — core supplier master with `drug_license_no` and `license_expiry_date`
- `master.supplier_contacts` — named contacts per supplier
- `master.supplier_addresses` — billing, shipping, and registered addresses

## Key Analytics Objects

### `v_supplier_license_expiry`
Every active supplier with their license validity status:
- `VALID` — license valid for more than 90 days
- `WARNING` — license expires within 31–90 days
- `CRITICAL` — license expires within 30 days
- `EXPIRED` — license has already expired

Fields include primary contact name, phone, and email for immediate action.

### `v_supplier_directory`
Active suppliers enriched with primary contact details and shipping address. Used for order dispatch and communication.

## Alert Rules

### `SupplierLicenseExpiryRule`
Reads `supplier_license_expiry` counts from the warehouse probe. Fires:
- **CRITICAL** if any expired license exists
- **CRITICAL** if any license expires within 30 days
- **WARNING** if any license expires within 31–90 days

## Procurement Performance
Full supplier performance KPIs are available in `v_supplier_performance` (procurement domain):
- Fulfillment rate (received / created POs)
- Average actual lead time vs declared `typical_lead_time_days`
- Fill rate (units received / units ordered)

## Power BI Usage
`v_supplier_license_expiry` for compliance monitoring. `v_supplier_directory` for contact lists. Join with `v_supplier_performance` on `supplier_id` for the full supplier scorecard.
