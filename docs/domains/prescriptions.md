# Domain: Prescription Analytics & Compliance

## Purpose
Track prescription dispensing for regulatory compliance. Indian drug laws require controlled substances (Schedule H1, X, NDPS) to be dispensed only with a valid prescription. This domain provides the audit views and compliance checks required by law.

## Status
**Implemented as analytics views** — no separate Kafka pipeline. Data is read directly from the master database via FDW.

## Source Tables (via FDW)
- `master.prescriptions` — prescription headers (doctor, patient, date)
- `master.prescription_items` — prescribed drugs with dosage
- `master.sales` — cross-referenced via `sales.prescription_id`
- `master.products` — `drug_schedule` field (OTC, Schedule H, H1, X, NDPS)
- `master.customers` — customer details including `government_id`

## Key Analytics Objects

### `v_prescription_fill_rate`
Per-prescription summary: how many prescribed products resulted in a completed sale.

### `v_controlled_substance_register`
**Legally required register.** All Schedule H1, X, and NDPS drug dispensing records including:
- Patient name and government ID
- Drug name, schedule, and quantity
- Prescribing doctor and prescription date
- Whether a prescription was linked (`missing_prescription` flag)

### `v_prescription_compliance_violations`
Subset of `v_controlled_substance_register` where `missing_prescription = TRUE`.
Any rows here represent active regulatory violations.

### `v_doctor_prescribing_patterns`
Drug-level prescribing volume per doctor for utilisation review.

## Alert Rule
`ControlledSubstanceComplianceRule` — fires CRITICAL if any violations exist in `v_prescription_compliance_violations`. Requires the warehouse DB to be reachable.

## Regulatory Context
Under the Drugs and Cosmetics Act (India):
- **Schedule H**: Prescription required, recorded in dispensing register
- **Schedule H1**: Prescription retained, patient identity verified
- **Schedule X**: Prescription in triplicate, government forms may be required
- **NDPS**: Narcotic Drugs and Psychotropic Substances — most restricted, strict register required

## Power BI Usage
Connect to `v_controlled_substance_register` for the regulatory dispensing register. Connect to `v_prescription_compliance_violations` for compliance monitoring. No joins needed.

## No Pipeline Required
Prescription data is operational (entered at point of sale in the OLTP system). Analytics reads it directly via FDW — no Kafka events or Spark jobs needed.
