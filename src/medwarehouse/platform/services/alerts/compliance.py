"""Alert rules for regulatory compliance (supplier licenses, controlled substances)."""
from __future__ import annotations

from typing import Any

from medwarehouse.platform.models import Alert
from medwarehouse.platform.utils.time import utc_now
from medwarehouse.platform.services.alerts._rules import AlertRule


class SupplierLicenseExpiryRule(AlertRule):
    def evaluate(self, metrics: dict[str, Any]) -> Alert | None:
        warehouse = metrics.get("warehouse", {})
        if not warehouse.get("reachable", False):
            return None
        expiry = warehouse.get("supplier_license_expiry", {})
        expired  = expiry.get("expired",  0) or 0
        critical = expiry.get("critical", 0) or 0
        warning  = expiry.get("warning",  0) or 0
        if expired > 0:
            return Alert(
                id="compliance:supplier_license_expired",
                name="Supplier Drug License Expired",
                severity="CRITICAL",
                status="ACTIVE",
                message=f"{expired} active supplier(s) have an expired drug license. Procurement from unlicensed suppliers is illegal.",
                source="compliance",
                timestamp=utc_now(),
                metadata={"expired": expired, "critical": critical, "warning": warning},
            )
        if critical > 0:
            return Alert(
                id="compliance:supplier_license_critical",
                name="Supplier Drug License Expiring Soon",
                severity="CRITICAL",
                status="ACTIVE",
                message=f"{critical} active supplier(s) have a drug license expiring within 30 days.",
                source="compliance",
                timestamp=utc_now(),
                metadata={"expired": expired, "critical": critical, "warning": warning},
            )
        if warning > 0:
            return Alert(
                id="compliance:supplier_license_warning",
                name="Supplier Drug License Renewal Due",
                severity="WARNING",
                status="ACTIVE",
                message=f"{warning} active supplier(s) have a drug license expiring within 90 days.",
                source="compliance",
                timestamp=utc_now(),
                metadata={"expired": expired, "critical": critical, "warning": warning},
            )
        return None


class ControlledSubstanceComplianceRule(AlertRule):
    def evaluate(self, metrics: dict[str, Any]) -> Alert | None:
        warehouse = metrics.get("warehouse", {})
        if not warehouse.get("reachable", False):
            return None
        violations = (warehouse.get("compliance", {}) or {}).get("controlled_substance_violations", 0) or 0
        if violations <= 0:
            return None
        return Alert(
            id="compliance:controlled_substance_no_prescription",
            name="Controlled Substance Dispensed Without Prescription",
            severity="CRITICAL",
            status="ACTIVE",
            message=(
                f"{violations} sale(s) of Schedule H1/X/NDPS drugs recorded without a linked prescription. "
                "This is a regulatory violation under Indian drug laws."
            ),
            source="compliance",
            timestamp=utc_now(),
            metadata={"violation_count": violations},
        )
