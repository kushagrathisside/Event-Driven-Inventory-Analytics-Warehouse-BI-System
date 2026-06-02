"""Alert rules for infrastructure and warehouse reachability."""
from __future__ import annotations

from typing import Any

from medwarehouse.platform.models import Alert
from medwarehouse.platform.utils.time import utc_now
from medwarehouse.platform.services.alerts._rules import AlertRule


class WarehouseReachabilityRule(AlertRule):
    def evaluate(self, metrics: dict[str, Any]) -> Alert | None:
        warehouse = metrics.get("warehouse", {})
        if warehouse.get("reachable", False):
            return None
        return Alert(
            id="infra:warehouse_db",
            name="Warehouse Unreachable",
            severity="CRITICAL",
            status="ACTIVE",
            message=warehouse.get("error") or "The analytics warehouse is unreachable.",
            source="warehouse",
            timestamp=utc_now(),
            metadata={"target": warehouse.get("target")},
        )


class InfraServiceRule(AlertRule):
    def __init__(self, *, service_name: str, label: str) -> None:
        self._service_name = service_name
        self._label = label

    def evaluate(self, metrics: dict[str, Any]) -> Alert | None:
        status = metrics.get("infra", {}).get("service_map", {}).get(self._service_name)
        if status == "running":
            return None
        return Alert(
            id=f"infra:{self._service_name}",
            name=f"{self._label} Service Down",
            severity="CRITICAL",
            status="ACTIVE",
            message=f"{self._label} is not running (reported state: {status or 'unknown'}).",
            source="infra",
            timestamp=utc_now(),
            metadata={"service_name": self._service_name, "status": status},
        )
