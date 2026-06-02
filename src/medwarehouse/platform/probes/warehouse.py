from __future__ import annotations

from medwarehouse.config import AppSettings, get_settings
from medwarehouse.platform.probes.base import Probe
from medwarehouse.platform.utils.warehouse import collect_warehouse_state


class WarehouseProbe(Probe):
    name = "warehouse"

    def __init__(self, *, settings: AppSettings | None = None) -> None:
        self._settings = settings or get_settings()

    def _collect(self) -> dict[str, object]:
        payload = collect_warehouse_state(settings=self._settings)
        payload["status"] = "ok" if payload["reachable"] else "warning"
        return payload

