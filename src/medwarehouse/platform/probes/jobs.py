from __future__ import annotations

from collections import Counter
from typing import Any

from medwarehouse.platform.control import ControlPlaneService, get_control_plane_service
from medwarehouse.platform.probes.base import Probe


class JobProbe(Probe):
    name = "jobs"

    def __init__(self, *, control_plane: ControlPlaneService | None = None) -> None:
        self._control_plane = control_plane or get_control_plane_service()

    def _collect(self) -> dict[str, Any]:
        items = self._control_plane.job_snapshots()
        status_counts = Counter(item["status"] for item in items)
        return {
            "status": "ok",
            "items": items,
            "summary": {
                "total": len(items),
                "running": status_counts.get("running", 0) + status_counts.get("starting", 0),
                "failed": status_counts.get("failed", 0) + status_counts.get("orphaned", 0),
                "idle": status_counts.get("idle", 0),
                "by_status": dict(status_counts),
            },
        }

