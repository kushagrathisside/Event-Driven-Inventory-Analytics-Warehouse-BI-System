from __future__ import annotations

from typing import Any

from medwarehouse.config import AppSettings, get_settings
from medwarehouse.platform.probes.base import Probe
from medwarehouse.platform.utils.filesystem import summarize_artifact_tree


class ArtifactProbe(Probe):
    name = "artifacts"

    def __init__(self, *, settings: AppSettings | None = None) -> None:
        self._settings = settings or get_settings()

    def _collect(self) -> dict[str, Any]:
        p = self._settings.paths
        return {
            "status": "ok",
            "artifacts": {
                "inventory_bronze":   summarize_artifact_tree(p.bronze_inventory_path),
                "inventory_silver":   summarize_artifact_tree(p.silver_inventory_path),
                "inventory_quarantine": summarize_artifact_tree(p.quarantine_inventory_path),
                "procurement_bronze": summarize_artifact_tree(p.bronze_procurement_path),
                "procurement_silver": summarize_artifact_tree(p.silver_procurement_path),
                "procurement_quarantine": summarize_artifact_tree(p.quarantine_procurement_path),
                "sales_bronze":       summarize_artifact_tree(p.bronze_sales_path),
                "sales_silver":       summarize_artifact_tree(p.silver_sales_path),
                "sales_quarantine":   summarize_artifact_tree(p.quarantine_sales_path),
                # Legacy keys kept for backward compatibility with existing UI/tests
                "bronze":     summarize_artifact_tree(p.bronze_inventory_path),
                "silver":     summarize_artifact_tree(p.silver_inventory_path),
                "quarantine": summarize_artifact_tree(p.quarantine_inventory_path),
            },
        }
