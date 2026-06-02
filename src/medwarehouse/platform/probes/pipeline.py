from __future__ import annotations

from typing import Any

from medwarehouse.config import AppSettings, get_settings
from medwarehouse.platform.probes.base import Probe
from medwarehouse.platform.utils.filesystem import summarize_artifact_tree
from medwarehouse.platform.utils.time import utc_now
from medwarehouse.platform.utils.warehouse import collect_warehouse_state


class PipelineProbe(Probe):
    name = "pipeline"

    def __init__(self, *, settings: AppSettings | None = None) -> None:
        self._settings = settings or get_settings()

    def _collect(self) -> dict[str, Any]:
        warehouse = collect_warehouse_state(settings=self._settings)
        return self._build_result(warehouse)

    def collect_with_warehouse(self, warehouse_state: dict[str, Any]) -> dict[str, Any]:
        """Collect pipeline metrics using a pre-fetched warehouse state dict.

        Called by StatusService to avoid a second DB roundtrip when WarehouseProbe
        has already queried the same connection.
        """
        try:
            result = self._build_result(warehouse_state)
        except Exception as exc:
            result = self._error_payload(exc)
        result.setdefault("probe", self.name)
        result.setdefault("checked_at", utc_now())
        return result

    def _build_result(self, warehouse: dict[str, Any]) -> dict[str, Any]:
        p = self._settings.paths

        # Inventory artifacts
        inv_bronze     = summarize_artifact_tree(p.bronze_inventory_path)
        inv_silver     = summarize_artifact_tree(p.silver_inventory_path)
        inv_quarantine = summarize_artifact_tree(p.quarantine_inventory_path)

        # Procurement artifacts
        proc_bronze     = summarize_artifact_tree(p.bronze_procurement_path)
        proc_silver     = summarize_artifact_tree(p.silver_procurement_path)
        proc_quarantine = summarize_artifact_tree(p.quarantine_procurement_path)

        # Sales artifacts
        sales_bronze     = summarize_artifact_tree(p.bronze_sales_path)
        sales_silver     = summarize_artifact_tree(p.silver_sales_path)
        sales_quarantine = summarize_artifact_tree(p.quarantine_sales_path)

        def _count(key: str):
            return warehouse["counts"].get(key)

        def _ts(key: str):
            return warehouse["last_updates"].get(key)

        return {
            "status": "ok" if warehouse["reachable"] else "warning",
            "domains": {
                "inventory": {
                    "bronze":    {"count": inv_bronze["count"],  "last_update": inv_bronze["updated_at"],  "path": inv_bronze["path"]},
                    "silver":    {"count": inv_silver["count"],  "last_update": inv_silver["updated_at"],  "path": inv_silver["path"],
                                  "quarantine": inv_quarantine["count"]},
                    "staging":   {"count": _count("analytics.stg_inventory_events"),  "last_update": _ts("analytics.stg_inventory_events")},
                    "warehouse": {"reachable": warehouse["reachable"],
                                  "fact_count": _count("analytics.fact_inventory_events"),
                                  "snapshot_count": _count("analytics.v_inventory_snapshot"),
                                  "last_update": _ts("analytics.fact_inventory_events")},
                },
                "procurement": {
                    "bronze":    {"count": proc_bronze["count"],  "last_update": proc_bronze["updated_at"],  "path": proc_bronze["path"]},
                    "silver":    {"count": proc_silver["count"],  "last_update": proc_silver["updated_at"],  "path": proc_silver["path"],
                                  "quarantine": proc_quarantine["count"]},
                    "staging":   {"count": _count("analytics.stg_procurement_events"),  "last_update": _ts("analytics.stg_procurement_events")},
                    "warehouse": {"reachable": warehouse["reachable"],
                                  "fact_count": _count("analytics.fact_procurement_events"),
                                  "last_update": _ts("analytics.fact_procurement_events")},
                },
                "sales": {
                    "bronze":    {"count": sales_bronze["count"],  "last_update": sales_bronze["updated_at"],  "path": sales_bronze["path"]},
                    "silver":    {"count": sales_silver["count"],  "last_update": sales_silver["updated_at"],  "path": sales_silver["path"],
                                  "quarantine": sales_quarantine["count"]},
                    "staging":   {"count": _count("analytics.stg_sales_events"),  "last_update": _ts("analytics.stg_sales_events")},
                    "warehouse": {"reachable": warehouse["reachable"],
                                  "fact_count": _count("analytics.fact_sales_events"),
                                  "last_update": _ts("analytics.fact_sales_events")},
                },
            },
            # Legacy top-level keys kept for backward compatibility with existing tests/UI
            "stages": {
                "bronze":    {"count": inv_bronze["count"],  "last_update": inv_bronze["updated_at"],  "path": inv_bronze["path"]},
                "silver":    {"count": inv_silver["count"],  "last_update": inv_silver["updated_at"],  "path": inv_silver["path"],
                              "quarantine": inv_quarantine["count"], "quarantine_last_update": inv_quarantine["updated_at"]},
                "staging":   {"count": _count("analytics.stg_inventory_events"),  "last_update": _ts("analytics.stg_inventory_events")},
                "warehouse": {"reachable": warehouse["reachable"],
                              "fact_count": _count("analytics.fact_inventory_events"),
                              "snapshot_count": _count("analytics.v_inventory_snapshot"),
                              "last_update": _ts("analytics.fact_inventory_events")},
            },
            "consistency": {
                "bronze_vs_silver_gap": inv_bronze["count"] - inv_silver["count"],
                "silver_vs_staging_gap": (
                    None
                    if _count("analytics.stg_inventory_events") is None
                    else inv_silver["count"] - _count("analytics.stg_inventory_events")
                ),
                "staging_vs_fact_gap": (
                    None
                    if _count("analytics.stg_inventory_events") is None
                    or _count("analytics.fact_inventory_events") is None
                    else _count("analytics.stg_inventory_events") - _count("analytics.fact_inventory_events")
                ),
                "fact_vs_snapshot_gap": (
                    None
                    if _count("analytics.fact_inventory_events") is None
                    or _count("analytics.v_inventory_snapshot") is None
                    else _count("analytics.fact_inventory_events") - _count("analytics.v_inventory_snapshot")
                ),
            },
        }
