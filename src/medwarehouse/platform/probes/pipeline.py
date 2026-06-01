from __future__ import annotations

from medwarehouse.config import AppSettings, get_settings
from medwarehouse.platform.probes.base import Probe
from medwarehouse.platform.utils.filesystem import summarize_artifact_tree
from medwarehouse.platform.utils.warehouse import collect_inventory_warehouse_state


class PipelineProbe(Probe):
    name = "pipeline"

    def __init__(self, *, settings: AppSettings | None = None) -> None:
        self._settings = settings or get_settings()

    def _collect(self) -> dict[str, object]:
        bronze = summarize_artifact_tree(self._settings.paths.bronze_inventory_path)
        silver = summarize_artifact_tree(self._settings.paths.silver_inventory_path)
        quarantine = summarize_artifact_tree(self._settings.paths.quarantine_inventory_path)
        warehouse = collect_inventory_warehouse_state(settings=self._settings)

        staging_count = warehouse["counts"].get("analytics.stg_inventory_events")
        fact_count = warehouse["counts"].get("analytics.fact_inventory_events")
        snapshot_count = warehouse["counts"].get("analytics.v_inventory_snapshot")

        return {
            "status": "ok" if warehouse["reachable"] else "warning",
            "stages": {
                "bronze": {
                    "count": bronze["count"],
                    "last_update": bronze["updated_at"],
                    "path": bronze["path"],
                },
                "silver": {
                    "count": silver["count"],
                    "last_update": silver["updated_at"],
                    "path": silver["path"],
                    "quarantine": quarantine["count"],
                    "quarantine_last_update": quarantine["updated_at"],
                },
                "staging": {
                    "count": staging_count,
                    "last_update": warehouse["last_updates"].get("analytics.stg_inventory_events"),
                },
                "warehouse": {
                    "reachable": warehouse["reachable"],
                    "fact_count": fact_count,
                    "snapshot_count": snapshot_count,
                    "last_update": warehouse["last_updates"].get(
                        "analytics.fact_inventory_events"
                    ),
                },
            },
            "consistency": {
                "bronze_vs_silver_gap": bronze["count"] - silver["count"],
                "silver_vs_staging_gap": (
                    None if staging_count is None else silver["count"] - staging_count
                ),
                "staging_vs_fact_gap": (
                    None
                    if staging_count is None or fact_count is None
                    else staging_count - fact_count
                ),
                "fact_vs_snapshot_gap": (
                    None
                    if fact_count is None or snapshot_count is None
                    else fact_count - snapshot_count
                ),
            },
        }

