from __future__ import annotations

from typing import Any

from medwarehouse.config import AppSettings, get_settings
from medwarehouse.platform.probes.base import Probe
from medwarehouse.platform.utils.airflow import collect_airflow_dag_state


_ALL_DAG_IDS = (
    "inventory_gold_pipeline",
    "procurement_gold_pipeline",
    "sales_gold_pipeline",
)


class AirflowProbe(Probe):
    """
    Collects Airflow DAG state for all three domain pipelines.

    Top-level fields mirror the legacy single-DAG payload (inventory pipeline)
    so existing status.py and alert rules keep working.  A 'dags' dict
    exposes the full per-DAG state for the UI and new multi-DAG alert rules.
    """

    name = "airflow"

    def __init__(self, *, settings: AppSettings | None = None) -> None:
        self._settings = settings or get_settings()

    def _collect(self) -> dict[str, Any]:
        dags_root = self._settings.paths.project_root / "airflow" / "dags"

        per_dag: dict[str, dict[str, Any]] = {}
        for dag_id in _ALL_DAG_IDS:
            per_dag[dag_id] = collect_airflow_dag_state(
                dag_id=dag_id,
                dag_path=dags_root / f"{dag_id}.py",
                settings=self._settings,
            )

        primary = per_dag.get("inventory_gold_pipeline", {})
        any_failed = any(
            (d.get("last_run") or {}).get("state") == "failed"
            for d in per_dag.values()
        )
        all_exist = all(d.get("dag_exists", False) for d in per_dag.values())

        return {
            **primary,
            "dags": per_dag,
            "any_dag_failed": any_failed,
            "all_dags_exist": all_exist,
            "status": "ok" if all_exist else "warning",
        }
