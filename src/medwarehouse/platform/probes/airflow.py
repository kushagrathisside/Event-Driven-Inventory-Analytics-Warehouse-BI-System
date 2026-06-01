from __future__ import annotations

from pathlib import Path

from medwarehouse.config import AppSettings, get_settings
from medwarehouse.platform.probes.base import Probe
from medwarehouse.platform.utils.airflow import collect_airflow_dag_state


class AirflowProbe(Probe):
    name = "airflow"

    def __init__(
        self,
        *,
        settings: AppSettings | None = None,
        dag_id: str = "inventory_gold_pipeline",
        dag_path: Path | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._dag_id = dag_id
        self._dag_path = dag_path or (
            self._settings.paths.project_root / "airflow" / "dags" / f"{dag_id}.py"
        )

    def _collect(self) -> dict[str, object]:
        payload = collect_airflow_dag_state(
            dag_id=self._dag_id,
            dag_path=self._dag_path,
            settings=self._settings,
        )
        payload["status"] = "ok" if payload["dag_exists"] else "warning"
        return payload

