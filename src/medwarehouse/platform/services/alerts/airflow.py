"""Alert rules for Airflow DAG health and staleness."""
from __future__ import annotations

from typing import Any

from medwarehouse.platform.models import Alert
from medwarehouse.platform.utils.time import lag_seconds, utc_now
from medwarehouse.platform.services.alerts._rules import AlertRule


class AirflowFailureRule(AlertRule):
    def evaluate(self, metrics: dict[str, Any]) -> Alert | None:
        last_run = metrics.get("airflow", {}).get("last_run")
        if not last_run:
            return None
        if (last_run.get("state") or "").lower() != "failed":
            return None
        return Alert(
            id="airflow:dag_failure",
            name="Airflow DAG Failure",
            severity="CRITICAL",
            status="ACTIVE",
            message="The latest Airflow DAG run failed.",
            source="airflow",
            timestamp=utc_now(),
            metadata={"dag_id": metrics.get("airflow", {}).get("dag_id"), "last_run": last_run},
        )


class AirflowStalenessRule(AlertRule):
    def __init__(self, *, threshold_seconds: float = 86400.0) -> None:
        self._threshold = threshold_seconds

    def evaluate(self, metrics: dict[str, Any]) -> Alert | None:
        airflow = metrics.get("airflow", {})
        if not airflow.get("metadata_available"):
            return None
        last_run = airflow.get("last_run")
        if last_run is None:
            return Alert(
                id="airflow:no_runs",
                name="No Airflow DAG Runs",
                severity="WARNING",
                status="ACTIVE",
                message="Airflow metadata is available but no DAG run has been recorded.",
                source="airflow",
                timestamp=utc_now(),
                metadata={"dag_id": airflow.get("dag_id")},
            )
        lag = lag_seconds(airflow.get("last_run_at"))
        if lag is None or lag <= self._threshold:
            return None
        severity = "CRITICAL" if lag > self._threshold * 2 else "WARNING"
        return Alert(
            id="airflow:stale_runs",
            name="Stale Airflow DAG Activity",
            severity=severity,
            status="ACTIVE",
            message=f"No Airflow DAG activity has been recorded for {int(lag)} seconds.",
            source="airflow",
            timestamp=utc_now(),
            metadata={"dag_id": airflow.get("dag_id"), "lag_seconds": lag, "threshold_seconds": self._threshold},
        )
