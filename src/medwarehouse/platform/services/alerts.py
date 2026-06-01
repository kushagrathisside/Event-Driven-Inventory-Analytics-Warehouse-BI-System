from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import replace
from typing import Any, Iterable

from medwarehouse.platform.models import Alert, SEVERITY_ORDER
from medwarehouse.platform.utils.time import lag_seconds, utc_now


class AlertRule:
    def evaluate(self, metrics: dict[str, Any]) -> Alert | None:
        raise NotImplementedError


class FreshnessAlertRule(AlertRule):
    def __init__(
        self,
        *,
        stage: str,
        label: str,
        expected_seconds: float,
    ) -> None:
        self._stage = stage
        self._label = label
        self._expected_seconds = expected_seconds

    def evaluate(self, metrics: dict[str, Any]) -> Alert | None:
        stage_metrics = metrics.get("pipeline", {}).get(self._stage, {})
        last_update = stage_metrics.get("last_update")
        lag = stage_metrics.get("lag_seconds")
        if last_update is None or lag is None:
            return None

        if lag > (self._expected_seconds * 5):
            severity = "CRITICAL"
        elif lag > (self._expected_seconds * 2):
            severity = "WARNING"
        else:
            return None

        return Alert(
            id=f"freshness:{self._stage}",
            name=f"{self._label} Freshness",
            severity=severity,
            status="ACTIVE",
            message=(
                f"{self._label} has not updated for {int(lag)} seconds. "
                f"Expected within {int(self._expected_seconds)} seconds."
            ),
            source=self._stage,
            timestamp=utc_now(),
            metadata={
                "stage": self._stage,
                "lag_seconds": lag,
                "expected_seconds": self._expected_seconds,
                "last_update": last_update,
            },
        )


class VolumeBaselineStore:
    def __init__(self, *, window_size: int = 6) -> None:
        self._window_size = window_size
        self._samples: dict[str, deque[int]] = defaultdict(lambda: deque(maxlen=self._window_size))

    def average(self, key: str) -> float | None:
        samples = self._samples.get(key)
        if not samples:
            return None
        return sum(samples) / len(samples)

    def sample_count(self, key: str) -> int:
        samples = self._samples.get(key)
        return len(samples or ())

    def append(self, key: str, value: int | None) -> None:
        if value is None:
            return
        self._samples[key].append(int(value))


class VolumeAnomalyRule(AlertRule):
    def __init__(
        self,
        *,
        stage: str,
        label: str,
        baseline_store: VolumeBaselineStore,
        minimum_samples: int = 3,
    ) -> None:
        self._stage = stage
        self._label = label
        self._baseline_store = baseline_store
        self._minimum_samples = minimum_samples

    def evaluate(self, metrics: dict[str, Any]) -> Alert | None:
        stage_metrics = metrics.get("pipeline", {}).get(self._stage, {})
        current = stage_metrics.get("count")
        if current is None:
            return None

        baseline_key = f"pipeline:{self._stage}:count"
        average = self._baseline_store.average(baseline_key)
        if average is None or self._baseline_store.sample_count(baseline_key) < self._minimum_samples:
            return None
        if average <= 0:
            return None

        severity = None
        if current <= average * 0.2:
            severity = "CRITICAL"
        elif current <= average * 0.5:
            severity = "WARNING"
        if severity is None:
            return None

        return Alert(
            id=f"volume:{self._stage}",
            name=f"{self._label} Volume Drop",
            severity=severity,
            status="ACTIVE",
            message=(
                f"{self._label} volume dropped to {current} against a rolling baseline "
                f"of {average:.1f}."
            ),
            source=self._stage,
            timestamp=utc_now(),
            metadata={
                "stage": self._stage,
                "current_count": current,
                "baseline_average": average,
            },
        )


class QuarantineAlertRule(AlertRule):
    def __init__(
        self,
        *,
        warning_threshold: int = 1,
        critical_threshold: int = 5,
    ) -> None:
        self._warning_threshold = warning_threshold
        self._critical_threshold = critical_threshold

    def evaluate(self, metrics: dict[str, Any]) -> Alert | None:
        silver = metrics.get("pipeline", {}).get("silver", {})
        quarantine = silver.get("quarantine", 0) or 0
        if quarantine >= self._critical_threshold:
            severity = "CRITICAL"
        elif quarantine >= self._warning_threshold:
            severity = "WARNING"
        else:
            return None

        return Alert(
            id="quality:quarantine",
            name="Quarantine Volume",
            severity=severity,
            status="ACTIVE",
            message=f"Quarantine contains {quarantine} parquet files pending investigation.",
            source="silver",
            timestamp=utc_now(),
            metadata={
                "quarantine_count": quarantine,
                "warning_threshold": self._warning_threshold,
                "critical_threshold": self._critical_threshold,
            },
        )


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
        infra = metrics.get("infra", {})
        service_map = infra.get("service_map", {})
        status = service_map.get(self._service_name)
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


class PipelineConsistencyRule(AlertRule):
    def evaluate(self, metrics: dict[str, Any]) -> Alert | None:
        bronze = metrics.get("pipeline", {}).get("bronze", {})
        silver = metrics.get("pipeline", {}).get("silver", {})
        bronze_count = bronze.get("count", 0) or 0
        silver_count = silver.get("count", 0) or 0
        if bronze_count <= 0:
            return None

        gap = bronze_count - silver_count
        if gap <= 0:
            return None

        severity = "CRITICAL" if silver_count == 0 and bronze_count >= 5 else "WARNING"
        return Alert(
            id="pipeline:bronze_silver_gap",
            name="Bronze To Silver Mismatch",
            severity=severity,
            status="ACTIVE",
            message=(
                f"Bronze shows {bronze_count} parquet files while Silver shows {silver_count}. "
                "The pipeline may be falling behind or producing incomplete output."
            ),
            source="pipeline",
            timestamp=utc_now(),
            metadata={
                "bronze_count": bronze_count,
                "silver_count": silver_count,
                "gap": gap,
            },
        )


class AirflowFailureRule(AlertRule):
    def evaluate(self, metrics: dict[str, Any]) -> Alert | None:
        airflow = metrics.get("airflow", {})
        last_run = airflow.get("last_run")
        if not last_run:
            return None

        state = (last_run.get("state") or "").lower()
        if state != "failed":
            return None
        return Alert(
            id="airflow:dag_failure",
            name="Airflow DAG Failure",
            severity="CRITICAL",
            status="ACTIVE",
            message="The latest Airflow DAG run failed.",
            source="airflow",
            timestamp=utc_now(),
            metadata={"dag_id": airflow.get("dag_id"), "last_run": last_run},
        )


class AirflowStalenessRule(AlertRule):
    def __init__(self, *, threshold_seconds: float = 86400.0) -> None:
        self._threshold_seconds = threshold_seconds

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

        last_run_at = airflow.get("last_run_at")
        lag = lag_seconds(last_run_at)
        if lag is None or lag <= self._threshold_seconds:
            return None

        severity = "CRITICAL" if lag > (self._threshold_seconds * 2) else "WARNING"
        return Alert(
            id="airflow:stale_runs",
            name="Stale Airflow DAG Activity",
            severity=severity,
            status="ACTIVE",
            message=(
                f"No Airflow DAG activity has been recorded for {int(lag)} seconds."
            ),
            source="airflow",
            timestamp=utc_now(),
            metadata={
                "dag_id": airflow.get("dag_id"),
                "lag_seconds": lag,
                "threshold_seconds": self._threshold_seconds,
            },
        )


class AlertEngine:
    def __init__(self, rules: Iterable[AlertRule], *, recent_limit: int = 30) -> None:
        self._rules = list(rules)
        self._recent_limit = recent_limit
        self._recent_events: deque[Alert] = deque(maxlen=recent_limit)
        self._active_alerts: dict[str, Alert] = {}
        self._baseline_store = next(
            (
                rule._baseline_store
                for rule in self._rules
                if isinstance(rule, VolumeAnomalyRule)
            ),
            None,
        )

    def evaluate(self, metrics: dict[str, Any]) -> list[Alert]:
        candidate_alerts = [alert for alert in (rule.evaluate(metrics) for rule in self._rules) if alert]
        self._reconcile(candidate_alerts)
        self._record_baselines(metrics)
        return self.active_alerts()

    def active_alerts(self) -> list[Alert]:
        return sorted(
            self._active_alerts.values(),
            key=lambda alert: (-SEVERITY_ORDER.get(alert.severity, 0), alert.timestamp, alert.id),
        )

    def recent_events(self) -> list[Alert]:
        return list(reversed(self._recent_events))

    def summary(self) -> dict[str, Any]:
        active = self.active_alerts()
        return {
            "total": len(active),
            "critical": sum(1 for alert in active if alert.severity == "CRITICAL"),
            "warning": sum(1 for alert in active if alert.severity == "WARNING"),
            "info": sum(1 for alert in active if alert.severity == "INFO"),
            "sources": sorted({alert.source for alert in active}),
        }

    def snapshot(self) -> dict[str, Any]:
        return {
            "active": [alert.to_dict() for alert in self.active_alerts()],
            "recent": [alert.to_dict() for alert in self.recent_events()],
            "summary": self.summary(),
        }

    def _reconcile(self, candidate_alerts: list[Alert]) -> None:
        now = utc_now()
        candidate_map = {alert.id: alert for alert in candidate_alerts}

        for alert_id, candidate in candidate_map.items():
            active_candidate = replace(candidate, timestamp=now, status="ACTIVE")
            previous = self._active_alerts.get(alert_id)
            if previous is None or previous.fingerprint() != active_candidate.fingerprint():
                self._active_alerts[alert_id] = active_candidate
                self._recent_events.append(active_candidate)

        for alert_id, previous in list(self._active_alerts.items()):
            if alert_id in candidate_map:
                continue
            resolved = replace(previous, status="RESOLVED", timestamp=now)
            self._recent_events.append(resolved)
            self._active_alerts.pop(alert_id, None)

    def _record_baselines(self, metrics: dict[str, Any]) -> None:
        if self._baseline_store is None:
            return
        for stage in ("bronze", "silver"):
            count = metrics.get("pipeline", {}).get(stage, {}).get("count")
            self._baseline_store.append(f"pipeline:{stage}:count", count)


def build_default_alert_engine() -> AlertEngine:
    baseline_store = VolumeBaselineStore(window_size=6)
    rules: list[AlertRule] = [
        FreshnessAlertRule(stage="bronze", label="Bronze", expected_seconds=600.0),
        FreshnessAlertRule(stage="silver", label="Silver", expected_seconds=1800.0),
        FreshnessAlertRule(stage="warehouse", label="Warehouse", expected_seconds=3600.0),
        VolumeAnomalyRule(stage="bronze", label="Bronze", baseline_store=baseline_store),
        VolumeAnomalyRule(stage="silver", label="Silver", baseline_store=baseline_store),
        QuarantineAlertRule(warning_threshold=1, critical_threshold=5),
        WarehouseReachabilityRule(),
        InfraServiceRule(service_name="kafka", label="Kafka"),
        InfraServiceRule(service_name="postgres", label="PostgreSQL"),
        PipelineConsistencyRule(),
        AirflowFailureRule(),
        AirflowStalenessRule(threshold_seconds=86400.0),
    ]
    return AlertEngine(rules)

