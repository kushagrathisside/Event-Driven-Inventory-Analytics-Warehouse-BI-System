"""Alert rules for inventory pipeline freshness, volume, and consistency."""
from __future__ import annotations

from typing import Any

from medwarehouse.platform.models import Alert
from medwarehouse.platform.utils.time import lag_seconds, utc_now
from medwarehouse.platform.services.alerts._rules import AlertRule
from medwarehouse.platform.services.alerts._engine import VolumeBaselineStore  # canonical definition


class FreshnessAlertRule(AlertRule):
    """
    Unified freshness alert for both the legacy inventory stages
    (reads from metrics.pipeline.<stage>) and all three Kafka domain stages
    (reads from metrics.pipeline.domains.<domain>.<stage>).

    Pass `domain=None` (default) for the inventory legacy path.
    Pass `domain="procurement"` / `domain="sales"` for the per-domain path.
    """

    def __init__(
        self,
        *,
        stage: str,
        label: str,
        expected_seconds: float,
        domain: str | None = None,
    ) -> None:
        self._stage = stage
        self._label = label
        self._expected_seconds = expected_seconds
        self._domain = domain
        self._alert_id = f"freshness:{domain}:{stage}" if domain else f"freshness:{stage}"
        self._source = domain or stage

    def evaluate(self, metrics: dict[str, Any]) -> Alert | None:
        pipeline = metrics.get("pipeline", {})
        if self._domain:
            stage_metrics = pipeline.get("domains", {}).get(self._domain, {}).get(self._stage, {})
        else:
            stage_metrics = pipeline.get(self._stage, {})

        last_update = stage_metrics.get("last_update")
        lag = lag_seconds(last_update)
        if last_update is None or lag is None:
            return None

        if lag > self._expected_seconds * 5:
            severity = "CRITICAL"
        elif lag > self._expected_seconds * 2:
            severity = "WARNING"
        else:
            return None

        return Alert(
            id=self._alert_id,
            name=f"{self._label} Freshness",
            severity=severity,
            status="ACTIVE",
            message=(
                f"{self._label} has not updated for {int(lag)} seconds. "
                f"Expected within {int(self._expected_seconds)} seconds."
            ),
            source=self._source,
            timestamp=utc_now(),
            metadata={
                "stage": self._stage,
                "domain": self._domain,
                "lag_seconds": lag,
                "expected_seconds": self._expected_seconds,
                "last_update": last_update,
            },
        )


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

    @property
    def baseline_store(self) -> VolumeBaselineStore:
        return self._baseline_store

    def evaluate(self, metrics: dict[str, Any]) -> Alert | None:
        current = metrics.get("pipeline", {}).get(self._stage, {}).get("count")
        if current is None:
            return None
        key = f"pipeline:{self._stage}:count"
        average = self._baseline_store.average(key)
        if average is None or self._baseline_store.sample_count(key) < self._minimum_samples:
            return None
        if average <= 0:
            return None

        if current <= average * 0.2:
            severity = "CRITICAL"
        elif current <= average * 0.5:
            severity = "WARNING"
        else:
            return None

        return Alert(
            id=f"volume:{self._stage}",
            name=f"{self._label} Volume Drop",
            severity=severity,
            status="ACTIVE",
            message=f"{self._label} volume dropped to {current} against a rolling baseline of {average:.1f}.",
            source=self._stage,
            timestamp=utc_now(),
            metadata={"stage": self._stage, "current_count": current, "baseline_average": average},
        )


class QuarantineAlertRule(AlertRule):
    def __init__(self, *, warning_threshold: int = 1, critical_threshold: int = 5) -> None:
        self._warning = warning_threshold
        self._critical = critical_threshold

    def evaluate(self, metrics: dict[str, Any]) -> Alert | None:
        quarantine = (metrics.get("pipeline", {}).get("silver", {}).get("quarantine", 0) or 0)
        if quarantine >= self._critical:
            severity = "CRITICAL"
        elif quarantine >= self._warning:
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
            metadata={"quarantine_count": quarantine,
                      "warning_threshold": self._warning, "critical_threshold": self._critical},
        )


class PipelineConsistencyRule(AlertRule):
    def evaluate(self, metrics: dict[str, Any]) -> Alert | None:
        bronze = metrics.get("pipeline", {}).get("bronze", {})
        silver = metrics.get("pipeline", {}).get("silver", {})
        bronze_count = bronze.get("count", 0) or 0
        silver_count = silver.get("count", 0) or 0
        if bronze_count <= 0 or bronze_count - silver_count <= 0:
            return None
        gap = bronze_count - silver_count
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
            metadata={"bronze_count": bronze_count, "silver_count": silver_count, "gap": gap},
        )
