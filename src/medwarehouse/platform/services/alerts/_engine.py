"""AlertEngine: reconciles, deduplicates, and persists alert state."""
from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import replace
from typing import Any, Iterable

from medwarehouse.platform.models import Alert, SEVERITY_ORDER
from medwarehouse.platform.utils.time import utc_now

from medwarehouse.platform.services.alerts._rules import AlertRule

# VolumeAnomalyRule is defined in inventory.py; imported here for the isinstance check.
def _is_volume_anomaly_rule(rule: AlertRule) -> bool:
    return type(rule).__name__ == "VolumeAnomalyRule"


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


class AlertEngine:
    def __init__(self, rules: Iterable[AlertRule], *, recent_limit: int = 30) -> None:
        self._rules = list(rules)
        self._recent_limit = recent_limit
        self._recent_events: deque[Alert] = deque(maxlen=recent_limit)
        self._active_alerts: dict[str, Alert] = {}
        self._baseline_store = next(
            (rule.baseline_store for rule in self._rules if _is_volume_anomaly_rule(rule)),
            None,
        )

    def evaluate(self, metrics: dict[str, Any]) -> list[Alert]:
        candidates = [a for a in (rule.evaluate(metrics) for rule in self._rules) if a]
        self._reconcile(candidates)
        self._record_baselines(metrics)
        return self.active_alerts()

    def active_alerts(self) -> list[Alert]:
        return sorted(
            self._active_alerts.values(),
            key=lambda a: (-SEVERITY_ORDER.get(a.severity, 0), a.timestamp, a.id),
        )

    def recent_events(self) -> list[Alert]:
        return list(reversed(self._recent_events))

    def summary(self) -> dict[str, Any]:
        active = self.active_alerts()
        return {
            "total": len(active),
            "critical": sum(1 for a in active if a.severity == "CRITICAL"),
            "warning":  sum(1 for a in active if a.severity == "WARNING"),
            "info":     sum(1 for a in active if a.severity == "INFO"),
            "sources":  sorted({a.source for a in active}),
        }

    def snapshot(self) -> dict[str, Any]:
        return {
            "active": [a.to_dict() for a in self.active_alerts()],
            "recent": [a.to_dict() for a in self.recent_events()],
            "summary": self.summary(),
        }

    def _reconcile(self, candidates: list[Alert]) -> None:
        now = utc_now()
        candidate_map = {a.id: a for a in candidates}
        for alert_id, candidate in candidate_map.items():
            active = replace(candidate, timestamp=now, status="ACTIVE")
            previous = self._active_alerts.get(alert_id)
            if previous is None or previous.fingerprint() != active.fingerprint():
                self._active_alerts[alert_id] = active
                self._recent_events.append(active)
        for alert_id, previous in list(self._active_alerts.items()):
            if alert_id not in candidate_map:
                resolved = replace(previous, status="RESOLVED", timestamp=now)
                self._recent_events.append(resolved)
                self._active_alerts.pop(alert_id, None)

    def _record_baselines(self, metrics: dict[str, Any]) -> None:
        if self._baseline_store is None:
            return
        for stage in ("bronze", "silver"):
            count = metrics.get("pipeline", {}).get(stage, {}).get("count")
            self._baseline_store.append(f"pipeline:{stage}:count", count)
