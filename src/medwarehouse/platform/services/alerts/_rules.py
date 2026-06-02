"""Base class and shared types for all alert rules."""
from __future__ import annotations

from typing import Any

from medwarehouse.platform.models import Alert


class AlertRule:
    def evaluate(self, metrics: dict[str, Any]) -> Alert | None:
        raise NotImplementedError
