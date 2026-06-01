from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from medwarehouse.platform.utils.time import utc_now


class Probe(ABC):
    name = "probe"

    def collect(self) -> dict[str, Any]:
        try:
            payload = self._collect()
        except Exception as exc:
            payload = self._error_payload(exc)
        payload.setdefault("probe", self.name)
        payload.setdefault("checked_at", utc_now())
        return payload

    @abstractmethod
    def _collect(self) -> dict[str, Any]:
        raise NotImplementedError

    def _error_payload(self, exc: Exception) -> dict[str, Any]:
        return {
            "probe": self.name,
            "status": "error",
            "error": str(exc),
        }

