from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


SEVERITY_ORDER = {
    "INFO": 0,
    "WARNING": 1,
    "CRITICAL": 2,
}


@dataclass(frozen=True)
class Alert:
    id: str
    name: str
    severity: str
    status: str
    message: str
    source: str
    timestamp: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def fingerprint(self) -> tuple[object, ...]:
        return (
            self.id,
            self.severity,
            self.status,
            self.message,
            self.source,
            tuple(sorted(self.metadata.items())),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "severity": self.severity,
            "status": self.status,
            "message": self.message,
            "source": self.source,
            "timestamp": self.timestamp,
            "metadata": self.metadata,
        }

