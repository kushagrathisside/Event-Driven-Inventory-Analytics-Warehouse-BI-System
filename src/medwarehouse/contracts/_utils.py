"""Utilities shared across all domain event contracts."""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import NAMESPACE_URL, uuid5


def to_utc_iso(timestamp: datetime) -> str:
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc).isoformat()


def deterministic_uuid(seed: str, name: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"{seed}:{name}"))
