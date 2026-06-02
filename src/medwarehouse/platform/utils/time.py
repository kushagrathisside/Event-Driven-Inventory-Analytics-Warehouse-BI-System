from __future__ import annotations

from datetime import datetime, timezone


def _now_utc_dt() -> datetime:
    """Internal helper returning a timezone-aware datetime. Use utc_now() for the public API."""
    return datetime.now(timezone.utc)


def utc_now() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return _now_utc_dt().isoformat()


def safe_isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def lag_seconds(value: str | None, *, reference: datetime | None = None) -> float | None:
    parsed = parse_timestamp(value)
    if parsed is None:
        return None
    ref = reference or _now_utc_dt()
    return max(0.0, (ref - parsed).total_seconds())
