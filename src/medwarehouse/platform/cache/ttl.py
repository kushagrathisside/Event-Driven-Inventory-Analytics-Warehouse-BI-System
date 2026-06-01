from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class CacheEntry:
    value: Any
    created_at: float


class TTLCache:
    def __init__(self, *, ttl_seconds: float = 60.0) -> None:
        self._ttl_seconds = ttl_seconds
        self._lock = threading.Lock()
        self._entries: dict[str, CacheEntry] = {}

    @property
    def ttl_seconds(self) -> float:
        return self._ttl_seconds

    def get_or_create(
        self,
        key: str,
        builder: Callable[[], Any],
        *,
        force_refresh: bool = False,
    ) -> tuple[Any, bool]:
        with self._lock:
            entry = self._entries.get(key)
            if (
                not force_refresh
                and entry is not None
                and (time.monotonic() - entry.created_at) < self._ttl_seconds
            ):
                return entry.value, True

        value = builder()

        with self._lock:
            self._entries[key] = CacheEntry(value=value, created_at=time.monotonic())
        return value, False

    def invalidate(self, key: str | None = None) -> None:
        with self._lock:
            if key is None:
                self._entries.clear()
                return
            self._entries.pop(key, None)

