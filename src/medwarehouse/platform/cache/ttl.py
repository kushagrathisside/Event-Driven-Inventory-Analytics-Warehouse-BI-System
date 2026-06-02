from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class _CacheEntry:
    value: Any
    created_at: float


class TTLCache:
    """
    Thread-safe TTL cache with protection against concurrent stampedes.

    When multiple threads attempt to build the same key simultaneously (e.g.
    after a cache miss or forced refresh), only the first thread runs the
    builder.  All subsequent threads wait for the in-progress build to finish
    and then receive the same result rather than each launching their own
    expensive build in parallel.
    """

    def __init__(self, *, ttl_seconds: float = 60.0) -> None:
        self._ttl = ttl_seconds
        self._lock = threading.Lock()
        self._entries: dict[str, _CacheEntry] = {}
        self._in_progress: dict[str, threading.Event] = {}

    @property
    def ttl_seconds(self) -> float:
        return self._ttl

    def get_or_create(
        self,
        key: str,
        builder: Callable[[], Any],
        *,
        force_refresh: bool = False,
    ) -> tuple[Any, bool]:
        """Return (value, served_from_cache).

        Exactly one thread runs builder() per concurrent miss; all other
        threads wait for that single result via a per-key threading.Event.
        """
        while True:
            with self._lock:
                entry = self._entries.get(key)
                valid = (
                    not force_refresh
                    and entry is not None
                    and (time.monotonic() - entry.created_at) < self._ttl
                )
                if valid:
                    return entry.value, True

                event = self._in_progress.get(key)
                if event is not None:
                    waiting_event = event
                else:
                    # We are the builder for this key.
                    build_event = threading.Event()
                    self._in_progress[key] = build_event
                    waiting_event = None
                    break

            # Another thread is building — wait outside the lock, then retry.
            waiting_event.wait(timeout=30.0)
            force_refresh = False

        # We are the designated builder.
        try:
            value = builder()
            with self._lock:
                self._entries[key] = _CacheEntry(value=value, created_at=time.monotonic())
            return value, False
        finally:
            with self._lock:
                build_event.set()
                self._in_progress.pop(key, None)

    def invalidate(self, key: str | None = None) -> None:
        with self._lock:
            if key is None:
                self._entries.clear()
            else:
                self._entries.pop(key, None)
