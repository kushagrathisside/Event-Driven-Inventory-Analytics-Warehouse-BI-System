"""
Lightweight idempotency cache for stock entry operations.

Stores results keyed by client-supplied `Idempotency-Key` header values.
Entries expire after TTL_SECONDS (default 24 hours). A background daemon
thread runs eviction every EVICTION_INTERVAL_SECONDS to prevent unbounded
memory growth in long-running processes.

For production, replace _store with a Redis or PostgreSQL-backed implementation
that survives process restarts and supports multiple webapp instances.
"""
from __future__ import annotations

import threading
import time
from typing import Any


_TTL_SECONDS = 86_400          # 24 hours
_EVICTION_INTERVAL = 3_600     # evict expired keys every hour


class IdempotencyStore:
    def __init__(
        self,
        *,
        ttl_seconds: float = _TTL_SECONDS,
        eviction_interval: float = _EVICTION_INTERVAL,
    ) -> None:
        self._ttl = ttl_seconds
        self._store: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()
        # Daemon thread so it does not block interpreter shutdown.
        self._eviction_thread = threading.Thread(
            target=self._eviction_loop,
            args=(eviction_interval,),
            daemon=True,
            name="idempotency-eviction",
        )
        self._eviction_thread.start()

    def get(self, key: str) -> Any | None:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            stored_at, result = entry
            if time.monotonic() - stored_at > self._ttl:
                del self._store[key]
                return None
            return result

    def set(self, key: str, result: Any) -> None:
        with self._lock:
            self._store[key] = (time.monotonic(), result)

    def evict_expired(self) -> int:
        now = time.monotonic()
        with self._lock:
            expired = [k for k, (ts, _) in self._store.items() if now - ts > self._ttl]
            for k in expired:
                del self._store[k]
        return len(expired)

    def _eviction_loop(self, interval: float) -> None:
        while True:
            time.sleep(interval)
            self.evict_expired()


_default_store = IdempotencyStore()


def get_idempotency_store() -> IdempotencyStore:
    return _default_store
