"""Small in-memory cache with explicit stale-result support."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import RLock
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class CacheEntry(Generic[T]):
    value: T
    expires_at: datetime
    stale_until: datetime


class InMemoryResultCache:
    """A bounded cache.

    It intentionally has no persistence requirement. Production users can
    replace it with a Redis-backed implementation without changing provider or
    business code, because caching sits behind the hub boundary.
    """

    def __init__(self, max_entries: int = 512) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be at least one")
        self.max_entries = max_entries
        self._entries: OrderedDict[str, CacheEntry[object]] = OrderedDict()
        self._lock = RLock()

    def get_fresh(self, key: str) -> object | None:
        now = _utc_now()
        with self._lock:
            entry = self._entries.get(key)
            # A zero TTL is deliberately never fresh.  Using ``<`` here made
            # it occasionally fresh when two reads landed in the same clock
            # tick, which is especially visible on Windows.
            if entry is None or entry.expires_at <= now:
                return None
            self._entries.move_to_end(key)
            return entry.value

    def get_stale(self, key: str) -> object | None:
        now = _utc_now()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None or entry.stale_until <= now:
                return None
            self._entries.move_to_end(key)
            return entry.value

    def set(self, key: str, value: object, ttl: timedelta, stale_ttl: timedelta) -> None:
        now = _utc_now()
        entry = CacheEntry(
            value=value,
            expires_at=now + max(ttl, timedelta()),
            stale_until=now + max(ttl, timedelta()) + max(stale_ttl, timedelta()),
        )
        with self._lock:
            self._entries[key] = entry
            self._entries.move_to_end(key)
            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)
