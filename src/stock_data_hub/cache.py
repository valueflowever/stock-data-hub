"""Small in-memory cache with explicit stale-result support."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from math import isfinite
from threading import RLock
from time import monotonic
from typing import Generic, TypeVar

from stock_data_hub._workers import remaining_timeout

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


@dataclass(frozen=True)
class CachedFetch(Generic[T]):
    value: T
    fetched_at: datetime
    cache_hit: bool = False


class SharedFetchCache:
    """Short-lived SDK snapshots shared by quote, snapshot, and instrument requests.

    Fetches for different keys are independent; identical concurrent fetches
    share one result, and failed refreshes never promote old data to fresh.
    """

    def __init__(self, ttl_seconds: float = 3, max_entries: int = 16) -> None:
        if not isfinite(ttl_seconds) or ttl_seconds < 0 or max_entries < 1:
            raise ValueError("Invalid shared-fetch cache limits")
        self.ttl_seconds = ttl_seconds
        self.max_entries = max_entries
        self._values: OrderedDict[str, tuple[float, CachedFetch]] = OrderedDict()
        self._inflight: dict[str, Future] = {}
        self._fetch_lock = RLock()

    def get(self, key: str, loader: Callable[[], T]) -> CachedFetch[T]:
        with self._fetch_lock:
            cached = self._values.get(key)
            if cached is not None and cached[0] > monotonic():
                self._values.move_to_end(key)
                return replace(cached[1], cache_hit=True)
            pending = self._inflight.get(key)
            owner = pending is None
            if owner:
                pending = Future()
                self._inflight[key] = pending
        assert pending is not None
        if not owner:
            return replace(pending.result(timeout=remaining_timeout(20.0)), cache_hit=True)
        try:
            result = CachedFetch(loader(), _utc_now())
            with self._fetch_lock:
                self._values[key] = (monotonic() + self.ttl_seconds, result)
                self._values.move_to_end(key)
                while len(self._values) > self.max_entries:
                    self._values.popitem(last=False)
            pending.set_result(result)
            return result
        except BaseException as exc:
            pending.set_exception(exc)
            raise
        finally:
            with self._fetch_lock:
                self._inflight.pop(key, None)

    def clear(self) -> None:
        with self._fetch_lock:
            self._values.clear()
