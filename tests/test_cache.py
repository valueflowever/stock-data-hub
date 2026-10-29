from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event

import pytest

from stock_data_hub.cache import InMemoryResultCache, SharedFetchCache


def test_zero_ttl_is_never_returned_as_fresh() -> None:
    cache = InMemoryResultCache()
    cache.set("quote", "value", timedelta(), timedelta(minutes=1))

    assert cache.get_fresh("quote") is None
    assert cache.get_stale("quote") == "value"


def test_cache_rejects_non_positive_capacity() -> None:
    with pytest.raises(ValueError, match="max_entries"):
        InMemoryResultCache(max_entries=0)


def test_shared_fetch_coalesces_and_preserves_original_acquisition_time() -> None:
    cache = SharedFetchCache()
    started, release = Event(), Event()
    calls = 0

    def loader() -> object:
        nonlocal calls
        calls += 1
        started.set()
        assert release.wait(3)
        return object()

    with ThreadPoolExecutor(max_workers=2) as pool:
        owner = pool.submit(cache.get, "snapshot", loader)
        try:
            assert started.wait(2)
            waiter = pool.submit(cache.get, "snapshot", loader)
        finally:
            release.set()
        first, second = owner.result(timeout=2), waiter.result(timeout=2)
    third = cache.get("snapshot", loader)
    assert first.value is second.value is third.value
    assert first.fetched_at == second.fetched_at == third.fetched_at
    assert not first.cache_hit
    assert second.cache_hit and third.cache_hit
    assert calls == 1
    cache.clear()
    assert cache.get("snapshot", loader).value is not first.value
    assert calls == 2


def test_failed_shared_refresh_does_not_cache_the_failure() -> None:
    cache = SharedFetchCache()

    def fail():
        raise RuntimeError("temporary outage")

    with pytest.raises(RuntimeError, match="temporary outage"):
        cache.get("snapshot", fail)
    assert cache.get("snapshot", lambda: "recovered").value == "recovered"
