from __future__ import annotations

from datetime import timedelta

import pytest

from stock_data_hub.cache import InMemoryResultCache


def test_zero_ttl_is_never_returned_as_fresh() -> None:
    cache = InMemoryResultCache()
    cache.set("quote", "value", timedelta(), timedelta(minutes=1))

    assert cache.get_fresh("quote") is None
    assert cache.get_stale("quote") == "value"


def test_cache_rejects_non_positive_capacity() -> None:
    with pytest.raises(ValueError, match="max_entries"):
        InMemoryResultCache(max_entries=0)
