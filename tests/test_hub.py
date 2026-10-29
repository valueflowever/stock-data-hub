from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from threading import Event, Lock
from time import sleep

import pytest

from stock_data_hub.errors import AllSourcesFailed, ProviderUnavailable, ResourceBusy
from stock_data_hub.hub import CachePolicy, DataHub, RouteKey
from stock_data_hub.models import (
    Adjust,
    AShareBar,
    AssetType,
    BarDataQuality,
    Capability,
    Interval,
    Market,
    Quote,
    SourcePayload,
)
from stock_data_hub.normalization import BEIJING_TZ
from stock_data_hub.providers.base import MarketDataProvider


class QuoteProvider(MarketDataProvider):
    capabilities = frozenset({Capability.QUOTE})

    def __init__(self, name: str, source: str, fail: bool = False) -> None:
        self.name = name
        self.source = source
        self.fail = fail
        self.calls = 0

    def fetch_quote(self, symbol: str) -> SourcePayload[Quote]:
        self.calls += 1
        if self.fail:
            raise ProviderUnavailable(f"{self.name} is unavailable")
        return SourcePayload(
            value=Quote(
                symbol=symbol,
                price=Decimal("123.45"),
                ts=datetime.now(tz=BEIJING_TZ),
            ),
            source=self.source,
        )


class SlowQuoteProvider(QuoteProvider):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.started = 0
        self.finished = Event()

    def fetch_quote(self, symbol: str) -> SourcePayload[Quote]:
        self.started += 1
        try:
            sleep(0.05)
            return super().fetch_quote(symbol)
        finally:
            self.finished.set()


class USQuoteProvider(QuoteProvider):
    markets = frozenset({Market.US})

    def fetch_quote(self, symbol: str) -> SourcePayload[Quote]:
        payload = super().fetch_quote(symbol)
        return SourcePayload(
            value=Quote(
                symbol=payload.value.symbol,
                price=payload.value.price,
                ts=payload.value.ts,
                market=Market.US,
            ),
            source=payload.source,
        )


class CNQuoteProvider(QuoteProvider):
    markets = frozenset({Market.CN})


class MultiMarketQuoteProvider(QuoteProvider):
    markets = frozenset({Market.CN, Market.US})

    def fetch_quote(self, symbol: str) -> SourcePayload[Quote]:
        self.calls += 1
        market = Market.US if symbol.endswith(".US") else Market.CN
        if market == Market.US:
            raise ProviderUnavailable("US quote upstream is unavailable")
        return SourcePayload(
            value=Quote(
                symbol=symbol,
                price=Decimal("123.45"),
                ts=datetime.now(tz=BEIJING_TZ),
                market=market,
            ),
            source=self.source,
        )


class BarsProvider(MarketDataProvider):
    capabilities = frozenset({Capability.BARS})
    markets = frozenset({Market.CN})
    intervals = frozenset({Interval.DAY_1})
    adjustments = frozenset(Adjust)

    def __init__(self, name: str, *, empty: bool = False) -> None:
        self.name = name
        self.empty = empty
        self.calls = 0

    def fetch_bars(
        self,
        symbol: str,
        interval: Interval,
        start: datetime,
        end: datetime,
        adjust: Adjust,
    ) -> SourcePayload[list[AShareBar]]:
        self.calls += 1
        if self.empty:
            return SourcePayload(value=[], source=f"{self.name}:bars")
        return SourcePayload(
            value=[
                AShareBar(
                    symbol=symbol,
                    ts=datetime(2026, 7, 17, tzinfo=BEIJING_TZ),
                    open=Decimal(10),
                    high=Decimal(11),
                    low=Decimal("9.5"),
                    close=Decimal("10.5"),
                )
            ],
            source=f"{self.name}:bars",
        )


def test_hub_falls_back_to_second_provider() -> None:
    primary = QuoteProvider("primary", "primary:quote", fail=True)
    fallback = QuoteProvider("fallback", "fallback:quote")
    hub = DataHub([primary, fallback], attempts_per_provider=1)

    result = asyncio.run(hub.get_quote("600519"))

    assert result.value.symbol == "600519.SH"
    assert result.metadata.source == "fallback:quote"
    assert result.metadata.attempted_sources == ("primary", "fallback")
    assert primary.calls == 1
    assert fallback.calls == 1
    health = {entry.provider: entry for entry in hub.health()}
    assert health["primary"].failure_count == 1
    assert health["fallback"].success_count == 1


def test_hub_returns_a_fresh_cached_result_without_hitting_provider_twice() -> None:
    provider = QuoteProvider("primary", "primary:quote")
    hub = DataHub([provider], attempts_per_provider=1)

    first = asyncio.run(hub.get_quote("600519"))
    second = asyncio.run(hub.get_quote("600519"))

    assert not first.metadata.cache_hit
    assert second.metadata.cache_hit
    assert not second.metadata.stale
    assert provider.calls == 1


def test_hub_marks_stale_cache_when_live_source_fails() -> None:
    provider = QuoteProvider("primary", "primary:quote")
    hub = DataHub(
        [provider],
        attempts_per_provider=1,
        cache_policies={
            Capability.QUOTE: CachePolicy(
                ttl=timedelta(seconds=-1),
                stale_ttl=timedelta(minutes=1),
            )
        },
    )

    asyncio.run(hub.get_quote("600519"))
    provider.fail = True
    result = asyncio.run(hub.get_quote("600519"))

    assert result.metadata.cache_hit
    assert result.metadata.stale
    assert provider.calls == 2
    assert "All live sources failed" in result.metadata.warnings[-1]


def test_open_circuit_skips_repeat_attempts() -> None:
    provider = QuoteProvider("primary", "primary:quote", fail=True)
    hub = DataHub(
        [provider],
        attempts_per_provider=1,
        failure_threshold=1,
        circuit_cooldown=timedelta(minutes=1),
    )

    with pytest.raises(AllSourcesFailed):
        asyncio.run(hub.get_quote("600519"))
    with pytest.raises(AllSourcesFailed):
        asyncio.run(hub.get_quote("600519"))

    assert provider.calls == 1
    assert hub.health()[0].status == "open"


def test_circuit_breaker_is_isolated_by_market_route() -> None:
    provider = MultiMarketQuoteProvider("multi", "multi:quote")
    hub = DataHub(
        [provider],
        attempts_per_provider=1,
        failure_threshold=1,
        circuit_cooldown=timedelta(minutes=1),
    )

    with pytest.raises(AllSourcesFailed):
        asyncio.run(hub.get_quote("AAPL.US"))
    china = asyncio.run(hub.get_quote("600519"))

    health = {entry.route: entry for entry in hub.health()}
    assert china.value.market == Market.CN
    assert health["quote:US:stock"].status == "open"
    assert health["quote:CN:stock"].status == "closed"
    assert provider.calls == 2


def test_hub_times_out_a_stuck_provider() -> None:
    provider = SlowQuoteProvider("slow", "slow:quote")
    hub = DataHub(
        [provider],
        attempts_per_provider=1,
        provider_timeout_seconds=0.01,
    )

    with pytest.raises(AllSourcesFailed) as raised:
        asyncio.run(hub.get_quote("600519"))

    assert "timed out" in raised.value.errors["slow"]


def test_capability_timeout_override_takes_precedence() -> None:
    provider = SlowQuoteProvider("slow", "slow:quote")
    hub = DataHub(
        [provider],
        attempts_per_provider=1,
        provider_timeout_seconds=0.01,
        provider_timeout_overrides={Capability.QUOTE: 0.2},
    )

    result = asyncio.run(hub.get_quote("600519"))

    assert result.metadata.source == "slow:quote"


def test_capability_timeout_override_must_be_positive() -> None:
    with pytest.raises(ValueError, match="timeout overrides"):
        DataHub(provider_timeout_overrides={Capability.SNAPSHOT: 0})


def test_hub_does_not_retry_a_timed_out_sdk_thread() -> None:
    provider = SlowQuoteProvider("slow", "slow:quote")
    hub = DataHub(
        [provider],
        attempts_per_provider=3,
        provider_timeout_seconds=0.01,
        retry_delay_seconds=0,
    )

    with pytest.raises(AllSourcesFailed):
        asyncio.run(hub.get_quote("600519"))

    assert provider.started == 1
    assert provider.finished.wait(1)
    assert provider.calls == 1


def test_hub_coalesces_concurrent_identical_requests() -> None:
    provider = SlowQuoteProvider("slow", "slow:quote")
    hub = DataHub([provider], attempts_per_provider=1, provider_timeout_seconds=1)

    async def fetch_all() -> list[object]:
        return await asyncio.gather(*(hub.get_quote("600519") for _ in range(5)))

    results = asyncio.run(fetch_all())

    assert len(results) == 5
    assert provider.calls == 1


def test_hub_falls_back_when_a_source_returns_empty_bars_on_a_weekday() -> None:
    empty = BarsProvider("empty", empty=True)
    fallback = BarsProvider("fallback")
    hub = DataHub([empty, fallback], attempts_per_provider=1)
    day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)

    result = asyncio.run(hub.get_bars("600519", Interval.DAY_1, day, day, Adjust.QFQ))

    assert result.metadata.source == "fallback:bars"
    assert result.metadata.attempted_sources == ("empty", "fallback")


def test_hub_accepts_empty_bars_for_a_weekend_window() -> None:
    provider = BarsProvider("empty", empty=True)
    hub = DataHub([provider], attempts_per_provider=1)
    start = datetime(2026, 7, 18, tzinfo=BEIJING_TZ)
    end = datetime(2026, 7, 19, 23, 59, tzinfo=BEIJING_TZ)

    result = asyncio.run(hub.get_bars("600519", Interval.DAY_1, start, end, Adjust.QFQ))

    assert result.value == []
    assert result.metadata.source == "empty:bars"


def test_hub_returns_empty_after_all_sources_confirm_no_weekday_bars() -> None:
    first = BarsProvider("first", empty=True)
    second = BarsProvider("second", empty=True)
    hub = DataHub([first, second], attempts_per_provider=2)
    day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)

    result = asyncio.run(hub.get_bars("600519", Interval.DAY_1, day, day))

    assert result.value == []
    assert result.metadata.attempted_sources == ("first", "second")
    assert "All eligible live sources returned no bars" in result.metadata.warnings[-1]
    assert first.calls == 1
    assert second.calls == 1


def test_route_key_prioritizes_the_asset_specific_provider() -> None:
    general = BarsProvider("general")
    etf = BarsProvider("etf")
    hub = DataHub(
        [general, etf],
        attempts_per_provider=1,
        source_priority={
            RouteKey(Capability.BARS, Market.CN, AssetType.ETF, Interval.DAY_1): (
                "etf",
                "general",
            )
        },
    )
    day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)

    result = asyncio.run(hub.get_bars("510300", Interval.DAY_1, day, day))

    assert result.metadata.source == "etf:bars"
    assert etf.calls == 1
    assert general.calls == 0


def test_hub_routes_global_symbol_only_to_a_supporting_provider() -> None:
    china = CNQuoteProvider("china", "china:quote")
    us = USQuoteProvider("us", "us:quote")
    hub = DataHub([china, us], attempts_per_provider=1)

    result = asyncio.run(hub.get_quote("AAPL"))

    assert result.value.symbol == "AAPL.US"
    assert result.value.market == Market.US
    assert result.metadata.attempted_sources == ("us",)
    assert china.calls == 0


class BlockingQuoteProvider(QuoteProvider):
    def __init__(self) -> None:
        super().__init__("blocked", "blocked:quote")
        self.started = Event()
        self.release = Event()
        self.finished = Event()
        self.lock = Lock()
        self.running = 0

    def fetch_quote(self, symbol: str) -> SourcePayload[Quote]:
        with self.lock:
            self.running += 1
        self.started.set()
        try:
            assert self.release.wait(3)
            return super().fetch_quote(symbol)
        finally:
            with self.lock:
                self.running -= 1
                if self.running == 0:
                    self.finished.set()


async def _wait_for_event(event: Event) -> None:
    async def poll() -> None:
        while not event.is_set():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(poll(), timeout=2)


def test_blocked_sdk_slots_do_not_block_fallback_or_trip_its_circuit() -> None:
    primary = BlockingQuoteProvider()
    fallback = QuoteProvider("fallback", "fallback:quote")
    hub = DataHub(
        [primary, fallback],
        max_concurrent_requests=2,
        attempts_per_provider=1,
        provider_timeout_seconds=0.1,
        failure_threshold=100,
    )

    async def run() -> None:
        try:
            first = await asyncio.gather(hub.get_quote("600519"), hub.get_quote("600000"))
            later = [await hub.get_quote(symbol) for symbol in ("600001", "600002", "600003")]
            assert all(item.metadata.source == "fallback:quote" for item in [*first, *later])
            health = {item.provider: item for item in hub.health()}
            assert health["blocked"].timeout_count == 2
            assert health["blocked"].failure_count == 2
            assert health["blocked"].capacity_rejections == 3
            assert health["fallback"].failure_count == 0
            assert health["fallback"].status == "closed"
            assert hub.metrics()["active_provider_calls"]["blocked"] == 2
            assert hub.metrics()["fallbacks"] == 5
        finally:
            primary.release.set()
            await _wait_for_event(primary.finished)

    asyncio.run(run())


def test_cancelling_first_coalesced_waiter_does_not_cancel_the_other_waiter() -> None:
    provider = BlockingQuoteProvider()
    hub = DataHub([provider], attempts_per_provider=1)

    async def run() -> None:
        first = asyncio.create_task(hub.get_quote("600519"))
        second = None
        try:
            await _wait_for_event(provider.started)
            second = asyncio.create_task(hub.get_quote("600519.SH"))
            await asyncio.sleep(0)
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
            provider.release.set()
            result = await asyncio.wait_for(second, 2)
            cached = await hub.get_quote("600519")
            assert result.value == cached.value
            assert cached.metadata.cache_hit
            assert provider.calls == 1
            assert hub.metrics()["coalesced_requests"] == 1
        finally:
            provider.release.set()
            await asyncio.gather(
                first, *([second] if second is not None else []), return_exceptions=True
            )

    asyncio.run(run())


def test_distinct_request_limit_allows_existing_coalesced_waiters() -> None:
    provider = BlockingQuoteProvider()
    hub = DataHub([provider], max_inflight_requests=1)

    async def run() -> None:
        first = asyncio.create_task(hub.get_quote("600519"))
        second = None
        try:
            await _wait_for_event(provider.started)
            with pytest.raises(ResourceBusy):
                await hub.get_quote("600000")
            second = asyncio.create_task(hub.get_quote("600519"))
            await asyncio.sleep(0)
            provider.release.set()
            assert (await first).value == (await second).value
            assert hub.metrics()["failures"] == 1
        finally:
            provider.release.set()
            await asyncio.gather(
                first, *([second] if second is not None else []), return_exceptions=True
            )

    asyncio.run(run())


class PartialBarsProvider(BarsProvider):
    def __init__(self, *, filtered: bool = False) -> None:
        super().__init__("partial")
        self.filtered = filtered

    def fetch_bars(self, symbol, interval, start, end, adjust):
        payload = super().fetch_bars(symbol, interval, start, end, adjust)
        return replace(
            payload,
            quality=BarDataQuality(
                start,
                end,
                payload.value[0].ts,
                payload.value[-1].ts,
                2 if self.filtered else 1,
                1,
                filtered_rows=int(self.filtered),
                truncated=not self.filtered,
            ),
        )


@pytest.mark.parametrize("filtered", [False, True])
def test_partial_history_prefers_a_complete_fallback(filtered: bool) -> None:
    partial = PartialBarsProvider(filtered=filtered)
    complete = BarsProvider("complete")
    hub = DataHub([partial, complete], attempts_per_provider=1)
    day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)

    result = asyncio.run(hub.get_bars("600519", "1d", day, day))

    assert result.metadata.source == "complete:bars"
    assert not result.metadata.quality.truncated
    assert result.metadata.quality.filtered_rows == 0
    assert partial.calls == complete.calls == 1


def test_partial_history_is_reported_and_does_not_become_verified_cache() -> None:
    partial = PartialBarsProvider()
    hub = DataHub([partial], attempts_per_provider=1)
    day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)

    for _ in range(2):
        result = asyncio.run(hub.get_bars("600519", "1d", day, day))
        assert result.metadata.quality.truncated
        assert result.metadata.quality.missing_rows is None
        assert not result.metadata.cache_hit
        assert "No complete fresh source" in result.metadata.warnings[-1]
    assert partial.calls == 2


def test_an_empty_source_and_a_failed_source_do_not_confirm_empty_history() -> None:
    class FailedBarsProvider(BarsProvider):
        def fetch_bars(self, *args, **kwargs):
            raise ProviderUnavailable("upstream failed")

    hub = DataHub(
        [BarsProvider("empty", empty=True), FailedBarsProvider("failed")], attempts_per_provider=1
    )
    day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)
    with pytest.raises(AllSourcesFailed):
        asyncio.run(hub.get_bars("600519", "1d", day, day))


@pytest.mark.parametrize("filtered", [False, True])
def test_unusable_partial_history_is_not_a_confirmed_empty_result(filtered: bool) -> None:
    class UnusableHistory(BarsProvider):
        def fetch_bars(self, symbol, interval, start, end, adjust):
            return SourcePayload(
                [],
                quality=BarDataQuality(
                    start,
                    end,
                    None,
                    None,
                    1,
                    0,
                    filtered_rows=int(filtered),
                    truncated=not filtered,
                ),
            )

    hub = DataHub([UnusableHistory("bad-history")], attempts_per_provider=1)
    day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)
    with pytest.raises(AllSourcesFailed, match="no usable bars"):
        asyncio.run(hub.get_bars("600519", "1d", day, day))


def test_original_fetch_age_shortens_cache_lifetime(monkeypatch) -> None:
    import stock_data_hub.cache as cache_module
    import stock_data_hub.hub as hub_module

    class Clock(datetime):
        current = datetime(2026, 7, 17, 10, tzinfo=BEIJING_TZ)

        @classmethod
        def now(cls, tz=None):
            return cls.current.astimezone(tz)

    original = Clock.current - timedelta(seconds=2)

    class AgedProvider(QuoteProvider):
        def fetch_quote(self, symbol):
            return replace(super().fetch_quote(symbol), fetched_at=original, cache_hit=True)

    monkeypatch.setattr(hub_module, "datetime", Clock)
    monkeypatch.setattr(cache_module, "_utc_now", lambda: Clock.current)
    provider = AgedProvider("aged", "aged:quote")
    hub = DataHub([provider], attempts_per_provider=1)

    first = asyncio.run(hub.get_quote("600519"))
    assert first.metadata.fetched_at == original
    assert first.metadata.received_at == Clock.current
    assert first.metadata.age_seconds == 2
    assert first.metadata.cache_hit
    assert not first.metadata.stale
    Clock.current += timedelta(milliseconds=500)
    assert asyncio.run(hub.get_quote("600519")).metadata.age_seconds == 2.5
    assert provider.calls == 1
    Clock.current += timedelta(seconds=1)
    result = asyncio.run(hub.get_quote("600519"))
    assert provider.calls == 2
    assert result.metadata.stale
    assert result.metadata.fetched_at == original
    assert result.metadata.age_seconds == 3.5


def test_peer_timeout_budget_is_separate_from_the_local_budget() -> None:
    primary = BlockingQuoteProvider()
    peer = SlowQuoteProvider("peer", "peer:quote")
    peer.is_peer = True
    hub = DataHub(
        [primary, peer],
        attempts_per_provider=1,
        provider_timeout_seconds=1,
        local_timeout_seconds=0.02,
        peer_timeout_seconds=1,
        request_timeout_seconds=2,
    )
    try:
        result = asyncio.run(hub.get_quote("600519"))
        assert result.metadata.source == "peer:quote"
        assert result.metadata.attempted_sources == ("blocked", "peer")
    finally:
        primary.release.set()
        assert primary.finished.wait(2)
