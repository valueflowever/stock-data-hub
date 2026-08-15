from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
from time import sleep

import pytest

from stock_data_hub.errors import AllSourcesFailed, ProviderUnavailable
from stock_data_hub.hub import CachePolicy, DataHub, RouteKey
from stock_data_hub.models import (
    Adjust,
    AShareBar,
    AssetType,
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
    def fetch_quote(self, symbol: str) -> SourcePayload[Quote]:
        sleep(0.05)
        return super().fetch_quote(symbol)


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
