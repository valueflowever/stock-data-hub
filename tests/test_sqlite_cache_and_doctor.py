"""Tests for optional SQLiteResultCache persistence and ``stock-data-hub doctor``."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from time import sleep

import pytest

import stock_data_hub.cli as cli_module
from stock_data_hub.doctor import ProbeSpec, run_doctor
from stock_data_hub.errors import ProviderUnavailable
from stock_data_hub.hub import CachePolicy, DataHub, RouteKey
from stock_data_hub.models import (
    Adjust,
    AShareBar,
    AssetType,
    BarDataQuality,
    Capability,
    CompanyProfile,
    DailyClose,
    DataResult,
    Disclosure,
    FetchMetadata,
    Instrument,
    Interval,
    Market,
    Quote,
    QuoteFreshness,
    RestrictedRelease,
    Snapshot,
    SnapshotRow,
    SourcePayload,
    TradeCalendar,
)
from stock_data_hub.normalization import BEIJING_TZ
from stock_data_hub.providers.base import MarketDataProvider
from stock_data_hub.providers.defaults import create_default_hub
from stock_data_hub.sqlite_cache import SQLiteResultCache


def _sample_metadata(*, quality: BarDataQuality | None = None) -> FetchMetadata:
    now = datetime(2026, 7, 17, 15, 30, tzinfo=BEIJING_TZ)
    return FetchMetadata(
        source="fixture:source",
        fetched_at=now,
        received_at=now + timedelta(seconds=1),
        age_seconds=1.0,
        latency_ms=12.5,
        attempted_sources=("primary", "fixture"),
        cache_hit=False,
        stale=False,
        warnings=("sample warning",),
        quality=quality,
    )


def test_sqlite_cache_roundtrips_all_contract_payloads(tmp_path: Path) -> None:
    db_path = tmp_path / "cache" / "hub.db"
    day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)
    quality = BarDataQuality(
        requested_start=day,
        requested_end=day,
        available_start=day,
        available_end=day,
        input_rows=1,
        returned_rows=1,
        filtered_rows=0,
        duplicate_rows=0,
        missing_rows=None,
        truncated=False,
        volume_unit="shares",
        currency="CNY",
    )
    samples: dict[str, DataResult[object]] = {
        "bars": DataResult(
            [
                AShareBar(
                    symbol="600519.SH",
                    ts=day,
                    open=Decimal("1400.5"),
                    high=Decimal("1420.0"),
                    low=Decimal("1395.0"),
                    close=Decimal("1415.25"),
                    volume=Decimal(500_000),
                    amount=Decimal("707625000.0"),
                    is_closed=True,
                    market=Market.CN,
                )
            ],
            _sample_metadata(quality=quality),
        ),
        "empty_bars": DataResult([], _sample_metadata()),
        "quote": DataResult(
            Quote("00700.HK", Decimal("480.2"), day, Market.HK, QuoteFreshness.DELAYED),
            _sample_metadata(),
        ),
        "snapshot": DataResult(
            Snapshot(
                rows=(
                    SnapshotRow(
                        "600519.SH",
                        "贵州茅台",
                        Decimal("1415.25"),
                        Decimal(707625000),
                        Decimal(1780000000000),
                        Decimal("0.42"),
                        Decimal("1.05"),
                    ),
                ),
                has_market_cap=True,
                fetched_at=day,
            ),
            _sample_metadata(),
        ),
        "instruments": DataResult(
            [Instrument("510300.SH", "沪深300ETF", Market.CN, AssetType.ETF)],
            _sample_metadata(),
        ),
        "calendar": DataResult(
            TradeCalendar(
                frozenset({date(2026, 7, 16), date(2026, 7, 17)}),
                date(2026, 7, 16),
                date(2026, 7, 17),
                day,
            ),
            _sample_metadata(),
        ),
        "profile": DataResult(
            CompanyProfile("600519.SH", "白酒", "茅台酒生产销售", None),
            _sample_metadata(),
        ),
        "releases": DataResult(
            [
                RestrictedRelease(
                    "600519.SH",
                    date(2026, 8, 1),
                    Decimal(10000),
                    Decimal(14000000),
                    Decimal("0.01"),
                    "首发原股东限售股份",
                )
            ],
            _sample_metadata(),
        ),
        "disclosures": DataResult(
            [Disclosure("600519.SH", date(2026, 7, 17), "2026年半年度报告")],
            _sample_metadata(),
        ),
        "us_closes": DataResult(
            [DailyClose("AAPL", date(2026, 7, 17), Decimal("215.75"))],
            _sample_metadata(),
        ),
    }

    with SQLiteResultCache(db_path) as writer:
        for key, result in samples.items():
            writer.set(key, result, timedelta(minutes=5), timedelta(hours=1))
        assert writer.stats() == {"total": len(samples), "fresh": len(samples), "stale": 0}

    # Re-open from a brand new instance to verify true disk persistence.
    with SQLiteResultCache(db_path) as reader:
        for key, expected in samples.items():
            assert reader.get_fresh(key) == expected


def test_sqlite_cache_supports_script_rerun_hit_and_offline_stale_fallback(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "persistent.db"
    day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)

    class ToggleBarsProvider(MarketDataProvider):
        name = "primary"
        capabilities = frozenset({Capability.BARS})
        markets = frozenset({Market.CN})

        def __init__(self, *, fail: bool = False) -> None:
            self.fail = fail
            self.calls = 0

        def fetch_bars(self, symbol, interval, start, end, adjust):
            self.calls += 1
            if self.fail:
                raise ProviderUnavailable("offline")
            return SourcePayload(
                [
                    AShareBar(
                        symbol=symbol,
                        ts=day,
                        open=Decimal(100),
                        high=Decimal(105),
                        low=Decimal(98),
                        close=Decimal(103),
                    )
                ],
                source="primary:bars",
            )

    # Run 1: fetches live data and persists to SQLite.
    p1 = ToggleBarsProvider(fail=False)
    hub1 = DataHub(
        [p1],
        cache=SQLiteResultCache(db_path),
        cache_policies={Capability.BARS: CachePolicy(timedelta(minutes=10), timedelta(days=3))},
        attempts_per_provider=1,
    )
    first = asyncio.run(hub1.get_bars("600519", Interval.DAY_1, day, day))
    hub1.close()
    assert p1.calls == 1
    assert not first.metadata.cache_hit

    # Run 2 (new Hub + new SQLite connection): fresh cache hit, 0 network calls!
    p2 = ToggleBarsProvider(fail=True)
    hub2 = DataHub(
        [p2],
        cache=SQLiteResultCache(db_path),
        cache_policies={Capability.BARS: CachePolicy(timedelta(minutes=10), timedelta(days=3))},
        attempts_per_provider=1,
    )
    second = asyncio.run(hub2.get_bars("600519", Interval.DAY_1, day, day))
    hub2.close()
    assert p2.calls == 0
    assert second.metadata.cache_hit
    assert not second.metadata.stale
    assert second.value == first.value

    # Run 3: simulate fresh TTL expiry while network is down -> stale disk fallback!
    conn = sqlite3.connect(str(db_path))
    conn.execute("UPDATE cache_entries SET expires_at = 0")
    conn.commit()
    conn.close()

    p3 = ToggleBarsProvider(fail=True)
    hub3 = DataHub(
        [p3],
        cache=SQLiteResultCache(db_path),
        cache_policies={Capability.BARS: CachePolicy(timedelta(minutes=10), timedelta(days=3))},
        attempts_per_provider=1,
    )
    third = asyncio.run(hub3.get_bars("600519", Interval.DAY_1, day, day))
    hub3.close()
    assert p3.calls == 1
    assert third.metadata.cache_hit
    assert third.metadata.stale
    assert "All live sources failed" in third.metadata.warnings[-1]
    assert third.value == first.value


def test_sqlite_cache_eviction_zero_ttl_and_schema_isolation(tmp_path: Path) -> None:
    db_path = tmp_path / "evict.db"
    with SQLiteResultCache(db_path, max_entries=2) as cache:
        cache.set("zero", "stale-only", timedelta(), timedelta(minutes=1))
        assert cache.get_fresh("zero") is None
        assert cache.get_stale("zero") == "stale-only"

        cache.set("k1", "v1", timedelta(minutes=5), timedelta(minutes=5))
        cache.set("k2", "v2", timedelta(minutes=5), timedelta(minutes=5))
        # "zero" is oldest and evicted because max_entries=2.
        assert cache.get_stale("zero") is None
        assert cache.get_fresh("k1") == "v1"
        assert cache.get_fresh("k2") == "v2"

        # Incompatible schema versions are rejected and pruned cleanly.
        conn = sqlite3.connect(str(db_path))
        conn.execute(
            "UPDATE cache_entries SET schema_version = 'legacy:1.0' WHERE cache_key = 'k1'"
        )
        conn.commit()
        conn.close()
        assert cache.get_fresh("k1") is None
        assert cache.prune_expired() == 0

    hub = create_default_hub(cache_path=tmp_path / "default.db", disabled_providers=["yfinance"])
    assert isinstance(hub._cache, SQLiteResultCache)
    hub.close()
    with pytest.raises(ValueError, match="either cache or cache_path"):
        create_default_hub(cache=SQLiteResultCache(":memory:"), cache_path=tmp_path / "both.db")


def test_doctor_probes_providers_and_recommends_optimal_config(
    monkeypatch, capsys: pytest.CaptureFixture[str]
) -> None:
    reference = datetime(2026, 7, 17, 15, 30, tzinfo=BEIJING_TZ)
    bar_day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)
    bar_15m = datetime(2026, 7, 17, 15, 0, tzinfo=BEIJING_TZ)

    class FastProvider(MarketDataProvider):
        name = "fast_src"
        capabilities = frozenset({Capability.BARS, Capability.QUOTE})
        markets = frozenset({Market.CN})
        intervals = frozenset({Interval.DAY_1, Interval.MINUTE_15})
        adjustments = frozenset({Adjust.QFQ, Adjust.NONE})

        def fetch_bars(self, symbol, interval, start, end, adjust):
            ts = bar_15m if interval == Interval.MINUTE_15 else bar_day
            return SourcePayload(
                [AShareBar(symbol, ts, Decimal(10), Decimal(11), Decimal(9), Decimal(10))],
                source="fast_src:live",
            )

        def fetch_quote(self, symbol):
            return SourcePayload(Quote(symbol, Decimal("10.5"), reference), source="fast_src:qt")

    class SlowUnadjustedProvider(MarketDataProvider):
        name = "slow_raw"
        capabilities = frozenset({Capability.BARS})
        markets = frozenset({Market.CN})
        intervals = frozenset({Interval.DAY_1, Interval.MINUTE_15})
        adjustments = frozenset({Adjust.NONE})

        def fetch_bars(self, symbol, interval, start, end, adjust):
            assert adjust == Adjust.NONE
            sleep(0.04)
            ts = bar_15m if interval == Interval.MINUTE_15 else bar_day
            return SourcePayload(
                [AShareBar(symbol, ts, Decimal(10), Decimal(11), Decimal(9), Decimal(10))],
                source="slow_raw:tcp",
            )

    class DeadProvider(MarketDataProvider):
        name = "dead_src"
        capabilities = frozenset({Capability.BARS, Capability.QUOTE})
        markets = frozenset({Market.CN})
        intervals = frozenset({Interval.DAY_1})

        def fetch_bars(self, *args, **kwargs):
            raise ProviderUnavailable("blocked in current network")

        def fetch_quote(self, *args, **kwargs):
            raise ProviderUnavailable("blocked in current network")

    hub = DataHub([SlowUnadjustedProvider(), DeadProvider(), FastProvider()])
    probes = (
        ProbeSpec(
            "cn_1d", Capability.BARS, Market.CN, "600519.SH", AssetType.STOCK,
            Interval.DAY_1, Adjust.QFQ, 7,
        ),
        ProbeSpec(
            "cn_15m", Capability.BARS, Market.CN, "600519.SH", AssetType.STOCK,
            Interval.MINUTE_15, Adjust.QFQ, 7,
        ),
        ProbeSpec("cn_quote", Capability.QUOTE, Market.CN, "600519.SH", AssetType.STOCK),
    )

    report = asyncio.run(
        run_doctor(hub, markets=(Market.CN,), timeout_seconds=2.0, probes=probes, now=reference)
    )

    assert report.recommendation.disabled_providers == ("dead_src",)
    assert report.recommendation.source_priority[
        RouteKey(Capability.BARS, Market.CN, AssetType.STOCK, Interval.DAY_1)
    ] == ("fast_src", "slow_raw")
    assert report.recommendation.source_priority[
        RouteKey(Capability.BARS, Market.CN, AssetType.STOCK, Interval.MINUTE_15)
    ] == ("fast_src", "slow_raw")
    assert report.recommendation.source_priority[
        RouteKey(Capability.BARS, Market.CN, AssetType.STOCK, Interval.HOUR_1)
    ] == ("fast_src", "slow_raw")
    assert report.recommendation.source_priority[RouteKey(Capability.QUOTE, Market.CN)] == (
        "fast_src",
    )

    text = report.format_text()
    assert "fast_src" in text and "dead_src" in text
    assert "disabled_providers=['dead_src']" in report.recommendation.python_snippet

    as_dict = report.to_dict()
    assert as_dict["recommendation"]["source_priority"]["bars:CN:stock:1d"] == [
        "fast_src",
        "slow_raw",
    ]

    # Verify CLI ``stock-data-hub doctor --json`` integrates cleanly.
    monkeypatch.setattr(cli_module, "create_default_hub", lambda **_: hub)

    async def stub_run_doctor(target_hub, **kwargs):
        return await run_doctor(target_hub, probes=probes, now=reference, **kwargs)

    monkeypatch.setattr(cli_module, "run_doctor", stub_run_doctor)
    cli_module.main(["doctor", "--markets", "CN", "--timeout", "2", "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert payload["recommendation"]["disabled_providers"] == ["dead_src"]
    assert payload["recommendation"]["source_priority"]["quote:CN"] == ["fast_src"]
