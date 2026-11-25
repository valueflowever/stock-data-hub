"""Regression tests for multi-source failover, thread safety, intraday bars, and consistency."""

from __future__ import annotations

import asyncio
import io
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from threading import Barrier, Event, Thread
from time import sleep
from types import SimpleNamespace
from urllib.error import HTTPError
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

import stock_data_hub.hub as hub_module
import stock_data_hub.providers.akshare as ak_adapter
from stock_data_hub._workers import suppress_thread_stdout
from stock_data_hub.errors import ProviderUnavailable, UnsupportedCapability
from stock_data_hub.hub import DataHub, RouteKey
from stock_data_hub.models import (
    Adjust,
    AShareBar,
    AssetType,
    Capability,
    Interval,
    Market,
    Quote,
    QuoteFreshness,
    SourcePayload,
)
from stock_data_hub.normalization import BEIJING_TZ, asset_type_from_symbol
from stock_data_hub.providers.baostock import BaoStockProvider
from stock_data_hub.providers.base import MarketDataProvider
from stock_data_hub.providers.calendar import ExchangeCalendarProvider
from stock_data_hub.providers.defaults import create_default_hub
from stock_data_hub.providers.efinance import EFinanceProvider
from stock_data_hub.providers.pytdx import PyTDXProvider
from stock_data_hub.providers.remote import RemoteHubProvider
from stock_data_hub.providers.tencent import TencentQuoteProvider
from stock_data_hub.providers.yfinance import YFinanceProvider
from stock_data_hub.trading_sessions import window_has_trading_session


def test_suppress_thread_stdout_is_isolated_across_overlapping_threads(monkeypatch) -> None:
    captured = io.StringIO()
    monkeypatch.setattr(sys, "stdout", captured)
    entered_a = Event()
    entered_b = Event()
    release_a = Event()
    release_b = Event()

    def worker_a() -> None:
        with suppress_thread_stdout():
            print("hidden-a")
            entered_a.set()
            assert release_a.wait(2)

    def worker_b() -> None:
        assert entered_a.wait(2)
        with suppress_thread_stdout():
            print("hidden-b")
            entered_b.set()
            assert release_b.wait(2)

    thread_a = Thread(target=worker_a)
    thread_b = Thread(target=worker_b)
    thread_a.start()
    thread_b.start()
    assert entered_b.wait(2)
    print("visible-during")
    release_a.set()
    thread_a.join(2)
    print("visible-between")
    release_b.set()
    thread_b.join(2)
    print("visible-after")

    assert sys.stdout is captured
    assert captured.getvalue().splitlines() == [
        "visible-during",
        "visible-between",
        "visible-after",
    ]


def test_half_open_circuit_allows_only_one_probe_and_backs_off_on_repeat_failure() -> None:
    probe_started = Event()
    release_probe = Event()

    class FlakyProvider(MarketDataProvider):
        name = "flaky"
        capabilities = frozenset({Capability.QUOTE})

        def __init__(self) -> None:
            self.calls = 0
            self.block_next = False

        def fetch_quote(self, symbol: str) -> SourcePayload[Quote]:
            self.calls += 1
            if self.block_next:
                probe_started.set()
                assert release_probe.wait(3)
            raise ProviderUnavailable("upstream down")

    class HealthyProvider(MarketDataProvider):
        name = "backup"
        capabilities = frozenset({Capability.QUOTE})

        def __init__(self) -> None:
            self.calls = 0

        def fetch_quote(self, symbol: str) -> SourcePayload[Quote]:
            self.calls += 1
            return SourcePayload(
                Quote(symbol=symbol, price=Decimal("10.5"), ts=datetime.now(tz=BEIJING_TZ)),
                source="backup:quote",
            )

    flaky = FlakyProvider()
    backup = HealthyProvider()
    hub = DataHub(
        [flaky, backup],
        attempts_per_provider=1,
        failure_threshold=1,
        circuit_cooldown=timedelta(milliseconds=30),
    )

    async def run() -> None:
        first = await hub.get_quote("600519")
        assert first.metadata.source == "backup:quote"
        assert flaky.calls == 1
        first_open_until = {h.provider: h for h in hub.health()}["flaky"].circuit_open_until
        assert first_open_until is not None

        await asyncio.sleep(0.05)
        flaky.block_next = True
        probe_task = asyncio.create_task(hub.get_quote("600000"))

        async def wait_probe() -> None:
            while not probe_started.is_set():
                if probe_task.done():
                    raise AssertionError(f"probe_task finished early: {probe_task.result()!r}")
                await asyncio.sleep(0.002)

        await asyncio.wait_for(wait_probe(), timeout=2.0)
        assert {h.provider: h for h in hub.health()}["flaky"].status == "half_open"

        # While the single half-open probe is running, concurrent calls immediately fall back.
        concurrent = await hub.get_quote("600001")
        assert concurrent.metadata.source == "backup:quote"
        assert flaky.calls == 2

        release_probe.set()
        probe_result = await probe_task
        assert probe_result.metadata.source == "backup:quote"
        second_open_until = {h.provider: h for h in hub.health()}["flaky"].circuit_open_until
        assert second_open_until is not None
        assert second_open_until > first_open_until + timedelta(milliseconds=40)

    asyncio.run(run())


def test_holiday_and_premarket_windows_accept_empty_bars() -> None:
    # China National Day holiday (Thursday 2026-10-01 to Friday 2026-10-02).
    cn_holiday_start = datetime(2026, 10, 1, tzinfo=BEIJING_TZ)
    cn_holiday_end = datetime(2026, 10, 2, tzinfo=BEIJING_TZ)
    assert not window_has_trading_session(
        cn_holiday_start,
        cn_holiday_end,
        Market.CN,
        now=datetime(2026, 10, 10, 16, 0, tzinfo=BEIJING_TZ),
    )

    # Monday morning Beijing time: US Monday session has not opened yet.
    us_day = datetime(2026, 7, 20, tzinfo=BEIJING_TZ)
    assert not window_has_trading_session(
        us_day,
        us_day,
        Market.US,
        now=datetime(2026, 7, 20, 10, 0, tzinfo=BEIJING_TZ),
    )
    assert window_has_trading_session(
        us_day,
        us_day,
        Market.US,
        now=datetime(2026, 7, 20, 22, 0, tzinfo=BEIJING_TZ),
    )


def test_akshare_clean_empty_participates_in_all_empty_consensus(monkeypatch) -> None:
    class EmptyAkShare:
        def stock_zh_a_hist(self, **_: object) -> pd.DataFrame:
            return pd.DataFrame()

        def stock_zh_a_daily(self, **_: object) -> pd.DataFrame:
            return pd.DataFrame()

    class EmptyFirst(MarketDataProvider):
        name = "first"
        capabilities = frozenset({Capability.BARS})
        markets = frozenset({Market.CN})

        def fetch_bars(self, *args, **kwargs):
            return SourcePayload([], source="first:empty")

    monkeypatch.setattr(ak_adapter, "_load_akshare", lambda: EmptyAkShare())
    hub = DataHub([EmptyFirst(), ak_adapter.AkShareProvider()], attempts_per_provider=1)
    day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)
    result = asyncio.run(hub.get_bars("600519", Interval.DAY_1, day, day))

    assert result.value == []
    assert result.metadata.attempted_sources == ("first", "akshare")
    assert "All eligible live sources returned no bars" in result.metadata.warnings[-1]
    ak_health = {entry.provider: entry for entry in hub.health()}["akshare"]
    assert ak_health.failure_count == 0


def test_daily_window_normalization_keeps_first_day_and_hits_cache() -> None:
    class CountingBars(MarketDataProvider):
        name = "bars"
        capabilities = frozenset({Capability.BARS})
        markets = frozenset({Market.CN, Market.US})

        def __init__(self) -> None:
            self.calls = 0

        def fetch_bars(self, symbol, interval, start, end, adjust):
            self.calls += 1
            market = Market.US if symbol.endswith(".US") else Market.CN
            return SourcePayload(
                [
                    AShareBar(
                        symbol=symbol,
                        ts=start,
                        open=Decimal(10),
                        high=Decimal(11),
                        low=Decimal(9),
                        close=Decimal("10.5"),
                        market=market,
                    )
                ],
                source="bars:test",
            )

    provider = CountingBars()
    hub = DataHub([provider], attempts_per_provider=1)

    first = asyncio.run(
        hub.get_bars(
            "600519",
            Interval.DAY_1,
            datetime(2026, 7, 13, 16, 10, 5, tzinfo=BEIJING_TZ),
            datetime(2026, 7, 17, 16, 10, 5, tzinfo=BEIJING_TZ),
        )
    )
    second = asyncio.run(
        hub.get_bars(
            "600519",
            Interval.DAY_1,
            datetime(2026, 7, 13, 16, 11, 20, tzinfo=BEIJING_TZ),
            datetime(2026, 7, 17, 16, 11, 20, tzinfo=BEIJING_TZ),
        )
    )
    assert first.value[0].ts == datetime(2026, 7, 13, tzinfo=BEIJING_TZ)
    assert second.metadata.cache_hit
    assert provider.calls == 1

    # US Eastern calendar day maps to the same US date label, not the next Beijing day.
    ny = ZoneInfo("America/New_York")
    us = asyncio.run(
        hub.get_bars(
            "AAPL.US",
            Interval.DAY_1,
            datetime(2026, 7, 17, 0, 0, tzinfo=ny),
            datetime(2026, 7, 17, 23, 59, tzinfo=ny),
        )
    )
    assert us.value[0].ts == datetime(2026, 7, 17, tzinfo=BEIJING_TZ)


def test_eod_only_source_falls_back_to_live_source_for_active_session(monkeypatch) -> None:
    yesterday = datetime(2026, 7, 16, tzinfo=BEIJING_TZ)
    today = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)
    monkeypatch.setattr(hub_module, "latest_active_session_date", lambda market: date(2026, 7, 17))

    class EodProvider(MarketDataProvider):
        name = "eod"
        capabilities = frozenset({Capability.BARS})
        markets = frozenset({Market.CN})
        supports_intraday_daily_bar = False

        def fetch_bars(self, symbol, interval, start, end, adjust):
            return SourcePayload(
                [
                    AShareBar(
                        symbol=symbol,
                        ts=yesterday,
                        open=Decimal(10),
                        high=Decimal(11),
                        low=Decimal(9),
                        close=Decimal(10),
                    )
                ],
                source="eod:bars",
            )

    class LiveProvider(MarketDataProvider):
        name = "live"
        capabilities = frozenset({Capability.BARS})
        markets = frozenset({Market.CN})

        def __init__(self, fail: bool = False) -> None:
            self.fail = fail

        def fetch_bars(self, symbol, interval, start, end, adjust):
            if self.fail:
                raise ProviderUnavailable("live down")
            return SourcePayload(
                [
                    AShareBar(
                        symbol=symbol,
                        ts=yesterday,
                        open=Decimal(10),
                        high=Decimal(11),
                        low=Decimal(9),
                        close=Decimal(10),
                    ),
                    AShareBar(
                        symbol=symbol,
                        ts=today,
                        open=Decimal(10),
                        high=Decimal(12),
                        low=Decimal(10),
                        close=Decimal(11),
                    ),
                ],
                source="live:bars",
            )

    hub = DataHub([EodProvider(), LiveProvider()], attempts_per_provider=1)
    result = asyncio.run(hub.get_bars("600519", Interval.DAY_1, yesterday, today))
    assert result.metadata.source == "live:bars"
    assert [bar.ts for bar in result.value] == [yesterday, today]
    assert result.metadata.attempted_sources == ("eod", "live")

    # When live provider fails, gracefully return the EOD candidate with warning.
    degraded_hub = DataHub([EodProvider(), LiveProvider(fail=True)], attempts_per_provider=1)
    degraded = asyncio.run(degraded_hub.get_bars("600519", Interval.DAY_1, yesterday, today))
    assert degraded.metadata.source == "eod:bars"
    assert any("EOD-only" in warning for warning in degraded.metadata.warnings)


def test_beijing_exchange_symbols_do_not_trip_pytdx_or_yfinance_circuits() -> None:
    class FallbackBars(MarketDataProvider):
        name = "fallback"
        capabilities = frozenset({Capability.BARS})
        markets = frozenset({Market.CN})

        def fetch_bars(self, symbol, interval, start, end, adjust):
            return SourcePayload(
                [
                    AShareBar(
                        symbol=symbol,
                        ts=start,
                        open=Decimal(10),
                        high=Decimal(11),
                        low=Decimal(9),
                        close=Decimal(10),
                    )
                ],
                source="fallback:bars",
            )

    pytdx = PyTDXProvider("127.0.0.1", client_factory=lambda: SimpleNamespace())
    yahoo = YFinanceProvider(ticker_factory=lambda _: SimpleNamespace())
    hub = DataHub(
        [pytdx, yahoo, FallbackBars()],
        attempts_per_provider=2,
        failure_threshold=1,
    )
    day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)
    for code in ("920002.BJ", "430047.BJ"):
        result = asyncio.run(hub.get_bars(code, Interval.DAY_1, day, day, Adjust.NONE))
        assert result.metadata.source == "fallback:bars"

    health = {entry.provider: entry for entry in hub.health()}
    assert health["pytdx"].failure_count == 0
    assert health["pytdx"].status == "closed"
    assert health["yfinance"].failure_count == 0
    assert health["yfinance"].status == "closed"


def test_baostock_filters_suspended_days_and_supports_intraday_bars() -> None:
    class FakeResult:
        error_code = "0"
        error_msg = "success"

        def __init__(self, fields: list[str], rows: list[list[str]]) -> None:
            self.fields = fields
            self._rows = rows
            self._idx = -1

        def next(self) -> bool:
            self._idx += 1
            return self._idx < len(self._rows)

        def get_row_data(self) -> list[str]:
            return self._rows[self._idx]

    class FakeClient:
        def __init__(self) -> None:
            self.queries: list[tuple[str, str, str]] = []

        def login(self) -> SimpleNamespace:
            return SimpleNamespace(error_code="0", error_msg="success")

        def query_history_k_data_plus(
            self, code: str, fields: str, *, frequency: str, **_: object
        ) -> FakeResult:
            self.queries.append((code, fields, frequency))
            if frequency == "d":
                return FakeResult(
                    fields.split(","),
                    [
                        ["2026-07-16", code, "10", "11", "9", "10.5", "1000", "10500", "1"],
                        ["2026-07-17", code, "10.5", "10.5", "10.5", "10.5", "", "", "0"],
                    ],
                )
            return FakeResult(
                fields.split(","),
                [
                    [
                        "2026-07-17",
                        "20260717094500000",
                        code,
                        "10",
                        "11",
                        "9.8",
                        "10.5",
                        "500",
                        "5250",
                    ],
                    [
                        "2026-07-17",
                        "100000",
                        code,
                        "10.5",
                        "11.2",
                        "10.4",
                        "11.0",
                        "600",
                        "6600",
                    ],
                ],
            )

        def logout(self) -> None:
            return None

    client = FakeClient()
    provider = BaoStockProvider(client_loader=lambda: client)

    daily = provider.fetch_bars(
        "600519.SH",
        Interval.DAY_1,
        datetime(2026, 7, 16, tzinfo=BEIJING_TZ),
        datetime(2026, 7, 17, tzinfo=BEIJING_TZ),
        Adjust.QFQ,
    )
    assert len(daily.value) == 1
    assert daily.value[0].ts == datetime(2026, 7, 16, tzinfo=BEIJING_TZ)

    intraday = provider.fetch_bars(
        "600519.SH",
        Interval.MINUTE_15,
        datetime(2026, 7, 17, 9, 30, tzinfo=BEIJING_TZ),
        datetime(2026, 7, 17, 10, 30, tzinfo=BEIJING_TZ),
        Adjust.QFQ,
    )
    assert [bar.ts for bar in intraday.value] == [
        datetime(2026, 7, 17, 9, 45, tzinfo=BEIJING_TZ),
        datetime(2026, 7, 17, 10, 0, tzinfo=BEIJING_TZ),
    ]
    assert client.queries[1][2] == "15"

    with pytest.raises(UnsupportedCapability):
        provider.fetch_bars(
            "000001.SH",
            Interval.DAY_1,
            datetime(2026, 7, 16, tzinfo=BEIJING_TZ),
            datetime(2026, 7, 17, tzinfo=BEIJING_TZ),
            Adjust.QFQ,
            asset_type=AssetType.INDEX,
        )


def test_pytdx_supports_intraday_fallback_servers_and_missing_datetime_column() -> None:
    attempted_hosts: list[tuple[str, int]] = []
    requests: list[tuple[int, int, str]] = []

    class FakeTdx:
        def connect(self, host: str, port: int, *, time_out: float) -> bool:
            attempted_hosts.append((host, port))
            return host == "good.tdx"

        def get_security_bars(
            self, category: int, market: int, code: str, offset: int, count: int
        ) -> list[dict[str, object]]:
            requests.append((category, market, code))
            return [
                {
                    "date": "2026-07-17 10:00:00",
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.5,
                    "close": 10.5,
                    "vol": 10,
                    "amount": 10500,
                }
            ]

        def disconnect(self) -> None:
            return None

    provider = PyTDXProvider("bad.tdx:7709,good.tdx:7710", client_factory=FakeTdx)
    ts = datetime(2026, 7, 17, 10, 0, tzinfo=BEIJING_TZ)
    result = provider.fetch_bars("600519.SH", Interval.MINUTE_15, ts, ts, Adjust.NONE)

    assert attempted_hosts == [("bad.tdx", 7709), ("good.tdx", 7710)]
    assert requests == [(1, 1, "600519")]
    assert result.source == "pytdx:good.tdx:7710"
    assert result.value[0].ts == ts
    assert result.value[0].volume == Decimal(1000)


def test_yfinance_intraday_open_to_close_conversion_and_brk_b_closes() -> None:
    seen_symbols: list[str] = []
    ny = ZoneInfo("America/New_York")

    class YFPricesMissingError(RuntimeError):
        pass

    class FakeTicker:
        def __init__(self, symbol: str) -> None:
            self.symbol = symbol

        def history(self, *, interval: str, **_: object) -> pd.DataFrame:
            if self.symbol == "EMPTY":
                raise YFPricesMissingError("no price data found")
            if interval == "15m":
                return pd.DataFrame(
                    {"Open": [200.0], "High": [202.0], "Low": [199.5], "Close": [201.0], "Volume": [500]},
                    index=pd.DatetimeIndex([datetime(2026, 7, 17, 9, 30, tzinfo=ny)], name="Datetime"),
                )
            return pd.DataFrame(
                {"Close": [420.5]},
                index=pd.DatetimeIndex([datetime(2026, 7, 17, 0, 0, tzinfo=ny)], name="Date"),
            )

    def factory(symbol: str) -> FakeTicker:
        seen_symbols.append(symbol)
        return FakeTicker(symbol)

    provider = YFinanceProvider(ticker_factory=factory)
    bars = provider.fetch_bars(
        "AAPL.US",
        Interval.MINUTE_15,
        datetime(2026, 7, 17, 21, 0, tzinfo=BEIJING_TZ),
        datetime(2026, 7, 17, 22, 0, tzinfo=BEIJING_TZ),
        Adjust.NONE,
    )
    # 09:30 EDT bar open + 15m = 09:45 EDT = 21:45+08:00 close.
    assert bars.value[0].ts == datetime(2026, 7, 17, 21, 45, tzinfo=BEIJING_TZ)

    closes = provider.fetch_us_daily_closes("BRK.B", 10)
    assert seen_symbols[-1] == "BRK-B"
    assert closes.value[0].day == date(2026, 7, 17)

    empty = provider.fetch_bars(
        "EMPTY.US",
        Interval.DAY_1,
        datetime(2026, 7, 17, tzinfo=BEIJING_TZ),
        datetime(2026, 7, 17, tzinfo=BEIJING_TZ),
        Adjust.NONE,
    )
    assert empty.value == []


def test_akshare_prefers_sina_bars_caches_us_spot_and_supports_global_etfs(monkeypatch) -> None:
    class FakeAk:
        def __init__(self) -> None:
            self.us_spot_calls = 0
            self.em_bars_calls = 0

        def stock_zh_a_daily(self, **_: object) -> pd.DataFrame:
            return pd.DataFrame(
                {
                    "date": ["2026-07-17"],
                    "open": [10],
                    "high": [11],
                    "low": [9],
                    "close": [10.5],
                    "volume": [1000],
                    "amount": [10500],
                }
            )

        def stock_zh_a_hist(self, **_: object) -> pd.DataFrame:
            self.em_bars_calls += 1
            raise AssertionError("Eastmoney should not be hit when prefer_sina_bars succeeds")

        def stock_us_spot_em(self) -> pd.DataFrame:
            self.us_spot_calls += 1
            return pd.DataFrame({"代码": ["105.AAPL", "107.SPY"]})

        def stock_us_hist(self, symbol: str, **_: object) -> pd.DataFrame:
            return pd.DataFrame(
                {
                    "日期": ["2026-07-17"],
                    "开盘": [200],
                    "最高": [205],
                    "最低": [198],
                    "收盘": [202],
                    "成交量": [10000],
                }
            )

    sdk = FakeAk()
    monkeypatch.setattr(ak_adapter, "_load_akshare", lambda: sdk)
    provider = ak_adapter.AkShareProvider(prefer_sina_bars=True)
    day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)

    cn = provider.fetch_bars("600519.SH", Interval.DAY_1, day, day, Adjust.QFQ)
    assert cn.source == "akshare:sina"
    assert sdk.em_bars_calls == 0

    assert provider.supports_request(
        Capability.BARS, market=Market.US, asset_type=AssetType.ETF, interval=Interval.DAY_1
    )
    assert provider.supports_request(
        Capability.QUOTE, market=Market.HK, asset_type=AssetType.ETF
    )

    provider.fetch_bars("AAPL.US", Interval.DAY_1, day, day, Adjust.QFQ)
    provider.fetch_bars("SPY.US", Interval.DAY_1, day, day, Adjust.QFQ, asset_type=AssetType.ETF)
    assert sdk.us_spot_calls == 1

    # Snapshot parsing preserves prefix validation so sh000001 cannot masquerade as 000001.SZ.
    snap = ak_adapter._parse_snapshot(
        pd.DataFrame(
            {
                "代码": ["sh000001", "sz000001"],
                "名称": ["上证指数", "平安银行"],
                "最新价": [3500, 10.5],
                "成交额": [100, 200],
                "涨跌幅": [0.5, 1.0],
            }
        ),
        has_market_cap=False,
    )
    assert [row.symbol for row in snap.rows] == ["000001.SZ"]


def test_tencent_quote_provider_and_batch_prefetch_work_across_markets() -> None:
    requested_urls: list[str] = []

    class TextResponse:
        def __init__(self, text: str) -> None:
            self._data = text.encode("gb18030")

        def __enter__(self) -> TextResponse:  # noqa: PYI034 - Python 3.10 has no typing.Self
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def read(self, size: int = -1) -> bytes:
            return self._data[:size] if size >= 0 else self._data

    def opener(request, *, timeout: float) -> TextResponse:
        assert timeout > 0
        requested_urls.append(request.full_url)
        parts_cn = [""] * 35
        parts_cn[1] = "贵州茅台"
        parts_cn[2] = "600519"
        parts_cn[3] = "1420.50"
        parts_cn[30] = "20260717150000"

        parts_sz = [""] * 35
        parts_sz[1] = "平安银行"
        parts_sz[2] = "000001"
        parts_sz[3] = "11.20"
        parts_sz[30] = "20260717150000"

        parts_hk = [""] * 35
        parts_hk[1] = "腾讯控股"
        parts_hk[2] = "00700"
        parts_hk[3] = "480.20"
        parts_hk[30] = "2026/07/17 16:08:00"

        parts_us = [""] * 35
        parts_us[1] = "Apple"
        parts_us[2] = "AAPL"
        parts_us[3] = "215.75"

        body = "\n".join(
            [
                f'v_sh600519="{"~".join(parts_cn)}";',
                f'v_sz000001="{"~".join(parts_sz)}";',
                f'v_hk00700="{"~".join(parts_hk)}";',
                f'v_usAAPL="{"~".join(parts_us)}";',
            ]
        )
        return TextResponse(body)

    tencent = TencentQuoteProvider(opener=opener)
    hub = DataHub([tencent], attempts_per_provider=1)
    items = asyncio.run(hub.get_quotes(["600519", "000001.SZ", "00700.HK", "AAPL.US"]))

    assert [item.symbol for item in items] == ["600519.SH", "000001.SZ", "00700.HK", "AAPL.US"]
    assert items[0].data is not None and items[0].data.price == Decimal("1420.50")
    assert items[0].data.freshness == QuoteFreshness.REALTIME
    assert items[1].data is not None and items[1].data.price == Decimal("11.20")
    assert items[2].data is not None and items[2].data.freshness == QuoteFreshness.DELAYED
    assert items[3].data is not None and items[3].data.price == Decimal("215.75")
    # CN pair (600519, 000001) was batched into 1 HTTP request instead of 2!
    assert any("sh600519,sz000001" in url for url in requested_urls)

    # Alias normalization matches get_quote.
    aliased = asyncio.run(hub.get_quotes(["600519"], market="A", asset_type="A_SHARE"))
    assert aliased[0].data is not None and aliased[0].data.symbol == "600519.SH"


def test_efinance_batch_prefetch_uses_single_call() -> None:
    calls: list[list[str]] = []

    class Stock:
        def get_latest_quote(self, ids: list[str], *, quote_id_mode: bool) -> pd.DataFrame:
            assert quote_id_mode
            calls.append(list(ids))
            return pd.DataFrame(
                {
                    "行情ID": ids,
                    "最新价": ["1400.0" if qid == "1.600519" else "10.5" for qid in ids],
                }
            )

    ef = EFinanceProvider(client_loader=lambda: SimpleNamespace(stock=Stock()))
    hub = DataHub([ef], attempts_per_provider=1)
    items = asyncio.run(hub.get_quotes(["600519.SH", "000001.SZ"]))
    assert len(calls) == 1
    assert calls[0] == ["1.600519", "0.000001"]
    assert [item.data.price for item in items if item.data] == [Decimal("1400.0"), Decimal("10.5")]


def test_exchange_calendar_fallback_and_bj_index_and_default_hub_config() -> None:
    assert asset_type_from_symbol("899050.BJ") == AssetType.INDEX

    class BrokenCalendar(MarketDataProvider):
        name = "akshare"
        capabilities = frozenset({Capability.TRADE_CALENDAR})

        def fetch_trade_calendar(self):
            raise ProviderUnavailable("sina down")

    hub = DataHub([BrokenCalendar(), ExchangeCalendarProvider(start_year=2025, end_year=2026)])
    result = asyncio.run(hub.get_trade_calendar())
    assert result.metadata.source == "exchange_calendar:XSHG"
    assert date(2026, 7, 17) in result.value.trade_dates
    assert date(2026, 10, 1) not in result.value.trade_dates

    custom = create_default_hub(
        disabled_providers=["yfinance", "tickflow"],
        peer_urls=["https://peer.example/", "https://peer.example"],
        source_priority={RouteKey(Capability.QUOTE, Market.CN): ("tencent", "efinance")},
    )
    registered = {entry.provider for entry in custom.health()}
    assert "yfinance" not in registered
    assert "tickflow" not in registered
    assert "tencent" in registered
    assert "exchange_calendar" in registered
    assert "remote-1" in registered
    assert "remote-2" not in registered


def test_remote_provider_preserves_http_error_status_and_local_cache_sharing() -> None:
    def failing_opener(request, *, timeout: float):
        raise HTTPError(
            request.full_url,
            401,
            "Unauthorized",
            {},
            io.BytesIO(b'{"detail": "Missing or invalid data hub key"}'),
        )

    remote = RemoteHubProvider("https://peer.example", opener=failing_opener)
    with pytest.raises(ProviderUnavailable, match="HTTP 401.*Missing or invalid data hub key"):
        remote.fetch_quote("600519.SH")

    class LocalQuote(MarketDataProvider):
        name = "local"
        capabilities = frozenset({Capability.QUOTE})

        def __init__(self) -> None:
            self.calls = 0

        def fetch_quote(self, symbol: str) -> SourcePayload[Quote]:
            self.calls += 1
            return SourcePayload(
                Quote(symbol=symbol, price=Decimal(100), ts=datetime.now(tz=BEIJING_TZ)),
                source="local:quote",
            )

    local = LocalQuote()
    hub = DataHub([local], attempts_per_provider=1)
    first = asyncio.run(hub.get_quote("600519", allow_peers=True))
    forwarded = asyncio.run(hub.get_quote("600519", allow_peers=False))
    assert first.value == forwarded.value
    assert forwarded.metadata.cache_hit
    assert local.calls == 1


def test_concurrent_burst_waits_for_healthy_worker_slots_without_false_busy() -> None:
    barrier = Barrier(2)

    class SlowHealthyProvider(MarketDataProvider):
        name = "healthy"
        capabilities = frozenset({Capability.QUOTE})

        def fetch_quote(self, symbol: str) -> SourcePayload[Quote]:
            if symbol in {"600519.SH", "600000.SH"}:
                barrier.wait(timeout=2)
                sleep(0.08)
            return SourcePayload(
                Quote(symbol=symbol, price=Decimal(10), ts=datetime.now(tz=BEIJING_TZ)),
                source="healthy:quote",
            )

    hub = DataHub(
        [SlowHealthyProvider()],
        max_concurrent_requests=2,
        provider_timeout_seconds=5.0,
        provider_queue_timeout_seconds=1.0,
        attempts_per_provider=1,
    )

    async def run() -> list:
        return await asyncio.gather(
            hub.get_quote("600519"),
            hub.get_quote("600000"),
            hub.get_quote("600001"),
            hub.get_quote("600002"),
        )

    results = asyncio.run(run())
    assert len(results) == 4
    assert {entry.provider: entry for entry in hub.health()}["healthy"].capacity_rejections == 0
