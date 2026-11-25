"""Automated financial data quality (DQ), convergence, session-alignment, and security tests."""

from __future__ import annotations

import asyncio
import math
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from urllib.error import URLError
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

try:
    from fastapi.testclient import TestClient
except ModuleNotFoundError:
    TestClient = None

import stock_data_hub.cli as cli_module
import stock_data_hub.hub as hub_module
import stock_data_hub.providers.akshare as ak_adapter
from stock_data_hub._workers import RateLimiter
from stock_data_hub.doctor import run_doctor
from stock_data_hub.errors import AllSourcesFailed, ProviderUnavailable
from stock_data_hub.hub import DataHub
from stock_data_hub.indicators.contracts import IndicatorSpec
from stock_data_hub.indicators.engine import IndicatorEngine
from stock_data_hub.models import (
    Adjust,
    AdjustFactor,
    AdjustMethod,
    AShareBar,
    AssetType,
    Capability,
    Interval,
    Market,
    Snapshot,
    SnapshotRow,
    SourcePayload,
)
from stock_data_hub.normalization import (
    BEIJING_TZ,
    is_bar_closed,
    normalize_symbol,
    prefixed_a_share_symbol,
)
from stock_data_hub.providers.baostock import BaoStockProvider
from stock_data_hub.providers.base import MarketDataProvider
from stock_data_hub.providers.defaults import create_default_hub
from stock_data_hub.providers.efinance import EFinanceProvider
from stock_data_hub.providers.pytdx import PyTDXProvider
from stock_data_hub.providers.remote import RemoteHubProvider
from stock_data_hub.providers.yfinance import YFinanceProvider
from stock_data_hub.service import create_app
from stock_data_hub.sqlite_cache import PERSISTENT_CACHE_POLICIES, SQLiteResultCache


class _HistoryBarsHub:
    def __init__(self, bars: list[AShareBar]) -> None:
        self._bars = bars
        self.calls = 0

    async def get_bars(
        self,
        symbol: str,
        interval: Interval,
        start: datetime,
        end: datetime,
        adjust: Adjust,
        market: Market,
        asset_type: AssetType,
        *,
        allow_peers: bool = True,
    ):
        from stock_data_hub.models import DataResult, FetchMetadata

        self.calls += 1
        selected = [bar for bar in self._bars if start <= bar.ts <= end]
        return DataResult(
            selected,
            FetchMetadata(
                source="dq:fixture",
                fetched_at=datetime(2026, 7, 17, 16, 0, tzinfo=BEIJING_TZ),
                latency_ms=1.0,
                attempted_sources=("dq",),
            ),
        )


def test_dq_1_recursive_indicator_convergence_invariance_and_ratio_guard() -> None:
    """DQ-1: Live 1-bar signal queries must match 500-bar backtests within 1e-4."""
    pytest.importorskip("talib")
    first = datetime(2024, 1, 1, tzinfo=BEIJING_TZ)
    bars: list[AShareBar] = []
    for idx in range(500):
        wave = 12.0 * math.sin(idx / 7.0) + 5.0 * math.cos(idx / 3.0)
        close = Decimal(f"{150.0 + idx * 0.15 + wave:.4f}")
        bars.append(
            AShareBar(
                symbol="600519.SH",
                ts=first + timedelta(days=idx),
                open=close - Decimal("0.8"),
                high=close + Decimal("2.1"),
                low=close - Decimal("1.9"),
                close=close,
                volume=Decimal(100_000 + (idx % 17) * 5_000),
                amount=close * Decimal(100_000),
                is_closed=True,
                market=Market.CN,
            )
        )

    specs = [
        IndicatorSpec("overlap.ema", {"period": 20}, alias="ema20"),
        IndicatorSpec("momentum.rsi", {"period": 14}, alias="rsi14"),
        IndicatorSpec("trend.macd", alias="macd"),
        IndicatorSpec("volatility.atr", {"period": 14}, alias="atr14"),
        IndicatorSpec("trend.adx", {"period": 14}, alias="adx14"),
    ]
    full_engine = IndicatorEngine(_HistoryBarsHub(bars))  # type: ignore[arg-type]
    live_hub = _HistoryBarsHub(bars)
    live_engine = IndicatorEngine(live_hub)  # type: ignore[arg-type]

    full_res = asyncio.run(
        full_engine.calculate("600519", Interval.DAY_1, bars[0].ts, bars[-1].ts, specs, adjust="none")
    )
    live_res = asyncio.run(
        live_engine.calculate("600519", Interval.DAY_1, bars[-1].ts, bars[-1].ts, specs, adjust="none")
    )

    assert live_hub.calls == 1
    full_last = full_res.value.rows[-1].values
    live_last = live_res.value.rows[-1].values
    for col in (
        "ema20",
        "rsi14",
        "macd.macd",
        "macd.signal",
        "macd.histogram",
        "atr14",
        "adx14.adx",
        "adx14.plus_di",
        "adx14.minus_di",
    ):
        assert full_last[col] is not None and live_last[col] is not None
        assert abs(live_last[col] - full_last[col]) < Decimal("0.0001"), (
            f"{col} diverged: live={live_last[col]} vs full={full_last[col]}"
        )

    # Non-positive arithmetic-QFQ prices must not produce inverted/corrupted ROC outputs.
    neg_bars = [
        AShareBar(
            symbol="600519.SH",
            ts=first + timedelta(days=idx),
            open=Decimal(price),
            high=Decimal(price) + Decimal("0.5"),
            low=Decimal(price) - Decimal("0.5"),
            close=Decimal(price),
            volume=Decimal(1000),
            market=Market.CN,
        )
        for idx, price in enumerate([-2, -1, 0, 1, 2, 4])
    ]
    roc_res = asyncio.run(
        IndicatorEngine(_HistoryBarsHub(neg_bars)).calculate(  # type: ignore[arg-type]
            "600519",
            Interval.DAY_1,
            neg_bars[2].ts,
            neg_bars[-1].ts,
            [IndicatorSpec("momentum.roc", {"period": 2}, alias="roc2")],
            adjust=Adjust.QFQ,
        )
    )
    # Index 2 (close=0 vs -2), index 3 (close=1 vs -1), index 4 (close=2 vs 0) all touch <=0;
    # only index 5 (close=4 vs 1) has strictly positive base & current price (+300%).
    assert [row.values["roc2"] for row in roc_res.value.rows] == [
        None,
        None,
        None,
        Decimal("300.0"),
    ]
    assert any("non-positive adjusted price" in w for w in roc_res.metadata.warnings)


def test_dq_2_session_boundary_intraday_alignment() -> None:
    """DQ-2: OPEN-labeled 1h bars clamp to US 16:00 ET, US 13:00 ET early close, and HK 12:00 HKT."""
    ny = ZoneInfo("America/New_York")
    hk = ZoneInfo("Asia/Hong_Kong")

    class SessionTicker:
        def __init__(self, symbol: str) -> None:
            self.symbol = symbol

        def history(self, *, start: str, **_: object) -> pd.DataFrame:
            if self.symbol == "AAPL" and "2026-11" in start:
                # Black Friday early close (09:30 - 13:00 EST)
                opens = [
                    datetime(2026, 11, 27, 9, 30, tzinfo=ny),
                    datetime(2026, 11, 27, 10, 30, tzinfo=ny),
                    datetime(2026, 11, 27, 11, 30, tzinfo=ny),
                    datetime(2026, 11, 27, 12, 30, tzinfo=ny),
                ]
            elif self.symbol == "AAPL":
                # Regular US session (09:30 - 16:00 EDT); last bar opens 15:30 EDT (30m long)
                opens = [datetime(2026, 7, 17, hour, 30, tzinfo=ny) for hour in range(9, 16)]
            else:
                # HK morning (09:30 - 12:00 HKT) + afternoon (13:00 - 16:00 HKT)
                opens = [
                    datetime(2026, 7, 17, 9, 30, tzinfo=hk),
                    datetime(2026, 7, 17, 10, 30, tzinfo=hk),
                    datetime(2026, 7, 17, 11, 30, tzinfo=hk),
                    datetime(2026, 7, 17, 13, 0, tzinfo=hk),
                    datetime(2026, 7, 17, 14, 0, tzinfo=hk),
                    datetime(2026, 7, 17, 15, 0, tzinfo=hk),
                ]
            n = len(opens)
            return pd.DataFrame(
                {
                    "Open": [100.0] * n,
                    "High": [102.0] * n,
                    "Low": [99.0] * n,
                    "Close": [101.0] * n,
                    "Volume": [1000] * n,
                },
                index=pd.DatetimeIndex(opens, name="Datetime"),
            )

    yf = YFinanceProvider(ticker_factory=SessionTicker)

    # 1. US regular session queried up to exact 16:00 EDT close keeps the 15:30-16:00 bar!
    us_open = datetime(2026, 7, 17, 9, 30, tzinfo=ny)
    us_close = datetime(2026, 7, 17, 16, 0, tzinfo=ny)
    us_bars = yf.fetch_bars("AAPL.US", Interval.HOUR_1, us_open, us_close, Adjust.NONE)
    assert len(us_bars.value) == 7
    assert us_bars.value[-1].ts == us_close.astimezone(BEIJING_TZ)
    assert is_bar_closed(
        us_bars.value[-1].ts,
        Interval.HOUR_1,
        now=datetime(2026, 7, 17, 16, 1, tzinfo=ny),
        market=Market.US,
    )

    # 2. US Black Friday early close clamps 12:30 EST -> 13:00 EST!
    bf_open = datetime(2026, 11, 27, 9, 30, tzinfo=ny)
    bf_close = datetime(2026, 11, 27, 13, 0, tzinfo=ny)
    bf_bars = yf.fetch_bars("AAPL.US", Interval.HOUR_1, bf_open, bf_close, Adjust.NONE)
    assert len(bf_bars.value) == 4
    assert bf_bars.value[-1].ts == bf_close.astimezone(BEIJING_TZ)

    # 3. HK morning 11:30 HKT bar clamps to 12:00 HKT lunch break (not 12:30 HKT)!
    hk_bars = yf.fetch_bars(
        "00700.HK",
        Interval.HOUR_1,
        datetime(2026, 7, 17, 9, 30, tzinfo=hk),
        datetime(2026, 7, 17, 16, 0, tzinfo=hk),
        Adjust.NONE,
    )
    assert [bar.ts.hour for bar in hk_bars.value] == [10, 11, 12, 14, 15, 16]


def test_dq_3_cross_provider_reconciliation_suspended_filtering_and_adjust_factors(
    monkeypatch, tmp_path: Path
) -> None:
    """DQ-3: Suspended flat 0-vol rows are uniformly excluded; adjust_method & factors work."""
    start = datetime(2026, 7, 16, tzinfo=BEIJING_TZ)
    end = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)

    # Both days present in raw frames, with 2026-07-17 being a 0-volume flat suspension placeholder.
    ef_frame = pd.DataFrame(
        {
            "股票代码": ["600519", "600519"],
            "日期": ["2026-07-16", "2026-07-17"],
            "开盘": [1400, 1410],
            "最高": [1420, 1410],
            "最低": [1395, 1410],
            "收盘": [1410, 1410],
            "成交量": [100, 0],
            "成交额": [14100000, 0],
        }
    )
    sina_frame = pd.DataFrame(
        {
            "date": ["2026-07-16", "2026-07-17"],
            "open": [1400, 1410],
            "high": [1420, 1410],
            "low": [1395, 1410],
            "close": [1410, 1410],
            "volume": [10000, 0],
            "amount": [14100000, 0],
        }
    )
    tdx_rows = [
        {"datetime": "2026-07-16 15:00", "open": 1400, "high": 1420, "low": 1395, "close": 1410, "vol": 100, "amount": 14100000},
        {"datetime": "2026-07-17 15:00", "open": 1410, "high": 1410, "low": 1410, "close": 1410, "vol": 0, "amount": 0},
    ]

    ef = EFinanceProvider(
        client_loader=lambda: SimpleNamespace(
            stock=SimpleNamespace(get_quote_history=lambda *a, **k: ef_frame)
        )
    )
    tdx = PyTDXProvider(
        "127.0.0.1",
        client_factory=lambda: SimpleNamespace(
            connect=lambda *a, **k: True,
            get_security_bars=lambda *a, **k: tdx_rows,
            disconnect=lambda: None,
        ),
    )
    fake_ak = SimpleNamespace(
        stock_zh_a_hist_em=lambda **_: ef_frame,
        stock_zh_a_hist_sina=lambda **kwargs: (
            pd.DataFrame({"date": ["2026-07-16"], "qfq_factor": ["0.95"], "hfq_factor": ["12.5"]})
            if "factor" in str(kwargs.get("adjust", ""))
            else sina_frame
        ),
    )
    monkeypatch.setattr(ak_adapter, "_load_akshare", lambda: fake_ak)
    ak_em = ak_adapter.AkShareProvider(prefer_sina_bars=False)
    ak_sina = ak_adapter.AkShareProvider(prefer_sina_bars=True)

    raw_results = [
        ef.fetch_bars("600519.SH", Interval.DAY_1, start, end, Adjust.NONE),
        tdx.fetch_bars("600519.SH", Interval.DAY_1, start, end, Adjust.NONE),
        ak_em.fetch_bars("600519.SH", Interval.DAY_1, start, end, Adjust.NONE),
        ak_sina.fetch_bars("600519.SH", Interval.DAY_1, start, end, Adjust.NONE),
    ]
    for res in raw_results:
        assert len(res.value) == 1
        assert res.value[0].ts == start
        assert res.value[0].volume == Decimal(10_000)
        assert res.quality is not None
        assert res.quality.suspended_rows == 1
        assert res.quality.filtered_rows == 0
        assert res.quality.adjust_method == AdjustMethod.RAW.value

    # QFQ explicitly distinguishes arithmetic (Eastmoney) vs proportional (Sina).
    qfq_em = ef.fetch_bars("600519.SH", Interval.DAY_1, start, end, Adjust.QFQ)
    qfq_sina = ak_sina.fetch_bars("600519.SH", Interval.DAY_1, start, end, Adjust.QFQ)
    assert qfq_em.quality is not None and qfq_em.quality.adjust_method == AdjustMethod.ARITHMETIC.value
    assert qfq_sina.quality is not None and qfq_sina.quality.adjust_method == AdjustMethod.PROPORTIONAL.value

    # Adjust factors via BaoStock -> AkShare fallback + SQLite roundtrip.
    class FakeBsFactorResult:
        error_code = "0"
        error_msg = "ok"

        def __init__(self) -> None:
            self.fields = ["code", "dividOperateDate", "foreAdjustFactor", "backAdjustFactor"]
            self._done = False

        def next(self) -> bool:
            if self._done:
                return False
            self._done = True
            return True

        def get_row_data(self) -> list[str]:
            return ["sh.600519", "2026-07-16", "0.950000", "12.500000"]

    bs = BaoStockProvider(
        client_loader=lambda: SimpleNamespace(
            login=lambda: SimpleNamespace(error_code="0"),
            query_adjust_factor=lambda **_: FakeBsFactorResult(),
            logout=lambda: None,
        )
    )
    cache = SQLiteResultCache(tmp_path / "factors.db")
    hub = DataHub([bs, ak_sina], cache=cache, cache_policies=PERSISTENT_CACHE_POLICIES)
    factors = asyncio.run(hub.get_adjust_factors("600519", date(2026, 7, 1), date(2026, 7, 31)))
    cached_factors = asyncio.run(
        hub.get_adjust_factors("600519", date(2026, 7, 1), date(2026, 7, 31))
    )
    hub.close()
    assert factors.value == [
        AdjustFactor("600519.SH", date(2026, 7, 16), Decimal("0.950000"), Decimal("12.500000"))
    ]
    assert cached_factors.metadata.cache_hit
    assert cached_factors.value == factors.value

    # Index prefix symmetry: SH000001 & prefixed_a_share_symbol("000001.SH").
    assert normalize_symbol("SH000001") == "000001.SH"
    assert normalize_symbol("SH000300") == "000300.SH"
    assert prefixed_a_share_symbol("000001.SH") == "sh000001"


def test_dq_4_market_close_cache_transition_and_weekly_eod_lag(
    monkeypatch, tmp_path: Path
) -> None:
    """DQ-4: Developing pre-close bars in SQLite cache immediately expire across 15:05 close."""

    class ControllableClock(datetime):
        current = datetime(2026, 7, 17, 14, 59, 0, tzinfo=BEIJING_TZ)

        @classmethod
        def now(cls, tz=None):
            return cls.current.astimezone(tz) if tz is not None else cls.current

    monkeypatch.setattr(hub_module, "datetime", ControllableClock)
    day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)

    class LiveSessionProvider(MarketDataProvider):
        name = "live"
        capabilities = frozenset({Capability.BARS})
        markets = frozenset({Market.CN})

        def __init__(self) -> None:
            self.calls = 0

        def fetch_bars(self, symbol, interval, start, end, adjust):
            self.calls += 1
            closed = is_bar_closed(day, Interval.DAY_1, now=ControllableClock.current, market=Market.CN)
            price = Decimal("1415.0") if closed else Decimal("1400.0")
            return SourcePayload(
                [AShareBar(symbol, day, Decimal(1390), price, Decimal(1388), price, is_closed=closed)],
                source="live:cn",
            )

    provider = LiveSessionProvider()
    with SQLiteResultCache(tmp_path / "close_transition.db") as cache:
        hub = DataHub(
            [provider],
            cache=cache,
            cache_policies=PERSISTENT_CACHE_POLICIES,
            attempts_per_provider=1,
        )
        # 14:59:00 -> fetches developing bar (is_closed=False, close=1400.0)
        pre_close = asyncio.run(hub.get_bars("600519", Interval.DAY_1, day, day))
        assert not pre_close.value[0].is_closed
        assert pre_close.value[0].close == Decimal("1400.0")
        assert provider.calls == 1

        # 15:05:05 (6m05s later, well inside 15m PERSISTENT_CACHE_POLICIES, but past 15:05 close!)
        ControllableClock.current = datetime(2026, 7, 17, 15, 5, 5, tzinfo=BEIJING_TZ)
        post_close = asyncio.run(hub.get_bars("600519", Interval.DAY_1, day, day))
        assert not post_close.metadata.cache_hit
        assert post_close.value[0].is_closed
        assert post_close.value[0].close == Decimal("1415.0")
        assert provider.calls == 2

    # Weekly EOD lag detection: when TickFlow only has last week's bar on Wednesday, Hub falls back.
    monkeypatch.setattr(hub_module, "latest_active_session_date", lambda market: date(2026, 7, 15))
    prev_week = datetime(2026, 7, 6, tzinfo=BEIJING_TZ)
    curr_week = datetime(2026, 7, 13, tzinfo=BEIJING_TZ)

    class WeeklyEod(MarketDataProvider):
        name = "tickflow"
        capabilities = frozenset({Capability.BARS})
        markets = frozenset({Market.CN})
        supports_intraday_daily_bar = False

        def fetch_bars(self, symbol, interval, start, end, adjust):
            return SourcePayload(
                [AShareBar(symbol, prev_week, Decimal(10), Decimal(11), Decimal(9), Decimal(10))],
                source="tickflow:free",
            )

    class WeeklyLive(MarketDataProvider):
        name = "efinance"
        capabilities = frozenset({Capability.BARS})
        markets = frozenset({Market.CN})

        def fetch_bars(self, symbol, interval, start, end, adjust):
            return SourcePayload(
                [
                    AShareBar(symbol, prev_week, Decimal(10), Decimal(11), Decimal(9), Decimal(10)),
                    AShareBar(symbol, curr_week, Decimal(10), Decimal(12), Decimal(10), Decimal(11)),
                ],
                source="efinance:eastmoney",
            )

    weekly_hub = DataHub([WeeklyEod(), WeeklyLive()], attempts_per_provider=1)
    weekly_res = asyncio.run(weekly_hub.get_bars("600519", Interval.WEEK_1, prev_week, curr_week))
    assert weekly_res.metadata.source == "efinance:eastmoney"
    assert len(weekly_res.value) == 2


def test_dq_5_universe_sanity_floor_sina_truncation_and_security_guards(
    monkeypatch, tmp_path: Path
) -> None:
    """DQ-5: Truncated snapshots/minute bars never pollute verified cache; security guards hold."""
    # 1. Partial 150-row snapshot fails the 4000-row sanity floor and does not pollute SQLite cache.
    partial_df = pd.DataFrame(
        {
            "股票代码": [f"{600000 + i:06d}" for i in range(150)],
            "股票名称": [f"Stock{i}" for i in range(150)],
            "最新价": [10.0] * 150,
            "成交额": [1000.0] * 150,
        }
    )
    bad_ef = EFinanceProvider(
        client_loader=lambda: SimpleNamespace(
            stock=SimpleNamespace(get_realtime_quotes=lambda: partial_df)
        ),
        min_snapshot_rows=4000,
    )
    with SQLiteResultCache(tmp_path / "sanity.db") as cache:
        fail_hub = DataHub([bad_ef], cache=cache, attempts_per_provider=1)
        with pytest.raises(AllSourcesFailed, match="incomplete"):
            asyncio.run(fail_hub.get_snapshot())
        assert cache.stats()["total"] == 0

        class FullSnapshotProvider(MarketDataProvider):
            name = "backup_snap"
            capabilities = frozenset({Capability.SNAPSHOT})

            def fetch_snapshot(self):
                return SourcePayload(
                    Snapshot(
                        rows=(
                            SnapshotRow("600519.SH", "茅台", Decimal(1400), None, None, None, None),
                        ),
                        has_market_cap=False,
                        fetched_at=datetime.now(tz=BEIJING_TZ),
                    ),
                    source="backup:snap",
                )

        fallback_hub = DataHub([bad_ef, FullSnapshotProvider()], cache=cache, attempts_per_provider=1)
        snap = asyncio.run(fallback_hub.get_snapshot())
        assert snap.metadata.source == "backup:snap"

    # 2. Sina 1023-bar minute cap marks truncated=True when earlier trading sessions were requested.
    recent_start = datetime(2026, 7, 1, 9, 45, tzinfo=BEIJING_TZ)
    sina_1023 = pd.DataFrame(
        {
            "date": [
                (recent_start + timedelta(minutes=15 * i)).strftime("%Y-%m-%d %H:%M:%S")
                for i in range(1023)
            ],
            "open": [10.0] * 1023,
            "high": [10.5] * 1023,
            "low": [9.8] * 1023,
            "close": [10.2] * 1023,
            "volume": [1000] * 1023,
            "amount": [10200] * 1023,
        }
    )
    monkeypatch.setattr(
        ak_adapter,
        "_load_akshare",
        lambda: SimpleNamespace(
            stock_zh_a_hist_min_sina=lambda **_: sina_1023,
            stock_zh_a_hist_min_em=lambda **_: (_ for _ in ()).throw(RuntimeError("em down")),
        ),
    )
    ak_prov = ak_adapter.AkShareProvider(prefer_sina_bars=True)
    with SQLiteResultCache(tmp_path / "trunc.db") as trunc_cache:
        trunc_hub = DataHub([ak_prov], cache=trunc_cache, attempts_per_provider=1)
        partial_15m = asyncio.run(
            trunc_hub.get_bars(
                "600519",
                Interval.MINUTE_15,
                datetime(2026, 1, 5, 9, 30, tzinfo=BEIJING_TZ),
                recent_start + timedelta(days=15),
                Adjust.QFQ,
            )
        )
        assert partial_15m.metadata.quality is not None
        assert partial_15m.metadata.quality.truncated is True
        assert trunc_cache.stats()["total"] == 0

    # 3. CLI refuses unauthenticated 0.0.0.0 bind unless explicitly overridden.
    monkeypatch.delenv("STOCK_DATA_HUB_API_KEY", raising=False)
    monkeypatch.delenv("STOCK_DATA_HUB_ALLOW_UNAUTHENTICATED", raising=False)
    with pytest.raises(SystemExit, match="Refusing to bind unauthenticated"):
        cli_module.main(["serve", "--host", "0.0.0.0", "--port", "8000"])

    # 4. RemoteHubProvider redacts internal peer hostname/IP from error messages.
    peer = RemoteHubProvider(
        "http://10.24.8.99:9000",
        name="remote-1",
        opener=lambda *a, **k: (_ for _ in ()).throw(URLError("connection refused")),
    )
    with pytest.raises(ProviderUnavailable) as exc_info:
        peer.fetch_quote("600519.SH")
    assert "10.24.8.99" not in str(exc_info.value)
    assert "remote-1" in str(exc_info.value)

    # 5. Doctor partial-market probe (--markets US) does not globally disable multi-market providers.
    class MultiMarketPartialFail(MarketDataProvider):
        name = "multi_src"
        capabilities = frozenset({Capability.QUOTE})
        markets = frozenset({Market.CN, Market.US})

        def fetch_quote(self, symbol: str):
            if symbol.endswith(".US"):
                raise ProviderUnavailable("US blocked")
            raise AssertionError("CN was not probed")

    report = asyncio.run(
        run_doctor(DataHub([MultiMarketPartialFail()]), markets=(Market.US,), timeout_seconds=1.0)
    )
    assert "multi_src" not in report.recommendation.disabled_providers

    # 6. Default hub isolates CNINFO worker pool from AkShare market-data pool & RateLimiter works.
    default_hub = create_default_hub(disabled_providers=["yfinance", "tickflow"])
    registered = {h.provider for h in default_hub.health()}
    assert "cninfo" in registered and "akshare" in registered

    limiter = RateLimiter(rate_per_second=20.0, burst=2)
    assert limiter.reserve() == 0.0
    assert limiter.reserve() == 0.0
    wait = limiter.reserve()
    assert wait is not None and wait > 0.0

    # 7. HTTP service enforces intraday/disclosure window bounds & serves /adjust-factors.
    if TestClient is not None:
        app = create_app(DataHub([peer], attempts_per_provider=1), max_intraday_days=30)
        client = TestClient(app)
        too_wide = client.get(
            "/bars/600519?interval=15m&start=2025-01-01T00:00:00Z&end=2026-01-01T00:00:00Z"
        )
        assert too_wide.status_code == 422
        assert "cannot exceed 30 days" in too_wide.json()["error"]["message"]
        too_wide_disc = client.get(
            "/disclosures/600519?start=2010-01-01&end=2026-01-01"
        )
        assert too_wide_disc.status_code == 422
