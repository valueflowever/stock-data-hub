"""Regression tests for Data Contract 2.1, quant semantics, concurrency, caching, and API safety."""

from __future__ import annotations

import asyncio
import io
import sqlite3
import time
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from urllib.error import HTTPError
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

try:
    from fastapi.testclient import TestClient
except ModuleNotFoundError:
    TestClient = None

import stock_data_hub._workers as workers_mod
import stock_data_hub.hub as hub_mod
import stock_data_hub.providers.akshare as ak_adapter
from stock_data_hub import (
    DATA_CONTRACT_VERSION,
    Adjust,
    AdjustFactor,
    AllSourcesFailed,
    AShareBar,
    AssetType,
    BaoStockProvider,
    Capability,
    DataHub,
    DataValidationError,
    EFinanceProvider,
    IndicatorEngine,
    IndicatorSpec,
    Interval,
    InvalidRequest,
    Market,
    NoProviderConfigured,
    ProviderUnavailable,
    PyTDXProvider,
    RemoteHubProvider,
    SQLiteResultCache,
    TencentQuoteProvider,
    UnsupportedCapability,
    YFinanceProvider,
    __version__,
    create_app,
    create_default_hub,
)
from stock_data_hub._workers import BoundedWorkers
from stock_data_hub.cli import _is_loopback_host
from stock_data_hub.models import AdjustMethod, SourcePayload
from stock_data_hub.normalization import BEIJING_TZ, is_bar_closed, normalize_symbol
from stock_data_hub.providers._parsing import normalize_bar_timestamp
from stock_data_hub.sqlite_cache import PERSISTENT_CACHE_POLICIES
from stock_data_hub.trading_sessions import window_has_trading_session


def test_contract_2_1_adjust_factors_cn_hk_us_and_strict_offset_validation(
    monkeypatch, tmp_path: Path
) -> None:
    assert DATA_CONTRACT_VERSION == "2.1"
    assert __version__ == "0.8.0"

    # 1. CN: BaoStock (multiplier 0.8) and AkShare Sina (divisor 1.25 -> 1/1.25 = 0.8) reconcile
    # with explicit qfq_offset=0, hfq_offset=0.
    class BsFactorResult:
        error_code = "0"
        fields = ("code", "dividOperateDate", "foreAdjustFactor", "backAdjustFactor")

        def __init__(self) -> None:
            self._done = False

        def next(self) -> bool:
            if self._done:
                return False
            self._done = True
            return True

        def get_row_data(self) -> list[str]:
            return ["sh.600519", "2026-07-16", "0.8", "12.5"]

    bs = BaoStockProvider(
        client_loader=lambda: SimpleNamespace(
            login=lambda: SimpleNamespace(error_code="0"),
            query_adjust_factor=lambda **_: BsFactorResult(),
            logout=lambda: None,
        )
    )
    fake_ak = SimpleNamespace(
        stock_zh_a_hist_sina=lambda **kwargs: (
            pd.DataFrame({"date": ["2026-07-16"], "qfq_factor": ["1.25"]})
            if kwargs.get("adjust") == "qfq-factor"
            else pd.DataFrame({"date": ["2026-07-16"], "hfq_factor": ["12.5"]})
        ),
        stock_hk_hist_sina=lambda **kwargs: (
            pd.DataFrame({"date": ["2026-07-16"], "qfq_factor": ["0.95"]})
            if kwargs.get("adjust") == "qfq-factor"
            else pd.DataFrame(
                {"date": ["2026-07-16"], "hfq_factor": ["1.10"], "cash": ["2.35"]}
            )
        ),
    )
    monkeypatch.setattr(ak_adapter, "_load_akshare", lambda: fake_ak)
    ak_prov = ak_adapter.AkShareProvider()

    bs_factors = bs.fetch_adjust_factors("600519.SH", date(2026, 7, 1), date(2026, 7, 31)).value
    ak_factors = ak_prov.fetch_adjust_factors(
        "600519.SH", date(2026, 7, 1), date(2026, 7, 31)
    ).value
    expected_cn = [
        AdjustFactor(
            symbol="600519.SH",
            ex_date=date(2026, 7, 16),
            qfq_factor=Decimal("0.8"),
            hfq_factor=Decimal("12.5"),
            qfq_offset=Decimal(0),
            hfq_offset=Decimal(0),
            market=Market.CN,
        )
    ]
    assert bs_factors == expected_cn
    assert ak_factors == expected_cn

    # 2. HK: Preserves affine cash offset on hfq; refuses missing cash offset instead of assuming 0.
    hk_factors = ak_prov.fetch_adjust_factors(
        "00700.HK", date(2026, 7, 1), date(2026, 7, 31)
    ).value
    assert hk_factors == [
        AdjustFactor(
            symbol="00700.HK",
            ex_date=date(2026, 7, 16),
            qfq_factor=Decimal("0.95"),
            hfq_factor=Decimal("1.10"),
            qfq_offset=Decimal(0),
            hfq_offset=Decimal("2.35"),
            market=Market.HK,
        )
    ]

    with SQLiteResultCache(tmp_path / "factors_2_1.db") as cache:
        hub = DataHub([ak_prov], cache=cache, attempts_per_provider=1)
        hk_res = asyncio.run(hub.get_adjust_factors("00700.HK"))
        hk_cached = asyncio.run(hub.get_adjust_factors("00700.HK"))
        assert hk_cached.metadata.cache_hit
        assert hk_cached.value == hk_res.value

    # Missing cash column on HK hfq-factor must raise DataValidationError, never default to 0.
    fake_ak.stock_hk_hist_sina = lambda **kwargs: (
        pd.DataFrame({"date": ["2026-07-16"], "qfq_factor": ["0.95"]})
        if kwargs.get("adjust") == "qfq-factor"
        else pd.DataFrame({"date": ["2026-07-16"], "hfq_factor": ["1.10"]})
    )
    with pytest.raises(DataValidationError, match="cash offset"):
        ak_prov.fetch_adjust_factors("00700.HK")

    # 3. US: AkShare does not pretend to support US adjust factors or US HFQ daily closes.
    assert not ak_prov.supports_request(Capability.ADJUST_FACTORS, market=Market.US)
    with pytest.raises(UnsupportedCapability):
        ak_prov.fetch_adjust_factors("AAPL.US")
    with pytest.raises(NoProviderConfigured):
        asyncio.run(DataHub([ak_prov]).get_adjust_factors("AAPL.US"))
    with pytest.raises(UnsupportedCapability):
        ak_prov.fetch_us_daily_closes("AAPL", 10, adjust=Adjust.HFQ)

    # 4. Hub contract validator rejects factor without explicit offset (None != 0).
    class MissingOffsetProvider( bs.__class__.__bases__[0] ):  # type: ignore[misc]
        name = "bad_offset"
        capabilities = frozenset({Capability.ADJUST_FACTORS})

        def fetch_adjust_factors(self, symbol, start=None, end=None):
            return SourcePayload(
                [
                    AdjustFactor(
                        symbol=symbol,
                        ex_date=date(2026, 7, 16),
                        qfq_factor=Decimal("0.9"),
                        qfq_offset=None,
                        market=Market.HK,
                    )
                ],
                source="bad_offset",
            )

    with pytest.raises(AllSourcesFailed, match="never substitute 0"):
        asyncio.run(DataHub([MissingOffsetProvider()], attempts_per_provider=1).get_adjust_factors("00700.HK"))


def test_weekly_and_monthly_period_end_anchors_and_index_raw_adjust_method() -> None:
    ny = ZoneInfo("America/New_York")

    # Yahoo stamps Monday 2026-07-13 for weekly and 2026-07-01 for monthly;
    # US Good Friday week (Monday 2026-03-30, Good Friday 2026-04-03 closed) anchors to 2026-04-02.
    class YahooTicker:
        def history(self, *, interval: str, start: str, **_: object) -> pd.DataFrame:
            if interval == "1wk" and "2026-03" in start:
                dates = [datetime(2026, 3, 30, tzinfo=ny)]
            elif interval == "1wk":
                dates = [datetime(2026, 7, 13, tzinfo=ny)]
            else:
                dates = [datetime(2026, 7, 1, tzinfo=ny)]
            return pd.DataFrame(
                {"Open": [100], "High": [110], "Low": [95], "Close": [105], "Volume": [1000]},
                index=pd.DatetimeIndex(dates, name="Date"),
            )

    yf = YFinanceProvider(ticker_factory=lambda _: YahooTicker())
    ef = EFinanceProvider(
        client_loader=lambda: SimpleNamespace(
            stock=SimpleNamespace(
                # Developing mid-week Wednesday 2026-07-15 bar from Eastmoney
                get_quote_history=lambda *a, **k: pd.DataFrame(
                    {
                        "股票代码": ["600519"],
                        "日期": ["2026-07-15"],
                        "开盘": [1400],
                        "最高": [1420],
                        "最低": [1390],
                        "收盘": [1410],
                        "成交量": [100],
                    }
                )
            )
        )
    )

    hub_us = DataHub([yf], attempts_per_provider=1)
    # Querying mid-week (Mon 07-13 to Wed 07-15) keeps the week bar anchored at Fri 2026-07-17!
    us_wk = asyncio.run(
        hub_us.get_bars(
            "AAPL.US",
            Interval.WEEK_1,
            datetime(2026, 7, 13, tzinfo=BEIJING_TZ),
            datetime(2026, 7, 15, tzinfo=BEIJING_TZ),
        )
    )
    assert [b.ts for b in us_wk.value] == [datetime(2026, 7, 17, tzinfo=BEIJING_TZ)]

    us_good_friday_wk = asyncio.run(
        hub_us.get_bars(
            "AAPL.US",
            Interval.WEEK_1,
            datetime(2026, 3, 30, tzinfo=BEIJING_TZ),
            datetime(2026, 4, 3, tzinfo=BEIJING_TZ),
        )
    )
    assert [b.ts for b in us_good_friday_wk.value] == [datetime(2026, 4, 2, tzinfo=BEIJING_TZ)]

    us_mo = asyncio.run(
        hub_us.get_bars(
            "AAPL.US",
            Interval.MONTH_1,
            datetime(2026, 7, 1, tzinfo=BEIJING_TZ),
            datetime(2026, 7, 15, tzinfo=BEIJING_TZ),
        )
    )
    assert [b.ts for b in us_mo.value] == [datetime(2026, 7, 31, tzinfo=BEIJING_TZ)]

    cn_wk = asyncio.run(
        DataHub([ef], attempts_per_provider=1).get_bars(
            "600519.SH",
            Interval.WEEK_1,
            datetime(2026, 7, 13, tzinfo=BEIJING_TZ),
            datetime(2026, 7, 15, tzinfo=BEIJING_TZ),
        )
    )
    assert [b.ts for b in cn_wk.value] == [datetime(2026, 7, 17, tzinfo=BEIJING_TZ)]

    # Index bars always report AdjustMethod.RAW even if Adjust.QFQ was requested.
    idx_ef = EFinanceProvider(
        client_loader=lambda: SimpleNamespace(
            stock=SimpleNamespace(
                get_quote_history=lambda *a, **k: pd.DataFrame(
                    {
                        "股票代码": ["000001"],
                        "日期": ["2026-07-17"],
                        "开盘": [3500],
                        "最高": [3520],
                        "最低": [3490],
                        "收盘": [3510],
                        "成交量": [1000],
                    }
                )
            )
        )
    )
    idx_bars = idx_ef.fetch_bars(
        "000001.SH",
        Interval.DAY_1,
        datetime(2026, 7, 17, tzinfo=BEIJING_TZ),
        datetime(2026, 7, 17, tzinfo=BEIJING_TZ),
        Adjust.QFQ,
        asset_type=AssetType.INDEX,
    )
    assert idx_bars.quality is not None
    assert idx_bars.quality.adjust_method == AdjustMethod.RAW.value


def test_default_instruments_priority_and_livez_endpoint() -> None:
    hub = create_default_hub(disabled_providers=["yfinance", "tickflow"])
    assert hub._priority_for(Capability.INSTRUMENTS, None, None, None, None) == (
        "akshare",
        "efinance",
    )
    hub.close()

    if TestClient is not None:
        app = create_app(DataHub(), api_key="strict-secret")
        client = TestClient(app, raise_server_exceptions=False)
        # /livez is unauthenticated for container probes; /health & /metrics remain protected.
        livez = client.get("/livez")
        assert livez.status_code == 200
        assert livez.json() == {"status": "ok"}
        assert client.get("/health").status_code == 401
        assert client.get("/metrics").status_code == 401

        # Mixed naive + aware datetimes on /bars never crash with HTTP 500.
        mixed = client.get(
            "/bars/600519?interval=15m&start=2026-07-01T09:30:00&end=2026-07-02T15:00:00%2B08:00",
            headers={"X-Data-Hub-Key": "strict-secret"},
        )
        assert mixed.status_code == 503  # NoProviderConfigured on empty DataHub, NOT 500!

        # /indicator-computations enforces max_intraday_days symmetrically with /bars.
        limited_client = TestClient(create_app(DataHub(), max_intraday_days=30))
        too_wide_ind = limited_client.post(
            "/indicator-computations",
            json={
                "symbol": "600519",
                "interval": "15m",
                "start": "2026-01-01T09:30:00+08:00",
                "end": "2026-03-15T15:00:00+08:00",
                "adjust": "qfq",
                "indicators": [{"id": "overlap.sma", "params": {"period": 5}}],
            },
        )
        assert too_wide_ind.status_code == 422
        assert "cannot exceed 30 days" in too_wide_ind.json()["error"]["message"]


def test_missing_symbols_do_not_poison_quote_circuit_breakers_and_batch_prefetch_falls_back() -> None:
    ef = EFinanceProvider(
        client_loader=lambda: SimpleNamespace(
            stock=SimpleNamespace(
                get_latest_quote=lambda ids, quote_id_mode: (
                    pd.DataFrame({"行情ID": ["1.600519"], "最新价": ["1400.0"]})
                    if "1.600519" in ids
                    else pd.DataFrame()
                )
            )
        )
    )
    tc_urls: list[str] = []

    def tencent_opener(req, *, timeout):
        tc_urls.append(req.full_url)
        lines = []
        if "sh600519" in req.full_url:
            lines.append('v_sh600519="1~茅台~600519~1400.00~~~~~~~~~~~~~~~~~~~~~~~~~~~20260717150000~";')
        if "sz000001" in req.full_url:
            lines.append('v_sz000001="51~平安~000001~11.20~~~~~~~~~~~~~~~~~~~~~~~~~~~20260717150000~";')
        body = "\n".join(lines) if lines else 'v_pv_none_match="1";'
        return io.BytesIO(body.encode("gb18030"))

    tc = TencentQuoteProvider(opener=tencent_opener)
    hub = DataHub([ef, tc], attempts_per_provider=2, failure_threshold=2)

    batch = asyncio.run(hub.get_quotes(["688997.SH", "688998.SH", "688999.SH"]))
    assert all(item.error is not None for item in batch)
    assert all(h.status == "closed" and h.failure_count == 0 for h in hub.health())

    # Valid symbol immediately succeeds!
    valid = asyncio.run(hub.get_quote("600519.SH"))
    assert valid.value.price == Decimal("1400.0")

    # When primary batch provider (efinance) is down, batch prefetch falls back to tencent in 1 call!
    broken_ef = EFinanceProvider(
        client_loader=lambda: SimpleNamespace(
            stock=SimpleNamespace(
                get_latest_quote=lambda *a, **k: (_ for _ in ()).throw(RuntimeError("em blocked"))
            )
        )
    )
    tc_urls.clear()
    tc.clear_cache()
    overseas_hub = DataHub([broken_ef, tc], attempts_per_provider=1, failure_threshold=1)
    res = asyncio.run(overseas_hub.get_quotes(["600519.SH", "000001.SZ"]))
    assert [item.data.price for item in res if item.data] == [Decimal("1400.00"), Decimal("11.20")]
    assert any("sh600519,sz000001" in u for u in tc_urls)


def test_indicator_warmup_over_remote_peer_stays_within_span_limit() -> None:
    pytest.importorskip("talib")
    if TestClient is None:
        pytest.skip("requires service extra")

    base = datetime(2026, 6, 1, 9, 45, tzinfo=BEIJING_TZ)
    peer_bars = [
        AShareBar(
            symbol="600519.SH",
            ts=base + timedelta(days=d, minutes=15 * m),
            open=Decimal(100),
            high=Decimal(105),
            low=Decimal(99),
            close=Decimal(100 + (d + m) % 5),
            volume=Decimal(1000),
            amount=Decimal(101000),
            is_closed=True,
            market=Market.CN,
        )
        for d in range(25)
        for m in range(4)  # 100 bars (< 270 warmup for ADX(14))
    ]

    class PeerSource(EFinanceProvider.__bases__[0]):
        name = "peer_src"
        capabilities = frozenset({Capability.BARS})
        markets = frozenset({Market.CN})

        def fetch_bars(self, symbol, interval, start, end, adjust):
            return SourcePayload(
                [b for b in peer_bars if start <= b.ts <= end], source="peer_src:em"
            )

    peer_client = TestClient(create_app(DataHub([PeerSource()], attempts_per_provider=1)))

    def opener(request, *, timeout: float):
        url = request.full_url.replace("http://peer.internal", "")
        r = peer_client.get(url, headers=dict(request.header_items()))
        if r.status_code >= 400:
            raise HTTPError(request.full_url, r.status_code, r.reason_phrase, {}, io.BytesIO(r.content))
        return io.BytesIO(r.content)

    remote = RemoteHubProvider("http://peer.internal", name="remote-1", opener=opener)
    hub = DataHub([remote], attempts_per_provider=1, failure_threshold=1)
    engine = IndicatorEngine(hub)
    last_ts = peer_bars[-1].ts

    result = asyncio.run(
        engine.calculate(
            "600519.SH",
            Interval.MINUTE_15,
            last_ts,
            last_ts,
            [IndicatorSpec("trend.adx", {"period": 14})],
            adjust=Adjust.QFQ,
        )
    )
    assert len(result.value.rows) == 1
    health = {h.provider: h for h in hub.health()}["remote-1"]
    assert health.status == "closed"
    assert health.failure_count == 0


def test_timezone_symbol_cache_sqlite_workers_and_provider_safety_regressions(
    monkeypatch, tmp_path: Path
) -> None:
    edt = ZoneInfo("America/New_York")
    # 1. US Friday 12:00-13:00 EDT (Beijing Saturday 00:00-01:00) is an active intraday window!
    us_noon = datetime(2026, 7, 17, 12, 0, tzinfo=edt).astimezone(BEIJING_TZ)
    us_1pm = datetime(2026, 7, 17, 13, 0, tzinfo=edt).astimezone(BEIJING_TZ)
    assert window_has_trading_session(
        us_noon,
        us_1pm,
        Market.US,
        now=datetime(2026, 7, 17, 17, 0, tzinfo=edt),
        interval=Interval.MINUTE_15,
    )

    # Fixed-offset +08:00 fromisoformat matches ZoneInfo("Asia/Shanghai")
    tue_morning = datetime(2026, 7, 21, 10, 0, tzinfo=BEIJING_TZ)
    assert not window_has_trading_session(
        datetime.fromisoformat("2026-07-21T00:00:00+08:00"),
        datetime.fromisoformat("2026-07-21T00:00:00+08:00"),
        Market.US,
        now=tue_morning,
    )

    # XSHG 2026-12-28 cross-year week closes after Thursday 2026-12-31 15:05+08:00
    assert is_bar_closed(
        datetime(2026, 12, 28, tzinfo=BEIJING_TZ),
        Interval.WEEK_1,
        now=datetime(2026, 12, 31, 16, 0, tzinfo=BEIJING_TZ),
        market=Market.CN,
    )
    # Canonical Beijing-midnight daily labels remain idempotent for US/HK markets
    us_canonical_day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)
    assert (
        normalize_bar_timestamp(
            us_canonical_day,
            Interval.DAY_1,
            intraday_timestamp_label=None,
            market=Market.US,
        )
        == us_canonical_day
    )
    assert _is_loopback_host("[::1]") and _is_loopback_host("127.0.0.2")
    assert not _is_loopback_host("0.0.0.0")

    # 2. Symbol normalization prevents silent .US misrouting
    assert normalize_symbol("sh600519.sh") == "600519.SH"
    assert normalize_symbol("SH000001.SH") == "000001.SH"
    assert normalize_symbol("HK00700") == "00700.HK"
    assert normalize_symbol("BRK.B") == "BRK.B.US"
    with pytest.raises(InvalidRequest):
        normalize_symbol("AAPL.NASDAQ")
    with pytest.raises(InvalidRequest):
        normalize_symbol("600519.SS")

    # 3. InMemoryResultCache list immutability & EOD lag not blocking live recovery
    yesterday = datetime(2026, 7, 16, tzinfo=BEIJING_TZ)
    today = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)
    monkeypatch.setattr(hub_mod, "latest_active_session_date", lambda market: date(2026, 7, 17))

    class EodOnly(BaoStockProvider.__bases__[0]):
        name = "tickflow"
        capabilities = frozenset({Capability.BARS})
        markets = frozenset({Market.CN})
        supports_intraday_daily_bar = False

        def fetch_bars(self, symbol, interval, start, end, adjust):
            return SourcePayload(
                [AShareBar(symbol, yesterday, Decimal(10), Decimal(11), Decimal(9), Decimal(10))],
                source="tickflow:free",
            )

    class RecoveringLive(BaoStockProvider.__bases__[0]):
        name = "efinance"
        capabilities = frozenset({Capability.BARS})
        markets = frozenset({Market.CN})

        def __init__(self) -> None:
            self.fail = True

        def fetch_bars(self, symbol, interval, start, end, adjust):
            if self.fail:
                raise ProviderUnavailable("blip")
            return SourcePayload(
                [
                    AShareBar(symbol, yesterday, Decimal(10), Decimal(11), Decimal(9), Decimal(10)),
                    AShareBar(symbol, today, Decimal(10), Decimal(12), Decimal(10), Decimal(11)),
                ],
                source="efinance:eastmoney",
            )

    live = RecoveringLive()
    with SQLiteResultCache(tmp_path / "eod.db") as sq:
        hub = DataHub([EodOnly(), live], cache=sq, cache_policies=PERSISTENT_CACHE_POLICIES, attempts_per_provider=1)
        lagged = asyncio.run(hub.get_bars("600519", Interval.DAY_1, yesterday, today))
        assert lagged.metadata.source == "tickflow:free"
        live.fail = False
        recovered = asyncio.run(hub.get_bars("600519", Interval.DAY_1, yesterday, today))
        assert not recovered.metadata.cache_hit
        assert recovered.metadata.source == "efinance:eastmoney"
        assert len(recovered.value) == 2

    mem_hub = DataHub([RecoveringLive()], attempts_per_provider=1)
    mem_hub._providers["efinance"].fail = False  # type: ignore[attr-defined]
    r1 = asyncio.run(mem_hub.get_bars("600519", Interval.DAY_1, yesterday, today))
    r1.value.clear()
    r2 = asyncio.run(mem_hub.get_bars("600519", Interval.DAY_1, yesterday, today))
    assert r2.metadata.cache_hit and len(r2.value) == 2

    # 4. SQLiteResultCache survives write-lock contention on lazy touch & uses index scan
    db_file = tmp_path / "wal_lock.db"
    with SQLiteResultCache(db_file, busy_timeout_ms=10, touch_interval_seconds=0.0) as sq_cache:
        sq_cache.set("k1", "v1", timedelta(minutes=5), timedelta(hours=1))
        plan = sq_cache._conn.execute(
            "EXPLAIN QUERY PLAN DELETE FROM cache_entries WHERE stale_until <= ?",
            (time.time(),),
        ).fetchall()
        assert all("SCAN" not in str(row) for row in plan)

        blocker = sqlite3.connect(str(db_file), timeout=0.01)
        blocker.execute("BEGIN IMMEDIATE")
        blocker.execute(
            "INSERT INTO cache_entries VALUES ('lock', 'v', X'789c', 9999999999, 9999999999, 0)"
        )
        try:
            assert sq_cache.get_fresh("k1") == "v1"
            assert sq_cache.get_stale("k1") == "v1"
        finally:
            blocker.rollback()
            blocker.close()

    # 5. BoundedWorkers: Thread.start failure & RateLimiter slot/refund safety
    workers = BoundedWorkers(2, name="safe-workers")
    busy_evt, hold_evt = Event(), Event()

    async def verify_workers() -> None:
        t1 = asyncio.create_task(
            workers.run(lambda: (busy_evt.set(), hold_evt.wait(2), "ok")[2], timeout=2.0)
        )
        await asyncio.sleep(0.03)
        assert busy_evt.is_set()
        victim_ran = False

        def victim() -> None:
            nonlocal victim_ran
            victim_ran = True

        workers._work_queue.put(victim)
        with monkeypatch.context() as mp:
            mp.setattr(
                workers_mod.Thread,
                "start",
                lambda self: (_ for _ in ()).throw(RuntimeError("no thread")),
            )
            # Existing live worker thread drains both victim and new task without semaphore error!
            hold_evt.set()
            assert await t1 == "ok"
            assert await workers.run(lambda: "second", timeout=1.0) == "second"
            assert victim_ran is True

        rl_w = BoundedWorkers(2, name="rl", max_rps=0.5)
        await rl_w.run(lambda: 1, timeout=5.0)
        await rl_w.run(lambda: 2, timeout=5.0)
        c1 = asyncio.create_task(rl_w.run(lambda: 3, timeout=10.0))
        await asyncio.sleep(0.02)
        # Sleeping on RateLimiter must NOT hold _slots!
        assert rl_w._slots._value == 2
        c1.cancel()
        await asyncio.gather(c1, return_exceptions=True)
        assert rl_w._rate_limiter is not None and rl_w._rate_limiter._tokens >= 0.0

    asyncio.run(verify_workers())

    # 6. PyTDX intraday pagination, Sina truncation -> Eastmoney fallback, Remote URL encoding & 'Z'
    pages: list[int] = []

    class PagedTdx:
        def connect(self, *a, **k):
            return True

        def disconnect(self):
            pass

        def get_security_bars(self, category, market, code, offset, count):
            pages.append(offset)
            if offset == 0:
                return [
                    {"datetime": "2026-07-17 14:45:00", "open": 10, "high": 11, "low": 9, "close": 10, "vol": 10, "amount": 1000},
                    {"datetime": "2026-07-17 15:00:00", "open": 10, "high": 11, "low": 9, "close": 10, "vol": 10, "amount": 1000},
                ]
            return [
                {"datetime": "2026-07-17 09:45:00", "open": 10, "high": 11, "low": 9, "close": 10, "vol": 10, "amount": 1000},
            ]

    tdx = PyTDXProvider("127.0.0.1", client_factory=PagedTdx, page_size=2, max_rows=10)
    tdx_res = tdx.fetch_bars(
        "600519.SH",
        Interval.MINUTE_15,
        datetime(2026, 7, 17, 9, 45, tzinfo=BEIJING_TZ),
        datetime(2026, 7, 17, 15, 0, tzinfo=BEIJING_TZ),
        Adjust.NONE,
    )
    assert pages == [0, 2]
    assert len(tdx_res.value) == 3

    # Sina 1023 truncation falls back to complete Eastmoney minute history
    recent_start = datetime(2026, 7, 1, 9, 45, tzinfo=BEIJING_TZ)
    sina_1023 = pd.DataFrame(
        {
            "date": [(recent_start + timedelta(minutes=15 * i)).strftime("%Y-%m-%d %H:%M:%S") for i in range(1023)],
            "open": [10.0] * 1023,
            "high": [10.5] * 1023,
            "low": [9.8] * 1023,
            "close": [10.2] * 1023,
            "volume": [1000] * 1023,
            "amount": [10200] * 1023,
        }
    )
    em_complete = pd.DataFrame(
        {
            "时间": ["2026-01-05 09:45:00", "2026-07-10 15:00:00"],
            "开盘": [10.0, 10.2],
            "最高": [10.5, 10.6],
            "最低": [9.8, 10.0],
            "收盘": [10.2, 10.4],
            "成交量": [10, 20],
            "成交额": [10200, 20800],
        }
    )
    monkeypatch.setattr(
        ak_adapter,
        "_load_akshare",
        lambda: SimpleNamespace(
            stock_zh_a_hist_min_sina=lambda **_: sina_1023,
            stock_zh_a_hist_min_em=lambda **_: em_complete,
        ),
    )
    ak_fb = ak_adapter.AkShareProvider(prefer_sina_bars=True).fetch_bars(
        "600519.SH",
        Interval.MINUTE_15,
        datetime(2026, 1, 5, 9, 30, tzinfo=BEIJING_TZ),
        datetime(2026, 7, 10, 15, 0, tzinfo=BEIJING_TZ),
        Adjust.QFQ,
    )
    assert ak_fb.source == "akshare:eastmoney"
    assert ak_fb.quality is not None and not ak_fb.quality.truncated

    # RemoteHubProvider URL-encodes path segments and parses trailing 'Z' timestamps
    seen_urls: list[str] = []

    def z_opener(req, *, timeout):
        seen_urls.append(req.full_url)
        payload = (
            b'{"data":{"symbol":"600519.SH","industry":"Spirits","main_business":"Liquor","business_scope":null},'
            b'"metadata":{"source":"cninfo","fetched_at":"2026-07-17T08:00:00Z","latency_ms":1.0,'
            b'"attempted_sources":["cninfo"],"cache_hit":false,"stale":false,"warnings":[],"contract_version":"2.1"}}'
        )
        return io.BytesIO(payload)

    peer = RemoteHubProvider("https://peer.example/api", api_key="secret", opener=z_opener)
    with pytest.raises(InvalidRequest):
        peer.fetch_company_profile("..")
    prof = peer.fetch_company_profile("../metrics?x=1#f")
    assert prof.fetched_at == datetime(2026, 7, 17, 16, 0, tzinfo=BEIJING_TZ)
    assert "../metrics" not in seen_urls[0]
    assert "..%2Fmetrics%3Fx%3D1%23f" in seen_urls[0]
