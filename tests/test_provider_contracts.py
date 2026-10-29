from __future__ import annotations

import json
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

import stock_data_hub.providers.akshare as ak_adapter
from stock_data_hub.errors import DataValidationError
from stock_data_hub.models import Adjust, Interval, Market
from stock_data_hub.normalization import (
    BEIJING_TZ,
    is_bar_closed,
    normalize_a_share_symbol,
    normalize_symbol,
)
from stock_data_hub.providers._parsing import bar_payload_from_dataframe
from stock_data_hub.providers.efinance import EFinanceProvider
from stock_data_hub.providers.tickflow import TickFlowProvider
from stock_data_hub.providers.yfinance import YFinanceProvider

FIXTURES = json.loads(
    (Path(__file__).parent / "fixtures" / "provider_bars.json").read_text(encoding="utf-8")
)


@pytest.mark.parametrize("raw", ["920002", "920002.BJ", "BJ920002"])
def test_new_beijing_codes_preserve_the_exchange(raw):
    assert normalize_symbol(raw) == "920002.BJ"
    assert normalize_a_share_symbol(raw) == "920002.BJ"
    with pytest.raises(DataValidationError):
        normalize_symbol("920002.SH")


def test_same_numeric_code_is_routed_to_distinct_eastmoney_ids():
    calls = []

    class Stock:
        def get_quote_history(self, code, *, beg, end, klt, fqt, quote_id_mode):
            assert quote_id_mode
            calls.append(code)
            price = 3500 if code == "1.000001" else 10
            return pd.DataFrame(
                {
                    "股票代码": ["000001"],
                    "date": ["2026-07-17"],
                    "open": [price],
                    "high": [price],
                    "low": [price],
                    "close": [price],
                    "volume": [100],
                }
            )

        def get_latest_quote(self, ids, *, quote_id_mode):
            assert quote_id_mode
            return pd.DataFrame({"行情ID": ids, "最新价": [3500 if ids[0] == "1.000001" else 10]})

    provider = EFinanceProvider(client_loader=lambda: SimpleNamespace(stock=Stock()))
    day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)
    index = provider.fetch_bars("000001.SH", Interval.DAY_1, day, day, Adjust.NONE)
    stock = provider.fetch_bars("000001.SZ", Interval.DAY_1, day, day, Adjust.NONE)
    assert calls == ["1.000001", "0.000001"]
    assert index.value[0].close == 3500
    assert stock.value[0].close == 10
    assert provider.fetch_quote("000001.SH").value.price == 3500
    assert provider.fetch_quote("000001.SZ").value.price == 10


def test_efinance_rejects_a_quote_from_the_wrong_exchange():
    stock = SimpleNamespace(
        get_latest_quote=lambda *a, **k: pd.DataFrame({"行情ID": ["0.000001"], "最新价": [10]})
    )
    provider = EFinanceProvider(client_loader=lambda: SimpleNamespace(stock=stock))
    with pytest.raises(DataValidationError, match="unique row"):
        provider.fetch_quote("000001.SH")


def test_eastmoney_and_sina_history_have_identical_share_volumes(monkeypatch):
    day = datetime(2021, 7, 23, tzinfo=BEIJING_TZ)
    frame = pd.DataFrame(FIXTURES["efinance"])
    ef = EFinanceProvider(
        client_loader=lambda: SimpleNamespace(
            stock=SimpleNamespace(get_quote_history=lambda *a, **k: frame)
        )
    )
    eastmoney = ef.fetch_bars("600519.SH", Interval.DAY_1, day, day, Adjust.QFQ)
    sdk = SimpleNamespace(
        stock_zh_a_hist=lambda **kwargs: frame,
        stock_zh_a_daily=lambda **kwargs: pd.DataFrame(FIXTURES["sina_equivalent"]),
    )
    monkeypatch.setattr(ak_adapter, "_load_akshare", lambda: sdk)
    ak = ak_adapter.AkShareProvider()
    ak_eastmoney = ak.fetch_bars("600519.SH", Interval.DAY_1, day, day, Adjust.QFQ)

    def fail(**kwargs):
        raise OSError("Eastmoney unavailable")

    sdk.stock_zh_a_hist = fail
    sina = ak.fetch_bars("600519.SH", Interval.DAY_1, day, day, Adjust.QFQ)
    for result in (eastmoney, ak_eastmoney, sina):
        assert result.value[0].volume == Decimal(4758500)
        assert result.value[0].amount == Decimal(9057762000)
        assert result.quality.volume_unit == "shares"


@pytest.mark.parametrize("sample", FIXTURES["tickflow"])
def test_tickflow_recorded_units_and_trading_dates(sample):
    client = SimpleNamespace(
        klines=SimpleNamespace(get=lambda *a, **k: pd.DataFrame(sample["frame"]))
    )
    provider = TickFlowProvider(client_factory=lambda: client)
    day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)
    result = provider.fetch_bars(sample["symbol"], Interval.DAY_1, day, day, Adjust.NONE)
    assert result.value[0].ts == day
    assert result.value[0].volume == Decimal(sample["expected_volume"])
    assert result.quality.currency == sample["currency"]


@pytest.mark.parametrize(
    ("day", "market", "now", "expected"),
    [
        ("2026-06-01", Market.US, "2026-06-02T00:30:00+08:00", False),
        ("2026-06-01", Market.US, "2026-06-02T04:00:00+08:00", True),
        ("2026-01-05", Market.US, "2026-01-06T04:30:00+08:00", False),
        ("2026-01-05", Market.US, "2026-01-06T05:00:00+08:00", True),
        ("2026-11-27", Market.US, "2026-11-28T02:00:00+08:00", True),
        ("2026-07-17", Market.HK, "2026-07-17T17:00:00+08:00", True),
        ("2026-12-24", Market.HK, "2026-12-24T12:11:00+08:00", True),
    ],
)
def test_exchange_sessions_determine_daily_completion(day, market, now, expected):
    label = datetime.fromisoformat(day).replace(tzinfo=BEIJING_TZ)
    assert is_bar_closed(label, Interval.DAY_1, datetime.fromisoformat(now), market) is expected


def test_holiday_week_and_weekend_month_close_at_the_last_session():
    assert is_bar_closed(
        datetime(2026, 3, 30, tzinfo=BEIJING_TZ),
        Interval.WEEK_1,
        datetime(2026, 4, 3, 4, 0, tzinfo=BEIJING_TZ),
        Market.US,
    )
    assert is_bar_closed(
        datetime(2026, 1, 1, tzinfo=BEIJING_TZ),
        Interval.MONTH_1,
        datetime(2026, 1, 30, 15, 5, tzinfo=BEIJING_TZ),
        Market.CN,
    )


def test_yahoo_date_labels_match_other_providers_at_exact_daily_boundaries():
    frame = pd.DataFrame(
        {"Open": [10], "High": [11], "Low": [9], "Close": [10]},
        index=pd.DatetimeIndex(
            [datetime(2026, 7, 17, tzinfo=ZoneInfo("America/New_York"))], name="Date"
        ),
    )
    provider = YFinanceProvider(ticker_factory=lambda _: SimpleNamespace(history=lambda **k: frame))
    day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)
    result = provider.fetch_bars("AAPL.US", Interval.DAY_1, day, day, Adjust.NONE)
    assert len(result.value) == 1
    assert result.value[0].ts == day


def test_adjusted_negative_prices_survive_and_invalid_rows_are_reported():
    start = datetime(2026, 7, 13, tzinfo=BEIJING_TZ)
    frame = pd.DataFrame(
        {
            "date": ["2026-07-13", "2026-07-14", "2026-07-15"],
            "open": [-10, 10, 10],
            "high": [-8, 12, 12],
            "low": [-12, 9, 9],
            "close": [-9, 11, "NaN"],
        }
    )
    arguments = {
        "source": "fixture",
        "symbol": "600519.SH",
        "interval": Interval.DAY_1,
        "start": start,
        "end": start + timedelta(days=2),
        "market": Market.CN,
        "time_columns": ("date",),
    }
    result = bar_payload_from_dataframe(frame, adjust=Adjust.QFQ, **arguments)
    assert [bar.close for bar in result.value] == [Decimal(-9), Decimal(11)]
    assert result.quality.filtered_rows == 1
    assert result.quality.input_rows == 3
    assert result.quality.returned_rows == 2
    assert result.quality.missing_rows is None
    assert "Filtered 1" in result.warnings[0]
    unadjusted = bar_payload_from_dataframe(frame, adjust=Adjust.NONE, **arguments)
    assert unadjusted.quality.filtered_rows == 2


def test_tickflow_paginates_inside_the_requested_history_and_marks_limits():
    days = [datetime(2026, 7, day, tzinfo=BEIJING_TZ) for day in (13, 14, 15, 16)]
    calls = []
    closed = []

    def get(symbol, *, start_time, end_time, count, period, adjust, as_dataframe):
        calls.append((start_time, end_time))
        selected = [day for day in days if start_time <= int(day.timestamp() * 1000) <= end_time][
            -count:
        ]
        return pd.DataFrame(
            [
                {
                    "timestamp": int(day.timestamp() * 1000),
                    "trade_date": day.date().isoformat(),
                    "open": 10,
                    "high": 11,
                    "low": 9,
                    "close": 10,
                    "volume": 1,
                }
                for day in selected
            ]
        )

    def client():
        return SimpleNamespace(klines=SimpleNamespace(get=get), close=lambda: closed.append(True))

    provider = TickFlowProvider(client_factory=client, page_size=2, max_rows=10)
    result = provider.fetch_bars("600519.SH", Interval.DAY_1, days[0], days[-1], Adjust.NONE)
    assert [bar.ts for bar in result.value] == days
    assert len(calls) == 2 and calls[1][1] < calls[0][1]
    assert result.quality.truncated is False
    assert closed == [True]
    limited = TickFlowProvider(client_factory=client, page_size=2, max_rows=2)
    partial = limited.fetch_bars("600519.SH", Interval.DAY_1, days[0], days[-1], Adjust.NONE)
    assert partial.quality.truncated is True
    assert partial.quality.available_start == days[2]
    assert any("partial" in warning for warning in partial.warnings)
