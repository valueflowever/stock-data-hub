from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from stock_data_hub.errors import DataValidationError
from stock_data_hub.models import Adjust, Interval, Market
from stock_data_hub.normalization import (
    BEIJING_TZ,
    is_bar_closed,
    normalize_a_share_symbol,
    normalize_symbol,
)
from stock_data_hub.providers._parsing import bar_payload_from_dataframe
from stock_data_hub.providers.yfinance import YFinanceProvider


@pytest.mark.parametrize("raw", ["920002", "920002.BJ", "BJ920002"])
def test_new_beijing_codes_preserve_the_exchange(raw):
    assert normalize_symbol(raw) == "920002.BJ"
    assert normalize_a_share_symbol(raw) == "920002.BJ"
    with pytest.raises(DataValidationError):
        normalize_symbol("920002.SH")


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
