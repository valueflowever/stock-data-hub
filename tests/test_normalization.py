from __future__ import annotations

from datetime import datetime

import pytest

from stock_data_hub.errors import DataValidationError
from stock_data_hub.models import AssetType, Interval, Market
from stock_data_hub.normalization import (
    BEIJING_TZ,
    asset_type_from_symbol,
    is_bar_closed,
    market_from_symbol,
    normalize_a_share_symbol,
    normalize_symbol,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("600519", "600519.SH"),
        ("sh600519", "600519.SH"),
        ("000001.SZ", "000001.SZ"),
        ("430047", "430047.BJ"),
    ],
)
def test_normalize_a_share_symbol(raw: str, expected: str) -> None:
    assert normalize_a_share_symbol(raw) == expected


def test_normalize_rejects_unknown_symbol() -> None:
    with pytest.raises(DataValidationError):
        normalize_a_share_symbol("700001")


@pytest.mark.parametrize("raw", ["600519.SZ", "company-600519", "600519.NYSE"])
def test_normalize_rejects_ambiguous_or_wrong_exchange_symbols(raw: str) -> None:
    with pytest.raises(DataValidationError):
        normalize_a_share_symbol(raw)


@pytest.mark.parametrize(
    ("raw", "market", "expected"),
    [
        ("510300", None, "510300.SH"),
        ("159915", None, "159915.SZ"),
        ("000001.SH", None, "000001.SH"),
        ("700", Market.HK, "00700.HK"),
        ("AAPL", Market.US, "AAPL.US"),
        ("MSFT", None, "MSFT.US"),
    ],
)
def test_normalize_symbol_supports_etf_and_global_suffixes(
    raw: str,
    market: Market | None,
    expected: str,
) -> None:
    assert normalize_symbol(raw, market) == expected


def test_market_is_derived_from_normalized_symbol() -> None:
    assert market_from_symbol("00700.HK") == Market.HK
    assert market_from_symbol("AAPL.US") == Market.US
    assert market_from_symbol("600519.SH") == Market.CN


@pytest.mark.parametrize(
    ("symbol", "expected"),
    [
        ("600519.SH", AssetType.STOCK),
        ("510300.SH", AssetType.ETF),
        ("159915.SZ", AssetType.ETF),
        ("000001.SH", AssetType.INDEX),
        ("399001.SZ", AssetType.INDEX),
        ("AAPL.US", AssetType.STOCK),
    ],
)
def test_asset_type_is_inferred_for_provider_routing(
    symbol: str,
    expected: AssetType,
) -> None:
    assert asset_type_from_symbol(symbol) == expected


def test_normalize_symbol_keeps_sh_indices_but_rejects_wrong_stock_exchange() -> None:
    assert normalize_symbol("000001.SH") == "000001.SH"
    with pytest.raises(DataValidationError):
        normalize_symbol("600519.SZ")


def test_daily_bar_is_closed_after_a_share_close() -> None:
    bar_time = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)
    before_close = datetime(2026, 7, 17, 15, 4, tzinfo=BEIJING_TZ)
    after_close = datetime(2026, 7, 17, 15, 5, tzinfo=BEIJING_TZ)
    assert not is_bar_closed(bar_time, Interval.DAY_1, before_close)
    assert is_bar_closed(bar_time, Interval.DAY_1, after_close)


def test_intraday_bar_timestamp_marks_its_close() -> None:
    fifteen_minute_bar = datetime(2026, 7, 17, 10, 0, tzinfo=BEIJING_TZ)
    hourly_bar = datetime(2026, 7, 17, 10, 0, tzinfo=BEIJING_TZ)

    assert not is_bar_closed(
        fifteen_minute_bar,
        Interval.MINUTE_15,
        datetime(2026, 7, 17, 9, 59, tzinfo=BEIJING_TZ),
    )
    assert is_bar_closed(
        fifteen_minute_bar,
        Interval.MINUTE_15,
        datetime(2026, 7, 17, 10, 0, tzinfo=BEIJING_TZ),
    )
    assert not is_bar_closed(
        hourly_bar,
        Interval.HOUR_1,
        datetime(2026, 7, 17, 9, 59, tzinfo=BEIJING_TZ),
    )
    assert is_bar_closed(
        hourly_bar,
        Interval.HOUR_1,
        datetime(2026, 7, 17, 10, 0, tzinfo=BEIJING_TZ),
    )


def test_weekly_bar_closes_after_friday_market_close() -> None:
    monday = datetime(2026, 7, 13, tzinfo=BEIJING_TZ)

    assert not is_bar_closed(
        monday,
        Interval.WEEK_1,
        datetime(2026, 7, 17, 15, 4, tzinfo=BEIJING_TZ),
    )
    assert is_bar_closed(
        monday,
        Interval.WEEK_1,
        datetime(2026, 7, 17, 15, 5, tzinfo=BEIJING_TZ),
    )
