from __future__ import annotations

from datetime import datetime

import pandas as pd

import stock_data_hub.providers.akshare as adapter
from stock_data_hub.models import Adjust, AssetType, Interval, Market
from stock_data_hub.normalization import BEIJING_TZ
from stock_data_hub.providers._parsing import decimal_or_none


def test_snapshot_can_prefer_sina_as_an_independent_fallback(monkeypatch) -> None:
    class AkShare:
        def stock_zh_a_spot(self) -> pd.DataFrame:
            return pd.DataFrame(
                {
                    "代码": ["sh600519"],
                    "名称": ["贵州茅台"],
                    "最新价": [1400],
                    "成交额": [1_000_000_000],
                    "涨跌幅": [1.2],
                }
            )

        def stock_zh_a_spot_em(self) -> pd.DataFrame:
            raise AssertionError("Eastmoney should not run after Sina succeeds")

    monkeypatch.setattr(adapter, "_load_akshare", lambda: AkShare())

    result = adapter.AkShareProvider(prefer_sina_snapshot=True).fetch_snapshot()

    assert result.source == "akshare:sina"
    assert result.value.rows[0].symbol == "600519.SH"
    assert result.value.has_market_cap is False


def test_empty_eastmoney_bars_are_a_valid_empty_result(monkeypatch) -> None:
    class EmptyBarsAkShare:
        def stock_zh_a_hist(self, **_: object) -> pd.DataFrame:
            return pd.DataFrame()

        def stock_zh_a_daily(self, **_: object) -> pd.DataFrame:
            raise AssertionError("Sina fallback should not run for a valid empty result")

    monkeypatch.setattr(adapter, "_load_akshare", lambda: EmptyBarsAkShare())

    result = adapter.AkShareProvider().fetch_bars(
        "600519",
        Interval.DAY_1,
        datetime(2026, 7, 18, tzinfo=BEIJING_TZ),
        datetime(2026, 7, 19, tzinfo=BEIJING_TZ),
        Adjust.QFQ,
    )

    assert result.value == []
    assert result.source == "akshare:eastmoney"


def test_empty_restricted_release_schedule_does_not_become_an_outage(monkeypatch) -> None:
    class EmptyReleaseAkShare:
        def stock_restricted_release_queue_em(self, **_: object) -> pd.DataFrame:
            return pd.DataFrame()

        def stock_restricted_release_queue_sina(self, **_: object) -> pd.DataFrame:
            raise AssertionError("Sina fallback should not run for a valid empty result")

    monkeypatch.setattr(adapter, "_load_akshare", lambda: EmptyReleaseAkShare())

    result = adapter.AkShareProvider().fetch_restricted_releases("600519")

    assert result.value == []
    assert result.source == "akshare:eastmoney"


def test_non_finite_decimal_is_not_accepted() -> None:
    assert decimal_or_none("NaN") is None
    assert decimal_or_none("Infinity") is None


def test_etf_bars_use_the_fund_endpoint_instead_of_the_stock_endpoint(monkeypatch) -> None:
    class AkShare:
        def __init__(self) -> None:
            self.etf_kwargs: dict[str, object] = {}

        def fund_etf_hist_em(self, **kwargs: object) -> pd.DataFrame:
            self.etf_kwargs = kwargs
            return pd.DataFrame(
                {
                    "日期": ["2026-07-17"],
                    "开盘": [4.0],
                    "最高": [4.1],
                    "最低": [3.9],
                    "收盘": [4.05],
                    "成交量": [100],
                    "成交额": [405],
                }
            )

        def stock_zh_a_hist(self, **_: object) -> pd.DataFrame:
            raise AssertionError("ETF must not be routed to stock_zh_a_hist")

    client = AkShare()
    monkeypatch.setattr(adapter, "_load_akshare", lambda: client)

    result = adapter.AkShareProvider().fetch_bars(
        "510300.SH",
        Interval.DAY_1,
        datetime(2026, 7, 1, tzinfo=BEIJING_TZ),
        datetime(2026, 7, 31, tzinfo=BEIJING_TZ),
        Adjust.QFQ,
        asset_type=AssetType.ETF,
    )

    assert result.value[0].symbol == "510300.SH"
    assert result.value[0].market == Market.CN
    assert client.etf_kwargs["symbol"] == "510300"


def test_chinese_index_bars_use_the_index_endpoint(monkeypatch) -> None:
    class AkShare:
        def index_zh_a_hist(self, **kwargs: object) -> pd.DataFrame:
            self.index_kwargs = kwargs
            return pd.DataFrame(
                {
                    "日期": ["2026-07-17"],
                    "开盘": [3500],
                    "最高": [3550],
                    "最低": [3490],
                    "收盘": [3520],
                    "成交量": [100],
                    "成交额": [352000],
                }
            )

        def stock_zh_a_hist(self, **_: object) -> pd.DataFrame:
            raise AssertionError("Index must not be routed to stock_zh_a_hist")

    client = AkShare()
    monkeypatch.setattr(adapter, "_load_akshare", lambda: client)

    result = adapter.AkShareProvider().fetch_bars(
        "000001.SH",
        Interval.WEEK_1,
        datetime(2026, 7, 1, tzinfo=BEIJING_TZ),
        datetime(2026, 7, 31, tzinfo=BEIJING_TZ),
        Adjust.NONE,
    )

    assert result.value[0].market == Market.CN
    assert client.index_kwargs["period"] == "weekly"
