"""Credential-free Yahoo Finance fallback for global bars and delayed quotes."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from math import isfinite
from typing import Any

import pandas as pd

from stock_data_hub._workers import remaining_timeout
from stock_data_hub.errors import DataValidationError, ProviderUnavailable, UnsupportedCapability
from stock_data_hub.models import (
    Adjust,
    AdjustMethod,
    AShareBar,
    AssetType,
    Capability,
    DailyClose,
    Interval,
    Market,
    Quote,
    QuoteFreshness,
    SourcePayload,
)
from stock_data_hub.normalization import BEIJING_TZ, asset_type_from_symbol, market_from_symbol
from stock_data_hub.providers._parsing import (
    IntradayTimestampLabel,
    bar_payload_from_dataframe,
    decimal_or_none,
    record_value,
    required_positive_decimal,
    to_datetime,
)
from stock_data_hub.providers.base import MarketDataProvider
from stock_data_hub.trading_sessions import market_timezone

_YFINANCE_INTRADAY_TIMESTAMP_LABEL = IntradayTimestampLabel.OPEN


class YFinanceProvider(MarketDataProvider):
    """Use Yahoo as the final global-market fallback without credentials."""

    name = "yfinance"
    capabilities = frozenset({Capability.BARS, Capability.QUOTE, Capability.US_DAILY_CLOSES})
    markets = frozenset({Market.CN, Market.HK, Market.US})
    asset_types = frozenset({AssetType.STOCK, AssetType.ETF, AssetType.INDEX})
    intervals = frozenset(Interval)
    adjustments = frozenset({Adjust.QFQ, Adjust.NONE})
    accepts_asset_type = True
    accepts_us_close_adjust = True

    def __init__(
        self, *, ticker_factory: Callable[[str], Any] | None = None, timeout_seconds: float = 15.0
    ) -> None:
        self._ticker_factory = ticker_factory
        if not isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("Yahoo timeout must be positive")
        self._timeout_seconds = timeout_seconds

    def fetch_bars(
        self,
        symbol: str,
        interval: Interval,
        start: datetime,
        end: datetime,
        adjust: Adjust,
        *,
        asset_type: AssetType | None = None,
    ) -> SourcePayload[list[AShareBar]]:
        if adjust == Adjust.HFQ:
            raise UnsupportedCapability("YFinance does not provide a trustworthy HFQ series")
        yahoo_symbol = _yahoo_symbol(symbol)
        ticker = self._ticker(yahoo_symbol)
        is_intraday = interval in {Interval.MINUTE_15, Interval.HOUR_1}
        query_start = _yahoo_query_start(start, interval)
        query_end = (end.date() + timedelta(days=1)).isoformat()
        try:
            dataframe = ticker.history(
                start=query_start,
                end=query_end,
                interval=_YFINANCE_INTERVALS[interval],
                auto_adjust=adjust == Adjust.QFQ,
                actions=False,
                repair=True,
                raise_errors=True,
                timeout=remaining_timeout(self._timeout_seconds),
            )
        except Exception as exc:
            if type(exc).__name__ == "YFPricesMissingError":
                dataframe = pd.DataFrame()
            else:
                raise ProviderUnavailable(f"YFinance could not obtain bars for {symbol}") from exc
        if not isinstance(dataframe, pd.DataFrame):
            raise DataValidationError("YFinance bars response was not a dataframe")
        frame = dataframe.reset_index() if not dataframe.empty else pd.DataFrame()
        return bar_payload_from_dataframe(
            frame,
            source="yfinance:yahoo",
            warnings=("Yahoo quotes and the newest bar may be delayed.",),
            adjust_method=AdjustMethod.PROPORTIONAL,
            symbol=symbol,
            interval=interval,
            start=start,
            end=end,
            market=market_from_symbol(symbol),
            time_columns=("Datetime", "Date", "date"),
            open_columns=("Open", "open"),
            high_columns=("High", "high"),
            low_columns=("Low", "low"),
            close_columns=("Close", "close"),
            volume_columns=("Volume", "volume"),
            amount_columns=(),
            intraday_timestamp_label=_YFINANCE_INTRADAY_TIMESTAMP_LABEL if is_intraday else None,
            adjust=adjust,
            asset_type=asset_type_from_symbol(symbol, asset_type),
        )

    def fetch_quote(
        self,
        symbol: str,
        *,
        asset_type: AssetType | None = None,
    ) -> SourcePayload[Quote]:
        yahoo_symbol = _yahoo_symbol(symbol)
        try:
            info = self._ticker(yahoo_symbol).fast_info
            price = _mapping_value(info, "last_price", "lastPrice")
        except Exception as exc:
            raise ProviderUnavailable(f"YFinance could not obtain a quote for {symbol}") from exc
        return SourcePayload(
            value=Quote(
                symbol=symbol,
                price=required_positive_decimal(price, "latest price"),
                ts=datetime.now(tz=BEIJING_TZ),
                market=market_from_symbol(symbol),
                freshness=QuoteFreshness.DELAYED,
            ),
            source="yfinance:yahoo",
            warnings=("Yahoo quote timestamps are not guaranteed to be realtime.",),
        )

    def fetch_us_daily_closes(
        self,
        ticker: str,
        max_rows: int,
        *,
        adjust: Adjust = Adjust.NONE,
    ) -> SourcePayload[list[DailyClose]]:
        if adjust == Adjust.HFQ:
            raise UnsupportedCapability("YFinance does not provide HFQ US daily closes")
        yahoo_symbol = _yahoo_symbol(f"{ticker}.US")
        try:
            dataframe = self._ticker(yahoo_symbol).history(
                period=_history_period(max_rows),
                interval="1d",
                auto_adjust=adjust == Adjust.QFQ,
                actions=False,
                repair=True,
                raise_errors=True,
                timeout=remaining_timeout(self._timeout_seconds),
            )
        except Exception as exc:
            raise ProviderUnavailable(
                f"YFinance could not obtain daily closes for {ticker}"
            ) from exc
        if not isinstance(dataframe, pd.DataFrame):
            raise DataValidationError("YFinance daily-close response was not a dataframe")
        us_tz = market_timezone(Market.US)
        closes: list[DailyClose] = []
        for row in dataframe.reset_index().tail(max_rows).to_dict("records"):
            timestamp = to_datetime(
                record_value(row, "Date", "Datetime", "date"), naive_timezone=us_tz
            )
            close = decimal_or_none(record_value(row, "Close", "close"))
            if timestamp is None or close is None or close <= 0:
                continue
            closes.append(
                DailyClose(symbol=ticker, day=timestamp.astimezone(us_tz).date(), close=close)
            )
        if not closes:
            raise ProviderUnavailable(f"YFinance returned no usable daily closes for {ticker}")
        return SourcePayload(
            value=sorted(closes, key=lambda item: item.day),
            source="yfinance:yahoo",
            warnings=("Yahoo end-of-day data may be delayed or subsequently adjusted.",),
        )

    def _ticker(self, symbol: str) -> Any:
        if self._ticker_factory is not None:
            return self._ticker_factory(symbol)
        try:
            import yfinance
        except ModuleNotFoundError as exc:
            raise ProviderUnavailable(
                "YFinance is not installed. Install stock-data-hub[sources]."
            ) from exc
        return yfinance.Ticker(symbol)


def _yahoo_query_start(start: datetime, interval: Interval) -> str:
    day = start.date()
    if interval == Interval.WEEK_1:
        day = day - timedelta(days=day.weekday())
    elif interval == Interval.MONTH_1:
        day = day.replace(day=1)
    elif interval in {Interval.MINUTE_15, Interval.HOUR_1}:
        # Account for timezone offset when querying intraday bars near day boundaries.
        day = day - timedelta(days=1)
    return day.isoformat()


def _yahoo_symbol(symbol: str) -> str:
    code, exchange = symbol.rsplit(".", maxsplit=1)
    if exchange == "US":
        return code.replace(".", "-")
    if exchange == "HK":
        return f"{code.lstrip('0').zfill(4)}.HK"
    if exchange == "SH":
        return f"{code}.SS"
    if exchange == "SZ":
        return f"{code}.SZ"
    raise UnsupportedCapability(f"YFinance has no symbol mapping for {symbol}")


def _mapping_value(value: Any, *names: str) -> object | None:
    for name in names:
        try:
            if isinstance(value, Mapping) and name in value:
                return value[name]
            item = value[name]
            if item is not None:
                return item
        except (KeyError, TypeError):
            continue
    return None


def _history_period(max_rows: int) -> str:
    # Include generous holiday safety margins (e.g. Feb or Nov/Dec have ~19 US trading days).
    if max_rows <= 3:
        return "5d"
    if max_rows <= 15:
        return "1mo"
    if max_rows <= 50:
        return "3mo"
    if max_rows <= 105:
        return "6mo"
    if max_rows <= 220:
        return "1y"
    if max_rows <= 1100:
        return "5y"
    return "max"


_YFINANCE_INTERVALS = {
    Interval.MINUTE_15: "15m",
    Interval.HOUR_1: "1h",
    Interval.DAY_1: "1d",
    Interval.WEEK_1: "1wk",
    Interval.MONTH_1: "1mo",
}
