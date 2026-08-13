"""Credential-free Yahoo Finance fallback for global bars and delayed quotes."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from stock_data_hub.errors import DataValidationError, ProviderUnavailable
from stock_data_hub.models import (
    Adjust,
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
from stock_data_hub.normalization import BEIJING_TZ, market_from_symbol
from stock_data_hub.providers._parsing import (
    bars_from_dataframe,
    decimal_or_none,
    record_value,
    required_positive_decimal,
    to_datetime,
)
from stock_data_hub.providers.base import MarketDataProvider


class YFinanceProvider(MarketDataProvider):
    """Use Yahoo as the final global-market fallback without credentials."""

    name = "yfinance"
    capabilities = frozenset({Capability.BARS, Capability.QUOTE, Capability.US_DAILY_CLOSES})
    markets = frozenset({Market.CN, Market.HK, Market.US})
    asset_types = frozenset({AssetType.STOCK, AssetType.ETF, AssetType.INDEX})
    intervals = frozenset({Interval.DAY_1, Interval.WEEK_1, Interval.MONTH_1})
    adjustments = frozenset({Adjust.QFQ, Adjust.NONE})
    accepts_asset_type = True

    def __init__(self, *, ticker_factory: Callable[[str], Any] | None = None) -> None:
        self._ticker_factory = ticker_factory

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
            raise ProviderUnavailable("YFinance does not provide a trustworthy HFQ series")
        ticker = self._ticker(_yahoo_symbol(symbol))
        try:
            dataframe = ticker.history(
                start=start.date().isoformat(),
                end=(end.date() + timedelta(days=1)).isoformat(),
                interval=_YFINANCE_INTERVALS[interval],
                auto_adjust=adjust == Adjust.QFQ,
                actions=False,
                repair=True,
                raise_errors=True,
            )
        except Exception as exc:
            raise ProviderUnavailable(f"YFinance could not obtain bars for {symbol}") from exc
        if not isinstance(dataframe, pd.DataFrame):
            raise DataValidationError("YFinance bars response was not a dataframe")
        frame = dataframe.reset_index()
        bars = bars_from_dataframe(
            frame,
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
        )
        return SourcePayload(
            value=bars,
            source="yfinance:yahoo",
            warnings=("Yahoo quotes and the newest bar may be delayed.",),
        )

    def fetch_quote(
        self,
        symbol: str,
        *,
        asset_type: AssetType | None = None,
    ) -> SourcePayload[Quote]:
        try:
            info = self._ticker(_yahoo_symbol(symbol)).fast_info
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
    ) -> SourcePayload[list[DailyClose]]:
        try:
            dataframe = self._ticker(ticker).history(
                period=_history_period(max_rows),
                interval="1d",
                auto_adjust=False,
                actions=False,
                repair=True,
                raise_errors=True,
            )
        except Exception as exc:
            raise ProviderUnavailable(
                f"YFinance could not obtain daily closes for {ticker}"
            ) from exc
        if not isinstance(dataframe, pd.DataFrame):
            raise DataValidationError("YFinance daily-close response was not a dataframe")
        closes: list[DailyClose] = []
        for _, row in dataframe.reset_index().tail(max_rows).iterrows():
            timestamp = to_datetime(record_value(row, "Date", "Datetime", "date"))
            close = decimal_or_none(record_value(row, "Close", "close"))
            if timestamp is None or close is None or close <= 0:
                continue
            closes.append(DailyClose(symbol=ticker, day=timestamp.date(), close=close))
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


def _yahoo_symbol(symbol: str) -> str:
    code, exchange = symbol.rsplit(".", maxsplit=1)
    if exchange == "US":
        return code
    if exchange == "HK":
        return f"{code.lstrip('0').zfill(4)}.HK"
    if exchange == "SH":
        return f"{code}.SS"
    if exchange == "SZ":
        return f"{code}.SZ"
    raise ProviderUnavailable(f"YFinance has no symbol mapping for {symbol}")


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
    if max_rows <= 5:
        return "5d"
    if max_rows <= 22:
        return "1mo"
    if max_rows <= 66:
        return "3mo"
    if max_rows <= 132:
        return "6mo"
    if max_rows <= 264:
        return "1y"
    if max_rows <= 1320:
        return "5y"
    return "max"


_YFINANCE_INTERVALS = {
    Interval.DAY_1: "1d",
    Interval.WEEK_1: "1wk",
    Interval.MONTH_1: "1mo",
}
