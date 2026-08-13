"""Anonymous TickFlow adapter for global daily-or-longer bars."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import redirect_stdout
from datetime import datetime
from io import StringIO
from typing import Any

import pandas as pd

from stock_data_hub.errors import DataValidationError, ProviderUnavailable
from stock_data_hub.models import (
    Adjust,
    AShareBar,
    AssetType,
    Capability,
    Interval,
    Market,
    SourcePayload,
)
from stock_data_hub.normalization import market_from_symbol
from stock_data_hub.providers._parsing import bars_from_dataframe
from stock_data_hub.providers.base import MarketDataProvider


class TickFlowProvider(MarketDataProvider):
    """Use only TickFlow's credential-free historical data service."""

    name = "tickflow"
    markets = frozenset({Market.CN, Market.HK, Market.US})
    asset_types = frozenset({AssetType.STOCK, AssetType.ETF})
    intervals = frozenset({Interval.DAY_1, Interval.WEEK_1, Interval.MONTH_1})
    adjustments = frozenset({Adjust.QFQ, Adjust.HFQ, Adjust.NONE})
    capabilities = frozenset({Capability.BARS})

    def __init__(
        self,
        *,
        client_factory: Callable[[], Any] | None = None,
    ) -> None:
        self._client_factory = client_factory

    def fetch_bars(
        self,
        symbol: str,
        interval: Interval,
        start: datetime,
        end: datetime,
        adjust: Adjust,
    ) -> SourcePayload[list[AShareBar]]:
        client = self._client()
        kwargs: dict[str, Any] = {
            "period": _TICKFLOW_PERIODS[interval],
            "count": 10_000,
            "adjust": _TICKFLOW_ADJUST[adjust],
            "as_dataframe": True,
        }
        try:
            dataframe = client.klines.get(symbol, **kwargs)
        except Exception as exc:
            raise ProviderUnavailable(f"TickFlow could not obtain bars for {symbol}") from exc
        if not isinstance(dataframe, pd.DataFrame):
            raise DataValidationError("TickFlow bars response was not a dataframe")
        market = market_from_symbol(symbol)
        bars = bars_from_dataframe(
            dataframe,
            symbol=symbol,
            interval=interval,
            start=start,
            end=end,
            market=market,
            time_columns=("trade_date", "timestamp"),
        )
        return SourcePayload(
            value=bars,
            source="tickflow:free",
            warnings=("TickFlow free service returns historical bars only.",),
        )

    def _client(self) -> Any:
        if self._client_factory is not None:
            return self._client_factory()
        try:
            from tickflow import TickFlow
        except ModuleNotFoundError as exc:
            raise ProviderUnavailable(
                "TickFlow is not installed. Install stock-data-hub[sources]."
            ) from exc
        # TickFlow's free constructor prints an emoji notice.  Redirecting it
        # avoids a UnicodeEncodeError in Windows consoles using GBK.
        with redirect_stdout(StringIO()):
            return TickFlow.free()


_TICKFLOW_PERIODS = {
    Interval.DAY_1: "1d",
    Interval.WEEK_1: "1w",
    Interval.MONTH_1: "1M",
}

_TICKFLOW_ADJUST = {
    Adjust.QFQ: "forward",
    Adjust.HFQ: "backward",
    Adjust.NONE: "none",
}
