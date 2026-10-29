"""Anonymous TickFlow adapter for global daily-or-longer bars."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import redirect_stdout, suppress
from datetime import datetime, timedelta
from decimal import Decimal
from io import StringIO
from math import isfinite
from typing import Any

import pandas as pd

from stock_data_hub._workers import remaining_timeout
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
from stock_data_hub.providers._parsing import bar_payload_from_dataframe, record_value, to_datetime
from stock_data_hub.providers.base import MarketDataProvider
from stock_data_hub.trading_sessions import market_timezone


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
        page_size: int = 10_000,
        max_rows: int = 100_000,
        timeout_seconds: float = 15.0,
    ) -> None:
        if (
            not 1 <= page_size <= 10_000
            or max_rows < page_size
            or not isfinite(timeout_seconds)
            or timeout_seconds <= 0
        ):
            raise ValueError("Invalid TickFlow history or timeout limits")
        self._client_factory = client_factory
        self._page_size = page_size
        self._max_rows = max_rows
        self._timeout_seconds = timeout_seconds

    def fetch_bars(
        self,
        symbol: str,
        interval: Interval,
        start: datetime,
        end: datetime,
        adjust: Adjust,
    ) -> SourcePayload[list[AShareBar]]:
        client = self._client()
        market = market_from_symbol(symbol)
        zone = market_timezone(market)
        query_start = datetime.combine(start.date(), datetime.min.time(), tzinfo=zone)
        query_end = datetime.combine(
            end.date() + timedelta(days=1), datetime.min.time(), tzinfo=zone
        )
        cursor = int(query_end.timestamp() * 1000) - 1
        kwargs: dict[str, Any] = {
            "period": _TICKFLOW_PERIODS[interval],
            "start_time": int(query_start.timestamp() * 1000),
            "adjust": _TICKFLOW_ADJUST[adjust],
            "as_dataframe": True,
        }
        frames: list[pd.DataFrame] = []
        loaded = 0
        truncated = False
        try:
            while loaded < self._max_rows:
                remaining_timeout(self._timeout_seconds)
                count = min(self._page_size, self._max_rows - loaded)
                dataframe = client.klines.get(symbol, count=count, end_time=cursor, **kwargs)
                if not isinstance(dataframe, pd.DataFrame):
                    raise DataValidationError("TickFlow bars response was not a dataframe")
                if dataframe.empty:
                    break
                frames.append(dataframe)
                loaded += len(dataframe)
                stamps = [
                    stamp
                    for _, row in dataframe.iterrows()
                    if (
                        stamp := to_datetime(
                            record_value(row, "timestamp", "trade_date"), naive_timezone=zone
                        )
                    )
                    is not None
                ]
                if not stamps:
                    raise DataValidationError("TickFlow history page had no usable timestamps")
                earliest = min(stamps)
                if len(dataframe) < count or earliest <= query_start:
                    break
                next_cursor = int(earliest.timestamp() * 1000) - 1
                if next_cursor >= cursor or loaded >= self._max_rows:
                    truncated = True
                    break
                cursor = next_cursor
        except DataValidationError:
            raise
        except Exception as exc:
            raise ProviderUnavailable(f"TickFlow could not obtain bars for {symbol}") from exc
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                with suppress(Exception):
                    close()
        return bar_payload_from_dataframe(
            pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(),
            source="tickflow:free",
            warnings=("TickFlow free service returns historical bars only.",),
            truncated=truncated,
            symbol=symbol,
            interval=interval,
            start=start,
            end=end,
            market=market,
            time_columns=("trade_date", "timestamp"),
            adjust=adjust,
            # TickFlow free CN history retains Eastmoney lots; HK/US use shares.
            volume_multiplier=Decimal(100) if market == Market.CN else Decimal(1),
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
            return TickFlow.free(timeout=self._timeout_seconds, max_retries=0)


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
