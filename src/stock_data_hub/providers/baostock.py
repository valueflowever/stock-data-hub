"""BaoStock adapter for free Chinese daily, weekly, and monthly bars."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import redirect_stdout, suppress
from datetime import datetime
from io import StringIO
from threading import RLock
from typing import Any

import pandas as pd

from stock_data_hub.errors import ProviderUnavailable
from stock_data_hub.models import (
    Adjust,
    AShareBar,
    AssetType,
    Capability,
    Interval,
    Market,
    SourcePayload,
)
from stock_data_hub.providers._parsing import bar_payload_from_dataframe
from stock_data_hub.providers.base import MarketDataProvider


class BaoStockProvider(MarketDataProvider):
    """Use BaoStock's login/session API as a historical-bar fallback."""

    name = "baostock"
    markets = frozenset({Market.CN})
    asset_types = frozenset({AssetType.STOCK, AssetType.ETF})
    intervals = frozenset({Interval.DAY_1, Interval.WEEK_1, Interval.MONTH_1})
    adjustments = frozenset(Adjust)
    capabilities = frozenset({Capability.BARS})

    def __init__(self, *, client_loader: Callable[[], Any] | None = None) -> None:
        self._client_loader = client_loader
        self._lock = RLock()

    def fetch_bars(
        self,
        symbol: str,
        interval: Interval,
        start: datetime,
        end: datetime,
        adjust: Adjust,
    ) -> SourcePayload[list[AShareBar]]:
        if interval not in _BAOSTOCK_FREQUENCIES:
            raise ProviderUnavailable(f"BaoStock does not serve {interval.value} bars")
        client = self._client()
        # The official SDK prints login/logout messages directly. The data
        # layer should leave logging policy to its host application.
        with self._lock, redirect_stdout(StringIO()):
            login = client.login()
            if str(getattr(login, "error_code", "-1")) != "0":
                raise ProviderUnavailable(
                    f"BaoStock login failed: {getattr(login, 'error_msg', 'unknown error')}"
                )
            try:
                result = client.query_history_k_data_plus(
                    _baostock_symbol(symbol),
                    "date,code,open,high,low,close,volume,amount",
                    start_date=start.strftime("%Y-%m-%d"),
                    end_date=end.strftime("%Y-%m-%d"),
                    frequency=_BAOSTOCK_FREQUENCIES[interval],
                    adjustflag=_BAOSTOCK_ADJUST[adjust],
                )
                if str(getattr(result, "error_code", "-1")) != "0":
                    raise ProviderUnavailable(
                        f"BaoStock query failed: {getattr(result, 'error_msg', 'unknown error')}"
                    )
                rows: list[list[str]] = []
                while result.next():
                    rows.append(result.get_row_data())
                dataframe = pd.DataFrame(rows, columns=result.fields)
            finally:
                with suppress(Exception):
                    client.logout()
        return bar_payload_from_dataframe(
            dataframe,
            source="baostock",
            symbol=symbol,
            interval=interval,
            start=start,
            end=end,
            market=Market.CN,
            time_columns=("date",),
            adjust=adjust,
        )

    def _client(self) -> Any:
        if self._client_loader is not None:
            return self._client_loader()
        try:
            import baostock
        except ModuleNotFoundError as exc:
            raise ProviderUnavailable(
                "BaoStock is not installed. Install stock-data-hub[sources]."
            ) from exc
        return baostock


def _baostock_symbol(symbol: str) -> str:
    code, exchange = symbol.split(".", maxsplit=1)
    return f"{exchange.lower()}.{code}"


_BAOSTOCK_FREQUENCIES = {
    Interval.DAY_1: "d",
    Interval.WEEK_1: "w",
    Interval.MONTH_1: "m",
}

_BAOSTOCK_ADJUST = {
    Adjust.HFQ: "1",
    Adjust.QFQ: "2",
    Adjust.NONE: "3",
}
