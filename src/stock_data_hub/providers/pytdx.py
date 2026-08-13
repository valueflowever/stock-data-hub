"""Optional PyTDX adapter for a configured Chinese quote server."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import suppress
from datetime import datetime
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
from stock_data_hub.providers._parsing import bars_from_dataframe
from stock_data_hub.providers.base import MarketDataProvider


class PyTDXProvider(MarketDataProvider):
    """Fetch unadjusted A-share daily bars from an explicitly configured server."""

    name = "pytdx"
    capabilities = frozenset({Capability.BARS})
    markets = frozenset({Market.CN})
    asset_types = frozenset({AssetType.STOCK})
    intervals = frozenset({Interval.DAY_1})
    adjustments = frozenset({Adjust.NONE})

    def __init__(
        self,
        host: str,
        port: int = 7709,
        *,
        client_factory: Callable[[], Any] | None = None,
        page_size: int = 800,
        max_rows: int = 12_000,
    ) -> None:
        if not str(host).strip():
            raise ValueError("PyTDX host cannot be empty")
        if port < 1 or port > 65535:
            raise ValueError("PyTDX port must be between 1 and 65535")
        if page_size < 1:
            raise ValueError("PyTDX page size must be positive")
        if max_rows < page_size:
            raise ValueError("PyTDX max rows must be at least one page")
        self.host = str(host).strip()
        self.port = port
        self._client_factory = client_factory
        self._page_size = page_size
        self._max_rows = max_rows

    def fetch_bars(
        self,
        symbol: str,
        interval: Interval,
        start: datetime,
        end: datetime,
        adjust: Adjust,
    ) -> SourcePayload[list[AShareBar]]:
        if interval != Interval.DAY_1 or adjust != Adjust.NONE:
            raise ProviderUnavailable("PyTDX only provides unadjusted daily bars")
        code, exchange = symbol.split(".", maxsplit=1)
        if exchange not in {"SH", "SZ"}:
            raise ProviderUnavailable(f"PyTDX does not support exchange {exchange}")
        client = self._client()
        rows: list[object] = []
        try:
            connected = client.connect(self.host, self.port)
            if connected is False:
                raise ProviderUnavailable(f"PyTDX could not connect to {self.host}:{self.port}")
            market = 1 if exchange == "SH" else 0
            for offset in range(0, self._max_rows, self._page_size):
                page = client.get_security_bars(9, market, code, offset, self._page_size)
                if not page:
                    break
                rows.extend(page)
                frame = _to_frame(client, page)
                parsed = pd.to_datetime(frame.get("datetime"), errors="coerce")
                if not parsed.empty and parsed.min().date() <= start.date():
                    break
                if len(page) < self._page_size:
                    break
        except ProviderUnavailable:
            raise
        except Exception as exc:
            raise ProviderUnavailable(f"PyTDX could not obtain bars for {symbol}") from exc
        finally:
            with suppress(Exception):
                client.disconnect()
        dataframe = _to_frame(client, rows)
        if not isinstance(dataframe, pd.DataFrame):
            raise DataValidationError("PyTDX bars response could not be converted to a dataframe")
        return SourcePayload(
            value=bars_from_dataframe(
                dataframe,
                symbol=symbol,
                interval=interval,
                start=start,
                end=end,
                market=Market.CN,
                time_columns=("datetime", "date"),
                volume_columns=("vol", "volume"),
            ),
            source=f"pytdx:{self.host}:{self.port}",
        )

    def _client(self) -> Any:
        if self._client_factory is not None:
            return self._client_factory()
        try:
            from pytdx.hq import TdxHq_API
        except ModuleNotFoundError as exc:
            raise ProviderUnavailable(
                "PyTDX is not installed. Install stock-data-hub[sources]."
            ) from exc
        return TdxHq_API(raise_exception=True, auto_retry=True)


def _to_frame(client: Any, rows: object) -> pd.DataFrame:
    if isinstance(rows, pd.DataFrame):
        return rows
    converter = getattr(client, "to_df", None)
    if callable(converter):
        converted = converter(rows)
        if isinstance(converted, pd.DataFrame):
            return converted
    return pd.DataFrame(rows)
