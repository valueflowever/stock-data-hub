"""Optional PyTDX adapter for a configured Chinese quote server."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import suppress
from datetime import date, datetime
from decimal import Decimal
from math import isfinite
from typing import Any

import pandas as pd

from stock_data_hub._workers import remaining_timeout
from stock_data_hub.errors import DataValidationError, ProviderUnavailable, UnsupportedCapability
from stock_data_hub.models import (
    Adjust,
    AShareBar,
    AssetType,
    Capability,
    Interval,
    Market,
    SourcePayload,
)
from stock_data_hub.normalization import BEIJING_TZ, asset_type_from_symbol, to_beijing
from stock_data_hub.providers._parsing import (
    IntradayTimestampLabel,
    bar_payload_from_dataframe,
    to_datetime,
)
from stock_data_hub.providers.base import MarketDataProvider

_PYTDX_CATEGORIES = {
    Interval.MINUTE_15: 1,
    Interval.HOUR_1: 3,
    Interval.DAY_1: 9,
    Interval.WEEK_1: 5,
    Interval.MONTH_1: 6,
}
_PYTDX_INTRADAY_TIMESTAMP_LABEL = IntradayTimestampLabel.CLOSE


class PyTDXProvider(MarketDataProvider):
    """Fetch unadjusted A-share/ETF/index bars from explicitly configured TDX server(s)."""

    name = "pytdx"
    capabilities = frozenset({Capability.BARS})
    markets = frozenset({Market.CN})
    asset_types = frozenset({AssetType.STOCK, AssetType.ETF, AssetType.INDEX})
    intervals = frozenset(Interval)
    adjustments = frozenset({Adjust.NONE})
    accepts_asset_type = True

    def __init__(
        self,
        host: str,
        port: int = 7709,
        *,
        fallback_servers: Sequence[tuple[str, int]] = (),
        client_factory: Callable[[], Any] | None = None,
        page_size: int = 800,
        max_rows: int = 12_000,
        timeout_seconds: float = 10.0,
    ) -> None:
        servers = _parse_servers(host, port, fallback_servers)
        if page_size < 1:
            raise ValueError("PyTDX page size must be positive")
        if max_rows < page_size:
            raise ValueError("PyTDX max rows must be at least one page")
        if not isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("PyTDX timeout must be positive")
        self._servers = servers
        self.host, self.port = servers[0]
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
        *,
        asset_type: AssetType | None = None,
    ) -> SourcePayload[list[AShareBar]]:
        if adjust != Adjust.NONE or interval not in _PYTDX_CATEGORIES:
            raise UnsupportedCapability(
                f"PyTDX only provides unadjusted bars for supported intervals, not {interval.value}/{adjust.value}"
            )
        code, exchange = symbol.split(".", maxsplit=1)
        if exchange not in {"SH", "SZ"}:
            raise UnsupportedCapability(f"PyTDX does not support exchange {exchange}")

        resolved_asset = asset_type_from_symbol(symbol, asset_type)
        category = _PYTDX_CATEGORIES[interval]
        market = 1 if exchange == "SH" else 0
        is_intraday = interval in {Interval.MINUTE_15, Interval.HOUR_1}

        last_error: Exception | None = None
        for server_host, server_port in self._servers:
            client = self._client()
            rows: list[object] = []
            try:
                connected = client.connect(
                    server_host, server_port, time_out=remaining_timeout(self._timeout_seconds)
                )
                if connected is False:
                    raise ProviderUnavailable(
                        f"PyTDX could not connect to {server_host}:{server_port}"
                    )
                getter = (
                    getattr(client, "get_index_bars", None)
                    if resolved_asset == AssetType.INDEX
                    else None
                ) or client.get_security_bars
                for offset in range(0, self._max_rows, self._page_size):
                    page_timeout = remaining_timeout(self._timeout_seconds)
                    _apply_socket_timeout(client, page_timeout)
                    page = getter(category, market, code, offset, self._page_size)
                    if not page:
                        break
                    rows.extend(page)
                    frame = _to_frame(client, page)
                    if is_intraday:
                        earliest_ts = _earliest_bar_timestamp(frame)
                        if earliest_ts is not None and earliest_ts <= to_beijing(start):
                            break
                    else:
                        earliest = _earliest_bar_date(frame)
                        if earliest is not None and earliest <= start.date():
                            break
                    if len(page) < self._page_size:
                        break
            except ProviderUnavailable as exc:
                last_error = exc
                continue
            except Exception as exc:  # noqa: BLE001 - isolate third-party SDK failures per server
                last_error = ProviderUnavailable(
                    f"PyTDX could not obtain bars for {symbol} from {server_host}:{server_port}"
                )
                last_error.__cause__ = exc
                continue
            finally:
                with suppress(Exception):
                    client.disconnect()

            dataframe = _to_frame(client, rows)
            if not isinstance(dataframe, pd.DataFrame):
                raise DataValidationError(
                    "PyTDX bars response could not be converted to a dataframe"
                )
            if is_intraday:
                earliest_all_ts = _earliest_bar_timestamp(dataframe)
                is_truncated = (
                    len(rows) >= self._max_rows
                    and earliest_all_ts is not None
                    and earliest_all_ts > to_beijing(start)
                )
            else:
                earliest_all = _earliest_bar_date(dataframe)
                is_truncated = (
                    len(rows) >= self._max_rows
                    and earliest_all is not None
                    and earliest_all > start.date()
                )
            return bar_payload_from_dataframe(
                dataframe,
                source=f"pytdx:{server_host}:{server_port}",
                symbol=symbol,
                interval=interval,
                start=start,
                end=end,
                market=Market.CN,
                time_columns=("datetime", "date"),
                volume_columns=("vol", "volume"),
                volume_multiplier=Decimal(100),
                intraday_timestamp_label=_PYTDX_INTRADAY_TIMESTAMP_LABEL if is_intraday else None,
                adjust=adjust,
                asset_type=resolved_asset,
                truncated=is_truncated,
            )
        assert last_error is not None
        raise last_error

    def _client(self) -> Any:
        if self._client_factory is not None:
            return self._client_factory()
        try:
            from pytdx.hq import TdxHq_API
        except ModuleNotFoundError as exc:
            raise ProviderUnavailable(
                "PyTDX is not installed. Install stock-data-hub[sources]."
            ) from exc
        return TdxHq_API(raise_exception=True, auto_retry=False)


def _parse_servers(
    host: str,
    default_port: int,
    fallback_servers: Sequence[tuple[str, int]],
) -> tuple[tuple[str, int], ...]:
    if default_port < 1 or default_port > 65535:
        raise ValueError("PyTDX port must be between 1 and 65535")
    parsed: list[tuple[str, int]] = []
    for raw_entry in str(host).split(","):
        entry = raw_entry.strip()
        if not entry:
            continue
        if ":" in entry:
            entry_host, raw_port = entry.rsplit(":", maxsplit=1)
            entry_host = entry_host.strip()
            try:
                entry_port = int(raw_port.strip())
            except ValueError as exc:
                raise ValueError(f"Invalid PyTDX port in {entry!r}") from exc
        else:
            entry_host, entry_port = entry, default_port
        if not entry_host:
            raise ValueError("PyTDX host cannot be empty")
        if entry_port < 1 or entry_port > 65535:
            raise ValueError("PyTDX port must be between 1 and 65535")
        if (entry_host, entry_port) not in parsed:
            parsed.append((entry_host, entry_port))
    for server_host, server_port in fallback_servers:
        cleaned = str(server_host).strip()
        if not cleaned:
            raise ValueError("PyTDX host cannot be empty")
        if server_port < 1 or server_port > 65535:
            raise ValueError("PyTDX port must be between 1 and 65535")
        if (cleaned, server_port) not in parsed:
            parsed.append((cleaned, server_port))
    if not parsed:
        raise ValueError("PyTDX host cannot be empty")
    return tuple(parsed)


def _apply_socket_timeout(client: Any, timeout: float) -> None:
    sock = getattr(client, "client", None)
    setter = getattr(sock, "settimeout", None)
    if callable(setter):
        with suppress(Exception):
            setter(timeout)


def _earliest_bar_timestamp(dataframe: pd.DataFrame | None) -> datetime | None:
    if dataframe is None or dataframe.empty:
        return None
    for column in ("datetime", "date"):
        if column in dataframe.columns:
            stamps = [
                parsed
                for val in dataframe[column].dropna().tolist()
                if (parsed := to_datetime(val, naive_timezone=BEIJING_TZ)) is not None
            ]
            if stamps:
                return min(stamps)
    return None


def _earliest_bar_date(dataframe: pd.DataFrame | None) -> date | None:
    if dataframe is None or dataframe.empty:
        return None
    for column in ("datetime", "date"):
        if column in dataframe.columns:
            parsed = pd.to_datetime(dataframe[column], errors="coerce").dropna()
            if not parsed.empty:
                return parsed.min().date()
    return None


def _to_frame(client: Any, rows: object) -> pd.DataFrame:
    if isinstance(rows, pd.DataFrame):
        return rows
    converter = getattr(client, "to_df", None)
    if callable(converter):
        converted = converter(rows)
        if isinstance(converted, pd.DataFrame):
            return converted
    return pd.DataFrame(rows)
