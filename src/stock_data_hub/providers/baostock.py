"""BaoStock adapter for free Chinese daily, intraday, weekly, and monthly bars."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import suppress
from datetime import date, datetime
from threading import RLock
from typing import Any

import pandas as pd

from stock_data_hub._workers import remaining_timeout, suppress_thread_stdout
from stock_data_hub.errors import ProviderUnavailable, UnsupportedCapability
from stock_data_hub.models import (
    Adjust,
    AdjustFactor,
    AdjustMethod,
    AShareBar,
    AssetType,
    Capability,
    Interval,
    Market,
    SourcePayload,
)
from stock_data_hub.normalization import asset_type_from_symbol, normalize_a_share_symbol
from stock_data_hub.providers._parsing import (
    IntradayTimestampLabel,
    bar_payload_from_dataframe,
    decimal_or_none,
    record_value,
    to_datetime,
)
from stock_data_hub.providers.base import MarketDataProvider

_BAOSTOCK_INTRADAY_TIMESTAMP_LABEL = IntradayTimestampLabel.CLOSE


class BaoStockProvider(MarketDataProvider):
    """Use BaoStock's login/session API as a historical-bar fallback."""

    name = "baostock"
    markets = frozenset({Market.CN})
    asset_types = frozenset({AssetType.STOCK, AssetType.ETF, AssetType.INDEX})
    intervals = frozenset(Interval)
    adjustments = frozenset(Adjust)
    capabilities = frozenset({Capability.BARS, Capability.ADJUST_FACTORS})
    accepts_asset_type = True

    def __init__(
        self,
        *,
        client_loader: Callable[[], Any] | None = None,
        timeout_seconds: float = 20.0,
    ) -> None:
        self._client_loader = client_loader
        self._timeout_seconds = timeout_seconds
        self._lock = RLock()

    def supports_request(
        self,
        capability: Capability,
        *,
        market: Market | None = None,
        asset_type: AssetType | None = None,
        interval: Interval | None = None,
        adjust: Adjust | None = None,
    ) -> bool:
        if not super().supports_request(
            capability,
            market=market,
            asset_type=asset_type,
            interval=interval,
            adjust=adjust,
        ):
            return False
        if capability == Capability.BARS:
            if asset_type == AssetType.INDEX and adjust not in {None, Adjust.NONE}:
                return False
            if asset_type in {AssetType.ETF, AssetType.INDEX} and interval in {
                Interval.MINUTE_15,
                Interval.HOUR_1,
            }:
                return False
        if capability == Capability.ADJUST_FACTORS:
            return market in {None, Market.CN} and asset_type in {None, AssetType.STOCK}
        return True

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
        if interval not in _BAOSTOCK_FREQUENCIES:
            raise UnsupportedCapability(f"BaoStock does not serve {interval.value} bars")
        resolved_asset = asset_type_from_symbol(symbol, asset_type)
        is_intraday = interval in {Interval.MINUTE_15, Interval.HOUR_1}
        if resolved_asset == AssetType.INDEX and adjust != Adjust.NONE:
            raise UnsupportedCapability("BaoStock serves unadjusted index bars only")
        if is_intraday and resolved_asset != AssetType.STOCK:
            raise UnsupportedCapability(
                f"BaoStock does not serve {interval.value} bars for {resolved_asset.value}"
            )
        client = self._client()
        fields = _fields_for(interval)
        # The official SDK prints login/logout messages directly and shares one
        # global TCP connection. Suppress stdout per-thread and check budgets.
        remaining_timeout(self._timeout_seconds)
        with self._lock, suppress_thread_stdout():
            remaining_timeout(self._timeout_seconds)
            login = client.login()
            if str(getattr(login, "error_code", "-1")) != "0":
                raise ProviderUnavailable(
                    f"BaoStock login failed: {getattr(login, 'error_msg', 'unknown error')}"
                )
            try:
                remaining_timeout(self._timeout_seconds)
                result = client.query_history_k_data_plus(
                    _baostock_symbol(symbol),
                    fields,
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
                    remaining_timeout(self._timeout_seconds)
                    rows.append(result.get_row_data())
                dataframe = pd.DataFrame(rows, columns=result.fields)
            finally:
                with suppress(Exception):
                    client.logout()
        if not dataframe.empty and "tradestatus" in dataframe.columns:
            # Exclude suspended trading days so BaoStock matches other providers.
            dataframe = dataframe[dataframe["tradestatus"].astype(str).str.strip() != "0"]
        if is_intraday and not dataframe.empty:
            dataframe = _normalize_intraday_time(dataframe)
        return bar_payload_from_dataframe(
            dataframe,
            source="baostock",
            adjust_method=AdjustMethod.PROPORTIONAL,
            symbol=symbol,
            interval=interval,
            start=start,
            end=end,
            market=Market.CN,
            time_columns=("datetime", "time", "date") if is_intraday else ("date",),
            intraday_timestamp_label=_BAOSTOCK_INTRADAY_TIMESTAMP_LABEL if is_intraday else None,
            adjust=adjust,
            asset_type=resolved_asset,
        )

    def fetch_adjust_factors(
        self,
        symbol: str,
        start: date | None = None,
        end: date | None = None,
    ) -> SourcePayload[list[AdjustFactor]]:
        normalized = normalize_a_share_symbol(symbol)
        if normalized.endswith(".BJ") or asset_type_from_symbol(normalized) != AssetType.STOCK:
            raise UnsupportedCapability(
                f"BaoStock only serves adjustment factors for SH/SZ A-share stocks, not {normalized}"
            )
        client = self._client()
        start_text = (start or date(1990, 12, 19)).strftime("%Y-%m-%d")
        end_text = (end or date.today()).strftime("%Y-%m-%d")
        remaining_timeout(self._timeout_seconds)
        with self._lock, suppress_thread_stdout():
            remaining_timeout(self._timeout_seconds)
            login = client.login()
            if str(getattr(login, "error_code", "-1")) != "0":
                raise ProviderUnavailable(
                    f"BaoStock login failed: {getattr(login, 'error_msg', 'unknown error')}"
                )
            try:
                remaining_timeout(self._timeout_seconds)
                result = client.query_adjust_factor(
                    code=_baostock_symbol(normalized),
                    start_date=start_text,
                    end_date=end_text,
                )
                if str(getattr(result, "error_code", "-1")) != "0":
                    raise ProviderUnavailable(
                        f"BaoStock adjust-factor query failed: {getattr(result, 'error_msg', 'unknown error')}"
                    )
                rows: list[list[str]] = []
                while result.next():
                    remaining_timeout(self._timeout_seconds)
                    rows.append(result.get_row_data())
                dataframe = pd.DataFrame(rows, columns=result.fields)
            finally:
                with suppress(Exception):
                    client.logout()
        by_date: dict[date, AdjustFactor] = {}
        if not dataframe.empty:
            for row in dataframe.to_dict("records"):
                parsed_ts = to_datetime(record_value(row, "dividOperateDate", "ex_date", "date"))
                if parsed_ts is None:
                    continue
                ex_day = parsed_ts.date()
                if (start is not None and ex_day < start) or (end is not None and ex_day > end):
                    continue
                qfq = decimal_or_none(record_value(row, "foreAdjustFactor", "qfq_factor"))
                hfq = decimal_or_none(record_value(row, "backAdjustFactor", "hfq_factor"))
                if qfq is None and hfq is None:
                    continue
                by_date[ex_day] = AdjustFactor(
                    symbol=normalized,
                    ex_date=ex_day,
                    qfq_factor=qfq,
                    hfq_factor=hfq,
                    market=Market.CN,
                )
        return SourcePayload(
            sorted(by_date.values(), key=lambda item: item.ex_date),
            source="baostock",
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


def _fields_for(interval: Interval) -> str:
    if interval in {Interval.MINUTE_15, Interval.HOUR_1}:
        return "date,time,code,open,high,low,close,volume,amount"
    if interval == Interval.DAY_1:
        return "date,code,open,high,low,close,volume,amount,tradestatus"
    return "date,code,open,high,low,close,volume,amount"


def _normalize_intraday_time(dataframe: pd.DataFrame) -> pd.DataFrame:
    if "time" not in dataframe.columns:
        return dataframe
    frame = dataframe.copy()
    dates = frame["date"].astype(str).str.strip() if "date" in frame.columns else None
    timestamps: list[str] = []
    for index, raw_time in enumerate(frame["time"].astype(str).str.strip()):
        digits = "".join(ch for ch in raw_time if ch.isdigit())
        if len(digits) >= 14:
            timestamps.append(
                f"{digits[:4]}-{digits[4:6]}-{digits[6:8]} "
                f"{digits[8:10]}:{digits[10:12]}:{digits[12:14]}"
            )
        elif len(digits) == 6 and dates is not None:
            day = dates.iloc[index]
            timestamps.append(f"{day} {digits[:2]}:{digits[2:4]}:{digits[4:6]}")
        else:
            timestamps.append(raw_time)
    frame["datetime"] = timestamps
    return frame


def _baostock_symbol(symbol: str) -> str:
    code, exchange = symbol.split(".", maxsplit=1)
    return f"{exchange.lower()}.{code}"


_BAOSTOCK_FREQUENCIES = {
    Interval.MINUTE_15: "15",
    Interval.HOUR_1: "60",
    Interval.DAY_1: "d",
    Interval.WEEK_1: "w",
    Interval.MONTH_1: "m",
}

_BAOSTOCK_ADJUST = {
    Adjust.HFQ: "1",
    Adjust.QFQ: "2",
    Adjust.NONE: "3",
}
