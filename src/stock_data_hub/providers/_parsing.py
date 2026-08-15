"""Small parsing helpers shared by optional provider adapters."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any

import pandas as pd

from stock_data_hub.errors import DataValidationError
from stock_data_hub.models import AShareBar, Interval, Market
from stock_data_hub.normalization import BEIJING_TZ, is_bar_closed, to_beijing


class IntradayTimestampLabel(str, Enum):
    """Meaning of the raw timestamp supplied by an intraday provider."""

    OPEN = "open"
    CLOSE = "close"


_INTRADAY_DURATIONS = {
    Interval.MINUTE_15: timedelta(minutes=15),
    Interval.HOUR_1: timedelta(hours=1),
}


def bars_from_dataframe(
    dataframe: pd.DataFrame | None,
    *,
    symbol: str,
    interval: Interval,
    start: datetime,
    end: datetime,
    market: Market,
    time_columns: Sequence[str],
    open_columns: Sequence[str] = ("open", "开盘"),
    high_columns: Sequence[str] = ("high", "最高"),
    low_columns: Sequence[str] = ("low", "最低"),
    close_columns: Sequence[str] = ("close", "收盘"),
    volume_columns: Sequence[str] = ("volume", "vol", "成交量"),
    amount_columns: Sequence[str] = ("amount", "成交额"),
    intraday_timestamp_label: IntradayTimestampLabel | None = None,
    now: datetime | None = None,
) -> list[AShareBar]:
    """Normalize a provider dataframe into validated, chronologically sorted bars.

    The public Hub contract uses a bar-close timestamp for intraday data.
    Every intraday adapter must therefore declare whether its raw source label
    denotes the bar open or close before data enters the shared contract.
    """

    if dataframe is None or dataframe.empty:
        return []
    start_bj = to_beijing(start)
    end_bj = to_beijing(end)
    bars: list[AShareBar] = []
    for _, row in dataframe.iterrows():
        raw_timestamp = to_datetime(record_value(row, *time_columns))
        timestamp = normalize_bar_timestamp(
            raw_timestamp,
            interval,
            intraday_timestamp_label=intraday_timestamp_label,
        )
        if timestamp is None or timestamp < start_bj or timestamp > end_bj:
            continue
        try:
            open_price = required_positive_decimal(record_value(row, *open_columns), "open")
            high_price = required_positive_decimal(record_value(row, *high_columns), "high")
            low_price = required_positive_decimal(record_value(row, *low_columns), "low")
            close_price = required_positive_decimal(record_value(row, *close_columns), "close")
            if (
                low_price > min(open_price, close_price)
                or max(open_price, close_price) > high_price
            ):
                raise DataValidationError("Invalid OHLC relationship")
            volume = decimal_or_none(record_value(row, *volume_columns))
            amount = decimal_or_none(record_value(row, *amount_columns))
            if (volume is not None and volume < 0) or (amount is not None and amount < 0):
                raise DataValidationError("Volume and amount cannot be negative")
        except DataValidationError:
            continue
        bars.append(
            AShareBar(
                symbol=symbol,
                ts=timestamp,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=volume,
                amount=amount,
                is_closed=is_bar_closed(timestamp, interval, now=now, market=market),
                market=market,
            )
        )
    return sorted(bars, key=lambda item: item.ts)


def normalize_bar_timestamp(
    value: datetime | None,
    interval: Interval,
    *,
    intraday_timestamp_label: IntradayTimestampLabel | None,
) -> datetime | None:
    """Return the Hub's canonical bar timestamp from a provider label."""

    if value is None:
        return None
    timestamp = to_beijing(value)
    duration = _INTRADAY_DURATIONS.get(interval)
    if duration is None:
        return timestamp
    if intraday_timestamp_label is None:
        raise ValueError(
            f"{interval.value} bars require an explicit intraday_timestamp_label"
        )
    if intraday_timestamp_label is IntradayTimestampLabel.CLOSE:
        return timestamp
    if intraday_timestamp_label is IntradayTimestampLabel.OPEN:
        return timestamp + duration
    raise ValueError(
        f"Unsupported intraday timestamp label: {intraday_timestamp_label!r}"
    )


def record_value(record: Any, *names: str) -> object | None:
    """Return the first present non-null field, including dotted mapping paths."""

    for name in names:
        value = _field_value(record, name)
        if not is_missing(value):
            return value
    return None


def to_datetime(value: object | None) -> datetime | None:
    if is_missing(value):
        return None
    if isinstance(value, datetime):
        return to_beijing(value)
    if isinstance(value, date):
        return to_beijing(datetime.combine(value, datetime.min.time()))
    if isinstance(value, (int, float)) or (isinstance(value, str) and value.strip().isdigit()):
        try:
            text = str(value).strip()
            numeric = float(value)
            if len(text) == 8 and text.isdigit() and 19000101 <= int(text) <= 29991231:
                return datetime.strptime(text, "%Y%m%d").replace(tzinfo=BEIJING_TZ)
            if abs(numeric) >= 10_000_000_000:
                numeric /= 1000
            if abs(numeric) >= 100_000_000:
                return datetime.fromtimestamp(numeric, tz=timezone.utc).astimezone(BEIJING_TZ)
        except (TypeError, ValueError, OSError, OverflowError):
            return None
    try:
        parsed = pd.to_datetime(value).to_pydatetime()
    except (TypeError, ValueError, OverflowError):
        return None
    return to_beijing(parsed)


def decimal_or_none(value: object | None) -> Decimal | None:
    if is_missing(value):
        return None
    try:
        decimal = Decimal(str(value).replace(",", ""))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return decimal if decimal.is_finite() else None


def required_positive_decimal(value: object | None, field: str) -> Decimal:
    decimal = decimal_or_none(value)
    if decimal is None or decimal <= 0:
        raise DataValidationError(f"Missing or invalid {field}")
    return decimal


def is_missing(value: object | None) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _field_value(record: Any, name: str) -> object | None:
    current: Any = record
    for part in name.split("."):
        if isinstance(current, Mapping):
            if part not in current:
                return None
            current = current[part]
        elif hasattr(current, "index") and part in current.index:
            current = current.get(part)
        else:
            return None
    return current
