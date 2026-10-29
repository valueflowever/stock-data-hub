"""Small parsing helpers shared by optional provider adapters."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone, tzinfo
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any

import pandas as pd

from stock_data_hub.errors import DataValidationError
from stock_data_hub.models import Adjust, AShareBar, BarDataQuality, Interval, Market, SourcePayload
from stock_data_hub.normalization import BEIJING_TZ, is_bar_closed, to_beijing
from stock_data_hub.trading_sessions import market_timezone


class IntradayTimestampLabel(str, Enum):
    """Meaning of the raw timestamp supplied by an intraday provider."""

    OPEN = "open"
    CLOSE = "close"


_INTRADAY_DURATIONS = {
    Interval.MINUTE_15: timedelta(minutes=15),
    Interval.HOUR_1: timedelta(hours=1),
}


@dataclass
class ParseDiagnostics:
    input_rows: int = 0
    filtered_rows: int = 0
    duplicate_rows: int = 0


def bar_payload_from_dataframe(
    dataframe: pd.DataFrame | None,
    *,
    source: str,
    warnings: tuple[str, ...] = (),
    truncated: bool = False,
    **kwargs: Any,
) -> SourcePayload[list[AShareBar]]:
    diagnostics = ParseDiagnostics()
    bars = bars_from_dataframe(dataframe, diagnostics=diagnostics, **kwargs)
    if diagnostics.filtered_rows:
        warnings = (*warnings, f"Filtered {diagnostics.filtered_rows} invalid bar row(s).")
    if diagnostics.duplicate_rows:
        warnings = (*warnings, f"Removed {diagnostics.duplicate_rows} duplicate timestamp row(s).")
    if truncated:
        warnings = (*warnings, "Provider history limit reached; the requested history is partial.")
    return SourcePayload(
        bars,
        source=source,
        warnings=warnings,
        quality=BarDataQuality(
            requested_start=to_beijing(kwargs["start"]),
            requested_end=to_beijing(kwargs["end"]),
            available_start=bars[0].ts if bars else None,
            available_end=bars[-1].ts if bars else None,
            input_rows=diagnostics.input_rows,
            returned_rows=len(bars),
            filtered_rows=diagnostics.filtered_rows,
            duplicate_rows=diagnostics.duplicate_rows,
            truncated=truncated,
            currency={Market.CN: "CNY", Market.HK: "HKD", Market.US: "USD"}[kwargs["market"]],
        ),
    )


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
    adjust: Adjust = Adjust.NONE,
    volume_multiplier: Decimal = Decimal(1),
    diagnostics: ParseDiagnostics | None = None,
) -> list[AShareBar]:
    """Normalize a provider dataframe into validated, chronologically sorted bars.

    The public Hub contract uses a bar-close timestamp for intraday data.
    Every intraday adapter must therefore declare whether its raw source label
    denotes the bar open or close before data enters the shared contract.
    """

    diagnostics = diagnostics if diagnostics is not None else ParseDiagnostics()
    if not volume_multiplier.is_finite() or volume_multiplier <= 0:
        raise ValueError("volume_multiplier must be finite and positive")
    if dataframe is None or dataframe.empty:
        return []
    diagnostics.input_rows = len(dataframe)
    start_bj = to_beijing(start)
    end_bj = to_beijing(end)
    bars: list[AShareBar] = []
    for _, row in dataframe.iterrows():
        raw_timestamp = to_datetime(
            record_value(row, *time_columns), naive_timezone=market_timezone(market)
        )
        timestamp = normalize_bar_timestamp(
            raw_timestamp,
            interval,
            intraday_timestamp_label=intraday_timestamp_label,
            market=market,
        )
        if timestamp is None:
            diagnostics.filtered_rows += 1
            continue
        if timestamp < start_bj or timestamp > end_bj:
            continue
        try:
            parse_price = required_positive_decimal if adjust == Adjust.NONE else required_decimal
            open_price = parse_price(record_value(row, *open_columns), "open")
            high_price = parse_price(record_value(row, *high_columns), "high")
            low_price = parse_price(record_value(row, *low_columns), "low")
            close_price = parse_price(record_value(row, *close_columns), "close")
            if (
                low_price > min(open_price, close_price)
                or max(open_price, close_price) > high_price
            ):
                raise DataValidationError("Invalid OHLC relationship")
            volume = decimal_or_none(record_value(row, *volume_columns))
            if volume is not None:
                volume *= volume_multiplier
            amount = decimal_or_none(record_value(row, *amount_columns))
            if (volume is not None and volume < 0) or (amount is not None and amount < 0):
                raise DataValidationError("Volume and amount cannot be negative")
        except DataValidationError:
            diagnostics.filtered_rows += 1
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
    unique = {bar.ts: bar for bar in bars}
    diagnostics.duplicate_rows = len(bars) - len(unique)
    return sorted(unique.values(), key=lambda item: item.ts)


def normalize_bar_timestamp(
    value: datetime | None,
    interval: Interval,
    *,
    intraday_timestamp_label: IntradayTimestampLabel | None,
    market: Market = Market.CN,
) -> datetime | None:
    """Return the Hub's canonical bar timestamp from a provider label."""

    if value is None:
        return None
    timestamp = to_beijing(value)
    duration = _INTRADAY_DURATIONS.get(interval)
    if duration is None:
        day = timestamp.astimezone(market_timezone(market)).date()
        return datetime.combine(day, datetime.min.time(), tzinfo=BEIJING_TZ)
    if intraday_timestamp_label is None:
        raise ValueError(f"{interval.value} bars require an explicit intraday_timestamp_label")
    if intraday_timestamp_label is IntradayTimestampLabel.CLOSE:
        return timestamp
    if intraday_timestamp_label is IntradayTimestampLabel.OPEN:
        return timestamp + duration
    raise ValueError(f"Unsupported intraday timestamp label: {intraday_timestamp_label!r}")


def record_value(record: Any, *names: str) -> object | None:
    """Return the first present non-null field, including dotted mapping paths."""

    for name in names:
        value = _field_value(record, name)
        if not is_missing(value):
            return value
    return None


def to_datetime(value: object | None, *, naive_timezone: tzinfo = BEIJING_TZ) -> datetime | None:
    if is_missing(value):
        return None
    if isinstance(value, datetime):
        return to_beijing(
            value if value.tzinfo is not None else value.replace(tzinfo=naive_timezone)
        )
    if isinstance(value, date):
        return to_beijing(datetime.combine(value, datetime.min.time(), tzinfo=naive_timezone))
    if isinstance(value, (int, float)) or (isinstance(value, str) and value.strip().isdigit()):
        try:
            text = str(value).strip()
            numeric = float(value)
            if len(text) == 8 and text.isdigit() and 19000101 <= int(text) <= 29991231:
                return to_beijing(datetime.strptime(text, "%Y%m%d").replace(tzinfo=naive_timezone))
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
    return to_beijing(
        parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=naive_timezone)
    )


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


def required_decimal(value: object | None, field: str) -> Decimal:
    decimal = decimal_or_none(value)
    if decimal is None:
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
