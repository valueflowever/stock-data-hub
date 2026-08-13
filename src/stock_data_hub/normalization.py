"""Shared normalization rules, independent from any upstream SDK."""

from __future__ import annotations

import re
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from stock_data_hub.errors import InvalidRequest
from stock_data_hub.models import AssetType, Interval, Market

BEIJING_TZ = ZoneInfo("Asia/Shanghai")
A_SHARE_DAILY_CLOSE_TIME = time(15, 5)


def normalize_a_share_symbol(raw: str) -> str:
    """Return a canonical A-share symbol such as 000001.SZ or 600519.SH."""

    value = str(raw or "").strip().upper()
    match = re.fullmatch(
        r"(?:(?P<prefix>SH|SZ|BJ))?(?P<digits>\d{6})(?:\.(?P<suffix>SH|SZ|BJ))?",
        value,
    )
    if match is None:
        raise InvalidRequest(f"Invalid A-share symbol: {raw!r}")
    digits = match.group("digits")
    if digits.startswith(("5", "6", "9")):
        inferred_suffix = "SH"
    elif digits.startswith(("4", "8")):
        inferred_suffix = "BJ"
    elif digits.startswith(("0", "1", "2", "3")):
        inferred_suffix = "SZ"
    else:
        raise InvalidRequest(f"Cannot infer A-share exchange for symbol: {raw!r}")
    prefix = match.group("prefix")
    suffix = match.group("suffix")
    if prefix is not None and suffix is not None and prefix != suffix:
        raise InvalidRequest(f"Conflicting exchanges in A-share symbol: {raw!r}")
    supplied_suffix = prefix or suffix
    if supplied_suffix is not None and supplied_suffix != inferred_suffix:
        raise InvalidRequest(f"Exchange does not match A-share symbol: {raw!r}")
    return f"{digits}.{inferred_suffix}"


def prefixed_a_share_symbol(symbol: str) -> str:
    normalized = normalize_a_share_symbol(symbol)
    code, suffix = normalized.split(".", maxsplit=1)
    return f"{suffix.lower()}{code}"


def normalize_symbol(raw: str, market: Market | str | None = None) -> str:
    """Return a provider-neutral symbol such as ``600519.SH`` or ``AAPL.US``.

    A six-digit Chinese code without a suffix is inferred as an A-share/ETF.
    Supplying an exchange suffix avoids silently rewriting index-style codes
    such as ``000001.SH``.
    """

    value = str(raw or "").strip().upper()
    requested_market = _normalize_market(market)
    if not value:
        raise InvalidRequest("symbol cannot be empty")

    if value.endswith(".HK"):
        if requested_market not in (None, Market.HK):
            raise InvalidRequest(f"Symbol market does not match {market!r}: {raw!r}")
        code = value[:-3]
        if not re.fullmatch(r"\d{1,5}", code):
            raise InvalidRequest(f"Invalid Hong Kong symbol: {raw!r}")
        return f"{code.zfill(5)}.HK"

    if value.endswith(".US"):
        if requested_market not in (None, Market.US):
            raise InvalidRequest(f"Symbol market does not match {market!r}: {raw!r}")
        return _normalize_us_symbol(value[:-3])

    if re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", value):
        if requested_market not in (None, Market.CN):
            raise InvalidRequest(f"Symbol market does not match {market!r}: {raw!r}")
        code, suffix = value.split(".", maxsplit=1)
        inferred_suffix = _infer_cn_exchange(code)
        is_sh_index = code.startswith("000") and suffix == "SH"
        if inferred_suffix is not None and suffix != inferred_suffix and not is_sh_index:
            raise InvalidRequest(f"Exchange does not match Chinese symbol: {raw!r}")
        return value

    if requested_market == Market.HK:
        if not re.fullmatch(r"\d{1,5}", value):
            raise InvalidRequest(f"Invalid Hong Kong symbol: {raw!r}")
        return f"{value.zfill(5)}.HK"
    if requested_market == Market.US:
        return _normalize_us_symbol(value)
    if requested_market in (None, Market.CN) and re.fullmatch(r"(?:SH|SZ|BJ)?\d{6}", value):
        return normalize_a_share_symbol(value)
    if requested_market is None:
        return _normalize_us_symbol(value)
    raise InvalidRequest(f"Invalid symbol for {requested_market.value}: {raw!r}")


def market_from_symbol(symbol: str) -> Market:
    normalized = normalize_symbol(symbol)
    if normalized.endswith(".HK"):
        return Market.HK
    if normalized.endswith(".US"):
        return Market.US
    return Market.CN


def asset_type_from_symbol(
    symbol: str,
    asset_type: AssetType | str | None = None,
) -> AssetType:
    """Return an explicit asset type or infer common Chinese code families.

    Global stock and ETF symbols share the same exchange notation, so callers
    should pass ``asset_type`` when that distinction affects provider routing.
    """

    requested = normalize_asset_type(asset_type)
    if requested is not None:
        return requested
    normalized = normalize_symbol(symbol)
    if market_from_symbol(normalized) != Market.CN:
        return AssetType.STOCK
    code, exchange = normalized.split(".", maxsplit=1)
    if (exchange == "SH" and code.startswith("000")) or (
        exchange == "SZ" and code.startswith("399")
    ):
        return AssetType.INDEX
    if code.startswith(("15", "16", "18", "50", "51", "52", "53", "56", "58")):
        return AssetType.ETF
    return AssetType.STOCK


def normalize_asset_type(value: AssetType | str | None) -> AssetType | None:
    if value is None or isinstance(value, AssetType):
        return value
    aliases = {
        "STOCK": AssetType.STOCK,
        "A_SHARE": AssetType.STOCK,
        "A": AssetType.STOCK,
        "ETF": AssetType.ETF,
        "FUND": AssetType.ETF,
        "INDEX": AssetType.INDEX,
    }
    try:
        return aliases[str(value).strip().upper()]
    except KeyError as exc:
        raise InvalidRequest(f"Unsupported asset type: {value!r}") from exc


def _normalize_market(value: Market | str | None) -> Market | None:
    if value is None:
        return None
    if isinstance(value, Market):
        return value
    aliases = {
        "A": Market.CN,
        "A_SHARE": Market.CN,
        "CN": Market.CN,
        "ETF": Market.CN,
        "HK": Market.HK,
        "US": Market.US,
    }
    try:
        return aliases[str(value).strip().upper()]
    except KeyError as exc:
        raise InvalidRequest(f"Unsupported market: {value!r}") from exc


def _normalize_us_symbol(value: str) -> str:
    ticker = value.strip().upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9.-]{0,14}", ticker):
        raise InvalidRequest(f"Invalid US symbol: {value!r}")
    return f"{ticker}.US"


def _infer_cn_exchange(code: str) -> str | None:
    if code.startswith(("5", "6", "9")):
        return "SH"
    if code.startswith(("4", "8")):
        return "BJ"
    if code.startswith(("0", "1", "2", "3")):
        return "SZ"
    return None


def to_beijing(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=BEIJING_TZ)
    return value.astimezone(BEIJING_TZ)


def is_bar_closed(
    value: datetime,
    interval: Interval,
    now: datetime | None = None,
    market: Market = Market.CN,
) -> bool:
    """Return whether a bar whose timestamp marks its opening is complete."""

    timestamp = to_beijing(value)
    reference = to_beijing(now or datetime.now(tz=BEIJING_TZ))
    if interval == Interval.DAY_1:
        if market == Market.CN:
            close_at = datetime.combine(
                timestamp.date(), A_SHARE_DAILY_CLOSE_TIME, tzinfo=BEIJING_TZ
            )
            return close_at <= reference
        return timestamp.date() < reference.date()
    if interval == Interval.WEEK_1:
        week_end = timestamp + timedelta(days=4 - timestamp.weekday())
        if market == Market.CN:
            close_at = datetime.combine(
                week_end.date(), A_SHARE_DAILY_CLOSE_TIME, tzinfo=BEIJING_TZ
            )
            return close_at <= reference
        return week_end.date() < reference.date()
    if interval == Interval.MONTH_1:
        if timestamp.month == 12:
            next_month = timestamp.replace(year=timestamp.year + 1, month=1, day=1)
        else:
            next_month = timestamp.replace(month=timestamp.month + 1, day=1)
        return next_month <= reference
    duration = {
        Interval.MINUTE_15: timedelta(minutes=15),
        Interval.HOUR_1: timedelta(hours=1),
    }[interval]
    return timestamp + duration <= reference
