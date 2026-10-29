"""Exchange sessions, including holidays, daylight saving, and early closes."""

from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime, timedelta
from functools import lru_cache
from threading import RLock
from zoneinfo import ZoneInfo

import exchange_calendars as calendars

from stock_data_hub.models import Interval, Market

_CALENDAR_NAMES = {Market.CN: "XSHG", Market.HK: "XHKG", Market.US: "XNYS"}
_TIMEZONES = {
    Market.CN: ZoneInfo("Asia/Shanghai"),
    Market.HK: ZoneInfo("Asia/Hong_Kong"),
    Market.US: ZoneInfo("America/New_York"),
}
_CALENDAR_LOCK = RLock()


def market_timezone(market: Market) -> ZoneInfo:
    return _TIMEZONES[market]


@lru_cache(maxsize=64)
def _calendar(market: Market, year: int):
    # The calendar dispatcher has mutable shared caches. Construct one bounded
    # year at a time, and serialize construction across provider threads.
    with _CALENDAR_LOCK:
        return calendars.get_calendar(
            _CALENDAR_NAMES[market],
            start=f"{year}-01-01",
            end=f"{year}-12-31",
        )


def period_end(day: date, interval: Interval) -> date:
    if interval is Interval.WEEK_1:
        return day + timedelta(days=6 - day.weekday())
    if interval is Interval.MONTH_1:
        return day.replace(day=monthrange(day.year, day.month)[1])
    return day


@lru_cache(maxsize=8192)
def period_close(day: date, interval: Interval, market: Market) -> datetime | None:
    """Return an actual session close; None means no known session in the period.

    Calendar range errors are left to callers, who must treat them conservatively.
    """
    end = period_end(day, interval)
    first = day
    if interval is Interval.WEEK_1:
        first = day - timedelta(days=day.weekday())
    elif interval is Interval.MONTH_1:
        first = day.replace(day=1)
    candidate = end
    while candidate >= first:
        calendar = _calendar(market, candidate.year)
        # A calendar's first/last session may sit inside the year's boundaries.
        if candidate.isoformat() in calendar.sessions:
            close = calendar.session_close(candidate.isoformat()).to_pydatetime()
            # Preserve the existing mainland publication grace; HK has a
            # closing auction after the continuous-session close.
            minutes = {Market.CN: 5, Market.HK: 10, Market.US: 0}[market]
            return close + timedelta(minutes=minutes)
        candidate -= timedelta(days=1)
    return None
