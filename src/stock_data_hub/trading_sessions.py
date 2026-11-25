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


def window_has_trading_session(
    start: datetime,
    end: datetime,
    market: Market,
    now: datetime | None = None,
) -> bool:
    """Return whether ``[start, end]`` contains at least one already-opened session."""

    zone = market_timezone(market)
    reference = (now or datetime.now(tz=zone)).astimezone(zone)
    first_day = start.date() if start.tzinfo is None else start.astimezone(zone).date()
    end_day = end.date() if end.tzinfo is None else end.astimezone(zone).date()
    # Daily date labels are carried as Beijing midnight; when callers pass
    # Beijing midnight for US/HK dates, keep the label's calendar date rather
    # than shifting it backward into the previous local evening.
    if start.tzinfo is not None and start.tzinfo == ZoneInfo("Asia/Shanghai") and (
        start.hour == 0 and start.minute == 0 and start.second == 0 and start.microsecond == 0
    ):
        first_day = start.date()
    if end.tzinfo is not None and end.tzinfo == ZoneInfo("Asia/Shanghai") and (
        end.hour == 0 and end.minute == 0 and end.second == 0 and end.microsecond == 0
    ):
        end_day = end.date()

    last_day = min(end_day, reference.date())
    if first_day > last_day:
        return False

    try:
        # Check from most recent day backward over a bounded 31-day span first;
        # no exchange holiday lasts longer than a month.
        scan_start = max(first_day, last_day - timedelta(days=31))
        candidate = last_day
        while candidate >= scan_start:
            calendar = _calendar(market, candidate.year)
            iso = candidate.isoformat()
            if iso in calendar.sessions:
                session_open = calendar.session_open(iso).to_pydatetime()
                if session_open <= reference:
                    return True
            candidate -= timedelta(days=1)
        # A span longer than 31 days before scan_start always contains trading days.
        return scan_start > first_day
    except (ValueError, OverflowError):
        days = min((last_day - first_day).days, 6)
        return any((first_day + timedelta(days=offset)).weekday() < 5 for offset in range(days + 1))


def latest_active_session_date(market: Market, now: datetime | None = None) -> date | None:
    """Return the most recent exchange session date that has already opened."""

    zone = market_timezone(market)
    reference = (now or datetime.now(tz=zone)).astimezone(zone)
    candidate = reference.date()
    try:
        for _ in range(31):
            calendar = _calendar(market, candidate.year)
            iso = candidate.isoformat()
            if (
                iso in calendar.sessions
                and calendar.session_open(iso).to_pydatetime() <= reference
            ):
                return candidate
            candidate -= timedelta(days=1)
    except (ValueError, OverflowError):
        return None
    return None


def calendar_trade_dates(
    market: Market = Market.CN,
    *,
    start_year: int = 2005,
    end_year: int | None = None,
) -> frozenset[date]:
    """Return known exchange sessions across a bounded year span."""

    last_year = end_year or (datetime.now(tz=market_timezone(market)).year + 1)
    dates: set[date] = set()
    for year in range(start_year, last_year + 1):
        try:
            calendar = _calendar(market, year)
        except (ValueError, OverflowError):
            continue
        for session in calendar.sessions:
            dates.add(session.date())
    return frozenset(dates)


@lru_cache(maxsize=8192)
def _session_bounds(
    market: Market,
    day: date,
) -> tuple[datetime, datetime | None, datetime | None, datetime] | None:
    zone = market_timezone(market)
    try:
        calendar = _calendar(market, day.year)
    except (ValueError, OverflowError):
        return None
    iso = day.isoformat()
    if iso not in calendar.sessions:
        return None
    sess_open = calendar.session_open(iso).to_pydatetime().astimezone(zone)
    sess_close = calendar.session_close(iso).to_pydatetime().astimezone(zone)
    break_start: datetime | None = None
    break_end: datetime | None = None
    raw_break_start = calendar.session_break_start(iso)
    raw_break_end = calendar.session_break_end(iso)
    if (
        raw_break_start is not None
        and hasattr(raw_break_start, "to_pydatetime")
        and str(raw_break_start) != "NaT"
    ):
        break_start = raw_break_start.to_pydatetime().astimezone(zone)
    if (
        raw_break_end is not None
        and hasattr(raw_break_end, "to_pydatetime")
        and str(raw_break_end) != "NaT"
    ):
        break_end = raw_break_end.to_pydatetime().astimezone(zone)
    return sess_open, break_start, break_end, sess_close


def intraday_bar_close(
    open_timestamp: datetime,
    duration: timedelta,
    market: Market,
) -> datetime:
    """Compute canonical bar-close timestamp for an OPEN-labeled intraday bar.

    Clamps partial session-ending bars (such as the US 15:30-16:00 ET 1h bar,
    US early-close half-days at 13:00 ET, and HK 11:30-12:00 HKT morning end)
    to the actual session or lunch-break close rather than overshooting.
    """

    zone = market_timezone(market)
    local_open = (
        open_timestamp.replace(tzinfo=zone)
        if open_timestamp.tzinfo is None
        else open_timestamp.astimezone(zone)
    )
    raw_close = local_open + duration
    bounds = _session_bounds(market, local_open.date())
    if bounds is not None:
        sess_open, break_start, _, sess_close = bounds
        if (
            break_start is not None
            and sess_open <= local_open < break_start
            and raw_close > break_start
        ):
            return break_start
        if sess_open <= local_open < sess_close and raw_close > sess_close:
            return sess_close
    return raw_close
