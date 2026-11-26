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


def _period_first(day: date, interval: Interval) -> date:
    if interval is Interval.WEEK_1:
        return day - timedelta(days=day.weekday())
    if interval is Interval.MONTH_1:
        return day.replace(day=1)
    return day


def _is_universal_non_session(day: date) -> bool:
    """Weekends and New Year's Day (Jan 1) are closed on CN, HK, and US exchanges."""
    return day.weekday() >= 5 or (day.month == 1 and day.day == 1)


@lru_cache(maxsize=8192)
def period_anchor_date(day: date, interval: Interval, market: Market) -> date:
    """Return the canonical period-end trading date for weekly/monthly bars.

    Anchors weekly and monthly bars to the last actual exchange session in that
    period (for example Friday, or Thursday when Friday is a holiday). Outside
    published calendar ranges, falls back to the last non-Jan-1 weekday.
    """
    if interval not in {Interval.WEEK_1, Interval.MONTH_1}:
        return day
    first = _period_first(day, interval)
    end = period_end(day, interval)
    candidate = end
    fallback_weekday: date | None = None
    while candidate >= first:
        try:
            calendar = _calendar(market, candidate.year)
        except (ValueError, OverflowError):
            if not _is_universal_non_session(candidate) and fallback_weekday is None:
                fallback_weekday = candidate
            candidate -= timedelta(days=1)
            continue
        if candidate.isoformat() in calendar.sessions:
            return candidate
        candidate -= timedelta(days=1)
    if fallback_weekday is not None:
        return fallback_weekday
    # Full-week/month holiday fallback: use Friday for weekly or month-end for monthly.
    if interval is Interval.WEEK_1:
        return first + timedelta(days=4)
    return end


@lru_cache(maxsize=8192)
def period_close(day: date, interval: Interval, market: Market) -> datetime | None:
    """Return an actual session close; None means no known session in the period.

    Calendar range errors are left to callers, who must treat them conservatively.
    """
    end = period_end(day, interval)
    first = _period_first(day, interval)
    candidate = end
    while candidate >= first:
        try:
            calendar = _calendar(market, candidate.year)
        except (ValueError, OverflowError):
            if _is_universal_non_session(candidate):
                candidate -= timedelta(days=1)
                continue
            raise
        # A calendar's first/last session may sit inside the year's boundaries.
        if candidate.isoformat() in calendar.sessions:
            close = calendar.session_close(candidate.isoformat()).to_pydatetime()
            # Preserve the existing mainland publication grace; HK has a
            # closing auction after the continuous-session close.
            minutes = {Market.CN: 5, Market.HK: 10, Market.US: 0}[market]
            return close + timedelta(minutes=minutes)
        candidate -= timedelta(days=1)
    return None


def _is_beijing_midnight_label(value: datetime) -> bool:
    return (
        value.tzinfo is not None
        and value.utcoffset() == timedelta(hours=8)
        and value.hour == 0
        and value.minute == 0
        and value.second == 0
        and value.microsecond == 0
    )


def window_has_trading_session(
    start: datetime,
    end: datetime,
    market: Market,
    now: datetime | None = None,
    interval: Interval = Interval.DAY_1,
) -> bool:
    """Return whether ``[start, end]`` contains at least one already-opened session."""

    zone = market_timezone(market)
    reference = (now or datetime.now(tz=zone)).astimezone(zone)

    if interval in {Interval.MINUTE_15, Interval.HOUR_1}:
        local_start = (
            start.replace(tzinfo=zone) if start.tzinfo is None else start.astimezone(zone)
        )
        local_end = end.replace(tzinfo=zone) if end.tzinfo is None else end.astimezone(zone)
        effective_end = min(local_end, reference)
        if local_start > effective_end:
            return False
        first_day = local_start.date()
        last_day = effective_end.date()
        try:
            scan_start = max(first_day, last_day - timedelta(days=31))
            candidate = last_day
            while candidate >= scan_start:
                calendar = _calendar(market, candidate.year)
                iso = candidate.isoformat()
                if iso in calendar.sessions:
                    sess_open = calendar.session_open(iso).to_pydatetime().astimezone(zone)
                    sess_close = calendar.session_close(iso).to_pydatetime().astimezone(zone)
                    if sess_open <= effective_end and sess_close >= local_start:
                        return True
                candidate -= timedelta(days=1)
            return scan_start > first_day
        except (ValueError, OverflowError):
            days = min((last_day - first_day).days, 6)
            return any(
                (first_day + timedelta(days=offset)).weekday() < 5 for offset in range(days + 1)
            )

    first_day = start.date() if start.tzinfo is None else start.astimezone(zone).date()
    end_day = end.date() if end.tzinfo is None else end.astimezone(zone).date()
    # Daily+ date labels are carried as Beijing midnight (+08:00); keep the
    # label's calendar date rather than shifting it into the previous US/HK evening.
    if _is_beijing_midnight_label(start):
        first_day = start.date()
    if _is_beijing_midnight_label(end):
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
