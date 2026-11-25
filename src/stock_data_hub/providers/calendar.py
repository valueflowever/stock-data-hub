"""Local offline fallback for CN trading calendars using exchange-calendars."""

from __future__ import annotations

from datetime import datetime

from stock_data_hub.errors import DataValidationError
from stock_data_hub.models import Capability, Market, SourcePayload, TradeCalendar
from stock_data_hub.normalization import BEIJING_TZ
from stock_data_hub.providers.base import MarketDataProvider
from stock_data_hub.trading_sessions import calendar_trade_dates


class ExchangeCalendarProvider(MarketDataProvider):
    """Serve mainland China trade dates locally when upstream calendars fail."""

    name = "exchange_calendar"
    capabilities = frozenset({Capability.TRADE_CALENDAR})
    markets = frozenset({Market.CN})

    def __init__(self, *, start_year: int = 2005, end_year: int | None = None) -> None:
        self._start_year = start_year
        self._end_year = end_year

    def fetch_trade_calendar(self) -> SourcePayload[TradeCalendar]:
        dates = calendar_trade_dates(
            Market.CN,
            start_year=self._start_year,
            end_year=self._end_year,
        )
        if not dates:
            raise DataValidationError("Local exchange calendar produced no trading dates")
        return SourcePayload(
            value=TradeCalendar(
                trade_dates=dates,
                start_date=min(dates),
                end_date=max(dates),
                fetched_at=datetime.now(tz=BEIJING_TZ),
            ),
            source="exchange_calendar:XSHG",
            warnings=("Used local exchange-calendars fallback for trade calendar.",),
        )
