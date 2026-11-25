"""Extension contract for data-source adapters."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime

from stock_data_hub.errors import UnsupportedCapability
from stock_data_hub.models import (
    Adjust,
    AdjustFactor,
    AShareBar,
    AssetType,
    Capability,
    CompanyProfile,
    DailyClose,
    Disclosure,
    Instrument,
    Interval,
    Market,
    Quote,
    RestrictedRelease,
    Snapshot,
    SourcePayload,
    TradeCalendar,
)


class MarketDataProvider:
    """Base class for a source adapter.

    A new provider only implements capabilities it can serve. The hub checks
    capabilities before calling it, and default methods make missing
    implementations fail loudly rather than silently returning partial data.
    """

    name = "unnamed"
    capabilities: frozenset[Capability] = frozenset()
    # An empty set retains backward compatibility for third-party providers:
    # it means the adapter may receive any market symbol.
    markets: frozenset[Market] = frozenset()
    asset_types: frozenset[AssetType] = frozenset()
    intervals: frozenset[Interval] = frozenset()
    adjustments: frozenset[Adjust] = frozenset()
    accepts_asset_type = False
    accepts_us_close_adjust = False
    is_peer = False
    # Historical-only sources (e.g. TickFlow free) omit the developing daily bar
    # during market hours. Setting this to False lets the hub prefer a live
    # provider when the requested window reaches the active trading session.
    supports_intraday_daily_bar = True
    supports_batch_quotes = False

    def clear_cache(self) -> None:
        """Clear optional adapter-local shared snapshots."""

    def prefetch_quotes(
        self,
        symbols: Sequence[str],
        *,
        asset_type: AssetType | None = None,
    ) -> None:
        """Optionally warm adapter-local quote cache in one batch upstream call."""

    def supports(self, capability: Capability) -> bool:
        return capability in self.capabilities

    def supports_market(self, market: Market) -> bool:
        return not self.markets or market in self.markets

    def supports_request(
        self,
        capability: Capability,
        *,
        market: Market | None = None,
        asset_type: AssetType | None = None,
        interval: Interval | None = None,
        adjust: Adjust | None = None,
    ) -> bool:
        if not self.supports(capability):
            return False
        if market is not None and not self.supports_market(market):
            return False
        if asset_type is not None and self.asset_types and asset_type not in self.asset_types:
            return False
        if capability == Capability.BARS:
            if interval is not None and self.intervals and interval not in self.intervals:
                return False
            if adjust is not None and self.adjustments and adjust not in self.adjustments:
                return False
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
        raise self._unsupported(Capability.BARS)

    def fetch_quote(
        self,
        symbol: str,
        *,
        asset_type: AssetType | None = None,
    ) -> SourcePayload[Quote]:
        raise self._unsupported(Capability.QUOTE)

    def fetch_snapshot(self) -> SourcePayload[Snapshot]:
        raise self._unsupported(Capability.SNAPSHOT)

    def fetch_instruments(self) -> SourcePayload[list[Instrument]]:
        raise self._unsupported(Capability.INSTRUMENTS)

    def fetch_trade_calendar(self) -> SourcePayload[TradeCalendar]:
        raise self._unsupported(Capability.TRADE_CALENDAR)

    def fetch_company_profile(self, symbol: str) -> SourcePayload[CompanyProfile]:
        raise self._unsupported(Capability.COMPANY_PROFILE)

    def fetch_restricted_releases(self, symbol: str) -> SourcePayload[list[RestrictedRelease]]:
        raise self._unsupported(Capability.RESTRICTED_RELEASES)

    def fetch_disclosures(
        self,
        symbol: str,
        start: date,
        end: date,
    ) -> SourcePayload[list[Disclosure]]:
        raise self._unsupported(Capability.DISCLOSURES)

    def fetch_us_daily_closes(
        self,
        ticker: str,
        max_rows: int,
        *,
        adjust: Adjust = Adjust.NONE,
    ) -> SourcePayload[list[DailyClose]]:
        raise self._unsupported(Capability.US_DAILY_CLOSES)

    def fetch_adjust_factors(
        self,
        symbol: str,
        start: date | None = None,
        end: date | None = None,
    ) -> SourcePayload[list[AdjustFactor]]:
        raise self._unsupported(Capability.ADJUST_FACTORS)

    def _unsupported(self, capability: Capability) -> UnsupportedCapability:
        return UnsupportedCapability(f"{self.name} does not support {capability.value}")
