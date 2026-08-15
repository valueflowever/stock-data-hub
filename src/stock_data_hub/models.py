"""Provider-neutral market-data contracts.

All public objects in this module are deliberately plain dataclasses. A host
application can use the package without inheriting a framework, ORM, or the
current repository's configuration model.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Generic, TypeVar


class Capability(str, Enum):
    BARS = "bars"
    QUOTE = "quote"
    SNAPSHOT = "snapshot"
    INSTRUMENTS = "instruments"
    TRADE_CALENDAR = "trade_calendar"
    COMPANY_PROFILE = "company_profile"
    RESTRICTED_RELEASES = "restricted_releases"
    DISCLOSURES = "disclosures"
    US_DAILY_CLOSES = "us_daily_closes"


class Market(str, Enum):
    """Market region encoded in a normalized symbol."""

    CN = "CN"
    HK = "HK"
    US = "US"


class AssetType(str, Enum):
    """Provider-neutral asset classification used for endpoint routing."""

    STOCK = "stock"
    ETF = "etf"
    INDEX = "index"


class Interval(str, Enum):
    MINUTE_15 = "15m"
    HOUR_1 = "1h"
    DAY_1 = "1d"
    WEEK_1 = "1w"
    MONTH_1 = "1M"


class Adjust(str, Enum):
    QFQ = "qfq"
    HFQ = "hfq"
    NONE = "none"


class QuoteFreshness(str, Enum):
    REALTIME = "realtime"
    DELAYED = "delayed"
    EOD = "eod"


@dataclass(frozen=True)
class AShareBar:
    """An OHLCV bar.

    The historical ``AShareBar`` name is retained for compatibility; the hub
    now also uses this contract for Hong Kong and US bars.  ``market`` tells
    callers which normalized symbol convention applies.  ``ts`` is the bar's
    close timestamp for intraday data and its trading date for daily data.
    """

    symbol: str
    ts: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None = None
    amount: Decimal | None = None
    is_closed: bool = True
    market: Market = Market.CN


# Clearer name for new integrations while preserving the original public type.
MarketBar = AShareBar


@dataclass(frozen=True)
class Quote:
    symbol: str
    price: Decimal
    ts: datetime
    market: Market = Market.CN
    freshness: QuoteFreshness = QuoteFreshness.REALTIME


@dataclass(frozen=True)
class Instrument:
    symbol: str
    name: str
    market: Market = Market.CN
    asset_type: AssetType = AssetType.STOCK


@dataclass(frozen=True)
class SnapshotRow:
    symbol: str
    name: str
    price: Decimal | None
    amount: Decimal | None
    market_cap: Decimal | None
    turnover_ratio: Decimal | None
    change_pct: Decimal | None


@dataclass(frozen=True)
class Snapshot:
    rows: tuple[SnapshotRow, ...]
    has_market_cap: bool
    fetched_at: datetime


@dataclass(frozen=True)
class TradeCalendar:
    trade_dates: frozenset[date]
    start_date: date
    end_date: date
    fetched_at: datetime


@dataclass(frozen=True)
class CompanyProfile:
    symbol: str
    industry: str | None
    main_business: str | None
    business_scope: str | None


@dataclass(frozen=True)
class RestrictedRelease:
    symbol: str
    release_date: date
    shares: Decimal | None
    market_value: Decimal | None
    ratio_of_total: Decimal | None
    share_type: str | None


@dataclass(frozen=True)
class Disclosure:
    symbol: str
    announce_date: date
    title: str


@dataclass(frozen=True)
class DailyClose:
    symbol: str
    day: date
    close: Decimal


T = TypeVar("T")


@dataclass(frozen=True)
class SourcePayload(Generic[T]):
    """A provider response before the hub adds routing metadata.

    The source field can name the actual upstream selected inside an adapter.
    For example, one AkShare adapter can truthfully report akshare:sina after
    an Eastmoney attempt fails.
    """

    value: T
    source: str | None = None
    warnings: tuple[str, ...] = ()
    # Peer adapters preserve the remote node's explicit stale-data state.
    stale: bool = False


@dataclass(frozen=True)
class FetchMetadata:
    source: str
    fetched_at: datetime
    latency_ms: float
    attempted_sources: tuple[str, ...] = ()
    cache_hit: bool = False
    stale: bool = False
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class DataResult(Generic[T]):
    value: T
    metadata: FetchMetadata
