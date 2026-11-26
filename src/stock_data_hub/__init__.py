"""Standalone, source-agnostic market data access layer."""

import logging

from stock_data_hub._workers import RateLimiter
from stock_data_hub.doctor import (
    DoctorRecommendation,
    DoctorReport,
    ProbeResult,
    ProbeSpec,
    run_doctor,
)
from stock_data_hub.errors import (
    AllSourcesFailed,
    DataHubError,
    DataValidationError,
    IndicatorBackendUnavailable,
    IndicatorCalculationError,
    IndicatorError,
    IndicatorNotFound,
    InvalidIndicatorRequest,
    InvalidRequest,
    NoProviderConfigured,
    ProviderError,
    ProviderUnavailable,
    ResourceBusy,
    SymbolDataUnavailable,
    UnsupportedCapability,
)
from stock_data_hub.hub import CachePolicy, DataHub, RouteKey
from stock_data_hub.indicators import (
    FactorFrame,
    IndicatorDefinition,
    IndicatorEngine,
    IndicatorRegistry,
    IndicatorResult,
    IndicatorSpec,
    create_default_indicator_engine,
)
from stock_data_hub.models import (
    DATA_CONTRACT_VERSION,
    Adjust,
    AdjustFactor,
    AdjustMethod,
    AShareBar,
    AssetType,
    BarDataQuality,
    BatchQuoteError,
    BatchQuoteItem,
    Capability,
    CompanyProfile,
    DailyClose,
    DataResult,
    Disclosure,
    FetchMetadata,
    Instrument,
    Interval,
    Market,
    MarketBar,
    Quote,
    QuoteFreshness,
    RestrictedRelease,
    Snapshot,
    SnapshotRow,
    TradeCalendar,
)
from stock_data_hub.normalization import BEIJING_TZ
from stock_data_hub.providers.akshare import AkShareProvider, CninfoProvider
from stock_data_hub.providers.baostock import BaoStockProvider
from stock_data_hub.providers.calendar import ExchangeCalendarProvider
from stock_data_hub.providers.defaults import create_default_hub
from stock_data_hub.providers.efinance import EFinanceProvider
from stock_data_hub.providers.pytdx import PyTDXProvider
from stock_data_hub.providers.remote import RemoteHubProvider
from stock_data_hub.providers.tencent import TencentQuoteProvider
from stock_data_hub.providers.tickflow import TickFlowProvider
from stock_data_hub.providers.yfinance import YFinanceProvider
from stock_data_hub.serialization import to_jsonable
from stock_data_hub.service import create_app
from stock_data_hub.sqlite_cache import PERSISTENT_CACHE_POLICIES, SQLiteResultCache

__version__ = "0.8.0"

logging.getLogger("stock_data_hub").addHandler(logging.NullHandler())

__all__ = [
    "BEIJING_TZ",
    "DATA_CONTRACT_VERSION",
    "PERSISTENT_CACHE_POLICIES",
    "AShareBar",
    "Adjust",
    "AdjustFactor",
    "AdjustMethod",
    "AkShareProvider",
    "AllSourcesFailed",
    "AssetType",
    "BaoStockProvider",
    "BarDataQuality",
    "BatchQuoteError",
    "BatchQuoteItem",
    "CachePolicy",
    "Capability",
    "CninfoProvider",
    "CompanyProfile",
    "DailyClose",
    "DataHub",
    "DataHubError",
    "DataResult",
    "DataValidationError",
    "Disclosure",
    "DoctorRecommendation",
    "DoctorReport",
    "EFinanceProvider",
    "ExchangeCalendarProvider",
    "FactorFrame",
    "FetchMetadata",
    "IndicatorBackendUnavailable",
    "IndicatorCalculationError",
    "IndicatorDefinition",
    "IndicatorEngine",
    "IndicatorError",
    "IndicatorNotFound",
    "IndicatorRegistry",
    "IndicatorResult",
    "IndicatorSpec",
    "Instrument",
    "Interval",
    "InvalidIndicatorRequest",
    "InvalidRequest",
    "Market",
    "MarketBar",
    "NoProviderConfigured",
    "ProbeResult",
    "ProbeSpec",
    "ProviderError",
    "ProviderUnavailable",
    "PyTDXProvider",
    "Quote",
    "QuoteFreshness",
    "RateLimiter",
    "RemoteHubProvider",
    "ResourceBusy",
    "RestrictedRelease",
    "RouteKey",
    "SQLiteResultCache",
    "Snapshot",
    "SnapshotRow",
    "SymbolDataUnavailable",
    "TencentQuoteProvider",
    "TickFlowProvider",
    "TradeCalendar",
    "UnsupportedCapability",
    "YFinanceProvider",
    "__version__",
    "create_app",
    "create_default_hub",
    "create_default_indicator_engine",
    "run_doctor",
    "to_jsonable",
]
