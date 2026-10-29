"""Typed wire envelopes; imported only by the optional HTTP service."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from stock_data_hub.health import ProviderHealth
from stock_data_hub.indicators.contracts import FactorColumn, IndicatorDefinition, IndicatorMetadata
from stock_data_hub.models import (
    Adjust,
    AssetType,
    FetchMetadata,
    Instrument,
    Interval,
    Market,
    SnapshotRow,
)

T = TypeVar("T")


class DataEnvelope(BaseModel, Generic[T]):
    data: T


class DataResponse(DataEnvelope[T], Generic[T]):
    metadata: FetchMetadata


class SnapshotPage(BaseModel):
    rows: list[SnapshotRow]
    has_market_cap: bool
    fetched_at: datetime
    total_rows: int
    offset: int
    limit: int


class InstrumentPage(BaseModel):
    items: list[Instrument]
    total: int
    offset: int
    limit: int


class CalendarPage(BaseModel):
    trade_dates: list[date]
    start_date: date
    end_date: date
    fetched_at: datetime
    count: int


class HealthResponse(DataEnvelope[list[ProviderHealth]]):
    pass


class HubMetrics(BaseModel):
    requests: int
    cache_hits: int
    stale_returns: int
    coalesced_requests: int
    fallbacks: int
    failures: int
    active_provider_calls: dict[str, int]


class ErrorDetail(BaseModel):
    type: str
    message: str
    sources: dict[str, str] | None = None


class ErrorResponse(BaseModel):
    error: ErrorDetail


class ValidationResponse(BaseModel):
    detail: list[dict[str, Any]]


class AuthenticationResponse(BaseModel):
    detail: str


class IndicatorCatalogPage(BaseModel):
    catalog_version: str
    items: list[IndicatorDefinition]
    total: int
    offset: int
    limit: int


class IndicatorDefinitionData(BaseModel):
    catalog_version: str
    item: IndicatorDefinition


class FactorRecord(BaseModel):
    model_config = ConfigDict(extra="allow")

    __pydantic_extra__: dict[str, Decimal | None] = Field(init=False)
    ts: datetime
    is_closed: bool


class FactorFrameData(BaseModel):
    symbol: str
    market: Market
    asset_type: AssetType
    interval: Interval
    adjust: Adjust
    requested_start: datetime
    requested_end: datetime
    columns: list[FactorColumn]
    rows: list[FactorRecord]


class IndicatorResponse(BaseModel):
    data: FactorFrameData
    metadata: IndicatorMetadata
