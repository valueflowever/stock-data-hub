"""Framework-neutral contracts for technical indicators and factor frames."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from stock_data_hub.models import Adjust, AssetType, FetchMetadata, Interval, Market


class IndicatorParameterKind(str, Enum):
    INTEGER = "integer"
    NUMBER = "number"


ParameterValue = int | float
FactorValue = Decimal | None


@dataclass(frozen=True)
class IndicatorParameter:
    name: str
    kind: IndicatorParameterKind
    default: ParameterValue
    minimum: ParameterValue
    maximum: ParameterValue
    description: str


@dataclass(frozen=True)
class IndicatorOutput:
    name: str
    description: str
    dtype: str = "decimal"


@dataclass(frozen=True)
class IndicatorDefinition:
    id: str
    definition_version: str
    name: str
    description: str
    category: str
    required_inputs: tuple[str, ...]
    parameters: tuple[IndicatorParameter, ...]
    outputs: tuple[IndicatorOutput, ...]
    default_lookback: int
    backend: str
    variant: str | None = None
    overlay: bool = False
    status: str = "stable"
    available: bool = False
    unavailable_reason: str | None = None


@dataclass(frozen=True)
class IndicatorSpec:
    id: str
    params: Mapping[str, ParameterValue] | None = None
    alias: str | None = None
    definition_version: str | None = None


@dataclass(frozen=True)
class IndicatorExecution:
    indicator_id: str
    definition_version: str
    alias: str
    params: Mapping[str, ParameterValue]
    outputs: Mapping[str, str]
    lookback: int
    backend: str
    backend_version: str


@dataclass(frozen=True)
class FactorColumn:
    name: str
    role: str
    dtype: str
    indicator_id: str | None = None
    definition_version: str | None = None
    output: str | None = None
    params: Mapping[str, ParameterValue] | None = None
    valid_from: datetime | None = None


@dataclass(frozen=True)
class FactorRow:
    ts: datetime
    is_closed: bool
    values: Mapping[str, FactorValue]


@dataclass(frozen=True)
class FactorFrame:
    symbol: str
    market: Market
    asset_type: AssetType
    interval: Interval
    adjust: Adjust
    requested_start: datetime
    requested_end: datetime
    columns: tuple[FactorColumn, ...]
    rows: tuple[FactorRow, ...]

    def to_records(self) -> list[dict[str, Any]]:
        """Return flat records while retaining Decimal values for exact serialization."""

        return [{"ts": row.ts, "is_closed": row.is_closed, **row.values} for row in self.rows]

    def to_pandas(self) -> Any:
        """Return a float-oriented DataFrame suitable for research and backtests."""

        import pandas as pd

        records = []
        for row in self.rows:
            values = {
                key: float(value) if isinstance(value, Decimal) else value
                for key, value in row.values.items()
            }
            records.append({"ts": row.ts, "is_closed": row.is_closed, **values})
        if not records:
            names = ["is_closed", *(column.name for column in self.columns)]
            frame = pd.DataFrame(columns=names)
            frame.index = pd.DatetimeIndex([], name="ts")
            return frame
        return pd.DataFrame.from_records(records).set_index("ts")


@dataclass(frozen=True)
class IndicatorRunMetadata:
    catalog_version: str
    computed_at: datetime
    latency_ms: float
    calculation_start: datetime | None
    warmup_rows_required: int
    warmup_rows_loaded: int
    input_rows: int
    output_rows: int
    input_fingerprint: str
    closed_only: bool
    adjustment_as_of: datetime | None
    executions: tuple[IndicatorExecution, ...]
    cache_hit: bool = False


@dataclass(frozen=True)
class IndicatorMetadata:
    source: str
    fetched_at: datetime
    latency_ms: float
    attempted_sources: tuple[str, ...]
    cache_hit: bool
    stale: bool
    warnings: tuple[str, ...]
    computation: IndicatorRunMetadata

    @classmethod
    def from_bars(
        cls,
        metadata: FetchMetadata,
        computation: IndicatorRunMetadata,
        warnings: tuple[str, ...],
        total_latency_ms: float,
    ) -> IndicatorMetadata:
        return cls(
            source=metadata.source,
            fetched_at=metadata.fetched_at,
            latency_ms=total_latency_ms,
            attempted_sources=metadata.attempted_sources,
            cache_hit=metadata.cache_hit,
            stale=metadata.stale,
            warnings=warnings,
            computation=computation,
        )


@dataclass(frozen=True)
class IndicatorResult:
    value: FactorFrame
    metadata: IndicatorMetadata
