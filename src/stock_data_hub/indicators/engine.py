"""Single-symbol, multi-indicator computation over normalized Hub bars."""

from __future__ import annotations

import asyncio
import hashlib
import math
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from decimal import Decimal
from time import perf_counter

import numpy as np

from stock_data_hub.errors import IndicatorCalculationError, InvalidIndicatorRequest
from stock_data_hub.hub import DataHub
from stock_data_hub.indicators.catalog import CATALOG_VERSION
from stock_data_hub.indicators.contracts import (
    FactorColumn,
    FactorFrame,
    FactorRow,
    IndicatorDefinition,
    IndicatorMetadata,
    IndicatorResult,
    IndicatorRunMetadata,
    IndicatorSpec,
)
from stock_data_hub.indicators.registry import (
    IndicatorRegistry,
    ResolvedIndicator,
    create_default_registry,
)
from stock_data_hub.models import Adjust, AShareBar, AssetType, Interval, Market
from stock_data_hub.normalization import (
    BEIJING_TZ,
    asset_type_from_symbol,
    market_from_symbol,
    normalize_symbol,
    to_beijing,
)

_INPUT_COLUMNS = ("open", "high", "low", "close", "volume", "amount")


class IndicatorEngine:
    def __init__(
        self,
        hub: DataHub,
        registry: IndicatorRegistry | None = None,
        *,
        max_indicators: int = 32,
        max_input_rows: int = 100_000,
        max_output_cells: int = 2_000_000,
        max_warmup_expansions: int = 4,
        max_concurrent_calculations: int = 2,
    ) -> None:
        if max_indicators < 1:
            raise ValueError("max_indicators must be at least one")
        if max_input_rows < 1:
            raise ValueError("max_input_rows must be at least one")
        if max_output_cells < 1:
            raise ValueError("max_output_cells must be at least one")
        if max_warmup_expansions < 0:
            raise ValueError("max_warmup_expansions cannot be negative")
        if max_concurrent_calculations < 1:
            raise ValueError("max_concurrent_calculations must be at least one")
        self.hub = hub
        self.registry = registry or create_default_registry()
        self.max_indicators = max_indicators
        self.max_input_rows = max_input_rows
        self.max_output_cells = max_output_cells
        self.max_warmup_expansions = max_warmup_expansions
        self._calculation_slots = asyncio.BoundedSemaphore(max_concurrent_calculations)

    def catalog(self) -> tuple[IndicatorDefinition, ...]:
        return self.registry.catalog()

    def definition(self, indicator_id: str) -> IndicatorDefinition:
        return self.registry.definition(indicator_id)

    async def calculate(
        self,
        symbol: str,
        interval: Interval | str,
        start: datetime,
        end: datetime,
        indicators: Sequence[IndicatorSpec],
        *,
        adjust: Adjust | str,
        market: Market | str | None = None,
        asset_type: AssetType | str | None = None,
        closed_only: bool = True,
        include_inputs: bool = True,
        on_insufficient: str = "null",
        allow_peers: bool = True,
    ) -> IndicatorResult:
        started_at = perf_counter()
        if len(indicators) > self.max_indicators:
            raise InvalidIndicatorRequest(
                f"At most {self.max_indicators} indicators can be computed at once"
            )
        if on_insufficient not in {"null", "error"}:
            raise InvalidIndicatorRequest("on_insufficient must be 'null' or 'error'")

        normalized_symbol = normalize_symbol(symbol, market)
        normalized_market = market_from_symbol(normalized_symbol)
        normalized_asset_type = asset_type_from_symbol(normalized_symbol, asset_type)
        normalized_interval = Interval(interval)
        normalized_adjust = Adjust(adjust)
        start_bj = to_beijing(start)
        end_bj = to_beijing(end)
        if start_bj > end_bj:
            raise InvalidIndicatorRequest("start must be before end")

        resolved = self.registry.resolve(indicators)
        required_warmup = max(item.execution.lookback for item in resolved)
        bars_result, eligible_bars, excluded_open = await self._load_bars_with_warmup(
            normalized_symbol,
            normalized_interval,
            start_bj,
            end_bj,
            normalized_adjust,
            normalized_market,
            normalized_asset_type,
            required_warmup,
            closed_only,
            allow_peers,
        )

        pre_bars = [bar for bar in eligible_bars if bar.ts < start_bj]
        output_bars = [bar for bar in eligible_bars if start_bj <= bar.ts <= end_bj]
        output_columns = sum(len(item.definition.outputs) for item in resolved)
        if include_inputs:
            output_columns += len(_INPUT_COLUMNS)
        output_cells = len(output_bars) * output_columns
        if output_cells > self.max_output_cells:
            raise InvalidIndicatorRequest(
                f"Indicator output exceeds the {self.max_output_cells}-cell limit"
            )
        loaded_warmup = min(len(pre_bars), required_warmup)
        if loaded_warmup < required_warmup and on_insufficient == "error":
            raise InvalidIndicatorRequest(
                f"Insufficient history: required {required_warmup} warm-up bars, "
                f"loaded {loaded_warmup}"
            )

        retained_pre = pre_bars[-required_warmup:] if required_warmup else []
        fingerprint_bars = [*retained_pre, *output_bars]
        if len(fingerprint_bars) > self.max_input_rows:
            raise InvalidIndicatorRequest(
                f"Indicator input exceeds the {self.max_input_rows}-row limit"
            )

        compute_started_at = perf_counter()
        async with self._calculation_slots:
            factor_columns, factor_values = await asyncio.to_thread(
                _compute_factor_values,
                resolved,
                pre_bars,
                output_bars,
                include_inputs,
            )
        compute_latency_ms = (perf_counter() - compute_started_at) * 1000

        rows = tuple(
            FactorRow(
                ts=bar.ts,
                is_closed=bar.is_closed,
                values={name: values[index] for name, values in factor_values.items()},
            )
            for index, bar in enumerate(output_bars)
        )
        frame = FactorFrame(
            symbol=normalized_symbol,
            market=normalized_market,
            asset_type=normalized_asset_type,
            interval=normalized_interval,
            adjust=normalized_adjust,
            requested_start=start_bj,
            requested_end=end_bj,
            columns=factor_columns,
            rows=rows,
        )

        warnings = list(bars_result.metadata.warnings)
        if loaded_warmup < required_warmup:
            warnings.append(
                f"Insufficient history: required {required_warmup} warm-up bars, "
                f"loaded {loaded_warmup}; leading values are null."
            )
        if excluded_open:
            warnings.append(f"Excluded {excluded_open} unclosed bar(s) from indicator inputs.")
        if not closed_only and any(not bar.is_closed for bar in output_bars):
            warnings.append("Results include an unclosed bar and may change before it closes.")
        if normalized_adjust is Adjust.QFQ:
            warnings.append(
                "QFQ history may be revised after later corporate actions and is not point-in-time."
            )
        warnings = list(dict.fromkeys(warnings))

        calculation_start = fingerprint_bars[0].ts if fingerprint_bars else None
        computation = IndicatorRunMetadata(
            catalog_version=CATALOG_VERSION,
            computed_at=datetime.now(tz=BEIJING_TZ),
            latency_ms=compute_latency_ms,
            calculation_start=calculation_start,
            warmup_rows_required=required_warmup,
            warmup_rows_loaded=loaded_warmup,
            input_rows=len(fingerprint_bars),
            output_rows=len(output_bars),
            input_fingerprint=_fingerprint(
                fingerprint_bars,
                normalized_symbol,
                normalized_interval,
                normalized_adjust,
            ),
            closed_only=closed_only,
            adjustment_as_of=(
                bars_result.metadata.fetched_at if normalized_adjust is not Adjust.NONE else None
            ),
            executions=tuple(item.execution for item in resolved),
        )
        total_latency_ms = (perf_counter() - started_at) * 1000
        metadata = IndicatorMetadata.from_bars(
            bars_result.metadata,
            computation,
            tuple(warnings),
            total_latency_ms,
        )
        return IndicatorResult(value=frame, metadata=metadata)

    async def _load_bars_with_warmup(
        self,
        symbol: str,
        interval: Interval,
        start: datetime,
        end: datetime,
        adjust: Adjust,
        market: Market,
        asset_type: AssetType,
        required_warmup: int,
        closed_only: bool,
        allow_peers: bool,
    ):
        span = _initial_warmup_span(interval, required_warmup)
        previous_earliest: datetime | None = None
        result = None
        eligible: list[AShareBar] = []
        excluded_open = 0
        for expansion in range(self.max_warmup_expansions + 1):
            candidate_start = _safe_subtract(start, span)
            result = await self.hub.get_bars(
                symbol,
                interval,
                candidate_start,
                end,
                adjust,
                market,
                asset_type,
                allow_peers=allow_peers,
            )
            bars = _sorted_unique(result.value)
            excluded_open = sum(1 for bar in bars if closed_only and not bar.is_closed)
            eligible = [bar for bar in bars if bar.is_closed or not closed_only]
            if len(eligible) > self.max_input_rows:
                raise InvalidIndicatorRequest(
                    f"Indicator input exceeds the {self.max_input_rows}-row limit"
                )
            loaded = sum(1 for bar in eligible if bar.ts < start)
            if loaded >= required_warmup or required_warmup == 0:
                break
            earliest = eligible[0].ts if eligible else None
            if expansion == self.max_warmup_expansions:
                break
            if previous_earliest is not None and earliest == previous_earliest:
                break
            previous_earliest = earliest
            span *= 2
        assert result is not None
        return result, eligible, excluded_open


def create_default_indicator_engine(hub: DataHub) -> IndicatorEngine:
    return IndicatorEngine(hub)


def _compute_factor_values(
    resolved: Sequence[ResolvedIndicator],
    pre_bars: Sequence[AShareBar],
    output_bars: Sequence[AShareBar],
    include_inputs: bool,
) -> tuple[tuple[FactorColumn, ...], Mapping[str, list[Decimal | None]]]:
    columns: list[FactorColumn] = []
    values: dict[str, list[Decimal | None]] = {}
    if include_inputs:
        valid_from = output_bars[0].ts if output_bars else None
        for name in _INPUT_COLUMNS:
            columns.append(
                FactorColumn(name=name, role="input", dtype="decimal", valid_from=valid_from)
            )
            values[name] = [getattr(bar, name) for bar in output_bars]

    for item in resolved:
        execution = item.execution
        spec_pre = list(pre_bars[-execution.lookback :]) if execution.lookback else []
        calculation_bars = [*spec_pre, *output_bars]
        output_values: Mapping[str, np.ndarray]
        if output_bars:
            inputs = _input_arrays(
                calculation_bars, item.definition.required_inputs, item.definition.id
            )
            output_values = item.backend.compute(
                item.definition.id,
                execution.params,
                inputs,
            )
        else:
            output_values = {
                output.name: np.asarray([], dtype=np.float64) for output in item.definition.outputs
            }

        expected_outputs = {output.name for output in item.definition.outputs}
        if set(output_values) != expected_outputs:
            raise IndicatorCalculationError(
                f"{item.backend.name} returned unexpected outputs for {item.definition.id}"
            )
        offset = len(spec_pre)
        for output in item.definition.outputs:
            raw = np.asarray(output_values[output.name])
            if len(raw) != len(calculation_bars):
                raise IndicatorCalculationError(
                    f"{item.backend.name} returned a misaligned {output.name} output"
                )
            converted = [_finite_decimal(value) for value in raw[offset:]]
            column_name = execution.outputs[output.name]
            valid_from = next(
                (
                    output_bars[index].ts
                    for index, value in enumerate(converted)
                    if value is not None
                ),
                None,
            )
            values[column_name] = converted
            columns.append(
                FactorColumn(
                    name=column_name,
                    role="factor",
                    dtype=output.dtype,
                    indicator_id=item.definition.id,
                    definition_version=item.definition.definition_version,
                    output=output.name,
                    params=execution.params,
                    valid_from=valid_from,
                )
            )
    return tuple(columns), values


def _input_arrays(
    bars: Sequence[AShareBar],
    required_inputs: Sequence[str],
    indicator_id: str,
) -> dict[str, np.ndarray]:
    inputs: dict[str, np.ndarray] = {}
    for name in required_inputs:
        raw = [getattr(bar, name) for bar in bars]
        if any(value is None for value in raw):
            raise InvalidIndicatorRequest(
                f"{indicator_id} requires non-null {name} for every input bar"
            )
        inputs[name] = np.asarray([float(value) for value in raw], dtype=np.float64)
    return inputs


def _finite_decimal(value: object) -> Decimal | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return Decimal(str(number))


def _sorted_unique(bars: Sequence[AShareBar]) -> list[AShareBar]:
    by_timestamp = {bar.ts: bar for bar in bars}
    return sorted(by_timestamp.values(), key=lambda bar: bar.ts)


def _initial_warmup_span(interval: Interval, lookback: int) -> timedelta:
    if lookback <= 0:
        return timedelta()
    if interval is Interval.MINUTE_15:
        return timedelta(minutes=15 * lookback * 16)
    if interval is Interval.HOUR_1:
        return timedelta(hours=lookback * 8)
    if interval is Interval.DAY_1:
        return timedelta(days=lookback * 2)
    if interval is Interval.WEEK_1:
        return timedelta(weeks=lookback * 2)
    return timedelta(days=31 * lookback * 2)


def _safe_subtract(value: datetime, span: timedelta) -> datetime:
    try:
        return value - span
    except OverflowError:
        return datetime.min.replace(tzinfo=value.tzinfo)


def _fingerprint(
    bars: Sequence[AShareBar],
    symbol: str,
    interval: Interval,
    adjust: Adjust,
) -> str:
    digest = hashlib.sha256()
    digest.update(f"v1|{symbol}|{interval.value}|{adjust.value}\n".encode("ascii"))
    for bar in bars:
        fields = (
            bar.ts.isoformat(),
            format(bar.open, "f"),
            format(bar.high, "f"),
            format(bar.low, "f"),
            format(bar.close, "f"),
            "" if bar.volume is None else format(bar.volume, "f"),
            "" if bar.amount is None else format(bar.amount, "f"),
            "1" if bar.is_closed else "0",
        )
        digest.update(("|".join(fields) + "\n").encode("ascii"))
    return f"sha256:{digest.hexdigest()}"
