"""Stable indicator registry with explicit parameter and backend resolution."""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace

from stock_data_hub.errors import (
    IndicatorBackendUnavailable,
    IndicatorNotFound,
    InvalidIndicatorRequest,
)
from stock_data_hub.indicators.backends.base import IndicatorBackend
from stock_data_hub.indicators.catalog import DEFINITIONS_BY_ID, INDICATOR_DEFINITIONS
from stock_data_hub.indicators.contracts import (
    IndicatorDefinition,
    IndicatorExecution,
    IndicatorParameterKind,
    IndicatorSpec,
    ParameterValue,
)

_ALIAS_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
_RESERVED_COLUMNS = {
    "ts",
    "is_closed",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
}


@dataclass(frozen=True)
class ResolvedIndicator:
    definition: IndicatorDefinition
    backend: IndicatorBackend
    execution: IndicatorExecution


class IndicatorRegistry:
    def __init__(
        self,
        definitions: Sequence[IndicatorDefinition] = INDICATOR_DEFINITIONS,
        backends: Iterable[IndicatorBackend] = (),
    ) -> None:
        self._definitions = {definition.id: definition for definition in definitions}
        self._backends = {backend.name: backend for backend in backends}

    def catalog(self) -> tuple[IndicatorDefinition, ...]:
        entries = []
        for definition in self._definitions.values():
            backend = self._backends.get(definition.backend)
            available = backend is not None and backend.supports(definition.id)
            entries.append(
                replace(
                    definition,
                    available=available,
                    unavailable_reason=(
                        None
                        if available
                        else f"Install the optional {definition.backend} indicator backend."
                    ),
                )
            )
        return tuple(entries)

    def definition(self, indicator_id: str) -> IndicatorDefinition:
        normalized = str(indicator_id).strip().lower()
        definition = self._definitions.get(normalized)
        if definition is None:
            raise IndicatorNotFound(f"Unknown indicator: {indicator_id}")
        return next(item for item in self.catalog() if item.id == normalized)

    def resolve(self, specs: Sequence[IndicatorSpec]) -> tuple[ResolvedIndicator, ...]:
        if not specs:
            raise InvalidIndicatorRequest("At least one indicator is required")

        resolved: list[ResolvedIndicator] = []
        aliases: set[str] = set()
        columns: set[str] = set(_RESERVED_COLUMNS)
        for spec in specs:
            definition = self._raw_definition(spec.id)
            if (
                spec.definition_version is not None
                and spec.definition_version != definition.definition_version
            ):
                raise InvalidIndicatorRequest(
                    f"{definition.id} definition version is {definition.definition_version}, "
                    f"not {spec.definition_version}"
                )
            params = _normalize_params(definition, spec.params or {})
            _validate_cross_parameters(definition.id, params)
            alias = str(spec.alias or definition.id.rsplit(".", 1)[-1]).strip()
            if not _ALIAS_PATTERN.fullmatch(alias):
                raise InvalidIndicatorRequest(
                    f"Invalid alias {alias!r}; use a letter followed by letters, digits, or underscores"
                )
            if alias in aliases:
                raise InvalidIndicatorRequest(f"Duplicate indicator alias: {alias}")
            aliases.add(alias)

            backend = self._backends.get(definition.backend)
            if backend is None or not backend.supports(definition.id):
                raise IndicatorBackendUnavailable(
                    f"{definition.id} requires the optional {definition.backend} backend"
                )
            lookback = backend.lookback(definition.id, params)
            if lookback < 0:
                raise InvalidIndicatorRequest(f"{definition.id} returned a negative lookback")

            output_columns = {
                output.name: alias if len(definition.outputs) == 1 else f"{alias}.{output.name}"
                for output in definition.outputs
            }
            conflicts = columns.intersection(output_columns.values())
            if conflicts:
                raise InvalidIndicatorRequest(
                    f"Indicator output column conflicts with another column: {min(conflicts)}"
                )
            columns.update(output_columns.values())
            execution = IndicatorExecution(
                indicator_id=definition.id,
                definition_version=definition.definition_version,
                alias=alias,
                params=params,
                outputs=output_columns,
                lookback=lookback,
                backend=backend.name,
                backend_version=backend.version,
            )
            resolved.append(ResolvedIndicator(definition, backend, execution))
        return tuple(resolved)

    def _raw_definition(self, indicator_id: str) -> IndicatorDefinition:
        normalized = str(indicator_id).strip().lower()
        definition = self._definitions.get(normalized)
        if definition is None:
            raise IndicatorNotFound(f"Unknown indicator: {indicator_id}")
        return definition


def create_default_registry() -> IndicatorRegistry:
    backends: list[IndicatorBackend] = []
    try:
        from stock_data_hub.indicators.backends.talib import TaLibBackend

        backends.append(TaLibBackend())
    except (ImportError, OSError):
        pass
    return IndicatorRegistry(backends=backends)


def _normalize_params(
    definition: IndicatorDefinition,
    supplied: Mapping[str, ParameterValue],
) -> dict[str, ParameterValue]:
    expected = {parameter.name: parameter for parameter in definition.parameters}
    unknown = set(supplied).difference(expected)
    if unknown:
        raise InvalidIndicatorRequest(f"Unknown parameter for {definition.id}: {min(unknown)}")
    result: dict[str, ParameterValue] = {}
    for name, parameter in expected.items():
        value = supplied.get(name, parameter.default)
        if isinstance(value, bool):
            raise InvalidIndicatorRequest(f"{definition.id}.{name} must be a number")
        if parameter.kind is IndicatorParameterKind.INTEGER:
            if not isinstance(value, int):
                raise InvalidIndicatorRequest(f"{definition.id}.{name} must be an integer")
            normalized: ParameterValue = int(value)
        else:
            if not isinstance(value, (int, float)):
                raise InvalidIndicatorRequest(f"{definition.id}.{name} must be a number")
            try:
                normalized = float(value)
            except OverflowError as exc:
                raise InvalidIndicatorRequest(
                    f"{definition.id}.{name} must be a finite number"
                ) from exc
            if not math.isfinite(normalized):
                raise InvalidIndicatorRequest(f"{definition.id}.{name} must be a finite number")
        if normalized < parameter.minimum or normalized > parameter.maximum:
            raise InvalidIndicatorRequest(
                f"{definition.id}.{name} must be between "
                f"{parameter.minimum} and {parameter.maximum}"
            )
        result[name] = normalized
    return result


def _validate_cross_parameters(
    indicator_id: str,
    params: Mapping[str, ParameterValue],
) -> None:
    if indicator_id == "trend.macd" and params["fast_period"] >= params["slow_period"]:
        raise InvalidIndicatorRequest("trend.macd fast_period must be less than slow_period")


assert set(DEFINITIONS_BY_ID) == {definition.id for definition in INDICATOR_DEFINITIONS}
