"""Stable technical-indicator contracts and computation facade."""

from stock_data_hub.indicators.catalog import CATALOG_VERSION, INDICATOR_DEFINITIONS
from stock_data_hub.indicators.contracts import (
    FactorColumn,
    FactorFrame,
    FactorRow,
    IndicatorDefinition,
    IndicatorExecution,
    IndicatorMetadata,
    IndicatorOutput,
    IndicatorParameter,
    IndicatorParameterKind,
    IndicatorResult,
    IndicatorRunMetadata,
    IndicatorSpec,
)
from stock_data_hub.indicators.engine import IndicatorEngine, create_default_indicator_engine
from stock_data_hub.indicators.registry import IndicatorRegistry

__all__ = [
    "CATALOG_VERSION",
    "INDICATOR_DEFINITIONS",
    "FactorColumn",
    "FactorFrame",
    "FactorRow",
    "IndicatorDefinition",
    "IndicatorEngine",
    "IndicatorExecution",
    "IndicatorMetadata",
    "IndicatorOutput",
    "IndicatorParameter",
    "IndicatorParameterKind",
    "IndicatorRegistry",
    "IndicatorResult",
    "IndicatorRunMetadata",
    "IndicatorSpec",
    "create_default_indicator_engine",
]
