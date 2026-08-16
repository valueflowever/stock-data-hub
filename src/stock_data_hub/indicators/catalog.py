"""Curated public indicator catalog.

Backend discovery must never add public functions or change defaults without
a definition-version change in this module.
"""

from __future__ import annotations

from stock_data_hub.indicators.contracts import (
    IndicatorDefinition,
    IndicatorOutput,
    IndicatorParameter,
    IndicatorParameterKind,
)

CATALOG_VERSION = "1.0.0"
DEFINITION_VERSION = "1.0.0"


def _period(
    name: str,
    default: int,
    *,
    minimum: int = 2,
    maximum: int = 10_000,
) -> IndicatorParameter:
    return IndicatorParameter(
        name=name,
        kind=IndicatorParameterKind.INTEGER,
        default=default,
        minimum=minimum,
        maximum=maximum,
        description="Number of input bars used by the calculation.",
    )


def _value(description: str) -> tuple[IndicatorOutput, ...]:
    return (IndicatorOutput("value", description),)


INDICATOR_DEFINITIONS = (
    IndicatorDefinition(
        id="overlap.sma",
        definition_version=DEFINITION_VERSION,
        name="Simple Moving Average",
        description="Arithmetic mean of close prices over a fixed period.",
        category="overlap",
        required_inputs=("close",),
        parameters=(_period("period", 20),),
        outputs=_value("Simple moving average."),
        default_lookback=19,
        backend="ta-lib",
        overlay=True,
    ),
    IndicatorDefinition(
        id="overlap.ema",
        definition_version=DEFINITION_VERSION,
        name="Exponential Moving Average",
        description="TA-Lib exponential moving average seeded by its canonical lookback.",
        category="overlap",
        required_inputs=("close",),
        parameters=(_period("period", 20),),
        outputs=_value("Exponential moving average."),
        default_lookback=19,
        backend="ta-lib",
        variant="talib",
        overlay=True,
    ),
    IndicatorDefinition(
        id="momentum.rsi",
        definition_version=DEFINITION_VERSION,
        name="Relative Strength Index",
        description="Wilder-style relative strength index calculated by TA-Lib.",
        category="momentum",
        required_inputs=("close",),
        parameters=(_period("period", 14),),
        outputs=_value("Relative strength index on a 0-100 scale."),
        default_lookback=14,
        backend="ta-lib",
        variant="wilder",
    ),
    IndicatorDefinition(
        id="trend.macd",
        definition_version=DEFINITION_VERSION,
        name="Moving Average Convergence Divergence",
        description="Difference between fast and slow EMAs with a signal EMA.",
        category="trend",
        required_inputs=("close",),
        parameters=(
            _period("fast_period", 12),
            _period("slow_period", 26),
            _period("signal_period", 9),
        ),
        outputs=(
            IndicatorOutput("macd", "Fast EMA minus slow EMA."),
            IndicatorOutput("signal", "Signal EMA of MACD."),
            IndicatorOutput("histogram", "MACD minus signal."),
        ),
        default_lookback=33,
        backend="ta-lib",
        variant="talib",
    ),
    IndicatorDefinition(
        id="volatility.bbands",
        definition_version=DEFINITION_VERSION,
        name="Bollinger Bands",
        description="Simple moving average with symmetric standard-deviation bands.",
        category="volatility",
        required_inputs=("close",),
        parameters=(
            _period("period", 20),
            IndicatorParameter(
                name="standard_deviations",
                kind=IndicatorParameterKind.NUMBER,
                default=2.0,
                minimum=0.1,
                maximum=10.0,
                description="Number of population standard deviations on each side.",
            ),
        ),
        outputs=(
            IndicatorOutput("upper", "Upper band."),
            IndicatorOutput("middle", "Middle moving average."),
            IndicatorOutput("lower", "Lower band."),
        ),
        default_lookback=19,
        backend="ta-lib",
        variant="sma",
        overlay=True,
    ),
    IndicatorDefinition(
        id="volatility.atr",
        definition_version=DEFINITION_VERSION,
        name="Average True Range",
        description="Wilder-smoothed true range calculated by TA-Lib.",
        category="volatility",
        required_inputs=("high", "low", "close"),
        parameters=(_period("period", 14),),
        outputs=_value("Average true range."),
        default_lookback=14,
        backend="ta-lib",
        variant="wilder",
    ),
    IndicatorDefinition(
        id="momentum.stoch",
        definition_version=DEFINITION_VERSION,
        name="Stochastic Oscillator",
        description="Slow stochastic oscillator with simple moving-average smoothing.",
        category="momentum",
        required_inputs=("high", "low", "close"),
        parameters=(
            _period("fast_k_period", 14),
            _period("slow_k_period", 3),
            _period("slow_d_period", 3),
        ),
        outputs=(
            IndicatorOutput("k", "Slow percent K."),
            IndicatorOutput("d", "Slow percent D."),
        ),
        default_lookback=17,
        backend="ta-lib",
        variant="slow-sma",
    ),
    IndicatorDefinition(
        id="trend.adx",
        definition_version=DEFINITION_VERSION,
        name="Average Directional Index",
        description="Wilder ADX with positive and negative directional indicators.",
        category="trend",
        required_inputs=("high", "low", "close"),
        parameters=(_period("period", 14),),
        outputs=(
            IndicatorOutput("adx", "Average directional index."),
            IndicatorOutput("plus_di", "Positive directional indicator."),
            IndicatorOutput("minus_di", "Negative directional indicator."),
        ),
        default_lookback=27,
        backend="ta-lib",
        variant="wilder",
    ),
    IndicatorDefinition(
        id="momentum.roc",
        definition_version=DEFINITION_VERSION,
        name="Rate of Change",
        description="Percentage change from the close a fixed number of bars ago.",
        category="momentum",
        required_inputs=("close",),
        parameters=(_period("period", 10, minimum=1),),
        outputs=_value("Rate of change in percent."),
        default_lookback=10,
        backend="ta-lib",
    ),
    IndicatorDefinition(
        id="volume.obv",
        definition_version=DEFINITION_VERSION,
        name="On-Balance Volume",
        description="Cumulative volume signed by close-price direction.",
        category="volume",
        required_inputs=("close", "volume"),
        parameters=(),
        outputs=_value("On-balance volume."),
        default_lookback=0,
        backend="ta-lib",
        variant="window-seeded",
    ),
)

DEFINITIONS_BY_ID = {definition.id: definition for definition in INDICATOR_DEFINITIONS}
