"""TA-Lib backend for the curated stable indicator catalog."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Mapping
from typing import Any

import numpy as np

from stock_data_hub.errors import IndicatorCalculationError
from stock_data_hub.indicators.backends.base import IndicatorBackend
from stock_data_hub.indicators.contracts import ParameterValue


class TaLibBackend(IndicatorBackend):
    name = "ta-lib"

    def __init__(self) -> None:
        self._talib = importlib.import_module("talib")
        self._abstract = importlib.import_module("talib.abstract")
        self.version = str(self._talib.__version__)
        self._computers: Mapping[str, Callable[..., Mapping[str, np.ndarray]]] = {
            "overlap.sma": self._sma,
            "overlap.ema": self._ema,
            "momentum.rsi": self._rsi,
            "trend.macd": self._macd,
            "volatility.bbands": self._bbands,
            "volatility.atr": self._atr,
            "momentum.stoch": self._stoch,
            "trend.adx": self._adx,
            "momentum.roc": self._roc,
            "volume.obv": self._obv,
        }

    def supports(self, indicator_id: str) -> bool:
        return indicator_id in self._computers

    def lookback(self, indicator_id: str, params: Mapping[str, ParameterValue]) -> int:
        functions = self._function_parameters(indicator_id, params)
        try:
            lookbacks = []
            for function_name, function_params in functions:
                function = self._abstract.Function(function_name)
                if function_params:
                    function.set_parameters(function_params)
                lookbacks.append(int(function.lookback))
            return max(lookbacks)
        except Exception as exc:
            raise IndicatorCalculationError(
                f"TA-Lib could not determine lookback for {indicator_id}"
            ) from exc

    def compute(
        self,
        indicator_id: str,
        params: Mapping[str, ParameterValue],
        inputs: Mapping[str, np.ndarray],
    ) -> Mapping[str, np.ndarray]:
        computer = self._computers.get(indicator_id)
        if computer is None:
            raise IndicatorCalculationError(f"TA-Lib does not implement {indicator_id}")
        normalized = {
            name: np.ascontiguousarray(values, dtype=np.float64) for name, values in inputs.items()
        }
        try:
            return computer(params, normalized)
        except IndicatorCalculationError:
            raise
        except Exception as exc:
            raise IndicatorCalculationError(f"TA-Lib failed to compute {indicator_id}") from exc

    def _function_parameters(
        self,
        indicator_id: str,
        params: Mapping[str, ParameterValue],
    ) -> tuple[tuple[str, Mapping[str, Any]], ...]:
        period = int(params.get("period", 0))
        if indicator_id == "overlap.sma":
            return (("SMA", {"timeperiod": period}),)
        if indicator_id == "overlap.ema":
            return (("EMA", {"timeperiod": period}),)
        if indicator_id == "momentum.rsi":
            return (("RSI", {"timeperiod": period}),)
        if indicator_id == "trend.macd":
            return (("MACD", _macd_params(params)),)
        if indicator_id == "volatility.bbands":
            deviation = float(params["standard_deviations"])
            return (
                (
                    "BBANDS",
                    {
                        "timeperiod": period,
                        "nbdevup": deviation,
                        "nbdevdn": deviation,
                        "matype": 0,
                    },
                ),
            )
        if indicator_id == "volatility.atr":
            return (("ATR", {"timeperiod": period}),)
        if indicator_id == "momentum.stoch":
            return (("STOCH", _stoch_params(params)),)
        if indicator_id == "trend.adx":
            values = {"timeperiod": period}
            return (("ADX", values), ("PLUS_DI", values), ("MINUS_DI", values))
        if indicator_id == "momentum.roc":
            return (("ROC", {"timeperiod": period}),)
        if indicator_id == "volume.obv":
            return (("OBV", {}),)
        raise IndicatorCalculationError(f"TA-Lib does not implement {indicator_id}")

    def _sma(self, params: Mapping[str, ParameterValue], values: Mapping[str, np.ndarray]):
        return {"value": self._talib.SMA(values["close"], timeperiod=int(params["period"]))}

    def _ema(self, params: Mapping[str, ParameterValue], values: Mapping[str, np.ndarray]):
        return {"value": self._talib.EMA(values["close"], timeperiod=int(params["period"]))}

    def _rsi(self, params: Mapping[str, ParameterValue], values: Mapping[str, np.ndarray]):
        return {"value": self._talib.RSI(values["close"], timeperiod=int(params["period"]))}

    def _macd(self, params: Mapping[str, ParameterValue], values: Mapping[str, np.ndarray]):
        macd, signal, histogram = self._talib.MACD(values["close"], **_macd_params(params))
        return {"macd": macd, "signal": signal, "histogram": histogram}

    def _bbands(self, params: Mapping[str, ParameterValue], values: Mapping[str, np.ndarray]):
        deviation = float(params["standard_deviations"])
        upper, middle, lower = self._talib.BBANDS(
            values["close"],
            timeperiod=int(params["period"]),
            nbdevup=deviation,
            nbdevdn=deviation,
            matype=0,
        )
        return {"upper": upper, "middle": middle, "lower": lower}

    def _atr(self, params: Mapping[str, ParameterValue], values: Mapping[str, np.ndarray]):
        result = self._talib.ATR(
            values["high"],
            values["low"],
            values["close"],
            timeperiod=int(params["period"]),
        )
        return {"value": result}

    def _stoch(self, params: Mapping[str, ParameterValue], values: Mapping[str, np.ndarray]):
        k, d = self._talib.STOCH(
            values["high"], values["low"], values["close"], **_stoch_params(params)
        )
        return {"k": k, "d": d}

    def _adx(self, params: Mapping[str, ParameterValue], values: Mapping[str, np.ndarray]):
        period = int(params["period"])
        arguments = (values["high"], values["low"], values["close"])
        return {
            "adx": self._talib.ADX(*arguments, timeperiod=period),
            "plus_di": self._talib.PLUS_DI(*arguments, timeperiod=period),
            "minus_di": self._talib.MINUS_DI(*arguments, timeperiod=period),
        }

    def _roc(self, params: Mapping[str, ParameterValue], values: Mapping[str, np.ndarray]):
        return {"value": self._talib.ROC(values["close"], timeperiod=int(params["period"]))}

    def _obv(self, _: Mapping[str, ParameterValue], values: Mapping[str, np.ndarray]):
        return {"value": self._talib.OBV(values["close"], values["volume"])}


def _macd_params(params: Mapping[str, ParameterValue]) -> dict[str, int]:
    return {
        "fastperiod": int(params["fast_period"]),
        "slowperiod": int(params["slow_period"]),
        "signalperiod": int(params["signal_period"]),
    }


def _stoch_params(params: Mapping[str, ParameterValue]) -> dict[str, int]:
    return {
        "fastk_period": int(params["fast_k_period"]),
        "slowk_period": int(params["slow_k_period"]),
        "slowk_matype": 0,
        "slowd_period": int(params["slow_d_period"]),
        "slowd_matype": 0,
    }
