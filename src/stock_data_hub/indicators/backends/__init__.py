"""Optional calculation backends for the stable indicator registry."""

from stock_data_hub.indicators.backends.base import IndicatorBackend
from stock_data_hub.indicators.backends.talib import TaLibBackend

__all__ = ["IndicatorBackend", "TaLibBackend"]
