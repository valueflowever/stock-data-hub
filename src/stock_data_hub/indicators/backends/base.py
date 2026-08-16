"""Backend extension contract for indicator calculations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping

import numpy as np

from stock_data_hub.indicators.contracts import ParameterValue


class IndicatorBackend(ABC):
    name: str
    version: str

    @abstractmethod
    def supports(self, indicator_id: str) -> bool:
        """Return whether this backend implements a stable public indicator ID."""

    @abstractmethod
    def lookback(self, indicator_id: str, params: Mapping[str, ParameterValue]) -> int:
        """Return the exact number of leading observations without an output."""

    @abstractmethod
    def compute(
        self,
        indicator_id: str,
        params: Mapping[str, ParameterValue],
        inputs: Mapping[str, np.ndarray],
    ) -> Mapping[str, np.ndarray]:
        """Compute named outputs aligned one-to-one with input observations."""
