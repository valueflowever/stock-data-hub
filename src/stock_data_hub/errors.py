"""Errors with enough context for callers to make a safe degradation decision."""

from __future__ import annotations

from collections.abc import Mapping

from stock_data_hub.models import Capability


class DataHubError(RuntimeError):
    """Base error for the standalone data package."""


class ProviderError(DataHubError):
    """A provider could not return a trustworthy response."""


class ProviderUnavailable(ProviderError):
    """A provider or its upstream is temporarily unavailable."""


class UnsupportedCapability(ProviderError):
    """The selected provider cannot serve this capability."""


class DataValidationError(ProviderError):
    """A provider returned a response that does not meet the common contract."""


class InvalidRequest(DataValidationError):
    """The caller supplied a symbol or routing value that cannot be normalized."""


class IndicatorError(DataHubError):
    """Base class for deterministic indicator-computation failures."""


class IndicatorNotFound(IndicatorError):
    """The requested public indicator ID is not registered."""


class InvalidIndicatorRequest(InvalidRequest, IndicatorError):
    """An indicator specification or computation option is invalid."""


class IndicatorBackendUnavailable(IndicatorError):
    """The optional backend for a registered indicator is not installed."""


class IndicatorCalculationError(IndicatorError):
    """A registered backend could not compute a valid result."""


class NoProviderConfigured(DataHubError):
    """No registered provider supports the requested capability."""


class AllSourcesFailed(DataHubError):
    """Every eligible source failed and no usable stale result existed."""

    def __init__(self, capability: Capability, errors: Mapping[str, str]) -> None:
        self.capability = capability
        self.errors = dict(errors)
        detail = "; ".join(f"{source}: {reason}" for source, reason in self.errors.items())
        super().__init__(
            f"All sources failed for {capability.value}: {detail or 'no eligible source'}"
        )
