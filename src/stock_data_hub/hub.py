"""Async facade for routing, caching, retries, and source-health tracking."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from math import isfinite
from threading import Lock
from time import perf_counter
from typing import TypeVar

from stock_data_hub._workers import BoundedWorkers, WorkerBusy
from stock_data_hub.cache import InMemoryResultCache
from stock_data_hub.errors import (
    AllSourcesFailed,
    DataHubError,
    DataValidationError,
    NoProviderConfigured,
    ProviderUnavailable,
    ResourceBusy,
    UnsupportedCapability,
)
from stock_data_hub.health import ProviderHealth, SourceHealthRegistry
from stock_data_hub.models import (
    Adjust,
    AShareBar,
    AssetType,
    BarDataQuality,
    Capability,
    CompanyProfile,
    DailyClose,
    DataResult,
    Disclosure,
    FetchMetadata,
    Instrument,
    Interval,
    Market,
    Quote,
    RestrictedRelease,
    Snapshot,
    SourcePayload,
    TradeCalendar,
)
from stock_data_hub.normalization import (
    BEIJING_TZ,
    asset_type_from_symbol,
    market_from_symbol,
    normalize_a_share_symbol,
    normalize_symbol,
    to_beijing,
)
from stock_data_hub.providers.base import MarketDataProvider


@dataclass(frozen=True)
class CachePolicy:
    ttl: timedelta
    stale_ttl: timedelta


@dataclass(frozen=True)
class RouteKey:
    capability: Capability
    market: Market | None = None
    asset_type: AssetType | None = None
    interval: Interval | None = None
    adjust: Adjust | None = None


DEFAULT_CACHE_POLICIES: Mapping[Capability, CachePolicy] = {
    Capability.QUOTE: CachePolicy(timedelta(seconds=3), timedelta(seconds=30)),
    Capability.SNAPSHOT: CachePolicy(timedelta(seconds=15), timedelta(minutes=2)),
    Capability.BARS: CachePolicy(timedelta(seconds=60), timedelta(minutes=10)),
    Capability.INSTRUMENTS: CachePolicy(timedelta(hours=6), timedelta(days=1)),
    Capability.TRADE_CALENDAR: CachePolicy(timedelta(hours=6), timedelta(days=2)),
    Capability.COMPANY_PROFILE: CachePolicy(timedelta(days=1), timedelta(days=14)),
    Capability.RESTRICTED_RELEASES: CachePolicy(timedelta(minutes=30), timedelta(hours=12)),
    Capability.DISCLOSURES: CachePolicy(timedelta(minutes=5), timedelta(hours=2)),
    Capability.US_DAILY_CLOSES: CachePolicy(timedelta(minutes=30), timedelta(hours=12)),
}

T = TypeVar("T")


class _EmptyBarsResult(DataValidationError):
    def __init__(self, payload: SourcePayload[list[AShareBar]]) -> None:
        self.payload = payload
        super().__init__("provider returned no bars for an active market-date window")


class DataHub:
    """The only surface a host application needs for market data.

    Providers are synchronous by design because most financial SDKs are
    synchronous. The facade runs them outside the event loop, so a FastAPI,
    scheduler, CLI, or notebook can all use the same independently maintained
    package.
    """

    def __init__(
        self,
        providers: Iterable[MarketDataProvider] = (),
        *,
        source_priority: Mapping[Capability | str | RouteKey, Sequence[str]] | None = None,
        cache: InMemoryResultCache | None = None,
        cache_policies: Mapping[Capability | str, CachePolicy] | None = None,
        attempts_per_provider: int = 2,
        retry_delay_seconds: float = 0.15,
        failure_threshold: int = 2,
        circuit_cooldown: timedelta = timedelta(seconds=30),
        provider_timeout_seconds: float | None = 20.0,
        provider_timeout_overrides: Mapping[Capability | str, float | None] | None = None,
        max_concurrent_requests: int = 8,
        request_timeout_seconds: float = 240.0,
        local_timeout_seconds: float = 90.0,
        peer_timeout_seconds: float = 95.0,
        max_inflight_requests: int = 512,
    ) -> None:
        if attempts_per_provider < 1:
            raise ValueError("attempts_per_provider must be at least one")
        if retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds cannot be negative")
        if provider_timeout_seconds is not None and (
            not isfinite(provider_timeout_seconds) or provider_timeout_seconds <= 0
        ):
            raise ValueError("provider_timeout_seconds must be positive or None")
        normalized_timeout_overrides: dict[Capability, float | None] = {}
        for raw_capability, timeout_seconds in (provider_timeout_overrides or {}).items():
            capability = Capability(raw_capability)
            if timeout_seconds is not None and (
                not isfinite(timeout_seconds) or timeout_seconds <= 0
            ):
                raise ValueError("provider timeout overrides must be positive or None")
            normalized_timeout_overrides[capability] = timeout_seconds
        if max_concurrent_requests < 1:
            raise ValueError("max_concurrent_requests must be at least one")
        if any(
            not isfinite(value) or value <= 0
            for value in (request_timeout_seconds, local_timeout_seconds, peer_timeout_seconds)
        ):
            raise ValueError("Request and peer deadlines must be positive")
        if max_inflight_requests < 1:
            raise ValueError("max_inflight_requests must be positive")

        self._providers: dict[str, MarketDataProvider] = {}
        self._source_priority = _normalize_priority(source_priority)
        self._cache_policies = _merge_cache_policies(cache_policies)
        self._cache = cache or InMemoryResultCache()
        self._attempts_per_provider = attempts_per_provider
        self._retry_delay_seconds = retry_delay_seconds
        self._provider_timeout_seconds = provider_timeout_seconds
        self._provider_timeout_overrides = normalized_timeout_overrides
        self._worker_capacity = max_concurrent_requests
        self._workers: dict[str, BoundedWorkers] = {}
        self._request_timeout_seconds = request_timeout_seconds
        self._local_timeout_seconds = local_timeout_seconds
        self._peer_timeout_seconds = peer_timeout_seconds
        self._max_inflight_requests = max_inflight_requests
        self._inflight: dict[tuple[int, str], asyncio.Task] = {}
        self._inflight_lock = Lock()
        self._metrics_lock = Lock()
        self._metrics = dict.fromkeys(
            (
                "requests",
                "cache_hits",
                "stale_returns",
                "coalesced_requests",
                "fallbacks",
                "failures",
            ),
            0,
        )
        self._health = SourceHealthRegistry(
            failure_threshold=failure_threshold,
            cooldown=circuit_cooldown,
        )
        for provider in providers:
            self.register(provider)

    def register(self, provider: MarketDataProvider) -> None:
        name = str(provider.name).strip()
        if not name:
            raise ValueError("Provider name cannot be empty")
        if name in self._providers:
            raise ValueError(f"A provider named {name!r} is already registered")
        self._providers[name] = provider
        self._workers[name] = BoundedWorkers(self._worker_capacity, name=f"data-hub:{name}")
        self._health.ensure(name)

    def clear_cache(self) -> None:
        self._cache.clear()
        for provider in self._providers.values():
            provider.clear_cache()

    def health(self) -> tuple[ProviderHealth, ...]:
        return self._health.snapshots()

    def metrics(self) -> dict[str, object]:
        with self._metrics_lock:
            counts = dict(self._metrics)
        return {**counts, "active_provider_calls": {k: v.active for k, v in self._workers.items()}}

    def _count(self, name: str) -> None:
        with self._metrics_lock:
            self._metrics[name] += 1

    async def get_bars(
        self,
        symbol: str,
        interval: Interval | str,
        start: datetime,
        end: datetime,
        adjust: Adjust | str = Adjust.QFQ,
        market: Market | str | None = None,
        asset_type: AssetType | str | None = None,
        *,
        allow_peers: bool = True,
    ) -> DataResult[list[AShareBar]]:
        normalized = normalize_symbol(symbol, market)
        normalized_market = market_from_symbol(normalized)
        normalized_interval = Interval(interval)
        normalized_adjust = Adjust(adjust)
        normalized_asset_type = asset_type_from_symbol(normalized, asset_type)
        start_bj = to_beijing(start)
        end_bj = to_beijing(end)
        if start_bj > end_bj:
            raise ValueError("start must be before end")
        return await self._resolve(
            Capability.BARS,
            _cache_key(
                Capability.BARS,
                normalized,
                normalized_interval.value,
                start_bj.isoformat(),
                end_bj.isoformat(),
                normalized_adjust.value,
                normalized_asset_type.value,
            ),
            lambda provider: _fetch_bars(
                provider,
                normalized,
                normalized_interval,
                start_bj,
                end_bj,
                normalized_adjust,
                normalized_asset_type,
            ),
            market=normalized_market,
            asset_type=normalized_asset_type,
            interval=normalized_interval,
            adjust=normalized_adjust,
            allow_peers=allow_peers,
        )

    async def get_quote(
        self,
        symbol: str,
        market: Market | str | None = None,
        asset_type: AssetType | str | None = None,
        *,
        allow_peers: bool = True,
    ) -> DataResult[Quote]:
        normalized = normalize_symbol(symbol, market)
        normalized_asset_type = asset_type_from_symbol(normalized, asset_type)
        return await self._resolve(
            Capability.QUOTE,
            _cache_key(Capability.QUOTE, normalized, normalized_asset_type.value),
            lambda provider: _fetch_quote(provider, normalized, normalized_asset_type),
            market=market_from_symbol(normalized),
            asset_type=normalized_asset_type,
            allow_peers=allow_peers,
        )

    async def get_snapshot(self, *, allow_peers: bool = True) -> DataResult[Snapshot]:
        return await self._resolve(
            Capability.SNAPSHOT,
            _cache_key(Capability.SNAPSHOT),
            lambda provider: provider.fetch_snapshot(),
            allow_peers=allow_peers,
        )

    async def get_instruments(self, *, allow_peers: bool = True) -> DataResult[list[Instrument]]:
        return await self._resolve(
            Capability.INSTRUMENTS,
            _cache_key(Capability.INSTRUMENTS),
            lambda provider: provider.fetch_instruments(),
            allow_peers=allow_peers,
        )

    async def get_trade_calendar(
        self,
        *,
        allow_peers: bool = True,
    ) -> DataResult[TradeCalendar]:
        return await self._resolve(
            Capability.TRADE_CALENDAR,
            _cache_key(Capability.TRADE_CALENDAR),
            lambda provider: provider.fetch_trade_calendar(),
            allow_peers=allow_peers,
        )

    async def get_company_profile(
        self,
        symbol: str,
        *,
        allow_peers: bool = True,
    ) -> DataResult[CompanyProfile]:
        normalized = normalize_a_share_symbol(symbol)
        return await self._resolve(
            Capability.COMPANY_PROFILE,
            _cache_key(Capability.COMPANY_PROFILE, normalized),
            lambda provider: provider.fetch_company_profile(normalized),
            allow_peers=allow_peers,
        )

    async def get_restricted_releases(
        self,
        symbol: str,
        *,
        allow_peers: bool = True,
    ) -> DataResult[list[RestrictedRelease]]:
        normalized = normalize_a_share_symbol(symbol)
        return await self._resolve(
            Capability.RESTRICTED_RELEASES,
            _cache_key(Capability.RESTRICTED_RELEASES, normalized),
            lambda provider: provider.fetch_restricted_releases(normalized),
            allow_peers=allow_peers,
        )

    async def get_disclosures(
        self,
        symbol: str,
        start: date,
        end: date,
        *,
        allow_peers: bool = True,
    ) -> DataResult[list[Disclosure]]:
        if start > end:
            raise ValueError("start must be before end")
        normalized = normalize_a_share_symbol(symbol)
        return await self._resolve(
            Capability.DISCLOSURES,
            _cache_key(Capability.DISCLOSURES, normalized, start.isoformat(), end.isoformat()),
            lambda provider: provider.fetch_disclosures(normalized, start, end),
            allow_peers=allow_peers,
        )

    async def get_us_daily_closes(
        self,
        ticker: str,
        max_rows: int = 80,
        *,
        allow_peers: bool = True,
    ) -> DataResult[list[DailyClose]]:
        normalized = normalize_symbol(ticker, Market.US).removesuffix(".US")
        if max_rows < 1:
            raise ValueError("max_rows must be positive")
        return await self._resolve(
            Capability.US_DAILY_CLOSES,
            _cache_key(Capability.US_DAILY_CLOSES, normalized, str(max_rows)),
            lambda provider: _fetch_us_daily_closes(provider, normalized, max_rows),
            allow_peers=allow_peers,
        )

    async def _resolve(
        self,
        capability: Capability,
        cache_key: str,
        invoke: Callable[[MarketDataProvider], SourcePayload[T]],
        market: Market | None = None,
        asset_type: AssetType | None = None,
        interval: Interval | None = None,
        adjust: Adjust | None = None,
        allow_peers: bool = True,
    ) -> DataResult[T]:
        self._count("requests")
        cache_key = _scoped_cache_key(cache_key, allow_peers)
        fresh = self._cache.get_fresh(cache_key)
        if fresh is not None:
            self._count("cache_hits")
            return _cached_result(fresh, stale=False)

        loop = asyncio.get_running_loop()
        inflight_key = (id(loop), cache_key)
        with self._inflight_lock:
            shared = self._inflight.get(inflight_key)
            if shared is None:
                if len(self._inflight) >= self._max_inflight_requests:
                    self._count("failures")
                    raise ResourceBusy("Too many distinct requests are already in flight")
                shared = loop.create_task(
                    self._resolve_uncached(
                        capability,
                        cache_key,
                        invoke,
                        market=market,
                        asset_type=asset_type,
                        interval=interval,
                        adjust=adjust,
                        allow_peers=allow_peers,
                    )
                )
                self._inflight[inflight_key] = shared
                shared.add_done_callback(lambda task: self._finish_inflight(inflight_key, task))
            else:
                self._count("coalesced_requests")
        try:
            # Every caller (including the first) is only a waiter. Cancelling
            # one HTTP request cannot cancel work still needed by another.
            result = await asyncio.shield(shared)
            if result.metadata.stale:
                self._count("stale_returns")
            return result
        except DataHubError:
            self._count("failures")
            raise

    def _finish_inflight(self, key: tuple[int, str], task: asyncio.Task) -> None:
        with self._inflight_lock:
            if self._inflight.get(key) is task:
                self._inflight.pop(key)
        _consume_task_exception(task)

    async def _resolve_uncached(
        self,
        capability: Capability,
        cache_key: str,
        invoke: Callable[[MarketDataProvider], SourcePayload[T]],
        *,
        market: Market | None,
        asset_type: AssetType | None,
        interval: Interval | None,
        adjust: Adjust | None,
        allow_peers: bool,
    ) -> DataResult[T]:
        started_at = perf_counter()
        deadline = started_at + self._request_timeout_seconds
        local_deadline = min(deadline, started_at + self._local_timeout_seconds)
        providers = self._providers_for(
            capability,
            market,
            asset_type,
            interval,
            adjust,
            allow_peers,
        )
        health_route = _health_route(capability, market, asset_type, interval, adjust)
        if not providers:
            stale = self._cache.get_stale(cache_key)
            if stale is not None:
                return _cached_result(stale, stale=True)
            raise NoProviderConfigured(f"No provider supports {capability.value}")

        errors: dict[str, str] = {}
        attempted: list[str] = []
        empty_candidate: tuple[MarketDataProvider, SourcePayload[T], float] | None = None
        partial_candidate: tuple[MarketDataProvider, SourcePayload[T], float] | None = None
        all_empty = True
        for provider in providers:
            provider_deadline = deadline if provider.is_peer else local_deadline
            if perf_counter() >= provider_deadline:
                errors[provider.name] = "routing deadline exhausted"
                all_empty = False
                continue
            if not self._health.can_attempt(provider.name, health_route):
                errors[provider.name] = "circuit breaker is open"
                all_empty = False
                continue

            attempted.append(provider.name)
            payload, latency_ms, error = await self._attempt_provider(
                provider,
                invoke,
                health_route,
                capability,
                provider_deadline,
            )
            if error is None and payload is not None:
                if payload.fetched_at is not None:
                    age = datetime.now(tz=BEIJING_TZ) - to_beijing(payload.fetched_at)
                    if age >= self._cache_policies[capability].ttl:
                        payload = replace(payload, stale=True)
                if payload.stale or (
                    payload.quality is not None
                    and (payload.quality.truncated or payload.quality.filtered_rows)
                ):
                    partial_candidate = partial_candidate or (provider, payload, latency_ms)
                    all_empty = False
                    continue
                if len(attempted) > 1:
                    self._count("fallbacks")
                return self._store_result(
                    capability, cache_key, provider, payload, attempted, started_at
                )

            assert error is not None
            errors[provider.name] = _short_error(error)
            if isinstance(error, _EmptyBarsResult) and payload is not None:
                empty_candidate = (provider, payload, latency_ms)
            else:
                all_empty = False

        stale = self._cache.get_stale(cache_key)
        if stale is not None:
            return _cached_result(
                stale,
                stale=True,
                warning="All live sources failed; returned the last verified cached result.",
            )
        if partial_candidate is not None:
            provider, payload, _ = partial_candidate
            payload = replace(
                payload, warnings=(*payload.warnings, "No complete fresh source was available.")
            )
            return self._store_result(
                capability, cache_key, provider, payload, attempted, started_at
            )
        if empty_candidate is not None and all_empty:
            provider, payload, _ = empty_candidate
            payload = replace(
                payload,
                warnings=(
                    *payload.warnings,
                    "All eligible live sources returned no bars for the requested window.",
                ),
            )
            return self._store_result(
                capability, cache_key, provider, payload, attempted, started_at
            )
        raise AllSourcesFailed(capability, errors)

    def _store_result(
        self,
        capability: Capability,
        cache_key: str,
        provider: MarketDataProvider,
        payload: SourcePayload[T],
        attempted: Sequence[str],
        started_at: float,
    ) -> DataResult[T]:
        received_at = datetime.now(tz=BEIJING_TZ)
        fetched_at = to_beijing(payload.fetched_at) if payload.fetched_at else received_at
        age = max((received_at - fetched_at).total_seconds(), 0.0)
        result = DataResult(
            payload.value,
            FetchMetadata(
                source=payload.source or provider.name,
                fetched_at=fetched_at,
                received_at=received_at,
                age_seconds=age,
                latency_ms=(perf_counter() - started_at) * 1000,
                attempted_sources=tuple(attempted),
                stale=payload.stale,
                cache_hit=payload.cache_hit,
                warnings=payload.warnings,
                quality=payload.quality,
            ),
        )
        if payload.quality and (payload.quality.truncated or payload.quality.filtered_rows):
            # An incomplete response must never replace a verified cached history.
            return result
        policy = self._cache_policies[capability]
        age_delta = timedelta(seconds=age)
        fresh_remaining = max(policy.ttl - age_delta, timedelta())
        if payload.stale:
            fresh_remaining = timedelta()
        total_remaining = max(policy.ttl + policy.stale_ttl - age_delta, timedelta())
        self._cache.set(
            cache_key, result, fresh_remaining, max(total_remaining - fresh_remaining, timedelta())
        )
        return result

    async def _attempt_provider(
        self,
        provider: MarketDataProvider,
        invoke: Callable[[MarketDataProvider], SourcePayload[T]],
        health_route: str,
        capability: Capability,
        deadline: float,
    ) -> tuple[SourcePayload[T] | None, float, Exception | None]:
        last_error: Exception | None = None
        total_latency_ms = 0.0
        configured_timeout = (
            self._peer_timeout_seconds
            if provider.is_peer
            else self._provider_timeout_overrides.get(capability, self._provider_timeout_seconds)
        )
        for attempt in range(self._attempts_per_provider):
            started_at = perf_counter()
            remaining = deadline - started_at
            if remaining <= 0:
                return None, total_latency_ms, ProviderUnavailable("Routing deadline exhausted")
            timeout_seconds = (
                min(configured_timeout, remaining) if configured_timeout else remaining
            )
            try:
                payload = await self._invoke_provider(provider, invoke, timeout_seconds)
                latency_ms = (perf_counter() - started_at) * 1000
                total_latency_ms += latency_ms
                if not isinstance(payload, SourcePayload):
                    raise DataValidationError(
                        f"{provider.name} returned {type(payload).__name__}, not SourcePayload"
                    )
                self._health.record_success(provider.name, health_route, total_latency_ms)
                return payload, total_latency_ms, None
            except UnsupportedCapability as exc:
                return None, total_latency_ms, exc
            except _EmptyBarsResult as exc:
                total_latency_ms += (perf_counter() - started_at) * 1000
                return exc.payload, total_latency_ms, exc  # type: ignore[return-value]
            except WorkerBusy as exc:
                self._health.record_capacity_rejection(provider.name, health_route)
                return None, total_latency_ms, exc
            except asyncio.TimeoutError:
                total_latency_ms += (perf_counter() - started_at) * 1000
                last_error = ProviderUnavailable(
                    f"{provider.name} timed out after {timeout_seconds} seconds"
                )
                self._health.record_timeout(provider.name, health_route)
                # Python cannot safely cancel a running SDK thread. Starting
                # another attempt would duplicate the same upstream request.
                break
            # Provider adapters wrap most SDK errors, but third-party adapters
            # are allowed to raise arbitrary exceptions at this isolation boundary.
            except Exception as exc:  # noqa: BLE001
                total_latency_ms += (perf_counter() - started_at) * 1000
                last_error = exc
            if attempt + 1 < self._attempts_per_provider and self._retry_delay_seconds:
                await asyncio.sleep(
                    min(
                        self._retry_delay_seconds * (attempt + 1), max(deadline - perf_counter(), 0)
                    )
                )

        assert last_error is not None
        self._health.record_failure(provider.name, health_route, last_error)
        return None, total_latency_ms, last_error

    async def _invoke_provider(
        self,
        provider: MarketDataProvider,
        invoke: Callable[[MarketDataProvider], SourcePayload[T]],
        timeout_seconds: float | None,
    ) -> SourcePayload[T]:
        return await self._workers[provider.name].run(
            lambda: invoke(provider),
            timeout=timeout_seconds,
            queue_timeout=min(0.05, timeout_seconds) if timeout_seconds else 0.05,
        )

    def _providers_for(
        self,
        capability: Capability,
        market: Market | None = None,
        asset_type: AssetType | None = None,
        interval: Interval | None = None,
        adjust: Adjust | None = None,
        allow_peers: bool = True,
    ) -> tuple[MarketDataProvider, ...]:
        supported = [
            provider
            for provider in self._providers.values()
            if allow_peers or not provider.is_peer
            if provider.supports_request(
                capability,
                market=market,
                asset_type=asset_type,
                interval=interval,
                adjust=adjust,
            )
        ]
        preferred = self._priority_for(capability, market, asset_type, interval, adjust)
        if not preferred:
            return tuple(supported)

        positions = {name: index for index, name in enumerate(preferred)}
        return tuple(
            sorted(
                supported,
                key=lambda provider: (positions.get(provider.name, len(positions)), provider.name),
            )
        )

    def _priority_for(
        self,
        capability: Capability,
        market: Market | None,
        asset_type: AssetType | None,
        interval: Interval | None,
        adjust: Adjust | None,
    ) -> tuple[str, ...]:
        candidates = (
            RouteKey(capability, market, asset_type, interval, adjust),
            RouteKey(capability, market, asset_type, interval),
            RouteKey(capability, market, asset_type),
            RouteKey(capability, market),
            RouteKey(capability),
        )
        for key in candidates:
            if key in self._source_priority:
                return self._source_priority[key]
        return ()


def _normalize_priority(
    raw: Mapping[Capability | str | RouteKey, Sequence[str]] | None,
) -> dict[RouteKey, tuple[str, ...]]:
    if not raw:
        return {}
    result: dict[RouteKey, tuple[str, ...]] = {}
    for capability, names in raw.items():
        if isinstance(capability, RouteKey):
            normalized = capability
        else:
            normalized_capability = (
                capability if isinstance(capability, Capability) else Capability(capability)
            )
            normalized = RouteKey(normalized_capability)
        result[normalized] = tuple(str(name) for name in names)
    return result


def _merge_cache_policies(
    custom: Mapping[Capability | str, CachePolicy] | None,
) -> dict[Capability, CachePolicy]:
    result = dict(DEFAULT_CACHE_POLICIES)
    if not custom:
        return result
    for capability, policy in custom.items():
        normalized = capability if isinstance(capability, Capability) else Capability(capability)
        result[normalized] = policy
    return result


def _cache_key(capability: Capability, *parts: str) -> str:
    return "|".join((capability.value, *parts))


def _scoped_cache_key(cache_key: str, allow_peers: bool) -> str:
    return cache_key if allow_peers else f"{cache_key}|scope:local-only"


def _health_route(
    capability: Capability,
    market: Market | None,
    asset_type: AssetType | None,
    interval: Interval | None,
    adjust: Adjust | None,
) -> str:
    parts = [capability.value]
    parts.extend(
        value.value for value in (market, asset_type, interval, adjust) if value is not None
    )
    return ":".join(parts)


def _cached_result(
    value: object,
    *,
    stale: bool,
    warning: str | None = None,
) -> DataResult[T]:
    if not isinstance(value, DataResult):
        raise DataValidationError("Cache contains an unexpected value")
    warnings = value.metadata.warnings
    if warning and warning not in warnings:
        warnings = (*warnings, warning)
    return DataResult(
        value=value.value,
        metadata=replace(
            value.metadata,
            cache_hit=True,
            stale=stale or value.metadata.stale,
            warnings=warnings,
            age_seconds=max(
                (
                    datetime.now(tz=BEIJING_TZ) - to_beijing(value.metadata.fetched_at)
                ).total_seconds(),
                0.0,
            ),
        ),
    )


def _short_error(error: BaseException) -> str:
    message = str(error).strip().replace("\n", " ")
    return f"{type(error).__name__}: {message}"[:500]


def _consume_task_exception(task: asyncio.Task[object]) -> None:
    if not task.cancelled():
        task.exception()


def _fetch_bars(
    provider: MarketDataProvider,
    symbol: str,
    interval: Interval,
    start: datetime,
    end: datetime,
    adjust: Adjust,
    asset_type: AssetType,
) -> SourcePayload[list[AShareBar]]:
    if provider.accepts_asset_type:
        payload = provider.fetch_bars(
            symbol,
            interval,
            start,
            end,
            adjust,
            asset_type=asset_type,
        )
    else:
        payload = provider.fetch_bars(symbol, interval, start, end, adjust)
    if not isinstance(payload, SourcePayload) or not isinstance(payload.value, list):
        raise DataValidationError("Provider bars must be a SourcePayload containing a list")
    for bar in payload.value:
        if (
            not isinstance(bar, AShareBar)
            or bar.symbol != symbol
            or bar.market != market_from_symbol(symbol)
        ):
            raise DataValidationError("Provider returned bars for a different symbol or market")
        prices = (bar.open, bar.high, bar.low, bar.close)
        if any(not value.is_finite() for value in prices):
            raise DataValidationError("Provider returned non-finite bar prices")
        if adjust is Adjust.NONE and any(value <= 0 for value in prices):
            raise DataValidationError("Provider returned a non-positive unadjusted bar price")
        if bar.low > min(bar.open, bar.close) or max(bar.open, bar.close) > bar.high:
            raise DataValidationError("Provider returned an invalid OHLC relationship")
        if bar.ts.tzinfo is None or not start <= bar.ts <= end:
            raise DataValidationError("Provider returned a bar outside the requested time window")
        if type(bar.is_closed) is not bool:
            raise DataValidationError("Provider returned a non-boolean is_closed")
        if any(
            value is not None and (not value.is_finite() or value < 0)
            for value in (bar.volume, bar.amount)
        ):
            raise DataValidationError("Provider returned an invalid volume or amount")
    if len({bar.ts for bar in payload.value}) != len(payload.value):
        raise DataValidationError("Provider returned duplicate bar timestamps")
    payload = replace(payload, value=sorted(payload.value, key=lambda bar: bar.ts))
    if payload.quality is None:
        payload = replace(
            payload,
            quality=BarDataQuality(
                start,
                end,
                payload.value[0].ts if payload.value else None,
                payload.value[-1].ts if payload.value else None,
                len(payload.value),
                len(payload.value),
                currency={Market.CN: "CNY", Market.HK: "HKD", Market.US: "USD"}[
                    market_from_symbol(symbol)
                ],
            ),
        )
    if (
        not payload.value
        and payload.quality is not None
        and (payload.quality.truncated or payload.quality.filtered_rows)
    ):
        raise DataValidationError(
            "Provider returned no usable bars from incomplete or invalid history"
        )
    if not payload.value and _bar_window_requires_data(start, end):
        raise _EmptyBarsResult(payload)
    return payload


def _fetch_quote(
    provider: MarketDataProvider,
    symbol: str,
    asset_type: AssetType,
) -> SourcePayload[Quote]:
    if provider.accepts_asset_type:
        payload = provider.fetch_quote(symbol, asset_type=asset_type)
    else:
        payload = provider.fetch_quote(symbol)
    if not isinstance(payload, SourcePayload) or not isinstance(payload.value, Quote):
        raise DataValidationError("Provider returned an invalid quote contract")
    quote = payload.value
    if quote.symbol != symbol or quote.market != market_from_symbol(symbol):
        raise DataValidationError("Provider returned a quote for a different symbol or market")
    if not quote.price.is_finite() or quote.price <= 0:
        raise DataValidationError("Provider returned an invalid quote price")
    return payload


def _fetch_us_daily_closes(
    provider: MarketDataProvider,
    ticker: str,
    max_rows: int,
) -> SourcePayload[list[DailyClose]]:
    payload = provider.fetch_us_daily_closes(ticker, max_rows)
    if not payload.value:
        raise ProviderUnavailable(f"{provider.name} returned no US daily closes for {ticker}")
    return payload


def _bar_window_requires_data(start: datetime, end: datetime) -> bool:
    """Distinguish an upstream empty response from a weekend/future query."""

    today = datetime.now(tz=BEIJING_TZ).date()
    first_day = to_beijing(start).date()
    last_day = min(to_beijing(end).date(), today)
    if first_day > last_day:
        return False
    # Any seven-day span contains a weekday. Short spans only need a bounded
    # scan, so very large historical requests remain constant-time.
    days = min((last_day - first_day).days, 6)
    return any((first_day + timedelta(days=offset)).weekday() < 5 for offset in range(days + 1))
