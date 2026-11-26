"""Async facade for routing, caching, retries, and source-health tracking."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from math import isfinite
from threading import Lock
from time import perf_counter
from typing import TypeVar

from stock_data_hub._workers import BoundedWorkers, WorkerBusy
from stock_data_hub.cache import InMemoryResultCache, ResultCache
from stock_data_hub.errors import (
    AllSourcesFailed,
    DataHubError,
    DataValidationError,
    InvalidRequest,
    NoProviderConfigured,
    ProviderUnavailable,
    ResourceBusy,
    SymbolDataUnavailable,
    UnsupportedCapability,
)
from stock_data_hub.health import ProviderHealth, SourceHealthRegistry
from stock_data_hub.models import (
    Adjust,
    AdjustFactor,
    AShareBar,
    AssetType,
    BarDataQuality,
    BatchQuoteError,
    BatchQuoteItem,
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
    is_bar_closed,
    market_from_symbol,
    normalize_a_share_symbol,
    normalize_asset_type,
    normalize_market,
    normalize_symbol,
    to_beijing,
)
from stock_data_hub.providers.base import MarketDataProvider
from stock_data_hub.trading_sessions import (
    latest_active_session_date,
    period_anchor_date,
    period_end,
    window_has_trading_session,
)

logger = logging.getLogger("stock_data_hub")


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
    Capability.ADJUST_FACTORS: CachePolicy(timedelta(hours=12), timedelta(days=7)),
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
        cache: ResultCache | None = None,
        cache_policies: Mapping[Capability | str, CachePolicy] | None = None,
        attempts_per_provider: int = 2,
        retry_delay_seconds: float = 0.15,
        failure_threshold: int = 2,
        circuit_cooldown: timedelta = timedelta(seconds=30),
        provider_timeout_seconds: float | None = 20.0,
        provider_timeout_overrides: Mapping[Capability | str, float | None] | None = None,
        provider_queue_timeout_seconds: float = 2.0,
        max_concurrent_requests: int = 8,
        provider_rate_limits: Mapping[str, float] | None = None,
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
        if not isfinite(provider_queue_timeout_seconds) or provider_queue_timeout_seconds < 0:
            raise ValueError("provider_queue_timeout_seconds must be finite and nonnegative")
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

        normalized_rate_limits: dict[str, float] = {}
        for raw_provider, rps in (provider_rate_limits or {}).items():
            if not isfinite(rps) or rps <= 0:
                raise ValueError("provider_rate_limits values must be finite and positive")
            normalized_rate_limits[str(raw_provider).strip()] = float(rps)

        self._providers: dict[str, MarketDataProvider] = {}
        self._source_priority = _normalize_priority(source_priority)
        self._cache_policies = _merge_cache_policies(cache_policies)
        self._cache = cache or InMemoryResultCache()
        self._attempts_per_provider = attempts_per_provider
        self._retry_delay_seconds = retry_delay_seconds
        self._provider_timeout_seconds = provider_timeout_seconds
        self._provider_timeout_overrides = normalized_timeout_overrides
        self._provider_queue_timeout_seconds = provider_queue_timeout_seconds
        self._worker_capacity = max_concurrent_requests
        self._provider_rate_limits = normalized_rate_limits
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
        self._workers[name] = BoundedWorkers(
            self._worker_capacity,
            name=f"data-hub:{name}",
            max_rps=self._provider_rate_limits.get(name),
        )
        self._health.ensure(name)

    def clear_cache(self) -> None:
        self._cache.clear()
        for provider in self._providers.values():
            provider.clear_cache()

    def close(self) -> None:
        close_cache = getattr(self._cache, "close", None)
        if callable(close_cache):
            close_cache()

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
        start_bj, end_bj = normalize_bar_window(
            start, end, normalized_interval, market=normalized_market
        )
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
            bar_end=end_bj,
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

    async def get_quotes(
        self,
        symbols: Sequence[str],
        market: Market | str | None = None,
        asset_type: AssetType | str | None = None,
        *,
        allow_peers: bool = True,
    ) -> list[BatchQuoteItem]:
        """Fetch up to 100 quotes; preserve individual failures and routing metadata."""
        if isinstance(symbols, str) or not 1 <= len(symbols) <= 100:
            raise ValueError("Provide between 1 and 100 symbols")
        normalized_market = normalize_market(market)
        normalized_asset = normalize_asset_type(asset_type)
        entries: dict[str, BatchQuoteItem | None] = {}
        for symbol in symbols:
            try:
                entries.setdefault(normalize_symbol(symbol, normalized_market), None)
            except DataHubError as exc:
                entries.setdefault(
                    symbol,
                    BatchQuoteItem(symbol, error=BatchQuoteError(type(exc).__name__, str(exc))),
                )

        valid_symbols = [symbol for symbol, item in entries.items() if item is None]
        if len(valid_symbols) > 1:
            await self._prefetch_batch_quotes(
                valid_symbols,
                asset_type=normalized_asset,
                allow_peers=allow_peers,
            )

        gate = asyncio.Semaphore(8)

        async def fetch(symbol: str) -> BatchQuoteItem:
            error_item = entries[symbol]
            if error_item is not None:
                return error_item
            async with gate:
                try:
                    result = await self.get_quote(
                        symbol, asset_type=normalized_asset, allow_peers=allow_peers
                    )
                    return BatchQuoteItem(symbol, result.value, result.metadata)
                except DataHubError as exc:
                    return BatchQuoteItem(
                        symbol, error=BatchQuoteError(type(exc).__name__, str(exc))
                    )

        return list(await asyncio.gather(*(fetch(symbol) for symbol in entries)))

    async def _prefetch_batch_quotes(
        self,
        symbols: Sequence[str],
        *,
        asset_type: AssetType | None,
        allow_peers: bool,
    ) -> None:
        grouped: dict[tuple[Market, AssetType], list[str]] = {}
        for symbol in symbols:
            resolved_asset = asset_type_from_symbol(symbol, asset_type)
            raw_key = _cache_key(Capability.QUOTE, symbol, resolved_asset.value)
            if self._cache_get_fresh(raw_key, allow_peers=allow_peers) is not None:
                continue
            grouped.setdefault((market_from_symbol(symbol), resolved_asset), []).append(symbol)

        tasks = []
        for (group_market, group_asset), group_symbols in grouped.items():
            if len(group_symbols) < 2:
                continue
            route = _health_route(Capability.QUOTE, group_market, group_asset, None, None)
            providers = [
                provider
                for provider in self._providers_for(
                    Capability.QUOTE,
                    market=group_market,
                    asset_type=group_asset,
                    allow_peers=allow_peers,
                )
                if provider.supports_batch_quotes
                and self._health.can_attempt(provider.name, route)
            ]
            if not providers:
                continue

            async def warm_group(
                candidate_providers: list[MarketDataProvider] = providers,
                target_symbols: list[str] = group_symbols,
                target_asset: AssetType = group_asset,
            ) -> None:
                for target_provider in candidate_providers:
                    timeout_seconds = (
                        self._peer_timeout_seconds
                        if target_provider.is_peer
                        else self._provider_timeout_overrides.get(
                            Capability.QUOTE, self._provider_timeout_seconds
                        )
                    )
                    try:
                        await self._workers[target_provider.name].run(
                            lambda p=target_provider: p.prefetch_quotes(
                                target_symbols, asset_type=target_asset
                            ),
                            timeout=timeout_seconds,
                            queue_timeout=self._queue_timeout(timeout_seconds),
                        )
                        return
                    except Exception as exc:  # noqa: BLE001 - try next batch-capable fallback
                        logger.debug(
                            "Batch quote prefetch skipped for %s: %s",
                            target_provider.name,
                            _short_error(exc),
                        )

            tasks.append(warm_group())
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

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
        adjust: Adjust | str = Adjust.NONE,
        allow_peers: bool = True,
    ) -> DataResult[list[DailyClose]]:
        normalized = normalize_symbol(ticker, Market.US).removesuffix(".US")
        normalized_adjust = Adjust(adjust)
        if max_rows < 1:
            raise ValueError("max_rows must be positive")
        return await self._resolve(
            Capability.US_DAILY_CLOSES,
            _cache_key(
                Capability.US_DAILY_CLOSES, normalized, str(max_rows), normalized_adjust.value
            ),
            lambda provider: _fetch_us_daily_closes(
                provider, normalized, max_rows, normalized_adjust
            ),
            market=Market.US,
            adjust=normalized_adjust,
            allow_peers=allow_peers,
        )

    async def get_adjust_factors(
        self,
        symbol: str,
        start: date | None = None,
        end: date | None = None,
        market: Market | str | None = None,
        asset_type: AssetType | str | None = None,
        *,
        allow_peers: bool = True,
    ) -> DataResult[list[AdjustFactor]]:
        if start is not None and end is not None and start > end:
            raise ValueError("start must be before end")
        normalized = normalize_symbol(symbol, market)
        normalized_market = market_from_symbol(normalized)
        normalized_asset = asset_type_from_symbol(normalized, asset_type)
        return await self._resolve(
            Capability.ADJUST_FACTORS,
            _cache_key(
                Capability.ADJUST_FACTORS,
                normalized,
                start.isoformat() if start is not None else "",
                end.isoformat() if end is not None else "",
                normalized_asset.value,
            ),
            lambda provider: _fetch_adjust_factors(
                provider, normalized, start, end, normalized_asset
            ),
            market=normalized_market,
            asset_type=normalized_asset,
            allow_peers=allow_peers,
        )

    def _cache_get_fresh(
        self,
        raw_cache_key: str,
        *,
        allow_peers: bool,
        interval: Interval | None = None,
    ) -> object | None:
        candidate = self._cache.get_fresh(_local_cache_key(raw_cache_key))
        if candidate is None and allow_peers:
            candidate = self._cache.get_fresh(_peer_cache_key(raw_cache_key))
        if candidate is not None and _has_newly_closed_bar(candidate, interval):
            return None
        return candidate

    def _cache_get_stale(self, raw_cache_key: str, *, allow_peers: bool) -> object | None:
        candidate = self._cache.get_stale(_local_cache_key(raw_cache_key))
        if candidate is None and allow_peers:
            candidate = self._cache.get_stale(_peer_cache_key(raw_cache_key))
        return candidate

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
        bar_end: datetime | None = None,
    ) -> DataResult[T]:
        self._count("requests")
        raw_cache_key = cache_key
        scoped_cache_key = _scoped_cache_key(raw_cache_key, allow_peers)
        fresh = self._cache_get_fresh(raw_cache_key, allow_peers=allow_peers, interval=interval)
        if fresh is not None:
            self._count("cache_hits")
            return _cached_result(fresh, stale=False)

        loop = asyncio.get_running_loop()
        inflight_key = (id(loop), scoped_cache_key)
        with self._inflight_lock:
            shared = self._inflight.get(inflight_key)
            if shared is None:
                if len(self._inflight) >= self._max_inflight_requests:
                    self._count("failures")
                    raise ResourceBusy("Too many distinct requests are already in flight")
                shared = loop.create_task(
                    self._resolve_uncached(
                        capability,
                        raw_cache_key,
                        scoped_cache_key,
                        invoke,
                        market=market,
                        asset_type=asset_type,
                        interval=interval,
                        adjust=adjust,
                        allow_peers=allow_peers,
                        bar_end=bar_end,
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
            return _clone_result(result)
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
        raw_cache_key: str,
        cache_key: str,
        invoke: Callable[[MarketDataProvider], SourcePayload[T]],
        *,
        market: Market | None,
        asset_type: AssetType | None,
        interval: Interval | None,
        adjust: Adjust | None,
        allow_peers: bool,
        bar_end: datetime | None,
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
            stale = self._cache_get_stale(raw_cache_key, allow_peers=allow_peers)
            if stale is not None:
                return _cached_result(stale, stale=True)
            raise NoProviderConfigured(f"No provider supports {capability.value}")

        active_session = (
            latest_active_session_date(market)
            if (
                capability is Capability.BARS
                and interval in {Interval.DAY_1, Interval.WEEK_1, Interval.MONTH_1}
                and market is not None
                and bar_end is not None
            )
            else None
        )
        errors: dict[str, str] = {}
        attempted: list[str] = []
        empty_candidate: tuple[MarketDataProvider, SourcePayload[T], float] | None = None
        partial_candidate: tuple[MarketDataProvider, SourcePayload[T], float, bool] | None = None
        all_empty = True
        for provider in providers:
            provider_deadline = deadline if provider.is_peer else local_deadline
            if perf_counter() >= provider_deadline:
                errors[provider.name] = "routing deadline exhausted"
                all_empty = False
                continue
            allowed, probe_id = self._health.try_acquire_attempt(provider.name, health_route)
            if not allowed:
                errors[provider.name] = "circuit breaker is open"
                all_empty = False
                continue

            attempted.append(provider.name)
            try:
                payload, latency_ms, error = await self._attempt_provider(
                    provider,
                    invoke,
                    health_route,
                    capability,
                    provider_deadline,
                    probe_id=probe_id,
                )
            finally:
                if probe_id is not None:
                    self._health.finish_probe(provider.name, health_route, probe_id)

            if error is None and payload is not None:
                if payload.fetched_at is not None:
                    age = datetime.now(tz=BEIJING_TZ) - to_beijing(payload.fetched_at)
                    if age >= self._cache_policies[capability].ttl:
                        payload = replace(payload, stale=True)
                has_quality_issue = payload.quality is not None and (
                    payload.quality.truncated or payload.quality.filtered_rows > 0
                )
                is_eod_lag = (
                    active_session is not None
                    and interval is not None
                    and bar_end is not None
                    and not provider.supports_intraday_daily_bar
                    and period_end(bar_end.date(), interval) >= active_session
                    and isinstance(payload.value, list)
                    and bool(payload.value)
                    and isinstance(payload.value[-1], AShareBar)
                    and period_end(payload.value[-1].ts.date(), interval) < active_session
                )
                if payload.stale or has_quality_issue or is_eod_lag:
                    candidate_payload = payload
                    if is_eod_lag:
                        warning = "EOD-only source may be missing the latest active trading session."
                        if warning not in candidate_payload.warnings:
                            candidate_payload = replace(
                                candidate_payload,
                                warnings=(*candidate_payload.warnings, warning),
                            )
                    candidate = (provider, candidate_payload, latency_ms, is_eod_lag)
                    partial_candidate = _pick_better_partial(partial_candidate, candidate)
                    all_empty = False
                    logger.info(
                        "Deferring partial/stale/EOD result from %s on %s; trying fallback",
                        provider.name,
                        health_route,
                    )
                    continue
                if len(attempted) > 1:
                    self._count("fallbacks")
                    logger.info(
                        "Fallback succeeded via %s on %s after %s",
                        provider.name,
                        health_route,
                        ",".join(attempted[:-1]),
                    )
                return self._store_result(
                    capability, raw_cache_key, cache_key, provider, payload, attempted, started_at
                )

            assert error is not None
            errors[provider.name] = _short_error(error)
            if isinstance(error, _EmptyBarsResult) and payload is not None:
                empty_candidate = (provider, payload, latency_ms)
            else:
                all_empty = False
                if not isinstance(error, UnsupportedCapability):
                    logger.warning(
                        "Provider %s failed on %s: %s",
                        provider.name,
                        health_route,
                        errors[provider.name],
                    )

        stale = self._cache_get_stale(raw_cache_key, allow_peers=allow_peers)
        if stale is not None and _prefer_stale_over_partial(stale, partial_candidate):
            return _cached_result(
                stale,
                stale=True,
                warning="All live sources failed; returned the last verified cached result.",
            )
        if partial_candidate is not None:
            provider, payload, _, is_eod_lag = partial_candidate
            if len(attempted) > 1 and attempted[0] != provider.name:
                self._count("fallbacks")
            payload = replace(
                payload, warnings=(*payload.warnings, "No complete fresh source was available.")
            )
            return self._store_result(
                capability,
                raw_cache_key,
                cache_key,
                provider,
                payload,
                attempted,
                started_at,
                is_eod_lag=is_eod_lag,
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
                capability, raw_cache_key, cache_key, provider, payload, attempted, started_at
            )
        raise AllSourcesFailed(capability, errors)

    def _store_result(
        self,
        capability: Capability,
        raw_cache_key: str,
        cache_key: str,
        provider: MarketDataProvider,
        payload: SourcePayload[T],
        attempted: Sequence[str],
        started_at: float,
        *,
        is_eod_lag: bool = False,
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
            return _clone_result(result)
        policy = self._cache_policies[capability]
        age_delta = timedelta(seconds=age)
        fresh_remaining = max(policy.ttl - age_delta, timedelta())
        if payload.stale or is_eod_lag:
            # Stale or EOD-lagged fallback data is kept only for offline stale
            # recovery and must never block live providers on subsequent calls.
            fresh_remaining = timedelta()
        elif (
            capability is Capability.BARS
            and isinstance(payload.value, list)
            and any(isinstance(bar, AShareBar) and not bar.is_closed for bar in payload.value)
        ):
            # Developing intraday/current-session bars must never stay cached as
            # fresh for 15 minutes across the session close.
            fresh_remaining = min(fresh_remaining, timedelta(seconds=15))
        total_remaining = max(policy.ttl + policy.stale_ttl - age_delta, timedelta())
        stale_remaining = max(total_remaining - fresh_remaining, timedelta())
        target_key = (
            _peer_cache_key(raw_cache_key) if provider.is_peer else _local_cache_key(raw_cache_key)
        )
        self._cache.set(target_key, _clone_result(result), fresh_remaining, stale_remaining)
        return _clone_result(result)

    async def _attempt_provider(
        self,
        provider: MarketDataProvider,
        invoke: Callable[[MarketDataProvider], SourcePayload[T]],
        health_route: str,
        capability: Capability,
        deadline: float,
        *,
        probe_id: int | None = None,
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
            except SymbolDataUnavailable as exc:
                total_latency_ms += (perf_counter() - started_at) * 1000
                self._health.record_transport_alive(
                    provider.name, health_route, probe_id=probe_id
                )
                return None, total_latency_ms, exc
            except InvalidRequest as exc:
                total_latency_ms += (perf_counter() - started_at) * 1000
                return None, total_latency_ms, exc
            except _EmptyBarsResult as exc:
                total_latency_ms += (perf_counter() - started_at) * 1000
                self._health.record_transport_alive(
                    provider.name, health_route, probe_id=probe_id
                )
                return exc.payload, total_latency_ms, exc  # type: ignore[return-value]
            except WorkerBusy as exc:
                self._health.record_capacity_rejection(
                    provider.name, health_route, probe_id=probe_id
                )
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
                base_delay = self._retry_delay_seconds * (2**attempt)
                jitter = random.uniform(0.0, base_delay * 0.25)
                await asyncio.sleep(min(base_delay + jitter, max(deadline - perf_counter(), 0)))

        assert last_error is not None
        self._health.record_failure(provider.name, health_route, last_error, probe_id=probe_id)
        return None, total_latency_ms, last_error

    def _queue_timeout(self, timeout_seconds: float | None) -> float:
        if timeout_seconds is None:
            return self._provider_queue_timeout_seconds
        return min(self._provider_queue_timeout_seconds, timeout_seconds * 0.25)

    async def _invoke_provider(
        self,
        provider: MarketDataProvider,
        invoke: Callable[[MarketDataProvider], SourcePayload[T]],
        timeout_seconds: float | None,
    ) -> SourcePayload[T]:
        return await self._workers[provider.name].run(
            lambda: invoke(provider),
            timeout=timeout_seconds,
            queue_timeout=self._queue_timeout(timeout_seconds),
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


def _local_cache_key(raw_cache_key: str) -> str:
    return f"{raw_cache_key}|scope:local-only"


def _peer_cache_key(raw_cache_key: str) -> str:
    return f"{raw_cache_key}|scope:peer"


def _has_newly_closed_bar(cached: object, interval: Interval | None) -> bool:
    if interval is None or not isinstance(cached, DataResult) or not isinstance(cached.value, list):
        return False
    now = datetime.now(tz=BEIJING_TZ)
    for item in cached.value:
        if (
            isinstance(item, AShareBar)
            and not item.is_closed
            and is_bar_closed(item.ts, interval, now=now, market=item.market)
        ):
            return True
    return False


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


def _clone_result(result: DataResult[T]) -> DataResult[T]:
    if isinstance(result.value, list):
        return DataResult(value=list(result.value), metadata=result.metadata)  # type: ignore[arg-type]
    return result


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
    cloned_value = list(value.value) if isinstance(value.value, list) else value.value
    return DataResult(
        value=cloned_value,
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
    if not payload.value and _bar_window_requires_data(
        start, end, market_from_symbol(symbol), interval
    ):
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
    adjust: Adjust = Adjust.NONE,
) -> SourcePayload[list[DailyClose]]:
    if provider.accepts_us_close_adjust:
        payload = provider.fetch_us_daily_closes(ticker, max_rows, adjust=adjust)
    elif adjust == Adjust.NONE:
        payload = provider.fetch_us_daily_closes(ticker, max_rows)
    else:
        raise UnsupportedCapability(f"{provider.name} does not support adjusted US daily closes")
    if not payload.value:
        raise ProviderUnavailable(f"{provider.name} returned no US daily closes for {ticker}")
    return payload


def _fetch_adjust_factors(
    provider: MarketDataProvider,
    symbol: str,
    start: date | None,
    end: date | None,
    asset_type: AssetType = AssetType.STOCK,
) -> SourcePayload[list[AdjustFactor]]:
    if asset_type != AssetType.STOCK:
        raise UnsupportedCapability(
            f"Adjustment factors are only supported for stock symbols, not {asset_type.value} ({symbol})"
        )
    payload = provider.fetch_adjust_factors(symbol, start, end)
    if not isinstance(payload, SourcePayload) or not isinstance(payload.value, list):
        raise DataValidationError("Provider adjust factors must be a SourcePayload containing a list")
    expected_market = market_from_symbol(symbol)
    for factor in payload.value:
        if (
            not isinstance(factor, AdjustFactor)
            or factor.symbol != symbol
            or factor.market != expected_market
        ):
            raise DataValidationError("Provider returned adjust factors for a different symbol or market")
        if factor.qfq_factor is None and factor.hfq_factor is None:
            raise DataValidationError("Provider returned an empty adjust factor row")
        if (factor.qfq_factor is None) != (factor.qfq_offset is None):
            raise DataValidationError(
                "qfq_factor and qfq_offset must both be provided or both be None; "
                "never substitute 0 for an unprovided offset"
            )
        if (factor.hfq_factor is None) != (factor.hfq_offset is None):
            raise DataValidationError(
                "hfq_factor and hfq_offset must both be provided or both be None; "
                "never substitute 0 for an unprovided offset"
            )
        if any(
            val is not None and (not val.is_finite() or val <= 0)
            for val in (factor.qfq_factor, factor.hfq_factor)
        ):
            raise DataValidationError("Provider returned a non-positive or non-finite adjust factor")
        if any(
            off is not None and not off.is_finite()
            for off in (factor.qfq_offset, factor.hfq_offset)
        ):
            raise DataValidationError("Provider returned a non-finite adjust offset")
        if expected_market == Market.CN and any(
            off is not None and off != Decimal(0)
            for off in (factor.qfq_offset, factor.hfq_offset)
        ):
            raise DataValidationError("Mainland China A-share adjust offsets must be 0")
        if (start is not None and factor.ex_date < start) or (
            end is not None and factor.ex_date > end
        ):
            raise DataValidationError("Provider returned an adjust factor outside the requested window")
    if len({item.ex_date for item in payload.value}) != len(payload.value):
        raise DataValidationError("Provider returned duplicate adjust factor dates")
    return replace(payload, value=sorted(payload.value, key=lambda item: item.ex_date))


def normalize_bar_window(
    start: datetime,
    end: datetime,
    interval: Interval,
    *,
    market: Market = Market.CN,
) -> tuple[datetime, datetime]:
    """Normalize bar boundaries to Beijing time and date labels for daily+ intervals."""

    start_bj = to_beijing(start)
    end_bj = to_beijing(end)
    if start_bj > end_bj:
        raise ValueError("start must be before end")
    if interval in {Interval.DAY_1, Interval.WEEK_1, Interval.MONTH_1}:
        start_day = start.date()
        end_day = end.date()
        if start_day > end_day:
            start_day, end_day = start_bj.date(), end_bj.date()
        if interval in {Interval.WEEK_1, Interval.MONTH_1}:
            end_day = max(end_day, period_anchor_date(end_day, interval, market))
        return (
            datetime.combine(start_day, time.min, tzinfo=BEIJING_TZ),
            datetime.combine(end_day, time.min, tzinfo=BEIJING_TZ),
        )
    return start_bj, end_bj


def _prefer_stale_over_partial(
    stale: object,
    partial: tuple[MarketDataProvider, SourcePayload[object], float, bool] | None,
) -> bool:
    if partial is None:
        return True
    _, payload, _, _ = partial
    if payload.stale:
        return True
    quality = payload.quality
    if quality is not None and quality.truncated:
        return True
    return (
        isinstance(stale, DataResult)
        and stale.metadata.quality is not None
        and stale.metadata.quality.available_end is not None
        and quality is not None
        and quality.available_end is not None
        and quality.available_end < stale.metadata.quality.available_end
    )


def _pick_better_partial(
    existing: tuple[MarketDataProvider, SourcePayload[T], float, bool] | None,
    incoming: tuple[MarketDataProvider, SourcePayload[T], float, bool],
) -> tuple[MarketDataProvider, SourcePayload[T], float, bool]:
    if existing is None:
        return incoming
    return incoming if _partial_rank(incoming) > _partial_rank(existing) else existing


def _partial_rank(
    candidate: tuple[MarketDataProvider, SourcePayload[T], float, bool],
) -> tuple[int, int, int, float, int, int]:
    _, payload, _, is_eod_lag = candidate
    quality = payload.quality
    not_stale = int(not payload.stale)
    not_truncated = int(not (quality.truncated if quality is not None else False))
    not_eod_lag = int(not is_eod_lag)
    available_end_ts = (
        quality.available_end.timestamp()
        if quality is not None and quality.available_end is not None
        else (payload.fetched_at.timestamp() if payload.fetched_at is not None else 0.0)
    )
    negative_filtered = -(quality.filtered_rows if quality is not None else 0)
    returned_rows = quality.returned_rows if quality is not None else 0
    return (
        not_stale,
        not_truncated,
        not_eod_lag,
        available_end_ts,
        negative_filtered,
        returned_rows,
    )


def _bar_window_requires_data(
    start: datetime,
    end: datetime,
    market: Market = Market.CN,
    interval: Interval = Interval.DAY_1,
) -> bool:
    """Distinguish an upstream empty response from a holiday/weekend/future query."""

    return window_has_trading_session(start, end, market, interval=interval)
