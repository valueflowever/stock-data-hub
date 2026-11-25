"""Live connectivity & latency self-check (``stock-data-hub doctor``)."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from math import isfinite
from time import perf_counter

from stock_data_hub._workers import BoundedWorkers
from stock_data_hub.errors import UnsupportedCapability
from stock_data_hub.hub import DataHub, RouteKey, _fetch_bars, _fetch_quote, normalize_bar_window
from stock_data_hub.models import Adjust, AssetType, Capability, Interval, Market
from stock_data_hub.normalization import BEIJING_TZ
from stock_data_hub.providers.base import MarketDataProvider
from stock_data_hub.serialization import to_jsonable


@dataclass(frozen=True)
class ProbeSpec:
    name: str
    capability: Capability
    market: Market
    symbol: str | None = None
    asset_type: AssetType | None = None
    interval: Interval | None = None
    adjust: Adjust | None = None
    lookback_days: int = 14


@dataclass(frozen=True)
class ProbeResult:
    provider: str
    probe: str
    capability: Capability
    market: Market
    interval: Interval | None
    ok: bool
    latency_ms: float
    source: str | None = None
    detail: str = ""


@dataclass(frozen=True)
class DoctorRecommendation:
    disabled_providers: tuple[str, ...]
    source_priority: Mapping[RouteKey, tuple[str, ...]]
    recommended_timeout_seconds: float
    python_snippet: str
    env_vars: Mapping[str, str]


@dataclass(frozen=True)
class DoctorReport:
    checked_at: datetime
    timeout_seconds: float
    markets: tuple[Market, ...]
    results: tuple[ProbeResult, ...]
    recommendation: DoctorRecommendation

    def format_text(self) -> str:
        lines = [
            f"Stock Data Hub Doctor ({self.checked_at.strftime('%Y-%m-%d %H:%M:%S %Z')})",
            (
                f"Markets: {', '.join(m.value for m in self.markets)} | "
                f"Timeout: {self.timeout_seconds:.1f}s"
            ),
            "",
            f"{'PROVIDER':<18} {'PROBE':<14} {'STATUS':<7} {'LATENCY':>9}  DETAIL",
            "-" * 78,
        ]
        for item in self.results:
            status = "OK" if item.ok else "FAIL"
            latency = f"{item.latency_ms:7.0f}ms"
            detail = item.detail
            if item.source and item.ok and item.source != item.provider:
                detail = f"[{item.source}] {detail}"
            lines.append(f"{item.provider:<18} {item.probe:<14} {status:<7} {latency:>9}  {detail}")

        lines.append("")
        lines.append("Recommended configuration:")
        if self.recommendation.disabled_providers:
            lines.append(
                "  Disable unreachable providers: "
                + ", ".join(self.recommendation.disabled_providers)
            )
        else:
            lines.append("  All probed providers succeeded on at least one route.")
        lines.append(
            f"  Suggested provider_timeout_seconds: "
            f"{self.recommendation.recommended_timeout_seconds:.1f}s"
        )
        for route, order in self.recommendation.source_priority.items():
            lines.append(f"  {_route_label(route)}: {' -> '.join(order)}")
        lines.append("")
        lines.append("Python snippet:")
        lines.append(self.recommendation.python_snippet)
        return "\n".join(lines)

    def to_dict(self) -> dict[str, object]:
        return {
            "checked_at": self.checked_at.isoformat(),
            "timeout_seconds": self.timeout_seconds,
            "markets": [market.value for market in self.markets],
            "results": to_jsonable(self.results),
            "recommendation": {
                "disabled_providers": list(self.recommendation.disabled_providers),
                "source_priority": {
                    _route_label(route): list(order)
                    for route, order in self.recommendation.source_priority.items()
                },
                "recommended_timeout_seconds": self.recommendation.recommended_timeout_seconds,
                "env_vars": dict(self.recommendation.env_vars),
                "python_snippet": self.recommendation.python_snippet,
            },
        }


DEFAULT_PROBES: tuple[ProbeSpec, ...] = (
    ProbeSpec(
        "cn_1d", Capability.BARS, Market.CN, "600519.SH", AssetType.STOCK, Interval.DAY_1,
        Adjust.QFQ, 14,
    ),
    ProbeSpec(
        "cn_15m", Capability.BARS, Market.CN, "600519.SH", AssetType.STOCK, Interval.MINUTE_15,
        Adjust.QFQ, 7,
    ),
    ProbeSpec("cn_quote", Capability.QUOTE, Market.CN, "600519.SH", AssetType.STOCK),
    ProbeSpec("cn_calendar", Capability.TRADE_CALENDAR, Market.CN),
    ProbeSpec(
        "hk_1d", Capability.BARS, Market.HK, "00700.HK", AssetType.STOCK, Interval.DAY_1,
        Adjust.QFQ, 14,
    ),
    ProbeSpec("hk_quote", Capability.QUOTE, Market.HK, "00700.HK", AssetType.STOCK),
    ProbeSpec(
        "us_1d", Capability.BARS, Market.US, "AAPL.US", AssetType.STOCK, Interval.DAY_1,
        Adjust.QFQ, 14,
    ),
    ProbeSpec(
        "us_15m", Capability.BARS, Market.US, "AAPL.US", AssetType.STOCK, Interval.MINUTE_15,
        Adjust.QFQ, 7,
    ),
    ProbeSpec("us_quote", Capability.QUOTE, Market.US, "AAPL.US", AssetType.STOCK),
)


async def run_doctor(
    hub: DataHub,
    *,
    markets: Iterable[Market | str] = (Market.CN, Market.HK, Market.US),
    timeout_seconds: float = 12.0,
    probes: Sequence[ProbeSpec] | None = None,
    now: datetime | None = None,
) -> DoctorReport:
    """Probe each registered provider in ``hub`` and recommend an optimal setup."""

    if not isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    selected_markets = tuple(
        dict.fromkeys(
            item if isinstance(item, Market) else Market(str(item).strip().upper())
            for item in markets
        )
    )
    if not selected_markets:
        raise ValueError("At least one market must be selected")
    active_probes = tuple(
        spec
        for spec in (probes or DEFAULT_PROBES)
        if spec.market in selected_markets
    )
    reference = now or datetime.now(tz=BEIJING_TZ)
    providers = list(hub._providers.values())

    async def probe_provider(provider: MarketDataProvider) -> list[ProbeResult]:
        worker = BoundedWorkers(1, name=f"doctor:{provider.name}")
        provider_results: list[ProbeResult] = []
        for spec in active_probes:
            effective_adjust = _select_adjust(provider, spec)
            if not provider.supports_request(
                spec.capability,
                market=spec.market,
                asset_type=spec.asset_type,
                interval=spec.interval,
                adjust=effective_adjust,
            ):
                continue
            provider_results.append(
                await _execute_probe(
                    provider,
                    worker,
                    spec,
                    effective_adjust,
                    reference,
                    timeout_seconds,
                )
            )
        return provider_results

    nested = await asyncio.gather(*(probe_provider(provider) for provider in providers))
    results = tuple(
        sorted(
            (item for group in nested for item in group),
            key=lambda item: (item.probe, not item.ok, item.latency_ms, item.provider),
        )
    )
    recommendation = build_recommendation(
        results,
        selected_markets=selected_markets,
        providers={provider.name: provider for provider in providers},
    )
    return DoctorReport(
        checked_at=reference,
        timeout_seconds=timeout_seconds,
        markets=selected_markets,
        results=results,
        recommendation=recommendation,
    )


def build_recommendation(
    results: Sequence[ProbeResult],
    *,
    selected_markets: Sequence[Market] | None = None,
    providers: Mapping[str, MarketDataProvider] | None = None,
) -> DoctorRecommendation:
    by_provider: dict[str, list[ProbeResult]] = {}
    for item in results:
        by_provider.setdefault(item.provider, []).append(item)

    probed_market_set = set(selected_markets) if selected_markets is not None else None
    disabled_names: list[str] = []
    for provider_name, items in by_provider.items():
        if not items or any(item.ok for item in items):
            continue
        if providers is not None and probed_market_set is not None:
            provider_obj = providers.get(provider_name)
            if (
                provider_obj is not None
                and provider_obj.markets
                and not provider_obj.markets.issubset(probed_market_set)
            ):
                # Do not globally disable a multi-market provider when only a subset
                # of its markets was probed (e.g. ``--markets US`` failing should not
                # disable CN company/disclosure routes).
                continue
        disabled_names.append(provider_name)
    disabled = tuple(sorted(disabled_names))

    route_candidates: dict[RouteKey, list[ProbeResult]] = {}
    for item in results:
        if not item.ok:
            continue
        key = _route_key_for_probe(item)
        if key is not None:
            route_candidates.setdefault(key, []).append(item)

    source_priority: dict[RouteKey, tuple[str, ...]] = {}
    for route, items in sorted(route_candidates.items(), key=lambda pair: _route_label(pair[0])):
        ordered = tuple(
            dict.fromkeys(
                item.provider for item in sorted(items, key=lambda r: (r.latency_ms, r.provider))
            )
        )
        source_priority[route] = ordered
    cn_15m = RouteKey(Capability.BARS, Market.CN, AssetType.STOCK, Interval.MINUTE_15)
    cn_1h = RouteKey(Capability.BARS, Market.CN, AssetType.STOCK, Interval.HOUR_1)
    if cn_15m in source_priority and cn_1h not in source_priority:
        source_priority[cn_1h] = source_priority[cn_15m]

    ok_latencies = sorted(item.latency_ms for item in results if item.ok)
    if ok_latencies:
        p95 = ok_latencies[min(len(ok_latencies) - 1, int(len(ok_latencies) * 0.95))]
        recommended_timeout = min(30.0, max(10.0, round((p95 * 3.0) / 1000.0, 1)))
    else:
        recommended_timeout = 20.0

    env_vars: dict[str, str] = {
        "STOCK_DATA_HUB_PROVIDER_TIMEOUT_SECONDS": f"{recommended_timeout:.1f}",
    }
    if disabled:
        env_vars["STOCK_DATA_HUB_DISABLED_PROVIDERS"] = ",".join(disabled)

    snippet = _format_python_snippet(disabled, source_priority, recommended_timeout)
    return DoctorRecommendation(
        disabled_providers=disabled,
        source_priority=source_priority,
        recommended_timeout_seconds=recommended_timeout,
        python_snippet=snippet,
        env_vars=env_vars,
    )


async def _execute_probe(
    provider: MarketDataProvider,
    worker: BoundedWorkers,
    spec: ProbeSpec,
    adjust: Adjust | None,
    reference: datetime,
    timeout_seconds: float,
) -> ProbeResult:
    started_at = perf_counter()

    def call() -> tuple[str | None, str]:
        if spec.capability == Capability.BARS:
            assert spec.symbol is not None and spec.interval is not None and adjust is not None
            start_dt, end_dt = normalize_bar_window(
                reference - timedelta(days=spec.lookback_days),
                reference,
                spec.interval,
            )
            payload = _fetch_bars(
                provider,
                spec.symbol,
                spec.interval,
                start_dt,
                end_dt,
                adjust,
                spec.asset_type or AssetType.STOCK,
            )
            if not payload.value:
                raise ValueError("returned 0 bars")
            latest = payload.value[-1].ts.strftime("%Y-%m-%d %H:%M")
            suffix = f" ({adjust.value})" if adjust != spec.adjust else ""
            return payload.source, f"{len(payload.value)} bars, latest {latest}{suffix}"
        if spec.capability == Capability.QUOTE:
            assert spec.symbol is not None
            payload = _fetch_quote(provider, spec.symbol, spec.asset_type or AssetType.STOCK)
            return payload.source, f"price={payload.value.price} ({payload.value.freshness.value})"
        if spec.capability == Capability.TRADE_CALENDAR:
            payload = provider.fetch_trade_calendar()
            return payload.source, f"{len(payload.value.trade_dates)} trade dates"
        raise UnsupportedCapability(f"Unsupported probe capability {spec.capability.value}")

    try:
        source, detail = await worker.run(call, timeout=timeout_seconds, queue_timeout=0.5)
        latency_ms = (perf_counter() - started_at) * 1000.0
        return ProbeResult(
            provider=provider.name,
            probe=spec.name,
            capability=spec.capability,
            market=spec.market,
            interval=spec.interval,
            ok=True,
            latency_ms=latency_ms,
            source=source or provider.name,
            detail=detail,
        )
    except asyncio.TimeoutError:
        latency_ms = (perf_counter() - started_at) * 1000.0
        return ProbeResult(
            provider=provider.name,
            probe=spec.name,
            capability=spec.capability,
            market=spec.market,
            interval=spec.interval,
            ok=False,
            latency_ms=latency_ms,
            detail=f"TimeoutError: exceeded {timeout_seconds:.1f}s",
        )
    except Exception as exc:  # noqa: BLE001 - report arbitrary provider failure cleanly
        latency_ms = (perf_counter() - started_at) * 1000.0
        message = str(exc).strip().replace("\n", " ")[:120]
        return ProbeResult(
            provider=provider.name,
            probe=spec.name,
            capability=spec.capability,
            market=spec.market,
            interval=spec.interval,
            ok=False,
            latency_ms=latency_ms,
            detail=f"{type(exc).__name__}: {message}",
        )


def _select_adjust(provider: MarketDataProvider, spec: ProbeSpec) -> Adjust | None:
    if spec.capability != Capability.BARS or spec.adjust is None:
        return spec.adjust
    if not provider.adjustments or spec.adjust in provider.adjustments:
        return spec.adjust
    if Adjust.NONE in provider.adjustments:
        return Adjust.NONE
    return spec.adjust


def _route_key_for_probe(item: ProbeResult) -> RouteKey | None:
    if item.capability == Capability.BARS:
        if item.market == Market.CN and item.interval in {Interval.DAY_1, Interval.MINUTE_15}:
            return RouteKey(Capability.BARS, Market.CN, AssetType.STOCK, item.interval)
        if item.market in {Market.HK, Market.US} and item.interval == Interval.DAY_1:
            return RouteKey(Capability.BARS, item.market)
    if item.capability == Capability.QUOTE:
        return RouteKey(Capability.QUOTE, item.market)
    if item.capability == Capability.TRADE_CALENDAR:
        return RouteKey(Capability.TRADE_CALENDAR)
    return None


def _route_label(route: RouteKey) -> str:
    parts = [route.capability.value]
    for field in (route.market, route.asset_type, route.interval, route.adjust):
        if field is not None:
            parts.append(field.value)
    return ":".join(parts)


def _format_python_snippet(
    disabled: tuple[str, ...],
    priority: Mapping[RouteKey, tuple[str, ...]],
    timeout_seconds: float,
) -> str:
    lines = [
        "from stock_data_hub import (",
        "    AssetType,",
        "    Capability,",
        "    Interval,",
        "    Market,",
        "    RouteKey,",
        "    create_default_hub,",
        ")",
        "",
        "hub = create_default_hub(",
        '    cache_path="~/.cache/stock_data_hub.db",',
        f"    provider_timeout_seconds={timeout_seconds:.1f},",
    ]
    if disabled:
        lines.append(f"    disabled_providers={list(disabled)!r},")
    if priority:
        lines.append("    source_priority={")
        for route, order in priority.items():
            args = [f"Capability.{route.capability.name}"]
            if route.market is not None:
                args.append(f"Market.{route.market.name}")
            if route.asset_type is not None:
                args.append(f"AssetType.{route.asset_type.name}")
            if route.interval is not None:
                args.append(f"Interval.{route.interval.name}")
            lines.append(f"        RouteKey({', '.join(args)}): {order!r},")
        lines.append("    },")
    lines.append(")")
    return "\n".join(lines)
