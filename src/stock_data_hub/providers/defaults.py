"""Practical default provider composition for the optional source extra."""

from __future__ import annotations

import importlib.util
from collections.abc import Iterable, Mapping, Sequence
from datetime import timedelta
from pathlib import Path

from stock_data_hub.cache import ResultCache
from stock_data_hub.hub import CachePolicy, DataHub, RouteKey
from stock_data_hub.models import AssetType, Capability, Interval, Market
from stock_data_hub.providers.akshare import AkShareProvider, CninfoProvider
from stock_data_hub.providers.baostock import BaoStockProvider
from stock_data_hub.providers.base import MarketDataProvider
from stock_data_hub.providers.calendar import ExchangeCalendarProvider
from stock_data_hub.providers.efinance import EFinanceProvider
from stock_data_hub.providers.pytdx import PyTDXProvider
from stock_data_hub.providers.remote import RemoteHubProvider
from stock_data_hub.providers.tencent import TencentQuoteProvider
from stock_data_hub.providers.tickflow import TickFlowProvider
from stock_data_hub.providers.yfinance import YFinanceProvider
from stock_data_hub.sqlite_cache import PERSISTENT_CACHE_POLICIES, SQLiteResultCache


def create_default_hub(
    *,
    pytdx_host: str | None = None,
    pytdx_port: int = 7709,
    peer_urls: Iterable[str] = (),
    peer_api_key: str | None = None,
    disabled_providers: Iterable[str] = (),
    source_priority: Mapping[Capability | str | RouteKey, Sequence[str]] | None = None,
    provider_timeout_seconds: float | None = 20.0,
    provider_queue_timeout_seconds: float = 2.0,
    snapshot_timeout_seconds: float = 45.0,
    request_timeout_seconds: float = 240.0,
    local_timeout_seconds: float = 90.0,
    peer_timeout_seconds: float = 95.0,
    max_concurrent_requests: int = 8,
    provider_rate_limits: Mapping[str, float] | None = None,
    min_cn_snapshot_rows: int = 4000,
    attempts_per_provider: int = 2,
    failure_threshold: int = 2,
    circuit_cooldown: timedelta = timedelta(seconds=30),
    cache: ResultCache | None = None,
    cache_path: str | Path | None = None,
    cache_policies: Mapping[Capability | str, CachePolicy] | None = None,
) -> DataHub:
    """Build a credential-free hub from the installed provider packages.

    PyTDX is enabled only when a quote-server host is supplied. No API tokens
    or credential environment variables are read by this composition.
    """

    disabled = {str(item).strip().lower() for item in disabled_providers if str(item).strip()}
    providers: list[MarketDataProvider] = []
    if "tickflow" not in disabled and _installed("tickflow"):
        providers.append(TickFlowProvider())
    efinance_installed = "efinance" not in disabled and _installed("efinance")
    if efinance_installed:
        providers.append(
            EFinanceProvider(
                min_snapshot_rows=min_cn_snapshot_rows,
                min_instrument_rows=min_cn_snapshot_rows,
            )
        )
    if "tencent" not in disabled:
        providers.append(TencentQuoteProvider())
    if "pytdx" not in disabled and pytdx_host and _installed("pytdx"):
        providers.append(PyTDXProvider(pytdx_host, pytdx_port))
    cninfo_enabled = "cninfo" not in disabled and "akshare" not in disabled
    # EFinance already covers Eastmoney. Prefer Sina first inside AkShare when
    # EFinance is enabled so an Eastmoney outage is not paid for twice.
    if "akshare" not in disabled:
        providers.append(
            AkShareProvider(
                prefer_sina_snapshot=efinance_installed,
                prefer_sina_bars=efinance_installed,
                include_cninfo=not cninfo_enabled,
                min_snapshot_rows=min_cn_snapshot_rows,
                min_instrument_rows=min_cn_snapshot_rows,
            )
        )
    if cninfo_enabled:
        providers.append(CninfoProvider())
    if "baostock" not in disabled and _installed("baostock"):
        providers.append(BaoStockProvider())
    if "yfinance" not in disabled and _installed("yfinance"):
        providers.append(YFinanceProvider())
    if "exchange_calendar" not in disabled:
        providers.append(ExchangeCalendarProvider())

    normalized_peer_urls = (peer_urls,) if isinstance(peer_urls, str) else peer_urls
    deduped_peers: list[str] = []
    for raw_url in normalized_peer_urls:
        cleaned = str(raw_url).strip().rstrip("/")
        if cleaned and cleaned not in deduped_peers:
            deduped_peers.append(cleaned)
    for index, peer_url in enumerate(deduped_peers, start=1):
        peer_name = f"remote-{index}"
        if peer_name in disabled:
            continue
        providers.append(
            RemoteHubProvider(
                peer_url,
                name=peer_name,
                api_key=peer_api_key,
                timeout_seconds=peer_timeout_seconds,
            )
        )

    if cache is not None and cache_path is not None:
        raise ValueError("Pass either cache or cache_path, not both")
    resolved_cache: ResultCache | None = cache
    resolved_policies: dict[Capability | str, CachePolicy] | None = (
        dict(cache_policies) if cache_policies is not None else None
    )
    if cache_path is not None:
        resolved_cache = SQLiteResultCache(cache_path)
        merged_policies: dict[Capability | str, CachePolicy] = dict(PERSISTENT_CACHE_POLICIES)
        if cache_policies:
            merged_policies.update(cache_policies)
        resolved_policies = merged_policies

    merged_priority: dict[Capability | str | RouteKey, Sequence[str]] = dict(_DEFAULT_PRIORITY)
    if source_priority:
        merged_priority.update(source_priority)

    resolved_rate_limits: dict[str, float] = dict(_DEFAULT_PROVIDER_RPS)
    if provider_rate_limits is not None:
        resolved_rate_limits.update(provider_rate_limits)

    return DataHub(
        providers,
        source_priority=merged_priority,
        cache=resolved_cache,
        cache_policies=resolved_policies,
        attempts_per_provider=attempts_per_provider,
        failure_threshold=failure_threshold,
        circuit_cooldown=circuit_cooldown,
        provider_timeout_seconds=provider_timeout_seconds,
        provider_timeout_overrides={Capability.SNAPSHOT: snapshot_timeout_seconds},
        provider_queue_timeout_seconds=provider_queue_timeout_seconds,
        max_concurrent_requests=max_concurrent_requests,
        provider_rate_limits=resolved_rate_limits,
        request_timeout_seconds=request_timeout_seconds,
        local_timeout_seconds=local_timeout_seconds,
        peer_timeout_seconds=peer_timeout_seconds,
    )


def _installed(package: str) -> bool:
    return importlib.util.find_spec(package) is not None


_DEFAULT_PRIORITY: dict[Capability | RouteKey, tuple[str, ...]] = {
    RouteKey(Capability.BARS, Market.CN, AssetType.STOCK, Interval.DAY_1): (
        "tickflow",
        "efinance",
        "pytdx",
        "akshare",
        "baostock",
        "yfinance",
    ),
    RouteKey(Capability.BARS, Market.CN, AssetType.STOCK, Interval.MINUTE_15): (
        "efinance",
        "pytdx",
        "akshare",
        "baostock",
        "yfinance",
    ),
    RouteKey(Capability.BARS, Market.CN, AssetType.STOCK, Interval.HOUR_1): (
        "efinance",
        "pytdx",
        "akshare",
        "baostock",
        "yfinance",
    ),
    RouteKey(Capability.BARS, Market.CN, AssetType.ETF, Interval.DAY_1): (
        "tickflow",
        "efinance",
        "pytdx",
        "akshare",
        "baostock",
        "yfinance",
    ),
    RouteKey(Capability.BARS, Market.CN, AssetType.ETF, Interval.MINUTE_15): (
        "efinance",
        "pytdx",
        "akshare",
        "yfinance",
    ),
    RouteKey(Capability.BARS, Market.CN, AssetType.ETF, Interval.HOUR_1): (
        "efinance",
        "pytdx",
        "akshare",
        "yfinance",
    ),
    RouteKey(Capability.BARS, Market.CN, AssetType.INDEX): (
        "efinance",
        "pytdx",
        "akshare",
        "baostock",
        "yfinance",
    ),
    RouteKey(Capability.BARS, Market.CN): (
        "tickflow",
        "efinance",
        "pytdx",
        "akshare",
        "baostock",
        "yfinance",
    ),
    RouteKey(Capability.BARS, Market.HK): ("tickflow", "akshare", "yfinance"),
    RouteKey(Capability.BARS, Market.US): ("tickflow", "akshare", "yfinance"),
    RouteKey(Capability.QUOTE, Market.CN): ("efinance", "tencent", "akshare", "yfinance"),
    RouteKey(Capability.QUOTE, Market.HK): ("tencent", "akshare", "yfinance"),
    RouteKey(Capability.QUOTE, Market.US): ("tencent", "akshare", "yfinance"),
    Capability.SNAPSHOT: ("efinance", "akshare"),
    Capability.INSTRUMENTS: ("akshare", "efinance"),
    Capability.TRADE_CALENDAR: ("akshare", "exchange_calendar"),
    Capability.COMPANY_PROFILE: ("cninfo", "akshare"),
    Capability.DISCLOSURES: ("cninfo", "akshare"),
    Capability.US_DAILY_CLOSES: ("akshare", "yfinance"),
    Capability.ADJUST_FACTORS: ("baostock", "akshare"),
}

_DEFAULT_PROVIDER_RPS: Mapping[str, float] = {
    "tickflow": 8.0,
    "efinance": 6.0,
    "tencent": 15.0,
    "pytdx": 10.0,
    "akshare": 5.0,
    "cninfo": 4.0,
    "baostock": 5.0,
    "yfinance": 4.0,
}
