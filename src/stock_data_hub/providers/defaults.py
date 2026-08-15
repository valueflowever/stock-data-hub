"""Practical default provider composition for the optional source extra."""

from __future__ import annotations

import importlib.util
from collections.abc import Iterable

from stock_data_hub.hub import DataHub, RouteKey
from stock_data_hub.models import AssetType, Capability, Interval, Market
from stock_data_hub.providers.akshare import AkShareProvider
from stock_data_hub.providers.baostock import BaoStockProvider
from stock_data_hub.providers.base import MarketDataProvider
from stock_data_hub.providers.efinance import EFinanceProvider
from stock_data_hub.providers.pytdx import PyTDXProvider
from stock_data_hub.providers.remote import RemoteHubProvider
from stock_data_hub.providers.tickflow import TickFlowProvider
from stock_data_hub.providers.yfinance import YFinanceProvider


def create_default_hub(
    *,
    pytdx_host: str | None = None,
    pytdx_port: int = 7709,
    peer_urls: Iterable[str] = (),
    peer_api_key: str | None = None,
    snapshot_timeout_seconds: float = 45.0,
) -> DataHub:
    """Build a credential-free hub from the installed provider packages.

    PyTDX is enabled only when a quote-server host is supplied. No API tokens
    or credential environment variables are read by this composition.
    """

    providers: list[MarketDataProvider] = []
    if _installed("tickflow"):
        providers.append(TickFlowProvider())
    efinance_installed = _installed("efinance")
    if efinance_installed:
        providers.append(EFinanceProvider())
    if pytdx_host and _installed("pytdx"):
        providers.append(PyTDXProvider(pytdx_host, pytdx_port))
    # EFinance already covers Eastmoney. Use Sina first in the AkShare fallback
    # so a regional Eastmoney outage is not paid for twice on the same route.
    providers.append(AkShareProvider(prefer_sina_snapshot=efinance_installed))
    if _installed("baostock"):
        providers.append(BaoStockProvider())
    if _installed("yfinance"):
        providers.append(YFinanceProvider())
    normalized_peer_urls = (peer_urls,) if isinstance(peer_urls, str) else peer_urls
    for index, peer_url in enumerate(normalized_peer_urls, start=1):
        providers.append(
            RemoteHubProvider(
                peer_url,
                name=f"remote-{index}",
                api_key=peer_api_key,
            )
        )
    return DataHub(
        providers,
        source_priority=_DEFAULT_PRIORITY,
        provider_timeout_overrides={Capability.SNAPSHOT: snapshot_timeout_seconds},
    )


def _installed(package: str) -> bool:
    return importlib.util.find_spec(package) is not None


_DEFAULT_PRIORITY = {
    RouteKey(Capability.BARS, Market.CN, AssetType.STOCK, Interval.DAY_1): (
        "tickflow",
        "efinance",
        "pytdx",
        "akshare",
        "baostock",
        "yfinance",
    ),
    RouteKey(Capability.BARS, Market.CN, AssetType.ETF, Interval.DAY_1): (
        "tickflow",
        "efinance",
        "akshare",
        "baostock",
        "yfinance",
    ),
    RouteKey(Capability.BARS, Market.CN, AssetType.INDEX): ("efinance", "akshare", "yfinance"),
    RouteKey(Capability.BARS, Market.CN): (
        "tickflow",
        "efinance",
        "akshare",
        "baostock",
        "yfinance",
    ),
    RouteKey(Capability.BARS, Market.HK): ("tickflow", "akshare", "yfinance"),
    RouteKey(Capability.BARS, Market.US): ("tickflow", "akshare", "yfinance"),
    RouteKey(Capability.QUOTE, Market.CN): ("efinance", "akshare", "yfinance"),
    RouteKey(Capability.QUOTE, Market.HK): ("akshare", "yfinance"),
    RouteKey(Capability.QUOTE, Market.US): ("akshare", "yfinance"),
    Capability.SNAPSHOT: ("efinance", "akshare"),
    Capability.INSTRUMENTS: ("efinance", "akshare"),
    Capability.TRADE_CALENDAR: ("akshare",),
    Capability.US_DAILY_CLOSES: ("akshare", "yfinance"),
}
