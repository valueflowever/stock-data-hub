"""Lightweight, credential-free Tencent Finance quote adapter (CN/HK/US)."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from datetime import datetime
from math import isfinite
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener, urlopen

from stock_data_hub._workers import remaining_timeout
from stock_data_hub.cache import SharedFetchCache
from stock_data_hub.errors import DataValidationError, ProviderUnavailable, UnsupportedCapability
from stock_data_hub.models import (
    AssetType,
    Capability,
    Market,
    Quote,
    QuoteFreshness,
    SourcePayload,
)
from stock_data_hub.normalization import (
    BEIJING_TZ,
    market_from_symbol,
    normalize_symbol,
    to_beijing,
)
from stock_data_hub.providers._parsing import required_positive_decimal
from stock_data_hub.providers.base import MarketDataProvider
from stock_data_hub.trading_sessions import market_timezone

_TENCENT_ENDPOINT = "https://qt.gtimg.cn/q="
_LINE_PATTERN = re.compile(r'v_(?P<key>[A-Za-z0-9._-]+)="(?P<payload>[^"]*)";?')
_BATCH_SIZE = 60


class TencentQuoteProvider(MarketDataProvider):
    """Fast HTTP quote provider supporting CN (SH/SZ/BJ), HK, and US symbols."""

    name = "tencent"
    capabilities = frozenset({Capability.QUOTE})
    markets = frozenset({Market.CN, Market.HK, Market.US})
    asset_types = frozenset({AssetType.STOCK, AssetType.ETF, AssetType.INDEX})
    accepts_asset_type = True
    supports_batch_quotes = True

    def __init__(
        self,
        *,
        timeout_seconds: float = 10.0,
        use_environment_proxy: bool = True,
        max_response_bytes: int = 2 * 1024 * 1024,
        opener: Callable[..., Any] | None = None,
    ) -> None:
        if not isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("Tencent timeout must be positive")
        if max_response_bytes < 1:
            raise ValueError("Tencent response limit must be positive")
        self._timeout_seconds = timeout_seconds
        self._max_response_bytes = max_response_bytes
        self._cache = SharedFetchCache(ttl_seconds=3.0, max_entries=256)
        if opener is not None:
            self._opener = opener
        elif use_environment_proxy:
            self._opener = urlopen
        else:
            self._opener = build_opener(ProxyHandler({})).open

    def clear_cache(self) -> None:
        self._cache.clear()

    def prefetch_quotes(
        self,
        symbols: Sequence[str],
        *,
        asset_type: AssetType | None = None,
    ) -> None:
        normalized_pairs: list[tuple[str, str]] = []
        seen: set[str] = set()
        for raw_symbol in symbols:
            normalized = normalize_symbol(raw_symbol)
            if normalized in seen:
                continue
            seen.add(normalized)
            normalized_pairs.append((normalized, _tencent_symbol(normalized)))

        for offset in range(0, len(normalized_pairs), _BATCH_SIZE):
            chunk = normalized_pairs[offset : offset + _BATCH_SIZE]
            raw_map = self._fetch_raw_map([key for _, key in chunk])
            fetched_at = datetime.now(tz=BEIJING_TZ)
            for normalized, key in chunk:
                payload_text = raw_map.get(key)
                if not payload_text:
                    continue
                try:
                    quote = _parse_quote(normalized, payload_text, fetched_at)
                except DataValidationError:
                    continue
                self._cache.put(normalized, quote, fetched_at=fetched_at)

    def fetch_quote(
        self,
        symbol: str,
        *,
        asset_type: AssetType | None = None,
    ) -> SourcePayload[Quote]:
        normalized = normalize_symbol(symbol)
        market = market_from_symbol(normalized)
        key = _tencent_symbol(normalized)

        def load() -> Quote:
            raw_map = self._fetch_raw_map([key])
            payload_text = raw_map.get(key)
            if not payload_text:
                raise DataValidationError(f"Tencent quote response has no row for {normalized}")
            return _parse_quote(normalized, payload_text, datetime.now(tz=BEIJING_TZ))

        fetched = self._cache.get(normalized, load)
        warnings = (
            ("Tencent global-market quotes may be delayed.",) if market != Market.CN else ()
        )
        return SourcePayload(
            value=fetched.value,
            source="tencent:qt",
            warnings=warnings,
            fetched_at=fetched.fetched_at,
            cache_hit=fetched.cache_hit,
        )

    def _fetch_raw_map(self, keys: Sequence[str]) -> dict[str, str]:
        url = _TENCENT_ENDPOINT + ",".join(keys)
        request = Request(
            url,
            headers={
                "Accept": "text/plain,*/*",
                "User-Agent": "stock-data-hub/0.7.1",
            },
            method="GET",
        )
        try:
            with self._opener(request, timeout=remaining_timeout(self._timeout_seconds)) as resp:
                raw = resp.read(self._max_response_bytes + 1)
                if len(raw) > self._max_response_bytes:
                    raise DataValidationError("Tencent quote response exceeded the size limit")
                text = raw.decode("gb18030", errors="replace")
        except DataValidationError:
            raise
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            raise ProviderUnavailable(f"Tencent quote request failed for {','.join(keys)}") from exc

        parsed: dict[str, str] = {}
        for match in _LINE_PATTERN.finditer(text):
            parsed[match.group("key")] = match.group("payload")
        return parsed


def _tencent_symbol(symbol: str) -> str:
    code, exchange = symbol.rsplit(".", maxsplit=1)
    if exchange in {"SH", "SZ", "BJ"}:
        return f"{exchange.lower()}{code}"
    if exchange == "HK":
        return f"hk{code.zfill(5)}"
    if exchange == "US":
        return f"us{code}"
    raise UnsupportedCapability(f"Tencent has no symbol mapping for {symbol}")


def _parse_quote(symbol: str, payload: str, fallback_ts: datetime) -> Quote:
    parts = payload.split("~")
    if len(parts) <= 3:
        raise DataValidationError(f"Tencent quote payload is incomplete for {symbol}")
    price = required_positive_decimal(parts[3].strip(), "latest price")
    market = market_from_symbol(symbol)
    ts = _parse_timestamp(parts, market) or fallback_ts
    freshness = QuoteFreshness.REALTIME if market == Market.CN else QuoteFreshness.DELAYED
    return Quote(symbol=symbol, price=price, ts=ts, market=market, freshness=freshness)


def _parse_timestamp(parts: Sequence[str], market: Market) -> datetime | None:
    if len(parts) <= 30:
        return None
    raw = parts[30].strip()
    if not raw:
        return None
    zone = market_timezone(market)
    if len(raw) == 14 and raw.isdigit():
        try:
            return to_beijing(datetime.strptime(raw, "%Y%m%d%H%M%S").replace(tzinfo=zone))
        except ValueError:
            return None
    for fmt in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return to_beijing(datetime.strptime(raw, fmt).replace(tzinfo=zone))
        except ValueError:
            continue
    return None
