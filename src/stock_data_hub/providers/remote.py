"""HTTP adapter for a peer Stock Data Hub in another network region."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import ProxyHandler, Request, build_opener, urlopen

from stock_data_hub.errors import DataValidationError, ProviderUnavailable
from stock_data_hub.models import (
    Adjust,
    AShareBar,
    AssetType,
    Capability,
    CompanyProfile,
    DailyClose,
    Disclosure,
    Instrument,
    Interval,
    Market,
    Quote,
    QuoteFreshness,
    RestrictedRelease,
    Snapshot,
    SnapshotRow,
    SourcePayload,
    TradeCalendar,
)
from stock_data_hub.normalization import market_from_symbol, to_beijing
from stock_data_hub.providers.base import MarketDataProvider


class RemoteHubProvider(MarketDataProvider):
    """Use another Hub node as the final fallback without sharing SDK state."""

    capabilities = frozenset(Capability)
    markets = frozenset(Market)
    asset_types = frozenset(AssetType)
    intervals = frozenset(Interval)
    adjustments = frozenset(Adjust)
    accepts_asset_type = True
    is_peer = True

    def __init__(
        self,
        base_url: str,
        *,
        name: str | None = None,
        timeout_seconds: float = 15.0,
        use_environment_proxy: bool = False,
        max_response_bytes: int = 32 * 1024 * 1024,
        api_key: str | None = None,
        opener: Callable[..., Any] | None = None,
    ) -> None:
        normalized_url = str(base_url).strip().rstrip("/")
        parsed_url = urlsplit(normalized_url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            raise ValueError("Remote Hub URL must use http:// or https://")
        if parsed_url.username is not None or parsed_url.password is not None:
            raise ValueError("Remote Hub URL cannot contain credentials")
        if parsed_url.query or parsed_url.fragment:
            raise ValueError("Remote Hub URL cannot contain a query or fragment")
        if timeout_seconds <= 0:
            raise ValueError("Remote Hub timeout must be positive")
        if max_response_bytes < 1:
            raise ValueError("Remote Hub response limit must be positive")
        self.base_url = normalized_url
        self.name = name or f"remote:{normalized_url}"
        self.timeout_seconds = timeout_seconds
        self.max_response_bytes = max_response_bytes
        self.api_key = str(api_key or "").strip() or None
        if opener is not None:
            self._opener = opener
        elif use_environment_proxy:
            self._opener = urlopen
        else:
            self._opener = build_opener(ProxyHandler({})).open

    def fetch_bars(
        self,
        symbol: str,
        interval: Interval,
        start: datetime,
        end: datetime,
        adjust: Adjust,
        *,
        asset_type: AssetType | None = None,
    ) -> SourcePayload[list[AShareBar]]:
        body = self._get(
            f"/bars/{symbol}",
            {
                "start": start.isoformat(),
                "end": end.isoformat(),
                "interval": interval.value,
                "adjust": adjust.value,
                "market": market_from_symbol(symbol).value,
                "asset_type": asset_type.value if asset_type is not None else None,
            },
        )
        market = market_from_symbol(symbol)
        bars = [_parse_bar(item, symbol, interval, market) for item in _data_list(body)]
        return _payload(body, bars, self.name)

    def fetch_quote(
        self,
        symbol: str,
        *,
        asset_type: AssetType | None = None,
    ) -> SourcePayload[Quote]:
        body = self._get(
            f"/quote/{symbol}",
            {
                "market": market_from_symbol(symbol).value,
                "asset_type": asset_type.value if asset_type is not None else None,
            },
        )
        data = _data_mapping(body)
        try:
            quote = Quote(
                symbol=str(data["symbol"]),
                price=_decimal(data["price"], "price"),
                ts=_datetime(data["ts"], "quote timestamp"),
                market=Market(str(data["market"])),
                freshness=QuoteFreshness(str(data.get("freshness", "realtime"))),
            )
        except (KeyError, ValueError) as exc:
            raise DataValidationError("Remote Hub returned an invalid quote") from exc
        return _payload(body, quote, self.name)

    def fetch_us_daily_closes(
        self,
        ticker: str,
        max_rows: int,
    ) -> SourcePayload[list[DailyClose]]:
        body = self._get(f"/us-daily-closes/{ticker}", {"max_rows": max_rows})
        closes: list[DailyClose] = []
        for item in _data_list(body):
            try:
                closes.append(
                    DailyClose(
                        symbol=str(item["symbol"]),
                        day=_datetime(item["day"], "daily close date").date(),
                        close=_decimal(item["close"], "close"),
                    )
                )
            except KeyError as exc:
                raise DataValidationError("Remote Hub returned an invalid daily close") from exc
        return _payload(body, closes, self.name)

    def fetch_snapshot(self) -> SourcePayload[Snapshot]:
        body, raw_rows = self._paged("/snapshot", "rows", "total_rows")
        data = _data_mapping(body)
        rows = tuple(_parse_snapshot_row(item) for item in raw_rows)
        try:
            snapshot = Snapshot(
                rows=rows,
                has_market_cap=bool(data["has_market_cap"]),
                fetched_at=_datetime(data["fetched_at"], "snapshot timestamp"),
            )
        except KeyError as exc:
            raise DataValidationError("Remote Hub returned an invalid snapshot") from exc
        return _payload(body, snapshot, self.name)

    def fetch_instruments(self) -> SourcePayload[list[Instrument]]:
        body, raw_items = self._paged("/instruments", "items", "total")
        instruments: list[Instrument] = []
        for item in raw_items:
            try:
                instruments.append(
                    Instrument(
                        symbol=str(item["symbol"]),
                        name=str(item["name"]),
                        market=Market(str(item.get("market", "CN"))),
                        asset_type=AssetType(str(item.get("asset_type", "stock"))),
                    )
                )
            except (KeyError, ValueError) as exc:
                raise DataValidationError("Remote Hub returned an invalid instrument") from exc
        return _payload(body, instruments, self.name)

    def fetch_trade_calendar(self) -> SourcePayload[TradeCalendar]:
        body = self._get("/trade-calendar", {})
        data = _data_mapping(body)
        raw_dates = data.get("trade_dates")
        if not isinstance(raw_dates, list):
            raise DataValidationError("Remote Hub returned an invalid trade calendar")
        try:
            calendar = TradeCalendar(
                trade_dates=frozenset(_date(item, "trade date") for item in raw_dates),
                start_date=_date(data["start_date"], "calendar start date"),
                end_date=_date(data["end_date"], "calendar end date"),
                fetched_at=_datetime(data["fetched_at"], "calendar timestamp"),
            )
        except KeyError as exc:
            raise DataValidationError("Remote Hub returned an invalid trade calendar") from exc
        return _payload(body, calendar, self.name)

    def fetch_company_profile(self, symbol: str) -> SourcePayload[CompanyProfile]:
        body = self._get(f"/company-profile/{symbol}", {})
        data = _data_mapping(body)
        try:
            profile = CompanyProfile(
                symbol=str(data["symbol"]),
                industry=_optional_text(data.get("industry")),
                main_business=_optional_text(data.get("main_business")),
                business_scope=_optional_text(data.get("business_scope")),
            )
        except KeyError as exc:
            raise DataValidationError("Remote Hub returned an invalid company profile") from exc
        return _payload(body, profile, self.name)

    def fetch_restricted_releases(
        self,
        symbol: str,
    ) -> SourcePayload[list[RestrictedRelease]]:
        body = self._get(f"/restricted-releases/{symbol}", {})
        releases: list[RestrictedRelease] = []
        for item in _data_list(body):
            try:
                releases.append(
                    RestrictedRelease(
                        symbol=str(item["symbol"]),
                        release_date=_date(item["release_date"], "release date"),
                        shares=_optional_decimal(item.get("shares")),
                        market_value=_optional_decimal(item.get("market_value")),
                        ratio_of_total=_optional_decimal(item.get("ratio_of_total")),
                        share_type=_optional_text(item.get("share_type")),
                    )
                )
            except KeyError as exc:
                raise DataValidationError(
                    "Remote Hub returned an invalid restricted release"
                ) from exc
        return _payload(body, releases, self.name)

    def fetch_disclosures(
        self,
        symbol: str,
        start: date,
        end: date,
    ) -> SourcePayload[list[Disclosure]]:
        body = self._get(
            f"/disclosures/{symbol}",
            {"start": start.isoformat(), "end": end.isoformat()},
        )
        disclosures: list[Disclosure] = []
        for item in _data_list(body):
            try:
                disclosures.append(
                    Disclosure(
                        symbol=str(item["symbol"]),
                        announce_date=_date(item["announce_date"], "announce date"),
                        title=str(item["title"]),
                    )
                )
            except KeyError as exc:
                raise DataValidationError("Remote Hub returned an invalid disclosure") from exc
        return _payload(body, disclosures, self.name)

    def _paged(
        self,
        path: str,
        items_field: str,
        total_field: str,
    ) -> tuple[Mapping[str, Any], list[Mapping[str, Any]]]:
        offset = 0
        limit = 2000
        first_body: Mapping[str, Any] | None = None
        values: list[Mapping[str, Any]] = []
        for _ in range(100):
            body = self._get(path, {"offset": offset, "limit": limit})
            first_body = first_body or body
            data = _data_mapping(body)
            page = data.get(items_field)
            if not isinstance(page, list) or not all(isinstance(item, Mapping) for item in page):
                raise DataValidationError(f"Remote Hub returned invalid {items_field}")
            values.extend(page)
            try:
                total = int(data[total_field])
            except (KeyError, TypeError, ValueError) as exc:
                raise DataValidationError(f"Remote Hub returned an invalid {total_field}") from exc
            if len(values) >= total or not page:
                return first_body, values
            offset += len(page)
        raise DataValidationError("Remote Hub pagination exceeded 100 pages")

    def _get(self, path: str, params: Mapping[str, object | None]) -> Mapping[str, Any]:
        query = urlencode({key: value for key, value in params.items() if value is not None})
        url = f"{self.base_url}{path}?{query}" if query else f"{self.base_url}{path}"
        headers = {"Accept": "application/json", "X-Data-Hub-Hop": "1"}
        if self.api_key is not None:
            headers["X-Data-Hub-Key"] = self.api_key
        request = Request(url, headers=headers, method="GET")
        try:
            with self._opener(request, timeout=self.timeout_seconds) as response:
                raw = response.read(self.max_response_bytes + 1)
                if len(raw) > self.max_response_bytes:
                    raise DataValidationError("Remote Hub response exceeded the size limit")
                body = json.loads(raw.decode("utf-8"))
        except DataValidationError:
            raise
        except (
            HTTPError,
            URLError,
            TimeoutError,
            OSError,
            UnicodeError,
            json.JSONDecodeError,
        ) as exc:
            raise ProviderUnavailable(f"Remote Hub request failed: {url}") from exc
        if not isinstance(body, Mapping):
            raise DataValidationError("Remote Hub response was not a JSON object")
        if "error" in body:
            raise ProviderUnavailable(f"Remote Hub rejected the request: {body['error']}")
        return body


def _parse_bar(
    item: Mapping[str, Any],
    symbol: str,
    interval: Interval,
    market: Market,
) -> AShareBar:
    try:
        bar = AShareBar(
            symbol=str(item.get("symbol", symbol)),
            ts=_datetime(item["ts"], "bar timestamp"),
            open=_decimal(item["open"], "open"),
            high=_decimal(item["high"], "high"),
            low=_decimal(item["low"], "low"),
            close=_decimal(item["close"], "close"),
            volume=_optional_decimal(item.get("volume")),
            amount=_optional_decimal(item.get("amount")),
            is_closed=bool(item.get("is_closed", True)),
            market=Market(str(item.get("market", market.value))),
        )
        if bar.low > min(bar.open, bar.close) or max(bar.open, bar.close) > bar.high:
            raise DataValidationError("Remote Hub returned an invalid OHLC relationship")
        if (bar.volume is not None and bar.volume < 0) or (
            bar.amount is not None and bar.amount < 0
        ):
            raise DataValidationError("Remote Hub returned a negative volume or amount")
        return bar
    except (KeyError, ValueError) as exc:
        raise DataValidationError(f"Remote Hub returned an invalid {interval.value} bar") from exc


def _parse_snapshot_row(item: Mapping[str, Any]) -> SnapshotRow:
    try:
        return SnapshotRow(
            symbol=str(item["symbol"]),
            name=str(item["name"]),
            price=_optional_decimal(item.get("price")),
            amount=_optional_decimal(item.get("amount")),
            market_cap=_optional_decimal(item.get("market_cap")),
            turnover_ratio=_optional_decimal(item.get("turnover_ratio")),
            change_pct=_optional_decimal(item.get("change_pct")),
        )
    except KeyError as exc:
        raise DataValidationError("Remote Hub returned an invalid snapshot row") from exc


def _payload(body: Mapping[str, Any], value: Any, peer_name: str) -> SourcePayload[Any]:
    metadata = body.get("metadata")
    source = None
    warnings: tuple[str, ...] = ()
    if isinstance(metadata, Mapping):
        source_value = metadata.get("source")
        source = f"{peer_name}/{source_value}" if source_value else peer_name
        raw_warnings = metadata.get("warnings")
        if isinstance(raw_warnings, list):
            warnings = tuple(str(item) for item in raw_warnings)
        stale_warning = "Peer Hub returned stale cached data."
        if metadata.get("stale") and stale_warning not in warnings:
            warnings = (*warnings, stale_warning)
    stale = bool(metadata.get("stale")) if isinstance(metadata, Mapping) else False
    return SourcePayload(
        value=value,
        source=source or peer_name,
        warnings=warnings,
        stale=stale,
    )


def _data_mapping(body: Mapping[str, Any]) -> Mapping[str, Any]:
    value = body.get("data")
    if not isinstance(value, Mapping):
        raise DataValidationError("Remote Hub response had no data object")
    return value


def _data_list(body: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    value = body.get("data")
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise DataValidationError("Remote Hub response had no data list")
    return value


def _datetime(value: object, field: str) -> datetime:
    try:
        return to_beijing(datetime.fromisoformat(str(value)))
    except ValueError as exc:
        raise DataValidationError(f"Remote Hub returned an invalid {field}") from exc


def _date(value: object, field: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise DataValidationError(f"Remote Hub returned an invalid {field}") from exc


def _decimal(value: object, field: str) -> Decimal:
    result = _optional_decimal(value)
    if result is None or result <= 0:
        raise DataValidationError(f"Remote Hub returned an invalid {field}")
    return result


def _optional_decimal(value: object | None) -> Decimal | None:
    if value is None:
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _optional_text(value: object | None) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None
