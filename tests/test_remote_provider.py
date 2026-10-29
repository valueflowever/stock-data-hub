from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from datetime import date, datetime, timedelta
from decimal import Decimal
from io import BytesIO
from urllib.parse import parse_qs, urlparse
from urllib.request import Request

import pytest

try:
    from fastapi.testclient import TestClient
except ModuleNotFoundError:
    TestClient = None

from stock_data_hub.errors import DataValidationError
from stock_data_hub.hub import DataHub
from stock_data_hub.models import Adjust, AssetType, Interval, Market, QuoteFreshness
from stock_data_hub.normalization import BEIJING_TZ
from stock_data_hub.providers.remote import RemoteHubProvider
from stock_data_hub.service import create_app


class JsonResponse:
    def __init__(self, body: object) -> None:
        self._stream = BytesIO(json.dumps(body).encode("utf-8"))

    def __enter__(self) -> JsonResponse:  # noqa: PYI034 - Python 3.10 has no typing.Self
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)


def _metadata(*, stale: bool = False) -> dict[str, object]:
    return {
        "source": "yfinance:yahoo",
        "fetched_at": "2026-07-17T16:00:00+08:00",
        "latency_ms": 12.5,
        "attempted_sources": ["yfinance"],
        "cache_hit": False,
        "stale": stale,
        "warnings": ["peer warning"],
        "contract_version": "2.0",
    }


def test_remote_provider_parses_bars_and_preserves_peer_metadata() -> None:
    calls: list[tuple[str, float, dict[str, str]]] = []

    def opener(request: Request, *, timeout: float) -> JsonResponse:
        calls.append(
            (
                request.full_url,
                timeout,
                {key.lower(): value for key, value in request.header_items()},
            )
        )
        return JsonResponse(
            {
                "data": [
                    {
                        "symbol": "AAPL.US",
                        "ts": "2026-07-17T00:00:00+08:00",
                        "open": "210.0",
                        "high": "213.0",
                        "low": "209.0",
                        "close": "212.5",
                        "volume": "1000",
                        "amount": None,
                        "is_closed": True,
                        "market": "US",
                    }
                ],
                "metadata": _metadata(stale=True),
            }
        )

    provider = RemoteHubProvider(
        "https://overseas.example/",
        name="overseas",
        timeout_seconds=7,
        api_key="peer-secret",
        opener=opener,
    )
    result = provider.fetch_bars(
        "AAPL.US",
        Interval.DAY_1,
        datetime(2026, 7, 1, tzinfo=BEIJING_TZ),
        datetime(2026, 7, 31, tzinfo=BEIJING_TZ),
        Adjust.QFQ,
        asset_type=AssetType.STOCK,
    )

    assert result.value[0].close == Decimal("212.5")
    assert result.value[0].market == Market.US
    assert result.source == "overseas/yfinance:yahoo"
    assert result.stale
    assert result.warnings == ("peer warning", "Peer Hub returned stale cached data.")
    url, timeout, headers = calls[0]
    query = parse_qs(urlparse(url).query)
    assert query["asset_type"] == ["stock"]
    assert query["adjust"] == ["qfq"]
    assert timeout == 7
    assert headers["x-data-hub-hop"] == "1"
    assert headers["x-data-hub-key"] == "peer-secret"


def test_remote_provider_parses_delayed_quote() -> None:
    def opener(_: Request, *, timeout: float) -> JsonResponse:
        assert 90 < timeout <= 95
        return JsonResponse(
            {
                "data": {
                    "symbol": "00700.HK",
                    "price": "560.5",
                    "ts": "2026-07-17T16:00:00+08:00",
                    "market": "HK",
                    "freshness": "delayed",
                },
                "metadata": _metadata(),
            }
        )

    provider = RemoteHubProvider("https://overseas.example", opener=opener)
    result = provider.fetch_quote("00700.HK")

    assert result.value.price == Decimal("560.5")
    assert result.value.market == Market.HK
    assert result.value.freshness == QuoteFreshness.DELAYED


def test_remote_provider_collects_all_instrument_pages() -> None:
    offsets: list[int] = []

    def opener(request: Request, *, timeout: float) -> JsonResponse:
        assert 90 < timeout <= 95
        offset = int(parse_qs(urlparse(request.full_url).query)["offset"][0])
        offsets.append(offset)
        items = (
            [
                {
                    "symbol": "600519.SH",
                    "name": "Kweichow Moutai",
                    "market": "CN",
                    "asset_type": "stock",
                },
                {
                    "symbol": "510300.SH",
                    "name": "CSI 300 ETF",
                    "market": "CN",
                    "asset_type": "etf",
                },
            ]
            if offset == 0
            else [
                {
                    "symbol": "AAPL.US",
                    "name": "Apple",
                    "market": "US",
                    "asset_type": "stock",
                }
            ]
        )
        return JsonResponse(
            {
                "data": {"items": items, "total": 3, "offset": offset, "limit": 2000},
                "metadata": _metadata(),
            }
        )

    provider = RemoteHubProvider("https://peer.example", opener=opener)
    result = provider.fetch_instruments()

    assert offsets == [0, 2]
    assert [item.symbol for item in result.value] == ["600519.SH", "510300.SH", "AAPL.US"]
    assert result.value[1].asset_type == AssetType.ETF


def test_remote_provider_parses_reference_and_company_capabilities() -> None:
    def opener(request: Request, *, timeout: float) -> JsonResponse:
        assert 90 < timeout <= 95
        path = urlparse(request.full_url).path
        data_by_path: dict[str, object] = {
            "/trade-calendar": {
                "trade_dates": ["2026-07-16", "2026-07-17"],
                "start_date": "2026-01-01",
                "end_date": "2026-12-31",
                "fetched_at": "2026-07-17T16:00:00+08:00",
            },
            "/company-profile/600519.SH": {
                "symbol": "600519.SH",
                "industry": "Beverages",
                "main_business": "Spirits",
                "business_scope": None,
            },
            "/restricted-releases/600519.SH": [
                {
                    "symbol": "600519.SH",
                    "release_date": "2026-07-17",
                    "shares": "1000",
                    "market_value": "12345.6",
                    "ratio_of_total": "0.1",
                    "share_type": "IPO",
                }
            ],
            "/disclosures/600519.SH": [
                {
                    "symbol": "600519.SH",
                    "announce_date": "2026-07-17",
                    "title": "Interim report",
                }
            ],
            "/us-daily-closes/AAPL": [{"symbol": "AAPL", "day": "2026-07-17", "close": "212.5"}],
        }
        return JsonResponse({"data": data_by_path[path], "metadata": _metadata()})

    provider = RemoteHubProvider("https://peer.example", opener=opener)
    calendar = provider.fetch_trade_calendar()
    profile = provider.fetch_company_profile("600519.SH")
    releases = provider.fetch_restricted_releases("600519.SH")
    disclosures = provider.fetch_disclosures(
        "600519.SH",
        date(2026, 7, 1),
        date(2026, 7, 31),
    )
    closes = provider.fetch_us_daily_closes("AAPL", 10)

    assert len(calendar.value.trade_dates) == 2
    assert profile.value.industry == "Beverages"
    assert releases.value[0].shares == Decimal(1000)
    assert disclosures.value[0].title == "Interim report"
    assert closes.value[0].close == Decimal("212.5")


@pytest.mark.parametrize(
    "url",
    [
        "peer.example",
        "http://",
        "https://user:secret@peer.example",
        "https://peer.example?region=us",
        "https://peer.example#fragment",
    ],
)
def test_remote_provider_rejects_invalid_or_credentialed_urls(url: str) -> None:
    with pytest.raises(ValueError):
        RemoteHubProvider(url)


@pytest.mark.skipif(TestClient is None, reason="requires the optional service extra")
def test_forwarded_request_cannot_use_a_peer_or_peer_backed_cache() -> None:
    calls = 0

    def opener(_: Request, *, timeout: float) -> JsonResponse:
        nonlocal calls
        calls += 1
        assert 90 < timeout <= 95
        return JsonResponse(
            {
                "data": {
                    "symbol": "AAPL.US",
                    "price": "212.5",
                    "ts": "2026-07-17T16:00:00+08:00",
                    "market": "US",
                    "freshness": "delayed",
                },
                "metadata": _metadata(),
            }
        )

    peer = RemoteHubProvider("https://peer.example", name="peer", opener=opener)
    client = TestClient(create_app(DataHub([peer], attempts_per_provider=1)))

    regular = client.get("/quote/AAPL.US")
    forwarded = client.get("/quote/AAPL.US", headers={"X-Data-Hub-Hop": "1"})

    assert regular.status_code == 200
    assert regular.json()["metadata"]["source"] == "peer/yfinance:yahoo"
    assert forwarded.status_code == 503
    assert forwarded.json()["error"]["type"] == "NoProviderConfigured"
    assert calls == 1


def _bar_body():
    return {
        "data": [
            {
                "symbol": "600519.SH",
                "market": "CN",
                "ts": "2026-07-17T00:00:00+08:00",
                "open": "10",
                "high": "11",
                "low": "9",
                "close": "10.5",
                "volume": "1000",
                "amount": "10000",
                "is_closed": True,
            }
        ],
        "metadata": _metadata(),
    }


def _remote_bars(body, adjust=Adjust.NONE):
    provider = RemoteHubProvider(
        "https://peer.example", opener=lambda *args, **kwargs: JsonResponse(body)
    )
    day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)
    return provider.fetch_bars("600519.SH", Interval.DAY_1, day, day, adjust)


@pytest.mark.parametrize(
    "section,field,value,match",
    [
        ("metadata", "contract_version", "1.0", "contract 2.0"),
        ("metadata", "contract_version", None, "contract 2.0"),
        ("metadata", "fetched_at", None, "fetched_at"),
        ("metadata", "cache_hit", "false", "non-boolean"),
        ("data", "symbol", "000001.SZ", "different symbol"),
        ("data", "market", "US", "different symbol"),
        ("data", "is_closed", "false", "non-boolean"),
        ("data", "volume", "NaN", "invalid volume"),
        ("data", "ts", "2026-07-18T00:00:00+08:00", "outside"),
    ],
)
def test_remote_rejects_ambiguous_or_malformed_bar_contracts(section, field, value, match):
    body = _bar_body()
    target = body["data"][0] if section == "data" else body["metadata"]
    target[field] = value
    with pytest.raises(DataValidationError, match=match):
        _remote_bars(body)


def test_remote_preserves_nonpositive_adjusted_prices_and_rejects_duplicates():
    body = _bar_body()
    body["data"][0].update(open="-2", high="0", low="-3", close="-1")
    assert _remote_bars(body, Adjust.QFQ).value[0].close == Decimal(-1)
    with pytest.raises(DataValidationError):
        _remote_bars(body, Adjust.NONE)
    body["data"].append(deepcopy(body["data"][0]))
    with pytest.raises(DataValidationError, match="duplicate"):
        _remote_bars(body, Adjust.QFQ)


@pytest.mark.parametrize(
    "field,value",
    [
        ("volume_unit", "lots"),
        ("input_rows", -1),
        ("input_rows", True),
        ("returned_rows", 2),
        ("currency", "USD"),
        ("truncated", "false"),
    ],
)
def test_remote_rejects_quality_metadata_inconsistent_with_bars(field, value):
    body = _bar_body()
    quality = {
        "requested_start": "2026-07-17T00:00:00+08:00",
        "requested_end": "2026-07-17T00:00:00+08:00",
        "available_start": "2026-07-17T00:00:00+08:00",
        "available_end": "2026-07-17T00:00:00+08:00",
        "input_rows": 1,
        "returned_rows": 1,
        "volume_unit": "shares",
        "currency": "CNY",
    }
    body["metadata"]["quality"] = quality
    assert _remote_bars(body).quality.returned_rows == 1
    quality[field] = value
    with pytest.raises(DataValidationError):
        _remote_bars(body)


@pytest.mark.parametrize(
    "problem", ["empty_page", "changed_total", "changed_fetch", "repeat", "offset"]
)
def test_remote_pagination_rejects_missing_or_mixed_pages(problem):
    def opener(request, **kwargs):
        offset = int(parse_qs(urlparse(request.full_url).query)["offset"][0])
        data = {
            "items": [{"symbol": "600519.SH" if offset == 0 else "600000.SH", "name": "Example"}],
            "offset": offset,
            "total": 2,
        }
        metadata = _metadata()
        if offset:
            if problem == "empty_page":
                data["items"] = []
            elif problem == "changed_total":
                data["total"] = 3
            elif problem == "changed_fetch":
                metadata["fetched_at"] = "2026-07-17T16:00:01+08:00"
            elif problem == "repeat":
                data["items"][0]["symbol"] = "600519.SH"
            else:
                data["offset"] = 0
        return JsonResponse({"data": data, "metadata": metadata})

    provider = RemoteHubProvider("https://peer.example", opener=opener)
    with pytest.raises(DataValidationError):
        provider.fetch_instruments()


def test_hub_preserves_peer_fetch_time_and_marks_old_data_stale():
    body = _bar_body()
    original = datetime.now(tz=BEIJING_TZ) - timedelta(minutes=5)
    body["metadata"]["fetched_at"] = original.isoformat()
    body["metadata"]["cache_hit"] = True
    peer = RemoteHubProvider(
        "https://peer.example", opener=lambda *args, **kwargs: JsonResponse(body)
    )
    hub = DataHub([peer], attempts_per_provider=1)
    day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)
    result = asyncio.run(hub.get_bars("600519", "1d", day, day))
    assert result.metadata.fetched_at == original
    assert result.metadata.received_at > result.metadata.fetched_at
    assert result.metadata.age_seconds > 60
    assert result.metadata.cache_hit and result.metadata.stale
