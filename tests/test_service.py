from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest

try:
    from fastapi.testclient import TestClient
except ModuleNotFoundError:
    TestClient = None

from stock_data_hub.errors import ProviderUnavailable
from stock_data_hub.hub import DataHub
from stock_data_hub.models import Capability, Market, Quote, SourcePayload
from stock_data_hub.normalization import BEIJING_TZ, market_from_symbol
from stock_data_hub.providers.base import MarketDataProvider
from stock_data_hub.service import create_app

pytestmark = pytest.mark.skipif(TestClient is None, reason="requires the optional service extra")


class ServiceQuoteProvider(MarketDataProvider):
    name = "service-test"
    capabilities = frozenset({Capability.QUOTE})

    def fetch_quote(self, symbol: str) -> SourcePayload[Quote]:
        return SourcePayload(
            value=Quote(
                symbol=symbol,
                price=Decimal("123.45"),
                ts=datetime(2026, 7, 17, 10, 0, tzinfo=BEIJING_TZ),
                market=market_from_symbol(symbol),
            ),
            source="service-test:quote",
        )


def test_quote_service_returns_json_safe_result_and_metadata() -> None:
    app = create_app(DataHub([ServiceQuoteProvider()], attempts_per_provider=1))
    response = TestClient(app).get("/quote/600519")

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["symbol"] == "600519.SH"
    assert payload["data"]["price"] == "123.45"
    assert payload["data"]["ts"].endswith("+08:00")
    assert payload["metadata"]["source"] == "service-test:quote"
    assert payload["metadata"]["stale"] is False


def test_service_api_key_protects_health_and_data_routes() -> None:
    app = create_app(
        DataHub([ServiceQuoteProvider()], attempts_per_provider=1),
        api_key="service-secret",
    )
    client = TestClient(app)

    missing = client.get("/health")
    invalid = client.get("/quote/600519", headers={"X-Data-Hub-Key": "wrong"})
    allowed = client.get(
        "/quote/600519",
        headers={"X-Data-Hub-Key": "service-secret"},
    )

    assert missing.status_code == 401
    assert invalid.status_code == 401
    assert allowed.status_code == 200


def test_blank_service_api_key_keeps_local_development_open() -> None:
    app = create_app(
        DataHub([ServiceQuoteProvider()], attempts_per_provider=1),
        api_key="  ",
    )

    assert TestClient(app).get("/health").status_code == 200


def test_service_exposes_all_hub_capabilities_as_routes() -> None:
    app = create_app(DataHub([ServiceQuoteProvider()], attempts_per_provider=1))
    paths = {route.path for route in app.routes}

    assert {
        "/health",
        "/quote/{symbol}",
        "/bars/{symbol}",
        "/snapshot",
        "/instruments",
        "/trade-calendar",
        "/company-profile/{symbol}",
        "/restricted-releases/{symbol}",
        "/disclosures/{symbol}",
        "/us-daily-closes/{ticker}",
    } <= paths


def test_quote_service_accepts_a_global_market_argument() -> None:
    app = create_app(DataHub([ServiceQuoteProvider()], attempts_per_provider=1))
    response = TestClient(app).get("/quote/AAPL?market=US")

    assert response.status_code == 200
    assert response.json()["data"]["symbol"] == "AAPL.US"
    assert response.json()["data"]["market"] == Market.US.value


def test_service_preserves_validation_and_availability_status_codes() -> None:
    configured = create_app(DataHub([ServiceQuoteProvider()], attempts_per_provider=1))
    invalid = TestClient(configured).get("/company-profile/not-a-symbol")

    unavailable = create_app(DataHub())
    missing = TestClient(unavailable).get("/quote/600519")

    assert invalid.status_code == 422
    assert invalid.json()["error"]["type"] == "InvalidRequest"
    assert missing.status_code == 503
    assert missing.json()["error"]["type"] == "NoProviderConfigured"


def test_us_daily_close_service_rejects_an_invalid_ticker() -> None:
    app = create_app(DataHub([ServiceQuoteProvider()], attempts_per_provider=1))

    response = TestClient(app).get("/us-daily-closes/not%2Fa%2Fticker")

    assert response.status_code in {404, 422}


def test_batch_quotes_deduplicate_aliases_and_keep_per_symbol_errors() -> None:
    class BatchProvider(ServiceQuoteProvider):
        def __init__(self):
            self.calls = []

        def fetch_quote(self, symbol):
            self.calls.append(symbol)
            if symbol == "600000.SH":
                raise ProviderUnavailable("quote unavailable")
            return super().fetch_quote(symbol)

    provider = BatchProvider()
    client = TestClient(create_app(DataHub([provider], attempts_per_provider=1)))
    response = client.post("/quotes", json={"symbols": ["600519", "600519.SH", "600000", "???"]})
    assert response.status_code == 200
    items = response.json()["data"]
    assert [item["symbol"] for item in items] == ["600519.SH", "600000.SH", "???"]
    assert items[0]["data"]["price"] == "123.45"
    assert items[0]["metadata"]["contract_version"] == "2.0"
    assert items[0]["error"] is None
    assert items[1]["data"] is None
    assert items[1]["error"]["type"] == "AllSourcesFailed"
    assert items[2]["error"]["type"] == "InvalidRequest"
    assert sorted(provider.calls) == ["600000.SH", "600519.SH"]


def test_batch_limits_authentication_and_metrics() -> None:
    hub = DataHub([ServiceQuoteProvider()], attempts_per_provider=1)
    client = TestClient(create_app(hub, api_key="secret"))
    headers = {"X-Data-Hub-Key": "secret"}
    assert client.post("/quotes", json={"symbols": ["600519"]}).status_code == 401
    assert client.get("/metrics").status_code == 401
    for symbols in ([], ["600519"] * 101):
        assert client.post("/quotes", json={"symbols": symbols}, headers=headers).status_code == 422
    for _ in range(2):
        assert (
            client.post("/quotes", json={"symbols": ["600519"]}, headers=headers).status_code == 200
        )
    metrics = client.get("/metrics", headers=headers)
    assert metrics.status_code == 200
    assert metrics.json()["data"]["requests"] == 2
    assert metrics.json()["data"]["cache_hits"] == 1
    health = client.get("/health", headers=headers).json()["data"][0]
    assert health["timeout_count"] == health["capacity_rejections"] == 0


def test_batch_forwarded_requests_disable_peer_routing() -> None:
    provider = ServiceQuoteProvider()
    provider.is_peer = True
    client = TestClient(create_app(DataHub([provider], attempts_per_provider=1)))
    response = client.post("/quotes", json={"symbols": ["600519"]}, headers={"X-Data-Hub-Hop": "1"})
    assert response.status_code == 200
    assert response.json()["data"][0]["error"]["type"] == "NoProviderConfigured"


def test_openapi_describes_data_metadata_decimal_and_dynamic_factor_columns() -> None:
    client = TestClient(create_app(DataHub()))
    document = client.get("/openapi.json").json()
    components = document["components"]["schemas"]

    def resolve(schema):
        return components[schema["$ref"].rsplit("/", 1)[-1]] if "$ref" in schema else schema

    def response_schema(path, method="get"):
        return resolve(
            document["paths"][path][method]["responses"]["200"]["content"]["application/json"][
                "schema"
            ]
        )

    quote = response_schema("/quote/{symbol}")
    quote_data = resolve(quote["properties"]["data"])
    assert quote_data["properties"]["price"]["type"] == "string"
    assert "metadata" in quote["required"]
    metadata = resolve(quote["properties"]["metadata"])
    assert {"fetched_at", "received_at", "age_seconds", "quality", "contract_version"} <= metadata[
        "properties"
    ].keys()
    bars = response_schema("/bars/{symbol}")["properties"]["data"]
    assert bars["type"] == "array"
    assert "is_closed" in resolve(bars["items"])["properties"]
    batch = response_schema("/quotes", "post")["properties"]["data"]
    assert {"symbol", "data", "metadata", "error"} <= resolve(batch["items"])["properties"].keys()
    frame = resolve(response_schema("/indicator-computations", "post")["properties"]["data"])
    row = resolve(frame["properties"]["rows"]["items"])
    assert {"ts", "is_closed"} <= row["properties"].keys()
    assert {item["type"] for item in row["additionalProperties"]["anyOf"]} == {"string", "null"}
