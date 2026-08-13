from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest

try:
    from fastapi.testclient import TestClient
except ModuleNotFoundError:
    TestClient = None

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
