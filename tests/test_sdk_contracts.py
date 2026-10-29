"""Exercise installed SDK request builders/decoders with offline transports."""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from stock_data_hub.models import Adjust, Interval
from stock_data_hub.normalization import BEIJING_TZ
from stock_data_hub.providers.efinance import EFinanceProvider
from stock_data_hub.providers.tickflow import TickFlowProvider

_SAMPLES = json.loads(
    (Path(__file__).parent / "fixtures/provider_bars.json").read_text(encoding="utf-8")
)


def test_installed_efinance_sends_exact_exchange_ids_without_symbol_lookup(monkeypatch):
    sdk = pytest.importorskip("efinance")
    from efinance.common import config, getter

    history_ids, quote_ids = [], []

    def get(url, *, params, **kwargs):
        query = dict(params)
        if url.endswith("/stock/kline/get"):
            secid = query["secid"]
            history_ids.append(secid)
            price = 4000 if secid.startswith("1.") else 10
            body = {
                "data": {
                    "name": "Example",
                    "klines": [
                        f"2026-07-17,{price},{price},{price + 1},{price - 1},10,10000,1,0,0,1"
                    ],
                }
            }
        else:
            assert url.endswith("/ulist.np/get"), "The SDK must not perform a name/code lookup"
            secid = query["secids"]
            quote_ids.append(secid)
            fields = {name: field for field, name in config.EASTMONEY_QUOTE_FIELDS.items()}
            row = dict.fromkeys(config.EASTMONEY_QUOTE_FIELDS, 0)
            for name, value in {
                "代码": "000001",
                "名称": "Example",
                "市场编号": int(secid.split(".")[0]),
                "最新价": 4000 if secid.startswith("1.") else 10,
                "更新时间戳": 1784257200,
                "最新交易日": 20260717,
            }.items():
                row[fields[name]] = value
            body = {"data": {"diff": [row]}}
        return SimpleNamespace(json=lambda: body)

    monkeypatch.setattr(getter.session, "get", get)
    provider = EFinanceProvider(client_loader=lambda: sdk)
    day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)
    for symbol, expected in (("000001.SH", Decimal(4000)), ("000001.SZ", Decimal(10))):
        bars = provider.fetch_bars(symbol, Interval.DAY_1, day, day, Adjust.NONE)
        quote = provider.fetch_quote(symbol)
        assert bars.value[0].close == quote.value.price == expected
        assert bars.value[0].symbol == quote.value.symbol == symbol
        assert bars.value[0].volume == 1000
    assert history_ids == quote_ids == ["1.000001", "0.000001"]


@pytest.mark.parametrize("sample", _SAMPLES["tickflow"], ids=lambda sample: sample["symbol"])
def test_installed_tickflow_preserves_recorded_units_dates_and_request_bounds(sample):
    pytest.importorskip("tickflow")
    from tickflow.resources.klines import Klines

    compact = {key: value for key, value in sample["frame"].items() if key != "trade_date"}
    requests = []

    class Transport:
        def get(self, path, *, params):
            assert path == "/v1/klines"
            requests.append(params)
            return {"data": compact}

    closed = []
    client = SimpleNamespace(klines=Klines(Transport()), close=lambda: closed.append(True))
    day = datetime(2026, 7, 17, tzinfo=BEIJING_TZ)
    result = TickFlowProvider(client_factory=lambda: client).fetch_bars(
        sample["symbol"], Interval.DAY_1, day, day, Adjust.NONE
    )
    assert result.value[0].ts == day
    assert result.value[0].volume == Decimal(sample["expected_volume"])
    assert requests[0]["start_time"] <= compact["timestamp"][0] <= requests[0]["end_time"]
    assert requests[0]["adjust"] == "none"
    assert closed == [True]
