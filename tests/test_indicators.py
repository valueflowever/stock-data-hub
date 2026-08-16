from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from decimal import Decimal

import numpy as np
import pytest

try:
    from fastapi.testclient import TestClient
except ModuleNotFoundError:
    TestClient = None

from stock_data_hub.errors import (
    IndicatorBackendUnavailable,
    IndicatorNotFound,
    InvalidIndicatorRequest,
)
from stock_data_hub.indicators.backends.base import IndicatorBackend
from stock_data_hub.indicators.backends.talib import TaLibBackend
from stock_data_hub.indicators.catalog import DEFINITIONS_BY_ID, INDICATOR_DEFINITIONS
from stock_data_hub.indicators.contracts import IndicatorSpec
from stock_data_hub.indicators.engine import IndicatorEngine
from stock_data_hub.indicators.registry import IndicatorRegistry
from stock_data_hub.models import (
    Adjust,
    AShareBar,
    DataResult,
    FetchMetadata,
    Interval,
    Market,
)
from stock_data_hub.normalization import BEIJING_TZ
from stock_data_hub.service import create_app


class FixtureBackend(IndicatorBackend):
    name = "ta-lib"
    version = "fixture-1"

    def supports(self, indicator_id: str) -> bool:
        return indicator_id in DEFINITIONS_BY_ID

    def lookback(self, indicator_id: str, params: Mapping[str, int | float]) -> int:
        if indicator_id == "volume.obv":
            return 0
        if indicator_id == "trend.macd":
            return int(params["slow_period"]) + int(params["signal_period"]) - 2
        if indicator_id == "momentum.stoch":
            return (
                int(params["fast_k_period"])
                + int(params["slow_k_period"])
                + int(params["slow_d_period"])
                - 3
            )
        period = int(params.get("period", 1))
        if indicator_id in {"momentum.rsi", "volatility.atr", "momentum.roc"}:
            return period
        if indicator_id == "trend.adx":
            return 2 * period - 1
        return period - 1

    def compute(
        self,
        indicator_id: str,
        params: Mapping[str, int | float],
        inputs: Mapping[str, np.ndarray],
    ) -> Mapping[str, np.ndarray]:
        length = len(next(iter(inputs.values())))
        lookback = self.lookback(indicator_id, params)
        close = inputs["close"]
        definition = DEFINITIONS_BY_ID[indicator_id]
        results = {}
        for output_index, output in enumerate(definition.outputs):
            values = np.full(length, np.nan)
            values[lookback:] = close[lookback:] + output_index
            results[output.name] = values
        return results


class FixtureHub:
    def __init__(
        self,
        bars: Sequence[AShareBar],
        *,
        stale: bool = False,
        cache_hit: bool = False,
    ) -> None:
        self.bars = list(bars)
        self.stale = stale
        self.cache_hit = cache_hit
        self.calls = 0
        self.allow_peers: list[bool] = []

    async def get_bars(
        self,
        _symbol: str,
        _interval: Interval,
        start: datetime,
        end: datetime,
        _adjust: Adjust,
        _market: Market,
        _asset_type: object,
        *,
        allow_peers: bool = True,
    ) -> DataResult[list[AShareBar]]:
        self.calls += 1
        self.allow_peers.append(allow_peers)
        values = [bar for bar in self.bars if start <= bar.ts <= end]
        return DataResult(
            values,
            FetchMetadata(
                source="fixture:bars",
                fetched_at=datetime(2026, 8, 16, 10, tzinfo=BEIJING_TZ),
                latency_ms=1.5,
                attempted_sources=("fixture",),
                cache_hit=self.cache_hit,
                stale=self.stale,
                warnings=("fixture warning",) if self.stale else (),
            ),
        )


def _bars(
    count: int,
    *,
    unclosed_index: int | None = None,
    missing_volume: bool = False,
) -> list[AShareBar]:
    first = datetime(2024, 1, 1, tzinfo=BEIJING_TZ)
    result = []
    for index in range(count):
        close = Decimal(100) + Decimal(index) + Decimal(index % 5) / 10
        result.append(
            AShareBar(
                symbol="600519.SH",
                ts=first + timedelta(days=index),
                open=close - 1,
                high=close + 2,
                low=close - 2,
                close=close,
                volume=None if missing_volume else Decimal(1000 + index),
                amount=Decimal(100_000 + index),
                is_closed=index != unclosed_index,
                market=Market.CN,
            )
        )
    return result


def _engine(hub: FixtureHub) -> IndicatorEngine:
    registry = IndicatorRegistry(backends=[FixtureBackend()])
    return IndicatorEngine(hub, registry)


def test_catalog_is_curated_and_marks_backend_availability() -> None:
    registry = IndicatorRegistry(backends=[FixtureBackend()])

    catalog = registry.catalog()

    assert len(catalog) == 10
    assert {item.id for item in catalog} == set(DEFINITIONS_BY_ID)
    assert all(item.available and item.definition_version == "1.0.0" for item in catalog)
    assert DEFINITIONS_BY_ID["momentum.rsi"].variant == "wilder"


def test_catalog_remains_discoverable_without_optional_backend() -> None:
    registry = IndicatorRegistry()

    assert all(not item.available for item in registry.catalog())
    with pytest.raises(IndicatorBackendUnavailable, match="optional ta-lib"):
        registry.resolve([IndicatorSpec("momentum.rsi")])


def test_registry_validates_indicator_specs_and_column_names() -> None:
    registry = IndicatorRegistry(backends=[FixtureBackend()])
    resolved = registry.resolve([IndicatorSpec("momentum.rsi", params={"period": 7}, alias="rsi7")])

    assert resolved[0].execution.lookback == 7
    assert resolved[0].execution.outputs == {"value": "rsi7"}

    with pytest.raises(IndicatorNotFound):
        registry.definition("unknown")
    with pytest.raises(InvalidIndicatorRequest, match="Unknown parameter"):
        registry.resolve([IndicatorSpec("momentum.rsi", params={"window": 7})])
    with pytest.raises(InvalidIndicatorRequest, match="between"):
        registry.resolve([IndicatorSpec("momentum.rsi", params={"period": 1})])
    with pytest.raises(InvalidIndicatorRequest, match="fast_period"):
        registry.resolve(
            [
                IndicatorSpec(
                    "trend.macd",
                    params={"fast_period": 30, "slow_period": 20},
                )
            ]
        )
    with pytest.raises(InvalidIndicatorRequest, match="Duplicate"):
        registry.resolve(
            [
                IndicatorSpec("momentum.rsi", alias="same"),
                IndicatorSpec("overlap.sma", alias="same"),
            ]
        )
    with pytest.raises(InvalidIndicatorRequest, match="definition version"):
        registry.resolve([IndicatorSpec("momentum.rsi", definition_version="9.0.0")])
    with pytest.raises(InvalidIndicatorRequest, match="must be a number"):
        registry.resolve([IndicatorSpec("momentum.rsi", params={"period": True})])
    with pytest.raises(InvalidIndicatorRequest, match="finite number"):
        registry.resolve(
            [IndicatorSpec("volatility.bbands", params={"standard_deviations": float("nan")})]
        )


def test_engine_fetches_once_and_returns_a_reproducible_factor_frame() -> None:
    bars = _bars(80)
    hub = FixtureHub(bars, stale=True, cache_hit=True)
    engine = _engine(hub)
    start = bars[40].ts
    end = bars[49].ts

    result = asyncio.run(
        engine.calculate(
            "600519",
            Interval.DAY_1,
            start,
            end,
            [
                IndicatorSpec("momentum.rsi", params={"period": 5}, alias="rsi5"),
                IndicatorSpec("overlap.sma", params={"period": 20}, alias="sma20"),
            ],
            adjust=Adjust.NONE,
        )
    )

    assert hub.calls == 1
    assert result.value.symbol == "600519.SH"
    assert len(result.value.rows) == 10
    assert [column.name for column in result.value.columns] == [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "rsi5",
        "sma20",
    ]
    assert result.metadata.source == "fixture:bars"
    assert result.metadata.stale
    assert result.metadata.cache_hit
    assert "fixture warning" in result.metadata.warnings
    assert result.metadata.computation.warmup_rows_required == 19
    assert result.metadata.computation.warmup_rows_loaded == 19
    assert result.metadata.computation.input_rows == 29
    assert result.metadata.computation.input_fingerprint.startswith("sha256:")
    assert result.value.to_pandas().index.name == "ts"
    assert result.value.to_pandas()["rsi5"].notna().all()

    replay = asyncio.run(
        _engine(FixtureHub(bars)).calculate(
            "600519",
            Interval.DAY_1,
            start,
            end,
            [
                IndicatorSpec("momentum.rsi", params={"period": 5}, alias="rsi5"),
                IndicatorSpec("overlap.sma", params={"period": 20}, alias="sma20"),
            ],
            adjust=Adjust.NONE,
        )
    )
    assert replay.value == result.value
    assert (
        replay.metadata.computation.input_fingerprint
        == result.metadata.computation.input_fingerprint
    )


def test_another_indicator_does_not_change_existing_indicator_values() -> None:
    bars = _bars(80)
    start = bars[40].ts
    end = bars[49].ts
    rsi = IndicatorSpec("momentum.rsi", params={"period": 5}, alias="rsi5")

    single = asyncio.run(
        _engine(FixtureHub(bars)).calculate("600519", "1d", start, end, [rsi], adjust="none")
    )
    combined = asyncio.run(
        _engine(FixtureHub(bars)).calculate(
            "600519",
            "1d",
            start,
            end,
            [rsi, IndicatorSpec("overlap.sma", params={"period": 30}, alias="sma30")],
            adjust="none",
        )
    )

    assert [row.values["rsi5"] for row in single.value.rows] == [
        row.values["rsi5"] for row in combined.value.rows
    ]


def test_engine_reports_insufficient_history_and_supports_strict_mode() -> None:
    bars = _bars(8)
    start = bars[2].ts
    end = bars[-1].ts
    spec = IndicatorSpec("overlap.sma", params={"period": 5}, alias="sma5")

    partial = asyncio.run(
        _engine(FixtureHub(bars)).calculate("600519", "1d", start, end, [spec], adjust="none")
    )

    assert partial.metadata.computation.warmup_rows_required == 4
    assert partial.metadata.computation.warmup_rows_loaded == 2
    assert "Insufficient history" in partial.metadata.warnings[0]
    assert [row.values["sma5"] for row in partial.value.rows[:2]] == [None, None]

    with pytest.raises(InvalidIndicatorRequest, match="Insufficient history"):
        asyncio.run(
            _engine(FixtureHub(bars)).calculate(
                "600519",
                "1d",
                start,
                end,
                [spec],
                adjust="none",
                on_insufficient="error",
            )
        )


def test_engine_excludes_unclosed_bars_and_rejects_missing_volume() -> None:
    bars = _bars(50, unclosed_index=49)
    result = asyncio.run(
        _engine(FixtureHub(bars)).calculate(
            "600519",
            "1d",
            bars[40].ts,
            bars[49].ts,
            [IndicatorSpec("momentum.rsi", params={"period": 5})],
            adjust="qfq",
        )
    )

    assert len(result.value.rows) == 9
    assert any("Excluded 1 unclosed" in warning for warning in result.metadata.warnings)
    assert any("not point-in-time" in warning for warning in result.metadata.warnings)
    assert result.metadata.computation.adjustment_as_of is not None

    missing_volume = _bars(50, missing_volume=True)
    with pytest.raises(InvalidIndicatorRequest, match="non-null volume"):
        asyncio.run(
            _engine(FixtureHub(missing_volume)).calculate(
                "600519",
                "1d",
                missing_volume[20].ts,
                missing_volume[30].ts,
                [IndicatorSpec("volume.obv")],
                adjust="none",
            )
        )


def test_engine_enforces_output_budget() -> None:
    bars = _bars(50)
    hub = FixtureHub(bars)
    engine = IndicatorEngine(
        hub,
        IndicatorRegistry(backends=[FixtureBackend()]),
        max_output_cells=20,
    )

    with pytest.raises(InvalidIndicatorRequest, match="output exceeds"):
        asyncio.run(
            engine.calculate(
                "600519",
                "1d",
                bars[20].ts,
                bars[30].ts,
                [IndicatorSpec("momentum.rsi")],
                adjust="none",
            )
        )


def test_talib_backend_matches_catalog_lookbacks_and_known_sma() -> None:
    pytest.importorskip("talib")
    backend = TaLibBackend()

    for definition in INDICATOR_DEFINITIONS:
        params = {parameter.name: parameter.default for parameter in definition.parameters}
        assert backend.lookback(definition.id, params) == definition.default_lookback

    values = backend.compute(
        "overlap.sma",
        {"period": 3},
        {"close": np.asarray([1.0, 2.0, 3.0, 4.0])},
    )["value"]
    assert np.isnan(values[:2]).all()
    assert values[2:].tolist() == [2.0, 3.0]


def test_talib_backend_computes_every_catalog_output() -> None:
    pytest.importorskip("talib")
    backend = TaLibBackend()
    close = np.asarray([100 + index + (index % 7) / 10 for index in range(120)])
    inputs = {
        "open": close - 1,
        "high": close + 2,
        "low": close - 2,
        "close": close,
        "volume": np.asarray([10_000 + index for index in range(120)], dtype=float),
    }

    for definition in INDICATOR_DEFINITIONS:
        params = {parameter.name: parameter.default for parameter in definition.parameters}
        outputs = backend.compute(
            definition.id,
            params,
            {name: inputs[name] for name in definition.required_inputs},
        )
        assert set(outputs) == {output.name for output in definition.outputs}
        assert all(len(values) == len(close) for values in outputs.values())
        assert all(np.isfinite(values[-1]) for values in outputs.values())


def test_default_engine_builds_a_real_talib_factor_frame() -> None:
    pytest.importorskip("talib")
    bars = _bars(120)
    engine = IndicatorEngine(FixtureHub(bars))
    specs = [IndicatorSpec(definition.id) for definition in INDICATOR_DEFINITIONS]

    result = asyncio.run(
        engine.calculate(
            "600519",
            "1d",
            bars[60].ts,
            bars[70].ts,
            specs,
            adjust="none",
        )
    )

    assert len(result.value.rows) == 11
    factor_columns = [column for column in result.value.columns if column.role == "factor"]
    assert len(factor_columns) == sum(
        len(definition.outputs) for definition in INDICATOR_DEFINITIONS
    )
    assert all(value is not None for value in result.value.rows[0].values.values())
    assert {execution.backend for execution in result.metadata.computation.executions} == {"ta-lib"}


@pytest.mark.skipif(TestClient is None, reason="requires the optional service extra")
def test_indicator_http_catalog_and_computation_contract() -> None:
    bars = _bars(60)
    hub = FixtureHub(bars)
    engine = _engine(hub)
    app = create_app(hub, indicator_engine=engine)
    client = TestClient(app)

    catalog = client.get("/indicators?category=momentum")
    detail = client.get("/indicators/momentum.rsi")
    response = client.post(
        "/indicator-computations",
        headers={"X-Data-Hub-Hop": "1"},
        json={
            "symbol": "600519",
            "interval": "1d",
            "start": bars[30].ts.isoformat(),
            "end": bars[35].ts.isoformat(),
            "adjust": "none",
            "indicators": [
                {"id": "momentum.rsi", "alias": "rsi5", "params": {"period": 5}},
                {
                    "id": "trend.macd",
                    "alias": "macd",
                    "params": {"fast_period": 3, "slow_period": 6, "signal_period": 2},
                },
            ],
        },
    )

    assert catalog.status_code == 200
    assert catalog.json()["data"]["total"] == 3
    assert detail.status_code == 200
    assert detail.json()["data"]["item"]["variant"] == "wilder"
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]["symbol"] == "600519.SH"
    assert len(payload["data"]["rows"]) == 6
    assert payload["data"]["rows"][0]["close"] == "130"
    assert payload["data"]["rows"][0]["rsi5"] == "130.0"
    assert payload["metadata"]["source"] == "fixture:bars"
    assert payload["metadata"]["computation"]["catalog_version"] == "1.0.0"
    assert hub.calls == 1
    assert hub.allow_peers == [False]


@pytest.mark.skipif(TestClient is None, reason="requires the optional service extra")
def test_indicator_http_validation_and_error_mapping() -> None:
    bars = _bars(30)
    hub = FixtureHub(bars)
    client = TestClient(create_app(hub, indicator_engine=_engine(hub)))

    unknown = client.get("/indicators/not.registered")
    missing_adjust = client.post(
        "/indicator-computations",
        json={
            "symbol": "600519",
            "start": bars[10].ts.isoformat(),
            "end": bars[20].ts.isoformat(),
            "indicators": [{"id": "momentum.rsi"}],
        },
    )
    invalid = client.post(
        "/indicator-computations",
        json={
            "symbol": "600519",
            "start": bars[10].ts.isoformat(),
            "end": bars[20].ts.isoformat(),
            "adjust": "none",
            "indicators": [{"id": "momentum.rsi", "params": {"period": 1}}],
        },
    )

    assert unknown.status_code == 404
    assert unknown.json()["error"]["type"] == "IndicatorNotFound"
    assert missing_adjust.status_code == 422
    assert invalid.status_code == 422
    assert invalid.json()["error"]["type"] == "InvalidIndicatorRequest"

    unavailable_engine = IndicatorEngine(hub, IndicatorRegistry())
    unavailable_client = TestClient(create_app(hub, indicator_engine=unavailable_engine))
    unavailable = unavailable_client.post(
        "/indicator-computations",
        json={
            "symbol": "600519",
            "start": bars[10].ts.isoformat(),
            "end": bars[20].ts.isoformat(),
            "adjust": "none",
            "indicators": [{"id": "momentum.rsi"}],
        },
    )
    assert unavailable.status_code == 503
    assert unavailable.json()["error"]["type"] == "IndicatorBackendUnavailable"
