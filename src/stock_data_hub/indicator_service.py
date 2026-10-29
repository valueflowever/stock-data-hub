"""FastAPI routes for the optional technical-indicator service."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from fastapi import Header, Query
from pydantic import BaseModel, ConfigDict, Field, StrictFloat, StrictInt

from stock_data_hub.indicators.catalog import CATALOG_VERSION
from stock_data_hub.indicators.contracts import IndicatorSpec
from stock_data_hub.indicators.engine import IndicatorEngine
from stock_data_hub.models import Adjust, AssetType, Interval, Market
from stock_data_hub.serialization import to_jsonable
from stock_data_hub.service_schemas import (
    DataEnvelope,
    IndicatorCatalogPage,
    IndicatorDefinitionData,
    IndicatorResponse,
)


class IndicatorSpecRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    params: dict[str, StrictInt | StrictFloat] = Field(default_factory=dict)
    alias: str | None = None
    definition_version: str | None = None

    def to_contract(self) -> IndicatorSpec:
        return IndicatorSpec(
            id=self.id,
            params=self.params,
            alias=self.alias,
            definition_version=self.definition_version,
        )


class IndicatorComputationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    interval: Interval = Interval.DAY_1
    start: datetime
    end: datetime
    adjust: Adjust
    market: Market | None = None
    asset_type: AssetType | None = None
    closed_only: bool = True
    include_inputs: bool = True
    on_insufficient: Literal["null", "error"] = "null"
    indicators: list[IndicatorSpecRequest] = Field(min_length=1, max_length=32)


def register_indicator_routes(app: Any, engine: IndicatorEngine) -> None:
    @app.get("/indicators", tags=["indicators"], response_model=DataEnvelope[IndicatorCatalogPage])
    async def indicator_catalog(
        search: str | None = None,
        category: str | None = None,
        available: bool | None = None,
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=200, ge=1, le=500),
    ) -> dict[str, Any]:
        query = str(search or "").strip().casefold()
        normalized_category = str(category or "").strip().casefold()
        definitions = [
            definition
            for definition in engine.catalog()
            if (
                not query
                or query in definition.id.casefold()
                or query in definition.name.casefold()
                or query in definition.description.casefold()
            )
            and (not normalized_category or definition.category.casefold() == normalized_category)
            and (available is None or definition.available is available)
        ]
        page = definitions[offset : offset + limit]
        return {
            "data": {
                "catalog_version": CATALOG_VERSION,
                "items": to_jsonable(page),
                "total": len(definitions),
                "offset": offset,
                "limit": limit,
            }
        }

    @app.get(
        "/indicators/{indicator_id}",
        tags=["indicators"],
        response_model=DataEnvelope[IndicatorDefinitionData],
    )
    async def indicator_definition(indicator_id: str) -> dict[str, Any]:
        return {
            "data": {
                "catalog_version": CATALOG_VERSION,
                "item": to_jsonable(engine.definition(indicator_id)),
            }
        }

    @app.post("/indicator-computations", tags=["indicators"], response_model=IndicatorResponse)
    async def indicator_computation(
        request: IndicatorComputationRequest,
        data_hub_hop: int = Header(default=0, alias="X-Data-Hub-Hop", ge=0),
    ) -> dict[str, Any]:
        result = await engine.calculate(
            request.symbol,
            request.interval,
            request.start,
            request.end,
            [item.to_contract() for item in request.indicators],
            adjust=request.adjust,
            market=request.market,
            asset_type=request.asset_type,
            closed_only=request.closed_only,
            include_inputs=request.include_inputs,
            on_insufficient=request.on_insufficient,
            allow_peers=data_hub_hop == 0,
        )
        data = to_jsonable(result.value)
        data["rows"] = to_jsonable(result.value.to_records())
        return {"data": data, "metadata": to_jsonable(result.metadata)}
