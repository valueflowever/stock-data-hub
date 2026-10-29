"""Bounded batch quotes with an independent result for every symbol."""

from __future__ import annotations

from typing import Any

from fastapi import Header
from pydantic import BaseModel, ConfigDict, Field

from stock_data_hub.hub import DataHub
from stock_data_hub.models import AssetType, BatchQuoteItem, Market
from stock_data_hub.serialization import to_jsonable
from stock_data_hub.service_schemas import DataEnvelope


class QuoteBatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbols: list[str] = Field(min_length=1, max_length=100)
    market: Market | None = None
    asset_type: AssetType | None = None


def register_quote_routes(app: Any, hub: DataHub) -> None:
    @app.post("/quotes", tags=["market"], response_model=DataEnvelope[list[BatchQuoteItem]])
    async def quotes(
        request: QuoteBatchRequest,
        data_hub_hop: int = Header(default=0, alias="X-Data-Hub-Hop", ge=0),
    ) -> dict[str, Any]:
        results = await hub.get_quotes(
            request.symbols,
            request.market,
            request.asset_type,
            allow_peers=data_hub_hop == 0,
        )
        return {"data": to_jsonable(results)}
