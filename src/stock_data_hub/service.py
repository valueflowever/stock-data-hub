"""Optional HTTP/OpenAPI facade for ``DataHub``.

The core package stays framework-free.  Install the ``service`` extra only
when another process, language, or AI tool needs to call it over HTTP.
"""

from __future__ import annotations

import os
import secrets
from collections.abc import Sequence
from datetime import date, datetime
from typing import Any, TypeVar

from stock_data_hub.errors import (
    AllSourcesFailed,
    DataHubError,
    InvalidRequest,
    NoProviderConfigured,
    ProviderError,
)
from stock_data_hub.hub import DataHub
from stock_data_hub.models import (
    Adjust,
    AssetType,
    DataResult,
    Instrument,
    Interval,
    Market,
    SnapshotRow,
)
from stock_data_hub.providers.defaults import create_default_hub
from stock_data_hub.serialization import to_jsonable

T = TypeVar("T", Instrument, SnapshotRow)


def create_app(hub: DataHub | None = None, *, api_key: str | None = None) -> Any:
    """Create a small FastAPI application around a hub instance.

    Passing a hub is useful for hosts that configure alternate providers.  If
    omitted, the service uses the default provider composition. Set ``api_key``
    or ``STOCK_DATA_HUB_API_KEY`` to protect every data and health endpoint.
    """

    try:
        from fastapi import Depends, FastAPI, Header, HTTPException, Query, Security
        from fastapi.responses import JSONResponse
        from fastapi.security import APIKeyHeader
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "HTTP service support is optional. Install stock-data-hub[service]."
        ) from exc

    data_hub = hub or create_default_hub()
    configured_api_key = _clean_secret(
        api_key if api_key is not None else os.getenv("STOCK_DATA_HUB_API_KEY")
    )
    api_key_header = APIKeyHeader(name="X-Data-Hub-Key", auto_error=False)

    async def require_api_key(
        provided: str | None = Security(api_key_header),
    ) -> None:
        if configured_api_key is None:
            return
        if provided is None or not secrets.compare_digest(provided, configured_api_key):
            raise HTTPException(status_code=401, detail="Missing or invalid data hub key")

    app = FastAPI(
        title="Stock Data Hub",
        version="0.5.0",
        description="Provider-neutral market-data access with source and cache metadata.",
        dependencies=[Depends(require_api_key)],
    )
    app.state.data_hub = data_hub
    app.state.api_key_required = configured_api_key is not None

    @app.exception_handler(DataHubError)
    async def handle_data_hub_error(_: Any, exc: DataHubError) -> Any:
        if isinstance(exc, InvalidRequest):
            status_code = 422
        elif isinstance(exc, (AllSourcesFailed, NoProviderConfigured)):
            status_code = 503
        elif isinstance(exc, ProviderError):
            status_code = 502
        else:
            status_code = 500
        error: dict[str, Any] = {"type": type(exc).__name__, "message": str(exc)}
        if isinstance(exc, AllSourcesFailed):
            error["sources"] = exc.errors
        return JSONResponse(status_code=status_code, content={"error": error})

    @app.exception_handler(ValueError)
    async def handle_value_error(_: Any, exc: ValueError) -> Any:
        return JSONResponse(
            status_code=422,
            content={"error": {"type": type(exc).__name__, "message": str(exc)}},
        )

    @app.get("/health", tags=["service"])
    async def health() -> dict[str, Any]:
        return {"data": to_jsonable(data_hub.health())}

    @app.get("/quote/{symbol}", tags=["market"])
    async def quote(
        symbol: str,
        market: Market | None = None,
        asset_type: AssetType | None = None,
        data_hub_hop: int = Header(default=0, alias="X-Data-Hub-Hop", ge=0),
    ) -> dict[str, Any]:
        return _result_body(
            await data_hub.get_quote(
                symbol,
                market,
                asset_type,
                allow_peers=data_hub_hop == 0,
            )
        )

    @app.get("/bars/{symbol}", tags=["market"])
    async def bars(
        symbol: str,
        start: datetime,
        end: datetime,
        interval: Interval = Interval.DAY_1,
        adjust: Adjust = Adjust.QFQ,
        market: Market | None = None,
        asset_type: AssetType | None = None,
        data_hub_hop: int = Header(default=0, alias="X-Data-Hub-Hop", ge=0),
    ) -> dict[str, Any]:
        return _result_body(
            await data_hub.get_bars(
                symbol,
                interval,
                start,
                end,
                adjust,
                market,
                asset_type,
                allow_peers=data_hub_hop == 0,
            )
        )

    @app.get("/snapshot", tags=["market"])
    async def snapshot(
        search: str | None = None,
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=200, ge=1, le=2000),
        data_hub_hop: int = Header(default=0, alias="X-Data-Hub-Hop", ge=0),
    ) -> dict[str, Any]:
        result = await data_hub.get_snapshot(allow_peers=data_hub_hop == 0)
        rows, total = _page(result.value.rows, search, offset, limit)
        data = to_jsonable(result.value)
        data["rows"] = to_jsonable(rows)
        data["total_rows"] = total
        data["offset"] = offset
        data["limit"] = limit
        return _result_body(result, data=data)

    @app.get("/instruments", tags=["reference"])
    async def instruments(
        search: str | None = None,
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=200, ge=1, le=2000),
        data_hub_hop: int = Header(default=0, alias="X-Data-Hub-Hop", ge=0),
    ) -> dict[str, Any]:
        result = await data_hub.get_instruments(allow_peers=data_hub_hop == 0)
        items, total = _page(result.value, search, offset, limit)
        data = {
            "items": to_jsonable(items),
            "total": total,
            "offset": offset,
            "limit": limit,
        }
        return _result_body(result, data=data)

    @app.get("/trade-calendar", tags=["reference"])
    async def trade_calendar(
        start: date | None = None,
        end: date | None = None,
        data_hub_hop: int = Header(default=0, alias="X-Data-Hub-Hop", ge=0),
    ) -> dict[str, Any]:
        if start is not None and end is not None and start > end:
            raise ValueError("start must be before end")
        result = await data_hub.get_trade_calendar(allow_peers=data_hub_hop == 0)
        dates = sorted(
            trade_date
            for trade_date in result.value.trade_dates
            if (start is None or trade_date >= start) and (end is None or trade_date <= end)
        )
        data = to_jsonable(result.value)
        data["trade_dates"] = to_jsonable(dates)
        data["count"] = len(dates)
        return _result_body(result, data=data)

    @app.get("/company-profile/{symbol}", tags=["company"])
    async def company_profile(
        symbol: str,
        data_hub_hop: int = Header(default=0, alias="X-Data-Hub-Hop", ge=0),
    ) -> dict[str, Any]:
        return _result_body(
            await data_hub.get_company_profile(symbol, allow_peers=data_hub_hop == 0)
        )

    @app.get("/restricted-releases/{symbol}", tags=["company"])
    async def restricted_releases(
        symbol: str,
        data_hub_hop: int = Header(default=0, alias="X-Data-Hub-Hop", ge=0),
    ) -> dict[str, Any]:
        return _result_body(
            await data_hub.get_restricted_releases(symbol, allow_peers=data_hub_hop == 0)
        )

    @app.get("/disclosures/{symbol}", tags=["company"])
    async def disclosures(
        symbol: str,
        start: date,
        end: date,
        data_hub_hop: int = Header(default=0, alias="X-Data-Hub-Hop", ge=0),
    ) -> dict[str, Any]:
        return _result_body(
            await data_hub.get_disclosures(
                symbol,
                start,
                end,
                allow_peers=data_hub_hop == 0,
            )
        )

    @app.get("/us-daily-closes/{ticker}", tags=["market"])
    async def us_daily_closes(
        ticker: str,
        max_rows: int = Query(default=80, ge=1, le=5000),
        data_hub_hop: int = Header(default=0, alias="X-Data-Hub-Hop", ge=0),
    ) -> dict[str, Any]:
        return _result_body(
            await data_hub.get_us_daily_closes(
                ticker,
                max_rows,
                allow_peers=data_hub_hop == 0,
            )
        )

    return app


def _result_body(result: DataResult[Any], *, data: Any | None = None) -> dict[str, Any]:
    return {
        "data": to_jsonable(result.value if data is None else data),
        "metadata": to_jsonable(result.metadata),
    }


def _page(
    items: Sequence[T],
    search: str | None,
    offset: int,
    limit: int,
) -> tuple[tuple[T, ...], int]:
    query = search.strip().casefold() if search else ""
    filtered = tuple(
        item
        for item in items
        if not query or query in item.symbol.casefold() or query in item.name.casefold()
    )
    return filtered[offset : offset + limit], len(filtered)


def _clean_secret(value: str | None) -> str | None:
    cleaned = str(value or "").strip()
    return cleaned or None
