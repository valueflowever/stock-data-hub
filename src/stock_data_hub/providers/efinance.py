"""EFinance adapter for Chinese bars and Eastmoney-backed market snapshots."""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from typing import Any

import pandas as pd

from stock_data_hub.cache import SharedFetchCache
from stock_data_hub.errors import DataValidationError, ProviderUnavailable
from stock_data_hub.models import (
    Adjust,
    AShareBar,
    AssetType,
    Capability,
    Instrument,
    Interval,
    Market,
    Quote,
    Snapshot,
    SnapshotRow,
    SourcePayload,
)
from stock_data_hub.normalization import (
    asset_type_from_symbol,
    normalize_a_share_symbol,
    normalize_symbol,
)
from stock_data_hub.providers._parsing import (
    IntradayTimestampLabel,
    bar_payload_from_dataframe,
    decimal_or_none,
    record_value,
    required_positive_decimal,
)
from stock_data_hub.providers.base import MarketDataProvider

_EASTMONEY_INTRADAY_TIMESTAMP_LABEL = IntradayTimestampLabel.CLOSE


class EFinanceProvider(MarketDataProvider):
    """Free Eastmoney-backed adapter for A-share/ETF bars and snapshots."""

    name = "efinance"
    markets = frozenset({Market.CN})
    asset_types = frozenset({AssetType.STOCK, AssetType.ETF, AssetType.INDEX})
    intervals = frozenset(Interval)
    adjustments = frozenset(Adjust)
    capabilities = frozenset(
        {Capability.BARS, Capability.QUOTE, Capability.SNAPSHOT, Capability.INSTRUMENTS}
    )
    accepts_asset_type = True

    def __init__(self, *, client_loader: Callable[[], Any] | None = None) -> None:
        self._client_loader = client_loader
        self._snapshots = SharedFetchCache()

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
        symbol = normalize_symbol(symbol, Market.CN)
        try:
            dataframe = self._client().stock.get_quote_history(
                _quote_id(symbol),
                beg=start.strftime("%Y%m%d"),
                end=end.strftime("%Y%m%d"),
                klt=_EFINANCE_PERIODS[interval],
                fqt=_EFINANCE_ADJUST[adjust],
                quote_id_mode=True,
            )
        except KeyError as exc:
            raise ProviderUnavailable(f"EFinance does not support {interval.value} bars") from exc
        except Exception as exc:
            raise ProviderUnavailable(f"EFinance could not obtain bars for {symbol}") from exc
        if dataframe is not None and not isinstance(dataframe, pd.DataFrame):
            raise DataValidationError("EFinance bars response was not a dataframe")
        _validate_history_symbol(dataframe, symbol)
        return bar_payload_from_dataframe(
            dataframe,
            source="efinance:eastmoney",
            symbol=symbol,
            interval=interval,
            start=start,
            end=end,
            market=Market.CN,
            time_columns=("日期", "date"),
            intraday_timestamp_label=_EASTMONEY_INTRADAY_TIMESTAMP_LABEL,
            adjust=adjust,
            volume_multiplier=Decimal(100),
        )

    def fetch_quote(
        self, symbol: str, *, asset_type: AssetType | None = None
    ) -> SourcePayload[Quote]:
        symbol = normalize_symbol(symbol, Market.CN)
        quote_id = _quote_id(symbol)
        # A precise secid avoids index/stock collisions and fetching thousands
        # of securities just to answer one quote request.
        fetched = self._snapshots.get(
            quote_id,
            lambda: self._client().stock.get_latest_quote([quote_id], quote_id_mode=True),
        )
        row = _find_quote_row(fetched.value, quote_id)
        return SourcePayload(
            value=Quote(
                symbol=symbol,
                price=required_positive_decimal(
                    record_value(row, "最新价", "price"), "latest price"
                ),
                ts=fetched.fetched_at,
                market=Market.CN,
            ),
            source="efinance:eastmoney",
            fetched_at=fetched.fetched_at,
            cache_hit=fetched.cache_hit,
        )

    def fetch_snapshot(self) -> SourcePayload[Snapshot]:
        fetched = self._snapshots.get("snapshot", self._realtime_dataframe)
        dataframe = fetched.value
        rows: list[SnapshotRow] = []
        for _, row in dataframe.iterrows():
            code = _six_digit_code(record_value(row, "股票代码", "代码", "code"))
            name = record_value(row, "股票名称", "名称", "name")
            if code is None or name is None:
                continue
            try:
                symbol = normalize_a_share_symbol(code)
            except DataValidationError:
                continue
            rows.append(
                SnapshotRow(
                    symbol=symbol,
                    name=str(name).strip(),
                    price=decimal_or_none(record_value(row, "最新价", "price")),
                    amount=decimal_or_none(record_value(row, "成交额", "amount")),
                    market_cap=decimal_or_none(record_value(row, "总市值", "market_cap")),
                    turnover_ratio=decimal_or_none(record_value(row, "换手率", "turnover_ratio")),
                    change_pct=decimal_or_none(record_value(row, "涨跌幅", "change_pct")),
                )
            )
        if not rows:
            raise DataValidationError("EFinance snapshot response had no usable rows")
        return SourcePayload(
            value=Snapshot(
                rows=tuple(sorted(rows, key=lambda item: item.symbol)),
                has_market_cap=True,
                fetched_at=fetched.fetched_at,
            ),
            source="efinance:eastmoney",
            fetched_at=fetched.fetched_at,
            cache_hit=fetched.cache_hit,
        )

    def fetch_instruments(self) -> SourcePayload[list[Instrument]]:
        snapshot_payload = self.fetch_snapshot()
        return SourcePayload(
            value=[
                Instrument(
                    symbol=row.symbol,
                    name=row.name,
                    market=Market.CN,
                    asset_type=asset_type_from_symbol(row.symbol),
                )
                for row in snapshot_payload.value.rows
            ],
            source=snapshot_payload.source,
            fetched_at=snapshot_payload.fetched_at,
            cache_hit=snapshot_payload.cache_hit,
        )

    def clear_cache(self) -> None:
        self._snapshots.clear()

    def _realtime_dataframe(self) -> pd.DataFrame:
        try:
            dataframe = self._client().stock.get_realtime_quotes()
        except Exception as exc:
            raise ProviderUnavailable("EFinance realtime snapshot is unavailable") from exc
        if not isinstance(dataframe, pd.DataFrame):
            raise DataValidationError("EFinance realtime response was not a dataframe")
        if dataframe.empty:
            raise ProviderUnavailable("EFinance returned an empty realtime snapshot")
        return dataframe

    def _client(self) -> Any:
        if self._client_loader is not None:
            return self._client_loader()
        try:
            import efinance
        except ModuleNotFoundError as exc:
            raise ProviderUnavailable(
                "EFinance is not installed. Install stock-data-hub[sources]."
            ) from exc
        return efinance


def _quote_id(symbol: str) -> str:
    code, exchange = symbol.rsplit(".", maxsplit=1)
    return f"{'1' if exchange == 'SH' else '0'}.{code}"


def _find_quote_row(dataframe: pd.DataFrame, quote_id: str) -> pd.Series:
    if not isinstance(dataframe, pd.DataFrame) or dataframe.empty:
        raise DataValidationError("EFinance quote response was empty or invalid")
    column = next((c for c in ("行情ID", "secid") if c in dataframe.columns), None)
    if column is None:
        raise DataValidationError("EFinance quote response has no unambiguous market ID")
    matches = dataframe[dataframe[column].astype(str).str.strip() == quote_id]
    if len(matches) != 1:
        raise DataValidationError(f"EFinance quote response has no unique row for {quote_id}")
    return matches.iloc[0]


def _validate_history_symbol(dataframe: pd.DataFrame | None, symbol: str) -> None:
    if dataframe is None or dataframe.empty:
        return
    code = symbol.split(".", maxsplit=1)[0]
    for column in ("股票代码", "代码", "code"):
        if column in dataframe.columns:
            actual = dataframe[column].astype(str).str.zfill(6)
            if not actual.eq(code).all():
                raise DataValidationError(
                    f"EFinance returned history for a different symbol: {symbol}"
                )
    for column in ("行情ID", "secid"):
        if column in dataframe.columns and not dataframe[column].eq(_quote_id(symbol)).all():
            raise DataValidationError("EFinance returned history from a different exchange")


def _six_digit_code(value: object | None) -> str | None:
    match = re.search(r"(\d{6})", str(value or ""))
    return match.group(1) if match else None


_EFINANCE_PERIODS = {
    Interval.MINUTE_15: 15,
    Interval.HOUR_1: 60,
    Interval.DAY_1: 101,
    Interval.WEEK_1: 102,
    Interval.MONTH_1: 103,
}

_EFINANCE_ADJUST = {
    Adjust.NONE: 0,
    Adjust.QFQ: 1,
    Adjust.HFQ: 2,
}
