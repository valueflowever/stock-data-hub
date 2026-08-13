"""EFinance adapter for Chinese bars and Eastmoney-backed market snapshots."""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import datetime
from typing import Any

import pandas as pd

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
from stock_data_hub.normalization import BEIJING_TZ, asset_type_from_symbol
from stock_data_hub.providers._parsing import (
    bars_from_dataframe,
    decimal_or_none,
    record_value,
    required_positive_decimal,
)
from stock_data_hub.providers.base import MarketDataProvider


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

    def __init__(self, *, client_loader: Callable[[], Any] | None = None) -> None:
        self._client_loader = client_loader

    def fetch_bars(
        self,
        symbol: str,
        interval: Interval,
        start: datetime,
        end: datetime,
        adjust: Adjust,
    ) -> SourcePayload[list[AShareBar]]:
        try:
            dataframe = self._client().stock.get_quote_history(
                _cn_code(symbol),
                beg=start.strftime("%Y%m%d"),
                end=end.strftime("%Y%m%d"),
                klt=_EFINANCE_PERIODS[interval],
                fqt=_EFINANCE_ADJUST[adjust],
                quote_symbol_mode=True,
            )
        except KeyError as exc:
            raise ProviderUnavailable(f"EFinance does not support {interval.value} bars") from exc
        except Exception as exc:
            raise ProviderUnavailable(f"EFinance could not obtain bars for {symbol}") from exc
        if dataframe is not None and not isinstance(dataframe, pd.DataFrame):
            raise DataValidationError("EFinance bars response was not a dataframe")
        return SourcePayload(
            value=bars_from_dataframe(
                dataframe,
                symbol=symbol,
                interval=interval,
                start=start,
                end=end,
                market=Market.CN,
                time_columns=("日期", "date"),
            ),
            source="efinance:eastmoney",
        )

    def fetch_quote(self, symbol: str) -> SourcePayload[Quote]:
        dataframe = self._realtime_dataframe()
        row = _find_code_row(dataframe, _cn_code(symbol))
        return SourcePayload(
            value=Quote(
                symbol=symbol,
                price=required_positive_decimal(
                    record_value(row, "最新价", "price"), "latest price"
                ),
                ts=datetime.now(tz=BEIJING_TZ),
                market=Market.CN,
            ),
            source="efinance:eastmoney",
        )

    def fetch_snapshot(self) -> SourcePayload[Snapshot]:
        dataframe = self._realtime_dataframe()
        rows: list[SnapshotRow] = []
        for _, row in dataframe.iterrows():
            code = _six_digit_code(record_value(row, "股票代码", "代码", "code"))
            name = record_value(row, "股票名称", "名称", "name")
            if code is None or name is None:
                continue
            suffix = (
                "SH"
                if code.startswith(("5", "6", "9"))
                else "BJ"
                if code.startswith(("4", "8"))
                else "SZ"
            )
            rows.append(
                SnapshotRow(
                    symbol=f"{code}.{suffix}",
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
                fetched_at=datetime.now(tz=BEIJING_TZ),
            ),
            source="efinance:eastmoney",
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
        )

    def _realtime_dataframe(self) -> pd.DataFrame:
        try:
            dataframe = self._client().stock.get_realtime_quotes()
        except Exception as exc:
            raise ProviderUnavailable("EFinance realtime snapshot is unavailable") from exc
        if dataframe is None or dataframe.empty:
            raise ProviderUnavailable("EFinance returned an empty realtime snapshot")
        if not isinstance(dataframe, pd.DataFrame):
            raise DataValidationError("EFinance realtime response was not a dataframe")
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


def _cn_code(symbol: str) -> str:
    return symbol.split(".", maxsplit=1)[0]


def _find_code_row(dataframe: pd.DataFrame, code: str) -> pd.Series:
    code_column = next(
        (column for column in ("股票代码", "代码", "code") if column in dataframe.columns),
        dataframe.columns[0],
    )
    matches = dataframe[
        dataframe[code_column].astype(str).str.extract(r"(\d{6})", expand=False) == code
    ]
    if matches.empty:
        raise DataValidationError(f"EFinance snapshot has no row for {code}")
    return matches.iloc[0]


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
