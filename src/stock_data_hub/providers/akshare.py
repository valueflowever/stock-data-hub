"""Adapter for the data capabilities already used by the host project.

The adapter has no dependency on the host project's source tree. It only uses
the public AkShare package and normalizes every response into this package's
contracts. Actual upstream selection is preserved in SourcePayload.source.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pandas as pd

from stock_data_hub.errors import DataValidationError, ProviderUnavailable
from stock_data_hub.models import (
    Adjust,
    AShareBar,
    AssetType,
    Capability,
    CompanyProfile,
    DailyClose,
    Disclosure,
    Instrument,
    Interval,
    Market,
    Quote,
    QuoteFreshness,
    RestrictedRelease,
    Snapshot,
    SnapshotRow,
    SourcePayload,
    TradeCalendar,
)
from stock_data_hub.normalization import (
    BEIJING_TZ,
    asset_type_from_symbol,
    market_from_symbol,
    normalize_a_share_symbol,
    normalize_symbol,
    prefixed_a_share_symbol,
    to_beijing,
)
from stock_data_hub.providers._parsing import (
    IntradayTimestampLabel,
    bars_from_dataframe,
    decimal_or_none,
    is_missing,
    record_value,
    required_positive_decimal,
    to_datetime,
)
from stock_data_hub.providers.base import MarketDataProvider


_EASTMONEY_INTRADAY_TIMESTAMP_LABEL = IntradayTimestampLabel.CLOSE
_SINA_INTRADAY_TIMESTAMP_LABEL = IntradayTimestampLabel.CLOSE


class AkShareProvider(MarketDataProvider):
    """Current-source adapter: Eastmoney, Sina, CNINFO through AkShare."""

    name = "akshare"
    markets = frozenset({Market.CN, Market.HK, Market.US})
    asset_types = frozenset({AssetType.STOCK, AssetType.ETF, AssetType.INDEX})
    intervals = frozenset(Interval)
    adjustments = frozenset(Adjust)
    accepts_asset_type = True
    capabilities = frozenset(
        {
            Capability.BARS,
            Capability.QUOTE,
            Capability.SNAPSHOT,
            Capability.INSTRUMENTS,
            Capability.TRADE_CALENDAR,
            Capability.COMPANY_PROFILE,
            Capability.RESTRICTED_RELEASES,
            Capability.DISCLOSURES,
            Capability.US_DAILY_CLOSES,
        }
    )

    def __init__(self, *, prefer_sina_snapshot: bool = False) -> None:
        self._prefer_sina_snapshot = prefer_sina_snapshot

    def supports_request(
        self,
        capability: Capability,
        *,
        market: Market | None = None,
        asset_type: AssetType | None = None,
        interval: Interval | None = None,
        adjust: Adjust | None = None,
    ) -> bool:
        if not super().supports_request(
            capability,
            market=market,
            asset_type=asset_type,
            interval=interval,
            adjust=adjust,
        ):
            return False
        if capability in {Capability.BARS, Capability.QUOTE}:
            if market in {Market.HK, Market.US} and asset_type not in {None, AssetType.STOCK}:
                return False
            if capability == Capability.BARS and market == Market.US:
                return interval in {Interval.DAY_1, Interval.WEEK_1, Interval.MONTH_1}
            return True
        return market in {None, Market.CN}

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
        ak = _load_akshare()
        normalized = normalize_symbol(symbol)
        market = market_from_symbol(normalized)
        normalized_asset_type = asset_type_from_symbol(normalized, asset_type)
        adjust_value = "" if adjust == Adjust.NONE else adjust.value
        try:
            dataframe = _fetch_market_bars(
                ak,
                normalized,
                normalized_asset_type,
                interval,
                start,
                end,
                adjust_value,
            )
            if dataframe is None or dataframe.empty:
                if _empty_bar_window_is_expected(start, end):
                    return SourcePayload(value=[], source="akshare:eastmoney")
                raise ProviderUnavailable(f"Eastmoney returned empty bars for {normalized}")
            bars = _parse_bars(
                dataframe,
                normalized,
                interval,
                start,
                end,
                market,
                intraday_timestamp_label=_EASTMONEY_INTRADAY_TIMESTAMP_LABEL,
            )
            if not bars:
                raise DataValidationError("Eastmoney returned no usable bars")
            return SourcePayload(value=bars, source="akshare:eastmoney")
        except Exception as primary_error:
            if market != Market.CN or normalized_asset_type != AssetType.STOCK:
                raise ProviderUnavailable(
                    f"AkShare could not obtain {interval.value} bars for {normalized}"
                ) from primary_error
            if interval in (Interval.WEEK_1, Interval.MONTH_1):
                raise ProviderUnavailable(
                    f"AkShare could not obtain {interval.value} A-share bars from Eastmoney"
                ) from primary_error
            try:
                dataframe = _fetch_sina_a_share_bars(
                    ak,
                    normalized,
                    interval,
                    start,
                    end,
                    adjust_value,
                )
                if dataframe is None or dataframe.empty:
                    if _empty_bar_window_is_expected(start, end):
                        return SourcePayload(
                            value=[],
                            source="akshare:sina",
                            warnings=("Eastmoney bars were unavailable; used Sina fallback.",),
                        )
                    raise ProviderUnavailable(f"Sina returned empty bars for {normalized}")
                bars = _parse_bars(
                    dataframe,
                    normalized,
                    interval,
                    start,
                    end,
                    Market.CN,
                    intraday_timestamp_label=_SINA_INTRADAY_TIMESTAMP_LABEL,
                )
                if not bars:
                    raise DataValidationError("Sina returned no usable bars")
                return SourcePayload(
                    value=bars,
                    source="akshare:sina",
                    warnings=("Eastmoney bars were unavailable; used Sina fallback.",),
                )
            except Exception as fallback_error:
                raise ProviderUnavailable(
                    "AkShare could not obtain A-share bars from Eastmoney or Sina"
                ) from fallback_error

    def fetch_quote(
        self,
        symbol: str,
        *,
        asset_type: AssetType | None = None,
    ) -> SourcePayload[Quote]:
        normalized = normalize_symbol(symbol)
        market = market_from_symbol(normalized)
        normalized_asset_type = asset_type_from_symbol(normalized, asset_type)
        code = normalized.split(".", maxsplit=1)[0]
        ak = _load_akshare()
        if market != Market.CN or normalized_asset_type != AssetType.STOCK:
            try:
                dataframe = _quote_dataframe(ak, normalized, normalized_asset_type)
                row = _find_market_row(dataframe, code, market)
                return SourcePayload(
                    value=Quote(
                        symbol=normalized,
                        price=required_positive_decimal(
                            record_value(row, "最新价", "price"), "latest price"
                        ),
                        ts=datetime.now(tz=BEIJING_TZ),
                        market=market,
                        freshness=(
                            QuoteFreshness.REALTIME
                            if market == Market.CN
                            else QuoteFreshness.DELAYED
                        ),
                    ),
                    source="akshare:eastmoney",
                    warnings=(
                        ("The upstream global-market quote may be delayed.",)
                        if market != Market.CN
                        else ()
                    ),
                )
            except Exception as exc:
                raise ProviderUnavailable(
                    f"AkShare could not obtain a quote for {normalized}"
                ) from exc
        try:
            row = _find_snapshot_row(ak.stock_zh_a_spot_em(), code)
            return SourcePayload(
                value=Quote(
                    symbol=normalized,
                    price=required_positive_decimal(
                        record_value(row, "最新价", "price"), "latest price"
                    ),
                    ts=datetime.now(tz=BEIJING_TZ),
                    market=Market.CN,
                ),
                source="akshare:eastmoney",
            )
        except Exception:  # noqa: BLE001 - an upstream failure triggers Sina fallback
            try:
                row = _find_snapshot_row(ak.stock_zh_a_spot(), code)
                return SourcePayload(
                    value=Quote(
                        symbol=normalized,
                        price=required_positive_decimal(
                            record_value(row, "最新价", "price"), "latest price"
                        ),
                        ts=datetime.now(tz=BEIJING_TZ),
                        market=Market.CN,
                    ),
                    source="akshare:sina",
                    warnings=("Eastmoney quote was unavailable; used Sina fallback.",),
                )
            except Exception as fallback_error:
                raise ProviderUnavailable(
                    f"AkShare could not obtain a realtime quote for {normalized}"
                ) from fallback_error

    def fetch_snapshot(self) -> SourcePayload[Snapshot]:
        ak = _load_akshare()
        if self._prefer_sina_snapshot:
            try:
                return SourcePayload(
                    value=_parse_snapshot(ak.stock_zh_a_spot(), has_market_cap=False),
                    source="akshare:sina",
                )
            except Exception:  # noqa: BLE001 - an upstream failure triggers fallback
                try:
                    return SourcePayload(
                        value=_parse_snapshot(ak.stock_zh_a_spot_em(), has_market_cap=True),
                        source="akshare:eastmoney",
                        warnings=("Sina snapshot was unavailable; used Eastmoney fallback.",),
                    )
                except Exception as fallback_error:
                    raise ProviderUnavailable(
                        "AkShare could not obtain an A-share market snapshot"
                    ) from fallback_error
        try:
            return SourcePayload(
                value=_parse_snapshot(ak.stock_zh_a_spot_em(), has_market_cap=True),
                source="akshare:eastmoney",
            )
        except Exception:  # noqa: BLE001 - an upstream failure triggers Sina fallback
            try:
                return SourcePayload(
                    value=_parse_snapshot(ak.stock_zh_a_spot(), has_market_cap=False),
                    source="akshare:sina",
                    warnings=("Eastmoney snapshot was unavailable; used Sina fallback.",),
                )
            except Exception as fallback_error:
                raise ProviderUnavailable(
                    "AkShare could not obtain an A-share market snapshot"
                ) from fallback_error

    def fetch_instruments(self) -> SourcePayload[list[Instrument]]:
        ak = _load_akshare()
        dataframe = ak.stock_info_a_code_name()
        if dataframe is None or dataframe.empty:
            raise ProviderUnavailable("AkShare returned an empty A-share instrument list")
        code_column = "code" if "code" in dataframe.columns else dataframe.columns[0]
        name_column = "name" if "name" in dataframe.columns else dataframe.columns[1]
        instruments: list[Instrument] = []
        for _, row in dataframe.iterrows():
            code = _six_digit_code(row.get(code_column))
            name = _clean_text(row.get(name_column))
            if not code or not name:
                continue
            try:
                instruments.append(Instrument(symbol=normalize_a_share_symbol(code), name=name))
            except DataValidationError:
                continue
        if not instruments:
            raise DataValidationError("AkShare instrument list had no usable A-share symbols")
        return SourcePayload(value=instruments, source="akshare:instrument_list")

    def fetch_trade_calendar(self) -> SourcePayload[TradeCalendar]:
        ak = _load_akshare()
        dataframe = ak.tool_trade_date_hist_sina()
        if dataframe is None or dataframe.empty or "trade_date" not in dataframe.columns:
            raise ProviderUnavailable("Sina trading calendar is unavailable")
        dates = {
            parsed
            for value in dataframe["trade_date"].dropna().tolist()
            if (parsed := _to_date(value)) is not None
        }
        if not dates:
            raise DataValidationError("Sina trading calendar had no usable dates")
        return SourcePayload(
            value=TradeCalendar(
                trade_dates=frozenset(dates),
                start_date=min(dates),
                end_date=max(dates),
                fetched_at=datetime.now(tz=BEIJING_TZ),
            ),
            source="akshare:sina",
        )

    def fetch_company_profile(self, symbol: str) -> SourcePayload[CompanyProfile]:
        normalized = normalize_a_share_symbol(symbol)
        ak = _load_akshare()
        dataframe = ak.stock_profile_cninfo(symbol=normalized.split(".", maxsplit=1)[0])
        if dataframe is None or dataframe.empty:
            raise ProviderUnavailable(f"CNINFO profile is unavailable for {normalized}")
        row = dataframe.iloc[0]
        industry = _clean_text(record_value(row, "所属行业", "industry"))
        main_business = _clean_text(record_value(row, "主营业务", "main_business"))
        business_scope = _clean_text(record_value(row, "经营范围", "business_scope"))
        if not industry or not (main_business or business_scope):
            raise DataValidationError(
                f"CNINFO profile lacks sufficient business fields for {normalized}"
            )
        return SourcePayload(
            value=CompanyProfile(
                symbol=normalized,
                industry=industry,
                main_business=main_business,
                business_scope=business_scope,
            ),
            source="akshare:cninfo",
        )

    def fetch_restricted_releases(self, symbol: str) -> SourcePayload[list[RestrictedRelease]]:
        normalized = normalize_a_share_symbol(symbol)
        code = normalized.split(".", maxsplit=1)[0]
        ak = _load_akshare()
        try:
            releases = _parse_releases_em(
                ak.stock_restricted_release_queue_em(symbol=code), normalized
            )
            # An empty release schedule is a valid answer.  Falling back in
            # that case can turn "no releases" into a misleading outage.
            return SourcePayload(value=releases, source="akshare:eastmoney")
        except Exception:  # noqa: BLE001 - an upstream failure triggers Sina fallback
            try:
                releases = _parse_releases_sina(
                    ak.stock_restricted_release_queue_sina(symbol=code), normalized
                )
                return SourcePayload(
                    value=releases,
                    source="akshare:sina",
                    warnings=("Eastmoney release data was unavailable; used Sina fallback.",),
                )
            except Exception as fallback_error:
                raise ProviderUnavailable(
                    f"AkShare could not obtain restricted-release data for {normalized}"
                ) from fallback_error

    def fetch_disclosures(
        self,
        symbol: str,
        start: date,
        end: date,
    ) -> SourcePayload[list[Disclosure]]:
        normalized = normalize_a_share_symbol(symbol)
        ak = _load_akshare()
        dataframe = ak.stock_zh_a_disclosure_report_cninfo(
            symbol=normalized.split(".", maxsplit=1)[0],
            market="沪深京",
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
        )
        if dataframe is None or dataframe.empty:
            return SourcePayload(value=[], source="akshare:cninfo")
        disclosures: list[Disclosure] = []
        for _, row in dataframe.iterrows():
            announce_date = _to_date(record_value(row, "公告时间", "announce_date"))
            title = _clean_text(record_value(row, "公告标题", "title"))
            if announce_date is None or not title:
                continue
            disclosures.append(
                Disclosure(symbol=normalized, announce_date=announce_date, title=title)
            )
        disclosures.sort(key=lambda item: (item.announce_date, item.title))
        return SourcePayload(value=disclosures, source="akshare:cninfo")

    def fetch_us_daily_closes(self, ticker: str, max_rows: int) -> SourcePayload[list[DailyClose]]:
        normalized = ticker.upper().strip()
        ak = _load_akshare()
        dataframe = ak.stock_us_daily(symbol=normalized)
        if dataframe is None or dataframe.empty:
            return SourcePayload(value=[], source="akshare:us_daily")
        closes: list[DailyClose] = []
        for _, row in dataframe.tail(max_rows).iterrows():
            day = _to_date(record_value(row, "date", "日期"))
            close = decimal_or_none(record_value(row, "close", "收盘"))
            if day is None or close is None or close <= 0:
                continue
            closes.append(DailyClose(symbol=normalized, day=day, close=close))
        closes.sort(key=lambda item: item.day)
        return SourcePayload(value=closes, source="akshare:us_daily")


def _fetch_market_bars(
    ak: Any,
    symbol: str,
    asset_type: AssetType,
    interval: Interval,
    start: datetime,
    end: datetime,
    adjust: str,
) -> pd.DataFrame:
    code, _ = symbol.split(".", maxsplit=1)
    market = market_from_symbol(symbol)
    period = {
        Interval.DAY_1: "daily",
        Interval.WEEK_1: "weekly",
        Interval.MONTH_1: "monthly",
    }.get(interval)
    if period is not None:
        common = {
            "symbol": code,
            "period": period,
            "start_date": start.strftime("%Y%m%d"),
            "end_date": end.strftime("%Y%m%d"),
        }
        if market == Market.HK:
            return ak.stock_hk_hist(**common, adjust=adjust)
        if market == Market.US:
            return ak.stock_us_hist(
                **{**common, "symbol": _eastmoney_us_symbol(ak, code)},
                adjust=adjust,
            )
        if asset_type == AssetType.ETF:
            return ak.fund_etf_hist_em(**common, adjust=adjust)
        if asset_type == AssetType.INDEX:
            return ak.index_zh_a_hist(**common)
        return ak.stock_zh_a_hist(**common, adjust=adjust)

    start_text = start.strftime("%Y-%m-%d %H:%M:%S")
    end_text = end.strftime("%Y-%m-%d %H:%M:%S")
    minute_period = "15" if interval == Interval.MINUTE_15 else "60"
    if market == Market.HK:
        return ak.stock_hk_hist_min_em(
            symbol=code,
            period=minute_period,
            adjust=adjust,
            start_date=start_text,
            end_date=end_text,
        )
    if market == Market.US:
        raise ProviderUnavailable("AkShare does not expose 15m or 1h US bars in this adapter")
    if asset_type == AssetType.ETF:
        return ak.fund_etf_hist_min_em(
            symbol=code,
            period=minute_period,
            adjust=adjust,
            start_date=start_text,
            end_date=end_text,
        )
    if asset_type == AssetType.INDEX:
        return ak.index_zh_a_hist_min_em(
            symbol=code,
            period=minute_period,
            start_date=start_text,
            end_date=end_text,
        )
    return ak.stock_zh_a_hist_min_em(
        symbol=code,
        start_date=start_text,
        end_date=end_text,
        period=minute_period,
        adjust=adjust,
    )


def _fetch_sina_a_share_bars(
    ak: Any,
    symbol: str,
    interval: Interval,
    start: datetime,
    end: datetime,
    adjust: str,
) -> pd.DataFrame:
    if interval == Interval.DAY_1:
        return ak.stock_zh_a_daily(
            symbol=prefixed_a_share_symbol(symbol),
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            adjust=adjust,
        )
    return ak.stock_zh_a_minute(
        symbol=prefixed_a_share_symbol(symbol),
        period="15" if interval == Interval.MINUTE_15 else "60",
        adjust=adjust,
    )


def _quote_dataframe(ak: Any, symbol: str, asset_type: AssetType) -> pd.DataFrame:
    market = market_from_symbol(symbol)
    if market == Market.HK:
        return ak.stock_hk_spot_em()
    if market == Market.US:
        return ak.stock_us_spot_em()
    if asset_type == AssetType.ETF:
        return ak.fund_etf_spot_em()
    if asset_type == AssetType.INDEX:
        series = "上证系列指数" if symbol.endswith(".SH") else "深证系列指数"
        return ak.stock_zh_index_spot_em(symbol=series)
    return ak.stock_zh_a_spot_em()


def _find_market_row(dataframe: pd.DataFrame | None, code: str, market: Market) -> pd.Series:
    if dataframe is None or dataframe.empty:
        raise DataValidationError("Quote response is empty")
    columns = [
        column
        for column in ("代码", "编码", "简称", "symbol", "code")
        if column in dataframe.columns
    ]
    if not columns:
        columns = [dataframe.columns[0]]
    target = code.upper()
    for column in columns:
        values = dataframe[column].astype(str).str.strip().str.upper()
        if market == Market.US:
            mask = (values == target) | values.str.endswith(f".{target}")
        elif market == Market.HK:
            mask = values.str.extract(r"(\d{1,5})", expand=False).str.zfill(5) == target.zfill(5)
        else:
            mask = values.str.extract(r"(\d{6})", expand=False) == target
        matches = dataframe[mask.fillna(False)]
        if not matches.empty:
            return matches.iloc[0]
    raise DataValidationError(f"Quote response has no row for {code}")


def _eastmoney_us_symbol(ak: Any, ticker: str) -> str:
    row = _find_market_row(ak.stock_us_spot_em(), ticker, Market.US)
    value = _clean_text(record_value(row, "代码", "code"))
    if value and re.fullmatch(r"\d+\.[A-Z0-9.-]+", value.upper()):
        return value.upper()
    encoding = _clean_text(record_value(row, "编码"))
    short_name = _clean_text(record_value(row, "简称"))
    if encoding and short_name and short_name.upper() == ticker.upper():
        return f"{encoding}.{short_name.upper()}"
    raise DataValidationError(f"Could not resolve Eastmoney's market code for {ticker}")


def _empty_bar_window_is_expected(start: datetime, end: datetime) -> bool:
    now = datetime.now(tz=BEIJING_TZ)
    effective_end = min(to_beijing(end), now)
    current = to_beijing(start).date()
    last = effective_end.date()
    if current > last:
        return True
    while current <= last:
        if current.weekday() < 5:
            return False
        current += timedelta(days=1)
    return True


def _load_akshare() -> Any:
    try:
        import akshare as ak
    except ModuleNotFoundError as exc:
        raise ProviderUnavailable(
            "AkShare is not installed. Install the stock-data-hub package dependencies."
        ) from exc
    return ak


def _parse_bars(
    dataframe: pd.DataFrame | None,
    symbol: str,
    interval: Interval,
    start: datetime,
    end: datetime,
    market: Market = Market.CN,
    *,
    intraday_timestamp_label: IntradayTimestampLabel | None = None,
) -> list[AShareBar]:
    return bars_from_dataframe(
        dataframe,
        symbol=symbol,
        interval=interval,
        start=start,
        end=end,
        market=market,
        time_columns=("时间", "日期", "date", "day"),
        open_columns=("开盘", "open"),
        high_columns=("最高", "high"),
        low_columns=("最低", "low"),
        close_columns=("收盘", "close"),
        volume_columns=("成交量", "volume"),
        amount_columns=("成交额", "amount"),
        intraday_timestamp_label=intraday_timestamp_label,
    )


def _find_snapshot_row(dataframe: pd.DataFrame | None, code: str) -> pd.Series:
    if dataframe is None or dataframe.empty:
        raise DataValidationError("Snapshot response is empty")
    code_column = "代码" if "代码" in dataframe.columns else dataframe.columns[0]
    matches = dataframe[
        dataframe[code_column].astype(str).str.extract(r"(\d{6})", expand=False) == code
    ]
    if matches.empty:
        raise DataValidationError(f"Snapshot has no row for {code}")
    return matches.iloc[0]


def _parse_snapshot(dataframe: pd.DataFrame | None, has_market_cap: bool) -> Snapshot:
    if dataframe is None or dataframe.empty:
        raise DataValidationError("Snapshot response is empty")
    rows: list[SnapshotRow] = []
    for _, row in dataframe.iterrows():
        code = _six_digit_code(record_value(row, "代码", "code"))
        name = _clean_text(record_value(row, "名称", "name"))
        if not code or not name:
            continue
        try:
            symbol = normalize_a_share_symbol(code)
        except DataValidationError:
            continue
        rows.append(
            SnapshotRow(
                symbol=symbol,
                name=name,
                price=decimal_or_none(record_value(row, "最新价", "price")),
                amount=decimal_or_none(record_value(row, "成交额", "amount")),
                market_cap=decimal_or_none(record_value(row, "总市值", "market_cap"))
                if has_market_cap
                else None,
                turnover_ratio=decimal_or_none(record_value(row, "换手率", "turnover_ratio"))
                if has_market_cap
                else None,
                change_pct=decimal_or_none(record_value(row, "涨跌幅", "change_pct")),
            )
        )
    if not rows:
        raise DataValidationError("Snapshot response has no usable rows")
    rows.sort(key=lambda item: item.symbol)
    return Snapshot(
        rows=tuple(rows),
        has_market_cap=has_market_cap,
        fetched_at=datetime.now(tz=BEIJING_TZ),
    )


def _parse_releases_em(
    dataframe: pd.DataFrame | None,
    symbol: str,
) -> list[RestrictedRelease]:
    if dataframe is None or dataframe.empty:
        return []
    releases: list[RestrictedRelease] = []
    for _, row in dataframe.iterrows():
        release_date = _to_date(record_value(row, "解禁时间", "release_date"))
        if release_date is None:
            continue
        releases.append(
            RestrictedRelease(
                symbol=symbol,
                release_date=release_date,
                shares=decimal_or_none(record_value(row, "解禁数量", "shares")),
                market_value=decimal_or_none(record_value(row, "实际解禁数量市值", "market_value")),
                ratio_of_total=decimal_or_none(record_value(row, "占总市值比例", "ratio_of_total")),
                share_type=_clean_text(record_value(row, "限售股类型", "share_type")),
            )
        )
    return sorted(releases, key=lambda item: item.release_date)


def _parse_releases_sina(
    dataframe: pd.DataFrame | None,
    symbol: str,
) -> list[RestrictedRelease]:
    if dataframe is None or dataframe.empty:
        return []
    releases: list[RestrictedRelease] = []
    for _, row in dataframe.iterrows():
        release_date = _to_date(record_value(row, "解禁日期", "release_date"))
        if release_date is None:
            continue
        shares_wan = decimal_or_none(record_value(row, "解禁数量", "shares"))
        value_yi = decimal_or_none(record_value(row, "解禁股流通市值", "market_value"))
        releases.append(
            RestrictedRelease(
                symbol=symbol,
                release_date=release_date,
                shares=shares_wan * Decimal(10000) if shares_wan is not None else None,
                market_value=value_yi * Decimal(100000000) if value_yi is not None else None,
                ratio_of_total=None,
                share_type=None,
            )
        )
    return sorted(releases, key=lambda item: item.release_date)


def _six_digit_code(value: object) -> str | None:
    match = re.search(r"(\d{6})", str(value or ""))
    return match.group(1) if match else None


def _to_date(value: object) -> date | None:
    timestamp = to_datetime(value)
    return timestamp.date() if timestamp is not None else None


def _clean_text(value: object) -> str | None:
    if is_missing(value):
        return None
    text = str(value).strip()
    return text or None
