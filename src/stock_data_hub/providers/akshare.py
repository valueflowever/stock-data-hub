"""Adapter for the data capabilities already used by the host project.

The adapter has no dependency on the host project's source tree. It only uses
the public AkShare package and normalizes every response into this package's
contracts. Actual upstream selection is preserved in SourcePayload.source.
"""

from __future__ import annotations

import re
from contextlib import suppress
from dataclasses import replace
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pandas as pd

from stock_data_hub.cache import SharedFetchCache
from stock_data_hub.errors import DataValidationError, ProviderUnavailable, UnsupportedCapability
from stock_data_hub.models import (
    Adjust,
    AdjustFactor,
    AdjustMethod,
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
    bar_payload_from_dataframe,
    decimal_or_none,
    is_missing,
    record_value,
    required_positive_decimal,
    to_datetime,
)
from stock_data_hub.providers.base import MarketDataProvider
from stock_data_hub.trading_sessions import window_has_trading_session

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
    accepts_us_close_adjust = True
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
            Capability.ADJUST_FACTORS,
        }
    )

    def __init__(
        self,
        *,
        prefer_sina_snapshot: bool = False,
        prefer_sina_bars: bool = False,
        include_cninfo: bool = True,
        min_snapshot_rows: int = 1,
        min_instrument_rows: int = 1,
    ) -> None:
        if min_snapshot_rows < 1 or min_instrument_rows < 1:
            raise ValueError("Minimum row thresholds must be positive")
        self._prefer_sina_snapshot = prefer_sina_snapshot
        self._prefer_sina_bars = prefer_sina_bars
        self._include_cninfo = include_cninfo
        self._min_snapshot_rows = min_snapshot_rows
        self._min_instrument_rows = min_instrument_rows
        self._shared_snapshots = SharedFetchCache()
        # Eastmoney US symbol mapping changes rarely; cache for 1 hour so US
        # bar fetches do not re-download ~10k US rows on every single call.
        self._us_symbol_cache = SharedFetchCache(ttl_seconds=3600.0, max_entries=4)

    def supports_request(
        self,
        capability: Capability,
        *,
        market: Market | None = None,
        asset_type: AssetType | None = None,
        interval: Interval | None = None,
        adjust: Adjust | None = None,
    ) -> bool:
        if not self._include_cninfo and capability in {
            Capability.COMPANY_PROFILE,
            Capability.DISCLOSURES,
        }:
            return False
        if not super().supports_request(
            capability,
            market=market,
            asset_type=asset_type,
            interval=interval,
            adjust=adjust,
        ):
            return False
        if capability == Capability.ADJUST_FACTORS:
            return asset_type in {None, AssetType.STOCK}
        if capability in {Capability.BARS, Capability.QUOTE}:
            if market in {Market.HK, Market.US} and asset_type not in {
                None,
                AssetType.STOCK,
                AssetType.ETF,
            }:
                return False
            if capability == Capability.BARS and market == Market.US:
                return interval in {Interval.DAY_1, Interval.WEEK_1, Interval.MONTH_1}
            return True
        if capability == Capability.US_DAILY_CLOSES:
            return market in {None, Market.US}
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
        can_use_sina = _supports_sina_bars(market, normalized_asset_type, interval, adjust)

        def us_spot_loader() -> pd.DataFrame:
            return self._us_symbol_cache.get("us-spot", ak.stock_us_spot_em).value

        def fetch_eastmoney(warnings: tuple[str, ...] = ()) -> SourcePayload[list[AShareBar]]:
            dataframe = _fetch_market_bars(
                ak,
                normalized,
                normalized_asset_type,
                interval,
                start,
                end,
                adjust_value,
                us_spot_loader=us_spot_loader,
            )
            if dataframe is None or dataframe.empty:
                return bar_payload_from_dataframe(
                    pd.DataFrame(),
                    source="akshare:eastmoney",
                    warnings=warnings,
                    adjust_method=AdjustMethod.ARITHMETIC,
                    symbol=normalized,
                    interval=interval,
                    start=start,
                    end=end,
                    market=market,
                    time_columns=("时间", "日期", "date", "day"),
                    intraday_timestamp_label=_EASTMONEY_INTRADAY_TIMESTAMP_LABEL
                    if interval in {Interval.MINUTE_15, Interval.HOUR_1}
                    else None,
                    adjust=adjust,
                    asset_type=normalized_asset_type,
                    volume_multiplier=Decimal(100) if market == Market.CN else Decimal(1),
                )
            return _parse_bars(
                dataframe,
                normalized,
                interval,
                start,
                end,
                market,
                intraday_timestamp_label=_EASTMONEY_INTRADAY_TIMESTAMP_LABEL
                if interval in {Interval.MINUTE_15, Interval.HOUR_1}
                else None,
                adjust=adjust,
                adjust_method=AdjustMethod.ARITHMETIC,
                asset_type=normalized_asset_type,
                volume_multiplier=Decimal(100) if market == Market.CN else Decimal(1),
                source="akshare:eastmoney",
                warnings=warnings,
            )

        def fetch_sina(warnings: tuple[str, ...] = ()) -> SourcePayload[list[AShareBar]]:
            dataframe = _fetch_sina_a_share_bars(
                ak,
                normalized,
                normalized_asset_type,
                interval,
                start,
                end,
                adjust_value,
            )
            volume_multiplier = (
                Decimal(100) if normalized_asset_type == AssetType.ETF else Decimal(1)
            )
            truncated = _sina_bars_truncated(dataframe, interval, normalized_asset_type, start)
            if dataframe is None or dataframe.empty:
                return bar_payload_from_dataframe(
                    pd.DataFrame(),
                    source="akshare:sina",
                    warnings=warnings,
                    truncated=truncated,
                    adjust_method=AdjustMethod.PROPORTIONAL,
                    symbol=normalized,
                    interval=interval,
                    start=start,
                    end=end,
                    market=Market.CN,
                    time_columns=("时间", "日期", "date", "day"),
                    intraday_timestamp_label=_SINA_INTRADAY_TIMESTAMP_LABEL
                    if interval in {Interval.MINUTE_15, Interval.HOUR_1}
                    else None,
                    adjust=adjust,
                    asset_type=normalized_asset_type,
                    volume_multiplier=volume_multiplier,
                )
            return _parse_bars(
                dataframe,
                normalized,
                interval,
                start,
                end,
                Market.CN,
                intraday_timestamp_label=_SINA_INTRADAY_TIMESTAMP_LABEL
                if interval in {Interval.MINUTE_15, Interval.HOUR_1}
                else None,
                adjust=adjust,
                adjust_method=AdjustMethod.PROPORTIONAL,
                asset_type=normalized_asset_type,
                truncated=truncated,
                volume_multiplier=volume_multiplier,
                source="akshare:sina",
                warnings=warnings,
            )

        expects_data = window_has_trading_session(start, end, market)
        if self._prefer_sina_bars and can_use_sina:
            primary_name = "Sina"
            primary_fn = fetch_sina

            def fallback_eastmoney() -> SourcePayload[list[AShareBar]]:
                return fetch_eastmoney(
                    warnings=("Sina bars were unavailable; used Eastmoney fallback.",)
                )

            fallback_fn = fallback_eastmoney
        else:
            primary_name = "Eastmoney"
            primary_fn = fetch_eastmoney

            def fallback_sina() -> SourcePayload[list[AShareBar]]:
                return fetch_sina(
                    warnings=("Eastmoney bars were unavailable; used Sina fallback.",)
                )

            fallback_fn = fallback_sina if can_use_sina else None

        primary_empty: SourcePayload[list[AShareBar]] | None = None
        try:
            payload = primary_fn()
            if payload.value or not expects_data:
                return payload
            if payload.quality is not None and payload.quality.filtered_rows:
                raise DataValidationError(f"{primary_name} returned no usable bars")
            primary_empty = payload
        except Exception as primary_error:
            if fallback_fn is None:
                raise ProviderUnavailable(
                    f"AkShare could not obtain {interval.value} bars for {normalized}"
                ) from primary_error
            try:
                fallback_payload = fallback_fn()
                if fallback_payload.value or not expects_data:
                    return fallback_payload
                if fallback_payload.quality is not None and fallback_payload.quality.filtered_rows:
                    raise DataValidationError("Fallback returned no usable bars")
                return fallback_payload
            except Exception as fallback_error:
                raise ProviderUnavailable(
                    "AkShare could not obtain A-share bars from Eastmoney or Sina"
                ) from fallback_error

        # Primary cleanly returned empty on an active trading window; try fallback
        # if available, otherwise return the clean empty payload for hub consensus.
        if fallback_fn is not None:
            try:
                fallback_payload = fallback_fn()
                if fallback_payload.value:
                    return fallback_payload
                if fallback_payload.quality is None or not fallback_payload.quality.filtered_rows:
                    return fallback_payload
            except Exception:  # noqa: BLE001 - primary already confirmed clean empty
                return primary_empty
        return primary_empty

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
                fetched = self._shared_snapshots.get(
                    f"{market.value}:{normalized_asset_type.value}:{normalized.rsplit('.', 1)[-1]}",
                    lambda: _quote_dataframe(ak, normalized, normalized_asset_type),
                )
                row = _find_market_row(fetched.value, code, market)
                return SourcePayload(
                    value=Quote(
                        symbol=normalized,
                        price=required_positive_decimal(
                            record_value(row, "最新价", "price"), "latest price"
                        ),
                        ts=fetched.fetched_at,
                        market=market,
                        freshness=(
                            QuoteFreshness.REALTIME
                            if market == Market.CN
                            else QuoteFreshness.DELAYED
                        ),
                    ),
                    source="akshare:eastmoney",
                    fetched_at=fetched.fetched_at,
                    cache_hit=fetched.cache_hit,
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
        cached_snapshot = self._shared_snapshots.peek("cn-snapshot")
        if cached_snapshot is None and hasattr(ak, "stock_bid_ask_em"):
            with suppress(Exception):
                return _fetch_single_cn_quote_em(ak, normalized)
        payload = (
            replace(
                cached_snapshot.value,
                fetched_at=cached_snapshot.fetched_at,
                cache_hit=cached_snapshot.cache_hit,
            )
            if cached_snapshot is not None
            else self.fetch_snapshot()
        )
        row = next((row for row in payload.value.rows if row.symbol == normalized), None)
        if row is None:
            raise DataValidationError(f"AkShare snapshot has no row for {normalized}")
        return SourcePayload(
            Quote(
                symbol=normalized,
                price=required_positive_decimal(row.price, "latest price"),
                ts=payload.value.fetched_at,
                market=market,
            ),
            source=payload.source,
            warnings=payload.warnings,
            fetched_at=payload.fetched_at,
            cache_hit=payload.cache_hit,
        )

    def fetch_snapshot(self) -> SourcePayload[Snapshot]:
        fetched = self._shared_snapshots.get("cn-snapshot", self._fetch_snapshot_uncached)
        return replace(fetched.value, fetched_at=fetched.fetched_at, cache_hit=fetched.cache_hit)

    def clear_cache(self) -> None:
        self._shared_snapshots.clear()
        self._us_symbol_cache.clear()

    def _fetch_snapshot_uncached(self) -> SourcePayload[Snapshot]:
        ak = _load_akshare()
        min_rows = self._min_snapshot_rows
        if self._prefer_sina_snapshot:
            try:
                sina_fn = _ak_fn(ak, "stock_zh_a_spot", "stock_zh_a_spot_sina")
                return SourcePayload(
                    value=_parse_snapshot(sina_fn(), has_market_cap=False, min_rows=min_rows),
                    source="akshare:sina",
                )
            except Exception:  # noqa: BLE001 - an upstream failure triggers fallback
                try:
                    return SourcePayload(
                        value=_parse_snapshot(
                            ak.stock_zh_a_spot_em(), has_market_cap=True, min_rows=min_rows
                        ),
                        source="akshare:eastmoney",
                        warnings=("Sina snapshot was unavailable; used Eastmoney fallback.",),
                    )
                except Exception as fallback_error:
                    raise ProviderUnavailable(
                        "AkShare could not obtain an A-share market snapshot"
                    ) from fallback_error
        try:
            return SourcePayload(
                value=_parse_snapshot(
                    ak.stock_zh_a_spot_em(), has_market_cap=True, min_rows=min_rows
                ),
                source="akshare:eastmoney",
            )
        except Exception:  # noqa: BLE001 - an upstream failure triggers Sina fallback
            try:
                sina_fn = _ak_fn(ak, "stock_zh_a_spot", "stock_zh_a_spot_sina")
                return SourcePayload(
                    value=_parse_snapshot(sina_fn(), has_market_cap=False, min_rows=min_rows),
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
        by_symbol: dict[str, Instrument] = {}
        for row in dataframe.to_dict("records"):
            code = _six_digit_code(row.get(code_column))
            name = _clean_text(row.get(name_column))
            if not code or not name:
                continue
            try:
                symbol = normalize_a_share_symbol(code)
            except DataValidationError:
                continue
            by_symbol[symbol] = Instrument(
                symbol=symbol,
                name=name,
                market=Market.CN,
                asset_type=asset_type_from_symbol(symbol),
                status="st" if "ST" in name.upper() else "active",
            )
        if not by_symbol:
            raise DataValidationError("AkShare instrument list had no usable A-share symbols")
        if len(by_symbol) < self._min_instrument_rows:
            raise DataValidationError(
                f"AkShare instrument list is incomplete: {len(by_symbol)} rows < {self._min_instrument_rows}"
            )
        delisted_loaded = _merge_delisted_instruments(ak, by_symbol)
        warnings: tuple[str, ...] = (
            ()
            if delisted_loaded
            else (
                "Instrument list contains active listings only; delisted endpoints were unavailable.",
            )
        )
        return SourcePayload(
            value=sorted(by_symbol.values(), key=lambda item: item.symbol),
            source="akshare:instrument_list",
            warnings=warnings,
        )

    def fetch_trade_calendar(self) -> SourcePayload[TradeCalendar]:
        ak = _load_akshare()
        calendar_fn = _ak_fn(ak, "tool_trade_date_hist_sina", "calendar_trade_date_hist_sina")
        dataframe = calendar_fn()
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
        return _fetch_cninfo_company_profile(symbol)

    def fetch_restricted_releases(self, symbol: str) -> SourcePayload[list[RestrictedRelease]]:
        normalized = normalize_a_share_symbol(symbol)
        code = normalized.split(".", maxsplit=1)[0]
        ak = _load_akshare()
        pit_warning = (
            "Restricted-release market_value and ratio_of_total may reflect current spot prices "
            "rather than point-in-time historical values."
        )
        try:
            releases = _parse_releases_em(
                ak.stock_restricted_release_queue_em(symbol=code), normalized
            )
            # An empty release schedule is a valid answer.  Falling back in
            # that case can turn "no releases" into a misleading outage.
            return SourcePayload(
                value=releases,
                source="akshare:eastmoney",
                warnings=(pit_warning,) if releases else (),
            )
        except Exception:  # noqa: BLE001 - an upstream failure triggers Sina fallback
            try:
                releases = _parse_releases_sina(
                    ak.stock_restricted_release_queue_sina(symbol=code), normalized
                )
                warnings: tuple[str, ...] = (
                    "Eastmoney release data was unavailable; used Sina fallback.",
                    *([pit_warning] if releases else []),
                )
                return SourcePayload(
                    value=releases,
                    source="akshare:sina",
                    warnings=warnings,
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
        return _fetch_cninfo_disclosures(symbol, start, end)

    def fetch_us_daily_closes(
        self,
        ticker: str,
        max_rows: int,
        *,
        adjust: Adjust = Adjust.NONE,
    ) -> SourcePayload[list[DailyClose]]:
        normalized = ticker.upper().strip()
        ak = _load_akshare()
        us_daily_fn = _ak_fn(ak, "stock_us_daily", "stock_us_hist_sina")
        dataframe = (
            us_daily_fn(symbol=normalized)
            if adjust == Adjust.NONE
            else us_daily_fn(symbol=normalized, adjust=adjust.value)
        )
        if dataframe is None or dataframe.empty:
            return SourcePayload(value=[], source="akshare:us_daily")
        closes: list[DailyClose] = []
        for row in dataframe.tail(max_rows).to_dict("records"):
            day = _to_date(record_value(row, "date", "日期"))
            close = decimal_or_none(record_value(row, "close", "收盘"))
            if day is None or close is None or close <= 0:
                continue
            closes.append(DailyClose(symbol=normalized, day=day, close=close))
        closes.sort(key=lambda item: item.day)
        return SourcePayload(value=closes, source="akshare:us_daily")

    def fetch_adjust_factors(
        self,
        symbol: str,
        start: date | None = None,
        end: date | None = None,
    ) -> SourcePayload[list[AdjustFactor]]:
        normalized = normalize_symbol(symbol)
        market = market_from_symbol(normalized)
        if asset_type_from_symbol(normalized) != AssetType.STOCK:
            raise UnsupportedCapability(
                f"AkShare adjustment factors only support stock symbols, not {normalized}"
            )
        ak = _load_akshare()
        code = normalized.split(".", maxsplit=1)[0]
        try:
            if market == Market.CN:
                fn = _ak_fn(ak, "stock_zh_a_daily", "stock_zh_a_hist_sina")
                target = prefixed_a_share_symbol(normalized)
            elif market == Market.HK:
                fn = _ak_fn(ak, "stock_hk_daily", "stock_hk_hist_sina")
                target = code.zfill(5)
            else:
                fn = _ak_fn(ak, "stock_us_daily", "stock_us_hist_sina")
                target = code
            qfq_df = fn(symbol=target, adjust="qfq-factor")
            hfq_df = fn(symbol=target, adjust="hfq-factor")
        except UnsupportedCapability:
            raise
        except Exception as exc:
            raise ProviderUnavailable(
                f"AkShare could not obtain adjustment factors for {normalized}"
            ) from exc
        factors = _merge_factor_frames(normalized, market, qfq_df, hfq_df, start=start, end=end)
        return SourcePayload(factors, source="akshare:sina")


class CninfoProvider(MarketDataProvider):
    """Isolated CNINFO adapter for company profiles and regulatory disclosures."""

    name = "cninfo"
    markets = frozenset({Market.CN})
    capabilities = frozenset({Capability.COMPANY_PROFILE, Capability.DISCLOSURES})

    def fetch_company_profile(self, symbol: str) -> SourcePayload[CompanyProfile]:
        return _fetch_cninfo_company_profile(symbol)

    def fetch_disclosures(
        self,
        symbol: str,
        start: date,
        end: date,
    ) -> SourcePayload[list[Disclosure]]:
        return _fetch_cninfo_disclosures(symbol, start, end)


def _supports_sina_bars(
    market: Market,
    asset_type: AssetType,
    interval: Interval,
    adjust: Adjust,
) -> bool:
    if market != Market.CN:
        return False
    if asset_type == AssetType.STOCK:
        return interval in {Interval.DAY_1, Interval.MINUTE_15, Interval.HOUR_1}
    if asset_type == AssetType.ETF:
        return interval == Interval.DAY_1 and adjust == Adjust.NONE
    return False


def _fetch_market_bars(
    ak: Any,
    symbol: str,
    asset_type: AssetType,
    interval: Interval,
    start: datetime,
    end: datetime,
    adjust: str,
    *,
    us_spot_loader: Any = None,
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
            return _ak_fn(ak, "stock_hk_hist", "stock_hk_hist_em")(**common, adjust=adjust)
        if market == Market.US:
            return _ak_fn(ak, "stock_us_hist", "stock_us_hist_em")(
                **{
                    **common,
                    "symbol": _eastmoney_us_symbol(ak, code, spot_loader=us_spot_loader),
                },
                adjust=adjust,
            )
        if asset_type == AssetType.ETF:
            return ak.fund_etf_hist_em(**common, adjust=adjust)
        if asset_type == AssetType.INDEX:
            return _ak_fn(ak, "index_zh_a_hist", "index_zh_hist_em")(**common)
        return _ak_fn(ak, "stock_zh_a_hist", "stock_zh_a_hist_em")(**common, adjust=adjust)

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
        return _ak_fn(ak, "index_zh_a_hist_min_em", "index_zh_hist_min_em")(
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
    asset_type: AssetType,
    interval: Interval,
    start: datetime,
    end: datetime,
    adjust: str,
) -> pd.DataFrame:
    prefixed = prefixed_a_share_symbol(symbol)
    if asset_type == AssetType.ETF:
        return ak.fund_etf_hist_sina(symbol=prefixed)
    if interval == Interval.DAY_1:
        return _ak_fn(ak, "stock_zh_a_daily", "stock_zh_a_hist_sina")(
            symbol=prefixed,
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            adjust=adjust,
        )
    return _ak_fn(ak, "stock_zh_a_minute", "stock_zh_a_hist_min_sina")(
        symbol=prefixed,
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
        if symbol.endswith(".SH"):
            series_candidates = ("上证系列指数", "中证系列指数")
        elif symbol.endswith(".SZ"):
            series_candidates = ("深证系列指数", "中证系列指数")
        else:
            series_candidates = ("沪深重要指数", "中证系列指数")
        index_spot_fn = _ak_fn(ak, "stock_zh_index_spot_em", "index_zh_spot_em")
        frames = []
        for series in series_candidates:
            with suppress(Exception):
                frame = index_spot_fn(symbol=series)
                if isinstance(frame, pd.DataFrame) and not frame.empty:
                    frames.append(frame)
        if not frames:
            raise ProviderUnavailable(f"AkShare index spot is unavailable for {symbol}")
        return pd.concat(frames, ignore_index=True)
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


def _eastmoney_us_symbol(ak: Any, ticker: str, *, spot_loader: Any = None) -> str:
    spot_frame = spot_loader() if callable(spot_loader) else ak.stock_us_spot_em()
    row = _find_market_row(spot_frame, ticker, Market.US)
    value = _clean_text(record_value(row, "代码", "code"))
    if value and re.fullmatch(r"\d+\.[A-Z0-9.-]+", value.upper()):
        return value.upper()
    encoding = _clean_text(record_value(row, "编码"))
    short_name = _clean_text(record_value(row, "简称"))
    if encoding and short_name and short_name.upper() == ticker.upper():
        return f"{encoding}.{short_name.upper()}"
    raise DataValidationError(f"Could not resolve Eastmoney's market code for {ticker}")


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
    adjust: Adjust = Adjust.NONE,
    adjust_method: AdjustMethod | str | None = None,
    asset_type: AssetType | None = None,
    truncated: bool = False,
    volume_multiplier: Decimal = Decimal(1),
    source: str = "akshare",
    warnings: tuple[str, ...] = (),
) -> SourcePayload[list[AShareBar]]:
    return bar_payload_from_dataframe(
        dataframe,
        source=source,
        warnings=warnings,
        truncated=truncated,
        adjust_method=adjust_method,
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
        adjust=adjust,
        asset_type=asset_type,
        volume_multiplier=volume_multiplier,
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


def _parse_snapshot(
    dataframe: pd.DataFrame | None,
    has_market_cap: bool,
    *,
    min_rows: int = 1,
) -> Snapshot:
    if dataframe is None or dataframe.empty:
        raise DataValidationError("Snapshot response is empty")
    rows: list[SnapshotRow] = []
    for row in dataframe.to_dict("records"):
        raw_code = _clean_text(record_value(row, "代码", "code"))
        name = _clean_text(record_value(row, "名称", "name"))
        if not raw_code or not name:
            continue
        try:
            symbol = normalize_a_share_symbol(raw_code)
        except DataValidationError:
            code = _six_digit_code(raw_code)
            if not code or re.match(r"^(?:SH|SZ|BJ)", raw_code.upper()):
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
    if len(rows) < min_rows:
        raise DataValidationError(
            f"Snapshot response is incomplete: {len(rows)} usable rows < {min_rows}"
        )
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
    for row in dataframe.to_dict("records"):
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
    for row in dataframe.to_dict("records"):
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


def _ak_fn(ak: Any, *names: str) -> Any:
    for name in names:
        fn = getattr(ak, name, None)
        if callable(fn):
            return fn
    raise ProviderUnavailable(
        f"Installed AkShare has none of the expected endpoints: {', '.join(names)}"
    )


def _sina_bars_truncated(
    dataframe: pd.DataFrame | None,
    interval: Interval,
    asset_type: AssetType,
    start: datetime,
) -> bool:
    if dataframe is None or dataframe.empty or len(dataframe) < 1000:
        return False
    if interval not in {Interval.MINUTE_15, Interval.HOUR_1} and asset_type != AssetType.ETF:
        return False
    stamps = [
        parsed
        for row in dataframe.head(5).to_dict("records") + dataframe.tail(5).to_dict("records")
        if (parsed := to_datetime(record_value(row, "时间", "日期", "date", "day"))) is not None
    ]
    if not stamps:
        return False
    earliest = min(stamps)
    start_bj = to_beijing(start)
    if earliest <= start_bj:
        return False
    probe_end = earliest - timedelta(minutes=15 if interval != Interval.DAY_1 else 1440)
    return probe_end >= start_bj and window_has_trading_session(start_bj, probe_end, Market.CN)


def _fetch_single_cn_quote_em(ak: Any, symbol: str) -> SourcePayload[Quote]:
    code = symbol.split(".", maxsplit=1)[0]
    dataframe = ak.stock_bid_ask_em(symbol=code)
    if not isinstance(dataframe, pd.DataFrame) or dataframe.empty:
        raise DataValidationError("Eastmoney single-stock quote was empty")
    items = {
        str(row.get("item", "")).strip(): row.get("value")
        for row in dataframe.to_dict("records")
    }
    price = required_positive_decimal(items.get("最新"), "latest price")
    now = datetime.now(tz=BEIJING_TZ)
    return SourcePayload(
        value=Quote(symbol=symbol, price=price, ts=now, market=Market.CN),
        source="akshare:eastmoney",
        fetched_at=now,
    )


def _merge_delisted_instruments(ak: Any, by_symbol: dict[str, Instrument]) -> bool:
    loaded_any = False
    sh_fn = getattr(ak, "stock_info_sh_delist", None)
    if callable(sh_fn):
        with suppress(Exception):
            frame = sh_fn()
            if isinstance(frame, pd.DataFrame) and not frame.empty:
                loaded_any = True
                for row in frame.to_dict("records"):
                    code = _six_digit_code(record_value(row, "公司代码", "证券代码", "code"))
                    name = _clean_text(record_value(row, "公司简称", "证券简称", "name"))
                    if not code or not name:
                        continue
                    with suppress(DataValidationError):
                        sym = normalize_a_share_symbol(code)
                        by_symbol.setdefault(
                            sym,
                            Instrument(
                                symbol=sym,
                                name=name,
                                market=Market.CN,
                                asset_type=asset_type_from_symbol(sym),
                                status="delisted",
                                list_date=_to_date(record_value(row, "上市日期", "list_date")),
                                delist_date=_to_date(
                                    record_value(row, "暂停上市日期", "终止上市日期", "delist_date")
                                ),
                            ),
                        )
    sz_fn = getattr(ak, "stock_info_sz_delist", None)
    if callable(sz_fn):
        with suppress(Exception):
            frame = sz_fn(symbol="终止上市公司")
            if isinstance(frame, pd.DataFrame) and not frame.empty:
                loaded_any = True
                for row in frame.to_dict("records"):
                    code = _six_digit_code(record_value(row, "证券代码", "公司代码", "code"))
                    name = _clean_text(record_value(row, "证券简称", "公司简称", "name"))
                    if not code or not name:
                        continue
                    with suppress(DataValidationError):
                        sym = normalize_a_share_symbol(code)
                        by_symbol.setdefault(
                            sym,
                            Instrument(
                                symbol=sym,
                                name=name,
                                market=Market.CN,
                                asset_type=asset_type_from_symbol(sym),
                                status="delisted",
                                list_date=_to_date(record_value(row, "上市日期", "list_date")),
                                delist_date=_to_date(record_value(row, "终止上市日期", "delist_date")),
                            ),
                        )
    return loaded_any


def _fetch_cninfo_company_profile(symbol: str) -> SourcePayload[CompanyProfile]:
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
        warnings=(
            "Company profile reflects current upstream classification and is not point-in-time.",
        ),
    )


def _fetch_cninfo_disclosures(
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
    has_date_only = False
    for row in dataframe.to_dict("records"):
        raw_announce = record_value(row, "公告时间", "announce_date")
        parsed_dt = to_datetime(raw_announce)
        title = _clean_text(record_value(row, "公告标题", "title"))
        if parsed_dt is None or not title:
            continue
        has_exact_time = (
            isinstance(raw_announce, datetime)
            or ":" in str(raw_announce)
        ) and (parsed_dt.hour != 0 or parsed_dt.minute != 0 or parsed_dt.second != 0)
        if not has_exact_time:
            has_date_only = True
        disclosures.append(
            Disclosure(
                symbol=normalized,
                announce_date=parsed_dt.date(),
                title=title,
                published_at=parsed_dt if has_exact_time else None,
            )
        )
    disclosures.sort(key=lambda item: (item.announce_date, item.title))
    date_only_warning = (
        "Disclosure announce_date lacks intraday publication timestamps; post-close filings "
        "stamped day T are tradable no earlier than the next session."
    )
    warnings = (date_only_warning,) if has_date_only else ()
    return SourcePayload(value=disclosures, source="akshare:cninfo", warnings=warnings)


def _merge_factor_frames(
    symbol: str,
    market: Market,
    qfq_df: pd.DataFrame | None,
    hfq_df: pd.DataFrame | None,
    *,
    start: date | None,
    end: date | None,
) -> list[AdjustFactor]:
    qfq_map: dict[date, Decimal] = {}
    hfq_map: dict[date, Decimal] = {}
    if isinstance(qfq_df, pd.DataFrame) and not qfq_df.empty:
        for row in qfq_df.to_dict("records"):
            day = _to_date(record_value(row, "date", "日期"))
            factor = decimal_or_none(record_value(row, "qfq_factor", "qfq-factor", "factor"))
            if day is not None and factor is not None and factor > 0:
                qfq_map[day] = factor
    if isinstance(hfq_df, pd.DataFrame) and not hfq_df.empty:
        for row in hfq_df.to_dict("records"):
            day = _to_date(record_value(row, "date", "日期"))
            factor = decimal_or_none(record_value(row, "hfq_factor", "hfq-factor", "factor"))
            if day is not None and factor is not None and factor > 0:
                hfq_map[day] = factor
    all_days = sorted(set(qfq_map).union(hfq_map))
    return [
        AdjustFactor(
            symbol=symbol,
            ex_date=day,
            qfq_factor=qfq_map.get(day),
            hfq_factor=hfq_map.get(day),
            market=market,
        )
        for day in all_days
        if (start is None or day >= start) and (end is None or day <= end)
    ]


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
