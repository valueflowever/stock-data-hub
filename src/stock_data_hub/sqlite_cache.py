"""Optional zero-dependency SQLite persistent cache with explicit stale fallback."""

from __future__ import annotations

import json
import logging
import sqlite3
import zlib
from collections.abc import Mapping
from contextlib import suppress
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from threading import RLock
from time import time
from typing import Any

from stock_data_hub.hub import CachePolicy
from stock_data_hub.models import (
    DATA_CONTRACT_VERSION,
    AdjustFactor,
    AShareBar,
    AssetType,
    BarDataQuality,
    Capability,
    CompanyProfile,
    DailyClose,
    DataResult,
    Disclosure,
    FetchMetadata,
    Instrument,
    Market,
    Quote,
    QuoteFreshness,
    RestrictedRelease,
    Snapshot,
    SnapshotRow,
    TradeCalendar,
)

logger = logging.getLogger("stock_data_hub")

# Practical defaults when a persistent SQLite cache is enabled via cache_path:
# short-to-medium fresh TTL to avoid hammering free sources on script reruns,
# plus generous stale TTL for graceful offline / outage degradation.
PERSISTENT_CACHE_POLICIES: Mapping[Capability, CachePolicy] = {
    Capability.QUOTE: CachePolicy(timedelta(seconds=5), timedelta(minutes=10)),
    Capability.SNAPSHOT: CachePolicy(timedelta(seconds=30), timedelta(hours=1)),
    Capability.BARS: CachePolicy(timedelta(minutes=15), timedelta(days=7)),
    Capability.INSTRUMENTS: CachePolicy(timedelta(hours=12), timedelta(days=7)),
    Capability.TRADE_CALENDAR: CachePolicy(timedelta(days=1), timedelta(days=30)),
    Capability.COMPANY_PROFILE: CachePolicy(timedelta(days=7), timedelta(days=60)),
    Capability.RESTRICTED_RELEASES: CachePolicy(timedelta(hours=6), timedelta(days=7)),
    Capability.DISCLOSURES: CachePolicy(timedelta(minutes=30), timedelta(days=7)),
    Capability.US_DAILY_CLOSES: CachePolicy(timedelta(hours=1), timedelta(days=7)),
    Capability.ADJUST_FACTORS: CachePolicy(timedelta(days=1), timedelta(days=30)),
}

_SCHEMA_VERSION = f"sqlite-cache-v2:{DATA_CONTRACT_VERSION}"


class SQLiteResultCache:
    """Thread- and multi-process-safe SQLite cache compatible with ``DataHub``.

    Uses WAL mode, zlib-compressed typed JSON (no ``pickle``), contract-version
    isolation, bounded LRU eviction, lazy read-timestamp touches (so reads stay
    pure WAL SELECTs without write-lock contention), and graceful degradation
    on lock contention.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        max_entries: int = 16_384,
        busy_timeout_ms: int = 5000,
        touch_interval_seconds: float = 60.0,
    ) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be at least one")
        if busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms cannot be negative")
        if touch_interval_seconds < 0:
            raise ValueError("touch_interval_seconds cannot be negative")
        self.path = Path(path).expanduser()
        if str(path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.max_entries = max_entries
        self._touch_interval_seconds = touch_interval_seconds
        self._writes_since_evict = 0
        self._lock = RLock()
        self._conn = sqlite3.connect(
            ":memory:" if str(path) == ":memory:" else str(self.path),
            timeout=busy_timeout_ms / 1000.0,
            check_same_thread=False,
        )
        with self._lock:
            self._conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)}")
            if str(path) != ":memory:":
                self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cache_entries (
                    cache_key TEXT PRIMARY KEY,
                    schema_version TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    expires_at REAL NOT NULL,
                    stale_until REAL NOT NULL,
                    last_accessed REAL NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_cache_last_accessed
                ON cache_entries (last_accessed)
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_cache_stale_until
                ON cache_entries (stale_until)
                """
            )
            with suppress(sqlite3.Error):
                self._conn.execute(
                    "DELETE FROM cache_entries WHERE schema_version != ?",
                    (_SCHEMA_VERSION,),
                )
            self._conn.commit()

    def get_fresh(self, key: str) -> object | None:
        return self._get(key, allow_stale=False)

    def get_stale(self, key: str) -> object | None:
        return self._get(key, allow_stale=True)

    def set(self, key: str, value: object, ttl: timedelta, stale_ttl: timedelta) -> None:
        now = _now_epoch()
        fresh_seconds = max(ttl.total_seconds(), 0.0)
        stale_seconds = max(stale_ttl.total_seconds(), 0.0)
        expires_at = now + fresh_seconds
        stale_until = expires_at + stale_seconds
        blob = _encode_value(value)
        try:
            with self._lock:
                self._conn.execute(
                    """
                    INSERT INTO cache_entries (
                        cache_key, schema_version, payload, expires_at, stale_until, last_accessed
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(cache_key) DO UPDATE SET
                        schema_version = excluded.schema_version,
                        payload = excluded.payload,
                        expires_at = excluded.expires_at,
                        stale_until = excluded.stale_until,
                        last_accessed = excluded.last_accessed
                    """,
                    (key, _SCHEMA_VERSION, blob, expires_at, stale_until, now),
                )
                self._evict_locked(now)
                self._conn.commit()
        except sqlite3.Error as exc:
            logger.warning("SQLiteResultCache write skipped on %s: %s", self.path, exc)

    def clear(self) -> None:
        try:
            with self._lock:
                self._conn.execute("DELETE FROM cache_entries")
                self._conn.commit()
        except sqlite3.Error as exc:
            logger.warning("SQLiteResultCache clear failed on %s: %s", self.path, exc)

    def prune_expired(self) -> int:
        now = _now_epoch()
        try:
            with self._lock:
                cursor = self._conn.execute(
                    "DELETE FROM cache_entries WHERE stale_until <= ? OR schema_version != ?",
                    (now, _SCHEMA_VERSION),
                )
                self._conn.commit()
                return int(cursor.rowcount)
        except sqlite3.Error as exc:
            logger.warning("SQLiteResultCache prune failed on %s: %s", self.path, exc)
            return 0

    def stats(self) -> dict[str, int]:
        now = _now_epoch()
        try:
            with self._lock:
                total = int(
                    self._conn.execute(
                        "SELECT COUNT(*) FROM cache_entries WHERE schema_version = ?",
                        (_SCHEMA_VERSION,),
                    ).fetchone()[0]
                )
                fresh = int(
                    self._conn.execute(
                        """
                        SELECT COUNT(*) FROM cache_entries
                        WHERE schema_version = ? AND expires_at > ?
                        """,
                        (_SCHEMA_VERSION, now),
                    ).fetchone()[0]
                )
                stale = int(
                    self._conn.execute(
                        """
                        SELECT COUNT(*) FROM cache_entries
                        WHERE schema_version = ? AND expires_at <= ? AND stale_until > ?
                        """,
                        (_SCHEMA_VERSION, now, now),
                    ).fetchone()[0]
                )
                return {"total": total, "fresh": fresh, "stale": stale}
        except sqlite3.Error as exc:
            logger.warning("SQLiteResultCache stats failed on %s: %s", self.path, exc)
            return {"total": 0, "fresh": 0, "stale": 0}

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> SQLiteResultCache:  # noqa: PYI034 - Python 3.10 compatibility
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _get(self, key: str, *, allow_stale: bool) -> object | None:
        now = _now_epoch()
        try:
            with self._lock:
                row = self._conn.execute(
                    """
                    SELECT schema_version, payload, expires_at, stale_until, last_accessed
                    FROM cache_entries
                    WHERE cache_key = ?
                    """,
                    (key,),
                ).fetchone()
                if row is None:
                    return None
                schema_version, blob, expires_at, stale_until, last_accessed = row
                if schema_version != _SCHEMA_VERSION:
                    self._conn.execute("DELETE FROM cache_entries WHERE cache_key = ?", (key,))
                    self._conn.commit()
                    return None
                # Match InMemoryResultCache semantics: zero TTL (expires_at <= now) is never fresh.
                cutoff = stale_until if allow_stale else expires_at
                if cutoff <= now:
                    return None
                try:
                    decoded = _decode_value(blob)
                except (ValueError, KeyError, TypeError, zlib.error, json.JSONDecodeError):
                    self._conn.execute("DELETE FROM cache_entries WHERE cache_key = ?", (key,))
                    self._conn.commit()
                    return None
                if now - float(last_accessed) >= self._touch_interval_seconds:
                    try:
                        self._conn.execute(
                            "UPDATE cache_entries SET last_accessed = ? WHERE cache_key = ?",
                            (now, key),
                        )
                        self._conn.commit()
                    except sqlite3.Error as touch_exc:
                        with suppress(sqlite3.Error):
                            self._conn.rollback()
                        logger.debug(
                            "SQLiteResultCache touch skipped under lock contention on %s: %s",
                            self.path,
                            touch_exc,
                        )
                return decoded
        except sqlite3.Error as exc:
            logger.warning("SQLiteResultCache read degraded on %s: %s", self.path, exc)
            return None

    def _evict_locked(self, now: float) -> None:
        self._writes_since_evict += 1
        if self.max_entries > 64 and self._writes_since_evict < 32:
            return
        self._writes_since_evict = 0
        self._conn.execute(
            "DELETE FROM cache_entries WHERE stale_until <= ?",
            (now,),
        )
        count = int(self._conn.execute("SELECT COUNT(*) FROM cache_entries").fetchone()[0])
        overflow = count - self.max_entries
        if overflow > 0:
            self._conn.execute(
                """
                DELETE FROM cache_entries
                WHERE cache_key IN (
                    SELECT cache_key FROM cache_entries
                    ORDER BY last_accessed ASC
                    LIMIT ?
                )
                """,
                (overflow,),
            )


def _now_epoch() -> float:
    return time()


def _encode_value(value: object) -> bytes:
    if isinstance(value, DataResult):
        envelope = {
            "kind": "DataResult",
            "value": _encode_payload_value(value.value),
            "metadata": _encode_metadata(value.metadata),
        }
    else:
        envelope = {"kind": "raw", "value": value}
    raw = json.dumps(envelope, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return zlib.compress(raw, level=1)


def _decode_value(blob: bytes) -> object:
    raw = zlib.decompress(blob).decode("utf-8")
    envelope = json.loads(raw)
    if not isinstance(envelope, Mapping):
        raise TypeError("Corrupt cache envelope")
    kind = envelope.get("kind")
    if kind == "raw":
        return envelope.get("value")
    if kind == "DataResult":
        return DataResult(
            value=_decode_payload_value(envelope["value"]),
            metadata=_decode_metadata(envelope["metadata"]),
        )
    raise ValueError(f"Unsupported cache envelope kind: {kind!r}")


def _encode_payload_value(value: object) -> dict[str, Any]:
    if isinstance(value, Quote):
        return {"type": "Quote", "data": _encode_quote(value)}
    if isinstance(value, Snapshot):
        return {"type": "Snapshot", "data": _encode_snapshot(value)}
    if isinstance(value, TradeCalendar):
        return {
            "type": "TradeCalendar",
            "data": {
                "trade_dates": sorted(item.isoformat() for item in value.trade_dates),
                "start_date": value.start_date.isoformat(),
                "end_date": value.end_date.isoformat(),
                "fetched_at": value.fetched_at.isoformat(),
            },
        }
    if isinstance(value, CompanyProfile):
        return {
            "type": "CompanyProfile",
            "data": {
                "symbol": value.symbol,
                "industry": value.industry,
                "main_business": value.main_business,
                "business_scope": value.business_scope,
            },
        }
    if isinstance(value, list):
        if not value:
            return {"type": "list:empty", "data": []}
        first = value[0]
        if isinstance(first, AShareBar):
            return {"type": "list:AShareBar", "data": [_encode_bar(item) for item in value]}
        if isinstance(first, Instrument):
            return {
                "type": "list:Instrument",
                "data": [
                    {
                        "symbol": item.symbol,
                        "name": item.name,
                        "market": item.market.value,
                        "asset_type": item.asset_type.value,
                        "status": item.status,
                        "list_date": item.list_date.isoformat() if item.list_date else None,
                        "delist_date": item.delist_date.isoformat() if item.delist_date else None,
                    }
                    for item in value
                ],
            }
        if isinstance(first, AdjustFactor):
            return {
                "type": "list:AdjustFactor",
                "data": [
                    {
                        "symbol": item.symbol,
                        "ex_date": item.ex_date.isoformat(),
                        "qfq_factor": _opt_dec_str(item.qfq_factor),
                        "hfq_factor": _opt_dec_str(item.hfq_factor),
                        "qfq_offset": _opt_dec_str(item.qfq_offset),
                        "hfq_offset": _opt_dec_str(item.hfq_offset),
                        "market": item.market.value,
                    }
                    for item in value
                ],
            }
        if isinstance(first, RestrictedRelease):
            return {
                "type": "list:RestrictedRelease",
                "data": [
                    {
                        "symbol": item.symbol,
                        "release_date": item.release_date.isoformat(),
                        "shares": _opt_dec_str(item.shares),
                        "market_value": _opt_dec_str(item.market_value),
                        "ratio_of_total": _opt_dec_str(item.ratio_of_total),
                        "share_type": item.share_type,
                    }
                    for item in value
                ],
            }
        if isinstance(first, Disclosure):
            return {
                "type": "list:Disclosure",
                "data": [
                    {
                        "symbol": item.symbol,
                        "announce_date": item.announce_date.isoformat(),
                        "title": item.title,
                        "published_at": (
                            item.published_at.isoformat() if item.published_at else None
                        ),
                    }
                    for item in value
                ],
            }
        if isinstance(first, DailyClose):
            return {
                "type": "list:DailyClose",
                "data": [
                    {
                        "symbol": item.symbol,
                        "day": item.day.isoformat(),
                        "close": format(item.close, "f"),
                    }
                    for item in value
                ],
            }
    raise TypeError(f"Unsupported value type for SQLiteResultCache: {type(value).__name__}")


def _decode_payload_value(raw: Mapping[str, Any]) -> Any:
    kind = raw["type"]
    data = raw["data"]
    if kind == "list:empty":
        return []
    if kind == "list:AShareBar":
        return [_decode_bar(item) for item in data]
    if kind == "Quote":
        return _decode_quote(data)
    if kind == "Snapshot":
        return _decode_snapshot(data)
    if kind == "list:Instrument":
        return [
            Instrument(
                symbol=item["symbol"],
                name=item["name"],
                market=Market(item["market"]),
                asset_type=AssetType(item["asset_type"]),
                status=item.get("status", "active"),
                list_date=(
                    date.fromisoformat(item["list_date"]) if item.get("list_date") else None
                ),
                delist_date=(
                    date.fromisoformat(item["delist_date"]) if item.get("delist_date") else None
                ),
            )
            for item in data
        ]
    if kind == "list:AdjustFactor":
        return [
            AdjustFactor(
                symbol=item["symbol"],
                ex_date=date.fromisoformat(item["ex_date"]),
                qfq_factor=_opt_dec(item["qfq_factor"]),
                hfq_factor=_opt_dec(item["hfq_factor"]),
                qfq_offset=_opt_dec(item.get("qfq_offset")),
                hfq_offset=_opt_dec(item.get("hfq_offset")),
                market=Market(item["market"]),
            )
            for item in data
        ]
    if kind == "TradeCalendar":
        return TradeCalendar(
            trade_dates=frozenset(date.fromisoformat(d) for d in data["trade_dates"]),
            start_date=date.fromisoformat(data["start_date"]),
            end_date=date.fromisoformat(data["end_date"]),
            fetched_at=_from_iso(data["fetched_at"]),
        )
    if kind == "CompanyProfile":
        return CompanyProfile(
            symbol=data["symbol"],
            industry=data["industry"],
            main_business=data["main_business"],
            business_scope=data["business_scope"],
        )
    if kind == "list:RestrictedRelease":
        return [
            RestrictedRelease(
                symbol=item["symbol"],
                release_date=date.fromisoformat(item["release_date"]),
                shares=_opt_dec(item["shares"]),
                market_value=_opt_dec(item["market_value"]),
                ratio_of_total=_opt_dec(item["ratio_of_total"]),
                share_type=item["share_type"],
            )
            for item in data
        ]
    if kind == "list:Disclosure":
        return [
            Disclosure(
                symbol=item["symbol"],
                announce_date=date.fromisoformat(item["announce_date"]),
                title=item["title"],
                published_at=(
                    _from_iso(item["published_at"])
                    if item.get("published_at")
                    else None
                ),
            )
            for item in data
        ]
    if kind == "list:DailyClose":
        return [
            DailyClose(
                symbol=item["symbol"],
                day=date.fromisoformat(item["day"]),
                close=Decimal(item["close"]),
            )
            for item in data
        ]
    raise ValueError(f"Unsupported cached payload type: {kind!r}")


def _encode_bar(bar: AShareBar) -> list[Any]:
    return [
        bar.symbol,
        bar.ts.isoformat(),
        format(bar.open, "f"),
        format(bar.high, "f"),
        format(bar.low, "f"),
        format(bar.close, "f"),
        _opt_dec_str(bar.volume),
        _opt_dec_str(bar.amount),
        bar.is_closed,
        bar.market.value,
    ]


def _decode_bar(item: list[Any]) -> AShareBar:
    return AShareBar(
        symbol=item[0],
        ts=_from_iso(item[1]),
        open=Decimal(item[2]),
        high=Decimal(item[3]),
        low=Decimal(item[4]),
        close=Decimal(item[5]),
        volume=_opt_dec(item[6]),
        amount=_opt_dec(item[7]),
        is_closed=bool(item[8]),
        market=Market(item[9]),
    )


def _encode_quote(quote: Quote) -> dict[str, Any]:
    return {
        "symbol": quote.symbol,
        "price": format(quote.price, "f"),
        "ts": quote.ts.isoformat(),
        "market": quote.market.value,
        "freshness": quote.freshness.value,
    }


def _decode_quote(data: Mapping[str, Any]) -> Quote:
    return Quote(
        symbol=data["symbol"],
        price=Decimal(data["price"]),
        ts=_from_iso(data["ts"]),
        market=Market(data["market"]),
        freshness=QuoteFreshness(data["freshness"]),
    )


def _encode_snapshot(snapshot: Snapshot) -> dict[str, Any]:
    return {
        "has_market_cap": snapshot.has_market_cap,
        "fetched_at": snapshot.fetched_at.isoformat(),
        "rows": [
            [
                row.symbol,
                row.name,
                _opt_dec_str(row.price),
                _opt_dec_str(row.amount),
                _opt_dec_str(row.market_cap),
                _opt_dec_str(row.turnover_ratio),
                _opt_dec_str(row.change_pct),
            ]
            for row in snapshot.rows
        ],
    }


def _decode_snapshot(data: Mapping[str, Any]) -> Snapshot:
    return Snapshot(
        rows=tuple(
            SnapshotRow(
                symbol=row[0],
                name=row[1],
                price=_opt_dec(row[2]),
                amount=_opt_dec(row[3]),
                market_cap=_opt_dec(row[4]),
                turnover_ratio=_opt_dec(row[5]),
                change_pct=_opt_dec(row[6]),
            )
            for row in data["rows"]
        ),
        has_market_cap=bool(data["has_market_cap"]),
        fetched_at=_from_iso(data["fetched_at"]),
    )


def _encode_metadata(meta: FetchMetadata) -> dict[str, Any]:
    quality = None
    if meta.quality is not None:
        quality = {
            "requested_start": meta.quality.requested_start.isoformat(),
            "requested_end": meta.quality.requested_end.isoformat(),
            "available_start": (
                meta.quality.available_start.isoformat()
                if meta.quality.available_start is not None
                else None
            ),
            "available_end": (
                meta.quality.available_end.isoformat()
                if meta.quality.available_end is not None
                else None
            ),
            "input_rows": meta.quality.input_rows,
            "returned_rows": meta.quality.returned_rows,
            "filtered_rows": meta.quality.filtered_rows,
            "duplicate_rows": meta.quality.duplicate_rows,
            "missing_rows": meta.quality.missing_rows,
            "truncated": meta.quality.truncated,
            "volume_unit": meta.quality.volume_unit,
            "currency": meta.quality.currency,
            "suspended_rows": meta.quality.suspended_rows,
            "adjust_method": meta.quality.adjust_method,
        }
    return {
        "source": meta.source,
        "fetched_at": meta.fetched_at.isoformat(),
        "latency_ms": meta.latency_ms,
        "attempted_sources": list(meta.attempted_sources),
        "cache_hit": meta.cache_hit,
        "stale": meta.stale,
        "warnings": list(meta.warnings),
        "received_at": meta.received_at.isoformat() if meta.received_at is not None else None,
        "age_seconds": meta.age_seconds,
        "quality": quality,
        "contract_version": meta.contract_version,
    }


def _decode_metadata(data: Mapping[str, Any]) -> FetchMetadata:
    raw_quality = data.get("quality")
    quality = None
    if isinstance(raw_quality, Mapping):
        quality = BarDataQuality(
            requested_start=_from_iso(raw_quality["requested_start"]),
            requested_end=_from_iso(raw_quality["requested_end"]),
            available_start=(
                _from_iso(raw_quality["available_start"])
                if raw_quality.get("available_start")
                else None
            ),
            available_end=(
                _from_iso(raw_quality["available_end"])
                if raw_quality.get("available_end")
                else None
            ),
            input_rows=int(raw_quality["input_rows"]),
            returned_rows=int(raw_quality["returned_rows"]),
            filtered_rows=int(raw_quality["filtered_rows"]),
            duplicate_rows=int(raw_quality["duplicate_rows"]),
            missing_rows=(
                int(raw_quality["missing_rows"])
                if raw_quality.get("missing_rows") is not None
                else None
            ),
            truncated=bool(raw_quality["truncated"]),
            volume_unit=str(raw_quality["volume_unit"]),
            currency=str(raw_quality["currency"]),
            suspended_rows=int(raw_quality.get("suspended_rows", 0)),
            adjust_method=str(raw_quality.get("adjust_method", "raw")),
        )
    fetched_at = _from_iso(data["fetched_at"])
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)
    return FetchMetadata(
        source=str(data["source"]),
        fetched_at=fetched_at,
        latency_ms=float(data["latency_ms"]),
        attempted_sources=tuple(str(item) for item in data["attempted_sources"]),
        cache_hit=bool(data["cache_hit"]),
        stale=bool(data["stale"]),
        warnings=tuple(str(item) for item in data["warnings"]),
        received_at=(
            _from_iso(data["received_at"]) if data.get("received_at") else None
        ),
        age_seconds=float(data["age_seconds"]),
        quality=quality,
        contract_version=str(data.get("contract_version", DATA_CONTRACT_VERSION)),
    )


def _from_iso(value: object) -> datetime:
    text = str(value).strip()
    if text.endswith(("Z", "z")):
        text = f"{text[:-1]}+00:00"
    return datetime.fromisoformat(text)


def _opt_dec_str(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def _opt_dec(value: str | None) -> Decimal | None:
    return None if value is None else Decimal(value)
