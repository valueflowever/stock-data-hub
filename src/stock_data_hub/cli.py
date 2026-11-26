"""Small command-line entry point for the HTTP service and live diagnostics."""

from __future__ import annotations

import argparse
import asyncio
import ipaddress
import json
import logging
import os

from stock_data_hub.doctor import run_doctor
from stock_data_hub.providers.defaults import create_default_hub
from stock_data_hub.service import create_app


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="stock-data-hub")
    subparsers = parser.add_subparsers(dest="command", required=True)
    serve = subparsers.add_parser("serve", help="start the HTTP/OpenAPI service")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument(
        "--cache-path",
        default=os.getenv("STOCK_DATA_HUB_CACHE_PATH") or None,
        help="optional SQLite cache path for persistent caching and offline stale fallback",
    )
    serve.add_argument(
        "--pytdx-host",
        help="enable PyTDX with host[:port] (comma-separated for fallback servers)",
    )
    serve.add_argument(
        "--pytdx-port",
        type=int,
        default=_environment_int("STOCK_DATA_HUB_PYTDX_PORT", 7709),
    )
    serve.add_argument(
        "--disable-provider",
        action="append",
        default=[],
        help="disable a local provider by name (repeatable)",
    )
    serve.add_argument(
        "--provider-timeout",
        type=float,
        default=_environment_float("STOCK_DATA_HUB_PROVIDER_TIMEOUT_SECONDS", 20.0),
        help="per-provider timeout for standard requests (default: 20 seconds)",
    )
    serve.add_argument(
        "--request-timeout",
        type=float,
        default=_environment_float("STOCK_DATA_HUB_REQUEST_TIMEOUT_SECONDS", 240.0),
        help="total routing budget including peers (seconds)",
    )
    serve.add_argument(
        "--local-timeout",
        type=float,
        default=_environment_float("STOCK_DATA_HUB_LOCAL_TIMEOUT_SECONDS", 90.0),
        help="combined local-source routing budget (seconds)",
    )
    serve.add_argument(
        "--peer-timeout",
        type=float,
        default=_environment_float("STOCK_DATA_HUB_PEER_TIMEOUT_SECONDS", 95.0),
        help="per-peer budget; allow the peer to finish its local routing",
    )
    serve.add_argument(
        "--snapshot-timeout",
        type=float,
        default=_environment_float("STOCK_DATA_HUB_SNAPSHOT_TIMEOUT_SECONDS", 45.0),
        help="per-provider timeout for full-market snapshots (default: 45 seconds)",
    )
    serve.add_argument(
        "--peer-url",
        action="append",
        default=[],
        help="add another Stock Data Hub as a final fallback (repeatable)",
    )
    serve.add_argument(
        "--allow-unauthenticated",
        action="store_true",
        help="explicitly allow binding to a non-loopback host without STOCK_DATA_HUB_API_KEY",
    )

    doctor = subparsers.add_parser(
        "doctor",
        help="probe each configured provider live and recommend the fastest setup",
    )
    doctor.add_argument(
        "--markets",
        default="CN,HK,US",
        help="comma-separated markets to probe (default: CN,HK,US)",
    )
    doctor.add_argument(
        "--timeout",
        type=float,
        default=12.0,
        help="per-probe timeout in seconds (default: 12)",
    )
    doctor.add_argument(
        "--pytdx-host",
        help="include PyTDX with host[:port] (comma-separated for fallback servers)",
    )
    doctor.add_argument(
        "--pytdx-port",
        type=int,
        default=_environment_int("STOCK_DATA_HUB_PYTDX_PORT", 7709),
    )
    doctor.add_argument(
        "--peer-url",
        action="append",
        default=[],
        help="include a peer Stock Data Hub URL (repeatable)",
    )
    doctor.add_argument(
        "--disable-provider",
        action="append",
        default=[],
        help="skip probing a provider by name (repeatable)",
    )
    doctor.add_argument(
        "--json",
        action="store_true",
        help="print machine-readable JSON instead of a table",
    )

    args = parser.parse_args(argv)

    if args.command == "serve":
        configured_key = (os.getenv("STOCK_DATA_HUB_API_KEY") or "").strip()
        allow_unauth = args.allow_unauthenticated or _environment_bool(
            "STOCK_DATA_HUB_ALLOW_UNAUTHENTICATED"
        )
        if not _is_loopback_host(args.host) and not configured_key:
            if not allow_unauth:
                raise SystemExit(
                    "Refusing to bind unauthenticated HTTP service to non-loopback host "
                    f"{args.host!r}; set STOCK_DATA_HUB_API_KEY or pass --allow-unauthenticated."
                )
            logging.getLogger("stock_data_hub").warning(
                "Starting Stock Data Hub on %s:%s without API key authentication.",
                args.host,
                args.port,
            )
        try:
            import uvicorn
        except ModuleNotFoundError as exc:
            raise SystemExit("Install stock-data-hub[service] to run the HTTP service.") from exc
        peer_urls = [*args.peer_url, *_environment_list("STOCK_DATA_HUB_PEER_URLS")]
        disabled_providers = [
            *args.disable_provider,
            *_environment_list("STOCK_DATA_HUB_DISABLED_PROVIDERS"),
        ]
        hub = create_default_hub(
            pytdx_host=args.pytdx_host or os.getenv("STOCK_DATA_HUB_PYTDX_HOST"),
            pytdx_port=args.pytdx_port,
            peer_urls=peer_urls,
            peer_api_key=os.getenv("STOCK_DATA_HUB_PEER_API_KEY"),
            disabled_providers=disabled_providers,
            provider_timeout_seconds=args.provider_timeout,
            snapshot_timeout_seconds=args.snapshot_timeout,
            request_timeout_seconds=args.request_timeout,
            local_timeout_seconds=args.local_timeout,
            peer_timeout_seconds=args.peer_timeout,
            cache_path=args.cache_path,
        )
        try:
            uvicorn.run(create_app(hub), host=args.host, port=args.port)
        finally:
            hub.close()
        return

    if args.command == "doctor":
        peer_urls = [*args.peer_url, *_environment_list("STOCK_DATA_HUB_PEER_URLS")]
        disabled_providers = [
            *args.disable_provider,
            *_environment_list("STOCK_DATA_HUB_DISABLED_PROVIDERS"),
        ]
        markets = [item.strip().upper() for item in args.markets.split(",") if item.strip()]
        hub = create_default_hub(
            pytdx_host=args.pytdx_host or os.getenv("STOCK_DATA_HUB_PYTDX_HOST"),
            pytdx_port=args.pytdx_port,
            peer_urls=peer_urls,
            peer_api_key=os.getenv("STOCK_DATA_HUB_PEER_API_KEY"),
            disabled_providers=disabled_providers,
        )
        try:
            report = asyncio.run(
                run_doctor(hub, markets=markets, timeout_seconds=args.timeout)
            )
        finally:
            hub.close()
        if args.json:
            print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
        else:
            print(report.format_text())


def _environment_list(name: str) -> list[str]:
    return [item.strip() for item in os.getenv(name, "").split(",") if item.strip()]


def _environment_float(name: str, default: float) -> float:
    value = os.getenv(name, "").strip()
    if not value:
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise SystemExit(f"{name} must be a number") from exc


def _environment_int(name: str, default: int) -> int:
    value = os.getenv(name, "").strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise SystemExit(f"{name} must be an integer") from exc


def _environment_bool(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _is_loopback_host(host: str) -> bool:
    cleaned = host.strip().lower()
    if cleaned == "localhost":
        return True
    try:
        return ipaddress.ip_address(cleaned.strip("[]")).is_loopback
    except ValueError:
        return False


if __name__ == "__main__":
    main()
