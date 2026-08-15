"""Small command-line entry point for the optional HTTP service."""

from __future__ import annotations

import argparse
import os

from stock_data_hub.providers.defaults import create_default_hub
from stock_data_hub.service import create_app


def main() -> None:
    parser = argparse.ArgumentParser(prog="stock-data-hub")
    subparsers = parser.add_subparsers(dest="command", required=True)
    serve = subparsers.add_parser("serve", help="start the HTTP/OpenAPI service")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--pytdx-host", help="enable PyTDX with this quote-server host")
    serve.add_argument("--pytdx-port", type=int, default=7709)
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
    args = parser.parse_args()

    if args.command == "serve":
        try:
            import uvicorn
        except ModuleNotFoundError as exc:
            raise SystemExit("Install stock-data-hub[service] to run the HTTP service.") from exc
        peer_urls = [*args.peer_url, *_environment_list("STOCK_DATA_HUB_PEER_URLS")]
        hub = create_default_hub(
            pytdx_host=args.pytdx_host or os.getenv("STOCK_DATA_HUB_PYTDX_HOST"),
            pytdx_port=args.pytdx_port,
            peer_urls=peer_urls,
            peer_api_key=os.getenv("STOCK_DATA_HUB_PEER_API_KEY"),
            snapshot_timeout_seconds=args.snapshot_timeout,
        )
        uvicorn.run(create_app(hub), host=args.host, port=args.port)


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


if __name__ == "__main__":
    main()
