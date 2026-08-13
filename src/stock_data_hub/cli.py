"""Small command-line entry point for the optional HTTP service."""

from __future__ import annotations

import argparse

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
        hub = create_default_hub(
            pytdx_host=args.pytdx_host,
            pytdx_port=args.pytdx_port,
            peer_urls=args.peer_url,
        )
        uvicorn.run(create_app(hub), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
