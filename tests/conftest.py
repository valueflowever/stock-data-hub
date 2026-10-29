"""Keep full CI from silently skipping unavailable optional dependencies."""

from __future__ import annotations

import importlib
import os

import pytest


def pytest_sessionstart(session: pytest.Session) -> None:
    if os.getenv("STOCK_DATA_HUB_REQUIRE_EXTRAS") != "full":
        return
    failures = []
    for module in (
        "fastapi",
        "httpx",
        "talib",
        "tickflow",
        "efinance",
        "baostock",
        "pytdx",
        "yfinance",
    ):
        try:
            importlib.import_module(module)
        except (ImportError, OSError) as exc:
            failures.append(f"{module}: {exc}")
    if failures:
        raise pytest.UsageError(
            "Full validation requires .[dev,service,indicators,sources]; unavailable dependencies: "
            + "; ".join(failures)
        )
