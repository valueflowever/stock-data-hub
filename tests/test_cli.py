from __future__ import annotations

import pytest

from stock_data_hub.cli import _environment_float, _environment_list


def test_environment_list_ignores_blanks(monkeypatch) -> None:
    monkeypatch.setenv(
        "STOCK_DATA_HUB_PEER_URLS",
        " https://china.example, ,https://overseas.example/ ",
    )

    assert _environment_list("STOCK_DATA_HUB_PEER_URLS") == [
        "https://china.example",
        "https://overseas.example/",
    ]


def test_environment_float_uses_default_and_parses_value(monkeypatch) -> None:
    monkeypatch.delenv("STOCK_DATA_HUB_SNAPSHOT_TIMEOUT_SECONDS", raising=False)
    assert _environment_float("STOCK_DATA_HUB_SNAPSHOT_TIMEOUT_SECONDS", 45.0) == 45.0

    monkeypatch.setenv("STOCK_DATA_HUB_SNAPSHOT_TIMEOUT_SECONDS", "60")
    assert _environment_float("STOCK_DATA_HUB_SNAPSHOT_TIMEOUT_SECONDS", 45.0) == 60.0


def test_environment_float_rejects_invalid_value(monkeypatch) -> None:
    monkeypatch.setenv("STOCK_DATA_HUB_SNAPSHOT_TIMEOUT_SECONDS", "slow")

    with pytest.raises(SystemExit, match="must be a number"):
        _environment_float("STOCK_DATA_HUB_SNAPSHOT_TIMEOUT_SECONDS", 45.0)
