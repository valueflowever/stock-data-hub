from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from stock_data_hub.models import Interval, Market
from stock_data_hub.normalization import BEIJING_TZ
from stock_data_hub.providers._parsing import (
    IntradayTimestampLabel,
    bars_from_dataframe,
)


def _bar_frame(timestamp: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "time": [timestamp],
            "open": [10.0],
            "high": [11.0],
            "low": [9.5],
            "close": [10.5],
            "volume": [100],
            "amount": [1050],
        }
    )


def _bars(
    timestamp: str,
    *,
    label: IntradayTimestampLabel | None,
    start: datetime,
    end: datetime,
    now: datetime,
) -> list:
    return bars_from_dataframe(
        _bar_frame(timestamp),
        symbol="600519.SH",
        interval=Interval.MINUTE_15,
        start=start,
        end=end,
        market=Market.CN,
        time_columns=("time",),
        intraday_timestamp_label=label,
        now=now,
    )


def test_open_labeled_intraday_bar_is_normalized_before_window_filtering() -> None:
    close_time = datetime(2026, 7, 17, 10, 0, tzinfo=BEIJING_TZ)

    bars = _bars(
        "2026-07-17 09:45:00",
        label=IntradayTimestampLabel.OPEN,
        start=close_time,
        end=close_time,
        now=datetime(2026, 7, 17, 9, 59, tzinfo=BEIJING_TZ),
    )

    assert len(bars) == 1
    assert bars[0].ts == close_time
    assert bars[0].is_closed is False


def test_close_labeled_intraday_bar_keeps_its_source_timestamp() -> None:
    close_time = datetime(2026, 7, 17, 10, 0, tzinfo=BEIJING_TZ)

    bars = _bars(
        "2026-07-17 10:00:00",
        label=IntradayTimestampLabel.CLOSE,
        start=close_time,
        end=close_time,
        now=close_time,
    )

    assert len(bars) == 1
    assert bars[0].ts == close_time
    assert bars[0].is_closed is True


def test_intraday_provider_must_declare_its_timestamp_label() -> None:
    close_time = datetime(2026, 7, 17, 10, 0, tzinfo=BEIJING_TZ)

    with pytest.raises(ValueError, match="intraday_timestamp_label"):
        _bars(
            "2026-07-17 10:00:00",
            label=None,
            start=close_time,
            end=close_time,
            now=close_time,
        )
