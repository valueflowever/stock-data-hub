from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd

from stock_data_hub.models import Adjust, Interval, Market, QuoteFreshness
from stock_data_hub.normalization import BEIJING_TZ
from stock_data_hub.providers.baostock import BaoStockProvider
from stock_data_hub.providers.efinance import EFinanceProvider
from stock_data_hub.providers.pytdx import PyTDXProvider
from stock_data_hub.providers.tickflow import TickFlowProvider
from stock_data_hub.providers.yfinance import YFinanceProvider, _yahoo_symbol

START = datetime(2026, 7, 1, tzinfo=BEIJING_TZ)
END = datetime(2026, 7, 31, 23, 59, tzinfo=BEIJING_TZ)


def _bar_frame(*, chinese: bool = False) -> pd.DataFrame:
    if chinese:
        return pd.DataFrame(
            {
                "日期": ["2026-07-17"],
                "开盘": ["10.0"],
                "最高": ["11.0"],
                "最低": ["9.5"],
                "收盘": ["10.5"],
                "成交量": [100],
                "成交额": [1050],
            }
        )
    return pd.DataFrame(
        {
            "trade_date": ["20260717"],
            "open": ["10.0"],
            "high": ["11.0"],
            "low": ["9.5"],
            "close": ["10.5"],
            "vol": [100],
            "amount": [1050],
        }
    )


def test_tickflow_uses_only_anonymous_history() -> None:
    class Klines:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, object]]] = []

        def get(self, symbol: str, **kwargs: object) -> pd.DataFrame:
            self.calls.append((symbol, kwargs))
            return pd.DataFrame(
                {
                    "trade_date": ["2026-07-17"],
                    "open": ["10.0"],
                    "high": ["11.0"],
                    "low": ["9.5"],
                    "close": ["10.5"],
                    "volume": [100],
                    "amount": [1050],
                }
            )

    client = SimpleNamespace(klines=Klines())
    provider = TickFlowProvider(client_factory=lambda: client)
    result = provider.fetch_bars("600519.SH", Interval.WEEK_1, START, END, Adjust.QFQ)

    assert result.value[0].symbol == "600519.SH"
    assert result.value[0].market == Market.CN
    assert result.source == "tickflow:free"
    assert result.warnings
    assert "start_time" not in client.klines.calls[0][1]
    assert not provider.supports_quote if hasattr(provider, "supports_quote") else True

    from stock_data_hub.models import Capability

    assert not provider.supports(Capability.QUOTE)


def test_yfinance_normalizes_global_bars_and_delayed_quotes() -> None:
    requested: list[str] = []

    class Ticker:
        def __init__(self) -> None:
            self.fast_info = {"last_price": "201.25"}

        def history(self, **_: object) -> pd.DataFrame:
            frame = pd.DataFrame(
                {
                    "Open": [200.0],
                    "High": [203.0],
                    "Low": [199.0],
                    "Close": [201.25],
                    "Volume": [1000],
                },
                index=pd.DatetimeIndex(["2026-07-17"], name="Date"),
            )
            return frame

    def ticker_factory(symbol: str) -> Ticker:
        requested.append(symbol)
        return Ticker()

    provider = YFinanceProvider(ticker_factory=ticker_factory)
    bars = provider.fetch_bars("AAPL.US", Interval.DAY_1, START, END, Adjust.QFQ)
    quote = provider.fetch_quote("00700.HK")
    closes = provider.fetch_us_daily_closes("AAPL", 10)

    assert bars.value[0].market == Market.US
    assert quote.value.price == Decimal("201.25")
    assert quote.value.market == Market.HK
    assert quote.value.freshness == QuoteFreshness.DELAYED
    assert closes.value[0].symbol == "AAPL"
    assert closes.value[0].close == Decimal("201.25")
    assert requested == ["AAPL", "0700.HK", "AAPL"]
    assert _yahoo_symbol("510300.SH") == "510300.SS"


def test_pytdx_connects_to_configured_server_for_unadjusted_daily_bars() -> None:
    class Client:
        def __init__(self) -> None:
            self.connected: tuple[str, int] | None = None
            self.request: tuple[int, int, str, int, int] | None = None
            self.disconnected = False

        def connect(self, host: str, port: int) -> bool:
            self.connected = (host, port)
            return True

        def get_security_bars(
            self, category: int, market: int, code: str, offset: int, count: int
        ) -> list[dict[str, object]]:
            self.request = (category, market, code, offset, count)
            return [
                {
                    "datetime": "2026-07-17 15:00:00",
                    "open": 10,
                    "high": 11,
                    "low": 9.5,
                    "close": 10.5,
                    "vol": 100,
                    "amount": 1050,
                }
            ]

        def to_df(self, rows: object) -> pd.DataFrame:
            return pd.DataFrame(rows)

        def disconnect(self) -> None:
            self.disconnected = True

    client = Client()
    provider = PyTDXProvider(
        "119.147.212.81",
        client_factory=lambda: client,
        page_size=800,
    )
    result = provider.fetch_bars("600519.SH", Interval.DAY_1, START, END, Adjust.NONE)

    assert result.value[0].close == Decimal("10.5")
    assert client.connected == ("119.147.212.81", 7709)
    assert client.request == (9, 1, "600519", 0, 800)
    assert client.disconnected


def test_efinance_normalizes_bars_quote_and_snapshot() -> None:
    class Stock:
        def __init__(self) -> None:
            self.history_kwargs: dict[str, object] = {}

        def get_quote_history(self, _: str, **kwargs: object) -> pd.DataFrame:
            self.history_kwargs = kwargs
            return _bar_frame(chinese=True)

        def get_realtime_quotes(self) -> pd.DataFrame:
            return pd.DataFrame(
                {
                    "股票代码": ["600519", "159915"],
                    "股票名称": ["茅台", "ETF"],
                    "最新价": ["10.5", "1.0"],
                    "成交额": [1050, 200],
                    "总市值": [10000, 2000],
                    "换手率": ["1.2", "0.8"],
                    "涨跌幅": ["0.5", "-0.2"],
                }
            )

    stock = Stock()
    provider = EFinanceProvider(client_loader=lambda: SimpleNamespace(stock=stock))
    bars = provider.fetch_bars("600519.SH", Interval.DAY_1, START, END, Adjust.QFQ)
    quote = provider.fetch_quote("600519.SH")
    snapshot = provider.fetch_snapshot()

    assert bars.value[0].market == Market.CN
    assert stock.history_kwargs["fqt"] == 1
    assert quote.value.price == Decimal("10.5")
    assert [row.symbol for row in snapshot.value.rows] == ["159915.SZ", "600519.SH"]


def test_baostock_logs_in_queries_and_logs_out() -> None:
    class Result:
        error_code = "0"
        error_msg = "success"

        def __init__(self) -> None:
            self.fields = ["date", "code", "open", "high", "low", "close", "volume", "amount"]
            self._rows = [["2026-07-17", "sh.600519", "10", "11", "9.5", "10.5", "100", "1050"]]
            self._position = -1

        def next(self) -> bool:
            self._position += 1
            return self._position < len(self._rows)

        def get_row_data(self) -> list[str]:
            return self._rows[self._position]

    class Client:
        def __init__(self) -> None:
            self.logout_called = False
            self.symbol: str | None = None

        def login(self) -> SimpleNamespace:
            return SimpleNamespace(error_code="0", error_msg="success")

        def query_history_k_data_plus(self, symbol: str, *_: object, **__: object) -> Result:
            self.symbol = symbol
            return Result()

        def logout(self) -> None:
            self.logout_called = True

    client = Client()
    provider = BaoStockProvider(client_loader=lambda: client)
    result = provider.fetch_bars("600519.SH", Interval.MONTH_1, START, END, Adjust.QFQ)

    assert result.value[0].close == Decimal("10.5")
    assert client.symbol == "sh.600519"
    assert client.logout_called
