"""Built-in adapters and the provider extension contract."""

from stock_data_hub.providers.akshare import AkShareProvider, CninfoProvider
from stock_data_hub.providers.baostock import BaoStockProvider
from stock_data_hub.providers.base import MarketDataProvider
from stock_data_hub.providers.calendar import ExchangeCalendarProvider
from stock_data_hub.providers.efinance import EFinanceProvider
from stock_data_hub.providers.pytdx import PyTDXProvider
from stock_data_hub.providers.remote import RemoteHubProvider
from stock_data_hub.providers.tencent import TencentQuoteProvider
from stock_data_hub.providers.tickflow import TickFlowProvider
from stock_data_hub.providers.yfinance import YFinanceProvider

__all__ = [
    "AkShareProvider",
    "BaoStockProvider",
    "CninfoProvider",
    "EFinanceProvider",
    "ExchangeCalendarProvider",
    "MarketDataProvider",
    "PyTDXProvider",
    "RemoteHubProvider",
    "TencentQuoteProvider",
    "TickFlowProvider",
    "YFinanceProvider",
]
