# stock-data-hub

面向 A 股、ETF、指数、港股和美股的多源股票市场数据包。它针对国内与海外
网络环境对数据服务商的可达性差异，按市场、标的类型和数据能力自动选择可用
的匿名数据源，并在单一来源失败时回退到后备来源。

`stock-data-hub` 可作为 Python 库嵌入应用，也可启动为只读 HTTP / OpenAPI
服务，供其他语言、工作流或 AI 工具使用。

## 适用范围

项目以股票市场数据为核心，不覆盖下单、账户、持仓或其他会改变外部状态的能力。

| 类别 | 覆盖范围 |
| --- | --- |
| 市场 | A 股、港股、美股 |
| 标的 | 股票、A 股 ETF、指数；全球市场可显式指定 ETF |
| 行情 | 日 / 周 / 月 K 线、报价、A 股全市场快照 |
| 参考与公司数据 | 标的列表、交易日历、公司资料、公告、限售解禁 |
| 美股补充数据 | 日线收盘价 |

所有返回结果都会携带来源、抓取时间、缓存命中、延迟与陈旧数据标记，调用方可以
据此判断数据质量和降级状态。

## 数据源与默认路由

默认组合只使用无需第三方访问凭据的数据源；不会读取或配置 API Token。

| 数据源 | 主要能力 | 启用方式 |
| --- | --- | --- |
| TickFlow | 多市场历史 K 线 | 安装 `tickflow[all]` |
| EFinance | A 股 / ETF K 线、报价、全市场快照、标的列表 | 安装 `efinance` |
| AkShare | 多市场 K 线与报价，以及交易日历、公司资料、公告和解禁等 | 核心依赖 |
| PyTDX | A 股未复权日线 | 安装 `pytdx` 并配置行情服务器 |
| BaoStock | A 股 / ETF 历史 K 线 | 安装 `baostock` |
| YFinance | 全球市场 K 线与延时报价 | 安装 `yfinance` |
| 远端节点 | 另一部署区域提供的同一只读服务 | 配置 `peer_urls` 或 `--peer-url` |

主要默认顺序如下；未安装的可选数据源会自动跳过。

| 场景 | 默认尝试顺序 |
| --- | --- |
| A 股日线 | TickFlow → EFinance → PyTDX（仅未复权）→ AkShare → BaoStock → YFinance |
| A 股 ETF 日线 | TickFlow → EFinance → AkShare → BaoStock → YFinance |
| A 股指数 K 线 | EFinance → AkShare → YFinance |
| 港股 / 美股 K 线 | TickFlow → AkShare → YFinance |
| A 股报价 | EFinance → AkShare → YFinance |
| 港股 / 美股报价 | AkShare → YFinance |
| A 股快照 | EFinance → AkShare |

## 安装

仅作为 Python 库使用：

```powershell
pip install -e .
```

安装所有免费数据源、HTTP 服务和开发工具：

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e ".[dev,service,sources]"
```

Linux 或 macOS：

```bash
source .venv/bin/activate
pip install -e ".[dev,service,sources]"
```

## Python 快速开始

```python
import asyncio
from datetime import datetime, timedelta

from stock_data_hub import BEIJING_TZ, Interval, create_default_hub


async def main() -> None:
    client = create_default_hub()

    quote = await client.get_quote("600519")
    print(quote.value.price, quote.metadata.source)

    end = datetime.now(tz=BEIJING_TZ)
    bars = await client.get_bars(
        "600519",
        Interval.DAY_1,
        end - timedelta(days=60),
        end,
    )
    print(len(bars.value), bars.metadata.source)


asyncio.run(main())
```

时间参数建议使用带时区的 `datetime`。为兼容旧调用，未带时区的时间会按
北京时间解释。

规范化代码示例：

| 市场 | 示例 |
| --- | --- |
| A 股 / ETF | `600519.SH`、`159915.SZ`；六位代码可省略后缀 |
| 港股 | `00700.HK` |
| 美股 | `AAPL.US` |

港股和美股建议显式附加后缀。无法仅从代码区分全球 ETF 与股票时，可传入
`asset_type=AssetType.ETF`。

```python
from stock_data_hub import AssetType, Interval

bars = await client.get_bars(
    "SPY.US", Interval.DAY_1, start, end, asset_type=AssetType.ETF
)
```

PyTDX 只在显式给出行情服务器时启用：

```python
client = create_default_hub(pytdx_host="119.147.212.81", pytdx_port=7709)
```

## HTTP / OpenAPI 服务

安装 `service` extra 后可启动本地只读服务：

```powershell
stock-data-hub serve --host 127.0.0.1 --port 8000
```

- OpenAPI 文档：`http://127.0.0.1:8000/docs`
- OpenAPI JSON：`http://127.0.0.1:8000/openapi.json`

常用接口：

| 能力 | HTTP 路径 |
| --- | --- |
| 报价、K 线、全市场快照 | `/quote/{symbol}`、`/bars/{symbol}`、`/snapshot` |
| 标的与交易日历 | `/instruments`、`/trade-calendar` |
| 公司资料、公告、限售解禁 | `/company-profile/{symbol}`、`/disclosures/{symbol}`、`/restricted-releases/{symbol}` |
| 美股日线收盘价、运行状态 | `/us-daily-closes/{ticker}`、`/health` |

例如：

```powershell
curl http://127.0.0.1:8000/quote/600519
curl "http://127.0.0.1:8000/bars/AAPL?market=US&interval=1d&start=2026-06-01T00:00:00%2B08:00&end=2026-06-30T23:59:00%2B08:00"
```

数据接口返回 `{ "data": ..., "metadata": ... }`。金融数值以字符串编码，避免
JSON 浮点误差；时间使用 ISO 8601 格式。自动化调用应保留并判断
`metadata.source`、`metadata.stale` 与 `metadata.cache_hit`。

## 国内与海外部署互备

可在国内、海外分别启动服务，并把另一地区的 URL 配置为本地免费数据源均失败
后的最终后备。例如：

```powershell
# 国内节点
stock-data-hub serve --host 0.0.0.0 --port 8000 --peer-url http://overseas-node:9000

# 海外节点
stock-data-hub serve --host 0.0.0.0 --port 9000 --peer-url http://china-node:8000
```

Python 中等价配置：

```python
client = create_default_hub(peer_urls=["http://overseas-node:9000"])
```

本地来源始终优先。跨区域请求会自动避免双向转发循环，返回的来源字段会保留完整
的实际来源链，便于排查数据从哪个地区和服务商取得。

## 可靠性行为

- 按能力、市场、资产类型、周期和复权方式选择来源，并按优先级自动回退。
- 单个来源会进行短暂重试；连续失败会暂时熔断，冷却后恢复尝试。
- 对适合缓存的数据使用 fresh / stale 两段缓存；实时来源都失败时，旧数据会被
  显式标记为陈旧，不会伪装成实时数据。
- 相同请求会合并上游调用，避免并发重复拉取。
- 工作日的异常空 K 线会继续尝试下一来源；周末或未来日期范围可以返回合法空列表。

陈旧数据不适合作为下单或实时信号的唯一依据。业务侧应根据 `metadata.stale`
降低信号等级或停止自动任务。

## 开发与验证

```powershell
pytest
ruff check src tests
python -m build --no-isolation
```

`service`、`sources` 均为可选依赖；核心库不会强制安装 Web 服务框架或所有
第三方数据源。
