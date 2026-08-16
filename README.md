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
| 派生数据 | 稳定技术指标目录、单标的多指标计算、可复现 factor frame |
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
pip install -e ".[dev,service,sources,indicators]"
```

Linux 或 macOS：

```bash
source .venv/bin/activate
pip install -e ".[dev,service,sources,indicators]"
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

## K 线时间契约

Hub 对外返回的分钟 K 线 (`15m`、`1h`) 中，`AShareBar.ts` 始终表示该根
K 线的**收盘时刻**，而不是任意上游数据源的原始标签。例如，结束于
`10:00` 的 15 分钟 K 线返回 `10:00+08:00`。日线及更高周期保留其规范化的
交易周期日期。

每个 provider 都必须在适配边界显式声明上游分钟线标签是开盘还是收盘；开盘
标签会在进入 Hub 契约前转换为收盘时刻。调用方不需要、也不应根据数据源再
自行加上周期。

> **0.5.1 迁移说明：** 分钟 K 线时间语义现由 Hub 明确保证。自定义 provider
> 若复用内部 DataFrame 解析器，必须声明其 `intraday_timestamp_label`；远端 Hub
> 节点则应返回同一收盘时间契约。

PyTDX 只在显式给出行情服务器时启用：

```python
client = create_default_hub(pytdx_host="119.147.212.81", pytdx_port=7709)
```

## 技术指标与 factor frame

指标能力是可选依赖，不会扩大只使用行情 Hub 时的安装面：

```powershell
pip install -e ".[indicators]"
```

当前稳定目录包含 SMA、EMA、Wilder RSI、MACD、Bollinger Bands、Wilder ATR、
Stochastic、ADX/DMI、ROC 和 OBV。公共指标 ID、参数、输出和公式变体由项目目录
固定，不会根据本机安装的第三方库动态变化。

一次请求会先取得一份规范化 K 线，再计算多个指标：

```python
from stock_data_hub import (
    Adjust,
    IndicatorSpec,
    Interval,
    create_default_hub,
    create_default_indicator_engine,
)

hub = create_default_hub()
indicators = create_default_indicator_engine(hub)
result = await indicators.calculate(
    "600519",
    Interval.DAY_1,
    start,
    end,
    [
        IndicatorSpec("momentum.rsi", {"period": 14}, alias="rsi14"),
        IndicatorSpec("trend.macd", alias="macd"),
        IndicatorSpec("volatility.atr", {"period": 14}, alias="atr14"),
    ],
    adjust=Adjust.QFQ,
)
factor_frame = result.value.to_pandas()
print(factor_frame[["close", "rsi14", "macd.histogram", "atr14"]].tail())
```

计算契约：

- 默认只使用 `is_closed=True` 的 K 线，并按每个指标的精确 lookback 自动向前补数。
- 结果只返回请求区间；HTTP 缺失值为 `null`，不补零、不前向填充。
- 同一请求中的多个指标共用一次取数，但各自使用独立 warm-up，因此加入长周期指标
  不会改变已有短周期指标值。
- `metadata.computation` 记录目录/后端版本、实际计算起点、warm-up 行数和输入 SHA-256，
  原有 `source`、`stale`、`cache_hit` 继续描述底层行情。
- EMA、RSI、MACD、OBV 等路径相关指标应一次计算完整回测区间，不要拼接多个短窗口。
  OBV 的稳定变体为 `window-seeded`，绝对值以本次计算窗口起点为基准。
- QFQ 历史可能在后续公司行为发生后被重算，不等同于 point-in-time 数据；响应会携带
  `adjustment_as_of` 和显式警告。
- 默认单次最多 32 个指标、100,000 根输入 K 线和 2,000,000 个输出单元；指标计算使用
  独立的受限线程槽，不占用行情 provider 的并发预算。

指标值在该根 K 线收盘后才可用。回测使用者应默认在下一根 K 线执行信号，避免用
当前收盘指标在同一收盘价成交。

## HTTP / OpenAPI 服务

安装 `service` extra 后可启动本地只读服务：

```powershell
stock-data-hub serve --host 127.0.0.1 --port 8000
```

部署环境应设置服务密钥；设置后，所有数据与健康检查请求都必须携带
`X-Data-Hub-Key`：

```powershell
$env:STOCK_DATA_HUB_API_KEY = "replace-with-a-long-random-secret"
stock-data-hub serve --host 0.0.0.0 --port 8000
curl -Headers @{ "X-Data-Hub-Key" = $env:STOCK_DATA_HUB_API_KEY } `
  http://127.0.0.1:8000/health
```

密钥为空时保持无鉴权模式，仅适合回环地址或受控的容器内网。

- OpenAPI 文档：`http://127.0.0.1:8000/docs`
- OpenAPI JSON：`http://127.0.0.1:8000/openapi.json`

常用接口：

| 能力 | HTTP 路径 |
| --- | --- |
| 报价、K 线、全市场快照 | `/quote/{symbol}`、`/bars/{symbol}`、`/snapshot` |
| 指标目录、单标的多指标 | `GET /indicators`、`POST /indicator-computations` |
| 标的与交易日历 | `/instruments`、`/trade-calendar` |
| 公司资料、公告、限售解禁 | `/company-profile/{symbol}`、`/disclosures/{symbol}`、`/restricted-releases/{symbol}` |
| 美股日线收盘价、运行状态 | `/us-daily-closes/{ticker}`、`/health` |

例如：

```powershell
curl http://127.0.0.1:8000/quote/600519
curl "http://127.0.0.1:8000/bars/AAPL?market=US&interval=1d&start=2026-06-01T00:00:00%2B08:00&end=2026-06-30T23:59:00%2B08:00"
```

多指标计算使用 JSON 请求体，并要求显式指定复权方式：

```json
{
  "symbol": "600519",
  "interval": "1d",
  "start": "2025-01-01T00:00:00+08:00",
  "end": "2026-01-01T00:00:00+08:00",
  "adjust": "qfq",
  "closed_only": true,
  "indicators": [
    {"id": "momentum.rsi", "alias": "rsi14", "params": {"period": 14}},
    {"id": "trend.macd", "alias": "macd", "params": {}}
  ]
}
```

factor frame 的 `rows` 使用平铺结构，例如 `ts`、OHLCV、`rsi14`、`macd.macd`、
`macd.signal` 和 `macd.histogram`。金融数值仍以字符串编码，warm-up 或输入不足使用
`null`。未安装 `indicators` extra 时目录仍可查询，但会标记 `available=false`，计算请求
返回 503，不影响其他行情接口。

数据接口返回 `{ "data": ..., "metadata": ... }`。金融数值以字符串编码，避免
JSON 浮点误差；时间使用 ISO 8601 格式。自动化调用应保留并判断
`metadata.source`、`metadata.stale` 与 `metadata.cache_hit`。

### Docker 镜像

仓库自带生产镜像，默认安装 HTTP 服务和全部免费数据源：

```powershell
docker build -t stock-data-hub:0.6.0 .
docker run --rm -p 127.0.0.1:18767:8000 `
  -e STOCK_DATA_HUB_API_KEY=replace-with-a-long-random-secret `
  stock-data-hub:0.6.0
```

容器以非 root 用户运行。正式部署应把镜像推送到私有或可信镜像仓库，并固定版本，
不要让业务项目通过宿主机绝对路径安装源码。

## 国内与海外部署互备

可在国内、海外分别启动服务，并把另一地区的 URL 配置为本地免费数据源均失败
后的最终后备。例如：

```powershell
# 国内节点
stock-data-hub serve --host 0.0.0.0 --port 8000 --peer-url http://overseas-node:9000

# 海外节点
stock-data-hub serve --host 0.0.0.0 --port 9000 --peer-url http://china-node:8000
```

容器部署可完全使用环境变量：

```env
STOCK_DATA_HUB_API_KEY=<本节点访问密钥>
STOCK_DATA_HUB_PEER_URLS=https://overseas-hub.internal
STOCK_DATA_HUB_PEER_API_KEY=<对端节点访问密钥>
STOCK_DATA_HUB_PYTDX_HOST=
STOCK_DATA_HUB_SNAPSHOT_TIMEOUT_SECONDS=45
```

`STOCK_DATA_HUB_PEER_URLS` 支持逗号分隔多个节点。跨公网连接必须使用带鉴权的
HTTPS，优先放在 WireGuard、Tailscale 或云厂商私网中；不要把无鉴权服务直接暴露
在公网。

全市场快照需要分页读取数千只股票，默认给每个供应商 45 秒；报价、K 线等接口仍使用
20 秒预算。网络较慢的区域可以通过 `STOCK_DATA_HUB_SNAPSHOT_TIMEOUT_SECONDS` 单独调整，
无需放大其他接口的超时。

Python 中等价配置：

```python
client = create_default_hub(
    peer_urls=["http://overseas-node:9000"],
    peer_api_key="peer-secret",
)
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
