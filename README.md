# stock-data-hub

面向 A 股、ETF、指数、港股和美股的多源股票市场数据包。它针对国内与海外
网络环境对数据服务商的可达性差异，按市场、标的类型和数据能力自动选择可用
的匿名数据源，并在单一来源失败时回退到后备来源。

`stock-data-hub` 可作为 Python 库嵌入应用，也可启动为只读 HTTP / OpenAPI
服务，供其他语言、工作流或 AI 工具使用。

当前版本为 **0.7.0**，行情数据契约为 **2.0**。从旧版升级时，需要同步升级远端
节点，并重新获取已持久化的旧 K 线；成交量单位与美股日线时间标签有兼容性变更。
详见 [CHANGELOG.md](CHANGELOG.md)。

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

    batch = await client.get_quotes(["600519", "000001.SZ", "00700.HK"])
    for item in batch:
        print(item.symbol, item.data.price if item.data else item.error.message)

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
| A 股 / ETF | `600519.SH`、`159915.SZ`、`920001.BJ`；六位代码可省略后缀 |
| 港股 | `00700.HK` |
| 美股 | `AAPL.US` |

港股和美股建议显式附加后缀。无法仅从代码区分全球 ETF 与股票时，可传入
`asset_type=AssetType.ETF`。

`000001.SH` 是上证指数，`000001.SZ` 是平安银行；不能丢弃交易所后缀。北交所的
`920xxx`、`4xxxxx`、`8xxxxx` 代码统一归入 `.BJ`。

```python
from stock_data_hub import AssetType, Interval

bars = await client.get_bars(
    "SPY.US", Interval.DAY_1, start, end, asset_type=AssetType.ETF
)
```

## K 线数据契约

Hub 对外返回的分钟 K 线 (`15m`、`1h`) 中，`AShareBar.ts` 始终表示该根
K 线的**收盘时刻**，而不是任意上游数据源的原始标签。例如，结束于
`10:00` 的 15 分钟 K 线返回 `10:00+08:00`。

日线及更高周期的 `ts` 是**交易日期 / 周期日期标签**，统一编码为北京时间
`00:00`，不能当成实际收盘时刻。例如，美股 7 月 17 日的日线返回
`2026-07-17T00:00:00+08:00`；其实际收盘发生在北京时间次日凌晨。

每个 provider 都必须在适配边界显式声明上游分钟线标签是开盘还是收盘；开盘
标签会在进入 Hub 契约前转换为收盘时刻。调用方不需要、也不应根据数据源再
自行加上周期。

`is_closed` 按沪深、香港、纽约交易日历判断，覆盖休市、夏令时、提前收盘和周 / 月
最后一个交易日。A 股在收盘后预留 5 分钟，港股预留 10 分钟以涵盖收市竞价。
交易日历超出已知范围时采用保守规则：当地整个周期日期过去后才视为收盘。

成交量统一使用**股 / 基金份额**，指数使用成分证券成交股数；成交额使用对应市场
本币（CNY、HKD、USD），不做汇率转换。EFinance、AkShare 东方财富、PyTDX 与
TickFlow 免费 A 股接口的“手”会转换为“股”；BaoStock、Sina、YFinance 和
TickFlow 港美股数据保留股数。调用方不要再次乘以 100。

未复权 OHLC 必须为正数。前 / 后复权历史允许有限的零值或负值，但仍校验 OHLC
大小关系。无效行和重复时间戳会在解析阶段计数，缺失量额保留为 `None` / `null`。

历史响应的 `metadata.quality` 描述实际覆盖范围：

| 字段 | 含义 |
| --- | --- |
| `requested_start` / `requested_end` | 请求边界 |
| `available_start` / `available_end` | 返回的首末时间标签，空结果为 `null` |
| `input_rows` / `returned_rows` | 上游原始行数 / 请求范围内有效返回行数 |
| `filtered_rows` / `duplicate_rows` | 无效行 / 重复时间戳数量 |
| `truncated` | 已确认触及上游分页或条数限制 |
| `missing_rows` | 可确认的缺失数量；无法可靠判断时为 `null` |
| `volume_unit` / `currency` | 固定 `shares` / 成交额币种 |

不能仅用首末时间或交易日历推断停牌、新上市证券缺了多少根 K 线。
TickFlow 会向前翻页，默认每页最多 10,000 行、总计最多 100,000 行；触顶会标记
`truncated=true`。遇到被截断或过滤的历史，Hub 会优先尝试其他完整来源；只剩部分
数据时保留质量标记与警告，并且不将其写入已验证缓存。

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
  `adjustment_as_of` 和显式警告。该时间沿用行情的原始采集时间，经过远端转发也不重置。
- 默认单次最多 32 个指标、100,000 根输入 K 线和 2,000,000 个输出单元；指标计算使用
  独立的受限线程槽，默认最多 2 个同时计算，等待超过 5 秒返回 503。
- 分钟线 warm-up 至少回看 7 天，并最多扩窗 4 次，以覆盖周末、假日或停牌。
  取消请求后，正在运行的原生计算仍占据其槽位，直到真正结束。

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
| 报价、批量报价、K 线、全市场快照 | `/quote/{symbol}`、`POST /quotes`、`/bars/{symbol}`、`/snapshot` |
| 指标目录、单标的多指标 | `GET /indicators`、`POST /indicator-computations` |
| 标的与交易日历 | `/instruments`、`/trade-calendar` |
| 公司资料、公告、限售解禁 | `/company-profile/{symbol}`、`/disclosures/{symbol}`、`/restricted-releases/{symbol}` |
| 美股日线收盘价、运行状态、计数指标 | `/us-daily-closes/{ticker}`、`/health`、`/metrics` |

例如：

```powershell
curl http://127.0.0.1:8000/quote/600519
curl "http://127.0.0.1:8000/bars/AAPL?market=US&interval=1d&start=2026-06-01T00:00:00%2B08:00&end=2026-06-30T23:59:00%2B08:00"
```

批量报价使用 `POST /quotes`，请求体例如：

```json
{"symbols": ["600519", "600519.SH", "000001.SZ", "00700.HK"]}
```

一次接收 1–100 个代码，规范化后去重并保持首次出现的顺序，最多同时执行 8 个单项
请求。响应为 `{"data": [{"symbol": "...", "data": {...}, "metadata": {...}, "error": null}]}`。
失败项的 `data`、`metadata` 为 `null`，`error` 含 `type` 和 `message`；单个无效代码
或来源故障不会丢失其他结果。AkShare 股票报价共用短期全市场快照，EFinance 报价
按精确行情 ID 查询并短期缓存。

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

单项数据接口返回 `{ "data": ..., "metadata": ... }`。金融数值以字符串编码，避免
JSON 浮点误差；时间使用 ISO 8601 格式。OpenAPI 包含具体数据、元数据、分页和
动态指标列的响应类型。

`metadata.fetched_at` 是原始采集时间；`received_at` 是本节点收到来源结果的时间；
`age_seconds` 是返回时距原始采集经过的秒数，缓存命中时会更新。跨节点转发不会
重置数据年龄或延长其可缓存期限。`cache_hit` 可来自本节点或上游缓存；`stale`
表示已超过本节点新鲜期或上游明确返回了陈旧数据。

`GET /metrics` 返回请求数、本地新鲜缓存命中数、陈旧返回数、合并请求数、回退成功数、
失败数和各来源当前仍在运行的调用数；`/health` 额外给出各路由超时次数与容量拒绝次数。
这些指标在当前进程内累计，重启归零；两个接口沿用 `X-Data-Hub-Key` 鉴权。

### Docker 镜像

仓库自带生产镜像，默认安装 HTTP 服务和全部免费数据源：

```powershell
docker build -t stock-data-hub:0.7.0 .
docker run --rm -p 127.0.0.1:18767:8000 `
  -e STOCK_DATA_HUB_API_KEY=replace-with-a-long-random-secret `
  stock-data-hub:0.7.0
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
STOCK_DATA_HUB_REQUEST_TIMEOUT_SECONDS=240
STOCK_DATA_HUB_LOCAL_TIMEOUT_SECONDS=90
STOCK_DATA_HUB_PEER_TIMEOUT_SECONDS=95
```

`STOCK_DATA_HUB_PEER_URLS` 支持逗号分隔多个节点。跨公网连接必须使用带鉴权的
HTTPS，优先放在 WireGuard、Tailscale 或云厂商私网中；不要把无鉴权服务直接暴露
在公网。

全市场快照默认给每个本地供应商 45 秒，其他能力为 20 秒；同一次请求中，本地来源
合计最多 90 秒，每个远端节点最多 95 秒（涵盖其 90 秒本地路由及网络开销），全部
来源合计最多 240 秒。重试与排队也消耗对应预算。

可分别使用 `--snapshot-timeout`、`--local-timeout`、`--peer-timeout`、
`--request-timeout` 或上述环境变量调整。自定义远端节点预算时，应让调用端的 peer
预算覆盖对端的 local 预算，并为 HTTP 客户端和反向代理设置相应的总体超时。

Python 中等价配置：

```python
client = create_default_hub(
    peer_urls=["http://overseas-node:9000"],
    peer_api_key="peer-secret",
    local_timeout_seconds=90,
    peer_timeout_seconds=95,
    request_timeout_seconds=240,
)
```

本地来源始终优先。跨区域请求会自动避免双向转发循环，返回的来源字段会保留完整
的实际来源链，便于排查数据从哪个地区和服务商取得。

远端 K 线必须声明 `metadata.contract_version="2.0"`。旧版节点的成交量单位不明确，
会被拒绝并尝试下一来源。远端快照和标的列表分页时，若总数、原始采集时间或页面
发生不一致，会报错重试，避免返回拼接了不同批次的数据。

## 可靠性行为

- 按能力、市场、资产类型、周期和复权方式选择来源，并按优先级自动回退。
- 单个来源会进行短暂重试；连续失败会暂时熔断，冷却后恢复尝试。
- 对适合缓存的数据使用 fresh / stale 两段缓存；实时来源都失败时，旧数据会被
  显式标记为陈旧，不会伪装成实时数据。
- 相同请求会合并上游调用，避免并发重复拉取。
- 任意一个等待者取消不会取消其他等待者需要的同一上游调用。
- 每个 provider 有独立的并发槽，默认 8 个；`DataHub(max_concurrent_requests=...)`
  从 0.7.0 起表示每来源上限。单一 SDK 卡住不会耗尽其他来源或指标计算的槽位。
- 原生 SDK 线程无法被 Python 安全强制终止。超时后立即回退、不重复启动同一来源，
  原线程结束前仍占用自己的槽位；已支持的 SDK 同时配置原生网络超时和分页预算检查。
- 最多合并管理 512 个不同的在途请求，容量不足会拒绝新请求。来源排队拥塞单独计数，
  不作为上游故障触发熔断。
- 工作日的异常空 K 线会继续尝试下一来源；只有各来源都确认空结果时才返回空历史，
  不会把“一个空结果加一个故障”解释为无数据。周末或未来日期范围允许合法空列表。
- `hub.clear_cache()` 同时清除 Hub 缓存和适配器共享快照缓存。

陈旧数据不适合作为下单或实时信号的唯一依据。业务侧应根据 `metadata.stale`
降低信号等级或停止自动任务。

## 开发与验证

```powershell
pip install -e ".[dev,service,sources,indicators]"
$env:STOCK_DATA_HUB_REQUIRE_EXTRAS = "full"
python -m pytest -q
python -m ruff check src tests
python -m build
```

Linux 可用 `STOCK_DATA_HUB_REQUIRE_EXTRAS=full python -m pytest -q`。完整验证模式会
在缺少 HTTP、TA-Lib 或数据源 SDK 时直接失败，避免可选测试被静默跳过。

GitHub Actions 覆盖 Linux / Python 3.10、Windows / Python 3.12 的核心与完整依赖
组合，并执行 Ruff 和 wheel / sdist 构建。核心组合允许跳过未安装的可选功能；完整
组合必须执行它们。测试使用脱敏历史样本与离线传输替身，不依赖实时行情可达性；
样本来源见 [tests/fixtures/README.md](tests/fixtures/README.md)。

`service`、`sources`、`indicators` 均为可选依赖；核心库不会强制安装 Web 服务框架、
TA-Lib 或所有第三方数据源。
