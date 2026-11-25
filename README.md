# stock-data-hub

一个把多个免费股票数据源包成统一接口的 Python 库，支持 **A 股（沪 / 深 / 北交所）、ETF、指数、港股和美股**，也可以直接启动成只读的 HTTP 接口服务。

平时写看盘脚本、做量化研究或者给 AI 工具接行情时，用免费数据源很容易遇到这几件烦心事：
- **网络水土不服**：国内直连不上 Yahoo Finance，海外服务器连东方财富或新浪又经常超时；就算在同一个地区，单个免费接口跑多了也容易限流抽风。
- **各家返回格式对不上**：成交量有的按“手”（100 股）、有的按“股”；分钟线时间戳有的标开盘那一刻、有的标收盘那一刻；上证指数 `000001.SH` 和平安银行 `000001.SZ` 一不小心还会串台。

`stock-data-hub` 做的事很直接：**按需把多个免费源串成一条备用链**。首选源挂了、超时了或者盘中数据还没更新，自动换下一个源去拉；数据返回给你之前，先把股票代码、时区、收盘时间、成交量单位全部对齐成一套标准格式。

---

## 安装

只当普通 Python 库用：

```powershell
pip install -e .
```

推荐把可选的免费数据源、技术指标（TA-Lib）和 HTTP 服务一起装上：

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e ".[dev,service,sources,indicators]"
```

Linux / macOS：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,service,sources,indicators]"
```

---

## 30 秒上手

```python
import asyncio
from datetime import datetime, timedelta
from stock_data_hub import Adjust, BEIJING_TZ, Interval, create_default_hub


async def main() -> None:
    hub = create_default_hub(
        # 可选：加一行本地缓存，反复跑脚本不用每次都去请求上游
        cache_path="~/.cache/stock_data_hub.db",
    )
    try:
        # 1. 查单只最新价
        quote = await hub.get_quote("600519")
        print(quote.value.price, quote.metadata.source)

        # 2. 一次查多只（A 股、港股、美股可以混着传，单只报错不影响其他股票）
        batch = await hub.get_quotes(["600519", "000001.SZ", "00700.HK", "AAPL.US"])
        for item in batch:
            print(item.symbol, item.data.price if item.data else item.error.message)

        # 3. 拉最近 60 天的日 K 线（支持 15m / 1h / 1d / 1w / 1M）
        end = datetime.now(tz=BEIJING_TZ)
        bars = await hub.get_bars(
            "600519",
            Interval.DAY_1,
            end - timedelta(days=60),
            end,
            Adjust.QFQ,
        )
        print(f"拿到 {len(bars.value)} 根日线，来自 {bars.metadata.source}")
    finally:
        hub.close()


asyncio.run(main())
```

每次返回的结果里都带着 `metadata`，你可以看到这笔数据到底是从哪个源拿到的（`source`）、试过哪些源（`attempted_sources`）、是不是命中了缓存（`cache_hit`），以及是不是断网兜底返回的旧数据（`stale`）。

---

## 先跑一次网络自检（`doctor`）

不同网络环境（家里宽带、国内云主机、海外 VPS、开没开代理）能连上的数据源差别很大。不确定怎么配的时候，可以先在命令行跑一下自检：

```powershell
stock-data-hub doctor
```

它会挨个实测你机器上已安装的免费源能不能连通、耗时多少毫秒，最后直接打印出一段适合你当前网络的 `create_default_hub(...)` 配置代码（自动把连不上的源关掉，把快的源排在前面）。

也可以只测指定市场或加测通达信服务器：

```powershell
stock-data-hub doctor --markets CN,US --timeout 10 --pytdx-host 119.147.212.81:7709
stock-data-hub doctor --json
```

---

## 支持的数据源与回退顺序

所有内置数据源都是免注册、免 Token 的公开接口（无需配任何第三方密钥）：

| 数据源 | 主要用途 | 怎么启用 |
| :--- | :--- | :--- |
| `tickflow` | A 股 / 港股 / 美股历史日、周、月线（不含盘中实时 K 线） | 安装 `tickflow[all]` |
| `efinance` | A 股 / ETF / 指数 K 线（含分钟线）、批量报价、全市场快照、股票列表 | 安装 `efinance` |
| `tencent` | A 股（沪 / 深 / 北 / ETF / 指数）、港股、美股轻量批量报价 | 内置，零额外依赖 |
| `akshare` | 多市场 K 线（东财 + 新浪互备）、快照、股票列表（含退市股）、日历、解禁、复权因子 | 核心依赖（兼容 1.14–1.20+） |
| `cninfo` | 巨潮资讯网公司资料、监管公告（与行情接口分开排队，互不卡死） | 内置 |
| `pytdx` | 通达信 A 股 / ETF / 指数**不复权**日线与分钟线，支持填多个服务器互备 | 安装 `pytdx` 并传入服务器地址 |
| `baostock` | A 股历史日线与分钟线（自动过滤停牌日）、ETF / 指数历史线、A 股复权因子 | 安装 `baostock` |
| `yfinance` | 港美股及全球市场日线、分钟线与延时报价 | 安装 `yfinance` |
| `exchange_calendar` | 本地离线 A 股交易日历兜底（新浪日历挂了也能用） | 内置 |
| `remote-N` | 你在另一个地区部署的 `stock-data-hub` 节点，本地全挂时跨区兜底 | 配置 `peer_urls` 或 `--peer-url` |

没装的库会自动跳过。默认回退顺序如下：

- **A 股日 / 周 / 月线**：`tickflow`（如果盘中或刚收盘还没出最新一根，会自动换后面的实时源）→ `efinance` → `pytdx`（仅不复权）→ `akshare`（优先新浪）→ `baostock` → `yfinance`
- **A 股 15m / 1h 分钟线**：`efinance` → `pytdx`（仅不复权）→ `akshare`（新浪分钟线最多只返约 1023 根，不够长时会自动切东财）→ `baostock` → `yfinance`
- **港股 / 美股 K 线**：`tickflow`（日线以上）→ `akshare`（港股全周期、美股日线以上）→ `yfinance`（含分钟线）
- **实时 / 延时报价**：A 股优先 `efinance` → `tencent` → `akshare` → `yfinance`；港美股优先 `tencent` → `akshare` → `yfinance`
- **历史复权因子**：`baostock` → `akshare`

如果你想自己调整顺序或关掉某个源，在初始化时传参即可：

```python
from stock_data_hub import AssetType, Capability, Interval, Market, RouteKey, create_default_hub

hub = create_default_hub(
    disabled_providers=["yfinance"],
    pytdx_host="119.147.212.81:7709,114.80.63.12:7709",
    source_priority={
        RouteKey(Capability.BARS, Market.CN, AssetType.STOCK, Interval.DAY_1): (
            "efinance",
            "tickflow",
            "akshare",
            "baostock",
        ),
    },
)
```

---

## 用之前建议了解的几个细节

### 1. 股票代码怎么传？
- **A 股股票和 ETF**：传 `600519`、`600519.SH`、`sh600519`、`159915.SZ`、`920001.BJ` 都可以，会自动转成统一的 `600519.SH` 格式。如果后缀写错了（比如 `600519.SZ`）或者前导零丢了（比如把 `000001` 传成了 `1`），会直接报错提醒，不会张冠李戴。
- **A 股指数**：请带上交易所后缀或前缀，比如上证指数传 `000001.SH`（或 `SH000001`），平安银行传 `000001.SZ`，两者严格区分。
- **港股与美股**：港股如 `00700.HK`（传 `700.HK` 会自动补齐 5 位零），美股如 `AAPL.US`、`BRK.B.US`。查美股 ETF（如 `SPY.US`）时可多传一个 `asset_type="etf"`。

### 2. 价格、成交量和时间是怎么统一的？
- **价格用 `Decimal`**：避免浮点数出现 `10.199999999999999` 这类精度尾巴；HTTP 接口里统一返回字符串。
- **成交量统一是“股”**：东方财富、PyTDX、TickFlow A 股原本返回的“手”已经自动乘了 100，**拿到数据后不要再自己乘 100**。成交额保留对应市场的本币（人民币、港币、美元）。
- **分钟线时间统一标“收盘那一刻”**：比如早上 `09:45–10:00` 这根 15 分钟 K 线，时间固定是 `10:00+08:00`。像 Yahoo 这种原本标开盘时间的源会自动换算成收盘时间；美股最后半小时（`15:30–16:00` 美东时间）的 `1h` K 线也会准确对齐到 `16:00` 收盘，不会因为粗暴加 1 小时越界到 `16:30` 而丢数据。
- **日线时间只是“日期标签”**：日线、周线、月线的 `ts` 统一写成该交易日的 `00:00:00+08:00`（例如美股 7 月 17 日的日线也是 `2026-07-17T00:00:00+08:00`），它只代表哪一天的 K 线，不代表收盘那一秒。判断这根 K 线收没收盘请看 `bar.is_closed` 字段（已处理好夏令时、节假日和半日市提前收盘）。
- **停牌空转行自动过滤**：有些数据源在股票停牌日会塞一行成交量为 0 的一字平盘数据，解析时会自动剔除并记录在 `metadata.quality.suspended_rows` 里，保证不同源拉出来的 K 线根数一致。

### 3. 前复权（`qfq`）、不复权（`none`）和复权因子怎么选？
- **日常看盘或一次性拉完整区间算指标**：直接传 `Adjust.QFQ` 即可。注意东方财富的前复权用的是“算术减息法”（分红多的老股票很多年前的价格可能变成 0 或负数），而新浪、BaoStock、YFinance 用的是“等比复权法”；返回结果的 `metadata.quality.adjust_method` 会标明是 `arithmetic`、`proportional` 还是 `raw`。
- **如果你要每天增量往自己的数据库里存历史数据**：**千万不要每天只拉当天的前复权价格直接往旧表后面追加**，否则一遇到分红送股，除权日前后的历史价格就会断层。正确做法是存 `Adjust.NONE`（原始不复权价），再配合复权因子接口动态计算：

```python
from datetime import date
from stock_data_hub import Adjust, Interval

raw_bars = await hub.get_bars("600519.SH", Interval.DAY_1, start, end, Adjust.NONE)
factors = await hub.get_adjust_factors("600519.SH", start=date(2020, 1, 1), end=date(2026, 7, 17))
```

### 4. 本地缓存（`cache_path`）是怎么工作的？
传入 `cache_path="~/.cache/stock_data_hub.db"` 后，数据会缓存在本地 SQLite 文件里：
- 刚拉过的 K 线默认 15 分钟内重跑脚本直接读本地，避免频繁请求免费源被封 IP；
- 如果遇到断网或所有源都挂了，7 天内的旧缓存可以作为兜底返回，同时把 `metadata.stale` 标成 `True` 提醒你这是旧数据；
- 如果你在收盘前（比如 `14:58`）拉过一次包含当天盘中未收盘的 K 线，等到 `15:05` 收盘后再跑脚本时，它会自动识别出“已经收盘了”，重新去上游拉收盘终值，不会拿收盘前的半成品糊弄你。

### 5. 关于股票列表、公告和公司资料的回测提醒
- `get_instruments()` 在走 `akshare` 时会尽量合并沪深已退市股票，并在 `Instrument.status` 中标出 `active`、`st` 或 `delisted`；如果走实时快照源则会附带“不含退市股”的警告，做历史回测选股池时请留意幸存者偏差。
- 公司资料（`get_company_profile`）和限售解禁市值反映的是当前最新分类与现价折算，不是历史某天的截面；巨潮公告（`get_disclosures`）如果上游只给了日期没给具体几点几分，A 股大量盘后公告默认应当在下一个交易日再用于回测交易，避免偷看未来信息。

---

## 计算技术指标（TA-Lib）

安装 `pip install -e ".[indicators]"` 后，可以用统一的参数算 10 种常用技术指标：`overlap.sma`、`overlap.ema`、`momentum.rsi`、`trend.macd`、`volatility.bbands`、`volatility.atr`、`momentum.stoch`、`trend.adx`、`momentum.roc`、`volume.obv`。

```python
from stock_data_hub import (
    Adjust,
    IndicatorSpec,
    Interval,
    create_default_hub,
    create_default_indicator_engine,
)

hub = create_default_hub(cache_path="~/.cache/stock_data_hub.db")
indicators = create_default_indicator_engine(hub)

result = await indicators.calculate(
    "600519.SH",
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
df = result.value.to_pandas()
print(df[["close", "rsi14", "macd.histogram", "atr14"]].tail())
```

这里专门解决了一个量化常见的坑：**EMA、RSI、MACD、ATR、ADX 这类带记忆的指标，如果只往前多拿十几根 K 线就开始算，算出来的初值和看盘软件、全历史回测能差出好几个点**。因此引擎会自动往前多拉 190～270 根已收盘 K 线做收敛预热，不管你是回测一次拉 3 年，还是实盘每天只查最后 1 天，同一天的指标数值都能对得上（默认也会自动踢掉盘中还没走完的 K 线）。

---

## 启动 HTTP 服务与跨地区互备

需要给其他语言、工作流或者 AI 工具调用时，可以启动内置的 FastAPI 服务：

```powershell
# 仅本机访问（无需配密码）
stock-data-hub serve --host 127.0.0.1 --port 8000 --cache-path ~/.cache/stock_data_hub.db
```

如果要监听 `0.0.0.0` 给局域网或外部访问，**默认必须设置访问密钥**（调用时带请求头 `X-Data-Hub-Key`；如果确实在安全的内网容器里不想设密码，需要显式加上 `--allow-unauthenticated`）：

```powershell
$env:STOCK_DATA_HUB_API_KEY = "换成一个足够长的随机密码"
stock-data-hub serve --host 0.0.0.0 --port 8000
```

启动后浏览器打开 `http://127.0.0.1:8000/docs` 就能看到完整的交互式接口文档。常用接口：

| 用途 | 接口路径 |
| :--- | :--- |
| 单只报价 / 批量报价 | `GET /quote/{symbol}`、`POST /quotes`（一次最多 100 只） |
| K 线 / 复权因子 | `GET /bars/{symbol}`、`GET /adjust-factors/{symbol}` |
| 全市场快照 / 股票列表 / 交易日历 | `GET /snapshot`、`GET /instruments`、`GET /trade-calendar` |
| 技术指标目录 / 批量计算指标 | `GET /indicators`、`POST /indicator-computations` |
| 公司资料 / 公告 / 限售解禁 | `GET /company-profile/{symbol}`、`GET /disclosures/{symbol}`、`GET /restricted-releases/{symbol}` |
| 美股收盘价 / 健康状态 / 运行计数 | `GET /us-daily-closes/{ticker}`、`GET /health`、`GET /metrics` |

### Docker 与国内外双节点互备

仓库自带 `Dockerfile`：

```powershell
docker build -t stock-data-hub:0.7.1 .
docker run --rm -p 127.0.0.1:18767:8000 `
  -e STOCK_DATA_HUB_API_KEY=replace-with-a-long-random-secret `
  stock-data-hub:0.7.1
```

如果你在国内和海外各有一台机器，可以通过 `--peer-url`（或环境变量 `STOCK_DATA_HUB_PEER_URLS`、`STOCK_DATA_HUB_PEER_API_KEY`）把对方配成最终兜底节点：平时优先走本机直连；只有本机所有免费源都拿不到数据时，才去请求对端节点，并且会自动防循环转发。

---

## 开发与测试

```powershell
$env:STOCK_DATA_HUB_REQUIRE_EXTRAS = "full"
python -m pytest -q
python -m ruff check src tests
python -m build
```

所有测试均使用本地脱敏样本和离线桩数据运行，不依赖外网实时行情即可完整验证。
