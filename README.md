# stock-data-hub

面向 **A 股（沪 / 深 / 北交所）、ETF、指数、港股与美股** 的多源金融市场数据访问层与技术指标计算引擎。项目针对国内与海外网络环境对各免费行情源的可达性差异，按**市场、资产类型、周期与复权方式**自动编排路由与故障回退，同时内置严格的金融数据清洗、跨市场交易时段对齐、递归指标收敛预热及可选的本地持久化缓存。

`stock-data-hub` 既可作为零 Web 框架依赖的 Python 异步 SDK 嵌入量化研究、回测或实盘信号系统，也可一键启动为只读 HTTP / OpenAPI 服务供跨语言服务、工作流或 AI Agent 调用。

当前版本为 **0.7.0**，行情数据契约版本为 **2.0**。升级注意事项详见 [CHANGELOG.md](CHANGELOG.md)。

---

## 核心架构与数据流全景

```text
[上游免费数据源] TickFlow / EFinance / Tencent / AkShare(东财·新浪) / Cninfo(巨潮) / PyTDX / BaoStock / YFinance / Peer节点
       │  (按源独立隔离复用工作线程槽 + 令牌桶主动限流 RateLimiter + 原生超时预算控制)
       ▼
[多源路由与容错] DataHub ──► 单飞请求合并(inflight shield) ──► 半开单探针指数退避熔断(SourceHealthRegistry)
       │  (日/周/月 EOD 历史源滞后自动切换实时源 + 已开盘时段空结果多源共识 + 跨区域 X-Data-Hub-Hop 防环兜底)
       ▼
[金融清洗归一化] normalization.py + _parsing.py ──► 符号/前导零校验、股数单位统一、交易小节收盘时刻对齐、Decimal 无损精度
       ▼
[缓存与指标引擎] InMemoryResultCache / SQLiteResultCache(WAL 无锁惰性读 + 跨收盘自动失效) ──► IndicatorEngine(收敛预热 + TA-Lib)
       ▼
[对外消费界面] Python Async SDK (`DataHub`)  │  FastAPI OpenAPI 服务 (`/bars`, `/quotes`, `/adjust-factors`, ...)  │  CLI (`serve` / `doctor`)
```

---

## 功能覆盖总览

项目专注于只读市场数据与确定性技术指标计算，不涉及交易下单、账户或持仓管理。

| 维度 | 覆盖能力 |
| :--- | :--- |
| **覆盖市场** | A 股（上交所 `.SH`、深交所 `.SZ`、北交所 `.BJ`）、港股（`.HK`）、美股（`.US`） |
| **标的类型** | 股票（`stock`）、场内 ETF（`etf`）、市场指数（`index`） |
| **行情能力** | `15m` / `1h` 分钟线、`1d` / `1w` / `1M` K 线、单票报价、跨市场批量报价（`1–100` 只）、A 股全市场快照 |
| **复权与因子** | 未复权（`none`）、前复权（`qfq`）、后复权（`hfq`），以及**独立历史除权除息复权因子序列（`adjust_factors`）** |
| **衍生指标** | 10 类版本化稳定技术指标目录、单标的多指标一次取数计算、递归指标（EMA/RSI/MACD/ATR/ADX）独立收敛预热、可复现 `FactorFrame` |
| **参考与公司数据** | A 股证券列表（含退市股合并与 ST 状态标记）、交易日历（含本地离线兜底）、公司资料、监管公告、限售解禁排期、美股日线收盘价 |
| **运维与巡检** | `stock-data-hub doctor` 实盘连通性与延迟自检、`/health` 路由级熔断状态、`/metrics` 吞吐与回退计数 |

---

## 数据源矩阵与默认路由

默认组合仅使用无需第三方 API Token 的公开数据源（同时兼容 `akshare` 1.14–1.20+ 新旧函数命名），各源拥有独立的并发槽位、令牌桶限流（`provider_rate_limits`）与路由级熔断器：

| 数据源 (`name`) | 核心能力与复权特征 | 启用条件 | 默认限速 |
| :--- | :--- | :--- | :--- |
| `tickflow` | A 股 / 港股 / 美股历史日、周、月 K 线（仅历史 EOD，等比复权） | 安装 `tickflow[all]` | `8 req/s` |
| `efinance` | A 股 / ETF / 指数全周期 K 线（算术复权）、精确 `secid` 批量报价、全市场快照、证券列表 | 安装 `efinance` | `6 req/s` |
| `tencent` | A 股（沪 / 深 / 北 / ETF / 指数）与港美股轻量 HTTP 批量报价 | 内置（零额外依赖） | `15 req/s` |
| `akshare` | 多市场 K 线（东财算术复权 + 新浪等比复权互备）、报价、快照、含退市证券列表、交易日历、限售解禁、跨市场复权因子 | 核心依赖 | `5 req/s` |
| `cninfo` | A 股公司资料（`company_profile`）与巨潮监管公告（`disclosures`），与行情源独立隔离线程池与熔断 | 内置（基于核心 `akshare`） | `4 req/s` |
| `pytdx` | A 股 / ETF / 指数未复权（`none`）日、周、月、分钟线，支持多台通达信服务器 TCP 互备 | 安装 `pytdx` 并配置服务器 | `10 req/s` |
| `baostock` | A 股日、周、月、分钟线（等比复权，自动剔除停牌日）、ETF / 指数日以上 K 线、**A 股历史复权因子** | 安装 `baostock` | `5 req/s` |
| `yfinance` | 全球市场日、周、月、分钟线（等比复权）与延时报价 | 安装 `yfinance` | `4 req/s` |
| `exchange_calendar` | 基于 `exchange-calendars` 的本地 A 股交易日历离线兜底 | 内置（核心依赖） | 本地内存 |
| `remote-N` | 另一网络区域部署的 `stock-data-hub` 只读互备节点 | 配置 `peer_urls` 或 `--peer-url` | 远端预算控制 |

### 默认路由优先级

未安装、被禁用或不支持当前请求（如交易所、资产类型、复权方式不匹配）的数据源会自动跳过，不会误计入失败或触发熔断：

| 业务场景 | 默认回退顺序 |
| :--- | :--- |
| **A 股日线 (`1d`)** | `tickflow`（盘中/刚收盘缺当期时自动降级）→ `efinance` → `pytdx`（仅 `none`）→ `akshare`（优先新浪）→ `baostock` → `yfinance` |
| **A 股周 / 月线 (`1w` / `1M`)** | `tickflow`（当周/当月滞后时自动降级）→ `efinance` → `pytdx`（仅 `none`）→ `akshare` → `baostock` → `yfinance` |
| **A 股分钟线 (`15m` / `1h`)** | `efinance` → `pytdx`（仅 `none`）→ `akshare`（优先新浪，触及 ~1023 根上限自动标 `truncated` 回退东财）→ `baostock` → `yfinance` |
| **A 股 ETF K 线** | 日线同上；分钟线：`efinance` → `pytdx`（仅 `none`）→ `akshare` → `yfinance` |
| **A 股指数 K 线** | `efinance` → `pytdx`（仅 `none`）→ `akshare` → `baostock`（仅日线以上 `none`）→ `yfinance` |
| **港股 / 美股 K 线** | `tickflow`（日线以上）→ `akshare`（港股全周期、美股日线以上）→ `yfinance`（含 `15m` / `1h`） |
| **A 股报价 / 批量报价** | `efinance`（批查）→ `tencent`（批查）→ `akshare`（单票轻量盘口优先）→ `yfinance` |
| **港股 / 美股报价 / 批量报价** | `tencent`（批查）→ `akshare` → `yfinance` |
| **复权因子 (`adjust_factors`)** | `baostock`（沪深股票精确前后复权因子）→ `akshare`（A 股 / 港股 / 美股新浪复权因子） |
| **A 股全市场快照 / 证券列表** | `efinance` → `akshare`（默认强制校验全市场有效证券数 $\ge 4000$，防止分页残缺污染缓存） |
| **公司资料 / 监管公告** | `cninfo`（独立工作槽）→ `akshare` |
| **交易日历** | `akshare` → `exchange_calendar`（本地离线兜底） |

---

## 安装指南

仅安装核心 Python SDK：

```powershell
pip install -e .
```

安装全部免费数据源、技术指标（TA-Lib）、HTTP 服务与开发测试依赖（推荐）：

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

## Python SDK 使用指南

### 1. 基础行情、批量报价与量化标准落库范式（原始价 + 独立复权因子）

> **量化防污染建议**：切勿按日增量拉取前复权（`Adjust.QFQ`）数据直接追加写入本地历史库，否则除权除息日会产生历史价格跳空断层。需要自建历史时序库时，请统一拉取 **`Adjust.NONE` 未复权 K 线 + `get_adjust_factors()` 独立复权因子**，在回测或研究时按需动态等比复权。

```python
import asyncio
import logging
from datetime import date, datetime, timedelta

from stock_data_hub import (
    BEIJING_TZ,
    Adjust,
    AssetType,
    Capability,
    Interval,
    Market,
    RouteKey,
    create_default_hub,
)

logging.basicConfig(level=logging.INFO)
logging.getLogger("stock_data_hub").setLevel(logging.INFO)

hub = create_default_hub(
    # 开启本地 SQLite WAL 持久化缓存（默认容量 16,384 条，惰性无锁读，盘中未收盘 K 线跨收盘自动失效）
    cache_path="~/.cache/stock_data_hub.db",
    # 国内无代理环境可禁用不可达源；也可先运行 `stock-data-hub doctor` 自动生成最优配置
    # disabled_providers=["yfinance"],
    # 可选：配置多台 PyTDX 通达信服务器作为 A 股未复权 TCP 兜底
    # pytdx_host="119.147.212.81:7709,114.80.63.12:7709",
    # 可选：自定义各数据源主动限流阈值（req/s）与路由优先级
    provider_rate_limits={"efinance": 6.0, "akshare": 5.0},
    source_priority={
        RouteKey(Capability.BARS, Market.CN, AssetType.STOCK, Interval.DAY_1): (
            "efinance",
            "tickflow",
            "akshare",
            "baostock",
        ),
    },
)


async def main() -> None:
    try:
        end = datetime.now(tz=BEIJING_TZ)

        # 1. 跨市场批量报价（自动按市场分组单次预热，单只失败不影响整批）
        quotes = await hub.get_quotes(["600519", "000001.SH", "000001.SZ", "00700.HK", "AAPL.US"])
        for item in quotes:
            val = item.data.price if item.data else item.error.message
            print(item.symbol, val, item.metadata.source if item.metadata else None)

        # 2. 推荐量化落盘方式：未复权原始日线 + 独立复权因子
        raw_bars = await hub.get_bars(
            "600519.SH", Interval.DAY_1, end - timedelta(days=90), end, Adjust.NONE
        )
        factors = await hub.get_adjust_factors(
            "600519.SH", start=date(2020, 1, 1), end=end.date()
        )
        print(
            f"Bars: {len(raw_bars.value)} ({raw_bars.metadata.quality.adjust_method}), "
            f"Factors: {len(factors.value)} from {factors.metadata.source}"
        )

        # 3. 美股 1h 分钟线（15:30-16:00 ET 尾盘半小时 K 线自动对齐至 16:00 ET 收盘时刻）
        us_1h = await hub.get_bars(
            "AAPL.US", Interval.HOUR_1, end - timedelta(days=10), end, Adjust.QFQ
        )
        print("US 1h:", len(us_1h.value), us_1h.metadata.source)
    finally:
        hub.close()


asyncio.run(main())
```

### 2. 证券代码归一化规则

| 市场与类别 | 规范格式示例 | 兼容输入与防错校验 |
| :--- | :--- | :--- |
| **A 股股票 / ETF** | `600519.SH`、`000001.SZ`、`159915.SZ`、`920001.BJ` | 支持裸 6 位代码（`600519`）、前缀写法（`sh600519`）；错配交易所（如 `600519.SZ`）或丢失前导零（如 `1`）直接抛 `InvalidRequest` 拒绝静默错配 |
| **A 股指数** | `000001.SH`（上证指数）、`399001.SZ`、`899050.BJ` | 严格区分 `000001.SH`（指数）与 `000001.SZ`（平安银行）；同时支持前缀写法 `SH000001`、`SH000300` |
| **港股** | `00700.HK`、`09988.HK` | `700.HK` 或 `market="HK", symbol="700"` 自动补齐 5 位前导零为 `00700.HK` |
| **美股 / 全球 ETF** | `AAPL.US`、`BRK.B.US`、`SPY.US` | 全球市场 ETF 与股票共享交易所后缀，影响路由时可显式传 `asset_type=AssetType.ETF` |

---

## 金融数据契约与防污染保障（Contract 2.0）

1. **分钟 / 小时 K 线收盘时刻对齐（防越界丢尾盘 K 线）**
   - 所有 `15m` 与 `1h` K 线的 `AShareBar.ts` 统一表示**该根 K 线的实际收盘时刻**（北京时间 `+08:00`）。
   - 对原始标签为“开盘时刻”的上游源（如 `YFinance`），适配层不仅加上周期时长，还会自动按交易所当日实际交易小节闭市边界截断对齐：例如美股常规日 `15:30 ET` 开盘的尾盘半小时 `1h` K 线精确对齐至 **`16:00 ET`**（而非越界至 `16:30 ET` 导致窗口过滤丢失），美股感恩节次日提前收盘半日市 `12:30 ET` 对齐至 **`13:00 ET`**，港股早盘 `11:30 HKT` 对齐至午休闭市 **`12:00 HKT`**。
2. **日线及以上周期标签与 `is_closed` 精确判定**
   - `1d` / `1w` / `1M` 的 `ts` 为**当地交易日期标签**（统一编码为该日期的 `00:00:00+08:00`，不可当作实际收盘瞬时时刻）。
   - `is_closed` 基于沪深（`XSHG`，收盘后留 5 分钟缓冲）、香港（`XHKG`，留 10 分钟涵盖收市竞价）、纽约（`XNYS`，自动处理夏令时与半日市提前收盘）日历精确判定。
3. **跨源单位、停牌过滤与复权算法元数据（`BarDataQuality`）**
   - **量额精度**：价格、成交量、成交额全程使用 `Decimal`；成交量统一转换为**股 / 基金份额**（自动将东财、PyTDX、TickFlow A 股的“手”乘以 100），成交额保留本币（`CNY` / `HKD` / `USD`）。
   - **停牌空转行统一剔除**：非指数日线中 `volume == 0` 且四价合一的一字停牌占位行会在解析层统一剔除并计入 `quality.suspended_rows`（不计入错误 `filtered_rows`），确保新浪、东财、BaoStock、PyTDX 返回序列对齐。
   - **复权口径显式区分**：`quality.adjust_method` 明确标记 `"raw"`（未复权）、`"proportional"`（新浪 / BaoStock / YFinance / TickFlow 等比复权）或 `"arithmetic"`（东方财富算术减息复权，高分红老股早期可能出现零或负值）。
   - **残缺与截断拦截**：`TickFlow`、`PyTDX` 及 `AkShare` 新浪分钟线触及条数上限时均显式标记 `quality.truncated = True`；Hub 会自动继续尝试完整后备源，且**绝不将截断或存在脏行过滤的残缺历史写入验证缓存**。
4. **防未来函数（PIT）与幸存者偏差提示**
   - **证券主表（`Instrument`）**：提供 `status`（`active` / `st` / `delisted`）、`list_date` 与 `delist_date`；`AkShareProvider` 会自动合并沪深终止上市股票列表以缓解回测幸存者偏差（实时快照派生的列表会附带显式非全历史警告）。
   - **公司资料 / 解禁 / 公告**：`CompanyProfile` 与 `RestrictedRelease`（含按现价折算的解禁市值）均在 `metadata.warnings` 标注非 Point-in-Time 警示；`Disclosure` 提供 `published_at`（当上游仅有日期无日内发布时间戳时自动提示盘后公告须顺延至下一交易日使用）。

---

## 技术指标与可复现 Factor Frame

安装可选依赖 `pip install -e ".[indicators]"` 后启用基于 TA-Lib 的稳定指标目录（`CATALOG_VERSION = "1.0.0"`）：

| 指标 ID | 类别 | 默认参数 | 最小出数 `default_lookback` | 收敛预热 `default_warmup` | 说明 |
| :--- | :--- | :--- | :---: | :---: | :--- |
| `overlap.sma` | 均线 | `period=20` | `19` | `19` | 简单移动平均（有限记忆） |
| `overlap.ema` | 均线 | `period=20` | `19` | `190` | 指数移动平均（递归收敛预热） |
| `momentum.rsi` | 动量 | `period=14` | `14` | `200` | Wilder 平滑 RSI（递归收敛预热） |
| `trend.macd` | 趋势 | `12, 26, 9` | `33` | `264` | MACD 快慢 EMA 与信号线（递归收敛预热） |
| `volatility.bbands` | 波动 | `period=20, std=2.0` | `19` | `19` | 布林带上中下轨（有限记忆） |
| `volatility.atr` | 波动 | `period=14` | `14` | `200` | Wilder 平滑真实波幅（递归收敛预热） |
| `momentum.stoch` | 动量 | `14, 3, 3` | `17` | `17` | 慢速随机指标 KD |
| `trend.adx` | 趋势 | `period=14` | `27` | `270` | Wilder 双重平滑 ADX / `+DI` / `-DI` |
| `momentum.roc` | 动量 | `period=10` | `10` | `10` | 变动率百分比（自动屏蔽非正复权价） |
| `volume.obv` | 量能 | 无 | `0` | `0` | 窗口起点基准能量潮（`window-seeded`） |

```python
from stock_data_hub import (
    Adjust,
    IndicatorSpec,
    Interval,
    create_default_hub,
    create_default_indicator_engine,
)

hub = create_default_hub(cache_path="~/.cache/stock_data_hub.db")
engine = create_default_indicator_engine(hub)

result = await engine.calculate(
    "600519.SH",
    Interval.DAY_1,
    start,
    end,
    [
        IndicatorSpec("momentum.rsi", {"period": 14}, alias="rsi14"),
        IndicatorSpec("trend.macd", alias="macd"),
        IndicatorSpec("trend.adx", {"period": 14}, alias="adx14"),
    ],
    adjust=Adjust.QFQ,
    closed_only=True,
)
df = result.value.to_pandas()
print(df[["close", "rsi14", "macd.histogram", "adx14.adx"]].tail())
```

**关键计算契约**：
- **`lookback` 与 `warmup` 解耦（实盘与回测信号零漂移）**：引擎按每个指标独立的收敛预热长度 `warmup`（如 `RSI(14)` 自动向前预热 200 根已收盘 K 线）单次拉取历史并各自切片计算。既保证同一请求内增删长周期指标不会改变短周期指标输出，又保证**实盘单日查询与多年全量回测在同一点位的递归指标误差 $< 10^{-4}$**；新股上市不足推荐收敛长度但已满足最小 `lookback` 时正常出数并附带 `Partial convergence warm-up` 警告。
- **非正复权价比率保护**：当算术前复权序列出现 `close <= 0` 时，`momentum.roc` 会自动将涉及零或负基准价的输出位置置为 `None` 并输出警告，防止跨零轴产生反转收益率。
- **严禁未收盘污染**：默认 `closed_only=True` 自动剔除盘中未完结 K 线；指标值仅在该根 K 线收盘后可用，回测应在下一根 K 线执行交易信号。

---

## CLI 工具与 HTTP / OpenAPI 服务

### 1. 实盘连通性自检（`stock-data-hub doctor`）

部署前可一键实测当前网络环境下各已安装数据源的可用性与真实延迟，自动生成推荐的 `create_default_hub(...)` 代码片段与环境变量（局部市场探测如 `--markets US` 不会误将跨市场源加入全局禁用列表）：

```powershell
stock-data-hub doctor
stock-data-hub doctor --markets CN,US --timeout 10 --pytdx-host 119.147.212.81:7709
stock-data-hub doctor --json
```

### 2. 启动只读 HTTP 服务（`stock-data-hub serve`）

本地回环开发模式（`127.0.0.1`）：

```powershell
stock-data-hub serve --host 127.0.0.1 --port 8000 --cache-path ~/.cache/stock_data_hub.db
```

生产或非回环监听（如 `0.0.0.0`）**默认强制要求设置 `STOCK_DATA_HUB_API_KEY`**（所有请求须携带请求头 `X-Data-Hub-Key`，采用常量时间比对防侧信道探测；若确需在受控私网内无密钥监听非回环地址，须显式传入 `--allow-unauthenticated` 或 `STOCK_DATA_HUB_ALLOW_UNAUTHENTICATED=1`）：

```powershell
$env:STOCK_DATA_HUB_API_KEY = "replace-with-a-long-random-secret"
stock-data-hub serve --host 0.0.0.0 --port 8000 --cache-path /var/lib/stock-data-hub/cache.db
```

### 3. HTTP 接口一览

所有金融数值均序列化为精确十进制字符串（避免 JSON 浮点精度丢失），时间采用带时区 ISO 8601。OpenAPI 文档位于 `/docs` 与 `/openapi.json`。

| 分类 | 方法与路径 | 说明与保护边界 |
| :--- | :--- | :--- |
| **行情** | `GET /quote/{symbol}` | 单标的实时 / 延时报价 |
| **行情** | `POST /quotes` | 批量报价（`1–100` 个代码，自动别名去重、逐项独立返回错误） |
| **行情** | `GET /bars/{symbol}` | 标准 K 线（单次窗口上限：`15m` 最多 366 天、`1h` 最多 730 天） |
| **复权** | `GET /adjust-factors/{symbol}` | 独立除权除息前后复权因子序列（支持 `start` / `end` 过滤） |
| **行情** | `GET /snapshot` | A 股全市场快照（支持 `search`、`offset`、`limit<=2000` 高效切片序列化） |
| **行情** | `GET /us-daily-closes/{ticker}` | 美股日线收盘价（支持 `max_rows<=5000` 与 `adjust=none\|qfq\|hfq`） |
| **指标** | `GET /indicators`、`GET /indicators/{id}` | 查询版本化技术指标目录与参数/预热定义 |
| **指标** | `POST /indicator-computations` | 单标的多指标计算（单次最多 32 个指标、10 万输入行、200 万输出单元） |
| **参考** | `GET /instruments`、`GET /trade-calendar` | 证券主表（含 `status`/`list_date`/`delist_date`）与交易日历 |
| **公司** | `GET /company-profile/{symbol}` | A 股公司行业与主营业务资料 |
| **公司** | `GET /restricted-releases/{symbol}` | A 股限售解禁排期 |
| **公司** | `GET /disclosures/{symbol}` | A 股监管公告查询（单次窗口上限 1825 天） |
| **运维** | `GET /health`、`GET /metrics` | 路由级熔断器健康状态、缓存命中率、回退数与活跃线程槽计数 |

---

## Docker 部署与跨区域双节点互备

构建并运行非 root 生产容器：

```powershell
docker build -t stock-data-hub:0.7.0 .
docker run --rm -p 127.0.0.1:18767:8000 `
  -e STOCK_DATA_HUB_API_KEY=replace-with-a-long-random-secret `
  stock-data-hub:0.7.0
```

国内与海外节点可通过 `STOCK_DATA_HUB_PEER_URLS` 互为最终后备（本地源始终优先，`X-Data-Hub-Hop` 自动阻断双向转发死循环，跨节点转发保留原始 `fetched_at` 与 `adjustment_as_of` 不虚增缓存寿命，且 503 错误响应自动脱敏对端内网 IP / 域名）：

| 环境变量 | 默认值 | 用途 |
| :--- | :--- | :--- |
| `STOCK_DATA_HUB_API_KEY` | 空 | 本节点 `X-Data-Hub-Key` 鉴权密钥（绑定非回环地址必填） |
| `STOCK_DATA_HUB_ALLOW_UNAUTHENTICATED` | `false` | 受控内网下显式允许非回环地址无鉴权启动 |
| `STOCK_DATA_HUB_CACHE_PATH` | 空 | 可选 SQLite 持久化缓存文件路径 |
| `STOCK_DATA_HUB_PEER_URLS` | 空 | 逗号分隔的远端互备节点 URL |
| `STOCK_DATA_HUB_PEER_API_KEY` | 空 | 请求远端互备节点时携带的密钥 |
| `STOCK_DATA_HUB_PYTDX_HOST` / `PORT` | 空 / `7709` | 逗号分隔的 PyTDX 行情服务器地址与默认端口 |
| `STOCK_DATA_HUB_DISABLED_PROVIDERS` | 空 | 逗号分隔需禁用的本地数据源名称 |
| `STOCK_DATA_HUB_PROVIDER_TIMEOUT_SECONDS` | `20` | 单次常规数据源调用超时（秒） |
| `STOCK_DATA_HUB_SNAPSHOT_TIMEOUT_SECONDS` | `45` | 全市场快照单源超时（秒） |
| `STOCK_DATA_HUB_LOCAL_TIMEOUT_SECONDS` | `90` | 单请求本地数据源合计预算（秒） |
| `STOCK_DATA_HUB_PEER_TIMEOUT_SECONDS` | `95` | 单个远端互备节点调用预算（秒） |
| `STOCK_DATA_HUB_REQUEST_TIMEOUT_SECONDS` | `240` | 单请求包含远端互备在内的总预算（秒） |

---

## 可靠性、存储并发与数据质量校验（DQ）

- **SQLite WAL 高并发持久化缓存**：采用类型化 JSON + `zlib` 压缩（严禁 `pickle` 反序列化风险），按契约版本自动隔离；读取采用 60 秒惰性访问时间刷新，使 99%+ 缓存命中保持纯只读 WAL `SELECT`，彻底消除读写锁争用；锁超时异常自动降级为缓存未命中，绝不引发 HTTP 500；本地源单键存储共享使默认容量提升至 **16,384 条**。
- **盘中转盘后跨收盘零脏读**：包含 `is_closed=False` 的盘中发展中 K 线新鲜期上限自动收紧至 15 秒，且在读取时实时复核交易小节是否已收盘——一旦跨越收盘时刻立即穿透拉取终值 K 线，杜绝盘后批处理读到收盘前半成品缓存。
- **隔离线程复用与令牌桶限流**：每个 Provider 拥有独立信号量槽位与可复用守护工作线程，卡死的第三方同步 SDK 不会提前释放槽位或波及其他源；令牌桶允许突发并发同时平滑限制持续抓取速率，配合指数退避 + 随机 Jitter 重试与半开单探针熔断保护出口 IP。

### 开发验证与自动化测试

```powershell
$env:STOCK_DATA_HUB_REQUIRE_EXTRAS = "full"
python -m pytest -q
python -m ruff check src tests
python -m build
```

测试套件内置离线真实 SDK 传输契约测试与 **5 大核心金融数据质量（DQ）回归测试**（[tests/test_dq_audit.py](tests/test_dq_audit.py)）：
1. **递归指标长短窗口收敛不变量**：验证 1 日实盘信号查询与 500 日全历史回测的 `EMA/RSI/MACD/ATR/ADX` 偏差 $< 10^{-4}$，并验证非正复权价 `ROC` 保护；
2. **跨市场非整点交易小节对齐**：验证美股常规收盘（`16:00 ET`）、半日市提前收盘（`13:00 ET`）及港股午休（`12:00 HKT`）尾盘 `1h` K 线零丢失对齐；
3. **跨数据源量价对账与复权因子闭环**：验证停牌零成交量占位行跨源一致剔除、算术/等比复权显式标记及复权因子缓存往返；
4. **盘中转盘后跨收盘缓存状态机**：验证 `14:59` 盘中缓存在 `15:05` 收盘后即刻失效重拉，以及周/月线 EOD 滞后自动切换实时源；
5. **全市场基数防残缺、分钟线截断保护与安全边界**：验证残缺快照与新浪截断分钟线不污染验证缓存、非回环无鉴权启动拦截及 503 内网地址脱敏。
