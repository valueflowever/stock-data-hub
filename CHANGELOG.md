# 更新记录

## 0.8.0

### Data Contract 2.1 与核心金融契约升级

- **复权因子仿射契约升级（`DATA_CONTRACT_VERSION = "2.1"`）**：`AdjustFactor` 新增显式 `qfq_offset` / `hfq_offset`（统一遵循 $P_{\text{adj}} = P_{\text{raw}} \times \text{factor} + \text{offset}$），严格区分“确定为 `0`”与“数据源未提供（`None`）”，禁止缺失 `offset` 静默回退为 `0`；将新浪 A 股除数型 `qfq_factor` 归一化为乘数，完整保留新浪港股后复权 `cash` 调整项，并禁用存在上游脏数据且缺失后复权的 AkShare 新浪美股复权因子。
- **周线 / 月线跨源统一周期末锚点**：`WEEK_1` 与 `MONTH_1` 在解析与窗口归一化边界统一锚定到该周期内最后一个实际交易日（如周五，遇节假日顺延至节前最后交易日），消除 Yahoo（周初/月初标签）与国内数据源之间的日期错位。
- **证券主表默认优先 `akshare`**：`Capability.INSTRUMENTS` 默认路由切换为 `("akshare", "efinance")`，开箱即默认合并沪深已退市股票及上市/退市日期，降低回测幸存者偏差。
- **时区、日历与符号归一化加固**：拆分日线日期标签与分钟线真实小节时间交集判定，修复美股夏令时正午 `12:00 EDT`（北京时间次日零点）误判无交易、固定偏移 `+08:00` 与 `ZoneInfo` 不等价及跨年周线越界问题；收紧 `normalize_symbol` 美股兜底规则，避免 `SH600519.SH`、`HK00700` 或未知后缀符号静默误入美股路由。

### 并发韧性、缓存安全与 HTTP 互备加固

- **单标的无行情与路由熔断解耦**：新增 `SymbolDataUnavailable`，查询未上市/已退市代码或盘前/停牌无成交价标的时不再误记路由级熔断，消除 2 个无效代码触发全站报价熔断的级联风险。
- **工作池、限流器与半开状态机修复**：修复 `BoundedWorkers` 在线程启动异常时的信号量双重释放与 FIFO 队列任务误删问题；将限流等待移至获取工作槽之前并支持协程取消时退还令牌；`SourceHealthRegistry` 引入探针令牌绑定与传输存活恢复。
- **缓存隔离与 SQLite WAL 读降级修复**：内存缓存返回列表增加浅拷贝防原地篡改；`SQLiteResultCache` 隔离 60 秒惰性访问刷新写锁冲突（不再吞掉已解码命中结果），消除每次写入的无索引全表扫描；`BaoStockProvider` 显式标记历史 EOD 属性并升级为进程全局会话锁，禁止 EOD 滞后降级结果长时占用新鲜缓存。
- **HTTP 服务与跨区互备加固**：新增免鉴权极简存活探针 `GET /livez`（配合 Docker `HEALTHCHECK`）与 `create_app` `lifespan` 关闭钩子；修复 `/bars` 混传 naive/aware 时间导致的 500 错误，为 `/indicator-computations` 补齐分钟线跨度护栏并限制指标预热最大回溯跨度；`RemoteHubProvider` 对路径参数全面百分号编码并兼容 Python 3.10 `'Z'` 时间戳；发版工作流增加 `pytest` 门禁。

## 0.7.1

### 核心金融准确性与指标修复

- **递归指标收敛预热（`warmup` 与 `lookback` 解耦）**：为 `EMA`、`Wilder RSI`、`MACD`、`Wilder ATR`、`Wilder ADX` 引入独立收敛预热窗口（190–270 根 K 线），消除实盘短窗口查询与全量历史回测之间的初值未收敛偏差；同时为 `momentum.roc` 增加非正算术复权价屏蔽保护。
- **跨市场分钟线交易小节精确对齐**：修复 `OPEN` 标签 `1h` K 线在美股收盘（`15:30–16:00 ET`）、美股半日市（`12:30–13:00 ET`）与港股早盘（`11:30–12:00 HKT`）盲加 1 小时越界导致尾盘 K 线被窗口过滤丢弃的问题。
- **独立复权因子与复权口径透明化**：新增 `Capability.ADJUST_FACTORS`（`hub.get_adjust_factors` 与 `GET /adjust-factors/{symbol}`，支持 BaoStock 与 AkShare 跨市场复权因子）；在 `BarDataQuality.adjust_method` 显式区分 `raw`、`proportional`（等比）与 `arithmetic`（算术减息）。
- **跨源单位、停牌过滤与符号归一化**：统一 K 线成交量为股 / 基金份额，自动过滤非指数日线零成交量平盘停牌占位行（记录 `suspended_rows`）；精确区分 `000001.SH` 指数与 `000001.SZ` 股票，兼容 `SH000001`/`SH000300` 前缀与北交所 `920xxx`；全面兼容 `akshare` 1.14–1.20+ 新旧函数签名。
- **防未来函数（PIT）与幸存者偏差治理**：`Instrument` 新增 `status`、`list_date`、`delist_date` 并自动合并沪深退市股票列表；`Disclosure` 新增 `published_at` 并为公司资料、解禁与无日内时间戳公告补充显式非 PIT 警示。

### 可靠性、存储并发与安全加固

- **SQLite WAL 持久化缓存（`SQLiteResultCache`）**：支持跨进程 WAL 模式、60 秒惰性访问时间刷新（纯读零写锁争用）、锁冲突优雅降级、单份本地/远端作用域去重（默认容量 16,384 条）。
- **盘中转盘后跨收盘零脏读与全周期滞后检测**：含 `is_closed=False` 的发展中 K 线限制短新鲜期并在跨越收盘时刻后立即失效重拉；EOD 历史源滞后自动回退扩展至日、周、月全周期。
- **防残缺入库与截断检测**：默认对 A 股全市场快照与证券列表实施 $\ge 4000$ 行基数水位线校验；补齐新浪分钟线 ~1,023 根上限 `truncated=True` 检测；残缺或截断结果绝不写入验证缓存。
- **隔离线程复用、令牌桶限流与熔断**：新增 `CninfoProvider` 将巨潮公告/资料与东财/新浪行情工作槽及熔断器解耦；`BoundedWorkers` 支持守护线程复用与 `RateLimiter` 令牌桶限速；重试升级为指数退避加随机 Jitter，熔断器支持半开单探针与指数冷却。
- **新增轻量数据源与自检工具**：新增零依赖 `TencentQuoteProvider`（A/港/美批量报价）、本地离线 `ExchangeCalendarProvider` 以及 `stock-data-hub doctor` 实盘连通性自检（修复局部市场探测误生成全局禁用问题）。
- **HTTP 服务安全与边界保护**：非回环地址（如 `0.0.0.0`）未配置 `STOCK_DATA_HUB_API_KEY` 时默认拒绝裸奔启动（需显式 `--allow-unauthenticated`）；远端互备报错自动脱敏内网 Peer URL；修复 `/snapshot` 与 `/trade-calendar` 全表重复序列化，并为 `/bars` 与 `/disclosures` 增加单次时间跨度上限。

### 从 0.6.x 升级说明

1. 更新依赖：核心新增 `exchange-calendars>=4.5,<5`，可选源建议 EFinance ≥ 0.5.9、TickFlow ≥ 0.1.25；HTTP 服务使用 Pydantic 2。
2. 同步升级互备节点：远端 K 线响应必须声明 `contract_version="2.0"`，旧节点将被拒绝。
3. 重新拉取已持久化的旧版 K 线与衍生指标缓存：0.7.1 统一了股数单位、美股日线日期标签及递归指标收敛预热长度。
4. 若在容器中使用 `0.0.0.0` 监听，请配置环境变量 `STOCK_DATA_HUB_API_KEY`（或在受控内网显式设置 `STOCK_DATA_HUB_ALLOW_UNAUTHENTICATED=1`）。
