# A 股与港股通模拟扫描和盘中监控

这套能力只做研究、模拟记录和人工复核提醒，不连接券商，也不会自动下单。
原有收盘分析与 20 个交易日模拟状态继续由 `00-daily-analysis.yml` 保存；新增
工作流不会重置该状态。

## 云端运行节奏

- `00-daily-analysis.yml`：交易日北京时间 18:00 执行收盘分析和模拟记账。
- `01-intraday-session.yml`：不再配置 GitHub 原生 `schedule`。外部
  cron-job.org 在交易日北京时间 09:20 以 `workflow_dispatch` 触发
  `session=morning`，12:50 触发 `session=afternoon`。GitHub Actions 页面仍可用
  Run workflow 手动选择 `auto`、`morning` 或 `afternoon`。每个连续监控时段正常约
  60 秒取一次基础实时行情；A 股使用腾讯批量行情，港股在配置 Longbridge 凭据后
  优先使用带最新成交时间戳的授权 L1 批量行情。港股连接建立时验证 OpenAPI 港股
  个股实时权限，每轮验证本次快照拉取和逐只提供方时间戳；最新成交时间超过交易新鲜度
  阈值时，实时链路仍可保持健康，但该价格不会触发买卖建议。价格临近风险位或
  条件位时约 30 秒一次，数据源降级时放慢到约 120 秒。腾讯港股延迟行情只保留
  为诊断证据，不会被放宽新鲜度后用于买卖判断。每次未取消的盘中 Actions 都会
  强制执行 Longbridge、PRIMARY、交易日历和 PushPlus 严格验收，证据不完整即失败。
- `02-market-scan.yml`：北京时间 10:30、14:30 和 19:15 分层扫描 A 股全市场与
  港股通成分。全市场阶段不调用模型；只对规则短名单加载历史，再让通义千问和
  DeepSeek 独立复核。A 股快照按东财、Sina 双源重试和降级；港股通成分接口异常时，
  只能使用独立保存且仍在有效期内的已验证成员集合过滤新鲜全港行情，不会把
  非港股通标的混入。A/H 两个市场独立隔离：一个市场不可用时只封锁该市场，另一市场
  仍可完成双模型复核和条件买入计划；整轮会标记为 `degraded` 并接受严格门禁检查。
  上午和下午的盘中长任务会在 10:42、14:42 检查对应扫描是否漏跑，18:00 收盘任务
  会在 19:27 检查收盘扫描；缺失时只补触发一次。原任务与兜底任务共享时段账本和
  concurrency，同一时段成功后不重复执行，超过最晚边界则只记审计、不追补旧行情。
- `04-longbridge-preflight.yml`：手动、只读检查 Longbridge OAuth 行情包与港股
  提供方时间戳；该工作流不注入 PushPlus，不创建交易上下文，也不下单。

外部触发和 GitHub runner 仍可能排队，因此这些时间不是交易所级低延迟保证。任何
超过
90 秒、缺少时间戳或关键字段不完整的最新成交价，都不能触发交易类人工复核提醒。
Longbridge 官方字段中的 `timestamp` 是“最新价格时间”而不是响应生成时间；已验证
实时权限且本次成功返回的快照，若只是因为标的没有近期成交而超过 90 秒，会单独
记录为“无近期成交快照”，不冒充可交易新鲜价，也不误计为行情接口降级。
若 dispatch 到达时原定交易时段已经结束，系统会保留上午/下午的原始审计身份、
保留最近有效状态、记录 `late_schedule_skipped`，并通过可重试 outbox 发送该时段的
能力提醒；不会把迟到的上午任务改写成下午任务，也不会回填或伪造已经错过的盘中
行情。GitHub 任务摘要会明确标记“未执行”。触发迟到不能被解释为盘中监控成功；
若需要有服务等级保证的实时监控，必须迁移到持久云端运行器或合法行情商的实时
推送服务。

## 决策规则

监控频率和交易频率彼此独立。系统可以频繁观察，但只有当候选在扣除手续费、
税费和滑点后，预期风险收益比与年化效用明显优于现有方案时，才允许产生人工
复核事件。报告可安全推送不等于候选可交易；全市场候选还必须逐只满足完整深研、
双模型一致、无硬风险、进入新鲜有效价格区间等条件。

分钟循环只使用确定性规则、最新可靠行情、已落盘的止损/目标、可信候选计划和
模拟持仓，不在每轮新增大模型调用。`sharp_rise`、`sharp_drop`、`stop_loss`、
`target_reached`、adaptive、数据质量、信号时间、行情时间、价格、原因和决策结果
仍完整留账，但原始事件不会未经决策加工直接推送。

用户端 Push 只回答“现在怎么办”：仅当操作建议改变、风险实质升级、有效
突破/跌破新的可靠关键位、相对上次显著恶化、风险解除后重新触发，或新可靠计划
替换旧计划时发送。普通涨跌阈值触发但最佳动作仍为“不操作”时不推送；冷却时间
届满本身也不会机械重发。推送中的买入、加仓、减仓、止盈和止损均给出当前价格、
建议数量或仓位比例、触发价、委托参考价、失效条件、有效期、可靠依据、下一触发
条件和数据质量；下单仍需人工确认，系统不会连接券商。没有可靠资金与仓位尺寸的
候选观察只留审计记录，不冒充可执行买入建议，也不编造持仓、成本、Level-2、资金、
新闻或盘口。

数据覆盖不足只有在连续多轮且已经影响决策可靠性时推送一次降级提醒；同一故障或
部分降级状态不重复刷屏，可靠行情恢复后只推送一次恢复通知并清除告警状态。扫描
报告会分别写明 A 股、港股通的可用状态、来源、抓取时间、提供方时间和阻断原因，
不会因一个市场故障把另一个市场的事实误写成“快照不可用”。

全市场扫描仍未取得完整基本面、公告、政策、资金和授权 Level-2，因此不会输出
盘口或资金流伪结论。但若规则过滤、完整价格计划、扣费后风险收益、数据质量和
通义千问 + DeepSeek 独立复核全部通过，候选会标记为 `conditional_buy`；只有下一次
分钟监控拿到新鲜行情且价格进入买入区，才发送首笔 2.5%–10% 模拟净值的动态建仓
建议，仍需人工确认。每份扫描报告同时展示“全市场输入 → L1 → 历史/计划 → 双模型 → 可建仓”
漏斗及逐层淘汰原因；没有候选是正常结果，输入为零则是运行故障。

## 死拿与策略影子账户实验

实验以 2026-08-07 收盘为冻结时点，从下一个交易日开始比较用户确认的 14 只核心
持仓；被明确排除的微小持仓不计入。A 账户 `buy_and_hold_baseline` 永久保持冻结
数量，只按可靠收盘价更新净值。B 账户 `strategy_shadow_portfolio` 从完全相同的
持仓、初始净值和零现金出发，只执行实验开始后实时产生并立即落盘的明确模拟决策；
卖出现金留在账户内，后续只有新有效买入/加仓信号才可使用。

策略影子账户把股票仓位 75%–85%、现金 15%–25% 作为常规目标带，而不是禁止优质
机会使用资金的绝对边界。现金已经达到或超过25%时，普通目标位止盈不再继续机械
减仓，而是明确提示“建议不减仓，继续持有”；硬止损和确定性风险退出始终优先，
不受现金护栏阻断。

买入侧按同一套确定性证据分级：普通机会的现金软底线/单股上限为15%/15%，强机会为
5%/35%，只有全市场技术排名前三、数据质量不低于0.80、扣费后风险回报比不低于2.0，
且千问与DeepSeek置信度均不低于0.90的极强机会，才允许现金最低接近0%、单股最高
接近50%。仓位增量同样分级：普通机会首笔/加仓为2.5%/2.5%，强机会为5%/5%，极强
机会首笔为10%、后续每次加仓为5%。若推荐档位受剩余现金或单股上限约束，系统会按
10%→5%→2.5%自动降一级，而不是因固定档位直接错过仍可承受的机会。每次加仓都必须
来自更新的一轮可信扫描和双模型一致复核，且当前价格不得低于该持仓的实验基准价。
所有档位都会先扣除待执行买入预留与成本，不允许现金为负，也不会放宽计划止损。
旁路观察账户没有可信的完整现金/NAV时不会套用 PRIMARY 现金比例，也不会把其数据
写入 PRIMARY 实验。

历史成本仅用于展示用户历史浮盈亏，不作为实验收益起点。两账户以冻结日可靠收盘
市值建立相同初始净值；正式 NAV 统一折算为人民币，其中港股在整个 20→60 日实验
永久使用 2026-08-07 基准汇率 `1 HKD = 0.8865 CNY`，不混入后续汇率波动。原始
CNY/HKD 分项和旧 1:1 口径只保留审计。策略账户采用统一人民币模拟购买力，A 股与
港股通卖出所得可跨市场再配置，不模拟真实港股通清算时差，也不允许现金为负。
比较中扣除显式标记为 simulation assumption 的手续费、税费和滑点。A 股新买入与
加仓按 100 股整数手；卖出和清仓可处理已有尾数。没有可靠港股每手 metadata 时不
猜测，并明确标记 `HK board lot not modeled`。信号账本只追加不回改；模拟成交使用
信号以后第一笔满足新鲜度要求的
可验证行情。`scheduler_missed`、`data_unavailable`、`stale_quote` 或
`execution_missed` 均不补单，也不记为策略主动不操作，避免前视偏差。

每个交易日收盘生成“死拿 vs 策略”私密成绩单，以扣费后的策略超额收益为第一
指标、两账户最大回撤差为第二指标，并分别展示现有持仓管理与新候选选股贡献。
第 20 个交易日只做第一阶段检查，不停止也不重置；同一状态自动继续至第 60 个
交易日完成首轮正式评估。整个实验只做模拟和人工复核，不连接券商、不读取券商
账户、不生成委托，也不使用真实资金。

由于仓库公开，14 只持仓的真实数量、历史成本和绝对净值不会写入源码、公开报告或
明文 artifact。初始化完成后 PRIMARY 只允许从 AES 加密状态恢复；cache 与 artifact
都无法恢复时直接失败，初始持仓 Secret 也不得用来重建。私密每日成绩单只通过
PushPlus 发送。公开 artifact 仅允许保存不含绝对资产信息的运行诊断。

## 分层账户观察与每日优先级

正式 A/B 只包含 `PRIMARY_PORTFOLIO` 的 14 只核心持仓。父亲账户、本人第二账户和
妹妹账户分别作为 `FAMILY_WATCHLIST`、`SECONDARY_ACCOUNT_WATCH` 与
`SISTER_MANAGED_WATCH` 旁路观察层：它们可复用同一 symbol 的行情、收盘研究和参考
价位，但不会写入 PRIMARY 信号、成交、现金、NAV、回撤、覆盖率或实验天数。
收盘分析按 P0 PRIMARY、P1 父亲、P2 其他已持有账户、P3 候选顺序执行；可选层失败
不会拖累 PRIMARY，重复 symbol 不重复分析。`002759` 在明确成交前始终只是 candidate。

公开代码只保存 symbol、账户层和 held/candidate 状态。第二账户及妹妹账户的真实
数量与成本如需用于人工风险复核，只能放入 GitHub Repository Secret
`WATCH_ACCOUNTS_PRIVATE_JSON`；运行时只在生成 Push 文案时读取建议股数，不输出日志、
不写入会话状态、报告或明文 artifact。PRIMARY 数量同样只从已加密的 A/B 状态在内存
中读取。没有私密数量时，卖出建议只写“全部持仓”或持仓比例，不猜测股数。
普通涨跌、cooldown 到期和结论未变化继续不 Push；旁路提醒只有在关键位、风险等级
或人工复核动作发生实质变化时才发送，并带清晰账户前缀。

旧 `paper_trade_tracker.py` 的 100 万元标准化实验及历史状态继续保留，但正常每日
full 运行不再推进或 Push。只有手动选择 `simulation-summary` 才会查看
“旧版标准化20日模拟实验（非真实持仓A/B）”。

## Level-2 和数据降级

Level-2 只允许使用交易所授权、持牌数据商或用户本人账户已获行情权限的来源。
系统不会绕过付费墙、账户权限、地区限制或服务条款，也不会把普通五档、成交量
或推算结果冒充 Level-2。

无论用户是否上传分时图、K 线、盘口或成交量，系统都应主动查询可获得且带来源、
授权状态和时间戳的数据。数据源按以下顺序增强：合法授权的 Level-2 深度盘口；
新鲜 L1、逐笔、分时与成交量；OHLCV 和技术指标；公告、财务、估值、资金、行业
与政策。Level-2 接入失败不能被静默忽略，报告必须记录失败原因、实际使用的数据
层级和相应置信度折扣。

分钟监控的 Level-2 适配层默认关闭，不会因为检测到 API Key 就自行宣称已获
权限。只有用户已经合法开通数据商权限、对应 provider 明确返回授权结果后，
适配层才逐标的检查：

- provider 身份与授权记录中的 provider 是否一致；
- 授权范围是否覆盖该标的所属 A 股或港股市场，授权检查是否仍新鲜且未过期；
- 数据载荷是否明确声明为 `level2`，普通 L1 即使字段很多也不能通过；
- provider 时间戳是否在允许的新鲜度内，未来时间戳和缺失时间戳都拒绝；
- 买卖两侧是否达到配置的最低深度，价格排序、数量、订单数和买一卖一关系是否
  合理。

任一检查失败都会逐标的记录为 `unauthorized`、`stale`、`incomplete`、
`invalid`、`provider_error` 或 `unavailable`，同时切换到声明式 fallback。
系统继续使用新鲜 L1、合法可得的逐笔、OHLCV/技术指标、公告/基本面、资金、
政策与行业研究，但按失败类别降低候选置信度。Level-2 缺失本身不会阻断已有的
止损风险监控，不过不能成为“主力抢筹、洗盘、诱多”等盘口结论的证据。

接入新的持牌 provider 时，应实现 `scripts.level2_adapter.AuthorizedLevel2Provider`
协议并由可信运行环境注入；provider 自己负责按官方流程鉴权和订阅。不要把账户
凭据写入代码、artifact、报告或日志，也不要通过公共 GitHub runner 连接用户
电脑上的本地行情网关。

没有授权 Level-2 时，系统仍主动使用可获得的新鲜基础行情、OHLCV、成交量、
波动率、支撑压力和估值字段交叉验证，但会明确标记以下限制：

- 无可靠盘口时，不把“抢筹、洗盘、诱多”写成事实；
- 无完整基本面、公告或资金数据时，不输出伪精确上涨概率；
- 数据过期、覆盖不足或来源失败时，停止交易类提醒并保留上一份有效状态；
- PushPlus 失败时保存待发事件，下次运行重试，扫描和模拟记录不会丢失。

## GitHub Actions 配置

敏感值继续放在 Repository secrets；非敏感阈值放在 Repository variables。
主要变量如下：

| 变量 | 默认值 | 用途 |
| --- | ---: | --- |
| `MINUTE_INTRADAY_MONITOR_ENABLED` | `true` | 由分钟工作流接管旧盘中检查 |
| `INTRADAY_NORMAL_INTERVAL_SECONDS` | `60` | 正常监控间隔 |
| `INTRADAY_FAST_INTERVAL_SECONDS` | `30` | 临近或触发风险条件时的间隔 |
| `INTRADAY_DEGRADED_INTERVAL_SECONDS` | `120` | 数据源降级时的间隔 |
| `INTRADAY_QUOTE_FRESHNESS_SECONDS` | `90` | 可触发交易建议的最新成交价最大年龄 |
| `INTRADAY_MIN_QUOTE_COVERAGE` | `0.8` | 最低基础行情覆盖率 |
| `LONGBRIDGE_OAUTH_CLIENT_ID` | - | 港股实时 L1 的 OAuth client_id（Variable 或 Secret） |
| `LONGBRIDGE_OAUTH_TOKEN_CACHE_B64` | - | 港股实时 L1 的 OAuth token cache（Secret） |
| `MARKET_SCAN_TOP_A_HISTORY` | `40` | 进入历史计算的 A 股数量 |
| `MARKET_SCAN_TOP_HK_HISTORY` | `20` | 进入历史计算的港股通数量 |
| `MARKET_SCAN_FINAL_TOP_N` | `12` | 进入双模型复核的最多数量 |
| `MARKET_SCAN_MIN_NET_RR` | `1.8` | 扣费后最低风险收益比 |
| `MARKET_SCAN_HK_MEMBERSHIP_CACHE_MAX_AGE_HOURS` | `840` | 港股通成员集合最多复用35天；刷新报价不延长成员有效期 |
| `MARKET_SCAN_SNAPSHOT_RETRY_BACKOFF_SECONDS` | `1.0` | A/H 主备快照重试的指数退避基数（秒） |
| `MARKET_SCAN_MIN_ACTIONABLE_DATA_QUALITY` | `0.70` | 可进入盘中买入区复核的最低数据质量 |

港股实时监控也兼容 Legacy `LONGBRIDGE_APP_KEY`、`LONGBRIDGE_APP_SECRET`、
`LONGBRIDGE_ACCESS_TOKEN` 三件套；OAuth 与 Legacy 均未配置或认证失败时，港股行情
会严格降级并停止交易类建议，不会把腾讯约 15 分钟延迟行情伪装成实时数据。
同一轮遇到 Longbridge `SessionError` 或连接失效时会丢弃旧 QuoteContext、重建会话
并做一次有界重试；诊断只保留异常类型、代码和脱敏消息。PRIMARY 腾讯批量行情会对
未覆盖或陈旧标的改走备用入口，主备都无新鲜价格时仍保持 fail-closed，不生成买卖
建议。故障和恢复各只推送一次，详细主备覆盖与会话恢复计数保存在验收报告中。

### OAuth 显示 `HK_Basic` / 行情恰好延迟 15 分钟

Longbridge 在 2026-07 修复过一次 OAuth 行情权限绑定问题。服务端修复后，旧 OAuth
缓存不能只等待 refresh token 自动刷新，必须在可交互电脑上重新授权一次。仓库的
辅助脚本会先把旧缓存移动到带时间戳的备份，再打开官方授权流程：

```bash
python -m pip install 'longbridge>=4.0.5,<5'
python scripts/generate_longbridge_oauth_token.py \
  --client-id "$LONGBRIDGE_OAUTH_CLIENT_ID" \
  --force-reauthorize \
  --require-hk-realtime \
  --verify-symbol 700.HK
```

成功输出必须包含港股个股实时包（通常为 `HK_L1_OpenAPI`）；只有 `HK_Basic` 或只有
恒生指数实时包均不通过。随后把新生成的
`~/.longbridge/openapi/tokens/<client_id>` 重新 base64 编码，覆盖 GitHub
Environment `STOCK_LIST` 中原有的 `LONGBRIDGE_OAUTH_TOKEN_CACHE_B64` Secret。
Linux 可执行：

```bash
LB_TOKEN_CACHE="$HOME/.longbridge/openapi/tokens/$LONGBRIDGE_OAUTH_CLIENT_ID"
base64 -w 0 "$LB_TOKEN_CACHE"
```

macOS 可执行 `base64 < "$LB_TOKEN_CACHE" | tr -d '\n'`。不要把输出写入仓库、
日志或 artifact。更新 Secret 后，先手动运行
`Longbridge 港股实时行情只读预检`：`permission` 模式可在午休/休市时验证权限包；
`live` 模式须在港股连续交易时段运行，并要求每只测试标的的提供方时间戳不超过
配置的新鲜度阈值。两个模式都不会发送 PushPlus。

如果强制重新授权后仍只有 `HK_Basic`，再到 Longbridge Developer Center 检查
OpenAPI（不是 App/PC/Web）的港股行情权限；账户确有实时包却未下发时，应把
`quote_package_details()` 的非敏感包名和 member ID 私下提供给 Longbridge 支持，
而不是通过调大 `INTRADAY_QUOTE_FRESHNESS_SECONDS` 绕过。

模型密钥和 PushPlus Token 使用既有 `LLM_DASHSCOPE_API_KEY`、
`LLM_DEEPSEEK_API_KEY` 与 `PUSHPLUS_TOKEN` secrets。停用新增能力时，在
GitHub Actions 页面分别 Disable `01-intraday-session.yml` 和
`02-market-scan.yml`；原有 20 日模拟仍可独立继续。
