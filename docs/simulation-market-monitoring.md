# A 股与港股通模拟扫描和盘中监控

这套能力只做研究、模拟记录和人工复核提醒，不连接券商，也不会自动下单。
原有收盘分析与 20 个交易日模拟状态继续由 `00-daily-analysis.yml` 保存；新增
工作流不会重置该状态。

## 云端运行节奏

- `00-daily-analysis.yml`：交易日北京时间 18:00 执行收盘分析和模拟记账。
- `01-intraday-session.yml`：交易日上午、下午各启动一个连续监控时段。正常约
  60 秒取一次基础实时行情；价格临近风险位或条件位时约 30 秒一次；数据源降级
  时放慢到约 120 秒。
- `02-market-scan.yml`：北京时间 10:30、14:30 和 19:15 分层扫描 A 股全市场与
  港股通成分。全市场阶段不调用模型；只对规则短名单加载历史，再让通义千问和
  DeepSeek 独立复核。

GitHub 的定时触发可能排队，因此这些时间不是交易所级低延迟保证。任何超过
90 秒、缺少时间戳或关键字段不完整的行情，都不能触发交易类人工复核提醒。

## 决策规则

监控频率和交易频率彼此独立。系统可以频繁观察，但只有当候选在扣除手续费、
税费和滑点后，预期风险收益比与年化效用明显优于现有方案时，才允许产生人工
复核事件。报告可安全推送不等于候选可交易；全市场候选还必须逐只满足完整深研、
双模型一致、无硬风险、进入新鲜有效价格区间等条件。

目前全市场扫描尚未取得完整基本面、公告、政策、资金和授权 Level-2，因此候选
统一标记为“等待深研”，不会被分钟监控提升为买入提醒。

## Level-2 和数据降级

Level-2 只允许使用交易所授权、持牌数据商或用户本人账户已获行情权限的来源。
系统不会绕过付费墙、账户权限、地区限制或服务条款，也不会把普通五档、成交量
或推算结果冒充 Level-2。

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
| `INTRADAY_QUOTE_FRESHNESS_SECONDS` | `90` | 行情最大允许年龄 |
| `INTRADAY_MIN_QUOTE_COVERAGE` | `0.8` | 最低基础行情覆盖率 |
| `MARKET_SCAN_TOP_A_HISTORY` | `40` | 进入历史计算的 A 股数量 |
| `MARKET_SCAN_TOP_HK_HISTORY` | `20` | 进入历史计算的港股通数量 |
| `MARKET_SCAN_FINAL_TOP_N` | `12` | 进入双模型复核的最多数量 |
| `MARKET_SCAN_MIN_NET_RR` | `1.8` | 扣费后最低风险收益比 |

模型密钥和 PushPlus Token 使用既有 `LLM_DASHSCOPE_API_KEY`、
`LLM_DEEPSEEK_API_KEY` 与 `PUSHPLUS_TOKEN` secrets。停用新增能力时，在
GitHub Actions 页面分别 Disable `01-intraday-session.yml` 和
`02-market-scan.yml`；原有 20 日模拟仍可独立继续。
