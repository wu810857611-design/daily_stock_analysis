#!/usr/bin/env python3
"""Fail closed unless intraday market data and PushPlus satisfy the live contract."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence


PASS_PUSHPLUS_STATUSES = {"actionable_sent", "no_action_no_send"}


def verify_state(state: Mapping[str, Any]) -> dict[str, Any]:
    provider = state.get("provider") or {}
    quote_fetcher = provider.get("quote_fetcher") or {}
    integration = provider.get("integration_verification") or {}
    pushplus = provider.get("pushplus_session") or {}
    issues: list[str] = []

    hk_requested = int(quote_fetcher.get("hk_requested") or 0)
    hk_live = int(quote_fetcher.get("hk_live_snapshot_covered") or 0)
    hk_fresh = int(quote_fetcher.get("hk_fresh_upgraded") or 0)
    hk_timestamped = int(quote_fetcher.get("hk_provider_timestamped") or 0)
    hk_cycles = int(integration.get("hk_cycles_checked") or 0)
    hk_live_cycles = int(integration.get("hk_cycles_fully_live") or 0)
    hk_fresh_cycles = int(integration.get("hk_cycles_fully_fresh") or 0)
    hk_degraded_cycles = int(integration.get("hk_degraded_cycles") or 0)
    hk_reasons = dict(integration.get("hk_degradation_reasons") or {})

    if not quote_fetcher.get("longbridge_configured"):
        issues.append("longbridge_not_configured")
    if not quote_fetcher.get("hk_realtime_entitled"):
        issues.append("longbridge_realtime_permission_unverified")
    if quote_fetcher.get("longbridge_auth_blocked"):
        issues.append("longbridge_oauth_auth_blocked")
    if hk_requested <= 0 or hk_cycles <= 0:
        issues.append("no_hk_quote_cycle_checked")
    if hk_requested > 0 and hk_live != hk_requested:
        issues.append("hk_live_snapshot_coverage_incomplete")
    if hk_requested > 0 and hk_timestamped != hk_requested:
        issues.append("hk_provider_timestamp_incomplete")
    if hk_cycles > 0 and hk_live_cycles != hk_cycles:
        issues.append("hk_session_not_fully_live")
    if hk_degraded_cycles > 0 or hk_reasons:
        issues.append("hk_degradation_observed")
    if int(integration.get("primary_degraded_cycles") or 0) > 0:
        issues.append("primary_quote_degradation_observed")
    if int(integration.get("calendar_degraded_cycles") or 0) > 0:
        issues.append("market_calendar_degradation_observed")
    if int(integration.get("signal_persist_failures") or 0) > 0:
        issues.append("signal_persist_failure_observed")
    if provider.get("degraded"):
        issues.append("primary_quote_currently_degraded")
    if provider.get("calendar_degraded"):
        issues.append("market_calendar_degraded")

    push_status = str(pushplus.get("status") or "missing")
    pending_actionable = int(pushplus.get("pending_actionable") or 0)
    pending_system = int(pushplus.get("pending_system") or 0)
    if not pushplus.get("configured"):
        issues.append("pushplus_not_configured")
    if push_status not in PASS_PUSHPLUS_STATUSES:
        issues.append(f"pushplus_{push_status}")
    if pending_actionable or pending_system:
        issues.append("pushplus_pending_retry")

    return {
        "passed": not issues,
        "market_data": {
            "longbridge_configured": bool(
                quote_fetcher.get("longbridge_configured")
            ),
            "hk_realtime_entitled": bool(
                quote_fetcher.get("hk_realtime_entitled")
            ),
            "latest_live_snapshot": hk_live,
            "latest_trade_price_fresh": hk_fresh,
            "latest_timestamped": hk_timestamped,
            "latest_requested": hk_requested,
            "fully_live_cycles": hk_live_cycles,
            "fully_trade_price_fresh_cycles": hk_fresh_cycles,
            "checked_cycles": hk_cycles,
            "price_timestamp_stale_observations": int(
                integration.get("hk_price_timestamp_stale_observations") or 0
            ),
            "longbridge_recovery_attempts": int(
                integration.get("longbridge_recovery_attempts") or 0
            ),
            "longbridge_recovery_successes": int(
                integration.get("longbridge_recovery_successes") or 0
            ),
            "longbridge_recovery_failures": int(
                integration.get("longbridge_recovery_failures") or 0
            ),
            "longbridge_last_error": dict(
                quote_fetcher.get("longbridge_last_error") or {}
            ),
            "longbridge_auth_mode": str(
                quote_fetcher.get("longbridge_auth_mode") or "unknown"
            ),
            "longbridge_oauth_cache_source": str(
                quote_fetcher.get("longbridge_oauth_cache_source") or "unknown"
            ),
            "longbridge_auth_blocked": bool(
                quote_fetcher.get("longbridge_auth_blocked")
            ),
            "longbridge_auth_blocks": int(
                integration.get("longbridge_auth_blocks") or 0
            ),
            "primary_fallback_requests": int(
                integration.get("primary_fallback_requests") or 0
            ),
            "primary_fallback_covered": int(
                integration.get("primary_fallback_covered") or 0
            ),
            "tencent_alternate_route_requests": int(
                integration.get("tencent_alternate_route_requests")
                or integration.get("primary_fallback_requests")
                or 0
            ),
            "tencent_alternate_route_fresh_covered": int(
                integration.get("tencent_alternate_route_fresh_covered")
                or integration.get("primary_fallback_covered")
                or 0
            ),
            "tencent_route_price_returned": int(
                integration.get("tencent_route_price_returned") or 0
            ),
            "tencent_route_provider_timestamped": int(
                integration.get("tencent_route_provider_timestamped") or 0
            ),
            "tencent_route_stale_or_untradeable": int(
                integration.get("tencent_route_stale_or_untradeable") or 0
            ),
            "tencent_route_missing": int(
                integration.get("tencent_route_missing") or 0
            ),
            "hk_primary_fallback_fresh_covered": int(
                integration.get("hk_primary_fallback_fresh_covered") or 0
            ),
        },
        "degradation": {
            "hk_degraded_cycles": hk_degraded_cycles,
            "hk_reasons": hk_reasons,
            "primary_degraded_cycles": int(
                integration.get("primary_degraded_cycles") or 0
            ),
            "calendar_degraded_cycles": int(
                integration.get("calendar_degraded_cycles") or 0
            ),
            "signal_persist_failures": int(
                integration.get("signal_persist_failures") or 0
            ),
            "signal_persist_failure_reasons": dict(
                integration.get("signal_persist_failure_reasons") or {}
            ),
        },
        "pushplus": {
            "status": push_status,
            "actionable_sent": int(pushplus.get("actionable_sent") or 0),
            "suppressed_no_action": int(
                pushplus.get("suppressed_no_action") or 0
            ),
            "pending_actionable": pending_actionable,
            "pending_system": pending_system,
        },
        "issues": list(dict.fromkeys(issues)),
    }


def render_markdown(result: Mapping[str, Any]) -> str:
    market = result["market_data"]
    degradation = result["degradation"]
    pushplus = result["pushplus"]
    verdict = "✅ 完全通过" if result["passed"] else "❌ 未完全通过"
    reasons = degradation.get("hk_reasons") or {}
    reason_text = (
        "、".join(f"{reason}={count}" for reason, count in sorted(reasons.items()))
        if reasons
        else "无"
    )
    issue_text = "、".join(result.get("issues") or []) or "无"
    return "\n".join(
        [
            "## 盘中行情与推送严格验证",
            "",
            f"- 结论：{verdict}",
            (
                "- Longbridge：实时权限 "
                f"{'已验证' if market['hk_realtime_entitled'] else '未验证'}；"
                "最新轮实时快照 "
                f"{market['latest_live_snapshot']}/{market['latest_requested']}；"
                f"提供方时间戳 {market['latest_timestamped']}/"
                f"{market['latest_requested']}；完整实时快照轮次 "
                f"{market['fully_live_cycles']}/{market['checked_cycles']}"
            ),
            (
                "- 可用于建议的新鲜成交价：最新轮 "
                f"{market['latest_trade_price_fresh']}/"
                f"{market['latest_requested']}；全数新鲜轮次 "
                f"{market['fully_trade_price_fresh_cycles']}/"
                f"{market['checked_cycles']}"
            ),
            (
                "- 无近期成交快照："
                f"{market['price_timestamp_stale_observations']} 次；"
                "这些快照不触发买卖建议，也不计作实时行情源降级"
            ),
            (
                "- 自动恢复：Longbridge 会话重建 "
                f"{market['longbridge_recovery_attempts']} 次（成功 "
                f"{market['longbridge_recovery_successes']}、失败 "
                f"{market['longbridge_recovery_failures']}）；认证熔断 "
                f"{market['longbridge_auth_blocks']} 次；模式 "
                f"{market['longbridge_auth_mode']}；缓存来源 "
                f"{market['longbridge_oauth_cache_source']}"
            ),
            (
                "- 腾讯备用路由：新鲜恢复 "
                f"{market['tencent_alternate_route_fresh_covered']}/"
                f"{market['tencent_alternate_route_requests']} 个标的路由次；"
                f"返回价格 {market['tencent_route_price_returned']}；"
                f"带时间戳 {market['tencent_route_provider_timestamped']}；"
                f"陈旧/不可交易 {market['tencent_route_stale_or_untradeable']}；"
                f"缺失 {market['tencent_route_missing']}；"
                "港股通过90秒门槛恢复 "
                f"{market['hk_primary_fallback_fresh_covered']} 个标的次"
            ),
            (
                "- 行情降级：港股降级轮次 "
                f"{degradation['hk_degraded_cycles']}；"
                f"PRIMARY 降级轮次 {degradation['primary_degraded_cycles']}；"
                f"交易日历降级轮次 {degradation['calendar_degraded_cycles']}；"
                f"原因 {reason_text}"
            ),
            (
                "- 信号落盘：失败 "
                f"{degradation['signal_persist_failures']} 次；原因 "
                + (
                    "、".join(
                        f"{reason}={count}"
                        for reason, count in sorted(
                            degradation[
                                "signal_persist_failure_reasons"
                            ].items()
                        )
                    )
                    or "无"
                )
            ),
            (
                "- PushPlus："
                f"{pushplus['status']}；明确买卖建议已发送 "
                f"{pushplus['actionable_sent']}；无动作拦截 "
                f"{pushplus['suppressed_no_action']}；待重试 "
                f"{pushplus['pending_actionable'] + pushplus['pending_system']}"
            ),
            f"- 未通过项：{issue_text}",
            "",
        ]
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--state",
        type=Path,
        default=Path("data/intraday/session_state.json"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        state = json.loads(args.state.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"盘中严格验证失败：无法读取状态文件（{type(exc).__name__}）")
        return 2
    result = verify_state(state)
    markdown = render_markdown(result)
    print(markdown)
    summary_path = os.getenv("GITHUB_STEP_SUMMARY", "").strip()
    if summary_path:
        try:
            with Path(summary_path).open("a", encoding="utf-8") as handle:
                handle.write(markdown)
        except OSError as exc:
            print(f"写入 GitHub 摘要失败：{type(exc).__name__}")
    if not result["passed"] and os.getenv("GITHUB_ACTIONS") == "true":
        print(
            "::error title=行情与推送未完全通过::"
            + ", ".join(result["issues"])
        )
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
