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
    hk_fresh = int(quote_fetcher.get("hk_fresh_upgraded") or 0)
    hk_timestamped = int(quote_fetcher.get("hk_provider_timestamped") or 0)
    hk_cycles = int(integration.get("hk_cycles_checked") or 0)
    hk_fresh_cycles = int(integration.get("hk_cycles_fully_fresh") or 0)
    hk_degraded_cycles = int(integration.get("hk_degraded_cycles") or 0)
    hk_reasons = dict(integration.get("hk_degradation_reasons") or {})

    if not quote_fetcher.get("longbridge_configured"):
        issues.append("longbridge_not_configured")
    if hk_requested <= 0 or hk_cycles <= 0:
        issues.append("no_hk_quote_cycle_checked")
    if hk_requested > 0 and hk_fresh != hk_requested:
        issues.append("hk_fresh_coverage_incomplete")
    if hk_requested > 0 and hk_timestamped != hk_requested:
        issues.append("hk_provider_timestamp_incomplete")
    if hk_cycles > 0 and hk_fresh_cycles != hk_cycles:
        issues.append("hk_session_not_fully_fresh")
    if hk_degraded_cycles > 0 or hk_reasons:
        issues.append("hk_degradation_observed")
    if int(integration.get("primary_degraded_cycles") or 0) > 0:
        issues.append("primary_quote_degradation_observed")
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
            "latest_fresh": hk_fresh,
            "latest_timestamped": hk_timestamped,
            "latest_requested": hk_requested,
            "fully_fresh_cycles": hk_fresh_cycles,
            "checked_cycles": hk_cycles,
        },
        "degradation": {
            "hk_degraded_cycles": hk_degraded_cycles,
            "hk_reasons": hk_reasons,
            "primary_degraded_cycles": int(
                integration.get("primary_degraded_cycles") or 0
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
                "- Longbridge：最新轮新鲜行情 "
                f"{market['latest_fresh']}/{market['latest_requested']}；"
                f"提供方时间戳 {market['latest_timestamped']}/"
                f"{market['latest_requested']}；完整新鲜轮次 "
                f"{market['fully_fresh_cycles']}/{market['checked_cycles']}"
            ),
            (
                "- 行情降级：港股降级轮次 "
                f"{degradation['hk_degraded_cycles']}；"
                f"PRIMARY 降级轮次 {degradation['primary_degraded_cycles']}；"
                f"原因 {reason_text}"
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
