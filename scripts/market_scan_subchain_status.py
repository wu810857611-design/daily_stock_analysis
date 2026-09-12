#!/usr/bin/env python3
"""Persist and notify market-scan watchdog health without failing intraday."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, MutableMapping, Sequence
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pushplus_notify import send_markdown  # noqa: E402


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
_BEARER = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+\-/=]+")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    finally:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass


def _safe_error(value: Any) -> str:
    return _BEARER.sub("Bearer <redacted>", str(value or "").strip())[:500]


def classify_watchdog(
    payload: Mapping[str, Any], *, exit_code: int
) -> tuple[str, str, bool]:
    sync = payload.get("sync")
    sync_status = str(sync.get("status") or "") if isinstance(sync, Mapping) else ""
    error = _safe_error(sync.get("error")) if isinstance(sync, Mapping) else ""
    if exit_code == 0 and (not sync_status or sync_status == "synced"):
        return "healthy", "", False
    reason = error or (
        "watchdog_report_missing"
        if not payload
        else f"watchdog_exit_{int(exit_code)}"
    )
    delegated = reason.startswith("market_scan_run_failed:")
    return "degraded", reason, delegated


def _render_alert(
    *, trade_date: str, session: str, reason: str, recovery: bool
) -> tuple[str, str]:
    if recovery:
        title = f"SYSTEM：全市场买入子链恢复 - {trade_date} {session}"
        heading = "全市场买入子链恢复"
        detail = "watchdog 已重新同步当前时段的可信候选产物。"
    else:
        title = f"SYSTEM：全市场买入子链故障 - {trade_date} {session}"
        heading = "全市场买入子链故障"
        detail = f"故障：`{reason}`"
    content = "\n".join(
        [
            f"# {heading}",
            "",
            f"- 交易日：{trade_date}",
            f"- 时段：{session}",
            f"- {detail}",
            "",
            "影响：本时段不会热加载新的建仓候选；已有持仓和主盘中行情监控继续运行。",
            "行情新鲜度、买入区、止损、风险收益、仓位现金、人工确认和双模型门禁均保持有效。",
        ]
    )
    return title, content + "\n"


def _default_sender(*, title: str, content: str) -> bool:
    token = str(os.getenv("PUSHPLUS_TOKEN") or "").strip()
    if not token:
        return False
    send_markdown(
        token=token,
        topic=str(os.getenv("PUSHPLUS_TOPIC") or "").strip(),
        title=title,
        content=content,
    )
    return True


def reconcile_subchain_status(
    *,
    watchdog_report: Path,
    state_path: Path,
    output_path: Path,
    exit_code: int,
    session: str,
    now: datetime,
    sender: Callable[..., bool] = _default_sender,
) -> dict[str, Any]:
    payload = _read_json(watchdog_report)
    status, reason, delegated = classify_watchdog(payload, exit_code=exit_code)
    previous_state = _read_json(state_path)
    slots = previous_state.get("slots")
    if not isinstance(slots, MutableMapping):
        slots = {}
    previous = slots.get(session)
    if not isinstance(previous, Mapping):
        previous = {}
    trade_date = now.astimezone(SHANGHAI_TZ).date().isoformat()
    fingerprint = hashlib.sha256(
        f"{trade_date}:{session}:{status}:{reason}".encode("utf-8")
    ).hexdigest()
    notification: dict[str, Any]
    previous_fingerprint = str(previous.get("fingerprint") or "")
    previous_notification = previous.get("notification")
    previous_notification_status = (
        str(previous_notification.get("status") or "")
        if isinstance(previous_notification, Mapping)
        else ""
    )
    previous_owned = bool(previous.get("notification_owned_by_watchdog"))
    recovery = (
        status == "healthy"
        and previous_owned
        and previous_notification_status in {"sent", "suppressed_duplicate"}
    )
    should_send = status == "degraded" and not delegated
    if recovery:
        should_send = True
    if status == "degraded" and delegated:
        notification = {
            "status": "delegated_to_market_scan",
            "reason": "02-market-scan owns its failure and recovery notification",
        }
    elif (
        should_send
        and fingerprint == previous_fingerprint
        and previous_notification_status in {"sent", "suppressed_duplicate"}
    ):
        notification = {"status": "suppressed_duplicate"}
    elif should_send:
        title, content = _render_alert(
            trade_date=trade_date,
            session=session,
            reason=reason,
            recovery=recovery,
        )
        try:
            sent = bool(sender(title=title, content=content))
            error = "" if sent else "sender_returned_false"
        except Exception as exc:  # noqa: BLE001 - persisted for later retry.
            sent = False
            error = f"{type(exc).__name__}: {_safe_error(exc)}"
        notification = {
            "status": "sent" if sent else "pending",
            "error": error,
        }
    else:
        notification = {"status": "not_required"}
    result = {
        "schema_version": 1,
        "trade_date": trade_date,
        "session": session,
        "observed_at": now.astimezone(SHANGHAI_TZ).isoformat(timespec="seconds"),
        "operational_status": status,
        "reason": reason,
        "impact": (
            "none"
            if status == "healthy"
            else "new_buy_candidate_hot_reload_unavailable"
        ),
        "main_intraday_health_affected": False,
        "watchdog_exit_code": int(exit_code),
        "watchdog": payload,
        "notification": notification,
    }
    slots[session] = {
        "status": status,
        "fingerprint": fingerprint,
        "updated_at": result["observed_at"],
        "notification_owned_by_watchdog": bool(
            (status == "degraded" and not delegated)
            or (recovery and notification.get("status") != "sent")
        ),
        "notification": notification,
    }
    state = {"schema_version": 1, "slots": dict(slots)}
    _write_json(state_path, state)
    _write_json(output_path, result)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--watchdog-report", required=True, type=Path)
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--watchdog-exit-code", required=True, type=int)
    parser.add_argument("--session", required=True, choices=("morning", "afternoon"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = reconcile_subchain_status(
        watchdog_report=args.watchdog_report,
        state_path=args.state,
        output_path=args.output,
        exit_code=args.watchdog_exit_code,
        session=args.session,
        now=datetime.now(SHANGHAI_TZ),
    )
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
