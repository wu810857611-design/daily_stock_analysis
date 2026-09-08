#!/usr/bin/env python3
"""Resolve and fail-close one scheduled intraday session invocation."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.market_scan_calendar import evaluate_market_sessions

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
SCHEDULE_TO_SESSION = {
    "20 1 * * 1-5": "morning",
    "50 4 * * 1-5": "afternoon",
}
SESSION_WINDOWS = {
    "morning": {
        "scheduled": time(9, 20),
        "earliest": time(9, 15),
        "supervisor": time(9, 28),
        "latest": time(10, 15),
        "search_start": time(8, 50),
    },
    "afternoon": {
        "scheduled": time(12, 50),
        "earliest": time(12, 45),
        "supervisor": time(12, 58),
        "latest": time(13, 45),
        "search_start": time(12, 20),
    },
}


def parse_now(value: str) -> datetime:
    if not value:
        return datetime.now(SHANGHAI_TZ)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed.astimezone(SHANGHAI_TZ)


def resolve_session(*, event_name: str, event_schedule: str, requested_session: str, now: datetime) -> str:
    if event_name == "schedule":
        try:
            return SCHEDULE_TO_SESSION[event_schedule]
        except KeyError as exc:
            raise ValueError(f"unsupported intraday schedule: {event_schedule}") from exc
    requested = str(requested_session or "auto").strip().lower()
    if requested in SESSION_WINDOWS:
        return requested
    if requested == "auto":
        return "morning" if now.timetz().replace(tzinfo=None) < time(12, 2) else "afternoon"
    raise ValueError(f"unsupported intraday session: {requested}")


def _resolve_trade_date(value: str, now: datetime) -> date:
    requested = str(value or "auto").strip().lower()
    if requested in {"", "auto"}:
        return now.date()
    return date.fromisoformat(requested)


def evaluate_session_start(
    *,
    event_name: str,
    event_schedule: str,
    requested_session: str,
    requested_trade_date: str,
    trigger_source: str,
    now: datetime,
    market_sessions: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    observed = now.astimezone(SHANGHAI_TZ)
    session = resolve_session(
        event_name=event_name,
        event_schedule=event_schedule,
        requested_session=requested_session,
        now=observed,
    )
    trade_date = _resolve_trade_date(requested_trade_date, observed)
    window = SESSION_WINDOWS[session]
    base = {
        "schema_version": 1,
        "observed_at": observed.isoformat(timespec="seconds"),
        "trade_date": trade_date.isoformat(),
        "session": session,
        "session_key": f"{trade_date.isoformat()}:{session}",
        "claim_key": f"{trade_date.isoformat()}-{session}",
        "trigger_source": str(trigger_source or event_name or "unknown"),
        "scheduled_at": datetime.combine(trade_date, window["scheduled"], tzinfo=SHANGHAI_TZ).isoformat(
            timespec="seconds"
        ),
        "safe_window_start": datetime.combine(trade_date, window["earliest"], tzinfo=SHANGHAI_TZ).isoformat(
            timespec="seconds"
        ),
        "safe_window_end": datetime.combine(trade_date, window["latest"], tzinfo=SHANGHAI_TZ).isoformat(
            timespec="seconds"
        ),
        "should_run": True,
        "skip_reason": "",
    }
    if trade_date != observed.date():
        return {**base, "should_run": False, "skip_reason": "stale_trade_date"}

    calendar = dict(market_sessions or evaluate_market_sessions(observed))
    base.update(
        {
            "calendar_status": str(calendar.get("status") or "unknown"),
            "calendar_degraded": bool(calendar.get("calendar_degraded")),
            "active_markets": list(calendar.get("active_markets") or []),
            "market_states": dict(calendar.get("market_states") or {}),
        }
    )
    if not calendar.get("should_run"):
        return {**base, "should_run": False, "skip_reason": "all_markets_closed"}

    local_time = observed.timetz().replace(tzinfo=None)
    if local_time < window["earliest"]:
        return {**base, "should_run": False, "skip_reason": "session_not_open"}
    if local_time > window["latest"]:
        return {**base, "should_run": False, "skip_reason": "session_too_late"}
    return base


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    finally:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass


def write_outputs(path: str, result: Mapping[str, Any]) -> None:
    if not path:
        return
    keys = (
        "should_run",
        "trade_date",
        "session",
        "session_key",
        "claim_key",
        "trigger_source",
        "skip_reason",
        "calendar_status",
        "calendar_degraded",
        "active_markets",
        "safe_window_end",
    )
    with Path(path).open("a", encoding="utf-8") as handle:
        for key in keys:
            value = result.get(key, "")
            if isinstance(value, bool):
                value = str(value).lower()
            elif isinstance(value, Sequence) and not isinstance(value, str):
                value = ",".join(str(item) for item in value)
            scalar = str(value).replace("\r", " ").replace("\n", " ")[:500]
            handle.write(f"{key}={scalar}\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-name", default="workflow_dispatch")
    parser.add_argument("--event-schedule", default="")
    parser.add_argument("--requested-session", default="auto")
    parser.add_argument("--requested-trade-date", default="auto")
    parser.add_argument("--trigger-source", default="external_dispatch")
    parser.add_argument("--now", default="")
    parser.add_argument("--github-output", default="")
    parser.add_argument("--report", type=Path, default=Path("reports/intraday_slot_guard.json"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = evaluate_session_start(
        event_name=args.event_name,
        event_schedule=args.event_schedule,
        requested_session=args.requested_session,
        requested_trade_date=args.requested_trade_date,
        trigger_source=args.trigger_source,
        now=parse_now(args.now),
    )
    atomic_write_json(args.report, result)
    write_outputs(args.github_output, result)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
