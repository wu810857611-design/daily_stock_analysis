#!/usr/bin/env python3
"""Resolve one market-scan slot and reject duplicates or stale starts."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from datetime import datetime, time
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
SCHEDULE_TO_SLOT = {
    "30 2 * * 1-5": "morning",
    "30 6 * * 1-5": "afternoon",
    "15 11 * * 1-5": "close",
}
SLOT_WINDOWS = {
    "morning": (time(10, 20), time(11, 15)),
    "afternoon": (time(14, 20), time(15, 15)),
    "close": (time(19, 5), time(21, 0)),
}


def _parse_now(value: str) -> datetime:
    if not value:
        return datetime.now(SHANGHAI_TZ)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed.astimezone(SHANGHAI_TZ)


def resolve_slot(
    *, event_name: str, event_schedule: str, requested_slot: str
) -> str:
    if event_name == "schedule":
        try:
            return SCHEDULE_TO_SLOT[event_schedule]
        except KeyError as exc:
            raise ValueError(f"unsupported market-scan schedule: {event_schedule}") from exc
    requested = str(requested_slot or "auto").strip().lower()
    if requested in SLOT_WINDOWS:
        return requested
    if requested in {"", "auto", "manual"}:
        return "manual"
    raise ValueError(f"unsupported market-scan slot: {requested}")


def _read_ledger(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {"schema_version": 1, "slots": {}}
    if not isinstance(payload, Mapping) or not isinstance(payload.get("slots"), Mapping):
        return {"schema_version": 1, "slots": {}}
    return {"schema_version": 1, "slots": dict(payload["slots"])}


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
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


def evaluate_slot(
    *,
    event_name: str,
    event_schedule: str,
    requested_slot: str,
    trigger_source: str,
    now: datetime,
    ledger: Mapping[str, Any],
) -> dict[str, Any]:
    slot = resolve_slot(
        event_name=event_name,
        event_schedule=event_schedule,
        requested_slot=requested_slot,
    )
    base = {
        "slot": slot,
        "trigger_source": str(trigger_source or event_name or "unknown"),
        "observed_at": now.isoformat(timespec="seconds"),
        "should_run": True,
        "skip_reason": "",
    }
    if slot == "manual":
        return {**base, "slot_key": "manual", "scheduled_for": "manual"}

    slot_key = f"{now.date().isoformat()}:{slot}"
    scheduled_for = f"{now.date().isoformat()}:{SLOT_WINDOWS[slot][0].isoformat()}"
    local_time = now.timetz().replace(tzinfo=None)
    earliest, latest = SLOT_WINDOWS[slot]
    if local_time < earliest:
        return {
            **base,
            "slot_key": slot_key,
            "scheduled_for": scheduled_for,
            "should_run": False,
            "skip_reason": "slot_not_open",
        }
    if local_time > latest:
        return {
            **base,
            "slot_key": slot_key,
            "scheduled_for": scheduled_for,
            "should_run": False,
            "skip_reason": "slot_too_late",
        }
    previous = (ledger.get("slots") or {}).get(slot_key)
    if isinstance(previous, Mapping) and previous.get("status") == "completed":
        return {
            **base,
            "slot_key": slot_key,
            "scheduled_for": scheduled_for,
            "should_run": False,
            "skip_reason": "slot_already_completed",
        }
    return {**base, "slot_key": slot_key, "scheduled_for": scheduled_for}


def mark_completed(
    *, ledger_path: Path, slot_key: str, now: datetime, run_id: str
) -> None:
    if not slot_key or slot_key == "manual":
        return
    ledger = _read_ledger(ledger_path)
    slots = ledger.setdefault("slots", {})
    slots[slot_key] = {
        "status": "completed",
        "completed_at": now.isoformat(timespec="seconds"),
        "run_id": str(run_id or ""),
    }
    # The ledger is an idempotency aid, not an unbounded audit log.
    for key in sorted(slots)[:-90]:
        slots.pop(key, None)
    _atomic_write_json(ledger_path, ledger)


def _write_outputs(path: str, result: Mapping[str, Any]) -> None:
    if not path:
        return
    with Path(path).open("a", encoding="utf-8") as handle:
        for key in (
            "should_run",
            "slot",
            "slot_key",
            "scheduled_for",
            "trigger_source",
            "skip_reason",
        ):
            value = result.get(key, "")
            if isinstance(value, bool):
                value = str(value).lower()
            scalar = str(value).replace("\r", " ").replace("\n", " ")[:500]
            handle.write(f"{key}={scalar}\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=Path("data/market_scan"))
    parser.add_argument("--event-name", default="workflow_dispatch")
    parser.add_argument("--event-schedule", default="")
    parser.add_argument("--requested-slot", default="auto")
    parser.add_argument("--trigger-source", default="manual")
    parser.add_argument("--now", default="")
    parser.add_argument("--github-output", default="")
    parser.add_argument("--report", type=Path, default=Path("reports/market_scan_slot_guard.json"))
    parser.add_argument("--mark-complete", action="store_true")
    parser.add_argument("--slot-key", default="")
    parser.add_argument("--run-id", default="")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    now = _parse_now(args.now)
    ledger_path = args.state_dir / "slot_ledger.json"
    if args.mark_complete:
        mark_completed(
            ledger_path=ledger_path,
            slot_key=args.slot_key,
            now=now,
            run_id=args.run_id,
        )
        return 0
    result = evaluate_slot(
        event_name=args.event_name,
        event_schedule=args.event_schedule,
        requested_slot=args.requested_slot,
        trigger_source=args.trigger_source,
        now=now,
        ledger=_read_ledger(ledger_path),
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_json(args.report, result)
    _write_outputs(args.github_output, result)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
