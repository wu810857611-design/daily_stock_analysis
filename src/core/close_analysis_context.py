"""Bound delayed scheduled closes to one completed A/H session, before next open."""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

SHANGHAI = ZoneInfo("Asia/Shanghai")
CLOSE_CRON = "0 10 * * 1-5"


def close_reference_time(trade_date: str, now: datetime) -> datetime:
    """Freeze the completed session at 18:00; never carry it into next trading morning."""
    if now.tzinfo is None:
        raise ValueError("close analysis requires an aware observed time")
    day = date.fromisoformat(trade_date)
    reference = datetime.combine(day, time(18), tzinfo=SHANGHAI)
    deadline = datetime.combine(day + timedelta(days=1), time(9), tzinfo=SHANGHAI)
    if day.weekday() >= 5 or not reference <= now < deadline:
        raise ValueError("close analysis outside 18:00 to next-day 09:00 window")
    return reference


def scheduled_close_context(run: Mapping[str, Any], *, schedule: str,
                            now: datetime) -> dict[str, Any]:
    """Use immutable run creation time, including reruns, rather than a new wall-clock date.

    GitHub exposes the cron expression, not its intended timestamp. Only the
    latest weekday 18:00 occurrence in a bounded overnight window is admitted;
    missing metadata, future creation and old reruns remain fail-closed.
    Exchange holidays are handled by the per-market analysis filter, not guessed here.
    """
    if schedule != CLOSE_CRON or run.get("event") != "schedule":
        raise ValueError("unrecognised scheduled close event")
    created = datetime.fromisoformat(str(run.get("created_at") or "").replace("Z", "+00:00"))
    if created.tzinfo is None or now.tzinfo is None or created > now:
        raise ValueError("invalid scheduled run creation time")
    local_created = created.astimezone(SHANGHAI)
    day = local_created.date()
    if local_created.time() < time(18):
        day -= timedelta(days=1)
    reference = close_reference_time(day.isoformat(), created)
    close_reference_time(day.isoformat(), now)
    return {
        "trade_date": day.isoformat(),
        "analysis_reference_time": reference.isoformat(),
        "run_created_at": created.isoformat(),
        "observed_at": now.astimezone(SHANGHAI).isoformat(),
        "deadline": datetime.combine(day + timedelta(days=1), time(9), tzinfo=SHANGHAI).isoformat(),
        "date_source": "bounded_latest_cron_occurrence_from_run_created_at",
        "close_scan_should_run": now <= datetime.combine(day, time(21), tzinfo=SHANGHAI),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-metadata", type=Path, required=True)
    parser.add_argument("--schedule", required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = scheduled_close_context(
            json.loads(args.run_metadata.read_text()), schedule=args.schedule,
            now=datetime.now(SHANGHAI),
        )
    except (ValueError, TypeError) as exc:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps({"status": "rejected", "reason": str(exc)}) + "\n")
        parser.exit(1, f"scheduled close rejected: {exc}\n")
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps({"status": "accepted", **result}, indent=2) + "\n")
    print(result["trade_date"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
