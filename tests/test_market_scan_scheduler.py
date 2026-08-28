"""Offline contracts for redundant, idempotent market-scan scheduling."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml

from scripts.market_scan_slot_guard import (
    _write_outputs,
    evaluate_slot,
    mark_completed,
    resolve_slot,
)
from scripts.market_scan_watchdog import (
    existing_run_covers_slot,
    run_watchdog,
)


TZ = ZoneInfo("Asia/Shanghai")
ROOT = Path(__file__).resolve().parents[1]


def test_schedule_slots_map_to_stable_names() -> None:
    assert resolve_slot(
        event_name="schedule",
        event_schedule="30 2 * * 1-5",
        requested_slot="auto",
    ) == "morning"
    assert resolve_slot(
        event_name="schedule",
        event_schedule="30 6 * * 1-5",
        requested_slot="auto",
    ) == "afternoon"
    assert resolve_slot(
        event_name="schedule",
        event_schedule="15 11 * * 1-5",
        requested_slot="auto",
    ) == "close"


def test_slot_guard_rejects_late_and_completed_slot(tmp_path: Path) -> None:
    ledger_path = tmp_path / "slot_ledger.json"
    on_time = datetime(2026, 8, 28, 10, 42, tzinfo=TZ)
    result = evaluate_slot(
        event_name="workflow_dispatch",
        event_schedule="",
        requested_slot="morning",
        trigger_source="watchdog",
        now=on_time,
        ledger={"schema_version": 1, "slots": {}},
    )
    assert result["should_run"] is True
    assert result["slot_key"] == "2026-08-28:morning"

    mark_completed(
        ledger_path=ledger_path,
        slot_key=result["slot_key"],
        now=on_time,
        run_id="123",
    )
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    duplicate = evaluate_slot(
        event_name="workflow_dispatch",
        event_schedule="",
        requested_slot="morning",
        trigger_source="watchdog",
        now=on_time,
        ledger=ledger,
    )
    assert duplicate["should_run"] is False
    assert duplicate["skip_reason"] == "slot_already_completed"

    late = evaluate_slot(
        event_name="workflow_dispatch",
        event_schedule="",
        requested_slot="morning",
        trigger_source="watchdog",
        now=datetime(2026, 8, 28, 11, 16, tzinfo=TZ),
        ledger={"schema_version": 1, "slots": {}},
    )
    assert late["should_run"] is False
    assert late["skip_reason"] == "slot_too_late"


def test_slot_guard_outputs_cannot_inject_additional_github_outputs(
    tmp_path: Path,
) -> None:
    output = tmp_path / "github-output.txt"

    _write_outputs(
        str(output),
        {
            "should_run": True,
            "slot": "morning",
            "slot_key": "2026-08-28:morning",
            "scheduled_for": "2026-08-28:10:20:00",
            "trigger_source": "watchdog\nshould_run=false",
            "skip_reason": "",
        },
    )

    lines = output.read_text(encoding="utf-8").splitlines()
    assert lines.count("should_run=true") == 1
    assert "trigger_source=watchdog should_run=false" in lines


def test_watchdog_dispatches_only_when_slot_is_not_covered() -> None:
    now = datetime(2026, 8, 28, 10, 42, tzinfo=TZ)

    class FakeClient:
        def __init__(self, runs):
            self.runs = runs
            self.dispatches = []

        def recent_runs(self, workflow, ref):
            assert workflow == "02-market-scan.yml"
            assert ref == "main"
            return self.runs

        def dispatch(self, workflow, ref, slot):
            self.dispatches.append((workflow, ref, slot))

    covered_client = FakeClient(
        [
            {
                "created_at": "2026-08-28T02:31:00Z",
                "status": "completed",
                "conclusion": "success",
            }
        ]
    )
    covered = run_watchdog(
        slot="morning",
        now_fn=lambda: now,
        sleep_fn=lambda _seconds: None,
        client=covered_client,
        workflow="02-market-scan.yml",
        ref="main",
    )
    assert covered["status"] == "already_covered"
    assert covered_client.dispatches == []

    failed_client = FakeClient(
        [
            {
                "created_at": "2026-08-28T02:31:00Z",
                "status": "completed",
                "conclusion": "failure",
            }
        ]
    )
    dispatched = run_watchdog(
        slot="morning",
        now_fn=lambda: now,
        sleep_fn=lambda _seconds: None,
        client=failed_client,
        workflow="02-market-scan.yml",
        ref="main",
    )
    assert dispatched["status"] == "dispatched"
    assert failed_client.dispatches == [
        ("02-market-scan.yml", "main", "morning")
    ]


def test_existing_run_filter_is_market_slot_specific() -> None:
    runs = [
        {
            "created_at": "2026-08-28T06:31:00Z",
            "status": "in_progress",
            "conclusion": None,
        }
    ]
    assert existing_run_covers_slot(
        runs, slot="afternoon", session_date=datetime(2026, 8, 28).date()
    )
    assert not existing_run_covers_slot(
        runs, slot="morning", session_date=datetime(2026, 8, 28).date()
    )


def test_workflows_wire_active_watchdogs_and_slot_guard() -> None:
    scan_text = (ROOT / ".github/workflows/02-market-scan.yml").read_text(
        encoding="utf-8"
    )
    scan = yaml.load(scan_text, Loader=yaml.BaseLoader)
    assert scan["on"]["workflow_dispatch"]["inputs"]["slot"]
    assert "market_scan_slot_guard.py" in scan_text
    assert "snapshot-retry-backoff-seconds" in scan_text

    intraday_text = (ROOT / ".github/workflows/01-intraday-session.yml").read_text(
        encoding="utf-8"
    )
    intraday = yaml.load(intraday_text, Loader=yaml.BaseLoader)
    assert intraday["permissions"]["actions"] == "write"
    assert "market_scan_watchdog.py" in intraday_text
    assert 'exit "$WATCHDOG_STATUS"' in intraday_text

    daily_text = (ROOT / ".github/workflows/00-daily-analysis.yml").read_text(
        encoding="utf-8"
    )
    daily = yaml.load(daily_text, Loader=yaml.BaseLoader)
    assert daily["jobs"]["close-scan-watchdog"]["permissions"]["actions"] == "write"
