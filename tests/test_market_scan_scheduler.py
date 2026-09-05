"""Offline contracts for redundant, idempotent market-scan scheduling."""

from __future__ import annotations

import json
import io
import tarfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import yaml
import pytest

from scripts.market_scan_calendar import evaluate_market_sessions
from scripts.market_scan_slot_guard import (
    _write_outputs,
    evaluate_slot,
    mark_completed,
    resolve_slot,
)
from scripts.market_scan_watchdog import (
    _read_market_scan_latest_from_artifact,
    _validate_synced_market_scan,
    existing_run_covers_slot,
    run_watchdog,
)


TZ = ZoneInfo("Asia/Shanghai")
ROOT = Path(__file__).resolve().parents[1]


def _market_scan_artifact(payload: Mapping[str, Any], *, member_name: str = "data/market_scan/latest.json") -> bytes:
    latest = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    nested_buffer = io.BytesIO()
    with tarfile.open(fileobj=nested_buffer, mode="w:gz") as archive:
        member = tarfile.TarInfo(member_name)
        member.size = len(latest)
        archive.addfile(member, io.BytesIO(latest))
    outer_buffer = io.BytesIO()
    with zipfile.ZipFile(outer_buffer, mode="w") as archive:
        archive.writestr("market-scan-state.tar.gz", nested_buffer.getvalue())
    return outer_buffer.getvalue()


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


def test_calendar_gate_skips_weekend_and_isolates_divergent_market_holiday() -> None:
    saturday = evaluate_market_sessions(
        datetime(2026, 8, 29, 10, 42, tzinfo=TZ),
        phase_resolver=lambda _market, **_kwargs: "non_trading",
    )
    assert saturday["should_run"] is False
    assert saturday["status"] == "market_closed"
    assert saturday["active_markets"] == []

    split = evaluate_market_sessions(
        datetime(2026, 9, 3, 10, 42, tzinfo=TZ),
        phase_resolver=lambda market, **_kwargs: (
            "non_trading" if market == "cn" else "intraday"
        ),
    )
    assert split["should_run"] is True
    assert split["status"] == "partial_market_open"
    assert split["active_markets"] == ["hk"]
    assert split["market_states"] == {"cn": "closed", "hk": "open_session_day"}


def test_calendar_unknown_fails_open_without_claiming_calendar_health() -> None:
    result = evaluate_market_sessions(
        datetime(2026, 8, 31, 10, 42, tzinfo=TZ),
        phase_resolver=lambda _market, **_kwargs: "unknown",
    )
    assert result["should_run"] is True
    assert result["calendar_degraded"] is True
    assert result["status"] == "calendar_degraded"
    assert result["active_markets"] == ["cn", "hk"]


def test_slot_guard_marks_confirmed_closed_day_as_neutral_skip() -> None:
    result = evaluate_slot(
        event_name="workflow_dispatch",
        event_schedule="",
        requested_slot="morning",
        trigger_source="watchdog",
        now=datetime(2026, 8, 29, 10, 42, tzinfo=TZ),
        ledger={"schema_version": 1, "slots": {}},
        market_sessions={
            "status": "market_closed",
            "should_run": False,
            "calendar_degraded": False,
            "active_markets": [],
            "market_states": {"cn": "closed", "hk": "closed"},
        },
    )
    assert result["should_run"] is False
    assert result["skip_reason"] == "all_markets_closed"
    assert result["calendar_status"] == "market_closed"


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


def test_watchdog_syncs_current_slot_artifact_atomically(tmp_path: Path) -> None:
    now = datetime(2026, 8, 28, 10, 42, tzinfo=TZ)
    payload = {
        "generated_at": "2026-08-28T10:31:00+08:00",
        "simulation_only": True,
        "auto_order_enabled": False,
        "human_confirmation_required": True,
        "scheduler": {"slot": "morning", "trigger_source": "schedule"},
        "candidates": [],
    }
    archive = _market_scan_artifact(payload)

    class FakeClient:
        def recent_runs(self, _workflow, _ref):
            return [
                {
                    "id": 88,
                    "created_at": "2026-08-28T02:31:00Z",
                    "status": "completed",
                    "conclusion": "success",
                }
            ]

        def dispatch(self, _workflow, _ref, _slot):
            raise AssertionError("covered slot must not be dispatched")

        def artifacts_for_run(self, run_id):
            assert run_id == 88
            return [
                {
                    "id": 99,
                    "name": "market-scan-state",
                    "expired": False,
                    "created_at": "2026-08-28T02:40:00Z",
                }
            ]

        def download_artifact(self, artifact_id):
            assert artifact_id == 99
            return archive

    target = tmp_path / "data" / "market_scan" / "latest.json"
    result = run_watchdog(
        slot="morning",
        now_fn=lambda: now,
        sleep_fn=lambda _seconds: None,
        client=FakeClient(),
        workflow="02-market-scan.yml",
        ref="main",
        sync_latest_path=target,
        sync_timeout_seconds=0,
    )

    assert result["status"] == "already_covered"
    assert result["sync"]["status"] == "synced"
    assert result["sync"]["run_id"] == 88
    assert result["sync"]["artifact_id"] == 99
    assert json.loads(target.read_text(encoding="utf-8")) == payload
    assert not list(target.parent.glob(".latest.json.*.tmp"))


def test_watchdog_sync_skips_newer_duplicate_run_without_artifact(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 28, 10, 50, tzinfo=TZ)
    payload = {
        "generated_at": "2026-08-28T10:31:00+08:00",
        "simulation_only": True,
        "auto_order_enabled": False,
        "human_confirmation_required": True,
        "scheduler": {"slot": "morning", "trigger_source": "schedule"},
        "candidates": [],
    }
    archive = _market_scan_artifact(payload)

    class FakeClient:
        def recent_runs(self, _workflow, _ref):
            return [
                {
                    "id": 89,
                    "created_at": "2026-08-28T02:42:00Z",
                    "status": "completed",
                    "conclusion": "success",
                },
                {
                    "id": 88,
                    "created_at": "2026-08-28T02:31:00Z",
                    "status": "completed",
                    "conclusion": "success",
                },
            ]

        def dispatch(self, _workflow, _ref, _slot):
            raise AssertionError("covered slot must not be dispatched")

        def artifacts_for_run(self, run_id):
            if run_id == 89:
                return []
            assert run_id == 88
            return [
                {
                    "id": 99,
                    "name": "market-scan-state",
                    "expired": False,
                    "created_at": "2026-08-28T02:40:00Z",
                }
            ]

        def download_artifact(self, artifact_id):
            assert artifact_id == 99
            return archive

    target = tmp_path / "latest.json"
    result = run_watchdog(
        slot="morning",
        now_fn=lambda: now,
        sleep_fn=lambda _seconds: None,
        client=FakeClient(),
        workflow="02-market-scan.yml",
        ref="main",
        sync_latest_path=target,
        sync_timeout_seconds=0,
    )

    assert result["sync"]["status"] == "synced"
    assert result["sync"]["run_id"] == 88
    assert json.loads(target.read_text(encoding="utf-8")) == payload


def test_watchdog_artifact_rejects_traversal_and_wrong_slot() -> None:
    valid = {
        "generated_at": "2026-08-28T10:31:00+08:00",
        "simulation_only": True,
        "auto_order_enabled": False,
        "human_confirmation_required": True,
        "scheduler": {"slot": "afternoon"},
    }
    with pytest.raises(ValueError, match="unsafe artifact path"):
        _read_market_scan_latest_from_artifact(
            _market_scan_artifact(valid, member_name="../data/market_scan/latest.json")
        )

    raw_latest = _read_market_scan_latest_from_artifact(
        _market_scan_artifact(valid)
    )
    with pytest.raises(ValueError, match="slot does not match"):
        _validate_synced_market_scan(
            raw_latest,
            slot="morning",
            observed_at=datetime(2026, 8, 28, 10, 42, tzinfo=TZ),
        )


def test_watchdog_returns_before_wait_or_api_calls_when_markets_are_closed() -> None:
    calls: list[str] = []

    class FailIfCalledClient:
        def recent_runs(self, _workflow, _ref):
            raise AssertionError("closed-day watchdog must not query workflow runs")

        def dispatch(self, _workflow, _ref, _slot):
            raise AssertionError("closed-day watchdog must not dispatch")

    result = run_watchdog(
        slot="close",
        now_fn=lambda: datetime(2026, 8, 29, 5, 0, tzinfo=TZ),
        sleep_fn=lambda _seconds: calls.append("sleep"),
        client=FailIfCalledClient(),
        workflow="02-market-scan.yml",
        ref="main",
        session_gate=lambda _now: {
            "status": "market_closed",
            "should_run": False,
            "active_markets": [],
            "market_states": {"cn": "closed", "hk": "closed"},
        },
    )
    assert result["status"] == "market_closed"
    assert calls == []


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
    assert '--markets "${{ steps.slot_guard.outputs.active_markets }}"' in scan_text
    assert "snapshot-retry-backoff-seconds" in scan_text

    intraday_text = (ROOT / ".github/workflows/01-intraday-session.yml").read_text(
        encoding="utf-8"
    )
    intraday = yaml.load(intraday_text, Loader=yaml.BaseLoader)
    assert intraday["permissions"]["actions"] == "write"
    assert "market_scan_calendar.py" in intraday_text
    assert "steps.market_calendar.outputs.should_run == 'true'" in intraday_text
    assert "market_scan_watchdog.py" in intraday_text
    assert "--sync-latest-path" in intraday_text
    assert 'exit "$WATCHDOG_STATUS"' in intraday_text

    daily_text = (ROOT / ".github/workflows/00-daily-analysis.yml").read_text(
        encoding="utf-8"
    )
    daily = yaml.load(daily_text, Loader=yaml.BaseLoader)
    close_watchdog = daily["jobs"]["close-scan-watchdog"]
    assert close_watchdog["permissions"]["actions"] == "write"
    assert close_watchdog["if"] == (
        "github.event_name == 'schedule' && "
        "github.event.schedule == '0 10 * * 1-5'"
    )
