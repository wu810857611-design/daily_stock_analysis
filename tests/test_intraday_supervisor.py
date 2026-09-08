"""Offline contracts for the top-level intraday scheduler supervisor."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import pytest
import yaml

from scripts import intraday_supervisor as supervisor_module
from scripts.intraday_slot_guard import evaluate_session_start, resolve_session
from scripts.intraday_supervisor import (
    GitHubActionsClient,
    deliver_alert_once,
    evaluate_supervisor,
    retry_pending_alerts,
    resolve_supervisor_session,
    run_matches_session,
)

TZ = ZoneInfo("Asia/Shanghai")
ROOT = Path(__file__).resolve().parents[1]


def _open_calendar(_now: datetime) -> Mapping[str, Any]:
    return {
        "status": "open",
        "should_run": True,
        "calendar_degraded": False,
        "active_markets": ["cn", "hk"],
        "market_states": {"cn": "open_session_day", "hk": "open_session_day"},
    }


class FakeClient:
    def __init__(self, runs=None, *, query_error=None, dispatch_error=None):
        self.runs = list(runs or [])
        self.query_error = query_error
        self.dispatch_error = dispatch_error
        self.queries = []
        self.dispatches = []

    def recent_runs(self, workflow, ref):
        self.queries.append((workflow, ref))
        if self.query_error:
            raise self.query_error
        return self.runs

    def dispatch(self, workflow, ref, *, session, trade_date):
        self.dispatches.append((workflow, ref, session, trade_date))
        if self.dispatch_error:
            raise self.dispatch_error


class FakeHttpResponse:
    def __init__(self, status, body=b""):
        self.status = status
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return self.body


def test_github_client_dispatches_explicit_auditable_session_inputs():
    calls = []

    def opener(outgoing, timeout):
        calls.append((outgoing, timeout))
        return FakeHttpResponse(204)

    client = GitHubActionsClient(
        repo="owner/repo",
        token="test-token",
        opener=opener,
    )
    client.dispatch(
        "01-intraday-session.yml",
        "main",
        session="morning",
        trade_date="2026-09-07",
    )

    outgoing, timeout = calls[0]
    payload = json.loads(outgoing.data.decode("utf-8"))
    assert outgoing.method == "POST"
    assert timeout == 20
    assert outgoing.full_url.endswith("/actions/workflows/01-intraday-session.yml/dispatches")
    assert payload == {
        "ref": "main",
        "inputs": {
            "session": "morning",
            "trade_date": "2026-09-07",
            "trigger_source": "supervisor",
            "max_cycles": "0",
        },
    }


@pytest.mark.parametrize(
    ("session", "now"),
    [
        ("morning", datetime(2026, 9, 7, 9, 30, tzinfo=TZ)),
        ("afternoon", datetime(2026, 9, 7, 13, 0, tzinfo=TZ)),
    ],
)
def test_missing_external_cron_is_dispatched_inside_safe_window(session, now):
    client = FakeClient()
    result = evaluate_supervisor(
        session=session,
        now=now,
        client=client,
        workflow="01-intraday-session.yml",
        ref="main",
        state={},
        session_gate=_open_calendar,
    )

    assert result["status"] == "dispatched"
    assert result["dispatch_succeeded"] is True
    assert client.dispatches == [("01-intraday-session.yml", "main", session, "2026-09-07")]


def test_delayed_github_schedule_maps_to_original_session_and_never_dispatches():
    now = datetime(2026, 9, 7, 15, 26, tzinfo=TZ)
    session = resolve_supervisor_session(
        event_name="schedule",
        event_schedule="28 1 * * 1-5",
        requested_session="auto",
        now=now,
    )
    client = FakeClient()

    result = evaluate_supervisor(
        session=session,
        now=now,
        client=client,
        workflow="01-intraday-session.yml",
        ref="main",
        state={},
        session_gate=_open_calendar,
    )

    assert session == "morning"
    assert result["status"] == "session_missing_too_late"
    assert result["alert_required"] is True
    assert result["dispatch_attempted"] is False
    assert client.dispatches == []


def test_three_trigger_sources_cannot_create_another_effective_run():
    runs = [
        {
            "id": 1,
            "display_title": ("intraday | session=morning | date=auto | source=cron_job_org"),
            "created_at": "2026-09-07T01:20:00Z",
            "status": "in_progress",
            "conclusion": None,
        },
        {
            "id": 2,
            "display_title": ("intraday | session=morning | date=auto | source=schedule"),
            "created_at": "2026-09-07T01:21:00Z",
            "status": "queued",
            "conclusion": None,
        },
        {
            "id": 3,
            "display_title": ("intraday | session=morning | date=2026-09-07 | " "source=supervisor"),
            "created_at": "2026-09-07T01:22:00Z",
            "status": "completed",
            "conclusion": "success",
        },
    ]
    client = FakeClient(runs)
    result = evaluate_supervisor(
        session="morning",
        now=datetime(2026, 9, 7, 9, 30, tzinfo=TZ),
        client=client,
        workflow="01-intraday-session.yml",
        ref="main",
        state={},
        session_gate=_open_calendar,
    )

    assert result["status"] == "already_covered"
    assert result["matching_run_count"] == 3
    assert client.dispatches == []


def test_cancelled_pending_duplicate_does_not_hide_running_effective_run():
    client = FakeClient(
        [
            {
                "id": 4,
                "created_at": "2026-09-07T01:20:00Z",
                "run_started_at": None,
                "status": "completed",
                "conclusion": "cancelled",
            },
            {
                "id": 5,
                "created_at": "2026-09-07T01:20:05Z",
                "run_started_at": "2026-09-07T01:20:20Z",
                "status": "in_progress",
                "conclusion": None,
            },
        ]
    )

    result = evaluate_supervisor(
        session="morning",
        now=datetime(2026, 9, 7, 9, 30, tzinfo=TZ),
        client=client,
        workflow="01-intraday-session.yml",
        ref="main",
        state={},
        session_gate=_open_calendar,
    )

    assert result["status"] == "already_covered"
    assert result["covered_run_id"] == 5
    assert client.dispatches == []


def test_failed_effective_run_is_not_hidden_by_later_duplicate_success():
    client = FakeClient(
        [
            {
                "id": 6,
                "created_at": "2026-09-07T01:20:00Z",
                "run_started_at": "2026-09-07T01:20:10Z",
                "status": "completed",
                "conclusion": "failure",
            },
            {
                "id": 7,
                "created_at": "2026-09-07T01:20:05Z",
                "run_started_at": "2026-09-07T01:25:00Z",
                "status": "completed",
                "conclusion": "success",
            },
        ]
    )

    result = evaluate_supervisor(
        session="morning",
        now=datetime(2026, 9, 7, 9, 35, tzinfo=TZ),
        client=client,
        workflow="01-intraday-session.yml",
        ref="main",
        state={},
        session_gate=_open_calendar,
    )

    assert result["status"] == "session_run_failed"
    assert result["covered_run_id"] == 6
    assert client.dispatches == []


@pytest.mark.parametrize(
    ("status", "conclusion"),
    [("queued", None), ("in_progress", None), ("completed", "success")],
)
def test_queued_running_or_completed_run_covers_session(status, conclusion):
    client = FakeClient(
        [
            {
                "id": 10,
                "created_at": "2026-09-07T04:50:00Z",
                "status": status,
                "conclusion": conclusion,
            }
        ]
    )
    result = evaluate_supervisor(
        session="afternoon",
        now=datetime(2026, 9, 7, 13, 0, tzinfo=TZ),
        client=client,
        workflow="01-intraday-session.yml",
        ref="main",
        state={},
        session_gate=_open_calendar,
    )
    assert result["status"] == "already_covered"
    assert client.dispatches == []


def test_failed_existing_session_alerts_without_retry_dispatch():
    client = FakeClient(
        [
            {
                "id": 11,
                "created_at": "2026-09-07T01:20:00Z",
                "status": "completed",
                "conclusion": "failure",
            }
        ]
    )
    result = evaluate_supervisor(
        session="morning",
        now=datetime(2026, 9, 7, 9, 35, tzinfo=TZ),
        client=client,
        workflow="01-intraday-session.yml",
        ref="main",
        state={},
        session_gate=_open_calendar,
    )
    assert result["status"] == "session_run_failed"
    assert result["alert_required"] is True
    assert client.dispatches == []


@pytest.mark.parametrize(
    ("status", "run_started_at", "expected"),
    [
        ("queued", None, "session_not_started_before_deadline"),
        (
            "completed",
            "2026-09-07T02:16:00Z",
            "session_started_too_late",
        ),
    ],
)
def test_queued_or_late_started_run_cannot_mask_missed_session(status, run_started_at, expected):
    client = FakeClient(
        [
            {
                "id": 12,
                "created_at": "2026-09-07T01:20:00Z",
                "run_started_at": run_started_at,
                "status": status,
                "conclusion": "success" if status == "completed" else None,
            }
        ]
    )
    result = evaluate_supervisor(
        session="morning",
        now=datetime(2026, 9, 7, 10, 16, tzinfo=TZ),
        client=client,
        workflow="01-intraday-session.yml",
        ref="main",
        state={},
        session_gate=_open_calendar,
    )

    assert result["status"] == expected
    assert result["alert_required"] is True
    assert client.dispatches == []


def test_too_early_completed_run_is_ignored_and_missing_session_is_dispatched():
    client = FakeClient(
        [
            {
                "id": 13,
                "display_title": ("intraday | session=morning | date=auto | source=manual"),
                "created_at": "2026-09-07T01:00:00Z",
                "run_started_at": "2026-09-07T01:00:05Z",
                "status": "completed",
                "conclusion": "success",
            }
        ]
    )
    result = evaluate_supervisor(
        session="morning",
        now=datetime(2026, 9, 7, 9, 30, tzinfo=TZ),
        client=client,
        workflow="01-intraday-session.yml",
        ref="main",
        state={},
        session_gate=_open_calendar,
    )

    assert result["status"] == "dispatched"
    assert result["ignored_early_run_count"] == 1
    assert len(client.dispatches) == 1


def test_before_supervisor_window_waits_for_primary_source():
    client = FakeClient()
    result = evaluate_supervisor(
        session="morning",
        now=datetime(2026, 9, 7, 9, 25, tzinfo=TZ),
        client=client,
        workflow="01-intraday-session.yml",
        ref="main",
        state={},
        session_gate=_open_calendar,
    )
    assert result["status"] == "waiting_for_primary"
    assert client.dispatches == []


def test_confirmed_non_trading_day_does_not_query_or_dispatch():
    client = FakeClient(query_error=AssertionError("must not query"))
    result = evaluate_supervisor(
        session="morning",
        now=datetime(2026, 9, 6, 9, 30, tzinfo=TZ),
        client=client,
        workflow="01-intraday-session.yml",
        ref="main",
        state={},
        session_gate=lambda _now: {
            "status": "market_closed",
            "should_run": False,
            "active_markets": [],
            "market_states": {"cn": "closed", "hk": "closed"},
        },
    )
    assert result["status"] == "market_closed"
    assert result["alert_required"] is False
    assert client.queries == []
    assert client.dispatches == []


@pytest.mark.parametrize(
    ("client", "expected"),
    [
        (FakeClient(query_error=RuntimeError("query unavailable")), "github_api_failed"),
        (FakeClient(dispatch_error=RuntimeError("dispatch unavailable")), "dispatch_failed"),
    ],
)
def test_github_api_and_dispatch_failures_become_system_alerts(client, expected):
    result = evaluate_supervisor(
        session="morning",
        now=datetime(2026, 9, 7, 9, 30, tzinfo=TZ),
        client=client,
        workflow="01-intraday-session.yml",
        ref="main",
        state={},
        session_gate=_open_calendar,
    )
    sent = []
    assert deliver_alert_once(
        result,
        state={},
        now=datetime(2026, 9, 7, 9, 30, tzinfo=TZ),
        sender=lambda **payload: sent.append(payload) or True,
    )
    assert result["status"] == expected
    assert result["notification"]["status"] == "sent"
    assert sent and sent[0]["title"].startswith("SYSTEM：盘中调度故障")


def test_pushplus_failure_is_persisted_for_retry_and_success_is_deduplicated():
    now = datetime(2026, 9, 7, 10, 16, tzinfo=TZ)
    result = evaluate_supervisor(
        session="morning",
        now=now,
        client=FakeClient(),
        workflow="01-intraday-session.yml",
        ref="main",
        state={},
        session_gate=_open_calendar,
    )
    state = {}

    assert not deliver_alert_once(
        result,
        state=state,
        now=now,
        sender=lambda **_payload: False,
    )
    key = "2026-09-07:morning:session_missing_too_late"
    assert state["alerts"][key]["status"] == "pending"
    assert result["notification"]["status"] == "failed"

    sends = []
    assert deliver_alert_once(
        result,
        state=state,
        now=now,
        sender=lambda **payload: sends.append(payload) or True,
    )
    assert state["alerts"][key]["status"] == "sent"
    assert state["alerts"][key]["attempts"] == 2
    assert deliver_alert_once(
        result,
        state=state,
        now=now,
        sender=lambda **_payload: pytest.fail("duplicate alert must not send"),
    )
    assert result["notification"]["status"] == "suppressed_duplicate"
    assert len(sends) == 1


def test_later_supervisor_run_retries_an_earlier_pending_alert():
    now = datetime(2026, 9, 7, 12, 58, tzinfo=TZ)
    state = {
        "alerts": {
            "2026-09-07:morning:session_missing_too_late": {
                "status": "pending",
                "attempts": 1,
                "title": "SYSTEM：盘中调度故障 - 2026-09-07 morning",
                "content": "# 盘中调度故障\n",
            }
        }
    }
    sends = []

    ok, outcomes = retry_pending_alerts(
        state=state,
        now=now,
        sender=lambda **payload: sends.append(payload) or True,
    )

    alert = state["alerts"]["2026-09-07:morning:session_missing_too_late"]
    assert ok is True
    assert outcomes[0]["status"] == "sent"
    assert alert["status"] == "sent"
    assert alert["attempts"] == 2
    assert len(sends) == 1


def test_cli_exposes_pushplus_delivery_failure_and_persists_pending_alert(monkeypatch, tmp_path):
    state_path = tmp_path / "supervisor-state.json"
    report_path = tmp_path / "supervisor-report.json"
    result = {
        "trade_date": "2026-09-07",
        "session": "morning",
        "session_key": "2026-09-07:morning",
        "observed_at": "2026-09-07T10:16:00+08:00",
        "status": "session_missing_too_late",
        "alert_required": True,
        "alert_code": "session_missing_too_late",
        "error": "No valid 01 run",
    }
    monkeypatch.setenv("GH_TOKEN", "test-token")
    monkeypatch.setattr(
        supervisor_module,
        "evaluate_supervisor",
        lambda **_kwargs: dict(result),
    )
    monkeypatch.setattr(
        supervisor_module,
        "_default_notification_sender",
        lambda **_payload: False,
    )

    exit_code = supervisor_module.main(
        [
            "--repo",
            "owner/repo",
            "--session",
            "morning",
            "--now",
            "2026-09-07T10:16:00+08:00",
            "--state",
            str(state_path),
            "--report",
            str(report_path),
        ]
    )

    state = json.loads(state_path.read_text(encoding="utf-8"))
    report = json.loads(report_path.read_text(encoding="utf-8"))
    alert_key = "2026-09-07:morning:session_missing_too_late"
    assert exit_code == 2
    assert report["notification"]["status"] == "failed"
    assert state["alerts"][alert_key]["status"] == "pending"


def test_repeated_supervisor_does_not_repeat_recent_dispatch():
    state = {}
    client = FakeClient()
    first = evaluate_supervisor(
        session="morning",
        now=datetime(2026, 9, 7, 9, 30, tzinfo=TZ),
        client=client,
        workflow="01-intraday-session.yml",
        ref="main",
        state=state,
        session_gate=_open_calendar,
    )
    second = evaluate_supervisor(
        session="morning",
        now=datetime(2026, 9, 7, 9, 31, tzinfo=TZ),
        client=client,
        workflow="01-intraday-session.yml",
        ref="main",
        state=state,
        session_gate=_open_calendar,
    )
    assert first["status"] == "dispatched"
    assert second["status"] == "dispatch_already_requested"
    assert len(client.dispatches) == 1


def test_run_matching_uses_explicit_markers_and_legacy_time_fallback():
    explicit = {
        "display_title": ("intraday | session=morning | date=2026-09-07 | source=supervisor"),
        "created_at": "2026-09-07T01:20:00Z",
    }
    legacy = {
        "display_title": "分钟级盘中模拟监控",
        "created_at": "2026-09-07T04:50:00Z",
    }
    assert run_matches_session(explicit, trade_date="2026-09-07", session="morning")
    assert not run_matches_session(explicit, trade_date="2026-09-07", session="afternoon")
    assert run_matches_session(legacy, trade_date="2026-09-07", session="afternoon")


def test_intraday_entry_guard_enforces_trade_date_calendar_and_late_window():
    on_time = evaluate_session_start(
        event_name="workflow_dispatch",
        event_schedule="",
        requested_session="morning",
        requested_trade_date="2026-09-07",
        trigger_source="cron_job_org",
        now=datetime(2026, 9, 7, 9, 20, tzinfo=TZ),
        market_sessions=_open_calendar(datetime.now(TZ)),
    )
    assert on_time["should_run"] is True
    assert on_time["session_key"] == "2026-09-07:morning"
    assert on_time["claim_key"] == "2026-09-07-morning"

    stale = evaluate_session_start(
        event_name="workflow_dispatch",
        event_schedule="",
        requested_session="morning",
        requested_trade_date="2026-09-06",
        trigger_source="supervisor",
        now=datetime(2026, 9, 7, 9, 30, tzinfo=TZ),
        market_sessions=_open_calendar(datetime.now(TZ)),
    )
    assert stale["should_run"] is False
    assert stale["skip_reason"] == "stale_trade_date"

    late = evaluate_session_start(
        event_name="workflow_dispatch",
        event_schedule="",
        requested_session="afternoon",
        requested_trade_date="auto",
        trigger_source="supervisor",
        now=datetime(2026, 9, 7, 15, 26, tzinfo=TZ),
        market_sessions=_open_calendar(datetime.now(TZ)),
    )
    assert late["should_run"] is False
    assert late["skip_reason"] == "session_too_late"

    closed = evaluate_session_start(
        event_name="workflow_dispatch",
        event_schedule="",
        requested_session="morning",
        requested_trade_date="auto",
        trigger_source="cron_job_org",
        now=datetime(2026, 9, 6, 9, 20, tzinfo=TZ),
        market_sessions={"status": "market_closed", "should_run": False},
    )
    assert closed["should_run"] is False
    assert closed["skip_reason"] == "all_markets_closed"


def test_native_intraday_schedules_map_to_sessions_even_when_delayed():
    delayed = datetime(2026, 9, 7, 15, 26, tzinfo=TZ)
    assert (
        resolve_session(
            event_name="schedule",
            event_schedule="20 1 * * 1-5",
            requested_session="auto",
            now=delayed,
        )
        == "morning"
    )
    assert (
        resolve_session(
            event_name="schedule",
            event_schedule="50 4 * * 1-5",
            requested_session="auto",
            now=delayed,
        )
        == "afternoon"
    )


def test_workflows_wire_two_layer_resilience_and_idempotency():
    intraday_text = (ROOT / ".github/workflows/01-intraday-session.yml").read_text(encoding="utf-8")
    intraday = yaml.load(intraday_text, Loader=yaml.BaseLoader)
    assert {item["cron"] for item in intraday["on"]["schedule"]} == {
        "20 1 * * 1-5",
        "50 4 * * 1-5",
    }
    inputs = intraday["on"]["workflow_dispatch"]["inputs"]
    assert inputs["trade_date"]
    assert inputs["trigger_source"]
    assert "intraday_slot_guard.py" in intraday_text
    assert "intraday-session-claim-" in intraday_text
    assert "duplicate_session_claim" in intraday_text
    assert "claim_restore_failed" in intraday_text
    assert "强制暴露盘中入口调度故障" in intraday_text
    assert "session_too_late|stale_trade_date" in intraday_text
    assert "market_scan_watchdog.py" in intraday_text
    assert '--late-start-policy "$LATE_START_POLICY"' in intraday_text
    assert "严格验证行情与 PushPlus" in intraday_text

    supervisor_text = (ROOT / ".github/workflows/03-intraday-supervisor.yml").read_text(encoding="utf-8")
    supervisor = yaml.load(supervisor_text, Loader=yaml.BaseLoader)
    assert supervisor["permissions"]["actions"] == "write"
    assert {item["cron"] for item in supervisor["on"]["schedule"]} == {
        "28 1 * * 1-5",
        "16 2 * * 1-5",
        "58 4 * * 1-5",
        "46 5 * * 1-5",
    }
    assert "intraday_supervisor.py" in supervisor_text
    assert "--workflow 01-intraday-session.yml" in supervisor_text
    assert "PUSHPLUS_TOKEN" in supervisor_text
    assert "强制暴露调度故障" in supervisor_text
