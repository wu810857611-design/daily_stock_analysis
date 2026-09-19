"""Offline contracts for redundant, idempotent market-scan scheduling."""

from __future__ import annotations

import json
import io
import tarfile
import zipfile
from datetime import datetime
from pathlib import Path
from email.message import Message
from typing import Any, Mapping
from urllib import parse, request, response
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
    GitHubActionsClient,
    _read_market_scan_latest_from_artifact,
    _validate_synced_market_scan,
    existing_run_covers_slot,
    run_watchdog,
)


TZ = ZoneInfo("Asia/Shanghai")
ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("expired_first_url", [False, True])
def test_watchdog_real_redirect_handler_does_not_forward_github_auth(
    tmp_path: Path, expired_first_url: bool,
) -> None:
    """Exercise urllib's actual 302 handler, artifact decoding and atomic sync."""
    now = datetime(2026, 9, 9, 10, 55, tzinfo=TZ)
    payload = {
        "generated_at": "2026-09-09T10:43:08+08:00",
        "simulation_only": True,
        "auto_order_enabled": False,
        "human_confirmation_required": True,
        "scheduler": {"slot": "morning"},
        "candidates": [],
    }
    archive = _market_scan_artifact(payload)
    signed_url_count = 0
    storage_calls = []

    class OfflineHTTPS(request.HTTPSHandler):
        def https_open(self, req):
            nonlocal signed_url_count
            url = parse.urlsplit(req.full_url)
            headers = Message()
            status = 200
            if url.hostname == "api.github.com":
                assert req.get_header("Authorization") == "Bearer offline-secret"
                if url.path.endswith("/runs"):
                    body = json.dumps({"workflow_runs": [{
                        "id": 88, "created_at": "2026-09-09T02:42:00Z",
                        "status": "completed", "conclusion": "success",
                    }]}).encode()
                elif url.path.endswith("/artifacts"):
                    body = json.dumps({"artifacts": [{
                        "id": 99, "name": "market-scan-state", "expired": False,
                    }]}).encode()
                else:
                    assert url.path.endswith("/artifacts/99/zip")
                    signed_url_count += 1
                    headers["Location"] = (
                        "https://storage.example/artifact.zip?sig=a%2Fb%2Bc"
                        f"&generation={signed_url_count}"
                    )
                    status, body = 302, b""
            else:
                assert url.hostname == "storage.example"
                assert not req.has_header("Authorization")
                assert "sig=a%2Fb%2Bc" in url.query
                storage_calls.append(req.full_url)
                if expired_first_url and len(storage_calls) == 1:
                    status, body = 401, b"expired signed URL"
                else:
                    body = archive
            result = response.addinfourl(io.BytesIO(body), headers, req.full_url, status)
            result.msg = {200: "OK", 302: "Found", 401: "Unauthorized"}[status]
            return result

    client = GitHubActionsClient(
        repo="example/repo", token="offline-secret",
        opener=request.build_opener(OfflineHTTPS()).open,
    )
    target = tmp_path / "latest.json"
    result = run_watchdog(
        slot="morning", now_fn=lambda: now, sleep_fn=lambda _seconds: None,
        client=client, workflow="02-market-scan.yml", ref="main",
        sync_latest_path=target, sync_timeout_seconds=15,
    )
    assert result["sync"]["status"] == "synced"
    assert result["sync"]["attempts"] == (2 if expired_first_url else 1)
    assert signed_url_count == len(storage_calls) == result["sync"]["attempts"]
    assert json.loads(target.read_text()) == payload
    assert result["sync"]["content_fingerprint"]
    assert "offline-secret" not in json.dumps(result)


def test_watchdog_timeout_reports_upstream_failure_and_keeps_previous_file(tmp_path: Path) -> None:
    calls = []

    class Client:
        def recent_runs(self, *_args):
            calls.append("query")
            return [{
                "id": 88, "created_at": "2026-09-09T06:42:00Z",
                "status": "in_progress" if len(calls) == 1 else "completed",
                "conclusion": None if len(calls) == 1 else "failure",
            }]

        def dispatch(self, *_args):
            pytest.fail("already running scan must not be dispatched")

    target = tmp_path / "latest.json"
    target.write_text("previous validated snapshot")
    result = run_watchdog(
        slot="afternoon", now_fn=lambda: datetime(2026, 9, 9, 14, 50, tzinfo=TZ),
        sleep_fn=lambda _seconds: pytest.fail("zero-budget sync must not sleep"),
        client=Client(), workflow="02-market-scan.yml", ref="main",
        sync_latest_path=target, sync_timeout_seconds=0,
    )
    assert result["sync"]["status"] == "failed"
    assert result["sync"]["error"] == "market_scan_run_failed:failure:run=88"
    assert target.read_text() == "previous validated snapshot"


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
    observed_failure = run_watchdog(
        slot="morning",
        now_fn=lambda: now,
        sleep_fn=lambda _seconds: None,
        client=failed_client,
        workflow="02-market-scan.yml",
        ref="main",
    )
    assert observed_failure["status"] == "already_failed"
    assert observed_failure["observed_run"]["conclusion"] == "failure"
    assert failed_client.dispatches == []


def test_watchdog_reports_in_progress_without_duplicate_dispatch(tmp_path: Path) -> None:
    now = datetime(2026, 9, 18, 14, 44, tzinfo=TZ)

    class Client:
        def recent_runs(self, *_args):
            return [{
                "id": 141,
                "created_at": "2026-09-18T06:42:00Z",
                "status": "in_progress",
                "conclusion": None,
            }]

        def dispatch(self, *_args):
            pytest.fail("in-progress scan must not be dispatched twice")

    result = run_watchdog(
        slot="afternoon",
        now_fn=lambda: now,
        sleep_fn=lambda _seconds: None,
        client=Client(),
        workflow="02-market-scan.yml",
        ref="main",
        sync_latest_path=tmp_path / "latest.json",
        sync_timeout_seconds=0,
    )
    assert result["status"] == "already_covered"
    assert result["sync"] == {
        "status": "in_progress",
        "attempts": 1,
        "run_id": 141,
        "artifact_id": None,
        "generated_at": "",
        "content_fingerprint": "",
        "path": str(tmp_path / "latest.json"),
        "error": "market_scan_in_progress:run=141",
    }


@pytest.mark.parametrize(
    ("conclusion", "expected"),
    [
        ("cancelled", "market_scan_run_cancelled:run=141"),
        ("timed_out", "market_scan_run_timeout:run=141"),
        ("failure", "market_scan_run_failed:failure:run=141"),
    ],
)
def test_watchdog_preserves_terminal_run_reason(
    tmp_path: Path, conclusion: str, expected: str
) -> None:
    now = datetime(2026, 9, 18, 14, 50, tzinfo=TZ)

    class Client:
        def recent_runs(self, *_args):
            return [{
                "id": 141,
                "created_at": "2026-09-18T06:42:00Z",
                "status": "completed",
                "conclusion": conclusion,
            }]

        def dispatch(self, *_args):
            pytest.fail("terminal current-slot scan must not be re-dispatched")

    result = run_watchdog(
        slot="afternoon",
        now_fn=lambda: now,
        sleep_fn=lambda _seconds: None,
        client=Client(),
        workflow="02-market-scan.yml",
        ref="main",
        sync_latest_path=tmp_path / "latest.json",
        sync_timeout_seconds=0,
    )
    assert result["sync"]["status"] == "failed"
    assert result["sync"]["run_id"] == 141
    assert result["sync"]["error"] == expected


def test_watchdog_distinguishes_successful_run_with_artifact_not_ready(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 9, 18, 14, 50, tzinfo=TZ)

    class Client:
        def recent_runs(self, *_args):
            return [{
                "id": 143,
                "created_at": "2026-09-18T06:42:00Z",
                "status": "completed",
                "conclusion": "success",
            }]

        def dispatch(self, *_args):
            pytest.fail("successful scan must not be dispatched twice")

        def artifacts_for_run(self, _run_id):
            return []

    result = run_watchdog(
        slot="afternoon",
        now_fn=lambda: now,
        sleep_fn=lambda _seconds: None,
        client=Client(),
        workflow="02-market-scan.yml",
        ref="main",
        sync_latest_path=tmp_path / "latest.json",
        sync_timeout_seconds=0,
    )
    assert result["sync"]["status"] == "artifact_not_ready"
    assert result["sync"]["error"] == "market_scan_state_artifact_not_ready:run=143"


def test_watchdog_guard_skipped_success_does_not_hide_cancelled_scan(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 9, 18, 16, 2, tzinfo=TZ)

    class Client:
        def recent_runs(self, *_args):
            return [
                {
                    "id": 142,
                    "created_at": "2026-09-18T07:58:00Z",
                    "status": "completed",
                    "conclusion": "success",
                },
                {
                    "id": 141,
                    "created_at": "2026-09-18T06:42:00Z",
                    "status": "completed",
                    "conclusion": "cancelled",
                },
            ]

        def dispatch(self, *_args):
            pytest.fail("observe-only reconciliation must not dispatch")

        def artifacts_for_run(self, run_id):
            assert run_id == 142
            return []

    result = run_watchdog(
        slot="afternoon",
        now_fn=lambda: now,
        sleep_fn=lambda _seconds: None,
        client=Client(),
        workflow="02-market-scan.yml",
        ref="main",
        observe_only=True,
        sync_latest_path=tmp_path / "latest.json",
        sync_timeout_seconds=0,
    )

    assert result["sync"]["status"] == "failed"
    assert result["sync"]["run_id"] == 141
    assert result["sync"]["error"] == "market_scan_run_cancelled:run=141"


def test_watchdog_observe_only_never_dispatches_even_after_safe_window(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 9, 18, 16, 2, tzinfo=TZ)

    class Client:
        def recent_runs(self, *_args):
            return []

        def dispatch(self, *_args):
            pytest.fail("observe-only final check must never dispatch")

    result = run_watchdog(
        slot="afternoon",
        now_fn=lambda: now,
        sleep_fn=lambda _seconds: None,
        client=Client(),
        workflow="02-market-scan.yml",
        ref="main",
        sync_latest_path=tmp_path / "latest.json",
        sync_timeout_seconds=0,
        observe_only=True,
    )
    assert result["status"] == "missing"
    assert result["sync"]["status"] == "missing"
    assert result["sync"]["error"] == "market_scan_missing"


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
    assert "--total-timeout-seconds" in scan_text
    assert "data/market_scan/runtime.json" in scan_text

    intraday_text = (ROOT / ".github/workflows/01-intraday-session.yml").read_text(
        encoding="utf-8"
    )
    intraday = yaml.load(intraday_text, Loader=yaml.BaseLoader)
    assert intraday["permissions"]["actions"] == "write"
    assert "market_scan_calendar.py" in intraday_text
    assert "steps.market_calendar.outputs.should_run == 'true'" in intraday_text
    assert "market_scan_watchdog.py" in intraday_text
    assert "--sync-timeout-seconds 120" in intraday_text
    assert "--observe-only" in intraday_text
    assert "--sync-latest-path" in intraday_text
    assert "market_scan_subchain_status.py" in intraday_text
    assert "market_scan_subchain_state.json" in intraday_text
    assert "全市场买入子链已独立降级" in intraday_text
    assert "全市场买入子链状态落盘或通知失败" in intraday_text
    assert 'exit "$WATCHDOG_STATUS"' not in intraday_text
    assert 'exit "$MONITOR_STATUS"' in intraday_text

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
