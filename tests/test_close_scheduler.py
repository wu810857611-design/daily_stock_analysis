"""Offline close scheduler contracts; no production dispatch or notification."""
from datetime import datetime
import json
from pathlib import Path
import subprocess
from unittest.mock import patch

import pytest
import yaml

from scripts import close_run_guard, intraday_supervisor
from src.services.close_analysis_context import SHANGHAI, resolve_close_context
from src.services.close_scheduler import admit_close_run, close_runs, evaluate_close_supervisor

ROOT = Path(__file__).resolve().parents[1]


def at(value="2026-10-12T18:08:00+08:00"):
    return datetime.fromisoformat(value)


def run(run_id=1, **kwargs):
    return {"id": run_id, "event": "workflow_dispatch", "created_at": "2026-10-12T10:00:00Z",
            "display_title": "close | mode=full | date=auto | source=cron_job_org",
            "status": "completed", "conclusion": "success", "run_started_at": "2026-10-12T10:00:10Z",
            "run_attempt": 1, **kwargs}


class Client:
    def __init__(self, runs=(), query_error=None, dispatch_error=None):
        self.runs = list(runs)
        self.query_error, self.dispatch_error = query_error, dispatch_error
        self.calls = []

    def recent_runs(self, workflow, ref):
        assert workflow == "00-daily-analysis.yml"
        if self.query_error:
            raise self.query_error
        return self.runs

    def dispatch(self, workflow, ref, *, session, trade_date):
        self.calls.append((workflow, ref, session, trade_date))
        if self.dispatch_error:
            raise self.dispatch_error


def evaluate(client=None, now=None, state=None, **kwargs):
    return evaluate_close_supervisor(now=now or at(), client=client or Client(), ref="main",
        state=state if state is not None else {}, calendar=lambda _: {"cn", "hk"}, **kwargs)


def test_missing_primary_has_one_bounded_replacement_and_a_real_fault_notice():
    client, state = Client(), {}
    first = evaluate(client, state=state)
    assert first["dispatch_succeeded"] and first["alert_required"]
    assert first["alert_code"] == "close_primary_missing_rescued"
    assert client.calls == [("00-daily-analysis.yml", "main", "close", "2026-10-12")]
    second = evaluate(client, now=at("2026-10-12T18:15:00+08:00"), state=state)
    assert second["status"] == "close_dispatch_unconfirmed" and len(client.calls) == 1


@pytest.mark.parametrize("conclusion", ["failure", "cancelled", "timed_out", "action_required"])
def test_failed_existing_run_is_alerted_without_reanalysis(conclusion):
    client = Client([run(conclusion=conclusion)])
    result = evaluate(client)
    assert result["alert_code"] == "close_run_failed" and not client.calls


@pytest.mark.parametrize("status", ["queued", "pending", "waiting", "requested"])
def test_existing_queue_never_causes_second_dispatch(status):
    client = Client([run(status=status, conclusion=None, run_started_at=None)])
    assert evaluate(client)["status"] == "already_requested"
    late = evaluate(client, now=at("2026-10-12T18:25:00+08:00"))
    assert late["alert_code"] == "close_run_not_started" and not client.calls


def test_running_then_overdue_is_distinct_from_completed():
    client = Client([run(status="in_progress", conclusion=None)])
    assert evaluate(client)["status"] == "already_running"
    assert evaluate(client, now=at("2026-10-12T19:46:00+08:00"))["alert_code"] == "close_run_overdue"
    assert evaluate(Client([run()]))["status"] == "already_completed"


@pytest.mark.parametrize("now,target", [
    ("2026-10-12T19:30:01+08:00", "2026-10-12"),
    ("2026-10-13T03:32:00+08:00", "2026-10-12"),
    ("2026-10-13T10:00:00+08:00", "2026-10-12"),
])
def test_late_or_overnight_check_is_alert_only_for_original_day(now, target):
    client = Client()
    result = evaluate(client, now=at(now), trade_date=target)
    assert result["trade_date"] == target and result["alert_code"] == "close_run_missing_late"
    assert not client.calls


def test_before_close_and_market_closed_do_not_query_or_dispatch():
    client = Client(query_error=AssertionError("no request before slot"))
    assert evaluate(client, now=at("2026-10-12T17:59:59+08:00"))["status"] == "before_close"
    result = evaluate_close_supervisor(now=at(), client=client, ref="main", state={}, calendar=lambda _: set())
    assert result["status"] == "market_closed" and not client.calls


def test_calendar_and_api_faults_do_not_guess_or_dispatch():
    client = Client(query_error=RuntimeError("API unavailable"))
    assert evaluate(client)["alert_code"] == "github_api_failed" and not client.calls
    def broken(_):
        raise RuntimeError("calendar unavailable")
    result = evaluate_close_supervisor(now=at(), client=Client(), ref="main", state={}, calendar=broken)
    assert result["alert_code"] == "calendar_failed"


def test_uncertain_post_is_not_repeated_and_is_preserved_in_state():
    client, state = Client(dispatch_error=RuntimeError("HTTP 503")), {}
    assert evaluate(client, state=state)["alert_code"] == "dispatch_failed"
    assert evaluate(client, state=state)["alert_code"] == "close_dispatch_unconfirmed"
    assert len(client.calls) == 1


def test_api_truncated_day_cannot_prove_no_previous_owner():
    client = Client([run(i) for i in range(100)])
    assert evaluate(client)["alert_code"] == "github_api_failed" and not client.calls


def test_dates_modes_and_midnight_are_matched_without_next_day_rollover():
    rows = [run(), run(2, created_at="2026-10-12T19:32:03Z"),
            run(3, display_title="close | mode=market-only | date=auto"),
            run(4, display_title="close | mode=full | date=2026-10-13"),
            run(5, created_at="2026-10-13T01:00:00Z"),
            run(6, event="push"), run(7, created_at="not-a-date")]
    assert [r["id"] for r in close_runs(rows, "2026-10-12")] == [1, 2]


def test_later_created_external_owner_blocks_an_older_created_delayed_native_run():
    context = {"trade_date": "2026-10-12", "open_markets": ["cn", "hk"]}
    current = run(1, status="in_progress", conclusion=None, run_started_at="2026-10-12T19:32:03Z")
    external = run(2, created_at="2026-10-12T10:00:12Z", run_started_at="2026-10-12T10:00:20Z")
    result = admit_close_run(context=context, current=current, runs=[current, external])
    assert not result["should_run"] and result["owner_run_id"] == 2
    # A green duplicate cannot mask the original failing owner.
    client = Client([current, {**external, "conclusion": "failure"}])
    assert evaluate(client)["alert_code"] == "close_run_failed"


def test_new_admission_skips_closed_days_and_reruns_and_does_not_claim_on_api_error(tmp_path, monkeypatch):
    context = {"trade_date": "2026-10-12", "open_markets": ["cn", "hk"]}
    current = run(status="in_progress", conclusion=None)
    assert admit_close_run(context=context, current=current, runs=[current])["should_run"]
    assert not admit_close_run(context={**context, "open_markets": []}, current=current, runs=[]) ["should_run"]
    assert admit_close_run(context=context, current={**current, "run_attempt": 2}, runs=[])["status"] == "rerun_blocked"
    metadata, output, github_output = (tmp_path / n for n in ("run.json", "audit.json", "outputs"))
    metadata.write_text(json.dumps(current))
    monkeypatch.setenv("GH_TOKEN", "offline-placeholder")
    with patch.object(close_run_guard, "resolve_close_context", return_value=context), \
         patch.object(close_run_guard, "GitHubActionsClient", return_value=Client(query_error=RuntimeError("API unavailable"))):
        assert close_run_guard.main(["--repo", "fixture/repo", "--ref", "main", "--event", "workflow_dispatch",
            "--run-metadata", str(metadata), "--output", str(output), "--github-output", str(github_output)]) == 2
    assert "should_run=false" in github_output.read_text() and json.loads(output.read_text())["status"] == "admission_failed"


def test_cancelled_without_runner_is_not_an_owner_and_failed_owner_still_blocks():
    context = {"trade_date": "2026-10-12", "open_markets": ["hk"]}
    current = run(3, status="in_progress", conclusion=None)
    cancelled = run(1, conclusion="cancelled", run_started_at=None)
    assert admit_close_run(context=context, current=current, runs=[cancelled])["should_run"]
    failed = run(2, conclusion="failure")
    assert not admit_close_run(context=context, current=current, runs=[cancelled, failed])["should_run"]


def test_explicit_date_cannot_override_original_creation_slot():
    kwargs = dict(event="workflow_dispatch", schedule="", created_at="2026-10-12T19:32:03Z",
                  now=at("2026-10-13T03:40:00+08:00"), budget_minutes=90)
    assert resolve_close_context(**kwargs, requested_date="2026-10-12")["trade_date"] == "2026-10-12"
    with pytest.raises(ValueError, match="does not match"):
        resolve_close_context(**kwargs, requested_date="2026-10-13")


def test_close_fault_notification_uses_existing_dedup_and_retry_without_real_push():
    result = evaluate(now=at("2026-10-12T19:46:00+08:00"))
    state, calls = {}, []
    def sender(**kwargs):
        calls.append(kwargs)
        return True
    assert intraday_supervisor.deliver_alert_once(result, state=state, now=at(), sender=sender)
    assert "收盘调度故障" in calls[0]["title"]
    assert intraday_supervisor.deliver_alert_once(result, state=state, now=at(), sender=sender)
    assert len(calls) == 1 and result["notification"]["status"] == "suppressed_duplicate"


def test_close_dispatch_http_payload_cannot_force_or_dispatch_intraday():
    from tests.test_intraday_supervisor import FakeHttpResponse
    calls = []
    def opener(request, timeout):
        calls.append(request)
        return FakeHttpResponse(204)
    client = intraday_supervisor.GitHubActionsClient(repo="fixture/repo", token="offline-placeholder", opener=opener)
    client.dispatch("00-daily-analysis.yml", "main", session="close", trade_date="2026-10-12")
    payload = json.loads(calls[0].data)
    assert payload == {"ref": "main", "inputs": {"mode": "full", "force_run": "false", "trade_date": "2026-10-12", "trigger_source": "supervisor"}}


@pytest.mark.parametrize("valid", [True, False])
def test_actual_supervisor_cli_keeps_midnight_day_or_alerts_invalid_metadata(tmp_path, monkeypatch, valid):
    metadata = tmp_path / "metadata.json"
    metadata.write_text(json.dumps({"created_at": "2026-10-12T19:35:00Z"} if valid else {}))
    state, report = tmp_path / "state.json", tmp_path / "report.json"
    client = Client([run(conclusion="failure")])
    monkeypatch.setenv("GH_TOKEN", "offline-placeholder")
    with patch.object(intraday_supervisor, "GitHubActionsClient", return_value=client), \
         patch.object(intraday_supervisor, "_default_notification_sender", return_value=True):
        status = intraday_supervisor.main(["--repo", "fixture/repo", "--event-name", "schedule",
            "--event-schedule", "8 10 * * 1-5", "--now", "2026-10-13T03:40:00+08:00",
            "--run-metadata", str(metadata), "--state", str(state), "--report", str(report)])
    result = json.loads(report.read_text())
    assert status == 1 and not client.calls and result["notification"]["status"] == "sent"
    if valid:
        assert result["trade_date"] == "2026-10-12" and result["alert_code"] == "close_run_failed"
    else:
        assert result["alert_code"] == "close_context_failed"
    assert json.loads(state.read_text())["alerts"]


def test_workflow_admission_precedes_all_state_writes_and_external_has_scan_watchdog():
    wf = yaml.load((ROOT / ".github/workflows/00-daily-analysis.yml").read_text(), Loader=yaml.BaseLoader)
    assert wf["concurrency"] == {"group": "stock-analysis", "cancel-in-progress": "false"}
    for name in ("analyze", "close-scan-watchdog"):
        assert wf["jobs"][name]["needs"] == "close-admission"
        assert "should_run == 'true'" in wf["jobs"][name]["if"]
    assert "inputs.mode == 'full'" in wf["jobs"]["close-scan-watchdog"]["if"]
    for file in ("00-daily-analysis.yml", "03-intraday-supervisor.yml"):
        workflow = yaml.load((ROOT / ".github/workflows" / file).read_text(), Loader=yaml.BaseLoader)
        for job in workflow["jobs"].values():
            for step in job["steps"]:
                if "run" in step:
                    checked = subprocess.run(["bash", "-n"], input=step["run"], text=True, capture_output=True)
                    assert checked.returncode == 0, (step["name"], checked.stderr)
