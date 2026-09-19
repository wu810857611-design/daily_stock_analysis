from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from scripts.market_scan_subchain_status import reconcile_subchain_status


TZ = ZoneInfo("Asia/Shanghai")


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_failed_market_scan_is_delegated_and_does_not_notify_twice(tmp_path: Path):
    report = tmp_path / "watchdog.json"
    _write(
        report,
        {
            "status": "dispatched",
            "sync": {
                "status": "timeout",
                "error": "market_scan_run_failed:failure:run=123",
            },
        },
    )
    calls = []
    result = reconcile_subchain_status(
        watchdog_report=report,
        state_path=tmp_path / "state.json",
        output_path=tmp_path / "result.json",
        exit_code=1,
        session="morning",
        now=datetime(2026, 9, 11, 12, 2, tzinfo=TZ),
        sender=lambda **kwargs: calls.append(kwargs) or True,
    )
    assert result["operational_status"] == "degraded"
    assert result["main_intraday_health_affected"] is False
    assert result["impact"] == "new_buy_candidate_hot_reload_unavailable"
    assert result["notification"]["status"] == "delegated_to_market_scan"
    assert calls == []


def test_watchdog_owned_failure_notifies_once_and_reports_recovery(tmp_path: Path):
    report = tmp_path / "watchdog.json"
    state = tmp_path / "state.json"
    output = tmp_path / "result.json"
    calls = []

    def sender(**kwargs):
        calls.append(kwargs)
        return True
    now = datetime(2026, 9, 11, 12, 2, tzinfo=TZ)
    _write(
        report,
        {
            "status": "dispatched",
            "sync": {
                "status": "timeout",
                "error": "RuntimeError:GitHub API unavailable",
            },
        },
    )
    first = reconcile_subchain_status(
        watchdog_report=report,
        state_path=state,
        output_path=output,
        exit_code=1,
        session="morning",
        now=now,
        sender=sender,
    )
    duplicate = reconcile_subchain_status(
        watchdog_report=report,
        state_path=state,
        output_path=output,
        exit_code=1,
        session="morning",
        now=now,
        sender=sender,
    )
    assert first["notification"]["status"] == "sent"
    assert duplicate["notification"]["status"] == "suppressed_duplicate"
    assert len(calls) == 1

    _write(report, {"status": "already_covered", "sync": {"status": "synced"}})
    recovered = reconcile_subchain_status(
        watchdog_report=report,
        state_path=state,
        output_path=output,
        exit_code=0,
        session="morning",
        now=now,
        sender=sender,
    )
    assert recovered["operational_status"] == "healthy"
    assert recovered["notification"]["status"] == "sent"
    assert "恢复" in calls[-1]["title"]


def test_missing_watchdog_report_is_a_nonfatal_degradation(tmp_path: Path):
    result = reconcile_subchain_status(
        watchdog_report=tmp_path / "missing.json",
        state_path=tmp_path / "state.json",
        output_path=tmp_path / "result.json",
        exit_code=1,
        session="afternoon",
        now=datetime(2026, 9, 11, 16, 2, tzinfo=TZ),
        sender=lambda **_kwargs: False,
    )
    assert result["operational_status"] == "degraded"
    assert result["reason"] == "watchdog_report_missing"
    assert result["notification"]["status"] == "pending"
    assert result["main_intraday_health_affected"] is False


def test_in_progress_scan_is_pending_without_notification(tmp_path: Path):
    report = tmp_path / "watchdog.json"
    calls = []
    _write(
        report,
        {
            "status": "already_covered",
            "sync": {
                "status": "in_progress",
                "run_id": 141,
                "error": "market_scan_in_progress:run=141",
            },
        },
    )
    result = reconcile_subchain_status(
        watchdog_report=report,
        state_path=tmp_path / "state.json",
        output_path=tmp_path / "result.json",
        exit_code=0,
        session="afternoon",
        now=datetime(2026, 9, 18, 16, 2, tzinfo=TZ),
        sender=lambda **kwargs: calls.append(kwargs) or True,
    )
    assert result["operational_status"] == "pending"
    assert result["reason"] == "market_scan_in_progress:run=141"
    assert result["impact"] == "new_buy_candidate_hot_reload_pending"
    assert result["main_intraday_health_affected"] is False
    assert result["notification"]["status"] == "not_required"
    assert calls == []
