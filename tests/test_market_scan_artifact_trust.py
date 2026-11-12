"""Regression tests for unified market-scan artifact trust semantics."""

from __future__ import annotations

import hashlib
import io
import json
import tarfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from scripts.market_scan_artifact_trust import (
    SHANGHAI_TZ,
    validate_startup_market_scan,
    validate_watchdog_market_scan,
)
from scripts.market_scan_startup_restore import restore_latest_trusted_market_scan
from scripts.market_scan_watchdog import run_watchdog
from scripts.market_scan_subchain_status import classify_watchdog
from scripts.intraday_session import read_candidate_plan_snapshot


def _payload(*, generated_at: str, slot: str) -> dict[str, Any]:
    return {
        "generated_at": generated_at,
        "simulation_only": True,
        "auto_order_enabled": False,
        "human_confirmation_required": True,
        "scheduler": {"slot": slot},
        "candidates": [],
    }


def _artifact(payload: dict[str, Any]) -> bytes:
    latest = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    nested_buffer = io.BytesIO()
    with tarfile.open(fileobj=nested_buffer, mode="w:gz") as archive:
        info = tarfile.TarInfo("data/market_scan/latest.json")
        info.size = len(latest)
        archive.addfile(info, io.BytesIO(latest))

    outer_buffer = io.BytesIO()
    with zipfile.ZipFile(outer_buffer, mode="w") as archive:
        archive.writestr("market-scan-state.tar.gz", nested_buffer.getvalue())
    return outer_buffer.getvalue()


class FakeClient:
    def __init__(
        self,
        *,
        conclusion: str = "success",
        payload: dict[str, Any] | None = None,
        head_branch: str = "main",
    ) -> None:
        self.conclusion = conclusion
        self.payload = payload or _payload(
            generated_at="2026-09-29T10:43:00+08:00",
            slot="morning",
        )
        self.head_branch = head_branch
        self.artifact_calls = 0

    def recent_runs(self, _workflow: str, _ref: str):
        return [{
            "id": 175,
            "created_at": "2026-09-29T02:42:03Z",
            "status": "completed",
            "conclusion": self.conclusion,
            "head_branch": self.head_branch,
        }]

    def artifacts_for_run(self, run_id: int):
        self.artifact_calls += 1
        assert run_id == 175
        return [{
            "id": 1101,
            "name": "market-scan-state",
            "expired": False,
            "created_at": "2026-09-29T02:49:33Z",
        }]

    def download_artifact(self, artifact_id: int):
        assert artifact_id == 1101
        return _artifact(self.payload)


def test_successful_same_day_morning_artifact_restores_for_afternoon(
    tmp_path: Path,
) -> None:
    target = tmp_path / "latest.json"
    client = FakeClient()
    observed = datetime(2026, 9, 29, 12, 50, tzinfo=SHANGHAI_TZ)

    report = restore_latest_trusted_market_scan(
        client=client,
        workflow="02-market-scan.yml",
        ref="main",
        session="afternoon",
        observed_at=observed,
        target_path=target,
    )

    assert report["status"] == "trusted"
    assert report["trusted"] is True
    assert report["run_id"] == 175
    assert report["content_fingerprint"] == hashlib.sha256(target.read_bytes()).hexdigest()
    assert json.loads(target.read_text(encoding="utf-8"))["scheduler"]["slot"] == "morning"


@pytest.mark.parametrize("conclusion", ["failure", "cancelled", "timed_out"])
def test_terminal_non_success_run_never_restores_candidate_plan(
    tmp_path: Path,
    conclusion: str,
) -> None:
    target = tmp_path / "latest.json"
    target.write_text('{"stale":"cache"}', encoding="utf-8")
    client = FakeClient(conclusion=conclusion)

    report = restore_latest_trusted_market_scan(
        client=client,
        workflow="02-market-scan.yml",
        ref="main",
        session="afternoon",
        observed_at=datetime(2026, 9, 29, 12, 50, tzinfo=SHANGHAI_TZ),
        target_path=target,
    )

    assert report["trusted"] is False
    assert report["status"] == "degraded"
    assert not target.exists()
    assert client.artifact_calls == 0


@pytest.mark.parametrize(
    "payload",
    [
        _payload(generated_at="2026-09-27T10:43:00+08:00", slot="morning"),
        _payload(generated_at="2026-09-29T19:30:00+08:00", slot="close"),
        _payload(generated_at="2026-09-28T14:43:00+08:00", slot="afternoon"),
    ],
)
def test_stale_wrong_slot_or_wrong_date_artifact_is_not_trusted_for_afternoon(
    tmp_path: Path,
    payload: dict[str, Any],
) -> None:
    target = tmp_path / "latest.json"
    report = restore_latest_trusted_market_scan(
        client=FakeClient(payload=payload),
        workflow="02-market-scan.yml",
        ref="main",
        session="afternoon",
        observed_at=datetime(2026, 9, 29, 12, 50, tzinfo=SHANGHAI_TZ),
        target_path=target,
    )

    assert report["trusted"] is False
    assert report["status"] == "degraded"
    assert not target.exists()
    assert report["rejected"]


def test_wrong_branch_success_run_is_not_trusted(tmp_path: Path) -> None:
    target = tmp_path / "latest.json"
    report = restore_latest_trusted_market_scan(
        client=FakeClient(head_branch="feature/other"),
        workflow="02-market-scan.yml",
        ref="main",
        session="afternoon",
        observed_at=datetime(2026, 9, 29, 12, 50, tzinfo=SHANGHAI_TZ),
        target_path=target,
    )

    assert report["trusted"] is False
    assert not target.exists()


def test_watchdog_and_startup_share_the_same_core_payload_fingerprint() -> None:
    payload = _payload(
        generated_at="2026-09-29T10:43:00+08:00",
        slot="morning",
    )
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    observed = datetime(2026, 9, 29, 10, 55, tzinfo=SHANGHAI_TZ)

    _watchdog_payload, _watchdog_generated, watchdog_fingerprint = (
        validate_watchdog_market_scan(raw, slot="morning", observed_at=observed)
    )
    _startup_payload, _startup_generated, startup_fingerprint = (
        validate_startup_market_scan(raw, session="morning", observed_at=observed)
    )

    assert watchdog_fingerprint == startup_fingerprint == hashlib.sha256(raw).hexdigest()


def test_previous_day_afternoon_is_allowed_only_for_morning_startup() -> None:
    raw = json.dumps(
        _payload(
            generated_at="2026-09-28T14:43:00+08:00",
            slot="afternoon",
        ),
        ensure_ascii=False,
        sort_keys=True,
    ).encode("utf-8")

    validate_startup_market_scan(
        raw,
        session="morning",
        observed_at=datetime(2026, 9, 29, 9, 20, tzinfo=SHANGHAI_TZ),
    )
    with pytest.raises(ValueError, match="afternoon startup"):
        validate_startup_market_scan(
            raw,
            session="afternoon",
            observed_at=datetime(2026, 9, 29, 12, 50, tzinfo=SHANGHAI_TZ),
        )


class CoverageDegradedClient(FakeClient):
    def __init__(self):
        payload = _payload(generated_at="2026-09-29T10:43:00+08:00", slot="morning")
        payload["scheduler"]["trigger_source"] = "watchdog"
        payload.update(
            operational_status="degraded", safe_to_push=True, review_complete=True,
            operational_failures=["hk_connect_snapshot_incomplete"],
            market_operational_status={"A": "healthy", "HK_CONNECT": "degraded"},
            diagnostics={"hk_snapshot_coverage": {
                "expected_count": 2, "covered_count": 1, "missing_codes": ["HK00010"],
            }},
            candidates=[
                {"market": "A", "code": "600802", "action": "conditional_buy",
                 "eligible_for_intraday_review": True, "review_complete": True, "research_status": "actionable"},
                {"market": "HK_CONNECT", "code": "HK09999", "action": "watch", "eligible_for_intraday_review": False},
            ],
        )
        super().__init__(conclusion="failure", payload=payload)
        self.steps = [
            {"name": "执行分层选股（不连接券商）", "conclusion": "success"},
            {"name": "上传固定名状态供其他模拟工作流恢复", "conclusion": "success"},
            {"name": "严格确认买入链路健康", "conclusion": "failure"},
        ]
        self.artifact_created_at = "2026-09-29T02:49:33Z"

    def recent_runs(self, workflow, ref):
        runs = super().recent_runs(workflow, ref)
        runs[0]["run_attempt"] = 2
        return runs

    def jobs_for_run_attempt(self, run_id, attempt):
        assert (run_id, attempt) == (175, 2)
        return [{"name": "scan", "status": "completed", "conclusion": "failure",
                 "started_at": "2026-09-29T02:42:06Z", "completed_at": "2026-09-29T02:50:00Z",
                 "steps": self.steps}]

    def artifacts_for_run(self, run_id):
        items = super().artifacts_for_run(run_id)
        items[0]["created_at"] = self.artifact_created_at
        return items


@pytest.mark.parametrize("defect", [
    None, "hk_buy", "market_mismatch", "review_incomplete", "other_failure",
    "failed_archive_step", "previous_attempt_artifact", "inconsistent_coverage", "other_trigger",
])
def test_coverage_degraded_scan_requires_proven_artifact_and_market_isolation(tmp_path, defect):
    client = CoverageDegradedClient()
    if defect == "hk_buy":
        client.payload["candidates"][1].update(action="conditional_buy", eligible_for_intraday_review=True)
    elif defect == "market_mismatch":
        client.payload["candidates"][1]["market"] = "A"
    elif defect == "review_incomplete":
        client.payload["review_complete"] = False
    elif defect == "other_failure":
        client.payload["operational_failures"].append("a_snapshot_unavailable")
    elif defect == "failed_archive_step":
        client.steps[1]["conclusion"] = "failure"
    elif defect == "previous_attempt_artifact":
        client.artifact_created_at = "2026-09-29T02:41:59Z"
    elif defect == "inconsistent_coverage":
        client.payload["diagnostics"]["hk_snapshot_coverage"]["missing_codes"] = []
    elif defect == "other_trigger":
        client.payload["scheduler"]["trigger_source"] = "manual"

    target = tmp_path / "latest.json"
    sync = run_watchdog(
        slot="morning", now_fn=lambda: datetime(2026, 9, 29, 10, 55, tzinfo=SHANGHAI_TZ),
        sleep_fn=lambda _: None, client=client, workflow="02-market-scan.yml", ref="main",
        session_gate=lambda _: {"should_run": True}, sync_latest_path=target,
        sync_timeout_seconds=0, observe_only=True,
    )
    if defect is None:
        assert sync["status"] == "already_degraded"
        assert sync["sync"]["status"] == "synced_degraded"
        assert classify_watchdog(sync, exit_code=0) == ("degraded", "hk_connect_snapshot_incomplete", True)
        assert json.loads(target.read_text()) == client.payload
        plan = read_candidate_plan_snapshot(
            target, now=datetime(2026, 9, 29, 10, 55, tzinfo=SHANGHAI_TZ),
        )
        assert [item["code"] for item in plan.plans] == ["600802"]
    else:
        assert sync["sync"]["status"] == "failed"
        assert not target.exists()

    restored = restore_latest_trusted_market_scan(
        client=client, workflow="02-market-scan.yml", ref="main", session="afternoon",
        observed_at=datetime(2026, 9, 29, 12, 50, tzinfo=SHANGHAI_TZ), target_path=target,
    )
    assert restored["trusted"] is (defect is None)
    if defect is None:
        assert restored["reason"] == "completed_degraded_artifact_validated"
        assert restored["operational_status"] == "degraded"
        assert json.loads(target.read_text()) == client.payload
    else:
        assert not target.exists()
