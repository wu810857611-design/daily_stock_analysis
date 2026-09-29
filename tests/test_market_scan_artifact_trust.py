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
