"""Shared trust contract for market-scan candidate artifacts.

Both same-session watchdog hot-load and intraday startup restore must validate
candidate plans through this module. A failed run may supply its archived
state only when the final health gate failed on isolated Stock Connect coverage.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import stat
import tarfile
import tempfile
import zipfile
from datetime import datetime, time as datetime_time, timedelta
from pathlib import Path, PurePosixPath
from typing import Any, Mapping
from zoneinfo import ZoneInfo

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")

SCAN_SLOT_WINDOWS = {
    "morning": (datetime_time(10, 20), datetime_time(11, 15)),
    "afternoon": (datetime_time(14, 20), datetime_time(15, 15)),
    "close": (datetime_time(19, 5), datetime_time(21, 0)),
}
HK_COVERAGE_DEGRADED = "hk_connect_snapshot_incomplete"
_SCAN_STEP = "执行分层选股（不连接券商）"
_UPLOAD_STEP = "上传固定名状态供其他模拟工作流恢复"
_STRICT_STEP = "严格确认买入链路健康"


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed.astimezone(SHANGHAI_TZ)


def completed_scan_source(run: Mapping[str, Any]) -> bool:
    return run.get("status") == "completed" and run.get("conclusion") in {"success", "failure"}


def scan_state_artifacts(
    client: Any, run: Mapping[str, Any], *, ref: str,
) -> tuple[list[Mapping[str, Any]], tuple[datetime, datetime] | None]:
    """Limit failed runs to the attempt that uploaded state and failed only the health gate."""
    run_id = int(run.get("id") or 0)
    window = None
    if run.get("conclusion") == "failure":
        attempt = int(run.get("run_attempt") or 0)
        if attempt <= 0 or run.get("head_branch") != ref:
            raise ValueError("unproven failed market-scan run attempt")
        jobs = client.jobs_for_run_attempt(run_id, attempt)
        scans = [job for job in jobs if job.get("name") == "scan"]
        unrelated_failures = [
            job for job in jobs
            if job.get("name") != "scan" and job.get("conclusion") not in {"success", "skipped"}
        ]
        if len(scans) != 1 or unrelated_failures:
            raise ValueError("unproven failed market-scan job")
        job = scans[0]
        steps = job.get("steps") or []
        conclusions = {step["name"]: step.get("conclusion") for step in steps}
        if (
            job.get("status") != "completed" or job.get("conclusion") != "failure"
            or conclusions.get(_SCAN_STEP) != "success"
            or conclusions.get(_UPLOAD_STEP) != "success"
            or [step["name"] for step in steps if step.get("conclusion") not in {"success", "skipped"}] != [_STRICT_STEP]
            or conclusions.get(_STRICT_STEP) != "failure"
        ):
            raise ValueError("market-scan failed outside its final health gate")
        window = (parse_datetime(job["started_at"]), parse_datetime(job["completed_at"]))

    artifacts = [
        item for item in client.artifacts_for_run(run_id)
        if item.get("name") == "market-scan-state" and not item.get("expired")
    ]
    if window is not None:
        artifacts = [
            item for item in artifacts if item.get("created_at")
            and window[0] <= parse_datetime(item["created_at"]) <= window[1]
        ]
    return sorted(artifacts, key=lambda item: str(item.get("created_at") or ""), reverse=True), window


def validate_coverage_degraded_scan(
    payload: Mapping[str, Any], *, generated_at: datetime,
    attempt_window: tuple[datetime, datetime],
) -> None:
    """Confirm that a completed partial snapshot cannot supply HK entries."""
    if not (
        attempt_window[0] <= generated_at <= attempt_window[1]
        and payload["scheduler"].get("trigger_source") in {"schedule", "watchdog"}
        and payload.get("operational_status") == "degraded"
        and payload.get("operational_failures") == [HK_COVERAGE_DEGRADED]
        and payload.get("market_operational_status") == {"A": "healthy", "HK_CONNECT": "degraded"}
        and payload.get("review_complete") is True
        and payload.get("safe_to_push") is True
    ):
        raise ValueError("market-scan is not an isolated HK coverage degradation")
    coverage = (payload.get("diagnostics") or {}).get("hk_snapshot_coverage") or {}
    expected, covered = coverage.get("expected_count"), coverage.get("covered_count")
    missing = coverage.get("missing_codes")
    if not (
        type(expected) is int and type(covered) is int and 0 < covered < expected
        and isinstance(missing, list) and len(missing) == expected - covered
        and all(isinstance(item, str) and re.fullmatch(r"HK\d{5}", item) for item in missing)
        and len(set(missing)) == len(missing)
    ):
        raise ValueError("market-scan HK coverage counters are inconsistent")
    candidates = payload.get("candidates")
    if not isinstance(candidates, list):
        raise ValueError("market-scan candidate list is missing")
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            raise ValueError("invalid candidate in degraded market-scan")
        market, symbol = candidate.get("market"), str(candidate.get("code") or "")
        if market == "A" and re.fullmatch(r"\d{6}", symbol):
            continue
        if not (
            market == "HK_CONNECT" and re.fullmatch(r"HK\d{5}", symbol)
            and candidate.get("action") == "watch"
            and candidate.get("eligible_for_intraday_review") is False
        ):
            raise ValueError("invalid market or actionable HK candidate in degraded market-scan")


def _safe_archive_path(name: str) -> PurePosixPath:
    path = PurePosixPath(str(name or ""))
    if not name or path.is_absolute() or ".." in path.parts:
        raise ValueError(f"unsafe artifact path: {name}")
    return path


def read_market_scan_latest_from_artifact(archive_bytes: bytes) -> bytes:
    """Read only data/market_scan/latest.json from the nested Actions artifact."""

    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as outer:
        tar_info = None
        for item in outer.infolist():
            path = _safe_archive_path(item.filename)
            mode = (item.external_attr >> 16) & 0o170000
            if mode == stat.S_IFLNK:
                raise ValueError(f"artifact zip contains symlink: {item.filename}")
            if path.name == "market-scan-state.tar.gz":
                if tar_info is not None:
                    raise ValueError("artifact contains duplicate market-scan-state archives")
                tar_info = item
        if tar_info is None:
            raise ValueError("artifact is missing market-scan-state.tar.gz")
        nested = outer.read(tar_info)

    latest_member = None
    with tarfile.open(fileobj=io.BytesIO(nested), mode="r:gz") as archive:
        for member in archive.getmembers():
            path = _safe_archive_path(member.name)
            if member.issym() or member.islnk():
                raise ValueError(f"artifact tar contains link: {member.name}")
            if not (member.isfile() or member.isdir()):
                raise ValueError(f"artifact tar contains special file: {member.name}")
            if path.as_posix() == "data/market_scan/latest.json":
                if not member.isfile() or latest_member is not None:
                    raise ValueError("artifact latest.json is missing or duplicated")
                latest_member = member
        if latest_member is None:
            raise ValueError("artifact is missing data/market_scan/latest.json")
        handle = archive.extractfile(latest_member)
        if handle is None:
            raise ValueError("artifact latest.json cannot be read")
        return handle.read()


def _decode_base_payload(raw_latest: bytes) -> tuple[Mapping[str, Any], datetime, str, str]:
    try:
        payload = json.loads(raw_latest.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("market-scan latest.json is invalid JSON") from exc
    if not isinstance(payload, Mapping):
        raise ValueError("market-scan latest.json root must be an object")
    if not (
        payload.get("simulation_only") is True
        and payload.get("auto_order_enabled") is False
        and payload.get("human_confirmation_required") is True
    ):
        raise ValueError("market-scan simulation safety contract is invalid")
    scheduler = payload.get("scheduler")
    if not isinstance(scheduler, Mapping):
        raise ValueError("market-scan scheduler metadata is missing")
    slot = str(scheduler.get("slot") or "")
    if slot not in SCAN_SLOT_WINDOWS:
        raise ValueError("market-scan slot is unsupported")
    generated_at = parse_datetime(str(payload.get("generated_at") or ""))
    fingerprint = hashlib.sha256(raw_latest).hexdigest()
    return payload, generated_at, fingerprint, slot


def validate_watchdog_market_scan(
    raw_latest: bytes,
    *,
    slot: str,
    observed_at: datetime,
) -> tuple[Mapping[str, Any], datetime, str]:
    payload, generated_at, fingerprint, payload_slot = _decode_base_payload(raw_latest)
    if payload_slot != slot:
        raise ValueError("market-scan slot does not match watchdog slot")
    observed = observed_at.astimezone(SHANGHAI_TZ)
    generated_time = generated_at.timetz().replace(tzinfo=None)
    window_start, window_end = SCAN_SLOT_WINDOWS[slot]
    if generated_at.date() != observed.date() or not window_start <= generated_time <= window_end:
        raise ValueError("market-scan generated_at is outside the current slot")
    return payload, generated_at, fingerprint


def validate_startup_market_scan(
    raw_latest: bytes,
    *,
    session: str,
    observed_at: datetime,
    max_age_hours: float = 24.0,
) -> tuple[Mapping[str, Any], datetime, str]:
    """Validate a recent artifact's slot and date for intraday startup.

    Morning may use a valid previous-calendar-day afternoon/close scan or
    a same-day morning scan.  Afternoon may use only same-day morning/afternoon
    scans.  This intentionally fails closed after weekends/long holidays rather
    than treating old candidate plans as fresh enough to enter a new session.
    """

    payload, generated_at, fingerprint, payload_slot = _decode_base_payload(raw_latest)
    observed = observed_at.astimezone(SHANGHAI_TZ)
    age = observed - generated_at
    if age < timedelta(minutes=-5):
        raise ValueError("market-scan generated_at is implausibly in the future")
    if age > timedelta(hours=float(max_age_hours)):
        raise ValueError("market-scan artifact is stale")

    if session == "morning":
        same_day_ok = generated_at.date() == observed.date() and payload_slot == "morning"
        previous_day_ok = (
            generated_at.date() == observed.date() - timedelta(days=1)
            and payload_slot in {"afternoon", "close"}
        )
        if not (same_day_ok or previous_day_ok):
            raise ValueError("market-scan slot/date is not valid for morning startup")
    elif session == "afternoon":
        if not (
            generated_at.date() == observed.date()
            and payload_slot in {"morning", "afternoon"}
        ):
            raise ValueError("market-scan slot/date is not valid for afternoon startup")
    else:
        raise ValueError(f"unsupported intraday startup session: {session}")

    return payload, generated_at, fingerprint


def atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    finally:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
