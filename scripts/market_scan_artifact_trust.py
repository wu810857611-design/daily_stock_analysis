"""Shared trust contract for market-scan candidate artifacts.

Both same-session watchdog hot-load and intraday startup restore must validate
candidate plans through this module.  A file merely existing in cache/artifacts
is never proof that the producing scan succeeded or that the payload belongs to
an acceptable slot/date.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
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


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed.astimezone(SHANGHAI_TZ)


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
    """Validate a recent successful artifact for intraday startup.

    Morning may use a successful previous-calendar-day afternoon/close scan or
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
