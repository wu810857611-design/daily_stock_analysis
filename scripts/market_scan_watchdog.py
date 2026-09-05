#!/usr/bin/env python3
"""Dispatch a missing market-scan slot from an already-running workflow."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import stat
import sys
import tarfile
import tempfile
import time
import zipfile
from datetime import datetime, time as datetime_time
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping, Sequence
from urllib import error, parse, request
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.market_scan_calendar import evaluate_market_sessions  # noqa: E402


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
SLOT_TIMES = {
    "morning": (datetime_time(10, 42), datetime_time(11, 15)),
    "afternoon": (datetime_time(14, 42), datetime_time(15, 15)),
    "close": (datetime_time(19, 27), datetime_time(21, 0)),
}
SLOT_SEARCH_START = {
    "morning": datetime_time(10, 20),
    "afternoon": datetime_time(14, 20),
    "close": datetime_time(19, 5),
}


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed.astimezone(SHANGHAI_TZ)


def resolve_slot(requested: str, now: datetime) -> str:
    slot = str(requested or "auto").strip().lower()
    if slot in SLOT_TIMES:
        return slot
    if slot != "auto":
        raise ValueError(f"unsupported watchdog slot: {slot}")
    return "morning" if now.hour < 12 else "afternoon"


def target_at(slot: str, now: datetime) -> tuple[datetime, datetime]:
    target_time, latest_time = SLOT_TIMES[slot]
    return (
        datetime.combine(now.date(), target_time, tzinfo=SHANGHAI_TZ),
        datetime.combine(now.date(), latest_time, tzinfo=SHANGHAI_TZ),
    )


def existing_run_covers_slot(
    runs: Sequence[Mapping[str, Any]], *, slot: str, session_date: Any
) -> bool:
    earliest = SLOT_SEARCH_START[slot]
    latest = SLOT_TIMES[slot][1]
    for item in runs:
        try:
            created = _parse_datetime(str(item.get("created_at") or ""))
        except ValueError:
            continue
        local_time = created.timetz().replace(tzinfo=None)
        if created.date() != session_date or not earliest <= local_time <= latest:
            continue
        status = str(item.get("status") or "")
        conclusion = str(item.get("conclusion") or "")
        if status in {"queued", "in_progress", "waiting", "pending"}:
            return True
        if conclusion == "success":
            return True
    return False


def _runs_for_slot(
    runs: Sequence[Mapping[str, Any]], *, slot: str, session_date: Any
) -> list[Mapping[str, Any]]:
    earliest = SLOT_SEARCH_START[slot]
    latest = SLOT_TIMES[slot][1]
    matched: list[Mapping[str, Any]] = []
    for item in runs:
        try:
            created = _parse_datetime(str(item.get("created_at") or ""))
        except ValueError:
            continue
        local_time = created.timetz().replace(tzinfo=None)
        if created.date() == session_date and earliest <= local_time <= latest:
            matched.append(item)
    return sorted(
        matched,
        key=lambda item: str(item.get("created_at") or ""),
        reverse=True,
    )


class GitHubActionsClient:
    def __init__(
        self,
        *,
        repo: str,
        token: str,
        opener: Callable[..., Any] = request.urlopen,
    ) -> None:
        self.repo = repo
        self.token = token
        self.opener = opener

    def _call(
        self, method: str, url: str, payload: Mapping[str, Any] | None = None
    ) -> tuple[int, bytes]:
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "daily-stock-analysis/market-scan-watchdog",
        }
        outgoing = request.Request(
            url,
            data=(
                json.dumps(payload, separators=(",", ":")).encode("utf-8")
                if payload is not None
                else None
            ),
            headers=headers,
            method=method,
        )
        try:
            with self.opener(outgoing, timeout=20) as response:
                return int(getattr(response, "status", 200)), response.read()
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")[:500]
            raise RuntimeError(f"GitHub API {exc.code}: {body}") from exc

    def recent_runs(self, workflow: str, ref: str) -> list[Mapping[str, Any]]:
        query = parse.urlencode({"branch": ref, "per_page": 50})
        url = (
            f"https://api.github.com/repos/{self.repo}/actions/workflows/"
            f"{parse.quote(workflow, safe='')}/runs?{query}"
        )
        status, body = self._call("GET", url)
        if status != 200:
            raise RuntimeError(f"GitHub runs query returned {status}")
        payload = json.loads(body.decode("utf-8"))
        return list(payload.get("workflow_runs") or [])

    def dispatch(self, workflow: str, ref: str, slot: str) -> None:
        url = (
            f"https://api.github.com/repos/{self.repo}/actions/workflows/"
            f"{parse.quote(workflow, safe='')}/dispatches"
        )
        status, _body = self._call(
            "POST",
            url,
            {
                "ref": ref,
                "inputs": {"slot": slot, "trigger_source": "watchdog"},
            },
        )
        if status not in {201, 204}:
            raise RuntimeError(f"GitHub workflow dispatch returned {status}")

    def artifacts_for_run(self, run_id: int) -> list[Mapping[str, Any]]:
        url = (
            f"https://api.github.com/repos/{self.repo}/actions/runs/"
            f"{int(run_id)}/artifacts?per_page=100"
        )
        status_code, body = self._call("GET", url)
        if status_code != 200:
            raise RuntimeError(f"GitHub artifacts query returned {status_code}")
        payload = json.loads(body.decode("utf-8"))
        return list(payload.get("artifacts") or [])

    def download_artifact(self, artifact_id: int) -> bytes:
        url = (
            f"https://api.github.com/repos/{self.repo}/actions/artifacts/"
            f"{int(artifact_id)}/zip"
        )
        status_code, body = self._call("GET", url)
        if status_code != 200:
            raise RuntimeError(f"GitHub artifact download returned {status_code}")
        return body


def _safe_archive_path(name: str) -> PurePosixPath:
    path = PurePosixPath(str(name or ""))
    if not name or path.is_absolute() or ".." in path.parts:
        raise ValueError(f"unsafe artifact path: {name}")
    return path


def _read_market_scan_latest_from_artifact(archive_bytes: bytes) -> bytes:
    """Read only data/market_scan/latest.json from the nested artifact."""

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


def _validate_synced_market_scan(
    raw_latest: bytes, *, slot: str, observed_at: datetime
) -> tuple[Mapping[str, Any], datetime, str]:
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
    if not isinstance(scheduler, Mapping) or scheduler.get("slot") != slot:
        raise ValueError("market-scan slot does not match watchdog slot")
    generated_at = _parse_datetime(str(payload.get("generated_at") or ""))
    generated_time = generated_at.timetz().replace(tzinfo=None)
    if (
        generated_at.date() != observed_at.astimezone(SHANGHAI_TZ).date()
        or not SLOT_SEARCH_START[slot] <= generated_time <= SLOT_TIMES[slot][1]
    ):
        raise ValueError("market-scan generated_at is outside the current slot")
    fingerprint = hashlib.sha256(raw_latest).hexdigest()
    return payload, generated_at, fingerprint


def _atomic_write(path: Path, content: bytes) -> None:
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


def _sync_successful_slot_artifact(
    *,
    client: GitHubActionsClient,
    workflow: str,
    ref: str,
    slot: str,
    observed_at: datetime,
    target_path: Path,
    sleep_fn: Callable[[float], None],
    timeout_seconds: float,
    poll_seconds: float,
) -> dict[str, Any]:
    attempts = max(1, int(max(timeout_seconds, 0.0) / max(poll_seconds, 1.0)) + 1)
    last_error = "successful_current_slot_run_not_ready"
    for attempt in range(1, attempts + 1):
        try:
            runs = _runs_for_slot(
                client.recent_runs(workflow, ref),
                slot=slot,
                session_date=observed_at.date(),
            )
            successful_runs = [
                run
                for run in runs
                if str(run.get("status") or "") == "completed"
                and str(run.get("conclusion") or "") == "success"
            ]
            for successful in successful_runs:
                run_id = int(successful.get("id") or 0)
                try:
                    artifacts = client.artifacts_for_run(run_id)
                    state_artifacts = [
                        item
                        for item in sorted(
                            artifacts,
                            key=lambda value: str(value.get("created_at") or ""),
                            reverse=True,
                        )
                        if item.get("name") == "market-scan-state"
                        and not item.get("expired")
                    ]
                    if not state_artifacts:
                        last_error = (
                            f"market_scan_state_artifact_not_ready:run={run_id}"
                        )
                        continue
                    for artifact in state_artifacts:
                        artifact_id = int(artifact.get("id") or 0)
                        try:
                            raw_archive = client.download_artifact(artifact_id)
                            raw_latest = _read_market_scan_latest_from_artifact(
                                raw_archive
                            )
                            (
                                _payload,
                                generated_at,
                                fingerprint,
                            ) = _validate_synced_market_scan(
                                raw_latest,
                                slot=slot,
                                observed_at=observed_at,
                            )
                        except Exception as exc:  # noqa: BLE001
                            last_error = (
                                f"{type(exc).__name__}:{exc}:"
                                f"run={run_id}:artifact={artifact_id}"
                            )
                            continue
                        _atomic_write(target_path, raw_latest)
                        return {
                            "status": "synced",
                            "attempts": attempt,
                            "run_id": run_id,
                            "artifact_id": artifact_id,
                            "generated_at": generated_at.isoformat(
                                timespec="seconds"
                            ),
                            "content_fingerprint": fingerprint,
                            "path": str(target_path),
                            "error": "",
                        }
                except Exception as exc:  # noqa: BLE001
                    last_error = f"{type(exc).__name__}:{exc}:run={run_id}"
        except Exception as exc:  # noqa: BLE001 - retry bounded transient artifact state.
            last_error = f"{type(exc).__name__}:{exc}"
        if attempt < attempts:
            sleep_fn(max(poll_seconds, 1.0))
    return {
        "status": "timeout",
        "attempts": attempts,
        "run_id": None,
        "artifact_id": None,
        "generated_at": "",
        "content_fingerprint": "",
        "path": str(target_path),
        "error": last_error,
    }


def run_watchdog(
    *,
    slot: str,
    now_fn: Callable[[], datetime],
    sleep_fn: Callable[[float], None],
    client: GitHubActionsClient,
    workflow: str,
    ref: str,
    session_gate: Callable[[datetime], Mapping[str, Any]] = evaluate_market_sessions,
    sync_latest_path: Path | None = None,
    sync_timeout_seconds: float = 2400.0,
    sync_poll_seconds: float = 15.0,
) -> dict[str, Any]:
    now = now_fn().astimezone(SHANGHAI_TZ)
    resolved_slot = resolve_slot(slot, now)
    calendar = dict(session_gate(now))
    if not calendar.get("should_run"):
        return {
            "status": "market_closed",
            "slot": resolved_slot,
            "observed_at": now.isoformat(timespec="seconds"),
            "calendar_status": calendar.get("status") or "market_closed",
            "active_markets": list(calendar.get("active_markets") or []),
            "market_states": dict(calendar.get("market_states") or {}),
        }
    target, latest = target_at(resolved_slot, now)
    while now < target:
        sleep_fn(min(60.0, (target - now).total_seconds()))
        now = now_fn().astimezone(SHANGHAI_TZ)
    if now > latest:
        return {
            "status": "skipped_late",
            "slot": resolved_slot,
            "observed_at": now.isoformat(timespec="seconds"),
            "calendar_status": calendar.get("status") or "unknown",
            "active_markets": list(calendar.get("active_markets") or []),
        }
    runs = client.recent_runs(workflow, ref)
    covered = existing_run_covers_slot(
        runs, slot=resolved_slot, session_date=now.date()
    )
    if covered:
        result = {
            "status": "already_covered",
            "slot": resolved_slot,
            "observed_at": now.isoformat(timespec="seconds"),
            "calendar_status": calendar.get("status") or "unknown",
            "active_markets": list(calendar.get("active_markets") or []),
        }
    else:
        client.dispatch(workflow, ref, resolved_slot)
        result = {
            "status": "dispatched",
            "slot": resolved_slot,
            "observed_at": now.isoformat(timespec="seconds"),
            "calendar_status": calendar.get("status") or "unknown",
            "active_markets": list(calendar.get("active_markets") or []),
        }
    if sync_latest_path is not None:
        result["sync"] = _sync_successful_slot_artifact(
            client=client,
            workflow=workflow,
            ref=ref,
            slot=resolved_slot,
            observed_at=now,
            target_path=sync_latest_path,
            sleep_fn=sleep_fn,
            timeout_seconds=sync_timeout_seconds,
            poll_seconds=sync_poll_seconds,
        )
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slot", default="auto")
    parser.add_argument("--repo", required=True)
    parser.add_argument("--ref", default="main")
    parser.add_argument("--workflow", default="02-market-scan.yml")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--sync-latest-path", type=Path)
    parser.add_argument("--sync-timeout-seconds", type=float, default=2400.0)
    parser.add_argument("--sync-poll-seconds", type=float, default=15.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    token = str(os.getenv("GH_TOKEN") or "").strip()
    if not token:
        raise SystemExit("GH_TOKEN is required for market-scan watchdog")
    client = GitHubActionsClient(repo=args.repo, token=token)
    result = run_watchdog(
        slot=args.slot,
        now_fn=lambda: datetime.now(SHANGHAI_TZ),
        sleep_fn=time.sleep,
        client=client,
        workflow=args.workflow,
        ref=args.ref,
        sync_latest_path=args.sync_latest_path,
        sync_timeout_seconds=args.sync_timeout_seconds,
        sync_poll_seconds=args.sync_poll_seconds,
    )
    serialised = json.dumps(result, ensure_ascii=False)
    print(serialised)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(serialised + "\n", encoding="utf-8")
    sync = result.get("sync")
    return 1 if isinstance(sync, Mapping) and sync.get("status") != "synced" else 0


if __name__ == "__main__":
    raise SystemExit(main())
