#!/usr/bin/env python3
"""Dispatch a missing market-scan slot from an already-running workflow."""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, time as datetime_time
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from urllib import error, parse, request
from zoneinfo import ZoneInfo


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


def run_watchdog(
    *,
    slot: str,
    now_fn: Callable[[], datetime],
    sleep_fn: Callable[[float], None],
    client: GitHubActionsClient,
    workflow: str,
    ref: str,
) -> dict[str, Any]:
    now = now_fn().astimezone(SHANGHAI_TZ)
    resolved_slot = resolve_slot(slot, now)
    target, latest = target_at(resolved_slot, now)
    while now < target:
        sleep_fn(min(60.0, (target - now).total_seconds()))
        now = now_fn().astimezone(SHANGHAI_TZ)
    if now > latest:
        return {
            "status": "skipped_late",
            "slot": resolved_slot,
            "observed_at": now.isoformat(timespec="seconds"),
        }
    runs = client.recent_runs(workflow, ref)
    if existing_run_covers_slot(runs, slot=resolved_slot, session_date=now.date()):
        return {
            "status": "already_covered",
            "slot": resolved_slot,
            "observed_at": now.isoformat(timespec="seconds"),
        }
    client.dispatch(workflow, ref, resolved_slot)
    return {
        "status": "dispatched",
        "slot": resolved_slot,
        "observed_at": now.isoformat(timespec="seconds"),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slot", default="auto")
    parser.add_argument("--repo", required=True)
    parser.add_argument("--ref", default="main")
    parser.add_argument("--workflow", default="02-market-scan.yml")
    parser.add_argument("--output", type=Path)
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
    )
    serialised = json.dumps(result, ensure_ascii=False)
    print(serialised)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(serialised + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
