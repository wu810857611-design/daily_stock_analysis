#!/usr/bin/env python3
"""Ensure each scheduled intraday session exists without creating late signals."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, MutableMapping, Sequence
from urllib import error, parse, request

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.intraday_slot_guard import (  # noqa: E402
    SESSION_WINDOWS,
    SHANGHAI_TZ,
    atomic_write_json,
    parse_now,
)
from scripts.market_scan_calendar import evaluate_market_sessions  # noqa: E402
from scripts.pushplus_notify import PushPlusError, send_markdown  # noqa: E402

SUPERVISOR_SCHEDULE_TO_SESSION = {
    "28 1 * * 1-5": "morning",
    "16 2 * * 1-5": "morning",
    "58 4 * * 1-5": "afternoon",
    "46 5 * * 1-5": "afternoon",
}
ACTIVE_RUN_STATUSES = {"queued", "in_progress", "waiting", "pending", "requested"}
FAILED_CONCLUSIONS = {"failure", "cancelled", "timed_out", "action_required"}


def resolve_supervisor_session(*, event_name: str, event_schedule: str, requested_session: str, now: datetime) -> str:
    if event_name == "schedule":
        try:
            return SUPERVISOR_SCHEDULE_TO_SESSION[event_schedule]
        except KeyError as exc:
            raise ValueError(f"unsupported supervisor schedule: {event_schedule}") from exc
    requested = str(requested_session or "auto").strip().lower()
    if requested in SESSION_WINDOWS:
        return requested
    if requested == "auto":
        return "morning" if now.hour < 12 else "afternoon"
    raise ValueError(f"unsupported supervisor session: {requested}")


def _parse_run_time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed.astimezone(SHANGHAI_TZ)


def _effective_run_start(run: Mapping[str, Any]) -> datetime | None:
    """Return the real runner start, with a legacy fallback for old fixtures/runs."""

    started = _parse_run_time(run.get("run_started_at"))
    if started is not None or "run_started_at" in run:
        return started
    status = str(run.get("status") or "")
    if status in {"in_progress", "completed"}:
        return _parse_run_time(run.get("created_at"))
    return None


def _title_marker(title: str, key: str) -> str:
    match = re.search(rf"(?:^|\|)\s*{re.escape(key)}=([^|]+)", title)
    return match.group(1).strip().lower() if match else ""


def run_matches_session(run: Mapping[str, Any], *, trade_date: str, session: str) -> bool:
    created = _parse_run_time(run.get("created_at"))
    if created is None:
        return False
    title = str(run.get("display_title") or run.get("run_name") or "").lower()
    marked_session = _title_marker(title, "session")
    marked_date = _title_marker(title, "date")
    if marked_session:
        if marked_session != session:
            return False
        return marked_date in {"", "auto", trade_date} and (
            marked_date == trade_date or created.date().isoformat() == trade_date
        )

    if created.date().isoformat() != trade_date:
        return False
    local_time = created.timetz().replace(tzinfo=None)
    window = SESSION_WINDOWS[session]
    return window["search_start"] <= local_time <= window["latest"]


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

    def _call(self, method: str, url: str, payload: Mapping[str, Any] | None = None) -> tuple[int, bytes]:
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "daily-stock-analysis/intraday-supervisor",
        }
        outgoing = request.Request(
            url,
            data=(json.dumps(payload, separators=(",", ":")).encode("utf-8") if payload is not None else None),
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
        query = parse.urlencode({"branch": ref, "per_page": 100})
        workflow_path = parse.quote(workflow, safe="")
        url = f"https://api.github.com/repos/{self.repo}/actions/workflows/" f"{workflow_path}/runs?{query}"
        status, body = self._call("GET", url)
        if status != 200:
            raise RuntimeError(f"GitHub runs query returned {status}")
        payload = json.loads(body.decode("utf-8"))
        return list(payload.get("workflow_runs") or [])

    def dispatch(self, workflow: str, ref: str, *, session: str, trade_date: str) -> None:
        workflow_path = parse.quote(workflow, safe="")
        url = f"https://api.github.com/repos/{self.repo}/actions/workflows/" f"{workflow_path}/dispatches"
        status, _body = self._call(
            "POST",
            url,
            {
                "ref": ref,
                "inputs": {
                    "session": session,
                    "trade_date": trade_date,
                    "trigger_source": "supervisor",
                    "max_cycles": "0",
                },
            },
        )
        if status not in {201, 204}:
            raise RuntimeError(f"GitHub workflow dispatch returned {status}")


def _base_result(*, now: datetime, session: str, calendar: Mapping[str, Any]) -> dict[str, Any]:
    trade_date = now.date().isoformat()
    window = SESSION_WINDOWS[session]
    return {
        "schema_version": 1,
        "observed_at": now.isoformat(timespec="seconds"),
        "trade_date": trade_date,
        "session": session,
        "session_key": f"{trade_date}:{session}",
        "calendar_status": str(calendar.get("status") or "unknown"),
        "calendar_degraded": bool(calendar.get("calendar_degraded")),
        "active_markets": list(calendar.get("active_markets") or []),
        "safe_window_end": datetime.combine(now.date(), window["latest"], tzinfo=SHANGHAI_TZ).isoformat(
            timespec="seconds"
        ),
        "status": "unknown",
        "dispatch_attempted": False,
        "dispatch_succeeded": False,
        "alert_required": False,
        "alert_code": "",
        "error": "",
    }


def _fault(result: dict[str, Any], code: str, message: str) -> dict[str, Any]:
    result.update(
        {
            "status": code,
            "alert_required": True,
            "alert_code": code,
            "error": str(message or "")[:1000],
        }
    )
    return result


def evaluate_supervisor(
    *,
    session: str,
    now: datetime,
    client: Any,
    workflow: str,
    ref: str,
    state: MutableMapping[str, Any],
    session_gate: Callable[[datetime], Mapping[str, Any]] = evaluate_market_sessions,
) -> dict[str, Any]:
    observed = now.astimezone(SHANGHAI_TZ)
    calendar = dict(session_gate(observed))
    result = _base_result(now=observed, session=session, calendar=calendar)
    if not calendar.get("should_run"):
        result["status"] = "market_closed"
        return result

    try:
        runs = client.recent_runs(workflow, ref)
    except Exception as exc:  # noqa: BLE001 - converted into a SYSTEM alert.
        return _fault(
            result,
            "github_api_failed",
            f"{type(exc).__name__}: {exc}",
        )

    matching = [item for item in runs if run_matches_session(item, trade_date=result["trade_date"], session=session)]
    matching.sort(key=lambda item: str(item.get("created_at") or ""))
    result["matching_run_count"] = len(matching)
    result["matching_runs"] = [
        {
            "id": item.get("id"),
            "status": item.get("status"),
            "conclusion": item.get("conclusion"),
            "created_at": item.get("created_at"),
            "run_started_at": item.get("run_started_at"),
            "html_url": item.get("html_url"),
        }
        for item in matching[:10]
    ]
    if matching:
        window = SESSION_WINDOWS[session]
        window_start = datetime.combine(observed.date(), window["earliest"], tzinfo=SHANGHAI_TZ)
        window_end = datetime.combine(observed.date(), window["latest"], tzinfo=SHANGHAI_TZ)
        started_candidates = []
        ignored_early = []
        for item in matching:
            started_at = _effective_run_start(item)
            if started_at is None:
                continue
            if started_at < window_start:
                ignored_early.append(item)
                continue
            started_candidates.append((started_at, item))
        result["ignored_early_run_count"] = len(ignored_early)

        if started_candidates:
            started_candidates.sort(key=lambda pair: (pair[0], str(pair[1].get("created_at") or "")))
            primary = started_candidates[0][1]
        else:
            queued_candidates = [
                item
                for item in matching
                if str(item.get("status") or "") in ACTIVE_RUN_STATUSES and not item.get("conclusion")
            ]
            primary = queued_candidates[0] if queued_candidates else None

        if primary is None:
            if ignored_early:
                result["ignored_early_run_ids"] = [item.get("id") for item in ignored_early[:10]]
            matching = []
        else:
            status = str(primary.get("status") or "")
            conclusion = str(primary.get("conclusion") or "")
            created = _parse_run_time(primary.get("created_at"))
            started = _effective_run_start(primary)
            result["covered_run_id"] = primary.get("id")
            result["covered_run_status"] = status
            result["covered_run_conclusion"] = conclusion
            result["covered_run_created_at"] = created.isoformat(timespec="seconds") if created else ""
            result["covered_run_started_at"] = started.isoformat(timespec="seconds") if started else ""
            if conclusion in FAILED_CONCLUSIONS:
                return _fault(
                    result,
                    "session_run_failed",
                    f"01 run {primary.get('id')} concluded {conclusion}",
                )
            if created is not None and created > window_end:
                return _fault(
                    result,
                    "session_started_too_late",
                    f"01 run {primary.get('id')} was created at {created.isoformat()}",
                )
            if started is not None and started > window_end:
                return _fault(
                    result,
                    "session_started_too_late",
                    f"01 run {primary.get('id')} started at {started.isoformat()}",
                )
            if observed > window_end and started is None:
                return _fault(
                    result,
                    "session_not_started_before_deadline",
                    f"01 run {primary.get('id')} remained {status} after the safe window",
                )
            if status in ACTIVE_RUN_STATUSES or (status == "completed" and conclusion == "success"):
                result["status"] = "already_covered"
                return result
            return _fault(
                result,
                "session_run_unhealthy",
                f"01 run {primary.get('id')} has status={status}, conclusion={conclusion}",
            )

    local_time = observed.timetz().replace(tzinfo=None)
    window = SESSION_WINDOWS[session]
    if local_time < window["supervisor"]:
        result["status"] = "waiting_for_primary"
        return result
    if local_time > window["latest"]:
        return _fault(
            result,
            "session_missing_too_late",
            "No queued, running, or completed 01 run was found before the safe window closed",
        )

    dispatches = state.setdefault("dispatches", {})
    prior = dispatches.get(result["session_key"])
    if isinstance(prior, Mapping):
        requested_at = _parse_run_time(prior.get("requested_at"))
        if requested_at is not None and (observed - requested_at).total_seconds() < 900:
            result["status"] = "dispatch_already_requested"
            result["dispatch_requested_at"] = requested_at.isoformat(timespec="seconds")
            return result

    result["dispatch_attempted"] = True
    try:
        client.dispatch(
            workflow,
            ref,
            session=session,
            trade_date=result["trade_date"],
        )
    except Exception as exc:  # noqa: BLE001 - converted into a SYSTEM alert.
        return _fault(
            result,
            "dispatch_failed",
            f"{type(exc).__name__}: {exc}",
        )
    result["status"] = "dispatched"
    result["dispatch_succeeded"] = True
    dispatches[result["session_key"]] = {
        "status": "requested",
        "requested_at": observed.isoformat(timespec="seconds"),
        "workflow": workflow,
        "ref": ref,
    }
    for key in sorted(dispatches)[:-90]:
        dispatches.pop(key, None)
    return result


def render_system_alert(result: Mapping[str, Any]) -> tuple[str, str]:
    session = str(result.get("session") or "unknown")
    trade_date = str(result.get("trade_date") or "unknown")
    code = str(result.get("alert_code") or result.get("status") or "unknown")
    title = f"SYSTEM：盘中调度故障 - {trade_date} {session}"
    content = "\n".join(
        [
            "# 盘中调度故障",
            "",
            f"- 交易日：{trade_date}",
            f"- 时段：{session}",
            f"- 故障码：`{code}`",
            f"- 检查时间：{result.get('observed_at') or 'unknown'}",
            f"- 诊断：{result.get('error') or 'unknown'}",
            "",
            "超过安全窗口不会补跑交易信号；无法确认幂等时也会停止 dispatch。",
            "行情新鲜度、买入区、止损、风险收益、仓位现金、人工确认和双模型硬门禁均保持有效。",
        ]
    )
    return title, content + "\n"


def _default_notification_sender(*, title: str, content: str) -> bool:
    token = str(os.getenv("PUSHPLUS_TOKEN") or "").strip()
    if not token:
        raise PushPlusError("PUSHPLUS_TOKEN is not configured")
    send_markdown(
        token=token,
        topic=str(os.getenv("PUSHPLUS_TOPIC") or "").strip(),
        title=title,
        content=content,
    )
    return True


def _alert_key(result: Mapping[str, Any]) -> str:
    return f"{result.get('trade_date')}:{result.get('session')}:" f"{result.get('alert_code')}"


def retry_pending_alerts(
    *,
    state: MutableMapping[str, Any],
    now: datetime,
    sender: Callable[..., bool],
    exclude_key: str = "",
) -> tuple[bool, list[dict[str, Any]]]:
    """Retry earlier unsent SYSTEM alerts before handling the current result."""

    alerts = state.setdefault("alerts", {})
    outcomes: list[dict[str, Any]] = []
    all_sent = True
    pending = [
        (key, value)
        for key, value in sorted(alerts.items())
        if key != exclude_key and isinstance(value, MutableMapping) and value.get("status") == "pending"
    ][:5]
    for key, value in pending:
        title = str(value.get("title") or "")
        content = str(value.get("content") or "")
        attempts = int(value.get("attempts") or 0) + 1
        if not title or not content:
            sent = False
            failure = "pending_alert_payload_missing"
        else:
            try:
                sent = bool(sender(title=title, content=content))
                failure = "" if sent else "sender_returned_false"
            except Exception as exc:  # noqa: BLE001 - persisted for later retry.
                sent = False
                failure = f"{type(exc).__name__}: {exc}"
        value.update(
            {
                "status": "sent" if sent else "pending",
                "attempts": attempts,
                "last_attempt_at": now.isoformat(timespec="seconds"),
                "sent_at": now.isoformat(timespec="seconds") if sent else "",
                "last_error": failure[:500],
            }
        )
        outcomes.append(
            {
                "alert_key": key,
                "status": "sent" if sent else "failed",
                "attempts": attempts,
                "error": failure[:500],
            }
        )
        all_sent = all_sent and sent
    return all_sent, outcomes


def deliver_alert_once(
    result: MutableMapping[str, Any],
    *,
    state: MutableMapping[str, Any],
    now: datetime,
    sender: Callable[..., bool],
) -> bool:
    if not result.get("alert_required"):
        result["notification"] = {"status": "not_required"}
        return True
    alert_key = _alert_key(result)
    alerts = state.setdefault("alerts", {})
    previous = alerts.get(alert_key)
    if isinstance(previous, Mapping) and previous.get("status") == "sent":
        result["notification"] = {
            "status": "suppressed_duplicate",
            "alert_key": alert_key,
            "sent_at": previous.get("sent_at"),
        }
        return True
    previous_state = previous if isinstance(previous, Mapping) else {}
    attempts = int(previous_state.get("attempts") or 0) + 1
    title, content = render_system_alert(result)
    try:
        sent = bool(sender(title=title, content=content))
        failure = "" if sent else "sender_returned_false"
    except Exception as exc:  # noqa: BLE001 - persisted for retry.
        sent = False
        failure = f"{type(exc).__name__}: {exc}"
    alerts[alert_key] = {
        "status": "sent" if sent else "pending",
        "attempts": attempts,
        "last_attempt_at": now.isoformat(timespec="seconds"),
        "sent_at": now.isoformat(timespec="seconds") if sent else "",
        "last_error": failure[:500],
        "title": title,
        "content": content,
    }
    for key in sorted(alerts)[:-180]:
        alerts.pop(key, None)
    result["notification"] = {
        "status": "sent" if sent else "failed",
        "alert_key": alert_key,
        "attempts": attempts,
        "error": failure[:500],
    }
    return sent


def load_state(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return {"schema_version": 1, "alerts": {}, "dispatches": {}}
    if not isinstance(payload, Mapping):
        return {"schema_version": 1, "alerts": {}, "dispatches": {}}
    return {
        "schema_version": 1,
        "alerts": dict(payload.get("alerts") or {}),
        "dispatches": dict(payload.get("dispatches") or {}),
    }


def write_outputs(path: str, result: Mapping[str, Any], exit_code: int) -> None:
    if not path:
        return
    outputs = {
        "status": result.get("status") or "unknown",
        "trade_date": result.get("trade_date") or "",
        "session": result.get("session") or "",
        "session_key": result.get("session_key") or "",
        "alert_required": str(bool(result.get("alert_required"))).lower(),
        "alert_code": result.get("alert_code") or "",
        "notification_status": (result.get("notification") or {}).get("status") or "unknown",
        "exit_code": exit_code,
    }
    with Path(path).open("a", encoding="utf-8") as handle:
        for key, value in outputs.items():
            scalar = str(value).replace("\r", " ").replace("\n", " ")[:500]
            handle.write(f"{key}={scalar}\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--ref", default="main")
    parser.add_argument("--workflow", default="01-intraday-session.yml")
    parser.add_argument("--event-name", default="workflow_dispatch")
    parser.add_argument("--event-schedule", default="")
    parser.add_argument("--session", default="auto")
    parser.add_argument("--now", default="")
    parser.add_argument(
        "--state",
        type=Path,
        default=Path("data/scheduler/intraday_supervisor_state.json"),
    )
    parser.add_argument("--report", type=Path, default=Path("reports/intraday_supervisor.json"))
    parser.add_argument("--github-output", default="")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    now = parse_now(args.now)
    session = resolve_supervisor_session(
        event_name=args.event_name,
        event_schedule=args.event_schedule,
        requested_session=args.session,
        now=now,
    )
    token = str(os.getenv("GH_TOKEN") or "").strip()
    state = load_state(args.state)
    if not token:
        calendar = dict(evaluate_market_sessions(now))
        result = _fault(
            _base_result(now=now, session=session, calendar=calendar),
            "github_token_missing",
            "GH_TOKEN is required for supervisor run checks and dispatch",
        )
    else:
        client = GitHubActionsClient(repo=args.repo, token=token)
        result = evaluate_supervisor(
            session=session,
            now=now,
            client=client,
            workflow=args.workflow,
            ref=args.ref,
            state=state,
        )
    current_alert_key = _alert_key(result) if result.get("alert_required") else ""
    pending_ok, pending_outcomes = retry_pending_alerts(
        state=state,
        now=now,
        sender=_default_notification_sender,
        exclude_key=current_alert_key,
    )
    result["pending_notification_retries"] = pending_outcomes
    notification_ok = deliver_alert_once(
        result,
        state=state,
        now=now,
        sender=_default_notification_sender,
    )
    state["updated_at"] = now.isoformat(timespec="seconds")
    atomic_write_json(args.state, state)
    atomic_write_json(args.report, result)
    exit_code = 0
    if result.get("alert_required"):
        exit_code = 1 if notification_ok and pending_ok else 2
    elif not pending_ok:
        exit_code = 2
    write_outputs(args.github_output, result, exit_code)
    print(json.dumps(result, ensure_ascii=False))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
