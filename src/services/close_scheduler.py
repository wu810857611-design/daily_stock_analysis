"""Close-slot admission and bounded missing-run supervision.

The caller must serialize all close invocations with the existing workflow
concurrency group. API uncertainty fails closed; failed attempts are not rerun.
"""
from datetime import date, datetime, time, timedelta
import re

from src.services.close_analysis_context import CLOSE_TIME, NEXT_DAY_CUTOFF, SHANGHAI, close_open_markets

RESCUE_END = time(19, 30)
START_GRACE = timedelta(minutes=20)
RUN_GRACE = timedelta(minutes=105)
ACTIVE = {"queued", "pending", "requested", "waiting", "in_progress"}


def run_time(value):
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(SHANGHAI) if parsed.tzinfo else None


def close_runs(runs, trade_date):
    target = date.fromisoformat(trade_date)
    start = datetime.combine(target, CLOSE_TIME, SHANGHAI)
    end = datetime.combine(target + timedelta(days=1), NEXT_DAY_CUTOFF, SHANGHAI)
    if len(runs) >= 100 and all((run_time(r.get("created_at")) or end) >= start for r in runs):
        raise ValueError("close run history is truncated; cannot establish an earlier owner")
    matching = []
    for run in runs:
        created = run_time(run.get("created_at"))
        if created is None or not start <= created < end:
            continue
        if run.get("event") not in {"schedule", "workflow_dispatch"}:
            continue
        title = str(run.get("display_title") or run.get("run_name") or "")
        mode = re.search(r"(?:^|\|)\s*mode=([^|]+)", title)
        marked = re.search(r"(?:^|\|)\s*date=([^|]+)", title)
        if mode and mode.group(1).strip() not in {"full", "stocks-only"}:
            continue
        if marked and marked.group(1).strip() not in {"", "auto", trade_date}:
            continue
        # A cancellation before a runner starts has not claimed an analysis.
        if run.get("conclusion") == "cancelled" and not run.get("run_started_at"):
            continue
        matching.append(run)
    return sorted(matching, key=lambda r: (str(r.get("created_at") or ""), int(r["id"])))


def admit_close_run(*, context, current, runs):
    result = {"schema_version": 1, "trade_date": context["trade_date"],
              "current_run_id": current["id"], "should_run": False, "status": "unknown"}
    if int(current.get("run_attempt") or 1) != 1:
        return {**result, "status": "rerun_blocked"}
    if not context["open_markets"]:
        return {**result, "status": "market_closed"}
    # Execution order matters: an older-created native run may stay queued
    # while an external invocation actually completes first.
    prior = [r for r in close_runs(runs, context["trade_date"])
             if int(r["id"]) != int(current["id"])
             and (r.get("status") == "completed" or r.get("status") == "in_progress")]
    prior.sort(key=lambda r: (str(r.get("run_started_at") or r.get("created_at") or ""), int(r["id"])))
    if prior:
        return {**result, "status": "duplicate_close", "owner_run_id": prior[0]["id"],
                "owner_status": prior[0].get("status"), "owner_conclusion": prior[0].get("conclusion")}
    return {**result, "status": "admitted", "should_run": True}


def evaluate_close_supervisor(*, now, client, ref, state, trade_date="", calendar=close_open_markets):
    observed = now.astimezone(SHANGHAI)
    target = date.fromisoformat(trade_date) if trade_date else observed.date()
    if not trade_date and observed.time() < NEXT_DAY_CUTOFF:
        target -= timedelta(days=1)
    start = datetime.combine(target, CLOSE_TIME, SHANGHAI)
    end = datetime.combine(target, RESCUE_END, SHANGHAI)
    result = {"schema_version": 1, "observed_at": observed.isoformat(timespec="seconds"),
              "trade_date": target.isoformat(), "session": "close", "session_key": f"{target}:close",
              "safe_window_end": end.isoformat(), "status": "unknown", "alert_required": False,
              "alert_code": "", "error": "", "dispatch_attempted": False, "dispatch_succeeded": False}

    def fault(code, message):
        return {**result, "status": code, "alert_required": True, "alert_code": code, "error": str(message)[:1000]}

    if target > observed.date() or observed < start:
        return {**result, "status": "before_close"}
    try:
        markets = sorted(calendar(target.isoformat()))
    except Exception as exc:
        return fault("calendar_failed", f"{type(exc).__name__}: {exc}")
    result["active_markets"] = markets
    if not markets:
        return {**result, "status": "market_closed"}
    try:
        matching = close_runs(client.recent_runs("00-daily-analysis.yml", ref), target.isoformat())
    except Exception as exc:
        return fault("github_api_failed", f"{type(exc).__name__}: {exc}")
    result["matching_runs"] = [{k: r.get(k) for k in ("id", "status", "conclusion", "created_at", "html_url")} for r in matching]
    if matching:
        started = [r for r in matching if r.get("status") in {"completed", "in_progress"}]
        owner = min(started, key=lambda r: (str(r.get("run_started_at") or r.get("created_at") or ""), int(r["id"]))) if started else matching[0]
        result["covered_run_id"] = owner["id"]
        if owner.get("status") == "completed":
            if owner.get("conclusion") == "success":
                return {**result, "status": "already_completed"}
            return fault("close_run_failed", f"Close run {owner['id']} ended {owner.get('conclusion')}; no automatic rerun")
        if owner.get("status") not in ACTIVE:
            return fault("close_run_unknown", f"Unknown run status: {owner.get('status')}")
        begun = run_time(owner.get("run_started_at")) if owner.get("status") == "in_progress" else None
        created = run_time(owner.get("created_at"))
        if begun and observed - begun > RUN_GRACE:
            return fault("close_run_overdue", f"Close run {owner['id']} has exceeded its completion window")
        if not begun and observed - created > START_GRACE:
            return fault("close_run_not_started", f"Close run {owner['id']} exists but has not started")
        return {**result, "status": "already_running" if begun else "already_requested"}
    if observed > end:
        return fault("close_run_missing_late", "Close run missing after 19:30; alert only, no late dispatch")
    dispatches = state.setdefault("dispatches", {})
    if result["session_key"] in dispatches:
        return fault("close_dispatch_unconfirmed", "An earlier dispatch was attempted but no run is visible; do not dispatch twice")
    result["dispatch_attempted"] = True
    dispatches[result["session_key"]] = {"requested_at": observed.isoformat(timespec="seconds"), "status": "attempted"}
    try:
        client.dispatch("00-daily-analysis.yml", ref, session="close", trade_date=target.isoformat())
    except Exception as exc:
        return fault("dispatch_failed", f"{type(exc).__name__}: {exc}")
    return {**result, "status": "dispatched", "dispatch_succeeded": True,
            "alert_required": True, "alert_code": "close_primary_missing_rescued",
            "error": "Primary 18:00 close trigger was missing; one bounded replacement was requested; completion is not yet verified"}
