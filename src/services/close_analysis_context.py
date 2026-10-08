"""Bounded A/H close-run date context; never infer a date from a retry's clock."""
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

SHANGHAI = ZoneInfo("Asia/Shanghai")
CLOSE_TIME = time(18)
# Both A/H continuous sessions start later; reserve the pre-open period too.
NEXT_DAY_CUTOFF = time(9)
CLOSE_CRON = "0 10 * * 1-5"


def close_reference_time(trade_date: str, *, now: datetime | None = None) -> datetime:
    """Validate a close date and return its frozen postmarket reference clock."""
    target = date.fromisoformat(trade_date)
    observed = now or datetime.now(SHANGHAI)
    if observed.tzinfo is None:
        raise ValueError("close-run clock must include a timezone")
    observed = observed.astimezone(SHANGHAI)
    reference = datetime.combine(target, CLOSE_TIME, SHANGHAI)
    cutoff = datetime.combine(target + timedelta(days=1), NEXT_DAY_CUTOFF, SHANGHAI)
    if target.weekday() >= 5 or not reference <= observed < cutoff:
        raise ValueError("close date is outside its 18:00 to next-day 09:00 window")
    return reference


def close_open_markets(trade_date: str) -> set[str]:
    """Strict calendar check for a dated close run; calendar failure is an error."""
    import exchange_calendars as xcals

    target = date.fromisoformat(trade_date)
    return {market for market, exchange in (("cn", "XSHG"), ("hk", "XHKG"))
            if xcals.get_calendar(exchange).is_session(target)}


def resolve_close_context(*, event: str, schedule: str, created_at: str,
                          now: datetime, budget_minutes: int) -> dict:
    """Use original run creation + known cron, including for rerun attempts.

    GitHub can create a scheduled run after midnight, so its local creation
    date alone is not the close date. Only the immediately preceding bounded
    cron slot is accepted; old runs cannot be revived by rerunning them.
    """
    if now.tzinfo is None or budget_minutes <= 0:
        raise ValueError("invalid close-run clock or budget")
    observed = now.astimezone(SHANGHAI)
    if event not in {"schedule", "workflow_dispatch"}:
        raise ValueError("unsupported close-run event")
    if event == "schedule" and schedule != CLOSE_CRON:
        raise ValueError("unrecognised close schedule")
    created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    if created.tzinfo is None or created > observed:
        raise ValueError("invalid original run creation timestamp")
    anchor = created.astimezone(SHANGHAI)
    target = anchor.date()
    if anchor.time() < CLOSE_TIME:
        target -= timedelta(days=1)
    # No weekend/holiday rolling: a different close session is not this run.
    reference = close_reference_time(target.isoformat(), now=anchor)
    close_reference_time(target.isoformat(), now=observed)
    cutoff = datetime.combine(target + timedelta(days=1), NEXT_DAY_CUTOFF, SHANGHAI)
    if observed + timedelta(minutes=budget_minutes) > cutoff:
        raise ValueError("insufficient close-run budget before next-day 09:00")
    return {"trade_date": target.isoformat(), "reference_time": reference.isoformat(),
            "observed_at": observed.isoformat(), "original_created_at": created_at,
            "cutoff_at": cutoff.isoformat(), "cutoff_epoch": int(cutoff.timestamp()),
            "open_markets": sorted(close_open_markets(target.isoformat()))}
