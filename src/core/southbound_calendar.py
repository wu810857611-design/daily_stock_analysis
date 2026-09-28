"""Audited Southbound Stock Connect trading calendar.

The runtime must not infer HK_CONNECT tradability from XHKG alone. This
module reads a versioned annual calendar snapshot derived from official SSE
and HKEX notices. Missing, malformed or out-of-coverage data fails closed as
unknown so callers can degrade without inventing tradability.
"""

from __future__ import annotations

import json
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Mapping, Optional
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CALENDAR_DIR = PROJECT_ROOT / "data" / "trading_calendars"
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
AFTERNOON_CUTOFF = time(12, 0)


def _parse_date(value: Any) -> Optional[date]:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _calendar_path(day: date, calendar_dir: Path = CALENDAR_DIR) -> Path:
    return calendar_dir / f"southbound_{day.year}.json"


def resolve_southbound_trade_status(
    observed_at: datetime,
    *,
    calendar_dir: Path = CALENDAR_DIR,
) -> dict[str, Any]:
    """Resolve official Southbound service availability for one timestamp.

    status is open|closed|unknown. open only means the official annual
    Southbound schedule itself allows service at this time; the caller must
    still require both mainland and Hong Kong exchange sessions to be open.
    """

    observed = observed_at.astimezone(SHANGHAI_TZ)
    day = observed.date()
    path = _calendar_path(day, calendar_dir)
    base = {
        "status": "unknown",
        "observed_at": observed.isoformat(timespec="seconds"),
        "date": day.isoformat(),
        "calendar_path": str(path),
        "reason": "",
        "source_count": 0,
    }
    if not path.exists():
        return {**base, "reason": "southbound_calendar_missing"}

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return {**base, "reason": "southbound_calendar_unreadable"}

    if not isinstance(payload, Mapping):
        return {**base, "reason": "southbound_calendar_invalid_root"}
    if payload.get("schema_version") != 1:
        return {**base, "reason": "southbound_calendar_schema_unsupported"}
    if str(payload.get("market") or "") != "HK_CONNECT_SOUTHBOUND":
        return {**base, "reason": "southbound_calendar_market_mismatch"}

    coverage_start = _parse_date(payload.get("coverage_start"))
    coverage_end = _parse_date(payload.get("coverage_end"))
    if (
        coverage_start is None
        or coverage_end is None
        or not (coverage_start <= day <= coverage_end)
    ):
        return {**base, "reason": "southbound_calendar_out_of_coverage"}

    sources = payload.get("sources")
    source_count = len(sources) if isinstance(sources, list) else 0
    if source_count < 1:
        return {
            **base,
            "source_count": 0,
            "reason": "southbound_calendar_sources_missing",
        }

    ranges = payload.get("full_day_closed_ranges")
    if not isinstance(ranges, list):
        return {
            **base,
            "source_count": source_count,
            "reason": "southbound_calendar_ranges_invalid",
        }
    for item in ranges:
        if not isinstance(item, Mapping):
            return {
                **base,
                "source_count": source_count,
                "reason": "southbound_calendar_range_invalid",
            }
        start = _parse_date(item.get("start"))
        end = _parse_date(item.get("end"))
        if start is None or end is None or end < start:
            return {
                **base,
                "source_count": source_count,
                "reason": "southbound_calendar_range_invalid",
            }
        if start <= day <= end:
            return {
                **base,
                "status": "closed",
                "source_count": source_count,
                "reason": str(item.get("reason") or "official_full_day_closure"),
            }

    afternoon = payload.get("afternoon_closed_dates")
    if not isinstance(afternoon, list):
        return {
            **base,
            "source_count": source_count,
            "reason": "southbound_calendar_afternoon_invalid",
        }
    for item in afternoon:
        if not isinstance(item, Mapping):
            return {
                **base,
                "source_count": source_count,
                "reason": "southbound_calendar_afternoon_invalid",
            }
        item_day = _parse_date(item.get("date"))
        if item_day is None:
            return {
                **base,
                "source_count": source_count,
                "reason": "southbound_calendar_afternoon_invalid",
            }
        if (
            item_day == day
            and observed.timetz().replace(tzinfo=None) >= AFTERNOON_CUTOFF
        ):
            return {
                **base,
                "status": "closed",
                "source_count": source_count,
                "reason": str(item.get("reason") or "official_afternoon_closure"),
            }

    return {
        **base,
        "status": "open",
        "source_count": source_count,
        "reason": "official_calendar_open",
    }
