#!/usr/bin/env python3
"""Evaluate A-share and Hong Kong trading sessions for scheduled scanners."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.core.trading_calendar import MarketPhase, infer_market_phase  # noqa: E402


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
SCANNED_MARKETS = ("cn", "hk")


def _parse_now(value: str) -> datetime:
    if not value:
        return datetime.now(SHANGHAI_TZ)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed.astimezone(SHANGHAI_TZ)


def _phase_value(value: Any) -> str:
    if isinstance(value, MarketPhase):
        return value.value
    return str(value or "unknown").strip().lower()


def evaluate_market_sessions(
    now: datetime,
    *,
    phase_resolver: Callable[..., Any] = infer_market_phase,
    markets: Sequence[str] = SCANNED_MARKETS,
) -> dict[str, Any]:
    """Return a fail-open calendar gate with explicit per-market states.

    A confirmed ``non_trading`` phase closes only that market.  Calendar
    ``unknown`` remains active so a calendar outage cannot silently drop a real
    trading session; downstream quote freshness and safety gates still decide
    whether any candidate is actionable.
    """

    observed = now.astimezone(SHANGHAI_TZ)
    market_states: dict[str, str] = {}
    active_markets: list[str] = []
    for market in markets:
        phase = _phase_value(phase_resolver(market, current_time=observed))
        if phase == MarketPhase.NON_TRADING.value:
            state = "closed"
        elif phase == MarketPhase.UNKNOWN.value:
            state = "unknown"
            active_markets.append(market)
        else:
            state = "open_session_day"
            active_markets.append(market)
        market_states[market] = state

    all_closed = bool(market_states) and not active_markets
    calendar_degraded = any(state == "unknown" for state in market_states.values())
    if all_closed:
        status = "market_closed"
    elif calendar_degraded:
        status = "calendar_degraded"
    elif len(active_markets) < len(market_states):
        status = "partial_market_open"
    else:
        status = "open"
    return {
        "schema_version": 1,
        "observed_at": observed.isoformat(timespec="seconds"),
        "session_date": observed.date().isoformat(),
        "status": status,
        "should_run": not all_closed,
        "all_markets_closed": all_closed,
        "calendar_degraded": calendar_degraded,
        "active_markets": active_markets,
        "market_states": market_states,
    }


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        Path(temporary_name).replace(path)
    finally:
        try:
            Path(temporary_name).unlink()
        except FileNotFoundError:
            pass


def _write_outputs(path: str, result: Mapping[str, Any]) -> None:
    if not path:
        return
    outputs = {
        "should_run": str(bool(result.get("should_run"))).lower(),
        "calendar_status": str(result.get("status") or "unknown"),
        "calendar_degraded": str(bool(result.get("calendar_degraded"))).lower(),
        "active_markets": ",".join(result.get("active_markets") or []),
    }
    with Path(path).open("a", encoding="utf-8") as handle:
        for key, value in outputs.items():
            handle.write(f"{key}={value[:500]}\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--now", default="")
    parser.add_argument("--github-output", default="")
    parser.add_argument(
        "--report", type=Path, default=Path("reports/market_session_gate.json")
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = evaluate_market_sessions(_parse_now(args.now))
    _atomic_write_json(args.report, result)
    _write_outputs(args.github_output, result)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
