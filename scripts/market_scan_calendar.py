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

from src.core.southbound_calendar import resolve_southbound_trade_status  # noqa: E402
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


def _southbound_value(value: Any) -> tuple[str, dict[str, Any]]:
    if isinstance(value, Mapping):
        details = dict(value)
        return str(details.get("status") or "unknown").strip().lower(), details
    return str(value or "unknown").strip().lower(), {}


def evaluate_market_sessions(
    now: datetime,
    *,
    phase_resolver: Callable[..., Any] = infer_market_phase,
    southbound_resolver: Callable[..., Any] = resolve_southbound_trade_status,
    markets: Sequence[str] = SCANNED_MARKETS,
) -> dict[str, Any]:
    """Return explicit A-share and HK_CONNECT calendar states.

    The A-share gate retains the historical fail-open behavior for an unknown
    exchange calendar because downstream quote freshness remains a hard gate.
    HK_CONNECT is stricter: XHKG being open is not sufficient.  Southbound must
    also be confirmed open by the audited annual Stock Connect schedule and the
    mainland exchange day must be confirmed open.  Missing Southbound calendar
    data therefore fails closed for HK_CONNECT and is reported as degraded.
    """

    observed = now.astimezone(SHANGHAI_TZ)
    raw_phases = {
        market: _phase_value(phase_resolver(market, current_time=observed))
        for market in markets
    }
    market_states: dict[str, str] = {}
    active_markets: list[str] = []
    southbound_details: dict[str, Any] = {}

    for market in markets:
        phase = raw_phases.get(market, MarketPhase.UNKNOWN.value)
        if market != "hk":
            if phase == MarketPhase.NON_TRADING.value:
                state = "closed"
            elif phase == MarketPhase.UNKNOWN.value:
                state = "unknown"
                active_markets.append(market)
            else:
                state = "open_session_day"
                active_markets.append(market)
            market_states[market] = state
            continue

        southbound_status, southbound_details = _southbound_value(
            southbound_resolver(observed)
        )
        mainland_phase = raw_phases.get("cn", MarketPhase.UNKNOWN.value)
        if southbound_status == "closed":
            state = "closed"
        elif southbound_status != "open":
            state = "calendar_unavailable"
        elif (
            phase == MarketPhase.NON_TRADING.value
            or mainland_phase == MarketPhase.NON_TRADING.value
        ):
            state = "closed"
        elif (
            phase == MarketPhase.UNKNOWN.value
            or mainland_phase == MarketPhase.UNKNOWN.value
        ):
            state = "calendar_unavailable"
        else:
            state = "open_session_day"
            active_markets.append(market)
        market_states[market] = state

    confirmed_closed = bool(market_states) and all(
        state == "closed" for state in market_states.values()
    )
    calendar_degraded = any(
        state in {"unknown", "calendar_unavailable"}
        for state in market_states.values()
    )
    if calendar_degraded:
        status = "calendar_degraded"
    elif confirmed_closed:
        status = "market_closed"
    elif len(active_markets) < len(market_states):
        status = "partial_market_open"
    else:
        status = "open"

    return {
        "schema_version": 1,
        "observed_at": observed.isoformat(timespec="seconds"),
        "session_date": observed.date().isoformat(),
        "status": status,
        "should_run": bool(active_markets),
        "all_markets_closed": confirmed_closed,
        "calendar_degraded": calendar_degraded,
        "active_markets": active_markets,
        "market_states": market_states,
        "raw_exchange_phases": raw_phases,
        "southbound_calendar": southbound_details,
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
