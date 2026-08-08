#!/usr/bin/env python3
"""Adaptive, simulation-only A/H-share intraday monitoring session.

This module deliberately stays separate from the close-analysis workflow.  It
keeps one GitHub Actions job alive during a market session, fetches basic
quotes in batches, evaluates deterministic risk conditions, and sends only
state-transition alerts.  It never imports a broker or places an order.

Normal sampling is every 60 seconds.  A symbol near a configured threshold
temporarily switches the session to 30-second sampling, subject to the quote
provider's minimum interval.  Repeated quote failures degrade to a slower
cadence instead of terminating the whole session.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sqlite3
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, MutableMapping, Optional, Sequence
from urllib import request
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.intraday_monitor import (  # noqa: E402
    QuoteSnapshot,
    ReferenceLevels,
    RiskAlert,
    evaluate_quote,
    send_pushplus,
)
from scripts.adaptive_signal_policy import (  # noqa: E402
    CONSIDER_ENTRY,
    RISK_EXIT_REVIEW,
    evaluate_adaptive_signal,
    input_from_mapping as adaptive_input_from_mapping,
)
from scripts.account_watchlists import (  # noqa: E402
    PRIMARY_SYMBOLS,
    all_quote_symbols as account_quote_symbols,
    load_private_watch_config,
    watch_contexts_by_symbol,
)
from scripts.level2_adapter import (  # noqa: E402
    LEVEL2_AVAILABLE,
    LEVEL2_PROVIDER_ERROR,
    Level2Assessment,
    Level2DataAdapter,
)
from scripts.normalize_stock_list import canonical_symbol, normalize_stock_list  # noqa: E402
from scripts.shadow_ab_experiment import (  # noqa: E402
    BASELINE_DATE as SHADOW_BASELINE_DATE,
    ExperimentInputError as ShadowExperimentError,
    execute_pending as execute_shadow_pending,
    initial_symbols as shadow_initial_symbols,
    load_or_initialize as load_or_initialize_shadow,
    record_daily_nav as record_shadow_daily_nav,
    record_signal as record_shadow_signal,
    record_status as record_shadow_status,
    render_scorecard as render_shadow_scorecard,
    save_state as save_shadow_state,
    strategy_cash as shadow_strategy_cash,
    strategy_nav as shadow_strategy_nav,
    strategy_quantity as shadow_strategy_quantity,
    update_latest_quotes as update_shadow_latest_quotes,
)


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
STATE_SCHEMA_VERSION = 2
DEFAULT_NORMAL_INTERVAL_SECONDS = 60.0
DEFAULT_FAST_INTERVAL_SECONDS = 30.0
DEFAULT_DEGRADED_INTERVAL_SECONDS = 120.0
DEFAULT_FRESHNESS_SECONDS = 90.0
DEFAULT_MIN_QUOTE_COVERAGE = 0.8
DEFAULT_NEAR_LEVEL_PCT = 0.75
DEFAULT_NEAR_CHANGE_POINTS = 0.5
DEFAULT_FAST_HOLD_SECONDS = 300.0
DEFAULT_COOLDOWN_SECONDS = 900.0
DEFAULT_DETERIORATION_PCT = 1.0
DEFAULT_LOW_COVERAGE_LIMIT = 3
DEFAULT_REFERENCE_SIGNAL_MAX_AGE_DAYS = 7
DEFAULT_CANDIDATE_PLAN_MAX_AGE_DAYS = 1
DEFAULT_EXPECTED_HOLDING_DAYS = 20.0
MAX_EXTRA_CANDIDATE_SYMBOLS = 12
TENCENT_BATCH_ENDPOINT = "https://qt.gtimg.cn/q="
_TENCENT_RECORD = re.compile(r'v_([A-Za-z0-9]+)="([^"]*)"')
_SEVERITY_RANK = {"info": 0, "warning": 1, "high": 2, "critical": 3}
_OPEN_PHASES = {"intraday", "closing_auction"}
_SYSTEM_PUSH_CONDITIONS = {
    "data_quality",
    "reference_data_quality",
    "schedule_late",
}


class SessionError(RuntimeError):
    """Raised for invalid configuration or irrecoverable local state."""


@dataclass(frozen=True)
class RealtimeQuote:
    symbol: str
    name: str = ""
    price: Optional[float] = None
    change_pct: Optional[float] = None
    volume: Optional[float] = None
    provider_timestamp: Optional[str] = None
    fetched_at: Optional[str] = None
    stale_seconds: Optional[float] = None
    is_stale: bool = True
    source: str = ""


@dataclass(frozen=True)
class CycleResult:
    checked_at: str
    active_symbols: List[str]
    quotes: List[RealtimeQuote]
    valid_quote_count: int
    coverage: float
    new_event_count: int
    pending_event_count: int
    notified_event_count: int
    next_interval_seconds: float
    degraded: bool
    level2_assessments: List[Level2Assessment]


@dataclass(frozen=True)
class SessionResult:
    started_at: str
    ended_at: str
    cycles: int
    quote_cycles: int
    events_created: int
    events_notified: int
    final_pending_events: int
    termination_reason: str
    state_path: Path
    report_path: Path


def _finite_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _positive_float(value: Any) -> Optional[float]:
    number = _finite_float(value)
    return number if number is not None and number > 0 else None


def _nonnegative_float(value: Any) -> Optional[float]:
    number = _finite_float(value)
    return number if number is not None and number >= 0 else None


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=SHANGHAI_TZ)
    return value.astimezone(SHANGHAI_TZ).isoformat(timespec="seconds")


def _parse_iso(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed.astimezone(SHANGHAI_TZ)


def _is_user_push_allowed(event: Mapping[str, Any]) -> bool:
    payload = event.get("payload") or {}
    return bool(
        payload.get("kind") == "trade_decision"
        or (
            event.get("symbol") == "SYSTEM"
            and event.get("condition") in _SYSTEM_PUSH_CONDITIONS
        )
    )


def _suppress_legacy_raw_outbox(
    state: MutableMapping[str, Any], *, now: datetime
) -> int:
    """Move pre-upgrade raw alerts out of delivery without deleting audit data."""

    kept = []
    suppressed = []
    for event in state.get("outbox", []):
        if _is_user_push_allowed(event):
            kept.append(event)
        else:
            suppressed.append(
                {
                    **event,
                    "cancelled_at": _iso(now),
                    "cancel_reason": "legacy_raw_event_requires_decision_gate",
                }
            )
    if not suppressed:
        return 0
    state["outbox"] = kept
    state.setdefault("cancelled_events", []).extend(suppressed)
    ledger = state.setdefault("event_ledger", [])
    ledger_ids = {str(item.get("event_id") or "") for item in ledger}
    for event in suppressed:
        if str(event.get("event_id") or "") not in ledger_ids:
            event["decision_result"] = "suppressed_legacy_raw_push"
            ledger.append(event)
    return len(suppressed)


def market_for_symbol(symbol: str) -> str:
    return "hk" if canonical_symbol(symbol).startswith("HK") else "cn"


def _a_share_exchange(code: str) -> str:
    if code.startswith(("4", "8")) or code.startswith("92"):
        return "bj"
    if code.startswith(("5", "6", "9")):
        return "sh"
    return "sz"


def to_tencent_symbol(symbol: str) -> str:
    canonical = canonical_symbol(symbol)
    if canonical.startswith("HK") and canonical[2:].isdigit():
        return f"hk{canonical[2:].zfill(5)}"
    if canonical.isdigit() and len(canonical) == 6:
        return f"{_a_share_exchange(canonical)}{canonical}"
    raise SessionError(f"腾讯批量行情不支持股票代码: {symbol}")


def from_tencent_symbol(provider_symbol: str) -> Optional[str]:
    token = (provider_symbol or "").strip().lower()
    if token.startswith("hk") and token[2:].isdigit():
        return f"HK{token[2:].zfill(5)}"
    if token[:2] in {"sh", "sz", "bj"} and token[2:].isdigit():
        return token[2:].zfill(6)
    return None


def parse_tencent_timestamp(raw: Any) -> Optional[datetime]:
    digits = re.sub(r"\D", "", str(raw or ""))
    if len(digits) < 14:
        return None
    try:
        return datetime.strptime(digits[:14], "%Y%m%d%H%M%S").replace(
            tzinfo=SHANGHAI_TZ
        )
    except ValueError:
        return None


def parse_tencent_batch(
    payload: str,
    requested_symbols: Sequence[str],
    *,
    fetched_at: datetime,
    freshness_seconds: float = DEFAULT_FRESHNESS_SECONDS,
) -> Dict[str, RealtimeQuote]:
    """Parse one Tencent multi-symbol response into basic, freshness-checked quotes."""

    requested = {canonical_symbol(symbol) for symbol in requested_symbols}
    parsed: Dict[str, RealtimeQuote] = {}
    for provider_symbol, raw_fields in _TENCENT_RECORD.findall(payload or ""):
        symbol = from_tencent_symbol(provider_symbol)
        if symbol is None or symbol not in requested:
            continue
        fields = raw_fields.split("~")
        price = _positive_float(fields[3] if len(fields) > 3 else None)
        previous_close = _positive_float(fields[4] if len(fields) > 4 else None)
        volume = _nonnegative_float(fields[6] if len(fields) > 6 else None)
        change_pct = _finite_float(fields[32] if len(fields) > 32 else None)
        if change_pct is None and price is not None and previous_close:
            change_pct = (price - previous_close) / previous_close * 100
        provider_time = parse_tencent_timestamp(fields[30] if len(fields) > 30 else None)
        age_seconds: Optional[float] = None
        fresh = False
        if provider_time is not None:
            age_seconds = (fetched_at.astimezone(SHANGHAI_TZ) - provider_time).total_seconds()
            # Small provider/runner clock skew is tolerated; a quote far in the
            # future is as unsafe as an old quote.
            fresh = -30.0 <= age_seconds <= freshness_seconds
        parsed[symbol] = RealtimeQuote(
            symbol=symbol,
            name=str(fields[1] if len(fields) > 1 else "").strip(),
            price=price,
            change_pct=change_pct,
            volume=volume,
            provider_timestamp=_iso(provider_time) if provider_time else None,
            fetched_at=_iso(fetched_at),
            stale_seconds=max(0.0, age_seconds) if age_seconds is not None else None,
            is_stale=not fresh or price is None,
            source="tencent_batch",
        )

    for symbol in requested:
        parsed.setdefault(
            symbol,
            RealtimeQuote(
                symbol=symbol,
                fetched_at=_iso(fetched_at),
                is_stale=True,
                source="tencent_batch",
            ),
        )
    return parsed


class TencentBatchQuoteFetcher:
    """One HTTP request per chunk, returning only basic quote fields."""

    min_interval_seconds = 30.0

    def __init__(
        self,
        *,
        endpoint: str = TENCENT_BATCH_ENDPOINT,
        timeout_seconds: float = 8.0,
        chunk_size: int = 50,
        opener: Callable[..., Any] = request.urlopen,
        freshness_seconds: float = DEFAULT_FRESHNESS_SECONDS,
    ):
        self.endpoint = endpoint
        self.timeout_seconds = timeout_seconds
        self.chunk_size = max(1, int(chunk_size))
        self.opener = opener
        self.freshness_seconds = freshness_seconds

    def fetch(
        self, symbols: Sequence[str], *, now: Optional[datetime] = None
    ) -> Dict[str, RealtimeQuote]:
        fetched_at = now or datetime.now(SHANGHAI_TZ)
        canonical = list(dict.fromkeys(canonical_symbol(symbol) for symbol in symbols))
        results: Dict[str, RealtimeQuote] = {}
        for offset in range(0, len(canonical), self.chunk_size):
            chunk = canonical[offset : offset + self.chunk_size]
            query = ",".join(to_tencent_symbol(symbol) for symbol in chunk)
            outgoing = request.Request(
                f"{self.endpoint}{query}",
                headers={
                    "Referer": "https://finance.qq.com/",
                    "User-Agent": "daily-stock-analysis/intraday-session",
                },
                method="GET",
            )
            try:
                with self.opener(outgoing, timeout=self.timeout_seconds) as response:
                    body = response.read()
                payload = body.decode("gbk", errors="replace")
                results.update(
                    parse_tencent_batch(
                        payload,
                        chunk,
                        fetched_at=fetched_at,
                        freshness_seconds=self.freshness_seconds,
                    )
                )
            except Exception as exc:
                print(f"腾讯批量行情失败（{','.join(chunk)}）: {exc}", file=sys.stderr)
                for symbol in chunk:
                    results[symbol] = RealtimeQuote(
                        symbol=symbol,
                        fetched_at=_iso(fetched_at),
                        is_stale=True,
                        source="tencent_batch_error",
                    )
        return results


def _empty_state(now: Optional[datetime] = None) -> Dict[str, Any]:
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "symbols": {},
        "outbox": [],
        # Raw market/risk events remain append-only for audit and the shadow
        # experiment even when the user-facing decision gate suppresses Push.
        "event_ledger": [],
        "decision_notifications": {},
        "provider": {
            "consecutive_low_coverage": 0,
            "degraded": False,
            "health_alert_active": False,
            "data_capabilities": {
                "realtime_price": "available_when_fresh",
                "provider_timestamp": "required",
                "volume": "available_when_provider_supplies",
                "intraday_kline": "not_collected_by_this_monitor",
                "level2_order_book": "unavailable",
            },
        },
        "updated_at": _iso(now or datetime.now(SHANGHAI_TZ)),
    }


def load_state_v2(path: Path, *, now: Optional[datetime] = None) -> Dict[str, Any]:
    if not path.exists():
        return _empty_state(now)
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SessionError(f"无法读取盘中会话状态 {path}: {exc}") from exc
    if not isinstance(state, dict):
        raise SessionError(f"盘中会话状态不是 JSON 对象: {path}")
    if state.get("schema_version") == STATE_SCHEMA_VERSION:
        if not isinstance(state.get("symbols"), dict) or not isinstance(
            state.get("outbox"), list
        ):
            raise SessionError(f"盘中会话状态 v2 结构无效: {path}")
        state.setdefault("provider", {})
        state.setdefault("event_ledger", [])
        state.setdefault("decision_notifications", {})
        _suppress_legacy_raw_outbox(
            state, now=now or datetime.now(SHANGHAI_TZ)
        )
        return state
    if state.get("schema_version") == 1:
        # Best-effort migration from the former once-per-day condition list.
        migrated = _empty_state(now)
        for trade_date, symbols in (state.get("conditions_by_date") or {}).items():
            if not isinstance(symbols, Mapping):
                continue
            for symbol, conditions in symbols.items():
                symbol_state = migrated["symbols"].setdefault(
                    canonical_symbol(symbol), {"conditions": {}}
                )
                for condition in conditions if isinstance(conditions, list) else []:
                    symbol_state["conditions"][str(condition)] = {
                        "status": "active",
                        "severity": "warning",
                        "activated_at": f"{trade_date}T00:00:00+08:00",
                        "cleared_at": None,
                        "last_notified_at": f"{trade_date}T00:00:00+08:00",
                        "last_notified_price": None,
                        "last_event_id": None,
                    }
        return migrated
    raise SessionError(f"盘中会话状态版本不受支持: {path}")


def save_state_v2(path: Path, state: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(payload)
        os.replace(temporary_name, path)
    except Exception:
        try:
            os.unlink(temporary_name)
        except OSError:
            pass
        raise


def load_reference_levels_batch(
    database_path: Path,
    symbols: Sequence[str],
    *,
    now: Optional[datetime] = None,
    max_signal_age_days: int = DEFAULT_REFERENCE_SIGNAL_MAX_AGE_DAYS,
) -> Dict[str, ReferenceLevels]:
    """Load all latest reference levels once at session start.

    The existing helper opens SQLite once per symbol.  That is acceptable for a
    one-shot check but unnecessary in a minute loop, so this loader keeps a
    single read-only connection and performs compact per-symbol queries.
    """

    result = {canonical_symbol(symbol): ReferenceLevels() for symbol in symbols}
    if not database_path.exists():
        return result
    try:
        connection = sqlite3.connect(f"file:{database_path}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
    except sqlite3.Error:
        return result

    def columns(table: str) -> set[str]:
        try:
            return {
                str(row[1])
                for row in connection.execute(f'PRAGMA table_info("{table}")').fetchall()
            }
        except sqlite3.Error:
            return set()

    signal_columns = columns("decision_signals")
    history_columns = columns("analysis_history")
    current = now or datetime.now(SHANGHAI_TZ)
    if current.tzinfo is None:
        current = current.replace(tzinfo=SHANGHAI_TZ)
    current_utc = current.astimezone(timezone.utc)
    cutoff_utc = current_utc - timedelta(days=max(0, max_signal_age_days))
    current_text = current_utc.strftime("%Y-%m-%d %H:%M:%S")
    cutoff_text = cutoff_utc.strftime("%Y-%m-%d %H:%M:%S")
    history_cutoff_text = (
        current.astimezone(SHANGHAI_TZ).replace(tzinfo=None)
        - timedelta(days=max(0, max_signal_age_days))
    ).strftime("%Y-%m-%d %H:%M:%S")
    try:
        for symbol in result:
            aliases = {symbol.upper()}
            if symbol.startswith("HK"):
                digits = symbol[2:].zfill(5)
                aliases.update(
                    {
                        digits,
                        f"HK.{digits}",
                        f"{digits}.HK",
                        f"HK{digits}",
                    }
                )
            elif symbol.isdigit() and len(symbol) == 6:
                exchange = _a_share_exchange(symbol).upper()
                aliases.update(
                    {
                        f"{exchange}{symbol}",
                        f"{exchange}.{symbol}",
                        f"{symbol}.{exchange}",
                    }
                )
                if exchange == "SH":
                    aliases.update(
                        {f"SS{symbol}", f"SS.{symbol}", f"{symbol}.SS"}
                    )
            placeholders = ",".join("?" for _ in aliases)
            signal = None
            history = None
            if "stock_code" in signal_columns:
                filters = [f"UPPER(stock_code) IN ({placeholders})"]
                parameters: List[Any] = list(aliases)
                if "source_type" in signal_columns:
                    filters.append("(source_type IS NULL OR source_type = 'analysis')")
                if "status" in signal_columns:
                    filters.append("(status IS NULL OR status = 'active')")
                if "expires_at" in signal_columns:
                    if "created_at" in signal_columns:
                        filters.append(
                            "((expires_at IS NOT NULL "
                            "AND datetime(expires_at) > datetime(?)) "
                            "OR (expires_at IS NULL AND created_at IS NOT NULL "
                            "AND datetime(created_at) >= datetime(?)))"
                        )
                        parameters.extend([current_text, cutoff_text])
                    else:
                        filters.append(
                            "(expires_at IS NOT NULL "
                            "AND datetime(expires_at) > datetime(?))"
                        )
                        parameters.append(current_text)
                elif "created_at" in signal_columns:
                    filters.append(
                        "(created_at IS NOT NULL "
                        "AND datetime(created_at) >= datetime(?))"
                    )
                    parameters.append(cutoff_text)
                else:
                    # An undated analysis signal cannot safely remain a
                    # permanent intraday stop/target reference.
                    filters.append("0 = 1")
                ordering = (
                    "datetime(created_at) DESC, id DESC"
                    if {"created_at", "id"}.issubset(signal_columns)
                    else "rowid DESC"
                )
                try:
                    signal = connection.execute(
                        "SELECT * FROM decision_signals WHERE "
                        + " AND ".join(filters)
                        + f" ORDER BY {ordering} LIMIT 1",
                        parameters,
                    ).fetchone()
                except sqlite3.Error:
                    signal = None
            if "code" in history_columns:
                history_date_column = (
                    "created_at"
                    if "created_at" in history_columns
                    else "trade_date"
                    if "trade_date" in history_columns
                    else ""
                )
                ordering = (
                    "datetime(created_at) DESC, id DESC"
                    if {"created_at", "id"}.issubset(history_columns)
                    else "datetime(trade_date) DESC, id DESC"
                    if {"trade_date", "id"}.issubset(history_columns)
                    else "rowid DESC"
                )
                if history_date_column:
                    try:
                        history = connection.execute(
                            f"SELECT * FROM analysis_history "
                            f"WHERE UPPER(code) IN ({placeholders}) "
                            f"AND {history_date_column} IS NOT NULL "
                            f"AND datetime({history_date_column}) >= datetime(?) "
                            f"ORDER BY {ordering} LIMIT 1",
                            [*aliases, history_cutoff_text],
                        ).fetchone()
                    except sqlite3.Error:
                        history = None

            def value(row: Optional[sqlite3.Row], field: str) -> Any:
                return row[field] if row is not None and field in row.keys() else None

            signal_stop = _positive_float(value(signal, "stop_loss"))
            signal_target = _positive_float(value(signal, "target_price"))
            history_stop = _positive_float(value(history, "stop_loss"))
            history_target = _positive_float(value(history, "take_profit"))
            result[symbol] = ReferenceLevels(
                name=str(value(signal, "stock_name") or value(history, "name") or ""),
                stop_loss=signal_stop if signal_stop is not None else history_stop,
                target_price=(
                    signal_target if signal_target is not None else history_target
                ),
                stop_source="decision_signals" if signal_stop is not None else (
                    "analysis_history" if history_stop is not None else ""
                ),
                target_source="decision_signals" if signal_target is not None else (
                    "analysis_history" if history_target is not None else ""
                ),
            )
    finally:
        connection.close()
    return result


def default_phase_resolver(market: str, now: datetime) -> str:
    try:
        from src.core.trading_calendar import infer_market_phase

        phase = infer_market_phase(market, current_time=now)
        return str(getattr(phase, "value", phase))
    except Exception as exc:
        print(f"{market} 市场阶段判断失败，按关闭处理: {exc}", file=sys.stderr)
        return "unknown"


def active_symbols_at(
    symbols: Sequence[str],
    now: datetime,
    phase_resolver: Callable[[str, datetime], str] = default_phase_resolver,
) -> List[str]:
    phase_cache = market_phases_at(symbols, now, phase_resolver)
    active: List[str] = []
    for symbol in symbols:
        market = market_for_symbol(symbol)
        if phase_cache[market] in _OPEN_PHASES:
            active.append(canonical_symbol(symbol))
    return active


def market_phases_at(
    symbols: Sequence[str],
    now: datetime,
    phase_resolver: Callable[[str, datetime], str] = default_phase_resolver,
) -> Dict[str, str]:
    """Resolve each represented market exactly once for a loop iteration."""

    phases: Dict[str, str] = {}
    for symbol in symbols:
        market = market_for_symbol(symbol)
        if market not in phases:
            phases[market] = str(phase_resolver(market, now))
    return phases


def _condition_severity(alert: RiskAlert, levels: ReferenceLevels) -> str:
    if alert.condition == "sharp_drop" and alert.change_pct is not None:
        return "critical" if alert.change_pct <= -5.0 else "high"
    if (
        alert.condition == "stop_loss"
        and levels.stop_loss
        and alert.price <= levels.stop_loss * 0.98
    ):
        return "critical"
    return alert.severity


def _price_deteriorated(
    condition: str,
    current_price: float,
    last_price: Any,
    deterioration_pct: float,
) -> bool:
    previous = _positive_float(last_price)
    if previous is None:
        return False
    ratio = deterioration_pct / 100.0
    if condition in {"sharp_drop", "stop_loss"}:
        return current_price <= previous * (1 - ratio)
    if condition in {"sharp_rise", "target_reached"}:
        return current_price >= previous * (1 + ratio)
    return False


def _event_id(
    *,
    now: datetime,
    symbol: str,
    condition: str,
    transition: str,
    severity: str,
    price: Optional[float],
) -> str:
    price_bucket = f"{price:.3f}" if price is not None else "none"
    material = (
        f"{now.date().isoformat()}|{symbol}|{condition}|{transition}|"
        f"{severity}|{price_bucket}|{_iso(now)}"
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:20]


def _queue_event(
    state: MutableMapping[str, Any],
    *,
    now: datetime,
    symbol: str,
    name: str,
    condition: str,
    transition: str,
    severity: str,
    price: Optional[float],
    change_pct: Optional[float],
    reference_price: Optional[float],
    message: str,
    volume: Optional[float] = None,
    queue_for_push: bool = True,
    payload: Optional[Mapping[str, Any]] = None,
    decision_result: str = "pending",
) -> Dict[str, Any]:
    event = {
        "event_id": _event_id(
            now=now,
            symbol=symbol,
            condition=condition,
            transition=transition,
            severity=severity,
            price=price,
        ),
        "created_at": _iso(now),
        "symbol": symbol,
        "name": name,
        "condition": condition,
        "transition": transition,
        "severity": severity,
        "price": price,
        "change_pct": change_pct,
        "reference_price": reference_price,
        "volume": volume,
        "message": message,
        "decision_result": decision_result,
        "attempts": 0,
        "next_attempt_at": _iso(now),
    }
    if payload:
        event["payload"] = dict(payload)
    ledger = state.setdefault("event_ledger", [])
    ledger_ids = {str(item.get("event_id")) for item in ledger}
    if event["event_id"] not in ledger_ids:
        ledger.append(event)
    existing_ids = {str(item.get("event_id")) for item in state.setdefault("outbox", [])}
    if queue_for_push and event["event_id"] not in existing_ids:
        state["outbox"].append(event)
    return event


def _cancel_outbox_events(
    state: MutableMapping[str, Any],
    *,
    now: datetime,
    predicate: Callable[[Mapping[str, Any]], bool],
    reason: str,
) -> int:
    """Cancel no-longer-valid pending events while retaining a bounded audit."""

    kept: List[Dict[str, Any]] = []
    cancelled: List[Dict[str, Any]] = []
    for event in state.get("outbox", []):
        if predicate(event):
            cancelled.append(
                {
                    **event,
                    "cancelled_at": _iso(now),
                    "cancel_reason": reason,
                }
            )
        else:
            kept.append(event)
    if cancelled:
        state["outbox"] = kept
        audit = state.setdefault("cancelled_events", [])
        audit.extend(cancelled)
        del audit[:-200]
    return len(cancelled)


def _cancel_events_for_cleared_conditions(
    state: MutableMapping[str, Any], *, now: datetime
) -> int:
    def is_cleared(event: Mapping[str, Any]) -> bool:
        symbol = str(event.get("symbol") or "")
        condition = str(event.get("condition") or "")
        if not symbol or symbol == "SYSTEM" or not condition:
            return False
        condition_state = (
            state.get("symbols", {})
            .get(symbol, {})
            .get("conditions", {})
            .get(condition, {})
        )
        return condition_state.get("status") == "cleared"

    return _cancel_outbox_events(
        state,
        now=now,
        predicate=is_cleared,
        reason="condition_cleared_before_delivery",
    )


def update_condition_state(
    state: MutableMapping[str, Any],
    *,
    now: datetime,
    quote: RealtimeQuote,
    alerts: Sequence[RiskAlert],
    levels: ReferenceLevels,
    cooldown_seconds: float,
    deterioration_pct: float,
    down_threshold_pct: float = 3.0,
    up_threshold_pct: float = 5.0,
    clear_hysteresis_pct: float = 0.5,
) -> int:
    """Persist raw condition transitions without directly pushing price alarms."""

    symbol_state = state.setdefault("symbols", {}).setdefault(
        quote.symbol, {"conditions": {}}
    )
    symbol_state["last_quote"] = asdict(quote)
    conditions = symbol_state.setdefault("conditions", {})
    current_by_condition = {alert.condition: alert for alert in alerts}
    created = 0

    for condition, alert in current_by_condition.items():
        severity = _condition_severity(alert, levels)
        previous = conditions.get(condition) or {}
        transition = ""
        if previous.get("status") != "active":
            transition = "activated"
        elif _SEVERITY_RANK.get(severity, 0) > _SEVERITY_RANK.get(
            str(previous.get("severity") or "info"), 0
        ):
            transition = "severity_up"
        elif _price_deteriorated(
            condition,
            alert.price,
            previous.get("last_event_price") or previous.get("last_notified_price"),
            deterioration_pct,
        ):
            transition = "deteriorated"

        condition_state = {
            **previous,
            "status": "active",
            "severity": severity,
            "activated_at": previous.get("activated_at") or _iso(now),
            "cleared_at": None,
        }
        conditions[condition] = condition_state
        if transition:
            event = _queue_event(
                state,
                now=now,
                symbol=quote.symbol,
                name=quote.name or levels.name,
                condition=condition,
                transition=transition,
                severity=severity,
                price=alert.price,
                change_pct=alert.change_pct,
                reference_price=alert.reference_price,
                message=alert.message,
                volume=quote.volume,
                queue_for_push=False,
                decision_result="awaiting_decision_gate",
                payload={
                    "quote_time": quote.provider_timestamp,
                    "signal_time": _iso(now),
                    "data_quality": "stale" if quote.is_stale else "fresh_l1",
                    "source": quote.source,
                },
            )
            condition_state["last_event_id"] = event["event_id"]
            condition_state["last_event_at"] = _iso(now)
            condition_state["last_event_price"] = alert.price
            created += 1

    # A valid fresh quote may clear a previously active condition.  Missing or
    # stale quotes never clear risk state.
    if quote.price is not None and not quote.is_stale:
        for condition, condition_state in list(conditions.items()):
            if (
                condition not in current_by_condition
                and condition_state.get("status") == "active"
            ):
                may_clear = True
                if condition == "stop_loss" and levels.stop_loss:
                    may_clear = quote.price > levels.stop_loss * (
                        1 + clear_hysteresis_pct / 100
                    )
                elif condition == "target_reached" and levels.target_price:
                    may_clear = quote.price < levels.target_price * (
                        1 - clear_hysteresis_pct / 100
                    )
                elif condition == "sharp_drop":
                    may_clear = (
                        quote.change_pct is not None
                        and quote.change_pct
                        > -(max(0.0, down_threshold_pct - clear_hysteresis_pct))
                    )
                elif condition == "sharp_rise":
                    may_clear = (
                        quote.change_pct is not None
                        and quote.change_pct
                        < max(0.0, up_threshold_pct - clear_hysteresis_pct)
                    )
                if may_clear:
                    condition_state["status"] = "cleared"
                    condition_state["cleared_at"] = _iso(now)
                    condition_state["severity"] = "info"
                    _cancel_outbox_events(
                        state,
                        now=now,
                        predicate=lambda event, symbol=quote.symbol, current=condition: (
                            event.get("symbol") == symbol
                            and event.get("condition") == current
                        ),
                        reason="condition_cleared_before_delivery",
                    )
                    _queue_event(
                        state,
                        now=now,
                        symbol=quote.symbol,
                        name=quote.name or levels.name,
                        condition=condition,
                        transition="cleared",
                        severity="info",
                        price=quote.price,
                        change_pct=quote.change_pct,
                        reference_price=(
                            levels.stop_loss
                            if condition == "stop_loss"
                            else levels.target_price
                            if condition == "target_reached"
                            else None
                        ),
                        volume=quote.volume,
                        message="该底层风险/价格条件已解除。",
                        queue_for_push=False,
                        decision_result="risk_cleared_no_push",
                        payload={
                            "quote_time": quote.provider_timestamp,
                            "signal_time": _iso(now),
                            "data_quality": "fresh_l1",
                            "source": quote.source,
                        },
                    )
                    decision_state = (
                        state.setdefault("decision_notifications", {})
                        .setdefault(quote.symbol, {})
                        .setdefault(condition, {})
                    )
                    decision_state["status"] = "cleared"
                    decision_state["cleared_at"] = _iso(now)
                    created += 1
    return created


def is_near_threshold(
    quote: RealtimeQuote,
    levels: ReferenceLevels,
    *,
    down_threshold_pct: float,
    up_threshold_pct: float,
    near_level_pct: float,
    near_change_points: float,
) -> bool:
    if quote.price is None or quote.is_stale:
        return False
    if levels.stop_loss:
        distance = abs(quote.price - levels.stop_loss) / levels.stop_loss * 100
        if distance <= near_level_pct:
            return True
    if levels.target_price:
        distance = abs(quote.price - levels.target_price) / levels.target_price * 100
        if distance <= near_level_pct:
            return True
    if quote.change_pct is not None:
        if abs(quote.change_pct - up_threshold_pct) <= near_change_points:
            return True
        if abs(quote.change_pct + down_threshold_pct) <= near_change_points:
            return True
    return False


def load_candidate_plans(
    path: Optional[Path], *, now: Optional[datetime] = None
) -> List[Dict[str, Any]]:
    """Read the optional, read-only JSON interface from a scanner/planner.

    The monitor never asks the scanner to trade.  A producer must explicitly
    label every item ``scope=simulation`` or ``scope=watchlist``; unlabeled
    objects are ignored by the policy gate below.
    """

    if path is None or not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"候选计划 JSON 不可用，跳过自适应复核: {exc}", file=sys.stderr)
        return []
    trusted_scan = False
    scan_artifact = isinstance(payload, Mapping)
    if scan_artifact:
        generated_at = _parse_iso(payload.get("generated_at"))
        current = now or datetime.now(SHANGHAI_TZ)
        if current.tzinfo is None:
            current = current.replace(tzinfo=SHANGHAI_TZ)
        current = current.astimezone(SHANGHAI_TZ)
        generated_fresh = (
            generated_at is not None
            and 0
            <= (current - generated_at).total_seconds()
            <= DEFAULT_CANDIDATE_PLAN_MAX_AGE_DAYS * 86400
        )
        trusted_scan = bool(
            payload.get("simulation_only") is True
            and payload.get("auto_order_enabled") is False
            and payload.get("human_confirmation_required") is True
            and payload.get("safe_to_push") is True
            and payload.get("review_complete") is True
            and generated_fresh
        )
        items = payload.get("candidates")
    else:
        items = payload
    # A root scanner artifact is an all-or-nothing trust boundary.  An expired,
    # incomplete, or blocked scan must not regain entry through a candidate's
    # pre-existing ``scope=simulation`` field.  Bare lists remain supported for
    # explicitly supplied local watchlists.
    if scan_artifact and not trusted_scan:
        return []
    if not isinstance(items, list):
        return []
    normalised: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        candidate = dict(item)
        trusted_candidate = bool(
            trusted_scan
            and candidate.get("eligible_for_intraday_review") is True
            and candidate.get("review_complete") is True
            and candidate.get("action") == "conditional_buy"
            and candidate.get("research_status") in {"ready", "actionable", "approved"}
            and candidate.get("hard_risk_veto") is not True
            and candidate.get("model_disagreement") is not True
        )
        if not candidate.get("scope") and trusted_candidate:
            # A scanner artifact satisfying the simulation/human-review
            # contract, plus an explicit per-candidate eligibility gate, is
            # converted into a watchlist scope.  A root-level ``safe_to_push``
            # only means that a research report may be sent; it never upgrades
            # a watch/deep-research candidate into an entry review.
            candidate["scope"] = "watchlist"
            candidate["position_state"] = "flat"
            candidate.setdefault(
                "expected_holding_days", DEFAULT_EXPECTED_HOLDING_DAYS
            )
            plan = candidate.get("plan")
            if isinstance(plan, Mapping):
                round_trip = _nonnegative_float(plan.get("round_trip_cost_bps"))
                if round_trip is not None:
                    candidate.setdefault(
                        "market_costs",
                        {
                            "entry_fee_bps": round_trip / 2,
                            "exit_fee_bps": round_trip / 2,
                        },
                    )
            reviews = [
                candidate.get("qwen_review"),
                candidate.get("deepseek_review"),
            ]
            confidences = [
                _finite_float(review.get("confidence"))
                for review in reviews
                if isinstance(review, Mapping)
            ]
            if confidences and all(value is not None for value in confidences):
                candidate.setdefault("confidence", min(confidences))
            availability = candidate.get("data_availability")
            if isinstance(availability, Mapping):
                candidate.setdefault(
                    "data_quality",
                    (
                        "high"
                        if availability.get("basic_quote") == "available"
                        and availability.get("ohlcv") == "available"
                        else "degraded"
                    ),
                )
        if (
            str(candidate.get("scope") or "").lower()
            in {"simulation", "watchlist"}
            and (
                not scan_artifact
                or trusted_candidate
            )
        ):
            normalised.append(candidate)
    return normalised


def _candidate_plan_payload(
    candidate: Mapping[str, Any],
    quote: RealtimeQuote,
    *,
    confidence_multiplier: float = 1.0,
) -> Optional[Dict[str, Any]]:
    scope = str(candidate.get("scope") or "").strip().lower()
    if scope not in {"simulation", "watchlist"}:
        return None
    try:
        symbol = canonical_symbol(str(candidate.get("symbol") or candidate.get("code") or ""))
    except Exception:
        return None
    if symbol != quote.symbol or quote.price is None or quote.is_stale:
        return None
    provider_time = _parse_iso(quote.provider_timestamp)
    if provider_time is None:
        return None
    fetched_time = _parse_iso(quote.fetched_at)
    if fetched_time is None:
        return None
    age = max(0.0, (fetched_time - provider_time).total_seconds())
    if age is None or age < 0 or age > DEFAULT_FRESHNESS_SECONDS:
        return None

    plan = candidate.get("plan")
    plan = plan if isinstance(plan, Mapping) else candidate
    entry_low = _positive_float(plan.get("entry_low"))
    entry_high = _positive_float(plan.get("entry_high"))
    plan_price = _positive_float(plan.get("plan_price", plan.get("entry_mid")))
    if plan_price is None and entry_low is not None and entry_high is not None:
        plan_price = (entry_low + entry_high) / 2
    raw_confidence = _finite_float(candidate.get("confidence"))
    multiplier = _finite_float(confidence_multiplier)
    if multiplier is None or not 0 <= multiplier <= 1:
        multiplier = 0.0
    adjusted_confidence = (
        min(1.0, max(0.0, raw_confidence * multiplier))
        if raw_confidence is not None
        else None
    )
    payload = {
        "symbol": symbol,
        # Scanner markets use A/HK_CONNECT; the deterministic policy contract
        # uses cn/hk, so canonical symbol identity is the single source of truth.
        "market": market_for_symbol(symbol),
        "plan_price": plan_price,
        "stop_loss": plan.get("stop_loss"),
        "target_price": plan.get("target_price", plan.get("take_profit_1")),
        "confidence": adjusted_confidence,
        "data_quality": candidate.get("data_quality"),
        "market_costs": candidate.get("market_costs"),
        "expected_holding_days": candidate.get("expected_holding_days"),
        "quote_price": quote.price,
        "data_age_seconds": age,
        "position_state": (
            "held"
            if str(candidate.get("holding_state") or "").strip().lower()
            in {"paper_held", "simulation_holding"}
            else str(candidate.get("position_state") or "unknown").strip().lower()
        ),
        "incumbent_annualized_utility": candidate.get(
            "incumbent_annualized_utility", 0.0
        ),
    }
    # Do not call the underlying policy's emergency incomplete-plan path.
    # Every monitor-generated manual-review event must start from a complete
    # plan and explicit market-cost assumptions.
    required = (
        payload["plan_price"],
        _positive_float(payload["stop_loss"]),
        _positive_float(payload["target_price"]),
        _finite_float(payload["confidence"]),
        _positive_float(payload["expected_holding_days"]),
        payload["data_quality"],
    )
    costs = payload["market_costs"]
    if (
        any(value is None or value == "" for value in required)
        or not isinstance(costs, Mapping)
        or _nonnegative_float(costs.get("entry_fee_bps")) is None
        or _nonnegative_float(costs.get("exit_fee_bps")) is None
    ):
        return None
    payload["_scope"] = scope
    payload["_holding_state"] = str(candidate.get("holding_state") or "").strip().lower()
    payload["_name"] = str(candidate.get("name") or quote.name or "")
    payload["_entry_low"] = entry_low
    payload["_entry_high"] = entry_high
    payload["_raw_confidence"] = raw_confidence
    payload["_confidence_multiplier"] = multiplier
    if entry_low is None or entry_high is None or entry_low > entry_high:
        return None
    return payload


def _clear_adaptive_entry_review(
    state: MutableMapping[str, Any],
    *,
    symbol: str,
    now: datetime,
    reason: str,
) -> None:
    symbol_state = state.get("symbols", {}).get(symbol)
    if isinstance(symbol_state, MutableMapping):
        reviews = symbol_state.get("adaptive_reviews")
        if isinstance(reviews, MutableMapping):
            entry_state = reviews.get("adaptive_entry_review")
            if (
                isinstance(entry_state, MutableMapping)
                and entry_state.get("status") == "active"
            ):
                entry_state["status"] = "cleared"
                entry_state["cleared_at"] = _iso(now)
    _cancel_outbox_events(
        state,
        now=now,
        predicate=lambda event: (
            event.get("symbol") == symbol
            and event.get("condition") == "adaptive_entry_review"
        ),
        reason=reason,
    )


def enqueue_adaptive_plan_reviews(
    state: MutableMapping[str, Any],
    *,
    now: datetime,
    quotes: Sequence[RealtimeQuote],
    candidates: Sequence[Mapping[str, Any]],
    confidence_multipliers: Optional[Mapping[str, float]] = None,
) -> int:
    """Queue deduplicated simulation/manual-review events from complete plans."""

    by_symbol = {quote.symbol: quote for quote in quotes}
    created = 0
    for candidate in candidates:
        raw_symbol = str(candidate.get("symbol") or candidate.get("code") or "")
        try:
            symbol = canonical_symbol(raw_symbol)
        except Exception:
            continue
        quote = by_symbol.get(symbol)
        if quote is None:
            _clear_adaptive_entry_review(
                state,
                symbol=symbol,
                now=now,
                reason="candidate_quote_unavailable_before_delivery",
            )
            continue
        payload = _candidate_plan_payload(
            candidate,
            quote,
            confidence_multiplier=(
                confidence_multipliers.get(symbol, 1.0)
                if confidence_multipliers is not None
                else 1.0
            ),
        )
        if payload is None:
            _clear_adaptive_entry_review(
                state,
                symbol=symbol,
                now=now,
                reason="candidate_plan_or_quote_invalid_before_delivery",
            )
            continue
        fingerprint_payload = {
            key: payload.get(key)
            for key in (
                "symbol",
                "market",
                "plan_price",
                "stop_loss",
                "target_price",
                "confidence",
                "expected_holding_days",
                "data_quality",
                "market_costs",
                "_raw_confidence",
                "_confidence_multiplier",
                "_entry_low",
                "_entry_high",
                "_scope",
                "_holding_state",
            )
        }
        fingerprint = hashlib.sha256(
            json.dumps(
                fingerprint_payload, ensure_ascii=False, sort_keys=True
            ).encode("utf-8")
        ).hexdigest()[:20]
        symbol_state = state.setdefault("symbols", {}).setdefault(
            symbol, {"conditions": {}}
        )
        reviews = symbol_state.setdefault("adaptive_reviews", {})
        entry_state = reviews.get("adaptive_entry_review") or {}
        position_state = str(payload.get("position_state") or "unknown")
        if position_state != "flat":
            _clear_adaptive_entry_review(
                state,
                symbol=symbol,
                now=now,
                reason="candidate_no_longer_flat_before_delivery",
            )
        if (
            entry_state.get("status") == "active"
            and entry_state.get("fingerprint") != fingerprint
        ):
            entry_state["status"] = "cleared"
            entry_state["cleared_at"] = _iso(now)
            reviews["adaptive_entry_review"] = entry_state
            _cancel_outbox_events(
                state,
                now=now,
                predicate=lambda event, current_symbol=symbol: (
                    event.get("symbol") == current_symbol
                    and event.get("condition") == "adaptive_entry_review"
                ),
                reason="candidate_plan_changed_before_delivery",
            )
        in_entry_zone = bool(
            position_state == "flat"
            and payload["_entry_low"] <= quote.price <= payload["_entry_high"]
            and quote.price > float(payload["stop_loss"])
            and quote.price < float(payload["target_price"])
        )
        if position_state == "flat" and not in_entry_zone:
            if entry_state.get("status") == "active":
                entry_state["status"] = "cleared"
                entry_state["cleared_at"] = _iso(now)
                reviews["adaptive_entry_review"] = entry_state
                _cancel_outbox_events(
                    state,
                    now=now,
                    predicate=lambda event, current_symbol=symbol: (
                        event.get("symbol") == current_symbol
                        and event.get("condition") == "adaptive_entry_review"
                    ),
                    reason="candidate_left_entry_zone_before_delivery",
                )
            continue
        if (
            position_state == "flat"
            and entry_state.get("status") == "active"
            and entry_state.get("fingerprint") == fingerprint
        ):
            continue

        policy_state = symbol_state.setdefault(
            "adaptive_policy",
            {"incumbent_annualized_utility": 0.0},
        )
        same_plan_rearmed = bool(
            entry_state.get("status") == "cleared"
            and entry_state.get("fingerprint") == fingerprint
        )
        payload["incumbent_annualized_utility"] = (
            0.0
            if same_plan_rearmed
            else _finite_float(policy_state.get("incumbent_annualized_utility"))
            or 0.0
        )
        try:
            decision = evaluate_adaptive_signal(adaptive_input_from_mapping(payload))
        except Exception as exc:
            print(
                f"{symbol} 自适应候选策略评估失败，已跳过且不影响行情监控: {exc}",
                file=sys.stderr,
            )
            continue
        if not decision.eligible_for_manual_review:
            continue
        if decision.candidate_action == CONSIDER_ENTRY:
            if not in_entry_zone:
                continue
            condition = "adaptive_entry_review"
            wording = (
                f"模拟/自选股候选已进入计划观察区 "
                f"{payload['_entry_low']:.3f}–{payload['_entry_high']:.3f}，"
                "并通过税费滑点后的风险收益门槛，仅供人工复核；"
                "这不是买入指令，系统不会下单。"
            )
            if payload["_confidence_multiplier"] < 1:
                wording += (
                    f" 当前缺少可验证的可靠 Level-2，置信度已按 "
                    f"{payload['_confidence_multiplier']:.0%} 折减并使用"
                    "基础行情、技术与研究数据降级评估。"
                )
        elif decision.candidate_action == RISK_EXIT_REVIEW:
            if payload["_holding_state"] not in {"paper_held", "simulation_holding"}:
                # A missing/real-world holding state must never be translated
                # into a sell-like message.
                continue
            condition = "adaptive_risk_review"
            wording = (
                "模拟持仓触及计划风险位，仅供人工复核；这不是卖出指令，"
                "系统不知道真实持仓且不会下单。"
            )
        else:
            continue
        previous_review = reviews.get(condition) or {}
        if (
            previous_review.get("status") == "active"
            and previous_review.get("fingerprint") == fingerprint
        ):
            continue
        event = _queue_event(
            state,
            now=now,
            symbol=symbol,
            name=str(payload["_name"]),
            condition=condition,
            transition="manual_review",
            severity="warning" if condition == "adaptive_entry_review" else "high",
            price=quote.price,
            change_pct=quote.change_pct,
            reference_price=_positive_float(payload["plan_price"]),
            volume=quote.volume,
            message=wording,
            queue_for_push=False,
            decision_result="awaiting_decision_gate",
            payload={
                "signal_time": _iso(now),
                "quote_time": quote.provider_timestamp,
                "data_quality": payload.get("data_quality"),
                "source": quote.source,
                "market": payload.get("market"),
                "entry_low": payload.get("_entry_low"),
                "entry_high": payload.get("_entry_high"),
                "stop_loss": payload.get("stop_loss"),
                "target_price": payload.get("target_price"),
                "confidence": payload.get("confidence"),
                "raw_confidence": payload.get("_raw_confidence"),
                "confidence_multiplier": payload.get("_confidence_multiplier"),
                "market_costs": payload.get("market_costs"),
                "plan_fingerprint": fingerprint,
                "scope": payload.get("_scope"),
            },
        )
        reviews[condition] = {
            "fingerprint": fingerprint,
            "event_id": event["event_id"],
            "reviewed_at": _iso(now),
            "activated_at": _iso(now),
            "cleared_at": None,
            "status": "active",
            "scope": payload["_scope"],
            "simulation_only": True,
        }
        utility = _finite_float(
            getattr(decision, "annualized_after_cost_utility", None)
        )
        if utility is not None and condition == "adaptive_entry_review":
            policy_state.update(
                {
                    "fingerprint": fingerprint,
                    "incumbent_annualized_utility": utility,
                    "updated_at": _iso(now),
                    "simulation_only": True,
                }
            )
        created += 1
    return created


def clear_removed_adaptive_plan_reviews(
    state: MutableMapping[str, Any],
    *,
    now: datetime,
    candidates: Sequence[Mapping[str, Any]],
) -> int:
    """Invalidate queued entry reviews no longer present in the trusted plan set."""

    current_symbols = set()
    for candidate in candidates:
        try:
            current_symbols.add(
                canonical_symbol(
                    str(candidate.get("symbol") or candidate.get("code") or "")
                )
            )
        except Exception:
            continue

    cleared = 0
    for symbol, symbol_state in list(state.get("symbols", {}).items()):
        if symbol in current_symbols or not isinstance(symbol_state, MutableMapping):
            continue
        reviews = symbol_state.get("adaptive_reviews")
        if not isinstance(reviews, MutableMapping):
            continue
        symbol_cleared = False
        for condition, review_state in list(reviews.items()):
            if (
                not str(condition).startswith("adaptive_")
                or not isinstance(review_state, MutableMapping)
                or review_state.get("status") != "active"
            ):
                continue
            review_state["status"] = "cleared"
            review_state["cleared_at"] = _iso(now)
            symbol_cleared = True
            cleared += 1
        if symbol_cleared:
            _cancel_outbox_events(
                state,
                now=now,
                predicate=lambda event, current_symbol=symbol: (
                    event.get("symbol") == current_symbol
                    and str(event.get("condition") or "").startswith("adaptive_")
                ),
                reason="candidate_removed_or_scan_became_untrusted",
            )
    return cleared


_TRADE_ACTION_LABELS = {
    "buy_0_25": "模拟买入0.25成",
    "buy_0_5": "模拟买入0.5成",
    "add_0_25": "模拟加仓0.25成",
    "add_0_5": "模拟加仓0.5成",
    "reduce_1_4": "模拟减仓1/4",
    "reduce_1_3": "模拟减仓1/3",
    "reduce_1_2": "模拟减仓1/2",
    "clear": "模拟清仓",
}


def _shadow_strategy_quantity(
    shadow_state: Optional[Mapping[str, Any]], symbol: str
) -> float:
    if not shadow_state:
        return 0.0
    try:
        return shadow_strategy_quantity(shadow_state, symbol)
    except (KeyError, TypeError, ValueError):
        return 0.0


def _shadow_strategy_cash(
    shadow_state: Optional[Mapping[str, Any]], symbol: str
) -> float:
    if not shadow_state:
        return 0.0
    currency = "HKD" if symbol.startswith("HK") else "CNY"
    try:
        return shadow_strategy_cash(shadow_state, currency)
    except (KeyError, TypeError, ValueError):
        return 0.0


def _affordable_candidate_action(
    shadow_state: Optional[Mapping[str, Any]],
    *,
    symbol: str,
    confidence: float,
    market_costs: Optional[Mapping[str, Any]],
) -> Optional[str]:
    """Choose only a size coverable by same-currency cash after costs."""

    if not shadow_state:
        return None
    cash = _shadow_strategy_cash(shadow_state, symbol)
    try:
        nav = float(shadow_strategy_nav(shadow_state))
    except (KeyError, ShadowExperimentError, TypeError, ValueError):
        return None
    if cash <= 0 or not math.isfinite(nav) or nav <= 0:
        return None
    costs = market_costs or {}
    fee_bps = _nonnegative_float(costs.get("entry_fee_bps")) or 0.0
    slippage_bps = _nonnegative_float(costs.get("entry_slippage_bps")) or 0.0
    cost_multiplier = 1.0 + (fee_bps + slippage_bps) / 10_000.0
    choices = [("buy_0_5", 0.05), ("buy_0_25", 0.025)]
    if confidence < 0.8:
        choices = choices[1:]
    for action, fraction in choices:
        if cash + 1e-9 >= nav * fraction * cost_multiplier:
            return action
    return None


def _mark_raw_decision(
    event: MutableMapping[str, Any],
    *,
    result: str,
    decision_event_id: Optional[str] = None,
) -> None:
    event["decision_result"] = result
    if decision_event_id:
        event["decision_event_id"] = decision_event_id


def _raw_decision_priority(
    event: Mapping[str, Any], levels: Mapping[str, ReferenceLevels]
) -> int:
    condition = str(event.get("condition") or "")
    price = _positive_float(event.get("price"))
    symbol = str(event.get("symbol") or "")
    if condition == "stop_loss":
        stop = _positive_float(levels.get(symbol, ReferenceLevels()).stop_loss)
        if event.get("severity") == "critical" or (
            price is not None and stop is not None and price <= stop * 0.98
        ):
            return 100
        return 70
    if condition == "adaptive_risk_review":
        return 80
    if condition == "target_reached":
        target = _positive_float(levels.get(symbol, ReferenceLevels()).target_price)
        return 65 if price is not None and target is not None and price >= target * 1.05 else 50
    if condition == "adaptive_entry_review":
        return 30
    return 0


def enqueue_cash_available_candidate_rechecks(
    state: MutableMapping[str, Any],
    *,
    now: datetime,
    quotes: Sequence[RealtimeQuote],
    candidates: Sequence[Mapping[str, Any]],
    shadow_state: Optional[Mapping[str, Any]],
) -> int:
    """Re-arm a still-valid observed candidate when same-market cash appears."""

    if shadow_state is None:
        return 0
    candidate_symbols = set()
    for candidate in candidates:
        try:
            candidate_symbols.add(
                canonical_symbol(str(candidate.get("symbol") or candidate.get("code") or ""))
            )
        except Exception:
            continue
    quote_by_symbol = {quote.symbol: quote for quote in quotes}
    created = 0
    for symbol, conditions in (state.get("decision_notifications") or {}).items():
        if symbol not in candidate_symbols or not isinstance(conditions, Mapping):
            continue
        decision_state = conditions.get("adaptive_entry_review") or {}
        if decision_state.get("last_action") != "observe":
            continue
        review = (
            state.get("symbols", {})
            .get(symbol, {})
            .get("adaptive_reviews", {})
            .get("adaptive_entry_review", {})
        )
        source_payload = decision_state.get("source_payload") or {}
        if _affordable_candidate_action(
            shadow_state,
            symbol=symbol,
            confidence=_finite_float(source_payload.get("confidence")) or 0.0,
            market_costs=(source_payload.get("market_costs") or {}),
        ) is None:
            continue
        if (
            review.get("status") != "active"
            or review.get("fingerprint") != source_payload.get("plan_fingerprint")
        ):
            continue
        quote = quote_by_symbol.get(symbol)
        low = _positive_float(source_payload.get("entry_low"))
        high = _positive_float(source_payload.get("entry_high"))
        stop = _positive_float(source_payload.get("stop_loss"))
        target = _positive_float(source_payload.get("target_price"))
        if (
            quote is None
            or quote.is_stale
            or quote.price is None
            or not all((low, high, stop, target))
            or not (low <= quote.price <= high)
            or not (stop < quote.price < target)
        ):
            continue
        payload = {
            **dict(source_payload),
            "signal_time": _iso(now),
            "quote_time": quote.provider_timestamp,
            "source": quote.source,
        }
        _queue_event(
            state,
            now=now,
            symbol=symbol,
            name=quote.name,
            condition="adaptive_entry_review",
            transition="cash_available_recheck",
            severity="warning",
            price=quote.price,
            change_pct=quote.change_pct,
            reference_price=low,
            volume=quote.volume,
            message="影子账户已有同币种可用现金，重新核对仍有效的可信候选计划。",
            queue_for_push=False,
            payload=payload,
            decision_result="awaiting_decision_gate",
        )
        created += 1
    return created


def process_actionable_decisions(
    state: MutableMapping[str, Any],
    *,
    now: datetime,
    raw_events: Sequence[MutableMapping[str, Any]],
    levels: Mapping[str, ReferenceLevels],
    shadow_state: Optional[MutableMapping[str, Any]] = None,
    signal_recorder: Optional[
        Callable[[MutableMapping[str, Any], Mapping[str, Any]], Mapping[str, Any]]
    ] = None,
) -> int:
    """Turn raw events into deterministic simulation decisions.

    Plain percentage moves remain in ``event_ledger`` but never pass this gate.
    A decision is pushed only when a trusted plan/reference level changes the
    simulated action or materially upgrades its risk tier.  No LLM, broker,
    Level-2, news, flow or fabricated evidence is used here.
    """

    created = 0
    actionable = {
        "stop_loss",
        "target_reached",
        "adaptive_entry_review",
        "adaptive_risk_review",
    }
    by_symbol: Dict[str, List[MutableMapping[str, Any]]] = {}
    for event in raw_events:
        if str(event.get("condition") or "") in actionable:
            by_symbol.setdefault(str(event.get("symbol") or ""), []).append(event)
    winner_ids = {
        str(
            max(
                events,
                key=lambda item: (
                    _raw_decision_priority(item, levels),
                    str(item.get("event_id") or ""),
                ),
            ).get("event_id")
            or ""
        )
        for events in by_symbol.values()
    }
    for raw_event in raw_events:
        condition = str(raw_event.get("condition") or "")
        transition = str(raw_event.get("transition") or "")
        if condition in {
            "data_quality",
            "reference_data_quality",
            "schedule_late",
        } or transition == "cleared":
            continue
        if condition in {"sharp_rise", "sharp_drop"}:
            _mark_raw_decision(raw_event, result="no_operation_price_move_only")
            continue
        if condition in actionable and str(raw_event.get("event_id") or "") not in winner_ids:
            _mark_raw_decision(raw_event, result="merged_into_stronger_same_cycle_decision")
            continue

        symbol = str(raw_event.get("symbol") or "")
        price = _positive_float(raw_event.get("price"))
        raw_payload = raw_event.get("payload") or {}
        quote_time = str(raw_payload.get("quote_time") or "")
        data_quality = str(raw_payload.get("data_quality") or "unknown")
        quantity = _shadow_strategy_quantity(shadow_state, symbol)
        key_level: Optional[float] = None
        next_trigger = "等待新的可靠计划或关键价位；无条件变化时不操作。"
        action = "no_operation"
        conclusion = "不操作 / 继续持有"
        position_change = "0"
        basis = "当前证据不足以改变模拟仓位。"
        severity = str(raw_event.get("severity") or "info")

        if price is None or data_quality not in {"fresh_l1", "high", "medium"}:
            _mark_raw_decision(raw_event, result="no_operation_unreliable_data")
            continue

        reference = levels.get(symbol, ReferenceLevels())
        if condition == "stop_loss":
            key_level = _positive_float(reference.stop_loss) or _positive_float(
                raw_event.get("reference_price")
            )
            if quantity <= 0 or key_level is None:
                _mark_raw_decision(raw_event, result="no_operation_no_shadow_holding_or_level")
                continue
            if price <= key_level * 0.98 or severity == "critical":
                action = "clear"
                conclusion = "模拟清仓"
                position_change = "-100%"
            else:
                action = "reduce_1_3"
                conclusion = "模拟减仓1/3"
                position_change = "-1/3"
            basis = "价格有效跌破系统已有可靠止损参考位，既有风险确认条件已满足。"
            next_trigger = (
                f"若继续跌至 {key_level * 0.98:.3f} 或风险升级则模拟清仓；"
                f"重新站回 {key_level * 1.005:.3f} 则撤销本风险预案。"
            )
        elif condition == "target_reached":
            key_level = _positive_float(reference.target_price) or _positive_float(
                raw_event.get("reference_price")
            )
            if quantity <= 0 or key_level is None:
                _mark_raw_decision(raw_event, result="no_operation_no_shadow_holding_or_level")
                continue
            if price >= key_level * 1.05:
                action = "reduce_1_2"
                conclusion = "模拟减仓1/2"
                position_change = "-1/2"
            else:
                action = "reduce_1_4"
                conclusion = "模拟减仓1/4"
                position_change = "-1/4"
            basis = "价格有效到达系统已有可靠目标参考位，按既定计划锁定部分收益。"
            next_trigger = (
                f"若继续有效突破 {key_level * 1.05:.3f} 则升级为模拟减仓1/2；"
                f"回落至 {key_level * 0.995:.3f} 下方则撤销本止盈预案。"
            )
        elif condition == "adaptive_entry_review":
            key_level = _positive_float(raw_payload.get("entry_low"))
            entry_high = _positive_float(raw_payload.get("entry_high"))
            stop_loss = _positive_float(raw_payload.get("stop_loss"))
            target = _positive_float(raw_payload.get("target_price"))
            confidence = _finite_float(raw_payload.get("confidence")) or 0.0
            if not all((key_level, entry_high, stop_loss, target)):
                _mark_raw_decision(raw_event, result="no_operation_incomplete_plan")
                continue
            if quantity > 0:
                _mark_raw_decision(raw_event, result="no_operation_already_held")
                continue
            affordable_action = _affordable_candidate_action(
                shadow_state,
                symbol=symbol,
                confidence=confidence,
                market_costs=(raw_payload.get("market_costs") or {}),
            )
            if affordable_action is None:
                action = "observe"
                conclusion = "进入观察，等待资金条件"
                position_change = "0"
                basis = "候选已通过扣费后的风险收益门槛并进入可靠买入区，但同币种现金不足以覆盖最小模拟仓位和成本。"
            else:
                action = affordable_action
                conclusion = _TRADE_ACTION_LABELS[action]
                position_change = "+5% NAV" if action == "buy_0_5" else "+2.5% NAV"
                basis = "候选已进入可信计划区，并通过交易成本、数据质量和风险收益门槛。"
            next_trigger = (
                f"仅在 {key_level:.3f}–{entry_high:.3f} 内维持预案；"
                f"跌破 {stop_loss:.3f} 或超过 {target:.3f} 时取消追入。"
            )
        elif condition == "adaptive_risk_review":
            key_level = _positive_float(raw_payload.get("stop_loss")) or _positive_float(
                raw_event.get("reference_price")
            )
            if quantity <= 0 or key_level is None:
                _mark_raw_decision(raw_event, result="no_operation_no_shadow_holding_or_level")
                continue
            action = "reduce_1_2"
            conclusion = "模拟减仓1/2"
            position_change = "-1/2"
            basis = "模拟持仓触及已落盘候选计划风险位，确定性风险门已触发。"
            next_trigger = (
                f"若继续跌破 {key_level * 0.98:.3f} 则模拟清仓；"
                f"重新站回 {key_level * 1.005:.3f} 则撤销剩余减仓预案。"
            )
        else:
            _mark_raw_decision(raw_event, result="no_operation_unsupported_event")
            continue

        fingerprint_material = {
            "condition": condition,
            "action": action,
            "severity": severity,
            "key_level": key_level,
            "plan_fingerprint": raw_payload.get("plan_fingerprint"),
        }
        fingerprint = hashlib.sha256(
            json.dumps(fingerprint_material, sort_keys=True).encode("utf-8")
        ).hexdigest()[:20]
        decision_state = (
            state.setdefault("decision_notifications", {})
            .setdefault(symbol, {})
            .setdefault(condition, {})
        )
        if (
            decision_state.get("status") == "active"
            and decision_state.get("fingerprint") == fingerprint
        ):
            _mark_raw_decision(raw_event, result="suppressed_unchanged_decision")
            continue
        if decision_state.get("status") == "active":
            _cancel_outbox_events(
                state,
                now=now,
                predicate=lambda event, current_symbol=symbol, current=condition: (
                    event.get("symbol") == current_symbol
                    and event.get("condition") == current
                    and (event.get("payload") or {}).get("kind")
                    == "trade_decision"
                ),
                reason="actionable_decision_replaced_before_delivery",
            )

        signal_id = ""
        if action in _TRADE_ACTION_LABELS and signal_recorder is not None and shadow_state:
            signal = {
                "signal_id": f"signal-{raw_event.get('event_id')}",
                "event_id": str(raw_event.get("event_id") or ""),
                "trade_date": now.date().isoformat(),
                "symbol": symbol,
                "signal_time": _iso(now),
                "quote_time": quote_time,
                "signal_price": price,
                "action": action,
                "position_delta": position_change,
                "reason": basis,
                "current_key_level": key_level,
                "next_trigger": next_trigger,
                "data_quality": data_quality,
                "category": (
                    "existing_position_management"
                    if symbol in (shadow_state.get("initial_positions") or {})
                    else "new_candidate_selection"
                ),
                "market_costs": raw_payload.get("market_costs"),
            }
            try:
                saved_signal = signal_recorder(shadow_state, signal)
            except Exception as exc:
                _mark_raw_decision(raw_event, result=f"signal_persist_failed:{exc}")
                continue
            signal_id = str(saved_signal.get("signal_id") or signal["signal_id"])

        decision_event = _queue_event(
            state,
            now=now,
            symbol=symbol,
            name=str(raw_event.get("name") or ""),
            condition=condition,
            transition=action,
            severity=severity,
            price=price,
            change_pct=_finite_float(raw_event.get("change_pct")),
            reference_price=key_level,
            volume=_nonnegative_float(raw_event.get("volume")),
            message=basis,
            decision_result="push_actionable_decision",
            payload={
                "kind": "trade_decision",
                "conclusion": conclusion,
                "action": _TRADE_ACTION_LABELS.get(action, "不操作 / 等待条件"),
                "position_change": position_change,
                "basis": basis,
                "key_level": key_level,
                "next_trigger": next_trigger,
                "data_quality": data_quality,
                "quote_time": quote_time,
                "signal_id": signal_id,
                "source_event_id": raw_event.get("event_id"),
                "simulation_only": True,
                "human_review_required": True,
            },
        )
        decision_state.update(
            {
                "status": "active",
                "fingerprint": fingerprint,
                "last_action": action,
                "source_event_id": raw_event.get("event_id"),
                "source_payload": dict(raw_payload),
                "decision_event_id": decision_event["event_id"],
                "updated_at": _iso(now),
            }
        )
        _mark_raw_decision(
            raw_event,
            result=f"decision:{action}",
            decision_event_id=decision_event["event_id"],
        )
        created += 1
    return created


def process_watch_account_decisions(
    state: MutableMapping[str, Any],
    *,
    now: datetime,
    raw_events: Sequence[MutableMapping[str, Any]],
    levels: Mapping[str, ReferenceLevels],
    contexts_by_symbol: Optional[
        Mapping[str, Sequence[Mapping[str, str]]]
    ] = None,
) -> int:
    """Create account-labelled human-review alerts without touching PRIMARY A/B.

    Watch layers reuse the already-fetched quote and reference level.  This
    function has no shadow-state or signal-recorder argument by design, so a
    FAMILY/SECONDARY/SISTER decision cannot enter PRIMARY NAV or execution.
    """

    contexts = contexts_by_symbol or watch_contexts_by_symbol()
    actionable = {
        "stop_loss",
        "target_reached",
        "adaptive_entry_review",
        "adaptive_risk_review",
    }
    candidates: Dict[tuple[str, str], List[MutableMapping[str, Any]]] = {}
    for event in raw_events:
        symbol = str(event.get("symbol") or "")
        condition = str(event.get("condition") or "")
        transition = str(event.get("transition") or "")
        if condition in {"sharp_rise", "sharp_drop"}:
            continue
        if condition not in actionable and transition != "cleared":
            continue
        for context in contexts.get(symbol, ()):
            key = (str(context.get("layer") or ""), symbol)
            candidates.setdefault(key, []).append(event)

    created = 0
    watch_state = state.setdefault("watch_decision_notifications", {})
    for (layer, symbol), events in candidates.items():
        context = next(
            (
                item
                for item in contexts.get(symbol, ())
                if str(item.get("layer") or "") == layer
            ),
            None,
        )
        if context is None:
            continue
        event = max(
            events,
            key=lambda item: (
                1 if str(item.get("transition") or "") == "cleared" else 2,
                _raw_decision_priority(item, levels),
                str(item.get("event_id") or ""),
            ),
        )
        condition = str(event.get("condition") or "")
        transition = str(event.get("transition") or "")
        status = str(context.get("status") or "")
        raw_payload = event.get("payload") or {}
        price = _positive_float(event.get("price"))
        data_quality = str(raw_payload.get("data_quality") or "unknown")
        if price is None or data_quality not in {"fresh_l1", "high", "medium"}:
            continue

        condition_state = (
            watch_state.setdefault(layer, {})
            .setdefault(symbol, {})
            .setdefault(condition, {})
        )
        reference = levels.get(symbol, ReferenceLevels())
        key_level: Optional[float] = None
        action = "不操作"
        position_change = "0"
        conclusion = "继续持有"
        basis = "没有新的可靠条件改变当前人工复核结论。"
        next_trigger = "等待新的可靠关键位或研究计划。"

        if transition == "cleared":
            if condition_state.get("status") != "active":
                continue
            conclusion = "风险解除，继续持有" if status != "candidate" else "风险解除，继续观察"
            basis = "此前已提醒的风险条件已由新鲜行情确认解除。"
            action = "不操作"
        elif condition == "adaptive_entry_review":
            if status != "candidate":
                continue
            low = _positive_float(raw_payload.get("entry_low"))
            high = _positive_float(raw_payload.get("entry_high"))
            stop = _positive_float(raw_payload.get("stop_loss"))
            target = _positive_float(raw_payload.get("target_price"))
            if not all((low, high, stop, target)):
                continue
            key_level = low
            conclusion = "候选观察，进入计划买入区"
            action = "继续观察"
            basis = "候选首次进入已有可信计划的可操作观察区；尚未确认实际买入。"
            next_trigger = (
                f"仅在 {low:.3f}–{high:.3f} 内保持候选；"
                f"跌破 {stop:.3f} 或超过 {target:.3f} 时取消候选预案。"
            )
        elif status == "candidate":
            # A not-yet-held candidate can never receive a reduce/clear alert.
            continue
        elif condition in {"stop_loss", "adaptive_risk_review"}:
            key_level = _positive_float(reference.stop_loss) or _positive_float(
                raw_payload.get("stop_loss")
            ) or _positive_float(event.get("reference_price"))
            if key_level is None:
                continue
            if price <= key_level * 0.98 or event.get("severity") == "critical":
                conclusion = "人工复核清仓"
                action = "人工复核清仓"
                position_change = "人工复核 -100%"
            elif condition == "adaptive_risk_review":
                conclusion = "人工复核减仓1/2"
                action = "人工复核减仓1/2"
                position_change = "人工复核 -1/2"
            else:
                conclusion = "人工复核减仓1/3"
                action = "人工复核减仓1/3"
                position_change = "人工复核 -1/3"
            basis = "价格有效跌破已有可靠风险参考位，需要人工复核持仓风险。"
            next_trigger = (
                f"若继续跌至 {key_level * 0.98:.3f} 则风险升级；"
                f"重新站回 {key_level * 1.005:.3f} 则撤销本预案。"
            )
        elif condition == "target_reached":
            key_level = _positive_float(reference.target_price) or _positive_float(
                event.get("reference_price")
            )
            if key_level is None:
                continue
            if price >= key_level * 1.05:
                conclusion = "人工复核减仓1/2"
                action = "人工复核减仓1/2"
                position_change = "人工复核 -1/2"
            else:
                conclusion = "人工复核减仓1/4"
                action = "人工复核减仓1/4"
                position_change = "人工复核 -1/4"
            basis = "价格到达已有可靠目标参考位，需要人工复核是否锁定部分收益。"
            next_trigger = (
                f"继续有效突破 {key_level * 1.05:.3f} 则风险收益结论升级；"
                f"回落至 {key_level * 0.995:.3f} 下方则撤销本预案。"
            )
        else:
            continue

        fingerprint = hashlib.sha256(
            json.dumps(
                {
                    "condition": condition,
                    "transition": transition,
                    "conclusion": conclusion,
                    "severity": str(event.get("severity") or "info"),
                    "key_level": key_level,
                    "plan_fingerprint": raw_payload.get("plan_fingerprint"),
                },
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()[:20]
        if (
            condition_state.get("status") == "active"
            and condition_state.get("fingerprint") == fingerprint
        ):
            continue

        unique_condition = f"watch:{layer}:{condition}"
        _cancel_outbox_events(
            state,
            now=now,
            predicate=lambda queued, current_symbol=symbol, current=unique_condition: (
                queued.get("symbol") == current_symbol
                and queued.get("condition") == current
            ),
            reason="watch_decision_replaced_before_delivery",
        )
        decision_event = _queue_event(
            state,
            now=now,
            symbol=symbol,
            name=str(context.get("name") or event.get("name") or ""),
            condition=unique_condition,
            transition="watch_human_review",
            severity=str(event.get("severity") or "warning"),
            price=price,
            change_pct=_finite_float(event.get("change_pct")),
            reference_price=key_level,
            volume=_nonnegative_float(event.get("volume")),
            message=basis,
            decision_result="push_watch_account_decision",
            payload={
                "kind": "trade_decision",
                "account_layer": layer,
                "account_prefix": str(context.get("push_prefix") or ""),
                "holding_status": status,
                "conclusion": conclusion,
                "action": action,
                "position_change": position_change,
                "basis": basis,
                "key_level": key_level,
                "next_trigger": next_trigger,
                "data_quality": data_quality,
                "quote_time": raw_payload.get("quote_time"),
                "source_event_id": event.get("event_id"),
                "simulation_only": True,
                "human_review_required": True,
                "primary_shadow_eligible": False,
            },
        )
        condition_state.update(
            {
                "status": "cleared" if transition == "cleared" else "active",
                "fingerprint": fingerprint,
                "decision_event_id": decision_event["event_id"],
                "updated_at": _iso(now),
            }
        )
        event.setdefault("watch_decision_results", {})[layer] = (
            f"decision:{conclusion}"
        )
        created += 1
    return created


def render_outbox(events: Sequence[Mapping[str, Any]], now: datetime) -> str:
    labels = {
        "sharp_drop": "日内跌幅风险",
        "sharp_rise": "日内快速上涨",
        "stop_loss": "到达止损参考区",
        "target_reached": "到达目标参考区",
        "data_quality": "行情数据降级",
        "reference_data_quality": "参考位数据降级",
        "schedule_late": "计划时段未执行",
        "adaptive_entry_review": "模拟候选人工复核",
        "adaptive_risk_review": "模拟持仓风险复核",
    }
    lines = [
        "# 盘中模拟决策提醒",
        "",
        f"- 时间：{_iso(now)}",
        f"- 新事件：{len(events)}",
        "",
        "> 仅供模拟和人工复核，不构成交易指令；系统不会连接券商或自动下单。",
        "",
    ]
    for event in events:
        name = str(event.get("name") or "").strip()
        symbol = str(event.get("symbol") or "SYSTEM")
        price = _positive_float(event.get("price"))
        change_pct = _finite_float(event.get("change_pct"))
        volume = _nonnegative_float(event.get("volume"))
        display = f"{name} ({symbol})" if name else symbol
        payload = event.get("payload") or {}
        if payload.get("kind") == "trade_decision":
            account_prefix = str(payload.get("account_prefix") or "")
            if account_prefix:
                display = f"{account_prefix}{display}"
            key_level = _positive_float(payload.get("key_level"))
            lines.extend(
                [
                    f"## {display}｜{payload.get('conclusion') or '等待条件'}",
                    "",
                    f"- 当前价格：{price:.3f}" if price is not None else "- 当前价格：无可靠价格",
                    (
                        f"- 当前涨跌：{change_pct:+.2f}%"
                        if change_pct is not None
                        else "- 当前涨跌：无可靠数据"
                    ),
                    f"- 建议动作：{payload.get('action') or '不操作'}",
                    f"- 模拟仓位变化：{payload.get('position_change') or '0'}",
                    f"- 主要依据：{payload.get('basis') or event.get('message')}",
                    (
                        f"- 关键价位：{key_level:.3f}"
                        if key_level is not None
                        else "- 关键价位：没有可用的可靠关键位"
                    ),
                    f"- 下一触发条件：{payload.get('next_trigger') or '等待新的可靠条件'}",
                    (
                        f"- 数据质量：{payload.get('data_quality') or 'unknown'}；"
                        f"行情时间：{payload.get('quote_time') or 'unknown'}"
                    ),
                    f"- 信号号：`{payload.get('signal_id') or event.get('event_id')}`",
                    "",
                    "仅供模拟和人工复核，不构成交易指令；系统不会连接券商或自动下单。",
                    "",
                ]
            )
            continue
        details = []
        if price is not None:
            details.append(f"价格 {price:.3f}")
        if change_pct is not None:
            details.append(f"涨跌 {change_pct:+.2f}%")
        if volume is not None:
            details.append(f"成交量 {volume:.0f}")
        lines.extend(
            [
                f"## {display} · {labels.get(str(event.get('condition')), event.get('condition'))}",
                "",
                f"- 严重度：{event.get('severity')}",
                f"- 状态：{event.get('transition')}",
                f"- 数据：{'；'.join(details) if details else '无可靠价格'}",
                f"- 事件号：`{event.get('event_id')}`",
                f"- 提示：{event.get('message')}",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def _default_notification_sender(*, title: str, content: str) -> bool:
    token = os.getenv("PUSHPLUS_TOKEN", "").strip()
    if not token:
        return False
    return send_pushplus(
        token=token,
        topic=os.getenv("PUSHPLUS_TOPIC", "").strip(),
        title=title,
        content=content,
        timeout_seconds=10.0,
    )


def flush_outbox(
    state: MutableMapping[str, Any],
    *,
    now: datetime,
    sender: Callable[..., bool] = _default_notification_sender,
    event_predicate: Optional[Callable[[Mapping[str, Any]], bool]] = None,
) -> int:
    _suppress_legacy_raw_outbox(state, now=now)
    # Defensive restart guard: an event retained by a prior failed delivery
    # must not be sent after a newer fresh quote has cleared its condition.
    _cancel_events_for_cleared_conditions(state, now=now)
    due: List[Dict[str, Any]] = []
    for event in state.get("outbox", []):
        if event_predicate is not None and not event_predicate(event):
            continue
        next_attempt = _parse_iso(event.get("next_attempt_at"))
        if next_attempt is None or next_attempt <= now:
            due.append(event)
    if not due:
        return 0

    content = render_outbox(due, now)
    account_prefixes = {
        str((event.get("payload") or {}).get("account_prefix") or "")
        for event in due
        if str((event.get("payload") or {}).get("account_prefix") or "")
    }
    has_unlabelled = any(
        not str((event.get("payload") or {}).get("account_prefix") or "")
        for event in due
    )
    if len(account_prefixes) == 1 and not has_unlabelled:
        title_prefix = next(iter(account_prefixes))
    elif account_prefixes:
        title_prefix = "【多账户】"
    else:
        title_prefix = ""
    try:
        sent = bool(
            sender(
                title=(
                    f"{title_prefix}盘中模拟决策提醒 - "
                    f"{now.strftime('%m-%d %H:%M')}"
                ),
                content=content,
            )
        )
    except Exception as exc:
        print(f"PushPlus 推送异常，保留 outbox 重试: {exc}", file=sys.stderr)
        sent = False

    if not sent:
        for event in due:
            attempts = int(event.get("attempts") or 0) + 1
            event["attempts"] = attempts
            delay = min(600, 30 * (2 ** min(attempts - 1, 5)))
            event["next_attempt_at"] = _iso(now + timedelta(seconds=delay))
        return 0

    sent_ids = {event["event_id"] for event in due}
    state["outbox"] = [
        event for event in state.get("outbox", []) if event.get("event_id") not in sent_ids
    ]
    for event in due:
        symbol_state = state.get("symbols", {}).get(event.get("symbol"), {})
        condition_state = symbol_state.get("conditions", {}).get(event.get("condition"))
        if isinstance(condition_state, MutableMapping):
            condition_state["last_notified_at"] = _iso(now)
            condition_state["last_notified_price"] = event.get("price")
            condition_state["last_event_id"] = event.get("event_id")
        if event.get("condition") == "data_quality":
            provider = state.setdefault("provider", {})
            if event.get("transition") == "degraded":
                provider["health_alert_notified"] = True
            elif event.get("transition") == "recovered":
                provider["health_alert_notified"] = False
    return len(due)


def _queue_data_health_event(
    state: MutableMapping[str, Any],
    *,
    now: datetime,
    valid_count: int,
    total_count: int,
) -> int:
    provider = state.setdefault("provider", {})
    if provider.get("health_alert_active"):
        return 0
    provider["health_alert_active"] = True
    _queue_event(
        state,
        now=now,
        symbol="SYSTEM",
        name="行情数据源",
        condition="data_quality",
        transition="degraded",
        severity="warning",
        price=None,
        change_pct=None,
        reference_price=None,
        message=(
            f"连续多轮有效行情不足（本轮 {valid_count}/{total_count}），"
            "已自动降频并等待数据源恢复；缺失行情不会触发买卖提示。"
        ),
        payload={
            "signal_time": _iso(now),
            "quote_time": None,
            "data_quality": "degraded",
            "valid_count": valid_count,
            "total_count": total_count,
        },
        decision_result="push_data_degradation_once",
    )
    return 1


def _queue_reference_health_event(
    state: MutableMapping[str, Any],
    *,
    now: datetime,
    covered_count: int,
    total_count: int,
    database_available: bool,
) -> int:
    provider = state.setdefault("provider", {})
    if provider.get("reference_alert_active"):
        return 0
    provider["reference_alert_active"] = True
    _queue_event(
        state,
        now=now,
        symbol="SYSTEM",
        name="止损/目标参考位",
        condition="reference_data_quality",
        transition="degraded",
        severity="warning",
        price=None,
        change_pct=None,
        reference_price=None,
        message=(
            f"参考位覆盖 {covered_count}/{total_count}；"
            f"收盘数据库{'存在' if database_available else '缺失'}。"
            "本会话仍可按新鲜实时涨跌监控，但不会假装已覆盖缺失标的的"
            "止损或目标位。"
        ),
        payload={
            "signal_time": _iso(now),
            "quote_time": None,
            "data_quality": "reference_levels_degraded",
            "covered_count": covered_count,
            "total_count": total_count,
        },
        decision_result="push_reference_degradation_once",
    )
    return 1


def run_cycle(
    *,
    symbols: Sequence[str],
    primary_symbols: Optional[Sequence[str]] = None,
    state: MutableMapping[str, Any],
    levels: Mapping[str, ReferenceLevels],
    fetcher: Any,
    now: datetime,
    min_quote_coverage: float = DEFAULT_MIN_QUOTE_COVERAGE,
    down_threshold_pct: float = 3.0,
    up_threshold_pct: float = 5.0,
    near_level_pct: float = DEFAULT_NEAR_LEVEL_PCT,
    near_change_points: float = DEFAULT_NEAR_CHANGE_POINTS,
    fast_hold_seconds: float = DEFAULT_FAST_HOLD_SECONDS,
    normal_interval_seconds: float = DEFAULT_NORMAL_INTERVAL_SECONDS,
    fast_interval_seconds: float = DEFAULT_FAST_INTERVAL_SECONDS,
    degraded_interval_seconds: float = DEFAULT_DEGRADED_INTERVAL_SECONDS,
    low_coverage_limit: int = DEFAULT_LOW_COVERAGE_LIMIT,
    cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS,
    deterioration_pct: float = DEFAULT_DETERIORATION_PCT,
    candidate_plans: Sequence[Mapping[str, Any]] = (),
    level2_adapter: Optional[Level2DataAdapter] = None,
    notification_sender: Callable[..., bool] = _default_notification_sender,
    shadow_state: Optional[MutableMapping[str, Any]] = None,
    shadow_signal_recorder: Optional[
        Callable[[MutableMapping[str, Any], Mapping[str, Any]], Mapping[str, Any]]
    ] = None,
    shadow_freshness_seconds: float = DEFAULT_FRESHNESS_SECONDS,
) -> CycleResult:
    ledger_start = len(state.setdefault("event_ledger", []))
    clear_removed_adaptive_plan_reviews(
        state,
        now=now,
        candidates=candidate_plans,
    )
    canonical = list(dict.fromkeys(canonical_symbol(symbol) for symbol in symbols))
    primary_set = (
        {canonical_symbol(symbol) for symbol in primary_symbols}
        if primary_symbols is not None
        else set(canonical)
    )
    primary_canonical = [symbol for symbol in canonical if symbol in primary_set]
    context_index = watch_contexts_by_symbol()
    watch_only_symbols = set(context_index) - primary_set
    try:
        fetched = fetcher.fetch(canonical, now=now)
    except Exception as exc:
        print(f"批量行情本轮失败，进入降级统计: {exc}", file=sys.stderr)
        fetched = {}
    quotes = [
        fetched.get(symbol)
        or RealtimeQuote(
            symbol=symbol,
            fetched_at=_iso(now),
            is_stale=True,
            source="missing",
        )
        for symbol in canonical
    ]
    primary_valid = [
        quote
        for quote in quotes
        if quote.symbol in primary_set
        and quote.price is not None
        and not quote.is_stale
    ]
    quote_map = {quote.symbol: quote for quote in quotes}
    if shadow_state is not None:
        shadow_quote_map = {
            symbol: quote
            for symbol, quote in quote_map.items()
            if symbol not in watch_only_symbols
        }
        # Pending signals are evaluated before this cycle creates any new
        # signal, guaranteeing that a signal cannot fill on its own quote.
        execute_shadow_pending(
            shadow_state,
            shadow_quote_map,
            now=now,
            freshness_seconds=shadow_freshness_seconds,
        )
        update_shadow_latest_quotes(
            shadow_state,
            shadow_quote_map,
            now,
            freshness_seconds=shadow_freshness_seconds,
        )
    coverage = (
        len(primary_valid) / len(primary_canonical) if primary_canonical else 0.0
    )
    provider = state.setdefault("provider", {})
    layer_symbols: Dict[str, set[str]] = {}
    for symbol, contexts in context_index.items():
        if symbol not in canonical:
            continue
        for context in contexts:
            layer_symbols.setdefault(str(context.get("layer") or ""), set()).add(
                symbol
            )
    provider["coverage_by_account_layer"] = {
        "PRIMARY_PORTFOLIO": {
            "valid": len(primary_valid),
            "total": len(primary_canonical),
            "coverage": coverage,
            "drives_primary_degradation": True,
        },
        **{
            layer: {
                "valid": sum(
                    1
                    for quote in quotes
                    if quote.symbol in members
                    and quote.price is not None
                    and not quote.is_stale
                ),
                "total": len(members),
                "coverage": (
                    sum(
                        1
                        for quote in quotes
                        if quote.symbol in members
                        and quote.price is not None
                        and not quote.is_stale
                    )
                    / len(members)
                    if members
                    else 0.0
                ),
                "drives_primary_degradation": False,
            }
            for layer, members in layer_symbols.items()
        },
    }
    recovery_events = 0
    depth_adapter = level2_adapter or Level2DataAdapter()
    try:
        level2_by_symbol = depth_adapter.assess(canonical, now=now)
        if not isinstance(level2_by_symbol, Mapping):
            raise TypeError("level2 assessment must be a mapping")
        level2_by_symbol = dict(level2_by_symbol)
    except Exception:
        # An injected provider/adapter remains an external runtime boundary.
        # Preserve the fresh L1 safety monitor even if that boundary fails in
        # an unexpected way.
        level2_by_symbol = {
            symbol: Level2Assessment(
                symbol=symbol,
                status=LEVEL2_PROVIDER_ERROR,
                usable_as_level2=False,
                confidence_multiplier=0.60,
                reason_codes=("level2_adapter_failed",),
            )
            for symbol in canonical
        }
    for symbol in canonical:
        if not isinstance(level2_by_symbol.get(symbol), Level2Assessment):
            level2_by_symbol[symbol] = Level2Assessment(
                symbol=symbol,
                status=LEVEL2_PROVIDER_ERROR,
                usable_as_level2=False,
                confidence_multiplier=0.60,
                reason_codes=("level2_assessment_invalid",),
            )
    try:
        level2_configured = bool(getattr(depth_adapter, "configured", False))
    except Exception:
        level2_configured = False
    level2_assessments = [
        level2_by_symbol[symbol]
        for symbol in canonical
        if symbol in level2_by_symbol
    ]
    level2_available = sum(
        1 for assessment in level2_assessments if assessment.usable_as_level2
    )
    level2_statuses: Dict[str, int] = {}
    for assessment in level2_assessments:
        level2_statuses[assessment.status] = (
            level2_statuses.get(assessment.status, 0) + 1
        )
    provider["level2"] = {
        "configured": level2_configured,
        "provider": str(
            getattr(getattr(depth_adapter, "provider", None), "provider_name", "")
            or ""
        ),
        "usable_symbols": level2_available,
        "total_symbols": len(canonical),
        "coverage": level2_available / len(canonical) if canonical else 0.0,
        "statuses": level2_statuses,
        "assessments": {
            assessment.symbol: assessment.as_dict()
            for assessment in level2_assessments
        },
        "fallback_active": level2_available < len(canonical),
        "last_checked_at": _iso(now),
    }
    provider.setdefault("data_capabilities", {})["level2_order_book"] = (
        "authorized_fresh_complete"
        if level2_available == len(canonical) and canonical
        else "partial_authorized_fresh_complete"
        if level2_available
        else "unavailable_using_declared_fallback"
    )
    if not primary_valid or coverage < min_quote_coverage:
        provider["consecutive_low_coverage"] = (
            int(provider.get("consecutive_low_coverage") or 0) + 1
        )
    else:
        provider["consecutive_low_coverage"] = 0
        provider["degraded"] = False
        if provider.get("health_alert_active"):
            _cancel_outbox_events(
                state,
                now=now,
                predicate=lambda event: (
                    event.get("symbol") == "SYSTEM"
                    and event.get("condition") == "data_quality"
                ),
                reason="quote_coverage_recovered_before_delivery",
            )
            if provider.get("health_alert_notified"):
                _queue_event(
                    state,
                    now=now,
                    symbol="SYSTEM",
                    name="行情数据源",
                    condition="data_quality",
                    transition="recovered",
                    severity="info",
                    price=None,
                    change_pct=None,
                    reference_price=None,
                    message="连续覆盖率已恢复到决策要求，行情数据源已恢复。",
                    payload={
                        "signal_time": _iso(now),
                        "quote_time": None,
                        "data_quality": "recovered",
                        "valid_count": len(primary_valid),
                        "total_count": len(primary_canonical),
                    },
                    decision_result="push_data_recovery_once",
                )
                recovery_events += 1
        provider["health_alert_active"] = False

    degraded = int(provider.get("consecutive_low_coverage") or 0) >= low_coverage_limit
    provider["degraded"] = degraded
    created = recovery_events
    if degraded:
        created += _queue_data_health_event(
            state,
            now=now,
            valid_count=len(primary_valid),
            total_count=len(primary_canonical),
        )

    fast_until = _parse_iso(provider.get("fast_until"))
    fresh_risk_or_near = False
    for quote in quotes:
        reference = levels.get(quote.symbol, ReferenceLevels())
        conservative_quote = QuoteSnapshot(
            symbol=quote.symbol,
            name=quote.name or reference.name,
            price=quote.price,
            change_pct=quote.change_pct,
            is_stale=quote.is_stale,
            source=quote.source,
        )
        alerts = evaluate_quote(
            conservative_quote,
            reference,
            down_threshold_pct=down_threshold_pct,
            up_threshold_pct=up_threshold_pct,
        )
        created += update_condition_state(
            state,
            now=now,
            quote=quote,
            alerts=alerts,
            levels=reference,
            cooldown_seconds=cooldown_seconds,
            deterioration_pct=deterioration_pct,
            down_threshold_pct=down_threshold_pct,
            up_threshold_pct=up_threshold_pct,
        )
        if alerts or is_near_threshold(
            quote,
            reference,
            down_threshold_pct=down_threshold_pct,
            up_threshold_pct=up_threshold_pct,
            near_level_pct=near_level_pct,
            near_change_points=near_change_points,
        ):
            fresh_risk_or_near = True
            candidate = now + timedelta(seconds=fast_hold_seconds)
            if fast_until is None or candidate > fast_until:
                fast_until = candidate

    if fast_until is not None:
        provider["fast_until"] = _iso(fast_until)
    created += enqueue_adaptive_plan_reviews(
        state,
        now=now,
        quotes=quotes,
        candidates=candidate_plans,
        confidence_multipliers={
            symbol: assessment.confidence_multiplier
            for symbol, assessment in level2_by_symbol.items()
        },
    )
    created += enqueue_cash_available_candidate_rechecks(
        state,
        now=now,
        quotes=quotes,
        candidates=candidate_plans,
        shadow_state=shadow_state,
    )
    raw_events = [
        event
        for event in state.get("event_ledger", [])[ledger_start:]
        if isinstance(event, MutableMapping)
    ]
    created += process_actionable_decisions(
        state,
        now=now,
        raw_events=[
            event
            for event in raw_events
            if str(event.get("symbol") or "") not in watch_only_symbols
        ],
        levels=levels,
        shadow_state=shadow_state,
        signal_recorder=shadow_signal_recorder,
    )
    created += process_watch_account_decisions(
        state,
        now=now,
        raw_events=raw_events,
        levels=levels,
        contexts_by_symbol=context_index,
    )
    provider["last_coverage"] = coverage
    provider["last_checked_at"] = _iso(now)
    state["updated_at"] = _iso(now)

    notified = flush_outbox(state, now=now, sender=notification_sender)
    provider_minimum = max(0.0, float(getattr(fetcher, "min_interval_seconds", 0.0)))
    if fresh_risk_or_near:
        # Partial provider degradation must not slow symbols whose own fresh
        # quote is already at/near a risk threshold.
        interval = fast_interval_seconds
    elif degraded:
        interval = degraded_interval_seconds
    elif fast_until is not None and fast_until > now:
        interval = fast_interval_seconds
    else:
        interval = normal_interval_seconds
    interval = max(interval, provider_minimum)
    return CycleResult(
        checked_at=_iso(now),
        active_symbols=canonical,
        quotes=quotes,
        valid_quote_count=len(primary_valid),
        coverage=coverage,
        new_event_count=created,
        pending_event_count=len(state.get("outbox", [])),
        notified_event_count=notified,
        next_interval_seconds=interval,
        degraded=degraded,
        level2_assessments=level2_assessments,
    )


def render_session_report(
    *,
    now: datetime,
    symbols: Sequence[str],
    last_cycle: Optional[CycleResult],
    cycles: int,
    events_created: int,
    events_notified: int,
    pending_events: int,
    provider_status: Optional[Mapping[str, Any]] = None,
) -> str:
    provider_status = provider_status or {}
    reference = provider_status.get("reference_levels") or {}
    level2 = provider_status.get("level2") or {}
    late_schedule = provider_status.get("late_schedule") or {}
    lines = [
        "# 分钟级盘中模拟监控",
        "",
        f"- 更新时间：{_iso(now)}",
        f"- 配置标的：{len(symbols)}",
        f"- 会话循环：{cycles}",
        f"- 产生事件：{events_created}",
        f"- 已推送事件：{events_notified}",
        f"- 待重试事件：{pending_events}",
        f"- 会话状态：{provider_status.get('session_status') or 'running'}",
        (
            f"- 参考位覆盖：{reference.get('covered_symbols', 0)}/"
            f"{reference.get('total_symbols', len(symbols))}"
            f"（{float(reference.get('coverage') or 0):.2%}）"
        ),
        "",
        "> 仅用于模拟监控和人工复核；不连接券商，不自动下单。",
        "",
        "## 数据能力说明",
        "",
        "- 实时价格：腾讯批量基础行情；提供方时间戳缺失或超过 90 秒时不触发交易类提醒。",
        "- 成交量：仅在基础行情字段可用时记录。",
        "- 分时 K 线：本监控尚未采集，不会声称已分析。",
        (
            "- Level-2 盘口："
            + (
                f"已对 {level2.get('usable_symbols', 0)}/"
                f"{level2.get('total_symbols', len(symbols))} 个标的通过授权、"
                "新鲜度和深度完整性校验。"
                if level2.get("usable_symbols")
                else "当前状态 unavailable：没有通过授权、新鲜度和完整性校验的可靠数据；"
                "本监控仅继续使用新鲜基础行情和已落盘的参考位/候选计划，并降低"
                "候选信号置信度。技术、公告、基本面或资金证据只有在上游明确提供"
                "来源与时间戳时才可使用，本轮不会声称已自行采集。"
            )
        ),
        "- 数据等级护栏：L1 永远不会标记或展示为 Level-2。",
    ]
    level2_statuses = level2.get("statuses") or {}
    if isinstance(level2_statuses, Mapping) and level2_statuses:
        lines.append(
            "- Level-2 状态计数："
            + "；".join(
                f"{status}={count}"
                for status, count in sorted(level2_statuses.items())
            )
        )
    assessments = level2.get("assessments") or {}
    reason_counts: Dict[str, int] = {}
    if isinstance(assessments, Mapping):
        for assessment in assessments.values():
            if not isinstance(assessment, Mapping):
                continue
            for reason in assessment.get("reason_codes") or []:
                text = str(reason or "").strip()
                if text:
                    reason_counts[text] = reason_counts.get(text, 0) + 1
    if reason_counts:
        lines.append(
            "- Level-2 降级原因："
            + "；".join(
                f"{reason}={count}" for reason, count in sorted(reason_counts.items())
            )
        )
    if late_schedule:
        lines.extend(
            [
                "",
                "## 调度时效",
                "",
                "- 本轮未抓取盘中行情：外部 dispatch 到达时，目标交易时段已经结束。",
                f"- 实际到达：{late_schedule.get('observed_at') or '-'}",
                f"- 目标结束：{late_schedule.get('intended_end_at') or '-'}",
                f"- 超时秒数：{float(late_schedule.get('delay_seconds') or 0):.0f}",
                "- 处理：保留既有状态，不补造历史盘中数据，也不发送买卖信号。",
            ]
        )
    if last_cycle is not None:
        primary_coverage = (
            provider_status.get("coverage_by_account_layer", {}).get(
                "PRIMARY_PORTFOLIO", {}
            )
        )
        primary_total = int(
            primary_coverage.get("total", last_cycle.valid_quote_count)
        )
        lines.extend(
            [
                "",
                "## 最近一轮",
                "",
                f"- 开市标的：{len(last_cycle.active_symbols)}",
                (
                    f"- PRIMARY有效行情：{last_cycle.valid_quote_count}/"
                    f"{primary_total}（{last_cycle.coverage:.2%}）"
                ),
                f"- 下一轮间隔：{last_cycle.next_interval_seconds:.0f} 秒",
                f"- 数据源降级：{'是' if last_cycle.degraded else '否'}",
                (
                    f"- Level-2 有效覆盖："
                    f"{sum(1 for item in last_cycle.level2_assessments if item.status == LEVEL2_AVAILABLE)}"
                    f"/{len(last_cycle.level2_assessments)}"
                ),
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def _write_report(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _notify_shadow_scorecard(
    shadow_state: MutableMapping[str, Any],
    *,
    now: datetime,
    sender: Callable[..., bool],
) -> bool:
    daily_nav = shadow_state.get("daily_nav") or []
    if not daily_nav:
        return False
    notifications = shadow_state.setdefault("scorecard_notifications", {})
    for item in daily_nav:
        trade_date = str(item.get("trade_date") or "")
        notifications.setdefault(trade_date, {"status": "pending", "attempts": 0})

    for item in daily_nav:
        trade_date = str(item.get("trade_date") or "")
        notification = notifications[trade_date]
        if notification.get("status") == "sent":
            continue
        try:
            sent = bool(
                sender(
                    title=f"策略 vs 死拿每日成绩单 - {trade_date}",
                    content=render_shadow_scorecard(
                        shadow_state, trade_date=trade_date
                    ),
                )
            )
        except Exception as exc:
            print(
                f"A/B 每日成绩单 PushPlus 异常，保留状态下次重试: {exc}",
                file=sys.stderr,
            )
            sent = False
        notification["attempts"] = int(notification.get("attempts") or 0) + 1
        notification["last_attempt_at"] = _iso(now)
        if not sent:
            return False
        notification["status"] = "sent"
        notification["sent_at"] = _iso(now)
    return True


def run_session(
    *,
    stocks: str,
    end_at: datetime,
    database_path: Path,
    state_path: Path,
    report_path: Path,
    candidate_plans_path: Optional[Path] = None,
    shadow_state_path: Optional[Path] = None,
    shadow_report_path: Optional[Path] = None,
    shadow_initial_portfolio_json: Optional[str] = None,
    fetcher: Any = None,
    level2_adapter: Optional[Level2DataAdapter] = None,
    phase_resolver: Callable[[str, datetime], str] = default_phase_resolver,
    clock: Callable[[], datetime] = lambda: datetime.now(SHANGHAI_TZ),
    sleeper: Callable[[float], None] = time.sleep,
    notification_sender: Callable[..., bool] = _default_notification_sender,
    normal_interval_seconds: float = DEFAULT_NORMAL_INTERVAL_SECONDS,
    fast_interval_seconds: float = DEFAULT_FAST_INTERVAL_SECONDS,
    degraded_interval_seconds: float = DEFAULT_DEGRADED_INTERVAL_SECONDS,
    freshness_seconds: float = DEFAULT_FRESHNESS_SECONDS,
    min_quote_coverage: float = DEFAULT_MIN_QUOTE_COVERAGE,
    down_threshold_pct: float = 3.0,
    up_threshold_pct: float = 5.0,
    near_level_pct: float = DEFAULT_NEAR_LEVEL_PCT,
    near_change_points: float = DEFAULT_NEAR_CHANGE_POINTS,
    fast_hold_seconds: float = DEFAULT_FAST_HOLD_SECONDS,
    low_coverage_limit: int = DEFAULT_LOW_COVERAGE_LIMIT,
    cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS,
    deterioration_pct: float = DEFAULT_DETERIORATION_PCT,
    max_cycles: int = 0,
    late_start_policy: str = "error",
) -> SessionResult:
    configured_symbols = normalize_stock_list(stocks)
    if not configured_symbols:
        raise SessionError("股票列表为空")
    if end_at.tzinfo is None:
        end_at = end_at.replace(tzinfo=SHANGHAI_TZ)
    end_at = end_at.astimezone(SHANGHAI_TZ)
    if normal_interval_seconds <= 0 or fast_interval_seconds <= 0:
        raise SessionError("监控间隔必须大于零")
    if not 0 <= min_quote_coverage <= 1:
        raise SessionError("最低行情覆盖率必须在 0 到 1 之间")
    if low_coverage_limit < 1:
        raise SessionError("连续低覆盖轮数必须至少为 1")
    if late_start_policy not in {"error", "skip"}:
        raise SessionError("late_start_policy 必须是 error 或 skip")

    started = clock().astimezone(SHANGHAI_TZ)
    try:
        # Validate the optional Secret without logging or persisting its
        # contents. Intraday decisions use only public held/candidate roles.
        load_private_watch_config()
    except ValueError:
        # A malformed optional side-account payload must never block PRIMARY.
        # Ignore its private fields without echoing any payload value.
        print(
            "::warning::旁路观察账户私密配置无效；本轮忽略私密字段，PRIMARY继续",
            file=sys.stderr,
        )
    shadow_state: Optional[MutableMapping[str, Any]] = None
    if shadow_state_path is not None:
        try:
            # Legacy argument retained for API compatibility only. PRIMARY is
            # restore-only and must never consume an initialization payload.
            shadow_state = load_or_initialize_shadow(
                shadow_state_path,
                None,
                started,
            )
        except ShadowExperimentError as exc:
            raise SessionError(f"A/B 影子账户状态无效: {exc}") from exc
    if end_at <= started:
        if late_start_policy == "error":
            raise SessionError(
                f"监控时段已过期：当前 {_iso(started)}，结束时间 {_iso(end_at)}"
            )
        state = load_state_v2(state_path, now=started)
        provider = state.setdefault("provider", {})
        delay_seconds = max(0.0, (started - end_at).total_seconds())
        provider["session_status"] = "late_schedule_skipped"
        provider["late_schedule"] = {
            "session": (
                "morning"
                if end_at.timetz().replace(tzinfo=None) <= datetime_time(12, 2)
                else "afternoon"
            ),
            "observed_at": _iso(started),
            "intended_end_at": _iso(end_at),
            "delay_seconds": delay_seconds,
            "policy": "skip_without_backfill",
        }
        notified = 0
        created = 0
        session_name = str(provider["late_schedule"]["session"])
        notification_key = f"{started.date().isoformat()}:{session_name}"
        notification_states = provider.get("late_schedule_notifications")
        if not isinstance(notification_states, MutableMapping):
            notification_states = {}
            provider["late_schedule_notifications"] = notification_states
        notification_state = notification_states.get(notification_key)
        pending_ids = {
            str(event.get("event_id") or "")
            for event in state.get("outbox", [])
            if event.get("condition") == "schedule_late"
            and event.get("schedule_key") == notification_key
        }
        should_queue = not isinstance(notification_state, MutableMapping) or (
            notification_state.get("status") != "sent"
            and str(notification_state.get("event_id") or "") not in pending_ids
        )
        if should_queue:
            event = _queue_event(
                state,
                now=started,
                symbol="SYSTEM",
                name=f"{session_name} 盘中监控",
                condition="schedule_late",
                transition="missed",
                severity="warning",
                price=None,
                change_pct=None,
                reference_price=None,
                message=(
                    f"外部 dispatch 在目标结束时间 {_iso(end_at)} 之后"
                    f" {delay_seconds / 60:.0f} 分钟才到达；本轮已安全跳过，"
                    "没有补造盘中数据，也没有产生买卖信号。"
                ),
                payload={
                    "signal_time": _iso(started),
                    "quote_time": None,
                    "data_quality": "scheduler_missed",
                    "intended_end_at": _iso(end_at),
                },
                decision_result="push_scheduler_missed_once",
            )
            event["schedule_key"] = notification_key
            notification_state = {
                "status": "pending",
                "event_id": event["event_id"],
                "queued_at": _iso(started),
            }
            notification_states[notification_key] = notification_state
            created = 1
            for stale_key in sorted(notification_states)[:-20]:
                notification_states.pop(stale_key, None)
        event_id = str(notification_state.get("event_id") or "")
        notified = flush_outbox(
            state,
            now=started,
            sender=notification_sender,
            event_predicate=lambda event: (
                event.get("condition") == "schedule_late"
                and event.get("schedule_key") == notification_key
            ),
        )
        event_pending = any(
            str(event.get("event_id") or "") == event_id
            for event in state.get("outbox", [])
        )
        if event_id and not event_pending:
            notification_state["status"] = "sent"
            notification_state["notified_at"] = _iso(started)
        state["updated_at"] = _iso(started)
        if shadow_state is not None and shadow_state_path is not None:
            record_shadow_status(
                shadow_state,
                "scheduler_missed",
                now=started,
                reason="target_session_arrived_after_end_without_backfill",
                details={"intended_end_at": _iso(end_at), "session": session_name},
            )
            save_shadow_state(shadow_state_path, shadow_state)
            if shadow_report_path is not None:
                _write_report(shadow_report_path, render_shadow_scorecard(shadow_state))
        save_state_v2(state_path, state)
        _write_report(
            report_path,
            render_session_report(
                now=started,
                symbols=configured_symbols,
                last_cycle=None,
                cycles=0,
                events_created=created,
                events_notified=notified,
                pending_events=len(state.get("outbox", [])),
                provider_status=provider,
            ),
        )
        return SessionResult(
            started_at=_iso(started),
            ended_at=_iso(started),
            cycles=0,
            quote_cycles=0,
            events_created=created,
            events_notified=notified,
            final_pending_events=len(state.get("outbox", [])),
            termination_reason="late_schedule_skipped",
            state_path=state_path,
            report_path=report_path,
        )

    candidate_plans = load_candidate_plans(candidate_plans_path, now=started)
    extra_candidates: List[str] = []
    for candidate in candidate_plans:
        try:
            symbol = canonical_symbol(
                str(candidate.get("symbol") or candidate.get("code") or "")
            )
        except Exception:
            continue
        if symbol and symbol not in configured_symbols and symbol not in extra_candidates:
            extra_candidates.append(symbol)
        if len(extra_candidates) >= MAX_EXTRA_CANDIDATE_SYMBOLS:
            break
    shadow_symbols: List[str] = []
    if shadow_state is not None:
        shadow_symbols.extend(shadow_initial_symbols(shadow_state))
        shadow_positions = (
            shadow_state.get("strategy_shadow_portfolio", {}).get("positions", {})
        )
        shadow_symbols.extend(str(symbol) for symbol in shadow_positions)
    # PRIMARY is always first.  Side accounts share one deduplicated quote
    # request but never become PRIMARY A/B positions or coverage denominators.
    symbols = list(
        dict.fromkeys(
            [
                *PRIMARY_SYMBOLS,
                *configured_symbols,
                *account_quote_symbols(),
                *extra_candidates,
                *shadow_symbols,
            ]
        )
    )

    state = load_state_v2(state_path, now=started)
    clear_removed_adaptive_plan_reviews(
        state,
        now=started,
        candidates=candidate_plans,
    )
    levels = load_reference_levels_batch(database_path, symbols, now=started)
    primary_reference_symbols = list(PRIMARY_SYMBOLS)
    covered_references = sum(
        1
        for symbol in primary_reference_symbols
        for reference in (levels.get(symbol, ReferenceLevels()),)
        if reference.stop_loss is not None or reference.target_price is not None
    )
    reference_summary = {
        "database_available": database_path.exists(),
        "covered_symbols": covered_references,
        "total_symbols": len(primary_reference_symbols),
        "coverage": (
            covered_references / len(primary_reference_symbols)
            if primary_reference_symbols
            else 0.0
        ),
        "scope": "PRIMARY_PORTFOLIO_only",
        "status": (
            "available"
            if covered_references == len(primary_reference_symbols)
            else "partial"
            if covered_references
            else "unavailable"
        ),
    }
    provider = state.setdefault("provider", {})
    provider.pop("late_schedule", None)
    provider["reference_levels"] = reference_summary
    provider["candidate_plan_monitoring"] = {
        "path": str(candidate_plans_path) if candidate_plans_path else "",
        "plan_count": len(candidate_plans),
        "extra_symbols": extra_candidates,
        "extra_symbol_limit": MAX_EXTRA_CANDIDATE_SYMBOLS,
    }
    provider.setdefault(
        "data_capabilities",
        {
            "realtime_price": "available_when_fresh",
            "provider_timestamp": "required",
            "volume": "available_when_provider_supplies",
            "intraday_kline": "not_collected_by_this_monitor",
            "level2_order_book": "unavailable",
        },
    )
    if covered_references == 0:
        _queue_reference_health_event(
            state,
            now=started,
            covered_count=0,
            total_count=len(primary_reference_symbols),
            database_available=database_path.exists(),
        )
    else:
        if provider.get("reference_alert_active"):
            _cancel_outbox_events(
                state,
                now=started,
                predicate=lambda event: (
                    event.get("symbol") == "SYSTEM"
                    and event.get("condition") == "reference_data_quality"
                ),
                reason="reference_levels_recovered_before_delivery",
            )
        provider["reference_alert_active"] = False

    quote_fetcher = fetcher or TencentBatchQuoteFetcher(
        freshness_seconds=freshness_seconds
    )
    depth_adapter = level2_adapter or Level2DataAdapter()
    cycles = 0
    quote_cycles = 0
    created_total = 0
    notified_total = 0
    last_cycle: Optional[CycleResult] = None
    termination_reason = "end_at"

    while True:
        now = clock().astimezone(SHANGHAI_TZ)
        if now >= end_at or (max_cycles and cycles >= max_cycles):
            termination_reason = "max_cycles" if max_cycles and cycles >= max_cycles else "end_at"
            break
        phases = market_phases_at(symbols, now, phase_resolver)
        provider["market_phases"] = phases
        phase_values = set(phases.values())
        if phase_values and phase_values == {"non_trading"}:
            termination_reason = "all_markets_non_trading"
            provider["session_status"] = termination_reason
            state["updated_at"] = _iso(now)
            save_state_v2(state_path, state)
            if shadow_state is not None and shadow_state_path is not None:
                save_shadow_state(shadow_state_path, shadow_state)
                if shadow_report_path is not None:
                    _write_report(shadow_report_path, render_shadow_scorecard(shadow_state))
            _write_report(
                report_path,
                render_session_report(
                    now=now,
                    symbols=symbols,
                    last_cycle=last_cycle,
                    cycles=cycles,
                    events_created=created_total,
                    events_notified=notified_total,
                    pending_events=len(state.get("outbox", [])),
                    provider_status=provider,
                ),
            )
            break
        if phase_values and phase_values.issubset({"non_trading", "unknown"}) and (
            "unknown" in phase_values
        ):
            termination_reason = "calendar_unknown_no_open_market"
            provider["session_status"] = termination_reason
            provider["calendar_degraded"] = True
            state["updated_at"] = _iso(now)
            save_state_v2(state_path, state)
            if shadow_state is not None and shadow_state_path is not None:
                record_shadow_status(
                    shadow_state,
                    "data_unavailable",
                    now=now,
                    reason="calendar_unknown_no_open_market",
                )
                save_shadow_state(shadow_state_path, shadow_state)
                if shadow_report_path is not None:
                    _write_report(shadow_report_path, render_shadow_scorecard(shadow_state))
            _write_report(
                report_path,
                render_session_report(
                    now=now,
                    symbols=symbols,
                    last_cycle=last_cycle,
                    cycles=cycles,
                    events_created=created_total,
                    events_notified=notified_total,
                    pending_events=len(state.get("outbox", [])),
                    provider_status=provider,
                ),
            )
            break
        provider["calendar_degraded"] = "unknown" in phase_values
        active = [
            canonical_symbol(symbol)
            for symbol in symbols
            if phases.get(market_for_symbol(symbol)) in _OPEN_PHASES
        ]
        cycles += 1
        interval = normal_interval_seconds
        if active:
            last_cycle = run_cycle(
                symbols=active,
                primary_symbols=[
                    symbol for symbol in active if symbol in set(PRIMARY_SYMBOLS)
                ],
                state=state,
                levels=levels,
                fetcher=quote_fetcher,
                now=now,
                min_quote_coverage=min_quote_coverage,
                down_threshold_pct=down_threshold_pct,
                up_threshold_pct=up_threshold_pct,
                near_level_pct=near_level_pct,
                near_change_points=near_change_points,
                fast_hold_seconds=fast_hold_seconds,
                normal_interval_seconds=normal_interval_seconds,
                fast_interval_seconds=fast_interval_seconds,
                degraded_interval_seconds=degraded_interval_seconds,
                low_coverage_limit=low_coverage_limit,
                cooldown_seconds=cooldown_seconds,
                deterioration_pct=deterioration_pct,
                candidate_plans=candidate_plans,
                level2_adapter=depth_adapter,
                notification_sender=notification_sender,
                shadow_state=shadow_state,
                shadow_signal_recorder=(
                    record_shadow_signal if shadow_state is not None else None
                ),
                shadow_freshness_seconds=freshness_seconds,
            )
            quote_cycles += 1
            created_total += last_cycle.new_event_count
            notified_total += last_cycle.notified_event_count
            interval = last_cycle.next_interval_seconds
        else:
            # Markets are closed/lunching/unknown.  Pending outbox events are
            # still retried without fetching prices.
            notified_total += flush_outbox(
                state, now=now, sender=notification_sender
            )
            state["updated_at"] = _iso(now)

        save_state_v2(state_path, state)
        if shadow_state is not None and shadow_state_path is not None:
            save_shadow_state(shadow_state_path, shadow_state)
            if shadow_report_path is not None:
                _write_report(shadow_report_path, render_shadow_scorecard(shadow_state))
        _write_report(
            report_path,
            render_session_report(
                now=now,
                symbols=symbols,
                last_cycle=last_cycle,
                cycles=cycles,
                events_created=created_total,
                events_notified=notified_total,
                pending_events=len(state.get("outbox", [])),
                provider_status=provider,
            ),
        )
        if max_cycles and cycles >= max_cycles:
            termination_reason = "max_cycles"
            break
        remaining = max(0.0, (end_at - clock().astimezone(SHANGHAI_TZ)).total_seconds())
        if remaining <= 0:
            break
        sleeper(min(interval, remaining))

    ended = clock().astimezone(SHANGHAI_TZ)
    provider["session_status"] = termination_reason
    state["updated_at"] = _iso(ended)
    if shadow_state is not None and shadow_state_path is not None:
        if (
            ended.date().isoformat() > SHADOW_BASELINE_DATE
            and ended.timetz().replace(tzinfo=None) >= datetime_time(16, 0)
        ):
            strategy_positions = (
                shadow_state.get("strategy_shadow_portfolio", {}).get("positions", {})
            )
            closing_symbols = list(
                dict.fromkeys(
                    [*shadow_initial_symbols(shadow_state), *strategy_positions.keys()]
                )
            )
            closing_quotes: Dict[str, Any] = dict(
                shadow_state.get("strategy_shadow_portfolio", {}).get(
                    "latest_quotes", {}
                )
            )
            try:
                fetched_close = quote_fetcher.fetch(closing_symbols, now=ended)
                closing_quotes.update(fetched_close)
            except Exception as exc:
                record_shadow_status(
                    shadow_state,
                    "data_unavailable",
                    now=ended,
                    reason="closing_quote_fetch_failed",
                    details={"error_type": type(exc).__name__},
                )
            execute_shadow_pending(
                shadow_state,
                {
                    symbol: quote
                    for symbol, quote in closing_quotes.items()
                    if symbol
                    not in (set(watch_contexts_by_symbol()) - set(PRIMARY_SYMBOLS))
                },
                now=ended,
                freshness_seconds=freshness_seconds,
                session_closed=True,
            )
            record_shadow_daily_nav(
                shadow_state,
                ended.date().isoformat(),
                closing_quotes,
                recorded_at=ended,
            )
            _notify_shadow_scorecard(
                shadow_state,
                now=ended,
                sender=notification_sender,
            )
        save_shadow_state(shadow_state_path, shadow_state)
        if shadow_report_path is not None:
            _write_report(shadow_report_path, render_shadow_scorecard(shadow_state))
    save_state_v2(state_path, state)
    _write_report(
        report_path,
        render_session_report(
            now=ended,
            symbols=symbols,
            last_cycle=last_cycle,
            cycles=cycles,
            events_created=created_total,
            events_notified=notified_total,
            pending_events=len(state.get("outbox", [])),
            provider_status=provider,
        ),
    )
    return SessionResult(
        started_at=_iso(started),
        ended_at=_iso(ended),
        cycles=cycles,
        quote_cycles=quote_cycles,
        events_created=created_total,
        events_notified=notified_total,
        final_pending_events=len(state.get("outbox", [])),
        termination_reason=termination_reason,
        state_path=state_path,
        report_path=report_path,
    )


def resolve_session_end(now: datetime, session: str) -> datetime:
    local = now.astimezone(SHANGHAI_TZ)
    normalized = session.strip().lower()
    if normalized == "auto":
        # 12:02–12:29 used to select an already-expired morning window and
        # return a misleading zero-loop success.
        normalized = "morning" if local.time() < datetime_time(12, 2) else "afternoon"
    end_time = {
        "morning": datetime_time(12, 2),
        "afternoon": datetime_time(16, 2),
    }.get(normalized)
    if end_time is None:
        raise SessionError("session 必须是 auto、morning 或 afternoon")
    return datetime.combine(local.date(), end_time, tzinfo=SHANGHAI_TZ)


def _positive_number(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("必须是数字") from exc
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("必须是大于零的有限数字")
    return value


def _coverage(raw: str) -> float:
    value = _finite_float(raw)
    if value is None or not 0 <= value <= 1:
        raise argparse.ArgumentTypeError("必须在 0 到 1 之间")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stocks", required=True, help="逗号或空格分隔的 A/H 股票代码")
    parser.add_argument(
        "--session",
        choices=("auto", "morning", "afternoon"),
        default="auto",
    )
    parser.add_argument("--db", type=Path, default=Path("data/stock_analysis.db"))
    parser.add_argument(
        "--state", type=Path, default=Path("data/intraday/session_state.json")
    )
    parser.add_argument(
        "--report", type=Path, default=Path("reports/intraday_session.md")
    )
    parser.add_argument(
        "--candidate-plans",
        type=Path,
        default=None,
        help="可选：只读 simulation/watchlist 候选计划 JSON",
    )
    parser.add_argument(
        "--shadow-state",
        type=Path,
        default=None,
        help="可选：加密运行环境中解密后的 A/B 影子账户状态路径",
    )
    parser.add_argument(
        "--shadow-report",
        type=Path,
        default=None,
        help="可选：私密 A/B 每日成绩单路径",
    )
    parser.add_argument(
        "--normal-interval", type=_positive_number, default=DEFAULT_NORMAL_INTERVAL_SECONDS
    )
    parser.add_argument(
        "--fast-interval", type=_positive_number, default=DEFAULT_FAST_INTERVAL_SECONDS
    )
    parser.add_argument(
        "--degraded-interval",
        type=_positive_number,
        default=DEFAULT_DEGRADED_INTERVAL_SECONDS,
    )
    parser.add_argument(
        "--freshness-seconds", type=_positive_number, default=DEFAULT_FRESHNESS_SECONDS
    )
    parser.add_argument(
        "--min-quote-coverage", type=_coverage, default=DEFAULT_MIN_QUOTE_COVERAGE
    )
    parser.add_argument("--down-threshold", type=_positive_number, default=3.0)
    parser.add_argument("--up-threshold", type=_positive_number, default=5.0)
    parser.add_argument("--near-level-pct", type=_positive_number, default=0.75)
    parser.add_argument("--near-change-points", type=_positive_number, default=0.5)
    parser.add_argument("--fast-hold-seconds", type=_positive_number, default=300.0)
    parser.add_argument("--low-coverage-limit", type=int, default=3)
    parser.add_argument("--cooldown-seconds", type=_positive_number, default=900.0)
    parser.add_argument("--deterioration-pct", type=_positive_number, default=1.0)
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=0,
        help="测试用；0 表示持续到绝对结束时间",
    )
    parser.add_argument(
        "--late-start-policy",
        choices=("error", "skip"),
        default="error",
        help="计划任务迟到且时段已结束时：报错或保留状态后安全跳过",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    now = datetime.now(SHANGHAI_TZ)
    try:
        result = run_session(
            stocks=args.stocks,
            end_at=resolve_session_end(now, args.session),
            database_path=args.db,
            state_path=args.state,
            report_path=args.report,
            candidate_plans_path=args.candidate_plans,
            shadow_state_path=args.shadow_state,
            shadow_report_path=args.shadow_report,
            normal_interval_seconds=args.normal_interval,
            fast_interval_seconds=args.fast_interval,
            degraded_interval_seconds=args.degraded_interval,
            freshness_seconds=args.freshness_seconds,
            min_quote_coverage=args.min_quote_coverage,
            down_threshold_pct=args.down_threshold,
            up_threshold_pct=args.up_threshold,
            near_level_pct=args.near_level_pct,
            near_change_points=args.near_change_points,
            fast_hold_seconds=args.fast_hold_seconds,
            low_coverage_limit=args.low_coverage_limit,
            cooldown_seconds=args.cooldown_seconds,
            deterioration_pct=args.deterioration_pct,
            max_cycles=max(0, args.max_cycles),
            late_start_policy=args.late_start_policy,
        )
    except SessionError as exc:
        print(f"分钟级盘中监控失败: {exc}", file=sys.stderr)
        return 2
    if result.termination_reason == "late_schedule_skipped":
        message = (
            "分钟级盘中监控未执行：计划任务到达过晚，已保留状态并安全跳过；"
            f"pending={result.final_pending_events} state={result.state_path}"
        )
        print(message)
        if os.getenv("GITHUB_ACTIONS", "").strip().lower() == "true":
            print(f"::warning title=盘中模拟监控未执行::{message}")
            summary_path = os.getenv("GITHUB_STEP_SUMMARY", "").strip()
            if summary_path:
                try:
                    with Path(summary_path).open("a", encoding="utf-8") as handle:
                        handle.write("## ⚠️ 盘中模拟监控未执行\n\n")
                        handle.write(message + "\n")
                except OSError as exc:
                    print(f"写入 GitHub 摘要失败: {exc}", file=sys.stderr)
    else:
        print(
            "分钟级盘中监控完成: "
            f"cycles={result.cycles} quote_cycles={result.quote_cycles} "
            f"events_created={result.events_created} "
            f"events_notified={result.events_notified} "
            f"pending={result.final_pending_events} state={result.state_path}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
