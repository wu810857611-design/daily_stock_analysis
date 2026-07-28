#!/usr/bin/env python3
"""Export one simulation-only signal snapshot from the analysis SQLite database.

The generated JSON is an input for ``scripts/paper_trade_tracker.py``.  This
script never connects to a broker and never submits real orders.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sqlite3
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


DEFAULT_DATABASE = "data/stock_analysis.db"
DEFAULT_MIN_COVERAGE = 0.8


class ExportError(RuntimeError):
    """Raised when the export cannot produce a safe tracker snapshot."""


class CoverageError(ExportError):
    """Raised when usable symbol coverage is below the configured threshold."""

    def __init__(self, snapshot: Dict[str, Any]):
        metadata = snapshot["metadata"]
        super().__init__(
            "usable coverage "
            f"{metadata['coverage']:.2%} is below minimum {metadata['min_coverage']:.2%}"
        )
        self.snapshot = snapshot


def canonicalize_symbol(raw: Any, market: Any = None) -> str:
    """Canonicalise common aliases, using ``HKxxxxx`` for Hong Kong stocks."""

    symbol = str(raw or "").strip().upper()
    market_name = str(market or "").strip().lower()
    if not symbol:
        return ""

    match = re.fullmatch(r"HK\.?(\d{1,5})", symbol)
    if match:
        return f"HK{match.group(1).zfill(5)}"
    match = re.fullmatch(r"(\d{1,5})\.HK", symbol)
    if match:
        return f"HK{match.group(1).zfill(5)}"
    if market_name == "hk" and symbol.isdigit() and 1 <= len(symbol) <= 5:
        return f"HK{symbol.zfill(5)}"
    if symbol.isdigit() and 4 <= len(symbol) <= 5:
        return f"HK{symbol.zfill(5)}"

    match = re.fullmatch(r"(?:SH|SS|SZ|BJ)\.?(\d{6})", symbol)
    if match:
        return match.group(1)
    match = re.fullmatch(r"(\d{6})\.(?:SH|SS|SZ|BJ)", symbol)
    if match:
        return match.group(1)
    return symbol


def parse_stocks(raw: str) -> List[str]:
    """Parse and de-duplicate a user-facing comma/whitespace separated list."""

    symbols: List[str] = []
    seen = set()
    for item in re.split(r"[\s,;，、；]+", raw or ""):
        symbol = canonicalize_symbol(item)
        if symbol and symbol not in seen:
            seen.add(symbol)
            symbols.append(symbol)
    if not symbols:
        raise ExportError("--stocks must contain at least one stock code")
    return symbols


def normalise_trade_date(raw: str) -> str:
    try:
        return date.fromisoformat(str(raw or "").strip()).isoformat()
    except ValueError as exc:
        raise ExportError("--trade-date must use YYYY-MM-DD") from exc


def map_action(raw: Any) -> str:
    """Collapse report actions to tracker actions, defaulting ambiguity to hold."""

    text = str(raw or "").strip().lower().replace("_", " ").replace("-", " ")
    text = re.sub(r"\s+", " ", text)
    if not text:
        return "hold"

    # Negated instructions must win before positive token matching.
    hold_phrases = (
        "不建议买入",
        "避免买入",
        "暂不买入",
        "不要买入",
        "不宜买入",
        "不建议卖出",
        "暂不卖出",
        "不要卖出",
        "无需卖出",
        "观望",
        "观察",
        "等待",
        "持有",
        "中性",
        "neutral",
        "watch",
        "wait",
        "hold",
    )
    if any(phrase in text for phrase in hold_phrases):
        return "hold"

    sell_phrases = (
        "强烈卖出",
        "卖出",
        "清仓",
        "减仓",
        "止损",
        "离场",
        "退出",
        "strong sell",
        "sell",
        "reduce",
        "trim",
        "close",
        "exit",
        "avoid",
        "alert",
    )
    if any(phrase in text for phrase in sell_phrases):
        return "sell"

    buy_phrases = (
        "强烈买入",
        "买入",
        "建仓",
        "加仓",
        "增持",
        "布局",
        "strong buy",
        "buy",
        "add",
        "accumulate",
    )
    if any(phrase in text for phrase in buy_phrases):
        return "buy"
    return "hold"


def _positive_number(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number <= 0:
        return None
    return number


def _json_object(value: Any) -> Dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _nested(payload: Mapping[str, Any], path: Iterable[str]) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _first_nested(payload: Mapping[str, Any], paths: Iterable[Tuple[str, ...]]) -> Any:
    for path in paths:
        value = _nested(payload, path)
        if value not in (None, ""):
            return value
    return None


_CONTEXT_PRICE_PATHS = (
    ("enhanced_context", "realtime", "price"),
    ("realtime_quote_raw", "price"),
    ("realtime_quote", "price"),
    ("enhanced_context", "today", "close"),
    ("enhanced_context", "today", "price"),
    ("today", "close"),
    ("today", "price"),
)
_RAW_PRICE_PATHS = (
    ("current_price",),
    ("reference_price",),
    ("close",),
    ("price",),
    ("dashboard", "current_price"),
)
_RAW_ACTION_PATHS = (
    ("operation_advice",),
    ("action",),
    ("decision_type",),
    ("recommendation",),
    ("dashboard", "operation_advice"),
    ("dashboard", "action"),
)
_RAW_NAME_PATHS = (
    ("name",),
    ("stock_name",),
    ("enhanced_context", "stock_name"),
)


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _load_analysis_rows(
    connection: sqlite3.Connection,
    trade_date: str,
    requested: set[str],
) -> Dict[str, sqlite3.Row]:
    if not _table_exists(connection, "analysis_history"):
        return {}
    rows = connection.execute(
        """
        SELECT id, code, name, operation_advice, analysis_summary,
               raw_result, context_snapshot, created_at
        FROM analysis_history
        WHERE date(created_at) = ?
        ORDER BY created_at DESC, id DESC
        """,
        (trade_date,),
    )
    latest: Dict[str, sqlite3.Row] = {}
    for row in rows:
        symbol = canonicalize_symbol(row["code"])
        if symbol in requested and symbol not in latest:
            latest[symbol] = row
    return latest


def _load_decision_rows(
    connection: sqlite3.Connection,
    trade_date: str,
    requested: set[str],
) -> Dict[str, sqlite3.Row]:
    if not _table_exists(connection, "decision_signals"):
        return {}

    if _table_exists(connection, "analysis_history"):
        query = """
            SELECT ds.id, ds.stock_code, ds.stock_name, ds.market, ds.action,
                   ds.action_label, ds.reason, ds.created_at,
                   ah.name AS joined_name,
                   ah.operation_advice AS joined_operation_advice,
                   ah.analysis_summary AS joined_analysis_summary,
                   ah.raw_result AS joined_raw_result,
                   ah.context_snapshot AS joined_context_snapshot
            FROM decision_signals AS ds
            LEFT JOIN analysis_history AS ah ON ah.id = ds.source_report_id
            WHERE date(ds.created_at) = ?
            ORDER BY ds.created_at DESC, ds.id DESC
        """
    else:
        query = """
            SELECT ds.id, ds.stock_code, ds.stock_name, ds.market, ds.action,
                   ds.action_label, ds.reason, ds.created_at,
                   NULL AS joined_name,
                   NULL AS joined_operation_advice,
                   NULL AS joined_analysis_summary,
                   NULL AS joined_raw_result,
                   NULL AS joined_context_snapshot
            FROM decision_signals AS ds
            WHERE date(ds.created_at) = ?
            ORDER BY ds.created_at DESC, ds.id DESC
        """

    latest: Dict[str, sqlite3.Row] = {}
    for row in connection.execute(query, (trade_date,)):
        symbol = canonicalize_symbol(row["stock_code"], row["market"])
        if symbol in requested and symbol not in latest:
            latest[symbol] = row
    return latest


def _load_close_prices(
    connection: sqlite3.Connection,
    trade_date: str,
    requested: set[str],
) -> Tuple[Dict[str, Tuple[float, str]], Dict[str, str]]:
    if not _table_exists(connection, "stock_daily"):
        return {}, {}
    prices: Dict[str, Tuple[float, str]] = {}
    stale_dates: Dict[str, str] = {}
    rows = connection.execute(
        """
        SELECT id, code, date, close
        FROM stock_daily
        WHERE date <= ?
        ORDER BY date DESC, id DESC
        """,
        (trade_date,),
    )
    for row in rows:
        symbol = canonicalize_symbol(row["code"])
        if symbol not in requested or symbol in prices:
            continue
        price = _positive_number(row["close"])
        price_date = str(row["date"])
        if price is not None and price_date == trade_date:
            prices[symbol] = (price, f"stock_daily:{row['date']}")
        elif price is not None and symbol not in stale_dates:
            stale_dates[symbol] = price_date
    return prices, stale_dates


def _analysis_values(row: sqlite3.Row) -> Dict[str, Any]:
    raw_result = _json_object(row["raw_result"])
    context_snapshot = _json_object(row["context_snapshot"])
    return {
        "raw_action": row["operation_advice"] or _first_nested(raw_result, _RAW_ACTION_PATHS),
        "name": row["name"] or _first_nested(raw_result, _RAW_NAME_PATHS) or "",
        "reason": row["analysis_summary"] or "",
        "context_snapshot": context_snapshot,
        "raw_result": raw_result,
    }


def _decision_values(row: sqlite3.Row) -> Dict[str, Any]:
    raw_result = _json_object(row["joined_raw_result"])
    context_snapshot = _json_object(row["joined_context_snapshot"])
    return {
        "raw_action": row["action"] or row["joined_operation_advice"],
        "name": row["stock_name"] or row["joined_name"] or _first_nested(raw_result, _RAW_NAME_PATHS) or "",
        "reason": row["reason"] or row["action_label"] or row["joined_analysis_summary"] or "",
        "context_snapshot": context_snapshot,
        "raw_result": raw_result,
    }


def _merge_analysis_fallback(
    decision_values: Dict[str, Any],
    analysis_row: Optional[sqlite3.Row],
) -> Dict[str, Any]:
    """Fill an unjoined decision signal from the latest same-day stock report."""

    if analysis_row is None:
        return decision_values
    fallback = _analysis_values(analysis_row)
    merged = dict(decision_values)
    for key in ("raw_action", "name", "reason"):
        if merged.get(key) in (None, ""):
            merged[key] = fallback.get(key)
    for key in ("context_snapshot", "raw_result"):
        if not merged.get(key):
            merged[key] = fallback.get(key)
    return merged


def _fallback_price(values: Mapping[str, Any]) -> Tuple[Optional[float], Optional[str]]:
    context_price = _positive_number(
        _first_nested(values.get("context_snapshot", {}), _CONTEXT_PRICE_PATHS)
    )
    if context_price is not None:
        return context_price, "analysis_history:context_snapshot"
    raw_price = _positive_number(_first_nested(values.get("raw_result", {}), _RAW_PRICE_PATHS))
    if raw_price is not None:
        return raw_price, "analysis_history:raw_result"
    return None, None


def build_snapshot(
    connection: sqlite3.Connection,
    *,
    stocks: Sequence[str],
    trade_date: str,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
) -> Dict[str, Any]:
    """Build one tracker-compatible snapshot or raise ``CoverageError``."""

    if not 0 < min_coverage <= 1:
        raise ExportError("--min-coverage must be greater than 0 and at most 1")
    trade_date = normalise_trade_date(trade_date)
    requested = parse_stocks(",".join(str(item) for item in stocks))
    requested_set = set(requested)

    connection.row_factory = sqlite3.Row
    analysis_rows = _load_analysis_rows(connection, trade_date, requested_set)
    decision_rows = _load_decision_rows(connection, trade_date, requested_set)
    daily_prices, stale_price_dates = _load_close_prices(connection, trade_date, requested_set)

    signals: List[Dict[str, Any]] = []
    missing_details: List[Dict[str, Any]] = []
    source_counts = {"decision_signals": 0, "analysis_history": 0}

    for symbol in requested:
        source = ""
        values: Optional[Dict[str, Any]] = None
        if symbol in decision_rows:
            source = "decision_signals"
            values = _merge_analysis_fallback(
                _decision_values(decision_rows[symbol]),
                analysis_rows.get(symbol),
            )
        elif symbol in analysis_rows:
            source = "analysis_history"
            values = _analysis_values(analysis_rows[symbol])

        reasons: List[str] = []
        if values is None:
            reasons.append("no_signal_or_analysis_for_trade_date")
        elif values.get("raw_action") in (None, ""):
            reasons.append("no_action_in_signal_or_analysis")

        price: Optional[float] = None
        price_source: Optional[str] = None
        if symbol in daily_prices:
            price, price_source = daily_prices[symbol]
        elif values is not None:
            price, price_source = _fallback_price(values)
        if price is None:
            if symbol in stale_price_dates:
                reasons.append(f"stale_stock_daily_price:{stale_price_dates[symbol]}")
            else:
                reasons.append("no_reference_price_for_trade_date")

        if reasons:
            missing_details.append({"symbol": symbol, "reasons": reasons})
            continue

        source_counts[source] += 1
        signals.append(
            {
                "symbol": symbol,
                "name": str(values.get("name") or "").strip(),
                "signal": map_action(values.get("raw_action")),
                "close": price,
                "reason": str(values.get("reason") or "").strip(),
                "source": source,
                "price_source": price_source,
            }
        )

    coverage = len(signals) / len(requested)
    snapshot = {
        "trade_date": trade_date,
        "signals": signals,
        "metadata": {
            "simulation_only": True,
            "places_real_orders": False,
            "requested_symbols": requested,
            "covered_symbols": [signal["symbol"] for signal in signals],
            "missing_symbols": [item["symbol"] for item in missing_details],
            "missing_details": missing_details,
            "requested_count": len(requested),
            "covered_count": len(signals),
            "coverage": coverage,
            "min_coverage": float(min_coverage),
            "source_counts": source_counts,
        },
    }
    if coverage < min_coverage:
        raise CoverageError(snapshot)
    return snapshot


def _write_snapshot(path: str, snapshot: Mapping[str, Any]) -> None:
    content = json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if path == "-":
        sys.stdout.write(content)
        return
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Export one simulation-only daily signal snapshot for paper_trade_tracker.py; "
            "never places real orders."
        )
    )
    parser.add_argument("--db", default=DEFAULT_DATABASE, help="analysis SQLite database path")
    parser.add_argument("--stocks", required=True, help="comma-separated stock codes")
    parser.add_argument("--trade-date", required=True, help="trade date in YYYY-MM-DD form")
    parser.add_argument(
        "--min-coverage",
        type=float,
        default=DEFAULT_MIN_COVERAGE,
        help="minimum usable requested-symbol fraction (0, 1], default: 0.8",
    )
    parser.add_argument("--output", default="-", help="output JSON path, or - for stdout")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    database_path = Path(args.db)
    if not database_path.is_file():
        print(f"paper-signal export error: database not found: {database_path}", file=sys.stderr)
        return 2

    try:
        database_uri = f"{database_path.resolve().as_uri()}?mode=ro"
        with sqlite3.connect(database_uri, uri=True) as connection:
            snapshot = build_snapshot(
                connection,
                stocks=parse_stocks(args.stocks),
                trade_date=args.trade_date,
                min_coverage=args.min_coverage,
            )
        _write_snapshot(args.output, snapshot)
    except CoverageError as exc:
        _write_snapshot(args.output, exc.snapshot)
        print(f"paper-signal export error: {exc}", file=sys.stderr)
        return 2
    except (ExportError, OSError, sqlite3.Error) as exc:
        print(f"paper-signal export error: {exc}", file=sys.stderr)
        return 2

    metadata = snapshot["metadata"]
    print(
        "paper-signal export: "
        f"{metadata['covered_count']}/{metadata['requested_count']} symbols "
        f"({metadata['coverage']:.2%} coverage)",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
