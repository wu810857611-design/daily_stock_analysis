#!/usr/bin/env python3
"""Deterministic, simulation-only shadow portfolio A/B experiment.

The module contains no broker integration and performs no network requests.  A
caller supplies the encrypted/private initial portfolio, fresh intraday quotes,
and reliable daily closing quotes.  The immutable ledgers then compare a fixed
buy-and-hold account with a strategy account that follows every recorded
simulation signal.

Formal values are reported in CNY using the experiment's immutable baseline
HKD/CNY conversion.  Original CNY and HKD components remain available for
audit, while the strategy account uses one CNY purchasing-power balance.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import tempfile
from datetime import date, datetime, time as datetime_time
from pathlib import Path
from typing import Any, Dict, Mapping, MutableMapping, Optional, Sequence, Tuple, Union
from zoneinfo import ZoneInfo


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
SCHEMA_VERSION = 2
BASELINE_DATE = "2026-08-07"
CHECKPOINT_DAYS = 20
FORMAL_EVALUATION_DAYS = 60
HKD_CNY_BASELINE_FX = 0.8865
VALUATION_MODE = "fixed_baseline_fx_to_cny"

# Public market metadata only.  Actual quantities and historical costs must be
# supplied privately at runtime and must not be committed to a public repo.
INITIAL_INSTRUMENTS: Tuple[Mapping[str, Any], ...] = (
    {"symbol": "688333", "name": "铂力特", "currency": "CNY", "verified_close": 110.34, "quote_time": "2026-08-07T16:14:35+08:00"},
    {"symbol": "300499", "name": "高澜股份", "currency": "CNY", "verified_close": 29.10, "quote_time": "2026-08-07T16:14:39+08:00"},
    {"symbol": "688608", "name": "恒玄科技", "currency": "CNY", "verified_close": 112.31, "quote_time": "2026-08-07T16:14:54+08:00"},
    {"symbol": "300408", "name": "三环集团", "currency": "CNY", "verified_close": 127.39, "quote_time": "2026-08-07T16:14:42+08:00"},
    {"symbol": "002185", "name": "华天科技", "currency": "CNY", "verified_close": 17.98, "quote_time": "2026-08-07T16:14:36+08:00"},
    {"symbol": "688135", "name": "利扬芯片", "currency": "CNY", "verified_close": 24.90, "quote_time": "2026-08-07T16:14:57+08:00"},
    {"symbol": "600094", "name": "大名城", "currency": "CNY", "verified_close": 4.46, "quote_time": "2026-08-07T16:14:48+08:00"},
    {"symbol": "601318", "name": "中国平安", "currency": "CNY", "verified_close": 53.38, "quote_time": "2026-08-07T16:14:35+08:00"},
    {"symbol": "302132", "name": "中航成飞", "currency": "CNY", "verified_close": 56.96, "quote_time": "2026-08-07T16:14:27+08:00"},
    {"symbol": "HK01347", "name": "华虹宏力", "currency": "HKD", "verified_close": 141.20, "quote_time": "2026-08-07T16:08:18+08:00"},
    {"symbol": "HK00981", "name": "中芯国际", "currency": "HKD", "verified_close": 66.90, "quote_time": "2026-08-07T16:08:24+08:00"},
    {"symbol": "HK06181", "name": "老铺黄金", "currency": "HKD", "verified_close": 352.00, "quote_time": "2026-08-07T16:08:24+08:00"},
    {"symbol": "HK02522", "name": "一脉阳光", "currency": "HKD", "verified_close": 4.775, "quote_time": "2026-08-07T16:08:24+08:00"},
    {"symbol": "HK06166", "name": "剑桥科技", "currency": "HKD", "verified_close": 84.65, "quote_time": "2026-08-07T16:08:24+08:00"},
)

DEFAULT_COSTS = {
    "entry_fee_bps": 10.0,
    "exit_fee_bps": 10.0,
    "entry_slippage_bps": 5.0,
    "exit_slippage_bps": 5.0,
    "source": "simulation_assumption",
}

_ACTION_ALIASES = {
    "buy_0_25": "buy_0.25_cheng",
    "buy_0.25_cheng": "buy_0.25_cheng",
    "模拟买入0.25成": "buy_0.25_cheng",
    "buy_0_5": "buy_0.5_cheng",
    "buy_0.5_cheng": "buy_0.5_cheng",
    "模拟买入0.5成": "buy_0.5_cheng",
    "add_0_25": "add_0.25_cheng",
    "add_0.25_cheng": "add_0.25_cheng",
    "模拟加仓0.25成": "add_0.25_cheng",
    "add_0_5": "add_0.5_cheng",
    "add_0.5_cheng": "add_0.5_cheng",
    "模拟加仓0.5成": "add_0.5_cheng",
    "reduce_1_4": "reduce_1_4",
    "模拟减仓1/4": "reduce_1_4",
    "reduce_1_3": "reduce_1_3",
    "模拟减仓1/3": "reduce_1_3",
    "reduce_1_2": "reduce_1_2",
    "模拟减仓1/2": "reduce_1_2",
    "clear": "clear",
    "模拟清仓": "clear",
}
_BUY_NAV_FRACTIONS = {
    "buy_0.25_cheng": 0.025,
    "buy_0.5_cheng": 0.05,
    "add_0.25_cheng": 0.025,
    "add_0.5_cheng": 0.05,
}
_SELL_POSITION_FRACTIONS = {
    "reduce_1_4": 0.25,
    "reduce_1_3": 1.0 / 3.0,
    "reduce_1_2": 0.5,
    "clear": 1.0,
}


class ExperimentInputError(ValueError):
    """Raised when an input would make the experiment unsafe or ambiguous."""


def initial_symbols(state: Optional[Mapping[str, Any]] = None) -> Tuple[str, ...]:
    """Return the immutable initial-universe symbols in repository format."""

    if state is None:
        return tuple(item["symbol"] for item in INITIAL_INSTRUMENTS)
    positions = state.get("initial_positions")
    if not isinstance(positions, Mapping):
        raise ExperimentInputError("state.initial_positions must be an object")
    return tuple(
        item["symbol"] for item in INITIAL_INSTRUMENTS if item["symbol"] in positions
    )


def _finite(value: Any, field: str, *, positive: bool = False) -> float:
    if isinstance(value, bool):
        raise ExperimentInputError(f"{field} must be a finite number")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ExperimentInputError(f"{field} must be a finite number") from exc
    if not math.isfinite(number) or (positive and number <= 0):
        qualifier = "positive " if positive else ""
        raise ExperimentInputError(f"{field} must be a {qualifier}finite number")
    return number


def _parse_datetime(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ExperimentInputError(f"{field} must be an ISO-8601 datetime") from exc
    if parsed.tzinfo is None:
        raise ExperimentInputError(f"{field} must include a timezone")
    return parsed.astimezone(SHANGHAI_TZ)


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=SHANGHAI_TZ)
    return value.astimezone(SHANGHAI_TZ).isoformat(timespec="seconds")


def _canonical_symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper().replace("HK.", "HK")
    if symbol.endswith(".HK"):
        symbol = "HK" + symbol[:-3].zfill(5)
    if symbol.startswith("HK") and symbol[2:].isdigit():
        return "HK" + symbol[2:].zfill(5)
    return symbol


def _currency_fx(currency: Any) -> float:
    normalized = str(currency or "").upper()
    if normalized == "CNY":
        return 1.0
    if normalized == "HKD":
        return HKD_CNY_BASELINE_FX
    raise ExperimentInputError(f"unsupported experiment currency: {normalized}")


def _amount_cny(amount: Any, currency: Any, field: str = "amount") -> float:
    return _finite(amount, field) * _currency_fx(currency)


def _nav_cny(by_currency: Mapping[str, Any]) -> float:
    return sum(
        _amount_cny(amount, currency, f"nav_by_currency.{currency}")
        for currency, amount in by_currency.items()
    )


def _portfolio_payload(value: Union[Mapping[str, Any], str, Path]) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, Path):
        source = value.read_text(encoding="utf-8")
    elif isinstance(value, str) and value.lstrip().startswith(("{", "[")):
        source = value
    else:
        source = Path(str(value)).read_text(encoding="utf-8")
    try:
        payload = json.loads(source)
    except json.JSONDecodeError as exc:
        raise ExperimentInputError(f"invalid initial portfolio JSON: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise ExperimentInputError("initial portfolio JSON must be an object")
    return payload


def _position_records(payload: Mapping[str, Any]) -> Sequence[Mapping[str, Any]]:
    raw = payload.get("positions", payload)
    if isinstance(raw, Mapping):
        records = []
        for symbol, details in raw.items():
            if not isinstance(details, Mapping):
                raise ExperimentInputError(f"position {symbol} must be an object")
            records.append({"symbol": symbol, **details})
        return records
    if isinstance(raw, list) and all(isinstance(item, Mapping) for item in raw):
        return raw
    raise ExperimentInputError("positions must be a list or symbol-keyed object")


def initialize_state(
    portfolio: Union[Mapping[str, Any], str, Path],
    *,
    created_at: Optional[datetime] = None,
    default_costs: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Create equal A/B accounts from a private 14-position payload."""

    payload = _portfolio_payload(portfolio)
    metadata = {item["symbol"]: item for item in INITIAL_INSTRUMENTS}
    supplied: Dict[str, Dict[str, Any]] = {}
    for record in _position_records(payload):
        symbol = _canonical_symbol(record.get("symbol"))
        if symbol not in metadata:
            raise ExperimentInputError(f"unexpected initial symbol: {symbol or '<missing>'}")
        if symbol in supplied:
            raise ExperimentInputError(f"duplicate initial symbol: {symbol}")
        quantity = _finite(record.get("quantity"), f"{symbol}.quantity", positive=True)
        historical_cost = _finite(
            record.get("historical_cost", record.get("cost")),
            f"{symbol}.historical_cost",
            positive=True,
        )
        instrument = metadata[symbol]
        supplied[symbol] = {
            "symbol": symbol,
            "name": instrument["name"],
            "currency": instrument["currency"],
            "quantity": quantity,
            "historical_cost": historical_cost,
            "experiment_basis_price": float(instrument["verified_close"]),
            "baseline_quote_time": instrument["quote_time"],
            "baseline_quote_source": "tencent_batch_verified",
        }
    missing = sorted(set(metadata) - set(supplied))
    if missing:
        raise ExperimentInputError(f"initial portfolio is missing symbols: {','.join(missing)}")

    by_currency = {"CNY": 0.0, "HKD": 0.0}
    for position in supplied.values():
        by_currency[position["currency"]] += (
            position["quantity"] * position["experiment_basis_price"]
        )
    initial_nav_mixed = sum(by_currency.values())
    initial_nav = _nav_cny(by_currency)
    costs = _normalise_costs(default_costs or DEFAULT_COSTS)
    now = created_at or datetime.now(SHANGHAI_TZ)
    state = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": "actual-core-holdings-2026-08-07",
        "baseline_date": BASELINE_DATE,
        "simulation_only": True,
        "human_review_required": True,
        "places_real_orders": False,
        "broker_connected": False,
        "created_at": _iso(now),
        "valuation_assumption": {
            "mode": VALUATION_MODE,
            "baseline_date": BASELINE_DATE,
            "hkd_cny": HKD_CNY_BASELINE_FX,
            "locked": True,
            "description": "Formal NAV uses the immutable 2026-08-07 HKD/CNY baseline FX; local-currency components are retained for audit.",
        },
        "config": {
            "checkpoint_days": CHECKPOINT_DAYS,
            "formal_evaluation_days": FORMAL_EVALUATION_DAYS,
            "default_costs": costs,
            "purchasing_power_assumption": "simulation assumption: unified CNY purchasing power with fixed baseline FX",
            "share_unit_assumptions": {
                "a_share_buy_lot": 100,
                "a_share_sell_tail_allowed": True,
                "hk_board_lot": "not_modeled",
            },
        },
        "initial_positions": supplied,
        "initial_nav": initial_nav,
        "initial_nav_cny": initial_nav,
        "initial_nav_by_currency": by_currency,
        "initial_nav_mixed_local_currency_1_to_1": initial_nav_mixed,
        "buy_and_hold_baseline": {
            "positions": copy.deepcopy(supplied),
            "cash_by_currency": {"CNY": 0.0, "HKD": 0.0},
        },
        "strategy_shadow_portfolio": {
            "positions": copy.deepcopy(supplied),
            "cash_by_currency": {"CNY": 0.0, "HKD": 0.0},
            "cash_cny": 0.0,
            "last_prices": {
                symbol: float(position["experiment_basis_price"])
                for symbol, position in supplied.items()
            },
            "latest_quotes": {},
        },
        "event_ledger": [],
        "signal_ledger": [],
        "execution_ledger": [],
        "trades": [],
        "pending_signal_ids": [],
        "daily_nav": [],
        "status_ledger": [],
        "metrics": {
            "completed_trading_days": 0,
            "phase": "before_20_day_checkpoint",
            "buy_and_hold_peak": initial_nav,
            "strategy_peak": initial_nav,
            "buy_and_hold_max_drawdown": 0.0,
            "strategy_max_drawdown": 0.0,
        },
    }
    _validate_state(state)
    return state


def _migrate_trade_to_cny(trade: MutableMapping[str, Any]) -> None:
    """Add formal CNY audit fields without changing an immutable trade fact."""

    currency = str(trade.get("currency") or "").upper()
    fx = _currency_fx(currency)
    trade.setdefault("fx_to_cny", fx)
    for local_field, cny_field in (
        ("gross_amount", "gross_amount_cny"),
        ("transaction_cost", "transaction_cost_cny"),
        ("slippage", "slippage_cny"),
        ("market_move_from_signal", "market_move_from_signal_cny"),
    ):
        if local_field in trade:
            trade.setdefault(cny_field, float(trade[local_field]) * fx)


def _migrate_daily_nav_to_cny(
    state: MutableMapping[str, Any], record: MutableMapping[str, Any]
) -> None:
    """Correct derived v1 score fields while preserving their mixed-unit audit."""

    baseline_parts = record.get("buy_and_hold_nav_by_currency") or {}
    strategy_parts = record.get("strategy_nav_by_currency") or {}
    record.setdefault(
        "buy_and_hold_nav_mixed_local_currency_1_to_1",
        float(record.get("buy_and_hold_nav", sum(baseline_parts.values()))),
    )
    record.setdefault(
        "strategy_nav_mixed_local_currency_1_to_1",
        float(record.get("strategy_nav", sum(strategy_parts.values()))),
    )
    baseline_nav = _nav_cny(baseline_parts)
    strategy_nav_value = _nav_cny(strategy_parts)
    initial_nav = float(state["initial_nav"])
    record["buy_and_hold_nav"] = baseline_nav
    record["buy_and_hold_nav_cny"] = baseline_nav
    record["buy_and_hold_return"] = baseline_nav / initial_nav - 1.0
    record["strategy_nav"] = strategy_nav_value
    record["strategy_nav_cny"] = strategy_nav_value
    record["strategy_return"] = strategy_nav_value / initial_nav - 1.0
    record["excess_nav"] = strategy_nav_value - baseline_nav
    record["excess_nav_cny"] = strategy_nav_value - baseline_nav
    record["excess_return_points"] = (strategy_nav_value - baseline_nav) / initial_nav
    strategy_cash_parts = record.get("strategy_cash_by_currency") or {}
    cash_cny = _nav_cny(strategy_cash_parts)
    record["strategy_cash"] = cash_cny
    record["strategy_cash_cny"] = cash_cny
    record["strategy_cash_ratio"] = (
        cash_cny / strategy_nav_value if strategy_nav_value > 0 else 0.0
    )
    for item in record.get("operation_attribution") or []:
        if not isinstance(item, MutableMapping):
            continue
        item.setdefault("estimated_gain_local", float(item.get("estimated_gain", 0.0)))
        currency = "HKD" if str(item.get("symbol") or "").startswith("HK") else "CNY"
        item["estimated_gain"] = _amount_cny(
            item["estimated_gain_local"], currency, "operation_attribution.estimated_gain"
        )
        item["estimated_gain_cny"] = item["estimated_gain"]
    candidate = sum(
        float(item.get("estimated_gain_cny", 0.0))
        for item in record.get("operation_attribution") or []
        if item.get("contribution_type") == "new_candidate_selection"
    )
    record.setdefault(
        "new_candidate_selection_mixed_local_currency_1_to_1",
        float(record.get("new_candidate_selection", 0.0)),
    )
    record.setdefault(
        "existing_position_management_mixed_local_currency_1_to_1",
        float(record.get("existing_position_management", 0.0)),
    )
    record["new_candidate_selection"] = candidate
    record["existing_position_management"] = record["excess_nav"] - candidate
    eligible_trades = [
        item
        for item in state.get("trades", [])
        if str(item.get("execution_time") or "")[:10] <= str(record.get("trade_date") or "")
    ]
    transaction_cost = sum(float(item.get("transaction_cost_cny", 0.0)) for item in eligible_trades)
    slippage = sum(float(item.get("slippage_cny", 0.0)) for item in eligible_trades)
    record["transaction_cost"] = transaction_cost
    record["slippage"] = slippage
    record["total_cost_and_slippage"] = transaction_cost + slippage
    record["valuation_mode"] = VALUATION_MODE
    record["hkd_cny_baseline_fx"] = HKD_CNY_BASELINE_FX


def _migrate_v1_to_v2(state: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    """Migrate the already-encrypted PR #4 state without rebuilding the account."""

    if state.get("schema_version") != 1:
        return state
    if state.get("baseline_date") != BASELINE_DATE:
        raise ExperimentInputError("cannot migrate an unexpected shadow A/B baseline")
    legacy_valuation = state.get("valuation_assumption")
    if isinstance(legacy_valuation, Mapping) and legacy_valuation.get("locked") is True:
        legacy_fx = legacy_valuation.get("hkd_cny")
        if legacy_fx is not None and float(legacy_fx) != HKD_CNY_BASELINE_FX:
            raise ExperimentInputError("locked baseline FX conflicts with 0.8865")
    by_currency = state.get("initial_nav_by_currency")
    if not isinstance(by_currency, Mapping):
        raise ExperimentInputError("legacy state has no auditable initial currency split")
    legacy_initial_nav = float(state.get("initial_nav", sum(by_currency.values())))
    initial_nav = _nav_cny(by_currency)
    state["initial_nav_mixed_local_currency_1_to_1"] = legacy_initial_nav
    state["initial_nav"] = initial_nav
    state["initial_nav_cny"] = initial_nav
    state["valuation_assumption"] = {
        "mode": VALUATION_MODE,
        "baseline_date": BASELINE_DATE,
        "hkd_cny": HKD_CNY_BASELINE_FX,
        "locked": True,
        "description": "Formal NAV uses the immutable 2026-08-07 HKD/CNY baseline FX; local-currency components are retained for audit.",
    }
    config = state.setdefault("config", {})
    config["purchasing_power_assumption"] = (
        "simulation assumption: unified CNY purchasing power with fixed baseline FX"
    )
    config["share_unit_assumptions"] = {
        "a_share_buy_lot": 100,
        "a_share_sell_tail_allowed": True,
        "hk_board_lot": "not_modeled",
    }
    strategy = state.get("strategy_shadow_portfolio")
    if not isinstance(strategy, MutableMapping):
        raise ExperimentInputError("legacy state has no strategy account")
    legacy_cash = strategy.get("cash_by_currency") or {}
    if not isinstance(legacy_cash, Mapping):
        raise ExperimentInputError("legacy strategy cash split is invalid")
    unified_cash = _nav_cny(legacy_cash)
    strategy["legacy_cash_by_currency_at_fx_migration"] = copy.deepcopy(legacy_cash)
    strategy["cash_cny"] = unified_cash
    strategy["cash_by_currency"] = {"CNY": unified_cash, "HKD": 0.0}
    for trade in state.get("trades", []):
        if isinstance(trade, MutableMapping):
            _migrate_trade_to_cny(trade)
    state["schema_version"] = SCHEMA_VERSION
    for record in state.get("daily_nav", []):
        if isinstance(record, MutableMapping):
            _migrate_daily_nav_to_cny(state, record)
    metrics = state.setdefault("metrics", {})
    baseline_peak = initial_nav
    strategy_peak = initial_nav
    baseline_max_drawdown = 0.0
    strategy_max_drawdown = 0.0
    for record in state.get("daily_nav", []):
        baseline_nav = float(record["buy_and_hold_nav"])
        strategy_nav_value = float(record["strategy_nav"])
        baseline_peak = max(baseline_peak, baseline_nav)
        strategy_peak = max(strategy_peak, strategy_nav_value)
        baseline_drawdown = max(0.0, 1.0 - baseline_nav / baseline_peak)
        strategy_drawdown = max(0.0, 1.0 - strategy_nav_value / strategy_peak)
        baseline_max_drawdown = max(baseline_max_drawdown, baseline_drawdown)
        strategy_max_drawdown = max(strategy_max_drawdown, strategy_drawdown)
        record["buy_and_hold_drawdown"] = baseline_drawdown
        record["strategy_drawdown"] = strategy_drawdown
        record["buy_and_hold_max_drawdown"] = baseline_max_drawdown
        record["strategy_max_drawdown"] = strategy_max_drawdown
    metrics["buy_and_hold_peak"] = baseline_peak
    metrics["strategy_peak"] = strategy_peak
    metrics["buy_and_hold_max_drawdown"] = baseline_max_drawdown
    metrics["strategy_max_drawdown"] = strategy_max_drawdown
    state["migration_audit"] = {
        "from_schema_version": 1,
        "to_schema_version": SCHEMA_VERSION,
        "preserved_baseline_date": BASELINE_DATE,
        "preserved_ledgers": True,
        "formal_valuation_mode": VALUATION_MODE,
        "hkd_cny_baseline_fx": HKD_CNY_BASELINE_FX,
    }
    return state


def _validate_state(state: Mapping[str, Any]) -> None:
    if state.get("schema_version") != SCHEMA_VERSION:
        raise ExperimentInputError("unsupported shadow A/B state schema")
    if (
        state.get("simulation_only") is not True
        or state.get("places_real_orders") is not False
        or state.get("broker_connected") is not False
        or state.get("human_review_required") is not True
    ):
        raise ExperimentInputError("shadow A/B state violated simulation-only safety flags")
    if state.get("baseline_date") != BASELINE_DATE:
        raise ExperimentInputError("unexpected shadow A/B baseline date")
    valuation = state.get("valuation_assumption")
    if (
        not isinstance(valuation, Mapping)
        or valuation.get("mode") != VALUATION_MODE
        or valuation.get("baseline_date") != BASELINE_DATE
        or valuation.get("locked") is not True
        or float(valuation.get("hkd_cny", 0.0)) != HKD_CNY_BASELINE_FX
    ):
        raise ExperimentInputError("shadow A/B fixed baseline FX is missing or altered")
    expected = set(initial_symbols())
    positions = state.get("initial_positions")
    if not isinstance(positions, Mapping) or set(positions) != expected:
        raise ExperimentInputError("shadow A/B state does not contain the exact initial universe")
    for key in (
        "signal_ledger",
        "execution_ledger",
        "trades",
        "pending_signal_ids",
        "daily_nav",
        "status_ledger",
    ):
        if not isinstance(state.get(key), list):
            raise ExperimentInputError(f"state.{key} must be a list")
    baseline = state.get("buy_and_hold_baseline")
    strategy = state.get("strategy_shadow_portfolio")
    if not isinstance(baseline, Mapping) or not isinstance(strategy, Mapping):
        raise ExperimentInputError("both shadow accounts are required")
    if set((baseline.get("positions") or {})) != expected:
        raise ExperimentInputError("buy-and-hold account cannot change its initial universe")
    if baseline.get("positions") != positions:
        raise ExperimentInputError("buy-and-hold positions must remain equal to the initial snapshot")
    by_currency = state.get("initial_nav_by_currency")
    if not isinstance(by_currency, Mapping):
        raise ExperimentInputError("initial NAV currency split is required")
    expected_initial_nav = _nav_cny(by_currency)
    if not math.isclose(float(state.get("initial_nav", -1.0)), expected_initial_nav, rel_tol=0.0, abs_tol=1e-6):
        raise ExperimentInputError("formal initial NAV must use the locked baseline FX")
    cash_cny = _finite(strategy.get("cash_cny"), "strategy.cash_cny")
    if cash_cny < -1e-8:
        raise ExperimentInputError("unified CNY purchasing power cannot be negative")
    cash_audit = strategy.get("cash_by_currency")
    if not isinstance(cash_audit, Mapping):
        raise ExperimentInputError("strategy cash audit split is required")
    if (
        not math.isclose(
            _finite(cash_audit.get("CNY", 0.0), "strategy.cash_by_currency.CNY"),
            cash_cny,
            rel_tol=0.0,
            abs_tol=1e-6,
        )
        or abs(
            _finite(cash_audit.get("HKD", 0.0), "strategy.cash_by_currency.HKD")
        )
        > 1e-8
    ):
        raise ExperimentInputError(
            "strategy cash audit must mirror unified CNY purchasing power"
        )


def load_state(path: Union[str, Path]) -> Optional[Dict[str, Any]]:
    state_path = Path(path)
    if not state_path.exists():
        return None
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ExperimentInputError(f"cannot load shadow A/B state: {exc}") from exc
    if not isinstance(state, dict):
        raise ExperimentInputError("shadow A/B state must be an object")
    _migrate_v1_to_v2(state)
    _validate_state(state)
    return state


def load_or_initialize(
    path: Union[str, Path],
    initial_portfolio_json: Optional[Union[Mapping[str, Any], str, Path]],
    now: datetime,
) -> Dict[str, Any]:
    """Load PRIMARY state; the completed experiment can never be reborn."""

    existing = load_state(path)
    if existing is not None:
        return existing
    del initial_portfolio_json, now
    raise ExperimentInputError(
        "PRIMARY shadow A/B state is missing; refusing reset or reinitialization"
    )


def save_state(path: Union[str, Path], state: Mapping[str, Any]) -> None:
    """Validate and atomically save a JSON state file."""

    _validate_state(state)
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", dir=str(destination.parent)
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, destination)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def _normalise_costs(raw: Mapping[str, Any]) -> Dict[str, Any]:
    result = {}
    for key in (
        "entry_fee_bps",
        "exit_fee_bps",
        "entry_slippage_bps",
        "exit_slippage_bps",
    ):
        value = _finite(raw.get(key, DEFAULT_COSTS[key]), f"market_costs.{key}")
        if value < 0:
            raise ExperimentInputError(f"market_costs.{key} cannot be negative")
        result[key] = value
    result["source"] = str(raw.get("source") or "signal_override")
    return result


def _strategy_nav(state: Mapping[str, Any]) -> float:
    strategy = state["strategy_shadow_portfolio"]
    prices = strategy.get("last_prices") or {}
    value = _finite(strategy.get("cash_cny"), "strategy.cash_cny")
    for symbol, position in strategy["positions"].items():
        price = _finite(prices.get(symbol), f"last_prices.{symbol}", positive=True)
        value += _amount_cny(
            float(position["quantity"]) * price,
            position["currency"],
            f"positions.{symbol}.market_value",
        )
    return value


def strategy_quantity(state: Mapping[str, Any], symbol: str) -> float:
    """Return the B-account quantity without consulting any real account."""

    position = state["strategy_shadow_portfolio"]["positions"].get(
        _canonical_symbol(symbol)
    )
    return 0.0 if position is None else float(position["quantity"])


def strategy_cash_cny(state: Mapping[str, Any]) -> float:
    """Return authoritative unified CNY purchasing power."""

    _validate_state(state)
    return float(state["strategy_shadow_portfolio"]["cash_cny"])


def strategy_cash(state: Mapping[str, Any], currency: str = "CNY") -> float:
    """Backward-compatible wrapper for unified CNY purchasing power.

    ``currency`` remains accepted for caller compatibility; A- and H-share
    decisions draw from the same CNY balance under the fixed-FX assumption.
    """

    del currency
    return strategy_cash_cny(state)


def strategy_nav(state: Mapping[str, Any]) -> float:
    """Return the current fixed-FX CNY B-account NAV used for sizing."""

    _validate_state(state)
    return _strategy_nav(state)


def record_signal(
    state: MutableMapping[str, Any],
    signal: Optional[Mapping[str, Any]] = None,
    **overrides: Any,
) -> Dict[str, Any]:
    """Append an immutable actionable signal and mark it pending."""

    _validate_state(state)
    raw = dict(signal or {})
    raw.update(overrides)
    event_id = str(raw.get("event_id") or "")
    canonical = _canonical_symbol(raw.get("symbol"))
    signal_dt = _parse_datetime(raw.get("signal_time"), "signal_time")
    quote_dt = _parse_datetime(raw.get("quote_time"), "quote_time")
    if signal_dt.date() <= date.fromisoformat(BASELINE_DATE):
        raise ExperimentInputError("signals on or before the baseline date are observation-only")
    if quote_dt > signal_dt:
        raise ExperimentInputError("quote_time cannot be later than signal_time")
    normalised_action = _ACTION_ALIASES.get(str(raw.get("action") or "").strip())
    if normalised_action is None:
        raise ExperimentInputError(
            f"unsupported simulation action: {raw.get('action')!r}"
        )
    price = _finite(raw.get("signal_price"), "signal_price", positive=True)
    if not str(event_id or "").strip():
        raise ExperimentInputError("event_id is required")
    reason = str(raw.get("reason") or "")
    if not reason.strip():
        raise ExperimentInputError("reason is required")

    initial = state["initial_positions"]
    category = (
        "existing_position_management"
        if canonical in initial
        else "new_candidate_selection"
    )
    if canonical.startswith("HK") and canonical[2:].isdigit():
        currency = "HKD"
    elif canonical.isdigit() and len(canonical) == 6:
        currency = "CNY"
    else:
        raise ExperimentInputError(f"unsupported symbol format: {canonical}")

    requested_notional = None
    position_delta: Dict[str, Any]
    if normalised_action in _BUY_NAV_FRACTIONS:
        nav = _finite(
            (
                _strategy_nav(state)
                if raw.get("strategy_nav") is None
                else raw.get("strategy_nav")
            ),
            "strategy_nav",
            positive=True,
        )
        fraction = _BUY_NAV_FRACTIONS[normalised_action]
        requested_notional = nav * fraction
        position_delta = {"basis": "strategy_nav", "fraction": fraction}
    else:
        fraction = _SELL_POSITION_FRACTIONS[normalised_action]
        position_delta = {"basis": "current_position", "fraction": -fraction}

    material = "|".join(
        (
            str(event_id),
            canonical,
            _iso(signal_dt),
            _iso(quote_dt),
            normalised_action,
        )
    )
    signal_id = hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]
    for existing in state["signal_ledger"]:
        if existing.get("signal_id") == signal_id:
            return existing

    costs = _normalise_costs(
        raw.get("market_costs") or state["config"]["default_costs"]
    )
    record = {
        "signal_id": signal_id,
        "event_id": str(event_id),
        "trade_date": signal_dt.date().isoformat(),
        "symbol": canonical,
        "currency": currency,
        "signal_time": _iso(signal_dt),
        "quote_time": _iso(quote_dt),
        "signal_price": price,
        "action": normalised_action,
        "position_delta": position_delta,
        "requested_notional": requested_notional,
        "requested_notional_currency": "CNY" if requested_notional is not None else None,
        "reason": reason,
        "current_key_level": copy.deepcopy(
            raw.get("current_key_level", raw.get("key_level"))
        ),
        "next_trigger": str(raw.get("next_trigger") or ""),
        "data_quality": str(raw.get("data_quality") or "unknown"),
        "market_costs": costs,
        "contribution_type": category,
        "simulation_only": True,
        "human_review_required": True,
    }
    state["signal_ledger"].append(record)
    state["pending_signal_ids"].append(signal_id)
    return record


def _quote_payload(raw: Any) -> Mapping[str, Any]:
    if isinstance(raw, Mapping):
        return raw
    if hasattr(raw, "__dict__"):
        return vars(raw)
    raise ExperimentInputError("quote must be an object")


def _execution_result(
    state: MutableMapping[str, Any],
    *,
    signal: Mapping[str, Any],
    status: str,
    execution_time: datetime,
    reason: str,
    extra: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    material = f"{signal['signal_id']}|{status}|{_iso(execution_time)}|{reason}"
    result = {
        "execution_id": hashlib.sha256(material.encode("utf-8")).hexdigest()[:24],
        "signal_id": signal["signal_id"],
        "event_id": signal["event_id"],
        "symbol": signal["symbol"],
        "status": status,
        "execution_time": _iso(execution_time),
        "reason": reason,
        "simulation_only": True,
        **dict(extra or {}),
    }
    state["execution_ledger"].append(result)
    if signal["signal_id"] in state["pending_signal_ids"]:
        state["pending_signal_ids"].remove(signal["signal_id"])
    if status == "execution_missed":
        record_status(
            state,
            "execution_missed",
            now=execution_time,
            symbol=signal["symbol"],
            signal_id=signal["signal_id"],
            reason=reason,
        )
    return result


def execute_pending(
    state: MutableMapping[str, Any],
    quotes: Mapping[str, Any],
    *,
    now: Optional[datetime] = None,
    freshness_seconds: float = 90.0,
    session_closed: bool = False,
) -> Sequence[Mapping[str, Any]]:
    """Execute pending signals only on the first fresh post-signal quote."""

    _validate_state(state)
    current = now or datetime.now(SHANGHAI_TZ)
    if current.tzinfo is None:
        current = current.replace(tzinfo=SHANGHAI_TZ)
    current = current.astimezone(SHANGHAI_TZ)
    max_age = _finite(freshness_seconds, "freshness_seconds")
    if max_age < 0:
        raise ExperimentInputError("freshness_seconds cannot be negative")
    signals = {item["signal_id"]: item for item in state["signal_ledger"]}
    outcomes: list[Mapping[str, Any]] = []
    for signal_id in list(state["pending_signal_ids"]):
        signal = signals.get(signal_id)
        if signal is None:
            raise ExperimentInputError(f"pending signal is absent from ledger: {signal_id}")
        signal_dt = _parse_datetime(signal["signal_time"], "signal.signal_time")
        if current.date() > signal_dt.date():
            outcomes.append(
                _execution_result(
                    state,
                    signal=signal,
                    status="execution_missed",
                    execution_time=current,
                    reason="no_same_day_post_signal_quote",
                )
            )
            continue
        raw_quote = quotes.get(signal["symbol"])
        if raw_quote is None:
            if session_closed:
                outcomes.append(
                    _execution_result(
                        state,
                        signal=signal,
                        status="execution_missed",
                        execution_time=current,
                        reason="data_unavailable",
                    )
                )
            continue
        quote = _quote_payload(raw_quote)
        price_value = quote.get("price", quote.get("close"))
        try:
            observed_price = _finite(
                price_value, f"{signal['symbol']}.price", positive=True
            )
            quote_dt = _parse_datetime(
                quote.get("provider_timestamp", quote.get("quote_time")),
                f"{signal['symbol']}.quote_time",
            )
        except ExperimentInputError:
            if session_closed:
                outcomes.append(
                    _execution_result(
                        state,
                        signal=signal,
                        status="execution_missed",
                        execution_time=current,
                        reason="data_unavailable",
                    )
                )
            continue
        stale_seconds = (current - quote_dt).total_seconds()
        stale = bool(quote.get("is_stale", False)) or stale_seconds < 0 or stale_seconds > max_age
        if stale:
            if session_closed:
                outcomes.append(
                    _execution_result(
                        state,
                        signal=signal,
                        status="execution_missed",
                        execution_time=current,
                        reason="stale_quote",
                    )
                )
            continue
        if quote_dt <= signal_dt:
            if session_closed:
                outcomes.append(
                    _execution_result(
                        state,
                        signal=signal,
                        status="execution_missed",
                        execution_time=current,
                        reason="no_post_signal_fresh_quote",
                    )
                )
            continue

        strategy = state["strategy_shadow_portfolio"]
        positions = strategy["positions"]
        symbol = signal["symbol"]
        currency = signal["currency"]
        fx_to_cny = _currency_fx(currency)
        action = signal["action"]
        costs = signal["market_costs"]
        is_buy = action in _BUY_NAV_FRACTIONS
        fee_bps = costs["entry_fee_bps"] if is_buy else costs["exit_fee_bps"]
        slippage_bps = (
            costs["entry_slippage_bps"] if is_buy else costs["exit_slippage_bps"]
        )
        direction = 1.0 if is_buy else -1.0
        execution_price = observed_price * (1.0 + direction * slippage_bps / 10_000.0)
        position = positions.get(symbol)

        if is_buy:
            if action.startswith("add_") and position is None:
                outcomes.append(
                    _execution_result(
                        state,
                        signal=signal,
                        status="execution_missed",
                        execution_time=current,
                        reason="position_not_held",
                    )
                )
                continue
            notional_cny = float(signal["requested_notional"])
            raw_quantity = math.floor(notional_cny / (execution_price * fx_to_cny))
            quantity = (
                float((raw_quantity // 100) * 100)
                if currency == "CNY"
                else float(raw_quantity)
            )
            if quantity < 1:
                outcomes.append(
                    _execution_result(
                        state,
                        signal=signal,
                        status="execution_missed",
                        execution_time=current,
                        reason=(
                            "notional_below_a_share_buy_lot"
                            if currency == "CNY"
                            else "notional_below_one_share"
                        ),
                    )
                )
                continue
            gross = quantity * execution_price
            fee = gross * fee_bps / 10_000.0
            required_cash_cny = (gross + fee) * fx_to_cny
            if float(strategy["cash_cny"]) + 1e-9 < required_cash_cny:
                outcomes.append(
                    _execution_result(
                        state,
                        signal=signal,
                        status="execution_missed",
                        execution_time=current,
                        reason="insufficient_unified_cny_cash",
                    )
                )
                continue
            prior_quantity = float(position["quantity"]) if position else 0.0
            positions[symbol] = {
                "symbol": symbol,
                "name": (position or {}).get("name", symbol),
                "currency": currency,
                "quantity": prior_quantity + quantity,
                "historical_cost": (position or {}).get("historical_cost"),
                "experiment_basis_price": (position or {}).get(
                    "experiment_basis_price", execution_price
                ),
            }
            strategy["cash_cny"] = float(strategy["cash_cny"]) - required_cash_cny
            side = "paper_buy"
        else:
            if position is None or float(position.get("quantity", 0.0)) <= 0:
                outcomes.append(
                    _execution_result(
                        state,
                        signal=signal,
                        status="execution_missed",
                        execution_time=current,
                        reason="position_not_held",
                    )
                )
                continue
            fraction = abs(float(signal["position_delta"]["fraction"]))
            held_quantity = float(position["quantity"])
            quantity = (
                held_quantity
                if fraction >= 1.0
                else float(math.floor(held_quantity * fraction))
            )
            if quantity < 1:
                outcomes.append(
                    _execution_result(
                        state,
                        signal=signal,
                        status="execution_missed",
                        execution_time=current,
                        reason="position_delta_below_one_share",
                    )
                )
                continue
            gross = quantity * execution_price
            fee = gross * fee_bps / 10_000.0
            remaining = held_quantity - quantity
            if remaining <= 1e-10:
                positions.pop(symbol, None)
            else:
                position["quantity"] = remaining
            cash_change_cny = (gross - fee) * fx_to_cny
            strategy["cash_cny"] = float(strategy["cash_cny"]) + cash_change_cny
            side = "paper_sell"

        if float(strategy["cash_cny"]) < -1e-8:
            raise ExperimentInputError("simulation attempted to create negative CNY cash")
        if abs(float(strategy["cash_cny"])) <= 1e-8:
            strategy["cash_cny"] = 0.0
        # Kept only as a backward-compatible audit view.  CNY purchasing power
        # is authoritative, and HKD cash is never a separate spending silo.
        strategy["cash_by_currency"] = {
            "CNY": float(strategy["cash_cny"]),
            "HKD": 0.0,
        }

        strategy["last_prices"][symbol] = observed_price
        slippage = abs(execution_price - observed_price) * quantity
        market_move = (observed_price - float(signal["signal_price"])) * quantity
        trade = {
            "signal_id": signal_id,
            "event_id": signal["event_id"],
            "symbol": symbol,
            "currency": currency,
            "side": side,
            "action": action,
            "signal_time": signal["signal_time"],
            "signal_price": signal["signal_price"],
            "execution_time": _iso(current),
            "quote_time": _iso(quote_dt),
            "observed_quote_price": observed_price,
            "execution_price": execution_price,
            "quantity": quantity,
            "gross_amount": gross,
            "gross_amount_cny": gross * fx_to_cny,
            "transaction_cost": fee,
            "transaction_cost_cny": fee * fx_to_cny,
            "slippage": slippage,
            "slippage_cny": slippage * fx_to_cny,
            "market_move_from_signal": market_move,
            "market_move_from_signal_cny": market_move * fx_to_cny,
            "fx_to_cny": fx_to_cny,
            "cash_change_cny": (
                -(gross + fee) * fx_to_cny
                if side == "paper_buy"
                else (gross - fee) * fx_to_cny
            ),
            "contribution_type": signal["contribution_type"],
            "simulation_only": True,
        }
        state["trades"].append(trade)
        outcomes.append(
            _execution_result(
                state,
                signal=signal,
                status="executed",
                execution_time=current,
                reason="first_fresh_post_signal_quote",
                extra=trade,
            )
        )
    return outcomes


def record_status(
    state: MutableMapping[str, Any],
    status: str,
    *,
    now: datetime,
    reason: str,
    symbol: Optional[str] = None,
    signal_id: Optional[str] = None,
    details: Optional[Mapping[str, Any]] = None,
) -> str:
    """Append an operational status; it is never treated as a hold decision."""

    timestamp = _iso(now)
    material = "|".join(
        (str(status), timestamp, str(symbol or ""), str(signal_id or ""), str(reason))
    )
    status_id = hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]
    if any(item.get("status_id") == status_id for item in state["status_ledger"]):
        return status_id
    state["status_ledger"].append(
        {
            "status_id": status_id,
            "status": str(status),
            "time": timestamp,
            "symbol": _canonical_symbol(symbol) if symbol else None,
            "signal_id": signal_id,
            "reason": str(reason),
            "details": copy.deepcopy(dict(details or {})),
            "simulation_only": True,
        }
    )
    return status_id


def _daily_quote(raw: Any, symbol: str, trade_date: str) -> float:
    quote = _quote_payload(raw)
    price = _finite(quote.get("close", quote.get("price")), f"{symbol}.close", positive=True)
    quote_time = _parse_datetime(
        quote.get("provider_timestamp", quote.get("quote_time")),
        f"{symbol}.quote_time",
    )
    if quote_time.date().isoformat() != trade_date:
        raise ExperimentInputError(f"{symbol} closing quote is not from {trade_date}")
    market_close = datetime_time(16, 0) if symbol.startswith("HK") else datetime_time(15, 0)
    if quote_time.timetz().replace(tzinfo=None) < market_close:
        raise ExperimentInputError(f"{symbol} quote is not a verified closing quote")
    # A finalized same-day close remains valid after the intraday 90-second
    # freshness window; the explicit market-close timestamp is the close gate.
    return price


def _nav_by_currency(
    positions: Mapping[str, Mapping[str, Any]],
    cash: Mapping[str, Any],
    prices: Mapping[str, float],
) -> Dict[str, float]:
    result = {"CNY": float(cash.get("CNY", 0.0)), "HKD": float(cash.get("HKD", 0.0))}
    for symbol, position in positions.items():
        currency = str(position["currency"])
        result.setdefault(currency, 0.0)
        result[currency] += float(position["quantity"]) * prices[symbol]
    return result


def _candidate_contribution(state: Mapping[str, Any], prices: Mapping[str, float]) -> float:
    initial = set(state["initial_positions"])
    contribution = 0.0
    strategy_positions = state["strategy_shadow_portfolio"]["positions"]
    for symbol, position in strategy_positions.items():
        if symbol not in initial:
            contribution += _amount_cny(
                float(position["quantity"]) * prices[symbol],
                position["currency"],
                f"candidate.{symbol}.market_value",
            )
    for trade in state["trades"]:
        if trade["symbol"] in initial:
            continue
        if trade["side"] == "paper_buy":
            contribution -= float(trade["gross_amount_cny"]) + float(
                trade["transaction_cost_cny"]
            )
        else:
            contribution += float(trade["gross_amount_cny"]) - float(
                trade["transaction_cost_cny"]
            )
    return contribution


def _operation_attribution(
    state: Mapping[str, Any], prices: Mapping[str, float]
) -> Sequence[Mapping[str, Any]]:
    """Simple close-marked attribution; portfolio NAV excess remains authoritative."""

    result = []
    for trade in state["trades"]:
        symbol = trade["symbol"]
        close_price = prices.get(symbol)
        if close_price is None:
            continue
        quantity = float(trade["quantity"])
        execution_price = float(trade["execution_price"])
        fx_to_cny = float(trade.get("fx_to_cny", _currency_fx(trade["currency"])))
        fee = float(trade["transaction_cost_cny"])
        if trade["side"] == "paper_sell":
            estimated_gain = (execution_price - close_price) * quantity * fx_to_cny - fee
            interpretation = "减仓后避免损失" if estimated_gain >= 0 else "减仓后卖飞"
        else:
            estimated_gain = (close_price - execution_price) * quantity * fx_to_cny - fee
            interpretation = "买入后贡献" if estimated_gain >= 0 else "买入后拖累"
        result.append(
            {
                "signal_id": trade["signal_id"],
                "symbol": symbol,
                "action": trade["action"],
                "contribution_type": trade["contribution_type"],
                "estimated_gain": estimated_gain,
                "estimated_gain_cny": estimated_gain,
                "interpretation": interpretation,
                "as_of_close": close_price,
                "simulation_only": True,
            }
        )
    return result


def update_latest_quotes(
    state: MutableMapping[str, Any],
    quotes: Mapping[str, Any],
    now: datetime,
    *,
    freshness_seconds: float = 90.0,
) -> int:
    """Persist only fresh provider-timestamped quotes for later close valuation."""

    _validate_state(state)
    current = now if now.tzinfo is not None else now.replace(tzinfo=SHANGHAI_TZ)
    current = current.astimezone(SHANGHAI_TZ)
    max_age = _finite(freshness_seconds, "freshness_seconds")
    latest = state["strategy_shadow_portfolio"].setdefault("latest_quotes", {})
    updated = 0
    for raw_symbol, raw_quote in quotes.items():
        symbol = _canonical_symbol(raw_symbol)
        try:
            quote = _quote_payload(raw_quote)
            price = _finite(
                quote.get("price", quote.get("close")),
                f"{symbol}.price",
                positive=True,
            )
            quote_time = _parse_datetime(
                quote.get("provider_timestamp", quote.get("quote_time")),
                f"{symbol}.quote_time",
            )
        except ExperimentInputError:
            continue
        age = (current - quote_time).total_seconds()
        if bool(quote.get("is_stale", False)) or age < 0 or age > max_age:
            continue
        existing = latest.get(symbol)
        if existing is not None:
            existing_time = _parse_datetime(
                existing.get("provider_timestamp"), f"{symbol}.stored_quote_time"
            )
            if quote_time <= existing_time:
                continue
        latest[symbol] = {
            "symbol": symbol,
            "price": price,
            "provider_timestamp": _iso(quote_time),
            "captured_at": _iso(current),
            "is_stale": False,
            "source": str(quote.get("source") or ""),
        }
        state["strategy_shadow_portfolio"]["last_prices"][symbol] = price
        updated += 1
    return updated


def record_daily_nav(
    state: MutableMapping[str, Any],
    trade_date: str,
    quotes: Optional[Mapping[str, Any]] = None,
    *,
    recorded_at: Optional[datetime] = None,
) -> bool:
    """Append one complete, reliable same-date A/B close record."""

    _validate_state(state)
    try:
        parsed_date = date.fromisoformat(str(trade_date))
    except ValueError as exc:
        raise ExperimentInputError("trade_date must use YYYY-MM-DD") from exc
    if parsed_date <= date.fromisoformat(BASELINE_DATE):
        raise ExperimentInputError("daily A/B returns begin after the baseline date")
    existing = next(
        (item for item in state["daily_nav"] if item.get("trade_date") == trade_date),
        None,
    )
    if existing is not None:
        return True

    quote_source = (
        quotes
        if quotes is not None
        else state["strategy_shadow_portfolio"].get("latest_quotes", {})
    )

    baseline_positions = state["buy_and_hold_baseline"]["positions"]
    strategy_positions = state["strategy_shadow_portfolio"]["positions"]
    required = set(baseline_positions) | set(strategy_positions)
    prices: Dict[str, float] = {}
    missing = []
    for symbol in sorted(required):
        raw = quote_source.get(symbol)
        if raw is None:
            missing.append(symbol)
            continue
        try:
            prices[symbol] = _daily_quote(raw, symbol, trade_date)
        except ExperimentInputError:
            missing.append(symbol)
    if missing:
        record_status(
            state,
            "data_unavailable",
            now=recorded_at or datetime.now(SHANGHAI_TZ),
            reason="incomplete_reliable_closing_prices",
            details={"trade_date": trade_date, "symbols": missing},
        )
        return False

    strategy = state["strategy_shadow_portfolio"]
    strategy["last_prices"].update(prices)
    baseline_by_currency = _nav_by_currency(
        baseline_positions,
        state["buy_and_hold_baseline"]["cash_by_currency"],
        prices,
    )
    strategy_by_currency = _nav_by_currency(
        strategy_positions, strategy["cash_by_currency"], prices
    )
    baseline_nav_mixed = sum(baseline_by_currency.values())
    strategy_nav_mixed = sum(strategy_by_currency.values())
    baseline_nav = _nav_cny(baseline_by_currency)
    strategy_nav = _nav_cny(strategy_by_currency)
    initial_nav = float(state["initial_nav"])
    metrics = state["metrics"]
    baseline_peak = max(float(metrics["buy_and_hold_peak"]), baseline_nav)
    strategy_peak = max(float(metrics["strategy_peak"]), strategy_nav)
    baseline_drawdown = max(0.0, 1.0 - baseline_nav / baseline_peak)
    strategy_drawdown = max(0.0, 1.0 - strategy_nav / strategy_peak)
    metrics["buy_and_hold_peak"] = baseline_peak
    metrics["strategy_peak"] = strategy_peak
    metrics["buy_and_hold_max_drawdown"] = max(
        float(metrics["buy_and_hold_max_drawdown"]), baseline_drawdown
    )
    metrics["strategy_max_drawdown"] = max(
        float(metrics["strategy_max_drawdown"]), strategy_drawdown
    )
    day_index = len(state["daily_nav"]) + 1
    candidate_contribution = _candidate_contribution(state, prices)
    operation_attribution = _operation_attribution(state, prices)
    excess = strategy_nav - baseline_nav
    transaction_cost = sum(
        float(item["transaction_cost_cny"]) for item in state["trades"]
    )
    slippage = sum(float(item["slippage_cny"]) for item in state["trades"])
    strategy_cash_cny_value = float(strategy["cash_cny"])
    record = {
        "trade_date": trade_date,
        "day_index": day_index,
        "buy_and_hold_nav": baseline_nav,
        "buy_and_hold_nav_cny": baseline_nav,
        "buy_and_hold_nav_by_currency": baseline_by_currency,
        "buy_and_hold_nav_mixed_local_currency_1_to_1": baseline_nav_mixed,
        "buy_and_hold_return": baseline_nav / initial_nav - 1.0,
        "strategy_nav": strategy_nav,
        "strategy_nav_cny": strategy_nav,
        "strategy_nav_by_currency": strategy_by_currency,
        "strategy_nav_mixed_local_currency_1_to_1": strategy_nav_mixed,
        "strategy_return": strategy_nav / initial_nav - 1.0,
        "excess_nav": excess,
        "excess_nav_cny": excess,
        "excess_return_points": (strategy_nav - baseline_nav) / initial_nav,
        "buy_and_hold_drawdown": baseline_drawdown,
        "strategy_drawdown": strategy_drawdown,
        "buy_and_hold_max_drawdown": metrics["buy_and_hold_max_drawdown"],
        "strategy_max_drawdown": metrics["strategy_max_drawdown"],
        "strategy_cash": strategy_cash_cny_value,
        "strategy_cash_cny": strategy_cash_cny_value,
        "strategy_cash_by_currency": copy.deepcopy(strategy["cash_by_currency"]),
        "strategy_cash_ratio": (
            strategy_cash_cny_value / strategy_nav
            if strategy_nav > 0
            else 0.0
        ),
        "trade_count": len(state["trades"]),
        "transaction_cost": transaction_cost,
        "slippage": slippage,
        "total_cost_and_slippage": transaction_cost + slippage,
        "existing_position_management": excess - candidate_contribution,
        "new_candidate_selection": candidate_contribution,
        "operation_attribution": operation_attribution,
        "valuation_mode": VALUATION_MODE,
        "hkd_cny_baseline_fx": HKD_CNY_BASELINE_FX,
        "simulation_only": True,
    }
    state["daily_nav"].append(record)
    metrics["completed_trading_days"] = day_index
    metrics["phase"] = (
        "formal_60_day_evaluation_complete"
        if day_index >= FORMAL_EVALUATION_DAYS
        else "after_20_day_checkpoint"
        if day_index >= CHECKPOINT_DAYS
        else "before_20_day_checkpoint"
    )
    return True


def _pct(value: float) -> str:
    return f"{value * 100:+.2f}%"


def render_scorecard(
    state: Mapping[str, Any], *, trade_date: Optional[str] = None
) -> str:
    """Render one plain-language strategy-vs-hold scorecard (latest by default)."""

    _validate_state(state)
    if not state["daily_nav"]:
        return (
            "# 策略 vs 完全死拿\n\n"
            "尚无完整可靠的实验日收盘数据。\n\n"
            "> 仅供模拟和人工复核，不构成交易指令；系统不会连接券商或自动下单。\n"
        )
    latest = (
        next(
            (
                item
                for item in state["daily_nav"]
                if str(item.get("trade_date") or "") == trade_date
            ),
            None,
        )
        if trade_date is not None
        else state["daily_nav"][-1]
    )
    if latest is None:
        raise ExperimentInputError(f"scorecard trade date not found: {trade_date}")
    day_index = int(latest["day_index"])
    target = CHECKPOINT_DAYS if day_index <= CHECKPOINT_DAYS else FORMAL_EVALUATION_DAYS
    excess = float(latest["excess_nav"])
    comparison = "多赚" if excess >= 0 else "少赚"
    sample = (
        "已达到60日正式评估点。"
        if day_index >= FORMAL_EVALUATION_DAYS
        else "已过20日检查点，实验继续到60日。"
        if day_index >= CHECKPOINT_DAYS
        else "样本仍不足，结论仅作阶段观察。"
    )
    lines = [
        "# 策略 vs 完全死拿",
        "",
        f"实验第 {day_index} / {target} 个交易日",
        "",
        f"- 完全死拿账户：{latest['buy_and_hold_nav'] / 10_000:,.2f} 万元人民币",
        f"- 死拿累计收益率：{_pct(float(latest['buy_and_hold_return']))}",
        f"- 完全按策略账户：{latest['strategy_nav'] / 10_000:,.2f} 万元人民币",
        f"- 策略累计收益率：{_pct(float(latest['strategy_return']))}",
        f"- 听策略相比死拿：{comparison} {abs(excess) / 10_000:,.2f} 万，{_pct(float(latest['excess_return_points']))}",
        f"- 死拿最大回撤：{_pct(-float(latest['buy_and_hold_max_drawdown']))}",
        f"- 策略最大回撤：{_pct(-float(latest['strategy_max_drawdown']))}",
        f"- 策略当前现金：{latest['strategy_cash'] / 10_000:,.2f} 万元人民币",
        f"- 策略当前现金比例：{_pct(float(latest['strategy_cash_ratio']))}",
        f"- 累计模拟交易次数：{latest['trade_count']}",
        f"- 累计手续费及模拟滑点：{latest['total_cost_and_slippage'] / 10_000:,.2f} 万",
        f"- 现有持仓管理贡献：{latest['existing_position_management'] / 10_000:+,.2f} 万",
        f"- 新候选选股贡献：{latest['new_candidate_selection'] / 10_000:+,.2f} 万",
    ]
    attributions = sorted(
        latest.get("operation_attribution") or [],
        key=lambda item: abs(float(item.get("estimated_gain") or 0.0)),
        reverse=True,
    )[:3]
    if attributions:
        lines.extend(
            [
                "- 操作增益归因："
                + "；".join(
                    f"{item['symbol']} {item['interpretation']} "
                    f"{float(item['estimated_gain']) / 10_000:+,.2f} 万"
                    for item in attributions
                )
            ]
        )
    lines.extend(
        [
            "",
            f"当前阶段结论：策略目前相比死拿{comparison}{abs(excess) / 10_000:,.2f} 万；{sample}",
            "",
            f"> 正式净值统一使用人民币；HKD按2026-08-07锁定汇率1 HKD = {HKD_CNY_BASELINE_FX:.4f} CNY换算。",
            "> simulation assumption: unified CNY purchasing power with fixed baseline FX; HK board lot not modeled.",
            "> 仅供模拟和人工复核，不构成交易指令；系统不会连接券商或自动下单。",
            "",
        ]
    )
    return "\n".join(lines)


__all__ = [
    "BASELINE_DATE",
    "CHECKPOINT_DAYS",
    "DEFAULT_COSTS",
    "ExperimentInputError",
    "FORMAL_EVALUATION_DAYS",
    "HKD_CNY_BASELINE_FX",
    "INITIAL_INSTRUMENTS",
    "execute_pending",
    "initial_symbols",
    "initialize_state",
    "load_state",
    "load_or_initialize",
    "record_daily_nav",
    "record_signal",
    "record_status",
    "render_scorecard",
    "save_state",
    "strategy_cash",
    "strategy_cash_cny",
    "strategy_nav",
    "strategy_quantity",
    "update_latest_quotes",
]
