#!/usr/bin/env python3
"""Persistent, deterministic paper-trading tracker.

The tracker never connects to a broker and never places an order.  It consumes a
small JSON snapshot containing a trade date and closing prices/signals, stores
the snapshot by date, and rebuilds the paper portfolio from those snapshots.
Rebuilding makes a repeated run for the same date idempotent and also makes a
corrected historical snapshot safe.  A close-generated signal is held as
pending and can only be simulated at the next trading snapshot's price.
Portfolio values are dimensionless paper net-value units: each symbol's
local-currency return is standardised into the configured target weight.  The
tracker does not perform FX conversion.

Example input::

    {
      "trade_date": "2026-07-28",
      "signals": [
        {
          "symbol": "300408",
          "name": "三环集团",
          "signal": "buy",
          "close": 39.50,
          "reason": "paper-test signal only"
        },
        {
          "symbol": "HK00981",
          "name": "中芯国际",
          "signal": "hold",
          "close": 58.20
        }
      ]
    }

The input root may use ``results`` instead of ``signals``.  Common result-field
aliases such as ``stock_code``, ``action`` and ``current_price`` are accepted.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import tempfile
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple


SCHEMA_VERSION = 1
DEFAULT_TARGET_DAYS = 20
DEFAULT_INITIAL_CASH = 1_000_000.0
DEFAULT_TARGET_WEIGHT = 0.08
DEFAULT_TRANSACTION_COST_BPS = 10.0

BUY_ALIASES = {
    "buy",
    "strong_buy",
    "add",
    "increase",
    "b",
    "买入",
    "加仓",
    "增持",
    "强烈买入",
}
HOLD_ALIASES = {
    "hold",
    "wait",
    "watch",
    "neutral",
    "h",
    "持有",
    "观望",
    "等待",
    "中性",
    "不变",
}
SELL_ALIASES = {
    "sell",
    "strong_sell",
    "reduce",
    "close",
    "exit",
    "s",
    "卖出",
    "减仓",
    "清仓",
    "离场",
}


class TrackerInputError(ValueError):
    """Raised when a signal snapshot is unsafe or ambiguous."""


def _normalise_symbol(raw: Any) -> str:
    symbol = str(raw or "").strip().upper()
    if not symbol:
        return ""

    hk_prefix = re.fullmatch(r"HK[.\-]?(\d{1,5})", symbol)
    hk_suffix = re.fullmatch(r"(\d{1,5})[.\-]?HK", symbol)
    hk_match = hk_prefix or hk_suffix
    if hk_match:
        return f"HK{hk_match.group(1).zfill(5)}"
    return symbol


def _finite_number(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise TrackerInputError(f"{field} must be a finite number")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise TrackerInputError(f"{field} must be a finite number") from exc
    if not math.isfinite(number):
        raise TrackerInputError(f"{field} must be a finite number")
    return number


def _first_present(record: Mapping[str, Any], names: Iterable[str]) -> Any:
    for name in names:
        if name in record and record[name] is not None:
            return record[name]
    return None


def _normalise_action(raw: Any) -> str:
    if raw is None:
        raise TrackerInputError("each signal requires signal/action")
    value = str(raw).strip().lower().replace("-", "_").replace(" ", "_")
    if value in BUY_ALIASES:
        return "buy"
    if value in HOLD_ALIASES:
        return "hold"
    if value in SELL_ALIASES:
        return "sell"
    raise TrackerInputError(f"unsupported signal/action: {raw!r}")


def _normalise_trade_date(raw: Any) -> str:
    if raw is None:
        raise TrackerInputError("trade_date is required")
    try:
        parsed = date.fromisoformat(str(raw).strip())
    except ValueError as exc:
        raise TrackerInputError("trade_date must use YYYY-MM-DD") from exc
    if parsed.weekday() >= 5:
        raise TrackerInputError("trade_date falls on a weekend; only market-day snapshots are counted")
    return parsed.isoformat()


def normalise_snapshot(payload: Any, trade_date_override: Optional[str] = None) -> Dict[str, Any]:
    """Validate and canonicalise one daily signal snapshot."""

    if isinstance(payload, list):
        raw_signals = payload
        raw_date = trade_date_override
    elif isinstance(payload, Mapping):
        raw_signals = payload.get("signals", payload.get("results"))
        raw_date = trade_date_override or payload.get("trade_date") or payload.get("date")
    else:
        raise TrackerInputError("signals JSON must be an object or a list")

    trade_date = _normalise_trade_date(raw_date)
    if not isinstance(raw_signals, list) or not raw_signals:
        raise TrackerInputError("signals/results must be a non-empty list")

    signals: List[Dict[str, Any]] = []
    seen_symbols = set()
    for index, raw_record in enumerate(raw_signals):
        if not isinstance(raw_record, Mapping):
            raise TrackerInputError(f"signal #{index + 1} must be an object")

        raw_symbol = _first_present(raw_record, ("symbol", "stock_code", "code", "ticker"))
        symbol = _normalise_symbol(raw_symbol)
        if not symbol:
            raise TrackerInputError(f"signal #{index + 1} requires symbol/stock_code")
        if symbol in seen_symbols:
            raise TrackerInputError(f"duplicate symbol in one trade date: {symbol}")
        seen_symbols.add(symbol)

        raw_price = _first_present(raw_record, ("close", "price", "reference_price", "current_price"))
        price = _finite_number(raw_price, f"{symbol}.close")
        if price <= 0:
            raise TrackerInputError(f"{symbol}.close must be greater than zero")

        raw_weight = _first_present(raw_record, ("target_weight", "weight"))
        target_weight: Optional[float] = None
        if raw_weight is not None:
            target_weight = _finite_number(raw_weight, f"{symbol}.target_weight")
            if not 0 <= target_weight <= 1:
                raise TrackerInputError(f"{symbol}.target_weight must be between 0 and 1")

        name = str(_first_present(raw_record, ("name", "stock_name")) or "").strip()
        reason = str(_first_present(raw_record, ("reason", "summary", "rationale")) or "").strip()
        signals.append(
            {
                "symbol": symbol,
                "name": name,
                "signal": _normalise_action(
                    _first_present(raw_record, ("signal", "action", "operation", "recommendation"))
                ),
                "close": price,
                "target_weight": target_weight,
                "reason": reason,
            }
        )

    signals.sort(key=lambda item: item["symbol"])
    return {"trade_date": trade_date, "signals": signals}


def new_state(
    *,
    target_trading_days: int = DEFAULT_TARGET_DAYS,
    initial_cash: float = DEFAULT_INITIAL_CASH,
    default_target_weight: float = DEFAULT_TARGET_WEIGHT,
    transaction_cost_bps: float = DEFAULT_TRANSACTION_COST_BPS,
) -> Dict[str, Any]:
    """Create an empty paper-trading state."""

    if target_trading_days <= 0:
        raise TrackerInputError("target_trading_days must be greater than zero")
    if initial_cash <= 0:
        raise TrackerInputError("initial_cash must be greater than zero")
    if not 0 < default_target_weight <= 1:
        raise TrackerInputError("default_target_weight must be greater than zero and at most one")
    if transaction_cost_bps < 0:
        raise TrackerInputError("transaction_cost_bps cannot be negative")
    return {
        "schema_version": SCHEMA_VERSION,
        "simulation_only": True,
        "config": {
            "target_trading_days": int(target_trading_days),
            "initial_cash": float(initial_cash),
            "default_target_weight": float(default_target_weight),
            "transaction_cost_bps": float(transaction_cost_bps),
        },
        "inputs": {},
        "portfolio_days": [],
        "symbol_days": [],
        "trades": [],
        "positions": {},
        "pending_signals": None,
        "cash": float(initial_cash),
        "metrics": {},
    }


def load_state(path: Path) -> Optional[Dict[str, Any]]:
    """Load a state file, returning ``None`` when it does not exist."""

    if not path.exists():
        return None
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TrackerInputError(f"cannot read tracker state {path}: {exc}") from exc
    if not isinstance(state, dict) or state.get("schema_version") != SCHEMA_VERSION:
        raise TrackerInputError(f"unsupported tracker state schema in {path}")
    if not isinstance(state.get("inputs"), dict) or not isinstance(state.get("config"), dict):
        raise TrackerInputError(f"invalid tracker state structure in {path}")
    return state


def _round_money(value: float) -> float:
    return round(value, 4)


def _round_quantity(value: float) -> float:
    return round(value, 8)


def _position_value(position: Mapping[str, Any], prices: Mapping[str, float]) -> float:
    symbol = str(position["symbol"])
    return float(position["quantity"]) * prices[symbol]


def _portfolio_value(cash: float, positions: Mapping[str, Mapping[str, Any]], prices: Mapping[str, float]) -> float:
    return cash + sum(_position_value(position, prices) for position in positions.values())


def _buy(
    *,
    symbol: str,
    name: str,
    price: float,
    desired_market_value: float,
    positions: MutableMapping[str, Dict[str, Any]],
    cash: float,
    fee_rate: float,
) -> Tuple[float, Optional[Dict[str, Any]]]:
    position = positions.get(symbol)
    current_quantity = float(position["quantity"]) if position else 0.0
    current_value = current_quantity * price
    desired_notional = max(desired_market_value - current_value, 0.0)
    if desired_notional <= 1e-9 or cash <= 1e-9:
        return cash, None

    affordable_notional = cash / (1 + fee_rate)
    notional = min(desired_notional, affordable_notional)
    quantity = notional / price
    fee = notional * fee_rate
    if quantity <= 1e-12:
        return cash, None

    previous_cost_basis = current_quantity * float(position["average_cost"]) if position else 0.0
    new_quantity = current_quantity + quantity
    average_cost = (previous_cost_basis + notional + fee) / new_quantity
    positions[symbol] = {
        "symbol": symbol,
        "name": name or (position or {}).get("name", ""),
        "quantity": _round_quantity(new_quantity),
        "average_cost": _round_money(average_cost),
        "last_price": price,
    }
    trade = {
        "symbol": symbol,
        "name": name,
        "side": "paper_buy",
        "quantity": _round_quantity(quantity),
        "price": price,
        "notional": _round_money(notional),
        "transaction_cost": _round_money(fee),
        "realized_pnl": 0.0,
    }
    return cash - notional - fee, trade


def _sell(
    *,
    symbol: str,
    name: str,
    price: float,
    desired_market_value: float,
    positions: MutableMapping[str, Dict[str, Any]],
    cash: float,
    fee_rate: float,
) -> Tuple[float, Optional[Dict[str, Any]]]:
    position = positions.get(symbol)
    if not position:
        return cash, None

    current_quantity = float(position["quantity"])
    target_quantity = min(max(desired_market_value / price, 0.0), current_quantity)
    quantity = current_quantity - target_quantity
    if quantity <= 1e-12:
        return cash, None

    notional = quantity * price
    fee = notional * fee_rate
    realized_pnl = notional - fee - quantity * float(position["average_cost"])
    remaining_quantity = current_quantity - quantity
    if remaining_quantity <= 1e-8:
        positions.pop(symbol, None)
    else:
        position["quantity"] = _round_quantity(remaining_quantity)
        position["last_price"] = price

    trade = {
        "symbol": symbol,
        "name": name or position.get("name", ""),
        "side": "paper_sell",
        "quantity": _round_quantity(quantity),
        "price": price,
        "notional": _round_money(notional),
        "transaction_cost": _round_money(fee),
        "realized_pnl": _round_money(realized_pnl),
    }
    return cash + notional - fee, trade


def rebuild_state(state: Mapping[str, Any]) -> Dict[str, Any]:
    """Rebuild all derived records from canonical per-date inputs."""

    config = dict(state["config"])
    rebuilt = new_state(
        target_trading_days=int(config["target_trading_days"]),
        initial_cash=float(config["initial_cash"]),
        default_target_weight=float(config["default_target_weight"]),
        transaction_cost_bps=float(config["transaction_cost_bps"]),
    )
    rebuilt["inputs"] = dict(state["inputs"])

    initial_cash = float(config["initial_cash"])
    default_target_weight = float(config["default_target_weight"])
    fee_rate = float(config["transaction_cost_bps"]) / 10_000
    cash = initial_cash
    positions: Dict[str, Dict[str, Any]] = {}
    prices: Dict[str, float] = {}
    names: Dict[str, str] = {}
    previous_ending_value: Optional[float] = None
    peak_value = initial_cash
    maximum_drawdown = 0.0
    pending_snapshot: Optional[Dict[str, Any]] = None

    for trade_date in sorted(rebuilt["inputs"]):
        snapshot = normalise_snapshot(rebuilt["inputs"][trade_date])
        signal_by_symbol = {item["symbol"]: item for item in snapshot["signals"]}
        execution_signals = [] if pending_snapshot is None else pending_snapshot["signals"]
        execution_by_symbol = {item["symbol"]: item for item in execution_signals}
        previous_prices = dict(prices)
        quantities_before = {symbol: float(position["quantity"]) for symbol, position in positions.items()}

        for signal in snapshot["signals"]:
            symbol = signal["symbol"]
            prices[symbol] = float(signal["close"])
            if signal["name"]:
                names[symbol] = signal["name"]
            if symbol in positions:
                positions[symbol]["last_price"] = prices[symbol]
                if signal["name"]:
                    positions[symbol]["name"] = signal["name"]

        pre_trade_value = _portfolio_value(cash, positions, prices)
        day_trades: List[Dict[str, Any]] = []
        for signal in execution_signals:
            symbol = signal["symbol"]
            action = signal["signal"]
            current_day_signal = signal_by_symbol.get(symbol)
            if current_day_signal is None:
                raise TrackerInputError(
                    f"{trade_date} is missing the next-day price for pending symbol {symbol}"
                )
            price = float(current_day_signal["close"])
            explicit_weight = signal["target_weight"]
            current_value = float(positions.get(symbol, {}).get("quantity", 0.0)) * price

            trade: Optional[Dict[str, Any]] = None
            if action == "buy":
                if symbol in positions and explicit_weight is None:
                    desired_value = current_value
                else:
                    weight = default_target_weight if explicit_weight is None else float(explicit_weight)
                    desired_value = pre_trade_value * weight
                cash, trade = _buy(
                    symbol=symbol,
                    name=signal["name"],
                    price=price,
                    desired_market_value=desired_value,
                    positions=positions,
                    cash=cash,
                    fee_rate=fee_rate,
                )
            elif action == "sell":
                weight = 0.0 if explicit_weight is None else float(explicit_weight)
                cash, trade = _sell(
                    symbol=symbol,
                    name=signal["name"],
                    price=price,
                    desired_market_value=pre_trade_value * weight,
                    positions=positions,
                    cash=cash,
                    fee_rate=fee_rate,
                )

            if trade:
                trade["trade_date"] = trade_date
                trade["signal_date"] = pending_snapshot["trade_date"]
                trade["reason"] = signal["reason"]
                day_trades.append(trade)

        ending_value = _portfolio_value(cash, positions, prices)
        invested_value = ending_value - cash
        daily_return = None if previous_ending_value is None else ending_value / previous_ending_value - 1
        cumulative_return = ending_value / initial_cash - 1
        peak_value = max(peak_value, ending_value)
        drawdown = ending_value / peak_value - 1
        maximum_drawdown = max(maximum_drawdown, -drawdown)

        portfolio_day = {
            "trade_date": trade_date,
            "starting_value": _round_money(
                initial_cash if previous_ending_value is None else previous_ending_value
            ),
            "ending_value": _round_money(ending_value),
            "cash": _round_money(cash),
            "invested_value": _round_money(invested_value),
            "daily_return": None if daily_return is None else daily_return,
            "cumulative_return": cumulative_return,
            "drawdown": drawdown,
            "trade_count": len(day_trades),
        }
        rebuilt["portfolio_days"].append(portfolio_day)
        rebuilt["trades"].extend(day_trades)

        tracked_symbols = sorted(
            set(quantities_before) | set(positions) | set(signal_by_symbol) | set(execution_by_symbol)
        )
        for symbol in tracked_symbols:
            signal = signal_by_symbol.get(symbol)
            executed_signal = execution_by_symbol.get(symbol)
            price = prices[symbol]
            quantity_before = quantities_before.get(symbol, 0.0)
            quantity_after = float(positions.get(symbol, {}).get("quantity", 0.0))
            symbol_trades = [trade for trade in day_trades if trade["symbol"] == symbol]
            transaction_cost = sum(float(trade["transaction_cost"]) for trade in symbol_trades)
            trade_quantity = sum(
                float(trade["quantity"]) * (1 if trade["side"] == "paper_buy" else -1)
                for trade in symbol_trades
            )
            market_value = quantity_after * price
            average_cost = float(positions.get(symbol, {}).get("average_cost", 0.0))
            unrealized_return = None
            if quantity_after > 0 and average_cost > 0:
                unrealized_return = price / average_cost - 1
            rebuilt["symbol_days"].append(
                {
                    "trade_date": trade_date,
                    "symbol": symbol,
                    "name": names.get(symbol, ""),
                    "signal": signal["signal"] if signal else "no_update",
                    "executed_signal": executed_signal["signal"] if executed_signal else None,
                    "executed_signal_date": (
                        pending_snapshot["trade_date"] if executed_signal and pending_snapshot else None
                    ),
                    "close": price,
                    "price_stale": signal is None,
                    "previous_close": previous_prices.get(symbol),
                    "quantity_before": _round_quantity(quantity_before),
                    "quantity_after": _round_quantity(quantity_after),
                    "trade_quantity": _round_quantity(trade_quantity),
                    "transaction_cost": _round_money(transaction_cost),
                    "market_value": _round_money(market_value),
                    "portfolio_weight": 0.0 if ending_value <= 0 else market_value / ending_value,
                    "average_cost": _round_money(average_cost),
                    "unrealized_return": unrealized_return,
                    "reason": signal["reason"] if signal else "",
                }
            )

        previous_ending_value = ending_value
        pending_snapshot = snapshot

    return_days = [
        float(day["daily_return"]) for day in rebuilt["portfolio_days"] if day["daily_return"] is not None
    ]
    positive_days = sum(day_return > 0 for day_return in return_days)
    snapshot_days = len(rebuilt["portfolio_days"])
    completed_days = len(return_days)
    target_days = int(config["target_trading_days"])
    ending_value = previous_ending_value if previous_ending_value is not None else initial_cash
    rebuilt["positions"] = positions
    rebuilt["pending_signals"] = pending_snapshot
    rebuilt["cash"] = _round_money(cash)
    rebuilt["metrics"] = {
        "snapshot_days": snapshot_days,
        "baseline_days": 1 if snapshot_days else 0,
        "completed_trading_days": completed_days,
        "target_trading_days": target_days,
        "remaining_trading_days": max(target_days - completed_days, 0),
        "status": "complete" if completed_days >= target_days else "running",
        "ending_value": _round_money(ending_value),
        "cumulative_return": ending_value / initial_cash - 1,
        "evaluated_return_days": len(return_days),
        "positive_return_days": positive_days,
        "win_rate": positive_days / len(return_days) if return_days else 0.0,
        "max_drawdown": maximum_drawdown,
        "trade_count": len(rebuilt["trades"]),
    }
    return rebuilt


def update_state(
    state: Dict[str, Any],
    snapshot: Dict[str, Any],
    *,
    target_trading_days: Optional[int] = None,
    initial_cash: Optional[float] = None,
    default_target_weight: Optional[float] = None,
    transaction_cost_bps: Optional[float] = None,
) -> Dict[str, Any]:
    """Insert or replace one date and rebuild derived records."""

    config = dict(state["config"])
    if target_trading_days is not None:
        config["target_trading_days"] = int(target_trading_days)
    if initial_cash is not None:
        config["initial_cash"] = float(initial_cash)
    if default_target_weight is not None:
        config["default_target_weight"] = float(default_target_weight)
    if transaction_cost_bps is not None:
        config["transaction_cost_bps"] = float(transaction_cost_bps)

    candidate = new_state(
        target_trading_days=int(config["target_trading_days"]),
        initial_cash=float(config["initial_cash"]),
        default_target_weight=float(config["default_target_weight"]),
        transaction_cost_bps=float(config["transaction_cost_bps"]),
    )
    candidate["inputs"] = dict(state["inputs"])
    candidate["inputs"][snapshot["trade_date"]] = snapshot
    return rebuild_state(candidate)


def _percent(value: Optional[float]) -> str:
    if value is None:
        return "—"
    return f"{value * 100:.2f}%"


def _escape_markdown(value: Any) -> str:
    return str(value or "").replace("|", "\\|").replace("\n", " ")


def render_summary(state: Mapping[str, Any]) -> str:
    """Render a concise Markdown progress and risk summary."""

    metrics = state["metrics"]
    config = state["config"]
    positions = state["positions"]
    lines = [
        "# 20个交易日模拟净值跟踪",
        "",
        "> 仅用于模拟验证，不连接券商、不下真实订单，也不构成投资建议。",
        (
            "> 模拟净值使用标准化单位：A股按 CNY、港股按 HKD 分别计算本币收益后按目标权重合并；"
            "暂不包含 HKD/CNY 汇率变动。金额和数量均为模拟计算单位，不代表可直接结算的人民币或真实持仓。"
        ),
        "",
        "## 进度与表现",
        "",
        f"- 状态：{'已完成' if metrics['status'] == 'complete' else '进行中'}",
        (
            f"- 评估进度：基线 {metrics['baseline_days']} 日 + "
            f"已评估 {metrics['completed_trading_days']} / {metrics['target_trading_days']} "
            f"个交易区间（剩余 {metrics['remaining_trading_days']}）"
        ),
        f"- 收盘快照总数：{metrics['snapshot_days']}",
        f"- 初始模拟净值单位：{float(config['initial_cash']):,.2f}",
        f"- 当前模拟净值单位：{float(metrics['ending_value']):,.2f}",
        f"- 累计收益率：{_percent(float(metrics['cumulative_return']))}",
        (
            f"- 日胜率：{_percent(float(metrics['win_rate']))}"
            f"（上涨 {metrics['positive_return_days']} / 可评估 {metrics['evaluated_return_days']}）"
        ),
        f"- 最大回撤：{_percent(float(metrics['max_drawdown']))}",
        f"- 模拟成交次数：{metrics['trade_count']}",
        "",
        (
            "首日只建立基线并保存待执行信号，不计为收益区间；"
            "每个收盘信号在下一份交易日快照价格才模拟执行。"
        ),
        "日胜率按已评估交易区间的正收益占比计算；最大回撤按模拟组合历史峰值计算。",
    ]

    inputs = state.get("inputs") if isinstance(state.get("inputs"), Mapping) else {}
    if inputs:
        latest_date = sorted(inputs)[-1]
        latest_snapshot = normalise_snapshot(inputs[latest_date])
        action_labels = {"buy": "模拟买入", "hold": "观察/持有", "sell": "模拟卖出"}
        lines.extend(
            [
                "",
                f"## 最新模拟信号（{latest_date}）",
                "",
                "| 代码 | 名称 | 信号 | 本币参考收盘价 | 理由摘要 |",
                "| --- | --- | --- | ---: | --- |",
            ]
        )
        for signal in latest_snapshot["signals"]:
            reason = str(signal.get("reason") or "")
            if len(reason) > 120:
                reason = reason[:117] + "..."
            lines.append(
                "| {symbol} | {name} | {action} | {close:.4f} | {reason} |".format(
                    symbol=_escape_markdown(signal["symbol"]),
                    name=_escape_markdown(signal.get("name")),
                    action=action_labels[signal["signal"]],
                    close=float(signal["close"]),
                    reason=_escape_markdown(reason),
                )
            )

    lines.extend(
        [
            "",
            "## 每日组合记录",
            "",
            "| 日期 | 期末净值单位 | 当日收益 | 累计收益 | 回撤 | 模拟成交 |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for day in state["portfolio_days"]:
        lines.append(
            "| {date} | {value:,.2f} | {daily} | {cumulative} | {drawdown} | {trades} |".format(
                date=day["trade_date"],
                value=float(day["ending_value"]),
                daily=_percent(day["daily_return"]),
                cumulative=_percent(float(day["cumulative_return"])),
                drawdown=_percent(float(day["drawdown"])),
                trades=day["trade_count"],
            )
        )

    lines.extend(
        [
            "",
            "## 当前模拟持仓",
            "",
            "| 代码 | 名称 | 模拟数量 | 本币参考价 | 本币名义值 | 净值权重 | 本币浮动收益 |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    if positions:
        latest_symbols = {
            record["symbol"]: record
            for record in state["symbol_days"]
            if record["trade_date"] == state["portfolio_days"][-1]["trade_date"]
        }
        for symbol in sorted(positions):
            position = positions[symbol]
            record = latest_symbols[symbol]
            lines.append(
                "| {symbol} | {name} | {quantity:.4f} | {price:.4f} | {value:,.2f} | {weight} | {pnl} |".format(
                    symbol=_escape_markdown(symbol),
                    name=_escape_markdown(position.get("name")),
                    quantity=float(position["quantity"]),
                    price=float(position["last_price"]),
                    value=float(record["market_value"]),
                    weight=_percent(float(record["portfolio_weight"])),
                    pnl=_percent(record["unrealized_return"]),
                )
            )
    else:
        lines.append("| — | 当前无模拟持仓 | — | — | — | — | — |")
    lines.append("")
    return "\n".join(lines)


def atomic_write_text(path: Path, content: str) -> None:
    """Write a UTF-8 file atomically in its destination directory."""

    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def save_state(path: Path, state: Mapping[str, Any]) -> None:
    atomic_write_text(path, json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def _read_json(path: str) -> Any:
    if path == "-":
        source = sys.stdin.read()
    else:
        source = Path(path).read_text(encoding="utf-8")
    try:
        return json.loads(source)
    except json.JSONDecodeError as exc:
        raise TrackerInputError(f"invalid signals JSON: {exc}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Append one date to a persistent 20-day paper-trading tracker; never places real orders."
    )
    parser.add_argument("--signals", help="daily signals JSON file, or - for stdin")
    parser.add_argument(
        "--render-only",
        action="store_true",
        help="rebuild only the Markdown summary from an existing state",
    )
    parser.add_argument("--trade-date", help="YYYY-MM-DD; required only when the JSON root is a list")
    parser.add_argument("--state", default="data/paper_trade/state.json", help="persistent state JSON path")
    parser.add_argument(
        "--summary",
        default="reports/paper_trade_20d.md",
        help="generated Markdown summary path",
    )
    parser.add_argument("--target-days", type=int, help="override the target number of trading days")
    parser.add_argument("--initial-cash", type=float, help="override initial paper cash and rebuild all dates")
    parser.add_argument(
        "--default-target-weight",
        type=float,
        help="opening weight for buy signals without target_weight (default: 0.08)",
    )
    parser.add_argument(
        "--transaction-cost-bps",
        type=float,
        help="simulated one-way transaction cost in basis points (default: 10)",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    state_path = Path(args.state)
    summary_path = Path(args.summary)
    try:
        if args.render_only:
            state = load_state(state_path)
            if state is None:
                raise TrackerInputError(f"tracker state does not exist: {state_path}")
            state = rebuild_state(state)
            atomic_write_text(summary_path, render_summary(state))
            metrics = state["metrics"]
            print(
                "paper-trade tracker summary: "
                f"baseline {metrics['baseline_days']} + "
                f"{metrics['completed_trading_days']}/{metrics['target_trading_days']} "
                "evaluated intervals"
            )
            return 0
        if not args.signals:
            raise TrackerInputError("--signals is required unless --render-only is used")
        payload = _read_json(args.signals)
        snapshot = normalise_snapshot(payload, trade_date_override=args.trade_date)
        state = load_state(state_path) or new_state(
            target_trading_days=args.target_days or DEFAULT_TARGET_DAYS,
            initial_cash=args.initial_cash or DEFAULT_INITIAL_CASH,
            default_target_weight=args.default_target_weight or DEFAULT_TARGET_WEIGHT,
            transaction_cost_bps=(
                DEFAULT_TRANSACTION_COST_BPS
                if args.transaction_cost_bps is None
                else args.transaction_cost_bps
            ),
        )
        state = update_state(
            state,
            snapshot,
            target_trading_days=args.target_days,
            initial_cash=args.initial_cash,
            default_target_weight=args.default_target_weight,
            transaction_cost_bps=args.transaction_cost_bps,
        )
        save_state(state_path, state)
        atomic_write_text(summary_path, render_summary(state))
    except (OSError, TrackerInputError) as exc:
        parser.exit(2, f"paper-trade tracker error: {exc}\n")

    metrics = state["metrics"]
    print(
        "paper-trade tracker: "
        f"baseline {metrics['baseline_days']} + "
        f"{metrics['completed_trading_days']}/{metrics['target_trading_days']} evaluated intervals, "
        f"cumulative return {_percent(metrics['cumulative_return'])}, "
        f"max drawdown {_percent(metrics['max_drawdown'])}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
