#!/usr/bin/env python3
"""Deterministic, simulation-only adaptive signal policy.

This module evaluates a long-entry plan against explicit market costs and risk
levels.  It emits candidates for *manual review* only.  It has no broker
integration, performs no network requests, and cannot place orders.

The policy deliberately separates monitoring frequency from trading frequency:
an upstream monitor may call it often, while hysteresis and after-cost risk
economics decide whether a materially new candidate is worth surfacing.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import tempfile
from dataclasses import asdict, dataclass
from dataclasses import replace as dataclass_replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, Tuple


SIMULATION_ONLY = True
PLACES_REAL_ORDERS = False

HOLD = "hold"
CONSIDER_ENTRY = "consider_entry"
RISK_EXIT_REVIEW = "risk_exit_review"

POLICY_STATE_SCHEMA_VERSION = 1
POSITION_STATES = frozenset({"held", "flat", "unknown"})
TRADING_DAYS_PER_YEAR = 252.0

_QUALITY_SCORES = {
    "excellent": 1.0,
    "high": 1.0,
    "strong": 1.0,
    "good": 0.9,
    "fair": 0.7,
    "medium": 0.7,
    "acceptable": 0.7,
    # Degraded data is intentionally non-actionable by default.
    "degraded": 0.0,
    "partial": 0.0,
    "poor": 0.0,
    "stale": 0.0,
    "missing": 0.0,
    "fetch_failed": 0.0,
    "unknown": 0.0,
}


@dataclass(frozen=True)
class MarketCosts:
    """Round-trip market-cost assumptions, expressed in basis points.

    ``entry_fee_bps`` and ``exit_fee_bps`` may include commissions, platform
    charges, taxes, and levies.  Slippage is kept separate so the report can
    explain which assumptions created the cost drag.
    """

    entry_fee_bps: float
    exit_fee_bps: float
    entry_slippage_bps: float = 0.0
    exit_slippage_bps: float = 0.0

    def round_trip_bps(self) -> float:
        values = (
            self.entry_fee_bps,
            self.exit_fee_bps,
            self.entry_slippage_bps,
            self.exit_slippage_bps,
        )
        if any(not _is_finite_number(value) or float(value) < 0 for value in values):
            raise ValueError("market costs must be finite, non-negative basis-point values")
        return sum(float(value) for value in values)

    def round_trip_rate(self) -> float:
        return self.round_trip_bps() / 10_000.0


@dataclass(frozen=True)
class AdaptiveSignalInput:
    """One candidate plan plus the incumbent policy state."""

    symbol: str
    market: str
    plan_price: Optional[float]
    stop_loss: Optional[float]
    target_price: Optional[float]
    confidence: Optional[float]
    data_quality: Any
    market_costs: Optional[MarketCosts]
    expected_holding_days: Optional[float]
    quote_price: Optional[float] = None
    data_age_seconds: Optional[float] = None
    position_state: str = "unknown"
    incumbent_annualized_utility: float = 0.0


@dataclass(frozen=True)
class AdaptivePolicyConfig:
    """Configurable safety and economic gates."""

    max_data_age_seconds: float = 90.0
    min_data_quality_score: float = 0.7
    min_confidence: float = 0.5
    min_net_reward: float = 0.0
    min_net_risk_reward: float = 1.5
    hysteresis_utility_delta: float = 0.005
    min_expected_holding_days: float = 1.0
    max_expected_holding_days: float = 252.0


@dataclass(frozen=True)
class AdaptivePolicyDecision:
    """A simulation candidate; never an executable order."""

    symbol: str
    market: str
    position_state: str
    candidate_action: str
    reason_codes: Tuple[str, ...]
    eligible_for_manual_review: bool
    risk_priority: bool
    simulation_only: bool = SIMULATION_ONLY
    places_real_orders: bool = PLACES_REAL_ORDERS
    requires_human_confirmation: bool = True
    round_trip_cost_bps: Optional[float] = None
    round_trip_cost_rate: Optional[float] = None
    gross_reward: Optional[float] = None
    gross_risk: Optional[float] = None
    net_reward: Optional[float] = None
    net_risk: Optional[float] = None
    net_risk_reward: Optional[float] = None
    expected_holding_days: Optional[float] = None
    annualized_net_reward: Optional[float] = None
    annualized_net_risk: Optional[float] = None
    confidence_adjusted_utility: Optional[float] = None
    annualized_after_cost_utility: Optional[float] = None
    data_quality_score: Optional[float] = None
    incumbent_annualized_utility: Optional[float] = None
    utility_improvement: Optional[float] = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class PolicyStateError(ValueError):
    """Raised when the simulation-only incumbent state is unsafe to use."""


def evaluate_adaptive_signal(
    signal: AdaptiveSignalInput,
    config: AdaptivePolicyConfig = AdaptivePolicyConfig(),
) -> AdaptivePolicyDecision:
    """Evaluate a plan using after-cost risk economics.

    Safety ordering is intentional:

    1. Invalid symbol/market identity or stale quote timing degrades to ``hold``.
    2. A fresh quote through a known hard stop produces an urgent
       ``risk_exit_review`` only for an explicitly held position.
    3. Normal entry candidates require an explicitly flat position plus a
       complete, directionally valid plan,
       acceptable data quality, positive after-cost reward, sufficient net
       risk/reward, and a material annualized utility improvement over the
       incumbent.
    """

    common = {
        "symbol": str(signal.symbol or "").strip().upper(),
        "market": str(signal.market or "").strip().lower(),
        "position_state": _position_state(signal.position_state),
    }
    config_error = _validate_config(config)
    if config_error:
        return _hold(common, config_error)

    identity_reasons = []
    if not common["symbol"]:
        identity_reasons.append("missing_symbol")
    if common["market"] not in {"cn", "hk"}:
        identity_reasons.append("unsupported_market")
    elif common["symbol"] and not _is_valid_symbol(common["symbol"], common["market"]):
        identity_reasons.append("invalid_symbol_format")
    if identity_reasons:
        return _hold(common, *identity_reasons)
    if common["position_state"] not in POSITION_STATES:
        return _hold(common, "unsupported_position_state")

    age = _finite_optional(signal.data_age_seconds)
    if age is None or age < 0:
        return _hold(common, "missing_or_invalid_data_age")
    if age > config.max_data_age_seconds:
        return _hold(common, "stale_data")

    plan_price = _positive_optional(signal.plan_price)
    quote_price = _positive_optional(signal.quote_price)
    stop_loss = _positive_optional(signal.stop_loss)
    # A fresh quote and known stop are enough to surface an urgent risk review
    # when the original plan price is unavailable.  When a plan price *is*
    # present, the stop must still be directionally valid (stop < entry).
    if (
        quote_price is not None
        and stop_loss is not None
        and (plan_price is None or stop_loss < plan_price)
        and quote_price <= stop_loss
    ):
        cost_bps, cost_rate = _safe_costs(signal.market_costs)
        if common["position_state"] == "held":
            return AdaptivePolicyDecision(
                **common,
                candidate_action=RISK_EXIT_REVIEW,
                reason_codes=("hard_stop_breached",),
                eligible_for_manual_review=True,
                risk_priority=True,
                round_trip_cost_bps=cost_bps,
                round_trip_cost_rate=cost_rate,
                incumbent_annualized_utility=_finite_optional(
                    signal.incumbent_annualized_utility
                ),
            )
        return _hold(
            common,
            (
                "plan_invalidated_flat_position"
                if common["position_state"] == "flat"
                else "plan_invalidated_position_unknown"
            ),
            round_trip_cost_bps=cost_bps,
            round_trip_cost_rate=cost_rate,
        )

    missing_reasons = []
    target_price = _positive_optional(signal.target_price)
    confidence = _finite_optional(signal.confidence)
    holding_days = _positive_optional(signal.expected_holding_days)
    if common["position_state"] != "flat":
        missing_reasons.append(
            "position_already_held"
            if common["position_state"] == "held"
            else "position_state_unknown"
        )
    if plan_price is None:
        missing_reasons.append("missing_or_invalid_plan_price")
    if stop_loss is None:
        missing_reasons.append("missing_or_invalid_stop_loss")
    if target_price is None:
        missing_reasons.append("missing_or_invalid_target_price")
    if confidence is None or not 0.0 <= confidence <= 1.0:
        missing_reasons.append("missing_or_invalid_confidence")
    if holding_days is None:
        missing_reasons.append("missing_or_invalid_expected_holding_days")
    elif not (
        config.min_expected_holding_days
        <= holding_days
        <= config.max_expected_holding_days
    ):
        missing_reasons.append("expected_holding_days_out_of_range")

    quality_score = _quality_score(signal.data_quality)
    if quality_score is None:
        missing_reasons.append("missing_or_invalid_data_quality")

    cost_bps, cost_rate = _safe_costs(signal.market_costs)
    if cost_bps is None or cost_rate is None:
        missing_reasons.append("missing_or_invalid_market_costs")

    incumbent_utility = _finite_optional(signal.incumbent_annualized_utility)
    if incumbent_utility is None:
        missing_reasons.append("missing_or_invalid_incumbent_annualized_utility")

    if missing_reasons:
        return _hold(
            common,
            *missing_reasons,
            data_quality_score=quality_score,
            round_trip_cost_bps=cost_bps,
            round_trip_cost_rate=cost_rate,
        )

    assert plan_price is not None
    assert stop_loss is not None
    assert target_price is not None
    assert confidence is not None
    assert holding_days is not None
    assert quality_score is not None
    assert cost_bps is not None
    assert cost_rate is not None
    assert incumbent_utility is not None

    if not stop_loss < plan_price < target_price:
        return _hold(
            common,
            "invalid_price_direction",
            data_quality_score=quality_score,
            round_trip_cost_bps=cost_bps,
            round_trip_cost_rate=cost_rate,
        )

    gross_reward = target_price / plan_price - 1.0
    gross_risk = 1.0 - stop_loss / plan_price
    net_reward = gross_reward - cost_rate
    net_risk = gross_risk + cost_rate
    net_risk_reward = net_reward / net_risk
    holding_period_utility = quality_score * (
        confidence * net_reward - (1.0 - confidence) * net_risk
    )
    annualization_factor = TRADING_DAYS_PER_YEAR / holding_days
    annualized_net_reward = net_reward * annualization_factor
    annualized_net_risk = net_risk * annualization_factor
    annualized_utility = holding_period_utility * annualization_factor
    improvement = annualized_utility - incumbent_utility

    metrics = {
        "round_trip_cost_bps": cost_bps,
        "round_trip_cost_rate": cost_rate,
        "gross_reward": gross_reward,
        "gross_risk": gross_risk,
        "net_reward": net_reward,
        "net_risk": net_risk,
        "net_risk_reward": net_risk_reward,
        "expected_holding_days": holding_days,
        "annualized_net_reward": annualized_net_reward,
        "annualized_net_risk": annualized_net_risk,
        "confidence_adjusted_utility": holding_period_utility,
        "annualized_after_cost_utility": annualized_utility,
        "data_quality_score": quality_score,
        "incumbent_annualized_utility": incumbent_utility,
        "utility_improvement": improvement,
    }

    gates = []
    if quality_score < config.min_data_quality_score:
        gates.append("data_quality_below_threshold")
    if confidence < config.min_confidence:
        gates.append("confidence_below_threshold")
    if net_reward <= config.min_net_reward:
        gates.append("net_reward_below_threshold")
    if net_risk_reward < config.min_net_risk_reward:
        gates.append("net_risk_reward_below_threshold")
    if improvement < config.hysteresis_utility_delta:
        gates.append("hysteresis_not_cleared")

    if gates:
        return AdaptivePolicyDecision(
            **common,
            candidate_action=HOLD,
            reason_codes=tuple(gates),
            eligible_for_manual_review=False,
            risk_priority=False,
            **metrics,
        )

    return AdaptivePolicyDecision(
        **common,
        candidate_action=CONSIDER_ENTRY,
        reason_codes=("after_cost_risk_adjusted_candidate",),
        eligible_for_manual_review=True,
        risk_priority=False,
        **metrics,
    )


def input_from_mapping(payload: Mapping[str, Any]) -> AdaptiveSignalInput:
    """Build :class:`AdaptiveSignalInput` from a JSON-compatible mapping."""

    raw_costs = payload.get("market_costs")
    costs: Optional[MarketCosts] = None
    if isinstance(raw_costs, Mapping):
        try:
            costs = MarketCosts(
                entry_fee_bps=raw_costs.get("entry_fee_bps"),
                exit_fee_bps=raw_costs.get("exit_fee_bps"),
                entry_slippage_bps=raw_costs.get("entry_slippage_bps", 0.0),
                exit_slippage_bps=raw_costs.get("exit_slippage_bps", 0.0),
            )
        except TypeError:
            costs = None
    return AdaptiveSignalInput(
        symbol=str(payload.get("symbol") or ""),
        market=str(payload.get("market") or ""),
        plan_price=payload.get("plan_price"),
        stop_loss=payload.get("stop_loss"),
        target_price=payload.get("target_price"),
        confidence=payload.get("confidence"),
        data_quality=payload.get("data_quality"),
        market_costs=costs,
        expected_holding_days=payload.get("expected_holding_days"),
        quote_price=payload.get("quote_price"),
        data_age_seconds=payload.get("data_age_seconds"),
        position_state=payload.get("position_state", "unknown"),
        incumbent_annualized_utility=payload.get(
            "incumbent_annualized_utility",
            0.0,
        ),
    )


def _hold(
    common: Mapping[str, str],
    *reason_codes: str,
    data_quality_score: Optional[float] = None,
    round_trip_cost_bps: Optional[float] = None,
    round_trip_cost_rate: Optional[float] = None,
) -> AdaptivePolicyDecision:
    return AdaptivePolicyDecision(
        symbol=common["symbol"],
        market=common["market"],
        position_state=common["position_state"],
        candidate_action=HOLD,
        reason_codes=tuple(reason_codes),
        eligible_for_manual_review=False,
        risk_priority=False,
        data_quality_score=data_quality_score,
        round_trip_cost_bps=round_trip_cost_bps,
        round_trip_cost_rate=round_trip_cost_rate,
    )


def _position_state(value: Any) -> str:
    return str(value or "unknown").strip().lower() or "unknown"


def _is_valid_symbol(symbol: str, market: str) -> bool:
    if market == "cn":
        return re.fullmatch(r"\d{6}", symbol) is not None
    if market == "hk":
        return re.fullmatch(r"HK\d{5}", symbol) is not None
    return False


def _validate_config(config: AdaptivePolicyConfig) -> Optional[str]:
    values = (
        config.max_data_age_seconds,
        config.min_data_quality_score,
        config.min_confidence,
        config.min_net_reward,
        config.min_net_risk_reward,
        config.hysteresis_utility_delta,
        config.min_expected_holding_days,
        config.max_expected_holding_days,
    )
    if any(not _is_finite_number(value) for value in values):
        return "invalid_policy_config"
    if config.max_data_age_seconds < 0:
        return "invalid_policy_config"
    if not 0 <= config.min_data_quality_score <= 1:
        return "invalid_policy_config"
    if not 0 <= config.min_confidence <= 1:
        return "invalid_policy_config"
    if config.min_net_risk_reward < 0 or config.hysteresis_utility_delta < 0:
        return "invalid_policy_config"
    if not (
        1.0 <= config.min_expected_holding_days
        <= config.max_expected_holding_days
        <= TRADING_DAYS_PER_YEAR
    ):
        return "invalid_policy_config"
    return None


def _quality_score(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if _is_finite_number(value):
        score = float(value)
        return score if 0.0 <= score <= 1.0 else None
    text = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    return _QUALITY_SCORES.get(text)


def _safe_costs(costs: Optional[MarketCosts]) -> tuple[Optional[float], Optional[float]]:
    if costs is None:
        return None, None
    try:
        bps = costs.round_trip_bps()
    except (TypeError, ValueError):
        return None, None
    return bps, bps / 10_000.0


def _is_finite_number(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _finite_optional(value: Any) -> Optional[float]:
    return float(value) if _is_finite_number(value) else None


def _positive_optional(value: Any) -> Optional[float]:
    number = _finite_optional(value)
    return number if number is not None and number > 0 else None


def new_policy_state() -> dict[str, Any]:
    """Return an empty, simulation-only incumbent state."""

    return {
        "schema_version": POLICY_STATE_SCHEMA_VERSION,
        "simulation_only": True,
        "places_real_orders": False,
        "records": {},
    }


def load_policy_state(path: Path) -> dict[str, Any]:
    """Load and validate adaptive policy state, or return an empty state."""

    if not path.exists():
        return new_policy_state()
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PolicyStateError(f"cannot read adaptive policy state {path}: {exc}") from exc
    _validate_policy_state(state)
    return state


def save_policy_state(path: Path, state: Mapping[str, Any]) -> None:
    """Atomically save a validated simulation-only policy state."""

    _validate_policy_state(state)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def incumbent_annualized_utility(
    state: Mapping[str, Any],
    *,
    symbol: str,
    market: str,
) -> float:
    """Return the comparable incumbent utility for one market/symbol."""

    _validate_policy_state(state)
    key = _state_key(symbol, market)
    record = state["records"].get(key)
    if not isinstance(record, Mapping):
        return 0.0
    value = _finite_optional(record.get("incumbent_annualized_utility"))
    if value is None:
        raise PolicyStateError(f"invalid incumbent utility for {key}")
    return value


def update_policy_state(
    state: Mapping[str, Any],
    decision: AdaptivePolicyDecision,
    *,
    evaluated_at: Optional[str] = None,
) -> dict[str, Any]:
    """Record one decision without ever creating an executable instruction."""

    _validate_policy_state(state)
    if not decision.simulation_only or decision.places_real_orders:
        raise PolicyStateError("only simulation-only, non-order decisions may be persisted")
    key = _state_key(decision.symbol, decision.market)
    if not key:
        raise PolicyStateError("cannot persist an invalid symbol or market")

    records = {
        str(record_key): dict(record_value)
        for record_key, record_value in state["records"].items()
    }
    previous = records.get(key, {})
    incumbent = _finite_optional(previous.get("incumbent_annualized_utility"))
    if incumbent is None:
        incumbent = 0.0
    if (
        decision.candidate_action == CONSIDER_ENTRY
        and decision.eligible_for_manual_review
        and _is_finite_number(decision.annualized_after_cost_utility)
    ):
        incumbent = float(decision.annualized_after_cost_utility)
    elif "plan_invalidated_flat_position" in decision.reason_codes:
        incumbent = 0.0

    records[key] = {
        "symbol": decision.symbol,
        "market": decision.market,
        "position_state": decision.position_state,
        "incumbent_annualized_utility": incumbent,
        "last_candidate_action": decision.candidate_action,
        "last_reason_codes": list(decision.reason_codes),
        "last_annualized_after_cost_utility": decision.annualized_after_cost_utility,
        "last_expected_holding_days": decision.expected_holding_days,
        "evaluated_at": _normalise_evaluated_at(evaluated_at),
        "simulation_only": True,
        "places_real_orders": False,
    }
    candidate = new_policy_state()
    candidate["records"] = records
    _validate_policy_state(candidate)
    return candidate


def evaluate_and_persist(
    signal: AdaptiveSignalInput,
    *,
    state_path: Path,
    config: AdaptivePolicyConfig = AdaptivePolicyConfig(),
    evaluated_at: Optional[str] = None,
) -> AdaptivePolicyDecision:
    """Minute-monitor integration helper: load incumbent, evaluate, save atomically."""

    state = load_policy_state(state_path)
    symbol = str(signal.symbol or "").strip().upper()
    market = str(signal.market or "").strip().lower()
    key = _state_key(symbol, market)
    incumbent = (
        incumbent_annualized_utility(state, symbol=symbol, market=market)
        if key
        else 0.0
    )
    evaluated_signal = dataclass_replace(
        signal,
        incumbent_annualized_utility=incumbent,
    )
    decision = evaluate_adaptive_signal(evaluated_signal, config)
    if _state_key(decision.symbol, decision.market):
        updated = update_policy_state(state, decision, evaluated_at=evaluated_at)
        save_policy_state(state_path, updated)
    return decision


def _state_key(symbol: str, market: str) -> str:
    normalized_symbol = str(symbol or "").strip().upper()
    normalized_market = str(market or "").strip().lower()
    if not _is_valid_symbol(normalized_symbol, normalized_market):
        return ""
    return f"{normalized_market}:{normalized_symbol}"


def _validate_policy_state(state: Any) -> None:
    if not isinstance(state, Mapping):
        raise PolicyStateError("adaptive policy state must be an object")
    if state.get("schema_version") != POLICY_STATE_SCHEMA_VERSION:
        raise PolicyStateError("unsupported adaptive policy state schema")
    if state.get("simulation_only") is not True or state.get("places_real_orders") is not False:
        raise PolicyStateError("adaptive policy state must remain simulation-only")
    records = state.get("records")
    if not isinstance(records, Mapping):
        raise PolicyStateError("adaptive policy state records must be an object")
    for key, record in records.items():
        if not isinstance(record, Mapping):
            raise PolicyStateError(f"adaptive policy record {key!r} must be an object")
        expected_key = _state_key(record.get("symbol"), record.get("market"))
        if not expected_key or str(key) != expected_key:
            raise PolicyStateError(f"adaptive policy record key mismatch: {key!r}")
        if (
            record.get("simulation_only") is not True
            or record.get("places_real_orders") is not False
        ):
            raise PolicyStateError(f"adaptive policy record {key!r} is not simulation-only")
        if not _is_finite_number(record.get("incumbent_annualized_utility")):
            raise PolicyStateError(f"adaptive policy record {key!r} has invalid incumbent utility")


def _normalise_evaluated_at(value: Optional[str]) -> str:
    if value is None:
        return datetime.now(timezone.utc).isoformat()
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PolicyStateError("evaluated_at must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise PolicyStateError("evaluated_at must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat()


def _read_json(path: str) -> Any:
    source = sys.stdin.read() if path == "-" else Path(path).read_text(encoding="utf-8")
    return json.loads(source)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", help="JSON input file, or - for stdin")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        payload = _read_json(args.input)
        if not isinstance(payload, Mapping):
            raise ValueError("input JSON must be an object")
        decision = evaluate_adaptive_signal(input_from_mapping(payload))
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        parser.exit(2, f"adaptive policy error: {exc}\n")
    print(json.dumps(decision.as_dict(), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
