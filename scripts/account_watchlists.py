#!/usr/bin/env python3
"""Public account-layer symbols and private watch-position input validation.

Only symbols, display names, account roles, and held/candidate status belong in
this public module.  Quantities, costs, asset values, and cash snapshots must be
supplied at runtime through ``WATCH_ACCOUNTS_PRIVATE_JSON`` (normally a GitHub
Secret).  The parser deliberately returns data without logging or persisting it.

Private JSON schema::

    {
      "secondary_account": {
        "positions": [
          {"symbol": "563230", "quantity": 1000, "historical_cost": 1.23}
        ],
        "informational_snapshot": {"captured_at": "...", "total_assets": 1}
      },
      "sister_managed": {
        "positions": [
          {"symbol": "002594", "quantity": 100, "historical_cost": 50}
        ]
      }
    }

Numbers above are intentionally fictitious examples.  Snapshot fields are
informational only and must never be treated as durable purchasing power.
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Tuple

from scripts.normalize_stock_list import canonical_symbol


WATCH_ACCOUNTS_PRIVATE_ENV = "WATCH_ACCOUNTS_PRIVATE_JSON"

PRIMARY_SYMBOLS: Tuple[str, ...] = (
    "688333",
    "300499",
    "688608",
    "300408",
    "002185",
    "688135",
    "600094",
    "601318",
    "302132",
    "HK01347",
    "HK00981",
    "HK06181",
    "HK02522",
    "HK06166",
)


@dataclass(frozen=True)
class WatchInstrument:
    symbol: str
    name: str
    status: str


@dataclass(frozen=True)
class WatchLayer:
    key: str
    push_prefix: str
    instruments: Tuple[WatchInstrument, ...]


FAMILY_WATCHLIST = WatchLayer(
    key="FAMILY_WATCHLIST",
    push_prefix="【父亲账户观察】",
    instruments=(
        WatchInstrument("000100", "TCL科技", "held_unknown_size"),
        WatchInstrument("000725", "京东方A", "held_unknown_size"),
        WatchInstrument("603296", "华勤技术", "held_unknown_size"),
        WatchInstrument("301308", "江波龙", "held_unknown_size"),
    ),
)

SECONDARY_ACCOUNT_WATCH = WatchLayer(
    key="SECONDARY_ACCOUNT_WATCH",
    push_prefix="【第二账户】",
    instruments=(
        WatchInstrument("563230", "卫星ETF", "held_private_size"),
        WatchInstrument("002759", "ST天际", "candidate"),
    ),
)

SISTER_MANAGED_WATCH = WatchLayer(
    key="SISTER_MANAGED_WATCH",
    push_prefix="【妹妹账户】",
    instruments=(
        WatchInstrument("HK01347", "华虹半导体", "held_private_size"),
        WatchInstrument("002594", "比亚迪", "held_private_size"),
        WatchInstrument("HK06181", "老铺黄金", "held_private_size"),
        WatchInstrument("HK00981", "中芯国际H", "held_private_size"),
        WatchInstrument("603083", "剑桥科技A", "held_private_size"),
        WatchInstrument("HK09988", "阿里巴巴", "held_private_size"),
        WatchInstrument("002095", "生意宝", "held_private_size"),
        WatchInstrument("601727", "上海电气", "held_private_size"),
        WatchInstrument("563230", "卫星ETF", "held_private_size"),
        WatchInstrument("HK02522", "一脉阳光", "held_private_size"),
    ),
)

WATCH_LAYERS: Tuple[WatchLayer, ...] = (
    FAMILY_WATCHLIST,
    SECONDARY_ACCOUNT_WATCH,
    SISTER_MANAGED_WATCH,
)


def _unique(symbols: Tuple[str, ...] | list[str]) -> Tuple[str, ...]:
    return tuple(dict.fromkeys(canonical_symbol(symbol) for symbol in symbols))


def priority_analysis_pools() -> Dict[str, Tuple[str, ...]]:
    """Return P0-P3 canonical pools, deduplicated across earlier priorities."""

    p0 = _unique(list(PRIMARY_SYMBOLS))
    seen = set(p0)

    p1 = tuple(
        instrument.symbol
        for instrument in FAMILY_WATCHLIST.instruments
        if instrument.symbol not in seen
    )
    seen.update(p1)

    p2_raw = [
        instrument.symbol
        for layer in (SECONDARY_ACCOUNT_WATCH, SISTER_MANAGED_WATCH)
        for instrument in layer.instruments
        if instrument.status != "candidate"
    ]
    p2 = tuple(symbol for symbol in _unique(p2_raw) if symbol not in seen)
    seen.update(p2)

    p3_raw = [
        instrument.symbol
        for layer in WATCH_LAYERS
        for instrument in layer.instruments
        if instrument.status == "candidate"
    ]
    p3 = tuple(symbol for symbol in _unique(p3_raw) if symbol not in seen)
    return {
        "P0_PRIMARY": p0,
        "P1_FAMILY": p1,
        "P2_ACCOUNT_HOLDINGS": p2,
        "P3_CANDIDATES": p3,
    }


def tiered_daily_symbols() -> Dict[str, Tuple[str, ...]]:
    """Stable alias used by the close-analysis workflow."""

    return priority_analysis_pools()


def all_quote_symbols() -> Tuple[str, ...]:
    """Return one deduplicated quote universe in priority order."""

    pools = priority_analysis_pools()
    return _unique([symbol for values in pools.values() for symbol in values])


def watch_contexts_by_symbol() -> Dict[str, Tuple[Dict[str, str], ...]]:
    """Map each symbol to public, account-isolated notification contexts."""

    result: Dict[str, list[Dict[str, str]]] = {}
    for layer in WATCH_LAYERS:
        for instrument in layer.instruments:
            result.setdefault(instrument.symbol, []).append(
                {
                    "layer": layer.key,
                    "push_prefix": layer.push_prefix,
                    "symbol": instrument.symbol,
                    "name": instrument.name,
                    "status": instrument.status,
                }
            )
    return {symbol: tuple(contexts) for symbol, contexts in result.items()}


def _positive_number(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a positive number")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a positive number") from exc
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"{field} must be a positive number")
    return number


def load_private_watch_config(raw: Optional[str] = None) -> Dict[str, Any]:
    """Validate private holdings without printing, logging, or persisting them."""

    source = raw if raw is not None else os.getenv(WATCH_ACCOUNTS_PRIVATE_ENV, "")
    if not str(source or "").strip():
        return {}
    try:
        payload = json.loads(str(source))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{WATCH_ACCOUNTS_PRIVATE_ENV} is not valid JSON") from exc
    if not isinstance(payload, Mapping):
        raise ValueError(f"{WATCH_ACCOUNTS_PRIVATE_ENV} must be a JSON object")

    permitted = {
        "secondary_account": {
            instrument.symbol
            for instrument in SECONDARY_ACCOUNT_WATCH.instruments
            if instrument.status != "candidate"
        },
        "sister_managed": {
            instrument.symbol for instrument in SISTER_MANAGED_WATCH.instruments
        },
    }
    normalised: Dict[str, Any] = {}
    unknown_accounts = set(payload) - set(permitted)
    if unknown_accounts:
        raise ValueError("private watch config contains an unsupported account layer")
    for account, allowed_symbols in permitted.items():
        raw_account = payload.get(account)
        if raw_account is None:
            continue
        if not isinstance(raw_account, Mapping):
            raise ValueError(f"{account} must be a JSON object")
        positions = raw_account.get("positions", [])
        if not isinstance(positions, list):
            raise ValueError(f"{account}.positions must be a list")
        seen = set()
        checked = []
        for index, item in enumerate(positions):
            if not isinstance(item, Mapping):
                raise ValueError(f"{account}.positions[{index}] must be an object")
            symbol = canonical_symbol(str(item.get("symbol") or ""))
            if symbol not in allowed_symbols or symbol in seen:
                raise ValueError(f"{account} contains an unsupported or duplicate symbol")
            seen.add(symbol)
            checked.append(
                {
                    "symbol": symbol,
                    "quantity": _positive_number(
                        item.get("quantity"), f"{account}.{symbol}.quantity"
                    ),
                    "historical_cost": _positive_number(
                        item.get("historical_cost"),
                        f"{account}.{symbol}.historical_cost",
                    ),
                }
            )
        account_result: Dict[str, Any] = {"positions": checked}
        snapshot = raw_account.get("informational_snapshot")
        if snapshot is not None:
            if not isinstance(snapshot, Mapping):
                raise ValueError(f"{account}.informational_snapshot must be an object")
            # The caller may display this as an explicitly dated snapshot, but
            # it must not be used as durable cash or automatic position sizing.
            account_result["informational_snapshot"] = dict(snapshot)
        normalised[account] = account_result
    return normalised


__all__ = [
    "FAMILY_WATCHLIST",
    "PRIMARY_SYMBOLS",
    "SECONDARY_ACCOUNT_WATCH",
    "SISTER_MANAGED_WATCH",
    "WATCH_ACCOUNTS_PRIVATE_ENV",
    "WATCH_LAYERS",
    "WatchInstrument",
    "WatchLayer",
    "all_quote_symbols",
    "load_private_watch_config",
    "priority_analysis_pools",
    "tiered_daily_symbols",
    "watch_contexts_by_symbol",
]
