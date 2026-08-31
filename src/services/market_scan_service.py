# -*- coding: utf-8 -*-
"""Deterministic A-share and Hong Kong Stock Connect market scanner.

The scanner deliberately separates cheap, vectorised market-wide work from
expensive per-symbol and LLM work:

* L1 normalises and filters full-market snapshots without invoking an LLM.
* L2 loads history for at most ``top_a_history + top_hk_history`` symbols.
* L3 sends only the final shortlist to two independent reviewers.

All external providers are injectable so unit tests never need network, model,
notification, or broker access.  This module has no order-placement capability.
"""

from __future__ import annotations

import copy
import json
import math
import os
import re
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from src.services.position_sizing_policy import classify_opportunity


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
SCHEMA_VERSION = 1
MARKET_A = "A"
MARKET_HK = "HK_CONNECT"
INITIAL_POSITION_FRACTION = 0.025
HK_MEMBERSHIP_WARNING_THRESHOLDS = (168.0, 72.0, 24.0)

SnapshotLoader = Callable[[], Any]
HistoryLoader = Callable[[str, int], Any]
Reviewer = Callable[[Sequence[Mapping[str, Any]]], Any]
Clock = Callable[[], datetime]
Sleeper = Callable[[float], None]


class MarketScanError(RuntimeError):
    """Raised when a scan cannot satisfy its safety contract."""


def _membership_cache_warning(
    age_hours: float, max_age_hours: float
) -> Dict[str, Any]:
    remaining = max(0.0, float(max_age_hours) - float(age_hours))
    threshold = next(
        (
            value
            for value in sorted(HK_MEMBERSHIP_WARNING_THRESHOLDS)
            if remaining <= value
        ),
        None,
    )
    level = (
        "critical"
        if threshold == 24.0
        else "warning"
        if threshold == 72.0
        else "notice"
        if threshold == 168.0
        else "none"
    )
    return {
        "membership_remaining_hours": round(remaining, 4),
        "membership_warning_level": level,
        "membership_warning_threshold_hours": threshold,
    }


@dataclass(frozen=True)
class MarketScanConfig:
    """Runtime limits and guardrails for one scan."""

    top_a_history: int = 40
    top_hk_history: int = 20
    final_top_n: int = 12
    history_lookback_days: int = 120
    min_history_bars: int = 60
    min_a_amount: float = 50_000_000.0
    min_hk_amount: float = 20_000_000.0
    max_a_change_pct: float = 9.5
    max_hk_change_pct: float = 15.0
    min_listing_days: int = 20
    max_ma20_extension: float = 0.15
    max_annualized_volatility: float = 1.10
    max_drawdown: float = 0.40
    min_net_rr: float = 1.8
    max_stop_distance_pct: float = 0.12
    a_round_trip_cost_bps: float = 25.0
    hk_round_trip_cost_bps: float = 50.0
    a_cache_max_age_hours: float = 6.0
    hk_cache_max_age_hours: float = 6.0
    hk_membership_cache_max_age_hours: float = 24.0 * 35.0
    min_actionable_data_quality: float = 0.70
    enabled_markets: Tuple[str, ...] = (MARKET_A, MARKET_HK)
    snapshot_retries: int = 2
    snapshot_retry_backoff_seconds: float = 0.0
    a_cache_path: Path = Path("data/market_scan/a_share_snapshot.json")
    hk_cache_path: Path = Path("data/market_scan/hk_connect_snapshot.json")
    hk_membership_cache_path: Path = Path(
        "data/market_scan/hk_connect_membership.json"
    )

    def __post_init__(self) -> None:
        if self.top_a_history < 1 or self.top_hk_history < 1:
            raise ValueError("history shortlist sizes must be positive")
        if not 1 <= self.final_top_n <= self.top_a_history + self.top_hk_history:
            raise ValueError("final_top_n must fit inside the history shortlist")
        if self.min_history_bars < 30:
            raise ValueError("min_history_bars must be at least 30")
        if self.min_net_rr <= 0:
            raise ValueError("min_net_rr must be positive")
        if self.snapshot_retries < 1 or self.snapshot_retries > 5:
            raise ValueError("snapshot_retries must be between 1 and 5")
        if self.snapshot_retry_backoff_seconds < 0:
            raise ValueError("snapshot_retry_backoff_seconds cannot be negative")
        if self.hk_membership_cache_max_age_hours <= 0:
            raise ValueError("hk_membership_cache_max_age_hours must be positive")
        if not 0.70 <= self.min_actionable_data_quality <= 1:
            raise ValueError(
                "min_actionable_data_quality must be between 0.70 and 1"
            )
        enabled = tuple(dict.fromkeys(self.enabled_markets))
        if not enabled or any(item not in {MARKET_A, MARKET_HK} for item in enabled):
            raise ValueError("enabled_markets must contain A and/or HK_CONNECT")
        object.__setattr__(self, "enabled_markets", enabled)


@dataclass
class L1Result:
    """Market-wide deterministic screening result."""

    a_candidates: List[Dict[str, Any]]
    hk_candidates: List[Dict[str, Any]]
    as_of: Dict[str, str]
    diagnostics: Dict[str, Any]
    a_safe_halt: bool = False
    hk_safe_halt: bool = False
    push_block_reasons: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class TradePlanValidation:
    valid: bool
    reasons: Tuple[str, ...]
    net_rr: Optional[float]


def _now_shanghai() -> datetime:
    return datetime.now(tz=SHANGHAI_TZ)


def _iso_datetime(value: Any, *, fallback: Optional[datetime] = None) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except (TypeError, ValueError):
            parsed = fallback or _now_shanghai()
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed.astimezone(SHANGHAI_TZ).isoformat(timespec="seconds")


def _parse_datetime(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed.astimezone(timezone.utc)


def _finite_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value) if math.isfinite(float(value)) else None
    if isinstance(value, (pd.Timestamp, datetime)):
        return _iso_datetime(value)
    if pd.isna(value):
        return None
    return value


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(file_descriptor, "w", encoding="utf-8") as handle:
            json.dump(_json_safe(payload), handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    finally:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass


def _normalise_symbol(value: Any, market: str) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if market == MARKET_HK:
        match = re.fullmatch(r"HK[.\-]?(\d{1,5})", text)
        if not match:
            match = re.fullmatch(r"(\d{1,5})[.\-]?HK", text)
        if not match and text.isdigit() and len(text) <= 5:
            match = re.fullmatch(r"(\d{1,5})", text)
        return f"HK{match.group(1).zfill(5)}" if match else ""

    match = re.fullmatch(r"(?:SH|SZ|BJ|SS)[.\-]?(\d{6})", text)
    if not match:
        match = re.fullmatch(r"(\d{6})[.\-]?(?:SH|SZ|BJ|SS)", text)
    if not match and text.isdigit() and len(text) == 6:
        match = re.fullmatch(r"(\d{6})", text)
    return match.group(1) if match else ""


def _coerce_snapshot_payload(
    raw: Any,
    *,
    now: datetime,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    metadata: Dict[str, Any] = {}
    records = raw
    if isinstance(raw, tuple) and len(raw) == 2:
        records, raw_metadata = raw
        if isinstance(raw_metadata, Mapping):
            metadata = dict(raw_metadata)
    elif isinstance(raw, Mapping):
        metadata = {
            key: value
            for key, value in raw.items()
            if key not in {"records", "items", "data", "stocks"}
        }
        for key in ("records", "items", "data", "stocks"):
            if key in raw:
                records = raw[key]
                break
    frame = records.copy() if isinstance(records, pd.DataFrame) else pd.DataFrame(records or [])
    raw_as_of = str(metadata.get("as_of") or "").strip()
    metadata["as_of"] = _iso_datetime(raw_as_of) if raw_as_of else ""
    metadata["fetched_at"] = _iso_datetime(metadata.get("fetched_at"), fallback=now)
    return frame, metadata


SNAPSHOT_ALIASES: Dict[str, Tuple[str, ...]] = {
    "code": ("code", "symbol", "stock_code", "代码", "股票代码", "证券代码"),
    "name": ("name", "stock_name", "名称", "股票名称", "证券简称", "中文名称"),
    "price": ("price", "latest", "last", "最新价", "现价", "收盘"),
    "change_pct": ("change_pct", "pct_chg", "涨跌幅", "涨幅"),
    "volume": ("volume", "vol", "成交量"),
    "amount": ("amount", "turnover", "成交额", "成交金额"),
    "turnover_rate": ("turnover_rate", "换手率"),
    "volume_ratio": ("volume_ratio", "量比"),
    "pe": ("pe", "pe_ratio", "市盈率-动态", "市盈率"),
    "pb": ("pb", "pb_ratio", "市净率"),
    "total_mv": ("total_mv", "market_cap", "总市值"),
    "change_60d": ("change_60d", "60日涨跌幅"),
    "list_date": ("list_date", "上市日期"),
    "listing_days": ("listing_days", "上市天数"),
    "is_connect": ("is_connect", "港股通", "is_hk_connect"),
}


def _first_column(frame: pd.DataFrame, aliases: Iterable[str]) -> Optional[pd.Series]:
    for name in aliases:
        if name in frame.columns:
            return frame[name]
    return None


def _normalise_snapshot(frame: pd.DataFrame, *, market: str, now: datetime) -> pd.DataFrame:
    normalised = pd.DataFrame(index=frame.index)
    for target, aliases in SNAPSHOT_ALIASES.items():
        source = _first_column(frame, aliases)
        if source is not None:
            normalised[target] = source

    if "code" not in normalised.columns:
        return pd.DataFrame(columns=("code", "name", "market"))
    normalised["code"] = normalised["code"].map(lambda value: _normalise_symbol(value, market))
    normalised["name"] = normalised.get("name", pd.Series("", index=normalised.index)).fillna("").astype(str).str.strip()
    normalised["market"] = market

    for column in (
        "price",
        "change_pct",
        "volume",
        "amount",
        "turnover_rate",
        "volume_ratio",
        "pe",
        "pb",
        "total_mv",
        "change_60d",
        "listing_days",
    ):
        if column in normalised.columns:
            normalised[column] = pd.to_numeric(normalised[column], errors="coerce")
        else:
            normalised[column] = np.nan

    if "list_date" in normalised.columns:
        list_dates = pd.to_datetime(normalised["list_date"].astype(str), errors="coerce")
        calculated_days = (pd.Timestamp(now.date()) - list_dates.dt.normalize()).dt.days
        normalised["listing_days"] = normalised["listing_days"].fillna(calculated_days)
    if "is_connect" in normalised.columns:
        normalised["is_connect"] = normalised["is_connect"].map(
            lambda value: str(value).strip().lower() in {"1", "true", "yes", "y", "是", "沪港通", "深港通"}
            if not isinstance(value, bool)
            else value
        )

    normalised = normalised[normalised["code"].astype(bool)]
    normalised = normalised.drop_duplicates(subset=["code"], keep="first")
    return normalised.reset_index(drop=True)


def _filter_and_rank_l1(
    frame: pd.DataFrame,
    *,
    market: str,
    config: MarketScanConfig,
    limit: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    total = len(frame)
    if total == 0:
        return [], {"input_count": 0, "eligible_count": 0, "filtered": {"empty": 0}}

    min_amount = config.min_a_amount if market == MARKET_A else config.min_hk_amount
    max_change = config.max_a_change_pct if market == MARKET_A else config.max_hk_change_pct

    required_missing = (
        frame["price"].isna()
        | frame["change_pct"].isna()
        | frame["volume"].isna()
        | frame["amount"].isna()
    )
    invalid_name = pd.Series(False, index=frame.index)
    new_listing_name = pd.Series(False, index=frame.index)
    if market == MARKET_A:
        invalid_name = frame["name"].str.upper().str.contains(
            r"(?:^|\*)ST|退$|退市",
            regex=True,
            na=False,
        )
        # AkShare's realtime snapshot normally has no listing date.  N/C are
        # exchange status prefixes for the first day / early post-IPO period.
        new_listing_name = frame["name"].str.upper().str.contains(
            r"^[NCＮＣ]",
            regex=True,
            na=False,
        )
    suspended = (frame["price"] <= 0) | (frame["volume"] <= 0) | (frame["amount"] <= 0)
    illiquid = frame["amount"] < min_amount
    overextended = frame["change_pct"] > max_change
    too_new = frame["listing_days"].notna() & (frame["listing_days"] < config.min_listing_days)

    eligible_mask = ~(
        required_missing
        | invalid_name
        | new_listing_name
        | suspended
        | illiquid
        | overextended
        | too_new
    )
    eligible = frame.loc[eligible_mask].copy()
    diagnostics = {
        "input_count": total,
        "eligible_count": len(eligible),
        "filtered": {
            "data_missing": int(required_missing.sum()),
            "st_or_delisting": int(invalid_name.sum()),
            "new_listing_name": int(new_listing_name.sum()),
            "suspended_or_zero_turnover": int(suspended.sum()),
            "amount_below_minimum": int(illiquid.sum()),
            "extreme_chase": int(overextended.sum()),
            "listing_history_too_short": int(too_new.sum()),
        },
        "minimum_amount": min_amount,
        "maximum_change_pct": max_change,
    }
    if eligible.empty:
        return [], diagnostics

    amount_rank = eligible["amount"].rank(pct=True).fillna(0.0)
    change_score = (1.0 - (eligible["change_pct"] - 3.0).abs() / max(max_change, 1.0)).clip(0.0, 1.0)
    turnover = eligible["turnover_rate"].clip(lower=0.0)
    turnover_score = (turnover / 8.0).clip(0.0, 1.0).fillna(0.25)
    volume_ratio = eligible["volume_ratio"].clip(lower=0.0)
    volume_score = (volume_ratio / 2.0).clip(0.0, 1.0).fillna(0.25)
    change_60d = eligible["change_60d"].clip(lower=-30.0, upper=80.0)
    change_60d_score = ((change_60d + 30.0) / 110.0).fillna(0.5)

    eligible["l1_score"] = (
        amount_rank * 40.0
        + change_score * 25.0
        + turnover_score * 15.0
        + volume_score * 10.0
        + change_60d_score * 10.0
    )
    eligible = eligible.sort_values(
        ["l1_score", "amount", "code"],
        ascending=[False, False, True],
        kind="stable",
    ).head(limit)
    return [_json_safe(record) for record in eligible.to_dict(orient="records")], diagnostics


def _normalise_history(raw: Any) -> Tuple[pd.DataFrame, str]:
    source = ""
    records = raw
    if isinstance(raw, tuple) and len(raw) == 2:
        records, source = raw
    frame = records.copy() if isinstance(records, pd.DataFrame) else pd.DataFrame(records or [])
    if frame.empty:
        return frame, str(source or "")

    aliases = {
        "date": ("date", "trade_date", "datetime", "日期"),
        "open": ("open", "开盘"),
        "high": ("high", "最高"),
        "low": ("low", "最低"),
        "close": ("close", "收盘", "price"),
        "volume": ("volume", "vol", "成交量"),
        "amount": ("amount", "成交额"),
    }
    normalised = pd.DataFrame(index=frame.index)
    for target, candidates in aliases.items():
        column = _first_column(frame, candidates)
        if column is not None:
            normalised[target] = column
    if "close" not in normalised.columns:
        return pd.DataFrame(), str(source or "")
    for column in ("open", "high", "low"):
        if column not in normalised.columns:
            normalised[column] = normalised["close"]
    if "volume" not in normalised.columns:
        normalised["volume"] = 0.0
    for column in ("open", "high", "low", "close", "volume", "amount"):
        if column in normalised.columns:
            normalised[column] = pd.to_numeric(normalised[column], errors="coerce")
    if "date" in normalised.columns:
        normalised["date"] = pd.to_datetime(normalised["date"], errors="coerce")
        normalised = normalised.sort_values("date", kind="stable")
    normalised = normalised.dropna(subset=["close", "high", "low"])
    return normalised.reset_index(drop=True), str(source or "")


def _maximum_drawdown(close: pd.Series) -> float:
    rolling_peak = close.cummax()
    drawdown = close / rolling_peak - 1.0
    value = _finite_float(drawdown.min())
    return value if value is not None else -1.0


def _history_features(
    candidate: Mapping[str, Any],
    history: pd.DataFrame,
    *,
    source: str,
    config: MarketScanConfig,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if len(history) < config.min_history_bars:
        return None, "history_too_short"

    close = history["close"].astype(float)
    high = history["high"].astype(float)
    low = history["low"].astype(float)
    previous_close = close.shift(1)
    true_range = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    price = _finite_float(candidate.get("price")) or _finite_float(close.iloc[-1])
    ma20 = _finite_float(close.tail(20).mean())
    ma60 = _finite_float(close.tail(60).mean())
    previous_ma20 = _finite_float(close.iloc[-25:-5].mean()) if len(close) >= 25 else None
    atr = _finite_float(true_range.tail(14).mean())
    if not price or not ma20 or not ma60 or not atr or atr <= 0:
        return None, "history_metrics_missing"

    returns = close.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    volatility = _finite_float(returns.tail(20).std(ddof=0) * math.sqrt(252.0)) or 0.0
    drawdown = _maximum_drawdown(close.tail(60))
    momentum20 = price / float(close.iloc[-21]) - 1.0 if len(close) >= 21 and close.iloc[-21] > 0 else 0.0
    momentum60 = price / float(close.iloc[-61]) - 1.0 if len(close) >= 61 and close.iloc[-61] > 0 else 0.0
    ma20_slope = ma20 / previous_ma20 - 1.0 if previous_ma20 and previous_ma20 > 0 else 0.0
    ma20_extension = price / ma20 - 1.0

    if price < ma60 or ma20_slope < -0.01:
        return None, "weak_or_falling_trend"
    if ma20_extension > config.max_ma20_extension:
        return None, "extreme_ma20_extension"
    if volatility > config.max_annualized_volatility:
        return None, "volatility_too_high"
    if drawdown < -config.max_drawdown:
        return None, "drawdown_too_large"

    recent_low = _finite_float(low.tail(10).min()) or ma20
    support_candidates = [level for level in (recent_low, ma20) if 0 < level < price]
    support = max(support_candidates) if support_candidates else min(ma20, price - atr)
    resistance = _finite_float(high.tail(20).max()) or price

    enriched = dict(candidate)
    enriched.update(
        {
            "history_source": source,
            "history_bars": len(history),
            "price": price,
            "ma20": ma20,
            "ma60": ma60,
            "ma20_slope": ma20_slope,
            "ma20_extension": ma20_extension,
            "momentum20": momentum20,
            "momentum60": momentum60,
            "atr14": atr,
            "atr_pct": atr / price,
            "annualized_volatility20": volatility,
            "max_drawdown60": drawdown,
            "support": support,
            "resistance": resistance,
        }
    )
    return enriched, None


def _rank_l2(candidates: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    if not candidates:
        return []
    frame = pd.DataFrame(candidates)
    ranked_groups: List[pd.DataFrame] = []
    for _market, group in frame.groupby("market", sort=False):
        current = group.copy()
        momentum_raw = current["momentum20"] * 0.65 + current["momentum60"] * 0.35
        trend_raw = (
            (current["price"] > current["ma20"]).astype(float) * 0.35
            + (current["ma20"] > current["ma60"]).astype(float) * 0.35
            + current["ma20_slope"].clip(-0.05, 0.10) * 3.0
        )
        current["technical_score"] = (
            current["l1_score"] * 0.25
            + momentum_raw.rank(pct=True).fillna(0.0) * 25.0
            + trend_raw.rank(pct=True).fillna(0.0) * 25.0
            + (-current["annualized_volatility20"]).rank(pct=True).fillna(0.0) * 12.5
            + current["max_drawdown60"].rank(pct=True).fillna(0.0) * 12.5
        )
        ranked_groups.append(current)
    combined = pd.concat(ranked_groups, ignore_index=True)
    combined = combined.sort_values(
        ["technical_score", "amount", "code"],
        ascending=[False, False, True],
        kind="stable",
    )
    return [_json_safe(record) for record in combined.to_dict(orient="records")]


def compute_net_rr(
    *,
    entry: float,
    stop_loss: float,
    target: float,
    round_trip_cost_bps: float,
) -> Optional[float]:
    """Calculate reward/risk after an approximate full round-trip cost."""

    if not (0 < stop_loss < entry < target):
        return None
    cost = entry * max(round_trip_cost_bps, 0.0) / 10_000.0
    net_reward = target - entry - cost
    net_risk = entry - stop_loss + cost
    if net_reward <= 0 or net_risk <= 0:
        return None
    return net_reward / net_risk


def validate_trade_plan(
    plan: Mapping[str, Any],
    *,
    min_net_rr: float,
) -> TradePlanValidation:
    """Validate price ordering and the configured net risk/reward boundary."""

    fields = {
        name: _finite_float(plan.get(name))
        for name in ("entry_low", "entry_high", "stop_loss", "take_profit_1", "take_profit_2", "net_rr")
    }
    reasons: List[str] = []
    if any(value is None for value in fields.values()):
        reasons.append("missing_or_non_finite_price")
        return TradePlanValidation(False, tuple(reasons), fields["net_rr"])

    assert all(value is not None for value in fields.values())
    if not (
        fields["stop_loss"]
        < fields["entry_low"]
        <= fields["entry_high"]
        < fields["take_profit_1"]
        < fields["take_profit_2"]
    ):
        reasons.append("invalid_price_order")
    if fields["net_rr"] + 1e-9 < min_net_rr:
        reasons.append("net_rr_below_minimum")
    return TradePlanValidation(not reasons, tuple(reasons), fields["net_rr"])


def _build_trade_plan(
    candidate: Mapping[str, Any],
    *,
    config: MarketScanConfig,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    price = _finite_float(candidate.get("price"))
    atr = _finite_float(candidate.get("atr14"))
    support = _finite_float(candidate.get("support"))
    if not price or not atr or not support:
        return None, "plan_inputs_missing"

    entry_high = price - 0.20 * atr
    entry_low = max(support, price - 0.80 * atr)
    if entry_low > entry_high:
        entry_low = max(0.01, entry_high - 0.30 * atr)
    entry_mid = (entry_low + entry_high) / 2.0
    stop_loss = min(support - 0.35 * atr, entry_low - atr)
    if stop_loss <= 0 or stop_loss >= entry_low:
        return None, "invalid_stop_loss"
    risk = entry_mid - stop_loss
    if risk <= 0 or risk / entry_mid > config.max_stop_distance_pct:
        return None, "stop_distance_too_large"

    take_profit_1 = entry_mid + 2.30 * risk
    take_profit_2 = entry_mid + 3.20 * risk
    costs = (
        config.a_round_trip_cost_bps
        if candidate.get("market") == MARKET_A
        else config.hk_round_trip_cost_bps
    )
    net_rr = compute_net_rr(
        entry=entry_mid,
        stop_loss=stop_loss,
        target=take_profit_1,
        round_trip_cost_bps=costs,
    )
    if net_rr is None:
        return None, "net_rr_unavailable"

    plan = {
        "entry_low": round(entry_low, 4),
        "entry_high": round(entry_high, 4),
        "entry_mid": round(entry_mid, 4),
        "stop_loss": round(stop_loss, 4),
        "take_profit_1": round(take_profit_1, 4),
        "take_profit_2": round(take_profit_2, 4),
        "net_rr": round(net_rr, 4),
        "round_trip_cost_bps": costs,
        "condition": "仅当价格进入买入区且趋势、成交量未恶化时考虑",
        "invalidation": "跌破止损失效点则原计划失效",
        "take_profit_rule": "第一目标分批减仓，第二目标继续减仓或使用移动止损",
    }
    validation = validate_trade_plan(plan, min_net_rr=config.min_net_rr)
    if not validation.valid:
        return None, ",".join(validation.reasons)
    return plan, None


def _normalise_verdict(value: Any) -> str:
    text = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if text in {"pass", "buy", "approve", "conditional_buy", "通过", "买入", "可考虑"}:
        return "pass"
    if text in {"reject", "sell", "avoid", "否决", "拒绝", "回避", "卖出"}:
        return "reject"
    return "watch"


def _normalise_review_payload(raw: Any, candidates: Sequence[Mapping[str, Any]], market_by_code: Mapping[str, str]) -> Dict[str, Dict[str, Any]]:
    value = raw
    if isinstance(raw, Mapping):
        for key in ("reviews", "candidates", "results", "items"):
            if isinstance(raw.get(key), list):
                value = raw[key]
                break
    if not isinstance(value, list):
        value = []

    reviews: Dict[str, Dict[str, Any]] = {}
    for item in value:
        if not isinstance(item, Mapping):
            continue
        raw_code = item.get("code") or item.get("symbol") or item.get("stock_code")
        market = market_by_code.get(str(raw_code or "").strip(), "")
        code = _normalise_symbol(raw_code, market) if market else ""
        if not code:
            for candidate in candidates:
                candidate_code = str(candidate.get("code") or "")
                if str(raw_code or "").strip().upper() == candidate_code:
                    code = candidate_code
                    break
        if not code or code not in market_by_code:
            continue
        confidence = _finite_float(item.get("confidence"))
        reviews[code] = {
            "verdict": _normalise_verdict(item.get("verdict") or item.get("action")),
            "confidence": min(max(confidence if confidence is not None else 0.0, 0.0), 1.0),
            "hard_risk": bool(item.get("hard_risk") or item.get("hard_reject")),
            "thesis": str(item.get("thesis") or item.get("reason") or "").strip(),
            "risks": [str(text).strip() for text in (item.get("risks") or []) if str(text).strip()],
            "invalidators": [
                str(text).strip()
                for text in (item.get("invalidators") or item.get("invalidation") or [])
                if str(text).strip()
            ],
            "facts": [str(text).strip() for text in (item.get("facts") or []) if str(text).strip()],
            "inferences": [
                str(text).strip() for text in (item.get("inferences") or []) if str(text).strip()
            ],
            "view": str(item.get("view") or "").strip(),
        }
    return reviews


def _missing_review(reason: str) -> Dict[str, Any]:
    return {
        "verdict": "watch",
        "confidence": 0.0,
        "hard_risk": False,
        "thesis": "",
        "risks": [reason],
        "invalidators": [],
        "facts": [],
        "inferences": [],
        "view": "",
    }


def _build_v4_evidence_contract(
    candidate: Mapping[str, Any],
    *,
    as_of: str,
    snapshot_fetched_at: str = "",
    snapshot_source: str = "",
) -> Dict[str, Any]:
    """Separate verified facts, deterministic inferences, and cautious views."""

    valuation_available = any(
        _finite_float(candidate.get(field)) is not None for field in ("pe", "pb")
    )
    quote_available = all(
        _finite_float(candidate.get(field)) is not None
        for field in ("price", "amount", "change_pct")
    )
    ohlcv_available = int(candidate.get("history_bars") or 0) >= 60
    availability = {
        "basic_quote": "available" if quote_available else "partial",
        "ohlcv": "available" if ohlcv_available else "partial",
        "valuation": "partial" if valuation_available else "unavailable",
        "fundamentals": "unavailable",
        "industry": "unavailable",
        "announcements": "unavailable",
        "capital_flow": "unavailable",
        "policy": "unavailable",
        "order_book_l1": "unavailable",
        "level2": "unavailable",
    }
    data_quality = (
        (0.30 if quote_available else 0.10)
        + (0.40 if ohlcv_available else 0.10)
        + (0.10 if valuation_available else 0.0)
    )
    if candidate.get("market") == MARKET_A:
        market_costs = {
            "entry_fee_bps": 3.0,
            "exit_fee_bps": 8.0,
            "entry_slippage_bps": 7.0,
            "exit_slippage_bps": 7.0,
        }
        policy_market = "cn"
    else:
        market_costs = {
            "entry_fee_bps": 10.0,
            "exit_fee_bps": 10.0,
            "entry_slippage_bps": 15.0,
            "exit_slippage_bps": 15.0,
        }
        policy_market = "hk"
    facts = {
        "as_of": as_of,
        "snapshot_fetched_at": snapshot_fetched_at,
        "snapshot_source": snapshot_source,
        "price": candidate.get("price"),
        "change_pct": candidate.get("change_pct"),
        "amount": candidate.get("amount"),
        "turnover_rate": candidate.get("turnover_rate"),
        "volume_ratio": candidate.get("volume_ratio"),
        "pe": candidate.get("pe"),
        "pb": candidate.get("pb"),
        "ma20": candidate.get("ma20"),
        "ma60": candidate.get("ma60"),
        "atr14": candidate.get("atr14"),
        "annualized_volatility20": candidate.get("annualized_volatility20"),
        "max_drawdown60": candidate.get("max_drawdown60"),
        "history_source": candidate.get("history_source"),
    }
    above_ma20 = bool(
        _finite_float(candidate.get("price"))
        and _finite_float(candidate.get("ma20"))
        and float(candidate["price"]) > float(candidate["ma20"])
    )
    inferences = {
        "trend": (
            "价格位于MA20上方，技术趋势暂偏强"
            if above_ma20
            else "未确认价格位于MA20上方"
        ),
        "support_hypothesis": "支撑与压力来自OHLCV规则计算，需由后续行情验证",
        "capital_behavior": (
            "缺少可靠Level-2和逐笔资金数据，不能把抢筹、洗盘或诱多表述为事实"
        ),
    }
    view = {
        "short_term": "技术候选，等待深度研究与触发条件确认",
        "swing": "仅保留条件价格计划，不构成买入建议",
        "medium_term": "基本面、公告、行业与政策数据不足",
        "long_term": "基本面与估值覆盖不足，无法形成长期观点",
    }
    return {
        "data_availability": availability,
        "data_quality": round(min(data_quality, 1.0), 4),
        "facts": _json_safe(facts),
        "inferences": inferences,
        "view": view,
        "scenario_probabilities": None,
        "scenario_probability_reason": "数据维度不足，不输出伪精确的涨跌概率",
        "t_trade": {
            "eligible": False,
            "reason": "缺少可靠分时、盘口和Level-2数据，无法验证扣费后胜率与期望收益",
        },
        "simulated_portfolio_weight": 0.0,
        "simulation_advice": "等待",
        "scope": "simulation",
        "symbol": candidate.get("code"),
        "policy_market": policy_market,
        "position_state": "flat",
        "expected_holding_days": 10,
        "market_costs": market_costs,
        "human_confirmation_required": True,
        "eligible_for_intraday_review": False,
        "deep_research_missing": [
            "fundamentals",
            "industry",
            "announcements",
            "capital_flow",
            "policy",
            "order_book_l1",
            "level2",
        ],
    }


class MarketScanService:
    """Run a layered market scan with independent model review."""

    def __init__(
        self,
        *,
        a_snapshot_loader: SnapshotLoader,
        hk_connect_snapshot_loader: SnapshotLoader,
        history_loader: HistoryLoader,
        qwen_reviewer: Optional[Reviewer],
        deepseek_reviewer: Optional[Reviewer],
        hk_all_snapshot_loader: Optional[SnapshotLoader] = None,
        config: Optional[MarketScanConfig] = None,
        clock: Clock = _now_shanghai,
        sleeper: Sleeper = time.sleep,
    ):
        self.a_snapshot_loader = a_snapshot_loader
        self.hk_connect_snapshot_loader = hk_connect_snapshot_loader
        self.hk_all_snapshot_loader = hk_all_snapshot_loader
        self.history_loader = history_loader
        self.qwen_reviewer = qwen_reviewer
        self.deepseek_reviewer = deepseek_reviewer
        self.config = config or MarketScanConfig()
        self.clock = clock
        self.sleeper = sleeper

    def _wait_before_snapshot_retry(self, attempt: int) -> None:
        """Apply bounded exponential backoff between provider attempts."""

        delay = self.config.snapshot_retry_backoff_seconds * (2 ** max(attempt, 0))
        if delay > 0:
            self.sleeper(delay)

    def run_l1(self) -> L1Result:
        """Run the full-market, vectorised stage.  This method never calls an LLM."""

        now = self.clock()
        if now.tzinfo is None:
            now = now.replace(tzinfo=SHANGHAI_TZ)

        a_enabled = MARKET_A in self.config.enabled_markets
        hk_enabled = MARKET_HK in self.config.enabled_markets
        if a_enabled:
            a_frame, a_metadata, a_safe_halt, a_warning = self._load_a_snapshot(now)
        else:
            a_frame = pd.DataFrame(columns=("code", "name", "market"))
            a_metadata = {"source": "market_closed", "session_status": "closed"}
            a_safe_halt = False
            a_warning = ""
        a_candidates: List[Dict[str, Any]] = []
        a_diagnostics: Dict[str, Any] = {
            "input_count": 0,
            "eligible_count": 0,
            "filtered": {},
        }
        if not a_safe_halt:
            a_candidates, a_diagnostics = _filter_and_rank_l1(
                a_frame,
                market=MARKET_A,
                config=self.config,
                limit=self.config.top_a_history,
            )

        if hk_enabled:
            hk_frame, hk_metadata, hk_safe_halt, hk_warning = (
                self._load_hk_connect_snapshot(now)
            )
        else:
            hk_frame = pd.DataFrame(columns=("code", "name", "market"))
            hk_metadata = {"source": "market_closed", "session_status": "closed"}
            hk_safe_halt = False
            hk_warning = ""
        hk_candidates: List[Dict[str, Any]] = []
        hk_diagnostics: Dict[str, Any] = {
            "input_count": 0,
            "eligible_count": 0,
            "filtered": {},
        }
        if not hk_safe_halt:
            hk_candidates, hk_diagnostics = _filter_and_rank_l1(
                hk_frame,
                market=MARKET_HK,
                config=self.config,
                limit=self.config.top_hk_history,
            )

        block_reasons = [reason for reason in (a_warning, hk_warning) if reason]
        return L1Result(
            a_candidates=a_candidates,
            hk_candidates=hk_candidates,
            as_of={
                MARKET_A: str(a_metadata.get("as_of") or ""),
                MARKET_HK: str(hk_metadata.get("as_of") or ""),
            },
            diagnostics={
                MARKET_A: a_diagnostics,
                MARKET_HK: hk_diagnostics,
                "l1_llm_calls": 0,
                "a_snapshot_source": a_metadata.get("source") or "",
                "hk_snapshot_source": hk_metadata.get("source") or "",
                "a_snapshot_fetched_at": a_metadata.get("fetched_at") or "",
                "hk_snapshot_fetched_at": hk_metadata.get("fetched_at") or "",
                "a_snapshot_provider_errors": a_metadata.get("provider_errors") or [],
                "hk_snapshot_provider_errors": hk_metadata.get("provider_errors") or [],
                "hk_membership_age_hours": hk_metadata.get("membership_age_hours"),
                "hk_membership_source": hk_metadata.get("membership_source") or "",
                "hk_membership_remaining_hours": hk_metadata.get(
                    "membership_remaining_hours"
                ),
                "hk_membership_warning_level": hk_metadata.get(
                    "membership_warning_level"
                )
                or "none",
                "hk_membership_warning_threshold_hours": hk_metadata.get(
                    "membership_warning_threshold_hours"
                ),
                "a_full_market_strict": not a_safe_halt,
                "hk_connect_strict": not hk_safe_halt,
                "enabled_markets": list(self.config.enabled_markets),
                "market_session_status": {
                    MARKET_A: "active" if a_enabled else "closed",
                    MARKET_HK: "active" if hk_enabled else "closed",
                },
            },
            a_safe_halt=a_safe_halt,
            hk_safe_halt=hk_safe_halt,
            push_block_reasons=block_reasons,
        )

    def _load_a_snapshot(
        self,
        now: datetime,
    ) -> Tuple[pd.DataFrame, Dict[str, Any], bool, str]:
        provider_errors: List[str] = []
        for attempt in range(self.config.snapshot_retries):
            if attempt:
                self._wait_before_snapshot_retry(attempt - 1)
            try:
                raw = self.a_snapshot_loader()
                frame_raw, metadata = _coerce_snapshot_payload(raw, now=now)
                if not bool(metadata.get("is_full_a_universe")):
                    raise MarketScanError("A-share provider did not prove full-market coverage")
                frame = _normalise_snapshot(frame_raw, market=MARKET_A, now=now)
                if frame.empty:
                    raise MarketScanError("A-share full-market provider returned no symbols")
                cache_payload = {
                    "schema_version": SCHEMA_VERSION,
                    "saved_at": _iso_datetime(now),
                    "as_of": metadata["as_of"],
                    "fetched_at": metadata["fetched_at"],
                    "source": str(metadata.get("source") or "a_share_provider"),
                    "provider_errors": [
                        *provider_errors,
                        *(str(item) for item in (metadata.get("provider_errors") or [])),
                    ],
                    "is_full_a_universe": True,
                    "records": frame.to_dict(orient="records"),
                }
                _atomic_write_json(self.config.a_cache_path, cache_payload)
                metadata["source"] = cache_payload["source"]
                metadata["provider_errors"] = cache_payload["provider_errors"]
                return frame.reset_index(drop=True), metadata, False, ""
            except Exception as exc:  # noqa: BLE001 - bounded retries precede cache fallback.
                provider_errors.append(f"attempt_{attempt + 1}:{type(exc).__name__}:{exc}")

        cached = self._read_snapshot_cache(
            now,
            path=self.config.a_cache_path,
            market=MARKET_A,
            trust_field="is_full_a_universe",
            max_age_hours=self.config.a_cache_max_age_hours,
        )
        if cached is not None:
            frame, metadata = cached
            metadata["source"] = f"{metadata.get('source') or 'unknown'}:last_good_cache"
            metadata["provider_error"] = provider_errors[-1] if provider_errors else ""
            metadata["provider_errors"] = provider_errors
            return frame, metadata, False, ""

        reason = "A股全市场快照不可用或缓存过期，已安全停止本轮主动推送"
        return (
            pd.DataFrame(columns=("code", "name", "market")),
            {
                "as_of": "",
                "fetched_at": "",
                "source": "unavailable",
                "provider_error": provider_errors[-1] if provider_errors else "",
                "provider_errors": provider_errors,
            },
            True,
            reason,
        )

    def _load_hk_connect_snapshot(
        self,
        now: datetime,
    ) -> Tuple[pd.DataFrame, Dict[str, Any], bool, str]:
        provider_errors: List[str] = []
        for attempt in range(self.config.snapshot_retries):
            if attempt:
                self._wait_before_snapshot_retry(attempt - 1)
            try:
                raw = self.hk_connect_snapshot_loader()
                frame_raw, metadata = _coerce_snapshot_payload(raw, now=now)
                if not bool(metadata.get("is_connect_universe")):
                    raise MarketScanError("HK provider did not prove Stock Connect membership")
                frame = _normalise_snapshot(frame_raw, market=MARKET_HK, now=now)
                if "is_connect" in frame.columns:
                    frame = frame[frame["is_connect"] == True].copy()  # noqa: E712 - pandas mask
                if frame.empty:
                    raise MarketScanError("HK Stock Connect provider returned no eligible constituents")
                cache_payload = {
                    "schema_version": SCHEMA_VERSION,
                    "saved_at": _iso_datetime(now),
                    "as_of": metadata["as_of"],
                    "fetched_at": metadata["fetched_at"],
                    "membership_fetched_at": _iso_datetime(
                        metadata.get("membership_fetched_at"), fallback=now
                    ),
                    "source": str(metadata.get("source") or "hk_connect_provider"),
                    "membership_source": str(
                        metadata.get("source") or "hk_connect_provider"
                    ),
                    "provider_errors": list(metadata.get("provider_errors") or []),
                    "is_connect_universe": True,
                    "membership_codes": sorted(set(frame["code"].astype(str))),
                    "records": frame.to_dict(orient="records"),
                }
                _atomic_write_json(self.config.hk_cache_path, cache_payload)
                self._write_hk_membership_cache(cache_payload)
                metadata["source"] = cache_payload["source"]
                metadata["provider_errors"] = cache_payload["provider_errors"]
                metadata["membership_age_hours"] = 0.0
                metadata["membership_source"] = cache_payload["membership_source"]
                metadata.update(
                    _membership_cache_warning(
                        0.0, self.config.hk_membership_cache_max_age_hours
                    )
                )
                return frame.reset_index(drop=True), metadata, False, ""
            except Exception as exc:  # noqa: BLE001 - bounded retries precede cache fallback.
                provider_errors.append(f"connect:{type(exc).__name__}:{exc}")

        membership = self._read_hk_membership_cache(now)
        if membership is not None and self.hk_all_snapshot_loader is not None:
            membership_codes, membership_metadata = membership
            for attempt in range(self.config.snapshot_retries):
                if attempt:
                    self._wait_before_snapshot_retry(attempt - 1)
                try:
                    raw = self.hk_all_snapshot_loader()
                    frame_raw, metadata = _coerce_snapshot_payload(raw, now=now)
                    if not bool(metadata.get("is_full_hk_universe")):
                        raise MarketScanError("HK quote fallback did not prove full-market coverage")
                    frame = _normalise_snapshot(frame_raw, market=MARKET_HK, now=now)
                    frame = frame[frame["code"].isin(membership_codes)].copy()
                    if frame.empty:
                        raise MarketScanError(
                            "HK quote fallback had no overlap with cached Connect membership"
                        )
                    frame["is_connect"] = True
                    provider_errors.extend(
                        str(item) for item in (metadata.get("provider_errors") or [])
                    )
                    quote_source = str(
                        metadata.get("source") or "hk_full_market_provider"
                    )
                    cache_payload = {
                        "schema_version": SCHEMA_VERSION,
                        "saved_at": _iso_datetime(now),
                        "as_of": metadata["as_of"],
                        "fetched_at": metadata["fetched_at"],
                        "membership_fetched_at": membership_metadata[
                            "membership_fetched_at"
                        ],
                        "source": quote_source,
                        "membership_source": membership_metadata[
                            "membership_source"
                        ],
                        "provider_errors": provider_errors,
                        "is_connect_universe": True,
                        "membership_codes": sorted(membership_codes),
                        "records": frame.to_dict(orient="records"),
                    }
                    _atomic_write_json(self.config.hk_cache_path, cache_payload)
                    metadata.update(
                        {
                            "source": quote_source,
                            "provider_errors": provider_errors,
                            "membership_age_hours": membership_metadata[
                                "membership_age_hours"
                            ],
                            "membership_source": membership_metadata[
                                "membership_source"
                            ],
                            "membership_remaining_hours": membership_metadata[
                                "membership_remaining_hours"
                            ],
                            "membership_warning_level": membership_metadata[
                                "membership_warning_level"
                            ],
                            "membership_warning_threshold_hours": (
                                membership_metadata[
                                    "membership_warning_threshold_hours"
                                ]
                            ),
                        }
                    )
                    return frame.reset_index(drop=True), metadata, False, ""
                except Exception as exc:  # noqa: BLE001 - bounded provider fallback.
                    provider_errors.append(f"all_hk:{type(exc).__name__}:{exc}")

        cached = self._read_snapshot_cache(
            now,
            path=self.config.hk_cache_path,
            market=MARKET_HK,
            trust_field="is_connect_universe",
            max_age_hours=self.config.hk_cache_max_age_hours,
            membership_max_age_hours=self.config.hk_membership_cache_max_age_hours,
        )
        if cached is not None:
            frame, metadata = cached
            metadata["source"] = f"{metadata.get('source') or 'unknown'}:last_good_cache"
            metadata["provider_error"] = provider_errors[-1] if provider_errors else ""
            metadata["provider_errors"] = provider_errors
            return frame, metadata, False, ""

        reason = "港股通成分数据不可用或缓存过期，已安全停止本轮主动推送"
        return (
            pd.DataFrame(columns=("code", "name", "market")),
            {
                "as_of": "",
                "fetched_at": "",
                "source": "unavailable",
                "provider_error": provider_errors[-1] if provider_errors else "",
                "provider_errors": provider_errors,
            },
            True,
            reason,
        )

    def _write_hk_membership_cache(self, snapshot: Mapping[str, Any]) -> None:
        """Persist constituent membership separately from volatile quote data."""

        membership_codes = sorted(
            {
                code
                for code in (
                    _normalise_symbol(item, MARKET_HK)
                    for item in (snapshot.get("membership_codes") or [])
                )
                if code
            }
        )
        if not membership_codes:
            raise MarketScanError("HK Connect membership cache would be empty")
        payload = {
            "schema_version": SCHEMA_VERSION,
            "saved_at": str(snapshot.get("membership_fetched_at") or ""),
            "membership_fetched_at": str(
                snapshot.get("membership_fetched_at") or ""
            ),
            "membership_source": str(
                snapshot.get("membership_source") or snapshot.get("source") or ""
            ),
            "is_connect_universe": True,
            "membership_codes": membership_codes,
        }
        _atomic_write_json(self.config.hk_membership_cache_path, payload)

    def _read_hk_membership_cache(
        self, now: datetime
    ) -> Optional[Tuple[set[str], Dict[str, Any]]]:
        """Read membership independently from quote freshness.

        A fresh all-HK quote can safely refresh prices only when it is filtered
        through a bounded, previously proven Stock Connect constituent set.
        Refreshing quotes never extends the constituent-set lifetime.
        """

        payload: Optional[Mapping[str, Any]] = None
        for path in (
            self.config.hk_membership_cache_path,
            self.config.hk_cache_path,
        ):
            try:
                candidate = json.loads(path.read_text(encoding="utf-8"))
            except (FileNotFoundError, OSError, json.JSONDecodeError):
                continue
            if isinstance(candidate, Mapping) and candidate.get(
                "is_connect_universe"
            ):
                payload = candidate
                break
        if payload is None:
            return None
        if not isinstance(payload, Mapping) or not payload.get("is_connect_universe"):
            return None
        membership_fetched_at = _parse_datetime(
            payload.get("membership_fetched_at") or payload.get("saved_at")
        )
        if membership_fetched_at is None:
            return None
        age_hours = (
            now.astimezone(timezone.utc) - membership_fetched_at
        ).total_seconds() / 3600.0
        if (
            age_hours < 0
            or age_hours > self.config.hk_membership_cache_max_age_hours
        ):
            return None
        raw_membership_codes = payload.get("membership_codes")
        if isinstance(raw_membership_codes, list):
            codes = {
                code
                for code in (
                    _normalise_symbol(item, MARKET_HK)
                    for item in raw_membership_codes
                )
                if code
            }
        else:
            frame = _normalise_snapshot(
                pd.DataFrame(payload.get("records") or []),
                market=MARKET_HK,
                now=now,
            )
            codes = set(frame["code"].astype(str)) if not frame.empty else set()
        if not codes:
            return None
        return codes, {
            "membership_fetched_at": _iso_datetime(membership_fetched_at),
            "membership_age_hours": round(age_hours, 4),
            "membership_source": str(
                payload.get("membership_source") or payload.get("source") or ""
            ),
            **_membership_cache_warning(
                age_hours, self.config.hk_membership_cache_max_age_hours
            ),
        }

    @staticmethod
    def _read_snapshot_cache(
        now: datetime,
        *,
        path: Path,
        market: str,
        trust_field: str,
        max_age_hours: float,
        membership_max_age_hours: Optional[float] = None,
    ) -> Optional[Tuple[pd.DataFrame, Dict[str, Any]]]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            return None
        if not isinstance(payload, Mapping) or not payload.get(trust_field):
            return None
        saved_at = _parse_datetime(payload.get("saved_at"))
        now_utc = now.astimezone(timezone.utc)
        if saved_at is None:
            return None
        age_hours = (now_utc - saved_at).total_seconds() / 3600.0
        if age_hours < 0 or age_hours > max_age_hours:
            return None
        membership_fetched_at = _parse_datetime(
            payload.get("membership_fetched_at") or payload.get("saved_at")
        )
        if membership_max_age_hours is not None:
            if membership_fetched_at is None:
                return None
            membership_age_hours = (
                now_utc - membership_fetched_at
            ).total_seconds() / 3600.0
            if (
                membership_age_hours < 0
                or membership_age_hours > membership_max_age_hours
            ):
                return None
        frame = _normalise_snapshot(
            pd.DataFrame(payload.get("records") or []),
            market=market,
            now=now,
        )
        if frame.empty:
            return None
        source = str(payload.get("source") or "")
        if market == MARKET_HK:
            source = source.replace(":cached_connect_membership", "")
        metadata = {
            "as_of": str(payload.get("as_of") or ""),
            "fetched_at": str(payload.get("fetched_at") or payload.get("saved_at") or ""),
            "source": source,
            "cache_age_hours": age_hours,
            trust_field: True,
        }
        if membership_fetched_at is not None:
            membership_age_hours = (
                now_utc - membership_fetched_at
            ).total_seconds() / 3600.0
            metadata["membership_age_hours"] = round(membership_age_hours, 4)
            metadata["membership_source"] = str(
                payload.get("membership_source") or ""
            )
            if membership_max_age_hours is not None:
                metadata.update(
                    _membership_cache_warning(
                        membership_age_hours, membership_max_age_hours
                    )
                )
        return frame, metadata

    def run(self) -> Dict[str, Any]:
        """Run all stages and return a JSON-serialisable result."""

        generated_at = _iso_datetime(self.clock())
        l1 = self.run_l1()
        history_candidates: List[Dict[str, Any]] = []
        history_rejections: Dict[str, str] = {}
        for candidate in [*l1.a_candidates, *l1.hk_candidates]:
            code = str(candidate.get("code") or "")
            try:
                raw_history = self.history_loader(code, self.config.history_lookback_days)
                history, source = _normalise_history(raw_history)
                enriched, rejection = _history_features(
                    candidate,
                    history,
                    source=source,
                    config=self.config,
                )
            except Exception as exc:  # noqa: BLE001 - one symbol must not abort the scan.
                enriched, rejection = None, f"history_error:{type(exc).__name__}"
            if enriched is None:
                history_rejections[code] = rejection or "history_unavailable"
                continue
            plan, plan_rejection = _build_trade_plan(enriched, config=self.config)
            if plan is None:
                history_rejections[code] = plan_rejection or "trade_plan_invalid"
                continue
            enriched["plan"] = plan
            history_candidates.append(enriched)

        ranked = _rank_l2(history_candidates)[: self.config.final_top_n]
        review_payload = []
        for candidate in ranked:
            payload = self._candidate_for_review(candidate)
            is_a_share = candidate.get("market") == MARKET_A
            payload.update(
                _build_v4_evidence_contract(
                    candidate,
                    as_of=str(l1.as_of.get(str(candidate.get("market"))) or ""),
                    snapshot_fetched_at=str(
                        l1.diagnostics.get(
                            "a_snapshot_fetched_at"
                            if is_a_share
                            else "hk_snapshot_fetched_at"
                        )
                        or ""
                    ),
                    snapshot_source=str(
                        l1.diagnostics.get(
                            "a_snapshot_source" if is_a_share else "hk_snapshot_source"
                        )
                        or ""
                    ),
                )
            )
            payload["review_request"] = {
                "proposed_action": "advance_to_intraday_entry_zone_review",
                "deterministic_initial_position_fraction_range": [0.025, 0.10],
                "sizing_policy": (
                    "规则门在双模型复核后按普通/强/极强机会分级，"
                    "模型不得自行决定仓位"
                ),
                "immediate_buy": False,
                "auto_order": False,
                "human_confirmation_required": True,
            }
            review_payload.append(payload)
        qwen_reviews, qwen_error = self._call_reviewer(
            "qwen",
            self.qwen_reviewer,
            review_payload,
            ranked,
        )
        deepseek_reviews, deepseek_error = self._call_reviewer(
            "deepseek",
            self.deepseek_reviewer,
            review_payload,
            ranked,
        )
        review_complete = not qwen_error and not deepseek_error

        candidates: List[Dict[str, Any]] = []
        candidate_rejection_reasons: Dict[str, int] = {}
        candidate_rejection_reasons_by_market: Dict[str, Dict[str, int]] = {
            MARKET_A: {},
            MARKET_HK: {},
        }
        dual_pass_count = 0
        actionable_count = 0
        actionable_by_market = {MARKET_A: 0, MARKET_HK: 0}
        market_snapshot_complete = {
            MARKET_A: not l1.a_safe_halt,
            MARKET_HK: not l1.hk_safe_halt,
        }

        def count_candidate_rejection(reason: str, market: str) -> None:
            candidate_rejection_reasons[reason] = (
                candidate_rejection_reasons.get(reason, 0) + 1
            )
            market_reasons = candidate_rejection_reasons_by_market.setdefault(
                market, {}
            )
            market_reasons[reason] = market_reasons.get(reason, 0) + 1

        for rank, candidate in enumerate(ranked, start=1):
            code = str(candidate["code"])
            candidate_market = str(candidate.get("market") or "")
            candidate_snapshot_complete = bool(
                market_snapshot_complete.get(candidate_market, False)
            )
            qwen = qwen_reviews.get(code) or _missing_review(qwen_error or "qwen_review_missing")
            deepseek = deepseek_reviews.get(code) or _missing_review(
                deepseek_error or "deepseek_review_missing"
            )
            disagreement = qwen["verdict"] != deepseek["verdict"]
            hard_risk = bool(qwen["hard_risk"] or deepseek["hard_risk"])
            both_pass = qwen["verdict"] == deepseek["verdict"] == "pass"
            if both_pass:
                dual_pass_count += 1
            final_score = (
                float(candidate.get("technical_score") or 0.0) * 0.70
                + float(qwen["confidence"]) * 15.0
                + float(deepseek["confidence"]) * 15.0
            )
            evidence_contract = _build_v4_evidence_contract(
                candidate,
                as_of=str(l1.as_of.get(str(candidate.get("market"))) or ""),
                snapshot_fetched_at=str(
                    l1.diagnostics.get(
                        "a_snapshot_fetched_at"
                        if candidate.get("market") == MARKET_A
                        else "hk_snapshot_fetched_at"
                    )
                    or ""
                ),
                snapshot_source=str(
                    l1.diagnostics.get(
                        "a_snapshot_source"
                        if candidate.get("market") == MARKET_A
                        else "hk_snapshot_source"
                    )
                    or ""
                ),
            )
            candidate_output = dict(candidate)
            review_confidence = min(
                (float(qwen["confidence"]) + float(deepseek["confidence"])) / 2.0,
                float(evidence_contract["data_quality"]),
            )
            plan = candidate.get("plan") or {}
            actionable = bool(
                review_complete
                and both_pass
                and not disagreement
                and not hard_risk
                and candidate_snapshot_complete
                and float(evidence_contract["data_quality"])
                >= self.config.min_actionable_data_quality
                and plan
            )
            action = "conditional_buy" if actionable else "watch"
            if actionable:
                actionable_count += 1
                actionable_by_market[candidate_market] = (
                    actionable_by_market.get(candidate_market, 0) + 1
                )
                opportunity = classify_opportunity(
                    rank=rank,
                    data_quality=evidence_contract["data_quality"],
                    net_rr=plan.get("net_rr"),
                    qwen_confidence=qwen.get("confidence"),
                    deepseek_confidence=deepseek.get("confidence"),
                )
                evidence_contract.update(
                    {
                        "simulated_portfolio_weight": (
                            opportunity.initial_position_fraction
                        ),
                        "simulation_advice": (
                            "进入买入区后建议首笔建仓"
                            f"{opportunity.initial_position_fraction * 100:g}%"
                        ),
                        "eligible_for_intraday_review": True,
                        "opportunity_tier": opportunity.tier,
                        "cash_floor_ratio": opportunity.cash_floor_ratio,
                        "max_single_position_ratio": (
                            opportunity.max_single_position_ratio
                        ),
                        "initial_position_fraction": (
                            opportunity.initial_position_fraction
                        ),
                        "add_position_fraction": (
                            opportunity.add_position_fraction
                        ),
                        "opportunity_tier_evidence": {
                            "rank": rank,
                            "data_quality": evidence_contract["data_quality"],
                            "net_rr": plan.get("net_rr"),
                            "qwen_confidence": qwen.get("confidence"),
                            "deepseek_confidence": deepseek.get("confidence"),
                        },
                    }
                )
                evidence_contract["view"] = {
                    **dict(evidence_contract.get("view") or {}),
                    "short_term": "双模型与规则门已通过，仅在新鲜价格进入计划区时首笔建仓",
                    "swing": (
                        f"首笔按{opportunity.initial_position_fraction * 100:g}%"
                        "模拟净值分级试错，后续加仓仍需新一轮确认"
                    ),
                }
            elif not candidate_snapshot_complete:
                count_candidate_rejection(
                    "a_share_snapshot_incomplete"
                    if candidate_market == MARKET_A
                    else "hk_connect_snapshot_incomplete",
                    candidate_market,
                )
            elif not review_complete:
                count_candidate_rejection(
                    "dual_model_review_incomplete", candidate_market
                )
            elif hard_risk:
                count_candidate_rejection("hard_risk_veto", candidate_market)
            elif disagreement:
                count_candidate_rejection("model_disagreement", candidate_market)
            elif not both_pass:
                count_candidate_rejection(
                    "dual_model_not_both_pass", candidate_market
                )
            else:
                count_candidate_rejection(
                    "data_quality_below_actionable_threshold", candidate_market
                )
            candidate_output.update(
                {
                    "rank": rank,
                    "qwen_review": qwen,
                    "deepseek_review": deepseek,
                    "model_disagreement": disagreement,
                    "hard_risk_veto": hard_risk,
                    "review_complete": review_complete,
                    "action": action,
                    "final_score": round(final_score, 4),
                    "confidence": round(review_confidence, 4),
                    "research_status": (
                        "actionable"
                        if actionable
                        else (
                            "data_quality_not_passed"
                            if (
                                candidate_snapshot_complete
                                and review_complete
                                and both_pass
                                and not hard_risk
                            )
                            else (
                                "snapshot_incomplete"
                                if not candidate_snapshot_complete
                                else "review_not_passed"
                            )
                        )
                    ),
                    "entry_low": plan.get("entry_low"),
                    "entry_high": plan.get("entry_high"),
                    "stop_loss": plan.get("stop_loss"),
                    "take_profit_1": plan.get("take_profit_1"),
                    "take_profit_2": plan.get("take_profit_2"),
                    "action_reason": self._action_reason(
                        review_complete=review_complete,
                        disagreement=disagreement,
                        hard_risk=hard_risk,
                        both_pass=both_pass,
                        actionable=actionable,
                        market=candidate_market,
                        market_snapshot_complete=candidate_snapshot_complete,
                        initial_position_fraction=(
                            evidence_contract.get("initial_position_fraction")
                        ),
                    ),
                    **evidence_contract,
                }
            )
            candidates.append(_json_safe(candidate_output))

        enabled_markets = set(self.config.enabled_markets)
        market_block_reasons = {
            MARKET_A: (
                ["A股全市场快照不可用或缓存过期，已阻止A股主动推荐"]
                if l1.a_safe_halt
                else []
            ),
            MARKET_HK: (
                ["港股通成分或报价快照不可用，已阻止港股通主动推荐"]
                if l1.hk_safe_halt
                else []
            ),
        }
        push_block_reasons: List[str] = []
        active_snapshot_halts = {
            MARKET_A: l1.a_safe_halt,
            MARKET_HK: l1.hk_safe_halt,
        }
        if enabled_markets and all(
            active_snapshot_halts[market] for market in enabled_markets
        ):
            push_block_reasons.extend(l1.push_block_reasons)
        if not review_complete:
            push_block_reasons.append("通义或 DeepSeek 独立复核未完成，已停止主动推荐推送")
        if not candidates:
            push_block_reasons.append("没有通过数据、趋势和风险回报护栏的候选")

        operational_failures: List[str] = []
        operational_warnings: List[str] = []
        if l1.a_safe_halt:
            operational_failures.append("a_share_full_market_snapshot_unavailable")
        if l1.hk_safe_halt:
            operational_failures.append("hk_connect_snapshot_unavailable")
        if ranked and not review_complete:
            operational_failures.append("dual_model_review_incomplete")
        membership_warning_level = str(
            l1.diagnostics.get("hk_membership_warning_level") or "none"
        )
        membership_warning_threshold = l1.diagnostics.get(
            "hk_membership_warning_threshold_hours"
        )
        if (
            MARKET_HK in enabled_markets
            and membership_warning_level != "none"
            and membership_warning_threshold is not None
        ):
            operational_warnings.append(
                "hk_connect_membership_cache_expires_within_"
                f"{int(float(membership_warning_threshold))}h"
            )
        all_active_markets_blocked = bool(enabled_markets) and all(
            active_snapshot_halts[market] for market in enabled_markets
        )
        any_active_market_blocked = any(
            active_snapshot_halts[market] for market in enabled_markets
        )
        if all_active_markets_blocked:
            operational_status = "failed"
        elif ranked and not review_complete:
            operational_status = "failed"
        elif any_active_market_blocked:
            operational_status = "degraded"
        else:
            operational_status = "healthy"

        history_rejection_counts: Dict[str, int] = {}
        for reason in history_rejections.values():
            history_rejection_counts[reason] = history_rejection_counts.get(reason, 0) + 1
        a_input_count = int(l1.diagnostics.get(MARKET_A, {}).get("input_count") or 0)
        hk_input_count = int(l1.diagnostics.get(MARKET_HK, {}).get("input_count") or 0)
        a_eligible_count = int(
            l1.diagnostics.get(MARKET_A, {}).get("eligible_count") or 0
        )
        hk_eligible_count = int(
            l1.diagnostics.get(MARKET_HK, {}).get("eligible_count") or 0
        )
        buy_funnel = {
            "snapshot_input_count": a_input_count + hk_input_count,
            "a_snapshot_input_count": a_input_count,
            "hk_snapshot_input_count": hk_input_count,
            "l1_eligible_count": a_eligible_count + hk_eligible_count,
            "l1_shortlist_count": len(l1.a_candidates) + len(l1.hk_candidates),
            "history_requested_count": len(l1.a_candidates) + len(l1.hk_candidates),
            "history_and_plan_valid_count": len(history_candidates),
            "dual_model_reviewed_count": len(ranked) if review_complete else 0,
            "dual_model_pass_count": dual_pass_count,
            "actionable_count": actionable_count,
            "history_rejection_reasons": history_rejection_counts,
            "candidate_rejection_reasons": candidate_rejection_reasons,
            "candidate_rejection_reasons_by_market": (
                candidate_rejection_reasons_by_market
            ),
            "actionable_by_market": actionable_by_market,
        }

        result = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": generated_at,
            "as_of": l1.as_of,
            "simulation_only": True,
            "auto_order_enabled": False,
            "human_confirmation_required": True,
            "v4_contract_version": "market-scan-v4-mvp",
            "data_policy": {
                "active_fetch": True,
                "description": (
                    "系统主动查询可得的全市场基础行情与候选OHLCV；"
                    "未取得的盘口、Level-2、资金、公告和基本面不得推断或编造。"
                ),
                "level2": {
                    "status": "unavailable",
                    "acquisition_policy": "authorized_or_licensed_sources_only",
                    "fallback": (
                        "使用新鲜基础行情、OHLCV、成交量、波动和估值做交叉验证；"
                        "不把普通行情或推算结果冒充Level-2。"
                    ),
                },
            },
            "safe_to_push": not push_block_reasons,
            "push_block_reasons": list(dict.fromkeys(push_block_reasons)),
            "market_block_reasons": market_block_reasons,
            "review_complete": review_complete,
            "review_errors": {
                key: value
                for key, value in {"qwen": qwen_error, "deepseek": deepseek_error}.items()
                if value
            },
            "operational_status": operational_status,
            "operational_failures": operational_failures,
            "operational_warnings": operational_warnings,
            "market_operational_status": {
                MARKET_A: (
                    "closed"
                    if MARKET_A not in enabled_markets
                    else "blocked"
                    if l1.a_safe_halt
                    else "healthy"
                ),
                MARKET_HK: (
                    "closed"
                    if MARKET_HK not in enabled_markets
                    else "blocked"
                    if l1.hk_safe_halt
                    else "healthy"
                ),
            },
            "diagnostics": {
                **l1.diagnostics,
                "history_requested_count": len(l1.a_candidates) + len(l1.hk_candidates),
                "history_accepted_count": len(history_candidates),
                "history_rejections": history_rejections,
                "llm_candidate_count": len(ranked),
                "llm_calls": {
                    "qwen": 1 if ranked and self.qwen_reviewer is not None else 0,
                    "deepseek": 1 if ranked and self.deepseek_reviewer is not None else 0,
                },
                "buy_funnel": buy_funnel,
            },
            "candidates": candidates,
            "disclaimer": (
                "仅用于模拟研究和条件价格计划，不保证收益，不连接券商，也不会自动下单。"
                "任何计划都需在执行前重新核对实时价格、公告、流动性和个人风险承受能力。"
                "当前MVP仍缺少完整基本面、公告、政策、资金和可靠Level-2数据；只有规则、"
                "扣费后风险回报与双模型同时通过的候选，才会按2.5%–10%模拟净值分级进入"
                "盘中价格区复核。"
                "最终是否执行仍由用户人工决定。"
            ),
        }
        return _json_safe(result)

    @staticmethod
    def _candidate_for_review(candidate: Mapping[str, Any]) -> Dict[str, Any]:
        fields = (
            "code",
            "name",
            "market",
            "price",
            "amount",
            "change_pct",
            "turnover_rate",
            "volume_ratio",
            "pe",
            "pb",
            "total_mv",
            "technical_score",
            "ma20",
            "ma60",
            "momentum20",
            "momentum60",
            "atr_pct",
            "annualized_volatility20",
            "max_drawdown60",
            "support",
            "resistance",
            "plan",
        )
        return {field: copy.deepcopy(candidate.get(field)) for field in fields}

    @staticmethod
    def _call_reviewer(
        label: str,
        reviewer: Optional[Reviewer],
        payload: Sequence[Mapping[str, Any]],
        candidates: Sequence[Mapping[str, Any]],
    ) -> Tuple[Dict[str, Dict[str, Any]], str]:
        if not payload:
            return {}, ""
        if reviewer is None:
            return {}, f"{label}_reviewer_not_configured"
        market_by_code = {
            str(candidate["code"]): str(candidate["market"])
            for candidate in candidates
        }
        try:
            raw = reviewer(copy.deepcopy(list(payload)))
            reviews = _normalise_review_payload(raw, candidates, market_by_code)
        except Exception as exc:  # noqa: BLE001 - model failure becomes a safe watch result.
            return {}, f"{label}_review_failed:{type(exc).__name__}"
        missing = [code for code in market_by_code if code not in reviews]
        if missing:
            return reviews, f"{label}_review_missing:{','.join(missing)}"
        return reviews, ""

    @staticmethod
    def _action_reason(
        *,
        review_complete: bool,
        disagreement: bool,
        hard_risk: bool,
        both_pass: bool,
        actionable: bool,
        market: str,
        market_snapshot_complete: bool,
        initial_position_fraction: Any = None,
    ) -> str:
        if not review_complete:
            return "双模型独立复核不完整，降级为观察"
        if hard_risk:
            return "至少一个模型识别到硬风险，否决主动推荐"
        if not market_snapshot_complete:
            market_label = "A股" if market == MARKET_A else "港股通"
            return f"{market_label}根快照不完整，仅该市场候选降级为观察"
        if disagreement:
            return "通义与 DeepSeek 意见冲突，降级为观察"
        if both_pass and actionable:
            try:
                fraction_text = f"{float(initial_position_fraction) * 100:g}%"
            except (TypeError, ValueError):
                fraction_text = "动态仓位"
            return (
                "技术规则、扣费后风险回报与双模型复核通过；"
                f"仅在新鲜价格进入计划区后首笔模拟建仓{fraction_text}，并需人工确认"
            )
        if both_pass:
            return "双模型通过，但数据质量未达到当前主动建仓阈值，保持观察"
        return "双模型未同时通过，保持观察"


def default_a_snapshot_loader() -> Mapping[str, Any]:
    """Load a full A-share snapshot through independent AkShare routes."""

    import akshare as ak

    provider_errors: List[str] = []
    for provider_name in ("stock_zh_a_spot_em", "stock_zh_a_spot"):
        try:
            frame = getattr(ak, provider_name)()
            if not isinstance(frame, pd.DataFrame) or frame.empty:
                raise MarketScanError("provider returned an empty full-market snapshot")
            return {
                "records": frame,
                "fetched_at": _iso_datetime(_now_shanghai()),
                "source": f"akshare.{provider_name}",
                "provider_errors": provider_errors,
                "is_full_a_universe": True,
            }
        except Exception as exc:  # noqa: BLE001 - explicit independent provider chain.
            provider_errors.append(f"{provider_name}:{type(exc).__name__}:{exc}")
    raise MarketScanError("; ".join(provider_errors) or "A-share providers unavailable")


def default_hk_connect_snapshot_loader() -> Mapping[str, Any]:
    """Load the strict Stock Connect universe and snapshot through AkShare."""

    import akshare as ak

    frame = ak.stock_hk_ggt_components_em()
    return {
        "records": frame,
        "fetched_at": _iso_datetime(_now_shanghai()),
        "source": "akshare.stock_hk_ggt_components_em",
        "is_connect_universe": True,
    }


def default_hk_all_snapshot_loader() -> Mapping[str, Any]:
    """Load all-HK quotes for filtering through cached Connect membership."""

    import akshare as ak

    provider_errors: List[str] = []
    for provider_name in ("stock_hk_spot_em", "stock_hk_spot"):
        try:
            frame = getattr(ak, provider_name)()
            if not isinstance(frame, pd.DataFrame) or frame.empty:
                raise MarketScanError("provider returned an empty full-market snapshot")
            return {
                "records": frame,
                "fetched_at": _iso_datetime(_now_shanghai()),
                "source": f"akshare.{provider_name}",
                "provider_errors": provider_errors,
                "is_full_hk_universe": True,
            }
        except Exception as exc:  # noqa: BLE001 - explicit independent provider chain.
            provider_errors.append(f"{provider_name}:{type(exc).__name__}:{exc}")
    raise MarketScanError("; ".join(provider_errors) or "HK providers unavailable")


def default_history_loader(stock_code: str, lookback_days: int) -> Any:
    """Reuse the project's DB-first multi-provider history chain."""

    from src.services.history_loader import load_history_df

    return load_history_df(stock_code, days=lookback_days)


def render_market_scan_markdown(result: Mapping[str, Any]) -> str:
    """Render a concise, mobile-friendly scanner report."""

    diagnostics = result.get("diagnostics") or {}
    as_of = result.get("as_of") or {}

    def snapshot_summary(market: str) -> str:
        is_a_share = market == MARKET_A
        market_status = (result.get("market_operational_status") or {}).get(market)
        if market_status == "closed":
            return "休市；本轮未抓取、未筛选"
        strict = bool(
            diagnostics.get(
                "a_full_market_strict" if is_a_share else "hk_connect_strict"
            )
        )
        if not strict:
            return "不可用（该市场已安全阻断）"
        source = diagnostics.get(
            "a_snapshot_source" if is_a_share else "hk_snapshot_source"
        ) or "未知来源"
        fetched_at = diagnostics.get(
            "a_snapshot_fetched_at" if is_a_share else "hk_snapshot_fetched_at"
        ) or "未知"
        provider_time = as_of.get(market) or "提供方未返回"
        return (
            f"研究快照可用；报价来源={source}；本轮抓取={fetched_at}；"
            f"提供方时间={provider_time}"
        )

    hk_membership_source = diagnostics.get("hk_membership_source") or "未单独提供"
    hk_membership_age = diagnostics.get("hk_membership_age_hours")
    hk_membership_remaining = diagnostics.get("hk_membership_remaining_hours")
    hk_membership_warning = diagnostics.get("hk_membership_warning_level") or "none"
    hk_membership_summary = (
        "休市；本轮未使用"
        if (result.get("market_operational_status") or {}).get(MARKET_HK) == "closed"
        else f"来源={hk_membership_source}；缓存年龄="
        + (f"{float(hk_membership_age):.1f}小时" if hk_membership_age is not None else "未知")
        + "；距硬到期="
        + (
            f"{float(hk_membership_remaining):.1f}小时"
            if hk_membership_remaining is not None
            else "未知"
        )
        + f"；预警={hk_membership_warning}"
    )
    funnel = diagnostics.get("buy_funnel") or {}

    lines = [
        "# A股 + 港股通全市场分层选股",
        "",
        f"- 数据时间：{result.get('generated_at') or '未知'}",
        f"- A股快照：{snapshot_summary(MARKET_A)}",
        f"- 港股通快照：{snapshot_summary(MARKET_HK)}",
        f"- 港股通成分：{hk_membership_summary}",
        "- 模式：模拟研究；不连接券商；不自动下单",
        f"- 研究扫描链路：{result.get('operational_status') or 'unknown'}",
        "- 运行预警："
        + ("、".join(result.get("operational_warnings") or []) or "无"),
        f"- 可执行条件候选：{funnel.get('actionable_count', 0)}",
        "- 可交易性：研究快照不等于盘中新鲜报价；建仓前仍须通过新鲜L1与人工确认",
        f"- 主动取数：{(result.get('data_policy') or {}).get('description') or '已启用'}",
        "",
    ]
    block_reasons = result.get("push_block_reasons") or []
    market_block_reasons = result.get("market_block_reasons") or {}
    if block_reasons or any(market_block_reasons.values()):
        lines.extend(["## 安全状态", ""])
        lines.extend(f"- {reason}" for reason in block_reasons)
        for market, reasons in market_block_reasons.items():
            for reason in reasons or []:
                lines.append(f"- {market}：{reason}")
        lines.append("")

    if funnel:
        lines.extend(
            [
                "## 买入候选漏斗",
                "",
                f"- 全市场输入：{funnel.get('snapshot_input_count', 0)}",
                f"- L1合格/短名单：{funnel.get('l1_eligible_count', 0)} / "
                f"{funnel.get('l1_shortlist_count', 0)}",
                f"- 历史与交易计划有效：{funnel.get('history_and_plan_valid_count', 0)}",
                f"- 双模型完成/同时通过：{funnel.get('dual_model_reviewed_count', 0)} / "
                f"{funnel.get('dual_model_pass_count', 0)}",
                f"- 可进入盘中买入区复核：{funnel.get('actionable_count', 0)}",
                f"- 分市场可复核：{json.dumps(funnel.get('actionable_by_market') or {}, ensure_ascii=False)}",
                f"- 历史/计划淘汰原因：{json.dumps(funnel.get('history_rejection_reasons') or {}, ensure_ascii=False)}",
                f"- 双模型/质量淘汰原因：{json.dumps(funnel.get('candidate_rejection_reasons') or {}, ensure_ascii=False)}",
                "",
            ]
        )

    candidates = result.get("candidates") or []
    if not candidates:
        lines.extend(["## 本轮结果", "", "没有通过全部数据和风险护栏的候选。", ""])
    for candidate in candidates:
        plan = candidate.get("plan") or {}
        action_label = "条件建仓" if candidate.get("action") == "conditional_buy" else "观察"
        availability = candidate.get("data_availability") or {}
        facts = candidate.get("facts") or {}
        inferences = candidate.get("inferences") or {}
        view = candidate.get("view") or {}
        lines.extend(
            [
                f"## {candidate.get('rank')}. {candidate.get('name') or candidate.get('code')} "
                f"({candidate.get('code')}) · {action_label}",
                "",
                f"- 唯一模拟建议：{candidate.get('simulation_advice') or '等待'}",
                f"- 条件买入区（须有新鲜行情并人工确认）：{plan.get('entry_low', '-')} – {plan.get('entry_high', '-')}",
                f"- 计划失效点：{plan.get('stop_loss', '-')}",
                f"- 观察减仓区：{plan.get('take_profit_1', '-')} / {plan.get('take_profit_2', '-')}",
                f"- 计入成本后的风险回报比：{plan.get('net_rr', '-')}",
                f"- 支撑/压力：{candidate.get('support', '-')} / {candidate.get('resistance', '-')}",
                f"- 模型分歧：{'是' if candidate.get('model_disagreement') else '否'}",
                f"- 最终原因：{candidate.get('action_reason') or '-'}",
                "",
                "### 已核验事实",
                "",
                f"- 提供方行情时间：{facts.get('as_of') or '提供方未返回'}",
                f"- 本轮抓取时间/来源：{facts.get('snapshot_fetched_at') or '-'} / "
                f"{facts.get('snapshot_source') or '-'}",
                f"- 价格/涨跌/成交额：{facts.get('price', '-')} / "
                f"{facts.get('change_pct', '-')}% / {facts.get('amount', '-')}",
                f"- PE/PB：{facts.get('pe', '-')} / {facts.get('pb', '-')}",
                "",
                "### 规则推断（不是事实）",
                "",
                f"- 趋势：{inferences.get('trend') or '-'}",
                f"- 资金行为：{inferences.get('capital_behavior') or '-'}",
                "",
                "### 审慎观点",
                "",
                f"- 短线：{view.get('short_term') or '-'}",
                f"- 波段：{view.get('swing') or '-'}",
                f"- 中线：{view.get('medium_term') or '-'}",
                f"- 长线：{view.get('long_term') or '-'}",
                f"- 数据能力：基础行情={availability.get('basic_quote', '-')}，"
                f"OHLCV={availability.get('ohlcv', '-')}，"
                f"盘口L1={availability.get('order_book_l1', '-')}，"
                f"Level-2={availability.get('level2', '-')}",
                f"- 通义意见：{(candidate.get('qwen_review') or {}).get('verdict', '-')}；"
                f"{(candidate.get('qwen_review') or {}).get('thesis', '')}",
                f"- DeepSeek 意见：{(candidate.get('deepseek_review') or {}).get('verdict', '-')}；"
                f"{(candidate.get('deepseek_review') or {}).get('thesis', '')}",
                "",
            ]
        )

    lines.extend(["---", "", str(result.get("disclaimer") or "")])
    return "\n".join(lines).rstrip() + "\n"
