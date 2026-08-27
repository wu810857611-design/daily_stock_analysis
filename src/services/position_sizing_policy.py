# -*- coding: utf-8 -*-
"""Deterministic opportunity tiers for simulated position sizing.

The policy deliberately separates the first entry size from the eventual
single-name ceiling. Stronger evidence permits a larger first tranche and a
higher ceiling, while later adds still require independent re-confirmation.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


STANDARD_TIER = "standard"
STRONG_TIER = "strong"
EXCEPTIONAL_TIER = "exceptional"
DEFAULT_OPPORTUNITY_TIER = STANDARD_TIER
POSITION_STEP_FRACTION = 0.025
SUPPORTED_TRANCHE_FRACTIONS = (0.10, 0.05, 0.025)


@dataclass(frozen=True)
class OpportunityPolicy:
    tier: str
    label: str
    cash_floor_ratio: float
    max_single_position_ratio: float
    initial_position_fraction: float
    add_position_fraction: float


OPPORTUNITY_POLICIES = {
    STANDARD_TIER: OpportunityPolicy(
        tier=STANDARD_TIER,
        label="普通机会",
        cash_floor_ratio=0.15,
        max_single_position_ratio=0.15,
        initial_position_fraction=0.025,
        add_position_fraction=0.025,
    ),
    STRONG_TIER: OpportunityPolicy(
        tier=STRONG_TIER,
        label="强机会",
        cash_floor_ratio=0.05,
        max_single_position_ratio=0.35,
        initial_position_fraction=0.05,
        add_position_fraction=0.05,
    ),
    EXCEPTIONAL_TIER: OpportunityPolicy(
        tier=EXCEPTIONAL_TIER,
        label="极强机会",
        cash_floor_ratio=0.0,
        max_single_position_ratio=0.50,
        initial_position_fraction=0.10,
        add_position_fraction=0.05,
    ),
}


def _finite(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def opportunity_policy(tier: Any) -> OpportunityPolicy:
    """Return a known policy, failing closed to the standard tier."""

    normalized = str(tier or "").strip().lower()
    return OPPORTUNITY_POLICIES.get(
        normalized,
        OPPORTUNITY_POLICIES[DEFAULT_OPPORTUNITY_TIER],
    )


def classify_opportunity(
    *,
    rank: Any,
    data_quality: Any,
    net_rr: Any,
    qwen_confidence: Any,
    deepseek_confidence: Any,
) -> OpportunityPolicy:
    """Classify an already-actionable candidate without model discretion.

    The scanner's existing dual-pass, hard-risk, fresh-snapshot, and complete
    plan gates run before this classifier. This function only decides the
    deterministic tranche sizes and how far repeated confirmation may scale.
    """

    values = {
        "rank": _finite(rank),
        "data_quality": _finite(data_quality),
        "net_rr": _finite(net_rr),
        "qwen_confidence": _finite(qwen_confidence),
        "deepseek_confidence": _finite(deepseek_confidence),
    }
    if any(value is None for value in values.values()):
        return opportunity_policy(STANDARD_TIER)

    minimum_model_confidence = min(
        values["qwen_confidence"],
        values["deepseek_confidence"],
    )
    if (
        values["rank"] <= 3
        and values["data_quality"] >= 0.80
        and values["net_rr"] >= 2.0
        and minimum_model_confidence >= 0.90
    ):
        return opportunity_policy(EXCEPTIONAL_TIER)
    if (
        values["rank"] <= 5
        and values["data_quality"] >= 0.80
        and values["net_rr"] >= 2.0
        and minimum_model_confidence >= 0.85
    ):
        return opportunity_policy(STRONG_TIER)
    return opportunity_policy(STANDARD_TIER)


__all__ = [
    "DEFAULT_OPPORTUNITY_TIER",
    "EXCEPTIONAL_TIER",
    "OPPORTUNITY_POLICIES",
    "OpportunityPolicy",
    "POSITION_STEP_FRACTION",
    "SUPPORTED_TRANCHE_FRACTIONS",
    "STANDARD_TIER",
    "STRONG_TIER",
    "classify_opportunity",
    "opportunity_policy",
]
