"""Deterministic numeric facts and fail-closed review consistency checks.

Percent fields are percentage points (0.27972 means 0.27972%, not 27.972%).
This checks structured assertions and directly labelled snapshot claims; it
does not claim to verify arbitrary prose or invent missing source facts.
"""
from __future__ import annotations

import math
import re
from typing import Any, Mapping


def numeric_facts(candidate: Mapping[str, Any]) -> dict:
    result = {}
    for field in ("price", "change_pct", "amount", "pe", "pb"):
        raw = candidate.get(field)
        if isinstance(raw, (int, float)) and not isinstance(raw, bool) and math.isfinite(raw):
            result[field] = raw
    if result:
        result["currency"] = "HKD" if str(candidate.get("code", "")).startswith("HK") else "CNY"
    return result


def fact_contract(candidate: Mapping[str, Any]) -> dict:
    values = numeric_facts(candidate)
    currency = values.get("currency", "")
    labels = {"price": "快照价", "change_pct": "日涨跌幅", "amount": "成交额", "pe": "PE", "pb": "PB"}
    rendered = []
    for field, value in values.items():
        if field == "currency":
            continue
        unit = "%" if field == "change_pct" else currency if field in {"price", "amount"} else "倍"
        rendered.append(f"{labels[field]} {value:g}{unit}")
    return {"values": values,
            "units": {"price": currency, "amount": currency, "change_pct": "percentage_points", "pe": "multiple", "pb": "multiple"},
            "rendered": "；".join(rendered)}


def _matches(field: str, expected: Any, observed: Any) -> bool:
    if isinstance(expected, str):
        return observed == expected
    if not isinstance(observed, (int, float)) or isinstance(observed, bool) or not math.isfinite(observed):
        return False
    # Two decimal places for percentage points/prices/ratios. Amount prose can
    # round to 0.01 billion (0.005 * 1e8); structured amounts use tighter bounds.
    return math.isclose(expected, observed, rel_tol=1e-6, abs_tol=0.0051 if field != "amount" else 0.51)


_CLAIMS = {
    "change_pct": r"(?:日(?:内)?(?:涨跌幅|涨幅|跌幅|涨跌)|日涨|日跌|当日涨幅|当日跌幅)\s*(?:约|为|达|达到|了|：|:|=)?\s*([+-]?\d+(?:\.\d+)?)\s*[%％]",
    "price": r"(?:快照价|最新价|现价|当前价)\s*(?:约|为|：|:|=)?\s*([+-]?\d+(?:\.\d+)?)\s*(港元|港币|人民币|元|HKD|CNY)?",
    "amount": r"成交额\s*(?:约|为|：|:|=)?\s*([+-]?\d+(?:\.\d+)?)\s*(亿|万)?\s*(港元|港币|人民币|元|HKD|CNY)?",
}


def validate_review_facts(review: Mapping[str, Any], candidate: Mapping[str, Any], *, require_echo: bool = False) -> list[dict]:
    expected = numeric_facts(candidate)
    errors = []
    echoed = review.get("numeric_facts")
    if expected and (require_echo or echoed is not None):
        if not isinstance(echoed, Mapping):
            errors.append({"field": "numeric_facts", "error": "missing_or_invalid"})
        else:
            for field, value in expected.items():
                if not _matches(field, value, echoed.get(field)):
                    errors.append({"field": field, "expected": value, "observed": echoed.get(field), "error": "structured_mismatch"})
            for field in set(echoed) - set(expected):
                errors.append({"field": field, "error": "unsupported_numeric_fact"})
    texts = [str(review.get(key) or "") for key in ("thesis", "view")]
    for key in ("facts", "risks", "inferences", "invalidators"):
        values = review.get(key) or []
        texts.extend([values] if isinstance(values, str) else [str(value) for value in values])
    for text in texts:
        for field, pattern in _CLAIMS.items():
            if field not in expected:
                continue
            for match in re.finditer(pattern, text, flags=re.IGNORECASE):
                value = float(match.group(1))
                if field == "change_pct" and any(word in match.group(0) for word in ("日跌", "跌幅")):
                    value = -abs(value)
                if field == "amount":
                    multiplier = {"亿": 1e8, "万": 1e4}.get(match.group(2), 1)
                    value *= multiplier
                    valid = math.isclose(expected[field], value, rel_tol=1e-6,
                                         abs_tol=0.0051 * multiplier)
                    currency = match.group(3)
                else:
                    valid = _matches(field, expected[field], value)
                    currency = match.group(2) if field == "price" else None
                if currency:
                    currency = "HKD" if currency.lower() in {"港元", "港币", "hkd"} else "CNY"
                    if currency != expected.get("currency"):
                        valid = False
                if not valid:
                    errors.append({"field": field, "expected": expected[field], "observed": value,
                                   "error": "snapshot_claim_mismatch", "claim": match.group(0)})
    return errors
