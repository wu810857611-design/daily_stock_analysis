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
        if field in {"pe", "pb"} and not (isinstance(raw, (int, float)) and not isinstance(raw, bool) and math.isfinite(raw)):
            def mapping(value):
                return value if isinstance(value, Mapping) else {}
            research = mapping(candidate.get("research_evidence"))
            data = mapping(mapping(research.get("fundamentals")).get("data"))
            valuation = mapping(mapping(data.get("valuation")).get("data"))
            raw = valuation.get("pe_ratio" if field == "pe" else "pb_ratio")
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
            "nullable_missing_fields": [field for field in ("pe", "pb") if field not in values],
            "units": {"price": currency, "amount": currency, "change_pct": "percentage_points", "pe": "multiple", "pb": "multiple"},
            "rendered": "；".join(rendered)}


def _matches(field: str, expected: Any, observed: Any) -> bool:
    if isinstance(expected, str):
        return observed == expected
    if not isinstance(observed, (int, float)) or isinstance(observed, bool) or not math.isfinite(observed):
        return False
    # Structured echoes retain the existing tolerance; prose display precision
    # is handled separately and never substitutes for these source values.
    return math.isclose(expected, observed, rel_tol=1e-6, abs_tol=0.0051 if field != "amount" else 0.51)


_NUMBER = r"[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?"
_CLAIMS = {
    "change_pct": rf"(?P<label>日(?:内)?(?:涨跌幅|涨幅|跌幅|涨跌)|日涨|日跌|当日涨幅|当日跌幅)\s*(?:约|为|达|达到|了|：|:|=)?\s*(?P<number>{_NUMBER})\s*[%％]",
    "price": rf"(?:快照价|最新价|现价|当前价)\s*(?:约|为|：|:|=)?\s*({_NUMBER})\s*(港元|港币|人民币|元|HKD|CNY)?",
    "amount": rf"成交额\s*(?:约|为|：|:|=)?\s*({_NUMBER})\s*(亿|万)?\s*(港元|港币|人民币|元|HKD|CNY)?",
    "pe": rf"(?:(?<![A-Za-z])PE(?![A-Za-z])(?:\(TTM\))?|市盈率(?:\(TTM\))?)\s*(?:约|为|：|:|=)?\s*({_NUMBER})",
    "pb": rf"(?:(?<![A-Za-z])PB(?![A-Za-z])|市净率)\s*(?:约|为|：|:|=)?\s*({_NUMBER})",
}


def _prose_matches(field: str, expected: float, observed: float, literal: str,
                   multiplier: float = 1) -> bool:
    """Compare a labelled claim at its printed precision, after unit conversion.

    Structured echoes still use _matches. A rounded sentence is not a new
    source value, and cannot replace the original input used by trading gates.
    """
    if not math.isfinite(observed):
        return False
    # A coarse zero (e.g. 0e308) must not erase a nonzero source value.
    # Retain the original small rounding allowance for literal zero.
    if observed == 0:
        return _matches(field, expected, observed)
    parts = literal.lower().split("e")
    decimals = len(parts[0].split(".")[1]) if "." in parts[0] else 0
    try:
        exponent = int(parts[1]) if len(parts) == 2 else 0
    except ValueError:
        return False
    if not -308 <= exponent <= 308:
        return False
    rounding = 0.5 * (10.0 ** (exponent - decimals)) * multiplier
    if not math.isfinite(rounding):
        return False
    if expected * observed < 0:
        return False
    base = 0.51 if field == "amount" else 0.0051
    return math.isclose(expected, observed, rel_tol=1e-6,
                        abs_tol=max(base, rounding) + 1e-12)


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
                if field in {"pe", "pb"} and echoed[field] is None:
                    continue
                errors.append({"field": field, "observed": echoed[field],
                               "error": "unsupported_numeric_fact"})
    texts = [str(review.get(key) or "") for key in ("thesis", "view")]
    for key in ("facts", "risks", "inferences", "invalidators"):
        values = review.get(key) or []
        texts.extend([values] if isinstance(values, str) else [str(value) for value in values])
    for text in texts:
        for field, pattern in _CLAIMS.items():
            for match in re.finditer(pattern, text, flags=re.IGNORECASE):
                literal = match.group("number") if field == "change_pct" else match.group(1)
                value = float(literal)
                if field not in expected:
                    errors.append({"field": field, "observed": value,
                                   "error": "unsupported_snapshot_claim", "claim": match.group(0)})
                    continue
                direction = 0
                if field == "change_pct":
                    label = match.group("label")
                    if label in {"日跌", "日跌幅", "日内跌幅", "当日跌幅"}:
                        direction = -1
                        value = -abs(value)
                    elif label in {"日涨", "日涨幅", "日内涨幅", "当日涨幅"}:
                        direction = 1
                if field == "amount":
                    multiplier = {"亿": 1e8, "万": 1e4}.get(match.group(2), 1)
                    value *= multiplier
                    valid = _prose_matches(field, expected[field], value, literal, multiplier)
                    currency = match.group(3)
                else:
                    valid = _prose_matches(field, expected[field], value, literal)
                    currency = match.group(2) if field == "price" else None
                if field == "change_pct" and direction:
                    if direction * expected[field] < 0 or (direction > 0 and value < 0):
                        valid = False
                if currency:
                    currency = "HKD" if currency.lower() in {"港元", "港币", "hkd"} else "CNY"
                    if currency != expected.get("currency"):
                        valid = False
                if not valid:
                    errors.append({"field": field, "expected": expected[field], "observed": value,
                                   "error": "snapshot_claim_mismatch", "claim": match.group(0)})
    return errors
