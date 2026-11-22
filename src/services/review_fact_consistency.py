"""Deterministic numeric facts and fail-closed review consistency checks.

Percent fields are percentage points (0.27972 means 0.27972%, not 27.972%).
This checks structured assertions and directly labelled snapshot claims; it
does not claim to verify arbitrary prose or invent missing source facts.
"""
from __future__ import annotations

import math
import re
from datetime import date
from typing import Any, Mapping

from src.services.market_scan_research import has_announcement_excerpt
from src.services.review_buyback_prices import buyback_price_errors, buyback_price_evidence


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
    contract = {
        "values": values,
        "nullable_missing_fields": [field for field in ("pe", "pb") if field not in values],
        "units": {"price": currency, "amount": currency, "change_pct": "percentage_points", "pe": "multiple", "pb": "multiple"},
        "rendered": "；".join(rendered),
    }
    prices = buyback_price_evidence(candidate)
    if prices:
        contract["buyback_prices"] = prices
    return contract


def _matches(field: str, expected: Any, observed: Any) -> bool:
    if isinstance(expected, str):
        return observed == expected
    if not isinstance(observed, (int, float)) or isinstance(observed, bool) or not math.isfinite(observed):
        return False
    # Structured echoes retain the existing tolerance; prose display precision
    # is handled separately and never substitutes for these source values.
    return math.isclose(expected, observed, rel_tol=1e-6, abs_tol=0.0051 if field != "amount" else 0.51)


# Consume the entire grouped token before validating its grouping. Otherwise
# "1,199,779,572" is silently read as 1, and malformed groups can match a prefix.
_NUMBER = r"[+-]?\d+(?:,\d+)*(?:\.\d+)?(?:[eE][+-]?\d+)?"
_VALID_NUMBER = re.compile(r"[+-]?(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d+)?(?:[eE][+-]?\d+)?")
_CLAIMS = {
    "change_pct": rf"(?P<label>日(?:内)?(?:涨跌幅|涨幅|跌幅|涨跌)|日涨|日跌|当日涨幅|当日跌幅)\s*(?:约|为|达|达到|了|：|:|=)?\s*(?P<number>{_NUMBER})\s*[%％]",
    "price": rf"(?:快照价|最新价|现价|当前价)\s*(?:约|为|：|:|=)?\s*({_NUMBER})\s*(港元|港币|人民币|元|HKD|CNY)?",
    "amount": rf"成交额\s*(?:约|为|：|:|=)?\s*({_NUMBER})\s*(亿|万)?\s*(港元|港币|人民币|元|HKD|CNY)?",
    "pe": rf"(?:(?<![A-Za-z])PE(?![A-Za-z])(?:\(TTM\))?|市盈率(?:\(TTM\))?)\s*(?:约|为|：|:|=)?\s*({_NUMBER})",
    "pb": rf"(?:(?<![A-Za-z])PB(?![A-Za-z])|市净率)\s*(?:约|为|：|:|=)?\s*({_NUMBER})",
}


def _stop_distance_context(prefix: str, candidate: Mapping[str, Any], currency: str) -> bool:
    """Recognize a distance label, checking any explicit stop against the plan."""
    context = re.search(
        rf"止损(?:价|位)?\s*(?:(?:约|为|：|:|=)?\s*(?P<stop>{_NUMBER})\s*"
        r"(?P<currency>港元|港币|人民币|元|HKD|CNY)?\s*)?距(?:离)?\s*$",
        prefix, flags=re.IGNORECASE,
    )
    if not context:
        return False
    literal = context.group("stop")
    if literal is None:
        return True
    plan = candidate.get("plan")
    stop = plan.get("stop_loss") if isinstance(plan, Mapping) else None
    if (not _VALID_NUMBER.fullmatch(literal)
            or not isinstance(stop, (int, float)) or isinstance(stop, bool)
            or not math.isfinite(stop) or stop <= 0
            or not _matches("price", stop, float(literal.replace(",", "")))):
        return False
    unit = context.group("currency")
    return not unit or ("HKD" if unit.lower() in {"港元", "港币", "hkd"} else "CNY") == currency


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
    parts = literal.replace(",", "").lower().split("e")
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


# Only admit source-specific figures with an identifiable issuer, date and unit.
# These values never replace numeric_facts in the required snapshot echo.
def _dated_evidence(candidate: Mapping[str, Any]) -> dict:
    facts = candidate.get("facts") or {}
    research = candidate.get("research_evidence") or {}
    if not isinstance(facts, Mapping) or not isinstance(research, Mapping):
        return {}
    cutoff = str(facts.get("snapshot_fetched_at") or facts.get("as_of") or "")[:10]
    code = str(candidate.get("code") or "")
    name = str(candidate.get("name") or "")
    if not re.fullmatch(r"\d{6}", code) or not name or not cutoff:
        return {}
    records = {}
    for block, field in (("announcements", "pe"), ("announcements_and_news", "amount")):
        section = research.get(block)
        if not isinstance(section, Mapping):
            continue
        for item in section.get("items") or []:
            if not isinstance(item, Mapping):
                continue
            try:
                day = date.fromisoformat(str(item.get("published_date") or "")[:10])
            except ValueError:
                continue
            if day.isoformat() >= cutoff:
                continue
            if field == "pe":
                body = re.sub(r"\s+", "", str(item.get("excerpt") or ""))
                if not (has_announcement_excerpt(item) and re.search(rf"证券代码[:：]{code}(?!\d)", body)
                        and name in body and item.get("source") == "cninfo"):
                    continue
                values = re.findall(r"(?:本公司|公司股票价格)[^。；]{0,48}?[，,]动态市盈率(?:处于|为)"
                                    r"(\d+(?:\.\d+)?)倍", body)
                amounts = [float(v) for v in values]
            else:
                title = str(item.get("title") or "")
                if not title.startswith(f"{name}{code}龙虎榜数据"):
                    continue
                snippet = str(item.get("snippet") or "")
                if f"交易所{day.year}年{day.month}月{day.day}日公布" not in snippet:
                    continue
                values = re.findall(re.escape(name) + r"当日收报[^。；]{0,100}?成交额(\d+(?:\.\d+)?)(亿|万)元?", snippet)
                amounts = [float(v) * (1e8 if unit == "亿" else 1e4) for v, unit in values]
            if amounts:
                records.setdefault((field, day), set()).update(amounts)
    return records


def _dated_values(field: str, prefix: str, records: dict) -> set[float] | None:
    """None denotes a snapshot claim; a dated claim needs one unambiguous source value."""
    if field not in {"pe", "amount"}:
        return None
    clause = re.split(r"[。！？\n]", prefix)[-1]
    if field == "pe":
        scope = re.search(r"(\d{4}-\d{2}-\d{2})(?:公司发布[^，；]{0,18}|异常波动)?公告"
                          r"[^。！？]{0,85}动态$", clause)
    else:
        scope = re.search(r"(?<![\d-])((?:\d{4}-)?\d{1,2}[-月]\d{1,2})日?"
                          r"(?:因[^。；、，,]{1,16}[上登]龙虎榜|[上登]龙虎榜|涨停|收报|收盘)[^。；]{0,48}$", clause)
    if not scope or re.search(r"当前|今日|今天|最新|快照|同行|行业|同业|板块|市场整体|另一|其他公司", scope[0]):
        return None
    token = scope[1].replace("月", "-")
    parts = tuple(int(part) for part in token.split("-"))
    values = set()
    for (kind, day), numbers in records.items():
        if kind == field and (day.year, day.month, day.day)[-len(parts):] == parts:
            values.update(numbers)
    return values


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
    texts = [(key, str(review.get(key) or "")) for key in ("thesis", "view")]
    for key in ("facts", "risks", "inferences", "invalidators"):
        values = review.get(key) or []
        texts.extend((key, value) for value in
                     ([values] if isinstance(values, str) else [str(value) for value in values]))
    dated = _dated_evidence(candidate)
    buyback_prices = buyback_price_evidence(candidate)
    facts = candidate.get("facts")
    as_of = str(facts.get("as_of") or facts.get("snapshot_fetched_at") or "") if isinstance(facts, Mapping) else ""
    for text_field, text in texts:
        errors.extend(buyback_price_errors(text, text_field, buyback_prices, as_of))
        for field, pattern in _CLAIMS.items():
            for match in re.finditer(pattern, text, flags=re.IGNORECASE):
                literal = match.group("number") if field == "change_pct" else match.group(1)
                context_start = max(0, match.start() - 120)
                context = {"text_field": text_field,
                           "claim_context": text[context_start:min(match.end() + 120, context_start + 480)]}
                if not _VALID_NUMBER.fullmatch(literal):
                    errors.append({"field": field, "error": "invalid_numeric_format",
                                   "claim": match.group(0), **context})
                    continue
                if (field == "change_pct"
                        and match.group("label") in {"日涨幅", "日跌幅", "日涨跌幅", "日涨跌"}
                        and re.search(
                            r"连续\s*(?:[2-9]|[1-9]\d|[二三四五六七八九两]|"
                            r"[一二三四五六七八九]?十[一二三四五六七八九]?)\s*(?:个\s*交易)?\s*$",
                            text[max(0, match.start() - 32):match.start()])):
                    # An explicit multi-day window is not today's return.
                    # Keep the original risk; echoes and same-sentence daily
                    # claims still undergo their independent numeric checks.
                    continue
                if field in {"pe", "pb"}:
                    research_date = re.match(
                        r"(\d{4}-\d{2}-\d{2})\s*(?:[A-Za-z\u4e00-\u9fff]{2,12}口径研究估值|(?:研究|估值)(?:口径|日期|时点))",
                        text[match.start(1):match.start(1) + 32],
                    )
                    if research_date and literal == research_date.group(1)[:4]:
                        try:
                            date.fromisoformat(research_date.group(1))
                        except ValueError:
                            pass  # Invalid dates retain the numeric check.
                        else:
                            # A marked research date is not a PE/PB multiple;
                            # keep the original risk and all numeric checks.
                            continue
                if (field == "price" and not match.group(2)
                        and re.match(r"[%％]", text[match.end():])
                        and _stop_distance_context(text[max(0, match.start() - 96):match.start()],
                                                   candidate, expected.get("currency", ""))):
                    # "止损距现价9.4%" is a relative distance, not a price.
                    # Keep the original risk; structured echoes and absolute
                    # price claims still use their existing checks.
                    continue
                if (field == "amount" and not match.group(2) and not match.group(3)
                        and re.search(r"(?:占|佔)(?:当日|當日|总|總)?\s*$",
                                      text[max(0, match.start() - 16):match.start()])
                        and re.match(r"[%％]", text[match.end():])):
                    # "占成交额6.29%" states a share, not a currency amount.
                    # It is outside the snapshot-amount contract; structured
                    # echoes and any absolute amount claims still get checked.
                    continue
                value = float(literal.replace(",", ""))
                sourced = _dated_values(field, text[context_start:match.start()], dated)
                if sourced is not None and len(sourced) != 1:
                    errors.append({"field": field, "error": "unsupported_dated_claim",
                                   "claim": match.group(0), **context})
                    continue
                dated_value = next(iter(sourced)) if sourced else None
                reference = dated_value if dated_value is not None else expected.get(field)
                if reference is None:
                    errors.append({"field": field, "observed": value,
                                   "error": "unsupported_snapshot_claim", "claim": match.group(0), **context})
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
                    valid = _prose_matches(field, reference, value, literal, multiplier)
                    currency = match.group(3)
                else:
                    valid = _prose_matches(field, reference, value, literal)
                    currency = match.group(2) if field == "price" else None
                if field == "change_pct" and direction:
                    if direction * expected[field] < 0 or (direction > 0 and value < 0):
                        valid = False
                if currency:
                    currency = "HKD" if currency.lower() in {"港元", "港币", "hkd"} else "CNY"
                    if currency != expected.get("currency"):
                        valid = False
                if not valid:
                    errors.append({"field": field, "expected": reference, "observed": value,
                                   "error": "dated_claim_mismatch" if dated_value is not None else "snapshot_claim_mismatch", "claim": match.group(0), **context})
    return errors
