"""Verify labelled per-share prices of an issuer's reported daily repurchase."""
from __future__ import annotations

import math
import re
import unicodedata
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from src.services.market_scan_research import has_announcement_excerpt

_NUMBER = r"[+-]?\d+(?:,\d+)*(?:\.\d+)?(?:[eE][+-]?\d+)?"
_VALID_NUMBER = re.compile(r"[+-]?(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d+)?(?:[eE][+-]?\d+)?")
_CCY = r"HKD|港元|港币|CNY|RMB|人民币(?:元)?|USD|美元|元"
_PRICE_UNIT = rf"(?:[万亿]?(?:{_CCY})|[万亿%倍]|日元|欧元|(?-i:[A-Z]{{3}}))"
_SCALAR = re.compile(
    r"(?:(?P<high>最高(?:回购|购回|成交)?价(?:格)?|(?:价格?|区间)上限)"
    r"|(?P<low>最低(?:回购|购回|成交)?价(?:格)?|(?:价格?|区间)下限)"
    r"|(?P<average>均价|平均(?:回购|购回|成交)?价(?:格)?))"
    rf"\s*(?:约为|约|为|是|[:=])?\s*(?P<value>{_NUMBER})\s*(?P<unit>{_PRICE_UNIT})?", re.I,
)
_RANGE = re.compile(
    rf"(?:价格(?:区间|范围)?|(?:回购|购回|成交)价(?:格)?(?:区间|范围)?|每股(?:价格?)?"
    rf"|以(?={_NUMBER}[^,。]{{0,48}}?(?:回购|购回)))"
    rf"\s*(?:约为|介于|为|在|从|约|[:=])?\s*\(?\s*(?P<low>{_NUMBER})\s*(?P<low_unit>{_PRICE_UNIT})?"
    rf"\s*[-–—~～至到]\s*(?P<high>{_NUMBER})\s*(?P<high_unit>{_PRICE_UNIT})?", re.I,
)
_DATE = re.compile(r"(?:(\d{4})年)?(\d{1,2})月(\d{1,2})[日号]?|(\d{4})-(\d{2})-(\d{2})")
_SHARES = re.compile(rf"(?:回购|购回)(?:了|股份|股票|公司股份)?\s*({_NUMBER})\s*([万亿]?)股")
_COST = re.compile(rf"(?:斥资|耗资|(?:回购)?总(?:代价|金额)|回购金额)\s*(?:为|达)?\s*({_NUMBER})\s*([万亿]?)\s*({_CCY})", re.I)
_BUYBACK = re.compile(r"回购|购回")
_PLANNED = re.compile(r"(?:拟|计划|建议|假设|预计|打算|考虑)(?:以.{0,24}?)?(?:回购|购回)|(?:回购|购回)(?:授权|计划|方案|预案)")
# Separate a later plan from an already reported fill in the same sentence.
_BREAK = re.compile(r"[。！？!?；;\n]+|,(?=[^,]{0,20}(?:" + _PLANNED.pattern +
                    r"))|,(?=[^,]{0,12}(?:已于|实际|已经)[^,]{0,20}(?:回购|购回))")
_RELATIVE_DAY = re.compile(r"昨日|昨天|今日|今天|上周|上月|前日")
_OTHER_PRICE = re.compile(r"股价|股票(?:当日|日内|交易)|收盘价|目标价|买入区|止损|日内行情|当日行情|市场交易价|二级市场")
_FOLLOWUP = re.compile(r"^(?:其中|本次|其)?\s*(?:每股|最低|最高|平均|均价|回购价|价格区间|成交均价)")
_OTHER_SUBJECT = re.compile(r"子公司|母公司|控股股东|供应商|同行|竞争对手|其他公司|另(?:有|一家|据)|相比|分别|多家公司")
_ROLES = ("low", "high", "average")
_TRANSLATE = str.maketrans("購價幣萬億總為區間於約數額", "购价币万亿总为区间于约数额")


def _normalize(value: Any) -> str:
    return unicodedata.normalize("NFKC", str(value or "")).translate(_TRANSLATE)


def _currency(unit: str) -> str:
    if unit.upper() in {"HKD", "港元", "港币"}:
        return "HKD"
    if unit.upper() in {"USD", "美元"}:
        return "USD"
    if unit.upper() in {"CNY", "RMB", "人民币", "人民币元", "元"}:
        return "CNY"
    return ""


def _price(literal: str):
    if not _VALID_NUMBER.fullmatch(literal):
        return None
    try:
        price = Decimal(literal.replace(",", ""))
    except InvalidOperation:
        return None
    return price if price > 0 and -308 <= price.as_tuple().exponent <= 308 and math.isfinite(float(price)) else None


def _roles(sentence: str):
    for pattern in (_RANGE, _SCALAR):
        for match in pattern.finditer(sentence):
            # The last subject in this clause may be the market or a trade plan.
            prefix = sentence[:match.start()]
            prefix = _BUYBACK.split(prefix)[-1]
            aggregate = re.search(r"(?:多日|密集|历次|累计)(?:回购|购回)?(?:价格?)?$", sentence[:match.start()])
            if _OTHER_PRICE.search(prefix) or aggregate:
                continue
            if pattern is _RANGE:
                yield "low", match["low"], match["low_unit"] or match["high_unit"], match
                yield "high", match["high"], match["high_unit"] or match["low_unit"], match
            else:
                role = next(role for role in _ROLES if match[role])
                yield role, match["value"], match["unit"], match


def _execution_sentences(text: str):
    previous = False
    for part in _BREAK.split(text):
        sentence = part.strip()
        if not sentence:
            continue
        active = _BUYBACK.search(sentence) or (previous and _FOLLOWUP.match(sentence))
        previous = bool(active and not _PLANNED.search(sentence))
        if previous:
            yield sentence


def _date_parts(match: re.Match) -> tuple:
    y, m, d = match.group(1, 2, 3) if match[2] else match.group(4, 5, 6)
    return y, int(m), int(d)


def _day_of_source(text: str, published: str):
    try:
        publication = date.fromisoformat(published[:10])
    except ValueError:
        return None
    days = set()
    for match in _DATE.finditer(text):
        if re.match(r"\s*[电讯]", text[match.end():]):
            continue
        year, month, day = _date_parts(match)
        try:
            event = date(int(year or publication.year), month, day)
            if not year and publication.month == 1 and month == 12:
                event = event.replace(year=event.year - 1)
        except ValueError:
            return None
        days.add(event)
    if not days and re.search(r"(?:今日|当天)(?:公司)?(?:回购|购回)", text):
        days.add(publication)
    if len(days) == 1:
        event = days.pop()
        if event <= publication:
            return event.isoformat()
    return None


def _issuer_news(title: str, name: str, code: str) -> bool:
    # Match the actual subject before the event, not a supplier or a name
    # appearing in the article's market comparison.
    subject = re.sub(r"-(?:W|SW|B|S|R)$", "", name, flags=re.I)
    names = [re.escape(v) for v in (name, subject) if v]
    if code:
        names.append(r"(?:HK)?" + re.escape(code) + r"(?!\d)")
    if not names:
        return False
    listing = rf"(?:\s*\(\s*(?:HK)?{re.escape(code)}(?:\.HK|\.SH|\.SZ)?\s*\))?" if code else ""
    prefix = re.match(r"^(?:" + "|".join(names) + ")" + listing, title, re.I)
    if not prefix or not _BUYBACK.search(title):
        return False
    lead = _BUYBACK.split(title[prefix.end():], maxsplit=1)[0]
    return bool(re.fullmatch(
        r"(?:[\s:,\d./年月日号万亿元]|HKD|港元|港币|美元|人民币|于|在港交所|公告|当日|今日|"
        r"连续|累计|再次|已|共|斥资|耗资|合计|以|每股|至|-)*", lead, re.I))


def _evidence_rows(candidate):
    research = candidate.get("research_evidence")
    if not isinstance(research, Mapping):
        return
    code = str(candidate.get("code") or "").removeprefix("HK")
    name = _normalize(candidate.get("name")).strip()
    for block, official in (("announcements_and_news", False), ("announcements", True)):
        data = research.get(block)
        if not isinstance(data, Mapping) or not isinstance(data.get("items"), (list, tuple)):
            continue
        for item in data["items"]:
            if not isinstance(item, Mapping):
                continue
            title = _normalize(item.get("title"))
            if official:
                if not has_announcement_excerpt(item):
                    continue
                body = _normalize(item["excerpt"])
            else:
                if not _issuer_news(title, name, code):
                    continue
                body = _normalize(item.get("snippet"))
                # A clearly different subject in a summary cannot supply this
                # issuer's prices. Keep the issuer-led headline on its own.
                tickers = re.findall(r"\((?:HK)?(\d{5,6})(?:\.(?:HK|SH|SZ))?\)", body, re.I)
                if (_OTHER_SUBJECT.search(body) or any(t != code for t in tickers)
                        or not ((name and name in body) or (code and code in tickers)
                                or re.match(r"(?:\d{1,4}[年月日号,-])+\s*(?:,\s*)?(?:本?公司|本集团)|^(?:本?公司|本集团)", body))):
                    body = ""
            if not _PLANNED.search(title):
                yield official, title + "\n" + body, str(item.get("published_date") or "")


def _magnitude(match: re.Match) -> float:
    value = _price(match[1])
    return float(value) * {"": 1, "万": 1e4, "亿": 1e8}[match[2]] if value else 0


def buyback_price_evidence(candidate: Mapping[str, Any]) -> list[dict]:
    observations = defaultdict(lambda: defaultdict(set))
    for official, text, published in _evidence_rows(candidate):
        if not _BUYBACK.search(text):
            continue
        # An unlabelled table spanning dates/currencies cannot prove the
        # endpoint of a daily buyback range.
        day = _day_of_source(text, published)
        currencies = {_currency(c) for c in re.findall(_CCY, text, re.I) if c != "元"}
        if not currencies and not str(candidate.get("code", "")).startswith("HK") and "元" in text:
            currencies.add("CNY")
        if not day or len(currencies) != 1:
            continue
        key = day, currencies.pop()
        for sentence in _execution_sentences(text):
            for role, literal, unit, _ in _roles(sentence):
                value = _price(literal)
                if value is not None and (not unit or unit == "元" or _currency(unit) == key[1]):
                    observations[key][role, official].add(float(value))
            shares, costs = list(_SHARES.finditer(sentence)), list(_COST.finditer(sentence))
            if len(shares) == len(costs) == 1 and not re.search(r"累计|授权|约|逾|超过|近\d", sentence):
                quantity, cost = _magnitude(shares[0]), _magnitude(costs[0])
                if quantity > 0 and cost > 0 and math.isfinite(cost / quantity):
                    observations[key]["derived_average", official].add(cost / quantity)
    rows = []
    for (day, currency), values in sorted(observations.items()):
        row = {"date": day, "currency": currency}
        for role in _ROLES:
            # Resolve competing sources at equal priority conservatively.
            keys = [(role, True), (role, False)]
            if role == "average":
                keys += [("derived_average", True), ("derived_average", False)]
            for key in keys:
                known = values.get(key)
                if known:
                    if len(known) == 1:
                        row[role] = next(iter(known))
                    break
        rows.append(row)
    return rows


def buyback_price_errors(text: str, field: str, records: list[dict], as_of: str = "") -> list[dict]:
    errors = []
    context_day = None
    for sentence in _execution_sentences(_normalize(text)):
        dates = list(_DATE.finditer(sentence))
        if _BUYBACK.search(sentence):
            context_day = None
        for role, literal, unit, match in _roles(sentence):
            previous_dates = [d for d in dates if d.end() <= match.start()]
            day = previous_dates[-1] if previous_dates else context_day
            eligible = records
            if day:
                year, month, n = _date_parts(day)
                eligible = [r for r in eligible if r["date"].endswith(f"-{month:02d}-{n:02d}")
                            and (not year or r["date"][:4] == year)]
            elif dates:
                eligible = []  # A later date can belong to a different event.
            elif relative := _RELATIVE_DAY.search(sentence):
                eligible = []
                offsets = {"昨日": 1, "昨天": 1, "今日": 0, "今天": 0, "前日": 2}
                if relative[0] in offsets and as_of:
                    try:
                        event = date.fromisoformat(as_of[:10]) - timedelta(days=offsets[relative[0]])
                    except (ValueError, OverflowError):
                        pass
                    else:
                        eligible = [r for r in records if r["date"] == event.isoformat()]
            if unit and unit != "元":
                eligible = [r for r in eligible if _currency(unit) == r["currency"]]
            source = eligible[0] if len(eligible) == 1 else {}
            expected = source.get(role)
            price = _price(literal)
            observed = float(price) if price is not None else None
            if (expected is not None and price is not None and math.isclose(
                    expected, observed, rel_tol=0, abs_tol=0.5 * 10.0 ** price.as_tuple().exponent + 1e-12)):
                continue
            errors.append({
                "field": "buyback_" + role, "text_field": field, "claim": match[0],
                "error": "buyback_price_mismatch" if expected is not None else "unsupported_buyback_price",
                "expected": expected, "observed": observed, "date": source.get("date"), "currency": source.get("currency"),
            })
        if dates:
            context_day = dates[-1]
    return errors
