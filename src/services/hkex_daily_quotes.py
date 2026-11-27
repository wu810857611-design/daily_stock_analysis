"""Unadjusted HKD closing prices and turnover from HKEX's dated Daily Quotations."""
from datetime import date
from html import unescape
import re

import requests

from src.services.research_execution import record_isolated_phase

SOURCE = "hkex.daily_quotations"
START = b'<a name = "quotations">'
END = b'<a name = "sales_all">'
MAX_QUOTATION_BYTES = 8 * 1024 * 1024


def parse_daily_quotes(data: bytes, trade_date: date, symbols: set[str]) -> dict:
    header, marker, rest = data.partition(START)
    section, end, _ = rest.partition(END)
    report_date = re.search(rb"\bDATE:\s*(\d{1,2}) ([A-Z]{3}) (\d{4})\b", header)
    months = b"JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split()
    expected = (trade_date.day, months[trade_date.month - 1], trade_date.year)
    if not report_date or (int(report_date[1]), report_date[2], int(report_date[3])) != expected:
        raise ValueError("HKEX daily quotation trade date mismatch")
    if not marker or not end or b"PRV.CLO./" not in section or b"TURNOVER ($)" not in section:
        raise ValueError("HKEX quotation section missing or incomplete")

    # The exchange prints each security on two fixed-column lines. HTML page
    # breaks may bisect a pair; only the QUOTATIONS section has this layout.
    lines = unescape(re.sub(r"<[^>]*>", "", section.decode("latin1"))).splitlines()
    rows, suspended, seen, duplicate = {}, set(), set(), set()
    for index, first in enumerate(lines):
        digits = first[:6].lstrip("* ").strip()
        if not digits.isdigit() or first[24:27] != "HKD":
            continue
        code = "HK" + digits.zfill(5)
        if code not in symbols:
            continue
        if code in seen:
            duplicate.add(code)
            continue
        seen.add(code)
        if first[27:].strip() == "TRADING SUSPENDED":
            suspended.add(code)
            continue
        if index + 1 >= len(lines):
            continue
        second = lines[index + 1]
        top, bottom = first[27:].split(), second[27:].split()
        if second[:27].strip() or len(top) != 4 or len(bottom) != 4:
            continue
        previous, close, volume, amount = top[0], bottom[0], top[3], bottom[3]
        if not all(re.fullmatch(r"\d+(?:\.\d+)?", v) for v in (previous, close)):
            continue
        if not all(re.fullmatch(r"(?:\d+|\d{1,3}(?:,\d{3})+)", v) for v in (volume, amount)):
            continue
        if float(previous) <= 0 or float(close) <= 0:
            continue
        rows[code] = dict(code=code, name=first[7:24].strip(), price=float(close),
                          change_pct=(float(close) / float(previous) - 1) * 100,
                          volume=int(volume.replace(",", "")), amount=int(amount.replace(",", "")),
                          is_connect=True, source=SOURCE, trade_date=trade_date.isoformat())
    for code in duplicate:
        rows.pop(code, None)
    return {"rows": rows, "suspended": sorted(suspended - duplicate)}


def load_daily_quotes(trade_date: date, symbols: set[str]) -> dict:
    url = f"https://www.hkex.com.hk/eng/stat/smstat/dayquot/d{trade_date:%y%m%d}e.htm"
    with requests.get(url, headers={"Cache-Control": "no-cache"}, stream=True, timeout=(8, 12)) as response:
        record_isolated_phase(f"official_http_status_{response.status_code}")
        response.raise_for_status()
        data = bytearray()
        for chunk in response.iter_content(65536):
            offset = max(0, len(data) - len(END))
            data.extend(chunk)
            if len(data) > MAX_QUOTATION_BYTES:
                raise ValueError("HKEX quotation section exceeds limit")
            if data.find(END, offset) >= 0:
                break
    return parse_daily_quotes(bytes(data), trade_date, symbols)
