"""Current southbound stock eligibility from the Shanghai and Shenzhen exchanges."""
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from io import BytesIO
import re

from openpyxl import load_workbook
import requests


SOURCE = "sse.szse.eligible_stocks"
SZ_PARAMS = {"CATALOGID": "SGT_GGTBDQD", "TABKEY": "tab1"}


def _fetch(url, params, referer, *, binary=False):
    response = requests.get(url, params=params, headers={"Referer": referer}, timeout=(8, 15))
    response.raise_for_status()
    return response.content if binary else response.json()


def parse_membership(shanghai, shenzhen, workbook: bytes, trade_date: date) -> set[str]:
    """Accept a complete same-day roster; Shenzhen's export lacks security types."""
    day = trade_date.isoformat()
    rows = shanghai["result"]
    page = shanghai["pageHelp"]
    if not rows or page["pageCount"] != 1 or page["pageNo"] != 1 or page["total"] != len(rows):
        raise ValueError("incomplete Shanghai eligible list")
    kinds = {}
    for row in rows:
        code = row["SECURITY_CODE"]
        kind = row["SECURITY_TYPE"]
        if (row["UPDATE_DATE"] != day or row["TRADE_FLAG"] != "1" or
                not re.fullmatch(r"[0-9]{5}", code) or code in kinds or kind not in {"股票", "ETF"}):
            raise ValueError("unverified Shanghai eligible security")
        kinds[code] = kind

    table, = (item for item in shenzhen if item["metadata"]["tabkey"] == "tab1")
    meta = table["metadata"]
    if meta["catalogid"] != SZ_PARAMS["CATALOGID"] or meta["subname"] != day:
        raise ValueError("Shenzhen eligible list date or catalog mismatch")
    book = load_workbook(BytesIO(workbook), read_only=True)
    try:
        sheet = book[meta["name"]]
        # The published XLSX sometimes declares its used range as A1 only.
        sheet.reset_dimensions()
        values = iter(sheet.values)
        if next(values)[:3] != ("证券代码", "中文简称", "英文简称"):
            raise ValueError("Shenzhen eligible list schema mismatch")
        codes = [row[0] for row in values]
    finally:
        book.close()
    first_page = [row["zqdm"] for row in table["data"]]
    if (not first_page or len(codes) != meta["recordcount"] or len(codes) != len(set(codes))
            or codes[:len(first_page)] != first_page):
        raise ValueError("incomplete Shenzhen eligible list")
    if not set(codes).issubset(kinds):
        raise ValueError("missing security type for Shenzhen-only constituent")
    stocks = {"HK" + code for code, kind in kinds.items() if kind == "股票"}
    if not stocks:
        raise ValueError("empty eligible stock list")
    return stocks


def load_membership(trade_date: date) -> set[str]:
    sse = "https://www.sse.com.cn/services/hkexsc/disclo/eligible/"
    szse = "https://www.szse.cn/szhk/hkbussiness/underlylist/"
    endpoint = "https://www.szse.cn/api/report/ShowReport"
    with ThreadPoolExecutor(max_workers=3) as pool:
        shanghai = pool.submit(_fetch, "https://query.sse.com.cn/commonQuery.do", {
            "sqlId": "COMMON_SSE_JYFW_HGT_XXPL_BDZQQD_L", "isPagination": "true",
            "pageHelp.pageSize": 1000, "pageHelp.pageNo": 1}, sse)
        shenzhen = pool.submit(_fetch, endpoint + "/data", {**SZ_PARAMS, "SHOWTYPE": "JSON",
                                                            "PAGENO": 1}, szse)
        workbook = pool.submit(_fetch, endpoint, {**SZ_PARAMS, "SHOWTYPE": "xlsx"}, szse, binary=True)
        return parse_membership(shanghai.result(), shenzhen.result(), workbook.result(), trade_date)
