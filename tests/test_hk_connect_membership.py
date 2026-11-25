from datetime import date
from io import BytesIO

from openpyxl import Workbook
import pytest

from src.services.hk_connect_membership import parse_membership


@pytest.mark.parametrize("fault", ["", "partial", "older", "future", "sz_only", "truncated", "duplicate"])
def test_official_eligible_stock_roster(fault):
    day = date(2026, 11, 25)
    codes = ["00123", "02800"]
    rows = [{"SECURITY_CODE": code, "SECURITY_TYPE": kind, "UPDATE_DATE": day.isoformat(),
             "TRADE_FLAG": "1"} for code, kind in zip(codes, ["股票", "ETF"])]
    sh = {"result": rows, "pageHelp": {"pageCount": 1, "pageNo": 1, "total": len(rows)}}
    sz = [{"data": [{"zqdm": c} for c in codes], "metadata": {"name": "港股通标的证券名单",
           "tabkey": "tab1", "catalogid": "SGT_GGTBDQD", "subname": day.isoformat(),
           "recordcount": len(codes)}}]
    if fault == "partial":
        sh["pageHelp"]["pageCount"] = 2
    if fault == "older":
        rows[0]["UPDATE_DATE"] = "2026-11-24"
    if fault == "future":
        sz[0]["metadata"]["subname"] = "2026-11-26"
    if fault == "sz_only":
        codes[0] = sz[0]["data"][0]["zqdm"] = "00999"
    if fault == "truncated":
        codes.pop()
    if fault == "duplicate":
        codes[1] = codes[0]
    book = Workbook()
    book.active.title = "港股通标的证券名单"
    book.active.append(["证券代码", "中文简称", "英文简称"])
    for code in codes:
        book.active.append([code, "中文", "EN"])
    stream = BytesIO()
    book.save(stream)
    if fault:
        with pytest.raises(ValueError):
            parse_membership(sh, sz, stream.getvalue(), day)
    else:
        assert parse_membership(sh, sz, stream.getvalue(), day) == {"HK00123"}
