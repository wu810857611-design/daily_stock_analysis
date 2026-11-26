"""Read-only, single-symbol TDX connectivity probe; no production configuration."""
import configparser
import importlib.util
import json
import os
import re
import sys
import tempfile
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from time import sleep
from zoneinfo import ZoneInfo
from pathlib import Path
from urllib.request import build_opener, HTTPRedirectHandler, Request, urlopen

SYMBOL = "300308.SZ"
URL = "https://txmcp.tdx.com.cn:3001/txmcp"
TOKEN = os.environ["TDX_DATA_TOKEN"].strip()


def emit(label, value):
    data = json.dumps(value, ensure_ascii=False).replace(TOKEN, "[redacted]")
    print(label, data, flush=True)


def test_snapshot():
    assets = Path(importlib.util.find_spec("tdxaidata").origin).parent / "lib"
    with tempfile.TemporaryDirectory() as folder:
        lib = Path(folder) / "libTdxAiData.so"
        shutil.copyfile(assets / lib.name, lib)
        config = configparser.ConfigParser(interpolation=None)
        config.read(assets / "TdxAiData.ini", encoding="utf-8-sig")
        config["Token"]["token"] = TOKEN
        with lib.with_name("TdxAiData.ini").open("w") as handle:
            config.write(handle, space_around_delimiters=False)
        os.environ["TDX_AI_DATA_LIB"] = str(lib)
        from tdxaidata import tqs
        with ThreadPoolExecutor(max_workers=2) as pool:
            for sample in range(1, 3):
                started = datetime.now(timezone.utc)
                comparison = pool.submit(sina_quote)
                dates = tqs.get_more_info_batch(stock_list=[SYMBOL], field_list=["HqDate"])
                quotes = tqs.get_market_snapshot_batch(stock_list=[SYMBOL])
                received = datetime.now(timezone.utc)
                row = (quotes or {}).get(SYMBOL) or {}
                fields = {k: row[k] for k in (
                    "Now", "LastClose", "PreClose", "RefreshTime", "Time", "Date",
                    "Volume", "Vol", "TotalVol", "TotalVolume", "Amount", "TotalAmount",
                    "Total", "Open", "High", "Low",
                ) if k in row}
                fields["HqDate"] = ((dates or {}).get(SYMBOL) or {}).get("HqDate")
                age, ok = session_age(fields["HqDate"], fields.get("RefreshTime"), received)
                emit("TDX_LIVE_SAMPLE", {
                    "sample":sample, "security":SYMBOL, "started_utc":started.isoformat(),
                    "received_utc":received.isoformat(), "age_s":age, "fresh":ok,
                    "available_fields":sorted(row), "quote":fields,
                })
                try:
                    other = comparison.result()
                except Exception as exc:
                    emit("CROSSCHECK_ERROR", {"sample":sample,"type":type(exc).__name__})
                else:
                    emit("SINA_LIVE_SAMPLE", {"sample":sample, **other})
                if sample == 1:
                    sleep(15)


def session_age(day, stamp, received):
    try:
        exchange = datetime.strptime(str(day)+str(stamp).zfill(6), "%Y%m%d%H%M%S").replace(
            tzinfo=ZoneInfo("Asia/Shanghai")
        )
    except (ValueError, TypeError):
        return None, False
    local = received.astimezone(exchange.tzinfo)
    age = (received-exchange).total_seconds()
    in_session = any(left <= local.strftime("%H:%M:%S") < right
                     for left,right in [("09:30:00","11:30:00"),("13:00:00","15:00:00")])
    return round(age,3), local.date() == exchange.date() and in_session and 0 <= age <= 90


def sina_quote():
    # Use the live, unadjusted Shenzhen quote and original cumulative share volume.
    request = Request("https://hq.sinajs.cn/list=sz300308",headers={
        "Referer":"https://finance.sina.com.cn/","User-Agent":"Mozilla/5.0"
    })
    with urlopen(request, timeout=8) as response:
        name, body = response.read(4096).decode("gb18030").strip().split("=",1)
    received = datetime.now(timezone.utc)
    if name.strip() != "var hq_str_sz300308":
        raise ValueError("Sina symbol mismatch")
    data = body.split('"')[1].split(",")
    day, stamp = data[30].replace("-",""), data[31].replace(":","")
    age, ok = session_age(day, stamp, received)
    return {"received_utc":received.isoformat(),"trade_date":data[30],"quote_time":data[31],
            "fresh":ok,"age_s":age,"CNY":data[3],"previous_close":data[2],
            "high":data[4],"low":data[5],"volume_shares":data[8],"amount_cny":data[9]}


def test_time_gate():
    now = datetime(2026,11,26,2,46,37,tzinfo=timezone.utc)
    assert session_age("20261126", "104633", now)[1]
    assert not session_age("20261126", "104638", now)[1]
    assert session_age("20261126", "104507", now)[1]
    assert not session_age("20261126", "104506", now)[1]
    assert not session_age("20261125", "104637", now)[1]
    emit("TIME_GATE", {"normal":"pass","future":"rejected","age_90s":"pass",
                       "age_91s":"rejected","previous_day":"rejected"})


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


def test_mcp():
    http = build_opener(NoRedirect())
    headers = {"Authorization": "Bearer " + TOKEN, "Content-Type": "application/json",
               "Accept": "application/json, text/event-stream"}
    def rpc(method, params, number=None):
        payload = {"jsonrpc": "2.0", "method": method, "params": params}
        if number is not None:
            payload["id"] = number
        request = Request(URL, data=json.dumps(payload).encode(), headers=headers)
        with http.open(request, timeout=22) as response:
            if response.headers.get("Mcp-Session-Id"):
                headers["Mcp-Session-Id"] = response.headers["Mcp-Session-Id"]
            if number is None:
                return {}
            if "text/event-stream" in response.headers.get("Content-Type", ""):
                obj = next(json.loads(line[5:]) for line in response if line.startswith(b"data:") and line[5:].strip())
            else:
                obj = json.load(response)
            if "error" in obj:
                emit(method, obj["error"].get("code"))
                raise ValueError(method)
            return obj["result"]
    init = rpc("initialize", {"protocolVersion":"2025-03-26","capabilities":{},
                               "clientInfo":{"name":"read-only-verification","version":"1"}}, 1)
    headers["MCP-Protocol-Version"] = init["protocolVersion"]
    rpc("notifications/initialized", {})
    tools = {t["name"]: t for t in rpc("tools/list", {}, 2)["tools"]}
    def call(name,args,number):
        schema = tools[name]["inputSchema"]
        if not set(schema.get("required", [])) <= args.keys() <= schema["properties"].keys():
            raise ValueError("incompatible TDX tool schema")
        result = rpc("tools/call", {"name":name,"arguments":args}, number)
        if result.get("isError"):
            raise ValueError("TDX query failed")
        return result["content"][0]["text"]

    quote = call("tdx_quotes", {"code":"300308", "setcode":"0", "hasHQInfo":"1", "bspNum":"0"}, 3)
    details = json.loads(quote[quote.index("{"):])
    if details["BaseInfo"]["Code"] != "300308":
        raise ValueError("wrong security")
    hq = details["HQInfo"]
    emit("MCP_QUOTE", {"code":"300308", "received_utc":datetime.now(timezone.utc).isoformat(),
                       **{k:hq[k] for k in ("Now","HQDate","HQTime")}})
    for index, args in enumerate([
        {"name":"中际旭创","bdate":"20261108","edate":"20261108"},
        {"name":"中际旭创","bdate":"20261105","edate":"20261109","keywords":"光模块"},
        {"name":"中际旭创","bdate":"20261105","edate":"20261105"},
        {"query":"中际旭创|20261029|20261115|高速光模块持续放量|"},
    ],4):
        body = json.loads(call("wenda_report_query", args, index))
        table = body.get("data") or []
        emit("REPORT_MATCH", {"query":body.get("query"), "rows":max(0,len(table)-1)})
        if len(table)<2:
            continue
        columns = table[0]
        emit("REPORT_COLUMNS",columns)
        target = False
        for values in table[1:]:
            record = dict(zip(columns,values))
            meta = {k:record[k] for k in ("标题","时间","链接","来源","机构","证券公司","作者") if k in record}
            emit("REPORT_CITATION", meta)
            if any(x in str(meta.get("标题")) for x in ("中际旭创","300308","高速光模块持续放量","营收扣非环比高增","业绩符合预期，龙头优势凸显")):
                target = True
        if target:
            return
    raise ValueError("no matched company report title")

if __name__ == "__main__":
    if not TOKEN:
        sys.exit("Missing credential")
    test_time_gate()
    emit("REQUEST_AT_UTC", datetime.now(timezone.utc).isoformat())
    try:
        (test_snapshot if sys.argv[1] == "sdk" else test_mcp)()
    except Exception as exc:
        emit("PROBE_ERROR",{"class":type(exc).__name__,"status":getattr(exc,"code",None),"message":str(exc)[:180]})
        sys.exit(1)
