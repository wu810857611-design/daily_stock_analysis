"""Read-only, single-symbol TDX connectivity probe; no production configuration."""
import configparser
import importlib.util
import json
import os
import re
import sys
import tempfile
import shutil
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import build_opener, HTTPRedirectHandler, Request

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
        dates = tqs.get_more_info_batch(stock_list=[SYMBOL], field_list=["HqDate"])
        quotes = tqs.get_market_snapshot_batch(stock_list=[SYMBOL])
        row = (quotes or {}).get(SYMBOL) or {}
        fields = {k: row[k] for k in ("Now", "RefreshTime", "Time", "Date") if k in row}
        fields["HqDate"] = ((dates or {}).get(SYMBOL) or {}).get("HqDate")
        emit("SDK_QUOTE", fields)
        if not fields.get("Now") or not fields["HqDate"] or not (fields.get("Time") or fields.get("RefreshTime")):
            raise ValueError("no dated quote")


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
    queries = {
        "tdx_quotes": {"code":"300308", "setcode":"0", "hasHQInfo":"1", "bspNum":"0"},
        "wenda_report_query": {"query":"中际旭创 最新券商研报", "symbol":"300308", "name":"中际旭创"}
    }
    for i, (name, args) in enumerate(queries.items(), 3):
        schema = tools[name]["inputSchema"]
        assert set(schema.get("required", [])) <= args.keys() <= schema["properties"].keys()
        result = rpc("tools/call", {"name":name,"arguments":args}, i)
        content = result.get("content", [])
        emit("RESULT", {"tool":name, "error":result.get("isError",False), "blocks":len(content)})
        for block in content[:2]:
            body = block.get("text", "")
            if name == "tdx_quotes":
                emit("MCP_QUOTE", body[:1200])
            else:
                report_metadata(body)
        if result.get("isError"):
            raise ValueError("query failed")


def report_metadata(text):
    # Log bibliographic metadata only. The licensed body stays in runner memory.
    emit("REPORT_SHAPE", {"chars":len(text), "prefix":text[:90]})
    def visit(obj, depth=0):
        if depth > 9:
            return
        if isinstance(obj, dict):
            meta = {}
            for k,v in obj.items():
                if isinstance(v, (str,int)) and re.search(
                    r"title|机构|券商|标题|日期|作者|时间|公司名称|链接|来源|url|source|author|org|broker|rname|date|time|publish|link|reportid|infoCode",
                    k,re.I
                ) and len(str(v)) < 380:
                    meta[k] = v
            if meta:
                emit("REPORT_METADATA", meta)
            elif depth < 5:
                emit("REPORT_FIELDS", list(obj)[:35])
            for v in obj.values():
                if isinstance(v,(dict,list)) or isinstance(v,str) and v.lstrip().startswith(("{","[")):
                    visit(v, depth+1)
        elif isinstance(obj,list):
            for item in obj[:3]:
                visit(item,depth+1)
        elif isinstance(obj,str):
            try:
                visit(json.loads(obj),depth+1)
            except (ValueError, RecursionError):
                return
    try:
        visit(json.loads(text))
    except ValueError:
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        for line in lines[:45]:
            if 0<len(line)<420 and re.search(r"标题|机构|证券|发布日期|报告日期|原文|https?://|^#|^\|",line):
                emit("REPORT_METADATA",line[:240])

if __name__ == "__main__":
    if not TOKEN:
        sys.exit("Missing credential")
    emit("REQUEST_AT_UTC", datetime.now(timezone.utc).isoformat())
    try:
        (test_snapshot if sys.argv[1] == "sdk" else test_mcp)()
    except Exception as exc:
        emit("PROBE_ERROR",{"class":type(exc).__name__,"status":getattr(exc,"code",None),"message":str(exc)[:180]})
        sys.exit(1)
