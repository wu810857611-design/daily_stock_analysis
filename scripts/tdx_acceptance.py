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
    for i, (name, query) in enumerate([
        ("tdx_quotes", "中际旭创 300308.SZ 最新成交价、交易日期和行情时间"),
        ("wenda_report_query", "中际旭创 300308 最新一篇券商研报的标题、机构、发布日期、原文链接")], 3):
        tool = tools.get(name)
        if not tool:
            emit("MISSING_TOOL", {"expected":name,"available":list(tools)})
            continue
        schema = tool["inputSchema"]
        emit("SCHEMA", {name:schema})
        args = {}
        for field in schema.get("required", []):
            if field.lower() in ("query", "question", "input", "keyword", "keywords", "prompt"):
                args[field] = query
            elif field.lower() in ("code", "stock_code", "symbol"):
                args[field] = SYMBOL
        if not args or set(schema.get("required", [])) - args.keys():
            continue
        result = rpc("tools/call", {"name":name,"arguments":args}, i)
        for item in result.get("content", [])[:1]:
            s = item.get("text", "")
            if name == "tdx_quotes":
                emit("MCP_QUOTE",s[:1200])
            else:
                # Only citation metadata; avoid publishing licensed report body.
                for line in s.splitlines():
                    if re.search(r"标题|机构名称|研究机构|发布日期|报告日期|原文链接|https?://|^#",line) and len(line)<350:
                        emit("REPORT_REFERENCE",line[:300])
                        if "http" in line:
                            break
                emit("REPORT_RESPONSE",{"chars":len(s),"error":result.get("isError",False),"format":s[:38]})

if __name__ == "__main__":
    if not TOKEN:
        sys.exit("Missing credential")
    emit("REQUEST_AT_UTC", datetime.now(timezone.utc).isoformat())
    try:
        (test_snapshot if sys.argv[1] == "sdk" else test_mcp)()
    except Exception as exc:
        emit("PROBE_ERROR",{"class":type(exc).__name__,"status":getattr(exc,"code",None),"message":str(exc)[:180]})
        sys.exit(1)
