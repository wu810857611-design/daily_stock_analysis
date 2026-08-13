#!/usr/bin/env python3
"""Read-only Longbridge HK quote preflight; never sends notifications or orders."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.generate_longbridge_oauth_token import (
    _has_hk_realtime_package,
    _package_rows,
    _provider_field,
)
from scripts.intraday_session import SessionError, _noninteractive_longbridge_context


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_SYMBOLS = ("700.HK", "9988.HK")


def _normalise_symbols(raw: str) -> list[str]:
    symbols = []
    for value in re.split(r"[\s,]+", str(raw or "").strip()):
        if not value:
            continue
        symbol = value.upper()
        if not re.fullmatch(r"\d{1,5}\.HK", symbol):
            raise ValueError(f"invalid HK symbol: {value}")
        ticker = symbol[:-3].lstrip("0") or "0"
        normalised = f"{ticker}.HK"
        if normalised not in symbols:
            symbols.append(normalised)
    if not symbols:
        raise ValueError("at least one HK symbol is required")
    return symbols


def _parse_provider_timestamp(raw: Any) -> Optional[datetime]:
    if isinstance(raw, datetime):
        parsed = raw
    else:
        try:
            number = float(raw)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(number):
            return None
        if number > 10_000_000_000:
            number /= 1000.0
        try:
            parsed = datetime.fromtimestamp(number, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SHANGHAI_TZ)
    return parsed.astimezone(SHANGHAI_TZ)


def verify_context(
    context: Any,
    *,
    symbols: Sequence[str],
    now: datetime,
    freshness_seconds: float,
    require_fresh: bool = True,
) -> dict[str, Any]:
    package_rows = _package_rows(context)
    has_realtime_package = _has_hk_realtime_package(package_rows)
    try:
        quote_level = str(context.quote_level() or "unknown")
    except Exception:
        quote_level = "unavailable"

    records = context.quote(list(symbols)) or ()
    by_symbol = {str(_provider_field(record, "symbol") or "").upper(): record for record in records}
    quote_rows: list[dict[str, Any]] = []
    issues: list[str] = []
    if not has_realtime_package:
        issues.append("hk_realtime_package_missing")

    observed_at = now.astimezone(SHANGHAI_TZ)
    for symbol in symbols:
        record = by_symbol.get(symbol.upper())
        price = _provider_field(record, "last_done") if record is not None else None
        timestamp = _parse_provider_timestamp(_provider_field(record, "timestamp") if record is not None else None)
        age_seconds = (observed_at - timestamp).total_seconds() if timestamp is not None else None
        fresh = bool(price not in (None, "") and age_seconds is not None and -30.0 <= age_seconds <= freshness_seconds)
        if record is None:
            issues.append(f"quote_missing:{symbol}")
        elif price in (None, ""):
            issues.append(f"price_missing:{symbol}")
        if timestamp is None:
            issues.append(f"provider_timestamp_missing:{symbol}")
        elif require_fresh and not fresh:
            issues.append(f"provider_timestamp_stale:{symbol}")
        quote_rows.append(
            {
                "symbol": symbol,
                "price": str(price) if price not in (None, "") else None,
                "provider_timestamp": timestamp.isoformat() if timestamp else None,
                "age_seconds": round(max(0.0, age_seconds), 1) if age_seconds is not None else None,
                "fresh": fresh,
            }
        )

    package_keys = [row.get("key") or "unknown" for row in package_rows]
    return {
        "passed": not issues,
        "observed_at": observed_at.isoformat(),
        "quote_level": quote_level,
        "mode": "live" if require_fresh else "permission",
        "hk_realtime_package": has_realtime_package,
        "package_keys": package_keys,
        "quotes": quote_rows,
        "issues": issues,
    }


def render_markdown(result: Mapping[str, Any]) -> str:
    verdict = "✅ 通过" if result.get("passed") else "❌ 未通过"
    package_keys = result.get("package_keys") or []
    lines = [
        "## Longbridge 港股实时行情只读预检",
        "",
        f"- 结论：{verdict}",
        f"- 行情等级：{result.get('quote_level') or 'unknown'}",
        f"- 检查模式：{result.get('mode') or 'live'}",
        (
            "- 港股实时包："
            + ("已识别" if result.get("hk_realtime_package") else "未识别")
            + f"（{', '.join(str(item) for item in package_keys) or '无'}）"
        ),
        f"- 检查时间：{result.get('observed_at') or 'unknown'}",
        "",
        "| 标的 | 提供方时间戳 | 年龄（秒） | 新鲜 |",
        "| --- | --- | ---: | --- |",
    ]
    for quote in result.get("quotes") or []:
        lines.append(
            "| {symbol} | {timestamp} | {age} | {fresh} |".format(
                symbol=quote.get("symbol") or "-",
                timestamp=quote.get("provider_timestamp") or "缺失",
                age=(quote.get("age_seconds") if quote.get("age_seconds") is not None else "-"),
                fresh="是" if quote.get("fresh") else "否",
            )
        )
    issues = result.get("issues") or []
    lines.extend(
        [
            "",
            f"- 未通过项：{'、'.join(str(item) for item in issues) or '无'}",
            "",
            "> 本预检只读取行情和权限，不加载 PushPlus，不连接交易接口，也不下单。",
            "",
        ]
    )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--symbols",
        default=",".join(DEFAULT_SYMBOLS),
        help="Comma/space separated liquid HK symbols, default: 700.HK,9988.HK",
    )
    parser.add_argument("--freshness-seconds", type=float, default=90.0)
    parser.add_argument(
        "--permission-only",
        action="store_true",
        help=(
            "Require the HK real-time package and readable quotes, but report "
            "rather than fail on timestamp age outside an active HK session."
        ),
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        default=None,
        help="Optional path for a non-secret JSON result.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.freshness_seconds <= 0:
        print("Longbridge 预检失败：freshness-seconds 必须大于零")
        return 2
    try:
        symbols = _normalise_symbols(args.symbols)
        context = _noninteractive_longbridge_context()
        result = verify_context(
            context,
            symbols=symbols,
            now=datetime.now(SHANGHAI_TZ),
            freshness_seconds=args.freshness_seconds,
            require_fresh=not args.permission_only,
        )
    except (SessionError, ValueError) as exc:
        print(f"Longbridge 预检失败：{exc}")
        return 2
    except Exception as exc:
        print(f"Longbridge 预检失败：{type(exc).__name__}")
        return 2

    markdown = render_markdown(result)
    print(markdown)
    summary_path = os.getenv("GITHUB_STEP_SUMMARY", "").strip()
    if summary_path:
        with Path(summary_path).open("a", encoding="utf-8") as handle:
            handle.write(markdown)
    if args.json_output is not None:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
