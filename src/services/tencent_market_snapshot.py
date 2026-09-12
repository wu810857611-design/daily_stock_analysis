# -*- coding: utf-8 -*-
"""Fresh Tencent quote fallback for a previously verified symbol universe."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence
from urllib import request
from zoneinfo import ZoneInfo


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
PRIMARY_ENDPOINT = "https://qt.gtimg.cn/q="
ALTERNATE_ENDPOINT = "https://web.sqt.gtimg.cn/q="
_RECORD = re.compile(r'v_([A-Za-z0-9]+)="([^"]*)"')


def _number(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed


def _symbol_to_provider(symbol: str) -> str:
    token = str(symbol or "").strip().upper()
    if token.startswith("HK") and token[2:].isdigit():
        return f"hk{token[2:].zfill(5)}"
    raise ValueError(f"unsupported Tencent HK symbol: {symbol}")


def _symbol_from_provider(symbol: str) -> str:
    token = str(symbol or "").strip().lower()
    if token.startswith("hk") and token[2:].isdigit():
        return f"HK{token[2:].zfill(5)}"
    return ""


def _provider_time(value: Any) -> Optional[datetime]:
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) < 14:
        return None
    try:
        return datetime.strptime(digits[:14], "%Y%m%d%H%M%S").replace(
            tzinfo=SHANGHAI_TZ
        )
    except ValueError:
        return None


def _parse_payload(
    payload: str,
    requested: Sequence[str],
    *,
    now: datetime,
    freshness_seconds: float,
) -> Dict[str, Mapping[str, Any]]:
    expected = set(requested)
    parsed: Dict[str, Mapping[str, Any]] = {}
    for provider_symbol, raw_fields in _RECORD.findall(payload or ""):
        symbol = _symbol_from_provider(provider_symbol)
        if not symbol or symbol not in expected:
            continue
        fields = raw_fields.split("~")
        price = _number(fields[3] if len(fields) > 3 else None)
        change_pct = _number(fields[32] if len(fields) > 32 else None)
        volume = _number(fields[6] if len(fields) > 6 else None)
        # Tencent field 37 is turnover in ten-thousand currency units.
        turnover = _number(fields[37] if len(fields) > 37 else None)
        timestamp = _provider_time(fields[30] if len(fields) > 30 else None)
        if timestamp is None or price is None or price <= 0:
            continue
        age_seconds = (now.astimezone(SHANGHAI_TZ) - timestamp).total_seconds()
        if not -30.0 <= age_seconds <= freshness_seconds:
            continue
        if volume is None or volume <= 0 or turnover is None or turnover <= 0:
            continue
        parsed[symbol] = {
            "code": symbol,
            "name": str(fields[1] if len(fields) > 1 else "").strip(),
            "price": price,
            "change_pct": change_pct,
            "volume": volume,
            "amount": turnover * 10_000.0,
            "provider_timestamp": timestamp.isoformat(timespec="seconds"),
            "is_connect": True,
        }
    return parsed


def load_fresh_hk_membership_snapshot(
    symbols: Sequence[str],
    *,
    now: Optional[datetime] = None,
    freshness_seconds: float = 90.0,
    timeout_seconds: float = 8.0,
    chunk_size: int = 50,
    opener: Callable[..., Any] = request.urlopen,
    endpoints: Sequence[str] = (PRIMARY_ENDPOINT, ALTERNATE_ENDPOINT),
) -> Mapping[str, Any]:
    """Fetch fresh quotes for a cached, independently verified HK Connect set.

    Old or timestamp-less quotes are omitted.  Membership provenance remains
    owned by the caller and this function never expands the supplied universe.
    """

    fetched_at = now or datetime.now(SHANGHAI_TZ)
    canonical = sorted(
        {
            f"HK{str(symbol).strip().upper().removeprefix('HK').zfill(5)}"
            for symbol in symbols
            if str(symbol or "").strip().upper().removeprefix("HK").isdigit()
        }
    )
    if not canonical:
        raise ValueError("verified HK Connect membership is empty")

    records: Dict[str, Mapping[str, Any]] = {}
    provider_errors: List[str] = []
    route_stats: List[Dict[str, Any]] = []
    for offset in range(0, len(canonical), max(1, int(chunk_size))):
        unresolved = canonical[offset : offset + max(1, int(chunk_size))]
        for route_index, endpoint in enumerate(endpoints):
            if not unresolved:
                break
            route = "primary" if route_index == 0 else "alternate"
            query = ",".join(_symbol_to_provider(symbol) for symbol in unresolved)
            outgoing = request.Request(
                f"{endpoint}{query}",
                headers={
                    "Referer": "https://finance.qq.com/",
                    "User-Agent": "daily-stock-analysis/market-scan",
                },
                method="GET",
            )
            try:
                with opener(outgoing, timeout=timeout_seconds) as response:
                    payload = response.read().decode("gbk", errors="replace")
                parsed = _parse_payload(
                    payload,
                    unresolved,
                    now=fetched_at,
                    freshness_seconds=freshness_seconds,
                )
            except Exception as exc:  # noqa: BLE001 - bounded provider fallback.
                provider_errors.append(f"{route}:{type(exc).__name__}:{exc}")
                route_stats.append(
                    {"route": route, "requested": len(unresolved), "fresh": 0, "failed": True}
                )
                continue
            records.update(parsed)
            route_stats.append(
                {
                    "route": route,
                    "requested": len(unresolved),
                    "fresh": len(parsed),
                    "failed": False,
                }
            )
            unresolved = [symbol for symbol in unresolved if symbol not in parsed]

    if not records:
        detail = "; ".join(provider_errors) or "no fresh timestamped quotes"
        raise RuntimeError(f"Tencent HK membership quote fallback unavailable: {detail}")
    provider_times = sorted(str(item["provider_timestamp"]) for item in records.values())
    return {
        "records": list(records.values()),
        "as_of": provider_times[-1],
        "fetched_at": fetched_at.isoformat(timespec="seconds"),
        "source": "tencent.hk_membership_batch",
        "provider_errors": provider_errors,
        "route_stats": route_stats,
        "requested_count": len(canonical),
        "fresh_count": len(records),
        "is_connect_universe": True,
    }
