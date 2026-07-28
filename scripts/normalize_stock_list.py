#!/usr/bin/env python3
"""Normalize and filter the user-facing stock list for GitHub Actions.

The upstream data layer accepts ``HK00981`` and ``00981.HK`` but not the
commonly pasted ``HK.00981`` form.  This small boundary normalizer keeps a bad
repository variable from silently routing Hong Kong symbols through A-share
providers.
"""

from __future__ import annotations

import argparse
import re
from typing import Iterable, List, Optional, Sequence


_SEPARATOR_RE = re.compile(r"[\s,;，、；]+")
_HK_PREFIX_RE = re.compile(r"^HK[.\-_]?(\d{4,5})$", re.IGNORECASE)
_HK_SUFFIX_RE = re.compile(r"^(\d{4,5})[.\-_]?HK$", re.IGNORECASE)


def canonical_symbol(raw: str) -> str:
    """Return the canonical symbol used by this repository."""

    value = str(raw or "").strip().upper()
    if not value:
        return ""
    match = _HK_PREFIX_RE.fullmatch(value) or _HK_SUFFIX_RE.fullmatch(value)
    if match:
        return f"HK{match.group(1).zfill(5)}"
    return value


def symbol_market(symbol: str) -> str:
    """Classify a normalized symbol into the supported workflow batches."""

    value = canonical_symbol(symbol)
    if value.startswith("HK") and value[2:].isdigit():
        return "hk"
    if value.isdigit() and len(value) == 6:
        return "cn"
    return "other"


def normalize_stock_list(value: str, market: Optional[str] = None) -> List[str]:
    """Normalize, de-duplicate, and optionally filter a stock list."""

    normalized: List[str] = []
    seen = set()
    for raw in _SEPARATOR_RE.split(value or ""):
        symbol = canonical_symbol(raw)
        if not symbol or symbol in seen:
            continue
        if market and symbol_market(symbol) != market:
            continue
        seen.add(symbol)
        normalized.append(symbol)
    return normalized


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stocks", help="comma/space separated stock list")
    parser.add_argument("--market", choices=("cn", "hk", "other"))
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    print(",".join(normalize_stock_list(args.stocks, market=args.market)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
