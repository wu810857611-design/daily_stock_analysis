#!/usr/bin/env python3
"""Read-only close-reference coverage check; never extends a plan's lifetime."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.account_watchlists import PRIMARY_SYMBOLS  # noqa: E402
from scripts.intraday_session import load_reference_levels_batch  # noqa: E402


def audit(database_path: Path) -> dict:
    details: dict = {}
    levels = load_reference_levels_batch(database_path, PRIMARY_SYMBOLS, diagnostics=details)
    covered = sum(level.stop_loss is not None or level.target_price is not None
                  for level in levels.values())
    return {"database_available": database_path.exists(), "covered_symbols": covered,
            "total_symbols": len(PRIMARY_SYMBOLS), "by_symbol": details,
            "needs_refresh": covered < len(PRIMARY_SYMBOLS)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.db)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Reference coverage {result['covered_symbols']}/{result['total_symbols']}; "
          f"needs_refresh={result['needs_refresh']}")
    return 2 if result["needs_refresh"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
