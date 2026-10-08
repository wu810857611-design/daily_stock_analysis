#!/usr/bin/env python3
"""Resolve and audit the close workflow's date before running any analysis."""
import argparse
from datetime import datetime
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.services.close_analysis_context import SHANGHAI, resolve_close_context
from scripts.account_watchlists import priority_analysis_pools
from scripts.normalize_stock_list import normalize_stock_list


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", required=True)
    parser.add_argument("--schedule", default="")
    parser.add_argument("--run-metadata", required=True)
    parser.add_argument("--budget-minutes", type=int, required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    try:
        metadata = json.loads(Path(args.run_metadata).read_text(encoding="utf-8"))
        context = resolve_close_context(
            event=args.event, schedule=args.schedule, created_at=metadata["created_at"],
            now=datetime.now(SHANGHAI), budget_minutes=args.budget_minutes,
        )
        pools = priority_analysis_pools()
        primary = list(pools["P0_PRIMARY"])
        active = set()
        for market in context["open_markets"]:
            active.update(normalize_stock_list(",".join(primary), market=market))
        context["active_primary"] = [symbol for symbol in primary if symbol in active]
        context["closed_primary"] = [symbol for symbol in primary if symbol not in active]
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(context, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(context, ensure_ascii=False))
        return 0
    except (ValueError, KeyError, OSError, ImportError) as exc:
        print(f"close-run context error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
