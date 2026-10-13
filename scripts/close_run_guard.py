#!/usr/bin/env python3
"""Fail-closed admission before restoring or writing close production state."""
import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.intraday_slot_guard import atomic_write_json
from scripts.intraday_supervisor import GitHubActionsClient
from src.services.close_analysis_context import SHANGHAI, resolve_close_context
from src.services.close_scheduler import admit_close_run


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--ref", required=True)
    parser.add_argument("--event", required=True)
    parser.add_argument("--schedule", default="")
    parser.add_argument("--trade-date", default="")
    parser.add_argument("--run-metadata", type=Path, required=True)
    parser.add_argument("--budget-minutes", type=int, default=90)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--github-output", required=True)
    args = parser.parse_args(argv)
    result = {"schema_version": 1, "should_run": False, "status": "admission_failed"}
    exit_code = 0
    try:
        metadata = json.loads(args.run_metadata.read_text())
        context = resolve_close_context(event=args.event, schedule=args.schedule,
            created_at=metadata["created_at"], now=datetime.now(SHANGHAI),
            budget_minutes=args.budget_minutes, requested_date=args.trade_date)
        token = str(os.getenv("GH_TOKEN") or "").strip()
        if not token:
            raise ValueError("GH_TOKEN is required to confirm close ownership")
        client = GitHubActionsClient(repo=args.repo, token=token)
        result = admit_close_run(context=context, current=metadata,
                                 runs=client.recent_runs("00-daily-analysis.yml", args.ref))
        if result["status"] == "rerun_blocked":
            exit_code = 2
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
        exit_code = 2
    atomic_write_json(args.output, result)
    with Path(args.github_output).open("a") as output:
        output.write(f"should_run={str(result['should_run']).lower()}\n")
    print(json.dumps(result, ensure_ascii=False))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
