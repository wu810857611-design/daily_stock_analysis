#!/usr/bin/env python3
"""Restore only a proven-success market-scan artifact for intraday startup."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.market_scan_artifact_trust import (  # noqa: E402
    SHANGHAI_TZ,
    atomic_write,
    read_market_scan_latest_from_artifact,
    validate_startup_market_scan,
)
from scripts.market_scan_watchdog import GitHubActionsClient  # noqa: E402


def _run_created_at(run: Mapping[str, Any]) -> str:
    return str(run.get("created_at") or "")


def restore_latest_trusted_market_scan(
    *,
    client: Any,
    workflow: str,
    ref: str,
    session: str,
    observed_at: datetime,
    target_path: Path,
) -> dict[str, Any]:
    """Restore newest valid payload produced by a completed successful scan."""

    rejected: list[dict[str, Any]] = []
    try:
        runs = list(client.recent_runs(workflow, ref))
    except Exception as exc:  # noqa: BLE001 - startup restore is fail-closed/degraded.
        target_path.unlink(missing_ok=True)
        return {
            "schema_version": 1,
            "status": "degraded",
            "trusted": False,
            "reason": f"market_scan_runs_api_error:{type(exc).__name__}:{exc}",
            "session": session,
            "observed_at": observed_at.astimezone(SHANGHAI_TZ).isoformat(timespec="seconds"),
            "main_intraday_health_affected": False,
            "candidate_plan_path": str(target_path),
            "rejected": [],
        }

    successful = [
        run
        for run in sorted(runs, key=_run_created_at, reverse=True)
        if str(run.get("status") or "") == "completed"
        and str(run.get("conclusion") or "") == "success"
        and (
            not str(run.get("head_branch") or "")
            or str(run.get("head_branch") or "") == ref
        )
    ]

    for run in successful:
        run_id = int(run.get("id") or 0)
        if run_id <= 0:
            continue
        try:
            artifacts = list(client.artifacts_for_run(run_id))
        except Exception as exc:  # noqa: BLE001
            rejected.append({
                "run_id": run_id,
                "reason": f"artifact_api_error:{type(exc).__name__}:{exc}",
            })
            continue

        state_artifacts = [
            item
            for item in sorted(
                artifacts,
                key=lambda value: str(value.get("created_at") or ""),
                reverse=True,
            )
            if item.get("name") == "market-scan-state"
            and not item.get("expired")
        ]
        if not state_artifacts:
            rejected.append({"run_id": run_id, "reason": "market_scan_state_missing"})
            continue

        for artifact in state_artifacts:
            artifact_id = int(artifact.get("id") or 0)
            try:
                archive = client.download_artifact(artifact_id)
                raw_latest = read_market_scan_latest_from_artifact(archive)
                _payload, generated_at, fingerprint = validate_startup_market_scan(
                    raw_latest,
                    session=session,
                    observed_at=observed_at,
                )
            except Exception as exc:  # noqa: BLE001
                rejected.append({
                    "run_id": run_id,
                    "artifact_id": artifact_id,
                    "reason": f"artifact_invalid:{type(exc).__name__}:{exc}",
                })
                continue

            atomic_write(target_path, raw_latest)
            return {
                "schema_version": 1,
                "status": "trusted",
                "trusted": True,
                "reason": "completed_success_artifact_validated",
                "session": session,
                "observed_at": observed_at.astimezone(SHANGHAI_TZ).isoformat(timespec="seconds"),
                "run_id": run_id,
                "artifact_id": artifact_id,
                "generated_at": generated_at.isoformat(timespec="seconds"),
                "content_fingerprint": fingerprint,
                "candidate_plan_path": str(target_path),
                "main_intraday_health_affected": False,
                "rejected": rejected[-10:],
            }

    # A cache file has no trustworthy run-conclusion provenance on its own.
    # Remove it unless this invocation re-proves it through a successful run.
    target_path.unlink(missing_ok=True)
    return {
        "schema_version": 1,
        "status": "degraded",
        "trusted": False,
        "reason": "no_recent_completed_success_trusted_artifact",
        "session": session,
        "observed_at": observed_at.astimezone(SHANGHAI_TZ).isoformat(timespec="seconds"),
        "candidate_plan_path": str(target_path),
        "main_intraday_health_affected": False,
        "rejected": rejected[-10:],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session", choices=("morning", "afternoon"), required=True)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--ref", required=True)
    parser.add_argument("--workflow", default="02-market-scan.yml")
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--observed-at", default="")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.observed_at:
        observed_at = datetime.fromisoformat(args.observed_at.replace("Z", "+00:00"))
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=SHANGHAI_TZ)
        observed_at = observed_at.astimezone(SHANGHAI_TZ)
    else:
        observed_at = datetime.now(SHANGHAI_TZ)

    token = str(os.getenv("GH_TOKEN") or os.getenv("GITHUB_TOKEN") or "").strip()
    if token:
        client = GitHubActionsClient(repo=args.repo, token=token)
        report = restore_latest_trusted_market_scan(
            client=client,
            workflow=args.workflow,
            ref=args.ref,
            session=args.session,
            observed_at=observed_at,
            target_path=args.target,
        )
    else:
        args.target.unlink(missing_ok=True)
        report = {
            "schema_version": 1,
            "status": "degraded",
            "trusted": False,
            "reason": "github_token_missing",
            "session": args.session,
            "observed_at": observed_at.isoformat(timespec="seconds"),
            "candidate_plan_path": str(args.target),
            "main_intraday_health_affected": False,
            "rejected": [],
        }

    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False))
    # Missing/untrusted candidates are a degraded buy subchain, not a reason to
    # kill the healthy primary intraday monitor.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
