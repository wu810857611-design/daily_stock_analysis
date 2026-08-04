#!/usr/bin/env python3
"""Validate both market-scan reviewers without scanning or mutating state.

This is a deliberately narrow production smoke test for the two structured
JSON model calls.  It does not fetch quotes, write market-scan state, send a
notification, or place an order.  Only non-sensitive pass/fail metadata is
written to the optional output file.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.market_scan import build_litellm_reviewer  # noqa: E402


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
SMOKE_CODE = "600000"
SMOKE_PAYLOAD = (
    {
        "code": SMOKE_CODE,
        "name": "结构化输出验收占位样本（非投资标的）",
        "market": "A",
        "smoke_test": True,
        "technical_score": 0.0,
        "data_quality": 0.0,
        "data_availability": {
            "quote": "unavailable",
            "ohlcv": "unavailable",
            "intraday": "unavailable",
            "level2": "unavailable",
            "fundamentals": "unavailable",
            "announcements": "unavailable",
            "fund_flow": "unavailable",
        },
        "plan": {},
        "instruction": (
            "仅验证 JSON 结构，不作投资判断；数据不足时应输出 watch 或 reject。"
        ),
    },
)


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    finally:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass


def _configured_model(label: str) -> str:
    names = (
        ("MARKET_SCAN_QWEN_MODEL", "LLM_DASHSCOPE_MODELS")
        if label == "qwen"
        else ("MARKET_SCAN_DEEPSEEK_MODEL", "LLM_DEEPSEEK_MODELS")
    )
    for name in names:
        value = str(os.getenv(name) or "").strip()
        if value:
            return next((item.strip() for item in value.split(",") if item.strip()), "")
    return ""


def _validate_review_payload(raw: Any) -> None:
    if not isinstance(raw, Mapping):
        raise ValueError("response_not_object")
    reviews = raw.get("reviews")
    if not isinstance(reviews, list) or len(reviews) != 1:
        raise ValueError("review_count_not_one")
    item = reviews[0]
    if not isinstance(item, Mapping):
        raise ValueError("review_not_object")
    if str(item.get("code") or "").strip().upper() != SMOKE_CODE:
        raise ValueError("code_coverage_mismatch")
    if str(item.get("verdict") or "").strip().lower() not in {
        "pass",
        "watch",
        "reject",
    }:
        raise ValueError("verdict_invalid")
    confidence = item.get("confidence")
    if (
        isinstance(confidence, bool)
        or not isinstance(confidence, (int, float))
        or not 0 <= float(confidence) <= 1
    ):
        raise ValueError("confidence_invalid")
    if not isinstance(item.get("hard_risk"), bool):
        raise ValueError("hard_risk_invalid")
    for field in ("facts", "inferences", "risks", "invalidators"):
        value = item.get(field)
        if not isinstance(value, list) or any(
            not isinstance(text, str) for text in value
        ):
            raise ValueError(f"{field}_invalid")
    for field in ("thesis", "view"):
        if not isinstance(item.get(field), str):
            raise ValueError(f"{field}_invalid")


def run_smoke() -> Dict[str, Any]:
    checks: Dict[str, Dict[str, Any]] = {}
    failures = []
    for label in ("qwen", "deepseek"):
        try:
            reviewer = build_litellm_reviewer(label)
            if reviewer is None:
                raise RuntimeError("reviewer_not_configured")
            raw = reviewer(SMOKE_PAYLOAD)
            _validate_review_payload(raw)
        except Exception as exc:  # noqa: BLE001 - report only safe error type.
            error_type = type(exc).__name__
            failures.append(f"{label}:{error_type}")
            checks[label] = {
                "provider": label,
                "model": _configured_model(label),
                "ok": False,
                "error_type": error_type,
            }
        else:
            checks[label] = {
                "provider": label,
                "model": _configured_model(label),
                "ok": True,
                "review_count": 1,
                "code_coverage": True,
                "error_type": "",
            }

    result = {
        "schema_version": 1,
        "generated_at": datetime.now(SHANGHAI_TZ).isoformat(),
        "simulation_only": True,
        "fetched_market_data": False,
        "state_mutated": False,
        "notification_sent": False,
        "auto_order_enabled": False,
        "checks": checks,
        "success": not failures,
        "failures": failures,
    }
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=None)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = run_smoke()
    except Exception as exc:  # noqa: BLE001 - concise failure for CI.
        print(f"双模型结构化输出验收失败: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    if args.output is not None:
        _atomic_write_json(args.output, result)
    if not result["success"]:
        print(
            "双模型结构化输出验收失败: " + ";".join(result["failures"]),
            file=sys.stderr,
        )
        return 2
    print(
        "双模型结构化输出验收通过: "
        + ", ".join(f"{label}=ok" for label in result["checks"])
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
