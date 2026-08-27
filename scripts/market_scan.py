#!/usr/bin/env python3
"""Run the simulation-only A-share and Hong Kong Stock Connect scanner."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.pushplus_notify import send_markdown  # noqa: E402
from src.services.market_scan_service import (  # noqa: E402
    MarketScanConfig,
    MarketScanService,
    default_a_snapshot_loader,
    default_history_loader,
    default_hk_all_snapshot_loader,
    default_hk_connect_snapshot_loader,
    render_market_scan_markdown,
)


DEFAULT_REPORT_PATH = Path("reports/market_scan.md")
DEFAULT_RESULT_PATH = Path("reports/market_scan.json")
DEFAULT_STATE_DIR = Path("data/market_scan")

Notifier = Callable[[str, str], Any]

MARKET_SCAN_REVIEW_SYSTEM_PROMPT = """
你是独立的股票风险复核员。本系统已经主动查询可获得的全市场基础行情与OHLCV；
若输入把基本面、行业、公告、资金、政策、盘口L1或Level-2标记为 unavailable，
必须明确数据不足，绝不补造新闻、资金流、主力意图、政策或价格。

即使用户没有提供分时图、K线、盘口或成交量，也必须优先使用系统主动取得且带
来源、授权状态和时间戳的数据，不得把“用户未上传截图”当成停止研究的理由。
数据获取优先级为：用户已合法授权且校验通过的Level-2深度盘口；新鲜L1报价与
逐笔/分时/成交量；OHLCV与技术指标；公司公告、财务、估值、资金、行业与政策。
若可靠Level-2不可用，应继续用这些彼此独立的数据源交叉验证，但必须降低盘口
相关推断的置信度，禁止用推算结果冒充Level-2。所有调仓判断以扣除手续费、税费
和滑点后的风险调整预期收益改善为目标，而不是以交易次数多少为目标。

严格区分：
1. facts：仅复述输入中有来源和时间的数据；
2. inferences：基于数据的可证伪推断，不得冒充事实；
3. view：审慎观点，需说明短线、波段、中线和长线的数据边界。

不得改写程序计算的观察买入区、止损和目标价。发现停牌、监管、财务造假、退市、
重大数据质量问题或流动性无法执行时 hard_risk=true。缺少可靠Level-2时，不得把
“主力抢筹、洗盘、诱多”写成事实。做T必须有分时、盘口、波动、胜率及扣费后正期望，
否则只能判 watch。模型不是fallback，意见将与另一个模型独立比对。

本任务的 verdict 不是立即买卖指令：若程序给出的完整买入区、止损、目标、数据质量
和扣费后风险回报可支持“进入盘中新鲜价格区复核”，且没有硬风险，可判 pass；
缺少 Level-2 本身不等于 reject，但必须降低相应推断置信度。pass 后程序最多生成
首笔 2.5% 模拟净值、仍需人工确认的条件建仓建议；当前价未进入买入区的标的仍不会
触发买入提醒。

输出严格 JSON，必须为每个输入代码且仅输出一条 review；`facts`、`inferences`、
`risks`、`invalidators` 各最多两条，`thesis` 和 `view` 各最多 120 个汉字：
{"reviews":[{"code":"...","verdict":"pass|watch|reject","confidence":0到1,
"hard_risk":false,"thesis":"一句话审慎观点","risks":["..."],
"invalidators":["..."],"facts":["..."],"inferences":["..."],"view":"..."}]}。
""".strip()


def _first_env(*names: str) -> str:
    for name in names:
        value = str(os.getenv(name) or "").strip()
        if value:
            return value
    return ""


def _first_csv_value(value: str) -> str:
    return next((item.strip() for item in str(value or "").split(",") if item.strip()), "")


def _parse_extra_headers(value: str) -> Dict[str, str]:
    if not value:
        return {}
    try:
        payload = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError("LLM extra headers must be a JSON object") from exc
    if not isinstance(payload, Mapping):
        raise ValueError("LLM extra headers must be a JSON object")
    return {str(key): str(item) for key, item in payload.items()}


def _reviewer_settings(label: str) -> Optional[Dict[str, Any]]:
    if label == "qwen":
        model = _first_env("MARKET_SCAN_QWEN_MODEL")
        if not model:
            model = _first_csv_value(_first_env("LLM_DASHSCOPE_MODELS"))
        key = _first_env("LLM_DASHSCOPE_API_KEY")
        if not key:
            key = _first_csv_value(_first_env("LLM_DASHSCOPE_API_KEYS"))
        base_url = _first_env("LLM_DASHSCOPE_BASE_URL")
        extra_headers = _first_env("LLM_DASHSCOPE_EXTRA_HEADERS")
        if model and "/" not in model:
            model = f"openai/{model}"
    else:
        model = _first_env("MARKET_SCAN_DEEPSEEK_MODEL")
        if not model:
            model = _first_csv_value(_first_env("LLM_DEEPSEEK_MODELS"))
        key = _first_env("LLM_DEEPSEEK_API_KEY", "DEEPSEEK_API_KEY")
        if not key:
            key = _first_csv_value(
                _first_env("LLM_DEEPSEEK_API_KEYS", "DEEPSEEK_API_KEYS")
            )
        base_url = _first_env("LLM_DEEPSEEK_BASE_URL")
        extra_headers = _first_env("LLM_DEEPSEEK_EXTRA_HEADERS")
        if model and "/" not in model:
            model = f"deepseek/{model}"
    if not model or not key:
        return None
    return {
        "model": model,
        "api_key": key,
        "api_base": base_url,
        "extra_headers": _parse_extra_headers(extra_headers),
    }


def _json_object_from_text(text: str) -> Mapping[str, Any]:
    cleaned = str(text or "").strip()
    fenced = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", cleaned, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        cleaned = fenced.group(1).strip()
    try:
        payload = json.loads(cleaned)
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start < 0 or end <= start:
            raise ValueError("reviewer returned no JSON object")
        payload = json.loads(cleaned[start : end + 1])
    if not isinstance(payload, Mapping):
        raise ValueError("reviewer response must be a JSON object")
    return payload


def build_litellm_reviewer(label: str) -> Optional[Callable[[Sequence[Mapping[str, Any]]], Any]]:
    """Create one explicit single-model reviewer without any fallback route."""

    settings = _reviewer_settings(label)
    if settings is None:
        return None

    def review(candidates: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
        import litellm

        user_prompt = json.dumps(
            {
                "task": "独立复核以下程序初筛候选；意见冲突会自动降级为观察。",
                "candidates": candidates,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        kwargs: Dict[str, Any] = {
            "model": settings["model"],
            "api_key": settings["api_key"],
            "messages": [
                {"role": "system", "content": MARKET_SCAN_REVIEW_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.1,
            "timeout": 120,
            "num_retries": 1,
            "response_format": {"type": "json_object"},
            "stream": False,
        }
        # Both production reviewers are hybrid-thinking models.  Their
        # reasoning tokens can consume a small output cap before the JSON
        # answer is complete.  Structured review is a deterministic extraction
        # task, so explicitly disable thinking.  Qwen's official JSON-mode
        # guidance also says not to set max_tokens; DeepSeek recommends a
        # sufficiently large cap to avoid truncating the JSON object.
        if label == "qwen":
            kwargs["extra_body"] = {"enable_thinking": False}
        else:
            kwargs["extra_body"] = {"thinking": {"type": "disabled"}}
            kwargs["max_tokens"] = 12_000
        if settings["api_base"]:
            kwargs["api_base"] = settings["api_base"]
        if settings["extra_headers"]:
            kwargs["extra_headers"] = settings["extra_headers"]
        response = litellm.completion(**kwargs)
        choices = getattr(response, "choices", None)
        if choices is None and isinstance(response, Mapping):
            choices = response.get("choices")
        if not choices:
            raise ValueError(f"{label} reviewer returned no choices")
        choice = choices[0]
        finish_reason = getattr(choice, "finish_reason", None)
        if finish_reason is None and isinstance(choice, Mapping):
            finish_reason = choice.get("finish_reason")
        if str(finish_reason or "").strip().lower() != "stop":
            raise ValueError(
                f"{label} reviewer did not finish cleanly: {finish_reason}"
            )
        message = getattr(choice, "message", None)
        if message is None and isinstance(choice, Mapping):
            message = choice.get("message")
        content = getattr(message, "content", None)
        if content is None and isinstance(message, Mapping):
            content = message.get("content")
        if not str(content or "").strip():
            raise ValueError(f"{label} reviewer returned empty content")
        return _json_object_from_text(str(content or ""))

    return review


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(file_descriptor, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    finally:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass


def _read_previous(path: Path) -> Optional[Mapping[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, Mapping) else None


def _relative_change(previous: Any, current: Any) -> float:
    try:
        old = float(previous)
        new = float(current)
    except (TypeError, ValueError):
        return float("inf") if previous != current else 0.0
    if old == 0:
        return float("inf") if new != 0 else 0.0
    return abs(new - old) / abs(old)


def has_obvious_change(
    previous: Optional[Mapping[str, Any]],
    current: Mapping[str, Any],
    *,
    level_change_threshold: float = 0.02,
) -> bool:
    """Return whether a safe report contains a material actionable change."""

    if not current.get("safe_to_push"):
        return False
    if previous is None:
        return True

    old_candidates = {
        str(item.get("code")): item
        for item in (previous.get("candidates") or [])
        if isinstance(item, Mapping)
    }
    new_candidates = {
        str(item.get("code")): item
        for item in (current.get("candidates") or [])
        if isinstance(item, Mapping)
    }
    if set(old_candidates) != set(new_candidates):
        return True
    old_actionable = {
        code for code, item in old_candidates.items() if item.get("action") == "conditional_buy"
    }
    new_actionable = {
        code for code, item in new_candidates.items() if item.get("action") == "conditional_buy"
    }
    if old_actionable != new_actionable:
        return True

    for code in set(old_candidates) & set(new_candidates):
        old = old_candidates[code]
        new = new_candidates[code]
        if old.get("action") != new.get("action"):
            return True
        if bool(old.get("hard_risk_veto")) != bool(new.get("hard_risk_veto")):
            return True
        old_plan = old.get("plan") or {}
        new_plan = new.get("plan") or {}
        for field in ("entry_low", "entry_high", "stop_loss", "take_profit_1", "take_profit_2"):
            if _relative_change(old_plan.get(field), new_plan.get(field)) >= level_change_threshold:
                return True
    return False


def _notification_succeeded(value: Any) -> bool:
    if value is True:
        return True
    if value is False or value is None:
        return False
    if isinstance(value, Mapping):
        return value.get("code") in (0, 200)
    return False


def _failure_fingerprint(result: Mapping[str, Any]) -> str:
    material = {
        "status": result.get("operational_status"),
        "failures": sorted(str(item) for item in (result.get("operational_failures") or [])),
    }
    return hashlib.sha256(
        json.dumps(material, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()[:20]


def _health_notification_content(
    result: Mapping[str, Any], report: str, *, recovered: bool
) -> str:
    if recovered:
        heading = "# 全市场买入链路已恢复"
        summary = "全市场输入与双模型买入候选链路已恢复正常，可继续按模拟规则筛选。"
    else:
        heading = "# 全市场买入链路故障"
        failures = "、".join(
            str(item) for item in (result.get("operational_failures") or [])
        ) or "unknown"
        summary = f"故障项：{failures}。本轮不会向盘中链路启用新的建仓候选。"
    return "\n".join(
        [
            heading,
            "",
            summary,
            "",
            "> 硬止损风险提醒继续有效；买入链路恢复前，常规止盈减仓不能被视为完整的组合再平衡。",
            "",
            report,
        ]
    )


def persist_result_and_notify(
    result: Mapping[str, Any],
    *,
    report_path: Path,
    result_path: Path,
    state_dir: Path,
    notify: bool,
    notifier: Optional[Notifier] = None,
    title: str = "A股+港股通全市场候选变化",
) -> Dict[str, Any]:
    """Persist report/history first, then attempt a non-blocking notification."""

    state_dir.mkdir(parents=True, exist_ok=True)
    latest_path = state_dir / "latest.json"
    last_notified_path = state_dir / "last_notified.json"
    health_path = state_dir / "operational_health.json"
    outbox_path = state_dir / "notification_outbox.json"
    history_path = state_dir / "history.jsonl"
    previous_notified = _read_previous(last_notified_path)
    previous_health = _read_previous(health_path) or {}
    report = render_market_scan_markdown(result)
    serialised = json.dumps(result, ensure_ascii=False, indent=2)

    _atomic_write_text(report_path, report)
    _atomic_write_text(result_path, serialised + "\n")
    _atomic_write_text(latest_path, serialised + "\n")
    with history_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(result, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())

    candidate_change = has_obvious_change(previous_notified, result)
    operational_failed = bool(
        result.get("operational_status") == "failed"
        or result.get("operational_failures")
    )
    failure_fingerprint = _failure_fingerprint(result) if operational_failed else ""
    previous_alert_active = bool(previous_health.get("failure_alert_active"))
    health_kind = ""
    if operational_failed and (
        not previous_alert_active
        or previous_health.get("last_alerted_fingerprint") != failure_fingerprint
    ):
        health_kind = "failure"
    elif not operational_failed and previous_alert_active:
        health_kind = "recovery"
    notification_kind = health_kind or (
        "candidate_change" if candidate_change and not operational_failed else ""
    )
    obvious_change = bool(notification_kind)
    health_state = {
        **dict(previous_health),
        "schema_version": 1,
        "updated_at": result.get("generated_at"),
        "current_status": "failed" if operational_failed else "healthy",
        "current_failure_fingerprint": failure_fingerprint,
        "current_failures": list(result.get("operational_failures") or []),
        "failure_alert_active": previous_alert_active,
    }
    _atomic_write_text(
        health_path,
        json.dumps(health_state, ensure_ascii=False, indent=2) + "\n",
    )
    outcome = {
        "persisted": True,
        "obvious_change": obvious_change,
        "notification_attempted": False,
        "notification_sent": False,
        "notification_error": "",
        "outbox_pending": False,
        "notification_kind": notification_kind,
    }
    if not notify or not obvious_change:
        return outcome

    selected_title = title
    selected_content = report
    if health_kind == "failure":
        selected_title = "全市场买入链路故障"
        selected_content = _health_notification_content(result, report, recovered=False)
    elif health_kind == "recovery":
        selected_title = "全市场买入链路已恢复"
        selected_content = _health_notification_content(result, report, recovered=True)

    def save_outbox(error: str) -> None:
        payload = {
            "schema_version": 1,
            "created_at": result.get("generated_at"),
            "title": selected_title,
            "error": error,
            "result": result,
        }
        _atomic_write_text(
            outbox_path,
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        )
        outcome["notification_error"] = error
        outcome["outbox_pending"] = True

    send = notifier
    if send is None:
        token = str(os.getenv("PUSHPLUS_TOKEN") or "").strip()
        topic = str(os.getenv("PUSHPLUS_TOPIC") or "").strip()
        if not token:
            save_outbox("PUSHPLUS_TOKEN is not configured")
            return outcome

        def send(default_title: str, content: str) -> Any:
            return send_markdown(
                token=token,
                title=default_title,
                content=content,
                topic=topic,
            )

    outcome["notification_attempted"] = True
    try:
        notification_result = send(selected_title, selected_content)
    except Exception as exc:  # noqa: BLE001 - state and report are already durable.
        save_outbox(f"{type(exc).__name__}: {exc}")
        return outcome
    if not _notification_succeeded(notification_result):
        save_outbox("notification provider did not return explicit success")
        return outcome
    outcome["notification_sent"] = True
    if health_kind == "failure":
        health_state.update(
            {
                "failure_alert_active": True,
                "last_alerted_fingerprint": failure_fingerprint,
                "last_alerted_at": result.get("generated_at"),
            }
        )
    elif health_kind == "recovery":
        health_state.update(
            {
                "failure_alert_active": False,
                "last_recovered_at": result.get("generated_at"),
            }
        )
        _atomic_write_text(last_notified_path, serialised + "\n")
    else:
        _atomic_write_text(last_notified_path, serialised + "\n")
    _atomic_write_text(
        health_path,
        json.dumps(health_state, ensure_ascii=False, indent=2) + "\n",
    )
    try:
        outbox_path.unlink()
    except FileNotFoundError:
        pass
    return outcome


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT_PATH)
    parser.add_argument("--result-json", type=Path, default=DEFAULT_RESULT_PATH)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE_DIR)
    parser.add_argument("--notify", action="store_true")
    parser.add_argument("--top-a-history", type=int, default=40)
    parser.add_argument("--top-hk-history", type=int, default=20)
    parser.add_argument("--final-top-n", type=int, default=12)
    parser.add_argument("--min-net-rr", type=float, default=1.8)
    parser.add_argument("--a-cache-max-age-hours", type=float, default=6.0)
    parser.add_argument("--hk-cache-max-age-hours", type=float, default=6.0)
    parser.add_argument("--hk-membership-cache-max-age-hours", type=float, default=840.0)
    parser.add_argument("--min-actionable-data-quality", type=float, default=0.70)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    config = MarketScanConfig(
        top_a_history=args.top_a_history,
        top_hk_history=args.top_hk_history,
        final_top_n=args.final_top_n,
        min_net_rr=args.min_net_rr,
        a_cache_max_age_hours=args.a_cache_max_age_hours,
        hk_cache_max_age_hours=args.hk_cache_max_age_hours,
        hk_membership_cache_max_age_hours=args.hk_membership_cache_max_age_hours,
        min_actionable_data_quality=args.min_actionable_data_quality,
        a_cache_path=args.state_dir / "a_share_snapshot.json",
        hk_cache_path=args.state_dir / "hk_connect_snapshot.json",
    )
    service = MarketScanService(
        a_snapshot_loader=default_a_snapshot_loader,
        hk_connect_snapshot_loader=default_hk_connect_snapshot_loader,
        hk_all_snapshot_loader=default_hk_all_snapshot_loader,
        history_loader=default_history_loader,
        qwen_reviewer=build_litellm_reviewer("qwen"),
        deepseek_reviewer=build_litellm_reviewer("deepseek"),
        config=config,
    )
    result = service.run()
    outcome = persist_result_and_notify(
        result,
        report_path=args.report,
        result_path=args.result_json,
        state_dir=args.state_dir,
        notify=args.notify,
    )
    print(
        json.dumps(
            {
                "candidate_count": len(result.get("candidates") or []),
                "safe_to_push": result.get("safe_to_push"),
                **outcome,
            },
            ensure_ascii=False,
        )
    )
    return 2 if result.get("operational_status") == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
