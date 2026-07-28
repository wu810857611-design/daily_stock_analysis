# -*- coding: utf-8 -*-
"""Offline contract tests for the layered full-market scanner."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Mapping, Sequence
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import yaml

from scripts.market_scan import MARKET_SCAN_REVIEW_SYSTEM_PROMPT, persist_result_and_notify
from src.services.market_scan_service import (
    MARKET_A,
    MARKET_HK,
    MarketScanConfig,
    MarketScanService,
    render_market_scan_markdown,
    validate_trade_plan,
)


NOW = datetime(2026, 7, 28, 14, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
ROOT_DIR = Path(__file__).resolve().parent.parent


def _a_snapshot() -> Mapping[str, Any]:
    return {
        "as_of": NOW.isoformat(),
        "source": "fake_a_full_market",
        "is_full_a_universe": True,
        "records": [
            {"代码": "600001", "名称": "甲公司", "最新价": 100, "涨跌幅": 2.5, "成交量": 1_000_000, "成交额": 900_000_000, "换手率": 3.0, "量比": 1.4, "市盈率-动态": 20, "市净率": 2.0},
            {"代码": "600002", "名称": "乙公司", "最新价": 100, "涨跌幅": 3.0, "成交量": 900_000, "成交额": 800_000_000, "换手率": 2.5, "量比": 1.2, "市盈率-动态": 25, "市净率": 2.5},
            {"代码": "600003", "名称": "丙公司", "最新价": 100, "涨跌幅": 1.5, "成交量": 800_000, "成交额": 700_000_000, "换手率": 2.0, "量比": 1.1},
            {"代码": "600004", "名称": "*ST风险", "最新价": 10, "涨跌幅": 1.0, "成交量": 700_000, "成交额": 600_000_000},
            {"代码": "600005", "名称": "停牌股", "最新价": 0, "涨跌幅": 0, "成交量": 0, "成交额": 0},
            {"代码": "600006", "名称": "追高股", "最新价": 30, "涨跌幅": 12, "成交量": 600_000, "成交额": 500_000_000},
            {"代码": "600007", "名称": "N新上市", "最新价": 30, "涨跌幅": 5, "成交量": 600_000, "成交额": 500_000_000},
        ],
    }


def _hk_snapshot(*, include_non_connect: bool = True) -> Mapping[str, Any]:
    records = [
        {"代码": "00700", "名称": "腾讯控股", "最新价": 100, "涨跌幅": 2.0, "成交量": 1_000_000, "成交额": 700_000_000, "is_connect": True},
        {"代码": "00981", "名称": "中芯国际", "最新价": 100, "涨跌幅": 3.0, "成交量": 900_000, "成交额": 650_000_000, "is_connect": True},
    ]
    if include_non_connect:
        records.append(
            {"代码": "09999", "名称": "非港股通", "最新价": 100, "涨跌幅": 1.0, "成交量": 800_000, "成交额": 600_000_000, "is_connect": False}
        )
    return {
        "as_of": NOW.isoformat(),
        "source": "fake_hk_connect",
        "is_connect_universe": True,
        "records": records,
    }


def _history(_code: str, _days: int) -> Any:
    dates = pd.date_range("2026-03-01", periods=90, freq="B")
    close = np.linspace(82.0, 100.0, len(dates)) + np.sin(np.linspace(0, 5, len(dates))) * 0.25
    frame = pd.DataFrame(
        {
            "date": dates,
            "open": close * 0.998,
            "high": close * 1.01,
            "low": close * 0.99,
            "close": close,
            "volume": np.linspace(800_000, 1_000_000, len(dates)),
            "amount": np.linspace(80_000_000, 100_000_000, len(dates)),
        }
    )
    return frame, "fake_history"


class ReviewRecorder:
    def __init__(self, verdict: str = "pass", *, hard_risk: bool = False):
        self.verdict = verdict
        self.hard_risk = hard_risk
        self.calls: list[list[Dict[str, Any]]] = []

    def __call__(self, candidates: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
        copied = [dict(candidate) for candidate in candidates]
        self.calls.append(copied)
        return {
            "reviews": [
                {
                    "code": candidate["code"],
                    "verdict": self.verdict,
                    "confidence": 0.8,
                    "hard_risk": self.hard_risk,
                    "thesis": f"{self.verdict} from fake reviewer",
                    "risks": [],
                    "invalidators": ["跌破程序止损"],
                }
                for candidate in candidates
            ]
        }


def _config(tmp_path: Path, **overrides: Any) -> MarketScanConfig:
    values = {
        "top_a_history": 2,
        "top_hk_history": 1,
        "final_top_n": 3,
        "min_a_amount": 1.0,
        "min_hk_amount": 1.0,
        "min_history_bars": 60,
        "a_cache_path": tmp_path / "a_share.json",
        "hk_cache_path": tmp_path / "hk_connect.json",
    }
    values.update(overrides)
    return MarketScanConfig(**values)


def _service(
    tmp_path: Path,
    *,
    qwen: Any,
    deepseek: Any,
    history_loader: Any = _history,
    a_loader: Any = _a_snapshot,
    hk_loader: Any = _hk_snapshot,
    config_overrides: Mapping[str, Any] | None = None,
    clock: Any = None,
) -> MarketScanService:
    return MarketScanService(
        a_snapshot_loader=a_loader,
        hk_connect_snapshot_loader=hk_loader,
        history_loader=history_loader,
        qwen_reviewer=qwen,
        deepseek_reviewer=deepseek,
        config=_config(tmp_path, **dict(config_overrides or {})),
        clock=clock or (lambda: NOW),
    )


def test_l1_is_vectorised_and_makes_zero_history_or_llm_calls(tmp_path: Path) -> None:
    qwen = ReviewRecorder()
    deepseek = ReviewRecorder()
    history_calls: list[str] = []

    def history_loader(code: str, days: int) -> Any:
        history_calls.append(code)
        return _history(code, days)

    service = _service(
        tmp_path,
        qwen=qwen,
        deepseek=deepseek,
        history_loader=history_loader,
    )
    result = service.run_l1()

    assert result.diagnostics["l1_llm_calls"] == 0
    assert qwen.calls == []
    assert deepseek.calls == []
    assert history_calls == []
    assert {item["code"] for item in result.a_candidates} <= {"600001", "600002", "600003"}
    assert result.diagnostics[MARKET_A]["filtered"]["st_or_delisting"] == 1
    assert result.diagnostics[MARKET_A]["filtered"]["suspended_or_zero_turnover"] >= 1
    assert result.diagnostics[MARKET_A]["filtered"]["extreme_chase"] == 1
    assert result.diagnostics[MARKET_A]["filtered"]["new_listing_name"] == 1


def test_only_shortlist_loads_history_and_models_review_final_batch_independently(
    tmp_path: Path,
) -> None:
    qwen = ReviewRecorder()
    deepseek = ReviewRecorder()
    history_calls: list[str] = []

    def history_loader(code: str, days: int) -> Any:
        history_calls.append(code)
        return _history(code, days)

    service = _service(
        tmp_path,
        qwen=qwen,
        deepseek=deepseek,
        history_loader=history_loader,
    )
    result = service.run()

    assert len(history_calls) == 3
    assert len(qwen.calls) == 1
    assert len(deepseek.calls) == 1
    assert len(qwen.calls[0]) == len(result["candidates"]) <= 3
    assert qwen.calls[0] is not deepseek.calls[0]
    assert all(
        {"amount", "turnover_rate", "volume_ratio", "pe", "pb", "data_availability"}
        <= set(candidate)
        for candidate in qwen.calls[0]
    )
    assert result["diagnostics"]["llm_calls"] == {"qwen": 1, "deepseek": 1}
    assert all(candidate["action"] == "watch" for candidate in result["candidates"])
    assert all(
        candidate["research_status"] == "deep_research_required"
        for candidate in result["candidates"]
    )
    assert all(candidate["simulation_advice"] == "等待" for candidate in result["candidates"])
    assert all(candidate["data_availability"]["level2"] == "unavailable" for candidate in result["candidates"])
    assert all(candidate["position_state"] == "flat" for candidate in result["candidates"])
    assert all(
        candidate["eligible_for_intraday_review"] is False
        for candidate in result["candidates"]
    )
    assert result["auto_order_enabled"] is False


def test_hk_universe_strictly_excludes_non_connect_symbols(tmp_path: Path) -> None:
    service = _service(
        tmp_path,
        qwen=ReviewRecorder(),
        deepseek=ReviewRecorder(),
        config_overrides={"top_hk_history": 3, "final_top_n": 3},
    )

    result = service.run_l1()

    assert {item["code"] for item in result.hk_candidates} == {"HK00700", "HK00981"}
    assert result.diagnostics["hk_connect_strict"] is True
    cached = json.loads((tmp_path / "hk_connect.json").read_text(encoding="utf-8"))
    assert cached["is_connect_universe"] is True
    assert {item["code"] for item in cached["records"]} == {"HK00700", "HK00981"}


def test_a_snapshot_provider_failure_uses_fresh_cache_without_rewriting_asof(
    tmp_path: Path,
) -> None:
    _service(
        tmp_path,
        qwen=ReviewRecorder(),
        deepseek=ReviewRecorder(),
    ).run_l1()
    original = json.loads((tmp_path / "a_share.json").read_text(encoding="utf-8"))

    def broken_a_loader() -> Any:
        raise TimeoutError("provider timeout")

    later = NOW + timedelta(hours=1)
    cached_result = _service(
        tmp_path,
        qwen=ReviewRecorder(),
        deepseek=ReviewRecorder(),
        a_loader=broken_a_loader,
        clock=lambda: later,
        config_overrides={"a_cache_max_age_hours": 2.0},
    ).run_l1()

    assert cached_result.a_safe_halt is False
    assert cached_result.as_of[MARKET_A] == original["as_of"]
    assert cached_result.as_of[MARKET_A] != later.isoformat()
    assert cached_result.diagnostics["a_snapshot_source"].endswith(":last_good_cache")


def test_fetch_time_is_not_fabricated_as_provider_market_timestamp(tmp_path: Path) -> None:
    def a_without_provider_time() -> Mapping[str, Any]:
        payload = dict(_a_snapshot())
        payload.pop("as_of", None)
        return payload

    def hk_without_provider_time() -> Mapping[str, Any]:
        payload = dict(_hk_snapshot())
        payload.pop("as_of", None)
        return payload

    result = _service(
        tmp_path,
        qwen=ReviewRecorder(),
        deepseek=ReviewRecorder(),
        a_loader=a_without_provider_time,
        hk_loader=hk_without_provider_time,
    ).run_l1()

    assert result.as_of[MARKET_A] == ""
    assert result.as_of[MARKET_HK] == ""
    assert result.diagnostics["a_snapshot_fetched_at"] == NOW.isoformat()
    assert result.diagnostics["hk_snapshot_fetched_at"] == NOW.isoformat()


def test_a_snapshot_stale_cache_safely_blocks_push(tmp_path: Path) -> None:
    stale_time = NOW - timedelta(hours=10)
    (tmp_path / "a_share.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "saved_at": stale_time.isoformat(),
                "as_of": stale_time.isoformat(),
                "source": "stale_fixture",
                "is_full_a_universe": True,
                "records": _a_snapshot()["records"],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def broken_a_loader() -> Any:
        raise TimeoutError("provider timeout")

    result = _service(
        tmp_path,
        qwen=ReviewRecorder(),
        deepseek=ReviewRecorder(),
        a_loader=broken_a_loader,
        config_overrides={"a_cache_max_age_hours": 1.0},
    ).run()

    assert result["safe_to_push"] is False
    assert result["diagnostics"]["a_full_market_strict"] is False
    assert any("A股全市场快照不可用" in reason for reason in result["push_block_reasons"])


def test_expired_hk_cache_safely_blocks_push_when_provider_fails(tmp_path: Path) -> None:
    stale_time = NOW - timedelta(hours=10)
    cache_path = tmp_path / "hk_connect.json"
    cache_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "saved_at": stale_time.isoformat(),
                "as_of": stale_time.isoformat(),
                "source": "stale_fixture",
                "is_connect_universe": True,
                "records": _hk_snapshot(include_non_connect=False)["records"],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def broken_hk_loader() -> Any:
        raise TimeoutError("provider timeout")

    service = _service(
        tmp_path,
        qwen=ReviewRecorder(),
        deepseek=ReviewRecorder(),
        hk_loader=broken_hk_loader,
        config_overrides={"hk_cache_max_age_hours": 1.0},
    )
    result = service.run()

    assert result["safe_to_push"] is False
    assert result["diagnostics"]["hk_connect_strict"] is False
    assert result["diagnostics"][MARKET_HK]["input_count"] == 0
    assert any("港股通成分数据不可用" in reason for reason in result["push_block_reasons"])


def test_trade_plan_rr_boundary_is_inclusive() -> None:
    base = {
        "entry_low": 99.0,
        "entry_high": 101.0,
        "stop_loss": 95.0,
        "take_profit_1": 109.0,
        "take_profit_2": 114.0,
        "net_rr": 1.8,
    }

    accepted = validate_trade_plan(base, min_net_rr=1.8)
    rejected = validate_trade_plan({**base, "net_rr": 1.799}, min_net_rr=1.8)

    assert accepted.valid is True
    assert rejected.valid is False
    assert rejected.reasons == ("net_rr_below_minimum",)


def test_model_conflict_or_hard_risk_downgrades_to_watch(tmp_path: Path) -> None:
    conflict = _service(
        tmp_path,
        qwen=ReviewRecorder("pass"),
        deepseek=ReviewRecorder("watch"),
    ).run()

    assert conflict["candidates"]
    assert all(candidate["action"] == "watch" for candidate in conflict["candidates"])
    assert all(candidate["model_disagreement"] is True for candidate in conflict["candidates"])

    hard_risk = _service(
        tmp_path,
        qwen=ReviewRecorder("pass"),
        deepseek=ReviewRecorder("pass", hard_risk=True),
    ).run()
    assert all(candidate["action"] == "watch" for candidate in hard_risk["candidates"])
    assert all(candidate["hard_risk_veto"] is True for candidate in hard_risk["candidates"])


def test_v4_prompt_and_report_separate_facts_inferences_views_and_mark_l2_unavailable(
    tmp_path: Path,
) -> None:
    result = _service(
        tmp_path,
        qwen=ReviewRecorder("pass"),
        deepseek=ReviewRecorder("pass"),
    ).run()
    candidate = result["candidates"][0]
    report = render_market_scan_markdown(result)

    assert all(token in MARKET_SCAN_REVIEW_SYSTEM_PROMPT for token in ("facts", "inferences", "view"))
    assert "Level-2" in MARKET_SCAN_REVIEW_SYSTEM_PROMPT
    assert candidate["data_availability"]["order_book_l1"] == "unavailable"
    assert candidate["data_availability"]["level2"] == "unavailable"
    assert candidate["scenario_probabilities"] is None
    assert candidate["t_trade"]["eligible"] is False
    assert candidate["scope"] == "simulation"
    assert candidate["policy_market"] in {"cn", "hk"}
    assert candidate["confidence"] <= candidate["data_quality"]
    assert candidate["simulation_advice"] == "等待"
    assert "### 已核验事实" in report
    assert "### 规则推断（不是事实）" in report
    assert "### 审慎观点" in report
    assert "Level-2=unavailable" in report


def test_push_failure_does_not_lose_report_or_independent_history(tmp_path: Path) -> None:
    result = {
        "generated_at": NOW.isoformat(),
        "as_of": {MARKET_A: NOW.isoformat(), MARKET_HK: NOW.isoformat()},
        "safe_to_push": True,
        "push_block_reasons": [],
        "candidates": [
            {
                "rank": 1,
                "code": "600001",
                "name": "甲公司",
                "action": "conditional_buy",
                "plan": {
                    "entry_low": 99,
                    "entry_high": 100,
                    "stop_loss": 95,
                    "take_profit_1": 110,
                    "take_profit_2": 115,
                    "net_rr": 2.0,
                },
                "qwen_review": {"verdict": "pass"},
                "deepseek_review": {"verdict": "pass"},
            }
        ],
        "disclaimer": "simulation only",
    }

    def broken_notifier(_title: str, _content: str) -> None:
        raise RuntimeError("PushPlus unavailable")

    outcome = persist_result_and_notify(
        result,
        report_path=tmp_path / "reports" / "scan.md",
        result_path=tmp_path / "reports" / "scan.json",
        state_dir=tmp_path / "state",
        notify=True,
        notifier=broken_notifier,
    )

    assert outcome["persisted"] is True
    assert outcome["notification_attempted"] is True
    assert outcome["notification_sent"] is False
    assert "PushPlus unavailable" in outcome["notification_error"]
    assert outcome["outbox_pending"] is True
    assert (tmp_path / "reports" / "scan.md").is_file()
    assert (tmp_path / "reports" / "scan.json").is_file()
    assert (tmp_path / "state" / "latest.json").is_file()
    history_lines = (tmp_path / "state" / "history.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(history_lines) == 1
    assert json.loads(history_lines[0])["candidates"][0]["code"] == "600001"
    assert not (tmp_path / "state" / "last_notified.json").exists()
    assert (tmp_path / "state" / "notification_outbox.json").is_file()

    retry_calls: list[str] = []

    def successful_notifier(title: str, _content: str) -> bool:
        retry_calls.append(title)
        return True

    retried = persist_result_and_notify(
        result,
        report_path=tmp_path / "reports" / "scan.md",
        result_path=tmp_path / "reports" / "scan.json",
        state_dir=tmp_path / "state",
        notify=True,
        notifier=successful_notifier,
    )
    assert retried["obvious_change"] is True
    assert retried["notification_sent"] is True
    assert retry_calls
    assert (tmp_path / "state" / "last_notified.json").is_file()
    assert not (tmp_path / "state" / "notification_outbox.json").exists()

    def should_not_run(_title: str, _content: str) -> bool:
        raise AssertionError("unchanged result should not be sent after confirmed success")

    unchanged = persist_result_and_notify(
        result,
        report_path=tmp_path / "reports" / "scan.md",
        result_path=tmp_path / "reports" / "scan.json",
        state_dir=tmp_path / "state",
        notify=True,
        notifier=should_not_run,
    )
    assert unchanged["obvious_change"] is False
    assert unchanged["notification_attempted"] is False


def test_false_notification_result_stays_pending_and_retries_next_run(tmp_path: Path) -> None:
    result = {
        "generated_at": NOW.isoformat(),
        "as_of": {MARKET_A: NOW.isoformat(), MARKET_HK: NOW.isoformat()},
        "safe_to_push": True,
        "push_block_reasons": [],
        "candidates": [{"code": "600001", "action": "watch", "plan": {}}],
        "disclaimer": "simulation only",
    }
    false_calls: list[int] = []

    def false_notifier(_title: str, _content: str) -> bool:
        false_calls.append(1)
        return False

    first = persist_result_and_notify(
        result,
        report_path=tmp_path / "scan.md",
        result_path=tmp_path / "scan.json",
        state_dir=tmp_path / "state",
        notify=True,
        notifier=false_notifier,
    )
    assert first["notification_sent"] is False
    assert first["outbox_pending"] is True
    assert len(false_calls) == 1
    assert not (tmp_path / "state" / "last_notified.json").exists()

    second = persist_result_and_notify(
        result,
        report_path=tmp_path / "scan.md",
        result_path=tmp_path / "scan.json",
        state_dir=tmp_path / "state",
        notify=True,
        notifier=lambda _title, _content: True,
    )
    assert second["obvious_change"] is True
    assert second["notification_sent"] is True


def test_market_scan_workflow_is_independent_simulation_with_fixed_state_artifact() -> None:
    path = ROOT_DIR / ".github" / "workflows" / "02-market-scan.yml"
    text = path.read_text(encoding="utf-8")
    workflow = yaml.load(text, Loader=yaml.BaseLoader)
    job = workflow["jobs"]["scan"]
    steps = job["steps"]

    assert workflow["on"]["schedule"]
    assert any(
        item.get("cron") == "15 11 * * 1-5"
        for item in workflow["on"]["schedule"]
    )
    assert job["env"]["MARKET_SCAN_MODE"] == "simulation_only"
    assert "LLM_DASHSCOPE_API_KEY" in job["env"]
    assert "LLM_DEEPSEEK_API_KEY" in job["env"]
    assert any(step.get("uses") == "actions/cache/restore@v4" for step in steps)
    assert any(step.get("uses") == "actions/cache/save@v4" for step in steps)
    state_upload = next(
        step
        for step in steps
        if step.get("name") == "上传固定名状态供其他模拟工作流恢复"
    )
    assert state_upload["with"]["name"] == "market-scan-state"
    assert state_upload["with"]["path"] == "market-scan-state.tar.gz"
    assert "tar -czf market-scan-state.tar.gz data/market_scan/" in text
    assert "actions/artifacts?name=market-scan-state" in text
    assert "unsafe market-scan entry" in text
    assert "scripts/market_scan.py" in text
    assert "00-daily-analysis.yml" not in text
    assert "01-adaptive-market-monitor.yml" not in text
    assert "broker" not in text.lower()
