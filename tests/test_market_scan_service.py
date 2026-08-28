# -*- coding: utf-8 -*-
"""Offline contract tests for the layered full-market scanner."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Mapping, Sequence
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest
import yaml

from scripts import market_scan_reviewer_smoke
from scripts.market_scan import (
    MARKET_SCAN_REVIEW_SYSTEM_PROMPT,
    build_litellm_reviewer,
    persist_result_and_notify,
)
from scripts.intraday_session import (
    RealtimeQuote,
    enqueue_adaptive_plan_reviews,
    load_candidate_plans,
    load_state_v2,
)
from src.services.market_scan_service import (
    MARKET_A,
    MARKET_HK,
    MarketScanConfig,
    MarketScanService,
    default_a_snapshot_loader,
    default_hk_all_snapshot_loader,
    render_market_scan_markdown,
    validate_trade_plan,
)
from src.services.position_sizing_policy import classify_opportunity


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
    def __init__(
        self,
        verdict: str = "pass",
        *,
        hard_risk: bool = False,
        confidence: float = 0.8,
    ):
        self.verdict = verdict
        self.hard_risk = hard_risk
        self.confidence = confidence
        self.calls: list[list[Dict[str, Any]]] = []

    def __call__(self, candidates: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
        copied = [dict(candidate) for candidate in candidates]
        self.calls.append(copied)
        return {
            "reviews": [
                {
                    "code": candidate["code"],
                    "verdict": self.verdict,
                    "confidence": self.confidence,
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
        "hk_membership_cache_path": tmp_path / "hk_connect_membership.json",
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
    hk_all_loader: Any = None,
    config_overrides: Mapping[str, Any] | None = None,
    clock: Any = None,
) -> MarketScanService:
    return MarketScanService(
        a_snapshot_loader=a_loader,
        hk_connect_snapshot_loader=hk_loader,
        hk_all_snapshot_loader=hk_all_loader,
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
        {
            "amount",
            "turnover_rate",
            "volume_ratio",
            "pe",
            "pb",
            "data_availability",
            "review_request",
        }
        <= set(candidate)
        for candidate in qwen.calls[0]
    )
    assert result["diagnostics"]["llm_calls"] == {"qwen": 1, "deepseek": 1}
    assert all(
        (candidate["facts"] or {}).get("snapshot_fetched_at") == NOW.isoformat()
        and (candidate["facts"] or {}).get("snapshot_source")
        for candidate in qwen.calls[0]
    )
    assert all(
        candidate["action"] == "conditional_buy"
        for candidate in result["candidates"]
    )
    assert all(
        candidate["research_status"] == "actionable"
        for candidate in result["candidates"]
    )
    assert all(
        candidate["simulation_advice"] == "进入买入区后建议首笔建仓2.5%"
        for candidate in result["candidates"]
    )
    assert all(candidate["data_availability"]["level2"] == "unavailable" for candidate in result["candidates"])
    assert all(candidate["position_state"] == "flat" for candidate in result["candidates"])
    assert all(
        candidate["eligible_for_intraday_review"] is True
        for candidate in result["candidates"]
    )
    assert result["diagnostics"]["buy_funnel"]["actionable_count"] == len(
        result["candidates"]
    )
    assert result["auto_order_enabled"] is False


def test_production_scan_artifact_is_accepted_by_intraday_buy_gate(
    tmp_path: Path,
) -> None:
    result = _service(
        tmp_path,
        qwen=ReviewRecorder("pass"),
        deepseek=ReviewRecorder("pass"),
    ).run()
    artifact = tmp_path / "latest.json"
    artifact.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")

    loaded = load_candidate_plans(artifact, now=NOW)

    assert loaded
    assert {item["code"] for item in loaded} == {
        item["code"] for item in result["candidates"]
    }
    assert all(float(item["data_quality"]) >= 0.70 for item in loaded)
    assert all(item["position_state"] == "flat" for item in loaded)
    candidate = loaded[0]
    plan = candidate["plan"]
    entry_price = float(plan["entry_mid"])
    quote = RealtimeQuote(
        symbol=str(candidate["code"]),
        name=str(candidate["name"]),
        price=entry_price,
        change_pct=0.0,
        provider_timestamp=NOW.isoformat(),
        fetched_at=NOW.isoformat(),
        stale_seconds=0.0,
        is_stale=False,
        source="integration_fixture",
    )
    state = load_state_v2(tmp_path / "missing_state.json", now=NOW)
    assert enqueue_adaptive_plan_reviews(
        state,
        now=NOW,
        quotes=[quote],
        candidates=[candidate],
    ) == 1
    assert state["event_ledger"][-1]["condition"] == "adaptive_entry_review"


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
    membership = json.loads(
        (tmp_path / "hk_connect_membership.json").read_text(encoding="utf-8")
    )
    assert cached["is_connect_universe"] is True
    assert set(cached["membership_codes"]) == {"HK00700", "HK00981"}
    assert set(membership["membership_codes"]) == {"HK00700", "HK00981"}
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


def test_default_full_market_loaders_use_independent_fallbacks(monkeypatch: Any) -> None:
    a_calls: list[str] = []
    hk_calls: list[str] = []

    def broken_a() -> Any:
        a_calls.append("eastmoney")
        raise TimeoutError("eastmoney unavailable")

    def sina_a() -> pd.DataFrame:
        a_calls.append("sina")
        return pd.DataFrame(
            [{"代码": "sh600001", "名称": "甲公司", "最新价": 10, "成交量": 1, "成交额": 1}]
        )

    def broken_hk() -> Any:
        hk_calls.append("eastmoney")
        raise TimeoutError("eastmoney unavailable")

    def sina_hk() -> pd.DataFrame:
        hk_calls.append("sina")
        return pd.DataFrame(
            [{"代码": "00700", "中文名称": "腾讯控股", "最新价": 100, "成交量": 1, "成交额": 1}]
        )

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(
            stock_zh_a_spot_em=broken_a,
            stock_zh_a_spot=sina_a,
            stock_hk_spot_em=broken_hk,
            stock_hk_spot=sina_hk,
        ),
    )

    a_payload = default_a_snapshot_loader()
    hk_payload = default_hk_all_snapshot_loader()

    assert a_calls == ["eastmoney", "sina"]
    assert hk_calls == ["eastmoney", "sina"]
    assert a_payload["source"] == "akshare.stock_zh_a_spot"
    assert hk_payload["source"] == "akshare.stock_hk_spot"
    assert a_payload["provider_errors"]
    assert hk_payload["provider_errors"]


def test_snapshot_retry_uses_bounded_backoff_before_recovery(tmp_path: Path) -> None:
    calls = 0
    sleeps: list[float] = []

    def flaky_a_loader() -> Mapping[str, Any]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("temporary disconnect")
        return _a_snapshot()

    service = MarketScanService(
        a_snapshot_loader=flaky_a_loader,
        hk_connect_snapshot_loader=_hk_snapshot,
        history_loader=_history,
        qwen_reviewer=ReviewRecorder(),
        deepseek_reviewer=ReviewRecorder(),
        config=_config(
            tmp_path,
            snapshot_retries=2,
            snapshot_retry_backoff_seconds=1.25,
        ),
        clock=lambda: NOW,
        sleeper=sleeps.append,
    )

    result = service.run_l1()

    assert result.a_safe_halt is False
    assert calls == 2
    assert sleeps == [1.25]
    assert result.diagnostics["a_snapshot_provider_errors"]


def test_hk_quote_fallback_filters_fresh_all_market_quotes_by_cached_membership(
    tmp_path: Path,
) -> None:
    _service(
        tmp_path,
        qwen=ReviewRecorder(),
        deepseek=ReviewRecorder(),
    ).run_l1()

    def broken_connect_loader() -> Any:
        raise TimeoutError("connect endpoint unavailable")

    def all_hk_loader() -> Mapping[str, Any]:
        return {
            "source": "fake_all_hk",
            "as_of": (NOW + timedelta(hours=1)).isoformat(),
            "is_full_hk_universe": True,
            "records": [
                {
                    "代码": "00700",
                    "中文名称": "腾讯控股",
                    "最新价": 101,
                    "涨跌幅": 1,
                    "成交量": 1_000,
                    "成交额": 1_000,
                },
                {
                    "代码": "09999",
                    "中文名称": "非港股通",
                    "最新价": 103,
                    "涨跌幅": 1,
                    "成交量": 1_000,
                    "成交额": 1_000,
                },
            ],
        }

    result = _service(
        tmp_path,
        qwen=ReviewRecorder(),
        deepseek=ReviewRecorder(),
        hk_loader=broken_connect_loader,
        hk_all_loader=all_hk_loader,
        clock=lambda: NOW + timedelta(hours=1),
        config_overrides={
            "hk_cache_max_age_hours": 0.1,
            "top_hk_history": 2,
        },
    ).run_l1()

    assert result.hk_safe_halt is False
    assert {item["code"] for item in result.hk_candidates} == {"HK00700"}
    assert result.diagnostics["hk_snapshot_source"].endswith(
        ":cached_connect_membership"
    )
    cached = json.loads((tmp_path / "hk_connect.json").read_text(encoding="utf-8"))
    assert {item["code"] for item in cached["records"]} == {"HK00700"}
    assert set(cached["membership_codes"]) == {"HK00700", "HK00981"}
    assert cached["membership_fetched_at"] == NOW.isoformat()


def test_dedicated_hk_membership_survives_volatile_quote_cache_loss(
    tmp_path: Path,
) -> None:
    _service(
        tmp_path,
        qwen=ReviewRecorder(),
        deepseek=ReviewRecorder(),
    ).run_l1()
    (tmp_path / "hk_connect.json").unlink()

    def broken_connect_loader() -> Any:
        raise TimeoutError("connect endpoint unavailable")

    result = _service(
        tmp_path,
        qwen=ReviewRecorder(),
        deepseek=ReviewRecorder(),
        hk_loader=broken_connect_loader,
        hk_all_loader=lambda: {
            "source": "fake_all_hk",
            "as_of": (NOW + timedelta(hours=1)).isoformat(),
            "is_full_hk_universe": True,
            "records": [
                {
                    "代码": "00700",
                    "中文名称": "腾讯控股",
                    "最新价": 101,
                    "涨跌幅": 1,
                    "成交量": 1_000,
                    "成交额": 1_000,
                },
                {
                    "代码": "09999",
                    "中文名称": "非港股通",
                    "最新价": 103,
                    "涨跌幅": 1,
                    "成交量": 1_000,
                    "成交额": 1_000,
                },
            ],
        },
        clock=lambda: NOW + timedelta(hours=1),
        config_overrides={"top_hk_history": 2},
    ).run_l1()

    assert result.hk_safe_halt is False
    assert {item["code"] for item in result.hk_candidates} == {"HK00700"}


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
    report = render_market_scan_markdown(
        _service(
            tmp_path,
            qwen=ReviewRecorder(),
            deepseek=ReviewRecorder(),
            a_loader=a_without_provider_time,
            hk_loader=hk_without_provider_time,
        ).run()
    )
    assert "A股快照：可用" in report
    assert "提供方时间=提供方未返回" in report


def test_a_snapshot_stale_cache_blocks_only_a_market(tmp_path: Path) -> None:
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
        config_overrides={
            "a_cache_max_age_hours": 1.0,
            "min_net_rr": 0.1,
        },
    ).run()

    assert result["safe_to_push"] is True
    assert result["operational_status"] == "degraded"
    assert "a_share_full_market_snapshot_unavailable" in result[
        "operational_failures"
    ]
    assert result["diagnostics"]["a_full_market_strict"] is False
    assert result["candidates"]
    assert all(item["market"] == MARKET_HK for item in result["candidates"])
    assert all(item["action"] == "conditional_buy" for item in result["candidates"])
    assert result["push_block_reasons"] == []
    assert result["market_block_reasons"][MARKET_A]


def test_expired_hk_cache_blocks_only_hk_when_provider_fails(tmp_path: Path) -> None:
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

    assert result["safe_to_push"] is True
    assert result["operational_status"] == "degraded"
    assert result["diagnostics"]["hk_connect_strict"] is False
    assert result["diagnostics"][MARKET_HK]["input_count"] == 0
    assert result["push_block_reasons"] == []
    assert result["market_block_reasons"][MARKET_HK]
    assert all(item["market"] == MARKET_A for item in result["candidates"])
    assert all(item["action"] == "conditional_buy" for item in result["candidates"])


def test_fresh_hk_quotes_cannot_extend_expired_connect_membership(
    tmp_path: Path,
) -> None:
    cache_path = tmp_path / "hk_connect.json"
    cache_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "saved_at": (NOW - timedelta(minutes=30)).isoformat(),
                "as_of": (NOW - timedelta(minutes=30)).isoformat(),
                "membership_fetched_at": (NOW - timedelta(days=36)).isoformat(),
                "source": "fresh_quotes_with_expired_membership",
                "is_connect_universe": True,
                "membership_codes": ["HK00700", "HK00981"],
                "records": _hk_snapshot(include_non_connect=False)["records"],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def broken_hk_loader() -> Any:
        raise TimeoutError("provider timeout")

    result = _service(
        tmp_path,
        qwen=ReviewRecorder(),
        deepseek=ReviewRecorder(),
        hk_loader=broken_hk_loader,
        config_overrides={
            "hk_cache_max_age_hours": 6.0,
            "hk_membership_cache_max_age_hours": 24.0 * 35.0,
        },
    ).run()

    assert result["operational_status"] == "degraded"
    assert result["diagnostics"]["hk_connect_strict"] is False
    assert "hk_connect_snapshot_unavailable" in result["operational_failures"]


def test_both_market_snapshots_unavailable_still_fail_closed(tmp_path: Path) -> None:
    def broken_loader() -> Any:
        raise TimeoutError("all providers unavailable")

    result = _service(
        tmp_path,
        qwen=ReviewRecorder(),
        deepseek=ReviewRecorder(),
        a_loader=broken_loader,
        hk_loader=broken_loader,
    ).run()

    assert result["operational_status"] == "failed"
    assert result["safe_to_push"] is False
    assert result["candidates"] == []
    assert set(result["operational_failures"]) == {
        "a_share_full_market_snapshot_unavailable",
        "hk_connect_snapshot_unavailable",
    }


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


def test_actionable_quality_cannot_be_configured_below_intraday_gate() -> None:
    with pytest.raises(ValueError, match="between 0.70 and 1"):
        MarketScanConfig(min_actionable_data_quality=0.69)


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
    assert candidate["simulation_advice"] == "进入买入区后建议首笔建仓2.5%"
    assert candidate["simulated_portfolio_weight"] == pytest.approx(0.025)
    assert candidate["opportunity_tier"] == "standard"
    assert candidate["cash_floor_ratio"] == pytest.approx(0.15)
    assert candidate["max_single_position_ratio"] == pytest.approx(0.15)
    assert candidate["initial_position_fraction"] == pytest.approx(0.025)
    assert candidate["add_position_fraction"] == pytest.approx(0.025)
    assert candidate["eligible_for_intraday_review"] is True
    assert "## 买入候选漏斗" in report
    assert "### 已核验事实" in report
    assert "### 规则推断（不是事实）" in report
    assert "### 审慎观点" in report
    assert "Level-2=unavailable" in report


def test_high_conviction_scan_emits_exceptional_dynamic_sizing(tmp_path: Path) -> None:
    result = _service(
        tmp_path,
        qwen=ReviewRecorder("pass", confidence=0.92),
        deepseek=ReviewRecorder("pass", confidence=0.94),
    ).run()

    candidate = result["candidates"][0]
    assert candidate["rank"] <= 3
    assert candidate["plan"]["net_rr"] >= 2.0
    assert candidate["opportunity_tier"] == "exceptional"
    assert candidate["cash_floor_ratio"] == pytest.approx(0.0)
    assert candidate["max_single_position_ratio"] == pytest.approx(0.50)
    assert candidate["simulated_portfolio_weight"] == pytest.approx(0.10)
    assert candidate["initial_position_fraction"] == pytest.approx(0.10)
    assert candidate["add_position_fraction"] == pytest.approx(0.05)
    assert candidate["opportunity_tier_evidence"] == {
        "rank": candidate["rank"],
        "data_quality": candidate["data_quality"],
        "net_rr": candidate["plan"]["net_rr"],
        "qwen_confidence": 0.92,
        "deepseek_confidence": 0.94,
    }


@pytest.mark.parametrize(
    (
        "rank",
        "data_quality",
        "net_rr",
        "qwen_confidence",
        "deepseek_confidence",
        "expected_tier",
        "expected_cash_floor",
        "expected_position_limit",
        "expected_initial_fraction",
        "expected_add_fraction",
    ),
    [
        (6, 0.80, 3.5, 0.95, 0.95, "standard", 0.15, 0.15, 0.025, 0.025),
        (4, 0.80, 2.0, 0.85, 0.88, "strong", 0.05, 0.35, 0.05, 0.05),
        (3, 0.80, 2.0, 0.90, 0.93, "exceptional", 0.0, 0.50, 0.10, 0.05),
    ],
)
def test_opportunity_tier_controls_soft_cash_floor_and_dynamic_concentration(
    rank: int,
    data_quality: float,
    net_rr: float,
    qwen_confidence: float,
    deepseek_confidence: float,
    expected_tier: str,
    expected_cash_floor: float,
    expected_position_limit: float,
    expected_initial_fraction: float,
    expected_add_fraction: float,
) -> None:
    policy = classify_opportunity(
        rank=rank,
        data_quality=data_quality,
        net_rr=net_rr,
        qwen_confidence=qwen_confidence,
        deepseek_confidence=deepseek_confidence,
    )

    assert policy.tier == expected_tier
    assert policy.cash_floor_ratio == pytest.approx(expected_cash_floor)
    assert policy.max_single_position_ratio == pytest.approx(
        expected_position_limit
    )
    assert policy.initial_position_fraction == pytest.approx(
        expected_initial_fraction
    )
    assert policy.add_position_fraction == pytest.approx(expected_add_fraction)


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


def test_operational_failure_alert_is_deduped_and_recovery_is_sent_once(
    tmp_path: Path,
) -> None:
    failure = {
        "generated_at": NOW.isoformat(),
        "as_of": {MARKET_A: "", MARKET_HK: ""},
        "safe_to_push": False,
        "push_block_reasons": ["全市场快照不可用"],
        "operational_status": "failed",
        "operational_failures": ["a_share_full_market_snapshot_unavailable"],
        "diagnostics": {"buy_funnel": {"snapshot_input_count": 0}},
        "candidates": [],
        "disclaimer": "simulation only",
    }
    calls: list[tuple[str, str]] = []

    first = persist_result_and_notify(
        failure,
        report_path=tmp_path / "scan.md",
        result_path=tmp_path / "scan.json",
        state_dir=tmp_path / "state",
        notify=True,
        notifier=lambda title, content: calls.append((title, content)) or True,
    )
    assert first["notification_kind"] == "failure"
    assert first["notification_sent"] is True
    assert calls[0][0] == "全市场买入链路故障"
    assert "硬止损风险提醒继续有效" in calls[0][1]

    changed_failure = {
        **failure,
        "safe_to_push": True,
        "candidates": [{"code": "600001", "action": "conditional_buy", "plan": {}}],
    }
    second = persist_result_and_notify(
        changed_failure,
        report_path=tmp_path / "scan.md",
        result_path=tmp_path / "scan.json",
        state_dir=tmp_path / "state",
        notify=True,
        notifier=lambda _title, _content: (_ for _ in ()).throw(
            AssertionError("unhealthy candidate changes must not bypass health deduplication")
        ),
    )
    assert second["notification_attempted"] is False

    recovery = {
        **failure,
        "generated_at": (NOW + timedelta(hours=1)).isoformat(),
        "safe_to_push": True,
        "push_block_reasons": [],
        "operational_status": "healthy",
        "operational_failures": [],
        "candidates": [{"code": "600001", "action": "watch", "plan": {}}],
    }
    recovered = persist_result_and_notify(
        recovery,
        report_path=tmp_path / "scan.md",
        result_path=tmp_path / "scan.json",
        state_dir=tmp_path / "state",
        notify=True,
        notifier=lambda title, content: calls.append((title, content)) or True,
    )
    assert recovered["notification_kind"] == "recovery"
    assert calls[-1][0] == "全市场买入链路已恢复"

    stable = persist_result_and_notify(
        recovery,
        report_path=tmp_path / "scan.md",
        result_path=tmp_path / "scan.json",
        state_dir=tmp_path / "state",
        notify=True,
        notifier=lambda _title, _content: (_ for _ in ()).throw(
            AssertionError("same recovery must be deduplicated")
        ),
    )
    assert stable["notification_attempted"] is False


def test_partial_market_degradation_dedupes_health_but_allows_healthy_market_change(
    tmp_path: Path,
) -> None:
    degraded = {
        "generated_at": NOW.isoformat(),
        "as_of": {MARKET_A: NOW.isoformat(), MARKET_HK: ""},
        "safe_to_push": True,
        "push_block_reasons": [],
        "market_block_reasons": {MARKET_A: [], MARKET_HK: ["blocked"]},
        "operational_status": "degraded",
        "operational_failures": ["hk_connect_snapshot_unavailable"],
        "diagnostics": {"buy_funnel": {"snapshot_input_count": 1}},
        "candidates": [{"code": "600001", "action": "watch", "plan": {}}],
        "disclaimer": "simulation only",
    }
    calls: list[tuple[str, str]] = []

    first = persist_result_and_notify(
        degraded,
        report_path=tmp_path / "scan.md",
        result_path=tmp_path / "scan.json",
        state_dir=tmp_path / "state",
        notify=True,
        notifier=lambda title, content: calls.append((title, content)) or True,
    )
    assert first["notification_kind"] == "failure"
    assert calls[-1][0] == "全市场买入链路部分降级"

    changed = {
        **degraded,
        "generated_at": (NOW + timedelta(minutes=30)).isoformat(),
        "candidates": [
            {"code": "600001", "action": "conditional_buy", "plan": {}}
        ],
    }
    second = persist_result_and_notify(
        changed,
        report_path=tmp_path / "scan.md",
        result_path=tmp_path / "scan.json",
        state_dir=tmp_path / "state",
        notify=True,
        notifier=lambda title, content: calls.append((title, content)) or True,
    )
    assert second["notification_kind"] == "candidate_change"
    assert len(calls) == 2


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
    assert "严格确认买入链路健康" in text
    assert "SCAN_EXIT_CODE" in text
    assert "00-daily-analysis.yml" not in text
    assert "01-adaptive-market-monitor.yml" not in text
    assert "broker" not in text.lower()


def test_structured_reviewers_disable_thinking_and_avoid_json_truncation(
    monkeypatch: Any,
) -> None:
    calls: list[Dict[str, Any]] = []

    def fake_completion(**kwargs: Any) -> Any:
        calls.append(kwargs)
        return SimpleNamespace(
            choices=[
                SimpleNamespace(
                    finish_reason="stop",
                    message=SimpleNamespace(
                        content=(
                            '{"reviews":[{"code":"600001","verdict":"watch",'
                            '"confidence":0,"hard_risk":false,"thesis":"",'
                            '"risks":[],"invalidators":[],"facts":[],'
                            '"inferences":[],"view":""}]}'
                        )
                    ),
                )
            ]
        )

    monkeypatch.setitem(
        sys.modules,
        "litellm",
        SimpleNamespace(completion=fake_completion),
    )
    monkeypatch.setenv("MARKET_SCAN_QWEN_MODEL", "qwen3.6-flash")
    monkeypatch.setenv("LLM_DASHSCOPE_API_KEY", "test-qwen-key")
    monkeypatch.setenv(
        "LLM_DASHSCOPE_BASE_URL",
        "https://dashscope.example/compatible-mode/v1",
    )
    monkeypatch.setenv("MARKET_SCAN_DEEPSEEK_MODEL", "deepseek-v4-flash")
    monkeypatch.setenv("LLM_DEEPSEEK_API_KEY", "test-deepseek-key")
    monkeypatch.setenv("LLM_DEEPSEEK_BASE_URL", "https://deepseek.example")

    qwen = build_litellm_reviewer("qwen")
    deepseek = build_litellm_reviewer("deepseek")
    assert qwen is not None
    assert deepseek is not None
    assert len(qwen([{"code": "600001"}])["reviews"]) == 1
    assert len(deepseek([{"code": "600001"}])["reviews"]) == 1

    qwen_kwargs, deepseek_kwargs = calls
    assert qwen_kwargs["extra_body"] == {"enable_thinking": False}
    assert "max_tokens" not in qwen_kwargs
    assert deepseek_kwargs["extra_body"] == {"thinking": {"type": "disabled"}}
    assert deepseek_kwargs["max_tokens"] == 12_000
    assert qwen_kwargs["response_format"] == {"type": "json_object"}
    assert deepseek_kwargs["response_format"] == {"type": "json_object"}
    assert qwen_kwargs["num_retries"] == deepseek_kwargs["num_retries"] == 1


@pytest.mark.parametrize(
    ("finish_reason", "content", "message"),
    [
        (None, '{"reviews":[]}', "did not finish cleanly"),
        ("length", '{"reviews":[]}', "did not finish cleanly"),
        ("stop", "", "empty content"),
        ("stop", "not-json", "no JSON object"),
    ],
)
def test_structured_reviewer_fails_closed_on_incomplete_or_invalid_response(
    monkeypatch: Any,
    finish_reason: Any,
    content: str,
    message: str,
) -> None:
    def fake_completion(**_kwargs: Any) -> Any:
        return SimpleNamespace(
            choices=[
                SimpleNamespace(
                    finish_reason=finish_reason,
                    message=SimpleNamespace(content=content),
                )
            ]
        )

    monkeypatch.setitem(
        sys.modules,
        "litellm",
        SimpleNamespace(completion=fake_completion),
    )
    monkeypatch.setenv("MARKET_SCAN_QWEN_MODEL", "qwen3.6-flash")
    monkeypatch.setenv("LLM_DASHSCOPE_API_KEY", "test-qwen-key")
    reviewer = build_litellm_reviewer("qwen")
    assert reviewer is not None

    with pytest.raises((ValueError, json.JSONDecodeError), match=message):
        reviewer([{"code": "600001"}])


def test_reviewer_smoke_is_isolated_from_scan_state_and_notifications(
    monkeypatch: Any,
) -> None:
    def fake_builder(label: str) -> Any:
        def review(payload: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
            assert payload[0]["smoke_test"] is True
            return {
                "reviews": [
                    {
                        "code": "600000",
                        "verdict": "watch",
                        "confidence": 0.0,
                        "hard_risk": False,
                        "facts": [],
                        "inferences": [],
                        "risks": ["验收占位样本无行情"],
                        "invalidators": [],
                        "thesis": f"{label} smoke",
                        "view": "仅验证格式",
                    }
                ]
            }

        return review

    monkeypatch.setattr(
        market_scan_reviewer_smoke,
        "build_litellm_reviewer",
        fake_builder,
    )
    result = market_scan_reviewer_smoke.run_smoke()

    assert result["success"] is True
    assert result["fetched_market_data"] is False
    assert result["state_mutated"] is False
    assert result["notification_sent"] is False
    assert result["auto_order_enabled"] is False
    assert set(result["checks"]) == {"qwen", "deepseek"}
