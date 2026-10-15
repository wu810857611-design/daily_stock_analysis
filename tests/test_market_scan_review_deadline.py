"""Real worker boundary regressions for the 2026-10-15 blocked SDK request."""
import json
import multiprocessing
import sys
import threading
import time
from types import SimpleNamespace

import pytest
import requests  # noqa: F401 - pre-import the worker's transport dependency.

from scripts import market_scan
from src.services.research_execution import run_isolated


def configure(monkeypatch, completion):
    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))
    monkeypatch.setenv("MARKET_SCAN_DEEPSEEK_MODEL", "offline-primary")
    monkeypatch.setenv("LLM_DEEPSEEK_API_KEY", "never-send-offline-secret")
    monkeypatch.setenv("LLM_DEEPSEEK_MODELS", "offline-primary,offline-fallback")
    monkeypatch.setattr(market_scan, "REVIEW_REQUEST_TIMEOUT_SECONDS", 0.25)
    monkeypatch.setattr(market_scan, "REVIEW_TOTAL_TIMEOUT_SECONDS", 5.0)
    monkeypatch.setattr(market_scan, "REVIEW_RETRY_BACKOFF_SECONDS", 0.01)


def response(kwargs):
    batch = json.loads(kwargs["messages"][1]["content"])["candidates"]
    return {"choices": [{"finish_reason": "stop", "message": {"content": json.dumps({
        "reviews": [{"code": c["code"], "verdict": "watch", "numeric_facts": {}}
                    for c in batch],
    })}}]}


def test_sdk_ignoring_timeout_is_reaped_then_configured_fallback_completes_all_batches(monkeypatch, tmp_path):
    attempts = tmp_path / "attempts"
    def completion(**kwargs):
        with attempts.open("a") as f:
            f.write(kwargs["model"] + "\n")
        if kwargs["model"].endswith("offline-primary"):
            threading.Event().wait()  # Ignores the SDK timeout argument entirely.
        return response(kwargs)
    configure(monkeypatch, completion)
    reviewer = market_scan.build_litellm_reviewer("deepseek")
    candidates = [{"code": str(600001 + i)} for i in range(12)]
    # Production invokes reviewers from a daemon thread, not the main thread.
    result = []
    worker = threading.Thread(target=lambda: result.append(reviewer(candidates)), daemon=True)
    started = time.monotonic()
    worker.start()
    worker.join(5)
    assert not worker.is_alive()
    assert len(result[0]["reviews"]) == 12
    assert time.monotonic() - started < 5
    assert attempts.read_text().splitlines() == [
        item for _ in range(3) for item in [
            "deepseek/offline-primary", "deepseek/offline-primary", "deepseek/offline-fallback",
        ]
    ]
    assert reviewer.diagnostics["completed_candidate_count"] == 12
    assert all(b["fallback_used"] for b in reviewer.diagnostics["batches"])
    assert all(r["status"] != "running" for r in reviewer.diagnostics["requests"])
    assert {r.get("error_code") for r in reviewer.diagnostics["requests"] if r["status"] == "failed"} == {"provider_timeout"}
    assert "never-send-offline-secret" not in json.dumps(reviewer.diagnostics)
    assert not multiprocessing.active_children()


def test_all_hanging_models_remain_failed_closed_with_terminal_diagnostics(monkeypatch):
    configure(monkeypatch, lambda **kwargs: threading.Event().wait())
    reviewer = market_scan.build_litellm_reviewer("deepseek")
    with pytest.raises(RuntimeError, match="provider_timeout"):
        reviewer([{"code": "600001"}])
    assert reviewer.diagnostics["status"] == "failed"
    assert reviewer.diagnostics["completed_candidate_count"] == 0
    assert reviewer.diagnostics["request_count"] == 3
    assert all(r["status"] == "failed" for r in reviewer.diagnostics["requests"])
    assert not multiprocessing.active_children()


def test_isolation_preserves_fatal_provider_status_without_retry_or_secret(monkeypatch):
    def completion(**kwargs):
        exc = RuntimeError("never-send-offline-secret")
        exc.status_code = 401
        raise exc
    configure(monkeypatch, completion)
    reviewer = market_scan.build_litellm_reviewer("deepseek")
    with pytest.raises(RuntimeError, match="provider_http_401"):
        reviewer([{"code": "600001"}])
    assert reviewer.diagnostics["request_count"] == 1
    assert reviewer.diagnostics["requests"][0]["status_code"] == 401
    assert "never-send-offline-secret" not in json.dumps(reviewer.diagnostics)


def test_review_network_cap_is_not_replaced_by_research_four_second_default(monkeypatch):
    import socket
    assert run_isolated(lambda: socket.getdefaulttimeout(), 10) == 4
    assert run_isolated(lambda: socket.getdefaulttimeout(), 1, network_timeout_seconds=20) == 1
    assert run_isolated(lambda: socket.getdefaulttimeout(), 10, network_timeout_seconds=8) == 8
    monkeypatch.setattr(requests.sessions.Session, "request", lambda self, *args, **kwargs: kwargs["timeout"])
    assert run_isolated(lambda: requests.get("https://offline.invalid"), 10) == (3, 4)
    assert run_isolated(lambda: requests.get("https://offline.invalid"), 10,
                        network_timeout_seconds=8) == (8, 8)


@pytest.mark.parametrize("recovers", [True, False])
def test_real_worker_preserves_numeric_rejection_and_retries_without_relaxing_validation(monkeypatch, tmp_path, recovers):
    calls = tmp_path / "numeric_attempts"
    def completion(**kwargs):
        count = int(calls.read_text()) + 1 if calls.exists() else 1
        calls.write_text(str(count))
        batch = json.loads(kwargs["messages"][1]["content"])["candidates"]
        item = batch[0]
        return {"choices": [{"finish_reason": "stop", "message": {"content": json.dumps({
            "reviews": [{"code": item["code"], "verdict": "pass",
                         "numeric_facts": item["numeric_fact_contract"]["values"],
                         "facts": ["日涨0.28%" if recovers and count > 1 else "日涨27.97%"]}],
        })}}]}
    configure(monkeypatch, completion)
    reviewer = market_scan.build_litellm_reviewer("deepseek")
    candidate = {"code": "HK01548", "price": 43.02, "change_pct": 0.27972, "amount": 1649000000}
    if recovers:
        assert reviewer([candidate])["reviews"][0]["facts"] == ["日涨0.28%"]
        assert calls.read_text() == "2"
    else:
        with pytest.raises(ValueError, match="response_numeric_fact_mismatch"):
            reviewer([candidate])
        assert reviewer.diagnostics["completed_candidate_count"] == 0
    error = reviewer.diagnostics["requests"][0]["numeric_errors"]["HK01548"][0]
    assert error["field"] == "change_pct" and error["expected"] == 0.27972
    assert not multiprocessing.active_children()
