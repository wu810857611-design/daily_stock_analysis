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


def configure(monkeypatch, completion, *, request_timeout=0.25):
    monkeypatch.setitem(sys.modules, "litellm", SimpleNamespace(completion=completion))
    monkeypatch.setenv("MARKET_SCAN_DEEPSEEK_MODEL", "offline-primary")
    monkeypatch.setenv("LLM_DEEPSEEK_API_KEY", "never-send-offline-secret")
    monkeypatch.setenv("LLM_DEEPSEEK_MODELS", "offline-primary,offline-fallback")
    monkeypatch.setattr(market_scan, "REVIEW_REQUEST_TIMEOUT_SECONDS", request_timeout)
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
    assert error["text_field"] == "facts" and error["claim_context"] == "日涨27.97%"
    assert all(request["retry_feedback"]["numeric_errors"]["HK01548"][0]["expected"] == 0.27972
               for request in reviewer.diagnostics["requests"][1:])
    if not recovers:
        assert calls.read_text() == "3"
        assert reviewer.diagnostics["requests"][-1]["model_role"] == "fallback"
    assert not multiprocessing.active_children()


@pytest.mark.parametrize("error_count", [1, 12])
def test_numeric_retry_gets_safe_feedback_without_changing_evidence_or_later_batches(monkeypatch, tmp_path, error_count):
    """The real worker must receive the failure it is being asked to correct."""
    captured = tmp_path / "requests.jsonl"

    def completion(**kwargs):
        body = json.loads(kwargs["messages"][1]["content"])
        with captured.open("a") as handle:
            handle.write(json.dumps(body, ensure_ascii=False) + "\n")
        reviews = []
        for item in body["candidates"]:
            review = {"code": item["code"], "verdict": "watch", "hard_risk": False,
                      "numeric_facts": item["numeric_fact_contract"]["values"]}
            if item["code"] == "600865":
                review["risks"] = (["公告提示估值偏高，与当前快照存在差异，需核实后继续观察。"]
                                   if body.get("retry_feedback") else
                                   ["2026-10-28公告市盈率100.24倍；UNTRUSTED_RESPONSE_TEXT"] * error_count)
            if item["code"] == "600002":
                review.update(verdict="reject", hard_risk=True)
            reviews.append(review)
        return {"choices": [{"finish_reason": "stop", "message": {
            "content": json.dumps({"reviews": reviews}, ensure_ascii=False)}}]}

    configure(monkeypatch, completion, request_timeout=1.0)
    candidates = [{"code": "600865", "pe": 218.41, "price": 12.3,
                   "research_evidence": {"announcements": {"items": [{
                       "published_date": "2026-10-28", "excerpt": "公司最新市盈率为100.24"
                   }]}}}] + [{"code": str(600001 + i)} for i in range(1, 8)]
    original = json.dumps(candidates, ensure_ascii=False, sort_keys=True)
    reviewer = market_scan.build_litellm_reviewer("deepseek")
    result = reviewer(candidates)
    assert len(result["reviews"]) == len(candidates)
    assert result["reviews"][0]["verdict"] == "watch"
    assert result["reviews"][1]["verdict"] == "reject"
    assert result["reviews"][1]["hard_risk"] is True
    assert json.dumps(candidates, ensure_ascii=False, sort_keys=True) == original
    bodies = [json.loads(line) for line in captured.read_text().splitlines()]
    assert len(bodies) == 3  # Failed batch, corrected retry, independent next batch.
    assert bodies[0]["candidates"] == bodies[1]["candidates"]
    assert "retry_feedback" not in bodies[0] and "retry_feedback" not in bodies[2]
    feedback = bodies[1]["retry_feedback"]
    assert feedback["error_code"] == "response_numeric_fact_mismatch"
    assert feedback["expected_codes"] == [item["code"] for item in candidates[:4]]
    assert feedback["numeric_errors"]["600865"] == [{
        "field": "pe", "error": "snapshot_claim_mismatch", "expected": 218.41,
    }] * min(error_count, 8)
    assert "UNTRUSTED_RESPONSE_TEXT" not in json.dumps(feedback)
    requests = reviewer.diagnostics["requests"]
    assert len(requests[0]["numeric_errors"]["600865"]) == error_count
    assert requests[0]["numeric_errors"]["600865"][0]["observed"] == 100.24
    assert "UNTRUSTED_RESPONSE_TEXT" in requests[0]["numeric_errors"]["600865"][0]["claim_context"]
    assert requests[1]["retry_feedback"] == feedback
    assert "retry_feedback" not in requests[2]
    assert not multiprocessing.active_children()


def test_validation_feedback_survives_transport_failure_to_same_channel_fallback(monkeypatch, tmp_path):
    calls = tmp_path / "fallback_requests.jsonl"

    def completion(**kwargs):
        body = json.loads(kwargs["messages"][1]["content"])
        with calls.open("a") as handle:
            handle.write(json.dumps({"model": kwargs["model"], "body": body}) + "\n")
        attempt = len(calls.read_text().splitlines())
        if attempt == 2:
            raise TimeoutError("never-send-offline-secret")
        item = body["candidates"][0]
        return {"choices": [{"finish_reason": "stop", "message": {"content": json.dumps({
            "reviews": [{"code": item["code"], "verdict": "watch",
                         "numeric_facts": item["numeric_fact_contract"]["values"],
                         "facts": ["现价8" if attempt == 1 else "现价12.3"]}],
        })}}]}

    configure(monkeypatch, completion, request_timeout=1.0)
    reviewer = market_scan.build_litellm_reviewer("deepseek")
    assert reviewer([{"code": "600865", "price": 12.3}])["reviews"][0]["verdict"] == "watch"
    requests = [json.loads(line) for line in calls.read_text().splitlines()]
    assert [item["model"] for item in requests] == [
        "deepseek/offline-primary", "deepseek/offline-primary", "deepseek/offline-fallback",
    ]
    assert requests[1]["body"]["retry_feedback"] == requests[2]["body"]["retry_feedback"]
    assert requests[2]["body"]["retry_feedback"]["numeric_errors"]["600865"][0]["expected"] == 12.3
    assert reviewer.diagnostics["requests"][1]["error_code"] == "provider_timeout"
    assert reviewer.diagnostics["batches"][0]["fallback_used"] is True
    assert "never-send-offline-secret" not in json.dumps(reviewer.diagnostics)
    assert not multiprocessing.active_children()


@pytest.mark.parametrize("failure", ["response_code_mismatch", "response_json_invalid"])
def test_shape_retry_uses_expected_codes_without_echoing_invalid_model_output(monkeypatch, tmp_path, failure):
    captured = tmp_path / "shape_requests.jsonl"

    def completion(**kwargs):
        body = json.loads(kwargs["messages"][1]["content"])
        with captured.open("a") as handle:
            handle.write(json.dumps(body) + "\n")
        if "retry_feedback" not in body:
            content = ('{"reviews":[{"code":"UNTRUSTED_MODEL_CODE","verdict":"watch"}]}'
                       if failure == "response_code_mismatch" else '{"reviews":[}')
            return {"choices": [{"finish_reason": "stop", "message": {"content": content}}]}
        return response(kwargs)

    configure(monkeypatch, completion, request_timeout=1.0)
    reviewer = market_scan.build_litellm_reviewer("deepseek")
    assert [item["code"] for item in reviewer([{"code": "600001"}, {"code": "600002"}])["reviews"]] == [
        "600001", "600002",
    ]
    requests = [json.loads(line) for line in captured.read_text().splitlines()]
    assert len(requests) == 2
    assert requests[1]["retry_feedback"] == {"error_code": failure, "expected_codes": ["600001", "600002"]}
    assert "UNTRUSTED_MODEL_CODE" not in json.dumps(requests[1])
    assert not multiprocessing.active_children()
