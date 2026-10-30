"""Exercise the real SDK in a fresh interpreter, outside unit-test SDK stubs."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


SDK_PROBE = r'''
import json
import multiprocessing
import os
from pathlib import Path
import sys
import threading

import httpx
import litellm
import openai
from litellm.litellm_core_utils.litellm_logging import Logging
from scripts import market_scan

root = Path(sys.argv[1])
invalid_first = sys.argv[2] == "invalid_first"
wire = root / "wire.jsonl"
logger_started = root / "logger_started"
logging_entered = threading.Event()

def handle(request):
    body = json.loads(request.content)
    content = json.loads(body["messages"][1]["content"])
    with wire.open("a") as output:
        output.write(json.dumps({"path": request.url.path, "body": body}) + "\n")
    reviews = []
    for index, candidate in enumerate(content["candidates"]):
        review = {"code": candidate["code"], "verdict": "watch", "hard_risk": False,
                  "numeric_facts": candidate["numeric_fact_contract"]["values"]}
        if index == 0:
            review["facts"] = ["现价8" if invalid_first and not content.get("retry_feedback")
                               else "现价12.3"]
        elif index == 1:
            review.update(verdict="reject", hard_risk=True)
        reviews.append(review)
    return httpx.Response(200, request=request, json={
        "id": "offline-response", "object": "chat.completion", "created": 1,
        "model": body["model"], "choices": [{"index": 0, "finish_reason": "stop",
        "message": {"role": "assistant", "content": json.dumps({"reviews": reviews})}}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20},
    })

def blocked_sdk_log(self, *args, **kwargs):
    logger_started.write_text("real SDK logging executor started")
    logging_entered.set()
    threading.Event().wait()

Logging.success_handler = blocked_sdk_log
litellm.telemetry = False
client = openai.OpenAI(api_key="offline-placeholder", base_url="https://offline.invalid/compatible-mode/v1",
    http_client=httpx.Client(transport=httpx.MockTransport(handle)))
real_completion = litellm.completion
def completion(**kwargs):
    response = real_completion(client=client, **kwargs)
    assert logging_entered.wait(1), "SDK logging executor never started"
    return response
litellm.completion = completion
os.environ.update(MARKET_SCAN_QWEN_MODEL="qwen3.8-max", LLM_DASHSCOPE_API_KEY="offline-placeholder",
    LLM_DASHSCOPE_BASE_URL="https://offline.invalid/compatible-mode/v1",
    LLM_DASHSCOPE_MODELS="qwen3.8-max,qwen3.7-max")
market_scan.REVIEW_REQUEST_TIMEOUT_SECONDS = 2.0
market_scan.REVIEW_TOTAL_TIMEOUT_SECONDS = 8.0
market_scan.REVIEW_RETRY_BACKOFF_SECONDS = 0.01
reviewer = market_scan.build_litellm_reviewer("qwen")
outcome = {}
candidates = [{"code": str(600001 + i), "price": 12.3} for i in range(4)]
def run():
    try:
        outcome["reviews"] = reviewer(candidates)["reviews"]
    except Exception as error:
        outcome["error"] = str(error)

# Match the production reviewer thread -> killable process boundary.
thread = threading.Thread(target=run, daemon=True)
thread.start()
thread.join(8)
assert not thread.is_alive(), "reviewer thread did not finish"
assert "error" not in outcome, outcome
assert [r["code"] for r in outcome["reviews"]] == [c["code"] for c in candidates]
assert outcome["reviews"][0]["verdict"] == "watch"
assert outcome["reviews"][1]["verdict"] == "reject"
assert outcome["reviews"][1]["hard_risk"] is True
assert logger_started.exists()
assert not multiprocessing.active_children()
calls = [json.loads(line) for line in wire.read_text().splitlines()]
assert len(calls) == (2 if invalid_first else 1)
for call in calls:
    assert call["path"] == "/compatible-mode/v1/chat/completions"
    assert call["body"]["model"] == "qwen3.8-max"
    assert call["body"]["enable_thinking"] is False
    assert not call["body"].get("stream", False)
requests = reviewer.diagnostics["requests"]
if invalid_first:
    assert requests[0]["error_code"] == "response_numeric_fact_mismatch"
    assert requests[1]["retry_feedback"]["numeric_errors"]["600001"][0]["expected"] == 12.3
    assert json.loads(calls[0]["body"]["messages"][1]["content"])["candidates"] == (
        json.loads(calls[1]["body"]["messages"][1]["content"])["candidates"])
assert all(r["model_role"] == "primary" for r in requests)
print(json.dumps({"request_count": len(calls), "completed": len(outcome["reviews"])}))
'''


@pytest.mark.parametrize("scenario", ["valid", "invalid_first"])
def test_real_qwen_sdk_logger_cannot_hold_validated_worker_open(tmp_path, scenario):
    env = {**os.environ, "LITELLM_LOCAL_MODEL_COST_MAP": "True", "HF_HUB_OFFLINE": "1",
           "DO_NOT_TRACK": "1"}
    result = subprocess.run(
        [sys.executable, "-c", SDK_PROBE, str(tmp_path), scenario],
        cwd=Path(__file__).resolve().parents[1], env=env,
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout.splitlines()[-1]) == {
        "request_count": 2 if scenario == "invalid_first" else 1, "completed": 4,
    }
