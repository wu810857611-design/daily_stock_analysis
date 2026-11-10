"""Exercise stage evidence through a real worker and real loopback HTTP transport."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import multiprocessing
import threading
import time

import httpx
import pytest

from scripts import market_scan
from tests.test_market_scan_review_deadline import configure, response
from tests.test_market_scan_service import ReviewRecorder, _review_response, _service


@pytest.fixture
def loopback_server():
    servers = []

    def start(mode, *, on_request=None):
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                if on_request is not None:
                    on_request()
                if mode == 'headers_pending':
                    time.sleep(3)
                try:
                    self.send_response(200)
                    self.send_header('Content-Length', str(len(body)))
                    self.end_headers()
                    self.wfile.flush()
                    if mode == 'body_pending':
                        time.sleep(3)
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(self, *_args):
                pass

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.02}, daemon=True)
        thread.start()
        servers.append((server, thread))
        return f'http://127.0.0.1:{server.server_port}/private-path?secret=never-log-this'

    yield start
    for server, thread in servers:
        server.shutdown()
        server.server_close()
        thread.join(1)


@pytest.mark.parametrize('label', ['qwen', 'deepseek'])
@pytest.mark.parametrize('mode', ['ok', 'headers_pending', 'body_pending', 'sdk_after_http'])
def test_real_transport_phases_distinguish_pending_http_and_post_http_sdk(monkeypatch, loopback_server, label, mode):
    url = loopback_server(mode)
    original_send = httpx.Client.send

    def completion(**kwargs):
        assert kwargs['stream'] is False and kwargs['num_retries'] == 0
        with httpx.Client(trust_env=False, timeout=kwargs['timeout']) as client:
            result = client.post(url, json=response(kwargs), headers={'Authorization': 'Bearer never-log-this'})
        if mode == 'sdk_after_http':
            threading.Event().wait()
        return result.json()

    configure(monkeypatch, completion, request_timeout=0.4)
    monkeypatch.setenv('MARKET_SCAN_QWEN_MODEL', 'offline-primary')
    monkeypatch.setenv('LLM_DASHSCOPE_API_KEY', 'never-send-offline-secret')
    monkeypatch.setenv('LLM_DASHSCOPE_MODELS', 'offline-primary,offline-fallback')
    reviewer = market_scan.build_litellm_reviewer(label)
    if mode == 'ok':
        assert reviewer([{'code': '600001'}])['reviews'][0]['verdict'] == 'watch'
        assert reviewer.diagnostics['completed_candidate_count'] == 1
    else:
        with pytest.raises(RuntimeError, match='provider_timeout'):
            reviewer([{'code': '600001'}])
        assert reviewer.diagnostics['completed_candidate_count'] == 0
        assert reviewer.diagnostics['request_count'] == 3
    for request in reviewer.diagnostics['requests']:
        assert request['started_at'].endswith('+08:00')
        evidence = request['execution']
        phases = [event['phase'] for event in evidence['phases']]
        assert 'sdk_call_started' in phases
        assert 'http11.receive_response_headers.started' in phases
        if mode == 'ok':
            assert 'http11.receive_response_body.complete' in phases
            assert 'response_validation_completed' in phases
            assert 'result_closed' in phases and evidence['last_phase'] == 'exit_requested'
            assert evidence['worker_exitcode'] == 0 and not evidence['deadline_exceeded']
            assert evidence['result_file_bytes'] > 0
        else:
            assert evidence['deadline_exceeded'] and evidence['result_file_bytes'] == 0
            assert 'response_validation_completed' not in phases
            assert ('http11.receive_response_headers.complete' in phases) == (mode != 'headers_pending')
            assert ('http11.receive_response_body.complete' in phases) == (mode == 'sdk_after_http')
        assert len(phases) <= 64
        assert all(set(event) == {'phase', 'elapsed_seconds'} for event in evidence['phases'])
    assert 'never-log-this' not in json.dumps(reviewer.diagnostics)
    assert 'never-send-offline-secret' not in json.dumps(reviewer.diagnostics)
    assert httpx.Client.send is original_send
    assert not multiprocessing.active_children()


def test_header_timeout_does_not_serialize_the_other_reviewer(monkeypatch, tmp_path, loopback_server):
    qwen_sent = threading.Event()
    deepseek_arrival = []
    qwen_arrival = []

    def qwen_received():
        qwen_arrival.append(time.monotonic())
        qwen_sent.set()

    def deepseek_received():
        assert qwen_sent.wait(1)
        deepseek_arrival.append(time.monotonic())

    qwen_url = loopback_server('headers_pending', on_request=qwen_received)
    deepseek_url = loopback_server('ok', on_request=deepseek_received)

    def completion(**kwargs):
        url = qwen_url if kwargs['model'].startswith('openai/') else deepseek_url
        candidates = json.loads(kwargs['messages'][1]['content'])['candidates']
        with httpx.Client(trust_env=False, timeout=kwargs['timeout']) as client:
            return client.post(url, json=_review_response(candidates)).json()

    configure(monkeypatch, completion, request_timeout=0.8)
    monkeypatch.setenv('MARKET_SCAN_QWEN_MODEL', 'offline-primary')
    monkeypatch.setenv('LLM_DASHSCOPE_API_KEY', 'never-send-offline-secret')
    monkeypatch.setenv('LLM_DASHSCOPE_MODELS', 'offline-primary,offline-fallback')
    qwen = market_scan.build_litellm_reviewer('qwen')
    deepseek = market_scan.build_litellm_reviewer('deepseek')
    result = _service(tmp_path, qwen=qwen, deepseek=deepseek).run()

    assert qwen_arrival[0] <= deepseek_arrival[0] < qwen_arrival[1]
    assert [r['model_role'] for r in qwen.diagnostics['requests']] == ['primary', 'primary', 'fallback']
    assert all(r['error_code'] == 'provider_timeout' for r in qwen.diagnostics['requests'])
    assert qwen.diagnostics['requests'][0]['execution']['last_phase'] == 'http11.receive_response_headers.started'
    assert deepseek.diagnostics['completed_candidate_count'] == len(result['candidates']) > 0
    assert result['review_errors'] == {'qwen': 'qwen_review_failed:provider_timeout'}
    assert result['safe_to_push'] is False
    assert not any(c['eligible_for_intraday_review'] for c in result['candidates'])
    assert not multiprocessing.active_children()


def test_review_stage_deadline_caps_the_worker_and_prevents_another_attempt(monkeypatch, tmp_path, loopback_server):
    url = loopback_server('headers_pending')

    def completion(**kwargs):
        with httpx.Client(trust_env=False, timeout=kwargs['timeout']) as client:
            return client.post(url, json=response(kwargs)).json()

    configure(monkeypatch, completion, request_timeout=3.0)
    deepseek = market_scan.build_litellm_reviewer('deepseek')
    result = _service(tmp_path, qwen=ReviewRecorder(), deepseek=deepseek,
                      config_overrides={'reviewer_stage_timeout_seconds': 0.8}).run()

    # The child can still be completing bounded process-group cleanup when the
    # scan hits its deadline. It must not keep the original three-second budget.
    cleanup_deadline = time.monotonic() + 0.75
    while deepseek.diagnostics.get('status') == 'running' and time.monotonic() < cleanup_deadline:
        time.sleep(0.01)
    assert deepseek.diagnostics['status'] == 'failed'
    assert deepseek.diagnostics['request_count'] == 1
    request = deepseek.diagnostics['requests'][0]
    assert 0 < request['timeout_seconds'] <= 0.8
    assert request['error_code'] == 'provider_timeout'
    assert result['safe_to_push'] is False
    assert not multiprocessing.active_children()
