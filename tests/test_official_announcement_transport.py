"""Bounded real HTTP transport, without calling any public endpoint."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import multiprocessing
import threading
import time

import pytest
import requests

from src.services.market_scan_research import ResearchBudget
from src.services.research_evidence import announcement_excerpt, cninfo_issuer
from src.services.research_execution import ResearchCallError


@pytest.fixture
def official_server(monkeypatch):
    servers = []
    get, post = requests.get, requests.post

    def start(mode):
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers.get('Content-Length', '0')))
                self.do_GET()

            def do_GET(self):
                if mode == 'headers_pending':
                    time.sleep(2)
                data = json.dumps([{'code': '301262', 'category': 'A股', 'orgId': 'fixture'}]).encode()
                try:
                    self.send_response(403 if mode == 'forbidden' else 200)
                    self.send_header('Content-Length', str(3 * 1024 * 1024 if mode == 'oversized' else len(data)))
                    self.end_headers()
                    self.wfile.flush()
                    if mode in {'oversized', 'body_pending'}:
                        time.sleep(2)
                    self.wfile.write(data)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(self, *_args):
                pass

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.02}, daemon=True)
        thread.start()
        servers.append((server, thread))
        # The source still validates its official URL. Only the test transport
        # destination is loopback; redirects and parser checks remain active.
        local = f'http://127.0.0.1:{server.server_port}/official'
        monkeypatch.setenv('NO_PROXY', '127.0.0.1')
        monkeypatch.setenv('no_proxy', '127.0.0.1')
        monkeypatch.setattr(requests, 'get', lambda url, **kw: get(local, **kw))
        monkeypatch.setattr(requests, 'post', lambda url, **kw: post(local, **kw))

    yield start
    for server, thread in servers:
        server.shutdown()
        server.server_close()
        thread.join(1)


def test_declared_oversized_pdf_is_rejected_before_slow_body_spends_request_budget(official_server):
    official_server('oversized')
    events = []
    budget = ResearchBudget(1.5, 'HK09987', 'announcements', events, request_timeout=1.0)
    with pytest.raises(ResearchCallError, match='announcement_document_too_large'):
        budget.call(lambda: announcement_excerpt('https://static.cninfo.com.cn/fixture.PDF', 'HK09987', ''),
                    'official_document:fixture', reserve_for_fallback=False)
    assert len(events) == 1 and events[0]['timeout_budget'] == 1.0
    phases = [p['phase'] for p in events[0]['execution']['phases']]
    assert 'official_declared_size_rejected' in phases
    assert 'official_body_read_started' not in phases
    assert not events[0]['execution']['deadline_exceeded']
    assert not multiprocessing.active_children()


@pytest.mark.parametrize('mode', ['ok', 'headers_pending', 'body_pending', 'forbidden'])
def test_official_http_phases_survive_success_failure_and_hard_deadline(official_server, mode):
    official_server(mode)
    events = []
    budget = ResearchBudget(1.5, '301262', 'announcements', events, request_timeout=1.0)
    callback = lambda: cninfo_issuer('301262')
    if mode == 'ok':
        assert budget.call(callback, 'cninfo:issuer_lookup:301262', reserve_for_fallback=False) == 'fixture'
    else:
        with pytest.raises(ResearchCallError, match='HTTPError' if mode == 'forbidden' else 'TimeoutError'):
            budget.call(callback, 'cninfo:issuer_lookup:301262', reserve_for_fallback=False)
    assert len(events) == 1  # No new retries, and the original request cap remains.
    execution = events[0]['execution']
    phases = [p['phase'] for p in execution['phases']]
    assert 'official_request_started' in phases
    assert ('official_headers_received' in phases) == (mode != 'headers_pending')
    assert ('official_body_read_started' in phases) == (mode in {'ok', 'body_pending'})
    assert ('official_json_decode_completed' in phases) == (mode == 'ok')
    if mode == 'forbidden':
        assert 'official_http_status_403' in phases
    assert execution['deadline_exceeded'] == (mode in {'headers_pending', 'body_pending'})
    assert all(set(p) == {'phase', 'elapsed_seconds'} for p in execution['phases'])
    assert len(phases) <= 64 and '127.0.0.1' not in json.dumps(execution)
    assert not multiprocessing.active_children()


@pytest.mark.parametrize('headers', [
    {}, {'Content-Length': '1'}, {'Content-Length': '-1'}, {'Content-Length': 'not-a-size'},
    {'Content-Length': '2097153,2097153'},
    {'Content-Length': '2097153', 'Content-Encoding': 'gzip'},
    {'Content-Length': '2097153', 'Transfer-Encoding': 'chunked'},
])
@pytest.mark.parametrize('kind', ['document', 'index'])
def test_missing_false_or_encoded_lengths_never_bypass_stream_cap(monkeypatch, headers, kind):
    from tests.test_official_announcement_excerpt import Response
    from src.services.research_evidence import cninfo_issuers

    response = Response(b'%PDF' + b'x' * (2 * 1024 * 1024))
    response.headers = headers
    read = []
    def chunks(size):
        read.append(size)
        yield response.data
    response.iter_content = chunks
    monkeypatch.setattr(requests, 'get', lambda *a, **kw: response)
    with pytest.raises(ValueError, match=f'announcement_{kind}_too_large'):
        if kind == 'document':
            announcement_excerpt('https://static.cninfo.com.cn/fixture.PDF', '301262', '')
        else:
            cninfo_issuers(False)
    assert read == [32768]


@pytest.mark.parametrize('kind', ['document', 'index'])
def test_clear_oversized_length_rejects_without_reading_body(monkeypatch, kind):
    from tests.test_official_announcement_excerpt import Response
    from src.services.research_evidence import cninfo_issuers

    response = Response(b'')
    response.headers = {'Content-Length': '2097153', 'Content-Encoding': 'identity'}
    def forbidden(*args, **kwargs):
        raise AssertionError('A declared oversized body must not be downloaded')
    response.iter_content = forbidden
    monkeypatch.setattr(requests, 'get', lambda *a, **kw: response)
    with pytest.raises(ValueError, match=f'announcement_{kind}_too_large'):
        if kind == 'document':
            announcement_excerpt('https://static.cninfo.com.cn/fixture.PDF', '301262', '')
        else:
            cninfo_issuers(False)


def test_accepted_real_pdf_keeps_original_text_identity_hash_and_phase_diagnostics(monkeypatch):
    import hashlib
    from tests.test_official_announcement_cid import cid_pdf
    from tests.test_official_announcement_excerpt import Response

    data = cid_pdf()
    response = Response(data)
    response.headers = {'Content-Length': str(len(data))}
    monkeypatch.setattr(requests, 'get', lambda *a, **kw: response)
    events = []
    budget = ResearchBudget(3.0, 'HK02020', 'announcements', events, request_timeout=2.0)
    document = budget.call(lambda: announcement_excerpt('https://static.cninfo.com.cn/fixture.PDF', 'HK02020', ''),
                           'official_document:fixture', reserve_for_fallback=False)
    assert document['document_sha256'] == hashlib.sha256(data).hexdigest()
    assert document['document_bytes'] == len(data)
    assert document['body_excerpt_available'] and not document['content_reviewed']
    assert document['text_extractor'] == 'pdfminer.six_adobe_gb1'
    phases = [p['phase'] for p in events[0]['execution']['phases']]
    assert 'official_body_read_completed' in phases
    assert 'official_pdf_parse_completed' in phases and 'official_pdf_text_completed' in phases
    with pytest.raises(ResearchCallError, match='announcement_document_identity_mismatch'):
        budget.call(lambda: announcement_excerpt('https://static.cninfo.com.cn/fixture.PDF', 'HK02021', ''),
                    'official_document:other-issuer', reserve_for_fallback=False)
