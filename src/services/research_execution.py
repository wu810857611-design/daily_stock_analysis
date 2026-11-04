"""Killable call boundary for scan-only, read-only third-party research.

Unlike a thread join, a deadline here also releases the work that timed out.
The Linux Actions runner supports fork; unsupported platforms fail explicitly
instead of silently falling back to leaking daemon threads.
"""
from __future__ import annotations

import json
import multiprocessing
import os
import pickle
import signal
import socket
import tempfile
import time
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Callable


_PROGRESS: ContextVar[dict | None] = ContextVar("scan_execution_progress", default=None)
_CALL_PHASES = {
    "worker_started", "transport_setup_started", "callback_started", "callback_completed",
    "worker_error", "sdk_import_started", "sdk_ready", "sdk_call_started",
    "sdk_call_returned", "sdk_call_failed", "response_validation_started",
    "response_validation_completed", "review_failed", "result_serializing",
    "result_serialized", "result_writing", "result_closed", "exit_requested",
    "http_send_started", "http_send_returned", "http_send_failed",
}
_HTTP_PHASES = {
    f"{event}.{state}"
    for event in (
        "connection.connect_tcp", "connection.connect_unix_socket", "connection.start_tls",
        "http11.send_request_headers", "http11.send_request_body", "http11.receive_response_headers",
        "http11.receive_response_body", "http11.response_closed", "http2.send_connection_init",
        "http2.send_request_headers", "http2.send_request_body", "http2.receive_response_headers",
        "http2.receive_response_body", "http2.response_closed",
    )
    for state in ("started", "complete", "failed")
}


def record_isolated_phase(phase: str) -> None:
    """Persist only fixed phase names and timings; never HTTP info, bodies or keys."""
    progress = _PROGRESS.get()
    if progress is None or not isinstance(phase, str) or phase not in _CALL_PHASES | _HTTP_PHASES:
        return
    events = progress["events"]
    events.append({"phase": phase, "elapsed_seconds": round(time.monotonic() - progress["started"], 6)})
    del events[:-64]
    path = Path(progress["path"])
    try:
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps({"phases": events, "last_phase": phase}), encoding="utf-8")
        temporary.replace(path)
    except OSError:
        # An unavailable diagnostic must not change the provider result or deadline.
        pass


def _trace_httpx() -> None:
    """Observe the synchronous SDK transport in this disposable worker only."""
    import httpx

    original_send = httpx.Client.send

    def traced_send(client: Any, request: Any, *args: Any, **kwargs: Any) -> Any:
        previous_trace = request.extensions.get("trace")

        def trace(event: str, info: dict) -> None:
            record_isolated_phase(event)  # Deliberately discard the potentially sensitive info.
            if previous_trace is not None:
                previous_trace(event, info)

        request.extensions["trace"] = trace
        record_isolated_phase("http_send_started")
        try:
            response = original_send(client, request, *args, **kwargs)
        except BaseException:
            record_isolated_phase("http_send_failed")
            raise
        record_isolated_phase("http_send_returned")
        return response

    httpx.Client.send = traced_send


class ResearchCallError(RuntimeError):
    def __init__(self, error_class: str, detail: str = "", evidence_diagnostics: dict | None = None) -> None:
        self.error_class = error_class
        self.evidence_diagnostics = evidence_diagnostics or {}
        super().__init__(f"{error_class}: {detail}"[:500])


def _execute(callback: Callable[[], Any], path: str, budget: float,
             network_timeout: float | None, progress_path: str | None, started: float) -> None:
    os.setsid()
    if progress_path is not None:
        _PROGRESS.set({"path": progress_path, "started": started, "events": []})
    record_isolated_phase("worker_started")
    # Request defaults live only in this disposable process. SDKs that bypass
    # requests are still subject to the parent process's total deadline.
    read_limit = min(4.0 if network_timeout is None else network_timeout, budget)
    socket.setdefaulttimeout(read_limit)
    import requests

    original_request = requests.sessions.Session.request

    def bounded_request(self: Any, *args: Any, **kwargs: Any) -> Any:
        configured = kwargs.get("timeout")
        limits = (min(3.0, budget) if network_timeout is None else read_limit, read_limit)
        if isinstance(configured, (int, float)):
            limits = tuple(min(item, configured) for item in limits)
        elif isinstance(configured, tuple) and len(configured) == 2:
            limits = tuple(min(item, value) if value is not None else item
                           for item, value in zip(limits, configured))
        kwargs["timeout"] = limits
        return original_request(self, *args, **kwargs)

    requests.sessions.Session.request = bounded_request
    if progress_path is not None:
        record_isolated_phase("transport_setup_started")
        _trace_httpx()
    try:
        record_isolated_phase("callback_started")
        payload = ("ok", callback())
        record_isolated_phase("callback_completed")
        record_isolated_phase("result_serializing")
        serialized = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
        if len(serialized) > 16 * 1024 * 1024:
            raise ValueError("research result exceeds 16 MiB")
    except BaseException as exc:
        record_isolated_phase("worker_error")
        # Only the official PDF reader attaches this bounded, public evidence.
        public = getattr(exc, "evidence_diagnostics", {})
        diagnostics = {}
        if isinstance(public, dict):
            for key, limit in (("document_source", 1000), ("document_sha256", 64), ("header_excerpt", 1600)):
                if isinstance(public.get(key), str):
                    diagnostics[key] = public[key][:limit]
            for key in ("document_bytes", "extracted_characters"):
                if isinstance(public.get(key), int) and not isinstance(public[key], bool):
                    diagnostics[key] = public[key]
            if isinstance(public.get("text_extractor"), str) and public["text_extractor"] in {
                "pypdf", "pdfminer.six_adobe_gb1"
            }:
                diagnostics["text_extractor"] = public["text_extractor"]
        serialized = pickle.dumps(("error", getattr(exc, "error_class", type(exc).__name__),
                                   str(exc)[:400], diagnostics))
    record_isolated_phase("result_serialized")
    record_isolated_phase("result_writing")
    Path(path).write_bytes(serialized)
    record_isolated_phase("result_closed")
    # The result file is complete and closed. SDK logging executors can keep
    # Python's thread shutdown waiting after the provider and validation have
    # finished, turning a completed call into a false TimeoutError. This is a
    # disposable, read-only worker: exit without waiting for SDK finalizers.
    # The parent still kills the entire process group and reaps this process.
    record_isolated_phase("exit_requested")
    os._exit(0)


def run_isolated(callback: Callable[[], Any], timeout_seconds: float, *,
                 network_timeout_seconds: float | None = None,
                 execution_diagnostics: dict | None = None) -> Any:
    """Finish, kill and reap one provider call within its total deadline.

    A small result file avoids pipe send/receive deadlocks for DataFrames.
    Only this process's own, locally written pickle is ever deserialized.
    """
    if timeout_seconds <= 0 or (network_timeout_seconds is not None and network_timeout_seconds <= 0):
        raise ResearchCallError("BudgetExhausted")
    if "fork" not in multiprocessing.get_all_start_methods():
        raise ResearchCallError("IsolationUnsupported")
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="scan-research-") as directory:
        path = str(Path(directory) / "result")
        progress_path = str(Path(directory) / "execution.json") if execution_diagnostics is not None else None
        process = multiprocessing.get_context("fork").Process(
            target=_execute, args=(callback, path, timeout_seconds, network_timeout_seconds, progress_path, started),
            daemon=False,
        )
        deadline_exceeded = False
        try:
            process.start()
            process.join(max(0.0, timeout_seconds - (time.monotonic() - started)))
            if process.is_alive():
                deadline_exceeded = True
                raise ResearchCallError("TimeoutError")
            if process.exitcode != 0 or not Path(path).is_file():
                raise ResearchCallError("WorkerExited", str(process.exitcode))
            payload = pickle.loads(Path(path).read_bytes())
            if payload[0] == "error":
                raise ResearchCallError(payload[1], payload[2], payload[3])
            return payload[1]
        finally:
            if process.pid is not None:
                # Kill the isolated group, including SDK helper processes, on
                # success as well as timeout. Never signal the parent's group.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                if process.is_alive():
                    process.kill()
                process.join(0.2)
                if execution_diagnostics is not None:
                    try:
                        progress = json.loads(Path(progress_path).read_text(encoding="utf-8"))
                    except (OSError, ValueError):
                        progress = {"phases": [], "last_phase": "unavailable"}
                    execution_diagnostics.update(
                        **progress, deadline_exceeded=deadline_exceeded,
                        worker_exitcode=process.exitcode,
                        result_file_bytes=Path(path).stat().st_size if Path(path).is_file() else 0,
                        elapsed_seconds=round(time.monotonic() - started, 6),
                    )
                if process.is_alive():
                    raise ResearchCallError("WorkerCleanupFailed")
                process.close()
