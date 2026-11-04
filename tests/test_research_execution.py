import multiprocessing
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from src.services.research_execution import ResearchCallError, record_isolated_phase, run_isolated


def hang():
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    while True:
        time.sleep(0.1)


def test_repeated_hangs_are_killed_and_reaped_without_pool_exhaustion():
    children = {item.pid for item in multiprocessing.active_children()}
    started = time.monotonic()
    for _ in range(10):
        with pytest.raises(ResearchCallError, match="TimeoutError"):
            run_isolated(hang, 0.05)
    assert time.monotonic() - started < 3
    assert {item.pid for item in multiprocessing.active_children()} == children
    assert run_isolated(lambda: {"after_hangs": True}, 2) == {"after_hangs": True}


def test_exception_and_budget_exhaustion_are_explicit():
    with pytest.raises(ResearchCallError, match="BudgetExhausted"):
        run_isolated(lambda: True, 0)
    with pytest.raises(ResearchCallError, match="ZeroDivisionError"):
        run_isolated(lambda: 1 / 0, 2)
    with pytest.raises(ResearchCallError, match="WorkerExited"):
        run_isolated(lambda: os._exit(3), 2)


def test_requests_connect_and_read_deadlines_are_local_to_worker():
    import requests

    original = requests.sessions.Session.request
    def captured(self, *args, **kwargs):
        return kwargs.get("timeout")
    requests.sessions.Session.request = captured
    try:
        assert run_isolated(lambda: requests.get("https://unused.invalid", timeout=None), 2) == (2, 2)
        assert requests.sessions.Session.request is captured
    finally:
        requests.sessions.Session.request = original


@pytest.mark.parametrize("fails", [False, True])
def test_completed_callback_does_not_wait_for_sdk_logging_executor(tmp_path, fails):
    completed = tmp_path / "completed"
    execution = {}

    def callback():
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(threading.Event().wait)
        completed.write_text("provider callback finished")
        if fails:
            raise ZeroDivisionError("preserve completed failure")
        # Larger than a pipe buffer; result-file transfer must remain intact.
        return {"evidence": "x" * 1024 * 1024}

    started = time.monotonic()
    if fails:
        with pytest.raises(ResearchCallError, match="ZeroDivisionError"):
            run_isolated(callback, 2, execution_diagnostics=execution)
    else:
        assert run_isolated(callback, 2, execution_diagnostics=execution) == {"evidence": "x" * 1024 * 1024}
    assert completed.read_text() == "provider callback finished"
    assert time.monotonic() - started < 1.5
    assert not multiprocessing.active_children()
    assert execution["last_phase"] == "exit_requested" and execution["worker_exitcode"] == 0
    assert "result_closed" in [event["phase"] for event in execution["phases"]]
    assert not execution["deadline_exceeded"] and execution["result_file_bytes"] > 0


def test_background_executor_does_not_make_unfinished_callback_usable(tmp_path):
    execution = {}
    def callback():
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(threading.Event().wait)
        (tmp_path / "started").write_text("provider still running")
        threading.Event().wait()
        return "must never become a completed result"

    with pytest.raises(ResearchCallError, match="TimeoutError"):
        run_isolated(callback, 0.25, execution_diagnostics=execution)
    assert (tmp_path / "started").exists()
    assert not multiprocessing.active_children()
    assert execution["last_phase"] == "callback_started" and execution["deadline_exceeded"]
    assert execution["result_file_bytes"] == 0


def test_progress_is_bounded_and_rejects_non_allowlisted_phase_text():
    execution = {}

    def callback():
        for _ in range(100):
            record_isolated_phase("sdk_ready")
        record_isolated_phase("never-log-this-secret")
        return "unchanged"

    assert run_isolated(callback, 2, execution_diagnostics=execution) == "unchanged"
    assert len(execution["phases"]) == 64
    assert "never-log-this-secret" not in str(execution)


def test_progress_write_failure_does_not_change_provider_result(monkeypatch):
    def denied(*_args, **_kwargs):
        raise PermissionError("diagnostic unavailable")

    monkeypatch.setattr(Path, "write_text", denied)
    execution = {}
    assert run_isolated(lambda: "unchanged", 2, execution_diagnostics=execution) == "unchanged"
    assert execution["last_phase"] == "unavailable" and execution["worker_exitcode"] == 0


def test_closed_result_at_deadline_is_not_accepted_as_completed(monkeypatch):
    monkeypatch.setattr(os, "_exit", lambda _code: threading.Event().wait())
    execution = {}
    with pytest.raises(ResearchCallError, match="TimeoutError"):
        run_isolated(lambda: "closed file is insufficient", 0.3, execution_diagnostics=execution)
    assert execution["last_phase"] == "exit_requested"
    assert execution["result_file_bytes"] > 0 and execution["deadline_exceeded"]
    assert not multiprocessing.active_children()


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux process-group cleanup probe")
def test_completed_worker_still_kills_sdk_descendant_process():
    def callback():
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        return child.pid

    pid = run_isolated(callback, 2)
    deadline = time.monotonic() + 1
    while time.monotonic() < deadline:
        try:
            # A killed orphan may briefly remain a zombie until init reaps it.
            state = Path(f"/proc/{pid}/stat").read_text().split(") ", 1)[1][0]
        except FileNotFoundError:
            break
        if state == "Z":
            break
        time.sleep(0.01)
    else:
        pytest.fail("SDK descendant outlived the completed isolated call")
    assert not multiprocessing.active_children()
