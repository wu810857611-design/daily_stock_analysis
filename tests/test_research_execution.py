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

from src.services.research_execution import ResearchCallError, run_isolated


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
            run_isolated(callback, 2)
    else:
        assert run_isolated(callback, 2) == {"evidence": "x" * 1024 * 1024}
    assert completed.read_text() == "provider callback finished"
    assert time.monotonic() - started < 1.5
    assert not multiprocessing.active_children()


def test_background_executor_does_not_make_unfinished_callback_usable(tmp_path):
    def callback():
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(threading.Event().wait)
        (tmp_path / "started").write_text("provider still running")
        threading.Event().wait()
        return "must never become a completed result"

    with pytest.raises(ResearchCallError, match="TimeoutError"):
        run_isolated(callback, 0.25)
    assert (tmp_path / "started").exists()
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
