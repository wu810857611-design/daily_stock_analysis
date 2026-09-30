import multiprocessing
import os
import signal
import time

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
