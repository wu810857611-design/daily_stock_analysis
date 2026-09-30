"""Killable call boundary for scan-only, read-only third-party research.

Unlike a thread join, a deadline here also releases the work that timed out.
The Linux Actions runner supports fork; unsupported platforms fail explicitly
instead of silently falling back to leaking daemon threads.
"""
from __future__ import annotations

import multiprocessing
import os
import pickle
import signal
import socket
import tempfile
import time
from pathlib import Path
from typing import Any, Callable


class ResearchCallError(RuntimeError):
    def __init__(self, error_class: str, detail: str = "") -> None:
        self.error_class = error_class
        super().__init__(f"{error_class}: {detail}"[:500])


def _execute(callback: Callable[[], Any], path: str, budget: float) -> None:
    os.setsid()
    # Request defaults live only in this disposable process. SDKs that bypass
    # requests are still subject to the parent process's total deadline.
    socket.setdefaulttimeout(min(4.0, budget))
    import requests

    original_request = requests.sessions.Session.request

    def bounded_request(self: Any, *args: Any, **kwargs: Any) -> Any:
        configured = kwargs.get("timeout")
        limits = (min(3.0, budget), min(4.0, budget))
        if isinstance(configured, (int, float)):
            limits = tuple(min(item, configured) for item in limits)
        elif isinstance(configured, tuple) and len(configured) == 2:
            limits = tuple(min(item, value) if value is not None else item
                           for item, value in zip(limits, configured))
        kwargs["timeout"] = limits
        return original_request(self, *args, **kwargs)

    requests.sessions.Session.request = bounded_request
    try:
        payload = ("ok", callback())
        serialized = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
        if len(serialized) > 16 * 1024 * 1024:
            raise ValueError("research result exceeds 16 MiB")
    except BaseException as exc:
        serialized = pickle.dumps(("error", type(exc).__name__, str(exc)[:400]))
    Path(path).write_bytes(serialized)


def run_isolated(callback: Callable[[], Any], timeout_seconds: float) -> Any:
    """Finish, kill and reap one provider call within its total deadline.

    A small result file avoids pipe send/receive deadlocks for DataFrames.
    Only this process's own, locally written pickle is ever deserialized.
    """
    if timeout_seconds <= 0:
        raise ResearchCallError("BudgetExhausted")
    if "fork" not in multiprocessing.get_all_start_methods():
        raise ResearchCallError("IsolationUnsupported")
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="scan-research-") as directory:
        path = str(Path(directory) / "result")
        process = multiprocessing.get_context("fork").Process(
            target=_execute, args=(callback, path, timeout_seconds), daemon=False,
        )
        try:
            process.start()
            process.join(max(0.0, timeout_seconds - (time.monotonic() - started)))
            if process.is_alive():
                raise ResearchCallError("TimeoutError")
            if process.exitcode != 0 or not Path(path).is_file():
                raise ResearchCallError("WorkerExited", str(process.exitcode))
            payload = pickle.loads(Path(path).read_bytes())
            if payload[0] == "error":
                raise ResearchCallError(payload[1], payload[2])
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
                if process.is_alive():
                    raise ResearchCallError("WorkerCleanupFailed")
                process.close()
