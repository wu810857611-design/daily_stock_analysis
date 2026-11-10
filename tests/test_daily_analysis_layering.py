"""Focused contracts for the tiered close-analysis workflow."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest

from scripts.account_watchlists import PRIMARY_SYMBOLS, priority_analysis_pools


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "00-daily-analysis.yml"


def _workflow_text() -> str:
    return WORKFLOW_PATH.read_text(encoding="utf-8")


def test_daily_schedule_and_manual_legacy_summary_are_preserved() -> None:
    text = _workflow_text()

    assert text.count("cron: '0 10 * * 1-5'") == 1
    assert "workflow_dispatch:" in text
    assert "- simulation-summary" in text
    assert text.count("scripts/paper_trade_tracker.py") == 1
    assert 'if [ "$MODE" = "simulation-summary" ]' in text
    assert 'title "旧版标准化20日模拟实验（非真实持仓A/B）"' in text


def test_full_close_run_does_not_advance_or_push_legacy_paper_experiment() -> None:
    text = _workflow_text()

    assert "--signals data/paper_trade/daily_signals.json" not in text
    assert '--title "收盘分析与20日模拟跟踪"' not in text
    assert text.count("scripts/pushplus_notify.py") == 1
    assert "data/paper_trade/state.json" in text  # history restore/backup remains
    assert "tar -czf paper-close-state.tar.gz data/stock_analysis.db data/paper_trade" in text
    assert "steps.package_close_state.outputs.available == 'true'" in text


def test_primary_pool_is_complete_first_and_watch_pools_are_disjoint() -> None:
    pools = priority_analysis_pools()

    assert len(PRIMARY_SYMBOLS) == 14
    assert pools["P0_PRIMARY"] == PRIMARY_SYMBOLS
    flattened = [symbol for symbols in pools.values() for symbol in symbols]
    assert len(flattened) == len(set(flattened))
    assert pools["P3_CANDIDATES"] == ("002759",)

    text = _workflow_text()
    primary_call = text.index('run_analysis_layer "P0_PRIMARY"')
    coverage_check = text.index("--output data/daily_analysis/primary_coverage.json")
    family_call = text.index('run_analysis_layer "P1_FAMILY"')
    holdings_call = text.index('run_analysis_layer "P2_ACCOUNT_HOLDINGS"')
    candidate_call = text.index('run_analysis_layer "P3_CANDIDATES"')
    assert primary_call < coverage_check < family_call < holdings_call < candidate_call
    assert '--stocks "$ACTIVE_PRIMARY_STOCKS"' in text
    assert "--min-coverage 1.0" in text


def test_watch_layer_failures_cannot_change_primary_coverage_gate() -> None:
    text = _workflow_text()

    assert 'run_analysis_layer "P0_PRIMARY" "本人主账户" "$PRIMARY_STOCKS" true' in text
    assert 'run_analysis_layer "P1_FAMILY" "父亲账户观察" "$FAMILY_STOCKS" false' in text
    assert (
        'run_analysis_layer "P2_ACCOUNT_HOLDINGS" "第二/妹妹账户持仓观察" '
        '"$ACCOUNT_HOLDING_STOCKS" false'
    ) in text
    assert 'run_analysis_layer "P3_CANDIDATES" "候选观察" "$CANDIDATE_STOCKS" false' in text
    assert "PRIMARY 不受影响" in text
    assert "OPTIONAL_ANALYSIS_DEADLINE" in text
    assert "JOB_BUDGET_STARTED_EPOCH" in text
    assert "OPTIONAL_SAFETY_MARGIN_SECONDS" in text


class ReportArchiveTests(unittest.TestCase):
    """Execute the deployed shell boundary with local commands, never production."""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.reports = self.root / "reports"
        self.reports.mkdir()
        self.stock = self.reports / "report_20261028.md"
        self.stock.write_text("previous CN batch", encoding="utf-8")
        (self.reports / "market_review_20261028.md").write_text("previous market")
        self.destination = self.reports / "p2_account_holdings" / "hk"

    def run_command(self, code: str, *, timed: bool = False) -> subprocess.CompletedProcess:
        text = _workflow_text()
        start = text.index("          run_with_report_archive() {")
        end = text.index("          run_analysis_layer() {", start)
        function = textwrap.dedent(text[start:end])
        invocation = 'run_with_report_archive "$1" '
        invocation += 'timeout 0.1s ' if timed else ''
        invocation += '"$2" -c "$3"'
        return subprocess.run(
            ["bash", "-c", "set -euo pipefail\n" + function + invocation,
             "archive-test", str(self.destination), sys.executable, code],
            cwd=self.root, capture_output=True, text=True, timeout=10,
        )

    def test_success_archives_only_this_invocations_report(self) -> None:
        result = self.run_command(
            "from pathlib import Path; Path('reports/report_20261028.md').write_text('new HK batch')"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.destination / self.stock.name).read_text(), "new HK batch")
        self.assertFalse((self.destination / "market_review_20261028.md").exists())
        self.assertEqual(self.stock.read_text(), "new HK batch")
        self.assertTrue((self.reports / "market_review_20261028.md").exists())

    def test_failed_no_output_cannot_archive_previous_batch(self) -> None:
        result = self.run_command("raise SystemExit(7)")
        self.assertEqual(result.returncode, 7)
        self.assertEqual(list(self.destination.iterdir()), [])
        self.assertEqual(self.stock.read_text(), "previous CN batch")

    def test_actual_timeout_cannot_archive_previous_batch(self) -> None:
        result = self.run_command("import time; time.sleep(5)", timed=True)
        self.assertEqual(result.returncode, 124)
        self.assertEqual(list(self.destination.iterdir()), [])
        self.assertEqual(self.stock.read_text(), "previous CN batch")

    def test_success_without_output_is_not_an_existing_report(self) -> None:
        result = self.run_command("pass")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(list(self.destination.iterdir()), [])

    def test_partial_output_is_preserved_without_hiding_failure(self) -> None:
        result = self.run_command(
            "from pathlib import Path; Path('reports/report_20261028.md').write_text('partial HK'); "
            "raise SystemExit(3)"
        )
        self.assertEqual(result.returncode, 3)
        self.assertEqual((self.destination / self.stock.name).read_text(), "partial HK")
        self.assertFalse((self.destination / "market_review_20261028.md").exists())

    def test_new_output_with_identical_bytes_still_belongs_to_this_call(self) -> None:
        result = self.run_command(
            "from pathlib import Path; Path('reports/report_20261028.md').write_text('previous CN batch')"
        )
        self.assertEqual(result.returncode, 0)
        self.assertTrue((self.destination / self.stock.name).exists())

    def test_all_close_report_invocations_use_archive_boundary(self) -> None:
        text = _workflow_text()
        self.assertEqual(text.count('run_with_report_archive "reports/${priority,,}/$market"'), 2)
        self.assertIn("run_with_report_archive reports/market", text)
        self.assertNotIn('cp reports/*.md "reports/${priority,,}/$market/"', text)

    def run_layer(self, required: bool, *, close_seconds: int, job_seconds: int):
        text = _workflow_text()
        functions = text[text.index("          run_with_report_archive() {"):
                         text.index("          # P0 必须先完整分析")]
        budget = text[text.index("          OPTIONAL_SAFETY_MARGIN_SECONDS="):
                      text.index('          run_analysis_layer "P1_FAMILY"')]
        bin_dir = self.root / "bin"
        bin_dir.mkdir(exist_ok=True)
        python = bin_dir / "python"
        python.write_text(
            '#!/usr/bin/env bash\nprintf "analysis %s %s %s\\n" "$BACKTEST_ENABLED" "$TEST_TIMEOUT" "$3"\n',
            encoding="utf-8",
        )
        python.chmod(0o755)
        setup = """
            set -euo pipefail
            export PATH="$PWD/bin:$PATH" BACKTEST_ENABLED=true
            python3() { local entry="$1"; shift; "$TEST_PYTHON" "$TEST_REPO/$entry" "$@"; }
            date() { echo 100000; }
            timeout() { export TEST_TIMEOUT="$3"; shift 3; "$@"; }
            TEST_PYTHON="$1"
            TEST_REPO="$2"
            CLOSE_CUTOFF_EPOCH=$((100000 + $3))
            ANALYSIS_TIMEOUT_MINUTES=90
            JOB_BUDGET_STARTED_EPOCH=$((100000 - 90 * 60 + $4))
            TRADE_DATE=2026-11-10
            FORCE_RUN_ARG=
        """
        command = 'run_analysis_layer "$5" "观察层" "HK09988" "$6"\n'
        return subprocess.run(
            ["bash", "-c", textwrap.dedent(setup) + textwrap.dedent(functions)
             + textwrap.dedent(budget) + command, "layer-test",
             sys.executable, str(ROOT), str(close_seconds), str(job_seconds),
             "P0_PRIMARY" if required else "P2_ACCOUNT_HOLDINGS", str(required).lower()],
            cwd=self.root, capture_output=True, text=True, timeout=10,
        )

    def test_only_primary_batch_runs_auto_backtest(self) -> None:
        primary = self.run_layer(True, close_seconds=3600, job_seconds=5400)
        optional = self.run_layer(False, close_seconds=3600, job_seconds=5400)
        self.assertEqual(primary.returncode, 0, primary.stderr)
        self.assertEqual(optional.returncode, 0, optional.stderr)
        self.assertIn("analysis true 3600s HK09988", primary.stdout)
        self.assertIn("analysis false 1800s HK09988", optional.stdout)

    def test_optional_limits_reserve_persistence_and_respect_close_cutoff(self) -> None:
        for close_seconds, job_seconds, expected in [(3600, 600, 120), (50, 5400, 50)]:
            with self.subTest(close_seconds=close_seconds, job_seconds=job_seconds):
                run = self.run_layer(False, close_seconds=close_seconds, job_seconds=job_seconds)
                self.assertEqual(run.returncode, 0, run.stderr)
                self.assertIn(f"analysis false {expected}s HK09988", run.stdout)

    def test_expired_optional_cutoff_does_not_fail_completed_primary(self) -> None:
        primary = self.run_layer(True, close_seconds=0, job_seconds=5400)
        optional = self.run_layer(False, close_seconds=0, job_seconds=5400)
        self.assertEqual(primary.returncode, 1)
        self.assertEqual(optional.returncode, 0, optional.stderr)
        self.assertIn("PRIMARY 不受影响", optional.stdout)
        self.assertNotIn("analysis false", optional.stdout)
