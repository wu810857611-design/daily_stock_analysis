"""Cross-midnight close-date regression and entrypoint contracts."""
from datetime import date, datetime
import json
from pathlib import Path
import sqlite3
import subprocess
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import yaml

from src.services.close_analysis_context import (
    CLOSE_CRON, SHANGHAI, close_open_markets, close_reference_time, resolve_close_context,
)
from scripts.export_paper_signals import CoverageError, build_snapshot
from tests.test_export_paper_signals import SCHEMA

ROOT = Path(__file__).resolve().parents[1]


def resolve(created="2026-10-07T17:14:33Z", now="2026-10-08T01:15:58+08:00", **kwargs):
    return resolve_close_context(event=kwargs.get("event", "schedule"),
                                 schedule=kwargs.get("schedule", CLOSE_CRON),
                                 created_at=created, now=datetime.fromisoformat(now),
                                 budget_minutes=kwargs.get("budget", 90))


def test_production_run_60_cross_midnight_has_previous_close_and_real_holiday_calendar():
    context = resolve()
    assert context["trade_date"] == "2026-10-07"
    assert context["reference_time"] == "2026-10-07T18:00:00+08:00"
    assert context["open_markets"] == ["hk"]  # A-share National Day holiday
    assert context["cutoff_at"] == "2026-10-08T09:00:00+08:00"


@pytest.mark.parametrize("created,now", [
    ("2026-10-08T10:00:01Z", "2026-10-08T23:59:59+08:00"),
    ("2026-10-08T10:00:01Z", "2026-10-09T00:00:01+08:00"),
])
def test_same_original_run_keeps_date_across_midnight(created, now):
    assert resolve(created, now)["trade_date"] == "2026-10-08"


@pytest.mark.parametrize("kwargs", [
    {"now": "2026-10-08T09:00:00+08:00"},
    {"now": "2026-10-08T07:30:01+08:00"},  # full job cannot finish before cutoff
    {"now": "2026-10-08T18:15:00+08:00"},  # old run rerun
    {"created": "2026-10-08T10:00:00Z", "now": "2026-10-08T01:15:58+08:00"},
    {"schedule": "0 11 * * 1-5"},
    {"created": ""},
    {"created": "2026-10-07T17:14:33"},
    {"budget": 0},
    {"created": "2026-10-11T17:00:00Z", "now": "2026-10-12T01:01:00+08:00"},
])
def test_ambiguous_stale_or_late_runs_remain_rejected(kwargs):
    with pytest.raises(ValueError):
        resolve(**kwargs)


def test_manual_overnight_close_uses_same_bounded_rule():
    assert resolve(event="workflow_dispatch")["trade_date"] == "2026-10-07"
    with pytest.raises(ValueError):
        resolve(event="workflow_dispatch", now="2026-10-08T12:00:00+08:00")
    with pytest.raises(ValueError):
        resolve(event="workflow_dispatch", now="2026-10-08T18:15:00+08:00")


def test_calendar_failure_is_not_an_open_market_fallback():
    with patch("exchange_calendars.get_calendar", side_effect=RuntimeError("calendar unavailable")):
        with pytest.raises(RuntimeError, match="calendar unavailable"):
            close_open_markets("2026-10-07")


def test_dated_close_entrypoint_filters_by_close_date_even_with_force_run():
    import main
    args = SimpleNamespace(close_analysis_date="2026-10-07", force_run=True,
                           no_market_review=True)
    config = SimpleNamespace(trading_day_check_enabled=False, market_review_enabled=False)
    reference = datetime(2026, 10, 7, 18, tzinfo=SHANGHAI)
    with patch("src.services.close_analysis_context.close_reference_time", return_value=reference), \
         patch("src.core.trading_calendar.get_open_markets_today", side_effect=AssertionError("wrong day")):
        stocks, _, skipped = main._compute_trading_day_filter(config, args, ["688333", "HK00981"])
    assert stocks == ["HK00981"]
    assert not skipped
    from src.core.pipeline import StockAnalysisPipeline
    assert StockAnalysisPipeline._resolve_resume_target_date("HK00981", reference) == date(2026, 10, 7)


def test_next_utc_day_analysis_is_exported_for_its_close_date_but_undated_price_is_rejected():
    db = sqlite3.connect(":memory:")
    db.executescript(SCHEMA)
    db.execute("INSERT INTO analysis_history VALUES (1, 'HK00981', 'test', 'hold', 'test', ?, '{}', ?)",
               (json.dumps({"current_price": 58.0, "close_analysis_date": "2026-10-07"}), "2026-10-08 00:15:00"))
    kwargs = dict(stocks=["HK00981"], trade_date="2026-10-07", min_coverage=1.0,
                  analysis_since="2026-10-08T00:00:00Z", require_dated_close=True)
    with pytest.raises(CoverageError):
        build_snapshot(db, **kwargs)
    db.execute("INSERT INTO stock_daily VALUES (1, 'HK00981', '2026-10-06', 57)")
    with pytest.raises(CoverageError) as exc:
        build_snapshot(db, **kwargs)
    assert "stale_stock_daily_price:2026-10-06" in str(exc.value.snapshot)
    db.execute("INSERT INTO stock_daily VALUES (2, 'HK00981', '2026-10-07', 58)")
    snapshot = build_snapshot(db, **kwargs)
    assert snapshot["signals"][0]["price_source"] == "stock_daily:2026-10-07"
    assert snapshot["trade_date"] == "2026-10-07"
    db.close()


def test_workflow_shell_parses_and_carries_date_to_both_required_and_optional_batches():
    workflow = yaml.safe_load((ROOT / ".github/workflows/00-daily-analysis.yml").read_text())
    steps = workflow["jobs"]["analyze"]["steps"]
    step = next(step for step in steps if step["name"] == "执行股票分析")
    result = subprocess.run(["bash", "-n"], input=step["run"], text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
    assert step["run"].count('--close-analysis-date "$TRADE_DATE"') == 2
    assert '--stocks "$ACTIVE_PRIMARY_STOCKS"' in step["run"]
    assert "--require-dated-close" in step["run"]
    paths = next(step["with"]["path"] for step in steps if step["name"] == "上传分析报告")
    assert "data/daily_analysis/close_context.json" in paths


def test_strict_close_audit_refuses_analysis_from_another_date_despite_valid_daily_price():
    db = sqlite3.connect(":memory:")
    db.executescript(SCHEMA)
    db.execute("INSERT INTO analysis_history VALUES (1, 'HK00981', 'test', 'hold', 'test', ?, '{}', ?)",
               (json.dumps({"close_analysis_date": "2026-10-08"}), "2026-10-07 17:15:00"))
    db.execute("INSERT INTO stock_daily VALUES (1, 'HK00981', '2026-10-07', 58)")
    with pytest.raises(CoverageError) as exc:
        build_snapshot(db, stocks=["HK00981"], trade_date="2026-10-07", min_coverage=1,
                       analysis_since="2026-10-07T17:00:00Z", require_dated_close=True)
    assert "analysis_close_date_missing_or_mismatched" in str(exc.value.snapshot)
    db.close()


def test_close_pipeline_uses_bounded_database_context_and_rejects_missing_close_bar():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from src.storage import DatabaseManager, StockDaily
    from src.core.pipeline import StockAnalysisPipeline

    # Exercise the actual storage queries without affecting the global singleton.
    engine = create_engine("sqlite:///:memory:")
    StockDaily.__table__.create(engine)
    db = object.__new__(DatabaseManager)
    db._SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    db._initialized = True
    with db.get_session() as session:
        session.add_all([StockDaily(code="HK00981", date=date(2026, 10, day), close=price)
                         for day, price in ((6, 57), (7, 58), (8, 99))])
        session.commit()
    pipeline = StockAnalysisPipeline.__new__(StockAnalysisPipeline)
    pipeline.db = db
    pipeline.close_analysis_date = date(2026, 10, 7)
    context = pipeline._get_analysis_context_with_market_fallback("HK00981")
    assert context["today"]["close"] == 58
    assert context["yesterday"]["close"] == 57
    assert db.get_analysis_context("HK00981")["today"]["close"] == 99
    pipeline.config = SimpleNamespace(enable_realtime_quote=False)
    pipeline.query_source = "cli"
    pipeline.close_analysis_date = date(2026, 10, 9)
    from src.enums import ReportType
    assert pipeline.analyze_stock("HK00981", ReportType.SIMPLE, "close-regression") is None
    # Missing close bars must not cause model-only plans to be persisted.
    assert db.get_analysis_context("HK00981")["date"] == "2026-10-08"
    engine.dispose()


def test_main_close_context_reaches_pipeline_without_mutating_runtime_config():
    import main
    from unittest.mock import MagicMock
    config = SimpleNamespace(enable_realtime_quote=True, enable_realtime_technical_indicators=True,
                             single_stock_notify=False, merge_email_notification=False,
                             market_review_enabled=False, market_review_region="cn",
                             daily_market_context_enabled=False, analysis_delay=0, backtest_enabled=False)
    args = SimpleNamespace(close_analysis_date="2026-10-07", no_context_snapshot=True,
                           no_market_review=True, workers=1, dry_run=True, no_notify=True,
                           single_notify=False)
    reference = datetime(2026, 10, 7, 18, tzinfo=SHANGHAI)
    pipeline = MagicMock()
    pipeline.run.return_value = []
    with patch("main._refresh_stock_index_cache_for_analysis"), \
         patch("main._compute_trading_day_filter", return_value=(["HK00981"], None, False)), \
         patch("src.services.close_analysis_context.close_reference_time", return_value=reference), \
         patch("src.core.pipeline.StockAnalysisPipeline", return_value=pipeline) as constructor:
        assert main.run_full_analysis(config, args, ["HK00981"])
    assert pipeline.run.call_args.kwargs["current_time"] == reference
    assert constructor.call_args.kwargs["close_analysis_date"] == date(2026, 10, 7)
    close_config = constructor.call_args.kwargs["config"]
    assert not close_config.enable_realtime_quote
    assert not close_config.enable_realtime_technical_indicators
    assert config.enable_realtime_quote and config.enable_realtime_technical_indicators


def test_workflow_context_cli_emits_active_and_closed_primary_with_real_calendar(tmp_path):
    from scripts import close_analysis_context as cli
    class FrozenClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls.fromisoformat("2026-10-08T01:15:58+08:00").astimezone(tz or SHANGHAI)
    metadata = tmp_path / "run.json"
    metadata.write_text(json.dumps({"created_at": "2026-10-07T17:14:33Z"}))
    output = tmp_path / "close_context.json"
    with patch.object(cli, "datetime", FrozenClock):
        assert cli.main(["--event", "schedule", "--schedule", CLOSE_CRON,
                         "--run-metadata", str(metadata), "--budget-minutes", "90",
                         "--output", str(output)]) == 0
    context = json.loads(output.read_text())
    assert context["trade_date"] == "2026-10-07"
    assert context["active_primary"] and context["closed_primary"]
    assert all(symbol.startswith("HK") for symbol in context["active_primary"])
    assert len(context["active_primary"]) + len(context["closed_primary"]) == 14


def test_close_date_marker_survives_raw_result_persistence_without_context_snapshot():
    from src.storage import DatabaseManager
    result = SimpleNamespace(close_analysis_date="2026-10-07", to_dict=lambda: {"code": "HK00981"})
    assert DatabaseManager._build_raw_result(result)["close_analysis_date"] == "2026-10-07"


@pytest.mark.parametrize("mode", ["--market-review", "--schedule"])
def test_close_date_flag_cannot_silently_enter_an_unhandled_mode(mode):
    import main
    import sys
    with patch.object(sys, "argv", ["main.py", "--close-analysis-date", "2026-10-07", mode]):
        with pytest.raises(SystemExit) as exc:
            main.parse_arguments()
    assert exc.value.code == 2
