"""Close recovery: exercise provider date contracts, storage and watchdog entrypoint."""
from datetime import date, datetime
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from scripts import market_scan_watchdog as watchdog
from src.core.pipeline import StockAnalysisPipeline
from src.services.close_analysis_context import CLOSE_CRON
from src.storage import DatabaseManager, StockDaily

TZ = watchdog.SHANGHAI_TZ
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def pipeline():
    engine = create_engine("sqlite:///:memory:")
    StockDaily.__table__.create(engine)
    db = object.__new__(DatabaseManager)
    db._SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    db._is_sqlite_engine = True
    db._sqlite_write_retry_max = 0
    db._initialized = True
    result = StockAnalysisPipeline.__new__(StockAnalysisPipeline)
    result.db = db
    result.close_analysis_date = date(2026, 10, 8)
    yield result
    engine.dispose()


@pytest.mark.parametrize("code", ["HK01347", "HK00981", "HK06181", "HK02522", "HK06166"])
def test_yahoo_exclusive_end_reaches_close_storage_for_all_missing_primary(pipeline, monkeypatch, code):
    from data_provider.yfinance_fetcher import YfinanceFetcher

    requests = []

    def download(**kwargs):
        requests.append(kwargs)
        # Model Yahoo's actual [start, end) contract, not a mocked manager result.
        dates = pd.date_range("2026-10-07", "2026-10-09", name="Date")
        raw = pd.DataFrame({"Open": [57, 58, 99], "High": [58, 59, 100],
                            "Low": [56, 57, 98], "Close": [57, 58, 99],
                            "Volume": [1000, 1100, 1200]}, index=dates)
        raw.columns = pd.MultiIndex.from_product([raw.columns, [kwargs["tickers"]]])
        return raw[(raw.index >= kwargs["start"]) & (raw.index < kwargs["end"])]

    monkeypatch.setattr("yfinance.download", download)
    fetcher = YfinanceFetcher()
    pipeline.fetcher_manager = SimpleNamespace(
        get_stock_name=lambda *_args, **_kwargs: code,
        get_daily_data=lambda *args, **kwargs: (fetcher.get_daily_data(*args, **kwargs), fetcher.name),
    )
    success, error = pipeline.fetch_and_save_stock_data(code, force_refresh=True)
    assert success, error
    assert requests[0]["end"] == "2026-10-09"
    context = pipeline._get_analysis_context_with_market_fallback(code)
    assert context["date"] == "2026-10-08"
    assert context["today"]["close"] == 58
    with pipeline.db.get_session() as session:
        assert [row.date for row in session.query(StockDaily).order_by(StockDaily.date)] == [
            date(2026, 10, 7), date(2026, 10, 8)]


def test_inclusive_provider_future_bar_is_trimmed_before_storage(pipeline):
    frame = pd.DataFrame({"date": ["2026-10-08", "2026-10-09"], "close": [58, 99]})
    daily = Mock(return_value=(frame, "inclusive-provider"))
    pipeline.fetcher_manager = SimpleNamespace(get_stock_name=Mock(return_value="test"), get_daily_data=daily)
    success, error = pipeline.fetch_and_save_stock_data("HK00981", force_refresh=True)
    assert success, error
    assert pipeline.db.get_analysis_context("HK00981")["date"] == "2026-10-08"
    assert pipeline.db.get_analysis_context("HK00981")["today"]["close"] == 58


def test_undated_fetch_keeps_existing_query_contract(pipeline):
    pipeline.close_analysis_date = None
    daily = Mock(return_value=(pd.DataFrame({"date": ["2026-10-08"], "close": [58]}), "test"))
    pipeline.fetcher_manager = SimpleNamespace(get_stock_name=Mock(return_value="test"), get_daily_data=daily)
    success, error = pipeline.fetch_and_save_stock_data(
        "HK00981", force_refresh=True, current_time=datetime(2026, 10, 8, 18, tzinfo=TZ))
    assert success, error
    daily.assert_called_once_with("HK00981", days=30)


@pytest.mark.parametrize("observe_only", [False, True])
@pytest.mark.parametrize("observed", ["2026-10-08T21:00:01+08:00", "2026-10-09T01:11:07+08:00"])
def test_delayed_close_watchdog_never_queries_dispatches_or_waits(observe_only, observed):
    client, sleep, gate = Mock(), Mock(), Mock()
    result = watchdog.run_watchdog(
        slot="close", session_date=date(2026, 10, 8),
        now_fn=lambda: datetime.fromisoformat(observed), sleep_fn=sleep, client=client,
        workflow="02-market-scan.yml", ref="main", session_gate=gate, observe_only=observe_only)
    assert result["status"] == "skipped_late"
    assert result["trade_date"] == "2026-10-08"
    assert result["latest_at"] == "2026-10-08T21:00:00+08:00"
    assert not client.mock_calls
    sleep.assert_not_called()
    gate.assert_not_called()


@pytest.mark.parametrize("observe_only", [False, True])
def test_close_watchdog_wait_crossing_midnight_cannot_query_next_session(observe_only):
    clock = iter([datetime(2026, 10, 8, 18, tzinfo=TZ), datetime(2026, 10, 9, 1, tzinfo=TZ)])
    client = Mock()
    result = watchdog.run_watchdog(
        slot="close", session_date=date(2026, 10, 8), now_fn=lambda: next(clock),
        sleep_fn=Mock(), client=client, workflow="02-market-scan.yml", ref="main",
        session_gate=lambda _now: {"should_run": True}, observe_only=observe_only)
    assert result["status"] == "skipped_late"
    assert not client.mock_calls


def test_current_close_watchdog_still_dispatches_missing_scan():
    client = Mock()
    client.recent_runs.return_value = []
    result = watchdog.run_watchdog(
        slot="close", session_date=date(2026, 10, 8),
        now_fn=lambda: datetime(2026, 10, 8, 19, 27, tzinfo=TZ), sleep_fn=Mock(),
        client=client, workflow="02-market-scan.yml", ref="main")
    assert result["status"] == "dispatched"
    client.dispatch.assert_called_once_with("02-market-scan.yml", "main", "close")


def test_production_run_61_watchdog_cli_keeps_original_date_and_exits_without_side_effects(tmp_path, monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 9, 1, 12, tzinfo=TZ).astimezone(tz)

    metadata = tmp_path / "run.json"
    metadata.write_text(json.dumps({"created_at": "2026-10-08T17:11:07Z"}))
    output = tmp_path / "result.json"
    client, sleep = Mock(), Mock()
    monkeypatch.setattr(watchdog, "datetime", Clock)
    monkeypatch.setattr(watchdog, "GitHubActionsClient", Mock(return_value=client))
    monkeypatch.setattr(watchdog.time, "sleep", sleep)
    monkeypatch.setenv("GH_TOKEN", "offline-test")
    assert watchdog.main(["--slot", "close", "--repo", "test/repo", "--close-run-metadata",
                          str(metadata), "--close-schedule", CLOSE_CRON, "--output", str(output)]) == 0
    result = json.loads(output.read_text())
    assert result["trade_date"] == "2026-10-08"
    assert result["status"] == "skipped_late"
    assert not client.mock_calls
    sleep.assert_not_called()


def test_close_watchdog_workflow_has_calendar_runtime_and_original_run_contract():
    workflow = yaml.safe_load((ROOT / ".github/workflows/00-daily-analysis.yml").read_text())
    steps = workflow["jobs"]["close-scan-watchdog"]["steps"]
    assert any(step.get("uses", "").startswith("actions/setup-python@") for step in steps)
    assert any("pip install 'exchange-calendars>=4.13.0'" in step.get("run", "") for step in steps)
    run = next(step["run"] for step in steps if "scripts/market_scan_watchdog.py" in step.get("run", ""))
    assert 'actions/runs/${GITHUB_RUN_ID}' in run
    assert "--close-run-metadata close-watchdog-run.json" in run
    assert '--close-schedule "$CLOSE_EVENT_SCHEDULE"' in run


@pytest.mark.parametrize("created,schedule", [
    ("2026-10-07T10:00:00Z", CLOSE_CRON),
    ("2026-10-08T17:11:07Z", "unknown-cron"),
])
def test_watchdog_cli_rejects_stale_run_and_unknown_schedule_before_actions(tmp_path, monkeypatch, created, schedule):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 9, 1, 12, tzinfo=TZ).astimezone(tz)

    metadata = tmp_path / "run.json"
    metadata.write_text(json.dumps({"created_at": created}))
    client, sleep = Mock(), Mock()
    monkeypatch.setattr(watchdog, "datetime", Clock)
    monkeypatch.setattr(watchdog, "GitHubActionsClient", Mock(return_value=client))
    monkeypatch.setattr(watchdog.time, "sleep", sleep)
    monkeypatch.setenv("GH_TOKEN", "offline-test")
    with pytest.raises(ValueError):
        watchdog.main(["--slot", "close", "--repo", "test/repo", "--close-run-metadata",
                       str(metadata), "--close-schedule", schedule])
    assert not client.mock_calls
    sleep.assert_not_called()
