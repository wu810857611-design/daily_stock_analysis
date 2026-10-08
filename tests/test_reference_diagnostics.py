from datetime import datetime
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

from scripts.intraday_session import load_reference_levels_batch, render_session_report

NOW = datetime(2026, 10, 8, 9, 20, tzinfo=ZoneInfo('Asia/Shanghai'))


def database(tmp_path):
    path = tmp_path / 'reference.db'
    db = sqlite3.connect(path)
    db.executescript('''CREATE TABLE analysis_history
        (id INTEGER PRIMARY KEY, code TEXT, stop_loss REAL, take_profit REAL, created_at TEXT);
        CREATE TABLE decision_signals
        (id INTEGER PRIMARY KEY, stock_code TEXT, source_type TEXT, status TEXT,
         stop_loss REAL, target_price REAL, created_at TEXT, expires_at TEXT);''')
    return path, db


def test_short_hk_alias_and_fresh_sources_are_traceable(tmp_path):
    path, db = database(tmp_path)
    db.execute("INSERT INTO analysis_history VALUES (1, '1548.HK', 40, 50, '2026-10-07 19:00:00')")
    db.commit()
    db.close()
    details = {}
    levels = load_reference_levels_batch(path, ['HK01548'], now=NOW, diagnostics=details)
    assert levels['HK01548'].stop_loss == 40
    assert details['HK01548']['stop_source'] == 'analysis_history'
    assert details['HK01548']['sources']['analysis_history']['selected_created_at'] == '2026-10-07 19:00:00'


def test_old_database_and_expired_signal_do_not_become_valid(tmp_path):
    path, db = database(tmp_path)
    db.execute("INSERT INTO analysis_history VALUES (1, '300408', 40, 50, '2026-09-24 19:00:00')")
    db.execute("INSERT INTO decision_signals VALUES (1, '300408', 'analysis', 'active', 42, 52, '2026-09-24 12:00:00', '2026-09-25 12:00:00')")
    db.commit()
    db.close()
    details = {}
    levels = load_reference_levels_batch(path, ['300408'], now=NOW, diagnostics=details)
    assert levels['300408'].stop_loss is None
    assert details['300408']['sources']['analysis_history']['status'] == 'too_old'
    assert details['300408']['sources']['decision_signals']['status'] == 'expired_signal'
    assert details['300408']['reason'] == 'no_valid_dated_reference'
    assert details['300408']['affected_capabilities'] == ['stop_loss_monitoring', 'target_price_monitoring']


def test_future_missing_and_empty_records_are_distinct(tmp_path):
    path, db = database(tmp_path)
    db.execute("INSERT INTO analysis_history VALUES (1, '300408', 40, 50, '2026-10-09 19:00:00')")
    db.execute("INSERT INTO analysis_history VALUES (2, '601857', NULL, NULL, '2026-10-07 19:00:00')")
    db.commit()
    db.close()
    details = {}
    levels = load_reference_levels_batch(path, ['300408', '601857', '600000'], now=NOW, diagnostics=details)
    assert all(x.stop_loss is None for x in levels.values())
    assert details['300408']['sources']['analysis_history']['status'] == 'future_dated'
    assert details['601857']['reason'] == 'levels_missing'
    assert details['600000']['reason'] == 'record_or_schema_missing'
    report = render_session_report(now=NOW, symbols=[], last_cycle=None, cycles=0,
        events_created=0, events_notified=0, pending_events=0,
        provider_status={'reference_levels': {'by_symbol': details}})
    assert 'stop_loss_monitoring' in report and '2026-10-09 19:00:00' in report


def test_missing_file_reports_database_missing(tmp_path):
    details = {}
    load_reference_levels_batch(tmp_path / 'missing.db', ['300408'], now=NOW, diagnostics=details)
    assert details['300408']['reason'] == 'database_missing'


def test_workflow_refresh_is_based_on_valid_reference_coverage():
    workflow = Path('.github/workflows/01-intraday-session.yml').read_text()
    assert 'if python3 scripts/reference_levels_audit.py' in workflow
    assert 'reference_restore_before.json' in workflow
    assert 'reference_restore_after.json' in workflow
    assert 'DEFAULT_REFERENCE_SIGNAL_MAX_AGE_DAYS' in workflow
