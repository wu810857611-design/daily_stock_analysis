from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest
import yaml

from src.core.close_analysis_context import CLOSE_CRON, close_reference_time, scheduled_close_context

TZ = ZoneInfo('Asia/Shanghai')


def local(value):
    return datetime.fromisoformat(value).replace(tzinfo=TZ)


def run(created):
    return {'event': 'schedule', 'created_at': created}


@pytest.mark.parametrize('created,observed,expected', [
    ('2026-10-08T10:01:00Z', '2026-10-08T18:03:00', '2026-10-08'),
    ('2026-10-07T17:14:34Z', '2026-10-08T01:15:58', '2026-10-07'),
    ('2026-10-09T17:14:34Z', '2026-10-10T01:15:58', '2026-10-09'),
    ('2026-10-08T15:59:00Z', '2026-10-09T00:01:00', '2026-10-08'),
])
def test_delayed_close_is_pinned_to_cron_day(created, observed, expected):
    result = scheduled_close_context(run(created), schedule=CLOSE_CRON, now=local(observed))
    assert result['trade_date'] == expected
    assert result['analysis_reference_time'] == expected + 'T18:00:00+08:00'
    assert result['observed_at'].startswith(observed)
    assert result['close_scan_should_run'] == (observed[:10] == expected and observed[11:16] <= '21:00')


@pytest.mark.parametrize('created,observed', [
    ('2026-10-07T17:14:34Z', '2026-10-08T09:00:00'),  # old rerun
    ('2026-10-08T01:01:00Z', '2026-10-08T09:02:00'),  # arrived after safe window
    ('2026-10-10T17:14:34Z', '2026-10-11T01:15:58'),  # no Saturday cron
    ('2026-10-09T10:00:00Z', '2026-10-08T18:00:00'),  # future metadata
    ('2026-10-08T18:00:00', '2026-10-08T18:01:00'),  # timezone missing
    ('', '2026-10-08T18:01:00'),
])
def test_ambiguous_or_stale_runs_fail_closed(created, observed):
    with pytest.raises(ValueError):
        scheduled_close_context(run(created), schedule=CLOSE_CRON, now=local(observed))


def test_unknown_cron_or_event_rejected():
    for event, cron in [('workflow_dispatch', CLOSE_CRON), ('schedule', '0 11 * * 1-5')]:
        with pytest.raises(ValueError):
            scheduled_close_context({'event': event, 'created_at': '2026-10-08T10:00:00Z'},
                                    schedule=cron, now=local('2026-10-08T18:01:00'))


def test_reference_window_does_not_extend_validity():
    assert close_reference_time('2026-10-08', local('2026-10-09T08:59:59')).hour == 18
    for observed in ['2026-10-08T17:59:59', '2026-10-09T09:00:00']:
        with pytest.raises(ValueError):
            close_reference_time('2026-10-08', local(observed))


def test_filter_uses_friday_session_on_saturday_and_holiday_is_not_shifted(monkeypatch):
    import main
    from src.core import trading_calendar

    class Clock:
        @staticmethod
        def now(tz):
            return local('2026-10-10T01:15:58').astimezone(tz)

    monkeypatch.setattr(main, 'datetime', Clock)
    monkeypatch.setattr(trading_calendar, '_XCALS_AVAILABLE', True)
    observed = []
    def is_open(market, day):
        observed.append(day.isoformat())
        return market == 'hk'  # A holiday must remain closed, not mapped to an older session.
    monkeypatch.setattr(trading_calendar, 'is_market_open', is_open)
    config = SimpleNamespace(trading_day_check_enabled=True, market_review_enabled=False)
    args = SimpleNamespace(close_analysis_date='2026-10-09', no_market_review=True, force_run=False)
    codes, _, _ = main._compute_trading_day_filter(config, args, ['300408', 'HK01347'])
    assert codes == ['HK01347']
    assert set(observed) == {'2026-10-09'}


def test_main_passes_same_pinned_time_to_pipeline(monkeypatch):
    monkeypatch.setenv('LITELLM_LOCAL_MODEL_COST_MAP', 'True')
    import main
    import src.core.pipeline as pipeline_module
    class Clock:
        @staticmethod
        def now(tz):
            return local('2026-10-09T01:15:58').astimezone(tz)
    monkeypatch.setattr(main, 'datetime', Clock)
    monkeypatch.setattr(main, '_refresh_stock_index_cache_for_analysis', lambda _: None)
    monkeypatch.setattr(main, '_run_auto_backtest', lambda _: None)
    # Keep real date validation and real pipeline target-date resolution; isolate provider/LLM/notifications.
    monkeypatch.setattr(main, '_compute_trading_day_filter', lambda c, a, s: (s, None, False))
    instance = MagicMock()
    instance.run.return_value = []
    factory = MagicMock(return_value=instance)
    monkeypatch.setattr(pipeline_module, 'StockAnalysisPipeline', factory)
    config = SimpleNamespace(market_review_enabled=False, max_workers=2, single_stock_notify=False,
                             analysis_delay=0, report_type='simple', backtest_enabled=False,
                             merge_email_notification=False)
    args = SimpleNamespace(close_analysis_date='2026-10-08', portfolio=None, workers=2,
                           no_market_review=True, no_notify=True, dry_run=False, stocks='300408')
    main.run_full_analysis(config, args, ['300408'], raise_errors=True)
    reference = instance.run.call_args.kwargs['current_time']
    assert reference == local('2026-10-08T18:00:00')
    from src.core.trading_calendar import get_effective_trading_date
    assert get_effective_trading_date('cn', current_time=reference).isoformat() == '2026-10-08'
    assert get_effective_trading_date('hk', current_time=reference).isoformat() == '2026-10-08'
    # Actual write timestamps remain DatabaseManager's UTC wall clock.
    assert Clock.now(timezone.utc).date().isoformat() == '2026-10-08'


def test_force_run_cannot_bypass_close_deadline(monkeypatch):
    import main
    class Clock:
        @staticmethod
        def now(tz):
            return local('2026-10-09T09:00:00').astimezone(tz)
    monkeypatch.setattr(main, 'datetime', Clock)
    with pytest.raises(ValueError, match='outside'):
        main.run_full_analysis(SimpleNamespace(),
                               SimpleNamespace(close_analysis_date='2026-10-08', force_run=True))


@pytest.mark.parametrize('mode', ['--schedule', '--serve', '--market-review', '--portfolio'])
def test_close_date_is_rejected_in_other_modes(monkeypatch, mode):
    import main
    import sys
    argv = ['main.py', '--close-analysis-date', '2026-10-08', '--no-market-review', mode]
    if mode == '--portfolio':
        argv.append('futu')
    monkeypatch.setattr(sys, 'argv', argv)
    with pytest.raises(SystemExit) as error:
        main.parse_arguments()
    assert error.value.code == 2


def test_workflow_wires_both_batches_and_installs_watchdog_dependencies():
    workflow = yaml.safe_load(Path('.github/workflows/00-daily-analysis.yml').read_text())
    steps = workflow['jobs']['analyze']['steps']
    context = next(s for s in steps if s['name'] == '固定定时收盘交易日')
    assert 'actions/runs/${GITHUB_RUN_ID}' in context['run']
    analysis = next(s['run'] for s in steps if s['name'] == '执行股票分析')
    assert analysis.count('"${CLOSE_DATE_ARGS[@]}" $FORCE_RUN_ARG') == 2
    assert 'CLOSE_DEADLINE_EPOCH - $(date +%s)' in analysis
    assert '已跨越自然日' not in analysis
    watchdog = workflow['jobs']['close-scan-watchdog']['steps']
    dependency_index = next(i for i, s in enumerate(watchdog) if 'exchange-calendars' in s.get('run', ''))
    run_index = next(i for i, s in enumerate(watchdog) if 'market_scan_watchdog.py' in s.get('run', ''))
    assert dependency_index < run_index
    scan_script = watchdog[run_index]['run']
    assert scan_script.index('close_scan_should_run') < scan_script.index('scripts/market_scan_watchdog.py')
    assert 'original_close_scan_window_ended' in scan_script


@pytest.mark.parametrize('expired', [False, True])
def test_real_workflow_function_passes_date_and_bounds_execution(tmp_path, expired):
    workflow = yaml.safe_load(Path('.github/workflows/00-daily-analysis.yml').read_text())
    analysis = next(s['run'] for s in workflow['jobs']['analyze']['steps'] if s['name'] == '执行股票分析')
    function = analysis[analysis.index('run_analysis_layer() {'):analysis.index('# P0 必须')]
    harness = '''
set -euo pipefail
CLOSE_DATE_ARGS=(--close-analysis-date 2026-10-08)
CLOSE_DEADLINE_EPOCH=$(( $(date +%s) + OFFSET ))
OPTIONAL_ANALYSIS_DEADLINE=$(( $(date +%s) + 60 ))
FORCE_RUN_ARG=""
python3() { printf '%s\n' '300408'; }
python() { printf '%s\n' "$*" >> calls.txt; }
timeout() {
  printf '%s\n' "$*" >> timeouts.txt
  shift 3
  "$@"
}
'''.replace('OFFSET', '-1' if expired else '60')
    invocation = '''
if run_analysis_layer P0 PRIMARY 300408 true; then echo success; else echo rejected; fi
run_analysis_layer P1 FAMILY 300408 false
'''
    result = subprocess.run(['bash'], input=harness + function + invocation,
                            cwd=tmp_path, text=True, capture_output=True, check=True)
    calls = tmp_path / 'calls.txt'
    if expired:
        assert not calls.exists()
        assert 'rejected' in result.stdout
    else:
        assert len(calls.read_text().splitlines()) == 4  # mandatory + optional A/H batches
        assert all('--close-analysis-date 2026-10-08' in line for line in calls.read_text().splitlines())
        assert len((tmp_path / 'timeouts.txt').read_text().splitlines()) == 4


def test_overnight_watchdog_exits_before_wait_or_dispatch(tmp_path):
    workflow = yaml.safe_load(Path('.github/workflows/00-daily-analysis.yml').read_text())
    script = next(s['run'] for s in workflow['jobs']['close-scan-watchdog']['steps']
                  if s['name'] == '兜底检查19:15全市场扫描')
    guard = script[script.index("if ! python3 - <<'PY'"):script.index('python3 scripts/market_scan_watchdog.py')]
    context = scheduled_close_context(run('2026-10-07T17:14:34Z'), schedule=CLOSE_CRON,
                                      now=local('2026-10-08T01:15:58'))
    audit = tmp_path / 'close-scan-watchdog.json'
    audit.write_text(json.dumps(context))
    subprocess.run(['bash'], input=guard + '\ntouch wrongly-dispatched\n', cwd=tmp_path,
                   capture_output=True, text=True, check=True)
    assert not (tmp_path / 'wrongly-dispatched').exists()
    assert json.loads(audit.read_text())['status'] == 'skipped_late'
