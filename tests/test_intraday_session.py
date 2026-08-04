import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

from scripts import intraday_session as intraday_session_module
from scripts.intraday_monitor import ReferenceLevels
from scripts.intraday_session import (
    RealtimeQuote,
    SessionError,
    TencentBatchQuoteFetcher,
    active_symbols_at,
    clear_removed_adaptive_plan_reviews,
    enqueue_adaptive_plan_reviews,
    flush_outbox,
    load_candidate_plans,
    load_reference_levels_batch,
    load_state_v2,
    parse_tencent_batch,
    resolve_session_end,
    run_cycle,
    run_session,
    save_state_v2,
    update_condition_state,
)


TZ = ZoneInfo("Asia/Shanghai")


def tencent_record(provider_symbol, name, price, previous_close, timestamp):
    fields = [""] * 50
    fields[0] = "51"
    fields[1] = name
    fields[2] = provider_symbol[2:]
    fields[3] = str(price)
    fields[4] = str(previous_close)
    fields[30] = timestamp
    fields[31] = str(price - previous_close)
    fields[32] = str((price - previous_close) / previous_close * 100)
    return f'v_{provider_symbol}="{"~".join(fields)}";'


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return self.payload.encode("gbk")


class FakeFetcher:
    min_interval_seconds = 0

    def __init__(self, batches):
        self.batches = list(batches)
        self.calls = []

    def fetch(self, symbols, *, now):
        self.calls.append((list(symbols), now))
        batch = self.batches.pop(0) if self.batches else {}
        if callable(batch):
            return batch(symbols, now)
        return dict(batch)


class FakeClock:
    def __init__(self, current):
        self.current = current
        self.sleeps = []

    def now(self):
        return self.current

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.current += timedelta(seconds=seconds)


def quote(symbol, price, change_pct, now, *, stale=False):
    return RealtimeQuote(
        symbol=symbol,
        name=symbol,
        price=price,
        change_pct=change_pct,
        provider_timestamp=now.isoformat(),
        fetched_at=now.isoformat(),
        stale_seconds=0,
        is_stale=stale,
        source="fake",
    )


class TencentBatchTests(unittest.TestCase):
    def test_mixed_a_h_batch_uses_one_request_and_enforces_freshness(self):
        now = datetime(2026, 7, 28, 10, 30, 30, tzinfo=TZ)
        payload = "\n".join(
            [
                tencent_record("sz300408", "三环集团", 40, 39, "20260728103000"),
                tencent_record("hk00981", "中芯国际", 51, 50, "20260728103000"),
            ]
        )
        calls = []

        def opener(outgoing, timeout):
            calls.append((outgoing.full_url, timeout))
            return FakeResponse(payload)

        fetcher = TencentBatchQuoteFetcher(opener=opener, chunk_size=50)
        result = fetcher.fetch(["300408", "HK00981"], now=now)

        self.assertEqual(len(calls), 1)
        self.assertIn("sz300408,hk00981", calls[0][0])
        self.assertEqual(result["300408"].price, 40)
        self.assertEqual(result["HK00981"].price, 51)
        self.assertFalse(result["300408"].is_stale)
        self.assertFalse(result["HK00981"].is_stale)
        self.assertEqual(result["300408"].source, "tencent_batch")

        stale_payload = tencent_record(
            "sz300408", "三环集团", 40, 39, "20260728102800"
        )
        stale = parse_tencent_batch(
            stale_payload, ["300408"], fetched_at=now, freshness_seconds=90
        )
        self.assertTrue(stale["300408"].is_stale)

    def test_exact_twelve_symbol_watchlist_is_fetched_in_one_batch(self):
        now = datetime(2026, 7, 28, 10, 30, 30, tzinfo=TZ)
        symbols = [
            "HK00981",
            "HK01347",
            "300408",
            "HK06181",
            "HK06166",
            "688333",
            "002185",
            "000100",
            "000725",
            "301308",
            "688825",
            "002647",
        ]
        provider_symbols = [
            "hk00981",
            "hk01347",
            "sz300408",
            "hk06181",
            "hk06166",
            "sh688333",
            "sz002185",
            "sz000100",
            "sz000725",
            "sz301308",
            "sh688825",
            "sz002647",
        ]
        payload = "\n".join(
            tencent_record(provider, provider, 40 + index, 39 + index, "20260728103000")
            for index, provider in enumerate(provider_symbols)
        )
        calls = []
        fetcher = TencentBatchQuoteFetcher(
            opener=lambda outgoing, timeout: calls.append(outgoing.full_url)
            or FakeResponse(payload),
            chunk_size=50,
        )

        result = fetcher.fetch(symbols, now=now)

        self.assertEqual(len(calls), 1)
        self.assertEqual(set(result), set(symbols))
        self.assertTrue(all(not item.is_stale for item in result.values()))

    def test_missing_timestamp_is_not_treated_as_fresh(self):
        now = datetime(2026, 7, 28, 10, 30, tzinfo=TZ)
        payload = tencent_record("sz300408", "三环集团", 40, 39, "")
        parsed = parse_tencent_batch(payload, ["300408"], fetched_at=now)
        self.assertTrue(parsed["300408"].is_stale)
        self.assertIsNone(parsed["300408"].provider_timestamp)


class ReferenceLevelTests(unittest.TestCase):
    def test_batch_loader_rejects_expired_signal_and_uses_fresh_history(self):
        now = datetime(2026, 7, 28, 10, 0, tzinfo=TZ)
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "reference.db"
            connection = sqlite3.connect(path)
            connection.executescript(
                """
                CREATE TABLE decision_signals (
                    id INTEGER PRIMARY KEY,
                    stock_code TEXT,
                    source_type TEXT,
                    status TEXT,
                    stop_loss REAL,
                    target_price REAL,
                    created_at TEXT,
                    expires_at TEXT
                );
                CREATE TABLE analysis_history (
                    id INTEGER PRIMARY KEY,
                    code TEXT,
                    stop_loss REAL,
                    take_profit REAL,
                    created_at TEXT
                );
                """
            )
            connection.execute(
                "INSERT INTO decision_signals VALUES "
                "(1, '300408', 'analysis', 'active', 99, 199, "
                "'2026-07-28 00:00:00', '2026-07-28 01:00:00')"
            )
            connection.execute(
                "INSERT INTO analysis_history VALUES "
                "(1, '300408', 48, 68, '2026-07-27 18:00:00')"
            )
            connection.commit()
            connection.close()

            levels = load_reference_levels_batch(path, ["300408"], now=now)

        self.assertEqual(levels["300408"].stop_loss, 48)
        self.assertEqual(levels["300408"].target_price, 68)
        self.assertEqual(levels["300408"].stop_source, "analysis_history")

    def test_batch_loader_rejects_undated_or_old_history(self):
        now = datetime(2026, 7, 28, 10, 0, tzinfo=TZ)
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "reference.db"
            connection = sqlite3.connect(path)
            connection.executescript(
                """
                CREATE TABLE analysis_history (
                    id INTEGER PRIMARY KEY,
                    code TEXT,
                    stop_loss REAL,
                    take_profit REAL,
                    created_at TEXT
                );
                INSERT INTO analysis_history VALUES
                    (1, '300408', 48, 68, '2026-07-01 00:00:00');
                """
            )
            connection.commit()
            connection.close()
            levels = load_reference_levels_batch(path, ["300408"], now=now)
        self.assertIsNone(levels["300408"].stop_loss)
        self.assertIsNone(levels["300408"].target_price)


class StateMachineTests(unittest.TestCase):
    def test_active_clear_reactivate_and_material_deterioration(self):
        now = datetime(2026, 7, 28, 10, 0, tzinfo=TZ)
        state = {
            "schema_version": 2,
            "symbols": {},
            "outbox": [],
            "provider": {},
            "updated_at": now.isoformat(),
        }
        levels = ReferenceLevels(stop_loss=50)

        first_quote = quote("300408", 49, -3.2, now)
        from scripts.intraday_monitor import evaluate_quote, QuoteSnapshot

        first_alerts = evaluate_quote(
            QuoteSnapshot(
                symbol="300408",
                name="三环集团",
                price=49,
                change_pct=-3.2,
            ),
            levels,
            down_threshold_pct=3,
            up_threshold_pct=5,
        )
        created = update_condition_state(
            state,
            now=now,
            quote=first_quote,
            alerts=first_alerts,
            levels=levels,
            cooldown_seconds=900,
            deterioration_pct=1,
        )
        self.assertEqual(created, 2)
        self.assertEqual(
            state["symbols"]["300408"]["conditions"]["stop_loss"]["status"],
            "active",
        )

        sent = []
        self.assertEqual(
            flush_outbox(
                state,
                now=now,
                sender=lambda **payload: sent.append(payload) or True,
            ),
            2,
        )
        self.assertEqual(len(sent), 1)
        self.assertEqual(state["outbox"], [])

        recovery_time = now + timedelta(minutes=2)
        update_condition_state(
            state,
            now=recovery_time,
            quote=quote("300408", 52, 0.5, recovery_time),
            alerts=[],
            levels=levels,
            cooldown_seconds=900,
            deterioration_pct=1,
        )
        self.assertEqual(
            state["symbols"]["300408"]["conditions"]["stop_loss"]["status"],
            "cleared",
        )

        repeat_time = now + timedelta(minutes=3)
        created = update_condition_state(
            state,
            now=repeat_time,
            quote=quote("300408", 49.5, -3.1, repeat_time),
            alerts=evaluate_quote(
                QuoteSnapshot(
                    symbol="300408",
                    name="三环集团",
                    price=49.5,
                    change_pct=-3.1,
                ),
                levels,
                down_threshold_pct=3,
                up_threshold_pct=5,
            ),
            levels=levels,
            cooldown_seconds=900,
            deterioration_pct=1,
        )
        self.assertEqual(created, 2)

        flush_outbox(
            state, now=repeat_time, sender=lambda **_payload: True
        )
        worse_time = repeat_time + timedelta(minutes=1)
        created = update_condition_state(
            state,
            now=worse_time,
            quote=quote("300408", 48.5, -4, worse_time),
            alerts=evaluate_quote(
                QuoteSnapshot(
                    symbol="300408",
                    name="三环集团",
                    price=48.5,
                    change_pct=-4,
                ),
                levels,
                down_threshold_pct=3,
                up_threshold_pct=5,
            ),
            levels=levels,
            cooldown_seconds=900,
            deterioration_pct=1,
        )
        self.assertGreaterEqual(created, 1)
        self.assertTrue(
            any(event["transition"] == "deteriorated" for event in state["outbox"])
        )

    def test_cooldown_and_severity_upgrade_can_realert(self):
        now = datetime(2026, 7, 28, 10, 0, tzinfo=TZ)
        state = {
            "schema_version": 2,
            "symbols": {
                "300408": {
                    "conditions": {
                        "sharp_drop": {
                            "status": "active",
                            "severity": "high",
                            "activated_at": now.isoformat(),
                            "cleared_at": None,
                            "last_notified_at": now.isoformat(),
                            "last_notified_price": 40,
                            "last_event_id": "old",
                        }
                    }
                }
            },
            "outbox": [],
            "provider": {},
            "updated_at": now.isoformat(),
        }
        from scripts.intraday_monitor import QuoteSnapshot, evaluate_quote

        cooldown_time = now + timedelta(minutes=16)
        cooldown_alert = evaluate_quote(
            QuoteSnapshot(
                symbol="300408", name="三环集团", price=40, change_pct=-3.2
            ),
            ReferenceLevels(),
            down_threshold_pct=3,
            up_threshold_pct=5,
        )
        created = update_condition_state(
            state,
            now=cooldown_time,
            quote=quote("300408", 40, -3.2, cooldown_time),
            alerts=cooldown_alert,
            levels=ReferenceLevels(),
            cooldown_seconds=900,
            deterioration_pct=1,
        )
        self.assertEqual(created, 1)
        self.assertEqual(state["outbox"][0]["transition"], "cooldown_repeat")

        state["outbox"] = []
        critical_time = cooldown_time + timedelta(minutes=1)
        critical_alert = evaluate_quote(
            QuoteSnapshot(
                symbol="300408", name="三环集团", price=39.9, change_pct=-5.1
            ),
            ReferenceLevels(),
            down_threshold_pct=3,
            up_threshold_pct=5,
        )
        created = update_condition_state(
            state,
            now=critical_time,
            quote=quote("300408", 39.9, -5.1, critical_time),
            alerts=critical_alert,
            levels=ReferenceLevels(),
            cooldown_seconds=900,
            deterioration_pct=1,
        )
        self.assertEqual(created, 1)
        self.assertEqual(state["outbox"][0]["transition"], "severity_up")
        self.assertEqual(state["outbox"][0]["severity"], "critical")

    def test_push_failure_keeps_outbox_and_retry_succeeds_after_restart(self):
        now = datetime(2026, 7, 28, 10, 0, tzinfo=TZ)
        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "state.json"
            state = {
                "schema_version": 2,
                "symbols": {},
                "outbox": [
                    {
                        "event_id": "event-1",
                        "created_at": now.isoformat(),
                        "symbol": "SYSTEM",
                        "name": "行情",
                        "condition": "data_quality",
                        "transition": "degraded",
                        "severity": "warning",
                        "price": None,
                        "change_pct": None,
                        "reference_price": None,
                        "message": "test",
                        "attempts": 0,
                        "next_attempt_at": now.isoformat(),
                    }
                ],
                "provider": {},
                "updated_at": now.isoformat(),
            }
            self.assertEqual(
                flush_outbox(state, now=now, sender=lambda **_payload: False), 0
            )
            self.assertEqual(state["outbox"][0]["attempts"], 1)
            save_state_v2(state_path, state)

            restored = load_state_v2(state_path, now=now)
            retry_time = now + timedelta(seconds=31)
            self.assertEqual(
                flush_outbox(
                    restored,
                    now=retry_time,
                    sender=lambda **_payload: True,
                ),
                1,
            )
            self.assertEqual(restored["outbox"], [])

    def test_failed_pending_event_is_cancelled_when_fresh_quote_clears_condition(self):
        from scripts.intraday_monitor import QuoteSnapshot, evaluate_quote

        now = datetime(2026, 7, 28, 10, 0, tzinfo=TZ)
        state = {
            "schema_version": 2,
            "symbols": {},
            "outbox": [],
            "provider": {},
            "updated_at": now.isoformat(),
        }
        levels = ReferenceLevels(stop_loss=50)
        alerts = evaluate_quote(
            QuoteSnapshot("300408", "三环集团", 49, -1),
            levels,
            down_threshold_pct=3,
            up_threshold_pct=5,
        )
        update_condition_state(
            state,
            now=now,
            quote=quote("300408", 49, -1, now),
            alerts=alerts,
            levels=levels,
            cooldown_seconds=900,
            deterioration_pct=1,
        )
        self.assertEqual(flush_outbox(state, now=now, sender=lambda **_: False), 0)
        self.assertEqual(len(state["outbox"]), 1)

        recovery = now + timedelta(seconds=31)
        update_condition_state(
            state,
            now=recovery,
            quote=quote("300408", 51, 0.2, recovery),
            alerts=[],
            levels=levels,
            cooldown_seconds=900,
            deterioration_pct=1,
        )
        calls = []
        self.assertEqual(
            flush_outbox(
                state,
                now=recovery,
                sender=lambda **payload: calls.append(payload) or True,
            ),
            0,
        )
        self.assertEqual(calls, [])
        self.assertEqual(state["outbox"], [])
        self.assertEqual(
            state["cancelled_events"][-1]["cancel_reason"],
            "condition_cleared_before_delivery",
        )

    def test_adaptive_reviews_require_complete_explicit_simulation_plan(self):
        now = datetime(2026, 7, 28, 10, 0, tzinfo=TZ)
        state = {
            "schema_version": 2,
            "symbols": {},
            "outbox": [],
            "provider": {},
            "updated_at": now.isoformat(),
        }
        complete = {
            "scope": "watchlist",
            "symbol": "300408",
            "market": "A",
            "entry_low": 99,
            "entry_high": 101,
            "plan_price": 100,
            "stop_loss": 95,
            "target_price": 120,
            "confidence": 0.8,
            "data_quality": "high",
            "expected_holding_days": 20,
            "position_state": "flat",
            "market_costs": {"entry_fee_bps": 10, "exit_fee_bps": 10},
        }
        self.assertEqual(
            enqueue_adaptive_plan_reviews(
                state,
                now=now,
                quotes=[quote("300408", 100, 1, now)],
                candidates=[complete],
            ),
            1,
        )
        self.assertIn("不是买入指令", state["outbox"][0]["message"])
        self.assertGreater(
            state["symbols"]["300408"]["adaptive_policy"][
                "incumbent_annualized_utility"
            ],
            0,
        )

        flush_outbox(state, now=now, sender=lambda **_: True)
        outside = now + timedelta(minutes=1)
        self.assertEqual(
            enqueue_adaptive_plan_reviews(
                state,
                now=outside,
                quotes=[quote("300408", 105, 1, outside)],
                candidates=[complete],
            ),
            0,
        )
        self.assertEqual(
            state["symbols"]["300408"]["adaptive_reviews"][
                "adaptive_entry_review"
            ]["status"],
            "cleared",
        )
        reentry = now + timedelta(minutes=2)
        self.assertEqual(
            enqueue_adaptive_plan_reviews(
                state,
                now=reentry,
                quotes=[quote("300408", 100, 0.5, reentry)],
                candidates=[complete],
            ),
            1,
        )

        unknown_holding = dict(complete)
        unknown_holding["symbol"] = "000725"
        self.assertEqual(
            enqueue_adaptive_plan_reviews(
                state,
                now=now,
                quotes=[quote("000725", 94, -2, now)],
                candidates=[unknown_holding],
            ),
            0,
        )
        self.assertFalse(any("卖出" in item["message"] for item in state["outbox"]))


class CycleTests(unittest.TestCase):
    def test_normal_is_60_seconds_and_near_threshold_is_30_seconds(self):
        now = datetime(2026, 7, 28, 10, 0, tzinfo=TZ)
        base_state = {
            "schema_version": 2,
            "symbols": {},
            "outbox": [],
            "provider": {},
            "updated_at": now.isoformat(),
        }
        normal = run_cycle(
            symbols=["300408", "HK00981"],
            state=base_state,
            levels={
                "300408": ReferenceLevels(stop_loss=30, target_price=60),
                "HK00981": ReferenceLevels(stop_loss=30, target_price=80),
            },
            fetcher=FakeFetcher(
                [
                    {
                        "300408": quote("300408", 40, 0.5, now),
                        "HK00981": quote("HK00981", 50, 0.4, now),
                    }
                ]
            ),
            now=now,
            notification_sender=lambda **_payload: True,
        )
        self.assertEqual(normal.next_interval_seconds, 60)

        near_state = {
            "schema_version": 2,
            "symbols": {},
            "outbox": [],
            "provider": {},
            "updated_at": now.isoformat(),
        }
        near = run_cycle(
            symbols=["300408", "HK00981"],
            state=near_state,
            levels={
                "300408": ReferenceLevels(stop_loss=39.8),
                "HK00981": ReferenceLevels(),
            },
            fetcher=FakeFetcher(
                [
                    {
                        "300408": quote("300408", 40, 0.5, now),
                        "HK00981": quote("HK00981", 50, 0.4, now),
                    }
                ]
            ),
            now=now,
            notification_sender=lambda **_payload: True,
        )
        self.assertEqual(near.next_interval_seconds, 30)

    def test_three_low_coverage_cycles_degrade_without_raising(self):
        now = datetime(2026, 7, 28, 10, 0, tzinfo=TZ)
        state = {
            "schema_version": 2,
            "symbols": {},
            "outbox": [],
            "provider": {},
            "updated_at": now.isoformat(),
        }
        missing = {
            "300408": RealtimeQuote(symbol="300408", is_stale=True),
            "HK00981": RealtimeQuote(symbol="HK00981", is_stale=True),
        }
        sender_calls = []
        results = []
        for index in range(3):
            results.append(
                run_cycle(
                    symbols=["300408", "HK00981"],
                    state=state,
                    levels={},
                    fetcher=FakeFetcher([missing]),
                    now=now + timedelta(minutes=index),
                    notification_sender=lambda **payload: sender_calls.append(payload)
                    or True,
                )
            )
        self.assertFalse(results[0].degraded)
        self.assertFalse(results[1].degraded)
        self.assertTrue(results[2].degraded)
        self.assertEqual(results[2].next_interval_seconds, 120)
        self.assertEqual(len(sender_calls), 1)

    def test_low_coverage_does_not_slow_a_fresh_near_risk_quote(self):
        now = datetime(2026, 7, 28, 10, 0, tzinfo=TZ)
        state = {
            "schema_version": 2,
            "symbols": {},
            "outbox": [],
            "provider": {"consecutive_low_coverage": 2},
            "updated_at": now.isoformat(),
        }
        result = run_cycle(
            symbols=["300408", "HK00981"],
            state=state,
            levels={"300408": ReferenceLevels(stop_loss=39.8)},
            fetcher=FakeFetcher(
                [
                    {
                        "300408": quote("300408", 40, 0.2, now),
                        "HK00981": RealtimeQuote(
                            symbol="HK00981", is_stale=True, source="missing"
                        ),
                    }
                ]
            ),
            now=now,
            notification_sender=lambda **_: True,
        )
        self.assertTrue(result.degraded)
        self.assertEqual(result.next_interval_seconds, 30)


class SessionLoopTests(unittest.TestCase):
    def test_market_filter_separates_a_and_h_sessions(self):
        now = datetime(2026, 7, 28, 15, 30, tzinfo=TZ)

        def resolver(market, _now):
            return "postmarket" if market == "cn" else "intraday"

        active = active_symbols_at(["300408", "HK00981"], now, resolver)
        self.assertEqual(active, ["HK00981"])

    def test_fake_clock_runs_to_absolute_end_without_real_sleep_or_network(self):
        start = datetime(2026, 7, 28, 15, 59, tzinfo=TZ)
        clock = FakeClock(start)

        def batch(symbols, now):
            return {
                symbol: quote(symbol, 40 if symbol == "300408" else 50, 0.2, now)
                for symbol in symbols
            }

        fetcher = FakeFetcher([batch, batch, batch, batch])
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            result = run_session(
                stocks="300408,HK00981",
                end_at=start + timedelta(seconds=125),
                database_path=root / "missing.db",
                state_path=root / "state.json",
                report_path=root / "report.md",
                fetcher=fetcher,
                phase_resolver=lambda _market, _now: "intraday",
                clock=clock.now,
                sleeper=clock.sleep,
                notification_sender=lambda **_payload: True,
            )

            self.assertEqual(result.cycles, 3)
            self.assertEqual(result.quote_cycles, 3)
            self.assertEqual(clock.sleeps, [60, 60, 5])
            self.assertEqual(len(fetcher.calls), 3)
            self.assertTrue(result.state_path.exists())
            self.assertTrue(result.report_path.exists())
            self.assertIn("不自动下单", result.report_path.read_text(encoding="utf-8"))
            saved = json.loads(result.state_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["schema_version"], 2)

    def test_full_a_h_holiday_exits_without_empty_polling(self):
        start = datetime(2026, 10, 1, 9, 17, tzinfo=TZ)
        clock = FakeClock(start)
        fetcher = FakeFetcher([])
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            result = run_session(
                stocks="300408,HK00981",
                end_at=start + timedelta(hours=3),
                database_path=root / "missing.db",
                state_path=root / "state.json",
                report_path=root / "report.md",
                fetcher=fetcher,
                phase_resolver=lambda _market, _now: "non_trading",
                clock=clock.now,
                sleeper=clock.sleep,
                notification_sender=lambda **_: True,
            )
            self.assertEqual(result.cycles, 0)
            self.assertEqual(result.quote_cycles, 0)
            self.assertEqual(result.termination_reason, "all_markets_non_trading")
            self.assertEqual(clock.sleeps, [])
            self.assertEqual(fetcher.calls, [])
            self.assertIn(
                "all_markets_non_trading",
                (root / "report.md").read_text(encoding="utf-8"),
            )

    def test_one_market_holiday_still_monitors_the_open_market(self):
        start = datetime(2026, 7, 28, 10, 0, tzinfo=TZ)
        clock = FakeClock(start)
        fetcher = FakeFetcher(
            [{"HK00981": quote("HK00981", 50, 0.2, start)}]
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            result = run_session(
                stocks="300408,HK00981",
                end_at=start + timedelta(minutes=5),
                database_path=root / "missing.db",
                state_path=root / "state.json",
                report_path=root / "report.md",
                fetcher=fetcher,
                phase_resolver=lambda market, _now: (
                    "non_trading" if market == "cn" else "intraday"
                ),
                clock=clock.now,
                sleeper=clock.sleep,
                notification_sender=lambda **_: True,
                max_cycles=1,
            )
        self.assertEqual(result.quote_cycles, 1)
        self.assertEqual(fetcher.calls[0][0], ["HK00981"])

    def test_missing_reference_database_is_visible_and_notified_once(self):
        start = datetime(2026, 7, 28, 10, 0, tzinfo=TZ)
        clock = FakeClock(start)
        notifications = []
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            run_session(
                stocks="300408",
                end_at=start + timedelta(minutes=5),
                database_path=root / "missing.db",
                state_path=root / "state.json",
                report_path=root / "report.md",
                fetcher=FakeFetcher(
                    [{"300408": quote("300408", 40, 0.2, start)}]
                ),
                phase_resolver=lambda _market, _now: "intraday",
                clock=clock.now,
                sleeper=clock.sleep,
                notification_sender=lambda **payload: notifications.append(payload)
                or True,
                max_cycles=1,
            )
            report = (root / "report.md").read_text(encoding="utf-8")
        self.assertEqual(len(notifications), 1)
        self.assertIn("参考位覆盖 0/1", notifications[0]["content"])
        self.assertIn("参考位覆盖：0/1", report)
        self.assertIn("Level-2 盘口", report)
        self.assertIn("unavailable", report)

    def test_trusted_market_scan_plan_adds_a_bounded_extra_quote_symbol(self):
        start = datetime(2026, 7, 28, 10, 0, tzinfo=TZ)
        clock = FakeClock(start)
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            plans = root / "latest.json"
            plans.write_text(
                json.dumps(
                    {
                        "generated_at": start.isoformat(),
                        "simulation_only": True,
                        "auto_order_enabled": False,
                        "human_confirmation_required": True,
                        "safe_to_push": True,
                        "review_complete": True,
                        "candidates": [
                            {
                                "code": "HK00700",
                                "name": "腾讯控股",
                                "market": "HK_CONNECT",
                                "action": "conditional_buy",
                                "research_status": "ready",
                                "eligible_for_intraday_review": True,
                                "review_complete": True,
                                "hard_risk_veto": False,
                                "model_disagreement": False,
                                "plan": {
                                    "entry_low": 99,
                                    "entry_high": 101,
                                    "entry_mid": 100,
                                    "stop_loss": 95,
                                    "take_profit_1": 120,
                                    "round_trip_cost_bps": 50,
                                },
                                "qwen_review": {"confidence": 0.8},
                                "deepseek_review": {"confidence": 0.75},
                                "data_availability": {
                                    "basic_quote": "available",
                                    "ohlcv": "available",
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            fetcher = FakeFetcher(
                [
                    {
                        "300408": quote("300408", 40, 0.2, start),
                        "HK00700": quote("HK00700", 100, 0.5, start),
                    }
                ]
            )
            run_session(
                stocks="300408",
                end_at=start + timedelta(minutes=5),
                database_path=root / "missing.db",
                state_path=root / "state.json",
                report_path=root / "report.md",
                candidate_plans_path=plans,
                fetcher=fetcher,
                phase_resolver=lambda _market, _now: "intraday",
                clock=clock.now,
                sleeper=clock.sleep,
                notification_sender=lambda **_: True,
                max_cycles=1,
            )
            saved = json.loads((root / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(fetcher.calls[0][0], ["300408", "HK00700"])
        self.assertEqual(
            saved["provider"]["candidate_plan_monitoring"]["extra_symbols"],
            ["HK00700"],
        )
        self.assertIn("adaptive_policy", saved["symbols"]["HK00700"])

    def test_safe_report_does_not_promote_deep_research_watch_to_entry_review(self):
        start = datetime(2026, 7, 28, 10, 0, tzinfo=TZ)
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "latest.json"
            path.write_text(
                json.dumps(
                    {
                        "generated_at": start.isoformat(),
                        "simulation_only": True,
                        "auto_order_enabled": False,
                        "human_confirmation_required": True,
                        "safe_to_push": True,
                        "review_complete": True,
                        "candidates": [
                            {
                                "code": "HK00700",
                                "action": "watch",
                                "research_status": "deep_research_required",
                                "review_complete": True,
                                "eligible_for_intraday_review": False,
                                "plan": {
                                    "entry_low": 99,
                                    "entry_high": 101,
                                    "stop_loss": 95,
                                    "take_profit_1": 120,
                                },
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            candidates = load_candidate_plans(path, now=start)

        self.assertEqual(candidates, [])

    def test_untrusted_scan_scope_cannot_bypass_root_contract(self):
        start = datetime(2026, 7, 28, 10, 0, tzinfo=TZ)
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "latest.json"
            path.write_text(
                json.dumps(
                    {
                        "generated_at": start.isoformat(),
                        "simulation_only": True,
                        "auto_order_enabled": False,
                        "human_confirmation_required": True,
                        "safe_to_push": False,
                        "review_complete": True,
                        "candidates": [
                            {
                                "code": "HK00700",
                                "scope": "simulation",
                                "action": "conditional_buy",
                                "research_status": "ready",
                                "review_complete": True,
                                "eligible_for_intraday_review": True,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            candidates = load_candidate_plans(path, now=start)

        self.assertEqual(candidates, [])

    def test_scan_plan_older_than_one_day_is_not_loaded(self):
        now = datetime(2026, 7, 28, 10, 0, tzinfo=TZ)
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "latest.json"
            path.write_text(
                json.dumps(
                    {
                        "generated_at": (now - timedelta(hours=25)).isoformat(),
                        "simulation_only": True,
                        "auto_order_enabled": False,
                        "human_confirmation_required": True,
                        "safe_to_push": True,
                        "review_complete": True,
                        "candidates": [
                            {
                                "code": "HK00700",
                                "action": "conditional_buy",
                                "research_status": "ready",
                                "review_complete": True,
                                "eligible_for_intraday_review": True,
                                "hard_risk_veto": False,
                                "model_disagreement": False,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            candidates = load_candidate_plans(path, now=now)

        self.assertEqual(candidates, [])

    def test_existing_scope_cannot_bypass_candidate_hard_risk_gate(self):
        start = datetime(2026, 7, 28, 10, 0, tzinfo=TZ)
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "latest.json"
            path.write_text(
                json.dumps(
                    {
                        "generated_at": start.isoformat(),
                        "simulation_only": True,
                        "auto_order_enabled": False,
                        "human_confirmation_required": True,
                        "safe_to_push": True,
                        "review_complete": True,
                        "candidates": [
                            {
                                "code": "HK00700",
                                "scope": "simulation",
                                "action": "watch",
                                "research_status": "deep_research_required",
                                "review_complete": True,
                                "eligible_for_intraday_review": True,
                                "hard_risk_veto": True,
                                "model_disagreement": False,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            candidates = load_candidate_plans(path, now=start)

        self.assertEqual(candidates, [])

    def test_removed_candidate_clears_active_review_and_failed_push_outbox(self):
        start = datetime(2026, 7, 28, 10, 0, tzinfo=TZ)
        state = {
            "schema_version": 2,
            "simulation_only": True,
            "places_real_orders": False,
            "symbols": {
                "HK00700": {
                    "conditions": {},
                    "adaptive_reviews": {
                        "adaptive_entry_review": {
                            "status": "active",
                            "fingerprint": "old-plan",
                        }
                    },
                }
            },
            "outbox": [
                {
                    "event_id": "old-entry",
                    "symbol": "HK00700",
                    "condition": "adaptive_entry_review",
                }
            ],
            "audit": [],
            "provider": {},
        }

        cleared = clear_removed_adaptive_plan_reviews(
            state,
            now=start,
            candidates=[],
        )

        review = state["symbols"]["HK00700"]["adaptive_reviews"][
            "adaptive_entry_review"
        ]
        self.assertEqual(cleared, 1)
        self.assertEqual(review["status"], "cleared")
        self.assertEqual(state["outbox"], [])
        self.assertEqual(
            state["cancelled_events"][-1]["cancel_reason"],
            "candidate_removed_or_scan_became_untrusted",
        )

    def test_same_symbol_invalid_plan_cancels_old_entry_outbox(self):
        start = datetime(2026, 7, 28, 10, 0, tzinfo=TZ)
        state = {
            "symbols": {
                "HK00700": {
                    "conditions": {},
                    "adaptive_reviews": {
                        "adaptive_entry_review": {
                            "status": "active",
                            "fingerprint": "old-plan",
                        }
                    },
                }
            },
            "outbox": [
                {
                    "event_id": "old-entry",
                    "symbol": "HK00700",
                    "condition": "adaptive_entry_review",
                }
            ],
            "cancelled_events": [],
        }
        invalid_candidate = {
            "code": "HK00700",
            "scope": "watchlist",
            "position_state": "flat",
            "plan": {
                "entry_low": 99,
                "entry_high": 101,
                "stop_loss": 95,
                # target and costs intentionally missing
            },
        }

        created = enqueue_adaptive_plan_reviews(
            state,
            now=start,
            quotes=[quote("HK00700", 100, 0.2, start)],
            candidates=[invalid_candidate],
        )

        self.assertEqual(created, 0)
        self.assertEqual(state["outbox"], [])
        self.assertEqual(
            state["symbols"]["HK00700"]["adaptive_reviews"][
                "adaptive_entry_review"
            ]["status"],
            "cleared",
        )
        self.assertEqual(
            state["cancelled_events"][-1]["cancel_reason"],
            "candidate_plan_or_quote_invalid_before_delivery",
        )

    def test_stale_quote_cancels_failed_entry_before_retry_delivery(self):
        start = datetime(2026, 7, 28, 10, 0, tzinfo=TZ)
        candidate = {
            "code": "HK00700",
            "scope": "watchlist",
            "position_state": "flat",
            "expected_holding_days": 10,
            "confidence": 0.8,
            "data_quality": "high",
            "market_costs": {
                "entry_fee_bps": 10,
                "exit_fee_bps": 10,
                "entry_slippage_bps": 15,
                "exit_slippage_bps": 15,
            },
            "plan": {
                "entry_low": 99,
                "entry_high": 101,
                "entry_mid": 100,
                "stop_loss": 95,
                "take_profit_1": 112,
            },
        }
        state = {"symbols": {}, "outbox": [], "cancelled_events": []}
        created = enqueue_adaptive_plan_reviews(
            state,
            now=start,
            quotes=[quote("HK00700", 100, 0.2, start)],
            candidates=[candidate],
        )
        self.assertEqual(created, 1)
        self.assertEqual(len(state["outbox"]), 1)

        stale_time = start + timedelta(minutes=1)
        enqueue_adaptive_plan_reviews(
            state,
            now=stale_time,
            quotes=[quote("HK00700", 100, 0.2, stale_time, stale=True)],
            candidates=[candidate],
        )
        notifications = []
        sent = flush_outbox(
            state,
            now=stale_time,
            sender=lambda **payload: notifications.append(payload) or True,
        )

        self.assertEqual(sent, 0)
        self.assertEqual(notifications, [])
        self.assertEqual(state["outbox"], [])
        self.assertEqual(
            state["cancelled_events"][-1]["cancel_reason"],
            "candidate_plan_or_quote_invalid_before_delivery",
        )

    def test_all_unknown_calendar_exits_as_explicit_degradation(self):
        start = datetime(2026, 7, 28, 9, 17, tzinfo=TZ)
        clock = FakeClock(start)
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            result = run_session(
                stocks="300408,HK00981",
                end_at=start + timedelta(hours=3),
                database_path=root / "missing.db",
                state_path=root / "state.json",
                report_path=root / "report.md",
                fetcher=FakeFetcher([]),
                phase_resolver=lambda _market, _now: "unknown",
                clock=clock.now,
                sleeper=clock.sleep,
            )
            report = (root / "report.md").read_text(encoding="utf-8")
        self.assertEqual(result.termination_reason, "calendar_unknown_no_open_market")
        self.assertEqual(clock.sleeps, [])
        self.assertIn("calendar_unknown_no_open_market", report)

    def test_expired_explicit_session_fails_instead_of_zero_loop_green(self):
        now = datetime(2026, 7, 28, 12, 10, tzinfo=TZ)
        self.assertEqual(
            resolve_session_end(now, "auto").hour,
            16,
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with self.assertRaisesRegex(SessionError, "监控时段已过期"):
                run_session(
                    stocks="300408",
                    end_at=datetime(2026, 7, 28, 12, 2, tzinfo=TZ),
                    database_path=root / "missing.db",
                    state_path=root / "state.json",
                    report_path=root / "report.md",
                    clock=lambda: now,
                )
            self.assertFalse((root / "state.json").exists())

    def test_late_scheduled_session_preserves_state_and_reports_explicit_skip(self):
        now = datetime(2026, 7, 28, 19, 5, tzinfo=TZ)
        notifications = []
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            state_path = root / "state.json"
            original = load_state_v2(state_path, now=now - timedelta(days=1))
            original["symbols"]["300408"] = {"conditions": {}}
            save_state_v2(state_path, original)

            result = run_session(
                stocks="300408",
                end_at=datetime(2026, 7, 28, 16, 2, tzinfo=TZ),
                database_path=root / "missing.db",
                state_path=state_path,
                report_path=root / "report.md",
                clock=lambda: now,
                late_start_policy="skip",
                notification_sender=lambda **payload: notifications.append(payload)
                or True,
            )
            restored = load_state_v2(state_path, now=now)
            report = (root / "report.md").read_text(encoding="utf-8")

        self.assertEqual(result.termination_reason, "late_schedule_skipped")
        self.assertEqual(result.quote_cycles, 0)
        self.assertIn("300408", restored["symbols"])
        self.assertEqual(
            restored["provider"]["session_status"], "late_schedule_skipped"
        )
        self.assertEqual(len(notifications), 1)
        self.assertIn("没有产生买卖信号", notifications[0]["content"])
        self.assertIn("late_schedule_skipped", report)
        self.assertIn("不补造历史盘中数据", report)

    def test_late_schedule_alert_retries_and_dedupes_each_session_window(self):
        morning = datetime(2026, 7, 28, 13, 5, tzinfo=TZ)
        afternoon = datetime(2026, 7, 28, 19, 5, tzinfo=TZ)
        attempts = []
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            state_path = root / "state.json"

            first = run_session(
                stocks="300408",
                end_at=datetime(2026, 7, 28, 12, 2, tzinfo=TZ),
                database_path=root / "missing.db",
                state_path=state_path,
                report_path=root / "morning.md",
                clock=lambda: morning,
                late_start_policy="skip",
                notification_sender=lambda **payload: attempts.append(payload)
                or False,
            )
            morning_state = load_state_v2(state_path, now=morning)
            second = run_session(
                stocks="300408",
                end_at=datetime(2026, 7, 28, 12, 2, tzinfo=TZ),
                database_path=root / "missing.db",
                state_path=state_path,
                report_path=root / "morning-repeat.md",
                clock=lambda: morning,
                late_start_policy="skip",
                notification_sender=lambda **payload: attempts.append(payload)
                or False,
            )
            after_repeat = load_state_v2(state_path, now=morning)
            third = run_session(
                stocks="300408",
                end_at=datetime(2026, 7, 28, 16, 2, tzinfo=TZ),
                database_path=root / "missing.db",
                state_path=state_path,
                report_path=root / "afternoon.md",
                clock=lambda: afternoon,
                late_start_policy="skip",
                notification_sender=lambda **payload: attempts.append(payload)
                or False,
            )
            final_state = load_state_v2(state_path, now=afternoon)

        self.assertEqual(first.events_created, 1)
        self.assertEqual(len(morning_state["outbox"]), 1)
        self.assertEqual(second.events_created, 0)
        self.assertEqual(len(after_repeat["outbox"]), 1)
        self.assertEqual(third.events_created, 1)
        self.assertEqual(len(final_state["outbox"]), 2)
        self.assertEqual(
            set(final_state["provider"]["late_schedule_notifications"]),
            {"2026-07-28:morning", "2026-07-28:afternoon"},
        )
        self.assertEqual(len(attempts), 2)

    def test_main_marks_late_skip_as_not_executed_in_github_summary(self):
        result = SimpleNamespace(
            termination_reason="late_schedule_skipped",
            cycles=0,
            quote_cycles=0,
            events_created=1,
            events_notified=0,
            final_pending_events=1,
            state_path=Path("data/intraday/session_state.json"),
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            summary_path = Path(temporary_directory) / "summary.md"
            with patch.object(
                intraday_session_module,
                "run_session",
                return_value=result,
            ), patch.dict(
                os.environ,
                {
                    "GITHUB_ACTIONS": "true",
                    "GITHUB_STEP_SUMMARY": str(summary_path),
                },
                clear=False,
            ), patch("builtins.print") as printer:
                exit_code = intraday_session_module.main(
                    ["--stocks", "300408", "--late-start-policy", "skip"]
                )
            summary = summary_path.read_text(encoding="utf-8")

        output = "\n".join(str(call.args[0]) for call in printer.call_args_list)
        self.assertEqual(exit_code, 0)
        self.assertIn("监控未执行", output)
        self.assertIn("::warning", output)
        self.assertIn("监控未执行", summary)

    def test_workflow_is_independent_and_never_overwrites_with_empty_state(self):
        workflow = (
            Path(__file__).resolve().parents[1]
            / ".github"
            / "workflows"
            / "01-intraday-session.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("cron: '17 1 * * 1-5'", workflow)
        self.assertIn("cron: '47 4 * * 1-5'", workflow)
        self.assertIn("timeout-minutes: 210", workflow)
        self.assertIn("group: intraday-session-", workflow)
        self.assertNotIn("RANDOM", workflow)
        self.assertIn("id: package_state", workflow)
        self.assertIn("steps.package_state.outputs.available == 'true'", workflow)
        self.assertNotIn('"updated_at":null', workflow)
        self.assertIn("paper-close-state&per_page=100", workflow)
        self.assertIn("market-scan-state&per_page=100", workflow)
        self.assertIn("data/market_scan/latest.json", workflow)
        self.assertIn("PRAGMA quick_check", workflow)
        self.assertIn("name: intraday-alert-state", workflow)
        self.assertIn("load_state_v2(", workflow)
        self.assertIn("不上传、不缓存损坏状态", workflow)
        self.assertIn('LATE_START_POLICY="skip"', workflow)
        self.assertIn('SESSION="morning"', workflow)
        self.assertIn('SESSION="afternoon"', workflow)
        self.assertIn('--late-start-policy "$LATE_START_POLICY"', workflow)
        daily_workflow = (
            Path(__file__).resolve().parents[1]
            / ".github"
            / "workflows"
            / "00-daily-analysis.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("MINUTE_INTRADAY_MONITOR_ENABLED", daily_workflow)
        self.assertIn("避免重复推送", daily_workflow)
        self.assertNotIn("cron: '30 2 * * 1-5'", daily_workflow)
        self.assertNotIn("cron: '30 6 * * 1-5'", daily_workflow)

    def test_monitor_module_has_no_broker_or_llm_import(self):
        source = (
            Path(__file__).resolve().parents[1] / "scripts" / "intraday_session.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("src.brokers", source)
        self.assertNotIn("litellm", source.lower())
        self.assertNotIn("GeminiAnalyzer", source)


if __name__ == "__main__":
    unittest.main()
