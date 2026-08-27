import base64
import json
import os
import sqlite3
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping
from unittest.mock import patch
from zoneinfo import ZoneInfo

from scripts import intraday_session as intraday_session_module
from scripts.intraday_monitor import ReferenceLevels
from scripts.intraday_session import (
    DecisionQuoteFetcher,
    LongbridgeBatchQuoteFetcher,
    RealtimeQuote,
    SessionError,
    TencentBatchQuoteFetcher,
    active_symbols_at,
    clear_removed_adaptive_plan_reviews,
    enqueue_adaptive_plan_reviews,
    enqueue_cash_available_candidate_rechecks,
    flush_outbox,
    load_candidate_plans,
    load_fixed_watch_candidate_plans,
    load_reference_levels_batch,
    load_state_v2,
    parse_tencent_batch,
    parse_longbridge_batch,
    parse_longbridge_timestamp,
    process_actionable_decisions,
    process_watch_account_decisions,
    resolve_session_end,
    run_cycle,
    run_session,
    save_state_v2,
    update_condition_state,
)
from scripts.account_watchlists import (
    PRIMARY_SYMBOLS,
    all_quote_symbols,
    load_private_watch_config,
    priority_analysis_pools,
    watch_contexts_by_symbol,
)
from scripts.shadow_ab_experiment import (
    INITIAL_INSTRUMENTS,
    initialize_state as initialize_shadow_state,
    record_daily_nav as record_shadow_daily_nav,
    record_signal as record_shadow_signal,
    save_state as save_shadow_state,
)
from scripts.check_intraday_integrations import verify_state


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


class FakeLongbridgeFetcher(FakeFetcher):
    min_interval_seconds = 30
    configured = True
    realtime_entitled = True


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


def synthetic_shadow_state():
    return initialize_shadow_state(
        {
            "positions": [
                {
                    "symbol": item["symbol"],
                    "quantity": 100,
                    "historical_cost": item["verified_close"],
                }
                for item in INITIAL_INSTRUMENTS
            ]
        },
        created_at=datetime(2026, 8, 9, 10, tzinfo=TZ),
    )


def write_fixed_candidate_signal(path, now, **overrides):
    values = {
        "id": 1,
        "stock_code": "002759",
        "stock_name": "ST天际",
        "source_type": "analysis",
        "source_report_id": 42,
        "action": "buy",
        "confidence": 1.0,
        "entry_low": 9.8,
        "entry_high": 10.2,
        "stop_loss": 9.0,
        "target_price": 12.0,
        "data_quality_summary_json": json.dumps({"level": "high"}),
        "plan_quality": "complete",
        "status": "active",
        "created_at": (now - timedelta(hours=12))
        .astimezone(timezone.utc)
        .strftime("%Y-%m-%d %H:%M:%S"),
        "expires_at": (now + timedelta(days=1))
        .astimezone(timezone.utc)
        .strftime("%Y-%m-%d %H:%M:%S"),
    }
    values.update(overrides)
    connection = sqlite3.connect(path)
    connection.execute(
        """
        CREATE TABLE decision_signals (
            id INTEGER PRIMARY KEY,
            stock_code TEXT,
            stock_name TEXT,
            source_type TEXT,
            source_report_id INTEGER,
            action TEXT,
            confidence REAL,
            entry_low REAL,
            entry_high REAL,
            stop_loss REAL,
            target_price REAL,
            data_quality_summary_json TEXT,
            plan_quality TEXT,
            status TEXT,
            expires_at TEXT,
            created_at TEXT
        )
        """
    )
    columns = tuple(values)
    connection.execute(
        f"INSERT INTO decision_signals ({','.join(columns)}) "
        f"VALUES ({','.join('?' for _ in columns)})",
        tuple(values[column] for column in columns),
    )
    connection.commit()
    connection.close()


class AccountWatchLayerTests(unittest.TestCase):
    def test_priority_pools_are_canonical_and_cross_tier_deduplicated(self):
        pools = priority_analysis_pools()
        self.assertEqual(pools["P0_PRIMARY"], PRIMARY_SYMBOLS)
        flattened = [symbol for values in pools.values() for symbol in values]
        self.assertEqual(len(flattened), len(set(flattened)))
        self.assertEqual(all_quote_symbols(), tuple(flattened))
        self.assertIn("563230", pools["P2_ACCOUNT_HOLDINGS"])
        self.assertEqual(pools["P3_CANDIDATES"], ("002759",))
        self.assertNotIn("HK01347", pools["P2_ACCOUNT_HOLDINGS"])

    def test_fixed_secondary_candidate_reuses_fresh_p3_plan_without_primary_signal(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        with tempfile.TemporaryDirectory() as temporary_directory:
            database_path = Path(temporary_directory) / "analysis.db"
            write_fixed_candidate_signal(database_path, now, stock_code="SZ002759")
            plans = load_fixed_watch_candidate_plans(database_path, now=now)

        self.assertEqual(len(plans), 1)
        self.assertEqual(plans[0]["symbol"], "002759")
        self.assertEqual(plans[0]["position_state"], "flat")
        state = intraday_session_module._empty_state(now)
        shadow = synthetic_shadow_state()

        def forbidden_recorder(*_args, **_kwargs):
            raise AssertionError("fixed watch candidate reached PRIMARY recorder")

        result = run_cycle(
            symbols=["300408", "002759"],
            primary_symbols=["300408"],
            state=state,
            levels={},
            fetcher=FakeFetcher(
                [
                    {
                        "300408": quote("300408", 40, 0.1, now),
                        "002759": quote("002759", 10, 0.2, now),
                    }
                ]
            ),
            now=now,
            candidate_plans=plans,
            shadow_state=shadow,
            shadow_signal_recorder=forbidden_recorder,
            notification_sender=lambda **_: False,
        )

        self.assertEqual(result.coverage, 1.0)
        self.assertEqual(shadow["signal_ledger"], [])
        self.assertEqual(shadow["execution_ledger"], [])
        self.assertEqual(state["outbox"], [])
        self.assertEqual(
            state["event_ledger"][-1]["watch_decision_results"]
            ["SECONDARY_ACCOUNT_WATCH"],
            "no_operation_no_explicit_buy_size",
        )

    def test_fixed_candidate_plan_loader_fails_closed_without_entry_zone(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        with tempfile.TemporaryDirectory() as temporary_directory:
            database_path = Path(temporary_directory) / "analysis.db"
            write_fixed_candidate_signal(database_path, now, entry_high=None)
            plans = load_fixed_watch_candidate_plans(database_path, now=now)
        self.assertEqual(plans, [])

    def test_fixed_candidate_plan_loader_rejects_expired_or_old_plan(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        cases = {
            "expired": {
                "expires_at": (now - timedelta(minutes=1))
                .astimezone(timezone.utc)
                .strftime("%Y-%m-%d %H:%M:%S")
            },
            "old": {
                "created_at": (now - timedelta(days=8))
                .astimezone(timezone.utc)
                .strftime("%Y-%m-%d %H:%M:%S")
            },
        }
        for label, overrides in cases.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as temporary_directory:
                database_path = Path(temporary_directory) / "analysis.db"
                write_fixed_candidate_signal(database_path, now, **overrides)
                self.assertEqual(
                    load_fixed_watch_candidate_plans(database_path, now=now),
                    [],
                )

    def test_fixed_candidate_requires_good_data_and_fresh_quote(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        with tempfile.TemporaryDirectory() as temporary_directory:
            database_path = Path(temporary_directory) / "analysis.db"
            write_fixed_candidate_signal(
                database_path,
                now,
                data_quality_summary_json=json.dumps({"level": "limited"}),
            )
            degraded_plan = load_fixed_watch_candidate_plans(database_path, now=now)
        self.assertEqual(len(degraded_plan), 1)
        state = intraday_session_module._empty_state(now)
        self.assertEqual(
            enqueue_adaptive_plan_reviews(
                state,
                now=now,
                quotes=[quote("002759", 10, 0.2, now)],
                candidates=degraded_plan,
            ),
            0,
        )

        fresh_plan = dict(degraded_plan[0])
        fresh_plan["data_quality"] = "high"
        self.assertEqual(
            enqueue_adaptive_plan_reviews(
                state,
                now=now,
                quotes=[quote("002759", 10, 0.2, now, stale=True)],
                candidates=[fresh_plan],
            ),
            0,
        )

    def test_fixed_candidate_same_plan_pushes_once_until_zone_reentry(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        plan = {
            "scope": "watchlist",
            "symbol": "002759",
            "name": "ST天际",
            "position_state": "flat",
            "expected_holding_days": 20,
            "confidence": 0.8,
            "data_quality": "high",
            "market_costs": {
                "entry_fee_bps": 10,
                "exit_fee_bps": 10,
                "entry_slippage_bps": 5,
                "exit_slippage_bps": 5,
            },
            "plan": {
                "entry_low": 9.8,
                "entry_high": 10.2,
                "plan_price": 10,
                "stop_loss": 9,
                "target_price": 12,
            },
        }
        state = intraday_session_module._empty_state(now)

        def cycle(price, checked_at):
            ledger_start = len(state.get("event_ledger", []))
            raw_created = enqueue_adaptive_plan_reviews(
                state,
                now=checked_at,
                quotes=[quote("002759", price, 0.2, checked_at)],
                candidates=[plan],
            )
            raw_events = state.get("event_ledger", [])[ledger_start:]
            watch_created = process_watch_account_decisions(
                state,
                now=checked_at,
                raw_events=raw_events,
                levels={},
            )
            return raw_created, watch_created

        self.assertEqual(cycle(10, now), (1, 0))
        self.assertEqual(
            flush_outbox(state, now=now, sender=lambda **_: True),
            0,
        )
        self.assertEqual(cycle(10, now + timedelta(minutes=1)), (0, 0))
        self.assertEqual(
            enqueue_adaptive_plan_reviews(
                state,
                now=now + timedelta(minutes=2),
                quotes=[
                    quote(
                        "002759",
                        10,
                        0.2,
                        now + timedelta(minutes=2),
                        stale=True,
                    )
                ],
                candidates=[plan],
            ),
            0,
        )
        self.assertEqual(cycle(10, now + timedelta(minutes=3)), (1, 0))
        clear_removed_adaptive_plan_reviews(
            state,
            now=now + timedelta(minutes=4),
            candidates=[],
        )
        self.assertEqual(
            state["watch_decision_notifications"]
            ["SECONDARY_ACCOUNT_WATCH"]["002759"]["adaptive_entry_review"],
            {},
        )
        self.assertEqual(cycle(10, now + timedelta(minutes=5)), (1, 0))
        self.assertEqual(cycle(10.5, now + timedelta(minutes=6)), (0, 0))
        self.assertEqual(cycle(10, now + timedelta(minutes=7)), (1, 0))

        state = intraday_session_module._empty_state(now)
        self.assertEqual(cycle(10, now), (1, 0))
        self.assertEqual(
            flush_outbox(state, now=now, sender=lambda **_: False),
            0,
        )
        clear_removed_adaptive_plan_reviews(
            state,
            now=now + timedelta(minutes=1),
            candidates=[],
        )
        self.assertEqual(state["outbox"], [])
        self.assertEqual(cycle(10, now + timedelta(minutes=2)), (1, 0))

    def test_private_watch_config_accepts_only_runtime_held_positions(self):
        parsed = load_private_watch_config(
            json.dumps(
                {
                    "secondary_account": {
                        "positions": [
                            {
                                "symbol": "563230",
                                "quantity": 1000,
                                "historical_cost": 1.23,
                            }
                        ],
                        "informational_snapshot": {
                            "captured_at": "2026-01-01T00:00:00+08:00",
                            "cash": 12345,
                        },
                    },
                    "sister_managed": {
                        "positions": [
                            {
                                "symbol": "002594",
                                "quantity": 100,
                                "historical_cost": 50,
                            }
                        ]
                    },
                }
            )
        )
        self.assertEqual(parsed["secondary_account"]["positions"][0]["symbol"], "563230")
        self.assertEqual(parsed["sister_managed"]["positions"][0]["symbol"], "002594")
        with self.assertRaises(ValueError):
            load_private_watch_config(
                json.dumps(
                    {
                        "secondary_account": {
                            "positions": [
                                {
                                    "symbol": "002759",
                                    "quantity": 100,
                                    "historical_cost": 10,
                                }
                            ]
                        }
                    }
                )
            )

    def test_side_quote_failures_do_not_change_primary_coverage_or_degrade(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        state = intraday_session_module._empty_state(now)
        result = run_cycle(
            symbols=["300408", "000100", "HK09988"],
            primary_symbols=["300408"],
            state=state,
            levels={},
            fetcher=FakeFetcher(
                [
                    {
                        "300408": quote("300408", 40, 0.1, now),
                        "000100": RealtimeQuote(symbol="000100", is_stale=True),
                        "HK09988": RealtimeQuote(symbol="HK09988", is_stale=True),
                    }
                ]
            ),
            now=now,
            min_quote_coverage=1.0,
            low_coverage_limit=1,
            notification_sender=lambda **_: True,
        )
        self.assertEqual(result.coverage, 1.0)
        self.assertFalse(result.degraded)
        coverage = state["provider"]["coverage_by_account_layer"]
        self.assertEqual(coverage["PRIMARY_PORTFOLIO"]["coverage"], 1.0)
        self.assertFalse(
            coverage["FAMILY_WATCHLIST"]["drives_primary_degradation"]
        )
        self.assertFalse(
            coverage["SISTER_MANAGED_WATCH"]["drives_primary_degradation"]
        )

    def test_live_hk_snapshot_without_recent_trade_does_not_degrade_primary(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        state = intraday_session_module._empty_state(now)
        inactive_snapshot = RealtimeQuote(
            symbol="HK02522",
            name="一脉阳光",
            price=4.82,
            provider_timestamp=(now - timedelta(minutes=4)).isoformat(),
            fetched_at=now.isoformat(),
            stale_seconds=240,
            provider_snapshot_fresh=True,
            is_stale=True,
            source="longbridge_batch",
        )

        result = run_cycle(
            symbols=["HK02522"],
            primary_symbols=["HK02522"],
            state=state,
            levels={"HK02522": ReferenceLevels(stop_loss=5.0)},
            fetcher=FakeFetcher([{"HK02522": inactive_snapshot}]),
            now=now,
            min_quote_coverage=1.0,
            low_coverage_limit=1,
            notification_sender=lambda **_: True,
        )

        self.assertEqual(result.valid_quote_count, 1)
        self.assertEqual(result.coverage, 1.0)
        self.assertFalse(result.degraded)
        self.assertEqual(state["outbox"], [])
        self.assertEqual(state["event_ledger"], [])

    def test_side_layer_never_calls_primary_signal_recorder(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        state = intraday_session_module._empty_state(now)
        called = []

        def forbidden_recorder(*_args, **_kwargs):
            called.append(True)
            raise AssertionError("watch layer must not record a PRIMARY signal")

        result = run_cycle(
            symbols=["300408", "000100"],
            primary_symbols=["300408"],
            state=state,
            levels={"000100": ReferenceLevels(stop_loss=9.9)},
            fetcher=FakeFetcher(
                [
                    {
                        "300408": quote("300408", 40, 0.1, now),
                        "000100": quote("000100", 9.8, -2.0, now),
                    }
                ]
            ),
            now=now,
            shadow_state=synthetic_shadow_state(),
            shadow_signal_recorder=forbidden_recorder,
            notification_sender=lambda **_: False,
        )
        self.assertFalse(called)
        self.assertEqual(result.coverage, 1.0)
        self.assertEqual(len(state["outbox"]), 1)
        payload = state["outbox"][0]["payload"]
        self.assertEqual(payload["account_layer"], "FAMILY_WATCHLIST")
        self.assertIs(payload["primary_shadow_eligible"], False)
        self.assertEqual(
            state["outbox"][0]["condition"],
            "watch:FAMILY_WATCHLIST:stop_loss",
        )

    def test_preexisting_watch_only_pending_signal_cannot_execute_on_side_quote(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        shadow = synthetic_shadow_state()
        shadow["strategy_shadow_portfolio"]["positions"]["000100"] = {
            "symbol": "000100",
            "name": "synthetic side position",
            "currency": "CNY",
            "quantity": 100,
            "historical_cost": 10,
            "experiment_basis_price": 10,
        }
        shadow["strategy_shadow_portfolio"]["last_prices"]["000100"] = 10
        signal = record_shadow_signal(
            shadow,
            event_id="legacy-side-pending",
            symbol="000100",
            signal_time=(now - timedelta(seconds=5)).isoformat(),
            quote_time=(now - timedelta(seconds=10)).isoformat(),
            signal_price=10,
            action="模拟清仓",
            reason="synthetic pending signal must remain isolated",
        )
        state = intraday_session_module._empty_state(now)

        run_cycle(
            symbols=["300408", "000100"],
            primary_symbols=["300408"],
            state=state,
            levels={},
            fetcher=FakeFetcher(
                [
                    {
                        "300408": quote("300408", 40, 0.1, now),
                        "000100": quote("000100", 11, 1.0, now),
                    }
                ]
            ),
            now=now,
            shadow_state=shadow,
            notification_sender=lambda **_: False,
        )

        self.assertIn(signal["signal_id"], shadow["pending_signal_ids"])
        self.assertEqual(shadow["execution_ledger"], [])
        self.assertEqual(shadow["trades"], [])
        self.assertEqual(
            shadow["strategy_shadow_portfolio"]["positions"]["000100"][
                "quantity"
            ],
            100,
        )

    def test_secondary_and_sister_alerts_never_enter_primary_execution(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        shadow = synthetic_shadow_state()
        state = intraday_session_module._empty_state(now)

        def forbidden_recorder(*_args, **_kwargs):
            raise AssertionError("side-account alert reached PRIMARY recorder")

        run_cycle(
            symbols=["300408", "563230", "HK09988"],
            primary_symbols=["300408"],
            state=state,
            levels={
                "563230": ReferenceLevels(target_price=1.1),
                "HK09988": ReferenceLevels(target_price=100),
            },
            fetcher=FakeFetcher(
                [
                    {
                        "300408": quote("300408", 40, 0.1, now),
                        "563230": quote("563230", 1.2, 1.0, now),
                        "HK09988": quote("HK09988", 105, 1.0, now),
                    }
                ]
            ),
            now=now,
            shadow_state=shadow,
            shadow_signal_recorder=forbidden_recorder,
            notification_sender=lambda **_: False,
        )
        self.assertEqual(shadow["signal_ledger"], [])
        self.assertEqual(shadow["execution_ledger"], [])
        self.assertEqual(shadow["trades"], [])
        self.assertEqual(
            {
                event["payload"]["account_layer"]
                for event in state["outbox"]
            },
            {"SECONDARY_ACCOUNT_WATCH", "SISTER_MANAGED_WATCH"},
        )

    def test_same_account_symbol_cycle_emits_only_strongest_watch_decision(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        state = intraday_session_module._empty_state(now)
        raw_events = [
            {
                "event_id": "target",
                "symbol": "000100",
                "name": "TCL科技",
                "condition": "target_reached",
                "transition": "activated",
                "severity": "warning",
                "price": 10.5,
                "reference_price": 10.0,
                "payload": {
                    "data_quality": "fresh_l1",
                    "quote_time": now.isoformat(),
                },
            },
            {
                "event_id": "stop",
                "symbol": "000100",
                "name": "TCL科技",
                "condition": "stop_loss",
                "transition": "activated",
                "severity": "critical",
                "price": 9.0,
                "reference_price": 9.5,
                "payload": {
                    "data_quality": "fresh_l1",
                    "quote_time": now.isoformat(),
                },
            },
        ]
        created = process_watch_account_decisions(
            state,
            now=now,
            raw_events=raw_events,
            levels={
                "000100": ReferenceLevels(stop_loss=9.5, target_price=10.0)
            },
        )
        self.assertEqual(created, 1)
        self.assertEqual(len(state["outbox"]), 1)
        self.assertEqual(
            state["outbox"][0]["payload"]["action"],
            "建议卖出全部持仓",
        )
        sent = []
        self.assertEqual(
            flush_outbox(
                state,
                now=now,
                sender=lambda **payload: sent.append(payload) or True,
            ),
            1,
        )
        self.assertTrue(sent[0]["title"].startswith("【父亲账户观察】"))
        self.assertIn("【父亲账户观察】TCL科技", sent[0]["content"])

    def test_secondary_candidate_can_observe_but_never_receive_sell_action(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        state = intraday_session_module._empty_state(now)
        stop_event = {
            "event_id": "candidate-stop",
            "symbol": "002759",
            "name": "ST天际",
            "condition": "stop_loss",
            "transition": "activated",
            "severity": "critical",
            "price": 8.0,
            "reference_price": 8.5,
            "payload": {
                "data_quality": "fresh_l1",
                "quote_time": now.isoformat(),
            },
        }
        self.assertEqual(
            process_watch_account_decisions(
                state,
                now=now,
                raw_events=[stop_event],
                levels={"002759": ReferenceLevels(stop_loss=8.5)},
            ),
            0,
        )
        for condition in ("target_reached", "adaptive_risk_review"):
            with self.subTest(condition=condition):
                isolated_state = intraday_session_module._empty_state(now)
                candidate_sell_event = {
                    **stop_event,
                    "event_id": f"candidate-{condition}",
                    "condition": condition,
                    "payload": {
                        **stop_event["payload"],
                        "stop_loss": 8.5,
                        "target_price": 10.5,
                    },
                }
                self.assertEqual(
                    process_watch_account_decisions(
                        isolated_state,
                        now=now,
                        raw_events=[candidate_sell_event],
                        levels={
                            "002759": ReferenceLevels(
                                stop_loss=8.5,
                                target_price=10.5,
                            )
                        },
                    ),
                    0,
                )
                self.assertEqual(isolated_state["outbox"], [])
        entry_event = {
            **stop_event,
            "event_id": "candidate-entry",
            "condition": "adaptive_entry_review",
            "severity": "warning",
            "price": 9.0,
            "payload": {
                "data_quality": "fresh_l1",
                "quote_time": now.isoformat(),
                "entry_low": 8.8,
                "entry_high": 9.2,
                "stop_loss": 8.5,
                "target_price": 10.5,
                "plan_fingerprint": "plan-1",
            },
        }
        self.assertEqual(
            process_watch_account_decisions(
                state,
                now=now,
                raw_events=[entry_event],
                levels={},
            ),
            0,
        )
        self.assertEqual(state["outbox"], [])

    def test_shared_symbol_has_one_quote_but_separate_account_contexts(self):
        symbols = all_quote_symbols()
        self.assertEqual(symbols.count("563230"), 1)
        contexts = watch_contexts_by_symbol()["563230"]
        self.assertEqual(
            {context["layer"] for context in contexts},
            {"SECONDARY_ACCOUNT_WATCH", "SISTER_MANAGED_WATCH"},
        )


class TencentBatchTests(unittest.TestCase):
    def test_longbridge_fetcher_caps_hk_batches_at_twenty(self):
        now = datetime(2026, 8, 11, 14, 45, tzinfo=TZ)
        calls = []

        class FakeContext:
            def quote_package_details(self):
                return [
                    SimpleNamespace(
                        key="HK_L1_OpenAPI",
                        name="LV1 Real-time Quotes",
                        description="",
                    )
                ]

            def quote(self, provider_symbols):
                calls.append(list(provider_symbols))
                return [
                    {
                        "symbol": symbol,
                        "last_done": "10.5",
                        "prev_close": "10",
                        "timestamp": now,
                    }
                    for symbol in provider_symbols
                ]

        symbols = [f"HK{index:05d}" for index in range(1, 22)]
        fetcher = LongbridgeBatchQuoteFetcher(
            configured=True,
            chunk_size=999,
            context_factory=FakeContext,
        )

        result = fetcher.fetch(symbols, now=now)

        self.assertEqual([len(chunk) for chunk in calls], [20, 1])
        self.assertEqual(set(result), set(symbols))
        self.assertTrue(all(not item.is_stale for item in result.values()))
        self.assertTrue(
            all(item.provider_snapshot_fresh for item in result.values())
        )

    def test_longbridge_fetcher_fails_closed_without_hk_realtime_package(self):
        now = datetime(2026, 8, 11, 14, 45, tzinfo=TZ)

        class BasicOnlyContext:
            def quote_package_details(self):
                return [
                    SimpleNamespace(
                        key="HK_Basic",
                        name="15-min Delay",
                        description="",
                    )
                ]

            def quote(self, provider_symbols):
                return [
                    {
                        "symbol": symbol,
                        "last_done": "10.5",
                        "prev_close": "10",
                        "timestamp": now - timedelta(minutes=15),
                    }
                    for symbol in provider_symbols
                ]

        fetcher = LongbridgeBatchQuoteFetcher(
            configured=True,
            context_factory=BasicOnlyContext,
        )

        result = fetcher.fetch(["HK00700"], now=now)["HK00700"]

        self.assertFalse(fetcher.realtime_entitled)
        self.assertFalse(result.provider_snapshot_fresh)
        self.assertTrue(result.is_stale)
        self.assertEqual(result.source, "longbridge_realtime_permission_missing")

    def test_oauth_cache_secret_accepts_wrapped_base64(self):
        payload = b'{"access_token":"test-only"}'
        encoded = base64.b64encode(payload).decode("ascii")
        wrapped = f"{encoded[:8]}\n{encoded[8:]}"
        with tempfile.TemporaryDirectory() as temporary_directory:
            cache_path = Path(temporary_directory) / "oauth-cache"
            with patch.dict(
                os.environ,
                {"LONGBRIDGE_OAUTH_TOKEN_CACHE_B64": wrapped},
                clear=False,
            ), patch.object(
                intraday_session_module,
                "_longbridge_oauth_cache_path",
                return_value=cache_path,
            ):
                restored = intraday_session_module._restore_longbridge_oauth_cache(
                    "test-client"
                )

            self.assertEqual(restored, cache_path)
            self.assertEqual(cache_path.read_bytes(), payload)

    def test_oauth_cache_secret_rejects_non_json_payload(self):
        encoded = base64.b64encode(b"not-json").decode("ascii")
        with tempfile.TemporaryDirectory() as temporary_directory:
            cache_path = Path(temporary_directory) / "oauth-cache"
            with patch.dict(
                os.environ,
                {"LONGBRIDGE_OAUTH_TOKEN_CACHE_B64": encoded},
                clear=False,
            ), patch.object(
                intraday_session_module,
                "_longbridge_oauth_cache_path",
                return_value=cache_path,
            ):
                with self.assertRaises(SessionError):
                    intraday_session_module._restore_longbridge_oauth_cache(
                        "test-client"
                    )
            self.assertFalse(cache_path.exists())

    def test_longbridge_batch_uses_provider_timestamp_and_rejects_delay(self):
        now = datetime(2026, 8, 11, 14, 45, tzinfo=TZ)
        fresh = parse_longbridge_batch(
            [
                {
                    "symbol": "6181.HK",
                    "last_done": "365.4",
                    "prev_close": "395.6",
                    "volume": 123456,
                    "timestamp": int((now - timedelta(seconds=12)).timestamp()),
                }
            ],
            ["HK06181"],
            fetched_at=now,
            freshness_seconds=90,
        )["HK06181"]
        self.assertFalse(fresh.is_stale)
        self.assertEqual(fresh.source, "longbridge_batch")
        self.assertAlmostEqual(fresh.change_pct, -7.63, places=2)
        self.assertEqual(fresh.stale_seconds, 12)

        delayed = parse_longbridge_batch(
            [
                {
                    "symbol": "6181.HK",
                    "last_done": "365.4",
                    "prev_close": "395.6",
                    "timestamp": int((now - timedelta(minutes=15)).timestamp()),
                }
            ],
            ["HK06181"],
            fetched_at=now,
            freshness_seconds=90,
        )["HK06181"]
        self.assertTrue(delayed.is_stale)
        self.assertEqual(delayed.stale_seconds, 900)

    def test_realtime_entitled_snapshot_separates_live_path_from_old_trade(self):
        now = datetime(2026, 8, 11, 14, 45, tzinfo=TZ)
        snapshot = parse_longbridge_batch(
            [
                {
                    "symbol": "2522.HK",
                    "last_done": "4.82",
                    "prev_close": "4.75",
                    "timestamp": int((now - timedelta(minutes=4)).timestamp()),
                }
            ],
            ["HK02522"],
            fetched_at=now,
            freshness_seconds=90,
            realtime_entitled=True,
        )["HK02522"]

        self.assertTrue(snapshot.provider_snapshot_fresh)
        self.assertTrue(snapshot.is_stale)
        self.assertEqual(snapshot.stale_seconds, 240)

    @unittest.skipUnless(hasattr(time, "tzset"), "requires POSIX timezone support")
    def test_longbridge_naive_sdk_timestamp_uses_runner_local_timezone(self):
        original_tz = os.environ.get("TZ")
        try:
            os.environ["TZ"] = "UTC"
            time.tzset()
            parsed = parse_longbridge_timestamp(datetime(2026, 8, 14, 2, 0, 14))
        finally:
            if original_tz is None:
                os.environ.pop("TZ", None)
            else:
                os.environ["TZ"] = original_tz
            time.tzset()

        self.assertEqual(parsed, datetime(2026, 8, 14, 10, 0, 14, tzinfo=TZ))

    def test_market_aware_fetcher_replaces_only_hk_with_fresh_longbridge(self):
        now = datetime(2026, 8, 11, 14, 45, tzinfo=TZ)
        cn = quote("300408", 40, 0.2, now)
        delayed_hk = RealtimeQuote(
            symbol="HK06181",
            name="老铺黄金",
            price=383.8,
            change_pct=-2.98,
            provider_timestamp=(now - timedelta(minutes=15)).isoformat(),
            fetched_at=now.isoformat(),
            stale_seconds=900,
            is_stale=True,
            source="tencent_batch",
        )
        fresh_hk = RealtimeQuote(
            symbol="HK06181",
            price=365.4,
            change_pct=-7.63,
            provider_timestamp=(now - timedelta(seconds=5)).isoformat(),
            fetched_at=now.isoformat(),
            stale_seconds=5,
            provider_snapshot_fresh=True,
            is_stale=False,
            source="longbridge_batch",
        )
        fetcher = DecisionQuoteFetcher(
            tencent_fetcher=FakeFetcher(
                [{"300408": cn, "HK06181": delayed_hk}]
            ),
            longbridge_fetcher=FakeLongbridgeFetcher(
                [{"HK06181": fresh_hk}]
            ),
        )

        result = fetcher.fetch(["300408", "HK06181"], now=now)

        self.assertIs(result["300408"], cn)
        self.assertEqual(result["HK06181"].price, 365.4)
        self.assertEqual(result["HK06181"].name, "老铺黄金")
        self.assertFalse(result["HK06181"].is_stale)
        self.assertEqual(fetcher.last_diagnostics["hk_fresh_upgraded"], 1)
        self.assertEqual(fetcher.last_diagnostics["hk_provider_timestamped"], 1)
        self.assertFalse(fetcher.last_diagnostics["hk_degraded"])
        self.assertEqual(fetcher.last_diagnostics["hk_degradation_reasons"], {})

    def test_market_aware_fetcher_keeps_live_snapshot_without_recent_trade(self):
        now = datetime(2026, 8, 11, 14, 45, tzinfo=TZ)
        delayed_hk = RealtimeQuote(
            symbol="HK02522",
            price=4.70,
            provider_timestamp=(now - timedelta(minutes=15)).isoformat(),
            is_stale=True,
            source="tencent_batch",
        )
        live_snapshot = RealtimeQuote(
            symbol="HK02522",
            price=4.82,
            provider_timestamp=(now - timedelta(minutes=4)).isoformat(),
            fetched_at=now.isoformat(),
            stale_seconds=240,
            provider_snapshot_fresh=True,
            is_stale=True,
            source="longbridge_batch",
        )
        fetcher = DecisionQuoteFetcher(
            tencent_fetcher=FakeFetcher([{"HK02522": delayed_hk}]),
            longbridge_fetcher=FakeLongbridgeFetcher(
                [{"HK02522": live_snapshot}]
            ),
        )

        result = fetcher.fetch(["HK02522"], now=now)

        self.assertEqual(result["HK02522"], live_snapshot)
        self.assertTrue(result["HK02522"].is_stale)
        self.assertEqual(
            fetcher.last_diagnostics["hk_live_snapshot_covered"], 1
        )
        self.assertEqual(fetcher.last_diagnostics["hk_fresh_upgraded"], 0)
        self.assertEqual(
            fetcher.last_diagnostics["hk_price_timestamp_stale"], 1
        )
        self.assertFalse(fetcher.last_diagnostics["hk_degraded"])

    def test_market_aware_fetcher_reports_missing_provider_timestamp(self):
        now = datetime(2026, 8, 11, 14, 45, tzinfo=TZ)
        delayed_hk = RealtimeQuote(
            symbol="HK06181",
            price=383.8,
            provider_timestamp=(now - timedelta(minutes=15)).isoformat(),
            is_stale=True,
            source="tencent_batch",
        )
        missing_timestamp = RealtimeQuote(
            symbol="HK06181",
            price=365.4,
            provider_timestamp=None,
            fetched_at=now.isoformat(),
            is_stale=True,
            source="longbridge_batch",
        )
        fetcher = DecisionQuoteFetcher(
            tencent_fetcher=FakeFetcher([{"HK06181": delayed_hk}]),
            longbridge_fetcher=FakeLongbridgeFetcher(
                [{"HK06181": missing_timestamp}]
            ),
        )

        result = fetcher.fetch(["HK06181"], now=now)

        self.assertIs(result["HK06181"], delayed_hk)
        self.assertEqual(fetcher.last_diagnostics["hk_fresh_upgraded"], 0)
        self.assertEqual(fetcher.last_diagnostics["hk_provider_timestamped"], 0)
        self.assertTrue(fetcher.last_diagnostics["hk_degraded"])
        self.assertEqual(
            fetcher.last_diagnostics["hk_degradation_reasons"],
            {"longbridge_provider_timestamp_missing": 1},
        )

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
            0,
        )
        self.assertEqual(len(sent), 0)
        self.assertEqual(len(state["event_ledger"]), 2)
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

        flush_outbox(state, now=repeat_time, sender=lambda **_payload: True)
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
            any(
                event["transition"] == "deteriorated"
                for event in state["event_ledger"]
            )
        )

    def test_cooldown_never_realerts_but_severity_upgrade_is_recorded(self):
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
        self.assertEqual(created, 0)
        self.assertEqual(state["outbox"], [])
        self.assertFalse(
            any(
                event.get("transition") == "cooldown_repeat"
                for event in state.get("event_ledger", [])
            )
        )

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
        self.assertEqual(state["outbox"], [])
        self.assertEqual(state["event_ledger"][-1]["transition"], "severity_up")
        self.assertEqual(state["event_ledger"][-1]["severity"], "critical")

    def test_decision_gate_pushes_action_not_plain_move_and_dedupes(self):
        from scripts.intraday_monitor import QuoteSnapshot, evaluate_quote

        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        state = load_state_v2(Path("/path/that/does/not/exist"), now=now)
        shadow = synthetic_shadow_state()
        levels = {"688333": ReferenceLevels(stop_loss=110)}
        alerts = evaluate_quote(
            QuoteSnapshot("688333", "铂力特", 109, -1),
            levels["688333"],
            down_threshold_pct=3,
            up_threshold_pct=5,
        )
        start = len(state["event_ledger"])
        update_condition_state(
            state,
            now=now,
            quote=quote("688333", 109, -1, now),
            alerts=alerts,
            levels=levels["688333"],
            cooldown_seconds=1,
            deterioration_pct=0.1,
        )
        raw = state["event_ledger"][start:]
        self.assertEqual(
            process_actionable_decisions(
                state,
                now=now,
                raw_events=raw,
                levels=levels,
                shadow_state=shadow,
                signal_recorder=record_shadow_signal,
            ),
            1,
        )
        self.assertEqual(len(state["outbox"]), 1)
        self.assertEqual(state["outbox"][0]["payload"]["action"], "建议减仓1/3")
        self.assertEqual(len(shadow["signal_ledger"]), 1)
        pushed = []
        flush_outbox(
            state,
            now=now,
            sender=lambda **payload: pushed.append(payload) or True,
            position_quantities={"PRIMARY_PORTFOLIO": {"688333": 100}},
        )
        self.assertIn("建议动作：建议减仓1/3", pushed[0]["content"])
        self.assertIn("建议数量：33 股", pushed[0]["content"])
        self.assertIn("触发价：110.000", pushed[0]["content"])
        self.assertIn("委托参考价：109.000", pushed[0]["content"])
        self.assertIn("失效条件：", pushed[0]["content"])
        self.assertIn("有效期：2026-08-10T15:00:00+08:00", pushed[0]["content"])

        later = now + timedelta(minutes=20)
        start = len(state["event_ledger"])
        update_condition_state(
            state,
            now=later,
            quote=quote("688333", 108.8, -1.1, later),
            alerts=evaluate_quote(
                QuoteSnapshot("688333", "铂力特", 108.8, -1.1),
                levels["688333"],
                down_threshold_pct=3,
                up_threshold_pct=5,
            ),
            levels=levels["688333"],
            cooldown_seconds=1,
            deterioration_pct=0.1,
        )
        process_actionable_decisions(
            state,
            now=later,
            raw_events=state["event_ledger"][start:],
            levels=levels,
            shadow_state=shadow,
            signal_recorder=record_shadow_signal,
        )
        self.assertEqual(state["outbox"], [])

        rise_time = later + timedelta(minutes=1)
        start = len(state["event_ledger"])
        rise_alerts = evaluate_quote(
            QuoteSnapshot("300499", "高澜股份", 30, 5.2),
            ReferenceLevels(),
            down_threshold_pct=3,
            up_threshold_pct=5,
        )
        update_condition_state(
            state,
            now=rise_time,
            quote=quote("300499", 30, 5.2, rise_time),
            alerts=rise_alerts,
            levels=ReferenceLevels(),
            cooldown_seconds=1,
            deterioration_pct=0.1,
        )
        process_actionable_decisions(
            state,
            now=rise_time,
            raw_events=state["event_ledger"][start:],
            levels={},
            shadow_state=shadow,
            signal_recorder=record_shadow_signal,
        )
        self.assertEqual(state["outbox"], [])
        self.assertEqual(
            state["event_ledger"][start]["decision_result"],
            "no_operation_price_move_only",
        )

    def test_high_cash_guardrail_holds_routine_profit_take_but_not_hard_stop(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        shadow = synthetic_shadow_state()
        added_cash = float(shadow["initial_nav"])
        shadow["strategy_shadow_portfolio"]["cash_cny"] = added_cash
        shadow["strategy_shadow_portfolio"]["cash_by_currency"] = {
            "CNY": added_cash,
            "HKD": 0.0,
        }
        target_state = load_state_v2(Path("/path/that/does/not/exist"), now=now)
        target_event = {
            "event_id": "cash-band-target",
            "symbol": "688333",
            "name": "铂力特",
            "condition": "target_reached",
            "transition": "activated",
            "severity": "warning",
            "price": 120,
            "reference_price": 115,
            "payload": {
                "quote_time": now.isoformat(),
                "data_quality": "fresh_l1",
            },
        }

        self.assertEqual(
            process_actionable_decisions(
                target_state,
                now=now,
                raw_events=[target_event],
                levels={"688333": ReferenceLevels(target_price=115)},
                shadow_state=shadow,
                signal_recorder=record_shadow_signal,
            ),
            1,
        )
        self.assertEqual(
            target_state["outbox"][0]["payload"]["action_code"],
            "hold_cash_guardrail",
        )
        self.assertGreaterEqual(
            target_state["outbox"][0]["payload"]["portfolio_cash_ratio"],
            0.25,
        )
        self.assertEqual(shadow["signal_ledger"], [])
        sent: list[Mapping[str, Any]] = []
        self.assertEqual(
            flush_outbox(
                target_state,
                now=now,
                sender=lambda **payload: sent.append(payload) or True,
            ),
            1,
        )
        self.assertIn("建议动作：建议不减仓，继续持有", sent[0]["content"])
        self.assertIn("硬止损不受本护栏影响", sent[0]["content"])

        stop_state = load_state_v2(Path("/path/that/does/not/exist"), now=now)
        stop_event = {
            **target_event,
            "event_id": "cash-band-hard-stop",
            "condition": "stop_loss",
            "severity": "critical",
            "price": 90,
            "reference_price": 100,
        }
        self.assertEqual(
            process_actionable_decisions(
                stop_state,
                now=now,
                raw_events=[stop_event],
                levels={"688333": ReferenceLevels(stop_loss=100)},
                shadow_state=shadow,
                signal_recorder=record_shadow_signal,
            ),
            1,
        )
        self.assertEqual(stop_state["outbox"][0]["payload"]["action"], "建议清仓")
        self.assertEqual(shadow["signal_ledger"][-1]["action"], "clear")

    def test_buy_guardrail_preserves_minimum_cash_and_reserves_pending_buys(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)

        def entry_event(event_id, symbol):
            return {
                "event_id": event_id,
                "symbol": symbol,
                "name": symbol,
                "condition": "adaptive_entry_review",
                "transition": "manual_review",
                "severity": "warning",
                "price": 100,
                "reference_price": 99,
                "payload": {
                    "quote_time": now.isoformat(),
                    "data_quality": "fresh_l1",
                    "entry_low": 99,
                    "entry_high": 101,
                    "stop_loss": 95,
                    "target_price": 120,
                    "confidence": 0.95,
                    "plan_fingerprint": f"plan-{symbol}",
                    "market_costs": {
                        "entry_fee_bps": 3,
                        "entry_slippage_bps": 7,
                    },
                },
            }

        low_cash_shadow = synthetic_shadow_state()
        low_cash = float(low_cash_shadow["initial_nav"]) * 0.18
        low_cash_shadow["strategy_shadow_portfolio"]["cash_cny"] = low_cash
        low_cash_shadow["strategy_shadow_portfolio"]["cash_by_currency"] = {
            "CNY": low_cash,
            "HKD": 0.0,
        }
        low_cash_state = load_state_v2(
            Path("/path/that/does/not/exist"), now=now
        )
        low_cash_event = entry_event("reserve-floor", "600001")
        self.assertEqual(
            process_actionable_decisions(
                low_cash_state,
                now=now,
                raw_events=[low_cash_event],
                levels={},
                shadow_state=low_cash_shadow,
                signal_recorder=record_shadow_signal,
            ),
            0,
        )
        self.assertEqual(
            low_cash_event["decision_result"],
            "no_operation_insufficient_cash_for_explicit_buy",
        )

        shadow = synthetic_shadow_state()
        available_cash = float(shadow["initial_nav"]) * 0.25
        shadow["strategy_shadow_portfolio"]["cash_cny"] = available_cash
        shadow["strategy_shadow_portfolio"]["cash_by_currency"] = {
            "CNY": available_cash,
            "HKD": 0.0,
        }
        state = load_state_v2(Path("/path/that/does/not/exist"), now=now)
        first = entry_event("pending-reserve-1", "600001")
        second = entry_event("pending-reserve-2", "600002")

        self.assertEqual(
            process_actionable_decisions(
                state,
                now=now,
                raw_events=[first, second],
                levels={},
                shadow_state=shadow,
                signal_recorder=record_shadow_signal,
            ),
            1,
        )
        self.assertEqual(state["outbox"][0]["payload"]["action_code"], "buy_0_25")
        self.assertEqual(len(shadow["pending_signal_ids"]), 1)
        self.assertEqual(
            second["decision_result"],
            "no_operation_insufficient_cash_for_explicit_buy",
        )

    def test_exceptional_opportunity_can_cross_normal_cash_floor_one_tranche_at_a_time(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        shadow = synthetic_shadow_state()
        available_cash = float(shadow["initial_nav"]) * 0.20
        shadow["strategy_shadow_portfolio"]["cash_cny"] = available_cash
        shadow["strategy_shadow_portfolio"]["cash_by_currency"] = {
            "CNY": available_cash,
            "HKD": 0.0,
        }
        state = load_state_v2(Path("/path/that/does/not/exist"), now=now)
        event = {
            "event_id": "exceptional-soft-cash-floor",
            "symbol": "600001",
            "name": "极强候选",
            "condition": "adaptive_entry_review",
            "transition": "manual_review",
            "severity": "warning",
            "price": 100,
            "reference_price": 99,
            "payload": {
                "quote_time": now.isoformat(),
                "data_quality": "fresh_l1",
                "entry_low": 99,
                "entry_high": 101,
                "stop_loss": 95,
                "target_price": 120,
                "confidence": 0.95,
                "plan_fingerprint": "exceptional-plan-1",
                "opportunity_tier": "exceptional",
                "market_costs": {
                    "entry_fee_bps": 3,
                    "entry_slippage_bps": 7,
                },
            },
        }

        self.assertEqual(
            process_actionable_decisions(
                state,
                now=now,
                raw_events=[event],
                levels={},
                shadow_state=shadow,
                signal_recorder=record_shadow_signal,
            ),
            1,
        )
        payload = state["outbox"][0]["payload"]
        self.assertEqual(payload["action_code"], "buy_1_0")
        self.assertEqual(payload["tranche_fraction"], 0.10)
        self.assertEqual(payload["opportunity_tier"], "exceptional")
        self.assertEqual(payload["max_single_position_ratio"], 0.50)
        self.assertLess(payload["projected_cash_ratio"], 0.15)
        self.assertEqual(shadow["signal_ledger"][-1]["action"], "buy_1.0_cheng")

        repeat_event = {
            **event,
            "event_id": "exceptional-soft-cash-floor-repeat",
            "payload": dict(event["payload"]),
        }
        self.assertEqual(
            process_actionable_decisions(
                state,
                now=now + timedelta(minutes=1),
                raw_events=[repeat_event],
                levels={},
                shadow_state=shadow,
                signal_recorder=record_shadow_signal,
            ),
            0,
        )
        self.assertEqual(
            repeat_event["decision_result"],
            "no_operation_scan_already_sized",
        )
        self.assertEqual(len(shadow["signal_ledger"]), 1)

    def test_held_exceptional_candidate_adds_only_after_right_side_confirmation(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)

        def held_shadow(basis_price: float):
            shadow = synthetic_shadow_state()
            added_cash = float(shadow["initial_nav"]) * 0.50
            shadow["strategy_shadow_portfolio"]["cash_cny"] = added_cash
            shadow["strategy_shadow_portfolio"]["cash_by_currency"] = {
                "CNY": added_cash,
                "HKD": 0.0,
            }
            shadow["strategy_shadow_portfolio"]["positions"]["600001"] = {
                "symbol": "600001",
                "name": "极强候选",
                "currency": "CNY",
                "quantity": 100,
                "historical_cost": basis_price,
                "experiment_basis_price": basis_price,
            }
            shadow["strategy_shadow_portfolio"]["last_prices"]["600001"] = 100
            return shadow

        def entry_event(event_id: str, price: float):
            return {
                "event_id": event_id,
                "symbol": "600001",
                "name": "极强候选",
                "condition": "adaptive_entry_review",
                "transition": "manual_review",
                "severity": "warning",
                "price": price,
                "reference_price": price - 1,
                "payload": {
                    "quote_time": now.isoformat(),
                    "data_quality": "fresh_l1",
                    "entry_low": price - 1,
                    "entry_high": price + 1,
                    "stop_loss": price - 5,
                    "target_price": price + 20,
                    "confidence": 0.95,
                    "plan_fingerprint": event_id,
                    "opportunity_tier": "exceptional",
                    "market_costs": {
                        "entry_fee_bps": 3,
                        "entry_slippage_bps": 7,
                    },
                },
            }

        confirmed_shadow = held_shadow(99)
        confirmed_state = load_state_v2(
            Path("/path/that/does/not/exist"), now=now
        )
        confirmed_event = entry_event("right-side-confirmed", 100)
        self.assertEqual(
            process_actionable_decisions(
                confirmed_state,
                now=now,
                raw_events=[confirmed_event],
                levels={},
                shadow_state=confirmed_shadow,
                signal_recorder=record_shadow_signal,
            ),
            1,
        )
        confirmed_payload = confirmed_state["outbox"][0]["payload"]
        self.assertEqual(confirmed_payload["action_code"], "add_0_5")
        self.assertEqual(confirmed_payload["tranche_fraction"], 0.05)
        self.assertEqual(
            confirmed_shadow["signal_ledger"][-1]["action"],
            "add_0.5_cheng",
        )

        losing_shadow = held_shadow(101)
        losing_state = load_state_v2(
            Path("/path/that/does/not/exist"), now=now
        )
        losing_event = entry_event("right-side-not-confirmed", 100)
        self.assertEqual(
            process_actionable_decisions(
                losing_state,
                now=now,
                raw_events=[losing_event],
                levels={},
                shadow_state=losing_shadow,
                signal_recorder=record_shadow_signal,
            ),
            0,
        )
        self.assertEqual(
            losing_event["decision_result"],
            "no_operation_add_requires_right_side_confirmation",
        )
        self.assertEqual(losing_shadow["signal_ledger"], [])

    def test_strong_opportunity_falls_back_to_smaller_affordable_tranche(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        shadow = synthetic_shadow_state()
        available_cash = float(shadow["initial_nav"]) * 0.09
        shadow["strategy_shadow_portfolio"]["cash_cny"] = available_cash
        shadow["strategy_shadow_portfolio"]["cash_by_currency"] = {
            "CNY": available_cash,
            "HKD": 0.0,
        }
        state = load_state_v2(Path("/path/that/does/not/exist"), now=now)
        event = {
            "event_id": "strong-fallback-tranche",
            "symbol": "600001",
            "name": "强候选",
            "condition": "adaptive_entry_review",
            "transition": "manual_review",
            "severity": "warning",
            "price": 100,
            "reference_price": 99,
            "payload": {
                "quote_time": now.isoformat(),
                "data_quality": "fresh_l1",
                "entry_low": 99,
                "entry_high": 101,
                "stop_loss": 95,
                "target_price": 120,
                "confidence": 0.90,
                "plan_fingerprint": "strong-plan-1",
                "opportunity_tier": "strong",
                "market_costs": {
                    "entry_fee_bps": 3,
                    "entry_slippage_bps": 7,
                },
            },
        }

        self.assertEqual(
            process_actionable_decisions(
                state,
                now=now,
                raw_events=[event],
                levels={},
                shadow_state=shadow,
                signal_recorder=record_shadow_signal,
            ),
            1,
        )
        payload = state["outbox"][0]["payload"]
        self.assertEqual(payload["action_code"], "buy_0_25")
        self.assertEqual(payload["opportunity_tier"], "strong")
        self.assertEqual(payload["initial_position_fraction"], 0.05)
        self.assertEqual(payload["tranche_fraction"], 0.025)
        self.assertGreaterEqual(payload["projected_cash_ratio"], 0.05)

    def test_exceptional_pending_tranches_never_exceed_half_position(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        shadow = synthetic_shadow_state()
        available_cash = float(shadow["initial_nav"])
        shadow["strategy_shadow_portfolio"]["cash_cny"] = available_cash
        shadow["strategy_shadow_portfolio"]["cash_by_currency"] = {
            "CNY": available_cash,
            "HKD": 0.0,
        }
        for index in range(20):
            record_shadow_signal(
                shadow,
                event_id=f"pending-half-position-{index}",
                symbol="600001",
                signal_time=(now + timedelta(seconds=index)).isoformat(),
                quote_time=now.isoformat(),
                signal_price=100,
                action="模拟买入0.25成",
                reason="测试待执行仓位预留",
            )

        decision = intraday_session_module._candidate_sizing_decision(
            shadow,
            symbol="600001",
            quote_price=100,
            opportunity_tier="exceptional",
            market_costs={"entry_fee_bps": 0, "entry_slippage_bps": 0},
        )

        self.assertIsNone(decision.action)
        self.assertEqual(decision.reason, "single_position_limit")
        self.assertAlmostEqual(decision.current_position_ratio, 0.0)
        self.assertGreater(decision.projected_position_ratio, 0.50)

    def test_signal_time_is_not_before_provider_quote_time(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        quote_time = now + timedelta(seconds=3)
        state = load_state_v2(Path("/path/that/does/not/exist"), now=now)
        state["provider"]["integration_verification"] = {
            "signal_persist_failures": 0,
            "signal_persist_failure_reasons": {},
        }
        shadow = synthetic_shadow_state()
        raw_event = {
            "event_id": "provider-clock-ahead",
            "symbol": "688333",
            "name": "铂力特",
            "condition": "stop_loss",
            "transition": "activated",
            "severity": "high",
            "price": 99,
            "change_pct": -2,
            "reference_price": 100,
            "payload": {
                "quote_time": quote_time.isoformat(),
                "data_quality": "fresh_l1",
            },
        }

        created = process_actionable_decisions(
            state,
            now=now,
            raw_events=[raw_event],
            levels={"688333": ReferenceLevels(stop_loss=100)},
            shadow_state=shadow,
            signal_recorder=record_shadow_signal,
        )

        self.assertEqual(created, 1)
        self.assertEqual(len(shadow["signal_ledger"]), 1)
        self.assertEqual(
            shadow["signal_ledger"][0]["signal_time"],
            quote_time.isoformat(),
        )
        self.assertEqual(raw_event["decision_result"], "decision:reduce_1_3")
        self.assertEqual(
            state["provider"]["integration_verification"]
            ["signal_persist_failures"],
            0,
        )

    def test_signal_persist_failure_is_counted_for_strict_validation(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        state = load_state_v2(Path("/path/that/does/not/exist"), now=now)
        state["provider"]["integration_verification"] = {
            "signal_persist_failures": 0,
            "signal_persist_failure_reasons": {},
        }
        shadow = synthetic_shadow_state()
        raw_event = {
            "event_id": "persist-failure",
            "symbol": "688333",
            "name": "铂力特",
            "condition": "stop_loss",
            "transition": "activated",
            "severity": "high",
            "price": 99,
            "reference_price": 100,
            "payload": {
                "quote_time": now.isoformat(),
                "data_quality": "fresh_l1",
            },
        }

        def failing_recorder(*_args, **_kwargs):
            raise ValueError("synthetic failure")

        created = process_actionable_decisions(
            state,
            now=now,
            raw_events=[raw_event],
            levels={"688333": ReferenceLevels(stop_loss=100)},
            shadow_state=shadow,
            signal_recorder=failing_recorder,
        )

        self.assertEqual(created, 0)
        verification = state["provider"]["integration_verification"]
        self.assertEqual(verification["signal_persist_failures"], 1)
        self.assertEqual(
            verification["signal_persist_failure_reasons"],
            {"ValueError": 1},
        )

    def test_laopu_large_drop_holds_above_stop_and_sells_after_stop_break(self):
        now = datetime(2026, 8, 11, 14, 45, tzinfo=TZ)
        state = load_state_v2(Path("/path/that/does/not/exist"), now=now)
        shadow = synthetic_shadow_state()
        levels = {"HK06181": ReferenceLevels(stop_loss=353)}

        first = run_cycle(
            symbols=["HK06181"],
            primary_symbols=["HK06181"],
            state=state,
            levels=levels,
            fetcher=FakeFetcher(
                [{"HK06181": quote("HK06181", 365.4, -7.63, now)}]
            ),
            now=now,
            shadow_state=shadow,
            notification_sender=lambda **_: False,
        )
        self.assertEqual(first.valid_quote_count, 1)
        self.assertEqual(state["outbox"], [])
        self.assertEqual(
            state["symbols"]["HK06181"]["conditions"]["sharp_drop"]
            ["status"],
            "active",
        )

        later = now + timedelta(minutes=1)
        run_cycle(
            symbols=["HK06181"],
            primary_symbols=["HK06181"],
            state=state,
            levels=levels,
            fetcher=FakeFetcher(
                [{"HK06181": quote("HK06181", 352.8, -10.82, later)}]
            ),
            now=later,
            shadow_state=shadow,
            notification_sender=lambda **_: False,
        )
        self.assertEqual(len(state["outbox"]), 2)
        primary_event = next(
            event
            for event in state["outbox"]
            if not (event.get("payload") or {}).get("account_layer")
        )
        self.assertEqual(
            primary_event["payload"]["conclusion"],
            "建议止损减仓1/3",
        )

    def test_same_cycle_keeps_only_strongest_action_per_symbol(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        state = load_state_v2(Path("/path/that/does/not/exist"), now=now)
        shadow = synthetic_shadow_state()
        quote_time = (now - timedelta(seconds=5)).isoformat()
        stop_event = {
            "event_id": "stop-same-cycle",
            "symbol": "688333",
            "name": "铂力特",
            "condition": "stop_loss",
            "transition": "activated",
            "severity": "high",
            "price": 99,
            "change_pct": -2,
            "reference_price": 100,
            "payload": {
                "quote_time": quote_time,
                "data_quality": "fresh_l1",
            },
        }
        adaptive_event = {
            "event_id": "adaptive-same-cycle",
            "symbol": "688333",
            "name": "铂力特",
            "condition": "adaptive_risk_review",
            "transition": "manual_review",
            "severity": "high",
            "price": 99,
            "change_pct": -2,
            "reference_price": 100,
            "payload": {
                "quote_time": quote_time,
                "data_quality": "fresh_l1",
                "stop_loss": 100,
            },
        }

        self.assertEqual(
            process_actionable_decisions(
                state,
                now=now,
                raw_events=[stop_event, adaptive_event],
                levels={"688333": ReferenceLevels(stop_loss=100)},
                shadow_state=shadow,
                signal_recorder=record_shadow_signal,
            ),
            1,
        )
        self.assertEqual(len(state["outbox"]), 1)
        self.assertEqual(state["outbox"][0]["payload"]["action"], "建议减仓1/2")
        self.assertEqual(len(shadow["signal_ledger"]), 1)
        self.assertEqual(
            stop_event["decision_result"],
            "merged_into_stronger_same_cycle_decision",
        )

    def test_private_share_quantity_is_rendered_but_never_persisted(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        state = load_state_v2(Path("/path/that/does/not/exist"), now=now)
        shadow = synthetic_shadow_state()
        shadow["strategy_shadow_portfolio"]["positions"]["688333"][
            "quantity"
        ] = 4321
        raw_event = {
            "event_id": "private-quantity-stop",
            "symbol": "688333",
            "name": "铂力特",
            "condition": "stop_loss",
            "transition": "activated",
            "severity": "high",
            "price": 99,
            "change_pct": -2,
            "reference_price": 100,
            "payload": {
                "quote_time": now.isoformat(),
                "data_quality": "fresh_l1",
            },
        }

        self.assertEqual(
            process_actionable_decisions(
                state,
                now=now,
                raw_events=[raw_event],
                levels={"688333": ReferenceLevels(stop_loss=100)},
                shadow_state=shadow,
            ),
            1,
        )
        self.assertNotIn("4321", json.dumps(state, ensure_ascii=False))

        sent = []
        self.assertEqual(
            flush_outbox(
                state,
                now=now,
                sender=lambda **payload: sent.append(payload) or True,
                position_quantities={
                    "PRIMARY_PORTFOLIO": {"688333": 4321}
                },
            ),
            1,
        )
        self.assertIn("建议数量：1440 股", sent[0]["content"])
        self.assertNotIn("4321", json.dumps(state, ensure_ascii=False))

    def test_observed_candidate_rechecks_when_same_currency_cash_appears(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        state = load_state_v2(Path("/path/that/does/not/exist"), now=now)
        shadow = synthetic_shadow_state()
        candidate = {
            "scope": "watchlist",
            "symbol": "HK00700",
            "market": "H",
            "entry_low": 99,
            "entry_high": 101,
            "plan_price": 100,
            "stop_loss": 95,
            "target_price": 120,
            "confidence": 0.95,
            "data_quality": "high",
            "expected_holding_days": 20,
            "position_state": "flat",
            "market_costs": {
                "entry_fee_bps": 10,
                "exit_fee_bps": 10,
                "entry_slippage_bps": 5,
                "exit_slippage_bps": 5,
            },
        }
        initial_quote = quote("HK00700", 100, 0.2, now)
        start = len(state["event_ledger"])
        self.assertEqual(
            enqueue_adaptive_plan_reviews(
                state,
                now=now,
                quotes=[initial_quote],
                candidates=[candidate],
            ),
            1,
        )
        self.assertEqual(
            process_actionable_decisions(
                state,
                now=now,
                raw_events=state["event_ledger"][start:],
                levels={},
                shadow_state=shadow,
                signal_recorder=record_shadow_signal,
            ),
            0,
        )
        self.assertEqual(state["outbox"], [])
        self.assertEqual(
            state["decision_notifications"]["HK00700"]
            ["adaptive_entry_review"]["last_action"],
            "observe",
        )
        self.assertEqual(shadow["signal_ledger"], [])

        shadow["strategy_shadow_portfolio"]["cash_cny"] = 1
        shadow["strategy_shadow_portfolio"]["cash_by_currency"] = {
            "CNY": 1,
            "HKD": 0,
        }
        later = now + timedelta(minutes=1)
        later_quote = quote("HK00700", 100, 0.2, later)
        start = len(state["event_ledger"])
        self.assertEqual(
            enqueue_cash_available_candidate_rechecks(
                state,
                now=later,
                quotes=[later_quote],
                candidates=[candidate],
                shadow_state=shadow,
            ),
            0,
        )
        self.assertEqual(len(state["event_ledger"]), start)

        shadow["strategy_shadow_portfolio"]["cash_cny"] = (
            shadow["initial_nav"] * 0.25
        )
        shadow["strategy_shadow_portfolio"]["cash_by_currency"] = {
            "CNY": shadow["strategy_shadow_portfolio"]["cash_cny"],
            "HKD": 0,
        }
        self.assertEqual(
            enqueue_cash_available_candidate_rechecks(
                state,
                now=later,
                quotes=[later_quote],
                candidates=[candidate],
                shadow_state=shadow,
            ),
            1,
        )
        self.assertEqual(
            process_actionable_decisions(
                state,
                now=later,
                raw_events=state["event_ledger"][start:],
                levels={},
                shadow_state=shadow,
                signal_recorder=record_shadow_signal,
            ),
            1,
        )
        self.assertEqual(len(state["outbox"]), 1)
        self.assertEqual(state["outbox"][0]["payload"]["action"], "建议买入0.25成")
        self.assertEqual(len(shadow["signal_ledger"]), 1)

    def test_legacy_raw_outbox_is_audited_but_never_pushed(self):
        now = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        state = load_state_v2(Path("/path/that/does/not/exist"), now=now)
        state["outbox"] = [
            {
                "event_id": "legacy-sharp-drop",
                "created_at": now.isoformat(),
                "symbol": "300408",
                "name": "三环集团",
                "condition": "sharp_drop",
                "transition": "activated",
                "severity": "high",
                "price": 100,
                "change_pct": -3.1,
                "message": "legacy raw alert",
                "attempts": 1,
                "next_attempt_at": now.isoformat(),
            }
        ]
        calls = []

        self.assertEqual(
            flush_outbox(
                state,
                now=now,
                sender=lambda **payload: calls.append(payload) or True,
            ),
            0,
        )
        self.assertEqual(calls, [])
        self.assertEqual(state["outbox"], [])
        self.assertEqual(
            state["cancelled_events"][-1]["cancel_reason"],
            "legacy_raw_event_requires_decision_gate",
        )
        self.assertEqual(
            state["event_ledger"][-1]["decision_result"],
            "suppressed_legacy_raw_push",
        )

    def test_shadow_scorecards_retry_oldest_pending_before_new_day(self):
        shadow = synthetic_shadow_state()
        for trade_date, multiplier in (("2026-08-10", 1.01), ("2026-08-11", 1.02)):
            closes = {
                item["symbol"]: {
                    "price": item["verified_close"] * multiplier,
                    "provider_timestamp": f"{trade_date}T16:00:00+08:00",
                    "is_stale": False,
                }
                for item in INITIAL_INSTRUMENTS
            }
            self.assertTrue(record_shadow_daily_nav(shadow, trade_date, closes))
        shadow["scorecard_notifications"] = {
            "2026-08-10": {"status": "pending", "attempts": 1}
        }
        sent = []

        self.assertTrue(
            intraday_session_module._notify_shadow_scorecard(
                shadow,
                now=datetime(2026, 8, 11, 16, 2, tzinfo=TZ),
                sender=lambda **payload: sent.append(payload) or True,
            )
        )
        self.assertEqual(
            [item["title"] for item in sent],
            [
                "策略 vs 死拿每日成绩单 - 2026-08-10",
                "策略 vs 死拿每日成绩单 - 2026-08-11",
            ],
        )
        self.assertIn("实验第 1 / 20", sent[0]["content"])
        self.assertIn("实验第 2 / 20", sent[1]["content"])

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

    def test_raw_condition_is_never_pushed_and_clear_is_audited(self):
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
        self.assertEqual(state["outbox"], [])

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
        self.assertTrue(
            any(
                event.get("condition") == "stop_loss"
                and event.get("transition") == "cleared"
                for event in state["event_ledger"]
            )
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
        self.assertEqual(state["outbox"], [])
        self.assertIn("不是买入指令", state["event_ledger"][-1]["message"])
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
        self.assertFalse(
            any("卖出" in item["message"] for item in state["event_ledger"])
        )


class PushOutcomeTests(unittest.TestCase):
    @staticmethod
    def _decision_event(now, *, action_code):
        return {
            "event_id": f"event-{action_code}",
            "created_at": now.isoformat(),
            "symbol": "603083",
            "name": "剑桥科技",
            "condition": "watch:SISTER_MANAGED_WATCH:target_reached",
            "transition": "watch_human_review",
            "severity": "warning",
            "price": 159.56,
            "change_pct": 4.73,
            "reference_price": 159.44,
            "message": "测试决策",
            "decision_result": "push_watch_account_decision",
            "attempts": 0,
            "next_attempt_at": now.isoformat(),
            "payload": {
                "kind": "trade_decision",
                "action": "不操作" if action_code == "hold" else "建议卖出持仓1/4",
                "action_code": action_code,
                "account_layer": "SISTER_MANAGED_WATCH",
                "account_prefix": "【妹妹账户】",
            },
        }

    def test_hold_decision_is_audited_but_not_sent(self):
        now = datetime(2026, 8, 12, 14, 0, tzinfo=TZ)
        event = self._decision_event(now, action_code="hold")
        state = intraday_session_module._empty_state(now)
        state["outbox"] = [event]
        state["event_ledger"] = [event]
        calls = []
        with patch.dict(os.environ, {"PUSHPLUS_TOKEN": "configured"}, clear=False):
            intraday_session_module._initialize_pushplus_session(state, now=now)
            notified = flush_outbox(
                state,
                now=now,
                sender=lambda **payload: calls.append(payload) or True,
            )

        self.assertEqual(notified, 0)
        self.assertEqual(calls, [])
        self.assertEqual(state["outbox"], [])
        self.assertEqual(
            state["cancelled_events"][-1]["cancel_reason"],
            "no_effective_trade_action",
        )
        self.assertEqual(
            state["provider"]["pushplus_session"]["status"],
            "no_action_no_send",
        )

    def test_explicit_sell_decision_records_successful_push(self):
        now = datetime(2026, 8, 12, 14, 26, tzinfo=TZ)
        event = self._decision_event(now, action_code="reduce_1_4")
        state = intraday_session_module._empty_state(now)
        state["outbox"] = [event]
        state["event_ledger"] = [event]
        with patch.dict(os.environ, {"PUSHPLUS_TOKEN": "configured"}, clear=False):
            intraday_session_module._initialize_pushplus_session(state, now=now)
            notified = flush_outbox(state, now=now, sender=lambda **_: True)

        self.assertEqual(notified, 1)
        self.assertEqual(state["outbox"], [])
        pushplus = state["provider"]["pushplus_session"]
        self.assertEqual(pushplus["status"], "actionable_sent")
        self.assertEqual(pushplus["actionable_sent"], 1)

    def test_strict_verifier_accepts_no_action_without_sending(self):
        result = verify_state(
            {
                "provider": {
                    "degraded": False,
                    "calendar_degraded": False,
                    "quote_fetcher": {
                        "longbridge_configured": True,
                        "hk_realtime_entitled": True,
                        "hk_requested": 6,
                        "hk_live_snapshot_covered": 6,
                        "hk_fresh_upgraded": 6,
                        "hk_provider_timestamped": 6,
                    },
                    "integration_verification": {
                        "hk_cycles_checked": 1,
                        "hk_cycles_fully_live": 1,
                        "hk_cycles_fully_fresh": 1,
                        "hk_degraded_cycles": 0,
                        "hk_degradation_reasons": {},
                        "primary_degraded_cycles": 0,
                        "calendar_degraded_cycles": 0,
                        "signal_persist_failures": 0,
                        "hk_price_timestamp_stale_observations": 17,
                    },
                    "pushplus_session": {
                        "configured": True,
                        "status": "no_action_no_send",
                        "pending_actionable": 0,
                        "pending_system": 0,
                    },
                }
            }
        )

        self.assertTrue(result["passed"])

    def test_strict_verifier_fails_on_degradation_or_push_retry(self):
        result = verify_state(
            {
                "provider": {
                    "degraded": True,
                    "quote_fetcher": {
                        "longbridge_configured": False,
                        "hk_realtime_entitled": False,
                        "hk_requested": 6,
                        "hk_live_snapshot_covered": 0,
                        "hk_fresh_upgraded": 0,
                        "hk_provider_timestamped": 0,
                    },
                    "integration_verification": {
                        "hk_cycles_checked": 1,
                        "hk_cycles_fully_live": 0,
                        "hk_cycles_fully_fresh": 0,
                        "hk_degraded_cycles": 1,
                        "hk_degradation_reasons": {
                            "longbridge_unconfigured": 6
                        },
                        "primary_degraded_cycles": 1,
                        "calendar_degraded_cycles": 1,
                        "signal_persist_failures": 1,
                        "signal_persist_failure_reasons": {
                            "ExperimentInputError": 1
                        },
                    },
                    "pushplus_session": {
                        "configured": True,
                        "status": "send_failed",
                        "pending_actionable": 1,
                        "pending_system": 0,
                    },
                }
            }
        )

        self.assertFalse(result["passed"])
        self.assertIn(
            "longbridge_realtime_permission_unverified", result["issues"]
        )
        self.assertIn("hk_degradation_observed", result["issues"])
        self.assertIn(
            "market_calendar_degradation_observed", result["issues"]
        )
        self.assertIn("signal_persist_failure_observed", result["issues"])
        self.assertIn("pushplus_send_failed", result["issues"])


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

        recovered = {
            "300408": quote("300408", 40, 0.1, now + timedelta(minutes=3)),
            "HK00981": quote("HK00981", 50, 0.1, now + timedelta(minutes=3)),
        }
        run_cycle(
            symbols=["300408", "HK00981"],
            state=state,
            levels={},
            fetcher=FakeFetcher([recovered]),
            now=now + timedelta(minutes=3),
            notification_sender=lambda **payload: sender_calls.append(payload) or True,
        )
        run_cycle(
            symbols=["300408", "HK00981"],
            state=state,
            levels={},
            fetcher=FakeFetcher([recovered]),
            now=now + timedelta(minutes=3),
            notification_sender=lambda **payload: sender_calls.append(payload) or True,
        )
        self.assertEqual(len(sender_calls), 2)
        self.assertIn("行情数据源已恢复", sender_calls[-1]["content"])

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

    def test_session_merges_all_shadow_symbols_without_any_broker_action(self):
        start = datetime(2026, 8, 10, 10, 0, tzinfo=TZ)
        clock = FakeClock(start)

        def batch(symbols, now):
            return {
                symbol: quote(symbol, 50, 0.1, now)
                for symbol in symbols
            }

        fetcher = FakeFetcher([batch])
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            save_shadow_state(root / "shadow.json", synthetic_shadow_state())
            run_session(
                stocks="300408",
                end_at=start + timedelta(hours=2),
                database_path=root / "missing.db",
                state_path=root / "session.json",
                report_path=root / "session.md",
                shadow_state_path=root / "shadow.json",
                shadow_report_path=root / "shadow.md",
                fetcher=fetcher,
                phase_resolver=lambda _market, _now: "intraday",
                clock=clock.now,
                sleeper=clock.sleep,
                notification_sender=lambda **_: True,
                max_cycles=1,
            )
            shadow = json.loads((root / "shadow.json").read_text(encoding="utf-8"))

        self.assertEqual(
            set(fetcher.calls[0][0]),
            set(all_quote_symbols()),
        )
        self.assertEqual(tuple(fetcher.calls[0][0][:14]), PRIMARY_SYMBOLS)
        self.assertEqual(
            shadow["buy_and_hold_baseline"]["positions"],
            shadow["strategy_shadow_portfolio"]["positions"],
        )
        self.assertIs(shadow["simulation_only"], True)
        self.assertIs(shadow["broker_connected"], False)
        self.assertEqual(shadow["trades"], [])

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
        self.assertEqual(
            fetcher.calls[0][0],
            [symbol for symbol in all_quote_symbols() if symbol.startswith("HK")],
        )

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
        self.assertEqual(
            fetcher.calls[0][0], [*all_quote_symbols(), "HK00700"]
        )
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

    def test_only_trusted_scan_artifact_can_activate_nonstandard_position_tier(self):
        start = datetime(2026, 7, 28, 10, 0, tzinfo=TZ)
        candidate = {
            "code": "600001",
            "scope": "simulation",
            "name": "甲公司",
            "action": "conditional_buy",
            "research_status": "actionable",
            "review_complete": True,
            "eligible_for_intraday_review": True,
            "hard_risk_veto": False,
            "model_disagreement": False,
            "opportunity_tier": "exceptional",
            "confidence": 0.95,
            "data_quality": "high",
            "expected_holding_days": 20,
            "market_costs": {
                "entry_fee_bps": 3,
                "exit_fee_bps": 3,
                "entry_slippage_bps": 7,
                "exit_slippage_bps": 7,
            },
            "plan": {
                "entry_low": 99,
                "entry_high": 101,
                "entry_mid": 100,
                "stop_loss": 95,
                "take_profit_1": 120,
            },
        }
        current_quote = quote("600001", 100, 0.5, start)
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            bare_path = root / "bare.json"
            bare_path.write_text(json.dumps([candidate]), encoding="utf-8")
            bare_plan = load_candidate_plans(bare_path, now=start)[0]
            bare_payload = intraday_session_module._candidate_plan_payload(
                bare_plan, current_quote
            )

            trusted_path = root / "trusted.json"
            trusted_path.write_text(
                json.dumps(
                    {
                        "generated_at": start.isoformat(),
                        "simulation_only": True,
                        "auto_order_enabled": False,
                        "human_confirmation_required": True,
                        "safe_to_push": True,
                        "review_complete": True,
                        "candidates": [candidate],
                    }
                ),
                encoding="utf-8",
            )
            trusted_plan = load_candidate_plans(trusted_path, now=start)[0]
            trusted_payload = intraday_session_module._candidate_plan_payload(
                trusted_plan, current_quote
            )

        self.assertIsNotNone(bare_payload)
        self.assertEqual(bare_payload["opportunity_tier"], "standard")
        self.assertEqual(bare_payload["initial_position_fraction"], 0.025)
        self.assertIsNotNone(trusted_payload)
        self.assertEqual(trusted_payload["opportunity_tier"], "exceptional")
        self.assertEqual(trusted_payload["initial_position_fraction"], 0.10)
        self.assertEqual(trusted_payload["max_single_position_ratio"], 0.50)
        self.assertEqual(trusted_payload["_scan_generated_at"], start.isoformat())

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

    def test_stale_quote_clears_raw_entry_without_any_direct_push(self):
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
        self.assertEqual(state["outbox"], [])
        self.assertEqual(len(state["event_ledger"]), 1)

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
            state["symbols"]["HK00700"]["adaptive_reviews"][
                "adaptive_entry_review"
            ]["status"],
            "cleared",
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
            saved_state = json.loads(
                (root / "state.json").read_text(encoding="utf-8")
            )
        self.assertEqual(result.termination_reason, "calendar_unknown_no_open_market")
        self.assertEqual(clock.sleeps, [])
        self.assertIn("calendar_unknown_no_open_market", report)
        self.assertEqual(
            saved_state["provider"]["integration_verification"]
            ["calendar_degraded_cycles"],
            1,
        )

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
        self.assertNotIn("schedule:", workflow)
        self.assertNotIn("cron:", workflow)
        self.assertIn("workflow_dispatch:", workflow)
        self.assertIn("- morning", workflow)
        self.assertIn("- afternoon", workflow)
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
        self.assertIn('SESSION="${{ github.event.inputs.session', workflow)
        self.assertIn('--late-start-policy "$LATE_START_POLICY"', workflow)
        self.assertIn('--shadow-state "$SHADOW_STATE_PLAIN"', workflow)
        self.assertNotIn("SHADOW_AB_INITIAL_PORTFOLIO_JSON", workflow)
        self.assertIn("WATCH_ACCOUNTS_PRIVATE_JSON", workflow)
        self.assertIn("拒绝重新初始化", workflow)
        self.assertIn("name: 严格验证行情与 PushPlus", workflow)
        self.assertIn("if: ${{ !cancelled() }}", workflow)
        self.assertNotIn("verify_integrations:", workflow)
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
