import copy
import math
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from scripts.shadow_ab_experiment import (
    INITIAL_INSTRUMENTS,
    ExperimentInputError,
    execute_pending,
    initial_symbols,
    initialize_state,
    load_or_initialize,
    load_state,
    record_daily_nav,
    record_signal,
    render_scorecard,
    save_state,
    strategy_cash,
    strategy_quantity,
    update_latest_quotes,
)


TZ = ZoneInfo("Asia/Shanghai")


def private_portfolio():
    # Synthetic quantities/costs: real user portfolio data must stay in a secret.
    return {
        "positions": [
            {
                "symbol": item["symbol"],
                "quantity": (index + 1) * 100,
                "historical_cost": item["verified_close"] * 1.1,
            }
            for index, item in enumerate(INITIAL_INSTRUMENTS)
        ]
    }


def quote(symbol, price, quote_time, *, stale=False):
    return {
        "symbol": symbol,
        "price": price,
        "provider_timestamp": quote_time,
        "is_stale": stale,
        "source": "deterministic_test",
    }


def close_quotes(trade_date, multiplier=1.0, extra=None):
    result = {
        item["symbol"]: quote(
            item["symbol"],
            item["verified_close"] * multiplier,
            f"{trade_date}T16:00:00+08:00",
        )
        for item in INITIAL_INSTRUMENTS
    }
    result.update(extra or {})
    return result


def trading_dates(start, count):
    current = date.fromisoformat(start)
    result = []
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


class ShadowAbExperimentTests(unittest.TestCase):
    def test_initializes_exact_public_mapping_equal_accounts_and_persists(self):
        portfolio = private_portfolio()
        state = initialize_state(
            portfolio, created_at=datetime(2026, 8, 9, 10, tzinfo=TZ)
        )

        expected_symbols = tuple(item["symbol"] for item in INITIAL_INSTRUMENTS)
        expected_nav = sum(
            record["quantity"] * item["verified_close"]
            for record, item in zip(portfolio["positions"], INITIAL_INSTRUMENTS)
        )
        self.assertEqual(initial_symbols(), expected_symbols)
        self.assertEqual(initial_symbols(state), expected_symbols)
        self.assertEqual(len(state["initial_positions"]), 14)
        self.assertAlmostEqual(state["initial_nav"], expected_nav)
        self.assertEqual(
            state["buy_and_hold_baseline"]["positions"],
            state["strategy_shadow_portfolio"]["positions"],
        )
        self.assertEqual(strategy_cash(state, "CNY"), 0)
        self.assertEqual(strategy_cash(state, "HKD"), 0)
        self.assertIs(state["simulation_only"], True)
        self.assertIs(state["places_real_orders"], False)
        self.assertIs(state["broker_connected"], False)

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            save_state(path, state)
            self.assertEqual(load_state(path), state)
            self.assertEqual(
                load_or_initialize(path, None, datetime(2026, 9, 1, tzinfo=TZ)),
                state,
            )
            with self.assertRaisesRegex(ExperimentInputError, "refusing reset"):
                load_or_initialize(
                    Path(directory) / "missing.json",
                    portfolio,
                    datetime(2026, 9, 1, tzinfo=TZ),
                )

    def test_rejects_old_signal_then_executes_only_next_fresh_quote(self):
        state = initialize_state(private_portfolio())
        with self.assertRaisesRegex(ExperimentInputError, "observation-only"):
            record_signal(
                state,
                {
                    "event_id": "historical",
                    "symbol": "688333",
                    "signal_time": "2026-08-07T14:00:05+08:00",
                    "quote_time": "2026-08-07T14:00:00+08:00",
                    "signal_price": 100,
                    "action": "模拟减仓1/3",
                    "reason": "historical only",
                },
            )

        a_quantity = state["buy_and_hold_baseline"]["positions"]["688333"][
            "quantity"
        ]
        signal = record_signal(
            state,
            {
                "event_id": "risk-1",
                "symbol": "688333",
                "signal_time": "2026-08-10T10:00:05+08:00",
                "quote_time": "2026-08-10T10:00:00+08:00",
                "signal_price": 100,
                "action": "模拟减仓1/3",
                "reason": "reliable stop breached",
                "current_key_level": 101,
                "next_trigger": "recover above 101",
                "data_quality": "high",
            },
        )
        immutable_signal = copy.deepcopy(signal)

        same_cycle = execute_pending(
            state,
            {
                "688333": quote(
                    "688333", 100, "2026-08-10T10:00:00+08:00"
                )
            },
            now=datetime(2026, 8, 10, 10, 0, 10, tzinfo=TZ),
        )
        self.assertEqual(same_cycle, [])
        self.assertEqual(len(state["pending_signal_ids"]), 1)

        outcomes = execute_pending(
            state,
            {
                "688333": quote(
                    "688333", 99, "2026-08-10T10:01:00+08:00"
                )
            },
            now=datetime(2026, 8, 10, 10, 1, 5, tzinfo=TZ),
        )
        self.assertEqual(outcomes[0]["status"], "executed")
        self.assertEqual(state["signal_ledger"][0], immutable_signal)
        self.assertEqual(
            state["buy_and_hold_baseline"]["positions"]["688333"]["quantity"],
            a_quantity,
        )
        sold_quantity = math.floor(a_quantity / 3)
        self.assertAlmostEqual(
            strategy_quantity(state, "688333"), a_quantity - sold_quantity
        )
        expected_execution_price = 99 * (1 - 5 / 10_000)
        expected_gross = sold_quantity * expected_execution_price
        expected_fee = expected_gross * 10 / 10_000
        self.assertAlmostEqual(
            strategy_cash(state, "CNY"), expected_gross - expected_fee
        )
        self.assertAlmostEqual(state["trades"][0]["transaction_cost"], expected_fee)
        self.assertGreater(state["trades"][0]["slippage"], 0)
        self.assertTrue(
            record_daily_nav(state, "2026-08-10", close_quotes("2026-08-10"))
        )
        attribution = state["daily_nav"][-1]["operation_attribution"][0]
        self.assertEqual(attribution["symbol"], "688333")
        self.assertEqual(attribution["interpretation"], "减仓后卖飞")

    def test_stale_quote_becomes_execution_missed_and_is_never_backfilled(self):
        state = initialize_state(private_portfolio())
        signal = record_signal(
            state,
            {
                "event_id": "risk-2",
                "symbol": "300499",
                "signal_time": "2026-08-10T14:00:05+08:00",
                "quote_time": "2026-08-10T14:00:00+08:00",
                "signal_price": 30,
                "action": "模拟减仓1/4",
                "reason": "risk escalated",
            },
        )
        original_quantity = strategy_quantity(state, "300499")
        missed = execute_pending(
            state,
            {
                "300499": quote(
                    "300499", 29, "2026-08-10T14:01:00+08:00", stale=True
                )
            },
            now=datetime(2026, 8, 10, 15, 0, tzinfo=TZ),
            session_closed=True,
        )
        self.assertEqual(missed[0]["status"], "execution_missed")
        self.assertEqual(missed[0]["reason"], "stale_quote")
        self.assertNotIn(signal["signal_id"], state["pending_signal_ids"])

        later = execute_pending(
            state,
            {
                "300499": quote(
                    "300499", 28, "2026-08-11T09:31:00+08:00"
                )
            },
            now=datetime(2026, 8, 11, 9, 31, 5, tzinfo=TZ),
        )
        self.assertEqual(later, [])
        self.assertEqual(strategy_quantity(state, "300499"), original_quantity)
        self.assertEqual(len(state["trades"]), 0)
        self.assertTrue(
            any(item["status"] == "execution_missed" for item in state["status_ledger"])
        )

    def test_latest_quotes_daily_nav_and_20_to_60_day_continuation(self):
        state = initialize_state(private_portfolio())
        first_date = "2026-08-10"
        first_quotes = close_quotes(first_date)
        self.assertEqual(
            update_latest_quotes(
                state,
                first_quotes,
                datetime(2026, 8, 10, 16, 0, 5, tzinfo=TZ),
                freshness_seconds=10,
            ),
            14,
        )
        self.assertTrue(record_daily_nav(state, first_date))

        dates = trading_dates("2026-08-11", 59)
        for trade_date in dates[:19]:
            self.assertTrue(record_daily_nav(state, trade_date, close_quotes(trade_date)))
        self.assertEqual(state["metrics"]["completed_trading_days"], 20)
        self.assertEqual(state["metrics"]["phase"], "after_20_day_checkpoint")

        self.assertTrue(record_daily_nav(state, dates[19], close_quotes(dates[19])))
        self.assertEqual(state["metrics"]["completed_trading_days"], 21)
        self.assertEqual(state["metrics"]["phase"], "after_20_day_checkpoint")

        for trade_date in dates[20:]:
            self.assertTrue(record_daily_nav(state, trade_date, close_quotes(trade_date)))
        self.assertEqual(state["metrics"]["completed_trading_days"], 60)
        self.assertEqual(
            state["metrics"]["phase"], "formal_60_day_evaluation_complete"
        )
        latest = state["daily_nav"][-1]
        self.assertAlmostEqual(latest["buy_and_hold_nav"], latest["strategy_nav"])
        self.assertEqual(latest["existing_position_management"], 0)
        self.assertEqual(latest["new_candidate_selection"], 0)
        scorecard = render_scorecard(state)
        self.assertIn("实验第 60 / 60 个交易日", scorecard)
        self.assertIn("仅供模拟和人工复核", scorecard)
        self.assertIn("不会连接券商或自动下单", scorecard)

    def test_incomplete_close_does_not_count_as_hold_or_experiment_day(self):
        state = initialize_state(private_portfolio())
        quotes = close_quotes("2026-08-10")
        quotes.pop("HK02522")

        self.assertFalse(record_daily_nav(state, "2026-08-10", quotes))
        self.assertEqual(state["daily_nav"], [])
        self.assertEqual(state["metrics"]["completed_trading_days"], 0)
        self.assertEqual(state["status_ledger"][-1]["status"], "data_unavailable")
        self.assertNotIn("hold", state["status_ledger"][-1])


if __name__ == "__main__":
    unittest.main()
