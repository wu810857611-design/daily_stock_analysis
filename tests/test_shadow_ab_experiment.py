import copy
import json
import math
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from scripts.shadow_ab_experiment import (
    HKD_CNY_BASELINE_FX,
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
    strategy_cash_cny,
    strategy_nav,
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
        expected_by_currency = {
            "CNY": sum(
                record["quantity"] * item["verified_close"]
                for record, item in zip(portfolio["positions"], INITIAL_INSTRUMENTS)
                if item["currency"] == "CNY"
            ),
            "HKD": sum(
                record["quantity"] * item["verified_close"]
                for record, item in zip(portfolio["positions"], INITIAL_INSTRUMENTS)
                if item["currency"] == "HKD"
            ),
        }
        expected_nav = (
            expected_by_currency["CNY"]
            + expected_by_currency["HKD"] * HKD_CNY_BASELINE_FX
        )
        self.assertEqual(initial_symbols(), expected_symbols)
        self.assertEqual(initial_symbols(state), expected_symbols)
        self.assertEqual(len(state["initial_positions"]), 14)
        self.assertAlmostEqual(state["initial_nav"], expected_nav)
        self.assertEqual(state["initial_nav_by_currency"], expected_by_currency)
        self.assertEqual(
            state["valuation_assumption"]["hkd_cny"], HKD_CNY_BASELINE_FX
        )
        self.assertIs(state["valuation_assumption"]["locked"], True)
        self.assertEqual(
            state["buy_and_hold_baseline"]["positions"],
            state["strategy_shadow_portfolio"]["positions"],
        )
        self.assertEqual(strategy_cash(state, "CNY"), 0)
        self.assertEqual(strategy_cash(state, "HKD"), 0)
        self.assertEqual(strategy_cash_cny(state), 0)
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
            with self.assertRaisesRegex(ExperimentInputError, "refusing reset"):
                load_or_initialize(
                    Path(directory) / "also-missing.json",
                    portfolio,
                    datetime(2026, 8, 9, tzinfo=TZ),
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

    def test_v1_state_migrates_in_place_without_resetting_ledgers(self):
        state = initialize_state(private_portfolio())
        self.assertTrue(
            record_daily_nav(state, "2026-08-10", close_quotes("2026-08-10"))
        )
        state["signal_ledger"] = [{"signal_id": "preserved-signal"}]
        state["execution_ledger"] = [{"execution_id": "preserved-execution"}]
        state["schema_version"] = 1
        state["valuation_assumption"] = {
            "mode": "mixed_local_currency_1_to_1"
        }
        state["initial_nav"] = sum(state["initial_nav_by_currency"].values())
        state.pop("initial_nav_cny", None)
        state.pop("initial_nav_mixed_local_currency_1_to_1", None)
        state["config"].pop("purchasing_power_assumption", None)
        state["config"].pop("share_unit_assumptions", None)
        state["strategy_shadow_portfolio"].pop("cash_cny", None)
        for item in state["daily_nav"]:
            item["buy_and_hold_nav"] = sum(
                item["buy_and_hold_nav_by_currency"].values()
            )
            item["strategy_nav"] = sum(item["strategy_nav_by_currency"].values())
            item["valuation_mode"] = "mixed_local_currency_1_to_1"

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy-state.json"
            path.write_text(json.dumps(state), encoding="utf-8")
            migrated = load_state(path)

        self.assertEqual(migrated["schema_version"], 2)
        self.assertEqual(migrated["signal_ledger"], [{"signal_id": "preserved-signal"}])
        self.assertEqual(
            migrated["execution_ledger"], [{"execution_id": "preserved-execution"}]
        )
        self.assertEqual(len(migrated["daily_nav"]), 1)
        self.assertEqual(migrated["daily_nav"][0]["valuation_mode"], "fixed_baseline_fx_to_cny")
        expected = (
            migrated["initial_nav_by_currency"]["CNY"]
            + migrated["initial_nav_by_currency"]["HKD"] * HKD_CNY_BASELINE_FX
        )
        self.assertAlmostEqual(migrated["initial_nav"], expected)
        self.assertEqual(migrated["metrics"]["completed_trading_days"], 1)

    def test_v1_migration_rejects_conflicting_locked_fx(self):
        state = initialize_state(private_portfolio())
        state["schema_version"] = 1
        state["valuation_assumption"] = {
            "mode": "fixed_baseline_fx_to_cny",
            "baseline_date": "2026-08-07",
            "hkd_cny": 0.9,
            "locked": True,
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "conflicting-state.json"
            path.write_text(json.dumps(state), encoding="utf-8")
            with self.assertRaisesRegex(ExperimentInputError, "conflicts"):
                load_state(path)

    def test_a_share_sale_can_fund_h_share_buy_with_unified_cny_cash(self):
        state = initialize_state(private_portfolio())
        record_signal(
            state,
            event_id="sell-a",
            symbol="302132",
            signal_time="2026-08-10T10:00:05+08:00",
            quote_time="2026-08-10T10:00:00+08:00",
            signal_price=50,
            action="模拟清仓",
            reason="test cross-market funding",
        )
        execute_pending(
            state,
            {"302132": quote("302132", 50, "2026-08-10T10:01:00+08:00")},
            now=datetime(2026, 8, 10, 10, 1, 5, tzinfo=TZ),
        )
        cash_after_sale = strategy_cash_cny(state)
        self.assertGreater(cash_after_sale, 0)

        record_signal(
            state,
            event_id="buy-h",
            symbol="HK00700",
            signal_time="2026-08-10T10:02:05+08:00",
            quote_time="2026-08-10T10:02:00+08:00",
            signal_price=100,
            action="模拟买入0.25成",
            reason="test use A-sale CNY for H-share",
        )
        outcomes = execute_pending(
            state,
            {"HK00700": quote("HK00700", 100, "2026-08-10T10:03:00+08:00")},
            now=datetime(2026, 8, 10, 10, 3, 5, tzinfo=TZ),
        )
        self.assertEqual(outcomes[0]["status"], "executed")
        trade = state["trades"][-1]
        self.assertEqual(trade["fx_to_cny"], HKD_CNY_BASELINE_FX)
        self.assertAlmostEqual(
            strategy_cash_cny(state),
            cash_after_sale
            - trade["gross_amount_cny"]
            - trade["transaction_cost_cny"],
        )
        self.assertGreaterEqual(strategy_cash_cny(state), 0)

    def test_exceptional_initial_signal_sizes_ten_percent_of_strategy_nav(self):
        state = initialize_state(private_portfolio())
        added_cash = float(state["initial_nav"]) * 0.20
        state["strategy_shadow_portfolio"]["cash_cny"] = added_cash
        state["strategy_shadow_portfolio"]["cash_by_currency"] = {
            "CNY": added_cash,
            "HKD": 0.0,
        }
        nav = strategy_nav(state)

        signal = record_signal(
            state,
            event_id="exceptional-ten-percent-entry",
            symbol="600001",
            signal_time="2026-08-10T10:00:05+08:00",
            quote_time="2026-08-10T10:00:00+08:00",
            signal_price=100,
            action="buy_1_0",
            reason="extreme opportunity tier initial tranche",
        )

        self.assertEqual(signal["action"], "buy_1.0_cheng")
        self.assertAlmostEqual(signal["requested_notional"], nav * 0.10)
        self.assertEqual(signal["position_delta"]["fraction"], 0.10)
        outcomes = execute_pending(
            state,
            {"600001": quote("600001", 100, "2026-08-10T10:01:00+08:00")},
            now=datetime(2026, 8, 10, 10, 1, 5, tzinfo=TZ),
        )
        self.assertEqual(outcomes[0]["status"], "executed")
        self.assertGreater(strategy_quantity(state, "600001"), 0)
        self.assertGreaterEqual(strategy_cash_cny(state), 0)

    def test_h_share_sale_can_fund_a_share_buy_in_100_share_lots(self):
        state = initialize_state(private_portfolio())
        record_signal(
            state,
            event_id="sell-h",
            symbol="HK06181",
            signal_time="2026-08-10T10:00:05+08:00",
            quote_time="2026-08-10T10:00:00+08:00",
            signal_price=300,
            action="模拟清仓",
            reason="test cross-market funding",
        )
        execute_pending(
            state,
            {"HK06181": quote("HK06181", 300, "2026-08-10T10:01:00+08:00")},
            now=datetime(2026, 8, 10, 10, 1, 5, tzinfo=TZ),
        )
        cash_after_sale = strategy_cash_cny(state)
        expected_hkd_net = (
            state["trades"][0]["gross_amount"]
            - state["trades"][0]["transaction_cost"]
        )
        self.assertAlmostEqual(cash_after_sale, expected_hkd_net * HKD_CNY_BASELINE_FX)

        record_signal(
            state,
            event_id="buy-a",
            symbol="600000",
            signal_time="2026-08-10T10:02:05+08:00",
            quote_time="2026-08-10T10:02:00+08:00",
            signal_price=10,
            action="模拟买入0.25成",
            reason="test use H-sale CNY for A-share",
        )
        outcomes = execute_pending(
            state,
            {"600000": quote("600000", 10, "2026-08-10T10:03:00+08:00")},
            now=datetime(2026, 8, 10, 10, 3, 5, tzinfo=TZ),
        )
        self.assertEqual(outcomes[0]["status"], "executed")
        self.assertEqual(int(state["trades"][-1]["quantity"]) % 100, 0)
        self.assertGreaterEqual(strategy_cash_cny(state), 0)

    def test_a_share_buy_below_100_shares_is_missed_and_clear_sells_tail(self):
        state = initialize_state(private_portfolio())
        strategy = state["strategy_shadow_portfolio"]
        strategy["cash_cny"] = 100_000
        strategy["cash_by_currency"] = {"CNY": 100_000, "HKD": 0.0}
        record_signal(
            state,
            event_id="too-small-a-buy",
            symbol="600001",
            signal_time="2026-08-10T10:00:05+08:00",
            quote_time="2026-08-10T10:00:00+08:00",
            signal_price=1000,
            action="模拟买入0.25成",
            reason="below one A-share lot",
        )
        missed = execute_pending(
            state,
            {"600001": quote("600001", 1000, "2026-08-10T10:01:00+08:00")},
            now=datetime(2026, 8, 10, 10, 1, 5, tzinfo=TZ),
        )
        self.assertEqual(missed[0]["status"], "execution_missed")
        self.assertEqual(missed[0]["reason"], "notional_below_a_share_buy_lot")
        self.assertEqual(strategy_cash_cny(state), 100_000)

        strategy["positions"]["688333"]["quantity"] = 155
        record_signal(
            state,
            event_id="clear-tail",
            symbol="688333",
            signal_time="2026-08-10T10:02:05+08:00",
            quote_time="2026-08-10T10:02:00+08:00",
            signal_price=100,
            action="模拟清仓",
            reason="sell the entire odd-lot tail",
        )
        cleared = execute_pending(
            state,
            {"688333": quote("688333", 100, "2026-08-10T10:03:00+08:00")},
            now=datetime(2026, 8, 10, 10, 3, 5, tzinfo=TZ),
        )
        self.assertEqual(cleared[0]["status"], "executed")
        self.assertEqual(cleared[0]["quantity"], 155)
        self.assertEqual(strategy_quantity(state, "688333"), 0)
        self.assertGreaterEqual(strategy_cash_cny(state), 0)

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

    def test_pending_signal_expires_before_next_trading_day_quote(self):
        state = initialize_state(private_portfolio())
        signal = record_signal(
            state,
            {
                "event_id": "friday-risk",
                "symbol": "688333",
                "signal_time": "2026-08-14T14:59:05+08:00",
                "quote_time": "2026-08-14T14:59:00+08:00",
                "signal_price": 100,
                "action": "模拟减仓1/3",
                "reason": "same-day execution required",
            },
        )
        original_quantity = strategy_quantity(state, "688333")

        outcomes = execute_pending(
            state,
            {
                "688333": quote(
                    "688333", 90, "2026-08-17T09:31:00+08:00"
                )
            },
            now=datetime(2026, 8, 17, 9, 31, 5, tzinfo=TZ),
        )

        self.assertEqual(outcomes[0]["status"], "execution_missed")
        self.assertEqual(outcomes[0]["reason"], "no_same_day_post_signal_quote")
        self.assertNotIn(signal["signal_id"], state["pending_signal_ids"])
        self.assertEqual(state["trades"], [])
        self.assertEqual(strategy_quantity(state, "688333"), original_quantity)
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
        self.assertAlmostEqual(
            latest["buy_and_hold_nav"],
            latest["buy_and_hold_nav_by_currency"]["CNY"]
            + latest["buy_and_hold_nav_by_currency"]["HKD"]
            * HKD_CNY_BASELINE_FX,
        )
        self.assertEqual(latest["valuation_mode"], "fixed_baseline_fx_to_cny")
        self.assertEqual(latest["existing_position_management"], 0)
        self.assertEqual(latest["new_candidate_selection"], 0)
        scorecard = render_scorecard(state)
        self.assertIn("实验第 60 / 60 个交易日", scorecard)
        self.assertIn("仅供模拟和人工复核", scorecard)
        self.assertIn("不会连接券商或自动下单", scorecard)
        self.assertIn("万元人民币", scorecard)
        self.assertIn("HK board lot not modeled", scorecard)

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
