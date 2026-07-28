import json
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

from scripts.adaptive_signal_policy import (
    CONSIDER_ENTRY,
    HOLD,
    RISK_EXIT_REVIEW,
    AdaptivePolicyConfig,
    AdaptiveSignalInput,
    MarketCosts,
    PolicyStateError,
    evaluate_and_persist,
    evaluate_adaptive_signal,
    incumbent_annualized_utility,
    load_policy_state,
    main,
    new_policy_state,
    save_policy_state,
)


def costs(
    *,
    entry_fee_bps=10,
    exit_fee_bps=10,
    entry_slippage_bps=5,
    exit_slippage_bps=5,
):
    return MarketCosts(
        entry_fee_bps=entry_fee_bps,
        exit_fee_bps=exit_fee_bps,
        entry_slippage_bps=entry_slippage_bps,
        exit_slippage_bps=exit_slippage_bps,
    )


def plan(**overrides):
    values = {
        "symbol": "300408",
        "market": "cn",
        "plan_price": 100.0,
        "stop_loss": 90.0,
        "target_price": 120.0,
        "confidence": 0.7,
        "data_quality": "good",
        "market_costs": costs(),
        "expected_holding_days": 5.0,
        "quote_price": 100.0,
        "data_age_seconds": 30.0,
        "position_state": "flat",
        "incumbent_annualized_utility": 0.0,
    }
    values.update(overrides)
    return AdaptiveSignalInput(**values)


class AdaptiveSignalPolicyTests(unittest.TestCase):
    def test_computes_after_cost_risk_metrics_and_surfaces_candidate(self):
        decision = evaluate_adaptive_signal(plan())

        self.assertEqual(decision.candidate_action, CONSIDER_ENTRY)
        self.assertTrue(decision.eligible_for_manual_review)
        self.assertFalse(decision.risk_priority)
        self.assertAlmostEqual(decision.round_trip_cost_bps, 30.0)
        self.assertAlmostEqual(decision.round_trip_cost_rate, 0.003)
        self.assertAlmostEqual(decision.gross_reward, 0.2)
        self.assertAlmostEqual(decision.gross_risk, 0.1)
        self.assertAlmostEqual(decision.net_reward, 0.197)
        self.assertAlmostEqual(decision.net_risk, 0.103)
        self.assertAlmostEqual(decision.net_risk_reward, 0.197 / 0.103)
        self.assertAlmostEqual(
            decision.confidence_adjusted_utility,
            0.9 * (0.7 * 0.197 - 0.3 * 0.103),
        )
        self.assertAlmostEqual(
            decision.annualized_after_cost_utility,
            decision.confidence_adjusted_utility * 252 / 5,
        )
        self.assertEqual(decision.expected_holding_days, 5.0)
        self.assertTrue(decision.simulation_only)
        self.assertFalse(decision.places_real_orders)
        self.assertTrue(decision.requires_human_confirmation)

    def test_cost_drag_can_turn_gross_opportunity_into_hold(self):
        decision = evaluate_adaptive_signal(
            plan(
                stop_loss=99.0,
                target_price=100.3,
                market_costs=costs(
                    entry_fee_bps=10,
                    exit_fee_bps=10,
                    entry_slippage_bps=10,
                    exit_slippage_bps=10,
                ),
            )
        )

        self.assertGreater(decision.gross_reward, 0)
        self.assertLess(decision.net_reward, 0)
        self.assertEqual(decision.candidate_action, HOLD)
        self.assertIn("net_reward_below_threshold", decision.reason_codes)

    def test_missing_plan_fields_degrade_safely_to_hold(self):
        cases = {
            "plan_price": {"plan_price": None},
            "stop_loss": {"stop_loss": None},
            "target_price": {"target_price": None},
            "confidence": {"confidence": None},
            "data_quality": {"data_quality": None},
            "market_costs": {"market_costs": None},
            "expected_holding_days": {"expected_holding_days": None},
        }
        for name, overrides in cases.items():
            with self.subTest(name=name):
                decision = evaluate_adaptive_signal(plan(**overrides))
                self.assertEqual(decision.candidate_action, HOLD)
                self.assertFalse(decision.eligible_for_manual_review)

    def test_invalid_price_direction_degrades_to_hold(self):
        for overrides in (
            {"stop_loss": 100.0},
            {"stop_loss": 101.0},
            {"target_price": 100.0},
            {"target_price": 99.0},
        ):
            with self.subTest(overrides=overrides):
                decision = evaluate_adaptive_signal(plan(**overrides))
                self.assertEqual(decision.candidate_action, HOLD)
                self.assertEqual(decision.reason_codes, ("invalid_price_direction",))

    def test_stale_data_holds_even_if_the_quote_appears_below_stop(self):
        decision = evaluate_adaptive_signal(
            plan(
                quote_price=89.0,
                data_age_seconds=901.0,
            ),
            AdaptivePolicyConfig(max_data_age_seconds=900.0),
        )

        self.assertEqual(decision.candidate_action, HOLD)
        self.assertEqual(decision.reason_codes, ("stale_data",))
        self.assertFalse(decision.risk_priority)

    def test_default_freshness_limit_is_ninety_seconds(self):
        decision = evaluate_adaptive_signal(plan(data_age_seconds=91.0))

        self.assertEqual(decision.candidate_action, HOLD)
        self.assertEqual(decision.reason_codes, ("stale_data",))

    def test_fresh_hard_stop_has_priority_over_incomplete_entry_plan(self):
        decision = evaluate_adaptive_signal(
            plan(
                quote_price=89.0,
                stop_loss=90.0,
                target_price=None,
                confidence=None,
                data_quality=None,
                market_costs=None,
                position_state="held",
            )
        )

        self.assertEqual(decision.candidate_action, RISK_EXIT_REVIEW)
        self.assertTrue(decision.eligible_for_manual_review)
        self.assertTrue(decision.risk_priority)
        self.assertEqual(decision.reason_codes, ("hard_stop_breached",))
        self.assertTrue(decision.simulation_only)
        self.assertFalse(decision.places_real_orders)

    def test_fresh_hard_stop_does_not_require_the_original_plan_price(self):
        decision = evaluate_adaptive_signal(
            plan(
                plan_price=None,
                quote_price=89.0,
                stop_loss=90.0,
                target_price=None,
                confidence=None,
                data_quality=None,
                market_costs=None,
                position_state="held",
            )
        )

        self.assertEqual(decision.candidate_action, RISK_EXIT_REVIEW)
        self.assertTrue(decision.risk_priority)
        self.assertEqual(decision.reason_codes, ("hard_stop_breached",))

    def test_hard_stop_validates_identity_before_risk_candidate(self):
        for overrides, reason in (
            ({"symbol": ""}, "missing_symbol"),
            ({"market": "us"}, "unsupported_market"),
            ({"symbol": "30040"}, "invalid_symbol_format"),
            ({"symbol": "00981", "market": "hk"}, "invalid_symbol_format"),
        ):
            with self.subTest(overrides=overrides):
                decision = evaluate_adaptive_signal(
                    plan(
                        quote_price=89.0,
                        stop_loss=90.0,
                        position_state="held",
                        **overrides,
                    )
                )
                self.assertEqual(decision.candidate_action, HOLD)
                self.assertFalse(decision.risk_priority)
                self.assertIn(reason, decision.reason_codes)

    def test_stop_breach_without_a_held_position_invalidates_plan_only(self):
        for position_state, reason in (
            ("flat", "plan_invalidated_flat_position"),
            ("unknown", "plan_invalidated_position_unknown"),
        ):
            with self.subTest(position_state=position_state):
                decision = evaluate_adaptive_signal(
                    plan(
                        quote_price=89.0,
                        stop_loss=90.0,
                        position_state=position_state,
                    )
                )
                self.assertEqual(decision.candidate_action, HOLD)
                self.assertFalse(decision.risk_priority)
                self.assertEqual(decision.reason_codes, (reason,))

    def test_only_flat_positions_receive_new_entry_candidates(self):
        for position_state, reason in (
            ("held", "position_already_held"),
            ("unknown", "position_state_unknown"),
            ("invalid", "unsupported_position_state"),
        ):
            with self.subTest(position_state=position_state):
                decision = evaluate_adaptive_signal(
                    plan(position_state=position_state)
                )
                self.assertEqual(decision.candidate_action, HOLD)
                self.assertIn(reason, decision.reason_codes)

    def test_hysteresis_suppresses_a_marginal_switch(self):
        baseline = evaluate_adaptive_signal(plan())
        incumbent = baseline.annualized_after_cost_utility - 0.002
        decision = evaluate_adaptive_signal(
            plan(incumbent_annualized_utility=incumbent),
            AdaptivePolicyConfig(hysteresis_utility_delta=0.005),
        )

        self.assertGreater(decision.net_risk_reward, 1.5)
        self.assertGreater(decision.net_reward, 0)
        self.assertEqual(decision.candidate_action, HOLD)
        self.assertIn("hysteresis_not_cleared", decision.reason_codes)
        self.assertAlmostEqual(decision.utility_improvement, 0.002)

    def test_material_utility_improvement_clears_hysteresis(self):
        decision = evaluate_adaptive_signal(
            plan(incumbent_annualized_utility=0.01),
            AdaptivePolicyConfig(hysteresis_utility_delta=0.005),
        )

        self.assertEqual(decision.candidate_action, CONSIDER_ENTRY)
        self.assertGreaterEqual(decision.utility_improvement, 0.005)

    def test_expected_holding_days_make_utility_comparable_across_horizons(self):
        five_day = evaluate_adaptive_signal(plan(expected_holding_days=5))
        ten_day = evaluate_adaptive_signal(plan(expected_holding_days=10))

        self.assertAlmostEqual(
            five_day.confidence_adjusted_utility,
            ten_day.confidence_adjusted_utility,
        )
        self.assertAlmostEqual(
            five_day.annualized_after_cost_utility,
            ten_day.annualized_after_cost_utility * 2,
        )
        self.assertAlmostEqual(
            five_day.annualized_net_reward,
            ten_day.annualized_net_reward * 2,
        )

    def test_expected_holding_days_enforce_safe_default_boundaries(self):
        for holding_days in (1.0, 252.0):
            with self.subTest(valid=holding_days):
                decision = evaluate_adaptive_signal(
                    plan(expected_holding_days=holding_days)
                )
                self.assertEqual(decision.candidate_action, CONSIDER_ENTRY)

        for holding_days in (0.001, 0.999, 252.001, 1_000.0):
            with self.subTest(invalid=holding_days):
                decision = evaluate_adaptive_signal(
                    plan(expected_holding_days=holding_days)
                )
                self.assertEqual(decision.candidate_action, HOLD)
                self.assertIn(
                    "expected_holding_days_out_of_range",
                    decision.reason_codes,
                )

    def test_holding_day_config_must_stay_within_one_trading_year(self):
        for config in (
            AdaptivePolicyConfig(min_expected_holding_days=0.5),
            AdaptivePolicyConfig(max_expected_holding_days=253),
            AdaptivePolicyConfig(
                min_expected_holding_days=10,
                max_expected_holding_days=5,
            ),
        ):
            with self.subTest(config=config):
                decision = evaluate_adaptive_signal(plan(), config)
                self.assertEqual(decision.candidate_action, HOLD)
                self.assertEqual(decision.reason_codes, ("invalid_policy_config",))

    def test_low_or_unknown_data_quality_is_not_actionable(self):
        for quality in ("poor", "unknown", 0.5):
            with self.subTest(quality=quality):
                decision = evaluate_adaptive_signal(plan(data_quality=quality))
                self.assertEqual(decision.candidate_action, HOLD)
                self.assertIn("data_quality_below_threshold", decision.reason_codes)

    def test_invalid_market_costs_degrade_to_hold(self):
        decision = evaluate_adaptive_signal(
            plan(market_costs=costs(entry_fee_bps=-1))
        )

        self.assertEqual(decision.candidate_action, HOLD)
        self.assertIn("missing_or_invalid_market_costs", decision.reason_codes)

    def test_normal_entry_requires_a_symbol_and_supported_market(self):
        cases = (
            ({"symbol": ""}, "missing_symbol"),
            ({"market": "us"}, "unsupported_market"),
            ({"market": ""}, "unsupported_market"),
            ({"symbol": "30040"}, "invalid_symbol_format"),
            ({"symbol": "ABC123"}, "invalid_symbol_format"),
            ({"symbol": "00981", "market": "hk"}, "invalid_symbol_format"),
            ({"symbol": "HK981", "market": "hk"}, "invalid_symbol_format"),
        )
        for overrides, reason in cases:
            with self.subTest(overrides=overrides):
                decision = evaluate_adaptive_signal(plan(**overrides))
                self.assertEqual(decision.candidate_action, HOLD)
                self.assertIn(reason, decision.reason_codes)

    def test_cli_emits_a_non_executable_simulation_decision(self):
        payload = {
            "symbol": "HK00981",
            "market": "hk",
            "plan_price": 50,
            "stop_loss": 45,
            "target_price": 60,
            "confidence": 0.75,
            "data_quality": "good",
            "data_age_seconds": 10,
            "quote_price": 50,
            "position_state": "flat",
            "expected_holding_days": 5,
            "market_costs": {
                "entry_fee_bps": 15,
                "exit_fee_bps": 15,
                "entry_slippage_bps": 5,
                "exit_slippage_bps": 5,
            },
        }
        with tempfile.TemporaryDirectory() as temporary_directory:
            input_path = Path(temporary_directory) / "plan.json"
            input_path.write_text(json.dumps(payload), encoding="utf-8")
            output = StringIO()
            with redirect_stdout(output):
                self.assertEqual(main([str(input_path)]), 0)
            decision = json.loads(output.getvalue())
            self.assertTrue(decision["simulation_only"])
            self.assertFalse(decision["places_real_orders"])
            self.assertTrue(decision["requires_human_confirmation"])

    def test_atomic_state_helper_supplies_incumbent_to_minute_monitor(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "adaptive_state.json"
            first = evaluate_and_persist(
                plan(),
                state_path=state_path,
                evaluated_at="2026-07-28T02:30:00Z",
            )
            self.assertEqual(first.candidate_action, CONSIDER_ENTRY)

            state = load_policy_state(state_path)
            self.assertTrue(state["simulation_only"])
            self.assertFalse(state["places_real_orders"])
            self.assertAlmostEqual(
                incumbent_annualized_utility(
                    state,
                    symbol="300408",
                    market="cn",
                ),
                first.annualized_after_cost_utility,
            )

            repeated = evaluate_and_persist(
                plan(),
                state_path=state_path,
                evaluated_at="2026-07-28T02:31:00Z",
            )
            self.assertEqual(repeated.candidate_action, HOLD)
            self.assertIn("hysteresis_not_cleared", repeated.reason_codes)
            json.loads(state_path.read_text(encoding="utf-8"))

    def test_policy_state_rejects_any_order_capable_payload(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "unsafe.json"
            state = new_policy_state()
            state["places_real_orders"] = True
            with self.assertRaises(PolicyStateError):
                save_policy_state(state_path, state)

    def test_persistent_helper_does_not_write_an_invalid_symbol(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            state_path = Path(temporary_directory) / "adaptive_state.json"
            decision = evaluate_and_persist(
                plan(
                    symbol="30040",
                    quote_price=89.0,
                    position_state="held",
                ),
                state_path=state_path,
                evaluated_at="2026-07-28T02:30:00Z",
            )

            self.assertEqual(decision.candidate_action, HOLD)
            self.assertIn("invalid_symbol_format", decision.reason_codes)
            self.assertFalse(state_path.exists())


if __name__ == "__main__":
    unittest.main()
