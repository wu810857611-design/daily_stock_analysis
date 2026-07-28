import json
import tempfile
import unittest
from pathlib import Path

from scripts.paper_trade_tracker import (
    TrackerInputError,
    load_state,
    main,
    new_state,
    normalise_snapshot,
    render_summary,
    update_state,
)


def snapshot(trade_date, *signals):
    return normalise_snapshot({"trade_date": trade_date, "signals": list(signals)})


def signal(symbol, action, close, **extra):
    return {"symbol": symbol, "signal": action, "close": close, **extra}


class PaperTradeTrackerTests(unittest.TestCase):
    def test_same_trade_date_is_idempotent(self):
        state = new_state(initial_cash=1_000, transaction_cost_bps=0)
        day = snapshot("2026-07-27", signal("AAA", "buy", 100, target_weight=0.5))

        once = update_state(state, day)
        twice = update_state(once, day)

        self.assertEqual(twice, once)
        self.assertEqual(len(twice["portfolio_days"]), 1)
        self.assertEqual(len(twice["symbol_days"]), 1)
        self.assertEqual(twice["positions"], {})
        self.assertAlmostEqual(twice["cash"], 1_000)
        self.assertEqual(twice["trades"], [])
        self.assertEqual(twice["pending_signals"]["signals"][0]["signal"], "buy")
        self.assertEqual(twice["metrics"]["snapshot_days"], 1)
        self.assertEqual(twice["metrics"]["completed_trading_days"], 0)

    def test_signal_executes_only_on_the_next_snapshot_price(self):
        state = new_state(initial_cash=1_000, transaction_cost_bps=0)
        state = update_state(
            state,
            snapshot("2026-07-27", signal("AAA", "buy", 100, target_weight=0.5)),
        )
        self.assertEqual(state["trades"], [])
        self.assertEqual(state["positions"], {})

        state = update_state(state, snapshot("2026-07-28", signal("AAA", "hold", 110)))

        self.assertEqual(len(state["trades"]), 1)
        self.assertEqual(state["trades"][0]["signal_date"], "2026-07-27")
        self.assertEqual(state["trades"][0]["trade_date"], "2026-07-28")
        self.assertAlmostEqual(state["trades"][0]["price"], 110)
        self.assertAlmostEqual(state["positions"]["AAA"]["quantity"], 500 / 110)

    def test_metrics_track_cumulative_return_win_rate_and_drawdown(self):
        state = new_state(initial_cash=1_000, transaction_cost_bps=0)
        state = update_state(
            state,
            snapshot("2026-07-27", signal("AAA", "buy", 100, target_weight=0.5)),
        )
        state = update_state(state, snapshot("2026-07-28", signal("AAA", "hold", 100)))
        state = update_state(state, snapshot("2026-07-29", signal("AAA", "hold", 110)))
        state = update_state(state, snapshot("2026-07-30", signal("AAA", "hold", 90)))

        metrics = state["metrics"]
        self.assertEqual(metrics["completed_trading_days"], 3)
        self.assertEqual(metrics["snapshot_days"], 4)
        self.assertEqual(metrics["baseline_days"], 1)
        self.assertAlmostEqual(metrics["cumulative_return"], -0.05)
        self.assertEqual(metrics["evaluated_return_days"], 3)
        self.assertEqual(metrics["positive_return_days"], 1)
        self.assertAlmostEqual(metrics["win_rate"], 1 / 3)
        self.assertAlmostEqual(metrics["max_drawdown"], 1 - 950 / 1050)
        self.assertIsNone(state["portfolio_days"][0]["daily_return"])
        self.assertAlmostEqual(state["portfolio_days"][1]["daily_return"], 0)
        self.assertAlmostEqual(state["portfolio_days"][2]["daily_return"], 0.05)

    def test_correcting_an_earlier_date_rebuilds_later_results(self):
        state = new_state(initial_cash=1_000, transaction_cost_bps=0)
        state = update_state(
            state,
            snapshot("2026-07-27", signal("AAA", "buy", 100, target_weight=1)),
        )
        state = update_state(state, snapshot("2026-07-28", signal("AAA", "hold", 110)))
        state = update_state(state, snapshot("2026-07-29", signal("AAA", "hold", 120)))
        self.assertGreater(state["metrics"]["ending_value"], 1_000)

        corrected_first_day = snapshot("2026-07-27", signal("AAA", "hold", 100))
        state = update_state(state, corrected_first_day)

        self.assertEqual(state["metrics"]["completed_trading_days"], 2)
        self.assertAlmostEqual(state["metrics"]["ending_value"], 1_000)
        self.assertEqual(state["positions"], {})
        self.assertEqual(state["trades"], [])

    def test_buy_without_explicit_weight_does_not_rebalance_an_existing_position(self):
        state = new_state(initial_cash=1_000, default_target_weight=0.5, transaction_cost_bps=0)
        state = update_state(state, snapshot("2026-07-27", signal("AAA", "buy", 100)))
        state = update_state(state, snapshot("2026-07-28", signal("AAA", "buy", 100)))
        state = update_state(state, snapshot("2026-07-29", signal("AAA", "hold", 90)))

        self.assertAlmostEqual(state["positions"]["AAA"]["quantity"], 5)
        self.assertEqual(len(state["trades"]), 1)

    def test_sell_closes_only_the_paper_position(self):
        state = new_state(initial_cash=1_000, transaction_cost_bps=10)
        state = update_state(
            state,
            snapshot("2026-07-27", signal("AAA", "buy", 100, target_weight=0.5)),
        )
        state = update_state(state, snapshot("2026-07-28", signal("AAA", "sell", 110)))
        state = update_state(state, snapshot("2026-07-29", signal("AAA", "hold", 120)))

        self.assertEqual(state["positions"], {})
        self.assertEqual(
            [trade["side"] for trade in state["trades"]],
            ["paper_buy", "paper_sell"],
        )
        self.assertGreater(state["trades"][-1]["realized_pnl"], 0)
        self.assertIs(state["simulation_only"], True)

    def test_field_and_hong_kong_symbol_aliases_are_canonicalised(self):
        parsed = normalise_snapshot(
            {
                "date": "2026-07-27",
                "results": [
                    {
                        "stock_code": "hk.00981",
                        "stock_name": "中芯国际",
                        "action": "观望",
                        "current_price": 58.2,
                    }
                ],
            }
        )
        self.assertEqual(parsed["signals"][0]["symbol"], "HK00981")
        self.assertEqual(parsed["signals"][0]["signal"], "hold")

        for raw_symbol in ("HK.00981", "00981.HK", "HK00981"):
            with self.subTest(raw_symbol=raw_symbol):
                alias = normalise_snapshot(
                    {
                        "trade_date": "2026-07-27",
                        "signals": [signal(raw_symbol, "hold", 58.2)],
                    }
                )
                self.assertEqual(alias["signals"][0]["symbol"], "HK00981")

    def test_weekends_are_rejected(self):
        with self.assertRaisesRegex(TrackerInputError, "weekend"):
            normalise_snapshot(
                {"trade_date": "2026-08-01", "signals": [signal("AAA", "hold", 100)]}
            )

    def test_cli_persists_state_and_writes_markdown_summary(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            signals_path = root / "signals.json"
            state_path = root / "state.json"
            summary_path = root / "summary.md"
            signals_path.write_text(
                json.dumps(
                    {
                        "trade_date": "2026-07-27",
                        "signals": [
                            {
                                "symbol": "300408",
                                "name": "三环集团",
                                "signal": "buy",
                                "close": 39.5,
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            exit_code = main(
                [
                    "--signals",
                    str(signals_path),
                    "--state",
                    str(state_path),
                    "--summary",
                    str(summary_path),
                    "--initial-cash",
                    "10000",
                    "--transaction-cost-bps",
                    "0",
                ]
            )

            self.assertEqual(exit_code, 0)
            saved = load_state(state_path)
            self.assertIsNotNone(saved)
            self.assertEqual(saved["metrics"]["snapshot_days"], 1)
            self.assertEqual(saved["metrics"]["completed_trading_days"], 0)
            summary_text = summary_path.read_text(encoding="utf-8")
            self.assertIn("# 20个交易日模拟净值跟踪", summary_text)
            self.assertIn("三环集团", summary_text)
            self.assertIn("不连接券商、不下真实订单", summary_text)
            self.assertIn("基线 1 日 + 已评估 0 / 20", summary_text)

    def test_summary_requires_one_baseline_plus_target_return_intervals(self):
        state = new_state(target_trading_days=2, initial_cash=1_000, transaction_cost_bps=0)
        state = update_state(state, snapshot("2026-07-27", signal("AAA", "hold", 100)))
        state = update_state(state, snapshot("2026-07-28", signal("AAA", "hold", 101)))
        self.assertEqual(state["metrics"]["status"], "running")
        self.assertEqual(state["metrics"]["completed_trading_days"], 1)

        state = update_state(state, snapshot("2026-07-29", signal("AAA", "hold", 102)))

        self.assertEqual(state["metrics"]["status"], "complete")
        self.assertEqual(state["metrics"]["remaining_trading_days"], 0)
        self.assertIn("- 状态：已完成", render_summary(state))
        self.assertIn("基线 1 日 + 已评估 2 / 2", render_summary(state))

    def test_summary_includes_latest_signal_table(self):
        state = new_state(initial_cash=1_000, transaction_cost_bps=0)
        state = update_state(
            state,
            snapshot(
                "2026-07-27",
                signal("HK.00981", "buy", 58.2, name="中芯国际", reason="模拟测试"),
            ),
        )
        summary_text = render_summary(state)
        self.assertIn("## 最新模拟信号（2026-07-27）", summary_text)
        self.assertIn("| HK00981 | 中芯国际 | 模拟买入 |", summary_text)

    def test_summary_explains_multi_currency_normalized_units(self):
        state = update_state(
            new_state(initial_cash=1_000, transaction_cost_bps=0),
            snapshot(
                "2026-07-27",
                signal("300408", "hold", 39.5, name="三环集团"),
                signal("HK00981", "hold", 58.2, name="中芯国际"),
            ),
        )

        summary_text = render_summary(state)

        self.assertIn("模拟净值使用标准化单位", summary_text)
        self.assertIn("A股按 CNY、港股按 HKD", summary_text)
        self.assertIn("暂不包含 HKD/CNY 汇率变动", summary_text)
        self.assertIn("不代表可直接结算的人民币", summary_text)
        self.assertIn("初始模拟净值单位", summary_text)
        self.assertIn("期末净值单位", summary_text)
        self.assertNotIn("初始模拟资金", summary_text)
        self.assertNotIn("当前模拟资产", summary_text)

    def test_render_only_cli_uses_existing_state(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            state_path = root / "state.json"
            summary_path = root / "summary.md"
            state = update_state(
                new_state(initial_cash=1_000, transaction_cost_bps=0),
                snapshot("2026-07-27", signal("AAA", "hold", 100)),
            )
            state_path.write_text(json.dumps(state), encoding="utf-8")
            self.assertEqual(
                main(
                    [
                        "--render-only",
                        "--state",
                        str(state_path),
                        "--summary",
                        str(summary_path),
                    ]
                ),
                0,
            )
            self.assertIn("20个交易日模拟净值跟踪", summary_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
