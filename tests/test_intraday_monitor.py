import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

from scripts.intraday_monitor import (
    MonitorError,
    QuoteSnapshot,
    ReferenceLevels,
    dedupe_alerts,
    evaluate_quote,
    load_reference_levels,
    new_state,
    run_monitor,
    send_pushplus,
)


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


class FakeFetcherManager:
    def __init__(self, quotes):
        self.quotes = quotes
        self.calls = []

    def get_realtime_quote(self, symbol, *, log_final_failure=True):
        self.calls.append((symbol, log_final_failure))
        return self.quotes.get(symbol)


def create_reference_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE analysis_history (
                id INTEGER PRIMARY KEY,
                code TEXT NOT NULL,
                name TEXT,
                stop_loss REAL,
                take_profit REAL,
                created_at TEXT
            );
            CREATE TABLE decision_signals (
                id INTEGER PRIMARY KEY,
                stock_code TEXT NOT NULL,
                stock_name TEXT,
                source_type TEXT,
                status TEXT,
                stop_loss REAL,
                target_price REAL,
                created_at TEXT,
                expires_at TEXT
            );
            """
        )
        connection.execute(
            """
            INSERT INTO analysis_history
                (code, name, stop_loss, take_profit, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("HK.00981", "中芯国际", 48.0, 68.0, "2026-07-27 18:00:00"),
        )
        connection.execute(
            """
            INSERT INTO decision_signals
                (stock_code, stock_name, source_type, status, stop_loss,
                 target_price, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "00981.HK",
                "中芯国际",
                "analysis",
                "active",
                50.0,
                70.0,
                "2026-07-28 09:00:00",
                None,
            ),
        )
        connection.commit()
    finally:
        connection.close()


class IntradayMonitorTests(unittest.TestCase):
    def test_loads_latest_signal_levels_for_hong_kong_alias(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            database_path = Path(temporary_directory) / "analysis.db"
            create_reference_database(database_path)

            levels = load_reference_levels(database_path, "HK00981")

        self.assertEqual(levels.name, "中芯国际")
        self.assertEqual(levels.stop_loss, 50.0)
        self.assertEqual(levels.target_price, 70.0)
        self.assertEqual(levels.stop_source, "decision_signals")
        self.assertEqual(levels.target_source, "decision_signals")

    def test_expired_signal_and_old_history_are_not_actionable(self):
        fixed_now = datetime(2026, 7, 28, 10, 30, tzinfo=SHANGHAI_TZ)
        with tempfile.TemporaryDirectory() as temporary_directory:
            database_path = Path(temporary_directory) / "analysis.db"
            create_reference_database(database_path)
            connection = sqlite3.connect(database_path)
            connection.execute(
                "UPDATE decision_signals "
                "SET expires_at = '2026-07-28 01:00:00'"
            )
            connection.execute(
                "UPDATE analysis_history SET created_at = '2026-07-01 00:00:00'"
            )
            connection.commit()
            connection.close()

            levels = load_reference_levels(
                database_path,
                "HK00981",
                now=fixed_now,
            )

        self.assertIsNone(levels.stop_loss)
        self.assertIsNone(levels.target_price)

    def test_emits_only_conservative_risk_and_volatility_conditions(self):
        quote = QuoteSnapshot(
            symbol="HK00981",
            name="中芯国际",
            price=49.0,
            change_pct=-4.2,
        )
        levels = ReferenceLevels(stop_loss=50.0, target_price=70.0)

        alerts = evaluate_quote(
            quote,
            levels,
            down_threshold_pct=3.0,
            up_threshold_pct=5.0,
        )

        self.assertEqual(
            {alert.condition for alert in alerts},
            {"sharp_drop", "stop_loss"},
        )
        self.assertTrue(all("不会下单" in alert.message for alert in alerts))
        self.assertFalse(any("建议买入" in alert.message for alert in alerts))
        self.assertFalse(any("建议卖出" in alert.message for alert in alerts))

        stale_alerts = evaluate_quote(
            QuoteSnapshot(
                symbol="HK00981",
                name="中芯国际",
                price=49.0,
                change_pct=-8.0,
                is_stale=True,
            ),
            levels,
            down_threshold_pct=3.0,
            up_threshold_pct=5.0,
        )
        self.assertEqual(stale_alerts, [])

    def test_dedupes_each_symbol_condition_per_shanghai_trade_date(self):
        alerts = evaluate_quote(
            QuoteSnapshot(
                symbol="300408",
                name="三环集团",
                price=30.0,
                change_pct=-4.0,
            ),
            ReferenceLevels(stop_loss=31.0),
            down_threshold_pct=3.0,
            up_threshold_pct=5.0,
        )
        state = new_state()

        first, first_suppressed = dedupe_alerts(alerts, state, "2026-07-28")
        repeated, repeated_suppressed = dedupe_alerts(alerts, state, "2026-07-28")
        next_day, next_day_suppressed = dedupe_alerts(alerts, state, "2026-07-29")

        self.assertEqual(len(first), 2)
        self.assertEqual(first_suppressed, 0)
        self.assertEqual(repeated, [])
        self.assertEqual(repeated_suppressed, 2)
        self.assertEqual(len(next_day), 2)
        self.assertEqual(next_day_suppressed, 0)

    def test_trigger_is_persisted_and_notified_once_without_network(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            database_path = root / "analysis.db"
            state_path = root / "state.json"
            report_path = root / "report.md"
            create_reference_database(database_path)
            manager = FakeFetcherManager(
                {
                    "HK00981": SimpleNamespace(
                        name="中芯国际",
                        price=49.0,
                        change_pct=-4.2,
                        is_stale=False,
                        source="fake",
                    )
                }
            )
            notifications = []

            def fake_sender(**payload):
                notifications.append(payload)
                return True

            fixed_now = datetime(2026, 7, 28, 10, 30, tzinfo=SHANGHAI_TZ)
            with patch.dict(os.environ, {"PUSHPLUS_TOKEN": "test-token"}, clear=False):
                first = run_monitor(
                    stocks="HK.00981",
                    database_path=database_path,
                    state_path=state_path,
                    report_path=report_path,
                    down_threshold_pct=3.0,
                    up_threshold_pct=5.0,
                    notify=True,
                    fetcher_manager=manager,
                    now=fixed_now,
                    notification_sender=fake_sender,
                )
                second = run_monitor(
                    stocks="00981.HK",
                    database_path=database_path,
                    state_path=state_path,
                    report_path=report_path,
                    down_threshold_pct=3.0,
                    up_threshold_pct=5.0,
                    notify=True,
                    fetcher_manager=manager,
                    now=fixed_now,
                    notification_sender=fake_sender,
                )

            self.assertEqual(len(first.alerts), 2)
            self.assertTrue(first.notified)
            self.assertEqual(second.alerts, [])
            self.assertFalse(second.notified)
            self.assertEqual(second.suppressed_count, 2)
            self.assertEqual(len(notifications), 1)
            self.assertEqual(notifications[0]["token"], "test-token")
            self.assertIn("盘中模拟风险监控", notifications[0]["content"])
            self.assertEqual(
                manager.calls,
                [("HK00981", False), ("HK00981", False)],
            )
            saved_state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(
                saved_state["conditions_by_date"]["2026-07-28"]["HK00981"],
                ["sharp_drop", "stop_loss"],
            )
            self.assertIn("仅用于模拟风险监控", report_path.read_text(encoding="utf-8"))

    def test_no_trigger_means_no_push_even_when_notify_is_requested(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            manager = FakeFetcherManager(
                {
                    "300408": {
                        "name": "三环集团",
                        "price": 40.0,
                        "change_pct": 0.8,
                    }
                }
            )
            notifications = []
            with patch.dict(os.environ, {"PUSHPLUS_TOKEN": "test-token"}, clear=False):
                result = run_monitor(
                    stocks="300408",
                    database_path=root / "missing.db",
                    state_path=root / "state.json",
                    report_path=root / "report.md",
                    down_threshold_pct=3.0,
                    up_threshold_pct=5.0,
                    notify=True,
                    fetcher_manager=manager,
                    now=datetime(2026, 7, 28, 14, 30, tzinfo=SHANGHAI_TZ),
                    notification_sender=lambda **payload: notifications.append(payload) or True,
                )

            self.assertEqual(result.alerts, [])
            self.assertFalse(result.notified)
            self.assertEqual(notifications, [])
            self.assertIn(
                "没有新的风险或显著波动条件触发",
                result.report_path.read_text(encoding="utf-8"),
            )

    def test_low_quote_coverage_writes_report_but_fails_without_persisting_state(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            state_path = root / "state.json"
            report_path = root / "report.md"
            manager = FakeFetcherManager(
                {
                    "300408": {
                        "name": "三环集团",
                        "price": 40.0,
                        "change_pct": 0.8,
                    },
                    "688333": None,
                }
            )

            with self.assertRaisesRegex(MonitorError, "有效实时行情覆盖率不足"):
                run_monitor(
                    stocks="300408,688333",
                    database_path=root / "missing.db",
                    state_path=state_path,
                    report_path=report_path,
                    down_threshold_pct=3.0,
                    up_threshold_pct=5.0,
                    min_quote_coverage=0.8,
                    fetcher_manager=manager,
                    now=datetime(2026, 7, 28, 14, 30, tzinfo=SHANGHAI_TZ),
                )

            self.assertFalse(state_path.exists())
            self.assertTrue(report_path.exists())
            report = report_path.read_text(encoding="utf-8")
            self.assertIn("有效行情：1/2（覆盖率 50.00%）", report)
            self.assertIn("数据可靠性警告", report)
            self.assertIn("本次任务将失败", report)

    def test_zero_valid_quotes_fail_even_when_minimum_coverage_is_zero(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            report_path = root / "report.md"

            with self.assertRaisesRegex(
                MonitorError, r"0/1（0\.00%）"
            ):
                run_monitor(
                    stocks="300408",
                    database_path=root / "missing.db",
                    state_path=root / "state.json",
                    report_path=report_path,
                    down_threshold_pct=3.0,
                    up_threshold_pct=5.0,
                    min_quote_coverage=0.0,
                    fetcher_manager=FakeFetcherManager({"300408": None}),
                    now=datetime(2026, 7, 28, 10, 30, tzinfo=SHANGHAI_TZ),
                )

            self.assertTrue(report_path.exists())

    def test_failed_push_is_not_deduped_and_successful_retry_persists_it(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            database_path = root / "analysis.db"
            state_path = root / "state.json"
            report_path = root / "report.md"
            create_reference_database(database_path)
            manager = FakeFetcherManager(
                {
                    "HK00981": SimpleNamespace(
                        name="中芯国际",
                        price=49.0,
                        change_pct=-4.2,
                        is_stale=False,
                        source="fake",
                    )
                }
            )
            attempts = []

            def failed_sender(**payload):
                attempts.append(("failed", payload))
                return False

            def successful_sender(**payload):
                attempts.append(("success", payload))
                return True

            fixed_now = datetime(2026, 7, 28, 10, 30, tzinfo=SHANGHAI_TZ)
            with patch.dict(os.environ, {"PUSHPLUS_TOKEN": "test-token"}, clear=False):
                with self.assertRaisesRegex(MonitorError, "PushPlus 推送失败"):
                    run_monitor(
                        stocks="HK.00981",
                        database_path=database_path,
                        state_path=state_path,
                        report_path=report_path,
                        down_threshold_pct=3.0,
                        up_threshold_pct=5.0,
                        notify=True,
                        fetcher_manager=manager,
                        now=fixed_now,
                        notification_sender=failed_sender,
                    )

                self.assertFalse(state_path.exists())

                retried = run_monitor(
                    stocks="HK.00981",
                    database_path=database_path,
                    state_path=state_path,
                    report_path=report_path,
                    down_threshold_pct=3.0,
                    up_threshold_pct=5.0,
                    notify=True,
                    fetcher_manager=manager,
                    now=fixed_now,
                    notification_sender=successful_sender,
                )

            self.assertEqual(len(retried.alerts), 2)
            self.assertTrue(retried.notified)
            self.assertEqual([attempt[0] for attempt in attempts], ["failed", "success"])
            saved_state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(
                saved_state["conditions_by_date"]["2026-07-28"]["HK00981"],
                ["sharp_drop", "stop_loss"],
            )

    def test_pushplus_sender_posts_markdown_over_https_without_live_network(self):
        captured = {}

        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            @staticmethod
            def read():
                return b'{"code": 200}'

        def fake_urlopen(outbound, timeout):
            captured["url"] = outbound.full_url
            captured["method"] = outbound.get_method()
            captured["payload"] = json.loads(outbound.data.decode("utf-8"))
            captured["timeout"] = timeout
            return FakeResponse()

        with patch("scripts.intraday_monitor.request.urlopen", side_effect=fake_urlopen):
            sent = send_pushplus(
                token="test-token",
                title="盘中模拟风险提醒",
                content="# 风险提醒",
                timeout_seconds=3.0,
            )

        self.assertTrue(sent)
        self.assertTrue(captured["url"].startswith("https://"))
        self.assertEqual(captured["method"], "POST")
        self.assertEqual(captured["payload"]["token"], "test-token")
        self.assertEqual(captured["payload"]["template"], "markdown")
        self.assertEqual(captured["timeout"], 3.0)


if __name__ == "__main__":
    unittest.main()
