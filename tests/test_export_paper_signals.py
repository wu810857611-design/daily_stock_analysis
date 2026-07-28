import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.export_paper_signals import (
    CoverageError,
    ExportError,
    build_snapshot,
    canonicalize_symbol,
    main,
    map_action,
    normalise_analysis_since,
)
from scripts.paper_trade_tracker import normalise_snapshot


SCHEMA = """
CREATE TABLE stock_daily (
    id INTEGER PRIMARY KEY,
    code TEXT NOT NULL,
    date TEXT NOT NULL,
    close REAL
);
CREATE TABLE analysis_history (
    id INTEGER PRIMARY KEY,
    code TEXT NOT NULL,
    name TEXT,
    operation_advice TEXT,
    analysis_summary TEXT,
    raw_result TEXT,
    context_snapshot TEXT,
    created_at TEXT
);
CREATE TABLE decision_signals (
    id INTEGER PRIMARY KEY,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    market TEXT NOT NULL,
    source_report_id INTEGER,
    action TEXT,
    action_label TEXT,
    reason TEXT,
    created_at TEXT
);
"""


def new_database(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(str(path))
    connection.executescript(SCHEMA)
    return connection


class ExportPaperSignalsTests(unittest.TestCase):
    def test_hk_aliases_are_canonicalized(self):
        aliases = ("HK.00981", "hk00981", "00981.HK", "00981")
        self.assertEqual(
            [canonicalize_symbol(alias) for alias in aliases],
            ["HK00981"] * len(aliases),
        )
        self.assertEqual(canonicalize_symbol("300408"), "300408")

    def test_latest_decision_signal_wins_and_joins_report_name(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / "analysis.db"
            connection = new_database(db_path)
            connection.execute(
                """
                INSERT INTO analysis_history
                    (id, code, name, operation_advice, analysis_summary,
                     raw_result, context_snapshot, created_at)
                VALUES (1, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "00981.HK",
                    "中芯国际",
                    "卖出",
                    "joined report",
                    "{}",
                    "{}",
                    "2026-07-28 08:00:00",
                ),
            )
            connection.executemany(
                """
                INSERT INTO decision_signals
                    (id, stock_code, stock_name, market, source_report_id,
                     action, action_label, reason, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (1, "00981", "", "hk", 1, "buy", "买入", "older", "2026-07-28 08:30:00"),
                    (2, "HK.00981", "", "hk", 1, "watch", "观望", "latest", "2026-07-28 09:30:00"),
                    (3, "00981", "", "hk", 1, "sell", "卖出", "future", "2026-07-29 09:30:00"),
                ],
            )
            connection.executemany(
                "INSERT INTO stock_daily (id, code, date, close) VALUES (?, ?, ?, ?)",
                [
                    (1, "00981.HK", "2026-07-27", 57.0),
                    (2, "HK00981", "2026-07-28", 58.2),
                    (3, "00981", "2026-07-29", 99.0),
                ],
            )
            connection.commit()

            snapshot = build_snapshot(
                connection,
                stocks=["HK.00981"],
                trade_date="2026-07-28",
                min_coverage=1.0,
            )
            connection.close()

        self.assertEqual(snapshot["signals"], [
            {
                "symbol": "HK00981",
                "name": "中芯国际",
                "signal": "hold",
                "close": 58.2,
                "reason": "latest",
                "source": "decision_signals",
                "price_source": "stock_daily:2026-07-28",
            }
        ])
        self.assertEqual(snapshot["metadata"]["coverage"], 1.0)
        self.assertTrue(snapshot["metadata"]["simulation_only"])
        self.assertFalse(snapshot["metadata"]["places_real_orders"])
        self.assertEqual(set(snapshot), {"trade_date", "signals", "metadata"})
        self.assertEqual(
            {"symbol", "name", "signal", "close", "reason"}.difference(snapshot["signals"][0]),
            set(),
        )
        self.assertEqual(normalise_snapshot(snapshot)["signals"][0]["symbol"], "HK00981")

    def test_analysis_history_and_realtime_context_are_fallbacks(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / "analysis.db"
            connection = new_database(db_path)
            connection.execute(
                """
                INSERT INTO analysis_history
                    (id, code, name, operation_advice, analysis_summary,
                     raw_result, context_snapshot, created_at)
                VALUES (1, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "300408",
                    "三环集团",
                    None,
                    "analysis fallback",
                    json.dumps({"operation_advice": "买入", "current_price": 40}),
                    json.dumps({"realtime_quote_raw": {"price": 39.5}}),
                    "2026-07-28T10:00:00",
                ),
            )
            connection.commit()

            snapshot = build_snapshot(
                connection,
                stocks=["300408"],
                trade_date="2026-07-28",
                min_coverage=1.0,
            )
            connection.close()

        signal = snapshot["signals"][0]
        self.assertEqual(signal["signal"], "buy")
        self.assertEqual(signal["close"], 39.5)
        self.assertEqual(signal["source"], "analysis_history")
        self.assertEqual(signal["price_source"], "analysis_history:context_snapshot")

    def test_unjoined_decision_matches_same_day_analysis_for_name_and_price(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / "analysis.db"
            connection = new_database(db_path)
            connection.execute(
                """
                INSERT INTO analysis_history
                    (id, code, name, operation_advice, analysis_summary,
                     raw_result, context_snapshot, created_at)
                VALUES (1, '01347.HK', '华虹半导体', '持有', 'same-day match',
                        '{}', ?, '2026-07-28 09:00:00')
                """,
                (json.dumps({"enhanced_context": {"realtime": {"price": 72.4}}}),),
            )
            connection.execute(
                """
                INSERT INTO decision_signals
                    (id, stock_code, stock_name, market, source_report_id,
                     action, action_label, reason, created_at)
                VALUES (1, 'HK01347', '', 'hk', NULL, 'sell', '卖出', '',
                        '2026-07-28 10:00:00')
                """
            )
            connection.commit()

            snapshot = build_snapshot(
                connection,
                stocks=["HK.01347"],
                trade_date="2026-07-28",
                min_coverage=1.0,
            )
            connection.close()

        signal = snapshot["signals"][0]
        self.assertEqual(signal["symbol"], "HK01347")
        self.assertEqual(signal["name"], "华虹半导体")
        self.assertEqual(signal["signal"], "sell")
        self.assertEqual(signal["close"], 72.4)
        self.assertEqual(signal["source"], "decision_signals")

    def test_action_mapping_defaults_ambiguous_language_to_hold(self):
        self.assertEqual(map_action("add"), "buy")
        self.assertEqual(map_action("reduce"), "sell")
        self.assertEqual(map_action("不建议买入，继续观察"), "hold")
        self.assertEqual(map_action("买盘增强，继续观察"), "hold")
        self.assertEqual(map_action("unrecognised prose"), "hold")

    def test_partial_coverage_is_reported_and_threshold_controls_failure(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / "analysis.db"
            connection = new_database(db_path)
            connection.execute(
                """
                INSERT INTO analysis_history
                    (id, code, name, operation_advice, analysis_summary,
                     raw_result, context_snapshot, created_at)
                VALUES (1, '300408', '三环集团', '持有', '', '{}', '{}',
                        '2026-07-28 10:00:00')
                """
            )
            connection.execute(
                "INSERT INTO stock_daily (id, code, date, close) VALUES (1, '300408', '2026-07-28', 39.5)"
            )
            connection.commit()

            snapshot = build_snapshot(
                connection,
                stocks=["300408", "HK01347"],
                trade_date="2026-07-28",
                min_coverage=0.5,
            )
            self.assertEqual(snapshot["metadata"]["coverage"], 0.5)
            self.assertEqual(snapshot["metadata"]["missing_symbols"], ["HK01347"])
            self.assertEqual(
                snapshot["metadata"]["missing_details"][0]["reasons"],
                [
                    "no_signal_or_analysis_for_trade_date",
                    "no_reference_price_for_trade_date",
                ],
            )

            with self.assertRaises(CoverageError) as raised:
                build_snapshot(
                    connection,
                    stocks=["300408", "HK01347"],
                    trade_date="2026-07-28",
                    min_coverage=0.75,
                )
            connection.close()

        self.assertEqual(raised.exception.snapshot["metadata"]["covered_count"], 1)

    def test_stale_stock_daily_price_does_not_count_toward_coverage(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / "analysis.db"
            connection = new_database(db_path)
            connection.execute(
                """
                INSERT INTO analysis_history
                    (id, code, name, operation_advice, analysis_summary,
                     raw_result, context_snapshot, created_at)
                VALUES (1, '300408', '三环集团', '持有', '', '{}', '{}',
                        '2026-07-28 10:00:00')
                """
            )
            connection.execute(
                "INSERT INTO stock_daily (id, code, date, close) VALUES (1, '300408', '2026-07-27', 39.5)"
            )
            connection.commit()

            with self.assertRaises(CoverageError) as raised:
                build_snapshot(
                    connection,
                    stocks=["300408"],
                    trade_date="2026-07-28",
                    min_coverage=1.0,
                )
            connection.close()

        snapshot = raised.exception.snapshot
        self.assertEqual(snapshot["metadata"]["coverage"], 0.0)
        self.assertEqual(
            snapshot["metadata"]["missing_details"],
            [{"symbol": "300408", "reasons": ["stale_stock_daily_price:2026-07-27"]}],
        )

    def test_analysis_since_selects_next_utc_day_records_for_previous_trade_date(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            db_path = Path(temporary_directory) / "analysis.db"
            connection = new_database(db_path)
            connection.execute(
                """
                INSERT INTO analysis_history
                    (id, code, name, operation_advice, analysis_summary,
                     raw_result, context_snapshot, created_at)
                VALUES (1, '300408', '三环集团', '买入', 'next UTC day analysis',
                        '{}', '{}', '2026-07-28 00:15:00')
                """
            )
            connection.execute(
                """
                INSERT INTO decision_signals
                    (id, stock_code, stock_name, market, source_report_id,
                     action, action_label, reason, created_at)
                VALUES (1, '300408', '三环集团', 'cn', 1, 'buy', '买入',
                        'next UTC day decision', '2026-07-28 00:16:00')
                """
            )
            connection.execute(
                "INSERT INTO stock_daily (id, code, date, close) VALUES (1, '300408', '2026-07-27', 39.5)"
            )
            connection.commit()

            with self.assertRaises(CoverageError):
                build_snapshot(
                    connection,
                    stocks=["300408"],
                    trade_date="2026-07-27",
                    min_coverage=1.0,
                )

            snapshot = build_snapshot(
                connection,
                stocks=["300408"],
                trade_date="2026-07-27",
                min_coverage=1.0,
                analysis_since="2026-07-28T00:00:00Z",
            )
            connection.close()

        self.assertEqual(snapshot["trade_date"], "2026-07-27")
        self.assertEqual(snapshot["signals"][0]["signal"], "buy")
        self.assertEqual(snapshot["signals"][0]["close"], 39.5)
        self.assertEqual(snapshot["signals"][0]["price_source"], "stock_daily:2026-07-27")
        self.assertEqual(snapshot["signals"][0]["source"], "decision_signals")
        self.assertEqual(snapshot["metadata"]["analysis_since"], "2026-07-28 00:00:00")

    def test_analysis_since_requires_explicit_utc_timezone(self):
        self.assertEqual(
            normalise_analysis_since("2026-07-28T00:00:00+00:00"),
            "2026-07-28 00:00:00",
        )
        with self.assertRaisesRegex(ExportError, "UTC timezone"):
            normalise_analysis_since("2026-07-28T00:00:00")

    def test_cli_writes_diagnostic_snapshot_when_coverage_is_too_low(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            db_path = root / "analysis.db"
            output_path = root / "signals.json"
            connection = new_database(db_path)
            connection.close()

            exit_code = main(
                [
                    "--db",
                    str(db_path),
                    "--stocks",
                    "300408,HK.00981",
                    "--trade-date",
                    "2026-07-28",
                    "--min-coverage",
                    "0.5",
                    "--output",
                    str(output_path),
                ]
            )

            self.assertEqual(exit_code, 2)
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["metadata"]["coverage"], 0.0)
            self.assertEqual(payload["metadata"]["missing_symbols"], ["300408", "HK00981"])


if __name__ == "__main__":
    unittest.main()
