import unittest
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from scripts.level2_adapter import (
    LEVEL2_AVAILABLE,
    LEVEL2_INCOMPLETE,
    LEVEL2_INVALID,
    LEVEL2_PROVIDER_ERROR,
    LEVEL2_STALE,
    LEVEL2_UNAUTHORIZED,
    LEVEL2_UNAVAILABLE,
    Level2Authorization,
    Level2DataAdapter,
    Level2Snapshot,
    OrderBookLevel,
)
from scripts.intraday_monitor import ReferenceLevels
from scripts.intraday_session import (
    RealtimeQuote,
    _candidate_plan_payload,
    load_state_v2,
    run_cycle,
)


TZ = ZoneInfo("Asia/Shanghai")


def authorization(now, *, authorized=True, scope=("cn",), provider="licensed_test"):
    return Level2Authorization(
        provider=provider,
        authorized=authorized,
        market_scope=scope,
        checked_at=now.isoformat(),
        expires_at=(now + timedelta(days=1)).isoformat(),
        account_reference="masked-account",
    )


def snapshot(
    now,
    *,
    symbol="300408",
    provider="licensed_test",
    data_tier="level2",
    auth=None,
    bid_count=5,
    ask_count=5,
):
    return Level2Snapshot(
        symbol=symbol,
        provider=provider,
        provider_timestamp=now.isoformat(),
        bids=tuple(
            OrderBookLevel(price=100 - index * 0.01, quantity=1000 + index)
            for index in range(bid_count)
        ),
        asks=tuple(
            OrderBookLevel(price=100.01 + index * 0.01, quantity=1000 + index)
            for index in range(ask_count)
        ),
        data_tier=data_tier,
        authorization=auth or authorization(now, provider=provider),
    )


class Provider:
    provider_name = "licensed_test"

    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def fetch_level2(self, symbols, *, now):
        self.calls.append((list(symbols), now))
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


class QuoteFetcher:
    min_interval_seconds = 0

    def __init__(self, quote):
        self.quote = quote

    def fetch(self, symbols, *, now):
        return {self.quote.symbol: self.quote}


class Level2DataAdapterTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 7, 29, 10, 30, tzinfo=TZ)

    def test_default_is_explicitly_unavailable_and_never_level2(self):
        result = Level2DataAdapter().assess(["300408"], now=self.now)["300408"]

        self.assertEqual(result.status, LEVEL2_UNAVAILABLE)
        self.assertFalse(result.usable_as_level2)
        self.assertLess(result.confidence_multiplier, 1)
        self.assertIn("fresh_l1_quote", result.fallback_inputs)
        self.assertIn("no_authorized_level2_provider", result.reason_codes)

    def test_authorized_fresh_complete_book_is_usable(self):
        provider = Provider({"300408": snapshot(self.now)})
        result = Level2DataAdapter(provider).assess(["300408"], now=self.now)[
            "300408"
        ]

        self.assertEqual(result.status, LEVEL2_AVAILABLE)
        self.assertTrue(result.usable_as_level2)
        self.assertEqual(result.confidence_multiplier, 1)
        self.assertEqual(result.bid_levels, 5)
        self.assertEqual(result.ask_levels, 5)
        self.assertEqual(provider.calls[0][0], ["300408"])

    def test_l1_payload_cannot_masquerade_as_level2(self):
        provider = Provider(
            {"300408": snapshot(self.now, data_tier="l1")}
        )
        result = Level2DataAdapter(provider).assess(["300408"], now=self.now)[
            "300408"
        ]

        self.assertEqual(result.status, LEVEL2_INVALID)
        self.assertFalse(result.usable_as_level2)
        self.assertIn("data_tier_not_level2", result.reason_codes)

    def test_missing_or_out_of_scope_authorization_fails_closed(self):
        provider = Provider(
            {
                "300408": snapshot(
                    self.now,
                    auth=authorization(
                        self.now,
                        authorized=False,
                        scope=("hk",),
                    ),
                )
            }
        )
        result = Level2DataAdapter(provider).assess(["300408"], now=self.now)[
            "300408"
        ]

        self.assertEqual(result.status, LEVEL2_UNAUTHORIZED)
        self.assertFalse(result.usable_as_level2)
        self.assertIn("provider_reports_unauthorized", result.reason_codes)
        self.assertIn("market_not_in_authorized_scope", result.reason_codes)

    def test_stale_snapshot_is_not_usable(self):
        old = self.now - timedelta(seconds=30)
        provider = Provider(
            {
                "300408": snapshot(
                    old,
                    auth=authorization(self.now),
                )
            }
        )
        result = Level2DataAdapter(
            provider, freshness_seconds=15
        ).assess(["300408"], now=self.now)["300408"]

        self.assertEqual(result.status, LEVEL2_STALE)
        self.assertFalse(result.usable_as_level2)
        self.assertEqual(result.age_seconds, 30)

    def test_incomplete_or_crossed_depth_is_not_usable(self):
        provider = Provider({"300408": snapshot(self.now, bid_count=2)})
        incomplete = Level2DataAdapter(provider).assess(
            ["300408"], now=self.now
        )["300408"]
        self.assertEqual(incomplete.status, LEVEL2_INCOMPLETE)
        self.assertIn("insufficient_bid_depth", incomplete.reason_codes)

        raw = snapshot(self.now)
        crossed = Level2Snapshot(
            symbol=raw.symbol,
            provider=raw.provider,
            provider_timestamp=raw.provider_timestamp,
            bids=(
                OrderBookLevel(price=101, quantity=100),
                *raw.bids[1:],
            ),
            asks=raw.asks,
            data_tier=raw.data_tier,
            authorization=raw.authorization,
        )
        invalid_depth = Level2DataAdapter(
            Provider({"300408": crossed})
        ).assess(["300408"], now=self.now)["300408"]
        self.assertEqual(invalid_depth.status, LEVEL2_INCOMPLETE)
        self.assertIn("crossed_or_locked_book", invalid_depth.reason_codes)

    def test_provider_failure_degrades_without_raising(self):
        result = Level2DataAdapter(
            Provider(RuntimeError("secret-free failure"))
        ).assess(["300408"], now=self.now)["300408"]

        self.assertEqual(result.status, LEVEL2_PROVIDER_ERROR)
        self.assertFalse(result.usable_as_level2)
        self.assertIn("provider_fetch_failed", result.reason_codes)

    def test_malformed_provider_payload_fails_closed_per_symbol(self):
        malformed = snapshot(self.now)
        object.__setattr__(malformed, "bids", None)
        result = Level2DataAdapter(
            Provider({"300408": malformed})
        ).assess(["300408"], now=self.now)["300408"]

        self.assertEqual(result.status, LEVEL2_PROVIDER_ERROR)
        self.assertFalse(result.usable_as_level2)
        self.assertIn("provider_payload_validation_failed", result.reason_codes)

        malformed_scope = snapshot(self.now)
        object.__setattr__(malformed_scope.authorization, "market_scope", None)
        scope_result = Level2DataAdapter(
            Provider({"300408": malformed_scope})
        ).assess(["300408"], now=self.now)["300408"]
        self.assertEqual(scope_result.status, LEVEL2_PROVIDER_ERROR)

    def test_source_market_mismatch_or_zero_depth_quantity_is_rejected(self):
        wrong_market = snapshot(self.now)
        object.__setattr__(wrong_market, "source_market", "hk")
        market_result = Level2DataAdapter(
            Provider({"300408": wrong_market})
        ).assess(["300408"], now=self.now)["300408"]
        self.assertEqual(market_result.status, LEVEL2_INVALID)
        self.assertIn("source_market_mismatch", market_result.reason_codes)

        zero_depth = snapshot(self.now)
        object.__setattr__(
            zero_depth,
            "bids",
            tuple(
                OrderBookLevel(price=100 - index * 0.01, quantity=0)
                for index in range(5)
            ),
        )
        depth_result = Level2DataAdapter(
            Provider({"300408": zero_depth})
        ).assess(["300408"], now=self.now)["300408"]
        self.assertEqual(depth_result.status, LEVEL2_INCOMPLETE)
        self.assertIn("bid_quantity_invalid", depth_result.reason_codes)

    def test_hk_requires_hk_entitlement_scope(self):
        provider = Provider(
            {
                "HK00981": snapshot(
                    self.now,
                    symbol="HK00981",
                    auth=authorization(self.now, scope=("cn",)),
                )
            }
        )
        result = Level2DataAdapter(provider).assess(["HK00981"], now=self.now)[
            "HK00981"
        ]

        self.assertEqual(result.status, LEVEL2_UNAUTHORIZED)
        self.assertIn("market_not_in_authorized_scope", result.reason_codes)

    def test_missing_level2_reduces_candidate_confidence_without_changing_l1_label(self):
        quote = RealtimeQuote(
            symbol="300408",
            name="三环集团",
            price=40,
            change_pct=1,
            provider_timestamp=self.now.isoformat(),
            fetched_at=self.now.isoformat(),
            stale_seconds=0,
            is_stale=False,
            source="tencent_batch",
        )
        payload = _candidate_plan_payload(
            {
                "symbol": "300408",
                "scope": "simulation",
                "confidence": 0.8,
                "data_quality": "high",
                "plan": {
                    "entry_low": 39,
                    "entry_high": 41,
                    "stop_loss": 37,
                    "target_price": 48,
                },
                "market_costs": {
                    "entry_fee_bps": 5,
                    "exit_fee_bps": 15,
                },
                "expected_holding_days": 20,
                "position_state": "flat",
            },
            quote,
            confidence_multiplier=0.75,
        )

        self.assertIsNotNone(payload)
        self.assertAlmostEqual(payload["confidence"], 0.6)
        self.assertEqual(payload["_raw_confidence"], 0.8)
        self.assertEqual(quote.source, "tencent_batch")

    def test_intraday_cycle_records_fallback_but_keeps_fresh_l1_risk_monitoring(self):
        quote = RealtimeQuote(
            symbol="300408",
            name="三环集团",
            price=40,
            change_pct=-4,
            provider_timestamp=self.now.isoformat(),
            fetched_at=self.now.isoformat(),
            stale_seconds=0,
            is_stale=False,
            source="tencent_batch",
        )
        state = load_state_v2(
            Path("/definitely/missing/level2-state.json"),
            now=self.now,
        )

        result = run_cycle(
            symbols=["300408"],
            state=state,
            levels={"300408": ReferenceLevels()},
            fetcher=QuoteFetcher(quote),
            now=self.now,
            level2_adapter=Level2DataAdapter(),
            notification_sender=lambda **_kwargs: False,
        )

        self.assertEqual(result.valid_quote_count, 1)
        self.assertEqual(result.quotes[0].source, "tencent_batch")
        self.assertEqual(result.level2_assessments[0].status, LEVEL2_UNAVAILABLE)
        self.assertEqual(
            state["provider"]["data_capabilities"]["level2_order_book"],
            "unavailable_using_declared_fallback",
        )
        self.assertTrue(state["provider"]["level2"]["fallback_active"])
        self.assertGreaterEqual(result.new_event_count, 1)

    def test_unexpected_adapter_failure_does_not_stop_l1_risk_monitoring(self):
        class ExplodingAdapter:
            configured = True
            provider = None

            @staticmethod
            def assess(_symbols, *, now):
                raise RuntimeError("provider boundary failed")

        quote = RealtimeQuote(
            symbol="300408",
            name="三环集团",
            price=40,
            change_pct=-4,
            provider_timestamp=self.now.isoformat(),
            fetched_at=self.now.isoformat(),
            stale_seconds=0,
            is_stale=False,
            source="tencent_batch",
        )
        state = load_state_v2(
            Path("/definitely/missing/level2-adapter-state.json"),
            now=self.now,
        )

        result = run_cycle(
            symbols=["300408"],
            state=state,
            levels={"300408": ReferenceLevels()},
            fetcher=QuoteFetcher(quote),
            now=self.now,
            level2_adapter=ExplodingAdapter(),
            notification_sender=lambda **_kwargs: False,
        )

        self.assertEqual(result.valid_quote_count, 1)
        self.assertEqual(result.level2_assessments[0].status, LEVEL2_PROVIDER_ERROR)
        self.assertGreaterEqual(result.new_event_count, 1)


if __name__ == "__main__":
    unittest.main()
