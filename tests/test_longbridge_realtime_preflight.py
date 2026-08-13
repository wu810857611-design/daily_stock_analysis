from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from scripts.check_longbridge_realtime import _normalise_symbols, verify_context
from scripts.generate_longbridge_oauth_token import (
    _has_hk_realtime_package,
    _set_aside_token_cache,
    _token_cache_path,
)


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


class FakeContext:
    def __init__(self, *, package_key: str, timestamp: datetime):
        self.package_key = package_key
        self.timestamp = timestamp

    def quote_package_details(self):
        return [
            SimpleNamespace(
                key=self.package_key,
                name=("15-min Delay" if self.package_key == "HK_Basic" else "LV1 Real-time Quotes"),
                description="",
            )
        ]

    def quote_level(self):
        return "LV1"

    def quote(self, symbols):
        return [
            SimpleNamespace(
                symbol=symbol,
                last_done="321.20",
                timestamp=self.timestamp,
            )
            for symbol in symbols
        ]


def test_hk_basic_is_not_mistaken_for_realtime_package():
    assert not _has_hk_realtime_package([{"key": "HK_Basic", "name": "15-min Delay", "description": ""}])
    assert not _has_hk_realtime_package(
        [
            {
                "key": "HK_HangSengIndex_AllTerminals",
                "name": "Real-time Quotes",
                "description": "",
            }
        ]
    )
    assert _has_hk_realtime_package(
        [
            {
                "key": "HK_L1_OpenAPI",
                "name": "LV1 Real-time Quotes",
                "description": "",
            }
        ]
    )


def test_force_reauthorization_sets_existing_cache_aside(tmp_path: Path):
    token_cache = tmp_path / "client-id"
    token_cache.write_text("old-token", encoding="utf-8")

    backup = _set_aside_token_cache(token_cache)

    assert backup is not None
    assert not token_cache.exists()
    assert backup.read_text(encoding="utf-8") == "old-token"
    assert ".before-reauth-" in backup.name


def test_token_cache_path_rejects_path_traversal():
    with pytest.raises(ValueError):
        _token_cache_path("../client-id")


def test_live_preflight_passes_realtime_package_and_fresh_provider_times():
    now = datetime(2026, 8, 13, 13, 30, tzinfo=SHANGHAI_TZ)
    context = FakeContext(
        package_key="HK_L1_OpenAPI",
        timestamp=now - timedelta(seconds=8),
    )

    result = verify_context(
        context,
        symbols=["700.HK", "9988.HK"],
        now=now,
        freshness_seconds=90,
    )

    assert result["passed"] is True
    assert result["hk_realtime_package"] is True
    assert all(quote["fresh"] for quote in result["quotes"])


def test_live_preflight_explains_oauth_hk_basic_delay():
    now = datetime(2026, 8, 13, 13, 30, tzinfo=SHANGHAI_TZ)
    context = FakeContext(
        package_key="HK_Basic",
        timestamp=now - timedelta(minutes=15),
    )

    result = verify_context(
        context,
        symbols=["700.HK"],
        now=now,
        freshness_seconds=90,
    )

    assert result["passed"] is False
    assert result["issues"] == [
        "hk_realtime_package_missing",
        "provider_timestamp_stale:700.HK",
    ]


def test_permission_preflight_allows_closed_market_timestamp_age():
    now = datetime(2026, 8, 13, 12, 15, tzinfo=SHANGHAI_TZ)
    context = FakeContext(
        package_key="HK_L1_OpenAPI",
        timestamp=now - timedelta(minutes=15),
    )

    result = verify_context(
        context,
        symbols=["700.HK"],
        now=now,
        freshness_seconds=90,
        require_fresh=False,
    )

    assert result["passed"] is True
    assert result["mode"] == "permission"
    assert result["quotes"][0]["fresh"] is False


def test_preflight_symbol_normalisation_is_strict_and_deduplicated():
    assert _normalise_symbols("00700.hk,700.HK 09988.HK") == [
        "700.HK",
        "9988.HK",
    ]
    with pytest.raises(ValueError):
        _normalise_symbols("AAPL.US")
