from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock
from zoneinfo import ZoneInfo

import pytest

from scripts.intraday_session import DecisionQuoteFetcher, RealtimeQuote


NOW = datetime(2026, 11, 26, 10, 48, 37, tzinfo=ZoneInfo("Asia/Shanghai"))
SYMBOL = "300308"
TDX_KEY = "300308.SZ"
HK = "HK00700"
OBSERVED_ROW = {
    "HqDate": "20261126",
    "RefreshTime": "104833",
    "Now": "426.35",
    "LastClose": "409.00",
    "Volume": "276961",
    "Name": "中际旭创",
    "DelayMin": "0",
    "IsHKGP": "0",
}


def providers(tencent_rows):
    tencent = SimpleNamespace(fetch=Mock(return_value=tencent_rows), last_diagnostics={})
    hk_quote = RealtimeQuote(
        HK, price=501, provider_timestamp=NOW.isoformat(),
        source="longbridge_batch", provider_snapshot_fresh=True, is_stale=False,
    )
    longbridge = SimpleNamespace(configured=True, fetch=Mock(return_value={HK: hk_quote}))
    return tencent, longbridge, hk_quote


@pytest.mark.parametrize("case", ["missing", "error", "stale", "healthy"])
def test_tdx_repairs_only_unusable_tencent_and_preserves_hk(case):
    stamp = NOW - timedelta(seconds=91 if case == "stale" else 2)
    baseline = RealtimeQuote(SYMBOL, price=425, provider_timestamp=stamp.isoformat(), is_stale=False)
    tencent, longbridge, hk_quote = providers({} if case == "missing" else {SYMBOL: baseline})
    if case == "error":
        tencent.fetch.side_effect = OSError("upstream")
    loader = Mock(return_value={TDX_KEY: OBSERVED_ROW})
    fetcher = DecisionQuoteFetcher(
        tencent_fetcher=tencent, longbridge_fetcher=longbridge, tdx_loader=loader,
    )
    rows = fetcher.fetch([SYMBOL, HK], now=NOW)

    if case == "healthy":
        loader.assert_not_called()
        assert rows[SYMBOL] == baseline
        assert fetcher.last_diagnostics["tdx_fallback"]["status"] == "tencent_healthy"
    else:
        loader.assert_called_once_with([TDX_KEY])
        assert rows[SYMBOL].price == 426.35
        assert rows[SYMBOL].volume == 276961
        assert rows[SYMBOL].source == "tdx_aidata_snapshot"
        assert rows[SYMBOL].provider_timestamp == NOW.replace(second=33).isoformat()
        assert 4 <= rows[SYMBOL].stale_seconds < 5
        assert not rows[SYMBOL].is_stale
        assert fetcher.last_diagnostics["tdx_fallback"]["status"] == "recovered"
    longbridge.fetch.assert_called_once_with([HK], now=NOW)
    assert rows[HK] == hk_quote


@pytest.mark.parametrize("overrides,status", [
    ({"HqDate": "20261125"}, "invalid_quote_timestamp"),
    ({"RefreshTime": ""}, "invalid_quote_timestamp"),
    ({"RefreshTime": "104706"}, "stale_or_future_quote"),
    ({"RefreshTime": "104638"}, "stale_or_future_quote"),
    ({"Now": "nan"}, "invalid_quote_price"),
    ({"LastClose": "0"}, "invalid_quote_price"),
])
def test_invalid_tdx_rows_preserve_stale_primary(overrides, status):
    baseline = RealtimeQuote(SYMBOL, price=424, provider_timestamp=NOW.isoformat(), is_stale=True)
    tencent, longbridge, _ = providers({SYMBOL: baseline})
    fetcher = DecisionQuoteFetcher(
        freshness_seconds=180, tencent_fetcher=tencent, longbridge_fetcher=longbridge,
        tdx_loader=Mock(return_value={TDX_KEY: {**OBSERVED_ROW, **overrides}}),
    )
    assert fetcher.fetch([SYMBOL], now=NOW)[SYMBOL] == baseline
    assert fetcher.last_diagnostics["tdx_fallback"]["status"] == status


@pytest.mark.parametrize("symbols,hour,minute", [
    ([HK], 10, 48), (["600000", "000001"], 10, 48), ([SYMBOL], 9, 25),
    ([SYMBOL], 12, 0), ([SYMBOL], 14, 57),
])
def test_tdx_called_only_for_verified_stock_during_continuous_trading(symbols, hour, minute):
    tencent, longbridge, _ = providers({})
    loader = Mock()
    fetcher = DecisionQuoteFetcher(
        tencent_fetcher=tencent, longbridge_fetcher=longbridge, tdx_loader=loader,
    )
    fetcher.fetch(symbols, now=NOW.replace(hour=hour, minute=minute))
    loader.assert_not_called()


def test_official_sdk_failure_preserves_hk_and_no_quotes_are_fabricated():
    tencent, longbridge, hk_quote = providers({})
    fetcher = DecisionQuoteFetcher(
        tencent_fetcher=tencent, longbridge_fetcher=longbridge,
        tdx_loader=Mock(side_effect=TimeoutError("unavailable")),
    )
    assert fetcher.fetch([SYMBOL, HK], now=NOW) == {HK: hk_quote}
    assert fetcher.last_diagnostics["tdx_fallback"]["status"] == "provider_error"


def test_sdk_config_is_private_and_removed_on_timeout(tmp_path, monkeypatch):
    import configparser

    from src.services import tdx_decision_quotes as adapter

    assets = tmp_path / "lib"
    assets.mkdir()
    (assets / "libTdxAiData.so").write_bytes(b"test")
    default_ini = assets / "TdxAiData.ini"
    default_ini.write_text("[Token]\ntoken=\n")
    monkeypatch.setenv("TDX_DATA_TOKEN", "test-value")
    monkeypatch.setattr(
        adapter.importlib.util, "find_spec",
        lambda _: SimpleNamespace(origin=str(tmp_path / "__init__.py")),
    )
    staged = []

    def read(codes, library):
        assert codes == [TDX_KEY]
        staged.append(library)
        assert library.parent.stat().st_mode & 0o077 == 0
        cfg = configparser.ConfigParser()
        cfg.read(library.with_name("TdxAiData.ini"))
        assert cfg["Token"]["token"] == "test-value"
        raise TimeoutError()

    monkeypatch.setattr(adapter, "_read_snapshots", read)
    monkeypatch.setattr(adapter, "run_isolated", lambda callback, timeout: callback())
    with pytest.raises(TimeoutError):
        adapter.fetch_dated_snapshots([TDX_KEY])
    assert not staged[0].parent.exists()
    assert default_ini.read_text() == "[Token]\ntoken=\n"


def test_multi_market_fallback_only_queries_missing_rows_and_keeps_existing_sources():
    baseline = RealtimeQuote("000001", price=12.4, provider_timestamp=NOW.isoformat(),
                             is_stale=False, source="tencent_batch")
    tencent, longbridge, hk_quote = providers({"000001": baseline})
    loader = Mock(return_value={
        "600519.SH": {**OBSERVED_ROW, "Name": "贵州茅台", "Now": "1469", "LastClose": "1474.84"},
        "920002.BJ": {**OBSERVED_ROW, "Name": "万达轴承", "Now": "41.8", "LastClose": "42.65"},
    })
    fetcher = DecisionQuoteFetcher(
        tencent_fetcher=tencent, longbridge_fetcher=longbridge, tdx_loader=loader,
        tdx_exchanges=("SH", "SZ", "BJ", "HK"),
    )
    rows = fetcher.fetch(["600519", "000001", "920002", HK], now=NOW)
    loader.assert_called_once_with(["600519.SH", "920002.BJ"])
    assert rows["000001"] == baseline and rows[HK] == hk_quote
    assert rows["600519"].price == 1469 and rows["920002"].price == 41.8
    assert fetcher.last_diagnostics["tdx_fallback"]["recovered_by_market"] == {"SH": 1, "BJ": 1}


@pytest.mark.parametrize("delay,market_flag,expected", [
    ("0", "1", "recovered"), ("15", "1", "provider_delay_or_unverified"),
    (None, "1", "provider_delay_or_unverified"), ("0", "0", "invalid_security_metadata"),
])
def test_hong_kong_requires_both_current_and_undelayed_provider_quote(delay, market_flag, expected):
    tencent, longbridge, _ = providers({})
    longbridge.fetch.side_effect = TimeoutError()
    loader = Mock(return_value={
        "00700.HK": {**OBSERVED_ROW, "IsHKGP": market_flag, "DelayMin": delay,
                     "Name": "腾讯控股", "Now": "341", "LastClose": "337.4"},
    })
    fetcher = DecisionQuoteFetcher(
        tencent_fetcher=tencent, longbridge_fetcher=longbridge, tdx_loader=loader,
        tdx_exchanges=("HK",),
    )
    rows = fetcher.fetch([HK], now=NOW)
    loader.assert_called_once_with(["00700.HK"])
    assert fetcher.last_diagnostics["longbridge_fetch_error"] == "TimeoutError"
    assert fetcher.last_diagnostics["tdx_fallback"]["status"] == expected
    assert rows[HK].is_stale is (expected != "recovered")


def test_hk_tencent_fallback_stays_ahead_of_tdx():
    baseline = RealtimeQuote(HK, price=341, provider_timestamp=NOW.isoformat(),
                             is_stale=False, source="tencent_batch")
    tencent, longbridge, _ = providers({HK: baseline})
    longbridge.fetch.return_value = {}
    loader = Mock()
    fetcher = DecisionQuoteFetcher(
        tencent_fetcher=tencent, longbridge_fetcher=longbridge, tdx_loader=loader, tdx_exchanges=("HK",),
    )
    rows = fetcher.fetch([HK], now=NOW)
    loader.assert_not_called()
    assert rows[HK].source == "tencent_batch:fresh_hk_fallback"


def test_sdk_never_batches_mainland_and_hong_kong_together(tmp_path, monkeypatch):
    import sys

    from src.services.tdx_decision_quotes import _read_snapshots

    def rows(stock_list, field_list):
        assert len(stock_list) <= 32
        assert not any(c.endswith(".HK") for c in stock_list) or all(c.endswith(".HK") for c in stock_list)
        return {c: OBSERVED_ROW for c in stock_list}

    tqs = SimpleNamespace(
        get_more_info_batch=Mock(side_effect=rows),
        get_market_snapshot_batch=Mock(side_effect=rows),
        get_stock_info_batch=Mock(side_effect=rows),
    )
    monkeypatch.setitem(sys.modules, "tdxaidata", SimpleNamespace(tqs=tqs))
    monkeypatch.setenv("TDX_AI_DATA_LIB", "")
    codes = [f"{i:06d}.SZ" for i in range(34)] + ["00700.HK"]
    assert set(_read_snapshots(codes, tmp_path / "lib.so")) == set(codes)
    assert tqs.get_market_snapshot_batch.call_count == 3
