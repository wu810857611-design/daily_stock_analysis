import multiprocessing
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from src.search_service import SearchResponse, SearchResult, SearchService
from src.services.market_scan_research import ResearchBudget, ScanResearchCollector, meaningful
from src.services.research_execution import ResearchCallError


def hanging(*args, **kwargs):
    while True:
        time.sleep(0.05)


def financial(**kwargs):
    return pd.DataFrame([{"报告期": "2026-06-30", "净资产收益率": 18.2,
                          "营业收入同比": 10.0, "归母净利润": 123.0}])


def announcements(**kwargs):
    return pd.DataFrame([{"代码": "603259", "公告标题": "药明康德半年度报告",
                          "公告时间": datetime.now(timezone.utc).isoformat(),
                          "公告链接": "https://static.cninfo.com.cn/finalpage/fixture.pdf"}])


class Provider:
    is_available = True
    def __init__(self, name, callback):
        self.name, self.callback = name, callback
    def search(self, *args, **kwargs):
        return self.callback()


def no_news():
    return SearchResponse("fixture", [], "Fixture", success=True)


def news_error():
    return SearchResponse("fixture", [], "Fixture", success=False, error_message="provider unavailable")


@pytest.fixture
def configured(monkeypatch):
    import akshare as ak
    import src.services.alphasift_service as alpha
    service = SearchService(searxng_public_instances_enabled=False)
    service._providers = [Provider("Fixture", no_news)]
    monkeypatch.setattr(alpha, "_get_dsa_search_service", lambda: service)
    monkeypatch.setattr("src.config.get_config", lambda: SimpleNamespace(
        fundamental_cache_ttl_seconds=20, fundamental_stage_timeout_seconds=0.5,
        fundamental_fetch_timeout_seconds=0.2, fundamental_retry_max=1,
        news_max_age_days=3, enable_fundamental_pipeline=True,
    ))
    monkeypatch.setattr(ak, "stock_financial_abstract", financial)
    monkeypatch.setattr(ak, "stock_financial_analysis_indicator", financial)
    monkeypatch.setattr(ak, "stock_zh_a_disclosure_report_cninfo", announcements)
    return ak, service


CANDIDATE = {"code": "603259", "name": "药明康德", "pe": 12,
             "_research_snapshot_source": "fixture_snapshot", "_research_snapshot_fetched_at": "2026-09-30T14:00:00+08:00"}


def test_first_fundamental_source_hangs_then_fallback_succeeds(configured, monkeypatch):
    ak, _ = configured
    monkeypatch.setattr(ak, "stock_financial_abstract", hanging)
    result = ScanResearchCollector(2)(CANDIDATE)
    assert result["fundamentals"]["data"]["growth"]["data"]["roe"] == 18.2
    assert result["fundamentals"]["data"]["valuation"]["data"]["pe_ratio"] == 12
    assert any(event["error_class"] == "TimeoutError" for event in result["diagnostics"])
    assert any(event["result"] == "success" and "financial" in event["provider"] for event in result["diagnostics"])
    assert not multiprocessing.active_children()
    assert result["simulation_only"] and result["auto_order_enabled"] is False


def test_hanging_news_provider_advances_to_next_with_existing_news_filters(configured):
    _, service = configured
    service._providers = [Provider("Hung", hanging), Provider("EmptySuccess", no_news)]
    result = ScanResearchCollector(2)(CANDIDATE)
    assert result["announcements_and_news"]["status"] == "no_results"
    assert not any(error == "news:provider_error" for error in result["errors"])
    assert any(event["provider"] == "Hung" and event["error_class"] == "TimeoutError" for event in result["diagnostics"])
    assert any(event["provider"] == "EmptySuccess" and event["result"] == "success" for event in result["diagnostics"])


def test_news_zero_results_differs_from_provider_failure(configured):
    _, service = configured
    assert ScanResearchCollector(2)(CANDIDATE)["announcements_and_news"]["status"] == "no_results"
    service._providers = [Provider("Failed", news_error)]
    result = ScanResearchCollector(2)(CANDIDATE)
    assert result["announcements_and_news"]["status"] == "provider_error"
    assert "news:provider_error" in result["errors"]


@pytest.mark.parametrize("change", [
    {"代码": "000001"}, {"公告链接": "https://static.cninfo.com.cn.evil.invalid/fixture.pdf"},
    {"公告时间": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()},
    {"公告时间": "unknown"}, {"公告时间": (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()},
])
def test_announcement_identity_source_and_publication_time_are_checked(configured, monkeypatch, change):
    ak, _ = configured
    def invalid(**kwargs):
        frame = announcements()
        for key, value in change.items():
            frame[key] = value
        return frame
    monkeypatch.setattr(ak, "stock_zh_a_disclosure_report_cninfo", invalid)
    result = ScanResearchCollector(2)(CANDIDATE)
    assert result["announcements"]["items"] == []
    assert result["announcements_and_news"]["evidence_type"] != "official_announcement_index"


def test_official_index_is_preserved_without_claiming_document_review(configured):
    result = ScanResearchCollector(2)(CANDIDATE)
    assert result["announcements"]["items"][0]["source"] == "cninfo"
    assert result["announcements"]["content_reviewed"] is False
    assert result["status"] == "partial"


def test_successful_cache_retains_fetch_time_and_expires(configured):
    collector = ScanResearchCollector(2)
    collector(CANDIDATE)
    again = collector(CANDIDATE)
    assert any(event["result"] == "cache_hit" and event["fetched_at"] for event in again["diagnostics"])
    collector._cache = {key: (saved - 25, fetched, value) for key, (saved, fetched, value) in collector._cache.items()}
    after_expiry = collector(CANDIDATE)
    assert not any(event["result"] == "cache_hit" for event in after_expiry["diagnostics"])


def test_symbol_total_budget_stops_before_later_sources_and_next_symbol_continues(configured, monkeypatch):
    ak, _ = configured
    monkeypatch.setattr(ak, "stock_financial_abstract", hanging)
    monkeypatch.setattr(ak, "stock_financial_analysis_indicator", hanging)
    start = time.monotonic()
    first = ScanResearchCollector(2)({**CANDIDATE, "_research_timeout_seconds": 0.7})
    assert time.monotonic() - start < 1.2
    assert first["fundamentals"]["data"]["growth"]["status"] == "unavailable"
    assert first["announcements"]["items"] == []
    monkeypatch.setattr(ak, "stock_financial_abstract", financial)
    second = ScanResearchCollector(2)(CANDIDATE)
    assert second["fundamentals"]["data"]["growth"]["data"]["roe"] == 18.2


def test_hard_timeout_does_not_consume_shared_manager_pool(configured, monkeypatch):
    ak, _ = configured
    monkeypatch.setattr(ak, "stock_financial_abstract", hanging)
    collector = ScanResearchCollector(2)
    for _ in range(10):
        result = collector(CANDIDATE)
        assert not any("worker pool exhausted" in str(event) for event in result["diagnostics"])
        assert meaningful(result["fundamentals"]["data"]["growth"]["data"])
    assert not multiprocessing.active_children()


def test_retry_is_bounded_and_diagnostic(configured):
    events = []
    budget = ResearchBudget(0.7, "603259", "news", events, retry_max=99, request_timeout=0.2)
    def transient():
        return SearchResponse("fixture", [], "RateLimited", success=False, error_message="429 rate limited")
    with pytest.raises(ResearchCallError, match="TransientProviderError"):
        budget.call(transient, "RateLimited")
    assert [event["attempt"] for event in events] == [1, 2]


def test_metadata_and_null_fields_do_not_count_as_research():
    assert not meaningful({"valuation": {"status": "partial", "data": {"pe": None, "pb": None}}})
    assert meaningful({"data": {"roe": 0}})


def test_financial_abstract_date_columns_become_a_dated_report(configured, monkeypatch):
    ak, _ = configured
    def vertical(**kwargs):
        return pd.DataFrame({"指标": ["营业收入同比", "净资产收益率", "归母净利润"],
                             "2026-06-30": [12.3, 18.2, 123.0], "2025-12-31": [8.0, 15.0, 100.0]})
    monkeypatch.setattr(ak, "stock_financial_abstract", vertical)
    result = ScanResearchCollector(2)(CANDIDATE)
    data = result["fundamentals"]["data"]
    assert data["growth"]["data"]["roe"] == 18.2
    assert data["earnings"]["data"]["financial_report"]["report_date"] == "2026-06-30"


def test_nonempty_but_useless_financial_table_advances_to_fallback(configured, monkeypatch):
    ak, _ = configured
    def invalid(**kwargs):
        return pd.DataFrame([{"报告期": "2026-06-30", "净资产收益率": None}])
    monkeypatch.setattr(ak, "stock_financial_abstract", invalid)
    collector = ScanResearchCollector(2)
    result = collector(CANDIDATE)
    assert result["fundamentals"]["data"]["growth"]["data"]["roe"] == 18.2
    assert "fundamentals:stock_financial_abstract:InvalidPayload" in result["errors"]
    assert not any(":invalid:" in key for key in collector._cache)
    assert any(event["error_class"] == "InvalidPayload" for event in result["diagnostics"])
    assert any(event.get("fallback_result") == "recovered" for event in result["diagnostics"])


def test_hk_announcement_query_is_company_scoped_and_exact_code_checked(configured, monkeypatch):
    ak, _ = configured
    def hk(**kwargs):
        assert kwargs["symbol"] == ""
        assert kwargs["market"] == "港股" and kwargs["keyword"] == "腾讯控股"
        frame = announcements()
        frame["代码"] = "00700"
        frame["公告链接"] = "https://www.hkexnews.hk/listedco/fixture.pdf"
        return frame
    monkeypatch.setattr(ak, "stock_zh_a_disclosure_report_cninfo", hk)
    monkeypatch.setattr("data_provider.yfinance_fundamental_adapter.YfinanceFundamentalAdapter.get_fundamental_bundle",
                        lambda self, code: {"growth": {"roe": 18.2}})
    result = ScanResearchCollector(2)({"code": "HK00700", "name": "腾讯控股"})
    assert result["announcements"]["items"][0]["url"].startswith("https://www.hkexnews.hk/")
