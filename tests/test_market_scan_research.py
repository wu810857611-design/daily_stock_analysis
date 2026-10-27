import multiprocessing
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from src.search_service import SearchResponse, SearchResult, SearchService, SearXNGSearchProvider
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
    monkeypatch.setattr(ak, "stock_financial_abstract_ths", lambda **kwargs: pd.DataFrame())
    for name in ("stock_value_em", "stock_zh_valuation_baidu", "stock_hk_valuation_baidu", "stock_news_em"):
        monkeypatch.setattr(ak, name, lambda **kwargs: pd.DataFrame())
    monkeypatch.setattr("src.services.market_scan_research.announcement_excerpt",
                        lambda *args: {"body_excerpt_available": False})
    monkeypatch.setattr(ak, "stock_zh_a_disclosure_report_cninfo", announcements)
    monkeypatch.setattr("src.services.market_scan_research.cninfo_issuers",
                        lambda offshore: {"00700" if offshore else "603259": "fixture"})
    def index(code, org_id, now, max_age_days):
        frame = ak.stock_zh_a_disclosure_report_cninfo(
            symbol=code[2:] if code.startswith("HK") else code,
            market="港股" if code.startswith("HK") else "沪深京", keyword="",
        )
        return {"rows": [{**row, "orgId": org_id} for row in frame.to_dict("records")],
                "query": {"symbol": code, "coverage": "latest_page_sample"}}
    monkeypatch.setattr("src.services.market_scan_research.cninfo_announcement_index", index)
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


def test_hanging_news_provider_advances_to_next_with_existing_news_filters(configured, monkeypatch):
    _, service = configured
    # The half-stage reserved for SearXNG must fit timeout, process-group
    # cleanup, and a second isolated call even on a busy CI runner. Budget
    # exhaustion has its own test; production settings are unchanged.
    from src.config import get_config
    config = get_config()
    config.fundamental_stage_timeout_seconds = 3.0
    config.fundamental_fetch_timeout_seconds = 0.5
    monkeypatch.setattr("src.config.get_config", lambda: config)
    service._providers = [Provider("Hung", hanging), Provider("EmptySuccess", no_news)]
    result = ScanResearchCollector(5)(CANDIDATE)
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


def test_searxng_hanging_instance_does_not_block_next_instance(configured, monkeypatch):
    _, service = configured
    # Exercise failover, with time for the timed-out process to be reaped
    # before starting the second instance. Total-budget exhaustion is covered
    # separately; this fixture does not change production timeout settings.
    from src.config import get_config
    config = get_config()
    config.fundamental_stage_timeout_seconds = 3.0
    config.fundamental_fetch_timeout_seconds = 0.5
    monkeypatch.setattr("src.config.get_config", lambda: config)
    provider = SearXNGSearchProvider(["https://hung.invalid", "https://working.invalid"])
    def instance(query, base_url, *args, **kwargs):
        if "hung" in base_url:
            hanging()
        return no_news()
    monkeypatch.setattr(provider, "_do_search", instance)
    service._providers = [provider]
    result = ScanResearchCollector(5)(CANDIDATE)
    assert result["announcements_and_news"]["status"] == "no_results"
    assert any(e["provider"] == "SearXNG:hung.invalid" and e["error_class"] == "TimeoutError"
               for e in result["diagnostics"])
    assert any(e["provider"] == "SearXNG:working.invalid" and e.get("fallback_result") == "recovered"
               for e in result["diagnostics"])


def test_bounded_searxng_discovery_retains_actual_cache_time(configured, monkeypatch):
    _, service = configured
    # This checks cache provenance, not deadline exhaustion. Leave enough time
    # for the real fork boundary on a busy runner; production budgets are not
    # changed, and timeout/fallback behavior has separate regressions.
    from src.config import get_config
    config = get_config()
    config.fundamental_stage_timeout_seconds = 2.0
    config.fundamental_fetch_timeout_seconds = 0.5
    monkeypatch.setattr("src.config.get_config", lambda: config)
    provider = SearXNGSearchProvider(use_public_instances=True)
    saved_at = time.time() - 4000
    def discover(cls):
        cls._public_instances_cache = (saved_at, ["https://fixture.invalid"])
        cls._public_instances_stale_retry_after = time.time() + 60
        return cls._public_instances_cache[1]
    monkeypatch.setattr(SearXNGSearchProvider, "_get_public_instances", classmethod(discover))
    monkeypatch.setattr(SearXNGSearchProvider, "_public_instances_cache", None)
    monkeypatch.setattr(SearXNGSearchProvider, "_public_instances_stale_retry_after", 0)
    monkeypatch.setattr(provider, "_do_search", lambda *args, **kwargs: no_news())
    service._providers = [provider]
    result = ScanResearchCollector(2)(CANDIDATE)
    assert result["announcements_and_news"]["status"] == "no_results"
    assert SearXNGSearchProvider._public_instances_cache[0] == saved_at
    assert SearXNGSearchProvider._public_instances_stale_retry_after > time.time()


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


def test_dated_valuation_fallback_and_field_provenance(configured, monkeypatch):
    ak, _ = configured
    monkeypatch.setattr(ak, "stock_value_em", lambda **kwargs: pd.DataFrame([
        {"数据日期": (datetime.now() - timedelta(days=1)).date(), "PE(TTM)": 12.5, "市净率": 1.2},
        {"数据日期": (datetime.now() - timedelta(days=2)).date(), "PE(TTM)": 99, "市净率": 99},
    ]))
    result = ScanResearchCollector(2)({"code": "603259", "name": "药明康德"})
    block = result["fundamentals"]["data"]["valuation"]
    assert block["data"] == {"pe_ratio": 12.5, "pb_ratio": 1.2}
    assert block["field_provenance"]["pe_ratio"]["as_of"]
    assert "pe_ratio" not in result["missing_fields"]
    assert result["status"] == "partial"  # Ratios do not imply complete research.


def test_fresh_cache_age_cannot_revive_stale_valuation_data(configured, monkeypatch):
    ak, _ = configured
    monkeypatch.setattr(ak, "stock_value_em", lambda **kwargs: pd.DataFrame([
        {"数据日期": datetime.now().date(), "PE(TTM)": 12.5, "市净率": 1.2}
    ]))
    collector = ScanResearchCollector(2)
    candidate = {"code": "603259", "name": "药明康德"}
    collector(candidate)
    for key, (saved, fetched, value) in list(collector._cache.items()):
        if "stock_value_em" in key:
            collector._cache[key] = (saved, fetched, {"as_of": "2020-01-01", "data": {"pe_ratio": 999}})
    result = collector(candidate)
    assert result["fundamentals"]["data"]["valuation"]["data"]["pe_ratio"] == 12.5
    assert not any(e["result"] == "cache_hit" and "stock_value_em" in e["provider"] for e in result["diagnostics"])


def test_hk_info_timeout_does_not_discard_available_financial_statements(configured, monkeypatch):
    ticker = SimpleNamespace(get_info=hanging,
        quarterly_income_stmt=pd.DataFrame({pd.Timestamp("2026-06-30"): {"Total Revenue": 100, "Net Income": 20}}),
        quarterly_cashflow=pd.DataFrame({pd.Timestamp("2026-03-31"): {"Operating Cash Flow": 30}}))
    monkeypatch.setattr("yfinance.Ticker", lambda code: ticker)
    result = ScanResearchCollector(2)({"code": "HK00700", "name": "腾讯控股"})
    report = result["fundamentals"]["data"]["earnings"]["data"]["financial_report"]
    assert report["revenue"] == 100
    assert report["field_periods"]["revenue"] == "2026-06-30"
    assert report["field_periods"]["operating_cash_flow"] == "2026-03-31"
    assert any(e["provider"] == "yfinance:info:HK00700" and e["error_class"] == "TimeoutError"
               for e in result["diagnostics"])
    assert not multiprocessing.active_children()


def test_hk_scanner_does_not_invent_profit_from_quarterly_revenue_and_ttm_margin(monkeypatch):
    from data_provider.yfinance_fundamental_adapter import YfinanceFundamentalAdapter
    ticker = SimpleNamespace(get_info=lambda: {"profitMargins": .3, "currency": "HKD"},
        quarterly_income_stmt=pd.DataFrame({pd.Timestamp("2026-06-30"): {"Total Revenue": 100}}),
        quarterly_cashflow=pd.DataFrame())
    monkeypatch.setattr("yfinance.Ticker", lambda code: ticker)
    report = YfinanceFundamentalAdapter().get_fundamental_bundle("HK00700", financial_only=True)["earnings"]["financial_report"]
    assert report["net_profit_parent"] is None


def test_stale_valuation_advances_to_baidu_and_rejects_future_records(configured, monkeypatch):
    ak, _ = configured
    monkeypatch.setattr(ak, "stock_value_em", lambda **kwargs: pd.DataFrame([
        {"数据日期": (datetime.now() - timedelta(days=10)).date(), "PE(TTM)": 50, "市净率": 8},
        {"数据日期": (datetime.now() + timedelta(days=1)).date(), "PE(TTM)": 60, "市净率": 9},
    ]))
    monkeypatch.setattr(ak, "stock_zh_valuation_baidu", lambda **kwargs: pd.DataFrame([
        {"date": datetime.now().date(), "value": 12 if kwargs["indicator"] == "市盈率(TTM)" else 2}
    ]))
    collector = ScanResearchCollector(2)
    result = collector({"code": "603259", "name": "药明康德"})
    assert result["fundamentals"]["data"]["valuation"]["data"] == {"pe_ratio": 12, "pb_ratio": 2}
    assert any("stock_value_em:InvalidPayload" in error for error in result["errors"])
    assert not any("stock_value_em" in key for key in collector._cache)


def test_news_fallback_uses_identity_date_and_existing_admission_filters(configured, monkeypatch):
    ak, service = configured
    service._providers = [Provider("Failed", news_error)]
    def news(**kwargs):
        assert kwargs == {"symbol": "603259"}
        return pd.DataFrame([
            {"新闻标题": "药明康德发布研发业务更新", "新闻内容": "药明康德603259发布研发业务更新",
             "新闻链接": "https://finance.eastmoney.com/a/fixture.html", "文章来源": "东方财富",
             "发布时间": datetime.now().isoformat()},
            {"新闻标题": "其他公司发布业务更新", "新闻内容": "与药明康德无关",
             "新闻链接": "https://finance.eastmoney.com/a/old.html", "发布时间": "2020-01-01"},
        ])
    monkeypatch.setattr(ak, "stock_news_em", news)
    result = ScanResearchCollector(2)(CANDIDATE)
    assert result["announcements_and_news"]["status"] == "partial"
    assert len(result["announcements_and_news"]["items"]) == 1
    assert result["announcements_and_news"]["provider"] == "Eastmoney"
    assert "news:provider_error" in result["errors"]  # Do not hide the primary fault.


def test_official_body_excerpt_does_not_claim_full_review(configured, monkeypatch):
    monkeypatch.setattr("src.services.market_scan_research.announcement_excerpt", lambda *args: {
        "excerpt": "药明康德公告内容摘录", "body_excerpt_available": True, "content_reviewed": False,
    })
    result = ScanResearchCollector(2)(CANDIDATE)
    assert result["announcements"]["body_excerpt_count"] == 1
    assert result["announcements"]["content_reviewed"] is False
    assert result["announcements"]["items"][0]["fetched_at"]
    assert "announcement_body" not in result["missing_fields"]


def test_terminal_document_uses_remaining_stage_without_reserving_unused_fallback(configured, monkeypatch):
    ak, _ = configured
    from src.config import get_config
    config = get_config()
    config.enable_fundamental_pipeline = False
    config.fundamental_stage_timeout_seconds = 3.0
    config.fundamental_fetch_timeout_seconds = 2.0
    monkeypatch.setattr("src.config.get_config", lambda: config)
    def slow_index(**kwargs):
        time.sleep(1.0)
        return announcements(**kwargs)
    def slow_body(*args):
        time.sleep(1.2)
        return {"body_excerpt_available": True, "content_reviewed": False, "excerpt": "正式公告摘录"}
    monkeypatch.setattr(ak, "stock_zh_a_disclosure_report_cninfo", slow_index)
    monkeypatch.setattr("src.services.market_scan_research.announcement_excerpt", slow_body)
    result = ScanResearchCollector(5)(CANDIDATE)
    assert result["announcements"]["body_excerpt_count"] == 1
    events = [e for e in result["diagnostics"] if e["stage"] == "announcements"]
    body = events[-1]
    assert body["provider"].startswith("official_document:") and body["result"] == "success"
    assert 1.2 < body["timeout_budget"] <= 2.0
    assert sum(e["elapsed"] for e in events) < 3.0
    assert result["announcements"]["content_reviewed"] is False
    assert not multiprocessing.active_children()


def test_terminal_call_still_obeys_original_request_cap_and_kills_hung_worker():
    events = []
    budget = ResearchBudget(1.0, "603259", "announcements", events, request_timeout=0.15)
    with pytest.raises(ResearchCallError, match="TimeoutError"):
        budget.call(hanging, "official_document:fixture", reserve_for_fallback=False)
    assert events[-1]["timeout_budget"] == 0.15
    assert not multiprocessing.active_children()


def test_collector_passes_verified_index_as_https_pdf_and_keeps_both_sources(configured, monkeypatch):
    ak, _ = configured
    date = datetime.now().strftime("%Y-%m-%d")
    index = ("http://www.cninfo.com.cn/new/disclosure/detail?stockCode=603259"
             f"&announcementId=1225599531&orgId=fixture&announcementTime={date}")
    expected = f"https://static.cninfo.com.cn/finalpage/{date}/1225599531.PDF"
    monkeypatch.setattr(ak, "stock_zh_a_disclosure_report_cninfo", lambda **kwargs: pd.DataFrame([{
        "代码": "603259", "公告标题": "药明康德公告", "公告时间": date, "公告链接": index,
    }]))
    def excerpt(url, code, name):
        assert url == expected and code == "603259" and name == "药明康德"
        return {"document_source": url, "excerpt": "药明康德公告摘录",
                "body_excerpt_available": True, "content_reviewed": False}
    monkeypatch.setattr("src.services.market_scan_research.announcement_excerpt", excerpt)
    result = ScanResearchCollector(2)(CANDIDATE)
    item = result["announcements"]["items"][0]
    assert item["url"] == index and item["document_source"] == expected
    assert result["announcements"]["body_excerpt_count"] == 1
    assert result["announcements"]["content_reviewed"] is False
    assert "announcement_body" not in result["missing_fields"]
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
    assert not meaningful({"currency": "CNY", "field_sources": {"roe": "fixture"}, "units": {"roe": "%"}})
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


def test_hk_announcement_query_is_issuer_scoped_and_exact_code_checked(configured, monkeypatch):
    ak, _ = configured
    def hk(**kwargs):
        assert kwargs["symbol"] == "00700"
        assert kwargs["market"] == "港股" and kwargs["keyword"] == ""
        frame = announcements()
        frame["代码"] = "00700"
        frame["公告链接"] = "https://www.hkexnews.hk/listedco/fixture.pdf"
        return frame
    monkeypatch.setattr(ak, "stock_zh_a_disclosure_report_cninfo", hk)
    monkeypatch.setattr("data_provider.yfinance_fundamental_adapter.YfinanceFundamentalAdapter.get_fundamental_bundle",
                        lambda self, code, **kwargs: {"growth": {"roe": 18.2}})
    result = ScanResearchCollector(2)({"code": "HK00700", "name": "腾讯控股"})
    assert result["announcements"]["items"][0]["url"].startswith("https://www.hkexnews.hk/")


def test_issuer_map_is_cached_in_parent_across_isolated_candidates(configured):
    collector = ScanResearchCollector(2)
    collector(CANDIDATE)
    again = collector(CANDIDATE)
    assert any(e["provider"] == "cninfo:issuers:szse" and e["result"] == "cache_hit"
               for e in again["diagnostics"])


def test_missing_issuer_never_falls_back_to_an_unscoped_company_search(configured, monkeypatch):
    monkeypatch.setattr("src.services.market_scan_research.cninfo_issuers", lambda offshore: {})
    def forbidden(*args):
        raise AssertionError("An unknown issuer must not query announcements")
    monkeypatch.setattr("src.services.market_scan_research.cninfo_announcement_index", forbidden)
    result = ScanResearchCollector(2)(CANDIDATE)
    assert "announcements:announcement_issuer_not_found" in result["errors"]
    assert result["announcements"]["status"] == "unavailable"


def test_rejected_index_row_keeps_identity_and_original_fields(configured, monkeypatch):
    monkeypatch.setattr("src.services.market_scan_research.cninfo_announcement_index", lambda *args: {
        "rows": [{**announcements().to_dict("records")[0], "代码": "603259", "orgId": "wrong"}],
        "query": {"coverage": "latest_page_sample"},
    })
    result = ScanResearchCollector(2)(CANDIDATE)
    block = result["announcements"]
    assert block["status"] == "invalid_payload" and not block["items"]
    assert block["rejected_rows"][0]["reason"] == "identity_mismatch"
    assert block["rejected_rows"][0]["title"] == "药明康德半年度报告"


def test_rejected_pdf_diagnostics_reach_candidate_artifact(configured, monkeypatch):
    from src.services.research_evidence import AnnouncementEvidenceError
    evidence = {"document_source": "https://static.cninfo.com.cn/finalpage/fixture.pdf",
                "document_sha256": "a" * 64, "document_bytes": 123,
                "extracted_characters": 100, "header_excerpt": "另一家公司的正文"}
    def rejected(*args):
        raise AnnouncementEvidenceError("announcement_document_identity_mismatch", evidence)
    monkeypatch.setattr("src.services.market_scan_research.announcement_excerpt", rejected)
    result = ScanResearchCollector(2)(CANDIDATE)
    item = result["announcements"]["items"][0]
    assert item["body_status"] == "announcement_document_identity_mismatch"
    assert item["body_diagnostics"] == evidence
    assert "announcement_body" in result["missing_fields"]
    assert result["status"] == "partial"
    assert any(e.get("evidence_diagnostics") == evidence for e in result["diagnostics"])


def test_actual_https_index_transport_integrates_with_parent_cache(configured, monkeypatch, tmp_path):
    from contextlib import nullcontext
    import json
    from src.services import research_evidence
    calls = tmp_path / "http_calls.jsonl"
    def response(payload):
        return nullcontext(SimpleNamespace(status_code=200, raise_for_status=lambda: None,
            iter_content=lambda size: [json.dumps(payload).encode()]))
    def record(kind, value):
        with calls.open("a") as handle:
            handle.write(json.dumps([kind, value]) + "\n")
    def get(url, **kwargs):
        record("map", url)
        return response({"stockList": [{"code": code, "orgId": "fixture"} for code in ("603259", "300473")]})
    def post(url, **kwargs):
        record("index", kwargs["data"]["stock"])
        return response({"totalAnnouncement": 1, "announcements": [{
            "secCode": kwargs["data"]["stock"].split(",")[0], "orgId": "fixture",
            "announcementId": "123456", "announcementTitle": "正式公告",
            "announcementTime": int((datetime.now(timezone.utc) - timedelta(hours=1)).timestamp() * 1000),
        }]})
    monkeypatch.setattr("requests.get", get)
    monkeypatch.setattr("requests.post", post)
    monkeypatch.setattr("src.services.market_scan_research.cninfo_issuers", research_evidence.cninfo_issuers)
    monkeypatch.setattr("src.services.market_scan_research.cninfo_announcement_index", research_evidence.cninfo_announcement_index)
    collector = ScanResearchCollector(2)
    for code in ("603259", "300473"):
        result = collector({**CANDIDATE, "code": code})
        assert result["announcements"]["items"]
        assert result["announcements"]["query"]["symbol"] == code
        assert result["announcements"]["content_reviewed"] is False
    recorded = [json.loads(line) for line in calls.read_text().splitlines()]
    assert recorded == [["map", "https://www.cninfo.com.cn/new/data/szse_stock.json"],
                        ["index", "603259,fixture"], ["index", "300473,fixture"]]
