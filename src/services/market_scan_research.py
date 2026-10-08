"""Evidence enrichment for scan finalists; no trading or model decisions.

Reuse the existing adapters and news admission filters, while each external
endpoint gets an independent killable boundary and a shared remaining budget.
"""
from __future__ import annotations

import copy
import json
import math
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from src.services.research_execution import ResearchCallError, run_isolated
from src.services.research_evidence import announcement_excerpt, recent_valuation


def meaningful(value: Any) -> bool:
    """Metadata and containers containing only nulls are not research evidence."""
    if isinstance(value, Mapping):
        return any(meaningful(v) for k, v in value.items() if k not in {
            "status", "source", "provider", "fetched_at", "as_of", "report_date",
            "published_at", "error", "errors", "coverage", "market", "source_chain", "cache_age_seconds",
            "currency", "field_sources", "field_periods", "period_basis", "units", "provenance", "field_provenance", "missing_fields",
        })
    if isinstance(value, (list, tuple)):
        return any(meaningful(v) for v in value)
    if isinstance(value, float):
        return math.isfinite(value)
    return value is not None and value != "" and not isinstance(value, bool)


class ResearchBudget:
    def __init__(self, seconds: float, symbol: str, stage: str, events: list[dict],
                 retry_max: int = 2, request_timeout: float = 4.0) -> None:
        self.deadline = time.monotonic() + max(0.0, seconds)
        self.symbol, self.stage, self.events = symbol, stage, events
        self.retry_max = min(2, max(1, retry_max))
        self.request_timeout = max(0.001, request_timeout)

    def call(self, callback: Callable[[], Any], provider: str) -> Any:
        last_error = ResearchCallError("BudgetExhausted")
        for attempt in range(1, self.retry_max + 1):
            remaining = self.deadline - time.monotonic()
            # Reserve half the remaining stage for subsequent fallback sources.
            budget = min(self.request_timeout, max(0.0, remaining / 2))
            if budget < 0.05:
                raise ResearchCallError("BudgetExhausted", self.stage)
            started = time.monotonic()
            event = {"stage": self.stage, "symbol": self.symbol, "provider": provider,
                     "started_at": datetime.now(timezone.utc).isoformat(), "attempt": attempt,
                     "timeout_budget": round(budget, 3), "elapsed": 0.0,
                     "result": "started", "error_class": ""}
            print(json.dumps({"event": "research_provider", **event}), flush=True)
            try:
                result = run_isolated(callback, budget)
                if getattr(result, "success", True) is False:
                    detail = str(getattr(result, "error_message", "") or "")
                    if any(term in detail.lower() for term in ("429", "rate limit", "限流", "503", "502")):
                        raise ResearchCallError("TransientProviderError", "retryable provider response")
                    raise ResearchCallError("ProviderResponseError", "provider returned failure")
                event.update(result="success", elapsed=round(time.monotonic() - started, 3),
                             fetched_at=datetime.now(timezone.utc).isoformat(),
                             fallback_result="recovered" if any(
                                 item.get("stage") == self.stage and item.get("result") == "failed"
                                 for item in self.events
                             ) else "primary_success")
                self.events.append(event)
                print(json.dumps({"event": "research_provider", **event}), flush=True)
                return result
            except ResearchCallError as exc:
                last_error = exc
                event.update(result="failed", error_class=exc.error_class,
                             elapsed=round(time.monotonic() - started, 3))
                self.events.append(event)
                print(json.dumps({"event": "research_provider", **event}), flush=True)
                if exc.error_class not in {"TimeoutError", "ConnectionError", "ConnectTimeout",
                                           "ReadTimeout", "RemoteDisconnected", "TransientProviderError"}:
                    break
                # Do not spend the entire source-stage budget retrying one
                # timeout. It already used its allocation; advance fallback.
                if exc.error_class == "TimeoutError":
                    break
                delay = min(0.1 * attempt, max(0.0, self.deadline - time.monotonic()))
                time.sleep(delay)
        raise last_error


class ScanResearchCollector:
    def __init__(self, timeout_seconds: float = 30.0) -> None:
        self.timeout_seconds = timeout_seconds
        self._cache: dict[str, tuple[float, str, Any]] = {}

    def __call__(self, candidate: Mapping[str, Any]) -> dict:
        from src.config import get_config
        config = get_config()
        code = str(candidate.get("code") or "")
        now = datetime.now(ZoneInfo("Asia/Shanghai"))
        total_budget = min(self.timeout_seconds, float(candidate.get("_research_timeout_seconds", self.timeout_seconds)))
        deadline = time.monotonic() + max(0.0, total_budget - 0.5)
        events: list[dict] = []
        errors: list[str] = []
        ttl = max(0, config.fundamental_cache_ttl_seconds)
        stage_limit = max(0.0, config.fundamental_stage_timeout_seconds)

        def stage(name: str) -> ResearchBudget:
            return ResearchBudget(min(stage_limit, max(0.0, deadline - time.monotonic())),
                                  code, name, events, config.fundamental_retry_max,
                                  min(config.fundamental_fetch_timeout_seconds, stage_limit / 2))

        def cached_call(budget: ResearchBudget, callback: Callable[[], Any], key: str,
                        cache_validator: Callable[[Any], bool] | None = None) -> Any:
            cached = self._cache.get(key)
            if cached and ttl > 0 and 0 <= time.monotonic() - cached[0] < ttl and (
                cache_validator is None or cache_validator(cached[2])
            ):
                events.append({"stage": budget.stage, "symbol": code, "provider": key,
                               "result": "cache_hit", "fetched_at": cached[1],
                               "cache_age_seconds": round(time.monotonic() - cached[0], 3)})
                return copy.deepcopy(cached[2])
            def checked_callback():
                result = callback()
                if cache_validator is not None and not cache_validator(result):
                    raise ResearchCallError("InvalidPayload", "no valid symbol financial fields")
                return result
            result = budget.call(checked_callback, key)
            cacheable = not result.empty if hasattr(result, "empty") else meaningful(result)
            if cacheable and cache_validator is not None:
                cacheable = cache_validator(result)
            if ttl > 0 and cacheable:
                self._cache[key] = (time.monotonic(), datetime.now(timezone.utc).isoformat(), copy.deepcopy(result))
            return result

        # Already acquired snapshot fields are research valuation evidence,
        # never a substitute for a fresh entry quote. No full-market re-fetch.
        valuation = {key: candidate.get(source) for key, source in
                     (("pe_ratio", "pe"), ("pb_ratio", "pb"), ("total_mv", "total_mv"))}
        valuation = {key: value for key, value in valuation.items()
                     if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)}
        fundamental = {"valuation": {"status": "partial" if valuation else "unavailable", "data": valuation,
                                     "source": candidate.get("_research_snapshot_source", ""),
                                     "fetched_at": candidate.get("_research_snapshot_fetched_at", "")}}
        budget = stage("fundamentals")
        try:
            if not config.enable_fundamental_pipeline:
                raise ResearchCallError("NotConfigured", "fundamental pipeline disabled")
            if code.startswith("HK"):
                from data_provider.yfinance_fundamental_adapter import YfinanceFundamentalAdapter
                bundle = YfinanceFundamentalAdapter().get_fundamental_bundle(
                    code, financial_only=True, call_runner=lambda callback, endpoint:
                    cached_call(budget, callback, f"yfinance:{endpoint}:{code}")
                )
            else:
                from data_provider.fundamental_adapter import AkshareFundamentalAdapter
                def adapter_call(fn: Callable[..., Any], kwargs: dict, validator=None) -> Any:
                    key = f"akshare:{fn.__name__}:{json.dumps(kwargs, sort_keys=True)}"
                    return cached_call(budget, lambda: fn(**kwargs), key, cache_validator=validator)
                bundle = AkshareFundamentalAdapter(call_runner=adapter_call).get_fundamental_bundle(code, financial_only=True)
            for block in ("growth", "earnings", "institution"):
                data = bundle.get(block) or {}
                provenance = next((item for item in reversed(events)
                                   if item.get("stage") == "fundamentals"
                                   and item.get("result") in {"success", "cache_hit"}), {})
                fundamental[block] = {"status": "partial" if meaningful(data) else "unavailable", "data": data,
                                      "source_chain": bundle.get("source_chain") or [],
                                      "fetched_at": provenance.get("fetched_at", ""),
                                      "cache_age_seconds": provenance.get("cache_age_seconds", 0)}
            errors.extend(f"fundamentals:{item}" for item in bundle.get("errors", []))
        except Exception as exc:
            errors.append(f"fundamentals:{getattr(exc, 'error_class', type(exc).__name__)}")
        for block in ("growth", "earnings", "institution"):
            fundamental.setdefault(block, {"status": "unavailable", "data": {}})
        # Per-symbol public valuation sources cover missing snapshot ratios.
        # They retain the provider's trading date and never become entry quotes.
        valuation_block = fundamental["valuation"]
        valuation_block["field_provenance"] = {
            field: {"source": valuation_block["source"], "fetched_at": valuation_block["fetched_at"],
                    "basis": "scan_snapshot"} for field in valuation
        }
        if config.enable_fundamental_pipeline and any(field not in valuation for field in ("pe_ratio", "pb_ratio")):
            budget = stage("fundamental_valuation")
            def valuation_call(fn_name: str, kwargs: dict, fields: dict) -> None:
                import akshare as ak
                key = f"akshare:{fn_name}:{json.dumps(kwargs, sort_keys=True)}"
                def fetch():
                    return recent_valuation(getattr(ak, fn_name)(**kwargs), now, config.news_max_age_days, fields)
                def valid(item):
                    try:
                        date = datetime.fromisoformat(item["as_of"]).date()
                        return bool(item.get("data")) and (now - timedelta(days=config.news_max_age_days)).date() <= date <= now.date()
                    except (KeyError, TypeError, ValueError):
                        return False
                try:
                    parsed = cached_call(budget, fetch, key, cache_validator=valid)
                    provenance = next((e for e in reversed(events) if e.get("provider") == key), {})
                    for field, value in parsed["data"].items():
                        if field not in valuation:
                            valuation[field] = value
                            valuation_block["field_provenance"][field] = {
                                "source": key, "as_of": parsed["as_of"], "basis": "dated_research_valuation",
                                "fetched_at": provenance.get("fetched_at", ""),
                                "cache_age_seconds": provenance.get("cache_age_seconds", 0),
                            }
                except Exception as exc:
                    errors.append(f"valuation:{fn_name}:{getattr(exc, 'error_class', type(exc).__name__)}")
            if not code.startswith("HK"):
                valuation_call("stock_value_em", {"symbol": code}, {"pe_ratio": "PE(TTM)", "pb_ratio": "市净率"})
            for field, indicator in (("pe_ratio", "市盈率(TTM)"), ("pb_ratio", "市净率")):
                if field not in valuation:
                    valuation_call("stock_hk_valuation_baidu" if code.startswith("HK") else "stock_zh_valuation_baidu",
                                   {"symbol": code[2:] if code.startswith("HK") else code,
                                    "indicator": indicator, "period": "近一年"}, {field: "value"})
        valuation_block.update(data=valuation, status="partial" if valuation else "unavailable",
                               units={"pe_ratio": "multiple", "pb_ratio": "multiple"})
        fundamental["coverage"] = {key: value["status"] for key, value in fundamental.items()
                                    if isinstance(value, Mapping) and "status" in value}
        fundamentals = {"status": "partial" if any(meaningful(v.get("data")) for v in fundamental.values()
                                                     if isinstance(v, Mapping)) else "unavailable", "data": fundamental}

        announcements = {"status": "unavailable", "evidence_type": "official_announcement_index",
                         "content_reviewed": False, "items": []}
        budget = stage("announcements")
        try:
            def fetch_announcements():
                import akshare as ak
                # HK has no stock-id map in AkShare: use a company-name query
                # then verify the exact code below, never an unfiltered query.
                offshore = code.startswith("HK")
                if offshore and not candidate.get("name"):
                    raise ResearchCallError("NotSupported", "HK announcement query requires company name")
                return ak.stock_zh_a_disclosure_report_cninfo(
                    symbol="" if offshore else code, market="港股" if offshore else "沪深京",
                    keyword=str(candidate.get("name") or "") if offshore else "",
                    start_date=(now - timedelta(days=config.news_max_age_days)).strftime("%Y%m%d"),
                    end_date=now.strftime("%Y%m%d"),
                )
            announcement_key = f"cninfo:announcements:{code}:{now.date()}"
            frame = cached_call(budget, fetch_announcements, announcement_key)
            items = []
            invalid_rows = 0
            for row in frame.to_dict("records"):
                url = str(row.get("公告链接") or "")
                host = (urlparse(url).hostname or "").lower()
                date = str(row.get("公告时间") or "")
                try:
                    published = datetime.fromisoformat(date.replace("Z", "+00:00")) if date else None
                except ValueError:
                    published = None
                if published is not None and published.tzinfo is None:
                    published = published.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
                identity = str(row.get("代码") or "").strip().zfill(5 if code.startswith("HK") else 6)
                target = code[2:] if code.startswith("HK") else code
                if identity != target:
                    continue
                if (not row.get("公告标题")
                        or host not in {"www.cninfo.com.cn", "static.cninfo.com.cn", "www.hkexnews.hk"}
                        or urlparse(url).scheme not in {"http", "https"} or published is None
                        or not now - timedelta(days=config.news_max_age_days) <= published <= datetime.now(now.tzinfo)):
                    invalid_rows += 1
                    continue
                items.append({"title": str(row["公告标题"]), "url": url,
                              "published_date": published.isoformat(), "source": "cninfo"})
            announcements.update(status="partial" if items else "no_results", items=items[:3])
            # At most one document per candidate; preserve an index if its
            # body is inaccessible, and never claim an excerpt is full review.
            for item in announcements["items"][:1]:
                try:
                    document = cached_call(budget, lambda: announcement_excerpt(item["url"], code,
                                             str(candidate.get("name") or "")), f"official_document:{item['url']}:{code}")
                    item.update(document)
                    item["fetched_at"] = next((e.get("fetched_at", "") for e in reversed(events)
                                                if e.get("provider") == f"official_document:{item['url']}:{code}"), "")
                except Exception as exc:
                    item["body_status"] = getattr(exc, "error_class", type(exc).__name__)
                    errors.append(f"announcement_body:{item['body_status']}")
            announcements["body_excerpt_count"] = sum(bool(item.get("body_excerpt_available")) for item in announcements["items"])
            if not items:
                self._cache.pop(announcement_key, None)
            if invalid_rows and not items:
                announcements["status"] = "invalid_payload"
                errors.append("announcements:invalid_identity_source_or_date")
        except Exception as exc:
            errors.append(f"announcements:{getattr(exc, 'error_class', type(exc).__name__)}")

        news = {"status": "unavailable", "evidence_type": "news_search_not_verified_exchange_announcements",
                "items": []}
        budget = stage("news")
        try:
            from src.services.alphasift_service import _get_dsa_search_service
            service = _get_dsa_search_service()
            if not service.is_available:
                raise ResearchCallError("NotConfigured", "no available news provider")
            search_budget = ResearchBudget(min(stage_limit / 2, max(0, budget.deadline - time.monotonic()) / 2),
                                           code, "news", events, config.fundamental_retry_max,
                                           config.fundamental_fetch_timeout_seconds)
            response = service.search_stock_news(
                code, str(candidate.get("name") or code), max_results=3,
                request_runner=lambda callback, provider: search_budget.call(callback, provider),
            )
            items = [{"title": item.title, "snippet": item.snippet[:500], "url": item.url,
                      "source": item.source, "published_date": item.published_date}
                     for item in response.results[:3]]
            news.update(status="partial" if response.success and items else
                        "no_results" if response.success else "provider_error",
                        provider=response.provider, items=items if response.success else [])
            if not response.success:
                errors.append("news:provider_error")
        except Exception as exc:
            errors.append(f"news:{getattr(exc, 'error_class', type(exc).__name__)}")

        if not news["items"] and not code.startswith("HK"):
            try:
                import akshare as ak
                from src.search_service import SearchResponse, SearchResult, SearchService
                frame = cached_call(budget, lambda: ak.stock_news_em(symbol=code), f"eastmoney:news:{code}")
                response = SearchResponse(code, [SearchResult(
                    title=str(row.get("新闻标题") or ""), snippet=str(row.get("新闻内容") or ""),
                    url=str(row.get("新闻链接") or ""), source=str(row.get("文章来源") or "Eastmoney"),
                    published_date=str(row.get("发布时间") or ""),
                ) for row in frame.to_dict("records") if urlparse(str(row.get("新闻链接") or "")).scheme in {"http", "https"}], "Eastmoney", success=True)
                filters = SearchService(searxng_public_instances_enabled=False)
                response = filters._filter_news_response(response, search_days=config.news_max_age_days,
                                                          max_results=10, log_scope=f"{code}:fallback")
                response = filters._rank_news_response(response, stock_code=code,
                          stock_name=str(candidate.get("name") or code), prefer_chinese=True,
                          max_results=10, log_scope=f"{code}:fallback")
                response = filters._filter_ranked_news_for_context(response, log_scope=f"{code}:fallback")
                # Company-specific evidence only; sector matches are not
                # counted as a company's research coverage.
                admitted = [item for item in response.results if item.relevance_category == filters._DIRECT_NEWS_CATEGORY
                            and item.published_date <= now.date().isoformat()][:3]
                if admitted:
                    news.update(status="partial", provider="Eastmoney", items=[{
                        "title": item.title, "snippet": item.snippet[:500], "url": item.url,
                        "source": item.source, "published_date": item.published_date,
                    } for item in admitted])
                news["fallback_status"] = "partial" if admitted else "no_results"
            except Exception as exc:
                news["fallback_status"] = "provider_error"
                errors.append(f"news_fallback:{getattr(exc, 'error_class', type(exc).__name__)}")

        report = fundamental["earnings"]["data"].get("financial_report") or {}
        missing = [f"valuation.{field}" for field in ("pe_ratio", "pb_ratio") if field not in valuation]
        missing += [f"financial_report.{field}" for field in ("report_date", "revenue", "net_profit_parent", "operating_cash_flow", "roe")
                if report.get(field) is None]
        missing += [f"growth.{field}" for field in ("revenue_yoy", "net_profit_yoy")
                    if fundamental["growth"]["data"].get(field) is None]
        if not news["items"]:
            missing.append("news")
        if not any(item.get("body_excerpt_available") for item in announcements["items"]):
            missing.append("announcement_body")

        has_data = meaningful(fundamentals.get("data")) or bool(news["items"] or announcements["items"])
        return {"attempted": True, "status": "partial" if has_data else "unavailable",
                "fetched_at": datetime.now(timezone.utc).isoformat(), "fundamentals": fundamentals,
                "announcements_and_news": news, "announcements": announcements,
                "diagnostics": events, "errors": errors,
                "missing_fields": missing,
                "simulation_only": True, "auto_order_enabled": False}
