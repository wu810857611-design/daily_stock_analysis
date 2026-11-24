"""Issuer news used when the configured search has no direct evidence."""
from collections import Counter
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse
from xml.etree import ElementTree
from zoneinfo import ZoneInfo
import re

import requests

from src.search_service import SearchResponse, SearchResult, SearchService


def eastmoney_news(keyword):
    import akshare as ak
    frame = ak.stock_news_em(symbol=keyword)
    return [SearchResult(title=str(row.get("新闻标题") or ""), snippet=str(row.get("新闻内容") or ""),
                         url=str(row.get("新闻链接") or ""), source=str(row.get("文章来源") or "Eastmoney"),
                         published_date=str(row.get("发布时间") or "")) for row in frame.to_dict("records")]


def rss_headlines(name, code, now, days):
    first = now.date() - timedelta(days=max(1, days))
    numeric_code = code.removeprefix("HK")
    query = f'"{name}" OR "{numeric_code}" after:{first} before:{now.date() + timedelta(days=1)}'
    params = {"q": query, "hl": "zh-CN", "gl": "CN", "ceid": "CN:zh-Hans"}
    with requests.get("https://news.google.com/rss/search", params=params, stream=True,
                      timeout=(2, 3), allow_redirects=False) as response:
        response.raise_for_status()
        data = bytearray()
        for part in response.iter_content(16384):
            data.extend(part)
            if len(data) > 512 * 1024:
                raise ValueError("oversized news feed")
    if b"<!DOCTYPE" in data.upper() or b"<!ENTITY" in data.upper():
        raise ValueError("invalid news feed")
    root = ElementTree.fromstring(bytes(data).decode("utf-8-sig"))
    if root.tag != "rss" or root.find("channel") is None:
        raise ValueError("unexpected news feed")
    results = []
    for item in root.findall("./channel/item")[:100]:
        source = (item.findtext("source") or "").strip()
        title = (item.findtext("title") or "").removesuffix(f" - {source}").strip()
        link = item.findtext("link") or ""
        try:
            when = parsedate_to_datetime(item.findtext("pubDate") or "")
        except (ValueError, TypeError, OverflowError):
            when = None
        results.append(SearchResult(title=title, snippet="", url=link, source=source,
                                    published_date=when.isoformat() if when and when.tzinfo else ""))
    return results


def admit_news(items, provider, code, name, now, days):
    first_date = now.date() - timedelta(days=max(1, days) - 1)
    rejected = Counter()
    candidates = []
    seen = set()
    times = {}
    for item in items:
        parsed = urlparse(item.url)
        if parsed.scheme not in {"https", "http"} or not parsed.hostname or not item.title.strip():
            rejected["invalid_article"] += 1
            continue
        if provider == "GoogleNewsRSS":
            # The feed is a headline index. Its HTML description contains links to other stories.
            if (parsed.hostname != "news.google.com" or not parsed.path.startswith("/rss/articles/")
                    or not item.source or not any(term in item.title for term in
                    SearchService._company_identity_terms(name) if not term.isdigit())):
                rejected["no_issuer_headline"] += 1
                continue
            if (re.search(r"实盘记录|持仓日记|盈亏公开|一周大事|多股|股涨停榜|持仓曝光", item.title)
                    or (code.startswith("HK") and re.search(r"融资净|融券|大宗交易|龙虎榜|北向资金", item.title))):
                rejected["market_or_portfolio_context"] += 1
                continue
        try:
            stamp = datetime.fromisoformat(str(item.published_date).replace("Z", "+00:00"))
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
            stamp = stamp.astimezone(now.tzinfo)
        except (ValueError, TypeError):
            rejected["invalid_date"] += 1
            continue
        if stamp > now or stamp.date() < first_date:
            rejected["future" if stamp > now else "stale"] += 1
            continue
        if item.url in seen:
            rejected["duplicate"] += 1
            continue
        seen.add(item.url)
        times[item.url] = stamp.isoformat()
        candidates.append(item)
    response = SearchResponse(code, candidates, provider, success=True)
    ranked = SearchService._rank_news_response(response, stock_code=code, stock_name=name,
                                              prefer_chinese=True, max_results=len(candidates), log_scope=code)
    filtered = SearchService._filter_ranked_news_for_context(ranked, log_scope=code)
    rejected["low_quality_or_irrelevant"] += len(candidates) - len(filtered.results)
    direct = [r for r in filtered.results if r.relevance_category == SearchService._DIRECT_NEWS_CATEGORY]
    rejected["not_direct"] += len(filtered.results) - len(direct)
    rejected["result_limit"] += max(0, len(direct) - 3)
    selected = [{"title": r.title, "snippet": r.snippet[:500], "url": r.url, "source": r.source,
                 "published_date": times[r.url][:10], "published_at": times[r.url],
                 "headline_only": not bool(r.snippet)} for r in direct[:3]]
    return selected, {"received": len(items), "admitted": len(selected),
                      "rejected": {reason: count for reason, count in rejected.items() if count}}
