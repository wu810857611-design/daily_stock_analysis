import pytest

from src.services.research_evidence import announcement_document_url, announcement_excerpt


@pytest.mark.parametrize("code,date,announcement_id", [
    ("002639", "2026-10-12", "1225599531"),
    ("HK01138", "2026-10-14 18:38:27", "1225605461"),
])
def test_real_scan_detail_links_resolve_without_http_requests(monkeypatch, code, date, announcement_id):
    from types import SimpleNamespace
    identity = code.removeprefix("HK")
    index = (f"http://www.cninfo.com.cn/new/disclosure/detail?stockCode={identity}"
             f"&announcementId={announcement_id}&orgId=fixture&announcementTime={date}")
    expected = f"https://static.cninfo.com.cn/finalpage/{date[:10]}/{announcement_id}.PDF"
    def get(url, **kwargs):
        assert url == expected  # Never request the HTTP index or HTML page.
        assert kwargs["allow_redirects"] is False and kwargs["timeout"] == (1.5, 2)
        return Response(b"%PDF fixture")
    monkeypatch.setattr("requests.get", get)
    monkeypatch.setattr("pypdf.PdfReader", lambda stream: SimpleNamespace(pages=[
        SimpleNamespace(extract_text=lambda: identity + "正式公告财务数据" * 50)
    ]))
    document = announcement_document_url(index, code, date[:10] + "T00:00:00+08:00")
    result = announcement_excerpt(document, code, "")
    assert result["document_source"] == expected
    assert result["body_excerpt_available"] and result["content_reviewed"] is False


@pytest.mark.parametrize("query", [
    "stockCode=600000&announcementId=1225599531&announcementTime=2026-10-12",
    "stockCode=002639&announcementId=../evil&announcementTime=2026-10-12",
    "stockCode=002639&announcementId=1&announcementId=2&announcementTime=2026-10-12",
    "stockCode=002639&announcementId=1&announcementTime=2026-10-11",
    "stockCode=002639&announcementId=1&announcementTime=2026-02-30",
    "stockCode=002639&announcementId=1&announcementTime=2026-10-12/../../evil",
    "stockCode=002639&announcementId=1",
])
def test_index_identity_date_and_ambiguous_parameters_fail_closed(query):
    with pytest.raises(ValueError):
        announcement_document_url("https://www.cninfo.com.cn/new/disclosure/detail?" + query,
                                  "002639", "2026-10-12T00:00:00+08:00")


@pytest.mark.parametrize("authority", ["user@www.cninfo.com.cn", "www.cninfo.com.cn:8080"])
def test_index_credentials_and_nonstandard_ports_are_not_converted(authority):
    with pytest.raises(ValueError, match="untrusted"):
        announcement_document_url(
            f"http://{authority}/new/disclosure/detail?stockCode=002639"
            "&announcementId=1&announcementTime=2026-10-12", "002639", "2026-10-12")


def test_index_publication_date_compares_in_shanghai_timezone():
    index = ("https://www.cninfo.com.cn/new/disclosure/detail?stockCode=002639"
             "&announcementId=1&announcementTime=2026-10-12 01:00:00")
    assert announcement_document_url(index, "002639", "2026-10-11T17:00:00+00:00") == (
        "https://static.cninfo.com.cn/finalpage/2026-10-12/1.PDF")


@pytest.mark.parametrize("url", ["http://static.cninfo.com.cn/a.pdf", "https://evil.invalid/a.pdf",
                                 "https://www.hkexnews.hk.evil.invalid/a.pdf", "https://user@www.hkexnews.hk/a.pdf"])
def test_untrusted_documents_are_rejected_before_request(url):
    with pytest.raises(ValueError, match="untrusted"):
        announcement_excerpt(url, "601857", "中国石油")


class Response:
    status_code = 200
    def __init__(self, data):
        self.data = data
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass
    def raise_for_status(self):
        pass
    def iter_content(self, size):
        yield self.data


def test_redirect_and_oversized_document_are_rejected(monkeypatch):
    response = Response(b"%PDF" + b"x" * (2 * 1024 * 1024))
    def get(url, **kwargs):
        assert kwargs["allow_redirects"] is False and kwargs["timeout"] == (1.5, 2)
        return response
    monkeypatch.setattr("requests.get", get)
    with pytest.raises(ValueError, match="too_large"):
        announcement_excerpt("https://static.cninfo.com.cn/a.pdf", "601857", "中国石油")
    response.status_code = 302
    with pytest.raises(ValueError, match="redirect"):
        announcement_excerpt("https://static.cninfo.com.cn/a.pdf", "601857", "中国石油")


def test_document_identity_required_and_excerpt_is_partial(monkeypatch):
    from types import SimpleNamespace
    monkeypatch.setattr("requests.get", lambda *args, **kwargs: Response(b"%PDF fixture"))
    monkeypatch.setattr("pypdf.PdfReader", lambda stream: SimpleNamespace(pages=[
        SimpleNamespace(extract_text=lambda: "中国石油601857 " + "正式公告财务数据" * 50)
    ]))
    result = announcement_excerpt("https://static.cninfo.com.cn/a.pdf", "601857", "中国石油")
    assert result["body_excerpt_available"] and result["content_reviewed"] is False
    with pytest.raises(ValueError, match="identity"):
        announcement_excerpt("https://static.cninfo.com.cn/a.pdf", "600000", "浦发银行")


@pytest.mark.parametrize("code,name,header", [
    ("HK01658", "邮储银行", "POSTAL SAV\nINGS BANK O\nF CHINA CO.\n, LTD.\n 1658\n董事會會議通告"),
    ("HK01093", "石药集团", "΅ ˾ \u0eee j 1093 \x81\nCSPC PHARMACEUTICAL GROUP LIMITED\n自願公告"),
])
def test_hk_real_pdf_headers_accept_zero_omitted_standalone_code(monkeypatch, code, name, header):
    from types import SimpleNamespace
    monkeypatch.setattr("requests.get", lambda *args, **kwargs: Response(b"%PDF fixture"))
    text = header + "\n" + "香港交易及結算所有限公司對本公告的內容概不負責。" * 6
    monkeypatch.setattr("pypdf.PdfReader", lambda stream: SimpleNamespace(pages=[
        SimpleNamespace(extract_text=lambda: text),
    ]))
    result = announcement_excerpt("https://static.cninfo.com.cn/a.pdf", code, name)
    assert result["body_excerpt_available"] and result["content_reviewed"] is False
    with pytest.raises(ValueError, match="identity"):
        announcement_excerpt("https://static.cninfo.com.cn/a.pdf", "HK09988", "阿里巴巴")


@pytest.mark.parametrize("numeric_text", [
    "截至2026年10月20日收入1093萬元", "Revenue 1093", "1093.25", "11093", "10930", "101093",
])
def test_hk_short_code_does_not_match_prose_prices_or_other_codes(monkeypatch, numeric_text):
    from types import SimpleNamespace
    monkeypatch.setattr("requests.get", lambda *args, **kwargs: Response(b"%PDF fixture"))
    text = numeric_text + "\n" + "另一家公司的正式公告正文以及財務數據。" * 8
    monkeypatch.setattr("pypdf.PdfReader", lambda stream: SimpleNamespace(pages=[
        SimpleNamespace(extract_text=lambda: text),
    ]))
    with pytest.raises(ValueError, match="identity"):
        announcement_excerpt("https://static.cninfo.com.cn/a.pdf", "HK01093", "石药集团")


@pytest.mark.parametrize("data,reason", [
    (b"<html>not a document</html>", "announcement_not_pdf"),
    (b"%PDF" + b"x" * (2 * 1024 * 1024), "announcement_document_too_large"),
], ids=["non_pdf", "oversized_pdf"])
def test_isolated_document_failure_keeps_exact_public_guard_reason(monkeypatch, data, reason):
    from src.services.research_execution import ResearchCallError, run_isolated
    monkeypatch.setattr("requests.get", lambda *args, **kwargs: Response(data))
    with pytest.raises(ResearchCallError) as caught:
        run_isolated(lambda: announcement_excerpt("https://static.cninfo.com.cn/a.pdf", "601857", "中国石油"), 1)
    assert caught.value.error_class == reason
