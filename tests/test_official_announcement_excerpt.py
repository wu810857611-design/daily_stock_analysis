import pytest

from src.services.research_evidence import announcement_excerpt


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
