import json
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from src.services.research_evidence import cninfo_announcement_index, cninfo_issuers


class Response:
    status_code = 200

    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def raise_for_status(self):
        pass

    def iter_content(self, size):
        yield json.dumps(self.payload).encode()


NOW = datetime(2026, 10, 27, 19, 0, tzinfo=ZoneInfo("Asia/Shanghai"))


@pytest.mark.parametrize("offshore,code,market", [(False, "300473", "szse"), (True, "02331", "hke")])
def test_official_issuer_maps_use_https_and_validate_exact_identity(monkeypatch, offshore, code, market):
    calls = []

    def get(url, **kwargs):
        calls.append(url)
        assert url == f"https://www.cninfo.com.cn/new/data/{market}_stock.json"
        assert kwargs["allow_redirects"] is False and kwargs["timeout"] == (1.5, 2)
        return Response({"stockList": [{"code": code, "orgId": "issuer"},
                                        {"code": code, "orgId": "invalid;query"}]})

    monkeypatch.setattr("requests.get", get)
    assert cninfo_issuers(offshore) == {code: "issuer"}
    assert len(calls) == 1


@pytest.mark.parametrize("code,column", [("300473", "szse"), ("HK02331", "hke")])
def test_index_requests_first_page_once_with_exact_issuer_and_retains_truncation(monkeypatch, code, column):
    calls = []
    identity = code.removeprefix("HK")
    published = datetime(2026, 10, 26, 16, 48, 30, tzinfo=NOW.tzinfo)

    def post(url, **kwargs):
        calls.append(kwargs["data"])
        assert url == "https://www.cninfo.com.cn/new/hisAnnouncement/query"
        assert kwargs["timeout"] == (1.5, 2) and kwargs["allow_redirects"] is False
        assert kwargs["data"]["stock"] == f"{identity},issuer"
        assert kwargs["data"]["searchkey"] == "" and kwargs["data"]["column"] == column
        assert kwargs["data"]["pageNum"] == "1" and kwargs["data"]["pageSize"] == "30"
        return Response({"totalAnnouncement": 31, "hasMore": True, "announcements": [{
            "secCode": identity, "orgId": "issuer", "announcementId": "1225623347",
            "announcementTitle": "董事会会议通告", "announcementTime": int(published.timestamp() * 1000),
        }]})

    monkeypatch.setattr("requests.post", post)
    result = cninfo_announcement_index(code, "issuer", NOW, 3)
    assert len(calls) == 1  # No preflight duplicate, no traversal of the remaining pages.
    assert result["query"]["has_more"] and result["query"]["total_announcements"] == 31
    assert result["query"]["coverage"] == "latest_page_sample"
    assert result["rows"][0]["公告时间"] == published.isoformat()
    assert f"stockCode={identity}" in result["rows"][0]["公告链接"]


@pytest.mark.parametrize("payload", [
    {}, {"totalAnnouncement": 5, "announcements": None},
    {"totalAnnouncement": False, "announcements": []},
    {"totalAnnouncement": 1, "announcements": "blocked"},
    {"totalAnnouncement": -1, "announcements": []},
])
def test_invalid_upstream_payload_is_not_reported_as_no_announcements(monkeypatch, payload):
    monkeypatch.setattr("requests.post", lambda *args, **kwargs: Response(payload))
    with pytest.raises(ValueError, match="invalid_payload"):
        cninfo_announcement_index("HK02331", "issuer", NOW, 3)


def test_explicit_empty_index_differs_from_invalid_payload(monkeypatch):
    monkeypatch.setattr("requests.post", lambda *args, **kwargs: Response({
        "totalAnnouncement": 0, "announcements": None,
    }))
    result = cninfo_announcement_index("HK02331", "issuer", NOW, 3)
    assert result["rows"] == [] and result["query"]["total_announcements"] == 0


def test_ambiguous_issuer_map_fails_closed(monkeypatch):
    monkeypatch.setattr("requests.get", lambda *args, **kwargs: Response({"stockList": [
        {"code": "02331", "orgId": "issuer-one"}, {"code": "02331", "orgId": "issuer-two"},
    ]}))
    with pytest.raises(ValueError, match="identity_ambiguous"):
        cninfo_issuers(True)


def test_index_redirect_is_rejected_without_following(monkeypatch):
    response = Response({"totalAnnouncement": 0, "announcements": []})
    response.status_code = 302
    monkeypatch.setattr("requests.post", lambda *args, **kwargs: response)
    with pytest.raises(ValueError, match="redirect"):
        cninfo_announcement_index("HK02331", "issuer", NOW, 3)
