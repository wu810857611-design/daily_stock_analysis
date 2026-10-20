"""Conservative parsing for symbol-scoped research fallbacks, never entry quotes."""
from __future__ import annotations

import io
import math
import re
from itertools import islice
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import pandas as pd


class AnnouncementEvidenceError(ValueError):
    """Stable, public guard reason retained across the isolated worker."""

    def __init__(self, reason: str) -> None:
        self.error_class = reason
        super().__init__(reason)


def recent_valuation(frame: Any, now: datetime, max_age_days: int, fields: dict[str, str]) -> dict:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return {}
    date_col = next((key for key in ("数据日期", "date") if key in frame.columns), None)
    if not date_col:
        return {}
    dates = pd.to_datetime(frame[date_col], errors="coerce", utc=True)
    # These endpoints publish trading dates rather than intraday timestamps.
    valid = dates.notna() & (dates.dt.date <= now.date()) & (
        dates.dt.date >= (now - timedelta(days=max_age_days)).date()
    )
    if not valid.any():
        return {}
    latest = dates[valid].idxmax()
    row = frame.loc[latest]
    values = {}
    for field, column in fields.items():
        try:
            value = float(row[column])
            if math.isfinite(value):
                values[field] = value
        except (KeyError, TypeError, ValueError):
            pass
    return {"data": values, "as_of": dates.loc[latest].date().isoformat()} if values else {}


def announcement_document_url(url: str, code: str, published_date: str) -> str:
    """Map a verified CNInfo index to its HTTPS document without fetching HTML.

    AkShare returns HTTP detail links, not PDFs. Only the exact official detail
    route is converted; its symbol and publication date must match the admitted
    index row. A missing document still fails closed in the existing PDF reader.
    """
    parsed = urlparse(url)
    if parsed.hostname != "www.cninfo.com.cn" or parsed.path != "/new/disclosure/detail":
        return url
    if (parsed.scheme not in {"http", "https"} or parsed.username or parsed.password
            or parsed.port not in (None, 80 if parsed.scheme == "http" else 443)
            or parsed.fragment):
        raise AnnouncementEvidenceError("untrusted_announcement_index")
    query = parse_qs(parsed.query, keep_blank_values=True)
    if any(len(query.get(key, [])) != 1 for key in ("stockCode", "announcementId", "announcementTime")):
        raise AnnouncementEvidenceError("invalid_announcement_index_parameters")
    identity = code[2:] if code.startswith("HK") else code
    announcement_id = query["announcementId"][0]
    date_text = query["announcementTime"][0]
    if query["stockCode"][0] != identity or not re.fullmatch(r"[0-9]{1,20}", announcement_id):
        raise AnnouncementEvidenceError("announcement_index_identity_mismatch")
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}(?: [0-9]{2}:[0-9]{2}:[0-9]{2})?", date_text):
        raise AnnouncementEvidenceError("invalid_announcement_index_date")
    try:
        document_date = datetime.fromisoformat(date_text).date()
        published = datetime.fromisoformat(published_date)
    except ValueError:
        raise AnnouncementEvidenceError("invalid_announcement_index_date") from None
    if published.tzinfo is not None:
        published = published.astimezone(ZoneInfo("Asia/Shanghai"))
    if document_date != published.date():
        raise AnnouncementEvidenceError("announcement_index_date_mismatch")
    return f"https://static.cninfo.com.cn/finalpage/{document_date.isoformat()}/{announcement_id}.PDF"


def announcement_excerpt(url: str, code: str, name: str) -> dict:
    """Read a bounded excerpt only from a validated official PDF URL.

    Caller supplies a killable deadline. No redirects, unlimited reads, or
    scanned-document guesses; an excerpt is explicitly not a full review.
    """
    import requests
    from pypdf import PdfReader

    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname not in {
        "static.cninfo.com.cn", "www.cninfo.com.cn", "www.hkexnews.hk"
    } or parsed.username or parsed.password or parsed.port not in (None, 443):
        raise AnnouncementEvidenceError("untrusted_announcement_document")
    with requests.get(url, stream=True, timeout=(1.5, 2), allow_redirects=False) as response:
        response.raise_for_status()
        if response.status_code != 200:
            raise AnnouncementEvidenceError("announcement_redirect_or_non_document")
        data = bytearray()
        for chunk in response.iter_content(32768):
            data.extend(chunk)
            if len(data) > 2 * 1024 * 1024:
                raise AnnouncementEvidenceError("announcement_document_too_large")
        if not data.startswith(b"%PDF"):
            raise AnnouncementEvidenceError("announcement_not_pdf")
    reader = PdfReader(io.BytesIO(data))
    text = "\n".join((page.extract_text() or "")[:10000] for page in islice(reader.pages, 3))
    compact = "".join(text.split())
    identity = code[2:] if code.startswith("HK") else code
    identity_present = bool(re.search(
        rf"(?<![A-Za-z0-9]){re.escape(identity)}(?![A-Za-z0-9])", compact
    )) or bool(name and name in compact)
    if code.startswith("HK") and re.fullmatch(r"[0-9]{5}", identity):
        # HK issuer headers commonly omit leading zeroes. PDF embedded fonts
        # can corrupt the surrounding label, so admit a short code-only header
        # line, never an arbitrary number inside prose, a price or a date.
        short_code = str(int(identity))
        header_code = re.compile(
            rf"[^0-9\u4e00-\u9fff]{{0,24}}(?<![A-Za-z0-9])0*{short_code}"
            rf"(?![A-Za-z0-9])[^0-9A-Za-z\u4e00-\u9fff]{{0,8}}"
        )
        identity_present = identity_present or any(
            header_code.fullmatch(line.strip()) for line in text[:1600].splitlines()
            if not re.search(r"[A-Za-z]{2,}", line)
        )
    if len(compact) < 80 or not identity_present:
        raise AnnouncementEvidenceError("announcement_document_identity_or_text_missing")
    return {"excerpt": text[:2000], "document_source": url,
            "extraction": "official_pdf_first_three_pages", "document_bytes": len(data),
            "content_reviewed": False, "body_excerpt_available": True}
