"""Conservative parsing for symbol-scoped research fallbacks, never entry quotes."""
from __future__ import annotations

import io
import math
from itertools import islice
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlparse

import pandas as pd


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
        raise ValueError("untrusted_announcement_document")
    with requests.get(url, stream=True, timeout=(1.5, 2), allow_redirects=False) as response:
        response.raise_for_status()
        if response.status_code != 200:
            raise ValueError("announcement_redirect_or_non_document")
        data = bytearray()
        for chunk in response.iter_content(32768):
            data.extend(chunk)
            if len(data) > 2 * 1024 * 1024:
                raise ValueError("announcement_document_too_large")
        if not data.startswith(b"%PDF"):
            raise ValueError("announcement_not_pdf")
    reader = PdfReader(io.BytesIO(data))
    text = "\n".join((page.extract_text() or "")[:10000] for page in islice(reader.pages, 3))
    compact = "".join(text.split())
    identity = code[2:] if code.startswith("HK") else code
    if len(compact) < 80 or not (identity in compact or (name and name in compact)):
        raise ValueError("announcement_document_identity_or_text_missing")
    return {"excerpt": text[:2000], "document_source": url,
            "extraction": "official_pdf_first_three_pages", "document_bytes": len(data),
            "content_reviewed": False, "body_excerpt_available": True}
