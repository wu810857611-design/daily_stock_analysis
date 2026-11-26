"""Read-only Tongdaxin A-share snapshot fallback for the Linux intraday runner."""

import configparser
import importlib.util
import os
from pathlib import Path
import shutil
import tempfile

from src.services.research_execution import run_isolated


def _read_snapshots(codes, library):
    # The SDK loads the token from an INI beside its native library.
    # This copy lives outside the checkout and is removed even on worker timeout.
    os.environ["TDX_AI_DATA_LIB"] = str(library)
    from tdxaidata import tqs

    dates = tqs.get_more_info_batch(stock_list=codes, field_list=["HqDate"])
    quotes = tqs.get_market_snapshot_batch(stock_list=codes)
    if not isinstance(quotes, dict) or not isinstance(dates, dict):
        return {}
    rows = {}
    for code in codes:
        quote, dated = quotes.get(code), dates.get(code)
        if isinstance(quote, dict) and isinstance(dated, dict):
            rows[code] = {**quote, "HqDate": dated.get("HqDate")}
    return rows


def fetch_dated_snapshots(codes):
    token = os.getenv("TDX_DATA_TOKEN", "").strip()
    if not token or any(ch in token for ch in "\r\n\0"):
        raise ValueError("Invalid TDX_DATA_TOKEN")
    sdk = importlib.util.find_spec("tdxaidata")
    if sdk is None:
        raise ImportError("tdxaidata SDK unavailable")
    assets = Path(sdk.origin).parent / "lib"
    with tempfile.TemporaryDirectory(prefix="tdx-l1-") as directory:
        library = Path(directory) / "libTdxAiData.so"
        shutil.copyfile(assets / library.name, library)
        config = configparser.ConfigParser(interpolation=None)
        config.read(assets / "TdxAiData.ini", encoding="utf-8-sig")
        config["Token"]["token"] = token
        with library.with_name("TdxAiData.ini").open("w", encoding="utf-8") as handle:
            config.write(handle, space_around_delimiters=False)
        return run_isolated(lambda: _read_snapshots(codes, library), 8.0)
