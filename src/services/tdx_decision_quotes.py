"""Read-only Tongdaxin snapshot fallback for the Linux intraday runner."""

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

    rows = {}
    # The vendor rejects batches that mix mainland and extended markets.
    for hong_kong in (False, True):
        market_codes = [code for code in codes if code.endswith(".HK") == hong_kong]
        for start in range(0, len(market_codes), 32):
            batch = market_codes[start:start + 32]
            dates = tqs.get_more_info_batch(stock_list=batch, field_list=["HqDate"])
            quotes = tqs.get_market_snapshot_batch(
                stock_list=batch, field_list=["Now", "LastClose", "Volume", "RefreshTime"],
            )
            metadata = tqs.get_stock_info_batch(
                stock_list=batch, field_list=["Name", "DelayMin", "IsHKGP"],
            )
            if not all(isinstance(payload, dict) for payload in (dates, quotes, metadata)):
                continue
            for code in batch:
                quote, dated, security = (payload.get(code) for payload in (quotes, dates, metadata))
                if all(isinstance(item, dict) for item in (quote, dated, security)):
                    rows[code] = {
                        **quote, "HqDate": dated.get("HqDate"), "Name": security.get("Name"),
                        "DelayMin": security.get("DelayMin"), "IsHKGP": security.get("IsHKGP"),
                    }
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
