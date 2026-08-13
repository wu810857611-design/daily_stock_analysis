#!/usr/bin/env python3
"""Generate the Longbridge SDK OAuth token cache for headless runtimes."""

from __future__ import annotations

import argparse
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Sequence


def _default_client_id() -> str:
    return (os.getenv("LONGBRIDGE_OAUTH_CLIENT_ID") or os.getenv("LONGBRIDGE_APP_KEY") or "").strip()


def _token_cache_path(client_id: str) -> Path:
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,160}", client_id):
        raise ValueError("Longbridge OAuth client_id contains unsafe characters")
    return Path.home() / ".longbridge" / "openapi" / "tokens" / client_id


def _provider_field(value: Any, name: str) -> Any:
    if isinstance(value, dict):
        return value.get(name)
    return getattr(value, name, None)


def _package_rows(context: Any) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for package in context.quote_package_details() or ():
        rows.append(
            {
                "key": str(_provider_field(package, "key") or "").strip(),
                "name": str(_provider_field(package, "name") or "").strip(),
                "description": str(_provider_field(package, "description") or "").strip(),
            }
        )
    return rows


def _has_hk_realtime_package(rows: Sequence[dict[str, str]]) -> bool:
    """Recognise HK stock real-time packages without mistaking index access."""

    for row in rows:
        key = str(row.get("key") or "").strip().lower()
        if key == "hk_basic" or not key.startswith("hk_"):
            continue
        if "hangsengindex" in key or "index" in key:
            continue
        if "l1" in key or "lv1" in key or "l2" in key or "lv2" in key:
            return True
    return False


def _next_backup_path(token_cache: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    candidate = token_cache.with_name(f"{token_cache.name}.before-reauth-{stamp}")
    counter = 1
    while candidate.exists():
        candidate = token_cache.with_name(f"{token_cache.name}.before-reauth-{stamp}-{counter}")
        counter += 1
    return candidate


def _set_aside_token_cache(token_cache: Path) -> Optional[Path]:
    if not token_cache.exists():
        return None
    backup = _next_backup_path(token_cache)
    token_cache.replace(backup)
    return backup


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run Longbridge OAuth authorization and persist the SDK token cache. "
            "Use this once on an interactive machine before schedule/Docker/GitHub Actions runs."
        )
    )
    parser.add_argument(
        "--client-id",
        default=_default_client_id(),
        help="OAuth client_id. Defaults to LONGBRIDGE_OAUTH_CLIENT_ID, then LONGBRIDGE_APP_KEY.",
    )
    parser.add_argument(
        "--verify-symbol",
        default="",
        help="Optional Longbridge symbol such as AAPL.US or 700.HK to verify QuoteContext after auth.",
    )
    parser.add_argument(
        "--force-reauthorize",
        action="store_true",
        help=(
            "Move an existing token cache to a timestamped backup before OAuth. "
            "Use this after Longbridge asks an OAuth client to re-authorize."
        ),
    )
    parser.add_argument(
        "--require-hk-realtime",
        action="store_true",
        help=(
            "Exit non-zero unless the authorized quote packages include an HK "
            "stock LV1/LV2 real-time package (HK_Basic does not pass)."
        ),
    )
    args = parser.parse_args(argv)

    client_id = (args.client_id or "").strip()
    if not client_id:
        parser.error("missing --client-id or LONGBRIDGE_OAUTH_CLIENT_ID")

    try:
        token_cache = _token_cache_path(client_id)
    except ValueError as exc:
        parser.error(str(exc))

    backup_path: Optional[Path] = None
    if args.force_reauthorize:
        token_cache.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        backup_path = _set_aside_token_cache(token_cache)
        if backup_path is not None:
            print(f"Existing OAuth token cache backed up to: {backup_path}")

    try:
        from longbridge.openapi import Config, OAuthBuilder, QuoteContext
    except Exception as exc:
        if backup_path is not None and not token_cache.exists():
            backup_path.replace(token_cache)
        raise SystemExit("longbridge SDK is not installed. Run `pip install -r requirements.txt` first.") from exc

    def show_url(url: str) -> None:
        print(f"Open this URL to authorize Longbridge OAuth:\n{url}\n")

    try:
        oauth = OAuthBuilder(client_id).build(show_url)
        config = Config.from_oauth(oauth)
    except Exception:
        if backup_path is not None and not token_cache.exists():
            backup_path.replace(token_cache)
            print("OAuth failed; restored the previous token cache.")
        raise

    context = None
    if args.verify_symbol or args.require_hk_realtime:
        ctx = QuoteContext(config)
        context = ctx

    if args.verify_symbol and context is not None:
        ctx = context
        quote = ctx.quote([args.verify_symbol])[0]
        timestamp = _provider_field(quote, "timestamp")
        print(
            f"Verified {args.verify_symbol}: "
            f"price={_provider_field(quote, 'last_done')} "
            f"provider_timestamp={timestamp or 'missing'}"
        )

    if args.require_hk_realtime and context is not None:
        rows = _package_rows(context)
        print("Authorized quote packages:")
        if not rows:
            print("- none reported")
        for row in rows:
            print(f"- {row['key'] or 'unknown'} | {row['name'] or 'unnamed'}")
        if not _has_hk_realtime_package(rows):
            print(
                "HK real-time permission check failed: the OAuth session does not "
                "report an HK stock LV1/LV2 package. HK_Basic is 15-minute delayed."
            )
            return 2

    if not token_cache.is_file():
        print(f"OAuth did not create the expected token cache: {token_cache}")
        return 2
    print(f"OAuth token cache: {token_cache}")
    print("For GitHub Actions, store the base64 of this file as " "`LONGBRIDGE_OAUTH_TOKEN_CACHE_B64`.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
