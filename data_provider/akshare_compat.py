"""Resolve the known AkShare market endpoint names across SDK versions."""

from typing import Any, Callable


# Keep the caller's provider identifier stable for caches and diagnostics.
# The order here is the current export, then any previous names for the same API.
_API_NAMES = {
    "stock_hk_hist": ("stock_hk_hist_em", "stock_hk_hist"),
    "stock_hk_spot": ("stock_hk_spot_sina", "stock_hk_spot"),
    "stock_zh_a_hist": ("stock_zh_a_hist_em", "stock_zh_a_hist"),
    "stock_zh_a_daily": (
        "stock_zh_a_hist_sina",
        "stock_zh_a_daily_sina",
        "stock_zh_a_daily",
    ),
    "stock_zh_a_spot": ("stock_zh_a_spot_sina", "stock_zh_a_spot"),
}


class AkshareAPIUnavailable(AttributeError):
    """The installed package exports none of the supported names for an API."""


def resolve_akshare_api(akshare: Any, api_name: str) -> Callable[..., Any]:
    """Select an installed endpoint without making or retrying network calls."""
    names = _API_NAMES.get(api_name, (api_name,))
    for name in names:
        endpoint = getattr(akshare, name, None)
        if callable(endpoint):
            return endpoint
    raise AkshareAPIUnavailable(f"AkShare endpoint unavailable: {', '.join(names)}")
