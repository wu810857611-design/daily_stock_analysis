from types import SimpleNamespace

import pytest

from data_provider.akshare_compat import AkshareAPIUnavailable, resolve_akshare_api


@pytest.mark.parametrize(
    "api_name",
    ["stock_hk_hist", "stock_hk_spot", "stock_zh_a_hist", "stock_zh_a_daily", "stock_zh_a_spot"],
)
def test_installed_akshare_exports_market_endpoint(api_name):
    import akshare

    assert callable(resolve_akshare_api(akshare, api_name))


def test_alias_is_selected_by_availability_without_retrying_a_provider_failure():
    def old(**kwargs):
        raise AssertionError("the legacy alias must not be called again")

    def current(**kwargs):
        raise AttributeError("upstream parsing failed")

    sdk = SimpleNamespace(stock_hk_hist=old, stock_hk_hist_em=current)
    selected = resolve_akshare_api(sdk, "stock_hk_hist")
    assert selected is current
    with pytest.raises(AttributeError, match="upstream parsing failed"):
        selected(symbol="00014")
    with pytest.raises(AkshareAPIUnavailable, match="stock_hk_hist_em"):
        resolve_akshare_api(SimpleNamespace(), "stock_hk_hist")
