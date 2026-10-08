from datetime import datetime

import pandas as pd
import pytest

from data_provider.fundamental_adapter import AkshareFundamentalAdapter, _pick_by_keywords, _safe_float


def adapter_for(frames):
    calls = []
    def runner(fn, kwargs, validator=None):
        calls.append((fn.__name__, kwargs))
        frame = frames.get(fn.__name__, pd.DataFrame())
        if isinstance(frame, Exception):
            raise frame
        if validator is not None and not validator(frame):
            raise ValueError("invalid financial payload")
        return frame
    return AkshareFundamentalAdapter(call_runner=runner), calls


def test_duplicate_financial_indicators_and_units_are_not_silently_lost():
    frame = pd.DataFrame({"指标": ["营业总收入", "营业总收入", "归母净利润", "归母净利润", "经营现金流"],
                          "20260630": [1000, 1000, 200, 200, 300], "20251231": [800, 800, 100, 100, 250]})
    adapter, calls = adapter_for({"stock_financial_abstract": frame})
    result = adapter.get_fundamental_bundle("601857", financial_only=True)
    report = result["earnings"]["financial_report"]
    assert report["report_date"] == "2026-06-30"
    assert report["revenue"] == 1000 and report["net_profit_parent"] == 200
    assert report["currency"] == "CNY"
    assert all(kwargs.get("symbol") == "601857" for _, kwargs in calls)
    assert calls[1][1]["start_year"] == str(datetime.now().year - 1)


def test_conflicting_duplicates_and_growth_rates_cannot_be_absolute_profit():
    assert _pick_by_keywords(pd.Series([100, 200], index=["归母净利润", "归母净利润"]), ["归母净利润"]) is None
    assert _pick_by_keywords(pd.Series([21], index=["净利润同比增长率"]), ["净利润"]) is None
    assert _safe_float(_pick_by_keywords(pd.Series([2], index=["营业收入(万元)"]), ["营业收入"])) == 20000


@pytest.mark.parametrize("value,expected", [("2.5亿元", 250000000), ("18.2%", 18.2), ("2万元", 20000),
                                           (float("inf"), None), (float("nan"), None), (True, None)])
def test_explicit_financial_units_and_non_finite_values(value, expected):
    assert _safe_float(value) == expected


def test_partial_primary_is_completed_by_same_period_fallback():
    first = pd.DataFrame([{"日期": "2026-06-30", "经营现金流": 300}])
    second = pd.DataFrame([{"日期": "2025-12-31", "营业收入": 10},
                           {"日期": "2026-06-30", "营业收入": "1.5亿元", "归母净利润": "2万元", "净资产收益率": "18%"}])
    adapter, _ = adapter_for({"stock_financial_abstract": first, "stock_financial_analysis_indicator": second})
    report = adapter.get_fundamental_bundle("601857", financial_only=True)["earnings"]["financial_report"]
    assert report["report_date"] == "2026-06-30"
    assert report["revenue"] == 150000000 and report["operating_cash_flow"] == 300
    assert report["field_sources"]["revenue"] == "stock_financial_analysis_indicator"


def test_period_mismatch_and_future_rows_are_not_merged():
    first = pd.DataFrame([{"日期": "2026-06-30", "经营现金流": 300}])
    second = pd.DataFrame([{"日期": "2025-12-31", "营业收入": 10}, {"日期": "2099-12-31", "营业收入": 999}])
    adapter, _ = adapter_for({"stock_financial_abstract": first, "stock_financial_analysis_indicator": second})
    result = adapter.get_fundamental_bundle("601857", financial_only=True)
    assert result["earnings"]["financial_report"]["revenue"] is None
    assert any("ReportPeriodMismatch" in error for error in result["errors"])
