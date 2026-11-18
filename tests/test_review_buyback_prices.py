"""Regressions for daily buyback roles in the normal scan review gate."""
import json

import pytest

from scripts.market_scan import ReviewResponseError, _review_retry_feedback, build_litellm_reviewer
from src.services.review_fact_consistency import fact_contract, numeric_facts, validate_review_facts
from tests.test_market_scan_service import _install_review_completion, _review_response, _service

NEWS = {
    "title": "小米集团-W：11月18日斥资4986.74万港元回购168万股",
    "snippet": "21财经11月18日电，小米集团-W（01810.HK）发布翌日披露报表，11月18日，公司回购168万股，"
               "每股回购最高价为29.7港元，最低价为29.62港元，回购总代价为4986.74万港元，回购股份拟注销。",
    "published_date": "2026-11-18",
}


def candidate(news=(), announcements=()):
    return {"code": "HK01810", "name": "小米集团-W", "price": 29.95, "research_evidence": {
        "announcements_and_news": {"items": list(news)}, "announcements": {"items": list(announcements)},
    }}


def check(stock, text, field="facts"):
    return validate_review_facts({"numeric_facts": numeric_facts(stock), field: [text]}, stock, require_echo=True)


def test_actual_close_claims_distinguish_extremes_from_weighted_mean():
    stock = candidate([NEWS])
    contract = fact_contract(stock)
    row, = contract["buyback_prices"]
    assert row == {"date": "2026-11-18", "currency": "HKD", "low": 29.62, "high": 29.7,
                   "average": pytest.approx(4986.74 / 168)}
    assert contract["values"] == numeric_facts(stock)
    assert check(stock, "已验证交易所翌日披露报表显示11月18日回购168万股拟注销，每股均价约29.68HKD") == []
    error, = check(stock, "官方翌日披露报表显示11月18日回购168万股，价格区间29.62–29.68HKD")
    assert (error["field"], error["error"], error["expected"], error["observed"]) == (
        "buyback_high", "buyback_price_mismatch", 29.70, 29.68)


@pytest.mark.parametrize("text", [
    "11月18日回购每股价格区间29.62至29.70港元",
    "11月18日以29.62–29.70HKD的价格购回股票",
    "11月18日实际回购168万股，最高回购价约为29.70港币，最低价29.62HKD，均价约29.68港元",
    "11月18日回购168万股。每股均价约29.68港元",
    "11月18日回购後，股票当日最高价36港元，最低价30港元",
    "拟以每股最高价35港元回购股份", "历次回购价格区间（23.00-30.00港元）",
])
def test_correct_prices_or_independent_price_contexts(text):
    assert check(candidate([NEWS]), text) == []


@pytest.mark.parametrize("text,role", [
    ("11月18日回购，价格区间29.70–29.62HKD", "buyback_low"),
    ("11月18日回购，平均成交价29.70港元", "buyback_average"),
    ("11月18日购回，最高价29.70美元", "buyback_high"),
    ("11月17日回购，价格上限29.70港元", "buyback_high"),
    ("11月18日回购，最高价29.70万港元", "buyback_high"),
    ("11月18日回购，最高价29.70%", "buyback_high"),
    ("11月18日回购，最高价29.7e2港元", "buyback_high"),
    ("11月18日回购，最高价29.7日元", "buyback_high"),
])
def test_incorrect_bounds_role_date_currency_and_scale(text, role):
    assert role in {e["field"] for e in check(candidate([NEWS]), text, "risks")}


def test_later_proposed_purchase_does_not_hide_an_earlier_completed_price():
    errors = check(candidate([NEWS]), "11月18日实际回购，最高价29.68港元，未来拟回购更多股份。")
    assert [(e["field"], e["expected"]) for e in errors] == [("buyback_high", 29.70)]


def test_relative_trade_date_uses_frozen_snapshot_and_never_borrows_another_day():
    stock = {**candidate([NEWS]), "facts": {"snapshot_fetched_at": "2026-11-19T10:29:33+08:00"}}
    assert check(stock, "昨日回购最高价29.70港元") == []
    assert check(stock, "今日回购最高价29.70港元")[0]["error"] == "unsupported_buyback_price"
    assert check(candidate([NEWS]), "昨日回购最高价29.70港元")[0]["error"] == "unsupported_buyback_price"


def test_only_derived_mean_cannot_supply_range_or_stock_quote():
    stock = candidate([{**NEWS, "snippet": ""}])
    row, = fact_contract(stock)["buyback_prices"]
    assert set(row) == {"date", "currency", "average"}
    assert check(stock, "11月18日回购，均价约29.68港元") == []
    errors = check(stock, "11月18日回购，价格区间29.62–29.68港元，现价8港元")
    assert [(e["field"], e["error"]) for e in errors[:2]] == [
        ("buyback_low", "unsupported_buyback_price"), ("buyback_high", "unsupported_buyback_price")]
    assert errors[2]["field"] == "price"


def test_dates_in_same_sentence_bind_to_their_own_price():
    yesterday = {k: v.replace("11月18", "11月17").replace("29.7港元", "31港元") for k, v in NEWS.items()}
    stock = candidate([yesterday, NEWS])
    assert check(stock, "11月17日回购最高价31港元，11月18日回购最高价29.70港元") == []
    errors = check(stock, "11月17日回购最高价29.70港元，11月18日回购最高价31港元")
    assert [(e["date"], e["expected"]) for e in errors] == [("2026-11-17", 31), ("2026-11-18", 29.7)]
    assert check(stock, "公司回购最高价29.70港元")[0]["error"] == "unsupported_buyback_price"


def test_formal_excerpt_overrides_conflicting_news_only_for_labelled_roles():
    conflicts = {**NEWS, "snippet": NEWS["snippet"].replace("29.7港元", "29.8港元")}
    stock = candidate([NEWS, conflicts])
    assert check(stock, "11月18日回购最高价29.70港元")[0]["error"] == "unsupported_buyback_price"
    pdf = {"title": "翌日披露报表", "published_date": "2026-11-18", "body_excerpt_available": True,
           "content_reviewed": False, "excerpt": "11月18日，本公司購回股份，最高成交價29.70港元。"}
    assert check(candidate([NEWS, conflicts], [pdf]), "11月18日回购最高价29.70港元") == []
    listing = {**pdf, "body_excerpt_available": False}
    unlabeled_table = {**pdf, "excerpt": "11月17日回购股份\n11月18日回购股份\nHKD\n29.68\n29.70"}
    assert "buyback_prices" not in fact_contract(candidate([], [listing, unlabeled_table]))


@pytest.mark.parametrize("headline", ["乙公司：11月18日回购168万股", "小米集团供应商乙公司11月18日回购",
                                     "小米集团-W：乙公司回购168万股", "小米集团-W（01234.HK）回购168万股"])
def test_a_different_issuer_cannot_supply_the_subjects_evidence(headline):
    assert "buyback_prices" not in fact_contract(candidate([{**NEWS, "title": headline}]))


def test_mixed_issuers_plan_and_future_date_do_not_supply_endpoints():
    bad_snippets = [
        "另一家乙公司（01234.HK）11月18日回购，每股最高价为29.70港元。",
        "乙公司11月18日回购，每股最高价为29.70港元。",
        "公司11月18日拟以每股最高价为29.70港元回购。",
    ]
    for snippet in bad_snippets:
        assert check(candidate([{**NEWS, "snippet": snippet}]), "11月18日回购最高价29.70港元")[0]["expected"] is None
    later = {k: v.replace("11月18", "11月19") for k, v in NEWS.items()}
    assert "buyback_prices" not in fact_contract(candidate([later]))


def test_retry_feedback_uses_only_fields_derived_from_input_evidence():
    stock = candidate([NEWS])
    exc = ReviewResponseError("response_numeric_fact_mismatch", numeric_errors={"HK01810": [
        {"field": "buyback_high", "error": "buyback_price_mismatch", "date": "2026-11-18",
         "currency": "HKD", "expected": "untrusted", "claim": "private"},
        {"field": "buyback_low", "error": "unsupported_buyback_price", "date": "private",
         "currency": "untrusted", "expected": "private"},
    ]})
    feedback = _review_retry_feedback(exc, [stock])
    assert feedback["numeric_errors"]["HK01810"] == [
        {"field": "buyback_high", "error": "buyback_price_mismatch", "date": "2026-11-18",
         "currency": "HKD", "expected": 29.7},
        {"field": "buyback_low", "error": "unsupported_buyback_price", "date": None,
         "currency": None, "expected": None},
    ]


@pytest.mark.parametrize("correct_on_retry", [True, False])
def test_buyback_mismatch_retries_only_affected_channel_and_preserves_dual_gate(monkeypatch, tmp_path, correct_on_retry):
    calls = []
    news = {"title": "甲公司：7月28日回购100股", "snippet": "公司7月28日回购100股，价格区间8.1至8.4元，均价8.2元。",
            "published_date": "2026-07-28"}
    research = candidate([news])["research_evidence"]

    def completion(**kwargs):
        request = json.loads(kwargs["messages"][1]["content"])
        model = kwargs["model"]
        calls.append((model, request))
        response = _review_response(request["candidates"])
        payload = json.loads(response["choices"][0]["message"]["content"])
        for review in payload["reviews"]:
            if review["code"] == "600001":
                if model == "deepseek/test-deepseek":
                    high = 8.4 if correct_on_retry and "retry_feedback" in request else 8.2
                    review["facts"] = [f"7月28日回购，价格区间8.1–{high}元"]
                else:
                    review["facts"] = ["7月28日回购，均价8.2元"]
        response["choices"][0]["message"]["content"] = json.dumps(payload)
        return response

    _install_review_completion(monkeypatch, completion)
    result = _service(tmp_path, qwen=build_litellm_reviewer("qwen"), deepseek=build_litellm_reviewer("deepseek"),
                      research_loader=lambda _: research).run()
    selected = next(c for c in result["candidates"] if c["code"] == "600001")
    assert result["review_complete"] is correct_on_retry
    assert all("retry_feedback" not in b for m, b in calls if m == "openai/test-qwen")
    attempts = [b for m, b in calls if m == "deepseek/test-deepseek" and any(
        c["code"] == "600001" for c in b["candidates"])]
    assert len(attempts) == 2
    assert attempts[0]["candidates"] == attempts[1]["candidates"]
    assert attempts[1]["retry_feedback"]["numeric_errors"] == {"600001": [{
        "field": "buyback_high", "error": "buyback_price_mismatch", "date": "2026-07-28",
        "currency": "CNY", "expected": 8.4}]}
    if correct_on_retry:
        assert selected["deepseek_review"]["numeric_consistency"]["status"] == "checked"
    else:
        assert selected["eligible_for_intraday_review"] is False
