import json
import sys
from types import SimpleNamespace

import pytest

from scripts.market_scan import build_litellm_reviewer
from src.services.review_fact_consistency import fact_contract, numeric_facts, validate_review_facts
from tests.test_market_scan_service import _service

CANDIDATE = {'code': 'HK01548', 'price': 43.02, 'change_pct': 0.27972, 'amount': 1649000000}


def test_percent_is_percentage_points_and_rounding_is_allowed():
    review = {'numeric_facts': numeric_facts(CANDIDATE),
              'facts': ['快照价43.02港元，日涨0.28%，成交额16.49亿港元']}
    assert validate_review_facts(review, CANDIDATE, require_echo=True) == []
    assert fact_contract(CANDIDATE)['units']['change_pct'] == 'percentage_points'
    assert '0.27972%' in fact_contract(CANDIDATE)['rendered']


@pytest.mark.parametrize('facts,field', [
    ('日涨27.97%', 'change_pct'), ('快照价43.02人民币', 'price'),
    ('成交额16.49万港元', 'amount'), ('当前价4302港元', 'price'),
])
def test_contradictions_are_detected_in_facts_and_risk_reasoning(facts, field):
    review = {'numeric_facts': numeric_facts(CANDIDATE), 'risks': [facts]}
    errors = validate_review_facts(review, CANDIDATE, require_echo=True)
    assert any(x['field'] == field for x in errors)


def test_missing_echo_and_missing_source_are_not_default_pass():
    assert validate_review_facts({}, CANDIDATE, require_echo=True)
    assert validate_review_facts({'numeric_facts': {'price': 3}}, {'code': '600000', 'amount': 1}, require_echo=True)
    negative = {**CANDIDATE, 'change_pct': -0.27972}
    assert not validate_review_facts({'facts': ['日跌0.28%']}, negative)


@pytest.mark.parametrize('recovers', [True, False])
def test_numeric_misread_retries_within_existing_budget(monkeypatch, recovers):
    calls = []
    monkeypatch.setenv('MARKET_SCAN_QWEN_MODEL', 'fixture')
    monkeypatch.setenv('LLM_DASHSCOPE_API_KEY', 'offline-fixture')
    monkeypatch.delenv('LLM_DASHSCOPE_MODELS', raising=False)
    monkeypatch.setattr('scripts.market_scan.time.sleep', lambda _: None)
    def completion(**kwargs):
        calls.append(kwargs)
        batch = json.loads(kwargs['messages'][1]['content'])['candidates']
        assert batch[0]['numeric_fact_contract']['values']['change_pct'] == 0.27972
        fact = '日涨0.28%' if recovers and len(calls) > 1 else '日涨27.97%'
        return {'choices': [{'finish_reason': 'stop', 'message': {'content': json.dumps({'reviews': [
            {'code': 'HK01548', 'verdict': 'pass', 'numeric_facts': numeric_facts(CANDIDATE), 'facts': [fact]}
        ]})}}]}
    monkeypatch.setitem(sys.modules, 'litellm', SimpleNamespace(completion=completion))
    reviewer = build_litellm_reviewer('qwen')
    if recovers:
        assert reviewer([CANDIDATE])['reviews'][0]['facts'] == ['日涨0.28%']
        assert reviewer.diagnostics['status'] == 'completed'
    else:
        with pytest.raises(ValueError, match='response_numeric_fact_mismatch'):
            reviewer([CANDIDATE])
        assert reviewer.diagnostics['completed_candidate_count'] == 0
    assert len(calls) == 2
    failed = reviewer.diagnostics['requests'][0]
    assert failed['numeric_errors']['HK01548'][0]['field'] == 'change_pct'
    assert failed['model'] == 'openai/fixture'


def test_injected_review_with_contradictory_vote_cannot_enter_conditional_consensus(tmp_path):
    def review(candidates):
        return {'reviews': [{'code': item['code'], 'verdict': 'pass', 'confidence': .9,
                             'hard_risk': False, 'facts': ['当前价10000元']} for item in candidates]}
    result = _service(tmp_path, qwen=review, deepseek=review).run()
    assert result['review_complete'] is True  # Calls completed; fact integrity failed.
    assert not any(item.get('eligible_for_intraday_review') for item in result['candidates'])
    assert all(item['qwen_review']['numeric_consistency']['status'] == 'inconsistent' for item in result['candidates'])
