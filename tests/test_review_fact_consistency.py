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


def test_dated_research_valuation_is_checked_without_replacing_entry_quote():
    candidate = {**CANDIDATE, "research_evidence": {"fundamentals": {"data": {
        "valuation": {"data": {"pe_ratio": 12.5, "pb_ratio": 1.2}, "status": "partial"}
    }}}}
    values = numeric_facts(candidate)
    assert values["price"] == CANDIDATE["price"] and values["pe"] == 12.5
    assert validate_review_facts({"numeric_facts": values, "facts": ["PE 125倍"]}, candidate)


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


@pytest.mark.parametrize('change', [1.05769, 1.30612, 3.103, 3.717, 2.069, 2.954, 2.412, 2.37515, -1.05769])
@pytest.mark.parametrize('label', ['日涨跌幅', '日内涨跌幅', '日涨跌'])
def test_neutral_change_labels_preserve_actual_sign(change, label):
    candidate = {**CANDIDATE, 'change_pct': change}
    review = {'numeric_facts': numeric_facts(candidate), 'facts': [f'{label}{change}%']}
    assert validate_review_facts(review, candidate, require_echo=True) == []


@pytest.mark.parametrize('claim,change', [
    ('日跌0.28%', -0.27972), ('日内跌幅0.28%', -0.27972),
    ('当日跌幅0.28%', -0.27972), ('日涨幅0.28%', 0.27972),
])
def test_explicit_direction_remains_checked(claim, change):
    candidate = {**CANDIDATE, 'change_pct': change}
    assert validate_review_facts({'facts': [claim]}, candidate) == []
    assert validate_review_facts({'facts': [claim]}, {**candidate, 'change_pct': -change})


@pytest.mark.parametrize('claim', ['日涨-0.28%', '日跌0%', '日跌+0.28%'])
def test_explicit_downward_or_contradictory_direction_cannot_pass_positive_input(claim):
    assert validate_review_facts({'facts': [claim]}, CANDIDATE)


def test_rounding_does_not_allow_opposite_neutral_direction():
    candidate = {**CANDIDATE, 'change_pct': 0.2}
    assert validate_review_facts({'facts': ['日涨跌幅-0.1%']}, candidate)


@pytest.mark.parametrize('candidate', [
    {**CANDIDATE, 'amount': 339629533.0, 'change_pct': 1.05769, 'pe': 47.90730029},
    {**CANDIDATE, 'amount': 522609577.0, 'change_pct': 1.30612},
    {**CANDIDATE, 'amount': 716082323.0, 'change_pct': 3.103},
    {**CANDIDATE, 'amount': 703023966.0, 'change_pct': 3.717},
])
def test_program_rendered_contract_can_be_repeated_without_false_mismatch(candidate):
    contract = fact_contract(candidate)
    assert validate_review_facts({'numeric_facts': contract['values'], 'facts': [contract['rendered']]},
                                 candidate, require_echo=True) == []


@pytest.mark.parametrize('claim', ['成交额3.3963e8港元', '成交额3.3963E+8HKD', '成交额3.3963亿港元'])
def test_scientific_amount_and_printed_precision_are_converted_before_checking(claim):
    candidate = {**CANDIDATE, 'amount': 339629533.0}
    assert validate_review_facts({'facts': [claim]}, candidate) == []


def test_one_decimal_pe_sentence_does_not_relax_structured_echo():
    candidate = {**CANDIDATE, 'pe': 47.90730029}
    correct = numeric_facts(candidate)
    assert validate_review_facts({'numeric_facts': correct, 'facts': ['PE 47.9倍']}, candidate,
                                 require_echo=True) == []
    errors = validate_review_facts({'numeric_facts': {**correct, 'pe': 47.9}}, candidate, require_echo=True)
    assert any(item['error'] == 'structured_mismatch' for item in errors)
    assert validate_review_facts({'numeric_facts': correct, 'facts': ['PE 479倍']}, candidate, require_echo=True)


@pytest.mark.parametrize('field', ['pe', 'pb'])
def test_only_missing_optional_valuation_may_be_echoed_as_null(field):
    contract = fact_contract(CANDIDATE)
    assert field in contract['nullable_missing_fields']
    assert validate_review_facts({'numeric_facts': {**contract['values'], field: None}}, CANDIDATE,
                                 require_echo=True) == []
    for fabricated in [0, 1.2, 'null']:
        errors = validate_review_facts({'numeric_facts': {**contract['values'], field: fabricated}},
                                       CANDIDATE, require_echo=True)
        assert any(item['error'] == 'unsupported_numeric_fact' and item['observed'] == fabricated
                   for item in errors)
    assert validate_review_facts({'numeric_facts': contract['values'], 'facts': [f'{field.upper()} 1.2倍']},
                                 CANDIDATE, require_echo=True)


def test_null_does_not_replace_known_valuation_or_add_unknown_fields():
    candidate = {**CANDIDATE, 'pb': 1.2}
    assert validate_review_facts({'numeric_facts': {**numeric_facts(candidate), 'pb': None}}, candidate,
                                 require_echo=True)
    assert validate_review_facts({'numeric_facts': {**numeric_facts(CANDIDATE), 'invented': None}},
                                 CANDIDATE, require_echo=True)


@pytest.mark.parametrize('claim', ['成交额0e9999亿港元', '当前价1e999港元', '日涨跌幅1e999%', '成交额3.3963e6港元'])
def test_nonfinite_or_wrong_scale_claims_fail_without_crashing(claim):
    assert validate_review_facts({'facts': [claim]}, CANDIDATE)


@pytest.mark.parametrize('claim', ['成交额0e308港元', '当前价0e308港元', '日涨跌幅0e308%'])
def test_coarse_zero_cannot_erase_nonzero_source_facts(claim):
    assert validate_review_facts({'numeric_facts': numeric_facts(CANDIDATE), 'facts': [claim]},
                                 CANDIDATE, require_echo=True)


@pytest.mark.parametrize('label', ['qwen', 'deepseek'])
def test_actual_review_batches_accept_neutral_scientific_and_nullable_contracts(monkeypatch, label):
    # Keep scripted SDK counters in-process; the actual isolated numeric-error
    # transport and recovery are covered by test_market_scan_review_deadline.
    monkeypatch.setattr('scripts.market_scan.run_isolated', lambda callback, *_a, **_k: callback())
    calls = []
    monkeypatch.setenv('MARKET_SCAN_QWEN_MODEL', 'fixture')
    monkeypatch.setenv('MARKET_SCAN_DEEPSEEK_MODEL', 'fixture')
    monkeypatch.setenv('LLM_DASHSCOPE_API_KEY', 'offline-fixture')
    monkeypatch.setenv('LLM_DEEPSEEK_API_KEY', 'offline-fixture')
    monkeypatch.delenv('LLM_DASHSCOPE_MODELS', raising=False)
    monkeypatch.delenv('LLM_DEEPSEEK_MODELS', raising=False)
    candidates = [{**CANDIDATE, 'code': f'HK{index:05}', 'amount': 339629533.0, 'change_pct': 1.05769}
                  for index in range(12)]
    def completion(**kwargs):
        calls.append(kwargs)
        batch = json.loads(kwargs['messages'][1]['content'])['candidates']
        reviews = [{'code': item['code'], 'verdict': 'pass',
                    'numeric_facts': {**item['numeric_fact_contract']['values'], 'pb': None},
                    'facts': [item['numeric_fact_contract']['rendered']]}
                   for item in batch]
        return {'choices': [{'finish_reason': 'stop', 'message': {'content': json.dumps({'reviews': reviews})}}]}
    monkeypatch.setitem(sys.modules, 'litellm', SimpleNamespace(completion=completion))
    reviewer = build_litellm_reviewer(label)
    assert len(reviewer(candidates)['reviews']) == 12
    assert reviewer.diagnostics['completed_candidate_count'] == 12
    assert reviewer.diagnostics['status'] == 'completed'
    assert all(request['status'] == 'completed' for request in reviewer.diagnostics['requests'])
    assert len(calls) == len(reviewer.diagnostics['batches'])


@pytest.mark.parametrize('recovers', [True, False])
def test_numeric_misread_retries_within_existing_budget(monkeypatch, recovers):
    monkeypatch.setattr('scripts.market_scan.run_isolated', lambda callback, *_a, **_k: callback())
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
