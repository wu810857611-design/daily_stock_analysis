"""Request the provider's live HK namespace without changing quote admission."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from scripts.intraday_session import TencentBatchQuoteFetcher, parse_tencent_batch, to_tencent_symbol
from src.services.tencent_market_snapshot import _parse_payload, load_fresh_hk_membership_snapshot

NOW = datetime(2026, 11, 4, 9, 32, tzinfo=ZoneInfo('Asia/Shanghai'))


def record(key='r_hk00981', *, age=5, price='42.560', code='00981', currency='HKD'):
    fields = [''] * 78
    fields[1:7] = ['中芯国际', code, price, '43.4', '42.8', '1000']
    fields[30] = (NOW - timedelta(seconds=age)).strftime('%Y/%m/%d %H:%M:%S') if age is not None else ''
    fields[32], fields[37], fields[75] = '-0.14', '76580520.000', currency
    return f'v_{key}="{"~".join(fields)}";'


class Response:
    def __init__(self, body): self.body = body.encode('gbk')
    def __enter__(self): return self
    def __exit__(self, *_args): pass
    def read(self): return self.body


@pytest.mark.parametrize('age,price,fresh', [(5, '42.560', True), (90, '42.560', True),
    (91, '42.560', False), (-31, '42.560', False), (None, '42.560', False), (5, '', False)])
def test_live_key_runs_through_both_real_parsers_with_original_time_and_price_gates(age, price, fresh):
    payload = record(age=age, price=price)
    intraday = parse_tencent_batch(payload, ['HK00981'], fetched_at=NOW)['HK00981']
    scan = _parse_payload(payload, ['HK00981'], now=NOW, freshness_seconds=90)
    assert (not intraday.is_stale) is fresh
    assert ('HK00981' in scan) is fresh
    if fresh:
        assert intraday.price == 42.56
        assert intraday.stale_seconds == age
        assert intraday.provider_timestamp != intraday.fetched_at
        assert scan['HK00981']['amount'] == 76_580_520


@pytest.mark.parametrize('key,code', [('r_hk00981', '01347'), ('r_hk00981', ''),
                                    ('r_sh00981', '00981'), ('x_r_hk00981', '00981')])
def test_new_namespace_requires_matching_full_provider_code_and_hk_market(key, code):
    payload = record(key, code=code)
    assert parse_tencent_batch(payload, ['HK00981'], fetched_at=NOW)['HK00981'].is_stale
    assert not _parse_payload(payload, ['HK00981'], now=NOW, freshness_seconds=90)


@pytest.mark.parametrize('alternate', [False, True])
def test_existing_routes_request_live_key_without_additional_calls(alternate):
    calls = []
    def opener(request, *, timeout):
        calls.append(request.full_url)
        assert timeout == 8
        assert request.full_url.endswith('q=r_hk00981')
        if alternate and len(calls) == 1:
            raise TimeoutError('primary unavailable')
        return Response(record())
    fetcher = TencentBatchQuoteFetcher(opener=opener, retry_backoff_seconds=0)
    quote = fetcher.fetch(['HK00981'], now=NOW)['HK00981']
    assert not quote.is_stale and quote.price == 42.56
    assert len(calls) == (2 if alternate else 1)
    assert fetcher.last_diagnostics['route_stats'][-1]['requested_keys'] == ['r_hk00981']
    calls.clear()
    snapshot = load_fresh_hk_membership_snapshot(['HK00981'], now=NOW, opener=opener)
    assert snapshot['fresh_count'] == 1
    assert len(calls) == (2 if alternate else 1)
    assert snapshot['route_stats'][-1]['requested_keys'] == ['r_hk00981']


def test_mixed_batch_keeps_a_share_key_and_accepts_legacy_hk_response():
    assert to_tencent_symbol('HK00981') == 'r_hk00981'
    assert to_tencent_symbol('601318') == 'sh601318'
    assert to_tencent_symbol('300408') == 'sz300408'
    payload = record('hk00981')
    assert not parse_tencent_batch(payload, ['HK00981'], fetched_at=NOW)['HK00981'].is_stale
    assert 'HK00981' in _parse_payload(payload, ['HK00981'], now=NOW, freshness_seconds=90)


@pytest.mark.parametrize('key', ['hk00981', 'r_hk00981'])
def test_hk_raw_turnover_is_hkd_and_is_not_multiplied_as_a_share_ten_thousands(key):
    quote = _parse_payload(record(key), ['HK00981'], now=NOW, freshness_seconds=90)['HK00981']
    assert quote['amount'] == 76_580_520


@pytest.mark.parametrize('currency', ['', 'USD', 'CNY'])
def test_live_hk_key_does_not_admit_an_unknown_or_different_currency(currency):
    payload = record(currency=currency)
    assert parse_tencent_batch(payload, ['HK00981'], fetched_at=NOW)['HK00981'].is_stale
    assert not _parse_payload(payload, ['HK00981'], now=NOW, freshness_seconds=90)
    fetcher = TencentBatchQuoteFetcher(opener=lambda *_args, **_kwargs: Response(payload),
                                       retry_backoff_seconds=0)
    assert fetcher.fetch(['HK00981'], now=NOW)['HK00981'].is_stale
    reason = 'unsupported_currency' if currency else 'missing_currency'
    assert fetcher.last_diagnostics['route_rejections'] == {reason: 2}
