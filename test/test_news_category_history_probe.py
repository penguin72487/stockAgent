from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader.news_fulltext_bulk import cnyes_url
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError
from scripts.probe_news_category_history import main, probe_query, validated_queries
from test_cnyes_news_bulk import BASE, payload


SOURCE = SimpleNamespace(provider='cnyes', cnyes_apis=(BASE,))


def plan(low=1388534400, high=1388620799):
    return {'contract_version': 1, 'provider': 'cnyes', 'queries': [
        {'url': cnyes_url(BASE, start=low, end=high), 'purpose': 'actual_UTC_day_control'}]}


class HTTP:
    provider_not_before = {}

    def __init__(self, response):
        self.response, self.calls = response, []

    def get(self, url):
        self.calls.append(url)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def test_canonical_one_second_and_actual_730_day_queries_are_metadata_controls():
    assert validated_queries(plan(1, 1), SOURCE, now=2)
    assert validated_queries(plan(1, 730 * 86400), SOURCE, now=730 * 86400)
    assert validated_queries(plan(), SOURCE, now=1388620799)[0]['purpose'] == 'actual_UTC_day_control'


@pytest.mark.parametrize('change', [
    {'contract_version': True}, {'contract_version': 2}, {'provider': 'other'},
    {'queries': []}, {'queries': {}}, {'queries': [None]}, {'extra': 'not_reviewed'},
])
def test_invalid_plan_rejected_before_any_http(change):
    value = plan()
    value.update(change)
    with pytest.raises(ValueError):
        validated_queries(value, SOURCE)


@pytest.mark.parametrize('suffix', [
    '&limit=30', '&startAt=1388534400', '&endAt=1388620799', '&unknown=1',
    '&token=secret', '&sort=asc', '&offset=30', '#fragment',
])
def test_unreviewed_or_ambiguous_query_dimensions_rejected(suffix):
    value = plan()
    value['queries'][0]['url'] += suffix
    with pytest.raises(ValueError):
        validated_queries(value, SOURCE)


@pytest.mark.parametrize('replace', [
    ('page=1', 'page=31'), ('page=1', 'page=01'), ('limit=30', 'limit=100'),
    ('startAt=1388534400', 'startAt=1388534400.0'),
    ('startAt=1388534400', 'startAt=-1'),
    ('startAt=1388534400', 'startAt=1388620800'),
    ('startAt=1388534400', 'startAt=%31%33%38%38%35%33%34%34%30%30'),
    ('headline?', 'invented_category?'), ('https:', 'http:'),
])
def test_canonical_reviewed_parameters_only(replace):
    value = plan()
    value['queries'][0]['url'] = value['queries'][0]['url'].replace(*replace)
    with pytest.raises((ValueError, NewsError)):
        validated_queries(value, SOURCE)


@pytest.mark.parametrize('low,high,now', [(1, 730 * 86400 + 1, 800 * 86400), (1, 2, 1), (2, 1, 3)])
def test_no_future_or_over_two_years_or_inverted_query(low, high, now):
    value = plan()
    value['queries'][0]['url'] = f'{BASE}?limit=30&page=1&startAt={low}&endAt={high}'
    with pytest.raises(ValueError):
        validated_queries(value, SOURCE, now=now)


def test_duplicate_or_over40_or_missing_purpose_rejected():
    value = plan()
    for items in ([value['queries'][0]] * 2, [value['queries'][0]] * 41, [{'url': value['queries'][0]['url']}],
                  [{'url': value['queries'][0]['url'], 'purpose': ''}],
                  [{'url': value['queries'][0]['url'], 'purpose': 'x' * 161}]):
        with pytest.raises(ValueError):
            validated_queries({**value, 'queries': items}, SOURCE)


def test_actual_out_of_bound_clock_and_payment_type_are_not_hidden_or_raw_body_saved():
    value = plan()
    data = payload(ids=(1, 2))
    data['items']['data'][0]['publishAt'] = 1388640000
    data['items']['data'][1]['payment'] = 1
    url = value['queries'][0]['url']
    http = HTTP(Response(url, 200, {'content-type': 'application/json'}, json.dumps(data).encode()))
    result = probe_query(http, SOURCE, value['queries'][0])
    assert http.calls == [url] and result['parser_accepted']
    assert result['rows_after_request_end'] == 1
    assert result['source_id_clock_payment_metadata'][1]['payment'] == 1
    assert not result['body_validation_performed'] and not result['raw_response_retained']
    assert result['article_receipts_created'] == 0 and result['diagnostic_only']
    assert 'content' not in json.dumps(result) and 'body_sha256' not in result


def test_existing_parser_duplicate_join_clock_metadata_preserved():
    value = plan()
    data = payload(ids=tuple(range(1, 31)), total=40, **{'from': 1, 'to': 60})
    data['items']['data'] = [post for post in data['items']['data'] for _ in range(2)]
    url = value['queries'][0]['url']
    result = probe_query(HTTP(Response(url, 200, {'content-type': 'application/json'}, json.dumps(data).encode())), SOURCE, value['queries'][0])
    assert result['parser_accepted'] and result['returned_rows'] == 60
    assert result['page']['unique_rows'] == 30 and result['page']['duplicate_rows'] == 30
    assert len(result['source_id_clock_payment_metadata']) == 60


def test_persisted_source_refusal_does_not_send_a_probe():
    http = HTTP(None)
    http.provider_not_before = {'cnyes': float('inf')}
    result = probe_query(http, SOURCE, plan()['queries'][0])
    assert not http.calls and result['error'] == 'persisted_provider_cooldown' and not result['sent']


@pytest.mark.parametrize('error', [NewsError('http_429', retry=True, delay=100), NewsError('robots_disallow'), NewsError('news_http_owner_already_running')])
def test_source_denials_and_exclusive_owner_failures_are_visible(error):
    http = HTTP(error)
    result = probe_query(http, SOURCE, plan()['queries'][0])
    assert result['error'] == error.code and not result['parser_accepted'] and result['retry'] == error.retry


@pytest.mark.parametrize('body,status', [(b'not_json', 200), (b'{"items":null}', 200), (b'{"statusCode":422,"errors":{"page":["maximum30"]}}', 422)])
def test_invalid_or_rejected_page_does_not_claim_accepted_body(body, status):
    query = plan()['queries'][0]
    result = probe_query(HTTP(Response(query['url'], status, {'content-type': 'application/json'}, body)), SOURCE, query)
    assert not result['parser_accepted'] and result['status'] == status
    assert 'source_id_clock_payment_metadata' not in result
    if status == 422:
        assert result['error_response_meta']['errors']['page'] == ['maximum30']


def test_invalid_plan_main_never_constructs_http_owner(tmp_path, monkeypatch):
    path = tmp_path / 'invalid.json'
    path.write_text(json.dumps({'queries': []}))
    monkeypatch.setattr('scripts.probe_news_category_history.load_config', lambda _: ({'root': 'absent'}, SimpleNamespace(sources={'cnyes': SOURCE}), tmp_path))
    monkeypatch.setattr('scripts.probe_news_category_history.NewsHTTP', lambda *a, **k: pytest.fail('invalid input acquired shared HTTP owner'))
    with pytest.raises(SystemExit) as caught:
        main(['--query-plan', str(path), '--output', str(tmp_path / 'never.json')])
    assert caught.value.code == 2 and not (tmp_path / 'never.json').exists()
