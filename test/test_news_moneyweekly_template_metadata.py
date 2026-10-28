"""Closed prior geometry is required before one different template question."""
from dataclasses import replace
import json

import pytest

from downloader import news_fulltext_candidate_diagnostics as jobs
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_moneyweekly_metadata import LAYOUT, QUESTION, template_metadata, template_result
from downloader.news_fulltext_policy import NewsError
from test_news_moneyweekly_body_geometry import (
    settings, queue, submit as submit_geometry, transport, claim, heads, MAGAZINE, FN, NEWS,
)


def template_page():
    return ('<html><head><meta charset="utf-8"></head><body><main><h1>PRIVATE HEADLINE</h1>'
        '<section><span>理周投研部</span><time>2026/10/8</time></section>'
        '<div class="div_Article_Info" itemprop="articleBody"><div>查無資料</div>'
        '<a href="/_Article?AID=123456">閱讀全文</a></div>'
        '<aside>PRIVATE PROSE</aside></main></body></html>').encode()


def seed(queue, settings, *, url=MAGAZINE, location=None):
    submitted = submit_geometry(queue, settings, url)
    response = (Response(url, 302, {'location': location}, b'PRIVATE REDIRECT') if location is not None
        else Response(url, 200, {'content-type': 'text/html'}, template_page()))
    http = transport(queue, response=response)
    job = claim(queue, settings, http)
    jobs.finish_candidate(queue, job, result=jobs.fetch_candidate(http, job))
    assert jobs.candidate_status(queue.root, submitted['request_id'])['state'] == 'observed'
    return submitted


def submit(queue, settings, url=MAGAZINE):
    return jobs.submit_candidate(queue.root, settings, queue.registry, url, LAYOUT,
        reason='Different unresolved header and withheld-Location metadata, not another geometry request',
        evidence_contract_version=66)


def test_prior_response_is_reused_and_exact_head_is_never_requeued(queue, settings):
    seed(queue, settings)
    before = heads(queue)
    request = submit(queue, settings)
    http = transport(queue, response=Response(MAGAZINE, 200, {'content-type': 'text/html'}, template_page()))
    job = claim(queue, settings, http)
    assert job is not None
    result = jobs.fetch_candidate(http, job)
    assert http.calls == [(MAGAZINE, False)] and template_result(result, MAGAZINE)
    assert result['question'] == QUESTION and result['structure']['global_permitted_author_present']
    assert result['structure']['bodies'][0]['notice_codes'] == ['no_data', 'read_full']
    assert result['structure']['body_links'][0]['declaration']['literal_url'].endswith('/_Article?AID=123456')
    assert any(n['date_shape'] for n in result['structure']['native_leaf_metadata'])
    assert any(n['permitted_author_exact'] for n in result['structure']['native_leaf_metadata'])
    assert 'PRIVATE' not in json.dumps(result) and '理周投研部' not in json.dumps(result)
    jobs.finish_candidate(queue, job, result=result)
    assert jobs.candidate_status(queue.root, request['request_id'])['state'] == 'observed'
    assert heads(queue) == before and not list(queue.root.rglob('*.parquet'))
    assert not submit(queue, settings)['submitted'] and claim(queue, settings, http) is None


def test_no_prior_or_already_explained_prior_cannot_buy_another_get(queue, settings):
    before = heads(queue)
    with pytest.raises(ValueError, match='closed_unresolved_moneyweekly_template_required'):
        submit(queue, settings)
    seed(queue, settings, url=FN, location='/ArticleData/Info/Article/123456')
    with pytest.raises(ValueError, match='closed_unresolved_moneyweekly_template_required'):
        submit(queue, settings, FN)
    assert heads(queue) == before


@pytest.mark.parametrize('mutation', ['head', 'source', 'state', 'result', 'request'])
def test_changed_prior_proof_is_rejected(queue, settings, mutation):
    seed(queue, settings)
    with queue.db:
        if mutation == 'head':
            queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?', (MAGAZINE,))
        elif mutation == 'state':
            queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome'")
        elif mutation == 'source':
            queue.registry.sources['moneyweekly'] = replace(queue.registry.sources['moneyweekly'], data_fingerprint='a'*64)
        else:
            row = queue.db.execute('SELECT * FROM native_candidate_jobs').fetchone()
            field = 'request_json' if mutation == 'request' else 'result_json'
            obj = json.loads(row[field]); obj['provider' if mutation == 'result' else 'reason'] = 'changed'
            queue.db.execute(f'UPDATE native_candidate_jobs SET {field}=?', (json.dumps(obj),))
    with pytest.raises(ValueError):
        submit(queue, settings)


@pytest.mark.parametrize('location,retained', [
    ('/ArticleData/Info/Article/123456?Type=0&Line=Include_FirstNews_159', True),
    ('/ArticleData/Info/Article/123456?token=PRIVATE', False),
    ('https://user:PRIVATE@www.moneyweekly.com.tw/ArticleData/Info/Article/123456', False),
    ('https://evil.test/PRIVATE', False),
    ('/ArticleData/Info/Article/123456#PRIVATE', False),
])
def test_withheld_redirect_is_classified_and_never_followed(queue, settings, location, retained):
    seed(queue, settings, url=FN, location=location)
    before = heads(queue)
    request = submit(queue, settings, FN)
    http = transport(queue, response=Response(FN, 302, {'location': location}, b'PRIVATE'))
    job = claim(queue, settings, http)
    result = jobs.fetch_candidate(http, job)
    assert template_result(result, FN)
    assert (result['structure']['literal_url'] is not None) is retained
    assert 'PRIVATE' not in json.dumps(result) and http.calls == [(FN, False)]
    assert not result['destination_requested'] and not result['identity_or_alias_accepted']
    jobs.finish_candidate(queue, job, result=result)
    assert jobs.candidate_status(queue.root, request['request_id'])['state'] == 'observed'
    assert heads(queue) == before


@pytest.mark.parametrize('change', [
    {'structure': {'kind': 'template', 'body': 'PRIVATE PROSE'}},
    {'raw_or_names_or_body_retained': True}, {'accepted_as_article': True}, {'new': 'PRIVATE'},
    {'timing': {'unexpected': 'PRIVATE'}},
    {'timing': dict.fromkeys(('http_seconds', 'parse_seconds', 'scope_seconds', 'total_seconds'), float('inf'))},
    {'timing': dict.fromkeys(('http_seconds', 'parse_seconds', 'scope_seconds', 'total_seconds'), True)},
])
def test_arbitrary_result_cannot_cross_the_durable_boundary(queue, settings, change):
    seed(queue, settings)
    submit(queue, settings)
    http = transport(queue, response=Response(MAGAZINE, 200, {'content-type': 'text/html'}, template_page()))
    job = claim(queue, settings, http)
    result = jobs.fetch_candidate(http, job)
    result.update(change)
    assert not template_result(result, MAGAZINE)
    jobs.finish_candidate(queue, job, result=result)
    row = jobs.candidate_status(queue.root, job['request_id'])
    assert row['state'] == 'observed_error' and 'PRIVATE' not in json.dumps(row)


def test_new_question_does_not_change_old_question_key_or_make_failure_counters_a_retry(queue, settings):
    seed(queue, settings)
    target = jobs._head(queue.db.execute('SELECT * FROM articles WHERE url=?', (MAGAZINE,)).fetchone())
    assert jobs._key(target, 'a'*64, evidence_contract_version=66) == jobs._key(
        dict(target, attempts=90), 'b'*64, evidence_contract_version=66)
    assert jobs._key(target, 'a'*64, evidence_contract_version=66) != jobs._key(target, 'a'*64, evidence_contract_version=65)


def test_404_stays_an_error_and_does_not_rearm(queue, settings):
    seed(queue, settings)
    submitted = submit(queue, settings)
    job = claim(queue, settings, transport(queue))
    with pytest.raises(NewsError, match='http_404') as exc:
        jobs.fetch_candidate(transport(queue, response=Response(MAGAZINE, 404, {}, b'PRIVATE')), job)
    jobs.finish_candidate(queue, job, error=exc.value)
    assert jobs.candidate_status(queue.root, submitted['request_id'])['state'] == 'observed_error'
    assert not submit(queue, settings)['submitted']
