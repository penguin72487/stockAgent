"""A distinct finite excerpt reuses closed geometry and template evidence."""
from dataclasses import replace
import hashlib
import json
import time

import pytest

from downloader import news_fulltext_candidate_diagnostics as jobs
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_moneyweekly_excerpt import (
    LAYOUT, QUESTION, _public_location, excerpt_metadata, excerpt_result,
)
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import NewsQueue
from test_news_moneyweekly_body_geometry import (
    settings, queue, submit as submit_geometry, transport, claim, heads, pump, MAGAZINE, FN,
)
from test_news_moneyweekly_template_metadata import submit as submit_template


def page():
    return ('<html><head><meta charset="utf-8"></head><body><main>'
        '<div>分類</div><div><h1>PRIVATE HEADLINE</h1></div>'
        '<div>2026/10/8 理周投研部</div><div>原生標籤</div>'
        '<div class="div_Article_Info" itemprop="articleBody">'
        '<div>合成短前文</div><div><a href="/Special/not-an-article">合成提示</a></div></div>'
        '</main><aside>PRIVATE FULL PROSE</aside></body></html>').encode()


def response(url=MAGAZINE, *, location='/ArticleData/Info/Article/123456?via=FN'):
    return (Response(FN, 302, {'location': location}, b'PRIVATE REDIRECT') if url == FN
        else Response(MAGAZINE, 200, {'content-type': 'text/html'}, page()))


def seed(queue, settings, url=MAGAZINE, *, location='/ArticleData/Info/Article/123456?via=FN'):
    requests = []
    for submitter in (submit_geometry, submit_template):
        submitted = submitter(queue, settings, url)
        http = transport(queue, response=response(url, location=location))
        job = claim(queue, settings, http)
        jobs.finish_candidate(queue, job, result=jobs.fetch_candidate(http, job))
        assert jobs.candidate_status(queue.root, submitted['request_id'])['state'] == 'observed'
        requests.append(submitted)
    return requests


def submit(queue, settings, url=MAGAZINE):
    return jobs.submit_candidate(queue.root, settings, queue.registry, url, LAYOUT,
        reason='Literal short native notice or tiny public navigation query after closed template evidence',
        evidence_contract_version=67)


@pytest.mark.parametrize('url', [MAGAZINE, FN])
def test_canonical_owner_records_once_without_accepting_a_body_or_destination(queue, settings, url):
    old = seed(queue, settings, url)
    before = heads(queue)
    budgets = list(map(tuple, queue.db.execute('SELECT * FROM queue_settings')))
    submitted = submit(queue, settings, url)
    http = transport(queue, response=response(url))
    job = claim(queue, settings, http)
    result = jobs.fetch_candidate(http, job)
    assert excerpt_result(result, url) and result['question'] == QUESTION
    assert http.calls == [(url, False)]
    assert not any(result[k] for k in ('accepted_as_article', 'body_completeness_verified',
        'identity_or_alias_accepted', 'author_or_publication_accepted', 'destination_requested',
        'raw_response_retained', 'full_body_retained'))
    if url == MAGAZINE:
        assert result['header']['text'] == '2026/10/8 理周投研部'
        assert result['body']['text'] == '合成短前文\n合成提示'
        assert [v['text'] for v in result['children']] == ['合成短前文', '合成提示']
        assert result['diagnostic_excerpt_retained'] and result['possibly_prose']
    else:
        assert result['declaration']['key'] == 'via' and result['declaration']['value'] == 'FN'
        assert result['declaration']['literal_url'].endswith('/Article/123456?via=FN')
        assert not result['raw_or_names_or_body_retained']
    assert 'PRIVATE' not in json.dumps(result) and '/Special/' not in json.dumps(result)
    jobs.finish_candidate(queue, job, result=result)
    pump(queue)
    status = jobs.candidate_status(queue.root, submitted['request_id'])
    assert status['state'] == 'observed'
    assert status['raw_or_names_or_body_retained'] is (url == MAGAZINE)
    archived = json.loads((queue.root/'reviewed_repairs/candidate_diagnostics/processed'/
        (submitted['request_id']+'.outcome.json')).read_bytes())
    assert archived['raw_or_names_or_body_retained'] is (url == MAGAZINE)
    assert heads(queue) == before and list(map(tuple, queue.db.execute('SELECT * FROM queue_settings'))) == budgets
    assert not queue.db.execute('SELECT 1 FROM accepted_batches').fetchone()
    assert not list(queue.root.rglob('*.parquet'))
    assert not submit(queue, settings, url)['submitted'] and claim(queue, settings, http) is None
    assert all(jobs.candidate_status(queue.root, r['request_id'])['state'] == 'observed' for r in old)


@pytest.mark.parametrize('mutation', ['missing', 'geometry_only', 'state', 'head', 'source', 'result', 'request'])
def test_only_an_exact_closed_unresolved_v66_question_is_eligible(queue, settings, mutation):
    if mutation == 'geometry_only':
        request = submit_geometry(queue, settings)
        http = transport(queue, response=response())
        job = claim(queue, settings, http)
        jobs.finish_candidate(queue, job, result=jobs.fetch_candidate(http, job))
    elif mutation != 'missing':
        requests = seed(queue, settings)
        rid = requests[-1]['request_id']
        with queue.db:
            if mutation == 'head': queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?', (MAGAZINE,))
            elif mutation == 'source':
                queue.registry.sources['moneyweekly'] = replace(queue.registry.sources['moneyweekly'], data_fingerprint='a'*64)
            elif mutation == 'state':
                queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?", (rid,))
            else:
                field = 'request_json' if mutation == 'request' else 'result_json'
                row = queue.db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (rid,)).fetchone()
                value = json.loads(row[field]); value['reason' if mutation == 'request' else 'question'] = 'changed'
                queue.db.execute(f'UPDATE native_candidate_jobs SET {field}=? WHERE request_id=?', (json.dumps(value), rid))
    before = heads(queue)
    with pytest.raises(ValueError, match='closed_unresolved_moneyweekly_excerpt_required'):
        submit(queue, settings)
    assert heads(queue) == before


@pytest.mark.parametrize('reference', [
    '/ArticleData/Info/Article/123456?token=AB', '/ArticleData/Info/Article/123456?session_id=AB',
    '/ArticleData/Info/Article/123456?code=AB', '/ArticleData/Info/Article/123456?via=PRIVATE',
    '/ArticleData/Info/Article/123456?via=AB&other=CD', '/ArticleData/Info/Article/123456?via=',
    '/ArticleData/Info/Article/123456?via=AB#PRIVATE', 'https://evil.test/ArticleData/Info/Article/123456?via=AB',
    'https://user:PRIVATE@www.moneyweekly.com.tw/ArticleData/Info/Article/123456?via=AB',
    '/ArticleData/Info/Article/0123456?via=AB', '/Account/Login?via=AB',
])
def test_credential_unknown_or_foreign_locations_are_only_hashed(queue, reference):
    result = excerpt_metadata(response(FN, location=reference), queue.registry.sources['moneyweekly'], FN)
    assert result['declaration'] is None and excerpt_result(result, FN)
    assert result['location_raw_sha256'] == hashlib.sha256(reference.encode()).hexdigest()
    assert 'PRIVATE' not in json.dumps(result)


def test_only_literal_same_source_public_query_reference_is_retained():
    reference = '/ArticleData/Info/Article/123456?via=FN'
    assert _public_location(reference, FN) == dict(raw_reference=reference,
        literal_url='https://www.moneyweekly.com.tw'+reference, key='via', value='FN')


@pytest.mark.parametrize('mutation', ['long_header', 'long_body', 'hidden', 'hidden_child', 'image', 'script', 'one_child', 'wrong_wrapper'])
def test_excerpt_cannot_expand_into_arbitrary_prose_or_hidden_media(queue, mutation):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(page(), 'lxml')
    body = soup.select_one('[itemprop=articleBody]')
    if mutation == 'long_header': soup.main.find_all('div', recursive=False)[2].string = '字'*129
    elif mutation == 'long_body': body.div.string = '字'*100
    elif mutation == 'hidden': body['hidden'] = ''
    elif mutation == 'hidden_child': body.div['style'] = 'display: none'
    elif mutation in {'image', 'script'}: body.append(soup.new_tag('img' if mutation == 'image' else 'script'))
    elif mutation == 'one_child': body.div.decompose()
    else: soup.h1.unwrap()
    with pytest.raises(NewsError):
        excerpt_metadata(Response(MAGAZINE, 200, {'content-type':'text/html'}, str(soup).encode()),
            queue.registry.sources['moneyweekly'], MAGAZINE)


@pytest.mark.parametrize('change', [dict(status=403), dict(status=429), dict(status=500), dict(status=404),
    dict(body=b'x'*((2<<20)+1)), dict(url=MAGAZINE+'0'), dict(headers={'content-type':'application/json'})])
def test_only_bounded_literal_successes_can_release_an_excerpt(queue, change):
    original = response()
    with pytest.raises(NewsError):
        excerpt_metadata(replace(original, **change), queue.registry.sources['moneyweekly'], MAGAZINE)


@pytest.mark.parametrize('mutation', ['header', 'hash', 'body', 'identity', 'author', 'question', 'unknown', 'timing', 'boolean_timing', 'boolean_version'])
def test_writer_rejects_forged_excerpt_grants_and_unbounded_fields(queue, settings, mutation):
    seed(queue, settings)
    submitted = submit(queue, settings)
    http = transport(queue, response=response())
    job = claim(queue, settings, http)
    result = jobs.fetch_candidate(http, job)
    if mutation == 'header': result['header']['text'] = '字'*129
    elif mutation == 'hash': result['body']['text_sha256'] = 'a'*64
    elif mutation == 'body': result['full_body_retained'] = True
    elif mutation == 'identity': result['identity_or_alias_accepted'] = True
    elif mutation == 'author': result['author_or_publication_accepted'] = True
    elif mutation == 'question': result['question'] = 'different'
    elif mutation == 'unknown': result['extra'] = 'PRIVATE FULL PROSE'
    elif mutation == 'boolean_version': result['contract_version'] = True
    else: result['timing'] = dict.fromkeys(('http_seconds','parse_seconds','scope_seconds','total_seconds'),
        float('inf') if mutation == 'timing' else True)
    assert not excerpt_result(result, MAGAZINE)
    jobs.finish_candidate(queue, job, result=result)
    status = jobs.candidate_status(queue.root, submitted['request_id'])
    assert status['state'] == 'observed_error' and not status['diagnostic_excerpt_retained']
    assert 'PRIVATE FULL PROSE' not in json.dumps(status)


def test_public_location_result_cannot_be_forged(queue):
    result = excerpt_metadata(response(FN), queue.registry.sources['moneyweekly'], FN)
    result['declaration']['value'] = 'XX'
    assert not excerpt_result(result, FN)


@pytest.mark.parametrize('mutation', ['lease', 'due', 'head', 'body', 'prior'])
def test_dispatch_rechecks_original_guards_without_sending_a_request(queue, settings, mutation):
    prior = seed(queue, settings)[-1]
    submit(queue, settings); pump(queue)
    with queue.db:
        if mutation == 'lease': queue.db.execute('UPDATE articles SET lease_owner=?,lease_until=?', ('other', time.time()+600))
        elif mutation == 'due': queue.db.execute('UPDATE articles SET due=?', (time.time()+600,))
        elif mutation == 'head': queue.db.execute('UPDATE articles SET attempts=attempts+1')
        elif mutation == 'body': queue.db.execute('UPDATE articles SET body_sha256=?', ('a'*64,))
        else: queue.db.execute("UPDATE native_candidate_jobs SET state='uncertain_outcome' WHERE request_id=?", (prior['request_id'],))
    http = transport(queue, response=response())
    assert jobs.claim_candidate(queue, http, settings, 'moneyweekly') is None and not http.calls


@pytest.mark.parametrize('url', [MAGAZINE, FN])
def test_uncertain_outcome_retention_is_truthful_and_not_automatically_repeated(queue, settings, url):
    seed(queue, settings, url)
    submitted = submit(queue, settings, url)
    http = transport(queue, response=response(url))
    claim(queue, settings, http)
    queue.close(); owner = NewsQueue(queue.root, queue.registry)
    try:
        pump(owner)
        status = jobs.candidate_status(owner.root, submitted['request_id'])
        assert status['state'] == 'uncertain_outcome'
        assert status['raw_or_names_or_body_retained'] is (None if url == MAGAZINE else False)
        assert not submit(owner, settings, url)['submitted']
        assert jobs.claim_candidate(owner, http, settings, 'moneyweekly') is None and not http.calls
    finally: owner.close()


def test_failed_excerpt_is_closed_and_key_never_depends_on_counters(queue, settings):
    seed(queue, settings)
    submitted = submit(queue, settings)
    job = claim(queue, settings, transport(queue))
    jobs.finish_candidate(queue, job, error=NewsError('http_404'))
    assert jobs.candidate_status(queue.root, submitted['request_id'])['state'] == 'observed_error'
    assert not submit(queue, settings)['submitted']
    target = jobs._head(queue.db.execute('SELECT * FROM articles WHERE url=?', (MAGAZINE,)).fetchone())
    assert jobs._key(target, 'a'*64, evidence_contract_version=67) == jobs._key(
        dict(target, attempts=999), 'b'*64, evidence_contract_version=67)
    assert jobs._key(target, 'a'*64, evidence_contract_version=67) != jobs._key(target, 'a'*64, evidence_contract_version=66)
