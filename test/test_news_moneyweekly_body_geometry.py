"""One fixed metadata observation of unresolved native bodies and namespaces."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import quote
from unittest.mock import patch

import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidates import candidate_evidence_allowed
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_moneyweekly import (
    BODY_GEOMETRY_LAYOUT, BODY_GEOMETRY_QUESTION, ROUTE_METADATA_LAYOUT, LEGACY_ARTICLE_PATTERN,
    moneyweekly_body_geometry, moneyweekly_body_geometry_result, moneyweekly_body_geometry_route,
)
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, _plain_text_node
from downloader.news_fulltext_queue import NewsQueue
from test_news_moneyweekly_native_routes import native_config
from test_news_reviewed_author_scope import reviewed_moneyweekly_config

BASE = 'https://www.moneyweekly.com.tw'
MAGAZINE = BASE + '/Magazine/Info/' + quote('理財周刊') + '/237193'
NEWS = BASE + '/ArticleData/Info/' + quote('理周新聞') + '/123456'
OPINION = BASE + '/ArticleData/Info/' + quote('觀點文章') + '/123457'
EXPLAIN = BASE + '/ArticleData/Info/' + quote('說解財經大小事') + '/123458'
FN = BASE + '/Home/ChangePage/FN/143434/?Type=0&Line=Include_FirstNews_159&Page=' + 'A0'*300
URLS = [MAGAZINE, NEWS, OPINION, EXPLAIN, FN, NEWS.replace('https:', 'http:')]


@pytest.fixture
def settings(tmp_path):
    value = native_config()
    value.update(root=str(tmp_path), exclusive_http_owner=False, min_free_disk_bytes=1)
    return value


@pytest.fixture
def queue(settings, tmp_path):
    value = NewsQueue(tmp_path, PolicyRegistry(settings, base=Path.cwd()))
    value.add_articles([{'url': url} for url in URLS])
    with value.db:
        value.db.execute("UPDATE articles SET state='unsupported_article_path',error_code='unsupported_article_path',"
            "attempts=0,due=0,last_checked_at_utc=NULL")
        value.db.execute("UPDATE articles SET state='body_not_found',error_code='body_not_found',attempts=2,"
            "last_checked_at_utc='2026-01-01T00:00:00+00:00' WHERE url=?", (MAGAZINE,))
    yield value
    value.close()


def heads(queue):
    return [dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')]


def submit(queue, settings, url=MAGAZINE):
    return diagnostic.submit_candidate(queue.root, settings, queue.registry, url, BODY_GEOMETRY_LAYOUT,
        reason='Explain the unresolved literal native editor without retaining prose', evidence_contract_version=65)


def envelope(url, body=None, *, status=200):
    return Response(url, status, {'content-type': 'text/html; charset=utf-8'}, body or (
        f'<html><head><meta charset="utf-8"><link rel="canonical" href="{url}">'
        f'<meta property="og:url" content="{url}"></head><body><h1>PRIVATE HEADLINE</h1>'
        '<div><div>2026/10/8</div><div>理周投研部</div></div>'
        '<div class="div_Article_Info" itemprop="articleBody"><p>PRIVATE PROSE</p>'
        '<p hidden>HIDDEN PROSE</p><p><img src="private-image" alt="PRIVATE ALT"></p>'
        '<div data-widget_id="ad">PRIVATE AD</div><script>PRIVATE SCRIPT</script></div>'
        '</body></html>').encode())


def transport(queue, *, response=None, error=None):
    calls = []
    def get(url, *, follow_redirects=True):
        calls.append((url, follow_redirects))
        if error:
            raise error
        return response or envelope(url)
    return SimpleNamespace(registry=queue.registry, get=get, calls=calls, provider_not_before={},
        blocked_gateway_hosts=lambda p: (), blocked_robots_origins=lambda p: ())


def pump(queue):
    # These fixtures verify admission semantics; scheduler deadlines have their
    # own tests and must not make setup depend on the host's I/O latency.
    with patch.object(diagnostic.time, 'monotonic', return_value=0.0):
        return diagnostic.pump_candidates(queue)


def claim(queue, settings, http):
    pump(queue)
    return diagnostic.claim_candidate(queue, http, settings, 'moneyweekly')


@pytest.mark.parametrize('url', URLS)
def test_original_query_and_http_scheme_are_observed_once_without_head_change(queue, settings, url):
    before = heads(queue)
    request = submit(queue, settings, url)
    http = transport(queue)
    job = claim(queue, settings, http)
    result = diagnostic.fetch_candidate(http, job)
    assert http.calls == [(url, False)]
    assert moneyweekly_body_geometry_result(result, url)
    assert result['question'] == BODY_GEOMETRY_QUESTION
    assert result['requested_url'] == result['final_url'] == url
    assert not any(text in json.dumps(result) for text in ('PRIVATE', 'HIDDEN', '理周投研部', 'private-image'))
    diagnostic.finish_candidate(queue, job, result=result)
    assert diagnostic.candidate_status(queue.root, request['request_id'])['state'] == 'observed'
    assert heads(queue) == before and not list(queue.root.rglob('*.parquet'))
    assert not submit(queue, settings, url)['submitted']
    assert claim(queue, settings, http) is None and len(http.calls) == 1


def test_body_shape_counts_and_hashes_use_the_existing_text_rule(queue):
    from bs4 import BeautifulSoup
    response = envelope(MAGAZINE)
    source = queue.registry.sources['moneyweekly']
    result = moneyweekly_body_geometry(response, source, MAGAZINE)
    html = result['html']
    body = html['bodies'][0]
    original = BeautifulSoup(response.body, source.html_parser).select_one(source.selectors[0])
    text = _plain_text_node(original, source.exclude_selectors, parser=source.html_parser)
    assert body['text_characters'] == len(text)
    assert body['text_sha256'] == hashlib.sha256(text.encode()).hexdigest()
    assert body['paragraph_count'] == 3 and body['image_count'] == body['script_count'] == 1
    assert body['paragraphs'][1]['inline_hidden_hint']
    assert html['author_matches_permitted'] and html['publication_day_valid']
    assert html['canonical_declarations'][0]['url'] == MAGAZINE
    assert not result['accepted_as_article'] and not result['body_completeness_verified']


@pytest.mark.parametrize('url', [
    MAGAZINE+'?token=secret', MAGAZINE+'#fragment', MAGAZINE.replace('/237193', '/0237193'),
    MAGAZINE.replace('www.', ''), NEWS.replace('moneyweekly.com.tw', 'evil.test'),
    NEWS.replace('/Info/', '/List/'), NEWS.replace(quote('理周新聞'), quote('其他分類')),
    NEWS.replace('123456', '1%2f2'), NEWS.replace('https://', 'https://secret@'),
    FN+'&token=secret', FN.replace('Type=0', 'Type=1'), FN.replace('A0'*300, 'public-prose'),
    FN.replace('&Line=', '&line='), BASE+'/_Article?AID=123456',
])
def test_route_scope_does_not_decode_navigation_or_guess_other_views(url):
    assert not moneyweekly_body_geometry_route(url)


@pytest.mark.parametrize('location', [NEWS, '/ArticleData/Info/Article/123456'])
def test_redirect_is_metadata_only_and_never_fetched(queue, settings, location):
    request = submit(queue, settings)
    response = Response(MAGAZINE, 302, {'location': location}, b'PRIVATE REDIRECT BODY')
    http = transport(queue, response=response)
    job = claim(queue, settings, http)
    result = diagnostic.fetch_candidate(http, job)
    assert result['html'] is None and result['location']
    assert not result['destination_requested'] and 'PRIVATE' not in json.dumps(result)
    diagnostic.finish_candidate(queue, job, result=result)
    assert diagnostic.candidate_status(queue.root, request['request_id'])['state'] == 'observed'
    assert http.calls == [(MAGAZINE, False)]


@pytest.mark.parametrize('url', ['https://user:secret@evil.test/path', 'https://evil.test/private',
    BASE+'/Account/Login?token=secret', NEWS+'?private=secret'])
def test_unreviewed_canonical_and_location_strings_are_hashed_and_withheld(queue, url):
    body = f'<html><head><link rel="canonical" href="{url}"></head><body></body></html>'.encode()
    source = queue.registry.sources['moneyweekly']
    result = moneyweekly_body_geometry(envelope(MAGAZINE, body), source, MAGAZINE)
    assert result['html']['canonical_declarations'][0] == dict(url=None, raw_sha256=hashlib.sha256(url.encode()).hexdigest())
    redirect = moneyweekly_body_geometry(Response(MAGAZINE, 302, {'location': url}, b''), source, MAGAZINE)
    assert redirect['location'] is None and moneyweekly_body_geometry_result(redirect, MAGAZINE)
    assert 'secret' not in json.dumps(result) + json.dumps(redirect)


@pytest.mark.parametrize('change', [
    {'last_checked_at_utc': None}, {'body_sha256': 'a'*64}, {'due': 9999999999},
    {'lease_owner': 'other', 'lease_until': 9999999999},
    {'state': 'article_rights_or_scope_excluded', 'error_code': 'article_rights_or_scope_excluded'},
])
def test_geometry_cannot_requeue_protected_or_unknown_body_failures(queue, settings, change):
    with queue.db:
        queue.db.execute('UPDATE articles SET '+','.join(key+'=?' for key in change)+' WHERE url=?', (*change.values(), MAGAZINE))
    before = heads(queue)
    with pytest.raises(ValueError):
        submit(queue, settings)
    assert heads(queue) == before


def test_fixed_question_is_separate_from_closed_first_hop_and_does_not_depend_on_parser(queue, settings):
    before = heads(queue)
    source = queue.registry.sources['moneyweekly']
    target = diagnostic._head(queue.db.execute('SELECT * FROM articles WHERE url=?', (MAGAZINE,)).fetchone())
    assert diagnostic._key(target, 'a'*64, evidence_contract_version=65) == diagnostic._key(
        dict(target, attempts=19), 'b'*64, evidence_contract_version=65)
    assert diagnostic._key(target, 'a'*64, evidence_contract_version=65) != diagnostic._key(target, 'a'*64, evidence_contract_version=64)
    assert candidate_evidence_allowed(source, source, 65)
    old_config = reviewed_moneyweekly_config()
    next(entry for entry in old_config['providers'] if entry['id'] == 'moneyweekly')['article_path_pattern'] = LEGACY_ARTICLE_PATTERN
    old = PolicyRegistry(old_config, base=Path.cwd()).sources['moneyweekly']
    assert not candidate_evidence_allowed(old, old, 65)
    assert not candidate_evidence_allowed(source, replace(source, permitted_author_names=('unknown',)), 65)
    with pytest.raises(ValueError):
        diagnostic.submit_candidate(queue.root, settings, queue.registry, MAGAZINE, ROUTE_METADATA_LAYOUT,
            reason='Wrong question and layout pair', evidence_contract_version=65)
    assert heads(queue) == before


@pytest.mark.parametrize('field', ['top', 'body', 'paragraph', 'timing', 'tag', 'false_flag', 'url'])
def test_result_boundary_rejects_prose_unknown_fields_or_forged_claims(queue, settings, field):
    request = submit(queue, settings)
    http = transport(queue)
    job = claim(queue, settings, http)
    result = diagnostic.fetch_candidate(http, job)
    if field == 'top': result['prose'] = 'PRIVATE LEAK'
    if field == 'body': result['html']['bodies'][0]['prose'] = 'PRIVATE LEAK'
    if field == 'paragraph': result['html']['bodies'][0]['paragraphs'][0]['text'] = 'PRIVATE LEAK'
    if field == 'timing': result['timing']['prose'] = 'PRIVATE LEAK'
    if field == 'tag': result['html']['bodies'][0]['tag'] = 'PRIVATE LEAK'
    if field == 'false_flag': result['body_completeness_verified'] = True
    if field == 'url': result['html']['canonical_declarations'][0]['url'] = BASE+'/private?prose=PRIVATE-LEAK'
    diagnostic.finish_candidate(queue, job, result=result)
    saved = diagnostic.candidate_status(queue.root, request['request_id'])
    assert saved['state'] == 'observed_error' and saved['result_json']['error'] == 'invalid_candidate_result'
    assert 'PRIVATE' not in json.dumps(saved)


def test_owner_restart_and_source_error_consume_question_without_retry(queue, settings):
    before = heads(queue)
    first = submit(queue, settings)
    http = transport(queue)
    assert claim(queue, settings, http)
    queue.owner = 'new-owner'
    queue.native_candidate_schema_ready = False
    diagnostic.pump_candidates(queue)
    assert diagnostic.candidate_status(queue.root, first['request_id'])['state'] == 'uncertain_outcome'
    assert not submit(queue, settings)['submitted']
    assert claim(queue, settings, http) is None and not http.calls
    second = submit(queue, settings, NEWS)
    job = claim(queue, settings, http)
    diagnostic.finish_candidate(queue, job, error=NewsError('source_access_denied'))
    saved = diagnostic.candidate_status(queue.root, second['request_id'])
    assert saved['state'] == 'observed_error' and saved['result_json']['error'] == 'source_access_denied'
    assert heads(queue) == before


def test_source_backoff_and_head_race_do_not_dispatch(queue, settings):
    submit(queue, settings)
    http = transport(queue)
    http.provider_not_before['moneyweekly'] = 9999999999
    assert claim(queue, settings, http) is None and not http.calls
    http.provider_not_before.clear()
    with queue.db:
        queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?', (MAGAZINE,))
    assert claim(queue, settings, http) is None and not http.calls


@pytest.mark.parametrize('status', [403, 404, 429, 500])
def test_http_refusals_are_errors_not_geometry(queue, status):
    with pytest.raises(NewsError, match='http_404' if status == 404 else 'literal_moneyweekly_body_geometry_required'):
        moneyweekly_body_geometry(envelope(MAGAZINE, status=status), queue.registry.sources['moneyweekly'], MAGAZINE)


def test_actual_404_is_durable_source_error_and_does_not_rearm_a_closed_question(queue, settings):
    before = heads(queue)
    request = submit(queue, settings)
    http = transport(queue, response=envelope(MAGAZINE, status=404))
    job = claim(queue, settings, http)
    with pytest.raises(NewsError, match='http_404') as failure:
        diagnostic.fetch_candidate(http, job)
    diagnostic.finish_candidate(queue, job, error=failure.value)
    status = diagnostic.candidate_status(queue.root, request['request_id'])
    assert status['state'] == 'observed_error' and status['result_json']['error'] == 'http_404'
    assert not submit(queue, settings)['submitted']
    assert claim(queue, settings, http) is None and http.calls == [(MAGAZINE, False)]
    assert heads(queue) == before and not list(queue.root.rglob('*.parquet'))
