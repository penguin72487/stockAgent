"""Known legacy routes get one source observation, never implicit admission."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import time

import pytest

from downloader import news_fulltext_candidate_diagnostics as diagnostic
from downloader.news_fulltext_candidates import candidate_evidence_allowed, candidate_response_metadata
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_moneyweekly import (
    ROUTE_METADATA_LAYOUT as LAYOUT, ROUTE_METADATA_QUESTION,
    moneyweekly_literal_unhandled_route, moneyweekly_route_metadata,
)
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_reviewed_author_scope import reviewed_moneyweekly_config

URL = 'https://www.moneyweekly.com.tw/_Article?AID=246465'
MAGAZINE = 'https://www.moneyweekly.com.tw/Magazine/Info/%E7%90%86%E8%B2%A1%E5%91%A8%E5%88%8A/257127'
DESTINATION = 'https://www.moneyweekly.com.tw/ArticleData/Info/Article/246465'


@pytest.fixture
def settings(tmp_path):
    config = reviewed_moneyweekly_config()
    config.update(root=str(tmp_path), exclusive_http_owner=False, min_free_disk_bytes=1)
    return config


@pytest.fixture
def queue(settings, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=Path.cwd()))
    queue.add_articles([{'url': URL}, {'url': MAGAZINE}])
    with queue.db:
        queue.db.execute("UPDATE articles SET state='unsupported_article_path',"
            "error_code='unsupported_article_path',attempts=1,due=0,last_checked_at_utc=?",
            ('2026-01-01T00:00:00+00:00',))
    yield queue
    queue.close()


def head(queue, url=URL):
    return dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())


def submit(queue, settings, url=URL, reason='Observe only one unresolved literal source route'):
    return diagnostic.submit_candidate(queue.root, settings, queue.registry, url, LAYOUT,
        reason=reason, evidence_contract_version=64)


def http(queue, *, envelope=None, error=None):
    calls = []
    def get(url, *, follow_redirects=True):
        calls.append((url, follow_redirects))
        if error:
            raise error
        return envelope or Response(url, 302, {'location': DESTINATION}, b'not an article')
    return SimpleNamespace(registry=queue.registry, get=get, calls=calls,
        provider_not_before={}, blocked_gateway_hosts=lambda p: (), blocked_robots_origins=lambda p: ())


def claim(queue, settings, transport):
    diagnostic.pump_candidates(queue)
    return diagnostic.claim_candidate(queue, transport, settings, 'moneyweekly')


@pytest.mark.parametrize('url', [URL, MAGAZINE])
def test_route_question_never_follows_or_admits_an_article(queue, settings, url):
    before = [head(queue, u) for u in (URL, MAGAZINE)]
    request = submit(queue, settings, url)
    transport = http(queue)
    job = claim(queue, settings, transport)
    result = diagnostic.fetch_candidate(transport, job)
    assert transport.calls == [(url, False)]
    assert result['location'] == DESTINATION and result['question'] == ROUTE_METADATA_QUESTION
    assert result['destination_requested'] is False and result['accepted_as_article'] is False
    assert result['body_completeness_verified'] is False and result['identity_or_alias_accepted'] is False
    assert result['raw_or_names_or_body_retained'] is False and result['raw_or_body_retained'] is False
    assert 'private_original' not in result and 'not an article' not in json.dumps(result)
    diagnostic.finish_candidate(queue, job, result=result)
    diagnostic.pump_candidates(queue)
    assert diagnostic.candidate_status(queue.root, request['request_id'])['state'] == 'observed'
    assert [head(queue, u) for u in (URL, MAGAZINE)] == before
    assert not list(queue.root.rglob('*.parquet'))


@pytest.mark.parametrize('url', [
    URL+'&token=secret', URL.replace('AID=', 'aid='), URL+'#fragment',
    URL.replace('246465', '0246465'), URL.replace('246465', '-1'),
    URL.replace('https:', 'http:'), URL.replace('www.', ''),
    URL.replace('www.moneyweekly.com.tw', 'evil.test'),
    URL.replace('/_Article', '/_Article/'), URL.replace('/_Article', '/%5fArticle'),
    MAGAZINE+'?extra=1', MAGAZINE.replace('/Magazine/', '/Magazine/List/'),
    MAGAZINE.replace('%E7%90%86%E8%B2%A1%E5%91%A8%E5%88%8A', 'OtherMagazine'),
    DESTINATION,
])
def test_fixed_question_does_not_expand_to_other_routes(url):
    assert not moneyweekly_literal_unhandled_route(url)


def test_current_html_keeps_only_header_urls_and_structure(queue):
    payload = (f'<html><head><link rel="canonical" href="{DESTINATION}">'
        f'<meta property="og:url" content="{DESTINATION}"></head>'
        '<body><h1>PRIVATE HEADLINE</h1><div class="div_Article_Info" itemprop="articleBody">'
        '<p>PRIVATE PROSE</p><div>本文限會員登入瀏覽</div></div></body></html>').encode()
    source = queue.registry.sources['moneyweekly']
    facts = candidate_response_metadata(Response(URL, 200, {'content-type':'text/html'}, payload), source, source,
        evidence_contract_version=64, requested_url=URL)
    assert facts['canonical_urls'] == facts['og_urls'] == [DESTINATION]
    assert facts['native_member_mask_present'] and facts['configured_body_count'] == facts['heading_count'] == 1
    assert facts['response_sha256'] == hashlib.sha256(payload).hexdigest()
    assert 'PRIVATE' not in json.dumps(facts) and '本文限會員登入瀏覽' not in json.dumps(facts)
    assert not facts['accepted_as_article'] and not facts['destination_requested']


@pytest.mark.parametrize('location', ['https://user:secret@evil.test/path', 'javascript:secret',
    'https://evil.test/path#secret', 'x'*2049, None])
def test_credentials_nonpublic_or_unbounded_locations_are_withheld(queue, location):
    source = queue.registry.sources['moneyweekly']
    facts = moneyweekly_route_metadata(Response(URL, 302, {'location':location}, b''), source, URL)
    assert facts['location'] is None
    assert 'secret' not in json.dumps(facts)


@pytest.mark.parametrize('status', [403, 404, 429, 500])
def test_refusals_cannot_be_classified_as_successful_route_evidence(queue, status):
    with pytest.raises(NewsError, match='literal_moneyweekly_first_hop_required'):
        moneyweekly_route_metadata(Response(URL, status, {}, b''), queue.registry.sources['moneyweekly'], URL)


def test_completed_question_is_not_rearmed_by_reason_counter_or_candidate_change(queue, settings):
    first = submit(queue, settings)
    transport = http(queue)
    job = claim(queue, settings, transport)
    diagnostic.finish_candidate(queue, job, result=diagnostic.fetch_candidate(transport, job))
    target = diagnostic._head(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    key = diagnostic._key(target, 'a'*64, evidence_contract_version=64)
    target.update(attempts=19, due=time.time()-1)
    assert key == diagnostic._key(target, 'b'*64, evidence_contract_version=64)
    again = submit(queue, settings, reason='A changed reason is not a new question')
    assert again['request_id'] == first['request_id'] and not again['submitted']
    assert claim(queue, settings, transport) is None and len(transport.calls) == 1


@pytest.mark.parametrize('change', [
    {'lease_owner':'other', 'lease_until':9999999999}, {'due':9999999999},
    {'body_sha256':'a'*64}, {'state':'complete', 'error_code':None},
    {'last_checked_at_utc':'not-a-clock'},
])
def test_stored_leased_future_or_unknown_heads_are_protected(queue, settings, change):
    with queue.db:
        queue.db.execute('UPDATE articles SET '+','.join(k+'=?' for k in change)+' WHERE url=?',
            (*change.values(), URL))
    before = head(queue)
    with pytest.raises(ValueError):
        submit(queue, settings)
    assert head(queue) == before


def test_source_pause_prevents_even_metadata_dispatch(queue, settings):
    submit(queue, settings)
    transport = http(queue)
    transport.provider_not_before['moneyweekly'] = time.time()+600
    assert claim(queue, settings, transport) is None and not transport.calls


def test_head_change_between_submission_and_claim_never_dispatches(queue, settings):
    submit(queue, settings)
    with queue.db:
        queue.db.execute('UPDATE articles SET attempts=attempts+1 WHERE url=?', (URL,))
    transport = http(queue)
    assert claim(queue, settings, transport) is None and not transport.calls


def test_crash_after_reservation_is_not_a_network_retry(queue, settings):
    request = submit(queue, settings)
    transport = http(queue)
    assert claim(queue, settings, transport)
    queue.owner = 'a-different-owner'
    queue.native_candidate_schema_ready = False
    diagnostic.pump_candidates(queue)
    assert diagnostic.candidate_status(queue.root, request['request_id'])['state'] == 'uncertain_outcome'
    assert not submit(queue, settings)['submitted']
    assert claim(queue, settings, transport) is None and not transport.calls


def test_metadata_contract_cannot_extend_article_or_provider_permissions(queue, settings):
    source = queue.registry.sources['moneyweekly']
    assert candidate_evidence_allowed(source, source, 64)
    assert not candidate_evidence_allowed(source, source, 1)
    assert not candidate_evidence_allowed(source, replace(source, provider='publisher'), 64)
    assert not candidate_evidence_allowed(source, replace(source, acquisition_mode='finite'), 64)
    assert source.article_path_pattern and not moneyweekly_literal_unhandled_route(DESTINATION)
    with pytest.raises(ValueError, match='unknown_url'):
        submit(queue, settings, URL.replace('246465', '999999999'))


@pytest.mark.parametrize('attempts', [0, 1])
def test_path_classification_without_a_source_check_preserves_its_unknown_clock(queue, settings, attempts):
    with queue.db:
        queue.db.execute('UPDATE articles SET attempts=?,last_checked_at_utc=NULL WHERE url=?', (attempts, URL))
    before=head(queue)
    request=submit(queue, settings)
    transport=http(queue)
    job=claim(queue, settings, transport)
    assert job is not None
    diagnostic.finish_candidate(queue, job, result=diagnostic.fetch_candidate(transport, job))
    assert diagnostic.candidate_status(queue.root,request['request_id'])['state']=='observed'
    assert head(queue)==before and head(queue)['last_checked_at_utc'] is None
