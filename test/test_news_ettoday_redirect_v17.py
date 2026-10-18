"""Synthetic source-redirect evidence; no publisher requests or real bodies."""
from contextlib import closing
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from types import SimpleNamespace

import pytest

from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_ettoday import ettoday_source_redirect_verified
from downloader.news_fulltext_http import RedirectHop, Response
from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_queue import NewsQueue, verify_bodies, utc_now, ettoday_source_redirect_parser_repair
from test_news_fulltext import config, FakeResponse, http_fixture
import test_news_ettoday as native
import test_news_ettoday_clock_v2 as clocks


FINANCE = native.URLS[0]
WWW = native.URLS[1].replace('3254110', '3254747')
OTHER_DAY = WWW.replace('/20261013/', '/20261012/')


def hop(start, end, *, status=301, location=None, body=b''):
    return RedirectHop(start, status, location or end, end, datetime.now(timezone.utc).isoformat(),
                       hashlib.sha256(body).hexdigest(), len(body))


def document(final):
    host = 'www' if 'www.' in final else 'finance'
    return str(native.page(host)).replace(native.URLS[host == 'www'], final).encode()


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('chain', [(FINANCE, WWW), (WWW, FINANCE), (OTHER_DAY, WWW),
    (OTHER_DAY.replace('https:', 'http:'), OTHER_DAY, WWW)])
def test_actual_chain_admits_alias_without_changing_native_values_or_original_request(tmp_path, parser, chain):
    start, final = chain[0], chain[-1]
    response = Response(final, 200, {'content-type': 'text/html'}, document(final),
                        tuple(hop(a, b) for a, b in zip(chain, chain[1:])))
    registry = clocks.registry(17)
    source = replace(registry.sources['ettoday'], html_parser=parser)
    registry.sources['ettoday'] = source
    calls = []
    def get(url, **kwargs):
        calls.append(url)
        return response
    row = {'url': start, 'id': b'1' * 16}
    record = fetch_article(SimpleNamespace(registry=registry, get=get), row)
    previous = extract_article(response.body, replace(source, embedded_article_layout='ettoday_visible_article_v16'), page_url=final)
    assert extract_article(response.body, source, page_url=final) == previous
    assert all(record[key] == value for key, value in previous.items())
    assert calls == [start] and record['requested_url'] == start and record['final_url'] == final
    assert record['source_request_url'] == final and record['raw_response_body'] == response.body
    assert record['url_resolution_basis'].startswith('verified_ettoday_literal_source_redirect_v17;')
    assert json.loads(record['transport_redirects_json']) == [asdict(item) for item in response.redirects]
    assert record['transport_redirect_contract_version'] == 1
    assert record['http_total_response_bytes'] == len(response.body)
    assert not record['publish'] and not record['remote_training_export'] and not record['historical_point_in_time']
    with closing(NewsQueue(tmp_path, registry)) as queue:
        queue.add_articles([{'url': start}])
        claimed = queue.claim_url(start, 300)
        record['article_id'] = claimed['id'].hex()
        record['source_response_article_id'] = claimed['id'].hex()
        queue.accept([record])
        assert verify_bodies(tmp_path)['body_versions_verified'] == 1


@pytest.mark.parametrize('version', range(1, 17))
def test_old_contracts_cannot_accept_cross_route_alias_even_with_source_chain(version):
    response = Response(WWW, 200, {'content-type': 'text/html'}, document(WWW), (hop(FINANCE, WWW),))
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        fetch_article(SimpleNamespace(registry=clocks.registry(version), get=lambda *a, **kw: response),
                      {'url': FINANCE, 'id': b'1' * 16})


@pytest.mark.parametrize('bad', ['absent', 'disconnected', 'location', 'different_id', 'unreviewed_host',
    'bad_query', 'downgrade', 'status', 'hash', 'bytes', 'clock', 'cycle', 'final', 'too_many'])
def test_equal_numeric_id_never_substitutes_for_complete_literal_source_evidence(bad):
    item = hop(FINANCE, WWW)
    final = WWW
    chain = (item,)
    if bad == 'absent': chain = ()
    elif bad == 'disconnected': chain = (replace(item, from_url=OTHER_DAY),)
    elif bad == 'location': chain = (replace(item, location=OTHER_DAY),)
    elif bad == 'different_id': chain = (hop(FINANCE, WWW.replace('3254747', '3254748')),)
    elif bad == 'unreviewed_host': chain = (hop(FINANCE, WWW.replace('www.', 'house.')),)
    elif bad == 'bad_query': chain = (hop(FINANCE, WWW + '?redirect=1'),)
    elif bad == 'downgrade': chain = (hop(FINANCE, WWW.replace('https:', 'http:')),)
    elif bad == 'status': chain = (replace(item, status=200),)
    elif bad == 'hash': chain = (replace(item, response_sha256='unknown'),)
    elif bad == 'bytes': chain = (replace(item, response_bytes=-1),)
    elif bad == 'clock': chain = (replace(item, observed_at_utc='2026-10-18T00:00:00'),)
    elif bad == 'cycle': chain = (item, hop(WWW, FINANCE), item)
    elif bad == 'final': final = OTHER_DAY
    elif bad == 'too_many': chain = (item,) * 6
    response = Response(final, 200, {'content-type': 'text/html'}, document(final), chain)
    assert not ettoday_source_redirect_verified(FINANCE, response)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        fetch_article(SimpleNamespace(registry=clocks.registry(17), get=lambda *a, **kw: response),
                      {'url': FINANCE, 'id': b'1' * 16})


@pytest.mark.parametrize('change', ['canonical', 'author', 'paid', 'body'])
def test_verified_redirect_does_not_weaken_final_article_validation(change):
    soup = native.page('www')
    if change == 'canonical': soup.select_one('link[rel=canonical]')['href'] += '1'
    elif change == 'author': soup.select_one('#credit').decompose()
    elif change == 'body': soup.select_one('.story').decompose()
    else: native.update_schema(soup, isAccessibleForFree=False)
    raw = str(soup).replace(native.URLS[1], WWW).encode()
    response = Response(WWW, 200, {'content-type': 'text/html'}, raw, (hop(FINANCE, WWW),))
    with pytest.raises(NewsError):
        fetch_article(SimpleNamespace(registry=clocks.registry(17), get=lambda *a, **kw: response),
                      {'url': FINANCE, 'id': b'1' * 16})


@pytest.mark.parametrize('follow', [False, True])
def test_transport_captures_only_actual_followed_source_hops(config, tmp_path, follow):
    start, middle, final = ('https://publisher.test/' + route for route in ('a', 'b', 'c'))
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        start: FakeResponse(b'first response', status=301, headers={'Location': '/b'}),
        middle: FakeResponse(status=307, headers={'Location': final}),
        final: FakeResponse(b'final response', headers={'Content-Type': 'text/html'}),
    })
    with closing(http): response = http.get(start, follow_redirects=follow)
    assert response.url == (final if follow else start)
    assert len(response.redirects) == (2 if follow else 0)
    if follow:
        first, second = response.redirects
        assert (first.from_url, first.status, first.location, first.to_url) == (start, 301, '/b', middle)
        assert first.response_sha256 == hashlib.sha256(b'first response').hexdigest()
        assert first.response_bytes == len(b'first response')
        assert (second.from_url, second.status, second.to_url) == (middle, 307, final)
        assert datetime.fromisoformat(first.observed_at_utc).utcoffset().total_seconds() == 0
    assert [url for url, _ in session.calls][1:] == ([start, middle, final] if follow else [start])


def test_v17_changes_only_capture_not_capacity_or_authorization():
    old, new = (clocks.registry(version).sources['ettoday'] for version in (16, 17))
    assert new.data_fingerprint != old.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint


@pytest.mark.parametrize('excluded', ['none', 'future_due', 'unobserved', 'future_observation', 'naive_observation',
    'leased', 'has_body', 'has_partition', 'unsupported_route', 'other_failure', 'wrong_error', 'complete'])
def test_once_only_missing_identity_retry_preserves_every_other_field_and_budget(tmp_path, excluded):
    old, new = clocks.registry(16), clocks.registry(17)
    assert ettoday_source_redirect_parser_repair(new.sources['ettoday'], old.sources['ettoday'].data_fingerprint)
    assert not ettoday_source_redirect_parser_repair(new.sources['ettoday'], clocks.registry(15).sources['ettoday'].data_fingerprint)
    url = FINANCE + ('?unknown=1' if excluded == 'unsupported_route' else '')
    with closing(NewsQueue(tmp_path, old)) as queue:
        queue.add_articles([{'url': url}])
        values = {'state': 'article_identity_not_verified', 'error_code': 'article_identity_not_verified',
                  'attempts': 3, 'due': 99, 'last_checked_at_utc': utc_now()}
        if excluded == 'future_due': values['due'] = (datetime.now(timezone.utc) + timedelta(days=1)).timestamp()
        elif excluded == 'unobserved': values['last_checked_at_utc'] = None
        elif excluded == 'future_observation': values['last_checked_at_utc'] = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        elif excluded == 'naive_observation': values['last_checked_at_utc'] = '2026-10-18T00:00:00'
        elif excluded == 'leased': values.update(lease_owner='existing-owner', lease_until=9999999999)
        elif excluded == 'has_body': values['body_sha256'] = 'a' * 64
        elif excluded == 'has_partition': values['parquet_path'] = 'bodies/unchanged.parquet'
        elif excluded == 'other_failure': values.update(state='article_attribution_not_verified', error_code='article_attribution_not_verified')
        elif excluded == 'wrong_error': values['error_code'] = 'http_429'
        elif excluded == 'complete': values['state'] = 'complete'
        with queue.db:
            queue.db.execute('UPDATE articles SET ' + ','.join(key + '=?' for key in values) + ' WHERE url=?', (*values.values(), url))
            queue.db.execute("INSERT INTO queue_settings VALUES ('failure_unchanged_synthetic', 'spent')")
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
    with closing(NewsQueue(tmp_path, new)) as queue:
        after = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        assert after == ({**before, 'state': 'retry'} if excluded == 'none' else before)
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key='failure_unchanged_synthetic'").fetchone()[0] == 'spent'
        marker = json.loads(queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'ettoday_source_redirect_recheck_v17:%'").fetchone()[0])
        assert len(marker['rows']) == (excluded == 'none') and not marker['accepted_as_bodies']
        with queue.db:
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified' WHERE url=?", (url,))
        queue.recheck_ettoday_source_redirects(old.sources['ettoday'].data_fingerprint)
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (url,)).fetchone()[0] == 'article_identity_not_verified'
