"""Observed numeric request spellings, never guessed aliases or HTTP probes."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import time
from types import SimpleNamespace

import pytest

from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_http import RedirectHop, Response
from downloader.news_fulltext_pocket import (
    POCKET_PATH_PATTERN, POCKET_V10_PATH_PATTERN, POCKET_V10_NEW_ARTICLE_REQUESTS,
    POCKET_V10_SCOPE_REPAIR_IDENTITY, POCKET_V10_PRIOR_HTTP_MEASUREMENT_IDENTITY,
    pocket_article_id, pocket_numeric_transport_verified,
)
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, pocket_native_parser_repair
from test_news_pocket_original import settings, page, versioned, URL, OLD
from test_news_pocket_legacy_body import inline_label, context_summary, illustrated, index_cta


def routes(settings):
    result = versioned(settings, 10)
    result['providers'][0]['article_path_pattern'] = POCKET_V10_PATH_PATTERN
    return result


def redirect(start, end, status=308):
    return RedirectHop(start, status, end, end, '2026-10-21T00:00:00+00:00', 'a' * 64, 22)


@pytest.mark.parametrize('path', ['/invest_news/article/12345', '/invest_news/article/12345/',
    '/school/report/BeforeMarket/12345', '/school/report/newflash/12345//', '/school/articles/12345'])
def test_literal_numeric_variants_only_for_new_request_contract(settings, path):
    literal = 'https://www.pocket.tw' + path
    assert pocket_article_id(literal, requested=True) is None
    assert pocket_article_id(literal, requested=True, adapter_contract_version=10) == '12345'
    assert pocket_article_id(literal) is None
    middle = OLD
    response = Response(URL, 200, {'content-type': 'text/html'}, page().encode(),
        (redirect(literal, middle, 301), redirect(middle, URL)))
    assert pocket_numeric_transport_verified(response, literal)
    calls = []
    http = SimpleNamespace(registry=PolicyRegistry(routes(settings), base=Path.cwd()), metrics=None,
        get=lambda url, **kwargs: (calls.append(url), response)[1])
    result = fetch_article(http, {'url': literal, 'id': b'a' * 16})
    assert calls == [literal] and result['requested_url'] == literal and result['final_url'] == URL
    assert len(json.loads(result['transport_redirects_json'])) == 2
    assert result['url_resolution_basis'].startswith('verified_pocket_literal_same_id_source_redirect_v1')
    assert result['body_sha256'] == extract_article(page().encode(), http.registry.sources['pocket'], page_url=URL)['body_sha256']


@pytest.mark.parametrize('literal', ['https://www.pocket.tw/school/articles/article-opaque/',
    'https://www.pocket.tw/school/videos/12345/', 'https://www.pocket.tw/etf/tw/00999/',
    URL + '?other=1', URL + '#fragment', URL.replace('12345', '012345'),
    URL.replace('www.pocket.tw', 'www.pocket.tw.other.example.org'),
    OLD + '//', URL.replace('https:', 'http:')])
def test_new_route_contract_still_rejects_unproved_or_nonarticle_requests(settings, literal):
    calls = []
    http = SimpleNamespace(registry=PolicyRegistry(routes(settings), base=Path.cwd()), metrics=None,
        get=lambda url, **kwargs: calls.append(url))
    with pytest.raises(NewsError, match='unsupported_article_path|awaiting_authorization'):
        fetch_article(http, {'url': literal, 'id': b'a' * 16})
    assert not calls


@pytest.mark.parametrize('change', ['absent', 'broken_from', 'wrong_status', 'location', 'foreign_host',
    'different_ID', 'query', 'naive_clock', 'invalid_clock', 'missing_hash', 'bad_bytes', 'too_many', 'wrong_final'])
def test_entire_actual_transport_chain_is_required(settings, change):
    literal = 'https://www.pocket.tw/invest_news/article/12345'
    hops = [redirect(literal, OLD, 301), redirect(OLD, URL)]
    final = URL
    if change == 'absent': hops = []
    elif change == 'broken_from': hops[1] = replace(hops[1], from_url=literal)
    elif change == 'wrong_status': hops[0] = replace(hops[0], status=200)
    elif change == 'location': hops[0] = replace(hops[0], location='/school/articles/54321/')
    elif change == 'foreign_host': hops[0] = replace(hops[0], to_url=OLD.replace('www.pocket.tw', 'other.example.org'))
    elif change == 'different_ID': hops[0] = replace(hops[0], to_url=OLD.replace('12345', '54321'))
    elif change == 'query': hops[0] = replace(hops[0], to_url=OLD + '?other=1')
    elif change == 'naive_clock': hops[0] = replace(hops[0], observed_at_utc='2026-10-21T00:00:00')
    elif change == 'invalid_clock': hops[0] = replace(hops[0], observed_at_utc='not a clock')
    elif change == 'missing_hash': hops[0] = replace(hops[0], response_sha256='')
    elif change == 'bad_bytes': hops[0] = replace(hops[0], response_bytes=-1)
    elif change == 'too_many': hops = [redirect(literal, OLD)] + [redirect(OLD, OLD)] * 7 + [redirect(OLD, URL)]
    else: final = URL.replace('12345', '54321')
    response = Response(final, 200, {'content-type': 'text/html'}, page().encode(), tuple(hops))
    assert not pocket_numeric_transport_verified(response, literal)
    calls = []
    http = SimpleNamespace(registry=PolicyRegistry(routes(settings), base=Path.cwd()), metrics=None,
        get=lambda url, **kwargs: (calls.append(url), response)[1])
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        fetch_article(http, {'url': literal, 'id': b'a' * 16})
    assert calls == [literal]


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
@pytest.mark.parametrize('raw', [page(), page(nested=False), str(inline_label()), str(context_summary()),
    str(illustrated()), str(index_cta())], ids=['nested', 'flat', 'inline', 'context', 'figure', 'cta'])
def test_new_request_scope_preserves_entire_previous_body_clock_and_use_output(settings, engine, raw):
    before, after = versioned(settings, 9), routes(settings)
    before['providers'][0]['html_parser'] = after['providers'][0]['html_parser'] = engine
    sources = [PolicyRegistry(c, base=Path.cwd()).sources['pocket'] for c in (before, after)]
    assert extract_article(raw.encode(), sources[0], page_url=URL) == extract_article(raw.encode(), sources[1], page_url=URL)
    assert not pocket_native_parser_repair(sources[1], sources[0].data_fingerprint)


def test_only_complete_installed_contract_pair_preserves_existing_http_measurements():
    config, _, base = load_config(Path('configs/news_fulltext.json'))
    old = deepcopy(config)
    entry = next(p for p in old['providers'] if p['id'] == 'pocket')
    entry.update(embedded_article_layout='pocket_visible_article_v9', article_path_pattern=POCKET_PATH_PATTERN)
    new = deepcopy(old)
    entry = next(p for p in new['providers'] if p['id'] == 'pocket')
    entry.update(embedded_article_layout='pocket_visible_article_v10', article_path_pattern=POCKET_V10_PATH_PATTERN)
    a, b = [PolicyRegistry(c, base=base).sources['pocket'] for c in (old, new)]
    assert (a.data_fingerprint, b.data_fingerprint) == POCKET_V10_SCOPE_REPAIR_IDENTITY
    assert b.parser_repair_prior_capture_fingerprint == a.data_fingerprint
    assert b.authorization_fingerprint == a.authorization_fingerprint
    assert (b.http_capacity_source_fingerprint, b.http_capacity_authorization_fingerprint) == POCKET_V10_PRIOR_HTTP_MEASUREMENT_IDENTITY
    assert b.http_capacity_source_fingerprint == a.http_capacity_source_fingerprint
    for mutation in ('authorization', 'interval_seconds', 'parallelism', 'note'):
        changed = deepcopy(new)
        target = next(p for p in changed['providers'] if p['id'] == 'pocket')
        if mutation == 'authorization': target[mutation]['scope'] += '; different scope'
        elif mutation == 'note': target[mutation] += '; different review'
        else: target[mutation] += 1
        source = PolicyRegistry(changed, base=base).sources['pocket']
        assert not source.http_capacity_source_fingerprint and not source.http_capacity_authorization_fingerprint


@pytest.mark.parametrize('obstruction', [None, 'body', 'parquet', 'lease_owner', 'lease_until',
    'future_due', 'missing_clock', 'future_clock', 'naive_clock', 'refusal', 'wrong_error',
    'zero_attempts', 'unknown_prior', 'changed_rights', 'local_ID_body'])
def test_once_only_route_repair_preserves_all_original_failure_evidence(settings, tmp_path, obstruction):
    old = PolicyRegistry(versioned(settings, 9), base=tmp_path)
    new_config = routes(settings)
    if obstruction == 'changed_rights': new_config['providers'][0]['authorization']['scope'] += '; other scope'
    new = PolicyRegistry(new_config, base=tmp_path)
    queue = NewsQueue(tmp_path, old)
    try:
        url = POCKET_V10_NEW_ARTICLE_REQUESTS[0]
        other = 'https://www.pocket.tw/invest_news/article/7913'
        queue.add_articles([{'url': url}, {'url': other}])
        with queue.db:
            queue.db.execute("UPDATE articles SET state='unsupported_article_path',error_code=state,attempts=2,"
                "due=0,last_checked_at_utc='2026-01-01T00:00:00+00:00'")
            if obstruction == 'body': queue.db.execute('UPDATE articles SET body_sha256=? WHERE url=?', ('a' * 64, url))
            elif obstruction == 'parquet': queue.db.execute('UPDATE articles SET parquet_path=? WHERE url=?', ('existing.parquet', url))
            elif obstruction in ('lease_owner', 'lease_until'):
                queue.db.execute('UPDATE articles SET ' + obstruction + '=? WHERE url=?',
                    ('another-owner' if obstruction == 'lease_owner' else time.time() + 100, url))
            elif obstruction == 'future_due': queue.db.execute('UPDATE articles SET due=? WHERE url=?', (time.time() + 3600, url))
            elif obstruction in ('missing_clock', 'future_clock', 'naive_clock'):
                value = {'missing_clock': None, 'future_clock': '2999-01-01T00:00:00+00:00',
                    'naive_clock': '2026-01-01T00:00:00'}[obstruction]
                queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?', (value, url))
            elif obstruction == 'refusal': queue.db.execute("UPDATE articles SET state='http_403',error_code='http_403' WHERE url=?", (url,))
            elif obstruction == 'wrong_error': queue.db.execute("UPDATE articles SET error_code='http_403' WHERE url=?", (url,))
            elif obstruction == 'zero_attempts': queue.db.execute('UPDATE articles SET attempts=0 WHERE url=?', (url,))
            elif obstruction == 'local_ID_body':
                canonical = 'https://www.pocket.tw/school/articles/1264528/'
                queue.add_articles([{'url': canonical}])
                queue.db.execute("UPDATE articles SET state='complete',body_sha256=?,validator_url=url WHERE url=?", ('a' * 64, canonical))
        def heads():
            return [dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')]
        before = heads()
        queue.registry = new
        prior = 'f' * 64 if obstruction == 'unknown_prior' else old.sources['pocket'].data_fingerprint
        queue.recheck_pocket_article_request_routes(prior)
        after = heads()
        if obstruction is None:
            expected = deepcopy(before)
            next(r for r in expected if r['url'] == url)['state'] = 'retry'
            assert after == expected
            marker = json.loads(queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'pocket_numeric_request_recheck_v10:%'").fetchone()[0])
            assert [r['url'] for r in marker['rows']] == [url] and not marker['accepted_as_bodies']
        else: assert after == before
        queue.recheck_pocket_article_request_routes(prior)
        assert heads() == after
    finally: queue.close()


def test_normal_catalog_load_only_requeues_observed_new_ID_requests(settings, tmp_path):
    config = versioned(settings, 9)
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    url, duplicate = POCKET_V10_NEW_ARTICLE_REQUESTS[0], 'https://www.pocket.tw/invest_news/article/7913'
    queue.add_articles([{'url': url}, {'url': duplicate}, {'url': URL}])
    with queue.db:
        queue.db.execute("UPDATE articles SET state='unsupported_article_path',error_code=state,attempts=3,"
            "due=0,last_checked_at_utc='2026-01-01T00:00:00+00:00' WHERE url!=?", (URL,))
        queue.db.execute("UPDATE articles SET state='body_not_found',error_code='body_not_found',attempts=4,due=? WHERE url=?", (time.time()+3600, URL))
    before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
    queue.close()
    queue = NewsQueue(tmp_path, PolicyRegistry(routes(settings), base=tmp_path))
    try:
        after = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        assert after[url] == {**before[url], 'state': 'retry'}
        assert after[duplicate] == before[duplicate] and after[URL] == before[URL]
    finally: queue.close()
