"""Literal opaque School identities; no numeric guesses, GETs or old-body replays."""
from copy import deepcopy
from pathlib import Path
import time
from types import SimpleNamespace

import pytest

from downloader.news_fulltext import fetch_article, load_config
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_pocket import (
    POCKET_V11_PATH_PATTERN, POCKET_V11_NEW_ARTICLE_REQUESTS, POCKET_V11_SCOPE_REPAIR_IDENTITY,
    POCKET_V10_PRIOR_HTTP_MEASUREMENT_IDENTITY, pocket_article_id, pocket_article_transport_verified,
)
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, pocket_native_parser_repair
from test_news_pocket_original import settings, page, URL, RELEASE, REVISION
from test_news_pocket_numeric_routes import routes, redirect
from test_news_pocket_legacy_body import inline_label, context_summary, illustrated, index_cta

OPAQUE = POCKET_V11_NEW_ARTICLE_REQUESTS[0]


def opaque_routes(config):
    result = routes(config)
    result['providers'][0].update(embedded_article_layout='pocket_visible_article_v11',
        article_path_pattern=POCKET_V11_PATH_PATTERN)
    return result


@pytest.mark.parametrize('literal', POCKET_V11_NEW_ARTICLE_REQUESTS)
@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
def test_literal_identity_and_complete_native_body_stay_distinct_from_numeric_ID(settings, literal, engine):
    config = opaque_routes(settings)
    config['providers'][0]['html_parser'] = engine
    registry = PolicyRegistry(config, base=Path.cwd())
    raw = page().replace(URL, literal).encode()
    response = Response(literal, 200, {'content-type': 'text/html'}, raw, ())
    assert pocket_article_id(literal) is None
    assert pocket_article_id(literal, requested=True, adapter_contract_version=10) is None
    identity = literal.rstrip('/').rsplit('/', 1)[1]
    assert pocket_article_id(literal, adapter_contract_version=11) == identity
    assert pocket_article_id(literal, requested=True, adapter_contract_version=11) == identity
    assert pocket_article_transport_verified(response, literal, adapter_contract_version=11)
    calls = []
    http = SimpleNamespace(registry=registry, metrics=None,
        get=lambda url, **kwargs: (calls.append(url), response)[1])
    actual = fetch_article(http, {'url': literal, 'id': b'a' * 16})
    assert calls == [literal] and actual['requested_url'] == actual['final_url'] == literal
    assert not actual.get('transport_redirects_json')
    expected = extract_article(page().encode(), registry.sources['pocket'], page_url=URL)
    # Identity is deliberately literal, not normalized to the numeric fixture.
    # Every other body/author/clock/use field must stay byte-for-byte equal.
    assert expected['declared_canonical_url'] == URL
    expected['declared_canonical_url'] = literal
    assert all(actual[key] == value for key, value in expected.items())
    assert actual['published_at_raw'] == RELEASE and actual['modified_at_raw'] == REVISION
    assert not actual['training_injection_enabled'] and not actual['remote_training_export']


@pytest.mark.parametrize('url', [OPAQUE[:-1], OPAQUE + '/', OPAQUE + '?x=1', OPAQUE + '#x',
    OPAQUE.replace('mvaoags6', 'MVAOAGS6'), OPAQUE.replace('mvaoags6', 'mvaoags'),
    OPAQUE.replace('mvaoags6', 'mvaoags66'), OPAQUE.replace('articles', 'videos'),
    OPAQUE.replace('https:', 'http:'), OPAQUE.replace('www.pocket.tw', 'www.pocket.tw.evil.example.org')])
def test_unreviewed_opaque_request_forms_still_fail_before_HTTP(settings, url):
    calls = []
    http = SimpleNamespace(registry=PolicyRegistry(opaque_routes(settings), base=Path.cwd()), metrics=None,
        get=lambda url, **kwargs: calls.append(url))
    assert pocket_article_id(url, requested=True, adapter_contract_version=11) is None
    with pytest.raises(NewsError, match='unsupported_article_path|awaiting_authorization'):
        fetch_article(http, {'url': url, 'id': b'a' * 16})
    assert not calls


@pytest.mark.parametrize('final,hops', [(URL, (redirect(OPAQUE, URL),)),
    (POCKET_V11_NEW_ARTICLE_REQUESTS[1], (redirect(OPAQUE, POCKET_V11_NEW_ARTICLE_REQUESTS[1]),)),
    (OPAQUE, (redirect(OPAQUE, OPAQUE),)), (OPAQUE, [])])
def test_no_opaque_redirect_or_numeric_mapping_is_inferred(settings, final, hops):
    response = Response(final, 200, {'content-type': 'text/html'}, page().replace(URL, final).encode(), hops)
    assert not pocket_article_transport_verified(response, OPAQUE, adapter_contract_version=11)
    http = SimpleNamespace(registry=PolicyRegistry(opaque_routes(settings), base=Path.cwd()), metrics=None,
        get=lambda *args, **kwargs: response)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        fetch_article(http, {'url': OPAQUE, 'id': b'a' * 16})


@pytest.mark.parametrize('old,new,code', [
    ('rel="canonical"', 'rel="alternate"', 'article_identity_not_verified'),
    ('property="og:type" content="article"', 'property="og:type" content="website"', 'article_identity_not_verified'),
    ('<h1>', '<h1 hidden>', 'article_identity_not_verified'),
    ('class="cms-body"', 'class="cms-body hidden"', 'article_identity_not_verified'),
    ('第2段', '<span aria-hidden="true">第2段</span>', 'article_identity_not_verified'),
    ('"name": "口袋學堂編輯團隊"', '"name": "外部供稿"', 'article_rights_or_scope_excluded'),
    ('"name": "口袋學堂"', '"name": "其他出版商"', 'article_identity_not_verified'),
    ('"isAccessibleForFree": true', '"isAccessibleForFree": false', 'paywall_or_login'),
    (REVISION, '2026-02-30T03:04:05.678Z', 'invalid_bulk_publication_clock'),
])
def test_opaque_route_does_not_relax_native_content_or_clock_boundaries(settings, old, new, code):
    raw = page().replace(URL, OPAQUE).replace(old, new).encode()
    source = PolicyRegistry(opaque_routes(settings), base=Path.cwd()).sources['pocket']
    with pytest.raises(NewsError, match=code):
        extract_article(raw, source, page_url=OPAQUE)


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
@pytest.mark.parametrize('raw', [page(), page(nested=False), str(inline_label()), str(context_summary()),
    str(illustrated()), str(index_cta())], ids=['nested', 'flat', 'inline', 'context', 'figure', 'cta'])
def test_all_numeric_outputs_and_redirect_contracts_are_unchanged(settings, engine, raw):
    before, after = routes(settings), opaque_routes(settings)
    before['providers'][0]['html_parser'] = after['providers'][0]['html_parser'] = engine
    a, b = [PolicyRegistry(c, base=Path.cwd()).sources['pocket'] for c in (before, after)]
    assert extract_article(raw.encode(), a, page_url=URL) == extract_article(raw.encode(), b, page_url=URL)
    literal = 'https://www.pocket.tw/invest_news/article/12345'
    assert pocket_article_transport_verified(Response(URL, 200, {}, b'', (redirect(literal, URL),)),
        literal, adapter_contract_version=11)
    assert not pocket_native_parser_repair(b, a.data_fingerprint)


def test_complete_installed_scope_pair_only_retains_prior_HTTP_identity():
    config, _, base = load_config(Path('configs/news_fulltext.json'))
    old = deepcopy(config)
    entry = next(p for p in old['providers'] if p['id'] == 'pocket')
    from downloader.news_fulltext_pocket import POCKET_V10_PATH_PATTERN
    entry.update(embedded_article_layout='pocket_visible_article_v10', article_path_pattern=POCKET_V10_PATH_PATTERN)
    new = deepcopy(old)
    next(p for p in new['providers'] if p['id'] == 'pocket').update(
        embedded_article_layout='pocket_visible_article_v11', article_path_pattern=POCKET_V11_PATH_PATTERN)
    a, b = [PolicyRegistry(c, base=base).sources['pocket'] for c in (old, new)]
    assert (a.data_fingerprint, b.data_fingerprint) == POCKET_V11_SCOPE_REPAIR_IDENTITY
    assert b.parser_repair_prior_capture_fingerprint == a.data_fingerprint
    assert b.authorization_fingerprint == a.authorization_fingerprint
    assert (b.http_capacity_source_fingerprint, b.http_capacity_authorization_fingerprint) == POCKET_V10_PRIOR_HTTP_MEASUREMENT_IDENTITY
    assert b.http_capacity_source_fingerprint == a.http_capacity_source_fingerprint
    for key in ('authorization', 'interval_seconds', 'parallelism', 'note'):
        changed = deepcopy(new)
        target = next(p for p in changed['providers'] if p['id'] == 'pocket')
        if key == 'authorization': target[key]['scope'] += '; other scope'
        elif key == 'note': target[key] += '; other review'
        else: target[key] += 1
        source = PolicyRegistry(changed, base=base).sources['pocket']
        assert not source.http_capacity_source_fingerprint and not source.http_capacity_authorization_fingerprint


@pytest.mark.parametrize('obstruction', [None, 'body', 'parquet', 'lease_owner', 'lease_until', 'future_due',
    'missing_clock', 'future_clock', 'naive_clock', 'refusal', 'wrong_error', 'zero_attempts', 'unknown_prior', 'changed_rights'])
def test_only_six_unstored_literal_heads_requeued_without_resetting_failure_evidence(settings, tmp_path, obstruction):
    prior = PolicyRegistry(routes(settings), base=tmp_path)
    config = opaque_routes(settings)
    if obstruction == 'changed_rights': config['providers'][0]['authorization']['scope'] += '; other scope'
    new = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path, prior)
    try:
        other = 'https://www.pocket.tw/school/report/newflash/8013'
        queue.add_articles([{'url': OPAQUE}, {'url': other}, {'url': URL}])
        with queue.db:
            queue.db.execute("UPDATE articles SET state='unsupported_article_path',error_code=state,attempts=2,"
                "due=0,last_checked_at_utc='2026-01-01T00:00:00+00:00'")
            if obstruction == 'body': queue.db.execute('UPDATE articles SET body_sha256=? WHERE url=?', ('a' * 64, OPAQUE))
            elif obstruction == 'parquet': queue.db.execute('UPDATE articles SET parquet_path=? WHERE url=?', ('old.parquet', OPAQUE))
            elif obstruction in ('lease_owner', 'lease_until'):
                queue.db.execute('UPDATE articles SET ' + obstruction + '=? WHERE url=?',
                    ('other-owner' if obstruction == 'lease_owner' else time.time() + 300, OPAQUE))
            elif obstruction == 'future_due': queue.db.execute('UPDATE articles SET due=? WHERE url=?', (time.time() + 3600, OPAQUE))
            elif obstruction in ('missing_clock', 'future_clock', 'naive_clock'):
                value = {'missing_clock': None, 'future_clock': '2999-01-01T00:00:00+00:00', 'naive_clock': '2026-01-01T00:00:00'}[obstruction]
                queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?', (value, OPAQUE))
            elif obstruction == 'refusal': queue.db.execute("UPDATE articles SET state='http_403',error_code='http_403' WHERE url=?", (OPAQUE,))
            elif obstruction == 'wrong_error': queue.db.execute("UPDATE articles SET error_code='http_403' WHERE url=?", (OPAQUE,))
            elif obstruction == 'zero_attempts': queue.db.execute('UPDATE articles SET attempts=0 WHERE url=?', (OPAQUE,))
        def heads(): return [dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')]
        before = heads()
        queue.registry = new
        fingerprint = 'f' * 64 if obstruction == 'unknown_prior' else prior.sources['pocket'].data_fingerprint
        queue.recheck_pocket_article_request_routes(fingerprint)
        after = heads()
        expected = deepcopy(before)
        if obstruction is None: next(row for row in expected if row['url'] == OPAQUE)['state'] = 'retry'
        assert after == expected
        queue.recheck_pocket_article_request_routes(fingerprint)
        assert heads() == after
    finally: queue.close()


def test_normal_startup_requeues_only_observed_opaque_cohort(settings, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(routes(settings), base=tmp_path))
    others = ['https://www.pocket.tw/school/report/newflash/8013', URL]
    queue.add_articles([{'url': url} for url in (*POCKET_V11_NEW_ARTICLE_REQUESTS, *others)])
    with queue.db:
        queue.db.execute("UPDATE articles SET state='unsupported_article_path',error_code=state,attempts=2,"
            "due=0,last_checked_at_utc='2026-01-01T00:00:00+00:00'")
    before = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
    queue.close()
    queue = NewsQueue(tmp_path, PolicyRegistry(opaque_routes(settings), base=tmp_path))
    try:
        after = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
        assert all(after[url] == {**before[url], 'state': 'retry'} for url in POCKET_V11_NEW_ARTICLE_REQUESTS)
        assert all(after[url] == before[url] for url in others)
    finally: queue.close()
