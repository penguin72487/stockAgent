"""Literal native views require their own complete article proof, not host aliases."""
from contextlib import closing
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from test_news_taiwanhot_native import URL, LEGACY, TEXT, config, page


BARE = URL.replace('www.taiwanhot.net', 'taiwanhot.net')
QUERY = URL + '?shem=aimgspe,'
V1_CAPTURE = '7c462aa61ebff890e661f6486ce904c867d03df8dd10c730e4d2a9a979a3ac0c'
V1_PARSER = 'a69d12dd73e9ad3f6270a9e2a1bbc3ff7df632e550cedf946fb98c645f3e4a85'


def registry(*, version=2, backend='lxml'):
    cfg = config()
    cfg['providers'][0]['embedded_article_layout'] = f'taiwanhot_visible_article_v{version}'
    cfg['providers'][0]['html_parser'] = backend
    return PolicyRegistry(cfg, base=Path.cwd())


@pytest.mark.parametrize('backend', ['html.parser', 'lxml'])
@pytest.mark.parametrize('requested,final', [(BARE, BARE), (QUERY, QUERY), (BARE + '?lang=tw', BARE + '?lang=tw'),
    (LEGACY, BARE), (QUERY, URL), (URL, BARE), (URL, URL)])
def test_v2_preserves_literal_request_response_and_source_body_without_second_request(backend, requested, final):
    policy, calls = registry(backend=backend), []
    def get(url, **kwargs):
        calls.append((url, kwargs))
        return Response(final, 200, {'content-type': 'text/html'}, page())
    row = {'url': requested, 'id': hashlib.sha256(requested.encode()).digest(), 'metadata_date_raw': '2099-01-01'}
    result = fetch_article(SimpleNamespace(registry=policy, get=get), row)
    assert calls == [(requested, {'headers': {}})]
    assert result['requested_url'] == requested and result['final_url'] == final
    assert result['source_request_url'] == final and result['raw_response_body'] == page()
    assert result['published_at_raw'] == '2026-10-14 15:30'
    assert result['metadata_date_raw'] == '2099-01-01'  # Never publication evidence.
    assert result['body'] == '原始小標\n' + TEXT and json.loads(result['authors_json']) == ['理財周刊／新聞中心']
    assert not result['publish'] and not result['remote_training_export'] and not result['historical_point_in_time']


@pytest.mark.parametrize('backend', ['html.parser', 'lxml'])
@pytest.mark.parametrize('declared', [URL, BARE])
def test_queryless_https_source_declaration_not_query_semantics_binds_actual_id(backend, declared):
    source = registry(backend=backend).sources['taiwanhot']
    result = extract_article(page(og_url=declared, extra_head=f'<link rel="canonical" href="{declared}">'), source, page_url=QUERY)
    assert result['body_sha256'] == extract_article(page(), source, page_url=URL)['body_sha256']


@pytest.mark.parametrize('final', [URL.replace('907503', '907504'), LEGACY, URL.replace('https:', 'http:'),
    URL.replace('www.', 'web.'), URL.replace('www.taiwanhot.net', 'else.taiwanhot.net'),
    URL.replace('/news/907503/', '/news/907503/%2F'), 'https://www.taiwanhot.net/news/907503/'])
def test_native_view_cannot_accept_different_id_insecure_unknown_host_or_legacy_result(final):
    policy = registry()
    response = Response(final, 200, {'content-type': 'text/html'}, page())
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        fetch_article(SimpleNamespace(registry=policy, get=lambda *a, **kw: response), {'url': QUERY})


@pytest.mark.parametrize('og', [QUERY, BARE + '?shem=anything', LEGACY, URL.replace('907503', '907504'),
    URL.replace('https:', 'http:'), URL.replace('www.', 'web.'), 'https://unrelated.test/news/907503/original'])
def test_current_declaration_must_be_queryless_registered_https_and_same_id(og):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(page(og_url=og), registry().sources['taiwanhot'], page_url=QUERY)


@pytest.mark.parametrize('status', [401, 403, 429, 451, 503])
def test_refusal_is_not_resolved_by_removing_query_changing_host_or_body_parsing(status):
    calls = []
    def get(url, **kwargs):
        calls.append(url)
        return Response(QUERY, status, {'content-type': 'text/html'}, page())
    with pytest.raises(NewsError):
        fetch_article(SimpleNamespace(registry=registry(), get=get), {'url': QUERY})
    assert calls == [QUERY]


@pytest.mark.parametrize('body,extra_head,extra_body', [
    (TEXT, '', '<div class="paywall">付費</div>'),
    ('本文禁止轉載。' + TEXT, '', ''),
    (TEXT, f'<script type="application/ld+json">{{"@type":"NewsArticle","url":"{URL}","isAccessibleForFree":false}}</script>', ''),
])
def test_native_views_do_not_expand_body_or_rights_scope(body, extra_head, extra_body):
    with pytest.raises(NewsError, match='paywall_or_login|article_rights_or_scope_excluded'):
        extract_article(page(body=body, extra_head=extra_head, extra_body=extra_body), registry().sources['taiwanhot'], page_url=QUERY)


def test_v1_contract_remains_strict_and_v2_identity_changes_only_this_provider():
    old, new = registry(version=1), registry()
    assert old.sources['taiwanhot'].data_fingerprint != new.sources['taiwanhot'].data_fingerprint
    assert old.sources['taiwanhot'].parser_repair_fingerprint != new.sources['taiwanhot'].parser_repair_fingerprint
    assert old.sources['taiwanhot'].authorization_fingerprint == new.sources['taiwanhot'].authorization_fingerprint
    for url in (BARE, QUERY):
        with pytest.raises(NewsError, match='article_identity_not_verified'):
            extract_article(page(), old.sources['taiwanhot'], page_url=url)


@pytest.mark.parametrize('declared', [BARE, BARE + '?native=view', URL + '?other=view'])
def test_current_paid_schema_at_same_id_actual_view_cannot_hide_behind_exact_url_comparison(declared):
    schema = json.dumps({'@type': 'NewsArticle', 'url': declared, 'isAccessibleForFree': False})
    with pytest.raises(NewsError, match='paywall_or_login'):
        extract_article(page(extra_head=f'<script type="application/ld+json">{schema}</script>'),
                        registry().sources['taiwanhot'], page_url=QUERY)


def pin_old_identity(queue, *, old=V1_CAPTURE, parser=V1_PARSER):
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (old, 'source_capture_fingerprint:taiwanhot'))
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps({
            'contract_version': 1, 'data_fingerprint': old, 'parser_fingerprint': parser}),
            'source_parser_repair_fingerprint:taiwanhot'))


def test_pinned_v1_repairs_only_newly_admitted_missing_unleased_views_and_preserves_evidence(tmp_path):
    with closing(NewsQueue(tmp_path, registry(version=1))) as queue:
        cases = [
            (BARE, 'article_identity_not_verified', None, None),
            (QUERY, 'unsupported_article_path', None, None),
            (URL, 'article_identity_not_verified', None, None),
            (LEGACY + '&p=907504', 'unsupported_article_path', None, None),
            (BARE.replace('907503', '907505'), 'article_identity_not_verified', 'existing-body', None),
            (QUERY.replace('907503', '907506'), 'unsupported_article_path', None, 'other-worker'),
            (BARE.replace('907503', '907507'), 'source_access_denied', None, None),
            (BARE.replace('907503', '907508'), 'http_429', None, None),
            (BARE.replace('907503', '907509'), 'robots_disallowed', None, None),
        ]
        for url, state, body, lease in cases:
            queue.add_articles([{'url': url}])
            with queue.db:
                queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=7,due=2000000000,'
                    'last_checked_at_utc=?,body_sha256=?,lease_owner=?,lease_until=? WHERE url=?',
                    (state, state, '2026-10-14T14:00:00+00:00', body, lease, 2000000100 if lease else None, url))
        before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        pin_old_identity(queue)
        queue.registry = registry()
        queue.reconcile_policy()
        after = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        for url, original in before.items():
            expected = {**original, 'state': 'retry'} if url in {BARE, QUERY} else original
            assert after[url] == expected
        proof = queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'taiwanhot_native_view_identity_repair_v2:%'").fetchall()
        assert len(proof) == 1 and json.loads(proof[0][0])['rows_moved'] == 2
        original_proof, changes = proof, queue.db.total_changes
        queue.reconcile_policy()
        assert queue.db.total_changes == changes
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'taiwanhot_native_view_identity_repair_v2:%'").fetchall() == original_proof


@pytest.mark.parametrize('old,parser', [('a' * 64, V1_PARSER), (V1_CAPTURE, 'b' * 64)])
def test_unmatched_old_capture_or_parser_cannot_trigger_pinned_repair(tmp_path, old, parser):
    with closing(NewsQueue(tmp_path, registry(version=1))) as queue:
        queue.add_articles([{'url': BARE}])
        with queue.db:
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',attempts=3")
        before = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        pin_old_identity(queue, old=old, parser=parser)
        queue.registry = registry()
        queue.reconcile_policy()
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == before


@pytest.mark.parametrize('field,value', [('article_selectors', ['body']), ('domains', ['other.test']),
    ('feeds', ['https://www.taiwanhot.net/']), ('require_author_attribution', False)])
def test_v2_keeps_the_original_typed_private_known_url_scope(field, value):
    cfg = config()
    cfg['providers'][0]['embedded_article_layout'] = 'taiwanhot_visible_article_v2'
    cfg['providers'][0][field] = deepcopy(value)
    with pytest.raises(ValueError):
        PolicyRegistry(cfg, base=Path.cwd())
