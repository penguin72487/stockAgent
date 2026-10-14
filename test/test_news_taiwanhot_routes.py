"""Literal short/old focus routes need an independently bound source identity."""
from contextlib import closing
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_public_originals import (
    TAIWANHOT_V2_ROUTE_REPAIR_IDENTITY, taiwanhot_request_ids, taiwanhot_route_matches,
)
from downloader.news_fulltext_queue import NewsQueue
from test_news_taiwanhot_native import URL, FOCUS, TEXT, config, page


SHORT = 'https://www.taiwanhot.net/news/907503'
OLD = 'https://www.taiwanhot.net/news/focus/56/%E5%8F%B0%E5%8C%97/907503/%E6%9C%AC%E7%AF%87%E6%96%B0%E8%81%9E'


def registry(version=3, backend='lxml'):
    cfg = config()
    cfg['providers'][0]['embedded_article_layout'] = f'taiwanhot_visible_article_v{version}'
    cfg['providers'][0]['html_parser'] = backend
    if version == 3:
        cfg['providers'][0]['article_path_pattern'] = '^(?:/news/(?:focus/)?[1-9][0-9]*(?:/|$)|/$)'
    return PolicyRegistry(cfg, base=Path.cwd())


@pytest.mark.parametrize('backend', ['html.parser', 'lxml'])
@pytest.mark.parametrize('requested,final', [(SHORT, SHORT), (SHORT+'/', URL), (SHORT+'?from=local', URL),
    (OLD, OLD), (OLD, URL), (FOCUS, URL), (URL, FOCUS), (OLD.replace('www.', ''), URL)])
def test_v3_literal_normal_transport_and_whole_body_bind_source_og_title_id(backend, requested, final):
    calls = []
    raw = page()
    def get(url, **kwargs):
        calls.append(url)
        return Response(final, 200, {'content-type':'text/html'}, raw)
    result = fetch_article(SimpleNamespace(registry=registry(backend=backend), get=get),
        {'url':requested, 'id':hashlib.sha256(requested.encode()).digest(), 'metadata_date_raw':'2099-01-01'})
    assert calls == [requested] and result['requested_url'] == requested and result['final_url'] == final
    assert result['source_native_article_id'] == '907503'
    assert result['raw_response_body'] == raw and result['body'] == '原始小標\n'+TEXT
    assert result['published_at_raw'] == '2026-10-14 15:30'
    assert json.loads(result['authors_json']) == ['理財周刊／新聞中心']
    assert not result['publish'] and not result['remote_training_export'] and not result['historical_point_in_time']


def test_focus_category_and_article_numbers_are_candidates_not_min_max_or_last_id():
    assert taiwanhot_request_ids(OLD) == {'56','907503'}
    assert taiwanhot_request_ids(FOCUS) == {'70','907503'}
    assert taiwanhot_route_matches(OLD, '907503', '本篇新聞')
    assert not taiwanhot_route_matches(OLD, '907503', '另外的新聞')
    assert not taiwanhot_route_matches(FOCUS, '70', '本篇新聞')


@pytest.mark.parametrize('url', [OLD.replace('%E6%9C%AC%E7%AF%87%E6%96%B0%E8%81%9E', 'different-title'),
    OLD.replace('/907503/', '/907504/'), URL.replace('907503','907504'),
    'https://www.taiwanhot.net/news/focus/907503/no-match/70/no-match'])
def test_admission_candidate_does_not_accept_a_different_title_or_article(url):
    final = URL if '/907504/' in url or 'different-title' in url else URL.replace('907503','70')
    raw = page(og_url=final)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        fetch_article(SimpleNamespace(registry=registry(), get=lambda *_a,**_k: Response(final,200,{'content-type':'text/html'},raw)),
                      {'url':url,'id':b'x'*32})


@pytest.mark.parametrize('version', [1,2])
def test_previous_contracts_do_not_silently_admit_short_or_old_focus_views(version):
    with pytest.raises(NewsError, match='unsupported_article_path'):
        fetch_article(SimpleNamespace(registry=registry(version), get=lambda *_a,**_k: pytest.fail('request')), {'url':SHORT})
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(page(), registry(version).sources['taiwanhot'], page_url=OLD)


@pytest.mark.parametrize('url', ['https://www.taiwanhot.net/news/0', 'https://www.taiwanhot.net/news/907503/%2f',
    'https://evil.test/news/907503', 'https://www.taiwanhot.net/?p=907503&p=907504',
    'https://www.taiwanhot.net/news/907503/not/extra/path', 'https://www.taiwanhot.net/news/focus/56/city/not-id/title'])
def test_unregistered_malformed_and_ambiguous_non_article_routes_are_not_admitted(url):
    assert not taiwanhot_request_ids(url)


@pytest.mark.parametrize('backend', ['html.parser','lxml'])
def test_normal_v2_metadata_values_body_hash_and_author_clocks_stay_unchanged(backend):
    old = extract_article(page(), registry(2,backend).sources['taiwanhot'], page_url=URL)
    new = extract_article(page(), registry(3,backend).sources['taiwanhot'], page_url=URL)
    assert all(new[k] == v for k,v in old.items() if k != 'article_identity_basis')
    assert registry(2).sources['taiwanhot'].authorization_fingerprint == registry(3).sources['taiwanhot'].authorization_fingerprint


def pin_v2(queue, capture=None, parser=None):
    old, repair = TAIWANHOT_V2_ROUTE_REPAIR_IDENTITY
    old, repair = capture or old, parser or repair
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (old,'source_capture_fingerprint:taiwanhot'))
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps({
            'contract_version':1,'data_fingerprint':old,'parser_fingerprint':repair}), 'source_parser_repair_fingerprint:taiwanhot'))


def test_pinned_v2_once_rechecks_unknown_metadata_without_erasing_original_rows(tmp_path):
    with closing(NewsQueue(tmp_path,registry(2))) as queue:
        cases = [(SHORT,'unsupported_article_path',None,None), (OLD,'article_identity_not_verified',None,None),
            (URL,'body_not_found',None,None), (URL+'?other=view','article_identity_not_verified','old-head',None),
            (URL+'?leased=view','body_not_found',None,'worker'), (URL+'?blocked=view','http_403',None,None),
            (URL+'?robots=view','robots_disallowed',None,None), (URL+'?rate=view','http_429',None,None)]
        for url,state,head,lease in cases:
            queue.add_articles([{'url':url}])
            with queue.db:
                queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=6,due=2000000000,'
                    'last_checked_at_utc=?,body_sha256=?,lease_owner=?,lease_until=? WHERE url=?',
                    (state,state,'2026-10-14T15:00:00+00:00',head,lease,2000000200 if lease else None,url))
        before = {r['url']:dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        pin_v2(queue)
        queue.registry = registry()
        queue.reconcile_policy()
        after = {r['url']:dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        for url,row in before.items():
            assert after[url] == ({**row,'state':'retry'} if url in {SHORT,OLD,URL} else row)
        proofs = queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'taiwanhot_route_metadata_observation_v3:%'").fetchall()
        assert len(proofs)==1 and json.loads(proofs[0][0])['rows_moved']==3
        queue.reconcile_policy()
        assert after == {r['url']:dict(r) for r in queue.db.execute('SELECT * FROM articles')}


@pytest.mark.parametrize('capture,parser', [('unknown',None),(None,'unknown')])
@pytest.mark.parametrize('state', ['unsupported_article_path','body_not_found','article_identity_not_verified'])
def test_unknown_prior_capture_or_parser_is_not_a_requeue_authorization(tmp_path,capture,parser,state):
    with closing(NewsQueue(tmp_path,registry(2))) as queue:
        queue.add_articles([{'url':SHORT}])
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=4,due=2000000000', (state,state))
        before = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        pin_v2(queue,capture,parser)
        queue.registry=registry()
        queue.reconcile_policy()
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone())==before
        assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE 'taiwanhot_route_metadata_observation_v3:%'").fetchone()
