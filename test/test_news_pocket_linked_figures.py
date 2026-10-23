"""Witnessed linked illustrations and as-of dates, never media/body invention."""
from copy import deepcopy
import json
from pathlib import Path
import time
from types import SimpleNamespace

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_pocket import (
    POCKET_V12_NEW_BODY_REQUESTS, POCKET_V12_SCOPE_REPAIR_IDENTITY,
    POCKET_V10_PRIOR_HTTP_MEASUREMENT_IDENTITY,
)
from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from test_news_pocket_original import settings, page, URL, RELEASE, REVISION
from test_news_pocket_opaque_routes import opaque_routes
from test_news_pocket_legacy_body import inline_label, context_summary, illustrated, index_cta

LAYOUT = 'pocket_visible_article_v12'
CAPTION = '原生圖說數值 -8.75、0 與 5.2%'


def current(config):
    result = opaque_routes(config)
    result['providers'][0]['embedded_article_layout'] = LAYOUT
    return result


def linked(*, identifier=False, label=False):
    soup = BeautifulSoup(page().replace(URL, POCKET_V12_NEW_BODY_REQUESTS[0]), 'lxml')
    figure = BeautifulSoup('<figure style="margin:28px 0;"><a href="原圖.png" target="_blank" '
        'rel="noopener noreferrer" aria-label="開啟原圖"><img src="/school/api/media/file/image.webp" '
        'alt="不是正文的圖中文字" width="1254" height="1254" loading="lazy" '
        'style="display:block;width:100%;height:auto;border-radius:8px;"/></a>'
        '<figcaption style="font-size:14px;">' + CAPTION + '</figcaption></figure>', 'lxml').figure.extract()
    if identifier: figure['id'] = 'power-architecture'
    soup.select_one('article.pocket-article > p').insert_after(figure)
    if label: soup.select_one('.article-meta').string = '口袋學堂｜資料截至 2023 年 2 月 14 日'
    return soup


def parse(config, raw):
    source = PolicyRegistry(config, base=Path.cwd()).sources['pocket']
    soup = BeautifulSoup(str(raw), 'lxml')
    url = soup.select_one('link[rel=canonical]')['href']
    return extract_article(str(raw).encode(), source, page_url=url)


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
@pytest.mark.parametrize('identifier', [False, True])
def test_linked_figure_keeps_caption_once_and_all_raw_clocks_without_fetching_media(settings, engine, identifier):
    config = current(settings)
    config['providers'][0]['html_parser'] = engine
    raw = linked(identifier=identifier)
    result = parse(config, raw)
    assert result['body'].count(CAPTION) == 1 and '不是正文的圖中文字' not in result['body']
    reference = parse(config, page().replace(URL, POCKET_V12_NEW_BODY_REQUESTS[0]))
    assert result['body'].replace(CAPTION, '').split() == reference['body'].split()
    assert result['published_at_raw'] == RELEASE and result['modified_at_raw'] == REVISION
    notices = json.loads(result['verified_article_notices_json'])
    assert any(n.get('source_tag') == 'figure' and n['source_layout_retained_in_original'] for n in notices)
    assert any(n.get('alt_raw') == '不是正文的圖中文字' and not n['media_requested'] for n in notices)
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    url = POCKET_V12_NEW_BODY_REQUESTS[0]
    registry = PolicyRegistry(config, base=Path.cwd())
    response = Response(url, 200, {'content-type': 'text/html'}, str(raw).encode(), ())
    fetched = fetch_article(SimpleNamespace(registry=registry, metrics=None,
        get=lambda *a, **k: response), {'url': url, 'id': b'a' * 16})
    assert fetched['body'] == result['body'] and not fetched['training_injection_enabled']
    assert not fetched['remote_training_export']
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        parse(opaque_routes(settings), raw)


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
def test_school_as_of_label_is_not_promoted_to_release_or_revision(settings, engine):
    config = current(settings)
    config['providers'][0]['html_parser'] = engine
    raw = BeautifulSoup(page(), 'lxml')
    raw.select_one('.article-meta').string = '口袋學堂｜資料截至 2023 年 2 月 14 日'
    result = parse(config, raw)
    assert result['body'] == parse(config, page())['body']
    assert result['published_at_raw'] == RELEASE and result['modified_at_raw'] == REVISION
    notice = next(n for n in json.loads(result['verified_article_notices_json'])
        if n['kind'] == 'native_inner_calendar_label')
    assert notice['text'] == '口袋學堂｜資料截至 2023 年 2 月 14 日'
    assert notice['calendar_date'] == '2023-02-14' and notice['clock_role'] == 'data_as_of_date'
    assert notice['not_promoted_to_release_or_revision_clock'] and not notice['matches_outer_visible_update_day']
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        parse(opaque_routes(settings), raw)


@pytest.mark.parametrize('mutation', ['extra_prose', 'extra_image', 'script', 'javascript', 'invalid_url',
    'remote_href', 'query', 'fragment', 'target', 'rel', 'onclick', 'svg', 'duplicate_id', 'bad_id',
    'hidden_figure', 'hidden_anchor', 'hidden_image', 'hidden_caption', 'foreign_credit', 'paid'])
def test_new_container_does_not_discard_unknown_prose_or_relax_visibility_scope(settings, mutation):
    soup = linked(identifier=True)
    figure, anchor = soup.figure, soup.figure.a
    if mutation == 'extra_prose': anchor.append('未核對的新正文')
    elif mutation == 'extra_image': anchor.append(soup.new_tag('img', src='other.webp'))
    elif mutation == 'script': anchor.append(soup.new_tag('script'))
    elif mutation == 'javascript': anchor['href'] = 'javascript:alert(1)'
    elif mutation == 'invalid_url': anchor['href'] = 'http://['
    elif mutation == 'remote_href': anchor['href'] = 'https://other.example/image.png'
    elif mutation == 'query': anchor['href'] += '?x=1'
    elif mutation == 'fragment': anchor['href'] += '#x'
    elif mutation == 'target': anchor['target'] = '_self'
    elif mutation == 'rel': anchor['rel'] = ['noopener']
    elif mutation == 'onclick': anchor['onclick'] = 'execute()'
    elif mutation == 'svg': anchor.img.name = 'svg'
    elif mutation == 'duplicate_id': figure.insert_after(soup.new_tag('div', id=figure['id']))
    elif mutation == 'bad_id': figure['id'] = 'unreviewed id'
    elif mutation.startswith('hidden_'):
        {'figure': figure, 'anchor': anchor, 'image': anchor.img, 'caption': figure.figcaption}[mutation[7:]]['aria-hidden'] = 'true'
    raw = str(soup)
    if mutation == 'foreign_credit': raw = raw.replace('"name": "口袋學堂編輯團隊"', '"name": "外部供稿"')
    elif mutation == 'paid': raw = raw.replace('"isAccessibleForFree": true', '"isAccessibleForFree": false')
    with pytest.raises(NewsError): parse(current(settings), raw)


@pytest.mark.parametrize('label', ['口袋學堂｜資料截至 2023 年 2 月 30 日',
    '口袋學堂｜資料截至昨天', '口袋學堂｜資料截至 2023 年 2 月 14 日 未核對文字'])
def test_unreviewed_as_of_labels_never_disappear_as_clocks(settings, label):
    soup = BeautifulSoup(page(), 'lxml')
    soup.select_one('.article-meta').string = label
    with pytest.raises(NewsError, match='invalid_bulk_publication_clock'):
        parse(current(settings), soup)


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
@pytest.mark.parametrize('raw', [page(), page(nested=False), str(inline_label()), str(context_summary()),
    str(illustrated()), str(index_cta())])
def test_previously_accepted_complete_outputs_are_unchanged(settings, engine, raw):
    old, new = opaque_routes(settings), current(settings)
    old['providers'][0]['html_parser'] = new['providers'][0]['html_parser'] = engine
    assert parse(old, raw) == parse(new, raw)


def test_reviewed_full_ABI_pair_preserves_HTTP_identity_not_changed_scope():
    from downloader.news_fulltext import load_config
    config, _, _ = load_config(Path('configs/news_fulltext.json'))
    old = deepcopy(config)
    next(p for p in old['providers'] if p['id'] == 'pocket')['embedded_article_layout'] = 'pocket_visible_article_v11'
    new = deepcopy(old)
    next(p for p in new['providers'] if p['id'] == 'pocket')['embedded_article_layout'] = LAYOUT
    a, b = [PolicyRegistry(c, base=Path.cwd()).sources['pocket'] for c in (old, new)]
    assert (a.data_fingerprint, b.data_fingerprint) == POCKET_V12_SCOPE_REPAIR_IDENTITY
    assert b.parser_repair_prior_capture_fingerprint == a.data_fingerprint
    assert (b.http_capacity_source_fingerprint, b.http_capacity_authorization_fingerprint) == POCKET_V10_PRIOR_HTTP_MEASUREMENT_IDENTITY
    next(p for p in new['providers'] if p['id'] == 'pocket')['authorization']['scope'] += '; changed rights'
    changed = PolicyRegistry(new, base=Path.cwd()).sources['pocket']
    assert (changed.http_capacity_source_fingerprint, changed.http_capacity_authorization_fingerprint) != POCKET_V10_PRIOR_HTTP_MEASUREMENT_IDENTITY


@pytest.mark.parametrize('index', [0, 1, 2])
@pytest.mark.parametrize('obstruction', [None, 'body', 'parquet', 'lease_owner', 'lease_until', 'future_due',
    'missing_clock', 'future_clock', 'naive_clock', 'refusal', 'wrong_error', 'zero_attempts', 'unknown_prior'])
def test_only_three_witnessed_unsaved_due_heads_can_retry_once(settings, tmp_path, index, obstruction):
    before_registry = PolicyRegistry(opaque_routes(settings), base=tmp_path)
    after_registry = PolicyRegistry(current(settings), base=tmp_path)
    queue = NewsQueue(tmp_path, before_registry)
    selected = POCKET_V12_NEW_BODY_REQUESTS[index]
    other = URL
    try:
        queue.add_articles([{'url': selected}, {'url': other}])
        error = 'article_identity_not_verified' if index < 2 else 'unsupported_article_path'
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=2,due=0,last_checked_at_utc=?',
                (error, error, '2026-01-01T00:00:00+00:00'))
            if obstruction == 'body': queue.db.execute('UPDATE articles SET body_sha256=? WHERE url=?', ('a'*64, selected))
            elif obstruction == 'parquet': queue.db.execute('UPDATE articles SET parquet_path=? WHERE url=?', ('old.parquet', selected))
            elif obstruction in {'lease_owner', 'lease_until'}:
                queue.db.execute('UPDATE articles SET '+obstruction+'=? WHERE url=?', ('other' if obstruction == 'lease_owner' else 1e12, selected))
            elif obstruction == 'future_due': queue.db.execute('UPDATE articles SET due=1e12 WHERE url=?', (selected,))
            elif obstruction in {'missing_clock', 'future_clock', 'naive_clock'}:
                value = {'missing_clock': None, 'future_clock': '2999-01-01T00:00:00+00:00', 'naive_clock': '2026-01-01T00:00:00'}[obstruction]
                queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?', (value, selected))
            elif obstruction == 'refusal': queue.db.execute("UPDATE articles SET state='http_403',error_code='http_403' WHERE url=?", (selected,))
            elif obstruction == 'wrong_error': queue.db.execute("UPDATE articles SET error_code='http_403' WHERE url=?", (selected,))
            elif obstruction == 'zero_attempts': queue.db.execute('UPDATE articles SET attempts=0 WHERE url=?', (selected,))
        before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        queue.registry = after_registry
        prior = 'f'*64 if obstruction == 'unknown_prior' else before_registry.sources['pocket'].data_fingerprint
        queue.recheck_pocket_article_request_routes(prior)
        after = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        assert after[other] == before[other]
        assert after[selected] == ({**before[selected], 'state': 'retry'} if obstruction is None else before[selected])
        queue.recheck_pocket_article_request_routes(prior)
        assert {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')} == after
    finally: queue.close()


@pytest.mark.parametrize('clock_error', ['unsupported_article_path', 'invalid_bulk_publication_clock'])
def test_canonical_startup_repairs_only_the_exact_observed_cohort(settings, tmp_path, clock_error):
    queue = NewsQueue(tmp_path, PolicyRegistry(opaque_routes(settings), base=tmp_path))
    others = [URL, POCKET_V12_NEW_BODY_REQUESTS[0].replace('mvexg3rz', 'unseen00')]
    queue.add_articles([{'url': u} for u in (*POCKET_V12_NEW_BODY_REQUESTS, *others)])
    with queue.db:
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',attempts=2,"
            "due=0,last_checked_at_utc='2026-01-01T00:00:00+00:00'")
        queue.db.execute('UPDATE articles SET state=?,error_code=? WHERE url=?',
            (clock_error, clock_error, POCKET_V12_NEW_BODY_REQUESTS[2]))
    before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
    queue.close()
    queue = NewsQueue(tmp_path, PolicyRegistry(current(settings), base=tmp_path))
    try:
        after = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        assert after == {u: ({**h, 'state': 'retry'} if u in POCKET_V12_NEW_BODY_REQUESTS else h)
            for u, h in before.items()}
    finally: queue.close()


def test_saved_numeric_article_alias_is_not_replayed_by_the_clock_repair(settings, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(opaque_routes(settings), base=tmp_path))
    queue.add_articles([{'url': POCKET_V12_NEW_BODY_REQUESTS[2]}, {'url': 'https://www.pocket.tw/school/articles/8013/'}])
    with queue.db:
        queue.db.execute("UPDATE articles SET state='unsupported_article_path',error_code='unsupported_article_path',attempts=2,"
            "due=0,last_checked_at_utc='2026-01-01T00:00:00+00:00'")
        queue.db.execute("UPDATE articles SET body_sha256=? WHERE url LIKE '%/articles/8013/'", ('a'*64,))
    before = [dict(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY url')]
    queue.close()
    queue = NewsQueue(tmp_path, PolicyRegistry(current(settings), base=tmp_path))
    try: assert [dict(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY url')] == before
    finally: queue.close()
