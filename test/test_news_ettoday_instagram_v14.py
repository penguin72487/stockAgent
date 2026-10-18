"""Native static preview grammar, source boundaries and one-time recovery.

Synthetic UI fixtures exercise the closed grammar without downloading social
media or checking private news HTML into Git. Actual-original dual-parser replay
and canonical body/receipt acceptance are separate operational evidence.
"""
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json

from bs4 import BeautifulSoup, Comment
import pytest

import downloader.news_fulltext_ettoday as adapter
import downloader.news_fulltext_static_instagram as instagram
from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_policy import ETTODAY_LAYOUT_VERSIONS, NewsError, extract_article
from downloader.news_fulltext_queue import NewsQueue, ettoday_instagram_preview_parser_repair, utc_now, verify_bodies
from scripts.audit_news_selected_articles import audit
from test_news_fulltext import FakeResponse, http_fixture
import test_news_ettoday as native
import test_news_ettoday_clock_v2 as clocks
import test_news_ettoday_static_imgur_v9 as imgur
import test_news_ettoday_static_threads_v10 as threads
import test_news_ettoday_split_reporter_v13 as split


PERMALINK = 'https://www.instagram.com/reel/Ab12Cd34Ef5/?utm_source=ig_embed&utm_campaign=loading'
CREDIT = '合成分享者（@synthetic_account）分享的貼文'
ICON = 'M1 1L2 2 synthetic decorative path'


@pytest.fixture(autouse=True)
def synthetic_decorative_icons(monkeypatch):
    monkeypatch.setattr(instagram, '_ICON_SHA256', hashlib.sha256(ICON.encode()).hexdigest())
    monkeypatch.setattr(instagram, '_ICON_CHARACTERS', len(ICON))
    monkeypatch.setattr(adapter, '_THREADS_ICON_SHA256', hashlib.sha256(threads.ICON.encode()).hexdigest())


def page(parser='lxml', host='finance', *, base=native.page, script_src='//www.instagram.com/embed.js'):
    soup = base(host, parser) if base is native.page else base(parser, host=host)

    def render(contract):
        if isinstance(contract, str):
            return contract
        tag, attrs, children = contract
        node = soup.new_tag(tag, attrs=deepcopy(attrs))
        if tag == 'path':
            node['d'] = ICON
        for child in children:
            node.append(render(child))
        return node

    story = soup.select_one('.story')
    story.append(render(instagram._preview_tree(PERMALINK, CREDIT)))
    script = soup.new_tag('script', attrs={'async': '', 'src': script_src})
    story.append(script)
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', version=14, host='finance'):
    source = replace(clocks.registry(version).sources['ettoday'], html_parser=parser)
    return extract_article(str(soup).encode(), source, page_url=native.URLS[host == 'www'])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('script_src', ['//www.instagram.com/embed.js', 'https://www.instagram.com/embed.js'])
def test_whole_story_and_native_clocks_unchanged_preview_and_share_credit_separate(parser, script_src):
    soup = page(parser, script_src=script_src); original = str(soup)
    result = extract(soup, parser)
    old = extract(native.page('finance', parser), parser, version=13)
    for field in ('body', 'body_sha256', 'title', 'authors_json', 'published_at_raw', 'modified_at_raw',
            'author_attribution_detail_json', 'verified_image_captions_json'):
        assert result[field] == old[field]
    assert str(soup) == original
    assert CREDIT not in result['body'] and 'Instagram' not in result['body']
    assert CREDIT not in result['authors_json'] and native.PROSE in result['body']
    notices = json.loads(result['verified_article_notices_json'])
    preview, = (n for n in notices if n['kind'] == 'native_static_instagram_preview_ui_declaration')
    assert preview['source_share_credit'] == CREDIT
    assert preview['source_blockquote_attributes'] == soup.blockquote.attrs
    assert preview['source_script_attributes'] == soup.blockquote.find_next_sibling('script').attrs
    assert preview['raw_declarations_preserved']
    for field in ('external_request_sent', 'script_executed', 'embedded_post_content_accepted',
            'share_credit_is_reporter_attribution'):
        assert preview[field] is False
    assert 'native_instagram_preview_ui_separate' in result['body_completeness']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']


def test_both_html_engines_produce_all_identical_article_values():
    raw = str(page()).encode()
    values = [extract_article(raw, replace(clocks.registry(14).sources['ettoday'], html_parser=p),
        page_url=native.URLS[0]) for p in ('lxml', 'html.parser')]
    assert values[0] == values[1]


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('version', range(1, 14))
def test_older_contracts_do_not_silently_change_their_instagram_interpretation(parser, version):
    with pytest.raises(NewsError, match='^article_identity_not_verified$'):
        extract(page(parser), parser, version)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['extra_text', 'extra_node', 'comment', 'hidden', 'aria_hidden',
    'style', 'icon', 'path_attribute', 'script_code', 'script_src', 'script_query', 'script_attribute',
    'missing_script', 'nonadjacent_script', 'duplicate', 'nested', 'query', 'fragment', 'wrong_permalink',
    'different_anchor', 'version', 'credit_prose', 'credit_markup', 'extra_script', 'label', 'unknown_attribute'])
def test_unreviewed_or_active_embed_and_prose_are_never_stripped(parser, change):
    soup = page(parser); widget = soup.blockquote; script = widget.find_next_sibling('script')
    if change == 'extra_text': widget.div.append('不能當成介面刪除的貼文正文。')
    elif change == 'extra_node': widget.div.append(soup.new_tag('aside'))
    elif change == 'comment': widget.div.append(Comment('not an empty whitespace node'))
    elif change == 'hidden': widget['hidden'] = ''
    elif change == 'aria_hidden': widget['aria-hidden'] = 'true'
    elif change == 'style': widget.div['style'] += 'display:none;'
    elif change == 'icon': widget.path['d'] += 'M0 0'
    elif change == 'path_attribute': widget.path['onclick'] = 'run()'
    elif change == 'script_code': script.string = 'run()'
    elif change == 'script_src': script['src'] = 'https://example.org/embed.js'
    elif change == 'script_query': script['src'] += '?other=1'
    elif change == 'script_attribute': script['defer'] = ''
    elif change == 'missing_script': script.decompose()
    elif change == 'nonadjacent_script': script.insert_before(Comment('not whitespace'))
    elif change == 'duplicate': script.insert_before(deepcopy(widget))
    elif change == 'nested': widget.wrap(soup.new_tag('div'))
    elif change == 'query': widget['data-instgrm-permalink'] += '&unknown=1'
    elif change == 'fragment': widget['data-instgrm-permalink'] += '#fragment'
    elif change == 'wrong_permalink': widget['data-instgrm-permalink'] = PERMALINK.replace('/reel/', '/p/')
    elif change == 'different_anchor': widget.a['href'] = PERMALINK.replace('Ab12Cd34Ef5', 'Ab12Cd34Ef6')
    elif change == 'version': widget['data-instgrm-version'] = '15'
    elif change == 'credit_prose': widget.p.a.string = '外站貼文內容不是分享標籤。'
    elif change == 'credit_markup': widget.p.a.append(soup.new_tag('b'))
    elif change == 'extra_script': widget.append(soup.new_tag('script'))
    elif change == 'label': widget.find(string='在 Instagram 查看這則貼文').replace_with('未審核的介面內容')
    elif change == 'unknown_attribute': widget['data-unknown'] = '1'
    original = str(soup)
    with pytest.raises(NewsError, match='^article_identity_not_verified$'):
        extract(soup, parser)
    assert str(soup) == original


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_no_new_host_or_reporter_permission_and_unrelated_scripts_remain_blocked(parser):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(page(parser, 'www'), parser, host='www')
    soup = page(parser); soup.select_one('#credit').decompose()
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        extract(soup, parser)
    soup = page(parser); soup.select_one('.story').append(soup.new_tag('script', src='https://other.example/code.js'))
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup, parser)
    assert adapter.ettoday_request_identity('https://house.ettoday.net/news/3210081') is None


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('base', [native.page, split.page, imgur.page, threads.page])
def test_old_native_layouts_replay_identically_under_new_abi(parser, base):
    old_soup = base('finance', parser) if base is native.page else base(parser, host='finance')
    assert extract(old_soup, parser, 13) == extract(old_soup, parser)
    result = extract(page(parser, base=base), parser)
    assert result['body_sha256'] == extract(old_soup, parser, 13)['body_sha256']


def test_version_ancestry_changes_only_capture_not_access_or_http_identity():
    first, old, new = (clocks.registry(v).sources['ettoday'] for v in (1, 13, 14))
    assert set(ETTODAY_LAYOUT_VERSIONS.values()) == set(range(1, clocks.future_version()))
    assert f'ettoday_visible_article_v{clocks.future_version()}' not in ETTODAY_LAYOUT_VERSIONS
    assert new.data_fingerprint != old.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint == first.authorization_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint == first.data_fingerprint
    assert ettoday_instagram_preview_parser_repair(new, old.data_fingerprint)
    assert not ettoday_instagram_preview_parser_repair(new, clocks.registry(12).sources['ettoday'].data_fingerprint)


@pytest.mark.parametrize('boundary', ['eligible', 'body', 'parquet', 'lease_owner', 'lease_until', 'future_due',
    'state', 'error', 'missing_clock', 'naive_clock', 'future_clock', 'invalid_clock', 'unknown_prior', 'skipped_v13', 'different_scope'])
def test_only_exact_observed_due_missing_unleased_head_requeues_once(tmp_path, boundary):
    old = clocks.registry(12 if boundary == 'skipped_v13' else 13, tmp_path)
    if boundary == 'different_scope':
        cfg = clocks.config(13); cfg['providers'][0]['authorization']['scope'] += '; altered'
        old = type(old)(cfg, base=tmp_path)
    url = adapter.ETTODAY_V14_PROVED_INSTAGRAM_PREVIEW_URLS[0]
    protected = ['https://finance.ettoday.net/news/3200319', 'https://finance.ettoday.net/news/3210082']
    with closing(NewsQueue(tmp_path, old)) as queue:
        queue.add_articles([{'url': u} for u in (url, *protected)])
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
            'attempts=6,due=1234,last_checked_at_utc=?', (utc_now(),))
        values = {'body': ('body_sha256', 'b' * 64), 'parquet': ('parquet_path', 'saved.parquet'),
            'lease_owner': ('lease_owner', 'other'), 'lease_until': ('lease_until', 9000000000),
            'future_due': ('due', 9000000000), 'state': ('state', 'http_403'), 'error': ('error_code', 'http_429'),
            'missing_clock': ('last_checked_at_utc', None), 'naive_clock': ('last_checked_at_utc', '2026-10-17T00:00:00'),
            'future_clock': ('last_checked_at_utc', (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()),
            'invalid_clock': ('last_checked_at_utc', 'invalid')}
        if boundary == 'unknown_prior':
            queue.db.execute("UPDATE queue_settings SET value='unknown' WHERE key='source_capture_fingerprint:ettoday'")
        elif boundary in values:
            field, value = values[boundary]; queue.db.execute(f'UPDATE articles SET {field}=? WHERE url=?', (value, url))
        queue.db.execute("INSERT INTO queue_settings VALUES ('failure_identity_metadata_attempts:v1:ettoday:20261018','4')")
        before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}; queue.db.commit()
    new = clocks.registry(14, tmp_path)
    with closing(NewsQueue(tmp_path, new)) as queue:
        expected = {u: {**r, 'state': 'retry'} if u == url and boundary == 'eligible' else r for u, r in before.items()}
        assert {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')} == expected
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'failure_identity_metadata_attempts:%'").fetchone()[0] == '4'
        marker = 'ettoday_instagram_preview_recheck_v14:' + new.sources['ettoday'].data_fingerprint
        row = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
        if boundary in {'unknown_prior', 'skipped_v13', 'different_scope'}:
            assert row is None
            return
        proof = json.loads(row[0])
        assert proof['rows'] == ([{k: before[url][k] for k in ('url', 'state', 'error_code', 'attempts', 'due', 'last_checked_at_utc')}]
            if boundary == 'eligible' else [])
        assert not proof['accepted_as_bodies'] and proof['attempts_due_error_and_observation_preserved']
        if boundary == 'eligible':
            queue.db.execute('UPDATE articles SET state=? WHERE url=?', (before[url]['state'], url)); queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')} == before
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == row[0]


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_canonical_fake_http_retains_original_and_receipt_without_any_social_request(tmp_path, parser):
    cfg = clocks.config(14); cfg['providers'][0]['html_parser'] = parser
    raw = str(page(parser)).encode(); url = native.URLS[0]
    http, session = http_fixture(cfg, tmp_path, {
        'https://finance.ettoday.net/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: FakeResponse(raw)})
    try:
        with closing(NewsQueue(tmp_path, http.registry)) as queue:
            queue.add_articles([{'url': url}])
            row = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
            article = fetch_article(http, row)
            assert article['raw_response_body'] == raw and not article['publish'] and not article['remote_training_export']
            queue.accept([article])
            assert verify_bodies(tmp_path)['accepted'] and audit(tmp_path, [url], registry=http.registry)['accepted']
            assert [call[0] for call in session.calls] == ['https://finance.ettoday.net/robots.txt', url]
    finally:
        http.close()
