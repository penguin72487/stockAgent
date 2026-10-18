"""Observed static UI is not a Threads post, reporter or replacement article.

All prose, IDs and icon paths are synthetic. The real decorative glyph hash is
verified separately by retained-response replay, never copied into a fixture.
"""
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json

from bs4 import BeautifulSoup, Comment
import pytest

import downloader.news_fulltext_ettoday as adapter
from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_queue import NewsQueue, ettoday_static_threads_parser_repair, utc_now
import test_news_ettoday as native
import test_news_ettoday_clock_v2 as clocks
import test_news_ettoday_break_credit_v8 as breaks
import test_news_ettoday_social_headline_v7 as social
import test_news_ettoday_static_imgur_v9 as imgur


ICON = 'M0 0L1 1'
PERMALINK = 'https://www.threads.com/@synthetic_user/post/Ab12Cd34Ef5'


@pytest.fixture(autouse=True)
def synthetic_decorative_icon(monkeypatch):
    monkeypatch.setattr(adapter, '_THREADS_ICON_SHA256', hashlib.sha256(ICON.encode()).hexdigest())


def page(parser='lxml', *, padding=False, host='finance'):
    soup = native.page(host, parser)
    story = soup.select_one('.story')
    if padding:
        strong = story.find_all('p', recursive=False)[1].strong
        strong.append(soup.new_tag('br', type='_moz'))
        strong.append('\n')
    template = ('<blockquote class="text-post-media" id="ig-tp-Ab12Cd34Ef5" '
        'data-text-post-version="0" data-text-post-permalink="' + PERMALINK + '" '
        'style="' + adapter._THREADS_WIDGET_STYLE + '"><a href="' + PERMALINK + '" '
        'target="_blank" style="' + adapter._THREADS_LINK_STYLE + '">'
        '<div style="padding:40px;display:flex;flex-direction:column;align-items:center;">'
        '<div style="display:block;height:32px;width:32px;padding-bottom:20px;">'
        '<svg aria-label="Threads" height="32px" role="img" viewbox="" width="32px" '
        'xmlns="http://www.w3.org/2000/svg"><path d="' + ICON + '"></path></svg></div>'
        '<div style="font-size:15px;line-height:21px;color:#000000;font-weight:600;">'
        '在 Threads 查看</div></div></a></blockquote>\n'
        '<script async src="https://www.threads.com/embed.js"></script>')
    fragment = BeautifulSoup(template, parser)
    for node in list(fragment.select('blockquote, script')):
        story.append(node)
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', *, version=10, host='finance'):
    source = replace(clocks.registry(version).sources['ettoday'], html_parser=parser)
    return extract_article(str(soup).encode(), source, page_url=native.URLS[host == 'www'])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('padding', [False, True])
def test_whole_prose_clocks_reporter_caption_and_raw_declarations_preserved(parser, padding):
    soup = page(parser, padding=padding)
    original = str(soup)
    result = extract(soup, parser)
    baseline = extract(native.page('finance', parser), parser, version=9)
    assert str(soup) == original
    for key in ('body', 'body_sha256', 'title', 'authors_json', 'published_at_raw', 'modified_at_raw',
            'author_attribution_detail_json', 'verified_image_captions_json'):
        assert result[key] == baseline[key]
    assert native.PROSE in result['body'] and native.NOTICE in result['body']
    assert '在 Threads 查看' not in result['body'] and 'synthetic_user' not in result['body']
    notices = json.loads(result['verified_article_notices_json'])
    notice, = [n for n in notices if n['kind'] == 'native_static_threads_preview_ui_declaration']
    assert notice['source_blockquote_attributes'] == soup.blockquote.attrs
    assert notice['source_link_attributes'] == soup.blockquote.a.attrs
    assert notice['source_script_attributes'] == soup.blockquote.find_next_sibling('script').attrs
    assert notice['raw_declarations_preserved']
    assert not any(notice[key] for key in ('external_request_sent', 'script_executed', 'embedded_post_content_accepted'))
    assert 'embedded_post_not_requested_or_transcribed' in result['body_completeness']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('version', range(1, 10))
def test_previous_versions_do_not_silently_accept_new_ui(parser, version):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(page(parser), parser, version=version)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('variant', ['native', 'social', 'break', 'imgur'])
def test_old_healthy_v9_outputs_are_exactly_equal_without_replay(parser, variant):
    soup = {'native': native.page, 'social': social.page, 'break': breaks.page, 'imgur': imgur.page}[variant]
    doc = soup('finance', parser) if variant == 'native' else soup(parser)
    assert extract(doc, parser, version=9) == extract(doc, parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('case', ['foreign_host', 'query', 'id_mismatch', 'version', 'widget_extra',
    'widget_style', 'widget_hidden', 'widget_comment', 'widget_text', 'widget_duplicate', 'widget_nested',
    'anchor_href', 'anchor_extra', 'anchor_target', 'anchor_style', 'anchor_prose', 'outer_extra',
    'icon_extra', 'svg_extra', 'svg_title', 'path_other', 'path_text', 'label_prose', 'label_nested',
    'label_hidden', 'intervening_text', 'intervening_comment', 'no_script', 'extra_script', 'script_body',
    'script_src', 'script_async', 'script_extra', 'www', 'bad_identity', 'no_reporter', 'wire',
    'bad_clock', 'hidden_prose', 'third_party_prose', 'mixed_imgur'])
def test_unknown_markup_prose_access_and_execution_stay_rejected(parser, case):
    host = 'www' if case == 'www' else 'finance'
    soup = page(parser, host=host)
    widget, script = soup.blockquote, soup.blockquote.find_next_sibling('script')
    anchor, outer = widget.a, widget.a.div
    icon, label = outer.find_all('div', recursive=False)
    if case in {'foreign_host', 'query'}:
        widget['data-text-post-permalink'] = PERMALINK.replace('www.threads.com', 'unreviewed.example') if case == 'foreign_host' else PERMALINK + '?x=1'
    elif case == 'id_mismatch': widget['id'] = 'ig-tp-OtherId1234'
    elif case == 'version': widget['data-text-post-version'] = '1'
    elif case in {'widget_extra', 'widget_style', 'widget_hidden'}:
        widget[{'widget_extra': 'data-other', 'widget_style': 'style', 'widget_hidden': 'hidden'}[case]] = 'unreviewed'
    elif case == 'widget_comment': widget.append(Comment('unreviewed'))
    elif case == 'widget_text': widget.append('embedded original post cannot be dropped')
    elif case == 'widget_duplicate': widget.insert_after(BeautifulSoup(str(widget), parser).blockquote)
    elif case == 'widget_nested': widget.wrap(soup.new_tag('div'))
    elif case == 'anchor_href': anchor['href'] = PERMALINK + '?different=1'
    elif case in {'anchor_extra', 'anchor_target', 'anchor_style'}:
        anchor[{'anchor_extra': 'rel', 'anchor_target': 'target', 'anchor_style': 'style'}[case]] = 'unknown'
    elif case == 'anchor_prose': anchor.append('do not delete source text')
    elif case == 'outer_extra': outer['data-other'] = ''
    elif case == 'icon_extra': icon.append(soup.new_tag('img'))
    elif case == 'svg_extra': icon.svg['onload'] = 'unverified()'
    elif case == 'svg_title': icon.svg.append(soup.new_tag('title'))
    elif case == 'path_other': icon.path['d'] = 'M100 100'
    elif case == 'path_text': icon.path.string = 'not UI'
    elif case == 'label_prose': label.string = 'unverified third-party post'
    elif case == 'label_nested': label.string.wrap(soup.new_tag('strong'))
    elif case == 'label_hidden': label['hidden'] = ''
    elif case == 'intervening_text': widget.insert_after('not whitespace')
    elif case == 'intervening_comment': widget.insert_after(Comment('unknown'))
    elif case == 'no_script': script.decompose()
    elif case == 'extra_script': script.insert_after(soup.new_tag('script'))
    elif case == 'script_body': script.string = 'unreviewed()'
    elif case == 'script_src': script['src'] = 'https://unreviewed.example/embed.js'
    elif case == 'script_async': del script['async']
    elif case == 'script_extra': script['defer'] = ''
    elif case == 'bad_identity': soup.select_one('head link[rel=canonical]')['href'] = native.URLS[0] + '1'
    elif case == 'no_reporter': soup.select_one('#credit').decompose()
    elif case == 'wire': soup.select_one('#credit').string = '中央社／台北報導'
    elif case == 'bad_clock': soup.select_one('meta[name=pubdate]')['content'] = '2026-10-13T22:19:00+08:00'
    elif case == 'hidden_prose': soup.select_one('#prose')['hidden'] = ''
    elif case == 'third_party_prose': label.string = native.PROSE
    elif case == 'mixed_imgur':
        for node in imgur.page(parser).select('.imgur-embed-pub,script[src]'):
            soup.select_one('.story').append(node)
    with pytest.raises(NewsError):
        extract(soup, parser, host=host)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('case', ['prose_after_break', 'no_moz_type', 'other_type', 'strong_extra',
    'caption_extra', 'nested_break', 'no_reporter', 'two_reporters', 'hidden_caption'])
def test_caption_padding_is_not_a_new_prose_or_attribution_cutoff(parser, case):
    soup = page(parser, padding=True)
    caption = soup.select_one('.story').find_all('p', recursive=False)[1]
    if case == 'prose_after_break': caption.strong.contents[-1].replace_with('must not be dropped')
    elif case == 'no_moz_type': del caption.br['type']
    elif case == 'other_type': caption.br['type'] = 'unknown'
    elif case == 'strong_extra': caption.strong['class'] = 'unknown'
    elif case == 'caption_extra': caption['data-unknown'] = ''
    elif case == 'nested_break': caption.br.wrap(soup.new_tag('span'))
    elif case == 'no_reporter': soup.select_one('#credit').decompose()
    elif case == 'two_reporters': soup.select_one('#credit').insert_after(BeautifulSoup('<p>記者其他記者／台北報導</p>', parser).p)
    elif case == 'hidden_caption': caption['hidden'] = ''
    with pytest.raises(NewsError):
        extract(soup, parser)


@pytest.mark.parametrize('prior_version', range(1, 10))
def test_capture_ancestry_changes_only_parser_not_source_or_http(prior_version):
    old, new = clocks.registry(prior_version).sources['ettoday'], clocks.registry(10).sources['ettoday']
    assert old.data_fingerprint != new.data_fingerprint
    assert old.authorization_fingerprint == new.authorization_fingerprint
    assert (old.http_capacity_source_fingerprint or old.data_fingerprint) == new.http_capacity_source_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == clocks.registry(9).sources['ettoday'].data_fingerprint
    assert ettoday_static_threads_parser_repair(new, old.data_fingerprint) == (prior_version == 9)


@pytest.mark.parametrize('ancestry', ['exact', 'unknown', 'changed_scope'])
@pytest.mark.parametrize('case', ['eligible', 'complete', 'saved_body', 'saved_path', 'lease_owner', 'lease_until',
    'unobserved', 'bad_clock', 'naive_clock', 'future_clock', 'wrong_error', 'http_403', 'http_429',
    'paywall_or_login', 'article_attribution_not_verified', 'retry'])
def test_only_new_two_missing_heads_reopen_once_and_old_completed_cohorts_do_not(tmp_path, ancestry, case):
    old, new = clocks.registry(9, tmp_path), clocks.registry(10, tmp_path)
    if ancestry == 'unknown':
        old.sources['ettoday'] = replace(old.sources['ettoday'], data_fingerprint='f' * 64,
            parser_repair_capture_fingerprint='f' * 64)
    elif ancestry == 'changed_scope':
        cfg = clocks.config(9)
        cfg['providers'][0]['authorization']['scope'] += '; unreviewed scope'
        old = type(old)(cfg, base=tmp_path)
    urls = adapter.ETTODAY_V10_PROVED_THREADS_URLS
    target = urls[0]
    untouched = (*adapter.ETTODAY_V9_PROVED_STATIC_IMGUR_URLS, *breaks.ETTODAY_V8_PROVED_BREAK_CREDIT_URLS,
        'https://finance.ettoday.net/news/2046282')
    with closing(NewsQueue(tmp_path, old)) as queue:
        queue.add_articles([{'url': url} for url in (*urls, *untouched)])
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code=state,"
            'attempts=4,due=9000000000,last_checked_at_utc=?', (utc_now(),))
        queue.db.execute("UPDATE articles SET error_code='article_identity_not_verified'")
        if case in {'complete', 'http_403', 'http_429', 'paywall_or_login', 'article_attribution_not_verified', 'retry'}:
            queue.db.execute('UPDATE articles SET state=?,error_code=? WHERE url=?', (case, case, target))
        elif case in {'saved_body', 'saved_path', 'lease_owner', 'lease_until'}:
            field, value = {'saved_body': ('body_sha256', 'a' * 64), 'saved_path': ('parquet_path', 'old.parquet'),
                'lease_owner': ('lease_owner', 'old-owner'), 'lease_until': ('lease_until', 9000000000)}[case]
            queue.db.execute('UPDATE articles SET ' + field + '=? WHERE url=?', (value, target))
        elif case in {'unobserved', 'bad_clock', 'naive_clock', 'future_clock'}:
            value = {'unobserved': None, 'bad_clock': 'unknown', 'naive_clock': '2026-01-01T00:00:00',
                'future_clock': (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()}[case]
            queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?', (value, target))
        elif case == 'wrong_error':
            queue.db.execute("UPDATE articles SET error_code='http_403' WHERE url=?", (target,))
        queue.db.execute("INSERT INTO queue_settings VALUES ('failure_identity_metadata_attempts:v1:ettoday:20261017','2')")
        before = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
        queue.db.commit()
    marker = 'ettoday_static_threads_recheck_v10:' + new.sources['ettoday'].data_fingerprint
    with closing(NewsQueue(tmp_path, new)) as queue:
        selected = {urls[1]} | ({target} if case == 'eligible' else set()) if ancestry == 'exact' else set()
        for url, previous in before.items():
            expected = {**previous, 'state': 'retry'} if url in selected else previous
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == expected
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key='failure_identity_metadata_attempts:v1:ettoday:20261017'").fetchone()[0] == '2'
        proof = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
        if ancestry != 'exact':
            assert proof is None
            return
        journal = json.loads(proof[0])
        assert len(journal['rows']) == len(selected)
        assert journal['attempts_due_error_and_observation_preserved'] and not journal['accepted_as_bodies']
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified'")
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert all(row[0] == 'article_identity_not_verified' for row in queue.db.execute('SELECT state FROM articles'))
