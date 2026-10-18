"""Native static Facebook preview is neither newsroom prose nor a video fetch."""
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json

from bs4 import BeautifulSoup, Comment
import pytest

from downloader.news_fulltext_ettoday import ETTODAY_V20_PROVED_STATIC_FACEBOOK_URLS, _FACEBOOK_SDK_LOADER
from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_queue import NewsQueue, ettoday_static_facebook_video_parser_repair, utc_now
import test_news_ettoday as native
import test_news_ettoday_clock_v2 as clocks
import test_news_ettoday_static_twitter_v19 as twitter


VIDEO = ('<div class="fb-video" data-allowfullscreen="1" '
    'data-href="/ETtodayPOLITICS/videos/vb.627711347355634/802790233181077/?type=3">\n'
    '<div class="fb-xfbml-parse-ignore"><blockquote '
    'cite="https://www.facebook.com/ETtodayPOLITICS/videos/802790233181077/">'
    '<a href="https://www.facebook.com/ETtodayPOLITICS/videos/802790233181077/">'
    'Original static video label -12.5%, 123456 shares and 0.75.<br>\n</a>'
    '</blockquote></div>\n</div>')
ROOT = '<div id="fb-root"><strong>&nbsp;</strong></div>'


def page(parser='lxml', *, host='finance'):
    soup = native.page(host, parser)
    for node in list(BeautifulSoup('<script>' + _FACEBOOK_SDK_LOADER + '</script>\n' + VIDEO + ROOT,
            'html.parser').contents):
        soup.select_one('.story').append(node.extract())
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', version=20, host='finance'):
    return extract_article(str(soup).encode(), replace(clocks.registry(version).sources['ettoday'], html_parser=parser),
        page_url=native.URLS[host == 'www'])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_original_newsroom_values_and_attribution_remain_exactly_unchanged(parser):
    soup = page(parser)
    original = str(soup)
    expected = extract(native.page('finance', parser), parser, version=19)
    actual = extract(soup, parser)
    assert str(soup) == original
    for key in ('title', 'body', 'body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw',
            'verified_image_captions_json', 'author_attribution_detail_json'):
        assert actual[key] == expected[key], key
    assert native.PROSE in actual['body'] and 'Original static video label' not in actual['body']
    notes = json.loads(actual['verified_article_notices_json'])
    video, = (n for n in notes if n['kind'] == 'native_static_facebook_video_preview_declaration')
    assert video['source_visible_video_label'] == 'Original static video label -12.5%, 123456 shares and 0.75.\n'
    assert video['source_quote_attributes']['cite'] == video['source_link_attributes']['href']
    assert video['source_loader_dom_lf_sha256'] == '8f98a1d665b42a1acdeeb0d86dd646c022a52efac31b3427e32d15a581838d83'
    assert video['raw_declarations_preserved'] and video['source_root_nonbreaking_space_preserved']
    assert video['excluded_from_newsroom_body']
    assert all(not video[k] for k in ('external_request_sent', 'script_executed',
        'media_content_accepted', 'video_label_is_reporter_attribution'))
    assert [n for n in notes if n['kind'] != video['kind']] == json.loads(expected['verified_article_notices_json'])
    assert 'video_label_preserved_in_raw_not_newsroom_body' in actual['body_completeness']
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup, parser, version=19)


def test_dom_newline_witness_is_not_mistaken_for_response_byte_identity():
    raw = str(page()).replace('\n', '\r\n').encode()
    values = [extract_article(raw, replace(clocks.registry(20).sources['ettoday'], html_parser=p),
        page_url=native.URLS[0]) for p in ('lxml', 'html.parser')]
    assert values[0] == values[1]
    notice, = (n for n in json.loads(values[0]['verified_article_notices_json'])
        if n['kind'] == 'native_static_facebook_video_preview_declaration')
    assert notice['text_witness_basis'] == 'HTML_input_CRLF_CR_to_LF; exact_bytes_in_shared_original'


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['script_code', 'script_comment', 'script_attrs', 'nested_script',
    'missing_script', 'extra_script', 'widget_attrs', 'hidden_widget', 'nested_widget', 'extra_widget',
    'missing_widget', 'changed_video', 'foreign_page', 'unknown_route_query', 'wrong_allowfullscreen',
    'extra_widget_text', 'widget_comment', 'missing_ignore', 'ignore_attrs', 'hidden_ignore',
    'quote_attrs', 'second_quote', 'wrong_quote_video', 'wrong_anchor_host', 'wrong_anchor_video',
    'anchor_attrs', 'anchor_comment', 'anchor_image', 'anchor_extra_text', 'anchor_long_label',
    'anchor_empty_label', 'anchor_br_attrs', 'anchor_missing_newline', 'extra_root', 'missing_root',
    'root_attrs', 'root_prose', 'root_strong_attrs', 'root_plain_space', 'root_unknown_child',
    'root_not_last_tag', 'foreign_reporter', 'missing_reporter', 'missing_newsroom_prose',
    'paid', 'wrong_canonical', 'wrong_clock', 'video_iframe', 'intervening_newsroom_prose'])
def test_unreviewed_code_markup_identity_and_access_remain_rejected(parser, change):
    soup = page(parser)
    story, widget = soup.select_one('.story'), soup.select_one('.fb-video')
    script, root = story.find('script', recursive=False), soup.select_one('#fb-root')
    quote, anchor = widget.blockquote, widget.a
    if change == 'script_code': script.string = _FACEBOOK_SDK_LOADER + 'alert(1)'
    elif change == 'script_comment': script.string = _FACEBOOK_SDK_LOADER + '//unobserved'
    elif change == 'script_attrs': script['defer'] = ''
    elif change == 'nested_script': script.wrap(soup.new_tag('p'))
    elif change == 'missing_script': script.decompose()
    elif change == 'extra_script': story.append(soup.new_tag('script'))
    elif change == 'widget_attrs': widget['data-width'] = '500'
    elif change == 'hidden_widget': widget['hidden'] = ''
    elif change == 'nested_widget': widget.wrap(soup.new_tag('p'))
    elif change == 'extra_widget': story.append(BeautifulSoup(VIDEO, parser).select_one('.fb-video'))
    elif change == 'missing_widget': widget.decompose()
    elif change == 'changed_video': widget['data-href'] = widget['data-href'].replace('802790233181077', '802790233181078')
    elif change == 'foreign_page': widget['data-href'] = widget['data-href'].replace('ETtodayPOLITICS', 'OtherPage')
    elif change == 'unknown_route_query': widget['data-href'] += '&other=1'
    elif change == 'wrong_allowfullscreen': widget['data-allowfullscreen'] = 'true'
    elif change == 'extra_widget_text': widget.append('unattributed prose')
    elif change == 'widget_comment': widget.append(Comment('unknown token'))
    elif change == 'missing_ignore': widget.div.unwrap()
    elif change == 'ignore_attrs': widget.div['id'] = 'unknown'
    elif change == 'hidden_ignore': widget.div['style'] = 'display:none'
    elif change == 'quote_attrs': quote['class'] = 'unknown'
    elif change == 'second_quote': widget.div.append(soup.new_tag('blockquote'))
    elif change == 'wrong_quote_video': quote['cite'] = quote['cite'].replace('802790233181077', '802790233181078')
    elif change == 'wrong_anchor_host': anchor['href'] = anchor['href'].replace('www.facebook.com', 'example.org')
    elif change == 'wrong_anchor_video': anchor['href'] = anchor['href'].replace('802790233181077', '802790233181078')
    elif change == 'anchor_attrs': anchor['target'] = '_blank'
    elif change == 'anchor_comment': anchor.insert(0, Comment('unknown token'))
    elif change == 'anchor_image': anchor.append(soup.new_tag('img', src='https://example.org/image.jpg'))
    elif change == 'anchor_extra_text': anchor.append('unattributed prose')
    elif change == 'anchor_long_label': anchor.contents[0].replace_with('x' * 257)
    elif change == 'anchor_empty_label': anchor.contents[0].replace_with(' ')
    elif change == 'anchor_br_attrs': anchor.br['id'] = 'unknown'
    elif change == 'anchor_missing_newline': anchor.contents[-1].extract()
    elif change == 'extra_root': story.append(BeautifulSoup(ROOT, parser).select_one('#fb-root'))
    elif change == 'missing_root': root.decompose()
    elif change == 'root_attrs': root['style'] = 'display:none'
    elif change == 'root_prose': root.strong.string = 'must not delete this text'
    elif change == 'root_strong_attrs': root.strong['class'] = 'unknown'
    elif change == 'root_plain_space': root.strong.string = ' '
    elif change == 'root_unknown_child': root.append(soup.new_tag('p'))
    elif change == 'root_not_last_tag': story.append(soup.new_tag('p'))
    elif change == 'foreign_reporter': story.select_one('#credit').string = '中央社記者報導'
    elif change == 'missing_reporter': story.select_one('#credit').decompose()
    elif change == 'missing_newsroom_prose':
        for node in list(story.find_all('p', recursive=False)):
            if node.get('id') != 'credit' and not node.find('img') and not node.strong:
                node.decompose()
        story.h2.decompose()
    elif change == 'paid': native.update_schema(soup, isAccessibleForFree=False)
    elif change == 'wrong_canonical': soup.select_one('link[rel=canonical]')['href'] += '1'
    elif change == 'wrong_clock': native.update_schema(soup, datePublished='2026-10-13T22:19:00+08:00')
    elif change == 'video_iframe': story.append(soup.new_tag('iframe', src='https://www.facebook.com/'))
    elif change == 'intervening_newsroom_prose': script.insert_after(soup.new_tag('p'))
    with pytest.raises(NewsError):
        extract(soup, parser)


@pytest.mark.parametrize('version', range(1, 21))
def test_supported_plain_articles_keep_all_values(version):
    assert extract(native.page(), version=version) == extract(native.page(), version=1)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_previously_accepted_twitter_and_www_are_not_changed(parser):
    assert twitter.extract(twitter.page(parser), parser, version=19) == twitter.extract(twitter.page(parser), parser, version=20)
    assert extract(native.page('www', parser), parser, host='www') == extract(native.page('www', parser), parser, version=19, host='www')
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(page(parser, host='www'), parser, host='www')
    with pytest.raises(ValueError):
        clocks.registry(clocks.future_version())


def test_exact_capture_ancestry_preserves_access_capacity_and_source_clocks():
    old, new = clocks.registry(19).sources['ettoday'], clocks.registry(20).sources['ettoday']
    assert ettoday_static_facebook_video_parser_repair(new, old.data_fingerprint)
    assert not ettoday_static_facebook_video_parser_repair(new, clocks.registry(18).sources['ettoday'].data_fingerprint)
    assert old.data_fingerprint != new.data_fingerprint
    assert old.authorization_fingerprint == new.authorization_fingerprint
    assert old.http_capacity_source_fingerprint == new.http_capacity_source_fingerprint
    assert new.parser_repair_capture_history[-1] == old.data_fingerprint


@pytest.mark.parametrize('excluded', ['none', 'future_due', 'unobserved', 'future_observation', 'naive_observation',
    'leased', 'has_body', 'has_partition', 'other_url', 'other_failure', 'wrong_error', 'complete'])
def test_retry_is_once_and_only_for_the_proved_missing_due_unleased_head(tmp_path, excluded):
    old, new = clocks.registry(19), clocks.registry(20)
    url = ETTODAY_V20_PROVED_STATIC_FACEBOOK_URLS[0]
    if excluded == 'other_url': url = url.replace('635788', '635789')
    with closing(NewsQueue(tmp_path, old)) as queue:
        queue.add_articles([{'url': url}])
        values = {'state': 'article_identity_not_verified', 'error_code': 'article_identity_not_verified',
            'attempts': 3, 'due': 99, 'last_checked_at_utc': utc_now()}
        if excluded == 'future_due': values['due'] = (datetime.now(timezone.utc) + timedelta(days=1)).timestamp()
        elif excluded == 'unobserved': values['last_checked_at_utc'] = None
        elif excluded == 'future_observation': values['last_checked_at_utc'] = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        elif excluded == 'naive_observation': values['last_checked_at_utc'] = '2026-10-18T00:00:00'
        elif excluded == 'leased': values.update(lease_owner='other_owner', lease_until=1)
        elif excluded == 'has_body': values['body_sha256'] = 'a' * 64
        elif excluded == 'has_partition': values['parquet_path'] = 'source.parquet'
        elif excluded == 'other_failure': values['state'] = 'article_attribution_not_verified'
        elif excluded == 'wrong_error': values['error_code'] = 'different_error'
        elif excluded == 'complete': values['state'] = 'complete'
        with queue.db:
            queue.db.execute('UPDATE articles SET ' + ','.join(k + '=?' for k in values) + ' WHERE url=?', (*values.values(), url))
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
    with closing(NewsQueue(tmp_path, new)) as queue:
        after = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        assert after == {**before, 'state': 'retry' if excluded == 'none' else before['state']}
        marker = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
            ('ettoday_static_facebook_video_recheck_v20:' + new.sources['ettoday'].data_fingerprint,)).fetchone()[0])
        assert len(marker['rows']) == (excluded == 'none') and not marker['accepted_as_bodies']
        with queue.db:
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified' WHERE url=?", (url,))
        queue.recheck_ettoday_static_facebook_video(old.sources['ettoday'].data_fingerprint)
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (url,)).fetchone()[0] == 'article_identity_not_verified'
