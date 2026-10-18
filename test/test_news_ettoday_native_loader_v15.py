"""Closed source loader/padding grammars, never social text or an author waiver."""
from contextlib import closing
from dataclasses import replace
import hashlib
import json

from bs4 import BeautifulSoup
import pytest

import downloader.news_fulltext_ettoday as adapter
from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from scripts.audit_news_selected_articles import audit
from test_news_fulltext import FakeResponse, http_fixture
import test_news_ettoday as native
import test_news_ettoday_clock_v2 as clocks
import test_news_ettoday_static_imgur_v9 as imgur
import test_news_ettoday_static_threads_v10 as threads
import test_news_ettoday_split_reporter_v13 as split


@pytest.fixture(autouse=True)
def synthetic_decorative_icon(monkeypatch):
    monkeypatch.setattr(adapter, '_THREADS_ICON_SHA256', hashlib.sha256(threads.ICON.encode()).hexdigest())


def page(parser='lxml', *, mode='twitter', host='finance'):
    soup = threads.page(parser, host=host) if mode == 'padding' else native.page(host, parser)
    body = soup.select_one('.story')
    if mode in {'padding', 'both'}:
        caption = body.find_all('p', recursive=False)[1].strong
        caption.append(soup.new_tag('br'))
        caption.append('\n')
    if mode in {'twitter', 'both'}:
        body.append(soup.new_tag('script', attrs={'async': '', 'src': 'https://platform.twitter.com/widgets.js',
                                                 'charset': 'utf-8'}))
    return BeautifulSoup(str(soup), parser)


def extract(soup, parser='lxml', version=15, host='finance'):
    source = replace(clocks.registry(version).sources['ettoday'], html_parser=parser)
    return extract_article(str(soup).encode(), source, page_url=native.URLS[host == 'www'])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('mode', ['twitter', 'padding', 'both'])
def test_whole_source_values_and_original_preserved_without_post_or_reporter_inference(parser, mode):
    soup = page(parser, mode=mode); original = str(soup)
    old_soup = threads.page(parser) if mode == 'padding' else native.page('finance', parser)
    baseline = extract(old_soup, parser, version=14)
    result = extract(soup, parser)
    assert str(soup) == original
    for key in ('body', 'body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw', 'verified_image_captions_json'):
        assert result[key] == baseline[key], key
    notices = json.loads(result['verified_article_notices_json'])
    if mode in {'twitter', 'both'}:
        loader, = (n for n in notices if n['kind'] == 'native_orphan_static_twitter_loader_declaration')
        assert not loader['script_executed'] and not loader['external_request_sent']
        assert not loader['post_identity_or_contents_inferred'] and not loader['native_post_markup_present']
        assert loader['source_script_attributes']['charset'] == 'utf-8'
    if mode in {'padding', 'both'}:
        assert any(n['kind'] == 'native_plain_break_caption_editor_padding' for n in notices)
    with pytest.raises(NewsError, match='article_(identity|attribution)_not_verified'):
        extract(soup, parser, version=14)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['query', 'http', 'charset', 'extra_attribute', 'inline_code', 'whitespace',
    'missing_async', 'duplicate', 'nested', 'native_tweet', 'tweet_iframe', 'unknown_script', 'hidden'])
def test_unknown_or_nonempty_loader_and_actual_post_markup_remain_rejected(parser, change):
    soup = page(parser); body = soup.select_one('.story')
    script = body.select_one('script[src="https://platform.twitter.com/widgets.js"]')
    if change == 'query': script['src'] += '?extra=1'
    elif change == 'http': script['src'] = script['src'].replace('https:', 'http:')
    elif change == 'charset': script['charset'] = 'UTF-8'
    elif change == 'extra_attribute': script['type'] = 'text/javascript'
    elif change == 'inline_code': script.string = 'unreviewed code'
    elif change == 'whitespace': script.string = ' '
    elif change == 'missing_async': del script['async']
    elif change == 'duplicate': body.append(soup.new_tag('script', attrs=dict(script.attrs)))
    elif change == 'nested':
        wrapper = soup.new_tag('p'); body.append(wrapper); wrapper.append(script.extract())
    elif change == 'native_tweet':
        widget = soup.new_tag('blockquote', attrs={'class': 'twitter-tweet'}); widget.string = 'unverified post'; body.append(widget)
    elif change == 'tweet_iframe': body.append(soup.new_tag('iframe', src='https://platform.twitter.com/embed'))
    elif change == 'unknown_script': body.append(soup.new_tag('script', src='https://other.example/code.js'))
    else: script['hidden'] = ''
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup, parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['attribute', 'text_after_break', 'text_before_caption', 'two_breaks',
    'missing_space', 'missing_reporter', 'second_reporter', 'wire', 'hidden'])
def test_caption_padding_cannot_hide_text_or_invent_reporter(parser, change):
    soup = page(parser, mode='padding'); body = soup.select_one('.story')
    strong = body.find_all('p', recursive=False)[1].strong
    if change == 'attribute': strong.br['class'] = 'unreviewed'
    elif change == 'text_after_break': strong.contents[-1].replace_with('unverified text')
    elif change == 'text_before_caption': strong.insert(0, 'unverified text')
    elif change == 'two_breaks': strong.insert(1, soup.new_tag('br'))
    elif change == 'missing_space': strong.contents[-1].extract()
    elif change == 'missing_reporter': soup.select_one('#credit').decompose()
    elif change == 'second_reporter':
        extra = soup.new_tag('p'); extra.string = '記者另一記者／台北報導'; body.append(extra)
    elif change == 'wire': soup.select_one('#credit').string = '中央社／台北報導'
    else: strong['hidden'] = ''
    with pytest.raises(NewsError, match='article_(attribution_not_verified|identity_not_verified|rights_or_scope_excluded)'):
        extract(soup, parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('base', [native.page, imgur.page, threads.page, split.page])
def test_existing_valid_originals_keep_every_extractor_field(parser, base):
    soup = base('finance', parser) if base is native.page else base(parser, host='finance')
    assert extract(soup, parser, 14) == extract(soup, parser, 15)


@pytest.mark.parametrize('mode', ['twitter', 'padding'])
def test_finance_only_native_extension_never_expands_other_branded_hosts(mode):
    with pytest.raises(NewsError):
        extract(page(mode=mode, host='www'), host='www')
    assert adapter.ettoday_request_identity('https://house.ettoday.net/news/3159604') is None


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('padding', [False, True])
def test_source_proved_540px_threads_ui_preserves_caption_reporter_and_whole_prose(parser, padding):
    soup = page(parser, mode='padding') if padding else threads.page(parser)
    widget = soup.select_one('.text-post-media')
    widget['style'] = widget['style'].replace('max-width:650px;', 'max-width:540px;')
    original = str(soup)
    result = extract(soup, parser)
    baseline = extract(threads.page(parser), parser, version=14)
    assert str(soup) == original
    for key in ('body', 'body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw',
            'verified_image_captions_json', 'author_attribution_detail_json'):
        assert result[key] == baseline[key], key
    notice, = (n for n in json.loads(result['verified_article_notices_json'])
        if n['kind'] == 'native_static_threads_preview_ui_declaration')
    assert notice['source_presentation_variant'] == 'native_540px_preview_width'
    assert notice['source_blockquote_attributes'] == widget.attrs
    with pytest.raises(NewsError):
        extract(soup, parser, version=14)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', ['unknown_width', 'hidden_css', 'extra_attribute', 'prose', 'unknown_icon',
    'label', 'iframe', 'inline_script', 'mismatched_post', 'no_reporter', 'www'])
def test_compact_width_is_not_a_general_widget_style_or_source_waiver(parser, change):
    host = 'www' if change == 'www' else 'finance'
    soup = threads.page(parser, host=host); widget = soup.select_one('.text-post-media')
    widget['style'] = widget['style'].replace('max-width:650px;', 'max-width:540px;')
    if change == 'unknown_width': widget['style'] = widget['style'].replace('540px', '539px')
    elif change == 'hidden_css': widget['style'] += 'display:none;'
    elif change == 'extra_attribute': widget['data-unreviewed'] = '1'
    elif change == 'prose': widget.a.append('unverified embedded prose')
    elif change == 'unknown_icon': widget.path['d'] = 'M1 1L2 2'
    elif change == 'label': widget.a.div.find_all('div', recursive=False)[1].string = 'unknown label'
    elif change == 'iframe': widget.a.append(soup.new_tag('iframe', src='https://www.threads.com/post/example'))
    elif change == 'inline_script': widget.find_next_sibling('script').string = 'unreviewed code'
    elif change == 'mismatched_post': widget['id'] = 'ig-tp-OtherId1234'
    elif change == 'no_reporter': soup.select_one('#credit').decompose()
    with pytest.raises(NewsError):
        extract(soup, parser, host=host)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_formal_fake_http_original_durable_receipt_and_replay_without_social_requests(tmp_path, parser):
    config = clocks.config(15); config['providers'][0]['html_parser'] = parser
    raw = str(page(parser, mode='both')).encode(); url = native.URLS[0]
    http, session = http_fixture(config, tmp_path, {
        'https://finance.ettoday.net/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'), url: FakeResponse(raw)})
    try:
        with closing(NewsQueue(tmp_path, http.registry)) as queue:
            queue.add_articles([{'url': url}])
            row = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
            article = fetch_article(http, row)
            assert article['raw_response_body'] == raw and not article['publish'] and not article['remote_training_export']
            queue.accept([article])
            assert verify_bodies(tmp_path)['accepted'] and audit(tmp_path, [url], registry=http.registry)['accepted']
            assert [r[0] for r in session.calls] == ['https://finance.ettoday.net/robots.txt', url]
    finally:
        http.close()
