"""Entire source paragraph figures, exact metadata and no image/credit guessing."""
import hashlib
import json
from unittest.mock import patch

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_publishers import _cnyes_matched_figure_paragraph_identity
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE, mobile_page
from test_news_cnyes_captions import caption_page, NOTICE_LINES
from test_news_cnyes_html import BODY, CLOCK, URL, source

KIND = 'same_article_matched_figure_paragraphs'
IMAGE = 'https://cimg.cnyes.cool/source-paragraph-image.jpg'
CAPTION = '來源照片圖說，不是正文或作者。'
P = '<p>原始收益 -12.50%，交易量 1,234 股，原始段落完整保留。</p>'
A = '<p>來源連結 <a href="https://public.example.org/source?price=-12.50&amp;shares=1234" target="_blank">原始標籤</a>。</p>'
LEAF = '<figure><img src="https://cimg.cnyes.cool/source-paragraph-other.jpg"></figure>'
FORESTS = [
    '<figure>' + P * 3 + A + '</figure>',
    '<figure><img src="' + IMAGE + '"><figcaption>' + CAPTION + '</figcaption>' + P * 4 + '</figure>',
    '<figure><img src="' + IMAGE + '">' + P + A + '</figure>',
    '<figure>' + P + '<img src="' + IMAGE + '"></figure>',
    '<figure><img src="' + IMAGE + '">' + P + LEAF + P * 3 + A + '</figure>',
    '<figure><p class="warning">' + '<br>'.join(NOTICE_LINES) + '</p></figure>',
]


@pytest.mark.parametrize('forest', FORESTS)
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=b', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_source_paragraphs_captions_links_numbers_and_order(cnyes_config, tmp_path, forest, parser, url, streamed):
    cnyes_config['providers'][0]['html_parser'] = parser
    parsed = BeautifulSoup(forest, parser)
    full = parsed.get_text('\n', strip=True)
    key = _cnyes_matched_figure_paragraph_identity(parsed.figure)
    assert key is not None and key[2] == KIND
    for caption in parsed.find_all('figcaption'):
        caption.decompose()
    prose = parsed.get_text('\n', strip=True)
    result = extract_article(caption_page(current_figure=forest, visible_figure=forest,
        schema_caption=full, streamed=streamed), source(cnyes_config, tmp_path), page_url=url)
    assert result['body'] == BODY * 8 + '\n' + prose
    assert result['body_sha256'] == hashlib.sha256(result['body'].encode()).hexdigest()
    assert result['published_at_raw'] == CLOCK and json.loads(result['authors_json']) == [AUTHOR]
    assert 'source_current_matched_figure_paragraphs_body_comparison_v1' in result['article_identity_basis']
    assert 'verified_current_visible_matched_figure_paragraphs_body' in result['body_completeness']
    assert CAPTION not in result['body'] and '首圖' not in result['body']
    pairs = json.loads(result['verified_image_captions_json'] or '[]')
    assert pairs == ([{'text': CAPTION, 'image_src': IMAGE,
        'structure_basis': 'same_article_matched_figure_explicit_caption_and_image'}] if CAPTION in full else [])
    notes = json.loads(result['verified_article_notices_json'])
    assert notes == [{'text': '', 'kind': KIND, 'image_sources': list(key[1][0]),
        'source_links': [{'href': href, 'text': text} for href, text in key[1][1]], 'source_dom_sha256': key[1][2]}]


@pytest.mark.parametrize('forest', FORESTS)
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_all_paragraphs_may_be_inside_figure_without_losing_the_article(cnyes_config, tmp_path, forest, parser):
    cnyes_config['providers'][0]['html_parser'] = parser
    parsed = BeautifulSoup(forest, parser)
    full = parsed.get_text('\n', strip=True)
    for caption in parsed.find_all('figcaption'):
        caption.decompose()
    expected = parsed.get_text('\n', strip=True)
    result = extract_article(caption_page(body='', current_figure=forest, visible_figure=forest,
        schema_caption=full), source(cnyes_config, tmp_path), page_url=MOBILE)
    assert result['body'] == expected


CHANGES = [
    lambda f: f.replace('-12.50', '-12.51'), lambda f: f.replace('1,234', '1,235'),
    lambda f: f.replace(IMAGE, IMAGE + '?other=1'), lambda f: f.replace(CAPTION, '不同圖說'),
    lambda f: f.replace('<figure>', '<figure hidden>', 1),
    lambda f: f.replace('<figure>', '<figure aria-hidden="true">', 1),
    lambda f: f.replace('<p>', '<p style="display:none">', 1),
    lambda f: f.replace('<p>', '<p data-unknown="true">', 1),
    lambda f: f.replace('<figure>', '<figure><!--unknown-->', 1),
    lambda f: f.replace('<img ', '<img onload="unknown()" ', 1),
    lambda f: f.replace('<figure>', '<figure><script>unknown()</script>', 1),
    lambda f: f.replace('</figure>', '<iframe src="https://public.example.org"></iframe></figure>', 1),
    lambda f: f.replace(IMAGE, 'https://user:secret@example.org/private'),
    lambda f: f.replace(IMAGE, 'javascript:unknown()'),
    lambda f: f.replace('</figure>', '<p>不可忽略的另一段資料 -99 股。</p></figure>', 1),
]


@pytest.mark.parametrize('change', CHANGES)
@pytest.mark.parametrize('side', ['current', 'visible'])
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_entire_dom_value_or_role_difference_cannot_be_hidden(cnyes_config, tmp_path, change, side, parser):
    cnyes_config['providers'][0]['html_parser'] = parser
    forest = FORESTS[1]
    text = BeautifulSoup(forest, parser).get_text('\n', strip=True)
    changes = {side + '_figure': change(forest)}
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=changes.get('current_figure', forest),
            visible_figure=changes.get('visible_figure', forest), schema_caption=text),
            source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('options', [
    {'changes': {'payment': 1}}, {'changes': {'newsId': 99}}, {'changes': {'author': '不同作者'}},
    {'changes': {'publishAt': 1388534401}}, {'changes': {'title': '不同標題'}},
    {'schema_changes': {'datePublished': '2014-01-01T00:00:01.000Z'}},
    {'schema_changes': {'author': [{'name': '不同作者'}]}},
    {'schema_changes': {'articleBody': '不能改寫完整來源。'}},
    {'schema_changes': {'isAccessibleForFree': False}},
])
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_source_identity_clock_author_access_and_schema_still_required(cnyes_config, tmp_path, options, parser):
    cnyes_config['providers'][0]['html_parser'] = parser
    forest = FORESTS[1]
    text = BeautifulSoup(forest, parser).get_text('\n', strip=True)
    def altered(**kwargs):
        return mobile_page(**{**kwargs,
            'changes': {**kwargs.get('changes', {}), **options.get('changes', {})},
            'schema_changes': {**kwargs.get('schema_changes', {}), **options.get('schema_changes', {})}})
    with patch('test_news_cnyes_captions.mobile_page', altered):
        payload = caption_page(current_figure=forest, visible_figure=forest, schema_caption=text)
    with pytest.raises(NewsError):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('forest', [
    '<figure>' + ('<p>' + 'x' * 100 + '</p>') * 33 + '</figure>',
    '<figure><figure><figure><figure>' + P + '</figure></figure></figure></figure>',
    '<figure><img src="' + IMAGE + '"><img src="' + IMAGE + '">' + P + '</figure>',
    '<figure><p><img src="' + IMAGE + '">來源文字。</p></figure>',
    '<figure><figcaption>不能推測圖像配對。</figcaption>' + P + '</figure>',
    '<figure>' + P + '<figcaption>不能推測圖像配對。</figcaption>' + LEAF + '</figure>',
    '<figure><p></p></figure>', '<figure>未分段的原始文字 -12.50。</figure>',
    '<figure><img src="' + IMAGE + '"><p>TG：<a href="https://t.me.evil.example/unknown">未知連結</a></p>' + P + '</figure>',
    '<figure><img src="' + IMAGE + '"><p>梅姊 LINE ID : <a href="https://lin.ee/unknown">未知標籤</a></p>' + P + '</figure>',
])
def test_untyped_ambiguous_or_unbounded_figure_stays_a_gap(forest):
    assert _cnyes_matched_figure_paragraph_identity(BeautifulSoup(forest, 'lxml').figure) is None
