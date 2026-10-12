"""Source image/paragraph forests retain prose, labels, clocks and scope."""
import hashlib
import json
from unittest.mock import patch

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE, mobile_page
from test_news_cnyes_captions import caption_page, NOTICE_LINES
from test_news_cnyes_html import BODY, CLOCK, URL, source

IMAGES = [f'https://cimg.cnyes.cool/source-image-paragraph-{n}.jpg' for n in range(3)]
LINK = 'https://public.example.org/original?quantity=123&loss=-12.50'
HREF = LINK.replace('&', '&amp;')
WARNING = '<p class="warning">' + '<br>\n'.join(NOTICE_LINES) + '</p>'
LEAF = f'<figure><img src="{IMAGES[0]}" alt="alt不得供正文999"></figure>'
DIRECT = f'<img src="{IMAGES[0]}" alt="alt不得供正文999">'
PLAIN = '<p><span>原始來源 -12.50%，交易量 1,234 股，不是圖說或免責聲明。</span></p>'
STRONG = '<p><strong>來源章節 [四巫日] 與 270 分鐘。</strong></p>'
REFERENCE = f'<p>來源連結 <a href="{HREF}">原文 -8.25%</a>。</p>'
QUOTE = ('<p>股票 <a href="https://www.cnyes.com/twstock/2330" target="_self" '
    'rel="noopener noreferrer" data-ga-target="news" data-ga-event-name="Click_Quote" '
    'data-ga-section="News_Article_文中行情" data-ga-click-item="TWS:2330:STOCK:COMMON">2330-TW</a>。</p>')
FORESTS = [
    '<figure>' + DIRECT + PLAIN + '</figure>',
    '<figure>' + LEAF + PLAIN + STRONG + REFERENCE + '</figure>',
    '<figure>' + DIRECT + PLAIN + LEAF.replace(IMAGES[0], IMAGES[1]) + QUOTE + REFERENCE + '</figure>',
    '<figure>' + LEAF + PLAIN + STRONG + REFERENCE + WARNING + '</figure>',
    '<figure>' + DIRECT + '<p><strong>\xa0</strong><strong>原文 -12.50% 與 1,234 股。</strong></p>'
        + f'<p><a href="{HREF}" target="_blank"><strong>原始來源連結。</strong></a></p></figure>',
    '<figure>' + LEAF + '<p>\xa0</p></figure><figure>' + LEAF.replace(IMAGES[0], IMAGES[1])
        + PLAIN + STRONG + REFERENCE + WARNING + '</figure>',
]
CAPTION_P = '<figure>' + DIRECT + '<p>圖文：來源照片 -12.50%，1,234 股。</p></figure>'
CAPTION_SPAN = (f'<figure><img src="{IMAGES[0]}" alt="來源照片-12.50%，1,234股。">'
    '<span>來源照片 -12.50%，1,234 股。</span></figure>')
KIND = 'same_article_image_paragraph_narrative'
CAPTION_KIND = 'same_article_labelled_image_paragraph_caption'
CAPTION_BASIS = 'same_article_visible_labelled_image_paragraph_and_source_image'


@pytest.mark.parametrize('forest', FORESTS)
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=b', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_all_original_paragraphs_numbers_and_links_enter_body_in_order(cnyes_config, tmp_path,
        forest, parser, url, streamed):
    cnyes_config['providers'][0]['html_parser'] = parser
    text = BeautifulSoup(forest, parser).get_text('\n', strip=True)
    result = extract_article(caption_page(current_figure=forest, visible_figure=forest,
        schema_caption=text, streamed=streamed), source(cnyes_config, tmp_path), page_url=url)
    assert result['body'] == BODY * 8 + '\n' + text
    assert '999' not in result['body'] and '首圖' not in result['body']
    assert '-12.50%' in result['body'] and '1,234' in result['body']
    assert result['published_at_raw'] == CLOCK and json.loads(result['authors_json']) == [AUTHOR]
    assert result['body_sha256'] == hashlib.sha256(result['body'].encode()).hexdigest()
    assert result['verified_image_captions_json'] is None
    notes = json.loads(result['verified_article_notices_json'])
    assert notes and all(note['kind'] == KIND and note['text'] == '' for note in notes)
    assert 'source_current_image_paragraph_narrative_body_comparison_v1' in result['article_identity_basis']
    assert 'verified_current_visible_image_paragraph_narrative_body' in result['body_completeness']
    assert 'source_current_image_leaf_prose_body_comparison_v1' not in result['article_identity_basis']
    assert 'source_current_nested_mixed_figure_schema_comparison_v1' not in result['article_identity_basis']


@pytest.mark.parametrize('forest', FORESTS)
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_all_source_prose_can_be_inside_image_container_without_becoming_empty(cnyes_config, tmp_path, forest, parser):
    cnyes_config['providers'][0]['html_parser'] = parser
    text = BeautifulSoup(forest, parser).get_text('\n', strip=True)
    result = extract_article(caption_page(body='', current_figure=forest, visible_figure=forest,
        schema_caption=text), source(cnyes_config, tmp_path), page_url=MOBILE)
    assert result['body'] == text
    assert result['body_sha256'] == hashlib.sha256(text.encode()).hexdigest()


@pytest.mark.parametrize('forest', [CAPTION_P, CAPTION_SPAN])
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('streamed', [False, True])
def test_explicit_source_labels_are_separate_actual_captions_not_alt_or_prose(cnyes_config, tmp_path, forest, parser, streamed):
    cnyes_config['providers'][0]['html_parser'] = parser
    text = BeautifulSoup(forest, parser).get_text(' ', strip=True)
    result = extract_article(caption_page(current_figure=forest, visible_figure=forest,
        schema_caption=text, streamed=streamed), source(cnyes_config, tmp_path), page_url=MOBILE)
    assert result['body'] == BODY * 8
    assert json.loads(result['verified_image_captions_json']) == [{
        'text': text, 'image_src': IMAGES[0], 'structure_basis': CAPTION_BASIS}]
    assert json.loads(result['verified_article_notices_json']) == [{
        'text': '', 'kind': CAPTION_KIND, 'image_sources': [IMAGES[0]], 'source_links': []}]
    assert result['published_at_raw'] == CLOCK and json.loads(result['authors_json']) == [AUTHOR]
    assert 'source_current_labelled_image_paragraph_caption_comparison_v1' in result['article_identity_basis']
    assert 'source_current_nested_figure_schema_comparison_v1' not in result['article_identity_basis']
    assert 'source_current_linked_caption_figure_comparison_v1' not in result['article_identity_basis']


CHANGES = [
    pytest.param(lambda f: f.replace('-12.50', '-12.51'), id='financial-value'),
    pytest.param(lambda f: f.replace('1,234', '1,235'), id='quantity'),
    pytest.param(lambda f: f.replace(IMAGES[0], IMAGES[1]), id='image-identity'),
    pytest.param(lambda f: f.replace('<figure>', '<figure hidden>', 1), id='hidden-root'),
    pytest.param(lambda f: f.replace('<img ', '<img hidden ', 1), id='hidden-image'),
    pytest.param(lambda f: f.replace('<p>', '<p style="display:none">', 1), id='hidden-paragraph'),
    pytest.param(lambda f: f.replace('<span>', '<span hidden>', 1), id='hidden-inline'),
    pytest.param(lambda f: f.replace('<figure>', '<figure><!--unknown-->', 1), id='comment'),
    pytest.param(lambda f: f.replace('<p>', '<p data-unknown="true">', 1), id='unknown-paragraph'),
    pytest.param(lambda f: f.replace('src="', 'onload="unreviewed()" src="', 1), id='image-event'),
    pytest.param(lambda f: f.replace('<span>', '<span data-unknown="true">', 1), id='unknown-inline'),
    pytest.param(lambda f: f.replace('<span>', '<span><em>').replace('</span>', '</em></span>'), id='unreviewed-depth'),
    pytest.param(lambda f: f + '<p>不可丟棄的額外正文。</p>', id='extra-prose'),
    pytest.param(lambda f: f.replace(IMAGES[0], 'javascript:unknown()'), id='script-image'),
    pytest.param(lambda f: f.replace(IMAGES[0], 'https://user:secret@example.org/private'), id='credential-image'),
    pytest.param(lambda f: f.replace(IMAGES[0], 'http://[invalid'), id='malformed-image'),
]


@pytest.mark.parametrize('mutate', CHANGES)
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_changed_visible_dom_cannot_supply_or_drop_any_source_word(cnyes_config, tmp_path, parser, mutate):
    cnyes_config['providers'][0]['html_parser'] = parser
    forest = FORESTS[0]
    text = BeautifulSoup(forest, parser).get_text('\n', strip=True)
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=forest, visible_figure=mutate(forest),
            schema_caption=text), source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('options', [
    {'changes': {'payment': 1}}, {'changes': {'newsId': 99}}, {'changes': {'author': '另一作者'}},
    {'changes': {'publishAt': 1388534401}}, {'schema_changes': {'datePublished': '2014-01-01T00:00:01.000Z'}},
    {'schema_changes': {'author': [{'name': '另一作者'}]}}, {'schema_changes': {'articleBody': '不能改寫來源完整正文。'}},
    {'schema_changes': {'isAccessibleForFree': False}},
])
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_new_paragraph_path_never_replaces_author_clock_rights_or_whole_schema(cnyes_config, tmp_path, options, parser):
    cnyes_config['providers'][0]['html_parser'] = parser
    forest = FORESTS[0]
    text = BeautifulSoup(forest, parser).get_text('\n', strip=True)
    def altered_page(**kwargs):
        return mobile_page(**{**kwargs, 'changes': {**kwargs.get('changes', {}), **options.get('changes', {})},
            'schema_changes': {**kwargs.get('schema_changes', {}), **options.get('schema_changes', {})}})
    with patch('test_news_cnyes_captions.mobile_page', altered_page):
        payload = caption_page(current_figure=forest, visible_figure=forest, schema_caption=text)
    with pytest.raises(NewsError):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_explicit_blank_image_placeholder_cannot_be_an_article(cnyes_config, tmp_path, parser):
    cnyes_config['providers'][0]['html_parser'] = parser
    blank = '<figure>' + LEAF + '<p>\xa0</p></figure>'
    with pytest.raises(NewsError):
        extract_article(caption_page(body='', current_figure=blank, visible_figure=blank,
            schema_caption=''), source(cnyes_config, tmp_path), page_url=MOBILE)
