"""Source-warning supplements and explicit source-absence outcomes."""
import hashlib
import html
import json

from bs4 import BeautifulSoup
import pytest

from downloader import news_fulltext_publishers as publishers
from downloader.news_fulltext_policy import NewsError, extract_article
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE
from test_news_cnyes_captions import caption_page
from test_news_cnyes_html import BODY, CLOCK, URL, empty_page, short_page, source


IMAGE = 'https://cimg.cnyes.cool/source-original.png'
LINKS = ('https://public.example.org/fund', 'https://public.example.org/research')
WARNING = '本公司所分析個別有價證券僅供參考，投資人應獨立判斷，審慎評估並自負投資風險。'
FIGURE = (f'<figure><img src="{IMAGE}"><p>▶進一步了解資產管理處 <a href="{LINKS[0]}">{LINKS[0]}</a></p>'
          f'<p>▶閱讀更多精彩文章 <a href="{LINKS[1]}">{LINKS[1]}</a></p><p class="warning">{WARNING}</p></figure>')
TEXT = BeautifulSoup(FIGURE, 'html.parser').figure.get_text('\n', strip=True)
EMPTY = '<p>&nbsp;</p>\n<div>\n<p>&nbsp;</p>\n</div>'


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=a', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_complete_warning_text_and_links_preserved_without_changing_narrative(cnyes_config, tmp_path, parser, url, streamed):
    cnyes_config['providers'][0]['html_parser'] = parser
    article = extract_article(caption_page(current_figure=FIGURE, visible_figure=FIGURE, schema_caption=TEXT,
        streamed=streamed), source(cnyes_config, tmp_path), page_url=url)
    assert article['body'] == BODY * 8 and article['published_at_raw'] == CLOCK
    assert json.loads(article['authors_json']) == [AUTHOR]
    assert article['verified_image_captions_json'] is None
    assert json.loads(article['verified_article_notices_json']) == [{
        'text': TEXT, 'kind': 'same_article_image_figure_one_line_warning_supplement', 'image_sources': [IMAGE],
        'source_links': [{'href': link, 'text': link} for link in LINKS]}]
    assert 'source_current_image_figure_one_line_warning_comparison_v1' in article['article_identity_basis']
    assert 'preserved_separately' in article['body_completeness']
    assert article['historical_point_in_time'] is False


MUTATIONS = [
    pytest.param(lambda f: f.replace(IMAGE, IMAGE + '?changed=1'), id='image'),
    pytest.param(lambda f: f.replace(LINKS[0], LINKS[1]), id='href'),
    pytest.param(lambda f: f.replace('資產管理處', '未知文章'), id='prefix'),
    pytest.param(lambda f: f.replace('僅供參考', '另加報酬 123'), id='warning'),
    pytest.param(lambda f: f.replace('<figure>', '<figure hidden>'), id='hidden-root'),
    pytest.param(lambda f: f.replace('<img ', '<img hidden '), id='hidden-image'),
    pytest.param(lambda f: f.replace('<p>', '<p style="display:none">', 1), id='hidden-paragraph'),
    pytest.param(lambda f: f.replace('<a ', '<a hidden ', 1), id='hidden-link'),
    pytest.param(lambda f: f.replace('<figure>', '<figure><!--unreviewed-->'), id='comment'),
    pytest.param(lambda f: f.replace('<img ', '<img onload="unknown()" '), id='event-image'),
    pytest.param(lambda f: f.replace('<a ', '<a onclick="unknown()" ', 1), id='event-link'),
    pytest.param(lambda f: f.replace(LINKS[0], 'https://user:secret@example.org/'), id='credential-url'),
    pytest.param(lambda f: f.replace(LINKS[0], 'javascript:unknown()'), id='script-url'),
    pytest.param(lambda f: f.replace(LINKS[0], 'http://[invalid'), id='malformed-url'),
    pytest.param(lambda f: f.replace('class="warning"', 'class="unknown"'), id='class'),
    pytest.param(lambda f: f.replace(WARNING, '<span>' + WARNING + '</span>'), id='nested-warning'),
    pytest.param(lambda f: f.replace(WARNING, WARNING + '<br>額外數字 789'), id='extra-warning'),
    pytest.param(lambda f: f.replace('</figure>', '<p>原始價格 -12.50%</p></figure>'), id='extra-prose'),
    pytest.param(lambda f: f.replace('</figure>', '<p>&nbsp;</p></figure>'), id='extra-blank'),
    pytest.param(lambda f: f.replace('>▶進一步', '><span>▶進一步').replace('管理處 ', '管理處</span> '), id='nested-prefix'),
]


@pytest.mark.parametrize('change', MUTATIONS)
@pytest.mark.parametrize('representation', ['visible', 'current'])
def test_complete_current_visible_dom_not_only_text_must_agree(cnyes_config, tmp_path, change, representation):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=change(FIGURE) if representation == 'current' else FIGURE,
            visible_figure=change(FIGURE) if representation == 'visible' else FIGURE, schema_caption=TEXT),
            source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('change', MUTATIONS[2:])
def test_unknown_or_hidden_content_not_authorized_by_three_matching_views(cnyes_config, tmp_path, change):
    fragment = change(FIGURE)
    text = BeautifulSoup(fragment, 'html.parser').figure.get_text('\n', strip=True)
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=fragment, visible_figure=fragment, schema_caption=text),
                        source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('schema', [WARNING, TEXT[::-1], TEXT + '\n未知數字 789', TEXT.replace(LINKS[0], LINKS[1])])
def test_whole_schema_cannot_be_replaced_with_warning_only(cnyes_config, tmp_path, schema):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=FIGURE, visible_figure=FIGURE, schema_caption=schema),
                        source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
def test_source_dom_identity_is_nonmutating(parser):
    figure = BeautifulSoup(FIGURE, parser).figure
    before = str(figure)
    key = publishers._cnyes_structured_figure_identity(figure)
    assert key is not None and key[1][2] == hashlib.sha256(before.encode()).hexdigest()
    assert str(figure) == before


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('body', ['(NOK)', '(NOK.A)', '(BRK-B)'])
def test_symbol_only_source_has_no_narrative_body(cnyes_config, tmp_path, parser, body):
    cnyes_config['providers'][0]['html_parser'] = parser
    with pytest.raises(NewsError, match='^source_body_quote_only$'):
        extract_article(short_page(body=body), source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('body', ['NOK', '(nok)', '(ABCDEFGHIJKLMNOPQ)', 'tiny'])
def test_symbol_grammar_does_not_turn_arbitrary_short_prose_into_source_stub(cnyes_config, tmp_path, body):
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(short_page(body=body), source(cnyes_config, tmp_path), page_url=URL)


def test_genuine_short_chinese_prose_is_not_reclassified_as_symbol_stub(cnyes_config, tmp_path):
    body = '(NOK) 來源確實有公開敘述。'
    article = extract_article(short_page(body=body), source(cnyes_config, tmp_path), page_url=URL)
    assert article['body'] == body


@pytest.mark.parametrize('options', [
    {'visible': '(NOK) other'}, {'change': {'payment': 1}}, {'change': {'payment': False}},
    {'change': {'content': '&lt;p&gt;(OTHER)&lt;/p&gt;'}}, {'change': {'publishAt': 1388534401}},
    {'change': {'video': 'video body'}}, {'schema_change': {'datePublished': '2014-01-01T00:00:00'}},
    {'conflict': True}, {'duplicate_key': True},
])
def test_source_stub_still_requires_whole_public_current_clock_proof(cnyes_config, tmp_path, options):
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(short_page(body='(NOK)', **options), source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('content', [EMPTY, html.escape(EMPTY)])
def test_exact_observed_nbsp_paragraph_div_is_empty_not_downloaded_body(cnyes_config, tmp_path, parser, content):
    cnyes_config['providers'][0]['html_parser'] = parser
    with pytest.raises(NewsError, match='^source_article_empty_template$'):
        extract_article(empty_page(change={'content': content, 'video': ''}), source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('content', [EMPTY.replace('&nbsp;', ' ', 1), EMPTY.replace('<div>', '<div hidden>'),
    EMPTY.replace('<div>', '<div><!--unreviewed-->'), EMPTY + '<script>hidden body</script>',
    EMPTY + '<img src="x">', EMPTY + '<p>real text</p>', html.escape(html.escape(EMPTY))])
def test_no_generic_textless_markup_relaxation(cnyes_config, tmp_path, content):
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(empty_page(change={'content': content, 'video': ''}), source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('change', [{'payment': 1}, {'payment': False}, {'publishAt': 1388534401},
    {'video': 'video body'}, {'title': 'another title'}, {'url': URL.replace('/42', '/43')}])
def test_empty_source_requires_identity_clock_and_no_video(cnyes_config, tmp_path, change):
    with pytest.raises(NewsError):
        extract_article(empty_page(change={'content': html.escape(EMPTY), 'video': '', **change}),
                        source(cnyes_config, tmp_path), page_url=URL)

