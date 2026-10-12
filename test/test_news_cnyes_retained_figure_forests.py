"""Three independently retained figure shapes preserve text rather than waive proof."""
import hashlib
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_publishers import _cnyes_structured_figure_identity
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE
from test_news_cnyes_captions import caption_page, NOTICE_LINES, NOTICE_TEXT
from test_news_cnyes_html import BODY, CLOCK, URL, source


IMAGES = [f'https://cimg.cnyes.cool/retained-source-forest-{n}.jpg' for n in range(6)]
LINK = 'https://public.example.org/original?value=123&source=article'
HREF = LINK.replace('&', '&amp;')
CAPTION = '來源製表附註：原始數字 -12.50%，1,234 股。'
LINKED = (f'<figure><a href="{HREF}"><img src="{IMAGES[0]}" alt="alt不供正文999"></a>'
    f'<figcaption><p>{CAPTION}</p><figure><a href="{HREF}"><img src="{IMAGES[1]}"></a>'
    '</figure></figcaption></figure>')
WARNING = '<p class="warning">' + '<br>\n'.join(NOTICE_LINES) + '</p>'
PLACEHOLDERS = (f'<figure><img src="{IMAGES[0]}"><figure><img src="{IMAGES[1]}"></figure>'
    + WARNING + '<p>\xa0</p><figure>\xa0</figure><p>\xa0</p></figure>')
LEAVES = [f'<figure><img src="{image}" alt="alt不供正文999"></figure>' for image in IMAGES[:5]]
PROSE_FIRST = '<p>原始收益 -12.50%，交易量 1,234 股。</p><p>來源正文 31.5%，不是風險聲明。</p>'
PROSE_LINK = f'<p>原始參考 <a href="{HREF}">來源連結</a>。</p>'
PROSE_LAST = '<p>原始股票 4915，獲利 20%。</p>'
DISCLOSURE = '<p class="warning">原始來源自述與聯絡說明，不刪除或改寫任何字句。</p>'
PROSE = ('<figure>' + ''.join(LEAVES[:3]) + PROSE_FIRST + ''.join(LEAVES[3:])
    + PROSE_LINK + PROSE_LAST + DISCLOSURE + '</figure>')
PROSE_TEXT = BeautifulSoup(PROSE, 'html.parser').get_text('\n', strip=True)
CASES = [('linked', LINKED, CAPTION), ('placeholders', PLACEHOLDERS, NOTICE_TEXT), ('prose', PROSE, PROSE_TEXT)]


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=b', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
@pytest.mark.parametrize('kind,fragment,text', CASES)
def test_entire_ordered_prose_or_separate_caption_warning_references(cnyes_config, tmp_path, parser, url, streamed,
        kind, fragment, text):
    cnyes_config['providers'][0]['html_parser'] = parser
    result = extract_article(caption_page(current_figure=fragment, visible_figure=fragment,
        schema_caption=text, streamed=streamed), source(cnyes_config, tmp_path), page_url=url)
    assert result['published_at_raw'] == CLOCK and json.loads(result['authors_json']) == [AUTHOR]
    assert result['body'] == BODY * 8 + ('\n' + PROSE_TEXT if kind == 'prose' else '')
    assert '999' not in result['body'] and '首圖' not in result['body']
    assert result['body_sha256'] == hashlib.sha256(result['body'].encode()).hexdigest()
    notes = json.loads(result['verified_article_notices_json'])
    if kind == 'linked':
        assert json.loads(result['verified_image_captions_json']) == [{
            'text': CAPTION, 'image_src': IMAGES[0],
            'structure_basis': 'same_article_visible_linked_caption_and_source_image_order'}]
        assert notes == [{'text': '', 'kind': 'same_article_linked_caption_figure',
            'image_sources': IMAGES[:2], 'source_links': [{'href': LINK, 'text': ''}] * 2}]
    elif kind == 'placeholders':
        assert result['verified_image_captions_json'] is None
        assert notes == [{'text': NOTICE_TEXT, 'kind': 'same_article_warning_placeholder_figure',
            'image_sources': IMAGES[:2], 'source_links': []}]
    else:
        assert result['verified_image_captions_json'] is None
        assert notes == [{'text': '', 'kind': 'same_article_image_leaf_prose_figure',
            'image_sources': IMAGES[:5], 'source_links': [{'href': LINK, 'text': '來源連結'}]}]
        assert '-12.50%' in result['body'] and '31.5%' in result['body'] and '20%' in result['body']
        assert result['body'].index('1,234') < result['body'].index('31.5') < result['body'].index('4915')
        assert 'all_source_prose_disclosures_and_values_preserved_in_original_order' in result['body_completeness']
        assert 'source_current_image_leaf_prose_body_comparison_v1' in result['article_identity_basis']
        assert 'caption_wrapped_narrative' not in result['article_identity_basis']


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('streamed', [False, True])
def test_all_article_prose_inside_image_forest_is_not_an_empty_body(cnyes_config, tmp_path, parser, streamed):
    cnyes_config['providers'][0]['html_parser'] = parser
    result = extract_article(caption_page(body='', current_figure=PROSE, visible_figure=PROSE,
        schema_caption=PROSE_TEXT, streamed=streamed), source(cnyes_config, tmp_path), page_url=MOBILE)
    assert result['body'] == PROSE_TEXT
    assert 'source_current_image_leaf_prose_body_comparison_v1' in result['article_identity_basis']


CHANGES = [
    pytest.param(lambda f: f.replace(IMAGES[0], IMAGES[1]), id='duplicate-image'),
    pytest.param(lambda f: f.replace(IMAGES[1], 'https://public.example.org/different.jpg'), id='different-image'),
    pytest.param(lambda f: f.replace(IMAGES[0], 'javascript:unreviewed()'), id='script-image'),
    pytest.param(lambda f: f.replace(IMAGES[0], 'https://user:secret@example.org/private'), id='credential-image'),
    pytest.param(lambda f: f.replace(IMAGES[0], 'http://[invalid'), id='malformed-image'),
    pytest.param(lambda f: f.replace('<figure>', '<figure hidden>', 1), id='hidden-root'),
    pytest.param(lambda f: f.replace('<figure>', '<figure><!--unknown-->', 1), id='comment'),
    pytest.param(lambda f: f.replace('<img ', '<img onload="unknown()" ', 1), id='image-event'),
    pytest.param(lambda f: f.replace('<img ', '<img hidden ', 1), id='hidden-image'),
    pytest.param(lambda f: f.replace('<p>', '<p style="display:none">', 1), id='hidden-paragraph'),
    pytest.param(lambda f: f.replace('<figure>', '<figure class="unknown">', 1), id='unknown-attributes'),
    pytest.param(lambda f: f.replace('</figure>', '<iframe src="https://public.example.org/embed"></iframe></figure>', 1), id='unknown-node'),
]


@pytest.mark.parametrize('kind,fragment,text', CASES)
@pytest.mark.parametrize('change', CHANGES)
@pytest.mark.parametrize('side', ['current', 'visible'])
def test_one_sided_source_mutation_preserves_strict_entire_dom_witness(cnyes_config, tmp_path, kind, fragment,
        text, change, side):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=change(fragment) if side == 'current' else fragment,
            visible_figure=change(fragment) if side == 'visible' else fragment, schema_caption=text),
            source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('kind,fragment,text', CASES)
@pytest.mark.parametrize('change', CHANGES[:1] + CHANGES[2:])
def test_matching_schema_does_not_allow_unknown_hidden_or_unsafe_nodes(cnyes_config, tmp_path, kind, fragment,
        text, change):
    markup = change(fragment)
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=markup, visible_figure=markup,
            schema_caption=BeautifulSoup(markup, 'html.parser').get_text('\n', strip=True)),
            source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('kind,fragment,text', CASES)
@pytest.mark.parametrize('change', [lambda text: text + '來源沒有的值123', lambda text: text[::-1]])
def test_full_schema_not_only_visible_current_must_match(cnyes_config, tmp_path, kind, fragment, text, change):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=fragment, visible_figure=fragment, schema_caption=change(text)),
            source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('markup', [
    LINKED.replace(HREF, 'javascript:unknown()'),
    LINKED.replace(HREF, 'https://user:secret@example.org/private'),
    LINKED.replace('<a ', '<a onclick="unknown()" ', 1),
    LINKED.replace('<figcaption>', '<figcaption hidden>'),
    LINKED.replace('<p>', '<p><span>').replace('</p>', '</span></p>'),
    LINKED.replace('</a>', '來源多出正文123</a>', 1),
    LINKED.replace(f'<a href="{HREF}"><img src="{IMAGES[1]}"></a>', f'<a href="https://public.example.org/other"><img src="{IMAGES[1]}"></a>'),
    PLACEHOLDERS.replace('\xa0', ' '),
    PLACEHOLDERS.replace('\xa0', '新增真實值123', 1),
    PLACEHOLDERS.replace(NOTICE_LINES[0], '未知內容不得當聲明'),
    PLACEHOLDERS.replace('<p class="warning">', '<p class="unknown">'),
    PLACEHOLDERS.replace('<br>', '<br style="display:none">', 1),
    PROSE.replace(HREF, 'javascript:unknown()'),
    PROSE.replace('<a ', '<a aria-hidden="true" '),
    PROSE.replace('<p class="warning">', '<p class="unknown">'),
    PROSE.replace('<p>', '<p><span>', 1).replace('</p>', '</span></p>', 1),
    PROSE.replace(DISCLOSURE, '<p class="warning"><span>未知樣式123</span></p>'),
    PROSE.replace(LEAVES[3], '<figure>' + LEAVES[3] + '</figure>'),
])
def test_each_typed_shape_keeps_its_original_structure_boundary(cnyes_config, tmp_path, markup):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=markup, visible_figure=markup,
            schema_caption=BeautifulSoup(markup, 'html.parser').get_text('\n', strip=True)),
            source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('kind,fragment,text', CASES)
def test_identity_is_nonmutating_complete_ordered_forest_witness(parser, kind, fragment, text):
    figure = BeautifulSoup(fragment, parser).figure
    before = str(figure)
    key = _cnyes_structured_figure_identity(figure)
    assert key is not None and key[1][2] == hashlib.sha256(before.encode()).hexdigest()
    assert str(figure) == before


def test_moved_image_with_same_text_and_image_list_does_not_keep_the_dom_witness():
    # Move within an admissible interleaving without changing separate lists.
    moved = PROSE.replace(LEAVES[2], '').replace(PROSE_FIRST, PROSE_FIRST + LEAVES[2])
    original = _cnyes_structured_figure_identity(BeautifulSoup(PROSE, 'lxml').figure)
    other = _cnyes_structured_figure_identity(BeautifulSoup(moved, 'lxml').figure)
    assert original is not None and other is not None
    assert original[0] == other[0] and original[1][:2] == other[1][:2]
    assert original[1][2] != other[1][2]
