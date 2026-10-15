"""Typed source components preserve all words, values and positions honestly."""
import hashlib
import json
from unittest.mock import patch

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_publishers import _cnyes_ordered_figure_forest_identity
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE, mobile_page
from test_news_cnyes_captions import caption_page, NOTICE_LINES
from test_news_cnyes_html import BODY, CLOCK, URL, source

IMAGES = [f'https://cimg.cnyes.cool/ordered-native-leaf-{n}.jpg' for n in range(18)]
LEAVES = [f'<figure><img src="{image}" alt="alt不供正文999"></figure>' for image in IMAGES]
DIRECT = f'<img src="{IMAGES[0]}" alt="alt不供正文999">'
LINK = 'https://public.example.org/source?loss=-12.50&shares=1234'
HREF = LINK.replace('&', '&amp;')
WARNING = '<p class="warning">' + '<br>\n'.join(NOTICE_LINES) + '</p>'
LICENSED_TEXT = ('(110) 金管投顧字第 009 號，本公司不保證獲利，投資人應獨立判斷，審慎評估並自負投資風險。 '
    '原始證券投資顧問股份有限公司 原始來源分析師客服專線：02-25426555')
PROSE = ('<figure>' + DIRECT + LEAVES[1] + '<p>原始收益 -12.50%，交易量 1,234 股。</p>'
    f'<p>原始連結 <a href="{HREF}"><strong>原文連結</strong></a>。</p>'
    '<p>來源文字聲明，不改寫或刪除。</p>' + WARNING + '</figure>')
QUOTE = ('<a href="https://www.cnyes.com/twstock/8358" target="_self" rel="noopener noreferrer" '
    'data-ga-target="news" data-ga-event-name="Click_Quote" data-ga-section="News_Article_文中行情" '
    'data-ga-click-item="TWS:8358:STOCK:COMMON">8358-TW</a>')
LONG_PROSE = ('<figure>' + ''.join(LEAVES[:12]) + '<p>原始收益 -12.50%，1,234 股。</p>'
    + '<p>來源股票 ' + QUOTE + '。</p>' + ''.join(f'<p>原始段落 {n}，未加工數值 -{n}.50%。</p>' for n in range(17))
    + LEAVES[12] + '<p class="warning">' + LICENSED_TEXT + '</p></figure>')
FOOTER_PROSE = ('<figure>' + DIRECT + '<p>\xa0</p>' + LEAVES[1]
    + '<p>原始收益 -12.50%，交易量 1,234 股。</p>' + f'<p>來源網址 <a href="{HREF}">完整原文</a>。</p>'
    + '<div>' + WARNING + '</div></figure>')
ORPHAN = ('<figure><figcaption><span style="letter-spacing: 0.7px; text-align: justify;">'
    '原始建案 4800 坪，投資 70%，另方投資 30%。</span></figcaption></figure>')
CREDIT = ('<figure>' + DIRECT + '<p>\xa0</p>' + ''.join(LEAVES[1:4])
    + '<p>作者：原始投顧 原始署名 分析師<br>\xa0</p></figure>')
WRAPPED_CREDIT = '<figure>' + DIRECT.replace(IMAGES[0], IMAGES[4]) + CREDIT + '</figure>'
PLACEHOLDER = '<figure>' + ''.join(LEAVES[:2]) + '<p>\xa0</p></figure>'
HEADING = ('<figure><h6><em>一、<a href="' + HREF + '">原始指標</a>續升 -12.50%，交易量 1,234 股。</em></h6>'
    + LEAVES[0] + '</figure>')
LICENSED = (f'<figure><img src="{IMAGES[0]}" alt="{LICENSED_TEXT}">'
    + '<p class="warning">' + LICENSED_TEXT + '</p></figure>')
LABEL = ('<figure><p><span style="background-color:#FFFF00">比較表</span></p>'
    + ''.join(LEAVES[:2]) + '<p>\xa0</p></figure>')
INVITATION = '<figure><span>《獲得更多「獨家」盤中資訊》</span></figure>'
NARRATIVE = 'same_article_ordered_image_leaf_paragraphs'
CASES = [
    (NARRATIVE, PROSE), (NARRATIVE, LONG_PROSE), (NARRATIVE, FOOTER_PROSE),
    ('same_article_unpaired_figcaption_text', ORPHAN),
    ('same_article_image_forest_source_credit', CREDIT), ('same_article_image_forest_source_credit', WRAPPED_CREDIT),
    ('same_article_image_leaf_placeholder_forest', PLACEHOLDER),
    ('same_article_image_leaf_numbered_heading', HEADING),
    ('same_article_image_licensed_notice', LICENSED),
    ('same_article_image_leaf_comparison_label', LABEL),
    ('same_article_figure_source_invitation', INVITATION),
]


@pytest.mark.parametrize('kind,markup', CASES)
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=b', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_exact_source_body_or_honest_component_and_order(cnyes_config, tmp_path, kind, markup, parser, url, streamed):
    cnyes_config['providers'][0]['html_parser'] = parser
    figure = BeautifulSoup(markup, parser).figure
    key = _cnyes_ordered_figure_forest_identity(figure)
    assert key is not None and key[2] == kind
    text = figure.get_text('\n', strip=True)
    # A textless forest by itself needs no schema repair. Actual4842433 also
    # contains a separate visible invitation; preserve both ordered roles.
    companion = INVITATION if kind == 'same_article_image_leaf_placeholder_forest' else ''
    schema_text = text + (BeautifulSoup(companion, parser).get_text('\n', strip=True) if companion else '')
    result = extract_article(caption_page(current_figure=markup + companion, visible_figure=markup + companion,
        schema_caption=schema_text, streamed=streamed), source(cnyes_config, tmp_path), page_url=url)
    assert result['body'] == BODY * 8 + ('\n' + text if kind == NARRATIVE else '')
    assert result['body_sha256'] == hashlib.sha256(result['body'].encode()).hexdigest()
    assert result['published_at_raw'] == CLOCK and json.loads(result['authors_json']) == [AUTHOR]
    assert result['verified_image_captions_json'] is None
    notes = json.loads(result['verified_article_notices_json'])
    expected_notes = [{'text': '' if kind == NARRATIVE else text, 'kind': kind,
        'image_sources': list(key[1][0]), 'source_links': [{'href': href, 'text': label} for href, label in key[1][1]],
        'source_dom_sha256': key[1][2]}]
    if companion:
        extra = _cnyes_ordered_figure_forest_identity(BeautifulSoup(companion, parser).figure)
        expected_notes.append({'text': extra[0], 'kind': extra[2], 'image_sources': [], 'source_links': [],
            'source_dom_sha256': extra[1][2]})
    assert notes == expected_notes
    assert 'source_current_ordered_figure_forest_comparison_v1' in result['article_identity_basis']
    assert 'source_current_figure_source_byline_comparison_v1' not in result['article_identity_basis']
    assert 'ordered_native_figure_labels_credits_notices_and_DOM_witnesses_preserved_separately' in result['body_completeness']
    assert '首圖' not in result['body'] and '999' not in result['body']
    if kind == NARRATIVE:
        assert '-12.50%' in result['body'] and '1,234' in result['body']
        assert 'verified_current_visible_ordered_image_leaf_paragraphs_body' in result['body_completeness']


@pytest.mark.parametrize('markup', [PROSE, LONG_PROSE, FOOTER_PROSE])
@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_all_source_paragraphs_can_be_inside_forest_without_a_fake_empty_body(cnyes_config, tmp_path, markup, parser):
    cnyes_config['providers'][0]['html_parser'] = parser
    text = BeautifulSoup(markup, parser).get_text('\n', strip=True)
    result = extract_article(caption_page(body='', current_figure=markup, visible_figure=markup,
        schema_caption=text), source(cnyes_config, tmp_path), page_url=MOBILE)
    assert result['body'] == text


CHANGES = [
    pytest.param(lambda m: m.replace('-12.50', '-12.51'), id='changed-value'),
    pytest.param(lambda m: m.replace('1,234', '1,235'), id='changed-shares'),
    pytest.param(lambda m: m.replace(IMAGES[0], IMAGES[2]), id='image'),
    pytest.param(lambda m: m.replace(HREF, HREF + '&amp;other=99'), id='source-link'),
    pytest.param(lambda m: m.replace('<figure>', '<figure hidden>', 1), id='hidden'),
    pytest.param(lambda m: m.replace('<figure>', '<figure><!--unknown-->', 1), id='comment'),
    pytest.param(lambda m: m.replace('<p>', '<p style="display:none">', 1), id='hidden-prose'),
    pytest.param(lambda m: m.replace('<p>', '<p data-unknown="true">', 1), id='unknown-prose'),
    pytest.param(lambda m: m.replace('<img ', '<img onload="unreviewed()" ', 1), id='image-event'),
    pytest.param(lambda m: m.replace(IMAGES[0], 'javascript:unreviewed()'), id='script-image'),
    pytest.param(lambda m: m.replace(IMAGES[0], 'https://user:secret@example.org/image'), id='credential-image'),
    pytest.param(lambda m: m.replace('</figure>', '<iframe src="https://public.example.org"></iframe></figure>', 1), id='unknown-node'),
]


@pytest.mark.parametrize('change', CHANGES)
@pytest.mark.parametrize('side', ['current', 'visible'])
def test_entire_one_sided_dom_or_value_change_stays_a_gap(cnyes_config, tmp_path, change, side):
    text = BeautifulSoup(PROSE, 'html.parser').get_text('\n', strip=True)
    options = {'current_figure': PROSE, 'visible_figure': PROSE, 'schema_caption': text}
    options[side + '_figure'] = change(PROSE)
    with pytest.raises(NewsError):
        extract_article(caption_page(**options), source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('markup', [
    PROSE.replace('<figure>', '<figure hidden>', 1), PROSE.replace('<p>', '<p data-unknown="true">', 1),
    PROSE.replace('<figure>', '<figure><!--unknown-->', 1), PROSE.replace(IMAGES[0], IMAGES[1]),
    PROSE.replace(HREF, 'javascript:unreviewed()'), PROSE.replace('<p class="warning">', '<p class="unknown">'),
    PROSE.replace(LEAVES[1], '<figure><p>不能扁平化未知巢狀文字。</p>' + LEAVES[1] + '</figure>'),
    PROSE.replace(LEAVES[1], '<figure>' + LEAVES[1] + '</figure>'),
    LICENSED.replace('客服專線', '未知說明'), LICENSED.replace('原始來源分析師', '其他來源分析師', 1),
    ORPHAN.replace('justify;', 'justify; display:none;'), ORPHAN.replace('<span ', '<span hidden ', 1),
    ORPHAN.replace('span', 'strong'), ORPHAN.replace('<span ', '<span data-unknown="true" ', 1),
    HEADING.replace('一、', '未知標籤：'), HEADING.replace('<em>', '<em hidden>'),
    LABEL.replace('background-color:#FFFF00', 'background-color:#FFFF00;display:none'),
    INVITATION.replace('獨家', '未知'), PLACEHOLDER.replace('<p>\xa0</p>', '<p data-unknown="true">來源原始數值123。</p>'),
    '<figure>' + ''.join(LEAVES[:17]) + '<p>原始收益 -12.50%。</p></figure>',
])
def test_same_text_and_schema_do_not_waive_structure_visibility_or_roles(cnyes_config, tmp_path, markup):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=markup, visible_figure=markup,
            schema_caption=BeautifulSoup(markup, 'html.parser').get_text('\n', strip=True)),
            source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('option', [
    {'changes': {'payment': 1}}, {'changes': {'newsId': 99}}, {'changes': {'author': '另一作者'}},
    {'changes': {'publishAt': 1388534401}}, {'schema_changes': {'isAccessibleForFree': False}},
    {'schema_changes': {'author': [{'name': '另一作者'}]}}, {'schema_changes': {'articleBody': '不可補寫來源文字。'}},
])
def test_forest_never_replaces_rights_author_or_publication_clock(cnyes_config, tmp_path, option):
    def changed(**kwargs):
        return mobile_page(**{**kwargs, 'changes': {**kwargs.get('changes', {}), **option.get('changes', {})},
            'schema_changes': {**kwargs.get('schema_changes', {}), **option.get('schema_changes', {})}})
    with patch('test_news_cnyes_captions.mobile_page', changed):
        page = caption_page(current_figure=PROSE, visible_figure=PROSE,
            schema_caption=BeautifulSoup(PROSE, 'html.parser').get_text('\n', strip=True))
    with pytest.raises(NewsError):
        extract_article(page, source(cnyes_config, tmp_path), page_url=MOBILE)


def test_entire_dom_witness_detects_moved_image_with_unchanged_text_and_image_list():
    moved = PROSE.replace(LEAVES[1], '').replace('<p>來源文字', LEAVES[1] + '<p>來源文字')
    keys = [_cnyes_ordered_figure_forest_identity(BeautifulSoup(markup, 'lxml').figure) for markup in (PROSE, moved)]
    assert all(keys) and keys[0][0] == keys[1][0] and keys[0][1][:2] == keys[1][1][:2]
    assert keys[0][1][2] != keys[1][1][2]


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_real_paragraph_in_image_forest_is_not_mislabeled_as_an_empty_placeholder(cnyes_config, tmp_path, parser):
    cnyes_config['providers'][0]['html_parser'] = parser
    markup = PLACEHOLDER.replace('<p>\xa0</p>', '<p>來源原始数值 -12.50%，交易量1,234股。</p>')
    figure = BeautifulSoup(markup, parser).figure
    assert _cnyes_ordered_figure_forest_identity(figure)[2] == NARRATIVE
    text = figure.get_text('\n', strip=True)
    result = extract_article(caption_page(current_figure=markup, visible_figure=markup,
        schema_caption=text), source(cnyes_config, tmp_path), page_url=MOBILE)
    assert result['body'] == BODY * 8 + '\n' + text
    assert json.loads(result['verified_article_notices_json'])[0]['kind'] == NARRATIVE
