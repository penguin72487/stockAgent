"""A typed mixed figure forest, never a generic schema/visibility waiver."""
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


IMAGES = [f'https://cimg.cnyes.cool/owned-mixed-figure-{n}.jpg' for n in range(4)]
LINK = 'https://www.example.org/owned-source?id=123'
HEADING = '<原始支撐點>'
FIRST = '<p><strong style="letter-spacing:0.7px; text-align:justify">&lt;原始支撐點&gt;</strong></p>'
VALUES = '<p>原始指數 16,838，支撐 15,159，報酬 -12.50%。</p>'
LEAVES = ''.join(f'<figure>\n<img src="{src}">\n</figure>' for src in IMAGES[1:3])
AFTER = '<p><strong>&lt;反彈觀察&gt;</strong></p><p>原始收盤 16,248，不改數值。</p>'
INVITATION = '<p><span>更多盤中即時資訊</span><span>請點擊以下連結↓↓↓</span></p>'
CTA = f'<figure><img src="{IMAGES[3]}">\n{INVITATION}\n</figure>'
WARNING = '<p class="warning">' + '<br>\n'.join(NOTICE_LINES) + '</p>'
FOOTER = f'<figure>\n<p>社群網站連結 : <a href="{LINK}">{LINK}</a></p>\n{WARNING}\n</figure>'
FOREST = f'<figure><img src="{IMAGES[0]}">\n{FIRST}{VALUES}{LEAVES}{AFTER}{CTA}{FOOTER}<p></p></figure>'
TEXT = BeautifulSoup(FOREST, 'html.parser').figure.get_text('\n', strip=True)


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=b', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_entire_mixed_source_text_images_links_and_numbers_remain_distinct(cnyes_config, tmp_path, parser, url, streamed):
    cnyes_config['providers'][0]['html_parser'] = parser
    result = extract_article(caption_page(current_figure=FOREST, visible_figure=FOREST,
        schema_caption=TEXT, streamed=streamed), source(cnyes_config, tmp_path), page_url=url)
    assert result['body'] == BODY * 8 and result['published_at_raw'] == CLOCK
    assert json.loads(result['authors_json']) == [AUTHOR]
    assert result['verified_image_captions_json'] is None
    assert json.loads(result['verified_article_notices_json']) == [{
        'text': TEXT, 'kind': 'same_article_nested_mixed_figure_text', 'image_sources': IMAGES,
        'source_links': [{'href': LINK, 'text': LINK}],
    }]
    assert '16,838' in TEXT and '15,159' in TEXT and '16,248' in TEXT and '-12.50%' in TEXT
    assert 'source_current_nested_mixed_figure_schema_comparison_v1' in result['article_identity_basis']
    assert 'mixed_nested_figure_text_preserved_separately_in_verified_article_notices_json' in result['body_completeness']
    assert HEADING not in result['body'] and NOTICE_TEXT not in result['body']


CHANGES = [
    pytest.param(lambda f: f.replace('16,838', '16,839'), id='changed-number'),
    pytest.param(lambda f: f.replace('-12.50%', '12.50%'), id='changed-sign'),
    pytest.param(lambda f: f.replace(IMAGES[1], IMAGES[2]), id='duplicate-image'),
    pytest.param(lambda f: f.replace(IMAGES[1], 'https://cimg.cnyes.cool/other.jpg'), id='changed-image'),
    pytest.param(lambda f: f.replace(IMAGES[1], 'https://user:secret@example.org/image'), id='credential-image'),
    pytest.param(lambda f: f.replace(IMAGES[1], 'http://[invalid'), id='malformed-image'),
    pytest.param(lambda f: f.replace(IMAGES[1], 'javascript:unknown()'), id='script-image'),
    pytest.param(lambda f: f.replace(LINK, 'https://www.example.org/other'), id='changed-link'),
    pytest.param(lambda f: f.replace(LINK, 'https://user:secret@example.org/link'), id='credential-link'),
    pytest.param(lambda f: f.replace(LINK, 'http://[invalid'), id='malformed-link'),
    pytest.param(lambda f: f.replace('<figure>', '<figure hidden>', 1), id='hidden-root'),
    pytest.param(lambda f: f.replace('<figure>\n', '<figure aria-hidden="true">\n', 1), id='hidden-leaf'),
    pytest.param(lambda f: f.replace('<img ', '<img hidden ', 1), id='hidden-image'),
    pytest.param(lambda f: f.replace('<img ', '<img onload="unknown()" ', 1), id='image-event'),
    pytest.param(lambda f: f.replace('<p>', '<p style="display:none">', 1), id='hidden-paragraph'),
    pytest.param(lambda f: f.replace('<strong>', '<strong hidden>'), id='hidden-heading'),
    pytest.param(lambda f: f.replace('<span>', '<span hidden>', 1), id='hidden-invitation'),
    pytest.param(lambda f: f.replace('letter-spacing:0.7px; text-align:justify', 'display:none'), id='changed-style'),
    pytest.param(lambda f: f.replace('<a ', '<a aria-hidden="true" ', 1), id='hidden-link'),
    pytest.param(lambda f: f.replace('<br>', '<br style="display:none">', 1), id='hidden-warning-break'),
    pytest.param(lambda f: f.replace('<figure>', '<figure><!--unreviewed-->', 1), id='comment'),
    pytest.param(lambda f: f.replace('<figure>\n', '<figure><figure>\n', 1), id='unknown-depth'),
    pytest.param(lambda f: f.replace('</figure>', '<figcaption>其他資料 123.45</figcaption></figure>', 1), id='extra-caption'),
    pytest.param(lambda f: f.replace('<p class="warning">', '<p class="unreviewed">'), id='unknown-warning-class'),
    pytest.param(lambda f: f.replace(NOTICE_LINES[0], '未知財務數字 123.45 不可當作聲明'), id='unknown-warning'),
    pytest.param(lambda f: f.replace('更多盤中即時資訊', '未知正文 123.45'), id='unknown-invitation'),
    pytest.param(lambda f: f.replace('社群網站連結 :', '未知來源 :'), id='unknown-social-label'),
    pytest.param(lambda f: f.replace(CTA, ''), id='missing-invitation'),
    pytest.param(lambda f: f.replace(FOOTER, ''), id='missing-footer'),
    pytest.param(lambda f: f.replace(LEAVES, LEAVES * 2), id='duplicate-leaves'),
    pytest.param(lambda f: f.replace(AFTER, ''), id='missing-prose'),
    pytest.param(lambda f: f.replace(LEAVES + AFTER, AFTER + LEAVES), id='changed-paragraph-image-position'),
    pytest.param(lambda f: f.replace(LEAVES, LEAVES.replace(IMAGES[1], 'TEMP').replace(IMAGES[2], IMAGES[1]).replace('TEMP', IMAGES[2])), id='changed-image-order'),
    pytest.param(lambda f: f.replace('<p></p></figure>', '<p>額外原值 789</p></figure>'), id='extra-tail'),
]


@pytest.mark.parametrize('change', CHANGES)
@pytest.mark.parametrize('representation', ['visible', 'current'])
def test_one_sided_mutation_never_waives_entire_source_agreement(cnyes_config, tmp_path, change, representation):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=change(FOREST) if representation == 'current' else FOREST,
            visible_figure=change(FOREST) if representation == 'visible' else FOREST, schema_caption=TEXT),
            source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('change', CHANGES[10:31])
def test_matching_schema_does_not_authorize_unknown_or_hidden_structure(cnyes_config, tmp_path, change):
    fragment = change(FOREST)
    text = BeautifulSoup(fragment, 'html.parser').figure.get_text('\n', strip=True)
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=fragment, visible_figure=fragment, schema_caption=text),
            source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('schema', [TEXT.replace('15,159', '15,160'), NOTICE_TEXT, TEXT + '\n未知資料 789', TEXT[::-1]])
def test_matching_visible_current_forest_still_needs_complete_schema(cnyes_config, tmp_path, schema):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=FOREST, visible_figure=FOREST, schema_caption=schema),
            source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
def test_hash_witness_is_nonmutating_and_catches_image_position_not_in_text_lists(parser):
    original = BeautifulSoup(FOREST, parser).figure
    changed = BeautifulSoup(FOREST.replace(LEAVES + AFTER, AFTER + LEAVES), parser).figure
    before = str(original)
    key = _cnyes_structured_figure_identity(original)
    assert key is not None and key[1][2] == hashlib.sha256(before.encode()).hexdigest()
    assert str(original) == before
    # The changed location leaves text, image order and links identical but
    # loses the observed interleaving. It must not produce an equal witness.
    other = _cnyes_structured_figure_identity(changed)
    assert other is None or other != key


def test_moving_an_image_within_valid_interleaving_changes_the_full_forest_witness():
    image_leaf = f'<figure>\n<img src="{IMAGES[2]}">\n</figure>'
    moved = FOREST.replace(image_leaf, '').replace(AFTER, AFTER + image_leaf)
    a = _cnyes_structured_figure_identity(BeautifulSoup(FOREST, 'lxml').figure)
    b = _cnyes_structured_figure_identity(BeautifulSoup(moved, 'lxml').figure)
    assert a is not None and b is not None
    assert a[0] == b[0] and a[1][:2] == b[1][:2]
    assert a[1][2] != b[1][2]
