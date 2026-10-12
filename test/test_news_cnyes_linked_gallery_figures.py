"""Original links and caption-only galleries need complete typed source proof."""
import hashlib
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_publishers import _cnyes_structured_figure_identity
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE
from test_news_cnyes_captions import caption_page
from test_news_cnyes_html import BODY, CLOCK, URL, source


LINK = 'https://public.example.org/original-form?value=123'
IMAGES = [f'https://cimg.cnyes.cool/owned-original-{n}.jpg' for n in range(8)]
LINK_TEXT = f'原始價格 -12.50%，三招看懂資料 <a href="{LINK}">{LINK}</a>'
LINK_FIGURE = f'<figure><img src="{IMAGES[0]}"><p>{LINK_TEXT}</p></figure>'
ANCHORED_IMAGE = f'<figure><a href="{LINK}"><img src="{IMAGES[1]}"></a></figure>'
LINK_FOREST = LINK_FIGURE + ANCHORED_IMAGE
LINK_NOTICE = BeautifulSoup(LINK_FIGURE, 'html.parser').figure.get_text('\n', strip=True)
CAPTION = '原始照片圖說：2022 年、報酬 -12.50%、成交量 1,234 股。'


def photo(n):
    return (f'<figure><img src="{IMAGES[n]}" alt="alt 不得變成正文 999">'
            f'<figcaption>{CAPTION}</figcaption></figure>')


GROUP = '<figure>' + ''.join(photo(n) for n in (5, 6, 7)) + '<p>&nbsp;</p></figure>'
GALLERY = (f'<figure><img src="{IMAGES[0]}" alt="alt 不得變成正文 999"><figcaption><p>{CAPTION}</p>'
           + ''.join(photo(n) for n in (1, 2, 3, 4)) + GROUP + '<p>&nbsp;</p></figcaption></figure>')
GALLERY_TEXT = BeautifulSoup(GALLERY, 'html.parser').figure.get_text('\n', strip=True)


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=b', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_original_link_text_and_textless_href_preserved_separately(cnyes_config, tmp_path, parser, url, streamed):
    cnyes_config['providers'][0]['html_parser'] = parser
    result = extract_article(caption_page(current_figure=LINK_FOREST, visible_figure=LINK_FOREST,
        schema_caption=LINK_NOTICE, streamed=streamed), source(cnyes_config, tmp_path), page_url=url)
    assert result['body'] == BODY * 8 and result['published_at_raw'] == CLOCK
    assert json.loads(result['authors_json']) == [AUTHOR]
    assert result['verified_image_captions_json'] is None
    assert json.loads(result['verified_article_notices_json']) == [
        {'text': LINK_NOTICE, 'kind': 'same_article_image_figure_link_text', 'image_sources': [IMAGES[0]],
         'source_links': [{'href': LINK, 'text': LINK}]},
        {'text': '', 'kind': 'same_article_linked_image_reference', 'image_sources': [IMAGES[1]],
         'source_links': [{'href': LINK, 'text': ''}]},
    ]
    assert 'source_current_linked_image_figure_schema_comparison_v1' in result['article_identity_basis']
    assert 'references_preserved_separately' in result['body_completeness']


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=a', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_complete_caption_only_gallery_preserves_every_repeated_caption(cnyes_config, tmp_path, parser, url, streamed):
    cnyes_config['providers'][0]['html_parser'] = parser
    result = extract_article(caption_page(body='', current_figure=GALLERY, visible_figure=GALLERY,
        schema_caption=GALLERY_TEXT, streamed=streamed), source(cnyes_config, tmp_path), page_url=url)
    assert result['body'] == GALLERY_TEXT and result['published_at_raw'] == CLOCK
    assert result['body'].count(CAPTION) == 8 and '999' not in result['body'] and '首圖' not in result['body']
    assert json.loads(result['authors_json']) == [AUTHOR]
    assert result['verified_article_notices_json'] is None
    assert json.loads(result['verified_image_captions_json']) == [
        {'text': CAPTION, 'image_src': src,
         'structure_basis': 'same_article_visible_caption_gallery_and_source_image_order'} for src in IMAGES]
    assert 'source_current_caption_gallery_text_body_comparison_v1' in result['article_identity_basis']
    assert result['body_completeness'] == ('verified_current_visible_caption_gallery_text; '
        'original_captions_in_source_order; image_pixels_not_downloaded_or_OCRed')
    assert result['historical_point_in_time'] is False


LINK_CHANGES = [
    pytest.param(lambda f: f.replace('-12.50%', '12.50%'), id='changed-sign'),
    pytest.param(lambda f: f.replace(IMAGES[1], IMAGES[0]), id='changed-image'),
    pytest.param(lambda f: f.replace(LINK, 'https://public.example.org/other'), id='changed-href'),
    pytest.param(lambda f: f.replace('<a ', '<a hidden ', 1), id='hidden-link'),
    pytest.param(lambda f: f.replace('<img ', '<img hidden ', 1), id='hidden-image'),
    pytest.param(lambda f: f.replace('<figure>', '<figure hidden>', 1), id='hidden-root'),
    pytest.param(lambda f: f.replace('<p>', '<p style="display:none">'), id='hidden-paragraph'),
    pytest.param(lambda f: f.replace('<figure>', '<figure><!--unreviewed-->', 1), id='comment'),
    pytest.param(lambda f: f.replace('<a ', '<a onclick="unknown()" ', 1), id='anchor-event'),
    pytest.param(lambda f: f.replace(IMAGES[0], 'javascript:unknown()'), id='script-image'),
    pytest.param(lambda f: f.replace(LINK, 'https://user:secret@example.org/private'), id='credential-link'),
    pytest.param(lambda f: f.replace(LINK, 'http://[invalid'), id='malformed-link'),
    pytest.param(lambda f: f.replace(LINK_NOTICE.split('\n')[0], '未知附文 123'), id='changed-description'),
    pytest.param(lambda f: f.replace(ANCHORED_IMAGE, ANCHORED_IMAGE.replace('<img ', '<span><img ').replace('</a>', '</span></a>')), id='unknown-image-wrapper'),
    pytest.param(lambda f: ANCHORED_IMAGE + LINK_FIGURE, id='changed-figure-order'),
    pytest.param(lambda f: f.replace(ANCHORED_IMAGE, ANCHORED_IMAGE.replace('</a>', '額外文字 123</a>')), id='nontextless-image-anchor'),
]


@pytest.mark.parametrize('change', LINK_CHANGES)
@pytest.mark.parametrize('representation', ['visible', 'current'])
def test_links_require_complete_same_text_images_href_order_and_visibility(cnyes_config, tmp_path, change, representation):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=change(LINK_FOREST) if representation == 'current' else LINK_FOREST,
            visible_figure=change(LINK_FOREST) if representation == 'visible' else LINK_FOREST,
            schema_caption=LINK_NOTICE), source(cnyes_config, tmp_path), page_url=MOBILE)


GALLERY_CHANGES = [
    pytest.param(lambda f: f.replace('-12.50%', '12.50%', 1), id='changed-sign'),
    pytest.param(lambda f: f.replace(IMAGES[1], IMAGES[2]), id='duplicate-image'),
    pytest.param(lambda f: f.replace(IMAGES[1], 'https://cimg.cnyes.cool/other.jpg'), id='changed-image'),
    pytest.param(lambda f: f.replace('<figure>', '<figure hidden>', 1), id='hidden-root'),
    pytest.param(lambda f: f.replace('<figcaption>', '<figcaption hidden>', 1), id='hidden-caption'),
    pytest.param(lambda f: f.replace('<img ', '<img hidden ', 1), id='hidden-image'),
    pytest.param(lambda f: f.replace('<img ', '<img onload="unknown()" ', 1), id='image-event'),
    pytest.param(lambda f: f.replace('<p>', '<p style="display:none">', 1), id='hidden-caption-paragraph'),
    pytest.param(lambda f: f.replace('<figure>', '<figure><!--unreviewed-->', 1), id='comment'),
    pytest.param(lambda f: f.replace(CAPTION, '<span>' + CAPTION + '</span>', 1), id='unknown-caption-markup'),
    pytest.param(lambda f: f.replace(GROUP, '<figure>' + GROUP + '</figure>'), id='unknown-depth'),
    pytest.param(lambda f: f.replace('<p>&nbsp;</p>', '<p>額外數字 789</p>', 1), id='real-group-tail'),
    pytest.param(lambda f: f.replace('<p>&nbsp;</p>', '<p> </p>', 1), id='unknown-placeholder'),
    pytest.param(lambda f: f.replace(IMAGES[0], 'https://user:secret@example.org/photo'), id='credential-image'),
    pytest.param(lambda f: f.replace(IMAGES[0], 'http://[invalid'), id='malformed-image'),
    pytest.param(lambda f: f.replace(IMAGES[0], 'javascript:unknown()'), id='script-image'),
    pytest.param(lambda f: f.replace(photo(1) + photo(2), photo(2) + photo(1)), id='changed-photo-order'),
    pytest.param(lambda f: f.replace(GROUP, ''), id='missing-group'),
    pytest.param(lambda f: f.replace(GROUP, GROUP + GROUP), id='duplicate-group'),
    pytest.param(lambda f: f.replace('alt 不得變成正文 999', 'changed alt', 1), id='changed-nonbody-image-metadata'),
]


@pytest.mark.parametrize('change', GALLERY_CHANGES)
@pytest.mark.parametrize('representation', ['visible', 'current'])
def test_gallery_whole_schema_cannot_hide_changed_pairing_unknown_or_hidden_nodes(cnyes_config, tmp_path, change, representation):
    with pytest.raises(NewsError):
        extract_article(caption_page(body='', current_figure=change(GALLERY) if representation == 'current' else GALLERY,
            visible_figure=change(GALLERY) if representation == 'visible' else GALLERY,
            schema_caption=GALLERY_TEXT), source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('fragment', [*(change.values[0](LINK_FOREST) for change in LINK_CHANGES[3:12]),
                                    *(change.values[0](GALLERY) for change in GALLERY_CHANGES[3:16])])
def test_matching_schema_and_current_do_not_authorize_unknown_structure(cnyes_config, tmp_path, fragment):
    soup = BeautifulSoup(fragment, 'html.parser')
    text = '\n'.join(f.get_text('\n', strip=True) for f in soup.find_all('figure', recursive=False))
    # lxml/html.parser wrapping is irrelevant to source text extraction here.
    if not text:
        text = soup.get_text('\n', strip=True)
    with pytest.raises(NewsError):
        extract_article(caption_page(body='' if 'figcaption' in fragment else BODY * 8,
            current_figure=fragment, visible_figure=fragment, schema_caption=text),
            source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('schema', [CAPTION, GALLERY_TEXT.replace('-12.50%', '12.50%', 1),
                                  GALLERY_TEXT + '\n未知數字 789', GALLERY_TEXT[::-1]])
def test_gallery_needs_the_entire_declared_text_not_one_caption_or_schema_only_body(cnyes_config, tmp_path, schema):
    with pytest.raises(NewsError):
        extract_article(caption_page(body='', current_figure=GALLERY, visible_figure=GALLERY, schema_caption=schema),
                        source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('fragment', [LINK_FIGURE, ANCHORED_IMAGE, GALLERY])
@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
def test_full_ordered_dom_witness_is_nonmutating(fragment, parser):
    figure = BeautifulSoup(fragment, parser).figure
    before = str(figure)
    key = _cnyes_structured_figure_identity(figure)
    assert key is not None and key[1][2] == hashlib.sha256(before.encode()).hexdigest()
    assert str(figure) == before


def test_matching_text_and_image_lists_do_not_erase_gallery_group_position():
    moved = GALLERY.replace(photo(4) + GROUP, '<figure>' + photo(4) + ''.join(photo(n) for n in (5, 6, 7)) + '<p>&nbsp;</p></figure>')
    original = _cnyes_structured_figure_identity(BeautifulSoup(GALLERY, 'lxml').figure)
    other = _cnyes_structured_figure_identity(BeautifulSoup(moved, 'lxml').figure)
    assert original is not None and other is not None
    assert original[0] == other[0] and original[1][:2] == other[1][:2]
    assert original[1][2] != other[1][2]
