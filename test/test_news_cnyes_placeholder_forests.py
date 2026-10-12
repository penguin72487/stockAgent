"""Two retained native failures: source placeholders do not lose actual text."""
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


IMAGES = ['https://cimg.cnyes.cool/original-placeholder-0.jpg',
          'https://cimg.cnyes.cool/original-placeholder-1.jpg']
LINKS = ['https://lin.ee/original', 'https://t.me/original?fbclid=source_123',
         'https://reurl.cc/original', 'https://lihi1.com/original?fbclid=source_456']
LABELS = [link.split('?')[0] for link in LINKS]
PREFIXES = ['梅姊 LINE ID :', 'TG 盤中即時資訊 :', '梅姊的 Instagram ：', 'YouTube 頻道 :']
SOCIAL = (f'<figure><img src="{IMAGES[0]}" alt="不當正文999"><p>\xa0</p>'
          + ''.join(f'<p>{prefix}\xa0<a href="{href}" target="_blank">{label}</a></p>'
                    for prefix, href, label in zip(PREFIXES, LINKS, LABELS, strict=True)) + '</figure>')
SOCIAL_TEXT = BeautifulSoup(SOCIAL, 'html.parser').figure.get_text('\n', strip=True)
CAPTION = '原始製表來源：數值 -12.50%，交易量 1,234 股。'
LEAF = f'<figure><img src="{IMAGES[1]}" alt="不當正文999"><figcaption>{CAPTION}</figcaption></figure>'
WRAPPED = '<figure>' + LEAF + '<p>\xa0</p></figure>'
CASES = [('social', SOCIAL, SOCIAL_TEXT), ('caption', WRAPPED, CAPTION)]


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=b', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
@pytest.mark.parametrize('kind,fragment,text', CASES)
def test_body_clocks_author_and_all_original_references_preserved(cnyes_config, tmp_path, parser, url,
        streamed, kind, fragment, text):
    cnyes_config['providers'][0]['html_parser'] = parser
    result = extract_article(caption_page(current_figure=fragment, visible_figure=fragment,
        schema_caption=text, streamed=streamed), source(cnyes_config, tmp_path), page_url=url)
    assert result['body'] == BODY * 8 and '999' not in result['body']
    assert result['body_sha256'] == hashlib.sha256(result['body'].encode()).hexdigest()
    assert json.loads(result['authors_json']) == [AUTHOR] and result['published_at_raw'] == CLOCK
    assert result['modified_at_raw'] is None and result['historical_point_in_time'] is False
    notes = json.loads(result['verified_article_notices_json'])
    if kind == 'social':
        assert result['verified_image_captions_json'] is None
        assert notes == [{'text': SOCIAL_TEXT, 'kind': 'same_article_image_figure_social_placeholder_links',
            'image_sources': [IMAGES[0]],
            'source_links': [{'href': href, 'text': label} for href, label in zip(LINKS, LABELS, strict=True)]}]
        assert 'source_current_image_figure_social_placeholder_comparison_v1' in result['article_identity_basis']
        assert 'social_link_supplement_preserved_separately' in result['body_completeness']
    else:
        assert json.loads(result['verified_image_captions_json']) == [{'text': CAPTION, 'image_src': IMAGES[1],
            'structure_basis': 'same_article_visible_caption_leaf_and_placeholder_order'}]
        assert notes == [{'text': '', 'kind': 'same_article_caption_leaf_placeholder',
                         'image_sources': [IMAGES[1]], 'source_links': []}]
        assert 'source_current_caption_leaf_placeholder_comparison_v1' in result['article_identity_basis']


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_repeated_caption_stays_with_two_original_images_in_source_order(cnyes_config, tmp_path, parser):
    cnyes_config['providers'][0]['html_parser'] = parser
    first = LEAF.replace(IMAGES[1], IMAGES[0])
    result = extract_article(caption_page(current_figure=first + WRAPPED, visible_figure=first + WRAPPED,
        schema_caption=CAPTION + '\n' + CAPTION), source(cnyes_config, tmp_path), page_url=MOBILE)
    assert json.loads(result['verified_image_captions_json']) == [
        {'text': CAPTION, 'image_src': IMAGES[0]},
        {'text': CAPTION, 'image_src': IMAGES[1],
         'structure_basis': 'same_article_visible_caption_leaf_and_placeholder_order'}]
    assert result['body'] == BODY * 8


CHANGES = [
    pytest.param(lambda f: f.replace('<figure>', '<figure hidden>', 1), id='hidden-root'),
    pytest.param(lambda f: f.replace('<figure>', '<figure class="unknown">', 1), id='unknown-attribute'),
    pytest.param(lambda f: f.replace('<figure>', '<figure><!--unknown-->', 1), id='comment'),
    pytest.param(lambda f: f.replace('<img ', '<img onload="unknown()" ', 1), id='event-image'),
    pytest.param(lambda f: f.replace('<img ', '<img hidden ', 1), id='hidden-image'),
    pytest.param(lambda f: f.replace('<p>', '<p style="display:none">', 1), id='hidden-placeholder'),
    pytest.param(lambda f: f.replace('\xa0', ' ', 1), id='not-source-placeholder'),
    pytest.param(lambda f: f.replace('\xa0', '真實值123', 1), id='extra-narrative'),
    pytest.param(lambda f: f.replace('https://cimg.cnyes.cool/', 'javascript:unknown()'), id='script-image'),
    pytest.param(lambda f: f.replace('https://cimg.cnyes.cool/', 'https://user:secret@example.org/'), id='credential-image'),
    pytest.param(lambda f: f.replace('https://cimg.cnyes.cool/', 'http://[invalid/'), id='malformed-image'),
    pytest.param(lambda f: f.replace('</figure>', '<iframe src="https://public.example.org/embed"></iframe></figure>', 1), id='unknown-node'),
]


@pytest.mark.parametrize('kind,fragment,text', CASES)
@pytest.mark.parametrize('change', CHANGES)
@pytest.mark.parametrize('side', ['current', 'visible', 'both'])
def test_entire_ordered_dom_and_schema_do_not_allow_unsafe_or_unknown_markup(cnyes_config, tmp_path,
        kind, fragment, text, change, side):
    modified = change(fragment)
    schema = BeautifulSoup(modified, 'html.parser').get_text('\n', strip=True) if side == 'both' else text
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=modified if side in {'current', 'both'} else fragment,
            visible_figure=modified if side in {'visible', 'both'} else fragment, schema_caption=schema),
            source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('fragment', [
    SOCIAL.replace('TG 盤中即時資訊 :', '未分類投資報酬 :'),
    SOCIAL.replace('target="_blank"', 'target="_self"', 1),
    SOCIAL.replace('<a ', '<a onclick="unknown()" ', 1),
    SOCIAL.replace('t.me/', 't.me.evil.example/'),
    SOCIAL.replace('?fbclid=source_123', '?unknown=source_123'),
    SOCIAL.replace('?fbclid=source_123', '?fbclid=source_123&other=123'),
    SOCIAL.replace('https://lin.ee/original', 'https://lin.ee/original#unknown'),
    SOCIAL.replace('https://lin.ee/original', 'javascript:unknown()'),
    SOCIAL.replace('>https://lin.ee/original<', '>來源其他名稱<'),
    SOCIAL.replace('>https://lin.ee/original<', '><span>https://lin.ee/original</span><'),
    SOCIAL.replace('<p>\xa0</p>', ''),
    SOCIAL.replace('</figure>', '<p>來源有更多正文123</p></figure>'),
    WRAPPED.replace('<figcaption>', '<figcaption hidden>'),
    WRAPPED.replace('<figcaption>', '<figcaption><span>').replace('</figcaption>', '</span></figcaption>'),
    WRAPPED.replace(LEAF, '<figure>' + LEAF + '</figure>'),
    WRAPPED.replace('<p>\xa0</p>', '<figure>\xa0</figure>'),
])
def test_matching_words_do_not_reclassify_arbitrary_flow_content(cnyes_config, tmp_path, fragment):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=fragment, visible_figure=fragment,
            schema_caption=BeautifulSoup(fragment, 'html.parser').get_text('\n', strip=True)),
            source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('kind,fragment,text', CASES)
@pytest.mark.parametrize('change', [lambda text: text[::-1], lambda text: text + '来源未提供123'])
def test_independent_whole_schema_still_required(cnyes_config, tmp_path, kind, fragment, text, change):
    with pytest.raises(NewsError):
        extract_article(caption_page(current_figure=fragment, visible_figure=fragment, schema_caption=change(text)),
            source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('kind,fragment,text', CASES)
def test_complete_dom_identity_is_nonmutating(parser, kind, fragment, text):
    figure = BeautifulSoup(fragment, parser).figure
    before = str(figure)
    key = _cnyes_structured_figure_identity(figure)
    assert key is not None and key[1][2] == hashlib.sha256(before.encode()).hexdigest()
    assert str(figure) == before
