"""A real narrative root must not be discarded with its image-only leaves."""
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_publishers import _cnyes_narrative_figure_views
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE, factset_page
from test_news_cnyes_html import CLOCK, URL, source


IMAGES = ['https://cimg.cnyes.cool/owned-narrative-1.jpg', 'https://cimg.cnyes.cool/owned-narrative-2.jpg']
LEAVES = [f'<figure><img src="{url}" data-width="535"></figure>' for url in IMAGES]
PARAGRAPHS = ['<p>原始值 -12.50%，不改數字。</p>',
              '<p><strong>配置觀察</strong>及<a href="https://example.org/own">原始參考</a>。</p>']
PARAGRAPHS.extend(f'<p>第 {number} 段原始數據 <strong>{number}.25</strong>。</p>' for number in range(3, 13))
PARAGRAPHS[-1] = '<p>\xa0</p>'
WARNING = '<p class="warning">原始基金風險<br>不得保證收益<br><u>原始波動數據</u><br>原始费用揭露<br>原始聯絡方式</p>'
FOREST = '<figure>' + LEAVES[0] + ''.join(PARAGRAPHS[:2]) + LEAVES[1] + ''.join(PARAGRAPHS[2:]) + WARNING + '</figure>'
TEXT = BeautifulSoup(FOREST, 'lxml').get_text(' ', strip=True)


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=a', MOBILE + '?exp=b', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_all_raw_prose_disclosures_numbers_and_clocks_are_preserved(cnyes_config, tmp_path, parser, url, streamed):
    cnyes_config['providers'][0]['html_parser'] = parser
    result = extract_article(factset_page(FOREST, schema=TEXT, streamed=streamed),
                             source(cnyes_config, tmp_path), page_url=url)
    assert ''.join(result['body'].split()) == ''.join(TEXT.split())
    assert '-12.50%' in result['body'] and '11.25' in result['body']
    assert '原始基金風險' in result['body'] and '原始聯絡方式' in result['body']
    assert result['published_at_raw'] == CLOCK and json.loads(result['authors_json']) == [AUTHOR]
    assert result['verified_image_captions_json'] is None and result['verified_article_notices_json'] is None
    assert 'source_current_narrative_figure_wrapper_comparison_v1' in result['article_identity_basis']
    assert 'all_source_prose_and_fund_disclosures_preserved' in result['body_completeness']


CHANGES = [
    pytest.param(lambda f: f.replace('-12.50%', '12.50%'), id='changed-sign'),
    pytest.param(lambda f: f.replace('11.25', '11.26'), id='changed-number'),
    pytest.param(lambda f: f.replace(IMAGES[0], IMAGES[1]), id='duplicate-image'),
    pytest.param(lambda f: f.replace(IMAGES[0], 'https://other.example/new.jpg'), id='changed-image'),
    pytest.param(lambda f: f.replace('<figure>', '<figure hidden>', 1), id='hidden-root'),
    pytest.param(lambda f: f.replace('<figure>', '<figure class="unknown">', 1), id='root-attributes'),
    pytest.param(lambda f: f.replace('<figure><img', '<figure aria-hidden="true"><img'), id='hidden-leaf'),
    pytest.param(lambda f: f.replace('<img ', '<img onload="unreviewed()" ', 1), id='image-event'),
    pytest.param(lambda f: f.replace('<p>', '<p style="display:none">', 1), id='hidden-paragraph'),
    pytest.param(lambda f: f.replace('<strong>', '<strong hidden>', 1), id='hidden-strong'),
    pytest.param(lambda f: f.replace('<u>', '<u aria-hidden="true">'), id='hidden-warning'),
    pytest.param(lambda f: f.replace('<br>', '<br style="display:none">', 1), id='hidden-warning-break'),
    pytest.param(lambda f: f.replace('https://example.org/own', 'javascript:unreviewed()'), id='script-link'),
    pytest.param(lambda f: f.replace(IMAGES[0], 'https://user:secret@example.org/private'), id='credential-image'),
    pytest.param(lambda f: f.replace('<figure>', '<figure><!--unreviewed-->', 1), id='comment'),
    pytest.param(lambda f: f.replace(LEAVES[1], '<figure><figure>' + LEAVES[1] + '</figure></figure>'), id='unknown-depth'),
    pytest.param(lambda f: f.replace('<strong>', '<em>', 1).replace('</strong>', '</em>', 1), id='unknown-markup'),
    pytest.param(lambda f: f.replace('<p class="warning">', '<p class="unknown">'), id='unknown-warning-class'),
    pytest.param(lambda f: f.replace('<u>原始波動數據</u>', ''), id='missing-disclosure'),
    pytest.param(lambda f: f.replace('<br>', '', 1), id='missing-warning-break'),
    pytest.param(lambda f: f.replace(PARAGRAPHS[2], ''), id='missing-paragraph'),
    pytest.param(lambda f: f.replace(LEAVES[1], ''), id='missing-image'),
    pytest.param(lambda f: f.replace(LEAVES[0] + PARAGRAPHS[0], PARAGRAPHS[0] + LEAVES[0]), id='changed-position'),
    pytest.param(lambda f: '<figure>' + f + '</figure>', id='second-outer-wrapper'),
    pytest.param(lambda f: f.replace('<p>\xa0</p>', '<p> </p>'), id='unknown-blank'),
    pytest.param(lambda f: f.replace('<p>\xa0</p>', '<p>未知資料123</p>'), id='unknown-tail-text'),
]


@pytest.mark.parametrize('change', CHANGES)
@pytest.mark.parametrize('side', ['visible', 'current'])
def test_one_sided_mutation_does_not_waive_full_dom_agreement(cnyes_config, tmp_path, change, side):
    with pytest.raises(NewsError):
        extract_article(factset_page(change(FOREST) if side == 'visible' else FOREST,
            current=change(FOREST) if side == 'current' else FOREST, schema=TEXT),
            source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('change', CHANGES[2:3] + CHANGES[4:])
def test_matching_unknown_or_hidden_structure_is_still_not_a_verified_body(cnyes_config, tmp_path, change):
    markup = change(FOREST)
    text = BeautifulSoup(markup, 'lxml').get_text(' ', strip=True)
    with pytest.raises(NewsError):
        extract_article(factset_page(markup, schema=text), source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('schema', [TEXT.replace('-12.50%', '12.50%'), TEXT + '其他資料123', '只有警語'])
def test_complete_schema_must_match_every_source_value(cnyes_config, tmp_path, schema):
    with pytest.raises(NewsError):
        extract_article(factset_page(FOREST, schema=schema), source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
def test_comparison_copy_never_mutates_source_or_removes_image_leaf(parser, cnyes_config, tmp_path):
    import html
    cnyes_config['providers'][0]['html_parser'] = parser
    original = BeautifulSoup(FOREST, parser)
    before = str(original)
    views = _cnyes_narrative_figure_views(original, html.escape(FOREST), source(cnyes_config, tmp_path), None)
    assert views is not None and str(original) == before
    assert all(len(view.find_all('figure')) == 2 for view in views)
    assert all(len(view.find_all('p')) == 13 for view in views)
