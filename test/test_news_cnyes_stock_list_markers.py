"""Observed stock-list formatting is comparison-only, never data cleaning."""
import json

import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE, factset_page, mobile_page
from test_news_cnyes_captions import CAPTION, FIGURE
from test_news_cnyes_html import BODY, CLOCK, URL, source


def stock_list_markup(heading='1. 外資累計賣超 5 日的股票'):
    return (f'<p>{heading}<br>測試甲 ('
            '<a href="https://www.cnyes.com/twstock/3481">3481-TW</a>)、\ufeff測試乙 ('
            '<a href="https://www.cnyes.com/twstock/2353">2353-TW</a>) '
            '原始值 -12.50、22 億、123 股。</p>')


@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=a', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_first_stock_after_heading_has_exact_current_source_prefix(cnyes_config, tmp_path, url, streamed):
    markup = stock_list_markup('3. 外資累計買超 3 日的股票').replace('<br>測試甲', '<br>\ufeff測試甲')
    result = extract_article(factset_page(markup, streamed=streamed), source(cnyes_config, tmp_path), page_url=url)
    assert result['body'].count('\ufeff') == 2
    assert '\ufeff測試甲 (' in result['body'] and ')、\ufeff測試乙 (' in result['body']
    assert all(value in result['body'] for value in ('3481-TW', '2353-TW', '-12.50', '22 億', '123 股'))
    assert result['published_at_raw'] == CLOCK and json.loads(result['authors_json']) == [AUTHOR]


@pytest.mark.parametrize('mutation', ['extra_heading_node', 'later_break', 'unknown_heading', 'repeated_prefix',
    'in_word', 'missing_quote', 'wrong_quote', 'hidden_quote', 'current_missing_prefix', 'current_quote_conflict',
    'duplicate_paragraph', 'schema_value_conflict'])
def test_first_stock_prefix_is_not_general_zero_width_normalization(cnyes_config, tmp_path, mutation):
    markup = stock_list_markup().replace('<br>測試甲', '<br>\ufeff測試甲')
    current = schema = None
    if mutation == 'extra_heading_node':
        markup = markup.replace('<br>\ufeff', '<br><span></span>\ufeff')
    elif mutation == 'later_break':
        markup = markup.replace('<br>\ufeff', '<br>其他敘述<br>\ufeff')
    elif mutation == 'unknown_heading':
        markup = markup.replace('外資累計賣超', '不明資料累計賣超')
    elif mutation == 'repeated_prefix':
        markup = markup.replace('<br>\ufeff', '<br>\ufeff\ufeff')
    elif mutation == 'in_word':
        markup = markup.replace('\ufeff測試甲', '測\ufeff試甲')
    elif mutation == 'missing_quote':
        markup = markup.replace('<a href="https://www.cnyes.com/twstock/3481">3481-TW</a>', '3481-TW')
    elif mutation == 'wrong_quote':
        markup = markup.replace('>3481-TW<', '>9999-TW<')
    elif mutation == 'hidden_quote':
        markup = markup.replace('<a href="https://www.cnyes.com/twstock/3481"', '<a hidden href="https://www.cnyes.com/twstock/3481"')
    elif mutation == 'current_missing_prefix':
        current = markup.replace('<br>\ufeff', '<br>')
    elif mutation == 'current_quote_conflict':
        current = markup.replace('/twstock/3481', '/twstock/3482')
    elif mutation == 'duplicate_paragraph':
        markup *= 2
    elif mutation == 'schema_value_conflict':
        schema = markup.replace('\ufeff', '').replace('-12.50', '-12.51')
    with pytest.raises(NewsError):
        extract_article(factset_page(markup, current=current, schema=schema), source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('heading', ['1. 外資累計賣超 5 日的股票', '5. 外資當日 (09) 賣超的股票',
                                     '4. 外資累計買超 2 日的股票', '5. 外資當日 (09) 買超的股票'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=a', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_verified_stock_list_marker_preserves_raw_values_and_source_clock(cnyes_config, tmp_path, heading, url, streamed):
    result = extract_article(factset_page(stock_list_markup(heading), streamed=streamed),
                             source(cnyes_config, tmp_path), page_url=url)
    assert ')、\ufeff測試乙 (' in result['body']
    assert all(value in result['body'] for value in ['3481-TW', '2353-TW', '-12.50', '22 億', '123 股'])
    assert 'source_stock_list_company_prefix_marker_comparison_v1' in result['article_identity_basis']
    assert result['published_at_raw'] == CLOCK and json.loads(result['authors_json']) == [AUTHOR]


@pytest.mark.parametrize('mutation', [
    'unknown_heading', 'missing_line_break', 'in_word', 'repeated_marker', 'missing_separator',
    'wrong_previous_host', 'wrong_next_host', 'wrong_next_label', 'next_query', 'next_credentials',
    'missing_next_link', 'current_link_conflict', 'current_marker_missing', 'schema_number_conflict',
    'duplicate_paragraph', 'hidden_visible_link', 'hidden_current_link', 'schema_extra_words',
])
def test_stock_list_comparison_never_repairs_unverified_words_values_links_or_visibility(cnyes_config, tmp_path, mutation):
    markup = stock_list_markup()
    current = schema = None
    if mutation == 'unknown_heading':
        markup = markup.replace('外資累計賣超', '不明資料累計賣超')
    elif mutation == 'missing_line_break':
        markup = markup.replace('<br>', '')
    elif mutation == 'in_word':
        markup = markup.replace('\ufeff測試乙', '測\ufeff試乙')
    elif mutation == 'repeated_marker':
        markup = markup.replace('\ufeff', '\ufeff\ufeff')
    elif mutation == 'missing_separator':
        markup = markup.replace(')、\ufeff', ')\ufeff')
    elif mutation == 'wrong_previous_host':
        markup = markup.replace('https://www.cnyes.com/twstock/3481', 'https://unreviewed.test/twstock/3481')
    elif mutation == 'wrong_next_host':
        markup = markup.replace('https://www.cnyes.com/twstock/2353', 'https://unreviewed.test/twstock/2353')
    elif mutation == 'wrong_next_label':
        markup = markup.replace('>2353-TW<', '>9999-TW<')
    elif mutation == 'next_query':
        markup = markup.replace('/twstock/2353"', '/twstock/2353?token=private"')
    elif mutation == 'next_credentials':
        markup = markup.replace('https://www.cnyes.com/twstock/2353', 'https://user:secret@www.cnyes.com/twstock/2353')
    elif mutation == 'missing_next_link':
        markup = markup.replace('<a href="https://www.cnyes.com/twstock/2353">2353-TW</a>', '2353-TW')
    elif mutation == 'current_link_conflict':
        current = markup.replace('/twstock/2353', '/twstock/2354')
    elif mutation == 'current_marker_missing':
        current = markup.replace('\ufeff', '')
    elif mutation == 'schema_number_conflict':
        schema = markup.replace('\ufeff', '').replace('-12.50', '-12.51')
    elif mutation == 'duplicate_paragraph':
        markup *= 2
    elif mutation == 'hidden_visible_link':
        markup = markup.replace('<a href="https://www.cnyes.com/twstock/2353"', '<a hidden href="https://www.cnyes.com/twstock/2353"')
    elif mutation == 'hidden_current_link':
        current = markup.replace('<a href="https://www.cnyes.com/twstock/2353"', '<a hidden href="https://www.cnyes.com/twstock/2353"')
    elif mutation == 'schema_extra_words':
        schema = markup.replace('\ufeff', '') + '<p>不同來源的未知內容。</p>'
    with pytest.raises(NewsError):
        extract_article(factset_page(markup, current=current, schema=schema),
                        source(cnyes_config, tmp_path), page_url=MOBILE)


def test_multiple_unique_source_paragraphs_keep_every_format_marker(cnyes_config, tmp_path):
    markup = ''.join(stock_list_markup(f'{i}. 外資累計賣超 {6-i} 日的股票') for i in range(1, 4))
    result = extract_article(factset_page(markup), source(cnyes_config, tmp_path), page_url=MOBILE)
    assert result['body'].count('\ufeff') == 3
    assert result['body'].count('-12.50') == 3


def test_actual_preceding_etf_quote_keeps_its_source_symbol(cnyes_config, tmp_path):
    markup = stock_list_markup().replace('3481', '00715L')
    result = extract_article(factset_page(markup), source(cnyes_config, tmp_path), page_url=MOBILE)
    assert '00715L-TW' in result['body'] and ')、\ufeff測試乙 (' in result['body']


def test_stock_list_and_figure_rules_compose_without_dropping_prose(cnyes_config, tmp_path):
    empty = '<figure>\n<figcaption>\xa0</figcaption>\n</figure>'
    markup = stock_list_markup() + FIGURE + empty
    schema = stock_list_markup().replace('\ufeff', '') + CAPTION
    result = extract_article(factset_page(markup, schema=schema), source(cnyes_config, tmp_path), page_url=MOBILE)
    assert ')、\ufeff測試乙 (' in result['body'] and '-12.50' in result['body']
    assert json.loads(result['verified_image_captions_json'])[0]['text'] == CAPTION
    assert 'source_current_empty_caption_placeholder_comparison_v1' in result['article_identity_basis']


def test_healthy_matching_schema_avoids_new_stock_list_reparse(cnyes_config, tmp_path, monkeypatch):
    from downloader import news_fulltext_publishers as publishers
    monkeypatch.setattr(publishers, '_cnyes_remove_verified_stock_list_markers',
                        lambda *_a: pytest.fail('healthy article reparsed a stock list'))
    result = extract_article(mobile_page(), source(cnyes_config, tmp_path), page_url=MOBILE)
    assert result['body'] == BODY * 8
