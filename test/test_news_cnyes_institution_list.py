"""Observed institution headings and exact empty declarations, not cleaning."""
import hashlib
import html
import json

import pytest

from downloader.news_fulltext_policy import extract_article, NewsError
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE, factset_page
from test_news_cnyes_html import CLOCK, URL, empty_page, source
from test_news_cnyes_stock_list_markers import stock_list_markup


@pytest.mark.parametrize('heading', ['1. 投信累計買超 3 日的股票', '2. 投信累計買超 2 日的股票',
    '3. 投信當日 (01) 買超的股票', '1. 外資累計賣超 5 日的股票'])
@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=a', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
def test_institution_list_preserves_raw_values_and_source_markers(cnyes_config, tmp_path, heading, parser, url, streamed):
    cnyes_config['providers'][0]['html_parser'] = parser
    result = extract_article(factset_page(stock_list_markup(heading), streamed=streamed),
        source(cnyes_config, tmp_path), page_url=url)
    assert ')、\ufeff測試乙 (' in result['body']
    assert all(value in result['body'] for value in ('3481-TW', '2353-TW', '-12.50', '22 億', '123 股'))
    assert result['published_at_raw'] == CLOCK and json.loads(result['authors_json']) == [AUTHOR]
    assert 'source_stock_list_company_prefix_marker_comparison_v1' in result['article_identity_basis']


@pytest.mark.parametrize('heading', ['1. 未知法人累計買超 3 日的股票', '1. 自營商累計買超 3 日的股票',
    '1. 投信累計買超 0 日的股票', '投信累計買超 3 日的股票'])
def test_unobserved_headings_not_generalized(cnyes_config, tmp_path, heading):
    with pytest.raises(NewsError):
        extract_article(factset_page(stock_list_markup(heading)), source(cnyes_config, tmp_path), page_url=MOBILE)


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('content', ['<p></p>', html.escape('<p></p>')])
def test_exact_empty_paragraph_is_source_absence_not_a_body(cnyes_config, tmp_path, parser, content):
    cnyes_config['providers'][0]['html_parser'] = parser
    with pytest.raises(NewsError, match='^source_article_empty_template$'):
        extract_article(empty_page(change={'content': content, 'video': ''}), source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('content', ['<p hidden></p>', '<p> </p>', '<p><!--unknown--></p>', '<p><br></p>',
    '<p></p><p></p>', '<p>actual text</p>', '<script></script>', '<p class="unknown"></p>'])
def test_only_observed_empty_declaration_is_classified(cnyes_config, tmp_path, content):
    with pytest.raises(NewsError) as caught:
        extract_article(empty_page(change={'content': content, 'video': ''}), source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_article_empty_template'


@pytest.mark.parametrize('change', [{'payment': 1}, {'publishAt': 1388534401}, {'video': 'a source video'},
    {'url': URL.replace('/42', '/43')}])
def test_empty_declaration_cannot_waive_free_same_id_clock_video(cnyes_config, tmp_path, change):
    with pytest.raises(NewsError) as caught:
        extract_article(empty_page(change={'content': html.escape('<p></p>'), 'video': '', **change}),
            source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_article_empty_template'


def test_html_upgrade_preserves_bulk_data_fingerprint(cnyes_config, tmp_path, monkeypatch):
    new = source(cnyes_config, tmp_path)
    original = hashlib.sha256

    def previous(value):
        try:
            payload = json.loads(value)
        except (ValueError, TypeError, UnicodeDecodeError):
            return original(value)
        if isinstance(payload, dict) and 'mobile_canonical_html_contract_version' in payload:
            assert payload['mobile_canonical_html_contract_version'] == 41
            assert payload['verified_empty_html_outcome_contract_version'] == 8
            payload['mobile_canonical_html_contract_version'] = 30
            return original(json.dumps(payload, sort_keys=True).encode())
        return original(value)

    monkeypatch.setattr('downloader.news_fulltext_policy.hashlib.sha256', previous)
    old = source(cnyes_config, tmp_path)
    assert new.data_fingerprint != old.data_fingerprint
    assert new.cnyes_bulk_data_fingerprint == old.cnyes_bulk_data_fingerprint
