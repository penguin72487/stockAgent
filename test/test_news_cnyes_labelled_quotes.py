"""Labelled quote grammars are source outcomes, never successful news bodies."""
from copy import deepcopy
import hashlib
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_publishers import cnyes_verified_quote_only_body
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import CLOCK, TITLE, URL, short_page, source

QUOTES = (
    '(AMTG) 22.20 -0.48(-2.11%)',
    '19.28 +5.28 +37.68%：(ECOM)',
    '7.08 -2.89(-28.99%)：Atlantic Power(AT)',
    '((COSH) 46.72 :',
    '((SQI) 22.38 :',
    '(RH):',
)


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('body', QUOTES)
def test_complete_source_labelled_quote_is_not_retryable_body_gap(cnyes_config, tmp_path, parser, body):
    cnyes_config['providers'][0]['html_parser'] = parser
    with pytest.raises(NewsError, match='source_body_quote_only') as caught:
        extract_article(short_page(body=body), source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code == 'source_body_quote_only'
    assert not caught.value.retry


@pytest.mark.parametrize('body', QUOTES)
@pytest.mark.parametrize('options', [
    {'change': {'payment': 1}}, {'change': {'publishAt': 1388534401}},
    {'change': {'content': '&lt;p&gt;unrelated original&lt;/p&gt;'}},
    {'schema_change': {'articleBody': 'unrelated original'}},
    {'visible': 'the original has additional prose'}, {'canonical': URL.replace('/42', '/43')},
    {'conflict': True}, {'duplicate_key': True},
])
def test_labels_never_waive_current_id_free_clock_or_whole_text(cnyes_config, tmp_path, body, options):
    with pytest.raises(NewsError) as caught:
        extract_article(short_page(body=body, **options), source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_body_quote_only'


@pytest.mark.parametrize('body', QUOTES)
@pytest.mark.parametrize('extra', [' useful prose', ' 中文正文', ' 123.45', ' (OTHER)', ':'])
def test_labelled_quote_does_not_discard_additional_text(cnyes_config, tmp_path, body, extra):
    page = short_page(body=body + extra)
    soup = BeautifulSoup(page, 'lxml')
    article = {'@type': 'NewsArticle', 'url': URL, 'headline': TITLE, 'datePublished': CLOCK}
    assert not cnyes_verified_quote_only_body(soup, source(cnyes_config, tmp_path), article, body+extra, page_url=URL)


@pytest.mark.parametrize('body', QUOTES)
@pytest.mark.parametrize('media', ['img', 'video', 'audio', 'iframe', 'object', 'embed', 'table'])
def test_labelled_quote_never_discards_media_or_data_tables(cnyes_config, tmp_path, body, media):
    payload = short_page(body=body).replace(b'</p></div>', f'</p><{media}></{media}></div>'.encode())
    with pytest.raises(NewsError) as caught:
        extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_body_quote_only'


@pytest.mark.parametrize('change', [{'video': 'source video'}, {'video': None}])
def test_identifier_colon_requires_explicit_empty_source_video(cnyes_config, tmp_path, change):
    with pytest.raises(NewsError) as caught:
        extract_article(short_page(body='(RH):', change=change), source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_body_quote_only'


@pytest.mark.parametrize('body', [
    '(AMTG) 22.20 -0.48', '(AMTG) 22.20 -0.48(2.11%)',
    '19.28 +5.28 37.68%：(ECOM)', '19.28 5.28 +37.68%：(ECOM)',
    '((COSH) 46.72', '(((COSH) 46.72:', '(RH)::', '(Rh):',
    '7.08 -2.89(-28.99%)：Atlantic Power(AT):',
    '7.08 -2.89(-28.99%)：Atlantic Power(AT) additional prose',
])
def test_unknown_numeric_or_malformed_fragments_remain_unresolved(cnyes_config, tmp_path, body):
    soup = BeautifulSoup(short_page(body=body), 'lxml')
    article = {'@type': 'NewsArticle', 'url': URL, 'headline': TITLE, 'datePublished': CLOCK}
    assert not cnyes_verified_quote_only_body(soup, source(cnyes_config, tmp_path), article, body, page_url=URL)


def test_quote6_identity_changes_html_only_not_rights_or_bulk(cnyes_config, tmp_path, monkeypatch):
    current = source(cnyes_config, tmp_path)
    original = hashlib.sha256

    def quote5(value):
        try:
            payload = json.loads(value)
        except (ValueError, TypeError, UnicodeDecodeError):
            return original(value)
        if isinstance(payload, dict) and 'verified_native_quote_only_outcome_contract_version' in payload:
            payload = deepcopy(payload)
            assert payload['verified_native_quote_only_outcome_contract_version'] == 6
            payload['verified_native_quote_only_outcome_contract_version'] = 5
            return original(json.dumps(payload, sort_keys=True).encode())
        return original(value)

    monkeypatch.setattr('downloader.news_fulltext_policy.hashlib.sha256', quote5)
    previous = source(cnyes_config, tmp_path)
    assert current.data_fingerprint != previous.data_fingerprint
    assert current.authorization_fingerprint == previous.authorization_fingerprint
    assert current.cnyes_bulk_data_fingerprint == previous.cnyes_bulk_data_fingerprint
