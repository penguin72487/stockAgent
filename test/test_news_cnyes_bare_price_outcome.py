"""Exact price/change/percent stubs are negative outcomes, never full bodies."""
import hashlib
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_publishers import cnyes_verified_quote_only_body
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import TITLE, CLOCK, URL, short_page, source


BODY = '7.55  -0.95(-11.15%)'


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('body', [BODY, '7.55-0.95(-11.15%)', '7.55 +0.95(+11.15%)', '0.055 -0.001(-1.11%)'])
def test_exact_bare_price_stub_is_a_nonretryable_source_outcome(cnyes_config, tmp_path, parser, body):
    cnyes_config['providers'][0]['html_parser'] = parser
    with pytest.raises(NewsError, match='source_body_quote_only') as caught:
        extract_article(short_page(body=body), source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code == 'source_body_quote_only' and not caught.value.retry


@pytest.mark.parametrize('options', [
    {'change': {'payment': 1}}, {'change': {'content': '&lt;p&gt;another body&lt;/p&gt;'}},
    {'visible': BODY + 'another body'}, {'canonical': URL.replace('/42', '/43')},
    {'change': {'publishAt': 1388534401}}, {'conflict': True}, {'duplicate_key': True},
    {'schema_change': {'articleBody': BODY.replace('7.55', '8.55')}},
])
def test_bare_price_still_requires_whole_free_original_and_aware_clock(cnyes_config, tmp_path, options):
    with pytest.raises(NewsError) as caught:
        extract_article(short_page(body=BODY, **options), source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_body_quote_only'


@pytest.mark.parametrize('body', [
    '7.55', '7.55 -0.95', '7.55 -0.95(-11.15%) useful prose', '7.55 -0.95(-11.15%)正文',
    '7.55 -0.95(11.15%)', '7.55 --0.95(-11.15%)', '7.55 +-0.95(-11.15%)',
    '7.55 -0.95(-11.15%):', '7.55 -0.95(-11.15%) -0.55', '7.55 -0.95(-11.15%)\n123.45',
])
def test_numeric_or_prose_fragment_is_not_inferred_as_a_price_stub(cnyes_config, tmp_path, body):
    soup = BeautifulSoup(short_page(body=body), 'lxml')
    article = {'@type': 'NewsArticle', 'url': URL, 'headline': TITLE, 'datePublished': CLOCK}
    assert not cnyes_verified_quote_only_body(soup, source(cnyes_config, tmp_path), article, body, page_url=URL)


@pytest.mark.parametrize('media', ['img', 'video', 'iframe', 'table'])
def test_bare_price_does_not_discard_another_body_representation(cnyes_config, tmp_path, media):
    payload = short_page(body=BODY).replace(b'</p></div>', f'</p><{media}></{media}></div>'.encode())
    with pytest.raises(NewsError) as caught:
        extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_body_quote_only'


def test_bare_price_v3_changes_only_html_outcome_identity(cnyes_config, tmp_path, monkeypatch):
    current = source(cnyes_config, tmp_path)
    original = hashlib.sha256
    def previous(value):
        try:
            payload = json.loads(value)
        except (ValueError, TypeError, UnicodeDecodeError):
            return original(value)
        if isinstance(payload, dict) and 'verified_native_quote_only_outcome_contract_version' in payload:
            assert payload['verified_native_quote_only_outcome_contract_version'] == 7
            payload['verified_native_quote_only_outcome_contract_version'] = 2
            return original(json.dumps(payload, sort_keys=True).encode())
        return original(value)
    monkeypatch.setattr('downloader.news_fulltext_policy.hashlib.sha256', previous)
    old = source(cnyes_config, tmp_path)
    assert current.data_fingerprint != old.data_fingerprint
    assert current.authorization_fingerprint == old.authorization_fingerprint
    assert current.cnyes_bulk_data_fingerprint == old.cnyes_bulk_data_fingerprint
