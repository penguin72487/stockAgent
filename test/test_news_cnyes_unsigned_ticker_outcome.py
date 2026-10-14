"""Unsigned positive quotes need actual token boundaries, not numeric inference."""
import hashlib
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_publishers import cnyes_verified_quote_only_body
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import TITLE, CLOCK, URL, short_page, source


BODY = '(GLT) 20.19  1.08(5.65%) :'


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('schema_flat', [False, True])
def test_unsigned_positive_ticker_source_has_no_narrative_and_no_permanent_retry(cnyes_config, tmp_path, parser, schema_flat):
    cnyes_config['providers'][0]['html_parser'] = parser
    schema = ''.join(BODY.split()) if schema_flat else BODY
    with pytest.raises(NewsError, match='^source_body_quote_only$') as caught:
        extract_article(short_page(body=BODY, schema_change={'articleBody': schema}),
            source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.retry is False


@pytest.mark.parametrize('body', [
    '(GLT)20.191.08(5.65%):', '(GLT) 20.191.08(5.65%):', '(GLT) 20.19 1.08(5.65%)',
    '(GLT) 20.19 1.08(-5.65%) :', '(GLT) 20.19 1.08(5.65%) : source prose',
    '(GLT) 20.19 1.08(5.65%) : 原始正文', '(GLT) 20.19 1.08(5.65%) : 3.14',
    '(glt) 20.19 1.08(5.65%) :', '(GLT) 20.19 1.08(5.65%%) :',
])
def test_unsigned_quote_does_not_guess_split_numbers_signs_or_drop_words(cnyes_config, tmp_path, body):
    soup = BeautifulSoup(short_page(body=body), 'lxml')
    article = {'@type': 'NewsArticle', 'url': URL, 'headline': TITLE, 'datePublished': CLOCK}
    assert not cnyes_verified_quote_only_body(soup, source(cnyes_config, tmp_path), article, body, page_url=URL)


@pytest.mark.parametrize('options', [
    {'visible': BODY.replace('20.19', '21.19')}, {'change': {'payment': 1}},
    {'change': {'content': '&lt;p&gt;another value&lt;/p&gt;'}},
    {'canonical': URL.replace('/42', '/43')}, {'change': {'publishAt': 1388534401}},
    {'schema_change': {'articleBody': BODY.replace('5.65', '5.75')}},
    {'conflict': True}, {'duplicate_key': True},
])
def test_unsigned_quote_requires_whole_free_content_identity_and_publication_clock(cnyes_config, tmp_path, options):
    with pytest.raises(NewsError) as caught:
        extract_article(short_page(body=BODY, **options), source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_body_quote_only'


@pytest.mark.parametrize('media', ['img', 'video', 'iframe', 'table'])
def test_unsigned_quote_cannot_discard_another_source_body(cnyes_config, tmp_path, media):
    payload = short_page(body=BODY).replace(b'</p></div>', f'</p><{media}></{media}></div>'.encode())
    with pytest.raises(NewsError) as caught:
        extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_body_quote_only'


def test_html36_and_quote6_change_only_html_not_permission_or_bulk(cnyes_config, tmp_path, monkeypatch):
    current = source(cnyes_config, tmp_path)
    real = hashlib.sha256
    def prior(value):
        try:
            payload = json.loads(value)
        except (ValueError, TypeError, UnicodeDecodeError):
            return real(value)
        if isinstance(payload, dict) and 'mobile_canonical_html_contract_version' in payload:
            assert payload['mobile_canonical_html_contract_version'] == 43
            assert payload['verified_native_quote_only_outcome_contract_version'] == 7
            payload['mobile_canonical_html_contract_version'] = 31
            payload['verified_native_quote_only_outcome_contract_version'] = 4
            return real(json.dumps(payload, sort_keys=True).encode())
        return real(value)
    monkeypatch.setattr('downloader.news_fulltext_policy.hashlib.sha256', prior)
    old = source(cnyes_config, tmp_path)
    assert current.data_fingerprint != old.data_fingerprint
    assert current.authorization_fingerprint == old.authorization_fingerprint
    assert current.cnyes_bulk_data_fingerprint == old.cnyes_bulk_data_fingerprint
