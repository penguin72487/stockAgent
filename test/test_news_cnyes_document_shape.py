"""Unretained failure metadata must not become source/body acceptance."""
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import PolicyRegistry
from scripts.inspect_cnyes_news import cnyes_html_structure
from test_news_cnyes_canonical_alias import MOBILE, mobile_page
from test_cnyes_news_bulk import cfg


@pytest.mark.parametrize('missing', ['article-container', 'canonical', 'both'])
def test_missing_native_scope_still_explains_declared_field_shapes(cfg, tmp_path, missing):
    source = PolicyRegistry(cfg, base=tmp_path).sources['cnyes']
    soup = BeautifulSoup(mobile_page(), 'lxml')
    if missing in {'article-container', 'both'}:
        soup.select_one('#article-container').decompose()
    if missing in {'canonical', 'both'}:
        soup.select_one('link[rel="canonical"]').decompose()
    result = cnyes_html_structure(soup, source, page_url=MOBILE)
    shape = result['document_representation_shape']
    assert not result['raw_public_scope'] and 'body_representations' not in result
    assert shape['current_data_present']
    assert shape['current_field_shapes']['content']['characters'] > 0
    assert len(shape['matching_schema_shapes']) == 1
    assert shape['raw_scope_or_article_acceptance_granted'] is False
    assert shape['source_text_exported'] is False


@pytest.mark.parametrize('payment', [0, 1, False, None, '0'])
def test_shapes_cannot_retain_paid_or_ambiguous_content(cfg, tmp_path, payment):
    source = PolicyRegistry(cfg, base=tmp_path).sources['cnyes']
    secret = 'confidential body and private-key-do-not-export'
    soup = BeautifulSoup(mobile_page(changes={'payment': payment, 'content': secret,
        'title': secret, 'author': secret}, schema_changes={'articleBody': secret,
        'headline': secret, 'author': {'name': secret}}), 'lxml')
    soup.select_one('#article-container').decompose()
    result = cnyes_html_structure(soup, source, page_url=MOBILE)
    assert not result['raw_public_scope']
    assert result['document_representation_shape']['current_payment_zero'] is (type(payment) is int and payment == 0)
    assert secret not in json.dumps(result, ensure_ascii=False)
    assert 'body_representations' not in result


@pytest.mark.parametrize('url', ['https://other.test/news/id/42', 'https://news.cnyes.com/news/id/43',
    'http://news.cnyes.com/news/id/42', 'https://news.cnyes.com:443/news/id/42', 'https://[invalid/news/id/42'])
def test_matching_schema_shape_never_borrows_other_article_or_origin(cfg, tmp_path, url):
    source = PolicyRegistry(cfg, base=tmp_path).sources['cnyes']
    soup = BeautifulSoup(mobile_page(schema_changes={'url': url}), 'lxml')
    result = cnyes_html_structure(soup, source, page_url=MOBILE)
    assert result['document_representation_shape']['matching_schema_shapes'] == []


def test_schema_document_object_and_output_bounds(cfg, tmp_path):
    source = PolicyRegistry(cfg, base=tmp_path).sources['cnyes']
    article = {'@type': 'NewsArticle', 'url': 'https://news.cnyes.com/news/id/42',
               'articleBody': 'must-not-be-exported'}
    raw = '<html><head>' + ''.join('<script type="application/ld+json">' +
        json.dumps([article] * 100) + '</script>' for _ in range(40)) + '</head></html>'
    result = cnyes_html_structure(BeautifulSoup(raw, 'lxml'), source, page_url=MOBILE)
    shape = result['document_representation_shape']
    assert shape['schema_document_limit_reached'] and shape['schema_object_limit_reached']
    assert shape['schema_documents_examined'] == 16 and shape['schema_objects_examined'] == 1024
    assert len(shape['matching_schema_shapes']) == 4 and shape['schema_shape_limit_reached']
    assert 'must-not-be-exported' not in json.dumps(result)
    assert not result['raw_public_scope'] and len(json.dumps(shape)) < 2500


@pytest.mark.parametrize('extra_headers', [0, 1, 10])
def test_native_scope_shape_reports_ambiguity_without_source_text(cfg, tmp_path, extra_headers):
    source = PolicyRegistry(cfg, base=tmp_path).sources['cnyes']
    soup = BeautifulSoup(mobile_page(), 'lxml')
    parent = soup.select_one('#article-container').find_parent('article')
    for _ in range(extra_headers):
        header = soup.new_tag('h1')
        header.string = 'private unrelated heading do not export'
        parent.append(header)
    result = cnyes_html_structure(soup, source, page_url=MOBILE)
    shape = result['document_representation_shape']['native_container_shapes'][0]
    assert shape['has_article_parent'] is True
    assert shape['article_header_count_capped_at_five'] == min(extra_headers + 1, 5)
    assert len(shape['article_header_shapes']) == min(extra_headers + 1, 4)
    assert shape['article_header_shapes'][0]['matches_current_headline'] is True
    if extra_headers:
        assert not result['raw_public_scope']
    assert 'private unrelated heading' not in json.dumps(result)


def test_malformed_native_data_keeps_only_error_flag(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext_publishers as publisher
    source = PolicyRegistry(cfg, base=tmp_path).sources['cnyes']
    monkeypatch.setattr(publisher, '_cnyes_current_article_data', lambda *_a, **_k:
        (_ for _ in ()).throw(ValueError('secret-payload-do-not-export')))
    result = cnyes_html_structure(BeautifulSoup(mobile_page(), 'lxml'), source, page_url=MOBILE)
    assert result['document_representation_shape']['current_shape_malformed'] is True
    assert result['document_representation_shape']['current_data_present'] is False
    assert not result['raw_public_scope'] and 'secret-payload' not in json.dumps(result)


@pytest.mark.parametrize('schema', ['{invalid', '[' * 2000, '{"@graph":[false,3]}'])
def test_malformed_diagnostic_schema_is_not_accepted_or_exported(cfg, tmp_path, schema):
    source = PolicyRegistry(cfg, base=tmp_path).sources['cnyes']
    soup = BeautifulSoup('<script type="application/ld+json">' + schema + '</script>', 'lxml')
    result = cnyes_html_structure(soup, source, page_url=MOBILE)
    assert result['raw_public_scope'] is False
    assert result['document_representation_shape']['matching_schema_shapes'] == []


def test_other_provider_and_unsafe_request_have_no_document_shape(cfg, tmp_path):
    source = PolicyRegistry(cfg, base=tmp_path).sources['cnyes']
    for url in ('http://news.cnyes.com/news/id/42', 'https://other.test/news/id/42',
                'https://news.cnyes.com@other.test/news/id/42', 'https://news.cnyes.com/news/id/not-numeric'):
        result = cnyes_html_structure(BeautifulSoup(mobile_page(), 'lxml'), source, page_url=url)
        assert 'document_representation_shape' not in result
        assert not result['raw_public_scope']
