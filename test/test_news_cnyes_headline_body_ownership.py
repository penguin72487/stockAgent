"""Headline ownership is structural, not a special case for individual URLs."""
import hashlib
import html
import json
from dataclasses import replace

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_publishers import _cnyes_visible_article_scope
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import CLOCK, TITLE, URL, short_page, source


BODY = '本期營收 12.5 億元，年增 7.25%，來源完整原始數據。'
FRAGMENTS = (
    '<h1>本期營收</h1><p>12.5 億元，年增 7.25%，來源完整原始數據。</p>',
    '<section><h1>本期營收</h1><p>12.5 億元，年增 7.25%，來源完整原始數據。</p></section>',
    '<section><h1>本期<h1>營收</h1></h1><p>12.5 億元，年增 7.25%，來源完整原始數據。</p></section>',
)


def headed_page(fragment):
    payload = short_page(body=BODY, change={'content': html.escape(fragment)})
    return payload.replace(('<p>' + html.escape(BODY) + '</p>').encode(), fragment.encode())


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('fragment', FRAGMENTS)
def test_body_h1_is_preserved_and_cannot_become_the_article_title(cnyes_config, tmp_path, parser, fragment):
    policy = replace(source(cnyes_config, tmp_path), html_parser=parser)
    payload = headed_page(fragment)
    soup = BeautifulSoup(payload, parser)
    header, container = _cnyes_visible_article_scope(soup)
    assert header.get_text(strip=True) == TITLE
    assert container.find('h1') is not None
    result = extract_article(payload, policy, page_url=URL)
    assert ''.join(result['body'].split()) == ''.join(BODY.split())
    assert result['title'] == TITLE
    assert result['published_at_raw'] == CLOCK
    assert json.loads(result['authors_json']) == ['來源宣告的測試作者']
    for value in ('12.5', '7.25%'):
        assert value in result['body']


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('fragment', [
    '<article><h1>target</h1><div id="article-container"><h1>body</h1></div><h1>second</h1></article>',
    '<article><div id="article-container"><h1>not a header</h1></div></article>',
    '<article><article><h1>nested</h1></article><div id="article-container">body</div></article>',
    '<article><article><h1>target</h1><div id="article-container">body</div></article></article>',
    '<article><h1>target</h1><div id="article-container"><article>neighbor</article></div></article>',
    '<article><h1>target</h1><div id="article-container">a</div><div id="article-container">b</div></article>',
    '<h1>page header</h1><div id="article-container"><h1>body</h1></div>',
])
def test_missing_ambiguous_or_foreign_article_ownership_is_not_relaxed(parser, fragment):
    assert _cnyes_visible_article_scope(BeautifulSoup(fragment, parser)) is None


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', [
    lambda p: p.replace(b'<h1>', b'<h1 hidden>', 1),
    lambda p: p.replace(b'<h1>', b'<h1 aria-hidden="true">', 1),
    lambda p: p.replace(b'<h1>', b'<h1 style="display:none">', 1),
    lambda p: p.replace(b'id="article-container"', b'id="article-container" hidden'),
    lambda p: p.replace(TITLE.encode(), b'foreign headline', 1),
    lambda p: p.replace(b'12.5', b'13.5', 1),
    lambda p: p.replace(b'<h1>', b'<h1>extra</h1><h1>', 1),
])
def test_structural_repair_cannot_supply_hidden_changed_or_foreign_text(cnyes_config, tmp_path, parser, change):
    with pytest.raises(NewsError):
        extract_article(change(headed_page(FRAGMENTS[0])),
                        replace(source(cnyes_config, tmp_path), html_parser=parser), page_url=URL)


def test_headline_ownership_only_changes_html_parser_not_access_or_api(cnyes_config, tmp_path, monkeypatch):
    current = source(cnyes_config, tmp_path)
    original = hashlib.sha256

    def prior(value):
        try:
            payload = json.loads(value)
        except (ValueError, TypeError, UnicodeDecodeError):
            return original(value)
        if isinstance(payload, dict) and 'visible_headline_body_ownership_contract_version' in payload:
            assert payload.pop('visible_headline_body_ownership_contract_version') == 1
            return original(json.dumps(payload, sort_keys=True).encode())
        return original(value)

    monkeypatch.setattr('downloader.news_fulltext_policy.hashlib.sha256', prior)
    previous = source(cnyes_config, tmp_path)
    assert current.data_fingerprint != previous.data_fingerprint
    assert current.parser_repair_fingerprint != previous.parser_repair_fingerprint
    for field in ('authorization_fingerprint', 'cnyes_bulk_data_fingerprint',
                  'cnyes_index_data_fingerprint', 'http_capacity_source_fingerprint', 'allowed'):
        assert getattr(current, field) == getattr(previous, field)
