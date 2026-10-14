"""Native zero-body anchors are not hidden financial prose or media."""
from dataclasses import replace
import json

import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import MOBILE, URL, mobile_page


@pytest.fixture
def source(cnyes_config, tmp_path):
    return PolicyRegistry(cnyes_config, base=tmp_path).sources['cnyes']


def with_anchor(markup):
    return mobile_page().replace(b'</div></article>', (markup + '</div></article>').encode())


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=a', MOBILE + '?exp=b', URL + '?exp=a'])
@pytest.mark.parametrize('inside', ['', ' \n ', '&nbsp;'])
def test_invisible_pure_empty_anchor_preserves_exact_body_title_author_and_clocks(source, parser, url, inside):
    source = replace(source, html_parser=parser)
    baseline = extract_article(mobile_page(), source, page_url=url)
    actual = extract_article(with_anchor('<a name="old-source-marker" href="#original" '
                                         'style="display: none">' + inside + '</a>'), source, page_url=url)
    for key in ('body', 'body_sha256', 'title', 'authors_json', 'published_at_raw', 'modified_at_raw',
                'body_completeness', 'article_identity_basis', 'declared_canonical_url'):
        assert actual[key] == baseline[key]


def test_same_source_empty_anchor_reproduces_previous_visibility_rejection_without_relabelling_data(source, monkeypatch):
    from downloader import news_fulltext_publishers as publisher
    payload = with_anchor('<a style="display:none"></a>')
    result = extract_article(payload, source, page_url=URL + '?exp=a')
    assert result['body']
    monkeypatch.setattr(publisher, '_cnyes_empty_anchor_has_no_body', lambda _node: False)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(payload, source, page_url=URL + '?exp=a')


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('markup', [
    '<a style="display:none">financial 123.45</a>',
    '<a hidden><img src="original.png"></a>',
    '<a hidden><span></span></a>', '<a hidden><svg></svg></a>',
    '<a hidden><script></script></a>', '<a hidden><input></a>',
    '<a hidden><!-- original reference --></a>', '<a hidden><!----></a>',
    '<a hidden title="123.45"></a>', '<a hidden aria-label="123.45"></a>',
    '<a hidden aria-labelledby="financial-data"></a>', '<a hidden aria-describedby="financial-data"></a>',
    '<a hidden itemprop="articleBody"></a>', '<a hidden content="123.45"></a>',
    '<a hidden data-content="123.45"></a>', '<a hidden data-body="123.45"></a>',
    '<p hidden><a></a></p>',
])
def test_empty_anchor_rule_never_waives_hidden_words_media_metadata_or_unknown_descendants(source, parser, markup):
    with pytest.raises(NewsError):
        extract_article(with_anchor(markup), replace(source, html_parser=parser), page_url=URL + '?exp=a')


@pytest.mark.parametrize('changes', [
    {'payment': 1}, {'payment': False}, {'newsId': 43}, {'author': 'another source'},
    {'publishAt': 1}, {'title': 'a different source title'},
])
def test_empty_anchor_never_waives_identity_rights_author_or_publication_clock(source, changes):
    payload = mobile_page(changes=changes).replace(b'</div></article>', b'<a hidden></a></div></article>')
    with pytest.raises(NewsError):
        extract_article(payload, source, page_url=URL + '?exp=a')
