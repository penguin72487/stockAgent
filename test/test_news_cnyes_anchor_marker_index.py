"""Linear current-link lookup preserves raw characters and duplicate refusal."""
import html
from types import SimpleNamespace

from bs4 import BeautifulSoup, Tag
import pytest

from downloader.news_fulltext_publishers import _cnyes_remove_verified_anchor_markers


def links(count, prefix='盤後速報 - '):
    return '<ul>' + ''.join(
        f'<li><a href="https://news.cnyes.com/news/id/{n + 1}">{prefix}\ufeff原始-12.5與22億-{n}</a></li>'
        for n in range(count)) + '</ul>'


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('count', [1, 20, 100])
def test_current_links_are_indexed_once_not_searched_per_candidate(monkeypatch, parser, count):
    raw = links(count)
    scope = BeautifulSoup(raw, parser)
    original = str(scope)
    searches = []
    find_all = Tag.find_all

    def observe(self, name=None, *args, **kwargs):
        if name == 'a':
            searches.append(dict(kwargs))
        return find_all(self, name, *args, **kwargs)

    monkeypatch.setattr(Tag, 'find_all', observe)
    assert _cnyes_remove_verified_anchor_markers(scope, html.escape(raw),
        SimpleNamespace(html_parser=parser)) == count
    assert searches == [{'href': True}, {'href': True}]
    assert '\ufeff' in original and '\ufeff' in raw
    assert '\ufeff' not in scope.get_text()
    assert scope.get_text().replace('盤後速報 - ', '') == BeautifulSoup(raw, parser).get_text().replace(
        '盤後速報 - \ufeff', '')


@pytest.mark.parametrize('mutation', ['duplicate', 'text_conflict', 'missing_marker', 'wrong_parent', 'mixed_children'])
def test_index_never_turns_ambiguous_or_different_current_links_into_a_witness(mutation):
    raw = links(1)
    current = raw
    if mutation == 'duplicate':
        current += raw
    if mutation == 'text_conflict':
        current = current.replace('22億', '23億')
    if mutation == 'missing_marker':
        current = current.replace('\ufeff', '')
    if mutation == 'wrong_parent':
        current = current.replace('<li>', '<p>').replace('</li>', '</p>')
    if mutation == 'mixed_children':
        current = current.replace('原始', '<strong>原始</strong>')
    scope = BeautifulSoup(raw, 'lxml')
    before = str(scope)
    assert _cnyes_remove_verified_anchor_markers(scope, html.escape(current),
        SimpleNamespace(html_parser='lxml')) == 0
    assert str(scope) == before


def test_non_candidate_does_not_parse_current_content(monkeypatch):
    raw = links(1, prefix='未驗證標題 - ')
    scope = BeautifulSoup(raw, 'lxml')

    def forbidden(*args, **kwargs):
        raise AssertionError('unnecessary_current_dom_parse')

    monkeypatch.setattr('bs4.BeautifulSoup', forbidden)
    assert _cnyes_remove_verified_anchor_markers(scope, html.escape(raw),
        SimpleNamespace(html_parser='lxml')) == 0
