"""Pocket current-failure evidence does not accept prose or invent clocks."""
from types import SimpleNamespace
import json

from bs4 import BeautifulSoup
from bs4.element import Script
import pytest

from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_pocket import pocket_clock_metadata
from scripts.probe_news_sources import probe_provider
from test_news_pocket_original import settings, page, URL, RELEASE, REVISION, POCKET_AUTHOR


def change_schema(soup, **changes):
    node = soup.find('script', type='application/ld+json')
    value = json.loads(node.get_text())
    value.update(changes)
    node.clear()
    node.append(Script(json.dumps(value, ensure_ascii=False)))


@pytest.mark.parametrize('engine', ['lxml', 'html.parser'])
def test_exact_native_clock_fields_are_separate_not_an_acceptance_or_body(engine):
    facts = pocket_clock_metadata(BeautifulSoup(page(),engine),page_url=URL)
    assert facts['reason'] == 'native_clock_boundary_shapes'
    assert facts['literal_schema_URL_agrees'] and facts['declared_author_is_known_Organization']
    assert facts['declared_release']['literal_clock_or_known_label'] == RELEASE
    assert facts['declared_revision']['literal_clock_or_known_label'] == REVISION
    assert facts['inner_label_count'] == facts['outer_label_count'] == facts['body_scope_count'] == 1
    assert '2023/02/15' in facts['outer_update_label']['literal_clock_or_known_label']
    assert '2023-02-15' in facts['inner_update_labels'][0]['literal_clock_or_known_label']
    assert not facts['accepted_as_article'] and not facts['publisher_requests'] and not facts['raw_names_or_body_retained']
    assert all(value not in json.dumps(facts,ensure_ascii=False) for value in ('第0段','保留負值','FAQ','合成資料'))


@pytest.mark.parametrize('change', [
    lambda s:s.select_one('script').decompose(),
    lambda s:s.select_one('script').__setattr__('string','{invalid'),
    lambda s:s.select_one('script').replace_with(BeautifulSoup(
        '<script type="application/ld+json">'+'x'*(128*1024+1)+'</script>','lxml').script),
    lambda s:s.head.extend(BeautifulSoup('<script type="application/ld+json">{}</script>'*16,'lxml').select('script')),
])
def test_schema_failures_remain_finite_and_cannot_export_body(change):
    soup = BeautifulSoup(page(),'lxml')
    change(soup)
    facts = pocket_clock_metadata(soup,page_url=URL)
    assert facts['reason'] in {'schema_count_not_bounded','schema_size_not_bounded','schema_not_decoded','single_Article_required'}
    assert not facts['accepted_as_article'] and 'declared_release' not in facts


def test_large_native_schema_is_rejected_before_decoding():
    soup = BeautifulSoup(page(),'lxml')
    node = soup.select_one('script')
    node.clear()
    node.append(Script('x'*(128*1024+1)))
    assert pocket_clock_metadata(soup,page_url=URL)['reason'] == 'schema_size_not_bounded'


@pytest.mark.parametrize('clock', [None,17,[],{'raw':'PRIVATE PROSE'},'PRIVATE PROSE','\ud800'])
def test_unknown_clock_types_or_words_are_not_exported_as_dates(clock):
    soup = BeautifulSoup(page(),'lxml')
    change_schema(soup,datePublished=clock)
    facts = pocket_clock_metadata(soup,page_url=URL)
    shape = facts['declared_release']
    assert 'literal_clock_or_known_label' not in shape and 'PRIVATE' not in json.dumps(facts)
    assert not facts['accepted_as_article']


def test_unknown_inner_label_and_person_are_not_misclassified_as_known_org():
    soup = BeautifulSoup(page(),'lxml')
    soup.select_one('.article-meta').string = '私人署名與新聞內文不可當成時鐘樣本'
    change_schema(soup,author={'@type':'Person','name':POCKET_AUTHOR})
    facts = pocket_clock_metadata(soup,page_url=URL)
    assert not facts['declared_author_is_known_Organization']
    assert 'literal_clock_or_known_label' not in facts['inner_update_labels'][0]
    assert '私人署名' not in json.dumps(facts,ensure_ascii=False)


def test_existing_one_response_probe_uses_metadata_without_extra_HTTP_or_acceptance(settings,tmp_path,capsys):
    settings['providers'][0]['embedded_article_layout'] = 'pocket_visible_article_v2'
    registry = PolicyRegistry(settings,base=tmp_path)
    calls = []
    def get(url):
        calls.append(url)
        return Response(url,200,{'content-type':'text/html'},page().encode())
    http = SimpleNamespace(registry=registry,provider_not_before={},robot_delays={},get=get)
    result = probe_provider(http,[URL])[0]
    assert calls == [URL] and not result['pocket_native_clock_validation']['accepted_as_article']
    assert 'private_diagnostic_path' not in result
    capsys.readouterr()
