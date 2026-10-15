"""Exact CSS oracle, bounded static hints and complete original article output."""
from dataclasses import replace

from bs4 import BeautifulSoup
import pytest
import soupsieve

from downloader import news_fulltext_policy as policy
from downloader.news_fulltext_metrics import NewsMetrics, HTML_SELECTOR_CANDIDATE_CONTRACT_VERSION
from scripts.benchmark_news_cpu import implementation, reference_select_nodes_v1
from test_news_fulltext import config


HTML = '''<html><head><link rel="canonical alternate" href="/wrong">
<link rel="canonical" href="/right"><meta property="og:title" content="Title"></head>
<body><section id="outer" class="ancestor"><article id="scope" class="story post-container">
<h1 class="post-title title">原始標題</h1>
<div class="entry bodyContent fullContent rendered"><p class="item first" data-empty=""
itemprop="articleBody other">-12.5 <a rel="author external">原作者</a></p>
<p class="item" id="duplicate" itemprop="articleBody">保留比率 2.3%</p>
<a rel="license" href="https://creativecommons.org/licenses/by/3.0/">授權</a></div>
<p class="item after" id="duplicate" data-label="a > b, c #scope .item">尾文</p>
<div class="other"><p class="item">不相鄰</p></div>
</article><article class="story"><p class="item">另一篇</p></article></section>
<div class="entry bodyContent">未滿足祖先／多class</div></body></html>'''


SELECTORS = [
    '.post-container .entry', '#outer .story .item', '#scope > .entry > p.item',
    'article.story div.entry', 'div.bodyContent.fullContent.rendered',
    '.first + .item', '.first ~ p.after', 'div.entry + p.item',
    'article > div.entry p[itemprop="articleBody"]', 'section article h1.title',
    '#outer #scope .item#duplicate', '#duplicate', '.missing .item',
    'link[rel="canonical"][href]', 'meta[property="og:title"][content]',
    '.entry a[rel~="author"]', '.entry [rel="license"]', '[data-empty]',
    '[data-label="a > b, c #scope .item"]', 'a[href*="creativecommons.org/"]',
    '.story > [itemprop="ARTICLEBODY" i]', 'p[itemprop="articleBody" s]',
    '.entry\n p.item', '.entry\t>\tp.item',
    # Unsupported grammar retains the full CSS engine, not a guessed hint.
    ':scope > .entry', '.story:has(.entry) .item', '.item:nth-child(2)',
    '#scope > .entry, #scope > p', 'div\\.bodyContent', '*|p',
]


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('scope', ['document', 'article', 'entry'])
@pytest.mark.parametrize('selector', SELECTORS)
@pytest.mark.parametrize('limit', [0, 1, 2])
def test_terminal_candidates_preserve_exact_context_order_duplicates_and_limit(parser, scope, selector, limit):
    soup = BeautifulSoup(HTML, parser)
    root = soup if scope == 'document' else soup.article if scope == 'article' else soup.find(class_='entry')
    before = str(soup)
    wanted = root.select(selector, limit=limit)
    actual = policy._select_nodes(root, selector, limit=limit)
    assert len(actual) == len(wanted)
    assert all(a is b for a, b in zip(actual, wanted, strict=True))
    assert str(soup) == before


@pytest.mark.parametrize('selector', ['.entry:scope', '.entry:not(.missing)', 'p, a',
                                     '#scope > :first-child', 'svg|title', '[href="bad\\\\name"]'])
def test_unknown_scope_namespace_escape_and_pseudo_keep_original_engine(selector):
    assert policy._selector_candidate_hint(selector) is None


@pytest.mark.parametrize('selector', ['.story > [rel="', '.entry..item', 'p[=value]', '.story >'])
def test_invalid_css_is_not_silently_empty_even_without_candidates(selector):
    root = BeautifulSoup('<div></div>', 'lxml')
    with pytest.raises(soupsieve.SelectorSyntaxError):
        root.select(selector)
    with pytest.raises(soupsieve.SelectorSyntaxError):
        policy._select_nodes(root, selector)


def test_attribute_value_never_becomes_class_id_or_combinator_hint():
    assert policy._selector_candidate_hint('[data-label="a > b, c #scope .item"]') == (None, 'data-label', True)
    soup = BeautifulSoup(HTML, 'lxml')
    selector = '[data-label="a > b, c #scope .item"]'
    assert policy._select_nodes(soup, selector) == soup.select(selector)


def test_cached_hint_contains_no_dom_and_sees_new_or_modified_documents():
    selector = '.story .entry'
    first = BeautifulSoup('<article class="story"><p class="entry">A</p></article>', 'lxml')
    second = BeautifulSoup('<article class="other"><p class="entry">B</p></article>', 'lxml')
    assert len(policy._select_nodes(first, selector)) == 1
    assert policy._select_nodes(second, selector) == []
    second.article['class'] = ['story']
    assert len(policy._select_nodes(second, selector)) == 1
    assert policy._selector_candidate_hint.cache_info().maxsize == 1024
    assert policy._selector_candidate_hint('.' + 'x' * 4097) is None


def test_candidate_chain_checks_only_terminal_matches_not_every_page_node(monkeypatch):
    soup = BeautifulSoup('<article class="story">' + '<p>ordinary</p>' * 2000
                         + '<div class="entry">完整正文</div></article>', 'lxml')
    original = policy.soupsieve.compile
    visited = []
    class Matcher:
        def __init__(self, matcher):
            self.matcher = matcher
        def match(self, node):
            visited.append(node)
            return self.matcher.match(node)
    monkeypatch.setattr(policy.soupsieve, 'compile', lambda *a, **k: Matcher(original(*a, **k)))
    assert policy._select_nodes(soup, '.story .entry') == [soup.div]
    assert visited == [soup.div]


def test_exact_previous_selector_reference_restores_on_exception():
    from downloader import news_fulltext_publishers as publishers
    original = policy._select_nodes
    native_original = publishers._select_nodes
    with pytest.raises(RuntimeError, match='deliberate'):
        with implementation(True, selector_chain_only=True):
            assert policy._select_nodes is reference_select_nodes_v1
            assert publishers._select_nodes is not native_original
            raise RuntimeError('deliberate')
    assert policy._select_nodes is original
    assert publishers._select_nodes is native_original
    with pytest.raises(ValueError, match='choose_one'):
        with implementation(True, selector_chain_only=True, id_prefix_only=True):
            pass


def test_real_extractor_metadata_body_and_failures_match_previous_selector(config, tmp_path):
    source = policy.PolicyRegistry(config, base=tmp_path).sources['publisher']
    source = replace(source, selectors=('.story .entry',), title_selector='.story h1.title',
                     author_selectors=('.story [rel~="author"]',))
    payload = HTML.replace('-12.5', '完整原始文字與數字 -12.5 比率 2.3% ' * 15).encode()
    before = source.data_fingerprint, source.authorization_fingerprint
    result = policy.extract_article(payload, source, page_url='https://publisher.test/article/1')
    with implementation(True, selector_chain_only=True):
        assert policy.extract_article(payload, source, page_url='https://publisher.test/article/1') == result
    assert (source.data_fingerprint, source.authorization_fingerprint) == before
    for negative in [b'<title>access denied</title>', b'<div data-paywall></div>', b'<article>short</article>']:
        outcomes = []
        for previous in (True, False):
            with implementation(previous, selector_chain_only=True), pytest.raises(policy.NewsError) as caught:
                policy.extract_article(negative, source, page_url='https://publisher.test/article/1')
            outcomes.append((caught.value.code, caught.value.retry, caught.value.delay))
        assert outcomes[0] == outcomes[1]


def test_loaded_performance_contract_not_source_permission_or_body_version():
    assert NewsMetrics().snapshot()['html_selector_candidate_contract_version'] == HTML_SELECTOR_CANDIDATE_CONTRACT_VERSION == 2


@pytest.mark.parametrize('scope', ['document', 'nested'])
@pytest.mark.parametrize('selector', ['p[ITEMPROP]', 'p[itemprop]', '#Root .item',
                                     'p[type="SUBMIT"]', 'p[type="submit"]', 'x|p'])
def test_xml_case_and_namespace_are_not_treated_as_html_candidates(scope, selector):
    soup = BeautifulSoup('<Root xmlns:x="urn:fixture"><div><p ITEMPROP="A" type="SUBMIT"/>'
                         '<p itemprop="B" type="submit"/><x:p class="item"/></div></Root>', 'xml')
    root = soup if scope == 'document' else soup.div
    assert policy._select_nodes(root, selector) == root.select(selector)
