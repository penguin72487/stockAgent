"""Policy inspection selects its exact source section before boilerplate removal."""
import pytest

from scripts.probe_news_sources import scoped_primary_document_text


@pytest.mark.parametrize('ancestor', ['header', 'nav', 'aside', 'footer', 'div'])
def test_declared_policy_is_not_lost_with_misnested_ancestor(ancestor):
    payload = (f'<html><body><{ancestor}><p>UNRELATED_NAV</p>'
        '<div class="source-policy"><h1>來源政策</h1><p>原始條件 &amp; 保留。</p>'
        '<script>DO_NOT_EXECUTE_OR_EXPORT</script><style>AD_STYLE</style>'
        '<nav>INNER_NAV</nav><p>最後一段。</p></div>'
        f'<p>UNRELATED_FOOTER</p></{ancestor}></body></html>').encode()
    assert scoped_primary_document_text(payload, '.source-policy') == '來源政策\n原始條件 & 保留。\n最後一段。'


@pytest.mark.parametrize('payload,selector,error', [
    (b'<div>not the declared document</div>', '.policy', 'unique_nonarticle'),
    (b'<div class="policy">one</div><div class="policy">two</div>', '.policy', 'unique_nonarticle'),
    (b'<body><p>whole page is not a policy scope</p></body>', 'body', 'unique_nonarticle'),
    (b'<html><body>whole page</body></html>', 'html', 'unique_nonarticle'),
    (b'<script>not policy text</script>', 'script', 'unique_nonarticle'),
    ('not bytes', '.policy', 'bounded_explicit'),
    (b'x' * ((1 << 20) + 1), '.policy', 'bounded_explicit'),
    (b'<div>one</div>', '', 'bounded_explicit'),
    (b'<div>one</div>', 'x' * 257, 'bounded_explicit'),
    (b'<div>one</div>', None, 'bounded_explicit'),
])
def test_missing_ambiguous_or_overbroad_scope_never_becomes_whole_page(payload, selector, error):
    with pytest.raises(ValueError, match=error):
        scoped_primary_document_text(payload, selector)


def test_scope_preserves_paragraphs_values_and_source_order():
    payload = b'<div id="policy"><p>2014 / -10.5 / 0</p><p>A<br>B</p><ul><li>C</li><li>D</li></ul></div>'
    assert scoped_primary_document_text(payload, '#policy') == '2014 / -10.5 / 0\nA\nB\nC\nD'
