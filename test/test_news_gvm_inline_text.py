"""Native static inline wrappers retain every written value, not exclusions."""
from contextlib import closing
from copy import deepcopy
import json

import pytest

from downloader.news_fulltext_gvm import GVM_INLINE_LAYOUT, GVM_V5_VERIFICATION
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config
from test_news_gvm_known_urls import extract
from test_news_gvm_native import MODIFIED, PUBLISHED, TEXT, URL, policy, schema
from test_news_gvm_tables import TABLE, table_entry, table_page


WRAPPERS = ('<div class="native-note"><a href="/article/12346">合成延伸閱讀保留原文字</a></div>',
    '<div class="native-written-notice"><strong>合成註記</strong><br>價格-1.25、比率1.00%<br>'
    '<a href="/article/12346">合成來源一</a><br><a href="https://example.org/">合成來源二</a><br>'
    '<a href="/article/12347">合成來源三</a></div>')


def inline_entry():
    return {**table_entry(), 'embedded_article_layout': GVM_INLINE_LAYOUT,
        'adapter_verification': GVM_V5_VERIFICATION}


@pytest.mark.parametrize('markup', [*WRAPPERS, ''.join(WRAPPERS), TABLE + ''.join(WRAPPERS)])
def test_actual_refusing_shapes_retain_written_text_order_credit_and_clocks(markup):
    soup = table_page(markup)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup, policy(table_entry()))
    result = extract(soup, policy(inline_entry()))
    assert TEXT in result['body'] and result['published_at_raw'] == PUBLISHED
    assert result['modified_at_raw'] == MODIFIED
    wrappers = soup.select('.article-content > div[class]')
    for wrapper in wrappers:
        for term in wrapper.stripped_strings:
            assert term in result['body']
    if 'native-written-notice' in markup:
        terms = ['合成註記', '價格-1.25、比率1.00%', '合成來源一', '合成來源二', '合成來源三']
        assert [result['body'].index(t) for t in terms] == sorted(result['body'].index(t) for t in terms)
    notices = json.loads(result['verified_article_notices_json'])
    assert notices['native_editor_inline_prose_grammar_version'] == 1
    assert not notices['media_or_scripts_requested_or_executed']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']


@pytest.mark.parametrize('markup', [
    '<div>unclassified</div>', '<div class="empty"></div>',
    '<div class="native-note" data-loader="remote">visible</div>',
    '<div class="native-note" hidden>hidden</div>',
    '<div class="native-note" aria-hidden="true">hidden</div>',
    '<div class="native-note" style="display:none">hidden</div>',
    '<div class="native-note"><div>nested block</div></div>',
    '<div class="native-note"><p>unreviewed block</p></div>',
    '<div class="native-note"><a onclick="anything()">dynamic</a></div>',
    '<div class="native-note"><a data-loader="remote">dynamic</a></div>',
    '<div class="native-note"><span style="display:none">hidden</span></div>',
    '<div class="native-note"><iframe src="https://example.org/">fallback</iframe></div>',
    '<div class="native-note"><script>anything()</script>visible</div>',
    '<div class="native-note"><form>unknown form</form></div>',
    '<div class="native-note"><button>remote loader</button></div>',
])
def test_unknown_hidden_or_dynamic_wrappers_still_refuse(markup):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(table_page(markup), policy(inline_entry()))


def test_inline_notice_cannot_replace_the_required_article_prose():
    soup = table_page(''.join(WRAPPERS) * 5)
    for node in soup.select('.article-content > p'):
        node.decompose()
    with pytest.raises(NewsError, match='body_not_found'):
        extract(soup, policy(inline_entry()))


@pytest.mark.parametrize('mutate,error', [
    (lambda s: s.select_one('.article-head').__setitem__('data-is-accessible', '0'), 'paywall_or_login'),
    (lambda s: s.select_one('link[rel=canonical]').__setitem__('href', URL + '9'), 'article_identity_not_verified'),
    (lambda s: schema(s, lambda a: a['author'][0].update(name='different')), 'article_rights_or_scope_excluded'),
    (lambda s: s.select_one('.article-content > p').append('本文轉載自別家媒體'), 'article_rights_or_scope_excluded'),
    (lambda s: s.select_one('meta[property="article:published_time"]').__setitem__('content', 'unknown'), 'invalid_bulk_publication_clock'),
])
def test_static_written_wrappers_preserve_earlier_gates(mutate, error):
    soup = table_page(''.join(WRAPPERS))
    mutate(soup)
    with pytest.raises(NewsError, match=error):
        extract(soup, policy(inline_entry()))


def test_exact_v4_predecessor_and_original_http_bucket_bindings():
    old, new = policy(table_entry()), policy(inline_entry())
    assert old.data_fingerprint != new.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    for key, value in [('interval_seconds', 5), ('parallelism', 2)]:
        changed = policy({**inline_entry(), key: value})
        assert changed.http_capacity_source_fingerprint != old.http_capacity_source_fingerprint
        assert changed.parser_repair_prior_capture_fingerprint != old.data_fingerprint


def test_v5_reconciliation_preserves_completed_and_other_failed_heads(config, tmp_path):
    settings = deepcopy(config)
    settings['providers'] = [table_entry()]
    with closing(NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))) as queue:
        queue.add_articles([{'url': URL}])
        with queue.db:
            queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
                "attempts=3,due=0,last_checked_at_utc='2026-01-01T00:00:00+00:00'")
        old = dict(queue.db.execute('SELECT * FROM articles').fetchone())
    settings['providers'] = [inline_entry()]
    with closing(NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == old


def test_closed_v4_question_cannot_reinterpret_v5():
    from downloader.news_fulltext_candidates import candidate_evidence_allowed
    from test_news_gvm_failure_context import table_source
    from pathlib import Path
    source = PolicyRegistry({'contract_version': 1, 'collection_purpose': 'personal_research',
        'publish': False, 'remote_training_export': False, 'providers': [inline_entry()]}, base=Path('.')).sources['gvm']
    assert source.allowed
    for version in (34, 35, 36, 37):
        assert not candidate_evidence_allowed(source, source, version)
    assert candidate_evidence_allowed(table_source(), table_source(), 37)


def test_all_registered_default_source_contracts_with_the_v5_candidate(monkeypatch):
    from pathlib import Path
    import test_news_fulltext as checks
    from downloader.news_fulltext import load_config
    settings, _, repo = load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(settings)
    candidate.pop('provider_catalog', None)
    entry = next(p for p in candidate['providers'] if p['id'] == 'gvm')
    entry.update(embedded_article_layout=GVM_INLINE_LAYOUT, adapter_verification=GVM_V5_VERIFICATION)
    registry = PolicyRegistry(candidate, base=repo)
    monkeypatch.setattr(checks, 'load_config', lambda _: (candidate, registry, repo))
    checks.test_default_configuration_separates_personal_capture_from_licenses()
