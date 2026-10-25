"""Synthetic written-body, table-header and empty chart-mount witnesses."""
from copy import deepcopy
from contextlib import closing
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_gvm import GVM_WRITTEN_FORMAT_LAYOUT, GVM_V8_VERIFICATION
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config
from test_news_gvm_known_urls import extract
from test_news_gvm_magazine import magazine_page
from test_news_gvm_media import media_page, untitled_media_entry
from test_news_gvm_native import PUBLISHED, TEXT, URL, policy, schema


CHART = ('<div class="flourish-embed flourish-chart" data-src="visualisation/123456">'
    '<script src="https://public.flourish.studio/resources/embed.js"></script></div>')


def written_entry():
    return {**untitled_media_entry(), 'embedded_article_layout': GVM_WRITTEN_FORMAT_LAYOUT,
        'adapter_verification': GVM_V8_VERIFICATION}


def chart_page(markup=CHART):
    soup = media_page('')
    fragment = BeautifulSoup('<section>' + markup + '</section>', 'lxml')
    for node in list(fragment.section.contents):
        soup.select_one('.article-content').append(node)
    return soup


@pytest.mark.parametrize('periodical', [False, True])
def test_complete_native_paragraphs_do_not_require_an_optional_summary(periodical):
    soup = magazine_page() if periodical else media_page('')
    soup.select_one('blockquote.article-head_blockquote').decompose()
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup, policy(untitled_media_entry()))
    result = extract(soup, policy(written_entry()))
    assert TEXT in result['body'] and result['published_at_raw'] == PUBLISHED
    notices = json.loads(result['verified_article_notices_json'])
    assert notices['native_full_written_body_has_no_summary']
    assert not result['historical_point_in_time']


@pytest.mark.parametrize('change,error', [
    ('empty', 'body_not_found'), ('short', 'body_not_found'),
    ('duplicate_lead', 'article_identity_not_verified'),
    ('hidden_paragraph', 'article_identity_not_verified'),
    ('canonical', 'article_identity_not_verified'), ('schema_credit', 'article_rights_or_scope_excluded'),
    ('nonfree', 'paywall_or_login'),
])
def test_missing_summary_does_not_relax_full_body_identity_access_or_credit(change, error):
    soup = media_page('')
    if change == 'duplicate_lead':
        soup.select_one('.article-content').append(deepcopy(soup.select_one('blockquote')))
    else:
        soup.select_one('blockquote').decompose()
    paragraphs = soup.select('.article-content > p')
    if change == 'empty':
        for node in paragraphs: node.decompose()
    elif change == 'short':
        for node in paragraphs: node.string = '短文'
    elif change == 'hidden_paragraph': paragraphs[0]['hidden'] = ''
    elif change == 'canonical': soup.select_one('link[rel=canonical]')['href'] += '9'
    elif change == 'schema_credit': schema(soup, lambda a: a['author'][0].update(name='不同署名'))
    elif change == 'nonfree': schema(soup, lambda a: a.update(isAccessibleForFree=False))
    with pytest.raises(NewsError, match=error):
        extract(soup, policy(written_entry()))


@pytest.mark.parametrize('scope', ['row', 'col', 'rowgroup', 'colgroup'])
def test_semantic_table_header_scope_preserves_every_cell(scope):
    soup = magazine_page()
    head = soup.select_one('th')
    head['scope'] = scope
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup, policy(untitled_media_entry()))
    result = extract(soup, policy(written_entry()))
    del head['scope']
    baseline = extract(soup, policy(untitled_media_entry()))
    for key in ('body', 'body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw'):
        assert result[key] == baseline[key]
    assert json.loads(result['verified_article_notices_json'])['native_visible_table_header_scope_verified']


@pytest.mark.parametrize('tag,attrs', [
    ('th', {'scope': ''}), ('th', {'scope': 'unknown'}), ('td', {'scope': 'col'}),
    ('th', {'scope': 'col', 'onload': 'anything()'}),
    ('th', {'scope': 'col', 'style': 'display:none'}),
    ('th', {'scope': 'col', 'rowspan': 'bad'}),
])
def test_header_scope_does_not_authorize_unknown_attributes_hidden_cells_or_bad_spans(tag, attrs):
    soup = magazine_page(); node = soup.select_one(tag); node.attrs.update(attrs)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup, policy(written_entry()))


def test_empty_chart_mount_does_not_supply_observations_or_remove_written_prose():
    baseline = extract(media_page(''), policy(untitled_media_entry()))
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(chart_page(), policy(untitled_media_entry()))
    result = extract(chart_page(), policy(written_entry()))
    for key in ('body', 'body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw'):
        assert result[key] == baseline[key]
    notices = json.loads(result['verified_article_notices_json'])
    assert notices['native_empty_flourish_chart_loader_verified']
    assert not notices['dynamic_chart_values_captured']
    assert not notices['media_or_scripts_requested_or_executed']
    assert 'empty_Flourish_chart_values_not_captured' in result['body_completeness']


@pytest.mark.parametrize('old,new', [
    ('flourish-chart', 'flourish-map'), ('visualisation/123456', 'visualisation/0'),
    ('visualisation/123456', 'visualisation/123456?key=anything'),
    ('https://public.flourish.studio/', 'http://public.flourish.studio/'),
    ('https://public.flourish.studio/', 'https://other.example/'),
    ('/resources/embed.js', '/resources/embed.js?token=anything'),
    ('data-src=', 'hidden="" data-src='), ('data-src=', 'onclick="anything()" data-src='),
    ('src=', 'onload="anything()" src='),
    ('</script>', 'anything()</script>'), ('</script>', '<!-- not empty --></script>'),
    ('</div>', 'fallback prose</div>'), ('</div>', '<p>written fallback</p></div>'),
    ('</div>', '<!-- unknown fallback --></div>'),
    ('</div>', '<script src="https://other.example/loader.js"></script></div>'),
])
def test_chart_fallbacks_unknown_loaders_and_hidden_mounts_remain_failures(old, new):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(chart_page(CHART.replace(old, new)), policy(written_entry()))


def test_nested_or_rogue_script_does_not_inherit_chart_exception():
    for markup in ('<div>' + CHART + '</div>', CHART + '<script>anything()</script>'):
        with pytest.raises(NewsError, match='article_identity_not_verified'):
            extract(chart_page(markup), policy(written_entry()))


def test_old_complete_articles_keep_every_field_except_named_parser_identity():
    for soup in (media_page(), magazine_page()):
        old, new = extract(soup, policy(untitled_media_entry())), extract(soup, policy(written_entry()))
        assert {k: v for k, v in old.items() if k != 'article_identity_basis'} == {
            k: v for k, v in new.items() if k != 'article_identity_basis'}


def test_only_exact_v7_scope_keeps_its_http_budget_and_repair_predecessor():
    old, new = policy(untitled_media_entry()), policy(written_entry())
    assert old.data_fingerprint != new.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    for key, value in [('interval_seconds', 5), ('parallelism', 2)]:
        different = policy({**written_entry(), key: value})
        assert different.http_capacity_source_fingerprint != old.http_capacity_source_fingerprint
        assert different.parser_repair_prior_capture_fingerprint != old.data_fingerprint


def test_registered_default_source_contract_with_v8_candidate(monkeypatch):
    from downloader.news_fulltext import load_config
    import test_news_fulltext as checks
    settings, _, repo = load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(settings); candidate.pop('provider_catalog', None)
    entry = next(p for p in candidate['providers'] if p['id'] == 'gvm')
    entry.update(embedded_article_layout=GVM_WRITTEN_FORMAT_LAYOUT, adapter_verification=GVM_V8_VERIFICATION)
    registry = PolicyRegistry(candidate, base=repo)
    monkeypatch.setattr(checks, 'load_config', lambda _: (candidate, registry, repo))
    checks.test_default_configuration_separates_personal_capture_from_licenses()


@pytest.mark.parametrize('old_version,new_version', [(5, 6), (6, 7), (7, 8), (8, 9)])
def test_parser_upgrade_preserves_failures_attempts_deadlines_and_completed_bodies(tmp_path, config, old_version, new_version):
    from test_news_gvm_inline_text import inline_entry
    from test_news_gvm_magazine import magazine_entry
    from test_news_gvm_static_editor import static_entry
    entries = {5: inline_entry, 6: magazine_entry, 7: untitled_media_entry, 8: written_entry, 9: static_entry}
    states = ['body_not_found', 'article_attribution_not_verified', 'article_title_not_verified',
        'article_identity_not_verified', 'paywall_or_login', 'article_rights_or_scope_excluded',
        'retry', 'retry_cooldown', 'complete']
    settings = deepcopy(config); settings['providers'] = [entries[old_version]()]
    with closing(NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))) as queue:
        queue.add_articles([{'url': URL + str(i)} for i in range(len(states))])
        with queue.db:
            for i, state in enumerate(states):
                queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=1234 WHERE url=?',
                    (state, state, URL + str(i)))
            queue.db.execute('UPDATE articles SET body_sha256=?,body_bytes=1,parquet_path=? WHERE state=?',
                ('retained-body', 'retained.parquet', 'complete'))
        before = [dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')]
    settings['providers'] = [entries[new_version]()]
    with closing(NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))) as queue:
        assert [dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')] == before
