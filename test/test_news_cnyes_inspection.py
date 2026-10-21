"""Inspection must not acquire an HTTP writer or retain restricted responses."""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_metrics import NewsMetrics
from downloader.news_fulltext_policy import PolicyRegistry, NewsError
from downloader.news_fulltext_queue import NewsQueue
from scripts import inspect_cnyes_news as inspector
from test_cnyes_news_bulk import cfg, payload, response
from test_news_fulltext import article_html


URL = 'https://news.cnyes.com/news/id/42'


@pytest.mark.parametrize('url,hint', [
    ('https://www.cnyes.com/twstock/2330', 'market_endpoint_route_candidate'),
    ('http://www.cnyes.com/twstock/2330/company/--?q=literal&amp;x=1', 'market_endpoint_route_candidate'),
    ('https://news.cnyes.com/Content/20110124/ABC12.shtml', 'legacy_shtml_article_route_candidate'),
    ('https://news.cnyes.com/20090124/12345.shtml', 'legacy_shtml_article_route_candidate'),
    ('https://news.cnyes.com/content/20110124/ABC12.shtml', 'legacy_shtml_article_route_candidate'),
    ('https://amp-news.cnyes.com/news/id/42', 'current_article_route_candidate'),
    ('https://news.cnyes.com/news/aigc/answer/abc', 'other_news_host_route_unreviewed'),
    ('https://hao.cnyes.com/hot', 'other_cnyes_route_unreviewed'),
    ('https://www.cnyes.com/company/profile', 'other_cnyes_route_unreviewed'),
    ('https://www.cnyes.com/twstockish/2330', 'other_cnyes_route_unreviewed'),
    ('https://www.cnyes.com:443/twstock/2330', 'foreign_origin_unreviewed'),
    ('https://www.cnyes.com.invalid/twstock/2330', 'foreign_origin_unreviewed'),
    ('https://news.cnyes.com/%43ontent/20110124/ABC12.shtml', 'other_news_host_route_unreviewed'),
    ('https://user:secret@www.cnyes.com/twstock/2330', 'invalid_or_oversized_url'),
    ('https://[invalid', 'invalid_or_oversized_url'),
    ('javascript:alert(1)', 'invalid_or_oversized_url'),
    ('x' * 8193, 'invalid_or_oversized_url'),
    (None, 'invalid_or_oversized_url'),
])
def test_literal_route_hints_are_syntax_only_without_identity_rewrites(url, hint):
    assert inspector.cnyes_literal_route_hint(url) == hint


def route_database():
    db = sqlite3.connect(':memory:')
    db.executescript('''CREATE TABLE articles(url TEXT,provider TEXT,state TEXT,due REAL,body_sha256 TEXT);
        CREATE INDEX dispatch ON articles(provider,state,due);''')
    return db


def test_unfinished_route_census_is_indexed_bounded_and_preserves_body_presence(monkeypatch):
    db = route_database()
    db.executemany('INSERT INTO articles VALUES (?,?,?,?,?)', [
        ('https://www.cnyes.com/twstock/2330', 'cnyes', 'unsupported_article_path', 1, None),
        ('https://www.cnyes.com/twstock/2330/company/--', 'cnyes', 'unsupported_article_path', 2, 'retained'),
        ('https://news.cnyes.com/20110124/12345.shtml', 'cnyes', 'unsupported_article_path', 3, None),
        ('https://hao.cnyes.com/hot', 'cnyes', 'unsupported_article_path', 4, None),
        ('https://news.cnyes.com/news/id/42', 'cnyes', 'body_not_found', 0, None),
        ('https://www.cnyes.com/twstock/9999', 'other', 'unsupported_article_path', 0, None),
    ])
    db.executemany('INSERT INTO articles VALUES (?,?,?,?,?)', [
        (f'completed-must-not-be-inspected-{i}', 'cnyes', 'complete', 0, 'stored') for i in range(10000)])
    db.commit()
    before = db.total_changes
    native_hint = inspector.cnyes_literal_route_hint
    def no_healthy_body(url):
        assert not url.startswith('completed-')
        return native_hint(url)
    monkeypatch.setattr(inspector, 'cnyes_literal_route_hint', no_healthy_body)
    statements, steps = [], []
    db.set_trace_callback(statements.append)
    db.set_progress_handler(lambda: steps.append(1) or 0, 100)
    result = inspector.cnyes_unsupported_path_inventory(db)
    assert len(steps) < 5  # The 10k completed heads are outside the index range.
    assert len(statements) == 1 and 'INDEXED BY dispatch' in statements[0]
    assert "state='unsupported_article_path'" in statements[0] and 'LIMIT 4097' in statements[0]
    assert db.total_changes == before
    assert result['heads_examined'] == result['total_unsupported_url_heads'] == 4
    assert result['all_current_unsupported_heads_included'] and not result['head_limit_reached']
    groups = result['route_hint_counts']
    assert groups['market_endpoint_route_candidate']['url_heads'] == 2
    assert groups['market_endpoint_route_candidate']['missing_body_heads'] == 1
    assert groups['market_endpoint_route_candidate']['stored_body_heads'] == 1
    assert groups['legacy_shtml_article_route_candidate']['url_heads'] == 1
    assert sum(group['url_heads'] for group in groups.values()) == 4
    for key in ('article_count_inferred', 'body_acceptance_granted', 'source_absence_or_permission_inferred',
                'completion_denominator_changed'):
        assert result[key] is False
    for key in ('publisher_requests', 'production_queue_writes', 'completed_body_rows_scanned', 'raw_or_body_payloads_read'):
        assert result[key] == 0
    db.close()


def test_route_census_overflow_does_not_claim_an_exact_total_or_complete_scope():
    db = route_database()
    db.executemany('INSERT INTO articles VALUES (?,?,?,?,?)', [
        (f'https://www.cnyes.com/twstock/{i}', 'cnyes', 'unsupported_article_path', i, None) for i in range(10)])
    result = inspector.cnyes_unsupported_path_inventory(db, head_limit=4)
    assert result['heads_examined'] == 4 and result['head_limit_reached']
    assert result['total_unsupported_url_heads'] is None
    assert result['all_current_unsupported_heads_included'] is False
    assert len(result['route_hint_counts']['market_endpoint_route_candidate']['literal_url_samples']) == 3
    db.close()


def test_invalid_or_foreign_route_samples_do_not_export_credentials():
    db = route_database()
    db.executemany('INSERT INTO articles VALUES (?,?,?,?,?)', [
        ('https://user:secret@www.cnyes.com/twstock/2330', 'cnyes', 'unsupported_article_path', 0, None),
        ('https://foreign.invalid/token/secret', 'cnyes', 'unsupported_article_path', 1, None),
    ])
    result = inspector.cnyes_unsupported_path_inventory(db)
    assert result['heads_examined'] == 2
    assert 'secret' not in json.dumps(result)
    assert all(group['literal_url_samples'] == [None] for group in result['route_hint_counts'].values())
    db.close()


@pytest.mark.parametrize('head_limit', [0, -1, 4097, True, 1.5])
def test_route_census_rejects_unbounded_or_invalid_work(head_limit):
    with pytest.raises(ValueError, match='1..4096'):
        inspector.cnyes_unsupported_path_inventory(None, head_limit=head_limit)


@pytest.mark.parametrize('visible,current,whitespace,entities', [
    ('股 價', '股價', True, False),
    ('A & B', 'A &amp; B', False, True),
    ('不同標題', '原始標題', False, False),
    ('相同標題', '相同標題', True, True),
    ('較長標題', '較長', False, False),
])
def test_title_comparison_is_bounded_metadata_not_title_text(visible, current, whitespace, entities):
    result = inspector.headline_comparison_metadata(visible, current)
    assert result['whitespace_equivalent'] is whitespace
    assert result['entity_decoded_equivalent'] is entities
    assert result['exact_match'] is (visible == current)
    assert result['visible_characters'] == len(visible)
    assert visible not in json.dumps(result, ensure_ascii=False)
    assert current not in json.dumps(result, ensure_ascii=False)
    assert set(result['first_different_codepoints']) == {'visible', 'current'}


@pytest.mark.parametrize('value', [None, False, {}, 0])
def test_title_comparison_requires_strings(value):
    assert inspector.headline_comparison_metadata('visible', value) == {'string_pair': False}


def environment(cfg, tmp_path, monkeypatch):
    cfg['root'] = 'store'
    registry = PolicyRegistry(cfg, base=tmp_path)
    queue = NewsQueue(tmp_path / 'store', registry)
    queue.close()
    atomic_write_json(tmp_path / 'store/status.json', {'activity': 'running', 'owner_id': 'original-owner'})
    monkeypatch.setattr(inspector, 'load_config', lambda _path: (cfg, registry, tmp_path))
    monkeypatch.setattr(inspector.subprocess, 'check_output', lambda *_args, **_kwargs: '')
    return registry, tmp_path / 'result.json'


def http_response(monkeypatch, response):
    calls = []
    http = SimpleNamespace(metrics=NewsMetrics(), provider_not_before={}, close=lambda: calls.append('close'))
    http.get = lambda url, **kwargs: (calls.append(url) or response)
    monkeypatch.setattr(inspector, 'NewsHTTP', lambda *_args: http)
    return calls


def test_readonly_inventory_does_not_construct_http_or_mutate_queue(cfg, tmp_path, monkeypatch):
    _, output = environment(cfg, tmp_path, monkeypatch)
    db_path = tmp_path / 'store/queue.sqlite3'
    before = db_path.read_bytes()
    monkeypatch.setattr(inspector, 'NewsHTTP', lambda *_args: pytest.fail('read-only inventory acquired HTTP owner'))
    assert inspector.main(['--output', str(output)]) == 0
    assert json.loads(output.read_text())['probes'] == []
    assert db_path.read_bytes() == before
    assert not (tmp_path / 'store/cooldowns.json').exists()


def test_path_only_mode_avoids_full_status_services_and_http(cfg, tmp_path, monkeypatch):
    _, output = environment(cfg, tmp_path, monkeypatch)
    db_path = tmp_path / 'store/queue.sqlite3'
    before = db_path.read_bytes()
    read_text = Path.read_text
    def no_status(path, *args, **kwargs):
        assert path != tmp_path / 'store/status.json'
        return read_text(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'read_text', no_status)
    monkeypatch.setattr(inspector, 'NewsHTTP', lambda *_a: pytest.fail('path census acquired HTTP'))
    monkeypatch.setattr(inspector.subprocess, 'check_output', lambda *_a, **_k: pytest.fail('path census scanned services'))
    assert inspector.main(['--unsupported-paths-only', '--output', str(output)]) == 0
    result = json.loads(output.read_text())
    assert result['total_unsupported_url_heads'] == 0 and result['publisher_requests'] == 0
    assert 'status' not in result and 'services' not in result
    assert db_path.read_bytes() == before


@pytest.mark.parametrize('args', [['--probe', URL], ['--canonical-fetch'], ['--unsupported-head-limit', '4097']])
def test_path_only_mode_rejects_probe_or_unbounded_work_before_loading_configuration(tmp_path, monkeypatch, args):
    monkeypatch.setattr(inspector, 'load_config', lambda *_a: pytest.fail('invalid inspection loaded configuration'))
    with pytest.raises(SystemExit) as result:
        inspector.main(['--unsupported-paths-only', '--output', str(tmp_path / 'result.json'), *args])
    assert result.value.code == 2


@pytest.mark.parametrize('canonical,url', [
    (False, 'https://unregistered.test/article/42'), (True, URL),
])
def test_invalid_probe_is_rejected_before_http_construction(cfg, tmp_path, monkeypatch, canonical, url):
    _, output = environment(cfg, tmp_path, monkeypatch)
    monkeypatch.setattr(inspector, 'NewsHTTP', lambda *_args: pytest.fail('invalid probe acquired HTTP owner'))
    args = ['--output', str(output), '--probe', url] + (['--canonical-fetch'] if canonical else [])
    with pytest.raises((NewsError, ValueError)):
        inspector.main(args)


@pytest.mark.parametrize('code', ['paywall_or_login', 'access_challenge', 'article_rights_or_scope_excluded',
                                 'article_license_not_verified', 'article_attribution_not_verified'])
def test_html_restriction_is_not_saved_as_raw_diagnostic(cfg, tmp_path, monkeypatch, code):
    _, output = environment(cfg, tmp_path, monkeypatch)
    raw = Response(URL, 200, {'content-type': 'text/html', 'set-cookie': 'secret', 'authorization': 'secret'}, article_html())
    calls = http_response(monkeypatch, raw)
    monkeypatch.setattr(inspector, 'extract_article', lambda *_args, **_kwargs: (_ for _ in ()).throw(NewsError(code)))
    assert inspector.main(['--output', str(output), '--probe', URL]) == 0
    probe = json.loads(output.read_text())['probes'][0]
    assert probe['extraction_error'] == code and probe['raw_response_retained'] is False
    assert 'private_raw_path' not in probe and not list(tmp_path.glob('*.raw'))
    assert 'secret' not in output.read_text() and calls == [URL, 'close']


def test_body_failure_without_public_identity_is_metadata_only(cfg, tmp_path, monkeypatch):
    _, output = environment(cfg, tmp_path, monkeypatch)
    raw = Response(URL, 200, {'content-type': 'text/html'}, b'<html><body></body></html>')
    http_response(monkeypatch, raw)
    monkeypatch.setattr(inspector, 'extract_article', lambda *_args, **_kwargs: (_ for _ in ()).throw(NewsError('body_not_found')))
    assert inspector.main(['--output', str(output), '--probe', URL]) == 0
    probe = json.loads(output.read_text())['probes'][0]
    assert probe['extraction_error'] == 'body_not_found' and probe['raw_response_retained'] is False
    assert not probe['cnyes_structure']['raw_public_scope']
    assert 'private_raw_path' not in probe and not list(tmp_path.glob('*.raw'))
    assert probe.get('article') is None


@pytest.mark.parametrize('changes,expected', [
    ({}, True), ({'payment': 1}, False), ({'payment': False}, False),
    ({'payment': None}, False), ({'newsId': 43}, False),
    ({'publishAt': 1388534401}, False), ({'url': URL.replace('/42', '/43')}, False),
])
def test_cnyes_body_comparison_metadata_requires_matching_public_identity(cfg, tmp_path, changes, expected):
    from bs4 import BeautifulSoup
    from test_news_cnyes_canonical_alias import MOBILE, mobile_page
    registry = PolicyRegistry(cfg, base=tmp_path)
    source = registry.sources['cnyes']
    metadata = inspector.cnyes_html_structure(BeautifulSoup(mobile_page(changes=changes), 'lxml'),
                                             source, page_url=MOBILE)
    assert metadata['raw_public_scope'] is expected
    assert bool(metadata.get('body_representations')) is expected
    assert 'body' not in metadata and 'author' not in metadata
    if expected:
        assert len({value['whitespace_normalized_sha256'] for value in metadata['body_representations'].values()}) == 1


def test_confirmed_free_body_mismatch_is_diagnostic_not_acceptance(cfg, tmp_path, monkeypatch):
    from test_news_cnyes_canonical_alias import mobile_page
    _, output = environment(cfg, tmp_path, monkeypatch)
    payload = mobile_page(schema_changes={'articleBody': '不同的來源宣告，不能用來替換正文。'})
    raw = Response(URL, 200, {'content-type': 'text/html'}, payload)
    http_response(monkeypatch, raw)
    monkeypatch.setattr(inspector, 'extract_article', lambda *_args, **_kwargs: (_ for _ in ()).throw(NewsError('body_not_found')))
    assert inspector.main(['--output', str(output), '--probe', URL]) == 0
    probe = json.loads(output.read_text())['probes'][0]
    assert probe['extraction_error'] == 'body_not_found' and probe['raw_response_retained'] is True
    assert probe['cnyes_structure']['raw_public_scope']
    bodies = probe['cnyes_structure']['body_representations']
    assert bodies['visible']['whitespace_normalized_sha256'] != bodies['schema']['whitespace_normalized_sha256']
    assert Path(probe['private_raw_path']).read_bytes() == payload
    assert probe.get('article') is None


def test_outer_title_whitespace_uses_shared_source_match_without_exporting_title(cfg, tmp_path):
    from bs4 import BeautifulSoup
    from test_news_cnyes_canonical_alias import MOBILE, TITLE, mobile_page
    title = TITLE + '\u00a0'
    source = PolicyRegistry(cfg, base=tmp_path).sources['cnyes']
    metadata = inspector.cnyes_html_structure(BeautifulSoup(mobile_page(changes={'title': title},
        schema_changes={'headline': title}), 'lxml'), source, page_url=MOBILE)
    assert metadata['raw_public_scope'] and metadata['visible_headline_matches_current']
    assert not metadata['headline_comparison']['exact_match']
    assert metadata['headline_comparison']['whitespace_equivalent']
    assert title not in json.dumps(metadata, ensure_ascii=False)


@pytest.mark.parametrize('change', ['none', 'wrong_canonical', 'wrong_og', 'missing_og', 'wrong_main_entity'])
def test_native_identity_diagnosis_reports_only_bounded_source_comparisons(cfg, tmp_path, change):
    from bs4 import BeautifulSoup
    from test_news_cnyes_canonical_alias import MOBILE, TITLE, mobile_page
    source = PolicyRegistry(cfg, base=tmp_path).sources['cnyes']
    raw = mobile_page()
    soup = BeautifulSoup(raw, 'lxml')
    if change == 'wrong_canonical':
        soup.select_one('link[rel="canonical"]')['href'] = URL + '0'
    elif change == 'wrong_og':
        soup.select_one('meta[property="og:url"]')['content'] = URL + '0'
    elif change == 'missing_og':
        soup.select_one('meta[property="og:url"]').decompose()
    elif change == 'wrong_main_entity':
        # Alter the actual NewsArticle inside @graph and reparse its real
        # script node; mutating a BeautifulSoup NavigableString is not an
        # equivalent source response and get_text() may then be empty.
        soup = BeautifulSoup(mobile_page(schema_changes={
            'mainEntityOfPage': {'@id': URL + '0'}}), 'lxml')
    result = inspector.cnyes_html_structure(soup, source, page_url=MOBILE)
    assert result['contract_version'] == 5
    assert result['canonical_path_matches_request'] is (change != 'wrong_canonical')
    assert result['canonical_public_origin']
    assert result['canonical_origin_kind'] == 'desktop_news'
    assert result['og_url_count'] == (0 if change == 'missing_og' else 1)
    assert result['og_url_matches_canonical'] is (change not in {'wrong_canonical', 'wrong_og', 'missing_og'})
    if change != 'wrong_canonical':
        assert result['schema_main_entity_matches_canonical'] is (change != 'wrong_main_entity')
    # Shape metadata does not grant body/identity acceptance or export the
    # source headline, author, query values or adjacent article prose.
    encoded = json.dumps(result, ensure_ascii=False)
    assert TITLE not in encoded and URL not in encoded and MOBILE not in encoded
    assert 'accepted_as_article' not in result


def test_schema_identity_shape_survives_missing_current_data_without_granting_raw(cfg, tmp_path):
    from bs4 import BeautifulSoup
    from test_news_cnyes_canonical_alias import MOBILE, mobile_page
    source = PolicyRegistry(cfg, base=tmp_path).sources['cnyes']
    result = inspector.cnyes_html_structure(BeautifulSoup(mobile_page(changes={'newsId': 43}), 'lxml'),
                                             source, page_url=MOBILE)
    assert result['current_schema_count'] == 1 and result['schema_main_entity_matches_canonical']
    assert not result['current_data_present'] and not result['raw_public_scope']
    assert 'body_representations' not in result and 'declared_clock_raw' not in result


@pytest.mark.parametrize('canonical,kind', [
    (URL, 'desktop_news'), (URL.replace('news.cnyes.com', 'm.cnyes.com'), 'mobile_news'),
    ('https://unknown.test/news/id/42', 'other'),
])
def test_diagnostic_distinguishes_canonical_origin_without_exporting_source_uri(cfg, tmp_path, canonical, kind):
    from bs4 import BeautifulSoup
    from test_news_cnyes_canonical_alias import MOBILE, mobile_page
    source = PolicyRegistry(cfg, base=tmp_path).sources['cnyes']
    soup = BeautifulSoup(mobile_page(), 'lxml')
    soup.select_one('link[rel="canonical"]')['href'] = canonical
    result = inspector.cnyes_html_structure(soup, source, page_url=MOBILE)
    assert result['canonical_origin_kind'] == kind
    assert canonical not in json.dumps(result, ensure_ascii=False)


@pytest.mark.parametrize('payment,retained', [(0, True), (1, False), (False, False), (None, False)])
def test_json_retention_requires_literal_public_payment_for_every_row(cfg, tmp_path, monkeypatch, payment, retained):
    _, output = environment(cfg, tmp_path, monkeypatch)
    data = payload(ids=(1, 2))
    data['items']['data'][1]['payment'] = payment
    raw = response(data)
    http_response(monkeypatch, raw)
    assert inspector.main(['--output', str(output), '--probe', raw.url]) == 0
    probe = json.loads(output.read_text())['probes'][0]
    assert probe['raw_response_retained'] is retained
    assert bool(list(tmp_path.glob('*.raw'))) is retained
