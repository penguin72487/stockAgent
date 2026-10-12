"""Inspection must not acquire an HTTP writer or retain restricted responses."""
from __future__ import annotations

import json
from pathlib import Path
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
