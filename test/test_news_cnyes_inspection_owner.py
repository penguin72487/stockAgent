"""Optional diagnosis defers to the live owner; never bypass its lock."""
import json

import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from scripts import inspect_cnyes_news as inspector
from test_cnyes_news_bulk import cfg as cnyes_config


URL = 'https://news.cnyes.com/news/id/42'


@pytest.fixture
def inspection(cnyes_config, tmp_path, monkeypatch):
    root = tmp_path / 'news'
    config = {**cnyes_config, 'root': 'news'}
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(root, registry)
    with queue.db:
        queue.add_articles([{'url': URL}])
    (root / 'status.json').write_text(json.dumps({'owner_id': 'live-canonical-owner'}))
    monkeypatch.setattr(inspector, 'load_config', lambda _: (config, registry, tmp_path))
    monkeypatch.setattr(inspector.subprocess, 'check_output', lambda *_a, **_k: '')
    yield queue, tmp_path / 'optional-inspection.json'
    queue.close()


@pytest.mark.parametrize('canonical', [False, True])
def test_live_owner_lock_is_explicit_deferred_without_queue_mutation(inspection, monkeypatch, canonical):
    queue, output = inspection
    before = [tuple(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    def busy(*_a, **_k):
        raise NewsError('news_http_owner_already_running')
    monkeypatch.setattr(inspector, 'NewsHTTP', busy)
    assert inspector.main(['--probe', URL, '--output', str(output)] + (['--canonical-fetch'] if canonical else [])) == 0
    report = json.loads(output.read_bytes())
    assert report['state'] == 'deferred_active_canonical_http_owner'
    assert report['reason'] == 'news_http_owner_already_running' and report['explicit_selected_urls'] == [URL]
    assert report['publisher_requests'] == 0 and report['probes'] == []
    assert not report['source_states_mutated'] and not report['raw_response_retained']
    assert not report['original_source_integrity_verified'] and not report['all_history_complete']
    assert [tuple(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY id')] == before


def test_other_initialization_failure_not_masked_as_busy_owner(inspection, monkeypatch):
    _, output = inspection
    def broken(*_a, **_k):
        raise NewsError('invalid_robots_configuration')
    monkeypatch.setattr(inspector, 'NewsHTTP', broken)
    with pytest.raises(NewsError, match='invalid_robots_configuration'):
        inspector.main(['--probe', URL, '--output', str(output)])
    assert 'state' not in json.loads(output.read_bytes())
