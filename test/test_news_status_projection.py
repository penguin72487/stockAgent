from itertools import product
import json

import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext_http import NewsHTTP
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, summarize_discovery_status
from scripts.benchmark_news_status_projection import legacy_discovery_status, profile_current_status
from test_news_fulltext import config


def assert_discovery_projection(db, clock):
    actual = summarize_discovery_status(db, clock)
    assert actual == legacy_discovery_status(db, clock)
    assert len(actual) == 5
    assert all(type(value) is int and value >= 0 for value in actual)


def add_discovery_cases(queue, clock):
    rows = [
        (f'https://publisher.test/discovery/{number}', kind, 'publisher', deadline, error, superseded)
        for number, (kind, deadline, error, superseded) in enumerate(product(
            ('feed', 'cnyes_news', 'wordpress_posts', 'rss_fulltext', 'unknown'),
            (0, clock - 0.001, clock, clock + 0.001, clock + 1000),
            (None, '', 'source_access_denied'), (None, '', '[]', '["child"]')))
    ]
    queue.db.executemany('INSERT INTO discovery(url,kind,provider,due,error_code,superseded_by_windows_json) '
                         'VALUES (?,?,?,?,?,?)', rows)


def test_exact_discovery_projection_empty_due_boundary_and_archived_errors(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        assert summarize_discovery_status(queue.db, 100) == (0, 0, 0, 0, 0)
        with queue.db:
            add_discovery_cases(queue, 100)
        changes = queue.db.total_changes
        for clock in (0, 99.999, 100, 100.001, 1100):
            assert_discovery_projection(queue.db, clock)
        assert queue.db.total_changes == changes
        # Literal non-NULL markers retain the old archive semantics; the
        # optimization must not reinterpret an empty JSON list as active.
        assert summarize_discovery_status(queue.db, 100)[2] == 225
        with queue.db:
            queue.db.execute("UPDATE discovery SET error_code=NULL,superseded_by_windows_json=NULL,due=0 "
                             "WHERE kind='cnyes_news'")
        assert_discovery_projection(queue.db, 100)
        queue.db.execute("DELETE FROM discovery WHERE kind='unknown'")
        assert_discovery_projection(queue.db, 100)
        queue.db.rollback()
        assert_discovery_projection(queue.db, 100)
    finally:
        queue.close()


def test_whole_status_benchmark_pins_owner_preserves_publication_and_source_rows(config, tmp_path, monkeypatch):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with queue.db:
            add_discovery_cases(queue, 100)
        queue.status(activity='fixture')
        before_status = sha256_file(tmp_path / 'status.json')
        before_settings = list(queue.db.execute('SELECT * FROM queue_settings ORDER BY key'))
        before_rows = list(queue.db.execute('SELECT * FROM discovery ORDER BY url'))
        changes = queue.db.total_changes

        def no_owner_or_http(*args, **kwargs):
            raise AssertionError('read-only replay must not start an acquisition owner')

        monkeypatch.setattr(NewsQueue, '__init__', no_owner_or_http)
        monkeypatch.setattr(NewsHTTP, '__init__', no_owner_or_http)
        result = profile_current_status(tmp_path, queue.registry, 3, compare_discovery_summary=True)
        assert result['accepted']
        assert result['publisher_requests'] == 0
        assert result['source_states_mutated'] is False
        assert result['status_published'] is False
        assert set(result['measurements']) == {'legacy', 'summary'}
        assert all(len(samples) == 3 for samples in result['measurements'].values())
        assert result['deterministic_projection_sha256']['legacy'] == result['deterministic_projection_sha256']['summary']
        assert len(result['top_query_execution_times']) <= 12
        assert sha256_file(tmp_path / 'status.json') == before_status
        assert queue.db.total_changes == changes
        assert list(queue.db.execute('SELECT * FROM queue_settings ORDER BY key')) == before_settings
        assert list(queue.db.execute('SELECT * FROM discovery ORDER BY url')) == before_rows
    finally:
        queue.close()


def test_status_benchmark_rejects_old_owner_or_unbounded_repetitions(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        queue.status()
        for repetitions in (0, 2, 16):
            with pytest.raises(ValueError, match='bounded_repetition_count_required'):
                profile_current_status(tmp_path, queue.registry, repetitions)
        status = json.loads((tmp_path / 'status.json').read_text())
        status['owner_id'] = 'previous-owner'
        atomic_write_json(tmp_path / 'status.json', status)
        with pytest.raises(ValueError, match='status_database_owner_mismatch'):
            profile_current_status(tmp_path, queue.registry, 3)
    finally:
        queue.close()
