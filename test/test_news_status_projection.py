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


@pytest.mark.parametrize('comparison,reference', [('compare_discovery_summary', 'deepcopy'),
    ('compare_capacity_copy', 'deepcopy'), ('compare_cycle_publication', 'deepcopy'),
    ('compare_service_transition', 'deepcopy'),
    ('compare_capacity_copy', 'recursive'), ('compare_bulk_coverage_summary', 'deepcopy')])
def test_whole_status_benchmark_pins_owner_preserves_publication_and_source_rows(config, tmp_path, monkeypatch,
                                                                              comparison, reference):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with queue.db:
            add_discovery_cases(queue, 100)
            queue.observe_bulk_coverage('https://publisher.test/api?day=1', 'publisher', 101, 3, 'old-clock')
            queue.observe_bulk_coverage('https://publisher.test/api?day=2', 'publisher', 23, 1, 'new-clock')
        before_coverage = [tuple(row) for row in queue.db.execute('SELECT * FROM bulk_api_coverage ORDER BY base_url')]
        if comparison == 'compare_capacity_copy':
            queue.http_capacity_snapshot = lambda: {'providers': {}, 'calibration': {'providers': {
                'publisher': {'history': [{'new_stored_url_heads': 10**90, 'errors': [0, 3],
                                           'signed_zero': -0.0, 'clock': 1234.5}]}}}}
        queue.status(activity='fixture')
        before_status = sha256_file(tmp_path / 'status.json')
        before_settings = list(queue.db.execute('SELECT * FROM queue_settings ORDER BY key'))
        before_rows = list(queue.db.execute('SELECT * FROM discovery ORDER BY url'))
        changes = queue.db.total_changes

        def no_owner_or_http(*args, **kwargs):
            raise AssertionError('read-only replay must not start an acquisition owner')

        monkeypatch.setattr(NewsQueue, '__init__', no_owner_or_http)
        monkeypatch.setattr(NewsHTTP, '__init__', no_owner_or_http)
        result = profile_current_status(tmp_path, queue.registry, 3, **{comparison: True}, capacity_copy_reference=reference)
        assert result['accepted']
        assert result['publisher_requests'] == 0
        assert result['source_states_mutated'] is False
        assert result['status_published'] is False
        assert set(result['measurements']) == {'legacy', 'summary'}
        assert all(len(samples) == 3 for samples in result['measurements'].values())
        assert result['deterministic_projection_sha256']['legacy'] == result['deterministic_projection_sha256']['summary']
        if comparison == 'compare_capacity_copy':
            assert result['capacity_copy']['all_published_values_preserved']
            assert result['capacity_copy']['profiled_implementation'] == 'summary'
            assert result['capacity_copy']['reference_implementation'] == reference
        if comparison == 'compare_cycle_publication':
            assert result['cycle_publication']['publication_counts'] == {'legacy': [2] * 3, 'summary': [1] * 3}
            assert result['cycle_publication']['all_final_values_preserved']
            assert result['cycle_publication']['profiled_implementation'] == 'summary'
        if comparison == 'compare_service_transition':
            assert result['contract_version'] == 11
            assert result['service_transition']['publication_counts'] == {'legacy': [2] * 3, 'summary': [1] * 3}
            assert result['service_transition']['same_waiting_projection']
        if comparison == 'compare_bulk_coverage_summary':
            assert result['contract_version'] == 10
            assert result['bulk_coverage_verification']['live_transactional_summary_verified']
            assert result['bulk_coverage_verification']['coverage_scopes'] == 2
            statements = [item['statement'] for item in result['top_query_execution_times']]
            assert not any('FROM bulk_api_coverage c LEFT JOIN' in sql for sql in statements)
        assert len(result['top_query_execution_times']) <= 12
        assert sha256_file(tmp_path / 'status.json') == before_status
        assert queue.db.total_changes == changes
        assert list(queue.db.execute('SELECT * FROM queue_settings ORDER BY key')) == before_settings
        assert list(queue.db.execute('SELECT * FROM discovery ORDER BY url')) == before_rows
        assert [tuple(row) for row in queue.db.execute('SELECT * FROM bulk_api_coverage ORDER BY base_url')] == before_coverage
    finally:
        queue.close()


def test_status_benchmark_rejects_old_owner_or_unbounded_repetitions(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        queue.status()
        for repetitions in (0, 2, 16):
            with pytest.raises(ValueError, match='bounded_repetition_count_required'):
                profile_current_status(tmp_path, queue.registry, repetitions)
        with pytest.raises(ValueError, match='one_projection_comparison_per_snapshot_required'):
            profile_current_status(tmp_path, queue.registry, 3, compare_capacity_copy=True,
                                   compare_discovery_summary=True)
        status = json.loads((tmp_path / 'status.json').read_text())
        status['owner_id'] = 'previous-owner'
        atomic_write_json(tmp_path / 'status.json', status)
        with pytest.raises(ValueError, match='status_database_owner_mismatch'):
            profile_current_status(tmp_path, queue.registry, 3)
    finally:
        queue.close()


def test_bulk_projection_replay_seeds_only_temporary_legacy_state(config, tmp_path, monkeypatch):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with queue.db:
            queue.observe_bulk_coverage('https://publisher.test/api?day=1', 'publisher', 13, 1, 'clock')
            for row in queue.db.execute("SELECT name FROM sqlite_master WHERE type='trigger' "
                                        "AND (name LIKE 'bulk_coverage_summary_%' OR name LIKE 'bulk_split_summary_%')").fetchall():
                queue.db.execute('DROP TRIGGER '+row[0])
            queue.db.execute('DROP TABLE bulk_endpoint_metrics')
            queue.db.execute('DROP INDEX bulk_coverage_endpoint_observed')
            queue.db.execute("DELETE FROM queue_settings WHERE key='bulk_coverage_summary_version'")
        queue.status()
        before = [tuple(row) for row in queue.db.execute('SELECT type,name,sql FROM sqlite_master ORDER BY name')]
        before_hash = sha256_file(tmp_path / 'status.json')

        def forbidden(*args, **kwargs):
            raise AssertionError('read-only comparison must not initialize a writer or HTTP')

        monkeypatch.setattr(NewsQueue, '__init__', forbidden)
        monkeypatch.setattr(NewsHTTP, '__init__', forbidden)
        result = profile_current_status(tmp_path, queue.registry, 3, compare_bulk_coverage_summary=True)
        proof = result['bulk_coverage_verification']
        assert result['accepted'] and proof['seeded_in_temporary_table']
        assert proof['coverage_scopes'] == 1 and not proof['schema_migrated']
        assert before == [tuple(row) for row in queue.db.execute('SELECT type,name,sql FROM sqlite_master ORDER BY name')]
        assert sha256_file(tmp_path / 'status.json') == before_hash
    finally:
        queue.close()
