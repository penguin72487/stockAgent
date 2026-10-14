"""A fast heartbeat must count retained diagnostic rows, not unique URLs."""
import json
import sqlite3

import pytest

from downloader.artifact_io import sha256_file
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_status import bulk_item_error_count, initialize_bulk_error_count
from scripts.benchmark_news_status_projection import profile_current_status
from test_news_fulltext import config


def rows(count=9):
    return [(f'https://publisher.test/bulk/{i // 3}', i % 3, 'publisher',
             'https://publisher.test/same-article', 'bulk_body_not_provided',
             '2026-10-13T07:00:00+00:00') for i in range(count)]


def assert_exact(db, expected):
    assert bulk_item_error_count(db) == expected
    assert db.execute('SELECT count(*) FROM bulk_item_errors').fetchone()[0] == expected


def test_transactional_count_insert_ignore_update_retirement_replace_page_and_rollback(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        assert_exact(queue.db, 0)
        with queue.db:
            queue.db.executemany('INSERT INTO bulk_item_errors VALUES (?,?,?,?,?,?)', rows())
        assert_exact(queue.db, 9)
        # Same article in nine row observations is nine diagnostics, not one.
        with queue.db:
            queue.db.executemany('INSERT OR IGNORE INTO bulk_item_errors VALUES (?,?,?,?,?,?)', rows())
            queue.db.execute("UPDATE bulk_item_errors SET error_code='schema_error'")
        assert_exact(queue.db, 9)
        before = [tuple(row) for row in queue.db.execute('SELECT * FROM bulk_item_errors ORDER BY request_url,item_number')]
        queue.db.execute('DELETE FROM bulk_item_errors WHERE request_url=?', (rows()[0][0],))
        assert_exact(queue.db, 6)
        queue.db.executemany('INSERT INTO bulk_item_errors VALUES (?,?,?,?,?,?)', rows(2))
        assert_exact(queue.db, 8)
        queue.db.rollback()
        assert_exact(queue.db, 9)
        assert before == [tuple(row) for row in queue.db.execute('SELECT * FROM bulk_item_errors ORDER BY request_url,item_number')]
        # Use the actual owner contract: delete a request, then insert its
        # replacement page, in one transaction. No SQL REPLACE on source rows.
        with queue.db:
            queue.db.execute('DELETE FROM bulk_item_errors WHERE request_url=?', (rows()[0][0],))
            queue.db.executemany('INSERT INTO bulk_item_errors VALUES (?,?,?,?,?,?)', rows(2))
        assert_exact(queue.db, 8)
        with queue.db:
            queue.db.execute('DELETE FROM bulk_item_errors WHERE item_number=2')
        assert_exact(queue.db, 6)
        changes = queue.db.total_changes
        queue.status()
        assert queue.db.total_changes == changes
        status = json.loads((tmp_path/'status.json').read_bytes())
        assert status['bulk_item_errors'] == 6
        assert status['bulk_item_error_count_contract_version'] == 1
        assert status['status_query_timing_contract_version'] == 1
        stages = queue.metrics.snapshot()['stages']
        for name in ('status_original_source_inventory', 'status_metrics_snapshot',
                     'status_discovery_projection', 'status_bulk_item_error_count',
                     'status_http_capacity_snapshot', 'status_json_publication'):
            assert stages[name]['count'] == 1
    finally:
        queue.close()


@pytest.mark.parametrize('damage', ('table', 'marker', 'row'))
def test_derived_count_rebuilds_without_source_changes_or_repeated_scan(config, tmp_path, damage):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with queue.db:
            queue.db.executemany('INSERT INTO bulk_item_errors VALUES (?,?,?,?,?,?)', rows())
        before = [tuple(row) for row in queue.db.execute('SELECT * FROM bulk_item_errors ORDER BY request_url,item_number')]
        with queue.db:
            if damage == 'table':
                queue.db.execute('DROP TABLE bulk_item_error_metrics')
            elif damage == 'marker':
                queue.db.execute("DELETE FROM queue_settings WHERE key='bulk_item_error_count_version'")
            else:
                queue.db.execute('DELETE FROM bulk_item_error_metrics')
        initialize_bulk_error_count(queue.db)
        assert_exact(queue.db, 9)
        assert before == [tuple(row) for row in queue.db.execute('SELECT * FROM bulk_item_errors ORDER BY request_url,item_number')]
        statements = []
        queue.db.set_trace_callback(statements.append)
        initialize_bulk_error_count(queue.db)
        assert not any('count(*) FROM bulk_item_errors' in statement for statement in statements)
        assert_exact(queue.db, 9)
    finally:
        queue.close()


def test_legacy_read_only_snapshot_fallback_and_malformed_metric_fail_closed(tmp_path):
    target = tmp_path/'legacy.sqlite3'
    with sqlite3.connect(target) as db:
        db.execute('CREATE TABLE bulk_item_errors(id INTEGER PRIMARY KEY)')
        db.executemany('INSERT INTO bulk_item_errors VALUES (?)', [(1,), (2,)])
    with sqlite3.connect(f'file:{target}?mode=ro', uri=True) as db:
        assert bulk_item_error_count(db) == 2
        assert db.total_changes == 0
    with sqlite3.connect(target) as db:
        db.execute('CREATE TABLE bulk_item_error_metrics(singleton INTEGER PRIMARY KEY,rows INTEGER)')
        with pytest.raises(ValueError, match='invalid_bulk_item_error_count_metric'):
            bulk_item_error_count(db)
        db.execute('INSERT INTO bulk_item_error_metrics VALUES (1,-1)')
        with pytest.raises(ValueError, match='invalid_bulk_item_error_count_metric'):
            bulk_item_error_count(db)


def test_whole_status_counter_comparison_is_exact_read_only_and_does_not_publish(config, tmp_path, monkeypatch):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with queue.db:
            queue.db.executemany('INSERT INTO bulk_item_errors VALUES (?,?,?,?,?,?)', rows())
        queue.status(activity='fixture')
        original = sha256_file(tmp_path/'status.json')
        changes = queue.db.total_changes
        result = profile_current_status(tmp_path, queue.registry, 3, compare_bulk_error_summary=True)
        assert result['accepted'] and result['bulk_item_error_rows'] == 9
        assert result['live_incremental_bulk_error_state_verified']
        assert not result['bulk_error_summary_seeded_in_temporary_table']
        assert result['deterministic_projection_sha256']['legacy'] == result['deterministic_projection_sha256']['summary']
        assert result['publisher_requests'] == 0 and not result['source_states_mutated']
        assert not result['status_published']
        assert sha256_file(tmp_path/'status.json') == original and queue.db.total_changes == changes
        # Pre-migration read-only replay may build one TEMP row, not a main
        # schema, source mutation or claim of live trigger acceptance.
        with queue.db:
            queue.db.execute('DROP TABLE bulk_item_error_metrics')
        result = profile_current_status(tmp_path, queue.registry, 3, compare_bulk_error_summary=True)
        assert result['accepted'] and result['bulk_error_summary_seeded_in_temporary_table']
        assert not result['live_incremental_bulk_error_state_verified']
        assert_exact(queue.db, 9)
        assert not queue.db.execute("SELECT 1 FROM sqlite_master WHERE name='bulk_item_error_metrics'").fetchone()
        assert sha256_file(tmp_path/'status.json') == original
    finally:
        queue.close()
