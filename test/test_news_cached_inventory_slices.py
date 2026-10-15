"""Bounded cache discovery is not acceptance; original validation stays mandatory."""
from dataclasses import replace
import os
from threading import Event

import pytest

from downloader.news_fulltext_policy import cnyes_bulk_fingerprint
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_replay import _inventory, ensure_replay_schema, replay_cached_cnyes
from test_cnyes_news_bulk import cfg, registry_http


def fake_envelopes(root, count):
    folder = root / 'quarantine'
    folder.mkdir(exist_ok=True)
    for number in range(count):
        (folder / f'{number:064x}.parquet').write_bytes(b'not proof of a source body')
    return folder


def test_small_slices_eventually_index_exact_same_set_without_whole_directory_or_false_completion(cfg, tmp_path):
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        fake_envelopes(tmp_path, 9)
        source = registry.sources['cnyes']
        for expected in range(1, 10):
            _inventory(queue, source, maximum_entries=1)
            assert queue.db.execute('SELECT count(*) FROM cached_bulk_replays').fetchone()[0] == expected
            assert not hasattr(queue, '_cached_cnyes_inventory_at')
        _inventory(queue, source, maximum_entries=1)
        assert queue._cached_cnyes_inventory_entries is None
        assert queue._cached_cnyes_inventory_fingerprint == cnyes_bulk_fingerprint(source)
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 0
    finally:
        queue.close()


def test_queue_close_and_restart_resume_discovery_not_reset_examined_checkpoints(cfg, tmp_path):
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    fake_envelopes(tmp_path, 4)
    _inventory(queue, registry.sources['cnyes'], maximum_entries=1)
    entries = queue._cached_cnyes_inventory_entries
    with queue.db:
        queue.db.execute("UPDATE cached_bulk_replays SET state='failed_validation',error_code='retained_bad_proof'")
    queue.close()
    assert list(entries) == []
    restarted = NewsQueue(tmp_path, registry)
    try:
        _inventory(restarted, registry.sources['cnyes'])
        assert restarted.db.execute('SELECT count(*) FROM cached_bulk_replays').fetchone()[0] == 4
        assert restarted.db.execute("SELECT count(*) FROM cached_bulk_replays WHERE state='failed_validation' AND error_code='retained_bad_proof'").fetchone()[0] == 1
    finally:
        restarted.close()


def test_stop_retains_unfinished_cursor_and_no_completed_hour_clock(cfg, tmp_path):
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        fake_envelopes(tmp_path, 3)
        _inventory(queue, registry.sources['cnyes'], maximum_entries=1)
        entries = queue._cached_cnyes_inventory_entries
        stop = Event()
        stop.set()
        _inventory(queue, registry.sources['cnyes'], stop=stop)
        assert queue._cached_cnyes_inventory_entries is entries
        assert queue.db.execute('SELECT count(*) FROM cached_bulk_replays').fetchone()[0] == 1
        assert not hasattr(queue, '_cached_cnyes_inventory_at')
        stop.clear()
        _inventory(queue, registry.sources['cnyes'], stop=stop)
        assert queue.db.execute('SELECT count(*) FROM cached_bulk_replays').fetchone()[0] == 3
    finally:
        queue.close()


def test_changed_api_fingerprint_closes_old_iterator_reexamines_exact_scope(cfg, tmp_path):
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        fake_envelopes(tmp_path, 3)
        source = registry.sources['cnyes']
        _inventory(queue, source, maximum_entries=1)
        old = queue._cached_cnyes_inventory_entries
        changed = replace(source, cnyes_bulk_data_fingerprint='changed-api-contract')
        _inventory(queue, changed)
        assert list(old) == []
        assert queue.db.execute('SELECT count(*) FROM cached_bulk_replays WHERE source_fingerprint=?',
            (cnyes_bulk_fingerprint(changed),)).fetchone()[0] == 3
    finally:
        queue.close()


def test_existing_exact_jobs_need_no_stat_or_redundant_writes(cfg, tmp_path, monkeypatch):
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        folder = fake_envelopes(tmp_path, 3)
        source = registry.sources['cnyes']
        ensure_replay_schema(queue)
        with queue.db:
            queue.db.executemany('INSERT INTO cached_bulk_replays(response_sha256,source_fingerprint,priority,state) '
                "VALUES (?,?,?,'checked')", [(f'{i:064x}', cnyes_bulk_fingerprint(source), -i) for i in range(3)])
        real = os.scandir
        class Entry:
            def __init__(self, entry): self.name = entry.name
            def is_symlink(self): return False
            def stat(self, **kwargs): raise AssertionError('known exact checkpoint must not stat its body')
        class Scan:
            def __init__(self):
                with real(folder) as entries: self.entries = iter([Entry(entry) for entry in entries])
            def __iter__(self): return self
            def __next__(self): return next(self.entries)
            def close(self): pass
        monkeypatch.setattr('downloader.news_fulltext_replay.os.scandir', lambda path: Scan())
        statements = []
        queue.db.set_trace_callback(statements.append)
        _inventory(queue, source)
        assert not any('INSERT OR IGNORE INTO cached_bulk_replays' in sql for sql in statements)
        assert [tuple(row) for row in queue.db.execute('SELECT priority,state FROM cached_bulk_replays ORDER BY response_sha256')] == [(0.0, 'checked'), (-1.0, 'checked'), (-2.0, 'checked')]
    finally:
        queue.close()


def test_unknown_symlink_names_and_invalid_envelopes_not_accepted_as_text(cfg, tmp_path):
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        folder = fake_envelopes(tmp_path, 1)
        (folder / ('f' * 64 + '.parquet')).symlink_to(folder / ('0' * 64 + '.parquet'))
        (folder / 'not-a-digest.parquet').write_bytes(b'not an envelope')
        assert replay_cached_cnyes(queue)['accepted_heads'] == 0
        assert queue.db.execute('SELECT count(*) FROM cached_bulk_replays').fetchone()[0] == 1
        assert queue.db.execute('SELECT count(*) FROM articles').fetchone()[0] == 0
        assert queue.db.execute('SELECT state FROM cached_bulk_replays').fetchone()[0] == 'failed_validation'
    finally:
        queue.close()


@pytest.mark.parametrize('kwargs', [
    {'maximum_entries': True}, {'maximum_entries': 0}, {'maximum_entries': 8193},
    {'maximum_wall_seconds': True}, {'maximum_wall_seconds': 0},
    {'maximum_wall_seconds': float('nan')}, {'maximum_wall_seconds': float('inf')},
    {'maximum_wall_seconds': 0.101},
])
def test_unbounded_or_untyped_inventory_switch_rejected(cfg, tmp_path, kwargs):
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        with pytest.raises(ValueError, match='unbounded cached inventory slice'):
            _inventory(queue, registry.sources['cnyes'], **kwargs)
    finally:
        queue.close()


def test_metadata_repair_uses_tiny_exact_predicate_index_and_preserves_other_values(cfg, tmp_path):
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        with queue.db:
            queue.add_articles([{'url': f'https://news.cnyes.com/news/id/{i}', 'date': '2026-10-01'} for i in range(1, 5)])
            queue.db.execute('UPDATE articles SET metadata_order_day=NULL WHERE url LIKE ? ', ('%/1',))
            queue.db.execute("UPDATE articles SET metadata_order_day=NULL,metadata_date_raw='unknown' WHERE url LIKE ?", ('%/2',))
        sql = "UPDATE articles SET metadata_order_day=substr(metadata_date_raw,1,10) WHERE host=? AND metadata_order_day IS NULL AND metadata_date_raw LIKE '____-__-__%'"
        plan = [row[3] for row in queue.db.execute('EXPLAIN QUERY PLAN ' + sql, ('news.cnyes.com',))]
        assert any('USING INDEX metadata_order_repair' in row for row in plan)
        before = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
        with queue.db: queue.db.execute(sql, ('news.cnyes.com',))
        after = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
        expected = {url: {**row, 'metadata_order_day': '2026-10-01'} if url.endswith('/1') else row
                    for url, row in before.items()}
        assert after == expected
    finally:
        queue.close()
