"""List completeness needs actual IDs/dates, not a queued child or one page."""
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
import os
import shutil
import time
from threading import Event
from urllib.parse import parse_qs

import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
from downloader.news_fulltext import accept_discovery
from downloader.news_fulltext_bulk import cnyes_pagination, cnyes_records, cnyes_scope, cnyes_url, cnyes_window_partitions
from downloader.news_fulltext_cnyes_history import (
    CONTRACT_VERSION, backfill, index_children, record_page, progress, repair_incomplete_scopes,
    reconcile_legacy_request_mismatches, reconcile_legacy_unavailable, seed_overflow_retained,
)
from downloader.news_fulltext_queue import NewsQueue, _accepted_batch_frontier
from test_cnyes_news_bulk import BASE, cfg, payload, registry_http, response


@pytest.fixture
def queue(cfg, tmp_path):
    registry, _ = registry_http(cfg, tmp_path)
    value = NewsQueue(tmp_path, registry)
    yield value
    value.close()


def bounds(url):
    q = parse_qs(url.split('?', 1)[1])
    return int(q['startAt'][0]), int(q['endAt'][0])


def source_payload(ids, *, epoch, page=1, total=None):
    data = payload(ids=ids, page=page, total=total)
    for post in data['items']['data']:
        post['publishAt'] = epoch
    return data


def capture(queue, url, data):
    source = queue.registry.sources['cnyes']
    with queue.db:
        queue.db.execute("INSERT OR IGNORE INTO discovery(url,kind,provider) VALUES (?,'cnyes_news','cnyes')", (url,))
    r = response(data, url)
    records, errors = cnyes_records(r, type('HTTP', (), {'registry': queue.registry})(), source)
    links = [(row['requested_url'], row['title'], row['metadata_date_raw']) for row in records]
    links.extend((row['url'], row.get('title', ''), row.get('metadata_date_raw', '')) for row in errors
                 if row['error'] not in {'bulk_post_not_public', 'bulk_exact_duplicate_row'})
    task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    accept_discovery(queue, task, r, links, records, errors)
    return r


def head(queue, url):
    return dict(queue.db.execute('SELECT * FROM cnyes_history_scope_heads WHERE scope_url=?', (cnyes_scope(url),)).fetchone())


def seed_partition(queue, total=4):
    parent = cnyes_url(BASE, start=1388534400, end=1388620799)
    children = cnyes_window_partitions(parent, 31)
    scope = cnyes_scope(parent)
    with queue.db:
        queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'cnyes',?,31,'original-parent-source-clock')", (scope, total))
        queue.db.execute("INSERT INTO bulk_window_gaps VALUES (?,'cnyes','cnyes_single_day_page_cap','original-gap-clock')", (scope,))
        queue.db.execute("INSERT INTO bulk_window_splits VALUES (?,'cnyes',?,'original-split-clock')", (scope, json.dumps(children)))
        index_children(queue, scope, 'cnyes', children)
    return parent, children


def test_accepted_batch_frontier_projects_bounded_registered_work_in_owner_status(queue):
    from downloader.news_fulltext_cnyes_history import accepted_batch_scan_key

    key = accepted_batch_scan_key(queue.registry.sources['cnyes'])
    with queue.db:
        queue.db.executemany('INSERT INTO accepted_batches(path,receipt_path) VALUES (?,?)',
                             [(path, path + '.receipt') for path in ('a', 'b', 'c', 'd', 'z')])
    queue.add_articles([{'url': 'https://news.cnyes.com/news/id/987654321', 'title': 'pending', 'date': None}])
    initial = queue.status(publish=False)['providers']['cnyes']['history_list_progress']['accepted_body_batch_scan']
    assert initial['scan_state'] == 'not_started'
    assert initial['remaining_frontier_batches'] == initial['estimated_frontier_batches'] == 5

    checkpoint = {'version': 1, 'state': 'scanning', 'path': 'b', 'end_path': 'd',
                  'checked': 2, 'indexed': 7, 'attempts': 0, 'retry_after': 0}
    with queue.db:
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (key, json.dumps(checkpoint)))
    current = queue.status(publish=False)['providers']['cnyes']['history_list_progress']['accepted_body_batch_scan']
    assert current['scanned_batches'] == 2 and current['indexed_candidates'] == 7
    assert current['remaining_frontier_batches'] == 2 and current['estimated_frontier_batches'] == 4
    assert current['all_history_complete'] is False and current['publisher_requests'] == 0

    # A new registration inside the frozen end changes the estimate; z remains outside it.
    with queue.db:
        queue.db.execute('INSERT INTO accepted_batches VALUES (?,?)', ('c1', 'c1.receipt'))
    updated = queue.status(publish=False)['providers']['cnyes']['history_list_progress']['accepted_body_batch_scan']
    assert updated['remaining_frontier_batches'] == 3 and updated['estimated_frontier_batches'] == 5


def test_accepted_batch_frontier_rejects_corrupt_or_changed_checkpoints(queue):
    from downloader.news_fulltext_cnyes_history import accepted_batch_scan_key

    key = accepted_batch_scan_key(queue.registry.sources['cnyes'])
    accepted = {'scan_state': 'scanning', 'scanned_batches': 1, 'indexed_candidates': 0}
    checkpoint = {'version': 1, 'state': 'scanning', 'path': 'a', 'end_path': 'z',
                  'checked': 1, 'indexed': 0, 'attempts': 0, 'retry_after': 0}
    with queue.db:
        queue.db.execute('INSERT INTO accepted_batches VALUES (?,?)', ('b', 'b.receipt'))
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (key, json.dumps(checkpoint)))
    expected = {'remaining_frontier_batches': 1, 'estimated_frontier_batches': 2}
    unknown = {'remaining_frontier_batches': None, 'estimated_frontier_batches': None}
    assert _accepted_batch_frontier(queue.db, key, accepted) == expected
    assert _accepted_batch_frontier(queue.db, key, {**accepted, 'scanned_batches': 2}) == unknown

    class ChangedDuringCount:
        def execute(self, statement, args=()):
            cursor = queue.db.execute(statement, args)
            if statement.startswith('SELECT count(*) FROM accepted_batches'):
                changed = {**checkpoint, 'checked': 2}
                queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps(changed), key))
            return cursor

    assert _accepted_batch_frontier(ChangedDuringCount(), key, accepted) == unknown
    with queue.db:
        checkpoint['attempts'] = True  # bool is not a valid integer checkpoint.
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps(checkpoint), key))
    assert _accepted_batch_frontier(queue.db, key, accepted) == unknown


def test_owner_initializes_legacy_missing_history_tables_before_frontier_status(queue):
    queue.add_articles([{'url': 'https://news.cnyes.com/news/id/987654321', 'title': 'pending', 'date': None}])
    old_schema_missing = (
        'cnyes_history_accepted_batch_candidates', 'cnyes_history_accepted_batch_outcomes',
        'cnyes_history_accepted_batch_local_retries', 'cnyes_history_accepted_missing_retries',
        'cnyes_history_accepted_invalid_retries', 'cnyes_history_retained_local_retries',
        'cnyes_history_retained_revision_audit', 'cnyes_history_retained_outcome_resolutions',
    )
    with queue.db:
        for table in old_schema_missing:
            queue.db.execute(f'DROP TABLE {table}')
    root, registry = queue.root, queue.registry
    queue.close()
    with closing(NewsQueue(root, registry)) as resumed:
        tables = {row[0] for row in resumed.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert set(old_schema_missing) <= tables
        scan = resumed.status(publish=False)['providers']['cnyes']['history_list_progress']['accepted_body_batch_scan']
        assert scan['scan_state'] == 'not_started'
        assert scan['scanned_batches'] == scan['remaining_frontier_batches'] == scan['estimated_frontier_batches'] == 0
        assert scan['all_history_complete'] is False and scan['publisher_requests'] == 0


def test_periodic_integrity_sweep_discovers_corrupt_leaf_and_keeps_source_429(queue):
    from downloader import news_fulltext_cnyes_history as history
    parent, children = seed_partition(queue)
    for number, child in enumerate(children):
        capture(queue, child, source_payload([number * 2 + 1, number * 2 + 2], epoch=bounds(child)[0]))
    assert head(queue, parent)['state'] == 'partition_list_complete'
    scope = cnyes_scope(children[0])
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_page_observations SET observation_json='broken' WHERE scope_url=?", (scope,))
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9000000000 WHERE url=?", (children[0],))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (children[0],)).fetchone())
    result = history.sweep_completed_scopes(queue, limit=100, max_seconds=10, now_epoch=1000)
    assert result['complete'] and result['invalid'] >= 1
    assert head(queue, children[0])['state'] == 'invalid_page_evidence'
    assert head(queue, parent)['state'] not in history.COMPLETE_STATES
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (children[0],)).fetchone()) == before
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_integrity_incidents').fetchone()[0] >= 1


def test_periodic_integrity_sweep_rebuilds_corrupt_proof_from_valid_local_evidence(queue):
    from downloader import news_fulltext_cnyes_history as history
    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([1, 2], epoch=bounds(url)[0]))
    prior_sha = head(queue, url)['proof_sha256']
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_scope_proofs SET proof_json='broken' WHERE proof_sha256=?", (prior_sha,))
    result = history.sweep_completed_scopes(queue, limit=100, max_seconds=10, now_epoch=1000)
    assert result['complete'] and result['invalid'] == 1
    assert head(queue, url)['state'] == 'list_complete'
    assert history._verified_partition_scope_proof(queue, cnyes_scope(url), queue.registry.sources['cnyes'])
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_corrupt_proof_audit').fetchone()[0] == 1


def test_integrity_sweep_retains_binary_corrupt_proof_before_rebuilding(queue):
    from downloader import news_fulltext_cnyes_history as history
    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([1], epoch=bounds(url)[0]))
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_scope_proofs SET proof_json=? WHERE proof_sha256=?',
                         (b'\xff\x00', head(queue, url)['proof_sha256']))
    result = history.sweep_completed_scopes(queue, limit=100, max_seconds=10, now_epoch=1000)
    assert result['invalid'] == 1 and head(queue, url)['state'] == 'list_complete'
    saved = json.loads(queue.db.execute('SELECT original_row_json FROM cnyes_history_corrupt_proof_audit').fetchone()[0])
    assert saved['proof_json'] == {'sqlite_type': 'blob', 'hex': 'ff00'}


def test_periodic_integrity_sweep_resumes_and_revisits_later_mutations(queue):
    from downloader import news_fulltext_cnyes_history as history
    urls = [cnyes_url(BASE, start=1388534400 + i * 100, end=1388534499 + i * 100) for i in range(3)]
    for i, url in enumerate(urls):
        capture(queue, url, source_payload([i + 1], epoch=bounds(url)[0]))
    first = history.sweep_completed_scopes(queue, limit=1, max_seconds=10, now_epoch=1000)
    assert not first['complete'] and first['checked'] == 1
    key = history.integrity_scan_key(queue.registry.sources['cnyes'])
    checkpoint = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0]
    stopped = Event()
    stopped.set()
    assert history.sweep_completed_scopes(queue, limit=10, max_seconds=10, now_epoch=1000, stop=stopped)['checked'] == 0
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0] == checkpoint
    scope = json.loads(checkpoint)['cursor']
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_page_observations SET observation_json='broken' WHERE scope_url=?", (scope,))
    assert history.sweep_completed_scopes(queue, limit=100, max_seconds=10, now_epoch=1000)['complete']
    assert queue.db.execute('SELECT state FROM cnyes_history_scope_heads WHERE scope_url=?', (scope,)).fetchone()[0] == 'list_complete'
    waiting = history.sweep_completed_scopes(queue, limit=100, max_seconds=10, now_epoch=1001)
    assert waiting['complete'] and waiting['checked'] == 0
    later = history.sweep_completed_scopes(queue, limit=100, max_seconds=10, now_epoch=1000 + history.INTEGRITY_INTERVAL_SECONDS)
    assert later['invalid'] >= 1
    assert queue.db.execute('SELECT state FROM cnyes_history_scope_heads WHERE scope_url=?', (scope,)).fetchone()[0] == 'invalid_page_evidence'


def test_integrity_scan_repairs_from_normal_owner_and_publishes_only_aggregate_counts(queue):
    from downloader import news_fulltext_cnyes_history as history
    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([1], epoch=bounds(url)[0]))
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_scope_proofs SET proof_json='broken' WHERE proof_sha256=?",
                         (head(queue, url)['proof_sha256'],))
    result = backfill(queue, limit=100, max_seconds=10)
    assert result['completed_integrity_scan']['checked'] >= 1
    assert result['completed_integrity_scan']['invalid'] == 1
    assert head(queue, url)['state'] == 'list_complete'
    scan = progress(queue.db, queue.registry.sources['cnyes'])['completed_integrity_scan']
    assert scan['scan_state'] == 'complete' and scan['checked_scopes'] >= 1 and scan['affected_scopes'] == 1
    assert scan['local_metadata_only'] and scan['publisher_requests'] == 0
    assert url not in json.dumps(scan)


@pytest.mark.parametrize(('bad', 'expected'), [
    ('broken-checkpoint', 'broken-checkpoint'),
    (b'\xff\x00', '{"value":{"hex":"ff00","sqlite_type":"blob"}}'),
])
def test_integrity_scan_preserves_bad_checkpoint_and_starts_again(queue, bad, expected):
    from downloader import news_fulltext_cnyes_history as history
    key = history.integrity_scan_key(queue.registry.sources['cnyes'])
    with queue.db:
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (key, bad))
    result = history.sweep_completed_scopes(queue, limit=10, max_seconds=10, now_epoch=1000)
    assert result['complete']
    assert queue.db.execute('SELECT original_value FROM cnyes_history_integrity_checkpoint_audit').fetchone()[0] == expected


def test_integrity_scan_quarantines_multiple_bad_siblings_before_rebuilding_parent(queue):
    from downloader import news_fulltext_cnyes_history as history
    parent, children = seed_partition(queue)
    for number, child in enumerate(children):
        capture(queue, child, source_payload([number * 2 + 1, number * 2 + 2], epoch=bounds(child)[0]))
    with queue.db:
        for child in children:
            queue.db.execute("UPDATE cnyes_history_page_observations SET observation_json='broken' WHERE scope_url=?",
                             (cnyes_scope(child),))
    result = history.sweep_completed_scopes(queue, limit=100, max_seconds=10, now_epoch=1000)
    assert result['invalid'] >= 2
    assert all(head(queue, child)['state'] == 'invalid_page_evidence' for child in children)
    assert head(queue, parent)['state'] not in history.COMPLETE_STATES


def test_integrity_scan_never_relabels_an_old_contract_child(queue):
    from downloader import news_fulltext_cnyes_history as history
    parent, children = seed_partition(queue)
    for number, child in enumerate(children):
        capture(queue, child, source_payload([number * 2 + 1, number * 2 + 2], epoch=bounds(child)[0]))
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_scope_heads SET authorization_fingerprint='old-authority' WHERE scope_url=?",
                         (cnyes_scope(children[0]),))
    old = head(queue, children[0])
    history.sweep_completed_scopes(queue, limit=100, max_seconds=10, now_epoch=1000)
    assert head(queue, children[0]) == old
    assert head(queue, parent)['state'] not in history.COMPLETE_STATES


def test_integrity_scan_keeps_preexisting_source_gap_and_clock(queue):
    from downloader import news_fulltext_cnyes_history as history
    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([1], epoch=bounds(url)[0]))
    scope = cnyes_scope(url)
    with queue.db:
        queue.db.execute("INSERT INTO bulk_window_gaps VALUES (?,'cnyes','source_conflict','original-clock')", (scope,))
    original = tuple(queue.db.execute('SELECT * FROM bulk_window_gaps WHERE scope_url=?', (scope,)).fetchone())
    history.sweep_completed_scopes(queue, limit=100, max_seconds=10, now_epoch=1000)
    assert head(queue, url)['state'] == 'invalid_scope_evidence'
    assert tuple(queue.db.execute('SELECT * FROM bulk_window_gaps WHERE scope_url=?', (scope,)).fetchone()) == original


def test_integrity_scan_stop_mid_scope_rolls_back_cursor_and_incidents(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history
    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([1], epoch=bounds(url)[0]))
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_page_observations SET observation_json='broken' WHERE scope_url=?",
                         (cnyes_scope(url),))
    original = head(queue, url)
    stopped = Event()
    real = history._integrity_quarantine
    def interrupt(*args, **kwargs):
        result = real(*args, **kwargs)
        stopped.set()
        return result
    monkeypatch.setattr(history, '_integrity_quarantine', interrupt)
    result = history.sweep_completed_scopes(queue, limit=100, max_seconds=10, now_epoch=1000, stop=stopped)
    assert result['checked'] == result['invalid'] == 0 and not result['complete']
    assert head(queue, url) == original
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_integrity_incidents').fetchone()[0] == 0
    checkpoint = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
        (history.integrity_scan_key(queue.registry.sources['cnyes']),)).fetchone()[0])
    assert checkpoint['cursor'] == '' and checkpoint['checked'] == checkpoint['invalid'] == 0


def test_integrity_scan_checkpoint_survives_owner_restart(cfg, tmp_path):
    from downloader import news_fulltext_cnyes_history as history
    registry, _ = registry_http(cfg, tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as first:
        urls = [cnyes_url(BASE, start=1388534400 + i * 100, end=1388534499 + i * 100) for i in range(3)]
        for i, url in enumerate(urls):
            capture(first, url, source_payload([i + 1], epoch=bounds(url)[0]))
        result = history.sweep_completed_scopes(first, limit=1, max_seconds=10, now_epoch=1000)
        assert result['checked'] == 1 and not result['complete']
    with closing(NewsQueue(tmp_path, registry)) as resumed:
        result = history.sweep_completed_scopes(resumed, limit=100, max_seconds=10, now_epoch=1001)
        assert result['checked'] == 2 and result['complete']
        scan = progress(resumed.db, registry.sources['cnyes'])['completed_integrity_scan']
        assert scan['checked_scopes'] == 3 and scan['affected_scopes'] == 0


def test_integrity_scan_rolls_back_prior_repairs_and_checkpoint_on_write_failure(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history
    urls = [cnyes_url(BASE, start=1388534400 + i * 100, end=1388534499 + i * 100) for i in range(2)]
    for i, url in enumerate(urls):
        capture(queue, url, source_payload([i + 1], epoch=bounds(url)[0]))
    with queue.db:
        for url in urls:
            queue.db.execute("UPDATE cnyes_history_scope_proofs SET proof_json='broken' WHERE proof_sha256=?",
                             (head(queue, url)['proof_sha256'],))
    before = [head(queue, url) for url in urls]
    real = history._repair_integrity_component
    calls = []
    def fail_after_second(*args, **kwargs):
        result = real(*args, **kwargs)
        calls.append(args[1])
        if len(calls) == 2:
            raise OSError('simulated local write failure')
        return result
    monkeypatch.setattr(history, '_repair_integrity_component', fail_after_second)
    with pytest.raises(OSError, match='simulated local write failure'):
        history.sweep_completed_scopes(queue, limit=100, max_seconds=10, now_epoch=1000)
    assert len(calls) == 2 and [head(queue, url) for url in urls] == before
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_integrity_incidents').fetchone()[0] == 0
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_corrupt_proof_audit').fetchone()[0] == 0
    assert queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?',
        (history.integrity_scan_key(queue.registry.sources['cnyes']),)).fetchone() is None


def test_integrity_memo_does_not_turn_a_stale_parent_reference_into_a_bad_child(queue):
    from downloader import news_fulltext_cnyes_history as history
    parent, children = seed_partition(queue)
    for number, child in enumerate(children):
        capture(queue, child, source_payload([number * 2 + 1, number * 2 + 2], epoch=bounds(child)[0]))
    old_sha = head(queue, parent)['proof_sha256']
    proof = json.loads(queue.db.execute('SELECT proof_json FROM cnyes_history_scope_proofs WHERE proof_sha256=?',
                                        (old_sha,)).fetchone()[0])
    proof['facts']['children'][0]['proof_sha256'] = 'f' * 64
    new_sha = history.digest(proof)
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_scope_proofs SET proof_sha256=?,proof_json=? WHERE proof_sha256=?',
                         (new_sha, history.encoded(proof), old_sha))
        queue.db.execute('UPDATE cnyes_history_scope_heads SET proof_sha256=? WHERE scope_url=?',
                         (new_sha, cnyes_scope(parent)))
    memo, source = {}, queue.registry.sources['cnyes']
    assert history._verified_partition_scope_proof(queue, cnyes_scope(parent), source, _memo=memo) is None
    assert history._verified_partition_scope_proof(queue, cnyes_scope(children[0]), source, _memo=memo)


def test_integrity_scan_uses_current_contract_index(queue):
    from downloader import news_fulltext_cnyes_history as history
    source = queue.registry.sources['cnyes']
    prefix = (source.provider, history.CONTRACT_VERSION, history.cnyes_bulk_fingerprint(source), source.authorization_fingerprint)
    params = tuple(value for state in history.INTEGRITY_STATES for value in (*prefix, state, '', 'z')) + (4,)
    plan = [row[3] for row in queue.db.execute('EXPLAIN QUERY PLAN ' + history.INTEGRITY_SCOPE_SEEK, params)]
    assert any('cnyes_history_current_contract' in line for line in plan)
    assert not any('USE TEMP B-TREE' in line for line in plan)


def test_actual_all_pages_then_parent_ID_day_proof_and_preserved_original_gap(queue):
    parent, children = seed_partition(queue, total=33)
    left, right = children
    capture(queue, left, source_payload(range(1, 31), epoch=bounds(left)[0], total=31))
    assert head(queue, left)['state'] == 'waiting_page_evidence'
    assert queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(parent),)).fetchone()
    capture(queue, cnyes_url(BASE, 2, start=bounds(left)[0], end=bounds(left)[1]),
            source_payload([31], epoch=bounds(left)[0], page=2, total=31))
    assert head(queue, left)['state'] == 'list_complete'
    assert queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(parent),)).fetchone()
    capture(queue, right, source_payload([32, 33], epoch=bounds(right)[0]))
    assert head(queue, parent)['state'] == 'partition_list_complete'
    retirement = queue.db.execute('SELECT * FROM cnyes_history_gap_retirements WHERE scope_url=?', (cnyes_scope(parent),)).fetchone()
    assert json.loads(retirement['gap_json'])['observed_at_utc'] == 'original-gap-clock'
    assert queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(parent),)).fetchone() is None
    proof = json.loads(queue.db.execute('SELECT proof_json FROM cnyes_history_scope_proofs WHERE proof_sha256=?',
                                       (retirement['proof_sha256'],)).fetchone()[0])
    assert proof['unique_article_IDs'] == 33 and proof['body_completeness_verified'] is False
    assert proof['historical_point_in_time'] is False


@pytest.mark.parametrize('conflict', ['parent_total', 'overlapping_ID', 'source_date'])
def test_real_partition_conflict_keeps_source_gap(queue, conflict):
    parent, children = seed_partition(queue, total=5 if conflict == 'parent_total' else 4)
    capture(queue, children[0], source_payload([1, 2], epoch=bounds(children[0])[0]))
    ids = [2, 3] if conflict == 'overlapping_ID' else [3, 4]
    epoch = bounds(children[0])[0] if conflict == 'source_date' else bounds(children[1])[0]
    capture(queue, children[1], source_payload(ids, epoch=epoch))
    assert queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(parent),)).fetchone()
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_gap_retirements').fetchone()[0] == 0


def test_completed_parent_reopens_on_forward_changed_child_source(queue):
    parent, children = seed_partition(queue)
    capture(queue, children[0], source_payload([1, 2], epoch=bounds(children[0])[0]))
    capture(queue, children[1], source_payload([3, 4], epoch=bounds(children[1])[0]))
    original_proof = head(queue, parent)['proof_sha256']
    capture(queue, children[1], source_payload([3, 4, 5], epoch=bounds(children[1])[0]))
    assert head(queue, parent)['state'] == 'inconsistent_partition_evidence'
    assert queue.db.execute('SELECT 1 FROM cnyes_history_scope_proofs WHERE proof_sha256=?', (original_proof,)).fetchone()
    assert queue.db.execute('SELECT error_code FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(parent),)).fetchone()[0] == 'cnyes_source_list_evidence_changed'


def test_forward_source_totals_and_post_conflicts_cannot_certify_multi_page_list(queue):
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    capture(queue, url, source_payload(range(1, 31), epoch=low, total=31))
    second = cnyes_url(BASE, 2, start=low, end=low + 100)
    capture(queue, second, source_payload([31, 32], epoch=low, page=2, total=32))
    assert head(queue, url)['state'] == 'inconsistent_source_snapshot'
    assert progress(queue.db)['all_history_complete'] is False


def test_empty_source_window_has_its_own_exact_metadata_evidence(queue):
    url = cnyes_url(BASE, start=1388534400, end=1388534401)
    data = source_payload([], epoch=1388534400)
    data['items']['last_page'] = 0
    capture(queue, url, data)
    assert head(queue, url)['state'] == 'list_complete'
    assert json.loads(head(queue, url)['member_ids_json']) == []
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 1
    from downloader.news_fulltext_cnyes_history import _verified_partition_scope_proof
    assert _verified_partition_scope_proof(queue, cnyes_scope(url), queue.registry.sources['cnyes'])


def test_single_true_source_day_overflow_never_becomes_complete(queue):
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    capture(queue, url, source_payload(range(1, 31), epoch=low, total=1000))
    assert queue.db.execute('SELECT error_code FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(url),)).fetchone()[0] == 'cnyes_single_day_page_cap'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_scope_heads').fetchone()[0] == 0


def seed_equivalent(queue, *, change_page2=False):
    low = 1388534400
    exemplar = cnyes_url(BASE, start=low, end=low + 100)
    alias = cnyes_url(BASE, start=low + 101, end=low + 300)
    with queue.db:
        from downloader.news_fulltext_cnyes_history import index_scope
        index_scope(queue, cnyes_scope(alias), 'cnyes')
        queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'cnyes',31,2,'old-alias-clock')", (cnyes_scope(alias),))
        for page, ids in [(1, range(1, 31)), (2, [32] if change_page2 else [31])]:
            page_url = cnyes_url(BASE, page, start=low + 101, end=low + 300)
            info, _ = cnyes_pagination(response(source_payload(ids, epoch=low, page=page, total=31),
                page_url))
            queue.db.execute('INSERT INTO bulk_page_evidence VALUES (?,?,?,?,?)',
                             (cnyes_scope(alias), page, 31, info['ids_sha256'], 'real-alias-page-clock'))
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due) VALUES (?,'cnyes_news','cnyes',9000000000)", (page_url,))
    return exemplar, alias


@pytest.mark.parametrize('change_page2', [False, True])
def test_effective_dates_first_page_not_enough_all_ID_hashes_required_no_clock_clone(queue, change_page2):
    exemplar, alias = seed_equivalent(queue, change_page2=change_page2)
    capture(queue, exemplar, source_payload(range(1, 31), epoch=bounds(exemplar)[0], total=31))
    assert head(queue, alias)['state'] == 'waiting_page_evidence'
    capture(queue, cnyes_url(BASE, 2, start=bounds(exemplar)[0], end=bounds(exemplar)[1]),
            source_payload([31], epoch=bounds(exemplar)[0], page=2, total=31))
    if change_page2:
        assert head(queue, alias)['state'] == 'waiting_original_page_recheck'
        direct = cnyes_url(BASE, 2, start=bounds(alias)[0], end=bounds(alias)[1])
        assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (direct,)).fetchone()[0] < 9000000000
    else:
        assert head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence'
        assert json.loads(head(queue, alias)['member_ids_json']) == list(range(1, 32))
        assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_ID_references WHERE scope_url=?', (cnyes_scope(alias),)).fetchone()[0] == 2
    assert queue.db.execute('SELECT observed_at_utc FROM bulk_api_coverage WHERE base_url=?', (cnyes_scope(alias),)).fetchone()[0] == 'old-alias-clock'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations WHERE scope_url=?', (cnyes_scope(alias),)).fetchone()[0] == 0


def test_repeated_source_bytes_keep_first_observation_and_old_retained_cannot_replace_forward(queue):
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    r = capture(queue, url, source_payload([1, 2], epoch=low))
    source = queue.registry.sources['cnyes']
    info, posts = cnyes_pagination(r)
    before = tuple(queue.db.execute('SELECT * FROM cnyes_history_page_observations').fetchone())
    with queue.db:
        record_page(queue, r, info, posts, source, {'kind': 'actual-retained-reference'},
                    observed_at='2019-01-01T00:00:00+00:00', origin='verified_retained_source_response')
    assert tuple(queue.db.execute('SELECT * FROM cnyes_history_page_observations').fetchone()) == before
    assert queue.db.execute('SELECT last_confirmed_at_utc FROM cnyes_history_page_heads').fetchone()[0].startswith('2026-')


@pytest.mark.parametrize('bad_json', ['{}', '{invalid'])
def test_same_source_response_recovers_corrupt_observation_with_preserved_evidence(queue, bad_json):
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    data = source_payload([1, 2], epoch=low)
    capture(queue, url, data)
    old = dict(queue.db.execute('SELECT * FROM cnyes_history_page_observations').fetchone())
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_page_observations SET observation_json=?', (bad_json,))
    capture(queue, url, data)
    row = queue.db.execute('SELECT o.* FROM cnyes_history_page_heads h '
        'JOIN cnyes_history_page_observations o ON o.observation_sha256=h.observation_sha256 '
        'WHERE h.scope_url=? AND h.page=1', (cnyes_scope(url),)).fetchone()
    from downloader.news_fulltext_cnyes_history import digest
    assert digest(json.loads(row['observation_json'])) == row['observation_sha256']
    assert json.loads(row['observation_json'])['request_url'] == url
    assert head(queue, url)['state'] == 'list_complete'
    archived = queue.db.execute('SELECT * FROM cnyes_history_corrupt_observation_audit').fetchall()
    assert len(archived) == 1
    damaged = json.loads(archived[0]['original_row_json'])
    assert damaged == {**old, 'observation_json': bad_json}


def test_damaged_other_page_does_not_claim_complete_or_abort_new_observation(queue):
    low = 1388534400
    first = cnyes_url(BASE, start=low, end=low + 100)
    second = cnyes_url(BASE, 2, start=low, end=low + 100)
    capture(queue, first, source_payload(range(1, 31), epoch=low, total=31))
    capture(queue, second, source_payload([31], epoch=low, page=2, total=31))
    assert head(queue, first)['state'] == 'list_complete'
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_page_observations SET observation_json=? WHERE page=1', ('{}',))
    capture(queue, second, source_payload([32], epoch=low, page=2, total=31))
    assert head(queue, first)['state'] not in ('list_complete', 'list_complete_by_page_ID_equivalence')
    assert queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(first),)).fetchone()


def test_corrupt_child_downgrades_parent_without_resetting_source_429(queue):
    from downloader.news_fulltext_cnyes_history import reconcile_scope

    parent, children = seed_partition(queue)
    capture(queue, children[0], source_payload([1, 2], epoch=bounds(children[0])[0]))
    capture(queue, children[1], source_payload([3, 4], epoch=bounds(children[1])[0]))
    assert head(queue, parent)['state'] == 'partition_list_complete'
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9000000000 WHERE url=?", (children[0],))
        queue.db.execute('UPDATE cnyes_history_page_observations SET observation_json=? WHERE scope_url=?',
                         ('{}', cnyes_scope(children[0])))
        prior_job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (children[0],)).fetchone())
        reconcile_scope(queue, cnyes_scope(children[0]), queue.registry.sources['cnyes'])
    assert head(queue, children[0])['state'] == 'invalid_page_evidence'
    assert head(queue, parent)['state'] == 'waiting_child_evidence'
    assert queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(parent),)).fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (children[0],)).fetchone()) == prior_job


def test_invalid_leaf_reopens_all_completed_ancestor_partitions(queue):
    from downloader.news_fulltext_cnyes_history import reconcile_scope

    low = 1388534400
    root = cnyes_url(BASE, start=low, end=low + 3 * 86400 - 1)
    left, right = cnyes_window_partitions(root, 31)
    leaves = cnyes_window_partitions(left, 31)
    assert len(leaves) == 2
    with queue.db:
        for url, total, children in [(root, 3, [left, right]), (left, 2, leaves)]:
            scope = cnyes_scope(url)
            queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'cnyes',?,31,'source-clock')", (scope, total))
            queue.db.execute("INSERT INTO bulk_window_gaps VALUES (?,'cnyes','cap','source-clock')", (scope,))
            queue.db.execute("INSERT INTO bulk_window_splits VALUES (?,'cnyes',?,'source-clock')", (scope, json.dumps(children)))
            index_children(queue, scope, 'cnyes', children)
    for number, url in enumerate([*leaves, right], 1):
        capture(queue, url, source_payload([number], epoch=bounds(url)[0]))
    assert head(queue, root)['state'] == 'partition_list_complete'
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_page_observations SET observation_json=? WHERE scope_url=?',
                         ('{}', cnyes_scope(leaves[0])))
        reconcile_scope(queue, cnyes_scope(leaves[0]), queue.registry.sources['cnyes'])
    assert head(queue, left)['state'] == 'waiting_child_evidence'
    assert head(queue, root)['state'] == 'waiting_child_evidence'
    assert queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(root),)).fetchone()
    capture(queue, leaves[0], source_payload([1], epoch=bounds(leaves[0])[0]))
    assert head(queue, leaves[0])['state'] == 'list_complete'
    assert head(queue, left)['state'] == 'partition_list_complete'
    assert head(queue, root)['state'] == 'partition_list_complete'
    assert queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(root),)).fetchone() is None


def test_corrupt_shared_observation_revalidates_each_alias_from_its_original_receipt(queue):
    exemplar, alias = seed_equivalent(queue)
    first = source_payload(range(1, 31), epoch=bounds(exemplar)[0], total=31)
    capture(queue, exemplar, first)
    capture(queue, cnyes_url(BASE, 2, start=bounds(exemplar)[0], end=bounds(exemplar)[1]),
            source_payload([31], epoch=bounds(exemplar)[0], page=2, total=31))
    assert head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence'
    old_sha = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads '
                               'WHERE scope_url=? AND page=1', (cnyes_scope(exemplar),)).fetchone()[0]
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_page_observations SET observation_json=? '
                         'WHERE observation_sha256=?', ('{}', old_sha))
    capture(queue, exemplar, first)
    assert head(queue, exemplar)['state'] == 'list_complete'
    assert head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence'
    old_heads = json.loads(queue.db.execute('SELECT original_heads_json FROM '
        'cnyes_history_corrupt_observation_audit').fetchone()[0])
    assert {row['scope_url'] for row in old_heads} == {cnyes_scope(exemplar), cnyes_scope(alias)}
    new_sha = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads '
                               'WHERE scope_url=? AND page=1', (cnyes_scope(alias),)).fetchone()[0]
    assert new_sha != old_sha
    assert queue.db.execute('SELECT 1 FROM cnyes_history_page_ID_references '
        'WHERE scope_url=? AND page=1 AND observation_sha256=?', (cnyes_scope(alias), new_sha)).fetchone()


def test_older_retained_response_does_not_replace_a_corrupt_forward_head(queue):
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    r = capture(queue, url, source_payload([1, 2], epoch=low))
    info, posts = cnyes_pagination(r)
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_page_observations SET observation_json=?', ('{}',))
        record_page(queue, r, info, posts, queue.registry.sources['cnyes'], {'kind': 'retained'},
                    observed_at='2019-01-01T00:00:00+00:00', origin='verified_retained_source_response')
    assert head(queue, url)['state'] == 'waiting_page_evidence'
    assert queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=?', (cnyes_scope(url),)).fetchone() is None
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_corrupt_observation_audit').fetchone()[0] == 1


def test_corrupt_observation_archive_and_replacement_rollback_together(queue, monkeypatch):
    import downloader.news_fulltext_cnyes_history as history

    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    r = capture(queue, url, source_payload([1, 2], epoch=low))
    info, posts = cnyes_pagination(r)
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_page_observations SET observation_json=?', ('{}',))
    original = dict(queue.db.execute('SELECT * FROM cnyes_history_page_observations').fetchone())
    original_head = dict(queue.db.execute('SELECT * FROM cnyes_history_page_heads').fetchone())
    monkeypatch.setattr(history, 'reconcile_scope', lambda *args: (_ for _ in ()).throw(RuntimeError('fixture failure')))
    with pytest.raises(RuntimeError, match='fixture failure'):
        with queue.db:
            record_page(queue, r, info, posts, queue.registry.sources['cnyes'], {'kind': 'new'})
    assert dict(queue.db.execute('SELECT * FROM cnyes_history_page_observations').fetchone()) == original
    assert dict(queue.db.execute('SELECT * FROM cnyes_history_page_heads').fetchone()) == original_head
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_corrupt_observation_audit').fetchone()[0] == 0


def test_new_observation_rebuilds_a_corrupt_proof_without_losing_its_old_bytes(queue):
    from downloader.news_fulltext_cnyes_history import digest

    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    data = source_payload([1, 2], epoch=low)
    capture(queue, url, data)
    old_sha = head(queue, url)['proof_sha256']
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_scope_proofs SET proof_json=? WHERE proof_sha256=?', ('{}', old_sha))
    capture(queue, url, data)
    row = queue.db.execute('SELECT * FROM cnyes_history_scope_proofs WHERE proof_sha256=?',
                           (head(queue, url)['proof_sha256'],)).fetchone()
    assert digest(json.loads(row['proof_json'])) == row['proof_sha256']
    assert head(queue, url)['state'] == 'list_complete'
    audit = queue.db.execute('SELECT original_row_json FROM cnyes_history_corrupt_proof_audit').fetchone()
    assert json.loads(audit[0]) == {'proof_sha256': old_sha, 'scope_url': cnyes_scope(url), 'proof_json': '{}'}


def test_integrity_scan_detects_invalid_shared_reference_without_resetting_429(queue):
    from downloader import news_fulltext_cnyes_history as history
    exemplar, alias = seed_equivalent(queue)
    capture(queue, exemplar, source_payload(range(1, 31), epoch=bounds(exemplar)[0], total=31))
    capture(queue, cnyes_url(BASE, 2, start=bounds(exemplar)[0], end=bounds(exemplar)[1]),
            source_payload([31], epoch=bounds(exemplar)[0], page=2, total=31))
    assert head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence'
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_page_ID_references SET original_ID_evidence_json='broken' "
                         'WHERE scope_url=? AND page=1', (cnyes_scope(alias),))
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9000000000 WHERE url=?", (alias,))
    original = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone())
    result = history.sweep_completed_scopes(queue, limit=100, max_seconds=10, now_epoch=1000)
    assert result['invalid'] >= 1 and head(queue, alias)['state'] == 'inconsistent_original_page_ID_reference'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone()) == original


@pytest.mark.parametrize('rate_limited', [False, True])
def test_reconciliation_rejects_corrupt_shared_reference_receipt(queue, rate_limited):
    from downloader.news_fulltext_cnyes_history import reconcile_scope

    exemplar, alias = seed_equivalent(queue)
    capture(queue, exemplar, source_payload(range(1, 31), epoch=bounds(exemplar)[0], total=31))
    capture(queue, cnyes_url(BASE, 2, start=bounds(exemplar)[0], end=bounds(exemplar)[1]),
            source_payload([31], epoch=bounds(exemplar)[0], page=2, total=31))
    assert head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence'
    with queue.db:
        if rate_limited:
            queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9000000000 WHERE url=?", (alias,))
        original_job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone())
        queue.db.execute('UPDATE cnyes_history_page_ID_references SET original_ID_evidence_json=? '
                         'WHERE scope_url=? AND page=1', ('{}', cnyes_scope(alias)))
        reconcile_scope(queue, cnyes_scope(alias), queue.registry.sources['cnyes'])
    assert head(queue, alias)['state'] == 'inconsistent_original_page_ID_reference'
    assert queue.db.execute('SELECT original_ID_evidence_json FROM cnyes_history_page_ID_references '
                            'WHERE scope_url=? AND page=1', (cnyes_scope(alias),)).fetchone()[0] == '{}'
    job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone())
    if rate_limited:
        assert job == original_job
    else:
        assert job['due'] < original_job['due'] and job['error_code'] is None and job['attempts'] == 0
        capture(queue, alias, source_payload(range(1, 31), epoch=bounds(alias)[0], total=31))
        assert head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence'
        assert queue.db.execute('SELECT original_ID_evidence_json FROM cnyes_history_page_ID_references '
                                'WHERE scope_url=? AND page=1', (cnyes_scope(alias),)).fetchone()[0] == '{}'


def test_reconciliation_rejects_validly_hashed_wrong_provider(queue):
    from downloader.news_fulltext_cnyes_history import digest, encoded, reconcile_scope

    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    capture(queue, url, source_payload([1, 2], epoch=low))
    row = queue.db.execute('SELECT * FROM cnyes_history_page_observations').fetchone()
    observation = json.loads(row['observation_json'])
    observation['provider'] = 'other_provider'
    changed_sha = digest(observation)
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_page_observations SET observation_sha256=?,observation_json=? '
                         'WHERE observation_sha256=?', (changed_sha, encoded(observation), row['observation_sha256']))
        queue.db.execute('UPDATE cnyes_history_page_heads SET observation_sha256=? '
                         'WHERE scope_url=? AND page=1', (changed_sha, cnyes_scope(url)))
        reconcile_scope(queue, cnyes_scope(url), queue.registry.sources['cnyes'])
    assert head(queue, url)['state'] != 'list_complete'
    capture(queue, url, source_payload([1, 2], epoch=low))
    assert head(queue, url)['state'] == 'list_complete'
    archived = queue.db.execute('SELECT original_row_json FROM cnyes_history_corrupt_observation_audit').fetchone()
    assert json.loads(json.loads(archived[0])['observation_json'])['provider'] == 'other_provider'


@pytest.mark.parametrize('corruption', ['ids_sha256', 'returned_rows', 'unique_rows', 'duplicate_rows', 'post_sha'])
def test_reconciliation_rejects_validly_hashed_invalid_page_metadata(queue, corruption):
    from downloader.news_fulltext_cnyes_history import digest, encoded, reconcile_scope

    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    data = source_payload([1, 2], epoch=low)
    capture(queue, url, data)
    row = queue.db.execute('SELECT * FROM cnyes_history_page_observations').fetchone()
    observation = json.loads(row['observation_json'])
    if corruption == 'ids_sha256':
        observation['pagination']['ids_sha256'] = '0' * 64
    elif corruption == 'post_sha':
        observation['identities'][0]['source_post_sha256'] = 'bad'
    else:
        observation['pagination'][corruption] += 1
    changed_sha = digest(observation)
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_page_observations SET observation_sha256=?,observation_json=? '
                         'WHERE observation_sha256=?', (changed_sha, encoded(observation), row['observation_sha256']))
        queue.db.execute('UPDATE cnyes_history_page_heads SET observation_sha256=? WHERE scope_url=? AND page=1',
                         (changed_sha, cnyes_scope(url)))
        reconcile_scope(queue, cnyes_scope(url), queue.registry.sources['cnyes'])
    assert head(queue, url)['state'] == 'invalid_page_evidence'
    capture(queue, url, data)
    assert head(queue, url)['state'] == 'list_complete'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_corrupt_observation_audit').fetchone()[0] == 1


def test_alias_proof_never_conceals_known_source_date_contradiction(queue):
    exemplar, alias = seed_equivalent(queue)
    # Metadata disagrees with the measured source date while preserving IDs.
    capture(queue, alias, source_payload(range(1, 31), epoch=bounds(alias)[0] - 86400, total=31))
    capture(queue, exemplar, source_payload(range(1, 31), epoch=bounds(exemplar)[0], total=31))
    capture(queue, cnyes_url(BASE, 2, start=bounds(exemplar)[0], end=bounds(exemplar)[1]),
            source_payload([31], epoch=bounds(exemplar)[0], page=2, total=31))
    assert head(queue, alias)['state'] == 'inconsistent_source_identity_or_date'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations WHERE scope_url=?',
                            (cnyes_scope(alias),)).fetchone()[0] == 1


def test_shared_alias_cannot_use_an_original_marked_unproven(queue):
    from downloader import news_fulltext_cnyes_history as history

    exemplar, alias = seed_equivalent(queue)
    capture(queue, exemplar, source_payload(range(1, 31), epoch=bounds(exemplar)[0], total=31))
    capture(queue, cnyes_url(BASE, 2, start=bounds(exemplar)[0], end=bounds(exemplar)[1]),
            source_payload([31], epoch=bounds(exemplar)[0], page=2, total=31))
    source = queue.registry.sources['cnyes']
    assert history._verified_partition_scope_proof(queue, cnyes_scope(alias), source)
    with queue.db:
        queue.db.execute("INSERT OR REPLACE INTO cnyes_history_unproven_pages VALUES (?,?,?)",
                         (cnyes_scope(exemplar), 1, '{}'))
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (alias,))
        before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone())
        assert not history._verified_partition_scope_proof(queue, cnyes_scope(alias), source)
        history.reconcile_scope(queue, cnyes_scope(alias), source)
    assert head(queue, alias)['state'] == 'inconsistent_original_page_ID_reference'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone()) == before


def test_duplicate_source_SQL_rows_retained_but_not_unique_article_denominator(queue):
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 1)
    data = source_payload([1, 1, 2, 2], epoch=low, total=4)
    data['items'].update({'from': 1, 'to': 4})
    capture(queue, url, data)
    proof = json.loads(queue.db.execute('SELECT proof_json FROM cnyes_history_scope_proofs WHERE proof_sha256=?',
                                       (head(queue, url)['proof_sha256'],)).fetchone()[0])
    assert proof['unique_article_IDs'] == 2
    assert proof['facts']['total_articles_raw'] == 4
    assert proof['facts']['duplicate_rows_observed'] == 2
    assert proof['facts']['declared_totals_are_not_unique_article_denominators']


def test_same_response_bytes_reuse_canonical_source_without_body_or_capture_rewrite(queue):
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 1)
    data = source_payload([1, 2], epoch=low)
    capture(queue, url, data)
    original = [dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')]
    capture(queue, url, data)
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
    assert [{k: v for k, v in dict(row).items() if k != 'last_seen_utc'} for row in queue.db.execute('SELECT * FROM articles ORDER BY url')] == [
        {k: v for k, v in row.items() if k != 'last_seen_utc'} for row in original]
    # The first source is already durable inside its body batch. Repeated
    # identical responses use that exact reference, not another raw copy.
    capture(queue, url, data)
    assert len(list(queue.root.glob('discovery_responses/*.parquet'))) == 0
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 1


def test_identical_empty_responses_keep_each_actual_request_envelope(queue):
    import pyarrow.parquet as pq

    urls = [cnyes_url(BASE, start=start, end=start + 100) for start in (1388534400, 1388534600)]
    responses = [capture(queue, url, source_payload([], epoch=bounds(url)[0])) for url in urls]
    assert responses[0].body == responses[1].body
    artifacts = []
    for url in urls:
        row = queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations WHERE scope_url=?',
                               (cnyes_scope(url),)).fetchone()
        artifact = json.loads(row[0])['artifact']
        original = pq.read_table(queue.root / artifact['parquet_path']).to_pylist()[0]
        assert original['request_url'] == url
        assert original['authorization_fingerprint'] == queue.registry.sources['cnyes'].authorization_fingerprint
        assert original['response_sha256'] == hashlib.sha256(responses[0].body).hexdigest()
        artifacts.append(artifact['parquet_path'])
    assert artifacts[0] != artifacts[1]


def test_repeated_empty_response_reuses_same_original_and_first_capture_clock(queue):
    import pyarrow.parquet as pq

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    data = source_payload([], epoch=bounds(url)[0])
    capture(queue, url, data)
    before = dict(queue.db.execute('SELECT * FROM cnyes_history_page_observations').fetchone())
    artifact = json.loads(before['observation_json'])['artifact']
    path = queue.root / artifact['parquet_path']
    original = pq.read_table(path).to_pylist()[0]
    signature = (path.stat().st_size, path.stat().st_mtime_ns, sha256_file(path))
    capture(queue, url, data)
    after = dict(queue.db.execute('SELECT * FROM cnyes_history_page_observations').fetchone())
    assert after == before
    assert (path.stat().st_size, path.stat().st_mtime_ns, sha256_file(path)) == signature
    assert pq.read_table(path).to_pylist()[0]['captured_at_utc'] == original['captured_at_utc']
    assert len(list(queue.root.glob('discovery_responses/**/*.parquet'))) == 1


def test_original_audit_cache_reuses_bytes_without_borrowing_request_or_authorization(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    url, other = [cnyes_url(BASE, start=start, end=start + 100) for start in (1388534400, 1388534600)]
    r = capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    artifact = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])['artifact']
    sha = hashlib.sha256(r.body).hexdigest()
    auth = queue.registry.sources['cnyes'].authorization_fingerprint
    real_read = history.pq.read_table
    reads = []
    def counted(*args, **kwargs):
        reads.append(args[0])
        return real_read(*args, **kwargs)
    monkeypatch.setattr(history.pq, 'read_table', counted)
    verifier = history._OriginalEnvelopeAuditCache(max_entries=64)
    assert verifier.check(queue.root, artifact, url, sha, auth) == 'verified'
    assert verifier.check(queue.root, artifact, other, sha, auth) == 'request_mismatch'
    assert verifier.check(queue.root, artifact, url, sha, 'different-authorization') == 'authorization_mismatch'
    assert len(reads) == 1
    assert verifier.check(queue.root, artifact, url, '0' * 64, auth) == 'invalid'
    assert len(reads) == 2


@pytest.mark.parametrize(('field', 'bad_value'), [
    ('parquet_path', 'discovery_responses/wrong-original.parquet'),
    ('contract', 'unrecognized_history_contract'),
    ('raw_response_bytes', -1),
    ('raw_response_bytes', True),
    ('raw_response_bytes', '__remove__'),
    ('article_bodies_accepted', True),
    ('article_bodies_accepted', '__remove__'),
])
def test_history_original_rejects_contradictory_receipt_without_overwriting_evidence(queue, field, bad_value):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    r = capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    artifact = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])['artifact']
    assert artifact['kind'] == 'cnyes_history_source_envelope'
    path, receipt_path = (queue.root / artifact[name] for name in ('parquet_path', 'receipt_path'))
    sha = hashlib.sha256(r.body).hexdigest()
    source = queue.registry.sources['cnyes']
    verifier = history._OriginalEnvelopeAuditCache()
    check = lambda: verifier.check(queue.root, artifact, url, sha, source.authorization_fingerprint)
    assert check() == 'verified'
    receipt = json.loads(receipt_path.read_bytes())
    if bad_value == '__remove__':
        del receipt[field]
    else:
        receipt[field] = bad_value
    atomic_write_json(receipt_path, receipt)
    original_bytes, bad_receipt_bytes = path.read_bytes(), receipt_path.read_bytes()

    assert history._original_envelope_state(queue.root, artifact, url, sha, source.authorization_fingerprint) == 'invalid'
    assert check() == 'invalid'
    replacement = history.retain_response(queue, r, source, accepted=[], restricted=False)
    assert replacement['parquet_path'] != artifact['parquet_path']
    assert path.read_bytes() == original_bytes and receipt_path.read_bytes() == bad_receipt_bytes
    assert history._original_envelope_state(queue.root, replacement, url, sha, source.authorization_fingerprint) == 'verified'


@pytest.mark.parametrize(('field', 'bad_value'), [
    ('article_count', 4),
    ('accepted_body_count', True),
    ('accepted_body_count', 4),
    ('accepted_body_count', '__remove__'),
])
def test_quarantine_receipt_counts_must_match_retained_errors(queue, field, bad_value):
    import pyarrow.parquet as pq
    from downloader import news_fulltext_cnyes_history as history

    url, r = seed_retained_work(queue)
    sha = hashlib.sha256(r.body).hexdigest()
    path = queue.root / 'quarantine' / (sha + '.parquet')
    original = pq.read_table(path).to_pylist()[0]
    original['errors_json'] = json.dumps([{'error': 'one'}, {'error': 'two'}])
    atomic_write_parquet(path, table_from_records([original]), durable=True)
    relative = str(path.relative_to(queue.root))
    receipt_path = path.with_suffix('.parquet.receipt.json')
    receipt = {'parquet_path': relative, 'sha256': sha256_file(path), 'response_sha256': sha,
               'publish': False, 'article_count': 3, 'accepted_body_count': 1}
    atomic_write_json(receipt_path, receipt)
    artifact = {'kind': 'canonical_quarantine', 'parquet_path': relative, 'receipt_path': relative + '.receipt.json'}
    auth = queue.registry.sources['cnyes'].authorization_fingerprint
    verifier = history._OriginalEnvelopeAuditCache()
    assert verifier.check(queue.root, artifact, url, sha, auth) == 'verified'
    if bad_value == '__remove__':
        del receipt[field]
    else:
        receipt[field] = bad_value
    atomic_write_json(receipt_path, receipt)
    assert history._original_envelope_state(queue.root, artifact, url, sha, auth) == 'invalid'
    assert verifier.check(queue.root, artifact, url, sha, auth) == 'invalid'


def test_original_audit_cache_keeps_artifact_kind_in_its_identity(queue):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    r = capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    artifact = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])['artifact']
    sha = hashlib.sha256(r.body).hexdigest()
    auth = queue.registry.sources['cnyes'].authorization_fingerprint
    verifier = history._OriginalEnvelopeAuditCache()
    assert verifier.check(queue.root, artifact, url, sha, auth) == 'verified'
    other = dict(artifact, kind='canonical_quarantine')
    assert history._original_envelope_state(queue.root, other, url, sha, auth) == 'invalid'
    assert verifier.check(queue.root, other, url, sha, auth) == 'invalid'


def test_original_audit_cache_detects_changed_file_receipt_and_missing_path(queue):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    r = capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    artifact = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])['artifact']
    path = queue.root / artifact['parquet_path']
    receipt = queue.root / artifact['receipt_path']
    original, original_receipt = path.read_bytes(), receipt.read_bytes()
    auth = queue.registry.sources['cnyes'].authorization_fingerprint
    verifier = history._OriginalEnvelopeAuditCache(max_entries=64)
    check = lambda: verifier.check(queue.root, artifact, url, hashlib.sha256(r.body).hexdigest(), auth)
    assert check() == 'verified'
    path.write_bytes(b'corrupt')
    assert check() == 'invalid'
    path.write_bytes(original)
    assert check() == 'verified'
    receipt.write_text('{"publish":true}')
    assert check() == 'invalid'
    receipt.write_bytes(original_receipt)
    assert check() == 'verified'
    held = path.with_suffix('.held')
    path.rename(held)
    assert check() == 'unavailable'
    held.rename(path)
    assert check() == 'verified'


def test_original_audit_cache_revalidates_same_size_and_mtime_replacement(queue):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    r = capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    artifact = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])['artifact']
    path = queue.root / artifact['parquet_path']
    sha = hashlib.sha256(r.body).hexdigest()
    auth = queue.registry.sources['cnyes'].authorization_fingerprint
    verifier = history._OriginalEnvelopeAuditCache(max_entries=64)
    assert verifier.check(queue.root, artifact, url, sha, auth) == 'verified'
    before = path.stat()
    raw = bytearray(path.read_bytes())
    raw[len(raw) // 2] ^= 1
    replacement = path.with_suffix('.replacement')
    replacement.write_bytes(raw)
    os.utime(replacement, ns=(before.st_atime_ns, before.st_mtime_ns))
    os.replace(replacement, path)
    after = path.stat()
    assert (after.st_size, after.st_mtime_ns) == (before.st_size, before.st_mtime_ns)
    assert after.st_ino != before.st_ino
    def outcome(check):
        try:
            return check()
        except OSError:
            return 'local_io_unavailable'
    cached = outcome(lambda: verifier.check(queue.root, artifact, url, sha, auth))
    direct = outcome(lambda: history._original_envelope_state(queue.root, artifact, url, sha, auth))
    assert cached == direct and cached != 'verified'


def test_original_audit_cache_rechecks_capture_time_after_wall_clock_moves_back(queue, monkeypatch):
    from datetime import timedelta
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    r = capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    artifact = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])['artifact']
    original = history.pq.read_table(queue.root / artifact['parquet_path']).to_pylist()[0]
    captured = datetime.fromisoformat(original['captured_at_utc'])
    sha = hashlib.sha256(r.body).hexdigest()
    auth = queue.registry.sources['cnyes'].authorization_fingerprint
    class Clock(datetime):
        current = captured + timedelta(hours=1)
        @classmethod
        def now(cls, tz=None):
            return cls.current
    monkeypatch.setattr(history, 'datetime', Clock)
    verifier = history._OriginalEnvelopeAuditCache()
    assert verifier.check(queue.root, artifact, url, sha, auth) == 'verified'
    Clock.current = captured - timedelta(hours=1)
    assert history._original_envelope_state(queue.root, artifact, url, sha, auth) == 'invalid'
    assert verifier.check(queue.root, artifact, url, sha, auth) == 'invalid'
    Clock.current = captured + timedelta(hours=1)
    assert verifier.check(queue.root, artifact, url, sha, auth) == 'verified'


@pytest.mark.parametrize('changed_file', ['parquet', 'receipt'])
def test_original_audit_cache_rehashes_when_filesystem_signatures_do_not_change(queue, monkeypatch, changed_file):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    r = capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    artifact = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])['artifact']
    path, receipt = (queue.root / artifact[name] for name in ('parquet_path', 'receipt_path'))
    sha = hashlib.sha256(r.body).hexdigest()
    auth = queue.registry.sources['cnyes'].authorization_fingerprint
    verifier = history._OriginalEnvelopeAuditCache(max_entries=64)
    assert verifier.check(queue.root, artifact, url, sha, auth) == 'verified'
    signatures = {item: history._original_envelope_file_signature(item) for item in (path, receipt)}
    monkeypatch.setattr(history, '_original_envelope_file_signature', lambda item: signatures[item])
    target = path if changed_file == 'parquet' else receipt
    data = bytearray(target.read_bytes())
    data[0] ^= 1
    target.write_bytes(data)

    def outcome(check):
        try:
            return check()
        except OSError:
            return 'local_io_unavailable'
    cached = outcome(lambda: verifier.check(queue.root, artifact, url, sha, auth))
    direct = outcome(lambda: history._original_envelope_state(queue.root, artifact, url, sha, auth))
    assert cached == direct and cached != 'verified'


def test_original_audit_cache_rejects_a_receipt_changed_during_verification(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    r = capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    artifact = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])['artifact']
    receipt = queue.root / artifact['receipt_path']
    auth = queue.registry.sources['cnyes'].authorization_fingerprint
    sha = hashlib.sha256(r.body).hexdigest()
    verifier = history._OriginalEnvelopeAuditCache(max_entries=64)
    real_read = history.pq.read_table
    def read_and_change(*args, **kwargs):
        table = real_read(*args, **kwargs)
        receipt.write_text('{"publish":true}')
        return table
    with monkeypatch.context() as patch:
        patch.setattr(history.pq, 'read_table', read_and_change)
        with pytest.raises(OSError, match='changed during verification'):
            verifier.check(queue.root, artifact, url, sha, auth)
    assert verifier.check(queue.root, artifact, url, sha, auth) == 'invalid'
    assert not verifier.entries


def test_direct_original_verification_retries_if_receipt_changes_during_read(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    r = capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    artifact = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])['artifact']
    receipt = queue.root / artifact['receipt_path']
    auth = queue.registry.sources['cnyes'].authorization_fingerprint
    sha = hashlib.sha256(r.body).hexdigest()
    real_read = history.pq.read_table
    def read_and_change(*args, **kwargs):
        table = real_read(*args, **kwargs)
        atomic_write_json(receipt, {'publish': True})
        return table
    with monkeypatch.context() as patch:
        patch.setattr(history.pq, 'read_table', read_and_change)
        with pytest.raises(OSError, match='changed during verification'):
            history._original_envelope_state(queue.root, artifact, url, sha, auth)
    assert history._original_envelope_state(queue.root, artifact, url, sha, auth) == 'invalid'


def test_original_audit_cache_persists_across_bounded_owner_slices(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    first, second = [cnyes_url(BASE, start=start, end=start + 100) for start in (1388534400, 1388534600)]
    capture(queue, first, source_payload([], epoch=bounds(first)[0]))
    artifact = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])['artifact']
    with monkeypatch.context() as patch:
        patch.setattr(history, 'retain_response', lambda *args, **kwargs: artifact)
        capture(queue, second, source_payload([], epoch=bounds(second)[0]))
    real_read = history.pq.read_table
    reads = []
    def counted(*args, **kwargs):
        reads.append(args[0])
        return real_read(*args, **kwargs)
    monkeypatch.setattr(history.pq, 'read_table', counted)
    assert history.audit_original_envelopes(queue, limit=1, max_seconds=10)['checked'] == 1
    assert history.audit_original_envelopes(queue, limit=1, max_seconds=10)['checked'] == 1
    assert len(reads) == 1
    assert head(queue, second)['state'] == 'waiting_original_page_recheck'


def test_fresh_empty_request_restores_its_own_retained_envelope_without_source_check(queue):
    urls = [cnyes_url(BASE, start=start, end=start + 100) for start in (1388534400, 1388534600)]
    for url in urls:
        capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    with queue.db:
        for table in ('cnyes_history_page_heads', 'cnyes_history_page_observations', 'cnyes_history_scope_heads'):
            queue.db.execute('DELETE FROM ' + table + ' WHERE scope_url=?', (cnyes_scope(urls[1]),))
        queue.db.execute('UPDATE discovery SET due=9e9 WHERE url=?', (urls[1],))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[1],)).fetchone())
    result = backfill(queue, max_seconds=10)
    assert result['processed'] == 1
    assert head(queue, urls[1])['state'] == 'list_complete'
    observation = json.loads(queue.db.execute(
        'SELECT observation_json FROM cnyes_history_page_observations WHERE scope_url=?',
        (cnyes_scope(urls[1]),)).fetchone()[0])
    assert observation['request_url'] == urls[1]
    assert observation['origin'] == 'verified_retained_source_response'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[1],)).fetchone()) == before


def test_new_response_repairs_old_cross_request_reference_and_preserves_audit(queue, monkeypatch):
    import pyarrow.parquet as pq
    from downloader import news_fulltext_cnyes_history as history

    first, second = [cnyes_url(BASE, start=start, end=start + 100) for start in (1388534400, 1388534600)]
    capture(queue, first, source_payload([], epoch=bounds(first)[0]))
    old_artifact = json.loads(queue.db.execute(
        'SELECT observation_json FROM cnyes_history_page_observations WHERE scope_url=?',
        (cnyes_scope(first),)).fetchone()[0])['artifact']
    old_path = queue.root / old_artifact['parquet_path']
    old_hash = sha256_file(old_path)
    with monkeypatch.context() as patch:
        # Simulate the former content-only naming bug without modifying production.
        patch.setattr(history, 'retain_response', lambda *args, **kwargs: old_artifact)
        capture(queue, second, source_payload([], epoch=bounds(second)[0]))
    bad = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_observations WHERE scope_url=?',
                           (cnyes_scope(second),)).fetchone()[0]
    capture(queue, second, source_payload([], epoch=bounds(second)[0]))
    observation = json.loads(queue.db.execute(
        'SELECT observation_json FROM cnyes_history_page_observations WHERE scope_url=?',
        (cnyes_scope(second),)).fetchone()[0])
    assert pq.read_table(queue.root / observation['artifact']['parquet_path']).to_pylist()[0]['request_url'] == second
    assert sha256_file(old_path) == old_hash
    audit = queue.db.execute('SELECT original_row_json,reason FROM cnyes_history_corrupt_observation_audit').fetchone()
    assert json.loads(audit[0])['observation_sha256'] == bad
    assert audit[1] == 'page_original_envelope_invalid_or_request_mismatch'
    assert head(queue, second)['state'] == 'list_complete'


def test_legacy_original_audit_resumes_and_recovers_without_resetting_429(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    first, second = [cnyes_url(BASE, start=start, end=start + 100) for start in (1388534400, 1388534600)]
    capture(queue, first, source_payload([], epoch=bounds(first)[0]))
    artifact = json.loads(queue.db.execute(
        'SELECT observation_json FROM cnyes_history_page_observations WHERE scope_url=?',
        (cnyes_scope(first),)).fetchone()[0])['artifact']
    with monkeypatch.context() as patch:
        patch.setattr(history, 'retain_response', lambda *args, **kwargs: artifact)
        capture(queue, second, source_payload([], epoch=bounds(second)[0]))
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (second,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone())
    stop = Event()
    stop.set()
    assert history.audit_original_envelopes(queue, stop=stop, limit=1, max_seconds=10)['checked'] == 0
    first_slice = history.audit_original_envelopes(queue, limit=1, max_seconds=10)
    assert not first_slice['complete'] and first_slice['checked'] == 1
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        for _ in range(8):
            result = history.audit_original_envelopes(resumed, limit=1, max_seconds=10)
            if result['complete']:
                break
        assert result['complete']
        assert history.progress(resumed.db, resumed.registry.sources['cnyes'])['original_envelope_audit']['mismatched_pages'] == 1
        assert head(resumed, second)['state'] == 'waiting_original_page_recheck'
        work = resumed.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (second,)).fetchone()
        assert work and work[0] == 'pending'
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone()) == before
        for _ in range(8):
            history.backfill(resumed, limit=8, max_seconds=10)
            if resumed.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                                  (second,)).fetchone()[0] == 'retained_request_mismatch':
                break
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone()) == before
        assert head(resumed, second)['state'] == 'waiting_original_page_recheck'
        capture(resumed, second, source_payload([], epoch=bounds(second)[0]))
        assert head(resumed, second)['state'] == 'list_complete'
        assert resumed.db.execute('SELECT reason FROM cnyes_history_corrupt_observation_audit').fetchone()[0] == (
            'page_original_envelope_invalid_or_request_mismatch')


@pytest.mark.parametrize('prior_state', [None, 'pending', 'retained_unavailable', 'same_terminal'])
@pytest.mark.parametrize('other_marker', [False, True])
@pytest.mark.parametrize('mismatch', ['request_mismatch', 'authorization_mismatch'])
def test_mismatched_original_discovered_by_alias_then_restored_direct_head_is_repaired(
        queue, monkeypatch, prior_state, other_marker, mismatch):
    from downloader import news_fulltext_cnyes_history as history

    low = 1388534400
    exemplar = cnyes_url(BASE, start=low, end=low + 100)
    alias = cnyes_url(BASE, start=low + 101, end=low + 300)
    first = source_payload([], epoch=low)
    info, _ = cnyes_pagination(response(first, alias))
    with queue.db:
        history.index_scope(queue, cnyes_scope(alias), 'cnyes')
        queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'cnyes',0,1,'old-alias-clock')", (cnyes_scope(alias),))
        queue.db.execute('INSERT INTO bulk_page_evidence VALUES (?,?,?,?,?)',
                         (cnyes_scope(alias), 1, 0, info['ids_sha256'], 'real-alias-page-clock'))
        queue.db.execute("INSERT INTO discovery(url,kind,provider,due) VALUES (?,'cnyes_news','cnyes',9e9)", (alias,))
    source = queue.registry.sources['cnyes']
    other = cnyes_url(BASE, start=1388535000, end=1388535100)
    wrong_source = (replace(source, authorization_fingerprint='different-authorization')
                    if mismatch == 'authorization_mismatch' else source)
    wrong_url = exemplar if mismatch == 'authorization_mismatch' else other
    wrong = history.retain_response(queue, response(first, wrong_url), wrong_source, accepted=[], restricted=False)
    with monkeypatch.context() as patch:
        patch.setattr(history, 'retain_response', lambda *args, **kwargs: wrong)
        capture(queue, exemplar, first)
    direct_scope, alias_scope = cnyes_scope(exemplar), cnyes_scope(alias)
    old_head = tuple(queue.db.execute('SELECT scope_url,page,observation_sha256,last_confirmed_at_utc '
                                     'FROM cnyes_history_page_heads WHERE scope_url=? AND page=1',
                                     (direct_scope,)).fetchone())
    assert queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads '
                            'WHERE scope_url=? AND page=1', (alias_scope,)).fetchone()[0] == old_head[2]
    with queue.db:
        queue.db.execute('DELETE FROM cnyes_history_page_heads WHERE scope_url=? AND page=1', (direct_scope,))
        queue.db.execute('DELETE FROM cnyes_history_accepted_batch_candidates')
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (exemplar,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (exemplar,)).fetchone())
    result = history.audit_original_envelopes(queue, limit=30, max_seconds=10, now_epoch=1000)
    assert result['mismatched'] == 1
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_retained_work WHERE request_url=?', (exemplar,)).fetchone()
    incident = queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
        ('cnyes_history_original_envelope_incident_v1:' + old_head[2],)).fetchone()
    assert json.loads(incident[0])['reason'] == mismatch
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations '
        'WHERE observation_sha256=?', (old_head[2],)).fetchone()[0])
    assert not history._has_alternate_original_candidate(queue, source, observation)
    old_sha = hashlib.sha256(b'older local revision').hexdigest()
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_page_heads VALUES (?,?,?,?)', old_head)
        if other_marker:
            queue.db.execute('INSERT INTO cnyes_history_unproven_pages VALUES (?,?,?)',
                (direct_scope, 1, json.dumps({'reason': 'unproven_or_different_original_page_ID_set',
                                             'actual_observation_sha256': old_head[2]})))
        if prior_state:
            info = observation['pagination']
            prior_sha = observation['response_sha256'] if prior_state == 'same_terminal' else old_sha
            saved_state = 'retained_unavailable' if prior_state == 'same_terminal' else prior_state
            queue.db.execute('INSERT INTO cnyes_history_retained_work '
                '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc,state) '
                'VALUES (?,?,?,?,?,?,?,?)', (exemplar, direct_scope, 1, prior_sha, info['total'], info['ids_sha256'],
                                             observation['source_observed_at_utc'], saved_state))
        queue.db.execute('INSERT OR IGNORE INTO cnyes_history_original_file_retries VALUES (?,?,?,?,?,0)',
            (history.original_envelope_audit_key(source), direct_scope, 1, old_head[2], 0))
    history.audit_original_envelopes(queue, limit=30, max_seconds=10, now_epoch=1001)
    work = queue.db.execute('SELECT state,response_sha256 FROM cnyes_history_retained_work WHERE request_url=?',
                            (exemplar,)).fetchone()
    if prior_state == 'pending':
        assert tuple(work) == ('pending', old_sha)  # An older unfinished revision keeps its turn.
        history.backfill(queue, limit=20, max_seconds=10)
        work = queue.db.execute('SELECT state,response_sha256 FROM cnyes_history_retained_work WHERE request_url=?',
                                (exemplar,)).fetchone()
    expected_state = 'retained_unavailable' if prior_state == 'same_terminal' else 'pending'
    assert tuple(work) == (expected_state, observation['response_sha256'])
    if prior_state and prior_state != 'same_terminal':
        archived = json.loads(queue.db.execute('SELECT original_work_json FROM cnyes_history_retained_revision_audit '
                                               'WHERE request_url=?', (exemplar,)).fetchone()[0])
        assert archived['work']['response_sha256'] == old_sha
    if other_marker:
        marker = json.loads(queue.db.execute('SELECT evidence_json FROM cnyes_history_unproven_pages '
            'WHERE scope_url=? AND page=1', (direct_scope,)).fetchone()[0])
        assert marker['reason'] == 'unproven_or_different_original_page_ID_set'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (exemplar,)).fetchone()) == before
    state = expected_state
    for _ in range(0 if prior_state == 'same_terminal' else 8):
        history.backfill(queue, limit=20, max_seconds=10)
        state = queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (exemplar,)).fetchone()[0]
        if state not in ('pending', 'overflow_pending', 'local_retry'):
            break
    assert state in ('retained_unavailable', 'retained_invalid', 'retained_request_mismatch')
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        source = resumed.registry.sources['cnyes']
        with resumed.db:
            resumed.db.execute('INSERT OR IGNORE INTO cnyes_history_original_file_retries VALUES (?,?,?,?,?,0)',
                (history.original_envelope_audit_key(source), direct_scope, 1, old_head[2], 0))
        history.audit_original_envelopes(resumed, limit=30, max_seconds=10, now_epoch=1002)
        assert resumed.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                                  (exemplar,)).fetchone()[0] == state
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (exemplar,)).fetchone()) == before
        # Simulate a later successful, exact source response. This is not an
        # HTTP call and is not allowed to relabel the previous artifact.
        capture(resumed, exemplar, first)
        restored = json.loads(resumed.db.execute('SELECT o.observation_json FROM cnyes_history_page_heads h '
            'JOIN cnyes_history_page_observations o ON o.observation_sha256=h.observation_sha256 '
            'WHERE h.scope_url=? AND h.page=1', (direct_scope,)).fetchone()[0])
        assert history._original_envelope_state(resumed.root, restored['artifact'], exemplar,
            restored['response_sha256'], source.authorization_fingerprint) == 'verified'
        assert resumed.db.execute('SELECT 1 FROM cnyes_history_corrupt_observation_audit').fetchone()


@pytest.mark.parametrize('rate_limited', [False, True])
@pytest.mark.parametrize('broken', ['{not-valid-json', 'null', '[]', '{"value":NaN}', '[' * 1200 + ']' * 1200])
def test_malformed_unproven_marker_does_not_stop_scope_and_preserves_evidence(queue, rate_limited, broken):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    scope = cnyes_scope(url)
    data = source_payload([], epoch=bounds(url)[0])
    capture(queue, url, data)
    assert head(queue, url)['state'] == 'list_complete'
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_unproven_pages VALUES (?,?,?)', (scope, 1, broken))
        queue.db.execute("UPDATE discovery SET due=9e9,error_code=?,attempts=? WHERE url=?",
                         ('http_429' if rate_limited else None, 3 if rate_limited else 0, url))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    source = queue.registry.sources['cnyes']
    with queue.db:
        history.reconcile_scope(queue, scope, source)
    assert head(queue, url)['state'] == 'waiting_original_page_recheck'
    assert queue.db.execute('SELECT evidence_json FROM cnyes_history_unproven_pages '
                            'WHERE scope_url=? AND page=1', (scope,)).fetchone()[0] == broken
    archives = [json.loads(row[0]) for row in queue.db.execute("SELECT value FROM queue_settings "
        "WHERE key LIKE 'cnyes_history_corrupt_unproven_marker_v1:%'")]
    assert any(item['scope_url'] == scope and item['page'] == 1 and item['original_evidence_json'] == broken
               for item in archives)
    after = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    if rate_limited:
        assert after == before
    else:
        assert after['due'] < before['due'] and after['attempts'] == 0
    with queue.db:
        history.reconcile_scope(queue, scope, source)
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == after
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        with resumed.db:
            history.reconcile_scope(resumed, scope, resumed.registry.sources['cnyes'])
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == after
        capture(resumed, url, data)  # A later simulated exact response, not network access.
        assert head(resumed, url)['state'] == 'list_complete'
        assert not resumed.db.execute('SELECT 1 FROM cnyes_history_unproven_pages WHERE scope_url=?', (scope,)).fetchone()
        assert any(json.loads(row[0])['original_evidence_json'] == broken for row in resumed.db.execute(
            "SELECT value FROM queue_settings WHERE key LIKE 'cnyes_history_corrupt_unproven_marker_v1:%'"))


def test_corrupt_unproven_marker_archive_conflict_does_not_recheck_source(queue):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    scope, broken = cnyes_scope(url), '{bad'
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    archive = {'scope_url': scope, 'page': 1, 'original_evidence_json': broken}
    key = 'cnyes_history_corrupt_unproven_marker_v1:' + history.digest(archive)
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_unproven_pages VALUES (?,?,?)', (scope, 1, broken))
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (key, 'conflicting evidence'))
        queue.db.execute('UPDATE discovery SET due=9e9,error_code=NULL,attempts=0 WHERE url=?', (url,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    with queue.db:
        history.reconcile_scope(queue, scope, queue.registry.sources['cnyes'])
    assert head(queue, url)['state'] == 'waiting_original_page_recheck'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0] == 'conflicting evidence'
    assert queue.db.execute('SELECT evidence_json FROM cnyes_history_unproven_pages WHERE scope_url=?', (scope,)).fetchone()[0] == broken


def test_corrupt_unproven_marker_archive_and_recheck_roll_back_together(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    scope = cnyes_scope(url)
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_unproven_pages VALUES (?,?,?)', (scope, 1, '{bad'))
        queue.db.execute('UPDATE discovery SET due=9e9,error_code=NULL,attempts=0 WHERE url=?', (url,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    with monkeypatch.context() as patch:
        def fail(*args, **kwargs):
            raise RuntimeError('injected before source recheck')
        patch.setattr(history, 'schedule_original_page_check', fail)
        with pytest.raises(RuntimeError, match='injected before source recheck'), queue.db:
            history.reconcile_scope(queue, scope, queue.registry.sources['cnyes'])
    assert queue.db.execute('SELECT evidence_json FROM cnyes_history_unproven_pages WHERE scope_url=?', (scope,)).fetchone()[0] == '{bad'
    assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE 'cnyes_history_corrupt_unproven_marker_v1:%'").fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    with queue.db:
        history.reconcile_scope(queue, scope, queue.registry.sources['cnyes'])
    assert head(queue, url)['state'] == 'waiting_original_page_recheck'
    assert queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE 'cnyes_history_corrupt_unproven_marker_v1:%'").fetchone()


@pytest.mark.parametrize('path', ['direct', 'alias_matching', 'alias_different'])
@pytest.mark.parametrize('broken', ['{bad', 'null', b'\xff\x00'])
def test_new_page_preserves_corrupt_marker_before_scope_audit(queue, path, broken):
    from downloader import news_fulltext_cnyes_history as history

    low = 1388534400
    if path == 'direct':
        url = cnyes_url(BASE, start=low, end=low + 100)
        data = source_payload([], epoch=low)
        capture(queue, url, data)
        scope, page = cnyes_scope(url), 1
    else:
        exemplar, alias = seed_equivalent(queue, change_page2=path == 'alias_different')
        capture(queue, exemplar, source_payload(range(1, 31), epoch=low, total=31))
        url = cnyes_url(BASE, 2, start=low, end=low + 100)
        data = source_payload([31], epoch=low, page=2, total=31)
        scope, page = cnyes_scope(alias), 2
    saved_raw = {'sqlite_type': 'blob', 'hex': broken.hex()} if isinstance(broken, bytes) else broken
    archive = {'scope_url': scope, 'page': page, 'original_evidence_json': saved_raw}
    archive_key = 'cnyes_history_corrupt_unproven_marker_v1:' + history.digest(archive)
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_unproven_pages VALUES (?,?,?)', (scope, page, broken))
    capture(queue, url, data)
    saved = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (archive_key,)).fetchone()
    assert saved and json.loads(saved[0]) == archive
    marker = queue.db.execute('SELECT evidence_json FROM cnyes_history_unproven_pages '
                              'WHERE scope_url=? AND page=?', (scope, page)).fetchone()
    if path == 'alias_different':
        assert json.loads(marker[0])['reason'] == 'unproven_or_different_original_page_ID_set'
    else:
        assert marker is None


@pytest.mark.parametrize('path', ['direct', 'alias_matching', 'alias_different'])
def test_new_page_keeps_corrupt_marker_when_archive_conflicts(queue, path):
    from downloader import news_fulltext_cnyes_history as history

    low = 1388534400
    if path == 'direct':
        url = cnyes_url(BASE, start=low, end=low + 100)
        data = source_payload([], epoch=low)
        capture(queue, url, data)
        scope, page = cnyes_scope(url), 1
    else:
        exemplar, alias = seed_equivalent(queue, change_page2=path == 'alias_different')
        capture(queue, exemplar, source_payload(range(1, 31), epoch=low, total=31))
        url = cnyes_url(BASE, 2, start=low, end=low + 100)
        data = source_payload([31], epoch=low, page=2, total=31)
        scope, page = cnyes_scope(alias), 2
    broken = '{bad'
    archive = {'scope_url': scope, 'page': page, 'original_evidence_json': broken}
    key = 'cnyes_history_corrupt_unproven_marker_v1:' + history.digest(archive)
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_unproven_pages VALUES (?,?,?)', (scope, page, broken))
        queue.db.execute('INSERT INTO queue_settings VALUES (?,?)', (key, 'conflicting evidence'))
    capture(queue, url, data)
    assert queue.db.execute('SELECT evidence_json FROM cnyes_history_unproven_pages '
                            'WHERE scope_url=? AND page=?', (scope, page)).fetchone()[0] == broken
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0] == 'conflicting evidence'
    assert queue.db.execute('SELECT state FROM cnyes_history_scope_heads WHERE scope_url=?', (scope,)).fetchone()[0] not in history.COMPLETE_STATES


@pytest.mark.parametrize('failure_stage', ['before_archive', 'after_remove'])
def test_new_page_corrupt_marker_archive_and_removal_roll_back_together(queue, monkeypatch, failure_stage):
    from downloader import news_fulltext_cnyes_history as history

    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    response_value = capture(queue, url, source_payload([], epoch=low))
    info, posts = cnyes_pagination(response_value)
    scope = cnyes_scope(url)
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations '
                                              'WHERE scope_url=?', (scope,)).fetchone()[0])
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_unproven_pages VALUES (?,?,?)', (scope, 1, '{bad'))
    old_head = tuple(queue.db.execute('SELECT * FROM cnyes_history_page_heads WHERE scope_url=?', (scope,)).fetchone())
    with monkeypatch.context() as patch:
        def fail(*args, **kwargs):
            raise RuntimeError('injected marker transaction failure')
        patch.setattr(history, '_archive_corrupt_unproven_marker' if failure_stage == 'before_archive' else 'reconcile_scope', fail)
        with pytest.raises(RuntimeError, match='injected marker transaction failure'), queue.db:
            history.record_page(queue, response_value, info, posts, queue.registry.sources['cnyes'], observation['artifact'])
    assert queue.db.execute('SELECT evidence_json FROM cnyes_history_unproven_pages WHERE scope_url=?', (scope,)).fetchone()[0] == '{bad'
    assert tuple(queue.db.execute('SELECT * FROM cnyes_history_page_heads WHERE scope_url=?', (scope,)).fetchone()) == old_head
    assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE 'cnyes_history_corrupt_unproven_marker_v1:%'").fetchone()


@pytest.mark.parametrize('broken', ['{bad', '[' * 1200 + ']' * 1200])
def test_original_audit_discovers_corrupt_marker_even_when_original_is_valid(queue, broken):
    from downloader import news_fulltext_cnyes_history as history

    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    scope = cnyes_scope(url)
    capture(queue, url, source_payload([], epoch=low))
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_unproven_pages VALUES (?,?,?)', (scope, 1, broken))
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (url,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    result = history.audit_original_envelopes(queue, limit=30, max_seconds=10, now_epoch=1000)
    assert result['checked'] == 1
    assert head(queue, url)['state'] == 'waiting_original_page_recheck'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    archive = {'scope_url': scope, 'page': 1, 'original_evidence_json': broken}
    key = 'cnyes_history_corrupt_unproven_marker_v1:' + history.digest(archive)
    assert json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0]) == archive


def test_older_retained_page_and_finished_work_handle_deep_corrupt_marker(queue):
    from downloader import news_fulltext_cnyes_history as history

    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    scope = cnyes_scope(url)
    response_value = capture(queue, url, source_payload([], epoch=low))
    info, posts = cnyes_pagination(response_value)
    source = queue.registry.sources['cnyes']
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations '
                                              'WHERE scope_url=?', (scope,)).fetchone()[0])
    old_head = tuple(queue.db.execute('SELECT * FROM cnyes_history_page_heads WHERE scope_url=?', (scope,)).fetchone())
    broken = '[' * 1200 + ']' * 1200
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_unproven_pages VALUES (?,?,?)', (scope, 1, broken))
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (url,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    with queue.db:
        history.record_page(queue, response_value, info, posts, source, observation['artifact'],
                            observed_at='2019-01-01T00:00:00+00:00', origin='verified_retained_source_response')
        history._reconsider_unverified_original_after_work(queue, source, scope, 1)
    assert tuple(queue.db.execute('SELECT * FROM cnyes_history_page_heads WHERE scope_url=?', (scope,)).fetchone()) == old_head
    assert queue.db.execute('SELECT evidence_json FROM cnyes_history_unproven_pages WHERE scope_url=?', (scope,)).fetchone()[0] == broken
    assert head(queue, url)['state'] == 'waiting_original_page_recheck'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    archive = {'scope_url': scope, 'page': 1, 'original_evidence_json': broken}
    key = 'cnyes_history_corrupt_unproven_marker_v1:' + history.digest(archive)
    assert json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0]) == archive


@pytest.mark.parametrize('old_work', [False, True])
def test_wrong_page_original_resumes_to_successful_job_recheck_after_restart(queue, monkeypatch, old_work):
    """A completed page with the wrong request needs fresh proof, not a false completion."""
    from downloader import news_fulltext_cnyes_history as history

    # Different source days cannot use the newer page-ID equivalence path;
    # this test continues to exercise the exact-request recheck fallback.
    first = cnyes_url(BASE, start=1388534400 - 86400, end=1388534500 - 86400)
    second = cnyes_url(BASE, start=1388534300, end=1388534500)
    data = source_payload([1], epoch=1388534400)
    data['items']['data'][0]['content'] = ''  # A list ID without an accepted article body.
    capture(queue, first, data)
    wrong = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations '
        'WHERE scope_url=?', (cnyes_scope(first),)).fetchone()[0])['artifact']
    with monkeypatch.context() as patch:
        patch.setattr(history, 'retain_response', lambda *args, **kwargs: wrong)
        r = capture(queue, second, data)
    response_sha = hashlib.sha256(r.body).hexdigest()
    info, _ = cnyes_pagination(r)
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=9e9,error_code=NULL,attempts=0 WHERE url=?', (second,))
        if old_work:
            queue.db.execute('INSERT INTO cnyes_history_retained_work '
                '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc,state) '
                'VALUES (?,?,?,?,?,?,?,?)', (second, cnyes_scope(second), 1, response_sha,
                    info['total'], info['ids_sha256'], '2020-01-01T00:00:00+00:00', 'retained_invalid'))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone())
    assert history.audit_original_envelopes(queue, limit=20, max_seconds=10)['mismatched'] == 1
    assert head(queue, second)['state'] == 'waiting_original_page_recheck'
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                            (second,)).fetchone()[0] == 'pending'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone()) == before
    root, registry = queue.root, queue.registry
    queue.close()
    with closing(NewsQueue(root, registry)) as resumed:
        for _ in range(12):
            history.backfill(resumed, limit=20, max_seconds=10)
            job = dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone())
            if job['due'] < before['due']:
                break
        assert job['due'] <= time.time()
        assert {k: v for k, v in job.items() if k not in ('due', 'etag', 'last_modified')} == {
            k: v for k, v in before.items() if k not in ('due', 'etag', 'last_modified')}
        assert head(resumed, second)['state'] == 'waiting_original_page_recheck'
        assert resumed.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        # A new response is simulated locally; the old observation is preserved in the audit.
        capture(resumed, second, data)
        assert head(resumed, second)['state'] == 'list_complete'
        assert resumed.db.execute('SELECT reason FROM cnyes_history_corrupt_observation_audit').fetchone()[0] == (
            'page_original_envelope_invalid_or_request_mismatch')


def test_legacy_original_audit_uses_an_earlier_exact_local_capture(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    first, second = [cnyes_url(BASE, start=start, end=start + 100) for start in (1388534400, 1388534600)]
    capture(queue, first, source_payload([], epoch=bounds(first)[0]))
    wrong = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])['artifact']
    source = queue.registry.sources['cnyes']
    exact = history.retain_response(queue, response(source_payload([], epoch=bounds(second)[0]), second),
                                    source, accepted=[], restricted=False)
    with monkeypatch.context() as patch:
        patch.setattr(history, 'retain_response', lambda *args, **kwargs: wrong)
        capture(queue, second, source_payload([], epoch=bounds(second)[0]))
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=9e9 WHERE url=?', (second,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone())
    assert history.audit_original_envelopes(queue, limit=20, max_seconds=10)['complete']
    for _ in range(8):
        history.backfill(queue, limit=20, max_seconds=10)
        if head(queue, second)['state'] == 'list_complete':
            break
    assert head(queue, second)['state'] == 'list_complete'
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations WHERE scope_url=?',
                                              (cnyes_scope(second),)).fetchone()[0])
    assert observation['artifact'] == exact
    assert observation['origin'] == 'verified_retained_source_response'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone()) == before


@pytest.mark.parametrize('mismatch', ['request', 'authorization'])
def test_wrong_original_local_work_does_not_requeue_itself_and_can_recover_later(queue, monkeypatch, mismatch):
    from downloader import news_fulltext_cnyes_history as history

    first, second = [cnyes_url(BASE, start=start, end=start + 100) for start in (1388534400, 1388534600)]
    capture(queue, first, source_payload([], epoch=bounds(first)[0]))
    source = queue.registry.sources['cnyes']
    if mismatch == 'request':
        wrong = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])['artifact']
    else:
        wrong = history.retain_response(queue, response(source_payload([], epoch=bounds(second)[0]), second),
                                        replace(source, authorization_fingerprint='foreign'), accepted=[], restricted=False)
    with monkeypatch.context() as patch:
        patch.setattr(history, 'retain_response', lambda *args, **kwargs: wrong)
        capture(queue, second, source_payload([], epoch=bounds(second)[0]))
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (second,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone())
    epoch = time.time()
    assert history.audit_original_envelopes(queue, limit=100, max_seconds=30, now_epoch=epoch)['complete']
    for _ in range(6):
        history.backfill(queue, limit=100, max_seconds=30)
    work = queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (second,)).fetchone()
    assert work['state'] in ('retained_unavailable', 'retained_invalid', 'retained_request_mismatch')
    outcomes = queue.db.execute('SELECT count(*) FROM cnyes_history_retained_outcome_resolutions '
                                'WHERE request_url=?', (second,)).fetchone()[0]
    for _ in range(4):
        history.backfill(queue, limit=100, max_seconds=30)
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                            (second,)).fetchone()[0] == work['state']
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_retained_outcome_resolutions '
                            'WHERE request_url=?', (second,)).fetchone()[0] == outcomes
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone()) == before

    exact = history.retain_response(queue, response(source_payload([], epoch=bounds(second)[0]), second),
                                    source, accepted=[], restricted=False)
    history.audit_original_envelopes(queue, limit=100, max_seconds=30,
                                     now_epoch=epoch + history.ORIGINAL_ENVELOPE_REAUDIT_SECONDS)
    for _ in range(6):
        history.backfill(queue, limit=100, max_seconds=30)
        if head(queue, second)['state'] == 'list_complete':
            break
    assert head(queue, second)['state'] == 'list_complete'
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations '
                                              'WHERE scope_url=?', (cnyes_scope(second),)).fetchone()[0])
    assert observation['artifact'] == exact
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone()) == before


def test_wrong_alias_local_work_waits_for_new_evidence_without_busy_loop(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    exemplar, alias = seed_equivalent(queue)
    low = bounds(exemplar)[0]
    first = source_payload(range(1, 31), epoch=low, total=31)
    source = queue.registry.sources['cnyes']
    wrong_url = cnyes_url(BASE, start=low + 301, end=low + 400)
    wrong = history.retain_response(queue, response(first, wrong_url), source, accepted=[], restricted=False)
    with monkeypatch.context() as patch:
        patch.setattr(history, 'retain_response', lambda *args, **kwargs: wrong)
        capture(queue, exemplar, first)
    capture(queue, cnyes_url(BASE, 2, start=low, end=bounds(exemplar)[1]),
            source_payload([31], epoch=low, page=2, total=31))
    exact_response = response(first, alias)
    with queue.db:
        queue.db.execute('DELETE FROM cnyes_history_page_heads WHERE scope_url=? AND page=1', (cnyes_scope(exemplar),))
        queue.db.execute('DELETE FROM cnyes_history_accepted_batch_candidates')
        queue.db.execute('DELETE FROM accepted_batches')
        queue.db.execute("UPDATE discovery SET response_sha256=?,last_checked_at_utc=?,error_code='http_429',"
                         'attempts=3,due=9e9 WHERE url=?',
                         (hashlib.sha256(exact_response.body).hexdigest(), '2020-01-01T00:00:00+00:00', alias))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone())
    epoch = time.time()
    assert history.audit_original_envelopes(queue, limit=100, max_seconds=30, now_epoch=epoch)['complete']
    for _ in range(6):
        history.backfill(queue, limit=100, max_seconds=30)
    work = queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (alias,)).fetchone()
    assert work['state'] in ('retained_unavailable', 'retained_invalid', 'retained_request_mismatch')
    outcomes = queue.db.execute('SELECT count(*) FROM cnyes_history_retained_outcome_resolutions '
                                'WHERE request_url=?', (alias,)).fetchone()[0]
    for _ in range(4):
        history.backfill(queue, limit=100, max_seconds=30)
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                            (alias,)).fetchone()[0] == work['state']
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_retained_outcome_resolutions '
                            'WHERE request_url=?', (alias,)).fetchone()[0] == outcomes
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone()) == before

    exact = history.retain_response(queue, exact_response, source, accepted=[], restricted=False)
    history.audit_original_envelopes(queue, limit=100, max_seconds=30,
                                     now_epoch=epoch + history.ORIGINAL_ENVELOPE_REAUDIT_SECONDS)
    for _ in range(6):
        history.backfill(queue, limit=100, max_seconds=30)
        if head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence':
            break
    assert head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence'
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations '
                                              'WHERE scope_url=?', (cnyes_scope(alias),)).fetchone()[0])
    assert observation['artifact'] == exact
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone()) == before


def test_wrong_originals_do_not_starve_later_recoverable_page(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    seed = cnyes_url(BASE, start=1388534400, end=1388534499)
    capture(queue, seed, source_payload([], epoch=bounds(seed)[0]))
    wrong = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])['artifact']
    urls = [cnyes_url(BASE, start=1388534500 + i * 100, end=1388534599 + i * 100) for i in range(5)]
    with monkeypatch.context() as patch:
        patch.setattr(history, 'retain_response', lambda *args, **kwargs: wrong)
        for url in urls:
            capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    source = queue.registry.sources['cnyes']
    exact = history.retain_response(queue, response(source_payload([], epoch=bounds(urls[-1])[0]), urls[-1]),
                                    source, accepted=[], restricted=False)
    with queue.db:
        for url in urls:
            queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (url,))
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    assert history.audit_original_envelopes(queue, limit=100, max_seconds=30)['mismatched'] == len(urls)
    for _ in range(16):
        history.backfill(queue, limit=1, max_seconds=30)
        if head(queue, urls[-1])['state'] == 'list_complete':
            break
    assert head(queue, urls[-1])['state'] == 'list_complete'
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations '
                                              'WHERE scope_url=?', (cnyes_scope(urls[-1]),)).fetchone()[0])
    assert observation['artifact'] == exact
    assert [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before


@pytest.mark.parametrize('rate_limited', [False, True])
def test_legacy_original_audit_recovers_alias_when_original_head_was_replaced(queue, monkeypatch, rate_limited):
    from downloader import news_fulltext_cnyes_history as history

    exemplar, alias = seed_equivalent(queue)
    low = bounds(exemplar)[0]
    first = source_payload(range(1, 31), epoch=low, total=31)
    wrong_url = cnyes_url(BASE, start=low + 301, end=low + 400)
    wrong = history.retain_response(queue, response(first, wrong_url), queue.registry.sources['cnyes'],
                                    accepted=[], restricted=False)
    with monkeypatch.context() as patch:
        patch.setattr(history, 'retain_response', lambda *args, **kwargs: wrong)
        capture(queue, exemplar, first)
    capture(queue, cnyes_url(BASE, 2, start=bounds(exemplar)[0], end=bounds(exemplar)[1]),
            source_payload([31], epoch=low, page=2, total=31))
    assert head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence'
    with queue.db:
        queue.db.execute('DELETE FROM cnyes_history_page_heads WHERE scope_url=? AND page=1', (cnyes_scope(exemplar),))
        if rate_limited:
            queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3 WHERE url=?", (alias,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone())
    result = history.audit_original_envelopes(queue, limit=20, max_seconds=10)
    assert result['complete'] and result['mismatched'] >= 1
    assert head(queue, alias)['state'] == 'waiting_original_page_recheck'
    after = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone())
    if rate_limited:
        assert after == before
    else:
        assert after['due'] < before['due'] and after['error_code'] is None
        capture(queue, alias, first)
        assert head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence'


def test_legacy_original_audit_replays_alias_local_original_before_rechecking(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    exemplar, alias = seed_equivalent(queue)
    low = bounds(exemplar)[0]
    first = source_payload(range(1, 31), epoch=low, total=31)
    source = queue.registry.sources['cnyes']
    exact_response = response(first, alias)
    exact = history.retain_response(queue, exact_response, source, accepted=[], restricted=False)
    wrong_url = cnyes_url(BASE, start=low + 301, end=low + 400)
    wrong = history.retain_response(queue, response(first, wrong_url), source, accepted=[], restricted=False)
    with monkeypatch.context() as patch:
        patch.setattr(history, 'retain_response', lambda *args, **kwargs: wrong)
        capture(queue, exemplar, first)
    capture(queue, cnyes_url(BASE, 2, start=bounds(exemplar)[0], end=bounds(exemplar)[1]),
            source_payload([31], epoch=low, page=2, total=31))
    with queue.db:
        queue.db.execute('DELETE FROM cnyes_history_page_heads WHERE scope_url=? AND page=1', (cnyes_scope(exemplar),))
        # Model a legacy shared head whose exemplar has no indexed accepted
        # batch; the independent alias original is the only local proof.
        queue.db.execute('DELETE FROM cnyes_history_accepted_batch_candidates')
        queue.db.execute('DELETE FROM accepted_batches')
        queue.db.execute('UPDATE discovery SET response_sha256=?,last_checked_at_utc=? WHERE url=?',
                         (hashlib.sha256(exact_response.body).hexdigest(), '2020-01-01T00:00:00+00:00', alias))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone())
    assert history.audit_original_envelopes(queue, limit=20, max_seconds=10)['complete']
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (alias,)).fetchone()[0] == 'pending'
    import pyarrow.parquet as pq
    original_clock = pq.read_table(queue.root / exact['parquet_path']).to_pylist()[0]['captured_at_utc']
    newer_head_clock = queue.db.execute('SELECT last_confirmed_at_utc FROM cnyes_history_page_heads '
                                        'WHERE scope_url=? AND page=1', (cnyes_scope(alias),)).fetchone()[0]
    assert datetime.fromisoformat(original_clock) < datetime.fromisoformat(newer_head_clock)
    for _ in range(8):
        history.backfill(queue, limit=20, max_seconds=10)
        if head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence':
            break
    assert head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence'
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations WHERE scope_url=?',
                                              (cnyes_scope(alias),)).fetchone()[0])
    assert observation['artifact'] == exact and observation['origin'] == 'verified_retained_source_response'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone()) == before


def test_unavailable_original_alias_recovers_its_own_local_page(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    exemplar, alias = seed_equivalent(queue)
    low = bounds(exemplar)[0]
    first = source_payload(range(1, 31), epoch=low, total=31)
    source = queue.registry.sources['cnyes']
    exact_response = response(first, alias)
    exact = history.retain_response(queue, exact_response, source, accepted=[], restricted=False)
    exemplar_original = history.retain_response(queue, response(first, exemplar), source, accepted=[], restricted=False)
    with monkeypatch.context() as patch:
        patch.setattr(history, 'retain_response', lambda *args, **kwargs: exemplar_original)
        capture(queue, exemplar, first)
    capture(queue, cnyes_url(BASE, 2, start=bounds(exemplar)[0], end=bounds(exemplar)[1]),
            source_payload([31], epoch=low, page=2, total=31))
    assert head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence'
    with queue.db:
        queue.db.execute('DELETE FROM cnyes_history_page_heads WHERE scope_url=? AND page=1', (cnyes_scope(exemplar),))
        queue.db.execute('DELETE FROM cnyes_history_accepted_batch_candidates')
        queue.db.execute('DELETE FROM accepted_batches')
        queue.db.execute('UPDATE discovery SET response_sha256=?,last_checked_at_utc=? WHERE url=?',
                         (hashlib.sha256(exact_response.body).hexdigest(), '2020-01-01T00:00:00+00:00', alias))
    (queue.root / exemplar_original['parquet_path']).rename(queue.root / 'exemplar-held.parquet')
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone())
    history.audit_original_envelopes(queue, limit=20, max_seconds=10)
    assert head(queue, alias)['state'] == 'waiting_original_page_recheck'
    work = queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (alias,)).fetchone()
    assert work and work[0] == 'pending'
    for _ in range(8):
        history.backfill(queue, limit=20, max_seconds=10)
        if head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence':
            break
    assert head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence'
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations WHERE scope_url=?',
                                              (cnyes_scope(alias),)).fetchone()[0])
    assert observation['artifact'] == exact and observation['origin'] == 'verified_retained_source_response'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone()) == before


def test_unverified_alias_does_not_convert_older_revision_into_local_only_work(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    exemplar, alias = seed_equivalent(queue)
    low = bounds(exemplar)[0]
    first = source_payload(range(1, 31), epoch=low, total=31)
    source = queue.registry.sources['cnyes']
    exact_response = response(first, alias)
    history.retain_response(queue, exact_response, source, accepted=[], restricted=False)
    exemplar_original = history.retain_response(queue, response(first, exemplar), source, accepted=[], restricted=False)
    with monkeypatch.context() as patch:
        patch.setattr(history, 'retain_response', lambda *args, **kwargs: exemplar_original)
        capture(queue, exemplar, first)
    capture(queue, cnyes_url(BASE, 2, start=bounds(exemplar)[0], end=bounds(exemplar)[1]),
            source_payload([31], epoch=low, page=2, total=31))
    evidence = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=1',
                                (cnyes_scope(alias),)).fetchone()
    old_sha = hashlib.sha256(b'older revision').hexdigest()
    with queue.db:
        queue.db.execute('DELETE FROM cnyes_history_page_heads WHERE scope_url=? AND page=1', (cnyes_scope(exemplar),))
        queue.db.execute('DELETE FROM cnyes_history_accepted_batch_candidates')
        queue.db.execute('DELETE FROM accepted_batches')
        queue.db.execute("UPDATE discovery SET response_sha256=?,last_checked_at_utc=?,error_code='http_429',attempts=3 WHERE url=?",
                         (hashlib.sha256(exact_response.body).hexdigest(), '2020-01-01T00:00:00+00:00', alias))
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
            'VALUES (?,?,?,?,?,?,?)', (alias, cnyes_scope(alias), 1, old_sha, evidence['total'],
                                       evidence['ids_sha256'], evidence['observed_at_utc']))
    (queue.root / exemplar_original['parquet_path']).rename(queue.root / 'exemplar-held.parquet')
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone())
    history.audit_original_envelopes(queue, limit=20, max_seconds=10)
    history.backfill(queue, limit=20, max_seconds=10)
    work = queue.db.execute('SELECT state,response_sha256 FROM cnyes_history_retained_work WHERE request_url=?', (alias,)).fetchone()
    assert tuple(work) == ('pending', hashlib.sha256(exact_response.body).hexdigest())
    archived = json.loads(queue.db.execute('SELECT original_work_json FROM cnyes_history_retained_revision_audit '
                                           'WHERE request_url=?', (alias,)).fetchone()[0])
    assert archived['work']['response_sha256'] == old_sha and archived['work']['state'] == 'retained_unavailable'
    for _ in range(8):
        history.backfill(queue, limit=20, max_seconds=10)
        if head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence':
            break
    assert head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone()) == before


@pytest.mark.parametrize('prior_state', ['pending', 'retained_unavailable'])
def test_wrong_original_alias_waits_for_older_revision_then_recovers_local_page(queue, monkeypatch, prior_state):
    from downloader import news_fulltext_cnyes_history as history

    exemplar, alias = seed_equivalent(queue)
    low = bounds(exemplar)[0]
    first = source_payload(range(1, 31), epoch=low, total=31)
    source = queue.registry.sources['cnyes']
    exact_response = response(first, alias)
    exact = history.retain_response(queue, exact_response, source, accepted=[], restricted=False)
    wrong_url = cnyes_url(BASE, start=low + 301, end=low + 400)
    wrong = history.retain_response(queue, response(first, wrong_url), source, accepted=[], restricted=False)
    with monkeypatch.context() as patch:
        patch.setattr(history, 'retain_response', lambda *args, **kwargs: wrong)
        capture(queue, exemplar, first)
    capture(queue, cnyes_url(BASE, 2, start=bounds(exemplar)[0], end=bounds(exemplar)[1]),
            source_payload([31], epoch=low, page=2, total=31))
    evidence = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=1',
                                (cnyes_scope(alias),)).fetchone()
    old_sha = hashlib.sha256(b'older revision').hexdigest()
    with queue.db:
        queue.db.execute('DELETE FROM cnyes_history_page_heads WHERE scope_url=? AND page=1', (cnyes_scope(exemplar),))
        queue.db.execute('DELETE FROM cnyes_history_accepted_batch_candidates')
        queue.db.execute('DELETE FROM accepted_batches')
        queue.db.execute("UPDATE discovery SET response_sha256=?,last_checked_at_utc=?,error_code='http_429',attempts=3 WHERE url=?",
                         (hashlib.sha256(exact_response.body).hexdigest(), '2020-01-01T00:00:00+00:00', alias))
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc,state) '
            'VALUES (?,?,?,?,?,?,?,?)', (alias, cnyes_scope(alias), 1, old_sha, evidence['total'],
                                         evidence['ids_sha256'], evidence['observed_at_utc'], prior_state))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone())
    history.audit_original_envelopes(queue, limit=20, max_seconds=10)
    assert head(queue, alias)['state'] == 'waiting_original_page_recheck'
    if prior_state == 'pending':
        history.backfill(queue, limit=20, max_seconds=10)
    work = queue.db.execute('SELECT state,response_sha256 FROM cnyes_history_retained_work WHERE request_url=?', (alias,)).fetchone()
    assert tuple(work) == ('pending', hashlib.sha256(exact_response.body).hexdigest())
    archived = json.loads(queue.db.execute('SELECT original_work_json FROM cnyes_history_retained_revision_audit '
                                           'WHERE request_url=?', (alias,)).fetchone()[0])
    assert archived['work']['response_sha256'] == old_sha and archived['work']['state'] == 'retained_unavailable'
    for _ in range(8):
        history.backfill(queue, limit=20, max_seconds=10)
        if head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence':
            break
    assert head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence'
    restored = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations WHERE scope_url=?',
                                           (cnyes_scope(alias),)).fetchone()[0])
    assert restored['artifact'] == exact and restored['origin'] == 'verified_retained_source_response'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone()) == before


@pytest.mark.parametrize('prior_state', ['pending', 'retained_unavailable'])
def test_wrong_direct_original_recovers_after_older_revision(queue, monkeypatch, prior_state):
    from downloader import news_fulltext_cnyes_history as history

    url, other = [cnyes_url(BASE, start=start, end=start + 100) for start in (1388534400, 1388534600)]
    data = source_payload([], epoch=bounds(url)[0])
    r = response(data, url)
    source = queue.registry.sources['cnyes']
    exact = history.retain_response(queue, r, source, accepted=[], restricted=False)
    wrong = history.retain_response(queue, response(data, other), source, accepted=[], restricted=False)
    with monkeypatch.context() as patch:
        patch.setattr(history, 'retain_response', lambda *args, **kwargs: wrong)
        capture(queue, url, data)
    info, _ = cnyes_pagination(r)
    old_sha = hashlib.sha256(b'older revision').hexdigest()
    with queue.db:
        queue.db.execute("UPDATE discovery SET due=9e9,error_code='http_429',attempts=3 WHERE url=?", (url,))
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc,state) '
            'VALUES (?,?,?,?,?,?,?,?)', (url, cnyes_scope(url), 1, old_sha, info['total'], info['ids_sha256'],
                                         '2020-01-01T00:00:00+00:00', prior_state))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    history.audit_original_envelopes(queue, limit=20, max_seconds=10)
    if prior_state == 'pending':
        history.backfill(queue, limit=20, max_seconds=10)
    work = queue.db.execute('SELECT state,response_sha256 FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()
    assert tuple(work) == ('pending', hashlib.sha256(r.body).hexdigest())
    archived = json.loads(queue.db.execute('SELECT original_work_json FROM cnyes_history_retained_revision_audit '
                                           'WHERE request_url=?', (url,)).fetchone()[0])
    assert archived['work']['response_sha256'] == old_sha and archived['work']['state'] == 'retained_unavailable'
    for _ in range(8):
        history.backfill(queue, limit=20, max_seconds=10)
        if head(queue, url)['state'] == 'list_complete':
            break
    assert head(queue, url)['state'] == 'list_complete'
    restored = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    assert restored['artifact'] == exact and restored['origin'] == 'verified_retained_source_response'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_legacy_original_audit_retries_missing_local_file_without_waking_source(queue):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    artifact = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])['artifact']
    original = queue.root / artifact['parquet_path']
    held = original.with_suffix('.held')
    original.rename(held)
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (url,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    result = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1000)
    assert result['complete'] and result['unverified'] == 1 and result['next_epoch'] == 4600
    assert head(queue, url)['state'] == 'waiting_original_page_recheck'
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_retained_work').fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    held.rename(original)
    result = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=4600)
    assert result['complete'] and result['unverified'] == 0 and result['next_epoch'] == 1000 + 86400
    assert head(queue, url)['state'] == 'list_complete'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_clean_original_audit_rechecks_new_heads_and_later_missing_files(queue):
    from downloader import news_fulltext_cnyes_history as history

    first, later = [cnyes_url(BASE, start=start, end=start + 100)
                    for start in (1388534400, 1388534600)]
    capture(queue, first, source_payload([], epoch=bounds(first)[0]))
    initial = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1000)
    assert initial['complete'] and initial['checked'] == 1
    assert initial['next_epoch'] == 1000 + 86400
    projected = history.progress(queue.db, queue.registry.sources['cnyes'])['original_envelope_audit']
    assert projected['next_audit_at_utc'] == datetime.fromtimestamp(87400, timezone.utc).isoformat()

    capture(queue, later, source_payload([], epoch=bounds(later)[0]))
    observation = json.loads(queue.db.execute(
        'SELECT observation_json FROM cnyes_history_page_observations WHERE scope_url=?',
        (cnyes_scope(later),)).fetchone()[0])
    original = queue.root / observation['artifact']['parquet_path']
    held = original.with_suffix('.held')
    original.rename(held)
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (later,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (later,)).fetchone())

    waiting = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1001)
    assert waiting['complete'] and waiting['checked'] == 0 and waiting['next_epoch'] == 87400
    missing = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=87400)
    assert missing['complete'] and missing['checked'] == 2 and missing['unverified'] == 1
    assert missing['next_epoch'] == 87400 + history.INTEGRITY_INTERVAL_SECONDS
    assert head(queue, later)['state'] == 'waiting_original_page_recheck'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (later,)).fetchone()) == before

    held.rename(original)
    recovered = history.audit_original_envelopes(queue, limit=20, max_seconds=10,
                                                 now_epoch=87400 + history.INTEGRITY_INTERVAL_SECONDS)
    assert recovered['complete'] and recovered['checked'] == 1 and recovered['unverified'] == 0
    assert recovered['next_epoch'] == 87400 + 86400
    assert head(queue, later)['state'] == 'list_complete'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (later,)).fetchone()) == before


def test_legacy_clean_original_audit_checkpoint_rechecks_after_restart(queue):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    source = queue.registry.sources['cnyes']
    assert history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1000)['complete']
    key = history.original_envelope_audit_key(source)
    checkpoint = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0])
    checkpoint['next_epoch'] = 0  # Old clean checkpoints had no revisit deadline.
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps(checkpoint), key))
    artifact = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])['artifact']
    (queue.root / artifact['parquet_path']).rename(queue.root / 'original-held.parquet')
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (url,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        result = history.audit_original_envelopes(resumed, limit=20, max_seconds=10, now_epoch=1001)
        assert result['complete'] and result['checked'] == 1 and result['unverified'] == 1
        assert result['next_epoch'] == 1001 + history.INTEGRITY_INTERVAL_SECONDS
        assert head(resumed, url)['state'] == 'waiting_original_page_recheck'
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_original_audit_retries_only_unverified_heads_and_keeps_full_deadline(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    urls = [cnyes_url(BASE, start=1388534400 + i * 100, end=1388534499 + i * 100) for i in range(12)]
    for url in urls:
        capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    missing = urls[7]
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations '
        'WHERE scope_url=?', (cnyes_scope(missing),)).fetchone()[0])
    original = queue.root / observation['artifact']['parquet_path']
    held = original.with_suffix('.held')
    original.rename(held)
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (missing,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (missing,)).fetchone())

    first = history.audit_original_envelopes(queue, limit=50, max_seconds=30, now_epoch=1000)
    assert first['complete'] and first['checked'] == 12 and first['unverified'] == 1
    assert first['next_epoch'] == 4600
    original_verifier = queue._cnyes_original_envelope_audit_cache.check
    checked = []
    def count_verifications(*args, **kwargs):
        checked.append(args[2])
        return original_verifier(*args, **kwargs)
    monkeypatch.setattr(queue._cnyes_original_envelope_audit_cache, 'check', count_verifications)

    waiting = history.audit_original_envelopes(queue, limit=50, max_seconds=30, now_epoch=4599)
    assert waiting['checked'] == 0 and waiting['next_epoch'] == 4600 and checked == []
    retry = history.audit_original_envelopes(queue, limit=50, max_seconds=30, now_epoch=4600)
    assert retry['complete'] and retry['checked'] == 1 and retry['unverified'] == 1
    assert retry['next_epoch'] == 8200 and checked == [missing]
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (missing,)).fetchone()) == before

    held.rename(original)
    checked.clear()
    recovered = history.audit_original_envelopes(queue, limit=50, max_seconds=30, now_epoch=8200)
    assert recovered['complete'] and recovered['checked'] == 1 and recovered['unverified'] == 0
    assert recovered['next_epoch'] == 87400 and checked == [missing]
    assert head(queue, missing)['state'] == 'list_complete'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (missing,)).fetchone()) == before
    progress = history.progress(queue.db, queue.registry.sources['cnyes'])['original_envelope_audit']
    assert progress['retry_pending_pages'] == 0
    assert progress['next_audit_at_utc'] == datetime.fromtimestamp(87400, timezone.utc).isoformat()


def test_original_audit_isolates_symlink_cycle_and_rechecks_after_repair(queue):
    from downloader import news_fulltext_cnyes_history as history

    urls = [cnyes_url(BASE, start=start, end=start + 99)
            for start in (1388534400, 1388534600)]
    for url in urls:
        capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    observation = json.loads(queue.db.execute(
        'SELECT observation_json FROM cnyes_history_page_observations WHERE scope_url=?',
        (cnyes_scope(urls[0]),)).fetchone()[0])
    original = queue.root / observation['artifact']['parquet_path']
    held = original.with_suffix('.held')
    original.rename(held)
    original.symlink_to(original.name)
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (urls[0],))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[0],)).fetchone())

    result = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1000)
    assert result['complete'] and result['checked'] == 2 and result['unverified'] == 1
    assert result['next_epoch'] == 4600
    assert head(queue, urls[1])['state'] == 'list_complete'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[0],)).fetchone()) == before

    original.unlink()
    held.rename(original)
    recovered = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=4600)
    assert recovered['complete'] and recovered['checked'] == 1 and recovered['unverified'] == 0
    assert head(queue, urls[0])['state'] == 'list_complete'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[0],)).fetchone()) == before


def test_original_retry_survives_restart_and_never_resets_source_429(queue):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    artifact = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])['artifact']
    original = queue.root / artifact['parquet_path']
    held = original.with_suffix('.held')
    original.rename(held)
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (url,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    assert history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1000)['next_epoch'] == 4600
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        assert history.progress(resumed.db, resumed.registry.sources['cnyes'])['original_envelope_audit']['retry_pending_pages'] == 1
        held.rename(original)
        result = history.audit_original_envelopes(resumed, limit=20, max_seconds=10, now_epoch=4600)
        assert result['checked'] == 1 and result['next_epoch'] == 87400
        assert head(resumed, url)['state'] == 'list_complete'
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_original_retry_runs_inside_canonical_backfill_owner(queue):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    original = queue.root / observation['artifact']['parquet_path']
    held = original.with_suffix('.held')
    original.rename(held)
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (url,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    assert history.audit_original_envelopes(queue, limit=20, max_seconds=10)['unverified'] == 1
    key = history.original_envelope_audit_key(queue.registry.sources['cnyes'])
    daily_deadline = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0])['next_epoch']
    held.rename(original)
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_original_file_retries SET not_before_epoch=0 WHERE audit_key=?', (key,))
    result = history.backfill(queue, limit=20, max_seconds=10)
    original_result = result['original_envelope_audit']
    assert original_result['complete'] and original_result['checked'] == 1
    assert original_result['next_epoch'] == daily_deadline
    assert head(queue, url)['state'] == 'list_complete'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


@pytest.mark.parametrize('damage', ['corrupt', 'missing'])
def test_original_audit_marks_broken_page_metadata_unverified_and_recovers(queue, damage):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    observation = dict(queue.db.execute('SELECT * FROM cnyes_history_page_observations').fetchone())
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (url,))
        if damage == 'corrupt':
            queue.db.execute("UPDATE cnyes_history_page_observations SET observation_json='damaged' WHERE observation_sha256=?",
                             (observation['observation_sha256'],))
        else:
            queue.db.execute('DELETE FROM cnyes_history_page_observations WHERE observation_sha256=?',
                             (observation['observation_sha256'],))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    result = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1000)
    assert result['complete'] and result['checked'] == 1 and result['unverified'] == 1 and result['next_epoch'] == 4600
    assert head(queue, url)['state'] == 'invalid_page_evidence'
    summary = history.progress(queue.db, queue.registry.sources['cnyes'])['original_envelope_audit']
    assert summary['out_of_scope_pages'] == 0 and summary['unverified_pages'] == 1 and summary['retry_pending_pages'] == 1
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    with queue.db:
        if damage == 'corrupt':
            queue.db.execute('UPDATE cnyes_history_page_observations SET observation_json=? WHERE observation_sha256=?',
                             (observation['observation_json'], observation['observation_sha256']))
        else:
            queue.db.execute('INSERT INTO cnyes_history_page_observations VALUES (?,?,?,?,?,?,?,?)', tuple(observation.values()))
    recovered = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=4600)
    assert recovered['complete'] and recovered['checked'] == 1 and recovered['unverified'] == 0
    assert recovered['next_epoch'] == 87400 and head(queue, url)['state'] == 'list_complete'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_original_audit_does_not_parse_an_absent_unproven_marker(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    original_loads = json.loads
    empty_inputs = []

    def measured_loads(value, *args, **kwargs):
        if value is None:
            empty_inputs.append(value)
        return original_loads(value, *args, **kwargs)

    monkeypatch.setattr(history.json, 'loads', measured_loads)
    result = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1000)
    assert result['complete'] and result['checked'] == 1 and result['unverified'] == 0
    assert empty_inputs == []


@pytest.mark.parametrize('change', ['endpoint', 'authorization', 'bulk_fingerprint'])
def test_original_audit_does_not_repair_a_previous_source_contract(queue, change):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    before_scope = head(queue, url)
    before_job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    source = queue.registry.sources['cnyes']
    if change == 'endpoint':
        source = replace(source, cnyes_apis=('https://api.cnyes.com/other',))
    elif change == 'authorization':
        source = replace(source, authorization_fingerprint='new_authorization')
    else:
        source = replace(source, cnyes_bulk_data_fingerprint='new_fingerprint')
    queue.registry.sources['cnyes'] = source
    result = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1000)
    summary = history.progress(queue.db, source)['original_envelope_audit']
    assert result['complete'] and result['checked'] == 1 and result['unverified'] == 0
    assert summary['out_of_scope_pages'] == 1 and summary['retry_pending_pages'] == 0
    assert head(queue, url) == before_scope
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before_job


def test_original_audit_unknown_old_metadata_does_not_rewrite_foreign_scope(queue):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    before_scope = head(queue, url)
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_page_observations SET observation_json='damaged'")
    source = replace(queue.registry.sources['cnyes'], authorization_fingerprint='new_authorization')
    queue.registry.sources['cnyes'] = source
    result = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1000)
    summary = history.progress(queue.db, source)['original_envelope_audit']
    assert result['unverified'] == 1 and summary['out_of_scope_pages'] == 0
    assert summary['retry_pending_pages'] == 1 and head(queue, url) == before_scope


def test_original_audit_keeps_foreign_scope_original_marker(queue):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    sha = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_scope_heads SET authorization_fingerprint='foreign'")
        queue.db.execute('INSERT INTO cnyes_history_unproven_pages VALUES (?,?,?)',
                         (cnyes_scope(url), 1, json.dumps({'reason': 'original_envelope_invalid',
                                                         'actual_observation_sha256': sha})))
    before_scope = head(queue, url)
    result = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1000)
    assert result['complete'] and result['unverified'] == 0 and head(queue, url) == before_scope
    assert queue.db.execute('SELECT 1 FROM cnyes_history_unproven_pages').fetchone()


def test_original_audit_reconciles_recovered_metadata_outside_file_audit(queue):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    row = dict(queue.db.execute('SELECT * FROM cnyes_history_page_observations').fetchone())
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (url,))
        queue.db.execute("UPDATE cnyes_history_page_observations SET observation_json='damaged'")
    before_job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    assert history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1000)['unverified'] == 1
    assert head(queue, url)['state'] == 'invalid_page_evidence'
    observation = json.loads(row['observation_json'])
    observation['artifact'] = {'kind': 'accepted_body_batch'}
    observation['response_sha256'] = 'a' * 64
    row['response_sha256'] = observation['response_sha256']
    row['observation_json'] = history.encoded(observation)
    row['observation_sha256'] = history.digest(observation)
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_page_observations VALUES (?,?,?,?,?,?,?,?)', tuple(row.values()))
        queue.db.execute('UPDATE cnyes_history_page_heads SET observation_sha256=?', (row['observation_sha256'],))
    assert not history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=4600)['complete']
    result = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=4600)
    assert result['complete'] and result['unverified'] == 0 and head(queue, url)['state'] == 'list_complete'
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_original_file_retries').fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before_job


@pytest.mark.parametrize('new_head', [True, False])
def test_original_audit_head_change_during_slice_is_not_reported_as_outside(queue, monkeypatch, new_head):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    old_sha = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    capture(queue, url, source_payload([1], epoch=bounds(url)[0]))
    new_sha = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    assert new_sha != old_sha
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_page_heads SET observation_sha256=?', (old_sha,))
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (url,))
    before_job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    original_check = history._history_scan_slice_done
    changed = False

    def advance_head(*args):
        nonlocal changed
        if not changed:
            changed = True
            if new_head:
                queue.db.execute('UPDATE cnyes_history_page_heads SET observation_sha256=?', (new_sha,))
            else:
                queue.db.execute('DELETE FROM cnyes_history_page_heads')
        return original_check(*args)

    monkeypatch.setattr(history, '_history_scan_slice_done', advance_head)
    result = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1000)
    summary = history.progress(queue.db, queue.registry.sources['cnyes'])['original_envelope_audit']
    assert result['checked'] == 1 and result['unverified'] == 1
    assert summary['out_of_scope_pages'] == 0 and summary['unverified_pages'] == 1
    if new_head:
        assert not result['complete'] and summary['retry_pending_pages'] == 1
        recovered = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1000)
        assert recovered['complete'] and recovered['checked'] == 1
        assert not queue.db.execute('SELECT 1 FROM cnyes_history_original_file_retries').fetchone()
    else:
        assert result['complete'] and head(queue, url)['state'] == 'waiting_page_evidence'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before_job


def test_original_audit_migrates_partial_classification_without_discarding_checkpoint(queue):
    from downloader import news_fulltext_cnyes_history as history

    urls = [cnyes_url(BASE, start=1388534400 + i * 100, end=1388534499 + i * 100) for i in range(2)]
    for url in urls:
        capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    first_url = min(urls, key=cnyes_scope)
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_page_observations SET observation_json='damaged' WHERE scope_url=?",
                         (cnyes_scope(first_url),))
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3,due=9e9 WHERE url=?", (first_url,))
    assert history.audit_original_envelopes(queue, limit=1, max_seconds=10, now_epoch=1000)['unverified'] == 1
    source = queue.registry.sources['cnyes']
    key = history.original_envelope_audit_key(source)
    checkpoint = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0])
    checkpoint.pop('classification_version')
    checkpoint['unverified'] = 0  # The older classifier treated this row as outside.
    legacy = json.dumps(checkpoint)
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (legacy, key))
        queue.db.execute('DELETE FROM cnyes_history_original_file_retries WHERE audit_key=?', (key,))
    resumed = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1001)
    assert resumed['complete'] and resumed['checked'] == 2 and resumed['unverified'] == 1
    assert resumed['next_epoch'] == 4601
    assert queue.db.execute('SELECT original_value FROM cnyes_history_integrity_checkpoint_audit '
                            'WHERE checkpoint_key=?', (key,)).fetchone()[0] == legacy
    summary = history.progress(queue.db, source)['original_envelope_audit']
    assert summary['unverified_pages'] == 1 and summary['out_of_scope_pages'] == 0


def test_original_retry_migrates_legacy_partial_checkpoint_without_losing_prior_missing(queue):
    from downloader import news_fulltext_cnyes_history as history

    urls = [cnyes_url(BASE, start=1388534400 + i * 100, end=1388534499 + i * 100) for i in range(2)]
    for url in urls:
        capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    first_url = min(urls, key=cnyes_scope)
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations '
        'WHERE scope_url=?', (cnyes_scope(first_url),)).fetchone()[0])
    (queue.root / observation['artifact']['parquet_path']).rename(queue.root / 'missing-held.parquet')
    assert history.audit_original_envelopes(queue, limit=1, max_seconds=10, now_epoch=1000)['checked'] == 1
    key = history.original_envelope_audit_key(queue.registry.sources['cnyes'])
    checkpoint = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0])
    checkpoint.pop('retry_index_ready')
    legacy = json.dumps(checkpoint)
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (legacy, key))
        queue.db.execute('DELETE FROM cnyes_history_original_file_retries WHERE audit_key=?', (key,))
    resumed = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1001)
    assert resumed['complete'] and resumed['checked'] == 2 and resumed['unverified'] == 1
    assert resumed['next_epoch'] == 4601
    assert queue.db.execute('SELECT original_value FROM cnyes_history_integrity_checkpoint_audit '
                            'WHERE checkpoint_key=?', (key,)).fetchone()[0] == legacy
    assert history.progress(queue.db, queue.registry.sources['cnyes'])['original_envelope_audit']['retry_pending_pages'] == 1


def test_original_retry_migrates_legacy_complete_missing_checkpoint_at_original_deadline(queue):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    (queue.root / observation['artifact']['parquet_path']).rename(queue.root / 'held-original.parquet')
    assert history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1000)['unverified'] == 1
    source = queue.registry.sources['cnyes']
    key = history.original_envelope_audit_key(source)
    checkpoint = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0])
    checkpoint.pop('retry_index_ready')
    checkpoint['next_epoch'] = 4600  # Prior versions repeated the whole missing-file scan hourly.
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps(checkpoint), key))
        queue.db.execute('DELETE FROM cnyes_history_original_file_retries WHERE audit_key=?', (key,))
    assert history.progress(queue.db, source)['original_envelope_audit']['retry_pending_pages'] is None
    assert history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=4599)['checked'] == 0
    rebuilt = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=4600)
    assert rebuilt['complete'] and rebuilt['checked'] == 1 and rebuilt['unverified'] == 1
    assert rebuilt['next_epoch'] == 8200
    assert history.progress(queue.db, source)['original_envelope_audit']['retry_pending_pages'] == 1


def test_original_retry_alternates_with_due_full_scan_across_restart(queue):
    from downloader import news_fulltext_cnyes_history as history

    urls = [cnyes_url(BASE, start=1388534400 + i * 100, end=1388534499 + i * 100) for i in range(2)]
    for index, url in enumerate(urls):
        capture(queue, url, source_payload([], epoch=bounds(url)[0]))
        observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations '
            'WHERE scope_url=?', (cnyes_scope(url),)).fetchone()[0])
        (queue.root / observation['artifact']['parquet_path']).rename(queue.root / f'held-{index}.parquet')
    assert history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1000)['unverified'] == 2
    key = history.original_envelope_audit_key(queue.registry.sources['cnyes'])
    assert history.audit_original_envelopes(queue, limit=1, max_seconds=10, now_epoch=87400)['checked'] == 1
    before = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0])
    assert before['state'] == 'complete' and before['next_epoch'] == 87400
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        result = history.audit_original_envelopes(resumed, limit=1, max_seconds=10, now_epoch=87400)
        after = json.loads(resumed.db.execute('SELECT value FROM queue_settings WHERE key=?', (key,)).fetchone()[0])
        assert result['checked'] == 1 and after['state'] == 'scanning' and after['checked'] == 1


def test_original_retry_moves_stale_identity_to_current_head(queue):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    old = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    (queue.root / old['artifact']['parquet_path']).rename(queue.root / 'held-old.parquet')
    assert history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1000)['unverified'] == 1
    old_sha = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    capture(queue, url, source_payload([1], epoch=bounds(url)[0]))
    current_sha = queue.db.execute('SELECT observation_sha256 FROM cnyes_history_page_heads').fetchone()[0]
    assert current_sha != old_sha
    first = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=4600)
    assert first['checked'] == 1 and not first['complete']
    assert queue.db.execute('SELECT observation_sha256 FROM cnyes_history_original_file_retries').fetchone()[0] == current_sha
    second = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=4600)
    assert second['checked'] == 1 and second['complete'] and second['next_epoch'] == 87400
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_original_file_retries').fetchone()
    assert queue.db.execute('SELECT 1 FROM cnyes_history_page_observations WHERE observation_sha256=?', (old_sha,)).fetchone()


def test_original_retry_keeps_corrupt_metadata_and_uses_bounded_index(queue):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    (queue.root / observation['artifact']['parquet_path']).rename(queue.root / 'held-original.parquet')
    assert history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1000)['unverified'] == 1
    key = history.original_envelope_audit_key(queue.registry.sources['cnyes'])
    query = ('SELECT r.scope_url FROM cnyes_history_original_file_retries r '
             'LEFT JOIN cnyes_history_page_heads h ON h.scope_url=r.scope_url AND h.page=r.page '
             'WHERE r.audit_key=? AND r.not_before_epoch<=? '
             'ORDER BY r.not_before_epoch,r.scope_url,r.page,r.observation_sha256 LIMIT ?')
    plan = ' '.join(row[3] for row in queue.db.execute('EXPLAIN QUERY PLAN ' + query, (key, 4600, 10)))
    assert 'cnyes_history_original_file_retry_due' in plan and 'TEMP B-TREE' not in plan
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_page_observations SET observation_json='damaged' WHERE scope_url=?",
                         (cnyes_scope(url),))
    result = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=4600)
    assert result['complete'] and result['checked'] == 1 and result['next_epoch'] == 8200
    retry = queue.db.execute('SELECT not_before_epoch,attempts FROM cnyes_history_original_file_retries').fetchone()
    assert tuple(retry) == (8200, 1)
    assert queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0] == 'damaged'


@pytest.mark.parametrize('bad', [float('inf'), float('nan'), -1, True, 1e18])
def test_original_retry_rejects_invalid_clock_without_state_mutation(queue, bad):
    from downloader import news_fulltext_cnyes_history as history

    with pytest.raises(ValueError, match='invalid original envelope audit clock'):
        history.audit_original_envelopes(queue, now_epoch=bad)
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_original_file_retries').fetchone()


@pytest.mark.parametrize('damage', ['missing', 'corrupt'])
@pytest.mark.parametrize('rate_limited', [False, True])
def test_legacy_original_audit_recovers_a_damaged_head_from_an_exact_local_copy(queue, damage, rate_limited):
    from downloader import news_fulltext_cnyes_history as history
    import pyarrow.parquet as pq

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    r = capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    old = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    original = queue.root / old['artifact']['parquet_path']
    held = original.with_suffix('.held')
    if damage == 'missing':
        original.rename(held)
    else:
        original.write_bytes(b'corrupt-original-evidence')
    exact = history.retain_response(queue, r, queue.registry.sources['cnyes'], accepted=[], restricted=False)
    assert exact['parquet_path'] != old['artifact']['parquet_path']
    exact_clock = pq.read_table(queue.root / exact['parquet_path']).to_pylist()[0]['captured_at_utc']
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=9e9 WHERE url=?', (url,))
        if rate_limited:
            queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=3 WHERE url=?", (url,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())

    audited = history.audit_original_envelopes(queue, limit=20, max_seconds=10)
    assert audited['complete'] and audited['unverified'] == 1
    assert head(queue, url)['state'] == 'waiting_original_page_recheck'
    work = queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()
    assert work and work[0] == 'pending'
    for _ in range(8):
        history.backfill(queue, limit=20, max_seconds=10)
        if head(queue, url)['state'] == 'list_complete':
            break
    assert head(queue, url)['state'] == 'list_complete'
    restored = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    assert restored['artifact'] == exact
    assert restored['origin'] == 'verified_retained_source_response'
    assert restored['source_observed_at_utc'] == exact_clock
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    if damage == 'missing':
        assert held.exists()
    else:
        assert original.read_bytes() == b'corrupt-original-evidence'
    assert queue.db.execute('SELECT 1 FROM cnyes_history_corrupt_observation_audit').fetchone()


def test_original_local_copy_disappearing_during_replay_does_not_wake_source(queue):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    r = capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    original = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    (queue.root / original['artifact']['parquet_path']).rename(queue.root / 'original-held.parquet')
    exact = history.retain_response(queue, r, queue.registry.sources['cnyes'], accepted=[], restricted=False)
    exact_path = queue.root / exact['parquet_path']
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=9e9 WHERE url=?', (url,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    history.audit_original_envelopes(queue, limit=20, max_seconds=10)
    held = exact_path.with_suffix('.held')
    exact_path.rename(held)
    history.backfill(queue, limit=20, max_seconds=10)
    retry = queue.db.execute('SELECT w.state,r.error_code FROM cnyes_history_retained_work w '
        'JOIN cnyes_history_retained_local_retries r USING(request_url) WHERE w.request_url=?', (url,)).fetchone()
    assert tuple(retry) == ('local_retry', 'original_local_candidate_unavailable')
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    held.rename(exact_path)
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=0 WHERE request_url=?', (url,))
    for _ in range(8):
        history.backfill(queue, limit=20, max_seconds=10)
        if head(queue, url)['state'] == 'list_complete':
            break
    assert head(queue, url)['state'] == 'list_complete'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_original_local_repair_resumes_after_owner_restart(queue):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    r = capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    original = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    (queue.root / original['artifact']['parquet_path']).rename(queue.root / 'original-held.parquet')
    exact = history.retain_response(queue, r, queue.registry.sources['cnyes'], accepted=[], restricted=False)
    with queue.db:
        queue.db.execute("UPDATE discovery SET due=9e9,error_code='http_429',attempts=3 WHERE url=?", (url,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    history.audit_original_envelopes(queue, limit=20, max_seconds=10)
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        assert resumed.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()[0] == 'pending'
        for _ in range(8):
            history.backfill(resumed, limit=20, max_seconds=10)
            if head(resumed, url)['state'] == 'list_complete':
                break
        assert head(resumed, url)['state'] == 'list_complete'
        restored = json.loads(resumed.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
        assert restored['artifact'] == exact and restored['origin'] == 'verified_retained_source_response'
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_unverified_original_rejects_another_requests_identical_local_bytes(queue):
    from downloader import news_fulltext_cnyes_history as history

    url, other = [cnyes_url(BASE, start=start, end=start + 100) for start in (1388534400, 1388534600)]
    data = source_payload([], epoch=bounds(url)[0])
    r = capture(queue, url, data)
    original = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    (queue.root / original['artifact']['parquet_path']).rename(queue.root / 'original-held.parquet')
    wrong = history.retain_response(queue, response(data, other), queue.registry.sources['cnyes'],
                                    accepted=[], restricted=False)
    legacy = queue.root / 'quarantine' / (hashlib.sha256(r.body).hexdigest() + '.parquet')
    legacy.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(queue.root / wrong['parquet_path'], legacy)
    shutil.copyfile(queue.root / wrong['receipt_path'], legacy.with_suffix('.parquet.receipt.json'))
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=9e9 WHERE url=?', (url,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    history.audit_original_envelopes(queue, limit=20, max_seconds=10)
    history.backfill(queue, limit=20, max_seconds=10)
    retry = queue.db.execute('SELECT w.state,r.error_code FROM cnyes_history_retained_work w '
        'JOIN cnyes_history_retained_local_retries r USING(request_url) WHERE w.request_url=?', (url,)).fetchone()
    assert tuple(retry) == ('local_retry', 'original_local_candidate_unavailable')
    assert head(queue, url)['state'] == 'waiting_original_page_recheck'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_unavailable_alternate_directory_does_not_stop_original_audit(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    original = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    (queue.root / original['artifact']['parquet_path']).rename(queue.root / 'original-held.parquet')
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=9e9 WHERE url=?', (url,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    def unavailable(*args, **kwargs):
        raise PermissionError('local candidate directory unavailable')
    monkeypatch.setattr(history, '_retained_envelopes', unavailable)
    result = history.audit_original_envelopes(queue, limit=20, max_seconds=10, now_epoch=1000)
    assert result['complete'] and result['unverified'] == 1 and result['next_epoch'] == 4600
    assert head(queue, url)['state'] == 'waiting_original_page_recheck'
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_retained_candidate_directory_failure_retries_without_stopping_owner(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    r = capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    original = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    (queue.root / original['artifact']['parquet_path']).rename(queue.root / 'original-held.parquet')
    history.retain_response(queue, r, queue.registry.sources['cnyes'], accepted=[], restricted=False)
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=9e9 WHERE url=?', (url,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    history.audit_original_envelopes(queue, limit=20, max_seconds=10)
    real_candidates = history._retained_envelopes
    def unavailable(root, sha, request_url=None, authorization_fingerprint=None):
        if request_url:
            raise PermissionError('local candidate directory unavailable')
        return real_candidates(root, sha, request_url, authorization_fingerprint)
    with monkeypatch.context() as patch:
        patch.setattr(history, '_retained_envelopes', unavailable)
        history.backfill(queue, limit=20, max_seconds=10)
    retry = queue.db.execute('SELECT w.state,r.error_code FROM cnyes_history_retained_work w '
        'JOIN cnyes_history_retained_local_retries r USING(request_url) WHERE w.request_url=?', (url,)).fetchone()
    assert tuple(retry) == ('local_retry', 'local_io_unavailable')
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=0 WHERE request_url=?', (url,))
    for _ in range(8):
        history.backfill(queue, limit=20, max_seconds=10)
        if head(queue, url)['state'] == 'list_complete':
            break
    assert head(queue, url)['state'] == 'list_complete'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_new_accepted_batch_wakes_unverified_original_without_waiting_for_next_audit(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    data = source_payload([1, 2], epoch=bounds(url)[0])
    r = response(data, url)
    source = queue.registry.sources['cnyes']
    original = history.retain_response(queue, r, source, accepted=[], restricted=False)
    with monkeypatch.context() as patch:
        patch.setattr(history, 'retain_response', lambda *args, **kwargs: original)
        capture(queue, url, data)
    batch = dict(queue.db.execute('SELECT path,receipt_path FROM accepted_batches').fetchone())
    with queue.db:
        queue.db.execute('DELETE FROM cnyes_history_accepted_batch_candidates')
        queue.db.execute('UPDATE discovery SET due=9e9 WHERE url=?', (url,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    (queue.root / original['parquet_path']).rename(queue.root / 'original-held.parquet')
    history.audit_original_envelopes(queue, limit=20, max_seconds=10)
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()
    sha = hashlib.sha256(r.body).hexdigest()
    with queue.db:
        history._register_accepted_batch_candidate(queue, url, sha, batch['path'], batch['receipt_path'])
    work = queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()
    assert work and work[0] == 'pending'
    for _ in range(8):
        history.backfill(queue, limit=20, max_seconds=10)
        if head(queue, url)['state'] == 'list_complete':
            break
    assert head(queue, url)['state'] == 'list_complete'
    restored = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    assert restored['artifact']['kind'] == 'accepted_body_batch'
    assert restored['artifact']['parquet_path'] == batch['path']
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_accepted_batch_recovery_before_original_audit_migration(queue):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    with queue.db:
        queue.db.execute('DROP TABLE cnyes_history_unproven_pages')
    history.initialize_accepted_batch_candidates(queue)
    sha = hashlib.sha256(b'legacy-accepted-response').hexdigest()
    with queue.db:
        assert history._register_accepted_batch_candidate(queue, url, sha, 'legacy.parquet', 'legacy.receipt')
    history.initialize(queue)
    assert queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates WHERE request_url=? AND response_sha256=?',
                            (url, sha)).fetchone()
    assert queue.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='cnyes_history_unproven_pages'").fetchone()


def test_late_candidate_does_not_wake_a_head_from_old_authorization(queue):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    r = capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    original = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    (queue.root / original['artifact']['parquet_path']).rename(queue.root / 'original-held.parquet')
    history.audit_original_envelopes(queue, limit=20, max_seconds=10)
    old_source = queue.registry.sources['cnyes']
    queue.registry.sources['cnyes'] = replace(old_source, authorization_fingerprint='new-authority')
    sha = hashlib.sha256(r.body).hexdigest()
    with queue.db:
        history._register_accepted_batch_candidate(queue, url, sha, 'candidate.parquet', 'candidate.receipt')
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()


@pytest.mark.parametrize('bad', ['broken-json', b'\xff\x00', json.dumps({'version': True})])
def test_legacy_original_audit_preserves_and_restarts_bad_checkpoint(queue, bad):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    key = history.original_envelope_audit_key(queue.registry.sources['cnyes'])
    with queue.db:
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (key, bad))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    result = history.audit_original_envelopes(queue, limit=20, max_seconds=10)
    assert result['complete'] and result['checked'] == 1
    assert queue.db.execute('SELECT 1 FROM cnyes_history_integrity_checkpoint_audit WHERE checkpoint_key=?', (key,)).fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_corrupt_exact_envelope_is_preserved_and_new_capture_is_recoverable(queue):
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    data = source_payload([], epoch=bounds(url)[0])
    r = capture(queue, url, data)
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    damaged = queue.root / observation['artifact']['parquet_path']
    damaged.write_bytes(b'corrupt-original-evidence')
    capture(queue, url, data)
    replacement = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    assert replacement['artifact']['parquet_path'] != observation['artifact']['parquet_path']
    assert damaged.read_bytes() == b'corrupt-original-evidence'
    assert len(history._retained_envelopes(queue.root, hashlib.sha256(r.body).hexdigest(), url,
                                           queue.registry.sources['cnyes'].authorization_fingerprint)) >= 4
    with queue.db:
        for table in ('cnyes_history_page_heads', 'cnyes_history_page_observations', 'cnyes_history_scope_heads'):
            queue.db.execute('DELETE FROM ' + table + ' WHERE scope_url=?', (cnyes_scope(url),))
        queue.db.execute('UPDATE discovery SET due=9e9 WHERE url=?', (url,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    assert backfill(queue, max_seconds=10)['processed'] == 1
    assert head(queue, url)['state'] == 'list_complete'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_exact_envelope_separates_changed_authorization_fingerprint(queue):
    import pyarrow.parquet as pq
    from downloader import news_fulltext_cnyes_history as history

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    r = capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    source = queue.registry.sources['cnyes']
    changed = replace(source, authorization_fingerprint='e' * 64)
    original = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])['artifact']
    other = history.retain_response(queue, r, changed, accepted=[], restricted=False)
    assert original['parquet_path'] != other['parquet_path']
    assert pq.read_table(queue.root / other['parquet_path']).to_pylist()[0]['authorization_fingerprint'] == 'e' * 64


def test_unrecorded_legacy_list_retains_exactly_one_source_without_recopied_body(queue):
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 1)
    data = source_payload([1, 2], epoch=low)
    capture(queue, url, data)
    with queue.db:  # Isolated fixture representing the old hash-only scheduler.
        queue.db.execute('DELETE FROM cnyes_history_page_observations')
        queue.db.execute('DELETE FROM cnyes_history_page_heads')
    capture(queue, url, data)
    capture(queue, url, data)
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
    assert len(list(queue.root.glob('discovery_responses/**/*.parquet'))) == 1
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 1


def test_restricted_response_keeps_public_ID_metadata_not_protected_text_or_body_receipt(queue):
    url = cnyes_url(BASE, start=1388534400, end=1388534401)
    data = source_payload([1, 2], epoch=1388534400)
    data['items']['data'][1]['payment'] = 1
    data['items']['data'][1]['content'] = 'SECRET PROTECTED CONTENT NOT TO PERSIST'
    capture(queue, url, data)
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
    raw = queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0]
    assert 'SECRET PROTECTED' not in raw and 'restricted_public_metadata_only' in raw
    assert not list(queue.root.glob('discovery_responses/*.parquet')) and not list(queue.root.glob('quarantine/*.parquet'))
    assert queue.db.execute('SELECT error_code FROM discovery WHERE url=?', (url,)).fetchone()[0] == 'bulk_response_contains_restricted_text'


def test_due_restricted_page_without_history_head_recovers_on_its_existing_request(cfg, tmp_path, monkeypatch):
    """An old mixed page can recover IDs without retaining its restricted envelope."""
    from downloader import news_fulltext as owner
    from test_news_fulltext import FakeResponse, http_fixture

    monkeypatch.setattr(owner, 'seed_discovery', lambda *_: None)
    monkeypatch.setattr(owner, 'refresh_disk_admission', lambda *_: False)
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    data = source_payload([1, 2], epoch=low)
    data['items']['data'][1]['payment'] = 1
    data['items']['data'][1]['content'] = 'SECRET PROTECTED CONTENT NOT TO PERSIST'
    r = response(data, url)
    info, _ = cnyes_pagination(r)
    digest = hashlib.sha256(r.body).hexdigest()
    http, session = http_fixture(cfg, tmp_path, {
        'https://api.cnyes.com/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: FakeResponse(r.body, headers={'Content-Type': 'application/json', 'ETag': 'same', 'Last-Modified': 'old-date'}),
    })
    queue = NewsQueue(tmp_path, http.registry)
    original_get = session.get
    def conditional_source(request_url, **kwargs):
        if request_url == url:
            headers = kwargs.get('headers', {})
            session.routes[url] = (FakeResponse(status=304, headers={'ETag': 'same'})
                                   if headers.get('If-None-Match') else
                                   FakeResponse(r.body, headers={'Content-Type': 'application/json', 'ETag': 'same', 'Last-Modified': 'old-date'}))
        return original_get(request_url, **kwargs)
    session.get = conditional_source
    try:
        source = queue.registry.sources['cnyes']
        with queue.db:
            queue.db.execute('INSERT INTO discovery(url,kind,provider,due,error_code,attempts,etag,last_modified,response_sha256,'
                'parser_fingerprint,last_checked_at_utc) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                (url, 'cnyes_news', 'cnyes', 0, 'bulk_response_contains_restricted_text', 0, 'same',
                 'old-date', digest, owner.discovery_fingerprint(source), '2020-01-01T00:00:00+00:00'))
            queue.db.execute('INSERT INTO bulk_page_evidence VALUES (?,?,?,?,?)',
                             (info['scope'], 1, info['total'], info['ids_sha256'], '2020-01-01T00:00:00+00:00'))
            queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'cnyes',?,?,'2020-01-01T00:00:00+00:00')",
                             (info['scope'], info['total'], info['pages']))
            queue.db.execute("INSERT INTO bulk_item_errors VALUES (?,1,'cnyes',?,'bulk_post_not_public','2020-01-01T00:00:00+00:00')",
                             (url, 'https://news.cnyes.com/news/id/2'))
        owner.run_cycle(queue, http, cfg, max_articles=0, max_discovery_requests=1)
        requests = [kwargs for requested, kwargs in session.calls if requested == url]
        assert len(requests) == 1 and 'If-None-Match' not in requests[0]['headers']
        assert 'If-Modified-Since' not in requests[0]['headers']
        row = queue.db.execute('SELECT o.observation_json FROM cnyes_history_page_heads h '
            'JOIN cnyes_history_page_observations o ON o.observation_sha256=h.observation_sha256 '
            'WHERE h.scope_url=? AND h.page=1', (info['scope'],)).fetchone()
        assert row is not None
        observation = json.loads(row[0])
        assert observation['artifact']['kind'] == 'restricted_public_metadata_only'
        assert [i['newsId'] for i in observation['identities']] == [1, 2]
        assert observation['body_completeness_verified'] is False
        assert 'SECRET PROTECTED' not in row[0]
        assert not list(tmp_path.glob('**/*.parquet'))
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert queue.db.execute('SELECT error_code FROM discovery WHERE url=?', (url,)).fetchone()[0] == 'bulk_response_contains_restricted_text'
        assert queue.db.execute('SELECT url FROM articles').fetchone()[0] == 'https://news.cnyes.com/news/id/1'

        # A new owner can reuse the durable witness. A 304 after a prior
        # transport failure must restore, rather than erase, the paid gap.
        queue.close()
        queue = NewsQueue(tmp_path, http.registry)
        with queue.db:
            queue.db.execute("UPDATE discovery SET due=0,error_code='http_429',attempts=1 WHERE url=?", (url,))
        owner.run_cycle(queue, http, cfg, max_articles=0, max_discovery_requests=1)
        requests = [kwargs for requested, kwargs in session.calls if requested == url]
        assert len(requests) == 2 and requests[-1]['headers']['If-None-Match'] == 'same'
        assert requests[-1]['headers']['If-Modified-Since'] == 'old-date'
        assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 1
        assert queue.db.execute('SELECT error_code FROM discovery WHERE url=?', (url,)).fetchone()[0] == 'bulk_response_contains_restricted_text'
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('failure', ['http_429', 'source_access_denied', 'worker_TimeoutError', None])
def test_missing_restricted_history_recovers_after_prior_failure_without_resetting_deadline(queue, failure):
    from downloader.news_fulltext import discovery_fingerprint
    from downloader.news_fulltext_cnyes_history import prepare_restricted_history_dispatch

    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    source = queue.registry.sources['cnyes']
    fingerprint = discovery_fingerprint(source, kind='cnyes_news')
    with queue.db:
        queue.db.execute("INSERT INTO discovery(url,kind,provider,due,error_code,attempts,etag,"
                         "last_modified,response_sha256,parser_fingerprint) "
                         "VALUES (?,'cnyes_news','cnyes',9000000000,?,1,'old','old-date',?,?)",
                         (url, failure, 'a' * 64, fingerprint))
        queue.db.execute("INSERT INTO bulk_item_errors VALUES (?,1,'cnyes',?,'bulk_post_not_public','old-clock')",
                         (url, 'https://news.cnyes.com/news/id/2'))
    original = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    assert not queue.discovery_candidates(source, 1)  # The existing future retry remains binding.
    task = dict(original)
    prepare_restricted_history_dispatch(queue, task, source, compatible_fingerprint=fingerprint)
    assert task['_cnyes_missing_restricted_history_evidence'] is True
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == original
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 0


def test_restricted_history_dispatch_reuses_only_verified_matching_page_evidence(queue):
    from downloader.news_fulltext import discovery_fingerprint
    from downloader.news_fulltext_cnyes_history import prepare_restricted_history_dispatch

    exemplar, alias = seed_equivalent(queue)
    capture(queue, exemplar, source_payload(range(1, 31), epoch=bounds(exemplar)[0], total=31))
    source = queue.registry.sources['cnyes']
    fingerprint = discovery_fingerprint(source, kind='cnyes_news')
    with queue.db:
        queue.db.execute("UPDATE discovery SET etag='same',response_sha256=?,parser_fingerprint=?,"
                         "error_code='bulk_response_contains_restricted_text' WHERE url=?",
                         ('a' * 64, fingerprint, alias))
    original = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone())
    task = dict(original)
    prepare_restricted_history_dispatch(queue, task, source, compatible_fingerprint=fingerprint)
    assert '_cnyes_missing_restricted_history_evidence' not in task  # Proven shared IDs.
    with queue.db:
        queue.db.execute('UPDATE bulk_page_evidence SET ids_sha256=? WHERE scope_url=? AND page=1',
                         ('b' * 64, cnyes_scope(alias)))
    task = dict(original)
    prepare_restricted_history_dispatch(queue, task, source, compatible_fingerprint=fingerprint)
    assert task['_cnyes_missing_restricted_history_evidence'] is True  # No proof for changed IDs.
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (alias,)).fetchone()) == original


def test_restricted_history_dispatch_rejects_stale_direct_response_witness(queue):
    from downloader.news_fulltext import discovery_fingerprint
    from downloader.news_fulltext_cnyes_history import prepare_restricted_history_dispatch

    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    data = source_payload([1, 2], epoch=low)
    data['items']['data'][1]['payment'] = 1
    capture(queue, url, data)
    source = queue.registry.sources['cnyes']
    fingerprint = discovery_fingerprint(source, kind='cnyes_news')
    with queue.db:
        queue.db.execute("UPDATE discovery SET etag='old',response_sha256=? WHERE url=?", ('a' * 64, url))
    task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    prepare_restricted_history_dispatch(queue, task, source, compatible_fingerprint=fingerprint)
    assert task['_cnyes_missing_restricted_history_evidence'] is True


def test_restricted_history_unconditional_304_is_not_accepted(cfg, tmp_path):
    from downloader.news_fulltext import discovery_fingerprint, fetch_discovery
    from downloader.news_fulltext_policy import NewsError
    from test_news_fulltext import FakeResponse, http_fixture

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    http, session = http_fixture(cfg, tmp_path, {
        'https://api.cnyes.com/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: FakeResponse(status=304),
    })
    source = http.registry.sources['cnyes']
    task = {'url': url, 'kind': 'cnyes_news', 'provider': 'cnyes', 'etag': 'old', 'last_modified': 'old-date',
            'parser_fingerprint': discovery_fingerprint(source, kind='cnyes_news'),
            '_cnyes_missing_restricted_history_evidence': True}
    try:
        with pytest.raises(NewsError, match='unverifiable_discovery_not_modified') as error:
            fetch_discovery(http, task)
        assert error.value.retry and error.value.delay == 60
        headers = [kwargs['headers'] for requested, kwargs in session.calls if requested == url][0]
        assert 'If-None-Match' not in headers and 'If-Modified-Since' not in headers
    finally:
        http.close()


def test_restricted_history_429_preserves_provider_and_page_cooldowns(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext as owner
    from downloader.news_fulltext_cnyes_history import prepare_restricted_history_dispatch
    from test_news_fulltext import FakeResponse, http_fixture

    monkeypatch.setattr(owner, 'seed_discovery', lambda *_: None)
    monkeypatch.setattr(owner, 'refresh_disk_admission', lambda *_: False)
    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    http, session = http_fixture(cfg, tmp_path, {
        'https://api.cnyes.com/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: FakeResponse(status=429, headers={'Retry-After': '120'}),
    })
    queue = NewsQueue(tmp_path, http.registry)
    source = http.registry.sources['cnyes']
    fingerprint = owner.discovery_fingerprint(source, kind='cnyes_news')
    try:
        with queue.db:
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due,error_code,attempts,etag,response_sha256,parser_fingerprint) "
                             "VALUES (?,'cnyes_news','cnyes',0,'bulk_response_contains_restricted_text',0,'old',?,?)",
                             (url, 'a' * 64, fingerprint))
            queue.db.execute("INSERT INTO bulk_item_errors VALUES (?,1,'cnyes',?,'bulk_post_not_public','old-clock')",
                             (url, 'https://news.cnyes.com/news/id/2'))
        owner.run_cycle(queue, http, cfg, max_articles=0, max_discovery_requests=1)
        row = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
        assert row['error_code'] == 'http_429' and row['attempts'] == 1 and row['due'] > time.time() + 100
        assert row['etag'] == 'old' and http.provider_not_before['cnyes'] > time.time() + 100
        assert len([x for x in session.calls if x[0] == url]) == 1
        owner.run_cycle(queue, http, cfg, max_articles=0, max_discovery_requests=1)
        assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == row
        assert len([x for x in session.calls if x[0] == url]) == 1
        task = dict(row)  # Inspect future dispatch, without making the retry due.
        prepare_restricted_history_dispatch(queue, task, source, compatible_fingerprint=fingerprint)
        assert task['_cnyes_missing_restricted_history_evidence'] is True
        assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == row
    finally:
        queue.close()
        http.close()


def test_restricted_history_dispatch_ignores_nonhistorical_unregistered_or_unrelated_pages(queue):
    from downloader.news_fulltext import discovery_fingerprint
    from downloader.news_fulltext_cnyes_history import prepare_restricted_history_dispatch

    source = queue.registry.sources['cnyes']
    fingerprint = discovery_fingerprint(source, kind='cnyes_news')
    historical = cnyes_url(BASE, start=1388534400, end=1388534500)
    task = {'url': historical, 'kind': 'cnyes_news', 'provider': 'cnyes', 'etag': 'same',
            'last_modified': None, 'response_sha256': 'a' * 64, 'parser_fingerprint': fingerprint,
            'error_code': None}
    cases = [task, dict(task, url=cnyes_url(BASE), error_code='bulk_response_contains_restricted_text'),
             dict(task, url=cnyes_url('https://api.cnyes.com/not-registered', start=1388534400, end=1388534500),
                  error_code='bulk_response_contains_restricted_text'),
             dict(task, parser_fingerprint='old', error_code='bulk_response_contains_restricted_text'),
             dict(task, etag=None, error_code='bulk_response_contains_restricted_text'),
             dict(task, kind='sitemap', error_code='bulk_response_contains_restricted_text')]
    for candidate in cases:
        before = dict(candidate)
        prepare_restricted_history_dispatch(queue, candidate, source, compatible_fingerprint=fingerprint)
        assert candidate == before


def seed_retained_work(queue, *, failure=None, valid_raw=True, low=1388534400):
    url = cnyes_url(BASE, start=low, end=low + 100)
    r = response(source_payload([1, 2], epoch=low), url)
    info, _ = cnyes_pagination(r)
    sha = hashlib.sha256(r.body).hexdigest()
    source = queue.registry.sources['cnyes']
    with queue.db:
        queue.db.execute("INSERT INTO discovery(url,kind,provider,due,error_code,attempts,response_sha256,last_checked_at_utc) "
            "VALUES (?,'cnyes_news','cnyes',9e9,?,?,?,'2020-01-01T00:00:00+00:00')", (url, failure, int(failure is not None), sha))
        queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'cnyes',2,1,'2020-source-clock')", (info['scope'],))
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
            "VALUES (?,?,1,?,2,?,'2020-01-01T00:00:00+00:00')", (url, info['scope'], sha, info['ids_sha256']))
    if valid_raw:
        target = queue.root / 'quarantine' / (sha + '.parquet')
        atomic_write_parquet(target, table_from_records([{'request_url': url, 'raw_response_body': r.body,
            'response_sha256': sha, 'captured_at_utc': '2020-01-01T00:00:00+00:00',
            'authorization_fingerprint': source.authorization_fingerprint, 'publish': False}]), durable=True)
        atomic_write_json(target.with_suffix('.parquet.receipt.json'), {
            'sha256': sha256_file(target), 'response_sha256': sha, 'publish': False}, durable=True)
    return url, r


def move_retained_to_discovery(queue, response):
    sha = hashlib.sha256(response.body).hexdigest()
    old = queue.root / 'quarantine' / (sha + '.parquet')
    new = queue.root / 'discovery_responses' / (sha + '.parquet')
    new.parent.mkdir(parents=True, exist_ok=True)
    old.replace(new)
    old.with_suffix('.parquet.receipt.json').replace(new.with_suffix('.parquet.receipt.json'))
    return new


@pytest.mark.parametrize(('kind', 'field', 'bad_value'), [
    ('canonical_quarantine', 'parquet_path', 'quarantine/wrong.parquet'),
    ('canonical_quarantine', 'accepted_body_count', 3),
    ('cnyes_history_source_envelope', 'parquet_path', 'discovery_responses/wrong.parquet'),
    ('cnyes_history_source_envelope', 'contract', 'unrecognized_history_contract'),
    ('cnyes_history_source_envelope', 'raw_response_bytes', -1),
    ('cnyes_history_source_envelope', 'article_bodies_accepted', True),
])
def test_retained_backfill_never_accepts_contradictory_receipt(queue, kind, field, bad_value):
    url, r = seed_retained_work(queue, failure='http_429')
    sha = hashlib.sha256(r.body).hexdigest()
    path = (move_retained_to_discovery(queue, r) if kind == 'cnyes_history_source_envelope'
            else queue.root / 'quarantine' / (sha + '.parquet'))
    receipt_path = path.with_suffix('.parquet.receipt.json')
    receipt = json.loads(receipt_path.read_bytes())
    receipt['parquet_path'] = str(path.relative_to(queue.root))
    if kind == 'canonical_quarantine':
        receipt.update(article_count=2, accepted_body_count=0)
    else:
        receipt.update(contract='cnyes_history_source_envelope_v1', raw_response_bytes=len(r.body),
                       article_bodies_accepted=False)
    receipt[field] = bad_value
    atomic_write_json(receipt_path, receipt)
    saved_original, saved_receipt = path.read_bytes(), receipt_path.read_bytes()
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())

    assert backfill(queue, max_seconds=10)['processed'] == 1
    work = queue.db.execute('SELECT state,outcome_json FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()
    assert work['state'] == 'retained_invalid'
    assert json.loads(work['outcome_json'])['error'] == 'invalid_original_envelope_receipt'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 0
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    assert (path.read_bytes(), receipt_path.read_bytes()) == (saved_original, saved_receipt)


@pytest.mark.parametrize('mutation', ['replace', 'disappear'])
def test_retained_backfill_retries_when_receipt_changes_during_read(queue, monkeypatch, mutation):
    from downloader import news_fulltext_cnyes_history as history

    url, r = seed_retained_work(queue, failure='http_429')
    path = queue.root / 'quarantine' / (hashlib.sha256(r.body).hexdigest() + '.parquet')
    receipt_path = path.with_suffix('.parquet.receipt.json')
    original_receipt = json.loads(receipt_path.read_bytes())
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    real_read = history.pq.read_table
    def read_then_replace(candidate, *args, **kwargs):
        table = real_read(candidate, *args, **kwargs)
        if candidate == path:
            if mutation == 'replace':
                atomic_write_json(receipt_path, {'publish': True})
            else:
                receipt_path.unlink()
        return table
    with monkeypatch.context() as patch:
        patch.setattr(history.pq, 'read_table', read_then_replace)
        result = backfill(queue, max_seconds=10)
    assert result['local_io_deferrals'] == 1
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()[0] == 'local_retry'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 0
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    atomic_write_json(receipt_path, original_receipt)
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=0 WHERE request_url=?', (url,))
    assert backfill(queue, max_seconds=10)['processed'] == 1
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()[0] == 'restored'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_retained_backfill_rolls_back_when_receipt_changes_during_recording(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    url, r = seed_retained_work(queue, failure='http_429')
    path = queue.root / 'quarantine' / (hashlib.sha256(r.body).hexdigest() + '.parquet')
    receipt_path = path.with_suffix('.parquet.receipt.json')
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    record = history.record_page

    def record_then_replace(*args, **kwargs):
        record(*args, **kwargs)
        atomic_write_json(receipt_path, {'publish': True})

    monkeypatch.setattr(history, 'record_page', record_then_replace)
    result = history.backfill(queue, max_seconds=10)
    assert result['local_io_deferrals'] == 1
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()[0] == 'local_retry'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 0
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 0
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_future_retained_capture_cannot_be_adopted_as_history_evidence(queue):
    import pyarrow.parquet as pq
    from downloader import news_fulltext_cnyes_history as history

    url, r = seed_retained_work(queue, failure='http_429')
    sha = hashlib.sha256(r.body).hexdigest()
    path = queue.root / 'quarantine' / (sha + '.parquet')
    receipt_path = path.with_suffix('.parquet.receipt.json')
    row = pq.read_table(path).to_pylist()[0]
    row['captured_at_utc'] = '2099-01-01T00:00:00+00:00'
    atomic_write_parquet(path, table_from_records([row]), durable=True)
    receipt = json.loads(receipt_path.read_bytes())
    receipt['sha256'] = sha256_file(path)
    atomic_write_json(receipt_path, receipt)
    relative = str(path.relative_to(queue.root))
    artifact = {'kind': 'canonical_quarantine', 'parquet_path': relative, 'receipt_path': relative + '.receipt.json'}
    auth = queue.registry.sources['cnyes'].authorization_fingerprint
    assert history._original_envelope_state(queue.root, artifact, url, sha, auth) == 'invalid'
    assert backfill(queue, max_seconds=10)['processed'] == 1
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()[0] == 'retained_invalid'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 0


@pytest.mark.parametrize('failure', [None, 'http_429', 'source_access_denied'])
def test_discovery_envelope_recovery_preserves_job_and_source_deadline(queue, failure):
    url, r = seed_retained_work(queue, failure=failure)
    path = move_retained_to_discovery(queue, r)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    assert backfill(queue, max_seconds=10)['processed'] == 1
    assert head(queue, url)['state'] == 'list_complete'
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    assert observation['artifact']['kind'] == 'cnyes_history_source_envelope'
    assert observation['artifact']['parquet_path'] == str(path.relative_to(queue.root))
    assert observation['source_observed_at_utc'] == '2020-01-01T00:00:00+00:00'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0


def test_discovery_envelope_same_bytes_different_request_is_not_relabelled(queue):
    import pyarrow.parquet as pq
    url, r = seed_retained_work(queue, failure='http_429')
    path = move_retained_to_discovery(queue, r)
    record = pq.read_table(path).to_pylist()[0]
    record['request_url'] = cnyes_url(BASE, start=bounds(url)[0] + 1, end=bounds(url)[1] + 1)
    atomic_write_parquet(path, table_from_records([record]), durable=True)
    atomic_write_json(path.with_suffix('.parquet.receipt.json'), {
        'sha256': sha256_file(path), 'response_sha256': hashlib.sha256(r.body).hexdigest(), 'publish': False}, durable=True)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    backfill(queue, max_seconds=10)
    row = queue.db.execute('SELECT state,outcome_json FROM cnyes_history_retained_work').fetchone()
    assert row['state'] == 'retained_request_mismatch'
    assert json.loads(row['outcome_json'])['error'] == 'retained_original_request_differs_not_relabelled'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 0
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_same_scope_new_request_does_not_relabel_an_accepted_batch(queue):
    import pyarrow.parquet as pq

    first = cnyes_url(BASE, start=1388534400, end=1388534500)
    second = BASE + '?endAt=1388534500&startAt=1388534400&page=1&limit=30'
    assert first != second and cnyes_scope(first) == cnyes_scope(second)
    data = source_payload([1], epoch=bounds(first)[0])
    capture(queue, first, data)
    original = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    assert original['request_url'] == first and original['artifact']['kind'] == 'accepted_body_batch'
    original_head = dict(queue.db.execute('SELECT state,body_sha256,parquet_path FROM articles').fetchone())
    capture(queue, second, data)
    current = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    assert current['request_url'] == second
    assert current['artifact']['kind'] == 'cnyes_history_source_envelope'
    retained, = pq.read_table(queue.root / current['artifact']['parquet_path']).to_pylist()
    assert retained['request_url'] == second and retained['response_sha256'] == current['response_sha256']
    assert dict(queue.db.execute('SELECT state,body_sha256,parquet_path FROM articles').fetchone()) == original_head
    audit = queue.db.execute('SELECT original_row_json,reason FROM cnyes_history_corrupt_observation_audit').fetchone()
    assert audit and json.loads(json.loads(audit['original_row_json'])['observation_json']) == original


def test_older_same_scope_local_replay_cannot_replace_newer_observation(queue):
    from downloader import news_fulltext_cnyes_history as history

    first = cnyes_url(BASE, start=1388534400, end=1388534500)
    second = BASE + '?endAt=1388534500&startAt=1388534400&page=1&limit=30'
    data = source_payload([1], epoch=bounds(first)[0])
    capture(queue, first, data)
    original = dict(queue.db.execute('SELECT * FROM cnyes_history_page_observations').fetchone())
    original_head = dict(queue.db.execute('SELECT * FROM cnyes_history_page_heads').fetchone())
    r = response(data, second)
    source = queue.registry.sources['cnyes']
    artifact = history.retain_response(queue, r, source, accepted=[], restricted=False)
    info, posts = cnyes_pagination(r)
    with queue.db:
        record_page(queue, r, info, posts, source, artifact,
                    observed_at='2020-01-01T00:00:00+00:00', origin='verified_retained_source_response')
    assert dict(queue.db.execute('SELECT * FROM cnyes_history_page_observations').fetchone()) == original
    assert dict(queue.db.execute('SELECT * FROM cnyes_history_page_heads').fetchone()) == original_head


def test_damaged_accepted_batch_is_not_reused_for_a_new_observation(queue):
    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    data = source_payload([1], epoch=bounds(url)[0])
    capture(queue, url, data)
    original = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    assert original['artifact']['kind'] == 'accepted_body_batch'
    receipt_path = queue.root / original['artifact']['receipt_path']
    receipt = json.loads(receipt_path.read_bytes())
    atomic_write_json(receipt_path, {**receipt, 'sha256': '0' * 64})
    capture(queue, url, data)
    current = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    assert current['artifact'] != original['artifact']
    assert current['request_url'] == url
    audit = queue.db.execute('SELECT original_row_json,reason FROM cnyes_history_corrupt_observation_audit').fetchone()
    assert audit and json.loads(json.loads(audit['original_row_json'])['observation_json']) == original
    assert json.loads(receipt_path.read_bytes())['sha256'] == '0' * 64


def test_verified_accepted_batch_is_reused_only_for_its_exact_request(queue):
    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    data = source_payload([1], epoch=bounds(url)[0])
    capture(queue, url, data)
    original = dict(queue.db.execute('SELECT * FROM cnyes_history_page_observations').fetchone())
    assert json.loads(original['observation_json'])['artifact']['kind'] == 'accepted_body_batch'
    capture(queue, url, data)
    current = dict(queue.db.execute('SELECT * FROM cnyes_history_page_observations').fetchone())
    assert current == original
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
    assert not list((queue.root / 'discovery_responses').rglob('*.parquet'))


@pytest.mark.parametrize('change', ['request', 'authorization', 'raw_bytes'])
def test_accepted_batch_identity_checked_even_with_a_valid_file_receipt(queue, change):
    import pyarrow as pa
    import pyarrow.parquet as pq

    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    data = source_payload([1], epoch=bounds(url)[0])
    capture(queue, url, data)
    original = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    path = queue.root / original['artifact']['parquet_path']
    receipt_path = queue.root / original['artifact']['receipt_path']
    table = pq.read_table(path)
    records = table.to_pylist()
    raw = next(item for item in records if item['raw_response_body'] is not None)
    if change == 'request':
        raw['source_request_url'] += '&unexpected=1'
    elif change == 'authorization':
        raw['authorization_fingerprint'] = 'different-authorization'
    else:
        raw['raw_response_body'] = b'different raw bytes'
    atomic_write_parquet(path, pa.Table.from_pylist(records, schema=table.schema), compression='zstd', durable=True)
    receipt = json.loads(receipt_path.read_bytes())
    atomic_write_json(receipt_path, {**receipt, 'sha256': sha256_file(path)})
    capture(queue, url, data)
    current = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    assert current['artifact'] != original['artifact'] and current['request_url'] == url
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_corrupt_observation_audit').fetchone()[0] == 1


def test_valid_discovery_envelope_wins_over_wrong_request_in_quarantine(queue):
    import pyarrow.parquet as pq
    url, r = seed_retained_work(queue)
    discovery_path = move_retained_to_discovery(queue, r)
    quarantine = queue.root / 'quarantine' / discovery_path.name
    record = pq.read_table(discovery_path).to_pylist()[0]
    record['request_url'] = cnyes_url(BASE, start=bounds(url)[0] + 1, end=bounds(url)[1] + 1)
    atomic_write_parquet(quarantine, table_from_records([record]), durable=True)
    atomic_write_json(quarantine.with_suffix('.parquet.receipt.json'), {
        'sha256': sha256_file(quarantine), 'response_sha256': hashlib.sha256(r.body).hexdigest(), 'publish': False}, durable=True)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    backfill(queue, max_seconds=10)
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work').fetchone()[0] == 'restored'
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    assert observation['artifact']['parquet_path'] == str(discovery_path.relative_to(queue.root))
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_invalid_discovery_envelope_does_not_bypass_source_failure(queue):
    url, r = seed_retained_work(queue, failure='http_429')
    path = move_retained_to_discovery(queue, r)
    receipt = path.with_suffix('.parquet.receipt.json')
    value = json.loads(receipt.read_bytes())
    value['sha256'] = '0' * 64
    atomic_write_json(receipt, value, durable=True)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    backfill(queue, max_seconds=10)
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work').fetchone()[0] == 'retained_invalid'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 0
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_new_direct_page_resolves_invalid_old_envelope_without_rewriting_failure(queue):
    url, original = seed_retained_work(queue)
    path = move_retained_to_discovery(queue, original)
    receipt = path.with_suffix('.parquet.receipt.json')
    value = json.loads(receipt.read_bytes())
    value['sha256'] = '0' * 64
    atomic_write_json(receipt, value, durable=True)
    original_files = path.read_bytes(), receipt.read_bytes()
    backfill(queue, max_seconds=10)
    old = dict(queue.db.execute('SELECT * FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone())
    assert old['state'] == 'retained_invalid'
    assert json.loads(old['outcome_json'])['error'] == 'invalid_original_envelope_receipt'

    newer = capture(queue, url, source_payload([1, 2, 3], epoch=bounds(url)[0]))
    assert hashlib.sha256(newer.body).hexdigest() != old['response_sha256']
    work = dict(queue.db.execute('SELECT * FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone())
    assert work['state'] == 'forward_evidence_available'
    assert work['response_sha256'] == old['response_sha256'] and work['outcome_json'] == old['outcome_json']
    assert (path.read_bytes(), receipt.read_bytes()) == original_files
    resolution = queue.db.execute('SELECT * FROM cnyes_history_retained_outcome_resolutions '
                                  "WHERE request_url=? AND evidence_kind='direct'", (url,)).fetchone()
    assert resolution['original_state'] == 'retained_invalid'
    assert resolution['original_outcome_json'] == old['outcome_json']
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations '
        'WHERE observation_sha256=?', (resolution['observation_sha256'],)).fetchone()[0])
    assert observation['request_url'] == url and observation['response_sha256'] == hashlib.sha256(newer.body).hexdigest()
    assert observation['origin'] == 'normal_source_response'


@pytest.mark.parametrize('evidence', ['valid', 'digest_corrupt', 'old_source_contract'])
def test_legacy_invalid_work_reconciles_only_with_current_verified_direct_page(queue, evidence):
    from downloader.news_fulltext_cnyes_history import LEGACY_MISMATCH_SCAN_KEY
    url, _ = seed_retained_work(queue)
    capture(queue, url, source_payload([1, 2, 3], epoch=bounds(url)[0]))
    previous = json.dumps({'error': 'invalid_original_envelope_receipt', 'publisher_requests': 0})
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_retained_work SET state='retained_invalid',outcome_json=? "
                         'WHERE request_url=?', (previous, url))
        queue.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)',
            ('cnyes_history_retained_mismatch_scan_v1', json.dumps({'version': 1, 'state': 'complete', 'request_url': url})))
        if evidence == 'digest_corrupt':
            queue.db.execute("UPDATE cnyes_history_page_observations SET observation_json='{}'")
        elif evidence == 'old_source_contract':
            queue.db.execute("UPDATE cnyes_history_page_observations SET source_bulk_fingerprint='old-contract'")
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    result = reconcile_legacy_request_mismatches(queue, max_seconds=10)
    assert LEGACY_MISMATCH_SCAN_KEY != 'cnyes_history_retained_mismatch_scan_v1'
    assert result['complete'] and result['checked'] == 1
    row = queue.db.execute('SELECT state,outcome_json FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()
    assert row['outcome_json'] == previous
    resolution = queue.db.execute('SELECT * FROM cnyes_history_retained_outcome_resolutions WHERE request_url=?',
                                  (url,)).fetchone()
    if evidence == 'valid':
        assert result['reclassified'] == 1 and row['state'] == 'forward_evidence_available'
        assert resolution['original_state'] == 'retained_invalid' and resolution['original_outcome_json'] == previous
        assert resolution['evidence_kind'] == 'direct' and resolution['observation_sha256']
    else:
        assert result['reclassified'] == 0 and row['state'] == 'retained_invalid' and resolution is None
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_discovery_envelope_io_error_retries_locally_without_resetting_job(queue, monkeypatch):
    from pathlib import Path
    url, r = seed_retained_work(queue)
    path = move_retained_to_discovery(queue, r)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    original = Path.read_bytes
    def unavailable(candidate):
        if candidate == path.with_suffix('.parquet.receipt.json'):
            raise OSError(5, 'temporary local error')
        return original(candidate)
    with monkeypatch.context() as patch:
        patch.setattr(Path, 'read_bytes', unavailable)
        assert backfill(queue, max_seconds=10)['local_io_deferrals'] == 1
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work').fetchone()[0] == 'local_retry'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=0 WHERE request_url=?', (url,))
    assert backfill(queue, max_seconds=10)['processed'] == 1
    assert head(queue, url)['state'] == 'list_complete'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


@pytest.mark.parametrize('failure', [None, 'discovery_http_429', 'discovery_http_403', 'transport_retry', 'bulk_pagination_totals_invalid'])
def test_retained_backfill_exact_source_clock_and_no_source_calls_or_failure_reset(queue, failure):
    url, _ = seed_retained_work(queue, failure=failure)
    before = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    result = backfill(queue, max_seconds=10)
    assert result['processed'] == 1 and result['exhausted']
    assert head(queue, url)['state'] == 'list_complete'
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    assert observation['source_observed_at_utc'] == '2020-01-01T00:00:00+00:00'
    assert observation['evidence_recorded_at_utc'] > observation['source_observed_at_utc']
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0


@pytest.mark.parametrize('failure', [None, 'discovery_http_429', 'discovery_http_403', 'transport_retry'])
def test_missing_raw_rechecks_only_successful_page_once_not_failed_deadline(queue, failure):
    url, _ = seed_retained_work(queue, failure=failure, valid_raw=False)
    original = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    backfill(queue, max_seconds=10)
    current = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    if failure:
        assert current == original
    else:
        assert current['due'] < original['due']
        assert {k: v for k, v in current.items() if k != 'due'} == {k: v for k, v in original.items() if k != 'due'}
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 0
    second = deepcopy(current)
    backfill(queue, max_seconds=10)
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == second


def test_stop_and_zero_time_slice_do_not_consume_or_defer_work(queue):
    seed_retained_work(queue)
    import threading
    stop = threading.Event()
    stop.set()
    assert backfill(queue, stop=stop)['processed'] == 0
    result = backfill(queue, max_seconds=0)
    assert result['processed'] == 0 and not result['exhausted']
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work').fetchone()[0] == 'pending'


def test_old_reference_wrong_request_is_not_source_corruption_or_backdated_capture(queue):
    url, r = seed_retained_work(queue)
    sha = hashlib.sha256(r.body).hexdigest()
    target = queue.root / 'quarantine' / (sha + '.parquet')
    import pyarrow.parquet as pq
    row = pq.read_table(target).to_pylist()[0]
    row['request_url'] = cnyes_url(BASE, start=bounds(url)[0] + 1, end=bounds(url)[1] + 1)
    atomic_write_parquet(target, table_from_records([row]), durable=True)
    atomic_write_json(target.with_suffix('.parquet.receipt.json'), {
        'sha256': sha256_file(target), 'response_sha256': sha, 'publish': False}, durable=True)
    backfill(queue, max_seconds=10)
    work = queue.db.execute('SELECT state,outcome_json FROM cnyes_history_retained_work').fetchone()
    assert work['state'] == 'retained_request_mismatch'
    assert 'retained_original_request_differs_not_relabelled' in work['outcome_json']
    assert json.loads(work['outcome_json'])['one_successful_page_evidence_recheck_scheduled']
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 0


def test_legacy_request_mismatches_reconcile_only_with_verified_direct_current_evidence(queue):
    first, _ = seed_retained_work(queue)
    second, _ = seed_retained_work(queue, low=1388620800)
    third, _ = seed_retained_work(queue, low=1388707200)
    capture(queue, first, source_payload([1, 2], epoch=bounds(first)[0]))
    previous = json.dumps({'error': 'retained_original_request_differs_not_relabelled',
                           'one_successful_page_evidence_recheck_scheduled': True})
    other = json.dumps({'error': 'invalid_original_envelope_receipt'})
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_retained_work SET state='retained_invalid',outcome_json=? "
                         'WHERE request_url IN (?,?)', (previous, first, second))
        queue.db.execute("UPDATE cnyes_history_retained_work SET state='retained_invalid',outcome_json=? "
                         'WHERE request_url=?', (other, third))
    discovery = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    first_slice = reconcile_legacy_request_mismatches(queue, limit=1, max_seconds=10)
    assert not first_slice['complete'] and first_slice['checked'] == 1
    for _ in range(5):
        result = reconcile_legacy_request_mismatches(queue, limit=1, max_seconds=10)
        if result['complete']:
            break
    assert result['complete']
    states = dict(queue.db.execute('SELECT request_url,state FROM cnyes_history_retained_work'))
    assert states == {first: 'forward_evidence_available', second: 'retained_request_mismatch', third: 'retained_invalid'}
    resolved = {row['request_url']: dict(row) for row in queue.db.execute('SELECT * FROM cnyes_history_retained_outcome_resolutions')}
    assert resolved[first]['original_outcome_json'] == resolved[second]['original_outcome_json'] == previous
    assert resolved[first]['observation_sha256'] and resolved[second]['observation_sha256'] is None
    assert queue.db.execute('SELECT outcome_json FROM cnyes_history_retained_work WHERE request_url=?', (first,)).fetchone()[0] == previous
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == discovery
    assert reconcile_legacy_request_mismatches(queue, max_seconds=10)['reclassified'] == 0
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_retained_outcome_resolutions').fetchone()[0] == 2


@pytest.mark.parametrize('corruption', ['digest', 'source_contract'])
def test_legacy_reconciliation_does_not_trust_invalid_or_old_direct_evidence(queue, corruption):
    url, _ = seed_retained_work(queue)
    capture(queue, url, source_payload([1, 2], epoch=bounds(url)[0]))
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_retained_work SET state='retained_invalid',outcome_json=? WHERE request_url=?",
                         (json.dumps({'error': 'retained_original_request_differs_not_relabelled'}), url))
        if corruption == 'digest':
            queue.db.execute("UPDATE cnyes_history_page_observations SET observation_json='{}'")
        else:
            queue.db.execute("UPDATE cnyes_history_page_observations SET source_bulk_fingerprint='old-contract'")
    result = reconcile_legacy_request_mismatches(queue, max_seconds=10)
    assert result['complete'] and result['reclassified'] == 1
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work').fetchone()[0] == 'retained_request_mismatch'
    assert queue.db.execute('SELECT observation_sha256 FROM cnyes_history_retained_outcome_resolutions').fetchone()[0] is None


@pytest.mark.parametrize('fenced', [False, True])
def test_later_direct_page_resolves_unavailable_even_after_scan_completed(queue, fenced):
    url, _ = seed_retained_work(queue, valid_raw=False)
    previous = json.dumps({'publisher_requests': 0, 'old_state': 'no_local_original'})
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_retained_work SET state='retained_unavailable',outcome_json=? WHERE request_url=?",
                         (previous, url))
        if fenced:
            from downloader.news_fulltext_policy import cnyes_bulk_fingerprint
            source = queue.registry.sources['cnyes']
            queue.db.execute('INSERT INTO cnyes_history_local_inventory_scopes VALUES (?,?,?,?,?)',
                (cnyes_scope(url), cnyes_bulk_fingerprint(source), source.authorization_fingerprint,
                 'ready', '2020-01-01T00:00:00+00:00'))
    assert reconcile_legacy_unavailable(queue, max_seconds=10)['resolved'] == 0
    capture(queue, url, source_payload([1, 2], epoch=bounds(url)[0]))
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()[0] == 'forward_evidence_available'
    receipt = queue.db.execute('SELECT * FROM cnyes_history_retained_outcome_resolutions '
                               "WHERE request_url=? AND evidence_kind='direct'", (url,)).fetchone()
    assert receipt['original_state'] == 'retained_unavailable' and receipt['original_outcome_json'] == previous
    assert receipt['evidence_kind'] == 'direct' and receipt['observation_sha256']
    candidate = queue.db.execute('SELECT original_state,original_outcome_json,resolution_state '
        "FROM cnyes_history_retained_outcome_resolutions WHERE request_url=? "
        "AND evidence_kind='new_accepted_body_batch_candidate'", (url,)).fetchone()
    assert tuple(candidate) == ('retained_unavailable', previous, 'pending')
    assert queue.db.execute('SELECT 1 FROM cnyes_history_local_inventory_scopes WHERE scope_url=?',
                            (cnyes_scope(url),)).fetchone() is None


def test_later_direct_page_resolves_candidate_waiting_on_local_retry(queue):
    from downloader.news_fulltext_cnyes_history import seed_accepted_body_batch_candidates
    urls = seed_unindexed_nonzero_pages(queue)
    seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    page = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone()
    job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    old_outcome = json.dumps({'error': 'previous_missing_local_original'})
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,'
            'source_checked_at_utc,state,outcome_json) VALUES (?,?,?,?,?,?,?,?,?)',
            (urls[2], cnyes_scope(urls[0]), 3, job['response_sha256'], page['total'], page['ids_sha256'],
             page['observed_at_utc'], 'retained_unavailable', old_outcome))
    assert seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['complete']
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_retained_work SET state='local_retry' WHERE request_url=?", (urls[2],))
        queue.db.execute('INSERT INTO cnyes_history_retained_local_retries VALUES (?,?,?,?,?,?)',
            (urls[2], 'pending', 9e9, 1, 'accepted_batch_scan_pending', '2020-01-01T00:00:00+00:00'))
    capture(queue, urls[2], source_payload([61], epoch=1388534400, page=3, total=61))
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                            (urls[2],)).fetchone()[0] == 'forward_evidence_available'
    assert queue.db.execute('SELECT 1 FROM cnyes_history_retained_local_retries WHERE request_url=?',
                            (urls[2],)).fetchone() is None
    event = queue.db.execute('SELECT original_state,original_outcome_json,observation_sha256 '
        "FROM cnyes_history_retained_outcome_resolutions WHERE request_url=? AND evidence_kind='direct'",
        (urls[2],)).fetchone()
    assert event['original_state'] == 'retained_unavailable' and event['original_outcome_json'] == old_outcome
    assert event['observation_sha256']


@pytest.mark.parametrize('corrupt_observation', [False, True])
def test_pending_failed_candidate_rechecks_existing_page_evidence_and_audits_original(queue, corrupt_observation):
    from downloader.news_fulltext_cnyes_history import _save_retained_resolution
    url, _ = seed_retained_work(queue, valid_raw=False)
    capture(queue, url, source_payload([1, 2, 3], epoch=bounds(url)[0]))
    previous = json.dumps({'error': 'invalid_original_envelope_receipt'})
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_retained_work SET state='retained_invalid',outcome_json=? WHERE request_url=?",
                         (previous, url))
        old = queue.db.execute('SELECT * FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()
        _save_retained_resolution(queue, old, 'pending', None, 'new_accepted_body_batch_candidate')
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=1,due=9e9 WHERE url=?", (url,))
        if corrupt_observation:
            queue.db.execute("UPDATE cnyes_history_page_observations SET observation_json='{}'")
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    backfill(queue, limit=8, max_seconds=10)
    row = queue.db.execute('SELECT state,outcome_json FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()
    resolution = queue.db.execute('SELECT * FROM cnyes_history_retained_outcome_resolutions WHERE request_url=? '
        "AND resolution_state='forward_evidence_available'", (url,)).fetchone()
    if corrupt_observation:
        assert row['state'] != 'forward_evidence_available' and resolution is None
    else:
        assert row['state'] == 'forward_evidence_available' and row['outcome_json'] == previous
        assert resolution['original_state'] == 'retained_invalid' and resolution['original_outcome_json'] == previous
        assert resolution['evidence_kind'] == 'direct' and resolution['observation_sha256']
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


@pytest.mark.parametrize('corrupt_observation', [False, True])
def test_ordinary_pending_page_requires_verified_current_observation(queue, corrupt_observation):
    url, _ = seed_retained_work(queue, valid_raw=False)
    capture(queue, url, source_payload([1, 2, 3], epoch=bounds(url)[0]))
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=1,due=9e9 WHERE url=?", (url,))
        if corrupt_observation:
            queue.db.execute("UPDATE cnyes_history_page_observations SET observation_json='{}'")
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    backfill(queue, limit=8, max_seconds=10)
    state = queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()[0]
    if corrupt_observation:
        assert state != 'forward_evidence_available'
    else:
        assert state == 'forward_evidence_available'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


@pytest.mark.parametrize('corrupt_reference', [False, True])
def test_ordinary_pending_shared_page_requires_matching_reference(queue, corrupt_reference):
    from downloader.news_fulltext_cnyes_history import index_scope
    urls, _ = seed_legacy_overflow(queue)
    source = queue.registry.sources['cnyes']
    r = response(source_payload(range(1, 31), epoch=bounds(urls[0])[0], total=931), urls[0])
    info, posts = cnyes_pagination(r)
    sha = hashlib.sha256(r.body).hexdigest()
    with queue.db:
        for url in urls:
            index_scope(queue, cnyes_scope(url), source.provider)
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
            "VALUES (?,?,1,?,931,?,'2020-01-01T00:00:00+00:00')",
            (urls[1], cnyes_scope(urls[1]), sha, info['ids_sha256']))
        record_page(queue, r, info, posts, source, {'kind': 'fixture'}, observed_at='2020-01-01T00:00:00+00:00')
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=1,due=9e9 WHERE url=?", (urls[1],))
        if corrupt_reference:
            queue.db.execute("UPDATE cnyes_history_page_ID_references SET original_ID_evidence_json='{}' WHERE scope_url=?",
                             (cnyes_scope(urls[1]),))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[1],)).fetchone())
    backfill(queue, limit=8, max_seconds=10)
    row = queue.db.execute('SELECT state,outcome_json FROM cnyes_history_retained_work WHERE request_url=?',
                           (urls[1],)).fetchone()
    if corrupt_reference:
        assert row['state'] != 'forward_evidence_available'
    else:
        assert row['state'] == 'forward_evidence_available'
        assert json.loads(row['outcome_json'])['evidence_kind'] == 'shared_page_id_reference'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[1],)).fetchone()) == before


@pytest.mark.parametrize('corruption', [None, 'parent_digest', 'child_head', 'child_digest',
                                        'child_observation_digest', 'child_member_ids'])
def test_pending_partition_requires_current_complete_child_proof(queue, corruption):
    parent, children = seed_partition(queue)
    capture(queue, children[0], source_payload([1, 2], epoch=bounds(children[0])[0]))
    capture(queue, children[1], source_payload([3, 4], epoch=bounds(children[1])[0]))
    assert head(queue, parent)['state'] == 'partition_list_complete'
    with queue.db:
        queue.db.execute("INSERT OR REPLACE INTO discovery(url,kind,provider,due,error_code,attempts) "
                         "VALUES (?,'cnyes_news','cnyes',9e9,'http_429',1)", (parent,))
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
            "VALUES (?,?,1,?,4,?,'2020-01-01T00:00:00+00:00')",
            (parent, cnyes_scope(parent), '0' * 64, '1' * 64))
        if corruption == 'parent_digest':
            queue.db.execute("UPDATE cnyes_history_scope_proofs SET proof_json='{}' WHERE proof_sha256=?",
                             (head(queue, parent)['proof_sha256'],))
        elif corruption == 'child_head':
            queue.db.execute("UPDATE cnyes_history_scope_heads SET state='waiting_page_evidence' WHERE scope_url=?",
                             (cnyes_scope(children[0]),))
        elif corruption == 'child_digest':
            queue.db.execute("UPDATE cnyes_history_scope_proofs SET proof_json='{}' WHERE proof_sha256=?",
                             (head(queue, children[0])['proof_sha256'],))
        elif corruption == 'child_observation_digest':
            queue.db.execute("UPDATE cnyes_history_page_observations SET observation_json='{}' WHERE observation_sha256="
                '(SELECT observation_sha256 FROM cnyes_history_page_heads WHERE scope_url=? AND page=1)',
                (cnyes_scope(children[0]),))
        elif corruption == 'child_member_ids':
            queue.db.execute("UPDATE cnyes_history_scope_heads SET member_ids_json='[1]' WHERE scope_url=?",
                             (cnyes_scope(children[0]),))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (parent,)).fetchone())
    backfill(queue, limit=8, max_seconds=10)
    state = queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (parent,)).fetchone()[0]
    assert (state == 'forward_evidence_available') == (corruption is None)
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (parent,)).fetchone()) == before


@pytest.mark.parametrize('extra', ['none', 'candidate_wake', 'wrong_scope', 'wrong_page'])
def test_ready_parent_pages_share_one_partition_verification(queue, monkeypatch, extra):
    import downloader.news_fulltext_cnyes_history as history
    # Isolate retained-work batching; the independent periodic integrity scan
    # is exercised by its own normal-owner and recovery tests.
    monkeypatch.setattr(history, 'sweep_completed_scopes', lambda *args, **kwargs:
        {'complete': True, 'checked': 0, 'invalid': 0, 'next_epoch': None, 'blocked': None})
    parent, children = seed_partition(queue)
    capture(queue, children[0], source_payload([1, 2], epoch=bounds(children[0])[0]))
    capture(queue, children[1], source_payload([3, 4], epoch=bounds(children[1])[0]))
    urls = [cnyes_url(BASE, page, start=bounds(parent)[0], end=bounds(parent)[1]) for page in range(1, 4)]
    if extra == 'wrong_scope':
        urls[-1] = cnyes_url(BASE, 3, start=bounds(parent)[0] + 86400, end=bounds(parent)[1] + 86400)
    elif extra == 'wrong_page':
        urls[-1] = cnyes_url(BASE, 4, start=bounds(parent)[0], end=bounds(parent)[1])
    with queue.db:
        for page, url in enumerate(urls, start=1):
            queue.db.execute("INSERT OR REPLACE INTO discovery(url,kind,provider,due,error_code,attempts) "
                             "VALUES (?,'cnyes_news','cnyes',9e9,'http_429',1)", (url,))
            queue.db.execute('INSERT INTO cnyes_history_retained_work '
                '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
                "VALUES (?,?,?, ?,4,?,'2020-01-01T00:00:00+00:00')",
                (url, cnyes_scope(parent), page, '0' * 64, '1' * 64))
        if extra == 'candidate_wake':
            previous = json.dumps({'error': 'invalid_original_envelope_receipt'})
            queue.db.execute("UPDATE cnyes_history_retained_work SET state='retained_invalid',outcome_json=? WHERE request_url=?",
                             (previous, urls[-1]))
            old = queue.db.execute('SELECT * FROM cnyes_history_retained_work WHERE request_url=?', (urls[-1],)).fetchone()
            history._save_retained_resolution(queue, old, 'pending', None, 'new_accepted_body_batch_candidate')
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    original = history._verified_partition_scope_proof
    calls = []
    def verify(*args, **kwargs):
        calls.append(args[1])
        return original(*args, **kwargs)
    monkeypatch.setattr(history, '_verified_partition_scope_proof', verify)
    result = backfill(queue, limit=8, max_seconds=10)
    assert calls == [cnyes_scope(parent)] and result['processed'] == len(urls)
    states = [queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                               (url,)).fetchone()[0] for url in urls]
    assert states[:2] == ['forward_evidence_available'] * 2
    assert (states[-1] == 'forward_evidence_available') == (extra == 'none')
    assert [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before
    assert queue.db.execute("SELECT count(*) FROM cnyes_history_retained_outcome_resolutions "
                            "WHERE evidence_kind='partition_scope_proof'").fetchone()[0] == (3 if extra == 'none' else 2)
    if extra == 'candidate_wake':
        prior = queue.db.execute('SELECT original_outcome_json FROM cnyes_history_retained_outcome_resolutions '
            "WHERE request_url=? AND evidence_kind='new_accepted_body_batch_candidate'", (urls[-1],)).fetchone()
        assert prior[0] == previous


def test_stop_during_partition_verification_preserves_pending_and_source_job(queue, monkeypatch):
    import threading
    import downloader.news_fulltext_cnyes_history as history
    parent, children = seed_partition(queue)
    capture(queue, children[0], source_payload([1, 2], epoch=bounds(children[0])[0]))
    capture(queue, children[1], source_payload([3, 4], epoch=bounds(children[1])[0]))
    with queue.db:
        queue.db.execute("INSERT OR REPLACE INTO discovery(url,kind,provider,due,error_code,attempts) "
                         "VALUES (?,'cnyes_news','cnyes',9e9,'http_429',1)", (parent,))
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
            "VALUES (?,?,1,?,4,?,'2020-01-01T00:00:00+00:00')",
            (parent, cnyes_scope(parent), '0' * 64, '1' * 64))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (parent,)).fetchone())
    stop = threading.Event()
    original = history._verified_partition_scope_proof
    def verify_and_stop(*args, **kwargs):
        result = original(*args, **kwargs)
        stop.set()
        return result
    monkeypatch.setattr(history, '_verified_partition_scope_proof', verify_and_stop)
    result = backfill(queue, stop=stop, limit=8, max_seconds=10)
    assert stop.is_set() and result['processed'] == 0
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                            (parent,)).fetchone()[0] == 'pending'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (parent,)).fetchone()) == before


def test_legacy_unavailable_scan_checkpoint_survives_restart(cfg, tmp_path):
    registry, _ = registry_http(cfg, tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as owner:
        urls = [seed_retained_work(owner, low=low)[0] for low in (1388534400, 1388620800)]
        for url in urls:
            capture(owner, url, source_payload([1, 2], epoch=bounds(url)[0]))
        with owner.db:
            owner.db.execute("UPDATE cnyes_history_retained_work SET state='retained_unavailable',outcome_json='{}'")
        result = reconcile_legacy_unavailable(owner, limit=1, max_seconds=10)
        assert not result['complete'] and result['resolved'] == 1
    with closing(NewsQueue(tmp_path, registry)) as resumed:
        for _ in range(3):
            result = reconcile_legacy_unavailable(resumed, limit=1, max_seconds=10)
            if result['complete']:
                break
        assert result['complete']
        assert dict(resumed.db.execute('SELECT state,count(*) FROM cnyes_history_retained_work GROUP BY state')) == {'forward_evidence_available': 2}
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_retained_outcome_resolutions').fetchone()[0] == 2


@pytest.mark.parametrize('tamper', [None, 'prior_ids', 'reference_receipt'])
def test_legacy_unavailable_shared_page_requires_original_identity_receipt(queue, tamper):
    from downloader.news_fulltext_cnyes_history import index_scope
    urls, _ = seed_legacy_overflow(queue)
    source = queue.registry.sources['cnyes']
    r = response(source_payload(range(1, 31), epoch=bounds(urls[0])[0], total=931), urls[0])
    info, posts = cnyes_pagination(r)
    sha = hashlib.sha256(r.body).hexdigest()
    with queue.db:
        for url in urls:
            index_scope(queue, cnyes_scope(url), source.provider)
        record_page(queue, r, info, posts, source, {'kind': 'fixture'}, observed_at='2020-01-01T00:00:00+00:00')
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc,state,outcome_json) '
            "VALUES (?,?,1,?,931,?,'old-clock','retained_unavailable','{}')",
            (urls[1], cnyes_scope(urls[1]), sha, info['ids_sha256']))
        if tamper == 'prior_ids':
            queue.db.execute("UPDATE cnyes_history_retained_work SET expected_ids_sha256='wrong'")
        elif tamper == 'reference_receipt':
            queue.db.execute("UPDATE cnyes_history_page_ID_references SET original_ID_evidence_json='{}'")
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    result = reconcile_legacy_unavailable(queue, max_seconds=10)
    assert result['complete']
    state = queue.db.execute('SELECT state FROM cnyes_history_retained_work').fetchone()[0]
    if tamper is None:
        assert result['resolved'] == 1 and state == 'forward_evidence_available'
        receipt = queue.db.execute('SELECT evidence_kind,observation_sha256 FROM cnyes_history_retained_outcome_resolutions').fetchone()
        assert receipt['evidence_kind'] == 'shared_page_id_reference' and receipt['observation_sha256']
    else:
        assert result['resolved'] == 0 and state == 'retained_unavailable'
        assert queue.db.execute('SELECT count(*) FROM cnyes_history_retained_outcome_resolutions').fetchone()[0] == 0
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before


@pytest.mark.parametrize('candidate_pending', [False, True])
@pytest.mark.parametrize('error', ['invalid_original_envelope_receipt',
                                   'retained_original_request_differs_not_relabelled'])
def test_invalid_original_requires_direct_page_unless_only_request_identity_differs(queue, candidate_pending, error):
    from downloader.news_fulltext_cnyes_history import index_scope, _save_retained_resolution
    urls, _ = seed_legacy_overflow(queue)
    source = queue.registry.sources['cnyes']
    r = response(source_payload(range(1, 31), epoch=bounds(urls[0])[0], total=931), urls[0])
    info, posts = cnyes_pagination(r)
    sha = hashlib.sha256(r.body).hexdigest()
    previous = json.dumps({'error': error})
    with queue.db:
        for url in urls:
            index_scope(queue, cnyes_scope(url), source.provider)
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc,state,outcome_json) '
            "VALUES (?,?,1,?,931,?,'old-clock','retained_invalid',?)",
            (urls[1], cnyes_scope(urls[1]), sha, info['ids_sha256'], previous))
        if candidate_pending:
            work = queue.db.execute('SELECT * FROM cnyes_history_retained_work WHERE request_url=?', (urls[1],)).fetchone()
            _save_retained_resolution(queue, work, 'pending', None, 'new_accepted_body_batch_candidate')
        record_page(queue, r, info, posts, source, {'kind': 'fixture'}, observed_at='2020-01-01T00:00:00+00:00')
    assert queue.db.execute('SELECT 1 FROM cnyes_history_page_ID_references WHERE scope_url=?',
                            (cnyes_scope(urls[1]),)).fetchone()
    state = queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (urls[1],)).fetchone()[0]
    if error == 'invalid_original_envelope_receipt':
        assert state == ('pending' if candidate_pending else 'retained_invalid')
        assert queue.db.execute("SELECT 1 FROM cnyes_history_retained_outcome_resolutions WHERE request_url=? "
                                "AND resolution_state='forward_evidence_available'", (urls[1],)).fetchone() is None
    else:
        assert state == 'forward_evidence_available'
        resolution = queue.db.execute('SELECT * FROM cnyes_history_retained_outcome_resolutions WHERE request_url=? '
                                      "AND resolution_state='forward_evidence_available'", (urls[1],)).fetchone()
        assert resolution['original_state'] == 'retained_invalid' and resolution['original_outcome_json'] == previous
        assert resolution['evidence_kind'] == 'shared_page_id_reference'


def test_changed_bulk_contract_cannot_reuse_old_child_proof_for_parent(queue, cfg, tmp_path):
    from downloader.news_fulltext_cnyes_history import reconcile_parents
    parent, children = seed_partition(queue)
    capture(queue, children[0], source_payload([1, 2], epoch=bounds(children[0])[0]))
    capture(queue, children[1], source_payload([3, 4], epoch=bounds(children[1])[0]))
    changed_cfg = deepcopy(cfg)
    changed_cfg['providers'][0]['cnyes_apis'].append(BASE.replace('headline', 'stock_report'))
    changed_registry, _ = registry_http(changed_cfg, tmp_path)
    changed = changed_registry.sources['cnyes']
    assert changed.cnyes_bulk_data_fingerprint != queue.registry.sources['cnyes'].cnyes_bulk_data_fingerprint
    with queue.db:
        reconcile_parents(queue, cnyes_scope(children[0]), changed)
    assert head(queue, parent)['state'] == 'waiting_child_evidence'
    assert queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(parent),)).fetchone()


def test_source_body_and_discovery_contracts_do_not_change_with_metadata_ABI(monkeypatch):
    from downloader.news_fulltext import discovery_fingerprint, load_config
    from pathlib import Path
    _, before, _ = load_config(Path('configs/news_fulltext.json'))
    monkeypatch.setattr('downloader.news_fulltext_cnyes_history.CONTRACT_VERSION', CONTRACT_VERSION + 1)
    _, after, _ = load_config(Path('configs/news_fulltext.json'))
    for provider, source in before.sources.items():
        assert source.data_fingerprint == after.sources[provider].data_fingerprint
        assert discovery_fingerprint(source) == discovery_fingerprint(after.sources[provider])


def test_page_evidence_can_be_distributed_across_two_original_queries(queue):
    exemplar, alias = seed_equivalent(queue)
    low, high = bounds(exemplar)
    exemplar_second = cnyes_url(BASE, 2, start=low, end=high)
    info, _ = cnyes_pagination(response(source_payload([31], epoch=low, page=2, total=31), exemplar_second))
    with queue.db:
        queue.db.execute('INSERT INTO bulk_page_evidence VALUES (?,?,?,?,?)',
                         (cnyes_scope(exemplar), 2, 31, info['ids_sha256'], 'real-exemplar-page-clock'))
    capture(queue, exemplar, source_payload(range(1, 31), epoch=low, total=31))
    alias_second = cnyes_url(BASE, 2, start=bounds(alias)[0], end=bounds(alias)[1])
    capture(queue, alias_second, source_payload([31], epoch=low, page=2, total=31))
    for url in (exemplar, alias):
        assert head(queue, url)['state'] == 'list_complete_by_page_ID_equivalence'
        assert json.loads(head(queue, url)['member_ids_json']) == list(range(1, 32))
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 2
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_ID_references').fetchone()[0] == 2


@pytest.mark.parametrize('failure', [None, 'http_403', 'http_429', 'transport_error'])
def test_unmatched_page_falls_back_once_without_resetting_source_failures(queue, failure):
    exemplar, alias = seed_equivalent(queue, change_page2=True)
    direct = cnyes_url(BASE, 2, start=bounds(alias)[0], end=bounds(alias)[1])
    with queue.db:
        queue.db.execute('UPDATE discovery SET error_code=?,attempts=? WHERE url=?', (failure, int(failure is not None), direct))
    prior = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (direct,)).fetchone())
    capture(queue, exemplar, source_payload(range(1, 31), epoch=bounds(exemplar)[0], total=31))
    second = cnyes_url(BASE, 2, start=bounds(exemplar)[0], end=bounds(exemplar)[1])
    capture(queue, second, source_payload([31], epoch=bounds(exemplar)[0], page=2, total=31))
    current = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (direct,)).fetchone())
    assert head(queue, alias)['state'] == 'waiting_original_page_recheck'
    if failure:
        assert current == prior
    else:
        assert current['due'] < prior['due']
        assert {k: v for k, v in current.items() if k != 'due'} == {k: v for k, v in prior.items() if k != 'due'}
    capture(queue, second, source_payload([31], epoch=bounds(exemplar)[0], page=2, total=31))
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (direct,)).fetchone()) == current


def test_changed_shared_page_reopens_completed_alias_until_original_source_check(queue):
    exemplar, alias = seed_equivalent(queue)
    low, high = bounds(exemplar)
    capture(queue, exemplar, source_payload(range(1, 31), epoch=low, total=31))
    second = cnyes_url(BASE, 2, start=low, end=high)
    capture(queue, second, source_payload([31], epoch=low, page=2, total=31))
    assert head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence'
    original_proof = head(queue, alias)['proof_sha256']
    capture(queue, second, source_payload([32], epoch=low, page=2, total=31))
    assert head(queue, alias)['state'] == 'waiting_original_page_recheck'
    assert queue.db.execute('SELECT 1 FROM cnyes_history_scope_proofs WHERE proof_sha256=?', (original_proof,)).fetchone()
    direct = cnyes_url(BASE, 2, start=bounds(alias)[0], end=bounds(alias)[1])
    capture(queue, direct, source_payload([32], epoch=low, page=2, total=31))
    assert head(queue, alias)['state'] == 'list_complete_by_page_ID_equivalence'
    assert json.loads(head(queue, alias)['member_ids_json']) == [*range(1, 31), 32]


def test_revised_source_total_ignores_obsolete_pages_but_preserves_observations(queue):
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    capture(queue, url, source_payload(range(1, 31), epoch=low, total=31))
    capture(queue, cnyes_url(BASE, 2, start=low, end=low + 100), source_payload([31], epoch=low, page=2, total=31))
    capture(queue, url, source_payload([1], epoch=low))
    assert head(queue, url)['state'] == 'list_complete'
    assert json.loads(head(queue, url)['member_ids_json']) == [1]
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 3


def test_forward_page_cap_growth_invalidates_old_complete_head(queue):
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    capture(queue, url, source_payload([1], epoch=low))
    assert head(queue, url)['state'] == 'list_complete'
    capture(queue, url, source_payload(range(1, 31), epoch=low, total=1000))
    assert head(queue, url)['state'] == 'source_page_cap_exceeded'
    assert queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(url),)).fetchone()


def test_legacy_zero_total_reuses_its_exact_retained_empty_envelope(queue):
    from downloader.news_fulltext_cnyes_history import initialize
    parent, children = seed_partition(queue, total=1)
    capture(queue, children[0], source_payload([], epoch=bounds(children[0])[0]))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (children[0],)).fetchone())
    with queue.db:
        for table in ('cnyes_history_page_heads', 'cnyes_history_page_observations', 'cnyes_history_scope_heads'):
            queue.db.execute('DELETE FROM ' + table)
        queue.db.execute("DELETE FROM queue_settings WHERE key='cnyes_history_evidence_initialization_v1'")
    initialize(queue)
    row = queue.db.execute('SELECT * FROM cnyes_history_retained_work').fetchone()
    assert row['expected_total'] == 0 and row['state'] == 'pending'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 0
    backfill(queue, max_seconds=10)
    assert head(queue, children[0])['state'] == 'list_complete'
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    assert observation['request_url'] == children[0]
    assert observation['artifact']['kind'] == 'cnyes_history_source_envelope'
    assert observation['origin'] == 'verified_retained_source_response'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (children[0],)).fetchone()) == before
    assert queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(parent),)).fetchone()


def test_legacy_zero_total_without_retained_envelope_never_creates_a_page(queue):
    from downloader.news_fulltext_cnyes_history import initialize
    parent, children = seed_partition(queue, total=1)
    capture(queue, children[0], source_payload([], epoch=bounds(children[0])[0]))
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    path = queue.root / observation['artifact']['parquet_path']
    path.unlink()
    path.with_suffix('.parquet.receipt.json').unlink()
    with queue.db:
        for table in ('cnyes_history_page_heads', 'cnyes_history_page_observations', 'cnyes_history_scope_heads'):
            queue.db.execute('DELETE FROM ' + table)
        queue.db.execute("DELETE FROM queue_settings WHERE key='cnyes_history_evidence_initialization_v1'")
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=1,due=9e9 WHERE url=?", (children[0],))
    initialize(queue)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (children[0],)).fetchone())
    backfill(queue, max_seconds=10)
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work').fetchone()[0] == 'retained_unavailable'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 0
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 0
    assert queue.db.execute('SELECT 1 FROM bulk_window_gaps WHERE scope_url=?', (cnyes_scope(parent),)).fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (children[0],)).fetchone()) == before


def test_incomplete_empty_scope_checks_its_existing_discovery_envelope_first(queue):
    from downloader.news_fulltext_cnyes_history import reconcile_scope
    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    with queue.db:
        for table in ('cnyes_history_page_heads', 'cnyes_history_page_observations', 'cnyes_history_scope_heads'):
            queue.db.execute('DELETE FROM ' + table)
        queue.db.execute('UPDATE discovery SET due=9e9 WHERE url=?', (url,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    with queue.db:
        reconcile_scope(queue, cnyes_scope(url), queue.registry.sources['cnyes'])
    assert head(queue, url)['state'] == 'waiting_page_evidence'
    work = queue.db.execute('SELECT * FROM cnyes_history_retained_work WHERE request_url=?', (url,)).fetchone()
    assert work is not None and work['expected_total'] == 0 and work['state'] == 'pending'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    assert backfill(queue, max_seconds=10)['processed'] == 1
    assert head(queue, url)['state'] == 'list_complete'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_owner_discovers_unindexed_legacy_empty_scope_without_rechecking_source(queue):
    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    complete = cnyes_url(BASE, start=1388534600, end=1388534700)
    capture(queue, url, source_payload([], epoch=bounds(url)[0]))
    capture(queue, complete, source_payload([1], epoch=bounds(complete)[0]))
    complete_before = head(queue, complete)
    with queue.db:
        for table in ('cnyes_history_page_heads', 'cnyes_history_page_observations', 'cnyes_history_scope_heads'):
            queue.db.execute('DELETE FROM ' + table + ' WHERE scope_url=?', (cnyes_scope(url),))
        queue.db.execute('UPDATE discovery SET due=9e9 WHERE url=?', (url,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    result = backfill(queue, max_seconds=10)
    assert result['processed'] == 1
    assert head(queue, url)['state'] == 'list_complete'
    assert head(queue, complete) == complete_before
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1


def test_unindexed_empty_same_response_from_other_request_requires_own_source_check(queue):
    original = cnyes_url(BASE, start=1388534400, end=1388534500)
    missing = cnyes_url(BASE, start=1388534600, end=1388534700)
    r = capture(queue, original, source_payload([], epoch=bounds(original)[0]))
    capture(queue, missing, source_payload([], epoch=bounds(missing)[0]))
    # Recreate a legacy content-only file with the first request's bytes, and
    # remove the second original: identical bytes alone must not prove it.
    first = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations WHERE scope_url=?',
                                        (cnyes_scope(original),)).fetchone()[0])['artifact']
    second = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations WHERE scope_url=?',
                                         (cnyes_scope(missing),)).fetchone()[0])['artifact']
    legacy = queue.root / 'discovery_responses' / (hashlib.sha256(r.body).hexdigest() + '.parquet')
    shutil.copyfile(queue.root / first['parquet_path'], legacy)
    # A content-only legacy receipt did not claim a parquet_path or a newer
    # envelope contract. Copying the modern receipt here would instead test a
    # contradictory path, which must fail before request identity is checked.
    atomic_write_json(legacy.with_suffix('.parquet.receipt.json'), {
        'sha256': sha256_file(legacy), 'response_sha256': hashlib.sha256(r.body).hexdigest(),
        'publish': False}, durable=True)
    (queue.root / second['parquet_path']).unlink()
    (queue.root / second['receipt_path']).unlink()
    with queue.db:
        for table in ('cnyes_history_page_heads', 'cnyes_history_page_observations', 'cnyes_history_scope_heads'):
            queue.db.execute('DELETE FROM ' + table + ' WHERE scope_url=?', (cnyes_scope(missing),))
        queue.db.execute('UPDATE discovery SET due=9e9 WHERE url=?', (missing,))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (missing,)).fetchone())
    result = backfill(queue, max_seconds=10)
    assert result['processed'] == 1
    work = queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (missing,)).fetchone()
    assert work['state'] == 'retained_request_mismatch'
    assert head(queue, missing)['state'] == 'waiting_page_evidence'
    assert head(queue, original)['state'] == 'list_complete'
    after = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (missing,)).fetchone())
    assert after['due'] < before['due'] and after['error_code'] is None
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0


def test_unindexed_empty_scan_resumes_in_bounded_slices_and_preserves_failed_job(queue):
    from downloader.news_fulltext_cnyes_history import seed_unobserved_empty_scopes
    urls = [cnyes_url(BASE, start=1388534400 + i * 100, end=1388534450 + i * 100) for i in range(5)]
    with queue.db:
        for index, url in enumerate(urls):
            queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'cnyes',0,1,'old-source-clock')", (cnyes_scope(url),))
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due,error_code,attempts) "
                             "VALUES (?,'cnyes_news','cnyes',9e9,?,?)",
                             (url, 'http_429' if index == 3 else None, int(index == 3)))
    failed_before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[3],)).fetchone())
    first = seed_unobserved_empty_scopes(queue, limit=2, max_seconds=10)
    assert first['scanned'] == 2 and not first['complete']
    queue.close()  # Release the owner before reopening the same test queue.
    resumed = NewsQueue(queue.root, queue.registry)
    try:
        second = seed_unobserved_empty_scopes(resumed, limit=2, max_seconds=10)
        last = seed_unobserved_empty_scopes(resumed, limit=2, max_seconds=10)
        assert second['scanned'] == 2 and not second['complete']
        assert last['scanned'] == 1 and last['complete']
        assert first['reconciled'] + second['reconciled'] + last['reconciled'] == 4
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (urls[3],)).fetchone()) == failed_before
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_scope_heads').fetchone()[0] == 4
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 0
        assert seed_unobserved_empty_scopes(resumed, limit=2, max_seconds=10)['scanned'] == 0
    finally:
        resumed.close()


def seed_unindexed_nonzero_pages(queue, *, failed_last=False):
    low = 1388534400
    urls = [cnyes_url(BASE, page, start=low, end=low + 100) for page in (1, 2, 3)]
    source = queue.registry.sources['cnyes']
    payloads = [source_payload(range(1, 31), epoch=low, page=1, total=61),
                source_payload(range(31, 61), epoch=low, page=2, total=61),
                source_payload([61], epoch=low, page=3, total=61)]
    with queue.db:
        queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'cnyes',61,3,'2020-01-01T00:00:00+00:00')",
                         (cnyes_scope(urls[0]),))
        for page, (url, data) in enumerate(zip(urls, payloads), 1):
            r = response(data, url)
            info, _ = cnyes_pagination(r)
            sha = hashlib.sha256(r.body).hexdigest()
            failure = 'http_429' if failed_last and page == 3 else None
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due,etag,error_code,attempts,response_sha256,last_checked_at_utc) "
                "VALUES (?,'cnyes_news','cnyes',9e9,'old-etag',?,?,?,'2020-01-01T00:00:00+00:00')",
                (url, failure, int(failure is not None), sha))
            queue.db.execute('INSERT INTO bulk_page_evidence VALUES (?,?,?,?,?)',
                (cnyes_scope(url), page, 61, info['ids_sha256'], '2020-01-01T00:00:00+00:00'))
            if page < 3:
                target = queue.root / ('quarantine' if page == 1 else 'discovery_responses') / (sha + '.parquet')
                atomic_write_parquet(target, table_from_records([{'request_url': url, 'raw_response_body': r.body,
                    'response_sha256': sha, 'captured_at_utc': '2020-01-01T00:00:00+00:00',
                    'authorization_fingerprint': source.authorization_fingerprint, 'publish': False}]), durable=True)
                atomic_write_json(target.with_suffix('.parquet.receipt.json'), {
                    'sha256': sha256_file(target), 'response_sha256': sha, 'publish': False}, durable=True)
    return urls


def test_nonzero_local_inventory_recovers_all_available_pages_before_source_recheck(queue):
    from downloader.news_fulltext_cnyes_history import seed_unobserved_nonzero_pages
    urls = seed_unindexed_nonzero_pages(queue)
    with queue.db:
        queue.add_articles([{'url': 'https://news.cnyes.com/news/id/987654321', 'title': 'pending', 'date': None}])
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    first = seed_unobserved_nonzero_pages(queue, limit=1, max_seconds=10)
    assert first['queued'] == 1 and not first['complete']
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        for _ in range(10):
            scan = seed_unobserved_nonzero_pages(resumed, limit=1, max_seconds=10)
            if scan['complete']:
                break
        assert scan['complete']
        report = progress(resumed.db, resumed.registry.sources['cnyes'])['unobserved_nonzero_retained_scan']
        assert report['scan_state'] == 'complete' and report['queued_candidate_pages'] == 2
        assert report['checked_steps'] >= 3 and report['all_history_complete'] is False
        assert urls[0] not in json.dumps(report)
        backfill(resumed, limit=1, max_seconds=10)
        assert resumed.db.execute("SELECT count(*) FROM cnyes_history_retained_work WHERE state='pending'").fetchone()[0] == 1
        assert [dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before
        for _ in range(10):
            result = backfill(resumed, limit=8, max_seconds=10)
            if resumed.db.execute("SELECT count(*) FROM cnyes_history_retained_work WHERE state='pending'").fetchone()[0] == 0:
                break
        assert head(resumed, urls[0])['state'] == 'waiting_page_evidence'
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 2
        # A continuous body backlog cannot starve the last missing page.
        repair_incomplete_scopes(resumed, max_seconds=10)
        after = [dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
        assert after[:2] == before[:2]
        assert after[2]['due'] < before[2]['due'] and after[2]['etag'] is None
        assert resumed.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0


def test_nonzero_local_inventory_never_resets_failed_source_job(queue):
    from downloader.news_fulltext_cnyes_history import seed_unobserved_nonzero_pages
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    for _ in range(10):
        scan = seed_unobserved_nonzero_pages(queue, limit=8, max_seconds=10)
        if scan['complete']:
            break
    assert scan['complete']
    for _ in range(10):
        backfill(queue, max_seconds=10)
        if queue.db.execute("SELECT count(*) FROM cnyes_history_retained_work WHERE state='pending'").fetchone()[0] == 0:
            break
    repair_incomplete_scopes(queue, max_seconds=10)
    assert head(queue, urls[0])['state'] == 'waiting_page_evidence'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before


def test_nonzero_inventory_without_any_local_original_still_tracks_missing_pages(queue):
    from downloader.news_fulltext_cnyes_history import _retained_envelopes, seed_unobserved_nonzero_pages

    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    for url in urls:
        sha = queue.db.execute('SELECT response_sha256 FROM discovery WHERE url=?', (url,)).fetchone()[0]
        for path, _ in _retained_envelopes(queue.root, sha):
            path.unlink(missing_ok=True)
            path.with_suffix('.parquet.receipt.json').unlink(missing_ok=True)
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    assert not seed_unobserved_nonzero_pages(queue, limit=1, max_seconds=10)['complete']
    assert queue.db.execute('SELECT 1 FROM cnyes_history_scope_heads').fetchone() is None
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        for _ in range(10):
            result = seed_unobserved_nonzero_pages(resumed, limit=1, max_seconds=10)
            if result['complete']:
                break
        assert result['complete'] and head(resumed, urls[0])['state'] == 'waiting_page_evidence'
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 0
        after = [dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
                 for url in urls]
        assert all(after[i]['due'] < before[i]['due'] and after[i]['etag'] is None for i in (0, 1))
        assert after[2] == before[2]  # A source refusal retains its own deadline and evidence.
        result = repair_incomplete_scopes(resumed, limit=8, max_seconds=10)
        assert result['page_checks_scheduled'] == 0  # Already scheduled once, not again.
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_retained_work').fetchone()[0] == 0


def test_incomplete_scope_sweep_continues_without_a_five_minute_pause(queue):
    from downloader.news_fulltext_cnyes_history import (
        REPAIR_CURSOR, maintenance_delay, save_proof, seed_unobserved_nonzero_pages,
    )

    urls = []
    with queue.db:
        for i in range(4):
            low = 1388534400 + i * 200
            url = cnyes_url(BASE, start=low, end=low + 100)
            urls.append(url)
            r = response(source_payload([i + 1], epoch=low, total=1), url)
            info, _ = cnyes_pagination(r)
            queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'cnyes',1,1,'2020-01-01T00:00:00+00:00')",
                             (cnyes_scope(url),))
            queue.db.execute("INSERT INTO bulk_page_evidence VALUES (?,1,1,?,'2020-01-01T00:00:00+00:00')",
                             (cnyes_scope(url), info['ids_sha256']))
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due,etag,error_code,attempts,response_sha256,last_checked_at_utc) "
                "VALUES (?,'cnyes_news','cnyes',9e9,'old-etag',?,?,?,'2020-01-01T00:00:00+00:00')",
                (url, 'http_429' if i == 3 else None, int(i == 3), hashlib.sha256(r.body).hexdigest()))
            save_proof(queue, cnyes_scope(url), queue.registry.sources['cnyes'],
                       'waiting_page_evidence' if i % 2 == 0 else 'inconsistent_source_snapshot', {}, set())
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    assert seed_unobserved_nonzero_pages(queue, limit=32, max_seconds=10)['complete']
    first = backfill(queue, limit=1, max_seconds=10)
    assert first['evidence_repair']['scopes_checked'] == 1
    assert not first['exhausted'] and maintenance_delay(first) == 0
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        for _ in range(10):
            result = backfill(resumed, limit=1, max_seconds=10)
            if result['exhausted']:
                break
        assert result['exhausted']
        assert resumed.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (REPAIR_CURSOR,)).fetchone() is None
        after = [dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
        assert all(after[i]['due'] < before[i]['due'] for i in range(3))
        assert after[3] == before[3]


def test_nonzero_complete_local_inventory_retires_its_fence_without_recheck(queue):
    from downloader.news_fulltext_cnyes_history import seed_unobserved_nonzero_pages
    urls = seed_unindexed_nonzero_pages(queue)
    source = queue.registry.sources['cnyes']
    r = response(source_payload([61], epoch=1388534400, page=3, total=61), urls[2])
    sha = hashlib.sha256(r.body).hexdigest()
    path = queue.root / 'quarantine' / (sha + '.parquet')
    atomic_write_parquet(path, table_from_records([{'request_url': urls[2], 'raw_response_body': r.body,
        'response_sha256': sha, 'captured_at_utc': '2020-01-01T00:00:00+00:00',
        'authorization_fingerprint': source.authorization_fingerprint, 'publish': False}]), durable=True)
    atomic_write_json(path.with_suffix('.parquet.receipt.json'), {
        'sha256': sha256_file(path), 'response_sha256': sha, 'publish': False}, durable=True)
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    assert seed_unobserved_nonzero_pages(queue, limit=8, max_seconds=10)['complete']
    backfill(queue, limit=8, max_seconds=10)
    assert head(queue, urls[0])['state'] == 'list_complete'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_local_inventory_scopes').fetchone()[0] == 0
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 3
    assert [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before


def seed_accepted_batch_for_history_page(queue, url, data, *, original_url=None, authorization=None,
                                         batch_tag='accepted'):
    r = response(data, url)
    sha = hashlib.sha256(r.body).hexdigest()
    relative = f'bodies/20261028/20261028T120000000000Z-{batch_tag}.parquet'
    receipt_relative = f'receipts/20261028T120000000000Z-{batch_tag}.json'
    target = queue.root / relative
    row = {'provider': 'cnyes', 'source_request_url': original_url or url,
        'raw_response_body': r.body, 'response_sha256': sha, 'captured_at_utc': '2020-01-01T00:00:00+00:00',
        'authorization_fingerprint': authorization or queue.registry.sources['cnyes'].authorization_fingerprint,
        'publish': False}
    atomic_write_parquet(target, table_from_records([row]), durable=True)
    atomic_write_json(queue.root / receipt_relative, {'contract_version': 1, 'parquet_path': relative,
        'sha256': sha256_file(target), 'rows': 1, 'publish': False}, durable=True)
    with queue.db:
        queue.db.execute('INSERT INTO accepted_batches VALUES (?,?)', (relative, receipt_relative))
    return relative, receipt_relative


def test_accepted_batch_index_recovers_exact_missing_history_page_without_copy_or_request(queue):
    from downloader.news_fulltext_cnyes_history import (
        retry_invalid_accepted_batches, seed_accepted_body_batch_candidates, seed_unobserved_nonzero_pages,
    )
    urls = seed_unindexed_nonzero_pages(queue)
    relative, receipt = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    first = seed_accepted_body_batch_candidates(queue, limit=1, max_seconds=10)
    assert first['indexed'] == 1 and first['complete']
    report = progress(queue.db, queue.registry.sources['cnyes'])['accepted_body_batch_scan']
    assert report['scan_state'] == 'scanning' and report['local_invalid_migration_state'] == 'not_started'
    assert report['scanned_batches'] == report['indexed_candidates'] == 1
    assert relative not in json.dumps(report) and report['all_history_complete'] is False
    assert retry_invalid_accepted_batches(queue, limit=8, max_seconds=10)['migration']['complete']
    report = progress(queue.db, queue.registry.sources['cnyes'])['accepted_body_batch_scan']
    assert report['scan_state'] == report['local_invalid_migration_state'] == 'complete'
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        for _ in range(10):
            scan = seed_accepted_body_batch_candidates(resumed, limit=1, max_seconds=10)
            if scan['complete']:
                break
        assert scan['complete']
        assert seed_unobserved_nonzero_pages(resumed, limit=8, max_seconds=10)['queued'] == 3
        backfill(resumed, limit=8, max_seconds=10)
        assert head(resumed, urls[0])['state'] == 'list_complete'
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 3
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_local_inventory_scopes').fetchone()[0] == 0
        observation = json.loads(resumed.db.execute('SELECT o.observation_json FROM cnyes_history_page_heads h '
            'JOIN cnyes_history_page_observations o ON o.observation_sha256=h.observation_sha256 '
            'WHERE h.scope_url=? AND h.page=3', (cnyes_scope(urls[0]),)).fetchone()[0])
        assert observation['source_observed_at_utc'] == '2020-01-01T00:00:00+00:00'
        assert observation['artifact'] == {'kind': 'accepted_body_batch', 'parquet_path': relative, 'receipt_path': receipt}
        assert [dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before
        assert resumed.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1


@pytest.mark.parametrize('raw_position', [0, 1, 15, 16, 29])
def test_accepted_batch_recovers_original_at_each_possible_chunk_position(queue, raw_position):
    import pyarrow.parquet as pq
    from downloader.news_fulltext_cnyes_history import seed_accepted_body_batch_candidates
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    relative, receipt_relative = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    path = queue.root / relative
    row = pq.read_table(path).to_pylist()[0]
    records = [{**row, 'raw_response_body': row['raw_response_body'] if i == raw_position else None}
               for i in range(30)]
    atomic_write_parquet(path, table_from_records(records), durable=True)
    receipt = json.loads((queue.root / receipt_relative).read_text())
    receipt.update(sha256=sha256_file(path), rows=len(records))
    atomic_write_json(queue.root / receipt_relative, receipt, durable=True)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    assert seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['indexed'] == 1
    for _ in range(10):
        backfill(queue, limit=8, max_seconds=10)
        if queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone():
            break
    assert head(queue, urls[0])['state'] == 'list_complete'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before


def test_existing_scope_waits_for_unscanned_body_batch_before_source_recheck(queue):
    from downloader.news_fulltext_cnyes_history import reconcile_scope, seed_accepted_body_batch_candidates
    urls = seed_unindexed_nonzero_pages(queue)
    seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    with queue.db:
        reconcile_scope(queue, cnyes_scope(urls[0]), queue.registry.sources['cnyes'])
    assert head(queue, urls[0])['state'] == 'waiting_page_evidence'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before
    assert seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['complete']
    for _ in range(4):
        backfill(queue, limit=8, max_seconds=10)
        if head(queue, urls[0])['state'] == 'list_complete':
            break
    assert head(queue, urls[0])['state'] == 'list_complete'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before


def test_pending_retained_work_waits_when_batch_index_temporarily_unreadable(queue, monkeypatch):
    import downloader.news_fulltext_cnyes_history as history
    urls = seed_unindexed_nonzero_pages(queue)
    seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    page = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone()
    job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc) '
            'VALUES (?,?,?,?,?,?,?)', (urls[2], cnyes_scope(urls[0]), 3, job['response_sha256'],
                                       page['total'], page['ids_sha256'], page['observed_at_utc']))
    original = history._accepted_batch_metadata_pairs
    def unavailable(*args, **kwargs):
        raise OSError('temporary local disk read failure')
    monkeypatch.setattr(history, '_accepted_batch_metadata_pairs', unavailable)
    result = backfill(queue, limit=8, max_seconds=10)
    assert result['accepted_body_batch_scan']['blocked'] == 'local_retry_wait'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == job
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                            (urls[2],)).fetchone()[0] in ('pending', 'local_retry')
    monkeypatch.setattr(history, '_accepted_batch_metadata_pairs', original)
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_accepted_batch_local_retries SET not_before_epoch=0')
    for _ in range(5):
        backfill(queue, limit=8, max_seconds=10)
        if queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone():
            break
    assert queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == job


@pytest.mark.parametrize('previous', ['retained_unavailable', 'retained_invalid', 'retained_request_mismatch'])
def test_new_batch_candidate_revisits_old_failed_local_work_without_losing_outcome(queue, previous):
    from downloader.news_fulltext_cnyes_history import seed_accepted_body_batch_candidates
    urls = seed_unindexed_nonzero_pages(queue)
    seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    page = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone()
    job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    old_outcome = json.dumps({'error': 'preserve_original_failed_candidate'})
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,'
            'source_checked_at_utc,state,outcome_json) VALUES (?,?,?,?,?,?,?,?,?)',
            (urls[2], cnyes_scope(urls[0]), 3, job['response_sha256'], page['total'],
             page['ids_sha256'], page['observed_at_utc'], previous, old_outcome))
    assert seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['complete']
    work = queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (urls[2],)).fetchone()
    assert work['state'] == 'pending'
    audit = queue.db.execute('SELECT original_state,original_outcome_json,resolution_state,evidence_kind '
        'FROM cnyes_history_retained_outcome_resolutions WHERE request_url=?', (urls[2],)).fetchone()
    assert tuple(audit) == (previous, old_outcome, 'pending', 'new_accepted_body_batch_candidate')
    for _ in range(5):
        backfill(queue, limit=8, max_seconds=10)
        if queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone():
            break
    assert queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == job


def test_absent_batch_after_inventory_allows_normal_source_recheck(queue):
    from downloader.news_fulltext_cnyes_history import reconcile_scope, seed_accepted_body_batch_candidates
    urls = seed_unindexed_nonzero_pages(queue)
    # The only accepted batch belongs to page 2, whose content is already
    # available in a separate original envelope; page 3 remains unproven.
    seed_accepted_batch_for_history_page(queue, urls[1],
        source_payload(range(31, 61), epoch=1388534400, page=2, total=61))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    with queue.db:
        reconcile_scope(queue, cnyes_scope(urls[0]), queue.registry.sources['cnyes'])
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before
    assert seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['complete']
    with queue.db:
        reconcile_scope(queue, cnyes_scope(urls[0]), queue.registry.sources['cnyes'])
    after = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    assert after['due'] < before['due'] and after['etag'] is None
    assert after['error_code'] is None and after['attempts'] == 0
    assert queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone() is None


def test_live_accepted_cnyes_raw_registers_reference_in_same_commit(queue):
    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    r = capture(queue, url, source_payload([1], epoch=1388534400))
    row = queue.db.execute('SELECT * FROM cnyes_history_accepted_batch_candidates WHERE request_url=?', (url,)).fetchone()
    assert row and row['response_sha256'] == hashlib.sha256(r.body).hexdigest()
    assert queue.db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (row['parquet_path'],)).fetchone()[0] == row['receipt_path']
    assert head(queue, url)['state'] == 'list_complete'


@pytest.mark.parametrize('history_absent', [False, True])
def test_startup_recovers_orphan_cnyes_batch_before_legacy_index_exists(queue, history_absent):
    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    r = capture(queue, url, source_payload([1], epoch=1388534400))
    accepted = tuple(queue.db.execute('SELECT path,receipt_path FROM accepted_batches').fetchone())
    # Simulate an old schema and a crash after durable files but before the
    # SQLite batch commit. Startup must replay the receipt and index its raw
    # request in the same commit, even on the first boot after an upgrade.
    with queue.db:
        queue.db.execute('DELETE FROM accepted_batches')
        queue.db.execute('DROP TABLE cnyes_history_accepted_batch_candidates')
        if history_absent:
            queue.db.execute('DROP TABLE cnyes_history_retained_work')
            queue.db.execute('DROP TABLE cnyes_history_retained_outcome_resolutions')
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        assert tuple(resumed.db.execute('SELECT path,receipt_path FROM accepted_batches').fetchone()) == accepted
        candidate = resumed.db.execute('SELECT * FROM cnyes_history_accepted_batch_candidates WHERE request_url=?',
                                       (url,)).fetchone()
        assert candidate and candidate['response_sha256'] == hashlib.sha256(r.body).hexdigest()
        assert (candidate['parquet_path'], candidate['receipt_path']) == accepted


def test_startup_orphan_batch_revisits_old_failed_work_in_same_recovery_commit(queue):
    url = cnyes_url(BASE, start=1388534400, end=1388534500)
    capture(queue, url, source_payload([1], epoch=1388534400))
    scope = cnyes_scope(url)
    legacy = dict(queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=1', (scope,)).fetchone())
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=9e9 WHERE url=?', (url,))
        for table in ('cnyes_history_page_heads', 'cnyes_history_page_observations', 'cnyes_history_scope_heads'):
            queue.db.execute('DELETE FROM ' + table + ' WHERE scope_url=?', (scope,))
        queue.db.execute('DELETE FROM cnyes_history_accepted_batch_candidates')
        queue.db.execute('DELETE FROM accepted_batches')
        job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
        old_outcome = json.dumps({'error': 'previous_missing_local_original'})
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,'
            'source_checked_at_utc,state,outcome_json) VALUES (?,?,?,?,?,?,?,?,?)',
            (url, scope, 1, job['response_sha256'], legacy['total'], legacy['ids_sha256'],
             legacy['observed_at_utc'], 'retained_unavailable', old_outcome))
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        assert resumed.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                                  (url,)).fetchone()[0] == 'pending'
        event = resumed.db.execute('SELECT original_state,original_outcome_json,resolution_state,evidence_kind '
            'FROM cnyes_history_retained_outcome_resolutions WHERE request_url=?', (url,)).fetchone()
        assert tuple(event) == ('retained_unavailable', old_outcome, 'pending', 'new_accepted_body_batch_candidate')
        for _ in range(5):
            backfill(resumed, limit=8, max_seconds=10)
            if resumed.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=1',
                                  (scope,)).fetchone():
                break
        assert resumed.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=1',
                                  (scope,)).fetchone()
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == job


def test_failed_candidate_wake_rolls_back_batch_commit_then_recovers_receipt(queue, monkeypatch):
    import downloader.news_fulltext_cnyes_history as history
    urls = seed_unindexed_nonzero_pages(queue)
    page = queue.db.execute('SELECT * FROM bulk_page_evidence WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone()
    job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    old_outcome = json.dumps({'error': 'previous_missing_local_original'})
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,'
            'source_checked_at_utc,state,outcome_json) VALUES (?,?,?,?,?,?,?,?,?)',
            (urls[2], cnyes_scope(urls[0]), 3, job['response_sha256'], page['total'], page['ids_sha256'],
             page['observed_at_utc'], 'retained_unavailable', old_outcome))
    def interrupted(*args, **kwargs):
        raise RuntimeError('simulated candidate wake interruption')
    with monkeypatch.context() as patch:
        patch.setattr(history, '_save_retained_resolution', interrupted)
        with pytest.raises(RuntimeError, match='candidate wake interruption'):
            capture(queue, urls[2], source_payload([61], epoch=1388534400, page=3, total=61))
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_accepted_batch_candidates').fetchone()[0] == 0
    assert queue.db.execute('SELECT state,outcome_json FROM cnyes_history_retained_work WHERE request_url=?',
                            (urls[2],)).fetchone()[:] == ('retained_unavailable', old_outcome)
    queue.recover_acceptance()
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                            (urls[2],)).fetchone()[0] == 'pending'
    assert queue.db.execute('SELECT original_outcome_json FROM cnyes_history_retained_outcome_resolutions '
                            'WHERE request_url=?', (urls[2],)).fetchone()[0] == old_outcome
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == job


@pytest.mark.parametrize('invalid', ['receipt', 'authorization', 'capture_clock', 'future_capture_clock'])
def test_accepted_batch_candidate_never_creates_proof_when_validation_fails(queue, invalid):
    import pyarrow.parquet as pq
    from downloader.news_fulltext_cnyes_history import seed_accepted_body_batch_candidates, seed_unobserved_nonzero_pages
    urls = seed_unindexed_nonzero_pages(queue)
    relative, receipt_relative = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61),
        authorization='different-authority' if invalid == 'authorization' else None)
    if invalid in {'capture_clock', 'future_capture_clock'}:
        path = queue.root / relative
        record = pq.read_table(path).to_pylist()[0]
        record['captured_at_utc'] = ('2020-01-01T00:00:00' if invalid == 'capture_clock'
                                     else '2099-01-01T00:00:00+00:00')
        atomic_write_parquet(path, table_from_records([record]), durable=True)
        receipt = json.loads((queue.root / receipt_relative).read_text())
        receipt['sha256'] = sha256_file(path)
        atomic_write_json(queue.root / receipt_relative, receipt, durable=True)
    if invalid == 'receipt':
        receipt = json.loads((queue.root / receipt_relative).read_text())
        receipt['sha256'] = '0' * 64
        atomic_write_json(queue.root / receipt_relative, receipt, durable=True)
    assert seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['indexed'] == 1
    assert seed_unobserved_nonzero_pages(queue, limit=8, max_seconds=10)['queued'] == 3
    backfill(queue, limit=8, max_seconds=10)
    outcome = queue.db.execute('SELECT state,outcome_json FROM cnyes_history_retained_work WHERE request_url=?',
                               (urls[2],)).fetchone()
    assert outcome['state'] == 'retained_invalid'
    assert json.loads(outcome['outcome_json'])['error'] == {
        'receipt': 'invalid_accepted_batch_receipt', 'authorization': 'retained_original_authorization_mismatch',
        'capture_clock': 'retained_original_capture_clock_invalid',
        'future_capture_clock': 'retained_original_capture_clock_invalid'}[invalid]
    assert queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone() is None
    assert head(queue, urls[0])['state'] == 'waiting_page_evidence'


@pytest.mark.parametrize('replace_before_read', [False, True])
def test_accepted_batch_backfill_retries_changed_receipt_and_preserves_remote_deadline(queue, monkeypatch, replace_before_read):
    from pathlib import Path
    from downloader import news_fulltext_cnyes_history as history

    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    relative, receipt_relative = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    assert history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['indexed'] == 1
    assert history.seed_unobserved_nonzero_pages(queue, limit=8, max_seconds=10)['queued'] == 3
    receipt_path = queue.root / receipt_relative
    saved_receipt = json.loads(receipt_path.read_bytes())
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    original_read = Path.read_bytes
    changed = False
    def read_then_replace(candidate):
        nonlocal changed
        if candidate == receipt_path and not changed and replace_before_read:
            changed = True
            atomic_write_json(receipt_path, {'publish': True})
        result = original_read(candidate)
        if candidate == receipt_path and not changed:
            changed = True
            atomic_write_json(receipt_path, {'publish': True})
        return result
    with monkeypatch.context() as patch:
        patch.setattr(Path, 'read_bytes', read_then_replace)
        history.backfill(queue, limit=8, max_seconds=10)
    assert changed
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (urls[2],)).fetchone()[0] == 'local_retry'
    assert queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[2]),)).fetchone() is None
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before
    atomic_write_json(receipt_path, saved_receipt)
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=0 WHERE request_url=?', (urls[2],))
    assert history.backfill(queue, limit=8, max_seconds=10)['processed'] >= 1
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (urls[2],)).fetchone()[0] == 'restored'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before


@pytest.mark.parametrize('invalid', ['request', 'raw_bytes', 'restricted', 'page_identity', 'provider'])
def test_accepted_batch_validates_exact_source_identity_and_public_page(queue, invalid):
    import pyarrow.parquet as pq
    from downloader.news_fulltext_cnyes_history import seed_accepted_body_batch_candidates, seed_unobserved_nonzero_pages
    urls = seed_unindexed_nonzero_pages(queue)
    data = source_payload([62] if invalid == 'page_identity' else [61], epoch=1388534400, page=3, total=61)
    if invalid == 'restricted':
        data['items']['data'][0]['payment'] = 1
    relative, receipt_relative = seed_accepted_batch_for_history_page(queue, urls[2], data)
    path = queue.root / relative
    row = pq.read_table(path).to_pylist()[0]
    if invalid == 'request':
        row['source_request_url'] = urls[1]
    elif invalid == 'raw_bytes':
        row['raw_response_body'] += b'changed'
    elif invalid == 'provider':
        row['provider'] = 'different-provider'
    atomic_write_parquet(path, table_from_records([row]), durable=True)
    receipt = json.loads((queue.root / receipt_relative).read_text())
    receipt['sha256'] = sha256_file(path)
    atomic_write_json(queue.root / receipt_relative, receipt, durable=True)
    with queue.db:
        queue.db.execute('UPDATE discovery SET response_sha256=? WHERE url=?', (row['response_sha256'], urls[2]))
    seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)
    if invalid == 'request':  # A stale or poisoned candidate is still never proof.
        with queue.db:
            queue.db.execute('INSERT OR IGNORE INTO cnyes_history_accepted_batch_candidates VALUES (?,?,?,?)',
                             (urls[2], row['response_sha256'], relative, receipt_relative))
    assert seed_unobserved_nonzero_pages(queue, limit=8, max_seconds=10)['queued'] == 3
    backfill(queue, limit=8, max_seconds=10)
    outcome = queue.db.execute('SELECT state,outcome_json FROM cnyes_history_retained_work WHERE request_url=?',
                               (urls[2],)).fetchone()
    assert outcome['state'] == 'retained_invalid'
    assert json.loads(outcome['outcome_json'])['error'] == {
        'request': 'accepted_batch_matching_original_missing', 'raw_bytes': 'retained_original_bytes_or_private_scope_invalid',
        'restricted': 'restricted_retained_envelope_not_reused', 'page_identity': 'retained_source_page_identity_mismatch',
        'provider': 'accepted_batch_matching_original_missing'}[invalid]
    assert queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone() is None


def test_accepted_batch_scan_local_failure_retries_without_publisher_changes(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history, news_fulltext_queue as queue_module
    urls = seed_unindexed_nonzero_pages(queue)
    relative, _ = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    original = queue_module.replay_source_path
    def unavailable(root, path):
        if path == relative:
            raise OSError(5, 'temporary local I/O')
        return original(root, path)
    with monkeypatch.context() as patch:
        patch.setattr(queue_module, 'replay_source_path', unavailable)
        scan = history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)
        assert scan['blocked'] == 'local_retry_wait' and scan['checked'] == 1
        assert history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['blocked'] == 'local_retry_wait'
    assert [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before
    marker = history.accepted_batch_scan_key(queue.registry.sources['cnyes'])
    state = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0])
    assert state['path'] == relative and state['attempts'] == 0
    retry = queue.db.execute('SELECT * FROM cnyes_history_accepted_batch_local_retries').fetchone()
    assert retry['parquet_path'] == relative and retry['local_attempts'] == 1
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_accepted_batch_local_retries SET not_before_epoch=?', (time.time() + 60,))
    waiting = backfill(queue, limit=8, max_seconds=10)
    assert waiting['exhausted'] and waiting['accepted_body_batch_scan']['blocked'] == 'local_retry_wait'
    assert waiting['unobserved_nonzero_page_scan']['complete']
    assert not history.accepted_batch_inventory_complete(queue, queue.registry.sources['cnyes'])
    assert history.maintenance_delay(waiting) >= 1
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_accepted_batch_local_retries SET not_before_epoch=0')
    assert history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['indexed'] == 1
    assert [r[0] for r in queue.db.execute('SELECT state FROM cnyes_history_accepted_batch_outcomes ORDER BY observed_at_utc')] == ['local_retry', 'recovered']
    assert [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before


def test_accepted_batch_scan_continues_past_local_io_and_recovers_after_restart(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history, news_fulltext_queue as queue_module
    urls = seed_unindexed_nonzero_pages(queue)
    first, _ = seed_accepted_batch_for_history_page(queue, urls[1],
        source_payload(range(31, 61), epoch=1388534400, page=2, total=61), batch_tag='a')
    second, _ = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61), batch_tag='b')
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    original = queue_module.replay_source_path
    def unavailable(root, path):
        if path == first:
            raise OSError(5, 'temporary local I/O')
        return original(root, path)
    with monkeypatch.context() as patch:
        patch.setattr(queue_module, 'replay_source_path', unavailable)
        scan = history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)
        assert scan['indexed'] == 1 and not scan['complete']
        assert queue.db.execute('SELECT parquet_path FROM cnyes_history_accepted_batch_candidates').fetchone()[0] == second
        assert not history.accepted_batch_inventory_complete(queue, queue.registry.sources['cnyes'])
        snapshot = progress(queue.db, queue.registry.sources['cnyes'])['accepted_body_batch_scan']
        assert snapshot['scan_state'] == 'local_retry_wait' and snapshot['local_retry_batches'] == 1
        assert snapshot['next_local_retry_epoch'] > time.time()
        assert first not in json.dumps(snapshot)
        with queue.db:
            queue.db.execute('UPDATE cnyes_history_accepted_batch_local_retries SET not_before_epoch=0')
        again = history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)
        retry = queue.db.execute('SELECT local_attempts,not_before_epoch FROM cnyes_history_accepted_batch_local_retries').fetchone()
        assert not again['complete'] and retry['local_attempts'] == 2 and retry['not_before_epoch'] > time.time()
        assert history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['blocked'] == 'local_retry_wait'
        for _ in range(8):
            history.backfill(queue, limit=16, max_seconds=10)
            if queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                                (cnyes_scope(urls[0]),)).fetchone():
                break
        assert queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                                (cnyes_scope(urls[0]),)).fetchone()
        assert not history.accepted_batch_inventory_complete(queue, queue.registry.sources['cnyes'])
        assert [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        assert [dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before
        assert not history.accepted_batch_inventory_complete(resumed, resumed.registry.sources['cnyes'])
        with resumed.db:
            resumed.db.execute('UPDATE cnyes_history_accepted_batch_local_retries SET not_before_epoch=0')
        scan = history.seed_accepted_body_batch_candidates(resumed, limit=8, max_seconds=10)
        assert scan['indexed'] == 1 and scan['complete']
        assert history.accepted_batch_inventory_complete(resumed, resumed.registry.sources['cnyes'])
        assert {r[0] for r in resumed.db.execute('SELECT parquet_path FROM cnyes_history_accepted_batch_candidates')} == {first, second}
        assert [r[0] for r in resumed.db.execute('SELECT state FROM cnyes_history_accepted_batch_outcomes ORDER BY observed_at_utc')] == ['local_retry', 'local_retry', 'recovered']
        assert [dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before


def test_accepted_batch_memory_pressure_stops_slice_and_retries_without_losing_cursor(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history
    urls = seed_unindexed_nonzero_pages(queue)
    first, _ = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61), batch_tag='a')
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    original = history._accepted_batch_metadata_pairs
    with monkeypatch.context() as patch:
        def unavailable(*args, **kwargs):
            raise MemoryError('temporary local resource pressure')
        patch.setattr(history, '_accepted_batch_metadata_pairs', unavailable)
        scan = history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)
        assert scan['blocked'] == 'local_retry_wait' and scan['checked'] == 0
        assert queue.db.execute('SELECT count(*) FROM cnyes_history_accepted_batch_local_retries').fetchone()[0] == 0
    marker = history.accepted_batch_scan_key(queue.registry.sources['cnyes'])
    with queue.db:
        state = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0])
        assert state['path'] == '' and state['attempts'] == 1
        state['retry_after'] = 0
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps(state), marker))
    assert history._accepted_batch_metadata_pairs is original
    scan = history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)
    assert scan['complete'] and scan['indexed'] == 1
    assert queue.db.execute('SELECT parquet_path FROM cnyes_history_accepted_batch_candidates').fetchone()[0] == first
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before


def test_accepted_batch_late_candidate_is_recovered_after_partial_inventory(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history, news_fulltext_queue as queue_module
    urls = seed_unindexed_nonzero_pages(queue)
    first, _ = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61), batch_tag='a')
    seed_accepted_batch_for_history_page(queue, urls[1],
        source_payload(range(31, 61), epoch=1388534400, page=2, total=61), batch_tag='b')
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    original = queue_module.replay_source_path
    def unavailable(root, path):
        if path == first:
            raise OSError(5, 'temporary local I/O')
        return original(root, path)
    with monkeypatch.context() as patch:
        patch.setattr(queue_module, 'replay_source_path', unavailable)
        for _ in range(8):
            history.backfill(queue, limit=16, max_seconds=10)
            if queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=2',
                                (cnyes_scope(urls[0]),)).fetchone():
                break
        assert queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=2',
                                (cnyes_scope(urls[0]),)).fetchone()
        assert not queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                                    (cnyes_scope(urls[0]),)).fetchone()
        assert not history.accepted_batch_inventory_complete(queue, queue.registry.sources['cnyes'])
        assert [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_accepted_batch_local_retries SET not_before_epoch=0')
    for _ in range(12):
        history.backfill(queue, limit=16, max_seconds=10)
        if queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone():
            break
    assert head(queue, urls[0])['state'] == 'list_complete'
    assert history.accepted_batch_inventory_complete(queue, queue.registry.sources['cnyes'])
    assert [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before


def test_missing_accepted_batch_is_retried_without_blocking_other_local_work(queue):
    from downloader import news_fulltext_cnyes_history as history
    urls = seed_unindexed_nonzero_pages(queue)
    missing, _ = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61), batch_tag='a')
    seed_accepted_batch_for_history_page(queue, urls[1],
        source_payload(range(31, 61), epoch=1388534400, page=2, total=61), batch_tag='b')
    path = queue.root / missing
    held = path.with_suffix('.held')
    path.rename(held)
    result = history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)
    assert result['complete'] and result['indexed'] == 1
    retry = queue.db.execute('SELECT * FROM cnyes_history_accepted_missing_retries').fetchone()
    assert retry['parquet_path'] == missing and retry['local_attempts'] == 1
    snapshot = progress(queue.db, queue.registry.sources['cnyes'])['accepted_body_batch_scan']
    assert snapshot['local_missing_retry_batches'] == 1 and snapshot['scan_state'] == 'local_retry_wait'
    for _ in range(8):
        history.backfill(queue, limit=16, max_seconds=10)
        if queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=2',
                            (cnyes_scope(urls[0]),)).fetchone():
            break
    assert queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=2',
                            (cnyes_scope(urls[0]),)).fetchone()
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                                (cnyes_scope(urls[0]),)).fetchone()
    queue.close()
    held.rename(path)
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        with resumed.db:
            resumed.db.execute('UPDATE cnyes_history_accepted_missing_retries SET not_before_epoch=0')
        for _ in range(12):
            history.backfill(resumed, limit=16, max_seconds=10)
            if resumed.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                                  (cnyes_scope(urls[0]),)).fetchone():
                break
        assert head(resumed, urls[0])['state'] == 'list_complete'
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_accepted_missing_retries').fetchone()[0] == 0
        assert [r[0] for r in resumed.db.execute('SELECT state FROM cnyes_history_accepted_batch_outcomes ORDER BY observed_at_utc')] == ['unavailable', 'recovered']


def test_missing_accepted_batch_waits_for_receipt_and_preserves_source_failure(queue):
    from downloader import news_fulltext_cnyes_history as history
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    relative, receipt_relative = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    path, receipt = queue.root / relative, queue.root / receipt_relative
    held_path, held_receipt = path.with_suffix('.held'), receipt.with_suffix('.held')
    path.rename(held_path)
    receipt.rename(held_receipt)
    history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)
    held_path.rename(path)
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_accepted_missing_retries SET not_before_epoch=0')
    retry = history.retry_missing_accepted_batches(queue, limit=8, max_seconds=10)
    assert retry['processed'] == 1 and retry['indexed'] == 0 and retry['next_retry_epoch'] > time.time()
    assert queue.db.execute('SELECT local_attempts FROM cnyes_history_accepted_missing_retries').fetchone()[0] == 2
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates').fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before
    held_receipt.rename(receipt)
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_accepted_missing_retries SET not_before_epoch=0')
    for _ in range(12):
        history.backfill(queue, limit=16, max_seconds=10)
        if queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone():
            break
    assert queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before


def test_initial_batch_inventory_retries_missing_receipt_without_fabricating_evidence(queue):
    from downloader import news_fulltext_cnyes_history as history
    urls = seed_unindexed_nonzero_pages(queue)
    relative, receipt_relative = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    receipt = queue.root / receipt_relative
    held = receipt.with_suffix('.held')
    receipt.rename(held)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    scan = history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)
    assert scan['complete'] and scan['indexed'] == 0
    retry = queue.db.execute('SELECT parquet_path,receipt_path FROM cnyes_history_accepted_missing_retries').fetchone()
    assert tuple(retry) == (relative, receipt_relative)
    assert queue.db.execute('SELECT reason FROM cnyes_history_accepted_batch_outcomes').fetchone()[0] == 'local_batch_receipt_missing'
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_accepted_batch_candidates').fetchone()
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_page_heads').fetchone()
    held.rename(receipt)
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_accepted_missing_retries SET not_before_epoch=0')
    for _ in range(12):
        history.backfill(queue, limit=16, max_seconds=10)
        if queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone():
            break
    assert queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                            (cnyes_scope(urls[0]),)).fetchone()
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before


@pytest.mark.parametrize('missing', ['batch', 'receipt'])
def test_indexed_batch_disappearing_before_validation_recovers_locally(queue, missing):
    from downloader import news_fulltext_cnyes_history as history
    urls = seed_unindexed_nonzero_pages(queue, failed_last=True)
    relative, receipt_relative = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    assert history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['indexed'] == 1
    target = queue.root / (relative if missing == 'batch' else receipt_relative)
    held = target.with_suffix('.held')
    target.rename(held)
    for _ in range(10):
        result = history.backfill(queue, limit=16, max_seconds=10)
        state = queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?',
                                 (urls[2],)).fetchone()
        if state and state['state'] in ('retained_invalid', 'retained_unavailable'):
            break
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                                (cnyes_scope(urls[0]),)).fetchone()
    retry = queue.db.execute('SELECT parquet_path,receipt_path,not_before_epoch,local_attempts '
                             'FROM cnyes_history_accepted_missing_retries').fetchone()
    assert tuple(retry[:2]) == (relative, receipt_relative) and retry['local_attempts'] == 1
    assert result['next_local_retry_epoch'] == retry['not_before_epoch']
    assert progress(queue.db, queue.registry.sources['cnyes'])['accepted_body_batch_scan']['scan_state'] == 'local_retry_wait'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before
    queue.close()
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_accepted_missing_retries').fetchone()[0] == 1
        held.rename(target)
        with resumed.db:
            resumed.db.execute('UPDATE cnyes_history_accepted_missing_retries SET not_before_epoch=0')
        for _ in range(12):
            history.backfill(resumed, limit=16, max_seconds=10)
            if resumed.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                                  (cnyes_scope(urls[0]),)).fetchone():
                break
        assert head(resumed, urls[0])['state'] == 'list_complete'
        assert dict(resumed.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before


def test_one_missing_indexed_batch_is_not_backed_off_once_per_page(queue):
    from downloader import news_fulltext_cnyes_history as history
    urls = seed_unindexed_nonzero_pages(queue)
    source = queue.registry.sources['cnyes']
    payloads = [source_payload(range(1, 31), epoch=1388534400, page=1, total=61),
                source_payload(range(31, 61), epoch=1388534400, page=2, total=61),
                source_payload([61], epoch=1388534400, page=3, total=61)]
    records = []
    for url, data in zip(urls, payloads):
        r = response(data, url)
        sha = hashlib.sha256(r.body).hexdigest()
        for path, _ in history._retained_envelopes(queue.root, sha):
            path.unlink(missing_ok=True)
            path.with_suffix('.parquet.receipt.json').unlink(missing_ok=True)
        records.append({'provider': 'cnyes', 'source_request_url': url, 'raw_response_body': r.body,
            'response_sha256': sha, 'captured_at_utc': '2020-01-01T00:00:00+00:00',
            'authorization_fingerprint': source.authorization_fingerprint, 'publish': False})
    relative, receipt_relative = 'bodies/mixed.parquet', 'receipts/mixed.json'
    atomic_write_parquet(queue.root / relative, table_from_records(records), durable=True)
    atomic_write_json(queue.root / receipt_relative, {'contract_version': 1, 'parquet_path': relative,
        'sha256': sha256_file(queue.root / relative), 'rows': len(records), 'publish': False}, durable=True)
    with queue.db:
        queue.db.execute('INSERT INTO accepted_batches VALUES (?,?)', (relative, receipt_relative))
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=1")
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    assert history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['indexed'] == 3
    target = queue.root / relative
    held = target.with_suffix('.held')
    target.rename(held)
    for _ in range(8):
        history.backfill(queue, limit=16, max_seconds=10)
        if queue.db.execute("SELECT count(*) FROM cnyes_history_retained_work WHERE state='retained_invalid'").fetchone()[0] == 3:
            break
    assert queue.db.execute("SELECT count(*) FROM cnyes_history_retained_work WHERE state='retained_invalid'").fetchone()[0] == 3
    assert queue.db.execute('SELECT local_attempts FROM cnyes_history_accepted_missing_retries').fetchall()[0][0] == 1
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_accepted_missing_retries').fetchone()[0] == 1
    held.rename(target)
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_accepted_missing_retries SET not_before_epoch=0')
    for _ in range(12):
        history.backfill(queue, limit=16, max_seconds=10)
        if queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 3:
            break
    assert head(queue, urls[0])['state'] == 'list_complete'
    assert [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before


def test_missing_batch_restored_before_nonzero_inventory_does_not_skip_other_pages(queue):
    from downloader import news_fulltext_cnyes_history as history
    urls = seed_unindexed_nonzero_pages(queue)
    relative, _ = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61), batch_tag='a')
    seed_accepted_batch_for_history_page(queue, urls[1],
        source_payload(range(31, 61), epoch=1388534400, page=2, total=61), batch_tag='b')
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    path = queue.root / relative
    held = path.with_suffix('.held')
    path.rename(held)
    assert history.seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)['complete']
    assert not queue.db.execute('SELECT 1 FROM cnyes_history_retained_work').fetchone()
    held.rename(path)
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_accepted_missing_retries SET not_before_epoch=0')
    assert history.retry_missing_accepted_batches(queue, limit=8, max_seconds=10)['indexed'] == 1
    scan = history.seed_unobserved_nonzero_pages(queue, limit=16, max_seconds=10)
    assert scan['queued'] == 3
    for _ in range(12):
        history.backfill(queue, limit=16, max_seconds=10)
        if head_row := queue.db.execute('SELECT state FROM cnyes_history_scope_heads WHERE scope_url=?',
                                        (cnyes_scope(urls[0]),)).fetchone():
            if head_row['state'] == 'list_complete':
                break
    assert head(queue, urls[0])['state'] == 'list_complete'
    assert {r[0] for r in queue.db.execute('SELECT page FROM cnyes_history_page_heads WHERE scope_url=?',
                                           (cnyes_scope(urls[0]),))} == {1, 2, 3}
    assert [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before


def test_accepted_batch_scan_records_missing_file_without_forging_candidate(queue):
    from downloader.news_fulltext_cnyes_history import seed_accepted_body_batch_candidates
    with queue.db:
        queue.db.execute('INSERT INTO accepted_batches VALUES (?,?)', ('bodies/missing.parquet', 'receipts/missing.json'))
    scan = seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)
    assert scan['complete'] and scan['indexed'] == 0
    outcome = queue.db.execute('SELECT state,reason FROM cnyes_history_accepted_batch_outcomes').fetchone()
    assert tuple(outcome) == ('unavailable', 'local_batch_file_missing')


def test_accepted_batch_index_falls_back_for_mixed_request_metadata(queue):
    import pyarrow.parquet as pq
    from downloader.news_fulltext_cnyes_history import seed_accepted_body_batch_candidates
    urls = seed_unindexed_nonzero_pages(queue)
    relative, receipt_relative = seed_accepted_batch_for_history_page(queue, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    path = queue.root / relative
    original = pq.read_table(path).to_pylist()[0]
    other = {**original, 'source_request_url': urls[0], 'response_sha256': 'f' * 64, 'raw_response_body': None}
    atomic_write_parquet(path, table_from_records([original, other]), durable=True)
    receipt = json.loads((queue.root / receipt_relative).read_text())
    receipt.update(sha256=sha256_file(path), rows=2)
    atomic_write_json(queue.root / receipt_relative, receipt, durable=True)
    result = seed_accepted_body_batch_candidates(queue, limit=8, max_seconds=10)
    assert result['complete'] and result['indexed'] == 1
    assert queue.db.execute('SELECT request_url FROM cnyes_history_accepted_batch_candidates').fetchone()[0] == urls[2]


def test_nonzero_inventory_preserves_request_mismatch_and_rechecks_only_after_local_work(queue):
    import pyarrow.parquet as pq
    from downloader.news_fulltext_cnyes_history import seed_unobserved_nonzero_pages
    urls = seed_unindexed_nonzero_pages(queue)
    sha = queue.db.execute('SELECT response_sha256 FROM discovery WHERE url=?', (urls[1],)).fetchone()[0]
    path = queue.root / 'discovery_responses' / (sha + '.parquet')
    original = pq.read_table(path).to_pylist()[0]
    original['request_url'] = cnyes_url(BASE, 2, start=1388534300, end=1388534400)
    atomic_write_parquet(path, table_from_records([original]), durable=True)
    atomic_write_json(path.with_suffix('.parquet.receipt.json'), {
        'sha256': sha256_file(path), 'response_sha256': sha, 'publish': False}, durable=True)
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    assert seed_unobserved_nonzero_pages(queue, limit=8, max_seconds=10)['complete']
    backfill(queue, limit=1, max_seconds=10)
    assert [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before
    backfill(queue, limit=8, max_seconds=10)
    outcome = queue.db.execute('SELECT state,outcome_json FROM cnyes_history_retained_work WHERE request_url=?',
                               (urls[1],)).fetchone()
    assert outcome['state'] == 'retained_request_mismatch'
    assert json.loads(outcome['outcome_json'])['error'] == 'retained_original_request_differs_not_relabelled'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 1
    assert head(queue, urls[0])['state'] == 'waiting_page_evidence'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[0],)).fetchone()) == before[0]
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0


def test_nonzero_inventory_invalid_cursor_is_preserved_and_restarted_without_duplicate_work(queue):
    from downloader.news_fulltext_cnyes_history import LEGACY_NONZERO_SCAN_PREFIX, seed_unobserved_nonzero_pages
    urls = seed_unindexed_nonzero_pages(queue)
    assert seed_unobserved_nonzero_pages(queue, limit=1, max_seconds=10)['queued'] == 1
    key = queue.db.execute('SELECT key FROM queue_settings WHERE key LIKE ?',
                           (LEGACY_NONZERO_SCAN_PREFIX + '%',)).fetchone()[0]
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', ('invalid-cursor-bytes', key))
    assert seed_unobserved_nonzero_pages(queue, limit=1, max_seconds=10)['restarted_invalid_checkpoint']
    for _ in range(10):
        scan = seed_unobserved_nonzero_pages(queue, limit=1, max_seconds=10)
        if scan['complete']:
            break
    assert scan['complete']
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key LIKE ?',
        (LEGACY_NONZERO_SCAN_PREFIX + 'invalid:%',)).fetchone()[0] == 'invalid-cursor-bytes'
    assert [row[0] for row in queue.db.execute('SELECT request_url FROM cnyes_history_retained_work ORDER BY request_url')] == sorted(urls[:2])
    assert queue.db.execute('SELECT scan_state FROM cnyes_history_local_inventory_scopes').fetchone()[0] == 'ready'


def test_nonzero_inventory_local_failure_does_not_wake_missing_publisher_pages(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history
    urls = seed_unindexed_nonzero_pages(queue)
    assert history.seed_unobserved_nonzero_pages(queue, limit=8, max_seconds=10)['complete']
    sha = queue.db.execute('SELECT response_sha256 FROM discovery WHERE url=?', (urls[0],)).fetchone()[0]
    path = queue.root / 'quarantine' / (sha + '.parquet')
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    original = history.pq.read_table
    def unavailable(candidate, *args, **kwargs):
        if candidate == path:
            raise OSError(5, 'temporary local read failure')
        return original(candidate, *args, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(history.pq, 'read_table', unavailable)
        assert backfill(queue, limit=8, max_seconds=10)['local_io_deferrals'] == 1
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (urls[0],)).fetchone()[0] == 'local_retry'
    assert [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=0 WHERE request_url=?', (urls[0],))
    backfill(queue, limit=8, max_seconds=10)
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (urls[0],)).fetchone()[0] == 'restored'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 2


def test_nonzero_inventory_changed_contract_releases_old_fence_without_erasing_identity(queue):
    from dataclasses import replace
    from downloader.news_fulltext_cnyes_history import seed_unobserved_nonzero_pages
    urls = seed_unindexed_nonzero_pages(queue)
    assert seed_unobserved_nonzero_pages(queue, limit=1, max_seconds=10)['queued'] == 1
    old = dict(queue.db.execute('SELECT * FROM cnyes_history_local_inventory_scopes').fetchone())
    queue.registry.sources['cnyes'] = replace(queue.registry.sources['cnyes'], authorization_fingerprint='new-authority')
    result = seed_unobserved_nonzero_pages(queue, limit=8, max_seconds=10)
    current = dict(queue.db.execute('SELECT * FROM cnyes_history_local_inventory_scopes').fetchone())
    assert result['stale_fences_reconciled'] == 1
    assert current == {**old, 'scan_state': 'ready'}
    assert head(queue, urls[0])['state'] == 'waiting_page_evidence'
    backfill(queue, limit=8, max_seconds=10)
    outcome = queue.db.execute('SELECT state,outcome_json FROM cnyes_history_retained_work WHERE request_url=?',
                               (urls[0],)).fetchone()
    assert outcome['state'] == 'retained_invalid'
    assert json.loads(outcome['outcome_json'])['error'] == 'retained_original_authorization_mismatch'


def test_owner_continues_bounded_local_history_without_any_http_work(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext as main, news_fulltext_cnyes_history as history
    from test_news_fulltext import http_fixture
    cfg['adaptive_rate'] = {'enabled': False}
    cfg['cached_cnyes_metadata_replay_enabled'] = False
    http, session = http_fixture(cfg, tmp_path, {})
    q = NewsQueue(tmp_path, http.registry)
    calls = []
    def local_slice(*args, **kwargs):
        calls.append(len(calls))
        return {'processed': 1, 'exhausted': len(calls) >= 3, 'next_local_retry_epoch': None,
                'unobserved_nonzero_page_scan': {'complete': len(calls) >= 3, 'checked': 1}}
    monkeypatch.setattr(history, 'backfill', local_slice)
    monkeypatch.setattr(main, 'seed_discovery', lambda q: None)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda q, config: False)
    try:
        result = main.run_cycle(q, http, cfg, max_articles=0, max_discovery_requests=0,
                                continuous_budgets=True, max_run_seconds=2)
        assert len(calls) == 3 and not session.calls
        assert result['cycle']['http_requests'].get('cnyes', 0) == 0
    finally:
        q.close()
        http.close()


def test_slow_cnyes_history_slice_stops_before_checkpoint_and_http(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext as main, news_fulltext_cnyes_history as history
    from downloader import news_fulltext_cnyes_checkpoint as checkpoint
    from test_news_fulltext import http_fixture
    from test_news_provider_cooperation import AdvancingCycleClock

    cfg['adaptive_rate'] = {'enabled': False}
    cfg['cached_cnyes_metadata_replay_enabled'] = False
    http, session = http_fixture(cfg, tmp_path, {})
    q = NewsQueue(tmp_path, http.registry)
    clock, stages = AdvancingCycleClock(), []

    def slow_history(*args, **kwargs):
        stages.append('history')
        clock.advance += 10
        return {'processed': 1, 'exhausted': False, 'next_local_retry_epoch': None,
                'unobserved_nonzero_page_scan': {'complete': False, 'checked': 1}}

    monkeypatch.setattr(main, 'time', clock)
    monkeypatch.setattr(main, 'seed_discovery', lambda q: None)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda q, config: False)
    monkeypatch.setattr(history, 'backfill', slow_history)
    monkeypatch.setattr(checkpoint, 'recover', lambda *args, **kwargs: stages.append('checkpoint'))
    try:
        result = main.run_cycle(q, http, cfg, max_articles=0, max_discovery_requests=0,
                                continuous_budgets=True, max_run_seconds=2, defer_final_status=True)
        assert stages == ['history'] and not session.calls
        assert result['cycle']['http_requests'].get('cnyes', 0) == 0
    finally:
        q.close()
        http.close()


def test_slow_cnyes_evidence_preparation_does_not_dispatch_after_deadline(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext as main, news_fulltext_cnyes_history as history
    from test_news_fulltext import http_fixture
    from test_news_provider_cooperation import AdvancingCycleClock

    cfg['adaptive_rate'] = {'enabled': False}
    http, session = http_fixture(cfg, tmp_path, {})
    q = NewsQueue(tmp_path, http.registry)
    clock, prepared = AdvancingCycleClock(), []
    url = cnyes_url(BASE)

    def slow_evidence(queue, task, source, **kwargs):
        prepared.append(task['url'])
        clock.advance += 130

    monkeypatch.setattr(main, 'time', clock)
    monkeypatch.setattr(main, 'seed_discovery', lambda q: None)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda q, config: False)
    monkeypatch.setattr(history, 'prepare_restricted_history_dispatch', slow_evidence)
    monkeypatch.setattr(q, 'discovery_candidates', lambda *args, **kwargs:
                        list(q.db.execute('SELECT * FROM discovery WHERE url=?', (url,))))
    try:
        with q.db:
            q.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'cnyes_news','cnyes')", (url,))
        result = main.run_cycle(q, http, cfg, max_articles=0, max_discovery_requests=1,
                                max_run_seconds=120, defer_final_status=True)
        assert prepared == [url] and not session.calls
        assert result['cycle']['discovery_attempts_started'] == 0
        assert q.db.execute('SELECT attempts,error_code,last_checked_at_utc FROM discovery WHERE url=?',
                            (url,)).fetchone()[:] == (0, None, None)
    finally:
        q.close()
        http.close()


def test_slow_error_rollup_does_not_start_idle_cnyes_replay_after_deadline(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext as main, news_fulltext_cnyes_history as history
    from downloader import news_fulltext_metadata_replay as replay
    from test_news_fulltext import http_fixture
    from test_news_provider_cooperation import AdvancingCycleClock

    cfg['adaptive_rate'] = {'enabled': False}
    cfg['cached_cnyes_metadata_replay_enabled'] = True
    http, session = http_fixture(cfg, tmp_path, {})
    q = NewsQueue(tmp_path, http.registry)
    clock, stages = AdvancingCycleClock(), []

    def slow_rollup():
        stages.append('rollup')
        clock.advance += 10

    monkeypatch.setattr(main, 'time', clock)
    monkeypatch.setattr(main, 'seed_discovery', lambda q: None)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda q, config: False)
    monkeypatch.setattr(history, 'backfill', lambda *args, **kwargs: {
        'processed': 0, 'exhausted': True, 'next_local_retry_epoch': None})
    monkeypatch.setattr(q, 'advance_discovery_error_rollup', slow_rollup)
    monkeypatch.setattr(replay, 'replay_retained_cnyes_metadata', lambda *args, **kwargs: stages.append('replay'))
    try:
        result = main.run_cycle(q, http, cfg, max_articles=0, max_discovery_requests=0,
                                continuous_budgets=True, max_run_seconds=2, defer_final_status=True)
        assert stages == ['rollup'] and not session.calls
        assert result['cycle']['http_requests'].get('cnyes', 0) == 0
    finally:
        q.close()
        http.close()


def test_owner_continues_scope_repair_slices_without_any_http_work(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext as main, news_fulltext_cnyes_history as history
    from test_news_fulltext import http_fixture
    cfg['adaptive_rate'] = {'enabled': False}
    cfg['cached_cnyes_metadata_replay_enabled'] = False
    http, session = http_fixture(cfg, tmp_path, {})
    q = NewsQueue(tmp_path, http.registry)
    calls = []

    def local_slice(*args, **kwargs):
        calls.append(1)
        return {'processed': 0, 'exhausted': len(calls) >= 3, 'next_local_retry_epoch': None,
                'repair_sweep_pending': len(calls) < 3,
                'evidence_repair': {'scopes_checked': int(len(calls) < 3)}}

    monkeypatch.setattr(history, 'backfill', local_slice)
    monkeypatch.setattr(main, 'seed_discovery', lambda q: None)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda q, config: False)
    try:
        result = main.run_cycle(q, http, cfg, max_articles=0, max_discovery_requests=0,
                                continuous_budgets=True, max_run_seconds=2)
        assert len(calls) == 3 and not session.calls
        assert result['cycle']['http_requests'].get('cnyes', 0) == 0
    finally:
        q.close()
        http.close()


def test_owner_wakes_for_future_local_history_retry_when_other_work_is_idle(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext as main, news_fulltext_cnyes_history as history
    from test_news_fulltext import http_fixture
    cfg.update(adaptive_rate={'enabled': False}, cached_cnyes_replay_enabled=False,
               cached_cnyes_metadata_replay_enabled=False, retained_cnyes_failure_replay_enabled=False)
    http, session = http_fixture(cfg, tmp_path, {})
    q = NewsQueue(tmp_path, http.registry)
    calls = []
    deadline = time.time() + .25

    def local_slice(*args, **kwargs):
        calls.append(time.time())
        if len(calls) == 2:
            http.stop.set()
        return {'processed': 0, 'exhausted': True, 'next_local_retry_epoch': deadline}

    monkeypatch.setattr(history, 'backfill', local_slice)
    monkeypatch.setattr(main, 'seed_discovery', lambda q: None)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda q, config: False)
    try:
        result = main.run_service(q, http, cfg, max_articles=0, max_discovery_requests=0,
                                  duration_seconds=2.5)
        assert result['activity'] == 'stopped'
        assert len(calls) == 2 and calls[1] >= deadline
        assert not session.calls
    finally:
        q.close()
        http.close()


def test_resident_owner_recovers_restored_history_batch_at_its_local_deadline(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext as main, news_fulltext_cnyes_history as history
    from test_news_fulltext import http_fixture
    cfg.update(adaptive_rate={'enabled': False}, cached_cnyes_replay_enabled=False,
               cached_cnyes_metadata_replay_enabled=False, retained_cnyes_failure_replay_enabled=False)
    http, session = http_fixture(cfg, tmp_path, {})
    q = NewsQueue(tmp_path, http.registry)
    urls = seed_unindexed_nonzero_pages(q, failed_last=True)
    relative, _ = seed_accepted_batch_for_history_page(q, urls[2],
        source_payload([61], epoch=1388534400, page=3, total=61))
    target = q.root / relative
    held = target.with_suffix('.held')
    target.rename(held)
    before = dict(q.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone())
    original_backfill = history.backfill
    timing, observed = {}, []

    def restore_when_waiting(*args, **kwargs):
        result = original_backfill(*args, **kwargs)
        observed.append({key: result.get(key) for key in ('exhausted', 'processed', 'next_local_retry_epoch',
            'legacy_mismatch_reconciliation', 'legacy_unavailable_reconciliation', 'unobserved_empty_scope_scan',
            'accepted_body_batch_scan', 'unobserved_nonzero_page_scan', 'overflow_retained_scan', 'evidence_repair')})
        if result['exhausted'] and result['next_local_retry_epoch'] is not None and not timing:
            timing['deadline'] = result['next_local_retry_epoch']
            held.rename(target)
        if q.db.execute('SELECT 1 FROM cnyes_history_page_heads WHERE scope_url=? AND page=3',
                        (cnyes_scope(urls[0]),)).fetchone():
            timing['recovered'] = time.time()
            http.stop.set()
        return result

    monkeypatch.setattr(history, 'backfill', restore_when_waiting)
    monkeypatch.setattr(main, 'seed_discovery', lambda q: None)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda q, config: False)
    try:
        result = main.run_service(q, http, cfg, max_articles=0, max_discovery_requests=0,
                                  duration_seconds=15)
        assert result['activity'] == 'stopped' and timing['recovered'] >= timing['deadline'], (timing, observed)
        assert head(q, urls[0])['state'] == 'list_complete'
        assert dict(q.db.execute('SELECT * FROM discovery WHERE url=?', (urls[2],)).fetchone()) == before
        assert q.db.execute('SELECT count(*) FROM cnyes_history_accepted_missing_retries').fetchone()[0] == 0
        assert not session.calls
    finally:
        q.close()
        http.close()


def test_owner_services_other_provider_while_history_inventory_continues(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext as main, news_fulltext_cnyes_history as history
    from test_news_fulltext import FakeResponse, article_html, config as publisher_config, http_fixture
    config = publisher_config.__wrapped__()
    config['providers'].append(deepcopy(cfg['providers'][0]))
    config['workers'] = 2
    config['cached_cnyes_metadata_replay_enabled'] = False
    events = []
    class VisibleResponse(FakeResponse):
        def iter_content(self, **kwargs):
            time.sleep(.02)
            events.append('publisher_response')
            yield self.body
    url = 'https://publisher.test/story/1'
    http, session = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: VisibleResponse(article_html())})
    q = NewsQueue(tmp_path, http.registry)
    with q.db:
        q.add_articles([{'url': url}])
    calls = []
    def local_slice(*args, **kwargs):
        calls.append(len(calls))
        time.sleep(.01)
        events.append('local_slice')
        return {'processed': 1, 'exhausted': len(calls) >= 20, 'next_local_retry_epoch': None,
                'unobserved_nonzero_page_scan': {'complete': len(calls) >= 20, 'checked': 1}}
    monkeypatch.setattr(history, 'backfill', local_slice)
    monkeypatch.setattr(main, 'seed_discovery', lambda q: None)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda q, config: False)
    try:
        result = main.run_cycle(q, http, config, max_articles=1, max_discovery_requests=0,
                                continuous_budgets=True, max_run_seconds=3)
        assert len(calls) == 20 and result['states']['complete'] == 1
        assert events.index('publisher_response') < max(i for i, value in enumerate(events) if value == 'local_slice')
        assert [call[0] for call in session.calls] == ['https://publisher.test/robots.txt', url]
    finally:
        q.close()
        http.close()


def test_owner_stops_local_history_immediately_when_stop_requested(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext as main, news_fulltext_cnyes_history as history
    from test_news_fulltext import http_fixture
    cfg['adaptive_rate'] = {'enabled': False}
    cfg['cached_cnyes_metadata_replay_enabled'] = False
    http, session = http_fixture(cfg, tmp_path, {})
    q = NewsQueue(tmp_path, http.registry)
    calls = []
    def local_slice(*args, **kwargs):
        calls.append(1)
        http.stop.set()
        return {'processed': 1, 'exhausted': False, 'next_local_retry_epoch': None,
                'unobserved_nonzero_page_scan': {'complete': False, 'checked': 1}}
    monkeypatch.setattr(history, 'backfill', local_slice)
    monkeypatch.setattr(main, 'seed_discovery', lambda q: None)
    monkeypatch.setattr(main, 'refresh_disk_admission', lambda q, config: False)
    try:
        result = main.run_cycle(q, http, cfg, max_articles=0, max_discovery_requests=0,
                                continuous_budgets=True, max_run_seconds=2)
        assert calls == [1] and not session.calls and result['activity'] == 'stopped'
    finally:
        q.close()
        http.close()


@pytest.mark.parametrize('change', ['source_bulk', 'authorization', 'metadata_version'])
def test_same_raw_bytes_new_contract_preserve_old_observation_and_current_head(queue, monkeypatch, change):
    from dataclasses import replace
    import downloader.news_fulltext_cnyes_history as history
    low = 1388534400
    url = cnyes_url(BASE, start=low, end=low + 100)
    r = capture(queue, url, source_payload([1], epoch=low))
    original = dict(queue.db.execute('SELECT * FROM cnyes_history_page_observations').fetchone())
    source = queue.registry.sources['cnyes']
    if change == 'source_bulk':
        source = replace(source, cnyes_bulk_data_fingerprint='changed-source-bulk-contract')
    elif change == 'authorization':
        source = replace(source, authorization_fingerprint='changed-authorization')
    else:
        monkeypatch.setattr(history, 'CONTRACT_VERSION', CONTRACT_VERSION + 1)
    info, posts = cnyes_pagination(r)
    with queue.db:
        record_page(queue, r, info, posts, source, {'kind': 'actual_response_reference'})
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_observations').fetchone()[0] == 2
    assert dict(queue.db.execute('SELECT * FROM cnyes_history_page_observations WHERE observation_sha256=?',
                                (original['observation_sha256'],)).fetchone()) == original
    current = head(queue, url)
    assert current['state'] == 'list_complete' and current['source_bulk_fingerprint'] == source.cnyes_bulk_data_fingerprint
    assert current['authorization_fingerprint'] == source.authorization_fingerprint
    assert current['evidence_contract_version'] == history.CONTRACT_VERSION


def legacy_missing_page(queue, *, failure=None):
    low = 1388534400
    first = cnyes_url(BASE, start=low, end=low + 100)
    second = cnyes_url(BASE, 2, start=low, end=low + 100)
    capture(queue, first, source_payload(range(1, 31), epoch=low, total=31))
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=9e9,etag=?,last_modified=?,error_code=?,attempts=?,'
            'response_sha256=?,last_checked_at_utc=? WHERE url=?',
            ('old-etag', 'old-last-modified', failure, int(failure is not None),
             'old-response', '2020-01-01T00:00:00+00:00', second))
    return first, second


def test_repair_scan_reports_a_fetched_scope_when_slice_expires_before_processing(queue, monkeypatch):
    from types import SimpleNamespace
    from downloader import news_fulltext_cnyes_history as history

    _, second = legacy_missing_page(queue)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone())
    monotonic = iter((0.0, 2.0))
    with monkeypatch.context() as patch:
        patch.setattr(history, 'time', SimpleNamespace(monotonic=lambda: next(monotonic), time=time.time))
        result = history.repair_incomplete_scopes(queue, max_seconds=1)
    assert result['scopes_checked'] == 0 and result['unfinished_slice']
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone()) == before


def test_history_repair_budget_exhaustion_keeps_the_owner_pending(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    _, second = legacy_missing_page(queue)
    # Finish unrelated one-time scans so this test isolates the repair budget.
    for seed in (history.reconcile_legacy_request_mismatches, history.reconcile_legacy_unavailable,
                 history.seed_unobserved_empty_scopes, history.seed_accepted_body_batch_candidates,
                 history.seed_unobserved_nonzero_pages, history.seed_overflow_retained):
        assert seed(queue, limit=128, max_seconds=10)['complete']
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone())
    original = history.seed_overflow_retained

    def slow_local_slice(*args, **kwargs):
        time.sleep(.02)
        return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(history, 'seed_overflow_retained', slow_local_slice)
        result = history.backfill(queue, max_seconds=.005)
    assert result['evidence_repair']['scopes_checked'] == 0
    assert result['repair_sweep_pending'] and not result['exhausted']
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone()) == before
    result = history.backfill(queue, max_seconds=10)
    assert result['evidence_repair']['page_checks_scheduled'] == 1
    assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (second,)).fetchone()[0] < before['due']


def test_history_repair_budget_exhaustion_does_not_spin_when_no_scopes_remain(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    for seed in (history.reconcile_legacy_request_mismatches, history.reconcile_legacy_unavailable,
                 history.seed_unobserved_empty_scopes, history.seed_accepted_body_batch_candidates,
                 history.seed_unobserved_nonzero_pages, history.seed_overflow_retained):
        assert seed(queue, limit=128, max_seconds=10)['complete']
    original = history.seed_overflow_retained

    def slow_local_slice(*args, **kwargs):
        time.sleep(.02)
        return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(history, 'seed_overflow_retained', slow_local_slice)
        result = history.backfill(queue, max_seconds=.005)
    assert result['exhausted'] and not result['repair_sweep_pending']
    assert history.maintenance_delay(result) == 300


def test_restart_repair_only_missing_successful_page_and_no_repeat_or_body_rewrite(queue):
    first, second = legacy_missing_page(queue)
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (first,)).fetchone())
    bodies = [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    proof = head(queue, first)['proof_sha256']
    result = repair_incomplete_scopes(queue, max_seconds=10)
    assert result == {'scopes_checked': 1, 'retained_pages_queued': 0, 'page_checks_scheduled': 1}
    job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone())
    assert job['due'] < 9e9 and job['etag'] is None and job['last_modified'] is None
    assert head(queue, first)['proof_sha256'] == proof
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (first,)).fetchone()) == before
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')] == bodies
    assert repair_incomplete_scopes(queue, max_seconds=10)['page_checks_scheduled'] == 0
    with queue.db:  # Same failed-to-finish evidence after an unchanged owner restart.
        queue.db.execute('UPDATE discovery SET due=9e9 WHERE url=?', (second,))
    assert repair_incomplete_scopes(queue, max_seconds=10)['page_checks_scheduled'] == 0
    capture(queue, second, source_payload([31], epoch=bounds(first)[0], page=2, total=31))
    assert head(queue, first)['state'] == 'list_complete'
    assert repair_incomplete_scopes(queue, max_seconds=10)['scopes_checked'] == 0
    assert queue.db.execute('SELECT 1 FROM cnyes_history_scope_proofs WHERE proof_sha256=?', (proof,)).fetchone()


@pytest.mark.parametrize('state', ['pending', 'overflow_pending', 'local_retry'])
def test_incomplete_repair_waits_for_local_page_work_before_source_recheck(queue, state):
    first, second = legacy_missing_page(queue)
    with queue.db:
        queue.db.execute('INSERT INTO cnyes_history_retained_work '
            '(request_url,scope_url,page,response_sha256,expected_total,expected_ids_sha256,source_checked_at_utc,state) '
            'VALUES (?,?,2,?,31,?,\'old-clock\',?)',
            (second, cnyes_scope(first), 'a' * 64, 'b' * 64, state))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone())
    result = repair_incomplete_scopes(queue, max_seconds=10)
    assert result['scopes_checked'] == 1 and result['page_checks_scheduled'] == 0
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone()) == before


def test_incomplete_repair_preserves_unreadable_candidate_for_local_retry(queue, monkeypatch):
    from pathlib import Path
    first, second = legacy_missing_page(queue)
    sha = 'a' * 64
    target = queue.root / 'quarantine' / (sha + '.parquet')
    with queue.db:
        queue.db.execute('INSERT INTO bulk_page_evidence VALUES (?,2,31,?,\'old-clock\')',
                         (cnyes_scope(first), 'b' * 64))
        queue.db.execute('UPDATE discovery SET response_sha256=? WHERE url=?', (sha, second))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone())
    original = Path.is_file
    def unavailable(candidate):
        if candidate == target:
            raise OSError(5, 'temporary local error')
        return original(candidate)
    with monkeypatch.context() as patch:
        patch.setattr(Path, 'is_file', unavailable)
        result = repair_incomplete_scopes(queue, max_seconds=10)
        assert result['retained_pages_queued'] == 1 and result['page_checks_scheduled'] == 0
        assert backfill(queue, max_seconds=10)['local_io_deferrals'] == 1
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (second,)).fetchone()[0] == 'local_retry'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone()) == before


def test_incomplete_multi_page_scope_reuses_exact_discovery_envelope(queue):
    first, second = legacy_missing_page(queue)
    r = response(source_payload([31], epoch=bounds(first)[0], page=2, total=31), second)
    info, _ = cnyes_pagination(r)
    sha = hashlib.sha256(r.body).hexdigest()
    source = queue.registry.sources['cnyes']
    target = queue.root / 'discovery_responses' / (sha + '.parquet')
    atomic_write_parquet(target, table_from_records([{'request_url': second, 'raw_response_body': r.body,
        'response_sha256': sha, 'captured_at_utc': '2020-01-01T00:00:00+00:00',
        'authorization_fingerprint': source.authorization_fingerprint, 'publish': False}]), durable=True)
    atomic_write_json(target.with_suffix('.parquet.receipt.json'), {
        'sha256': sha256_file(target), 'response_sha256': sha, 'publish': False}, durable=True)
    with queue.db:
        queue.db.execute('INSERT INTO bulk_page_evidence VALUES (?,2,31,?,?)',
                         (cnyes_scope(first), info['ids_sha256'], '2020-01-01T00:00:00+00:00'))
        queue.db.execute('UPDATE discovery SET response_sha256=? WHERE url=?', (sha, second))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone())
    bodies = [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    result = repair_incomplete_scopes(queue, max_seconds=10)
    assert result['retained_pages_queued'] == 1 and result['page_checks_scheduled'] == 0
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone()) == before
    assert backfill(queue, max_seconds=10)['processed'] == 1
    assert head(queue, first)['state'] == 'list_complete'
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone()) == before
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')] == bodies


@pytest.mark.parametrize('gate', ['http_403', 'http_429', 'transport_retry', 'split', 'due', 'inflight_article'])
def test_incomplete_repair_preserves_refusals_split_active_jobs_and_article_leases(queue, gate):
    first, second = legacy_missing_page(queue, failure=gate if gate.startswith(('http_', 'transport_')) else None)
    with queue.db:
        if gate == 'split':
            queue.db.execute('UPDATE discovery SET superseded_by_windows_json=? WHERE url=?', ('[]', second))
        elif gate == 'due':
            queue.db.execute('UPDATE discovery SET due=0 WHERE url=?', (second,))
        elif gate == 'inflight_article':
            queue.db.execute("UPDATE articles SET state='inflight',lease_owner='real-owner',lease_until=9e9 WHERE id=(SELECT id FROM articles LIMIT 1)")
    original = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone())
    bodies = [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    result = repair_incomplete_scopes(queue, max_seconds=10)
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')] == bodies
    if gate != 'inflight_article':
        assert result['page_checks_scheduled'] == 0
        assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (second,)).fetchone()) == original


def test_missing_list_evidence_reuses_verified_retained_envelope_without_http_or_body_reaccept(queue):
    from downloader.news_fulltext_cnyes_history import reconcile_scope
    url, _ = seed_retained_work(queue)
    with queue.db:
        queue.db.execute('DELETE FROM cnyes_history_retained_work')
        queue.db.execute('INSERT INTO bulk_page_evidence VALUES (?,1,2,?,?)',
            (cnyes_scope(url), cnyes_pagination(response(source_payload([1, 2], epoch=bounds(url)[0]), url))[0]['ids_sha256'],
             '2020-01-01T00:00:00+00:00'))
        reconcile_scope(queue, cnyes_scope(url), queue.registry.sources['cnyes'])
    original = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work').fetchone()[0] == 'pending'
    assert backfill(queue, max_seconds=10)['processed'] == 1
    assert head(queue, url)['state'] == 'list_complete'
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == original
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
    observation = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    assert observation['source_observed_at_utc'] == '2020-01-01T00:00:00+00:00'


def seed_legacy_overflow(queue):
    """Two real request identities share a saved ID hash but only one raw URL."""
    low = 1388534400
    urls = [cnyes_url(BASE, start=low + offset, end=low + 100 + offset) for offset in (0, 1)]
    r = response(source_payload(range(1, 31), epoch=low, total=931), urls[0])
    info, _ = cnyes_pagination(r)
    sha = hashlib.sha256(r.body).hexdigest()
    source = queue.registry.sources['cnyes']
    with queue.db:
        for url in urls:
            scope = cnyes_scope(url)
            queue.db.execute("INSERT INTO bulk_window_gaps VALUES (?,'cnyes','cnyes_single_day_page_cap','old-gap')", (scope,))
            queue.db.execute("INSERT INTO bulk_api_coverage VALUES (?,'cnyes',931,32,'old-coverage')", (scope,))
            queue.db.execute("INSERT INTO bulk_page_evidence VALUES (?,1,931,?,'old-page-clock')", (scope, info['ids_sha256']))
            queue.db.execute("INSERT INTO discovery(url,kind,provider,due,response_sha256,last_checked_at_utc) "
                             "VALUES (?,'cnyes_news','cnyes',9e9,?,'2020-01-01T00:00:00+00:00')", (url, sha))
    target = queue.root / 'quarantine' / (sha + '.parquet')
    atomic_write_parquet(target, table_from_records([{'request_url': urls[0], 'raw_response_body': r.body,
        'response_sha256': sha, 'captured_at_utc': '2020-01-01T00:00:00+00:00',
        'authorization_fingerprint': source.authorization_fingerprint, 'publish': False}]), durable=True)
    atomic_write_json(target.with_suffix('.parquet.receipt.json'), {
        'sha256': sha256_file(target), 'response_sha256': sha, 'publish': False}, durable=True)
    return urls, target


def test_overflow_retained_scan_resumes_and_shares_only_proven_page_without_http(queue):
    urls, _ = seed_legacy_overflow(queue)
    source = queue.registry.sources['cnyes']
    assert progress(queue.db, source)['overflow_retained_recovery']['scan_state'] == 'not_started'
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    first = seed_overflow_retained(queue, limit=1, max_seconds=10)
    assert not first['complete'] and first['queued'] == 1
    local = progress(queue.db, source)['overflow_retained_recovery']
    assert local['scan_state'] == 'scanning' and local['pending_retained_pages'] == 1
    # Until all eligible scopes are indexed, overflow work is not replayed.
    assert backfill(queue, limit=1, max_seconds=10)['processed'] == 0
    assert queue.db.execute("SELECT count(*) FROM cnyes_history_page_heads").fetchone()[0] == 0
    resumed = backfill(queue, limit=1, max_seconds=10)
    assert resumed['overflow_retained_scan']['complete'] and resumed['processed'] == 1
    local = progress(queue.db, source)['overflow_retained_recovery']
    assert local['scan_state'] == 'complete' and local['pending_retained_pages'] == 0
    assert local['local_raw_only'] and not local['category_gap_completion_claimed'] and local['publisher_requests'] == 0
    observations = queue.db.execute('SELECT scope_url,page,observation_sha256 FROM cnyes_history_page_heads ORDER BY scope_url').fetchall()
    assert len(observations) == 2 and observations[0]['observation_sha256'] == observations[1]['observation_sha256']
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_ID_references').fetchone()[0] == 1
    original = json.loads(queue.db.execute('SELECT observation_json FROM cnyes_history_page_observations').fetchone()[0])
    assert original['request_url'] == urls[0] and original['source_observed_at_utc'] == '2020-01-01T00:00:00+00:00'
    assert queue.db.execute('SELECT count(*) FROM bulk_window_gaps').fetchone()[0] == 2
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_gap_retirements').fetchone()[0] == 0
    assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before
    assert backfill(queue, limit=2, max_seconds=10)['processed'] == 0


@pytest.mark.parametrize('loss', ['missing', 'damaged_receipt'])
def test_overflow_invalid_retained_page_never_schedules_an_extra_source_request(queue, loss):
    _, target = seed_legacy_overflow(queue)
    assert seed_overflow_retained(queue, limit=8, max_seconds=10)['complete']
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    if loss == 'missing':
        target.unlink()
    else:
        receipt = target.with_suffix('.parquet.receipt.json')
        value = json.loads(receipt.read_bytes())
        value['sha256'] = '0' * 64
        atomic_write_json(receipt, value, durable=True)
    assert backfill(queue, max_seconds=10)['processed'] == 1
    state = queue.db.execute('SELECT state FROM cnyes_history_retained_work').fetchone()[0]
    assert state == ('retained_unavailable' if loss == 'missing' else 'retained_invalid')
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 0
    assert queue.db.execute('SELECT count(*) FROM bulk_window_gaps').fetchone()[0] == 2


def test_overflow_scan_stop_preserves_unfinished_checkpoint_and_refusals(queue):
    import threading
    urls, _ = seed_legacy_overflow(queue)
    stop = threading.Event()
    stop.set()
    assert seed_overflow_retained(queue, stop=stop)['checked'] == 0
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_retained_work').fetchone()[0] == 0
    with queue.db:
        queue.db.execute("UPDATE discovery SET error_code='http_429',attempts=2 WHERE url=?", (urls[0],))
    original = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[0],)).fetchone())
    assert seed_overflow_retained(queue, limit=8, max_seconds=10)['complete']
    assert backfill(queue, max_seconds=10)['processed'] == 1
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (urls[0],)).fetchone()) == original


def test_overflow_scan_checkpoint_survives_owner_restart(cfg, tmp_path):
    registry, _ = registry_http(cfg, tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as first:
        seed_legacy_overflow(first)
        assert not seed_overflow_retained(first, limit=1, max_seconds=10)['complete']
        assert first.db.execute("SELECT count(*) FROM cnyes_history_retained_work WHERE state='overflow_pending'").fetchone()[0] == 1
    with closing(NewsQueue(tmp_path, registry)) as resumed:
        for _ in range(5):
            result = backfill(resumed, limit=1, max_seconds=10)
            if result['exhausted']:
                break
        assert result['exhausted']
        assert resumed.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 2
        assert resumed.db.execute('SELECT count(*) FROM bulk_window_gaps').fetchone()[0] == 2


def test_overflow_scan_retries_transient_storage_without_skipping_page(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history
    seed_legacy_overflow(queue)
    read = history.pq.read_table
    def unavailable(*args, **kwargs):
        if kwargs.get('columns') == ['request_url']:
            raise OSError('temporary local storage outage')
        return read(*args, **kwargs)
    monkeypatch.setattr(history.pq, 'read_table', unavailable)
    result = seed_overflow_retained(queue, limit=8, max_seconds=10)
    # Unreadable candidates are durably enqueued before the cursor advances;
    # identity is still checked later, so neither candidate is yet evidence.
    assert result['complete'] and result['queued'] == 2
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_retained_work').fetchone()[0] == 2
    monkeypatch.setattr(history.pq, 'read_table', read)
    assert backfill(queue, max_seconds=10)['processed'] == 2
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 2


def test_overflow_scan_enqueues_unverified_candidates_if_stat_is_temporarily_unavailable(queue, monkeypatch):
    from pathlib import Path
    urls, target = seed_legacy_overflow(queue)
    is_file = Path.is_file
    def unavailable(path):
        if path == target:
            raise OSError(5, 'temporary storage I/O failure')
        return is_file(path)
    monkeypatch.setattr(Path, 'is_file', unavailable)
    result = seed_overflow_retained(queue, limit=8, max_seconds=10)
    assert result['complete'] and result['queued'] == 2
    work = queue.db.execute('SELECT request_url,state FROM cnyes_history_retained_work ORDER BY request_url').fetchall()
    assert {row['request_url'] for row in work} == set(urls)
    assert all(row['state'] == 'overflow_pending' for row in work)
    monkeypatch.setattr(Path, 'is_file', is_file)
    assert backfill(queue, max_seconds=10)['processed'] == 2
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 2
    assert queue.db.execute('SELECT count(*) FROM bulk_window_gaps').fetchone()[0] == 2


def test_overflow_unverified_wrong_request_is_not_reported_as_source_corruption(queue, monkeypatch):
    from pathlib import Path
    import pyarrow.parquet as pq
    urls, target = seed_legacy_overflow(queue)
    row = pq.read_table(target).to_pylist()[0]
    row['request_url'] = urls[1]  # The later candidate is the real original request.
    atomic_write_parquet(target, table_from_records([row]), durable=True)
    atomic_write_json(target.with_suffix('.parquet.receipt.json'), {
        'sha256': sha256_file(target), 'response_sha256': row['response_sha256'], 'publish': False}, durable=True)
    original = Path.is_file
    with monkeypatch.context() as patch:
        def unavailable(candidate):
            if candidate == target:
                raise OSError(5, 'temporary local error')
            return original(candidate)
        patch.setattr(Path, 'is_file', unavailable)
        assert seed_overflow_retained(queue, limit=8, max_seconds=10)['queued'] == 2
    assert backfill(queue, limit=8, max_seconds=10)['processed'] == 2
    states = dict(queue.db.execute('SELECT request_url,state FROM cnyes_history_retained_work'))
    assert states == {urls[0]: 'forward_evidence_available', urls[1]: 'restored'}
    receipt = queue.db.execute('SELECT * FROM cnyes_history_retained_outcome_resolutions WHERE request_url=?', (urls[0],)).fetchone()
    assert receipt['original_state'] == 'retained_request_mismatch'
    assert receipt['evidence_kind'] == 'shared_page_id_reference'
    assert json.loads(receipt['original_outcome_json'])['error'] == 'retained_original_request_differs_not_relabelled'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_heads').fetchone()[0] == 2
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_page_ID_references').fetchone()[0] == 1
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_gap_retirements').fetchone()[0] == 0


def test_overflow_local_retry_remains_in_pending_page_count(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history
    urls, target = seed_legacy_overflow(queue)
    assert seed_overflow_retained(queue, limit=8, max_seconds=10)['queued'] == 1
    original = history.pq.read_table
    with monkeypatch.context() as patch:
        def unavailable(candidate, *args, **kwargs):
            if candidate == target:
                raise OSError(5, 'temporary local error')
            return original(candidate, *args, **kwargs)
        patch.setattr(history.pq, 'read_table', unavailable)
        assert backfill(queue, max_seconds=10)['local_io_deferrals'] == 1
    local = progress(queue.db, queue.registry.sources['cnyes'])['overflow_retained_recovery']
    assert local['pending_retained_pages'] == local['local_retry_pages'] == 1
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=0 WHERE request_url=?', (urls[0],))
    assert backfill(queue, max_seconds=10)['processed'] == 1
    assert progress(queue.db, queue.registry.sources['cnyes'])['overflow_retained_recovery']['pending_retained_pages'] == 0


def test_retained_page_local_io_defers_without_remote_recheck_and_other_pages_continue(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history
    first, _ = seed_retained_work(queue)
    second, _ = seed_retained_work(queue, low=1388620800)
    source = queue.registry.sources['cnyes']
    target_sha = queue.db.execute('SELECT response_sha256 FROM discovery WHERE url=?', (first,)).fetchone()[0]
    read = history.pq.read_table
    def unavailable(path, *args, **kwargs):
        if path.name == target_sha + '.parquet':
            raise OSError(5, 'temporary storage I/O failure')
        return read(path, *args, **kwargs)
    monkeypatch.setattr(history.pq, 'read_table', unavailable)
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    result = backfill(queue, limit=8, max_seconds=10)
    assert result['local_io_deferrals'] == 1
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (first,)).fetchone()[0] == 'local_retry'
    assert queue.db.execute('SELECT state FROM cnyes_history_retained_work WHERE request_url=?', (second,)).fetchone()[0] == 'restored'
    assert head(queue, second)['state'] == 'list_complete'
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before
    retry = queue.db.execute('SELECT not_before_epoch FROM cnyes_history_retained_local_retries WHERE request_url=?', (first,)).fetchone()
    assert retry and retry[0] > time.time()
    assert backfill(queue, limit=8, max_seconds=10)['processed'] == 0
    monkeypatch.setattr(history.pq, 'read_table', read)
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=0 WHERE request_url=?', (first,))
    assert backfill(queue, limit=8, max_seconds=10)['processed'] == 1
    assert head(queue, first)['state'] == 'list_complete'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_retained_local_retries').fetchone()[0] == 0
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before
    assert progress(queue.db, source)['all_history_complete'] is False


@pytest.mark.parametrize('failure', ['stat', 'receipt', 'parquet', 'checksum', 'memory'])
def test_each_local_read_failure_is_retryable_without_publisher_recheck(queue, monkeypatch, failure):
    from pathlib import Path
    from downloader import news_fulltext_cnyes_history as history
    url, r = seed_retained_work(queue)
    path = queue.root / 'quarantine' / (hashlib.sha256(r.body).hexdigest() + '.parquet')
    before = tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    error = MemoryError('local pressure') if failure == 'memory' else OSError(5, 'temporary local error')
    with monkeypatch.context() as patch:
        if failure == 'stat':
            original = Path.is_file
            def unavailable(candidate):
                if candidate == path:
                    raise error
                return original(candidate)
            patch.setattr(Path, 'is_file', unavailable)
        elif failure == 'receipt':
            original = Path.read_bytes
            def unavailable(candidate):
                if candidate == path.with_suffix('.parquet.receipt.json'):
                    raise error
                return original(candidate)
            patch.setattr(Path, 'read_bytes', unavailable)
        elif failure in ('parquet', 'memory'):
            original = history.pq.read_table
            def unavailable(candidate, *args, **kwargs):
                if candidate == path:
                    raise error
                return original(candidate, *args, **kwargs)
            patch.setattr(history.pq, 'read_table', unavailable)
        else:
            original = history.sha256_file
            def unavailable(candidate):
                if candidate == path:
                    raise error
                return original(candidate)
            patch.setattr(history, 'sha256_file', unavailable)
        result = backfill(queue, max_seconds=10)
    assert result['local_io_deferrals'] == 1 and result['exhausted']
    retry = queue.db.execute('SELECT * FROM cnyes_history_retained_local_retries WHERE request_url=?', (url,)).fetchone()
    assert retry['original_state'] == 'pending' and retry['local_attempts'] == 1
    assert retry['error_code'] == ('local_memory_unavailable' if failure == 'memory' else 'local_io_unavailable')
    assert retry['not_before_epoch'] > time.time()
    assert result['next_local_retry_epoch'] == retry['not_before_epoch']
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
    assert progress(queue.db, queue.registry.sources['cnyes'])['overflow_retained_recovery']['local_retry_pages'] == 1
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=0 WHERE request_url=?', (url,))
    assert backfill(queue, max_seconds=10)['processed'] == 1
    assert head(queue, url)['state'] == 'list_complete'
    assert queue.db.execute('SELECT count(*) FROM cnyes_history_retained_local_retries').fetchone()[0] == 0
    assert tuple(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_local_retry_survives_owner_restart_and_obeys_deadline(cfg, tmp_path, monkeypatch):
    from pathlib import Path
    registry, _ = registry_http(cfg, tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as first:
        url, r = seed_retained_work(first)
        path = first.root / 'quarantine' / (hashlib.sha256(r.body).hexdigest() + '.parquet')
        original = Path.is_file
        def unavailable(candidate):
            if candidate == path:
                raise OSError(5, 'local storage unavailable')
            return original(candidate)
        with monkeypatch.context() as patch:
            patch.setattr(Path, 'is_file', unavailable)
            result = backfill(first, max_seconds=10)
        deadline = result['next_local_retry_epoch']
        with first.db:
            first.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=? WHERE request_url=?',
                             (deadline + 60, url))
    with closing(NewsQueue(tmp_path, registry)) as resumed:
        result = backfill(resumed, max_seconds=10)
        assert result['processed'] == 0 and result['exhausted']
        assert result['next_local_retry_epoch'] == deadline + 60
        with resumed.db:
            resumed.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=0 WHERE request_url=?', (url,))
        assert backfill(resumed, max_seconds=10)['processed'] == 1
        assert head(resumed, url)['state'] == 'list_complete'


def test_due_local_retry_and_new_work_share_single_item_slices(queue, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history
    first, r = seed_retained_work(queue)
    sha = hashlib.sha256(r.body).hexdigest()
    original = history.pq.read_table
    with monkeypatch.context() as patch:
        def unavailable(path, *args, **kwargs):
            if path.name == sha + '.parquet':
                raise OSError(5, 'temporary local error')
            return original(path, *args, **kwargs)
        patch.setattr(history.pq, 'read_table', unavailable)
        assert backfill(queue, limit=1, max_seconds=10)['local_io_deferrals'] == 1
    second, _ = seed_retained_work(queue, low=1388620800)
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_retained_local_retries SET not_before_epoch=0 WHERE request_url=?', (first,))
    assert backfill(queue, limit=1, max_seconds=10)['processed'] == 1
    assert backfill(queue, limit=1, max_seconds=10)['processed'] == 1
    assert head(queue, first)['state'] == head(queue, second)['state'] == 'list_complete'


def test_missing_source_endpoint_does_not_block_other_retained_pages(queue):
    first, _ = seed_retained_work(queue)
    second, _ = seed_retained_work(queue, low=1388620800)
    unavailable = first.replace(BASE, 'https://unregistered.invalid/news')
    with queue.db:
        queue.db.execute('UPDATE cnyes_history_retained_work SET request_url=? WHERE request_url=?', (unavailable, first))
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    result = backfill(queue, max_seconds=10)
    assert result['source_scope_deferrals'] == 1 and result['local_io_deferrals'] == 0
    retry = queue.db.execute('SELECT * FROM cnyes_history_retained_local_retries WHERE request_url=?', (unavailable,)).fetchone()
    assert retry['error_code'] == 'source_endpoint_unavailable' and retry['not_before_epoch'] > time.time()
    assert head(queue, second)['state'] == 'list_complete'
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before


def test_history_maintenance_delay_honours_future_deadline_and_bounds():
    from downloader.news_fulltext_cnyes_history import maintenance_delay
    assert maintenance_delay({'exhausted': False, 'next_local_retry_epoch': 1000}, now_epoch=0) == 2
    assert maintenance_delay({'exhausted': False, 'processed': 1}, now_epoch=0) == 0
    assert maintenance_delay({'exhausted': False, 'accepted_missing_batch_retries': {'processed': 1}}, now_epoch=0) == 0
    assert maintenance_delay({'exhausted': False, 'processed': 0,
        'unobserved_nonzero_page_scan': {'complete': False, 'checked': 1}}, now_epoch=0) == 0
    assert maintenance_delay({'exhausted': False, 'processed': 0,
        'unobserved_nonzero_page_scan': {'complete': False, 'checked': 0}}, now_epoch=0) == 2
    assert maintenance_delay({'exhausted': False,
        'unobserved_nonzero_page_scan': {'complete': False, 'stale_fences_reconciled': 1}}, now_epoch=0) == 0
    assert maintenance_delay({'exhausted': True, 'next_local_retry_epoch': 105}, now_epoch=100) == 5
    assert maintenance_delay({'exhausted': True, 'next_local_retry_epoch': 99}, now_epoch=100) == 1
    assert maintenance_delay({'exhausted': True, 'next_local_retry_epoch': 1000}, now_epoch=100) == 300
    for invalid in (None, True, float('nan'), float('inf'), -1, '105'):
        assert maintenance_delay({'exhausted': True, 'next_local_retry_epoch': invalid}, now_epoch=100) == 300


@pytest.mark.parametrize(('scan_key', 'counter'), [
    ('legacy_mismatch_reconciliation', 'checked'), ('legacy_unavailable_reconciliation', 'checked'),
    ('unobserved_empty_scope_scan', 'scanned'), ('overflow_retained_scan', 'checked'),
    ('completed_integrity_scan', 'checked'),
])
def test_history_maintenance_continues_each_productive_scan_without_waiting(scan_key, counter):
    from downloader.news_fulltext_cnyes_history import maintenance_delay
    assert maintenance_delay({'exhausted': False, scan_key: {'complete': False, counter: 1}}) == 0
    assert maintenance_delay({'exhausted': False, scan_key: {'complete': False, counter: 0}}) == 2
    assert maintenance_delay({'exhausted': False, scan_key: {'complete': True, counter: 1}}) == 2


@pytest.mark.parametrize('scan', ['mismatch', 'unavailable', 'empty', 'nonzero', 'overflow'])
def test_history_scan_makes_one_step_when_fetch_consumes_positive_slice(queue, monkeypatch, scan):
    from types import SimpleNamespace
    from downloader import news_fulltext_cnyes_history as history
    if scan == 'overflow':
        seed_legacy_overflow(queue)
        function, counter = history.seed_overflow_retained, 'checked'
    else:
        urls = [seed_retained_work(queue, failure='http_429', valid_raw=False, low=low)[0]
                for low in (1388534400, 1388620800)]
        with queue.db:
            if scan == 'mismatch':
                queue.db.execute("UPDATE cnyes_history_retained_work SET state='retained_invalid',outcome_json=?",
                    (json.dumps({'error': 'retained_original_request_differs_not_relabelled'}),))
            elif scan == 'unavailable':
                queue.db.execute("UPDATE cnyes_history_retained_work SET state='retained_unavailable'")
        function, counter = {
            'mismatch': (history.reconcile_legacy_request_mismatches, 'checked'),
            'unavailable': (history.reconcile_legacy_unavailable, 'checked'),
            'empty': (history.seed_unobserved_empty_scopes, 'scanned'),
            'nonzero': (history.seed_unobserved_nonzero_pages, 'checked'),
        }[scan]
        before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    clock = iter((0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
    real_time = history.time
    monkeypatch.setattr(history, 'time', SimpleNamespace(time=real_time.time, monotonic=lambda: next(clock, 10)))
    result = function(queue, limit=8, max_seconds=.001)
    assert result[counter] == 1 and not result['complete']
    from threading import Event
    stop = Event()
    stop.set()
    changes = queue.db.total_changes
    assert function(queue, limit=8, max_seconds=.001, stop=stop)[counter] == 0
    assert function(queue, limit=8, max_seconds=0)[counter] == 0
    assert queue.db.total_changes == changes
    if scan != 'overflow':
        assert [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before


def test_legacy_unavailable_scan_progress_wakes_the_next_owner_slice(queue):
    from downloader.news_fulltext_cnyes_history import maintenance_delay
    urls = [seed_retained_work(queue, failure='http_429', valid_raw=False, low=low)[0]
            for low in (1388534400, 1388620800, 1388707200)]
    with queue.db:
        queue.db.execute("UPDATE cnyes_history_retained_work SET state='retained_unavailable'")
    before = [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls]
    result = backfill(queue, limit=1, max_seconds=10)
    assert not result['exhausted'] and result['processed'] == 0
    assert result['legacy_unavailable_reconciliation']['checked'] == 1
    assert not result['legacy_unavailable_reconciliation']['complete']
    assert maintenance_delay(result) == 0
    assert [dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) for url in urls] == before


def test_new_source_total_rechecks_only_conflicting_page_once_until_actual_source_converges(queue):
    low = 1388534400
    first = cnyes_url(BASE, start=low, end=low + 100)
    second = cnyes_url(BASE, 2, start=low, end=low + 100)
    capture(queue, first, source_payload(range(1, 31), epoch=low, total=31))
    old = head(queue, first)['proof_sha256']
    capture(queue, second, source_payload([31, 32], epoch=low, page=2, total=32))
    assert head(queue, first)['state'] == 'inconsistent_source_snapshot'
    assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (first,)).fetchone()[0] <= time.time()
    assert queue.db.execute('SELECT etag FROM discovery WHERE url=?', (first,)).fetchone()[0] is None
    markers = queue.db.execute("SELECT count(*) FROM queue_settings WHERE key LIKE 'cnyes_history_direct_page_recheck_v1:%'").fetchone()[0]
    assert markers == 1
    capture(queue, second, source_payload([31, 32], epoch=low, page=2, total=32))
    assert queue.db.execute("SELECT count(*) FROM queue_settings WHERE key LIKE 'cnyes_history_direct_page_recheck_v1:%'").fetchone()[0] == markers
    capture(queue, first, source_payload(range(1, 31), epoch=low, total=32))
    assert head(queue, first)['state'] == 'list_complete'
    assert queue.db.execute('SELECT 1 FROM cnyes_history_scope_proofs WHERE proof_sha256=?', (old,)).fetchone()


@pytest.fixture
def unresolved_successful_conflict(queue, monkeypatch):
    """An actual-shaped pre-fix dead end, not a failed/unfinished HTTP job."""
    import downloader.news_fulltext as main
    import downloader.news_fulltext_cnyes_history as history
    clock = [time.time()]
    stamp = lambda: datetime.fromtimestamp(clock[0], timezone.utc).isoformat()
    monkeypatch.setattr(history.time, 'time', lambda: clock[0])
    monkeypatch.setattr(history, 'now', stamp)
    monkeypatch.setattr(main, 'utc_now', stamp)
    low = 1388534400
    first = cnyes_url(BASE, start=low, end=low + 100)
    second = cnyes_url(BASE, 2, start=low, end=low + 100)

    def old_one_shot(value, url, source, *, scope, evidence_sha, confirmed_at):
        return history.schedule_original_page_check(value, url,
            reason='contradictory_list_page_evidence', evidence_sha=evidence_sha)

    def successful_but_still_conflicting():
        # Real acceptance code produces new page-confirmation and job clocks.
        # Retain v1 scheduling during seeding to model the old owner exactly.
        with monkeypatch.context() as old:
            old.setattr(history, 'schedule_consistency_recheck', old_one_shot)
            for url, ids, page, total in ((first, range(1, 31), 1, 31), (second, [31, 32], 2, 32)):
                clock[0] += 1
                capture(queue, url, source_payload(ids, epoch=low, page=page, total=total))

    successful_but_still_conflicting()
    successful_but_still_conflicting()
    assert head(queue, first)['state'] == 'inconsistent_source_snapshot'
    scope = cnyes_scope(first)
    source = queue.registry.sources['cnyes']
    stable = 'cnyes_history_consistency_recheck_v2:' + history.digest((
        first, history.cnyes_bulk_fingerprint(source), source.authorization_fingerprint))
    return {'first': first, 'second': second, 'scope': scope, 'source': source,
        'stable': stable, 'clock': clock, 'capture': successful_but_still_conflicting}


def consistency_control(queue, conflict):
    row = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (conflict['stable'],)).fetchone()
    return json.loads(row[0]) if row else None


def checked_conflict_scope(queue, *, repair=repair_incomplete_scopes):
    """Check this fixture's single scope, even if the first seek used its slice."""
    result = repair(queue, max_seconds=10)
    if result['scopes_checked'] == 0 and result.get('unfinished_slice'):
        # Match the owner's existing prioritized continuation, without
        # treating a timed-out seek as a successful negative check.
        result = repair(queue, max_seconds=10, allow_one_after_timeout=True)
    assert result['scopes_checked'] == 1, result
    return result


@pytest.mark.parametrize('corrupt', [False, True])
def test_consistency_repair_checks_scope_after_slow_seek(queue, unresolved_successful_conflict, monkeypatch, corrupt):
    from types import SimpleNamespace
    import downloader.news_fulltext_cnyes_history as history

    conflict = unresolved_successful_conflict
    if corrupt:
        assert checked_conflict_scope(queue)['page_checks_scheduled'] == 1
        control = consistency_control(queue, conflict)
        conflict['clock'][0] = control['next_due_epoch'] + 1
        conflict['capture']()
        control['attempts'] = True
        with queue.db:
            queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?',
                             (json.dumps(control), conflict['stable']))
    before = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['first'],)).fetchone())
    ticks = iter((0.0, 11.0, 12.0, 23.0))
    calls = []

    def observe(*args, **kwargs):
        result = repair_incomplete_scopes(*args, **kwargs)
        calls.append((kwargs, result))
        return result

    with monkeypatch.context() as patch:
        patch.setattr(history, 'time', SimpleNamespace(
            monotonic=lambda: next(ticks, 24.0), time=lambda: conflict['clock'][0]))
        result = checked_conflict_scope(queue, repair=observe)
    assert len(calls) == 2
    assert calls[0][1]['scopes_checked'] == 0 and calls[0][1]['unfinished_slice']
    assert calls[1][0]['allow_one_after_timeout'] is True
    assert result['page_checks_scheduled'] == int(not corrupt)
    if corrupt:
        assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['first'],)).fetchone()) == before
        assert consistency_control(queue, conflict)['attempts'] is True


def test_still_conflicting_success_recovers_only_unfinished_page_preserving_v1_body_and_clock(queue, unresolved_successful_conflict):
    conflict = unresolved_successful_conflict
    original = [tuple(r) for r in queue.db.execute("SELECT * FROM queue_settings WHERE key LIKE 'cnyes_history_direct_page_recheck_v1:%' ORDER BY key")]
    bodies = [tuple(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    healthy = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['second'],)).fetchone())
    old = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['first'],)).fetchone())
    assert checked_conflict_scope(queue)['page_checks_scheduled'] == 1
    control = consistency_control(queue, conflict)
    job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['first'],)).fetchone())
    assert control['attempts'] == 1 and control['spacing_seconds'] == conflict['source'].discovery_interval_seconds
    assert control['next_due_epoch'] == max(conflict['clock'][0],
        datetime.fromisoformat(old['last_checked_at_utc']).timestamp() + control['spacing_seconds'])
    assert {k: v for k, v in job.items() if k not in {'due', 'etag', 'last_modified'}} == {
        k: v for k, v in old.items() if k not in {'due', 'etag', 'last_modified'}}
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['second'],)).fetchone()) == healthy
    assert [tuple(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY id')] == bodies
    assert [tuple(r) for r in queue.db.execute("SELECT * FROM queue_settings WHERE key LIKE 'cnyes_history_direct_page_recheck_v1:%' ORDER BY key")] == original
    assert head(queue, conflict['first'])['state'] == 'inconsistent_source_snapshot'


def test_consistency_recheck_needs_new_response_not_restart_or_proof_change(queue, unresolved_successful_conflict):
    from downloader.news_fulltext_cnyes_history import schedule_consistency_recheck
    conflict = unresolved_successful_conflict
    assert checked_conflict_scope(queue)['page_checks_scheduled'] == 1
    control = consistency_control(queue, conflict)
    with queue.db:
        queue.db.execute('UPDATE discovery SET due=9e9 WHERE url=?', (conflict['first'],))
    confirmed = queue.db.execute('SELECT last_confirmed_at_utc FROM cnyes_history_page_heads WHERE scope_url=? AND page=1', (conflict['scope'],)).fetchone()[0]
    assert not schedule_consistency_recheck(queue, conflict['first'], conflict['source'],
        scope=conflict['scope'], evidence_sha='changed-proof-without-source-response', confirmed_at=confirmed)
    queue.close()  # A restart releases the existing owner, never opens a second collector.
    with closing(NewsQueue(queue.root, queue.registry)) as resumed:
        assert checked_conflict_scope(resumed)['page_checks_scheduled'] == 0
        assert consistency_control(resumed, conflict) == control
        # A new successful job clock alone (e.g.304) cannot masquerade as a new
        # source-page confirmation. No actual body/list evidence was replaced.
        with resumed.db:
            resumed.db.execute('UPDATE discovery SET last_checked_at_utc=? WHERE url=?',
                (datetime.fromtimestamp(conflict['clock'][0] + 600, timezone.utc).isoformat(), conflict['first']))
        assert checked_conflict_scope(resumed)['page_checks_scheduled'] == 0
        assert consistency_control(resumed, conflict) == control


def test_repeated_successful_conflicts_back_off_at_source_cadence_not_permanent_one_shot(queue, unresolved_successful_conflict):
    conflict = unresolved_successful_conflict
    original = [tuple(r) for r in queue.db.execute("SELECT * FROM queue_settings WHERE key LIKE 'cnyes_history_direct_page_recheck_v1:%' ORDER BY key")]
    for count, factor in enumerate((1, 2, 4, 8, 16, 16), 1):
        assert checked_conflict_scope(queue)['page_checks_scheduled'] == 1
        control = consistency_control(queue, conflict)
        assert control['attempts'] == count
        assert control['spacing_seconds'] == factor * conflict['source'].discovery_interval_seconds
        assert checked_conflict_scope(queue)['page_checks_scheduled'] == 0
        conflict['clock'][0] = control['next_due_epoch'] + 1
        conflict['capture']()
    assert [tuple(r) for r in queue.db.execute("SELECT * FROM queue_settings WHERE key LIKE 'cnyes_history_direct_page_recheck_v1:%' ORDER BY key")] == original


@pytest.mark.parametrize('gate', ['http_403', 'http_429', 'transport_retry', 'attempts', 'split', 'due',
    'provider', 'kind', 'raw_hash', 'scope_complete', 'stale_contract', 'unconfirmed', 'disabled', 'before_first_recheck'])
def test_recurring_consistency_repair_never_overrides_other_source_evidence_or_schedules(queue, unresolved_successful_conflict, gate):
    from dataclasses import replace
    from downloader.news_fulltext_cnyes_history import schedule_consistency_recheck
    conflict = unresolved_successful_conflict
    assert checked_conflict_scope(queue)['page_checks_scheduled'] == 1
    prior_control = consistency_control(queue, conflict)
    conflict['clock'][0] = prior_control['next_due_epoch'] + 1
    conflict['capture']()
    source = conflict['source']
    with queue.db:
        if gate.startswith(('http_', 'transport_')):
            queue.db.execute('UPDATE discovery SET error_code=? WHERE url=?', (gate, conflict['first']))
        elif gate in {'attempts', 'due', 'kind', 'provider', 'raw_hash'}:
            field, value = {'attempts': ('attempts', 1), 'due': ('due', 0), 'kind': ('kind', 'index'),
                'provider': ('provider', 'not-cnyes'), 'raw_hash': ('response_sha256', 'unproven-hash')}[gate]
            queue.db.execute('UPDATE discovery SET ' + field + '=? WHERE url=?', (value, conflict['first']))
        elif gate == 'split':
            queue.db.execute('UPDATE discovery SET superseded_by_windows_json=? WHERE url=?', ('[]', conflict['first']))
        elif gate == 'scope_complete':
            queue.db.execute("UPDATE cnyes_history_scope_heads SET state='list_complete' WHERE scope_url=?", (conflict['scope'],))
        elif gate == 'stale_contract':
            queue.db.execute("UPDATE cnyes_history_scope_heads SET source_bulk_fingerprint='other-contract' WHERE scope_url=?", (conflict['scope'],))
        elif gate == 'unconfirmed':
            queue.db.execute('UPDATE cnyes_history_page_heads SET last_confirmed_at_utc=? WHERE scope_url=? AND page=1',
                (prior_control['page_confirmed_at_utc'], conflict['scope']))
        elif gate == 'before_first_recheck':
            original = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
                (prior_control['prior_v1_marker'],)).fetchone()[0])
            queue.db.execute('UPDATE cnyes_history_page_heads SET last_confirmed_at_utc=? WHERE scope_url=? AND page=1',
                (original['changed_at_utc'], conflict['scope']))
        elif gate == 'disabled':
            source = replace(source, allowed=False)
    job = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['first'],)).fetchone())
    confirmed = queue.db.execute('SELECT last_confirmed_at_utc FROM cnyes_history_page_heads WHERE scope_url=? AND page=1', (conflict['scope'],)).fetchone()[0]
    assert not schedule_consistency_recheck(queue, conflict['first'], source,
        scope=conflict['scope'], evidence_sha='a-new-proof-cannot-reset-gates', confirmed_at=confirmed)
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['first'],)).fetchone()) == job
    assert consistency_control(queue, conflict) == prior_control


@pytest.mark.parametrize('corrupt', ['empty_list', 'empty_dict', 'bad_json', 'negative_count', 'zero_count', 'bool_count',
    'wrong_url', 'wrong_scope', 'wrong_version', 'missing_prior', 'changed_prior', 'naive_clock'])
def test_corrupt_consistency_witness_cannot_mint_new_retry_budget(queue, unresolved_successful_conflict, corrupt):
    conflict = unresolved_successful_conflict
    assert checked_conflict_scope(queue)['page_checks_scheduled'] == 1
    control = consistency_control(queue, conflict)
    conflict['clock'][0] = control['next_due_epoch'] + 1
    conflict['capture']()
    value = deepcopy(control)
    with queue.db:
        if corrupt == 'missing_prior':
            queue.db.execute('DELETE FROM queue_settings WHERE key=?', (control['prior_v1_marker'],))
        elif corrupt == 'changed_prior':
            old = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (control['prior_v1_marker'],)).fetchone()[0])
            old['prior_job']['url'] = 'https://example.invalid/another-source'
            queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', (json.dumps(old), control['prior_v1_marker']))
        else:
            if corrupt == 'empty_list': value = []
            elif corrupt == 'empty_dict': value = {}
            elif corrupt == 'negative_count': value['attempts'] = -1
            elif corrupt == 'zero_count': value['attempts'] = 0
            elif corrupt == 'bool_count': value['attempts'] = True
            elif corrupt == 'wrong_url': value['url'] = conflict['second']
            elif corrupt == 'wrong_scope': value['scope_url'] = 'not-this-scope'
            elif corrupt == 'wrong_version': value['contract_version'] = 999
            elif corrupt == 'naive_clock': value['page_confirmed_at_utc'] = '2026-10-23T00:00:00'
            queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?',
                ('{' if corrupt == 'bad_json' else json.dumps(value), conflict['stable']))
    old = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['first'],)).fetchone())
    assert checked_conflict_scope(queue)['page_checks_scheduled'] == 0
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['first'],)).fetchone()) == old


def test_source_convergence_closes_scope_without_revisiting_healthy_pages_or_erasing_old_control(queue, unresolved_successful_conflict):
    conflict = unresolved_successful_conflict
    assert checked_conflict_scope(queue)['page_checks_scheduled'] == 1
    control = consistency_control(queue, conflict)
    proof = head(queue, conflict['first'])['proof_sha256']
    conflict['clock'][0] = control['next_due_epoch'] + 1
    capture(queue, conflict['first'], source_payload(range(1, 31), epoch=bounds(conflict['first'])[0], total=32))
    assert head(queue, conflict['first'])['state'] == 'list_complete'
    assert repair_incomplete_scopes(queue, max_seconds=10)['page_checks_scheduled'] == 0
    assert consistency_control(queue, conflict) == control
    assert queue.db.execute('SELECT 1 FROM cnyes_history_scope_proofs WHERE proof_sha256=?', (proof,)).fetchone()


def test_source_page_count_growth_rechecks_contradictory_old_pages_while_new_pages_missing(queue, unresolved_successful_conflict):
    conflict = unresolved_successful_conflict
    old_proof = head(queue, conflict['first'])['proof_sha256']
    # Real source growth changes a seven-page contradiction into an incomplete
    # eleven-page scope. The smaller fixture preserves that exact dependency.
    conflict['clock'][0] += 1
    low, high = bounds(conflict['first'])
    capture(queue, conflict['second'], source_payload(range(31, 61), epoch=low, page=2, total=92))
    assert head(queue, conflict['first'])['state'] == 'waiting_page_evidence'
    assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (conflict['first'],)).fetchone()[0] <= conflict['clock'][0]
    assert queue.db.execute('SELECT due FROM discovery WHERE url=?',
        (cnyes_url(BASE, 3, start=low, end=high),)).fetchone()[0] <= conflict['clock'][0]
    capture(queue, conflict['first'], source_payload(range(1, 31), epoch=low, total=92))
    healthy = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['first'],)).fetchone())
    assert checked_conflict_scope(queue)['page_checks_scheduled'] == 0
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (conflict['first'],)).fetchone()) == healthy
    for page, ids in ((3, range(61, 91)), (4, [91, 92])):
        conflict['clock'][0] += 1
        capture(queue, cnyes_url(BASE, page, start=low, end=high), source_payload(ids, epoch=low, page=page, total=92))
    assert head(queue, conflict['first'])['state'] == 'list_complete'
    assert json.loads(head(queue, conflict['first'])['member_ids_json']) == list(range(1, 93))
    assert queue.db.execute('SELECT 1 FROM cnyes_history_scope_proofs WHERE proof_sha256=?', (old_proof,)).fetchone()


def test_repair_frontier_rotates_past_unresolved_scope_and_stops_without_cursor_mutation(queue):
    import threading
    from downloader.news_fulltext_cnyes_history import REPAIR_CURSOR
    for day in range(3):
        low = 1388534400 + day * 86400
        capture(queue, cnyes_url(BASE, start=low, end=low + 100), source_payload(range(1, 31), epoch=low, total=31))
    assert repair_incomplete_scopes(queue, limit=1, max_seconds=10)['scopes_checked'] == 1
    cursor = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (REPAIR_CURSOR,)).fetchone()[0]
    stop = threading.Event()
    stop.set()
    assert repair_incomplete_scopes(queue, stop=stop)['scopes_checked'] == 0
    assert repair_incomplete_scopes(queue, max_seconds=0)['scopes_checked'] == 0
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (REPAIR_CURSOR,)).fetchone()[0] == cursor
    assert repair_incomplete_scopes(queue, limit=1, max_seconds=10)['scopes_checked'] == 1
    assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (REPAIR_CURSOR,)).fetchone()[0] > cursor
