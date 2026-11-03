"""Offline lifecycle and fencing for oversized retained-source repair."""
from contextlib import closing
import base64
import os
import threading
import time

import pytest

from downloader.artifact_io import sha256_file
from downloader.news_fulltext_metadata_replay import replay_retained_cnyes_metadata
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_cnyes_news_bulk import cfg
from test_news_cnyes_canonical_alias import mobile_page
from test_news_retained_metadata_replay import old_head


@pytest.fixture
def small_limit(monkeypatch):
    # The real >32 MiB decoder is separately exercised by worker tests; this
    # small threshold exercises the entire owner lifecycle without large I/O.
    from downloader import news_fulltext_metadata_replay as metadata
    monkeypatch.setattr(metadata, 'METADATA_REPLAY_MAX_BYTES', 64 << 10)
    return metadata.METADATA_REPLAY_MAX_BYTES


def oversized_head(queue, limit):
    payload = mobile_page() + b'<!--' + base64.b64encode(os.urandom(64 << 10)) + b'-->'
    head = old_head(queue, payload=payload)
    assert (queue.root / head['parquet_path']).stat().st_size > limit
    return head


def step(queue, limit, **kwargs):
    return replay_retained_cnyes_metadata(queue, maximum_bytes=limit, maximum_wall_seconds=1, **kwargs)


def wait_for(queue, limit, predicate, *, timeout=30):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        result = step(queue, limit)
        if predicate():
            return result
        time.sleep(.05)
    raise AssertionError('isolated retained-source work did not finish')


def test_large_replay_accepts_only_after_independent_proof_and_keeps_original(cfg, tmp_path, small_limit):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = oversized_head(queue, small_limit)
        original_path = tmp_path / original['parquet_path']
        receipt = queue.db.execute('SELECT receipt_path FROM accepted_batches').fetchone()[0]
        before = sha256_file(original_path), sha256_file(tmp_path / receipt)
        result = step(queue, small_limit)
        assert result['http_requests'] == result['repaired_versions'] == 0
        assert queue._large_metadata_job is not None
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original
        wait_for(queue, small_limit, lambda: queue.db.execute(
            "SELECT state FROM retained_metadata_replays").fetchone()[0] == 'verified_repair')
        current = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        assert current['parquet_path'] != original['parquet_path']
        for field in ('captured_at_utc', 'last_checked_at_utc', 'attempts', 'due', 'error_code'):
            assert current[field] == original[field]
        assert before == (sha256_file(original_path), sha256_file(tmp_path / receipt))
        assert queue.db.execute('SELECT count(*) FROM retained_metadata_resource_waits').fetchone()[0] == 0
        assert verify_bodies(tmp_path)['accepted']


def test_large_replay_does_not_complete_when_current_head_refuses_adoption(cfg, tmp_path, small_limit, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = oversized_head(queue, small_limit)
        accept = queue._accept_records
        def refused(records, relative, receipt, **kwargs):
            with queue.db:
                queue.db.execute("UPDATE articles SET state='retry',error_code='http_429',due=4102444800")
            return accept(records, relative, receipt, **kwargs)
        monkeypatch.setattr(queue, '_accept_records', refused)
        step(queue, small_limit)
        wait_for(queue, small_limit, lambda: queue.db.execute(
            "SELECT 1 FROM retained_metadata_large_retries WHERE last_error='metadata_acceptance_not_adopted'").fetchone())
        current = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        assert current['parquet_path'] == original['parquet_path'] and current['error_code'] == 'http_429'
        assert queue.db.execute('SELECT state FROM retained_metadata_replays').fetchone()[0] == 'resource_wait'
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 2


def test_megabyte_original_roundtrips_through_full_owner_without_changing_wire(cfg, tmp_path, small_limit):
    # Compression keeps the private fixture small on disk; the individual
    # original response stays below the configured 8 MiB source limit.
    payload = mobile_page() + b'<!--' + b'x' * (6 << 20) + b'-->'
    assert len(payload) < 8 << 20
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = old_head(queue, payload=payload)
        source_path = tmp_path / original['parquet_path']
        source_sha = sha256_file(source_path)
        step(queue, small_limit)
        assert queue._large_metadata_job is not None
        wait_for(queue, small_limit, lambda: queue.db.execute(
            "SELECT state FROM retained_metadata_replays").fetchone()[0] == 'verified_repair', timeout=90)
        current = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        assert current['parquet_path'] != original['parquet_path']
        for field in ('captured_at_utc', 'last_checked_at_utc', 'attempts', 'due', 'error_code'):
            assert current[field] == original[field]
        assert sha256_file(source_path) == source_sha
        assert verify_bodies(tmp_path)['accepted']


def test_many_megabyte_originals_cross_real_large_threshold_and_repair_only_target(cfg, tmp_path):
    import hashlib
    from types import SimpleNamespace
    import pyarrow.parquet as pq
    from downloader.news_fulltext import fetch_article
    from downloader.news_fulltext_http import Response
    from downloader.news_fulltext_metadata_replay import METADATA_REPLAY_MAX_BYTES

    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        records = []
        for number in range(6):
            story_id = str(100000 + number)
            url = 'https://m.cnyes.com/news/id/' + story_id
            queue.add_articles([{'url': url}])
            head = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
            raw = mobile_page().replace(b'42', story_id.encode()) + b'<!--' + b'x' * (6 << 20) + b'-->'
            assert len(raw) < 8 << 20
            http = SimpleNamespace(registry=queue.registry,
                get=lambda *_a, url=url, raw=raw, **_k: Response(url, 200, {'content-type': 'text/html'}, raw))
            record = fetch_article(http, head)
            record.update(captured_at_utc='2026-10-01T00:00:00+00:00', data_fingerprint='old-capture',
                          published_at_raw=None, authors_json='[]',
                          body_completeness='article_container_unverified', extraction_method='css:#article-container')
            record['body'] += '\u200c\u200c'
            record['body_sha256'] = hashlib.sha256(record['body'].encode()).hexdigest()
            record['body_bytes'] = len(record['body'].encode())
            records.append(record)
        queue.accept(records)
        before = {row['id']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
        target = next(iter(before))
        path = tmp_path / before[target]['parquet_path']
        meta = pq.ParquetFile(path).metadata
        decoded = sum(meta.row_group(i).total_byte_size for i in range(meta.num_row_groups))
        assert decoded > METADATA_REPLAY_MAX_BYTES == 32 << 20
        assert path.stat().st_size < 4 << 20  # Synthetic inputs are deliberately compressible.
        source_sha = sha256_file(path)
        del records
        until = time.monotonic() + 90
        while time.monotonic() < until:
            step(queue, METADATA_REPLAY_MAX_BYTES, target_article_ids=[target])
            row = queue.db.execute('SELECT state FROM retained_metadata_replays WHERE article_id=?', (target,)).fetchone()
            if row and row['state'] == 'verified_repair':
                break
            time.sleep(.05)
        else:
            raise AssertionError('large source did not complete through the canonical owner')
        after = {row['id']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
        assert after[target]['parquet_path'] != before[target]['parquet_path']
        for article_id in before.keys() - {target}:
            assert after[article_id] == before[article_id]
        for field in ('captured_at_utc', 'last_checked_at_utc', 'attempts', 'due', 'error_code'):
            assert after[target][field] == before[target][field]
        assert sha256_file(path) == source_sha and verify_bodies(tmp_path)['accepted']


def batch_fixture(queue, limit, monkeypatch):
    import test_news_retained_metadata_checkpoint_batch as fixtures

    original_page = fixtures.mobile_page
    padding = base64.b64encode(os.urandom(80 << 10))
    monkeypatch.setattr(fixtures, 'mobile_page', lambda: original_page() + b'<!--' + padding + b'-->')
    heads = fixtures.native_heads(queue, 2, repair_count=2)
    source = queue.root / next(iter(heads.values()))['parquet_path']
    assert source.stat().st_size > limit
    return heads, source


def test_same_large_original_repairs_two_heads_with_one_child(cfg, tmp_path, small_limit, monkeypatch):
    from downloader import news_fulltext_large_replay_owner as owner
    real_job, versions = owner.LargeReplayJob, []

    def tracked(request, **kwargs):
        versions.append(request['version'])
        return real_job(request, **kwargs)

    monkeypatch.setattr(owner, 'LargeReplayJob', tracked)
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        before, path = batch_fixture(queue, small_limit, monkeypatch)
        digest = sha256_file(path)
        step(queue, small_limit)
        assert len(queue._large_metadata_job['peers']) == 1 and versions == [2]
        wait_for(queue, small_limit, lambda: queue.db.execute(
            "SELECT count(*) FROM retained_metadata_replays WHERE state='verified_repair'").fetchone()[0] == 2)
        after = {row['id']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
        assert versions == [2]
        for article_id, head in after.items():
            assert head['parquet_path'] != before[article_id]['parquet_path']
            for key in ('captured_at_utc', 'last_checked_at_utc', 'attempts', 'due', 'error_code'):
                assert head[key] == before[article_id][key]
        assert sha256_file(path) == digest and verify_bodies(tmp_path)['accepted']


def test_real_threshold_batch_repairs_six_heads_with_three_children(cfg, tmp_path, monkeypatch):
    import pyarrow.parquet as pq
    from downloader import news_fulltext_large_replay_owner as owner
    from downloader.news_fulltext_metadata_replay import METADATA_REPLAY_MAX_BYTES
    import test_news_retained_metadata_checkpoint_batch as fixtures

    original_page = fixtures.mobile_page
    monkeypatch.setattr(fixtures, 'mobile_page', lambda: original_page() + b'<!--' + b'x' * (6 << 20) + b'-->')
    real_job, versions = owner.LargeReplayJob, []

    def tracked(request, **kwargs):
        versions.append(request['version'])
        return real_job(request, **kwargs)

    monkeypatch.setattr(owner, 'LargeReplayJob', tracked)
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        before = fixtures.native_heads(queue, 6, repair_count=6)
        path = tmp_path / next(iter(before.values()))['parquet_path']
        footer = pq.ParquetFile(path).metadata
        decoded = sum(footer.row_group(g).column(c).total_uncompressed_size
                      for g in range(footer.num_row_groups) for c in range(footer.num_columns))
        assert decoded > METADATA_REPLAY_MAX_BYTES == 32 << 20 and path.stat().st_size < 4 << 20
        digest = sha256_file(path)
        wait_for(queue, METADATA_REPLAY_MAX_BYTES, lambda: queue.db.execute(
            "SELECT count(*) FROM retained_metadata_replays WHERE state='verified_repair'").fetchone()[0] == 6,
            timeout=120)
        assert versions == [2, 2, 2]
        for row in queue.db.execute('SELECT * FROM articles'):
            assert row['parquet_path'] != before[row['id']]['parquet_path']
            for key in ('captured_at_utc', 'last_checked_at_utc', 'attempts', 'due', 'error_code'):
                assert row[key] == before[row['id']][key]
        assert sha256_file(path) == digest and verify_bodies(tmp_path)['accepted']


@pytest.mark.parametrize('error', ['worker_memory_exhausted', 'output_bound_exceeded', 'worker_process_failed'])
def test_failed_group_retries_independently_without_blocking_peer(cfg, tmp_path, small_limit, monkeypatch, error):
    from downloader import news_fulltext_large_replay_owner as owner
    real_job, versions = owner.LargeReplayJob, []

    class FailedBatch:
        def poll(self):
            return {'version': 2, 'state': 'resource_wait', 'error': error}

        def close(self):
            pass

    def injected(request, **kwargs):
        versions.append(request['version'])
        return FailedBatch() if request['version'] == 2 else real_job(request, **kwargs)

    monkeypatch.setattr(owner, 'LargeReplayJob', injected)
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        before, path = batch_fixture(queue, small_limit, monkeypatch)
        digest = sha256_file(path)
        step(queue, small_limit)
        until = time.monotonic() + 30
        while time.monotonic() < until:
            with queue.db:
                queue.db.execute('UPDATE retained_metadata_large_retries SET not_before_epoch=0')
            step(queue, small_limit)
            if queue.db.execute("SELECT count(*) FROM retained_metadata_replays WHERE state='verified_repair'").fetchone()[0] == 2:
                break
            time.sleep(.05)
        else:
            raise AssertionError('batch fallback did not finish')
        assert versions == [2, 1, 1]
        assert queue.db.execute('SELECT count(*) FROM retained_metadata_large_retry_history WHERE error=?',
                                ('batch_' + error,)).fetchone()[0] == 2
        assert all(row['parquet_path'] != before[row['id']]['parquet_path'] for row in queue.db.execute('SELECT * FROM articles'))
        assert sha256_file(path) == digest and verify_bodies(tmp_path)['accepted']


def test_bad_individual_proof_never_overwrites_head_or_blocks_valid_peer(cfg, tmp_path, small_limit, monkeypatch):
    from downloader import news_fulltext_large_replay_owner as owner
    real_job, versions = owner.LargeReplayJob, []

    class TamperedBatch:
        def __init__(self, request, **kwargs):
            self.inner = real_job(request, **kwargs)

        def poll(self):
            result = self.inner.poll()
            if result is not None and result.get('state') == 'ok':
                result['items'][0]['result']['metadata']['body_sha256'] = '0' * 64
            return result

        def close(self):
            self.inner.close()

    def injected(request, **kwargs):
        versions.append(request['version'])
        return TamperedBatch(request, **kwargs) if request['version'] == 2 else real_job(request, **kwargs)

    monkeypatch.setattr(owner, 'LargeReplayJob', injected)
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        before, path = batch_fixture(queue, small_limit, monkeypatch)
        digest = sha256_file(path)
        step(queue, small_limit)
        until = time.monotonic() + 30
        while time.monotonic() < until:
            with queue.db:
                queue.db.execute('UPDATE retained_metadata_large_retries SET not_before_epoch=0')
            step(queue, small_limit)
            if queue.db.execute("SELECT count(*) FROM retained_metadata_replays WHERE state='verified_repair'").fetchone()[0] == 2:
                break
            time.sleep(.05)
        else:
            raise AssertionError('individual proof did not recover')
        assert versions == [2, 1]
        assert queue.db.execute("SELECT count(*) FROM retained_metadata_large_retry_history "
                                "WHERE error='worker_head_identity_mismatch'").fetchone()[0] == 1
        assert all(row['parquet_path'] != before[row['id']]['parquet_path'] for row in queue.db.execute('SELECT * FROM articles'))
        assert sha256_file(path) == digest and verify_bodies(tmp_path)['accepted']


def test_stopped_between_batch_heads_resumes_from_durable_checkpoint(cfg, tmp_path, small_limit, monkeypatch):
    from downloader import news_fulltext_large_replay_owner as owner
    registry = PolicyRegistry(cfg, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        before, path = batch_fixture(queue, small_limit, monkeypatch)
        digest = sha256_file(path)
        step(queue, small_limit)
        task = queue._large_metadata_job
        task['job'].process.wait(timeout=30)
        checked, repaired = owner.poll_or_start(queue, registry.sources['cnyes'], small_limit, start=False)
        assert (checked, repaired) == (1, 1)
        assert len(task['pending_results']) == 1
        assert queue.db.execute("SELECT count(*) FROM retained_metadata_replays WHERE state='verified_repair'").fetchone()[0] == 1
    with closing(NewsQueue(tmp_path, registry)) as queue:
        wait_for(queue, small_limit, lambda: queue.db.execute(
            "SELECT count(*) FROM retained_metadata_replays WHERE state='verified_repair'").fetchone()[0] == 2)
        assert all(row['parquet_path'] != before[row['id']]['parquet_path'] for row in queue.db.execute('SELECT * FROM articles'))
        assert sha256_file(path) == digest and verify_bodies(tmp_path)['accepted']


def test_slow_first_batch_acceptance_yields_second_to_next_slice(cfg, tmp_path, small_limit, monkeypatch):
    from types import SimpleNamespace
    from downloader import news_fulltext_large_replay_owner as owner
    from downloader import news_fulltext_metadata_replay as metadata

    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        batch_fixture(queue, small_limit, monkeypatch)
        step(queue, small_limit)
        task = queue._large_metadata_job
        task['job'].process.wait(timeout=30)
        clock = [0.0]
        real_finish = owner._finish

        def slow_finish(*args, **kwargs):
            result = real_finish(*args, **kwargs)
            clock[0] += 1.0
            return result

        monkeypatch.setattr(owner, '_finish', slow_finish)
        monkeypatch.setattr(metadata, 'time', SimpleNamespace(monotonic=lambda: clock[0], time=time.time))
        result = replay_retained_cnyes_metadata(queue, maximum_bytes=small_limit, maximum_wall_seconds=0.5)
        assert result['heads_checked'] == result['repaired_versions'] == 1
        assert len(task['pending_results']) == 1
        assert queue.db.execute("SELECT count(*) FROM retained_metadata_replays WHERE state='verified_repair'").fetchone()[0] == 1
        clock[0] = 0.0
        result = replay_retained_cnyes_metadata(queue, maximum_bytes=small_limit, maximum_wall_seconds=0.5)
        assert result['heads_checked'] == result['repaired_versions'] == 1
        assert queue._large_metadata_job is None and verify_bodies(tmp_path)['accepted']


def test_expired_slice_still_starts_due_large_work_if_no_job_existed(cfg, tmp_path, small_limit, monkeypatch):
    from types import SimpleNamespace
    from downloader import news_fulltext_metadata_replay as metadata

    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        batch_fixture(queue, small_limit, monkeypatch)
        step(queue, small_limit)
        assert queue._large_metadata_job is not None
        stop = threading.Event()
        stop.set()
        step(queue, small_limit, stop=stop)
        assert queue._large_metadata_job is None
        clock = [0.0]
        real_ensure = metadata._ensure_cohort

        def slow_ensure(*args, **kwargs):
            result = real_ensure(*args, **kwargs)
            clock[0] += 1.0
            return result

        monkeypatch.setattr(metadata, '_ensure_cohort', slow_ensure)
        monkeypatch.setattr(metadata, 'time', SimpleNamespace(monotonic=lambda: clock[0], time=time.time))
        result = replay_retained_cnyes_metadata(queue, maximum_bytes=small_limit, maximum_wall_seconds=0.5)
        assert result['repaired_versions'] == 0
        assert queue._large_metadata_job is not None and len(queue._large_metadata_job['peers']) == 1
        assert verify_bodies(tmp_path)['accepted']


def test_batch_peer_with_new_refusal_is_not_overwritten(cfg, tmp_path, small_limit, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        before, _ = batch_fixture(queue, small_limit, monkeypatch)
        step(queue, small_limit)
        task = queue._large_metadata_job
        peer_id = task['peers'][0]['article_id']
        task['job'].process.wait(timeout=30)
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',error_code='new_refusal',due=123456 WHERE id=?", (peer_id,))
        wait_for(queue, small_limit, lambda: queue._large_metadata_job is None)
        peer = dict(queue.db.execute('SELECT * FROM articles WHERE id=?', (peer_id,)).fetchone())
        assert (peer['state'], peer['error_code'], peer['due'], peer['parquet_path']) == (
            'retry', 'new_refusal', 123456, before[peer_id]['parquet_path'])
        assert queue.db.execute("SELECT count(*) FROM retained_metadata_replays WHERE state='verified_repair'").fetchone()[0] == 1
        assert verify_bodies(tmp_path)['accepted']


def test_large_wait_is_not_cleared_while_legacy_peer_remains(cfg, tmp_path, small_limit, monkeypatch):
    from downloader import news_fulltext_large_replay_owner as owner
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        batch_fixture(queue, small_limit, monkeypatch)
        step(queue, small_limit)
        task = queue._large_metadata_job
        peer_id = task['peers'][0]['article_id']
        with queue.db:
            queue.db.execute("UPDATE retained_metadata_replays SET state='verified_repair', error_code=NULL WHERE article_id=?",
                             (task['article_id'],))
            queue.db.execute("UPDATE retained_metadata_replays SET state='failed_validation', "
                             "error_code='retained_metadata_batch_exceeds_resource_bound' WHERE article_id=?", (peer_id,))
        owner._clear(queue, task, clear_batch=True)
        assert queue.db.execute('SELECT count(*) FROM retained_metadata_resource_waits').fetchone()[0] == 1


def test_unobservable_live_reader_can_finish_through_canonical_owner(cfg, tmp_path, small_limit, monkeypatch):
    import sys
    from downloader import news_fulltext_large_replay as manager

    real_popen = manager.subprocess.Popen

    def delayed_popen(args, **kwargs):
        # Delay only this isolated test child so the parent's simulated clock
        # can advance beyond the old false-stall threshold before it exits.
        code = "import time,runpy; time.sleep(1); runpy.run_module('downloader.news_fulltext_large_replay_worker',run_name='__main__')"
        return real_popen([sys.executable, '-c', code], **kwargs)

    monkeypatch.setattr(manager.subprocess, 'Popen', delayed_popen)
    monkeypatch.setattr(manager, '_progress', lambda pid: None)
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = oversized_head(queue, small_limit)
        old_path = tmp_path / original['parquet_path']
        old_sha = sha256_file(old_path)
        assert step(queue, small_limit)['http_requests'] == 0
        job = queue._large_metadata_job['job']
        job.clock = lambda: job.last_progress + manager.STALL_SECONDS + 1
        assert job.process.poll() is None
        assert job.poll() is None and job.termination_at is None
        wait_for(queue, small_limit, lambda: queue.db.execute(
            "SELECT state FROM retained_metadata_replays").fetchone()[0] == 'verified_repair')
        current = queue.db.execute('SELECT * FROM articles').fetchone()
        assert current['parquet_path'] != original['parquet_path']
        assert current['captured_at_utc'] == original['captured_at_utc']
        assert current['last_checked_at_utc'] == original['last_checked_at_utc']
        assert sha256_file(old_path) == old_sha
        assert verify_bodies(tmp_path)['accepted']


def test_actual_over_32_mib_source_completes_owner_pipeline(cfg, tmp_path):
    # One real large column, not just a patched threshold. An oversized proof
    # also exercises adaptive output sizing before the owner accepts it.
    from downloader.news_fulltext_metadata_replay import METADATA_REPLAY_MAX_BYTES

    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        payload = mobile_page() + b'<!--' + base64.b64encode(os.urandom(26 << 20)) + b'-->'
        original = old_head(queue, payload=payload)
        source_path = tmp_path / original['parquet_path']
        original_sha = sha256_file(source_path)
        import pyarrow.parquet as pq
        metadata = pq.ParquetFile(source_path).metadata
        uncompressed = sum(metadata.row_group(g).column(c).total_uncompressed_size
                           for g in range(metadata.num_row_groups) for c in range(metadata.num_columns))
        assert uncompressed > METADATA_REPLAY_MAX_BYTES
        step(queue, METADATA_REPLAY_MAX_BYTES)
        wait_for(queue, METADATA_REPLAY_MAX_BYTES, lambda: queue.db.execute(
            "SELECT state FROM retained_metadata_replays").fetchone()[0] == 'verified_repair', timeout=90)
        assert sha256_file(source_path) == original_sha
        assert verify_bodies(tmp_path)['accepted']


def test_stop_terminates_reader_without_acceptance_and_restart_resumes(cfg, tmp_path, small_limit):
    registry = PolicyRegistry(cfg, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        original = oversized_head(queue, small_limit)
        step(queue, small_limit)
        job = queue._large_metadata_job['job']
        stop = threading.Event()
        stop.set()
        step(queue, small_limit, stop=stop)
        assert queue._large_metadata_job is None and job.process.poll() is not None
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original
        assert queue.db.execute('SELECT state FROM retained_metadata_replays').fetchone()[0] == 'resource_wait'
    with closing(NewsQueue(tmp_path, registry)) as queue:
        wait_for(queue, small_limit, lambda: queue.db.execute(
            "SELECT state FROM retained_metadata_replays").fetchone()[0] == 'verified_repair')
        assert verify_bodies(tmp_path)['accepted']


def test_inflight_refusal_cannot_be_overwritten_by_completed_proof(cfg, tmp_path, small_limit):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = oversized_head(queue, small_limit)
        step(queue, small_limit)
        job = queue._large_metadata_job['job']
        job.process.wait(timeout=30)
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',error_code='new_refusal',due=123456 WHERE id=?",
                             (original['id'],))
        step(queue, small_limit)
        current = queue.db.execute('SELECT * FROM articles').fetchone()
        assert (current['state'], current['error_code'], current['due'], current['parquet_path']) == (
            'retry', 'new_refusal', 123456, original['parquet_path'])
        assert queue._large_metadata_job is None
        assert verify_bodies(tmp_path)['accepted']


def test_ineligible_snapshot_cannot_authorize_a_replay(cfg, tmp_path, small_limit):
    from downloader import news_fulltext_large_replay_owner as owner

    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = oversized_head(queue, small_limit)
        step(queue, small_limit)
        task = queue._large_metadata_job
        task['job'].process.wait(timeout=30)
        with queue.db:
            queue.db.execute("UPDATE articles SET state='retry',error_code='new_refusal',due=123456 WHERE id=?",
                             (original['id'],))
        # Simulate a refusal landing between the candidate query and the
        # owner's snapshot, so equality alone cannot detect the transition.
        task['head'] = dict(queue.db.execute('SELECT * FROM articles WHERE id=?', (original['id'],)).fetchone())
        assert owner.poll_or_start(queue, queue.registry.sources['cnyes'], small_limit, start=False) == (0, 0)
        current = dict(queue.db.execute('SELECT * FROM articles WHERE id=?', (original['id'],)).fetchone())
        assert (current['state'], current['error_code'], current['due'], current['parquet_path']) == (
            'retry', 'new_refusal', 123456, original['parquet_path'])
        assert verify_bodies(tmp_path)['accepted']


def test_memory_failure_is_a_head_scoped_retry_and_grows_next_budget(cfg, tmp_path, small_limit, monkeypatch):
    from downloader import news_fulltext_large_replay_owner as owner
    real_job = owner.LargeReplayJob

    class ExhaustedJob:
        def __init__(self, request, **kwargs):
            self.request = request

        def poll(self):
            return {'version': 1, 'state': 'resource_wait', 'error': 'worker_memory_exhausted'}

        def close(self):
            pass

    monkeypatch.setattr(owner, 'LargeReplayJob', ExhaustedJob)
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = oversized_head(queue, small_limit)
        step(queue, small_limit)
        task = queue._large_metadata_job
        # Its first poll may happen on this call or on the next one.
        if task is not None:
            step(queue, small_limit)
        retry = queue.db.execute('SELECT * FROM retained_metadata_large_retries').fetchone()
        assert retry['article_id'] == original['id']
        assert retry['last_error'] == 'worker_memory_exhausted'
        assert retry['extra_address_bytes'] >= owner.INITIAL_ADDRESS_BYTES * 2
        assert queue.db.execute('SELECT state FROM retained_metadata_replays').fetchone()[0] == 'resource_wait'
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original
        monkeypatch.setattr(owner, 'LargeReplayJob', real_job)
        with queue.db:
            queue.db.execute('UPDATE retained_metadata_large_retries SET not_before_epoch=0')
        wait_for(queue, small_limit, lambda: queue.db.execute(
            "SELECT state FROM retained_metadata_replays").fetchone()[0] == 'verified_repair')
        assert queue.db.execute('SELECT last_error FROM retained_metadata_large_retries').fetchone() is None
        assert queue.db.execute('SELECT error FROM retained_metadata_large_retry_history').fetchone()[0] == 'worker_memory_exhausted'
        assert verify_bodies(tmp_path)['accepted']


def test_slow_large_reader_does_not_block_a_small_peer(cfg, tmp_path, small_limit, monkeypatch):
    from downloader import news_fulltext_large_replay_owner as owner
    from test_news_retained_metadata_checkpoint_batch import native_heads

    class WaitingJob:
        def __init__(self, request, **kwargs):
            self.closed = False

        def poll(self):
            return None

        def close(self):
            self.closed = True

    monkeypatch.setattr(owner, 'LargeReplayJob', WaitingJob)
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        large = oversized_head(queue, small_limit)
        step(queue, small_limit)
        job = queue._large_metadata_job['job']
        native_heads(queue, 1, repair_count=1)
        result = step(queue, small_limit)
        assert result['repaired_versions'] == 1 and result['http_requests'] == 0
        assert queue._large_metadata_job['job'] is job and not job.closed
        assert queue.db.execute('SELECT parquet_path FROM articles WHERE id=?', (large['id'],)).fetchone()[0] == large['parquet_path']
        assert verify_bodies(tmp_path)['accepted']
    assert job.closed


def test_failed_head_does_not_block_other_head_in_same_large_batch(cfg, tmp_path, small_limit, monkeypatch):
    from downloader import news_fulltext_large_replay_owner as owner
    import test_news_retained_metadata_checkpoint_batch as fixtures

    # This test targets the established single-head retry contract; batch
    # failure and fallback are covered separately below.
    monkeypatch.setattr(owner, 'MAX_BATCH_HEADS', 1)
    original_page = fixtures.mobile_page
    padding = base64.b64encode(os.urandom(80 << 10))
    monkeypatch.setattr(fixtures, 'mobile_page', lambda: original_page() + b'<!--' + padding + b'-->')
    real_job = owner.LargeReplayJob
    calls = []

    class ExhaustedJob:
        def poll(self):
            return {'version': 1, 'state': 'resource_wait', 'error': 'worker_memory_exhausted'}

        def close(self):
            pass

    def first_fails(request, **kwargs):
        calls.append(request['article_id'])
        return ExhaustedJob() if len(calls) == 1 else real_job(request, **kwargs)

    monkeypatch.setattr(owner, 'LargeReplayJob', first_fails)
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        heads = fixtures.native_heads(queue, 2, repair_count=2)
        assert len({head['parquet_path'] for head in heads.values()}) == 1
        assert (tmp_path / next(iter(heads.values()))['parquet_path']).stat().st_size > small_limit
        step(queue, small_limit)
        first = queue._large_metadata_job['article_id']
        step(queue, small_limit)
        second = queue._large_metadata_job['article_id']
        assert first != second and calls == [first.hex(), second.hex()]
        with queue.db:
            queue.db.execute('UPDATE retained_metadata_large_retries SET not_before_epoch=? WHERE article_id=?',
                             (time.time() + 3600, first))
        wait_for(queue, small_limit, lambda: queue.db.execute(
            'SELECT state FROM retained_metadata_replays WHERE article_id=?', (second,)).fetchone()[0] == 'verified_repair')
        assert queue.db.execute('SELECT state FROM retained_metadata_replays WHERE article_id=?',
                                (first,)).fetchone()[0] == 'resource_wait'
        assert verify_bodies(tmp_path)['accepted']


def test_unknown_progress_retry_preserves_peer_and_increases_timeout_after_restart(cfg, tmp_path, small_limit, monkeypatch):
    from downloader import news_fulltext_large_replay_owner as owner
    import test_news_retained_metadata_checkpoint_batch as fixtures

    monkeypatch.setattr(owner, 'MAX_BATCH_HEADS', 1)
    original_page = fixtures.mobile_page
    padding = base64.b64encode(os.urandom(80 << 10))
    monkeypatch.setattr(fixtures, 'mobile_page', lambda: original_page() + b'<!--' + padding + b'-->')
    created = []

    class UnknownJob:
        def __init__(self, request, *, unknown_stall_seconds):
            created.append((request['article_id'], unknown_stall_seconds))

        def poll(self):
            return {'version': 1, 'state': 'local_io_retry', 'error': 'worker_progress_unknown_timeout'}

        def close(self):
            pass

    monkeypatch.setattr(owner, 'LargeReplayJob', UnknownJob)
    registry = PolicyRegistry(cfg, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        heads = fixtures.native_heads(queue, 2, repair_count=2)
        source_path = tmp_path / next(iter(heads.values()))['parquet_path']
        before_sha = sha256_file(source_path)
        step(queue, small_limit)
        first = queue._large_metadata_job['article_id']
        step(queue, small_limit)
        second = queue._large_metadata_job['article_id']
        assert first != second
        assert created == [(first.hex(), 900), (second.hex(), 900)]
        retry = queue.db.execute('SELECT article_id,attempts,last_error FROM retained_metadata_large_retries').fetchone()
        assert tuple(retry) == (first, 1, 'worker_progress_unknown_timeout')
        assert {row['id']: dict(row) for row in queue.db.execute('SELECT * FROM articles')} == heads
        with queue.db:
            queue.db.execute('UPDATE retained_metadata_large_retries SET not_before_epoch=0 WHERE article_id=?', (first,))
        assert sha256_file(source_path) == before_sha
        assert verify_bodies(tmp_path)['accepted']

    with closing(NewsQueue(tmp_path, registry)) as queue:
        step(queue, small_limit)
        assert queue._large_metadata_job['article_id'] == first
        assert created[-1] == (first.hex(), 1800)
        assert sha256_file(source_path) == before_sha
        assert verify_bodies(tmp_path)['accepted']


def test_stalled_reader_is_terminated_then_killed_and_reported_as_retry(monkeypatch):
    from downloader import news_fulltext_large_replay as manager

    class FakeProcess:
        pid = 42
        def __init__(self):
            self.status = None
            self.terminated = self.killed = False

        def poll(self):
            return self.status

        def terminate(self):
            self.terminated = True

        def kill(self):
            self.killed = True
            self.status = -9

    clock = [0.0]
    job = object.__new__(manager.LargeReplayJob)
    job.clock = lambda: clock[0]
    job.last_progress = 0
    job.last_observation = None
    job.unknown_since = None
    job.unknown_stall_seconds = manager.UNKNOWN_PROGRESS_INITIAL_SECONDS
    job.termination_at = None
    job.termination_error = None
    job.process = FakeProcess()
    job.request = {'max_output_bytes': 1024}
    import tempfile
    job.output, job.error = tempfile.TemporaryFile(), tempfile.TemporaryFile()
    monkeypatch.setattr(manager, '_progress', lambda pid: (1, 1, 1, 1))
    assert job.poll() is None
    clock[0] = manager.STALL_SECONDS + 1
    assert job.poll() is None and job.process.terminated
    clock[0] += manager.TERMINATE_GRACE_SECONDS + 1
    assert job.poll() is None and job.process.killed
    assert job.poll() == {'version': 1, 'state': 'local_io_retry', 'error': 'worker_progress_unchanged_timeout'}


@pytest.mark.parametrize('message,expected', [
    (b"terminate called after throwing 'std::bad_alloc'", ('resource_wait', 'worker_memory_exhausted')),
    (b'unrelated native abort', ('local_io_retry', 'worker_process_failed')),
])
def test_only_known_allocation_abort_increases_memory_budget(message, expected):
    import signal
    import tempfile
    from downloader import news_fulltext_large_replay as manager

    class ExitedProcess:
        def poll(self):
            return -signal.SIGABRT

    job = object.__new__(manager.LargeReplayJob)
    job.process = ExitedProcess()
    job.termination_at = None
    job.request = {'max_output_bytes': 1024}
    job.output, job.error = tempfile.TemporaryFile(), tempfile.TemporaryFile()
    job.error.write(message)
    result = job.poll()
    assert (result['state'], result['error']) == expected


def test_large_retry_snapshot_is_source_scoped_and_never_exposes_paths(cfg, tmp_path, small_limit):
    from downloader.news_fulltext_metadata_replay import _ensure_cohort
    from downloader.news_fulltext_status import cnyes_metadata_large_replay

    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        source = queue.registry.sources['cnyes']
        assert cnyes_metadata_large_replay(queue.db, source, None) is None
        original = oversized_head(queue, small_limit)
        _ensure_cohort(queue, source)
        with queue.db:
            for fp in (source.data_fingerprint, 'old-source'):
                queue.db.execute('INSERT INTO retained_metadata_large_retries VALUES (?,?,?,?,?,?,?,?,?)',
                    (fp, original['id'], 'PRIVATE_PATH', 1, 1_800_000_000, 1024, 1024, 'PRIVATE_ERROR', 'now'))
        before = queue.db.total_changes
        view = cnyes_metadata_large_replay(queue.db, source, {'fingerprint': source.data_fingerprint})
        assert view['retry_records'] == 1 and view['assigned'] is True
        assert view['retry_records_are_article_completion'] is False
        assert 'PRIVATE' not in str(view) and queue.db.total_changes == before
        assert queue.status(publish=False)['cnyes_metadata_large_replay']['retry_records'] == 1


def test_changed_receipt_is_not_accepted_and_is_retried_locally(cfg, tmp_path, small_limit):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = oversized_head(queue, small_limit)
        step(queue, small_limit)
        job = queue._large_metadata_job['job']
        job.process.wait(timeout=30)
        receipt = tmp_path / queue._large_metadata_job['receipt_path']
        before = receipt.read_bytes()
        receipt.write_bytes(before + b' ')
        step(queue, small_limit)
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original
        retry = queue.db.execute('SELECT last_error FROM retained_metadata_large_retries').fetchone()
        assert retry[0] == 'worker_proof_changed_before_acceptance'
        receipt.write_bytes(before)
        with queue.db:
            queue.db.execute('UPDATE retained_metadata_large_retries SET not_before_epoch=0')
        wait_for(queue, small_limit, lambda: queue.db.execute(
            "SELECT state FROM retained_metadata_replays").fetchone()[0] == 'verified_repair')
        assert verify_bodies(tmp_path)['accepted']
