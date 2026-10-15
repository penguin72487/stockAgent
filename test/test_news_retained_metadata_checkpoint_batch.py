"""Local checkpoint batching must not batch or weaken immutable body acceptance."""
from contextlib import closing
import json
import sqlite3
import time
from types import SimpleNamespace

import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_metadata_replay import _ensure_cohort, replay_retained_cnyes_metadata
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_cnyes_news_bulk import cfg
from test_news_cnyes_canonical_alias import mobile_page


def native_heads(queue, count, *, repair_count=0, other_count=0):
    records = []
    for number in range(count):
        article_id = str(100000 + number)
        url = 'https://m.cnyes.com/news/id/' + article_id
        queue.add_articles([{'url': url}])
        head = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        payload = mobile_page().replace(b'42', article_id.encode())
        http = SimpleNamespace(registry=queue.registry,
                               get=lambda *_a, url=url, payload=payload, **_k: Response(
                                   url, 200, {'content-type': 'text/html'}, payload))
        record = fetch_article(http, head)
        record['captured_at_utc'] = '2026-10-01T00:00:00+00:00'
        record['data_fingerprint'] = 'retained-test-original'
        if number < repair_count:
            record.update(authors_json='[]', published_at_raw=None, body_completeness='article_container_unverified')
        if number >= count - other_count:
            record['final_url'] = 'https://api.cnyes.com/media/api/v1/newslist/category/headline'
        records.append(record)
    queue.accept(records)
    _ensure_cohort(queue, queue.registry.sources['cnyes'])
    return {row['id']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}


def checkpoint_states(queue):
    return [tuple(row) for row in queue.db.execute(
        'SELECT state,error_code FROM retained_metadata_replays ORDER BY article_id')]


def observed_replay(queue, **kwargs):
    trace = []
    queue.db.set_trace_callback(trace.append)
    try:
        result = replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1, **kwargs)
    finally:
        queue.db.set_trace_callback(None)
    return result, sum(statement == 'COMMIT' for statement in trace)


@pytest.mark.parametrize('count', [1, 8, 32, 64])
def test_one_checkpoint_transaction_per_complete_source_batch(cfg, tmp_path, count):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = native_heads(queue, count)
        paths = {head['parquet_path']: sha256_file(tmp_path / head['parquet_path']) for head in original.values()}
        result, commits = observed_replay(queue, maximum_heads=count)
        assert result['heads_checked'] == count and result['repaired_versions'] == result['http_requests'] == 0
        assert commits == 1
        assert checkpoint_states(queue) == [('current_metadata_complete', None)] * count
        assert {row['id']: dict(row) for row in queue.db.execute('SELECT * FROM articles')} == original
        assert all(sha256_file(tmp_path / path) == digest for path, digest in paths.items())
        metrics = queue.metrics.snapshot()['providers']['cnyes']['counts']
        assert metrics['retained_metadata_checkpoint_rows'] == count
        assert metrics['retained_metadata_checkpoint_transactions'] == 1


def test_mixed_nonmobile_and_complete_classifications_share_one_guarded_batch(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = native_heads(queue, 8, other_count=3)
        result, commits = observed_replay(queue)
        assert commits == 1 and result['heads_checked'] == 8
        states = checkpoint_states(queue)
        assert states.count(('not_mobile_html_candidate', None)) == 3
        assert states.count(('current_metadata_complete', None)) == 5
        assert {row['id']: dict(row) for row in queue.db.execute('SELECT * FROM articles')} == original


def test_bad_batch_receipt_checkpoints_failure_together_without_touching_heads(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = native_heads(queue, 8)
        receipt = queue.db.execute('SELECT receipt_path FROM accepted_batches').fetchone()[0]
        data = json.loads((tmp_path / receipt).read_bytes())
        data['sha256'] = 'wrong'
        atomic_write_json(tmp_path / receipt, data)
        result, commits = observed_replay(queue)
        assert commits == 1 and result['repaired_versions'] == 0
        assert checkpoint_states(queue) == [('failed_validation', 'retained_metadata_batch_receipt_invalid')] * 8
        assert {row['id']: dict(row) for row in queue.db.execute('SELECT * FROM articles')} == original


def test_deferred_active_work_is_checkpointed_once_never_repaired_over(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        native_heads(queue, 8, repair_count=8)
        with queue.db:
            queue.db.execute("UPDATE articles SET state='inflight',lease_owner='original-owner',lease_until=?,due=?,attempts=3,error_code='original'",
                             (time.time() + 1000, time.time() + 900))
        original = {row['id']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
        result, commits = observed_replay(queue)
        assert commits == 1 and result['repaired_versions'] == result['heads_checked'] == 0
        assert checkpoint_states(queue) == [('deferred', None)] * 8
        assert {row['id']: dict(row) for row in queue.db.execute('SELECT * FROM articles')} == original


def test_superseded_heads_are_checkpointed_once_not_reparsed(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        native_heads(queue, 8)
        with queue.db:
            queue.db.execute("UPDATE retained_metadata_replays SET original_body_sha256='previous-head'")
        result, commits = observed_replay(queue)
        assert commits == 1 and result['heads_checked'] == result['repaired_versions'] == 0
        assert checkpoint_states(queue) == [('superseded', None)] * 8


def test_acceptance_failure_keeps_only_unaccepted_repairs_pending(cfg, tmp_path, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = native_heads(queue, 8, repair_count=2)
        def fail(_records):
            raise OSError('injected acceptance failure')
        monkeypatch.setattr(queue, 'accept', fail)
        trace = []
        queue.db.set_trace_callback(trace.append)
        with pytest.raises(OSError, match='injected acceptance failure'):
            replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)
        queue.db.set_trace_callback(None)
        assert sum(statement == 'COMMIT' for statement in trace) == 1
        states = checkpoint_states(queue)
        assert states.count(('pending', None)) == 2
        assert states.count(('current_metadata_complete', None)) == 6
        assert {row['id']: dict(row) for row in queue.db.execute('SELECT * FROM articles')} == original


def test_verified_checkpoint_follows_independent_immutable_acceptance(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = native_heads(queue, 4, repair_count=4)
        result, commits = observed_replay(queue)
        assert result['repaired_versions'] == result['heads_checked'] == 4
        assert checkpoint_states(queue) == [('verified_repair', None)] * 4
        counts = queue.metrics.snapshot()['providers']['cnyes']['counts']
        assert counts['retained_metadata_checkpoint_transactions'] == 1
        assert counts['retained_metadata_checkpoint_rows'] == 4
        assert commits > 1  # Body prepare/receipt/commit retains its own durable boundaries.
        for head in queue.db.execute('SELECT * FROM articles'):
            for key in ('captured_at_utc', 'last_checked_at_utc', 'attempts', 'due', 'error_code'):
                assert head[key] == original[head['id']][key]
        assert verify_bodies(tmp_path)['accepted']


def test_failed_checkpoint_transaction_rolls_back_every_local_outcome(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = native_heads(queue, 4)
        failing_id = sorted(original)[1].hex()
        queue.db.execute("CREATE TRIGGER injected_checkpoint_failure BEFORE UPDATE ON retained_metadata_replays "
                         f"WHEN NEW.state='current_metadata_complete' AND hex(NEW.article_id)='{failing_id.upper()}' "
                         "BEGIN SELECT RAISE(ABORT,'injected checkpoint failure'); END")
        with pytest.raises(sqlite3.IntegrityError, match='injected checkpoint failure'):
            replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)
        assert checkpoint_states(queue) == [('pending', None)] * 4
        assert {row['id']: dict(row) for row in queue.db.execute('SELECT * FROM articles')} == original
        queue.db.execute('DROP TRIGGER injected_checkpoint_failure')
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['heads_checked'] == 4
        assert checkpoint_states(queue) == [('current_metadata_complete', None)] * 4


def test_empty_cohort_adds_no_fake_checkpoint_or_body_counters(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        _ensure_cohort(queue, queue.registry.sources['cnyes'])
        result, commits = observed_replay(queue)
        assert result['heads_checked'] == result['repaired_versions'] == result['http_requests'] == commits == 0
        counts = queue.metrics.snapshot()['providers']['cnyes']['counts']
        assert counts.get('retained_metadata_checkpoint_rows', 0) == counts.get('retained_metadata_checkpoint_transactions', 0) == 0
