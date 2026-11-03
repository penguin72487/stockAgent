"""Offline correction preserves source evidence and HTTP scheduling semantics."""
from contextlib import closing
import errno
import hashlib
import json
import sqlite3
import time
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_metadata_replay import replay_retained_cnyes_metadata
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_cnyes_news_bulk import cfg
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE, mobile_page
from test_news_cnyes_html import CLOCK
from test_news_cnyes_captions import caption_page
from test_news_cnyes_typed_chart_references import OUTER_LABEL


CAPTURED = '2026-10-01T00:00:00+00:00'


def test_outer_chart_prior_body_failure_replays_to_durable_head_without_http(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        payload = caption_page(current_figure=OUTER_LABEL, visible_figure=OUTER_LABEL,
                               schema_caption='當篇公司 K 線圖')
        original = old_head(queue, payload=payload)
        source = queue.registry.sources['cnyes']
        with queue.db:
            queue.db.execute('INSERT INTO retained_metadata_replays VALUES (?,?,?,?,?,?,?)',
                (source.parser_repair_prior_capture_fingerprint, original['id'], original['parquet_path'],
                 original['body_sha256'], 'failed_validation', 'body_not_found', CAPTURED))
        old_digest = sha256_file(tmp_path / original['parquet_path'])
        result = replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)
        assert result['http_requests'] == 0
        assert result['heads_checked'] == result['repaired_versions'] == 1
        head, record = read_head(queue)
        assert head['parquet_path'] != original['parquet_path']
        assert sha256_file(tmp_path / original['parquet_path']) == old_digest
        for key in ('captured_at_utc', 'last_checked_at_utc', 'attempts', 'due', 'error_code'):
            assert head[key] == original[key]
        assert record['source_observed_at_utc'] == CAPTURED
        assert record['published_at_raw'] == CLOCK
        assert json.loads(record['authors_json']) == [AUTHOR]
        assert json.loads(record['verified_image_captions_json'])[0]['text'] == '當篇公司 K 線圖'
        assert record['replay_preserves_last_http_check'] is True
        assert queue.db.execute('SELECT state FROM retained_metadata_replays WHERE source_fingerprint=?',
                                (source.data_fingerprint,)).fetchone()[0] == 'verified_repair'
        assert verify_bodies(tmp_path)['accepted']


def old_head(queue, *, url=MOBILE, payload=None, patch=None):
    queue.add_articles([{'url': url}])
    head = dict(queue.db.execute('SELECT * FROM articles').fetchone())
    raw = payload if payload is not None else mobile_page()
    http = SimpleNamespace(registry=queue.registry,
                           get=lambda *_a, **_k: Response(url, 200, {'content-type': 'text/html'}, raw))
    record = fetch_article(http, head)
    record.update(captured_at_utc=CAPTURED, data_fingerprint='old-capture', published_at_raw=None,
                  authors_json='[]', body_completeness='article_container_unverified',
                  extraction_method='css:#article-container')
    record['body'] += '\u200c\u200c'  # The old extraction accidentally included a loader.
    record['body_sha256'] = hashlib.sha256(record['body'].encode()).hexdigest()
    record['body_bytes'] = len(record['body'].encode())
    record.update(patch or {})
    queue.accept([record])
    return dict(queue.db.execute('SELECT * FROM articles').fetchone())


def read_head(queue):
    head = queue.db.execute('SELECT * FROM articles').fetchone()
    return head, pq.read_table(queue.root / head['parquet_path']).to_pylist()[0]


@pytest.mark.parametrize('dictionary', [False, True])
@pytest.mark.parametrize('row_group_size', [2, 16])
def test_selected_batch_records_preserve_shared_and_duplicate_envelopes(tmp_path, dictionary, row_group_size):
    import pyarrow as pa
    from downloader.news_fulltext_metadata_replay import _selected_batch_records

    first, second, envelope, other = (f'{number:064x}' for number in range(1, 5))
    rows = [
        {'article_id': envelope, 'source_response_article_id': envelope, 'raw_response_body': b'earlier', 'body': 'source'},
        {'article_id': first, 'source_response_article_id': envelope, 'raw_response_body': None, 'body': 'first'},
        {'article_id': other, 'source_response_article_id': other, 'raw_response_body': b'unneeded', 'body': 'other'},
        {'article_id': second, 'source_response_article_id': envelope, 'raw_response_body': None, 'body': 'second'},
        {'article_id': envelope, 'source_response_article_id': envelope, 'raw_response_body': b'last', 'body': 'source'},
        {'article_id': envelope, 'source_response_article_id': envelope, 'raw_response_body': None, 'body': 'source'},
    ]
    path = tmp_path / 'shared-envelope.parquet'
    pq.write_table(pa.Table.from_pylist(rows), path, use_dictionary=dictionary, row_group_size=row_group_size)
    batch = pq.ParquetFile(path)
    metadata = batch.read(columns=['article_id', 'source_response_article_id']).to_pylist()
    selected = [{'id': bytes.fromhex(first)}, {'id': bytes.fromhex(second)}]
    records, envelopes = _selected_batch_records(path, batch, metadata, selected)
    original = batch.read().to_pylist()
    original_records = {row['article_id']: row for row in original}
    original_envelopes = {row['article_id']: row['raw_response_body']
                          for row in original if row['raw_response_body'] is not None}
    assert records == {key: original_records[key] for key in (first, second)}
    assert envelopes == {envelope: original_envelopes[envelope]} == {envelope: b'last'}
    assert other not in records and other not in envelopes
    one, one_envelopes = _selected_batch_records(path, batch, metadata, selected[:1])
    assert one == {first: original_records[first]}
    assert one_envelopes == {envelope: b'last'}


def test_selected_batch_rejects_changed_article_positions(tmp_path):
    import pyarrow as pa
    from downloader.news_fulltext_metadata_replay import _selected_batch_records

    first, second = (f'{number:064x}' for number in (1, 2))
    path = tmp_path / 'changed-positions.parquet'
    pq.write_table(pa.Table.from_pylist([
        {'article_id': first, 'source_response_article_id': first, 'raw_response_body': b'first'},
        {'article_id': second, 'source_response_article_id': second, 'raw_response_body': b'second'},
    ]), path)
    batch = pq.ParquetFile(path)
    metadata = list(reversed(batch.read(columns=['article_id', 'source_response_article_id']).to_pylist()))
    with pytest.raises(ValueError, match='retained_metadata_batch_head_not_unique'):
        _selected_batch_records(path, batch, metadata, [{'id': bytes.fromhex(first)}])


def test_slow_batch_read_finishes_one_head_then_continues_next_slice(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext_metadata_replay as metadata
    from test_news_retained_metadata_checkpoint_batch import native_heads

    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        native_heads(queue, 2, repair_count=2)
        clock = [0.0]
        monkeypatch.setattr(metadata, 'time', SimpleNamespace(monotonic=lambda: clock[0], time=time.time))
        original_read = metadata._selected_batch_records

        def slow_read(*args):
            result = original_read(*args)
            clock[0] += 1.0
            return result

        monkeypatch.setattr(metadata, '_selected_batch_records', slow_read)
        for _ in range(2):
            clock[0] = 0.0
            result = replay_retained_cnyes_metadata(queue, maximum_wall_seconds=0.5)
            assert result['heads_checked'] == result['repaired_versions'] == 1
            assert result['http_requests'] == 0
        assert queue.db.execute("SELECT count(*) FROM retained_metadata_replays WHERE state='verified_repair'").fetchone()[0] == 2
        assert verify_bodies(tmp_path)['accepted']


def test_stop_during_batch_read_prevents_one_head_liveness_override(cfg, tmp_path, monkeypatch):
    import threading
    from downloader import news_fulltext_metadata_replay as metadata

    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old = old_head(queue)
        stop = threading.Event()
        original_read = metadata._selected_batch_records

        def stop_after_read(*args):
            result = original_read(*args)
            stop.set()
            return result

        with monkeypatch.context() as patch:
            patch.setattr(metadata, '_selected_batch_records', stop_after_read)
            result = replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1, stop=stop)
        assert result['heads_checked'] == result['repaired_versions'] == result['http_requests'] == 0
        assert queue.db.execute('SELECT state FROM retained_metadata_replays').fetchone()[0] == 'pending'
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == old
        stop.clear()
        assert replay_until_checked(queue)['repaired_versions'] == 1
        assert verify_bodies(tmp_path)['accepted']


def replay_until_checked(queue):
    # A bounded slice can expire before parsing under load. The persistent
    # owner retries it; test the eventual outcome without assuming one slice.
    for _ in range(8):
        result = replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)
        assert result['http_requests'] == 0
        if result['heads_checked']:
            return result
    return result


def test_repair_uses_no_http_and_preserves_old_receipts_clocks_and_attempts(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old = old_head(queue)
        with queue.db:
            queue.db.execute('UPDATE articles SET attempts=3,due=?,last_checked_at_utc=?', (12345, '2026-10-02T00:00:00+00:00'))
        old = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        original_digest = sha256_file(tmp_path / old['parquet_path'])
        attempts = queue.db.execute("SELECT value FROM counters WHERE key='article_attempts_finished'").fetchone()[0]
        result = replay_until_checked(queue)
        assert result['repaired_versions'] == 1 and result['http_requests'] == 0
        head, record = read_head(queue)
        assert record['published_at_raw'] == CLOCK and json.loads(record['authors_json']) == [AUTHOR]
        assert head['body_sha256'] != old['body_sha256']
        assert record['acquisition_transport'] == 'retained_raw_reparse'
        assert record['source_observed_at_utc'] == record['captured_at_utc'] == CAPTURED
        assert record['original_capture_data_fingerprint'] == 'old-capture'
        assert record['reparsed_at_utc'] > CAPTURED
        for key in ('last_checked_at_utc', 'captured_at_utc', 'attempts', 'due', 'etag', 'last_modified', 'error_code'):
            assert head[key] == old[key]
        assert sha256_file(tmp_path / old['parquet_path']) == original_digest
        assert queue.db.execute("SELECT value FROM counters WHERE key='article_attempts_finished'").fetchone()[0] == attempts
        assert verify_bodies(tmp_path)['accepted']
        assert replay_retained_cnyes_metadata(queue)['heads_checked'] == 0
        metrics = queue.metrics.snapshot()['providers']['cnyes']['counts']
        assert metrics['retained_metadata_repaired_versions'] == 1
        assert metrics['durably_accepted_cached_versions'] == 1
        assert metrics.get('durably_accepted_cached_new_url_heads', 0) == 0
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        assert replay_retained_cnyes_metadata(queue)['repaired_versions'] == 0
        assert verify_bodies(tmp_path)['body_versions_verified'] == 2


def test_idle_owner_runs_retained_metadata_without_publisher_requests(cfg, tmp_path):
    from downloader.news_fulltext import run_cycle
    from test_news_fulltext import http_fixture

    cfg.update(cached_cnyes_metadata_replay_enabled=True, cached_cnyes_replay_enabled=False,
               retained_cnyes_failure_replay_enabled=False, adaptive_rate={'enabled': False})
    http, session = http_fixture(cfg, tmp_path, {})
    with closing(NewsQueue(tmp_path, http.registry)) as queue:
        original = old_head(queue)
        try:
            run_cycle(queue, http, cfg, max_articles=0, max_discovery_requests=0,
                      max_run_seconds=10, continuous_budgets=True)
            head, record = read_head(queue)
            assert record['published_at_raw'] == CLOCK and json.loads(record['authors_json']) == [AUTHOR]
            assert head['last_checked_at_utc'] == original['last_checked_at_utc']
            assert not session.calls and verify_bodies(tmp_path)['accepted']
        finally:
            http.close()


@pytest.mark.parametrize('which', ['data', 'receipt'])
def test_missing_local_source_waits_and_recovers_after_restart(cfg, tmp_path, monkeypatch, which):
    registry = PolicyRegistry(cfg, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    clock = [time.time()]
    monkeypatch.setattr(time, 'time', lambda: clock[0])
    try:
        old = old_head(queue)
        relative = old['parquet_path'] if which == 'data' else queue.db.execute(
            'SELECT receipt_path FROM accepted_batches WHERE path=?', (old['parquet_path'],)).fetchone()[0]
        data_path = tmp_path / relative
        backup = data_path.with_name(data_path.name + '.fixture-backup')
        digest = sha256_file(data_path)
        data_path.rename(backup)  # Synthetic fixture only; keep its original bytes.
        try:
            result = replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)
            assert result['local_io_deferrals'] == 1 and result['http_requests'] == 0
            assert queue.db.execute('SELECT state FROM retained_metadata_replays').fetchone()[0] == 'pending'
            assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == old
        finally:
            backup.rename(data_path)
        assert sha256_file(data_path) == digest
    finally:
        queue.close()
    with closing(NewsQueue(tmp_path, registry)) as restarted:
        assert replay_retained_cnyes_metadata(restarted, maximum_wall_seconds=1)['heads_checked'] == 0
        clock[0] += 2
        result = replay_retained_cnyes_metadata(restarted, maximum_wall_seconds=1)
        assert result['repaired_versions'] == 1 and result['http_requests'] == 0
        assert restarted.db.execute('SELECT count(*) FROM retained_metadata_local_retries').fetchone()[0] == 0
        assert sha256_file(data_path) == digest and verify_bodies(tmp_path)['accepted']


def test_metadata_local_outage_backs_off_without_hot_loop(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext_metadata_replay as metadata

    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old = old_head(queue)
        clock = [time.time()]
        monkeypatch.setattr(time, 'time', lambda: clock[0])

        def unavailable(*_args):
            raise OSError('synthetic persistent local outage')

        monkeypatch.setattr(metadata, 'replay_source_path', unavailable)
        for attempt, delay in enumerate((1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300), 1):
            result = replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)
            assert result['local_io_deferrals'] == 1 and result['http_requests'] == 0
            row = queue.db.execute('SELECT local_attempts,not_before_epoch FROM retained_metadata_local_retries').fetchone()
            assert row[0] == attempt and row[1] == clock[0] + delay
            assert metadata.retained_metadata_next_wake_delay(queue, 500) == delay
            assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['heads_checked'] == 0
            clock[0] += delay
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == old
        assert queue.db.execute('SELECT state FROM retained_metadata_replays').fetchone()[0] == 'pending'


def test_unavailable_metadata_batch_does_not_starve_another_batch(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext_metadata_replay as metadata

    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old_head(queue)
        second_url = MOBILE.replace('/42', '/43')
        second_payload = mobile_page().replace(b'42', b'43')
        queue.add_articles([{'url': second_url}])
        second_head = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (second_url,)).fetchone())
        http = SimpleNamespace(registry=queue.registry,
            get=lambda *_a, **_k: Response(second_url, 200, {'content-type': 'text/html'}, second_payload))
        record = fetch_article(http, second_head)
        record.update(captured_at_utc=CAPTURED, data_fingerprint='old-capture', published_at_raw=None,
                      authors_json='[]', body_completeness='article_container_unverified',
                      extraction_method='css:#article-container')
        queue.accept([record])
        metadata._ensure_cohort(queue, queue.registry.sources['cnyes'])
        first = queue.db.execute('SELECT article_id,original_path FROM retained_metadata_replays '
                                 'ORDER BY original_path,article_id LIMIT 1').fetchone()
        blocked_id, blocked_path = first
        blocked_head = dict(queue.db.execute('SELECT * FROM articles WHERE id=?', (blocked_id,)).fetchone())
        now = time.time()
        monkeypatch.setattr(time, 'time', lambda: now)
        original_path = metadata.replay_source_path

        def fail_one_path(root, relative):
            if relative == blocked_path:
                raise OSError('synthetic per-batch outage')
            return original_path(root, relative)

        monkeypatch.setattr(metadata, 'replay_source_path', fail_one_path)
        first_result = replay_retained_cnyes_metadata(queue, maximum_heads=1, maximum_wall_seconds=1)
        assert first_result['local_io_deferrals'] == 1 and first_result['repaired_versions'] == 0
        second_result = replay_retained_cnyes_metadata(queue, maximum_heads=1, maximum_wall_seconds=1)
        assert second_result['repaired_versions'] == 1 and second_result['http_requests'] == 0
        assert dict(queue.db.execute('SELECT * FROM articles WHERE id=?', (blocked_id,)).fetchone()) == blocked_head
        assert queue.db.execute('SELECT state FROM retained_metadata_replays WHERE article_id=?', (blocked_id,)).fetchone()[0] == 'pending'


def test_replay_source_path_distinguishes_missing_and_unsafe_paths(tmp_path):
    from downloader.news_fulltext_queue import replay_source_path

    with pytest.raises(FileNotFoundError):
        replay_source_path(tmp_path, 'missing.parquet')
    with pytest.raises(ValueError, match='invalid source artifact path'):
        replay_source_path(tmp_path, '../outside.parquet')
    with pytest.raises(ValueError, match='invalid source artifact file'):
        replay_source_path(tmp_path, '.')


@pytest.mark.parametrize('relative', ['loop', 'loop/retained.parquet'])
def test_replay_source_path_rejects_symlink_cycles(tmp_path, relative):
    from downloader.news_fulltext_queue import replay_source_path

    (tmp_path / 'loop').symlink_to('loop')
    with pytest.raises(ValueError, match='invalid source artifact path'):
        replay_source_path(tmp_path, relative)


@pytest.mark.parametrize('resolver_name', ['source_path', 'replay_source_path'])
@pytest.mark.parametrize('error_number', [errno.ELOOP, errno.EACCES])
def test_source_path_preserves_other_io_errors(tmp_path, monkeypatch, resolver_name, error_number):
    from downloader import news_fulltext_queue

    path_type = type(tmp_path)
    original_resolve = path_type.resolve
    target = tmp_path / 'blocked'

    def resolve(self, *args, **kwargs):
        if self == target:
            raise OSError(error_number, 'synthetic resolution failure')
        return original_resolve(self, *args, **kwargs)

    monkeypatch.setattr(path_type, 'resolve', resolve)
    expected = ValueError if error_number == errno.ELOOP else PermissionError
    with pytest.raises(expected):
        getattr(news_fulltext_queue, resolver_name)(tmp_path, 'blocked')


def test_deferred_metadata_wakes_only_when_head_is_eligible(cfg, tmp_path):
    from downloader.news_fulltext_metadata_replay import _ensure_cohort, retained_metadata_next_wake_delay

    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old = old_head(queue)
        _ensure_cohort(queue, queue.registry.sources['cnyes'])
        with queue.db:
            queue.db.execute("UPDATE retained_metadata_replays SET state='deferred'")
            queue.db.execute("UPDATE articles SET state='paywall_or_login',error_code='original_refusal'")
        refused = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        assert retained_metadata_next_wake_delay(queue, 60) == 60
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 0
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == refused
        with queue.db:
            queue.db.execute("UPDATE articles SET state='complete',error_code=NULL")
        assert retained_metadata_next_wake_delay(queue, 60) == 10
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 1
        assert queue.db.execute('SELECT captured_at_utc FROM articles').fetchone()[0] == old['captured_at_utc']


def test_metadata_backlog_wakes_and_advances_in_bounded_idle_slices(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext_metadata_replay as metadata
    from downloader.news_fulltext import run_service
    from test_news_fulltext import http_fixture
    from test_news_retained_metadata_checkpoint_batch import native_heads

    cfg.update(cached_cnyes_metadata_replay_enabled=True, cached_cnyes_replay_enabled=False,
               retained_cnyes_failure_replay_enabled=False, adaptive_rate={'enabled': False},
               service_idle_max_seconds=30)
    monkeypatch.setattr(metadata, 'METADATA_REPLAY_POLL_SECONDS', .2)
    original_replay = metadata.replay_retained_cnyes_metadata

    def one_second_slice(queue, **kwargs):
        return original_replay(queue, maximum_wall_seconds=1, **kwargs)

    monkeypatch.setattr(metadata, 'replay_retained_cnyes_metadata', one_second_slice)
    http, session = http_fixture(cfg, tmp_path, {})
    with closing(NewsQueue(tmp_path, http.registry)) as queue:
        original = native_heads(queue, 34)
        try:
            http.defer('cnyes', 120)
            run_service(queue, http, cfg, max_articles=0, max_discovery_requests=0, duration_seconds=4)
            states = dict(queue.db.execute('SELECT state,count(*) FROM retained_metadata_replays GROUP BY state'))
            assert states == {'current_metadata_complete': 34}
            assert {row['id']: dict(row) for row in queue.db.execute('SELECT * FROM articles')} == original
            assert not session.calls and verify_bodies(tmp_path)['accepted']
        finally:
            http.close()


def test_idle_service_recovers_local_outage_while_publisher_is_cooling(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext_metadata_replay as metadata
    from downloader.news_fulltext import run_service
    from test_news_fulltext import http_fixture

    cfg.update(cached_cnyes_metadata_replay_enabled=True, cached_cnyes_replay_enabled=False,
               retained_cnyes_failure_replay_enabled=False, adaptive_rate={'enabled': False},
               service_idle_max_seconds=30)
    http, session = http_fixture(cfg, tmp_path, {})
    with closing(NewsQueue(tmp_path, http.registry)) as queue:
        old = old_head(queue)
        metadata._ensure_cohort(queue, queue.registry.sources['cnyes'])
        original_path = metadata.replay_source_path
        attempts = []

        def fail_once(root, relative):
            if relative == old['parquet_path']:
                attempts.append(time.monotonic())
                if len(attempts) == 1:
                    raise OSError('synthetic local outage')
            return original_path(root, relative)

        monkeypatch.setattr(metadata, 'replay_source_path', fail_once)
        try:
            http.defer('cnyes', 120)
            run_service(queue, http, cfg, max_articles=0, max_discovery_requests=0, duration_seconds=4)
            head, record = read_head(queue)
            assert len(attempts) == 2 and attempts[1] - attempts[0] >= 1
            assert record['published_at_raw'] == CLOCK and json.loads(record['authors_json']) == [AUTHOR]
            assert head['last_checked_at_utc'] == old['last_checked_at_utc']
            assert queue.db.execute('SELECT count(*) FROM retained_metadata_local_retries').fetchone()[0] == 0
            assert not session.calls and verify_bodies(tmp_path)['accepted']
        finally:
            http.close()


def test_available_publisher_dispatches_before_initial_idle_metadata(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext_metadata_replay as metadata
    from downloader.news_fulltext import run_cycle
    from test_news_cnyes_html import URL, short_page
    from test_news_fulltext import FakeResponse, http_fixture

    cfg.update(cached_cnyes_metadata_replay_enabled=True, cached_cnyes_replay_enabled=False,
               retained_cnyes_failure_replay_enabled=False, adaptive_rate={'enabled': False})
    http, session = http_fixture(cfg, tmp_path, {
        'https://news.cnyes.com/robots.txt': FakeResponse(b'User-agent: *\nAllow: /\n', headers={'Content-Type': 'text/plain'}),
        URL: FakeResponse(short_page()),
    })
    original_replay = metadata.replay_retained_cnyes_metadata
    observations = []

    def tracked_replay(queue, **kwargs):
        observations.append([url for url, _ in session.calls])
        return original_replay(queue, maximum_wall_seconds=1, **kwargs)

    monkeypatch.setattr(metadata, 'replay_retained_cnyes_metadata', tracked_replay)
    with closing(NewsQueue(tmp_path, http.registry)) as queue:
        old_head(queue)
        with queue.db:
            queue.add_articles([{'url': URL}])
        try:
            run_cycle(queue, http, cfg, max_articles=1, max_discovery_requests=0,
                      max_run_seconds=8, continuous_budgets=True)
            assert observations and URL in observations[0]
            assert queue.db.execute('SELECT state FROM articles WHERE url=?', (URL,)).fetchone()[0] == 'complete'
        finally:
            http.close()


@pytest.mark.parametrize('state', ['inflight', 'retry', 'retry_cooldown', 'paywall_or_login', 'source_not_found', 'awaiting_authorization'])
def test_no_repair_over_active_lease_refusal_or_retry(cfg, tmp_path, state):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old = old_head(queue)
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,due=?,error_code=?', (state, time.time() + 10000, 'original_outcome'))
        assert replay_retained_cnyes_metadata(queue)['repaired_versions'] == 0
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone())['state'] == state
        assert queue.db.execute('SELECT parquet_path FROM articles').fetchone()[0] == old['parquet_path']
        if state in {'retry', 'inflight', 'retry_cooldown'}:
            with queue.db:
                queue.db.execute("UPDATE articles SET state='complete',lease_owner=NULL,lease_until=NULL")
            assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 1


@pytest.mark.parametrize('patch,code', [
    ({'authorization_fingerprint': 'changed'}, 'retained_metadata_authorization_changed'),
    ({'authorization_json': '{}'}, 'retained_metadata_authorization_changed'),
    ({'published_at_raw': '2010-01-01T00:00:00Z'}, 'retained_metadata_publication_conflict'),
    ({'authors_json': '["conflicting source byline"]'}, 'retained_metadata_author_conflict'),
    ({'response_sha256': 'incorrect'}, 'retained_metadata_source_digest_mismatch'),
    ({'captured_at_utc': '2099-01-01T00:00:00+00:00'}, 'retained_metadata_observation_clock_invalid'),
    ({'captured_at_utc': '2026-01-01'}, 'retained_metadata_observation_clock_invalid'),
    ({'publish': True}, 'retained_metadata_unapproved_source_contract'),
    ({'remote_training_export': True}, 'retained_metadata_unapproved_source_contract'),
    ({'training_injection_enabled': True}, 'retained_metadata_unapproved_source_contract'),
    ({'commercial_use_enabled': True}, 'retained_metadata_unapproved_source_contract'),
    ({'fair_use_legally_determined': True}, 'retained_metadata_unapproved_source_contract'),
])
def test_unverified_or_conflicting_evidence_is_visible_never_silently_replaced(cfg, tmp_path, patch, code):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old = old_head(queue, patch=patch)
        result = replay_until_checked(queue)
        assert result['repaired_versions'] == 0
        state = queue.db.execute('SELECT state,error_code FROM retained_metadata_replays').fetchone()
        assert tuple(state) == ('failed_validation', code)
        assert queue.db.execute('SELECT parquet_path FROM articles').fetchone()[0] == old['parquet_path']


def test_receipt_corruption_does_not_change_source_head_or_abort_other_cohort(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old = old_head(queue)
        receipt = queue.db.execute('SELECT receipt_path FROM accepted_batches').fetchone()[0]
        data = json.loads((tmp_path / receipt).read_text())
        data['sha256'] = 'wrong'
        atomic_write_json(tmp_path / receipt, data)
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 0
        assert queue.db.execute('SELECT error_code FROM retained_metadata_replays').fetchone()[0] == 'retained_metadata_batch_receipt_invalid'
        assert queue.db.execute('SELECT parquet_path FROM articles').fetchone()[0] == old['parquet_path']


def test_acceptance_failure_is_not_checkpointed_and_restart_recovers_once(cfg, tmp_path, monkeypatch):
    registry = PolicyRegistry(cfg, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    old_head(queue)
    monkeypatch.setattr(queue, '_accept_records', lambda *_a: (_ for _ in ()).throw(OSError('commit interrupted')))
    try:
        with pytest.raises(OSError, match='commit interrupted'):
            replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)
        assert queue.db.execute('SELECT state FROM retained_metadata_replays').fetchone()[0] == 'pending'
    finally:
        queue.close()
    with closing(NewsQueue(tmp_path, registry)) as restarted:
        assert read_head(restarted)[1]['published_at_raw'] == CLOCK
        assert replay_retained_cnyes_metadata(restarted)['repaired_versions'] == 0
        assert verify_bodies(tmp_path)['body_versions_verified'] == 2


def test_durable_replay_receipt_cannot_overwrite_a_new_refusal_after_crash(cfg, tmp_path, monkeypatch):
    registry = PolicyRegistry(cfg, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    old = old_head(queue)
    monkeypatch.setattr(queue, '_accept_records', lambda *_a: (_ for _ in ()).throw(OSError('commit interrupted')))
    try:
        with pytest.raises(OSError):
            replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)
        with queue.db:
            queue.db.execute("UPDATE articles SET state='paywall_or_login',error_code='original_refusal'")
    finally:
        queue.close()
    with closing(NewsQueue(tmp_path, registry)) as restarted:
        head = restarted.db.execute('SELECT state,error_code,parquet_path FROM articles').fetchone()
        assert tuple(head) == ('paywall_or_login', 'original_refusal', old['parquet_path'])
        assert replay_retained_cnyes_metadata(restarted)['repaired_versions'] == 0


def test_changed_head_before_acceptance_is_rejected(cfg, tmp_path, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old = old_head(queue)
        accept = queue.accept
        def changed(records, **kwargs):
            with queue.db:
                queue.db.execute("UPDATE articles SET state='inflight',lease_owner='actual_http_owner',lease_until=?", (time.time() + 300,))
            return accept(records, **kwargs)
        monkeypatch.setattr(queue, 'accept', changed)
        with pytest.raises(ValueError, match='head_changed_before_acceptance'):
            replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)
        assert queue.db.execute('SELECT parquet_path FROM articles').fetchone()[0] == old['parquet_path']
        assert len(list((tmp_path / 'receipts').glob('*.json'))) == 1


def test_stop_and_small_materialization_bound_do_not_relabel_missing_metadata(cfg, tmp_path, monkeypatch):
    import threading
    from downloader import news_fulltext_metadata_replay as metadata

    registry = PolicyRegistry(cfg, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        old = old_head(queue)
        old_digest = sha256_file(tmp_path / old['parquet_path'])
        stop = threading.Event()
        stop.set()
        assert replay_retained_cnyes_metadata(queue, stop=stop)['repaired_versions'] == 0
        assert queue.db.execute('SELECT state FROM retained_metadata_replays').fetchone()[0] == 'pending'
        assert replay_retained_cnyes_metadata(queue, maximum_bytes=1, maximum_wall_seconds=1)['repaired_versions'] == 0
        assert tuple(queue.db.execute('SELECT state,error_code FROM retained_metadata_replays').fetchone()) == (
            'resource_wait', 'retained_metadata_batch_exceeds_resource_bound')
        wait = queue.db.execute('SELECT original_path,required_bytes,phase FROM retained_metadata_resource_waits').fetchone()
        assert tuple(wait) == (old['parquet_path'], (tmp_path / old['parquet_path']).stat().st_size, 'compressed_file')
        assert metadata.retained_metadata_next_wake_delay(queue, 60) == 10
        with monkeypatch.context() as isolated:
            isolated.setattr(metadata, 'replay_source_path', lambda *_: pytest.fail('over-budget path was read again'))
            assert replay_retained_cnyes_metadata(queue, maximum_bytes=1)['heads_checked'] == 0
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == old
    with closing(NewsQueue(tmp_path, registry)) as restarted:
        result = replay_retained_cnyes_metadata(restarted, maximum_wall_seconds=1)
        assert result['repaired_versions'] == 1 and result['http_requests'] == 0
        assert restarted.db.execute('SELECT count(*) FROM retained_metadata_resource_waits').fetchone()[0] == 0
        assert sha256_file(tmp_path / old['parquet_path']) == old_digest
        assert verify_bodies(tmp_path)['accepted']


def test_legacy_resource_failure_retries_without_resetting_other_failures(cfg, tmp_path):
    from downloader import news_fulltext_metadata_replay as metadata
    from test_news_retained_metadata_checkpoint_batch import native_heads

    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old = old_head(queue)
        native_heads(queue, 1)
        metadata._ensure_cohort(queue, queue.registry.sources['cnyes'])
        with queue.db:
            queue.db.execute("UPDATE retained_metadata_replays SET state='failed_validation',"
                             "error_code='retained_metadata_batch_exceeds_resource_bound' WHERE article_id=?", (old['id'],))
            queue.db.execute("UPDATE retained_metadata_replays SET state='failed_validation',"
                             "error_code='retained_metadata_batch_receipt_invalid' WHERE article_id<>?", (old['id'],))
        assert metadata.retained_metadata_next_wake_delay(queue, 60) == 10
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 1
        rows = {row['article_id']: (row['state'], row['error_code']) for row in queue.db.execute(
            'SELECT article_id,state,error_code FROM retained_metadata_replays')}
        assert rows.pop(old['id']) == ('verified_repair', None)
        assert list(rows.values()) == [('failed_validation', 'retained_metadata_batch_receipt_invalid')]
        assert verify_bodies(tmp_path)['accepted']


def test_uncompressed_budget_wait_keeps_other_metadata_outcomes(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext_metadata_replay as metadata
    import test_news_retained_metadata_checkpoint_batch as fixtures

    page = fixtures.mobile_page
    monkeypatch.setattr(fixtures, 'mobile_page', lambda: page() + b' ' * 100_000)
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = fixtures.native_heads(queue, 2, repair_count=1)
        paths = {head['parquet_path'] for head in original.values()}
        assert len(paths) == 1
        relative = paths.pop()
        batch = pq.ParquetFile(tmp_path / relative)
        uncompressed = sum(batch.metadata.row_group(g).column(c).total_uncompressed_size
                           for g in range(batch.metadata.num_row_groups)
                           for c in range(batch.metadata.num_columns))
        compressed = (tmp_path / relative).stat().st_size
        assert compressed < uncompressed < metadata.METADATA_REPLAY_MAX_BYTES
        result = replay_retained_cnyes_metadata(queue, maximum_bytes=compressed, maximum_wall_seconds=1)
        assert result['http_requests'] == result['repaired_versions'] == 0
        states = {row['state'] for row in queue.db.execute('SELECT state FROM retained_metadata_replays')}
        assert states == {'current_metadata_complete', 'resource_wait'}
        wait = queue.db.execute('SELECT required_bytes,phase FROM retained_metadata_resource_waits').fetchone()
        assert tuple(wait) == (uncompressed, 'arrow_uncompressed')
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 1
        assert {row['state'] for row in queue.db.execute('SELECT state FROM retained_metadata_replays')} == {
            'current_metadata_complete', 'verified_repair'}
        assert verify_bodies(tmp_path)['accepted']


def test_resource_wait_checkpoint_and_budget_observation_rollback_together(cfg, tmp_path):
    from downloader import news_fulltext_metadata_replay as metadata

    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old = old_head(queue)
        metadata._ensure_cohort(queue, queue.registry.sources['cnyes'])
        with queue.db:
            queue.db.execute('''CREATE TRIGGER inject_resource_wait_failure BEFORE INSERT
                ON retained_metadata_resource_waits BEGIN SELECT RAISE(ABORT, 'injected resource wait failure'); END''')
        with pytest.raises(sqlite3.IntegrityError, match='injected resource wait failure'):
            replay_retained_cnyes_metadata(queue, maximum_bytes=1, maximum_wall_seconds=1)
        assert queue.db.execute('SELECT state FROM retained_metadata_replays').fetchone()[0] == 'pending'
        assert queue.db.execute('SELECT count(*) FROM retained_metadata_resource_waits').fetchone()[0] == 0
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == old
        with queue.db:
            queue.db.execute('DROP TRIGGER inject_resource_wait_failure')
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 1
        assert verify_bodies(tmp_path)['accepted']


def test_resource_wait_owner_snapshot_is_source_scoped_and_read_only(cfg, tmp_path):
    from downloader import news_fulltext_metadata_replay as metadata
    from downloader.news_fulltext_status import cnyes_metadata_resource_waits

    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        source = queue.registry.sources['cnyes']
        assert cnyes_metadata_resource_waits(queue.db, None) is None
        assert cnyes_metadata_resource_waits(queue.db, source) is None
        metadata._ensure_cohort(queue, source)
        with queue.db:
            queue.db.executemany('INSERT INTO retained_metadata_resource_waits VALUES (?,?,?,?,?)', [
                (source.data_fingerprint, 'small-private-path', 512, 'compressed_file', CAPTURED),
                (source.data_fingerprint, 'large-private-path', metadata.METADATA_REPLAY_MAX_BYTES + 1,
                 'arrow_uncompressed', CAPTURED),
                ('old-source-fingerprint', 'old-private-path', metadata.METADATA_REPLAY_MAX_BYTES + 2,
                 'compressed_file', CAPTURED),
            ])
        changes = queue.db.total_changes
        expected = {'contract_version': 1, 'current_source_contract_only': True,
                    'records_are_article_counts': False, 'records': 2, 'above_current_bound_records': 1,
                    'current_bound_bytes': metadata.METADATA_REPLAY_MAX_BYTES,
                    'max_observed_required_bytes': metadata.METADATA_REPLAY_MAX_BYTES + 1}
        assert cnyes_metadata_resource_waits(queue.db, source) == expected
        assert queue.db.total_changes == changes
        assert queue.status(publish=False)['cnyes_metadata_resource_waits'] == expected
        assert 'private-path' not in json.dumps(expected)


def test_over_budget_batch_does_not_starve_other_heads_or_spin(cfg, tmp_path):
    from downloader import news_fulltext_metadata_replay as metadata
    from test_news_retained_metadata_checkpoint_batch import native_heads

    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old = old_head(queue)
        native_heads(queue, 2)
        source = queue.registry.sources['cnyes']
        blocked = queue.db.execute("SELECT id,parquet_path FROM articles WHERE url LIKE '%/100000'").fetchone()
        other = queue.db.execute("SELECT id,parquet_path FROM articles WHERE url LIKE '%/100001'").fetchone()
        assert blocked['parquet_path'] == other['parquet_path'] != old['parquet_path']
        with queue.db:
            queue.db.execute("UPDATE retained_metadata_replays SET state='resource_wait' WHERE article_id=?", (blocked['id'],))
            queue.db.execute('INSERT INTO retained_metadata_resource_waits VALUES (?,?,?,?,?)',
                (source.data_fingerprint, blocked['parquet_path'], metadata.METADATA_REPLAY_MAX_BYTES + 1,
                 'arrow_uncompressed', CAPTURED))
        assert metadata.retained_metadata_next_wake_delay(queue, 60) == 10
        result = replay_retained_cnyes_metadata(queue, maximum_heads=1, maximum_wall_seconds=1)
        assert result['repaired_versions'] == 1 and result['http_requests'] == 0
        assert queue.db.execute('SELECT state FROM retained_metadata_replays WHERE article_id=?', (other['id'],)).fetchone()[0] == 'pending'
        # The over-budget original is now assigned to an isolated reader. Its
        # in-flight poll wakes promptly without blocking the already-repaired
        # other batch, and its sibling becomes eligible after that proof.
        assert metadata.retained_metadata_next_wake_delay(queue, 60) == 1
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            replay_retained_cnyes_metadata(queue, maximum_heads=1, maximum_wall_seconds=1)
            states = {row['article_id']: row['state'] for row in queue.db.execute(
                'SELECT article_id,state FROM retained_metadata_replays')}
            if states[blocked['id']] == states[other['id']] == 'current_metadata_complete':
                break
            time.sleep(.05)
        assert states[blocked['id']] == states[other['id']] == 'current_metadata_complete'
        assert verify_bodies(tmp_path)['accepted']


@pytest.mark.parametrize('bound', [0, 65, True])
def test_unbounded_work_is_rejected(cfg, tmp_path, bound):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        with pytest.raises(ValueError, match='unbounded'):
            replay_retained_cnyes_metadata(queue, maximum_heads=bound)


@pytest.mark.parametrize('title', ['...', '…'])
def test_observed_loader_headline_is_repaired_without_discarding_original(cfg, tmp_path, title):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old_head(queue, patch={'title': title})
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 1
        record = read_head(queue)[1]
        assert record['title'] != title
        assert record['original_capture_title'] == title
        assert 'old_loader_title_preserved' in record['replay_title_basis']


@pytest.mark.parametrize('method', ['css_article', 'css:.other-container', 'api_fulltext'])
def test_loader_exception_does_not_apply_to_unobserved_extractors(cfg, tmp_path, method):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old_head(queue, patch={'title': '...', 'extraction_method': method})
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 0
        assert queue.db.execute('SELECT error_code FROM retained_metadata_replays').fetchone()[0] == 'retained_metadata_title_conflict'


def test_selector_migration_is_once_only_and_preserves_head_and_original_error(cfg, tmp_path):
    from downloader.news_fulltext_metadata_replay import _ensure_cohort
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old_head(queue, patch={'title': '...'})
        source = queue.registry.sources['cnyes']
        _ensure_cohort(queue, source)
        original = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        marker = 'retained_mobile_loader_selector_v1:' + source.data_fingerprint
        with queue.db:
            queue.db.execute('DELETE FROM queue_settings WHERE key=?', (marker,))
            queue.db.execute("UPDATE retained_metadata_replays SET state='failed_validation',error_code='retained_metadata_title_conflict'")
        _ensure_cohort(queue, source)
        assert tuple(queue.db.execute('SELECT state,error_code FROM retained_metadata_replays').fetchone()) == ('pending', 'retained_metadata_title_conflict')
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original
        with queue.db:
            queue.db.execute("UPDATE retained_metadata_replays SET state='failed_validation'")
        _ensure_cohort(queue, source)
        assert queue.db.execute('SELECT state FROM retained_metadata_replays').fetchone()[0] == 'failed_validation'


def test_actual_conflicting_headline_is_not_waived(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old_head(queue, patch={'title': 'another real source headline'})
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 0
        assert queue.db.execute('SELECT error_code FROM retained_metadata_replays').fetchone()[0] == 'retained_metadata_title_conflict'


def test_pinned_older_newsroom_review_keeps_historical_restrictions_and_same_scope(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        current = queue.registry.sources['cnyes'].authorization
        prior = {k: current[k] for k in ('kind', 'scope', 'evidence_url', 'reviewed_on', 'automated_access_review', 'rights_restrictions')}
        prior['reviewed_on'] = '2026-10-08'
        prior['rights_restrictions'] = prior['rights_restrictions'].split(' Hao is')[0]
        digest = hashlib.sha256(json.dumps(prior, sort_keys=True).encode()).hexdigest()
        old_head(queue, patch={'authorization_json': json.dumps(prior, sort_keys=True), 'authorization_fingerprint': digest})
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 1
        head, record = read_head(queue)
        assert head['authorization_fingerprint'] == record['authorization_fingerprint'] == digest
        assert json.loads(record['authorization_json']) == prior
        assert record['replay_policy_authorization_fingerprint'] == queue.registry.sources['cnyes'].authorization_fingerprint
        assert record['training_injection_enabled'] is False and record['remote_training_export'] is False


def test_api_representation_with_legitimately_missing_byline_is_not_an_html_error(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old_head(queue, patch={'final_url': 'https://api.cnyes.com/media/api/v1/newslist/category/headline'})
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 0
        assert queue.db.execute('SELECT state,error_code FROM retained_metadata_replays').fetchone()[0] == 'not_mobile_html_candidate'


def test_caption_repair_preserves_source_response_capture_and_separate_role(cfg, tmp_path):
    from test_news_cnyes_captions import CAPTION, IMAGE, caption_page
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = old_head(queue, payload=caption_page(), patch={'verified_image_captions_json': None})
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1,
                                             target_article_ids=[original['id']])['repaired_versions'] == 1
        head, record = read_head(queue)
        assert json.loads(record['verified_image_captions_json']) == [{'text': CAPTION, 'image_src': IMAGE}]
        assert CAPTION not in record['body']
        assert head['captured_at_utc'] == record['captured_at_utc'] == original['captured_at_utc']
        assert record['acquisition_transport'] == 'retained_raw_reparse'
        assert verify_bodies(tmp_path)['accepted']


def test_focused_source_repair_leaves_unrequested_old_heads_pending(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = old_head(queue)
        result = replay_retained_cnyes_metadata(queue, target_article_ids=[b'0' * 32], maximum_wall_seconds=1)
        assert result['heads_checked'] == result['repaired_versions'] == result['http_requests'] == 0
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original
        assert queue.db.execute('SELECT state FROM retained_metadata_replays').fetchone()[0] == 'pending'
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 1


def test_retained_observed_query_view_preserves_actual_requested_and_final_uri(cfg, tmp_path):
    url = MOBILE + '?exp=a'
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = old_head(queue, url=url)
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 1
        head, record = read_head(queue)
        assert head['url'] == record['requested_url'] == record['final_url'] == url
        assert head['captured_at_utc'] == original['captured_at_utc']
        assert 'observed_exp_a_request_variant' in record['article_identity_basis']
        assert verify_bodies(tmp_path)['accepted']


@pytest.mark.parametrize('targets', [[], [b'wrong'], ['0' * 64], [b'0' * 32, b'0' * 32], [bytes([i]) * 32 for i in range(33)]])
def test_focused_repair_cannot_hide_an_unbounded_or_ambiguous_cohort(cfg, tmp_path, targets):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        with pytest.raises(ValueError, match='invalid bounded'):
            replay_retained_cnyes_metadata(queue, target_article_ids=targets)
