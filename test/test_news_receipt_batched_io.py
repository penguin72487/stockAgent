"""I/O concurrency never becomes concurrent acceptance or skipped validation."""
from contextlib import closing
import json
from pathlib import Path
import threading

import pytest

from downloader import news_fulltext_queue as queue_module
from downloader import news_receipt_io as io_module
from downloader.artifact_io import atomic_write_json
from scripts.benchmark_news_acceptance_recovery import benchmark
from stockagent.private_json import loads
from test_news_acceptance_recovery import accepted_queue, config


@pytest.mark.parametrize('readers', [1, 2])
def test_all_original_inputs_are_bounded_ordered_and_read_once(tmp_path, readers):
    paths = []
    for number in range(273):
        path = tmp_path / f'{number:04}.json'
        path.write_bytes(json.dumps({'number': number, 'value': -1234567.25}).encode())
        paths.append(path)
    with closing(io_module.receipt_input_chunks(iter(paths), readers=readers)) as chunks:
        rows = [row for chunk in chunks for row in chunk]
    assert [p for p, _, _ in rows] == paths
    assert [loads(raw)['number'] for _, raw, _ in rows] == list(range(273))
    assert all(error is None for _, _, error in rows)
    assert not any(t.name.startswith('news-receipt-batch') for t in threading.enumerate())


@pytest.mark.parametrize('invalid', [-1, 3, True, 1.5, '2', None])
def test_reader_count_is_explicit_and_bounded(invalid):
    with pytest.raises(ValueError, match='bounded_batched_receipt_readers_required'):
        list(io_module.receipt_input_chunks([], readers=invalid))


def test_each_inflight_batch_has_a_byte_budget_and_large_inputs_fall_back(tmp_path, monkeypatch):
    monkeypatch.setattr(io_module, 'RECEIPT_CHUNK_BYTES', 16)
    paths = [tmp_path / f'{n}.json' for n in range(3)]
    for path, raw in zip(paths, [b'{}', b' ' * 17, b'[]']):
        path.write_bytes(raw)
    with closing(io_module.receipt_input_chunks(paths, readers=2)) as chunks:
        rows = list(chunks)
    assert rows == [[(paths[0], b'{}', None), (paths[1], None, None), (paths[2], None, None)]]


def test_iterator_does_not_eagerly_submit_all_history_and_closes_its_threads(tmp_path, monkeypatch):
    paths = [tmp_path / f'{n}.json' for n in range(2000)]
    consumed = []
    def inputs():
        for path in paths:
            consumed.append(path)
            yield path
    monkeypatch.setattr(io_module, '_read_chunk', lambda chunk: [(p, b'{}', None) for p in chunk])
    with closing(io_module.receipt_input_chunks(inputs(), readers=2)) as chunks:
        assert len(next(chunks)) == 128
        assert len(consumed) <= 384
    assert not any(t.name.startswith('news-receipt-batch') for t in threading.enumerate())


def test_explicit_outside_path_is_not_read_speculatively(tmp_path, monkeypatch):
    root, outside = tmp_path / 'queue', tmp_path / 'outside.json'
    def reject_read(*_args, **_kwargs):
        pytest.fail('an outside input was read before its path guard')
    monkeypatch.setattr(Path, 'open', reject_read)
    with closing(io_module.receipt_input_chunks([outside], readers=2, root=root)) as chunks:
        rows = list(chunks)
    assert rows[0][0][0] == outside and rows[0][0][1] is None
    assert isinstance(rows[0][0][2], ValueError)


@pytest.mark.parametrize('readers', [1, 2])
def test_decode_sql_and_valid_orphan_commit_remain_on_owner_before_later_io_error(config, tmp_path, monkeypatch, readers):
    queue, http = accepted_queue(config, tmp_path)
    try:
        with queue.db:
            queue.db.execute('DELETE FROM accepted_batches')
        (tmp_path / 'receipts' / 'zz-invalid.json').mkdir()
        owner_thread, seen, decode = threading.get_ident(), [], queue_module.decode_private_json
        def record_decode(data):
            seen.append(threading.get_ident())
            return decode(data)
        monkeypatch.setattr(queue_module, 'decode_private_json', record_decode)
        with pytest.raises(IsADirectoryError):
            queue.recover_acceptance(read_ahead_workers=readers)
        assert seen == [owner_thread]
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
        assert queue.db.execute('SELECT state FROM articles').fetchone()[0] == 'complete'
        assert not any(t.name.startswith('news-receipt-batch') for t in threading.enumerate())
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('corruption', ['', 'digest', 'rows', 'contract', 'json'])
@pytest.mark.parametrize('readers', [1, 2])
def test_canonical_orphan_validation_is_not_relaxed(config, tmp_path, corruption, readers):
    queue, http = accepted_queue(config, tmp_path)
    try:
        relative, receipt_relative = queue.db.execute('SELECT path,receipt_path FROM accepted_batches').fetchone()
        path = tmp_path / receipt_relative
        receipt = json.loads(path.read_text())
        if corruption == 'digest':
            receipt['sha256'] = '0' * 64
        elif corruption == 'rows':
            receipt['rows'] += 1
        elif corruption == 'contract':
            receipt['contract_version'] = -1
        path.write_text('{invalid}' if corruption == 'json' else json.dumps(receipt))
        with queue.db:
            queue.db.execute('DELETE FROM accepted_batches')
            queue.db.execute("UPDATE articles SET state='pending',body_sha256=NULL,body_bytes=NULL,parquet_path=NULL")
        if corruption:
            with pytest.raises(ValueError):
                queue.recover_acceptance(read_ahead_workers=readers)
            assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        else:
            queue.recover_acceptance(read_ahead_workers=readers)
            assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 1
            assert queue.db.execute('SELECT state FROM articles').fetchone()[0] == 'complete'
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('extra', [
    'null', '["台灣","\\ud800"]', '1234567890123456789012345678901234567890',
    '-0.0', '[NaN,Infinity,-Infinity,1e309]', '{"duplicate":1,"duplicate":2}',
])
def test_committed_receipt_still_fully_decodes_original_untyped_values(config, tmp_path, monkeypatch, extra):
    queue, http = accepted_queue(config, tmp_path)
    try:
        relative, receipt_relative = queue.db.execute('SELECT path,receipt_path FROM accepted_batches').fetchone()
        raw = '{"parquet_path":' + json.dumps(relative) + ',"extra":' + extra + '}'
        (tmp_path / receipt_relative).write_text(raw)
        decode, seen = queue_module.decode_private_json, []
        def record_decode(data):
            value = decode(data)
            seen.append(json.dumps(value, ensure_ascii=True))
            return value
        monkeypatch.setattr(queue_module, 'decode_private_json', record_decode)
        before = queue.db.total_changes
        queue.recover_acceptance(read_ahead_workers=2)
        assert seen == [json.dumps(json.loads(raw), ensure_ascii=True)]
        assert queue.db.total_changes == before
        assert queue.acceptance_receipt_io['maximum_prefetch_raw_bytes'] == 4 * (1 << 20) + 2
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('suffix', [',"invalid":', ',"tail":1} garbage'])
def test_bad_suffix_on_committed_receipt_is_not_skipped(config, tmp_path, suffix):
    queue, http = accepted_queue(config, tmp_path)
    try:
        relative, receipt_relative = queue.db.execute('SELECT path,receipt_path FROM accepted_batches').fetchone()
        (tmp_path / receipt_relative).write_text('{"parquet_path":' + json.dumps(relative) + suffix)
        with pytest.raises(json.JSONDecodeError):
            queue.recover_acceptance(read_ahead_workers=2)
    finally:
        queue.close()
        http.close()


def test_oversized_receipt_is_completely_reread_and_decoded_on_owner(config, tmp_path, monkeypatch):
    queue, http = accepted_queue(config, tmp_path)
    try:
        monkeypatch.setattr(io_module, 'RECEIPT_CHUNK_BYTES', 32)
        relative, receipt_relative = queue.db.execute('SELECT path,receipt_path FROM accepted_batches').fetchone()
        raw = json.dumps({'parquet_path': relative, 'large': '台灣' * 128})
        (tmp_path / receipt_relative).write_text(raw)
        seen, original = [], Path.read_bytes
        def read(path):
            seen.append((path, threading.get_ident()))
            return original(path)
        monkeypatch.setattr(Path, 'read_bytes', read)
        queue.recover_acceptance(read_ahead_workers=2)
        assert seen == [(tmp_path / receipt_relative, threading.get_ident())]
    finally:
        queue.close()
        http.close()


def test_large_committed_history_preserves_covering_query_count_and_has_no_writes(config, tmp_path):
    queue, http = accepted_queue(config, tmp_path)
    statements = []
    try:
        for number in range(260):
            relative, receipt = f'bodies/fixture-{number}.parquet', f'receipts/fixture-{number}.json'
            atomic_write_json(tmp_path / receipt, {'parquet_path': relative})
            queue.db.execute('INSERT INTO accepted_batches VALUES (?,?)', (relative, receipt))
        queue.db.commit()
        before = queue.db.total_changes
        queue.db.set_trace_callback(statements.append)
        queue.recover_acceptance(read_ahead_workers=2)
        assert queue.db.total_changes == before
        assert len([s for s in statements if s.startswith('SELECT receipt_path,path')]) == 3
        assert not any(s.startswith('SELECT 1 FROM accepted_batches') for s in statements)
    finally:
        queue.db.set_trace_callback(None)
        queue.close()
        http.close()


def test_batched_benchmark_uses_canonical_recovery_and_no_production_writes(config, tmp_path):
    queue, http = accepted_queue(config, tmp_path)
    try:
        report = benchmark(tmp_path, maximum_receipts=1, cycles=1, batched_read_ahead_workers=2)
        assert report['accepted'] and report['contract_version'] == 5
        assert report['decode_parity']['all_fields_equal']
        assert report['candidate_batched_read_ahead_workers'] == 2
        assert {row['mode'] for row in report['results']} == {'binary_serial', 'binary_batched_prefetch'}
        assert all(row['sqlite_changes'] == 0 for row in report['results'])
        assert report['source_receipts_unchanged'] and report['publisher_requests'] == 0
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('count,wall,cpu,expected', [
    (128, .1, .02, 2), (128, .02, .013, 2), (128, .1, .07, 0),
    (128, .001, .0001, 0), (127, .5, .01, 0), (0, .1, 0., 0),
])
def test_reader_activation_requires_a_complete_clearly_wait_bound_probe(count, wall, cpu, expected):
    assert io_module.adaptive_receipt_readers({'receipts': count, 'wall_seconds': wall, 'cpu_seconds': cpu}) == expected


def test_adaptive_can_detect_cold_tail_after_warm_prefix_without_rechecking_prior_receipts(config, tmp_path, monkeypatch):
    queue, http = accepted_queue(config, tmp_path)
    try:
        for number in range(410):
            relative, receipt = f'bodies/fixture-{number}.parquet', f'receipts/fixture-{number}.json'
            atomic_write_json(tmp_path / receipt, {'parquet_path': relative})
            queue.db.execute('INSERT INTO accepted_batches VALUES (?,?)', (relative, receipt))
        queue.db.commit()
        probes, decoded, decode = [], [], queue_module.decode_private_json
        def choose(probe):
            probes.append(dict(probe))
            return 0 if len(probes) == 1 else 2
        def record_decode(data):
            value = decode(data)
            decoded.append(value['parquet_path'])
            return value
        monkeypatch.setattr(queue_module, 'RECEIPT_PROBE_STRIDE', 2)
        monkeypatch.setattr(queue_module, 'adaptive_receipt_readers', choose)
        monkeypatch.setattr(queue_module, 'decode_private_json', record_decode)
        before = queue.db.total_changes
        queue.recover_acceptance()
        observation = queue.acceptance_receipt_io
        assert len(probes) == 2 and all(p['receipts'] == 128 for p in probes)
        assert observation['completed'] and observation['mode'] == 'adaptive'
        assert observation['chosen_readers'] == 2 and observation['started_readers_after_receipts'] == 384
        assert observation['receipts_decoded'] == len(decoded) == len(set(decoded)) == 411
        assert observation['prefetched_inputs'] == 27 and observation['synchronous_inputs'] == 384
        assert queue.db.total_changes == before and queue._acceptance_read_probe is None
        assert not any(t.name.startswith('news-receipt-batch') for t in threading.enumerate())
    finally:
        queue.close()
        http.close()


def test_warm_probe_does_not_create_an_io_pool(config, tmp_path, monkeypatch):
    queue, http = accepted_queue(config, tmp_path)
    try:
        monkeypatch.setattr(queue_module, 'adaptive_receipt_readers', lambda _probe: 0)
        def reject_pool(**_kwargs):
            raise AssertionError('warm recovery must not start reader threads')
        monkeypatch.setattr(io_module, 'ThreadPoolExecutor', reject_pool)
        queue.recover_acceptance()
        observation = queue.acceptance_receipt_io
        assert observation['completed'] and observation['chosen_readers'] == 0
        assert observation['prefetched_inputs'] == 0 and observation['synchronous_inputs'] == 1
    finally:
        queue.close()
        http.close()


def test_adaptive_benchmark_preserves_canonical_input_values_and_reports_chosen_strategy(config, tmp_path):
    queue, http = accepted_queue(config, tmp_path)
    try:
        report = benchmark(tmp_path, maximum_receipts=1, cycles=1, adaptive_read_ahead=True)
        assert report['accepted'] and report['contract_version'] == 6
        assert report['decode_parity']['all_fields_equal'] and report['candidate_adaptive_read_ahead']
        assert {r['mode'] for r in report['results']} == {'binary_serial', 'adaptive_batched_prefetch'}
        assert all(r['receipt_io']['receipts_decoded'] == 1 and r['receipt_io']['completed'] for r in report['results'])
        assert report['source_receipts_unchanged'] and report['publisher_requests'] == 0
    finally:
        queue.close()
        http.close()
