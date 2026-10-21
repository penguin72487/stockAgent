"""Removing buffering must not turn a short read into a complete receipt."""
from pathlib import Path

import pytest

from downloader import news_receipt_io as io
from scripts.benchmark_news_acceptance_recovery import benchmark, main
from test_news_acceptance_recovery import accepted_queue, config
from test_news_receipt_periodic_reprobe import committed_inputs


class RawInput:
    def __init__(self, raw, *, step=3, fail_after=None, close_error=None):
        self.raw, self.step, self.fail_after, self.close_error = raw, step, fail_after, close_error
        self.position, self.read_sizes, self.closed = 0, [], False

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.closed = True
        if self.close_error:
            raise self.close_error

    def read(self, size):
        self.read_sizes.append(size)
        if self.fail_after is not None and self.position >= self.fail_after:
            raise OSError('later raw read failed')
        result = self.raw[self.position:self.position + min(size, self.step)]
        self.position += len(result)
        return result


class InputPath:
    def __init__(self, stream):
        self.stream, self.opens = stream, []

    def open(self, *args, **kwargs):
        self.opens.append((args, kwargs))
        return self.stream


@pytest.mark.parametrize('raw', [b'', b'a', b'{}\r\n\0\xff', bytes(range(255))])
@pytest.mark.parametrize('limit', [0, 1, 3, 8, 300])
@pytest.mark.parametrize('step', [1, 3, 1024])
def test_short_reads_preserve_exact_prefix_and_actual_eof(raw, limit, step):
    path = InputPath(RawInput(raw, step=step))
    assert io._read_prefix(path, limit) == raw[:limit]
    assert path.opens == [(('rb',), {'buffering': 0})]
    assert path.stream.closed and path.stream.position <= limit
    assert path.stream.read_sizes[0] == limit
    assert all(size >= 0 for size in path.stream.read_sizes)


@pytest.mark.parametrize('fail_after', [0, 3, 6])
def test_partial_read_error_is_preserved_at_original_order_and_never_accepted(tmp_path, fail_after):
    first = tmp_path / 'first.json'; first.write_bytes(b'{}')
    failed = InputPath(RawInput(b'123456789', fail_after=fail_after))
    later = InputPath(RawInput(b'[]'))
    rows = io._read_chunk([first, failed, later])
    assert rows[0] == (first, b'{}', None)
    assert rows[1][:2] == (failed, None) and isinstance(rows[1][2], OSError)
    assert rows[2] == (later, None, None) and later.opens == []
    assert failed.stream.closed


def test_close_failure_does_not_accept_previously_read_bytes():
    path = InputPath(RawInput(b'{}', close_error=OSError('close failed')))
    rows = io._read_chunk([path])
    assert rows[0][:2] == (path, None) and isinstance(rows[0][2], OSError)


def test_none_from_raw_input_is_not_mistaken_for_eof():
    class WouldBlock(RawInput):
        def read(self, size):
            return None if self.position else super().read(size)
    path = InputPath(WouldBlock(b'{}'))
    rows = io._read_chunk([path])
    assert rows[0][:2] == (path, None) and isinstance(rows[0][2], TypeError)
    assert path.stream.closed


@pytest.mark.parametrize('kind', ['missing', 'directory', 'symlink_loop'])
def test_original_path_errors_keep_path_and_type(tmp_path, kind):
    path = tmp_path / 'bad.json'
    if kind == 'directory':
        path.mkdir()
    elif kind == 'symlink_loop':
        path.symlink_to(path)
    with pytest.raises(OSError) as reference:
        with path.open('rb') as stream:
            stream.read(64)
    with pytest.raises(type(reference.value)) as candidate:
        io._read_prefix(path, 64)
    assert candidate.value.errno == reference.value.errno
    assert candidate.value.filename == reference.value.filename


def test_path_subclass_open_and_context_are_not_bypassed(tmp_path):
    class CustomPath(type(Path())):
        calls = []
        def open(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            return super().open(*args, **kwargs)
    plain = tmp_path / 'input.json'; plain.write_bytes(b'{"value":-1.25}')
    path = CustomPath(plain)
    assert io._read_prefix(path, 1024) == plain.read_bytes()
    assert path.calls == [(('rb',), {'buffering': 0})]


@pytest.mark.parametrize('readers', [None, 0, 1, 2])
def test_new_benchmark_axis_only_changes_prefix_primitive(config, tmp_path, readers):
    queue, http = accepted_queue(config, tmp_path)
    original = io._read_prefix
    try:
        committed_inputs(queue, 9)
        before = queue.db.total_changes
        report = benchmark(tmp_path, maximum_receipts=5, cycles=1, cohort_sampling='stratified',
            prefix_io_only=True, prefix_io_readers=readers)
        assert report['accepted'] and report['contract_version'] == 10
        assert report['decode_parity']['all_fields_equal'] and report['prefix_input_parity']
        assert report['prefix_io_readers'] == readers and report['candidate_prefix_io_only']
        assert [r['mode'] for r in report['results']] == ['buffered_prefix', 'unbuffered_prefix',
            'unbuffered_prefix', 'buffered_prefix']
        assert all(r['receipt_json_reads'] == 5 and r['committed_lookup_statements'] == 1
            and r['sqlite_changes'] == 0 and r['receipt_io']['prefix_io_contract_version'] == 2
            for r in report['results'])
        assert report['source_receipts_unchanged'] and report['publisher_requests'] == 0
        assert not report['all_history_complete'] and not report['production_database_written']
        assert queue.db.total_changes == before and io._read_prefix is original
    finally:
        queue.close(); http.close()


@pytest.mark.parametrize('options', [
    {'prefix_io_only': 1}, {'prefix_io_readers': 2},
    {'prefix_io_only': True, 'prefix_io_readers': True},
    {'prefix_io_only': True, 'prefix_io_readers': -1},
    {'prefix_io_only': True, 'prefix_io_readers': 3},
    {'prefix_io_only': True, 'adaptive_read_ahead': True},
    {'prefix_io_only': True, 'batched_read_ahead_workers': 2},
    {'prefix_io_only': True, 'prefix_io_readers': 2, 'seed_wait_bound_prefix': True},
])
def test_invalid_or_confounding_axes_rejected_before_source_access(tmp_path, options):
    with pytest.raises(ValueError, match='required'):
        benchmark(tmp_path, **options)


def test_benchmark_restores_native_reader_after_recovery_error(config, tmp_path, monkeypatch):
    from downloader import news_fulltext_queue as q
    queue, http = accepted_queue(config, tmp_path)
    original = io._read_prefix
    try:
        monkeypatch.setattr(q.NewsQueue, 'recover_acceptance', lambda *a, **k: (_ for _ in ()).throw(ValueError('recovery failed')))
        with pytest.raises(ValueError, match='recovery failed'):
            benchmark(tmp_path, maximum_receipts=1, cycles=1, prefix_io_only=True)
        assert io._read_prefix is original
    finally:
        queue.close(); http.close()


def test_cli_accepts_only_the_explicit_prefix_io_axis(config, tmp_path):
    import json
    queue, http = accepted_queue(config, tmp_path)
    try:
        output = tmp_path / 'report.json'
        assert main(['--root', str(tmp_path), '--maximum-receipts', '1', '--cycles', '1',
            '--prefix-io-only', '--prefix-io-readers', '2', '--output', str(output)]) == 0
        report = json.loads(output.read_bytes())
        assert report['accepted'] and report['contract_version'] == 10
        assert report['prefix_io_readers'] == 2 and report['prefix_input_parity']
    finally:
        queue.close(); http.close()
