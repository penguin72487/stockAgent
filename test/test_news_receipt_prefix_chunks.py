"""Small receipt reads retain full prefixes, actual EOF and canonical replay."""
import json
import random
from pathlib import Path
import subprocess
import sys

import pytest

from downloader import news_receipt_io as io
from scripts.benchmark_news_acceptance_recovery import (
    _single_allocation_receipt_prefix, _geometric_receipt_prefix, benchmark, main,
)
from test_news_acceptance_recovery import accepted_queue, config
from test_news_receipt_periodic_reprobe import committed_inputs
from test_news_receipt_prefix_io import InputPath, RawInput


@pytest.mark.parametrize('length,limit,step', [
    (0, 0, 1), (0, 4096, 7), (1, 1, 1), (4095, 8192, 9000),
    (4096, 4096, 4096), (4097, 4097, 1), (12000, 16384, 7),
    (16000, 12000, 4096), (262144, 1 << 20, 1 << 20),
    ((1 << 20) + 2, (1 << 20) + 1, 1 << 22),
])
def test_chunk_growth_retains_exact_bytes_with_short_reads(length, limit, step):
    raw = bytes(range(256)) * ((length + 255) // 256)
    raw = raw[:length]
    reference, actual = (InputPath(RawInput(raw, step=step)) for _ in range(2))
    assert _geometric_receipt_prefix(actual, limit) == _single_allocation_receipt_prefix(reference, limit) == raw[:limit]
    assert actual.stream.read_sizes[0] == min(limit, 4096)
    assert actual.stream.position <= limit and actual.stream.closed
    assert all(0 <= size <= limit for size in actual.stream.read_sizes)


def test_tiny_receipt_never_requests_the_entire_batch_budget():
    raw = b'{"parquet_path":"bodies/example.parquet"}'
    path = InputPath(RawInput(raw, step=1 << 22))
    assert _geometric_receipt_prefix(path, (1 << 20) + 1) == raw
    assert path.stream.read_sizes == [4096, 4096]
    assert path.stream.closed


def test_growth_uses_full_blocks_without_interpreting_short_reads_as_eof():
    raw = b'x' * 25000
    path = InputPath(RawInput(raw, step=1 << 22))
    assert _geometric_receipt_prefix(path, 200000) == raw
    assert path.stream.read_sizes == [4096, 16384, 65536, 65536]


def test_seeded_prefix_parity_for_varied_inputs():
    rng = random.Random(20261022)
    for _ in range(100):
        raw = rng.randbytes(rng.randrange(0, 20000))
        limit, step = rng.randrange(0, 24000), rng.randrange(64, 10000)
        path = InputPath(RawInput(raw, step=step))
        assert _geometric_receipt_prefix(path, limit) == raw[:limit]


def test_later_read_failure_is_not_promoted_to_a_short_success():
    stream = RawInput(b'x' * 20000, step=4096, fail_after=4096)
    path = InputPath(stream)
    with pytest.raises(OSError, match='later raw read failed'):
        _geometric_receipt_prefix(path, 1 << 20)
    assert stream.closed and stream.position == 4096


@pytest.mark.parametrize('readers', [None, 0, 1, 2])
def test_chunk_benchmark_changes_only_prefix_allocation_strategy(config, tmp_path, readers):
    queue, http = accepted_queue(config, tmp_path)
    original = io._read_prefix
    try:
        committed_inputs(queue, 9)
        before = queue.db.total_changes
        result = benchmark(tmp_path, maximum_receipts=5, cycles=1,
            cohort_sampling='stratified', prefix_chunk_only=True, prefix_io_readers=readers)
        assert result['accepted'] and result['contract_version'] == 11
        assert result['candidate_prefix_chunk_only'] and not result['candidate_prefix_io_only']
        assert result['decode_parity']['all_fields_equal'] and result['prefix_input_parity']
        assert [row['mode'] for row in result['results']] == [
            'single_allocation_prefix', 'geometric_small_prefix',
            'geometric_small_prefix', 'single_allocation_prefix']
        assert all(row['receipt_json_reads'] == 5 and row['committed_lookup_statements'] == 1
            and row['sqlite_changes'] == 0 for row in result['results'])
        assert queue.db.total_changes == before and io._read_prefix is original
        assert result['source_receipts_unchanged'] and result['publisher_requests'] == 0
    finally:
        queue.close(); http.close()


@pytest.mark.parametrize('options', [
    {'prefix_chunk_only': 1}, {'prefix_io_only': True}, {'adaptive_read_ahead': True},
    {'prefix_io_readers': -1}, {'prefix_io_readers': True}, {'prefix_io_readers': 3},
    {'prefix_io_readers': 2, 'seed_wait_bound_prefix': True},
    {'batched_read_ahead_workers': 2}, {'binary_read_only': True},
])
def test_mixed_or_malformed_chunk_axis_is_rejected_before_source_access(tmp_path, options):
    with pytest.raises(ValueError, match='required'):
        benchmark(tmp_path, **{'prefix_chunk_only': True, **options})


def test_chunk_cli_saves_exact_axis_and_retains_no_write_contract(config, tmp_path):
    queue, http = accepted_queue(config, tmp_path)
    try:
        output = tmp_path / 'result.json'
        assert main(['--root', str(tmp_path), '--maximum-receipts', '1', '--cycles', '1',
            '--prefix-chunk-only', '--prefix-io-readers', '2', '--output', str(output)]) == 0
        result = json.loads(output.read_bytes())
        assert result['accepted'] and result['contract_version'] == 11
        assert result['prefix_io_readers'] == 2 and not result['production_database_written']
    finally:
        queue.close(); http.close()


def test_direct_benchmark_script_help_resolves_repo_without_pythonpath(tmp_path):
    import os
    environment = dict(os.environ)
    environment.pop('PYTHONPATH', None)
    script = Path(__file__).resolve().parents[1] / 'scripts' / 'benchmark_news_acceptance_recovery.py'
    result = subprocess.run([sys.executable, str(script), '--help'],
        cwd=tmp_path, env=environment, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert '--prefix-chunk-only' in result.stdout


def test_experimental_reader_is_restored_after_canonical_recovery_error(config, tmp_path, monkeypatch):
    from downloader import news_fulltext_queue as module
    queue, http = accepted_queue(config, tmp_path)
    original = io._read_prefix
    try:
        def fail(*args, **kwargs):
            raise ValueError('canonical recovery failed')
        monkeypatch.setattr(module.NewsQueue, 'recover_acceptance', fail)
        with pytest.raises(ValueError, match='canonical recovery failed'):
            benchmark(tmp_path, maximum_receipts=1, cycles=1, prefix_chunk_only=True, prefix_io_readers=2)
        assert io._read_prefix is original
    finally:
        queue.close(); http.close()


def test_production_prefix_remains_prior_single_allocation_implementation():
    path = InputPath(RawInput(b'{"valid":true}', step=1 << 22))
    limit = (1 << 20) + 1
    assert io._read_prefix(path, limit) == b'{"valid":true}'
    assert path.stream.read_sizes == [limit, limit - len(b'{"valid":true}')]
    assert io.RECEIPT_PREFIX_IO_VERSION == 2
