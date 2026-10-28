"""Full collector parity, source fences and cold authority with typed reuse."""
from __future__ import annotations

import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from types import CodeType, FunctionType
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from downloader import download_fred_crypto_macro_vintages as fred
from downloader.http_transport import HttpResponse
from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync import retained_source as retained
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json


def seed(tmp_path, monkeypatch):
    source = tmp_path / 'source'
    calls = []
    class Transport:
        def __init__(self, *_args, **_kwargs):
            pass
        def request_bytes(self, url, **_kwargs):
            params = parse_qs(urlsplit(url).query)
            calls.append(params)
            if 'realtime_start' not in params:
                raw = b'{"realtime_end":"2026-01-01"}'
            elif params['series_id'] == ['DFF']:
                raw = b'{"observations":[],"point_in_time_gap":"fixture unavailable vintage"}'
            elif params['realtime_start'] == ['2020-01-01']:
                raw = b'{"observations":[{"date":"2020-01-01","realtime_start":"2020-01-02","value":"-3.5"}]}'
            else:
                raw = b'{"observations":[{"date":"2025-01-01","realtime_start":"2025-01-02","value":"0.0"}]}'
            return HttpResponse(200, raw, {}, 1)
    monkeypatch.setattr(fred, '_configured_api_key', lambda *_args: 'fixture')
    monkeypatch.setattr(fred, 'ResilientHttpTransport', Transport)
    args = SimpleNamespace(output_dir=str(source), env_file='.env', start_date='2020-01-01',
        end_date='2026-01-01', series=['DGS10', 'DFF'], max_retries=0, workers=2,
        historical_recheck_days=30, refresh=False)
    with retained.collector_owner(source, 'fred-crypto-macro'):
        fred._run_collection(args)
    return source, args, calls, pl.read_parquet(source / 'observations.parquet')


def summary(source):
    return json.loads((source / 'download_summary.json').read_bytes())


def test_complete_refresh_reuses_raw_values_clocks_gaps_and_zero_without_extra_API(tmp_path, monkeypatch):
    source, args, calls, oracle = seed(tmp_path, monkeypatch)
    assert len(summary(source)['closed_projection']['windows']) == 2
    calls.clear()
    with retained.collector_owner(source, 'fred-crypto-macro'):
        fred._run_collection(args)
    assert len(calls) == 3  # Provider date + current window of each series.
    observed = summary(source)
    assert observed['closed_projection_reused_windows'] == 2
    assert observed['cached_windows'] == 2 and observed['fetched_windows'] == 2
    assert_frame_equal(pl.read_parquet(source / 'observations.parquet'), oracle)
    assert oracle['value'].to_list() == [-3.5, 0.0]


@pytest.mark.parametrize('fault', ['parquet', 'normalizer', 'receipt', 'window-digest'])
def test_invalid_projection_falls_back_to_verified_raw_without_historical_API(tmp_path, monkeypatch, fault):
    source, args, calls, oracle = seed(tmp_path, monkeypatch)
    observed = summary(source)
    if fault == 'parquet':
        oracle.with_columns(pl.lit(77.0).alias('value')).write_parquet(source / 'observations.parquet')
    elif fault == 'normalizer':
        original = fred.parse_initial_release_rows
        def changed_normalizer(*args, **kwargs):
            return original(*args, **kwargs)
        monkeypatch.setattr(fred, 'parse_initial_release_rows', changed_normalizer)
    elif fault == 'receipt':
        closed = next((source / 'receipts/DGS10').glob('2020-01-01_*.json'))
        row = json.loads(closed.read_bytes())
        row['audit_note'] = 'source receipt changed'
        atomic_write_json(closed, row)
    else:
        for row in observed['closed_projection']['windows'].values():
            row['rows_sha256'] = '0' * 64
        atomic_write_json(source / 'download_summary.json', observed)
    calls.clear()
    with retained.collector_owner(source, 'fred-crypto-macro'):
        fred._run_collection(args)
    assert len(calls) == 3
    assert summary(source)['closed_projection_reused_windows'] == (1 if fault == 'receipt' else 0)
    assert_frame_equal(pl.read_parquet(source / 'observations.parquet'), oracle)


def test_source_change_after_projection_use_rejects_canonical_replacement(tmp_path, monkeypatch):
    source, args, calls, oracle = seed(tmp_path, monkeypatch)
    raw_name = next(row['raw_path'] for key, row in summary(source)['closed_projection']['windows'].items()
        if key.startswith('DGS10:'))
    original_fetch = fred._fetch_window
    def changed_source(*args, **kwargs):
        path = source / raw_name
        path.write_bytes(path.read_bytes())  # Same bytes, new generation still must reject.
        return original_fetch(*args, **kwargs)
    monkeypatch.setattr(fred, '_fetch_window', changed_source)
    # The loaded-code fingerprint correctly rejects a changed fetcher, so keep
    # the seed's actual identity for this targeted post-admission race test.
    expected_identity = summary(source)['closed_projection']['normalizer_identity']
    monkeypatch.setattr(fred, 'normalizer_identity', lambda *a, **k: expected_identity)
    with retained.collector_owner(source, 'fred-crypto-macro'):
        with pytest.raises(ValueError, match='generation changed'):
            fred._run_collection(args)
    assert_frame_equal(pl.read_parquet(source / 'observations.parquet'), oracle)


def activate_fixture(source, cold, pins):
    with retained.collector_owner(source, 'fred-crypto-macro') as descriptor:
        release = packed.publish_packed_snapshot(cold, 'fred-crypto-macro', source, pack_buckets=2)
        binding = retained.prepare_retained_source_binding(source, cold, pins, release, source_lock_fd=descriptor)
        atomic_write_json(source / retained.BINDING, binding)
        originals = {name: (source / name).read_bytes() for name in binding['raw_files']}
        for name in originals:
            (source / name).unlink()  # Isolated fixture only, after complete original proof.
    return release, originals


def test_metadata_first_collector_avoids_old_raw_reads_but_publication_verifies_full_history(tmp_path, monkeypatch):
    source, args, calls, oracle = seed(tmp_path, monkeypatch)
    cold, pins = tmp_path / 'cold', tmp_path / 'pins'
    base, originals = activate_fixture(source, cold, pins)
    count = []
    original_read = packed._read_packed_files_verified
    def observed_read(*args, **kwargs):
        count.append(1)
        return original_read(*args, **kwargs)
    monkeypatch.setattr(packed, '_read_packed_files_verified', observed_read)
    with retained.collector_owner(source, 'fred-crypto-macro'):
        state = retained.load_retained_source(source, 'fred-crypto-macro', cold=cold, pin_root=pins,
            read_originals=False, defer_originals=True)
        assert not count and state.publication_request()._verified_read is None
        calls.clear()
        fred._run_collection(args, retained_raw=state)
        assert not count  # Includes two current API payload comparisons, without raw recreation.
        assert summary(source)['closed_projection_reused_windows'] == 2 and len(calls) == 3
        assert all(not (source / name).exists() for name in originals)
        assert_frame_equal(pl.read_parquet(source / 'observations.parquet'), oracle)
        saved = packed.publish_packed_snapshot(cold, 'fred-crypto-macro', source, pack_buckets=2,
            retained_source_files=state.publication_request(), source_guard=state.check_binding_generation)
        assert len(count) == 1
    restored = packed.fetch_packed_snapshot(cold, tmp_path / 'restore', saved)
    for name, raw in originals.items():
        assert (restored / name).read_bytes() == raw
    assert packed.verify_packed_snapshot(cold, saved, reconstruct_all=True)['independently_reconstructed_files'] == saved.manifest['source']['files']


def test_projection_is_not_a_cold_payload_integrity_or_deletion_proof(tmp_path, monkeypatch):
    source, args, _, _ = seed(tmp_path, monkeypatch)
    cold, pins = tmp_path / 'cold', tmp_path / 'pins'
    base, _ = activate_fixture(source, cold, pins)
    state = retained.load_retained_source(source, 'fred-crypto-macro', cold=cold, pin_root=pins,
        read_originals=False, defer_originals=True)
    path = cold / base.manifest['archive']['objects'][0]['relpath']
    payload = path.read_bytes()
    path.write_bytes(payload[:-1] + bytes([payload[-1] ^ 1]))
    # The service projection still holds original verified values. It does not
    # permit a publisher to accept corrupted D originals or retire any source.
    fred._run_collection(args, retained_raw=state)
    with pytest.raises(SnapshotError, match='checksum'):
        packed.publish_packed_snapshot(cold, 'fred-crypto-macro', source, pack_buckets=2,
            retained_source_files=state.publication_request(), source_guard=state.check_binding_generation)
    assert packed.resolve_latest_packed(cold, 'fred-crypto-macro').manifest_sha256 == base.manifest_sha256


def test_truncated_raw_cache_becomes_a_bounded_repair_not_an_uncaught_EOF(tmp_path, monkeypatch):
    source, args, calls, _ = seed(tmp_path, monkeypatch)
    raw = next(row['raw_path'] for key, row in summary(source)['closed_projection']['windows'].items()
        if key.startswith('DGS10:'))
    (source / raw).write_bytes(gzip.compress(b'{}')[:-5])
    calls.clear()
    with retained.collector_owner(source, 'fred-crypto-macro'):
        fred._run_collection(args)
    assert len(calls) == 4 and summary(source)['fetched_windows'] == 3
    assert pl.read_parquet(source / 'observations.parquet')['value'].to_list() == [-3.5, 0.0]


def test_projection_does_not_extend_the_official_history_recheck_interval(tmp_path, monkeypatch):
    source, args, calls, oracle = seed(tmp_path, monkeypatch)
    observed = summary(source)
    for key, record in observed['closed_projection']['windows'].items():
        series, start, end = key.split(':')
        path = source / 'receipts' / series / f'{start}_{end}.json'
        row = json.loads(path.read_bytes())
        row['completed_at_utc'] = '2001-01-01T00:00:00+00:00'
        atomic_write_json(path, row)
        record['receipt_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
    atomic_write_json(source / 'download_summary.json', observed)
    calls.clear()
    with retained.collector_owner(source, 'fred-crypto-macro'):
        fred._run_collection(args)
    assert len(calls) == 5
    assert summary(source)['closed_projection_reused_windows'] == 0
    assert_frame_equal(pl.read_parquet(source / 'observations.parquet'), oracle)


def projection_identity():
    return fred.normalizer_identity((fred.parse_initial_release_rows, fred._empty_initial_release_frame,
        fred._realtime_windows, fred._read_cached_window, fred._fetch_window, fred._run_collection),
        parameters={'schema_version': fred.SCHEMA_VERSION, 'request_contract_version': fred.REQUEST_CONTRACT_VERSION})


def test_loaded_identity_does_not_depend_on_active_nested_functions():
    before = projection_identity()
    nested = [value for value in fred._run_collection.__code__.co_consts if isinstance(value, CodeType)]
    def cell():
        value = None
        return (lambda: value).__closure__[0]
    held = [FunctionType(code, fred.__dict__, code.co_name,
        closure=tuple(cell() for _ in code.co_freevars)) for code in nested]
    assert held and projection_identity() == before


def test_loaded_identity_survives_a_fresh_process_and_different_hash_seed():
    script = ('from downloader import download_fred_crypto_macro_vintages as f; '
        'print(f.normalizer_identity((f.parse_initial_release_rows,f._empty_initial_release_frame,'
        'f._realtime_windows,f._read_cached_window,f._fetch_window,f._run_collection),'
        'parameters={"schema_version":f.SCHEMA_VERSION,"request_contract_version":f.REQUEST_CONTRACT_VERSION}))')
    outputs = [subprocess.check_output([sys.executable, '-c', script], cwd=Path(__file__).resolve().parents[1],
        env={**os.environ, 'PYTHONHASHSEED': seed}, text=True, timeout=30).strip() for seed in ('1', '999')]
    assert outputs == [projection_identity()] * 2
