"""Producer fencing and storage-accounting reuse; never provider requests."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from stockagent.live import shioaji_storage_epoch as epoch
from stockagent.live import shioaji_storage_monitor as monitor


def _fixture(tmp_path):
    parent = tmp_path / 'history'
    root = parent / 'TXFR1'
    with epoch.history_storage_writer(root):
        ticks = root / 'ticks' / 'trading_date=2026-10-01'
        ticks.mkdir(parents=True)
        (ticks / 'data.parquet').write_bytes(b'original')
        (root / 'manifest.json').write_text('{}')
    cache = tmp_path / 'cache'
    cache.mkdir(mode=0o700)
    spec = monitor.StorageDatasetSpec('futures_history', 'history', 'source', 'historical',
                                      (parent,), 'test')
    return root, cache, spec


def _scan(spec, cache=None, *, now=None, force=False):
    return monitor._scan_dataset(spec, now=now or datetime.now(UTC),
                                 inventory_cache=cache, force_full_scan=force)


def _values(row):
    return {key: value for key, value in row.items() if key != 'inventory_reuse'}


def test_writer_retains_lock_inode_and_invalidates_even_failed_inplace_write(tmp_path):
    root, _, _ = _fixture(tmp_path)
    lock = root / 'download.lock'
    inode, original = lock.stat().st_ino, lock.read_bytes()
    with epoch.history_storage_observation(root) as observed:
        assert observed['epoch']['contract'] == epoch.HISTORY_STORAGE_EPOCH
        with pytest.raises(RuntimeError, match='holds the lock'):
            with epoch.history_storage_writer(root):
                pytest.fail('writer ran during a reader lease')
    with pytest.raises(ValueError, match='interrupted'):
        with epoch.history_storage_writer(root):
            with epoch.history_storage_observation(root) as busy:
                assert busy is None
            (root / 'manifest.json').write_text('changed')
            raise ValueError('interrupted')
    assert lock.stat().st_ino == inode
    assert lock.read_bytes() != original
    with epoch.history_storage_observation(root) as observed:
        assert observed is not None


@pytest.mark.parametrize('kind', ['empty', 'invalid', 'oversize', 'symlink', 'hardlink', 'writable'])
def test_untrusted_legacy_lock_never_authorizes_reuse(tmp_path, kind):
    root = tmp_path / 'TXFR1'
    root.mkdir()
    lock = root / 'download.lock'
    lock.write_bytes(b'' if kind == 'empty' else b'{invalid')
    if kind == 'oversize':
        lock.write_bytes(b' ' * (epoch.MAX_EPOCH_BYTES + 1))
    elif kind in {'symlink', 'hardlink'}:
        other = root / 'other'
        lock.rename(other)
        lock.symlink_to(other) if kind == 'symlink' else os.link(other, lock)
    elif kind == 'writable':
        lock.chmod(0o666)
    with epoch.history_storage_observation(root) as observed:
        assert observed is None


def test_writer_rejects_redirected_ancestor_before_creating_external_directory(tmp_path):
    external = tmp_path / 'external'
    external.mkdir()
    alias = tmp_path / 'alias'
    alias.symlink_to(external, target_is_directory=True)
    with pytest.raises(ValueError, match='redirected'):
        with epoch.history_storage_writer(alias / 'new' / 'TXFR1'):
            pytest.fail('redirected writer entered')
    assert list(external.iterdir()) == []


def test_failed_epoch_fsync_cannot_enter_source_writer(tmp_path, monkeypatch):
    def failure(_):
        raise OSError('fsync failure')
    monkeypatch.setattr(epoch.os, 'fsync', failure)
    with pytest.raises(OSError, match='fsync failure'):
        with epoch.history_storage_writer(tmp_path / 'TXFR1'):
            pytest.fail('data writes permitted before durable mutation marker')


def test_cached_census_exact_and_unchanged_roots_skip_all_source_stats(tmp_path, monkeypatch):
    root, cache, spec = _fixture(tmp_path)
    now = datetime.now(UTC)
    original = _scan(spec, now=now)
    first = _scan(spec, cache, now=now)
    assert _values(first) == original
    assert first['inventory_reuse']['scanned_epoch_roots'] == 1
    def forbidden(_):
        pytest.fail('reused history performed another per-file metadata scan')
    monkeypatch.setattr(monitor, '_iter_file_metadata', forbidden)
    reused = _scan(spec, cache, now=now)
    assert _values(reused) == original
    assert reused['inventory_reuse']['avoided_file_stats'] == original['files']
    assert reused['inventory_reuse']['reused_epoch_roots'] == 1
    assert root.joinpath('ticks/trading_date=2026-10-01/data.parquet').read_bytes() == b'original'


def test_cached_daily_groups_preserve_taipei_midnight_rollover(tmp_path):
    root, cache, spec = _fixture(tmp_path)
    now = datetime(2026, 10, 16, 15, 59, tzinfo=UTC)
    mtimes = [datetime(2026, 9, 15, 15, 59, 59, tzinfo=UTC),
              datetime(2026, 9, 15, 16, tzinfo=UTC),
              datetime(2026, 10, 15, 15, 59, 59, tzinfo=UTC),
              datetime(2026, 10, 15, 16, tzinfo=UTC),
              datetime(2026, 10, 16, 16, tzinfo=UTC)]
    with epoch.history_storage_writer(root):
        for index, stamp in enumerate(mtimes):
            path = root / f'{index}.parquet'
            path.write_bytes(b'x' * (index + 1))
            os.utime(path, (stamp.timestamp(), stamp.timestamp()))
    first = _scan(spec, cache, now=now)
    assert _values(first) == _scan(spec, now=now)
    for advance in (timedelta(minutes=2), timedelta(hours=23)):
        after = now + advance
        reused = _scan(spec, cache, now=after)
        assert reused['inventory_reuse']['reused_epoch_roots'] == 1
        assert _values(reused) == _scan(spec, now=after)


@pytest.mark.parametrize('mutation', ['repair', 'delete', 'add', 'manifest'])
def test_canonical_mutation_invalidates_only_changed_generation(tmp_path, mutation):
    root, cache, spec = _fixture(tmp_path)
    _scan(spec, cache)
    path = root / 'ticks/trading_date=2026-10-01/data.parquet'
    with epoch.history_storage_writer(root):
        if mutation == 'repair':
            path.write_bytes(b'repaired original values')
        elif mutation == 'delete':
            path.unlink()
        elif mutation == 'add':
            (root / 'new.parquet').write_bytes(b'new')
        else:
            (root / 'manifest.json').write_text('{"new":true}')
    now = datetime.now(UTC)
    updated = _scan(spec, cache, now=now)
    assert updated['inventory_reuse']['scanned_epoch_roots'] == 1
    assert _values(updated) == _scan(spec, now=now)


@pytest.mark.parametrize('corruption', ['checksum', 'shape', 'symlink', 'oversize', 'ttl', 'force'])
def test_bad_or_expired_cache_reconciles_source_metadata(tmp_path, corruption):
    root, cache, spec = _fixture(tmp_path)
    now = datetime.now(UTC)
    _scan(spec, cache, now=now)
    path, = cache.iterdir()
    if corruption == 'checksum':
        raw = json.loads(path.read_text())
        raw['groups'][0][0] += 1
        path.write_text(json.dumps(raw))
    elif corruption == 'shape':
        raw = json.loads(path.read_text())
        raw.pop('sha256')
        raw['groups'] = [[True, 0, 0]]
        raw['sha256'] = monitor._inventory_digest(raw)
        path.write_text(json.dumps(raw))
    elif corruption == 'symlink':
        other = tmp_path / 'other'
        other.write_bytes(path.read_bytes())
        path.unlink()
        path.symlink_to(other)
    elif corruption == 'oversize':
        path.write_bytes(b' ' * (8 * 1024 * 1024 + 1))
    elif corruption == 'ttl':
        now += timedelta(seconds=monitor.INVENTORY_RECHECK_SECONDS)
    updated = _scan(spec, cache, now=now, force=corruption == 'force')
    assert updated['inventory_reuse']['scanned_epoch_roots'] == 1
    assert _values(updated) == _scan(spec, now=now)


def test_scan_never_holds_reader_lock_and_never_caches_a_changed_epoch(tmp_path, monkeypatch):
    root, cache, spec = _fixture(tmp_path)
    raw_scan = monitor._iter_file_metadata
    def racing_scan(path):
        with epoch.history_storage_writer(root):
            (root / 'manifest.json').write_text('updated during full scan')
        yield from raw_scan(path)
    monkeypatch.setattr(monitor, '_iter_file_metadata', racing_scan)
    updated = _scan(spec, cache)
    assert updated['inventory_reuse']['changed_during_scan_roots'] == 1
    assert list(cache.iterdir()) == []


def test_busy_writer_uses_original_full_scan_without_caching(tmp_path):
    root, cache, spec = _fixture(tmp_path)
    _scan(spec, cache)
    with epoch.history_storage_writer(root):
        now = datetime.now(UTC)
        original = _scan(spec, now=now)
        updated = _scan(spec, cache, now=now)
        assert _values(updated) == original
        assert updated['inventory_reuse']['reused_epoch_roots'] == 0
        assert updated['inventory_reuse']['legacy_or_busy_roots'] == 1


def test_only_changed_contract_rescans_and_force_detects_out_of_protocol_repair(tmp_path):
    root, cache, spec = _fixture(tmp_path)
    other = root.parent / 'MTXFR1'
    with epoch.history_storage_writer(other):
        (other / 'original.parquet').write_bytes(b'other contract')
    _scan(spec, cache)
    path = root / 'ticks/trading_date=2026-10-01/data.parquet'
    with epoch.history_storage_writer(root):
        path.write_bytes(b'canonical repair')
    updated = _scan(spec, cache)
    assert updated['inventory_reuse']['scanned_epoch_roots'] == 1
    assert updated['inventory_reuse']['reused_epoch_roots'] == 1
    # Manual in-place writes must request reconciliation, not pretend that
    # directory mtime or a producer protocol attests arbitrary operator edits.
    path.write_bytes(b'out-of-protocol repair, force full metadata reconciliation')
    now = datetime.now(UTC)
    forced = _scan(spec, cache, now=now, force=True)
    assert _values(forced) == _scan(spec, now=now)
    assert forced['inventory_reuse']['scanned_epoch_roots'] == 2


def test_failed_full_epoch_scan_cannot_publish_a_partial_reusable_cache(tmp_path, monkeypatch):
    _, cache, spec = _fixture(tmp_path)
    def broken_scan(_):
        yield 10, datetime.now(UTC).timestamp()
        raise RuntimeError('source scan failed')
    monkeypatch.setattr(monitor, '_iter_file_metadata', broken_scan)
    with pytest.raises(RuntimeError, match='source scan failed'):
        _scan(spec, cache)
    assert list(cache.iterdir()) == []


def test_metadata_cache_write_failure_does_not_discard_a_complete_census(tmp_path, monkeypatch):
    _, cache, spec = _fixture(tmp_path)
    now = datetime.now(UTC)
    expected = _scan(spec, now=now)
    def broken_write(*_, **__):
        raise OSError('metadata cache unavailable')
    monkeypatch.setattr(monitor, 'atomic_write_bytes', broken_write)
    measured = _scan(spec, cache, now=now)
    assert _values(measured) == expected
    assert measured['inventory_reuse']['cache_write_errors'] == 1


def test_benchmark_source_identity_detects_canonical_mutation_and_new_contract(tmp_path):
    from scripts.benchmark_shioaji_storage_epochs import _source_token
    root, _, spec = _fixture(tmp_path)
    original = _source_token(spec, monitor)
    assert _source_token(spec, monitor) == original
    with epoch.history_storage_writer(root):
        (root / 'manifest.json').write_text('new')
    changed = _source_token(spec, monitor)
    assert changed != original
    with epoch.history_storage_writer(root.parent / 'MTXFR1'):
        pass
    assert _source_token(spec, monitor) != changed


def test_enrollment_preserves_source_manifest_and_receipts_without_login(tmp_path, monkeypatch):
    from downloader import download_shioaji_tx_futures_ticks as downloader
    parent = tmp_path / 'history'
    root = parent / 'TXFR1'
    root.mkdir(parents=True)
    (root / 'manifest.json').write_text(json.dumps({'dataset': downloader.SOURCE, 'contract': 'TXFR1'}))
    (root / 'download.lock').touch(mode=0o600)
    (root / 'original.parquet').write_bytes(b'original data')
    hashes = {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
              for path in root.iterdir() if path.name != 'download.lock'}
    args = SimpleNamespace(dry_run=True, prepare_storage_epochs=True, history_root=parent,
                           tx_history_root=tmp_path / 'other/TXFR1', batch_receipt=tmp_path / 'enrollment.json')
    # Any accidental acquisition attempt must fail rather than using credentials.
    monkeypatch.setattr(downloader, '_run_batch', lambda *_: pytest.fail('provider path entered'))
    monkeypatch.setattr(downloader, '_active_history_processes', lambda: [])
    assert downloader.run_batch(args) == 0
    assert (root / 'download.lock').read_bytes() == b''
    args.dry_run = False
    assert downloader.run_batch(args) == 0
    receipt = json.loads(args.batch_receipt.read_text())
    assert receipt['provider_calls'] == receipt['logins'] == receipt['data_or_receipt_writes'] == 0
    assert receipt['roots'][0]['state'] == 'enrolled'
    assert all(hashlib.sha256((root / name).read_bytes()).hexdigest() == digest
               for name, digest in hashes.items())
    with epoch.history_storage_observation(root) as observed:
        assert observed is not None


def test_enrollment_defers_busy_batch_or_frozen_writer_without_source_mutation(tmp_path, monkeypatch):
    import fcntl
    from downloader import download_shioaji_tx_futures_ticks as downloader
    parent = tmp_path / 'history'
    parent.mkdir()
    args = SimpleNamespace(dry_run=False, prepare_storage_epochs=True, history_root=parent,
                           tx_history_root=tmp_path / 'other/TXFR1', batch_receipt=tmp_path / 'enrollment.json')
    with (parent / 'batch.lock').open('a+b') as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert downloader.run_batch(args) == 75
    monkeypatch.setattr(downloader, '_active_history_processes', lambda: [123456])
    assert downloader.run_batch(args) == 75
    assert not args.batch_receipt.exists()
