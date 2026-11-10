"""Physical catalog reuse must retain independent, exact logical versions."""
import errno
import os
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

from downloader.artifact_io import atomic_write_json
from stockagent.control import lakehouse
from stockagent.data_sync import catalog_incremental as incremental
from stockagent.data_sync.immutable_replication import seal, verify


def seed(tmp_path, files=None):
    source = tmp_path / 'mutable'
    source.mkdir()
    for name, payload in (files or {'a.parquet': b'original', 'nested/b.parquet': b'kept'}).items():
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    release = tmp_path / 'release'
    release.mkdir()
    expected, statistics = incremental.copy_catalog_data(source, release / 'data')
    manifest = seal(release, {'kind': 'ducklake_catalog_and_data', 'ducklake_snapshot_id': 1,
                              'producer_device_id': 'P', 'receiver_device_id': 'R'})
    assert expected == manifest['files']
    assert statistics['copied_bytes'] == sum(len(value) for value in (files or {'a': b'original', 'b': b'kept'}).values())
    return source, release, manifest


def test_seed_and_changed_versions_preserve_all_original_bytes(tmp_path):
    source, release, original = seed(tmp_path)
    assert not (release / 'data/a.parquet').stat().st_mode & 0o222
    assert not (source / 'a.parquet').samefile(release / 'data/a.parquet')
    (source / 'a.parquet').write_bytes(b'revision')  # same size, different bytes
    (source / 'extra.parquet').write_bytes(b'new')
    second = tmp_path / 'second'
    second.mkdir()
    expected, statistics = incremental.copy_catalog_data(source, second / 'data', previous=release)
    assert statistics['shared_files'] == 1
    assert statistics['copied_files'] == 2
    assert statistics['shared_bytes'] == 4
    assert statistics['copied_bytes'] == 11
    assert (second / 'data/nested/b.parquet').samefile(release / 'data/nested/b.parquet')
    assert not (second / 'data/a.parquet').samefile(release / 'data/a.parquet')
    assert seal(second, {'kind': 'ducklake_catalog_and_data'})['files'] == expected
    assert verify(release) == original
    assert (release / 'data/a.parquet').read_bytes() == b'original'
    (source / 'nested/b.parquet').unlink()
    third = tmp_path / 'third'
    third.mkdir()
    expected, _ = incremental.copy_catalog_data(source, third / 'data', previous=second)
    assert 'data/nested/b.parquet' not in expected
    assert (second / 'data/nested/b.parquet').read_bytes() == b'kept'
    verify(release)
    verify(second)


def test_unchanged_version_has_zero_payload_copies(tmp_path):
    source, release, _ = seed(tmp_path)
    (source / 'empty').mkdir()
    second = tmp_path / 'second'
    second.mkdir()
    expected, statistics = incremental.copy_catalog_data(source, second / 'data', previous=release)
    assert statistics['copied_bytes'] == 0
    assert statistics['shared_files'] == 2
    assert (second / 'data/empty').is_dir()
    assert seal(second, {'kind': 'ducklake_catalog_and_data'})['files'] == expected


def test_catalog_copy_reports_phase_counts_without_changing_the_copy(tmp_path):
    from stockagent.data_sync.work_progress import observe_work
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'one').write_bytes(b'first')
    (source / 'two').write_bytes(b'second')
    observed = []
    with observe_work(observed.append):
        expected, _ = incremental.copy_catalog_data(source, tmp_path / 'target')
    assert [row['completed_items'] for row in observed] == [0, 1, 2]
    assert all(row['phase'] == 'catalog_copy' and row['total_items'] == 2 for row in observed)
    assert set(expected) == {'data/one', 'data/two'}


def test_unlinking_one_immutable_alias_does_not_change_retained_version(tmp_path):
    source, release, original = seed(tmp_path)
    target = tmp_path / 'copy'
    incremental.copy_catalog_data(source, target, previous=release)
    alias = target / 'a.parquet'
    donor = release / 'data/a.parquet'
    assert alias.samefile(donor)
    alias.unlink()
    assert donor.read_bytes() == b'original'
    assert not donor.stat().st_mode & 0o222
    assert verify(release) == original


def test_writable_donor_is_copied_without_changing_old_permissions(tmp_path):
    source, release, _ = seed(tmp_path)
    donor = release / 'data/a.parquet'
    donor.chmod(0o644)
    second = tmp_path / 'second'
    second.mkdir()
    _, statistics = incremental.copy_catalog_data(source, second / 'data', previous=release)
    assert statistics['copied_files'] == 1
    assert not (second / 'data/a.parquet').samefile(donor)
    assert donor.stat().st_mode & 0o222


@pytest.mark.parametrize('error', [errno.EXDEV, errno.EOPNOTSUPP, errno.EPERM, errno.EMLINK])
def test_link_capability_failure_copies_exact_bytes(tmp_path, monkeypatch, error):
    source, release, _ = seed(tmp_path)
    def unavailable(*args, **kwargs):
        raise OSError(error, 'link unavailable')
    monkeypatch.setattr(incremental.os, 'link', unavailable)
    second = tmp_path / 'second'
    second.mkdir()
    expected, statistics = incremental.copy_catalog_data(source, second / 'data', previous=release)
    assert statistics['link_fallbacks'] == 2
    assert statistics['copied_files'] == 2
    assert seal(second, {'kind': 'ducklake_catalog_and_data'})['files'] == expected


def test_link_io_failure_is_not_hidden_as_a_capability_fallback(tmp_path, monkeypatch):
    source, release, _ = seed(tmp_path)
    def broken(*args, **kwargs):
        raise OSError(errno.EIO, 'storage failure')
    monkeypatch.setattr(incremental.os, 'link', broken)
    with pytest.raises(OSError, match='storage failure'):
        incremental.copy_catalog_data(source, tmp_path / 'copy', previous=release)
    assert not (tmp_path / 'copy/READY').exists()


def test_corrupt_donor_cannot_enter_a_new_version(tmp_path):
    source, release, _ = seed(tmp_path)
    (release / 'data/a.parquet').chmod(0o644)
    (release / 'data/a.parquet').write_bytes(b'bad data')
    (release / 'data/a.parquet').chmod(0o444)
    with pytest.raises(ValueError, match='donor bytes differ'):
        incremental.copy_catalog_data(source, tmp_path / 'copy', previous=release)
    assert not (tmp_path / 'copy/READY').exists()


def test_source_change_during_copy_fails_without_publishing(tmp_path, monkeypatch):
    source = tmp_path / 'mutable'
    source.mkdir()
    origin = source / 'a.parquet'
    origin.write_bytes(b'original')
    sync = incremental.os.fsync
    def change(fd):
        origin.write_bytes(b'revision')
        sync(fd)
    monkeypatch.setattr(incremental.os, 'fsync', change)
    with pytest.raises(ValueError, match='source changed'):
        incremental.copy_catalog_data(source, tmp_path / 'copy')
    assert not (tmp_path / 'copy/READY').exists()


def test_source_change_after_link_fails_final_complete_fence(tmp_path, monkeypatch):
    source, release, _ = seed(tmp_path)
    link = incremental.os.link
    def change(*args, **kwargs):
        link(*args, **kwargs)
        (source / 'a.parquet').write_bytes(b'revision')
    monkeypatch.setattr(incremental.os, 'link', change)
    with pytest.raises(ValueError, match='data changed'):
        incremental.copy_catalog_data(source, tmp_path / 'copy', previous=release)
    verify(release)


def test_wide_catalog_keeps_exact_revisions_and_zero_copy_unchanged_version(tmp_path):
    files = {f'year/{index:03}.parquet': f'raw,-{index},NULL,shares'.encode() for index in range(96)}
    source, release, original = seed(tmp_path, files)
    (source / 'empty-directory').mkdir()
    expected, statistics = incremental.copy_catalog_data(source, tmp_path / 'unchanged', previous=release)
    assert statistics['copied_bytes'] == 0 and statistics['shared_files'] == len(files)
    assert (tmp_path / 'unchanged/empty-directory').is_dir()
    assert expected == original['files']
    assert verify(release) == original
    (source / 'year/095.parquet').write_bytes(b'revised,-7,NULL,shares')
    second = tmp_path / 'revised'
    second.mkdir()
    expected, statistics = incremental.copy_catalog_data(source, second / 'data', previous=release)
    assert statistics['copied_files'] == 1 and statistics['shared_files'] == 95
    assert seal(second, original['context'])['files'] == expected
    assert verify(release) == original
    assert (release / 'data/year/095.parquet').read_bytes() == files['year/095.parquet']


@pytest.mark.parametrize('kind', ['file', 'directory', 'fifo'])
def test_wide_census_rejects_late_redirect_or_special_file_before_copy(tmp_path, kind):
    source = tmp_path / 'mutable'
    source.mkdir()
    for index in range(96):
        (source / f'{index:03}.parquet').write_bytes(b'preserved')
    invalid = source / 'zzz-invalid'
    if kind == 'fifo':
        os.mkfifo(invalid)
    else:
        invalid.symlink_to(source if kind == 'directory' else source / '000.parquet',
                           target_is_directory=kind == 'directory')
    with pytest.raises(ValueError):
        incremental.copy_catalog_data(source, tmp_path / 'destination')
    assert not (tmp_path / 'destination').exists()
    assert (source / '000.parquet').read_bytes() == b'preserved'


def test_wide_census_matches_the_independent_previous_complete_stat_contract(tmp_path):
    from scripts.benchmark_catalog_incremental import reference_files
    source = tmp_path / 'mutable'
    (source / 'empty').mkdir(parents=True)
    for index in range(97):
        (source / f'{index:03}.parquet').write_bytes(str(index).encode())
    assert incremental._files(source) == reference_files(source)


def test_wide_source_mutation_still_fails_complete_final_fence(tmp_path, monkeypatch):
    files = {f'{index:03}.parquet': b'original' for index in range(96)}
    source, release, original = seed(tmp_path, files)
    link = incremental.os.link
    def change(*args, **kwargs):
        link(*args, **kwargs)
        if Path(args[1]).name == '095.parquet':
            (source / '095.parquet').write_bytes(b'revision')
    monkeypatch.setattr(incremental.os, 'link', change)
    with pytest.raises(ValueError, match='data changed'):
        incremental.copy_catalog_data(source, tmp_path / 'copy', previous=release)
    assert verify(release) == original


@pytest.mark.parametrize('kind', ['file', 'directory', 'fifo', 'destination'])
def test_redirected_or_nonregular_source_does_not_publish(tmp_path, kind):
    source = tmp_path / 'mutable'
    source.mkdir()
    (source / 'good').write_bytes(b'data')
    target = tmp_path / 'copy'
    if kind == 'file':
        (source / 'bad').symlink_to(source / 'good')
    elif kind == 'directory':
        (source / 'bad').symlink_to(tmp_path, target_is_directory=True)
    elif kind == 'fifo':
        os.mkfifo(source / 'bad')
    else:
        target.symlink_to(source, target_is_directory=True)
    with pytest.raises(ValueError):
        incremental.copy_catalog_data(source, target)
    assert not (source / 'READY').exists()


def test_source_inode_is_never_used_even_if_a_donor_alias_points_to_it(tmp_path):
    source, release, _ = seed(tmp_path)
    origin = source / 'a.parquet'
    origin.unlink()
    os.link(release / 'data/a.parquet', origin)
    expected, statistics = incremental.copy_catalog_data(source, tmp_path / 'copy', previous=release)
    assert statistics['copied_files'] == 1
    assert not (tmp_path / 'copy/a.parquet').samefile(origin)
    assert expected['data/a.parquet']['bytes'] == 8


def test_donor_enrollment_requires_committed_snapshot_and_exact_root(tmp_path):
    source, release, manifest = seed(tmp_path)
    state, lake = tmp_path / 'state', tmp_path / 'lake'
    state.mkdir()
    (lake / 'releases').mkdir(parents=True)
    destination = lake / 'releases' / ('lake-' + manifest['identity_sha256'])
    release.rename(destination)
    c = {'state_root': str(state), 'lake_root': str(lake),
         'producer_device_id': 'P', 'receiver_device_id': 'R'}
    journal = state / 'export-1.json'
    row = {'state': 'publication_prepared', 'snapshot_id': 1,
           'delivery_identity_sha256': manifest['identity_sha256'], 'delivery_root': str(destination),
           'data_materialization': {'contract': incremental.CONTRACT}}
    atomic_write_json(journal, row)
    assert lakehouse._previous_catalog_export(c, 2) is None
    row['state'] = 'published'
    atomic_write_json(journal, row)
    assert lakehouse._previous_catalog_export(c, 2).root == destination
    assert lakehouse._previous_catalog_export(c, 1) is None
    row['delivery_root'] = str(source)
    atomic_write_json(journal, row)
    with pytest.raises(ValueError, match='scope differs'):
        lakehouse._previous_catalog_export(c, 2)


def test_worker_identity_enrolls_incremental_copy_implementation(monkeypatch):
    from scripts import manage_lakehouse as worker
    changed = {}
    monkeypatch.setattr(worker, 'digest', lambda path: changed.get(str(path.relative_to(worker.ROOT)), 'a' * 64))
    original = worker.worker_code_identity()
    changed['stockagent/data_sync/catalog_incremental.py'] = 'b' * 64
    assert worker.worker_code_identity() != original


def test_export_uses_previous_committed_data_and_keeps_original_retry_contract(tmp_path, monkeypatch):
    import shutil
    from stockagent.control import backup
    state, root, transport = tmp_path / 'state', tmp_path / 'lake', tmp_path / 'transport'
    state.mkdir()
    (root / 'data').mkdir(parents=True)
    (root / 'data/a.parquet').write_bytes(b'raw bytes')
    snapshot, notifications = [1], []

    class Connection:
        query = ''
        def execute(self, query):
            self.query = query
            return self
        def fetchone(self):
            return [snapshot[0] if 'max(snapshot_id)' in self.query else 1]
        def fetchall(self):
            return [(name,) for name in ('dataset_releases', 'source_objects', 'catalog_captures')]
        def close(self):
            pass

    class Database:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def execute(self, query):
            return SimpleNamespace(fetchone=lambda: ['same-mvcc-snapshot'])

    def replicate(source, destination, *args):
        shutil.copytree(source, destination)
        assert verify(source) == verify(destination)
        return {'verified': True}

    monkeypatch.setattr(lakehouse, 'guard', lambda c: None)
    monkeypatch.setattr(lakehouse, 'connect', lambda *args, **kwargs: Connection())
    monkeypatch.setattr(lakehouse, '_registered_table_digest', lambda *args: 'a' * 64)
    monkeypatch.setattr(lakehouse, 'replicate', replicate)
    monkeypatch.setattr(lakehouse, 'notify_transport', lambda c, paths: notifications.append(paths))
    monkeypatch.setattr(backup, 'logical_database_state', lambda db: {
        'identity_sha256': 'b' * 64, 'table_count': 1, 'row_count': 1})
    monkeypatch.setitem(sys.modules, 'psycopg', SimpleNamespace(connect=lambda **kwargs: Database()))
    monkeypatch.setattr(lakehouse.subprocess, 'run', lambda *args, **kwargs:
                        SimpleNamespace(returncode=0, stdout=b'pg logical dump'))
    c = {'state_root': str(state), 'lake_root': str(root), 'transport_root': str(transport),
         'binaries': str(tmp_path), 'catalog_database': 'isolated', 'catalog_role': 'isolated',
         'catalog_password': 'fixture-only', 'producer_device_id': 'P', 'receiver_device_id': 'R'}
    first = lakehouse.export(c, 1)
    assert first['state'] == 'published'
    assert first['data_materialization']['copied_bytes'] == 9
    snapshot[0] = 2
    second = lakehouse.export(c, 2)
    assert second['data_materialization']['copied_bytes'] == 0
    assert second['data_materialization']['shared_bytes'] == 9
    assert (Path(first['delivery_root']) / 'data/a.parquet').samefile(
        Path(second['delivery_root']) / 'data/a.parquet')
    assert lakehouse.export(c, 2) == second  # Exact committed retry, no new export.
    assert len(notifications) == 2
    assert (root / 'data/a.parquet').read_bytes() == b'raw bytes'
