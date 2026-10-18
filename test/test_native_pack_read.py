from contextlib import contextmanager
import hashlib
import gzip
import io
import os
from pathlib import Path
from types import SimpleNamespace
import zipfile

import pytest

from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync import windows_cold_io as native
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture(autouse=True)
def isolated_physical_capacity(monkeypatch):
    monkeypatch.setattr(native, "admit_workspace", lambda *_: {})


def sample(tmp_path):
    path = tmp_path / "pack.zip"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("tiny/a.json", b'{"value":1}')
        archive.writestr("many/b.npy", b"retained bytes" * 1000)
    return path, {"bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


@pytest.mark.parametrize('root,large,use_native', [
    ('/srv/stockagent-packed', True, True),
    ('/srv/stockagent-packed/.local-state/private-history', True, True),
    ('/srv/stockagent-packed', False, False),
    ('/elsewhere/cold', True, False),
])
def test_inventory_uses_existing_native_adapter_only_for_large_enrolled_D(tmp_path, monkeypatch, root, large, use_native):
    path = tmp_path / 'inventory.jsonl.gz'
    payload = gzip.compress(b'{"path":"raw/values.parquet","kind":"file"}\n')
    path.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    # Byte-reader internals are independently tested below. This dispatch test
    # substitutes only that verified boundary, without allocating a large file.
    descriptor = {'bytes': 8 * 1024**2 if large else 1, 'sha256': digest,
                  'relpath': 'objects/inventories/aa/test.jsonl.gz'}
    manifest = {'archive': {'inventory': descriptor}}
    monkeypatch.setattr(packed, '_path_under', lambda *_: path)
    real_is_file = Path.is_file
    marker = Path('/srv/stockagent-packed/.stockagent-d-primary')
    monkeypatch.setattr(Path, 'is_file', lambda value: True if value == marker else real_is_file(value))
    hashes, native_calls = [], []
    def full_hash(source):
        hashes.append(source)
        return digest
    @contextmanager
    def verified_source(source, item, **options):
        native_calls.append((source, item, options))
        yield io.BytesIO(payload)
    monkeypatch.setattr(packed, 'sha256_file', full_hash)
    monkeypatch.setattr(packed, '_native_pack_source', verified_source)
    rows = packed._load_inventory(Path(root), manifest)
    assert rows == [{'path': 'raw/values.parquet', 'kind': 'file'}]
    assert bool(native_calls) is use_native
    assert hashes == ([] if use_native else [path])
    if use_native:
        assert native_calls == [(path, {'bytes': descriptor['bytes'], 'sha256': digest},
                                 {'enabled': True, 'prefer_memory': False})]


def test_native_pack_sha_precedes_zip_and_scratch_is_unlinked(tmp_path, monkeypatch):
    path, item = sample(tmp_path)
    monkeypatch.setattr(packed.shutil, "disk_usage", lambda _: SimpleNamespace(free=64 * 1024**3))
    calls = []
    @contextmanager
    def reader(source):
        calls.append(source)
        with source.open("rb") as handle:
            yield handle
    monkeypatch.setattr(native, "binary_reader", reader)
    with packed._native_pack_source(path, item, enabled=True) as source:
        fd = source.fileno()
        assert os.fstat(fd).st_nlink == 0
        with zipfile.ZipFile(source) as archive:
            assert archive.testzip() is None
            assert archive.read("tiny/a.json") == b'{"value":1}'
    assert source.closed and calls == [path] and path.exists()


def test_native_pack_rejects_corrupt_native_pipe_before_zip(tmp_path, monkeypatch):
    path, item = sample(tmp_path)
    monkeypatch.setattr(packed.shutil, "disk_usage", lambda _: SimpleNamespace(free=64 * 1024**3))
    @contextmanager
    def corrupt(_):
        yield io.BytesIO(b"x" * item["bytes"])
    monkeypatch.setattr(native, "binary_reader", corrupt)
    with pytest.raises(SnapshotError, match="checksum"):
        with packed._native_pack_source(path, item, enabled=True):
            pytest.fail("unverified pack was exposed")


def test_native_pack_checks_ram_disk_reserve_and_mutation(tmp_path, monkeypatch):
    path, item = sample(tmp_path)
    monkeypatch.setattr(packed.shutil, "disk_usage", lambda _: SimpleNamespace(free=32 * 1024**3))
    with packed._native_pack_source(path, item, enabled=True) as source:
        assert source == path
        with zipfile.ZipFile(source) as archive:
            assert archive.testzip() is None
            assert archive.read("tiny/a.json") == b'{"value":1}'
    monkeypatch.setattr(packed.shutil, "disk_usage", lambda _: SimpleNamespace(free=64 * 1024**3))
    @contextmanager
    def reader(source):
        with source.open("rb") as handle:
            yield handle
    monkeypatch.setattr(native, "binary_reader", reader)
    with pytest.raises(SnapshotError, match="changed during"):
        with packed._native_pack_source(path, item, enabled=True):
            os.utime(path, ns=(1, 1))


@pytest.mark.parametrize("problem", [None, "corrupt", "changed-during-hash", "changed-during-use"])
def test_pressure_fallback_has_no_scratch_and_keeps_full_sha_and_generation(tmp_path, monkeypatch, problem):
    path, item = sample(tmp_path)
    monkeypatch.setattr(packed.shutil, "disk_usage", lambda _: SimpleNamespace(free=1))
    monkeypatch.setattr(packed.tempfile, "TemporaryFile", lambda **_: pytest.fail("pressure fallback wrote scratch"))
    monkeypatch.setattr(native, "admit_workspace", lambda *_: pytest.fail("pressure fallback admitted SSD scratch"))
    original_hash = native.hash_file
    hashed = []

    def full_hash(source):
        hashed.append(source)
        value = original_hash(source)
        if problem == "changed-during-hash":
            os.utime(source, ns=(1, 1))
        return value

    monkeypatch.setattr(native, "hash_file", full_hash)
    if problem == "corrupt":
        path.write_bytes(b"X" * item["bytes"])

    def read():
        with packed._native_pack_source(path, item, enabled=True) as source:
            assert source == path and hashed == [path]
            with zipfile.ZipFile(source) as archive:
                assert archive.testzip() is None
                assert archive.read("tiny/a.json") == b'{"value":1}'
            if problem == "changed-during-use":
                os.utime(path, ns=(1, 1))

    if problem:
        with pytest.raises(SnapshotError, match="checksum|signature|changed during"):
            read()
    else:
        read()
    assert path.exists() and hashed == [path]


@pytest.mark.parametrize("value", [True, 1, "true"])
def test_native_pack_flag_cannot_escape_canonical_d_guard(tmp_path, value):
    with pytest.raises(SnapshotError):
        packed.verify_packed_snapshot(tmp_path, None, d_primary_native_pack_reads=value)


def test_native_pack_authority_rechecks_original_primary_mount(monkeypatch):
    from stockagent.data_sync import cold_primary
    calls = []
    primary = Path('/srv/stockagent-packed')
    monkeypatch.setattr(cold_primary, '_check_d_primary_mount', lambda root: calls.append(root))
    packed._check_native_pack_read_authority(primary)
    assert calls == [primary]


def test_native_pack_private_authority_reuses_existing_local_only_owner(monkeypatch):
    from stockagent.data_sync import dataset_migration as migration
    primary = Path('/srv/stockagent-packed')
    private = primary / '.local-state/private-history'
    calls = []
    def owner(parent, policy, *, initialize):
        calls.append((parent, policy, initialize))
        return private
    monkeypatch.setattr(migration, 'private_history_root', owner)
    packed._check_native_pack_read_authority(private)
    assert calls == [(primary, {'private_history': {
        'enabled': True, 'relative_root': '.local-state/private-history',
        'shared_export': False, 'nas_export': False, 'source_retirement': False}}, False)]


@pytest.mark.parametrize('problem', ['owner-error', 'wrong-root'])
def test_native_private_authority_cannot_waive_original_privacy_gate(monkeypatch, problem):
    from stockagent.data_sync import dataset_migration as migration
    def owner(*args, **options):
        if problem == 'owner-error':
            raise SnapshotError('original private owner/permissions/marker differs')
        return Path('/elsewhere/private-history')
    monkeypatch.setattr(migration, 'private_history_root', owner)
    with pytest.raises(SnapshotError, match='owner/permissions/marker|authority differs'):
        packed._check_native_pack_read_authority(Path('/srv/stockagent-packed/.local-state/private-history'))


@pytest.mark.parametrize('root', [
    '/srv/stockagent-packed/.local-state/another-history',
    '/srv/stockagent-packed/.local-state/private-history/child',
    '/elsewhere/private-history',
])
def test_native_pack_private_scope_does_not_enroll_other_directories(root):
    with pytest.raises(SnapshotError, match='restricted to canonical D authority'):
        packed._check_native_pack_read_authority(Path(root))


def test_native_private_pack_permission_does_not_enable_native_blob_reads():
    with pytest.raises(SnapshotError, match='restricted to canonical D authority'):
        packed.verify_packed_snapshot(Path('/srv/stockagent-packed/.local-state/private-history'), None,
                                      d_primary_native_pack_reads=True, d_primary_native_blob_reads=True)


def test_default_pack_reader_keeps_existing_file_path(tmp_path):
    path, item = sample(tmp_path)
    with packed._native_pack_source(path, item, enabled=False) as source:
        assert source == path


def test_small_many_member_pack_recovers_all_members_without_native_scratch(tmp_path, monkeypatch):
    path = tmp_path / "many-member-pack.zip"
    originals = {f"source/{i:03d}.json": f'{{"value":{i}}}\n'.encode() for i in range(67)}
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in originals.items():
            archive.writestr(name, payload)
    item = {"bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "file_count": len(originals)}
    monkeypatch.setattr(native, "binary_reader", lambda *_: pytest.fail("small pack launched native reader"))
    monkeypatch.setattr(native, "admit_workspace", lambda *_: pytest.fail("memory read wrote SSD scratch"))
    with packed._native_pack_source(path, item, enabled=True) as source:
        assert isinstance(source, io.BytesIO)
        with zipfile.ZipFile(source) as archive:
            assert archive.testzip() is None
            assert {name: archive.read(name) for name in archive.namelist()} == originals
    assert source.closed and path.exists()


def test_native_pack_physical_capacity_cannot_use_virtual_free_space(tmp_path, monkeypatch):
    path, item = sample(tmp_path)
    monkeypatch.setattr(packed.shutil, "disk_usage", lambda _: SimpleNamespace(free=64 * 1024**3))
    def reject(*_):
        raise SnapshotError("physical backing drive lacks required reserve")
    monkeypatch.setattr(native, "admit_workspace", reject)
    with pytest.raises(SnapshotError, match="physical"):
        with packed._native_pack_source(path, item, enabled=True):
            pytest.fail("virtual capacity waived physical admission")
