from contextlib import contextmanager
import hashlib
import io
import os
from types import SimpleNamespace
import zipfile

import pytest

from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync import windows_cold_io as native
from stockagent.data_sync import training_return
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture(autouse=True)
def isolated_physical_capacity(monkeypatch):
    monkeypatch.setattr(training_return, "admit_workspace", lambda *_: {})


def sample(tmp_path):
    path = tmp_path / "pack.zip"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("tiny/a.json", b'{"value":1}')
        archive.writestr("many/b.npy", b"retained bytes" * 1000)
    return path, {"bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


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
    with pytest.raises(SnapshotError, match="space"):
        with packed._native_pack_source(path, item, enabled=True):
            pytest.fail("insufficient scratch admitted")
    monkeypatch.setattr(packed.shutil, "disk_usage", lambda _: SimpleNamespace(free=64 * 1024**3))
    @contextmanager
    def reader(source):
        with source.open("rb") as handle:
            yield handle
    monkeypatch.setattr(native, "binary_reader", reader)
    with pytest.raises(SnapshotError, match="changed during"):
        with packed._native_pack_source(path, item, enabled=True):
            os.utime(path, ns=(1, 1))


@pytest.mark.parametrize("value", [True, 1, "true"])
def test_native_pack_flag_cannot_escape_canonical_d_guard(tmp_path, value):
    with pytest.raises(SnapshotError):
        packed.verify_packed_snapshot(tmp_path, None, d_primary_native_pack_reads=value)


def test_default_pack_reader_keeps_existing_file_path(tmp_path):
    path, item = sample(tmp_path)
    with packed._native_pack_source(path, item, enabled=False) as source:
        assert source == path


def test_native_pack_physical_capacity_cannot_use_virtual_free_space(tmp_path, monkeypatch):
    path, item = sample(tmp_path)
    monkeypatch.setattr(packed.shutil, "disk_usage", lambda _: SimpleNamespace(free=64 * 1024**3))
    def reject(*_):
        raise SnapshotError("physical backing drive lacks required reserve")
    monkeypatch.setattr(training_return, "admit_workspace", reject)
    with pytest.raises(SnapshotError, match="physical"):
        with packed._native_pack_source(path, item, enabled=True):
            pytest.fail("virtual capacity waived physical admission")
