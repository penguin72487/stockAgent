import fcntl
import errno
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3

import pytest

from stockagent.data_sync import benchmark_fixtures as fixtures
from stockagent.data_sync.benchmark_fixtures import ImmutableFixtureInputs, RECEIPT


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _capture(parent, name, sources):
    root = parent / name
    root.mkdir()
    with ImmutableFixtureInputs(root) as inputs:
        for label, source in sources.items():
            inputs.copy(source, label)
        receipt = inputs.commit()
    return root, receipt


def test_complete_sources_shared_but_mutable_original_never_hardlinked(tmp_path):
    source = tmp_path / "live.parquet"
    source.write_bytes(b"all rows, raw nulls and float bits\0" * 500)
    first, cold = _capture(tmp_path, "a", {"source/data.parquet": source})
    second, warm = _capture(tmp_path, "b", {"source/data.parquet": source})
    a, b = first / "source/data.parquet", second / "source/data.parquet"
    assert cold["copied_files"] == 1 and cold["reused_files"] == 0
    assert warm["copied_bytes"] == 0 and warm["reused_bytes"] == source.stat().st_size
    assert a.stat().st_ino == b.stat().st_ino != source.stat().st_ino
    assert a.stat().st_mode & 0o222 == 0
    original = b.read_bytes()
    source.write_bytes(b"new current source")
    assert a.read_bytes() == b.read_bytes() == original
    a.unlink()  # A donor name retiring does not strand the surviving experiment.
    assert b.read_bytes() == original


def test_update_copies_only_changed_source_and_preserves_full_manifest(tmp_path):
    source = tmp_path / "current"
    source.mkdir()
    a, b = source / "a", source / "b"
    a.write_bytes(b"unchanged history")
    b.write_bytes(b"old tail")
    old, _ = _capture(tmp_path, "old", {"a": a, "b": b})
    b.write_bytes(b"corrected tail of different length")
    new, result = _capture(tmp_path, "new", {"a": a, "b": b})
    assert result["reused_files"] == result["copied_files"] == 1
    assert result["copied_bytes"] == b.stat().st_size
    assert (old / "a").stat().st_ino == (new / "a").stat().st_ino
    assert (old / "b").read_bytes() == b"old tail"
    assert (new / "b").read_bytes() == b.read_bytes()
    assert {item["name"] for item in result["files"]} == {"a", "b"}


@pytest.mark.parametrize("defect", ["writable", "wrong_hash", "symlink", "missing", "busy", "corrupt_receipt"])
def test_unsafe_or_busy_donor_falls_back_to_independent_verified_copy(tmp_path, defect):
    source = tmp_path / "live"
    source.write_bytes(b"exact source")
    old, _ = _capture(tmp_path, "old", {"data": source})
    donor = old / "data"
    lock = None
    if defect == "writable":
        donor.chmod(0o600)
    elif defect == "wrong_hash":
        donor.chmod(0o600)
        donor.write_bytes(b"wrong source")
        donor.chmod(0o400)
    elif defect == "symlink":
        donor.unlink()
        donor.symlink_to(source)
    elif defect == "missing":
        donor.unlink()
    elif defect == "busy":
        lock = (old / ".immutable-inputs.lock").open("rb")
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    else:
        (old / RECEIPT).write_text("[]")
    try:
        new, result = _capture(tmp_path, "new", {"data": source})
    finally:
        if lock:
            lock.close()
    assert result["reused_files"] == 0 and result["copied_files"] == 1
    assert (new / "data").read_bytes() == source.read_bytes()
    assert (new / "data").stat().st_ino != source.stat().st_ino


def test_existing_sealed_public_fixture_reused_without_editing_its_receipt(tmp_path):
    source = tmp_path / "live"
    source.write_bytes(b"full retained history")
    old = tmp_path / "old"
    (old / "source").mkdir(parents=True)
    frozen = old / "source/input.parquet"
    frozen.write_bytes(source.read_bytes())
    frozen.chmod(0o400)
    receipt = {"contract": "tw_public_private_full_builder_fixture_v1", "source_files": [
        {"name": "input.parquet", "size": source.stat().st_size, "sha256": _sha(source)}]}
    (old / "fixture.json").write_text(json.dumps(receipt))
    before = (old / "fixture.json").read_bytes()
    new, result = _capture(tmp_path, "new", {"source/input.parquet": source})
    assert result["reused_files"] == 1
    assert frozen.stat().st_ino == (new / "source/input.parquet").stat().st_ino
    assert (old / "fixture.json").read_bytes() == before
    assert not (old / RECEIPT).exists()


def test_incomplete_or_failed_capture_cannot_seed_reuse(tmp_path):
    source = tmp_path / "live"
    source.write_bytes(b"source")
    partial = tmp_path / "partial"
    partial.mkdir()
    with pytest.raises(RuntimeError), ImmutableFixtureInputs(partial) as inputs:
        inputs.copy(source, "data")
        raise RuntimeError("interrupted before coherent source fence")
    assert not (partial / RECEIPT).exists()
    new, receipt = _capture(tmp_path, "new", {"data": source})
    assert receipt["reused_files"] == 0
    assert (new / "data").stat().st_ino != (partial / "data").stat().st_ino


@pytest.mark.parametrize("name", ["../outside", "/outside", "a/../outside", "./data", "a//b", ""])
def test_member_cannot_escape_or_alias_experiment(tmp_path, name):
    source = tmp_path / "live"
    source.write_bytes(b"source")
    root = tmp_path / "new"
    root.mkdir()
    with ImmutableFixtureInputs(root) as inputs, pytest.raises(ValueError):
        inputs.copy(source, name)


def test_rejects_wrong_source_hash_and_existing_destination(tmp_path):
    source = tmp_path / "live"
    source.write_bytes(b"source")
    root = tmp_path / "new"
    root.mkdir()
    with ImmutableFixtureInputs(root) as inputs:
        with pytest.raises(ValueError, match="source differs"):
            inputs.copy(source, "data", expected_sha256="0" * 64)
        assert not (root / "data").exists()
        (root / "occupied").write_bytes(b"keep")
        with pytest.raises(ValueError, match="already exists"):
            inputs.copy(source, "occupied")
        assert (root / "occupied").read_bytes() == b"keep"


def test_mutation_during_freeze_prevents_commit(tmp_path, monkeypatch):
    source = tmp_path / "live"
    source.write_bytes(b"first")
    root = tmp_path / "new"
    root.mkdir()
    copyfileobj = fixtures.shutil.copyfileobj
    def changing_copy(reader, writer, length):
        copyfileobj(reader, writer, length)
        source.write_bytes(b"other")
    monkeypatch.setattr(fixtures.shutil, "copyfileobj", changing_copy)
    with ImmutableFixtureInputs(root) as inputs, pytest.raises(ValueError):
        inputs.copy(source, "data")
    assert not (root / RECEIPT).exists()
    assert not list(root.glob("*.partial"))


def test_fifo_metadata_is_not_read_or_blocked(tmp_path):
    source = tmp_path / "live"
    source.write_bytes(b"source")
    donor = tmp_path / "untrusted"
    donor.mkdir()
    os.mkfifo(donor / RECEIPT)
    _, receipt = _capture(tmp_path, "new", {"data": source})
    assert receipt["reused_files"] == 0


def test_commit_rechecks_full_payload_and_readonly_contract(tmp_path):
    source = tmp_path / "live"
    source.write_bytes(b"first")
    root = tmp_path / "new"
    root.mkdir()
    with ImmutableFixtureInputs(root) as inputs:
        inputs.copy(source, "data")
        (root / "data").chmod(0o600)
        (root / "data").write_bytes(b"other")
        (root / "data").chmod(0o400)
        with pytest.raises(ValueError, match="changed before commit"):
            inputs.commit()
    assert not (root / RECEIPT).exists()


def test_frozen_donor_can_be_the_selected_source_without_false_mutation(tmp_path):
    source = tmp_path / "live"
    source.write_bytes(b"history")
    old, _ = _capture(tmp_path, "old", {"data": source})
    frozen = old / "data"
    new, receipt = _capture(tmp_path, "new", {"data": frozen})
    assert receipt["reused_files"] == 1
    assert frozen.stat().st_ino == (new / "data").stat().st_ino


def test_commit_seals_member_set_and_cannot_be_overwritten(tmp_path):
    source = tmp_path / "live"
    source.write_bytes(b"history")
    root = tmp_path / "new"
    root.mkdir()
    with ImmutableFixtureInputs(root) as inputs:
        inputs.copy(source, "data")
        inputs.commit()
        with pytest.raises(RuntimeError, match="already committed"):
            inputs.copy(source, "more")
        with pytest.raises(ValueError, match="already committed"):
            inputs.commit()
    with pytest.raises(ValueError, match="fresh experiment"):
        ImmutableFixtureInputs(root)


def test_public_capture_uses_shared_inputs_and_keeps_full_source_and_code_proof(tmp_path, monkeypatch):
    from scripts import benchmark_tw_public_feature_assembly as benchmark
    repo = tmp_path / "repo"
    (repo / "stockagent/data").mkdir(parents=True)
    (repo / "stockagent/data/tw_public_features.py").write_bytes(b"# fixed builder\n")
    benchmarks = repo / "artifacts/benchmarks"
    benchmarks.mkdir(parents=True)
    source = tmp_path / "source"
    (source / "stocks").mkdir(parents=True)
    (source / "stocks/2330_features.parquet").write_bytes(b"symbol names only")
    (source / "prices.parquet").write_bytes(b"full price history")
    (source / "features.parquet").write_bytes(b"full features")
    monkeypatch.setattr(benchmark, "ROOT", repo)
    monkeypatch.setattr(benchmark.shutil, "disk_usage", lambda _: shutil._ntuple_diskusage(100 << 30, 0, 100 << 30))
    first, second = benchmarks / "a", benchmarks / "b"
    benchmark.capture(first, source, source / "stocks")
    benchmark.capture(second, source, source / "stocks")
    receipt = benchmark._verified_fixture(second)
    assert receipt["storage_build"]["reused_files"] == 2
    assert receipt["storage_build"]["copied_bytes"] == 0
    assert len(receipt["source_files"]) == 2
    assert receipt["production_writes"] is False and receipt["providers_called"] == 0
    assert (first / "source/prices.parquet").stat().st_ino == (second / "source/prices.parquet").stat().st_ino


def test_openbb_capture_shares_readonly_data_but_never_mutable_sqlite(tmp_path, monkeypatch):
    from scripts import benchmark_openbb_metadata_audit as benchmark
    source = tmp_path / "source"
    (source / "_state").mkdir(parents=True)
    payload = source / "data.parquet"
    payload.write_bytes(b"full source table")
    with sqlite3.connect(source / "_state/openbb_archive.sqlite3") as conn:
        conn.execute("CREATE TABLE tasks(task_id TEXT,active INTEGER,plan_token TEXT,status TEXT,output_path TEXT,rows INTEGER,endpoint TEXT)")
        conn.execute("CREATE TABLE archive_meta(key TEXT,value TEXT)")
        conn.execute("INSERT INTO archive_meta VALUES('active_plan_token','test')")
        conn.execute("INSERT INTO tasks VALUES('one',1,'test','success',?,2,'macro')", (str(payload),))
    monkeypatch.setattr(benchmark.subprocess, "run", lambda *a, **kw: type("Git", (), {"stdout": b"# baseline\n"})())
    monkeypatch.setattr(benchmark.shutil, "disk_usage", lambda _: shutil._ntuple_diskusage(100 << 30, 0, 100 << 30))
    a, b = tmp_path / "a", tmp_path / "b"
    benchmark.prepare(source, a, "test")
    receipt = benchmark.prepare(source, b, "test")
    assert receipt["storage_build"]["reused_files"] == 1
    assert (a / "data/one.parquet").stat().st_ino == (b / "data/one.parquet").stat().st_ino
    db_a, db_b = a / "_state/openbb_archive.sqlite3", b / "_state/openbb_archive.sqlite3"
    assert db_a.stat().st_ino != db_b.stat().st_ino
    with sqlite3.connect(db_b) as conn:
        conn.execute("UPDATE tasks SET rows=999")
    with sqlite3.connect(db_a) as conn:
        assert conn.execute("SELECT rows FROM tasks").fetchone()[0] == 2


def test_target_publication_io_error_propagates_without_copied_fallback(tmp_path, monkeypatch):
    source = tmp_path / "live"
    source.write_bytes(b"source")
    _capture(tmp_path, "old", {"data": source})
    def disk_full(*args, **kwargs):
        raise OSError(errno.ENOSPC, "publication disk full")
    monkeypatch.setattr(fixtures, "link_immutable_payload", disk_full)
    root = tmp_path / "new"
    root.mkdir()
    with ImmutableFixtureInputs(root) as inputs, pytest.raises(OSError) as error:
        inputs.copy(source, "data")
    assert error.value.errno == errno.ENOSPC
    assert not (root / "data").exists() and not (root / RECEIPT).exists()


def test_busy_public_source_rejects_before_creating_empty_generation(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from scripts import benchmark_tw_public_feature_assembly as benchmark
    from stockagent.live import tw_public_source_guard as source_guard
    repo = tmp_path / "repo"
    parent = repo / "artifacts/benchmarks"
    parent.mkdir(parents=True)
    source = tmp_path / "source"
    source.mkdir()
    (source / "prices.parquet").write_bytes(b"source")
    monkeypatch.setattr(benchmark, "ROOT", repo)
    monkeypatch.setattr(benchmark.shutil, "disk_usage", lambda _: shutil._ntuple_diskusage(100 << 30, 0, 100 << 30))
    @contextmanager
    def busy(_root):
        raise source_guard.TWPublicSourceUpdatePending(source, source / "writer.lock")
        yield
    monkeypatch.setattr(source_guard, "live_tw_public_source_read_guard", busy)
    root = parent / "new"
    with pytest.raises(source_guard.TWPublicSourceUpdatePending):
        benchmark.capture(root, source, source)
    assert not root.exists()
