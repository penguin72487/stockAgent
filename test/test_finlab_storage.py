from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from stockagent.data import finlab_storage as storage


def _payload(root, name, values=(1., -2., None)):
    (root / "datasets").mkdir(parents=True, exist_ok=True)
    (root / "receipts").mkdir(exist_ok=True)
    staged = root / "datasets" / (name + ".tmp")
    pq.write_table(pa.table({"source_index": ["2014-01-01", "2014-01-02", "2014-01-03"],
                             "0050": values}), staged)
    digest = hashlib.sha256(staged.read_bytes()).hexdigest()
    path = staged.with_name(f"{name}-{digest[:24]}.parquet")
    staged.rename(path)
    receipt = {"dataset": name, "parquet_path": path.relative_to(root).as_posix(),
               "sha256": digest, "parquet_size_bytes": path.stat().st_size}
    (root / "receipts" / f"{name}.json").write_text(json.dumps(receipt))
    return path, receipt


def _run(root, receipts, **kwargs):
    return storage.compact_history(root, receipts,
        reference_probe=kwargs.pop("reference_probe", lambda *a, **kw: []), **kwargs)


def test_original_writer_shares_only_readonly_exact_bytes_and_preserves_values(tmp_path):
    root = tmp_path / "data_finlab"
    first, receipt = _payload(root, "first")
    # A writable donor is not silently made readonly by the producer.
    second = first.with_name(f"second-{receipt['sha256'][:24]}.parquet")
    staged = first.with_name("new.tmp")
    staged.write_bytes(first.read_bytes())
    assert storage.install_history_payload(staged, second, receipt["sha256"]) == "new_immutable_native_payload"
    assert not os.path.samefile(first, second)
    assert second.stat().st_mode & 0o222 == 0
    third = first.with_name(f"third-{receipt['sha256'][:24]}.parquet")
    staged.write_bytes(first.read_bytes())
    assert storage.install_history_payload(staged, third, receipt["sha256"]) == "shared_exact_native_payload"
    assert os.path.samefile(second, third)
    assert staged.is_file()  # The original writer's finally block owns its temp.
    assert pq.read_table(third).to_pydict()["0050"] == [1., -2., None]
    changed, _ = _payload(root, "changed", (2., -2., None))
    assert not os.path.samefile(changed, third)


def test_audit_and_apply_preserve_every_native_path_receipt_and_historical_version(tmp_path):
    root, receipts = tmp_path / "data_finlab", tmp_path / "migration"
    first, a = _payload(root, "first")
    second, b = _payload(root, "second")
    different, _ = _payload(root, "different", (7., -2., None))
    old = root / "versions/first/old.json"
    old.parent.mkdir(parents=True)
    old.write_text(json.dumps(a))
    before = {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}
    inode = second.stat().st_ino
    audit = _run(root, receipts)
    assert audit["duplicate_groups"] == 1 and audit["state"] == "audited"
    assert second.stat().st_ino == inode
    result = _run(root, receipts, apply=True, expected_fingerprint=audit["plan_fingerprint"])
    assert result["state"] == "shared" and result["replaced_paths"] == 1
    assert result["retired_allocated_bytes"] == result["candidate_allocated_bytes"] > 0
    assert result["allocated_before"] - result["allocated_after"] == result["retired_allocated_bytes"]
    assert result["historical_receipts_modified"] == result["logical_paths_removed"] == 0
    assert result["cold_objects_removed"] == result["nas_archive_files_removed"] == 0
    assert result["receipts_readback"] == "unchanged"
    assert all(p.read_bytes() == raw for p, raw in before.items())
    assert os.path.samefile(first, second) and not os.path.samefile(different, second)
    assert first.stat().st_mode & 0o222 == 0
    assert pq.read_table(root / b["parquet_path"]).to_pydict()["0050"] == [1., -2., None]
    assert _run(root, receipts, apply=True)["replaced_paths"] == 0


@pytest.mark.parametrize("phase", ["initial", "preapply", "retired"])
def test_readers_and_mmap_prevent_destructive_apply_or_reclaim_claim(tmp_path, phase):
    root, receipts = tmp_path / "source", tmp_path / "migration"
    first, _ = _payload(root, "first")
    second, _ = _payload(root, "second")
    calls = []
    def probe(targets, **kwargs):
        calls.append(targets)
        return ["pid=7:maps-inode:1:2"] if len(calls) == {"initial": 1, "preapply": 2, "retired": 3}[phase] else []
    result = _run(root, receipts, apply=True, reference_probe=probe)
    assert result["retired_allocated_bytes"] == 0
    if phase == "retired":
        assert result["state"] == "shared" and result["retired_inode_references"]
    else:
        assert result["state"] == "deferred" and not os.path.samefile(first, second)


def test_native_sync_owner_lock_is_retained_and_nonblocking(tmp_path):
    root, receipts = tmp_path / "source", tmp_path / "migration"
    _payload(root, "first"), _payload(root, "second")
    lock = root / ".sync.lock"
    with lock.open("w") as stream:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        inode = lock.stat().st_ino
        result = _run(root, receipts, apply=True)
    assert result["state"] == "deferred" and result["reason"] == "native_sync_owner_busy"
    assert lock.stat().st_ino == inode


def test_unknown_hardlink_owner_is_retained_not_chmodded(tmp_path):
    root, receipts = tmp_path / "source", tmp_path / "migration"
    first, _ = _payload(root, "first")
    second, _ = _payload(root, "second")
    alias = tmp_path / "another-owner.parquet"
    os.link(first, alias)
    result = _run(root, receipts, apply=True)
    assert result["external_link_inodes_retained"] == 1 and result["replaced_paths"] == 0
    assert not os.path.samefile(first, second) and alias.stat().st_mode & 0o200


@pytest.mark.parametrize("damage", ["sha", "symlink", "receipt", "plan"])
def test_corruption_or_stale_plan_never_replaces_originals(tmp_path, damage):
    root, receipts = tmp_path / "source", tmp_path / "migration"
    first, _ = _payload(root, "first")
    second, _ = _payload(root, "second")
    if damage == "sha":
        raw = second.read_bytes()
        second.write_bytes(raw[:-1] + bytes([raw[-1] ^ 1]))
    elif damage == "symlink":
        second.unlink()
        second.symlink_to(first)
    elif damage == "receipt":
        (root / "receipts/second.json").unlink()
        (root / "receipts/second.json").symlink_to(root / "receipts/first.json")
    with pytest.raises(ValueError):
        _run(root, receipts, apply=True, expected_fingerprint="0" * 64 if damage == "plan" else None)
    assert first.stat().st_mode & 0o200
    assert json.loads(next(receipts.glob("*.result.json")).read_text())["replaced_paths"] == 0


def test_receipt_change_during_audit_fails_before_sharing(tmp_path, monkeypatch):
    root, receipts = tmp_path / "source", tmp_path / "migration"
    first, _ = _payload(root, "first")
    second, _ = _payload(root, "second")
    original = storage.stable_payload_hash
    def mutate(path, expected):
        digest = original(path, expected)
        control = root / "receipts/first.json"
        control.write_text(control.read_text() + " ")
        return digest
    monkeypatch.setattr(storage, "stable_payload_hash", mutate)
    with pytest.raises(ValueError, match="changed during exact audit"):
        _run(root, receipts, apply=True)
    assert not os.path.samefile(first, second)


def test_legacy_missing_size_requires_exact_sha_and_keeps_receipt_bytes(tmp_path):
    root, receipts = tmp_path / "source", tmp_path / "migration"
    first, _ = _payload(root, "first")
    second, value = _payload(root, "second")
    value.pop("parquet_size_bytes")
    path = root / "receipts/second.json"
    path.write_text(json.dumps(value))
    before = path.read_bytes()
    assert _run(root, receipts, apply=True)["state"] == "shared"
    assert path.read_bytes() == before and os.path.samefile(first, second)


def test_bounded_wait_uses_original_lock_and_does_not_interrupt_writer(tmp_path):
    root, receipts = tmp_path / "source", tmp_path / "migration"
    _payload(root, "first"), _payload(root, "second")
    with (root / ".sync.lock").open("w") as stream:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = _run(root, receipts, apply=True, wait_owner_seconds=0.02)
    assert result["state"] == "deferred" and result["replaced_paths"] == 0
    with pytest.raises(ValueError, match="wait must be finite"):
        _run(root, receipts, apply=True, wait_owner_seconds=float("nan"))
