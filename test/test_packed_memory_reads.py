"""Cold originals stay exact, bounded and read-only without a hot tree."""
from dataclasses import replace
import fcntl
import gzip
import io
import os

import pytest

from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync.desync_snapshots import SnapshotError


def fixture(tmp_path, *, chunked=False):
    source, cold = tmp_path / "source", tmp_path / "cold"
    (source / "raw").mkdir(parents=True)
    values = {"raw/a.json.gz": gzip.compress(b'{"value":-0.75,"missing":null}', mtime=0),
              "empty.txt": b"", "blob.bin": bytes(range(251)) * 9}
    for name, raw in values.items():
        (source / name).write_bytes(raw)
    (source / "link").symlink_to("blob.bin")
    release = packed.publish_packed_snapshot(cold, "source", source,
        loose_file_threshold_bytes=128, pack_buckets=1,
        **({"file_chunk_bytes": 64} if chunked else {}))
    return cold, release, values


@pytest.mark.parametrize("chunked", [False, True])
def test_batched_memory_read_uses_original_oracle_and_creates_no_materialization(tmp_path, monkeypatch, chunked):
    cold, release, values = fixture(tmp_path, chunked=chunked)
    before = {path.relative_to(cold): (path.stat().st_size, path.stat().st_ctime_ns)
              for path in cold.rglob("*") if path.is_file()}
    hash_file, calls = packed.sha256_file, []
    def hashed(path):
        calls.append(path)
        return hash_file(path)
    monkeypatch.setattr(packed, "sha256_file", hashed)
    monkeypatch.setattr(packed, "_copy_member_and_verify", lambda *a, **k: pytest.fail("memory read created a file"))
    assert packed.read_packed_files(cold, release, names=list(reversed(values)),
        maximum_bytes=4096, maximum_object_bytes=1024**2) == values
    assert before == {path.relative_to(cold): (path.stat().st_size, path.stat().st_ctime_ns)
                      for path in cold.rglob("*") if path.is_file()}
    assert len(calls) == len(set(calls))  # Shared encoded objects are fully hashed once.
    assert not (tmp_path / "hot").exists()


@pytest.mark.parametrize("names", [[], "blob.bin", ["missing"], ["raw"], ["link"],
                                  ["../escape"], ["/absolute"], ["raw//a.json.gz"],
                                  ["empty.txt", "empty.txt"], [False]])
def test_invalid_or_non_file_selections_are_never_accepted(tmp_path, names):
    cold, release, _ = fixture(tmp_path)
    with pytest.raises(SnapshotError):
        packed.read_packed_files(cold, release, names=names,
            maximum_bytes=4096, maximum_object_bytes=1024**2)


@pytest.mark.parametrize("maximum_bytes,maximum_object_bytes", [
    (False, 1024), (0, 1024), (-1, 1024), (64 * 1024**2 + 1, 1024),
    (1.5, 1024), (1024, False), (1024, 0), (1024, 4 * 1024**3 + 1)])
def test_invalid_admission_fails_before_any_cold_access(tmp_path, monkeypatch, maximum_bytes, maximum_object_bytes):
    monkeypatch.setattr(packed, "_check_packed_read_authority", lambda *a: pytest.fail("invalid read touched cold"))
    with pytest.raises(SnapshotError, match="bounded"):
        packed.read_packed_files(tmp_path, None, names=["a"], maximum_bytes=maximum_bytes,
                                 maximum_object_bytes=maximum_object_bytes)


@pytest.mark.parametrize("maximum_bytes,maximum_object_bytes", [(1, 1024**2), (4096, 1)])
def test_byte_reservation_is_enforced_before_payload_hashing(tmp_path, monkeypatch, maximum_bytes, maximum_object_bytes):
    cold, release, _ = fixture(tmp_path)
    hashed = packed.sha256_file
    def metadata_only(path):
        assert "inventories" in path.parts, "unadmitted read hashed a payload"
        return hashed(path)
    monkeypatch.setattr(packed, "sha256_file", metadata_only)
    with pytest.raises(SnapshotError, match="admitted"):
        packed.read_packed_files(cold, release, names=["blob.bin"],
            maximum_bytes=maximum_bytes, maximum_object_bytes=maximum_object_bytes)


@pytest.mark.parametrize("failure", ["corrupt", "replace-object", "replace-manifest", "replace-inventory"])
def test_no_originals_escape_a_failed_integrity_or_generation_check(tmp_path, monkeypatch, failure):
    cold, release, _ = fixture(tmp_path)
    blob = next(item for item in release.manifest["archive"]["objects"] if item["kind"] == "blob")
    path = cold / blob["relpath"]
    if failure == "corrupt":
        path.write_bytes(b"X" * path.stat().st_size)
    else:
        original = packed._verified_member_blocks
        def changing(*args, **kwargs):
            yield from original(*args, **kwargs)
            selected = {"replace-object": path, "replace-manifest": release.manifest_path,
                        "replace-inventory": cold / release.manifest["archive"]["inventory"]["relpath"]}[failure]
            replacement = selected.with_suffix(".replacement")
            replacement.write_bytes(selected.read_bytes())
            os.replace(replacement, selected)
        monkeypatch.setattr(packed, "_verified_member_blocks", changing)
    with pytest.raises(SnapshotError, match="checksum|generation"):
        packed.read_packed_files(cold, release, names=["blob.bin"],
            maximum_bytes=4096, maximum_object_bytes=1024**2)


def test_memory_read_holds_shared_original_owner_and_excludes_retention(tmp_path, monkeypatch):
    cold, release, values = fixture(tmp_path)
    original, observed = packed._verified_member_blocks, []
    def checking(*args, **kwargs):
        with (cold / ".local-state/locks/publish-retention-global.lock").open("r+") as owner:
            with pytest.raises(BlockingIOError):
                fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(owner, fcntl.LOCK_SH | fcntl.LOCK_NB)
        observed.append(True)
        yield from original(*args, **kwargs)
    monkeypatch.setattr(packed, "_verified_member_blocks", checking)
    assert packed.read_packed_files(cold, release, names=["blob.bin"],
        maximum_bytes=4096, maximum_object_bytes=1024**2) == {"blob.bin": values["blob.bin"]}
    assert observed == [True]


def test_read_scope_does_not_claim_availability_of_unselected_objects(tmp_path):
    cold, release, values = fixture(tmp_path)
    blob = next(item for item in release.manifest["archive"]["objects"] if item["kind"] == "blob")
    (cold / blob["relpath"]).unlink()
    assert packed.read_packed_files(cold, release, names=["raw/a.json.gz"],
        maximum_bytes=4096, maximum_object_bytes=1024**2) == {"raw/a.json.gz": values["raw/a.json.gz"]}
    with pytest.raises(SnapshotError):
        packed.verify_packed_snapshot(cold, release)


def test_mutated_in_memory_manifest_does_not_replace_the_fixed_manifest_bytes(tmp_path):
    cold, release, _ = fixture(tmp_path)
    changed = {**release.manifest, "metadata": {"invented": "snapshot"}}
    with pytest.raises(SnapshotError, match="differs from its fixed bytes"):
        packed.read_packed_files(cold, replace(release, manifest=changed), names=["empty.txt"],
            maximum_bytes=4096, maximum_object_bytes=1024**2)


def test_an_identical_external_manifest_copy_is_not_the_registered_cold_manifest(tmp_path):
    cold, release, _ = fixture(tmp_path)
    outside = tmp_path / "manifest-copy.json"
    outside.write_bytes(release.manifest_path.read_bytes())
    with pytest.raises(SnapshotError, match="outside cold authority|fixed cold namespace"):
        packed.read_packed_files(cold, replace(release, manifest_path=outside), names=["empty.txt"],
            maximum_bytes=4096, maximum_object_bytes=1024**2)


def test_matching_bytes_behind_a_redirected_object_are_rejected(tmp_path):
    cold, release, _ = fixture(tmp_path)
    blob = next(item for item in release.manifest["archive"]["objects"] if item["kind"] == "blob")
    path = cold / blob["relpath"]
    outside = tmp_path / "object-copy"
    outside.write_bytes(path.read_bytes())
    path.unlink()
    path.symlink_to(outside)
    with pytest.raises(SnapshotError):
        packed.read_packed_files(cold, release, names=["blob.bin"],
            maximum_bytes=4096, maximum_object_bytes=1024**2)


@pytest.mark.parametrize("scope", ["objects", "inventory", "manifest"])
def test_original_inode_behind_a_changed_parent_directory_is_not_accepted(tmp_path, monkeypatch, scope):
    cold, release, _ = fixture(tmp_path)
    blob = next(item for item in release.manifest["archive"]["objects"] if item["kind"] == "blob")
    paths = {"objects": (cold / blob["relpath"]).parent,
             "inventory": (cold / release.manifest["archive"]["inventory"]["relpath"]).parent,
             "manifest": release.manifest_path.parent}
    original = packed._verified_member_blocks
    def redirecting(*args, **kwargs):
        yield from original(*args, **kwargs)
        parent = paths[scope]
        moved = parent.with_name(parent.name + "-original")
        parent.rename(moved)
        parent.symlink_to(moved, target_is_directory=True)
    monkeypatch.setattr(packed, "_verified_member_blocks", redirecting)
    with pytest.raises(SnapshotError, match="ancestor|generation"):
        packed.read_packed_files(cold, release, names=["blob.bin"],
            maximum_bytes=4096, maximum_object_bytes=1024**2)


def test_an_unrelated_immutable_publication_does_not_invalidate_the_read(tmp_path, monkeypatch):
    cold, release, values = fixture(tmp_path)
    original = packed._verified_member_blocks
    def add_sibling(*args, **kwargs):
        yield from original(*args, **kwargs)
        (cold / "concurrent-immutable-sibling").mkdir()
    monkeypatch.setattr(packed, "_verified_member_blocks", add_sibling)
    assert packed.read_packed_files(cold, release, names=["blob.bin"],
        maximum_bytes=4096, maximum_object_bytes=1024**2) == {"blob.bin": values["blob.bin"]}


def test_replacing_the_original_owner_inode_rejects_the_result_on_context_exit(tmp_path, monkeypatch):
    cold, release, _ = fixture(tmp_path)
    original = packed._verified_member_blocks
    def replace_owner(*args, **kwargs):
        yield from original(*args, **kwargs)
        owner = cold / ".local-state/locks/publish-retention-global.lock"
        replacement = owner.with_name("replacement-owner")
        replacement.write_bytes(b"")
        os.replace(replacement, owner)
    monkeypatch.setattr(packed, "_verified_member_blocks", replace_owner)
    with pytest.raises(SnapshotError, match="owner changed"):
        packed.read_packed_files(cold, release, names=["blob.bin"],
            maximum_bytes=4096, maximum_object_bytes=1024**2)


@pytest.mark.parametrize("payload", [b"", b"short", b"expected extra"])
def test_shared_original_oracle_rejects_truncation_extra_bytes_and_wrong_sha(payload):
    with pytest.raises(SnapshotError, match="checksum|exceeds"):
        list(packed._verified_member_blocks(io.BytesIO(payload), 8, "0" * 64))


def test_private_D_history_must_pass_the_same_primary_mount_guard(monkeypatch):
    from pathlib import Path
    from stockagent.data_sync import cold_primary
    observed = []
    monkeypatch.setattr(cold_primary, "_check_d_primary_mount", lambda path: observed.append(path))
    packed._check_packed_read_authority(Path("/srv/stockagent-packed/.local-state/private-history"))
    assert observed == [Path("/srv/stockagent-packed")]
