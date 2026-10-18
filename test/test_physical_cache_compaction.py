from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path

import numpy as np
import pytest

from stockagent.data_sync import physical_cache_compaction as compaction


def _hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _commit(generation, *, abi=compaction.SOURCE_ABI):
    members = {path.name: {"bytes": path.stat().st_size, "sha256": _hash(path)}
               for path in sorted(generation.iterdir()) if path.suffix in {".npz", ".npy"}}
    manifest = {"abi": abi, "source_digest": generation.name.removeprefix("physical-"),
                "session_encoding": compaction.SESSION_ENCODING, "files": members}
    (generation / "manifest.json").write_text(json.dumps(manifest))
    (generation / "READY.json").write_text(json.dumps({
        "abi": abi, "source_digest": manifest["source_digest"],
        "manifest_sha256": _hash(generation / "manifest.json"), "verified_files": len(members),
    }))


def _generation(root, digit, value=7, *, abi=compaction.SOURCE_ABI):
    path = root / ("physical-" + digit * 64)
    path.mkdir(parents=True)
    np.savez(path / "session-2026-10-13.npz", values=np.arange(1024) + value)
    # Unshared masks/metadata must not be touched by the session compactor.
    np.save(path / "source_gaps.npy", np.array([digit == "b"], dtype=bool))
    (path / "session-2026-10-13.npz").chmod(0o600)
    _commit(path, abi=abi)
    return path


def _run(root, receipts, **options):
    return compaction.compact_physical_minute_cache(
        root, receipts, reference_probe=options.pop("reference_probe", lambda *a, **kw: []), **options)


def test_audit_apply_keeps_exact_arrays_and_metadata_and_is_idempotent(tmp_path):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    first, second = _generation(root, "a"), _generation(root, "b")
    before = {str(path): path.read_bytes() for path in root.rglob("*") if path.is_file()}
    old_payload_inode = (second / "session-2026-10-13.npz").stat().st_ino
    audit = _run(root, receipts)
    assert audit["state"] == "audited" and audit["duplicate_groups"] == 1
    assert (second / "session-2026-10-13.npz").stat().st_ino == old_payload_inode
    result = _run(root, receipts, apply=True, expected_fingerprint=audit["plan_fingerprint"])
    assert result["state"] == "compacted" and result["replaced_paths"] == 1
    assert result["retired_allocated_bytes"] == result["candidate_allocated_bytes"] > 0
    assert result["logical_paths_removed"] == result["cold_objects_removed"] == 0
    assert all(Path(name).read_bytes() == value for name, value in before.items())
    assert os.path.samefile(first / "session-2026-10-13.npz", second / "session-2026-10-13.npz")
    assert (first / "session-2026-10-13.npz").stat().st_mode & 0o222 == 0
    for generation in (first, second):
        with np.load(generation / "session-2026-10-13.npz", allow_pickle=False) as payload:
            np.testing.assert_array_equal(payload["values"], np.arange(1024) + 7)
    again = _run(root, receipts, apply=True)
    assert again["state"] == "compacted" and again["replaced_paths"] == 0
    assert again["retired_allocated_bytes"] == 0


def test_writer_atomic_replacement_changes_only_its_own_generation(tmp_path):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    first, second = _generation(root, "a"), _generation(root, "b")
    assert _run(root, receipts, apply=True)["state"] == "compacted"
    # Exercise the maintained writer, not a special compactor-only write path.
    from stockagent.data.tw_day_trade_carry_source import _atomic_npz, _verify_physical_cache_files
    before = (first / "session-2026-10-13.npz").read_bytes()
    with compaction._writer_lock(root / f".{second.name}.lock"):
        _atomic_npz(second / "session-2026-10-13.npz", values=np.arange(1024) + 9)
        _commit(second)
        manifest = json.loads((second / "manifest.json").read_text())
        accepted, receipt, method = _verify_physical_cache_files(
            cache_root=second, manifest_path=second / "manifest.json", ready_path=second / "READY.json",
            source_digest="b" * 64, required=[second / name for name in manifest["files"]],
            expected=manifest["files"])
        assert accepted and receipt and method == "full_sha256"
    assert (first / "session-2026-10-13.npz").read_bytes() == before
    assert not os.path.samefile(first / "session-2026-10-13.npz", second / "session-2026-10-13.npz")
    assert _run(root, receipts, apply=True)["duplicate_groups"] == 0


def test_external_alias_is_not_chmodded_or_replaced(tmp_path):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    first, second = _generation(root, "a"), _generation(root, "b")
    external = tmp_path / "other-owner.npz"
    os.link(first / "session-2026-10-13.npz", external)
    result = _run(root, receipts, apply=True)
    assert result["external_link_inodes_retained"] == 1
    assert result["replaced_paths"] == 0
    assert external.stat().st_mode & 0o777 == 0o600
    assert not os.path.samefile(second / "session-2026-10-13.npz", external)


def test_known_multiple_aliases_are_all_retained_and_one_inode_is_counted_once(tmp_path):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    first, second, third = (_generation(root, digit) for digit in "abc")
    (third / "session-2026-10-13.npz").unlink()
    os.link(first / "session-2026-10-13.npz", third / "session-2026-10-13.npz")
    _commit(third)
    result = _run(root, receipts, apply=True)
    assert result["replaced_paths"] == 1
    assert result["retired_allocated_bytes"] == (second / "session-2026-10-13.npz").stat().st_blocks * 512
    assert len({(path / "session-2026-10-13.npz").stat().st_ino for path in (first, second, third)}) == 1


@pytest.mark.parametrize("phase", ["initial", "preapply", "retired"])
def test_process_or_mmap_reference_defers_or_prevents_reclaim_claim(tmp_path, phase):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    _generation(root, "a"), _generation(root, "b")
    calls = []
    def probe(targets, **kwargs):
        calls.append(targets)
        return ["pid=123:maps:cache"] if len(calls) == {"initial": 1, "preapply": 2, "retired": 3}[phase] else []
    result = _run(root, receipts, apply=True, reference_probe=probe)
    assert result["retired_allocated_bytes"] == 0
    if phase == "retired":
        assert result["state"] == "compacted" and result["retained_open_inode_references"]
    else:
        assert result["state"] == "deferred" and result["replaced_paths"] == 0


def test_writer_lock_is_nonblocking_and_does_not_get_deleted(tmp_path):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    first = _generation(root, "a")
    _generation(root, "b")
    lock_path = root / f".{first.name}.lock"
    with lock_path.open("w") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        inode = lock_path.stat().st_ino
        result = _run(root, receipts, apply=True)
    assert result["state"] == "deferred" and result["reason"] == "writer_lock_busy"
    assert lock_path.stat().st_ino == inode and result["replaced_paths"] == 0


@pytest.mark.parametrize("damage", ["sha", "ready", "extra_session", "symlink", "mode"])
def test_corrupt_or_unsupported_members_fail_before_any_replacement(tmp_path, damage):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    first, second = _generation(root, "a"), _generation(root, "b")
    target = second / "session-2026-10-13.npz"
    if damage == "sha":
        raw = target.read_bytes()
        target.write_bytes(raw[:-1] + bytes([raw[-1] ^ 1]))
    elif damage == "ready":
        (second / "READY.json").write_text('{}')
    elif damage == "extra_session":
        (second / "session-2026-10-14.npz").write_bytes(b"unknown unique data")
    elif damage == "symlink":
        target.unlink()
        target.symlink_to(first / target.name)
    else:
        target.chmod(0o700)
    result = _run(root, receipts, apply=True)
    assert result["state"] == "failed" and result["replaced_paths"] == 0


def test_unsupported_abi_and_incomplete_generations_are_retained(tmp_path):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    _generation(root, "a"), _generation(root, "b", abi="unknown-v13")
    (root / ("physical-" + "c" * 64)).mkdir()
    result = _run(root, receipts, apply=True)
    assert result["state"] == "compacted" and result["replaced_paths"] == 0
    assert {row["reason"] for row in result["skipped_generations"]} == {"incomplete", "unsupported_abi"}


def test_stale_dry_run_cannot_apply(tmp_path):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    _generation(root, "a"), _generation(root, "b")
    result = _run(root, receipts, apply=True, expected_fingerprint="0" * 64)
    assert result["state"] == "failed" and result["replaced_paths"] == 0
    assert "fingerprint changed" in result["error"]


def test_failed_atomic_replace_keeps_both_payloads_and_can_retry(tmp_path, monkeypatch):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    first, second = _generation(root, "a"), _generation(root, "b")
    replace = os.replace
    def fail(source, destination):
        if ".session-share-" in str(source):
            raise OSError("injected rename failure")
        return replace(source, destination)
    monkeypatch.setattr(compaction.os, "replace", fail)
    result = _run(root, receipts, apply=True)
    assert result["state"] == "failed" and result["retired_allocated_bytes"] == 0
    assert (first / "session-2026-10-13.npz").read_bytes() == (second / "session-2026-10-13.npz").read_bytes()
    assert not list(root.rglob("*.tmp"))
    monkeypatch.setattr(compaction.os, "replace", replace)
    assert _run(root, receipts, apply=True)["state"] == "compacted"


def test_payload_change_during_hash_audit_denies_apply(tmp_path, monkeypatch):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    first, second = _generation(root, "a"), _generation(root, "b")
    stable_hash = compaction._stable_hash
    def change(path, expected):
        result = stable_hash(path, expected)
        if path.parent == first:
            os.utime(path, ns=(path.stat().st_atime_ns, path.stat().st_mtime_ns + 1))
        return result
    monkeypatch.setattr(compaction, "_stable_hash", change)
    result = _run(root, receipts, apply=True)
    assert result["state"] == "failed" and result["replaced_paths"] == 0
    assert not os.path.samefile(first / "session-2026-10-13.npz", second / "session-2026-10-13.npz")


def test_full_audit_hash_does_not_hold_generation_writer_locks(tmp_path, monkeypatch):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    generations = [_generation(root, digit) for digit in "ab"]
    original_hash = compaction._stable_hash
    def check(path, expected):
        for generation in generations:
            with compaction._writer_lock(root / f".{generation.name}.lock"):
                pass
        return original_hash(path, expected)
    monkeypatch.setattr(compaction, "_stable_hash", check)
    assert _run(root, receipts)["state"] == "audited"


def test_retired_inodes_stay_descriptor_pinned_until_reference_probe(tmp_path):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    _generation(root, "a"), _generation(root, "b")
    checked = []
    def probe(targets, *, inode_keys):
        if not targets:
            descriptors = []
            for path in Path("/proc/self/fd").iterdir():
                try:
                    info = path.stat()
                except FileNotFoundError:
                    continue
                if (info.st_dev, info.st_ino) in inode_keys:
                    descriptors.append(info)
            assert len(descriptors) == len(inode_keys)
            assert all(info.st_nlink == 0 for info in descriptors)
            checked.extend(inode_keys)
        return []
    result = _run(root, receipts, apply=True, reference_probe=probe)
    assert checked and result["retired_allocated_bytes"] > 0
    assert result["retired_inode_checks"] == "descriptor_pinned_bounded_batches_v1"


@pytest.mark.parametrize("unsupported", [False, True])
def test_hash_cache_hint_is_optional_and_never_changes_exact_digest(tmp_path, monkeypatch, unsupported):
    path = tmp_path / "payload"
    path.write_bytes(b"unchanged source bytes")
    calls = []
    def advise(*args):
        calls.append(args)
        if unsupported:
            raise OSError("filesystem does not implement this advisory hint")
    monkeypatch.setattr(compaction.os, "posix_fadvise", advise)
    assert compaction._stable_hash(path, compaction._info(path)) == hashlib.sha256(path.read_bytes()).hexdigest()
    assert len(calls) == 1


def test_busy_apply_group_is_retained_for_next_run(tmp_path, monkeypatch):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    _generation(root, "a"), _generation(root, "b")
    def busy(*args, **kwargs):
        raise BlockingIOError("writer arrived after audit")
    monkeypatch.setattr(compaction, "_share_group", busy)
    result = _run(root, receipts, apply=True)
    assert result["state"] == "partially_compacted" and result["busy_groups_retained"] == 1
    assert result["replaced_paths"] == result["retired_allocated_bytes"] == 0


def test_interrupted_replacement_keeps_bytes_and_durable_interrupted_result(tmp_path, monkeypatch):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    first, second = _generation(root, "a"), _generation(root, "b")
    original_replace = os.replace
    def interrupt(source, destination):
        if ".session-share-" in str(source):
            raise KeyboardInterrupt
        return original_replace(source, destination)
    monkeypatch.setattr(compaction.os, "replace", interrupt)
    with pytest.raises(KeyboardInterrupt):
        _run(root, receipts, apply=True)
    result = json.loads((receipts / "latest.json").read_text())
    assert result["state"] == "interrupted" and result["retired_allocated_bytes"] == 0
    assert (first / "session-2026-10-13.npz").read_bytes() == (second / "session-2026-10-13.npz").read_bytes()
    assert not list(root.rglob("*.tmp"))


@pytest.mark.parametrize("name", ["artifacts", "artifacts/cache", "artifacts/live/test", "../cache/test",
                                  "/srv/stockagent-packed", "artifacts/cache/../test", "artifacts/cache/.hidden"])
def test_policy_cannot_select_broad_or_source_or_hidden_paths(tmp_path, name):
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"schema_version": 1, "contract": compaction.CONTRACT, "cache_roots": [name]}))
    with pytest.raises(ValueError):
        compaction.policy_roots(policy, tmp_path)


def test_policy_selects_only_explicit_real_child_and_rejects_symlink(tmp_path):
    root = tmp_path / "artifacts/cache/minute-test"
    root.mkdir(parents=True)
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"schema_version": 1, "contract": compaction.CONTRACT,
                                 "cache_roots": ["artifacts/cache/minute-test"]}))
    assert compaction.policy_roots(policy, tmp_path) == [root]
    alias = tmp_path / "alias.json"
    alias.symlink_to(policy)
    with pytest.raises(ValueError):
        compaction.policy_roots(alias, tmp_path)


def test_existing_daily_owner_is_the_only_automation_entrypoint():
    wrapper = (Path(__file__).parents[1] / "scripts/run_artifact_dedup.sh").read_text()
    assert "/etc/stockagent/service-cache-compaction.json" in wrapper
    assert "--service-cache-policy" in wrapper
    assert 'exec 9>/run/lock/stockagent-artifact-dedup.lock' in wrapper
    assert "--sharing-candidates-only" in wrapper


def test_candidate_only_maintenance_does_not_scan_unique_session_payloads(tmp_path, monkeypatch):
    root, receipts = tmp_path / "cache", tmp_path / "receipts"
    _generation(root, "a"), _generation(root, "b", value=9)
    monkeypatch.setattr(compaction, "_stable_hash", lambda *a, **kw: pytest.fail("no duplicate candidates"))
    result = _run(root, receipts, apply=True, full_audit=False)
    assert result["state"] == "compacted" and result["duplicate_groups"] == 0
    assert result["hash_scope"] == "sharing_candidates_only"
    assert result["verified_session_bytes"] == 0 and result["metadata_only_payload_inodes"] == 2
