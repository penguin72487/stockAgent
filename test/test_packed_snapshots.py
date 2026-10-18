from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import pytest

from stockagent.data_sync.desync_snapshots import SnapshotError, scan_tree
import stockagent.data_sync.packed_snapshots as packed_snapshots
from scripts.packed_snapshot import main as packed_snapshot_main
from stockagent.data_sync.packed_snapshots import (
    audit_packed_store,
    fetch_packed_snapshot,
    fetch_packed_subtree,
    initialize_packed_layout,
    publish_packed_snapshot,
    resolve_latest_packed,
    resolve_packed_snapshot_id,
    resolve_packed_pin,
    stage_packed_root_files,
    verify_packed_snapshot,
    write_packed_pin,
)


def _source_tree(root: Path) -> Path:
    source = root / "source"
    (source / "empty").mkdir(parents=True)
    (source / "text").mkdir()
    (source / "text" / "first.json").write_text('{"first": 1}\n', encoding="utf-8")
    (source / "text" / "second.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    large = b"large-content\n" * 512
    (source / "large-a.bin").write_bytes(large)
    (source / "large-b.bin").write_bytes(large)
    (source / "current").symlink_to("text/first.json")
    return source


@pytest.mark.parametrize("selection", ["all", "excluded", "maximum", "selected", "override", "staging"])
def test_summary_only_census_keeps_every_fingerprint_without_allocating_entries(tmp_path, monkeypatch, selection):
    source = _source_tree(tmp_path)
    options = {}
    if selection == "excluded":
        options["excluded_subtrees"] = ("text",)
    elif selection == "maximum":
        options["maximum_file_bytes"] = 1024
    elif selection == "selected":
        options["included_root_files"] = ("large-a.bin",)
    elif selection == "override":
        frozen = tmp_path / "frozen-control.json"
        frozen.write_bytes(b"fixed control bytes")
        options["file_overrides"] = {"text/first.json": frozen}
    elif selection == "staging":
        (source / "text" / ".unfinished.123.456.tmp").write_bytes(b"not committed")
        options["private_staging_file_rules"] = ("artifact_io_atomic_sibling_v1",)
    entries, expected = packed_snapshots._collect_entries(source, **options)
    assert entries

    def forbid_entry_allocation(*args, **kwargs):
        pytest.fail("summary-only census must not build a second inventory")

    monkeypatch.setattr(packed_snapshots, "_SourceEntry", forbid_entry_allocation)
    events = []
    actual_entries, actual = packed_snapshots._collect_entries(
        source, **options, retain_entries=False, progress=events.append)
    assert actual_entries == [] and actual == expected
    assert events and all(event["acceptance"] is False for event in events)


@pytest.mark.parametrize("invalid", ["symlink", "fifo", "selected-directory"])
def test_summary_only_census_preserves_path_type_and_symlink_guards(tmp_path, invalid):
    source = _source_tree(tmp_path)
    options = {}
    if invalid == "symlink":
        (source / "unsafe").symlink_to("../outside")
    elif invalid == "fifo":
        os.mkfifo(source / "unsupported-pipe")
    else:
        options["included_root_files"] = ("empty",)
    with pytest.raises(SnapshotError):
        packed_snapshots._collect_entries(source, retain_entries=False, **options)


def test_publication_streams_final_census_but_still_rejects_a_changed_tree(tmp_path, monkeypatch):
    source, cold = _source_tree(tmp_path), tmp_path / "cold"
    events = []
    first = publish_packed_snapshot(cold, "memory-test", source, node_id="node-a", progress=events.append)
    stages = [event["stage"] for event in events]
    assert stages.index("verifying_complete_file_storage") < stages.index("committing_complete_inventory")
    assert stages.index("verifying_source_stability") < stages.index("source_fences_verified")
    assert all(event["acceptance"] is False for event in events)
    head = first.head_path.read_bytes()
    census = packed_snapshots._collect_entries

    def race_before_final_census(path, **options):
        if options.get("retain_entries") is False:
            (path / "text" / "first.json").write_bytes(b"changed after the complete payload hash")
        return census(path, **options)

    monkeypatch.setattr(packed_snapshots, "_collect_entries", race_before_final_census)
    with pytest.raises(SnapshotError, match="source tree changed"):
        publish_packed_snapshot(cold, "memory-test", source)
    assert first.head_path.read_bytes() == head


def test_publication_releases_consumed_previous_file_rows_during_hashing(tmp_path, monkeypatch):
    import weakref

    source, cold = _source_tree(tmp_path), tmp_path / "cold"
    first = publish_packed_snapshot(cold, "memory-test", source, node_id="node-a")
    load, hash_entry = packed_snapshots._iter_inventory, packed_snapshots._hash_source_entry
    live, observed = weakref.WeakValueDictionary(), []

    class TrackedFile(dict):
        pass

    def tracked_inventory(*args, **kwargs):
        for row in load(*args, **kwargs):
            if row.get("kind") == "file":
                tracked = TrackedFile(row)
                live[row["path"]] = tracked
                row = tracked
            yield row

    def observe_hash(*args, **kwargs):
        observed.append(len(live))
        return hash_entry(*args, **kwargs)

    monkeypatch.setattr(packed_snapshots, "_iter_inventory", tracked_inventory)
    monkeypatch.setattr(packed_snapshots, "_hash_source_entry", observe_hash)
    second = publish_packed_snapshot(cold, "memory-test", source)
    assert observed == [4, 3, 2, 1]
    assert not live and second.manifest_sha256 == first.manifest_sha256


def test_bounded_census_benchmark_is_readonly_and_checks_every_summary_field(tmp_path):
    from scripts.benchmark_packed_census import main

    source = _source_tree(tmp_path)
    _, before = packed_snapshots._collect_entries(source, retain_entries=False)
    output = tmp_path / "benchmark.json"
    assert main(["--source", str(source), "--maximum-files", "4", "--output", str(output)]) == 0
    result = json.loads(output.read_text())
    assert result["state"] == "verified_metadata_census_parity"
    assert result["summary"] == before
    assert [trial["retained_records"] for trial in result["trials"]] == [7, 0, 0, 7]
    assert result["source_payload_writes"] == result["production_cold_writes"] == result["provider_calls"] == 0
    assert packed_snapshots._collect_entries(source, retain_entries=False)[1] == before
    with pytest.raises(ValueError, match="file bound"):
        main(["--source", str(source), "--maximum-files", "3", "--output", str(tmp_path / "too-large.json")])
    assert not (tmp_path / "too-large.json").exists()


def _flat_release(tmp_path, *, threshold=1024):
    source = _source_tree(tmp_path)
    names = ["large-a.bin", "large-b.bin"]
    (source / names[0]).chmod(0o400)
    (source / names[1]).chmod(0o600)
    cold, destination = tmp_path / "cold", tmp_path / "flat"
    release = publish_packed_snapshot(cold, "flat-results", source, node_id="node-a",
        loose_file_threshold_bytes=threshold, included_root_files=names)
    destination.mkdir(mode=0o700)
    return source, cold, destination, release, names


@pytest.mark.parametrize("durable", [True, False])
def test_member_copy_sets_verified_mode_before_its_single_optional_durability_barrier(tmp_path, monkeypatch, durable):
    import hashlib
    import io

    content, mode, events = b"complete verified member", 0o400, []
    path = tmp_path / "member"
    fchmod, fsync = os.fchmod, os.fsync

    def set_mode(descriptor, wanted):
        fchmod(descriptor, wanted)
        events.append("mode")

    def durable_once(descriptor):
        assert events == ["mode"]
        assert stat.S_IMODE(os.fstat(descriptor).st_mode) == mode
        events.append("durable")
        return fsync(descriptor)

    monkeypatch.setattr(os, "fchmod", set_mode)
    monkeypatch.setattr(os, "fsync", durable_once)
    packed_snapshots._copy_member_and_verify(io.BytesIO(content), path,
        expected_size=len(content), expected_sha256=hashlib.sha256(content).hexdigest(),
        mode=mode, durable=durable)
    assert path.read_bytes() == content and stat.S_IMODE(path.stat().st_mode) == mode
    assert events == (["mode", "durable"] if durable else ["mode"])


def test_member_copy_never_writes_past_its_admitted_complete_file_size(tmp_path):
    import hashlib
    import io

    expected, requests = b"complete admitted member", []
    class GrewAfterVerification(io.BytesIO):
        def read(self, size=-1):
            requests.append(size)
            return super().read(size)

    path = tmp_path / "uncommitted-grown-member"
    with pytest.raises(SnapshotError, match="checksum|size"):
        packed_snapshots._copy_member_and_verify(
            GrewAfterVerification(expected + b"unexpected growth" * 128), path,
            expected_size=len(expected), expected_sha256=hashlib.sha256(expected).hexdigest(), mode=0o400)
    assert path.exists() and path.stat().st_size <= len(expected)
    assert requests == [len(expected) + 1]


@pytest.mark.parametrize("threshold", [1, 1024**2])
def test_flat_root_restore_reuses_full_copy_engine_without_another_hot_tree(tmp_path, monkeypatch, threshold):
    source, cold, destination, release, names = _flat_release(tmp_path, threshold=threshold)
    sentinel = destination / "unrelated-owner.data"
    sentinel.write_bytes(b"retain this unrelated original")
    before = packed_snapshots._source_stat(sentinel.stat())
    scan = packed_snapshots.scan_tree
    copies = []
    copy = packed_snapshots._copy_member_and_verify

    def no_whole_namespace_scan(path):
        assert Path(path) != destination
        return scan(path)

    def durable_copy(*args, **kwargs):
        copies.append(kwargs["durable"])
        return copy(*args, **kwargs)

    monkeypatch.setattr(packed_snapshots, "scan_tree", no_whole_namespace_scan)
    monkeypatch.setattr(packed_snapshots, "_copy_member_and_verify", durable_copy)
    staged = stage_packed_root_files(cold, destination, release,
                                    expected_names=reversed(names), maximum_bytes=1024**2)
    assert staged["snapshot_id"] == release.manifest["snapshot_id"]
    assert staged["manifest_sha256"] == release.manifest_sha256
    assert not staged["managed_materialization_created"] and not staged["consumer_commit"]
    assert staged["logical_bytes"] == sum((source / name).stat().st_size for name in names)
    assert copies == [True, True]
    assert sorted(staged["files"]) == names
    for name, path in staged["files"].items():
        assert path.parent == destination and path.name.startswith(".packed-stage-")
        assert path.read_bytes() == (source / name).read_bytes()
        assert stat.S_IMODE(path.stat().st_mode) == stat.S_IMODE((source / name).stat().st_mode)
        assert path.stat().st_nlink == 1
        assert not (destination / name).exists()  # no premature consumer commit
    assert len({path.stat().st_ino for path in staged["files"].values()}) == 2
    assert sorted(destination.iterdir()) == sorted([sentinel, *staged["files"].values()])
    assert packed_snapshots._source_stat(sentinel.stat()) == before
    assert sentinel.read_bytes() == b"retain this unrelated original"


@pytest.mark.parametrize("names", [None, [], "large-a.bin", ["large-a.bin"],
                                    ["large-a.bin", "absent"], ["../escape"]])
def test_flat_restore_refuses_implicit_or_incomplete_scope_before_cold_reads(tmp_path, monkeypatch, names):
    _source, cold, destination, release, _names = _flat_release(tmp_path)
    monkeypatch.setattr(packed_snapshots, "verify_packed_snapshot", lambda *a, **kw: pytest.fail("invalid scope read cold data"))
    with pytest.raises(SnapshotError):
        stage_packed_root_files(cold, destination, release, expected_names=names, maximum_bytes=1024**2)
    assert not list(destination.iterdir())


@pytest.mark.parametrize("limit", [0, -1, True, 1.5, 1])
def test_flat_restore_requires_an_explicit_sufficient_byte_bound(tmp_path, monkeypatch, limit):
    _source, cold, destination, release, names = _flat_release(tmp_path)
    monkeypatch.setattr(packed_snapshots, "verify_packed_snapshot", lambda *a, **kw: pytest.fail("unadmitted cold read"))
    with pytest.raises(SnapshotError):
        stage_packed_root_files(cold, destination, release, expected_names=names, maximum_bytes=limit)
    assert not list(destination.iterdir())


@pytest.mark.parametrize("change", ["whole-tree", "large-objects", "root-symlink", "writable-root"])
def test_flat_restore_refuses_ambiguous_release_or_unsafe_destination(tmp_path, monkeypatch, change):
    _source, cold, destination, release, names = _flat_release(tmp_path)
    if change == "whole-tree":
        release.manifest["source"]["selection"].pop("included_root_files")
    elif change == "large-objects":
        release.manifest["archive"]["objects"][0]["bytes"] = 8 * 1024**2
    elif change == "root-symlink":
        other = tmp_path / "other"
        destination.rename(other)
        destination.symlink_to(other, target_is_directory=True)
    else:
        destination.chmod(0o777)
    monkeypatch.setattr(packed_snapshots, "verify_packed_snapshot", lambda *a, **kw: pytest.fail("unsafe restore read cold data"))
    with pytest.raises(SnapshotError):
        stage_packed_root_files(cold, destination, release, expected_names=names, maximum_bytes=1024**2)
    assert not list(destination.iterdir())


def test_flat_restore_rechecks_root_after_cold_verification_before_creating_files(tmp_path, monkeypatch):
    _source, cold, destination, release, names = _flat_release(tmp_path)
    verify = packed_snapshots.verify_packed_snapshot
    displaced = tmp_path / "original-flat"

    def replace_root(*args, **kwargs):
        proof = verify(*args, **kwargs)
        destination.rename(displaced)
        destination.mkdir(mode=0o700)
        (destination / "foreign.data").write_bytes(b"not our namespace")
        return proof

    monkeypatch.setattr(packed_snapshots, "verify_packed_snapshot", replace_root)
    with pytest.raises(SnapshotError, match="directory changed"):
        stage_packed_root_files(cold, destination, release, expected_names=names, maximum_bytes=1024**2)
    assert not list(displaced.iterdir())
    assert [path.name for path in destination.iterdir()] == ["foreign.data"]


@pytest.mark.parametrize("failure", ["copy-interruption", "after-copy-mutation"])
def test_flat_restore_failures_retain_counted_private_stages_and_never_commit(tmp_path, monkeypatch, failure):
    from stockagent.data_sync.completed_forward_capacity import capacity_status

    _source, cold, destination, release, names = _flat_release(tmp_path, threshold=1)
    if failure == "copy-interruption":
        copy = packed_snapshots._copy_member_and_verify
        def interrupt(*args, **kwargs):
            copy(*args, **kwargs)
            raise OSError("interrupted copy")
        monkeypatch.setattr(packed_snapshots, "_copy_member_and_verify", interrupt)
        expected = OSError
    else:
        materialize = packed_snapshots._materialize_regular_files
        def mutate(*args, **kwargs):
            materialize(*args, **kwargs)
            path = next(destination.glob(".packed-stage-*"))
            path.chmod(0o600)
            path.write_bytes(b"changed after streamed verification")
        monkeypatch.setattr(packed_snapshots, "_materialize_regular_files", mutate)
        expected = SnapshotError
    with pytest.raises(expected):
        stage_packed_root_files(cold, destination, release, expected_names=names, maximum_bytes=1024**2)
    stages = list(destination.glob(".packed-stage-*"))
    assert stages and all(not (destination / name).exists() for name in names)
    status = capacity_status(destination)
    assert status["namespace_allocated_bytes"] >= sum(path.stat().st_blocks * 512 for path in stages)
    assert status["data_paths_removed"] == 0


def test_flat_restore_holds_canonical_retention_owner_across_verify_and_copy(tmp_path, monkeypatch):
    import fcntl

    _source, cold, destination, release, names = _flat_release(tmp_path)
    verify, materialize = packed_snapshots.verify_packed_snapshot, packed_snapshots._materialize_regular_files
    observations = []
    def held():
        with (cold / ".local-state/locks/publish-retention-global.lock").open("a") as owner:
            with pytest.raises(BlockingIOError):
                fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
    def verify_held(*args, **kwargs):
        held()
        observations.append("verify")
        return verify(*args, **kwargs)
    def copy_held(*args, **kwargs):
        held()
        observations.append("copy")
        return materialize(*args, **kwargs)
    monkeypatch.setattr(packed_snapshots, "verify_packed_snapshot", verify_held)
    monkeypatch.setattr(packed_snapshots, "_materialize_regular_files", copy_held)
    stage_packed_root_files(cold, destination, release, expected_names=names, maximum_bytes=1024**2)
    assert observations == ["verify", "copy"]


@pytest.mark.parametrize("missing", ["owner", "mount"])
def test_flat_restore_never_recreates_a_missing_cold_authority_or_owner(tmp_path, missing):
    _source, cold, destination, release, names = _flat_release(tmp_path)
    owner = cold / ".local-state/locks/publish-retention-global.lock"
    owner.unlink()
    if missing == "mount":
        (cold / ".stockagent-d-mount-required").write_text("not mounted")
    with pytest.raises(SnapshotError, match="owner|not mounted"):
        stage_packed_root_files(cold, destination, release, expected_names=names, maximum_bytes=1024**2)
    assert not owner.exists() and not list(destination.iterdir())


@pytest.mark.parametrize("replacement", ["missing", "regular", "symlink"])
def test_flat_restore_rechecks_the_existing_owner_after_waiting_for_its_lock(tmp_path, monkeypatch, replacement):
    import fcntl

    _source, cold, destination, release, names = _flat_release(tmp_path)
    owner = cold / ".local-state/locks/publish-retention-global.lock"
    identity, flock, changed = owner.stat(), fcntl.flock, []

    def replace_while_waiting(descriptor, operation):
        held = os.fstat(descriptor)
        if (operation & fcntl.LOCK_EX and not changed
                and (held.st_dev, held.st_ino) == (identity.st_dev, identity.st_ino)):
            owner.rename(owner.with_name("original-owner-retained.lock"))
            if replacement == "regular":
                owner.write_bytes(b"new unrelated owner")
            elif replacement == "symlink":
                owner.symlink_to("original-owner-retained.lock")
            changed.append(True)
        return flock(descriptor, operation)

    monkeypatch.setattr(fcntl, "flock", replace_while_waiting)
    with pytest.raises(SnapshotError, match="owner"):
        stage_packed_root_files(cold, destination, release, expected_names=names, maximum_bytes=1024**2)
    assert changed and not list(destination.iterdir())
    assert (owner.with_name("original-owner-retained.lock")).is_file()
    if replacement == "missing":
        assert not owner.exists()


def test_flat_restore_owner_replacement_during_copy_retains_exact_uncommitted_stages(tmp_path, monkeypatch):
    _source, cold, destination, release, names = _flat_release(tmp_path)
    owner = cold / ".local-state/locks/publish-retention-global.lock"
    copy, changed = packed_snapshots._copy_member_and_verify, []

    def replace_after_copy(*args, **kwargs):
        result = copy(*args, **kwargs)
        if not changed:
            owner.rename(owner.with_name("original-owner-retained.lock"))
            owner.write_bytes(b"new unrelated owner")
            changed.append(True)
        return result

    monkeypatch.setattr(packed_snapshots, "_copy_member_and_verify", replace_after_copy)
    with pytest.raises(SnapshotError, match="owner"):
        stage_packed_root_files(cold, destination, release, expected_names=names, maximum_bytes=1024**2)
    assert changed and list(destination.glob(".packed-stage-*"))
    assert not any((destination / name).exists() for name in names)
    assert owner.read_bytes() == b"new unrelated owner"


@pytest.mark.parametrize("replacement", ["missing", "symlink", "fifo"])
def test_flat_restore_open_cannot_create_or_follow_a_changed_owner(tmp_path, monkeypatch, replacement):
    _source, cold, destination, release, names = _flat_release(tmp_path)
    owner = cold / ".local-state/locks/publish-retention-global.lock"
    original_open, observations = os.open, []

    def replace_before_open(path, flags, *args, **kwargs):
        if Path(path) == owner and not observations:
            assert not flags & (os.O_CREAT | os.O_TRUNC)
            assert flags & os.O_NOFOLLOW and flags & os.O_NONBLOCK
            observations.append(True)
            owner.rename(owner.with_name("original-owner-retained.lock"))
            if replacement == "symlink":
                owner.symlink_to("original-owner-retained.lock")
            elif replacement == "fifo":
                os.mkfifo(owner)
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", replace_before_open)
    with pytest.raises(SnapshotError, match="owner"):
        stage_packed_root_files(cold, destination, release, expected_names=names, maximum_bytes=1024**2)
    assert observations and not list(destination.iterdir())
    if replacement == "missing":
        assert not owner.exists()


def test_flat_restore_never_borrows_a_hardlinked_publication_owner(tmp_path):
    _source, cold, destination, release, names = _flat_release(tmp_path)
    owner = cold / ".local-state/locks/publish-retention-global.lock"
    os.link(owner, tmp_path / "unrelated-owner-alias")
    with pytest.raises(SnapshotError, match="owner"):
        stage_packed_root_files(cold, destination, release, expected_names=names, maximum_bytes=1024**2)
    assert owner.stat().st_nlink == 2 and not list(destination.iterdir())


@pytest.mark.parametrize("operation", ["stage", "pin"])
def test_fixed_cold_read_admits_the_requested_authority_before_resolving_an_alias(tmp_path, monkeypatch, operation):
    _source, cold, destination, release, names = _flat_release(tmp_path)
    pin = tmp_path / "fixed.pin.json"
    write_packed_pin(pin, release)
    alias = tmp_path / "redirected-authority"
    alias.symlink_to(cold, target_is_directory=True)
    original, observations = packed_snapshots._check_packed_read_authority, []

    def admit_original_path(path):
        observations.append(path)
        if path == alias:
            raise SnapshotError("requested authority was redirected")
        original(path)

    monkeypatch.setattr(packed_snapshots, "_check_packed_read_authority", admit_original_path)
    with pytest.raises(SnapshotError, match="authority.*redirected"):
        if operation == "stage":
            stage_packed_root_files(alias, destination, release, expected_names=names, maximum_bytes=1024**2)
        else:
            resolve_packed_pin(alias, pin)
    assert observations == [alias] and not list(destination.iterdir())


def test_standard_pin_resolves_and_restores_the_exact_old_release_not_moving_head(tmp_path, monkeypatch):
    source, cold, destination, original, names = _flat_release(tmp_path)
    pin = tmp_path / "fixed.pin.json"
    write_packed_pin(pin, original)
    before = (source / names[0]).read_bytes()
    (source / names[0]).chmod(0o600)
    (source / names[0]).write_bytes(before + b"new source version")
    newer = publish_packed_snapshot(cold, "flat-results", source, included_root_files=names)
    assert newer.manifest["snapshot_id"] != original.manifest["snapshot_id"]
    monkeypatch.setattr(packed_snapshots, "resolve_latest_packed", lambda *a, **kw: pytest.fail("moving head consulted"))
    fixed = resolve_packed_pin(cold, pin, expected_dataset="flat-results")
    assert fixed.manifest_sha256 == original.manifest_sha256
    staged = stage_packed_root_files(cold, destination, fixed, expected_names=names, maximum_bytes=1024**2)
    assert staged["files"][names[0]].read_bytes() == before


@pytest.mark.parametrize("defect", ["schema", "boolean-schema", "extra-field", "missing-field",
                                    "manifest-sha", "embedded-manifest", "duplicate-field", "json"])
def test_packed_pin_rejects_invalid_binding_without_falling_back(tmp_path, monkeypatch, defect):
    _source, cold, _destination, original, _names = _flat_release(tmp_path)
    pin = tmp_path / "fixed.pin.json"
    write_packed_pin(pin, original)
    record = json.loads(pin.read_bytes())
    if defect == "schema": record["schema_version"] = 2
    elif defect == "boolean-schema": record["schema_version"] = True
    elif defect == "extra-field": record["implicit_latest"] = True
    elif defect == "missing-field": record.pop("manifest_sha256")
    elif defect == "manifest-sha": record["manifest_sha256"] = "0" * 64
    elif defect == "embedded-manifest": record["manifest"]["metadata"]["unverified"] = "changed"
    if defect == "json":
        pin.write_bytes(b"invalid JSON")
    elif defect == "duplicate-field":
        pin.write_text('{"schema_version":1,' + json.dumps(record)[1:])
    else:
        pin.write_text(json.dumps(record))
    monkeypatch.setattr(packed_snapshots, "resolve_latest_packed", lambda *a, **kw: pytest.fail("fallback consulted"))
    with pytest.raises(SnapshotError):
        resolve_packed_pin(cold, pin, expected_dataset="flat-results")


@pytest.mark.parametrize("defect", ["symlink", "fifo", "hardlink", "oversized"])
def test_packed_pin_metadata_is_bounded_regular_and_not_redirected(tmp_path, defect):
    _source, cold, _destination, original, _names = _flat_release(tmp_path)
    pin = tmp_path / "fixed.pin.json"
    write_packed_pin(pin, original)
    if defect == "symlink":
        target = tmp_path / "original.pin.json"
        pin.rename(target)
        pin.symlink_to(target)
    elif defect == "fifo":
        pin.unlink()
        os.mkfifo(pin)
    elif defect == "hardlink":
        os.link(pin, tmp_path / "another.pin.json")
    with pytest.raises(SnapshotError):
        resolve_packed_pin(cold, pin, maximum_pin_bytes=1 if defect == "oversized" else 1024**2)


def test_packed_pin_rechecks_control_generation_after_fixed_resolution(tmp_path, monkeypatch):
    _source, cold, _destination, original, _names = _flat_release(tmp_path)
    pin = tmp_path / "fixed.pin.json"
    write_packed_pin(pin, original)
    resolve = packed_snapshots.resolve_packed_snapshot_id
    def replace_pin(*args, **kwargs):
        fixed = resolve(*args, **kwargs)
        write_packed_pin(pin, fixed)
        return fixed
    monkeypatch.setattr(packed_snapshots, "resolve_packed_snapshot_id", replace_pin)
    with pytest.raises(SnapshotError, match="changed during fixed resolution"):
        resolve_packed_pin(cold, pin)


@pytest.mark.parametrize("defect", ["dataset", "missing-release", "missing-object", "missing-mount"])
def test_packed_pin_missing_or_wrong_cold_identity_is_not_an_implicit_cache_miss(tmp_path, monkeypatch, defect):
    _source, cold, _destination, original, _names = _flat_release(tmp_path)
    pin = tmp_path / "fixed.pin.json"
    write_packed_pin(pin, original)
    if defect == "missing-release":
        original.manifest_path.rename(original.manifest_path.with_name("retained-manifest.json"))
    elif defect == "missing-object":
        member = cold / original.manifest["archive"]["objects"][0]["relpath"]
        member.rename(member.with_name("retained-object"))
    elif defect == "missing-mount":
        (cold / ".stockagent-d-mount-required").write_text("not mounted")
    monkeypatch.setattr(packed_snapshots, "resolve_latest_packed", lambda *a, **kw: pytest.fail("missing fixed release fell back"))
    with pytest.raises(SnapshotError):
        resolve_packed_pin(cold, pin, expected_dataset="other" if defect == "dataset" else "flat-results")


def test_unmounted_d_cold_fallback_refuses_layout_and_publication(tmp_path: Path) -> None:
    sync_root = tmp_path / "cold"
    sync_root.mkdir()
    (sync_root / ".stockagent-d-mount-required").write_text("mount required\n")
    source = _source_tree(tmp_path)

    with pytest.raises(SnapshotError, match="not mounted"):
        initialize_packed_layout(sync_root, node_id="node-a")
    with pytest.raises(SnapshotError, match="not mounted"):
        publish_packed_snapshot(sync_root, "prices", source, node_id="node-a")
    assert not (sync_root / "objects").exists()


def test_packed_snapshot_round_trip_and_content_dedup(tmp_path: Path) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")

    resolved = publish_packed_snapshot(
        sync_root,
        "prices",
        source,
        loose_file_threshold_bytes=1024,
        pack_buckets=4,
    )
    archive = resolved.manifest["archive"]
    blobs = [item for item in archive["objects"] if item["kind"] == "blob"]
    packs = [item for item in archive["objects"] if item["kind"] == "pack"]

    assert len(blobs) == 1
    assert blobs[0]["file_count"] == 2
    assert 1 <= len(packs) <= 4
    assert archive["object_count"] == len(blobs) + len(packs)
    verification = verify_packed_snapshot(sync_root, resolved)
    assert verification["objects"] == archive["object_count"]

    target = fetch_packed_snapshot(sync_root, tmp_path / "materialized", resolved)
    assert (
        scan_tree(target)["portable_fingerprint_sha256"]
        == scan_tree(source)["portable_fingerprint_sha256"]
    )
    assert (target / "large-a.bin").read_bytes() == (
        source / "large-a.bin"
    ).read_bytes()
    assert (target / "text" / "first.json").read_text(encoding="utf-8") == (
        source / "text" / "first.json"
    ).read_text(encoding="utf-8")
    assert (target / "current").is_symlink()
    assert os.readlink(target / "current") == "text/first.json"
    assert verify_packed_snapshot(sync_root, resolved, materialized_path=target)[
        "materialized_verified"
    ]


def test_verification_reconstruction_preserves_all_bytes_without_ready_or_hot_exposure(tmp_path):
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    resolved = publish_packed_snapshot(sync_root, "prices", source, loose_file_threshold_bytes=1024)
    scratch = tmp_path / "private-recovery"
    scratch.mkdir(mode=0o700)
    target = fetch_packed_snapshot(sync_root, scratch, resolved, verification_only=True)
    assert target.name.startswith("." + resolved.manifest["snapshot_id"] + ".partial.")
    assert not (scratch / "prices" / resolved.manifest["snapshot_id"]).exists()
    assert not list(scratch.rglob("*.READY.json"))
    assert scan_tree(target)["portable_fingerprint_sha256"] == scan_tree(source)["portable_fingerprint_sha256"]
    assert verify_packed_snapshot(sync_root, resolved, materialized_path=target)["materialized_verified"]
    hot = fetch_packed_snapshot(sync_root, tmp_path / "managed", resolved)
    assert hot.name == resolved.manifest["snapshot_id"]
    assert list((tmp_path / "managed").rglob("*.READY.json"))


def test_recovery_progress_counts_verified_objects_and_materialized_bytes(tmp_path):
    source = _source_tree(tmp_path)
    cold = tmp_path / 'cold'
    initialize_packed_layout(cold, node_id='node-a')
    resolved = publish_packed_snapshot(cold, 'prices', source, loose_file_threshold_bytes=1024)
    scratch = tmp_path / 'private-recovery'
    scratch.mkdir(mode=0o700)
    events = []
    target = fetch_packed_snapshot(cold, scratch, resolved, verification_only=True, progress=events.append)
    objects = [event for event in events if event['stage'] == 'verify-cold-objects']
    copies = [event for event in events if event['stage'] == 'materialize-cold-archive']
    expected_objects = resolved.manifest['archive']['objects']
    assert objects[0]['completed_items'] == objects[0]['completed_bytes'] == 0
    assert objects[-1]['completed_items'] == objects[-1]['total_items'] == len(expected_objects)
    assert objects[-1]['completed_bytes'] == objects[-1]['total_bytes'] == sum(
        item['bytes'] for item in expected_objects)
    assert copies[0]['completed_items'] == copies[0]['completed_bytes'] == 0
    assert copies[-1]['completed_items'] == copies[-1]['total_items'] == resolved.manifest['source']['files']
    assert copies[-1]['completed_bytes'] == copies[-1]['total_bytes'] == resolved.manifest['source']['logical_bytes']
    assert scan_tree(target)['portable_fingerprint_sha256'] == scan_tree(source)['portable_fingerprint_sha256']
    assert not list(scratch.rglob('*.READY.json'))


def test_failed_object_hash_cannot_report_completed_verification(tmp_path):
    source = _source_tree(tmp_path)
    cold = tmp_path / 'cold'
    initialize_packed_layout(cold, node_id='node-a')
    resolved = publish_packed_snapshot(cold, 'prices', source, loose_file_threshold_bytes=1024)
    item = resolved.manifest['archive']['objects'][0]
    path = cold / item['relpath']
    value = path.read_bytes()
    path.write_bytes(bytes([value[0] ^ 1]) + value[1:])
    events = []
    with pytest.raises(SnapshotError, match='checksum mismatch'):
        verify_packed_snapshot(cold, resolved, progress=events.append)
    assert events and all(event['completed_items'] == 0 for event in events)
    assert source.is_dir() and path.exists()


@pytest.mark.parametrize("problem", ["non-private", "not-empty", "redirected"])
def test_verification_reconstruction_cannot_reuse_or_redirect_a_consumer_root(tmp_path, problem):
    parent = tmp_path / "scratch"
    parent.mkdir(mode=0o700)
    if problem == "non-private":
        parent.chmod(0o755)
    elif problem == "not-empty":
        (parent / "important").write_text("preserve")
    else:
        redirected = tmp_path / "alias"
        redirected.symlink_to(parent, target_is_directory=True)
        parent = redirected
    with pytest.raises(SnapshotError, match="fresh private"):
        fetch_packed_snapshot(tmp_path / "cold", parent, None, verification_only=True)
    assert parent.exists()


@pytest.mark.parametrize("value", [1, "true", None])
def test_verification_reconstruction_flag_rejects_ambiguous_values(tmp_path, value):
    with pytest.raises(SnapshotError, match="explicit boolean"):
        fetch_packed_snapshot(tmp_path / "cold", tmp_path / "unused", None, verification_only=value)


def test_blob_alias_reconstruction_reuses_verified_copy_and_preserves_each_mode(tmp_path, monkeypatch):
    source = _source_tree(tmp_path)
    (source / "large-a.bin").chmod(0o600)
    (source / "large-b.bin").chmod(0o640)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    resolved = publish_packed_snapshot(sync_root, "prices", source, loose_file_threshold_bytes=1024)
    calls = []
    original = packed_snapshots._copy_member_and_verify

    def traced(reader, path, **kwargs):
        if path.name in {"large-a.bin", "large-b.bin"}:
            calls.append((path.name, Path(reader.name)))
        return original(reader, path, **kwargs)

    monkeypatch.setattr(packed_snapshots, "_copy_member_and_verify", traced)
    target = fetch_packed_snapshot(sync_root, tmp_path / "materialized", resolved)
    assert [name for name, _ in calls] == ["large-a.bin", "large-b.bin"]
    assert sync_root in calls[0][1].parents
    assert calls[1][1].name == "large-a.bin" and sync_root not in calls[1][1].parents
    assert (target / "large-a.bin").read_bytes() == (target / "large-b.bin").read_bytes()
    assert (target / "large-a.bin").stat().st_ino != (target / "large-b.bin").stat().st_ino
    assert stat.S_IMODE((target / "large-a.bin").stat().st_mode) == 0o600
    assert stat.S_IMODE((target / "large-b.bin").stat().st_mode) == 0o640
    assert scan_tree(target)["portable_fingerprint_sha256"] == scan_tree(source)["portable_fingerprint_sha256"]


def test_batch_object_directory_barrier_precedes_manifest_and_head(tmp_path, monkeypatch):
    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    flushed = []
    flush = packed_snapshots._flush_object_directories
    write_manifest = packed_snapshots.write_immutable_json

    def record_flush(directories):
        assert not list((cold / "manifests").rglob("*.json"))
        assert not list((cold / "heads").rglob("*.json"))
        assert directories and cold in directories
        flush(directories)
        flushed.append(set(directories))

    def guarded_manifest(*args, **kwargs):
        assert len(flushed) == 1
        return write_manifest(*args, **kwargs)

    monkeypatch.setattr(packed_snapshots, "_flush_object_directories", record_flush)
    monkeypatch.setattr(packed_snapshots, "write_immutable_json", guarded_manifest)
    result = publish_packed_snapshot(cold, "prices", source, loose_file_threshold_bytes=1024,
                                     batch_directory_fsync=True)
    target = fetch_packed_snapshot(cold, tmp_path / "restore", result)
    assert scan_tree(target)["portable_fingerprint_sha256"] == scan_tree(source)["portable_fingerprint_sha256"]


def test_batch_directory_fsync_error_never_advances_head(tmp_path, monkeypatch):
    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initial = publish_packed_snapshot(cold, "prices", source)
    head = initial.head_path.read_bytes()
    (source / "new.bin").write_bytes(b"new-data" * 256)

    def reject_flush(directories):
        raise OSError("simulated cold directory durability failure")

    monkeypatch.setattr(packed_snapshots, "_flush_object_directories", reject_flush)
    with pytest.raises(OSError, match="durability failure"):
        publish_packed_snapshot(cold, "prices", source, batch_directory_fsync=True)
    assert initial.head_path.read_bytes() == head
    assert len(list(initial.manifest_path.parent.glob("*.json"))) == 1


def test_batched_object_install_fsyncs_payload_and_defers_directory(tmp_path, monkeypatch):
    cold = tmp_path / "cold"
    cold.mkdir()
    temporary = cold / "new.partial"
    temporary.write_bytes(b"durable payload")
    digest = packed_snapshots.sha256_file(temporary)
    destination = cold / f"objects/blobs/{digest[:2]}/{digest}.blob"
    events = []
    real_fsync = os.fsync

    def record_sync(fd):
        assert not destination.exists()  # file fsync before rename
        events.append(os.fstat(fd).st_mode)
        real_fsync(fd)

    monkeypatch.setattr(packed_snapshots.os, "fsync", record_sync)
    monkeypatch.setattr(packed_snapshots, "_fsync_directory", lambda *_: pytest.fail("per-object directory flush"))
    pending = set()
    packed_snapshots._install_immutable_object(cold, temporary, destination,
        expected_sha256=digest, pending_directories=pending)
    assert len(events) == 1 and stat.S_ISREG(events[0])
    assert destination.parent in pending and cold in pending


def test_metadata_only_resolver_allows_edge_to_hydrate_missing_objects(
    tmp_path: Path,
) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    published = publish_packed_snapshot(sync_root, "prices", source)
    for item in published.manifest["archive"]["objects"]:
        (sync_root / item["relpath"]).unlink()

    with pytest.raises(SnapshotError, match="not locally complete"):
        resolve_latest_packed(sync_root, "prices")

    resolved = resolve_latest_packed(sync_root, "prices", require_objects=False)
    by_id = resolve_packed_snapshot_id(
        sync_root,
        "prices",
        str(published.manifest["snapshot_id"]),
        require_objects=False,
    )
    assert resolved.manifest["snapshot_id"] == published.manifest["snapshot_id"]
    assert by_id.manifest["snapshot_id"] == published.manifest["snapshot_id"]


def test_shared_blob_reuse_does_not_write_duplicate_scratch(tmp_path, monkeypatch):
    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="test-node")
    copy_and_hash = packed_snapshots._copy_and_hash
    copied = []

    def record_copy(path, temporary):
        copied.append(path)
        return copy_and_hash(path, temporary)

    monkeypatch.setattr(packed_snapshots, "_copy_and_hash", record_copy)
    first = publish_packed_snapshot(cold, "first", source, loose_file_threshold_bytes=1024)
    assert len(copied) == 1  # two equal source files, one physical blob write
    copied.clear()
    second = publish_packed_snapshot(cold, "second", source, loose_file_threshold_bytes=1024)
    assert copied == []  # cross-dataset reuse still hashes the existing cold blob
    assert first.manifest["archive"]["stored_bytes"] == second.manifest["archive"]["stored_bytes"]
    assert second.manifest["archive"]["new_object_count"] == 0
    result = verify_packed_snapshot(cold, second, reconstruct_paths=["large-a.bin", "large-b.bin"])
    assert result["independently_reconstructed_files"] == 2


def test_shared_corrupt_blob_is_not_overwritten_or_published(tmp_path, monkeypatch):
    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="test-node")
    first = publish_packed_snapshot(cold, "first", source, loose_file_threshold_bytes=1024)
    blob = next(item for item in first.manifest["archive"]["objects"] if item["kind"] == "blob")
    path = cold / blob["relpath"]
    corrupt = b"X" * path.stat().st_size
    path.write_bytes(corrupt)
    monkeypatch.setattr(packed_snapshots, "_copy_and_hash", lambda *_: pytest.fail("duplicate scratch write"))
    with pytest.raises(SnapshotError, match="object is corrupt"):
        publish_packed_snapshot(cold, "second", source, loose_file_threshold_bytes=1024)
    assert path.read_bytes() == corrupt
    assert not (cold / "heads/second/test-node.json").exists()


def test_shared_blob_reuse_rechecks_source_signature(tmp_path, monkeypatch):
    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="test-node")
    first = publish_packed_snapshot(cold, "first", source, loose_file_threshold_bytes=1024)
    blob = next(item for item in first.manifest["archive"]["objects"] if item["kind"] == "blob")
    path = cold / blob["relpath"]
    hash_file = packed_snapshots.sha256_file

    def mutate_source_during_cold_hash(target):
        digest = hash_file(target)
        if target == path:
            original = source / "large-a.bin"
            original.write_bytes(b"Y" * original.stat().st_size)
        return digest

    monkeypatch.setattr(packed_snapshots, "sha256_file", mutate_source_during_cold_hash)
    with pytest.raises(SnapshotError, match="source file changed"):
        publish_packed_snapshot(cold, "second", source, loose_file_threshold_bytes=1024)
    assert not (cold / "heads/second/test-node.json").exists()


def test_shared_blob_reuse_rejects_cold_path_redirection(tmp_path):
    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="test-node")
    first = publish_packed_snapshot(cold, "first", source, loose_file_threshold_bytes=1024)
    blob = next(item for item in first.manifest["archive"]["objects"] if item["kind"] == "blob")
    path = cold / blob["relpath"]
    path.unlink()
    path.symlink_to(source / "large-a.bin")
    with pytest.raises(SnapshotError, match="object is redirected"):
        publish_packed_snapshot(cold, "second", source, loose_file_threshold_bytes=1024)
    assert not (cold / "heads/second/test-node.json").exists()


def test_selected_reconstruction_checks_pack_and_shared_blob_paths(tmp_path):
    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="test-node")
    resolved = publish_packed_snapshot(
        cold, "prices", source, loose_file_threshold_bytes=1024
    )
    result = verify_packed_snapshot(
        cold,
        resolved,
        reconstruct_paths=["text/first.json", "large-a.bin", "large-b.bin", "large-a.bin"],
    )
    assert result["independently_reconstructed_files"] == 3
    assert result["objects"] == resolved.manifest["archive"]["object_count"]
    assert not result["materialized_verified"]


@pytest.mark.parametrize("requested", ["current", "empty", "missing", "../escape"])
def test_selected_reconstruction_rejects_non_file_inventory_paths(tmp_path, requested):
    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="test-node")
    resolved = publish_packed_snapshot(cold, "prices", source)
    with pytest.raises(SnapshotError, match="non-file or unknown"):
        verify_packed_snapshot(cold, resolved, reconstruct_paths=[requested])


def test_selected_reconstruction_checks_file_hash_independent_of_zip_crc(tmp_path, monkeypatch):
    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="test-node")
    resolved = publish_packed_snapshot(cold, "prices", source)
    load_inventory = packed_snapshots._iter_inventory

    def wrong_file_hash(*args):
        for row in load_inventory(*args):
            if row["path"] == "text/first.json":
                row["sha256"] = "0" * 64
            yield row

    monkeypatch.setattr(packed_snapshots, "_iter_inventory", wrong_file_hash)
    with pytest.raises(SnapshotError, match="exact cold reconstruction failed"):
        verify_packed_snapshot(cold, resolved, reconstruct_paths=["text/first.json"])


def test_empty_reconstruction_request_still_hashes_all_cold_objects(tmp_path):
    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="test-node")
    resolved = publish_packed_snapshot(cold, "prices", source)
    obj = cold / resolved.manifest["archive"]["objects"][0]["relpath"]
    payload = bytearray(obj.read_bytes())
    payload[0] ^= 1
    obj.write_bytes(payload)
    with pytest.raises(SnapshotError, match="checksum mismatch"):
        verify_packed_snapshot(cold, resolved, reconstruct_paths=[])


def test_explicit_missing_object_recovery_publishes_current_bytes_under_new_identity(tmp_path):
    source = _source_tree(tmp_path)
    root = tmp_path / "cold"
    initialize_packed_layout(root, node_id="node-a")
    original = publish_packed_snapshot(root, "prices", source, pack_buckets=1)
    original_manifest = original.manifest_path.read_bytes()
    old = original.manifest["archive"]["objects"][0]
    (root / old["relpath"]).unlink()
    (source / "text/first.json").write_text('{"first": 2}\n')
    with pytest.raises(SnapshotError):
        publish_packed_snapshot(root, "prices", source)
    repaired = publish_packed_snapshot(root, "prices", source, pack_buckets=1,
                                      recover_missing_base_objects=True)
    assert repaired.manifest["snapshot_id"] != original.manifest["snapshot_id"]
    assert original.manifest_path.read_bytes() == original_manifest
    assert not (root / old["relpath"]).exists()  # newer bytes never pretend to be the lost old version
    assert repaired.manifest["archive"]["base_missing_objects_repacked_from_current_source"][0]["sha256"] == old["sha256"]
    verify_packed_snapshot(root, repaired)
    assert resolve_latest_packed(root, "prices").manifest["snapshot_id"] == repaired.manifest["snapshot_id"]


def test_recovery_checks_corrupt_reused_objects_before_head_update(tmp_path):
    source = _source_tree(tmp_path)
    root = tmp_path / "cold"
    initialize_packed_layout(root, node_id="node-a")
    original = publish_packed_snapshot(root, "prices", source, loose_file_threshold_bytes=1024)
    head = original.head_path.read_bytes()
    blob = next(o for o in original.manifest["archive"]["objects"] if o["kind"] == "blob")
    original_size = (root / blob["relpath"]).stat().st_size
    (root / blob["relpath"]).write_bytes(b"X" * original_size)
    (source / "text/first.json").write_text('{"first": 2}\n')
    with pytest.raises(SnapshotError, match="checksum mismatch"):
        publish_packed_snapshot(root, "prices", source, loose_file_threshold_bytes=1024,
                                recover_missing_base_objects=True)
    assert original.head_path.read_bytes() == head


def test_full_store_audit_hashes_all_objects_without_deleting(tmp_path: Path) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    published = publish_packed_snapshot(sync_root, "prices", source)

    result = audit_packed_store(sync_root)

    assert result["valid_manifests"] == 1
    assert result["resolved_datasets"] == 1
    assert result["stored_objects"] == published.manifest["archive"]["object_count"] + 1
    assert result["all_stored_object_sha256_valid"] is True
    assert result["deleted"] == 0


def test_group_writable_packed_root_repairs_public_directory_modes(
    tmp_path: Path,
) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    sync_root.mkdir()
    sync_root.chmod(0o2770)
    stale_bucket = sync_root / "objects" / "inventories" / "ff"
    stale_bucket.mkdir(parents=True)
    stale_bucket.chmod(0o2755)

    initialize_packed_layout(sync_root, node_id="node-a")
    resolved = publish_packed_snapshot(
        sync_root,
        "prices",
        source,
        loose_file_threshold_bytes=1024,
        pack_buckets=4,
    )

    public_directories = [
        path
        for top in ("heads", "manifests", "objects")
        for path in (sync_root / top).rglob("*")
        if path.is_dir()
    ]
    public_directories.extend(
        sync_root / top for top in ("heads", "manifests", "objects")
    )
    assert stale_bucket in public_directories
    assert all(path.stat().st_mode & stat.S_IWGRP for path in public_directories)
    assert all(path.stat().st_mode & stat.S_ISGID for path in public_directories)
    assert resolved.manifest_path.parent.stat().st_mode & stat.S_IWGRP


def test_packed_snapshot_fetch_subtree_is_atomic_and_verified(tmp_path: Path) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    resolved = publish_packed_snapshot(
        sync_root,
        "prices",
        source,
        loose_file_threshold_bytes=1024,
        pack_buckets=4,
    )

    target = fetch_packed_subtree(
        sync_root, tmp_path / "materialized-subtree", resolved, "text"
    )

    assert target.name == "text"
    assert (target / "first.json").read_bytes() == (
        source / "text" / "first.json"
    ).read_bytes()
    assert (target / "second.csv").read_bytes() == (
        source / "text" / "second.csv"
    ).read_bytes()
    assert not (target / "large-a.bin").exists()
    assert (
        fetch_packed_subtree(
            sync_root, tmp_path / "materialized-subtree", resolved, "text"
        )
        == target
    )


def test_packed_snapshot_fetch_subtree_rejects_missing_directory(
    tmp_path: Path,
) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    resolved = publish_packed_snapshot(sync_root, "prices", source)

    with pytest.raises(SnapshotError, match="subtree is missing"):
        fetch_packed_subtree(
            sync_root, tmp_path / "materialized-subtree", resolved, "missing"
        )


def test_packed_snapshot_excludes_reproducible_subtree(tmp_path: Path) -> None:
    source = _source_tree(tmp_path)
    cache = source / "text" / "cache"
    cache.mkdir()
    (cache / "derived.npy").write_bytes(b"reproducible-cache")
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")

    resolved = publish_packed_snapshot(
        sync_root,
        "prices",
        source,
        excluded_subtrees=["text/cache"],
    )
    target = fetch_packed_snapshot(sync_root, tmp_path / "materialized", resolved)

    assert resolved.manifest["source"]["excluded_subtrees"] == ["text/cache"]
    assert not (target / "text" / "cache").exists()
    assert (target / "text" / "first.json").is_file()
    assert verify_packed_snapshot(sync_root, resolved, materialized_path=target)[
        "materialized_verified"
    ]


def test_packed_snapshot_can_select_only_small_files(tmp_path: Path) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")

    resolved = publish_packed_snapshot(
        sync_root,
        "small-prices",
        source,
        loose_file_threshold_bytes=1024,
        pack_buckets=4,
        maximum_file_bytes=1023,
    )
    target = fetch_packed_snapshot(sync_root, tmp_path / "materialized", resolved)

    assert resolved.manifest["source"]["selection"] == {
        "maximum_file_bytes": 1023,
        "symlinks": "included",
    }
    assert resolved.manifest["source"]["files"] == 2
    assert resolved.manifest["source"]["omitted_files_above_maximum"] == 2
    assert all(
        item["kind"] == "pack" for item in resolved.manifest["archive"]["objects"]
    )
    assert (target / "text" / "first.json").is_file()
    assert not (target / "large-a.bin").exists()
    assert not (target / "large-b.bin").exists()
    assert verify_packed_snapshot(sync_root, resolved, materialized_path=target)[
        "materialized_verified"
    ]


def test_explicit_root_files_publish_without_copies_or_unrelated_traversal(tmp_path, monkeypatch):
    source = _source_tree(tmp_path)
    os.mkfifo(source / "unrelated-fifo")
    cold = tmp_path / "cold"
    names = ["large-a.bin", "large-b.bin"]

    def source_generations():
        return {name: (packed_snapshots._source_stat((source / name).stat()),
                       stat.S_IMODE((source / name).stat().st_mode)) for name in names}

    before = source_generations()
    walk = os.walk

    def no_source_walk(path, *args, **kwargs):
        assert Path(path).resolve() != source.resolve(), "selected publication traversed the whole namespace"
        return walk(path, *args, **kwargs)

    monkeypatch.setattr(os, "walk", no_source_walk)
    resolved = publish_packed_snapshot(cold, "one-forward", source, node_id="node-a",
        loose_file_threshold_bytes=1024, included_root_files=reversed(names),
        metadata={"scope": "two exact regular files only"})
    assert resolved.manifest["source"]["selection"] == {
        "maximum_file_bytes": None, "symlinks": "excluded", "included_root_files": names}
    assert resolved.manifest["source"]["files"] == 2
    assert resolved.manifest["source"]["directories"] == resolved.manifest["source"]["symlinks"] == 0
    assert source_generations() == before  # reading may advance atime, never mutation clocks
    fixed = resolve_packed_snapshot_id(cold, "one-forward", resolved.manifest["snapshot_id"])
    target = fetch_packed_snapshot(cold, tmp_path / "materialized", fixed)
    assert sorted(p.name for p in target.iterdir()) == names
    assert all((target / name).read_bytes() == (source / name).read_bytes() for name in names)
    assert verify_packed_snapshot(cold, fixed, materialized_path=target)["materialized_verified"]


@pytest.mark.parametrize("selection", [
    [], "large-a.bin", b"large-a.bin", {"large-a.bin": True}, 123, False,
    ["large-a.bin", "large-a.bin"], ["."], [".."], [".git"], [""],
    ["/etc/passwd"], ["text/first.json"], ["text/../large-a.bin"], ["./large-a.bin"],
    ["large-a.bin/"], ["text\\first.json"], ["line\nname"], ["nul\0name"], [None],
])
def test_invalid_root_selection_is_refused_before_cold_layout(tmp_path, selection):
    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    with pytest.raises(SnapshotError, match="root file"):
        publish_packed_snapshot(cold, "one-forward", source, node_id="node-a",
                                included_root_files=selection)
    assert not cold.exists()


def test_root_selection_generator_is_bounded_before_cold_layout(tmp_path, monkeypatch):
    source = _source_tree(tmp_path)
    monkeypatch.setattr(packed_snapshots, "MAX_INCLUDED_ROOT_FILES", 2)
    observed = []

    def selections():
        for index in range(1000000):
            observed.append(index)
            yield f"file-{index}"

    with pytest.raises(SnapshotError, match="bound"):
        publish_packed_snapshot(tmp_path / "cold", "one-forward", source,
                                included_root_files=selections())
    assert observed == [0, 1, 2] and not (tmp_path / "cold").exists()


@pytest.mark.parametrize("filters", [{"maximum_file_bytes": 1}, {"excluded_subtrees": ["large-a.bin"]}])
def test_exact_root_selection_cannot_silently_omit_requested_members(tmp_path, filters):
    source = _source_tree(tmp_path)
    with pytest.raises(SnapshotError, match="omission filters"):
        publish_packed_snapshot(tmp_path / "cold", "one-forward", source,
                                included_root_files=["large-a.bin"], **filters)
    assert not (tmp_path / "cold").exists()


@pytest.mark.parametrize("kind", ["directory", "symlink", "fifo", "missing"])
def test_explicit_root_member_must_be_a_present_regular_file(tmp_path, kind):
    source = _source_tree(tmp_path)
    if kind == "directory":
        name = "text"
    elif kind == "symlink":
        name = "current"
    elif kind == "fifo":
        name = "fifo"
        os.mkfifo(source / name)
    else:
        name = "absent"
    with pytest.raises((SnapshotError, FileNotFoundError)):
        publish_packed_snapshot(tmp_path / "cold", "one-forward", source, node_id="node-a",
                                included_root_files=[name])
    assert not list((tmp_path / "cold" / "heads").rglob("*.json"))


def test_selection_does_not_reuse_a_full_tree_or_different_provenance_head(tmp_path):
    source, cold = tmp_path / "source", tmp_path / "cold"
    source.mkdir()
    (source / "result.forward").write_bytes(b"exact original bytes")
    full = publish_packed_snapshot(cold, "one-forward", source, node_id="node-a")
    selected = publish_packed_snapshot(cold, "one-forward", source,
        included_root_files=["result.forward"], metadata={"commit": "accepted-v1"})
    assert full.manifest["snapshot_id"] != selected.manifest["snapshot_id"]
    same = publish_packed_snapshot(cold, "one-forward", source,
        included_root_files=["result.forward"], metadata={"commit": "accepted-v1"})
    assert same.manifest_sha256 == selected.manifest_sha256
    revised = publish_packed_snapshot(cold, "one-forward", source,
        included_root_files=["result.forward"], metadata={"commit": "accepted-v2"})
    assert revised.manifest["snapshot_id"] != selected.manifest["snapshot_id"]
    restored_full = publish_packed_snapshot(cold, "one-forward", source)
    assert "included_root_files" not in restored_full.manifest["source"]["selection"]
    # A fixed release never silently follows a newer head.
    fixed = resolve_packed_snapshot_id(cold, "one-forward", selected.manifest["snapshot_id"])
    assert fixed.manifest_sha256 == selected.manifest_sha256


def test_unrelated_writer_does_not_invalidate_an_explicit_file_release(tmp_path):
    source = _source_tree(tmp_path)
    calls = 0

    def unrelated_writer():
        nonlocal calls
        calls += 1
        (source / f"unrelated-{calls}").write_bytes(b"different completed key")

    resolved = publish_packed_snapshot(tmp_path / "cold", "one-forward", source,
        node_id="node-a", included_root_files=["large-a.bin"], source_guard=unrelated_writer)
    assert calls == 3 and resolved.manifest["source"]["files"] == 1
    assert resolved.manifest["source"]["logical_bytes"] == (source / "large-a.bin").stat().st_size


def test_selected_file_mutation_retains_existing_head(tmp_path, monkeypatch):
    source, cold = _source_tree(tmp_path), tmp_path / "cold"
    first = publish_packed_snapshot(cold, "one-forward", source, node_id="node-a",
                                   loose_file_threshold_bytes=1, included_root_files=["large-a.bin"])
    head_before = first.head_path.read_bytes()
    (source / "large-a.bin").write_bytes(b"changed bytes before new publication")
    original = packed_snapshots._copy_and_hash

    def mutate_after_copy(path, target):
        digest = original(path, target)
        path.write_bytes(b"racing changed bytes after copy")
        return digest

    monkeypatch.setattr(packed_snapshots, "_copy_and_hash", mutate_after_copy)
    with pytest.raises(SnapshotError, match="source file changed"):
        publish_packed_snapshot(cold, "one-forward", source,
                                loose_file_threshold_bytes=1, included_root_files=["large-a.bin"])
    assert first.head_path.read_bytes() == head_before


def test_replaced_selected_source_directory_cannot_commit_a_head(tmp_path, monkeypatch):
    import shutil

    source, cold = _source_tree(tmp_path), tmp_path / "cold"
    first = publish_packed_snapshot(cold, "one-forward", source, node_id="node-a",
                                   included_root_files=["large-a.bin"])
    before = first.head_path.read_bytes()
    original = packed_snapshots._collect_entries
    calls = 0

    def replace_after_first_census(path, **options):
        nonlocal calls
        result = original(path, **options)
        calls += 1
        if calls == 1:
            path.rename(tmp_path / "original-source")
            shutil.copytree(tmp_path / "original-source", path, symlinks=True)
        return result

    monkeypatch.setattr(packed_snapshots, "_collect_entries", replace_after_first_census)
    with pytest.raises(SnapshotError, match="source directory was replaced"):
        publish_packed_snapshot(cold, "one-forward", source, included_root_files=["large-a.bin"])
    assert first.head_path.read_bytes() == before and (tmp_path / "original-source").is_dir()


@pytest.mark.parametrize("change", ["missing", "extra", "nested", "duplicate", "null", "directory", "symlink-policy", "filter"])
def test_selected_inventory_cannot_claim_a_different_scope(tmp_path, change):
    import copy

    source, cold = _source_tree(tmp_path), tmp_path / "cold"
    resolved = publish_packed_snapshot(cold, "one-forward", source, node_id="node-a",
                                      included_root_files=["large-a.bin", "large-b.bin"])
    manifest = copy.deepcopy(resolved.manifest)
    selection = manifest["source"]["selection"]
    entries = packed_snapshots._load_inventory(cold, manifest)
    if change == "missing": selection["included_root_files"].pop()
    elif change == "extra": selection["included_root_files"].append("unrelated")
    elif change == "nested": selection["included_root_files"][0] = "nested/file"
    elif change == "duplicate": selection["included_root_files"][1] = "large-a.bin"
    elif change == "null": selection["included_root_files"] = None
    elif change == "directory": entries[0]["kind"] = "directory"
    elif change == "symlink-policy": selection["symlinks"] = "included"
    else: selection["maximum_file_bytes"] = 5
    with pytest.raises(SnapshotError):
        packed_snapshots._validate_inventory(manifest, entries)


def test_unchanged_publish_is_a_semantic_noop(tmp_path: Path) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    first = publish_packed_snapshot(
        sync_root,
        "prices",
        source,
        loose_file_threshold_bytes=1024,
        pack_buckets=4,
    )
    second = publish_packed_snapshot(
        sync_root,
        "prices",
        source,
        loose_file_threshold_bytes=1024,
        pack_buckets=4,
    )

    assert second.manifest["snapshot_id"] == first.manifest["snapshot_id"]
    assert second.manifest_sha256 == first.manifest_sha256
    assert second.head_path == first.head_path
    assert len(list((sync_root / "manifests" / "prices").glob("*.json"))) == 1


def test_source_guard_vetoes_semantic_noop_without_changing_existing_head(tmp_path: Path) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    first = publish_packed_snapshot(sync_root, "prices", source, pack_buckets=2)
    head_before = first.head_path.read_bytes()
    calls = 0

    def guard() -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise SnapshotError("producer proof changed")

    with pytest.raises(SnapshotError, match="producer proof changed"):
        publish_packed_snapshot(sync_root, "prices", source, pack_buckets=2, source_guard=guard)
    assert calls == 2
    assert first.head_path.read_bytes() == head_before
    assert len(list((sync_root / "manifests/prices").glob("*.json"))) == 1


@pytest.mark.parametrize("defer", [False, True])
def test_transport_notification_releases_global_publish_locks_even_for_noop(tmp_path, monkeypatch, defer):
    import fcntl
    from stockagent.data_sync import syncthing_scan

    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="node-a")
    observed = []

    def notify(sync_root, dataset, **kwargs):
        assert (sync_root / "heads" / dataset / "node-a.json").is_file()
        for name in ("publish-retention-global.lock", "publish-prices.lock"):
            with (sync_root / ".local-state/locks" / name).open("a+b") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        observed.append(kwargs)
        return True

    monkeypatch.setattr(syncthing_scan, "queue_after_publish" if defer else "scan_after_publish", notify)
    if defer:
        monkeypatch.setattr(syncthing_scan, "scan_after_publish", lambda *_a, **_k: pytest.fail("deferred commit cannot do network scan"))
    first = publish_packed_snapshot(cold, "prices", source, defer_scan=defer)
    second = publish_packed_snapshot(cold, "prices", source, defer_scan=defer)
    assert first.manifest["snapshot_id"] == second.manifest["snapshot_id"]
    assert len(observed) == 2
    assert observed[0]["new_object_paths"]


def test_independent_publishers_share_retention_but_same_dataset_stays_exclusive(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    import fcntl
    import threading
    from stockagent.data_sync import packed_snapshots as packed

    cold, source = tmp_path / 'cold', _source_tree(tmp_path)
    initialize_packed_layout(cold, node_id='node-a')
    entered, release = threading.Event(), threading.Event()
    original = packed._write_pack
    def pause_first(root, entries, temporary, **options):
        if threading.current_thread().name.startswith('paused-publisher'):
            entered.set()
            assert release.wait(15)
        return original(root, entries, temporary, **options)
    monkeypatch.setattr(packed, '_write_pack', pause_first)
    with ThreadPoolExecutor(1, thread_name_prefix='paused-publisher') as executor:
        future = executor.submit(publish_packed_snapshot, cold, 'first', source)
        try:
            assert entered.wait(10)
            with (cold / '.local-state/locks/publish-retention-global.lock').open('a+b') as owner:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with pytest.raises(packed.PackedPublicationBusy):
                publish_packed_snapshot(cold, 'first', source, publication_lock_nonblocking=True)
            second = publish_packed_snapshot(cold, 'second', source, publication_lock_nonblocking=True)
        finally:
            release.set()
        first = future.result(timeout=15)
    assert first.manifest['dataset'] == 'first' and second.manifest['dataset'] == 'second'
    verify_packed_snapshot(cold, first)
    verify_packed_snapshot(cold, second)


@pytest.mark.parametrize('batched', [False, True])
def test_racing_cas_install_never_replaces_the_winners_inode(tmp_path, monkeypatch, batched):
    import hashlib
    import os
    from stockagent.data_sync import packed_snapshots as packed
    cold = tmp_path / 'cold'
    initialize_packed_layout(cold, node_id='node-a')
    temporary = cold / '.local-state/staging/ours'
    temporary.write_bytes(b'exact source')
    destination = cold / 'objects/blobs/aa/winner'
    destination.parent.mkdir()
    winner = cold / '.local-state/staging/theirs'
    winner.write_bytes(temporary.read_bytes())
    native, observed, flushed = os.link, [], []
    def competing_link(source, target, **options):
        native(winner, target)
        observed.append(target.stat().st_ino)
        return native(source, target, **options)
    monkeypatch.setattr(packed.os, 'link', competing_link)
    monkeypatch.setattr(packed, '_fsync_directory', flushed.append)
    pending = set() if batched else None
    assert packed._install_immutable_object(cold, temporary, destination,
        expected_sha256=hashlib.sha256(b'exact source').hexdigest(),
        pending_directories=pending) is True
    assert destination.stat().st_ino == observed[0] and not temporary.exists()
    if batched:
        assert destination.parent in pending and cold in pending and not flushed
    else:
        assert flushed == [destination.parent]


@pytest.mark.parametrize('batched', [False, True])
def test_reused_cas_installer_durability_does_not_depend_on_its_peers(tmp_path, monkeypatch, batched):
    cold = tmp_path / 'cold'
    initialize_packed_layout(cold, node_id='node-a')
    temporary = cold / '.local-state/staging/ours'
    temporary.write_bytes(b'exact source')
    destination = cold / 'objects/blobs/aa/winner'
    destination.parent.mkdir()
    destination.write_bytes(temporary.read_bytes())
    inode = destination.stat().st_ino
    flushed = []
    monkeypatch.setattr(packed_snapshots, '_fsync_directory', flushed.append)
    pending = set() if batched else None
    assert packed_snapshots._install_immutable_object(cold, temporary, destination,
        expected_sha256=packed_snapshots.sha256_file(destination),
        pending_directories=pending) is True
    assert destination.stat().st_ino == inode and not temporary.exists()
    if batched:
        assert destination.parent in pending and cold in pending and not flushed
    else:
        assert flushed == [destination.parent]


@pytest.mark.parametrize('rules', ['finmind_parquet_promote_v1', False, [None], [{}], ['unknown']])
def test_private_staging_rules_fail_closed_before_source_publication(tmp_path, rules):
    source = _source_tree(tmp_path)
    cold = tmp_path / 'cold'
    with pytest.raises(SnapshotError, match='staging rules'):
        publish_packed_snapshot(cold, 'first', source, private_staging_file_rules=rules)
    assert not cold.exists()


def test_known_private_staging_rules_cannot_change_shared_publication_scope(tmp_path):
    source = _source_tree(tmp_path)
    cold = tmp_path / 'cold'
    with pytest.raises(SnapshotError, match='private observation cohort'):
        publish_packed_snapshot(cold, 'first', source,
                               private_staging_file_rules=['finmind_parquet_promote_v1'])
    assert not cold.exists()


@pytest.mark.parametrize('name,expected', [
    ('.openbb_l1.duckdb.1234.tmp', True),
    ('_state/.l1_compaction_latest.json.1234.tmp', True),
    ('catalog/.l1_compaction_status.parquet.1234.tmp', True),
    ('raw/.openbb_l1.duckdb.1234.tmp', False),
    ('.observation.parquet.1234.tmp', False),
    ('_state/.observation.parquet.1234.tmp', False),
    ('catalog/notes.tmp', False),
])
def test_openbb_uncommitted_sibling_rule_is_explicit_and_not_a_generic_tmp_filter(name, expected):
    assert packed_snapshots._is_private_writer_staging(name, ('openbb_l1_atomic_sibling_v1',)) is expected
    assert not packed_snapshots._is_private_writer_staging(name, ())


def test_frozen_observation_cannot_silently_disappear_before_census(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    frozen = tmp_path / 'frozen'
    frozen.write_bytes(b'original observation')
    with pytest.raises(SnapshotError, match='frozen observation paths disappeared'):
        packed_snapshots._collect_entries(source, file_overrides={'missing.parquet': frozen})


def test_queue_failure_after_head_commit_is_not_an_unpublished_release(tmp_path, monkeypatch):
    from stockagent.data_sync import syncthing_scan

    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="node-a")
    monkeypatch.setattr(syncthing_scan, "queue_after_publish", lambda *_a, **_k: (_ for _ in ()).throw(SnapshotError("queue fsync failed")))
    with pytest.raises(SnapshotError, match="queue fsync failed"):
        publish_packed_snapshot(cold, "prices", source, defer_scan=True)
    assert (cold / "heads/prices/node-a.json").is_file()


def test_deferred_scan_cannot_claim_queue_when_d_primary_disappears(tmp_path, monkeypatch):
    from stockagent.data_sync import syncthing_scan

    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="node-a")
    monkeypatch.setattr(syncthing_scan, "queue_after_publish", lambda *_a, **_k: False)
    with pytest.raises(SnapshotError, match="durable scan intent was not queued"):
        publish_packed_snapshot(cold, "prices", source, defer_scan=True)
    assert (cold / "heads/prices/node-a.json").is_file()


def test_changed_small_file_uses_delta_pack_and_reuses_old_members(
    tmp_path: Path,
) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    first = publish_packed_snapshot(
        sync_root,
        "prices",
        source,
        loose_file_threshold_bytes=1024,
        pack_buckets=1,
    )
    (source / "text" / "first.json").write_text(
        '{"first": 2}\n', encoding="utf-8"
    )

    second = publish_packed_snapshot(
        sync_root,
        "prices",
        source,
        loose_file_threshold_bytes=1024,
        pack_buckets=1,
    )
    archive = second.manifest["archive"]

    assert archive["base_snapshot_id"] == first.manifest["snapshot_id"]
    assert archive["reused_files"] == 3
    assert archive["changed_files"] == 1
    assert archive["new_stored_bytes"] < archive["stored_bytes"]
    assert any(
        item.get("member_selection") == "subset"
        for item in archive["objects"]
    )
    target = fetch_packed_snapshot(sync_root, tmp_path / "materialized", second)
    assert (target / "text" / "first.json").read_text(encoding="utf-8") == (
        '{"first": 2}\n'
    )
    assert (target / "text" / "second.csv").is_file()
    assert verify_packed_snapshot(sync_root, second, materialized_path=target)[
        "materialized_verified"
    ]


def test_latest_packed_snapshot_uses_per_node_heads(tmp_path: Path) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    publish_packed_snapshot(sync_root, "prices", source, pack_buckets=2)
    initialize_packed_layout(
        sync_root,
        node_id="node-b",
        replace_node_id=True,
    )
    (source / "text" / "first.json").write_text(
        '{"publisher": "node-b"}\n', encoding="utf-8"
    )
    expected = publish_packed_snapshot(sync_root, "prices", source, pack_buckets=2)

    actual = resolve_latest_packed(sync_root, "prices")

    assert actual.manifest["snapshot_id"] == expected.manifest["snapshot_id"]
    assert actual.manifest["publisher"]["node_id"] == "node-b"


def test_corrupt_object_is_rejected(tmp_path: Path) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    resolved = publish_packed_snapshot(
        sync_root,
        "prices",
        source,
        loose_file_threshold_bytes=1024,
        pack_buckets=2,
    )
    pack = next(
        item
        for item in resolved.manifest["archive"]["objects"]
        if item["kind"] == "pack"
    )
    pack_path = sync_root.joinpath(*Path(pack["relpath"]).parts)
    with pack_path.open("r+b") as stream:
        stream.seek(max(0, pack_path.stat().st_size // 2))
        original = stream.read(1)
        stream.seek(-1, os.SEEK_CUR)
        stream.write(bytes([original[0] ^ 0xFF]))

    with pytest.raises(SnapshotError, match="checksum mismatch"):
        verify_packed_snapshot(sync_root, resolved)


def test_small_pack_verification_reuses_one_bounded_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    resolved = publish_packed_snapshot(
        sync_root, "prices", source, loose_file_threshold_bytes=1024,
        pack_buckets=2,
    )
    packs = {
        sync_root / item["relpath"]
        for item in resolved.manifest["archive"]["objects"]
        if item["kind"] == "pack"
    }
    blobs = {
        sync_root / item["relpath"]
        for item in resolved.manifest["archive"]["objects"]
        if item["kind"] == "blob"
    }
    original_sha256_file = packed_snapshots.sha256_file
    hashed_paths: list[Path] = []

    def traced_sha256_file(path: Path) -> str:
        hashed_paths.append(path)
        return original_sha256_file(path)

    monkeypatch.setattr(packed_snapshots, "sha256_file", traced_sha256_file)
    bounded = verify_packed_snapshot(sync_root, resolved)
    assert not packs.intersection(hashed_paths)
    assert blobs.issubset(hashed_paths)

    hashed_paths.clear()
    monkeypatch.setattr(packed_snapshots, "MAX_IN_MEMORY_PACK_VERIFY_BYTES", 0)
    streamed = verify_packed_snapshot(sync_root, resolved)
    assert packs.issubset(hashed_paths)
    assert streamed == bounded


def test_verify_cli_profile_keeps_complete_verification_result(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    published = publish_packed_snapshot(sync_root, "prices", source)

    assert packed_snapshot_main([
        "verify", "prices", "--snapshot-id", published.manifest["snapshot_id"],
        "--sync-root", str(sync_root), "--profile",
    ]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["snapshot_id"] == published.manifest["snapshot_id"]
    assert payload["objects"] == published.manifest["archive"]["object_count"]
    assert payload["profile"]["scope"] == (
        "local_process_resolution_and_full_verify_only"
    )
    assert payload["profile"]["elapsed_seconds"] >= 0
    assert payload["profile"]["rchar_bytes"] is None or (
        payload["profile"]["rchar_bytes"] > 0
    )


@pytest.mark.parametrize("chunked", [False, True])
def test_verify_cli_reconstructs_every_original_without_materialization(tmp_path, capsys, chunked):
    source = _source_tree(tmp_path)
    (source / "large.bin").write_bytes(bytes(range(256)) * 17)
    sync_root = tmp_path / "sync"
    published = publish_packed_snapshot(sync_root, "prices", source, node_id="node-a",
        loose_file_threshold_bytes=1000, **({"file_chunk_bytes": 1024} if chunked else {}))
    receipt, progress = tmp_path / "result.json", tmp_path / "progress.json"
    assert packed_snapshot_main([
        "verify", "prices", "--snapshot-id", published.manifest["snapshot_id"],
        "--sync-root", str(sync_root), "--reconstruct-all-files", "--profile",
        "--result-receipt", str(receipt), "--progress-receipt", str(progress),
    ]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result == json.loads(receipt.read_text())
    assert result["full_original_file_reconstruction_verified"] is True
    assert result["independently_reconstructed_files"] == result["source_files"] == published.manifest["source"]["files"]
    assert result["materialized_verified"] is False
    assert result["proof_scope"] == "selected_fixed_packed_release_not_NAS_or_full_history"
    telemetry = json.loads(progress.read_text())
    assert telemetry["stage"] == "finished" and telemetry["acceptance_proof"] is False
    assert telemetry["snapshot_id"] == published.manifest["snapshot_id"]
    assert not (tmp_path / "materialized").exists()


@pytest.mark.parametrize("option", ["--result-receipt", "--progress-receipt"])
def test_verify_cli_preserves_prior_evidence_before_resolving(tmp_path, capsys, option):
    receipt = tmp_path / "prior.json"
    receipt.write_bytes(b"original receipt")
    assert packed_snapshot_main(["verify", "absent", "--sync-root", str(tmp_path / "missing"),
                                 option, str(receipt)]) == 2
    assert "must be new paths" in capsys.readouterr().err
    assert receipt.read_bytes() == b"original receipt"


def test_verify_cli_refuses_shared_result_progress_path(tmp_path, capsys):
    assert packed_snapshot_main(["verify", "absent", "--sync-root", str(tmp_path / "missing"),
                                 "--result-receipt", str(tmp_path / "new.json"),
                                 "--progress-receipt", str(tmp_path / "new.json")]) == 2
    assert "paths must differ" in capsys.readouterr().err
    assert not (tmp_path / "new.json").exists()


def test_verify_cli_damaged_object_never_creates_acceptance_receipt(tmp_path, capsys):
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    release = publish_packed_snapshot(sync_root, "prices", source, node_id="node-a")
    obj = release.manifest["archive"]["objects"][0]
    target = sync_root / obj["relpath"]
    original = target.read_bytes()
    target.write_bytes(bytes([original[0] ^ 1]) + original[1:])
    result, progress = tmp_path / "result.json", tmp_path / "progress.json"
    assert packed_snapshot_main(["verify", "prices", "--sync-root", str(sync_root),
                                 "--reconstruct-all-files", "--result-receipt", str(result),
                                 "--progress-receipt", str(progress)]) == 2
    assert "checksum mismatch" in capsys.readouterr().err
    assert not result.exists()
    assert json.loads(progress.read_text())["acceptance_proof"] is False


def test_symlink_that_escapes_source_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "unsafe").symlink_to("../outside")
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")

    with pytest.raises(SnapshotError, match="escapes the snapshot root"):
        publish_packed_snapshot(sync_root, "prices", source)
