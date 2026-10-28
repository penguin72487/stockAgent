"""Exact retained-original retirement through the existing shared executor."""
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
import time

import polars as pl
import pytest

from stockagent.data_sync import artifact_retirement as retirement
from stockagent.data_sync import materialized_cache, packed_snapshots as packed
from stockagent.data_sync import retained_source as retained
from stockagent.data_sync import cache_capacity
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.packed_retention import RetentionConfig

DATASET = "fred-crypto-macro"
REAL_USAGE = retirement._retained_raw_usage


@pytest.fixture
def retained_options(tmp_path, monkeypatch, request):
    source, cold, pins = (tmp_path / name for name in ("source", "cold", "pin-owner"))
    count = getattr(request, "param", 2)
    capacity_policy = {**cache_capacity.load_capacity_policy(), "minimum_free_bytes": 1024**2,
        "materialized_capacity_bytes": 128 * 1024**2}
    monkeypatch.setattr(cache_capacity, "load_capacity_policy", lambda: capacity_policy)
    (tmp_path / "capacity-controls").mkdir(mode=0o700)
    monkeypatch.setattr(cache_capacity, "CONTROL_ROOT", tmp_path / "capacity-controls")
    monkeypatch.setattr(cache_capacity, "_admit_backing_volume", lambda *_: {"state": "isolated-test-fixture"})
    with retained.collector_owner(source, DATASET) as descriptor:
        for index in range(count):
            # Same basenames must never overwrite each other in quarantine.
            name = f"raw/series-{index}/observations.json.gz"
            raw = gzip.compress(json.dumps({"value": index - 3.5}).encode(), mtime=0)
            path = source / name
            path.parent.mkdir(parents=True)
            path.write_bytes(raw)
        pl.DataFrame({"value": [-3.5, 0.0]}).write_parquet(source / "observations.parquet")
        release = packed.publish_packed_snapshot(cold, DATASET, source, node_id="test-node", pack_buckets=2)
        binding = retained.prepare_retained_source_binding(source, cold, pins, release,
            source_lock_fd=descriptor)
        atomic_write_json(source / retained.BINDING, binding)  # Isolated fixture activation only.
        increment = source / "raw/new-series/observations.json.gz"
        increment.parent.mkdir()
        increment.write_bytes(gzip.compress(b"new increment", mtime=0))
        policy = tmp_path / "retirement-policy.json"
        atomic_write_json(policy, {"schema_version": 1, "authority_node_id": "test-node",
            "required_peer_names": [], "retained_source_raw_datasets": [DATASET]})
        config = RetentionConfig(sync_root=cold, archive_root=tmp_path / "archive",
            materialized_root=pins, backup_config=tmp_path / "unused-backup.json",
            state_dir=tmp_path / "state", folder_id="fixture", required_peer_names=(),
            authority_node_id="test-node")
        monkeypatch.setattr(retirement, "verify_cold_resilience", lambda *_: {
            "cold_primary_verified": True, "backup_verified": False})
        monkeypatch.setattr(retirement, "_retained_raw_usage", lambda *_a, **_k: {
            "process_references": [], "service_references": {}, "configuration_references": {},
            "additional_pins": [], "active_leases": []})
        def runtime():
            return {"contract": "retained_raw_runtime_admission_v1", "state": "accepted",
                "source": str(source), "dataset": DATASET, "manifest_sha256": release.manifest_sha256,
                "implementation_sha256": retirement._retained_raw_code(),
                "canonical_retained_reader_writer_verified": True, "old_publisher_fenced": True,
                "worker": {"pid": 1234, "start_ticks": 100, "boot_id": "00000000-0000-0000-0000-000000000000"},
                "loaded_code_identity_sha256": "a" * 64, "handoff_receipt_sha256": "b" * 64}
        yield {"dataset": DATASET, "source": source, "config": config, "policy_path": policy,
            "state_root": tmp_path / "retirement-state", "source_lock_fd": descriptor,
            "repo_root": tmp_path, "runtime_probe": runtime,
            "peer_probe": lambda: {"ok": True, "checked_at": datetime.now(timezone.utc).isoformat(), "peers": []}}


def plan(options):
    return retirement.plan_retained_source_raw_retirement(**options)


def apply(options, inventory):
    return retirement.apply_retained_source_raw_retirement(**options,
        expected_fingerprint=inventory["plan_fingerprint"],
        _ingress_owner=options["source"].parent / "original-ingress.lock")


def assert_originals(options, inventory):
    for name, row in inventory["members"].items():
        assert hashlib.sha256((options["source"] / name).read_bytes()).hexdigest() == row["sha256"]


def journal(options):
    return json.loads((options["state_root"] / "source-raw-retirements" / f"{DATASET}.json").read_bytes())


def reconcile(options, inventory):
    return retirement.reconcile_retained_source_raw_retirement(dataset=DATASET,
        expected_fingerprint=inventory["plan_fingerprint"], source=options["source"],
        config=options["config"], state_root=options["state_root"], source_lock_fd=options["source_lock_fd"],
        _ingress_owner=options["source"].parent / "original-ingress.lock")


def interrupted_after_quarantine(options):
    original, calls = options["runtime_probe"], []
    def runtime():
        calls.append(True)
        value = original()
        if len(calls) >= 5:
            value["old_publisher_fenced"] = False
        return value
    changed = {**options, "runtime_probe": runtime}
    inventory = plan(changed)
    with pytest.raises(SnapshotError, match="runtime or binding changed after quarantine"):
        apply(changed, inventory)
    return inventory


def test_exact_nested_originals_retire_with_all_increments_and_services_preserved(retained_options):
    options, source = retained_options, retained_options["source"]
    before = {str(p.relative_to(source)): p.read_bytes() for p in source.rglob("*") if p.is_file()}
    inventory = plan(options)
    assert not inventory["blockers"] and len(inventory["members"]) == 2
    result = apply(options, inventory)
    assert result["deleted_names"] == 2 and result["action"] == "retired"
    assert result["reclaimed_allocated_file_bytes"] == inventory["reclaimable_allocated_bytes"]
    assert result["post_unlink_original_recovery"]["independently_reconstructed_files"] == 4
    assert result["post_unlink_original_recovery"]["manifest_sha256"] == inventory["manifest_sha256"]
    assert result["cold_objects_deleted"] == result["service_views_deleted"] == 0
    assert not result["NAS_backup_verified"]
    assert set(result["unlinked_member_evidence"]) == set(inventory["members"])
    for name, raw in before.items():
        if name in inventory["members"]:
            assert not (source / name).exists()
            proof = result["unlinked_member_evidence"][name]
            assert proof["links_before"] == 1 and proof["links_after"] == 0 and proof["descriptor_closed"]
        else:
            assert (source / name).read_bytes() == raw
    assert list((options["state_root"] / "source-raw-retirements").glob("*.quarantine-*")) == []
    state = retained.load_retained_source(source, DATASET, cold=options["config"].sync_root,
        pin_root=options["config"].materialized_root)
    for name in inventory["members"]:
        assert state.get(name) == before[name]
    assert apply(options, plan(options))["action"] == "already-retired"


def test_binding_without_live_reader_writer_admission_never_grants_deletion(retained_options):
    options = {**retained_options, "runtime_probe": None}
    inventory = plan(options)
    assert "retained-reader-writer-runtime-not-admitted" in inventory["blockers"]
    with pytest.raises(SnapshotError, match="retirement blocked"):
        apply(options, inventory)
    assert_originals(options, inventory)
    assert not (options["state_root"] / "source-raw-retirements").exists()


@pytest.mark.parametrize("field", ["manifest_sha256", "implementation_sha256", "old_publisher_fenced",
    "worker", "loaded_code_identity_sha256", "handoff_receipt_sha256"])
def test_incomplete_or_wrong_runtime_identity_blocks_before_quarantine(retained_options, field):
    original = retained_options["runtime_probe"]
    def wrong():
        value = original()
        value[field] = None
        return value
    options = {**retained_options, "runtime_probe": wrong}
    inventory = plan(options)
    assert inventory["blockers"] == ["retained-reader-writer-runtime-not-admitted"]
    with pytest.raises(SnapshotError, match="retirement blocked"):
        apply(options, inventory)
    assert_originals(options, inventory)


@pytest.mark.parametrize("gate", ["process_references", "service_references", "configuration_references",
    "additional_pins", "active_leases"])
def test_each_usage_gate_preserves_fixed_originals(retained_options, monkeypatch, gate):
    monkeypatch.setattr(retirement, "_retained_raw_usage", lambda *_a, **_k: {gate: ["in use"]})
    inventory = plan(retained_options)
    assert gate in inventory["blockers"]
    with pytest.raises(SnapshotError, match="retirement blocked"):
        apply(retained_options, inventory)
    assert_originals(retained_options, inventory)


def test_changed_current_raw_is_a_correction_to_preserve(retained_options):
    binding = json.loads((retained_options["source"] / retained.BINDING).read_bytes())
    name = next(iter(binding["raw_files"]))
    path = retained_options["source"] / name
    path.write_bytes(gzip.compress(b"corrected upstream original", mtime=0))
    with pytest.raises(SnapshotError, match="preserve the increment"):
        plan(retained_options)
    assert gzip.decompress(path.read_bytes()) == b"corrected upstream original"
    assert not retained_options["state_root"].exists()


@pytest.mark.parametrize("problem", ["missing", "symlink", "hardlink", "fd-budget"])
def test_unknown_absence_aliases_and_fd_exhaustion_never_retire(retained_options, monkeypatch, problem):
    options = retained_options
    inventory = plan(options)
    name = next(iter(inventory["members"]))
    path = options["source"] / name
    if problem == "missing":
        path.unlink()  # Only the isolated disposable fixture.
    elif problem == "symlink":
        copy = options["source"].parent / "outside-original"
        path.rename(copy)
        path.symlink_to(copy)
        with pytest.raises(SnapshotError, match="redirected"):
            plan(options)
        assert copy.is_file()
        return
    elif problem == "hardlink":
        (path.parent / "external-alias").hardlink_to(path)
    else:
        monkeypatch.setattr(retirement.resource, "getrlimit", lambda *_: (32, 32))
    fresh = plan(options)
    assert fresh["blockers"]
    with pytest.raises(SnapshotError, match="retirement blocked"):
        apply(options, fresh)
    assert not (options["state_root"] / "source-raw-retirements").exists()


def test_runtime_change_after_quarantine_keeps_every_original_with_unique_names(retained_options):
    original, calls = retained_options["runtime_probe"], []
    def runtime():
        calls.append(True)
        value = original()
        if len(calls) >= 5:
            value["old_publisher_fenced"] = False
        return value
    options = {**retained_options, "runtime_probe": runtime}
    inventory = plan(options)
    with pytest.raises(SnapshotError, match="runtime or binding changed after quarantine"):
        apply(options, inventory)
    state = journal(options)
    assert state["state"] == "retiring" and not state["unlinked_members"]
    assert len(set(state["quarantine_member_names"].values())) == len(inventory["members"])
    for name, row in inventory["members"].items():
        path = Path(state["quarantine"]) / state["quarantine_member_names"][name]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == row["sha256"]


def test_post_hash_mutation_even_with_restored_mtime_is_detected_before_unlink(retained_options):
    options, calls = retained_options, []
    original = options["peer_probe"]
    def peer():
        calls.append(True)
        if len(calls) == 3:
            state = journal(options)
            path = Path(state["quarantine"]) / next(iter(state["quarantine_member_names"].values()))
            before = path.stat()
            raw = path.read_bytes()
            path.write_bytes(bytes([raw[0] ^ 1]) + raw[1:])
            os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        return original()
    options["peer_probe"] = peer
    inventory = plan(options)
    with pytest.raises(SnapshotError, match="changed before exact unlink"):
        apply(options, inventory)
    assert not journal(options)["unlinked_members"]


@pytest.mark.parametrize("retained_options", [300], indirect=True)
def test_large_bounded_journal_retains_all_evidence_and_remains_idempotent(retained_options):
    inventory = plan(retained_options)
    result = apply(retained_options, inventory)
    assert result["deleted_names"] == 300
    path = retained_options["state_root"] / "source-raw-retirements" / f"{DATASET}.json"
    assert path.stat().st_size > 64 * 1024
    assert apply(retained_options, plan(retained_options))["action"] == "already-retired"


@pytest.mark.parametrize("damage", ["members", "binding", "recovery", "link", "bytes", "time"])
def test_incomplete_retirement_receipt_never_becomes_acceptance(retained_options, damage):
    apply(retained_options, plan(retained_options))
    path = retained_options["source"] / retirement._RETAINED_RAW_RECEIPT
    receipt = json.loads(path.read_bytes())
    if damage == "members":
        receipt["retired_members"].pop(next(iter(receipt["retired_members"])))
    elif damage == "binding":
        receipt["binding_sha256"] = "0" * 64
    elif damage == "recovery":
        receipt["post_unlink_original_recovery"]["independently_reconstructed_files"] -= 1
    elif damage == "link":
        next(iter(receipt["unlinked_member_evidence"].values()))["links_after"] = 1
    elif damage == "bytes":
        receipt["reclaimed_allocated_file_bytes"] += 512
    else:
        receipt["retired_at_utc"] = "2026-10-27T00:00:00"
    atomic_write_json(path, receipt)
    with pytest.raises(SnapshotError, match="receipt lacks|timestamp"):
        plan(retained_options)


@pytest.mark.parametrize("extra", ["none", "pin", "lease", "bad-lease"])
def test_real_managed_pin_and_lease_owner_keeps_other_consumers(retained_options, monkeypatch, extra):
    from stockagent.data_sync import artifact_maintenance, remote_legacy_return
    monkeypatch.setattr(retirement, "_retained_raw_usage", REAL_USAGE)
    monkeypatch.setattr(artifact_maintenance, "_ancestor_process_references", lambda _a, _b, refs: refs)
    monkeypatch.setattr(materialized_cache, "process_references_many", lambda *_a, **_k: [])
    monkeypatch.setattr(retirement, "artifact_service_references", lambda *_: {})
    monkeypatch.setattr(remote_legacy_return, "active_configuration_references_many", lambda *_: {})
    options, cfg = retained_options, retained_options["config"]
    state = retained.load_retained_source(options["source"], DATASET, cold=cfg.sync_root,
        pin_root=cfg.materialized_root, read_originals=False)
    release = state.request.release
    if extra == "pin":
        packed.write_packed_pin(cfg.materialized_root / "pins/research.pin.json", release)
    elif extra in {"lease", "bad-lease"}:
        path = materialized_cache._lease_path(cfg.materialized_root, DATASET, release.manifest["snapshot_id"])
        path.parent.mkdir(parents=True)
        atomic_write_json(path, {"schema_version": 1, "dataset": DATASET,
            "snapshot_id": release.manifest["snapshot_id"], "state": "hot",
            "target": str(materialized_cache._target_path(cfg.materialized_root, DATASET, path.stem)),
            "expires_ns": time.time_ns() + 60 * 10**9} if extra == "lease" else {"expires_ns": 0})
    if extra == "bad-lease":
        with pytest.raises(SnapshotError, match="unknown managed-use lease"):
            plan(options)
    else:
        inventory = plan(options)
        assert bool(inventory["usage"]["additional_pins"]) == (extra == "pin")
        assert bool(inventory["usage"]["active_leases"]) == (extra == "lease")


@pytest.mark.parametrize("fault", [None, "missing-guard", "backup-claim", "wrong-primary"])
def test_private_history_reuses_exact_original_D_guard_without_claiming_backup(monkeypatch, fault):
    root = Path("/srv/stockagent-packed/.local-state/private-history")
    calls = []
    def guard(path):
        calls.append(("private", path))
        if fault == "missing-guard":
            raise SnapshotError("private owner guard failed")
    def proof(path, release, backup):
        calls.append(("primary", path, release, backup))
        return {"cold_primary_verified": fault != "wrong-primary", "backup_verified": fault == "backup-claim"}
    monkeypatch.setattr(packed, "_check_packed_read_authority", guard)
    monkeypatch.setattr(retirement, "verify_cold_resilience", proof)
    options = SimpleNamespace(sync_root=root, backup_config=Path("unused-backup.json"))
    contract = retirement._retained_raw_contract(DATASET)
    if fault:
        with pytest.raises(SnapshotError, match="guard failed|single-D authority"):
            retirement._source_raw_cold_proof(contract, options, "fixed-private-release")
    else:
        result = retirement._source_raw_cold_proof(contract, options, "fixed-private-release")
        assert result == {"cold_primary_verified": True, "backup_verified": False,
            "private_history_guard_verified": True}
        assert calls == [("private", root), ("primary", Path("/srv/stockagent-packed"),
            "fixed-private-release", Path("unused-backup.json"))]


@pytest.mark.parametrize("root,expected", [
    (Path("/srv/stockagent-packed"), {"d_primary_native_blob_reads": True, "d_primary_native_pack_reads": True}),
    (Path("/srv/stockagent-packed/.local-state/private-history"),
        {"d_primary_native_blob_reads": False, "d_primary_native_pack_reads": True}),
    (Path("/tmp/unregistered-private-history"),
        {"d_primary_native_blob_reads": False, "d_primary_native_pack_reads": False}),
])
def test_native_read_scope_preserves_original_private_pack_only_admission(root, expected):
    assert retirement._source_raw_native_reads(root) == expected


def test_uncommitted_quarantine_restores_exact_names_then_allows_fresh_retirement(retained_options):
    options = retained_options
    inventory = interrupted_after_quarantine(options)
    before = journal(options)
    original = (options["state_root"] / "source-raw-retirements" / f"{DATASET}.json").read_bytes()
    result = reconcile(options, inventory)
    assert result["action"] == "restored-for-fresh-audit" and not result["raw_retirement_complete"]
    assert result["deleted_names"] == result["cold_objects_deleted"] == result["restored_from_D_original_bytes"] == 0
    assert Path(result["interrupted_journal_archive"]).read_bytes() == original
    assert not Path(before["quarantine"]).exists()
    assert_originals(options, inventory)
    assert journal(options)["state"] == "rolled-back"
    assert reconcile(options, inventory)["action"] == "already-rolled-back"
    fresh = plan(options)
    assert not fresh["blockers"]
    assert apply(options, fresh)["deleted_names"] == len(inventory["members"])


def test_prior_journal_without_batch_field_recovers_under_current_executor(retained_options):
    options = retained_options
    interrupted_after_quarantine(options)
    saved = journal(options)
    saved.pop("journal_checkpoint_members")
    keys = ("contract", "source", "snapshot_id", "manifest_sha256", "members", "source_identities",
        "policy_sha256", "materialized_root", "state_root", "implementation_sha256",
        "runtime_admission", "binding_sha256")
    fingerprint = hashlib.sha256(json.dumps({key: saved[key] for key in keys}, sort_keys=True).encode()).hexdigest()
    saved["plan_fingerprint"] = fingerprint
    path = options["state_root"] / "source-raw-retirements" / f"{DATASET}.json"
    atomic_write_json(path, saved)
    original = path.read_bytes()
    result = reconcile(options, {"plan_fingerprint": fingerprint})
    assert result["action"] == "restored-for-fresh-audit"
    assert Path(result["interrupted_journal_archive"]).read_bytes() == original
    assert_originals(options, saved)
    assert not plan(options)["blockers"]


def test_partial_unlink_recovers_missing_original_from_fixed_D_with_original_metadata(retained_options, monkeypatch):
    options, original_write = retained_options, retirement.atomic_write_json
    inventory = plan(options)
    def fail_after_first_unlink(path, value):
        original_write(path, value)
        if value.get("state") == "retiring" and len(value.get("unlinked_members", [])) == 1:
            raise OSError("simulated interruption after first exact unlink")
    with monkeypatch.context() as context:
        context.setattr(retirement, "atomic_write_json", fail_after_first_unlink)
        with pytest.raises(OSError, match="simulated interruption"):
            apply(options, inventory)
    old = journal(options)
    assert len(old["unlinked_members"]) == 1
    result = reconcile(options, inventory)
    assert result["restored_from_D_original_bytes"] == sum(
        inventory["members"][name]["bytes"] for name in old["unlinked_members"])
    assert_originals(options, inventory)
    for name in old["unlinked_members"]:
        info = (options["source"] / name).stat()
        expected = inventory["source_identities"][name]["signature"]
        assert info.st_mode == expected[2] and info.st_mtime_ns == expected[5]
    assert journal(options)["state"] == "rolled-back" and not plan(options)["blockers"]


def test_recovery_itself_resumes_after_atomic_rename_without_losing_failed_receipt(retained_options, monkeypatch):
    from scripts import deliver_vast_futures_preparation as native
    options = retained_options
    inventory = interrupted_after_quarantine(options)
    failed_raw = (options["state_root"] / "source-raw-retirements" / f"{DATASET}.json").read_bytes()
    original_rename, calls = native.rename_no_replace, []
    def fail_after_rename(source, destination):
        original_rename(source, destination)
        calls.append(True)
        if len(calls) == 1:
            raise OSError("simulated recovery interruption")
    with monkeypatch.context() as context:
        context.setattr(native, "rename_no_replace", fail_after_rename)
        with pytest.raises(OSError, match="recovery interruption"):
            reconcile(options, inventory)
    assert journal(options)["state"] == "restoring-raw-names"
    result = reconcile(options, inventory)
    assert Path(result["interrupted_journal_archive"]).read_bytes() == failed_raw
    assert_originals(options, inventory)
    assert not plan(options)["blockers"]


@pytest.mark.parametrize("retained_options", [64], indirect=True)
@pytest.mark.parametrize("after", [7, 39])
@pytest.mark.parametrize("phase", ["quarantine", "unlink", "restore"])
def test_crash_between_durable_batches_recovers_every_exact_original(
        retained_options, monkeypatch, phase, after):
    from scripts import deliver_vast_futures_preparation as native
    options = retained_options
    inventory = (interrupted_after_quarantine(options) if phase == "restore" else plan(options))
    source, state_root = options["source"], options["state_root"]
    raw_path = state_root / "source-raw-retirements" / f"{DATASET}.json"
    before = {str(p.relative_to(source)): p.read_bytes() for p in source.rglob("*")
        if p.is_file() and str(p.relative_to(source)) not in inventory["members"]}
    calls = []

    def is_quarantine(path):
        path = Path(path)
        return path.parent.parent == state_root / "source-raw-retirements" and path.parent.name.startswith(DATASET + ".quarantine-")

    def crash():
        calls.append(True)
        if len(calls) == after:
            raise OSError("interrupted between durable retirement batches")

    original_rename, original_unlink = os.rename, Path.unlink
    original_restore = native.rename_no_replace
    def rename(a, b, *args, **kwargs):
        result = original_rename(a, b, *args, **kwargs)
        if is_quarantine(b):
            crash()
        return result

    def unlink(path, *args, **kwargs):
        result = original_unlink(path, *args, **kwargs)
        if is_quarantine(path):
            crash()
        return result

    def restore(a, b):
        result = original_restore(a, b)
        if Path(b).is_relative_to(source / "raw"):
            crash()
        return result

    with monkeypatch.context() as context:
        if phase == "restore":
            context.setattr(native, "rename_no_replace", restore)
        elif phase == "quarantine":
            context.setattr(os, "rename", rename)
        else:
            context.setattr(Path, "unlink", unlink)
        with pytest.raises(OSError, match="between durable retirement batches"):
            reconcile(options, inventory) if phase == "restore" else apply(options, inventory)
    interrupted = raw_path.read_bytes()
    saved = json.loads(interrupted)
    key = {"quarantine": "quarantined_members", "unlink": "unlinked_members", "restore": "restored_names"}[phase]
    assert len(saved.get(key, [])) < after
    result = reconcile(options, inventory)
    assert result["action"] == "restored-for-fresh-audit" and not result["raw_retirement_complete"]
    assert result["deleted_names"] == result["cold_objects_deleted"] == 0
    if phase != "restore":
        assert Path(result["interrupted_journal_archive"]).read_bytes() == interrupted
    if phase == "unlink":
        assert result["restored_from_D_original_bytes"] == sum(
            inventory["members"][name]["bytes"] for name in list(inventory["members"])[:after])
    assert_originals(options, inventory)
    assert all((source / name).read_bytes() == raw for name, raw in before.items())
    assert journal(options)["state"] == "rolled-back" and not plan(options)["blockers"]
    assert apply(options, plan(options))["deleted_names"] == len(inventory["members"])


def test_current_correction_blocks_recovery_before_any_source_or_journal_change(retained_options):
    options = retained_options
    inventory = interrupted_after_quarantine(options)
    path = options["state_root"] / "source-raw-retirements" / f"{DATASET}.json"
    original = path.read_bytes()
    quarantine = Path(journal(options)["quarantine"])
    saved = {p.name: p.read_bytes() for p in quarantine.iterdir()}
    name = next(iter(inventory["members"]))
    (options["source"] / name).write_bytes(b"newly corrected source data")
    with pytest.raises(SnapshotError, match="changed original; preserve it"):
        reconcile(options, inventory)
    assert path.read_bytes() == original
    assert (options["source"] / name).read_bytes() == b"newly corrected source data"
    assert {p.name: p.read_bytes() for p in quarantine.iterdir()} == saved


def test_corrupt_fixed_D_original_blocks_recovery_without_touching_quarantine(retained_options):
    options = retained_options
    inventory = interrupted_after_quarantine(options)
    path = options["state_root"] / "source-raw-retirements" / f"{DATASET}.json"
    original = path.read_bytes()
    binding = retained.load_retained_source(options["source"], DATASET,
        cold=options["config"].sync_root, pin_root=options["config"].materialized_root,
        read_originals=False)
    obj = binding.request.release.manifest["archive"]["objects"][0]
    payload = options["config"].sync_root / obj["relpath"]
    raw = payload.read_bytes()
    payload.write_bytes(bytes([raw[0] ^ 1]) + raw[1:])
    with pytest.raises(SnapshotError):
        reconcile(options, inventory)
    assert path.read_bytes() == original
    assert all(not (options["source"] / name).exists() for name in inventory["members"])
    assert len(list(Path(journal(options)["quarantine"]).iterdir())) == len(inventory["members"])


def test_capacity_denial_preserves_failed_journal_and_all_quarantined_names(retained_options, monkeypatch):
    from stockagent.data_sync.completed_forward_capacity import CapacityDeferred
    options = retained_options
    inventory = interrupted_after_quarantine(options)
    path = options["state_root"] / "source-raw-retirements" / f"{DATASET}.json"
    original = path.read_bytes()
    def deny(*_):
        raise CapacityDeferred("test_backing_capacity_denied")
    monkeypatch.setattr(cache_capacity, "_admit_backing_volume", deny)
    with pytest.raises(CapacityDeferred, match="test_backing_capacity_denied"):
        reconcile(options, inventory)
    assert path.read_bytes() == original
    assert all(not (options["source"] / name).exists() for name in inventory["members"])
    assert len(list(Path(journal(options)["quarantine"]).iterdir())) == len(inventory["members"])


def test_committed_source_receipt_finalizes_interrupted_journal_without_restoring_raw(retained_options, monkeypatch):
    options, original_write = retained_options, retirement.atomic_write_json
    inventory = plan(options)
    def fail_after_commit(path, value):
        if value.get("action") == "retired":
            raise OSError("simulated interruption before final journal commit")
        original_write(path, value)
    with monkeypatch.context() as context:
        context.setattr(retirement, "atomic_write_json", fail_after_commit)
        with pytest.raises(OSError, match="final journal commit"):
            apply(options, inventory)
    assert journal(options)["state"] == "retiring"
    committed = (options["source"] / retirement._RETAINED_RAW_RECEIPT).read_bytes()
    result = reconcile(options, inventory)
    assert result["action"] == "retired" and result["reconciled_after_receipt"]
    assert (options["source"] / retirement._RETAINED_RAW_RECEIPT).read_bytes() == committed
    assert all(not (options["source"] / name).exists() for name in inventory["members"])
    assert journal(options)["state"] == "cold-raw-retired"
    assert apply(options, plan(options))["action"] == "already-retired"


def test_changed_interrupted_evidence_blocks_resumed_recovery_before_another_rename(retained_options, monkeypatch):
    from scripts import deliver_vast_futures_preparation as native
    options = retained_options
    inventory = interrupted_after_quarantine(options)
    original_rename, calls = native.rename_no_replace, []
    def fail_after_rename(source, destination):
        original_rename(source, destination)
        calls.append(True)
        raise OSError("simulated recovery interruption")
    with monkeypatch.context() as context:
        context.setattr(native, "rename_no_replace", fail_after_rename)
        with pytest.raises(OSError, match="recovery interruption"):
            reconcile(options, inventory)
    old = journal(options)
    archive = Path(old["rollback_operation"]) / "interrupted-journal.private.json"
    archive.write_bytes(b"modified failed evidence")
    current_names = {name for name in inventory["members"] if (options["source"] / name).exists()}
    with pytest.raises(SnapshotError, match="interrupted journal archive changed"):
        reconcile(options, inventory)
    assert {name for name in inventory["members"] if (options["source"] / name).exists()} == current_names
    assert journal(options) == old and len(calls) == 1
