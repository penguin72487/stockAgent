"""Content deduplication, backpressure and NAS proof boundaries for the queue."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import threading
import time

import pytest

from scripts.backup_delivery_receipt import (
    ACK, READY, atomic_public, publish, publish_readiness, signed, validate_ack, validate_readiness,
)
from scripts.verify_backup_delivery import verify
from stockagent.data_sync import backup_stream as module
from stockagent.data_sync.backup_stream import BackupStream, capture_catalog, coverage, empty_ledger, file_key, select_wave
from stockagent.data_sync.desync_snapshots import SnapshotError, scan_tree
from stockagent.data_sync.offhost_backup import export_incremental_delivery
from stockagent.data_sync.backup_recovery import assemble_release
from stockagent.data_sync.packed_snapshots import (
    fetch_packed_snapshot, initialize_packed_layout, publish_packed_snapshot, resolve_packed_snapshot_id,
)


@pytest.fixture
def stream(tmp_path):
    cold, source = tmp_path / "cold", tmp_path / "source"
    initialize_packed_layout(cold, node_id="penguin")
    source.mkdir()
    (source / "價格.txt").write_text("first vintage\n", encoding="utf-8")
    (source / "shared.txt").write_text("preserve provenance\n")
    first = publish_packed_snapshot(cold, "prices", source, loose_file_threshold_bytes=1024, pack_buckets=2)
    (source / "價格.txt").write_text("second vintage\n", encoding="utf-8")
    second = publish_packed_snapshot(cold, "prices", source, loose_file_threshold_bytes=1024, pack_buckets=2)
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"datasets": [{"dataset": "prices", "publish": True}]}))
    transport = tmp_path / "transport"
    transport.mkdir()
    (transport / ".stfolder").mkdir()
    (transport / ".stignore").write_text("(?d).staging\n(?d).staging/**\n")
    config = {"schema_version": 1, "cold_root": str(cold), "publication_catalog": str(catalog),
        "transport_root": str(transport), "receipt_root": str(tmp_path / "receipts"),
        "state_root": str(tmp_path / "state"), "required_mounts": {}, "require_transport_mount": False,
        "producer_device_id": "penguin-test", "receiver_device_id": "lab-test", "repository_id": "a" * 64,
        "maximum_batch_bytes": 2 * 1024**2, "maximum_batch_files": 3, "maximum_pending_bytes": 4 * 1024**2,
        "maximum_pending_deliveries": 1, "maximum_retained_transport_bytes": 64 * 1024**2,
        "reserve_bytes": 1024, "readiness_max_age_seconds": 900, "automatic_pruning": False,
        "automatic_batch_deletion": False, "source_cleanup_authorized": False}
    return BackupStream(config), cold, source, first, second


def readiness(queue, **changes):
    value = {"contract": READY, **{k: queue.config[k] for k in ("producer_device_id", "receiver_device_id", "repository_id")},
        "ingress_free_bytes": 64 * 1024**2, "nas_free_bytes": 64 * 1024**2, "reserve_bytes": 1024,
        "maximum_batch_bytes": 2 * 1024**2, "repository_check_mode": "full_read_data",
        "single_owner_verified": True, "nas_mount_guard_verified": True, "runtime_lock_verified": True,
        "automatic_pruning": False, "automatic_batch_deletion": False,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(), **changes}
    value = signed(value)
    atomic_public(queue.receipts / "readiness.json", value, replace=True)
    return value


def test_independent_publication_keeps_sending_during_stale_or_unavailable_nas(stream):
    queue = stream[0]
    queue.config.update(publication_requires_receiver_ready=False, maximum_batch_files=1)
    readiness(queue, observed_at_utc='2020-01-01T00:00:00+00:00', nas_mount_guard_verified=False,
              ingress_free_bytes=0, nas_free_bytes=0)
    first = queue.cycle()
    second = queue.cycle()
    assert first['pending_delivery_count'] == 1
    assert second['pending_delivery_count'] == 2
    assert not second['readiness_error']
    assert all(not row.get('acceptance') for row in queue.load_ledger()['deliveries'].values())


def test_independent_publication_still_refuses_another_receiver_and_full_source_storage(stream):
    queue = stream[0]
    queue.config['publication_requires_receiver_ready'] = False
    readiness(queue, receiver_device_id='unknown-peer')
    assert queue.cycle()['state'] == 'waiting_receiver_readiness'
    readiness(queue)
    queue.config['maximum_retained_transport_bytes'] = 1
    assert queue.cycle()['state'] == 'backpressure'
    assert not queue.load_ledger()['deliveries']


def test_delegated_cold_replication_ingests_old_ack_without_new_payload(stream):
    queue = stream[0]
    readiness(queue)
    queue.cycle()
    ledger = queue.load_ledger()
    delivery = next(iter(ledger['deliveries'].values()))
    acknowledge(queue, delivery)
    queue.config['cold_object_replication_enabled'] = False
    result = queue.cycle()
    after = queue.load_ledger()
    assert len(after['deliveries']) == len(ledger['deliveries'])
    assert next(iter(after['deliveries'].values()))['acceptance']
    assert result['state'] == 'cold_replication_delegated_to_immutable_lake'
    assert result['cold_object_replication_enabled'] is False
    assert queue.cold.exists()


def test_cold_delegation_requires_explicit_boolean(stream):
    with pytest.raises(SnapshotError, match='explicit boolean'):
        BackupStream({**stream[0].config, 'cold_object_replication_enabled': 'false'})
    with pytest.raises(SnapshotError, match='explicit boolean'):
        BackupStream({**stream[0].config, 'cold_metadata_replication_enabled': 'true'})


def test_delegated_objects_keep_exact_metadata_backup_on_the_existing_owner(stream):
    queue = stream[0]
    queue.config.update(cold_object_replication_enabled=False, cold_metadata_replication_enabled=True)
    readiness(queue)
    result = queue.cycle()
    ledger = queue.load_ledger()
    assert result['pipeline']['published_waves']
    assert all(row['role'] == 'cold_metadata' for d in ledger['deliveries'].values() for row in d['files'])
    assert queue.cold.exists()


def test_frozen_metadata_backfill_retains_a_head_that_advanced_after_capture(stream, tmp_path):
    queue, cold, source, first, second = stream
    fixed = capture_catalog(cold, Path(queue.config['publication_catalog']))
    prior = next(row for row in fixed['files'] if row['relative'].startswith('heads/'))
    raw = json.dumps(fixed).encode()
    path = tmp_path / 'fixed-catalog.json'
    path.write_bytes(raw)
    (source / '價格.txt').write_text('new head after the frozen cohort\n')
    publish_packed_snapshot(cold, 'prices', source, loose_file_threshold_bytes=1024, pack_buckets=2)
    queue.config.update(cold_object_replication_enabled=False, cold_metadata_replication_enabled=True,
                        metadata_backfill_catalog=str(path), metadata_backfill_sha256=hashlib.sha256(raw).hexdigest())
    readiness(queue)
    for _ in range(10):
        queue.cycle()
        ledger = queue.load_ledger()
        for delivery in ledger['deliveries'].values():
            if not delivery.get('acceptance'):
                acknowledge(queue, delivery)
        if any(file_key(prior) == file_key(row) for delivery in ledger['deliveries'].values() for row in delivery['files']):
            break
    assert any(file_key(prior) == file_key(row) for delivery in queue.load_ledger()['deliveries'].values()
               for row in delivery['files'])
    assert (cold / prior['relative']).read_bytes() != prior['captured_bytes_utf8'].encode()


def test_reported_pilot_metadata_is_sent_for_machine_restore_proof(stream):
    queue = stream[0]
    catalog = capture_catalog(queue.cold, Path(queue.config['publication_catalog']))
    pilot = next(row for row in catalog['files'] if row['relative'].startswith('manifests/'))
    ledger = empty_ledger()
    ledger['reported_baseline'] = [{'files': [pilot],
        **{key: 'a' * 64 for key in ('envelope_identity_sha256', 'envelope_file_sha256', 'snapshot_id', 'report_sha256')},
        'batch_relative': 'batch-pilot', 'repository_id': queue.config['repository_id'],
        'evidence_origin': 'user_relayed_acceptance'}]
    module.private_json(queue.ledger_path, ledger)
    queue.config.update(cold_object_replication_enabled=False, cold_metadata_replication_enabled=True,
                        maximum_batch_files=100)
    readiness(queue)
    queue.cycle()
    assert any(file_key(pilot) == file_key(row) for d in queue.load_ledger()['deliveries'].values() for row in d['files'])


def acknowledge(queue, delivery, **changes):
    dispatch = delivery["dispatch"]
    proof = {"repository_id": dispatch["repository_id"], "snapshot_id": "b" * 64,
        "envelope_identity_sha256": dispatch["envelope_identity_sha256"],
        "envelope_file_sha256": dispatch["envelope_file_sha256"],
        "complete_files": dispatch["complete_files"], "complete_bytes": dispatch["complete_bytes"],
        "repository_check_mode": "full_read_data", "repository_check_verified": True,
        "independent_restore_verified": True, "source_verifier_verified": True,
        "all_files_sha256_verified": True, "command_exit_codes": [0, 0, 0, 0],
        "complete_workflow_seconds": 1.2, "accepted_at_utc": datetime.now(timezone.utc).isoformat(), **changes}
    path = queue.state / "private-proof.json"
    path.write_text(json.dumps(proof))
    return publish(queue.transport / dispatch["batch_relative"], dispatch, path, queue.receipts)


def enable_pipeline(queue, *, waves=4):
    queue.config.update(maximum_pending_deliveries=4, maximum_pending_bytes=16 * 1024**2)
    queue.config["pipeline"] = {"maximum_waves_per_cycle": waves, "copy_workers": 2,
        "verify_workers": 2, "retry_delays_seconds": [30, 120, 600]}
    return BackupStream(queue.config)


def test_fd_capacity_inventory_counts_failed_staging_and_unjournaled_files(stream):
    queue=stream[0]
    (queue.transport/".staging/failed").mkdir(parents=True)
    (queue.transport/".staging/failed/partial.bin").write_bytes(b"x"*91)
    (queue.transport/"unknown.bin").write_bytes(b"y"*123)
    measured=queue.transport_usage()
    assert measured["retained_staging_bytes"] == 91
    assert measured["retained_transport_bytes"] == 91+123+(queue.transport/".stignore").stat().st_size


@pytest.mark.parametrize("kind",("file_link","directory_link","fifo"))
def test_fd_capacity_inventory_rejects_redirected_and_special_entries(stream,kind):
    queue=stream[0]
    outside=queue.state/"outside"
    outside.mkdir()
    (outside/"data").write_text("must not follow this")
    path=queue.transport/"unsafe"
    if kind == "fifo":
        os.mkfifo(path)
    else:
        path.symlink_to(outside if kind == "directory_link" else outside/"data")
    with pytest.raises(SnapshotError):
        queue.transport_usage()


def test_pipeline_publishes_disjoint_waves_without_waiting_for_nas_ack(stream):
    queue = enable_pipeline(stream[0])
    readiness(queue)
    result = queue.cycle()
    ledger = queue.load_ledger()
    assert 2 <= len(ledger["deliveries"]) <= 4
    assert result["pending_delivery_count"] == len(ledger["deliveries"])
    assert len(result["pipeline"]["published_waves"]) == len(ledger["deliveries"])
    observed = []
    for key, delivery in ledger["deliveries"].items():
        assert not delivery.get("acceptance")
        verify(queue.transport / delivery["dispatch"]["batch_relative"], key, workers=2)
        observed.extend(file_key(row) for row in delivery["files"])
    assert len(observed) == len(set(observed))


def test_pipeline_indexes_first_batch_while_exporting_second(stream, monkeypatch):
    queue = enable_pipeline(stream[0])
    readiness(queue)
    scan_started = threading.Event()
    real_export = module.export_incremental_delivery
    calls = []

    def export(*args, **kwargs):
        if calls:
            assert scan_started.wait(timeout=2), "previous READY must be indexed while source keeps producing"
        calls.append(1)
        return real_export(*args, **kwargs)

    def scan(ledger):
        assert len(ledger["deliveries"]) == 1
        scan_started.set()

    monkeypatch.setattr(module, "export_incremental_delivery", export)
    monkeypatch.setattr(queue, "scan_transport", scan)
    result = queue.cycle()
    assert len(calls) >= 2 and not result["pipeline"]["scan_errors"]


def test_failed_wave_is_requeued_while_other_waves_continue(stream, monkeypatch):
    queue = enable_pipeline(stream[0])
    readiness(queue)
    real_export = module.export_incremental_delivery
    failed = []

    def export(cold, rows, target, **kwargs):
        if not failed:
            failed.extend(file_key(row) for row in rows)
            target.mkdir(parents=True)
            (target / "failure-evidence.txt").write_text("simulated interrupted copy")
            raise OSError("transient storage failure")
        return real_export(cold, rows, target, **kwargs)

    monkeypatch.setattr(module, "export_incremental_delivery", export)
    result = queue.cycle()
    ledger = queue.load_ledger()
    assert result["pipeline"]["published_waves"] and len(result["pipeline"]["export_errors"]) == 1
    assert set(failed) <= ledger["export_retries"].keys()
    assert all(ledger["export_retries"][key]["next_attempt_epoch"] > time.time() for key in failed)
    assert not any(file_key(row) in failed for d in ledger["deliveries"].values() for row in d["files"])
    assert (queue.transport / result["pipeline"]["export_errors"][0]["staging_relative"] / "failure-evidence.txt").is_file()
    for delivery in list(ledger["deliveries"].values()):
        acknowledge(queue, delivery)
    for item in ledger["export_retries"].values():
        item["next_attempt_epoch"] = 0
    module.private_json(queue.ledger_path, ledger)
    queue.cycle()
    assert set(failed) <= {file_key(row) for d in queue.load_ledger()["deliveries"].values() for row in d["files"]}


def test_inventory_deduplicates_objects_across_retained_versions(stream):
    queue, cold, _, _, _ = stream
    catalog = capture_catalog(cold, Path(queue.config["publication_catalog"]))
    assert len(catalog["releases"]) == 2
    assert sum(r["current_head"] for r in catalog["releases"]) == 1
    assert len({r["relative"] for r in catalog["files"]}) == len(catalog["files"])
    assert catalog["missing_objects"] == []


def test_optional_unreferenced_cold_evidence_is_backed_without_inventing_release(stream):
    queue, cold, *_ = stream
    raw = b'unreferenced unique bytes'
    sha = hashlib.sha256(raw).hexdigest()
    relative = 'objects/blobs/' + sha[:2] + '/' + sha + '.blob'
    path = cold / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    baseline = capture_catalog(cold, Path(queue.config['publication_catalog']))
    observed = capture_catalog(cold, Path(queue.config['publication_catalog']), include_unreferenced_objects=True)
    assert not any(r['relative'] == relative for r in baseline['files'])
    orphan = next(r for r in observed['files'] if r['relative'] == relative)
    assert orphan['role'] == 'unreferenced_cold_object' and orphan['release_provenance_verified'] is False
    assert orphan['sha256'] == sha and observed['releases'] == baseline['releases']
    assert coverage(observed, empty_ledger())['unreferenced_cold_object_bytes'] == len(raw)


def test_missing_old_object_remains_a_gap_without_erasing_history(stream):
    queue, cold, _, first, second = stream
    old = {r["relpath"] for r in first.manifest["archive"]["objects"]}
    current = {r["relpath"] for r in second.manifest["archive"]["objects"]}
    missing = next(iter(old - current))
    (cold / missing).unlink()
    catalog = capture_catalog(cold, Path(queue.config["publication_catalog"]))
    assert len(catalog["releases"]) == 2
    assert any(r["missing_objects"] == [missing] for r in catalog["releases"])
    assert all(not r["missing_objects"] for r in catalog["releases"] if r["current_head"])
    assert first.manifest_path.exists()


def test_restricted_catalog_is_not_bypassed_by_the_backup_queue(stream):
    queue, cold, source, _, _ = stream
    publish_packed_snapshot(cold, "restricted", source, loose_file_threshold_bytes=1024, pack_buckets=2)
    Path(queue.config["publication_catalog"]).write_text(json.dumps({"datasets": [
        {"dataset": "prices", "publish": True}, {"dataset": "restricted", "publish": False}]}))
    catalog = capture_catalog(cold, Path(queue.config["publication_catalog"]))
    assert {r["dataset"] for r in catalog["releases"]} == {"prices"}
    assert catalog["excluded_manifests"]


def test_restricted_release_objects_are_not_relabelled_as_unreferenced(stream):
    queue, cold, source, *_ = stream
    (source / 'private.txt').write_text('restricted bytes')
    restricted = publish_packed_snapshot(cold, 'restricted', source, loose_file_threshold_bytes=1024, pack_buckets=2)
    Path(queue.config['publication_catalog']).write_text(json.dumps({'datasets': [
        {'dataset': 'prices', 'publish': True}, {'dataset': 'restricted', 'publish': False}]}))
    catalog = capture_catalog(cold, Path(queue.config['publication_catalog']), include_unreferenced_objects=True)
    rows = {r['relative']: r for r in catalog['files']}
    private_only = {r['relpath'] for r in restricted.manifest['archive']['objects']} - {
        r['relative'] for r in rows.values() if r['role'] == 'packed_object'}
    assert private_only and not private_only.intersection(rows)


def test_unreadable_restricted_manifest_blocks_unreferenced_export(stream):
    queue, cold, *_ = stream
    folder = cold / 'manifests/restricted'
    folder.mkdir()
    (folder / 'bad.json').write_text('corrupt restricted manifest')
    Path(queue.config['publication_catalog']).write_text(json.dumps({'datasets': [
        {'dataset': 'prices', 'publish': True}, {'dataset': 'restricted', 'publish': False}]}))
    result = capture_catalog(cold, Path(queue.config['publication_catalog']), include_unreferenced_objects=True)
    assert result['unreferenced_cold_objects_included'] is False and result['metadata_errors']


def test_incremental_deliveries_reconstruct_the_exact_canonical_union(stream, tmp_path):
    queue, cold, source, _, second = stream
    catalog = capture_catalog(cold, Path(queue.config["publication_catalog"]))
    rows = catalog["files"]
    union = tmp_path / "union"
    union.mkdir()
    for i, selection in enumerate(([r for r in rows if r["role"] == "cold_metadata"],
                                    [r for r in rows if r["role"] == "packed_object"])):
        root = tmp_path / ("wave" + str(i))
        proof = export_incremental_delivery(cold, selection, root, catalog_identity=catalog["identity_sha256"])
        verify(root, proof["envelope_identity_sha256"])
        assert proof["canonical_reconstruction_verified"] is False
        for row in selection:
            target = union / row["relative"]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((root / "cold" / row["relative"]).read_bytes())
    resolved = resolve_packed_snapshot_id(union, "prices", second.manifest["snapshot_id"])
    output = fetch_packed_snapshot(union, tmp_path / "recovered", resolved)
    assert scan_tree(output)["portable_fingerprint_sha256"] == scan_tree(source)["portable_fingerprint_sha256"]


def test_source_mutation_cannot_publish_ready(stream, tmp_path):
    queue, cold, _, _, _ = stream
    catalog = capture_catalog(cold, Path(queue.config["publication_catalog"]))
    row = next(r for r in catalog["files"] if r["role"] == "packed_object")
    (cold / row["relative"]).write_bytes(b"broken")
    destination = tmp_path / "bad-delivery"
    with pytest.raises(SnapshotError, match="changed"):
        export_incremental_delivery(cold, [row], destination, catalog_identity=catalog["identity_sha256"])
    assert not (destination / "READY").exists()


def test_absent_readiness_blocks_payload_publication(stream):
    queue, _, _, _, _ = stream
    result = queue.cycle()
    assert result["state"] == "waiting_receiver_readiness"
    assert result["pending_delivery_count"] == 0
    assert not list(queue.transport.glob("batch-*"))


def test_exact_nas_receipt_advances_bounded_queue_without_deleting_batches(stream):
    queue, _, _, _, _ = stream
    readiness(queue)
    first = queue.cycle()
    assert first["pending_delivery_count"] == 1
    ledger = queue.load_ledger()
    selected = next(iter(ledger["deliveries"].values()))
    assert queue.cycle()["pending_delivery_count"] == 1
    assert len(queue.load_ledger()["deliveries"]) == 1
    ack = acknowledge(queue, selected)
    validate_ack(ack, selected["dispatch"])
    result = queue.cycle()
    assert result["machine_acknowledged_files"] == len(selected["files"])
    assert len(queue.load_ledger()["deliveries"]) == 2
    assert (queue.transport / selected["dispatch"]["batch_relative"]).exists()
    assert result["all_history_backup_verified"] is False


@pytest.mark.parametrize("changes", [{"command_exit_codes": [0, 75]}, {"snapshot_id": "latest"},
    {"repository_id": "c" * 64}, {"all_files_sha256_verified": False}, {"complete_files": 100},
    {"repository_check_mode": "unchecked"}, {"complete_workflow_seconds": 0}])
def test_incomplete_or_unbound_acceptance_is_rejected(stream, changes):
    queue, _, _, _, _ = stream
    readiness(queue)
    queue.cycle()
    delivery = next(iter(queue.load_ledger()["deliveries"].values()))
    with pytest.raises(ValueError):
        acknowledge(queue, delivery, **changes)
    assert not list(queue.receipts.glob("acceptance-*.json"))


def test_reverse_channel_never_copies_private_fields(stream):
    queue, _, _, _, _ = stream
    readiness(queue)
    queue.cycle()
    delivery = next(iter(queue.load_ledger()["deliveries"].values()))
    ack = acknowledge(queue, delivery, password="do-not-copy", private_configuration="do-not-copy")
    assert "do-not-copy" not in json.dumps(ack)
    assert ack["independent_key_custody_verified"] is False
    assert ack["canonical_reconstruction_verified"] is False


@pytest.mark.parametrize("changes", [{"repository_id": "c" * 64}, {"nas_mount_guard_verified": False},
    {"observed_at_utc": "2000-01-01T00:00:00+00:00"}, {"automatic_pruning": True},
    {"ingress_free_bytes": -1}, {"runtime_lock_verified": False}])
def test_invalid_relay_readiness_blocks_publication(stream, changes):
    queue, _, _, _, _ = stream
    value = readiness(queue, **changes)
    with pytest.raises(ValueError):
        validate_readiness(value, queue.config)
    assert queue.cycle()["state"] == "waiting_receiver_readiness"


def test_relay_capacity_reserve_stops_new_batches(stream):
    queue, _, _, _, _ = stream
    readiness(queue, ingress_free_bytes=queue.config["reserve_bytes"])
    assert queue.cycle()["pending_delivery_count"] == 0


def test_sigkill_window_after_intent_is_recovered_without_duplicate_delivery(stream, monkeypatch):
    queue, _, _, _, _ = stream
    readiness(queue)
    original = module.os.rename
    def interrupt(*args):
        raise InterruptedError("simulate stop before atomic publication")
    monkeypatch.setattr(module.os, "rename", interrupt)
    with pytest.raises(InterruptedError):
        queue.cycle()
    assert len(queue.load_ledger()["deliveries"]) == 1
    monkeypatch.setattr(module.os, "rename", original)
    result = queue.cycle()
    assert result["pending_delivery_count"] == 1
    assert len(list(queue.transport.glob("batch-*"))) == 1
    assert len(list((queue.transport / "tools/dispatch").glob("*.json"))) == 1


def test_unready_staging_must_be_excluded_from_syncthing(stream):
    queue, _, _, _, _ = stream
    (queue.transport / ".stignore").write_text("")
    with pytest.raises(SnapshotError, match="staging"):
        queue.cycle()


def test_wave_budget_preserves_whole_objects_and_excludes_prior_selection(stream):
    queue, cold, _, _, _ = stream
    catalog = capture_catalog(cold, Path(queue.config["publication_catalog"]))
    wave = select_wave(catalog, set(), maximum_bytes=2 * 1024**2, maximum_files=2)
    assert len(wave) == 2
    assert sum(r["bytes"] for r in wave) + 1024**2 <= 2 * 1024**2
    assert {file_key(r) for r in wave}.isdisjoint({file_key(r) for r in select_wave(
        catalog, {file_key(r) for r in wave}, maximum_bytes=2 * 1024**2, maximum_files=2)})


def test_orphaned_failed_staging_is_retained_and_stops_new_copies(stream):
    queue, _, _, _, _ = stream
    readiness(queue)
    (queue.transport / ".staging").mkdir()
    failed = queue.transport / ".staging/failed-partial-copy"
    failed.write_bytes(b"x" * (queue.config["maximum_pending_bytes"] + 1))
    result = queue.cycle()
    assert result["pending_delivery_count"] == 0
    assert result["retained_staging_bytes"] == failed.stat().st_size
    assert failed.exists()


def test_recovery_requires_full_union_and_pinned_raw_manifest_bytes(stream, tmp_path):
    queue, cold, source, _, second = stream
    catalog = capture_catalog(cold, Path(queue.config["publication_catalog"]))
    inputs = []
    for role in ("cold_metadata", "packed_object"):
        delivery = tmp_path / role
        proof = export_incremental_delivery(cold, [r for r in catalog["files"] if r["role"] == role], delivery,
                                            catalog_identity=catalog["identity_sha256"])
        inputs.append({"root": str(delivery), "envelope_identity_sha256": proof["envelope_identity_sha256"],
                       "envelope_file_sha256": hashlib.sha256((delivery / "backup-envelope.json").read_bytes()).hexdigest()})
    kwargs = {"dataset": "prices", "snapshot_id": second.manifest["snapshot_id"], "manifest_sha256": second.manifest_sha256}
    with pytest.raises(SnapshotError, match="closure is incomplete"):
        assemble_release(inputs[:1], tmp_path / "incomplete", **kwargs)
    assert not (tmp_path / "incomplete").exists()
    with pytest.raises(SnapshotError, match="raw envelope"):
        assemble_release([{**inputs[0], "envelope_file_sha256": "f" * 64}], tmp_path / "forged", **kwargs)
    union = tmp_path / "union"
    proof = assemble_release(inputs, union, **kwargs)
    restored = fetch_packed_snapshot(union, tmp_path / "materialized", resolve_packed_snapshot_id(union, "prices", kwargs["snapshot_id"]))
    assert scan_tree(restored)["portable_fingerprint_sha256"] == scan_tree(source)["portable_fingerprint_sha256"]
    assert proof["nas_provenance_verified_by_this_tool"] is False
    assert not (union / "heads").exists()
    assert not (union / ".local-state").exists()


def auxiliary_inputs(queue, tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    (root / "README.md").write_text("frozen code\n")
    subprocess.run(["git", "add", "."], cwd=root, check=True)
    subprocess.run(["git", "-c", "user.name=backup-test", "-c", "user.email=backup-test@localhost",
                    "commit", "-qm", "synthetic code fixture"], cwd=root, check=True)
    control = tmp_path / "control"
    control.mkdir(mode=0o700)
    archive = control / "archive.backup"
    archive.write_bytes(b"synthetic test dump; no logical restore claimed")
    state = control / "state.json"
    body = {"contract": "all_user_table_columns_sorted_jsonb_rows_utc_v1", "tables": []}
    state.write_text(json.dumps({**body, "identity_sha256": module.identity_sha256(body)}))
    receipt = control / "latest.json"
    receipt.write_text(json.dumps({"state": "backup_written", "same_mvcc_snapshot_as_dump": True,
        "archive": str(archive), "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
        "logical_state_file": str(state), "logical_state_file_sha256": hashlib.sha256(state.read_bytes()).hexdigest(),
        "logical_state_identity_sha256": module.identity_sha256(body),
        "observed_at_utc": datetime.now(timezone.utc).isoformat()}))
    for path in (archive, state, receipt):
        path.chmod(0o600)
    queue.config["auxiliary"] = {"repository_root": str(root), "control_receipt": str(receipt),
        "interval_seconds": 3600, "maximum_bytes": 100 * 1024,
        "include_documentation": True, "include_configs": True}


def test_auxiliary_precedes_large_data_and_identical_capture_is_not_republished(stream, tmp_path):
    queue, _, _, _, _ = stream
    auxiliary_inputs(queue, tmp_path)
    readiness(queue)
    result = queue.cycle()
    assert result["auxiliary_error"] is None
    ledger = queue.load_ledger()
    key = ledger["last_auxiliary_delivery"]
    assert ledger["deliveries"][key]["dispatch"]["delivery_kind"] == "code_and_control_backup"
    assert not ledger.get("auxiliary_pending")
    acknowledge(queue, ledger["deliveries"][key])
    ledger["auxiliary_last_capture_epoch"] = 0
    module.private_json(queue.ledger_path, ledger)
    queue.cycle()
    ledger = queue.load_ledger()
    assert sum(d["dispatch"]["delivery_kind"] == "code_and_control_backup" for d in ledger["deliveries"].values()) == 1
    assert ledger["deliveries"][key]["acceptance"]["control_database_restore_verified"] is False


def test_auxiliary_restart_after_journal_commit_finishes_handoff_once(stream, tmp_path, monkeypatch):
    queue, _, _, _, _ = stream
    auxiliary_inputs(queue, tmp_path)
    readiness(queue)
    original = module.os.rename
    monkeypatch.setattr(module.os, "rename", lambda *a: (_ for _ in ()).throw(InterruptedError("synthetic SIGKILL window")))
    with pytest.raises(InterruptedError):
        queue.cycle()
    monkeypatch.setattr(module.os, "rename", original)
    result = queue.cycle()
    assert result["auxiliary_error"] is None
    assert not queue.load_ledger().get("auxiliary_pending")
    assert len(queue.load_ledger()["deliveries"]) == 1
    assert len(list(queue.transport.glob("batch-*"))) == 1


def test_explicit_refresh_preserves_identical_pending_snapshot_and_retains_superseded_work(stream, tmp_path):
    queue, _, _, _, _ = stream
    auxiliary_inputs(queue, tmp_path)
    queue.cycle()
    first = queue.load_ledger()["auxiliary_pending"]
    assert queue.refresh_auxiliary()["pending"] is True
    assert queue.load_ledger()["auxiliary_pending"] == first
    (tmp_path / "repo/README.md").write_text("new current code\n")
    queue.refresh_auxiliary()
    ledger = queue.load_ledger()
    assert ledger["auxiliary_pending"]["export"]["content_identity_sha256"] != first["export"]["content_identity_sha256"]
    assert (queue.transport / first["staging_relative"]).exists()


def test_relay_readiness_adapter_strips_secrets_and_rejects_another_repository(stream):
    queue, _, _, _, _ = stream
    body = readiness(queue)
    proof = queue.state / "private-readiness.json"
    proof.write_text(json.dumps({**body, "password": "synthetic-private", "nas_repository_path": "private-nas-path"}))
    public = publish_readiness(queue.config, proof, queue.receipts)
    assert "synthetic-private" not in json.dumps(public)
    assert "private-nas-path" not in json.dumps(public)
    proof.write_text(json.dumps({**body, "repository_id": "f" * 64}))
    with pytest.raises(ValueError, match="another NAS repository"):
        publish_readiness(queue.config, proof, queue.receipts)


def test_shipped_paired_profile_is_complete_for_receiver_readiness(stream):
    queue, _, _, _, _ = stream
    profile = json.loads((Path(__file__).resolve().parents[1] / "configs/data_sync/backup_receipts.json").read_bytes())
    private = readiness(queue)
    private.update({k: profile[k] for k in ("producer_device_id", "receiver_device_id", "repository_id")})
    proof = queue.state / "profile-readiness.json"
    proof.write_text(json.dumps(private))
    published = publish_readiness(profile, proof, queue.receipts)
    validate_readiness(published, profile)


def test_relayed_pilot_baseline_is_byte_bound_but_never_a_machine_ack(stream, tmp_path):
    queue, cold, _, _, _ = stream
    catalog = capture_catalog(cold, Path(queue.config["publication_catalog"]))
    staged = tmp_path / "pilot"
    exported = export_incremental_delivery(cold, catalog["files"], staged, catalog_identity=catalog["identity_sha256"])
    key = exported["envelope_identity_sha256"]
    delivery = queue.transport / ("batch-" + key)
    staged.rename(delivery)
    envelope = (delivery / "backup-envelope.json").read_bytes()
    report = {"evidence_origin": "user_relay_from_lab203_local_codex", "original_remote_receipts_received": False,
        "delivery": {"envelope_identity_sha256": key, "envelope_file_sha256": hashlib.sha256(envelope).hexdigest(),
            "repository_id": queue.config["repository_id"], "snapshot_id": "b" * 64,
            "files": len(catalog["files"]) + 2, "bytes": sum(r["bytes"] for r in catalog["files"]) + len(envelope) + 65,
            **{k: True for k in ("all_command_exit_codes_zero", "handoff_ready_both_envelope_hashes_exact_set_verified",
                "encrypted_backup_verified", "check_read_data_verified", "fixed_snapshot_independent_restore_verified",
                "source_verifier_verified", "all_401_file_sha256_verified")}}}
    report_path = tmp_path / "user-report.json"
    report_path.write_text(json.dumps(report))
    registered = queue.register_baseline(report_path, delivery)
    assert registered["machine_acknowledged"] is False
    queue.register_baseline(report_path, delivery)
    assert len(queue.load_ledger()["reported_baseline"]) == 1
    counts = coverage(catalog, queue.load_ledger())
    assert counts["user_reported_baseline_files"] == len(catalog["files"])
    assert counts["machine_acknowledged_files"] == 0
    report["delivery"]["envelope_file_sha256"] = "f" * 64
    report_path.write_text(json.dumps(report))
    with pytest.raises(SnapshotError, match="byte proof differs"):
        queue.register_baseline(report_path, delivery)
