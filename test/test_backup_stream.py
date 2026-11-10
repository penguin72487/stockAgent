"""Content deduplication, backpressure and NAS proof boundaries for the queue."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import fcntl
import json
import os
from pathlib import Path
import subprocess
import threading
import time
from types import SimpleNamespace
import zipfile

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


def test_catalog_snapshot_reuses_exact_pins_and_preserves_original_file_clock(tmp_path):
    catalog = {'contract': module.CATALOG, 'identity_sha256': 'a' * 64,
               'files': [{'relative': 'objects/aa/file', 'signature': [1, 2, 3, 4, 5]}],
               'observed_at_utc': '2026-11-03T00:00:00+00:00'}
    first = module.save_catalog_snapshot(tmp_path, catalog)
    path = tmp_path / 'catalog.json'
    original, original_stat = path.read_bytes(), path.stat()
    assert first['state'] == 'fresh_snapshot_written'
    next_catalog = {**catalog, 'observed_at_utc': '2026-11-03T00:01:00+00:00'}
    result = module.save_catalog_snapshot(tmp_path, next_catalog)
    assert result == {'state': 'unchanged_snapshot_reused',
                      'persisted_observed_at_utc': catalog['observed_at_utc']}
    assert path.read_bytes() == original and path.stat().st_ino == original_stat.st_ino
    marker = json.loads((tmp_path / 'catalog-observation.json').read_bytes())
    assert marker['observed_at_utc'] == next_catalog['observed_at_utc']
    assert marker['persisted_observed_at_utc'] == catalog['observed_at_utc']
    assert marker['NAS_backup_verified'] is False


@pytest.mark.parametrize('damage', ['identity', 'source_signature', 'snapshot_generation', 'marker'])
def test_catalog_snapshot_never_reuses_changed_source_or_file(tmp_path, damage):
    catalog = {'contract': module.CATALOG, 'identity_sha256': 'a' * 64,
               'files': [{'relative': 'objects/aa/file', 'signature': [1, 2, 3, 4, 5]}],
               'observed_at_utc': '2026-11-03T00:00:00+00:00'}
    module.save_catalog_snapshot(tmp_path, catalog)
    changed = deepcopy(catalog)
    changed['observed_at_utc'] = '2026-11-03T00:01:00+00:00'
    if damage == 'identity':
        changed['identity_sha256'] = 'b' * 64
    elif damage == 'source_signature':
        changed['files'][0]['signature'][4] += 1
    elif damage == 'snapshot_generation':
        (tmp_path / 'catalog.json').write_bytes(b'tampered')
    else:
        (tmp_path / 'catalog-observation.json').write_text('{bad json')
    result = module.save_catalog_snapshot(tmp_path, changed)
    assert result['state'] == 'fresh_snapshot_written'
    assert json.loads((tmp_path / 'catalog.json').read_bytes()) == changed


def test_catalog_snapshot_optimization_failure_keeps_existing_full_write(tmp_path, monkeypatch):
    catalog = {'identity_sha256': 'a' * 64, 'files': [{'relative': 'a', 'signature': [1, 2, 3, 4, 5]}],
               'observed_at_utc': '2026-11-03T00:00:00+00:00'}
    original = module.private_json
    def write(path, value, **kwargs):
        if path.name == 'catalog-observation.json':
            raise OSError('synthetic observation failure')
        return original(path, value, **kwargs)
    monkeypatch.setattr(module, 'private_json', write)
    result = module.save_catalog_snapshot(tmp_path, catalog)
    assert result['state'] == 'fresh_snapshot_written_observation_unavailable'
    assert json.loads((tmp_path / 'catalog.json').read_bytes()) == catalog
    assert not (tmp_path / 'catalog-observation.json').exists()


@pytest.mark.parametrize('schedule', ['inline', 'independent_timer'])
def test_private_dispatch_can_run_independently_without_recapturing_or_losing_live_status(stream, monkeypatch, schedule):
    queue, *_ = stream
    queue.config['private_history_backup'] = {'enabled': True}
    queue.config['private_history_schedule'] = schedule
    dispatched, observations = [], []
    monkeypatch.setattr(queue, 'private_history_cycle',
                        lambda **kw: dispatched.append(kw) or {'state': 'inline_private_result'})
    def public_cycle(**kwargs):
        (queue.state / 'private-status.json').write_text(json.dumps({
            'state': 'copying_current_private_wave', 'all_project_sources_NAS_verified': False}))
        return {'state': 'public_cycle_finished'}
    monkeypatch.setattr(queue, '_cycle', public_cycle)
    monkeypatch.setattr(queue, '_publish_cycle_status', observations.append)
    result = queue.cycle()
    assert result['state'] == 'public_cycle_finished'
    assert bool(dispatched) is (schedule == 'inline')
    assert result['private_history_backup']['state'] == (
        'inline_private_result' if schedule == 'inline' else 'copying_current_private_wave')
    assert observations == [result]


def test_unknown_private_schedule_does_not_change_the_original_queue(stream):
    queue, *_ = stream
    queue.config['private_history_schedule'] = 'typo'
    with pytest.raises(SnapshotError, match='unknown private backup schedule'):
        queue.cycle()


@pytest.mark.parametrize(("keys", "missing", "expected"), [
    ([], [], True),
    (["a", "a"], [], True),
    (("a", "b"), [], True),
    (["a", "missing"], [], False),
    (["a"], ["missing-object"], False),
    (["a", 1], [], False),
])
def test_release_coverage_preserves_reference_and_missing_object_semantics(keys, missing, expected):
    release = {"required_file_keys": keys, "missing_objects": missing}
    assert module._release_file_covered(release, {"a", "b"}) is expected


def test_release_coverage_still_rejects_late_unhashable_reference():
    release = {"required_file_keys": ["missing", []], "missing_objects": []}
    with pytest.raises(TypeError, match="unhashable"):
        module._release_file_covered(release, {"a"})
    # An incomplete release is never covered and does not inspect its keys.
    release["missing_objects"] = ["missing-object"]
    assert module._release_file_covered(release, {"a"}) is False


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


@pytest.mark.parametrize('age, expected', [(0, True), (120, True), (121, False),
                                         (-5, True), (-6, False)])
def test_transport_placement_hint_has_a_bounded_clock_and_requires_enrollment(tmp_path, age, expected):
    config = {'transport_namespace_handoff_contract': 'stockagent.transport_namespace_handoff.v1'}
    (tmp_path / 'attempt.json').write_text(json.dumps({
        'state': 'waiting_for_original_owner_or_reader', 'observed_at_epoch': 1000 - age}))
    assert module.transport_placement_pending(config, evidence=tmp_path, now=1000) is expected
    assert module.transport_placement_pending({}, evidence=tmp_path, now=1000) is False


@pytest.mark.parametrize('damage', ['absent', 'symlink', 'oversized', 'list', 'invalid', 'finished'])
def test_transport_placement_hint_does_not_block_on_invalid_or_completed_metadata(tmp_path, damage):
    config = {'transport_namespace_handoff_contract': 'stockagent.transport_namespace_handoff.v1'}
    path = tmp_path / 'attempt.json'
    if damage == 'symlink':
        target = tmp_path / 'other.json'
        target.write_text('{"state":"waiting_for_original_owner_or_reader","observed_at_epoch":1000}')
        path.symlink_to(target)
    elif damage == 'oversized':
        path.write_bytes(b' ' * 16385)
    elif damage == 'list':
        path.write_text('[]')
    elif damage == 'invalid':
        path.write_text('{broken')
    elif damage == 'finished':
        path.write_text('{"state":"completed","observed_at_epoch":1000}')
    assert module.transport_placement_pending(config, evidence=tmp_path, now=1000) is False


@pytest.mark.parametrize('purpose, expected', [
    ('namespace_handoff', True), ('old_D_transport_retirement', False),
    ('unknown', False), ([], False)])
def test_old_d_retirement_wait_cannot_veto_next_source_capture(tmp_path, purpose, expected):
    config = {'transport_namespace_handoff_contract': 'stockagent.transport_namespace_handoff.v1'}
    (tmp_path / 'attempt.json').write_text(json.dumps({
        'state': 'waiting_for_original_owner_or_reader', 'observed_at_epoch': 1000, 'purpose': purpose}))
    assert module.transport_placement_pending(config, evidence=tmp_path, now=1000) is expected


@pytest.mark.parametrize('change', [None, 'stale', 'future', 'contract', 'candidate', 'progress', 'forced', 'accepted'])
def test_drained_worker_handoff_keeps_priority_independently_of_gc(tmp_path, change):
    config = {'transport_namespace_handoff_contract': 'stockagent.transport_namespace_handoff.v1'}
    (tmp_path / 'attempt.json').write_text(json.dumps({
        'state': 'waiting_for_original_owner_or_reader', 'observed_at_epoch': 1000,
        'purpose': 'old_D_transport_retirement'}))
    worker = {'contract': 'stockagent.workspace_worker_handoff.v1',
        'state': 'waiting_for_original_owner', 'observed_at_epoch': 1000,
        'candidate_code_identity_sha256': 'a' * 64,
        'publication_progress_renewed': False, 'original_active_owner_forced': False}
    if change == 'stale':
        worker['observed_at_epoch'] = 879
    elif change == 'future':
        worker['observed_at_epoch'] = 1006
    elif change == 'contract':
        worker['contract'] = 'unknown'
    elif change == 'candidate':
        worker['candidate_code_identity_sha256'] = 'invalid'
    elif change == 'progress':
        worker['publication_progress_renewed'] = True
    elif change == 'forced':
        worker['original_active_owner_forced'] = True
    elif change == 'accepted':
        worker['state'] = 'accepted'
    (tmp_path / 'worker-reload.json').write_text(json.dumps(worker))
    assert module.transport_placement_pending(config, evidence=tmp_path, now=1000) is (change is None)
    assert module.transport_placement_pending({}, evidence=tmp_path, now=1000) is False


@pytest.mark.parametrize('lock,expected', [('catalog-owner.lock', False),
                                          ('dataset-versions/owner.lock', False),
                                          ('source-replication-owner.lock', True)])
def test_worker_yield_applies_only_to_a_boundary_the_stream_can_release(tmp_path, lock, expected):
    config = {'transport_namespace_handoff_contract': 'stockagent.transport_namespace_handoff.v1',
              'immutable_archive_state_root': str(tmp_path / 'catalog')}
    (tmp_path / 'worker-reload.json').write_text(json.dumps({
        'contract': 'stockagent.workspace_worker_handoff.v1', 'state': 'waiting_for_original_owner',
        'observed_at_epoch': 1000, 'candidate_code_identity_sha256': 'a' * 64,
        'publication_progress_renewed': False, 'original_active_owner_forced': False,
        'waiting_owner_lock': str(tmp_path / 'catalog' / lock)}))
    assert module.transport_placement_pending(config, evidence=tmp_path, now=1000) is expected
    (tmp_path / 'attempt.json').write_text(json.dumps({
        'state': 'waiting_for_original_owner_or_reader', 'observed_at_epoch': 1000,
        'purpose': 'namespace_handoff'}))
    assert module.transport_placement_pending(config, evidence=tmp_path, now=1000) is True


@pytest.mark.parametrize('independent_lock', [None, 'catalog-owner.lock', 'dataset-versions/owner.lock'])
def test_independent_gc_wait_continues_real_backup_cycle(stream, tmp_path, monkeypatch, independent_lock):
    queue, *_ = stream
    readiness(queue)
    queue.cycle()
    queue.config['transport_namespace_handoff_contract'] = 'stockagent.transport_namespace_handoff.v1'
    (tmp_path / 'attempt.json').write_text(json.dumps({
        'state': 'waiting_for_original_owner_or_reader',
        'observed_at_epoch': time.time(), 'purpose': 'old_D_transport_retirement'}))
    if independent_lock:
        archive = str(tmp_path / 'catalog')
        queue.config['immutable_archive_state_root'] = archive
        (tmp_path / 'worker-reload.json').write_text(json.dumps({
            'contract': 'stockagent.workspace_worker_handoff.v1', 'state': 'waiting_for_original_owner',
            'observed_at_epoch': time.time(), 'candidate_code_identity_sha256': 'a' * 64,
            'publication_progress_renewed': False, 'original_active_owner_forced': False,
            'waiting_owner_lock': str(Path(archive) / independent_lock)}))
    pending = module.transport_placement_pending
    monkeypatch.setattr(module, 'transport_placement_pending', lambda config: pending(config, evidence=tmp_path))
    capture = module.capture_catalog
    captures = []

    def observed_capture(*args, **kwargs):
        captures.append(True)
        return capture(*args, **kwargs)

    monkeypatch.setattr(module, 'capture_catalog', observed_capture)
    result = queue.cycle()
    assert captures == [True] and result['state'] != 'waiting_for_transport_placement'
    assert result['all_history_backup_verified'] is False
    assert result['source_cleanup_authorized'] is False
    assert not result['receipt_errors']


def test_queued_placement_yields_catalog_after_receipt_ingestion_and_keeps_catalog_clock(stream, monkeypatch):
    queue, *_ = stream
    readiness(queue)
    before = queue.cycle()
    envelope, delivery = next(iter(queue.load_ledger()['deliveries'].items()))
    acknowledge(queue, delivery)
    monkeypatch.setattr(module, 'transport_placement_pending', lambda _: True)
    monkeypatch.setattr(module, 'capture_catalog', lambda *a, **kw: pytest.fail('queued cutover recaptured history'))
    queue.config['auxiliary'] = {'enabled_for_test': True}
    monkeypatch.setattr(queue, 'stage_auxiliary', lambda *_: pytest.fail('queued cutover hashed the working tree'))
    queue.config['cold_transport_cache'] = {'enabled': True}
    retired = []

    def retire(ledger, **kwargs):
        assert ledger['deliveries'][envelope]['acceptance']
        retired.append(envelope)
        return {'retired_deliveries': 0, 'retired_logical_bytes': 0, 'errors': [], 'deferred': False}

    monkeypatch.setattr(queue, 'retire_transport_cache', retire)
    result = queue.cycle()
    assert retired == [envelope]
    assert queue.load_ledger()['deliveries'][envelope]['state'] == 'nas_file_restore_acknowledged'
    assert result['state'] == 'waiting_for_transport_placement'
    assert result['observed_at_utc'] == before['observed_at_utc']
    assert result['activity_at_utc'] > before['observed_at_utc']
    assert result['nas_covered_bytes'] == before['nas_covered_bytes']
    assert result['catalog_identity_sha256'] == before['catalog_identity_sha256']
    assert result['catalog_refresh']['state'] == 'deferred_transport_placement'
    assert result['catalog_refresh']['coverage_recomputed'] is False
    assert result['catalog_capture_started'] is False and result['prior_catalog_preserved'] is True
    assert result['source_cleanup_authorized'] is False
    assert json.loads((queue.state / 'status.json').read_bytes()) == result
    assert json.loads((queue.transport / 'tools/source-status.json').read_bytes()) == result
    # Returning the cycle also returns both original locks; no stale lease is removed.
    for name in ('cycle.lock', 'owner.lock'):
        with (queue.state / name).open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_capacity_wait_skips_catalog_but_ingests_ack_and_preserves_progress_clock(stream, monkeypatch):
    queue, *_ = stream
    readiness(queue)
    before = queue.cycle()
    ledger = queue.load_ledger()
    envelope, delivery = next(iter(ledger['deliveries'].items()))
    acknowledge(queue, delivery)
    usage = module.shutil.disk_usage(queue.transport)
    monkeypatch.setattr(module.shutil, 'disk_usage', lambda _: usage._replace(free=queue.config['reserve_bytes']))
    monkeypatch.setattr(module, 'capture_catalog', lambda *a, **kw: pytest.fail('capacity wait recaptured history'))
    queue.config['auxiliary'] = {'enabled_for_test': True}
    monkeypatch.setattr(queue, 'stage_auxiliary', lambda *_: pytest.fail('capacity wait hashed the working tree'))
    queue.config['cold_transport_cache'] = {'enabled': True}
    retired = []
    def retire(ledger, **kwargs):
        assert ledger['deliveries'][envelope]['acceptance']
        retired.append(envelope)
        return {'retired_deliveries': 0, 'retired_logical_bytes': 0, 'errors': [], 'deferred': False}
    monkeypatch.setattr(queue, 'retire_transport_cache', retire)
    result = queue.cycle()
    assert retired == [envelope]
    assert queue.load_ledger()['deliveries'][envelope]['state'] == 'nas_file_restore_acknowledged'
    assert result['state'] == 'backpressure'
    assert result['observed_at_utc'] == before['observed_at_utc']
    assert result['activity_at_utc'] > before['observed_at_utc']
    assert result['nas_covered_bytes'] == before['nas_covered_bytes']  # Not recomputed from the ledger.
    assert result['catalog_identity_sha256'] == before['catalog_identity_sha256']
    refresh = result['catalog_refresh']
    assert refresh['state'] == 'deferred_physical_capacity' and refresh['coverage_recomputed'] is False
    assert refresh['source_available_bytes'] == 0
    assert result['all_history_backup_verified'] is False and result['source_cleanup_authorized'] is False
    assert result['auxiliary_attempts'] == 0 and result['auxiliary_deferred_reason'] == 'physical_capacity'


def test_capacity_recheck_runs_after_retirement_and_recaptures_once_space_recovers(stream, monkeypatch):
    queue, cold, source, *_ = stream
    readiness(queue)
    queue.cycle()
    usage = module.shutil.disk_usage(queue.transport)
    free = [queue.config['reserve_bytes']]
    monkeypatch.setattr(module.shutil, 'disk_usage', lambda _: usage._replace(free=free[0]))
    capture = module.capture_catalog
    calls = []
    def observed_capture(*args, **kwargs):
        calls.append('captured')
        return capture(*args, **kwargs)
    monkeypatch.setattr(module, 'capture_catalog', observed_capture)
    assert queue.cycle()['catalog_refresh']['coverage_recomputed'] is False
    assert calls == []
    (source/'new-generation.txt').write_text('new source, never borrowed from the old catalog')
    publish_packed_snapshot(cold, 'prices', source, loose_file_threshold_bytes=1024, pack_buckets=2)
    queue.config['cold_transport_cache'] = {'enabled': True}
    def retire(*args, **kwargs):
        free[0] = usage.free  # Model the original owner's committed reclamation.
        return {'retired_deliveries': 0, 'retired_logical_bytes': 0, 'errors': [], 'deferred': False}
    monkeypatch.setattr(queue, 'retire_transport_cache', retire)
    resumed = queue.cycle()
    assert calls == ['captured'] and 'catalog_refresh' not in resumed
    assert resumed['retained_release_count'] == 3


def test_bootstrap_capacity_wait_has_no_invented_counts_and_inspection_still_captures(stream, monkeypatch):
    queue, *_ = stream
    usage = module.shutil.disk_usage(queue.transport)
    monkeypatch.setattr(module.shutil, 'disk_usage', lambda _: usage._replace(free=0))
    capture = module.capture_catalog
    calls = []
    def observed_capture(*args, **kwargs):
        calls.append('captured')
        return capture(*args, **kwargs)
    monkeypatch.setattr(module, 'capture_catalog', observed_capture)
    result = queue.cycle()
    assert result['state'] == 'backpressure' and result['observed_at_utc'] is None
    assert result.get('nas_covered_bytes') is None and result.get('available_file_count') is None
    assert not calls and not queue.load_ledger()['deliveries']
    inspected = queue.cycle(publish=False)
    assert calls == ['captured'] and inspected['retained_release_count'] == 2


@pytest.mark.parametrize("operation", ["publish", "inspect", "notify"])
def test_blocked_transport_scan_releases_owner_and_preserves_concurrent_ledger(stream, monkeypatch, operation):
    queue = stream[0]
    readiness(queue)
    if operation != "publish":
        queue.cycle()
    scanning, finish = threading.Event(), threading.Event()
    outcome = {}

    def scan(ledger):
        scanning.set()
        assert finish.wait(10), 'test must release the blocked scan'

    def cycle():
        try:
            outcome['result'] = queue.notify_transport() if operation == "notify" else queue.cycle(publish=operation == "publish")
        except Exception as error:
            outcome['error'] = error

    monkeypatch.setattr(queue, 'scan_transport', scan)
    worker = threading.Thread(target=cycle)
    worker.start()
    try:
        assert scanning.wait(5), 'the source must reach its transport scan'
        with (queue.state / 'owner.lock').open('a') as owner:
            deadline = time.monotonic() + 3
            while True:
                try:
                    fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        pytest.fail('network wait kept the publication owner locked')
                    time.sleep(0.02)
            ledger = queue.load_ledger()
            assert ledger['deliveries'], 'publication must be durable before yielding the owner'
            ledger['concurrent_owner_evidence'] = 'committed_while_network_scan_waited'
            module.private_json(queue.ledger_path, ledger)
    finally:
        finish.set()
        worker.join(timeout=5)
    assert not worker.is_alive()
    assert 'error' not in outcome, outcome.get('error')
    assert queue.load_ledger()['concurrent_owner_evidence'] == 'committed_while_network_scan_waited'


def scan_api(queue, monkeypatch, *, state="idle", fail_sub=None):
    from scripts import configure_artifact_ingress_syncthing as api
    queue.config["syncthing_folder_id"] = "backup-test"
    monkeypatch.setattr(queue, "paired_transport", lambda: ("test-api", "test-key", "backup-test"))
    calls, remaining_failures = [], [fail_sub] if fail_sub is not None else []

    def request(base, key, path, query=None, *, method="GET", timeout=30):
        calls.append((path, query, method, timeout))
        assert 1 <= timeout <= 30
        if path == "/rest/db/status":
            return {"state": state}
        assert path == "/rest/db/scan" and method == "POST"
        if remaining_failures and query["sub"] == remaining_failures[0]:
            remaining_failures.pop()
            raise TimeoutError("synthetic notification timeout; data stays committed")
        return {}

    monkeypatch.setattr(api, "request", request)
    return calls


def test_partial_scan_ack_is_durable_without_granting_nas_acceptance(stream, monkeypatch):
    queue = enable_pipeline(stream[0])
    readiness(queue)
    queue.cycle()
    ledger = queue.load_ledger()
    keys = list(ledger["deliveries"])
    assert len(keys) >= 2
    calls = scan_api(queue, monkeypatch, fail_sub=ledger["deliveries"][keys[1]]["dispatch"]["batch_relative"])
    before = queue.ledger_path.read_bytes()
    notification = queue.scan_transport(ledger)
    assert notification["state"] == "retryable_scan_error"
    assert len(notification["batch_scan_receipts"]) == 1
    assert queue.ledger_path.read_bytes() == before, "network worker never writes the owner journal"
    queue.record_transport_scan(ledger, notification)
    module.private_json(queue.ledger_path, ledger)
    restarted = BackupStream(queue.config)
    monkeypatch.setattr(restarted, "paired_transport", queue.paired_transport)
    current = restarted.load_ledger()
    retried = restarted.scan_transport(current)
    assert retried["state"] == "scan_request_acknowledged"
    restarted.record_transport_scan(current, retried)
    module.private_json(restarted.ledger_path, current)
    first = ledger["deliveries"][keys[0]]["dispatch"]["batch_relative"]
    assert sum(query.get("sub") == first for path, query, _, _ in calls if path == "/rest/db/scan") == 1
    assert not retried["NAS_backup_verified"]
    assert all(not row.get("acceptance") for row in current["deliveries"].values())
    assert all(row["transport_scan_notification"]["state"] == "scan_request_acknowledged"
               for row in current["deliveries"].values())
    changed_generation = deepcopy(current)
    changed_generation["deliveries"][keys[0]]["dispatch"]["identity_sha256"] = "b" * 64
    changed_generation["deliveries"][keys[0]].pop("transport_scan_notification")
    restarted.record_transport_scan(changed_generation, notification)
    assert "transport_scan_notification" not in changed_generation["deliveries"][keys[0]]


@pytest.mark.parametrize("state", ["scanning", ""])
def test_busy_or_invalid_transport_scan_state_never_posts_or_marks_ack(stream, monkeypatch, state):
    queue = stream[0]
    readiness(queue)
    queue.cycle()
    ledger = queue.load_ledger()
    calls = scan_api(queue, monkeypatch, state=state)
    notification = queue.scan_transport(ledger)
    queue.record_transport_scan(ledger, notification)
    assert notification["state"].startswith("deferred_")
    assert notification["pending_batch_scans"] == len(ledger["deliveries"])
    assert notification["batch_scan_receipts"] == []
    assert [path for path, _, _, _ in calls] == ["/rest/db/status"]
    assert all("transport_scan_notification" not in row for row in ledger["deliveries"].values())


def test_final_scan_timeout_keeps_cycle_receipt_and_committed_deliveries(stream, monkeypatch):
    queue = stream[0]
    readiness(queue)
    queue.cycle()
    before = queue.load_ledger()["deliveries"]
    scan_api(queue, monkeypatch, fail_sub="tools")
    result = queue.cycle(publish=False)
    assert result["transport_scan"]["state"] == "retryable_scan_error"
    assert result["pipeline"]["scan_errors"] == ["TimeoutError"]
    assert json.loads((queue.state / "status.json").read_bytes())["transport_scan"] == result["transport_scan"]
    assert queue.load_ledger()["deliveries"] == before
    for key, delivery in before.items():
        verify(queue.transport / delivery["dispatch"]["batch_relative"], key)


@pytest.mark.parametrize("publish", [False, True])
@pytest.mark.parametrize("folder_state", ["idle", "scanning"])
def test_cycle_final_status_and_clock_match_private_and_receiver_copies(stream, monkeypatch, publish, folder_state):
    queue = stream[0]
    readiness(queue)
    scan_api(queue, monkeypatch, state=folder_state)
    clock = [100.0]
    epoch = time.time()
    local_time = SimpleNamespace(**vars(time))
    local_time.perf_counter = lambda: clock[0]
    monkeypatch.setattr(module, "time", local_time)

    class ClockDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls.fromtimestamp(epoch + clock[0] - 100, tz)

    monkeypatch.setattr(module, "datetime", ClockDatetime)
    original_index, original_scan = module.recovery_index, queue.scan_transport

    def late_index(*args, **kwargs):
        clock[0] += 11
        return original_index(*args, **kwargs)

    def delayed_scan(ledger):
        clock[0] += 7
        return original_scan(ledger)

    monkeypatch.setattr(module, "recovery_index", late_index)
    monkeypatch.setattr(queue, "scan_transport", delayed_scan)
    result = queue.cycle(publish=publish)
    private = (queue.state / "status.json").read_bytes()
    receiver = (queue.transport / "tools/source-status.json").read_bytes()
    assert json.loads(private) == result
    assert json.loads(receiver) == result
    assert result["complete_cycle_seconds"] == clock[0] - 100
    assert result["observed_at_utc"] == ClockDatetime.now(timezone.utc).isoformat()
    assert private == receiver


def test_redirected_transport_pairing_still_rejects_before_scan(stream, monkeypatch):
    queue = stream[0]
    queue.config["syncthing_folder_id"] = "backup-test"
    def redirected():
        raise SnapshotError("backup transport no longer has the paired send-only scope")
    monkeypatch.setattr(queue, "paired_transport", redirected)
    with pytest.raises(SnapshotError, match="paired send-only"):
        queue.scan_transport(module.empty_ledger())


@pytest.mark.parametrize('idle_preflight', [False, True])
def test_notify_only_resumes_scan_without_catalog_capture_or_reexport(stream, monkeypatch, idle_preflight):
    queue = stream[0]
    readiness(queue)
    queue.cycle()
    before = deepcopy(queue.load_ledger()["deliveries"])
    calls = scan_api(queue, monkeypatch)
    monkeypatch.setattr(module, "capture_catalog", lambda *a, **kw: pytest.fail("notification cannot recapture history"))
    monkeypatch.setattr(module, "export_incremental_delivery", lambda *a, **kw: pytest.fail("notification cannot reexport payload"))
    notification = queue.notify_transport(idle_preflight=idle_preflight)
    assert notification["state"] == "scan_request_acknowledged"
    current = queue.load_ledger()["deliveries"]
    assert current.keys() == before.keys()
    for key, delivery in current.items():
        assert {name: value for name, value in delivery.items() if name != "transport_scan_notification"} == before[key]
        verify(queue.transport / delivery["dispatch"]["batch_relative"], key)
    assert len(calls) == 2 + len(before) + int(idle_preflight)
    assert json.loads((queue.state / "transport-scan-status.json").read_bytes()) == notification


@pytest.mark.parametrize('state', ['scanning', None, ''])
def test_notify_preflight_defers_without_loading_ledger_or_taking_publication_owner(stream, monkeypatch, state):
    queue = stream[0]
    calls = scan_api(queue, monkeypatch, state=state)
    monkeypatch.setattr(queue, 'load_ledger', lambda: pytest.fail('busy scan cannot load the large ledger'))
    monkeypatch.setattr(queue, '_save_ledger', lambda *a: pytest.fail('busy scan cannot rewrite the ledger'))
    with (queue.state/'owner.lock').open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = queue.notify_transport(idle_preflight=True)
    assert result['idle_preflight_only'] and result['pending_batch_scans'] is None
    assert not result['tools_scan_requested'] and not result['NAS_backup_verified']
    assert not result['batch_scan_receipts']
    assert len(calls) == 1 and calls[0][0] == '/rest/db/status'


def test_busy_notify_queues_one_exact_batch_without_holding_publication_owner(stream, monkeypatch):
    from scripts import configure_artifact_ingress_syncthing as api
    queue = stream[0]
    readiness(queue)
    queue.cycle()
    before = deepcopy(queue.load_ledger()['deliveries'])
    scan_api(queue, monkeypatch, state='scanning')
    queue.config['busy_scan_notification_enabled'] = True
    posts = []

    def request(base, key, path, query=None, **kwargs):
        if path == '/rest/db/status':
            return {'state': 'scanning'}
        posts.append(query)
        with (queue.state / 'owner.lock').open('a') as owner:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        intent = json.loads((queue.state / 'transport-busy-scan-intent.json').read_bytes())
        assert intent['state'] == 'requesting' and not intent['NAS_backup_verified']
        return None

    monkeypatch.setattr(api, 'request', request)
    result = queue.notify_transport(idle_preflight=True)
    first = next(iter(before))
    assert posts == [[('folder', 'backup-test'), ('sub', 'tools'),
                      ('sub', before[first]['dispatch']['batch_relative'])]]
    assert result['pending_batch_scans'] == len(before) - 1
    assert len(result['batch_scan_receipts']) == 1 and not result['NAS_backup_verified']
    after = queue.load_ledger()['deliveries']
    assert set(after) == set(before) and not any(v.get('acceptance') for v in after.values())
    assert after[first]['transport_scan_notification']['dispatch_identity_sha256'] == before[first]['dispatch']['identity_sha256']


def test_busy_scan_timeout_survives_restart_without_flooding_and_resumes_after_idle(stream, monkeypatch):
    from scripts import configure_artifact_ingress_syncthing as api
    queue = stream[0]
    readiness(queue)
    queue.cycle()
    scan_api(queue, monkeypatch, state='scanning')
    queue.config['busy_scan_notification_enabled'] = True
    states, posts = ['scanning'], []

    def request(base, key, path, query=None, **kwargs):
        if path == '/rest/db/status':
            return {'state': states[0]}
        posts.append(query)
        raise TimeoutError('the server may still execute this queued request')

    monkeypatch.setattr(api, 'request', request)
    result = queue.notify_transport(idle_preflight=True)
    assert result['state'] == 'retryable_scan_error' and not result['batch_scan_receipts']
    assert json.loads((queue.state / 'transport-busy-scan-intent.json').read_bytes())['state'] == 'waiting_idle'
    restarted = BackupStream(queue.config)
    monkeypatch.setattr(restarted, 'paired_transport', queue.paired_transport)
    with monkeypatch.context() as m:
        m.setattr(restarted, 'load_ledger', lambda: pytest.fail('uncertain busy request must use only its small intent'))
        deferred = restarted.notify_transport(idle_preflight=True)
    assert deferred['idle_preflight_only'] and len(posts) == 1
    calls = scan_api(restarted, monkeypatch, state='idle')
    retried = restarted.notify_transport(idle_preflight=True)
    assert retried['state'] == 'scan_request_acknowledged'
    assert all(v.get('transport_scan_notification') for v in restarted.load_ledger()['deliveries'].values())
    assert not any(v.get('acceptance') for v in restarted.load_ledger()['deliveries'].values())
    assert json.loads((queue.state / 'transport-busy-scan-intent.json').read_bytes())['state'] == 'idle_observed'
    assert any(path == '/rest/db/scan' for path, *_ in calls)


@pytest.mark.parametrize('state', ['scanning', '', None])
def test_busy_notify_does_not_queue_empty_or_unknown_scope(stream, monkeypatch, state):
    queue = stream[0]
    calls = scan_api(queue, monkeypatch, state=state)
    result = queue.scan_transport(module.empty_ledger(), queue_busy_scanner=True)
    assert result['state'].startswith('deferred_') and not result['batch_scan_receipts']
    assert [path for path, *_ in calls] == ['/rest/db/status']
    assert not (queue.state / 'transport-busy-scan-intent.json').exists()


def test_busy_scan_intent_is_single_flight_and_redirects_are_rejected(stream, monkeypatch, tmp_path):
    queue = stream[0]
    readiness(queue)
    queue.cycle()
    ledger = queue.load_ledger()
    calls = scan_api(queue, monkeypatch, state='scanning')
    lock = queue.state / 'transport-busy-scan.lock'
    with lock.open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = queue.scan_transport(ledger, queue_busy_scanner=True)
    assert result['state'] == 'deferred_existing_folder_scan'
    assert [path for path, *_ in calls] == ['/rest/db/status']
    (queue.state / 'transport-busy-scan-intent.json').symlink_to(tmp_path / 'outside')
    with pytest.raises(SnapshotError, match='intent is redirected'):
        queue.scan_transport(ledger, queue_busy_scanner=True)
    assert not any(path == '/rest/db/scan' for path, *_ in calls)


@pytest.mark.parametrize('state', ['syncing', 'scan-waiting', 'error'])
def test_uncertain_busy_scan_requires_actual_idle_before_retry(stream, monkeypatch, state):
    queue = stream[0]
    readiness(queue)
    queue.cycle()
    ledger = queue.load_ledger()
    calls = scan_api(queue, monkeypatch, state=state)
    intent = {'contract': 'backup_busy_scan_intent_v1', 'state': 'requesting'}
    module.private_json(queue.state / 'transport-busy-scan-intent.json', intent)
    result = queue.scan_transport(ledger, queue_busy_scanner=True)
    assert result['state'] == 'deferred_unconfirmed_scan_waiting_idle'
    assert json.loads((queue.state / 'transport-busy-scan-intent.json').read_bytes()) == intent
    assert [path for path, *_ in calls] == ['/rest/db/status']
    assert not result['batch_scan_receipts'] and not result['NAS_backup_verified']


def test_supervised_notifier_waits_for_original_owner_and_then_resumes(stream, monkeypatch):
    queue = stream[0]
    readiness(queue)
    queue.cycle()
    scan_api(queue, monkeypatch)
    entered, loaded = threading.Event(), threading.Event()
    original_write, original_load = module.private_json, queue.load_ledger
    outcome = {}

    def write(path, data, **kwargs):
        original_write(path, data, **kwargs)
        if data.get('state') == 'waiting_source_publication_owner':
            entered.set()

    def load():
        loaded.set()
        return original_load()

    def notify():
        try:
            outcome['result'] = queue.notify_transport(wait_for_owner=True, idle_preflight=True)
        except Exception as error:
            outcome['error'] = error

    monkeypatch.setattr(module, 'private_json', write)
    monkeypatch.setattr(queue, 'load_ledger', load)
    with (queue.state / 'owner.lock').open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        worker = threading.Thread(target=notify)
        worker.start()
        try:
            assert entered.wait(3) and worker.is_alive() and not loaded.is_set()
        finally:
            fcntl.flock(owner, fcntl.LOCK_UN)
            worker.join(timeout=8)
    assert not worker.is_alive() and 'error' not in outcome
    assert outcome['result']['state'] == 'scan_request_acknowledged'
    assert not outcome['result']['NAS_backup_verified']


def test_scan_budget_retains_exact_unnotified_batches_for_next_call(stream, monkeypatch):
    queue = enable_pipeline(stream[0])
    readiness(queue)
    queue.cycle()
    ledger = queue.load_ledger()
    calls = scan_api(queue, monkeypatch)
    monkeypatch.setattr(module, "MAX_TRANSPORT_SCAN_BATCHES", 1)
    while any("transport_scan_notification" not in row for row in ledger["deliveries"].values()):
        notification = queue.scan_transport(ledger)
        assert len(notification["batch_scan_receipts"]) == 1
        queue.record_transport_scan(ledger, notification)
    paths = [query["sub"] for path, query, _, _ in calls if path == "/rest/db/scan" and query["sub"] != "tools"]
    assert len(paths) == len(set(paths)) == len(ledger["deliveries"])


def test_scan_wall_budget_expires_without_starting_another_request(stream, monkeypatch):
    from scripts import configure_artifact_ingress_syncthing as api
    queue = stream[0]
    queue.config["syncthing_folder_id"] = "backup-test"
    monkeypatch.setattr(queue, "paired_transport", lambda: ("test-api", "test-key", "backup-test"))
    clock, calls = [100.0], []
    def request(base, key, path, query=None, **kwargs):
        calls.append(path)
        clock[0] = 131.0
        return {"state": "idle"}
    monkeypatch.setattr(api, "request", request)
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    notification = queue.scan_transport(module.empty_ledger())
    assert notification["state"] == "deferred_scan_budget"
    assert not notification["tools_scan_requested"]
    assert calls == ["/rest/db/status"]


@pytest.mark.parametrize("timeout", [0, True, "30", 1801])
def test_scan_rejects_invalid_budget_before_any_request(stream, monkeypatch, timeout):
    queue = stream[0]
    queue.config.update(syncthing_folder_id="backup-test", scan_timeout_seconds=timeout)
    monkeypatch.setattr(queue, "paired_transport", lambda: pytest.fail("invalid budget cannot reach the API"))
    with pytest.raises(SnapshotError, match="timeout must be bounded"):
        queue.scan_transport(module.empty_ledger())


def test_scan_rejects_unsafe_batch_path_before_any_request(stream, monkeypatch):
    queue = stream[0]
    queue.config["syncthing_folder_id"] = "backup-test"
    monkeypatch.setattr(queue, "paired_transport", lambda: pytest.fail("invalid path cannot reach the API"))
    ledger = {"deliveries": {"a" * 64: {"dispatch": {"identity_sha256": "b" * 64, "batch_relative": "../outside"}}}}
    with pytest.raises(SnapshotError, match="batch scan path"):
        queue.scan_transport(ledger)


def test_notify_only_rejects_redirected_owner_lock(stream, tmp_path):
    queue = stream[0]
    outside = tmp_path / "outside-lock"
    outside.write_text("preserve")
    (queue.state / "owner.lock").symlink_to(outside)
    with pytest.raises(SnapshotError, match="owner lock is redirected"):
        queue.notify_transport()
    assert outside.read_text() == "preserve"


def test_scan_http_error_closes_response_and_reports_bounded_capacity_reason(stream, monkeypatch):
    import io
    import urllib.error
    from scripts import configure_artifact_ingress_syncthing as api
    queue = stream[0]
    queue.config["syncthing_folder_id"] = "backup-test"
    monkeypatch.setattr(queue, "paired_transport", lambda: ("test-api", "test-key", "backup-test"))
    body = io.BytesIO(b'insufficient space on disk for database; never publish this raw body')
    def request(*args, **kwargs):
        raise urllib.error.HTTPError("test-api", 500, "scan failed", {}, body)
    monkeypatch.setattr(api, "request", request)
    result = queue.scan_transport(module.empty_ledger())
    assert body.closed
    assert result["state"] == "retryable_scan_error" and result["reason"] == "database_disk_reserve"
    assert result["http_status"] == 500 and result["error_type"] == "HTTPError"
    assert "never publish" not in json.dumps(result)


def test_catalog_capture_yields_owner_and_rebases_published_auxiliary_and_ack(stream, tmp_path, monkeypatch):
    queue = stream[0]
    auxiliary_inputs(queue, tmp_path)
    readiness(queue)
    capture = module.capture_catalog
    capturing, finish = threading.Event(), threading.Event()
    outcome = {}

    def blocked_capture(*args, **kwargs):
        result = capture(*args, **kwargs)
        capturing.set()
        assert finish.wait(10), 'test must finish the catalog observation'
        return result

    def cycle():
        try:
            outcome['result'] = queue.cycle()
        except Exception as error:
            outcome['error'] = error

    monkeypatch.setattr(module, 'capture_catalog', blocked_capture)
    worker = threading.Thread(target=cycle)
    worker.start()
    try:
        assert capturing.wait(5), 'the source must reach the full catalog capture'
        with pytest.raises(SnapshotError, match='another source stream cycle is active'):
            queue.cycle(publish=False)
        with (queue.state / 'owner.lock').open('a') as owner:
            try:
                fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                pytest.fail('read-only catalog capture retained the publication owner')
            ledger = queue.load_ledger()
            pending = ledger['auxiliary_pending']
            key = pending['export']['envelope_identity_sha256']
            queue.publish_closed(ledger, pending['files'], queue.transport / pending['staging_relative'],
                                 key, 'a' * 64, 'code_and_control_backup')
            queue.recover_publications(ledger)
            ledger['concurrent_catalog_evidence'] = 'keep the newer journal'
            module.private_json(queue.ledger_path, ledger)
            acknowledge(queue, ledger['deliveries'][key])
    finally:
        finish.set()
        worker.join(timeout=5)
    assert not worker.is_alive()
    assert 'error' not in outcome, outcome.get('error')
    ledger = queue.load_ledger()
    assert ledger['concurrent_catalog_evidence'] == 'keep the newer journal'
    assert ledger['last_auxiliary_delivery'] == key
    assert not ledger.get('auxiliary_pending')
    assert ledger['deliveries'][key]['state'] == 'nas_file_restore_acknowledged'
    assert len([d for d in ledger['deliveries'].values()
                if d['dispatch']['delivery_kind'] == 'code_and_control_backup']) == 1
    assert outcome['result']['auxiliary_nas_acknowledged'] is True
    verify(queue.transport / ledger['deliveries'][key]['dispatch']['batch_relative'], key)


def test_redirected_cycle_guard_rejected_before_catalog_or_publication(stream, tmp_path, monkeypatch):
    queue = stream[0]
    outside = tmp_path / 'unchanged-control'
    outside.write_text('preserve')
    (queue.state / 'cycle.lock').symlink_to(outside)
    monkeypatch.setattr(module, 'capture_catalog', lambda *a, **kw: pytest.fail('redirected cycle entered capture'))
    with pytest.raises(SnapshotError, match='cycle lock is redirected'):
        queue.cycle()
    assert outside.read_text() == 'preserve'
    assert not queue.load_ledger()['deliveries']


def test_failed_catalog_preserves_staged_auxiliary_and_concurrent_journal(stream, tmp_path, monkeypatch):
    queue = stream[0]
    auxiliary_inputs(queue, tmp_path)
    capturing, finish = threading.Event(), threading.Event()
    outcome = {}

    def failed_capture(*args, **kwargs):
        capturing.set()
        assert finish.wait(10)
        raise SnapshotError('synthetic full catalog failure')

    def cycle():
        try:
            queue.cycle()
        except Exception as error:
            outcome['error'] = error

    monkeypatch.setattr(module, 'capture_catalog', failed_capture)
    worker = threading.Thread(target=cycle)
    worker.start()
    try:
        assert capturing.wait(5)
        with (queue.state / 'owner.lock').open('a') as owner:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            ledger = queue.load_ledger()
            pending = deepcopy(ledger['auxiliary_pending'])
            ledger['concurrent_catalog_evidence'] = 'preserve after failed observation'
            module.private_json(queue.ledger_path, ledger)
    finally:
        finish.set()
        worker.join(timeout=5)
    assert not worker.is_alive()
    assert isinstance(outcome.get('error'), SnapshotError)
    ledger = queue.load_ledger()
    assert ledger['concurrent_catalog_evidence'] == 'preserve after failed observation'
    assert ledger['auxiliary_pending'] == pending
    assert not ledger['deliveries']
    verify(queue.transport / pending['staging_relative'], pending['export']['envelope_identity_sha256'])
    with (queue.state / 'owner.lock').open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_catalog_reentry_rechecks_storage_before_publication(stream, monkeypatch):
    queue = stream[0]
    readiness(queue)
    capture = module.capture_catalog
    guard = queue.storage_guard
    captured = False

    def observed(*args, **kwargs):
        nonlocal captured
        result = capture(*args, **kwargs)
        captured = True
        return result

    def changed_storage():
        if captured:
            raise SnapshotError('synthetic transport mount replacement')
        guard()

    monkeypatch.setattr(module, 'capture_catalog', observed)
    monkeypatch.setattr(queue, 'storage_guard', changed_storage)
    with pytest.raises(SnapshotError, match='transport mount replacement'):
        queue.cycle()
    assert not queue.load_ledger()['deliveries']
    assert not (queue.state / 'catalog.json').exists()
    assert not list(queue.transport.glob('batch-*'))


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


def test_auxiliary_snapshot_failure_exposes_bounded_reason_and_keeps_sources(stream, monkeypatch):
    queue, cold, source, *_ = stream
    queue.config['auxiliary'] = {'enabled': True}
    original = (source / '價格.txt').read_bytes()
    def failed(_):
        raise SnapshotError('retain failed staging; auxiliary capacity budget reached')
    monkeypatch.setattr(queue, 'stage_auxiliary', failed)
    result = queue.cycle(publish=False)
    assert result['auxiliary_error'] == 'SnapshotError'
    assert result['auxiliary_error_detail']['reason'] == 'retain failed staging; auxiliary capacity budget reached'
    assert result['auxiliary_attempts'] == 1 and result['auxiliary_retry_reasons'] == []
    assert (source / '價格.txt').read_bytes() == original and cold.is_dir()
    assert not queue.load_ledger()['deliveries']


def test_auxiliary_tree_race_retries_fresh_snapshot_without_discarding_failed_staging(stream, tmp_path, monkeypatch):
    from stockagent.data_sync import backup_auxiliary

    queue = stream[0]
    auxiliary_inputs(queue, tmp_path)
    root = Path(queue.config['auxiliary']['repository_root'])
    target = root / 'README.md'
    original_regular = backup_auxiliary._regular
    changed = []

    def change_once_before_copy(path):
        if path == target and not changed:
            target.write_text('new complete generation\n')
            changed.append(True)
        return original_regular(path)

    monkeypatch.setattr(backup_auxiliary, '_regular', change_once_before_copy)
    result = queue.cycle(publish=False)
    assert changed and result['auxiliary_error'] is None
    assert result['auxiliary_attempts'] == 2
    assert result['auxiliary_retry_reasons'] == ['code changed during its frozen working-tree backup']
    staging = list((queue.transport / '.staging').glob('auxiliary-*'))
    assert len(staging) == 2
    pending = queue.load_ledger()['auxiliary_pending']
    ready = queue.transport / pending['staging_relative']
    failed = next(path for path in staging if path != ready)
    assert not (failed / 'READY').exists()
    verify(ready, pending['export']['envelope_identity_sha256'])
    with zipfile.ZipFile(ready / 'code/working-tree.zip') as archive:
        assert archive.read('README.md') == b'new complete generation\n'


@pytest.mark.parametrize('reason', [
    'code changed during its frozen working-tree backup',
    'working tree changed during backup; READY withheld',
    'configuration changed during privacy classification',
])
def test_auxiliary_tree_race_retries_at_most_once_and_reports_failure(stream, monkeypatch, reason):
    queue = stream[0]
    queue.config['auxiliary'] = {'enabled': True}
    calls = []

    def fail(_):
        calls.append(True)
        raise SnapshotError(reason)

    monkeypatch.setattr(queue, 'stage_auxiliary', fail)
    result = queue.cycle(publish=False)
    assert len(calls) == result['auxiliary_attempts'] == 2
    assert result['auxiliary_retry_reasons'] == [reason]
    assert result['auxiliary_error_detail']['reason'] == reason


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


def test_new_atomic_head_during_catalog_capture_loads_its_exact_manifest(stream, monkeypatch):
    queue,cold,source,first,second=stream
    original=Path.glob
    def before_publication(path,pattern):
        results=list(original(path,pattern))
        if path==cold/'manifests' and pattern=='*/*.json':
            return iter([p for p in results if p!=second.manifest_path])
        return iter(results)
    monkeypatch.setattr(Path,'glob',before_publication)
    catalog=capture_catalog(cold,Path(queue.config['publication_catalog']))
    assert not catalog['metadata_errors'] and not catalog['missing_objects']
    assert len(catalog['releases'])==2
    latest=next(r for r in catalog['releases'] if r['snapshot_id']==second.manifest['snapshot_id'])
    assert latest['current_head'] and latest['manifest_sha256']==second.manifest_sha256
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


def test_capacity_inventory_restarts_after_concurrent_retirement_without_undercounting(stream, monkeypatch):
    queue = stream[0]
    wave = queue.transport / 'wave'
    wave.mkdir()
    vanished, retained = wave / 'retiring.bin', wave / 'retained.bin'
    vanished.write_bytes(b'x' * 91)
    retained.write_bytes(b'y' * 123)
    actual_stat = os.stat
    collided = []
    def concurrent_unlink(path, *args, **kwargs):
        if path == 'retiring.bin' and kwargs.get('dir_fd') is not None and not collided:
            collided.append(True)
            vanished.unlink()
            raise FileNotFoundError(2, 'concurrent GC unlink', str(vanished))
        return actual_stat(path, *args, **kwargs)
    monkeypatch.setattr(module.os, 'stat', concurrent_unlink)
    measured = queue.transport_usage()
    assert collided and not vanished.exists() and retained.read_bytes() == b'y' * 123
    assert measured['retained_transport_bytes'] == 123 + (queue.transport / '.stignore').stat().st_size
    assert measured['retained_staging_bytes'] == 0


def test_capacity_inventory_keeps_a_wave_renamed_into_staging_during_retry(stream, monkeypatch):
    queue = stream[0]
    wave = queue.transport / 'wave'
    wave.mkdir()
    (wave / 'payload').write_bytes(b'x' * 91)
    staging = queue.transport / '.staging'
    staging.mkdir()
    actual = queue._transport_usage_once
    attempts = []
    def rename_between_scan_and_stat():
        attempts.append(True)
        if len(attempts) == 1:
            wave.rename(staging / 'retiring-wave')
            raise FileNotFoundError(2, 'concurrent GC rename', str(wave))
        return actual()
    monkeypatch.setattr(queue, '_transport_usage_once', rename_between_scan_and_stat)
    measured = queue.transport_usage()
    assert len(attempts) == 2 and measured['retained_staging_bytes'] == 91
    assert measured['retained_transport_bytes'] == 91 + (queue.transport / '.stignore').stat().st_size
    assert (staging / 'retiring-wave/payload').read_bytes() == b'x' * 91


def test_capacity_inventory_repeated_changes_never_return_partial_capacity(stream, monkeypatch):
    queue, cold, source, *_ = stream
    original = (source / '價格.txt').read_bytes()
    attempts = []
    def unstable():
        attempts.append(True)
        raise FileNotFoundError(2, 'continued concurrent changes', str(queue.transport / 'wave'))
    monkeypatch.setattr(queue, '_transport_usage_once', unstable)
    with pytest.raises(SnapshotError, match='changed repeatedly'):
        queue.transport_usage()
    assert len(attempts) == 3 and cold.is_dir() and (source / '價格.txt').read_bytes() == original
    assert not queue.load_ledger()['deliveries']


def test_capacity_retry_rejects_replaced_authority_and_preserves_both_roots(stream, monkeypatch):
    queue = stream[0]
    retained = queue.state / 'original-transport'
    def replaced():
        queue.transport.rename(retained)
        queue.transport.mkdir()
        (queue.transport / 'unknown.bin').write_bytes(b'preserve replacement')
        raise FileNotFoundError(2, 'authority replaced', str(queue.transport / 'wave'))
    monkeypatch.setattr(queue, '_transport_usage_once', replaced)
    with pytest.raises(SnapshotError, match='authority changed'):
        queue.transport_usage()
    assert (retained / '.stignore').is_file()
    assert (queue.transport / 'unknown.bin').read_bytes() == b'preserve replacement'
    assert not queue.load_ledger()['deliveries']


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


def test_pipeline_ingests_ack_before_admitting_next_wave(stream, monkeypatch):
    queue = enable_pipeline(stream[0], waves=2)
    queue.config["maximum_pending_deliveries"] = 1
    readiness(queue)
    real_publish = queue.publish_closed
    acknowledged = []

    def publish_and_ack(ledger, rows, staging, key, catalog_identity, kind, **kwargs):
        real_publish(ledger, rows, staging, key, catalog_identity, kind, **kwargs)
        if not acknowledged:
            acknowledge(queue, ledger["deliveries"][key])
            acknowledged.append(key)

    monkeypatch.setattr(queue, "publish_closed", publish_and_ack)
    result = queue.cycle()
    ledger = queue.load_ledger()
    first = acknowledged[0]
    assert len(result["pipeline"]["published_waves"]) == 2
    assert len(ledger["deliveries"]) == 2 and ledger["deliveries"][first]["acceptance"]
    assert result["pending_delivery_count"] == 1
    first_keys = {file_key(row) for row in ledger["deliveries"][first]["files"]}
    second = next(key for key in ledger["deliveries"] if key != first)
    second_keys = {file_key(row) for row in ledger["deliveries"][second]["files"]}
    assert first_keys and second_keys and first_keys.isdisjoint(second_keys)
    assert result["machine_acknowledged_files"] == len(first_keys)


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


@pytest.mark.parametrize("damage", [None, "missing", "short", "symlink"])
@pytest.mark.parametrize("metadata_workers", [1, 4])
def test_catalog_observes_shared_objects_once_and_preserves_every_release(
        tmp_path, monkeypatch, damage, metadata_workers):
    cold, source = tmp_path / "cold", tmp_path / "source"
    initialize_packed_layout(cold, node_id="penguin")
    source.mkdir()
    for number in range(96):
        (source / f"{number:03}.bin").write_bytes(hashlib.sha256(str(number).encode()).digest())
    first = publish_packed_snapshot(cold, "prices", source, loose_file_threshold_bytes=1)
    (source / "new.bin").write_bytes(b"new observed source")
    second = publish_packed_snapshot(cold, "prices", source, loose_file_threshold_bytes=1)
    policy = tmp_path / "catalog.json"
    policy.write_text(json.dumps({"datasets": [{"dataset": "prices", "publish": True}]}))
    versions = [first, second]
    references = {
        version.manifest["snapshot_id"]: [version.manifest["archive"]["inventory"], *version.manifest["archive"]["objects"]]
        for version in versions
    }
    paths = {ref["relpath"] for refs in references.values() for ref in refs}
    shared = sorted({ref["relpath"] for ref in references[first.manifest["snapshot_id"]]}
                    & {ref["relpath"] for ref in references[second.manifest["snapshot_id"]]})[0]
    if damage:
        target = cold / shared
        target.unlink()
        if damage == "short":
            target.write_bytes(b"")
        elif damage == "symlink":
            target.symlink_to(source / "000.bin")
    seen = []
    original = module.PinnedObjectSignatures.regular_file_signature

    def observed(reader, relative):
        seen.append(relative)
        return original(reader, relative)

    monkeypatch.setattr(module.PinnedObjectSignatures, "regular_file_signature", observed)
    catalog = capture_catalog(cold, policy, metadata_workers=metadata_workers)
    assert len(seen) == len(paths) and set(seen) == paths
    assert len(catalog["releases"]) == len(versions)
    assert {row["relative"] for row in catalog["files"] if row["role"] == "packed_object"} == (
        paths - ({shared} if damage else set())
    )
    for release in catalog["releases"]:
        refs = references[release["snapshot_id"]]
        assert release["required_file_keys"] == sorted([
            release["manifest_relative"] + "@" + release["manifest_sha256"],
            *(ref["relpath"] + "@" + ref["sha256"] for ref in refs),
        ])
        assert release["missing_objects"] == ([shared] if damage else [])
    assert [row["relative"] for row in catalog["missing_objects"]] == ([shared] if damage else [])


@pytest.mark.parametrize("metadata_workers", [1, 4])
def test_catalog_rejects_object_directory_replacement_after_observation(stream, monkeypatch, metadata_workers):
    queue, cold, *_ = stream
    original = module.PinnedObjectSignatures.regular_file_signature
    replaced = []

    def replace_after_stat(reader, relative):
        observed = original(reader, relative)
        if not replaced:
            directory = (cold / relative).parent
            directory.rename(directory.with_name(directory.name + "-retained"))
            directory.mkdir()
            replaced.append(directory)
        return observed

    monkeypatch.setattr(module.PinnedObjectSignatures, "regular_file_signature", replace_after_stat)
    with pytest.raises(SnapshotError, match="directory changed"):
        capture_catalog(cold, Path(queue.config["publication_catalog"]), metadata_workers=metadata_workers)
    assert replaced


def test_parallel_catalog_preserves_complete_identity_and_does_overlap_leaf_stats(stream, monkeypatch):
    from threading import Barrier, Lock
    queue, cold, *_ = stream
    policy = Path(queue.config['publication_catalog'])
    baseline = capture_catalog(cold, policy, metadata_workers=1)
    barrier, lock, entered = Barrier(2), Lock(), []
    original = module.PinnedObjectSignatures.regular_file_signature

    def observed(reader, relative):
        with lock:
            entered.append(relative)
            wait = len(entered) <= 2
        if wait:
            barrier.wait(timeout=5)
        return original(reader, relative)

    monkeypatch.setattr(module.PinnedObjectSignatures, 'regular_file_signature', observed)
    parallel = capture_catalog(cold, policy, metadata_workers=4)
    assert entered
    assert {k:v for k,v in parallel.items() if k != 'observed_at_utc'} == {
        k:v for k,v in baseline.items() if k != 'observed_at_utc'}


def test_parallel_catalog_overlaps_manifest_reads_and_keeps_serial_identity(stream, monkeypatch):
    queue, cold, _, first, second = stream
    policy = Path(queue.config['publication_catalog'])
    baseline = capture_catalog(cold, policy, metadata_workers=1)
    names = {first.manifest_path.name, second.manifest_path.name}
    assert len(names) == 2
    original_open = os.open
    barrier = threading.Barrier(2, timeout=5)
    entered = []
    lock = threading.Lock()

    def observed_open(path, flags, *args, **kwargs):
        if path in names and kwargs.get('dir_fd') is not None:
            with lock:
                entered.append(path)
                wait = len(entered) <= 2
            if wait:
                barrier.wait()
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, 'open', observed_open)
    parallel = capture_catalog(cold, policy, metadata_workers=4)
    assert set(entered) == names
    assert {k: v for k, v in parallel.items() if k != 'observed_at_utc'} == {
        k: v for k, v in baseline.items() if k != 'observed_at_utc'}


@pytest.mark.parametrize('damage', ['invalid_json', 'symlink'])
def test_parallel_catalog_preserves_manifest_error_evidence(stream, damage):
    queue, cold, source, first, _ = stream
    path = first.manifest_path
    if damage == 'invalid_json':
        path.write_text('{invalid')
    else:
        path.unlink()
        path.symlink_to(source / 'shared.txt')
    policy = Path(queue.config['publication_catalog'])
    serial = capture_catalog(cold, policy, metadata_workers=1)
    parallel = capture_catalog(cold, policy, metadata_workers=4)
    assert serial['metadata_errors']
    assert {k: v for k, v in parallel.items() if k != 'observed_at_utc'} == {
        k: v for k, v in serial.items() if k != 'observed_at_utc'}


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


def test_canonical_orphan_census_avoids_recursive_type_probes_and_preserves_catalog(stream, monkeypatch):
    queue, cold, *_ = stream
    original = module._cold_object_paths
    monkeypatch.setattr(module, '_cold_object_paths', lambda cold, _pins:
                        module._recursive_cold_object_paths(cold))
    baseline = capture_catalog(cold, Path(queue.config['publication_catalog']), include_unreferenced_objects=True)
    monkeypatch.setattr(module, '_cold_object_paths', original)
    def no_recursive_walk(*args, **kwargs):
        pytest.fail('canonical CAS enumeration must not re-stat every known file type')
    monkeypatch.setattr(os, 'walk', no_recursive_walk)
    observed = capture_catalog(cold, Path(queue.config['publication_catalog']), include_unreferenced_objects=True)
    assert observed['identity_sha256'] == baseline['identity_sha256']
    assert observed['files'] == baseline['files']
    assert observed['releases'] == baseline['releases']
    assert observed['metadata_errors'] == []


def test_noncanonical_cold_namespace_keeps_original_recursive_error_evidence(stream):
    queue, cold, *_ = stream
    odd = cold / 'objects/unknown/nested/unique-original'
    odd.parent.mkdir(parents=True)
    odd.write_bytes(b'unique evidence retained')
    observed = capture_catalog(cold, Path(queue.config['publication_catalog']), include_unreferenced_objects=True)
    assert {'relative': odd.relative_to(cold).as_posix(), 'reason': 'SnapshotError'} in observed['metadata_errors']
    assert odd.read_bytes() == b'unique evidence retained'


def test_canonical_cold_census_rejects_redirected_unreferenced_shard(stream, tmp_path):
    queue, cold, *_ = stream
    outside = tmp_path / 'outside'
    outside.mkdir()
    shard = cold / 'objects/blobs/00'
    if shard.exists():
        # Pick a genuinely unused prefix; retain every real fixture object.
        shard = next(cold / 'objects/blobs' / f'{i:02x}' for i in range(256)
                     if not (cold / 'objects/blobs' / f'{i:02x}').exists())
    shard.symlink_to(outside, target_is_directory=True)
    with pytest.raises((OSError, SnapshotError)):
        capture_catalog(cold, Path(queue.config['publication_catalog']), include_unreferenced_objects=True)


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


@pytest.mark.parametrize('damage', ['manifest', 'head', 'historical_head'])
def test_unreadable_public_metadata_blocks_only_unreferenced_census(stream, damage):
    queue, cold, _source, first, _second = stream
    if damage == 'manifest':
        first.manifest_path.write_text('{invalid')
    elif damage == 'head':
        next((cold/'heads').glob('*/*.json')).write_text('{invalid')
    else:
        path = cold/'head-history/heads/prices/test'/('a'*64+'.json')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{invalid')
    result = capture_catalog(cold, Path(queue.config['publication_catalog']), include_unreferenced_objects=True)
    assert result['unreferenced_cold_objects_included'] is False
    assert result['metadata_errors'] and result['releases']


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


def test_unchanged_auxiliary_avoids_capacity_census_and_keeps_retained_payload(stream, tmp_path, monkeypatch):
    queue = stream[0]
    auxiliary_inputs(queue, tmp_path)
    readiness(queue)
    queue.cycle()
    ledger = queue.load_ledger()
    key = ledger['last_auxiliary_delivery']
    receipt = Path(queue.config['auxiliary']['control_receipt'])
    ledger['auxiliary_last_capture_epoch'] = 0
    retained = queue.transport/'.staging/failed-copy-to-preserve'
    retained.parent.mkdir(exist_ok=True)
    retained.write_bytes(b'unknown failed copy; never discard')
    monkeypatch.setattr(queue, 'transport_usage', lambda: pytest.fail('no export needs no capacity census'))
    queue.stage_auxiliary(ledger)
    assert not ledger.get('auxiliary_pending')
    assert ledger['auxiliary_last_capture_epoch'] > 0
    assert ledger['auxiliary_source_control_receipt_sha256'] == hashlib.sha256(receipt.read_bytes()).hexdigest()
    assert retained.read_bytes() == b'unknown failed copy; never discard'
    verify(queue.transport/ledger['deliveries'][key]['dispatch']['batch_relative'], key)
    assert len(ledger['deliveries']) == 1


def test_auxiliary_ledger_records_exported_generation_when_inputs_change_after_preflight(stream, tmp_path, monkeypatch):
    from stockagent.data_sync import backup_auxiliary

    queue = stream[0]
    auxiliary_inputs(queue, tmp_path)
    ledger = queue.load_ledger()
    root = Path(queue.config['auxiliary']['repository_root'])
    receipt = Path(queue.config['auxiliary']['control_receipt'])
    first_receipt = receipt.read_bytes()
    original_export = backup_auxiliary.export_auxiliary

    def change_inputs_then_export(*args, **kwargs):
        (root / 'README.md').write_text('changed after preflight\n')
        control = json.loads(receipt.read_bytes())
        control['observed_at_utc'] = '2026-01-02T03:04:05+00:00'
        receipt.write_text(json.dumps(control))
        return original_export(*args, **kwargs)

    monkeypatch.setattr(backup_auxiliary, 'export_auxiliary', change_inputs_then_export)
    queue.stage_auxiliary(ledger)
    pending = ledger['auxiliary_pending']
    assert ledger['last_auxiliary_content_identity'] == pending['export']['content_identity_sha256']
    assert ledger['auxiliary_source_control_receipt_sha256'] == hashlib.sha256(receipt.read_bytes()).hexdigest()
    assert ledger['auxiliary_source_control_receipt_sha256'] == pending['export']['control_source_receipt_sha256']

    # A later check of the same actual generation must not stage a duplicate.
    del ledger['auxiliary_pending']
    ledger['auxiliary_last_capture_epoch'] = 0
    monkeypatch.setattr(queue, 'transport_usage', lambda: pytest.fail('unchanged export needs no capacity census'))
    monkeypatch.setattr(backup_auxiliary, 'export_auxiliary', lambda *a, **kw: pytest.fail('duplicate export'))
    queue.stage_auxiliary(ledger)

    # Returning to the preflight receipt within the interval is a change from
    # the exported receipt and must therefore create a new delivery.
    receipt.write_bytes(first_receipt)
    monkeypatch.undo()
    queue.stage_auxiliary(ledger)
    assert ledger['auxiliary_pending']['export']['control_source_receipt_sha256'] == hashlib.sha256(first_receipt).hexdigest()


def test_unchanged_auxiliary_records_control_snapshot_used_after_preflight(stream, tmp_path, monkeypatch):
    from stockagent.data_sync import backup_auxiliary

    queue = stream[0]
    auxiliary_inputs(queue, tmp_path)
    ledger = queue.load_ledger()
    receipt = Path(queue.config['auxiliary']['control_receipt'])
    queue.stage_auxiliary(ledger)
    del ledger['auxiliary_pending']
    saved_receipt = receipt.read_bytes()
    changed = json.loads(saved_receipt)
    changed['observed_at_utc'] = '2026-01-02T03:04:05+00:00'
    changed_receipt = json.dumps(changed).encode()
    receipt.write_bytes(changed_receipt)
    ledger['auxiliary_last_capture_epoch'] = 0
    original_control_input = backup_auxiliary.control_input

    def original_generation_after_preflight(path):
        receipt.write_bytes(saved_receipt)
        return original_control_input(path)

    monkeypatch.setattr(backup_auxiliary, 'control_input', original_generation_after_preflight)
    queue.stage_auxiliary(ledger)
    assert not ledger.get('auxiliary_pending')
    assert ledger['auxiliary_source_control_receipt_sha256'] == hashlib.sha256(saved_receipt).hexdigest()

    # The previously probed generation returned after the unchanged check;
    # it must not be hidden by the interval shortcut.
    receipt.write_bytes(changed_receipt)
    monkeypatch.setattr(backup_auxiliary, 'control_input', original_control_input)
    queue.stage_auxiliary(ledger)
    assert ledger['auxiliary_pending']['export']['control_source_receipt_sha256'] == hashlib.sha256(changed_receipt).hexdigest()


@pytest.mark.parametrize("changed_input", ["code", "control_receipt"])
def test_changed_auxiliary_keeps_full_capacity_admission_before_export(stream, tmp_path, monkeypatch, changed_input):
    from stockagent.data_sync import backup_auxiliary
    queue = stream[0]
    auxiliary_inputs(queue, tmp_path)
    readiness(queue)
    queue.cycle()
    ledger = queue.load_ledger()
    ledger['auxiliary_last_capture_epoch'] = 0
    code = Path(queue.config['auxiliary']['repository_root'])/'README.md'
    receipt = Path(queue.config['auxiliary']['control_receipt'])
    if changed_input == "code":
        code.write_text('changed code must get a new backup\n')
    else:
        control = json.loads(receipt.read_bytes())
        control['observed_at_utc'] = "2026-01-02T03:04:05+00:00"
        receipt.write_text(json.dumps(control))
    retained_inputs = {path: path.read_bytes() for path in (code, receipt)}
    before = deepcopy(ledger)
    observed = []
    def full_capacity():
        observed.append(True)
        return {'retained_transport_bytes':queue.config['maximum_retained_transport_bytes'],
                'retained_staging_bytes':0}
    monkeypatch.setattr(queue, 'transport_usage', full_capacity)
    monkeypatch.setattr(backup_auxiliary, 'export_auxiliary', lambda *a, **kw: pytest.fail('capacity denied export'))
    with pytest.raises(SnapshotError, match='auxiliary capacity budget reached'):
        queue.stage_auxiliary(ledger)
    assert observed == [True] and ledger == before
    assert all(path.read_bytes() == body for path, body in retained_inputs.items())


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
