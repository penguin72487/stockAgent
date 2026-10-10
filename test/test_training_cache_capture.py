import json
from pathlib import Path
from types import SimpleNamespace
import pytest

from scripts import capture_inactive_training_cache as capture


def observation(*, age_hours=24, consumer=False):
    now = 10**18
    rows = [{"path": "view.npy", "kind": "file", "cross_filesystem": False,
             "signature": [1, 2, 100, now - age_hours * 3600 * 10**9, 5, 0o100640, 1]}]
    row = {"name": "legacy-view", "rows": rows, "newest_mtime_ns": rows[0]["signature"][3],
           "allocated_unique_file_bytes": 4096, "logical_bytes": 100,
           "process_references": ["active-job"] if consumer else [], "service_references": [], "service_error": None}
    policy = {"roots": {row["name"]: capture.portable_fingerprint(rows)}, "maximum_cohort_bytes": 100}
    return now, {"caches": [row]}, policy


def test_capture_selects_only_exact_old_inactive_enrollment():
    now, inventory, policy = observation()
    assert capture.select(inventory, policy, {}, now_ns=now) == (["legacy-view"], [])
    inventory["caches"][0]["name"] = "future-job-cache"
    assert capture.select(inventory, policy, {}, now_ns=now) == ([], [])


def test_captured_metadata_avoids_duplicate_transfer_without_authorizing_unlink():
    now, inventory, policy = observation()
    covered = {"legacy-view": {capture.portable_fingerprint(inventory["caches"][0]["rows"])}}
    assert capture.select(inventory, policy, covered, now_ns=now) == ([], [])
    inventory["caches"][0]["rows"][0]["signature"][1] += 10
    inventory["caches"][0]["rows"][0]["signature"][4] += 10
    # Controller-owned hardlink compaction changes inode/ctime; metadata alone
    # remains planning evidence. Actual retirement still rehashes all bytes.
    assert capture.select(inventory, policy, covered, now_ns=now) == ([], [])


def test_active_recent_changed_and_oversized_sources_wait():
    now, inventory, policy = observation(consumer=True)
    assert capture.select(inventory, policy, {}, now_ns=now)[1][0]["reason"] == "current-consumer-or-incomplete-observation"
    now, inventory, policy = observation(age_hours=1)
    assert capture.select(inventory, policy, {}, now_ns=now)[1][0]["reason"] == "twelve-hour-source-stability-pending"
    now, inventory, policy = observation()
    inventory["caches"][0]["rows"][0]["signature"][2] += 1
    assert capture.select(inventory, policy, {}, now_ns=now)[1][0]["reason"] == "enrolled-legacy-source-changed"
    now, inventory, policy = observation()
    policy["maximum_cohort_bytes"] = 99
    assert capture.select(inventory, policy, {}, now_ns=now)[1][0]["reason"] == "bounded-cohort-budget"


def test_receipt_approval_without_actual_received_members_is_not_coverage(tmp_path):
    (tmp_path / "cache.receipt.json").write_text(json.dumps({"scope": "cache", "state": "compressed_transport_received",
                                                          "producer_exit_code": 0, "compressed_sha256": "a" * 64,
                                                          "approved_cache_roots": ["cache/not-received"]}))
    assert capture.preserved_metadata([tmp_path]) == {}


def test_retained_index_must_match_received_compressed_sha(tmp_path):
    (tmp_path / "cache.receipt.json").write_text(json.dumps({"scope": "cache", "state": "compressed_transport_received",
                                                          "producer_exit_code": 0, "compressed_sha256": "a" * 64}))
    (tmp_path / "cache.original-index.json").write_text(json.dumps({"compressed_sha256": "b" * 64, "rows": []}))
    assert capture.preserved_metadata([tmp_path]) == {}


@pytest.mark.parametrize('covered', [True, False])
def test_capture_resource_drift_cannot_start_transfer_or_block_no_work(tmp_path,monkeypatch,covered):
    _,inventory,policy=observation()
    inventory.update(training_only_role_verified=True,node_profile={
        'machine_sha256':'current-node', 'limits':{'cpu_worker_budget':1,'memory_headroom_bytes':1}})
    measurement=tmp_path/'measurement.json'
    measurement.write_text(json.dumps({'state':'measured_remote_transport_accepted','selected_threads':2}))
    policy.update(schema_version=1,one_shot=True,origin_node_id='vastai1T',authority_node_id='penguin',
                  measurement_receipt=str(measurement),measurement_sha256=capture.sha256_file(measurement),
                  node_machine_sha256='previous-node')
    path=tmp_path/'policy.json';path.write_text(json.dumps(policy))
    monkeypatch.setattr(capture,'POLICY',path);monkeypatch.setattr(capture,'STATE',tmp_path)
    monkeypatch.setattr(capture,'private_file',lambda *a:None)
    monkeypatch.setattr(capture,'observe',lambda *a:inventory)
    fingerprints={'legacy-view':{capture.portable_fingerprint(inventory['caches'][0]['rows'])}} if covered else {}
    monkeypatch.setattr(capture,'preserved_metadata',lambda *a:fingerprints)
    monkeypatch.setattr(capture.subprocess,'run',lambda *a,**kw:pytest.fail('resource drift cannot authorize capture'))
    assert capture.capture(SimpleNamespace(ssh_target='unused',ssh_port=22,identity_file=tmp_path/'key'),[]) is False
    status=json.loads((tmp_path/'cache-capture-status.json').read_text())
    assert status['source_deleted'] is False
    assert status['state']==('enrolled_legacy_caches_captured' if covered else 'legacy_cache_capture_resource_remeasurement_pending')
