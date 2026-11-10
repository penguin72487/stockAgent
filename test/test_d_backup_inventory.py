import json
import os
from datetime import datetime, timezone

import pytest

from scripts.observe_d_backup_inventory import observe
from stockagent.data_sync.d_backup_inventory import CONTRACT, classify, public_pack_trial, public_progress, public_status


DIGEST = "a" * 64
IDENTITY = "b" * 64


def catalog(tmp_path):
    key = "objects/shared@" + DIGEST
    other = "objects/other@" + DIGEST
    payload = {"contract": "retained_packed_backup_catalog_v1", "releases": [
        {"dataset": "tw-public", "current_head": True, "required_file_keys": [key, other]},
        {"dataset": "binance-market", "current_head": True, "required_file_keys": [key]},
        {"dataset": "tw-public", "current_head": False, "required_file_keys": [key, "missing@" + DIGEST]},
    ], "files": [
        {"relative": "objects/shared", "sha256": DIGEST, "bytes": 100, "role": "packed_object"},
        {"relative": "objects/other", "sha256": DIGEST, "bytes": 40, "role": "unreferenced_cold_object"},
    ], "identity_sha256": IDENTITY}
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(payload))
    return path, payload


def test_shared_objects_are_once_globally_and_in_each_referencing_group(tmp_path):
    path, _ = catalog(tmp_path)
    result = classify(path, expected_identity=IDENTITY, expected_files=2, expected_bytes=140)
    groups = {r["id"]: r for r in result["groups"]}
    assert (result["total_files"], result["total_bytes"], result["unresolved_references"]) == (2, 140, 1)
    assert groups["taiwan"]["referenced_bytes"] == 140
    assert groups["crypto"]["referenced_bytes"] == 100
    assert groups["taiwan"]["retained_versions"] == 2
    assert groups["taiwan"]["current_versions"] == 1
    assert result["roles"]["unreferenced_cold_object"]["bytes"] == 40
    public = public_status(result, now=datetime.now(timezone.utc).timestamp())
    assert "objects/shared" not in json.dumps(public)
    assert IDENTITY not in json.dumps(public)
    assert public["groups"][0]["datasets"] == [{"name": "tw-public", "versions": 2}]


def test_rejects_wrong_owner_identity_count_and_duplicate_files(tmp_path):
    path, payload = catalog(tmp_path)
    with pytest.raises(ValueError, match="mismatched"):
        classify(path, expected_identity="c" * 64)
    with pytest.raises(ValueError, match="file_count"):
        classify(path, expected_identity=IDENTITY, expected_files=3)
    payload["files"].append(payload["files"][0])
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="duplicate"):
        classify(path, expected_identity=IDENTITY)


def test_catalog_replaced_during_read_never_becomes_result(tmp_path):
    path, payload = catalog(tmp_path)
    replacement = tmp_path / "replacement.json"
    replacement.write_text(json.dumps(payload))
    moved = False
    def replace_once(_stream, _size):
        nonlocal moved
        if not moved:
            os.replace(replacement, path)
            moved = True
    with pytest.raises(ValueError, match="generation_changed"):
        classify(path, expected_identity=IDENTITY, progress=replace_once)


def test_observer_reuses_identity_without_relabeling_scan_time(tmp_path):
    path, _ = catalog(tmp_path)
    status = tmp_path / "status.json"
    status.write_text(json.dumps({"catalog_identity_sha256": IDENTITY, "available_file_count": 2,
                                  "available_bytes": 140, "catalog_observed_at_utc": "2026-11-05T00:00:00+00:00"}))
    output, progress = tmp_path / "output.json", tmp_path / "progress.json"
    assert observe(path, status, output, progress) == "completed"
    original = output.read_bytes()
    assert observe(path, status, output, progress) == "current"
    assert output.read_bytes() == original
    assert json.loads(progress.read_text())["state"] == "current"
    assert output.stat().st_mode & 0o777 == 0o600


def test_public_projection_hides_unknown_fields_and_stale_observation(tmp_path):
    path, _ = catalog(tmp_path)
    result = classify(path, expected_identity=IDENTITY)
    result["private_path"] = "/private/secret"
    result["observed_at_utc"] = "2026-11-01T00:00:00+00:00"
    projected = public_status(result, now=datetime(2026, 11, 5, tzinfo=timezone.utc).timestamp())
    assert projected["stale"] is True
    assert "/private/secret" not in json.dumps(projected)
    assert public_status({"contract": "wrong"}, now=0) is None


def test_catalog_eta_is_phase_local_and_stale_progress_cannot_offer_eta():
    now = datetime(2026, 11, 5, tzinfo=timezone.utc).timestamp()
    progress = {"contract": CONTRACT, "state": "scanning", "observed_at_utc": "2026-11-05T00:00:00+00:00",
                "processed_bytes": 50, "total_bytes": 100, "completion_at_utc": "2026-11-05T00:01:00+00:00",
                "path": "/private/catalog"}
    active = public_progress(progress, now=now)
    assert active["scope"] == "catalog_classification_only"
    assert active["completion_at_utc"] == "2026-11-05T00:01:00+00:00"
    assert "/private" not in json.dumps(active)
    stale = public_progress(progress, now=now + 181)
    assert stale["state"] == "stale" and stale["completion_at_utc"] is None


def test_trial_projection_is_bounded_and_cannot_claim_production_changed():
    value = {"state": "completed", "source_files_modified": 0, "production_codec_changed": False,
             "files": [{"state": "roundtrip_verified_source_unchanged", "source": "sample.parquet", "source_bytes": 100,
                        "source_sha256": IDENTITY, "candidates": [{"codec": "zstd", "level": 3, "bytes": 80,
                            "write_fsync_seconds": 0.1, "warm_read_decode_seconds": 0.2,
                            "full_value_roundtrip_verified": True}]}]}
    projected = public_pack_trial(value)
    assert projected["scope"] == "e_small_sample_semantic_roundtrip_only"
    assert IDENTITY not in json.dumps(projected)
    value["files"][0]["source"] = "/private/sample.parquet"
    assert public_pack_trial(value) is None


def test_gateway_reads_only_small_sanitized_catalog_projection(tmp_path, monkeypatch):
    from test_public_dashboards import _test_server
    from stockagent.data_sync import d_backup_inventory
    from stockagent.service_layout import service_path
    path, _ = catalog(tmp_path)
    inventory = classify(path, expected_identity=IDENTITY)
    inventory["catalog_observed_at_utc"] = datetime.now(timezone.utc).isoformat()
    inventory["private_path"] = "/private/secret"
    destination = service_path("data_monitor/d_backup_inventory.json", root=tmp_path)
    destination.parent.mkdir(parents=True)
    destination.write_text(json.dumps(inventory))
    service_path("data_monitor/storage_status.json", root=tmp_path).write_text(
        json.dumps({"contract": "stockagent.storage_monitor.v1", "volumes": []}))
    monkeypatch.setattr(d_backup_inventory, "classify", lambda *_a, **_k: pytest.fail("HTTP scanned a catalog"))
    server = _test_server()
    server.repo_root = tmp_path
    try:
        payload = json.loads(server.data_monitor_storage().body)
        assert payload["d_backup_inventory"]["total_files"] == 2
        assert "/private" not in json.dumps(payload)
        assert IDENTITY not in json.dumps(payload)
    finally:
        server.server_close()
