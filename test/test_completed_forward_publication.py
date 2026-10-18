from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path

import pytest
import torch

from scripts import publish_data_releases as releases
from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync import completed_forward_publication as publication
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_retention import _local_protected_snapshot_ids
from stockagent.live.completed_inference_cache import cached_forward, identity_digest


def _fixture(tmp_path, monkeypatch):
    monkeypatch.setattr(releases, "REPO_ROOT", tmp_path)
    source = tmp_path / "cache"
    identity = {"fixture": "complete CPU output", "source": [1, 2, 3]}
    value = {"scores": torch.tensor([float("nan"), -0.0, 1.25]), "aux": [None, "raw"]}
    cached_forward(source, identity, lambda: value)
    key = identity_digest(identity)
    (source / "unselected-private-data").write_bytes(b"must not publish")
    entry = {"dataset": "completed-tw-forward", "source": str(source),
             "publication_family": "completed_tw_panel_forward_cpu_v3",
             "role": "private-derived-forward", "publish": True,
             "pack_buckets": 1, "loose_threshold_bytes": 1,
             "active_process_substrings": [], "note": "fixture; not full cache coverage"}
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"schema_version": 1, "datasets": [], "publication_families": [entry]}))
    cold = tmp_path / "cold"
    materialized = tmp_path / "materialized"
    materialized.mkdir(mode=0o700)
    retention = tmp_path / "retention.json"
    retention.write_text(json.dumps({"schema_version": 1, "sync_root": str(cold),
        "archive_root": str(tmp_path / "archive"), "materialized_root": str(materialized),
        "backup_config": "unused.json", "state_dir": str(tmp_path / "state"),
        "folder_id": "fixture", "required_peer_names": []}))
    return source, identity, key, catalog, cold, materialized, retention


def _argv(fixture, command="publish"):
    _, _, key, catalog, cold, _, retention = fixture
    return [command, "completed-tw-forward", "--completed-forward-key", key,
            "--catalog", str(catalog), "--sync-root", str(cold),
            "--retention-config", str(retention), "--node-id", "test-node"]


def test_catalog_family_cannot_implicitly_publish_whole_cache(tmp_path, monkeypatch, capsys):
    fixture = _fixture(tmp_path, monkeypatch)
    source, _, _, catalog, cold, _, _ = fixture
    assert releases.main(["status", "--catalog", str(catalog)]) == 0
    status = json.loads(capsys.readouterr().out)["datasets"][0]
    assert not status["publish_ready"]
    assert status["requires_explicit_completed_forward_key"]
    assert releases.main(["publish", "--all-ready", "--catalog", str(catalog),
                          "--sync-root", str(cold), "--node-id", "test-node"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["published"] == [] and len(result["skipped"]) == 1
    assert not list((cold / "manifests").glob("*/*.json"))
    assert (source / "unselected-private-data").read_bytes() == b"must not publish"


def test_family_template_is_not_a_training_or_dashboard_source_dataset(tmp_path, monkeypatch):
    fixture = _fixture(tmp_path, monkeypatch)
    catalog = fixture[3]
    assert json.loads(catalog.read_text())["datasets"] == []
    assert releases._load_catalog(catalog) == []
    assert len(releases._load_catalog(catalog, True)) == 1


def test_status_does_not_load_torch_or_decode_payloads(tmp_path, monkeypatch):
    import subprocess
    import sys

    fixture = _fixture(tmp_path, monkeypatch)
    code = ("import sys; from scripts.publish_data_releases import main; "
            f"assert main(['status', '--catalog', {str(fixture[3])!r}]) == 0; "
            "assert 'torch' not in sys.modules")
    result = subprocess.run([sys.executable, "-c", code], text=True, capture_output=True, timeout=10)
    assert result.returncode == 0, result.stderr


def test_explicit_key_uses_original_lock_publisher_and_retention_pin(tmp_path, monkeypatch, capsys):
    fixture = _fixture(tmp_path, monkeypatch)
    source, identity, key, _, cold, materialized, _ = fixture
    original = releases.publish_packed_snapshot
    calls = []

    def publish(*args, **kwargs):
        with (source / f"{key}.lock").open("rb") as other:
            with pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        calls.append(kwargs)
        return original(*args, **kwargs)

    monkeypatch.setattr(releases, "publish_packed_snapshot", publish)
    assert releases.main(_argv(fixture)) == 0
    result = json.loads(capsys.readouterr().out)["published"][0]
    pin = Path(result["fixed_pin"]["path"])
    assert pin.is_relative_to(materialized) and pin.name.endswith(".pin.json")
    assert len(calls) == 1
    names = [f"{key}.forward", f"{key}.receipt.json"]
    assert calls[0]["included_root_files"] == names
    fixed = packed.resolve_packed_pin(cold, pin, expected_dataset=result["dataset"])
    assert {obj["kind"] for obj in fixed.manifest["archive"]["objects"]} == {"blob"}
    proof = packed.verify_packed_snapshot(cold, fixed, reconstruct_paths=names)
    assert proof["independently_reconstructed_files"] == 2
    ids, evidence, errors = _local_protected_snapshot_ids(materialized, {fixed.manifest["snapshot_id"]})
    assert ids == {fixed.manifest["snapshot_id"]} and evidence and not errors
    target = packed.fetch_packed_snapshot(cold, tmp_path / "reader", fixed)
    assert sorted(p.name for p in target.iterdir()) == names
    assert all((target / name).read_bytes() == (source / name).read_bytes() for name in names)
    value, hit = cached_forward(target, identity, lambda: pytest.fail("must not compute"))
    assert hit["state"] == "disk_hit" and torch.isnan(value["scores"][0])
    assert torch.signbit(value["scores"][1])
    pin_before = pin.read_bytes(), pin.stat().st_mtime_ns
    monkeypatch.setattr(releases, "publish_packed_snapshot", lambda *_a, **_kw: pytest.fail("reuse fixed binding"))
    assert releases.main(_argv(fixture)) == 0
    repeat = json.loads(capsys.readouterr().out)["published"][0]
    assert repeat["fixed_pin"]["reused"] is True
    assert repeat["new_sync_bytes"] == repeat["new_sync_objects"] == repeat["changed_files"] == 0
    assert repeat["reused_files"] == 2
    assert (pin.read_bytes(), pin.stat().st_mtime_ns) == pin_before


@pytest.mark.parametrize("fault", ["missing-payload", "bad-receipt", "wrong-key", "noninteger-size", "checksum", "writable", "redirected", "missing-lock"])
def test_invalid_source_never_publishes_or_relabels(tmp_path, monkeypatch, capsys, fault):
    fixture = _fixture(tmp_path, monkeypatch)
    source, _, key, _, cold, _, _ = fixture
    payload = source / f"{key}.forward"
    receipt = source / f"{key}.receipt.json"
    if fault == "missing-payload":
        payload.unlink()
    elif fault == "bad-receipt":
        receipt.write_text("{")
    elif fault == "wrong-key":
        record = json.loads(receipt.read_text())
        record["identity"]["extra"] = "not this identity"
        receipt.write_text(json.dumps(record))
    elif fault == "noninteger-size":
        record = json.loads(receipt.read_text())
        record["payload_bytes"] = float(record["payload_bytes"])
        receipt.write_text(json.dumps(record))
    elif fault == "checksum":
        payload.chmod(0o600)
        payload.write_bytes(b"invalid")
        payload.chmod(0o400)
    elif fault == "writable":
        payload.chmod(0o600)
    elif fault == "redirected":
        real = source / "other.forward"
        payload.rename(real)
        payload.symlink_to(real)
    elif fault == "missing-lock":
        (source / f"{key}.lock").unlink()
    assert releases.main(_argv(fixture)) == 2
    assert not list((cold / "manifests").glob("*/*.json"))
    assert "ERROR" in capsys.readouterr().err
    if fault == "missing-lock":
        assert not (source / f"{key}.lock").exists()


def test_fixed_pin_failure_never_overwrites_or_republishes(tmp_path, monkeypatch, capsys):
    fixture = _fixture(tmp_path, monkeypatch)
    assert releases.main(_argv(fixture)) == 0
    pin = Path(json.loads(capsys.readouterr().out)["published"][0]["fixed_pin"]["path"])
    pin.write_text("{broken but retained}")
    monkeypatch.setattr(releases, "publish_packed_snapshot", lambda *_a, **_kw: pytest.fail("do not republish on pin failure"))
    assert releases.main(_argv(fixture)) == 2
    assert pin.read_text() == "{broken but retained}"


@pytest.mark.parametrize("option", ["--all-ready", "bad-key", "wrong-dataset", "publish-disabled"])
def test_key_cannot_bypass_catalog_or_bulk_selection(tmp_path, monkeypatch, capsys, option):
    fixture = _fixture(tmp_path, monkeypatch)
    args = _argv(fixture)
    if option == "--all-ready":
        args.append(option)
    elif option == "bad-key":
        args[args.index("--completed-forward-key") + 1] = "../../other"
    elif option == "wrong-dataset":
        args[1] = "unknown"
    elif option == "publish-disabled":
        catalog = fixture[3]
        data = json.loads(catalog.read_text())
        data["publication_families"][0]["publish"] = False
        catalog.write_text(json.dumps(data))
    assert releases.main(args) == 2
    assert "ERROR" in capsys.readouterr().err


def test_retention_root_mismatch_denied_before_publication(tmp_path, monkeypatch, capsys):
    fixture = _fixture(tmp_path, monkeypatch)
    retention = fixture[-1]
    data = json.loads(retention.read_text())
    data["sync_root"] = str(tmp_path / "another-authority")
    retention.write_text(json.dumps(data))
    monkeypatch.setattr(releases, "publish_packed_snapshot", lambda *_a, **_kw: pytest.fail("retention must cover this store"))
    assert releases.main(_argv(fixture)) == 2
    assert "ERROR" in capsys.readouterr().err


def test_exclusive_standard_pin_never_overwrites_unknown_file(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "value").write_text("known")
    release = packed.publish_packed_snapshot(tmp_path / "cold", "fixture", source, node_id="test-node")
    pin = tmp_path / "fixed.pin.json"
    pin.write_bytes(b"unknown original")
    with pytest.raises((SnapshotError, FileExistsError)):
        packed.write_packed_pin(pin, release, exclusive=True)
    assert pin.read_bytes() == b"unknown original"
    pin.unlink()
    packed.write_packed_pin(pin, release, exclusive=True)
    assert pin.stat().st_nlink == 1
    assert packed.resolve_packed_pin(tmp_path / "cold", pin).manifest_sha256 == release.manifest_sha256


@pytest.mark.parametrize("field,value", [
    ("included_root_files", ["anything"]), ("excluded_subtrees", []),
    ("source_coordination_lock", "another.lock"), ("source_coordination_lock_scope", "repo"),
    ("completed_forward_key", "1" * 64), ("publication_family", "unknown"),
])
def test_catalog_family_cannot_supply_implicit_selection_or_owner(tmp_path, monkeypatch, field, value):
    fixture = _fixture(tmp_path, monkeypatch)
    catalog = fixture[3]
    data = json.loads(catalog.read_text())
    data["publication_families"][0][field] = value
    catalog.write_text(json.dumps(data))
    with pytest.raises(SnapshotError):
        releases._load_catalog(catalog)


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "fifo"])
def test_original_key_owner_must_not_be_redirected_or_recreated(tmp_path, monkeypatch, capsys, kind):
    fixture = _fixture(tmp_path, monkeypatch)
    source, _, key, _, cold, _, _ = fixture
    owner = source / f"{key}.lock"
    previous = owner.read_bytes()
    owner.unlink()
    other = source / "other-owner.lock"
    other.write_bytes(previous)
    if kind == "symlink":
        owner.symlink_to(other)
    elif kind == "hardlink":
        os.link(other, owner)
    else:
        os.mkfifo(owner)
    assert releases.main(_argv(fixture)) == 2
    assert other.read_bytes() == previous
    assert not list((cold / "manifests").glob("*/*.json"))
    assert "ERROR" in capsys.readouterr().err


def test_waiting_original_owner_replacement_fails_closed(tmp_path, monkeypatch, capsys):
    fixture = _fixture(tmp_path, monkeypatch)
    source, _, key, _, cold, _, _ = fixture
    owner = source / f"{key}.lock"
    with owner.open("r+b") as held:
        fcntl.flock(held, fcntl.LOCK_EX)

        def release_and_replace(_seconds):
            owner.unlink()
            owner.write_bytes(b"replacement owner must not be overwritten")
            fcntl.flock(held, fcntl.LOCK_UN)

        monkeypatch.setattr(releases.time, "sleep", release_and_replace)
        assert releases.main(_argv(fixture)) == 2
    assert owner.read_bytes() == b"replacement owner must not be overwritten"
    assert not list((cold / "manifests").glob("*/*.json"))
    assert "ERROR" in capsys.readouterr().err


@pytest.mark.parametrize("overlap", ["cache", "cold"])
def test_fixed_pins_cannot_be_hidden_in_cache_or_cold(tmp_path, monkeypatch, capsys, overlap):
    fixture = _fixture(tmp_path, monkeypatch)
    retention = fixture[-1]
    data = json.loads(retention.read_text())
    data["materialized_root"] = str(fixture[0 if overlap == "cache" else 4])
    retention.write_text(json.dumps(data))
    monkeypatch.setattr(releases, "publish_packed_snapshot", lambda *_a, **_kw: pytest.fail("no unsafe publication"))
    assert releases.main(_argv(fixture)) == 2
    assert "ERROR" in capsys.readouterr().err


def test_pin_parent_alias_is_not_retention_protection(tmp_path, monkeypatch, capsys):
    fixture = _fixture(tmp_path, monkeypatch)
    materialized = fixture[-2]
    outside = tmp_path / "outside-retention"
    outside.mkdir()
    (materialized / "pins").symlink_to(outside, target_is_directory=True)
    monkeypatch.setattr(releases, "publish_packed_snapshot", lambda *_a, **_kw: pytest.fail("alias is not a retention owner"))
    assert releases.main(_argv(fixture)) == 2
    assert "ERROR" in capsys.readouterr().err
    assert not list(outside.iterdir())


def test_fixed_pin_metadata_cannot_hide_different_original_bytes(tmp_path, monkeypatch):
    fixture = _fixture(tmp_path, monkeypatch)
    source, _, key, catalog, cold, _, _ = fixture
    entry = publication.select_entry(releases._load_catalog(catalog, True)[0], key)
    proof = publication.validate_source(entry, source)
    # A fully valid packed release may still contain the wrong original bytes.
    # Full checksum/CRC alone is insufficient; compare its exact logical hashes.
    wrong = tmp_path / "wrong"
    wrong.mkdir()
    for name in entry["included_root_files"]:
        (wrong / name).write_bytes(b"not the original accepted CPU output")
    fixed = packed.publish_packed_snapshot(cold, entry["dataset"], wrong,
        node_id="test-node", included_root_files=entry["included_root_files"], metadata=proof.metadata)
    pin = publication.fixed_pin_path(entry, sync_root=cold, retention_config=fixture[-1], repo_root=tmp_path)
    pin.parent.mkdir(parents=True)
    packed.write_packed_pin(pin, fixed)
    with pytest.raises(SnapshotError, match="inventory"):
        publication.reuse_fixed_pin(cold, pin, entry, proof)


def test_source_proof_stays_narrow_but_rejects_changed_committed_pair(tmp_path, monkeypatch):
    fixture = _fixture(tmp_path, monkeypatch)
    source, _, key, catalog, _, _, _ = fixture
    entry = publication.select_entry(releases._load_catalog(catalog, True)[0], key)
    proof = publication.validate_source(entry, source)
    (source / "another-unrelated-key").write_bytes(b"unrelated")
    proof.guard()
    receipt = source / f"{key}.receipt.json"
    receipt.write_bytes(receipt.read_bytes() + b"\n")
    with pytest.raises(SnapshotError, match="changed"):
        proof.guard()


@pytest.mark.parametrize("threshold", [0, -1, True, 1.5, "1", 2**63])
def test_catalog_byte_threshold_is_finite_explicit_and_not_ambiguous(tmp_path, monkeypatch, threshold):
    fixture = _fixture(tmp_path, monkeypatch)
    catalog = fixture[3]
    data = json.loads(catalog.read_text())
    data["publication_families"][0]["loose_threshold_bytes"] = threshold
    catalog.write_text(json.dumps(data))
    with pytest.raises(SnapshotError):
        releases._load_catalog(catalog)
    data["publication_families"][0].update(loose_threshold_bytes=1, loose_threshold_mib=8)
    catalog.write_text(json.dumps(data))
    with pytest.raises(SnapshotError):
        releases._load_catalog(catalog)


def test_different_keys_share_the_existing_cold_payload_object(tmp_path, monkeypatch, capsys):
    fixture = _fixture(tmp_path, monkeypatch)
    source, identity, key, _, cold, _, _ = fixture
    assert releases.main(_argv(fixture)) == 0
    first = json.loads(capsys.readouterr().out)["published"][0]
    other_identity = {**identity, "another_input_identity": True}
    original_value, _ = cached_forward(source, identity, lambda: pytest.fail("source must exist"))
    _, result = cached_forward(source, other_identity, lambda: original_value)
    other_key = identity_digest(other_identity)
    assert result["storage_build"]["reused_payload"] is True
    args = _argv(fixture)
    args[args.index("--completed-forward-key") + 1] = other_key
    assert releases.main(args) == 0
    second = json.loads(capsys.readouterr().out)["published"][0]
    left = packed.resolve_packed_pin(cold, Path(first["fixed_pin"]["path"]))
    right = packed.resolve_packed_pin(cold, Path(second["fixed_pin"]["path"]))
    digest = json.loads((source / f"{key}.receipt.json").read_text())["payload_sha256"]
    objects = [[row for row in resolved.manifest["archive"]["objects"] if row["sha256"] == digest]
               for resolved in (left, right)]
    assert all(len(rows) == 1 and rows[0]["kind"] == "blob" for rows in objects)
    assert objects[0][0]["relpath"] == objects[1][0]["relpath"]
    assert second["new_sync_objects"] == 2  # unique receipt + inventory; not another full forward
