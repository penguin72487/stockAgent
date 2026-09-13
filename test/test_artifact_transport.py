from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from stockagent.data_sync import artifact_transport as transport
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def setup(tmp_path, monkeypatch):
    config = json.loads(Path("configs/data_sync/artifact_ingress.json").read_text())
    config.update(
        transport_root=str(tmp_path / "transport"), state_root=str(tmp_path / "state")
    )
    config["artifact"].update(
        relative_root="markets/selected", pack_buckets=2, loose_file_threshold_bytes=16
    )
    artifact_root = tmp_path / "source-artifacts"
    source = artifact_root / config["artifact"]["relative_root"]
    (source / "fold_11").mkdir(parents=True)
    (source / config["checkpoint"]).write_bytes(b"checkpoint")
    (source / "a.json").write_text('{"complete":true}')
    (source / "big.dat").write_bytes(b"x" * 64)
    (source / "big_duplicate.dat").write_bytes(b"x" * 64)
    (source / "another.dat").write_bytes(b"y" * 64)
    config["checkpoint_sha256"] = hashlib.sha256(b"checkpoint").hexdigest()
    monkeypatch.setattr(
        transport, "validate_cold_artifact_source", lambda *_: {"ok": True}
    )
    monkeypatch.setattr(transport, "artifact_process_references", lambda *_: [])
    return config, artifact_root, source


def _manifest(config, package):
    path = Path(config["transport_root"]) / "packages" / f"{package['package_id']}.json"
    return json.loads(path.read_text())


def _alter(config, package, change):
    manifest = _manifest(config, package)
    change(manifest)
    payload = transport._canonical_json_bytes(manifest)
    digest = hashlib.sha256(payload).hexdigest()
    (Path(config["transport_root"]) / "packages" / f"{digest}.json").write_bytes(
        payload
    )
    return digest


def test_roundtrip_idempotent_dedup_and_no_live_or_cold_writes(setup):
    config, artifact_root, source = setup
    first = transport.build_package(config, artifact_root)
    again = transport.build_package(config, artifact_root)
    assert first == again
    manifest = _manifest(config, first)
    for obj in manifest["objects"]:
        path = Path(config["transport_root"]) / obj["path"]
        assert path.stat().st_mode & 0o777 == 0o644
    assert len([obj for obj in manifest["objects"] if obj["kind"] == "blob"]) == 3
    # a.json is 17 bytes; duplicate big files share one blob.
    received = transport.receive_package(config, first["package_id"])
    destination = Path(received["artifact_root"]) / config["artifact"]["relative_root"]
    assert (destination / "big.dat").read_bytes() == (source / "big.dat").read_bytes()
    assert received["status"] == "verified_quarantine"
    assert not received["cold_published"] and not received["model_activated"]
    assert transport.receive_package(config, first["package_id"]) == received
    assert not (Path(config["transport_root"]) / "heads").exists()
    assert not (Path(config["transport_root"]) / ".local-state").exists()


@pytest.mark.parametrize(
    "reason", ["checkpoint", "active", "incomplete", "symlink", "bounds"]
)
def test_build_fails_closed(setup, monkeypatch, reason):
    config, artifact_root, source = setup
    if reason == "checkpoint":
        (source / config["checkpoint"]).write_bytes(b"different model")
    elif reason == "active":
        monkeypatch.setattr(
            transport, "artifact_process_references", lambda *_: ["pid=123"]
        )
    elif reason == "incomplete":

        def fail(*_):
            raise SnapshotError("not complete")

        monkeypatch.setattr(transport, "validate_cold_artifact_source", fail)
    elif reason == "symlink":
        (source / "alias").symlink_to("big.dat")
    else:
        config["maximum_logical_bytes"] = 10
    with pytest.raises(SnapshotError):
        transport.build_package(config, artifact_root)
    assert not (Path(config["transport_root"]) / "packages").exists()


def test_source_change_during_pack_does_not_commit(setup, monkeypatch):
    config, artifact_root, source = setup
    original = transport._write_pack

    def write(*args, **kwargs):
        original(*args, **kwargs)
        (source / "changed.txt").write_text("changed")

    monkeypatch.setattr(transport, "_write_pack", write)
    with pytest.raises(SnapshotError, match="changed"):
        transport.build_package(config, artifact_root)
    assert not (Path(config["transport_root"]) / "packages").exists()


@pytest.mark.parametrize(
    "mutation",
    [
        "traversal",
        "duplicate",
        "symlink",
        "scope",
        "mode",
        "size",
        "storage",
        "objects",
        "parent",
    ],
)
def test_receive_rejects_unsafe_package(setup, mutation):
    config, artifact_root, _ = setup
    package = transport.build_package(config, artifact_root)

    def change(manifest):
        row = next(row for row in manifest["entries"] if row["kind"] == "file")
        if mutation == "traversal":
            row["path"] = "../escape"
        elif mutation == "duplicate":
            manifest["entries"].append(row.copy())
        elif mutation == "symlink":
            row["kind"] = "symlink"
        elif mutation == "scope":
            manifest["scope"]["artifact"]["relative_root"] = "markets/not-authorized"
        elif mutation == "mode":
            row["mode"] = 0o4755
        elif mutation == "size":
            row["size"] = 9999
        elif mutation == "storage":
            row["storage"]["object"] = "0" * 64
        elif mutation == "objects":
            manifest["objects"][0]["bytes"] = 10**20
        elif mutation == "parent":
            row["path"] = "nonexistent/child"

    digest = _alter(config, package, change)
    with pytest.raises(SnapshotError):
        transport.receive_package(config, digest)
    assert not (Path(config["state_root"]) / "accepted").exists()


@pytest.mark.parametrize("failure", ["missing", "corrupt", "symlink", "manifest"])
def test_partial_or_corrupt_syncthing_arrival_is_not_ready(setup, failure):
    config, artifact_root, _ = setup
    package = transport.build_package(config, artifact_root)
    manifest = _manifest(config, package)
    obj = Path(config["transport_root"]) / manifest["objects"][0]["path"]
    if failure == "missing":
        obj.unlink()
    elif failure == "corrupt":
        obj.write_bytes(b"corrupt")
    elif failure == "symlink":
        original = obj.with_suffix(".outside")
        obj.rename(original)
        obj.symlink_to(original)
    else:
        (
            Path(config["transport_root"])
            / "packages"
            / f"{package['package_id']}.json"
        ).write_text("{}")
    with pytest.raises((SnapshotError, FileNotFoundError)):
        transport.receive_package(config, package["package_id"])
    assert not (Path(config["state_root"]) / "accepted").exists()


def test_existing_quarantine_is_never_silently_overwritten(setup):
    config, artifact_root, _ = setup
    package = transport.build_package(config, artifact_root)
    received = transport.receive_package(config, package["package_id"])
    checkpoint = (
        Path(received["artifact_root"])
        / config["artifact"]["relative_root"]
        / config["checkpoint"]
    )
    checkpoint.write_bytes(b"user edit")
    with pytest.raises(SnapshotError):
        transport.receive_package(config, package["package_id"])
    assert checkpoint.read_bytes() == b"user edit"


@pytest.mark.parametrize(
    "path", ["/srv/stockagent-packed", "/srv/stockagent-artifacts-hot", "/srv", "/"]
)
def test_canonical_namespace_is_never_transport_target(setup, path):
    config, artifact_root, _ = setup
    config["transport_root"] = path
    with pytest.raises(SnapshotError):
        transport.build_package(config, artifact_root)
