from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import urllib.parse
import json

import pytest

import stockagent.data_sync.syncthing_scan as scan
from stockagent.data_sync.desync_snapshots import SnapshotError


class _Response:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


def test_d_primary_publish_scans_objects_before_manifest_and_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    packed = tmp_path / "packed"
    packed.mkdir()
    assert scan.scan_after_publish(packed, "dataset") is False
    (packed / scan.D_PRIMARY_MARKER).write_text("{}")
    config = tmp_path / "config.xml"
    config.write_text(
        '<configuration><folder id="stockagent-packed" path="'
        + str(packed)
        + '" paused="false"/><gui><address>127.0.0.1:8384</address>'
        "<apikey>test-key</apikey></gui></configuration>"
    )
    monkeypatch.setattr(scan, "CANONICAL_ROOT", packed)
    monkeypatch.setattr(scan, "CONFIGS", (config,))
    monkeypatch.setattr(
        scan.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stderr=""),
    )
    scanned: list[str] = []

    def urlopen(request, *, timeout):
        assert timeout == 120
        assert request.get_header("X-api-key") == "test-key"
        scanned.append(
            urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)["sub"][
                0
            ]
        )
        return _Response()

    monkeypatch.setattr(scan.urllib.request, "urlopen", urlopen)
    assert scan.scan_after_publish(
        packed, "dataset", new_object_paths=("objects/blobs/aa/a.blob",)
    )
    assert scanned == ["objects/blobs/aa/a.blob", "manifests/dataset", "heads/dataset"]
    scanned.clear()
    assert scan.scan_after_publish(packed, "dataset", retry_full=True) is False
    pending = packed / ".local-state/scan-pending/dataset.json"
    pending.parent.mkdir(parents=True, exist_ok=True)
    pending.write_text("{}")
    assert scan.scan_after_publish(packed, "dataset", retry_full=True)
    assert scanned == ["objects", "manifests/dataset", "heads/dataset"]
    assert not pending.exists()

    monkeypatch.setattr(
        scan.urllib.request,
        "urlopen",
        lambda request, *, timeout: (_ for _ in ()).throw(OSError("API unavailable")),
    )
    with pytest.raises(SnapshotError, match="API unavailable"):
        scan.scan_after_publish(
            packed, "dataset", new_object_paths=("objects/packs/a.zip",)
        )
    assert pending.is_file()


def test_d_primary_retry_uses_recorded_paths_and_merges_later_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    packed = tmp_path / "packed"
    packed.mkdir()
    (packed / scan.D_PRIMARY_MARKER).write_text("{}")
    config = tmp_path / "config.xml"
    config.write_text(
        '<configuration><folder id="stockagent-packed" path="'
        + str(packed)
        + '" paused="false"/><gui><address>127.0.0.1:8384</address>'
        '<apikey>test-key</apikey></gui></configuration>'
    )
    monkeypatch.setattr(scan, "CANONICAL_ROOT", packed)
    monkeypatch.setattr(scan, "CONFIGS", (config,))
    monkeypatch.setattr(
        scan.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stderr=""),
    )
    pending = packed / ".local-state/scan-pending/dataset.json"
    pending.parent.mkdir(parents=True)
    pending.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "dataset": "dataset",
                "new_object_paths": ["objects/blobs/aa/old.blob"],
            }
        )
    )
    scanned: list[str] = []

    def urlopen(request, *, timeout):
        assert timeout == 120
        scanned.append(
            urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)["sub"][
                0
            ]
        )
        return _Response()

    monkeypatch.setattr(scan.urllib.request, "urlopen", urlopen)
    assert scan.scan_after_publish(
        packed, "dataset", new_object_paths=("objects/blobs/bb/new.blob",)
    )
    assert scanned == [
        "objects/blobs/aa/old.blob",
        "objects/blobs/bb/new.blob",
        "manifests/dataset",
        "heads/dataset",
    ]
    assert not pending.exists()
    pending.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "dataset": "dataset",
                "new_object_paths": ["objects/blobs/cc/retry.blob"],
            }
        )
    )
    scanned.clear()
    assert scan.scan_after_publish(packed, "dataset", retry_full=True)
    assert scanned == [
        "objects/blobs/cc/retry.blob",
        "manifests/dataset",
        "heads/dataset",
    ]
    assert not pending.exists()


def test_d_primary_scan_fails_closed_when_mount_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    packed = tmp_path / "packed"
    packed.mkdir()
    (packed / scan.D_PRIMARY_MARKER).write_text("{}")
    monkeypatch.setattr(scan, "CANONICAL_ROOT", packed)
    monkeypatch.setattr(
        scan.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=2, stderr="D missing"),
    )
    with pytest.raises(SnapshotError, match="D missing"):
        scan.scan_after_publish(packed, "dataset")
