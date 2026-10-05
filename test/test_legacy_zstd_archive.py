"""Original-byte, interruption and compressor-error boundaries for Zstd."""
from dataclasses import replace
import os
from pathlib import Path
import shutil

import pytest

from stockagent.data_sync import legacy_artifact_archive as archive
from stockagent.data_sync.desync_snapshots import SnapshotError


def fixture(tmp_path):
    root = tmp_path / "artifacts"
    source = root / "markets/example"
    source.mkdir(parents=True)
    (source / "tensor.npy").write_bytes(b"original tensor values\0" * 450_000)
    (source / "receipt.json").write_bytes(b'{"original": true}')
    for path in source.iterdir():
        os.utime(path, (1_000_000_000, 1_000_000_000))
    return archive.LegacyArchiveSpec("legacy-example", "markets/example", 7, tmp_path / "stage"), root, source


@pytest.mark.skipif(shutil.which("zstd") is None, reason="zstd CLI unavailable")
@pytest.mark.parametrize("profile", [archive.ZSTD_COMPRESSION_PROFILE, archive.ZSTD3_COMPRESSION_PROFILE])
def test_original_roundtrip_and_corrupt_frame(tmp_path, profile):
    spec, root, source = fixture(tmp_path)
    spec = replace(spec, compression_profile=profile)
    cold = tmp_path / "cold"
    archive.publish_archive(spec, root, cold, repo_root=tmp_path)
    destination = tmp_path / "restored"
    archive.restore_archive(spec, cold, destination, materialized_root=tmp_path / "restore-cache")
    original = source / "tensor.npy"
    assert (destination / "tensor.npy").read_bytes() == original.read_bytes()
    assert (destination / "tensor.npy").stat().st_mtime_ns == original.stat().st_mtime_ns
    stage = spec.stage_root / spec.dataset / "archive"
    manifest = archive.verify_archive_directory(stage)["manifest"]
    member = next(r for r in manifest["files"] if r["path"] == "tensor.npy")
    assert member["codec"] == "zstd"
    encoded = stage / member["encoded_path"]
    assert archive._decode_hash(encoded, "zstd")[1] == original.stat().st_size
    encoded.write_bytes(encoded.read_bytes()[:-3])
    with pytest.raises(SnapshotError, match="decode failed"):
        archive._decode_hash(encoded, "zstd")


@pytest.mark.skipif(shutil.which("zstd") is None, reason="zstd CLI unavailable")
def test_profile_change_resumes_existing_encoding(tmp_path):
    spec, root, source = fixture(tmp_path)
    spec = replace(spec, compression_profile=archive.ADAPTIVE_COMPRESSION_PROFILE)
    old = archive.prepare_archive(spec, root)
    spec = replace(spec, compression_profile=archive.ZSTD_COMPRESSION_PROFILE)
    resumed = archive.prepare_archive(spec, root)
    assert resumed["files"] == old["files"]
    assert archive.verify_archive_directory(spec.stage_root / spec.dataset / "archive", source)["files"] == 2


def test_unknown_encoding_rejected_before_output(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"original")
    target = tmp_path / "encoded"
    with pytest.raises(SnapshotError, match="parameters"):
        archive._encode_one(source, target, "unknown")
    assert not target.exists()


@pytest.mark.skipif(shutil.which("zstd") is None, reason="zstd CLI unavailable")
def test_zstd_encoding_deterministic(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"fixed source values\0" * 450_000)
    first, second = tmp_path / "first", tmp_path / "second"
    assert archive._encode_one(source, first, "zstd") == archive._encode_one(source, second, "zstd")
    assert first.read_bytes() == second.read_bytes()
