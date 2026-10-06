import json
import os
import shutil
from pathlib import Path
import pytest

from stockagent.data_sync.immutable_replication import digest, replicate, seal, verify


@pytest.fixture
def sealed(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "觀測.parquet").write_bytes(b"real test contract bytes")
    (source / "nested").mkdir()
    (source / "nested" / "provenance.json").write_text('{"date":"2026-10-05"}')
    seal(source, {"kind": "engineering_contract_fixture"})
    return source


@pytest.fixture
def rclone():
    selected = os.environ.get("STOCKAGENT_TEST_RCLONE") or shutil.which("rclone")
    if not selected:
        pytest.skip("the real rclone integration needs STOCKAGENT_TEST_RCLONE or PATH")
    path = Path(selected)
    assert path.is_file()
    return path


def test_real_copy_retry_noop_and_incomplete_resume(sealed, tmp_path, rclone):
    destination = tmp_path / "destination"
    first = replicate(sealed, destination, rclone)
    before = {p.relative_to(destination).as_posix(): digest(p) for p in destination.rglob("*") if p.is_file()}
    second = replicate(sealed, destination, rclone)
    assert first["delivery_identity_sha256"] == second["delivery_identity_sha256"]
    assert first["command_exit_codes"] == [0, 0]
    assert verify(destination) == verify(sealed)
    (destination / "READY").unlink()
    replicate(sealed, destination, rclone)
    assert {p.relative_to(destination).as_posix(): digest(p) for p in destination.rglob("*") if p.is_file()} == before


def test_same_length_conflict_refuses_before_writing(sealed, tmp_path, rclone):
    destination = tmp_path / "destination"
    destination.mkdir()
    original = (sealed / "觀測.parquet").read_bytes()
    (destination / "觀測.parquet").write_bytes(b"x" * len(original))
    with pytest.raises(ValueError, match="conflicting"):
        replicate(sealed, destination, rclone)
    assert not (destination / "manifest.json").exists()
    assert (destination / "觀測.parquet").read_bytes() == b"x" * len(original)


def test_source_mutation_and_extra_file_block_ready(sealed):
    (sealed / "nested/provenance.json").write_text("changed")
    with pytest.raises(ValueError, match="full SHA"):
        verify(sealed)


def test_unknown_destination_member_is_retained(sealed, tmp_path, rclone):
    destination = tmp_path / "destination"
    destination.mkdir()
    (destination / "unknown").write_bytes(b"unique retained evidence")
    with pytest.raises(ValueError, match="unexpected"):
        replicate(sealed, destination, rclone)
    assert (destination / "unknown").exists()


def test_symlink_and_wrong_ready_are_rejected(sealed, tmp_path):
    (sealed / "READY").write_text("0" * 64 + "\n")
    with pytest.raises(ValueError, match="READY"):
        verify(sealed)
    (sealed / "evil").symlink_to(tmp_path)
    with pytest.raises(ValueError, match="redirected"):
        verify(sealed)
