import os
from pathlib import Path

import pytest

from downloader import artifact_io


def test_private_atomic_publication_is_restricted_before_secret_bytes_move(tmp_path, monkeypatch):
    target = tmp_path / "private.env"
    target.write_text("previous\n")
    target.chmod(0o600)
    replace = os.replace
    observations = []
    def inspect(source, destination):
        observations.append(Path(source).stat().st_mode & 0o777)
        assert Path(source).read_text() == "PRIVATE=fake-test-value\n"
        replace(source, destination)
    monkeypatch.setattr(artifact_io.os, "replace", inspect)
    artifact_io.atomic_write_text(target, "PRIVATE=fake-test-value\n", durable=True, mode=0o600)
    assert observations == [0o600]
    assert target.stat().st_mode & 0o777 == 0o600


def test_private_collision_cannot_overwrite_or_remove_another_owned_temporary(tmp_path):
    target = tmp_path / "private.env"
    target.write_text("original\n")
    temporary = artifact_io._temporary_path(target)
    temporary.write_text("other-writer-or-retained-failure\n")
    with pytest.raises(FileExistsError):
        artifact_io.atomic_write_text(target, "new\n", mode=0o600)
    assert target.read_text() == "original\n"
    assert temporary.read_text() == "other-writer-or-retained-failure\n"


def test_unspecified_mode_preserves_the_existing_artifact_interface(tmp_path):
    target = tmp_path / "public.txt"
    artifact_io.atomic_write_text(target, "plain artifact\n", durable=True)
    assert target.read_text() == "plain artifact\n"
    assert not artifact_io._temporary_path(target).exists()
