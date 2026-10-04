"""Retained release extraction cannot escape or admit unrecorded source bytes."""
import hashlib
import json
import zipfile

import pytest

from scripts.accept_project_release import extract_recorded_sources
from stockagent.runtime_identity import identity_sha256


def _receipt(tmp_path, entries, *, listed=None):
    bundle = tmp_path / "source.zip"
    with zipfile.ZipFile(bundle, "w") as archive:
        for name, body in entries:
            archive.writestr(name, body)
    wheel = tmp_path / "source.whl"
    wheel.write_bytes(b"retained wheel fixture")
    files = listed or {name: hashlib.sha256(body).hexdigest() for name, body in entries}
    receipt = tmp_path / "release.json"
    receipt.write_text(json.dumps({
        "schema_version": 1, "state": "built",
        "code": {"files": files, "source_sha256": identity_sha256(files)},
        "source_bundle": {"file": bundle.name, "files": files,
                          "sha256": hashlib.sha256(bundle.read_bytes()).hexdigest(),
                          "bytes": bundle.stat().st_size},
        "wheel": {"file": wheel.name, "sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
                  "bytes": wheel.stat().st_size},
    }))
    return receipt


def test_release_extracts_only_recorded_regular_sources(tmp_path):
    receipt = _receipt(tmp_path, [("stockagent/module.py", b"value=1\n")])
    destination = tmp_path / "isolated"
    extract_recorded_sources(receipt, destination)
    assert (destination / "stockagent/module.py").read_bytes() == b"value=1\n"


def test_release_rejects_escape_before_writing_any_source(tmp_path):
    receipt = _receipt(tmp_path, [
        ("stockagent/module.py", b"value=1\n"), ("../escape.py", b"wrong\n"),
    ])
    destination = tmp_path / "isolated"
    with pytest.raises(ValueError, match="invalid source ZIP entry"):
        extract_recorded_sources(receipt, destination)
    assert not destination.exists()
    assert not (tmp_path / "escape.py").exists()


def test_release_rejects_source_bytes_that_differ_from_receipt(tmp_path):
    receipt = _receipt(tmp_path, [("stockagent/module.py", b"changed\n")], listed={
        "stockagent/module.py": hashlib.sha256(b"original\n").hexdigest(),
    })
    with pytest.raises(ValueError, match="source ZIP file checksum mismatch"):
        extract_recorded_sources(receipt, tmp_path / "isolated")
