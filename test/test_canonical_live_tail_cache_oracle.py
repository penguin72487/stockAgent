"""Original CPU references must not execute unrelated saved top-level code."""
import json
from pathlib import Path

import pytest

from downloader.artifact_io import sha256_file
from scripts import canonical_live_tail_cache_oracle as oracle


def receipt(tmp_path, monkeypatch):
    monkeypatch.setattr(oracle, "ROOT", tmp_path)
    folder = tmp_path / "artifacts/benchmarks/original-callers"
    folder.mkdir(parents=True)
    proofs = []
    for index, source in enumerate(oracle.SOURCES):
        path = folder / f"saved-{index}.py"
        path.write_text("def original(value):\n    return value + 1\n")
        path.chmod(0o400)
        proofs.append({"source": str(tmp_path / source), "path": str(path),
                       "sha256": sha256_file(path), "bytes": path.stat().st_size})
    target = folder / "receipt.json"
    target.write_text(json.dumps({"contract": "canonical_live_tail_callers_original_v1",
                                 "status": "frozen", "proofs": proofs}))
    return target, proofs


def test_exact_private_snapshot_receipt(tmp_path, monkeypatch):
    path, proofs = receipt(tmp_path, monkeypatch)
    value = oracle.original_callers_identity(path)
    assert value["receipt_sha256"] == sha256_file(path)
    assert {p["source"] for p in proofs} == set(value["proofs"])


@pytest.mark.parametrize("damage", ["mutable", "bytes", "hash", "source", "duplicate", "status"])
def test_snapshot_cannot_hide_changed_code_or_owner(tmp_path, monkeypatch, damage):
    path, proofs = receipt(tmp_path, monkeypatch)
    value = json.loads(path.read_text())
    if damage == "mutable":
        Path(proofs[0]["path"]).chmod(0o600)
    elif damage == "bytes":
        value["proofs"][0]["bytes"] += 1
    elif damage == "hash":
        value["proofs"][0]["sha256"] = "0" * 64
    elif damage == "source":
        value["proofs"][0]["source"] += ".unrecognized"
    elif damage == "duplicate":
        value["proofs"][1] = value["proofs"][0]
    else:
        value["status"] = "running"
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="original caller"):
        oracle.original_callers_identity(path)


def test_mechanically_restore_nested_function_without_executing_module(tmp_path):
    path = tmp_path / "original.py"
    path.write_text("raise RuntimeError('unrelated top-level code must not run')\n"
                    "def original(value):\n    def nested(v):\n        return v * 2\n"
                    "    return nested(value) + offset\n")
    namespace = oracle._function_namespace(path, {"offset": 3}, ("original",))
    assert namespace["original"](5) == 13
    assert namespace["original"].__code__.co_filename == str(path)


@pytest.mark.parametrize("code", ["def other():\n    pass\n", "@decorator\ndef original():\n    pass\n"])
def test_unknown_or_decorated_reference_fails_closed(tmp_path, code):
    path = tmp_path / "original.py"
    path.write_text(code)
    with pytest.raises(ValueError, match="saved original"):
        oracle._function_namespace(path, {}, ("original",))
