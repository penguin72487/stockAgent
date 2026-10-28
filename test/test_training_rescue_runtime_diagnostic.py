"""Original metadata evidence must never turn duplicate selection into a pass."""
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import zipfile

import pytest

from scripts import diagnose_training_rescue_runtime as diagnostic
from stockagent.training_rescue_worker import CONTRACT, digest


def test_original_wheel_is_a_real_unfiltered_metadata_location_even_in_CAS(tmp_path):
    source = tmp_path / "source"
    package = source / "stockagent"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    original = Path(__file__).resolve().parents[1] / "stockagent/runtime_identity.py"
    (package / "runtime_identity.py").write_bytes(original.read_bytes())
    wheel = tmp_path / ("a" * 64)  # The actual restore stores wheel bytes in a suffixless CAS.
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("stockagent-0.1.0.dist-info/METADATA", "Metadata-Version: 2.1\nName: stockagent\nVersion: 0.1.0\n")
        archive.writestr("stockagent-0.1.0.dist-info/WHEEL", "Wheel-Version: 1.0\nTag: py3-none-any\n")
    before = wheel.read_bytes()
    result = subprocess.run([sys.executable, "-B", "-c", diagnostic.PROBE, str(source), str(wheel)],
                            cwd=tmp_path, capture_output=True, text=True, check=True)
    value = json.loads(result.stdout)
    assert any(row["name"] == "stockagent" and row["version"] == "0.1.0"
               and str(wheel) in row["metadata_location"] for row in value["distributions_in_observed_order"])
    assert value["torch_imported"] is False
    assert wheel.read_bytes() == before


def runtime(tmp_path):
    prefix = tmp_path / "runtime"
    prefix.mkdir()
    path = prefix / "metadata"
    path.write_bytes(b"Name: example\nVersion: 1\n")
    row = {"relative": "metadata", "kind": "file", "bytes": path.stat().st_size,
           "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    proof = {"kind": "fixed_prefix_runtime", "prefix": str(prefix), "files": [row],
             "archive_sha256": "a" * 64, "archive_bytes": 20}
    return prefix, proof


@pytest.mark.parametrize("damage", ["file", "unknown", "duplicate", "redirected", "marker"])
def test_runtime_rejects_drift_and_unknown_material(tmp_path, damage):
    prefix, proof = runtime(tmp_path)
    if damage == "file":
        (prefix / "metadata").write_bytes(b"different")
    elif damage == "unknown":
        (prefix / "unregistered.dist-info").mkdir()
        (prefix / "unregistered.dist-info/METADATA").write_text("Version: 9")
    elif damage == "duplicate":
        proof["files"].append(copy.deepcopy(proof["files"][0]))
    elif damage == "redirected":
        (prefix / "metadata").unlink()
        (prefix / "metadata").symlink_to(tmp_path / "foreign")
    else:
        (prefix / ".stockagent-rescue-runtime.json").write_text('{"archive_sha256":"foreign"}')
    with pytest.raises((ValueError, OSError)):
        diagnostic.runtime_files(prefix, proof)


def test_runtime_original_restore_marker_and_compiler_cache_do_not_rewrite_prefix(tmp_path):
    prefix, proof = runtime(tmp_path)
    (prefix / ".stockagent-rescue-runtime.json").write_text(json.dumps({"archive_sha256": "a" * 64}))
    (prefix / "__pycache__").mkdir()
    (prefix / "__pycache__/metadata.pyc").write_bytes(b"compiled cache excluded by original capsule")
    before = {p: p.read_bytes() for p in prefix.rglob("*") if p.is_file()}
    observed = diagnostic.runtime_files(prefix, proof)
    assert prefix / "metadata" in observed
    assert before == {p: p.read_bytes() for p in prefix.rglob("*") if p.is_file()}


def fixture(tmp_path, monkeypatch):
    restored = tmp_path / "restored"
    restored.mkdir()
    prefix, proof = runtime(tmp_path)
    source = tmp_path / "source"
    source.mkdir()
    body = {"schema_version": 1, "python": "3.12.15", "implementation": "CPython", "machine": "x86_64",
            "system": "Linux", "packages": {"example": "1", "stockagent": "0.1.0"},
            "duplicate_distribution_versions": {"example": ["1", "2"]}}
    lock = {**body, "sha256": digest(body)}
    spec = {"contract": CONTRACT, "schema_version": 1, "run_id": "fixture", "source_root": str(source),
            "output_root": str(tmp_path / "artifacts/markets/fixture"), "config_file": str(tmp_path / "config.yaml"),
            "runtime_prefix": str(prefix), "world_size": 1, "global_batch": 4, "environment": {},
            "data_sources": [], "rebuild_steps": [], "runtime_lock": lock}
    spec["train_argv"] = ["train.py", "--config", spec["config_file"], "--output-dir", spec["output_root"], "--resume"]
    def put(raw):
        sha = hashlib.sha256(raw).hexdigest()
        row = {"relative": f"objects/{sha[:2]}/{sha}", "sha256": sha, "bytes": len(raw)}
        path = restored / row["relative"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        return row
    def encode(value):
        return (json.dumps(value, sort_keys=True) + "\n").encode()
    bindings = {"spec": put(encode(spec)), "runtime_proof": put(encode(proof)), "source_wheel": put(b"pinned wheel")}
    boot = {"runtime_archive": {"sha256": proof["archive_sha256"], "bytes": proof["archive_bytes"]},
            "runtime_archive_proof": bindings["runtime_proof"]}
    bindings["bootstrap"] = put(encode(boot))
    bindings["source_receipt"] = put(encode({"wheel": bindings["source_wheel"]}))
    contract = {"contract": diagnostic.CONTRACT, "run_id": "fixture", "original_spec_sha256": digest(spec),
                "original_bootstrap_id": digest(boot), "original_runtime_lock_sha256": lock["sha256"],
                "bindings": bindings}
    contract["identity_sha256"] = digest(contract)
    monkeypatch.setattr(diagnostic, "validate_bootstrap_binding", lambda *a, **k: None)
    monkeypatch.setattr(diagnostic, "verify_source_release", lambda *a, **k: {"source_files_verified": True})
    return restored, contract, body


@pytest.mark.parametrize("difference", [None, "duplicate", "missing", "new_package"])
def test_complete_observation_preserves_every_mismatch_and_strict_result(tmp_path, monkeypatch, difference):
    restored, contract, body = fixture(tmp_path, monkeypatch)
    actual = copy.deepcopy(body)
    if difference == "duplicate":
        actual["packages"]["example"] = "2"
    elif difference == "missing":
        del actual["packages"]["stockagent"]
    elif difference == "new_package":
        actual["packages"]["foreign"] = "9"
    actual["sha256"] = digest(actual)
    def observe(command, **kwargs):
        assert command[1:4] == ["-s", "-B", "-c"]
        assert kwargs["env"]["PYTHONNOUSERSITE"] == "1"
        assert "PYTHONPATH" not in kwargs["env"]
        assert "objects/" in command[-1]
        return SimpleNamespace(stdout=json.dumps({"runtime": actual, "distributions_in_observed_order": ["all entries"],
                                                "torch_imported": False}))
    monkeypatch.setattr(diagnostic.subprocess, "run", observe)
    result = diagnostic.diagnose(contract, contract["identity_sha256"], [restored])
    assert result["strict_original_runtime_lock_verified"] is (difference is None)
    assert bool(result["runtime_lock_differences"]) is (difference is not None)
    assert result["metadata_observation"]["runtime"] == actual
    assert result["forced_distribution_version_selection"] is False
    assert result["distribution_filtering_performed"] is False
    assert result["resume_qualified"] is False and result["packages_installed_or_removed"] == 0


def test_mid_probe_runtime_change_never_becomes_acceptance(tmp_path, monkeypatch):
    restored, contract, body = fixture(tmp_path, monkeypatch)
    def changed(*a, **kwargs):
        (tmp_path / "runtime/metadata").write_bytes(b"replaced after audit")
        return SimpleNamespace(stdout=json.dumps({"runtime": {**body, "sha256": digest(body)}, "torch_imported": False}))
    monkeypatch.setattr(diagnostic.subprocess, "run", changed)
    with pytest.raises(ValueError, match="changed during metadata"):
        diagnostic.diagnose(contract, contract["identity_sha256"], [restored])


def test_contract_must_be_pinned_before_any_probe(tmp_path, monkeypatch):
    restored, contract, _ = fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(diagnostic.subprocess, "run", lambda *a, **k: pytest.fail("no execution on unpinned contract"))
    contract["original_spec_sha256"] = "f" * 64
    with pytest.raises(ValueError, match="contract identity"):
        diagnostic.diagnose(contract, contract["identity_sha256"], [restored])


def test_duplicate_or_redirected_restored_member_is_never_selected(tmp_path):
    roots = [tmp_path / "one", tmp_path / "two"]
    raw = b"original"
    row = {"relative": "original.json", "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
    for root in roots:
        root.mkdir()
        (root / "original.json").write_bytes(raw)
    with pytest.raises(ValueError, match="exactly one"):
        diagnostic.read_bound(roots, row)
    (roots[1] / "original.json").unlink()
    (roots[1] / "original.json").symlink_to(roots[0] / "original.json")
    with pytest.raises(ValueError, match="symlink"):
        diagnostic.read_bound(roots, row)
