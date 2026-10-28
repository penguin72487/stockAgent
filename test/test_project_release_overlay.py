import hashlib
import json
from pathlib import Path
import zipfile

import pytest

from scripts import build_project_release as builder
from stockagent.runtime_identity import identity_sha256, verify_release_bundles, verify_source_release


def _base_release(tmp_path):
    release = tmp_path / "base"
    source = release / "build-source"
    files = {
        "stockagent/training/lifecycle.py": b"BASE_LIFECYCLE = True\n",
        "stockagent/data/example.py": b"VALUE = 'base'\n",
        "configs/example.yaml": b"data: original\n",
    }
    for name, content in files.items():
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    hashes = {name: hashlib.sha256(content).hexdigest() for name, content in files.items()}
    code = {name: digest for name, digest in hashes.items() if name.endswith(".py")}
    for name in ("old.whl", "source.zip"):
        (release / name).write_bytes(name.encode())
    def artifact(name):
        body = (release / name).read_bytes()
        return {"file": name, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
    receipt = release / "release.json"
    receipt.write_text(json.dumps({"schema_version": 1, "state": "built",
        "code": {"git_head": "base", "files": code, "source_sha256": identity_sha256(code)},
        "wheel": artifact("old.whl"), "source_bundle": {**artifact("source.zip"), "files": hashes}}))
    return receipt


def _fake_wheel(argv, *, cwd, **_kwargs):
    wheel = Path(argv[argv.index("--out-dir") + 1]) / "candidate.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        for file in Path(cwd).rglob("*.py"):
            archive.write(file, file.relative_to(cwd).as_posix())


def test_overlay_preserves_base_and_excludes_unselected_workspace_changes(tmp_path, monkeypatch):
    receipt = _base_release(tmp_path)
    replacement = tmp_path / "overrides/stockagent/data/example.py"
    replacement.parent.mkdir(parents=True)
    replacement.write_text("VALUE = 'candidate'\n")
    (replacement.parent / "unselected.py").write_text("SHOULD_NOT_BE_PROMOTED = True\n")
    monkeypatch.setattr(builder, "runtime_identity", lambda: {"sha256": "runtime"})
    monkeypatch.setattr(builder.subprocess, "run", _fake_wheel)
    result = builder.build_overlay_release(receipt, replacement.parents[2], tmp_path / "out",
        overrides=("stockagent/data/example.py",), uv="uv")
    output = Path(result["receipt"])
    tree = output.parent / "build-source"
    assert (tree / "stockagent/data/example.py").read_text() == "VALUE = 'candidate'\n"
    assert (tree / "configs/example.yaml").read_text() == "data: original\n"
    assert not (tree / "stockagent/data/unselected.py").exists()
    assert (receipt.parent / "build-source/stockagent/data/example.py").read_text() == "VALUE = 'base'\n"
    proof = verify_source_release(output, tree)
    assert proof["source_sha256"] == result["source_sha256"]
    assert verify_release_bundles(output)["bundles_verified"]
    manifest = json.loads(output.read_text())
    assert manifest["source_overlay"]["unselected_workspace_changes_promoted"] is False
    (tree / "stockagent/data/example.py").write_text("tampered\n")
    with pytest.raises(ValueError, match="source mismatch"):
        verify_source_release(output, tree)


@pytest.mark.parametrize("names", [("../../secret",), ("stockagent/data/example.py",) * 2,
                                    ("configs/example.yaml",)])
def test_overlay_rejects_paths_outside_unique_existing_code(tmp_path, names):
    receipt = _base_release(tmp_path)
    with pytest.raises(ValueError, match="unique existing frozen code"):
        builder.build_overlay_release(receipt, tmp_path, tmp_path / "out", overrides=names, uv="uv")


def test_overlay_adds_only_explicit_new_module_to_frozen_bundle(tmp_path, monkeypatch):
    receipt = _base_release(tmp_path)
    override_root = tmp_path / "overrides"
    addition = override_root / "stockagent/data/new.py"
    addition.parent.mkdir(parents=True)
    addition.write_text("VALUE = 'added'\n")
    (addition.parent / "unselected.py").write_text("SHOULD_NOT_BE_PROMOTED = True\n")
    monkeypatch.setattr(builder, "runtime_identity", lambda: {"sha256": "runtime"})
    monkeypatch.setattr(builder.subprocess, "run", _fake_wheel)
    result = builder.build_overlay_release(receipt, override_root, tmp_path / "out", overrides=(),
        additions=("stockagent/data/new.py",), uv="uv")
    output = Path(result["receipt"])
    tree = output.parent / "build-source"
    assert (tree / "stockagent/data/new.py").read_bytes() == addition.read_bytes()
    assert not (tree / "stockagent/data/unselected.py").exists()
    manifest = json.loads(output.read_text())
    digest = hashlib.sha256(addition.read_bytes()).hexdigest()
    assert manifest["source_overlay"]["additions"] == {"stockagent/data/new.py": digest}
    assert manifest["code"]["files"]["stockagent/data/new.py"] == digest
    assert verify_source_release(output, tree)["source_sha256"] == result["source_sha256"]
    assert verify_release_bundles(output)["bundles_verified"]


@pytest.mark.parametrize("additions", [
    ("stockagent/data/example.py",),
    ("stockagent/data/new.py", "stockagent/data/new.py"),
    ("stockagent/../secret.py",),
    ("stockagent/.private.py",),
    ("configs/private.py",),
    ("stockagent/data/private.json",),
])
def test_overlay_rejects_non_new_or_private_additions(tmp_path, additions):
    receipt = _base_release(tmp_path)
    with pytest.raises(ValueError, match="unique new canonical Python modules"):
        builder.build_overlay_release(receipt, tmp_path, tmp_path / "out", overrides=(),
                                      additions=additions, uv="uv")
