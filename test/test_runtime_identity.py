"""Exact runtime identity, tamper detection and stable repository observations."""

from copy import deepcopy
from pathlib import Path
import subprocess

import pytest

from stockagent.runtime_identity import (
    identity_sha256, source_identity, validate_runtime_lock, verify_source_release,
)


def _lock():
    body = {"schema_version": 1, "python": "3.12.14", "implementation": "CPython",
            "machine": "x86_64", "system": "Linux", "packages": {"torch": "2.13.0"}}
    return {**body, "sha256": identity_sha256(body)}


def test_runtime_lock_rejects_package_change_and_tampering():
    expected = _lock()
    actual = deepcopy(expected)
    assert validate_runtime_lock(expected, actual) == []
    actual["packages"]["torch"] = "2.14.0"
    assert any("torch" in item for item in validate_runtime_lock(expected, actual))
    expected["packages"]["torch"] = "2.14.0"
    assert validate_runtime_lock(expected, actual) == ["runtime lock checksum mismatch"]


def test_runtime_lock_rejects_missing_and_additional_packages():
    expected = _lock()
    actual = deepcopy(expected)
    actual["packages"] = {"numpy": "2.4.6"}
    differences = validate_runtime_lock(expected, actual)
    assert len(differences) == 2
    assert any("torch" in item for item in differences)
    assert any("numpy" in item for item in differences)


def test_runtime_observes_invalid_metadata_without_claiming_a_valid_lock(tmp_path, monkeypatch):
    import stockagent.runtime_identity as module

    class RemovedDistribution:
        _path = tmp_path / "removed-1.0.dist-info"

        @property
        def metadata(self):
            raise FileNotFoundError("removed cached metadata")

    class InstalledDistribution:
        metadata = {"Name": "real_package", "Version": "1.0"}
        version = "1.0"

    RemovedDistribution._path.mkdir()
    (RemovedDistribution._path / "INSTALLER").write_text("retained nonempty invalid metadata\n")
    monkeypatch.setattr(module.metadata, "distributions", lambda: [
        RemovedDistribution(), InstalledDistribution(),
    ])
    observed = module.runtime_identity()
    assert observed["packages"] == {"real-package": "1.0"}
    assert observed["metadata_errors"] == [{
        "entry": "removed-1.0.dist-info", "error_type": "FileNotFoundError",
    }]
    assert validate_runtime_lock(observed, observed) == [
        "runtime distribution metadata observation is incomplete",
    ]


def test_runtime_retains_empty_and_stale_finder_notes_without_inventing_packages(tmp_path, monkeypatch):
    import stockagent.runtime_identity as module

    class EmptyDistribution:
        metadata = {}
        _path = tmp_path / "empty-1.0.dist-info"

    class StaleDistribution:
        metadata = {}
        _path = tmp_path / "stale-1.0.dist-info"

    EmptyDistribution._path.mkdir()
    monkeypatch.setattr(module.metadata, "distributions", lambda: [
        EmptyDistribution(), StaleDistribution(),
    ])
    observed = module.runtime_identity()
    assert observed["packages"] == {}
    assert "metadata_errors" not in observed
    assert observed["non_distribution_metadata_entries"] == [
        {"entry": "empty-1.0.dist-info", "reason": "empty_directory"},
        {"entry": "stale-1.0.dist-info", "reason": "stale_finder_entry"},
    ]
    assert validate_runtime_lock(observed, observed) == []


def test_source_identity_includes_untracked_code_but_not_private_files(tmp_path):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "-c", "user.email=a@b.invalid", "-c",
                    "user.name=test", "commit", "--allow-empty", "-qm", "baseline"], check=True)
    code = tmp_path / "stockagent" / "nested" / "runner.py"
    code.parent.mkdir(parents=True)
    code.write_text("value = 1\n")
    plotter = tmp_path / "plot_epoch_curves.py"
    plotter.write_text("plotter_version = 1\n")
    private = tmp_path / ".env"
    private.write_text("SECRET=private\n")
    page = tmp_path / "services/public_dashboards/index.html"
    page.parent.mkdir(parents=True)
    page.write_text("<p>real public application</p>\n")
    first = source_identity(tmp_path)
    assert set(first["files"]) == {
        "stockagent/nested/runner.py", "services/public_dashboards/index.html", "plot_epoch_curves.py",
    }
    private.write_text("SECRET=changed\n")
    assert source_identity(tmp_path)["source_sha256"] == first["source_sha256"]
    plotter.write_text("plotter_version = 2\n")
    assert source_identity(tmp_path)["files"]["plot_epoch_curves.py"] != first["files"]["plot_epoch_curves.py"]
    code.write_text("value = 2\n")
    assert source_identity(tmp_path)["source_sha256"] != first["source_sha256"]
    page.write_text("<p>new public application</p>\n")
    assert source_identity(tmp_path)["files"]["services/public_dashboards/index.html"] != first["files"]["services/public_dashboards/index.html"]


def test_deployment_templates_and_provider_pages_are_versioned_inputs(tmp_path):
    import json
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "-c", "user.email=a@b.invalid", "-c",
                    "user.name=test", "commit", "--allow-empty", "-qm", "baseline"], check=True)
    unit = tmp_path/'deploy/systemd/stockagent-control-backup.service.in'
    page = tmp_path/'services/provider_dashboard/index.html'
    for path, body in ((unit, '[Service]\nMemoryMax=256M\n'), (page, '<p>versioned projection</p>\n')):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)
    identity = source_identity(tmp_path)
    assert set(identity['files']) == {str(p.relative_to(tmp_path)) for p in (unit, page)}
    receipt = tmp_path/'release.json'
    receipt.write_text(json.dumps({'schema_version':1, 'state':'built', 'code':identity,
                                  'source_bundle':{'files':identity['files'], 'sha256':'bundle'}}))
    assert verify_source_release(receipt, tmp_path)['verified_file_count'] == 2
    unit.write_text('[Service]\nMemoryMax=512M\n')
    assert source_identity(tmp_path)['source_sha256'] != identity['source_sha256']
    with pytest.raises(ValueError, match='source mismatch'):
        verify_source_release(receipt, tmp_path)


def test_source_identity_rejects_external_symlink(tmp_path):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    path = tmp_path / "stockagent"
    path.mkdir()
    (path / "outside.py").symlink_to(Path(__file__).resolve())
    with pytest.raises(ValueError, match="escapes repository"):
        source_identity(tmp_path)


def _release(tmp_path, files):
    import hashlib
    import json
    hashes = {}
    for name, body in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        hashes[name] = hashlib.sha256(body).hexdigest()
    receipt = tmp_path / "release.json"
    receipt.write_text(json.dumps({
        "schema_version": 1, "state": "built", "code": {
            "files": hashes, "source_sha256": identity_sha256(hashes), "git_head": "fixed",
        }, "source_bundle": {"files": hashes, "sha256": "bundle"},
        "selected_config": "configs/selected.yaml",
    }))
    return receipt


def test_frozen_release_verifies_without_git_and_rejects_source_change(tmp_path):
    receipt = _release(tmp_path, {"stockagent/module.py": b"value=1\n"})
    verified = verify_source_release(receipt, tmp_path)
    assert verified["verified_file_count"] == 1
    assert verified["source_files_verified"]
    assert not verified["loaded_service_revision_verified"]
    (tmp_path / "stockagent/module.py").write_text("value=2\n")
    with pytest.raises(ValueError, match="source mismatch"):
        verify_source_release(receipt, tmp_path)


def test_frozen_release_rejects_changed_inventory_and_path_escape(tmp_path):
    import json
    receipt = _release(tmp_path, {"stockagent/module.py": b"value=1\n"})
    value = json.loads(receipt.read_bytes())
    value["code"]["files"]["stockagent/module.py"] = "changed"
    receipt.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="inventory checksum"):
        verify_source_release(receipt, tmp_path)
    value["code"]["files"]["stockagent/module.py"] = value["source_bundle"]["files"]["stockagent/module.py"]
    value["source_bundle"]["files"]["../outside"] = "changed"
    value["code"]["source_sha256"] = identity_sha256(value["code"]["files"])
    receipt.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="escapes repository"):
        verify_source_release(receipt, tmp_path)


def test_training_provenance_binds_verified_source_receipt(tmp_path, monkeypatch):
    import stockagent.runtime_identity as module
    receipt = _release(tmp_path, {"stockagent/runtime_identity.py": b"value=1\n"})
    monkeypatch.setattr(module, "__file__", str(tmp_path / "stockagent/runtime_identity.py"))
    monkeypatch.setattr(module, "runtime_identity", _lock)
    monkeypatch.setenv("STOCKAGENT_CODE_RELEASE_RECEIPT", str(receipt))
    module.training_runtime_provenance.cache_clear()
    try:
        provenance = module.training_runtime_provenance()
        assert provenance["source_release"]["source_files_verified"]
        assert provenance["resume_policy"] == "canonical_checkpoint_contract"
    finally:
        module.training_runtime_provenance.cache_clear()
