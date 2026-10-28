"""Checkpoint preservation must explain missing bootstrap operands without WAN IO."""
import copy

import pytest

from stockagent.training_rescue import RescueError, bootstrap_declaration_gaps
from stockagent.training_rescue_worker import atomic_json, digest, signature
from test_training_rescue import formal, rescue, enroll_fixture


@pytest.mark.parametrize("capsule", [None, "", "relative/runtime.tar.gz", 42, "/bad\x00path"])
def test_incomplete_bootstrap_rejects_before_source_contact(rescue, formal, monkeypatch, capsule):
    spec = copy.deepcopy(formal[0])
    if capsule is not None:
        spec["runtime_capsule_path"] = capsule
    enroll_fixture(rescue, spec, monkeypatch)
    rescue.capture(spec["run_id"])
    path = rescue.path(spec["run_id"]) / "state.json"
    before, contents = signature(path), path.read_bytes()
    monkeypatch.setattr(rescue, "source", lambda *_: pytest.fail("no WAN contact for missing operands"))
    with pytest.raises(RescueError, match="bootstrap preservation enrollment is incomplete.*runtime_capsule_path"):
        rescue.seal_bootstrap(spec["run_id"])
    assert signature(path) == before and path.read_bytes() == contents
    assert not (rescue.cold / "bootstrap").exists()


@pytest.mark.parametrize("files", [None, [], {}, [None], [{"source_path": "/source"}]])
def test_missing_bootstrap_file_metadata_is_explicit_before_source_contact(rescue, formal, monkeypatch, files):
    spec = copy.deepcopy(formal[0])
    spec.update(runtime_capsule_path="/closed/runtime.tar.gz", bootstrap_files=files)
    enroll_fixture(rescue, spec, monkeypatch)
    monkeypatch.setattr(rescue, "source", lambda *_: pytest.fail("no WAN contact for malformed operands"))
    with pytest.raises(RescueError, match="bootstrap preservation enrollment is incomplete.*bootstrap_file"):
        rescue.seal_bootstrap(spec["run_id"])


def test_status_explains_checkpoint_only_enrollment_without_mutating_controls(rescue, formal, monkeypatch):
    spec = formal[0]
    enroll_fixture(rescue, spec, monkeypatch)
    rescue.capture(spec["run_id"])
    paths = [rescue.path(spec["run_id"]) / name for name in ("spec.json", "state.json")]
    original = [(signature(p), p.read_bytes()) for p in paths]
    monkeypatch.setattr(rescue, "source", lambda *_: pytest.fail("status cannot contact a source"))
    row = rescue.status()["runs"][0]
    assert row["checkpoint_epochs"] == {"train_2024/checkpoint_last.pt": 3}
    assert not row["recovery_prepared"] and not row["bootstrap_preserved"]
    assert row["bootstrap_declaration"] == {
        "contract": "bootstrap_enrollment_declaration_v1", "complete": False,
        "missing": ["runtime_capsule_path", "bootstrap_files", "runtime_lock",
                    "training_config_binding", "source_release_bindings"],
        "metadata_only": True, "fresh_D_readback_verified": False,
    }
    assert original == [(signature(p), p.read_bytes()) for p in paths]


def test_full_bootstrap_declarations_are_not_an_independent_D_readback(rescue, formal, monkeypatch):
    spec = copy.deepcopy(formal[0])
    spec.update(runtime_capsule_path="/closed/runtime.tar.gz", runtime_lock={"python": "fixture"},
        source_receipt="/closed/release.json", bootstrap_files=[
            {"source_path": path, "target_path": path, "role": role, "bytes": 1, "sha256": "a" * 64}
            for path, role in [(spec["config_file"], "training_config"),
                               ("/closed/release.json", "source_receipt"), ("/closed/source.zip", "source_zip")]])
    assert bootstrap_declaration_gaps(spec) == []
    enroll_fixture(rescue, spec, monkeypatch)
    state = rescue.journal(spec["run_id"])
    atomic_json(rescue.path(spec["run_id"]) / "state.json", state)
    row = rescue.status()["runs"][0]
    assert row["bootstrap_declaration"]["complete"]
    assert not row["bootstrap_declaration"]["fresh_D_readback_verified"]
    assert not row["bootstrap_preserved"] and not row["recovery_prepared"]
    assert row["pinned_sources_readback_verified"] is False
    assert digest(spec) == digest(rescue.spec(spec["run_id"]))
