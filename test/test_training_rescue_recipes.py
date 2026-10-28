"""Input revision identity must not change source capture or a live target lease.

Checkpoint capture uses real CPU fixtures. Packed readback and selected-target
metadata are isolated protocol fixtures, not market-history or CUDA acceptance.
"""
import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from stockagent.training_rescue import RescueError, input_source_readback_ready
from stockagent.training_rescue_recipes import INPUT_FIELDS, REVISION_FIELD, apply_input_recipe, input_recipe
from stockagent.training_rescue_worker import PREPARED_BUILD_POLICY, atomic_json, digest, signature
from test_training_rescue import formal, rescue, enroll_fixture, LocalSource, bootstrap_metadata_fixture


@pytest.fixture
def recipe_case(rescue, formal, monkeypatch, tmp_path):
    base = formal[0]
    source = enroll_fixture(rescue, base, monkeypatch)
    rescue.capture(base["run_id"])
    candidate = copy.deepcopy(base)
    candidate.update(prepared_build_policy=PREPARED_BUILD_POLICY,
        data_sources=[{"dataset": "fixture-originals", "snapshot_id": "fixed-originals",
                       "manifest_sha256": "a" * 64, "materialized_root": str(tmp_path / "raw-originals")}])
    return rescue, base, candidate, source


def admit_fixture_plan(owner, run):
    # Approval metadata fixture only: no claim that real packed raw was decoded.
    effective = owner.recovery_spec(run)
    state = owner.journal(run)
    state.update(bootstrap=bootstrap_metadata_fixture(owner, owner.spec(run)), source_identity={"boot_id": "fixture", "instance_id": "123"},
        input_verification={"spec_sha256": digest(effective), "level": "all_original_file_SHA_decoded"})
    owner.record(run, state)
    key = owner.issue_key(hours=1)
    return owner.plan(run, host="192.0.2.1", port=40032, user="root", key_id=key["key_id"],
                      host_key_line="[192.0.2.1]:40032 " + " ".join(key["public_key"].split()[:2]))


def test_working_directory_declaration_is_an_explicit_versioned_recovery_input(recipe_case):
    _, base, candidate, _ = recipe_case
    candidate["rebuild_working_directories"] = []
    revision = input_recipe(base, candidate)
    assert revision["inputs"]["rebuild_working_directories"] == []
    effective = apply_input_recipe(base, revision, digest(revision))
    assert effective["rebuild_working_directories"] == []
    assert effective["source_root"] == base["source_root"]
    assert effective["train_argv"] == base["train_argv"]


def test_recipe_registration_is_immutable_idempotent_and_does_not_reenroll_source(recipe_case, monkeypatch):
    owner, base, candidate, source = recipe_case
    path = owner.path(base["run_id"]) / "spec.json"
    original, before = path.read_bytes(), signature(path)
    state_before = owner.journal(base["run_id"])
    monkeypatch.setattr(owner, "source", lambda *_: pytest.fail("no source contact during input registration"))
    result = owner.register_input_recipe(candidate)
    revision_sha = result[REVISION_FIELD]
    assert result["original_enrollment_modified"] is result["source_contacted"] is result["training_started"] is False
    assert owner.spec(base["run_id"]) == base
    assert owner.execution_spec(base["run_id"]) == base
    effective = owner.recovery_spec(base["run_id"])
    assert effective[REVISION_FIELD] == revision_sha and digest(effective) != digest(base)
    assert {k: v for k, v in effective.items() if k not in INPUT_FIELDS | {REVISION_FIELD}} == {
        k: v for k, v in base.items() if k not in INPUT_FIELDS}
    assert path.read_bytes() == original and signature(path) == before
    assert owner.journal(base["run_id"]) == state_before
    pointer = path.with_name("input-recipe.json")
    record = owner.cold / "input-recipes" / base["run_id"] / (revision_sha + ".json")
    identities = signature(pointer), signature(record)
    assert owner.register_input_recipe(candidate)["reused"]
    assert identities == (signature(pointer), signature(record))
    assert owner.capture(base["run_id"], transport=source)["state"] == "unchanged_committed_recovery_point"
    assert signature(path) == before


@pytest.mark.parametrize("field", ["world_size", "global_batch", "environment", "source_root", "runtime_prefix",
                                   "output_root", "config_file", "train_argv", "source_releases", "invented_key"])
def test_input_revision_cannot_change_training_or_source_core(recipe_case, field):
    owner, base, candidate, _ = recipe_case
    bad = copy.deepcopy(candidate)
    if field in {"world_size", "global_batch"}:
        bad[field] *= 2
    elif field == "environment":
        bad[field] = {"POLARS_MAX_THREADS": "1"}
    elif field == "train_argv":
        bad[field] += ["--some-other-experiment"]
    elif field in {"source_releases", "invented_key"}:
        bad[field] = []
    else:
        bad[field] += "-changed"
        if field in {"output_root", "config_file"}:
            option = "--output-dir" if field == "output_root" else "--config"
            bad["train_argv"][bad["train_argv"].index(option) + 1] = bad[field]
    with pytest.raises(ValueError):
        owner.register_input_recipe(bad)
    assert not (owner.path(base["run_id"]) / "input-recipe.json").exists()
    assert not (owner.cold / "input-recipes").exists()


@pytest.mark.parametrize("damage", ["empty_sources", "no_shared_policy", "nested_revision", "body_core", "wrong_id"])
def test_revision_declarations_reject_unbound_or_forged_inputs(recipe_case, damage):
    _, base, candidate, _ = recipe_case
    if damage in {"empty_sources", "no_shared_policy", "nested_revision"}:
        if damage == "empty_sources":candidate["data_sources"] = []
        elif damage == "no_shared_policy":candidate.pop("prepared_build_policy")
        else:candidate[REVISION_FIELD] = "0" * 64
        with pytest.raises(ValueError):input_recipe(base, candidate)
    else:
        body = input_recipe(base, candidate);sha = digest(body)
        if damage == "body_core":body["inputs"]["global_batch"] = 1
        else:sha = "0" * 64
        with pytest.raises(ValueError):apply_input_recipe(base, body, sha)


def test_old_source_proof_cannot_admit_a_new_recipe(recipe_case):
    owner, base, candidate, _ = recipe_case
    run = base["run_id"]
    state = owner.journal(run)
    state.update(bootstrap={"id": "c" * 64}, source_identity={"boot_id": "fixture", "instance_id": "123"},
                 input_verification={"spec_sha256": digest(base), "level": "all_original_file_SHA_decoded"})
    owner.record(run, state)
    owner.register_input_recipe(candidate)
    assert not input_source_readback_ready(owner.recovery_spec(run), owner.journal(run))
    row = owner.status()["runs"][0]
    assert not row["recovery_prepared"] and not row["pinned_sources_readback_verified"]
    assert row[REVISION_FIELD] == owner.recovery_spec(run)[REVISION_FIELD]
    key = owner.issue_key(hours=1)
    with pytest.raises(RescueError, match="independently decoded"):
        owner.plan(run, host="192.0.2.1", port=40032, user="root", key_id=key["key_id"],
                   host_key_line="[192.0.2.1]:40032 " + " ".join(key["public_key"].split()[:2]))


def test_source_verification_receipt_binds_effective_recipe_not_original_capture(recipe_case, monkeypatch):
    import stockagent.data_sync.packed_snapshots as packed
    owner, base, candidate, _ = recipe_case
    owner.register_input_recipe(candidate)
    effective = owner.recovery_spec(base["run_id"])
    monkeypatch.setattr(packed, "resolve_packed_snapshot_id", lambda *args: SimpleNamespace(
        manifest_sha256="a" * 64, manifest={"source": {"files": 1}}))
    def verifier(root, resolved, **kwargs):
        assert kwargs["reconstruct_all"] is True
        return {"independently_reconstructed_files": 1}
    monkeypatch.setattr(packed, "verify_packed_snapshot", verifier)
    result = owner.verify_sources(base["run_id"])
    proof = json.loads(Path(result["receipt"]).read_bytes())
    assert proof["spec_sha256"] == digest(effective) != digest(base)
    assert input_source_readback_ready(effective, owner.journal(base["run_id"]))
    assert not input_source_readback_ready(base, owner.journal(base["run_id"]))
    assert owner.spec(base["run_id"]) == base


@pytest.mark.parametrize("damage", ["record", "pointer", "base"])
def test_corrupt_recipe_control_never_falls_back_to_checkpoint_only(recipe_case, damage):
    owner, base, candidate, _ = recipe_case
    result = owner.register_input_recipe(candidate)
    if damage == "record":
        record = owner.cold / "input-recipes" / base["run_id"] / (result[REVISION_FIELD] + ".json")
        body = json.loads(record.read_bytes());body["inputs"]["data_sources"][0]["manifest_sha256"] = "b" * 64
        atomic_json(record, body)
    elif damage == "pointer":
        pointer = owner.path(base["run_id"]) / "input-recipe.json"
        original = pointer.with_name("preserved-pointer.json");pointer.rename(original);pointer.symlink_to(original)
    else:
        changed = copy.deepcopy(base);changed["global_batch"] *= 2
        atomic_json(owner.path(base["run_id"]) / "spec.json", changed)
    with pytest.raises((ValueError, RescueError)):
        owner.recovery_spec(base["run_id"])
    with pytest.raises((ValueError, RescueError)):
        owner.status()


def test_new_recipe_invalidates_old_approved_plan_before_any_target_contact(recipe_case, monkeypatch):
    owner, base, candidate, _ = recipe_case
    owner.register_input_recipe(candidate)
    plan = admit_fixture_plan(owner, base["run_id"])
    assert owner.confirmed_plan(plan["plan_id"], plan["plan_id"])[REVISION_FIELD] == owner.recovery_spec(base["run_id"])[REVISION_FIELD]
    changed = copy.deepcopy(candidate);changed["data_sources"][0]["manifest_sha256"] = "b" * 64
    owner.register_input_recipe(changed)
    monkeypatch.setattr(owner, "target_ssh", lambda *_: pytest.fail("no target contact after recipe drift"))
    with pytest.raises(RescueError, match="key/spec no longer matches"):
        owner.recover(plan["plan_id"], confirm=plan["plan_id"], old_node_stopped_receipt=Path("/not-read"))
    assert owner.journal(base["run_id"])["generation"] == 1


def test_active_target_capture_and_lease_follow_approved_recipe_not_current_pointer(recipe_case, monkeypatch):
    owner, base, candidate, _ = recipe_case
    owner.register_input_recipe(candidate)
    first_spec = owner.recovery_spec(base["run_id"])
    planned = admit_fixture_plan(owner, base["run_id"])
    changed = copy.deepcopy(candidate);changed["data_sources"][0]["manifest_sha256"] = "b" * 64
    owner.register_input_recipe(changed)
    # Deliberately simulate a stale/moved current pointer. A live target must
    # still use the immutable revision in its own accepted plan.
    state = owner.journal(base["run_id"])
    state.update(state="recovering", generation=2, target_boot_id="fixture",
                 active_target={"plan_id": planned["plan_id"]})
    owner.record(base["run_id"], state)
    assert owner.execution_spec(base["run_id"]) == first_spec
    assert owner.recovery_spec(base["run_id"]) != first_spec
    requests = []
    target = LocalSource(first_spec, owner.state / "remote-fixture")
    target.rpc = lambda request, **kwargs: requests.append(request)
    monkeypatch.setattr(owner, "control_ssh", lambda *_: target)
    owner.renew_target(base["run_id"], state)
    assert requests[0]["lease"]["spec_sha256"] == digest(first_spec)
    assert owner.capture(base["run_id"], transport=target)["state"] == "unchanged_committed_recovery_point"
    with pytest.raises(RescueError, match="active target/migration"):
        owner.register_input_recipe(candidate)
    assert owner.spec(base["run_id"]) == base


def test_missing_original_control_cannot_attach_a_recipe(recipe_case, tmp_path):
    owner, base, candidate, _ = recipe_case
    candidate["input_controls"] = [{"path": str(tmp_path / "original-control.json"), "sha256": "f" * 64, "bytes": 10}]
    with pytest.raises((OSError, ValueError, RescueError)):
        owner.register_input_recipe(candidate)
    assert not (owner.path(base["run_id"]) / "input-recipe.json").exists()
