"""Isolated private restore audit; recorded bindings are not a new CPU resume."""
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from scripts import verify_training_rescue as verifier
from stockagent.training_rescue_worker import CONTRACT, digest

ROOT = Path(__file__).resolve().parents[1]


def fixture(tmp_path, *, bootstrap=False, recorded_damage=None, legacy_ref=False):
    root = tmp_path / "restored"
    root.mkdir()

    def raw_json(value):
        return json.dumps(value, sort_keys=True, indent=2).encode() + b"\n"

    def put(raw):
        sha = hashlib.sha256(raw).hexdigest()
        path = root / "objects" / sha[:2] / sha
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        return {"bytes": len(raw), "sha256": sha}

    def control(namespace, value):
        identity = digest(value)
        path = root / namespace / "fixture" / (identity + ".json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw_json(value))
        return identity

    checkpoint = "train_2024/checkpoint_last.pt"
    row = {"relative": checkpoint, "mode": 0o600, **put(b"opaque checkpoint; never deserialize in file audit")}
    proof = {"epoch": 9, "world_size": 2, "global_batch": 128,
             "full_optimizer_state": True, "semantic_manifest_sha256": "a" * 64}
    if recorded_damage == "batch":
        proof["global_batch"] = 64
    if recorded_damage == "weights":
        proof["full_optimizer_state"] = False
    if recorded_damage == "epoch":
        proof["epoch"] = True
    if recorded_damage == "missing_object":
        row["sha256"] = "e" * 64
    spec = {"contract": CONTRACT, "schema_version": 1, "run_id": "fixture",
            "source_root": "/isolated-fixture-source", "output_root": "/isolated/artifacts/markets/fixture",
            "config_file": "/isolated/config.yaml", "runtime_prefix": "/isolated/runtime",
            "world_size": 2, "global_batch": 128, "environment": {}, "data_sources": [], "rebuild_steps": []}
    spec["train_argv"] = ["train.py", "--config", spec["config_file"], "--output-dir", spec["output_root"], "--resume"]
    if legacy_ref:
        prepared = "/isolated/artifacts/data_preparation/fixture"
        spec["rebuild_steps"] = [["scripts/rebuild.py", "--output-root", prepared]]
        spec["prepared_reconciliation"] = [{"step_index": 0,
            "reference_path": "/isolated/original-oracle.json",
            "rebuilt_manifest": prepared + "/factorized_manifest.json"}]
    if bootstrap:
        config = {**put(b"fixed training config"), "role": "training_config", "source_path": spec["config_file"],
                  "target_path": spec["config_file"]}
        spec["bootstrap_files"] = [config]
        if legacy_ref:
            reference = spec["prepared_reconciliation"][0]
            spec["bootstrap_files"].append({**put(b"original prepared oracle"),
                "role": "parent_prepared_reference", "source_path": reference["rebuilt_manifest"],
                "target_path": reference["reference_path"]})
    spec_id = digest(spec)
    inventory = {"contract": CONTRACT, "spec_sha256": spec_id, "files": [row],
                 "context_sha256": digest([row]), "checkpoints": {checkpoint: proof}}
    if recorded_damage == "context":
        inventory["context_sha256"] = "c" * 64
    commit = {"contract": CONTRACT, "run_id": "fixture", "spec_sha256": spec_id, "inventory": inventory}
    commit_id = control("commits", commit)
    latest = {"commit_id": commit_id, "context_sha256": inventory["context_sha256"],
              "checkpoints": inventory["checkpoints"], "bytes": row["bytes"]}
    state = {"run_id": "fixture", "latest": latest}
    if bootstrap:
        boot = {"contract": CONTRACT, "spec_sha256": spec_id, "files": spec["bootstrap_files"],
                "runtime_archive": put(b"opaque runtime archive"), "runtime_archive_proof": put(b"{}")}
        state["bootstrap"] = {"id": control("bootstrap", boot)}
        source = {"contract": CONTRACT, "spec_sha256": spec_id, "sources": []}
        state["input_verification"] = {"id": control("source-verification", source)}
    controls = [{"relative": f"run-controls/fixture/{name}.json", **put(raw_json(value))}
                for name, value in (("spec", spec), ("state", state))]
    members = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        name = path.relative_to(root).as_posix()
        members.append({"relative": name, "bytes": path.stat().st_size,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "role": "private_rescue_object" if name.startswith("objects/") else "private_rescue_control"})
    plan = {"contract": "stockagent_private_rescue_NAS_closed_plan_v1", "members": members,
            "files": len(members), "source_bytes": sum(row["bytes"] for row in members),
            "runs": [{"run_id": "fixture", "spec_sha256": spec_id, "latest": latest, "controls": controls}],
            "route": "existing_encrypted_Restic_relay", "delivery_kind": "private_vast_recovery_backup",
            "encryption_required": True, "plaintext_lake_export_allowed": False}
    plan_path = root / "recovery-plan.private.json"
    save_plan(plan_path, plan)
    return root, plan_path, plan


def save_plan(path, plan):
    plan.pop("identity_sha256", None)
    plan["identity_sha256"] = digest(plan)
    path.write_text(json.dumps(plan))


@pytest.mark.parametrize("bootstrap", [False, True])
def test_original_bindings_full_files_without_live_policy_or_checkpoint_load(tmp_path, monkeypatch, bootstrap):
    root, plan_path, plan = fixture(tmp_path, bootstrap=bootstrap)
    monkeypatch.setattr(verifier, "checkpoint_summary", lambda *a, **kw: pytest.fail("never load checkpoints in file audit"))
    before = {path: path.read_bytes() for path in root.rglob("*") if path.is_file()}
    proof = verifier.verify_restored_plan(root, plan_path, plan["identity_sha256"])
    assert proof["files_verified"] == plan["files"]
    assert proof["bytes_verified"] == plan["source_bytes"]
    assert proof["exact_restored_set_and_full_SHA_verified"]
    assert proof["runs"][0]["recorded_checkpoint_epochs"] == {"train_2024/checkpoint_last.pt": 9}
    assert proof["runs"][0]["fresh_checkpoint_CPU_verified"] is False
    pending = proof["runs"][0]["pending"]
    assert ("no_pinned_runtime_bootstrap_in_this_closed_run" in pending) is not bootstrap
    assert ("no_pinned_source_readback_in_this_closed_run" in pending) is not bootstrap
    assert proof["training_started"] is False and proof["NAS_backup_verified"] is False
    assert {path: path.read_bytes() for path in root.rglob("*") if path.is_file()} == before


def test_legacy_prepared_reference_is_bound_only_from_original_validated_bootstrap(tmp_path):
    root, path, plan = fixture(tmp_path, bootstrap=True, legacy_ref=True)
    result = verifier.verify_restored_plan(root, path, plan["identity_sha256"])
    run = result["runs"][0]
    assert "legacy_prepared_reference_without_pinned_SHA" not in run["pending"]
    assert run["legacy_reference_bindings"] == [{
        "contract": "legacy_original_bootstrap_reference_binding_v1", "step_index": 0,
        "reference_path": "/isolated/original-oracle.json",
        "source_path": "/isolated/artifacts/data_preparation/fixture/factorized_manifest.json",
        "role": "parent_prepared_reference", "sha256": hashlib.sha256(b"original prepared oracle").hexdigest(),
        "bytes": len(b"original prepared oracle")}]
    assert run["fresh_checkpoint_CPU_verified"] is False
    assert "new_GPU_resume_not_performed" in run["pending"]
    assert result["training_started"] is False and result["NAS_backup_verified"] is False


def test_legacy_prepared_reference_without_bootstrap_stays_pending(tmp_path):
    root, path, plan = fixture(tmp_path, legacy_ref=True)
    run = verifier.verify_restored_plan(root, path, plan["identity_sha256"])["runs"][0]
    assert "legacy_prepared_reference_without_pinned_SHA" in run["pending"]
    assert "legacy_reference_bindings" not in run


@pytest.mark.parametrize("damage", ["wrong_role", "wrong_source", "wrong_target", "wrong_size", "bad_sha", "duplicate", "other_spec"])
def test_legacy_binding_never_infers_from_ambiguous_or_unbound_files(damage):
    ref = {"step_index": 0, "reference_path": "/isolated/oracle.json",
           "rebuilt_manifest": "/isolated/prepared/oracle.json"}
    row = {"role": "parent_prepared_reference", "source_path": ref["rebuilt_manifest"],
           "target_path": ref["reference_path"], "sha256": "a" * 64, "bytes": 100}
    spec = {"bootstrap_files": [row], "prepared_reconciliation": [ref]}
    bootstrap, inputs = {"files": [copy.deepcopy(row)]}, copy.deepcopy(spec)
    if damage == "wrong_role": bootstrap["files"][0]["role"] = "unrelated"
    elif damage == "wrong_source": bootstrap["files"][0]["source_path"] = "/wrong/source"
    elif damage == "wrong_target": bootstrap["files"][0]["target_path"] = "/wrong/target"
    elif damage == "wrong_size": bootstrap["files"][0]["bytes"] = True
    elif damage == "bad_sha": bootstrap["files"][0]["sha256"] = "bad"
    elif damage == "duplicate": bootstrap["files"].append(copy.deepcopy(row))
    else: inputs["changed"] = True
    if damage not in {"duplicate", "other_spec"}:
        spec["bootstrap_files"] = copy.deepcopy(bootstrap["files"])
        inputs = copy.deepcopy(spec)
    assert verifier._legacy_bootstrap_reference(spec, inputs, bootstrap, ref) is None


@pytest.mark.parametrize("damage", ["batch", "weights", "epoch", "missing_object", "context"])
def test_same_full_SHA_files_cannot_grant_invalid_recorded_resume_bindings(tmp_path, damage):
    root, path, plan = fixture(tmp_path, recorded_damage=damage)
    with pytest.raises(ValueError):
        verifier.verify_restored_plan(root, path, plan["identity_sha256"])


@pytest.mark.parametrize("damage", ["duplicate", "bool_files", "total", "bool_size", "path", "role", "CAS", "controls", "duplicate_run", "route"])
def test_reidentified_but_invalid_plan_rejected(tmp_path, damage):
    root, path, plan = fixture(tmp_path)
    if damage == "duplicate":
        plan["members"].append(plan["members"][0]); plan["files"] += 1
    elif damage == "bool_files":
        plan["files"] = True
    elif damage == "total":
        plan["source_bytes"] += 1
    elif damage == "bool_size":
        plan["members"][0]["bytes"] = True
    elif damage == "path":
        plan["members"][0]["relative"] = "../escape"
    elif damage == "role":
        plan["members"][0]["role"] = "unknown"
    elif damage == "CAS":
        next(row for row in plan["members"] if row["role"] == "private_rescue_object")["relative"] = "objects/aa/" + "a" * 64
    elif damage == "controls":
        plan["runs"][0]["controls"] *= 2
    elif damage == "duplicate_run":
        plan["runs"] *= 2
    else:
        plan["route"] = "plaintext_lake"
    save_plan(path, plan)
    with pytest.raises(ValueError):
        verifier.verify_restored_plan(root, path, plan["identity_sha256"])


@pytest.mark.parametrize("damage", ["extra", "missing", "same_size", "changed_size", "file_link", "directory_link", "FIFO"])
def test_exact_restore_cannot_hide_corruption_or_redirected_members(tmp_path, damage):
    root, plan_path, plan = fixture(tmp_path)
    member = root / plan["members"][0]["relative"]
    if damage == "extra":
        (root / "unplanned").write_bytes(b"extra")
    elif damage == "missing":
        member.unlink()
    elif damage == "same_size":
        member.write_bytes(b"x" * member.stat().st_size)
    elif damage == "changed_size":
        member.write_bytes(b"changed")
    elif damage == "file_link":
        target = tmp_path / "outside"
        member.rename(target); member.symlink_to(target)
    elif damage == "directory_link":
        directory = root / "commits"
        target = tmp_path / "outside"
        directory.rename(target); directory.symlink_to(target, target_is_directory=True)
    else:
        member.unlink(); os.mkfifo(member)
    with pytest.raises((ValueError, OSError)):
        verifier.verify_restored_plan(root, plan_path, plan["identity_sha256"])


def test_changed_tree_during_proof_rejects_final_acceptance(tmp_path, monkeypatch):
    root, path, plan = fixture(tmp_path)
    original = verifier._restored_member_sha
    def replace(member):
        proof = original(member)
        (root / "raced-extra").write_bytes(b"extra")
        return proof
    monkeypatch.setattr(verifier, "_restored_member_sha", replace)
    with pytest.raises(ValueError, match="changed during final"):
        verifier.verify_restored_plan(root, path, plan["identity_sha256"])


def test_original_external_plan_identity_required(tmp_path):
    root, path, _ = fixture(tmp_path)
    with pytest.raises(ValueError, match="identity"):
        verifier.verify_restored_plan(root, path, "a" * 64)


def test_standalone_cli_uses_only_stdlib_without_enrollment_or_GPU(tmp_path):
    root, path, plan = fixture(tmp_path, bootstrap=True)
    output = tmp_path / "offline-audit.json"
    command = [sys.executable, "-S", str(ROOT / "scripts/verify_training_rescue.py"),
               "--restored-root", str(root), "--restored-plan", str(path),
               "--plan-id", plan["identity_sha256"], "--output", str(output)]
    result = subprocess.run(command, cwd=tmp_path, text=True, capture_output=True, timeout=20)
    assert result.returncode == 0, result.stderr
    proof = json.loads(output.read_bytes())
    assert proof["torch_imported"] is False
    assert proof["source_files_deleted"] == 0
    assert proof["runs"][0]["fresh_checkpoint_CPU_verified"] is False
    again = subprocess.run(command, cwd=tmp_path, text=True, capture_output=True, timeout=20)
    assert again.returncode != 0 and "never overwrite" in again.stderr


def test_cli_rejects_output_within_restore_before_reading_payload(tmp_path):
    root, path, plan = fixture(tmp_path)
    output = root / "not-allowed.json"
    result = subprocess.run([sys.executable, "-S", str(ROOT / "scripts/verify_training_rescue.py"),
        "--restored-root", str(root), "--restored-plan", str(path), "--plan-id", plan["identity_sha256"],
        "--output", str(output)], cwd=tmp_path, text=True, capture_output=True, timeout=20)
    assert result.returncode != 0 and "outside" in result.stderr
    assert not output.exists()


def split_fixture(tmp_path):
    root, path, plan = fixture(tmp_path, bootstrap=True)
    other = tmp_path / "restored-part2"
    other.mkdir()
    (other / "recovery-plan.private.json").write_bytes(path.read_bytes())
    for row in plan["members"][::2]:
        old = root / row["relative"]
        new = other / row["relative"]
        new.parent.mkdir(parents=True, exist_ok=True)
        old.rename(new)
    return root, other, path, plan


def test_disjoint_restore_parts_verify_exact_union_without_another_full_copy(tmp_path):
    root, other, path, plan = split_fixture(tmp_path)
    proof = verifier.verify_restored_plan(root, path, plan["identity_sha256"], additional_roots=[other])
    assert proof["restored_parts_verified"] == 2
    assert proof["payload_copies_created"] == 0
    assert proof["files_verified"] == plan["files"]
    assert proof["bytes_verified"] == plan["source_bytes"]


@pytest.mark.parametrize("damage", ["repeated", "nested", "duplicate_source", "different_plan", "missing_plan"])
def test_parts_do_not_hide_duplicate_payloads_or_conflicting_plans(tmp_path, damage):
    root, other, path, plan = split_fixture(tmp_path)
    if damage == "repeated":
        other = root
    elif damage == "nested":
        other = root / "nested"
        other.mkdir()
    elif damage == "duplicate_source":
        row = plan["members"][0]
        copy = root / row["relative"]
        copy.parent.mkdir(parents=True, exist_ok=True)
        copy.write_bytes((other / row["relative"]).read_bytes())
    elif damage == "different_plan":
        (other / "recovery-plan.private.json").write_bytes(b"{}")
    else:
        (other / "recovery-plan.private.json").unlink()
    with pytest.raises(ValueError):
        verifier.verify_restored_plan(root, path, plan["identity_sha256"], additional_roots=[other])


def close_members(root, path, plan):
    plan["members"] = [{"relative": p.relative_to(root).as_posix(), "bytes": p.stat().st_size,
                        "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
                        "role": "private_rescue_object" if p.is_relative_to(root / "objects") else "private_rescue_control"}
                       for p in sorted(root.rglob("*")) if p.is_file() and p != path]
    plan["files"] = len(plan["members"])
    plan["source_bytes"] = sum(row["bytes"] for row in plan["members"])
    save_plan(path, plan)


def revised_fixture(tmp_path, *, active=False, current_source=False):
    """Opaque file-audit fixtures never claim raw decode or CPU execution."""
    from stockagent.training_rescue_recipes import apply_input_recipe, input_recipe
    from stockagent.training_rescue_worker import PREPARED_BUILDERS, PREPARED_BUILD_POLICY
    root, path, plan = fixture(tmp_path, bootstrap=True)

    def put(value):
        raw = json.dumps(value, sort_keys=True).encode()
        sha = hashlib.sha256(raw).hexdigest()
        p = root / "objects" / sha[:2] / sha
        p.parent.mkdir(parents=True, exist_ok=True); p.write_bytes(raw)
        return {"sha256": sha, "bytes": len(raw)}

    def load(row):
        sha = row["sha256"]
        return json.loads((root / "objects" / sha[:2] / sha).read_bytes())

    def record(namespace, value):
        sha = digest(value)
        p = root / namespace / "fixture" / (sha + ".json")
        p.parent.mkdir(parents=True, exist_ok=True); p.write_text(json.dumps(value))
        return sha

    binding = plan["runs"][0]
    values = {Path(row["relative"]).name: load(row) for row in binding["controls"]}
    base, state = values["spec.json"], values["state.json"]
    candidate = copy.deepcopy(base)
    candidate.update(prepared_build_policy=PREPARED_BUILD_POLICY,
        data_sources=[{"dataset": "originals", "snapshot_id": "fixed", "manifest_sha256": "a" * 64,
                       "materialized_root": "/isolated/raw-originals"}],
        rebuild_steps=[[PREPARED_BUILDERS["tw_futures_financial_v1"], "--output", "/isolated/artifacts/data_preparation/financial"]])
    oracle = put({"opaque_original_oracle": "requires separate CPU reconciliation"})
    bundle = put({"opaque_original_bundle": "requires separate decoder"})
    candidate["prepared_reconciliation"] = [{"step_index": 0, "kind": "file_set_v2",
        "reference_path": "/isolated/original-oracle.json", "reference_sha256": oracle["sha256"],
        "rebuilt_root": "/isolated/artifacts/data_preparation/financial",
        "representation_bundle": {"path": "/isolated/originals.zip", **bundle}}]
    revision = input_recipe(base, candidate); sha = record("input-recipes", revision)
    effective = apply_input_recipe(base, revision, sha)
    values["input-recipe.json"] = {"contract": "stockagent.training_input_recipe_pointer.v1",
                                  "revision_sha256": sha, "base_spec_sha256": digest(base)}
    if active:
        target_plan = {"contract": CONTRACT, "run_id": "fixture", "spec_sha256": digest(effective),
                       "input_recipe_revision_sha256": sha, "original_spec_sha256": digest(base)}
        values["target-plan.json"] = target_plan
        state["active_target"] = {"plan_id": digest(target_plan)}
    if current_source:
        source = {"contract": CONTRACT, "spec_sha256": digest(effective), "sources": []}
        state["input_verification"] = {"id": record("source-verification", source)}
    plan["contract"] = verifier.RESTORED_PLAN_V2
    binding.update(recovery_spec_sha256=digest(effective), execution_spec_sha256=digest(effective if active else base))

    def save_values():
        binding["controls"] = [{"relative": f"run-controls/fixture/{name}", **put(value)}
                               for name, value in values.items()]
        close_members(root, path, plan)
    save_values()
    return root, path, plan, values, effective, revision, bundle, save_values, record


@pytest.mark.parametrize("current_source", [False, True])
def test_revision_backup_preserves_old_checkpoint_without_promoting_old_source_proof(tmp_path, current_source):
    root, path, plan, values, effective, *_ = revised_fixture(tmp_path, current_source=current_source)
    before = {p: verifier.signature(p) for p in root.rglob("*") if p.is_file()}
    result = verifier.verify_restored_plan(root, path, plan["identity_sha256"])
    proof = result["runs"][0]
    assert proof["spec_sha256"] == proof["checkpoint_spec_sha256"] == proof["execution_spec_sha256"]
    assert proof["recovery_spec_sha256"] == digest(effective) != proof["spec_sha256"]
    assert proof["recorded_source_proof_matches_recovery_spec"] is current_source
    assert ("current_input_recipe_requires_its_own_full_D_source_readback" in proof["pending"]) is not current_source
    assert proof["fresh_full_D_source_decode_performed"] is proof["fresh_checkpoint_CPU_verified"] is False
    assert result["training_started"] is result["NAS_backup_verified"] is False
    assert before == {p: verifier.signature(p) for p in root.rglob("*") if p.is_file()}


def test_restored_running_target_stays_on_approved_revision_when_recovery_pointer_differs(tmp_path):
    from stockagent.training_rescue_recipes import apply_input_recipe
    root, path, plan, values, effective, revision, _, save, record = revised_fixture(tmp_path, active=True, current_source=True)
    new = copy.deepcopy(revision); new["inputs"]["data_sources"][0]["snapshot_id"] = "new-fixed"
    sha = record("input-recipes", new)
    changed = apply_input_recipe(values["spec.json"], new, sha)
    values["input-recipe.json"]["revision_sha256"] = sha
    plan["runs"][0]["recovery_spec_sha256"] = digest(changed); save()
    proof = verifier.verify_restored_plan(root, path, plan["identity_sha256"])["runs"][0]
    assert proof["execution_spec_sha256"] == digest(effective)
    assert proof["recovery_spec_sha256"] == digest(changed)
    assert proof["recorded_source_proof_matches_recovery_spec"] is False


@pytest.mark.parametrize("damage", ["missing_recipe", "missing_bundle", "missing_pointer", "pointer_base",
    "forged_recipe", "missing_target_plan", "wrong_target_plan", "unexpected_target_plan",
    "recovery_binding", "execution_binding", "legacy_namespace", "foreign_source_proof"])
def test_closed_revision_cannot_hide_missing_or_foreign_dependencies(tmp_path, damage):
    active = damage in {"missing_target_plan", "wrong_target_plan"}
    root, path, plan, values, _, revision, bundle, save, record = revised_fixture(tmp_path, active=active)
    if damage == "missing_recipe":
        sha = values["input-recipe.json"]["revision_sha256"]
        (root / "input-recipes/fixture" / (sha + ".json")).unlink()
    elif damage == "missing_bundle":
        (root / "objects" / bundle["sha256"][:2] / bundle["sha256"]).unlink()
    elif damage == "missing_pointer":
        values.pop("input-recipe.json")
    elif damage == "pointer_base":
        values["input-recipe.json"]["base_spec_sha256"] = "f" * 64
    elif damage == "forged_recipe":
        revision["inputs"]["world_size"] = 8
        values["input-recipe.json"]["revision_sha256"] = record("input-recipes", revision)
    elif damage == "missing_target_plan":
        values.pop("target-plan.json")
    elif damage == "wrong_target_plan":
        values["target-plan.json"]["spec_sha256"] = "f" * 64
    elif damage == "unexpected_target_plan":
        values["target-plan.json"] = {"unbound": True}
    elif damage.endswith("_binding"):
        plan["runs"][0][damage.split("_")[0] + "_spec_sha256"] = "f" * 64
    elif damage == "legacy_namespace":
        plan["contract"] = verifier.RESTORED_PLAN_V1
    else:
        values["state.json"]["input_verification"]["id"] = record("source-verification", {
            "contract": CONTRACT, "spec_sha256": "f" * 64, "sources": []})
    save()
    with pytest.raises(ValueError):
        verifier.verify_restored_plan(root, path, plan["identity_sha256"])


def test_revision_restore_standalone_without_site_packages_or_live_enrollment(tmp_path):
    root, path, plan, *_ = revised_fixture(tmp_path, active=True)
    output = tmp_path / "revision-audit.json"
    result = subprocess.run([sys.executable, "-S", str(ROOT / "scripts/verify_training_rescue.py"),
        "--restored-root", str(root), "--restored-plan", str(path), "--plan-id", plan["identity_sha256"],
        "--output", str(output)], cwd=tmp_path, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    proof = json.loads(output.read_bytes())
    assert proof["contract"].endswith(".v2")
    assert proof["torch_imported"] is proof["training_started"] is False


def test_penguin_prepared_acceptance_rejects_checkpoint_only_before_loading_context(tmp_path, monkeypatch):
    import stockagent.training_rescue as rescue_module
    root, _, plan = fixture(tmp_path, bootstrap=True)
    binding = plan["runs"][0]
    def value(name):
        row = next(row for row in binding["controls"] if row["relative"].endswith('/' + name + '.json'))
        return json.loads((root / "objects" / row["sha256"][:2] / row["sha256"]).read_bytes())
    base, state = value("spec"), value("state")
    # Even an old full-readback label cannot turn zero bound sources into READY.
    state["input_verification"].update(spec_sha256=digest(base), level="all_original_file_SHA_decoded")
    owner = SimpleNamespace(lock=lambda **_: nullcontext(), guard=lambda: None,
        spec=lambda _: base, recovery_spec=lambda _: base, execution_spec=lambda _: base,
        journal=lambda _: state)
    monkeypatch.setattr(rescue_module, "load_policy", lambda _: {})
    monkeypatch.setattr(rescue_module, "Rescue", lambda _: owner)
    monkeypatch.setattr(verifier, "checkpoint_summary", lambda *a, **kw: pytest.fail("never deserialize an unprepared recovery"))
    output = tmp_path / "unprepared-output.json"
    monkeypatch.setattr(sys, "argv", [str(ROOT / "scripts/verify_training_rescue.py"),
        "--run", "fixture", "--output", str(output)])
    with pytest.raises(ValueError, match="non-empty pinned inputs"):
        verifier.main()
    assert not output.exists()
