#!/usr/bin/env python3
"""Acceptance of real Penguin D preservation, not a new Vast GPU launch."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.training_rescue_worker import (  # noqa: E402
    CONTRACT, HASH, _strict_json, absolute, atomic_json, checkpoint_summary,
    digest, file_hash, identifier, input_control_bindings, preparation_references,
    relative, signature, validate_spec,
)
from stockagent.training_rescue_bootstrap import (  # noqa: E402
    MAX_BOOTSTRAP_CONTROL_BYTES, bootstrap_objects, validate_bootstrap_binding,
)

RESTORED_PLAN_V1 = "stockagent_private_rescue_NAS_closed_plan_v1"
RESTORED_PLAN_V2 = "stockagent_private_rescue_NAS_closed_plan_v2"


def _legacy_bootstrap_reference(spec, inputs, bootstrap, reference):
    """Resolve only an original legacy oracle already pinned by its bootstrap.

    A restored file with the right-looking name is insufficient: its role,
    original source, target and complete SHA/size must all be present in the
    unchanged enrollment and its separately validated bootstrap.  This only
    proves which bytes were enrolled, not that preparation or resume ran.
    """
    if bootstrap is None or digest(inputs) != digest(spec):
        return None
    candidates = [row for row in bootstrap.get("files", [])
                  if row.get("target_path") == reference.get("reference_path")]
    if len(candidates) != 1:
        return None
    row = candidates[0]
    if (row not in spec.get("bootstrap_files", [])
            or row.get("role") not in {"parent_prepared_reference", "value_prepared_reference"}
            or row.get("source_path") != reference.get("rebuilt_manifest")
            or not isinstance(row.get("sha256"), str) or not HASH.fullmatch(row["sha256"])
            or type(row.get("bytes")) is not int or not 0 < row["bytes"] <= MAX_BOOTSTRAP_CONTROL_BYTES):
        return None
    return row


def _restored_input_specs(base, state, values, control):
    """Resolve only the recipes and target plan in this closed backup.

    The source enrollment, current recovery inputs and a running target's
    approved inputs are three different identities. No moving local pointer
    or live service is consulted during an isolated restore.
    """
    from stockagent.training_rescue_recipes import apply_input_recipe
    run = base["run_id"]

    def revision_spec(revision):
        if not isinstance(revision, str) or not HASH.fullmatch(revision):
            raise ValueError("invalid restored input recipe identity")
        return apply_input_recipe(base, control(f"input-recipes/{run}/{revision}.json"), revision)

    recovery = base
    pointer = values.get("input-recipe.json")
    if pointer is not None:
        if (set(pointer) != {"contract", "revision_sha256", "base_spec_sha256"}
                or pointer["contract"] != "stockagent.training_input_recipe_pointer.v1"
                or pointer["base_spec_sha256"] != digest(base)):
            raise ValueError("restored recipe pointer differs from original enrollment")
        recovery = revision_spec(pointer["revision_sha256"])
    execution = base
    target = state.get("active_target")
    if target:
        plan = values.get("target-plan.json")
        if (not isinstance(target, dict) or not isinstance(target.get("plan_id"), str)
                or not HASH.fullmatch(target["plan_id"]) or not isinstance(plan, dict)
                or digest(plan) != target["plan_id"] or plan.get("run_id") != run):
            raise ValueError("restored active target lacks its exact approved plan")
        revision = plan.get("input_recipe_revision_sha256")
        if revision is not None:
            if plan.get("original_spec_sha256") != digest(base):
                raise ValueError("restored target plan differs from original enrollment")
            execution = revision_spec(revision)
        if plan.get("spec_sha256") != digest(execution):
            raise ValueError("restored target plan differs from its pinned input recipe")
    elif "target-plan.json" in values:
        raise ValueError("restored target plan is not bound to an active target")
    return recovery, execution


def _closed_bytes(path, *, maximum=32 * 1024**2):
    """Read bounded controls without following substituted links or FIFOs."""
    path = absolute(str(path))
    before = signature(path)
    if not stat.S_ISREG(before[-1]) or not 0 < before[2] <= maximum:
        raise ValueError("restored control is not a bounded regular file")
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as stream:
        held = os.fstat(stream.fileno())
        if (held.st_dev, held.st_ino, held.st_size, held.st_mtime_ns, held.st_ctime_ns, held.st_mode) != before:
            raise ValueError("restored control was replaced before read")
        raw = stream.read(maximum + 1)
        if len(raw) != before[2] or signature(path) != before:
            raise ValueError("restored control changed during read")
    absolute(str(path))
    return raw


def _restored_member_sha(path):
    path = absolute(str(path))
    before = signature(path)
    if not stat.S_ISREG(before[-1]):
        raise ValueError("restored payload is not a regular file")
    checksum = hashlib.sha256()
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as stream:
        held = os.fstat(stream.fileno())
        if (held.st_dev, held.st_ino, held.st_size, held.st_mtime_ns, held.st_ctime_ns, held.st_mode) != before:
            raise ValueError("restored payload was replaced before read")
        for block in iter(lambda: stream.read(4 * 1024**2), b""):
            checksum.update(block)
        held = os.fstat(stream.fileno())
        if (held.st_dev, held.st_ino, held.st_size, held.st_mtime_ns, held.st_ctime_ns, held.st_mode) != before:
            raise ValueError("restored payload changed during full read")
    if signature(path) != before:
        raise ValueError("restored payload path changed during full read")
    absolute(str(path))
    return checksum.hexdigest(), before


def verify_restored_plan(root: Path, plan_path: Path, plan_id: str, *, additional_roots=(),
                         base_roots=(), base_plan_path=None,
                         incremental_path=None, incremental_id=None) -> dict:
    """Verify an isolated exact restore with the original rescue contracts.

    This path is standard-library only. It never opens the Penguin enrollment,
    imports torch, restores a runtime, contacts a service or deserializes a
    checkpoint. Recorded checkpoint attestations are not fresh CPU proofs.
    """
    current_roots = [absolute(str(path)) for path in (root, *additional_roots)]
    roots = [*current_roots, *(absolute(str(path)) for path in base_roots)]
    incremental = None
    base_raw = incremental_raw = None
    if base_roots or base_plan_path is not None or incremental_path is not None or incremental_id is not None:
        if not base_roots or base_plan_path is None or incremental_path is None or incremental_id is None:
            raise ValueError("incremental restore needs its complete pinned base and delta arguments")
    if not 1 <= len(roots) <= 64:
        raise ValueError("invalid bounded restored part collection")
    root_identities = set()
    for index, path in enumerate(roots):
        info = path.lstat()
        identity = (info.st_dev, info.st_ino)
        if (not stat.S_ISDIR(info.st_mode) or identity in root_identities
                or any(path == old or path in old.parents or old in path.parents for old in roots[:index])):
            raise ValueError("restored parts must be distinct nonoverlapping directories")
        root_identities.add(identity)
    plan_path = absolute(str(plan_path))
    if not HASH.fullmatch(plan_id):
        raise ValueError("pin the original paired plan identity")
    plan_raw = _closed_bytes(plan_path)
    plan = _strict_json(plan_raw)
    if (plan.get("contract") not in {RESTORED_PLAN_V1, RESTORED_PLAN_V2}
            or plan.get("identity_sha256") != plan_id
            or digest({k: v for k, v in plan.items() if k != "identity_sha256"}) != plan_id
            or plan.get("route") != "existing_encrypted_Restic_relay"
            or plan.get("delivery_kind") != "private_vast_recovery_backup"
            or plan.get("encryption_required") is not True
            or plan.get("plaintext_lake_export_allowed") is not False):
        raise ValueError("restored plan differs from its original private identity")
    revised = plan["contract"] == RESTORED_PLAN_V2
    root_members = None
    if incremental_path is not None:
        from stockagent.training_rescue_backup import deliveries, validate_incremental
        base_plan_path = absolute(str(base_plan_path))
        incremental_path = absolute(str(incremental_path))
        base_raw = _closed_bytes(base_plan_path, maximum=1024**2)
        incremental_raw = _closed_bytes(incremental_path, maximum=1024**2)
        incremental = _strict_json(incremental_raw)
        if (not isinstance(incremental_id, str) or not HASH.fullmatch(incremental_id)
                or incremental.get("identity_sha256") != incremental_id):
            raise ValueError("pin the original incremental backup identity")
        base_plan = _strict_json(base_raw)
        validate_incremental(plan, plan_raw, base_plan, base_raw, incremental)
        options = {key: incremental[key] for key in ("maximum_batch_bytes", "maximum_batch_files")}
        current_parts = deliveries(plan, plan_raw, incremental=incremental, base_raw=base_raw, **options)
        previous_parts = deliveries(base_plan, base_raw, **options)
        if len(current_roots) != len(current_parts) or len(base_roots) != len(previous_parts):
            raise ValueError("incremental restore needs every original base and delta part in ordinal order")
        root_members = {path: {row["relative"] for row in part["members"]}
                        for path, part in zip(roots, [*current_parts, *previous_parts])}
    members = plan.get("members")
    if (not isinstance(members, list) or not 0 < len(members) <= 10000
            or type(plan.get("files")) is not int or len(members) != plan["files"]):
        raise ValueError("invalid bounded restored member census")
    rows = {}
    for row in members:
        name = relative(row["relative"])
        if (name in rows or type(row.get("bytes")) is not int or not 0 <= row["bytes"] <= 32 * 1024**3
                or not isinstance(row.get("sha256"), str) or not HASH.fullmatch(row["sha256"])):
            raise ValueError("conflicting or invalid restored member")
        parts = Path(name).parts
        if row.get("role") == "private_rescue_object":
            if parts != ("objects", row["sha256"][:2], row["sha256"]):
                raise ValueError("restored CAS name differs from its content identity")
        elif (row.get("role") != "private_rescue_control" or len(parts) < 3
              or parts[0] not in ({"commits", "bootstrap", "source-verification", "evidence"}
                                  | ({"input-recipes"} if revised else set()))
              or not name.endswith(".json")):
            raise ValueError("unrecognized private restored namespace")
        rows[name] = row
    total = sum(row["bytes"] for row in rows.values())
    if type(plan.get("source_bytes")) is not int or total != plan["source_bytes"] or total > 32 * 1024**3:
        raise ValueError("restored byte census differs")
    for root in roots:
        if plan_path.is_relative_to(root) and plan_path.relative_to(root).as_posix() != "recovery-plan.private.json":
            raise ValueError("the restored plan must stay outside its payload census")

    def actual_members():
        actual, directories, copies = {}, {}, {}
        def fail(error):
            raise error
        for root in roots:
            absolute(str(root))
            directories[root] = signature(root)
            for directory, dirs, files in os.walk(root, followlinks=False, onerror=fail):
                for name in dirs:
                    path = Path(directory) / name
                    if path.is_symlink() or not path.is_dir():
                        raise ValueError("restored tree contains a redirected directory")
                    directories[path] = signature(path)
                for name in files:
                    path = Path(directory) / name
                    if path.is_symlink() or not stat.S_ISREG(path.lstat().st_mode):
                        raise ValueError("restored tree contains a nonregular member")
                    name = path.relative_to(root).as_posix()
                    controls = ({"recovery-plan.private.json": plan_raw,
                                 "base-recovery-plan.private.json": base_raw,
                                 "incremental-backup.private.json": incremental_raw}
                                if incremental is not None and root in current_roots else
                                {"recovery-plan.private.json": base_raw if incremental is not None else plan_raw})
                    if name in controls:
                        before = signature(path)
                        if _closed_bytes(path) != controls[name] or signature(path) != before:
                            raise ValueError("restored part plan copies differ")
                        copies[path] = before
                    elif name in actual:
                        raise ValueError("restored parts contain a duplicate source member")
                    else:
                        if root_members is not None and name not in root_members[root]:
                            raise ValueError("restored member differs from its fixed base or incremental part")
                        actual[name] = path
            required_controls = ({"recovery-plan.private.json", "base-recovery-plan.private.json",
                                  "incremental-backup.private.json"}
                                 if incremental is not None and root in current_roots else
                                 {"recovery-plan.private.json"})
            if (len(roots) > 1 or incremental is not None) and any(root / name not in copies for name in required_controls):
                raise ValueError("each restored part needs its identical closed plan copy")
        if set(actual) != set(rows):
            raise ValueError("restored exact member set differs from the closed plan")
        return actual, directories, copies

    locations, directories, plan_copies = actual_members()
    observations = {}
    for name, row in rows.items():
        path = locations[name]
        checksum, before = _restored_member_sha(path)
        if before[2] != row["bytes"] or checksum != row["sha256"]:
            raise ValueError("restored original full SHA or size differs")
        observations[path] = before

    def control(name):
        if name not in rows:
            raise ValueError("referenced private control is outside the exact restored plan")
        row = rows[name]
        raw = _closed_bytes(locations[name])
        if hashlib.sha256(raw).hexdigest() != row["sha256"] or len(raw) != row["bytes"]:
            raise ValueError("restored bound control changed")
        return _strict_json(raw)

    def bound_object(row):
        sha = row["sha256"]
        if not isinstance(sha, str) or not HASH.fullmatch(sha):
            raise ValueError("invalid referenced private CAS digest")
        name = f"objects/{sha[:2]}/{sha}"
        if name not in rows or type(row.get("bytes")) is not int or rows[name]["bytes"] != row["bytes"]:
            raise ValueError("referenced private CAS object is outside the exact restored plan")
        return name

    runs = plan.get("runs")
    if not isinstance(runs, list) or not 0 < len(runs) <= 128:
        raise ValueError("invalid closed run census")
    run_proofs, run_ids = [], set()
    for binding in runs:
        run = identifier(binding["run_id"])
        if run in run_ids:
            raise ValueError("duplicate restored run identity")
        run_ids.add(run)
        controls = binding["controls"]
        required = {f"run-controls/{run}/{name}.json" for name in ("spec", "state")}
        optional = {f"run-controls/{run}/{name}.json" for name in ("input-recipe", "target-plan")} if revised else set()
        names = {row["relative"] for row in controls} if isinstance(controls, list) else set()
        if (not isinstance(controls, list) or len(controls) != len(names)
                or not required <= names or not names <= required | optional):
            raise ValueError("restored run controls differ from their fixed binding")
        values = {Path(row["relative"]).name: control(bound_object(row)) for row in controls}
        spec, state = values["spec.json"], values["state.json"]
        validate_spec(spec)
        if (spec["run_id"] != run or state["run_id"] != run or digest(spec) != binding["spec_sha256"]
                or state.get("latest") != binding.get("latest")):
            raise ValueError("restored run enrollment or latest commit differs")
        recovery_spec, execution_spec = _restored_input_specs(spec, state, values, control) if revised else (spec, spec)
        if revised and (binding.get("recovery_spec_sha256") != digest(recovery_spec)
                        or binding.get("execution_spec_sha256") != digest(execution_spec)):
            raise ValueError("restored input versions differ from the closed run binding")
        latest = binding.get("latest")
        if not isinstance(latest, dict) or not isinstance(latest.get("commit_id"), str) or not HASH.fullmatch(latest["commit_id"]):
            raise ValueError("restored run has no pinned committed recovery point")
        commit = control(f"commits/{run}/{latest['commit_id']}.json")
        inventory = commit["inventory"]
        if (commit.get("contract") != CONTRACT or commit.get("run_id") != run
                or digest(commit) != latest["commit_id"]
                or commit.get("spec_sha256") not in {digest(spec), digest(execution_spec)}
                or inventory.get("spec_sha256") != commit.get("spec_sha256")
                or digest(inventory["files"]) != inventory.get("context_sha256")
                or inventory["context_sha256"] != latest["context_sha256"]
                or inventory["checkpoints"] != latest["checkpoints"]):
            raise ValueError("restored commit or canonical context identity differs")
        context = {relative(row["relative"]): row for row in inventory["files"]}
        if (not 0 < len(context) <= 2048 or len(context) != len(inventory["files"])
                or sum(row["bytes"] for row in context.values()) != latest["bytes"]
                or latest["bytes"] > 4 * 1024**3 or not inventory["checkpoints"]):
            raise ValueError("invalid closed context member census")
        for row in context.values():
            bound_object(row)
        epochs = {}
        for name, proof in inventory["checkpoints"].items():
            if name not in context or type(proof.get("epoch")) is not int or proof["epoch"] < 0:
                raise ValueError("invalid recorded checkpoint binding")
            if proof.get("world_size") != spec["world_size"] or proof.get("global_batch") != spec["global_batch"]:
                raise ValueError("recorded checkpoint trajectory differs from enrollment")
            if (proof.get("full_optimizer_state") is not True
                    or not isinstance(proof.get("semantic_manifest_sha256"), str)
                    or not HASH.fullmatch(proof["semantic_manifest_sha256"])):
                raise ValueError("recorded checkpoint lacks a canonical optimizer attestation")
            epochs[name] = proof["epoch"]
        pending = ["fresh_checkpoint_CPU_deserialization_not_performed", "new_GPU_resume_not_performed"]
        bootstrap = None
        bootstrap_id = state.get("bootstrap", {}).get("id") if state.get("bootstrap") else None
        if bootstrap_id is None:
            pending.append("no_pinned_runtime_bootstrap_in_this_closed_run")
        else:
            if not isinstance(bootstrap_id, str) or not HASH.fullmatch(bootstrap_id):
                raise ValueError("invalid pinned bootstrap identity")
            bootstrap = control(f"bootstrap/{run}/{bootstrap_id}.json")
            if digest(bootstrap) != bootstrap_id:
                raise ValueError("restored bootstrap binding differs")
            def read_bootstrap_object(row):
                return _closed_bytes(locations[bound_object(row)], maximum=MAX_BOOTSTRAP_CONTROL_BYTES)
            validate_bootstrap_binding(spec, bootstrap, read_object=read_bootstrap_object,
                                       source_identity=state.get("source_identity"))
            for row in bootstrap_objects(bootstrap):
                bound_object(row)
        if not state.get("input_verification"):
            pending.append("no_pinned_source_readback_in_this_closed_run")
            source_matches_recovery = False
        else:
            source_id = state["input_verification"]["id"]
            if not isinstance(source_id, str) or not HASH.fullmatch(source_id):
                raise ValueError("invalid pinned source proof identity")
            source = control(f"source-verification/{run}/{source_id}.json")
            if (digest(source) != source_id or source.get("spec_sha256")
                    not in {digest(spec), digest(recovery_spec), digest(execution_spec)}):
                raise ValueError("restored source proof binding differs")
            source_matches_recovery = source.get("spec_sha256") == digest(recovery_spec)
        if revised and not source_matches_recovery:
            pending.append("current_input_recipe_requires_its_own_full_D_source_readback")
        legacy_reference_bindings = []
        specs = {digest(value): value for value in (spec, recovery_spec, execution_spec)}
        for inputs in specs.values():
            for capsule in inputs.get("data_capsules", []):
                for kind in ("archive", "proof"):
                    bound_object({"sha256": capsule[kind + "_sha256"], "bytes": capsule[kind + "_bytes"]})
            for row in input_control_bindings(inputs):
                bound_object(row)
            for step_index, references in preparation_references(inputs).items():
                for reference in references:
                    sha = reference.get("reference_sha256")
                    if sha is None:
                        original = _legacy_bootstrap_reference(spec, inputs, bootstrap, reference)
                        if original is None:
                            pending.append("legacy_prepared_reference_without_pinned_SHA")
                            continue
                        bound_object(original)
                        legacy_reference_bindings.append({
                            "contract": "legacy_original_bootstrap_reference_binding_v1",
                            "step_index": step_index,
                            "reference_path": reference["reference_path"],
                            "source_path": original["source_path"],
                            "role": original["role"],
                            "sha256": original["sha256"], "bytes": original["bytes"],
                        })
                        continue
                    name = f"objects/{sha[:2]}/{sha}"
                    if name not in rows:
                        raise ValueError("pinned original preparation reference is outside the restored plan")
                    for key in ("representation_bundle", "preserved_auxiliary_bundle", "original_file_set_bundle"):
                        if reference.get(key):
                            bound_object(reference[key])
        run_proof = {"run_id": run, "spec_sha256": digest(spec), "commit_id": latest["commit_id"],
                           "context_sha256": inventory["context_sha256"], "recorded_checkpoint_epochs": epochs,
                           "context_files_bound": len(context), "bootstrap_id": bootstrap_id,
                           "fresh_checkpoint_CPU_verified": False, "pending": pending}
        if legacy_reference_bindings:
            run_proof["legacy_reference_bindings"] = legacy_reference_bindings
        if revised:
            run_proof.update(recovery_spec_sha256=digest(recovery_spec), execution_spec_sha256=digest(execution_spec),
                checkpoint_spec_sha256=commit["spec_sha256"],
                input_recipe_revision_sha256=recovery_spec.get("input_recipe_revision_sha256"),
                recorded_source_proof_matches_recovery_spec=source_matches_recovery,
                fresh_full_D_source_decode_performed=False)
        run_proofs.append(run_proof)
    if (any(signature(path) != before for path, before in observations.items())
            or any(signature(path) != before for path, before in directories.items())
            or any(signature(path) != before for path, before in plan_copies.items())
            or actual_members() != (locations, directories, plan_copies) or _closed_bytes(plan_path) != plan_raw
            or incremental is not None and (_closed_bytes(base_plan_path) != base_raw
                                            or _closed_bytes(incremental_path) != incremental_raw)):
        raise ValueError("restored source changed during final exact proof")
    return {"contract": "stockagent.private_training_rescue_offline_readback.v3" if incremental is not None else
                        "stockagent.private_training_rescue_offline_readback.v2" if revised
                        else "stockagent.private_training_rescue_offline_readback.v1",
            "state": "fixed_backup_files_and_recorded_bindings_verified_semantic_restore_pending",
            "plan_identity_sha256": plan_id, "files_verified": len(rows), "bytes_verified": total,
            "restored_parts_verified": len(roots), "payload_copies_created": 0,
            "exact_restored_set_and_full_SHA_verified": True, "runs": run_proofs,
            "torch_imported": "torch" in sys.modules, "training_started": False,
            "source_files_deleted": 0, "NAS_backup_verified": False,
            **({"incremental_backup": {"identity_sha256": incremental_id,
                "base_plan_identity_sha256": incremental["base_plan_identity_sha256"],
                "reused_files_fully_reread": incremental["reused_files"],
                "reused_bytes_fully_reread": incremental["reused_bytes"],
                "new_files_fully_reread": incremental["new_files"],
                "new_bytes_fully_reread": incremental["new_bytes"]}} if incremental is not None else {}),
            "real_new_vast_resume_verified": False, "observed_at": time.time()}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run")
    p.add_argument("--restored-root", type=Path, help="Isolated exact private Restic restore; no Penguin policy required")
    p.add_argument("--restored-part", type=Path, action="append", default=[],
                   help="Additional disjoint restored part; audits their exact union without recopying payload")
    p.add_argument("--restored-plan", type=Path)
    p.add_argument("--plan-id", help="Original identity from the authenticated paired handoff")
    p.add_argument("--base-restored-part", type=Path, action="append", default=[],
                   help="Original fixed base part; no payload copy or replacement of its old plan")
    p.add_argument("--base-restored-plan", type=Path)
    p.add_argument("--incremental-manifest", type=Path)
    p.add_argument("--incremental-id", help="Paired incremental manifest identity; historical ACKs never skip a byte read")
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    if args.output.exists():
        raise ValueError("acceptance needs a new evidence path; never overwrite an older receipt")
    if args.restored_root is not None:
        if args.run is not None or args.restored_plan is None or args.plan_id is None:
            p.error("offline restore requires --restored-plan and --plan-id, without --run")
        if any(args.output.absolute().is_relative_to(root.absolute()) for root in [args.restored_root, *args.restored_part, *args.base_restored_part]):
            p.error("keep the audit output outside the immutable restored payload")
        result = verify_restored_plan(args.restored_root, args.restored_plan, args.plan_id,
                                      additional_roots=args.restored_part, base_roots=args.base_restored_part,
                                      base_plan_path=args.base_restored_plan,
                                      incremental_path=args.incremental_manifest, incremental_id=args.incremental_id)
        atomic_json(args.output.absolute(), result)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    if (args.run is None or args.restored_plan is not None or args.plan_id is not None or args.restored_part
            or args.base_restored_part or args.base_restored_plan is not None
            or args.incremental_manifest is not None or args.incremental_id is not None):
        p.error("use --run for Penguin acceptance or the complete offline restore arguments")
    from stockagent.training_rescue import Rescue, input_source_readback_ready, load_policy
    from stockagent.data_sync.windows_cold_io import binary_reader
    rescue = Rescue(load_policy(ROOT / "configs/training_rescue.json"))
    with rescue.lock(wait_seconds=120):
        rescue.guard()
        base, state = rescue.spec(args.run), rescue.journal(args.run)
        spec, execution = rescue.recovery_spec(args.run), rescue.execution_spec(args.run)
        if not input_source_readback_ready(spec, state):
            raise ValueError("prepared recovery requires non-empty pinned inputs and their own full D source readback")
        commit_id = state["latest"]["commit_id"]
        commit = json.loads((rescue.cold / "commits" / args.run / (commit_id + ".json")).read_bytes())
        bootstrap_id = state["bootstrap"]["id"]
        bootstrap = rescue.bootstrap(args.run, bootstrap_id)
        source_id = state["input_verification"]["id"]
        source = json.loads((rescue.cold / "source-verification" / args.run / (source_id + ".json")).read_bytes())
        if (digest(commit) != commit_id or digest(bootstrap) != bootstrap_id or digest(source) != source_id
                or commit.get("spec_sha256") not in {digest(base), digest(execution)}
                or commit["inventory"].get("spec_sha256") != commit.get("spec_sha256")
                or bootstrap.get("spec_sha256") != digest(base) or source.get("spec_sha256") != digest(spec)
                or state["input_verification"].get("level") != "all_original_file_SHA_decoded"
                or bootstrap.get("source_runtime_stability", {}).get("state") != "source_runtime_still_matches_closed_capsule"):
            raise ValueError("recovery evidence is incomplete or differs from this enrolled run")
        rows = [*commit["inventory"]["files"], *bootstrap_objects(bootstrap)]
        for inputs in {digest(value): value for value in (base, spec, execution)}.values():
            rows.extend(rescue.prepared_reference_objects(inputs))
            for _, _, proof, archive in rescue.input_capsule_objects(inputs):
                rows.extend((proof, archive))
        unique = {}
        for row in rows:
            sha = row["sha256"]
            if sha in unique and row["bytes"] != unique[sha]["bytes"]:
                raise ValueError("one private CAS digest has conflicting declared sizes")
            unique[sha] = row
        for row in unique.values():
            path = rescue.object_path(row["sha256"])
            if path.stat().st_size != row["bytes"] or rescue.object_hash(path) != row["sha256"]:
                raise ValueError("D object failed fresh independent acceptance readback")
        checkpoint_proofs = {}
        # Bounded transient validation, not a training view or a C cold copy.
        with tempfile.TemporaryDirectory(prefix="stockagent-rescue-cpu-proof-") as temp:
            root = Path(temp)
            validation_spec = {**spec, "output_root": str(root)}
            for row in commit["inventory"]["files"]:
                if row["relative"] not in commit["inventory"]["checkpoints"]:
                    continue
                path = root / relative(row["relative"]); path.parent.mkdir(parents=True)
                with binary_reader(rescue.object_path(row["sha256"])) as src, path.open("xb") as dst:
                    shutil.copyfileobj(src, dst, 4 * 1024**2)
                if file_hash(path) != row["sha256"]:
                    raise ValueError("actual full checkpoint failed cold reconstruction")
                proof = checkpoint_summary(path, validation_spec)
                proof.pop("signature", None)
                checkpoint_proofs[row["relative"]] = proof
        for action in ("is-enabled", "is-active"):
            subprocess.run(["systemctl", action, "stockagent-training-rescue.timer"],
                           check=True, capture_output=True)
        body = {"contract": spec["contract"], "state": "accepted_prepared_preservation_not_new_GPU_resume",
            "run_id": args.run, "observed_at": time.time(), "spec_sha256": digest(spec),
            "original_spec_sha256": digest(base), "execution_spec_sha256": digest(execution),
            "checkpoint_spec_sha256": commit["spec_sha256"],
            "input_recipe_revision_sha256": spec.get("input_recipe_revision_sha256"),
            "D_commit_id": commit_id, "D_bootstrap_id": bootstrap_id, "source_proof_id": source_id,
            "checkpoint_context_files": len(commit["inventory"]["files"]),
            "checkpoint_context_bytes": sum(r["bytes"] for r in commit["inventory"]["files"]),
            "runtime_compressed_bytes": bootstrap["runtime_archive"]["bytes"],
            "unique_D_payload_objects_verified": len(unique), "checkpoint_CPU_reconstruction": checkpoint_proofs,
            "original_source_files_SHA_verified": sum(r["verification"]["independently_reconstructed_files"] for r in source["sources"]),
            "preservation_timer_enabled_active": True, "source_lifecycle_state": state.get("source_lifecycle_state"),
            "real_new_vast_resume_verified": False, "cross_hardware_bitwise_claim": False,
            "automatic_destination_selection": False, "provider_Stop_or_Destroy_called": False,
            "training_started": False, "original_source_files_deleted": False,
            "prepared_views_retained_on_penguin": False, "temporary_CPU_validation_copy_retired": True,
            "code_sha256": {str(path.relative_to(ROOT)): file_hash(path) for path in
                [ROOT / "stockagent/training_rescue.py", ROOT / "stockagent/training_rescue_worker.py",
                 ROOT / "stockagent/training_rescue_recipes.py", ROOT / "scripts/verify_training_rescue.py",
                 ROOT / "stockagent/training_rescue_bootstrap.py",
                 ROOT / "scripts/training_rescue.py", ROOT / "scripts/run_training_rescue.sh"]}}
        atomic_json(args.output, body)
        print(json.dumps(body, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
