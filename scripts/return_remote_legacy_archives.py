#!/usr/bin/env python3
"""Inventory/return all Vast legacy artifacts through canonical cold packing.

One-shot supervised batch, sharing the existing ingress lock. Never broadens
Syncthing to raw artifacts, changes remote Git, or activates an archived model.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import socket
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import replace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.ingest_remote_cold_artifacts import _rsync_candidate, _ssh_base, _validate_ssh_target, RemoteCandidate
from scripts.configure_artifact_ingress_syncthing import credentials
from scripts.manage_packed_edge import _convergence
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json, sha256_file
from stockagent.data_sync.legacy_artifact_archive import COMPRESSION_PROFILES, DEFAULT_COMPRESSION_PROFILE, LegacyArchiveSpec, load_legacy_specs, prepare_archive, publish_archive, verify_archive_directory, verify_cold_archive
from stockagent.data_sync.materialized_cache import process_references
from stockagent.data_sync.packed_snapshots import resolve_latest_packed
from stockagent.data_sync.remote_legacy_return import dataset_name, identity, metadata_tree, real
from stockagent.data_sync.training_return import admit_workspace
from stockagent.data_sync.syncthing_scan import scan_after_publish

PUBLICATION_OWNER = Path("/run/lock/stockagent-remote-cold-artifact-ingress.lock")


def private_control_prefix() -> str:
    # Send exact locally read code through authenticated SSH stdin, never
    # execute synced code or modify the conflicted remote checkout.
    canonical_archive = (ROOT / "stockagent/data_sync/legacy_artifact_archive.py").read_text()
    code = "import sys, types\n"
    for name, relative in (("stockagent.data_sync.node_roles", "stockagent/data_sync/node_roles.py"),
                           ("stockagent.data_sync.artifact_consumers", "stockagent/data_sync/artifact_consumers.py")):
        code += f"m=types.ModuleType({name!r});m.__file__={'/root/stockAgent/' + relative!r};sys.modules[{name!r}]=m\n"
        code += f"exec({(ROOT / relative).read_text()!r},m.__dict__)\n"
    code += "archive_module = types.ModuleType('stockagent.data_sync.legacy_artifact_archive')\n"
    code += "archive_module.__file__ = '/private-ssh-control/legacy_artifact_archive.py'\n"
    code += "sys.modules[archive_module.__name__] = archive_module\n"
    code += "exec(" + repr(canonical_archive) + ", archive_module.__dict__)\n"
    code += "control_module = types.ModuleType('stockagent.data_sync.remote_legacy_return')\n"
    code += "sys.modules[control_module.__name__] = control_module\n"
    code += "exec(" + repr((ROOT / "stockagent/data_sync/remote_legacy_return.py").read_text()) + ", control_module.__dict__)\n"
    code += "from stockagent.data_sync.remote_legacy_return import *\n"
    return code


def remote(args, request: dict) -> dict:
    code = private_control_prefix()
    code += "\nrequest = json.loads(" + repr(json.dumps(request)) + ")\n"
    code += """
if request['action'] == 'inventory':
    result = inventory_scopes(Path('/root/stockAgent/artifacts'), request.get('include_roots'))
elif request['action'] == 'observe':
    result = metadata_tree(real(Path('/root/stockAgent/artifacts') / request['relative_root']))
    result['process_references'] = artifact_process_references(Path('/root/stockAgent/artifacts') / request['relative_root'], Path('/root/stockAgent/artifacts'))
elif request['action'] == 'exists':
    dataset_name(request['relative_root'])
    result = {'exists': real(Path('/root/stockAgent/artifacts') / request['relative_root']).exists()}
elif request['action'] == 'protect-recovery':
    result = install_recovery_hold(request['relative_root'])
elif request['action'] == 'retire':
    result = retire(Path('/root/stockAgent/artifacts'), Path('/root/stockAgent'), Path('/var/lib/stockagent-legacy-return'), request['ack'], apply=request['apply'])
else:
    raise SnapshotError('unknown fixed private archive control action')
print(json.dumps(result, sort_keys=True))
"""
    command = "cd /root/stockAgent && source scripts/runtime_env.sh && run_fintech_python -"
    completed = subprocess.run([*_ssh_base(args.identity_file, args.ssh_port), _validate_ssh_target(args.ssh_target), command],
                               input=code, text=True, capture_output=True, timeout=1800)
    if completed.returncode:
        # Credentials are never in argv/stdout; return only fixed-code errors.
        raise SnapshotError("remote archive control failed: " + completed.stderr[-1800:])
    for line in reversed(completed.stdout.splitlines()):
        try:
            result = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(result, dict):
            return result
    raise SnapshotError("remote control did not return a machine receipt")


def portable(rows: list[dict]) -> list[dict]:
    return [{"path": r["path"], "kind": r["kind"], "size": r["signature"][2] if r["kind"] == "file" else 0,
             "mode": r["signature"][5] & 0o7777, "mtime_ns": r["signature"][3]}
            for r in rows]


def write_status(args, ledger: dict) -> None:
    ledger["updated_at_epoch"] = time.time()
    atomic_write_json(args.state_root / "progress.json", ledger)
    compact = {k: ledger[k] for k in ("state", "updated_at_epoch")}
    compact["counts"] = {state: sum(r.get("state") == state for r in ledger["items"])
                         for state in sorted({r.get("state", "unknown") for r in ledger["items"]})}
    compact["cold_verified_source_bytes"] = sum(r.get("logical_bytes", 0) for r in ledger["items"] if r.get("cold_verified"))
    compact["remote_reclaimed_bytes"] = sum(r.get("retirement", {}).get("reclaimed_allocated_bytes", 0) for r in ledger["items"])
    compact["current_root"] = ledger.get("current_root")
    compact["transfer_compression"] = getattr(args, "transfer_compression", "none")
    compact["archive_compression"] = getattr(args, "archive_compression", DEFAULT_COMPRESSION_PROFILE)
    compact["batch_directory_fsync"] = getattr(args, "batch_directory_fsync", False)
    atomic_write_json(args.state_root / "summary.json", compact)
    print(json.dumps(compact), flush=True)


@contextmanager
def phase(args, row: dict, spec: LegacyArchiveSpec, name: str):
    """Expose the actual phase while preserving the transaction and checks."""
    row["state"] = name
    path = args.state_root / "item-status" / (spec.dataset + ".json")
    atomic_write_json(path, row)
    started = time.monotonic()
    try:
        yield
    finally:
        row.setdefault("phase_seconds", {})[name] = time.monotonic() - started
        atomic_write_json(path, row)


def ordered_items(items: list[dict], order: str, by_root: dict) -> list[dict]:
    if order == "smallest":
        return sorted(items, key=lambda r: (r.get("logical_bytes", 0), r["relative_root"]))
    if order != "reclaim-first":
        raise SnapshotError("unknown legacy return order")
    def score(row):
        original = by_root[row["relative_root"]]
        shared = any(r.get("kind") == "file" and r["signature"][6] != 1
                     for r in original.get("rows", []))
        reclaim = row.get("reclaimable_allocated_file_bytes", row.get("logical_bytes", 0))
        return shared, -int(reclaim), row.get("files", 0), row["relative_root"]
    return sorted(items, key=score)


@contextmanager
def cohort_owner(state_root: Path, *, wait: bool = False):
    """One progress writer per retained cohort, independent of the D owner."""
    if type(wait) is not bool:
        raise SnapshotError('cohort owner wait must be an explicit boolean')
    path = real(state_root / "cohort-owner.lock")
    with path.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | (0 if wait else fcntl.LOCK_NB))
        except BlockingIOError as error:
            raise SnapshotError("this retained legacy cohort already has an owner") from error
        yield


def archive_one(args, policy: dict, row: dict, spec: LegacyArchiveSpec) -> None:
    with phase(args, row, spec, "fresh-source-probe"):
        before = remote(args, {"action": "observe", "relative_root": row["relative_root"]})
    if before["fingerprint"] != row["fingerprint"] or before["process_references"]:
        raise SnapshotError("remote source changed or is in use; inventory again")
    _check_d_primary_mount(args.sync_root)
    if row.get("private_scratch_removed") is True:
        if ((args.state_root / "staging" / spec.dataset).exists()
                or (spec.stage_root / spec.dataset).exists()):
            raise SnapshotError("retired private scratch reappeared; preserve for audit")
        if row.get("cold_verified") is not True:
            raise SnapshotError("removed scratch has no verified cold release")
        result = {"dataset":spec.dataset, "snapshot_id":row["snapshot_id"],
                  "manifest_sha256":row["manifest_sha256"]}
        with phase(args, row, spec, "reuse-cold-after-local-scratch-retirement"):
            proof = verify_cold_archive(spec, args.sync_root,
                                        verification_root=args.state_root / "verification-scratch")
        exact_recovery(result, proof, resolve_latest_packed(args.sync_root, spec.dataset))
        commit_prepared_archive(args, policy, row, spec, before, None, None, None, None,
                                reused_result=result, reused_proof=proof)
        return
    # Original copy + encoded stage + independent reconstruction. One wave,
    # native ext4 and real C backing-space admission before any growth.
    scratch_parent = args.state_root / "staging"
    scratch_parent.mkdir(mode=0o700, exist_ok=True)
    admit_workspace(args.state_root, policy["reserve_bytes"] + 3 * row["logical_bytes"])
    if shutil.disk_usage(args.sync_root).free < policy["reserve_bytes"] + 2 * row["logical_bytes"]:
        raise SnapshotError("D cold primary lacks wave budget and reserve")
    scratch = scratch_parent / spec.dataset
    source_root = scratch / "artifacts"
    real(source_root / row["relative_root"])
    intent = {"relative_root": row["relative_root"], "remote_fingerprint": before["fingerprint"], "dataset": spec.dataset}
    intent_path = scratch / "transfer-intent.json"
    if scratch.exists():
        if not intent_path.is_file() or intent_path.is_symlink() or json.loads(intent_path.read_text()) != intent:
            raise SnapshotError("existing transfer scratch has no matching owner intent; retained")
        allowed_files = {"artifacts/" + row["relative_root"] + "/" + r["path"] for r in before["rows"] if r["kind"] == "file"} | {"transfer-intent.json"}
        tree = metadata_tree(scratch)
        if any(r["kind"] == "unsupported" or r["cross_filesystem"] or
               (r["kind"] == "file" and (r["path"] not in allowed_files or r["signature"][6] != 1)) for r in tree["rows"]):
            raise SnapshotError("existing transfer scratch has unknown or linked bytes; retained")
    else:
        scratch.mkdir(mode=0o700)
        atomic_write_json(intent_path, intent)
    candidate = RemoteCandidate(row["relative_root"], row["files"], row["logical_bytes"], "0" * 64,
                                row["newest_mtime_ns"], ())
    with phase(args, row, spec, "transferring-from-vast"):
        row["transfer_compression"] = args.transfer_compression
        _rsync_candidate(candidate, ssh_target=args.ssh_target, ssh_port=args.ssh_port,
                         identity_file=args.identity_file, remote_artifact_root="/root/stockAgent/artifacts",
                         destination_artifact_root=source_root, compression=args.transfer_compression)
    with phase(args, row, spec, "post-transfer-source-probe"):
        local = metadata_tree(source_root / row["relative_root"])
        after = remote(args, {"action": "observe", "relative_root": row["relative_root"]})
    if before != after or portable(local["rows"]) != portable(before["rows"]):
        raise SnapshotError("remote source or exact transferred paths/metadata changed")
    # Private, cohort-owned transfer and compression do not mutate the cold
    # authority or remote source. They may overlap another owned publication.
    # Immutable recovery may also run outside the shared mutation owner.
    with phase(args, row, spec, "encoding-private-stage"):
        prepare_archive(spec, source_root, manual_capture=True)
    commit_prepared_archive(args, policy, row, spec, before, scratch, source_root, intent, intent_path)


@contextmanager
def publication_owner(args, row, spec, *, waiting_phase="waiting-shared-owner"):
    with PUBLICATION_OWNER.open("a") as lock:
        with phase(args, row, spec, waiting_phase):
            fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def exact_recovery(result, proof, resolved):
    if any(proof.get(name) != result.get(name) for name in ("dataset", "snapshot_id", "manifest_sha256")):
        raise SnapshotError("independent recovery differs from the exact committed release")
    if (resolved.manifest["snapshot_id"] != proof["snapshot_id"]
            or resolved.manifest_sha256 != proof["manifest_sha256"]):
        raise SnapshotError("cold head changed after independent recovery; source retained")


def commit_prepared_archive(args, policy, row, spec, before, scratch, source_root, intent, intent_path,
                            *, reused_result=None, reused_proof=None):
    if reused_result is None:
        _check_d_primary_mount(args.sync_root)
        @contextmanager
        def owner():
            with publication_owner(args, row, spec):
                with phase(args, row, spec, "pre-commit-source-probe"):
                    current = remote(args, {"action": "observe", "relative_root": row["relative_root"]})
                if current != before or current["process_references"]:
                    raise SnapshotError("remote source changed while private encoding awaited the owner")
                yield
        with phase(args, row, spec, "publishing-d-cold"):
            row["archive_compression"] = spec.compression_profile
            result = publish_archive(spec, source_root, args.sync_root, repo_root=ROOT,
                                     manual_capture=True, defer_scan=True, publication_owner=owner,
                                     batch_directory_fsync=getattr(args, "batch_directory_fsync", False))
        with phase(args, row, spec, "independent-d-recovery"):
            proof = verify_cold_archive(spec, args.sync_root,
                                        verification_root=args.state_root / "verification-scratch")
    else:
        if scratch is not None or reused_proof is None:
            raise SnapshotError("cold reuse must not republish a private scratch")
        result, proof = reused_result, reused_proof
    row.update(result)
    resolved = resolve_latest_packed(args.sync_root, spec.dataset)
    exact_recovery(result, proof, resolved)
    manifest = proof["manifest"]
    row.update(cold_verified=True, decoded_originals_verified=True,
               snapshot_id=proof["snapshot_id"], manifest_sha256=proof["manifest_sha256"])
    atomic_write_json(args.state_root / "recovery" / (spec.dataset + ".json"), proof)
    with phase(args, row, spec, "post-recovery-source-probe"):
        current = remote(args, {"action": "observe", "relative_root": row["relative_root"]})
    if current != before:
        raise SnapshotError("remote source changed after D recovery; original retained")
    base, key = credentials()
    with phase(args, row, spec, "batched-object-before-head-scan"):
        scan_after_publish(args.sync_root, spec.dataset, retry_full=True, batch_object_paths=True)
    with phase(args, row, spec, "cold-verified-retirement-pending"):
        deadline = time.monotonic() + 300
        while _convergence(base, key, "stockagent-packed", "vastai1T").get("ok") is not True:
            if time.monotonic() >= deadline:
                raise SnapshotError("D is verified; waiting for packed peer convergence")
            time.sleep(1)
    for attempt in range(3):
        with publication_owner(args, row, spec, waiting_phase="waiting-retirement-owner"):
            _check_d_primary_mount(args.sync_root)
            resolved = resolve_latest_packed(args.sync_root, spec.dataset)
            exact_recovery(result, proof, resolved)
            # Do not mint a fresh acknowledgement timestamp from metadata.
            # Reconstruct outside the mutation owner again after expiry.
            if 0 <= time.time() - proof["verified_at_epoch"] <= 1500:
                current = remote(args, {"action": "observe", "relative_root": row["relative_root"]})
                if current != before or current["process_references"]:
                    raise SnapshotError("remote source changed while awaiting retirement; source retained")
                if _convergence(base, key, "stockagent-packed", "vastai1T").get("ok") is not True:
                    raise SnapshotError("packed peer changed before retirement; source retained")
                ack = {"contract": "d_verified_remote_legacy_return_v1", "authority_node_id": "penguin",
                       "origin_node_id": "vastai1T", "relative_root": row["relative_root"], "dataset": spec.dataset,
                       "snapshot_id": resolved.manifest["snapshot_id"], "manifest_sha256": resolved.manifest_sha256,
                       "legacy_manifest_sha256": resolved.manifest["metadata"]["legacy_manifest_sha256"],
                       "cold_verified": True, "verified_at_epoch": proof["verified_at_epoch"], "archive_manifest": manifest}
                ack["identity_sha256"] = identity(ack)
                atomic_write_json(args.state_root / "acknowledgements" / (spec.dataset + ".json"), ack)
                with phase(args, row, spec, "fresh-source-retirement"):
                    row["retirement"] = remote(args, {"action": "retire", "ack": ack,
                                                       "apply": policy["retire_verified_source"]})
                break
        with phase(args, row, spec, "refresh-expired-d-recovery"):
            proof = verify_cold_archive(spec, args.sync_root,
                                        verification_root=args.state_root / "verification-scratch")
        manifest = proof["manifest"]
        atomic_write_json(args.state_root / "recovery" / (spec.dataset + ".json"), proof)
    else:
        raise SnapshotError("full recovery repeatedly expired before retirement; original retained")
    row["state"] = "remote-source-retired" if row["retirement"]["deleted"] else "cold-verified-source-protected"
    if row["retirement"]["deleted"]:
        with phase(args, row, spec, "post-retirement-d-recovery"):
            post = verify_cold_archive(spec, args.sync_root,
                                       verification_root=args.state_root / "verification-scratch")
        if post["manifest_sha256"] != row["manifest_sha256"]:
            raise SnapshotError("D head changed after retirement; private source scratch retained")
        atomic_write_json(args.state_root / "post-retirement-recovery" / (spec.dataset + ".json"), post)
        row["post_retirement_d_recovery_verified"] = True
    row["state"] = "remote-source-retired" if row["retirement"]["deleted"] else "cold-verified-source-protected"
    if scratch is None:
        # A previous exact local prune already removed both copies. Never
        # hydrate them merely to finish a remote acknowledgement.
        row["private_scratch_removed"] = True
        return
    # Only these exact private scratch copies may go, after full D restore.
    # Interrupted or altered stages are retained, not blanket-deleted.
    encoded = spec.stage_root / spec.dataset
    verify_archive_directory(encoded / "archive", source_root / row["relative_root"],
                             spec=spec, manual_capture=True, artifact_root=source_root)
    expected_controls = {"source_plan.json", "archive/legacy_archive_manifest.json"}
    for member in manifest["files"]:
        expected_controls.add("archive/" + member["encoded_path"])
        receipt_name = "receipts/" + hashlib.sha256(member["path"].encode()).hexdigest() + ".json"
        expected_controls.add(receipt_name)
        if json.loads((encoded / receipt_name).read_text()) != member:
            raise SnapshotError("encoded scratch receipt changed; retained")
    if json.loads((encoded / "source_plan.json").read_text()) != [{"path": m["path"], "source": m["source"]} for m in manifest["files"]]:
        raise SnapshotError("encoded scratch plan changed; retained")
    observed_controls = {p.relative_to(encoded).as_posix() for p in encoded.rglob("*") if p.is_file()}
    if observed_controls != expected_controls:
        raise SnapshotError("archive scratch contains unknown files; retained")
    source_files = {p.relative_to(source_root / row["relative_root"]).as_posix()
                    for p in (source_root / row["relative_root"]).rglob("*") if p.is_file()}
    if source_files != {member["path"] for member in manifest["files"]}:
        raise SnapshotError("transfer scratch contains unknown files; retained")
    expected_scratch_files = {"artifacts/" + row["relative_root"] + "/" + m["path"] for m in manifest["files"]} | {"transfer-intent.json"}
    if json.loads(intent_path.read_text()) != intent:
        raise SnapshotError("transfer scratch intent changed; retained")
    if {p.relative_to(scratch).as_posix() for p in scratch.rglob("*") if p.is_file()} != expected_scratch_files:
        raise SnapshotError("outer transfer scratch contains unknown files; retained")
    for tree in (encoded, scratch):
        baseline = metadata_tree(real(tree))
        if (process_references(tree) or any(r["kind"] == "unsupported" or r["cross_filesystem"] or
             (r["kind"] == "file" and r["signature"][6] != 1) for r in baseline["rows"])):
            raise SnapshotError("scratch is in use, linked or crosses a filesystem; retained")
        quarantine = tree.with_name(tree.name + ".verified-prune-" + uuid.uuid4().hex)
        tree.rename(quarantine)
        if metadata_tree(quarantine)["rows"] != baseline["rows"] or process_references(quarantine):
            raise SnapshotError("scratch changed after quarantine; retained")
        _check_d_primary_mount(args.sync_root)
        shutil.rmtree(quarantine)
    row["private_scratch_removed"] = True


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", choices=("inventory", "partition", "archive"))
    p.add_argument("--ssh-target", default="root@114.32.64.6")
    p.add_argument("--ssh-port", type=int, default=40032)
    p.add_argument("--transfer-compression", choices=("none", "zstd-1"))
    p.add_argument("--archive-compression", choices=sorted(COMPRESSION_PROFILES))
    p.add_argument("--batch-directory-fsync", action=argparse.BooleanOptionalAction, default=None)
    p.add_argument("--identity-file", type=Path, default=Path("/root/.ssh/stockagent_vastai1t_ed25519"))
    p.add_argument("--sync-root", type=Path, default=Path("/srv/stockagent-packed"))
    p.add_argument("--state-root", type=Path, default=Path("/var/lib/stockagent-vast-legacy-return"))
    p.add_argument("--policy", type=Path, default=ROOT / "configs/data_sync/vastai_legacy_archive_return.json")
    p.add_argument("--max-items", type=int)
    p.add_argument("--order", choices=("smallest", "reclaim-first"), default="smallest")
    p.add_argument("--include-root", action="append", default=[], help="limit this verification wave, not the complete inventory")
    p.add_argument("--reuse-progress", type=Path, help="import an exact completed cohort after fresh D recovery and origin absence checks")
    p.add_argument("--from-inventory", type=Path, help="retained full inventory for explicit oversized-root partitions")
    p.add_argument("--partition-root", action="append", default=[])
    p.add_argument("--apply", action="store_true")
    args = p.parse_args()
    os.umask(0o077)
    if socket.gethostname() != "penguin" or args.sync_root != Path("/srv/stockagent-packed"):
        raise SnapshotError("legacy return may run only on enrolled penguin D authority")
    policy = json.loads(args.policy.read_text())
    expected = {"schema_version", "authority_node_id", "origin_node_id", "scopes", "minimum_stable_hours", "reserve_bytes", "maximum_item_bytes", "archive_only", "retire_verified_source"}
    if (not expected.issubset(policy) or set(policy) - expected - {"transfer_compression", "archive_compression", "batch_directory_fsync"}
        or policy["schema_version"] != 1 or policy["authority_node_id"] != "penguin"
        or policy["origin_node_id"] != "vastai1T" or policy["scopes"] != ["markets", "ablations"]
        or policy["minimum_stable_hours"] < 12 or policy["archive_only"] is not True
        or type(policy["retire_verified_source"]) is not bool):
        raise SnapshotError("invalid scoped legacy return preservation policy")
    configured_compression = policy.get("transfer_compression", "none")
    if not isinstance(configured_compression, str) or configured_compression not in {"none", "zstd-1"}:
        raise SnapshotError("invalid legacy transfer compression profile")
    args.transfer_compression = args.transfer_compression or configured_compression
    configured_archive = policy.get("archive_compression", DEFAULT_COMPRESSION_PROFILE)
    configured_batch = policy.get("batch_directory_fsync", False)
    if (not isinstance(configured_archive, str) or configured_archive not in COMPRESSION_PROFILES
        or type(configured_batch) is not bool):
        raise SnapshotError("invalid cold encoding/durability profile")
    args.archive_compression = args.archive_compression or configured_archive
    args.batch_directory_fsync = configured_batch if args.batch_directory_fsync is None else args.batch_directory_fsync
    real(args.state_root).mkdir(mode=0o700, parents=True, exist_ok=True)
    with cohort_owner(args.state_root):
        return run_cohort(args, policy)


def run_cohort(args, policy: dict) -> int:
    _check_d_primary_mount(args.sync_root)
    if args.command in {"inventory", "partition"}:
        if (args.state_root / "progress.json").exists() or (args.state_root / "inventory.json").exists():
            raise SnapshotError("preserve this cohort inventory/progress; use a fresh state root")
        if args.command == "partition":
            if not args.from_inventory or not args.partition_root:
                raise SnapshotError("partition needs a retained inventory and explicit roots")
            retained = json.loads(real(args.from_inventory).read_text())
            items = []
            for parent in retained["items"]:
                if parent["relative_root"] not in args.partition_root:
                    continue
                directory_names = {r["path"] for r in parent["rows"] if r["kind"] == "directory" and "/" not in r["path"]}
                covered_files = 0
                for name in sorted(directory_names):
                    members = [{**r, "path": r["path"][len(name)+1:]} for r in parent["rows"] if r["path"].startswith(name + "/")]
                    files = [r for r in members if r["kind"] == "file"]
                    covered_files += len(files)
                    items.append({"relative_root": parent["relative_root"] + "/" + name, "state": "inventoried",
                                  "files": len(files), "logical_bytes": sum(r["signature"][2] for r in files),
                                  "rows": members, "fingerprint": identity({"rows": members}),
                                  "newest_mtime_ns": max((r["signature"][3] for r in files), default=0),
                                  "process_references": [], "references_need_fresh_selected_probe": True})
                loose = [r for r in parent["rows"] if "/" not in r["path"] and r["kind"] != "directory"]
                for member in loose:
                    items.append({"relative_root": parent["relative_root"] + "/" + member["path"],
                                  "state": "non-directory-protected", "bytes": member["signature"][2],
                                  "signature": member["signature"], "reason": "parent-loose-file-or-link-needs-exact-file-bundle"})
                if covered_files + sum(r["kind"] == "file" for r in loose) != parent["files"]:
                    raise SnapshotError("partition omits original files; preserve parent")
            inventory = {"schema_version": 1, "origin_node_id": "vastai1T", "authority_node_id": "penguin",
                         "all_scopes_inventoried": False, "partitioned_from": str(args.from_inventory),
                         "captured_at_epoch": retained["captured_at_epoch"], "items": items}
        else:
            inventory = remote(args, {"action": "inventory", "include_roots": args.include_root})
        atomic_write_json(args.state_root / "inventory.json", inventory)
        catalog = {"schema_version": 1, "authority_node_id": "penguin", "archives": []}
        for row in inventory["items"]:
            if row["state"] == "inventoried" and row["files"]:
                catalog["archives"].append({"dataset": dataset_name(row["relative_root"]),
                    "relative_root": row["relative_root"], "minimum_stable_days": 7,
                    "manual_capture_min_stable_hours": policy["minimum_stable_hours"], "durable_staging": False,
                    "archive_only": True, "compression": getattr(args, "archive_compression", DEFAULT_COMPRESSION_PROFILE),
                    "stage_root": str(args.state_root / "encoded")})
        atomic_write_json(args.state_root / "archive-catalog.json", catalog)
        print(json.dumps({"state": "inventory-complete", "all_scopes_inventoried": inventory["all_scopes_inventoried"], "items": len(inventory["items"]),
                          "files": sum(r.get("files", 0) for r in inventory["items"]),
                          "logical_bytes": sum(r.get("logical_bytes", 0) for r in inventory["items"])}))
        return 0
    inventory = json.loads((args.state_root / "inventory.json").read_text())
    specs = load_legacy_specs(args.state_root / "archive-catalog.json")
    specs = {key: replace(spec, compression_profile=getattr(args, "archive_compression", spec.compression_profile))
             for key, spec in specs.items()}
    by_root = {r["relative_root"]: r for r in inventory["items"]}
    ledger = {"state": "dry-run" if not args.apply else "running",
              "items": [{k: v for k, v in r.items() if k != "rows"} for r in inventory["items"]],
              "inventory_path": str(args.state_root / "inventory.json")}
    progress = args.state_root / "progress.json"
    if progress.exists():
        ledger = json.loads(progress.read_text())
        ledger["items"] = [{k: v for k, v in r.items() if k != "rows"} for r in ledger["items"]]
        ledger["state"] = "dry-run" if not args.apply else "running"
    if args.reuse_progress:
        prior = json.loads(real(args.reuse_progress).read_text())
        completed = {r["relative_root"]: r for r in prior["items"] if r.get("state") == "remote-source-retired"}
        for row in ledger["items"]:
            old = completed.get(row["relative_root"])
            if old and old.get("fingerprint") == row.get("fingerprint"):
                proof = verify_cold_archive(specs[dataset_name(row["relative_root"])], args.sync_root,
                                            verification_root=args.state_root / "verification-scratch")
                if (proof["manifest_sha256"] != old.get("manifest_sha256")
                    or remote(args, {"action": "exists", "relative_root": row["relative_root"]})["exists"]):
                    raise SnapshotError("previous completed cohort no longer matches D or original absence")
                row.update(old)
    processed = 0
    for row in ordered_items(ledger["items"], args.order, by_root):
        if args.include_root and row["relative_root"] not in args.include_root:
            continue
        if row.get("state") == "remote-source-retired" or row.get("files", 0) == 0:
            if row.get("files", 0) == 0 and row["state"] == "inventoried":
                row["state"] = "empty-directory-protected"
            continue
        blockers = []
        if row.get("process_references"):
            blockers.append("active-process-reference-at-inventory")
        if any(r["kind"] == "unsupported" or r["cross_filesystem"] for r in by_root[row["relative_root"]]["rows"]):
            blockers.append("unsupported-entry")
        if row["logical_bytes"] > policy["maximum_item_bytes"]:
            blockers.append("root-needs-smaller-explicit-partitions")
        if time.time_ns() - row["newest_mtime_ns"] < policy["minimum_stable_hours"] * 3_600_000_000_000:
            blockers.append("source-not-twelve-hour-stable")
        if blockers:
            row.update(state="source-protected", blockers=blockers)
            continue
        if args.max_items is not None and processed >= args.max_items:
            break
        if not args.apply:
            row["state"] = "would-return-to-d-cold"
            continue
        processed += 1
        ledger["current_root"] = row["relative_root"]
        if row.get("error"):
            atomic_write_json(args.state_root / "attempts" / (
                dataset_name(row["relative_root"]) + "-prior-" + str(time.time_ns()) + ".json"), row)
        row.pop("error", None)
        row.pop("blockers", None)
        write_status(args, ledger)
        started = time.monotonic()
        row["phase_seconds"] = {}
        try:
            archive_one(args, policy, row, specs[dataset_name(row["relative_root"])])
        except Exception as error:
            row["state"] = "cold-verified-needs-audit" if row.get("cold_verified") else "return-failed-source-preserved"
            row["error"] = str(error)[-3000:]
        row["complete_workflow_seconds"] = time.monotonic() - started
        atomic_write_json(args.state_root / "attempts" / (
            dataset_name(row["relative_root"]) + "-" + str(time.time_ns()) + ".json"), row)
        atomic_write_json(args.state_root / "item-status" / (dataset_name(row["relative_root"]) + ".json"), row)
        write_status(args, ledger)
    ledger["state"] = ("batch-complete" if all(r.get("state") == "remote-source-retired" for r in ledger["items"])
                       else "batch-finished-with-protected-items") if args.apply else "dry-run-complete"
    ledger.pop("current_root", None)
    write_status(args, ledger)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
