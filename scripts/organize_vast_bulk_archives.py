#!/usr/bin/env python3
"""Finish received compressed streams under the existing cold-ingress owner.

Independently decode every original, register two full content-addressed blobs,
wait for real peer delivery, then separately dry-run/apply exact inactive roots.
No hot extraction, remote source archive, service restart or cold-object GC.
"""
from __future__ import annotations
import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.ingest_remote_cold_artifacts import _ssh_base, _validate_ssh_target
from scripts.configure_artifact_ingress_syncthing import credentials
from scripts.manage_packed_edge import _convergence
from stockagent.data_sync.bulk_archive import (
    CONTRACT, INCOMING, SYNC_ROOT, canonical_blobs, digest, index_zstd,
    ingress_owner, native_guard, load_policy, publish_preservation, verify_preservation, root_records, signature,
    atomic_write_json, verify_retained_preservation,
)
from stockagent.data_sync.desync_snapshots import SnapshotError, sha256_file
from stockagent.data_sync.windows_cold_io import metadata_path
from stockagent.data_sync.packed_snapshots import resolve_packed_snapshot_id

COORDINATOR_LOCK = Path("/run/lock/stockagent-vast-bulk-organizer.lock")
COORDINATOR_STATUS = Path("/var/lib/stockagent-vast-bulk-return/status.json")
BATCH_NAME = re.compile(r"[0-9]{8}T[0-9]{6}-[0-9a-f]{12}\Z")


def received_batches():
    """Include future sealed batches, without following arbitrary directories."""
    parent = metadata_path(INCOMING)
    if not parent.exists():
        return []
    return sorted(p for p in parent.iterdir()
                  if BATCH_NAME.fullmatch(p.name) and p.is_dir() and not p.is_symlink())


def _private_control_body():
    body = "import sys,types\n"
    for name, relative in (
        ("stockagent.data_sync.node_roles", "stockagent/data_sync/node_roles.py"),
        ("stockagent.data_sync.artifact_consumers", "stockagent/data_sync/artifact_consumers.py"),
        ("stockagent.data_sync.legacy_artifact_archive", "stockagent/data_sync/legacy_artifact_archive.py"),
        ("stockagent.data_sync.training_return", "stockagent/data_sync/training_return.py"),
        ("stockagent.data_sync.remote_legacy_return", "stockagent/data_sync/remote_legacy_return.py"),
        ("stockagent.data_sync.windows_cold_io", "stockagent/data_sync/windows_cold_io.py"),
        ("stockagent.data_sync.bulk_archive", "stockagent/data_sync/bulk_archive.py"),
        ("scripts.deduplicate_inactive_panel_caches", "scripts/deduplicate_inactive_panel_caches.py"),
        ("stockagent.data_sync.bulk_archive_retirement", "stockagent/data_sync/bulk_archive_retirement.py"),
    ):
        body += f"m=types.ModuleType({name!r});m.__file__={'/root/stockAgent/' + relative!r};sys.modules[{name!r}]=m\n"
        body += f"exec({(ROOT / relative).read_text()!r},m.__dict__)\n"
    return body


def _private_request(args, body, request_identity):
    command = [*_ssh_base(args.identity_file, args.ssh_port), _validate_ssh_target(args.ssh_target),
               "cd /root/stockAgent && source scripts/runtime_env.sh && run_fintech_python -"]
    process = subprocess.run(command, input=body, text=True, capture_output=True, timeout=1800)
    if process.returncode:
        # Configuration parser messages can contain private source fragments.
        # Keep them out of stdout and the published/member-index metadata.
        error_path = ROOT / "artifacts/operations/vast_bulk_return_20261004/retirement-errors" / (
            request_identity + "-" + str(time.time_ns()) + ".json")
        atomic_write_json(error_path, {"stderr": process.stderr, "returncode": process.returncode,
                                      "ack_identity_sha256": request_identity, "observed_at_epoch": time.time()})
        raise SnapshotError("remote preservation retirement rejected; private receipt " + str(error_path))
    return json.loads(process.stdout.splitlines()[-1])


def private_remote(args, ack, *, apply):
    body = _private_control_body()
    body += "import json\nfrom pathlib import Path\nfrom stockagent.data_sync.bulk_archive_retirement import retire\n"
    body += f"ack=json.loads({json.dumps(ack)!r})\n"
    body += f"result=retire(Path('/root/stockAgent'),ack,apply={apply!r})\nprint(json.dumps(result),flush=True)\n"
    return _private_request(args, body, ack["identity_sha256"])


def private_observe_roots(args, records):
    body = _private_control_body()
    body += "import json\nfrom pathlib import Path\nfrom stockagent.data_sync.bulk_archive_retirement import observe_preserved_roots\n"
    body += f"records=json.loads({json.dumps(records)!r})\n"
    body += "result=observe_preserved_roots(Path('/root/stockAgent'),records)\nprint(json.dumps(result),flush=True)\n"
    return _private_request(args, body, digest({"candidate_records": records}))


def verify_retained_proof(proof):
    return verify_retained_preservation(proof)


def retire_roots(args, directory, receipt, index, proof, state):
    records = root_records(index)
    approved = [r["relative_root"] for r in records if r["directory_root"] and not r["unsupported"]]
    if receipt["scope"] == "cache":
        approved = [name for name in approved if name in receipt.get("approved_cache_roots", [])]
    file_names = [r["path"] for r in index["rows"] if r["kind"] == "file"] if receipt["scope"] == "cache" else []
    results = state.setdefault("roots", {})
    candidates = [r for r in records if r["relative_root"] in approved
                  and results.get(r["relative_root"], {}).get("deleted") is not True]
    observation_started = time.perf_counter()
    observations = private_observe_roots(args, candidates) if candidates else {}
    if set(observations) != {r["relative_root"] for r in candidates}:
        raise SnapshotError("candidate observation omitted or added an approved root")
    observation_receipt = {"candidate_only": True, "source_deleted": False,
                           "observed_at_epoch": time.time(),
                           "seconds": time.perf_counter() - observation_started,
                           "dataset": proof.get("dataset"), "snapshot_id": proof.get("snapshot_id"),
                           "roots": observations}
    atomic_write_json(directory / (receipt["scope"] + ".candidate-observation.json"), observation_receipt)
    print(json.dumps({"scope": receipt["scope"], "phase": "read_only_candidate_observation",
                      "roots": len(observations), "eligible_roots": sum(
                          r.get("state") == "metadata-candidate-requires-exact-proof" for r in observations.values()),
                      "seconds": observation_receipt["seconds"]}), flush=True)
    for record in sorted(records, key=lambda r: (-r["logical_bytes"], r["relative_root"])):
        relative = record["relative_root"]
        if results.get(relative, {}).get("deleted") is True:
            continue
        if relative not in approved:
            results[relative] = {"state": "unsupported-or-unapproved-source-preserved", "deleted": False}
            continue
        observed = observations[relative]
        if (observed.get("state") != "metadata-candidate-requires-exact-proof"
                or observed.get("candidate_only") is not True
                or observed.get("deleted") is not False):
            if observed.get("deleted") is not False:
                raise SnapshotError("a read-only candidate observation cannot report deletion")
            results[relative] = observed
            atomic_write_json(directory / (receipt["scope"] + ".organization.json"), state)
            continue
        # Full original decode, anchored to immutable object signatures, remains
        # bounded to thirty minutes. Never refresh its timestamp from stat alone.
        if time.time() - proof["verified_at_epoch"] > 1500:
            files = verify_retained_proof(proof)
            from stockagent.data_sync.cold_primary import d_primary_read_alias
            from stockagent.data_sync.bulk_archive import preservation_payload
            aliases = {name: d_primary_read_alias(path) for name, path in files.items()}
            fresh = index_zstd(preservation_payload(aliases), expected_sha256=proof["compressed_sha256"],
                               scopes={receipt["scope"]})
            if fresh["rows"] != index["rows"]:
                raise SnapshotError("refreshed D originals differ")
            proof["verified_at_epoch"] = time.time()
            atomic_write_json(directory / (receipt["scope"] + ".cold-proof.json"), proof)
        verify_retained_proof(proof)
        base, key = credentials()
        if _convergence(base, key, "stockagent-packed", "vastai1T").get("ok") is not True:
            state["state"] = "cold_verified_waiting_peer"
            atomic_write_json(directory / (receipt["scope"] + ".organization.json"), state)
            return False
        ack = {**{k: proof[k] for k in (
            "dataset", "snapshot_id", "manifest_sha256", "preservation_contract",
            "compressed_sha256", "member_inventory_sha256", "source_scope",
            "cold_verified", "decoded_originals_verified", "verified_at_epoch")},
            "contract": "d_verified_bulk_archive_return_v1", "origin_node_id": "vastai1T",
            "authority_node_id": "penguin", "manual_immediate": True,
            "minimum_stable_hours": 12, "root": record,
            "approved_roots": approved, "cohort_file_names": file_names}
        policy = load_policy()
        if policy.get("shared_file_policy") == "unlink_preserved_names_only":
            ack.update(contract="d_verified_bulk_archive_return_v2",
                       shared_file_policy="unlink_preserved_names_only")
        ack["identity_sha256"] = digest(ack)
        try:
            plan = private_remote(args, ack, apply=False)
            result = plan
            if args.retire and plan["state"] == "would-retire":
                # Read current D authority and exact same immutable objects again
                # immediately before handing the private apply acknowledgement.
                try:
                    with ingress_owner(wait=False):
                        verify_retained_proof(proof)
                        if time.time() - proof["verified_at_epoch"] > 1500:
                            raise SnapshotError("independent recovery proof aged while preparing retirement")
                        result = private_remote(args, ack, apply=True)
                except BlockingIOError:
                    state["state"] = "cold_verified_waiting_retirement_owner"
                    atomic_write_json(directory / (receipt["scope"] + ".organization.json"), state)
                    return False
            results[relative] = result
        except (SnapshotError, subprocess.TimeoutExpired) as error:
            results[relative] = {"state": "source-preserved-needs-audit", "deleted": False,
                                 "error": str(error), "observed_at_epoch": time.time()}
        state["reclaimed_allocated_bytes"] = sum(r.get("reclaimed_allocated_bytes", 0) for r in results.values())
        atomic_write_json(directory / (receipt["scope"] + ".organization.json"), state)
        print(json.dumps({"scope": receipt["scope"], "root": relative,
                          "state": results[relative]["state"],
                          "reclaimed_allocated_bytes": state["reclaimed_allocated_bytes"]}), flush=True)
    state.update(state="cold_preserved_retirement_audited", completed_at_epoch=time.time(),
                 all_sources_retired=bool(approved) and all(results.get(r, {}).get("deleted") is True for r in approved))
    atomic_write_json(directory / (receipt["scope"] + ".organization.json"), state)
    return state["all_sources_retired"] if args.retire else True


def organize(args, directory, receipt):
    scope = receipt["scope"]
    # Independent read-only decoding may overlap a different scope's owned
    # publication. Never let its progress overwrite the retirement journal.
    suffix = ".organization.json" if args.apply else ".verification.json"
    status_path = directory / (scope + suffix)
    state = json.loads(status_path.read_text()) if status_path.exists() else {"schema_version": 1, "scope": scope}
    def progress(row):
        state.update(row, observed_at_epoch=time.time())
        atomic_write_json(status_path, state)
        print(json.dumps({"scope": scope, "phase": state["state"], **row}), flush=True)
    if (state.get("state") == "cold_preserved_retirement_audited"
            and (not args.retire or state.get("all_sources_retired") is True)):
        return True
    index_path = directory / (scope + ".original-index.json")
    if index_path.exists():
        index = json.loads(index_path.read_text())
        if index["compressed_sha256"] != receipt["compressed_sha256"]:
            raise SnapshotError("retained decoded index differs from received bytes")
    else:
        state["state"] = "decoding_all_originals"
        atomic_write_json(status_path, state)
        index = index_zstd(Path(receipt["payload"]), expected_sha256=receipt["compressed_sha256"], scopes={scope}, progress=progress)
        atomic_write_json(index_path, index)
        print(json.dumps({"scope": scope, "state": "all_original_members_decoded",
                          "files": index["files"], "logical_bytes": index["logical_bytes"]}), flush=True)
    if not args.apply:
        state.update(state="received_originals_verified_read_only", files=index["files"],
                     logical_bytes=index["logical_bytes"], compressed_sha256=index["compressed_sha256"],
                     observed_at_epoch=time.time())
        atomic_write_json(status_path, state)
        print(json.dumps({"scope": scope, "state": "verified_read_only_preservation_plan", "retire": False}), flush=True)
        return True
    try:
        state.update(state="originals_verified_queued_for_existing_ingress_owner", observed_at_epoch=time.time())
        atomic_write_json(status_path, state)
        print(json.dumps({"scope": scope, "state": state["state"]}), flush=True)
        # Periodic retries share the publication owner with completed returns.
        # A busy owner must not strand this coordinator on its first batch.
        proof_path = directory / (scope + ".cold-proof.json")
        if proof_path.exists():
            proof = json.loads(proof_path.read_text())
            if not proof.get("cold_verified") or not proof.get("decoded_originals_verified"):
                raise SnapshotError("a publication receipt cannot authorize source retirement")
            verify_retained_proof(proof)
        else:
            publication_path = directory / (scope + ".cold-publication.json")
            if publication_path.exists():
                publication = json.loads(publication_path.read_text())
            else:
                with ingress_owner(wait=False):
                    state["state"] = "publishing_canonical_compressed_cold"
                    atomic_write_json(status_path, state)
                    publication = publish_preservation(directory, receipt, index, repo_root=ROOT,
                                                       progress=progress, defer_recovery=True)
                    atomic_write_json(publication_path, publication)
            state["state"] = "independent_canonical_d_original_recovery"
            atomic_write_json(status_path, state)
            proof = verify_preservation(publication, index, progress=progress)
            atomic_write_json(proof_path, proof)
        state.update(state="canonical_d_originals_verified", cold_proof=str(proof_path),
                     dataset=proof["dataset"], snapshot_id=proof["snapshot_id"],
                     manifest_sha256=proof["manifest_sha256"])
        atomic_write_json(status_path, state)
        print(json.dumps({"scope": scope, "state": state["state"], "dataset": proof["dataset"]}), flush=True)
        if getattr(args, "_preserve_only", False):
            return True
        return retire_roots(args, directory, receipt, index, proof, state)
    except BlockingIOError:
        state.update(state="originals_verified_waiting_existing_ingress_owner", observed_at_epoch=time.time())
        atomic_write_json(status_path, state)
        return False


def run_batches(args):
    while True:
        batches = args.batch or received_batches()
        pending, failed = False, False
        # Received bytes must all become recoverable before slow/private
        # remote cleanup is considered. Keep the same coordinator and journal.
        preserve_first = getattr(args, "apply", False) and getattr(args, "retire", False)
        preservation_args = SimpleNamespace(**vars(args), _preserve_only=True) if preserve_first else args
        retirement_queue = []
        for directory in batches:
            directory = metadata_path(directory)
            if (directory.resolve() != directory or directory.parent != metadata_path(INCOMING)
                    or not BATCH_NAME.fullmatch(directory.name)):
                raise SnapshotError("unapproved or redirected private batch")
            intent = json.loads((directory / "intent.json").read_text())
            scopes = intent.get("scopes")
            if (intent.get("authority_node_id") != "penguin" or intent.get("origin_node_id") != "vastai1T"
                    or not isinstance(scopes, list) or not scopes
                    or len(scopes) != len(set(scopes))
                    or any(scope not in {"cache", "markets", "ablations"} for scope in scopes)):
                raise SnapshotError("batch origin/authority/scope identity differs")
            for scope in scopes:
                path = directory / (scope + ".receipt.json")
                if not path.exists():
                    pending = True
                    continue
                receipt = json.loads(path.read_text())
                if receipt.get('state') == 'transport_failed_partial_retained':
                    # Failed prefixes remain evidence; they are not complete archives.
                    continue
                if receipt.get("state") not in {"compressed_transport_received", "transport_incomplete_source_preserved"}:
                    pending = True
                    continue
                if not receipt.get("completed_at_epoch") or not receipt.get("compressed_sha256"):
                    pending = True
                    continue
                if receipt.get("scope") != scope:
                    raise SnapshotError("received scope differs from its sealed batch")
                try:
                    ready = organize(preservation_args, directory, receipt)
                    pending = not ready or pending
                    if preserve_first and ready:
                        retirement_queue.append((directory, receipt))
                except (SnapshotError, subprocess.TimeoutExpired) as error:
                    atomic_write_json(directory / (scope + ".organization-error.json"),
                                      {"state": "source_preserved_organization_failed", "error": str(error),
                                       "observed_at_epoch": time.time()})
                    print(json.dumps({"scope": scope, "state": "organization_failed_source_preserved", "error": str(error)}), flush=True)
                    failed = True
        for directory, receipt in retirement_queue:
            try:
                pending = not organize(args, directory, receipt) or pending
            except (SnapshotError, subprocess.TimeoutExpired) as error:
                atomic_write_json(directory / (receipt["scope"] + ".organization-error.json"),
                                  {"state": "source_preserved_organization_failed", "error": str(error),
                                   "observed_at_epoch": time.time()})
                failed = True
        if not args.watch:
            # A deferred owner/consumer is not a completed return.
            return 2 if failed else 75 if pending else 0
        if not pending and not failed:
            return 0
        print(json.dumps({"state": "waiting_for_received_payload_or_existing_owner", "batches": len(batches)}), flush=True)
        time.sleep(30)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--batch", action="append", type=Path)
    p.add_argument("--apply", action="store_true")
    p.add_argument("--retire", action="store_true")
    p.add_argument("--watch", action="store_true")
    p.add_argument("--capture-inactive-caches", action="store_true",
                   help="Retry the locally enrolled one-shot legacy cache cohort")
    p.add_argument("--ssh-target", default="root@114.32.64.6")
    p.add_argument("--ssh-port", type=int, default=40032)
    p.add_argument("--identity-file", type=Path, default=Path("/root/.ssh/stockagent_vastai1t_ed25519"))
    args = p.parse_args()
    if socket.gethostname() != "penguin" or args.retire and not args.apply:
        p.error("penguin only; explicit apply required for retirement")
    if args.capture_inactive_caches and (not args.apply or args.watch or args.batch):
        p.error("enrolled cache capture belongs to the single periodic default coordinator")
    native_guard()
    if not args.apply:
        return run_batches(args)
    os.umask(0o077)
    # Protect the retirement journal while decoding outside the shared lock.
    # Read-only decoding writes a different verification journal.
    with COORDINATOR_LOCK.open("a+b") as owner:
        try:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(json.dumps({"state": "existing_bulk_coordinator_busy"}), flush=True)
            return 75
        started = time.perf_counter()
        COORDINATOR_STATUS.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        atomic_write_json(COORDINATOR_STATUS, {"state": "checking_received_cohorts", "observed_at_epoch": time.time()})
        try:
            code = run_batches(args)
            if args.capture_inactive_caches:
                from scripts.capture_inactive_training_cache import capture
                if capture(args, received_batches()):
                    code = run_batches(args)
        except BaseException:
            atomic_write_json(COORDINATOR_STATUS, {"state": "source_preserved_needs_audit", "observed_at_epoch": time.time(),
                                                 "complete_workflow_seconds": time.perf_counter() - started})
            raise
        atomic_write_json(COORDINATOR_STATUS, {"state": "cohorts_checked" if code == 0 else "deferred" if code == 75 else "source_preserved_needs_audit",
                                             "exit_code": code, "observed_at_epoch": time.time(),
                                             "complete_workflow_seconds": time.perf_counter() - started,
                                             "full_history_verified": False})
        return code


if __name__ == "__main__":
    raise SystemExit(main())
