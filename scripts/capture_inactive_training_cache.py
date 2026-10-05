"""Retry only the enrolled legacy caches through the existing D receiver.

Called by the single bulk coordinator. It does not delete a source, select new
cache namespaces, waive consumer/stability checks, or retain future job caches.
"""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace

from scripts.inventory_remote_training_cache import observe
from stockagent.data_sync.bulk_archive import digest, root_records, atomic_write_json
from stockagent.data_sync.desync_snapshots import SnapshotError, sha256_file
from stockagent.data_sync.offhost_backup import private_file

ROOT = Path(__file__).resolve().parents[1]
POLICY = Path("/etc/stockagent/vast-legacy-cache-capture.json")
STATE = Path("/var/lib/stockagent-vast-bulk-return")


def portable_fingerprint(rows):
    return digest([{ "path": r["path"], "kind": r["kind"], "size": r["signature"][2] if r["kind"] == "file" else 0,
                    "mtime_ns": r["signature"][3], "mode": r["signature"][5] & 0o7777}
                   for r in rows])


def preserved_metadata(batches):
    """Planning evidence only; retirement still rehashes every original."""
    result = {}
    for directory in batches:
        receipt_path, index_path = directory / "cache.receipt.json", directory / "cache.original-index.json"
        if not receipt_path.is_file() or not index_path.is_file():
            continue
        receipt, index = json.loads(receipt_path.read_text()), json.loads(index_path.read_text())
        if (receipt.get("state") != "compressed_transport_received" or receipt.get("producer_exit_code") != 0
                or index.get("compressed_sha256") != receipt.get("compressed_sha256")):
            continue
        for record in root_records(index):
            if not record["directory_root"] or record["unsupported"]:
                continue
            prefix = record["relative_root"] + "/"
            portable = [{"path": r["path"][len(prefix):], "kind": r["kind"],
                         "size": r.get("size", 0), "mtime_ns": r["mtime_ns"], "mode": r["mode"]}
                        for r in record["rows"] if r["path"] != record["relative_root"]]
            result.setdefault(record["relative_root"].partition("/")[2], set()).add(digest(portable))
    return result


def select(inventory, policy, covered, *, now_ns):
    selected, deferred = [], []
    total = 0
    for row in sorted(inventory["caches"], key=lambda r: -r["allocated_unique_file_bytes"]):
        name = row["name"]
        if name not in policy["roots"]:
            continue
        fingerprint = portable_fingerprint(row["rows"])
        reason = None
        if row["service_error"] or row["service_references"] or row["process_references"]:
            reason = "current-consumer-or-incomplete-observation"
        elif fingerprint in covered.get(name, set()):
            continue
        elif fingerprint != policy["roots"][name]:
            reason = "enrolled-legacy-source-changed"
        elif any(r["kind"] not in {"file", "directory"} or r["cross_filesystem"] for r in row["rows"]):
            reason = "unsupported-source"
        elif now_ns - row["newest_mtime_ns"] < 12 * 3600 * 10**9:
            reason = "twelve-hour-source-stability-pending"
        elif total + row["logical_bytes"] > policy["maximum_cohort_bytes"]:
            reason = "bounded-cohort-budget"
        if reason:
            deferred.append({"name": name, "reason": reason})
        else:
            selected.append(name); total += row["logical_bytes"]
    return selected, deferred


def capture(args, batches):
    private_file(POLICY)
    policy = json.loads(POLICY.read_text())
    fields = {"schema_version", "one_shot", "origin_node_id", "authority_node_id", "roots", "maximum_cohort_bytes",
              "measurement_receipt", "measurement_sha256", "node_machine_sha256"}
    if (set(policy) != fields or policy["schema_version"] != 1 or policy["one_shot"] is not True
            or policy["authority_node_id"] != "penguin" or policy["origin_node_id"] != "vastai1T"
            or not isinstance(policy["roots"], dict) or not 0 < policy["maximum_cohort_bytes"] <= 256 * 1024**3
            or any(Path(name).name != name or name in {".", ".."} for name in policy["roots"])):
        raise SnapshotError("legacy cache capture enrollment identity/scope differs")
    measurement_path = Path(policy["measurement_receipt"])
    if measurement_path.resolve() != measurement_path or sha256_file(measurement_path) != policy["measurement_sha256"]:
        raise SnapshotError("measured transport settings changed")
    measurement = json.loads(measurement_path.read_text())
    threads = measurement.get("selected_threads")
    if measurement.get("state") != "measured_remote_transport_accepted" or type(threads) is not int or not 1 <= threads <= 16:
        raise SnapshotError("no accepted bounded transport measurement")
    inventory_path = STATE / "cache-capture-inventory.json"
    observation = observe(SimpleNamespace(output=inventory_path, ssh_target=args.ssh_target,
                                          ssh_port=args.ssh_port, identity_file=args.identity_file))
    if (observation.get("training_only_role_verified") is not True
            or observation["node_profile"]["machine_sha256"] != policy["node_machine_sha256"]
            or observation["node_profile"]["limits"]["cpu_worker_budget"] < threads
            or observation["node_profile"]["limits"]["memory_headroom_bytes"] < 2 * 1024**3):
        raise SnapshotError("capture measurement/role/resource budget no longer matches the actual remote node")
    atomic_write_json(inventory_path, observation)
    selected, deferred = select(observation, policy, preserved_metadata(batches), now_ns=time.time_ns())
    status = {"state": "legacy_cache_capture_deferred" if deferred else "enrolled_legacy_caches_captured",
              "selected_roots": selected, "deferred": deferred, "observed_at_epoch": time.time(),
              "source_deleted": False, "new_cache_namespaces_selected": False}
    atomic_write_json(STATE / "cache-capture-status.json", status)
    if not selected:
        return False
    command = [sys.executable, str(ROOT / "scripts/receive_vast_bulk_archives.py"), "--scope", "cache",
               "--cache-inventory", str(inventory_path), "--threads", str(threads), "--apply",
               "--ssh-target", args.ssh_target, "--ssh-port", str(args.ssh_port), "--identity-file", str(args.identity_file)]
    for name in selected:
        command += ["--include-cache-root", name]
    subprocess.run(command, check=True, cwd=ROOT)
    status.update(state="legacy_cache_transport_received_pending_cold_decode", observed_at_epoch=time.time())
    atomic_write_json(STATE / "cache-capture-status.json", status)
    return True
