"""Data-only recovery requests, consumed by fixed locally installed relay code.

The source never sends commands or private configuration. The receiver reuses
its backup service and owner; file backup remains independent of this queue.
"""
from __future__ import annotations

from datetime import datetime, timezone
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import time
import uuid

from scripts.backup_delivery_receipt import (
    HASH, atomic_public, read_json, signed, valid_signature, validate_ack, verify_dispatch_delivery,
)
from stockagent.data_sync.backup_auxiliary import verified_control
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.materialized_cache import process_references_many
from stockagent.data_sync.nas_recovery_acceptance import (
    PLAN, run_acceptance, validate_acceptance, validate_plan,
)
from stockagent.data_sync.offhost_backup import _regular, _sha, private_json
from stockagent.runtime_identity import runtime_identity, validate_runtime_lock

STATUS = "nas_semantic_recovery_queue_status_v1"
STATUS_FIELDS = {"contract", "producer_device_id", "receiver_device_id", "repository_id",
    "installed_handoff_identity_sha256", "runtime_lock_verified", "plans", "observed_at_utc",
    "complete_cycle_seconds", "full_history_backup_verified", "identity_sha256"}
IDENTITIES = ("producer_device_id", "receiver_device_id", "repository_id")


def paired(plan: dict, configuration: dict) -> None:
    validate_plan(plan)
    if any(plan[k] != configuration[k] for k in IDENTITIES):
        raise ValueError("recovery request belongs to another relay or repository")


def publish_requests(configuration: dict, transport: Path, ledger: dict) -> dict:
    """Call only under the source stream owner. Only accepted NAS pins are used."""
    policy = configuration.get("automatic_recovery", {})
    if policy.get("enabled") is not True:
        return {"state": "disabled", "published_requests": 0}
    if (set(policy) != {"enabled", "minimum_free_bytes", "maximum_restore_bytes"}
            or any(type(policy.get(k)) is not int or policy[k] <= 0 for k in ("minimum_free_bytes", "maximum_restore_bytes"))):
        raise ValueError("automatic recovery needs an explicit bounded source policy")
    root = transport / "tools/recovery-requests"
    if any(p.is_symlink() for p in (root, *root.parents)):
        raise ValueError("recovery request namespace is redirected")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    known = {}
    for path in sorted(root.glob("plan-*.json")):
        plan = read_json(path)
        paired(plan, configuration)
        if path.name != "plan-" + plan["identity_sha256"] + ".json":
            raise ValueError("immutable request filename differs")
        known[plan["identity_sha256"]] = plan
    new = 0
    if configuration.get("nas_recovery_plan"):
        initial = read_json(Path(configuration["nas_recovery_plan"]))
        paired(initial, configuration)
        destination = root / ("plan-" + initial["identity_sha256"] + ".json")
        atomic_public(destination, initial)
        new += initial["identity_sha256"] not in known
        known[initial["identity_sha256"]] = initial
    logical_states = {j["logical_state_identity_sha256"] for p in known.values()
                      for j in p["jobs"] if j["kind"] == "control"}
    errors = []
    for delivery in ledger["deliveries"].values():
        dispatch, ack = delivery["dispatch"], delivery.get("acceptance")
        if dispatch.get("delivery_kind") != "code_and_control_backup" or not ack:
            continue
        try:
            validate_ack(ack, dispatch)
            if any(dispatch[k] != configuration[k] for k in IDENTITIES):
                raise ValueError("control dispatch differs from this paired relay")
            batch = transport / dispatch["batch_relative"]
            receipt, _, logical = verified_control(batch / "control")
            logical_id = receipt["logical_state_identity_sha256"]
            if logical_id in logical_states:
                continue  # A code-only change does not require another DB restore.
            # Reuse the exact closed envelope before deriving a NEW request.
            verify_dispatch_delivery(batch, dispatch)
            plan = signed({"contract": PLAN, **{k: configuration[k] for k in IDENTITIES},
                "minimum_free_bytes": policy["minimum_free_bytes"],
                "maximum_restore_bytes": policy["maximum_restore_bytes"],
                "full_history_backup_verified": False, "jobs": [{
                    "job_id": "control-" + logical_id[:16], "kind": "control",
                    "nas_snapshot_id": ack["snapshot_id"],
                    **{k: dispatch[k] for k in ("envelope_identity_sha256", "envelope_file_sha256",
                                               "complete_files", "complete_bytes")},
                    "logical_state_identity_sha256": logical_id,
                    "table_count": logical["table_count"], "row_count": logical["row_count"]}]})
            paired(plan, configuration)
            atomic_public(root / ("plan-" + plan["identity_sha256"] + ".json"), plan)
            known[plan["identity_sha256"]] = plan
            logical_states.add(logical_id)
            new += 1
        except (OSError, ValueError, KeyError, TypeError, SnapshotError) as error:
            errors.append({"envelope_identity_sha256": dispatch["envelope_identity_sha256"],
                           "error_type": type(error).__name__})
    return {"state": "requests_published", "published_requests": new,
            "total_requests": len(known), "errors": errors}


def validate_configuration(c: dict) -> None:
    fields = {"schema_version", *IDENTITIES, "request_root", "receipt_root", "state_root", "scratch_root",
        "owner_lock", "nas_configuration", "restic", "password_file", "runtime_lock", "pg_bin",
        "minimum_free_bytes", "maximum_restore_bytes", "maximum_plans_per_cycle", "retry_delays_seconds",
        "installed_handoff_identity_sha256", "windows_distribution", "powershell"}
    if set(c) != fields or c["schema_version"] != 1:
        raise ValueError("recovery queue needs its exact locally installed configuration")
    if not HASH.fullmatch(c["repository_id"]) or not HASH.fullmatch(c["installed_handoff_identity_sha256"]):
        raise ValueError("recovery queue needs fixed repository and installed code identities")
    path_fields = fields - {"schema_version", *IDENTITIES, "minimum_free_bytes", "maximum_restore_bytes",
        "maximum_plans_per_cycle", "retry_delays_seconds", "installed_handoff_identity_sha256", "windows_distribution"}
    for key in path_fields:
        path = Path(c[key])
        if not path.is_absolute() or any(p.is_symlink() for p in (path, *path.parents)):
            raise ValueError("recovery queue path is relative or redirected")
    roots = [Path(c[k]) for k in ("request_root", "receipt_root", "state_root", "scratch_root")]
    if any(a.is_relative_to(b) or b.is_relative_to(a) for i, a in enumerate(roots) for b in roots[i + 1:]):
        raise ValueError("private scratch, state and Syncthing namespaces must be separate")
    for key in ("minimum_free_bytes", "maximum_restore_bytes", "maximum_plans_per_cycle"):
        if type(c[key]) is not int or c[key] <= 0:
            raise ValueError("recovery queue limits must be positive integers")
    if c["maximum_plans_per_cycle"] > 16 or not isinstance(c["windows_distribution"], str) or (
            c["windows_distribution"] and not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", c["windows_distribution"])):
        raise ValueError("recovery queue has unbounded work or invalid Windows identity")
    delays = c["retry_delays_seconds"]
    if not isinstance(delays, list) or not delays or any(type(x) is not int or x < 1 or x > 86400 for x in delays):
        raise ValueError("recovery queue needs bounded retry delays")


def physical_free_bytes(configuration: dict) -> int:
    """Measure the actual WSL VHDX backing drive, not its virtual capacity."""
    distro = configuration["windows_distribution"]
    wsl = "microsoft" in Path("/proc/sys/kernel/osrelease").read_text().lower()
    if not wsl:
        return shutil.disk_usage(configuration["scratch_root"]).free
    if not distro or (os.environ.get("WSL_DISTRO_NAME") and os.environ["WSL_DISTRO_NAME"] != distro):
        raise ValueError("recovery queue must identify its actual WSL distribution")
    # The executable and program are installed locally; the request cannot
    # contain PowerShell, shell syntax, paths or additional command arguments.
    command = r"""& { param($distribution)
$ErrorActionPreference='Stop'
$matches=@(Get-ChildItem 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Lxss' |
  Get-ItemProperty | Where-Object { $_.DistributionName -eq $distribution })
if ($matches.Count -ne 1) { throw 'WSL backing volume not identified' }
$base=$matches[0].BasePath
if ($base.StartsWith('\\?\')) { $base=$base.Substring(4) }
if (-not (Test-Path -LiteralPath (Join-Path $base 'ext4.vhdx'))) { throw 'VHDX not found' }
$drive=[System.IO.DriveInfo]::new([System.IO.Path]::GetPathRoot($base))
@{free_bytes=[int64]$drive.AvailableFreeSpace;distribution=$distribution} | ConvertTo-Json -Compress
}""" + " '" + distro + "'"
    encoded = base64.b64encode(command.encode("utf-16-le")).decode("ascii")
    result = subprocess.run([configuration["powershell"], "-NoProfile", "-NonInteractive", "-EncodedCommand",
                             encoded], capture_output=True, timeout=30)
    if result.returncode:
        raise ValueError("actual Windows backing-drive capacity is unavailable")
    # JSON is UTF-8/ASCII; localized PowerShell stderr may use another Windows
    # code page. Do not decode irrelevant stderr before inspecting exit status.
    value = json.loads(result.stdout.decode("utf-8-sig"))
    if value["distribution"] != distro or type(value["free_bytes"]) is not int or value["free_bytes"] < 0:
        raise ValueError("Windows backing-drive telemetry differs")
    return value["free_bytes"]


def cleanup_success(output: Path, plan: dict, acceptance: dict, journal: Path) -> None:
    """Remove only this journal-owned completed run's scratch; retain evidence."""
    validate_acceptance(acceptance, plan)
    attempt = read_json(journal)
    if (attempt.get("output") != str(output) or attempt.get("plan_identity_sha256") != plan["identity_sha256"]
            or output.name != attempt.get("output_name") or output.is_symlink()
            or not re.fullmatch(plan["identity_sha256"] + r"-[0-9a-f]{32}", output.name)):
        raise ValueError("scratch cleanup lacks the queue's durable creation intent")
    expected = {"cache", "acceptance.private.json", "failure.private.json", *[j["job_id"] for j in plan["jobs"]]}
    if not output.exists():
        return
    if _sha(_regular(output / "acceptance.private.json")) != acceptance["private_evidence_sha256"]:
        raise ValueError("retain recovery scratch whose private proof differs")
    if any(p.name not in expected for p in output.iterdir()):
        raise ValueError("preserve unknown recovery scratch entries")
    if process_references_many([output]):
        raise ValueError("completed recovery scratch is still referenced by a process")
    root_dev = output.stat().st_dev
    rows = []
    for directory, dirs, files in os.walk(output, followlinks=False):
        for name in dirs + files:
            path = Path(directory) / name
            info = path.lstat()
            if info.st_dev != root_dev or stat.S_ISLNK(info.st_mode) or not (
                    stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
                raise ValueError("scratch cleanup rejects redirects, mounts and special files")
            if stat.S_ISREG(info.st_mode):
                if info.st_nlink != 1:
                    raise ValueError("scratch cleanup preserves linked files")
                rows.append({"relative": path.relative_to(output).as_posix(), "bytes": info.st_size,
                             "sha256": _sha(path), "signature": [info.st_dev, info.st_ino,
                                 info.st_size, info.st_mtime_ns, info.st_ctime_ns]})
    evidence = journal.with_name(journal.stem + "-cleanup.json")
    private_json(evidence, {"state": "inventoried", "acceptance_identity_sha256": acceptance["identity_sha256"],
                            "output": str(output), "files": rows})
    if process_references_many([output]):
        raise ValueError("scratch acquired an active process reference")
    if {p.relative_to(output).as_posix() for p in output.rglob("*") if p.is_file()} != {r["relative"] for r in rows}:
        raise ValueError("completed scratch acquired unknown files")
    for row in rows:
        info = _regular(output / row["relative"]).stat()
        if [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns] != row["signature"]:
            raise ValueError("completed scratch changed after cleanup inventory")
    # Keep the compact verification receipts and logs outside disposable data.
    retained = journal.parent / (journal.stem + "-evidence")
    retained.mkdir(mode=0o700, exist_ok=True)
    for row in rows:
        path = output / row["relative"]
        if row["relative"] in {"acceptance.private.json", "failure.private.json"} or (
                "/postgresql/" in row["relative"] and path.name in {
                    "logical-restore.json", "postgres.log", "restore-diagnostics.private.log",
                    "socket-location.private.json", "socket-cleanup.private.json"}):
            target = retained / row["relative"]
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            shutil.copyfile(path, target)
            target.chmod(0o600)
    shutil.rmtree(output)
    private_json(evidence, {"state": "removed_completed_private_scratch", "files": rows,
        "acceptance_identity_sha256": acceptance["identity_sha256"], "output": str(output),
        "ingress_deleted": False, "nas_snapshots_deleted": False})


def run_cycle(configuration: dict) -> dict:
    validate_configuration(configuration)
    c = configuration
    identity = runtime_identity()
    if validate_runtime_lock(read_json(Path(c["runtime_lock"])), identity):
        raise ValueError("installed recovery role differs from its accepted runtime lock")
    started = time.perf_counter()
    state_root, scratch = Path(c["state_root"]), Path(c["scratch_root"])
    for root in (state_root, scratch):
        if not root.is_dir():
            raise ValueError("use the locally installed state and scratch roots")
    lock = _regular(Path(c["owner_lock"]))
    with lock.open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if (os.fstat(owner.fileno()).st_dev, os.fstat(owner.fileno()).st_ino) != (lock.stat().st_dev, lock.stat().st_ino):
            raise ValueError("existing backup owner changed")
        state_file = state_root / "queue-state.json"
        state = read_json(state_file) if state_file.exists() else {"plans": {}}
        paths = sorted(Path(c["request_root"]).glob("plan-*.json"), key=lambda p: (
            state["plans"].get(p.stem.removeprefix("plan-"), {}).get("last_attempt_epoch", 0), p.name))
        if len(paths) > 4096:
            raise ValueError("recovery request count exceeds the installed queue bound")
        results, executed = [], 0
        for path in paths:
            result = {"plan_identity_sha256": path.stem.removeprefix("plan-"), "state": "rejected"}
            try:
                if not HASH.fullmatch(result["plan_identity_sha256"]):
                    continue
                plan = read_json(path)
                paired(plan, c)
                if result["plan_identity_sha256"] != plan["identity_sha256"]:
                    raise ValueError("request filename differs from its fixed content")
                if plan["minimum_free_bytes"] < c["minimum_free_bytes"] or plan["maximum_restore_bytes"] > c["maximum_restore_bytes"]:
                    raise ValueError("request exceeds installed recovery resource limits")
                item = state["plans"].setdefault(plan["identity_sha256"], {"attempts": 0})
                receipt_path = Path(c["receipt_root"]) / ("recovery-acceptance-" + plan["identity_sha256"] + ".json")
                if receipt_path.exists():
                    accepted = read_json(receipt_path)
                    validate_acceptance(accepted, plan)
                    result["state"] = "accepted"
                    if item.get("active_journal"):
                        journal = Path(item["active_journal"])
                        output = Path(read_json(journal)["output"])
                        if journal.parent != state_root or output.parent != scratch:
                            raise ValueError("recovery journal is outside this installed queue")
                        cleanup_success(output, plan, accepted, journal)
                        item.pop("active_journal")
                    item["state"] = "accepted"
                elif executed >= c["maximum_plans_per_cycle"]:
                    result["state"] = "queued"
                elif item.get("next_attempt_epoch", 0) > time.time():
                    result.update(state="retry_wait", next_attempt_epoch=item["next_attempt_epoch"])
                else:
                    item["last_attempt_epoch"] = time.time()
                    if min(shutil.disk_usage(scratch).free, physical_free_bytes(c)) < (
                            c["minimum_free_bytes"] + c["maximum_restore_bytes"]):
                        raise ValueError("independent recovery exceeds real scratch capacity")
                    executed += 1
                    item["attempts"] += 1
                    name = plan["identity_sha256"] + "-" + uuid.uuid4().hex
                    output, journal = scratch / name, state_root / (name + ".json")
                    private_json(journal, {"output_name": name, "output": str(output),
                        "plan_identity_sha256": plan["identity_sha256"], "attempt": item["attempts"]})
                    item["active_journal"] = str(journal)
                    item["state"] = "running"
                    private_json(state_file, state)  # Restart never invents an acceptance.
                    accepted = run_acceptance(plan, nas_configuration=Path(c["nas_configuration"]),
                        restic=Path(c["restic"]), password_file=Path(c["password_file"]),
                        runtime_lock=Path(c["runtime_lock"]), owner_lock=lock, owner_handle=owner,
                        output=output, receipt_root=Path(c["receipt_root"]), pg_bin=Path(c["pg_bin"]))
                    cleanup_success(output, plan, accepted, journal)
                    item.update(state="accepted", next_attempt_epoch=0)
                    item.pop("active_journal", None)
                    result["state"] = "accepted"
            except (OSError, ValueError, KeyError, TypeError, RuntimeError, subprocess.TimeoutExpired) as error:
                result["error_type"] = type(error).__name__
                if result["plan_identity_sha256"] in state["plans"]:
                    item = state["plans"][result["plan_identity_sha256"]]
                    delay = c["retry_delays_seconds"][min(max(item["attempts"] - 1, 0), len(c["retry_delays_seconds"]) - 1)]
                    item.update(state="retry_wait", next_attempt_epoch=time.time() + delay)
                    result.update(state="retry_wait", next_attempt_epoch=item["next_attempt_epoch"])
            results.append(result)
            private_json(state_file, state)
        status = signed({"contract": STATUS, **{k: c[k] for k in IDENTITIES},
            "installed_handoff_identity_sha256": c["installed_handoff_identity_sha256"],
            "runtime_lock_verified": True, "plans": results, "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "complete_cycle_seconds": time.perf_counter() - started, "full_history_backup_verified": False})
        atomic_public(Path(c["receipt_root"]) / "recovery-queue-status.json", status, replace=True)
        return status


def queue_status(configuration: dict, transport: Path, receipts: Path) -> dict:
    """Source-side status. A missing hook stays explicit; file ACKs still count."""
    plans = {}
    for path in sorted((transport / "tools/recovery-requests").glob("plan-*.json")):
        plan = read_json(path)
        paired(plan, configuration)
        plans[plan["identity_sha256"]] = plan
    accepted, errors = [], []
    for key, plan in plans.items():
        path = receipts / ("recovery-acceptance-" + key + ".json")
        if path.exists():
            try:
                receipt = read_json(path)
                validate_acceptance(receipt, plan)
                accepted.append(key)
            except (OSError, ValueError, KeyError, TypeError) as error:
                errors.append({"plan_identity_sha256": key, "error_type": type(error).__name__})
    result = {"state": "awaiting_receiver_hook", "request_count": len(plans), "accepted_count": len(accepted),
              "accepted_plan_identities": accepted, "receipt_errors": errors}
    status_path = receipts / "recovery-queue-status.json"
    if status_path.exists():
        status = read_json(status_path)
        valid_signature(status, STATUS, fields=STATUS_FIELDS)
        if any(status[k] != configuration[k] for k in IDENTITIES) or status["runtime_lock_verified"] is not True:
            raise ValueError("automatic recovery heartbeat belongs to another relay or runtime")
        if (status["full_history_backup_verified"] is not False
                or not isinstance(status["plans"], list) or len(status["plans"]) > 4096
                or not HASH.fullmatch(status["installed_handoff_identity_sha256"])):
            raise ValueError("automatic recovery heartbeat exceeds its public scope")
        for item in status["plans"]:
            if (not isinstance(item, dict) or not {"plan_identity_sha256", "state"} <= set(item)
                    or set(item) - {"plan_identity_sha256", "state", "next_attempt_epoch", "error_type"}
                    or not HASH.fullmatch(item["plan_identity_sha256"])
                    or item["state"] not in {"accepted", "queued", "retry_wait", "rejected"}
                    or ("error_type" in item and not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,79}", item["error_type"]))):
                raise ValueError("automatic recovery status contains non-allowlisted fields")
        observed = datetime.fromisoformat(status["observed_at_utc"])
        age = (datetime.now(timezone.utc) - observed).total_seconds() if observed.tzinfo else float("inf")
        result.update(state="receiver_polling" if -300 <= age <= configuration["readiness_max_age_seconds"]
                      else "receiver_heartbeat_stale", receiver_observed_at_utc=status["observed_at_utc"],
                      receiver_plans=status["plans"])
    return result
