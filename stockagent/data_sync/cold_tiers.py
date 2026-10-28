"""Incremental -> stable cold tiers over the existing immutable byte ABI.

Tier membership is metadata: old manifests and readers keep the same CAS paths
and original bytes. Only explicitly enrolled Penguin maintenance may change
NTFS allocation; it never deletes a payload, creates an encoded second copy,
follows a synchronized command, or makes a NAS backup claim.
"""
from __future__ import annotations

import base64
from collections import deque
from contextlib import contextmanager
from datetime import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import time
from zoneinfo import ZoneInfo

from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_bytes, atomic_write_json

CONTRACT = "stockagent.cold_tiers.v1"
ROOT = Path(__file__).resolve().parents[2]
AUTHORITY = Path("/srv/stockagent-packed")
LOCAL_POLICY = Path("/etc/stockagent/cold-tiers.json")
CONTROL_POLICY = Path("/etc/stockagent/lakehouse-control.json")
PRIVATE_PREFIX = ".local-state/private-history/"
OBJECT = re.compile(r"(?P<private>\.local-state/private-history/)?objects/blobs/(?P<prefix>[0-9a-f]{2})/(?P<sha>[0-9a-f]{64})\.blob\Z")
HASH = re.compile(r"[0-9a-f]{64}\Z")


def identity(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def read_document(path: Path, maximum=4 * 1024**2) -> dict:
    if path.resolve(strict=True) != path or any(p.is_symlink() for p in (path, *path.parents)):
        raise SnapshotError("redirected tier metadata")
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size > maximum:
            raise SnapshotError("tier metadata is not a bounded regular file")
        raw = stream.read(before.st_size + 1)
        after = os.fstat(stream.fileno())
        if ((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
            raise SnapshotError("tier metadata changed during observation")
        named = path.lstat()
        if (named.st_dev, named.st_ino, named.st_size, named.st_mtime_ns, named.st_ctime_ns) != (
                after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise SnapshotError("tier metadata pathname changed during observation")
    if len(raw) > maximum:
        raise SnapshotError("tier metadata exceeds its bound")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise SnapshotError("tier metadata must be an object")
    return value


def load_policy(path=LOCAL_POLICY) -> dict:
    path = Path(path).absolute()
    value = read_document(path)
    if (value.get("contract") != CONTRACT or value.get("schema_version") != 1
            or value.get("authority_node_id") != "penguin"
            or value.get("sync_root") != str(AUTHORITY)
            or value.get("state_root") != str(AUTHORITY / ".local-state/cold-tiers")
            or value.get("compression") != "LZX" or value.get("delete_original_objects") is not False):
        raise SnapshotError("unsupported cold tier policy")
    limits = {"stable_after_hours": (1, 168), "minimum_object_bytes": (65536, 64*1024**2),
        "maximum_cycle_bytes": (1024**2, 64*1024**3), "maximum_cycle_objects": (1, 4096),
        "maximum_cycle_seconds": (10, 1800), "native_batch_bytes": (1024**2, 1024**3),
        "reserve_bytes": (64*1024**3, 1024**4), "baseline_seed_objects_per_cycle": (1, 8192),
        "daily_window_start_hour": (0, 23), "daily_window_end_hour": (1, 24)}
    expected = set(limits) | {"contract", "schema_version", "authority_node_id", "sync_root", "state_root",
        "compression", "delete_original_objects", "minimum_saving_ratio", "daily_window_timezone"}
    if set(value) != expected:
        raise SnapshotError("unknown or missing cold tier policy fields")
    for key, (low, high) in limits.items():
        if type(value.get(key)) is not int or not low <= value[key] <= high:
            raise SnapshotError("invalid cold tier limit: " + key)
    if not (type(value.get("minimum_saving_ratio")) in (int, float)
            and 0.01 <= value["minimum_saving_ratio"] < 1):
        raise SnapshotError("cold tiers require measured positive savings")
    if not value["daily_window_start_hour"] < value["daily_window_end_hour"]:
        raise SnapshotError("cold tier daily window must be bounded")
    ZoneInfo(value["daily_window_timezone"])
    return value


def is_work_window(policy: dict, now: float) -> bool:
    hour = datetime.fromtimestamp(now, ZoneInfo(policy["daily_window_timezone"])).hour
    return policy["daily_window_start_hour"] <= hour < policy["daily_window_end_hour"]


def object_relative(value: str) -> str:
    match = OBJECT.fullmatch(value)
    if match is None or match["prefix"] != match["sha"][:2]:
        raise SnapshotError("tier intent is not a canonical immutable blob")
    return value


def enqueue(sync_root: Path, dataset: str, paths, *, policy_path=LOCAL_POLICY) -> int:
    """Publication hook: bounded metadata only, no payload reads or encoding.

    Absence of local enrollment keeps all other nodes/private scopes unchanged.
    Failed notification leaves a committed source intact; retry is idempotent.
    """
    if sync_root not in {AUTHORITY, AUTHORITY / ".local-state/private-history"} or not policy_path.exists():
        return 0
    info = policy_path.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022
            or policy_path.is_symlink()):
        raise SnapshotError("cold tiers need an operator-owned local policy")
    policy = load_policy(policy_path)
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    _check_d_primary_mount(AUTHORITY)
    incoming = Path(policy["state_root"]) / "incoming"
    if any(p.is_symlink() for p in (incoming, *incoming.parents)):
        raise SnapshotError("redirected tier intent directory")
    incoming.mkdir(parents=True, exist_ok=True)
    if not isinstance(dataset, str) or not 1 <= len(dataset) <= 256:
        raise SnapshotError("invalid tier dataset identity")
    batch, count = [], 0

    def commit():
        nonlocal count
        if not batch:
            return
        body = {"contract": CONTRACT, "dataset": dataset, "objects": batch.copy()}
        digest = identity(body)
        destination = incoming / (digest + ".json")
        done = incoming.parent / "cold" / (digest + ".json")
        if not done.exists() and not destination.exists():
            atomic_write_json(destination, {**body, "identity_sha256": digest, "enqueued_at_epoch": time.time()})
        count += len(batch)
        batch.clear()

    for path in paths:
        if sync_root != AUTHORITY:
            path = PRIVATE_PREFIX + path
        if not OBJECT.fullmatch(path):
            continue
        if path in batch:
            continue
        batch.append(object_relative(path))
        if len(batch) == 4096:
            commit()
    commit()
    return count


def read_intent(path: Path) -> dict:
    value = read_document(path)
    body = {key: value[key] for key in ("contract", "dataset", "objects")}
    if (body["contract"] != CONTRACT or not isinstance(body["objects"], list)
            or not 1 <= len(body["objects"]) <= 4096 or len(set(body["objects"])) != len(body["objects"])
            or value.get("identity_sha256") != identity(body) or path.stem != identity(body)):
        raise SnapshotError("cold tier intent identity differs")
    for name in body["objects"]:
        object_relative(name)
    return value


@contextmanager
def maintenance_owner(policy: dict):
    """Use the existing global publisher/retention inode; never queue work ahead of acquisition."""
    state = Path(policy["state_root"])
    state.mkdir(parents=True, exist_ok=True)
    owner = state / "owner.lock"
    if owner.is_symlink():
        raise SnapshotError("redirected cold tier owner")
    with owner.open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


@contextmanager
def publication_window(rows):
    """Borrow existing exclusive owners without delaying an active reader.

    Transparent encoding preserves bytes but changes NTFS/WSL generations.
    Older full-recovery/catalog owners do not all take a shared retention
    lease. Fence those original control owners too, then exclude publishers,
    leased readers and retirement for just this bounded native batch. Busy
    owners release every partial acquisition and defer the tier cycle.
    """
    policy_info = CONTROL_POLICY.lstat()
    if policy_info.st_uid != 0 or policy_info.st_mode & 0o022:
        raise SnapshotError("cold tier control enrollment is not operator-owned")
    control = read_document(CONTROL_POLICY)
    state = Path(control.get("state_root", ""))
    if (control.get("cold_root") != str(AUTHORITY) or not state.is_absolute()
            or state.resolve(strict=True) != state or not state.is_dir()
            or state == AUTHORITY or AUTHORITY in state.parents):
        raise SnapshotError("cold tier control enrollment differs from its original authority")
    paths = [state / "dataset-versions/owner.lock",
             state / "source-replication-owner.lock", state / "catalog-owner.lock"]
    roots = [AUTHORITY]
    if any(row["path"].startswith(PRIVATE_PREFIX) for row in rows):
        roots.append(AUTHORITY / ".local-state/private-history")
    paths.extend(root / ".local-state/locks/publish-retention-global.lock" for root in roots)
    descriptors, identities = [], []

    def check_owner(path, fd, before):
        fields = ("st_dev", "st_ino", "st_mode", "st_nlink", "st_uid")
        expected = tuple(getattr(before, field) for field in fields)
        if (path.resolve(strict=True) != path
                or tuple(getattr(path.lstat(), field) for field in fields) != expected
                or tuple(getattr(os.fstat(fd), field) for field in fields) != expected):
            raise SnapshotError("cold tier publication owner was replaced")

    try:
        for path in paths:
            before = path.lstat()
            if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                    or before.st_uid != 0 or before.st_mode & 0o022 or path.resolve(strict=True) != path):
                raise SnapshotError("cold tier publication owner differs")
            fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
            descriptors.append(fd)
            check_owner(path, fd, before)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            check_owner(path, fd, before)
            identities.append((path, fd, before))
        yield
        for path, fd, before in identities:
            check_owner(path, fd, before)
        if read_document(CONTROL_POLICY) != control:
            raise SnapshotError("cold tier control enrollment changed during encoding")
    finally:
        for fd in reversed(descriptors):
            os.close(fd)


def seed_existing(policy: dict, *, policy_path=LOCAL_POLICY) -> dict:
    """One resumable metadata-only baseline; new publications have direct intents."""
    state = Path(policy["state_root"])
    cursor = state / "baseline.json"
    previous = read_document(cursor) if cursor.exists() else {"namespace": 0, "prefix": 0, "last": "", "objects_enqueued": 0}
    if previous.get("complete"):
        return previous
    namespace, prefix, last = previous["namespace"], previous["prefix"], previous["last"]
    if (type(namespace) is not int or not 0 <= namespace <= 2 or type(prefix) is not int
            or not 0 <= prefix <= 256 or not isinstance(last, str)
            or (last and re.fullmatch(r"[0-9a-f]{64}\.blob", last) is None)):
        raise SnapshotError("invalid cold baseline cursor")
    selected = []
    while namespace < 2 and len(selected) < policy["baseline_seed_objects_per_cycle"]:
        if prefix == 256:
            namespace, prefix, last = namespace + 1, 0, ""
            continue
        base = PRIVATE_PREFIX if namespace else ""
        directory = AUTHORITY / base / "objects/blobs" / f"{prefix:02x}"
        if directory.is_symlink():
            raise SnapshotError("redirected cold blob prefix")
        names = sorted(os.listdir(directory)) if directory.exists() else []
        for name in names:
            if name <= last:
                continue
            if re.fullmatch(r"[0-9a-f]{64}\.blob", name) is None:
                continue  # Unknown/partial files stay untouched, outside CAS tiering.
            relative = f"{base}objects/blobs/{prefix:02x}/{name}"
            object_relative(relative)
            selected.append(relative)
            last = name
            if len(selected) == policy["baseline_seed_objects_per_cycle"]:
                break
        else:
            prefix, last = prefix + 1, ""
            continue
        break
    enqueue(AUTHORITY, "existing-cold-baseline", selected, policy_path=policy_path)
    value = {"contract": CONTRACT, "namespace": namespace, "prefix": prefix, "last": last,
             "objects_enqueued": previous["objects_enqueued"] + len(selected), "complete": namespace == 2,
             "observed_at_epoch": time.time()}
    atomic_write_json(cursor, value)
    return value


def native_compress(rows: list[dict], policy: dict) -> list[dict]:
    from stockagent.data_sync.windows_cold_io import _windows_process_environment, windows_path
    program = (ROOT / "scripts/windows_cold_tiers.ps1").read_text()
    request = {"minimum_saving_ratio": policy["minimum_saving_ratio"], "objects": [
        {"path": windows_path(AUTHORITY / row["path"]), "sha256": row["sha256"], "bytes": row["bytes"]} for row in rows]}
    completed = subprocess.run(["powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand",
        base64.b64encode(program.encode("utf-16le")).decode()], input=json.dumps(request).encode(),
        capture_output=True, timeout=300, env=_windows_process_environment())
    if completed.returncode:
        diagnostic = Path(policy["state_root"]) / "diagnostics" / f"{time.time_ns()}.private.log"
        atomic_write_bytes(diagnostic, completed.stderr[-16384:], mode=0o600)
        raise SnapshotError("native cold tier encoding/readback failed; originals retained; diagnostics=" + str(diagnostic))
    result = json.loads(completed.stdout.decode("utf-8-sig"))["results"]
    if not isinstance(result, list) or len(result) != len(rows):
        raise SnapshotError("native tier result inventory differs")
    for expected, observed in zip(rows, result):
        if (observed.get("sha256") != expected["sha256"] or observed.get("bytes") != expected["bytes"]
                or observed.get("native_full_sha_verified") is not True
                or observed.get("codec") not in {"none", "LZX", "XPRESS4K", "XPRESS8K", "XPRESS16K"}
                or any(type(observed.get(key)) is not int or observed[key] < 0
                       for key in ("before_allocated_bytes", "after_allocated_bytes"))):
            raise SnapshotError("native original-byte proof differs")
    return result


def consolidate_intent(intent_path: Path, policy: dict, report: dict, *, now: float, apply: bool) -> bool:
    """Process one bounded wave. True requeues unfinished stable members."""
    state = Path(policy["state_root"])
    intent = read_intent(intent_path)
    progress_path = state / "work" / intent_path.name
    progress = read_document(progress_path) if progress_path.exists() else {"completed": {}}
    if (any(key not in intent["objects"] for key in progress["completed"])
            or (progress_path.exists() and progress.get("intent_identity_sha256") != intent["identity_sha256"])):
        raise SnapshotError("tier progress contains an invalid object identity")
    batch, batch_bytes = [], 0
    for name in intent["objects"]:
        path = AUTHORITY / name
        sha = path.stem
        if name in progress["completed"]:
            continue
        if path.resolve(strict=True) != path:
            raise SnapshotError("redirected cold tier payload")
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode):
            raise SnapshotError("cold tier payload is not regular")
        if now - info.st_mtime < policy["stable_after_hours"] * 3600:
            continue
        if info.st_size < policy["minimum_object_bytes"] or info.st_size > policy["maximum_cycle_bytes"]:
            progress["completed"][name] = {"state": "stable_existing_encoding", "bytes": info.st_size}
            continue
        if (report["objects_completed"] + len(batch) >= policy["maximum_cycle_objects"]
                or report["logical_bytes_processed"] + batch_bytes + info.st_size > policy["maximum_cycle_bytes"]
                or (batch and batch_bytes + info.st_size > policy["native_batch_bytes"])):
            break
        batch.append({"path": name, "sha256": sha, "bytes": info.st_size,
                      "inode": info.st_ino, "mode": info.st_mode, "mtime_ns": info.st_mtime_ns})
        batch_bytes += info.st_size
    if batch and apply:
        with publication_window(batch):
            results = native_compress(batch, policy)
            for before, row in zip(batch, results):
                after = (AUTHORITY / before["path"]).lstat()
                if (after.st_ino, after.st_size, after.st_mode, after.st_mtime_ns) != (
                        before["inode"], before["bytes"], before["mode"], before["mtime_ns"]):
                    raise SnapshotError("cold original identity/clock changed during physical encoding")
                progress["completed"][before["path"]] = {"state": "stable_verified", **row}
                report["allocation_bytes_saved"] += max(0, row["before_allocated_bytes"]-row["after_allocated_bytes"])
    report["objects_completed"] += len(batch)
    report["logical_bytes_processed"] += batch_bytes
    if apply:
        progress.update(contract=CONTRACT, intent_identity_sha256=intent["identity_sha256"],
                        observed_at_epoch=time.time())
        atomic_write_json(progress_path, progress)
        if len(progress["completed"]) == len(intent["objects"]):
            destination = state / "cold" / intent_path.name
            atomic_write_json(destination, {"intent": intent, "proof": progress})
            intent_path.unlink()  # Queue metadata only, never an original.
        elif batch:
            return True
    else:
        report["state"] = "dry_run_no_payload_reads_or_changes"
    return False


def cycle(policy: dict, *, policy_path=LOCAL_POLICY, apply=False, force_window=False, seed=False) -> dict:
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    started = time.monotonic()
    now = time.time()
    report = {"contract": CONTRACT, "observed_at_epoch": now, "original_objects_deleted": 0,
              "payload_copies_created": 0, "objects_completed": 0, "logical_bytes_processed": 0,
              "allocation_bytes_saved": 0, "apply": apply, "NAS_compression_claim": False,
              "failed_intents": 0, "intent_waves_attempted": 0}
    if not force_window and not is_work_window(policy, now):
        return {**report, "state": "outside_daily_window"}
    _check_d_primary_mount(AUTHORITY)
    free = os.statvfs(AUTHORITY)
    if free.f_bavail * free.f_frsize < policy["reserve_bytes"]:
        return {**report, "state": "waiting_d_capacity"}
    state = Path(policy["state_root"])
    if state.resolve(strict=False) != state or any(p.is_symlink() for p in state.parents):
        raise SnapshotError("redirected cold tier state")
    with maintenance_owner(policy):
        if seed and apply:
            report["baseline"] = seed_existing(policy, policy_path=policy_path)
        incoming = state / "incoming"
        paths = deque(sorted(incoming.glob("*.json"), key=lambda p: p.stat().st_mtime_ns)) if incoming.exists() else deque()
        while paths:
            intent_path = paths.popleft()
            if (time.monotonic()-started >= policy["maximum_cycle_seconds"]
                    or report["objects_completed"] >= policy["maximum_cycle_objects"]
                    or report["logical_bytes_processed"] >= policy["maximum_cycle_bytes"]
                    or report["intent_waves_attempted"] >= policy["maximum_cycle_objects"]):
                break
            report["intent_waves_attempted"] += 1
            try:
                if consolidate_intent(intent_path, policy, report, now=now, apply=apply):
                    paths.appendleft(intent_path)
            except BlockingIOError:
                report["state"] = "waiting_publication_window"
                break
            except (OSError, ValueError, KeyError, TypeError, SnapshotError, subprocess.SubprocessError) as error:
                # Preserve a bad cohort without acknowledging it or stalling
                # unrelated immutable objects. Every attempt remains bounded.
                report["failed_intents"] += 1
                if apply:
                    atomic_write_json(state / "failures" / intent_path.name, {
                        "contract": CONTRACT, "observed_at_epoch": time.time(),
                        "error_type": type(error).__name__, "private_reason": str(error)[:1024],
                        "original_objects_deleted": 0, "cohort_acknowledged": False})
        report.setdefault("state", "partial_failures_preserved" if report["failed_intents"] else "bounded_cycle_complete")
    report["complete_cycle_seconds"] = time.monotonic()-started
    return report


def status_record(report: dict, policy: dict, previous: dict) -> dict:
    """Keep the last actual work separate from a later outside-window poll."""
    current = {**report, "contract": CONTRACT, "observed_at_epoch": time.time(),
        "schedule": {key: policy[key] for key in ("daily_window_timezone", "daily_window_start_hour",
            "daily_window_end_hour", "stable_after_hours")}}
    previous_work = previous.get("last_work_cycle") if previous.get("contract") == CONTRACT else None
    if report.get("apply") is True and (report.get("intent_waves_attempted", 0) > 0
            or report.get("objects_completed", 0) > 0):
        current["last_work_cycle"] = dict(report)
    elif isinstance(previous_work, dict) and previous_work.get("contract") == CONTRACT:
        current["last_work_cycle"] = previous_work
    return current
