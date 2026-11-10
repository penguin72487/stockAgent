#!/usr/bin/env python3
"""Ingest allowlisted completed artifacts from an index-only SSH peer.

The remote artifact workspace remains mutable and outside Syncthing.  This
durable-node command observes only lifecycle-complete, unused roots, transfers
one exact root into a node-local quarantine, validates it again, then publishes
an immutable content-addressed release into ``stockagent-packed``.  Syncthing
sees only the atomically completed release.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import hashlib
import json
import inspect
import math
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import shutil
import stat
import subprocess
import sys
import time
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.artifact_maintenance import (  # noqa: E402
    automatic_dataset_name,
)
from stockagent.remote_ssh import (  # noqa: E402
    ssh_base as _ssh_base,
    validate_ssh_target as _validate_ssh_target,
    validate_remote_absolute as _validate_remote_absolute,
)
from stockagent.data_sync.cold_artifacts import (  # noqa: E402
    ColdArtifactSpec,
    publish_cold_artifact,
    validate_cold_artifact_source,
)
from stockagent.data_sync.desync_snapshots import (  # noqa: E402
    SnapshotError,
    _fsync_directory,
    _safe_relative_path,
    _utc_iso_from_ns,
    atomic_write_bytes,
    atomic_write_json,
)
from stockagent.data_sync.packed_snapshots import (  # noqa: E402
    resolve_latest_packed,
    resolve_packed_snapshot_id,
    verify_packed_snapshot,
)
from stockagent.data_sync.training_return import admitted, admit_workspace, load_policy, make_ack  # noqa: E402


INGRESS_SCHEMA_VERSION = 1
AUTOMATION_CONTRACT = "cold-return-v4-bounded-confirmations"
MAX_PENDING_RETURNS = 32
MAX_DEFERRED_RETURNS = 16384
INGRESS_OWNER = Path('/run/lock/stockagent-remote-cold-artifact-ingress-cycle.lock')
PUBLICATION_OWNER = Path('/run/lock/stockagent-remote-cold-artifact-ingress.lock')
RETURN_PAYLOAD_SCRATCH = Path('/srv/stockagent-d-volume/stockagent-completed-offline-scratch/remote-return')
E_RETURN_PAYLOAD_SCRATCH = Path('/srv/stockagent-workspace/restore/completed-return-staging')
E_WORKSPACE_MOUNT = Path('/srv/stockagent-workspace')
UNOBSERVABLE_CONSUMERS = "unobservable-consumers-preserve-source"
_SSH_TARGET_RE = re.compile(r"^[A-Za-z0-9_.@:-]+$")
_REMOTE_ABSOLUTE_RE = re.compile(r"^/[A-Za-z0-9_./-]+$")


@contextmanager
def ingress_owner(path: Path):
    """Keep one ingress writer while sharing only actual mutation boundaries."""
    wait = os.environ.get('COLD_ARTIFACT_INGRESS_LOCK_WAIT_SECONDS', '1800')
    if not re.fullmatch(r'[1-9][0-9]*', wait) or int(wait) > 3600:
        raise SnapshotError('ingress owner wait must be between 1 and 3600 seconds')
    if path.is_symlink() or path.parent.resolve() != path.parent:
        raise SnapshotError('ingress owner path is redirected')
    with path.open('a') as owner:
        # Use a bounded blocking waiter in the kernel's lock queue. Polling
        # LOCK_NB can starve behind continuously queued publication workers.
        result = subprocess.run(['flock', '--exclusive', '--wait', wait, str(owner.fileno())],
            pass_fds=(owner.fileno(),), capture_output=True, timeout=int(wait)+5, check=False)
        if result.returncode == 1:
            raise BlockingIOError('existing ingress owner is busy; no source was changed')
        if result.returncode != 0:
            raise SnapshotError('ingress owner acquisition failed; no source was changed')
        try:
            yield
        finally:
            fcntl.flock(owner, fcntl.LOCK_UN)

_REMOTE_DISCOVERY_PROGRAM = r"""
from pathlib import Path, PurePosixPath
import json
import os
import sys
from stockagent.data_sync.artifact_maintenance import (
    artifact_process_references,
    discover_completed_runs,
    newest_activity_ns,
)
from stockagent.data_sync.packed_snapshots import _collect_entries
from stockagent.data_sync.cold_artifacts import ColdArtifactSpec, validate_cold_artifact_source
from stockagent.data_sync.artifact_maintenance import automatic_dataset_name
from stockagent.data_sync.desync_snapshots import SnapshotError, _safe_relative_path

artifact_root = Path(sys.argv[1]).resolve()
scope = sys.argv[2]
scope_root = artifact_root.joinpath(*Path(scope).parts).resolve()
rows = []
if len(sys.argv) > 3:
    selected = _safe_relative_path(sys.argv[3], 'selected remote artifact')
    source = artifact_root.joinpath(*selected.parts)
    if not source.is_relative_to(scope_root) or source.resolve() != source:
        raise SnapshotError('selected remote artifact is outside its real scope')
    candidates = [source]
else:
    candidates = discover_completed_runs(artifact_root, scope)
for source in candidates:
    relative = source.relative_to(artifact_root).as_posix()
    if any(part.startswith('.') for part in Path(relative).parts):
        continue
    try:
        validate_cold_artifact_source(artifact_root, ColdArtifactSpec(
            automatic_dataset_name(relative), relative, None, 8388608, 32, 0,
            'training-lifecycle-v1'))
    except Exception:
        continue
    _entries, source_summary = _collect_entries(source)
    try:
        references = artifact_process_references(source, scope_root)
    except SnapshotError as error:
        if not str(error).startswith('incomplete process-reference scan'):
            raise
        references = ['unobservable-consumers-preserve-source']
    rows.append({
        "relative_root": source.relative_to(artifact_root).as_posix(),
        "files": source_summary["files"],
        "logical_bytes": source_summary["logical_bytes"],
        "portable_fingerprint_sha256": source_summary[
            "portable_fingerprint_sha256"
        ],
        "newest_mtime_ns": newest_activity_ns(source),
        "process_references": references,
    })
print(json.dumps({"schema_version": 1, "candidates": rows}, sort_keys=True))
""".strip()


@dataclass(frozen=True, slots=True)
class RemoteCandidate:
    relative_root: str
    files: int
    logical_bytes: int
    portable_fingerprint_sha256: str
    newest_mtime_ns: int
    process_references: tuple[str, ...]

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "RemoteCandidate":
        relative = _safe_relative_path(
            str(raw.get("relative_root", "")), "remote artifact relative_root"
        ).as_posix()
        files = int(raw.get("files", -1))
        logical_bytes = int(raw.get("logical_bytes", -1))
        portable_fingerprint = str(
            raw.get("portable_fingerprint_sha256", "")
        ).strip()
        newest_mtime_ns = int(raw.get("newest_mtime_ns", -1))
        references = raw.get("process_references", [])
        if files < 1 or logical_bytes < 0 or newest_mtime_ns < 1:
            raise SnapshotError(f"invalid remote artifact inventory: {relative}")
        if not re.fullmatch(r"[0-9a-f]{64}", portable_fingerprint):
            raise SnapshotError(f"invalid remote artifact fingerprint: {relative}")
        if not isinstance(references, list) or not all(
            isinstance(item, str) for item in references
        ):
            raise SnapshotError(f"invalid remote process evidence: {relative}")
        return cls(
            relative_root=relative,
            files=files,
            logical_bytes=logical_bytes,
            portable_fingerprint_sha256=portable_fingerprint,
            newest_mtime_ns=newest_mtime_ns,
            process_references=tuple(references),
        )

    def observation(self) -> tuple[int, int, int]:
        return self.files, self.logical_bytes, self.newest_mtime_ns


def _decode_discovery(stdout: str) -> list[RemoteCandidate]:
    payload: Mapping[str, Any] | None = None
    for line in reversed(stdout.splitlines()):
        try:
            candidate = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, Mapping) and "candidates" in candidate:
            payload = candidate
            break
    if payload is None or int(payload.get("schema_version", -1)) != INGRESS_SCHEMA_VERSION:
        raise SnapshotError("remote artifact discovery did not return schema v1 JSON")
    rows = payload.get("candidates")
    if not isinstance(rows, list):
        raise SnapshotError("remote artifact discovery candidates must be a list")
    result = [RemoteCandidate.from_mapping(row) for row in rows if isinstance(row, Mapping)]
    if len(result) != len(rows):
        raise SnapshotError("remote artifact discovery contains a non-object candidate")
    return result


def _remote_ancestor_reference_override() -> str:
    from stockagent.data_sync.artifact_maintenance import _ancestor_process_references

    # The shared scanner needs the maintenance module's globals, including its
    # /proc readers and fail-closed error handling. Defining it in the SSH
    # script's namespace loses those dependencies even after assigning it back.
    return (
        "import stockagent.data_sync.artifact_maintenance as maintenance\n"
        f"exec({inspect.getsource(_ancestor_process_references)!r}, maintenance.__dict__)\n"
    )


def _run_readonly_ssh_discovery(command: list[str]):
    """Retry only SSH transport failures for the read-only discovery probe.

    Exit 255 can also originate remotely, so retries are bounded and the last
    original exception (including private diagnostics) is preserved.  Do not
    reuse this for transfers, publication or retirement, whose outcomes can be
    ambiguous after a disconnect.
    """
    for attempt in range(3):
        try:
            return subprocess.run(command, check=True, capture_output=True,
                                  text=True, timeout=300)
        except subprocess.CalledProcessError as error:
            if error.returncode != 255 or attempt == 2:
                raise
            time.sleep(attempt + 1)


def discover_remote_artifacts(
    *,
    ssh_target: str,
    ssh_port: int,
    identity_file: Path,
    remote_repo_root: str,
    remote_artifact_root: str,
    scope: str,
    relative_root: str | None = None,
) -> list[RemoteCandidate]:
    target = _validate_ssh_target(ssh_target)
    repo_root = _validate_remote_absolute(remote_repo_root, "remote repo root")
    artifact_root = _validate_remote_absolute(
        remote_artifact_root, "remote artifact root"
    )
    safe_scope = _safe_relative_path(scope, "remote artifact scope").as_posix()
    selected = ""
    if relative_root is not None:
        selected = " " + shlex.quote(_safe_relative_path(relative_root, "selected remote artifact").as_posix())
    from stockagent.data_sync.artifact_maintenance import discover_completed_runs
    # Run the reviewed shared discovery implementation over SSH without
    # mutating the remote working tree (which may have an unfinished merge).
    program = _REMOTE_DISCOVERY_PROGRAM.replace(
        "artifact_root = Path(sys.argv[1]).resolve()",
        inspect.getsource(discover_completed_runs) + "\n" +
        _remote_ancestor_reference_override() +
        "artifact_root = Path(sys.argv[1]).resolve()",
    )
    remote_command = shlex.join(
        [
            "bash",
            "-lc",
            (
                f"cd {shlex.quote(repo_root)} && source scripts/runtime_env.sh && "
                "python_bin=\"$(resolve_fintech_python)\" && "
                f"exec \"$python_bin\" -c {shlex.quote(program)} "
                f"{shlex.quote(artifact_root)} {shlex.quote(safe_scope)}{selected}"
            ),
        ]
    )
    completed = _run_readonly_ssh_discovery(
        [*_ssh_base(identity_file, ssh_port), target, remote_command])
    return _decode_discovery(completed.stdout)


def eligible_candidates(
    candidates: Sequence[RemoteCandidate],
    *,
    include_roots: Sequence[str],
    stable_hours: float,
    now_ns: int | None = None,
    allow_unobservable_copy: bool = False,
) -> list[RemoteCandidate]:
    allowed = {
        _safe_relative_path(value, "remote artifact include root").as_posix()
        for value in include_roots
    }
    if not allowed:
        raise SnapshotError("at least one --include-root is required")
    current_ns = time.time_ns() if now_ns is None else int(now_ns)
    stable_ns = int(max(0.0, float(stable_hours)) * 3_600_000_000_000)
    rows = [
        row
        for row in candidates
        if row.relative_root in allowed
        and (not row.process_references or (allow_unobservable_copy
                                           and row.process_references == (UNOBSERVABLE_CONSUMERS,)))
        and current_ns - row.newest_mtime_ns >= stable_ns
    ]
    return sorted(rows, key=lambda row: (row.newest_mtime_ns, row.relative_root))


def _spec(candidate: RemoteCandidate, stable_hours: float) -> ColdArtifactSpec:
    return ColdArtifactSpec(
        dataset=automatic_dataset_name(candidate.relative_root),
        relative_root=candidate.relative_root,
        maximum_file_bytes=None,
        loose_file_threshold_bytes=8 * 1024 * 1024,
        pack_buckets=32,
        min_stable_hours=max(0.0, float(stable_hours)),
        completion_contract="training-lifecycle-v1",
    )


def _resolved_release(
    sync_root: Path,
    spec: ColdArtifactSpec,
    candidate: RemoteCandidate,
    *,
    verify_content: bool = True,
):
    if type(verify_content) is not bool:
        raise SnapshotError("release content verification must be a boolean")
    head_root = sync_root.resolve() / "heads" / spec.dataset
    if not head_root.exists():
        return None
    resolved = resolve_latest_packed(sync_root, spec.dataset)
    metadata = resolved.manifest.get("metadata", {})
    if metadata.get("artifact_relative_root") != spec.relative_root:
        raise SnapshotError("packed release root disagrees with remote ingress target")
    source = resolved.manifest.get("source", {})
    observed_identity = (
        int(source.get("files", -1)),
        int(source.get("logical_bytes", -1)),
        str(source.get("portable_fingerprint_sha256", "")),
    )
    remote_identity = (
        candidate.files,
        candidate.logical_bytes,
        candidate.portable_fingerprint_sha256,
    )
    if observed_identity != remote_identity:
        raise SnapshotError(
            "completed remote artifact changed after its immutable release; "
            "publish it under a new relative root"
        )
    if verify_content:
        verify_packed_snapshot(sync_root, resolved)
    return resolved


def _staging_artifact_root(
    state_root: Path, origin_node_id: str, spec: ColdArtifactSpec
) -> Path:
    node = re.sub(r"[^A-Za-z0-9_.-]", "-", origin_node_id).strip("-.")
    if not node:
        raise SnapshotError("origin node ID is empty")
    base = state_root.resolve()
    root = base / "ingress-staging" / node / spec.dataset
    for path in (root, root.parent, root.parent.parent):
        if path.is_symlink() or (path.exists() and not path.is_dir()):
            raise SnapshotError("ingress staging root contains a redirected component")
    if root.resolve(strict=False) != root:
        raise SnapshotError("ingress staging root is redirected")
    return root / "artifacts"


def return_staging_workspace(args, required_bytes: int) -> Path:
    """Keep receipts on SSD and admit payload scratch only on an enrolled volume.

    The explicit D namespace is private and excluded from replication. It is
    not an alternate cold authority and cannot silently fall back to C/WSL.
    Existing callers without --staging-root retain their native scratch gate.
    """
    selected = getattr(args, "staging_root", None)
    if selected is None:
        args.state_root.mkdir(parents=True, exist_ok=True)
        admit_workspace(args.state_root, required_bytes)
        return args.state_root
    if selected.absolute() == E_RETURN_PAYLOAD_SCRATCH:
        from stockagent.data_sync import workspace_volume

        if not getattr(args, "prestage_root", None) and not getattr(args, "policy", None):
            raise SnapshotError("E scratch requires the completed-return policy or exact prestage")
        expected = E_RETURN_PAYLOAD_SCRATCH
        if any(path.is_symlink() for path in (expected, expected.parent, E_WORKSPACE_MOUNT)):
            raise SnapshotError("completed return E workspace is redirected")
        # admit() measures both the enrolled ext4 mount and the actual E
        # backing volume, including its own reserved headroom.
        workspace_volume.admit(required_bytes)
        expected.mkdir(parents=True, mode=0o700, exist_ok=True)
        info = expected.stat()
        if (expected.resolve() != expected or info.st_dev != E_WORKSPACE_MOUNT.stat().st_dev
                or info.st_uid != os.geteuid() or (info.st_mode & 0o777) != 0o700):
            raise SnapshotError("completed return E workspace identity differs")
        return expected
    from stockagent.data_sync.cold_primary import _check_d_primary_mount

    canonical = Path("/srv/stockagent-packed")
    expected = RETURN_PAYLOAD_SCRATCH
    if selected.absolute() != expected or any(p.is_symlink() for p in (expected, *expected.parents)):
        raise SnapshotError("completed return staging must use the enrolled private D namespace")
    _check_d_primary_mount(canonical)
    mount = subprocess.run(["findmnt", "-J", "-T", str(expected.parent), "-o", "TARGET,SOURCE,FSTYPE"],
                           capture_output=True, text=True, timeout=10, check=True)
    records = json.loads(mount.stdout).get("filesystems", [])
    if (len(records) != 1 or records[0].get("source") not in {"D:", "D:\\"}
            or records[0].get("fstype") != "9p"):
        raise SnapshotError("completed return scratch is not the enrolled native D volume")
    expected.mkdir(parents=True, mode=0o700, exist_ok=True)
    before = expected.stat()
    space = os.statvfs(expected)
    if (before.st_dev != expected.parent.stat().st_dev
            or expected.stat().st_dev != before.st_dev
            or space.f_bavail * space.f_frsize < required_bytes):
        raise SnapshotError("completed return D workspace lacks guarded capacity")
    return expected


def staging_observation_matches(status: Mapping[str, Any], candidate: RemoteCandidate,
                                *, d_payload_scratch: bool) -> bool:
    """Account for the enrolled DrvFS rsync clock projection, not source drift.

    DrvFS exposes rsync's preserved mtime at whole-second resolution here.
    Keep the original nanoseconds in the origin receipt and require the exact
    remote observation again after transfer. File modes and every original
    payload SHA are still compared by the durable return acknowledgement.
    """
    if (int(status["files"]), int(status["logical_bytes"])) != (candidate.files, candidate.logical_bytes):
        return False
    actual = int(status["newest_mtime_ns"])
    expected = candidate.newest_mtime_ns
    return actual == expected or (d_payload_scratch and actual == expected // 1_000_000_000 * 1_000_000_000)


def _rsync_candidate(
    candidate: RemoteCandidate,
    *,
    ssh_target: str,
    ssh_port: int,
    identity_file: Path,
    remote_artifact_root: str,
    destination_artifact_root: Path,
    compression: str = "none",
    delete_extraneous: bool = True,
) -> None:
    if not isinstance(compression, str) or compression not in {"none", "zstd-1"}:
        raise SnapshotError("unsupported rsync transfer compression profile")
    target = _validate_ssh_target(ssh_target)
    remote_root = _validate_remote_absolute(
        remote_artifact_root, "remote artifact root"
    )
    relative = _safe_relative_path(
        candidate.relative_root, "remote artifact relative_root"
    )
    destination = destination_artifact_root.joinpath(*relative.parts)
    destination.mkdir(parents=True, exist_ok=True)
    ssh_command = shlex.join(_ssh_base(identity_file, ssh_port))
    source = f"{target}:{remote_root}/{relative.as_posix()}/"
    command = [
        "rsync",
        "--archive",
        "--partial",
        "--append-verify",
        "--protect-args",
        "-e",
        ssh_command,
    ]
    if delete_extraneous:
        command.append("--delete-delay")
    if compression == "zstd-1":
        command.extend(("--compress", "--compress-choice=zstd", "--compress-level=1"))
    command.extend((source, str(destination) + "/"))
    subprocess.run(
        command,
        check=True,
        timeout=21_600,
    )


# Identical byte-level proof on the remote and on the enrolled E workspace.
# Reject links and changing files; names/sizes alone do not establish that an
# unarchived staged checkpoint matches the source.
_CONTENT_DIGEST_PROGRAM = r"""
import hashlib, json, os, stat, sys
root = os.path.abspath(sys.argv[1])
if os.path.realpath(root) != root or not os.path.isdir(root):
    raise ValueError('content digest root is absent or redirected')
digest = hashlib.sha256()
files = total = 0
def fail(error):
    raise error
for current, dirs, names in os.walk(root, topdown=True, followlinks=False, onerror=fail):
    dirs.sort()
    names.sort()
    for name in dirs:
        if not stat.S_ISDIR(os.lstat(os.path.join(current, name)).st_mode):
            raise ValueError('content digest rejects linked or special directory')
    for name in names:
        path = os.path.join(current, name)
        before = os.lstat(path)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError('content digest rejects linked or special file')
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
        try:
            first = os.fstat(fd)
            identity = lambda s: (s.st_dev, s.st_ino, s.st_mode, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
            if identity(first) != identity(before):
                raise ValueError('file changed before content digest')
            item = hashlib.sha256()
            while True:
                block = os.read(fd, 4 * 1024 * 1024)
                if not block:
                    break
                item.update(block)
            if identity(os.fstat(fd)) != identity(first) or identity(os.lstat(path)) != identity(first):
                raise ValueError('file changed during content digest')
        finally:
            os.close(fd)
        relative = os.path.relpath(path, root).encode('utf-8', 'surrogateescape')
        digest.update(relative + b'\0' + str(first.st_size).encode('ascii') + b'\0' + item.digest() + b'\n')
        files += 1
        total += first.st_size
print(json.dumps({'files': files, 'logical_bytes': total, 'sha256': digest.hexdigest()}, sort_keys=True))
""".strip()


def _content_digest(command: Sequence[str]) -> dict[str, Any]:
    completed = subprocess.run(command, check=True, capture_output=True, text=True, timeout=21_600)
    for line in reversed(completed.stdout.splitlines()):
        try:
            result = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (isinstance(result, dict) and type(result.get("files")) is int
                and type(result.get("logical_bytes")) is int
                and re.fullmatch(r"[0-9a-f]{64}", str(result.get("sha256") or ""))):
            return result
    raise SnapshotError("content digest did not return valid evidence")


def _fence_e_stage(stage: Path, identity: tuple[int, int], *, physical: bool) -> None:
    from stockagent.data_sync import workspace_volume

    if physical:
        workspace_volume.admit(0)
    else:
        workspace_volume.check(physical=False)
    info = stage.stat()
    if stage.is_symlink() or (info.st_dev, info.st_ino) != identity:
        raise SnapshotError("E return workspace changed during transfer")


def _prestage_on_e(args, policy: Mapping[str, Any]) -> dict[str, Any]:
    """Preserve one exact completed run on E without publishing or retiring it."""
    selected = _safe_relative_path(args.prestage_root, "prestage root").as_posix()
    if not admitted(selected, dict(policy)):
        raise SnapshotError("prestage root is outside the completed-return policy")
    if getattr(args, "staging_root", None) != E_RETURN_PAYLOAD_SCRATCH:
        raise SnapshotError("prestage requires the enrolled E return workspace")
    discovery = dict(ssh_target=args.ssh_target, ssh_port=args.ssh_port,
        identity_file=args.identity_file, remote_repo_root=args.remote_repo_root,
        remote_artifact_root=args.remote_artifact_root,
        scope=PurePosixPath(selected).parts[0], relative_root=selected)
    found = discover_remote_artifacts(**discovery)
    eligible = eligible_candidates(found, include_roots=[selected],
        stable_hours=float(policy["stable_hours"]), allow_unobservable_copy=True)
    if len(eligible) != 1 or eligible[0].relative_root != selected:
        raise SnapshotError("exact completed prestage source is not eligible")
    candidate = eligible[0]
    if candidate.files > policy["maximum_run_files"] or candidate.logical_bytes > policy["maximum_run_bytes"]:
        raise SnapshotError("prestage source exceeds completed-return policy bounds")
    spec = _spec(candidate, float(policy["stable_hours"]))
    stage = return_staging_workspace(args, 2 * candidate.logical_bytes)
    stage_stat = stage.stat()
    stage_identity = (stage_stat.st_dev, stage_stat.st_ino)
    generation = hashlib.sha256(json.dumps({"root": selected,
        "files": candidate.files, "bytes": candidate.logical_bytes,
        "portable": candidate.portable_fingerprint_sha256,
        "newest_mtime_ns": candidate.newest_mtime_ns}, sort_keys=True).encode()).hexdigest()
    generation_root = stage / generation
    if generation_root.is_symlink():
        raise SnapshotError("E prestage generation is redirected")
    generation_root.mkdir(mode=0o700, exist_ok=True)
    if generation_root.resolve() != generation_root or generation_root.stat().st_dev != stage.stat().st_dev:
        raise SnapshotError("E prestage generation escaped its workspace")
    node = re.sub(r"[^A-Za-z0-9_.-]", "-", args.origin_node_id).strip("-.")
    lexical_root = generation_root / "ingress-staging" / node / spec.dataset / "artifacts"
    for parent in (lexical_root, *lexical_root.parents):
        if parent == generation_root:
            break
        if parent.is_symlink():
            raise SnapshotError("E prestage artifact path contains a symlink")
    artifact_root = _staging_artifact_root(generation_root, args.origin_node_id, spec)
    if artifact_root != lexical_root or not artifact_root.is_relative_to(generation_root):
        raise SnapshotError("E prestage artifact path is redirected")
    cursor = artifact_root
    for part in PurePosixPath(selected).parts:
        cursor = cursor / part
        if cursor.is_symlink() or (cursor.exists() and not cursor.is_dir()):
            raise SnapshotError("E prestage source destination is redirected")
    directory = args.state_root / "ingress-prestage-receipts"
    directory.mkdir(parents=True, mode=0o700, exist_ok=True)
    receipt_path = directory / f"{spec.dataset}-{generation}.json"
    if directory.is_symlink() or receipt_path.is_symlink():
        raise SnapshotError("prestage receipt path is redirected")
    previous = json.loads(receipt_path.read_bytes()) if receipt_path.exists() else None
    if previous is None:
        cycle_progress(args, "prestage-e-transfer", candidate)
        _rsync_candidate(candidate, ssh_target=args.ssh_target, ssh_port=args.ssh_port,
            identity_file=args.identity_file, remote_artifact_root=args.remote_artifact_root,
            destination_artifact_root=artifact_root, compression=args.transfer_compression,
            delete_extraneous=False)
    _fence_e_stage(stage, stage_identity, physical=True)
    local = validate_cold_artifact_source(artifact_root, spec)
    if not staging_observation_matches(local, candidate, d_payload_scratch=False):
        raise SnapshotError("E staged lifecycle inventory differs from origin")
    from stockagent.data_sync.packed_snapshots import _collect_entries
    _, inventory = _collect_entries(Path(local["source"]))
    if inventory["portable_fingerprint_sha256"] != candidate.portable_fingerprint_sha256:
        raise SnapshotError("E staged names or sizes differ from origin")
    cycle_progress(args, "prestage-e-content-proof", candidate)
    remote_root = _validate_remote_absolute(args.remote_artifact_root, "remote artifact root")
    source = str(PurePosixPath(remote_root).joinpath(*PurePosixPath(selected).parts))
    remote_command = shlex.join(["python3", "-c", _CONTENT_DIGEST_PROGRAM, source])
    remote_digest = _content_digest([*_ssh_base(args.identity_file, args.ssh_port),
        _validate_ssh_target(args.ssh_target), remote_command])
    local_digest = _content_digest([sys.executable, "-c", _CONTENT_DIGEST_PROGRAM, local["source"]])
    if remote_digest != local_digest or local_digest["files"] != candidate.files or local_digest["logical_bytes"] != candidate.logical_bytes:
        raise SnapshotError("E staged file SHA256 evidence differs from origin")
    current = discover_remote_artifacts(**discovery)
    if len(current) != 1 or current[0].observation() != candidate.observation() or current[0].portable_fingerprint_sha256 != candidate.portable_fingerprint_sha256 or (current[0].process_references and current[0].process_references != (UNOBSERVABLE_CONSUMERS,)):
        raise SnapshotError("prestage source changed or became active during transfer")
    _fence_e_stage(stage, stage_identity, physical=False)
    receipt = {"schema_version": 1, "contract": "completed_training_e_prestage_v1",
        "state": "staged_not_cold_published", "relative_root": selected,
        "dataset": spec.dataset, "source_generation": generation,
        "staged_artifact_root": str(artifact_root), "staged_source": local["source"],
        "source_observation": {"files": candidate.files, "logical_bytes": candidate.logical_bytes,
            "portable_fingerprint_sha256": candidate.portable_fingerprint_sha256,
            "newest_mtime_ns": candidate.newest_mtime_ns},
        "content_sha256_proof": local_digest, "recorded_at": _utc_iso_from_ns(time.time_ns()),
        "cold_published": False, "source_retired": False, "simulation_promotion": False}
    if previous is not None:
        if not isinstance(previous, dict) or any(previous.get(key) != receipt.get(key)
                for key in receipt if key != "recorded_at"):
            raise SnapshotError("existing E prestage receipt disagrees with current source")
        receipt = previous
    else:
        atomic_write_json(receipt_path, receipt)
    return {**receipt, "receipt_path": str(receipt_path), "reused": previous is not None}


def remote_retirement(args, ack: dict, policy: dict, *, apply: bool) -> dict:
    """Fixed SSH control only; preserve remote Git/conflicted checkout files."""
    module = REPO_ROOT / "stockagent/data_sync/training_return.py"
    payload = {"ack": ack, "policy": policy, "artifact_root": args.remote_artifact_root,
               "repo_root": args.remote_repo_root, "state_root": "/var/lib/stockagent-training-return",
               "apply": apply}
    program = module.read_text().replace("from __future__ import annotations", "")
    from stockagent.data_sync.artifact_retirement import _reclaimable_file_bytes
    from stockagent.data_sync.materialized_cache import process_references_many
    program = program.replace("from stockagent.data_sync.artifact_retirement import _reclaimable_file_bytes",
                              inspect.getsource(_reclaimable_file_bytes))
    program = program.replace("from stockagent.data_sync.materialized_cache import process_references_many",
                              "from stockagent.data_sync.materialized_cache import _path_is_under\n" +
                              "from typing import Iterable\n" + inspect.getsource(process_references_many))
    program += "\n" + _remote_ancestor_reference_override()
    program += "\nrequest = json.loads(" + repr(json.dumps(payload)) + ")\n"
    program += '''
artifact = Path(request['artifact_root'])
state = _real(Path(request['state_root']))
state.mkdir(mode=0o700, parents=True, exist_ok=True)
plan = plan_retirement(artifact, state, Path(request['repo_root']), request['ack'], request['policy'])
result = {'state': 'retirement_blocked' if plan['blockers'] else 'would_retire_verified_source',
          'deleted': False, 'relative_root': plan['relative_root'],
          'plan_fingerprint': plan['plan_fingerprint'], 'blockers': plan['blockers'],
          'service_dependency_error': plan['service_dependency_error'],
          'hot_retention': plan['hot_retention'],
          'reclaimable_allocated_bytes': plan['allocated_bytes']}
if request['apply'] and not plan['blockers']:
    result = apply_retirement(artifact, state, Path(request['repo_root']), request['ack'],
                              request['policy'], plan['plan_fingerprint'])
print(json.dumps(result, sort_keys=True))
'''
    target = _validate_ssh_target(args.ssh_target)
    repo = _validate_remote_absolute(args.remote_repo_root, "remote repo root")
    _validate_remote_absolute(args.remote_artifact_root, "remote artifact root")
    command = f"cd {shlex.quote(repo)} && source scripts/runtime_env.sh && run_fintech_python -"
    result = subprocess.run([*_ssh_base(args.identity_file, args.ssh_port), target, command],
                            input=program, capture_output=True, text=True, timeout=1800)
    if result.returncode:
        evidence = args.state_root / "training-return" / (ack["identity_sha256"] + ".control-error.private.log")
        atomic_write_bytes(evidence, result.stderr.encode(), mode=0o600)
        return {"state": "remote-retirement-failed", "deleted": False,
                "remote_exit_code": result.returncode, "diagnostics": str(evidence)}
    for line in reversed(result.stdout.splitlines()):
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and "deleted" in value:
            return value
    raise SnapshotError("remote returned-source control lacks a machine receipt")


def _read_waiting_marker(path: Path, policy: dict) -> dict:
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise SnapshotError("training return waiting marker is redirected")
    if not path.is_file() or path.stat().st_size > 8192:
        raise SnapshotError("training return waiting marker is not a bounded regular file")
    value = json.loads(path.read_text())
    if (not isinstance(value, dict)
            or set(value) != {"relative_root", "dataset", "snapshot_id", "manifest_sha256"}
            or any(not isinstance(v, str) for v in value.values())
            or not admitted(value["relative_root"], policy)
            or value["dataset"] != automatic_dataset_name(value["relative_root"])
            or not re.fullmatch(r"[0-9a-f]{64}", value["manifest_sha256"])):
        raise SnapshotError("invalid fixed training return waiting marker")
    _safe_relative_path(value["snapshot_id"], "waiting returned snapshot")
    return value


def _pending_name(wave: dict) -> str:
    return hashlib.sha256(json.dumps(wave, sort_keys=True, separators=(",", ":")).encode()).hexdigest() + ".json"


def _waiting_entries(state_root: Path, policy: dict) -> list[tuple[Path, dict]]:
    """Read a bounded private queue, including the original single-wave marker."""
    directory = state_root / "training-return-pending"
    if any(p.is_symlink() for p in (directory, *directory.parents)):
        raise SnapshotError("training return waiting marker directory is redirected")
    paths = []
    if directory.exists():
        if not directory.is_dir():
            raise SnapshotError("invalid training return waiting marker directory")
        for path in directory.iterdir():
            if len(paths) >= MAX_PENDING_RETURNS or not re.fullmatch(r"[0-9a-f]{64}\.json", path.name):
                raise SnapshotError("training return waiting marker queue exceeds its bound or identity")
            paths.append(path)
    legacy = state_root / "training-return-waiting-peer.json"
    if legacy.exists() or legacy.is_symlink():
        paths.append(legacy)
    if len(paths) > MAX_PENDING_RETURNS:
        raise SnapshotError("training return waiting marker queue exceeds its bound")
    entries, roots = [], set()
    for path in sorted(paths, key=lambda p: p.lstat().st_mtime_ns):
        wave = _read_waiting_marker(path, policy)
        if ((path != legacy and path.name != _pending_name(wave))
                or wave["relative_root"] in roots):
            raise SnapshotError("training return waiting marker identity conflicts")
        roots.add(wave["relative_root"])
        entries.append((path, wave))
    return entries


def waiting_returns(state_root: Path, policy: dict) -> list[dict]:
    return [wave for _, wave in _waiting_entries(state_root, policy)]


def _deferred_entries(state_root: Path, policy: dict) -> list[tuple[Path, dict]]:
    """Keep converged but unrestored waves outside the transport backlog."""
    directory = state_root / 'training-return-deferred'
    if any(p.is_symlink() for p in (directory, *directory.parents)):
        raise SnapshotError('deferred training return directory is redirected')
    if not directory.exists():
        return []
    if not directory.is_dir():
        raise SnapshotError('invalid deferred training return directory')
    entries, roots = [], set()
    for path in directory.iterdir():
        if len(entries) >= MAX_DEFERRED_RETURNS or not re.fullmatch(r'[0-9a-f]{64}\.json', path.name):
            raise SnapshotError('deferred training return queue exceeds its bound or identity')
        wave = _read_waiting_marker(path, policy)
        if path.name != _pending_name(wave) or wave['relative_root'] in roots:
            raise SnapshotError('deferred training return marker identity conflicts')
        roots.add(wave['relative_root'])
        entries.append((path, wave))
    return sorted(entries, key=lambda item: item[0].lstat().st_mtime_ns)


def _move_waiting_marker(path: Path, wave: dict, policy: dict, destination: Path) -> Path:
    """Durably move one fixed marker; interruption may leave two equal links."""
    if _read_waiting_marker(path, policy) != wave:
        raise SnapshotError('training return marker changed before queue handoff')
    if any(p.is_symlink() for p in (destination, *destination.parents)):
        raise SnapshotError('training return queue destination is redirected')
    destination.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = destination.stat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o022:
        raise SnapshotError('training return queue destination is not private')
    target = destination / _pending_name(wave)
    try:
        os.link(path, target, follow_symlinks=False)
    except FileExistsError:
        if _read_waiting_marker(target, policy) != wave:
            raise SnapshotError('training return queue target identity differs') from None
    if _read_waiting_marker(target, policy) != wave or _read_waiting_marker(path, policy) != wave:
        raise SnapshotError('training return marker changed during queue handoff')
    _fsync_directory(destination)
    path.unlink()
    _fsync_directory(path.parent)
    return target


def _resume_deferred_returns(state_root: Path, policy: dict) -> None:
    """Requeue preserved markers when the user permits restoration again."""
    pending = _waiting_entries(state_root, policy)
    for path, wave in _deferred_entries(state_root, policy):
        existing = next(((old_path, old) for old_path, old in pending
                         if old['relative_root'] == wave['relative_root']), None)
        if existing is not None and existing[1] != wave:
            raise SnapshotError('pending and deferred return generations conflict')
        if existing is not None:
            _fsync_directory(existing[0].parent)
            path.unlink()
            _fsync_directory(path.parent)
            continue
        if len(pending) >= MAX_PENDING_RETURNS:
            break
        target = _move_waiting_marker(path, wave, policy, state_root/'training-return-pending')
        pending.append((target, wave))


def waiting_return(state_root: Path, policy: dict) -> dict | None:
    """Compatibility projection: the oldest exact pending release, if any."""
    waves = waiting_returns(state_root, policy)
    return waves[0] if waves else None


def _enroll_return(args, wave: dict, policy: dict) -> Path:
    entries = _waiting_entries(args.state_root, policy)
    for path, previous in entries:
        if previous["relative_root"] == wave["relative_root"]:
            if previous != wave:
                raise SnapshotError("a different exact release for this training run is awaiting confirmation")
            return path
    if any(previous['relative_root'] == wave['relative_root']
           for _, previous in _deferred_entries(args.state_root, policy)):
        raise SnapshotError('this training return is awaiting deferred recovery')
    if len(entries) >= MAX_PENDING_RETURNS:
        raise SnapshotError("training return confirmation queue is full")
    path = args.state_root / "training-return-pending" / _pending_name(wave)
    if args.apply:
        atomic_write_json(path, wave)
    return path


def _clear_return(path: Path, wave: dict, policy: dict) -> None:
    if _read_waiting_marker(path, policy) != wave:
        raise SnapshotError("training return waiting wave changed during confirmation")
    path.unlink()


def _resolve_waiting_release(args, wave):
    """Use a retired wave's exact physical successor only with fixed evidence."""
    try:
        held = resolve_packed_snapshot_id(args.sync_root, wave['dataset'], wave['snapshot_id'])
    except FileNotFoundError:
        # Do not reinterpret a still-present but damaged release as a repack.
        original = args.sync_root/'manifests'/wave['dataset']/(wave['snapshot_id']+'.json')
        if os.path.lexists(original):
            raise
        from stockagent.data_sync import remote_return_repack, workspace_volume
        workspace_volume.check()
        held = remote_return_repack.resolve(args.sync_root, E_WORKSPACE_MOUNT, wave)
        return held, True
    if held.manifest_sha256 != wave['manifest_sha256']:
        raise SnapshotError('waiting training release changed')
    return held, False


def retry_training_layout(args) -> dict:
    """Retry one private enrolled layout using this existing owner, never Syncthing commands."""
    plan_path = os.environ.get("COLD_ARTIFACT_INGRESS_LAYOUT_PLAN", "").strip()
    if not plan_path:
        return {"state": "not-enrolled"}
    plan_path = _validate_remote_absolute(plan_path, "remote fixed layout plan")
    if not plan_path.startswith("/var/lib/stockagent-training-layout/"):
        raise SnapshotError("layout plan must be privately enrolled on the origin")
    repo = _validate_remote_absolute(args.remote_repo_root, "remote repo root")
    target = _validate_ssh_target(args.ssh_target)
    command = (f"cd {shlex.quote(repo)} && source scripts/runtime_env.sh && run_fintech_python "
               f"scripts/reorganize_training_storage.py --plan {shlex.quote(plan_path)} "
               "--state /var/lib/stockagent-training-layout/state --maximum-moves 32")
    try:
        completed = subprocess.run([*_ssh_base(args.identity_file, args.ssh_port), target, command],
                                   capture_output=True, text=True, timeout=120)
    except (OSError, subprocess.SubprocessError) as error:
        result = {"state": "retry-failed", "error_type": type(error).__name__,
                  "observed_at_epoch": time.time()}
        atomic_write_json(args.state_root / "remote-training-layout-status.json", result)
        return result
    for line in reversed(completed.stdout.splitlines()):
        try:
            result = json.loads(line)
        except json.JSONDecodeError:
            continue
        if completed.returncode == 0 and isinstance(result, dict) and "plan_identity" in result:
            atomic_write_json(args.state_root / "remote-training-layout-status.json", result)
            return result
    # Layout retries must not stop an independent backup wave. Keep an
    # explicit failure for the next timer; never report it as a successful move.
    result = {"state": "retry-failed", "remote_exit_code": completed.returncode,
              "observed_at_epoch": time.time()}
    atomic_write_json(args.state_root / "remote-training-layout-status.json", result)
    return result


def remote_cache_pressure(args) -> dict:
    """Run only the fixed expendable-cache policy on the enrolled SSH peer."""
    result = {'contract':'stockagent.remote_cache_pressure_observation.v1',
              'state':'waiting', 'observed_at_epoch':time.time()}
    script = REPO_ROOT/'scripts/reclaim_unused_vscode_servers.py'
    try:
        if script.is_symlink() or not script.is_file() or script.stat().st_size > 128*1024:
            raise SnapshotError('remote cache policy source is not a bounded regular file')
        program = ("scope={'__name__':'stockagent_remote_cache_policy'}\n"
                   f"exec(compile({script.read_text()!r}, 'cache_policy', 'exec'), scope)\n"
                   "import json\nprint(json.dumps(scope['reclaim_under_pressure'](), sort_keys=True))\n")
        completed = subprocess.run([*_ssh_base(args.identity_file, args.ssh_port),
            _validate_ssh_target(args.ssh_target), 'python3 -'], input=program,
            capture_output=True, text=True, timeout=60, check=False)
        if completed.returncode != 0:
            return {**result, 'error_type':'remote_exit_nonzero'}
        for line in reversed(completed.stdout.splitlines()):
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if (isinstance(value, dict) and value.get('contract') == 'stockagent.remote_cache_pressure_reclaim.v1'
                    and value.get('state') in {'checked','partial','not_needed'}
                    and type(value.get('removed_allocated_bytes')) is int
                    and value['removed_allocated_bytes'] >= 0
                    and type(value.get('profile_issues', 0)) is int
                    and 0 <= value.get('profile_issues', 0) <= 2):
                return {**result, 'state':value['state'],
                        'removed_allocated_bytes':value['removed_allocated_bytes'],
                        'profile_issues':value.get('profile_issues', 0),
                        'available_before_bytes':value.get('available_before_bytes'),
                        'available_after_bytes':value.get('available_after_bytes'),
                        'threshold_bytes':value.get('threshold_bytes')}
    except (OSError, ValueError, SnapshotError, subprocess.SubprocessError) as error:
        return {**result, 'error_type':type(error).__name__}
    return {**result, 'error_type':'invalid_remote_receipt'}


def returned_source(args, candidate, resolved, policy, *, retire_source: bool = True,
                    pending_original: dict | None = None) -> dict:
    if policy is None:
        return {}
    from stockagent.data_sync.cold_primary import verify_cold_resilience
    resilience = verify_cold_resilience(args.sync_root, resolved, REPO_ROOT / "configs/data_sync/packed_backup.json")
    if not (resilience.get("cold_primary_verified") or resilience.get("backup_verified")):
        raise SnapshotError("returned artifact cold authority is not ready")
    wave = {"relative_root": candidate.relative_root, "dataset": resolved.manifest["dataset"],
            "snapshot_id": resolved.manifest["snapshot_id"], "manifest_sha256": resolved.manifest_sha256}
    marker_wave = wave
    if pending_original is None:
        waiting_path = _enroll_return(args, wave, policy)
    else:
        fixed, repacked = _resolve_waiting_release(args, pending_original)
        if (not repacked or fixed.manifest_sha256 != resolved.manifest_sha256
                or fixed.manifest['snapshot_id'] != resolved.manifest['snapshot_id']
                or pending_original['relative_root'] != candidate.relative_root):
            raise SnapshotError('pending training repack binding changed before confirmation')
        matches = [(path, old) for path, old in _waiting_entries(args.state_root, policy)
                   if old == pending_original]
        if len(matches) != 1:
            raise SnapshotError('original pending training marker changed')
        waiting_path, marker_wave = matches[0]
    from scripts.configure_artifact_ingress_syncthing import credentials
    from scripts.manage_packed_edge import _convergence
    base, key = credentials()
    convergence = _convergence(base, key, "stockagent-packed", policy["origin_node_id"])
    if not convergence["ok"]:
        return {"deleted": False, "state": "waiting-packed-peer-convergence",
                "transport_checks": convergence["checks"]}
    # A recent source is a cheap negative gate. It is already preserved and
    # delivered, but needs no repeated full D decode or remote hashing during
    # its seven-day hot wait. Expiry only re-enables the ordinary full proof;
    # the remote canonical completion timestamp is checked again before unlink.
    minimum_hours = load_policy(policy)["minimum_hot_retention_hours"]
    retain_until = candidate.newest_mtime_ns / 1_000_000_000 + minimum_hours * 3600
    if retire_source and time.time() < retain_until:
        if args.apply:
            _clear_return(waiting_path, marker_wave, policy)
        return {"deleted": False, "state": "waiting-training-hot-retention",
                "retain_until_epoch": retain_until, "minimum_hot_retention_hours": minimum_hours,
                "cold_recovery_reused_or_claimed": False}
    if os.environ.get('STOCKAGENT_DEFER_OPTIONAL_RETURN_RESTORE') == '1':
        # The current user-requested migration prioritizes finishing the new
        # layout over an additional decode/restore. Preserve the exact marker
        # in the deferred queue; never manufacture a recovery or deletion ACK.
        if args.apply:
            deferred_dir = args.state_root/'training-return-deferred'
            if (len(_deferred_entries(args.state_root, policy)) >= MAX_DEFERRED_RETURNS
                    and not os.path.lexists(deferred_dir/_pending_name(marker_wave))):
                raise SnapshotError('deferred training return queue is full')
            _move_waiting_marker(waiting_path, marker_wave, policy, deferred_dir)
        return {"deleted": False, "state": "recovery-deferred-user-migration",
                "cold_recovery_completed": False}
    ack = make_ack(args.sync_root, resolved, relative_root=candidate.relative_root,
                   origin=args.origin_node_id)
    # Exact cold reconstruction may be slow. An earlier transport observation
    # cannot authorize later unlink: recheck the shared full gate after decode.
    convergence = _convergence(base, key, "stockagent-packed", policy["origin_node_id"])
    if not convergence["ok"]:
        return {"deleted": False, "state": "waiting-packed-peer-convergence",
                "transport_checks": convergence["checks"], "cold_recovery_completed": True}
    receipt = args.state_root / "training-return" / f"{ack['identity_sha256']}.json"
    if args.apply:
        atomic_write_json(receipt, {"ack": ack, "state": "durable_return_verified"})
    if retire_source:
        try:
            if args.apply:
                with ingress_owner(PUBLICATION_OWNER):
                    held = resolve_packed_snapshot_id(args.sync_root, wave['dataset'], wave['snapshot_id'])
                    if held.manifest_sha256 != wave['manifest_sha256']:
                        raise SnapshotError('fixed training cold release changed before source retirement')
                    current_resilience = verify_cold_resilience(
                        args.sync_root, held, REPO_ROOT / 'configs/data_sync/packed_backup.json')
                    if not (current_resilience.get('cold_primary_verified')
                            or current_resilience.get('backup_verified')):
                        raise SnapshotError('returned cold authority changed before source retirement')
                    convergence = _convergence(base, key, 'stockagent-packed', policy['origin_node_id'])
                    if not convergence['ok']:
                        return {'deleted': False, 'state': 'waiting-packed-peer-convergence',
                                'transport_checks': convergence['checks'], 'cold_recovery_completed': True}
                    result = remote_retirement(args, ack, policy, apply=True)
            else:
                result = remote_retirement(args, ack, policy, apply=False)
        except (OSError, SnapshotError, subprocess.SubprocessError) as error:
            result = {"state": "remote-retirement-failed", "deleted": False, "error_type": type(error).__name__}
    else:
        result = {"state": "source-not-currently-eligible", "deleted": False,
                  "cold_return_verified": True}
    if args.apply:
        atomic_write_json(receipt, {"ack": ack, "retirement": result})
        _clear_return(waiting_path, marker_wave, policy)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ssh-target",
        default=os.environ.get("COLD_ARTIFACT_INGRESS_SSH_TARGET"),
    )
    parser.add_argument(
        "--ssh-port",
        type=int,
        default=int(os.environ.get("COLD_ARTIFACT_INGRESS_SSH_PORT", "22")),
    )
    parser.add_argument(
        "--identity-file",
        type=Path,
        default=(
            Path(os.environ.get("STOCKAGENT_REMOTE_INGRESS_IDENTITY_FILE")
                 or os.environ["COLD_ARTIFACT_INGRESS_IDENTITY_FILE"])
            if (os.environ.get("STOCKAGENT_REMOTE_INGRESS_IDENTITY_FILE")
                or os.environ.get("COLD_ARTIFACT_INGRESS_IDENTITY_FILE"))
            else None
        ),
    )
    parser.add_argument(
        "--origin-node-id",
        default=os.environ.get("COLD_ARTIFACT_INGRESS_ORIGIN_NODE_ID", "vastai1T"),
    )
    parser.add_argument(
        "--remote-repo-root",
        default=os.environ.get(
            "COLD_ARTIFACT_INGRESS_REMOTE_REPO_ROOT", "/root/stockAgent"
        ),
    )
    parser.add_argument(
        "--remote-artifact-root",
        default=os.environ.get(
            "COLD_ARTIFACT_INGRESS_REMOTE_ARTIFACT_ROOT",
            "/root/stockAgent/artifacts",
        ),
    )
    parser.add_argument("--scope", default="ablations")
    parser.add_argument("--include-root", action="append", default=[])
    parser.add_argument("--prestage-root", help="Preserve one exact completed run on enrolled E without D publication or source retirement")
    parser.add_argument("--stable-hours", type=float, default=0.0)
    parser.add_argument("--max-publish", type=int, default=1)
    parser.add_argument("--transfer-compression", choices=("none", "zstd-1"),
                        default=os.environ.get("COLD_ARTIFACT_INGRESS_TRANSFER_COMPRESSION", "none"))
    parser.add_argument("--sync-root", type=Path, default=Path("/srv/stockagent-packed"))
    parser.add_argument(
        "--state-root", type=Path, default=Path("/var/lib/stockagent-cold-artifacts")
    )
    parser.add_argument("--staging-root", type=Path,
                        default=(Path(os.environ["COLD_ARTIFACT_INGRESS_STAGING_ROOT"])
                                 if os.environ.get("COLD_ARTIFACT_INGRESS_STAGING_ROOT") else None),
                        help="Optional enrolled D payload scratch; private receipts remain under --state-root")
    parser.add_argument("--node-id")
    parser.add_argument("--policy", type=Path,
                        help="Enroll lifecycle-complete markets/ablations returns under a bounded role policy")
    parser.add_argument("--output", type=Path, help="Private machine status for this existing ingress owner")
    parser.add_argument("--apply", action="store_true")
    return parser


def _last_completed_cycle(path: Path) -> dict | None:
    """Keep bounded, non-identifying counts while the next cycle runs."""
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, 'rb') as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                return None
            raw = stream.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            return None
        previous = json.loads(raw)
    except (OSError, ValueError, TypeError):
        return None
    if not isinstance(previous, dict) or previous.get('automation_contract') != AUTOMATION_CONTRACT:
        return None
    if previous.get('state') == 'cycle-complete' and previous.get('apply') is True:
        rows = previous.get('rows')
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            return None
        candidate = {key: previous.get(key) for key in
                     ('observed_at_epoch', 'observed', 'eligible', 'published',
                      'pending_return_count', 'pending_return_limit', 'deferred_return_count')}
        candidate['deferred'] = sum(row.get('action') in {'return-cycle-limit', 'publish-limit'} for row in rows)
    else:
        candidate = previous.get('last_completed_summary')
    if not isinstance(candidate, dict):
        return None
    stamp = candidate.get('observed_at_epoch')
    if type(stamp) not in (int, float) or not math.isfinite(stamp) or stamp <= 0:
        return None
    required = ('observed', 'eligible', 'published', 'deferred')
    if any(type(candidate.get(key)) is not int or not 0 <= candidate[key] <= 2**53-1 for key in required):
        return None
    result = {key: candidate[key] for key in ('observed_at_epoch', *required)}
    for key in ('pending_return_count', 'pending_return_limit', 'deferred_return_count'):
        value = candidate.get(key)
        if type(value) is int and 0 <= value <= 2**53-1:
            result[key] = value
    return result


def cycle_progress(args, phase: str, candidate=None) -> None:
    if args.apply and args.output:
        values = {"schema_version": INGRESS_SCHEMA_VERSION, "state": "cycle-running",
                  "phase": phase, "observed_at_epoch": time.time(),
                  "automation_contract": AUTOMATION_CONTRACT}
        if summary := _last_completed_cycle(args.output):
            values['last_completed_summary'] = summary
        if candidate is not None:
            values.update(current_root=candidate.relative_root, source_files=candidate.files,
                          source_logical_bytes=candidate.logical_bytes)
        atomic_write_json(args.output, values)


def cycle_failure(args, error: Exception) -> dict:
    """Preserve actionable private stderr, never dump an SSH program to logs."""
    failure = {"state": "cycle_failed", "error_type": type(error).__name__,
               "observed_at_epoch": time.time(), "automation_contract": AUTOMATION_CONTRACT}
    if args.output and args.output.is_file():
        if summary := _last_completed_cycle(args.output):
            failure['last_completed_summary'] = summary
        try:
            prior = json.loads(args.output.read_text())
            if prior.get("automation_contract") == AUTOMATION_CONTRACT and prior.get("state") == "cycle-running":
                failure.update({key: prior[key] for key in ("phase", "current_root") if key in prior})
        except (OSError, ValueError):
            pass
    if isinstance(error, subprocess.CalledProcessError):
        failure["remote_exit_code"] = error.returncode
        details = error.stderr or b""
        details = details.encode() if isinstance(details, str) else details
        evidence = args.state_root / "diagnostics" / f"cycle-{time.time_ns()}.private.log"
        if details and args.apply:
            atomic_write_bytes(evidence, details[-65536:], mode=0o600)
            failure["diagnostics"] = str(evidence)
    return failure


def _run_cycle(args, cycle_started: float) -> int:
    try:
        if not args.ssh_target or args.identity_file is None:
            raise SnapshotError("--ssh-target and --identity-file are required")
        policy = load_policy(args.policy) if args.policy else None
        if policy and args.origin_node_id != policy["origin_node_id"]:
            raise SnapshotError("remote origin differs from completed return policy")
        if args.prestage_root:
            if not policy or not args.apply:
                raise SnapshotError("E prestage requires the existing applied completed-return policy")
            receipt = _prestage_on_e(args, policy)
            result = {"schema_version": INGRESS_SCHEMA_VERSION, "state": "cycle-complete",
                "scope": "exact completed training E prestage", "apply": True,
                "observed_at_epoch": time.time(), "rows": [receipt],
                "cold_published": False, "source_roots_retired": 0}
            if args.output:
                atomic_write_json(args.output, result)
            print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
            return 0
        if args.apply and policy and os.environ.get('STOCKAGENT_REMOTE_CACHE_PRESSURE_GC') == '1':
            atomic_write_json(args.state_root/'remote-cache-pressure.json', remote_cache_pressure(args))
        if args.apply:
            retry_training_layout(args)
        include_roots = list(args.include_root)
        if not include_roots:
            include_roots = [
                item.strip()
                for item in os.environ.get(
                    "COLD_ARTIFACT_INGRESS_INCLUDE_ROOTS", ""
                ).split(",")
                if item.strip()
            ]
        discovery_args = dict(
            ssh_target=args.ssh_target,
            ssh_port=args.ssh_port,
            identity_file=args.identity_file,
            remote_repo_root=args.remote_repo_root,
            remote_artifact_root=args.remote_artifact_root,
        )
        cycle_progress(args, "discovering-completed-runs")
        scopes = policy["scopes"] if policy else [args.scope]
        defer_restore = os.environ.get('STOCKAGENT_DEFER_OPTIONAL_RETURN_RESTORE') == '1'
        if policy and args.apply and not defer_restore:
            _resume_deferred_returns(args.state_root, policy)
        pending = waiting_returns(args.state_root, policy) if policy else []
        deferred = [wave for _, wave in _deferred_entries(args.state_root, policy)] if policy else []
        deferred_by_root = {wave['relative_root']: wave for wave in deferred}
        if any(wave['relative_root'] in deferred_by_root and
               deferred_by_root[wave['relative_root']] != wave for wave in pending):
            raise SnapshotError('pending and deferred return generations conflict')
        attempts_path = args.state_root / "training-return-attempts.json"
        attempts = {}
        if policy:
            if attempts_path.is_symlink():
                raise SnapshotError("training return attempt journal is redirected")
            if attempts_path.exists():
                attempts = json.loads(attempts_path.read_text())
                if not isinstance(attempts, dict) or any(type(v) not in (int, float) for v in attempts.values()):
                    raise SnapshotError("training return attempt journal is invalid")
        ordered = sorted(pending, key=lambda wave: attempts.get(wave['relative_root'], 0))
        # While restoration is explicitly deferred, move all already-synced
        # fixed waves in this bounded queue through the same original gate.
        # This needs neither source deletion nor an SSH scan for each wave.
        waiting_batch = ordered if defer_restore else ordered[:1]
        waiting = waiting_batch[0] if waiting_batch else None
        if len(pending) >= MAX_PENDING_RETURNS:
            # Capacity-bounded backpressure affects new preservation only
            # after the independent confirmation queue is actually full.
            if defer_restore:
                observed = []  # No source may be deleted in this mode.
            else:
                selected_scope = PurePosixPath(waiting["relative_root"]).parts[0]
                observed = [row for row in discover_remote_artifacts(
                    **discovery_args, scope=selected_scope, relative_root=waiting["relative_root"]
                ) if row.relative_root == waiting["relative_root"]]
        else:
            observed = [row for scope in scopes for row in discover_remote_artifacts(**discovery_args, scope=scope)]
        if policy:
            include_roots = [r.relative_root for r in observed if admitted(r.relative_root, policy)
                             and r.files <= policy["maximum_run_files"]
                             and r.logical_bytes <= policy["maximum_run_bytes"]]
        if not include_roots and policy and not pending:
            result = {"schema_version": 1, "state": "cycle-complete", "apply": args.apply,
                      "observed_at_epoch": time.time(), "automation_contract": AUTOMATION_CONTRACT,
                      "observed": len(observed), "eligible": 0, "published": 0, "rows": [],
                      "pending_return_count": 0, "pending_return_limit": MAX_PENDING_RETURNS,
                      "deferred_return_count": len(deferred),
                      "scope": "completed training returns"}
            if args.output:
                atomic_write_json(args.output, result)
            print(json.dumps(result))
            return 0
        eligible = eligible_candidates(
            observed,
            include_roots=include_roots,
            stable_hours=policy["stable_hours"] if policy else args.stable_hours,
            allow_unobservable_copy=policy is not None,
        ) if include_roots else []
        if policy:
            eligible.sort(key=lambda r: (attempts.get(r.relative_root, 0), r.newest_mtime_ns, r.relative_root))
        pending_candidates = {row.relative_root: row for row in eligible}
        pending_roots = {wave["relative_root"] for wave in (*pending, *deferred)}
        eligible = [row for row in eligible if row.relative_root not in pending_roots]
        rows: list[dict[str, Any]] = []
        published = 0
        processed = 0
        for waiting in waiting_batch:
            # Fixed-wave confirmation is independent of the new-publication
            # budget. Busy shared transport cannot authorize source unlink.
            if args.apply:
                attempts[waiting["relative_root"]] = time.time()
                atomic_write_json(attempts_path, attempts)
            try:
                held, repacked = _resolve_waiting_release(args, waiting)
            except (OSError, ValueError, SnapshotError) as error:
                # Preserve this marker and let other waves progress on future
                # ticks; the UI still reports the unresolved fixed release.
                rows.append({'relative_root':waiting['relative_root'], 'action':'waiting-fixed-release-unavailable',
                             'error_type':type(error).__name__})
            else:
                source = held.manifest["source"]
                current = pending_candidates.get(waiting["relative_root"])
                # A new source revision must never be retired against an older
                # frozen return. Confirm that old wave without source mutation.
                exact = current is not None and current.files == source["files"] and (
                    current.logical_bytes == source["logical_bytes"] and
                    current.portable_fingerprint_sha256 == source["portable_fingerprint_sha256"])
                proxy = current if exact else RemoteCandidate(
                    waiting["relative_root"], source["files"], source["logical_bytes"],
                    source["portable_fingerprint_sha256"], 1, ())
                cycle_progress(args, "verifying-held-cold-wave", proxy)
                options = {'retire_source':bool(exact and not proxy.process_references)}
                if repacked:
                    options['pending_original'] = waiting
                try:
                    returned = returned_source(args, proxy, held, policy, **options)
                except (OSError, ValueError, SnapshotError) as error:
                    # One stale fixed wave must not starve unrelated markers.
                    rows.append({'relative_root': waiting['relative_root'], 'action': 'waiting-fixed-release-unavailable',
                                 'error_type': type(error).__name__})
                    continue
                rows.append({"relative_root": proxy.relative_root, "action": "confirming-held-wave",
                             "source_retirement": returned})
        for candidate in eligible:
            if policy:
                if len(waiting_returns(args.state_root, policy)) >= MAX_PENDING_RETURNS:
                    rows.append({"relative_root": candidate.relative_root, "action": "waiting-confirmation-capacity"})
                    continue
                if processed >= max(0, int(args.max_publish)):
                    rows.append({"relative_root": candidate.relative_root, "action": "return-cycle-limit"})
                    continue
                processed += 1
                if args.apply:
                    attempts[candidate.relative_root] = time.time()
                    atomic_write_json(attempts_path, attempts)
            spec = _spec(candidate, policy["stable_hours"] if policy else args.stable_hours)
            cycle_progress(args, "checking-existing-release", candidate)
            # The v2 ACK performs a fresh full reconstruction after transport
            # convergence. Avoid a second full read before that same proof.
            existing = (_resolved_release(args.sync_root, spec, candidate, verify_content=False)
                        if policy else _resolved_release(args.sync_root, spec, candidate))
            if existing is not None:
                cycle_progress(args, "verifying-cold-and-retirement-gates", candidate)
                source_retirement = returned_source(args, candidate, existing, policy,
                                                   retire_source=not candidate.process_references)
                rows.append(
                    {
                        "dataset": spec.dataset,
                        "relative_root": spec.relative_root,
                        "action": "existing-release-awaiting-return-confirmation"
                        if source_retirement.get("state") == "waiting-packed-peer-convergence" else "already-preserved",
                        "snapshot_id": existing.manifest["snapshot_id"],
                        "source_retirement": source_retirement,
                    }
                )
                continue
            if published >= max(0, int(args.max_publish)):
                rows.append(
                    {
                        "dataset": spec.dataset,
                        "relative_root": spec.relative_root,
                        "action": "publish-limit",
                    }
                )
                continue
            if not args.apply:
                rows.append(
                    {
                        "dataset": spec.dataset,
                        "relative_root": spec.relative_root,
                        "action": "would-ingest",
                        "files": candidate.files,
                        "logical_bytes": candidate.logical_bytes,
                    }
                )
                published += 1
                continue

            staging_base = args.state_root
            if policy:
                try:
                    staging_base = return_staging_workspace(
                        args, policy["reserve_bytes"] + 2*candidate.logical_bytes)
                except SnapshotError:
                    rows.append({"relative_root": candidate.relative_root, "action": "waiting-local-capacity"})
                    continue
            elif getattr(args, "staging_root", None) is not None:
                raise SnapshotError("return scratch requires the bounded training return policy")
            use_e_scratch = staging_base.absolute() == E_RETURN_PAYLOAD_SCRATCH
            e_identity = None
            if use_e_scratch:
                stage_stat = staging_base.stat()
                e_identity = (stage_stat.st_dev, stage_stat.st_ino)
                _fence_e_stage(staging_base, e_identity, physical=False)
            staging_artifact_root = _staging_artifact_root(
                staging_base, args.origin_node_id, spec
            )
            cycle_progress(args, "transferring-from-origin", candidate)
            _rsync_candidate(
                candidate,
                ssh_target=args.ssh_target,
                ssh_port=args.ssh_port,
                identity_file=args.identity_file,
                remote_artifact_root=args.remote_artifact_root,
                destination_artifact_root=staging_artifact_root,
                compression=args.transfer_compression,
            )
            if e_identity is not None:
                _fence_e_stage(staging_base, e_identity, physical=True)
            local_status = validate_cold_artifact_source(staging_artifact_root, spec)
            if not staging_observation_matches(
                    local_status, candidate, d_payload_scratch=staging_base.absolute() == RETURN_PAYLOAD_SCRATCH):
                raise SnapshotError(
                    f"remote artifact changed during ingress: {candidate.relative_root}"
                )
            after = discover_remote_artifacts(**discovery_args, scope=PurePosixPath(candidate.relative_root).parts[0]
                                             if policy else args.scope, relative_root=candidate.relative_root)
            current = {row.relative_root: row for row in after}.get(
                candidate.relative_root
            )
            if (
                current is None
                or (current.process_references and not (
                    policy and current.process_references == (UNOBSERVABLE_CONSUMERS,)))
                or current.observation() != candidate.observation()
                or current.portable_fingerprint_sha256 != candidate.portable_fingerprint_sha256
            ):
                raise SnapshotError(
                    f"remote artifact changed or became active during ingress: "
                    f"{candidate.relative_root}"
                )
            cycle_progress(args, "publishing-d-cold", candidate)
            if e_identity is not None:
                _fence_e_stage(staging_base, e_identity, physical=False)
            with ingress_owner(PUBLICATION_OWNER):
                resolved = publish_cold_artifact(
                    args.sync_root,
                    staging_artifact_root,
                    spec,
                    node_id=args.node_id,
                    repo_root=REPO_ROOT,
                    defer_scan=policy is not None,
                    metadata={
                        "ingress_origin_node_id": args.origin_node_id,
                        "ingress_remote_newest_mtime_ns": candidate.newest_mtime_ns,
                        "ingress_transport": "ssh-rsync",
                        "ingress_transport_compression": args.transfer_compression,
                        "ingress_staging_clock_contract": (
                            "drvfs_rsync_whole_seconds_origin_nanoseconds_in_receipt"
                            if staging_base.absolute() == RETURN_PAYLOAD_SCRATCH else "native_nanoseconds"),
                    },
                )
            if policy and defer_restore:
                # The publisher hashes all staged source files and commits the
                # fixed D generation. The user deferred an additional cold
                # payload readback during migration; keep this explicitly
                # distinct from an independent verification or restore ACK.
                proof = {'state': 'source_hashed_publication_readback_deferred',
                         'source_hashed_during_publication': True,
                         'independent_cold_readback': False,
                         'cold_reconstruction_verified': False}
            else:
                proof = verify_packed_snapshot(
                    args.sync_root,
                    resolved,
                    materialized_path=Path(str(local_status["source"])),
                )
            receipt = {
                "schema_version": INGRESS_SCHEMA_VERSION,
                "recorded_at": _utc_iso_from_ns(time.time_ns()),
                "origin_node_id": args.origin_node_id,
                "dataset": spec.dataset,
                "relative_root": spec.relative_root,
                "remote_observation": {
                    "files": candidate.files,
                    "logical_bytes": candidate.logical_bytes,
                    "newest_mtime_ns": candidate.newest_mtime_ns,
                },
                "snapshot_id": resolved.manifest["snapshot_id"],
                "manifest_sha256": resolved.manifest_sha256,
                "verification": proof,
                "staging_removed": False,
            }
            receipt_dir = args.state_root.resolve() / "ingress-receipts"
            receipt_dir.mkdir(parents=True, exist_ok=True)
            receipt_path = receipt_dir / f"{spec.dataset}.json"
            atomic_write_json(receipt_path, receipt)
            staging_dataset_root = staging_artifact_root.parent
            if e_identity is not None:
                _fence_e_stage(staging_base, e_identity, physical=False)
            if staging_dataset_root.is_symlink() or staging_dataset_root.resolve() != staging_dataset_root:
                raise SnapshotError('return staging dataset root changed before cleanup')
            shutil.rmtree(staging_dataset_root)
            receipt["staging_removed"] = True
            atomic_write_json(receipt_path, receipt)
            cycle_progress(args, "verifying-cold-and-retirement-gates", candidate)
            source_retirement = returned_source(args, candidate, resolved, policy,
                                               retire_source=not current.process_references)
            rows.append(
                {
                    "dataset": spec.dataset,
                    "relative_root": spec.relative_root,
                    "action": "published",
                    "snapshot_id": resolved.manifest["snapshot_id"],
                    "manifest_sha256": resolved.manifest_sha256,
                    "receipt": str(receipt_path),
                    "source_retirement": source_retirement,
                }
            )
            published += 1

        result = {
                    "schema_version": INGRESS_SCHEMA_VERSION,
                    "state": "cycle-complete",
                    "observed_at_epoch": time.time(),
                    "automation_contract": AUTOMATION_CONTRACT,
                    "apply": args.apply,
                    "observed": len(observed),
                    "allowlisted": len(include_roots),
                    "eligible": len(eligible),
                    "published": sum(row["action"] == "published" for row in rows),
                    "rows": rows,
                    "scope": "completed training returns" if policy else "explicit remote artifact roots",
                    "complete_workflow_seconds": time.perf_counter() - cycle_started,
                    "waiting_return": waiting_return(args.state_root, policy) if policy else None,
                    "pending_return_count": len(waiting_returns(args.state_root, policy)) if policy else 0,
                    "pending_return_limit": MAX_PENDING_RETURNS if policy else None,
                    "deferred_return_count": len(_deferred_entries(args.state_root, policy)) if policy else 0,
                    "source_roots_retired": sum(bool(row.get("source_retirement", {}).get("deleted")) for row in rows),
                    "remote_reclaimed_bytes": sum(int(row.get("source_retirement", {}).get("reclaimed_allocated_bytes", 0)) for row in rows),
                    "deferred_sources": sum(row.get("source_retirement", {}).get("deleted") is False for row in rows if "source_retirement" in row),
                }
        if args.output:
            atomic_write_json(args.output, result)
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except (
        OSError,
        SnapshotError,
        ValueError,
        subprocess.SubprocessError,
    ) as exc:
        failure = cycle_failure(args, exc)
        if args.output:
            atomic_write_json(args.output, failure)
        if isinstance(exc, subprocess.CalledProcessError):
            print(f"ERROR: remote operation failed (exit {exc.returncode}); private diagnostics retained", file=sys.stderr)
        else:
            print(f"ERROR: {exc}", file=sys.stderr)
        return 2


def main() -> int:
    started = time.perf_counter()
    args = build_parser().parse_args()
    if not args.apply:
        return _run_cycle(args, started)
    try:
        with ingress_owner(INGRESS_OWNER):
            return _run_cycle(args, started)
    except BlockingIOError as error:
        # A waiting non-owner must not overwrite the active owner's status.
        print(f'ERROR: {error}', file=sys.stderr)
        return 75
    except (OSError, SnapshotError) as error:
        print(f'ERROR: {error}', file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
