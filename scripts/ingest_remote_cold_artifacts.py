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
import json
import inspect
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import shutil
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
AUTOMATION_CONTRACT = "cold-return-v3-full-convergence"
INGRESS_OWNER = Path('/run/lock/stockagent-remote-cold-artifact-ingress-cycle.lock')
PUBLICATION_OWNER = Path('/run/lock/stockagent-remote-cold-artifact-ingress.lock')
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
    rows.append({
        "relative_root": source.relative_to(artifact_root).as_posix(),
        "files": source_summary["files"],
        "logical_bytes": source_summary["logical_bytes"],
        "portable_fingerprint_sha256": source_summary[
            "portable_fingerprint_sha256"
        ],
        "newest_mtime_ns": newest_activity_ns(source),
        "process_references": artifact_process_references(source, scope_root),
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
    from stockagent.data_sync.artifact_maintenance import discover_completed_runs, _ancestor_process_references
    # Run the reviewed shared discovery implementation over SSH without
    # mutating the remote working tree (which may have an unfinished merge).
    program = _REMOTE_DISCOVERY_PROGRAM.replace(
        "artifact_root = Path(sys.argv[1]).resolve()",
        inspect.getsource(discover_completed_runs) + "\n" +
        "import stockagent.data_sync.artifact_maintenance as maintenance\n" +
        inspect.getsource(_ancestor_process_references) + "\n" +
        "maintenance._ancestor_process_references = _ancestor_process_references\n" +
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
    completed = subprocess.run(
        [*_ssh_base(identity_file, ssh_port), target, remote_command],
        check=True,
        capture_output=True,
        text=True,
        timeout=300,
    )
    return _decode_discovery(completed.stdout)


def eligible_candidates(
    candidates: Sequence[RemoteCandidate],
    *,
    include_roots: Sequence[str],
    stable_hours: float,
    now_ns: int | None = None,
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
        and not row.process_references
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
    root = (state_root.resolve() / "ingress-staging" / node / spec.dataset).resolve()
    expected_parent = (state_root.resolve() / "ingress-staging" / node).resolve()
    try:
        root.relative_to(expected_parent)
    except ValueError as exc:
        raise SnapshotError(f"unsafe ingress staging root: {root}") from exc
    return root / "artifacts"


def _rsync_candidate(
    candidate: RemoteCandidate,
    *,
    ssh_target: str,
    ssh_port: int,
    identity_file: Path,
    remote_artifact_root: str,
    destination_artifact_root: Path,
    compression: str = "none",
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
        "--delete-delay",
        "--protect-args",
        "-e",
        ssh_command,
    ]
    if compression == "zstd-1":
        command.extend(("--compress", "--compress-choice=zstd", "--compress-level=1"))
    command.extend((source, str(destination) + "/"))
    subprocess.run(
        command,
        check=True,
        timeout=21_600,
    )


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
    from stockagent.data_sync.artifact_maintenance import _ancestor_process_references
    program += "\nimport stockagent.data_sync.artifact_maintenance as maintenance\n"
    program += inspect.getsource(_ancestor_process_references) + "\n"
    program += "maintenance._ancestor_process_references = _ancestor_process_references\n"
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


def waiting_return(state_root: Path, policy: dict) -> dict | None:
    """A private fixed release marker bounds the unconfirmed publication wave."""
    path = state_root / "training-return-waiting-peer.json"
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise SnapshotError("training return waiting marker is redirected")
    if not path.exists():
        return None
    value = json.loads(path.read_text())
    if (not isinstance(value, dict)
            or set(value) != {"relative_root", "dataset", "snapshot_id", "manifest_sha256"}
            or not admitted(value["relative_root"], policy)
            or value["dataset"] != automatic_dataset_name(value["relative_root"])
            or not re.fullmatch(r"[0-9a-f]{64}", value["manifest_sha256"])):
        raise SnapshotError("invalid fixed training return waiting marker")
    _safe_relative_path(value["snapshot_id"], "waiting returned snapshot")
    return value


def returned_source(args, candidate, resolved, policy, *, retire_source: bool = True) -> dict:
    if policy is None:
        return {}
    from stockagent.data_sync.cold_primary import verify_cold_resilience
    resilience = verify_cold_resilience(args.sync_root, resolved, REPO_ROOT / "configs/data_sync/packed_backup.json")
    if not (resilience.get("cold_primary_verified") or resilience.get("backup_verified")):
        raise SnapshotError("returned artifact cold authority is not ready")
    wave = {"relative_root": candidate.relative_root, "dataset": resolved.manifest["dataset"],
            "snapshot_id": resolved.manifest["snapshot_id"], "manifest_sha256": resolved.manifest_sha256}
    waiting_path = args.state_root / "training-return-waiting-peer.json"
    previous = waiting_return(args.state_root, policy)
    if previous and previous != wave:
        raise SnapshotError("a different exact training return is awaiting confirmation")
    if args.apply:
        atomic_write_json(waiting_path, wave)
    from scripts.configure_artifact_ingress_syncthing import credentials
    from scripts.manage_packed_edge import _convergence
    base, key = credentials()
    convergence = _convergence(base, key, "stockagent-packed", policy["origin_node_id"])
    if not convergence["ok"]:
        return {"deleted": False, "state": "waiting-packed-peer-convergence",
                "transport_checks": convergence["checks"]}
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
        if waiting_return(args.state_root, policy) != wave:
            raise SnapshotError("training return waiting wave changed during confirmation")
        waiting_path.unlink()
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
            Path(os.environ["COLD_ARTIFACT_INGRESS_IDENTITY_FILE"])
            if os.environ.get("COLD_ARTIFACT_INGRESS_IDENTITY_FILE")
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
    parser.add_argument("--stable-hours", type=float, default=0.0)
    parser.add_argument("--max-publish", type=int, default=1)
    parser.add_argument("--transfer-compression", choices=("none", "zstd-1"),
                        default=os.environ.get("COLD_ARTIFACT_INGRESS_TRANSFER_COMPRESSION", "none"))
    parser.add_argument("--sync-root", type=Path, default=Path("/srv/stockagent-packed"))
    parser.add_argument(
        "--state-root", type=Path, default=Path("/var/lib/stockagent-cold-artifacts")
    )
    parser.add_argument("--node-id")
    parser.add_argument("--policy", type=Path,
                        help="Enroll lifecycle-complete markets/ablations returns under a bounded role policy")
    parser.add_argument("--output", type=Path, help="Private machine status for this existing ingress owner")
    parser.add_argument("--apply", action="store_true")
    return parser


def cycle_progress(args, phase: str, candidate=None) -> None:
    if args.apply and args.output:
        values = {"schema_version": INGRESS_SCHEMA_VERSION, "state": "cycle-running",
                  "phase": phase, "observed_at_epoch": time.time(),
                  "automation_contract": AUTOMATION_CONTRACT}
        if candidate is not None:
            values.update(current_root=candidate.relative_root, source_files=candidate.files,
                          source_logical_bytes=candidate.logical_bytes)
        atomic_write_json(args.output, values)


def _run_cycle(args, cycle_started: float) -> int:
    try:
        if not args.ssh_target or args.identity_file is None:
            raise SnapshotError("--ssh-target and --identity-file are required")
        policy = load_policy(args.policy) if args.policy else None
        if policy and args.origin_node_id != policy["origin_node_id"]:
            raise SnapshotError("remote origin differs from completed return policy")
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
        waiting = waiting_return(args.state_root, policy) if policy else None
        if waiting:
            # The marker already selects the complete wave. Recheck its exact
            # origin instead of hashing every other completed run first.
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
        if not include_roots and policy and not waiting:
            result = {"schema_version": 1, "state": "cycle-complete", "apply": args.apply,
                      "observed_at_epoch": time.time(), "automation_contract": AUTOMATION_CONTRACT,
                      "observed": len(observed), "eligible": 0, "published": 0, "rows": [],
                      "scope": "completed training returns"}
            if args.output:
                atomic_write_json(args.output, result)
            print(json.dumps(result))
            return 0
        eligible = eligible_candidates(
            observed,
            include_roots=include_roots,
            stable_hours=policy["stable_hours"] if policy else args.stable_hours,
        ) if include_roots else []
        if waiting:
            # Finish/reverify one exact cold wave before starting another run.
            # Dependency-blocked retirements cease to block transfer once the
            # complete cold recovery and transport are independently proved.
            eligible = [row for row in eligible if row.relative_root == waiting["relative_root"]]
        attempts_path = args.state_root / "training-return-attempts.json"
        attempts = {}
        if policy:
            if attempts_path.is_symlink():
                raise SnapshotError("training return attempt journal is redirected")
            if attempts_path.exists():
                attempts = json.loads(attempts_path.read_text())
                if not isinstance(attempts, dict) or any(type(v) not in (int, float) for v in attempts.values()):
                    raise SnapshotError("training return attempt journal is invalid")
            eligible.sort(key=lambda r: (attempts.get(r.relative_root, 0), r.newest_mtime_ns, r.relative_root))
        rows: list[dict[str, Any]] = []
        published = 0
        processed = 0
        if waiting and not eligible:
            # Another authorized owner may have retired this run, or a new
            # consumer may be using it. Confirm only its exact cold/transport
            # wave; do not infer deletion or act on any missing/busy source.
            held = resolve_packed_snapshot_id(args.sync_root, waiting["dataset"], waiting["snapshot_id"])
            if held.manifest_sha256 != waiting["manifest_sha256"]:
                raise SnapshotError("waiting training release changed")
            source = held.manifest["source"]
            proxy = RemoteCandidate(waiting["relative_root"], source["files"], source["logical_bytes"],
                                    source["portable_fingerprint_sha256"], 1, ())
            cycle_progress(args, "verifying-held-cold-wave", proxy)
            rows.append({"relative_root": proxy.relative_root, "action": "held-wave-source-not-eligible",
                         "source_retirement": returned_source(args, proxy, held, policy, retire_source=False)})
        for candidate in eligible:
            if policy:
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
                source_retirement = returned_source(args, candidate, existing, policy)
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

            staging_artifact_root = _staging_artifact_root(
                args.state_root, args.origin_node_id, spec
            )
            if policy:
                args.state_root.mkdir(parents=True, exist_ok=True)
                try:
                    admit_workspace(args.state_root, policy["reserve_bytes"] + 2*candidate.logical_bytes)
                except SnapshotError:
                    rows.append({"relative_root": candidate.relative_root, "action": "waiting-local-capacity"})
                    continue
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
            local_status = validate_cold_artifact_source(staging_artifact_root, spec)
            if (
                int(local_status["files"]),
                int(local_status["logical_bytes"]),
                int(local_status["newest_mtime_ns"]),
            ) != candidate.observation():
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
                or current.process_references
                or current.observation() != candidate.observation()
            ):
                raise SnapshotError(
                    f"remote artifact changed or became active during ingress: "
                    f"{candidate.relative_root}"
                )
            cycle_progress(args, "publishing-d-cold", candidate)
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
                    },
                )
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
            shutil.rmtree(staging_dataset_root)
            receipt["staging_removed"] = True
            atomic_write_json(receipt_path, receipt)
            cycle_progress(args, "verifying-cold-and-retirement-gates", candidate)
            source_retirement = returned_source(args, candidate, resolved, policy)
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
        if args.output:
            atomic_write_json(args.output, {"state": "cycle_failed", "error_type": type(exc).__name__,
                                           "observed_at_epoch": time.time(), "automation_contract": AUTOMATION_CONTRACT})
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
