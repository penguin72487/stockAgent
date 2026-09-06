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
from dataclasses import dataclass
import json
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
from stockagent.data_sync.cold_artifacts import (  # noqa: E402
    ColdArtifactSpec,
    publish_cold_artifact,
    validate_cold_artifact_source,
)
from stockagent.data_sync.desync_snapshots import (  # noqa: E402
    SnapshotError,
    _safe_relative_path,
    _utc_iso_from_ns,
    atomic_write_json,
)
from stockagent.data_sync.packed_snapshots import (  # noqa: E402
    resolve_latest_packed,
    verify_packed_snapshot,
)


INGRESS_SCHEMA_VERSION = 1
_SSH_TARGET_RE = re.compile(r"^[A-Za-z0-9_.@:-]+$")
_REMOTE_ABSOLUTE_RE = re.compile(r"^/[A-Za-z0-9_./-]+$")

_REMOTE_DISCOVERY_PROGRAM = r"""
from pathlib import Path
import json
import sys
from stockagent.data_sync.artifact_maintenance import (
    artifact_process_references,
    discover_completed_runs,
    newest_activity_ns,
)
from stockagent.data_sync.packed_snapshots import _collect_entries

artifact_root = Path(sys.argv[1]).resolve()
scope = sys.argv[2]
scope_root = artifact_root.joinpath(*Path(scope).parts).resolve()
rows = []
for source in discover_completed_runs(artifact_root, scope):
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


def _validate_ssh_target(value: str) -> str:
    target = str(value).strip()
    if not target or not _SSH_TARGET_RE.fullmatch(target) or target.startswith("-"):
        raise SnapshotError(f"unsafe SSH target: {value!r}")
    return target


def _validate_remote_absolute(value: str, label: str) -> str:
    path = str(value).rstrip("/")
    pure = PurePosixPath(path)
    if (
        not pure.is_absolute()
        or ".." in pure.parts
        or not _REMOTE_ABSOLUTE_RE.fullmatch(path)
    ):
        raise SnapshotError(f"unsafe {label}: {value!r}")
    return path


def _ssh_base(identity_file: Path, port: int) -> list[str]:
    identity = identity_file.resolve()
    if not identity.is_file() or identity.is_symlink():
        raise SnapshotError(f"SSH identity is not a regular file: {identity}")
    if identity.stat().st_mode & 0o077:
        raise SnapshotError(f"SSH identity permissions must be 0600 or stricter: {identity}")
    if not 1 <= int(port) <= 65535:
        raise SnapshotError(f"invalid SSH port: {port}")
    return [
        "ssh",
        "-i",
        str(identity),
        "-p",
        str(int(port)),
        "-o",
        "BatchMode=yes",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "ConnectTimeout=15",
    ]


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
) -> list[RemoteCandidate]:
    target = _validate_ssh_target(ssh_target)
    repo_root = _validate_remote_absolute(remote_repo_root, "remote repo root")
    artifact_root = _validate_remote_absolute(
        remote_artifact_root, "remote artifact root"
    )
    safe_scope = _safe_relative_path(scope, "remote artifact scope").as_posix()
    remote_command = shlex.join(
        [
            "bash",
            "-lc",
            (
                f"cd {shlex.quote(repo_root)} && source scripts/runtime_env.sh && "
                "python_bin=\"$(resolve_fintech_python)\" && "
                f"exec \"$python_bin\" -c {shlex.quote(_REMOTE_DISCOVERY_PROGRAM)} "
                f"{shlex.quote(artifact_root)} {shlex.quote(safe_scope)}"
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
):
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
) -> None:
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
    subprocess.run(
        [
            "rsync",
            "--archive",
            "--partial",
            "--append-verify",
            "--delete-delay",
            "--protect-args",
            "-e",
            ssh_command,
            source,
            str(destination) + "/",
        ],
        check=True,
        timeout=21_600,
    )


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
    parser.add_argument("--sync-root", type=Path, default=Path("/srv/stockagent-packed"))
    parser.add_argument(
        "--state-root", type=Path, default=Path("/var/lib/stockagent-cold-artifacts")
    )
    parser.add_argument("--node-id")
    parser.add_argument("--apply", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        if not args.ssh_target or args.identity_file is None:
            raise SnapshotError("--ssh-target and --identity-file are required")
        include_roots = list(args.include_root)
        if not include_roots:
            include_roots = [
                item.strip()
                for item in os.environ.get(
                    "COLD_ARTIFACT_INGRESS_INCLUDE_ROOTS", ""
                ).split(",")
                if item.strip()
            ]
        observed = discover_remote_artifacts(
            ssh_target=args.ssh_target,
            ssh_port=args.ssh_port,
            identity_file=args.identity_file,
            remote_repo_root=args.remote_repo_root,
            remote_artifact_root=args.remote_artifact_root,
            scope=args.scope,
        )
        eligible = eligible_candidates(
            observed,
            include_roots=include_roots,
            stable_hours=args.stable_hours,
        )
        rows: list[dict[str, Any]] = []
        published = 0
        for candidate in eligible:
            spec = _spec(candidate, args.stable_hours)
            existing = _resolved_release(args.sync_root, spec, candidate)
            if existing is not None:
                rows.append(
                    {
                        "dataset": spec.dataset,
                        "relative_root": spec.relative_root,
                        "action": "already-preserved",
                        "snapshot_id": existing.manifest["snapshot_id"],
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
            _rsync_candidate(
                candidate,
                ssh_target=args.ssh_target,
                ssh_port=args.ssh_port,
                identity_file=args.identity_file,
                remote_artifact_root=args.remote_artifact_root,
                destination_artifact_root=staging_artifact_root,
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
            after = discover_remote_artifacts(
                ssh_target=args.ssh_target,
                ssh_port=args.ssh_port,
                identity_file=args.identity_file,
                remote_repo_root=args.remote_repo_root,
                remote_artifact_root=args.remote_artifact_root,
                scope=args.scope,
            )
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
            resolved = publish_cold_artifact(
                args.sync_root,
                staging_artifact_root,
                spec,
                node_id=args.node_id,
                repo_root=REPO_ROOT,
                metadata={
                    "ingress_origin_node_id": args.origin_node_id,
                    "ingress_remote_newest_mtime_ns": candidate.newest_mtime_ns,
                    "ingress_transport": "ssh-rsync",
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
            rows.append(
                {
                    "dataset": spec.dataset,
                    "relative_root": spec.relative_root,
                    "action": "published",
                    "snapshot_id": resolved.manifest["snapshot_id"],
                    "manifest_sha256": resolved.manifest_sha256,
                    "receipt": str(receipt_path),
                }
            )
            published += 1

        print(
            json.dumps(
                {
                    "schema_version": INGRESS_SCHEMA_VERSION,
                    "apply": args.apply,
                    "observed": len(observed),
                    "allowlisted": len(include_roots),
                    "eligible": len(eligible),
                    "published": sum(row["action"] == "published" for row in rows),
                    "rows": rows,
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        OSError,
        SnapshotError,
        ValueError,
        subprocess.SubprocessError,
    ) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
