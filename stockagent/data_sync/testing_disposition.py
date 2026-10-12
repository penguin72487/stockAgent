"""Lightweight user testing dispositions shared by storage control roles."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path, PurePosixPath

from stockagent.data_sync.desync_snapshots import SnapshotError, _safe_relative_path


def identity(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def real(path: Path) -> Path:
    path = path.absolute()
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise SnapshotError("testing disposition path is redirected")
    return path


CONTRACT = "explicit-testing-artifact-discard-v1"
DEFAULT_POLICY = Path(__file__).resolve().parents[2] / "configs/data_sync/testing_artifact_disposition_20261007_v2.json"


def load_policy(path: Path) -> dict:
    value = json.loads(real(path).read_bytes())
    body = {k: v for k, v in value.items() if k != "identity_sha256"}
    if (set(body) != {"contract", "authority_node_id", "origin_node_id", "authorization", "decision", "targets"}
            or value.get("contract") != CONTRACT or value.get("authority_node_id") != "penguin"
            or value.get("origin_node_id") != "vastai1T" or value.get("decision") != "discard_without_preservation"
            or not isinstance(value.get("authorization"), str) or not value["authorization"].strip()
            or value.get("identity_sha256") != identity(body)
            or not isinstance(value.get("targets"), list) or not value["targets"]):
        raise SnapshotError("invalid explicit testing-artifact disposition")
    seen = set()
    for row in value["targets"]:
        if set(row) != {"base", "relative", "kind", "reason"} or row["base"] not in {"artifact", "engineering"}:
            raise SnapshotError("invalid exact testing target")
        relative = _safe_relative_path(row["relative"], "testing discard target")
        if row["kind"] not in {"directory", "file"} or row["reason"] not in {"smoke-test", "benchmark", "validation-test"}:
            raise SnapshotError("unreviewed testing target type")
        if row["base"] == "artifact":
            if len(relative.parts) < 2 or relative.parts[0] not in {"smoke", "markets", "operations", "ablations", "cache"}:
                raise SnapshotError("testing discard cannot target an artifact namespace")
        elif (len(relative.parts) < 3 or not relative.parts[0].startswith(
                ("stockagent-remote-build-measurement-", "stockagent-control-pilot-")) or row["kind"] != "file"):
            raise SnapshotError("engineering discard must name an individual measurement file")
        key = (row["base"], relative)
        if key in seen or any(base == row["base"] and (
                other in relative.parents or relative in other.parents) for base, other in seen):
            raise SnapshotError("duplicate or overlapping testing targets")
        seen.add((row["base"], relative))
    return value


def artifact_roots(policy: dict) -> set[str]:
    return {r["relative"] for r in policy["targets"] if r["base"] == "artifact"}


def discarded_root(relative: str, policy: dict) -> bool:
    path = PurePosixPath(relative)
    return any(path == PurePosixPath(root) or PurePosixPath(root) in path.parents
               for root in artifact_roots(policy))


def manifest_discarded(manifest: dict, policy: dict) -> bool:
    meta = manifest.get("metadata", {})
    if meta.get("origin_node_id") not in {None, "vastai1T"}:
        return False
    name = meta.get("source_relative_root", meta.get("artifact_relative_root"))
    return isinstance(name, str) and discarded_root(name, policy)
