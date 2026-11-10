"""Lightweight user testing dispositions shared by storage control roles."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path, PurePosixPath
import re

from stockagent.data_sync.desync_snapshots import SnapshotError, _safe_relative_path


def identity(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def real(path: Path) -> Path:
    path = path.absolute()
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise SnapshotError("testing disposition path is redirected")
    return path


CONTRACT = "explicit-testing-artifact-discard-v1"
PREPARATION_CONTRACT = "explicit-local-preparation-benchmark-discard-v1"
LOCAL_TESTING_CONTRACT = "explicit-local-testing-artifact-discard-v1"
D_WORKFLOW_TESTING_CONTRACT = "explicit-d-workflow-testing-copy-discard-v1"
LOCAL_COPY_CONTRACTS = frozenset({LOCAL_TESTING_CONTRACT, D_WORKFLOW_TESTING_CONTRACT})
D_WORKFLOW_TESTING_ROOT = Path('/srv/stockagent-d-volume/.stockagent-workflow-speed-20261021/artifacts')
DEFAULT_POLICY = Path(__file__).resolve().parents[2] / "configs/data_sync/testing_artifact_disposition_20261007_v2.json"


def load_policy(path: Path) -> dict:
    value = json.loads(real(path).read_bytes())
    body = {k: v for k, v in value.items() if k != "identity_sha256"}
    local_preparation = value.get("contract") == PREPARATION_CONTRACT
    local_testing = value.get("contract") == LOCAL_TESTING_CONTRACT
    d_workflow_testing = value.get("contract") == D_WORKFLOW_TESTING_CONTRACT
    if (set(body) != {"contract", "authority_node_id", "origin_node_id", "authorization", "decision", "targets"}
            or value.get("contract") not in {CONTRACT, PREPARATION_CONTRACT, *LOCAL_COPY_CONTRACTS}
            or value.get("authority_node_id") != "penguin"
            or value.get("origin_node_id") != ("penguin" if local_preparation or local_testing or d_workflow_testing else "vastai1T")
            or value.get("decision") != "discard_without_preservation"
            or not isinstance(value.get("authorization"), str) or not value["authorization"].strip()
            or value.get("identity_sha256") != identity(body)
            or not isinstance(value.get("targets"), list) or not value["targets"]):
        raise SnapshotError("invalid explicit testing-artifact disposition")
    seen = set()
    for row in value["targets"]:
        allowed_bases = ({"preparation-benchmark"} if local_preparation else
                         {"d-workflow-benchmark"} if d_workflow_testing else
                         {"artifact"} if local_testing else {"artifact", "engineering"})
        if set(row) != {"base", "relative", "kind", "reason"} or row["base"] not in allowed_bases:
            raise SnapshotError("invalid exact testing target")
        relative = _safe_relative_path(row["relative"], "testing discard target")
        if row["kind"] not in {"directory", "file"} or row["reason"] not in {"smoke-test", "benchmark", "validation-test"}:
            raise SnapshotError("unreviewed testing target type")
        if row["base"] == "d-workflow-benchmark":
            # Only individual disposable trial measurements in the identified
            # older workflow-speed workspace; never D's cold CAS or receipts.
            trial = (len(relative.parts) >= 3 and relative.parts[0] == "benchmarks"
                     and relative.parts[1] in {"round20", "round31", "round37", "round69", "round78", "round89"}
                     and re.fullmatch(r"(?:full_)?trial_[0-9]+_(?:baseline|candidate)", relative.parts[2]) is not None)
            old_trial = (len(relative.parts) == 4 and relative.parts[:2] in {
                ("benchmarks", "round25"), ("benchmarks", "round27")}
                and relative.parts[2] == "trials" and re.fullmatch(r"[AB][1-8]", relative.parts[3]) is not None)
            exact_file = trial and len(relative.parts) == 4 and relative.parts[3] == "reported_metrics.csv" and row['kind'] == 'file'
            exact_directory = ((trial and len(relative.parts) == 3) or old_trial) and row['kind'] == 'directory'
            if not (row["reason"] == "benchmark" and (exact_file or exact_directory)):
                raise SnapshotError("D workflow disposition requires an exact reviewed trial copy")
        elif row["base"] == "preparation-benchmark":
            # Exact, reviewed children only. This is not authorization to retire
            # a data root, preparation namespace, or any cold publication.
            if (len(relative.parts) < 4 or not relative.parts[0].startswith("data_")
                    or relative.parts[0] == "data_" or relative.parts[1] != "preparation_sources"
                    or row["reason"] not in {"benchmark", "validation-test"}):
                raise SnapshotError("preparation discard must name an individual benchmark output")
        elif row["base"] == "artifact":
            if len(relative.parts) < 2 or relative.parts[0] not in {"smoke", "markets", "operations", "ablations", "cache", "benchmarks"}:
                raise SnapshotError("testing discard cannot target an artifact namespace")
            if local_testing and (relative.parts[0] not in {"benchmarks", "operations"}
                                  or row["reason"] not in {"benchmark", "validation-test"}):
                raise SnapshotError("local testing discard requires an exact reviewed performance artifact")
        elif (len(relative.parts) < 3 or not relative.parts[0].startswith(
                ("stockagent-remote-build-measurement-", "stockagent-control-pilot-")) or row["kind"] != "file"):
            raise SnapshotError("engineering discard must name an individual measurement file")
        key = (row["base"], relative)
        if key in seen or any(base == row["base"] and (
                other in relative.parents or relative in other.parents) for base, other in seen):
            raise SnapshotError("duplicate or overlapping testing targets")
        seen.add((row["base"], relative))
    return value


def target_path(policy: dict, target: dict, *, repo_root: Path, engineering_root: Path) -> Path:
    """Resolve both policy kinds into the common guarded discard lifecycle."""
    if target not in policy["targets"]:
        raise SnapshotError("testing target is outside the exact disposition")
    if policy["contract"] == PREPARATION_CONTRACT and target["base"] == "preparation-benchmark":
        base = repo_root
    elif policy["contract"] == D_WORKFLOW_TESTING_CONTRACT and target["base"] == "d-workflow-benchmark":
        base = D_WORKFLOW_TESTING_ROOT
    elif policy["contract"] in {CONTRACT, LOCAL_TESTING_CONTRACT} and target["base"] in {"artifact", "engineering"}:
        base = repo_root / "artifacts" if target["base"] == "artifact" else engineering_root
    else:
        raise SnapshotError("testing target base disagrees with its disposition")
    return real(base / _safe_relative_path(target["relative"], "testing discard target"))


def artifact_roots(policy: dict) -> set[str]:
    return {r["relative"] for r in policy["targets"] if r["base"] == "artifact"}


def discarded_root(relative: str, policy: dict) -> bool:
    path = PurePosixPath(relative)
    return any(path == PurePosixPath(root) or PurePosixPath(root) in path.parents
               for root in artifact_roots(policy))


def manifest_discarded(manifest: dict, policy: dict) -> bool:
    if policy["contract"] in LOCAL_COPY_CONTRACTS:
        # Local work-copy disposal grants no D/NAS deletion or recovery waiver.
        return False
    meta = manifest.get("metadata", {})
    if meta.get("origin_node_id") not in {None, "vastai1T"}:
        return False
    name = meta.get("source_relative_root", meta.get("artifact_relative_root"))
    return isinstance(name, str) and discarded_root(name, policy)
