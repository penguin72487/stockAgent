"""Read back a completed canonical compactor and independently count all members.

Never starts/restarts a service, calls a provider, writes a source DB or imports
a model runtime. Static prototype equality and actual publication are separate
gates; a running or changed native invocation is not completed acceptance.
"""
from __future__ import annotations

import argparse
import ast
from datetime import UTC, datetime
import inspect
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from scripts import compact_openbb_l1 as live
from scripts.benchmark_openbb_l1_status_counts import load_reference, prototype_source, sha
from scripts.openbb_l1_status_counts import POLICY

UNIT = "stockagent-openbb-l1-compaction.service"


def identity() -> dict[str, str]:
    keys = ("InvocationID", "ActiveState", "SubState", "MainPID", "Result", "ExecMainStatus")
    result = subprocess.run(["systemctl", "show", UNIT, *["-p" + key for key in keys]],
                            check=True, capture_output=True, text=True, timeout=5)
    return dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)


def validate_publication(attempt: dict, status: dict, unit: dict, previous: str, counts: dict[str, int]) -> None:
    if (unit.get("ActiveState") != "inactive" or unit.get("MainPID") != "0"
            or unit.get("Result") != "success" or unit.get("ExecMainStatus") != "0"
            or not unit.get("InvocationID") or unit["InvocationID"] == previous):
        raise ValueError("a fresh successfully completed native invocation is required")
    if (attempt.get("systemd_invocation_id") != unit["InvocationID"]
            or status.get("systemd_invocation_id") != unit["InvocationID"]
            or attempt.get("state") != "completed" or status.get("state") != "completed"
            or attempt.get("publication_stage") != "status_published" or attempt.get("exit_code") != 0
            or status.get("l0_deleted") is not False):
        raise ValueError("exact completed publication is required")
    expected = {"policy": POLICY, "path": "endpoint_index_seeks",
                "endpoints": len(counts), "members": sum(counts.values())}
    if status.get("member_count_audit") != expected or attempt.get("member_count_audit") != expected:
        raise ValueError("actual endpoint-seek execution counts are not independently verified")
    published = {row["endpoint"]: row["compacted_files"] for row in status["endpoint_status"]}
    if counts != published or sum(counts.values()) != status["compacted_files"]:
        raise ValueError("all legacy source member counts must match publication")
    if any(attempt.get(key) != status.get(key) for key in (
        "success_files", "success_rows", "compacted_files", "compacted_rows", "active_segments", "endpoints"
    )):
        raise ValueError("attempt and publication counts disagree")


def verify(comparison: Path, previous: str, output: Path) -> None:
    if output.exists() or output.is_symlink() or not output.resolve().is_relative_to(ROOT / "artifacts/operations"):
        raise ValueError("fresh operational evidence required")
    receipt = json.loads(comparison.read_bytes())
    if (receipt.get("contract") != "openbb_full_status_endpoint_seek_counts_abba_v2"
            or receipt.get("state") != "accepted_complete_status_output_parity"
            or receipt.get("full_archive_sha_unchanged") is not True
            or receipt.get("provider_calls") != 0 or receipt.get("production_writes") is not False):
        raise ValueError("matching accepted complete benchmark required")
    root = comparison.parent
    for name, expected in receipt["input"]["code_sha256"].items():
        if name != "scripts/compact_openbb_l1.py" and sha(ROOT / name) != expected:
            raise ValueError("benchmarked implementation closure changed")
    original = load_reference(root / "original_compactor.py")
    if sha(root / "original_compactor.py") != receipt["input"]["reference_sha256"]:
        raise ValueError("preserved original compactor changed")
    helper = ROOT / "scripts/openbb_l1_status_counts.py"
    if sha(helper) != receipt["input"]["code_sha256"]["scripts/openbb_l1_status_counts.py"]:
        raise ValueError("benchmarked query implementation changed")
    if ast.dump(ast.parse(inspect.getsource(live._write_status))) != ast.dump(ast.parse(
            prototype_source(inspect.getsource(original._write_status)))):
        raise ValueError("complete native status function differs from accepted prototype")
    for name in ("_mark_stale_segments", "_load_unassigned_shards"):
        if inspect.getsource(getattr(live, name)) != inspect.getsource(getattr(original, name)):
            raise ValueError("source selection or full footer validation changed")
    code = {name: sha(ROOT / name) for name in ("scripts/compact_openbb_l1.py", "scripts/openbb_l1_status_counts.py")}
    before = identity()
    state = ROOT / "data_openBB/_state"
    paths = (state / "l1_compaction_attempt_latest.json", state / "l1_compaction_latest.json")
    raw = [path.read_bytes() for path in paths]
    attempt, status = map(json.loads, raw)
    with sqlite3.connect((state / "openbb_archive.sqlite3").as_uri() + "?mode=ro", uri=True) as connection:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        counts = {str(row[0]): int(row[1]) for row in connection.execute(
            "SELECT endpoint,COUNT(*) FROM l1_compaction_members GROUP BY endpoint")}
        connection.rollback()
    validate_publication(attempt, status, before, previous, counts)
    import polars as pl
    if pl.read_parquet(ROOT / status["status_parquet"]).to_dicts() != status["endpoint_status"]:
        raise ValueError("full native Parquet readback mismatch")
    if (before != identity() or any(path.read_bytes() != value for path, value in zip(paths, raw, strict=True))
            or code != {name: sha(ROOT / name) for name in code}):
        raise ValueError("native owner/publication/code changed during observation")
    if datetime.fromisoformat(attempt["started_at_utc"]).timestamp() < max((ROOT / name).stat().st_mtime for name in code):
        raise ValueError("native owner started before this code generation")
    if "torch" in sys.modules:
        raise ValueError("native source probe imported a model runtime")
    atomic_write_json(output, {"state": "accepted_completed_native_status_counts",
        "observed_at_utc": datetime.now(UTC).isoformat(), "unit": before,
        "comparison_sha256": sha(comparison), "code_sha256": code, "member_count_audit": status["member_count_audit"],
        "all_endpoint_counts_match_legacy": True, "status_parquet_full_readback": True,
        "source_counts": {key: status[key] for key in ("success_files", "success_rows", "compacted_files", "compacted_rows", "active_segments")},
        "elapsed_seconds": attempt["elapsed_seconds"], "stage_seconds": attempt["stage_seconds"],
        "provider_calls": 0, "production_writes": False, "services_changed": False,
        "all_history_complete_claim": False, "whole_compaction_speedup_claim": False,
        "model_runtime_imported": "torch" in sys.modules})
    print(json.dumps({"state": "accepted_completed_native_status_counts", "member_count_audit": status["member_count_audit"]}))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--comparison", type=Path, required=True)
    parser.add_argument("--previous-invocation", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    verify(args.comparison.resolve(strict=True), args.previous_invocation, args.output.absolute())


if __name__ == "__main__":
    main()
