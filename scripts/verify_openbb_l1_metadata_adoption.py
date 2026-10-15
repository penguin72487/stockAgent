"""Read-only acceptance of a terminal normal OpenBB owner and its audit policy.

Bind exact invocation, fixture/code generation, native publication/counts and
the independent complete audit controls. This is not a payload bitrot audit or
a claim that all provider history is complete. No service action is performed.
"""
from __future__ import annotations

import argparse
import ast
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json  # noqa: E402
from scripts.benchmark_openbb_l1_footer_reader import digest, implementation_identity  # noqa: E402

UNIT = "stockagent-openbb-l1-compaction.service"
POLICY = "openbb_l1_physical_reflection_cache_v1"
IO_CONTRACT = "openbb_canonical_derivative_footer_reader_v4"


def metadata_io_source(source: str) -> str:
    """Exact accepted complete-function change, not a second audit pipeline."""
    replacements = {
        "    footer_open_attempts = 0\n": "    footer_open_attempts = 0\n    footer_opener = FooterOpener()\n",
        "            parquet_file = pq.ParquetFile(str(path))\n": "            parquet_file = footer_opener(str(path))\n",
        "            footer_open_attempts=footer_open_attempts,\n":
            "            footer_open_attempts=footer_open_attempts,\n            footer_io=footer_opener.snapshot(),\n",
    }
    for before, after in replacements.items():
        if source.count(before) != 1:
            raise ValueError("complete native footer audit shape changed")
        source = source.replace(before, after, 1)
    return source


def validate_native_source(reference: str, current: str) -> None:
    expected, actual = ast.parse(reference), ast.parse(current)
    original = next(node for node in expected.body if isinstance(node, ast.FunctionDef)
                    and node.name == "_mark_stale_segments")
    replacement = ast.parse(metadata_io_source(ast.get_source_segment(reference, original) + "\n")).body[0]
    expected.body[expected.body.index(original)] = replacement
    allowed = [node for node in actual.body if isinstance(node, ast.ImportFrom)
               and node.module == "scripts.openbb_l1_footer_io"
               and node.level == 0 and len(node.names) == 1
               and node.names[0].name == "FooterOpener" and node.names[0].asname is None]
    if len(allowed) != 1:
        raise ValueError("only the exact footer capability import is allowed")
    actual.body.remove(allowed[0])
    if ast.dump(expected) != ast.dump(actual):
        raise ValueError("complete native module differs beyond accepted metadata-only I/O")


def validate_metadata_helper_source(reference: str, current: str) -> None:
    # The complete measured serial constructor, capability/source guards,
    # properties, fallbacks and counters must remain AST-identical. Only the
    # deliberately rejected, never-native prefetch helper's teardown changed.
    trees = [ast.parse(source) for source in (reference, current)]
    for tree in trees:
        nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                 and node.name == "prefetched_footer_rows"]
        if len(nodes) != 1:
            raise ValueError("exact experimental prefetch boundary required")
        tree.body.remove(nodes[0])
    if ast.dump(trees[0]) != ast.dump(trees[1]):
        raise ValueError("measured serial metadata constructor/helper changed")


def validate_io_controls(controls: dict, fixture: dict) -> None:
    variants = ["adopted", "local", "metadata_only", "prefetch", "prefetch", "metadata_only", "local", "adopted"]
    trials = controls.get("trials", [])
    if (controls.get("contract") != IO_CONTRACT
            or controls.get("state") != "accepted_complete_audit_output_parity"
            or controls.get("exact_complete_output_parity") is not True
            or controls.get("all_success_segments") is not True
            or controls.get("provider_calls") != 0 or controls.get("production_writes") is not False
            or [row.get("variant") for row in trials] != variants
            or any(row.get("contract") != IO_CONTRACT or row.get("state") != "completed_trial"
                   or row.get("selected_segments") != fixture["selected_segments"]
                   or row.get("footer_open_attempts") != fixture["selected_segments"]
                   or row.get("all_success_segments") is not True
                   or row.get("stale_count") != 0 or row.get("provider_calls") != 0
                   or row.get("production_writes") is not False for row in trials)
            or len({(row.get("view_proofs"), row.get("complete_audit_output_sha256")) for row in trials}) != 1
            or not re.fullmatch(r"[0-9a-f]{64}", trials[0].get("complete_audit_output_sha256", ""))):
        raise ValueError("complete adopted/local/metadata/prefetch controls required")
    for row in trials:
        if row["variant"] not in {"metadata_only", "prefetch"}:
            continue
        io = row.get("footer_io") or {}
        if (io.get("policy") != "openbb_metadata_only_footer_io_v1"
                or io.get("metadata_only_supported") is not True
                or io.get("local_filesystem") is not True
                or io.get("actual_footer_opens") != fixture["selected_segments"]
                or io.get("metadata_only_opens") != fixture["selected_segments"]
                or io.get("canonical_retries") != 0):
            raise ValueError("actual metadata-only constructor execution required")


def segment_set_digest(connection: sqlite3.Connection) -> str:
    import hashlib
    result = hashlib.sha256()
    for row in connection.execute("""SELECT segment_id,endpoint,output_path,output_rows,
        output_bytes,schema_fingerprint FROM l1_compaction_segments
        WHERE status='success' ORDER BY segment_id"""):
        result.update(json.dumps(list(row), separators=(",", ":")).encode())
        result.update(b"\n")
    return result.hexdigest()


def owner() -> dict[str, str]:
    result = subprocess.run([
        "systemctl", "show", UNIT,
        "--property=ActiveState,SubState,MainPID,InvocationID,Result,ExecMainStatus",
    ], check=True, capture_output=True, text=True)
    return dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)


def validate(unit: dict, invocation: str, attempt: dict, published: dict,
             fixture: dict, controls: dict) -> None:
    io_controls = controls.get("contract") == IO_CONTRACT
    if io_controls:
        validate_io_controls(controls, fixture)
    if (not re.fullmatch(r"[0-9a-f]{32}", invocation)
            or unit != {"ActiveState": "inactive", "SubState": "dead", "MainPID": "0",
                        "InvocationID": invocation, "Result": "success", "ExecMainStatus": "0"}
            or attempt.get("state") != "completed" or attempt.get("exit_code") != 0
            or attempt.get("publication_stage") != "status_published"
            or published.get("state") != "completed"
            or any(value.get("systemd_invocation_id") != invocation for value in (attempt, published))
            or fixture.get("all_success_segments") is not True
            or (not io_controls and (controls.get("state") != "accepted_phase_comparison"
                or controls.get("candidate") != "schema_cache"
                or controls.get("exact_canonical_complete_output_parity") is not True))
            or controls.get("all_success_segments") is not True):
        raise ValueError("terminal owner, exact invocation or complete controls not accepted")
    audit = attempt.get("derivative_metadata_audit", {})
    cache = audit.get("physical_cache") or {}
    expected_proofs = controls["trials"][0]["view_proofs"]
    if (audit != published.get("derivative_metadata_audit") or audit.get("policy") != POLICY
            or audit.get("footer_open_attempts") != fixture["selected_segments"]
            or cache.get("hits", 0) < 1 or cache.get("hits", 0) + cache.get("misses", 0) != expected_proofs
            or cache.get("bypasses") != 0 or cache.get("evictions") != 0
            or cache.get("max_entries") != 64 or cache.get("max_serialized_bytes") != 2 * 1024**2
            or not 0 < cache.get("entries", 0) <= 64
            or not 0 < cache.get("retained_serialized_bytes", 0) <= 2 * 1024**2):
        raise ValueError("full native footer coverage or bounded physical-cache policy mismatch")
    if io_controls:
        io = audit.get("footer_io") or {}
        if (io != controls["trials"][2].get("footer_io")
                or published.get("l0_deleted") is not False):
            raise ValueError("actual native metadata-only execution or source preservation mismatch")
    for key in ("success_files", "success_rows", "compacted_files", "compacted_rows",
                "pending_files", "pending_rows", "active_segments", "endpoints", "new_segments",
                "stale_segments", "failed_segments", "deferred_failed_segments"):
        if key not in attempt or key not in published or attempt[key] != published[key]:
            raise ValueError(f"native attempt/publication counts disagree: {key}")
    if (attempt["active_segments"] != fixture["selected_segments"]
            or any(attempt[key] != 0 for key in ("pending_files", "pending_rows", "new_segments",
                                              "stale_segments", "failed_segments", "deferred_failed_segments"))):
        raise ValueError("native work is not fully terminal for the captured segment set")


def verify(invocation: str, fixture_root: Path, control_path: Path, output: Path) -> dict:
    if (output.exists() or output.is_symlink()
            or not output.resolve().is_relative_to(ROOT / "artifacts/operations")):
        raise ValueError("new private operations output required")
    fixture = json.loads((fixture_root / "fixture.json").read_bytes())
    identity = implementation_identity()
    control_fixture_path = control_path.parent / "fixture.json"
    control_fixture = json.loads(control_fixture_path.read_bytes())
    control_fixture_sha = digest(control_fixture_path)
    paths = [ROOT / "data_openBB/_state/l1_compaction_attempt_latest.json",
             ROOT / "data_openBB/_state/l1_compaction_latest.json", control_path]
    before_owner = owner()
    raw = [path.read_bytes() for path in paths]
    attempt, published, controls = [json.loads(value) for value in raw]
    io_controls = controls.get("contract") == IO_CONTRACT
    if io_controls:
        expected = json.loads(json.dumps(fixture["implementation"]))
        for name in ("scripts/compact_openbb_l1.py", "scripts/openbb_l1_footer_io.py",
                     "scripts/benchmark_openbb_l1_footer_reader.py"):
            expected["code"][name] = identity["code"][name]
        if identity != expected:
            raise ValueError("benchmarked complete implementation closure changed")
        # Preserve and verify every executed control dependency, including the
        # frozen harness. The later harness's factory instrumentation is not
        # imported by the native compactor; it cannot change measured receipts.
        for name, expected_sha in fixture["implementation"]["code"].items():
            if digest(fixture_root / "code-closure" / name) != expected_sha:
                raise ValueError("frozen complete control implementation changed")
        helper_name = "scripts/openbb_l1_footer_io.py"
        validate_metadata_helper_source((fixture_root / "code-closure" / helper_name).read_text(),
                                        (ROOT / helper_name).read_text())
        preserved = fixture_root / "code-closure/scripts/compact_openbb_l1.py"
        if digest(preserved) != fixture["implementation"]["code"]["scripts/compact_openbb_l1.py"]:
            raise ValueError("original complete compactor changed")
        validate_native_source(preserved.read_text(), (ROOT / "scripts/compact_openbb_l1.py").read_text())
        if datetime.fromisoformat(attempt["started_at_utc"]).timestamp() < max(
                (ROOT / name).stat().st_mtime for name in identity["code"]):
            raise ValueError("native owner started before accepted code generation")
        path = fixture_root / "segments.sqlite3"
        if digest(path) != fixture["manifest_sha256"]:
            raise ValueError("fixed complete derivative manifest changed")
        from scripts.benchmark_openbb_l1_footer_reader import file_signatures
        with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as fixed:
            fixed.execute("PRAGMA query_only=ON")
            if file_signatures(fixed) != fixture["source_signatures_sha256"]:
                raise ValueError("complete derivative source generation changed")
            expected_segments = segment_set_digest(fixed)
        with sqlite3.connect((ROOT / "data_openBB/_state/openbb_archive.sqlite3").as_uri() + "?mode=ro", uri=True) as native:
            native.execute("PRAGMA query_only=ON")
            native.execute("BEGIN")
            if segment_set_digest(native) != expected_segments:
                raise ValueError("native derivative set differs from complete control scope")
            native.rollback()
        import polars as pl
        if pl.read_parquet(ROOT / published["status_parquet"]).to_dicts() != published["endpoint_status"]:
            raise ValueError("full native status Parquet readback mismatch")
    elif identity != fixture["implementation"]:
        raise ValueError("exact fixture code/runtime generation changed")
    if (any(trial.get("fixture_sha256") != control_fixture_sha for trial in controls.get("trials", []))
            or any(fixture.get(key) != control_fixture.get(key) for key in (
                "manifest_sha256", "source_signatures_sha256", "selected_segments", "all_success_segments"))):
        raise ValueError("current native fixture and complete controls have different source generations")
    validate(before_owner, invocation, attempt, published, fixture, controls)
    if (owner() != before_owner or any(path.read_bytes() != value for path, value in zip(paths, raw))
            or implementation_identity() != identity):
        raise ValueError("owner/receipts/code generation changed during independent readback")
    result = {
        "contract": "openbb_native_physical_reflection_adoption_v1", "state": "accepted",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(), "owner": before_owner,
        "native_attempt": attempt, "native_publication": published,
        "fixture_sha256": digest(fixture_root / "fixture.json"), "controls_sha256": digest(control_path),
        "control_fixture_sha256": control_fixture_sha,
        "observer_code_sha256": digest(Path(__file__)),
        "implementation": identity, "provider_calls": 0, "services_changed": False,
        "production_writes": False, "cuda_initialized": False,
        "scope": "native policy/complete counts/publication acceptance; not payload value audit or whole-host speed",
        "metadata_io_complete_controls": io_controls,
        "source_set_and_generation_verified": io_controls,
        "complete_module_ast_matches_candidate": io_controls,
        "model_runtime_imported": "torch" in sys.modules,
    }
    if "torch" in sys.modules:
        raise ValueError("source observer imported a model runtime")
    atomic_write_json(output, result)
    print(json.dumps({"state": result["state"], "invocation": invocation,
                      "elapsed_seconds": attempt["elapsed_seconds"],
                      "derivative_metadata_audit": attempt["derivative_metadata_audit"]}), flush=True)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--invocation", required=True)
    parser.add_argument("--fixture", required=True, type=Path)
    parser.add_argument("--controls", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    verify(args.invocation, args.fixture, args.controls, args.output)


if __name__ == "__main__":
    main()
