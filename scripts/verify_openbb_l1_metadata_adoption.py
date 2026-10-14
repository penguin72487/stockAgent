"""Read-only acceptance of a terminal normal OpenBB owner and its audit policy.

Bind exact invocation, fixture/code generation, native publication/counts and
the independent complete audit controls. This is not a payload bitrot audit or
a claim that all provider history is complete. No service action is performed.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json  # noqa: E402
from scripts.benchmark_openbb_l1_footer_reader import digest, implementation_identity  # noqa: E402

UNIT = "stockagent-openbb-l1-compaction.service"
POLICY = "openbb_l1_physical_reflection_cache_v1"


def owner() -> dict[str, str]:
    result = subprocess.run([
        "systemctl", "show", UNIT,
        "--property=ActiveState,SubState,MainPID,InvocationID,Result,ExecMainStatus",
    ], check=True, capture_output=True, text=True)
    return dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)


def validate(unit: dict, invocation: str, attempt: dict, published: dict,
             fixture: dict, controls: dict) -> None:
    if (not re.fullmatch(r"[0-9a-f]{32}", invocation)
            or unit != {"ActiveState": "inactive", "SubState": "dead", "MainPID": "0",
                        "InvocationID": invocation, "Result": "success", "ExecMainStatus": "0"}
            or attempt.get("state") != "completed" or attempt.get("exit_code") != 0
            or attempt.get("publication_stage") != "status_published"
            or published.get("state") != "completed"
            or any(value.get("systemd_invocation_id") != invocation for value in (attempt, published))
            or fixture.get("all_success_segments") is not True
            or controls.get("state") != "accepted_phase_comparison"
            or controls.get("candidate") != "schema_cache"
            or controls.get("exact_canonical_complete_output_parity") is not True
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
    if identity != fixture["implementation"]:
        raise ValueError("exact fixture code/runtime generation changed")
    control_fixture_path = control_path.parent / "fixture.json"
    control_fixture = json.loads(control_fixture_path.read_bytes())
    control_fixture_sha = digest(control_fixture_path)
    paths = [ROOT / "data_openBB/_state/l1_compaction_attempt_latest.json",
             ROOT / "data_openBB/_state/l1_compaction_latest.json", control_path]
    before_owner = owner()
    raw = [path.read_bytes() for path in paths]
    attempt, published, controls = [json.loads(value) for value in raw]
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
    }
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
