"""Fresh-process ABBA of the full already-current close finalizer.

All real dates, coverage and input hashes are validated. The original exclusive
lease is represented by a shared lease only inside this read-only benchmark:
neither variant may build/download/write canonical receipts. Real reader/writer
exclusion and upgrade races have separate semantic tests. This measures the
complete validation path, not stale builds, inference or all-host idle usage.
"""
from __future__ import annotations

import argparse
import ast
from contextlib import ExitStack, redirect_stdout
from datetime import datetime, timezone
import fcntl
import hashlib
from importlib import metadata
import io
import json
from pathlib import Path
import resource
import subprocess
import sys
import time
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SOURCE = ROOT / "scripts/finalize_tw_public_completed_session.py"
CONTRACT = "completed_session_validation_abba_v2"
EXTRA_FIELDS = {
    "started_at_taipei", "completed_at_taipei", "lock_wait_ms",
    "derived_validation_contract", "derived_validation_count", "source_lease_mode",
}


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def json_bytes(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def write_once(path: Path, value) -> None:
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")


def outside_main(code: str):
    return [ast.dump(node, include_attributes=False) for node in ast.parse(code).body
            if getattr(node, "name", None) != "main"]


def implementation_identity() -> dict:
    """Fence full verifier dependencies and runtime, not just the changed main.

    Conservative package coverage is inexpensive engineering admission work,
    outside the timed path. It is not an inference cache dependency policy.
    """
    paths = {SOURCE, Path(__file__).resolve()}
    paths.update(ROOT / "scripts" / name for name in (
        "run_tw_public_0830_check.py", "watch_tw_public_publication_group.py",
        "check_tw_day_trade_preopen_readiness.py",
    ))
    for directory in ("downloader", "stockagent"):
        paths.update((ROOT / directory).rglob("*.py"))
    executable = Path(sys.executable).resolve(strict=True)
    return {
        "code_sha256": {str(path): sha(path.read_bytes()) for path in sorted(paths)},
        "runtime": {
            "executable": str(executable), "executable_sha256": sha(executable.read_bytes()),
            "python": sys.version,
            "packages": {name: metadata.version(name)
                         for name in ("numpy", "polars", "pyarrow", "torch")},
        },
    }


def inputs(live: Path, publications: Path) -> list[dict]:
    paths = set(live.glob("*.parquet")) | set(live.glob("*.summary.json"))
    for directory in (live / "stocks", live / "features"):
        paths.update(directory.glob("*.parquet"))
        paths.update(directory.glob("*.json"))
    paths.update(publications.glob("*/latest.json"))

    def dependencies(value):
        rows = value if isinstance(value, list) else [value]
        for value in rows:
            if not isinstance(value, dict):
                continue
            raw = value.get("path") or value.get("name")
            if raw and (value.get("sha256") or value.get("size") is not None):
                path = Path(str(raw)).expanduser()
                paths.add(path if path.is_absolute() else live / path)

    # Match the canonical verifier's input keys, not arbitrary output names in
    # a summary. A feature output name is relative to features/, not live_root.
    for path, keys in (
        (live / "tw_corporate_action_reference.summary.json", ("source_receipts",)),
        (live / "tw_corporate_action_entitlements.summary.json",
         ("reference_receipt", "universe_receipt", "raw_receipt_manifest")),
        (live / "stocks/official_symbol_build_summary.json",
         ("source_receipts", "fallback_source_receipts", "legacy_source_receipts",
          "lifecycle_source_receipts", "session_calendar_receipt", "session_calendar_summary_receipt")),
        (live / "features/tw_public_stock_daily.summary.json", ("source_receipts",)),
    ):
        summary = json.loads(path.read_bytes())
        for key in keys:
            dependencies(summary.get(key))
    result = []
    for path in sorted(paths):
        stat = path.stat()
        result.append({"path": str(path), "resolved": str(path.resolve()),
                       "dev": stat.st_dev, "ino": stat.st_ino,
                       "bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns})
    return result


def capture(fixture: Path, live: Path, publications: Path, expected: str) -> None:
    from scripts import finalize_tw_public_completed_session as module

    original = subprocess.run(
        ["git", "show", "HEAD:scripts/finalize_tw_public_completed_session.py"],
        cwd=ROOT, capture_output=True, check=True,
    ).stdout
    current = SOURCE.read_bytes()
    implementation = implementation_identity()
    if outside_main(original.decode()) != outside_main(current.decode()):
        raise RuntimeError("benchmark must change only the finalizer main orchestration")
    # A nonblocking read guard avoids starting a benchmark behind an actual
    # publisher. Read-only validation is still the authority, never file stats.
    with (live.parent / ".locks/tw-public-refresh.lock").open("a+") as handle:
        fcntl.flock(handle, fcntl.LOCK_SH | fcntl.LOCK_NB)
        before = inputs(live, publications)
        state = module._derived_state(live, expected_date=expected,
                                     session_date=datetime.now(module.TAIPEI).date().isoformat())
        if (not state["current"] or inputs(live, publications) != before
                or implementation_identity() != implementation):
            raise RuntimeError("full current source validation or stable input capture failed")
    fixture.mkdir(parents=True, exist_ok=False)
    with (fixture / "original.py").open("xb") as stream:
        stream.write(original)
    write_once(fixture / "fixture.json", {
        "contract": CONTRACT, "expected_date": expected,
        "live_root": str(live), "publications": str(publications),
        "original_code_sha256": sha(original), "candidate_code_sha256": sha(current),
        "source_inputs": before, "source_inputs_sha256": sha(json_bytes(before)),
        "implementation": implementation,
        "implementation_sha256": sha(json_bytes(implementation)),
        "complete_current_state": state, "captured_at_utc": datetime.now(timezone.utc).isoformat(),
    })
    print(json.dumps({"captured": str(fixture), "input_files": len(before), "complete": True}))


def worker(fixture: Path, variant: str, output: Path) -> None:
    from scripts import finalize_tw_public_completed_session as module
    import requests

    saved = json.loads((fixture / "fixture.json").read_bytes())
    if saved.get("contract") != CONTRACT:
        raise RuntimeError("capture a fresh v2 fixture; preserve earlier incomplete trials")
    if (saved.get("implementation") != implementation_identity()
            or saved.get("implementation_sha256") != sha(json_bytes(saved["implementation"]))):
        raise RuntimeError("verifier dependency or runtime generation changed")
    original = (fixture / "original.py").read_bytes()
    if (sha(original) != saved["original_code_sha256"]
            or sha(SOURCE.read_bytes()) != saved["candidate_code_sha256"]
            or outside_main(original.decode()) != outside_main(SOURCE.read_text())):
        raise RuntimeError("code generation changed")
    if variant == "original":
        node = next(node for node in ast.parse(original).body if getattr(node, "name", None) == "main")
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(SOURCE), "exec"), module.__dict__)
    live, publications = Path(saved["live_root"]), Path(saved["publications"])
    args = SimpleNamespace(live_root=live, publication_root=publications,
                           receipt=fixture / "never-written-canonical-receipt.json",
                           expected_date=saved["expected_date"], publication_phase=None,
                           workers=8, step_attempts=4, step_retry_base_seconds=1.0,
                           public_feature_incremental_days=14, force=False)
    receipts, lease_requests = [], []
    real_flock = fcntl.flock
    validate = module._derived_state
    validation_count = 0

    def forbidden(*a, **k):
        raise AssertionError("full already-current validation attempted a provider/build operation")

    def read_only_flock(fd, operation):
        lease_requests.append(operation)
        return real_flock(fd, fcntl.LOCK_SH if operation == fcntl.LOCK_EX else operation)

    def counted_validation(*a, **k):
        nonlocal validation_count
        validation_count += 1
        result = validate(*a, **k)
        if not result["current"]:
            raise RuntimeError("source no longer current; benchmark cannot repair it")
        return result

    with ExitStack() as stack:
        stack.enter_context(patch.object(module, "parse_args", lambda: args))
        stack.enter_context(patch.object(module, "_derived_state", counted_validation))
        stack.enter_context(patch.object(module, "_build_commands", forbidden))
        stack.enter_context(patch.object(module, "_atomic_json", lambda path, value: receipts.append(value)))
        stack.enter_context(patch.object(module.subprocess, "run", forbidden))
        stack.enter_context(patch.object(requests.Session, "request", forbidden))
        stack.enter_context(patch.object(module.fcntl, "flock", read_only_flock))
        torch = sys.modules.get("torch")
        if torch is not None:
            if torch.cuda.is_initialized():
                raise RuntimeError("benchmark must start CUDA-cold")
            for name in ("init", "is_available", "synchronize", "empty_cache"):
                stack.enter_context(patch.object(torch.cuda, name, forbidden))
        before = inputs(live, publications)
        if before != saved["source_inputs"]:
            raise RuntimeError("full source inputs changed before this fresh worker")
        started, cpu = time.perf_counter(), time.process_time()
        with redirect_stdout(io.StringIO()):
            exit_code = module.main()
        wall_seconds, cpu_seconds = time.perf_counter()-started, time.process_time()-cpu
        if (inputs(live, publications) != before
                or sha(SOURCE.read_bytes()) != saved["candidate_code_sha256"]
                or implementation_identity() != saved["implementation"]):
            raise RuntimeError("source or code changed during validation")
        if exit_code or len(receipts) != 1:
            raise RuntimeError("whole finalizer did not produce exactly one successful receipt")
        if torch is not None and torch.cuda.is_initialized():
            raise RuntimeError("validation initialized CUDA")
    receipt = receipts[0]
    if not (receipt["status"] == "ok" and receipt["reused"] and not receipt["steps"]
            and receipt["before"] == receipt["after"] == saved["complete_current_state"]):
        raise RuntimeError("whole completed-state output changed")
    if validation_count != (2 if variant == "original" else 1):
        raise RuntimeError("validation work count differs from the expected full workflow")
    semantics = {key: value for key, value in receipt.items() if key not in EXTRA_FIELDS}
    write_once(output, {
        "state": "completed_trial", "variant": variant,
        "wall_seconds": wall_seconds, "cpu_seconds": cpu_seconds,
        "rss_peak_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        "complete_receipt": receipt, "semantic_receipt_sha256": sha(json_bytes(semantics)),
        "source_inputs_sha256": saved["source_inputs_sha256"], "input_files": len(before),
        "validation_count": validation_count, "requested_lease_operations": lease_requests,
        "actual_source_lease": "shared_benchmark_only", "provider_calls": 0,
        "canonical_writes": False, "cuda_initialized": False,
        "scope": "entire_already_current_finalizer_including_all_dates_coverage_and_dependency_hashes",
        "original_code_sha256": saved["original_code_sha256"],
        "candidate_code_sha256": saved["candidate_code_sha256"],
        "contract": CONTRACT, "implementation_sha256": saved["implementation_sha256"],
    })
    print(json.dumps({"variant": variant, "wall_seconds": wall_seconds,
                      "cpu_seconds": cpu_seconds, "validation_count": validation_count}))


def aggregate(paths: list[Path], output: Path) -> None:
    import statistics

    rows = [json.loads(path.read_bytes()) for path in paths]
    if len(rows) != 4 or [row["variant"] for row in rows] != ["original", "candidate", "candidate", "original"]:
        raise RuntimeError("require all four complete fresh-process ABBA trials")
    if any(row.get("state") != "completed_trial" or row.get("provider_calls") != 0
           or row.get("canonical_writes") is not False or row.get("cuda_initialized") is not False
           for row in rows):
        raise RuntimeError("all four trials must be complete, read-only and CUDA-cold")
    if any(row.get("contract") != CONTRACT for row in rows):
        raise RuntimeError("all four trials must use the current bound implementation contract")
    for key in ("semantic_receipt_sha256", "source_inputs_sha256", "original_code_sha256",
                "candidate_code_sha256", "implementation_sha256"):
        if len({row[key] for row in rows}) != 1:
            raise RuntimeError(f"ABBA input/output generation mismatch: {key}")
    metrics = {}
    for name in ("wall_seconds", "cpu_seconds", "rss_peak_bytes"):
        a = statistics.mean(row[name] for row in rows if row["variant"] == "original")
        b = statistics.mean(row[name] for row in rows if row["variant"] == "candidate")
        metrics[name] = {"original_mean": a, "candidate_mean": b, "reduction_percent": (1-b/a)*100}
    write_once(output, {"state": "accepted_full_validation_abba", "trials": rows,
                        "contract": CONTRACT, "implementation_sha256": rows[0]["implementation_sha256"],
                        "metrics": metrics, "scope": rows[0]["scope"],
                        "exact_complete_semantic_receipt": True, "uncontrolled_os_page_cache": True,
                        "native_adoption_verified": False, "stale_build_speedup_claimed": False})
    print(json.dumps(metrics))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--capture", action="store_true")
    parser.add_argument("--live-root", type=Path, default=Path("/srv/stockagent-live/data_tw_public"))
    parser.add_argument("--publication-root", type=Path, default=ROOT / "artifacts/data_refresh/tw_public/publications")
    parser.add_argument("--expected-date")
    parser.add_argument("--variant", choices=("original", "candidate"))
    parser.add_argument("--aggregate", type=Path, nargs=4)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    fixture = args.fixture.absolute()
    if not fixture.resolve().is_relative_to((ROOT / "artifacts/benchmarks").resolve()):
        parser.error("fixture must be a private engineering benchmark")
    if args.capture:
        if not args.expected_date:
            parser.error("capture requires an exact --expected-date")
        capture(fixture, args.live_root.resolve(), args.publication_root.resolve(), args.expected_date)
    else:
        if args.output is None or not args.output.resolve().is_relative_to(fixture.resolve()) or args.output.exists():
            parser.error("use a new output under this private benchmark fixture")
        if args.aggregate:
            aggregate(args.aggregate, args.output)
        elif args.variant:
            worker(fixture, args.variant, args.output)
        else:
            parser.error("choose capture, variant or aggregate")


if __name__ == "__main__":
    main()
