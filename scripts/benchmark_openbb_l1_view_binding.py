"""Compare Parquet view binding on frozen, receipt-checked L1 path lists.

Only the input manifest and Parquet files are read. Each trial is a fresh
process and isolated in-memory DuckDB; no production view is published.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import resource
import sqlite3
import subprocess
import sys
import time


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _view_sql(segments: list[dict[str, object]], variant: str) -> str:
    if not segments:
        raise ValueError("at least one segment is required")
    if variant == "baseline":
        paths = ",".join(_sql_string(str(row["output_path"])) for row in segments)
        return f"SELECT * FROM read_parquet([{paths}], union_by_name=true)"
    if variant != "schema_groups":
        raise ValueError(f"unknown variant: {variant}")
    grouped: dict[str, list[str]] = {}
    for row in segments:
        fingerprint = str(row["schema_fingerprint"])
        if len(fingerprint) != 64:
            raise ValueError("missing or invalid schema fingerprint")
        grouped.setdefault(fingerprint, []).append(str(row["output_path"]))
    return " UNION ALL BY NAME ".join(
        "SELECT * FROM read_parquet(["
        + ",".join(_sql_string(path) for path in paths)
        + "], union_by_name=false)"
        for paths in grouped.values()
    )


def _file_signature(path: Path) -> tuple[int, int, int, int, int]:
    stat = path.stat()
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def _validate_segments(
    segments: list[dict[str, object]],
    *, production_proofs: dict[str, dict[str, object]] | None = None,
) -> dict[str, tuple[int, int, int, int, int]]:
    import pyarrow.parquet as pq

    signatures = {}
    for row in segments:
        path = Path(str(row["output_path"]))
        before = _file_signature(path)
        parquet = pq.ParquetFile(path)
        fingerprint = hashlib.sha256(
            parquet.schema_arrow.remove_metadata().serialize().to_pybytes()
        ).hexdigest()
        if (
            parquet.metadata.num_rows != row["output_rows"]
            or before[2] != row["output_bytes"]
            or fingerprint != row["schema_fingerprint"]
            or _file_signature(path) != before
        ):
            raise RuntimeError(f"segment contract mismatch: {path}")
        signatures[str(path)] = before
        if production_proofs is not None:
            from scripts.compact_openbb_l1 import _view_source_proof

            proof = _view_source_proof(
                path, str(row["segment_id"]), parquet, before,
                output_rows=int(row["output_rows"]), output_bytes=int(row["output_bytes"]),
                arrow_fingerprint=str(row["schema_fingerprint"]),
            )
            production_proofs[str(path)] = asdict(proof)
    return signatures


def _pinned_baseline(path: Path, expected_sha256: str) -> tuple[dict[str, object], dict[str, object]]:
    content = path.read_bytes()
    observed = hashlib.sha256(content).hexdigest()
    if observed != expected_sha256:
        raise RuntimeError("historical baseline receipt SHA256 mismatch")
    receipt = json.loads(content)
    if (
        receipt.get("state") != "verified"
        or receipt.get("row_verification_performed") is not True
        or receipt.get("schemas_equal") is not True
        or receipt.get("row_multisets_equal") is not True
        or receipt.get("all_source_footers_validated") is not True
        or receipt.get("source_signatures_unchanged") is not True
    ):
        raise RuntimeError("historical baseline receipt lacks complete row/source verification")
    baselines = [trial for trial in receipt.get("trials", []) if trial.get("variant") == "baseline"]
    if not baselines:
        raise RuntimeError("historical receipt contains no original baseline trial")
    reference = baselines[0]
    digest = reference.get("row_multiset_sha256")
    if (
        not isinstance(digest, str) or len(digest) != 64
        or any(char not in "0123456789abcdef" for char in digest)
        or type(reference.get("row_count")) is not int
        or reference["row_count"] != receipt.get("expected_rows")
        or not reference.get("columns") or not reference.get("duckdb_version")
        or any(any(trial.get(key) != reference.get(key) for key in (
            "columns", "row_count", "row_multiset_sha256", "duckdb_version",
        )) for trial in baselines)
    ):
        raise RuntimeError("historical baseline trials are incomplete or disagree")
    return receipt, reference


def _resources() -> dict[str, int]:
    result = {"peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024}
    for line in Path("/proc/self/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            result["rss_bytes"] = int(line.split()[1]) * 1024
    for line in Path("/proc/self/io").read_text().splitlines():
        key, _, value = line.partition(":")
        if key in {"read_bytes", "write_bytes", "rchar"}:
            result[key] = int(value)
    return result


def _host_memory() -> dict[str, int]:
    return {
        key: int(value.split()[0]) * 1024
        for line in Path("/proc/meminfo").read_text().splitlines()
        for key, _, value in (line.partition(":"),)
        if key in {"MemTotal", "MemAvailable", "SwapTotal", "SwapFree"}
    }


def _implementation_hashes(production: bool) -> dict[str, str]:
    names = ["scripts/benchmark_openbb_l1_view_binding.py"]
    if production:
        names += ["scripts/compact_openbb_l1.py", "scripts/openbb_l1_view_schema.py"]
    return {name: hashlib.sha256((REPO_ROOT / name).read_bytes()).hexdigest() for name in names}


def _worker(payload: dict[str, object]) -> dict[str, object]:
    address_limit = 8 * 1024**3
    resource.setrlimit(resource.RLIMIT_AS, (address_limit, address_limit))
    import duckdb

    segments = payload["segments"]
    production = payload["variant"] == "production_schema_groups"
    implementation = _implementation_hashes(production)
    if implementation != payload["implementation_sha256"]:
        raise RuntimeError("benchmark implementation changed before worker binding")
    sql = None if production else _view_sql(segments, str(payload["variant"]))
    signatures = {Path(path): tuple(value) for path, value in payload["source_signatures"].items()}
    if any(_file_signature(path) != old for path, old in signatures.items()):
        raise RuntimeError("selected segment changed before worker binding")
    observations = {"before_connect": _resources()}
    connection = duckdb.connect(
        ":memory:", config={"threads": 2, "memory_limit": "1GB"}
    )
    observations["after_connect"] = _resources()
    started = time.perf_counter()
    try:
        production_plan = None
        plan_seconds = None
        physical_groups = None
        if production:
            from scripts.compact_openbb_l1 import ViewSourceProof, _schema_grouped_view

            proofs = {
                path: ViewSourceProof(**{**proof, "file_signature": tuple(proof["file_signature"])})
                for path, proof in payload["production_proofs"].items()
            }
            production_plan = _schema_grouped_view(
                connection, [Path(str(row["output_path"])) for row in segments],
                Path(str(payload["output_dir"])), proofs,
            )
            if production_plan is None:
                raise RuntimeError("production schema-group policy rejected the selected sources; no fallback benchmark")
            sql = production_plan[0]
            physical_groups = len({proof.physical_identity for proof in proofs.values()})
            plan_seconds = round(time.perf_counter() - started, 6)
            observations["after_production_group_plan"] = _resources()
        connection.execute("CREATE VIEW candidate AS " + sql)
        bind_seconds = time.perf_counter() - started
        observations["after_bind"] = _resources()
        description = connection.execute("SELECT * FROM candidate LIMIT 0").description
        columns = [(item[0], str(item[1])) for item in description]
        observations["after_describe"] = _resources()
        digest = hashlib.sha256()
        rows = 0
        validation_started = time.perf_counter()
        if payload["verify_rows"]:
            cursor = connection.execute(
                "SELECT sha256(to_json(t)) AS digest, COUNT(*) AS multiplicity "
                "FROM candidate t GROUP BY 1 ORDER BY 1"
            )
            while batch := cursor.fetchmany(4096):
                for value, count in batch:
                    digest.update(bytes.fromhex(value))
                    digest.update(int(count).to_bytes(8, "little"))
                    rows += int(count)
        observations["after_validation"] = _resources()
        if any(_file_signature(path) != old for path, old in signatures.items()):
            raise RuntimeError("selected segment changed during worker binding or validation")
        if _implementation_hashes(production) != implementation:
            raise RuntimeError("benchmark implementation changed during worker execution")
        return {
            "variant": payload["variant"], "duckdb_version": duckdb.__version__,
            "address_space_limit_bytes": address_limit,
            "duckdb_memory_limit": "1GB", "duckdb_threads": 2,
            "nice": os.getpriority(os.PRIO_PROCESS, 0),
            "bind_seconds": round(bind_seconds, 6),
            "production_group_plan_seconds": plan_seconds,
            "physical_schema_groups": physical_groups,
            "production_view_input_signature": production_plan[1] if production_plan else None,
            "production_reader_policy": production_plan[2] if production_plan else None,
            "implementation_sha256": implementation,
            "validation_seconds": round(time.perf_counter() - validation_started, 6),
            "columns": columns, "sql_bytes": len(sql.encode()),
            "sql_sha256": hashlib.sha256(sql.encode()).hexdigest(),
            "row_count": rows if payload["verify_rows"] else None,
            "row_multiset_sha256": digest.hexdigest() if payload["verify_rows"] else None,
            "resources": observations,
        }
    finally:
        connection.close()


def benchmark(
    manifest: Path, *, endpoint: str, max_segments: int,
    sequence: tuple[str, ...] = ("baseline", "schema_groups", "schema_groups", "baseline"),
    verify_rows: bool = True, trial_timeout: float = 120,
    created_at_before: str | None = None, expected_contract_sha256: str | None = None,
    baseline_receipt: Path | None = None, baseline_receipt_sha256: str | None = None,
) -> dict[str, object]:
    allowed_variants = {"baseline", "schema_groups", "production_schema_groups"}
    if not sequence or not set(sequence) <= allowed_variants:
        raise ValueError("benchmark sequence contains no trials or an unknown variant")
    if (baseline_receipt is None) != (baseline_receipt_sha256 is None):
        raise ValueError("historical baseline requires both receipt path and SHA256")
    if "baseline" not in sequence and baseline_receipt is None:
        raise ValueError("candidate-only verification requires a hash-pinned historical baseline")
    if baseline_receipt is not None and not verify_rows:
        raise ValueError("historical baseline comparison requires complete row verification")
    if baseline_receipt_sha256 is not None and (
        len(baseline_receipt_sha256) != 64
        or any(char not in "0123456789abcdef" for char in baseline_receipt_sha256)
    ):
        raise ValueError("historical baseline SHA256 must be 64 lowercase hex characters")
    if "production_schema_groups" in sequence and endpoint != "regulators.sec.filing_headers":
        raise ValueError("production schema groups are limited to SEC filing_headers")
    historical, historical_reference = (
        _pinned_baseline(baseline_receipt, baseline_receipt_sha256)
        if baseline_receipt is not None else (None, None)
    )
    host_memory_before = _host_memory()
    connection = sqlite3.connect(f"{manifest.resolve().as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        clause = " AND created_at<=?" if created_at_before is not None else ""
        parameters = (endpoint, created_at_before, max_segments) if clause else (endpoint, max_segments)
        segments = [dict(row) for row in connection.execute(
            "SELECT segment_id, output_path, output_rows, output_bytes, schema_fingerprint "
            "FROM l1_compaction_segments WHERE status='success' AND endpoint=?"
            + clause + " ORDER BY segment_id LIMIT ?", parameters,
        )]
    finally:
        connection.close()
    if not segments:
        raise ValueError("no successful segments selected")
    contract_sha256 = hashlib.sha256(
        json.dumps(segments, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    if expected_contract_sha256 is not None and contract_sha256 != expected_contract_sha256:
        raise RuntimeError("ordered source contract does not match the requested frozen snapshot")
    expected_rows = sum(int(row["output_rows"]) for row in segments)
    if historical is not None and any(historical.get(key) != value for key, value in (
        ("endpoint", endpoint), ("segments", len(segments)), ("expected_rows", expected_rows),
        ("ordered_source_contract_sha256", contract_sha256), ("created_at_before", created_at_before),
    )):
        raise RuntimeError("historical baseline source contract does not match selected sources")
    production_proofs = {} if "production_schema_groups" in sequence else None
    implementations = {
        variant: _implementation_hashes(variant == "production_schema_groups")
        for variant in set(sequence)
    }
    footer_started = time.perf_counter()
    signatures = _validate_segments(segments, production_proofs=production_proofs)
    footer_seconds = round(time.perf_counter() - footer_started, 6)
    if any(_implementation_hashes(variant == "production_schema_groups") != expected
           for variant, expected in implementations.items()):
        raise RuntimeError("benchmark implementation changed during footer validation")
    trials = []
    for variant in sequence:
        payload = {
            "segments": segments, "variant": variant, "verify_rows": verify_rows,
            "source_signatures": signatures, "production_proofs": production_proofs,
            "output_dir": str(manifest.resolve().parent.parent),
            "implementation_sha256": implementations[variant],
        }
        completed = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--worker"],
            input=json.dumps(payload), text=True, capture_output=True,
            timeout=trial_timeout, check=True,
        )
        trials.append(json.loads(completed.stdout))
        if any(_file_signature(Path(path)) != old for path, old in signatures.items()):
            raise RuntimeError("selected segment changed during benchmark")
    reference = historical_reference or next(trial for trial in trials if trial["variant"] == "baseline")
    if any(trial["duckdb_version"] != reference["duckdb_version"] for trial in trials):
        raise RuntimeError("baseline and candidate DuckDB versions differ")
    schemas_equal = all(trial["columns"] == reference["columns"] for trial in trials)
    rows_equal = all(
        trial["row_count"] == expected_rows
        and trial["row_multiset_sha256"] == reference["row_multiset_sha256"]
        for trial in trials
    ) if verify_rows else None
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "state": "verified" if schemas_equal and rows_equal else "not_fully_verified",
        "source_open_mode": "read_only", "production_writes": False,
        "endpoint": endpoint, "segments": len(segments),
        "created_at_before": created_at_before,
        "schema_groups": len({row["schema_fingerprint"] for row in segments}),
        "physical_schema_groups": (
            len({proof["physical_identity"] for proof in production_proofs.values()})
            if production_proofs is not None else None
        ),
        "candidate_policy": "production_schema_groups" if production_proofs is not None else "arrow_fingerprint_benchmark_only",
        "source_footer_validation_seconds": footer_seconds,
        "expected_rows": expected_rows,
        "comparison_mode": "hash_pinned_historical_baseline" if historical is not None else "fresh_baseline",
        "historical_baseline": None if historical is None else {
            "receipt_path": str(baseline_receipt.resolve()),
            "receipt_sha256": baseline_receipt_sha256,
            "generated_at_utc": historical["generated_at_utc"],
            "reference_trial": historical_reference,
            "reference_trial_index": historical["trials"].index(historical_reference),
            "paired_timing_comparison": False,
        },
        "ordered_source_contract_sha256": contract_sha256,
        "all_source_footers_validated": True, "source_signatures_unchanged": True,
        "schemas_equal": schemas_equal, "row_multisets_equal": rows_equal,
        "row_verification_performed": verify_rows,
        "execution_limits": {
            "worker_concurrency": 1, "trial_timeout_seconds": trial_timeout,
            "address_space_limit_bytes": 8 * 1024**3,
            "duckdb_memory_limit": "1GB", "duckdb_threads": 2,
            "nice": os.getpriority(os.PRIO_PROCESS, 0),
            "address_limit_is_not_cgroup_rss_cap": True,
        },
        "host_memory_before_bytes": host_memory_before,
        "host_memory_after_bytes": _host_memory(),
        "trials": trials,
        "limits": [
            "Fresh in-memory DuckDB processes; not full service or cloned-catalog memory.",
            "Row comparison is a SHA256 multiset over canonical DuckDB row JSON, not row order.",
            "Grouped UNION ALL BY NAME may have different type-promotion semantics; schema and rows must match.",
            "Binding RSS is separate from row validation; OS cache and host load may affect time.",
            "A historical comparison proves the selected source contract and complete output parity, not historical source-byte identity or paired speedup.",
        ],
    }


def main() -> int:
    if sys.argv[1:] == ["--worker"]:
        print(json.dumps(_worker(json.load(sys.stdin))))
        return 0
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path("data_openBB/_state/openbb_archive.sqlite3"))
    parser.add_argument("--endpoint", default="regulators.sec.filing_headers")
    parser.add_argument("--max-segments", type=int, default=1024)
    parser.add_argument("--skip-row-verification", action="store_true")
    parser.add_argument("--created-at-before")
    parser.add_argument("--expected-contract-sha256")
    parser.add_argument("--sequence", choices=("ab", "abba", "candidate"), default="abba")
    parser.add_argument("--candidate-policy", choices=("arrow_fingerprint", "production"), default="arrow_fingerprint")
    parser.add_argument("--baseline-receipt", type=Path)
    parser.add_argument("--baseline-receipt-sha256")
    parser.add_argument("--trial-timeout", type=float, default=120)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.max_segments <= 0:
        parser.error("--max-segments must be positive")
    if args.trial_timeout <= 0:
        parser.error("--trial-timeout must be positive")
    if not args.output.resolve().is_relative_to(Path("artifacts/benchmarks").resolve()):
        parser.error("--output must be under artifacts/benchmarks")
    if (args.baseline_receipt is None) != (args.baseline_receipt_sha256 is None):
        parser.error("historical baseline requires both --baseline-receipt and --baseline-receipt-sha256")
    if args.sequence == "candidate" and args.baseline_receipt is None:
        parser.error("--sequence candidate requires a hash-pinned --baseline-receipt")
    if args.baseline_receipt is not None:
        if args.skip_row_verification:
            parser.error("historical baseline comparison requires complete row verification")
        if args.output.resolve() == args.baseline_receipt.resolve():
            parser.error("--output cannot overwrite the historical baseline receipt")
    candidate = "production_schema_groups" if args.candidate_policy == "production" else "schema_groups"
    sequence = (candidate,) if args.sequence == "candidate" else (
        ("baseline", candidate) if args.sequence == "ab" else ("baseline", candidate, candidate, "baseline")
    )
    os.nice(15)
    result = benchmark(
        args.manifest, endpoint=args.endpoint, max_segments=args.max_segments,
        verify_rows=not args.skip_row_verification,
        sequence=sequence,
        trial_timeout=args.trial_timeout, created_at_before=args.created_at_before,
        expected_contract_sha256=args.expected_contract_sha256,
        baseline_receipt=args.baseline_receipt,
        baseline_receipt_sha256=args.baseline_receipt_sha256,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: result[key] for key in ("state", "segments", "schema_groups", "schemas_equal", "row_multisets_equal")}))
    return 0 if result["state"] == "verified" else 2


if __name__ == "__main__":
    raise SystemExit(main())
