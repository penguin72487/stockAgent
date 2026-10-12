#!/usr/bin/env python3
"""Inventory data-processing engines and candidate costs in the entire checkout.

Static call sites are review candidates, never proofs of slowness, correctness,
source completeness or the need to replace an engine. No data/API/GPU access.
"""

from __future__ import annotations

import argparse
import ast
from collections import Counter
import csv
from datetime import UTC, datetime
import hashlib
from io import StringIO
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_text

ENGINES = {"pandas", "polars", "pyarrow", "duckdb", "numpy", "torch", "sqlite3", "psycopg", "psycopg2"}
METHODS = {"read_parquet", "scan_parquet", "read_csv", "scan_csv", "iter_batches", "ParquetWriter",
           "to_pandas", "to_numpy", "collect", "map_elements", "map_batches", "iter_rows", "iterrows",
           "to_dicts", "to_pylist", "memmap"}


def classify(path: str) -> str:
    name = Path(path).name
    if path.startswith(("test/", "tests/")) and not name.startswith("benchmark"):
        return "test"
    if any(word in name for word in ("benchmark", "profile", "probe")):
        return "benchmark_profile_probe"
    return "source"


def inspect_source(path: Path) -> dict:
    before = path.stat()
    content = path.read_bytes()
    after = path.stat()
    if (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
        after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns
    ):
        raise ValueError("file changed during static inventory")
    tree = ast.parse(content, filename=str(path))
    engines: set[str] = set()
    calls: Counter[str] = Counter()
    candidates = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            engines.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            engines.add(node.module.split(".")[0])
        elif isinstance(node, ast.Call):
            name = (node.func.attr if isinstance(node.func, ast.Attribute)
                    else node.func.id if isinstance(node.func, ast.Name) else "")
            if name in METHODS:
                calls[name] += 1
                if name in {"to_pandas", "map_elements", "iterrows"} or (
                    name == "read_parquet" and not any(k.arg == "columns" for k in node.keywords)
                ):
                    candidates.append({"line": node.lineno, "call": name, "evidence_level": "static_candidate_only"})
    relative = str(path.relative_to(ROOT))
    return {"path": relative, "kind": classify(relative), "sha256": hashlib.sha256(content).hexdigest(),
            "engines": sorted(engines & ENGINES), "methods": dict(sorted(calls.items())),
            "review_candidates": candidates, "measurement_status": "not_a_whole_workflow_benchmark"}


def audit(output: Path) -> dict:
    paths = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=ROOT
    ).decode().split("\0")
    rows, errors = [], []
    for relative in sorted(set(name for name in paths if name.endswith(".py"))):
        path = ROOT / relative
        if not path.is_file():
            continue  # preserve tracked user deletions
        try:
            rows.append(inspect_source(path))
        except (OSError, SyntaxError, ValueError) as exc:
            errors.append({"path": relative, "error_type": type(exc).__name__, "reason": str(exc)})
    counts = Counter(row["kind"] for row in rows)
    source_rows = [row for row in rows if row["kind"] == "source"]
    csv_path = output.with_suffix(".csv")
    csv_buffer = StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=["path", "kind", "sha256", "engines", "to_pandas",
        "map_elements", "iterrows", "read_parquet", "iter_batches", "memmap", "measurement_status"])
    writer.writeheader()
    for row in rows:
        writer.writerow({"path": row["path"], "kind": row["kind"], "sha256": row["sha256"],
            "engines": "|".join(row["engines"]), **{key: row["methods"].get(key, 0) for key in
                ("to_pandas", "map_elements", "iterrows", "read_parquet", "iter_batches", "memmap")},
            "measurement_status": row["measurement_status"]})
    atomic_write_text(csv_path, csv_buffer.getvalue())
    report = {
        "schema_version": 1, "state": "accepted_static_inventory" if not errors else "incomplete_static_inventory",
        "observed_at_utc": datetime.now(UTC).isoformat(), "git_head": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "files": len(rows), "by_kind": dict(counts), "parse_errors": errors,
        "source_engine_counts": {engine: sum(engine in row["engines"] for row in source_rows)
                                 for engine in sorted(ENGINES)},
        "source_direct_pandas_paths": [row["path"] for row in source_rows if "pandas" in row["engines"]],
        "csv_path": str(csv_path), "rows": rows,
        "scope": "Git-tracked and unignored Python source, tests and benchmark entrypoints in this checkout",
        "excludes": ["ignored/generated/data/vendor trees", "third-party SDK implementations", "non-Python code",
                     "Python embedded in generated notebook strings"],
        "interpretation": "counts/call-site candidates are not performance or correctness percentages; use paired workflow receipts",
        "provider_calls": 0, "production_writes": False, "global_optimal_claim": False,
    }
    atomic_write_json(output, report)
    return {key: report[key] for key in ("state", "files", "by_kind", "source_engine_counts", "parse_errors", "csv_path")}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("a NEW output path is required")
    import json
    print(json.dumps(audit(args.output), ensure_ascii=False))
