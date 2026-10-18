"""Compare the entire retained MOPS bulk-HTML corpus without upstream calls.

This measures the pure parsing phase, not a complete entitlement downloader or
publication. Freeze every bulk response privately, run the unedited Git control
and candidate in fresh processes, retain failures, and require full row parity.
Cold cache construction is separate from the warm ABBA comparison. Neither
variant changes source receipts, a production cache, service or source lease.
"""
from __future__ import annotations

import argparse
import ast
from datetime import UTC, date, datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import resource
import shutil
import subprocess
import sys
import time

from downloader.artifact_io import atomic_write_bytes, atomic_write_json


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def signature(path):
    item = path.stat()
    return [getattr(item, key) for key in
            ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")]


def validate_source_pair(reference, candidate):
    """Every existing function except the two cache callers stays unchanged."""
    functions = lambda source: {
        node.name: ast.dump(node, include_attributes=False)
        for node in ast.parse(source).body if isinstance(node, ast.FunctionDef)
    }
    old, new = functions(reference), functions(candidate)
    for name in old.keys() - {"_fetch_bulk_dividends", "_recover_retained_bulk_rows"}:
        if old[name] != new.get(name):
            raise ValueError(f"unrelated parser/workflow change: {name}")


def load_module(path, label):
    spec = importlib.util.spec_from_file_location(label, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[label] = module
    spec.loader.exec_module(module)
    return module


def typed_json(value):
    if isinstance(value, date):
        return {"date_type": value.isoformat()}
    raise TypeError(type(value).__name__)


def worker(args):
    fixture = json.loads((args.root / "fixture.json").read_bytes())
    module_path = args.root / ("reference.py" if args.mode == "reference" else "candidate.py")
    module = load_module(module_path, "bulk_cache_benchmark_" + args.mode)
    # Any accidental provider request is a hard failure, not hidden latency.
    def no_request(*_args, **_kwargs):
        raise AssertionError("provider I/O is outside this pure-parser benchmark")
    module.requests.sessions.Session.request = no_request
    original_parse = module.parse_mops_bulk_dividends
    parse_counts = {"successful": 0, "failed": 0}
    def counted_parse(*values, **options):
        try:
            rows = original_parse(*values, **options)
        except Exception:
            parse_counts["failed"] += 1
            raise
        parse_counts["successful"] += 1
        return rows
    module.parse_mops_bulk_dividends = counted_parse
    results = []
    start, cpu = time.monotonic(), time.process_time()
    for item in fixture["files"]:
        content = (args.root / "source" / item["name"]).read_bytes()
        if sha(content) != item["sha256"] or len(content) != item["bytes"]:
            raise ValueError("private bulk-response fixture changed")
        key = module.BulkDividendKey(item["market"], item["roc_year"])
        try:
            if args.mode == "reference":
                rows = module.parse_mops_bulk_dividends(content, key=key)
            else:
                rows = module._parse_mops_bulk_dividends_cached(
                    content, key=key, cache_root=args.root / "cache")
            encoded = json.dumps(rows, default=typed_json, allow_nan=False,
                                 ensure_ascii=False, separators=(",", ":")).encode()
            result = {"name": item["name"], "rows": len(rows), "row_sha256": sha(encoded)}
        except (ValueError, TypeError) as exc:
            result = {"name": item["name"], "error": type(exc).__name__ + ": " + str(exc)}
        results.append(result)
    receipt = {
        "mode": args.mode, "wall_seconds": time.monotonic() - start,
        "cpu_seconds": time.process_time() - cpu,
        "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        "results": results,
        "results_sha256": sha(json.dumps(results, sort_keys=True).encode()),
        "affinity": sorted(os.sched_getaffinity(0)), "nice": os.getpriority(os.PRIO_PROCESS, 0),
        "compute_environment": {key: os.environ.get(key) for key in
            ("POLARS_MAX_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS")},
        "code_sha256": sha(module_path.read_bytes()), "provider_calls": 0,
        "parse_invocations": parse_counts,
        "loaded_module_sha256": getattr(module, "_LOADED_MODULE_SHA256", None),
        "polars_threads": module.pl.thread_pool_size(),
    }
    atomic_write_json(args.output, receipt)
    print(json.dumps({key: value for key, value in receipt.items() if key != "results"}), flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--reference", default="HEAD")
    parser.add_argument("--mode", choices=("reference", "candidate"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    base = Path("artifacts/benchmarks").resolve()
    if not args.root.resolve().is_relative_to(base):
        parser.error("use a fresh private directory under artifacts/benchmarks")
    if args.mode:
        if args.output is None or not args.output.resolve().is_relative_to(args.root.resolve()):
            parser.error("worker receipt must stay within its private fixture")
        return worker(args)
    if not args.source or args.root.exists():
        parser.error("provide native bulk directory and a new fixture root")
    args.root.mkdir(mode=0o700, parents=True)
    if shutil.disk_usage(args.root).free < 2 << 30:
        raise ValueError("less than 2 GiB safe engineering capacity")
    code_path = Path("downloader/download_tw_corporate_action_entitlements.py")
    reference = subprocess.check_output(["git", "show", f"{args.reference}:{code_path}"])
    candidate = code_path.read_bytes()
    validate_source_pair(reference, candidate)
    atomic_write_bytes(args.root / "reference.py", reference, mode=0o600)
    atomic_write_bytes(args.root / "candidate.py", candidate, mode=0o600)
    atomic_write_bytes(args.root / "benchmark.py", Path(__file__).read_bytes(), mode=0o600)
    paths = sorted(args.source.glob("*.html"))
    if not paths:
        raise ValueError("native bulk corpus is empty")
    files = []
    for path in paths:
        match = re.fullmatch(r"(twse|tpex)-(\d+)(?:-asof-\d{8})?-v\d+\.html", path.name)
        if not match or path.is_symlink() or not path.is_file():
            raise ValueError("unknown or nonregular bulk response: " + path.name)
        before = signature(path)
        content = path.read_bytes()
        if signature(path) != before or len(content) != before[2]:
            raise ValueError("source changed during private freeze")
        atomic_write_bytes(args.root / "source" / path.name, content, mode=0o600)
        files.append({"name": path.name, "sha256": sha(content), "bytes": len(content),
                      "source_signature": before, "market": match[1], "roc_year": int(match[2])})
    fixture = {"observed_at_utc": datetime.now(UTC).isoformat(), "files": files,
               "source": str(args.source), "reference_sha256": sha(reference),
               "candidate_sha256": sha(candidate), "total_bytes": sum(item["bytes"] for item in files)}
    atomic_write_json(args.root / "fixture.json", fixture)
    print(json.dumps({"files": len(files), "bytes": fixture["total_bytes"]}), flush=True)
    rounds = []
    # First build is explicitly NOT a warm-cache speed result.
    for label, mode in (("cold", "candidate"), ("a1", "reference"),
                        ("b1", "candidate"), ("b2", "candidate"), ("a2", "reference")):
        output = args.root / f"{label}.json"
        subprocess.run([sys.executable, "-m", "scripts.benchmark_corporate_action_bulk_cache",
                        "--root", str(args.root), "--mode", mode, "--output", str(output)],
                       check=True, timeout=1800)
        result = json.loads(output.read_bytes())
        if rounds and result["results"] != rounds[0]["results"]:
            raise ValueError("complete source-row/failure output differs from control")
        if label in {"b1", "b2"} and result["parse_invocations"]["successful"]:
            raise ValueError("fresh warm worker reparsed previously successful responses")
        rounds.append(result | {"label": label})
    if sorted(args.source.glob("*.html")) != paths:
        raise ValueError("native bulk corpus membership changed during benchmark")
    for item in files:
        path = args.source / item["name"]
        if signature(path) != item["source_signature"] or sha(path.read_bytes()) != item["sha256"]:
            raise ValueError("native source changed during benchmark")
    if sha(code_path.read_bytes()) != fixture["candidate_sha256"]:
        raise ValueError("candidate code changed during benchmark")
    atomic_write_json(args.root / "measurement.json", {
        "state": "accepted_complete_pure_parse_corpus_not_downloader_or_publication",
        "source_unchanged": True, "files": len(files), "bytes": fixture["total_bytes"],
        "all_rows_and_failures_equal": True, "rounds": rounds,
        "os_page_cache_dropped": False, "exclusive_host": False,
    })


if __name__ == "__main__":
    main()
