"""Full canonical curve benchmark on private, verified real source copies.

No broker/model/API calls, public publication, source mutation or cache cleanup.
Baseline differs only by disabling the optional physical column cache. The
append operation uses real captured tail bytes, never invented observations.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from stockagent.live import taifex_history_column_cache as cache
from stockagent.live import taifex_history_selection as selection
from stockagent.live import taifex_volatility_dashboard as dashboard


def sha(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def copy_prefix(source: Path, destination: Path, *, size: int | None = None) -> dict:
    """Fence a real complete prefix, permitting only unchanged-prefix appends."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor = cache._open_regular(source)
    try:
        generation = cache._generation(os.fstat(descriptor))
        length = generation[2] if size is None else size
        if not 0 <= length <= generation[2]:
            raise ValueError("invalid source prefix length")
        digest = hashlib.sha256()
        offset = 0
        with destination.open("xb") as output:
            os.chmod(destination, 0o600)
            while offset < length:
                block = os.pread(descriptor, min(1024**2, length-offset), offset)
                if not block:
                    raise ValueError("source prefix truncated")
                output.write(block)
                digest.update(block)
                offset += len(block)
        if cache._prefix_digest(descriptor, length) != digest.hexdigest():
            raise ValueError("source prefix changed during copy")
        if length and os.pread(descriptor, 1, length-1) != b"\n" and source.suffix == ".jsonl":
            raise ValueError("source copy must end at a complete JSON line")
        return {"source": str(source), "prefix_bytes": length, "sha256": digest.hexdigest(),
                "generation_at_capture": list(generation)}
    finally:
        os.close(descriptor)


def prepare(fixture: Path, source: Path) -> None:
    if fixture.exists():
        raise FileExistsError("fixture already exists; use a new engineering path")
    if shutil.disk_usage(ROOT).free < 26 * 1024**3 + (source / "marks.jsonl").stat().st_size:
        raise RuntimeError("insufficient safe free space for source fixture")
    fixture.mkdir(mode=0o700, parents=True)
    state = fixture / "state"
    state.mkdir(mode=0o700)
    proofs = [copy_prefix(source / name, state / name) for name in ("state.json", "status.json")]
    live = source / "marks.jsonl"
    descriptor = cache._open_regular(live)
    try:
        length = os.fstat(descriptor).st_size
        tail_size = min(length, 256 * 1024)
        tail = os.pread(descriptor, tail_size, length-tail_size)
        boundary = tail.find(b"\n") + 1 if length > tail_size else 0
        captured_tail = tail[boundary:]
        if not captured_tail or not captured_tail.endswith(b"\n"):
            raise ValueError("real append tail is not complete")
        base_size = length - len(captured_tail)
    finally:
        os.close(descriptor)
    proofs.append(copy_prefix(live, state / "marks.jsonl", size=base_size))
    atomic_write_bytes(fixture / "captured-tail.jsonl", captured_tail, mode=0o600)
    # Verify those captured append bytes are still the actual source suffix.
    with live.open("rb") as stream:
        stream.seek(base_size)
        if stream.read(len(captured_tail)) != captured_tail:
            raise RuntimeError("captured source tail changed")
    for receipt in dashboard._history_backfill_receipts(source):
        name = receipt["path"].parent.name
        proofs.append(copy_prefix(receipt["mark_path"], state / "backfills" / name / "marks.jsonl"))
        proofs.append(copy_prefix(receipt["path"], state / "backfills" / name / "receipt.json"))
    atomic_write_json(fixture / "fixture.json", {"contract": "taifex_history_source_fixture_v1",
        "source": str(source), "proofs": proofs, "base_bytes": base_size,
        "captured_tail_bytes": len(captured_tail), "captured_tail_sha256": sha(fixture / "captured-tail.jsonl"),
        "clock": datetime.now(timezone.utc).isoformat(), "provider_calls": 0,
        "source_mutations": False, "cross_file_atomic_snapshot_claim": False})


def worker(fixture: Path, variant: str, range_key: str) -> dict:
    meta = json.loads((fixture / "fixture.json").read_bytes())
    os.environ[cache.CACHE_ENV] = str(fixture / "columns")
    os.environ[cache.ENABLE_ENV] = "1" if variant == "candidate" else "0"
    (fixture / "columns").mkdir(mode=0o700, exist_ok=True)
    began, cpu = time.perf_counter(), time.process_time()
    result = dashboard.build_dashboard_history_snapshot(state_dir=fixture / "state",
        now=datetime.fromisoformat(meta["clock"]), range_key=range_key)
    wall, cpu_seconds = time.perf_counter()-began, time.process_time()-cpu
    digest = hashlib.sha256(json.dumps(result, sort_keys=True, allow_nan=False,
        separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    return {"variant": variant, "range": range_key, "wall_seconds": wall,
        "cpu_seconds": cpu_seconds, "rss_peak_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        "output_sha256": digest, "history_rows": len(result["history"]),
        "anchor": result["anchor_at_utc"], "provider_calls": 0, "production_writes": False}


def launch(fixture: Path, variant: str, range_key: str) -> dict:
    result = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--worker", variant,
        "--fixture", str(fixture), "--range", range_key], capture_output=True, text=True, check=True)
    return json.loads(result.stdout.splitlines()[-1])


def benchmark(fixture: Path, output: Path, repeats: int) -> dict:
    if output.exists() or (fixture / "benchmark-started.json").exists():
        raise FileExistsError("benchmark/fixture already consumed; never overwrite prior evidence")
    code_paths = [Path(__file__), Path(cache.__file__), Path(dashboard.__file__), Path(selection.__file__)]
    before = {str(path): sha(path) for path in code_paths}
    meta = json.loads((fixture / "fixture.json").read_bytes())
    for proof in meta["proofs"]:
        path = fixture / "state" / Path(proof["source"]).relative_to(meta["source"])
        if sha(path) != proof["sha256"]:
            raise RuntimeError("private source fixture differs from captured real prefix")
    tail = (fixture / "captured-tail.jsonl").read_bytes()
    if hashlib.sha256(tail).hexdigest() != meta["captured_tail_sha256"]:
        raise RuntimeError("captured real append bytes changed")
    atomic_write_json(fixture / "benchmark-started.json", {"at_utc": datetime.now(timezone.utc).isoformat()})
    setup = launch(fixture, "candidate", "1d")
    # One append of real captured bytes to this exclusively owned disposable
    # source copy. It is not an append/replay to the real paper ledger.
    with (fixture / "state/marks.jsonl").open("ab") as stream:
        stream.write(tail)
    after_append = launch(fixture, "candidate", "1d")
    rows = []
    for _ in range(repeats):
        for variant in ("baseline", "candidate", "candidate", "baseline"):
            row = launch(fixture, variant, "1d")
            rows.append(row)
            print(json.dumps(row), flush=True)
    expected = rows[0]["output_sha256"]
    if any(row["output_sha256"] != expected for row in [after_append, *rows]):
        raise RuntimeError("complete canonical curve differs from raw control")
    medians = {}
    for variant in ("baseline", "candidate"):
        selected = [row for row in rows if row["variant"] == variant]
        medians[variant] = {key: statistics.median(row[key] for row in selected)
                           for key in ("wall_seconds", "cpu_seconds", "rss_peak_bytes")}
    after = {str(path): sha(path) for path in code_paths}
    if before != after:
        raise RuntimeError("benchmark code changed during measurement")
    result = {"contract": "taifex_history_complete_curve_comparison_v1", "state": "measurement_complete",
        "scope": "full_1d_curve_not_status_broker_or_whole_host", "medians": medians,
        "trials": rows, "cold_column_setup": setup, "verified_real_append_update": after_append,
        "append_tail_bytes": len(tail), "exact_output_parity": True, "code_stable": True,
        "source_fixture": str(fixture), "source_bytes": sum(row["prefix_bytes"] for row in meta["proofs"]),
        "cache_parquet_bytes": sum(path.stat().st_size for path in (fixture / "columns").rglob("*.parquet")),
        "cache_receipts": len(list((fixture / "columns").rglob("*.json"))),
        "provider_calls": 0, "production_writes": False, "code_sha256": before,
        "wall_improvement_observed": medians["candidate"]["wall_seconds"] < medians["baseline"]["wall_seconds"]}
    atomic_write_json(output, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--source", type=Path, default=ROOT / "artifacts/live/shioaji_taifex_volatility_simulation")
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--repeats", type=int, default=2, choices=range(1, 4))
    parser.add_argument("--worker", choices=("baseline", "candidate"))
    parser.add_argument("--range", default="1d", choices=tuple(dashboard.HISTORY_RANGE_SECONDS))
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(worker(args.fixture, args.worker, args.range)))
    elif args.prepare_only:
        prepare(args.fixture, args.source.resolve(strict=True))
        print(json.dumps({"fixture": str(args.fixture), "provider_calls": 0}))
    elif args.output is not None:
        benchmark(args.fixture, args.output, args.repeats)
    else:
        parser.error("--prepare-only or --output is required")


if __name__ == "__main__":
    main()
