"""Full real-source curve/update comparison against preserved original code.

Reuse the canonical source-fixture capture and original curve builders. Only
the column-cache module differs, in isolated fresh workers. Real captured tail
bytes are appended once to the exclusively owned fixture, never a live ledger.
All seven complete outputs, durable JSON readback, and same-process warm reads
are measured; proof-byte counters are diagnostics, not whole-host throughput.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import resource
import re
import shutil
import statistics
import subprocess
import sys
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import polars as pl
from downloader.artifact_io import atomic_write_json, atomic_write_text
from scripts import benchmark_taifex_history_columns as columns
from scripts import benchmark_taifex_history_selection as workflow

CONTRACT = "taifex_complete_curve_source_proof_memo_comparison_v1"
TRIAL_ORDER = ["baseline", "candidate", "candidate", "baseline"] * 2


def original_module(root: Path):
    proof = json.loads((root / "original-code-proof.json").read_bytes())
    path = root / "original_column_cache.py"
    if columns.sha(path) != proof["sha256"]:
        raise ValueError("preserved original implementation changed")
    spec = importlib.util.spec_from_file_location("taifex_preserved_column_cache", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def worker(
    root: Path, variant: str, trial: int, phase: str, measurement: str = "comparison"
) -> dict:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", measurement) is None:
        raise ValueError("invalid private measurement identifier")
    fixture = root / "fixture"
    active = original_module(root) if variant == "baseline" else workflow.cache
    cache_root = root / measurement / f"trial-{trial:02d}-{variant}"
    cache_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.environ[active.CACHE_ENV] = str(cache_root)
    os.environ[active.ENABLE_ENV] = "1"
    meta = json.loads((fixture / "fixture.json").read_bytes())
    now = datetime.fromisoformat(meta["clock"])
    prefix, source_digests = active._prefix_digest, active._source_digests
    proofs = []

    def record_prefix(fd, size):
        result = prefix(fd, size)
        proofs.append(
            {
                "path": os.readlink(f"/proc/self/fd/{fd}"),
                "kind": "verify",
                "bytes": size,
            }
        )
        return result

    def record_source(fd, size, previous):
        result = source_digests(fd, size, previous)
        proofs.append(
            {"path": os.readlink(f"/proc/self/fd/{fd}"), "kind": "build", "bytes": size}
        )
        return result

    ranges = (
        ["1d"] if phase == "setup" else list(workflow.dashboard.HISTORY_RANGE_SECONDS)
    )
    began, cpu = time.perf_counter(), time.process_time()
    with ExitStack() as stack:
        stack.enter_context(
            patch.object(workflow.cache, "history_scan", active.history_scan)
        )
        stack.enter_context(patch.object(active, "_prefix_digest", record_prefix))
        stack.enter_context(patch.object(active, "_source_digests", record_source))
        outputs, timings = {}, []
        for key in ranges:
            start, started_cpu = time.perf_counter(), time.process_time()
            curve = workflow.dashboard.build_dashboard_history_snapshot(
                state_dir=fixture / "state", now=now, range_key=key
            )
            destination = cache_root / f"{phase}-{key}.json"
            atomic_write_text(
                destination,
                json.dumps(curve, ensure_ascii=False, allow_nan=False) + "\n",
                durable=True,
                mode=0o600,
            )
            readback = json.loads(destination.read_bytes())
            if readback != curve:
                raise ValueError("complete curve changed in durable readback")
            outputs[key] = hashlib.sha256(
                json.dumps(
                    readback,
                    sort_keys=True,
                    allow_nan=False,
                    separators=(",", ":"),
                    ensure_ascii=False,
                ).encode()
            ).hexdigest()
            timings.append(
                {
                    "range": key,
                    "wall_seconds": time.perf_counter() - start,
                    "cpu_seconds": time.process_time() - started_cpu,
                    "rows": len(curve["history"]),
                    "json_bytes": destination.stat().st_size,
                }
            )
        complete_wall, complete_cpu = (
            time.perf_counter() - began,
            time.process_time() - cpu,
        )
        proof_count = len(proofs)
        warm_began, warm_cpu = time.perf_counter(), time.process_time()
        if phase != "setup":
            # Same native process: repeated unchanged-source curve requests.
            for key in ranges:
                curve = workflow.dashboard.build_dashboard_history_snapshot(
                    state_dir=fixture / "state", now=now, range_key=key
                )
                digest = hashlib.sha256(
                    json.dumps(
                        curve,
                        sort_keys=True,
                        allow_nan=False,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ).encode()
                ).hexdigest()
                if digest != outputs[key]:
                    raise ValueError("same-process warm curve differs")
        warm_wall, warm_cpu = (
            time.perf_counter() - warm_began,
            time.process_time() - warm_cpu,
        )
    return {
        "variant": variant,
        "trial": trial,
        "phase": phase,
        "ranges": timings,
        "wall_seconds": complete_wall,
        "cpu_seconds": complete_cpu,
        "output_sha256": outputs,
        "warm_wall_seconds": warm_wall,
        "warm_cpu_seconds": warm_cpu,
        "warm_proof_bytes": sum(row["bytes"] for row in proofs[proof_count:]),
        "proof_bytes": sum(row["bytes"] for row in proofs[:proof_count]),
        "proofs": proofs,
        "rss_peak_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        "threads": pl.thread_pool_size(),
        "provider_calls": 0,
        "model_forward_calls": 0,
        "production_writes": False,
        "durable_complete_readback": True,
        "warm_exact_output_parity": True,
    }


def launch(root: Path, variant: str, trial: int, phase: str, measurement: str) -> dict:
    run = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--root",
            str(root),
            "--worker",
            variant,
            "--trial",
            str(trial),
            "--phase",
            phase,
            "--measurement",
            measurement,
        ],
        capture_output=True,
        text=True,
    )
    if run.returncode:
        raise RuntimeError(
            f"isolated worker failed ({run.returncode}): {run.stderr[-4000:]}"
        )
    return json.loads(run.stdout.splitlines()[-1])


def benchmark(root: Path, output: Path) -> dict:
    root = root.resolve(strict=True)
    fixture = root / "fixture"
    measurement = output.stem
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", measurement) is None:
        raise ValueError("invalid private measurement identifier")
    trial_root = root / measurement
    if output.exists() or trial_root.exists():
        raise FileExistsError("use a fresh private fixture; preserve prior evidence")
    if shutil.disk_usage(root).free < 30 * 1024**3:
        raise RuntimeError("insufficient safe space for private cache trials")
    original_module(root)
    paths = [
        Path(__file__),
        Path(workflow.cache.__file__),
        Path(workflow.selection.__file__),
        Path(workflow.dashboard.__file__),
        Path(workflow.__file__),
        Path(columns.__file__),
        ROOT / "downloader/artifact_io.py",
        root / "original_column_cache.py",
        Path(pl.__file__),
    ]
    code = {str(path.resolve()): columns.sha(path) for path in paths}
    meta = json.loads((fixture / "fixture.json").read_bytes())
    # Setup uses the original complete prefix; verify before any private append.
    for proof in meta["proofs"]:
        path = fixture / "state" / Path(proof["source"]).relative_to(meta["source"])
        if columns.sha(path) != proof["sha256"]:
            raise ValueError("private source prefix changed before setup")
    tail = (fixture / "captured-tail.jsonl").read_bytes()
    if (
        len(tail) != meta["captured_tail_bytes"]
        or hashlib.sha256(tail).hexdigest() != meta["captured_tail_sha256"]
    ):
        raise ValueError("real captured append bytes changed")
    trial_root.mkdir(mode=0o700)
    atomic_write_json(
        trial_root / "benchmark-started.json",
        {"at_utc": datetime.now(timezone.utc).isoformat()},
    )
    setup = []
    for trial, variant in enumerate(TRIAL_ORDER):
        row = launch(root, variant, trial, "setup", measurement)
        atomic_write_json(trial_root / f"setup-{trial:02d}.json", row)
        setup.append(row)
        print(
            json.dumps(
                {
                    "stage": "base_setup",
                    "trial": trial,
                    "variant": variant,
                    "wall_seconds": row["wall_seconds"],
                }
            ),
            flush=True,
        )
    if any(row["output_sha256"] != setup[0]["output_sha256"] for row in setup):
        raise ValueError("base source complete curve differs before append")
    with (fixture / "state/marks.jsonl").open("ab") as stream:
        stream.write(tail)
        stream.flush()
        os.fsync(stream.fileno())
    frozen = workflow.fixture_proof(fixture)
    # Independent original raw parser, entire 1d curve on the exact same source.
    raw = columns.launch(fixture, "baseline", "1d")
    atomic_write_json(trial_root / "independent-raw-1d.json", raw)
    trials = []
    for trial, variant in enumerate(TRIAL_ORDER):
        row = launch(root, variant, trial, "update", measurement)
        atomic_write_json(trial_root / f"update-{trial:02d}.json", row)
        trials.append(row)
        print(
            json.dumps(
                {
                    "stage": "real_update",
                    "trial": trial,
                    "variant": variant,
                    "wall_seconds": row["wall_seconds"],
                    "cpu_seconds": row["cpu_seconds"],
                    "proof_bytes": row["proof_bytes"],
                }
            ),
            flush=True,
        )
    expected = trials[0]["output_sha256"]
    if (
        set(expected) != set(workflow.dashboard.HISTORY_RANGE_SECONDS)
        or any(row["output_sha256"] != expected for row in trials)
        or expected["1d"] != raw["output_sha256"]
    ):
        raise ValueError("complete seven-range/independent raw outputs differ")
    if frozen != workflow.fixture_proof(fixture) or code != {
        str(path.resolve()): columns.sha(path) for path in paths
    }:
        raise ValueError("source or code changed during measurement")
    metrics = (
        "wall_seconds",
        "cpu_seconds",
        "rss_peak_bytes",
        "proof_bytes",
        "warm_wall_seconds",
        "warm_cpu_seconds",
        "warm_proof_bytes",
    )
    medians = {
        variant: {
            metric: statistics.median(
                row[metric] for row in trials if row["variant"] == variant
            )
            for metric in metrics
        }
        for variant in ("baseline", "candidate")
    }
    result = {
        "contract": CONTRACT,
        "state": "measurement_complete",
        "fixture": str(fixture),
        "scope": "complete_seven_range_real_append_curve_durable_readback_and_warm_not_broker_or_whole_host",
        "trial_order": TRIAL_ORDER,
        "trials": trials,
        "base_setup": setup,
        "medians": medians,
        "independent_raw_1d": {
            "path": str(trial_root / "independent-raw-1d.json"),
            "sha256": columns.sha(trial_root / "independent-raw-1d.json"),
            "output_sha256": raw["output_sha256"],
        },
        "source_sha256": frozen,
        "code_sha256": code,
        "code_stable": True,
        "source_unchanged": True,
        "exact_output_parity": True,
        "balanced_abba": True,
        "provider_calls": 0,
        "production_writes": False,
        "model_forward_calls": 0,
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    atomic_write_json(output, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--worker", choices=("baseline", "candidate"))
    parser.add_argument("--trial", type=int, default=0)
    parser.add_argument("--phase", choices=("setup", "update"), default="update")
    parser.add_argument("--measurement", default="comparison")
    args = parser.parse_args()
    if args.worker:
        print(
            json.dumps(
                worker(
                    args.root.resolve(strict=True),
                    args.worker,
                    args.trial,
                    args.phase,
                    args.measurement,
                )
            )
        )
    elif args.output:
        print(
            json.dumps(
                {
                    "medians": benchmark(args.root, args.output)["medians"],
                    "output": str(args.output),
                }
            )
        )
    else:
        parser.error("--worker or --output required")


if __name__ == "__main__":
    main()
