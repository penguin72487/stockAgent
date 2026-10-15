"""Default TW index/chart workflow and idle RSS on unchanged real sources.

Fresh ABBA workers keep identical source data, observation anchor and derived
indexes. Baseline executes the previous full-row prewarm fallback; candidate
warms the accepted date catalog. Both still build and encode complete all-range
and selected-date minute curves, then perform an exact warm read. No HTTP,
provider, broker, model, live-cache flushing or service restart occurs here.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import gc
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from scripts.profile_public_gateway_residency import memory, owner_environment, signature
from stockagent.live import tw_day_trade_dashboard as dashboard
from stockagent.live import benchmark_history_projection as projection


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def source_inventory(root):
    paths = [root / name for name in ("signals.jsonl", "orders.jsonl", "fills.jsonl",
        "marks.jsonl", "benchmark_marks.jsonl", "benchmark_history.json", "events.jsonl",
        "latency.jsonl", "opening_signal_latency.jsonl", "overnight_history.json")]
    paths += [root / "benchmark_history_projection/v1/head.json"]
    for directory in ("position_history", "overnight_position_history"):
        paths.extend(sorted((root / directory).glob("*/*.json")))
    paths.extend(sorted((root / "benchmark_history_projection/v1/shards").glob("*/*.json.gz")))
    observations = signature(paths)
    # This history consumer reads exactly these two state fields (see the
    # complete bound dashboard module), not live positions or heartbeat clocks.
    # Keep the full state-file metadata separately; never relax live readers.
    try:
        state = dashboard._object(root / "state.json", use_cache=False)
    except (OSError, ValueError):
        state = {}
    observations["history_state_consumed_fields"] = {
        "product": state.get("product"), "enabled_markets": state.get("enabled_markets")}
    return observations


def encoded(value):
    body = json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode()
    return hashlib.sha256(body).hexdigest(), len(body)


def worker(root, cache, variant):
    os.environ[dashboard._LEDGER_SESSION_INDEX_CACHE_ENV] = str(cache)
    head = projection.load_benchmark_projection(state_dir=root,
        source_path=root / "benchmark_history.json", selected_sessions=())
    if head is None or not head.session_entries:
        raise ValueError("this measurement requires the current accepted real date catalog")
    dates = sorted(head.session_entries)
    samples = tuple(dict.fromkeys((dates[0], dates[len(dates)//2], dates[-1])))
    began, consumed = time.perf_counter(), time.process_time()
    load = dashboard.load_benchmark_projection
    if variant == "baseline":
        # Exactly the previous full prewarm: the full reader's own call has
        # selected_sessions=None and still validates every actual shard.
        def no_catalog(**options):
            return None if options.get("selected_sessions") == () else load(**options)
        with patch.object(dashboard, "load_benchmark_projection", no_catalog):
            indexes = dashboard.warm_dashboard_session_indexes(state_dir=root)
    else:
        indexes = dashboard.warm_dashboard_session_indexes(state_dir=root)
    after_index = memory()
    hashes, outputs = {"indexes": encoded(indexes)[0]}, []
    options = dict(state_dir=root, range_key="all", resolution="1m", history_encoding="minute_columns_v2",
                   use_memory_cache=False)  # The actual serialized-response gateway setting.
    for key, date in [("all", None), *((day, day) for day in samples)]:
        payload = dashboard.build_dashboard_history_snapshot(**options,
            start_date=date, end_date=date)
        digest, size = encoded(payload)
        hashes[key] = digest
        outputs.append({"request": key, "bytes": size,
                        "returned_points": payload.get("returned_points"),
                        "series_count": len(payload.get("minute_series", []))})
        del payload
    gc.collect()  # Same collection and its cost for both variants.
    work_wall, work_cpu = time.perf_counter()-began, time.process_time()-consumed
    after_work = memory()
    idle, idle_cpu = time.perf_counter(), time.process_time()
    time.sleep(3)
    after_idle = memory()
    idle_cpu = time.process_time()-idle_cpu
    started, consumed = time.perf_counter(), time.process_time()
    warm = dashboard.build_dashboard_history_snapshot(**options)
    digest, _ = encoded(warm)
    warm_wall, warm_cpu = time.perf_counter()-started, time.process_time()-consumed
    if digest != hashes["all"]:
        raise ValueError("warm read changed any complete curve output")
    return {"variant": variant, "wall_seconds": work_wall+warm_wall,
            "cpu_seconds": work_cpu+warm_cpu+idle_cpu, "output_sha256": hashes,
            "index_counts": indexes, "outputs": outputs, "after_index": after_index,
            "after_work": after_work, "after_idle": after_idle,
            "idle_seconds": time.perf_counter()-idle-warm_wall,
            "idle_cpu_seconds": idle_cpu, "warm_wall_seconds": warm_wall,
            "warm_cpu_seconds": warm_cpu,
            "rss_peak_bytes": max(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
                                  after_index["rss_bytes"], after_work["rss_bytes"], after_idle["rss_bytes"]),
            "idle_rss_bytes": after_idle["rss_bytes"], "warm_exact_output_parity": True}


def benchmark(root, output, *, repeats=2):
    if output.exists():
        raise FileExistsError("never overwrite prior evidence")
    owner, role = owner_environment()
    original_cache = Path(role.get(dashboard._LEDGER_SESSION_INDEX_CACHE_ENV, ""))
    if not original_cache.is_dir():
        raise ValueError("formal derived index cache unavailable")
    if shutil.disk_usage(output.parent).free < 30*1024**3:
        raise ValueError("keep the user's 30 GiB safe free-space boundary")
    paths = [Path(__file__), Path(dashboard.__file__), Path(projection.__file__),
             ROOT / "scripts/profile_public_gateway_residency.py"]
    code = {str(path): sha(path) for path in paths}
    before = source_inventory(root)
    state_metadata_before = signature([root / "state.json"])
    source_proof = {str(root / "benchmark_history.json"): sha(root / "benchmark_history.json"),
                    str(projection.projection_head_path(root)): sha(projection.projection_head_path(root))}
    input_receipt = output.with_name(output.stem + "-inputs.json")
    if input_receipt.exists():
        raise FileExistsError("never overwrite prior input-fence evidence")
    atomic_write_json(input_receipt, {"code_sha256": code, "source_signatures": before,
                                    "source_sha256": source_proof,
                                    "full_state_file_metadata": state_metadata_before})
    trials = []
    with tempfile.TemporaryDirectory(prefix="tw-catalog-prewarm-") as directory:
        temp = Path(directory)
        template = temp / "accepted-indexes"
        # Freeze derived indexes, not source data. Each worker's writes are
        # confined to its private cache and cannot replace the template/live cache.
        shutil.copytree(original_cache, template, symlinks=True)
        for index, variant in enumerate(("baseline", "catalog", "catalog", "baseline")*repeats):
            cache = temp / f"worker-{index}"
            shutil.copytree(template, cache, copy_function=os.symlink, symlinks=True)
            env = {**os.environ, **role}
            command = [sys.executable, str(Path(__file__).resolve()), "--worker", variant,
                "--root", str(root), "--cache", str(cache)]
            run = subprocess.run(command, check=True, capture_output=True, text=True,
                                 env=env, timeout=120)
            row = json.loads(run.stdout.splitlines()[-1])
            if trials and row["output_sha256"] != trials[0]["output_sha256"]:
                raise ValueError("complete indexed/default/selected curve outputs differ")
            trials.append(row)
            print(json.dumps({key: row[key] for key in (
                "variant", "wall_seconds", "cpu_seconds", "idle_rss_bytes", "rss_peak_bytes")}), flush=True)
    after, final_code = source_inventory(root), {str(path): sha(path) for path in paths}
    state_metadata_after = signature([root / "state.json"])
    if before != after or code != final_code:
        atomic_write_json(output, {"contract": "tw_complete_default_history_catalog_prewarm_idle_v1",
            "state": "rejected_input_or_code_change", "trials": trials,
            "source_unchanged": before == after, "code_stable": code == final_code,
            "changed_sources": [path for path in sorted(before.keys() | after.keys())
                                if before.get(path) != after.get(path)],
            "changed_code": [path for path in code if code[path] != final_code[path]],
            "input_receipt": str(input_receipt), "source_signatures_after": after,
            "code_sha256_after": final_code, "source_writes": False})
        raise ValueError("source or current code changed during balanced workflow measurement")
    if source_proof != {path: sha(path) for path in source_proof}:
        raise ValueError("canonical history/head content changed")
    metrics = ("wall_seconds", "cpu_seconds", "idle_rss_bytes", "rss_peak_bytes", "warm_wall_seconds")
    medians = {variant: {key: statistics.median(row[key] for row in trials if row["variant"] == variant)
                         for key in metrics} for variant in ("baseline", "catalog")}
    result = {"contract": "tw_complete_default_history_catalog_prewarm_idle_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(), "source_root": str(root),
        "window_anchor": "newest retained observation, unchanged canonical history contract",
        "formal_owner_environment": role, "formal_owner": owner,
        "code_sha256": code, "source_sha256": source_proof, "source_signatures": before,
        "trial_order": [row["variant"] for row in trials], "balanced_abba": True,
        "trials": trials, "medians": medians, "exact_output_parity": True,
        "consumed_inputs_unchanged": True, "code_stable": True, "source_writes": False,
        "state_metadata_before": state_metadata_before, "state_metadata_after": state_metadata_after,
        "full_state_metadata_unchanged": state_metadata_before == state_metadata_after,
        "input_scope": "all indexed/chart sources plus exact history product/enabled_markets; full live state metadata recorded separately",
        "live_cache_writes": False, "provider_calls": 0, "gpu_calls": 0,
        "boundary": "complete index prewarm/all/three selected minute curves plus exact warm all; excludes gateway status/upstreams/HTTP/other range patterns; no OS cache flush"}
    atomic_write_json(output, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT / "artifacts/live/tw_day_trade_simulation")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--worker", choices=("baseline", "catalog"))
    parser.add_argument("--repeats", type=int, choices=(1, 2), default=2)
    parser.add_argument("--cache", type=Path)
    args = parser.parse_args()
    if args.worker and args.cache:
        print(json.dumps(worker(args.root, args.cache, args.worker)))
    elif args.output:
        print(json.dumps(benchmark(args.root, args.output, repeats=args.repeats)["medians"]))
    else:
        parser.error("--output or complete worker arguments are required")


if __name__ == "__main__":
    main()
