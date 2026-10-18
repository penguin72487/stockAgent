"""Prewarm/read back the accepted physical TAIFEX display cache, never trade.

The only writes are private disposable column caches and an explicit evidence
file. Restart remains a named operator action; this verifier never restarts a
service, logs into a provider, imports CUDA or changes a source/ledger.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import resource
import statistics
import subprocess
import sys
import time

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from scripts.verify_taifex_web_recovery import fetch_json, validate_history_clock
from stockagent.live import taifex_history_column_cache as cache
from stockagent.live import taifex_history_segments as segments
from stockagent.live import taifex_history_selection as selection
from stockagent.live import taifex_volatility_dashboard as dashboard

UNIT = "stockagent-shioaji-taifex-dashboard.service"
PROTECTED = ("stockagent-shioaji-taifex-bidask.service", "stockagent-tw-day-trade-simulation.service")
SOURCE = ROOT / "artifacts/live/shioaji_taifex_volatility_simulation"
LAUNCHER = ROOT / "scripts/run_shioaji_taifex_dashboard.sh"
ALLOCATOR_CONTRACT = "taifex_complete_curve_allocator_idle_comparison_v1"
ALLOCATOR_CONF = "muzzy_decay_ms:1000"
SOURCE_PROOF_CONTRACT = "taifex_complete_curve_source_proof_memo_comparison_v1"
BASE_DELTA_CONTRACT = "taifex_complete_seven_curve_base_delta_comparison_v1"


def base_delta_acceptance(value, required, measurement):
    required.update((str(Path(segments.__file__).resolve()),
                     str(ROOT / "scripts/benchmark_taifex_history_segments.py"),
                     str(ROOT / "scripts/benchmark_taifex_history_selection.py")))
    trials = value["trials"]
    order = ["baseline", "candidate", "candidate", "baseline"] * 2
    control = value["independent_control"]
    independent = metadata(control["path"])
    expected = independent["trials"][0]["output_sha256"]
    seed = value["seed"]
    if len(trials) != len(order):
        return False, value["fixture"]
    metrics = ("wall_seconds", "cpu_seconds", "rss_peak_bytes", "parquet_written_bytes",
               "warm_wall_seconds", "warm_cpu_seconds")
    measured = {variant: {metric: statistics.median(row[metric] for row in trials
        if row.get("variant") == variant) for metric in metrics} for variant in ("baseline", "candidate")}
    totals = {variant: {metric: statistics.median(row[metric] + row["warm_" + metric]
        for row in trials if row.get("variant") == variant) for metric in ("wall_seconds", "cpu_seconds")}
        for variant in ("baseline", "candidate")}
    improvement = (
        value.get("source_unchanged") is True and value.get("balanced_abba") is True
        and value.get("model_forward_calls") == 0
        and sha(control["path"]) == control["sha256"]
        and independent.get("exact_output_parity") is True
        and Path(independent["fixture"]).resolve() == Path(value["fixture"]).resolve()
        and seed["fixture"] == value["fixture"] and seed["source_sha256"] == value["source_sha256"]
        and sha(Path(measurement).parent / "original_column_cache.py") == seed["original_code_sha256"]
        and measured == value["medians"] and set(expected) == set(dashboard.HISTORY_RANGE_SECONDS)
        and all(row.get("variant") == variant and row.get("threads") == 4
            and row.get("allocator_conf", "").endswith(ALLOCATOR_CONF) and row.get("output_sha256") == expected
            and row.get("warm_exact_output_parity") is True
            and row.get("warm_parquet_written_bytes") == 0 and row.get("warm_sink_calls") == 0
            and row.get("provider_calls") == 0 and row.get("model_forward_calls") == 0
            and row.get("production_writes") is False
            and len(row.get("ranges", [])) == len(expected)
            and {item["range"] for item in row["ranges"]} == set(expected)
            and all(item["rows"] > 0 and item["json_bytes"] > 0 for item in row["ranges"])
            for row, variant in zip(trials, order, strict=True))
        and measured["candidate"]["parquet_written_bytes"] < .05 * measured["baseline"]["parquet_written_bytes"]
        and measured["candidate"]["cpu_seconds"] <= 1.05 * measured["baseline"]["cpu_seconds"]
        and measured["candidate"]["wall_seconds"] <= 1.05 * measured["baseline"]["wall_seconds"]
        and all(totals["candidate"][metric] <= 1.05 * totals["baseline"][metric]
                for metric in ("wall_seconds", "cpu_seconds"))
    )
    return improvement, value["fixture"]


def source_proof_acceptance(value, required):
    """Require complete balanced trials and raw readback, not a summary claim."""
    fixture = Path(value["fixture"]).resolve()
    required.update(str(ROOT / f"scripts/{name}.py") for name in (
        "benchmark_taifex_source_proof_memo", "benchmark_taifex_history_selection",
        "benchmark_taifex_history_columns"))
    required.update((str(ROOT / "downloader/artifact_io.py"),
                     str(fixture.parent / "original_column_cache.py"),
                     str(Path(pl.__file__).resolve())))
    order = ["baseline", "candidate", "candidate", "baseline"] * 2
    trials = value.get("trials", [])
    if len(trials) != len(order) or [row.get("variant") for row in trials] != order:
        return False, str(fixture)
    metrics = ("wall_seconds", "cpu_seconds", "rss_peak_bytes", "proof_bytes",
               "warm_wall_seconds", "warm_cpu_seconds", "warm_proof_bytes")
    measured = {variant: {metric: statistics.median(row[metric] for row in trials
        if row.get("variant") == variant) for metric in metrics}
        for variant in ("baseline", "candidate")}
    # Include the subsequent same-process warm request sequence as well. A
    # cheaper prefix proof must not conceal an overall workflow regression.
    totals = {variant: {metric: statistics.median(row[metric] + row["warm_" + metric]
        for row in trials if row.get("variant") == variant)
        for metric in ("wall_seconds", "cpu_seconds")}
        for variant in ("baseline", "candidate")}
    expected = trials[0]["output_sha256"]
    raw = value["independent_raw_1d"]
    raw_readback = metadata(raw["path"])
    original = metadata(fixture.parent / "original-code-proof.json")
    source = fixture / "state/marks.jsonl"
    source_size = source.stat().st_size

    def complete_trial(row, variant):
        proofs = [proof for proof in row["proofs"] if proof["path"] == str(source)]
        return (row.get("phase") == "update" and row.get("variant") == variant
                and row.get("threads") == 4 and row.get("output_sha256") == expected
                and row.get("provider_calls") == 0 and row.get("model_forward_calls") == 0
                and row.get("production_writes") is False
                and row.get("durable_complete_readback") is True
                and row.get("warm_exact_output_parity") is True
                and row["warm_proof_bytes"] == 0
                and row["proof_bytes"] == sum(proof["bytes"] for proof in row["proofs"])
                and {item["range"] for item in row["ranges"]} == set(dashboard.HISTORY_RANGE_SECONDS)
                and len(row["ranges"]) == len(dashboard.HISTORY_RANGE_SECONDS)
                and all(item["rows"] > 0 and item["json_bytes"] > 0 for item in row["ranges"])
                and [(proof["kind"], proof["bytes"]) for proof in proofs]
                    == ([("build", source_size), ("verify", source_size)] if variant == "baseline"
                        else [("build", source_size)]))

    improvement = (
        value.get("source_unchanged") is True and value.get("balanced_abba") is True
        and value.get("trial_order") == order and value.get("model_forward_calls") == 0
        and measured == value.get("medians")
        and set(expected) == set(dashboard.HISTORY_RANGE_SECONDS)
        and sha(raw["path"]) == raw["sha256"]
        and raw_readback.get("variant") == "baseline" and raw_readback.get("range") == "1d"
        and raw_readback.get("provider_calls") == 0 and raw_readback.get("production_writes") is False
        and raw_readback["output_sha256"] == raw["output_sha256"] == expected["1d"]
        and original["sha256"] == sha(fixture.parent / "original_column_cache.py")
        and all(row.get("trial") == index and complete_trial(row, variant)
                for index, (row, variant) in enumerate(zip(trials, order, strict=True)))
        and measured["candidate"]["cpu_seconds"] < .95 * measured["baseline"]["cpu_seconds"]
        and measured["candidate"]["wall_seconds"] <= 1.05 * measured["baseline"]["wall_seconds"]
        and measured["candidate"]["proof_bytes"] < measured["baseline"]["proof_bytes"]
        and totals["candidate"]["wall_seconds"] <= 1.05 * totals["baseline"]["wall_seconds"]
        and totals["candidate"]["cpu_seconds"] <= totals["baseline"]["cpu_seconds"]
    )
    return improvement, str(fixture)


def metadata(path):
    with Path(path).open("rb") as stream:
        raw = stream.read(4 * 1024**2 + 1)
    if len(raw) > 4 * 1024**2:
        raise ValueError("oversized_metadata")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("invalid_metadata")
    return value


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def accepted_code(path):
    value = metadata(path)
    required = {str(Path(cache.__file__).resolve()), str(Path(dashboard.__file__).resolve()),
                str(Path(selection.__file__).resolve())}
    hashes = value.get("code_sha256", {})
    medians = value.get("medians", {})
    if value.get("contract") == "taifex_history_complete_curve_comparison_v1":
        required.add(str(ROOT / "scripts/benchmark_taifex_history_columns.py"))
        improvement = all(medians["candidate"][metric] < .8 * medians["baseline"][metric]
                          for metric in ("cpu_seconds", "rss_peak_bytes"))
        fixture_path = value["source_fixture"]
    elif value.get("contract") == "taifex_complete_seven_curve_thread_comparison_v1":
        required.update(str(ROOT / f"scripts/{name}.py") for name in (
            "benchmark_taifex_curve_threads", "benchmark_taifex_history_selection"))
        order = [16, 4, 4, 16] * 2
        control = value.get("independent_control", {})
        independent = metadata(control["path"])
        expected = independent["trials"][0]["output_sha256"]
        improvement = (
            value.get("source_unchanged") is True and value.get("balanced_abba") is True
            and value.get("trial_order") == order and value.get("model_forward_calls") == 0
            and sha(control["path"]) == control["sha256"]
            and independent.get("exact_output_parity") is True
            and Path(independent["fixture"]).resolve() == Path(value["fixture"]).resolve()
            and set(expected) == set(dashboard.HISTORY_RANGE_SECONDS)
            and len(value.get("trials", [])) == len(order)
            and all(row.get("threads") == count and row.get("variant") == "window"
                    and row.get("output_sha256") == expected
                    for row, count in zip(value["trials"], order, strict=True))
            and medians["4"]["cpu_seconds"] < .8 * medians["16"]["cpu_seconds"]
            and medians["4"]["wall_seconds"] <= 1.05 * medians["16"]["wall_seconds"]
            and medians["4"]["rss_peak_bytes"] < medians["16"]["rss_peak_bytes"]
        )
        fixture_path = value["fixture"]
    elif value.get("contract") == ALLOCATOR_CONTRACT:
        required.update(str(ROOT / f"scripts/{name}.py") for name in (
            "benchmark_taifex_allocator_idle", "benchmark_taifex_history_selection"))
        required.add(str(Path(pl.__file__).resolve()))
        order = ["baseline", "decay", "decay", "baseline"] * 2
        control = value.get("independent_control", {})
        independent = metadata(control["path"])
        expected = independent["trials"][0]["output_sha256"]
        trials = value.get("trials", [])
        if (len(trials) != len(order)
                or any(sum(row.get("allocator_variant") == variant for row in trials) != 4
                       for variant in ("baseline", "decay"))):
            raise ValueError("current_code_not_bound_to_accepted_measurement")
        # Recalculate the reported medians; a summary alone is not a complete
        # workflow, idle-reclamation or subsequent exact-read measurement.
        measured = {variant: {metric: statistics.median(row[metric] for row in trials
                    if row.get("allocator_variant") == variant)
                    for metric in ("wall_seconds", "cpu_seconds", "rss_peak_bytes", "idle_rss_bytes",
                                   "idle_cpu_seconds", "warm_read_wall_seconds", "warm_read_cpu_seconds")}
                    for variant in ("baseline", "decay")}
        improvement = (
            value.get("source_unchanged") is True and value.get("balanced_abba") is True
            and value.get("trial_order") == order and value.get("model_forward_calls") == 0
            and value.get("decay_conf") == ALLOCATOR_CONF
            and sha(control["path"]) == control["sha256"]
            and independent.get("exact_output_parity") is True
            and Path(independent["fixture"]).resolve() == Path(value["fixture"]).resolve()
            and set(expected) == set(dashboard.HISTORY_RANGE_SECONDS)
            and len(trials) == len(order) and measured == medians
            and all(row.get("threads") == 4 and row.get("variant") == "window"
                    and row.get("allocator_variant") == variant
                    and row.get("output_sha256") == expected
                    and row.get("warm_read_exact_output_parity") is True
                    and (variant != "decay" or row.get("allocator_conf", "").endswith(ALLOCATOR_CONF))
                    and len(row.get("idle_samples", [])) == 4
                    and row["idle_samples"][-1]["elapsed_seconds"] >= 10
                    and row["idle_samples"][-1]["rss_bytes"] == row["idle_rss_bytes"]
                    for row, variant in zip(trials, order, strict=True))
            and medians["decay"]["idle_rss_bytes"] < .5 * medians["baseline"]["idle_rss_bytes"]
            and medians["decay"]["wall_seconds"] <= 1.05 * medians["baseline"]["wall_seconds"]
            and medians["decay"]["cpu_seconds"] <= 1.05 * medians["baseline"]["cpu_seconds"]
            and medians["decay"]["idle_cpu_seconds"] < .2
            and medians["decay"]["warm_read_wall_seconds"] <= medians["baseline"]["warm_read_wall_seconds"] + .1
        )
        fixture_path = value["fixture"]
    elif value.get("contract") == SOURCE_PROOF_CONTRACT:
        improvement, fixture_path = source_proof_acceptance(value, required)
    elif value.get("contract") == BASE_DELTA_CONTRACT:
        try:
            improvement, fixture_path = base_delta_acceptance(value, required, path)
        except (KeyError, TypeError, IndexError, ValueError) as error:
            raise ValueError("current_code_not_bound_to_accepted_measurement") from error
    else:
        raise ValueError("current_code_not_bound_to_accepted_measurement")
    if (not value.get("exact_output_parity") or not value.get("code_stable") or not improvement
            or value.get("production_writes") is not False or value.get("provider_calls") != 0
            or set(hashes) != required or any(sha(name) != digest for name, digest in hashes.items())):
        raise ValueError("current_code_not_bound_to_accepted_measurement")
    fixture = metadata(Path(fixture_path) / "fixture.json")
    if Path(fixture["source"]).resolve() != SOURCE.resolve():
        raise ValueError("measurement_source_does_not_match_owner")
    return hashes


def identity(unit):
    result = subprocess.run(["systemctl", "show", unit, "-p", "ActiveState", "-p", "InvocationID", "-p", "MainPID"],
                            check=True, text=True, capture_output=True, timeout=5)
    return dict(row.split("=", 1) for row in result.stdout.splitlines() if "=" in row)


def owner_start_wall_ns(owner):
    """Bind freshness to the actual new systemd process, not an older writer."""
    run = subprocess.run(["systemctl", "show", UNIT, "-p", "MainPID", "-p", "InvocationID",
                          "-p", "ExecMainStartTimestampMonotonic"],
                         check=True, text=True, capture_output=True, timeout=5)
    fields = dict(row.split("=", 1) for row in run.stdout.splitlines() if "=" in row)
    if any(fields.get(key) != owner[key] for key in ("MainPID", "InvocationID")):
        raise ValueError("owner_changed_during_start_time_readback")
    start = int(fields["ExecMainStartTimestampMonotonic"]) * 1000
    monotonic = time.monotonic_ns()
    if not 0 < start <= monotonic:
        raise ValueError("invalid_display_owner_start_time")
    return time.time_ns() - monotonic + start


def owner_environment(owner):
    pid = int(owner.get("MainPID", "0"))
    if owner.get("ActiveState") != "active" or pid <= 0 or not owner.get("InvocationID"):
        raise ValueError("display_owner_not_running")
    args = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
    if (b"scripts/serve_shioaji_taifex_simulation_dashboard.py" not in args
            or Path(os.fsdecode(args[args.index(b"--state-dir") + 1])).resolve() != SOURCE.resolve()):
        raise ValueError("unexpected_display_owner")
    # Never include the complete environment in an output or error message.
    values = dict(row.split(b"=", 1) for row in Path(f"/proc/{pid}/environ").read_bytes().split(b"\0") if b"=" in row)
    root = Path(os.fsdecode(values[cache.CACHE_ENV.encode()])).resolve(strict=True)
    if not root.is_relative_to((ROOT / "artifacts/cache").resolve()):
        raise ValueError("unexpected_display_cache_root")
    enabled = values.get(cache.ENABLE_ENV.encode(), b"0") in {b"1", b"true", b"yes"}
    return root, enabled


def owner_polars_limit(owner):
    """Read only the named setting, never return a process's full environment."""
    values = Path(f"/proc/{int(owner['MainPID'])}/environ").read_bytes().split(b"\0")
    selected = [row.removeprefix(b"POLARS_MAX_THREADS=") for row in values
                if row.startswith(b"POLARS_MAX_THREADS=")]
    return int(selected[0]) if len(selected) == 1 and selected[0].isdigit() else None


def owner_allocator_override(owner):
    """Only return the role's named allocator setting, never provider secrets."""
    values = Path(f"/proc/{int(owner['MainPID'])}/environ").read_bytes().split(b"\0")
    selected = [row.removeprefix(b"_RJEM_MALLOC_CONF=") for row in values
                if row.startswith(b"_RJEM_MALLOC_CONF=")]
    return os.fsdecode(selected[0]) if len(selected) == 1 else None


def cache_readback(root):
    paths = [SOURCE / "marks.jsonl"] + [row["mark_path"] for row in dashboard._history_backfill_receipts(SOURCE)]
    rows = []
    for source in paths:
        if source.stat().st_size < cache.MIN_SOURCE_BYTES:
            continue
        key = hashlib.sha256(str(source.resolve()).encode()).hexdigest()
        receipt_path = root / "history-columns-v1" / f"{key}.json"
        value = metadata(receipt_path)
        payload = receipt_path.with_suffix(".parquet")
        if (value.get("contract") != cache.CONTRACT or value.get("rows", 0) <= 0
                or value.get("source") != str(source.resolve()) or sha(payload) != value["payload_sha256"]):
            raise ValueError("unverified_physical_column_payload")
        rows.append({"path": str(receipt_path), "rows": value["rows"], "payload_bytes": payload.stat().st_size,
                     "source_generation": value["source_generation"], "receipt_mtime_ns": receipt_path.stat().st_mtime_ns})
    if not rows:
        raise ValueError("no_verified_column_cache")
    return rows


def segmented_cache_readback(root):
    """Independent full source/payload proof, not a native-writer claim."""
    import fcntl

    paths = [SOURCE / "marks.jsonl"] + [row["mark_path"] for row in dashboard._history_backfill_receipts(SOURCE)]
    rows = []
    for source in paths:
        source = source.resolve(strict=True)
        if source.stat().st_size < cache.MIN_SOURCE_BYTES:
            continue
        key = hashlib.sha256(str(source).encode()).hexdigest()
        legacy = root / "history-columns-v1"
        receipt = legacy / f"{key}.segments-v2/receipt.json"
        lock_fd = cache._open_regular(legacy / f"{key}.lock")
        source_fd = None
        parts = []
        try:
            deadline = time.monotonic() + 15
            while True:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise ValueError("column_query_lease_still_busy")
                    time.sleep(.02)
            value = segments._load_manifest(receipt, source, dashboard._HISTORY_POLARS_SCHEMA)
            if not value or value["rows"] <= 0:
                raise ValueError("unverified_segment_manifest")
            source_fd = cache._open_regular(source)
            generation = cache._generation(os.fstat(source_fd))
            if (list(generation) != value["source_generation"]
                    or cache._prefix_digest(source_fd, generation[2]) != value["source_sha256"]):
                raise ValueError("segment_source_generation_or_sha_mismatch")
            segments._fence(source, source_fd, generation)
            parts = segments._pin_parts(receipt.parent, value["parts"])
            payloads = [{"path": str(receipt.parent / part["name"]), "sha256": part["sha256"],
                         "device": os.fstat(fd).st_dev, "inode": os.fstat(fd).st_ino,
                         "bytes": os.fstat(fd).st_size}
                        for part, fd in zip(value["parts"], parts, strict=True)]
            rows.append({"path": str(receipt), "rows": value["rows"], "layout": "base_delta_v2",
                         "payload_bytes": sum(part["bytes"] for part in payloads), "payloads": payloads,
                         "source_generation": list(generation), "source_sha256": value["source_sha256"],
                         "receipt_mtime_ns": receipt.stat().st_mtime_ns})
        finally:
            for fd in parts:
                os.close(fd)
            if source_fd is not None:
                os.close(source_fd)
            os.close(lock_fd)
    if not rows:
        raise ValueError("no_verified_segment_cache")
    return rows


def observe_segmented_apis(owner, columns):
    """Observe this native PID actually pinning the exact immutable objects.

    Closed sources must NOT be rewritten just to manufacture a fresh receipt.
    This proves reader adoption, not a naturally occurring new-source update.
    """
    expected = {(part["device"], part["inode"]) for row in columns for part in row["payloads"]}
    if not expected:
        raise ValueError("no_verified_segment_objects_to_observe")
    observed = set()
    directory = Path(f"/proc/{int(owner['MainPID'])}/fd")
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(observe_apis)
        deadline = time.monotonic() + 30
        while True:
            for path in directory.iterdir():
                try:
                    info = path.stat()
                except OSError:
                    continue
                identity = info.st_dev, info.st_ino
                if identity in expected:
                    observed.add(identity)
            if pending.done() and (observed == expected or time.monotonic() >= deadline):
                break
            time.sleep(.01)
        result = pending.result()
    if observed != expected:
        raise ValueError("native_pid_did_not_pin_all_verified_segment_objects")
    return {**result, "native_payload_inodes": [list(item) for item in sorted(observed)],
            "native_new_source_update_observed": False}


def observe_apis():
    """Physical deployment and strict source readiness are separate proofs.

    Do not relax the original all-strategy clock validator, hide an absent
    valuation, or spend 150 seconds repeatedly probing a known source gap.
    """
    base = "http://127.0.0.1:8765/"
    status, status_ms = fetch_json(base + "api/status")
    if status.get("simulation_only") is not True or status.get("production_order_possible") is not False:
        raise ValueError("display_violated_readonly_simulation_boundary")
    rows = []
    for key in dashboard.HISTORY_RANGE_SECONDS:
        history, elapsed = fetch_json(base + f"api/history?range={key}")
        current, _ = fetch_json(base + "api/status")
        if history.get("range") != key or len(history.get("history") or []) < 2:
            raise ValueError("display_history_response_invalid")
        source = datetime.fromisoformat(current["source_updated_at_utc"])
        end = datetime.fromisoformat(history["coverage_end_utc"])
        anchor = datetime.fromisoformat(history["anchor_at_utc"])
        if (any(value.tzinfo is None for value in (source, end, anchor))
                or not -60 <= (source-end).total_seconds() <= 180 or end > anchor):
            raise ValueError("display_cache_generation_has_not_caught_up")
        error = None
        try:
            validate_history_clock(current, history)
        except ValueError as failure:
            error = str(failure)
        rows.append({"range": key, "points": len(history["history"]), "total_ms": elapsed,
                     "coverage_end_utc": history["coverage_end_utc"], "strict_data_ready": error is None,
                     "strict_data_error": error})
    return {"url": base, "source_health": status["health"], "status_ms": status_ms,
            "ranges": rows, "strict_data_ready": all(row["strict_data_ready"] for row in rows),
            "source_mutations": False}


def baseline_data_diagnostic(acceptance, output):
    """Confirm a source gap in the original complete, frozen raw control."""
    hashes = accepted_code(acceptance)
    accepted = metadata(acceptance)
    if accepted.get("contract") != "taifex_history_complete_curve_comparison_v1":
        raise ValueError("raw_source_diagnostic_requires_original_raw_control")
    fixture = Path(accepted["source_fixture"])
    clock = datetime.fromisoformat(metadata(fixture / "fixture.json")["clock"])
    os.environ[cache.ENABLE_ENV] = "0"
    curve = dashboard.build_dashboard_history_snapshot(state_dir=fixture / "state", now=clock, range_key="1d")
    digest = hashlib.sha256(json.dumps(curve, sort_keys=True, allow_nan=False,
        separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    if digest != accepted["trials"][0]["output_sha256"]:
        raise ValueError("frozen_raw_control_changed")
    status = metadata(fixture / "state/status.json")
    ids, _ = dashboard._history_strategy_inputs(metadata(fixture / "state/state.json"), status)
    source = datetime.fromisoformat(status["updated_at_utc"])
    latest = {}
    for row in curve["history"]:
        latest[row["strategy_id"]] = max(latest.get(row["strategy_id"], 0), int(row["decision_ts_ns"]))
    gaps = [{"strategy_id": sid, "last_decision_ts_ns": latest.get(sid),
             "source_lag_seconds": (source.timestamp()-latest[sid]/1e9) if sid in latest else None}
            for sid in ids if sid not in latest or source.timestamp()-latest[sid]/1e9 > 180]
    accepted_code(acceptance)
    value = {"state": "original_raw_control_diagnostic", "output_sha256": digest, "code_sha256": hashes,
             "fixture_clock": clock.isoformat(), "source_updated_at_utc": source.isoformat(),
             "source_gaps": gaps, "provider_calls": 0, "gpu_calls": 0, "source_mutations": False,
             "boundary": "source_valuation_gap_not_physical_cache_or_broker_fill_proof"}
    atomic_write_json(output, value)
    print(json.dumps({"state": value["state"], "source_gap_count": len(gaps), "output": str(output)}))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--acceptance", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, help="Prewarm receipt; omit for isolated prewarm only")
    parser.add_argument("--baseline-data-diagnostic", action="store_true")
    args = parser.parse_args(argv)
    if args.output.exists():
        parser.error("new evidence output required")
    if args.baseline_data_diagnostic:
        if args.baseline:
            parser.error("source diagnostic does not take a runtime baseline")
        baseline_data_diagnostic(args.acceptance, args.output)
        return
    hashes = accepted_code(args.acceptance)
    segmented = str(ROOT / "scripts/benchmark_taifex_history_segments.py") in hashes
    owner = identity(UNIT)
    protected = {unit: identity(unit) for unit in PROTECTED}
    root, enabled = owner_environment(owner)
    result = {"contract": "taifex_column_runtime_readback_v1", "started_at_utc": datetime.now(timezone.utc).isoformat(),
              "owner": owner, "protected": protected, "code_sha256": hashes, "launcher_sha256": sha(LAUNCHER),
              "cache_root": str(root), "provider_calls": 0, "gpu_calls": 0, "source_mutations": False,
              "service_restart_requested": False, "column_env_enabled": enabled}
    if segmented:
        result["verifier_sha256"] = sha(Path(__file__))
    try:
        if args.baseline is None:
            if (segmented or str(ROOT / "scripts/benchmark_taifex_allocator_idle.py") in hashes
                    or str(ROOT / "scripts/benchmark_taifex_source_proof_memo.py") in hashes):
                result.update(prewarm_polars_threads=pl.thread_pool_size(),
                              prewarm_allocator_conf=os.environ.get("_RJEM_MALLOC_CONF", ""))
                if (result["prewarm_polars_threads"] != 4
                        or not result["prewarm_allocator_conf"].endswith(ALLOCATOR_CONF)):
                    raise ValueError("prewarm_has_not_loaded_measured_allocator")
            os.environ[cache.CACHE_ENV], os.environ[cache.ENABLE_ENV] = str(root), "1"
            if segmented:
                os.environ[segments.LAYOUT_ENV] = "base_delta_v2"
            began, cpu = time.perf_counter(), time.process_time()
            curve = dashboard.build_dashboard_history_snapshot(state_dir=SOURCE, now=datetime.now(timezone.utc), range_key="1d")
            result.update(state="isolated_prewarm_verified", wall_seconds=time.perf_counter()-began,
                          cpu_seconds=time.process_time()-cpu, history_rows=len(curve["history"]),
                          rss_peak_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024)
        else:
            baseline = metadata(args.baseline)
            if (baseline.get("state") != "isolated_prewarm_verified" or not enabled
                    or baseline["code_sha256"] != hashes or baseline["launcher_sha256"] != sha(LAUNCHER)
                    or baseline["cache_root"] != str(root) or baseline["protected"] != protected
                    or owner["InvocationID"] == baseline["owner"]["InvocationID"]):
                raise ValueError("runtime_not_bound_to_prewarm_and_protected_owners")
            if segmented and baseline.get("verifier_sha256") != result["verifier_sha256"]:
                raise ValueError("runtime_verifier_changed_since_prewarm")
            allocator = (segmented or str(ROOT / "scripts/benchmark_taifex_allocator_idle.py") in hashes
                         or str(ROOT / "scripts/benchmark_taifex_source_proof_memo.py") in hashes)
            if allocator or str(ROOT / "scripts/benchmark_taifex_curve_threads.py") in hashes:
                result["owner_polars_max_threads"] = owner_polars_limit(owner)
                if result["owner_polars_max_threads"] != 4:
                    raise ValueError("display_owner_has_not_loaded_measured_thread_pool")
            if allocator:
                result["owner_allocator_override"] = owner_allocator_override(owner)
                if (result["owner_allocator_override"] != ALLOCATOR_CONF
                        or baseline.get("prewarm_polars_threads") != 4
                        or not baseline.get("prewarm_allocator_conf", "").endswith(ALLOCATOR_CONF)):
                    raise ValueError("display_owner_has_not_loaded_measured_allocator")
            if segmented:
                values = Path(f"/proc/{int(owner['MainPID'])}/environ").read_bytes().split(b"\0")
                if f"{segments.LAYOUT_ENV}=base_delta_v2".encode() not in values:
                    raise ValueError("display_owner_has_not_loaded_base_delta_layout")
                result["columns"] = segmented_cache_readback(root)
                result["api"] = observe_segmented_apis(owner, result["columns"])
                result["state"] = "deployed_base_delta_reader_verified"
            else:
                result["api"] = observe_apis()
                result["state"] = "deployed_physical_cache_verified"
        if "columns" not in result:
            result["columns"] = segmented_cache_readback(root) if segmented else cache_readback(root)
        if args.baseline and not segmented and not any(
                row["receipt_mtime_ns"] > baseline["columns"][index]["receipt_mtime_ns"]
                for index, row in enumerate(result["columns"])):
            raise ValueError("new_owner_has_not_proved_a_column_refresh_yet")
        if args.baseline and (segmented or str(ROOT / "scripts/benchmark_taifex_source_proof_memo.py") in hashes):
            # An old owner can refresh between prewarm and restart. Such a
            # receipt is not proof that the new process has rebuilt a source.
            start_ns = owner_start_wall_ns(owner)
            result["owner_start_wall_ns"] = start_ns
            if not segmented and not any(row["receipt_mtime_ns"] > start_ns for row in result["columns"]):
                raise ValueError("no_column_refresh_after_actual_new_owner_start")
            if any(Path(path).stat().st_mtime_ns > start_ns for path in hashes):
                raise ValueError("benchmark_code_newer_than_display_owner")
        if identity(UNIT) != owner or {unit: identity(unit) for unit in PROTECTED} != protected:
            raise ValueError("owner_changed_during_readback")
        accepted_code(args.acceptance)
        if result["launcher_sha256"] != sha(LAUNCHER):
            raise ValueError("launcher_changed_during_readback")
    except Exception as error:
        result.update(state="failed_readback", error_type=type(error).__name__, error=str(error))
        raise
    finally:
        result["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        atomic_write_json(args.output, result)
    print(json.dumps({"state": result["state"], "output": str(args.output), "provider_calls": 0, "gpu_calls": 0}))


if __name__ == "__main__":
    main()
