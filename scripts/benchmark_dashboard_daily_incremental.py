"""Offline daily-addition probes with full-output oracles and durable timings.

Synthetic inputs, original producers/readers, no provider/GPU/order/service
actions. Source hashing, output rewrites and state I/O are included where used;
the causal-stage probe is NOT a whole Factorized normalizer benchmark.
"""
from __future__ import annotations

import argparse
import cProfile
from copy import deepcopy
from datetime import date, datetime, timedelta, UTC
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import resource
import pstats
import shutil
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if __name__ == "__main__":
    from stockagent.data.source_processing_runtime import configure_cpu_source_runtime
    source_profile = configure_cpu_source_runtime(2)
    import pyarrow as pa
    pa.set_cpu_count(source_profile["effective_threads"])

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from polars.testing import assert_frame_equal

from downloader.artifact_io import atomic_write_json
from scripts.prepare_tw_day_trade_factorized_panel import causal_state_blocks
from stockagent.data.causal_projection_state import load_state, save_state
from stockagent.data.factorized_panel import file_sha256
from stockagent.data.tw_public_features import build_tw_public_training_features
from stockagent.data.tw_public_release_schedule import RULES
from scripts.build_tw_official_symbol_parquets import _write_symbol_batch, _symbol_code_identity, _twse_frame, _official_frame
from scripts.build_tw_release_schedule_dataset import normalize_source
from stockagent.data.tw_public_release_schedule import feature_name
from stockagent.data.tw_public_incremental import session_fingerprints, table_content_sha256
from stockagent.data.parquet_group_projection import ParquetGroupProjection
from stockagent.data_sync.cache_capacity import load_capacity_policy
from stockagent.live.dashboard_updates import json_snapshot_metrics, read_json_snapshot

CODE = (Path(__file__), ROOT / "scripts/prepare_tw_day_trade_factorized_panel.py",
        ROOT / "stockagent/data/causal_projection_state.py", ROOT / "stockagent/data/tw_public_incremental.py",
        ROOT / "stockagent/data/tw_public_features.py", ROOT / "stockagent/live/dashboard_updates.py",
        ROOT / "scripts/build_tw_official_symbol_parquets.py", ROOT / "scripts/build_tw_release_schedule_dataset.py",
        ROOT / "stockagent/data/finlab_research_overlay.py", ROOT / "stockagent/data/factorized_normalization_reuse.py",
        ROOT / "stockagent/data_sync/factorized_projection_reuse.py", ROOT / "stockagent/data_sync/cache_capacity.py",
        ROOT / "stockagent/data/parquet_group_projection.py", ROOT / "stockagent/data_sync/immutable_cache_files.py",
        ROOT / "stockagent/data/partitioned_parquet.py", ROOT / "downloader/ohlcv_hot_tail.py",
        ROOT / "stockagent/data/panel.py",
        ROOT / "stockagent/data/factorized_panel.py", ROOT / "stockagent/data/tw_day_trade_mixed_frequency.py",
        ROOT / "stockagent/data/tw_public_release_schedule.py", ROOT / "stockagent/data/tw_security.py",
        ROOT / "stockagent/data/tw_delisting_rules.py", ROOT / "stockagent/data/source_processing_runtime.py",
        ROOT / "downloader/artifact_io.py", ROOT / "configs/data_sync/cache_capacity.json",
        ROOT / "configs/data_sync/prepared_capacity.json")


def timed(function):
    start, cpu = time.perf_counter(), time.process_time()
    value = function()
    return value, {"wall_ms": (time.perf_counter() - start) * 1000,
                   "cpu_ms": (time.process_time() - cpu) * 1000}


def snapshots(root, files=64):
    payload = {"day": "2026-10-19", "rows": [{"symbol": str(i), "value": i / 7, "missing": None}
                                             for i in range(512)]}
    trials = []
    for variant in ("full_decode", "source_incremental"):
        folder = root / variant
        folder.mkdir()
        paths = [folder / f"{index}.json" for index in range(files)]
        for path in paths:
            atomic_write_json(path, payload)
        before = json_snapshot_metrics()

        def work(updated_day):
            digest = hashlib.sha256()
            for step in range(3):
                if step == 1:
                    atomic_write_json(paths[-1], {**payload, "day": updated_day})
                for path in paths:
                    value = json.loads(path.read_bytes()) if variant == "full_decode" else read_json_snapshot(path)
                    digest.update(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())
            return digest.hexdigest()

        oracle, timing = timed(lambda: work("2026-10-20"))
        after = json_snapshot_metrics()
        # Keep first-fill cost above. A running monitor's later updates are a
        # separate observation, never relabel the first run as a warm hit.
        steady_oracle, steady = timed(lambda: work("2026-10-21"))
        steady_after = json_snapshot_metrics()
        trials.append({"variant": variant, **timing, "complete_output_sha256": oracle,
            "source_decodes": 3 * files if variant == "full_decode" else after["decodes"] - before["decodes"],
            "source_cache_hits": after["hits"] - before["hits"],
            "steady": {**steady, "complete_output_sha256": steady_oracle,
                "source_decodes": 3 * files if variant == "full_decode" else steady_after["decodes"] - after["decodes"],
                "source_cache_hits": steady_after["hits"] - after["hits"]}})
    assert trials[0]["complete_output_sha256"] == trials[1]["complete_output_sha256"]
    assert trials[0]["steady"]["complete_output_sha256"] == trials[1]["steady"]["complete_output_sha256"]
    return {"scope": "64 source snapshots x 3 updates; complete JSON encoding included", "trials": trials}


def official_symbols(root, sessions, symbols, workers=8):
    """Full per-symbol canonical transform/write; source merge is separate."""
    days = [date(2023, 1, 2) + timedelta(days=i) for i in range(sessions + 1)]
    frames = {}
    for symbol in range(symbols):
        code = str(1000 + symbol)
        prices = [100.0 + symbol + day / 10 for day in range(sessions + 1)]
        frames[code] = pl.DataFrame({"date": days, "symbol": [code] * len(days),
            "market": ["twse"] * len(days), "name": [code] * len(days), "security_type": ["stock"] * len(days),
            **{column: prices for column in ("open", "max", "min", "close")},
            "Trading_Volume": [10000.] * len(days), "signed_change": [0.] + [.1] * sessions,
            "source_factor": [float("nan")] * len(days), "source_adjclose": [float("nan")] * len(days),
            "reference_override": [float("nan")] * len(days), "_yahoo_fallback_factor": [float("nan")] * len(days),
            "_legacy_source_id": ["official"] * len(days), "quote_source": ["twse_official"] * len(days)})
    output, reference = root / "symbol_incremental", root / "symbol_full"
    reference_batched = root / "symbol_full_batched"
    partitioned = root / "symbol_partitioned"
    partitioned.mkdir()
    output.mkdir()
    reference.mkdir()
    reference_batched.mkdir()
    identity = _symbol_code_identity()
    states = {}

    def build(folder, count, previous=None, sink=None, *, batch_sync=True, layout="file"):
        result = _write_symbol_batch(folder, {code: frame.head(count) for code, frame in frames.items()},
            requested_end_date=days[count-1].isoformat(), workers=workers,
            previous_states=previous, state_sink=sink, code_identity=identity,
            batch_directory_fsync=batch_sync, output_layout=layout)
        for item in result:
            if item.status != "created":
                raise ValueError(item.message)
        return result

    _, bootstrap = timed(lambda: build(output, sessions, sink=states))
    # A fresh JSON readback makes checkpoint persistence part of the contract.
    checkpoint = root / "symbol_state.json"
    atomic_write_json(checkpoint, states)
    prior = json.loads(checkpoint.read_bytes())
    updated_states = {}
    result, incremental = timed(lambda: build(output, sessions + 1, prior, updated_states))
    _, full = timed(lambda: build(reference, sessions + 1, batch_sync=False))
    # Isolate suffix work from the earlier durability optimization: this
    # baseline has the SAME worker count, one directory barrier, complete raw
    # input/output proofs and checkpoint construction as the incremental path.
    _, full_batched = timed(lambda: build(reference_batched, sessions + 1, sink={}))
    unchanged_results, unchanged = timed(lambda: build(output, sessions + 1, updated_states, {}))
    assert all(item.reused_output for item in unchanged_results)
    partition_states = {}
    _, partition_bootstrap = timed(lambda: build(partitioned, sessions, sink=partition_states, layout="year"))
    atomic_write_json(root / "partition_symbol_state.json", partition_states)
    partition_prior = json.loads((root / "partition_symbol_state.json").read_bytes())
    partition_updated = {}
    partition_results, partition_timing = timed(lambda: build(partitioned, sessions + 1,
        partition_prior, partition_updated, layout="year"))
    partition_unchanged_results, partition_unchanged = timed(lambda: build(partitioned, sessions + 1,
        partition_updated, {}, layout="year"))
    assert all(item.reused_output for item in partition_unchanged_results)

    def oracle():
        from stockagent.data.partitioned_parquet import read_table
        for code in frames:
            assert_frame_equal(pl.read_parquet(output / f"{code}_features.parquet"),
                pl.read_parquet(reference / f"{code}_features.parquet"), check_exact=True)
            assert_frame_equal(pl.read_parquet(output / f"{code}_features.parquet"),
                pl.read_parquet(reference_batched / f"{code}_features.parquet"), check_exact=True)
            actual = read_table(partitioned / f"{code}_features.parquet")
            expected = pq.read_table(reference / f"{code}_features.parquet")
            assert table_content_sha256(actual) == table_content_sha256(expected)
    _, verification = timed(oracle)
    return {"scope": "all per-symbol canonical transformations, exact checkpoint proof and whole Parquet writes; excludes upstream official source merge",
        "full": full, "full_batched_with_same_proofs": full_batched,
        "incremental": incremental, "bootstrap": bootstrap, "independent_full_oracle": verification,
        "unchanged_verified_outputs": unchanged,
        "symbols": symbols, "history_rows": sessions * symbols,
        "workers": workers, "full_directory_commits": symbols,
        "full_batched_directory_commits": 1,
        "incremental_directory_commits": 1, "every_payload_fsync_retained": True,
        "reused_adjustment_rows": sum(item.reused_adjustment_rows for item in result),
        "rebuilt_adjustment_rows": sum(item.rebuilt_adjustment_rows for item in result),
        "summed_incremental_worker_seconds": {key: sum(getattr(item, key) for item in result)
            for key in ("input_proof_seconds", "normalization_seconds", "adjustment_seconds",
                        "quote_assembly_seconds", "parquet_write_seconds", "output_proof_seconds")},
        "all_columns_nulls_and_values_equal": True, "whole_changed_symbol_rewrite_retained": True,
        "partitioned": {"bootstrap": partition_bootstrap, "new_day": partition_timing,
            "summed_worker_seconds": {key: sum(getattr(item, key) for item in partition_results)
                for key in ("input_proof_seconds", "normalization_seconds", "adjustment_seconds",
                            "quote_assembly_seconds", "parquet_write_seconds", "output_proof_seconds")},
            "summed_writer_phase_seconds": {key: sum(item.partitioned_write["phase_seconds"].get(key, 0.0)
                for item in partition_results) for key in sorted({key for item in partition_results
                    for key in item.partitioned_write["phase_seconds"]})},
            "unchanged_verified_outputs": partition_unchanged,
            "written_partitions": sum(item.partitioned_write["written_partitions"] for item in partition_results),
            "reused_partitions": sum(item.partitioned_write["reused_partitions"] for item in partition_results),
            "written_rows": sum(item.partitioned_write["written_rows"] for item in partition_results),
            "reused_rows": sum(item.partitioned_write["reused_rows"] for item in partition_results),
            "all_columns_raw_bits_nulls_and_order_equal": True}}


def normalization_tail(root, sessions, symbols):
    days = [date(2023, 1, 2) + timedelta(days=i) for i in range(sessions + 2)]
    key = "security_lending:借券成交"
    path = root / "wide_finlab.parquet"
    pl.DataFrame({"source_index": [day.isoformat() for day in days[:-1]],
        **{str(1000 + index): [index + day / 10 for day in range(sessions + 1)]
           for index in range(symbols)}}).write_parquet(path, row_group_size=32)
    universe = [str(1000 + i) for i in range(symbols)]
    cutoff = days[-3]
    (full, _), full_time = timed(lambda: normalize_source(path, key, days, universe))
    (tail, _), delta_time = timed(lambda: normalize_source(path, key, days, universe, available_after=cutoff))
    assert_frame_equal(tail, full.filter(pl.col("date") > cutoff), check_exact=True)
    return {"scope": "actual wide daily FinLab normalizer; source-index read, original clocks and independent full-tail oracle",
        "full": full_time, "incremental": delta_time, "full_observations": full.height,
        "tail_observations": tail.height, "feature": feature_name(key),
        "all_columns_nulls_and_values_equal": True,
        "raw_archive_full_content_proof_measured": False}


def source_proof(root, sessions, symbols):
    """Full archive SHA plus original complete typed session proofs, not a hit."""
    days = [date(2023, 1, 2) + timedelta(days=i) for i in range(sessions + 1)]
    path = root / "dated_source.parquet"
    def raw(count):
        return pa.table({"date": [day.isoformat() for day in days[:count] for _ in range(symbols)],
            "symbol": [str(1000 + item) for _ in range(count) for item in range(symbols)],
            **{f"original_{index}": [None if (day + item + index) % 17 == 0 else
                (-0.0 if day % 37 == 0 else day + item / 100 + index / 10)
                for day in range(count) for item in range(symbols)] for index in range(24)}})
    # Fixed physical groups, not per-symbol/per-day tiny files. Existing raw
    # writers may regroup: that remains an explicit conservative cache miss.
    pq.write_table(raw(sessions), path, row_group_size=32_768, compression="zstd")
    previous = session_fingerprints(path, capture_physical=True)
    pq.write_table(raw(sessions + 1), path, row_group_size=32_768, compression="zstd")
    def proof(cached):
        before = file_sha256(path)
        value = session_fingerprints(path, previous=cached, capture_physical=cached is not None)
        if before != file_sha256(path):
            raise ValueError("source changed while measuring full content proof")
        return value
    full, full_time = timed(lambda: proof(None))
    incremental, delta_time = timed(lambda: proof(previous))
    assert {key: incremental[key] for key in full} == full
    return {"scope": "full source SHA before/after plus all typed session proofs; encoded prefix is not a hot-cache hit",
        "full": full_time, "incremental": delta_time,
        "source_bytes": path.stat().st_size, "rows": (sessions + 1) * symbols,
        "all_columns": 26, "work": incremental["row_group_work"],
        "all_typed_session_proofs_equal": True, "source_full_sha_retained": True}


def official_source_projection(root, sessions, symbols):
    days = [date(2023, 1, 2) + timedelta(days=i) for i in range(sessions + 1)]
    path = root / "native_official_source.parquet"
    def raw(count):
        return pa.table({"date": [day.isoformat() for day in days[:count] for _ in range(symbols)],
            "證券代號": [str(1000 + item) for _ in range(count) for item in range(symbols)],
            "證券名稱": [" 測試原始值 "] * (count * symbols),
            **{name: [str(100 + day / 10 + item) for day in range(count) for item in range(symbols)]
               for name in ("開盤價", "最高價", "最低價", "收盤價")},
            "成交股數": ["10,000"] * (count * symbols), "漲跌(+/-)": ["+"] * (count * symbols),
            "漲跌價差": ["0.1"] * (count * symbols)})
    pq.write_table(raw(sessions), path, row_group_size=32_768, compression="zstd")
    cache_root = root / "official_source_groups"
    identity = _symbol_code_identity()
    _, bootstrap = timed(lambda: _twse_frame(path, source_cache=ParquetGroupProjection(cache_root, identity)))
    pq.write_table(raw(sessions + 1), path, row_group_size=32_768, compression="zstd")
    def build(cache):
        before = file_sha256(path)
        frame = _twse_frame(path, source_cache=cache)
        if file_sha256(path) != before:
            raise ValueError("official source changed during projection")
        return frame
    full, full_time = timed(lambda: build(None))
    cache = ParquetGroupProjection(cache_root, identity)
    incremental, delta_time = timed(lambda: build(cache))
    assert table_content_sha256(full.to_arrow()) == table_content_sha256(incremental.to_arrow())
    return {"scope": "original TWSE row-local parser, both full source SHAs, new cache member writes and complete output concatenation",
        "full": full_time, "incremental": delta_time, "bootstrap": bootstrap,
        "all_rows": full.height, "all_columns": full.width, "source_bytes": path.stat().st_size,
        "work": cache.stats(), "all_typed_values_nulls_and_bits_equal": True,
        "whole_official_merge_measured": False}


def real_official_source_merge(root, public):
    """Read-only captured source; unchanged-source reuse is NOT new-day proof."""
    public = Path(public).resolve(strict=True)
    paths = [public / name for name in ("twse_daily_ohlcv.parquet", "tpex_daily_ohlcv.parquet")]
    metadata = [pq.read_metadata(path) for path in paths]
    rows = sum(item.num_rows for item in metadata)
    if rows > 20_000_000 or any(item.num_columns > 64 for item in metadata):
        raise ValueError("native source probe exceeds admitted shape")
    memory = {key: int(value.split()[0]) * 1024 for key, value in
              (line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())}
    if memory["MemAvailable"] < max(4 * 1024**3, rows * 1024):
        raise ValueError("native source probe lacks observed memory headroom")
    fixed_sources = {str(path): file_sha256(path) for path in paths}
    proofs, measurements = {}, {}
    cache_root = root / "real_official_source_groups"
    for label in ("full_parser", "cache_bootstrap", "verified_group_reuse"):
        print(json.dumps({"native_official_merge": label, "state": "started"}), flush=True)
        cache = None if label == "full_parser" else ParquetGroupProjection(cache_root, _symbol_code_identity(),
            capacity_policy=load_capacity_policy())
        values, timing = timed(lambda: _official_frame(public, source_cache=cache))
        frame, *other = values
        digest, verification_time = timed(lambda frame=frame: table_content_sha256(frame.to_arrow()))
        proofs[label] = {"typed_values_sha256": digest, "source_receipts": other[:-1],
                         "merge_statistics": asdict(other[-1])}
        measurements[label] = {**timing, "rows": frame.height, "columns": frame.width,
            "independent_typed_output_proof": verification_time,
            "source_projection": cache.stats() if cache is not None else {"enabled": False}}
        del frame, values
    if not proofs["full_parser"] == proofs["cache_bootstrap"] == proofs["verified_group_reuse"]:
        raise ValueError("native merged source oracle differs")
    if fixed_sources != {str(path): file_sha256(path) for path in paths}:
        raise ValueError("native captured sources changed during probe")
    return {"scope": "entire canonical official merge on fixed real sources, no legacy/Yahoo arguments; reuse is unchanged captured sources, NOT next-day append",
        "measurements": measurements, "source_rows": rows,
        "source_files": fixed_sources, "all_merged_typed_values_and_original_receipts_equal": True,
        "production_source_writes": False, "service_restarts": 0}


def public_features(root, sessions, symbols):
    source = root / "public"
    source.mkdir()
    days = [date(2024, 1, 2) + timedelta(days=i) for i in range(sessions + 1)]
    # These are explicitly synthetic exchange-session facts, not a real
    # calendar claim. The same source/calendar owner is used on both paths.
    def raw(count):
        return pl.DataFrame({"date": [day.isoformat() for day in days[:count] for _ in range(symbols)],
            "證券代號": [str(1000 + s) for _ in range(count) for s in range(symbols)],
            **{name: [str(100. + d / 10 + s) for d in range(count) for s in range(symbols)]
               for name in ("收盤價", "最高價", "最低價")},
            **{name: ["100000"] * (count * symbols) for name in ("成交股數", "成交金額", "成交筆數")}})

    raw(sessions).write_parquet(source / "twse_daily_ohlcv.parquet")
    path = root / "incremental.parquet"
    common = dict(input_dir=source, symbols_root=None, allow_daily_publication_lag=True)
    bootstrap, cold = timed(lambda: build_tw_public_training_features(**common, output_path=path,
        end_date=days[-2], incremental_auto=True))
    raw(sessions + 1).write_parquet(source / "twse_daily_ohlcv.parquet")
    result, delta = timed(lambda: build_tw_public_training_features(**common, output_path=path,
        end_date=days[-1], incremental_auto=True))
    reference = root / "full.parquet"
    full, baseline = timed(lambda: build_tw_public_training_features(**common, output_path=reference, end_date=days[-1]))
    def oracle():
        assert_frame_equal(pl.read_parquet(path), pl.read_parquet(reference), check_row_order=True, check_column_order=True)
    _, verification = timed(oracle)
    assert result.build_mode == "incremental_tail" and result.reused_rows == bootstrap.rows
    return {"scope": "complete canonical public-feature build on one synthetic OHLCV archive",
        "full": baseline, "incremental": delta, "bootstrap": cold, "independent_full_oracle": verification,
        "all_rows": result.rows, "all_columns": len(pl.read_parquet_schema(reference)),
        "feature_count": full.feature_count, "reused_rows": result.reused_rows,
        "rebuilt_rows": result.rows - result.reused_rows, "incremental_start_date": result.incremental_start_date,
        "input_partitions": result.input_partitions["work"], "stages_seconds": result.stage_elapsed_seconds,
        "full_output_bytes": reference.stat().st_size, "incremental_output_bytes": path.stat().st_size,
        "whole_parquet_rewrite_remains": True, "all_columns_values_nulls_equal": True}


def causal(root, sessions, symbols, features):
    days = [date(2024, 1, 2) + timedelta(days=i) for i in range(sessions + 2)]
    names = [str(1000 + index) for index in range(symbols)]
    definitions = [{"feature": f"f{i}", "rule": asdict(RULES["quarter"])} for i in range(features)]
    events = pl.DataFrame([{"date": days[day], "symbol": symbol, "feature": f"f{index}",
        "value": index + day / 11 + 1.123456789, "period": days[day].isoformat()}
        for day in range(0, sessions + 1, 20) for symbol in names for index in range(features)])
    options = dict(model_channel_policy="value_only", max_block_bytes=8 * 1024**2)
    old_states = []
    old = list(causal_state_blocks(days[:-1], names, definitions, events, None, checkpoint_row=sessions - 1,
        on_checkpoint=lambda state: old_states.append(deepcopy(state)), **options))
    old_dense = np.concatenate([block for _, block, _ in old])
    saved = save_state(root, "previous", old_states[0])

    def full_work():
        blocks = list(causal_state_blocks(days, names, definitions, events, None, checkpoint_row=sessions,
            on_checkpoint=lambda state: save_state(root, "full", state), **options))
        return np.concatenate([block for _, block, _ in blocks]), blocks[-1][2]

    def delta_work():
        state = load_state(root, saved)
        blocks = list(causal_state_blocks(days, names, definitions, events, None, resume_state=state,
            checkpoint_row=sessions, on_checkpoint=lambda value: save_state(root, "delta", value), **options))
        return np.concatenate([old_dense[:sessions - 1], *(block for _, block, _ in blocks)]), blocks[-1][2]

    reference, full_timing = timed(full_work)
    candidate, delta_timing = timed(delta_work)
    np.testing.assert_array_equal(reference[0].view(np.uint32), candidate[0].view(np.uint32))
    np.testing.assert_array_equal(reference[1], candidate[1])
    return {"scope": "canonical causal sweep + compressed lossless checkpoint I/O + complete dense assembly; NOT whole source normalization",
        "full": full_timing, "incremental": delta_timing, "history_sessions": sessions,
        "symbols": symbols, "individual_channels": features, "raw_event_rows": events.height,
        "reused_rows": sessions - 1, "rebuilt_completed_rows": 2, "dummy_terminal_row_preserved": True,
        "complete_input_bits_and_availability_counts_equal": True,
        "source_normalization_and_generation_publish_measured": False}


def real_factorized_base(root, capture_root):
    """Independent full BASE oracle, never a read of the daily owner's cache."""
    from dataclasses import replace
    import os
    import requests
    import torch
    import urllib.request
    from scripts import prepare_tw_day_trade_factorized_live as owner
    from scripts.benchmark_factorized_live_owner import shared_identity
    from scripts.benchmark_live_base_panel_cutoff import panel_identity
    from stockagent.data_sync.cache_capacity import load_prepared_capacity_policy, reserve_cache_write
    from stockagent.data_sync.immutable_cache_files import payload_info

    captured = Path(capture_root).resolve(strict=True)
    progress = captured / "progress.json"
    progress_sha = file_sha256(progress)
    snapshot = json.loads(progress.read_bytes())["stages"]["capture_real_sources"]["result"]
    if snapshot["contract"] != "captured_factorized_real_daily_sources_v1":
        raise ValueError("complete captured daily-owner sources required")

    def verify_sources():
        if file_sha256(progress) != progress_sha:
            raise ValueError("captured source receipt changed")
        for proof in snapshot["proofs"]:
            path = Path(proof["path"])
            info = payload_info(path)
            if (path.resolve(strict=True) != path or not path.is_relative_to(captured)
                    or info["mode"] & 0o222 or info["uid"] != os.geteuid()
                    or info["size"] != proof["bytes"] or file_sha256(path) != proof["sha256"]):
                raise ValueError("captured source is mutable, missing or changed")

    _, source_verification = timed(verify_sources)
    codes = shared_identity()
    def forbidden(*_args, **_kwargs):
        raise AssertionError("independent captured BASE oracle cannot call provider or CUDA")
    if torch.cuda.is_initialized():
        raise ValueError("independent BASE process already initialized CUDA")
    requests.sessions.Session.request = forbidden
    urllib.request.urlopen = forbidden
    torch.cuda._lazy_init = forbidden
    public = captured / "public_complete"
    parent = json.loads((captured / "parent/factorized_manifest.json").read_bytes())
    config = owner.load_config(captured / "config.json")
    cache = root / "independent_base_cache"
    if cache.exists():
        raise ValueError("independent BASE requires an empty private cache")
    config = replace(config, data=replace(config.data,
        parquet_root=str(public / "stocks"),
        tw_public_feature_path=str(public / "features/tw_public_stock_daily.parquet"),
        day_trade_physical_public_feature_path=str(public / "features/tw_public_stock_daily.parquet"),
        panel_cache_root=str(cache), panel_start_date=snapshot["original_start"]))
    def build():
        with reserve_cache_write(root, key=progress_sha,
                maximum_growth_bytes=load_prepared_capacity_policy()["maximum_generation_bytes"],
                family="prepared", policy=load_capacity_policy()):
            panel = owner.build_panel(config.data.parquet_root, **owner._build_panel_kwargs(config))
            panel = owner.subset_panel_symbols(panel, parent["symbols"], allow_missing_masked=True)
            if panel.feature_names != parent["base_feature_names"] or str(panel.dates[-1])[:10] != snapshot["latest_completed"]:
                raise ValueError("independent full BASE axes/completed session differ")
            return panel_identity(panel)
    oracle, cold = timed(build)
    _, final_source_verification = timed(verify_sources)
    if shared_identity() != codes:
        raise ValueError("independent BASE implementation changed")
    return {"scope": "full original build_panel on retained latest real sources; separate empty cache; no Factorized inference",
        "capture_receipt": {"path": str(progress), "sha256": progress_sha}, "code_sha256": codes,
        "source_verification": source_verification, "cold_complete_owner": cold,
        "final_source_verification": final_source_verification, "complete_base_oracle": oracle,
        "private_cache_initially_absent": True, "paired_daily_receipt_still_required": True}


def _bounded_json_receipt(path):
    path = Path(path).resolve(strict=True)
    with path.open("rb") as stream:
        raw = stream.read(64 * 1024**2 + 1)
    if len(raw) > 64 * 1024**2:
        raise ValueError("diagnostic receipt exceeds byte bound")
    digest = hashlib.sha256(raw).hexdigest()
    if file_sha256(path) != digest:
        raise ValueError("diagnostic receipt changed while reading")
    return json.loads(raw), {"path": str(path), "sha256": digest, "bytes": len(raw)}


def wait_for_daily_run(path, *, maximum_seconds=4*3600):
    """Observe only the named canonical benchmark run; never restart it."""
    started = time.monotonic()
    while True:
        receipt, _ = _bounded_json_receipt(path)
        state = receipt.get("state")
        if state == "succeeded" and receipt.get("exit_code") == 0:
            return
        if state not in {"queued", "running"}:
            raise ValueError(f"daily owner run did not succeed: {state}")
        elapsed = time.monotonic() - started
        if elapsed >= maximum_seconds:
            raise TimeoutError("daily owner acceptance still incomplete; no successful pair receipt")
        time.sleep(min(15, maximum_seconds - elapsed))


def verify_real_factorized_pair(output, daily_path, independent_path, *, expected_verifier_sha=None,
                                projection_verifier=None):
    """Join independent BASE and original whole-daily evidence, not two caches.

    Rechecks retained source bytes, owner code and every published Factorized
    member. No provider, source publication, trading or live readiness claim.
    """
    from scripts.benchmark_factorized_daily_owner import compare_complete
    from stockagent.data_sync.cache_capacity import GenerationWriteBudget, reserve_cache_write
    from stockagent.data_sync.immutable_cache_files import payload_info

    started = time.perf_counter()
    output = Path(output).absolute()
    if (output.exists() or output.is_symlink() or output.resolve() != output
            or not output.is_relative_to(ROOT / "artifacts/benchmarks")):
        raise ValueError("fresh private acceptance output required")
    own_code = expected_verifier_sha or file_sha256(Path(__file__))
    if file_sha256(Path(__file__)) != own_code:
        raise ValueError("pair verifier changed while waiting")
    daily, daily_proof = _bounded_json_receipt(daily_path)
    independent, independent_proof = _bounded_json_receipt(independent_path)
    if daily.get("status") != "accepted_real_source_daily_owner_not_pit_live_or_inference_readiness":
        raise ValueError("full real daily owner acceptance is absent")
    if independent.get("status") != "accepted_captured_source_stage_oracles_not_daily_or_live_readiness":
        raise ValueError("independent cold BASE acceptance is absent")
    reference = independent["real_factorized_base"]
    if reference.get("private_cache_initially_absent") is not True:
        raise ValueError("independent BASE cache was not fresh")
    stages = daily["stages"]
    measured = stages["new_session_complete_owner"]["result"]
    full = stages["fresh_full_latest_owner"]["result"]
    projection = stages["complete_independent_full_output_oracle"]["result"]
    if projection.get("every_individual_common_value_bit_equal") is not True:
        raise ValueError("complete Factorized cell oracle is absent")
    expected = reference["complete_base_oracle"]
    if (measured.get("complete_base_oracles") != [expected]
            or full.get("complete_base_oracles") != [expected]):
        raise ValueError("independent complete BASE fields/arrays/masks differ")
    captured, capture_proof = _bounded_json_receipt(reference["capture_receipt"]["path"])
    if capture_proof["sha256"] != reference["capture_receipt"]["sha256"]:
        raise ValueError("independent source-capture receipt changed")
    snapshot = captured["stages"]["capture_real_sources"]["result"]
    daily_snapshot = stages.get("reuse_captured_real_sources", stages.get("capture_real_sources", {})).get("result")
    if snapshot != daily_snapshot:
        raise ValueError("BASE and daily owner did not use the same captured release")
    captured_root = Path(capture_proof["path"]).parent
    for proof in snapshot["proofs"]:
        path = Path(proof["path"])
        if not path.resolve(strict=True).is_relative_to(captured_root):
            raise ValueError("source proof escaped private capture")
        info = payload_info(path)
        if (info["mode"] & 0o222 or info["uid"] != os.geteuid()
                or info["size"] != proof["bytes"] or file_sha256(path) != proof["sha256"]):
            raise ValueError("retained paired source bytes changed")
    codes = dict(daily["code_sha256"])
    for path, digest in reference["code_sha256"].items():
        if path in codes and codes[path] != digest:
            raise ValueError("independent BASE and daily implementation ABI differ")
        codes[path] = digest
    # Reuse the existing complete all-cell comparator, including its entire
    # member byte verification. The earlier receipt did not pin manifest
    # bytes, so merely trusting today's manifest inventory would be weaker.
    projection_recheck = (projection_verifier or compare_complete)(measured, full)
    if projection_recheck != projection:
        raise ValueError("complete paired Factorized projection changed")
    if (any(file_sha256(Path(path)) != digest for path, digest in codes.items())
            or file_sha256(Path(__file__)) != own_code):
        raise ValueError("paired original-owner implementation changed")
    for proof in (daily_proof, independent_proof, capture_proof):
        if file_sha256(Path(proof["path"])) != proof["sha256"]:
            raise ValueError("paired receipt changed during verification")
    result = {"contract": "independent_real_base_and_factorized_daily_pair_v1",
        "status": "accepted_real_source_daily_and_independent_base_not_pit_live_or_inference_readiness",
        "daily_owner_receipt": daily_proof, "independent_base_receipt": independent_proof,
        "source_capture_receipt": capture_proof, "complete_base_oracle": expected,
        "complete_projection_oracle": projection,
        "retained_source_members_verified": len(snapshot["proofs"]), "owner_code_sha256": codes,
        "verifier_code_sha256": own_code, "provider_calls": 0, "production_writes": False,
        "service_restarts": 0, "retrospective_subset_not_historical_pit": True,
        "verification_wall_ms": (time.perf_counter() - started) * 1000}
    output.mkdir(parents=True, mode=0o700)
    with reserve_cache_write(output, key=daily_proof["sha256"], maximum_growth_bytes=4*1024**2,
                             family="prepared", policy=load_capacity_policy()), GenerationWriteBudget(4*1024**2) as budget:
        budget.charge_json(result)
        atomic_write_json(output / "receipt.json", result)
    return result


def benchmark(output, *, sessions=320, symbols=128, features=32, stages=None, workers=8,
              real_public_root=None, factorized_capture_root=None):
    output = output.absolute()
    if (output.exists() or output.is_symlink() or output.resolve() != output
            or not output.is_relative_to(ROOT / "artifacts/benchmarks")):
        raise ValueError("fresh private output below artifacts/benchmarks required")
    session_limit = 8000 if set(stages or ()) == {"official_symbols"} else 1000
    if not 3 <= sessions <= session_limit or not 1 <= symbols <= 512 or not 1 <= features <= 256:
        raise ValueError("bounded sessions/symbols/features exceeded")
    if type(workers) is not int or not 1 <= workers <= 16:
        raise ValueError("bounded source worker count exceeded")
    if shutil.disk_usage(ROOT).free < 25 * 1024**3 + sessions * symbols * 4096:
        raise ValueError("retain 25 GiB after bounded benchmark admission")
    identity = {str(path.relative_to(ROOT)): file_sha256(path) for path in CODE}
    output.mkdir(parents=True, mode=0o700)
    result = {"contract": "all_dashboard_daily_incremental_probes_v1", "observed_at_utc": datetime.now(UTC).isoformat(),
        "synthetic_sources": real_public_root is None and factorized_capture_root is None,
        "provider_calls": 0, "production_source_writes": False,
        "production_publications": 0, "service_restarts": 0, "code_sha256": identity,
        "runtime": {"numpy": np.__version__, "polars": pl.__version__, "arrow": pa.__version__,
                    "polars_observed_threads": pl.thread_pool_size(), "arrow_observed_threads": pa.cpu_count(),
                    "process_cpu_quota": False}}
    available = {"snapshots", "public_features", "official_symbols", "normalization_tail", "source_proof",
                 "official_source_projection", "factorized_causal"}
    if real_public_root is not None:
        available.add("real_official_source_merge")
    if factorized_capture_root is not None:
        available.add("real_factorized_base")
    selected = set(stages or available)
    if factorized_capture_root is not None and selected != {"real_factorized_base"}:
        raise ValueError("independent BASE capture probe must be selected alone")
    if not selected <= available:
        raise ValueError("unknown measured stage")
    result["selected_stages"] = sorted(selected)
    for name, call in (("snapshots", lambda: snapshots(output)),
                       ("public_features", lambda: public_features(output, sessions, symbols)),
                       ("official_symbols", lambda: official_symbols(output, sessions, symbols, workers)),
                       ("normalization_tail", lambda: normalization_tail(output, sessions, symbols)),
                       ("source_proof", lambda: source_proof(output, sessions, symbols)),
                       ("official_source_projection", lambda: official_source_projection(output, sessions, symbols)),
                       ("real_official_source_merge", lambda: real_official_source_merge(output, real_public_root)),
                       ("real_factorized_base", lambda: real_factorized_base(output, factorized_capture_root)),
                       ("factorized_causal", lambda: causal(output, sessions, symbols, features))):
        if name not in selected:
            continue
        result[name] = call()
        atomic_write_json(output / "progress.json", result)
        print(json.dumps({name: result[name]}, ensure_ascii=False), flush=True)
    if identity != {str(path.relative_to(ROOT)): file_sha256(path) for path in CODE}:
        raise ValueError("measured implementation changed during the probe")
    result["status"] = ("accepted_synthetic_stage_oracles_not_live_readiness" if result["synthetic_sources"]
                        else "accepted_captured_source_stage_oracles_not_daily_or_live_readiness")
    result["rss_process_peak_bytes"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
    atomic_write_json(output / "receipt.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sessions", type=int, default=320)
    parser.add_argument("--symbols", type=int, default=128)
    parser.add_argument("--features", type=int, default=32)
    parser.add_argument("--workers", type=int, default=8, help="BASE batch workers; same count on both variants")
    parser.add_argument("--stages", nargs="+", help="Explicit diagnostic subset; omitted means the whole benchmark")
    parser.add_argument("--profile", action="store_true", help="Retain scoped CPU profile; profiled timings are diagnostic")
    parser.add_argument("--real-public-root", type=Path, help="Explicit read-only captured-source merge probe, not a production publication")
    parser.add_argument("--factorized-capture-root", type=Path, help="Complete retained real daily-owner capture for a separate cold BASE oracle")
    parser.add_argument("--daily-owner-receipt", type=Path, help="Pair a completed full real daily owner with independent cold BASE evidence")
    parser.add_argument("--independent-base-receipt", type=Path)
    parser.add_argument("--wait-for-daily-run", type=Path, help="Only observe this canonical benchmark run before pairing; never start/restart it")
    args = parser.parse_args()
    if args.daily_owner_receipt or args.independent_base_receipt or args.wait_for_daily_run:
        if (not args.daily_owner_receipt or not args.independent_base_receipt or args.profile
                or args.stages or args.factorized_capture_root or args.real_public_root):
            parser.error("pair verification requires both receipts and cannot mix benchmark stages")
        if args.wait_for_daily_run:
            expected_verifier_sha = file_sha256(Path(__file__))
            wait_for_daily_run(args.wait_for_daily_run)
        else:
            expected_verifier_sha = None
        result = verify_real_factorized_pair(args.output, args.daily_owner_receipt, args.independent_base_receipt,
                                            expected_verifier_sha=expected_verifier_sha)
        print(json.dumps({"status": result["status"], "receipt": str(args.output / "receipt.json")}))
        return
    kwargs = dict(sessions=args.sessions, symbols=args.symbols, features=args.features, stages=args.stages,
                  workers=args.workers, real_public_root=args.real_public_root,
                  factorized_capture_root=args.factorized_capture_root)
    if args.profile:
        profiler = cProfile.Profile()
        result = profiler.runcall(benchmark, args.output, **kwargs)
        profiler.dump_stats(args.output / "diagnostic_cpu.pstats")
        pstats.Stats(profiler).strip_dirs().sort_stats("cumulative").print_stats(35)
    else:
        result = benchmark(args.output, **kwargs)
    print(json.dumps({"status": result["status"], "receipt": str(args.output / "receipt.json")}))


if __name__ == "__main__":
    main()
