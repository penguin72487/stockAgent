#!/usr/bin/env python3
"""Publish bounded, immutable factorized inputs for the selected paper ABI.

Uses the original trained dictionary and the same release normalizers/causal
TTL, NULL and issuer lifecycle sweep as training. Private raw data never leaves
the local source tree. This is preparation, not training or order submission.
"""
from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import uuid

import numpy as np
import polars as pl
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from scripts.build_tw_release_schedule_dataset import normalize_source
from scripts.prepare_tw_day_trade_factorized_panel import (
    AnnualObservationEvents, annual_preparation_identity, causal_state_blocks, OBS_SCHEMA,
)
from stockagent.config import load_config
from stockagent.data.factorized_panel import VALUE_ONLY_CONTRACT, file_sha256, write_array_block, verify_factorized_members
from stockagent.data.finlab_acquisition_contract import read_receipt_bound_source
from stockagent.data.annual_cache_admission import MemoryAdmittedObservationCache
from stockagent.data.panel import build_panel
from stockagent.data.tw_day_trade_mixed_frequency import source_clock_lookup, rule_from_spec
from stockagent.data.tw_public_features import _exchange_session_dates
from stockagent.live.tw_public_source_guard import live_tw_public_source_read_guard
from stockagent.live.market_status import tw_stock_day_decision, TW_HOLIDAY_SCHEDULE_NAME
from stockagent.storage_layout import admit_output
from stockagent.training.checkpoint_contract import subset_panel_symbols
from train import _build_panel_kwargs


class AdmittedAnnualObservationEvents(AnnualObservationEvents):
    """Same causal stream, with resource policy only on actual cache misses."""

    def __init__(self, paths, *, cache_root=None, seed_paths=()):
        # Canonical chronology/period parsing stays with the parent. The
        # protected cache dependency is its deliberate specialization point;
        # no source precedence, causal sweep or content key is copied here.
        super().__init__(paths)
        if cache_root is not None:
            self._cache = MemoryAdmittedObservationCache(cache_root, seed_sources=seed_paths)


def publish_link(root: Path, target: Path) -> None:
    link = root / "current"
    if link.exists() and not link.is_symlink():
        raise ValueError("refusing to replace a non-symlink projection pointer")
    temporary = root / f".current.{os.getpid()}"
    temporary.symlink_to(target.resolve(), target_is_directory=True)
    os.replace(temporary, link)


def _next_projection_session(public: Path, completed: date):
    """Use the shared official calendar, not tomorrow's eligibility release.

    A scheduled session is a preparation clock only, never proof of an actual
    session, tradability, opening quote or fill. Unknown days fail closed rather
    than being skipped until a convenient weekday is found.
    """
    for offset in range(1, 32):
        candidate = completed + timedelta(days=offset)
        decision = tw_stock_day_decision(candidate, parquet_root=public / "stocks")
        if decision.status == "unknown":
            raise ValueError(f"next projection calendar is unknown: {candidate}: {decision.reason}")
        if decision.is_session:
            return candidate, {"status": decision.status, "reason": decision.reason,
                               "execution_or_eligibility_proof": False}
    raise ValueError("official calendar has no next scheduled session within 31 days")


def _native_context_identity(native_source: Path, manifest: dict) -> dict:
    """Publication bounds/known uploads are inputs, not cache implementation."""
    paths = {"source_manifest.json", "finlab/known_uploads.parquet"}
    bounds = manifest.get("source_repairs", {}).get("publication_lower_bounds")
    if bounds:
        candidate = native_source / bounds
        if not candidate.resolve(strict=True).is_relative_to(native_source.resolve()):
            raise ValueError("publication bound escaped its original source root")
        paths.add(bounds)
    return {name: file_sha256(native_source / name) if (native_source / name).is_file() else None
            for name in sorted(paths)}


def _verify_preparation_inputs(config_path, native_source, source_manifest, identity):
    if (file_sha256(config_path) != identity["config_sha"]
            or _native_context_identity(native_source, source_manifest) != identity["native_context"]):
        raise ValueError("configuration/publication context changed during projection; last good generation retained")


def prepare(*, config_path: Path, parent_path: Path, native_source: Path,
            public: Path, finlab: Path, output: Path, start_date: str,
            annual_cache_root: Path | None = None) -> dict:
    output = admit_output(output, "prepared")
    output.mkdir(parents=True, exist_ok=True)
    with (output / "prepare.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        with live_tw_public_source_read_guard(public / "stocks"):
            return _prepare(config_path=config_path, parent_path=parent_path,
                            native_source=native_source, public=public, finlab=finlab,
                            output=output, start_date=start_date, annual_cache_root=annual_cache_root)


def _prepare(*, config_path, parent_path, native_source, public, finlab, output, start_date,
             annual_cache_root=None):
    started = time.perf_counter()
    parent_sha = file_sha256(parent_path)
    parent = json.loads(parent_path.read_text())
    if (parent.get("contract") != VALUE_ONLY_CONTRACT or parent.get("status") != "complete"
            or parent.get("feature_lag") != 1 or parent.get("research_only") is not True
            or parent.get("historical_point_in_time") is not False):
        raise ValueError("a fixed original value-only research manifest is required")
    dictionary_path = parent_path.parent / "feature_dictionary.json"
    if file_sha256(dictionary_path) != parent["feature_dictionary_sha256"]:
        raise ValueError("original trained feature dictionary changed")
    definitions = json.loads(dictionary_path.read_text())["features"]
    by_name = {d["feature"]: d for d in definitions}
    individual = [by_name[n] for n in parent["individual_channels"]]
    common = [by_name[n] for n in parent["common_channels"]]
    all_defs = individual + common
    sessions = _exchange_session_dates(public)["_session_date"].to_list()
    summary_path = public / "features/tw_public_stock_daily.summary.json"
    public_summary = json.loads(summary_path.read_text())
    completed = date.fromisoformat(public_summary["requested_end_date"])
    decision, decision_proof = _next_projection_session(public, completed)
    sessions = sorted(set(day for day in sessions if day <= completed) | {decision})
    calendar_inputs = {name: file_sha256(public / name) for name in (
        TW_HOLIDAY_SCHEDULE_NAME, "twse_taiex_ohlc.parquet", "twse_taiex_ohlc.summary.json")}
    source_manifest_bytes = (native_source / "source_manifest.json").read_bytes()
    source_manifest = json.loads(source_manifest_bytes)
    native_context = _native_context_identity(native_source, source_manifest)
    if native_context["source_manifest.json"] != hashlib.sha256(source_manifest_bytes).hexdigest():
        raise ValueError("native publication manifest changed while capturing projection inputs")
    source_specs = {d.get("dataset"): d for d in source_manifest["feature_specs"] if d.get("source") == "FinLab"}
    captured = {}
    for d in all_defs:
        if d.get("source") != "FinLab":
            continue
        key = d["dataset"]
        spec = source_specs.get(key)
        if spec is None:
            raise ValueError(f"trained FinLab quantity is absent from its source: {key}")
        # The canonical receipt name is independent of the current content hash.
        from stockagent.data.finlab_acquisition_contract import safe_stem
        receipt_path = finlab / "receipts" / f"{safe_stem(key)}.json"
        original, receipt = read_receipt_bound_source(finlab, key, receipt_path)
        captured[key] = (original, receipt)
    identity = {
        "parent": parent_sha, "start_date": start_date, "completed": str(completed),
        "decision": str(decision), "public_sha": public_summary["output_receipt"]["sha256"],
        "decision_calendar": {**decision_proof, "input_sha256": calendar_inputs},
        "finlab": {key: receipt["sha256"] for key, (_, receipt) in captured.items()},
        "builder_sha": file_sha256(Path(__file__)),
        "sweep_sha": file_sha256(ROOT / "scripts/prepare_tw_day_trade_factorized_panel.py"),
        "annual_preparation": annual_preparation_identity(),
        "writer_sha": file_sha256(Path(write_array_block.__code__.co_filename)),
        "config_sha": file_sha256(config_path), "native_context": native_context,
    }
    generation = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    target = output / "generations" / generation
    if (target / "factorized_manifest.json").is_file():
        manifest = json.loads((target / "factorized_manifest.json").read_text())
        verify_factorized_members(target, manifest)
        _verify_preparation_inputs(config_path, native_source, source_manifest, identity)
        publish_link(output, target)
        return {"status": "already_current", "manifest": str(target / "factorized_manifest.json"),
                "decision_session": str(decision), "wall_seconds": time.perf_counter()-started}
    if target.exists():
        raise ValueError(f"invalid committed generation retained for audit: {target}")
    # Partial attempts never occupy the committed identity or poison retries.
    # Preserve failed attempt bytes; only a fully verified generation is renamed.
    out = target.parent / f".{generation}.attempt.{uuid.uuid4().hex}"
    out.mkdir(parents=True)
    config = load_config(config_path)
    data = replace(config.data, parquet_root=str(public / "stocks"),
                   tw_public_feature_path=str(public / "features/tw_public_stock_daily.parquet"),
                   day_trade_physical_public_feature_path=str(public / "features/tw_public_stock_daily.parquet"),
                   panel_cache_root=str(ROOT / "artifacts/cache/tw_factorized_live/base"),
                   panel_start_date=start_date)
    config = replace(config, data=data)
    panel = build_panel(data.parquet_root, **_build_panel_kwargs(config))
    panel = subset_panel_symbols(panel, parent["symbols"], allow_missing_masked=True)
    dates = [date.fromisoformat(str(d)[:10]) for d in panel.dates]
    if dates[-1] != completed or panel.feature_names != parent["base_feature_names"]:
        raise ValueError("live executable base/calendar does not match the selected trained ABI")
    horizon = max(max(rule_from_spec(d).carry_days, rule_from_spec(d).older_carry_days) for d in all_defs)
    seed_end = min(decision, date.fromisoformat(parent["dates"][-1]))
    needed_years = set(range((dates[0]-timedelta(days=horizon)).year, seed_end.year+1))
    seed_paths = []
    for proof in parent["annual_events"]:
        if int(Path(proof["path"]).stem) not in needed_years:
            continue
        local = parent_path.parent / Path(proof["path"]).name
        if not local.is_file() or file_sha256(local) != proof["sha256"]:
            raise ValueError(f"required fixed seed events are missing or changed: {local}")
        seed_paths.append(local)
    if {int(path.stem) for path in seed_paths} != needed_years:
        raise ValueError("source events do not cover the full TTL initialization horizon")
    delta_path = out / "delta_events.parquet"
    cutoff = date.fromisoformat(parent["dates"][-1])
    quality = []
    uploads_path = native_source / "finlab/known_uploads.parquet"
    uploads = None
    if uploads_path.is_file():
        uploads = pl.read_parquet(uploads_path).unpivot(index="source_index", variable_name="symbol", value_name="known_upload_on").with_columns(pl.col("known_upload_on").cast(pl.Date, strict=True)).drop_nulls()
    bounds_path = source_manifest.get("source_repairs", {}).get("publication_lower_bounds")
    bounds = pl.read_parquet(native_source / bounds_path) if bounds_path else None
    writer = None
    delta_rows = 0
    try:
        for index, d in enumerate(all_defs):
            name = d["feature"]
            if d.get("source") == "FinLab":
                original, receipt = captured[d["dataset"]]
                table, _ = normalize_source(original, d["dataset"], sessions, panel.symbols, uploads,
                                            publication_bounds=bounds, quality_rows=quality)
                period = pl.col("source_index").cast(pl.String) if "source_index" in table.columns else pl.col("date").cast(pl.String)
                table = table.select("date", "symbol", pl.lit(name).alias("feature"), pl.col(name).alias("value"), period.alias("period"))
                if rule_from_spec(d).kind == "quarter":
                    table = table.with_columns(pl.col("period").str.replace_all("-", "").alias("period"))
                if rule_from_spec(d).kind == "quarter_date":
                    stamp = pl.col("period").str.slice(0, 10).str.to_date()
                    table = table.with_columns(pl.concat_str(
                        (stamp.dt.year()-(stamp.dt.month()==3).cast(pl.Int32)).cast(pl.String),
                        pl.lit("Q"), stamp.dt.month().replace_strict({3:4,5:1,8:2,11:3}).cast(pl.String)).alias("period"))
                if file_sha256(original) != receipt["sha256"]:
                    raise ValueError(f"captured FinLab source changed during projection: {d['dataset']}")
            elif d.get("source") == "tw-public" and d.get("source_column"):
                table = pl.scan_parquet(public / "features/tw_public_stock_daily.parquet").select("date", "symbol", pl.col(d["source_column"]).alias("value")).filter(pl.col("date") >= cutoff).collect(engine="streaming")
                if rule_from_spec(d).scope == "market":
                    from stockagent.data.tw_native_panel_features import collapse_conflicting_observations
                    table, conflicts = collapse_conflicting_observations(table.filter(pl.col("value").is_finite()), ["date"])
                    if conflicts:
                        raise ValueError("shared public quantity differs across security rows")
                    table = table.with_columns(pl.lit("*").alias("symbol"))
                table = table.join(source_clock_lookup(sessions, d["clock"]), left_on="date", right_on="source_date", how="inner", suffix="_ready", validate="m:1").select(pl.col("date_ready").alias("date"), "symbol", pl.lit(name).alias("feature"), "value", pl.col("date").cast(pl.String).alias("period"))
            else:
                continue  # Existing low-frequency facts obey their original TTL, not invented daily updates.
            table = table.filter(pl.col("date").is_between(cutoff+timedelta(days=1), decision))
            if table.is_empty():
                continue
            table = table.select(pl.col(c).cast(dtype, strict=True) for c, dtype in OBS_SCHEMA.items()).with_columns(pl.lit(100000+index, dtype=pl.Int32).alias("_source_part"))
            arrow = table.to_arrow()
            if writer is None:
                writer = pq.ParquetWriter(delta_path, arrow.schema, compression="zstd")
            writer.write_table(arrow, row_group_size=128000)
            delta_rows += table.height
            print(f"[factorized-live] delta {name}: {table.height:,}", flush=True)
    finally:
        if writer is not None:
            writer.close()
    paths = seed_paths + ([delta_path] if writer is not None else [])
    # Native report facts clear omitted concepts, just as in the training sweep.
    groups = pl.DataFrame({"feature": [d["feature"] for d in all_defs],
                           "report_group": [d.get("report_group") or "" for d in all_defs]})
    resets = pl.scan_parquet(paths).select("date", "symbol", "feature", "period").join(groups.lazy(), on="feature", how="inner").filter(pl.col("report_group") != "").select("date", "symbol", "period", "report_group").unique().collect(engine="streaming")
    lifecycle = np.full((len(dates)+1, len(panel.symbols)), 1000000, dtype=np.int32)
    date_map = {day: row for row, day in enumerate(dates)}
    for s, symbol in enumerate(panel.symbols):
        stock = public / "stocks" / f"{symbol}_features.parquet"
        if not stock.is_file():
            continue
        raw = pl.read_parquet(stock, columns=["date", "lifecycle_episode_id"]).with_columns(pl.col("date").cast(pl.Date))
        starts = raw.group_by("lifecycle_episode_id").agg(pl.col("date").min().alias("start"))
        for day, start in raw.join(starts, on="lifecycle_episode_id", validate="m:1").select("date", "start").iter_rows():
            if day in date_map:
                lifecycle[date_map[day], s] = start.toordinal()
    lifecycle[-1] = lifecycle[-2]
    sweep_dates = dates + [decision]
    individual_events = AdmittedAnnualObservationEvents(
        paths, cache_root=annual_cache_root, seed_paths=seed_paths)
    blocks = []
    for begin, values, _ in causal_state_blocks(sweep_dates, panel.symbols, individual, individual_events, lifecycle, resets=resets,
                                              column_blocks=True, max_block_bytes=1024**3, model_channel_policy="value_only"):
        keep = min(values.shape[0], len(dates)-begin)
        if keep <= 0:
            continue
        parts = []
        for symbol_start, columns, part in values.symbol_blocks:
            proof = write_array_block(out / f"individual_{begin:05d}_{symbol_start:05d}.npy.zst", part[:keep], omit_zero_columns=True,
                                      logical_columns=columns, logical_shape=(keep, part.shape[1], values.shape[2]))
            proof["symbol_start"] = symbol_start
            parts.append(proof)
        blocks.append({"start": begin, "shape": [keep, len(panel.symbols), len(individual)], "symbol_blocks": parts})
        print(f"[factorized-live] committed {begin}:{begin+keep}", flush=True)
    common_events = AdmittedAnnualObservationEvents(
        paths, cache_root=annual_cache_root, seed_paths=seed_paths)
    common_parts = [values[:, 0] for _, values, _ in causal_state_blocks(sweep_dates, ["*"], common, common_events, None, model_channel_policy="value_only")]
    shared = np.concatenate(common_parts)[:len(dates)]
    np.save(out / "common.npy", shared, allow_pickle=False)
    os.link(dictionary_path, out / "feature_dictionary.json")
    receipt = {"contract": "factorized_paper_projection_lag1_v1", "status": "verified",
               "decision_session": str(decision), "completed_session": str(completed),
               "parent_manifest_sha256": parent_sha, "source_identity": identity,
               "delta_observations": delta_rows, "raw_source_publication": False,
               "historical_pit_proven": False, "publication_clocks": "original selected research normalization; retained raw capture receipts, no broker fill claim",
               "source_update_owner": str(Path(__file__).resolve()), "built_at_utc": datetime.now(UTC).isoformat()}
    manifest = {**parent, "dates": [str(day) for day in dates], "blocks": blocks,
                "common": {"path": "common.npy", "shape": list(shared.shape), "sha256": file_sha256(out / "common.npy"), "bytes": (out / "common.npy").stat().st_size},
                "observations": [], "annual_events": [], "paper_inference": receipt,
                "host_cache_bytes": 512*1024**2, "max_slab_bytes": 12*1024**3,
                "build_wall_seconds": time.perf_counter()-started}
    verify_factorized_members(out, manifest)
    atomic_write_json(out / "factorized_manifest.json", manifest)
    atomic_write_json(out / "projection_receipt.json", receipt)
    if file_sha256(parent_path) != parent_sha:
        raise ValueError("fixed parent changed during projection")
    current_summary = json.loads(summary_path.read_text())
    if (current_summary.get("requested_end_date") != str(completed)
            or current_summary.get("output_receipt", {}).get("sha256") != identity["public_sha"]
            or file_sha256(public / "features/tw_public_stock_daily.parquet") != identity["public_sha"]):
        raise ValueError("public source changed during projection; last good generation retained")
    if any(file_sha256(public / name) != digest for name, digest in calendar_inputs.items()):
        raise ValueError("official decision calendar changed during projection; last good generation retained")
    _verify_preparation_inputs(config_path, native_source, source_manifest, identity)
    os.replace(out, target)
    publish_link(output, target)
    return {"status": "verified", "manifest": str(target / "factorized_manifest.json"), "decision_session": str(decision),
            "dates": len(dates), "features": parent["logical_model_channels"], "wall_seconds": time.perf_counter()-started,
            "annual_cache": {"individual": individual_events.cache_stats(), "common": common_events.cache_stats()}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--parent-manifest", type=Path, required=True)
    parser.add_argument("--native-source", type=Path, required=True)
    parser.add_argument("--public-root", type=Path, default=Path("/srv/stockagent-live/data_tw_public"))
    parser.add_argument("--finlab-root", type=Path, default=ROOT / "data_finlab")
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--start-date", default="2026-07-01")
    parser.add_argument("--annual-cache-root", type=Path,
                        help="Optional verified derived event cache; raw source and model ABI unchanged")
    args = parser.parse_args()
    print(json.dumps(prepare(config_path=args.config, parent_path=args.parent_manifest,
                            native_source=args.native_source, public=args.public_root, finlab=args.finlab_root,
                            output=args.output_root, start_date=args.start_date,
                            annual_cache_root=args.annual_cache_root), ensure_ascii=False))


if __name__ == "__main__":
    main()
