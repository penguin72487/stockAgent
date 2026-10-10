#!/usr/bin/env python3
"""Build a cadence-aware, feature-only external view on the training node.

Reuses source release clocks, canonical normalizers/admission and the existing
trainer. Executor prices/rules remain the pinned ordinary sources, not model X.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, UTC, timedelta
import json
from pathlib import Path
import sys
import time

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from scripts.prepare_tw_day_trade_feature_catalog import sha256, write_csv
from scripts.build_tw_release_schedule_dataset import normalize_source
from scripts.stage_tw_public_research_release import _required_formal_members, _stage_copy
from stockagent.config import load_config
from stockagent.storage_layout import admit_output, node_root
from stockagent.data.tw_day_trade_mixed_frequency import CONTRACT, PRIVATE_USE, rule_from_spec, storage_lookup, source_clock_lookup
from stockagent.data.tw_public_release_schedule import align_observation_frame, prepare_observation_keys, max_carry_days, ALIGNMENT_CONTRACT, CONTRACT as RELEASE_SCHEDULE_CONTRACT
from stockagent.training.dataset import execution_feature_lag


def verify_sources(root: Path) -> dict:
    manifest = json.loads((root / "source_manifest.json").read_text())
    if (manifest.get("contract") != CONTRACT or manifest.get("source_only") is not True
            or manifest.get("private_delivery_authorized") is not True
            or manifest.get("use_restriction") != PRIVATE_USE):
        raise ValueError("source contract/private use mismatch")
    for relative, proof in manifest["files"].items():
        path = root / relative
        if not path.resolve().is_relative_to(root.resolve()) or sha256(path) != proof["sha256"]:
            raise ValueError(f"source hash mismatch: {relative}")
    from stockagent.data.tw_public_cross_source_fill import verify_staged_source_repairs,verify_staged_snapshot_retention
    verify_staged_source_repairs(root, manifest)
    verify_staged_snapshot_retention(root,manifest)
    return manifest


def attach_formal_companions(source: Path, out: Path, manifest: dict | None = None) -> dict:
    """Keep canonical corporate-action discovery next to a separate model X.

    Copy only the small formal action/receipt members, not the public feature
    matrix. Symlinks would break the canonical root-bound raw receipt guard.
    An existing member must match exactly; never overwrite divergent evidence.
    This also repairs a completed feature-only view without rebuilding its X.
    """
    manifest = verify_sources(source) if manifest is None else manifest
    if not out.is_dir():
        raise FileNotFoundError(out)
    summary = json.loads((source / "tw_corporate_action_entitlements.summary.json").read_text())
    members = _required_formal_members(summary, source)[1:]
    proofs = {}
    for relative in members:
        expected = manifest["files"].get(relative, {}).get("sha256")
        original = source / relative
        target = out / relative
        if not expected or sha256(original) != expected:
            raise ValueError(f"formal companion source mismatch: {relative}")
        if target.exists() or target.is_symlink():
            if target.is_symlink() or sha256(target) != expected:
                raise ValueError(f"existing formal companion mismatch: {relative}")
        else:
            _stage_copy(original, target, expected)
        proofs[relative] = {"sha256": expected, "bytes": target.stat().st_size}
    receipt = {"source_manifest_sha256": sha256(source / "source_manifest.json"),
               "formal_action_members": proofs, "status": "exact_companions_attached",
               "builder_sha256": sha256(Path(__file__)),
               "model_matrix_changed": False,
               "verified_at_utc": datetime.now(UTC).isoformat()}
    path = out / "formal_companion_receipt.json"
    if path.exists():
        existing = json.loads(path.read_text())
        if (existing.get("source_manifest_sha256") != receipt["source_manifest_sha256"]
                or existing.get("formal_action_members") != proofs):
            raise ValueError("existing formal companion receipt mismatch")
        return existing
    atomic_write_json(path, receipt)
    return receipt


def prepare(*, source: Path, out: Path, base_config: Path, minute_root: Path,
            snapshot_id: str, chunk_sessions: int = 16) -> dict:
    out = admit_output(out, "prepared")
    started = time.perf_counter()
    if out.exists():
        raise FileExistsError("use a fresh versioned training view; do not overwrite an accepted build")
    config = load_config(base_config)
    if execution_feature_lag(config.trading.execution_mode) != 1 or config.trading.execution_mode != "tw_day_trade":
        raise ValueError("mixed preopen view requires canonical lag-one tw_day_trade")
    if not 1 <= chunk_sessions <= 64:
        raise ValueError("bounded chunk_sessions must be 1..64")
    manifest = verify_sources(source)
    source_manifest_sha = sha256(source / "source_manifest.json")
    out.mkdir(parents=True)
    # Synthetic contract tests may omit the entire action archive. A real
    # pinned TW source contains these companions and must never drop them.
    companion_receipt = (attach_formal_companions(source, out, manifest)
        if "tw_corporate_action_entitlements.summary.json" in manifest["files"] else None)
    observations = out / "observations"
    observations.mkdir()
    end = date.fromisoformat(manifest["end_date"])
    start = date(2014, 1, 6)
    calendar = pl.read_parquet(source / "twse_taiex_ohlc.parquet", columns=["date"])
    sessions = sorted(set(calendar["date"].cast(pl.Date).to_list()))
    sessions = [d for d in sessions if date(2013, 1, 1) <= d <= end]
    lookup = storage_lookup(sessions)
    stock_paths = sorted((source / "stocks").glob("*_features.parquet"))
    raw_keys = pl.scan_parquet(stock_paths, include_file_paths="_path").select(
        pl.col("date").cast(pl.Date),
        pl.col("_path").str.extract(r"/([^/]+)_features\.parquet$", 1).alias("symbol"),
        "lifecycle_episode_id").filter(pl.col("date") <= end).collect(engine="streaming")
    if raw_keys.select(pl.struct("date", "symbol").is_duplicated().any()).item():
        raise ValueError("stock source has duplicate session/security keys")
    beginnings = raw_keys.group_by("symbol", "lifecycle_episode_id").agg(pl.col("date").min().alias("lifecycle_start"))
    keys = raw_keys.filter(pl.col("date") >= start).join(beginnings, on=["symbol", "lifecycle_episode_id"], validate="m:1").drop("lifecycle_episode_id")
    del raw_keys
    symbols = sorted(keys["symbol"].unique())
    uploads = None
    if (source / "finlab/known_uploads.parquet").is_file():
        uploads = pl.read_parquet(source / "finlab/known_uploads.parquet").unpivot(
            index="source_index", variable_name="symbol", value_name="known_upload_on")
        uploads = uploads.with_columns(pl.col("known_upload_on").cast(pl.Date, strict=True)).drop_nulls()
    public = source / "features/tw_public_stock_daily.parquet"
    definitions, excluded, quality = [], [], []
    for spec in manifest["feature_specs"]:
        name, rule = spec["feature"], rule_from_spec(spec)
        unmapped_periods = 0
        if spec["source"] == "FinLab":
            table, release_lookup = normalize_source(source / spec["path"], spec["dataset"], sessions, symbols,
                                        uploads, quality_rows=quality)
            source_periods = pq.ParquetFile(source/spec["path"]).metadata.num_rows
            unmapped_periods = source_periods - release_lookup["date"].drop_nulls().len()
        else:
            column = spec["source_column"]
            table = (pl.scan_parquet(public).select("date", "symbol", column)
                     .filter(pl.col("date").is_between(sessions[0], end) & pl.col(column).is_not_null())
                     .rename({"date": "source_date", column: name}).collect(engine="streaming"))
            table = table.join(source_clock_lookup(sessions, spec["clock"]), on="source_date", how="inner", validate="m:1").drop("source_date")
            table = table.with_columns(pl.when(pl.col(name).is_finite()).then(pl.col(name)).otherwise(None).alias(name))
        if not table.height or table[name].drop_nulls().n_unique() < 2:
            excluded.append({"feature": name, "reason": "no_variable_observation_in_selected_horizon", "observations": table.height})
            continue
        # Persist only normalized observed source facts, never a dense panel.
        target = observations / f"{name}.parquet"
        # Date-ordered small row groups make later bounded date windows real
        # predicate pruning, rather than rereading a symbol-major whole history
        # for every chunk. This changes storage, not observations or clocks.
        table.select("date", "symbol", name).sort("date", "symbol").write_parquet(
            target, compression="zstd", statistics=True, row_group_size=16_384)
        definitions.append({**spec, "observations": table.height, "first_usable": str(table["date"].min()),
            "last_usable": str(table["date"].max()), "unmapped_release_periods": unmapped_periods,
            "observation_sha256": sha256(target)})
        print(f"[mixed-view] normalize {name}: {table.height:,}", flush=True)
    if not definitions:
        raise ValueError("no admitted observations")
    names = [d["feature"] for d in definitions]
    channels = [channel for name in names for channel in (name, name+"__available", name+"__age_days", name+"__updated")]
    if len(channels) != len(set(channels)):
        raise ValueError("duplicate mixed feature schema")
    target = out / "model_inputs.parquet"
    rule_names = [n for n in pq.read_schema(public).names if n.startswith("_twpub_")]
    schema = pa.schema([pa.field("date", pa.date32()), pa.field("symbol", pa.string()),
                        *[pa.field(n, pa.float32()) for n in channels],
                        *[pa.field(n, pa.float64()) for n in rule_names]])
    counts = Counter()
    rows = 0
    external_rows = 0
    annual = []
    with pq.ParquetWriter(target, schema, compression="zstd") as writer:
        for year in sorted(keys["date"].dt.year().unique()):
            year_dates = [d for d in sessions if d.year == year and start <= d <= end]
            # Read the executor projection once per year, including the prior
            # stored session at the year boundary. It is never an X feature.
            first_stored = lookup.filter(pl.col("decision_date") == year_dates[0])["date"].item()
            year_rules = pl.scan_parquet(public).select("date", "symbol", *rule_names).filter(
                pl.col("date").is_between(first_stored, year_dates[-1])).collect(engine="streaming")
            for begin in range(0, len(year_dates), chunk_sessions):
                days = year_dates[begin:begin+chunk_sessions]
                if not days:
                    continue
                chunk = keys.filter(pl.col("date").is_in(days)).sort("date", "symbol")
                prepared_keys = prepare_observation_keys(chunk)
                values = []
                for spec in definitions:
                    name, rule = spec["feature"], rule_from_spec(spec)
                    observed = pl.scan_parquet(observations / f"{name}.parquet").filter(
                        pl.col("date").is_between(days[0]-timedelta(days=max_carry_days(rule)), days[-1])).collect(engine="streaming")
                    result = align_observation_frame(prepared_keys, observed, name, rule)
                    counts[name] += result[name+"__available"].sum()
                    values.extend(result[n] for n in (name, name+"__available", name+"__age_days", name+"__updated"))
                frame = chunk.select("date", "symbol").with_columns(values).rename({"date": "decision_date"})
                frame = frame.join(lookup, on="decision_date", how="inner", validate="m:1").drop("decision_date").select("date", "symbol", *channels)
                if frame.height != chunk.height:
                    raise ValueError("decision/session storage mapping changed row count")
                # Rule facts keep their original date, precision and clock.
                # Do not move execution eligibility together with model X.
                stored_dates = frame["date"].unique()
                rules = year_rules.filter(pl.col("date").is_in(stored_dates.implode()))
                frame = frame.join(rules, on=["date", "symbol"], how="full", coalesce=True, validate="1:1").select("date", "symbol", *channels, *rule_names)
                writer.write_table(frame.to_arrow().cast(schema), row_group_size=16_384)
                rows += frame.height
                external_rows += frame.height
            annual.append({"year": int(year), "stock_rows": keys.filter(pl.col("date").dt.year()==year).height})
            print(f"[mixed-view] committed {year}, cumulative rows={rows:,}", flush=True)
            del year_rules
        # The final completed source session is an executor row even though
        # its model window reads the previous row. Retain market-calendar rules.
        tail = pl.scan_parquet(public).select("date", "symbol", *rule_names).filter(
            pl.col("date") == keys["date"].max()).collect(engine="streaming").with_columns(
                pl.lit(None, dtype=pl.Float32).alias(n) for n in channels)
        writer.write_table(tail.select("date", "symbol", *channels, *rule_names).to_arrow().cast(schema), row_group_size=16_384)
        external_rows += tail.height
    # The external view owns model facts only; the separate canonical archive
    # owns day-trade rules and physical executor dependencies.
    generated = {
        "base_config": str(base_config.resolve()), "experiment_name": "tw-day-trade-mixed-frequency-20261004-v1",
        "runner": {"output_dir": str(node_root(ROOT)/"artifacts/markets"/out.parent.name/"training"), "resume": False, "post_train_infer": False},
        "data": {"parquet_root": str((source/"stocks").resolve()),
                 "tw_public_feature_path": str(target.resolve()),
                 "day_trade_physical_public_feature_path": str(public.resolve()),
                 "panel_cache_root": str((out/"panel_cache").resolve()),
                 "day_trade_minute_execution_root": str(minute_root.resolve()),
                 "day_trade_minute_execution_cache_dir": str(node_root(ROOT)/"artifacts/cache"/out.parent.name/"runtime-cache/physical-source"),
                 "feature_include": ["open_raw", "high_raw", "low_raw", "close_raw", "trading_volume_raw", *channels, "next_session_open_gap_logret"],
                 "feature_availability_indicators": [], "feature_shift_next_session": [], "feature_zero_fill": []},
        "training": {"pretrained_initialization_root": None,
                     "cache_train_tensors_on_gpu": False, "cache_eval_tensors_on_gpu": False,
                     "financial_transformer": {"feature_bottleneck_dim": 0}},
    }
    config_path = out / "training.yaml"
    config_path.write_text(yaml.safe_dump(generated, allow_unicode=True, sort_keys=False))
    resolved = load_config(config_path)
    if execution_feature_lag(resolved.trading.execution_mode) != 1 or resolved.runner.resume:
        raise ValueError("generated training config changed lag/fresh experiment semantics")
    if sha256(source / "source_manifest.json") != source_manifest_sha:
        raise ValueError("source manifest changed during build")
    verify_sources(source)
    write_csv(out/"feature_dictionary.csv", [{k:(json.dumps(v, ensure_ascii=False) if isinstance(v,dict) else v) for k,v in d.items()} for d in definitions])
    write_csv(out/"excluded_features.csv", excluded, ["feature", "reason", "observations"])
    write_csv(out/"annual_rows.csv", annual)
    atomic_write_json(out/"quality_masks.json", quality)
    result = {"contract": CONTRACT, "alignment_contract": ALIGNMENT_CONTRACT,
        "publication_schedule_contract": RELEASE_SCHEDULE_CONTRACT,
        "source_snapshot_id": snapshot_id, "source_manifest_sha256": source_manifest_sha,
        "research_only": True, "historical_point_in_time": False, "live_eligible": False,
        "use_restriction": PRIVATE_USE, "feature_view_ready": True, "execution_preflight_passed": False,
        "gpu_training_verified": False, "training_ready": False,
        "rows": external_rows, "decision_feature_rows": keys.height, "value_features": len(names), "model_channels": len(channels)+6,
        "first_decision_session": str(keys["date"].min()), "last_decision_session": str(keys["date"].max()),
        "storage_clock": "ready_session_t_stored_on_t_minus_1_for_canonical_execution_feature_lag_one",
        "feature_available_cells": dict(counts), "quality_barriers": len(quality),
        "matrix_sha256": sha256(target), "config_sha256": sha256(config_path),
        "builder_sha256": sha256(Path(__file__)), "build_wall_seconds": time.perf_counter()-started,
        "formal_companion_receipt_sha256": (sha256(out/"formal_companion_receipt.json")
            if companion_receipt else None),
        "created_at_utc": datetime.now(UTC).isoformat(),
        "limitations": ["current_provider_revision_not_original_historical_vintage",
                        "one_calendar_day_proxy_safety_is_not_proven_original_publication_time",
                        "no_missing_execution_price_imputation", "canonical_check_data_only_and_two_GPU_acceptance_still_required"]}
    atomic_write_json(out/"dataset_manifest.json", result)
    return {k:v for k,v in result.items() if k != "feature_available_cells"}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--base-config", type=Path, required=True)
    parser.add_argument("--minute-root", type=Path, required=True)
    parser.add_argument("--chunk-sessions", type=int, default=16)
    parser.add_argument("--attach-formal-companions-only", action="store_true",
                        help="attach exact formal receipts to an existing feature view without rebuilding its X")
    args=parser.parse_args()
    args.output_root = admit_output(args.output_root, "prepared")
    if Path("/etc/hostname").read_text().strip() == "penguin":
        parser.error("build training matrices on the remote training node, not penguin")
    if args.attach_formal_companions_only:
        view = json.loads((args.output_root / "dataset_manifest.json").read_text())
        if (view["source_manifest_sha256"] != sha256(args.source_root / "source_manifest.json")
                or view["source_snapshot_id"] != args.snapshot_id
                or sha256(args.output_root / "model_inputs.parquet") != view["matrix_sha256"]):
            raise ValueError("existing view does not bind the exact source and unchanged X")
        print(json.dumps(attach_formal_companions(args.source_root, args.output_root), ensure_ascii=False))
        return
    print(json.dumps(prepare(source=args.source_root, out=args.output_root, base_config=args.base_config,
        minute_root=args.minute_root, snapshot_id=args.snapshot_id, chunk_sessions=args.chunk_sessions), ensure_ascii=False))


if __name__ == "__main__":
    main()
