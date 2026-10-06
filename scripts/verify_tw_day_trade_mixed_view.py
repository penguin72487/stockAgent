#!/usr/bin/env python3
"""Audit a completed mixed-frequency X without constructing the dense panel.

This is columnar feature acceptance, NOT execution, CUDA/DDP or PIT acceptance.
Reuse the exact view manifest and dated lifetime rules, and retain every failure.
"""
from __future__ import annotations

import argparse
from datetime import datetime, UTC
import json
from pathlib import Path
import sys
import time

import polars as pl
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from scripts.prepare_tw_day_trade_feature_catalog import sha256
from scripts.report_tw_day_trade_mixed_admission import read_csv
from stockagent.data.tw_day_trade_mixed_frequency import CONTRACT, rule_from_spec
from stockagent.data.tw_public_release_schedule import max_carry_days


def channel_checks(feature: str, carry_days: int) -> list[pl.Expr]:
    value = pl.col(feature)
    available = pl.col(feature+"__available")
    age = pl.col(feature+"__age_days")
    updated = pl.col(feature+"__updated")
    # Full-join executor-only rows have four NULL model channels. Observed
    # NULL barriers may have updated=1, available=0 and no value/age.
    invalid = (
        (available.is_null() & (value.is_not_null() | age.is_not_null() | updated.is_not_null()))
        | (available.is_not_null() & (~available.is_in([0., 1.]) | updated.is_null() | ~updated.is_in([0., 1.])))
        | ((available == 0.) & (value.is_not_null() | age.is_not_null()))
        | ((available == 1.) & (value.is_null() | ~value.is_finite()
                               | age.is_null() | ~age.is_finite() | (age < 0.) | (age > carry_days)))
        | ((updated == 1.) & (available == 1.) & (age != 0.))
    ).fill_null(False)
    return [invalid.sum().alias(feature+"__invalid_cells"), available.sum().alias(feature+"__available_sum")]


def verify(view: Path, output: Path) -> dict:
    started = time.perf_counter()
    if output.exists():
        raise FileExistsError("keep previous acceptance evidence; choose a fresh output")
    manifest_path = view/"dataset_manifest.json"
    manifest_sha = sha256(manifest_path)
    proof = json.loads(manifest_path.read_text())
    if proof.get("contract") != CONTRACT or proof.get("feature_view_ready") is not True:
        raise ValueError("unaccepted mixed-frequency view contract")
    matrix = view/"model_inputs.parquet"
    before = matrix.stat()
    if sha256(matrix) != proof["matrix_sha256"]:
        raise ValueError("matrix differs from completed view receipt")
    if sha256(view/"training.yaml") != proof["config_sha256"]:
        raise ValueError("config differs from completed view receipt")
    definitions = read_csv(view/"feature_dictionary.csv")
    names = [r["feature"] for r in definitions]
    if (len(names) != len(set(names)) or len(names) != proof["value_features"]
            or set(names) != set(proof["feature_available_cells"])):
        raise ValueError("feature dictionary/count contract mismatch")
    schema = pq.read_schema(matrix)
    channels = [n+s for n in names for s in ("", "__available", "__age_days", "__updated")]
    if any(str(schema.field(n).type) != "float" for n in channels):
        raise ValueError("ML channels must retain Float32 storage")
    if any(str(f.type) != "double" for f in schema if f.name.startswith("_twpub_")):
        raise ValueError("executor rules must retain Float64 precision")
    metadata = pq.ParquetFile(matrix).metadata
    if metadata.num_rows != proof["rows"]:
        raise ValueError("matrix row count differs from receipt")
    checks = []
    for definition in definitions:
        spec = {"rule": json.loads(definition["rule"])}
        checks.extend(channel_checks(definition["feature"], max_carry_days(rule_from_spec(spec))))
    result = pl.scan_parquet(matrix).select(checks).collect(engine="streaming").row(0, named=True)
    invalid = {n: int(result[n+"__invalid_cells"]) for n in names if result[n+"__invalid_cells"]}
    counts = {n: int(result[n+"__available_sum"] or 0) for n in names}
    if invalid or counts != proof["feature_available_cells"]:
        raise ValueError(f"channel invariant or availability mismatch: invalid={invalid}")
    keys = pl.scan_parquet(matrix).select("date", "symbol").collect(engine="streaming")
    if keys.select(pl.any_horizontal(pl.all().is_null()).any()).item():
        raise ValueError("null matrix economic key")
    if keys.select(pl.struct("date", "symbol").is_duplicated().any()).item():
        raise ValueError("duplicate matrix economic key")
    if keys["date"].max().isoformat() != proof["last_decision_session"]:
        raise ValueError("executor tail must preserve the final completed source session")
    after = matrix.stat()
    if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        raise ValueError("matrix changed during feature audit")
    if sha256(manifest_path) != manifest_sha or sha256(view/"training.yaml") != proof["config_sha256"]:
        raise ValueError("view manifest changed during audit")
    receipt = {"status": "columnar_feature_invariants_accepted", "contract": CONTRACT,
               "source_snapshot_id": proof["source_snapshot_id"],
               "source_manifest_sha256": proof["source_manifest_sha256"],
               "matrix_sha256": proof["matrix_sha256"], "config_sha256": proof["config_sha256"],
               "rows": keys.height, "value_features": len(names), "model_channels": proof["model_channels"],
               "first_storage_session": str(keys["date"].min()), "last_storage_session": str(keys["date"].max()),
               "feature_available_cells_verified": True, "economic_keys_unique": True,
               "float32_model_float64_rules_verified": True, "finite_value_lifetime_and_mask_invariants_verified": True,
               "full_training_data_gate_passed": False, "gpu_ddp_verified": False,
               "training_ready": False, "historical_point_in_time": False,
               "verifier_sha256": sha256(Path(__file__)), "wall_seconds": time.perf_counter()-started,
               "verified_at_utc": datetime.now(UTC).isoformat()}
    atomic_write_json(output, receipt)
    return receipt


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--view-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args=parser.parse_args()
    print(json.dumps(verify(args.view_root, args.output), ensure_ascii=False))


if __name__ == "__main__":
    main()
