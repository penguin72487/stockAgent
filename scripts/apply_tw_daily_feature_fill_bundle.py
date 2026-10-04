#!/usr/bin/env python3
"""Apply verified daily patches to a NEW research dataset, never the strict base."""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
from itertools import zip_longest
import json
from pathlib import Path
import sys

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from scripts.prepare_tw_day_trade_feature_catalog import sha256, write_csv
from scripts.curate_tw_day_trade_training_dataset import validate_matrix


def verify_observed_unchanged(before: Path, after: Path, names: list[str]):
    columns = ["date", "symbol", *names]
    left = pq.ParquetFile(before).iter_batches(batch_size=65536, columns=columns)
    right = pq.ParquetFile(after).iter_batches(batch_size=65536, columns=columns)
    changed = {n:0 for n in names}
    for a,b in zip_longest(left, right):
        if a is None or b is None or len(a) != len(b):
            raise ValueError("changed row count/order")
        if not a.column(0).equals(b.column(0)) or not a.column(1).equals(b.column(1)):
            raise ValueError("changed economic keys/order")
        for i,n in enumerate(names, 2):
            av, bv = a.column(i), b.column(i)
            valid = av.is_valid().to_numpy(zero_copy_only=False)
            x,y = av.to_numpy(zero_copy_only=False),bv.to_numpy(zero_copy_only=False)
            if not np.array_equal(x[valid], y[valid]):
                raise ValueError(f"changed existing observed values: {n}")
            changed[n] += int((~valid & bv.is_valid().to_numpy(zero_copy_only=False)).sum())
    return changed


def build(parent: Path, bundle: Path, out: Path):
    if out.exists():
        raise FileExistsError("use a new versioned output directory")
    read = lambda p:json.loads(p.read_text())
    m,s,p = read(parent / "dataset_manifest.json"),read(parent / "feature_columns.json"),read(bundle / "bundle_manifest.json")
    matrix = parent / "model_inputs.parquet"
    if (sha256(matrix) != m["matrix"]["sha256"] or p["contract"] != "tw_daily_feature_missing_only_v1"
        or m["base_matrix_sha256"] != p["base_sha256"]):
        raise ValueError("daily patch not bound to this dataset's base")
    base = Path(m["base_matrix"]).parent
    if sha256(base / "quality_masks.json") != p["quality_masks_sha256"]:
        raise ValueError("explicit source-conflict masks changed")
    names = s["value_features"]
    implementation = sha256(Path(__file__))
    fills = {}
    for n,r in p["fills"].items():
        if n not in s["strict_base_features"] or sha256(Path(r["path"])) != r["sha256"]:
            raise ValueError("unknown or changed patch feature")
        fills[n] = pl.read_parquet(r["path"])
        if fills[n].select(pl.struct("date", "symbol").is_duplicated().any()).item():
            raise ValueError("duplicate patch keys")
    out.mkdir(parents=True)
    target = out / "model_inputs.parquet"
    expected = {n:0 for n in names}
    writer = None
    try:
        years = pl.read_parquet(matrix, columns=["date"])["date"].dt.year().unique().sort()
        for year in years:
            frame = pl.scan_parquet(matrix).filter(pl.col("date").dt.year() == year).collect()
            for n,t in fills.items():
                patch = t.filter(pl.col("date").dt.year() == year).rename({n:"_patch"})
                frame = frame.join(patch, on=["date", "symbol"], how="left", validate="1:1", maintain_order="left")
                if frame.filter(pl.col(n).is_not_null() & pl.col("_patch").is_not_null()).height:
                    raise ValueError("patch attempts to overwrite a primary observation")
                expected[n] += frame["_patch"].is_not_null().sum()
                frame = frame.with_columns(pl.coalesce(n, "_patch").cast(pl.Float32).alias(n)).drop("_patch")
                frame = frame.with_columns(pl.col(n).is_not_null().alias(n + "__available"))
            arrow = frame.to_arrow().cast(pq.ParquetFile(matrix).schema_arrow)
            if writer is None:
                writer = pq.ParquetWriter(target, arrow.schema, compression="zstd")
            writer.write_table(arrow, row_group_size=65536)
            print(f"[apply-daily-fill] {year}: {frame.height:,} rows", flush=True)
        writer.close()
        writer = None
        observed = verify_observed_unchanged(matrix, target, names)
        if observed != expected or sum(expected.values()) != p["filled_cells"]:
            raise ValueError("patch/realized-cell accounting mismatch")
        stats, annual = validate_matrix(target, names)
    finally:
        if writer:
            writer.close()
    s["source_base_features"] = s.pop("strict_base_features")
    s["contract"] = "tw_preopen_cross_source_research_v1_delay1d"
    s["daily_missing_only_filled_features"] = list(fills)
    atomic_write_json(out / "feature_columns.json", s)
    atomic_write_json(out / "validation.json", {**stats, "existing_observed_values_changed": 0,
        "missing_only_fills": observed})
    write_csv(out / "annual_coverage.csv", annual)
    old = read(parent / "validation.json")["features"]
    write_csv(out / "missing_values.csv", [{"feature": n, "old_missing": old[n]["missing"],
        "filled": observed[n], "remaining_missing": stats["features"][n]["missing"],
        "online_exhausted": False,
        "status": "needs_source_or_applicability_review" if stats["features"][n]["missing"] else "no_missing_in_scope"}
        for n in names])
    lineage = {"parent": str(parent.resolve()), "parent_manifest_sha256": sha256(parent / "dataset_manifest.json"),
        "parent_matrix_sha256": m["matrix"]["sha256"], "daily_bundle": str(bundle.resolve()),
        "daily_bundle_sha256": sha256(bundle / "bundle_manifest.json")}
    atomic_write_json(out / "lineage.json", lineage)
    if implementation != sha256(Path(__file__)) or sha256(matrix) != m["matrix"]["sha256"]:
        raise ValueError("input/code changed during build")
    result = {"contract": s["contract"], "created_at_utc": datetime.now(UTC).isoformat(),
        "research_training_features_ready": True, "strict_training_eligible": False,
        "historical_point_in_time": False, "publication_time_estimated": True,
        "extra_conservative_delay_calendar_days": 1, "live_eligible": False,
        "day_trade_execution_training_ready": False, "all_missing_resolved": False, "online_exhausted": False,
        "matrix": {"path": str(target.resolve()), "sha256": sha256(target), "rows": stats["rows"]},
        "value_features": len(names), "total_model_channels": 2 * len(names),
        "missing_only_daily_fills": sum(observed.values()), "existing_observed_values_changed": 0,
        "feature_schema_sha256": sha256(out / "feature_columns.json"),
        "validation_sha256": sha256(out / "validation.json"), "lineage": lineage,
        "implementation_sha256": implementation,
        "limitations": m["limitations"] + ["Source disagreement/unmapped/inapplicable cells remain unresolved",
            "Daily alternate values preserve feature definitions but are not official-source PIT claims"]}
    code = f'''from pathlib import Path
import json, sys
sys.path.insert(0, {str(ROOT)!r})
from scripts.apply_tw_daily_feature_fill_bundle import verify_observed_unchanged
from scripts.curate_tw_day_trade_training_dataset import validate_matrix
from scripts.prepare_tw_day_trade_feature_catalog import sha256
r = Path({str(out.resolve())!r})
m = json.loads((r / 'dataset_manifest.json').read_text())
s = json.loads((r / 'feature_columns.json').read_text())
assert sha256(r / 'model_inputs.parquet') == m['matrix']['sha256']
assert sha256(r / 'feature_columns.json') == m['feature_schema_sha256']
c = verify_observed_unchanged(Path(m['lineage']['parent']) / 'model_inputs.parquet', r / 'model_inputs.parquet', s['value_features'])
assert sum(c.values()) == m['missing_only_daily_fills']
q, annual = validate_matrix(r / 'model_inputs.parquet', s['value_features'])
print({{k:v for k,v in q.items() if k != 'features'}})
'''
    atomic_write_json(out / "verify_dataset.ipynb", {"nbformat":4,"nbformat_minor":5,"metadata":{},"cells":[
        {"cell_type":"markdown","id":"scope","metadata":{},"source":["# 缺值補洞驗收\n研究時間代理，不是歷史原始版本或成交回測；原始有效數值不得變動。"]},
        {"cell_type":"code","id":"verify","metadata":{},"execution_count":None,"outputs":[],"source":code.splitlines(keepends=True)}]})
    # Publish readiness only after every promised validation companion exists.
    atomic_write_json(out / "dataset_manifest.json", result)
    return result


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--parent",type=Path,required=True)
    p.add_argument("--bundle",type=Path,required=True)
    p.add_argument("--output-dir",type=Path,required=True)
    a=p.parse_args()
    print(json.dumps(build(a.parent,a.bundle,a.output_dir),ensure_ascii=False,indent=2))
