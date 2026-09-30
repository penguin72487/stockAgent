#!/usr/bin/env python3
"""Independent readback and causal/patch checks for the semantic research ABI."""
from __future__ import annotations

import argparse
from collections import Counter
from itertools import zip_longest
from pathlib import Path
import json
import sys

import numpy as np
import polars as pl
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from scripts.prepare_tw_day_trade_feature_catalog import sha256
from stockagent.data.tw_feature_semantics import FINANCIAL_IDENTITIES, identity_candidate
from stockagent.data.tw_public_release_schedule import NMI_FIRST_RELEASE, feature_name


def verify_tdcc_state(actual, observations, feature):
    """Independent NumPy as-of reconstruction, including explicit NULL barriers."""
    sources = observations.sort("date").partition_by("symbol", as_dict=True)
    checked, monthly_rows, expired = 0, 0, 0
    for symbol, group in actual.partition_by("symbol", as_dict=True).items():
        dates = group["date"].to_numpy().astype("datetime64[D]")
        expected = np.full(group.height, np.nan, dtype=np.float32)
        source = sources.get(symbol)
        limits = np.where(dates < np.datetime64("2015-05-01"), 62, 14)
        if source is not None:
            released = source["date"].to_numpy().astype("datetime64[D]")
            if np.any(released[1:] <= released[:-1]):
                raise ValueError("ambiguous TDCC observation clock")
            index = np.searchsorted(released, dates, side="right") - 1
            positions = np.flatnonzero(index >= 0)
            age = (dates[positions] - released[index[positions]]).astype(int)
            eligible = positions[age <= limits[positions]]
            expected[eligible] = source[feature].to_numpy()[index[eligible]]
            expired += int((age > limits[positions]).sum())
        observed = group[feature].to_numpy()
        if not np.array_equal(expected, observed, equal_nan=True):
            raise ValueError("TDCC dated state carry mismatch")
        monthly_rows += int(((limits == 62) & np.isfinite(expected)).sum())
        checked += group.height
    return {"rows_checked": checked, "historical_monthly_observed_rows": monthly_rows,
            "expired_state_rows": expired, "value_or_null_mismatches": 0}


def verify(root: Path):
    read = lambda p: json.loads(p.read_text())
    m = read(root / "dataset_manifest.json")
    parent = Path(m["parent"])
    before, after = parent / "model_inputs.parquet", root / "model_inputs.parquet"
    if sha256(before) != m["parent_matrix_sha256"] or sha256(after) != m["matrix"]["sha256"]:
        raise ValueError("matrix fingerprints changed")
    for filename, field in (("feature_columns.json", "feature_schema_sha256"),
                            ("validation.json", "validation_sha256"),
                            ("source_receipts.json", "source_receipts_sha256"),
                            ("missingness_causes.csv", "missingness_causes_sha256"),
                            ("remaining_feature_worklist.csv", "worklist_sha256"),
                            ("publication_rules.json", "rules_sha256")):
        if sha256(root / filename) != m[field]:
            raise ValueError(f"sidecar fingerprint changed: {filename}")
    schema = read(root / "feature_columns.json")
    names = schema["value_features"]
    counts, rows = Counter(), 0
    left = pq.ParquetFile(before).iter_batches(batch_size=65536)
    right = pq.ParquetFile(after).iter_batches(batch_size=65536)
    for a, b in zip_longest(left, right):
        if a is None or b is None or a.num_rows != b.num_rows or not a.schema.equals(b.schema):
            raise ValueError("schema/row count drift")
        if not a.column(0).equals(b.column(0)) or not a.column(1).equals(b.column(1)):
            raise ValueError("economic keys/order changed")
        for i, f in enumerate(names, 2):
            x, y = a.column(i).to_numpy(zero_copy_only=False), b.column(i).to_numpy(zero_copy_only=False)
            av, bv = np.isfinite(x), np.isfinite(y)
            counts[(f, "filled")] += int((~av & bv).sum())
            counts[(f, "invalidated")] += int((av & ~bv).sum())
            counts[(f, "revised_carried_state")] += int((av & bv & (x != y)).sum())
            mask = b.column(i + len(names)).to_numpy(zero_copy_only=False)
            if not np.array_equal(mask, bv):
                raise ValueError("mask mismatch")
        rows += a.num_rows
    changes = pl.read_csv(root / "changes.csv")
    for r in changes.iter_rows(named=True):
        for kind in ("filled", "invalidated", "revised_carried_state"):
            if counts[(r["feature"], kind)] != r[kind]:
                raise ValueError("recorded vs realized change mismatch")
    en = feature_name("block_trade:成交金額")
    zeros = pl.read_parquet(root / "verified_absence_zeros.parquet")
    context = pl.read_parquet(root / "previous_session_context.parquet", columns=["date", "symbol", "source_date", "market"])
    reports = pl.read_parquet(root / "block_market_reconciliation.parquet")
    proved = zeros.join(context, on=["date", "symbol"], validate="1:1").join(
        reports.select("date", "market", "absence_zero_verified"), on=["date", "market"], how="left", validate="m:1")
    if proved.height != zeros.height or proved.filter(~pl.col("absence_zero_verified").fill_null(False) | (pl.col(en) != 0) | (pl.col("date") <= pl.col("source_date"))).height:
        raise ValueError("event zeros lack market reconciliation/causal clock")
    actual = pl.read_parquet(after, columns=["date", "symbol", en]).join(zeros.select("date", "symbol"), on=["date", "symbol"], validate="1:1")
    if actual.height != zeros.height or actual[en].null_count() or actual.filter(pl.col(en) != 0).height:
        raise ValueError("event zero not materialized exactly")
    source_receipts = read(root / "source_receipts.json")
    normalized = Path(source_receipts["normalized_parent"])
    tf = feature_name("etl:inventory:大於四百張佔比")
    tdcc = verify_tdcc_state(pl.read_parquet(after, columns=["date", "symbol", tf]),
        pl.read_parquet(normalized / "observations" / f"{tf}.parquet"), tf)
    observations = {}
    for terms in FINANCIAL_IDENTITIES.values():
        for name, _ in terms:
            if name not in observations:
                f = feature_name("financial_statement:" + name)
                observations[name] = pl.read_parquet(normalized / "observations" / f"{f}.parquet").select(
                    "symbol", "date", pl.col("source_index").alias("period"), pl.col(f).alias("value"))
    identity_rows = 0
    for name, terms in FINANCIAL_IDENTITIES.items():
        f = feature_name("financial_statement:" + name)
        path = root / f"{f}.identity_fills.parquet"
        if not path.exists():
            continue
        fills = pl.read_parquet(path)
        candidate = identity_candidate(observations, terms)
        compared = fills.join(candidate, on=["period", "symbol"], suffix="_expected", how="left", validate="1:1")
        if compared.filter(pl.col("date_expected").is_null() | (pl.col("date") != pl.col("date_expected")) | (pl.col("value") != pl.col("value_expected"))).height:
            raise ValueError("accounting identity operands/clock mismatch")
        primary = pl.read_parquet(normalized / "observations" / f"{f}.parquet").select("symbol", pl.col("source_index").alias("period"))
        if fills.join(primary, on=["period", "symbol"]).height:
            raise ValueError("accounting identity overwrites an original report")
        identity_rows += fills.height
    nmi = [f for f in names if f.startswith("twfl_sched_tw_total_nmi_")]
    premature = pl.scan_parquet(after).filter(pl.col("date") <= NMI_FIRST_RELEASE).select(
        pl.sum_horizontal(pl.col(f).is_not_null().sum() for f in nmi)).collect().item()
    if premature:
        raise ValueError("NMI used before first public release")
    nm = read(normalized / "dataset_manifest.json")
    base = Path(nm["base_matrix"]).parent
    if sha256(Path(nm["base_matrix"])) != nm["base_matrix_sha256"]:
        raise ValueError("strict base changed")
    explicit = pl.DataFrame(read(base / "quality_masks.json")["cells"]).with_columns(pl.col("date").str.to_date())
    tested = 0
    for f in explicit["feature"].unique():
        t = pl.scan_parquet(after).select("date", "symbol", f).collect().join(
            explicit.filter(pl.col("feature") == f).select("date", "symbol"), on=["date", "symbol"], validate="1:1")
        if t[f].is_not_null().any():
            raise ValueError("official conflict mask lifted")
        tested += t.height
    reason = pl.read_csv(root / "missingness_causes.csv")
    stats = read(root / "validation.json")
    if reason["cells"].sum() != sum(v["missing"] for v in stats["features"].values()):
        raise ValueError("missingness categories do not reconcile")
    result = {"status": "passed", "rows_compared": rows, "feature_cells_compared": rows * len(names),
        "keys_or_order_changed": 0, "undocumented_value_changes": 0, "availability_mask_errors": 0,
        "verified_event_zero_cells": zeros.height, "verified_same_period_identity_observations": identity_rows,
        "premature_nmi_cells": premature, "explicit_official_conflict_cells_preserved": tested,
        "tdcc_independent_dated_carry": tdcc,
        "filled_cells": sum(counts[(f, "filled")] for f in names),
        "invalidated_cells": sum(counts[(f, "invalidated")] for f in names),
        "revised_carried_state_cells": sum(counts[(f, "revised_carried_state")] for f in names),
        "missingness_partition_total": reason["cells"].sum(), "all_missing_resolved": False,
        "matrix_sha256": m["matrix"]["sha256"], "verifier_sha256": sha256(Path(__file__))}
    atomic_write_json(root / "independent_acceptance.json", result)
    return result


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("dataset", type=Path)
    print(json.dumps(verify(p.parse_args().dataset), ensure_ascii=False, indent=2))
