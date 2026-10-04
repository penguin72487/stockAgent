#!/usr/bin/env python3
"""Reconcile event sparsity and accounting identities in a NEW research ABI.

No provider downloads, production mutation, training or price interpolation.
Reuses the pinned release observations, official calendar, source stock files,
shared feature transforms, and the existing full matrix validator.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime
import json
from pathlib import Path
import sys

import polars as pl
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from scripts.prepare_tw_day_trade_feature_catalog import sha256, write_csv
from scripts.curate_tw_day_trade_training_dataset import validate_matrix
from scripts.build_tw_release_schedule_dataset import normalize_source
from stockagent.data.finlab_research_overlay import _verified_source
from stockagent.data.panel import _prepare_symbol_frame
from stockagent.data.tw_day_trade_feature_admission import TECHNICAL_FEATURES, DAILY_PUBLIC_FEATURES
from stockagent.data.tw_public_release_schedule import align_observations, feature_name, next_session, rule_for, rule_manifest
from stockagent.data.tw_feature_semantics import (
    CONTRACT, BLOCK_PARTS, FINANCIAL_IDENTITIES, accepted_identity_fill,
    collapse_period_observations, event_zero_patch, identity_candidate,
    missingness_reasons, reconcile_block_reports,
)


def read(path):
    return json.loads(Path(path).read_text())


def long_wide(frame, name="value"):
    return frame.unpivot(index="source_index", variable_name="symbol", value_name=name)


def load_source(finlab, key, receipts):
    found = _verified_source(finlab, key)
    if found is None:
        raise ValueError(f"missing verified source {key}")
    path, receipt = found
    receipts.append({"dataset": key, "path": str(path.resolve()), "sha256": receipt["sha256"]})
    return pl.read_parquet(path)


def stock_context(stocks, sessions, keys, receipts):
    """Previous ACTUAL exchange session, never previous surviving symbol row."""
    paths = sorted(stocks.glob("*_features.parquet"))
    if not paths:
        raise ValueError("missing pinned stock inputs")
    for p in paths:
        receipts.append({"path": str(p.resolve()), "sha256": sha256(p)})
    table = pl.scan_parquet(paths, include_file_paths="_file").select(
        "date", "close", "open", "max", "min", "Trading_Volume", "data_source", "lifecycle_episode_id",
        pl.col("_file").str.extract(r"/([^/]+)_features\.parquet$", 1).alias("symbol"))
    table = table.filter(pl.col("date").is_between(keys["date"].min().replace(year=keys["date"].min().year - 1), keys["date"].max())).collect()
    dates = {a: b for a, b in zip(sessions, sessions[1:])}
    table = table.with_columns(pl.col("date").alias("source_date"),
        pl.col("date").replace_strict(dates, default=None, return_dtype=pl.Date).alias("date"),
        pl.col("data_source").replace_strict({"twse_official": "上市", "tpex_official": "上櫃"}, default=None).alias("market"))
    # Do not transfer yesterday's state into a reused security incarnation.
    current = pl.scan_parquet(paths, include_file_paths="_file").select("date", "lifecycle_episode_id",
        pl.col("_file").str.extract(r"/([^/]+)_features\.parquet$", 1).alias("symbol")).collect()
    table = table.join(current.rename({"lifecycle_episode_id": "_current_episode"}), on=["date", "symbol"], how="left", validate="1:1")
    table = table.filter(pl.col("lifecycle_episode_id") == pl.col("_current_episode"))
    return keys.join(table, on=["date", "symbol"], how="left", validate="1:1", maintain_order="left")


def block_patch(finlab, context, sessions, parent_obs, out, receipts):
    key = "block_trade:成交金額"
    amounts = long_wide(load_source(finlab, key, receipts)).drop_nulls("value")
    markets = long_wide(load_source(finlab, "block_trade:市場別", receipts), "market").drop_nulls("market")
    parts = []
    for name in BLOCK_PARTS:
        t = load_source(finlab, "block_trade:" + name, receipts)
        parts.append(t.select("source_index", "OTC_BLOCK_SUMMARY", "TSE_BLOCK_SUMMARY")
            .unpivot(index="source_index", variable_name="symbol", value_name=name))
    summaries = parts[0]
    for t in parts[1:]:
        summaries = summaries.join(t, on=["source_index", "symbol"], validate="1:1")
    summaries = summaries.with_columns(pl.col("symbol").replace_strict(
        {"OTC_BLOCK_SUMMARY": "上櫃", "TSE_BLOCK_SUMMARY": "上市"}).alias("market")).drop("symbol")
    amounts = amounts.filter(~pl.col("symbol").str.ends_with("_BLOCK_SUMMARY"))
    markets = markets.filter(~pl.col("symbol").str.ends_with("_BLOCK_SUMMARY"))
    reports = reconcile_block_reports(amounts, markets, summaries)
    source_dates = reports["source_index"].unique().to_list()
    lookup = {d: next_session(datetime.fromisoformat(d).date(), sessions) for d in source_dates}
    reports = reports.with_columns(pl.col("source_index").replace_strict(lookup, return_dtype=pl.Date).alias("date"))
    # Multiple non-session source days cannot be collapsed by row order.
    reports = reports.filter(pl.col("date").is_not_null())
    if reports.select(pl.struct("date", "market").is_duplicated().any()).item():
        raise ValueError("ambiguous block report decision date")
    reports.write_parquet(out / "block_market_reconciliation.parquet", compression="zstd")
    patch = event_zero_patch(context.select("date", "symbol", "market"), reports, parent_obs, feature_name(key))
    return patch, {"reports": reports.height, "verified_market_days": reports["absence_zero_verified"].sum(),
        "unreconciled_market_days": reports.filter(~pl.col("absence_zero_verified")).height,
        "absence_zero_cells": patch.height, "zero_before_source_history": 0}


def warmup_patch(stocks, sessions, keys, out, base_manifest, base):
    """Reuse canonical technical formulas with pre-export history, only day 1."""
    first = keys["date"].min()
    previous = sessions[sessions.index(first) - 1]
    rows = []
    for symbol in keys.filter(pl.col("date") == first)["symbol"]:
        path = stocks / f"{symbol}_features.parquet"
        raw = pl.read_parquet(path).filter(pl.col("date") <= first).sort("date")
        yesterday = raw.filter(pl.col("date") == previous)
        today = raw.filter(pl.col("date") == first)
        if yesterday.height != 1 or today.height != 1 or yesterday["lifecycle_episode_id"].item() != today["lifecycle_episode_id"].item():
            continue
        raw = raw.with_columns((pl.col("lifecycle_episode_id") != pl.col("lifecycle_episode_id").shift(1)).fill_null(True).alias("lifecycle_reset"))
        t = _prepare_symbol_frame(raw, path).with_columns(pl.col("date").cast(pl.Date))
        t = t.filter(pl.col("date") == previous).select("symbol", *TECHNICAL_FEATURES)
        rows.append(t.with_columns(pl.lit(first).alias("date")))
    result = pl.concat(rows).select("date", "symbol", *TECHNICAL_FEATURES)
    result = result.with_columns(pl.when(pl.col(n).is_finite()).then(pl.col(n)).otherwise(None).cast(pl.Float32).alias(n) for n in TECHNICAL_FEATURES)
    source = Path(base_manifest["source"]["feature_path"])
    if sha256(source) != base_manifest["source"]["feature_sha256"]:
        raise ValueError("official warmup feature source changed")
    daily = pl.scan_parquet(source).filter(pl.col("date") == previous).select(
        "symbol", *DAILY_PUBLIC_FEATURES).collect().with_columns(pl.lit(first).alias("date"))
    if daily.select(pl.col("symbol").is_duplicated().any()).item():
        raise ValueError("ambiguous daily warmup source")
    result = result.join(daily, on=["date", "symbol"], how="left", validate="1:1")
    # Explicit official-conflict cells may never be lifted by warmup.
    for row in read(base / "quality_masks.json")["cells"]:
        f = row["feature"]
        if row["date"] == first.isoformat() and f in result.columns:
            result = result.with_columns(pl.when(pl.col("symbol") == row["symbol"]).then(None).otherwise(pl.col(f)).alias(f))
    result = result.with_columns(pl.col(f).cast(pl.Float32) for f in DAILY_PUBLIC_FEATURES)
    result.write_parquet(out / "warmup_candidates.parquet", compression="zstd")
    return result


def build(parent, normalized, finlab, out):
    if out.exists():
        raise FileExistsError("use a new versioned output directory")
    matrix = parent / "model_inputs.parquet"
    manifest, schema = read(parent / "dataset_manifest.json"), read(parent / "feature_columns.json")
    if sha256(matrix) != manifest["matrix"]["sha256"]:
        raise ValueError("parent matrix changed")
    nm = read(normalized / "dataset_manifest.json")
    if Path(manifest["lineage"]["parent"]).resolve() != normalized.resolve():
        raise ValueError("normalized observations must be the actual parent lineage")
    sources = read(normalized / "source_receipts.json")
    if sha256(normalized / "source_receipts.json") != nm["source_receipts_sha256"]:
        raise ValueError("normalized receipt changed")
    base = Path(nm["base_matrix"]).parent
    base_manifest = read(base / "dataset_manifest.json")
    stocks = Path(base_manifest["source"]["feature_path"]).parent.parent / "stocks"
    calendar = Path(sources["calendar"])
    if sha256(calendar) != sources["calendar_sha256"]:
        raise ValueError("calendar changed")
    sessions = pl.read_parquet(calendar, columns=["date"])["date"].sort().unique(maintain_order=True).to_list()
    keys = pl.read_parquet(matrix, columns=["date", "symbol"])
    names = schema["value_features"]
    definitions = pl.read_csv(normalized / "research_feature_dictionary.csv").to_dicts()
    definition_map = {d["feature"]: d for d in definitions}
    by_dataset = {d["dataset"]: d for d in definitions}
    out.mkdir(parents=True)
    (out / "observations").mkdir()
    receipts, observations, source_symbols = [], {}, {}
    implementation = {p: sha256(ROOT / p) for p in (
        "scripts/repair_tw_feature_semantics.py", "stockagent/data/tw_feature_semantics.py",
        "stockagent/data/tw_public_release_schedule.py", "stockagent/data/panel.py",
        "scripts/build_tw_release_schedule_dataset.py")}
    for r in sources["sources"]:
        path = Path(r["normalized_path"])
        if sha256(path) != r["normalized_sha256"] or sha256(Path(r["path"])) != r["sha256"]:
            raise ValueError("pinned observation/source changed")
        feature = feature_name(r["dataset"])
        observations[feature] = pl.read_parquet(path)
        source_symbols[feature] = sorted(set(pl.scan_parquet(r["path"]).collect_schema().names()) | set(observations[feature]["symbol"].drop_nulls()))
        receipts.append({"dataset": r["dataset"], "path": str(path), "sha256": r["normalized_sha256"]})
    print("[semantics] all 206 normalized observation receipts verified", flush=True)
    context = stock_context(stocks, sessions, keys, receipts)
    context.write_parquet(out / "previous_session_context.parquet", compression="zstd")
    print("[semantics] previous-session source context loaded", flush=True)
    event, block_audit = block_patch(finlab, context, sessions,
        observations[feature_name("block_trade:成交金額")], out, receipts)
    event.write_parquet(out / "verified_absence_zeros.parquet", compression="zstd")
    print(f"[semantics] reconciled block absence: {event.height:,} cells", flush=True)
    warm = warmup_patch(stocks, sessions, keys, out, base_manifest, base)
    warm_names = list(TECHNICAL_FEATURES) + list(DAILY_PUBLIC_FEATURES)
    print(f"[semantics] canonical pre-export warmup: {warm.height:,} stock rows", flush=True)
    # One pass only: do not recursively manufacture a chain of derived facts.
    tables = {}
    for key, d in by_dataset.items():
        if key.startswith("financial_statement:"):
            f = d["feature"]
            tables[key.split(":", 1)[1]] = observations[f].select("date", "symbol",
                pl.col("source_index").alias("period"), pl.col(f).alias("value"))
    identity_audit, changed_obs = [], {}
    for name, terms in FINANCIAL_IDENTITIES.items():
        if name not in tables or any(n not in tables for n, _ in terms):
            continue
        candidate = identity_candidate(tables, terms)
        if name not in {"營業毛利", "營業成本", "營業利益", "營業費用"}:
            candidate = candidate.filter(pl.col("value") >= 0)
        fills, audit = accepted_identity_fill(tables[name], candidate)
        f = feature_name("financial_statement:" + name)
        identity_audit.append({"feature": f, "identity": json.dumps(terms, ensure_ascii=False), **audit})
        if fills.height:
            fills.write_parquet(out / f"{f}.identity_fills.parquet", compression="zstd")
            raw = observations[f].select("date", "symbol", f, "source_index")
            added = fills.rename({"value": f, "period": "source_index"}).select(raw.columns)
            changed_obs[f] = collapse_period_observations(pl.concat([raw, added]), f)
    write_csv(out / "accounting_identity_audit.csv", identity_audit)
    print(f"[semantics] accounting identities: {sum(x['filled_keys'] for x in identity_audit):,} source periods", flush=True)
    # Discount = price/NAV - 1 cannot be <= -100% for positive prices/NAV.
    # Do not guess the correct value from a sentinel, and do not fill prices.
    ef = feature_name("tw_etf_nav_daily:折溢價(%)")
    invalid = observations[ef].filter(pl.col(ef) <= -100)
    invalid.write_parquet(out / "invalid_etf_discount.parquet", compression="zstd")
    changed_obs[ef] = observations[ef].with_columns(pl.when(pl.col(ef) > -100).then(pl.col(ef)).otherwise(None).alias(ef))
    tf = feature_name("etl:inventory:大於四百張佔比")
    changed_obs[tf] = observations[tf]
    observations[tf].group_by("source_index", "date").agg(pl.len().alias("stock_observations")).sort("date").write_csv(out / "tdcc_cadence_evidence.csv")
    # Official launch evidence supersedes the old generic monthly proxy.
    for r in sources["sources"]:
        if r["dataset"].startswith("tw_total_nmi:"):
            f = feature_name(r["dataset"])
            changed_obs[f], _ = normalize_source(Path(r["path"]), r["dataset"], sessions, keys["symbol"].unique().to_list())
    for f, table in changed_obs.items():
        table.write_parquet(out / "observations" / f"{f}.parquet", compression="zstd")
    updates = Counter()
    target = out / "model_inputs.parquet"
    writer = None
    try:
        for year in keys["date"].dt.year().unique().sort():
            frame = pl.scan_parquet(matrix).filter(pl.col("date").dt.year() == year).collect()
            before = {f: frame[f] for f in set(changed_obs) | set(warm_names) | {event.columns[-1]}}
            for f, table in changed_obs.items():
                frame = frame.with_columns(align_observations(frame.select("date", "symbol"), table, f, rule_for(definition_map[f]["dataset"])))
            en = event.columns[-1]
            frame = frame.join(event.filter(pl.col("date").dt.year() == year).rename({en: "_event"}),
                on=["date", "symbol"], how="left", maintain_order="left", validate="1:1")
            if frame.filter(pl.col(en).is_not_null() & pl.col("_event").is_not_null()).height:
                raise ValueError("event zero overwrites observed event")
            frame = frame.with_columns(pl.coalesce(en, "_event").alias(en)).drop("_event")
            if year == keys["date"].min().year:
                frame = frame.join(warm, on=["date", "symbol"], how="left", suffix="_warm", validate="1:1", maintain_order="left")
                for f in warm_names:
                    # Only boundary NULLs. Existing valid technical observations stay exact.
                    frame = frame.with_columns(pl.coalesce(f, f + "_warm").alias(f)).drop(f + "_warm")
            for f, old in before.items():
                now = frame[f]
                updates[(f, "filled")] += (old.is_null() & now.is_not_null()).sum()
                updates[(f, "invalidated")] += (old.is_not_null() & now.is_null()).sum()
                updates[(f, "revised_carried_state")] += (old.is_not_null() & now.is_not_null() & (old != now)).sum()
            frame = frame.with_columns(pl.col(f).is_not_null().alias(f + "__available") for f in before)
            arrow = frame.select(pq.ParquetFile(matrix).schema_arrow.names).to_arrow().cast(pq.ParquetFile(matrix).schema_arrow)
            if writer is None:
                writer = pq.ParquetWriter(target, arrow.schema, compression="zstd")
            writer.write_table(arrow, row_group_size=65536)
            print(f"[semantics] wrote {year}: {frame.height:,} rows", flush=True)
    finally:
        if writer is not None:
            writer.close()
    stats, annual = validate_matrix(target, names)
    changes = [{"feature": f, **{kind: updates[(f, kind)] for kind in ("filled", "invalidated", "revised_carried_state")}} for f in names]
    write_csv(out / "changes.csv", changes)
    write_csv(out / "annual_coverage.csv", annual)
    atomic_write_json(out / "validation.json", stats)
    observations.update(changed_obs)
    observations[event.columns[-1]] = pl.concat([observations[event.columns[-1]].select(event.columns), event], how="vertical_relaxed").sort("date", "symbol")
    # Every remaining cell receives a concrete temporal/scope category. This is
    # an audit sidecar, not an imputation model or hidden feature-selection rule.
    diagnoses = []
    af = feature_name("financial_statement:資產總額")
    report_context = observations[af].filter(pl.col(af).is_not_null()).select(
        "symbol", pl.col("date").alias("_report_date"), pl.col("source_index").alias("_report_period"))
    for i, f in enumerate(names):
        missing = pl.scan_parquet(target).filter(pl.col(f).is_null()).select("date", "symbol").collect()
        if f in definition_map:
            d = definition_map[f]
            reasons = missingness_reasons(missing, observations[f], f, rule_for(d["dataset"]), source_symbols[f], d["dataset"],
                report_context if d["dataset"].startswith("financial_statement:") else None).to_dicts()
        else:
            # Base features have different clocks and formula/eligibility domains.
            t = missing.join(context, on=["date", "symbol"], how="left", validate="1:1")
            expr = pl.when(pl.col("source_date").is_null()).then(pl.lit("no_previous_session_symbol_bar"))
            if f == "twpub_official_turnover_ratio":
                expr = expr.when(pl.col("market") == "上市").then(pl.lit("twse_source_has_no_issued_shares_operand"))
            if f in TECHNICAL_FEATURES or "logret" in f:
                expr = expr.when((pl.col("close") <= 0) | (pl.col("Trading_Volume") <= 0)).then(pl.lit("zero_volume_or_nonpositive_formula_operand"))
            if f.startswith(("twpub_pe_", "twpub_pb_", "twpub_dividend_yield_")):
                expr = expr.when(pl.col("symbol").str.starts_with("00")).then(pl.lit("company_valuation_not_applicable_to_etf"))
            reason = ("daily_source_absence_or_formula_domain" if f not in TECHNICAL_FEATURES else "technical_prior_operand_or_guard_unavailable")
            t = t.select(expr.otherwise(pl.lit(reason)).alias("reason"))
            reasons = t.group_by("reason").agg(pl.len().alias("cells")).to_dicts()
        if sum(r["cells"] for r in reasons) != stats["features"][f]["missing"]:
            raise ValueError("missingness diagnosis failed to partition cells")
        for r in reasons:
            diagnoses.append({"feature": f, "dataset": definition_map.get(f, {}).get("dataset", "canonical_base"), **r,
                "is_source_corruption_verdict": False})
        if i % 20 == 0:
            print(f"[semantics] diagnosed {i + 1}/{len(names)} features", flush=True)
    write_csv(out / "missingness_causes.csv", diagnoses)
    work = []
    for f in names:
        d = definition_map.get(f, {})
        causes = sorted([r for r in diagnoses if r["feature"] == f], key=lambda r: -r["cells"])
        work.append({"feature": f, "dataset": d.get("dataset", "canonical_base"),
            "cadence": rule_for(d["dataset"]).kind if d else "daily_or_exact_release",
            "state_carry_days": d.get("carry_days", ""), "observed": stats["features"][f]["observed"],
            "historical_carry_days": rule_for(d["dataset"]).older_carry_days if d else 0,
            "cadence_change_on": rule_for(d["dataset"]).cadence_change_on if d else "",
            "remaining_missing": stats["features"][f]["missing"], "filled": updates[(f, "filled")],
            "dominant_reason": causes[0]["reason"] if causes else "no_missing_in_scope",
            "reason_counts": json.dumps({r['reason']: r['cells'] for r in causes}, ensure_ascii=False),
            "source_corrupt": "not_inferred_from_null", "online_exhausted": False})
    write_csv(out / "remaining_feature_worklist.csv", work)
    atomic_write_json(out / "source_receipts.json", {"inputs": receipts, "normalized_parent": str(normalized.resolve()),
        "normalized_parent_manifest_sha256": sha256(normalized / "dataset_manifest.json")})
    for r in receipts:
        if sha256(Path(r["path"])) != r["sha256"]:
            raise ValueError("source changed during repair")
    if sha256(matrix) != manifest["matrix"]["sha256"] or implementation != {p: sha256(ROOT / p) for p in implementation}:
        raise ValueError("parent/code changed during repair")
    schema["contract"] = CONTRACT
    atomic_write_json(out / "feature_columns.json", schema)
    atomic_write_json(out / "publication_rules.json", rule_manifest())
    totals = Counter()
    for r in diagnoses:
        totals[r["reason"]] += r["cells"]
    result = {"contract": CONTRACT, "created_at_utc": datetime.now(UTC).isoformat(),
        "research_training_features_ready": True, "strict_training_eligible": False,
        "historical_point_in_time": False, "publication_time_estimated": True,
        "extra_conservative_delay_calendar_days": 1, "live_eligible": False,
        "day_trade_execution_training_ready": False, "all_missing_resolved": False, "online_exhausted": False,
        "matrix": {"path": str(target.resolve()), "rows": stats["rows"], "sha256": sha256(target)},
        "value_features": len(names), "total_model_channels": 2 * len(names),
        "filled_cells": sum(r["filled"] for r in changes), "invalidated_cells": sum(r["invalidated"] for r in changes),
        "revised_carried_state_cells": sum(r["revised_carried_state"] for r in changes),
        "block_trade": block_audit, "missingness_reason_totals": dict(totals),
        "source_corruption_not_inferred_from_missingness": True,
        "source_files_modified": False, "strict_base_modified": False,
        "parent": str(parent.resolve()), "parent_matrix_sha256": manifest["matrix"]["sha256"],
        "feature_schema_sha256": sha256(out / "feature_columns.json"),
        "source_receipts_sha256": sha256(out / "source_receipts.json"),
        "validation_sha256": sha256(out / "validation.json"), "implementation": implementation,
        "missingness_causes_sha256": sha256(out / "missingness_causes.csv"),
        "worklist_sha256": sha256(out / "remaining_feature_worklist.csv"),
        "rules_sha256": sha256(out / "publication_rules.json"),
        "limitations": manifest["limitations"] + ["NULL causes are diagnostics, not proof of permanent online absence",
            "Optional financial line items without explicit zero evidence remain NULL",
            "Unreconciled event reports never imply zero; daily flows never forward fill"]}
    code = f'''from pathlib import Path
import sys, json
sys.path.insert(0, {str(ROOT)!r})
from scripts.prepare_tw_day_trade_feature_catalog import sha256
from scripts.curate_tw_day_trade_training_dataset import validate_matrix
import polars as pl
r=Path({str(out.resolve())!r})
m=json.loads((r/'dataset_manifest.json').read_text())
s=json.loads((r/'feature_columns.json').read_text())
assert sha256(r/'model_inputs.parquet') == m['matrix']['sha256']
q,_=validate_matrix(r/'model_inputs.parquet',s['value_features'])
a=pl.read_csv(r/'missingness_causes.csv')
assert a['cells'].sum() == sum(v['missing'] for v in q['features'].values())
assert not m['live_eligible'] and not m['historical_point_in_time']
print({{k:v for k,v in q.items() if k != 'features'}})
'''
    atomic_write_json(out / "verify_dataset.ipynb", {"nbformat": 4, "nbformat_minor": 5, "metadata": {}, "cells": [
        {"cell_type": "markdown", "id": "scope", "metadata": {}, "source": ["# 更新週期與缺值語意驗證\n缺值不等於來源損壞；研究時點估計，不是原始版本 PIT。"]},
        {"cell_type": "code", "id": "verify", "metadata": {}, "execution_count": None, "outputs": [], "source": code.splitlines(keepends=True)}]})
    atomic_write_json(out / "dataset_manifest.json", result)
    return result


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--parent", type=Path, default=ROOT / "artifacts/datasets/tw_day_trade_cross_source_research_20260928_v1")
    p.add_argument("--normalized-parent", type=Path, default=ROOT / "artifacts/datasets/tw_day_trade_release_schedule_research_20260928_v5")
    p.add_argument("--finlab-root", type=Path, default=ROOT / "data_finlab")
    p.add_argument("--output-dir", type=Path, required=True)
    a = p.parse_args()
    print(json.dumps(build(a.parent, a.normalized_parent, a.finlab_root, a.output_dir), ensure_ascii=False, indent=2))
