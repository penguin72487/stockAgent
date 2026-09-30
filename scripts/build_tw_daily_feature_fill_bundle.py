#!/usr/bin/env python3
"""Audit/fill curated daily-feature holes from receipt-verified alternate data.

Produces sparse missing-only patches; preserves canonical observed values and
explicit official-conflict masks. Does not rewrite the canonical source table.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from scripts.prepare_tw_day_trade_feature_catalog import sha256, write_csv
from stockagent.data.tw_public_cross_source_fill import missing_only, read_finmind, unique_values


def next_session_values(frame, expression, sessions):
    """Exact next market session; never next surviving row of a security."""
    table, bad = unique_values(frame.select(pl.col("date").cast(pl.String).str.slice(0, 10).alias("period"),
        pl.col("stock_id").alias("symbol"), expression.cast(pl.Float64).alias("value")), ["period", "symbol"])
    next_day = {str(a):str(b) for a,b in zip(sessions, sessions[1:])}
    table = table.with_columns(pl.col("period").replace_strict(next_day, default=None).alias("period"))
    return table.drop_nulls("period"), bad.height


def make_candidates(frames, sessions):
    expressions = {
        "TaiwanStockPrice": {
            "twpub_official_trading_volume_raw": pl.col("Trading_Volume"),
            "twpub_official_trading_value_raw": pl.col("Trading_money"),
            "twpub_official_trades_raw": pl.col("Trading_turnover"),
        },
        "TaiwanStockMarginPurchaseShortSale": {
            "twpub_margin_balance_lots_raw": pl.col("MarginPurchaseTodayBalance"),
            "twpub_short_balance_lots_raw": pl.col("ShortSaleTodayBalance"),
            "twpub_margin_balance_chg": ((pl.col("MarginPurchaseTodayBalance") - pl.col("MarginPurchaseYesterdayBalance")) / 1000).arcsinh(),
            "twpub_short_balance_chg": ((pl.col("ShortSaleTodayBalance") - pl.col("ShortSaleYesterdayBalance")) / 1000).arcsinh(),
            **{n: (pl.col(c) / 1000).arcsinh() for n,c in {
                "twpub_margin_buy_flow": "MarginPurchaseBuy", "twpub_margin_sell_flow": "MarginPurchaseSell",
                "twpub_short_buy_flow": "ShortSaleBuy", "twpub_short_sell_flow": "ShortSaleSell"}.items()},
        },
        "TaiwanStockPER": {
            "twpub_pe_raw": pl.when(pl.col("PER") > 0).then(pl.col("PER")).otherwise(None),
            "twpub_pb_raw": pl.when(pl.col("PBR") > 0).then(pl.col("PBR")).otherwise(None),
            "twpub_dividend_yield_pct_raw": pl.when(pl.col("dividend_yield") >= 0).then(pl.col("dividend_yield")).otherwise(None),
        },
    }
    # Foreign dealer / dealer self / hedging are disjoint components. The
    # legacy Dealer field is a different historical layout, NOT added again
    # when the disaggregated representation exists. Gate on overlap below.
    net = lambda category: pl.col(category + "_buy") - pl.col(category + "_sell")
    legacy_active = (pl.col("Dealer_buy") != 0) | (pl.col("Dealer_sell") != 0)
    split_active = pl.any_horizontal([pl.col(c + "_" + side) != 0
        for c in ("Dealer_self", "Dealer_Hedging") for side in ("buy", "sell")])
    # If both representations are nonzero, their non-overlap is unproven.
    dealer = pl.when(legacy_active & split_active).then(None).otherwise(
        net("Dealer") + net("Dealer_self") + net("Dealer_Hedging"))
    expressions["TaiwanStockInstitutionalInvestorsBuySellWide"] = {
        "twpub_foreign_net_buy_flow": (net("Foreign_Investor") / 1000).arcsinh(),
        "twpub_investment_trust_net_buy_flow": (net("Investment_Trust") / 1000).arcsinh(),
        "twpub_dealer_net_buy_flow": (dealer / 1000).arcsinh(),
        "twpub_institutional_net_buy_flow": ((net("Foreign_Investor") + net("Foreign_Dealer_Self")
            + net("Investment_Trust") + dealer) / 1000).arcsinh(),
    }
    for dataset, names in expressions.items():
        if dataset not in frames:
            continue
        for name, expression in names.items():
            candidate, conflicts = next_session_values(frames[dataset], expression, sessions)
            yield name, dataset, candidate, conflicts
    if "TaiwanStockPrice" in frames and "TaiwanStockShareholding" in frames:
        shares, _ = unique_values(frames["TaiwanStockShareholding"].select("date", "stock_id",
            pl.col("NumberOfSharesIssued").cast(pl.Float64).alias("value")), ["date", "stock_id"])
        shares = shares.rename({"value":"_shares"})
        joined = frames["TaiwanStockPrice"].join(shares, on=["date", "stock_id"], how="left", validate="m:1")
        candidate, conflicts = next_session_values(joined,
            pl.when(pl.col("_shares") > 0).then(pl.col("Trading_Volume") / pl.col("_shares")).otherwise(None), sessions)
        yield "twpub_official_turnover_ratio", "TaiwanStockPrice+TaiwanStockShareholding", candidate, conflicts


def official_volume_turnover(primary, shareholding, sessions):
    """Retain the official volume definition; supplement only the denominator."""
    shares, bad = next_session_values(shareholding, pl.col("NumberOfSharesIssued"), sessions)
    joined = primary.select("period", "symbol", "twpub_official_trading_volume_raw").join(
        shares.rename({"value": "_shares"}), on=["period", "symbol"], how="left", validate="1:1")
    candidate = joined.select("period", "symbol",
        pl.when((pl.col("_shares") > 0) & (pl.col("twpub_official_trading_volume_raw") >= 0))
        .then(pl.col("twpub_official_trading_volume_raw") / pl.col("_shares")).otherwise(None).alias("value"))
    return candidate.filter(pl.col("value").is_finite()), bad


def official_market_index(primary, calendar):
    lookup = calendar.sort("date").select(
        pl.col("date").shift(-1).cast(pl.String).alias("period"), pl.col("closing_index").alias("value"))
    return primary.select("period", "symbol").join(lookup.drop_nulls(), on="period", how="inner", validate="m:1")


def build(*, base, finmind, calendar, out, extend=None, market_only=False):
    if out.exists():
        raise FileExistsError("use a new versioned output directory")
    code = {p: sha256(ROOT / p) for p in ["scripts/build_tw_daily_feature_fill_bundle.py",
                                        "stockagent/data/tw_public_cross_source_fill.py"]}
    out.mkdir(parents=True)
    matrix = base / "model_inputs.parquet"
    m = json.loads((base / "dataset_manifest.json").read_text())
    if sha256(matrix) != m["matrix"]["sha256"]:
        raise ValueError("base matrix changed")
    schema = json.loads((base / "feature_columns.json").read_text())
    names = schema["value_features"]
    primary = pl.read_parquet(matrix, columns=["date", "symbol", *names]).with_columns(
        pl.col("date").cast(pl.String).alias("period"))
    symbols = primary["symbol"].unique().to_list()
    sessions = pl.read_parquet(calendar, columns=["date"])["date"].cast(pl.Date).sort().unique(maintain_order=True).to_list()
    masked = json.loads((base / "quality_masks.json").read_text())["cells"]
    mask_table = pl.DataFrame(masked).select(pl.col("date").alias("period"), "symbol", "feature")
    frames, receipts, audits, fill_paths = {}, {}, [], {}
    datasets = ([] if market_only else ["TaiwanStockShareholding"] if extend else ["TaiwanStockPrice", "TaiwanStockShareholding",
        "TaiwanStockMarginPurchaseShortSale", "TaiwanStockInstitutionalInvestorsBuySellWide", "TaiwanStockPER"])
    parent_bundle = None
    if extend:
        parent = json.loads((extend / "bundle_manifest.json").read_text())
        if parent["base_sha256"] != m["matrix"]["sha256"]:
            raise ValueError("extension base differs")
        for n, r in parent["fills"].items():
            if sha256(Path(r["path"])) != r["sha256"]:
                raise ValueError("prior patch changed")
            fill_paths[n] = r
        replaced = "twpub_twse_taiex_raw" if market_only else "twpub_official_turnover_ratio"
        audits = [r for r in json.loads((extend / "mapping_audit.json").read_text()) if r["feature"] != replaced]
        parent_bundle = {"path": str((extend / "bundle_manifest.json").resolve()),
                         "sha256": sha256(extend / "bundle_manifest.json")}
    for ds in datasets:
        root = finmind / ("complement" if ds == "TaiwanStockPER" else "sponsor")
        print(f"[daily-fill] receipt verification: {ds}", flush=True)
        frame, receipt = read_finmind(root, ds, first="2013-01-01", symbols=symbols)
        frames[ds] = frame.filter(pl.col("stock_id").is_in(symbols))
        receipts[ds] = receipt
    def candidates():
        if market_only:
            yield "twpub_twse_taiex_raw", "official_TWSE_monthly_archive_closing_index", official_market_index(
                primary, pl.read_parquet(calendar, columns=["date", "closing_index"])), 0
            return
        for item in make_candidates(frames, sessions):
            if item[0] != "twpub_official_turnover_ratio":
                yield item
        turnover, bad = official_volume_turnover(primary, frames["TaiwanStockShareholding"], sessions)
        yield "twpub_official_turnover_ratio", "official_volume+FinMind_NumberOfSharesIssued", turnover, bad
    for name, ds, candidate, conflicts in candidates():
        candidate = candidate.join(primary.select("period", "symbol"), on=["period", "symbol"], how="semi")
        candidate = candidate.join(mask_table.filter(pl.col("feature") == name).select("period", "symbol"),
            on=["period", "symbol"], how="anti")
        fills, audit, different = missing_only(primary.select("period", "symbol", pl.col(name).cast(pl.Float64).alias("value")),
            candidate, absolute_tolerance=1e-6, relative_tolerance=2e-6)
        audit.update(feature=name, source=ds, candidate_conflicting_keys=conflicts)
        audits.append(audit)
        print(f"[daily-fill] {name}: agreement={audit['agreement']:.6f}, fills={fills.height:,}", flush=True)
        if different.height:
            different.write_parquet(out / f"{name}.conflicts.parquet", compression="zstd")
        if fills.height:
            target = out / f"{name}.fills.parquet"
            fills.select(pl.col("period").str.to_date().alias("date"), "symbol", pl.col("value").alias(name)).write_parquet(
                target, compression="zstd")
            fill_paths[name] = {"path": str(target.resolve()), "sha256": sha256(target), "rows": fills.height}
    atomic_write_json(out / "source_receipts.json", receipts)
    atomic_write_json(out / "mapping_audit.json", audits)
    write_csv(out / "mapping_audit.csv", audits)
    if code != {p: sha256(ROOT / p) for p in code}:
        raise ValueError("implementation changed during build")
    result = {"contract": "tw_daily_feature_missing_only_v1", "created_at_utc": datetime.now(UTC).isoformat(),
        "parent_bundle": parent_bundle,
        "base_sha256": m["matrix"]["sha256"], "fills": fill_paths,
        "filled_cells": sum(a["filled_keys"] for a in audits),
        "all_missing_resolved": False, "online_exhausted": False,
        "quality_masks_sha256": sha256(base / "quality_masks.json"),
        "calendar_sha256": sha256(calendar), "source_receipts_sha256": sha256(out / "source_receipts.json"),
        "mapping_audit_sha256": sha256(out / "mapping_audit.json"),
        "implementation": code}
    atomic_write_json(out / "bundle_manifest.json", result)
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--base", type=Path, default=ROOT / "artifacts/datasets/tw_day_trade_curated_20260928_v3")
    p.add_argument("--finmind", type=Path, default=ROOT / "data_finmind")
    p.add_argument("--calendar", type=Path, default=ROOT / "data_tw_public/twse_taiex_ohlc.parquet")
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--extend-bundle", type=Path)
    p.add_argument("--market-only", action="store_true")
    a = p.parse_args()
    m = build(base=a.base, finmind=a.finmind, calendar=a.calendar, out=a.output_dir,
              extend=a.extend_bundle, market_only=a.market_only)
    print(json.dumps({k:v for k,v in m.items() if k != "fills"}, indent=2))


if __name__ == "__main__":
    main()
