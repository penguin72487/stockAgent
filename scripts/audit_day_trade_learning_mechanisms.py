#!/usr/bin/env python3
"""Read-only diagnostics; candidate counts are not realized fills or alpha."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--panel-cache", type=Path, required=True)
    parser.add_argument("--physical-cache", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.resolve().is_relative_to(args.root.resolve()):
        parser.error("write diagnostics outside the original experiment")
    meta = json.loads((args.panel_cache / "meta.json").read_text())
    generation = args.panel_cache / "generations" / meta["generation"]
    load = lambda name: np.load(generation / f"{name}.npy", mmap_mode="r")
    dates = load("dates").astype("datetime64[D]")
    symbols = json.loads((generation / "symbols.json").read_text())
    opening, closing = load("open_prices"), load("close_prices")
    eligible = load("day_trade_eligible_mask")
    tradable = load("tradable_mask")
    buy = load("day_trade_can_buy_open_mask")
    sell = load("day_trade_can_sell_open_mask")
    short = load("can_short_open_mask")
    physical = json.loads((args.physical_cache / "manifest.json").read_text())
    quote_valid = np.isfinite(opening) & np.isfinite(closing) & (opening > 0) & (closing > 0)
    raw_intraday = np.log(np.divide(closing, opening, out=np.ones_like(opening), where=quote_valid))
    periods = {}
    for label, start, end in [("train", "2014-01-01", "2025-01-01"),
                              ("validation", "2025-01-01", "2026-01-01"),
                              ("inspected_test", "2026-01-01", "2027-01-01")]:
        rows = (dates >= np.datetime64(start)) & (dates < np.datetime64(end))
        record = {"sessions": int(rows.sum())}
        for side, permission in [("long", buy), ("short", sell & short)]:
            legal = np.asarray(eligible[rows] & tradable[rows] & permission[rows])
            record[f"excluded_missing_price_{side}_symbol_days"] = int((legal & ~quote_valid[rows]).sum())
            legal &= quote_valid[rows]
            daily = np.where(legal, raw_intraday[rows], 0).sum(1) / np.maximum(legal.sum(1), 1)
            record[f"legal_universe_mean_open_close_log_return_{side}_bps"] = float(daily.mean() * 1e4)
        periods[label] = record
    variants = []
    proxy_cache = {}
    for variant in sorted(args.root.iterdir()):
        archive = variant / "fold_11/test_backtest.npz"
        if not archive.is_file():
            continue
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        with np.load(archive, allow_pickle=False) as z:
            if z["carry_universe"].tolist() != symbols:
                raise ValueError("artifact and cached source universes differ")
            if z["carry_release_id"].item() != physical["release_id"]:
                raise ValueError("artifact and physical source release differ")
            row = np.searchsorted(dates, z["dates"])
            if not np.array_equal(dates[row], z["dates"]):
                raise ValueError("artifact dates do not belong to the cache")
            w = z["requested_weights_history"]
            nav = np.r_[z["carry_segment_initial_nav"].item(), z["minute_nav"][:-1, -1]]
            cash = np.abs(w) * nav[:, None]
            below = (cash > 0) & (cash < 1000 * opening[row])
            direction_permission = np.where(w < 0, sell[row] & short[row], buy[row])
            legal = eligible[row] & tradable[row] & direction_permission
            for date in z["dates"].astype("datetime64[D]"):
                key = str(date)
                if key not in proxy_cache:
                    with np.load(args.physical_cache / f"session-{key}.npz") as source:
                        proxy_cache[key] = source["entry"][:, 2] > 0
            proxy = np.stack([proxy_cache[str(date)] for date in z["dates"].astype("datetime64[D]")])
            gross = np.abs(w).sum()
            variants.append({
                "variant": variant.name, "archive_sha256": digest,
                "requested_nonzero_positions": int((w != 0).sum()),
                "requested_positions_below_one_lot_fraction": float(below.sum() / max((w != 0).sum(), 1)),
                "requested_gross_below_one_lot_fraction": float(np.abs(w)[below].sum() / max(gross, 1e-30)),
                "requested_gross_blocked_by_open_permissions_fraction": float(np.abs(w)[~legal].sum() / max(gross, 1e-30)),
                "requested_gross_daily_proxy_fraction": float(np.abs(w)[proxy].sum() / max(gross, 1e-30)),
                "requested_short_fraction": float((-np.minimum(w, 0)).sum() / max(gross, 1e-30)),
            })
        if hashlib.sha256(archive.read_bytes()).hexdigest() != digest:
            raise RuntimeError("source artifact changed during audit")
    output = {
        "panel_generation": str(generation), "source_release": physical["release_id"],
        "physical_mode_counts": physical["mode_counts"], "periods": periods, "variants": variants,
        "interpretation": [
            "Raw universe open-close averages are descriptive, before fees/capacity; not strategy returns.",
            "Sub-lot diagnostics use saved requested weights and panel official-open sizing; not an attribution of all actual rejected fills.",
            "2026 was already inspected; do not select hyperparameters on these diagnostics.",
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps(output, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
