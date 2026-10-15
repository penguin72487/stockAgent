#!/usr/bin/env python3
"""Read-only, date-aligned diagnosis of saved canonical portfolio return tapes.

This recomputes reporting statistics only. It does not deserialize/reconstruct a
physical account or claim that an archive passes the source-backed replay gate.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.backtest.report import compute_metrics
from stockagent.backtest.simulator import BacktestResult


def return_metrics(r, b, turnovers=None):
    r, b = np.asarray(r, dtype=np.float64), np.asarray(b, dtype=np.float64)
    if r.ndim != 1 or r.shape != b.shape or not len(r):
        raise ValueError("return tapes must have nonempty identical daily axes")
    if not np.isfinite(r).all() or not np.isfinite(b).all():
        raise ValueError("nonfinite saved return tape")
    # Reuse canonical metric definitions, without presenting a fabricated account.
    return compute_metrics(BacktestResult(
        strategy_returns=r, benchmark_returns=b,
        turnovers=np.zeros_like(r) if turnovers is None else turnovers,
        weights_history=np.empty((len(r), 0)), execution_mode="tw_day_trade"))


def tape(path):
    with np.load(path, allow_pickle=False) as a:
        dates = a["dates"].astype("datetime64[D]")
        if len(np.unique(dates)) != len(dates) or not (np.diff(dates).astype(int) > 0).all():
            raise ValueError(f"invalid date axis: {path}")
        out = {k: a[k].copy() for k in ("strategy_returns", "benchmark_returns", "turnovers")}
        out["dates"] = dates
        w = a["requested_weights_history"] if "requested_weights_history" in a.files else None
        out["allocation"] = None if w is None else {
            "requested_long_mean": float(np.maximum(w, 0).sum(1).mean()),
            "requested_short_mean": float(np.maximum(-w, 0).sum(1).mean()),
            "requested_gross_mean": float(np.abs(w).sum(1).mean()),
            "active_names_mean": float((w != 0).sum(1).mean()),
            "max_name_weight": float(np.abs(w).max()),
            "concentration_hhi_mean": float(np.square(w).sum(1).mean()),
        }
        out["defaults"] = int(a["settlement_default"].sum()) if "settlement_default" in a.files else None
    return out


def audit(root, historical=None):
    result = {"contract": "saved_daily_log_return_quality_audit_v1",
              "scope": "reporting statistics, not physical account reconstruction",
              "root": str(root.resolve()), "cases": []}
    old = tape(historical) if historical else None
    reference = None
    for p in sorted(root.glob("*/fold_*/metrics.json")):
        if p.parent.parent.name.startswith("_"):
            continue
        case = p.parent.parent
        saved = json.loads(p.read_text())
        t = tape(p.with_name("test_backtest.npz"))
        metrics = return_metrics(t["strategy_returns"], t["benchmark_returns"], t["turnovers"])
        errors = {k: abs(float(metrics[k]) - float(v)) for k, v in saved["test_metrics"].items()
                  if k in metrics and v is not None}
        if max(errors.values(), default=0) > 1e-8:
            raise ValueError(f"saved/canonical reporting mismatch: {p}: {errors}")
        curves = sorted(case.glob("train_*/epoch_curve.jsonl"))
        rows = [json.loads(line) for line in curves[0].read_text().splitlines()] if curves else []
        val_rows = [r for r in rows if r.get("val_mean") is not None and np.isfinite(r["val_mean"])]
        best = min(val_rows, key=lambda r: r["val_mean"]) if val_rows else {}
        row = {"case": case.name, "fold": saved["fold_id"], "rows": len(t["dates"]),
               "date_start": str(t["dates"][0]), "date_end": str(t["dates"][-1]),
               "epochs": len(rows), "selected_epoch": best.get("epoch"),
               "val_metrics": saved["val_metrics"], "test_metrics": metrics,
               "benchmark_metrics": return_metrics(t["benchmark_returns"], t["benchmark_returns"]),
               "max_saved_metric_error": max(errors.values(), default=0),
               "allocation": t["allocation"], "defaults": t["defaults"],
               "curve_samples": [{k: r.get(k) for k in ("epoch", "train_loss", "val_mean", "test_mean", "lr", "train_grad_norm_before_clip_mean")}
                                 for r in rows if r["epoch"] in {1, 20, 44, 58, 100, best.get("epoch"), len(rows)}]}
        if reference is None:
            reference = t
        else:
            common, a, b = np.intersect1d(reference["dates"], t["dates"], return_indices=True)
            row["benchmark_alignment"] = {"common_rows": len(common), "max_abs_error": float(np.abs(reference["benchmark_returns"][a] - t["benchmark_returns"][b]).max())}
        if old is not None:
            common, a, b = np.intersect1d(old["dates"], t["dates"], return_indices=True)
            row["historical_common_dates"] = {
                "rows": len(common), "date_start": str(common[0]), "date_end": str(common[-1]),
                "historical_metrics": return_metrics(old["strategy_returns"][a], old["benchmark_returns"][a]),
                "current_metrics": return_metrics(t["strategy_returns"][b], t["benchmark_returns"][b]),
                "max_benchmark_abs_error": float(np.abs(old["benchmark_returns"][a] - t["benchmark_returns"][b]).max()),
                "boundary": "same-date return comparison; account start state and training may differ"}
        result["cases"].append(row)
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--historical", type=Path)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    report = audit(args.root, args.historical)
    report["script_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"cases": len(report["cases"]), "output": str(args.output)}))


if __name__ == "__main__":
    main()
