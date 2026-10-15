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
import torch

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
        out["intraday_nav"] = None
        if "minute_nav" in a.files and "carry_segment_initial_nav" in a.files:
            nav = np.asarray(a["minute_nav"], dtype=np.float64)
            initial = float(a["carry_segment_initial_nav"])
            if nav.shape == (len(dates), 270) and initial > 0 and np.isfinite(nav).all():
                path_nav = np.concatenate(([initial], nav.reshape(-1)))
                peaks = np.maximum.accumulate(path_nav)
                out["intraday_nav"] = {
                    "contract": "saved_270_ordered_minute_marks_with_segment_initial_nav",
                    "max_drawdown": float((path_nav / peaks - 1).min()),
                    "minimum_nav": float(path_nav.min()),
                    "segment_initial_nav": initial,
                }
    return out


def paired_block_bootstrap(r, b, *, repetitions, block_length, seed=42):
    """Paired circular blocks; intervals describe this tape, not new evidence.

    Keep strategy and benchmark rows paired and retain local serial dependence.
    This neither corrects trial selection nor supplies an untouched holdout.
    Metric calculations use exactly the canonical reporting implementation.
    """
    r, b = np.asarray(r), np.asarray(b)
    rng = np.random.default_rng(seed)
    block_length = min(len(r), max(1, int(block_length)))
    starts = rng.integers(0, len(r), size=(repetitions, (len(r) + block_length - 1) // block_length))
    indices = ((starts[:, :, None] + np.arange(block_length)) % len(r)).reshape(repetitions, -1)[:, :len(r)]
    keys = ("cumulative_return", "sharpe", "sortino", "max_drawdown")
    deltas = []
    for idx in indices:
        strategy = return_metrics(r[idx], b[idx])
        benchmark = return_metrics(b[idx], b[idx])
        deltas.append([strategy[k] - benchmark[k] for k in keys])
    values = np.asarray(deltas)
    return {
        "method": "paired_circular_moving_blocks",
        "repetitions": repetitions, "block_length_sessions": block_length, "seed": seed,
        "delta_95_percentile_interval": {k: np.quantile(values[:, i], [.025, .975]).tolist() for i, k in enumerate(keys)},
        "resample_fraction_all_four_deltas_positive": float((values > 0).all(1).mean()),
        "boundary": "Conditional resampling description; not a probability of future dominance, not multiple-trial adjusted, and not new out-of-sample evidence.",
    }


def return_diagnostics(t):
    r, b = t["strategy_returns"], t["benchmark_returns"]
    variance_b = float(np.var(b))
    n_tail = max(1, int(np.ceil(len(r) * .05)))
    split = len(r) // 2
    return {
        "daily_mean_log_return": float(r.mean()),
        "daily_log_return_std": float(r.std()),
        "daily_downside_deviation": float(np.sqrt(np.square(np.minimum(r, 0)).mean())),
        "beta_to_benchmark_daily_log": float(np.mean((r-r.mean()) * (b-b.mean())) / variance_b) if variance_b > 0 else None,
        "daily_tracking_error": float((r-b).std()),
        "worst_five_percent_mean_log_return": float(np.sort(r)[:n_tail].mean()),
        "worst_days": [{"date": str(t["dates"][i]), "net_log_return": float(r[i]), "benchmark_log_return": float(b[i])} for i in np.argsort(r)[:5]],
        "chronological_halves": [
            {"date_start": str(t["dates"][a]), "date_end": str(t["dates"][z-1]),
             "metrics": return_metrics(r[a:z], b[a:z], t["turnovers"][a:z])}
            for a, z in ((0, split), (split, len(r))) if z > a],
    }


def audit(root, historical=None, *, bootstrap_reps=0, block_length=10):
    result = {"contract": "saved_daily_log_return_quality_audit_v2",
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
        checkpoint_path = p.with_name("checkpoint_best.pt")
        checkpoint = torch.load(checkpoint_path, map_location="cpu", weights_only=True)
        selected_epoch = int(checkpoint["epoch"])
        selected_row = next((r for r in rows if int(r["epoch"]) == selected_epoch), {})
        if not all(torch.isfinite(v).all() for v in checkpoint["model_state_dict"].values()
                   if torch.is_tensor(v) and (v.is_floating_point() or v.is_complex())):
            raise ValueError(f"nonfinite checkpoint model: {checkpoint_path}")
        row = {"case": case.name, "fold": saved["fold_id"], "rows": len(t["dates"]),
               "date_start": str(t["dates"][0]), "date_end": str(t["dates"][-1]),
               "epochs": len(rows), "selected_epoch": selected_epoch,
               "curve_raw_min_epoch": best.get("epoch"),
               "selected_curve": {k: selected_row.get(k) for k in ("epoch", "train_loss", "val_mean", "test_mean")},
               "val_metrics": saved["val_metrics"], "test_metrics": metrics,
               "benchmark_metrics": return_metrics(t["benchmark_returns"], t["benchmark_returns"]),
               "max_saved_metric_error": max(errors.values(), default=0),
               "allocation": t["allocation"], "defaults": t["defaults"],
               "intraday_nav": t["intraday_nav"], "return_diagnostics": return_diagnostics(t),
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
                "historical_metrics": return_metrics(old["strategy_returns"][a], old["benchmark_returns"][a], old["turnovers"][a]),
                "current_metrics": return_metrics(t["strategy_returns"][b], t["benchmark_returns"][b], t["turnovers"][b]),
                "max_benchmark_abs_error": float(np.abs(old["benchmark_returns"][a] - t["benchmark_returns"][b]).max()),
                "boundary": "same-date return comparison; account start state and training may differ"}
        deployment = p.with_name("deployment_test_backtest.npz")
        if deployment.exists():
            deployed = tape(deployment)
            row["owned_deployment_prefix"] = {
                "rows": len(deployed["dates"]), "date_start": str(deployed["dates"][0]),
                "date_end": str(deployed["dates"][-1]),
                "metrics": return_metrics(deployed["strategy_returns"], deployed["benchmark_returns"], deployed["turnovers"]),
                "benchmark_metrics": return_metrics(deployed["benchmark_returns"], deployed["benchmark_returns"]),
                "boundary": "Canonical saved ownership prefix; no reset-account suffix splicing.",
            }
        if bootstrap_reps:
            row["paired_block_bootstrap"] = paired_block_bootstrap(
                t["strategy_returns"], t["benchmark_returns"],
                repetitions=bootstrap_reps, block_length=block_length)
        result["cases"].append(row)
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--historical", type=Path)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--bootstrap-reps", type=int, default=0)
    p.add_argument("--block-length", type=int, default=10)
    args = p.parse_args()
    if args.bootstrap_reps < 0 or args.block_length < 1:
        p.error("bootstrap repetitions must be nonnegative and block length positive")
    report = audit(args.root, args.historical, bootstrap_reps=args.bootstrap_reps, block_length=args.block_length)
    report["script_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"cases": len(report["cases"]), "output": str(args.output)}))


if __name__ == "__main__":
    main()
