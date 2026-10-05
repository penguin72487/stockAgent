#!/usr/bin/env python3
"""Read-only fold audit: selection, execution exposure and normalization state.

Checkpoint files must be trusted project artifacts. No training or source
artifact modification is performed. Test results are descriptive, never a
checkpoint-selection input.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
import sys

import numpy as np
import torch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from stockagent.training.lifecycle import validate_completed_training_artifacts
from stockagent.backtest.report import compute_metrics
from stockagent.backtest.simulator import BacktestResult


def sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def audit_variant(root: Path, fold_id: int) -> tuple[dict, dict, dict]:
    fold = root / f"fold_{fold_id:02d}"
    metrics = read_json(fold / "metrics.json")
    group_name = "train_" + "-".join(map(str, metrics["train_years"]))
    group = root / group_name
    inputs = [
        root / "run_manifest.json", root / "progress.json",
        fold / "metrics.json", fold / "checkpoint_best.pt",
        fold / "deployment_test_backtest.npz", group / "checkpoint_last.pt",
        group / "epoch_curve.jsonl", group / "causal_feature_rms_normalization.json",
    ]
    init_path = group / "pretrained_initialization.json"
    if init_path.exists():
        inputs.append(init_path)
    before = {str(p): sha256(p) for p in inputs}
    conformance = validate_completed_training_artifacts(
        root, fold_ids=[fold_id], group_names=[group_name],
    )
    rows = [json.loads(line) for line in (group / "epoch_curve.jsonl").read_text().splitlines()]
    by_epoch = {row["epoch"]: row for row in rows}
    if len(by_epoch) != len(rows):
        raise ValueError(f"duplicate epochs in {group}")
    ck = torch.load(fold / "checkpoint_best.pt", map_location="cpu", weights_only=False)
    last = torch.load(group / "checkpoint_last.pt", map_location="cpu", weights_only=False)
    config = read_json(root / "run_manifest.json")["configuration"]
    best_epoch = int(ck["epoch"])
    selected = by_epoch.get(best_epoch, {})  # epoch-zero cash is a valid checkpoint
    minimum = min(rows, key=lambda row: row["val_mean"])
    fitted = read_json(group / "causal_feature_rms_normalization.json")
    state_key = "candle_encoder.causal_feature_rms_scale"
    active_key = "candle_encoder.causal_feature_active_mask"
    best_scale = ck["model_state_dict"][state_key]
    last_scale = last["model_state_dict"][state_key]
    target_scale = torch.tensor([f["rms_scale"] for f in fitted["features"]])
    init = read_json(init_path) if init_path.exists() else {}
    resume_messages = sorted({
        line.strip() for path in root.glob("*.log")
        for line in path.read_text(errors="replace").splitlines()
        if "resumed from epoch" in line
    })
    result = {
        "variant": root.name,
        "fold_id": fold_id,
        "lifecycle_passed": not conformance.missing and not conformance.invalid,
        "epochs": len(rows),
        "selected_epoch": best_epoch,
        "raw_minimum_val_epoch": minimum["epoch"],
        "selected_val_loss": float(ck["best_val_loss"]),
        "train_loss_at_selected_epoch": selected.get("train_loss"),
        "last_train_loss": rows[-1]["train_loss"],
        "last_val_loss": rows[-1]["val_mean"],
        "last_lr_fraction_of_peak": rows[-1]["lr"] / config["training"]["learning_rate"],
        "zero_gradient_epochs": sum(r.get("train_zero_grad_batches", 0) > 0 for r in rows),
        "dead_account_epochs": sum(r.get("train_portfolio_final_alive", 1) == 0 for r in rows),
        "resumed": bool(resume_messages),
        "copied_pretrained_tensors": init.get("copied_backbone_tensor_count", 0),
        "best_vs_fitted_rms_max_difference": float((best_scale - target_scale).abs().max()),
        "best_vs_last_rms_max_difference": float((best_scale - last_scale).abs().max()),
    }
    for split in ("val", "test"):
        for key in ("cumulative_return", "sharpe", "max_drawdown", "turnover"):
            result[f"{split}_{key}"] = metrics[f"{split}_metrics"][key]
    with np.load(fold / "deployment_test_backtest.npz") as archive:
        w = archive["requested_weights_history"].astype(np.float64)
        gross = np.abs(w).sum(axis=1)
        squared = np.square(w).sum(axis=1)
        effective = np.divide(gross**2, squared, out=np.zeros_like(gross), where=squared > 0)
        returns = archive["strategy_returns"].astype(np.float64)
        result.update({
            "deployment_rows": len(returns),
            "requested_gross_median": float(np.median(gross)),
            "requested_gross_std": float(np.std(gross)),
            "requested_effective_stock_count_median": float(np.median(effective)),
            "requested_short_share": float(np.maximum(-w, 0).sum() / max(np.abs(w).sum(), 1e-30)),
            "executed_turnover_mean": float(archive["turnovers"].mean()),
            "nonzero_turnover_days": int(np.count_nonzero(archive["turnovers"])),
            "settlement_default_days": int(np.count_nonzero(archive["settlement_default"])),
        })
        deployment_metrics = compute_metrics(BacktestResult(
            strategy_returns=returns,
            benchmark_returns=archive["benchmark_returns"],
            turnovers=archive["turnovers"],
            weights_history=archive["weights_history"],
            execution_mode="tw_day_trade",
        ))
        for key in ("cumulative_return", "sharpe", "max_drawdown"):
            result[f"deployment_{key}"] = deployment_metrics[key]
    details = {
        "configuration": config,
        "checkpoint_fingerprints": ck["experiment_manifest"]["fingerprints"],
        "fitted_normalizer_fingerprint": fitted["normalizer_fingerprint"],
        "best_effective_scale": best_scale.tolist(),
        "last_effective_scale": last_scale.tolist(),
        "best_active_mask": ck["model_state_dict"][active_key].tolist(),
        "last_active_mask": last["model_state_dict"][active_key].tolist(),
        "resume_messages": resume_messages,
        "pretrained_source_sha256": init.get("source_checkpoint_sha256"),
        "incompatible_pretrained_tensors": init.get("incompatible_source_tensors", []),
        "lifecycle_missing": [str(x) for x in conformance.missing],
        "lifecycle_invalid": [str(x) for x in conformance.invalid],
    }
    if any(sha256(p) != before[str(p)] for p in inputs):
        raise RuntimeError(f"source artifacts changed during audit: {root}")
    return result, details, before


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--fold", type=int, default=11)
    args = parser.parse_args()
    if args.output_dir.resolve().is_relative_to(args.root.resolve()):
        raise ValueError("audit output must be outside the preserved experiment root")
    summaries, details, hashes = [], {}, {}
    for path in sorted(args.root.iterdir()):
        if not (path / f"fold_{args.fold:02d}" / "metrics.json").is_file():
            continue
        summary, detail, receipts = audit_variant(path, args.fold)
        summaries.append(summary)
        details[path.name] = detail
        hashes.update(receipts)
    if not summaries:
        raise ValueError("no fold results found")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    report = {"scope": "descriptive_single_fold_not_blind_model_selection",
              "summaries": summaries, "details": details, "source_sha256": hashes}
    (args.output_dir / "audit.json").write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    with (args.output_dir / "summary.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summaries[0]))
        writer.writeheader()
        writer.writerows(summaries)
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(1, 3, figsize=(18, 6), layout="constrained")
    labels = [r["variant"] for r in summaries]
    y = np.arange(len(labels))
    for axis, keys, title in [
        (axes[0], ["val_cumulative_return", "test_cumulative_return"], "Return: validation / inspected test"),
        (axes[1], ["val_sharpe", "test_sharpe"], "Sharpe: validation / inspected test"),
    ]:
        for delta, key in zip((-0.18, 0.18), keys):
            axis.barh(y + delta, [r[key] for r in summaries], height=0.35, label=key.split("_")[0])
        axis.set_yticks(y, labels if axis is axes[0] else [])
        axis.set_title(title)
        axis.legend()
    axes[2].barh(y, [r["requested_effective_stock_count_median"] for r in summaries])
    axes[2].set_xscale("log")
    axes[2].set_yticks(y, [])
    axes[2].set_title("Requested effective stock count (test)")
    fig.suptitle("Fold 11 only; historical resumed trajectories include RMS changes")
    fig.savefig(args.output_dir / "comparison.png", dpi=160)
    plt.close(fig)
    print(json.dumps(summaries, indent=2))


if __name__ == "__main__":
    main()
