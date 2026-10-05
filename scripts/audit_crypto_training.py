#!/usr/bin/env python3
"""Audit saved crypto training artifacts without loading models or running training.

Only first-test-year rows enter the diagnostic stitch. Each fold starts a fresh
account: this is not a continuous live-inventory replay. Fee addback is an
accounting sensitivity on the saved path, never a zero-fee counterfactual run.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np


class _Sources:
    def __init__(self, root: Path):
        self.root = root
        self.digests: dict[str, str] = {}

    def read(self, relative: str) -> bytes:
        path = self.root / relative
        payload = path.read_bytes()
        digest = hashlib.sha256(payload).hexdigest()
        if relative in self.digests and self.digests[relative] != digest:
            raise ValueError(f"source changed during audit: {relative}")
        self.digests[relative] = digest
        return payload

    def json(self, relative: str) -> Any:
        return json.loads(self.read(relative))

    def verify(self) -> None:
        for relative in tuple(self.digests):
            self.read(relative)


def _finite_array(value: Any, name: str, ndim: int) -> np.ndarray:
    values = np.asarray(value, dtype=np.float64)
    if values.ndim != ndim or not values.size or not np.isfinite(values).all():
        raise ValueError(f"{name} must be a nonempty finite {ndim}-D array")
    return values


def _dates(value: Any) -> np.ndarray:
    raw = np.asarray(value)
    if raw.ndim != 1 or not raw.size or raw.dtype.kind not in {"M", "U", "S"}:
        raise ValueError("dates must be a nonempty 1-D date array")
    days = raw.astype("datetime64[D]")
    if np.isnat(days).any() or np.any(days[1:] <= days[:-1]):
        raise ValueError("dates must be valid, strictly increasing and unique per UTC day")
    return days


def return_metrics(log_returns: Any) -> dict[str, float | int]:
    """Include initial NAV=1 in the running high, including a first-day loss."""
    values = _finite_array(log_returns, "log_returns", 1)
    with np.errstate(over="ignore", invalid="ignore"):
        cumulative = values.cumsum()
    if not np.isfinite(cumulative).all():
        raise ValueError("cumulative log returns overflow float64")
    peak = np.maximum.accumulate(np.r_[0.0, cumulative])[1:]
    with np.errstate(over="raise", invalid="raise"):
        try:
            total = float(np.expm1(cumulative[-1]))
        except FloatingPointError as error:
            raise ValueError("cumulative return overflows float64") from error
    return {
        "rows": int(values.size),
        "cumulative_return": total,
        "max_drawdown_including_initial_nav": float(np.expm1(cumulative - peak).min()),
        "annualized_mean_log_return_365": float(values.mean() * 365.0),
    }


def _weight_metrics(weights: np.ndarray) -> dict[str, float]:
    absolute = np.abs(weights)
    gross = absolute.sum(axis=1)
    return {
        "gross_mean": float(gross.mean()),
        "gross_max": float(gross.max()),
        "net_mean": float(weights.sum(axis=1).mean()),
        "long_gross_mean": float(np.maximum(weights, 0).sum(axis=1).mean()),
        "short_gross_mean": float(-np.minimum(weights, 0).sum(axis=1).mean()),
        "max_abs_weight_daily_mean": float(absolute.max(axis=1).mean()),
        "max_abs_weight": float(absolute.max()),
        "flat_row_fraction": float(np.mean(gross == 0)),
    }


def analyze_backtest(
    arrays: Mapping[str, Any], *, first_test_year: int, equal_side_fee_rate: float | None
) -> tuple[dict[str, Any], np.ndarray, np.ndarray]:
    dates = _dates(arrays["dates"])
    returns = _finite_array(arrays["strategy_returns"], "strategy_returns", 1)
    turnovers = _finite_array(arrays["turnovers"], "turnovers", 1)
    requested = _finite_array(arrays["requested_weights_history"], "requested_weights_history", 2)
    executed = _finite_array(arrays["weights_history"], "weights_history", 2)
    if requested.shape != executed.shape or any(
        len(a) != len(dates) for a in (returns, turnovers, requested, executed)
    ):
        raise ValueError("backtest date/return/turnover/weight shapes disagree")
    if np.any(turnovers < 0):
        raise ValueError("turnovers must be nonnegative")
    years = dates.astype("datetime64[Y]").astype(int) + 1970
    if int(years[0]) != first_test_year:
        raise ValueError("saved backtest does not start in the declared first test year")
    selected = years == first_test_year
    dates, returns, turnovers = dates[selected], returns[selected], turnovers[selected]
    requested, executed = requested[selected], executed[selected]
    expected_days = int((dates[-1] - dates[0]) / np.timedelta64(1, "D")) + 1
    result: dict[str, Any] = {
        "first_test_year": first_test_year,
        "date_start": str(dates[0]),
        "date_end": str(dates[-1]),
        "missing_calendar_days_inside_window": expected_days - len(dates),
        "full_calendar_year": bool(
            str(dates[0]) == f"{first_test_year}-01-01"
            and str(dates[-1]) == f"{first_test_year}-12-31"
            and expected_days == len(dates)
        ),
        **return_metrics(returns),
        "turnover_mean": float(turnovers.mean()),
        "requested_weights": _weight_metrics(requested),
        "executed_weights": _weight_metrics(executed),
        "requested_executed_abs_gap_mean": float(np.abs(requested - executed).sum(axis=1).mean()),
        "same_path_fee_addback": None,
    }
    if equal_side_fee_rate is not None:
        if not math.isfinite(equal_side_fee_rate) or equal_side_fee_rate < 0:
            raise ValueError("equal-side fee rate must be finite and nonnegative")
        with np.errstate(over="raise", invalid="raise"):
            try:
                added = np.logaddexp(returns, np.log(
                    equal_side_fee_rate * turnovers,
                    out=np.full_like(turnovers, -np.inf),
                    where=equal_side_fee_rate * turnovers > 0,
                ))
            except FloatingPointError as error:
                raise ValueError("same-path fee addback overflow") from error
        result["same_path_fee_addback"] = {
            "interpretation": "saved-path accounting sensitivity; NOT a zero-fee rerun; funding retained",
            "fee_rate_per_side": equal_side_fee_rate,
            "simple_fee_fraction_sum_on_daily_nav": float((equal_side_fee_rate * turnovers).sum()),
            "annualized_log_fee_drag_365": float((added - returns).mean() * 365.0),
            **return_metrics(added),
        }
    return result, dates, returns


def analyze_epoch_curve(
    rows: Sequence[Mapping[str, Any]], *, epochs_cap: int, early_stop_ratio: float,
    grad_clip_norm: float | None = None,
) -> dict[str, Any]:
    if not rows or epochs_cap <= 0 or not math.isfinite(early_stop_ratio) or early_stop_ratio < 0:
        raise ValueError("epoch curve and configured epoch limit must be valid and nonempty")
    epochs = [row.get("epoch") for row in rows]
    if any(isinstance(e, bool) or not isinstance(e, int) or e < 1 for e in epochs):
        raise ValueError("epoch identifiers must be positive integers")
    if any(right <= left for left, right in zip(epochs, epochs[1:])):
        raise ValueError("epochs must be strictly increasing without duplicates")
    for row in rows:
        for key in ("train_loss", "val_mean", "test_mean"):
            value = row.get(key)
            if value is not None and (not isinstance(value, (int, float)) or not math.isfinite(value)):
                raise ValueError(f"non-finite or invalid epoch {key}")
    eligible = [row for row in rows if row.get("val_mean") is not None]
    if not eligible:
        raise ValueError("epoch curve contains no finite validation observation")
    best = min(eligible, key=lambda row: row["val_mean"])
    snapshot_keys = ("epoch", "train_loss", "val_mean", "test_mean", "no_improve")
    steps = [row.get("train_optimizer_steps") for row in rows]
    for value in steps:
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, (int, float))
            or not math.isfinite(value) or value < 0 or value != int(value)
        ):
            raise ValueError("optimizer step observations must be nonnegative integers")
    patience = math.ceil(epochs_cap * early_stop_ratio)
    no_improve = rows[-1].get("no_improve")
    reached_patience = bool(patience > 0 and no_improve is not None and no_improve >= patience)
    if grad_clip_norm is not None and (not math.isfinite(grad_clip_norm) or grad_clip_norm < 0):
        raise ValueError("grad_clip_norm must be finite and nonnegative")
    observed_norms, single_update_norms, learning_rates = [], [], []
    zero_grad_rows, dead_rows = [], []
    for row in rows:
        norm = row.get("train_grad_norm_before_clip_mean")
        # A default zero without a measurement is missing evidence, not proof
        # of zero gradients. Multi-update means cannot count clipped updates.
        if row.get("train_grad_norm_measured") and row.get("train_grad_norm_observations", 0) > 0:
            if not isinstance(norm, (int, float)) or not math.isfinite(norm) or norm < 0:
                raise ValueError("measured gradient norm must be finite and nonnegative")
            observed_norms.append(float(norm))
            if row.get("train_optimizer_steps") == 1 and row["train_grad_norm_observations"] == 1:
                single_update_norms.append(float(norm))
        lr = row.get("lr")
        if lr is not None:
            if not isinstance(lr, (int, float)) or not math.isfinite(lr) or lr < 0:
                raise ValueError("learning rate must be finite and nonnegative")
            learning_rates.append(float(lr))
        if row.get("train_zero_grad_batches") is not None:
            zero_grad_rows.append(row["train_zero_grad_batches"] > 0)
        if row.get("train_first_dead_portfolio_batch") is not None:
            dead_rows.append(row["train_first_dead_portfolio_batch"] >= 0)
    gradient_telemetry = {
        "measured_epoch_rows": len(observed_norms),
        "missing_epoch_rows": len(rows) - len(observed_norms),
        "epoch_mean_norm_min": min(observed_norms) if observed_norms else None,
        "epoch_mean_norm_median": float(np.median(observed_norms)) if observed_norms else None,
        "epoch_mean_norm_max": max(observed_norms) if observed_norms else None,
        "configured_clip_norm": grad_clip_norm,
        "single_update_observations": len(single_update_norms),
        "single_updates_above_clip": (
            sum(norm > grad_clip_norm for norm in single_update_norms)
            if grad_clip_norm is not None and grad_clip_norm > 0 else None
        ),
        "epochs_with_zero_gradient_batch": sum(zero_grad_rows) if zero_grad_rows else None,
        "zero_gradient_telemetry_missing_rows": len(rows) - len(zero_grad_rows),
        "epochs_with_dead_account": sum(dead_rows) if dead_rows else None,
        "dead_account_telemetry_missing_rows": len(rows) - len(dead_rows),
        "interpretation": "Nonzero/clipped global gradients do not prove useful output-head gradients or generalization.",
    }
    return {
        "recorded_epochs": len(rows), "first_epoch": epochs[0], "last_epoch": epochs[-1],
        "configured_epochs_cap": epochs_cap,
        "early_stop_patience": patience,
        "end_consistent_with_early_stopping": reached_patience,
        "reached_configured_epochs_cap": epochs[-1] >= epochs_cap,
        "epoch_coverage_contiguous": epochs == list(range(epochs[0], epochs[-1] + 1)),
        "minimum_observed_val_loss_epoch": {key: best.get(key) for key in snapshot_keys},
        "last_epoch_values": {key: rows[-1].get(key) for key in snapshot_keys},
        "optimizer_steps_observed_total": int(sum(s for s in steps if s is not None)),
        "optimizer_steps_missing_rows": sum(s is None for s in steps),
        "optimizer_steps_distinct": sorted({int(s) for s in steps if s is not None}),
        "gradient_telemetry": gradient_telemetry,
        "learning_rate_distinct": sorted(set(learning_rates)),
        "learning_rate_missing_rows": len(rows) - len(learning_rates),
        "selection_note": "minimum curve validation is descriptive; saved fold best_val_loss controls checkpoint selection; test is audit-only",
    }


def audit_root(root: Path) -> dict[str, Any]:
    root = root.resolve(strict=True)
    sources = _Sources(root)
    summary = sources.json("summary.json")
    manifest = sources.json("run_manifest.json")
    progress = sources.json("progress.json")
    if not isinstance(summary, list) or not summary:
        raise ValueError("summary.json must contain a nonempty fold list")
    if manifest.get("execution_mode") != "crypto_perpetual":
        raise ValueError("audit requires a crypto_perpetual artifact root")
    cfg = manifest["configuration"]
    trading, training = cfg["trading"], cfg["training"]
    buy, sell = float(trading["buy_fee_rate"]), float(trading["sell_fee_rate"])
    if any(not math.isfinite(fee) or fee < 0 for fee in (buy, sell)):
        raise ValueError("manifest fee rates must be finite and nonnegative")
    equal_fee = buy if buy == sell else None
    folds, stitch_dates, stitch_returns = [], [], []
    seen_folds: set[int] = set()
    for record in sorted(summary, key=lambda row: row["fold_id"]):
        fold_id = record["fold_id"]
        if isinstance(fold_id, bool) or not isinstance(fold_id, int) or fold_id < 1:
            raise ValueError("fold identifiers must be positive integers")
        if fold_id in seen_folds:
            raise ValueError(f"duplicate fold id: {fold_id}")
        seen_folds.add(fold_id)
        for key in ("train_years", "val_years", "test_years"):
            years = record[key]
            if not isinstance(years, list) or not years or any(
                isinstance(year, bool) or not isinstance(year, int) or not 1 <= year <= 9999
                for year in years
            ) or years != sorted(set(years)):
                raise ValueError(f"{key} must contain unique increasing integer years")
        first_year = min(record["test_years"])
        selection_not_prior = any(year >= first_year for year in record["train_years"] + record["val_years"])
        folder = f"fold_{fold_id:02d}"
        complete = sources.json(f"{folder}/fold_complete.json")
        for key in ("fold_id", "train_years", "val_years", "test_years"):
            if complete.get(key) != record.get(key):
                raise ValueError(f"summary/fold completion identity mismatch: {folder}/{key}")
        if complete.get("status") != "complete":
            raise ValueError(f"fold is not recorded complete: {folder}")
        with np.load(io.BytesIO(sources.read(f"{folder}/test_backtest.npz")), allow_pickle=False) as archive:
            if "execution_mode" in archive and archive["execution_mode"].item() != "crypto_perpetual":
                raise ValueError(f"backtest execution mode is not crypto_perpetual: {folder}")
            saved_dates = _dates(archive["dates"])
            saved_years = saved_dates.astype("datetime64[Y]").astype(int) + 1970
            if not set(saved_years.tolist()).issubset(record["test_years"]):
                raise ValueError(f"backtest dates exceed declared test years: {folder}")
            stats, dates, returns = analyze_backtest(
                archive, first_test_year=first_year, equal_side_fee_rate=equal_fee
            )
        group = "train_" + "-".join(str(year) for year in record["train_years"])
        rows = [json.loads(line) for line in sources.read(f"{group}/epoch_curve.jsonl").splitlines() if line.strip()]
        folds.append({
            "fold_id": fold_id, "train_years": record["train_years"],
            "val_years": record["val_years"], "test_years": record["test_years"],
            "saved_best_val_loss": record["best_val_loss"],
            "saved_val_cumulative_return": record.get("val_metrics", {}).get("cumulative_return"),
            "included_in_oos_first_year_stitch": not selection_not_prior,
            "exclusion_reason": "first test year overlaps or precedes train/validation; not held out" if selection_not_prior else None,
            "training": analyze_epoch_curve(
                rows, epochs_cap=int(training["epochs"]),
                early_stop_ratio=float(training.get("early_stopping_no_improve_ratio", 0.0)),
                grad_clip_norm=(float(training["grad_clip_norm"]) if training.get("grad_clip_norm") is not None else None),
            ),
            "first_test_year_metrics": stats,
        })
        if not selection_not_prior:
            stitch_dates.extend(dates.tolist())
            stitch_returns.extend(returns.tolist())
    stitch: dict[str, Any] | None = None
    if stitch_dates:
        dates = _dates(np.asarray(stitch_dates, dtype="datetime64[D]"))
        stitch = {
            "account_contract": "reset account at each fold; no inventory transfer or fold-transition closing cost replay",
            "date_start": str(dates[0]), "date_end": str(dates[-1]),
            "fold_ids": [fold["fold_id"] for fold in folds if fold["included_in_oos_first_year_stitch"]],
            **return_metrics(stitch_returns),
        }
    sources.verify()
    return {
        "audit_schema_version": 1, "root": str(root),
        "lifecycle_state_recorded": progress.get("state"),
        "lifecycle_note": "recorded state and inspected completion markers; not a full lifecycle acceptance gate",
        "configuration_fingerprint": manifest.get("configuration_fingerprint"),
        "dataset_fingerprint": manifest.get("dataset_fingerprint"),
        "decision_clock": manifest.get("decision_clock"),
        "execution_clock": manifest.get("execution_clock"),
        "contract_versions": manifest.get("contract_versions"),
        "folds": folds, "oos_first_year_reset_account_stitch": stitch,
        "caveats": [
            "Each first test calendar year is inspected once; full future test tails are never compounded together.",
            "Train/validation-overlap years are excluded from OOS stitching but retained as experimental diagnostics.",
            "OOS means held out from that fold's fitting/selection; already inspected test years are not fresh model-selection evidence.",
            "Requested and executed weight snapshots share the saved ledger denominator; gross short exposure is not spendable cash.",
            "Fee addback holds the saved return/turnover path fixed; removing fees would change NAV, capacity and policy trajectory.",
            "Unequal side fees cannot be reconstructed from aggregate turnover alone; addback is omitted in that case.",
        ],
        "sources_sha256": sources.digests,
    }


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path, help="Existing crypto training artifact root")
    parser.add_argument("--output", type=Path, help="Optional stable JSON path outside the original artifact root")
    args = parser.parse_args(argv)
    if args.output is not None and args.output.resolve().is_relative_to(args.root.resolve()):
        parser.error("--output must be outside the original artifact root")
    result = audit_root(args.root)
    rendered = json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


if __name__ == "__main__":
    main()
