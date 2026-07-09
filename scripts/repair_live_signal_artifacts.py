from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.live.portfolio_state import portfolio_risk_summary
from stockagent.live.signal_engine import LiveSignalResult, _write_text_artifacts, write_live_weights_history


WEIGHT_COLUMNS = ("current_weight", "model_weight", "target_weight", "delta_weight", "abs_delta_weight")


def _finite_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except Exception:
        return default
    return number if np.isfinite(number) else default


def _repair_frame(path: Path) -> tuple[pl.DataFrame, int]:
    df = pl.read_parquet(path)
    if "tradable" not in df.columns:
        return df, 0

    bad_expr = ~pl.col("tradable")
    for column in ("current_weight", "model_weight", "target_weight"):
        if column in df.columns:
            bad_expr = bad_expr & (pl.col(column).abs() > 1e-9)
            break

    bad_count = df.filter(
        (~pl.col("tradable"))
        & (
            (pl.col("current_weight").abs() > 1e-9 if "current_weight" in df.columns else pl.lit(False))
            | (pl.col("model_weight").abs() > 1e-9 if "model_weight" in df.columns else pl.lit(False))
            | (pl.col("target_weight").abs() > 1e-9 if "target_weight" in df.columns else pl.lit(False))
            | (pl.col("delta_weight").abs() > 1e-9 if "delta_weight" in df.columns else pl.lit(False))
        )
    ).height

    if "position_status" not in df.columns:
        df = df.with_columns(
            pl.when(pl.col("tradable")).then(pl.lit("active")).otherwise(pl.lit("untradable")).alias("position_status")
        )

    updates: list[pl.Expr] = []
    for column in WEIGHT_COLUMNS:
        if column in df.columns:
            updates.append(pl.when(pl.col("tradable")).then(pl.col(column)).otherwise(0.0).alias(column))
    if "portfolio_contribution" in df.columns:
        updates.append(pl.when(pl.col("tradable")).then(pl.col("portfolio_contribution")).otherwise(0.0).alias("portfolio_contribution"))
    if "stock_return" in df.columns:
        updates.append(
            pl.when(pl.col("tradable"))
            .then(pl.col("stock_return"))
            .otherwise(
                pl.when(pl.col("price_return").is_finite() if "price_return" in df.columns else pl.lit(False))
                .then(0.0)
                .otherwise(None)
            )
            .alias("stock_return")
        )
    if "action" in df.columns:
        updates.append(pl.when(pl.col("tradable")).then(pl.col("action")).otherwise(pl.lit("HOLD")).alias("action"))
    if "constraint" in df.columns:
        updates.append(
            pl.when(pl.col("tradable")).then(pl.col("constraint")).otherwise(pl.lit("not_tradable")).alias("constraint")
        )
    if "decision_reason" in df.columns:
        updates.append(
            pl.when(pl.col("tradable"))
            .then(pl.col("decision_reason"))
            .otherwise(pl.lit("neutral_score; model_flat; action_hold; not_tradable"))
            .alias("decision_reason")
        )
    updates.append(
        pl.when(pl.col("tradable")).then(pl.col("position_status")).otherwise(pl.lit("untradable")).alias("position_status")
    )
    df = df.with_columns(updates)
    return df, int(bad_count)


def _top_positions(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    sorted_rows = sorted(
        rows,
        key=lambda row: (
            abs(_finite_float(row.get("target_weight"))),
            abs(_finite_float(row.get("current_weight"))),
            abs(_finite_float(row.get("delta_weight"))),
        ),
        reverse=True,
    )
    out: list[dict[str, Any]] = []
    for row in sorted_rows[: max(0, int(limit))]:
        weight = _finite_float(row.get("target_weight"))
        out.append(
            {
                "symbol": str(row.get("symbol") or ""),
                "name": str(row.get("name") or ""),
                "weight": weight,
                "abs_weight": abs(weight),
                "current_weight": _finite_float(row.get("current_weight")),
                "target_weight": weight,
                "delta_weight": _finite_float(row.get("delta_weight")),
                "current_price": row.get("current_price"),
                "model_weight": _finite_float(row.get("model_weight")),
                "tradable": bool(row.get("tradable", True)),
                "can_buy": bool(row.get("can_buy", True)),
                "can_sell": bool(row.get("can_sell", True)),
                "position_status": str(row.get("position_status") or ""),
            }
        )
    return out


def _risk_warnings(summary: dict[str, Any], target_risk: dict[str, float]) -> list[str]:
    warnings: list[str] = []
    turnover = _finite_float(summary.get("turnover"))
    if turnover > 1.0:
        warnings.append(f"turnover {turnover * 100:.1f}% exceeds 100.0%")
    top = float(target_risk.get("top_abs_weight", 0.0))
    if top > 0.10:
        warnings.append(f"top weight {top * 100:.1f}% exceeds 10.0%")
    gross = float(target_risk.get("gross", 0.0))
    if gross > 1.05:
        warnings.append(f"gross exposure {gross * 100:.1f}% exceeds 105.0%")
    return warnings


def _repair_signal_dir(signal_dir: Path, *, dry_run: bool = False) -> dict[str, Any] | None:
    decision_path = signal_dir / "decision_explanations.parquet"
    weights_path = signal_dir / "target_weights.parquet"
    summary_path = signal_dir / "summary.json"
    if not decision_path.exists() or not weights_path.exists() or not summary_path.exists():
        return None

    decision_df, decision_bad = _repair_frame(decision_path)
    weights_df, weights_bad = _repair_frame(weights_path)
    changed = decision_bad + weights_bad

    rebalance_path = signal_dir / "rebalance.parquet"
    rebalance_df: pl.DataFrame | None = None
    if rebalance_path.exists():
        rebalance_df, rebalance_bad = _repair_frame(rebalance_path)
        if "abs_delta_weight" in rebalance_df.columns:
            rebalance_df = rebalance_df.filter(pl.col("abs_delta_weight") > 1e-9)
        changed += rebalance_bad

    if changed <= 0:
        return {"path": str(signal_dir), "changed_rows": 0}

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    weight_rows = weights_df.to_dicts()
    decision_rows = decision_df.to_dicts()
    rebalance_rows = rebalance_df.to_dicts() if rebalance_df is not None else []

    target_weights = np.array([_finite_float(row.get("target_weight")) for row in weight_rows], dtype=np.float64)
    current_weights = np.array([_finite_float(row.get("current_weight")) for row in weight_rows], dtype=np.float64)
    target_risk = portfolio_risk_summary(target_weights)
    current_risk = portfolio_risk_summary(current_weights)
    top_limit = len(summary.get("top_positions") or []) or 20

    summary["current_gross"] = float(current_risk["gross"])
    summary["target_gross"] = float(target_risk["gross"])
    summary["current_risk"] = current_risk
    summary["target_risk"] = target_risk
    summary["risk_warnings"] = _risk_warnings(summary, target_risk)
    summary["top_positions"] = _top_positions(weight_rows, top_limit)
    if "model_explanation" in summary and isinstance(summary["model_explanation"], dict):
        summary["model_explanation"]["decision_rows"] = int(len(decision_rows))
        summary["model_explanation"]["actionable_decision_rows"] = int(
            sum(1 for row in decision_rows if str(row.get("action") or "").upper() != "HOLD")
        )
    summary["repair_note"] = "untradable symbols zeroed; positions assumed liquidated on previous tradable day"

    if not dry_run:
        weights_df.write_parquet(weights_path)
        decision_df.write_parquet(decision_path)
        if rebalance_df is not None:
            rebalance_df.write_parquet(rebalance_path)
        summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        _write_text_artifacts(
            LiveSignalResult(
                summary=summary,
                weights_rows=weight_rows,
                rebalance_rows=rebalance_rows,
                decision_rows=decision_rows,
                message="",
                output_dir=str(signal_dir),
            ),
            signal_dir,
        )
    return {"path": str(signal_dir), "changed_rows": changed}


def _sync_live_weights(signal_dirs: list[Path], *, fold_dir: Path, dry_run: bool = False) -> int:
    if dry_run:
        return 0
    count = 0
    for signal_dir in sorted(signal_dirs):
        summary_path = signal_dir / "summary.json"
        weights_path = signal_dir / "target_weights.parquet"
        if not summary_path.exists() or not weights_path.exists():
            continue
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        rows = pl.read_parquet(weights_path).to_dicts()
        if write_live_weights_history(fold_dir, summary, rows):
            count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", action="append", default=["artifacts/live_signals/tw", "artifacts/markets/tw/live_signals/tw"])
    parser.add_argument("--fold-dir", default="artifacts/markets/tw/fold_25")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    signal_dirs: list[Path] = []
    for root_text in args.root:
        root = Path(root_text)
        if root.exists():
            signal_dirs.extend(path.parent for path in root.glob("*/*/decision_explanations.parquet"))
    unique_dirs = sorted(set(signal_dirs))

    results = []
    for signal_dir in unique_dirs:
        result = _repair_signal_dir(signal_dir, dry_run=bool(args.dry_run))
        if result is not None and int(result.get("changed_rows") or 0) > 0:
            results.append(result)
            print(f"repair {result['changed_rows']:5d} rows  {result['path']}")
    synced = _sync_live_weights(unique_dirs, fold_dir=Path(args.fold_dir), dry_run=bool(args.dry_run))
    print(json.dumps({"signals_scanned": len(unique_dirs), "signals_repaired": len(results), "live_weights_synced": synced}, ensure_ascii=False))


if __name__ == "__main__":
    main()
