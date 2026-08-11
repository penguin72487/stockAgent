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
from stockagent.live.report_formatter import format_signal_message
from stockagent.live.signal_engine import LiveSignalResult, _write_text_artifacts, write_live_weights_history
from stockagent.data.panel import _tw_limit_price


WEIGHT_COLUMNS = ("current_weight", "model_weight", "target_weight", "delta_weight", "abs_delta_weight")
_TW_SYMBOL_CACHE: dict[str, dict[str, dict[str, float]]] = {}


def _finite_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except Exception:
        return default
    return number if np.isfinite(number) else default


def _date_key(value: Any) -> str:
    text = str(value or "").replace("T", " ").strip()
    return text[:10] if len(text) >= 10 else text


def _action(current_weight: float, target_weight: float, delta_weight: float) -> str:
    if abs(delta_weight) <= 1e-9:
        return "HOLD"
    if abs(target_weight) <= 1e-6 and abs(current_weight) > 1e-6:
        return "EXIT"
    if abs(target_weight) < abs(current_weight) and np.sign(target_weight) == np.sign(current_weight):
        return "REDUCE"
    return "BUY" if delta_weight > 0.0 else "SELL"


def _decision_reason(score: Any, model_weight: Any, action: str, constraint: str) -> str:
    score_value = _finite_float(score, default=np.nan)
    model_value = _finite_float(model_weight, default=0.0)
    if not np.isfinite(score_value):
        score_part = "score_unavailable"
    elif score_value > 0.0:
        score_part = "positive_score"
    elif score_value < 0.0:
        score_part = "negative_score"
    else:
        score_part = "neutral_score"
    if model_value > 0.0:
        model_part = "model_long"
    elif model_value < 0.0:
        model_part = "model_short"
    else:
        model_part = "model_flat"
    pieces = [score_part, model_part, f"action_{str(action).lower()}"]
    if constraint:
        pieces.append(str(constraint))
    return "; ".join(pieces)


def _position_status(tradable: bool, current_weight: float, target_weight: float, model_weight: float) -> str:
    if not tradable:
        return "untradable"
    if abs(target_weight) > 1e-9:
        return "active"
    if abs(model_weight) > 1e-9:
        return "model_flattened_by_constraints"
    return "flat"


def _load_tw_symbol_rows(symbol: str) -> dict[str, dict[str, float]]:
    key = str(symbol).strip()
    cached = _TW_SYMBOL_CACHE.get(key)
    if cached is not None:
        return cached
    path = Path("data_yahoo/tw_stocks") / f"{key}_features.parquet"
    if not path.exists():
        _TW_SYMBOL_CACHE[key] = {}
        return {}
    df = pl.read_parquet(path).sort("date")
    if "date" not in df.columns or "close" not in df.columns:
        _TW_SYMBOL_CACHE[key] = {}
        return {}
    dates = [_date_key(value) for value in df.get_column("date").to_list()]
    rule_dates = df.get_column("date").to_numpy().astype("datetime64[D]", copy=False)
    close_values = df.get_column("close").cast(pl.Float64, strict=False).to_numpy()
    dividends = (
        df.get_column("Dividends").cast(pl.Float64, strict=False).fill_null(0.0).to_numpy()
        if "Dividends" in df.columns
        else np.zeros_like(close_values, dtype=np.float64)
    )
    splits = (
        df.get_column("Stock Splits").cast(pl.Float64, strict=False).to_numpy()
        if "Stock Splits" in df.columns
        else np.full_like(close_values, np.nan, dtype=np.float64)
    )
    prev_close = np.empty_like(close_values, dtype=np.float64)
    prev_close[0] = np.nan
    prev_close[1:] = close_values[:-1]
    reference = prev_close - np.nan_to_num(dividends, nan=0.0)
    valid_split = np.isfinite(splits) & (splits > 0.0) & (splits != 1.0)
    reference[valid_split] = reference[valid_split] / splits[valid_split]
    reference = np.where(reference > 0.0, reference, np.nan)
    limit_up_values = _tw_limit_price(reference, 1.10, rule_dates)
    limit_down_values = _tw_limit_price(reference, 0.90, rule_dates)
    rows: dict[str, dict[str, float]] = {}
    for idx, date_key in enumerate(dates):
        rows[date_key] = {
            "close": float(close_values[idx]) if np.isfinite(close_values[idx]) else np.nan,
            "limit_up": float(limit_up_values[idx]) if np.isfinite(limit_up_values[idx]) else np.nan,
            "limit_down": float(limit_down_values[idx]) if np.isfinite(limit_down_values[idx]) else np.nan,
        }
    _TW_SYMBOL_CACHE[key] = rows
    return rows


def _tw_side_masks_for_row(row: dict[str, Any]) -> tuple[bool | None, bool | None]:
    symbol = str(row.get("symbol") or "").strip()
    if not symbol:
        return None, None
    date_key = _date_key(row.get("date")) or _date_key(row.get("panel_date"))
    symbol_rows = _load_tw_symbol_rows(symbol)
    info = symbol_rows.get(date_key)
    if not info:
        return None, None
    price = _finite_float(row.get("current_price"), default=np.nan)
    if not np.isfinite(price):
        price = _finite_float(row.get("trade_price"), default=np.nan)
    if not np.isfinite(price):
        price = _finite_float(row.get("panel_price"), default=np.nan)
    if not np.isfinite(price):
        price = _finite_float(info.get("close"), default=np.nan)
    if not np.isfinite(price):
        return None, None
    limit_up = _finite_float(info.get("limit_up"), default=np.nan)
    limit_down = _finite_float(info.get("limit_down"), default=np.nan)
    can_buy = None if not np.isfinite(limit_up) else bool(price < (limit_up - 1e-9))
    can_sell = None if not np.isfinite(limit_down) else bool(price > (limit_down + 1e-9))
    return can_buy, can_sell


def _existing_side_mask(row: dict[str, Any], column: str) -> bool | None:
    if column not in row:
        return None
    value = row.get(column)
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes"}:
            return True
        if text in {"false", "0", "no"}:
            return False
        return None
    try:
        return bool(value)
    except Exception:
        return None


def _effective_side_masks_for_row(row: dict[str, Any]) -> tuple[bool | None, bool | None]:
    """Return the most restrictive known buy/sell masks for a saved signal row."""
    can_buy = _existing_side_mask(row, "can_buy")
    can_sell = _existing_side_mask(row, "can_sell")
    tw_can_buy, tw_can_sell = _tw_side_masks_for_row(row)
    if tw_can_buy is not None:
        can_buy = bool(tw_can_buy) if can_buy is None else bool(can_buy and tw_can_buy)
    if tw_can_sell is not None:
        can_sell = bool(tw_can_sell) if can_sell is None else bool(can_sell and tw_can_sell)
    return can_buy, can_sell


def _row_invariant_issues(row: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    tradable = bool(row.get("tradable", True))
    current = _finite_float(row.get("current_weight"))
    model = _finite_float(row.get("model_weight"))
    target = _finite_float(row.get("target_weight"))
    delta = _finite_float(row.get("delta_weight"))
    abs_delta = _finite_float(row.get("abs_delta_weight"))
    can_buy = _existing_side_mask(row, "can_buy")
    can_sell = _existing_side_mask(row, "can_sell")
    if not tradable and any(abs(value) > 1e-9 for value in (current, model, target, delta)):
        issues.append("untradable_nonzero_weight")
    if abs((target - current) - delta) > 1e-9:
        issues.append("delta_mismatch")
    if abs(abs(delta) - abs_delta) > 1e-9:
        issues.append("abs_delta_mismatch")
    if can_buy is False and delta > 1e-9:
        issues.append("buy_blocked_but_delta_positive")
    if can_sell is False and delta < -1e-9:
        issues.append("sell_blocked_but_delta_negative")
    action = str(row.get("action") or "").strip().upper()
    expected_action = _action(current, target, delta)
    if action and action != expected_action:
        issues.append("action_mismatch")
    return issues


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
    rows = df.to_dicts()
    side_changed = 0
    repaired_rows: list[dict[str, Any]] = []
    for row in rows:
        if not bool(row.get("tradable", True)):
            repaired_rows.append(row)
            continue
        original_issues = _row_invariant_issues(row)
        can_buy, can_sell = _effective_side_masks_for_row(row)
        if can_buy is not None and _existing_side_mask(row, "can_buy") != can_buy:
            row["can_buy"] = can_buy
            side_changed += 1
        if can_sell is not None and _existing_side_mask(row, "can_sell") != can_sell:
            row["can_sell"] = can_sell
            side_changed += 1

        current = _finite_float(row.get("current_weight"))
        target = _finite_float(row.get("target_weight"))
        constraint = str(row.get("constraint") or "")
        if target > current and can_buy is False:
            target = current
            constraint = "buy_blocked"
        if target < current and can_sell is False:
            target = current
            constraint = "sell_blocked"
        delta = target - current
        old_target = _finite_float(row.get("target_weight"))
        old_delta = _finite_float(row.get("delta_weight"))
        if abs(old_target - target) > 1e-12 or abs(old_delta - delta) > 1e-12:
            side_changed += 1
        row["target_weight"] = target
        row["delta_weight"] = delta
        row["abs_delta_weight"] = abs(delta)
        row["action"] = _action(current, target, delta)
        row["constraint"] = constraint
        row["decision_reason"] = _decision_reason(row.get("score"), row.get("model_weight"), str(row["action"]), constraint)
        row["position_status"] = _position_status(
            bool(row.get("tradable", True)),
            current,
            target,
            _finite_float(row.get("model_weight")),
        )
        if original_issues and not _row_invariant_issues(row):
            side_changed += 1
        repaired_rows.append(row)
    if side_changed:
        df = pl.DataFrame(repaired_rows, infer_schema_length=None)
    return df, int(bad_count) + int(side_changed)


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


def _rebalance_rows_from_weights(rows: list[dict[str, Any]], min_abs_delta: float = 1e-9) -> list[dict[str, Any]]:
    out = [dict(row) for row in rows if abs(_finite_float(row.get("delta_weight"))) > float(min_abs_delta)]
    out.sort(key=lambda row: abs(_finite_float(row.get("delta_weight"))), reverse=True)
    return out


def _rebalance_needs_refresh(path: Path, repaired_rows: list[dict[str, Any]]) -> bool:
    if not path.exists():
        return bool(repaired_rows)
    try:
        existing = pl.read_parquet(path)
    except Exception:
        return True
    if existing.height != len(repaired_rows):
        return True
    if not repaired_rows:
        return False
    if "symbol" not in existing.columns or "target_weight" not in existing.columns or "delta_weight" not in existing.columns:
        return True
    check = existing.select(["symbol", "target_weight", "delta_weight"]).to_dicts()
    for left, right in zip(check, repaired_rows):
        if str(left.get("symbol")) != str(right.get("symbol")):
            return True
        if abs(_finite_float(left.get("target_weight")) - _finite_float(right.get("target_weight"))) > 1e-10:
            return True
        if abs(_finite_float(left.get("delta_weight")) - _finite_float(right.get("delta_weight"))) > 1e-10:
            return True
    return False


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


def _repair_signal_dir(signal_dir: Path, *, dry_run: bool = False, refresh_text: bool = False) -> dict[str, Any] | None:
    decision_path = signal_dir / "decision_explanations.parquet"
    weights_path = signal_dir / "target_weights.parquet"
    summary_path = signal_dir / "summary.json"
    if not weights_path.exists() or not summary_path.exists():
        return None

    if decision_path.exists():
        decision_df, decision_bad = _repair_frame(decision_path)
    else:
        decision_df = pl.DataFrame()
        decision_bad = 0
    weights_df, weights_bad = _repair_frame(weights_path)
    changed = decision_bad + weights_bad

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    weight_rows = weights_df.to_dicts()
    decision_rows = decision_df.to_dicts()
    rebalance_path = signal_dir / "rebalance.parquet"
    rebalance_df = weights_df.filter(pl.col("abs_delta_weight") > 1e-9) if "abs_delta_weight" in weights_df.columns else weights_df
    rebalance_df = rebalance_df.sort("abs_delta_weight", descending=True) if "abs_delta_weight" in rebalance_df.columns else rebalance_df
    rebalance_rows = _rebalance_rows_from_weights(weight_rows)
    rebalance_stale = _rebalance_needs_refresh(rebalance_path, rebalance_rows)

    if changed <= 0 and not rebalance_stale and not refresh_text:
        return {"path": str(signal_dir), "changed_rows": 0}

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
    summary["rebalance"] = rebalance_rows[:top_limit]
    summary["decision_explanations"] = [
        row for row in decision_rows if str(row.get("action") or "").upper() != "HOLD"
    ][:top_limit]
    if "model_explanation" in summary and isinstance(summary["model_explanation"], dict):
        by_symbol = {str(row.get("symbol") or ""): row for row in weight_rows}
        top_score_drivers = summary["model_explanation"].get("top_score_drivers")
        if isinstance(top_score_drivers, list):
            refreshed_score_drivers = []
            for item in top_score_drivers:
                if not isinstance(item, dict):
                    refreshed_score_drivers.append(item)
                    continue
                row = by_symbol.get(str(item.get("symbol") or ""))
                if row is not None:
                    item = dict(item)
                    item["target_weight"] = _finite_float(row.get("target_weight"))
                    item["current_price"] = row.get("current_price")
                    item["position_status"] = row.get("position_status")
                refreshed_score_drivers.append(item)
            summary["model_explanation"]["top_score_drivers"] = refreshed_score_drivers
        summary["model_explanation"]["decision_rows"] = int(len(decision_rows))
        summary["model_explanation"]["actionable_decision_rows"] = int(
            sum(1 for row in decision_rows if str(row.get("action") or "").upper() != "HOLD")
        )
    summary["repair_note"] = (
        "untradable symbols zeroed; positions assumed liquidated on previous tradable day; "
        "buy/sell deltas blocked when saved or recomputed side masks disallow that trade direction"
    )

    if not dry_run:
        if changed > 0 or rebalance_stale or refresh_text:
            weights_df.write_parquet(weights_path)
            if decision_path.exists():
                decision_df.write_parquet(decision_path)
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
        (signal_dir / "discord_message.md").write_text(
            format_signal_message(summary, max_rows=top_limit),
            encoding="utf-8",
        )
    return {"path": str(signal_dir), "changed_rows": changed, "refreshed_text": bool(refresh_text)}


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
    parser.add_argument("--refresh-text", action="store_true")
    args = parser.parse_args()

    signal_dirs: list[Path] = []
    for root_text in args.root:
        root = Path(root_text)
        if root.exists():
            signal_dirs.extend(path.parent for path in root.glob("**/decision_explanations.parquet"))
            signal_dirs.extend(path.parent for path in root.glob("**/target_weights.parquet"))
    unique_dirs = sorted(set(signal_dirs))

    results = []
    for signal_dir in unique_dirs:
        result = _repair_signal_dir(signal_dir, dry_run=bool(args.dry_run), refresh_text=bool(args.refresh_text))
        if result is not None and (int(result.get("changed_rows") or 0) > 0 or bool(result.get("refreshed_text"))):
            results.append(result)
            verb = "refresh" if bool(result.get("refreshed_text")) and int(result.get("changed_rows") or 0) <= 0 else "repair"
            print(f"{verb} {result['changed_rows']:5d} rows  {result['path']}")
    synced = _sync_live_weights(unique_dirs, fold_dir=Path(args.fold_dir), dry_run=bool(args.dry_run))
    print(json.dumps({"signals_scanned": len(unique_dirs), "signals_repaired": len(results), "live_weights_synced": synced}, ensure_ascii=False))


if __name__ == "__main__":
    main()
