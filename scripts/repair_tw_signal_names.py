#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import polars as pl

from stockagent.live.quote_provider import load_symbol_name_map
from stockagent.live.report_formatter import format_signal_message
from stockagent.live.signal_engine import LiveSignalResult, _write_text_artifacts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repair historical TW live-signal artifacts with Chinese symbol names.")
    parser.add_argument("--signals-root", default="artifacts/live_signals/tw")
    parser.add_argument("--symbols-root", default="data_yahoo/tw_stocks")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _symbol(row: dict[str, Any]) -> str:
    raw = row.get("symbol") if row.get("symbol") is not None else row.get("code")
    text = str(raw or "").strip()
    if text.endswith(".0"):
        head = text[:-2]
        if head.isdigit():
            return head
    return text


def _name_for(symbol: str, name_map: dict[str, str]) -> str | None:
    name = str(name_map.get(symbol) or "").strip()
    return name or None


def _repair_rows(rows: list[dict[str, Any]], name_map: dict[str, str]) -> int:
    changed = 0
    for row in rows:
        symbol = _symbol(row)
        name = _name_for(symbol, name_map)
        if symbol and str(row.get("symbol") or "").strip() != symbol:
            row["symbol"] = symbol
            changed += 1
        if name is None:
            continue
        old = str(row.get("name") or "").strip()
        if old != name:
            row["name"] = name
            changed += 1
    return changed


def _read_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return pl.read_parquet(path).to_dicts()


def _write_rows(path: Path, rows: list[dict[str, Any]], *, dry_run: bool) -> None:
    if dry_run or not rows:
        return
    pl.DataFrame(rows, infer_schema_length=None).write_parquet(path)


def _repair_parquet(path: Path, name_map: dict[str, str], *, dry_run: bool) -> tuple[list[dict[str, Any]], int]:
    rows = _read_rows(path)
    changed = _repair_rows(rows, name_map)
    if changed:
        _write_rows(path, rows, dry_run=dry_run)
    return rows, changed


def _repair_summary(summary: dict[str, Any], name_map: dict[str, str]) -> int:
    changed = 0
    for key in ("top_positions", "rebalance", "decision_explanations"):
        rows = summary.get(key)
        if isinstance(rows, list):
            changed += _repair_rows([row for row in rows if isinstance(row, dict)], name_map)
    explanation = summary.get("model_explanation")
    if isinstance(explanation, dict):
        rows = explanation.get("top_score_drivers")
        if isinstance(rows, list):
            changed += _repair_rows([row for row in rows if isinstance(row, dict)], name_map)
    return changed


def repair_signal_dir(summary_path: Path, name_map: dict[str, str], *, dry_run: bool) -> dict[str, Any]:
    output_dir = summary_path.parent
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"summary": str(summary_path), "status": "failed", "message": str(exc)}

    summary_changed = _repair_summary(summary, name_map)
    weights_rows, weights_changed = _repair_parquet(output_dir / "target_weights.parquet", name_map, dry_run=dry_run)
    rebalance_rows, rebalance_changed = _repair_parquet(output_dir / "rebalance.parquet", name_map, dry_run=dry_run)
    decision_rows, decision_changed = _repair_parquet(output_dir / "decision_explanations.parquet", name_map, dry_run=dry_run)

    total_changed = summary_changed + weights_changed + rebalance_changed + decision_changed
    text_rewritten = bool(weights_rows or rebalance_rows or decision_rows)
    if (total_changed or text_rewritten) and not dry_run:
        message = format_signal_message(summary, max_rows=max(1, len(summary.get("top_positions") or [])))
        result = LiveSignalResult(
            summary=summary,
            weights_rows=weights_rows,
            rebalance_rows=rebalance_rows,
            decision_rows=decision_rows,
            message=message,
            output_dir=str(output_dir),
        )
        summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        (output_dir / "discord_message.md").write_text(message, encoding="utf-8")
        _write_text_artifacts(result, output_dir)

    return {
        "summary": str(summary_path),
        "status": "updated" if total_changed else "unchanged",
        "text_rewritten": text_rewritten,
        "summary_rows_changed": summary_changed,
        "weights_rows_changed": weights_changed,
        "rebalance_rows_changed": rebalance_changed,
        "decision_rows_changed": decision_changed,
    }


def main() -> None:
    args = parse_args()
    name_map = load_symbol_name_map(args.symbols_root)
    if not name_map:
        raise SystemExit(f"No symbol names found under {args.symbols_root}")
    root = Path(args.signals_root)
    paths = sorted(root.rglob("summary.json"))
    results = [repair_signal_dir(path, name_map, dry_run=bool(args.dry_run)) for path in paths]
    counts: dict[str, int] = {}
    for result in results:
        counts[str(result["status"])] = counts.get(str(result["status"]), 0) + 1
    print(json.dumps({"signals": len(results), "counts": counts, "dry_run": bool(args.dry_run)}, ensure_ascii=False, indent=2))
    for result in results:
        if result["status"] != "unchanged":
            print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
