#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.backtest.holdings import save_realized_holdings_artifacts


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export inspectable realized holdings from a tw_minute fold artifact."
    )
    parser.add_argument("fold_dir", type=Path)
    parser.add_argument("--initial-capital", type=float, default=None)
    parser.add_argument("--no-plots", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    fold_dir = args.fold_dir.resolve()
    backtest_path = fold_dir / "test_backtest.npz"
    symbols_path = fold_dir / "deployment_test_symbols.json"
    if not backtest_path.is_file() or not symbols_path.is_file():
        raise RuntimeError(
            "fold must contain test_backtest.npz and deployment_test_symbols.json"
        )
    symbols = json.loads(symbols_path.read_text(encoding="utf-8"))
    if not isinstance(symbols, list):
        raise RuntimeError("deployment_test_symbols.json must contain a list")
    initial_capital = args.initial_capital
    manifest_path = fold_dir.parent / "run_manifest.json"
    if initial_capital is None and manifest_path.is_file():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        initial_capital = (
            manifest.get("configuration", {})
            .get("trading", {})
            .get("tw_minute_initial_equity")
        )
    with np.load(backtest_path, allow_pickle=False) as payload:
        execution_mode = str(payload["execution_mode"].item())
        if execution_mode != "tw_minute":
            raise RuntimeError(
                f"holdings exporter expected tw_minute, got {execution_mode!r}"
            )
        contract = save_realized_holdings_artifacts(
            fold_dir,
            payload["dates"],
            symbols,
            payload["weights_history"],
            initial_capital=(
                None if initial_capital is None else float(initial_capital)
            ),
            daily_performance=(
                pl.read_parquet(fold_dir / "test_daily_curve.parquet")
                if (fold_dir / "test_daily_curve.parquet").is_file()
                else None
            ),
            write_plots=not bool(args.no_plots),
            source_artifact=backtest_path.name,
        )
    print(json.dumps(contract, indent=2))


if __name__ == "__main__":
    main()
