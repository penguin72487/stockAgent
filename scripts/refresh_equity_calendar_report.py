#!/usr/bin/env python3
"""Render a versioned daily-display report from an immutable backtest artifact.

This is a report-only operation. Dated-futures and FIFO archives still require
their pinned source context and are rejected by the canonical loader if absent.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
import sys

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.backtest import report
from stockagent.training.trainer import _load_backtest_artifact


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for part in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(part)
    return digest.hexdigest()


def _session_coverage(days: np.ndarray, expected_path: Path | None) -> dict:
    if expected_path is None:
        return {"status": "not_independently_verified", "missing_expected_dates": []}
    expected_path = expected_path.resolve(strict=True)
    expected_hash = _sha256(expected_path)
    expected_raw = json.loads(expected_path.read_text(encoding="utf-8"))
    if not isinstance(expected_raw, list) or not all(isinstance(x, str) for x in expected_raw):
        raise ValueError("expected dates must be a JSON array of ISO date strings")
    expected = np.asarray(expected_raw, dtype="datetime64[D]")
    if np.isnat(expected).any() or np.any(expected[1:] <= expected[:-1]):
        raise ValueError("expected dates must be valid, unique and increasing")
    if not len(expected) or expected[0] > days[0] or expected[-1] < days[-1]:
        raise ValueError("expected date source does not span the observed account interval")
    expected = expected[(expected >= days[0]) & (expected <= days[-1])]
    if not len(expected):
        raise ValueError("expected date source has no coverage inside the observed interval")
    missing = np.setdiff1d(expected, days)
    if len(missing):
        raise ValueError(f"missing expected account marks: {missing.astype(str).tolist()}")
    coverage = {"status": "expected_dates_present", "source_identity": str(expected_path),
                "source_sha256": expected_hash, "expected_dates_checked": int(len(expected)),
                "missing_expected_dates": [],
                "additional_account_dates": np.setdiff1d(days, expected).astype(str).tolist()}
    if _sha256(expected_path) != expected_hash:
        raise ValueError("expected date source changed during the coverage check")
    return coverage


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backtest", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--source-identity", required=True)
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--benchmark-label", required=True)
    parser.add_argument("--scope-label", default="Canonical Stitched Walk-Forward Deployment")
    parser.add_argument("--expected-account-dates-json", type=Path,
                        help="Independently sourced session dates to check inside the observed interval")
    args = parser.parse_args()

    source = args.backtest.resolve(strict=True)
    source_hash = _sha256(source)
    if source_hash != args.expected_sha256:
        raise ValueError("backtest artifact SHA-256 differs from the requested source")
    result, dates = _load_backtest_artifact(source)
    if _sha256(source) != source_hash:
        raise ValueError("backtest artifact changed during canonical loading")
    account = report._account_metric_rows(result, dates)
    if account is None:
        account_dates = dates
        strategy_returns = report._clean_log_returns(result.strategy_returns)
        benchmark_returns = report._clean_log_returns(result.benchmark_returns)
        turnovers = np.asarray(result.turnovers, dtype=np.float64)
    else:
        account_dates, strategy_returns, benchmark_returns, turnovers = account

    strategy = report._trading_session_equity_display(account_dates, report._safe_equity_for_plot(strategy_returns))
    benchmark = report._trading_session_equity_display(account_dates, report._safe_equity_for_plot(benchmark_returns))
    if strategy is None or benchmark is None:
        raise ValueError("daily account display requires unique, ordered, midnight account dates")
    days, strategy_nav = strategy
    if not np.array_equal(days, benchmark[0]) or len(turnovers) != len(days):
        raise ValueError("strategy and benchmark account dates do not align")
    coverage = _session_coverage(days, args.expected_account_dates_json)
    metrics = report.compute_metrics(result)

    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    with (output / "equity_account_sessions.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["date", "strategy_equity", "benchmark_equity", "strategy_log_return",
                         "benchmark_log_return", "simulated_turnover", "no_simulated_trade"])
        for day, value, base, ret, base_ret, turnover in zip(
                days, strategy_nav, benchmark[1], strategy_returns, benchmark_returns, turnovers, strict=True):
            writer.writerow([str(day), float(value), float(base), float(ret), float(base_ret),
                             float(turnover), bool(turnover == 0)])

    report.plot_equity_curve(result, dates, output / "walkforward_equity_curve.png",
                             scope_label=args.scope_label, benchmark_label=args.benchmark_label)
    report.plot_equity_curve_log(result, dates, output / "walkforward_equity_curve_log.png",
                                 scope_label=args.scope_label, benchmark_label=args.benchmark_label)
    files = ["equity_account_sessions.csv", "walkforward_equity_curve.png", "walkforward_equity_curve_log.png"]
    receipt = {
        "report_contract": report.TRADING_SESSION_EQUITY_DISPLAY_CONTRACT,
        "source_identity": args.source_identity,
        "source_sha256": source_hash,
        "report_code_sha256": _sha256(ROOT / "stockagent/backtest/report.py"),
        "generator_sha256": _sha256(Path(__file__)),
        "account_mark_count": int(len(days)),
        "first_date": str(days[0]), "last_date": str(days[-1]),
        "session_coverage": coverage,
        "positive_simulated_turnover_marks": int(np.count_nonzero(np.asarray(turnovers) > 0)),
        "benchmark_label": args.benchmark_label,
        "unchanged_account_metrics": metrics,
        "display_rule": "observed account marks only; no dates, prices, returns or fills inferred",
        "outputs_sha256": {name: _sha256(output / name) for name in files},
    }
    (output / "account_session_display_receipt.json").write_text(
        json.dumps(receipt, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    print(json.dumps({"output": str(output), **receipt}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
