from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

from scripts.plot_ablation_analysis import (
    _fold_color_map,
    display_label,
    load,
    ordered_variant_names,
)


def _summary_rows() -> list[dict]:
    return [{"fold_id": 2}, {"fold_id": 1}]


def _write_baseline_snapshot(root: Path) -> None:
    fields = [
        "fold_id",
        "train_years",
        "val_years",
        "test_years",
        "cagr",
        "sharpe",
        "sortino",
        "max_drawdown",
        "turnover",
        "daily_hit_rate",
    ]
    with (root / "val_baseline_fold_metrics.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for fold_id in (1, 2):
            writer.writerow(
                {
                    "fold_id": fold_id,
                    "train_years": "2014-2015",
                    "val_years": "2016",
                    "test_years": "2017",
                    "cagr": 0.1,
                    "sharpe": fold_id,
                    "sortino": 2.0,
                    "max_drawdown": -0.1,
                    "turnover": 0.2,
                    "daily_hit_rate": 0.6,
                }
            )


def test_load_uses_baseline_snapshot_and_includes_new_complete_run(tmp_path: Path) -> None:
    _write_baseline_snapshot(tmp_path)
    variant = tmp_path / "future_capital_variant"
    variant.mkdir()
    (variant / "summary.json").write_text(json.dumps(_summary_rows()))

    with pytest.warns(UserWarning, match="baseline/summary.json is unavailable"):
        runs = load(tmp_path, "val")

    assert list(runs) == ["future_capital_variant", "baseline"]
    assert [row["fold_id"] for row in runs["future_capital_variant"]] == [1, 2]
    assert [row["val_metrics"]["sharpe"] for row in runs["baseline"]] == [1.0, 2.0]
    assert ordered_variant_names(runs) == ["future_capital_variant"]
    assert display_label("future_capital_variant") == "Future Capital Variant"


def test_initial_capital_labels_are_explicit() -> None:
    assert display_label("baseline") == "Baseline (capital TWD 10M)"
    assert display_label("initial_capital_1m") == "Capital TWD 1M"
    assert display_label("initial_capital_10m") == "Capital TWD 10M"
    assert display_label("initial_capital_100m") == "Capital TWD 100M"


def test_fold_palette_gets_darker_chronologically() -> None:
    colors = _fold_color_map(tuple(range(1, 12)))

    def luminance(color: tuple) -> float:
        red, green, blue, _ = color
        return 0.2126 * red + 0.7152 * green + 0.0722 * blue

    assert luminance(colors[1]) > luminance(colors[6]) > luminance(colors[11])


def _write_deployment_run(
    run_dir: Path,
    *,
    strategy_returns: list[float],
) -> None:
    run_dir.mkdir(parents=True)
    summary = [
        {
            "fold_id": 1,
            "train_years": [2014],
            "val_years": [2015],
            "test_years": [2016],
        }
    ]
    (run_dir / "summary.json").write_text(json.dumps(summary))
    fold_dir = run_dir / "fold_01"
    fold_dir.mkdir()
    rows = len(strategy_returns)
    np.savez(
        fold_dir / "deployment_test_backtest.npz",
        strategy_returns=np.asarray(strategy_returns, dtype=np.float64),
        benchmark_returns=np.zeros(rows, dtype=np.float64),
        turnovers=np.full(rows, 0.5, dtype=np.float64),
        dates=np.arange(
            np.datetime64("2016-01-04"),
            np.datetime64("2016-01-04") + np.timedelta64(rows, "D"),
        ),
    )


def test_load_deployment_uses_explicit_external_baseline_and_canonical_metrics(
    tmp_path: Path,
) -> None:
    root = tmp_path / "ablation"
    baseline_root = tmp_path / "baseline"
    _write_deployment_run(baseline_root, strategy_returns=[0.001, 0.002])
    _write_deployment_run(
        root / "variant",
        strategy_returns=[0.002, 0.003],
    )

    runs = load(root, "deployment", baseline_root=baseline_root)

    assert list(runs) == ["variant", "baseline"]
    assert runs["variant"][0]["deployment_rows"] == 2
    assert runs["variant"][0]["deployment_metrics"]["turnover"] == 0.5
    assert runs["variant"][0]["deployment_metrics"]["cagr"] > 0.0


def test_deployment_csv_audits_exact_calculation_date_interval(
    tmp_path: Path,
) -> None:
    root = tmp_path / "ablation"
    output_dir = tmp_path / "plots"
    _write_deployment_run(root / "baseline", strategy_returns=[0.001, 0.002])
    _write_deployment_run(root / "variant", strategy_returns=[0.002, 0.003])
    script = Path(__file__).resolve().parents[1] / "scripts/plot_ablation_analysis.py"

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--root",
            str(root),
            "--output-dir",
            str(output_dir),
            "--split",
            "deployment",
            "--prefix",
            "test",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    with (output_dir / "test_baseline_fold_metrics.csv").open(newline="") as fh:
        row = next(csv.DictReader(fh))
    assert row["calculation_rows"] == "2"
    assert row["calculation_date_start"] == "2016-01-04"
    assert row["calculation_date_end"] == "2016-01-05"


def test_same_year_experimental_fold_cannot_replace_prior_owned_test(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "baseline"
    run_dir.mkdir()
    summary = [
        {
            "fold_id": 11,
            "train_years": [2014, 2024],
            "val_years": [2025],
            "test_years": [2026],
        },
        {
            "fold_id": 12,
            "train_years": [2014, 2025],
            "val_years": [2026],
            "test_years": [2026],
        },
    ]
    (run_dir / "summary.json").write_text(json.dumps(summary))
    for fold_id, deployment_returns in ((11, []), (12, [0.9])):
        fold_dir = run_dir / f"fold_{fold_id:02d}"
        fold_dir.mkdir()
        rows = len(deployment_returns)
        np.savez(
            fold_dir / "deployment_test_backtest.npz",
            strategy_returns=np.asarray(deployment_returns, dtype=np.float64),
            benchmark_returns=np.zeros(rows, dtype=np.float64),
            turnovers=np.zeros(rows, dtype=np.float64),
            dates=np.arange(
                np.datetime64("2026-01-01"),
                np.datetime64("2026-01-01") + np.timedelta64(rows, "D"),
            ),
        )
    np.savez(
        run_dir / "fold_11" / "test_backtest.npz",
        strategy_returns=np.asarray([0.001, 0.002], dtype=np.float64),
        benchmark_returns=np.zeros(2, dtype=np.float64),
        turnovers=np.full(2, 0.5, dtype=np.float64),
        dates=np.asarray(["2026-02-26", "2026-02-27"], dtype="datetime64[D]"),
    )

    runs = load(tmp_path, "deployment")

    assert [row["fold_id"] for row in runs["baseline"]] == [11]
    assert runs["baseline"][0]["deployment_rows"] == 2
    assert runs["baseline"][0]["deployment_date_start"] == "2026-02-26"
    assert runs["baseline"][0]["deployment_metrics"]["cagr"] > 0.0


def test_deployment_plot_cli_bootstraps_repo_imports(tmp_path: Path) -> None:
    root = tmp_path / "ablation"
    baseline_root = tmp_path / "baseline"
    output_dir = tmp_path / "plots"
    _write_deployment_run(baseline_root, strategy_returns=[0.001, 0.002])
    _write_deployment_run(root / "variant", strategy_returns=[0.002, 0.003])
    script = Path(__file__).resolve().parents[1] / "scripts/plot_ablation_analysis.py"

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--root",
            str(root),
            "--baseline-root",
            str(baseline_root),
            "--output-dir",
            str(output_dir),
            "--split",
            "deployment",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert (output_dir / "deployment_paired_sharpe_effects.png").is_file()
    assert (output_dir / "deployment_risk_return_medians.png").is_file()
    for metric_name in (
        "cagr",
        "sharpe",
        "sortino",
        "turnover",
        "daily_hit_rate",
    ):
        assert (
            output_dir / f"deployment_risk_return_{metric_name}_medians.png"
        ).is_file()
        metric_dir = output_dir / "deployment_risk_return_by_fold" / metric_name
        assert (metric_dir / "fold_01.png").is_file()
        assert (metric_dir / "all_folds_connected.png").is_file()
