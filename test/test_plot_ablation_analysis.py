from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from scripts.plot_ablation_analysis import (
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
    assert display_label("baseline") == "Baseline (capital TWD 1M)"
    assert display_label("initial_capital_10m") == "Capital TWD 10M"
    assert display_label("initial_capital_100m") == "Capital TWD 100M"
