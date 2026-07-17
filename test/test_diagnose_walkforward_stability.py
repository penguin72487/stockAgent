from __future__ import annotations

import json
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.diagnose_walkforward_stability import (
    RunSpec,
    _parse_folds,
    _parse_run,
    collect_rows,
    diagnose_epochs,
    diagnose_weights,
)


def test_parse_run_and_folds() -> None:
    named = _parse_run("baseline=artifacts/base")
    unnamed = _parse_run("artifacts/candidate")

    assert named.name == "baseline"
    assert str(named.path) == "artifacts/base"
    assert unnamed.name == "candidate"
    assert _parse_folds("23,24,23,25") == [23, 24, 25]


def test_diagnose_weights_reports_effective_positions(tmp_path) -> None:
    path = tmp_path / "daily_weights.parquet"
    pq.write_table(
        pa.table(
            {
                "date": [datetime(2025, 1, 2), datetime(2025, 1, 3)],
                "AAA": [0.5, 1.0],
                "BBB": [-0.5, 0.0],
                "CCC": [0.0, 0.0],
            }
        ),
        path,
    )

    result = diagnose_weights(path, position_epsilon=1e-8)

    assert result is not None
    assert result.rows == 2
    assert result.symbols == 3
    assert result.avg_gross == pytest.approx(1.0)
    assert result.avg_net == pytest.approx(0.5)
    assert result.avg_positions == pytest.approx(1.5)
    assert result.avg_effective_positions == pytest.approx(1.5)
    assert result.avg_max_abs_weight == pytest.approx(0.75)
    assert result.p95_max_abs_weight == pytest.approx(0.975)
    assert result.top_symbol == "AAA"
    assert result.top_date == "2025-01-03"
    assert result.top_weight == pytest.approx(1.0)


def test_diagnose_epochs_uses_exact_training_group_and_steady_tail(tmp_path) -> None:
    group = tmp_path / "train_2020-2021"
    group.mkdir()
    records = [
        {"epoch": 1, "val_mean": 0.4, "epoch_wall_s": 100.0, "train_total_s": 90.0, "train_batches": 10},
        {"epoch": 2, "val_mean": 0.3, "epoch_wall_s": 12.0, "train_total_s": 10.0, "train_batches": 20},
        {"epoch": 3, "val_mean": 0.1, "epoch_wall_s": 10.0, "train_total_s": 8.0, "train_batches": 20},
        {"epoch": 4, "val_mean": 0.2, "epoch_wall_s": 11.0, "train_total_s": 9.0, "train_batches": 20},
    ]
    (group / "epoch_curve.jsonl").write_text(
        "".join(json.dumps(record) + "\n" for record in records),
        encoding="utf-8",
    )

    result = diagnose_epochs(tmp_path, [2020, 2021], batch_size=32)

    assert result["epochs_completed"] == 4
    assert result["best_epoch_from_curve"] == 3
    assert result["best_val_mean_from_curve"] == pytest.approx(0.1)
    assert result["steady_epoch_wall_s_median"] == pytest.approx(11.0)
    assert result["steady_train_s_median"] == pytest.approx(9.0)
    assert result["steady_batches_per_s_median"] == pytest.approx(20.0 / 9.0)
    assert result["steady_padded_samples_per_s_median"] == pytest.approx(32.0 * 20.0 / 9.0)
    assert result["timing_synchronized"] is False


def test_collect_rows_keeps_missing_fold_explicit(tmp_path) -> None:
    rows = collect_rows(
        [RunSpec(name="candidate", path=tmp_path)],
        [24, 25],
        position_epsilon=1e-8,
    )

    assert [(row["fold"], row["status"]) for row in rows] == [(24, "missing"), (25, "missing")]


def test_collect_rows_classifies_fold_superseded_by_final_overlap(tmp_path) -> None:
    final_dir = tmp_path / "fold_26"
    final_dir.mkdir()
    (final_dir / "metrics.json").write_text(
        json.dumps(
            {
                "fold_id": 26,
                "train_years": [2024],
                "val_years": [2026],
                "test_years": [2026],
                "val_metrics": {},
                "test_metrics": {},
            }
        ),
        encoding="utf-8",
    )
    (final_dir / "fold_complete.json").write_text(
        json.dumps({"status": "complete"}),
        encoding="utf-8",
    )

    rows = collect_rows(
        [RunSpec(name="candidate", path=tmp_path)],
        [25, 26],
        position_epsilon=1e-8,
    )

    assert rows[0]["status"] == "superseded_by_final_overlap"
    assert rows[0]["superseded_by_fold"] == 26
    assert rows[1]["status"] == "complete"
