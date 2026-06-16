import json
from pathlib import Path

from stockagent.training.trainer import (
    _infer_no_improve_epochs_from_curve,
    _resume_no_improve_epochs_from_checkpoint,
)


def _write_curve_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def test_infer_no_improve_from_legacy_val_mean_curve(tmp_path: Path) -> None:
    curve_path = tmp_path / "epoch_curve.jsonl"
    _write_curve_rows(
        curve_path,
        [
            {"epoch": 1, "val_mean": 1.0},
            {"epoch": 2, "val_mean": 1.1},
            {"epoch": 3, "val_mean": None},
            {"epoch": 4, "val_mean": 0.9},
            {"epoch": 5, "val_mean": 0.95},
            {"epoch": 6, "val_mean": 0.96},
        ],
    )

    assert _infer_no_improve_epochs_from_curve(curve_path) == 2
    assert _infer_no_improve_epochs_from_curve(curve_path, stop_before_epoch=4) == 1


def test_resume_no_improve_uses_checkpoint_over_curve(tmp_path: Path) -> None:
    curve_path = tmp_path / "epoch_curve.jsonl"
    _write_curve_rows(curve_path, [{"epoch": 1, "val_mean": 1.0}, {"epoch": 2, "val_mean": 1.2}])

    no_improve, source = _resume_no_improve_epochs_from_checkpoint(
        {"no_improve_epochs": 10},
        curve_path,
    )

    assert no_improve == 10
    assert source == "checkpoint"


def test_resume_no_improve_legacy_checkpoint_infers_full_curve(tmp_path: Path) -> None:
    curve_path = tmp_path / "epoch_curve.jsonl"
    _write_curve_rows(
        curve_path,
        [
            {"epoch": 1, "val_mean": 1.0},
            {"epoch": 2, "val_mean": 1.2},
            {"epoch": 3, "val_mean": 1.3},
        ],
    )

    no_improve, source = _resume_no_improve_epochs_from_checkpoint({}, curve_path)

    assert no_improve == 2
    assert source == "epoch_curve"


def test_resume_no_improve_without_checkpoint_state_or_curve_uses_default(tmp_path: Path) -> None:
    no_improve, source = _resume_no_improve_epochs_from_checkpoint(
        {},
        tmp_path / "missing_epoch_curve.jsonl",
    )

    assert no_improve == 0
    assert source == "default"


def test_infer_no_improve_prefers_explicit_curve_state(tmp_path: Path) -> None:
    curve_path = tmp_path / "epoch_curve.jsonl"
    _write_curve_rows(
        curve_path,
        [
            {"epoch": 1, "val_mean": 1.0, "no_improve": 0},
            {"epoch": 2, "val_mean": 1.2, "no_improve": 10},
        ],
    )

    assert _infer_no_improve_epochs_from_curve(curve_path) == 10
