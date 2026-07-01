import json

from stockagent.training.trainer import _fold_dir, _load_completed_fold_result


def _write_fold_artifacts(root, fold_id: int, *, marker: bool) -> None:
    fold_dir = _fold_dir(root, fold_id)
    fold_dir.mkdir(parents=True)
    payload = {
        "fold_id": fold_id,
        "train_years": [2024],
        "val_years": [2025],
        "test_years": [2026],
        "best_val_loss": -1.0,
        "val_ic": {},
        "val_metrics": {},
        "test_ic": {},
        "test_metrics": {},
        "test_integer_metrics": None,
    }
    (fold_dir / "metrics.json").write_text(json.dumps(payload), encoding="utf-8")
    (fold_dir / "model.pt").write_bytes(b"placeholder")
    (fold_dir / "test_backtest.npz").write_bytes(b"placeholder")
    if marker:
        (fold_dir / "fold_complete.json").write_text(
            json.dumps({"status": "complete"}),
            encoding="utf-8",
        )


def test_resume_does_not_treat_best_val_artifacts_as_completed(tmp_path):
    _write_fold_artifacts(tmp_path, 26, marker=False)

    assert _load_completed_fold_result(tmp_path, 26) is None


def test_resume_loads_fold_only_when_complete_marker_exists(tmp_path):
    _write_fold_artifacts(tmp_path, 25, marker=True)

    result = _load_completed_fold_result(tmp_path, 25)

    assert result is not None
    assert result.fold_id == 25
