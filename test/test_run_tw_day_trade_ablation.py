import json
from pathlib import Path

from scripts.run_tw_day_trade_ablation import _bar, _last_json, _progress


def test_last_json_reads_latest_epoch(tmp_path: Path) -> None:
    path = tmp_path / "epoch_curve.jsonl"
    path.write_text('\n'.join(json.dumps({"epoch": epoch}) for epoch in (1, 2, 3)) + '\n')
    assert _last_json(path)["epoch"] == 3


def test_progress_counts_completed_folds_and_active_epoch(tmp_path: Path) -> None:
    (tmp_path / "baseline" / "fold_01").mkdir(parents=True)
    (tmp_path / "baseline" / "fold_01" / "fold_complete.json").write_text("{}")
    curve_dir = tmp_path / "baseline" / "train_2014-2015"
    curve_dir.mkdir()
    (curve_dir / "epoch_curve.jsonl").write_text(json.dumps({"epoch": 50}) + "\n")

    percent, label = _progress(tmp_path, ["baseline", "variant"], folds=2, epochs=100)

    assert percent == 37.5
    assert "folds 1/4" in label
    assert "fold 2/2 epoch 50/100" in label


def test_progress_sums_two_concurrent_experiment_epochs(tmp_path: Path) -> None:
    for name, epoch in (("baseline", 25), ("variant", 75)):
        curve_dir = tmp_path / name / "train_2014"
        curve_dir.mkdir(parents=True)
        (curve_dir / "epoch_curve.jsonl").write_text(
            json.dumps({"epoch": epoch}) + "\n"
        )

    percent, label = _progress(
        tmp_path, ["baseline", "variant"], folds=2, epochs=100
    )

    assert percent == 25.0
    assert "baseline fold 1/2 epoch 25/100" in label
    assert "variant fold 1/2 epoch 75/100" in label


def test_bar_has_exact_percentage_and_label() -> None:
    rendered = _bar(12.5, "baseline fold 1")
    assert "12.50%" in rendered
    assert rendered.endswith("baseline fold 1")
