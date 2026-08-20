import json
from pathlib import Path

from scripts.run_tw_day_trade_ablation import (
    POSTPROCESS_PLOT_SPECS,
    _bar,
    _incomplete_fold_markers,
    _last_json,
    _load_effective_spec,
    _plot_completed_experiments,
    _progress,
    _single_experiment_concurrency,
)


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


def test_progress_sums_durable_partial_epochs_but_labels_only_current_job(
    tmp_path: Path,
) -> None:
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
    assert "variant fold 1/2 epoch 75/100" in label
    assert "baseline fold 1/2 epoch 25/100" not in label


def test_tw_day_trade_supervisor_always_runs_one_experiment() -> None:
    assert _single_experiment_concurrency(1) == 1
    assert _single_experiment_concurrency(2) == 1


def test_external_baseline_must_have_every_fold_completion_marker(
    tmp_path: Path,
) -> None:
    for fold in (1, 3):
        fold_dir = tmp_path / f"fold_{fold:02d}"
        fold_dir.mkdir()
        (fold_dir / "fold_complete.json").write_text("{}")

    assert _incomplete_fold_markers(tmp_path, 3) == [2]

    fold_dir = tmp_path / "fold_02"
    fold_dir.mkdir()
    (fold_dir / "fold_complete.json").write_text("{}")
    assert _incomplete_fold_markers(tmp_path, 3) == []


def test_supervisor_resolves_inherited_ablation_spec(tmp_path: Path) -> None:
    base = tmp_path / "base.yaml"
    base.write_text(
        """
base_config: configs/markets/base.yaml
output_root: artifacts/ablations/base
expected_fold_count: 12
matrix:
  include_baseline: true
  dimensions:
    - name: placeholder
      enabled: false
""",
        encoding="utf-8",
    )
    child = tmp_path / "child.yaml"
    child.write_text(
        """
base_spec: base.yaml
base_config: configs/markets/multi_basis.yaml
output_root: artifacts/ablations/multi_basis
""",
        encoding="utf-8",
    )

    spec, rows = _load_effective_spec(child)

    assert spec["expected_fold_count"] == 12
    assert spec["base_config"] == "configs/markets/multi_basis.yaml"
    assert spec["output_root"] == "artifacts/ablations/multi_basis"
    assert [row["name"] for row in rows] == ["baseline"]


def test_bar_has_exact_percentage_and_label() -> None:
    rendered = _bar(12.5, "baseline fold 1")
    assert "12.50%" in rendered
    assert rendered.endswith("baseline fold 1")


def test_primary_test_plot_uses_panel_history_owned_handoff_interval() -> None:
    assert POSTPROCESS_PLOT_SPECS == (
        ("val", "val", "Validation tensor loss contract"),
        (
            "deployment",
            "test",
            "Owned stitched deployment test: new model starts at each year's first target",
        ),
        (
            "test",
            "full_horizon_integer_audit",
            "Diagnostic only: expanding full-future exact whole-lot horizon",
        ),
    )


def test_plot_completed_experiments_refreshes_each_contract_surface(
    tmp_path: Path,
    monkeypatch,
) -> None:
    commands: list[list[str]] = []

    def capture(command: list[str], *, log=None) -> None:
        assert log is None
        commands.append(command)

    monkeypatch.setattr(
        "scripts.run_tw_day_trade_ablation._run_checked",
        capture,
    )
    root = tmp_path / "ablation"
    baseline = tmp_path / "baseline"

    _plot_completed_experiments(root, baseline_root=baseline)

    assert len(commands) == len(POSTPROCESS_PLOT_SPECS)
    assert [command[command.index("--split") + 1] for command in commands] == [
        "val",
        "deployment",
        "test",
    ]
    assert [command[command.index("--prefix") + 1] for command in commands] == [
        "val",
        "test",
        "full_horizon_integer_audit",
    ]
    assert all(
        command[command.index("--baseline-root") + 1] == str(baseline)
        for command in commands
    )
