import json
from pathlib import Path

from stockagent.training.trainer import (
    _EpochCurveLifecycle,
    _infer_no_improve_epochs_from_curve,
    _resume_no_improve_epochs_from_checkpoint,
    _trim_group_curve,
)
from stockagent.training import trainer


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


def test_epoch_curve_lifecycle_reuses_resume_record_and_deferred_plot(
    tmp_path: Path,
    monkeypatch,
) -> None:
    curve_path = tmp_path / "train_2020" / "epoch_curve.jsonl"
    calls: list[tuple[str, object]] = []
    monkeypatch.setattr(
        trainer,
        "_trim_group_curve",
        lambda path, epoch: calls.append(("trim", (path, epoch))),
    )
    monkeypatch.setattr(
        trainer,
        "_append_group_curve",
        lambda path, payload: calls.append(("append", (path, payload))),
    )
    monkeypatch.setattr(
        trainer,
        "_run_epoch_curve_plot_once",
        lambda path, interval, **_kwargs: calls.append(
            ("plot", (path, interval))
        )
        or {"total_s": 0.1},
    )
    lifecycle = _EpochCurveLifecycle(
        curve_path,
        enabled=True,
        interval=3,
        async_enabled=True,
        defer_until_end=True,
    )

    lifecycle.prepare_resume(4)
    lifecycle.record({"epoch": 4, "train_loss": 1.0}, request_plot=True)
    timing = lifecycle.flush()

    assert [name for name, _ in calls] == ["trim", "append", "plot"]
    assert timing == {"total_s": 0.1}


def test_epoch_curve_epoch_one_restart_archives_incompatible_partial_history(
    tmp_path: Path,
) -> None:
    curve_path = tmp_path / "train_2020" / "epoch_curve.jsonl"
    _write_curve_rows(curve_path, [{"epoch": 1}, {"epoch": 2}])

    _trim_group_curve(curve_path, 1)

    assert not curve_path.exists()
    archived = list(
        curve_path.parent.glob(
            "epoch_curve.unresumable_restart_from_epoch1.*.jsonl"
        )
    )
    assert len(archived) == 1
    assert archived[0].read_text(encoding="utf-8").count("\n") == 2


def test_epoch_curve_lifecycle_honors_synchronous_incremental_plot(
    tmp_path: Path,
    monkeypatch,
) -> None:
    curve_path = tmp_path / "epoch_curve.jsonl"
    curve_path.write_text("{}\n", encoding="utf-8")
    calls: list[tuple[Path, int, bool]] = []
    monkeypatch.setattr(
        trainer,
        "_run_epoch_curve_plot_once",
        lambda path, interval, *, write_parquet_cache=True: calls.append(
            (path, interval, write_parquet_cache)
        )
        or {},
    )
    lifecycle = _EpochCurveLifecycle(
        curve_path,
        enabled=True,
        interval=2,
        async_enabled=False,
        defer_until_end=False,
    )

    lifecycle.record({"epoch": 2, "train_loss": 1.0}, request_plot=True)
    lifecycle.flush()

    assert calls == [(curve_path, 2, False)]


def test_progress_bar_is_one_rank_aware_canonical_implementation(
    monkeypatch,
) -> None:
    calls: list[dict[str, object]] = []
    sentinel = object()

    def fake_tqdm(iterable=None, **kwargs):
        calls.append({"iterable": iterable, **kwargs})
        return sentinel

    monkeypatch.setattr(trainer, "tqdm", fake_tqdm)
    monkeypatch.setattr(trainer, "_distributed_is_rank0", lambda: False)

    result = trainer._progress_bar(
        [1, 2], desc=" Epochs", unit="epoch", leave=False
    )

    assert result is sentinel
    assert calls == [
        {
            "iterable": [1, 2],
            "desc": " Epochs",
            "unit": "epoch",
            "total": None,
            "leave": False,
            "dynamic_ncols": True,
            "disable": True,
        }
    ]

    from stockagent.training import index_derivatives_tick, minute

    assert minute._progress_bar is trainer._progress_bar
    assert index_derivatives_tick._progress_bar is trainer._progress_bar
