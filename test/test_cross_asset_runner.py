from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pytest
import torch

from stockagent import cross_asset_runner as runner
from stockagent.data.walkforward import WalkForwardFold


def _fold(
    fold_id: int,
    *,
    test_indices: tuple[int, ...] = (0, 1, 2),
    test_years: tuple[int, ...] = (2020,),
) -> WalkForwardFold:
    return WalkForwardFold(
        fold_id=fold_id,
        train_indices=np.asarray([0], dtype=np.int64),
        val_indices=np.asarray([0], dtype=np.int64),
        test_indices=np.asarray(test_indices, dtype=np.int64),
        train_years=[2018],
        val_years=[2019],
        test_years=list(test_years),
    )


def test_cli_exposes_no_alternate_fold_split_or_date_coverage_paths() -> None:
    args = runner.parse_args(["--config", "configs/markets/tw_public_lanten_market.yaml"])

    assert not hasattr(args, "first_test_year_only")
    assert not hasattr(args, "max_rows")
    for removed in (
        "--fold",
        "--checkpoint",
        "--split",
        "--max-rows",
        "--sample-method",
        "--first-test-year-only",
        "--no-first-test-year-only",
        "--graph-betweenness-max-vertices",
        "--graph-plot-max-nodes",
    ):
        with pytest.raises(SystemExit):
            runner.parse_args(
                ["--config", "configs/markets/tw_public_lanten_market.yaml", removed, "1"]
                if removed not in {"--first-test-year-only", "--no-first-test-year-only"}
                else ["--config", "configs/markets/tw_public_lanten_market.yaml", removed]
            )


@pytest.mark.parametrize(
    ("configured", "device", "expected_dtype", "expected_actual"),
    (
        ("bf16", "cuda", torch.bfloat16, "bf16"),
        ("fp16", "cuda", torch.float16, "fp16"),
        ("none", "cuda", None, "none"),
        ("tf32", "cuda", None, "none"),
        ("bf16", "cpu", None, "none"),
    ),
)
def test_amp_dtype_respects_config_and_actual_device(
    configured: str,
    device: str,
    expected_dtype: torch.dtype | None,
    expected_actual: str,
) -> None:
    config = SimpleNamespace(environment=SimpleNamespace(amp_dtype=configured))

    dtype, normalized, actual = runner._resolve_cuda_autocast_dtype(
        config,
        torch.device(device),
    )

    assert dtype == expected_dtype
    assert normalized == ("none" if configured == "tf32" else configured)
    assert actual == expected_actual


def test_runner_forces_compact_artifact_contract() -> None:
    args = runner.parse_args(["--config", "configs/markets/tw_public_lanten_market.yaml"])

    settings = runner._settings(args, progress_enabled=False)

    assert settings.compact_artifacts is True
    assert settings.progress_enabled is False
    assert settings.source_chunk_size == 128
    assert settings.max_repeated_rows == 4096
    assert settings.row_chunk_size == 0
    assert settings.counterfactual_compile is True
    assert settings.graph_betweenness_max_vertices == 0
    assert settings.graph_plot_max_nodes == 0


def test_configured_folds_require_every_canonical_checkpoint(tmp_path: Path) -> None:
    folds = [_fold(1), _fold(2), _fold(3)]
    for fold_id in (1, 3):
        fold_dir = tmp_path / f"fold_{fold_id:02d}"
        fold_dir.mkdir(parents=True)
        (fold_dir / "checkpoint_best.pt").touch()

    with pytest.raises(FileNotFoundError, match=r"missing_fold_ids=\[2\]"):
        runner._configured_fold_ids(folds, tmp_path)

    fold_02 = tmp_path / "fold_02"
    fold_02.mkdir()
    (fold_02 / "checkpoint_best.pt").touch()
    assert runner._configured_fold_ids(folds, tmp_path) == [1, 2, 3]


def test_lpt_fold_assignments_are_deterministic_balanced_and_complete() -> None:
    fold_ids = [1, 2, 3, 4, 5, 6]
    weights = {1: 10, 2: 9, 3: 8, 4: 7, 5: 6, 6: 5}

    assignments, loads = runner._lpt_fold_assignments(fold_ids, weights, world_size=2)

    assert assignments == [[1, 4, 5], [2, 3, 6]]
    assert loads == [23, 22]
    flattened = [fold_id for rank_folds in assignments for fold_id in rank_folds]
    assert sorted(flattened) == fold_ids
    assert len(flattened) == len(set(flattened))


def test_fold_row_weights_count_only_first_test_year_after_lookback() -> None:
    dates = np.asarray(
        [
            "2020-01-02",
            "2020-01-03",
            "2020-01-06",
            "2020-01-07",
            "2021-01-04",
            "2021-01-05",
            "2021-01-06",
        ],
        dtype="datetime64[D]",
    )
    fold = _fold(
        1,
        test_indices=tuple(range(len(dates))),
        test_years=(2020, 2021),
    )

    weights = runner._fold_row_weights(
        [1],
        {1: fold},
        SimpleNamespace(dates=dates),
        lookback=2,
    )

    assert weights == {1: 3}


def test_run_fold_forces_first_test_year_all_dates_and_lazy_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dates = np.asarray(
        ["2020-01-02", "2020-01-03", "2020-01-06", "2021-01-04"],
        dtype="datetime64[D]",
    )
    panel = SimpleNamespace(
        dates=dates,
        feature_names=["return_1d"],
        symbols=["A", "B"],
    )
    fold = _fold(1, test_indices=(0, 1, 2, 3), test_years=(2020, 2021))
    config = SimpleNamespace(training=SimpleNamespace(lookback=2))
    args = SimpleNamespace(strict=True)
    class FakeDataset:
        def __len__(self) -> int:
            return 2

    dataset = FakeDataset()
    source = SimpleNamespace(date_indices=np.asarray([1, 2], dtype=np.int64))
    calls: dict[str, Any] = {}

    monkeypatch.setattr(runner, "_align_panel_to_checkpoint_universe", lambda *values: panel)
    monkeypatch.setattr(
        runner,
        "load_model_from_checkpoint",
        lambda *values, **kwargs: (torch.nn.Identity(), {"checkpoint_epoch": 7}),
    )

    def fake_first_test_year_dataset(
        received_panel: Any,
        received_fold: Any,
        lookback: int,
    ) -> Any:
        calls["dataset"] = (
            received_panel,
            received_fold,
            lookback,
        )
        return dataset

    def fake_sample_dataset_source(
        received_dataset: Any,
        max_rows: int,
        method: str,
    ) -> Any:
        calls["sample"] = (received_dataset, max_rows, method)
        return source

    def fake_cross_asset(model: Any, batch: Any, **kwargs: Any) -> dict[str, Any]:
        calls["batch"] = batch
        calls["dates"] = kwargs["dates"]
        return {"sources": 2, "targets": 2}

    monkeypatch.setattr(runner, "_first_test_year_dataset", fake_first_test_year_dataset)
    monkeypatch.setattr(runner, "_sample_dataset_source", fake_sample_dataset_source)
    monkeypatch.setattr(runner, "abstract_cross_asset_transmission", fake_cross_asset)
    monkeypatch.setattr(runner, "_clear_explainability_runtime_cache", lambda: None)
    monkeypatch.setattr(runner, "_distributed_rank", lambda: 0)

    timing = runner._run_fold(
        args=args,
        config=config,
        panel=panel,
        fold=fold,
        training_output_dir=tmp_path / "training",
        cross_asset_output_root=tmp_path / "cross_asset",
        device=torch.device("cpu"),
        settings=SimpleNamespace(),
        amp_dtype=None,
        amp_dtype_name="none",
    )

    assert calls["dataset"][2:] == (2,)
    assert calls["sample"] == (dataset, 0, "even")
    assert calls["batch"] is source
    assert calls["dates"] == ["2020-01-03", "2020-01-06"]
    assert timing["test_year"] == 2020
    assert timing["first_test_year_only"] is True
    assert timing["exhaustive_dates"] is True
    assert timing["amp_dtype"] == "none"
    assert timing["sample_rows"] == timing["split_rows"] == 2


def test_fold_rounds_gather_every_rank_timing() -> None:
    assignments = [[1], [2]]

    def gather(payload: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            payload,
            {
                "rank": 1,
                "round": 0,
                "fold_id": 2,
                "timing": {"fold_id": 2, "elapsed_s": 2.0},
                "error": None,
            },
        ]

    timings = runner._execute_fold_rounds(
        assignments,
        rank=0,
        run_fold=lambda fold_id: {"fold_id": fold_id, "elapsed_s": 1.0},
        gather_payloads=gather,
    )

    assert [timing["fold_id"] for timing in timings] == [1, 2]


def test_fold_rounds_collect_local_exception_before_propagating() -> None:
    assignments = [[1], [2]]
    collective_was_called = False

    def fail_fold(_fold_id: int) -> dict[str, Any]:
        raise ValueError("intentional fold failure")

    def gather(payload: dict[str, Any]) -> list[dict[str, Any]]:
        nonlocal collective_was_called
        collective_was_called = True
        assert payload["error"]["type"] == "ValueError"
        return [
            payload,
            {
                "rank": 1,
                "round": 0,
                "fold_id": 2,
                "timing": {"fold_id": 2},
                "error": None,
            },
        ]

    with pytest.raises(RuntimeError, match="intentional fold failure"):
        runner._execute_fold_rounds(
            assignments,
            rank=0,
            run_fold=fail_fold,
            gather_payloads=gather,
        )

    assert collective_was_called is True


def test_idle_rank_still_participates_in_fold_round_collective() -> None:
    assignments = [[1], []]
    run_calls: list[int] = []

    def gather(payload: dict[str, Any]) -> list[dict[str, Any]]:
        assert payload["fold_id"] is None
        return [
            {
                "rank": 0,
                "round": 0,
                "fold_id": 1,
                "timing": {"fold_id": 1},
                "error": None,
            },
            payload,
        ]

    timings = runner._execute_fold_rounds(
        assignments,
        rank=1,
        run_fold=lambda fold_id: run_calls.append(fold_id) or {"fold_id": fold_id},
        gather_payloads=gather,
    )

    assert run_calls == []
    assert timings == [{"fold_id": 1}]
