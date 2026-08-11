from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from stockagent.backtest.tw_execution import EXECUTION_MODES
from stockagent.training import mode_adapter


def _config(execution_mode: str) -> SimpleNamespace:
    return SimpleNamespace(
        trading=SimpleNamespace(execution_mode=execution_mode),
        data=SimpleNamespace(
            minute_parquet_root="minute-root",
            index_derivatives_tick_dataset_root="tick-root",
        ),
    )


def test_training_mode_registry_covers_every_execution_mode() -> None:
    assert set(mode_adapter.TRAINING_MODE_SPECS) == set(EXECUTION_MODES)
    for execution_mode in EXECUTION_MODES:
        spec = mode_adapter.training_mode_spec(execution_mode)
        assert spec.execution_mode == execution_mode
        assert spec.product_family
        assert spec.frequency
        assert spec.decision_clock
        assert spec.execution_clock
        assert spec.recurrent_state_scope
        assert spec.terminal_policy
        assert spec.split_ownership
        assert spec.sample_order_contract
        assert spec.benchmark_contract
        assert spec.weight_snapshot_contract
        assert spec.turnover_contract


def test_day_count_training_group_name_is_stable() -> None:
    assert mode_adapter.date_training_group_name(
        np.asarray(["2026-08-01", "2026-08-03"], dtype="datetime64[D]")
    ) == "train_20260801-20260803"
    with pytest.raises(ValueError, match="strictly increasing"):
        mode_adapter.date_training_group_name(
            np.asarray(["2026-08-03", "2026-08-01"], dtype="datetime64[D]")
        )


def test_chronological_session_indices_never_shuffle_or_duplicate() -> None:
    assert mode_adapter.chronological_session_indices(
        np.asarray([2, 4, 7], dtype=np.int64)
    ) == [2, 4, 7]
    with pytest.raises(ValueError, match="strictly increasing"):
        mode_adapter.chronological_session_indices([2, 1, 3])
    with pytest.raises(ValueError, match="strictly increasing"):
        mode_adapter.chronological_session_indices([1, 1, 2])


@pytest.mark.parametrize(
    ("execution_mode", "product_family", "frequency"),
    [
        ("tw_minute", "taiwan_stock_etf", "one_minute"),
        ("tw_index_derivatives_tick", "taiwan_index_futures_tx", "completed_second"),
        (
            "tw_index_options_tick_long",
            "taiwan_index_options_txo_long_only",
            "completed_second",
        ),
        (
            "tw_index_options_tick_short",
            "taiwan_index_options_txo_margin_short_enabled",
            "completed_second",
        ),
    ],
)
def test_specialized_mode_contracts_are_explicit(
    execution_mode: str,
    product_family: str,
    frequency: str,
) -> None:
    spec = mode_adapter.training_mode_spec(execution_mode)
    assert spec.specialized
    assert spec.product_family == product_family
    assert spec.frequency == frequency
    assert spec.sample_order_contract == "chronological_sessions"
    assert spec.supports_isolated_folds is (execution_mode == "tw_minute")


def test_canonical_mode_is_not_dispatched_to_a_second_runner(tmp_path: Path) -> None:
    handled = mode_adapter.dispatch_specialized_training_mode(
        _config("tw_day_trade"),
        output_dir=tmp_path,
        mode="train",
        resume=True,
        start_fold=None,
        max_folds=None,
        active_strategy="none",
        isolate_train_folds=False,
    )
    assert handled is False


def test_specialized_dispatch_uses_registry_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []
    checkpoints: list[tuple[str, dict[str, object]]] = []

    def fake_runner(config: object, **kwargs: object) -> None:
        calls.append({"config": config, **kwargs})

    monkeypatch.setattr(
        mode_adapter,
        "import_module",
        lambda _name: SimpleNamespace(run_minute_training=fake_runner),
    )

    handled = mode_adapter.dispatch_specialized_training_mode(
        _config("tw_minute"),
        output_dir=tmp_path,
        mode="train",
        resume=True,
        start_fold=2,
        max_folds=3,
        active_strategy="none",
        isolate_train_folds=False,
        startup_checkpoint=lambda stage, **details: checkpoints.append(
            (stage, details)
        ),
    )

    assert handled is True
    assert len(calls) == 1
    assert calls[0]["output_dir"] == tmp_path
    assert calls[0]["start_fold"] == 2
    assert calls[0]["max_folds"] == 3
    assert checkpoints[0][0] == "tw_minute_runner_dispatch"
    assert checkpoints[0][1]["dataset_root"] == "minute-root"
    assert checkpoints[0][1]["frequency"] == "one_minute"
    assert checkpoints[-1][0] == "tw_minute_runner_complete"


def test_specialized_dispatch_rejects_unsupported_fold_isolation(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="canonical in-process group checkpoint"):
        mode_adapter.dispatch_specialized_training_mode(
            _config("tw_index_options_tick_short"),
            output_dir=tmp_path,
            mode="train",
            resume=True,
            start_fold=None,
            max_folds=None,
            active_strategy="none",
            isolate_train_folds=True,
        )


def test_tw_minute_dispatch_selects_isolated_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def fake_isolated_runner(config: object, **kwargs: object) -> None:
        calls.append({"config": config, **kwargs})

    monkeypatch.setattr(
        mode_adapter,
        "import_module",
        lambda _name: SimpleNamespace(
            run_minute_training_isolated=fake_isolated_runner
        ),
    )

    handled = mode_adapter.dispatch_specialized_training_mode(
        _config("tw_minute"),
        output_dir=tmp_path,
        mode="train",
        resume=True,
        start_fold=3,
        max_folds=2,
        active_strategy="distributed_data_parallel",
        isolate_train_folds=True,
    )

    assert handled is True
    assert len(calls) == 1
    assert calls[0]["start_fold"] == 3
    assert calls[0]["max_folds"] == 2
