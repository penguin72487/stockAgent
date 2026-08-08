from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import polars as pl
import pytest
import torch

from stockagent.backtest.tw_minute import (
    MinuteExecutionConfig,
    execute_minute_target,
    force_close_minute_session,
    initialize_minute_execution_state,
    minute_target_weights_from_logits,
)
from stockagent.config import load_config
from stockagent.data.tw_minute import (
    MINUTE_FEATURE_COLUMNS,
    MinuteDatasetIndex,
    MinuteDayPanel,
    MinuteFeatureNormalizer,
    _load_minute_short_rules,
    load_minute_dataset_index,
)
from stockagent.training.minute import (
    _MinuteSlabForwardAdapter,
    _build_windows,
    _execute_minute_chunk,
    _first_calendar_year_indices,
    _global_session_batches,
    _minute_benchmark_log_return,
    _minute_chunk_training_loss,
    run_minute_training,
)
from stockagent.training.lifecycle import validate_completed_training_artifacts
from stockagent.training import minute as minute_training
from stockagent.models.factory import build_model
from stockagent.models.transformer_base_portfolio import (
    action_channels_for_execution_mode,
)


def _config() -> MinuteExecutionConfig:
    return MinuteExecutionConfig(
        initial_equity=1_000_000.0,
        gross_exposure=0.9,
        maximum_name_weight=0.6,
        maximum_volume_participation=0.1,
        maximum_order_notional=1_000_000.0,
        slippage_bps_per_side=2.0,
    )


def test_first_calendar_year_indices_selects_only_first_test_year() -> None:
    dates = np.asarray(
        [
            "2022-12-30",
            "2023-01-03",
            "2023-12-29",
            "2024-01-02",
            "2025-01-02",
        ],
        dtype="datetime64[D]",
    )

    year, indices = _first_calendar_year_indices(dates, [1, 2, 3, 4])

    assert year == 2023
    assert indices.tolist() == [1, 2]


def test_minute_normalizer_rejects_negative_variance_statistics(
    tmp_path: Path,
) -> None:
    valid = {
        "feature_counts": {name: 2 for name in MINUTE_FEATURE_COLUMNS},
        "feature_sums": {name: 0.0 for name in MINUTE_FEATURE_COLUMNS},
        "feature_sum_squares": {name: 2.0 for name in MINUTE_FEATURE_COLUMNS},
    }
    valid["feature_sums"]["minutes_from_open"] = 200.0
    valid["feature_sum_squares"]["minutes_from_open"] = 1.0
    dataset = MinuteDatasetIndex(
        root=tmp_path,
        manifest={},
        symbols=("2330",),
        dates=np.asarray(["2026-01-02"], dtype="datetime64[D]"),
        partitions={"2026-01-02": valid},
    )

    with pytest.raises(RuntimeError, match="negative variance"):
        dataset.fit_normalizer([0])


def test_minute_group_runtime_release_resets_compiler_on_cpu(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(torch.compiler, "reset", lambda: calls.append("reset"))
    monkeypatch.setattr(minute_training.gc, "collect", lambda: calls.append("gc"))

    released = minute_training._release_minute_group_runtime(torch.device("cpu"))

    assert calls == ["reset", "gc"]
    assert released == {
        "allocated_before": 0,
        "allocated_after": 0,
        "reserved_before": 0,
        "reserved_after": 0,
    }


def _step(state, target: torch.Tensor, *, allow_new_entries: bool = True):
    return execute_minute_target(
        state,
        target_weights=target,
        execution_mask=torch.tensor([True, True]),
        execution_open=torch.tensor([100.0, 50.0]),
        exit_close=torch.tensor([100.0, 50.0]),
        future_volume_shares=torch.tensor([1_000_000.0, 1_000_000.0]),
        buy_fee_rates=torch.tensor([0.001, 0.001]),
        sell_fee_rates=torch.tensor([0.002, 0.002]),
        config=_config(),
        allow_new_entries=allow_new_entries,
    )


def test_minute_split_is_chronological_and_progress_counts_sessions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_batches: list[list[int]] = []
    seen_chunk_rows: list[int] = []
    progress_instances: list[object] = []

    class FakeCache:
        hits = 0
        misses = 0
        cache_bytes = 0

        def get_many(self, indices: list[int]) -> list[int]:
            self.misses += len(indices)
            return list(indices)

    class FakeProgress:
        def __init__(self, *, total: int) -> None:
            self.total = total
            self.updated = 0
            self.postfixes: list[dict[str, object]] = []

        def update(self, amount: int) -> None:
            self.updated += amount

        def set_postfix(
            self, values: dict[str, object], *, refresh: bool = True
        ) -> None:
            del refresh
            self.postfixes.append(values)

        def close(self) -> None:
            return None

    def fake_progress_bar(*_args, total=None, **_kwargs):
        progress = FakeProgress(total=int(total))
        progress_instances.append(progress)
        return progress

    def fake_run_day_batch(*, days, **kwargs):
        indices = [int(value) for value in days]
        seen_batches.append(indices)
        seen_chunk_rows.append(int(kwargs["decision_chunk_rows"]))
        rows = [
            {
                "date": str(np.datetime64("2020-01-01") + np.timedelta64(index, "D")),
                "net_return": 0.0,
                "log_return": 0.0,
                "turnover_notional": 0.0,
                "explicit_fees": 0.0,
                "slippage_cost": 0.0,
            }
            for index in indices
        ]
        return (
            rows,
            np.zeros((len(indices), 1), dtype=np.float64),
            minute_training.TimingBreakdown(batches=1),
        )

    monkeypatch.setattr(minute_training, "_progress_bar", fake_progress_bar)
    monkeypatch.setattr(minute_training, "_run_day_batch", fake_run_day_batch)
    config = SimpleNamespace(
        training=SimpleNamespace(
            batch_size_train=2,
            batch_size_eval=3,
            minute_decision_chunk_rows=10,
            minute_eval_decision_chunk_rows=4,
            grad_clip_norm=1.0,
        )
    )

    _summary, rows, _weights, timing = minute_training._run_split(
        name="chronological-train",
        indices=np.asarray([0, 1, 2, 3, 4], dtype=np.int64),
        dataset=SimpleNamespace(
            dates=np.arange(
                np.datetime64("2020-01-01"),
                np.datetime64("2020-01-06"),
                dtype="datetime64[D]",
            )
        ),
        normalizer=object(),
        model=torch.nn.Identity(),
        model_forward=lambda *_args: None,
        execution_forward=lambda *_args: (),
        day_cache=FakeCache(),
        config=config,
        execution_config=_config(),
        buy_fee_rates=torch.zeros(1),
        sell_fee_rates=torch.zeros(1),
        device=torch.device("cpu"),
        amp_dtype=None,
        optimizer=object(),
        benchmark_symbol_index=0,
    )

    assert seen_batches == [[0, 1], [2, 3], [4]]
    assert [row["date"] for row in rows] == [
        "2020-01-01",
        "2020-01-02",
        "2020-01-03",
        "2020-01-04",
        "2020-01-05",
    ]
    progress = progress_instances[0]
    assert isinstance(progress, FakeProgress)
    assert progress.total == 5
    assert progress.updated == 5
    assert timing.batches == 3
    assert seen_chunk_rows == [10, 10, 10]

    seen_batches.clear()
    seen_chunk_rows.clear()
    minute_training._run_split(
        name="chronological-eval",
        indices=np.asarray([0, 1, 2, 3, 4], dtype=np.int64),
        dataset=SimpleNamespace(
            dates=np.arange(
                np.datetime64("2020-01-01"),
                np.datetime64("2020-01-06"),
                dtype="datetime64[D]",
            )
        ),
        normalizer=object(),
        model=torch.nn.Identity(),
        model_forward=lambda *_args: None,
        execution_forward=lambda *_args: (),
        day_cache=FakeCache(),
        config=config,
        execution_config=_config(),
        buy_fee_rates=torch.zeros(1),
        sell_fee_rates=torch.zeros(1),
        device=torch.device("cpu"),
        amp_dtype=None,
        optimizer=None,
        benchmark_symbol_index=0,
    )
    assert seen_batches == [[0, 1, 2], [3, 4]]
    assert seen_chunk_rows == [4, 4]


def test_minute_ddp_global_batches_preserve_every_chronological_session() -> None:
    batches = _global_session_batches(
        range(19),
        global_batch_size=8,
        world_size=2,
    )

    assert [index for batch in batches for index in batch] == list(range(19))
    assert all(len(batch[0::2]) >= 1 for batch in batches)
    assert all(len(batch[1::2]) >= 1 for batch in batches)
    assert [len(batch) for batch in batches] == [7, 6, 6]
    with pytest.raises(ValueError, match="global batch size"):
        _global_session_batches(range(8), global_batch_size=1, world_size=2)


def test_minute_ddp_ragged_loss_matches_global_batch_gradient() -> None:
    parameter = torch.tensor(2.0, requires_grad=True)
    rank0_utility = parameter * torch.tensor([1.0, 2.0])
    rank1_utility = parameter * torch.tensor([3.0])
    rank0_loss = _minute_chunk_training_loss(
        rank0_utility,
        real_days=2,
        global_real_days=3,
        decisions_per_day=5,
        gradient_world_size=2,
    )
    rank1_loss = _minute_chunk_training_loss(
        rank1_utility,
        real_days=1,
        global_real_days=3,
        decisions_per_day=5,
        gradient_world_size=2,
    )
    ddp_averaged_loss = (rank0_loss + rank1_loss) / 2.0
    expected_loss = -(parameter * torch.tensor([1.0, 2.0, 3.0])).sum() / 15.0

    torch.testing.assert_close(ddp_averaged_loss, expected_loss)
    actual_gradient = torch.autograd.grad(ddp_averaged_loss, parameter)[0]
    expected_gradient = torch.autograd.grad(expected_loss, parameter)[0]
    torch.testing.assert_close(actual_gradient, expected_gradient)


def test_minute_empty_source_partition_preserves_missing_benchmark() -> None:
    day = MinuteDayPanel(
        trade_date=np.datetime64("2020-09-27"),
        features=np.zeros((270, 2, len(MINUTE_FEATURE_COLUMNS)), dtype=np.float32),
        feature_mask=np.zeros((270, 2), dtype=bool),
        execution_mask=np.zeros((270, 2), dtype=bool),
        execution_open=np.zeros((270, 2), dtype=np.float32),
        exit_close=np.zeros((270, 2), dtype=np.float32),
        future_volume_shares=np.zeros((270, 2), dtype=np.float32),
        session_close=np.zeros(2, dtype=np.float32),
        session_exit_mask=np.zeros(2, dtype=bool),
    )

    assert math.isnan(
        _minute_benchmark_log_return(
            day,
            first_execution_index=31,
            benchmark_symbol_index=0,
        )
    )

    day.feature_mask[31, 1] = True
    assert math.isnan(
        _minute_benchmark_log_return(
            day,
            first_execution_index=31,
            benchmark_symbol_index=0,
        )
    )


def test_minute_logits_are_masked_capped_and_leave_cash() -> None:
    weights = minute_target_weights_from_logits(
        torch.tensor([[10.0, 0.0, -10.0]]),
        torch.tensor([[True, True, False]]),
        gross_exposure=0.9,
        maximum_name_weight=0.4,
    )

    assert weights[0, 0] == pytest.approx(0.4)
    assert weights[0, 2] == 0.0
    assert float(weights.sum()) < 0.9


def test_minute_long_short_targets_use_raw_l1_and_fail_closed() -> None:
    logits = torch.tensor([[2.0, 1.0, -1.0, -2.0]], requires_grad=True)
    tradable = torch.ones_like(logits, dtype=torch.bool)
    shortable = torch.tensor([[True, True, False, True]])

    actual = minute_target_weights_from_logits(
        logits,
        tradable,
        gross_exposure=1.0,
        maximum_name_weight=1.0,
        long_only=False,
        shortable_mask=shortable,
    )
    torch.testing.assert_close(actual, torch.tensor([[0.4, 0.2, 0.0, -0.4]]))
    assert float(actual.detach().abs().sum()) == pytest.approx(1.0)
    actual.sum().backward()
    assert logits.grad is not None
    assert bool(torch.isfinite(logits.grad).all())


def test_minute_long_short_executor_enforces_capacity_and_covers_at_close() -> None:
    config = MinuteExecutionConfig(
        initial_equity=1_000_000.0,
        gross_exposure=1.0,
        maximum_name_weight=1.0,
        maximum_volume_participation=1.0,
        maximum_order_notional=1_000_000.0,
        slippage_bps_per_side=0.0,
        long_only=False,
    )
    state = initialize_minute_execution_state(
        num_symbols=2,
        config=config,
        device=torch.device("cpu"),
    )
    opened = execute_minute_target(
        state,
        target_weights=torch.tensor([-0.5, 0.5]),
        execution_mask=torch.tensor([True, True]),
        execution_open=torch.tensor([100.0, 100.0]),
        exit_close=torch.tensor([100.0, 100.0]),
        future_volume_shares=torch.tensor([1_000_000.0, 1_000_000.0]),
        buy_fee_rates=torch.tensor([0.001, 0.001]),
        sell_fee_rates=torch.tensor([0.002, 0.002]),
        config=config,
        allow_new_entries=True,
        short_open_mask=torch.tensor([True, False]),
        short_capacity_shares=torch.tensor([2_000.0, 0.0]),
    )

    assert float(opened.state.shares[0]) * config.initial_equity == pytest.approx(
        -2_000.0
    )
    assert float(opened.state.shares[1]) > 0.0
    closed = force_close_minute_session(
        opened.state,
        session_close=torch.tensor([100.0, 100.0]),
        session_exit_mask=torch.tensor([True, True]),
        last_minute_volume_shares=torch.tensor([1_000_000.0, 1_000_000.0]),
        sell_fee_rates=torch.tensor([0.002, 0.002]),
        buy_fee_rates=torch.tensor([0.001, 0.001]),
        config=config,
    )
    torch.testing.assert_close(closed.state.shares, torch.zeros(2))
    # Short cover uses the buy fee while the long liquidation uses sell fees.
    expected_close_fees = 2_000.0 * 100.0 * 0.001 + float(
        opened.state.shares[1]
    ) * config.initial_equity * 100.0 * 0.002
    assert float(closed.explicit_fees) == pytest.approx(expected_close_fees)


def test_minute_short_rule_sidecar_uses_exact_day_and_one_session_shift(
    tmp_path: Path,
) -> None:
    path = tmp_path / "tw_public.parquet"
    dates = np.asarray(
        ["2026-01-02", "2026-01-05", "2026-01-06"],
        dtype="datetime64[D]",
    )
    pl.DataFrame(
        {
            "date": [
                datetime(2026, 1, 2).date(),
                datetime(2026, 1, 2).date(),
                datetime(2026, 1, 5).date(),
                datetime(2026, 1, 5).date(),
                datetime(2026, 1, 6).date(),
                datetime(2026, 1, 6).date(),
            ],
            "symbol": ["0050", "2330"] * 3,
            "_twpub_day_trade_eligible": [1.0] * 6,
            # Exact-session direction blocks 0050 on the middle session.
            "_twpub_day_trade_short_open": [1.0, 1.0, 0.0, 1.0, 1.0, 1.0],
            # The missing middle-session evidence must not forward-fill.
            "_twpub_margin_short_evidence_next_session": [
                1.0,
                1.0,
                None,
                1.0,
                1.0,
                1.0,
            ],
            "_twpub_short_capacity_shares_next_session": [
                1_000.0,
                2_000.0,
                None,
                3_000.0,
                4_000.0,
                5_000.0,
            ],
        }
    ).write_parquet(path)

    shortable, capacity, fingerprint = _load_minute_short_rules(
        path,
        symbols=("0050", "2330"),
        dates=dates,
    )

    assert shortable.tolist() == [
        [False, False],
        [False, True],
        [False, True],
    ]
    assert capacity.tolist() == [[0, 0], [0, 2_000], [0, 3_000]]
    assert len(fingerprint) == 64


def test_minute_cash_outside_option_is_universe_invariant_and_learnable() -> None:
    learnable_logits = torch.zeros(1, 8, requires_grad=True)
    small = minute_target_weights_from_logits(
        torch.zeros(1, 2),
        torch.ones(1, 2, dtype=torch.bool),
        gross_exposure=0.9,
        maximum_name_weight=1.0,
        outside_cash_logit=4.0,
    )
    large = minute_target_weights_from_logits(
        learnable_logits,
        torch.ones(1, 8, dtype=torch.bool),
        gross_exposure=0.9,
        maximum_name_weight=1.0,
        outside_cash_logit=4.0,
    )
    shifted = minute_target_weights_from_logits(
        torch.full((1, 8), 4.0),
        torch.ones(1, 8, dtype=torch.bool),
        gross_exposure=0.9,
        maximum_name_weight=1.0,
        outside_cash_logit=4.0,
    )

    torch.testing.assert_close(small.sum(), large.sum())
    torch.testing.assert_close(
        small.sum(),
        torch.tensor(0.9 * torch.sigmoid(torch.tensor(-4.0)).item()),
    )
    torch.testing.assert_close(shifted.sum(), torch.tensor(0.45))
    large.sum().backward()
    assert learnable_logits.grad is not None
    assert bool(torch.all(learnable_logits.grad > 0.0))

    reference_logits = torch.tensor([[1.0, -2.0, 0.5, 3.0]])
    reference_mask = torch.tensor([[True, False, True, True]])
    actual = minute_target_weights_from_logits(
        reference_logits,
        reference_mask,
        gross_exposure=0.5,
        maximum_name_weight=1.0,
        outside_cash_logit=4.0,
    )
    safe = reference_logits.masked_fill(
        ~reference_mask, torch.finfo(torch.float32).min
    )
    conditional = torch.softmax(safe, dim=1).masked_fill(~reference_mask, 0.0)
    log_mean_exp = torch.logsumexp(safe, dim=1, keepdim=True) - math.log(3.0)
    expected = conditional * 0.5 * torch.sigmoid(log_mean_exp - 4.0)
    torch.testing.assert_close(actual, expected)


def test_minute_uncapped_daytrade_rules_keep_only_volume_capacity() -> None:
    weights = minute_target_weights_from_logits(
        torch.tensor([[8.0, 0.0]]),
        torch.ones(1, 2, dtype=torch.bool),
        gross_exposure=1.0,
        maximum_name_weight=None,
        outside_cash_logit=None,
        long_only=True,
    )
    assert float(weights[0, 0]) > 0.99

    config = MinuteExecutionConfig(
        initial_equity=1_000_000.0,
        gross_exposure=1.0,
        maximum_name_weight=None,
        maximum_volume_participation=0.5,
        maximum_order_notional=None,
        slippage_bps_per_side=0.0,
    )
    state = initialize_minute_execution_state(
        num_symbols=1,
        config=config,
        device=torch.device("cpu"),
    )
    result = execute_minute_target(
        state,
        target_weights=torch.tensor([1.0]),
        execution_mask=torch.tensor([True]),
        execution_open=torch.tensor([100.0]),
        exit_close=torch.tensor([100.0]),
        future_volume_shares=torch.tensor([10_000.0]),
        buy_fee_rates=torch.tensor([0.0]),
        sell_fee_rates=torch.tensor([0.0]),
        config=config,
        allow_new_entries=True,
    )

    # The request is 10,000 shares; with no notional ceiling, only 50% of the
    # 10,000-share next KBar volume limits the fill.
    torch.testing.assert_close(
        result.state.shares * config.initial_equity,
        torch.tensor([5_000.0]),
    )
    torch.testing.assert_close(result.slippage_cost, torch.tensor(0.0))


def test_minute_slab_adapter_fuses_identical_target_construction() -> None:
    class FakeSlabModel(torch.nn.Module):
        def forward_from_batched_panel_slabs(
            self,
            feature_slabs: torch.Tensor,
            mask: torch.Tensor,
            *,
            return_aux: bool,
        ) -> torch.Tensor:
            del feature_slabs, return_aux
            return torch.arange(mask.numel(), dtype=torch.float32).reshape(
                -1, mask.size(-1)
            ) / 8.0

    config = MinuteExecutionConfig(
        initial_equity=1_000_000.0,
        gross_exposure=0.5,
        maximum_name_weight=0.25,
        maximum_volume_participation=0.1,
        maximum_order_notional=100_000.0,
        slippage_bps_per_side=1.0,
        outside_cash_logit=4.0,
    )
    mask = torch.tensor(
        [
            [[True, True, False], [True, False, True]],
            [[False, True, True], [True, True, True]],
        ]
    )
    features = torch.zeros(2, 3, 3, 1)
    adapter = _MinuteSlabForwardAdapter(FakeSlabModel(), config)
    actual = adapter(features, mask)
    logits = torch.arange(mask.numel(), dtype=torch.float32).reshape(-1, 3) / 8.0
    expected = minute_target_weights_from_logits(
        logits,
        mask.reshape(-1, 3),
        gross_exposure=0.5,
        maximum_name_weight=0.25,
        outside_cash_logit=4.0,
    )

    torch.testing.assert_close(actual, expected)


def test_minute_daily_projection_adapter_preserves_implicit_and_blocked_cash() -> None:
    class FakeDailyProjectionModel(torch.nn.Module):
        portfolio_output_mode = "projection_l1"

        def forward_from_batched_panel_slabs(
            self,
            feature_slabs: torch.Tensor,
            mask: torch.Tensor,
            *,
            return_aux: bool,
        ) -> torch.Tensor:
            del feature_slabs, mask, return_aux
            # This is the already-resolved daily L1-ball request with 25% cash.
            # The first symbol's short is an executor rejection, not permission
            # to renormalize the two remaining longs back to gross one.
            return torch.tensor([[-0.25, 0.25, 0.25]])

    config = MinuteExecutionConfig(
        initial_equity=10_000_000.0,
        gross_exposure=1.0,
        maximum_name_weight=None,
        maximum_volume_participation=0.5,
        maximum_order_notional=None,
        slippage_bps_per_side=0.0,
        outside_cash_logit=None,
        long_only=False,
    )
    adapter = _MinuteSlabForwardAdapter(FakeDailyProjectionModel(), config)
    actual = adapter(
        torch.zeros(1, 1, 3, 1),
        torch.ones(1, 1, 3, dtype=torch.bool),
        torch.tensor([[[False, True, True]]]),
    )

    torch.testing.assert_close(actual, torch.tensor([[-0.25, 0.25, 0.25]]))
    executable = torch.where(
        (actual < 0.0) & torch.tensor([[True, False, False]]),
        torch.zeros_like(actual),
        actual,
    )
    assert float(executable.abs().sum()) == pytest.approx(0.5)
    assert 1.0 - float(executable.abs().sum()) == pytest.approx(0.5)


def test_minute_executor_trades_only_target_delta_and_forces_flat() -> None:
    state = initialize_minute_execution_state(
        num_symbols=2,
        config=_config(),
        device=torch.device("cpu"),
    )
    first = _step(state, torch.tensor([0.5, 0.0]))
    second = _step(first.state, torch.tensor([0.5, 0.0]))

    assert float(first.turnover_notional) > 0.0
    assert float(second.turnover_notional) < float(first.turnover_notional) * 0.01
    closed = force_close_minute_session(
        second.state,
        session_close=torch.tensor([100.0, 50.0]),
        session_exit_mask=torch.tensor([True, True]),
        last_minute_volume_shares=torch.tensor([1_000_000.0, 1_000_000.0]),
        sell_fee_rates=torch.tensor([0.002, 0.002]),
        config=_config(),
    )
    assert torch.count_nonzero(closed.state.shares) == 0
    assert 0.0 < float(closed.state.equity) < 1.0


def test_normalized_minute_ledger_resolves_sub_twd_fees_at_ten_million_nav() -> None:
    config = MinuteExecutionConfig(
        initial_equity=10_000_000.0,
        gross_exposure=1.0,
        maximum_name_weight=None,
        maximum_volume_participation=0.5,
        maximum_order_notional=None,
        slippage_bps_per_side=0.0,
    )
    state = initialize_minute_execution_state(
        num_symbols=1,
        config=config,
        device=torch.device("cpu"),
    )
    opened = execute_minute_target(
        state,
        target_weights=torch.tensor([1e-6]),
        execution_mask=torch.tensor([True]),
        execution_open=torch.tensor([100.0]),
        exit_close=torch.tensor([100.0]),
        future_volume_shares=torch.tensor([1_000_000.0]),
        buy_fee_rates=torch.tensor([0.001]),
        sell_fee_rates=torch.tensor([0.002]),
        config=config,
        allow_new_entries=True,
    )
    closed = force_close_minute_session(
        opened.state,
        session_close=torch.tensor([100.0]),
        session_exit_mask=torch.tensor([True]),
        last_minute_volume_shares=torch.tensor([1_000_000.0]),
        sell_fee_rates=torch.tensor([0.002]),
        config=config,
    )

    # NT$10 notional pays NT$0.01 on entry and NT$0.02 on exit. Currency-FP32
    # accounting at NT$10m rounds both away; normalized FP32 retains them in
    # the direct return path without requiring slow GPU FP64.
    assert float(opened.explicit_fees) == pytest.approx(0.01, abs=1e-6)
    assert float(closed.explicit_fees) == pytest.approx(0.02, abs=1e-6)
    assert float(opened.net_return) == pytest.approx(-1e-9, rel=1e-5)
    assert float(closed.net_return) == pytest.approx(-2e-9, rel=1e-5)


def test_last_decision_cannot_open_new_inventory() -> None:
    state = initialize_minute_execution_state(
        num_symbols=2,
        config=_config(),
        device=torch.device("cpu"),
    )
    result = _step(state, torch.tensor([0.5, 0.0]), allow_new_entries=False)

    assert torch.count_nonzero(result.state.shares) == 0
    assert float(result.turnover_notional) == 0.0


def test_minute_execution_is_differentiable_through_fees_and_price_move() -> None:
    state = initialize_minute_execution_state(
        num_symbols=2,
        config=_config(),
        device=torch.device("cpu"),
    )
    logits = torch.tensor([[1.0, 0.0]], requires_grad=True)
    target = minute_target_weights_from_logits(
        logits,
        torch.tensor([[True, True]]),
        gross_exposure=0.9,
        maximum_name_weight=0.6,
    )[0]
    result = execute_minute_target(
        state,
        target_weights=target,
        execution_mask=torch.tensor([True, True]),
        execution_open=torch.tensor([100.0, 50.0]),
        exit_close=torch.tensor([101.0, 49.0]),
        future_volume_shares=torch.tensor([1_000_000.0, 1_000_000.0]),
        buy_fee_rates=torch.tensor([0.001, 0.001]),
        sell_fee_rates=torch.tensor([0.002, 0.002]),
        config=_config(),
        allow_new_entries=True,
    )
    loss = -torch.log1p(result.net_return)
    loss.backward()

    assert logits.grad is not None
    assert torch.isfinite(logits.grad).all()
    assert float(logits.grad.abs().sum()) > 0.0


def test_batched_minute_executor_matches_independent_day_oracle_and_gradients() -> None:
    batched_logits = torch.tensor(
        [[1.0, -0.25], [-0.5, 0.75]], requires_grad=True
    )
    mask = torch.ones_like(batched_logits, dtype=torch.bool)
    batched_target = minute_target_weights_from_logits(
        batched_logits,
        mask,
        gross_exposure=0.9,
        maximum_name_weight=0.6,
    )
    batched_state = initialize_minute_execution_state(
        num_symbols=2,
        config=_config(),
        device=torch.device("cpu"),
        batch_size=2,
    )
    opens = torch.tensor([[100.0, 50.0], [80.0, 40.0]])
    closes = torch.tensor([[101.0, 49.0], [79.0, 42.0]])
    volumes = torch.full((2, 2), 1_000_000.0)
    buy_rates = torch.tensor([0.001, 0.001])
    sell_rates = torch.tensor([0.002, 0.002])
    batched = execute_minute_target(
        batched_state,
        target_weights=batched_target,
        execution_mask=mask,
        execution_open=opens,
        exit_close=closes,
        future_volume_shares=volumes,
        buy_fee_rates=buy_rates,
        sell_fee_rates=sell_rates,
        config=_config(),
        allow_new_entries=True,
    )
    batched_loss = -torch.log1p(batched.net_return).sum()
    batched_loss.backward()

    oracle_logits = batched_logits.detach().clone().requires_grad_(True)
    oracle_equity: list[torch.Tensor] = []
    oracle_returns: list[torch.Tensor] = []
    for row in range(2):
        target = minute_target_weights_from_logits(
            oracle_logits[row : row + 1],
            mask[row : row + 1],
            gross_exposure=0.9,
            maximum_name_weight=0.6,
        )[0]
        state = initialize_minute_execution_state(
            num_symbols=2,
            config=_config(),
            device=torch.device("cpu"),
        )
        result = execute_minute_target(
            state,
            target_weights=target,
            execution_mask=mask[row],
            execution_open=opens[row],
            exit_close=closes[row],
            future_volume_shares=volumes[row],
            buy_fee_rates=buy_rates,
            sell_fee_rates=sell_rates,
            config=_config(),
            allow_new_entries=True,
        )
        oracle_equity.append(result.state.equity)
        oracle_returns.append(result.net_return)
    oracle_loss = -torch.log1p(torch.stack(oracle_returns)).sum()
    oracle_loss.backward()

    torch.testing.assert_close(batched.state.equity, torch.stack(oracle_equity))
    torch.testing.assert_close(batched.net_return, torch.stack(oracle_returns))
    torch.testing.assert_close(batched_logits.grad, oracle_logits.grad)


def test_bounded_minute_chunk_matches_sequential_batched_oracle_and_gradients() -> None:
    targets = torch.tensor(
        [
            [[0.5, 0.2], [0.4, 0.3], [0.6, 0.1]],
            [[0.2, 0.5], [0.3, 0.4], [0.1, 0.6]],
        ],
        requires_grad=True,
    )
    masks = torch.ones(2, 3, 2, dtype=torch.bool)
    opens = torch.tensor(
        [
            [[100.0, 50.0], [101.0, 49.0], [100.0, 51.0]],
            [[80.0, 40.0], [79.0, 42.0], [81.0, 41.0]],
        ]
    )
    closes = opens * torch.tensor([[[1.001, 0.999]]])
    volumes = torch.full((2, 3, 2), 1_000_000.0)
    allow_entries = torch.tensor([True, True, False])
    buy_rates = torch.tensor([0.001, 0.001])
    sell_rates = torch.tensor([0.002, 0.002])
    state = initialize_minute_execution_state(
        num_symbols=2,
        config=_config(),
        device=torch.device("cpu"),
        batch_size=2,
    )
    chunk = _execute_minute_chunk(
        state.cash,
        state.shares,
        state.last_prices,
        state.equity,
        targets,
        masks,
        opens,
        closes,
        volumes,
        allow_entries,
        buy_fee_rates=buy_rates,
        sell_fee_rates=sell_rates,
        config=_config(),
    )
    chunk_loss = -chunk[4].sum()
    chunk_loss.backward()

    oracle_targets = targets.detach().clone().requires_grad_(True)
    oracle_state = initialize_minute_execution_state(
        num_symbols=2,
        config=_config(),
        device=torch.device("cpu"),
        batch_size=2,
    )
    oracle_log = torch.zeros(2)
    oracle_turnover = torch.zeros(2)
    oracle_fees = torch.zeros(2)
    oracle_slippage = torch.zeros(2)
    for minute in range(3):
        result = execute_minute_target(
            oracle_state,
            target_weights=oracle_targets[:, minute],
            execution_mask=masks[:, minute],
            execution_open=opens[:, minute],
            exit_close=closes[:, minute],
            future_volume_shares=volumes[:, minute],
            buy_fee_rates=buy_rates,
            sell_fee_rates=sell_rates,
            config=_config(),
            allow_new_entries=allow_entries[minute],
        )
        oracle_state = result.state
        oracle_log = oracle_log + torch.log1p(
            torch.clamp(result.net_return, min=-0.999999)
        )
        oracle_turnover = oracle_turnover + result.turnover_notional
        oracle_fees = oracle_fees + result.explicit_fees
        oracle_slippage = oracle_slippage + result.slippage_cost
    (-oracle_log.sum()).backward()

    for actual, expected in zip(
        chunk[:4],
        (
            oracle_state.cash,
            oracle_state.shares,
            oracle_state.last_prices,
            oracle_state.equity,
        ),
    ):
        torch.testing.assert_close(actual, expected)
    for actual, expected in zip(
        chunk[4:],
        (oracle_log, oracle_turnover, oracle_fees, oracle_slippage),
    ):
        torch.testing.assert_close(actual, expected)
    torch.testing.assert_close(targets.grad, oracle_targets.grad)


def test_batched_panel_slabs_match_independent_canonical_slab_forwards() -> None:
    config = load_config(
        Path(__file__).parents[1] / "configs/markets/tw_minute.yaml"
    )
    config.training.lookback = 2
    config.training.enable_torch_compile = False
    config.training.transformer_base_portfolio.attention_mode = "temporal_only"
    config.training.transformer_base_portfolio.use_latent_factors = False
    config.training.transformer_base_portfolio.use_market_tokens = False
    config.training.transformer_base_portfolio.d_model = 8
    config.training.transformer_base_portfolio.temporal_layers = 1
    config.training.transformer_base_portfolio.temporal_heads = 1
    config.training.transformer_base_portfolio.temporal_ffn_mult = 1
    config.training.transformer_base_portfolio.head_hidden_dim = 8
    model = build_model(
        config=config,
        lookback=2,
        num_features=len(MINUTE_FEATURE_COLUMNS),
        num_symbols=3,
        feature_names=MINUTE_FEATURE_COLUMNS,
    ).eval()
    generator = torch.Generator().manual_seed(7)
    slabs = torch.randn(
        2, 4, 3, len(MINUTE_FEATURE_COLUMNS), generator=generator
    )
    mask = torch.tensor(
        [
            [[True, True, False], [True, True, True], [True, False, True]],
            [[True, True, True], [False, True, True], [True, True, False]],
        ]
    )

    with torch.no_grad():
        batched = model.forward_from_batched_panel_slabs(
            slabs, mask, return_aux=False
        )
        oracle = torch.cat(
            [
                model.forward_from_panel_slab(
                    slabs[day], mask[day], return_aux=False
                )
                for day in range(2)
            ],
            dim=0,
        )

    torch.testing.assert_close(batched, oracle)


def test_official_tw_minute_config_uses_a_separate_training_contract() -> None:
    config_path = Path(__file__).parents[1] / "configs/markets/tw_minute.yaml"
    config = load_config(config_path)

    assert config.trading.execution_mode == "tw_minute"
    assert config.trading.frequency == "minute"
    assert config.trading.long_only is True
    assert config.training.transformer_base_portfolio.portfolio_output_mode == "logits"
    assert config.data.minute_require_research_ready is True
    assert action_channels_for_execution_mode("tw_minute") == ("target",)


def test_dual_5090_minute_config_is_within_fold_ddp() -> None:
    config_path = (
        Path(__file__).parents[1]
        / "configs/markets/tw_minute_dual_5090.yaml"
    )
    config = load_config(config_path)

    assert config.training.multi_gpu_strategy == "distributed_data_parallel"
    assert config.training.model_name == "financial_transformer"
    assert config.training.batch_size_train == 64
    assert config.training.batch_size_eval == 16
    assert config.training.minute_decision_chunk_rows == 16
    assert config.training.minute_eval_decision_chunk_rows == 16
    assert config.training.enable_lr_scheduler is False
    assert config.training.epochs == 1000
    assert config.training.finite_check_interval_steps == 0
    assert config.training.checkpoint_finite_check is False
    assert config.training.financial_transformer.d_model == 32
    assert config.training.financial_transformer.amp_native_position_add is True
    assert config.training.financial_transformer.checkpoint_blocks is True
    assert config.training.transformer_base_portfolio.amp_native_position_add is True
    assert config.training.transformer_base_portfolio.checkpoint_blocks is True
    assert (
        config.training.transformer_base_portfolio.portfolio_output_mode
        == "l1"
    )
    assert config.training.financial_transformer.temporal_pooling == "last"
    assert config.training.financial_transformer.temporal_query_mode == "last_only"
    assert config.training.financial_transformer.portfolio_mode == "long_short"
    assert (
        config.training.financial_transformer.portfolio_output_mode
        == "l1"
    )
    assert config.trading.long_only is False
    assert config.data.use_tw_public_rules is True
    assert config.trading.max_volume_participation == 0.5
    assert config.trading.tw_minute_initial_equity == 10_000_000.0
    assert config.trading.tw_minute_gross_exposure == 1.0
    assert config.trading.tw_minute_max_name_weight is None
    assert config.trading.tw_minute_outside_cash_logit is None
    assert config.trading.tw_minute_max_order_notional is None
    assert config.trading.tw_minute_slippage_bps_per_side == 0.0
    assert config.training.curve_plot_interval == 1
    assert config.training.epoch_test_curve is True
    assert config.training.curve_test_interval == 1
    assert config.training.curve_plot_async is True
    assert config.training.defer_epoch_curve_plot_until_end is False
    assert (
        config.runner.output_dir
        == "artifacts/markets/tw_minute_dual_5090_precision_balanced_v5"
    )


def test_dual_5090_minute_trading_contract_matches_daily_day_trade() -> None:
    root = Path(__file__).parents[1]
    daily = load_config(root / "configs/markets/tw_public_lanten_market_candles.yaml")
    minute = load_config(root / "configs/markets/tw_minute_dual_5090.yaml")

    assert minute.trading.tw_minute_initial_equity == 10_000_000.0
    assert (
        minute.trading.tw_minute_initial_equity
        == daily.trading.volume_participation_equity
    )
    for field in (
        "buy_fee_rate",
        "sell_fee_rate",
        "max_volume_participation",
        "volume_participation_equity",
        "long_only",
        "portfolio_activation",
        "min_trade_weight",
        "tw_commission_rate",
        "tw_commission_discount",
        "tw_commission_rebate_timing",
        "tw_day_trade_stock_sell_tax",
        "tw_day_trade_etf_sell_tax",
        "tw_minimum_commission",
        "tw_commission_rounding",
        "tw_tax_rounding",
    ):
        assert getattr(minute.trading, field) == getattr(daily.trading, field)
    assert (
        minute.training.financial_transformer.portfolio_output_mode
        == "l1"
    )
    assert (
        minute.training.loss_portfolio_activation
        == daily.training.loss_portfolio_activation
        == "pre_normalized"
    )


def test_model_mask_does_not_consume_next_bar_or_session_exit_availability() -> None:
    features = np.ones((270, 2, len(MINUTE_FEATURE_COLUMNS)), dtype=np.float32)
    feature_mask = np.ones((270, 2), dtype=bool)
    execution_mask = np.zeros((270, 2), dtype=bool)
    session_exit_mask = np.zeros(2, dtype=bool)
    day = MinuteDayPanel(
        trade_date=np.datetime64("2026-01-02"),
        features=features,
        feature_mask=feature_mask,
        execution_mask=execution_mask,
        execution_open=np.zeros((270, 2), dtype=np.float32),
        exit_close=np.zeros((270, 2), dtype=np.float32),
        future_volume_shares=np.zeros((270, 2), dtype=np.float32),
        session_close=np.zeros(2, dtype=np.float32),
        session_exit_mask=session_exit_mask,
    )

    _, model_mask = _build_windows(day, np.asarray([1]), lookback=2)

    assert model_mask.tolist() == [[True, True]]


def test_minute_partition_loads_sparse_full_market_rows_into_dense_day(
    tmp_path: Path,
) -> None:
    root = tmp_path / "minute"
    partition = root / "trade_date=2026-01-02"
    partition.mkdir(parents=True)
    output = partition / "data.parquet"
    feature_values = {
        name: [float(index + 1), float(index + 2)]
        for index, name in enumerate(MINUTE_FEATURE_COLUMNS)
    }
    frame = pl.DataFrame(
        {
            "ts": [
                datetime(2026, 1, 2, 9, 1),
                datetime(2026, 1, 2, 9, 2),
            ],
            "symbol": ["0050", "2330"],
            **feature_values,
            "feature_valid": [True, True],
            "label_valid_1m": [True, True],
            "execution_open_next_1m": [100.0, 200.0],
            "exit_close_next_1m": [101.0, 202.0],
            "future_volume_shares_next_1m": [1_000_000.0, 2_000_000.0],
            "session_close": [102.0, 203.0],
            "session_exit_valid": [True, True],
        }
    )
    frame.write_parquet(output)
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    counts = {name: 2 for name in MINUTE_FEATURE_COLUMNS}
    sums = {
        name: float(sum(feature_values[name])) for name in MINUTE_FEATURE_COLUMNS
    }
    squares = {
        name: float(sum(value * value for value in feature_values[name]))
        for name in MINUTE_FEATURE_COLUMNS
    }
    summary = {
        "trade_date": "2026-01-02",
        "output": str(output),
        "output_sha256": digest,
        "feature_counts": counts,
        "feature_sums": sums,
        "feature_sum_squares": squares,
    }
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 4,
                "feature_statistics_contract": "float64_sums_v1",
                "status": "research_ready",
                "research_ready": True,
                "source": "shioaji_kbars_1m",
                "decision_clock": "completed_right_labelled_1m_bar",
                "execution_clock": "next_1m_bar_open_proxy",
                "model_feature_columns": list(MINUTE_FEATURE_COLUMNS),
                "symbols": ["0050", "2330"],
                "dates": ["2026-01-02"],
                "partitions": [summary],
            }
        ),
        encoding="utf-8",
    )
    dataset = load_minute_dataset_index(root)
    normalizer = MinuteFeatureNormalizer(
        mean=np.zeros(len(MINUTE_FEATURE_COLUMNS), dtype=np.float32),
        scale=np.ones(len(MINUTE_FEATURE_COLUMNS), dtype=np.float32),
        counts=np.full(len(MINUTE_FEATURE_COLUMNS), 2, dtype=np.int64),
    )
    day = dataset.load_day(0, normalizer=normalizer)

    assert day.features.shape == (270, 2, len(MINUTE_FEATURE_COLUMNS))
    assert day.feature_mask[0, 0]
    assert day.feature_mask[1, 1]
    assert not day.feature_mask[0, 1]
    assert day.execution_open[0, 0] == pytest.approx(100.0)
    assert day.exit_close[1, 1] == pytest.approx(202.0)
    assert day.session_close.tolist() == pytest.approx([102.0, 203.0])


def test_minute_training_reuses_canonical_group_artifacts(
    tmp_path: Path,
) -> None:
    root = tmp_path / "minute"
    symbols = ["0050", "2330"]
    date_strings = [
        "2020-01-02",
        "2021-01-04",
        "2022-01-03",
        "2023-01-03",
    ]
    partitions: list[dict[str, object]] = []
    for date_offset, date_string in enumerate(date_strings):
        partition = root / f"trade_date={date_string}"
        partition.mkdir(parents=True)
        output = partition / "data.parquet"
        rows = len(symbols) * 2
        feature_values = {
            name: [
                float(date_offset + feature_index + row_index + 1)
                for row_index in range(rows)
            ]
            for feature_index, name in enumerate(MINUTE_FEATURE_COLUMNS)
        }
        frame = pl.DataFrame(
            {
                "ts": [
                    datetime.fromisoformat(f"{date_string} 09:01:00"),
                    datetime.fromisoformat(f"{date_string} 09:01:00"),
                    datetime.fromisoformat(f"{date_string} 09:02:00"),
                    datetime.fromisoformat(f"{date_string} 09:02:00"),
                ],
                "symbol": symbols * 2,
                **feature_values,
                "feature_valid": [True] * rows,
                "label_valid_1m": [True] * rows,
                "execution_open_next_1m": [100.0, 200.0] * 2,
                "exit_close_next_1m": [100.5, 201.0] * 2,
                "future_volume_shares_next_1m": [1_000_000.0] * rows,
                "session_close": [100.5, 201.0] * 2,
                "session_exit_valid": [True] * rows,
            }
        )
        frame.write_parquet(output)
        partitions.append(
            {
                "trade_date": date_string,
                "output": str(output),
                "output_sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
                "feature_counts": {
                    name: rows for name in MINUTE_FEATURE_COLUMNS
                },
                "feature_sums": {
                    name: float(sum(values))
                    for name, values in feature_values.items()
                },
                "feature_sum_squares": {
                    name: float(sum(value * value for value in values))
                    for name, values in feature_values.items()
                },
            }
        )
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 4,
                "feature_statistics_contract": "float64_sums_v1",
                "status": "research_ready",
                "research_ready": True,
                "source": "shioaji_kbars_1m",
                "decision_clock": "completed_right_labelled_1m_bar",
                "execution_clock": "next_1m_bar_open_proxy",
                "model_feature_columns": list(MINUTE_FEATURE_COLUMNS),
                "symbols": symbols,
                "dates": date_strings,
                "partitions": partitions,
            }
        ),
        encoding="utf-8",
    )

    config = load_config(Path(__file__).parents[1] / "configs/markets/tw_minute.yaml")
    config.data.minute_parquet_root = str(root)
    config.data.benchmark_name = symbols[0]
    config.data.minute_verify_partition_sha256 = True
    config.environment.device = "cpu"
    config.environment.amp_dtype = "tf32"
    config.training.enable_torch_compile = False
    config.training.enable_lr_scheduler = False
    config.training.epochs = 1
    config.training.lookback = 2
    config.training.batch_size_train = 2
    config.training.batch_size_eval = 2
    config.training.record_epoch_curve = True
    config.training.early_stopping_no_improve_ratio = 0.0
    config.training.transformer_base_portfolio.attention_mode = "temporal_only"
    config.training.transformer_base_portfolio.use_latent_factors = False
    config.training.transformer_base_portfolio.use_market_tokens = False
    config.training.transformer_base_portfolio.d_model = 8
    config.training.transformer_base_portfolio.temporal_layers = 1
    config.training.transformer_base_portfolio.temporal_heads = 1
    config.training.transformer_base_portfolio.temporal_ffn_mult = 1
    config.training.transformer_base_portfolio.joint_layers = 1
    config.training.transformer_base_portfolio.joint_heads = 1
    config.training.transformer_base_portfolio.joint_ffn_mult = 1
    config.training.transformer_base_portfolio.head_hidden_dim = 8
    config.trading.max_volume_participation = 0.5
    config.trading.tw_minute_initial_equity = 1_000_000.0
    config.trading.tw_minute_gross_exposure = 1.0
    config.trading.tw_minute_max_name_weight = None
    config.trading.tw_minute_outside_cash_logit = None
    config.trading.tw_minute_max_order_notional = None
    config.trading.tw_minute_slippage_bps_per_side = 0.0
    config.trading.tw_minute_first_decision_minute = 2
    config.trading.tw_minute_last_entry_minute = 2
    config.trading.tw_minute_last_decision_minute = 2
    output_dir = tmp_path / "artifacts"
    results = run_minute_training(
        config,
        output_dir=output_dir,
        mode="train",
        resume=True,
        start_fold=None,
        max_folds=1,
        active_strategy="none",
    )

    group_dir = output_dir / "train_2020-2021"
    assert (group_dir / "checkpoint_last.pt").is_file()
    assert (output_dir / "fold_01" / "checkpoint_best.pt").is_file()
    assert (output_dir / "fold_01" / "metrics.json").is_file()
    assert (output_dir / "fold_01" / "model.pt").is_file()
    assert (output_dir / "fold_01" / "test_backtest.npz").is_file()
    assert (output_dir / "fold_01" / "deployment_test_backtest.npz").is_file()
    assert (output_dir / "fold_01" / "equity_curve.png").is_file()
    assert (output_dir / "fold_01" / "equity_curve_log.png").is_file()
    assert (output_dir / "fold_01" / "annual_performance.png").is_file()
    assert (output_dir / "fold_01" / "annual_report.txt").is_file()
    assert (output_dir / "fold_01" / "plot_timing.json").is_file()
    assert (output_dir / "fold_01" / "mode_artifact_contract.json").is_file()
    assert (output_dir / "summary.json").is_file()
    run_manifest = json.loads(
        (output_dir / "minute_run_manifest.json").read_text(encoding="utf-8")
    )
    canonical_run_manifest = json.loads(
        (output_dir / "run_manifest.json").read_text(encoding="utf-8")
    )
    progress = json.loads(
        (output_dir / "progress.json").read_text(encoding="utf-8")
    )
    artifact_contract = json.loads(
        (output_dir / "fold_01" / "mode_artifact_contract.json").read_text(
            encoding="utf-8"
        )
    )
    assert run_manifest["sample_order_contract"] == "chronological_sessions"
    assert run_manifest == canonical_run_manifest
    assert progress["state"] == "complete"
    assert progress["phase"] == "complete"
    assert progress["group"]["name"] == "train_2020-2021"
    assert progress["fold"]["completed_ids"] == [1]
    assert artifact_contract["sample_order_contract"] == "chronological_sessions"
    assert artifact_contract["product_family"] == "taiwan_stock_etf"
    assert artifact_contract["frequency"] == "one_minute"
    curve_rows = [
        json.loads(line)
        for line in (group_dir / "epoch_curve.jsonl").read_text().splitlines()
    ]
    assert len(curve_rows) == 1
    assert curve_rows[0]["train_loss"] is not None
    assert curve_rows[0]["val_mean"] is not None
    assert curve_rows[0]["test_mean"] is not None
    assert curve_rows[0]["test_mean_scope"] == "first_calendar_year_of_fold_test"
    assert curve_rows[0]["test_sample_year"] == 2023
    assert curve_rows[0]["test_sample_rows"] == 1
    assert curve_rows[0]["epoch_total_s"] > 0.0
    assert curve_rows[0]["minute_days_per_train_batch"] == 2
    assert curve_rows[0]["minute_decision_chunk_rows"] == 10
    assert curve_rows[0]["minute_eval_decision_chunk_rows"] == 10
    assert curve_rows[0]["minute_optimizer_steps_per_epoch"] == 1
    assert curve_rows[0]["minute_train_cache_misses"] == 2
    assert curve_rows[0]["minute_val_cache_misses"] == 1
    assert curve_rows[0]["minute_test_cache_misses"] == 1
    assert (group_dir / "epoch_curve_every1.png").is_file()
    assert (group_dir / "epoch_timing_every1.png").is_file()
    conformance = validate_completed_training_artifacts(
        output_dir,
        fold_ids=[1],
        group_names=["train_2020-2021"],
    )
    conformance.require()
    assert results[0]["status"] == "complete"
