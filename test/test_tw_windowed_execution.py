from __future__ import annotations

import numpy as np
import pytest
import torch

from stockagent.data.panel import PanelData
from stockagent.data.walkforward import WalkForwardFold
from stockagent.explainability import (
    _estimated_first_test_year_explain_rows,
    _first_test_year_dataset,
)
from stockagent.training.dataset import CrossSectionalDataset
from stockagent.training.trainer import (
    _PanelSlabForwardWrapper,
    _call_panel_forward_for_batch,
    _combine_datasets_to_windowed,
    _densify_windowed_training_split_for_panel_slab,
    _maybe_cache_windowed_split_on_device,
    _pad_windowed_training_split,
    _prepare_windowed_split,
    _split_valid_indices,
)
from stockagent.training.windowed import WindowedSplitTensors, dataset_to_windowed_tensors


def _panel(rows: int = 7, symbols: int = 3) -> PanelData:
    features = np.broadcast_to(
        np.arange(rows, dtype=np.float32)[:, None, None],
        (rows, symbols, 1),
    ).copy()
    returns = np.broadcast_to(
        np.arange(rows, dtype=np.float32)[:, None] / 100.0,
        (rows, symbols),
    ).copy()
    tradable = np.ones((rows, symbols), dtype=bool)
    eligibility = np.asarray(
        [[(date + symbol) % 2 == 0 for symbol in range(symbols)] for date in range(rows)],
        dtype=bool,
    )
    short_capacity = (
        np.arange(rows * symbols, dtype=np.int64).reshape(rows, symbols) + 1
    ) * 1_000
    short_margin_rate = np.full((rows, symbols), 1.3, dtype=np.float32)
    return PanelData(
        dates=np.arange(rows).astype("datetime64[D]"),
        symbols=[f"S{symbol}" for symbol in range(symbols)],
        feature_names=["date_code"],
        features=features,
        returns_1d=returns,
        intraday_returns=returns + np.float32(0.25),
        tradable_mask=tradable,
        alive_mask=tradable.copy(),
        can_buy_mask=tradable.copy(),
        can_sell_mask=tradable.copy(),
        can_short_open_mask=tradable.copy(),
        can_short_open_open_mask=np.roll(eligibility, shift=1, axis=0),
        short_capacity_shares=short_capacity,
        short_margin_rate=short_margin_rate,
        benchmark_returns=np.arange(rows, dtype=np.float32),
        close_prices=np.full((rows, symbols), 100.0, dtype=np.float32),
        open_prices=np.full((rows, symbols), 90.0, dtype=np.float32),
        daily_volumes=np.full((rows, symbols), 1_000.0, dtype=np.float32),
        day_trade_eligible_mask=eligibility,
        day_trade_can_short_open_mask=eligibility.copy(),
        day_trade_can_buy_open_mask=eligibility.copy(),
        day_trade_can_sell_open_mask=np.flip(eligibility, axis=1).copy(),
        raw_close_returns_1d=returns.copy(),
        unresolved_corporate_action_mask=np.zeros_like(eligibility),
    )


def _direct_day_trade_split(
    *,
    padded: bool = False,
    execution_mode: str = "tw_day_trade",
) -> WindowedSplitTensors:
    rows = 7
    symbols = 3
    features = torch.arange(rows, dtype=torch.float32)[:, None, None].expand(rows, symbols, 1).clone()
    valid_indices = torch.tensor([3, 4, 4] if padded else [3, 4, 5], dtype=torch.long)
    sample_mask = torch.tensor([True, True, False] if padded else [True, True, True])
    eligibility = (
        torch.arange(rows)[:, None] + torch.arange(symbols)[None, :]
    ).remainder(2).eq(0)
    short_capacity = (
        torch.arange(rows * symbols, dtype=torch.int64).reshape(rows, symbols) + 1
    ) * 1_000
    short_margin_rate = torch.full((rows, symbols), 1.3, dtype=torch.float32)
    short_capacity_notional = short_capacity.to(dtype=torch.float32) * 11.0
    return WindowedSplitTensors(
        features=features,
        valid_indices=valid_indices,
        future_log_returns=torch.arange(rows * symbols, dtype=torch.float32).reshape(rows, symbols),
        tradable_mask=torch.ones((rows, symbols), dtype=torch.bool),
        can_buy_mask=torch.ones((rows, symbols), dtype=torch.bool),
        can_sell_mask=torch.ones((rows, symbols), dtype=torch.bool),
        can_short_open_mask=torch.ones((rows, symbols), dtype=torch.bool),
        can_short_open_open_mask=torch.roll(eligibility, shifts=1, dims=0),
        benchmark=torch.arange(rows, dtype=torch.float32),
        lookback=2,
        short_capacity_shares=short_capacity,
        short_margin_rate=short_margin_rate,
        short_capacity_notional=short_capacity_notional,
        sample_mask=sample_mask,
        execution_mode=execution_mode,
        session_advance_mask=torch.ones(rows, dtype=torch.bool),
        day_trade_eligible_mask=eligibility,
        day_trade_can_buy_open_mask=eligibility.clone(),
        day_trade_can_sell_open_mask=torch.flip(eligibility, dims=(1,)),
        unresolved_corporate_action_mask=torch.zeros_like(eligibility),
    )


def test_dataset_conversion_preserves_mode_and_uses_only_pre_session_features() -> None:
    panel = _panel()
    dataset = CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates),
        lookback=2,
        execution_mode="tw_day_trade",
    )
    split = dataset_to_windowed_tensors(dataset)

    assert split.execution_mode == "tw_day_trade"
    assert torch.equal(split.session_advance_mask, dataset.session_advance_mask_t)
    assert torch.equal(split.day_trade_eligible_mask, dataset.day_trade_eligible_mask_t)
    assert torch.equal(
        split.short_capacity_shares,
        dataset.short_capacity_shares_t,
    )
    assert torch.equal(split.short_margin_rate, dataset.short_margin_rate_t)
    assert torch.equal(
        split.short_capacity_notional,
        dataset.short_capacity_notional_t,
    )

    expected_x = torch.stack([dataset[row]["x"] for row in range(len(dataset))])
    materialized_x = split.materialize_windows()[0]
    torch.testing.assert_close(materialized_x, expected_x)

    # First target is t=2, so a lookback-2 day-trade decision may observe only
    # feature dates 0 and 1.  Feature date 2 would be same-session leakage.
    assert materialized_x[0, :, 0, 0].tolist() == [0.0, 1.0]
    assert split.future_log_returns[split.valid_indices[0], 0].item() == pytest.approx(0.27)


def test_daily_day_trade_full_minute_tape_stays_executor_only_through_windowing() -> None:
    panel = _panel(rows=5, symbols=2)
    tape = np.zeros((5, 2, 271, 2), dtype=np.float32)
    tape[:, :, 0, 0] = panel.open_prices
    tape[2, 1, 1, :] = np.asarray([101.0, 5_000.0], dtype=np.float32)
    panel.day_trade_minute_execution = tape
    dataset = CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates),
        lookback=2,
        execution_mode="tw_day_trade",
    )

    split = dataset_to_windowed_tensors(dataset)
    assert tuple(split.overnight_log_returns.shape) == (5, 2, 271, 2)
    assert split.features.shape[-1] == 1
    batch = split.batch_by_rows(
        0,
        1,
        torch.device("cpu"),
        non_blocking=False,
    )
    assert tuple(batch["overnight_log_returns"].shape) == (1, 2, 271, 2)
    assert batch["overnight_log_returns"][0, 1, 1].tolist() == [101.0, 5_000.0]


def test_panel_history_makes_split_row_zero_use_prior_causal_features() -> None:
    panel = _panel(rows=8, symbols=2)
    split_indices = np.arange(4, 8, dtype=np.int64)
    legacy = CrossSectionalDataset(
        panel,
        split_indices,
        lookback=2,
        execution_mode="tw_cash",
        lookback_context="split_only",
    )
    contextual = CrossSectionalDataset(
        panel,
        split_indices,
        lookback=2,
        execution_mode="tw_cash",
        lookback_context="panel_history",
    )

    # tw_cash target t sees only [t-2, t-1]. The target and every executor-side
    # tensor still come from t=4, the first row owned by this split.
    assert contextual.valid_indices.tolist() == [4, 5, 6, 7]
    assert legacy.valid_indices.tolist() == [6, 7]
    assert contextual[0]["x"][:, 0, 0].tolist() == [2.0, 3.0]
    first = contextual[0]
    expected_overnight = float(np.log(90.0 / 100.0))
    assert first["overnight_log_returns"][0].item() == pytest.approx(
        expected_overnight
    )
    assert first["future_log_returns"][0].item() == pytest.approx(
        0.03 - expected_overnight
    )
    assert (
        first["overnight_log_returns"][0] + first["future_log_returns"][0]
    ).item() == pytest.approx(0.03)
    np.testing.assert_array_equal(
        _split_valid_indices(
            panel,
            split_indices,
            2,
            "tw_cash",
            "panel_history",
        ),
        contextual.valid_indices,
    )

    windowed = dataset_to_windowed_tensors(contextual)
    materialized, *_ = windowed.materialize_windows()
    torch.testing.assert_close(materialized[0], contextual[0]["x"])
    slab = windowed.panel_slab_batch_by_rows(
        0,
        len(windowed),
        torch.device("cpu"),
        non_blocking=False,
    )
    assert slab is not None
    assert slab["feature_slab"][:, 0, 0].tolist() == [2.0, 3.0, 4.0, 5.0, 6.0]


def test_panel_slab_densify_preserves_panel_history_owned_start() -> None:
    panel = _panel(rows=8)
    date_indices = np.asarray([4, 5, 6, 7], dtype=np.int64)
    dataset = CrossSectionalDataset(
        panel,
        date_indices,
        lookback=2,
        execution_mode="tw_overnight",
        lookback_context="panel_history",
    )
    split = dataset_to_windowed_tensors(dataset)

    assert split.valid_indices.tolist() == [4, 5, 6, 7]
    dense = _densify_windowed_training_split_for_panel_slab(
        split,
        date_indices,
    )

    assert dense.valid_indices.tolist() == [4, 5, 6, 7]
    assert dense.sample_mask is None


def test_explainability_load_estimate_uses_panel_history_dataset_contract() -> None:
    panel = _panel(rows=8, symbols=2)
    split_indices = np.arange(4, 8, dtype=np.int64)
    fold = WalkForwardFold(
        fold_id=1,
        train_indices=np.arange(0, 2, dtype=np.int64),
        val_indices=np.arange(2, 4, dtype=np.int64),
        test_indices=split_indices,
        train_years=[1970],
        val_years=[1970],
        test_years=[1970],
    )

    common = {
        "fold": fold,
        "panel": panel,
        "lookback": 4,
        "max_rows": 0,
        "execution_mode": "tw_day_trade",
        "short_capacity_limit_enabled": True,
        "tw_corporate_action_mode": "avoid",
    }
    assert _estimated_first_test_year_explain_rows(
        **common,
        lookback_context="panel_history",
    ) == 4
    assert _estimated_first_test_year_explain_rows(
        **common,
        lookback_context="split_only",
    ) == 0
    assert _estimated_first_test_year_explain_rows(
        **(common | {"max_rows": 2}),
        lookback_context="panel_history",
    ) == 2


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_day_trade"])
def test_explainability_dataset_uses_real_execution_alignment(
    execution_mode: str,
) -> None:
    panel = _panel()
    all_rows = np.arange(panel.num_dates, dtype=np.int64)
    fold = WalkForwardFold(
        fold_id=1,
        train_indices=all_rows,
        val_indices=all_rows,
        test_indices=all_rows,
        train_years=[1970],
        val_years=[1970],
        test_years=[1970],
    )

    dataset = _first_test_year_dataset(
        panel,
        fold,
        lookback=2,
        execution_mode=execution_mode,
    )

    assert dataset.execution_mode == execution_mode
    assert dataset.valid_indices[0] == 2
    assert dataset[0]["x"][:, 0, 0].tolist() == [0.0, 1.0]
    if execution_mode == "tw_day_trade":
        assert dataset[0]["overnight_log_returns"][0].item() == 0.0
        assert dataset[0]["future_log_returns"][0].item() == pytest.approx(
            float(panel.intraday_returns[2, 0])
        )
    else:
        expected_total = float(panel.raw_close_returns_1d[1, 0])
        expected_overnight = float(np.log(90.0 / 100.0))
        assert dataset[0]["overnight_log_returns"][0].item() == pytest.approx(
            expected_overnight
        )
        assert dataset[0]["future_log_returns"][0].item() == pytest.approx(
            expected_total - expected_overnight
        )
        assert (
            dataset[0]["overnight_log_returns"][0]
            + dataset[0]["future_log_returns"][0]
        ).item() == pytest.approx(expected_total)


def test_tw_cash_windowed_batches_preserve_shifted_dual_phase_targets() -> None:
    panel = _panel()
    panel.raw_close_returns_1d[:] = np.broadcast_to(
        np.arange(panel.num_dates, dtype=np.float32)[:, None] / 10.0,
        panel.raw_close_returns_1d.shape,
    )
    dataset = CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates),
        lookback=2,
        execution_mode="tw_cash",
    )
    split = dataset_to_windowed_tensors(dataset)
    cpu = torch.device("cpu")
    batches = [
        split.batch_by_rows(0, len(split), cpu, non_blocking=False),
        split.batch_metadata_by_rows(0, len(split), cpu, non_blocking=False),
        split.batch_by_batch_indices(
            torch.arange(len(split)),
            cpu,
            non_blocking=False,
        ),
        split.batch_metadata_by_batch_indices(
            torch.arange(len(split)),
            cpu,
            non_blocking=False,
        ),
    ]
    slab = split.panel_slab_batch_by_rows(
        0,
        len(split),
        cpu,
        non_blocking=False,
    )
    assert slab is not None
    batches.append(slab)

    target_rows = split.valid_indices
    expected_total = torch.from_numpy(
        panel.raw_close_returns_1d[target_rows.numpy() - 1]
    )
    for batch in batches:
        torch.testing.assert_close(
            batch["overnight_log_returns"] + batch["future_log_returns"],
            expected_total,
        )
        # Targets begin at t=2. The common phase-decision window ends at t-1.
        if "x" in batch:
            assert batch["x"][0, :, 0, 0].tolist() == [0.0, 1.0]
        elif "feature_slab" in batch:
            assert batch["feature_slab"][:2, 0, 0].tolist() == [0.0, 1.0]
        else:
            assert batch["date_indices"][0].item() == 2


def test_day_trade_alignment_matches_indexed_contiguous_and_panel_slab_paths() -> None:
    split = _direct_day_trade_split()
    expected = torch.tensor(
        [
            [[1.0], [2.0]],
            [[2.0], [3.0]],
            [[3.0], [4.0]],
        ]
    )

    contiguous = split.batch_by_rows(0, 3, torch.device("cpu"), non_blocking=False)
    indexed = split._batch_from_row_indices(
        torch.arange(3),
        torch.device("cpu"),
        non_blocking=False,
    )
    for symbol in range(split.num_symbols):
        torch.testing.assert_close(contiguous["x"][:, :, symbol], expected)
        torch.testing.assert_close(indexed["x"][:, :, symbol], expected)

    slab = split.panel_slab_batch_by_rows(0, 3, torch.device("cpu"), non_blocking=False)
    assert slab is not None
    assert slab["feature_slab"][:, 0, 0].tolist() == [1.0, 2.0, 3.0, 4.0]
    assert slab["future_log_returns"][:, 0].tolist() == [9.0, 12.0, 15.0]


class _WindowSumPanelModel(torch.nn.Module):
    def __init__(self, lookback: int) -> None:
        super().__init__()
        self.lookback = int(lookback)
        self.generic_panel_calls = 0
        self.slab_calls = 0

    def forward(
        self,
        x: torch.Tensor,
        mask: torch.Tensor,
        return_aux: bool | None = None,
    ) -> torch.Tensor:
        del return_aux
        return x[..., 0].sum(dim=1).masked_fill(~mask, 0.0)

    def forward_from_panel(
        self,
        features: torch.Tensor,
        date_indices: torch.Tensor,
        mask: torch.Tensor,
        return_aux: bool | None = None,
    ) -> torch.Tensor:
        self.generic_panel_calls += 1
        date_indices = date_indices.to(device=features.device, dtype=torch.long)
        offsets = torch.arange(
            self.lookback - 1,
            -1,
            -1,
            device=features.device,
            dtype=torch.long,
        )
        x = features[date_indices[:, None] - offsets[None, :]]
        return self.forward(x.to(device=mask.device), mask, return_aux=return_aux)

    def forward_from_panel_slab(
        self,
        feature_slab: torch.Tensor,
        mask: torch.Tensor,
        return_aux: bool | None = None,
    ) -> torch.Tensor:
        self.slab_calls += 1
        x = (
            feature_slab.unfold(0, self.lookback, 1)
            .permute(0, 3, 1, 2)
            .contiguous()
        )
        return self.forward(x.to(device=mask.device), mask, return_aux=return_aux)


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_day_trade"])
def test_real_execution_feature_lag_matches_all_panel_forward_paths(
    execution_mode: str,
) -> None:
    split = _direct_day_trade_split(execution_mode=execution_mode)
    split.features[:, :, 0] = torch.tensor(
        [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0]
    )[:, None]
    cpu = torch.device("cpu")
    materialized = split.batch_by_rows(0, 3, cpu, non_blocking=False)
    metadata = split.batch_metadata_by_rows(0, 3, cpu, non_blocking=False)
    model = _WindowSumPanelModel(split.lookback)
    slab_model = _PanelSlabForwardWrapper(model)

    expected = model(
        materialized["x"],
        materialized["tradable_mask"],
        return_aux=False,
    )
    generic = _call_panel_forward_for_batch(
        panel_forward_model=model,
        panel_slab_model=None,
        split=split,
        batch=metadata,
        mask=metadata["tradable_mask"],
        device=cpu,
        non_blocking=False,
        return_aux=False,
        allow_slab=False,
    )
    metadata_slab = _call_panel_forward_for_batch(
        panel_forward_model=model,
        panel_slab_model=slab_model,
        split=split,
        batch=metadata,
        mask=metadata["tradable_mask"],
        device=cpu,
        non_blocking=False,
        return_aux=False,
    )
    direct_slab_batch = split.panel_slab_batch_by_rows(
        0,
        3,
        cpu,
        non_blocking=False,
    )
    assert direct_slab_batch is not None
    direct_slab = slab_model(
        direct_slab_batch["feature_slab"],
        direct_slab_batch["tradable_mask"],
    )

    # Targets are t=3,4,5. Every real mode must stop at t-1, yielding
    # [2,4], [4,8], [8,16]. A same-session leak would double these values.
    assert expected[:, 0].tolist() == [6.0, 12.0, 24.0]
    torch.testing.assert_close(generic, expected)
    torch.testing.assert_close(metadata_slab, expected)
    torch.testing.assert_close(direct_slab, expected)
    assert model.generic_panel_calls == 1
    assert model.slab_calls == 2


def test_naive_alignment_still_includes_target_session_feature() -> None:
    day = _direct_day_trade_split()
    naive = WindowedSplitTensors(
        features=day.features,
        valid_indices=day.valid_indices,
        future_log_returns=day.future_log_returns,
        tradable_mask=day.tradable_mask,
        can_buy_mask=day.can_buy_mask,
        can_sell_mask=day.can_sell_mask,
        benchmark=day.benchmark,
        lookback=day.lookback,
    )

    assert naive.execution_mode == "naive"
    assert naive.materialize_windows()[0][0, :, 0, 0].tolist() == [2.0, 3.0]
    assert len(naive.materialize_windows()) == 6


def test_all_batch_apis_gate_session_and_eligibility_on_sample_padding() -> None:
    split = _direct_day_trade_split(padded=True)
    cpu = torch.device("cpu")
    batches = [
        split.batch_by_rows(0, 3, cpu, non_blocking=False),
        split.batch_metadata_by_rows(0, 3, cpu, non_blocking=False),
        split.batch_by_batch_indices(torch.tensor([0, 1, 2]), cpu, non_blocking=False),
        split.batch_metadata_by_batch_indices(torch.tensor([0, 1, 2]), cpu, non_blocking=False),
    ]
    slab = split.panel_slab_batch_by_rows(0, 3, cpu, non_blocking=False)
    assert slab is not None
    batches.append(slab)

    for batch in batches:
        assert batch["sample_mask"].tolist() == [True, True, False]
        assert batch["session_advance_mask"].tolist() == [True, True, False]
        assert "day_trade_eligible_mask" in batch
        assert not batch["day_trade_eligible_mask"][-1].any()
        assert "day_trade_can_buy_open_mask" in batch
        assert "day_trade_can_sell_open_mask" in batch
        assert not batch["day_trade_can_buy_open_mask"][-1].any()
        assert not batch["day_trade_can_sell_open_mask"][-1].any()
        assert "unresolved_corporate_action_mask" in batch
        assert not batch["unresolved_corporate_action_mask"][-1].any()
        assert not batch["short_capacity_shares"][-1].any()
        assert not batch["short_capacity_notional"][-1].any()
        assert torch.isnan(batch["short_margin_rate"][-1]).all()
        torch.testing.assert_close(
            batch["short_capacity_notional"][:2],
            split.short_capacity_notional[torch.tensor([3, 4])],
        )


def test_execution_fields_survive_device_pin_symbol_subset_pad_and_clamp() -> None:
    split = _direct_day_trade_split()
    subset = split.subset_symbols(torch.tensor([2, 0]))
    padded = subset.pad_symbols(4, pad_symbol_index=1)
    clamped = padded.clamp_symbol_indices(2)
    cached = clamped.to_device_cache(torch.device("cpu"), non_blocking=False)
    pinned = cached.pin_memory()

    expected_subset = split.day_trade_eligible_mask[:, [2, 0]]
    expected_buy_open = split.day_trade_can_buy_open_mask[:, [2, 0]]
    expected_sell_open = split.day_trade_can_sell_open_mask[:, [2, 0]]
    expected_actions = split.unresolved_corporate_action_mask[:, [2, 0]]
    expected_capacity = split.short_capacity_shares[:, [2, 0]]
    expected_margin_rate = split.short_margin_rate[:, [2, 0]]
    expected_capacity_notional = split.short_capacity_notional[:, [2, 0]]
    assert subset.execution_mode == "tw_day_trade"
    assert torch.equal(subset.day_trade_eligible_mask, expected_subset)
    assert torch.equal(padded.day_trade_eligible_mask[:, :2], expected_subset)
    assert not padded.day_trade_eligible_mask[:, 2:].any()
    assert torch.equal(subset.day_trade_can_buy_open_mask, expected_buy_open)
    assert torch.equal(subset.day_trade_can_sell_open_mask, expected_sell_open)
    assert not padded.day_trade_can_buy_open_mask[:, 2:].any()
    assert not padded.day_trade_can_sell_open_mask[:, 2:].any()
    assert torch.equal(subset.unresolved_corporate_action_mask, expected_actions)
    assert not padded.unresolved_corporate_action_mask[:, 2:].any()
    assert torch.equal(subset.short_capacity_shares, expected_capacity)
    assert torch.equal(padded.short_capacity_shares[:, :2], expected_capacity)
    assert not padded.short_capacity_shares[:, 2:].any()
    assert torch.equal(subset.short_margin_rate, expected_margin_rate)
    assert torch.equal(padded.short_margin_rate[:, :2], expected_margin_rate)
    assert torch.isnan(padded.short_margin_rate[:, 2:]).all()
    assert torch.equal(
        subset.short_capacity_notional,
        expected_capacity_notional,
    )
    assert torch.equal(
        padded.short_capacity_notional[:, :2],
        expected_capacity_notional,
    )
    assert not padded.short_capacity_notional[:, 2:].any()
    assert torch.equal(
        cached.short_capacity_notional,
        padded.short_capacity_notional,
    )
    assert torch.equal(
        pinned.short_capacity_notional,
        padded.short_capacity_notional,
    )
    assert torch.equal(pinned.session_advance_mask, split.session_advance_mask)
    assert torch.equal(pinned.day_trade_eligible_mask, padded.day_trade_eligible_mask)
    assert pinned.symbol_indices is not None
    assert pinned.symbol_indices.tolist() == [1, 0, 1, 1]


def test_trainer_windowed_rebuilders_preserve_execution_contract() -> None:
    source = _direct_day_trade_split()
    source.session_advance_mask.copy_(
        torch.tensor([True, False, True, True, False, True, True])
    )

    def _assert_contract(
        candidate: WindowedSplitTensors,
        *,
        execution_mode: str = "tw_day_trade",
    ) -> None:
        assert candidate.execution_mode == execution_mode
        assert torch.equal(candidate.session_advance_mask, source.session_advance_mask)
        assert torch.equal(candidate.day_trade_eligible_mask, source.day_trade_eligible_mask)
        assert torch.equal(
            candidate.day_trade_can_buy_open_mask,
            source.day_trade_can_buy_open_mask,
        )
        assert torch.equal(
            candidate.day_trade_can_sell_open_mask,
            source.day_trade_can_sell_open_mask,
        )
        assert torch.equal(
            candidate.can_short_open_open_mask,
            source.can_short_open_open_mask,
        )
        assert torch.equal(
            candidate.unresolved_corporate_action_mask,
            source.unresolved_corporate_action_mask,
        )

    padded = _pad_windowed_training_split(source, batch_size=4)
    _assert_contract(padded)
    assert padded.sample_mask.tolist() == [True, True, True, False]

    # Densification is exercised with naive alignment. Day-trade targets need
    # one extra pre-session feature row and therefore use a different minimum
    # valid date than the historical naive helper contract.
    sparse = WindowedSplitTensors(
        features=source.features,
        valid_indices=torch.tensor([3, 5]),
        future_log_returns=source.future_log_returns,
        tradable_mask=source.tradable_mask,
        can_buy_mask=source.can_buy_mask,
        can_sell_mask=source.can_sell_mask,
        can_short_open_open_mask=source.can_short_open_open_mask,
        benchmark=source.benchmark,
        lookback=source.lookback,
        execution_mode="naive",
        session_advance_mask=source.session_advance_mask,
        day_trade_eligible_mask=source.day_trade_eligible_mask,
        day_trade_can_buy_open_mask=source.day_trade_can_buy_open_mask,
        day_trade_can_sell_open_mask=source.day_trade_can_sell_open_mask,
        unresolved_corporate_action_mask=source.unresolved_corporate_action_mask,
    )
    dense = _densify_windowed_training_split_for_panel_slab(
        sparse,
        np.asarray([2, 3, 4, 5], dtype=np.int64),
    )
    _assert_contract(dense, execution_mode="naive")
    assert dense.valid_indices.tolist() == [3, 4, 5]
    assert dense.sample_mask.tolist() == [True, False, True]

    prepared_dense = _prepare_windowed_split(
        dense,
        torch.device("cpu"),
        non_blocking=False,
    )
    _assert_contract(prepared_dense, execution_mode="naive")

    prepared = _prepare_windowed_split(
        source,
        torch.device("cpu"),
        non_blocking=False,
    )
    _assert_contract(prepared)
    cached = _maybe_cache_windowed_split_on_device(
        name="tw execution contract",
        split=prepared,
        device=torch.device("cpu"),
        enabled=True,
        target_fraction=1.0,
        safety_margin_gb=0.0,
    )
    _assert_contract(cached)

    reused = _prepare_windowed_split(
        source,
        torch.device("cpu"),
        non_blocking=False,
        shared_base=prepared,
    )
    _assert_contract(reused)
    assert reused.features.data_ptr() == prepared.features.data_ptr()
    assert reused.day_trade_eligible_mask.data_ptr() == prepared.day_trade_eligible_mask.data_ptr()

    panel = _panel()
    combined, lengths = _combine_datasets_to_windowed(
        [
            CrossSectionalDataset(
                panel,
                np.arange(0, 5),
                lookback=2,
                execution_mode="tw_day_trade",
            ),
            CrossSectionalDataset(
                panel,
                np.arange(2, 7),
                lookback=2,
                execution_mode="tw_day_trade",
            ),
        ]
    )
    assert combined.execution_mode == "tw_day_trade"
    assert combined.day_trade_eligible_mask is not None
    assert torch.equal(
        combined.can_short_open_open_mask,
        torch.from_numpy(
            panel.day_trade_can_short_open_mask
            & panel.day_trade_eligible_mask
            & panel.day_trade_can_sell_open_mask
        ),
    )
    # Canonical panel-history context keeps all five targets owned by the
    # second split; its first two windows read only already-observed rows.
    assert lengths == [3, 5]


def test_day_trade_windowed_split_fails_closed_without_historical_eligibility() -> None:
    panel = _panel()
    with pytest.raises(ValueError, match="point-in-time day_trade_eligible_mask"):
        WindowedSplitTensors(
            features=torch.from_numpy(panel.features),
            valid_indices=torch.tensor([2, 3]),
            future_log_returns=torch.from_numpy(panel.intraday_returns),
            tradable_mask=torch.from_numpy(panel.tradable_mask),
            can_buy_mask=torch.from_numpy(panel.can_buy_mask),
            can_sell_mask=torch.from_numpy(panel.can_sell_mask),
            benchmark=torch.from_numpy(panel.benchmark_returns),
            lookback=2,
            execution_mode="TW-DAY-TRADE",
        )


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_overnight"])
def test_carrying_dataset_fails_closed_without_corporate_action_mask(
    execution_mode: str,
) -> None:
    panel = _panel()
    panel.unresolved_corporate_action_mask = None

    with pytest.raises(ValueError, match="unresolved_corporate_action_mask"):
        CrossSectionalDataset(
            panel,
            np.arange(panel.num_dates),
            lookback=2,
            execution_mode=execution_mode,
        )


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_overnight"])
def test_carrying_dataset_fails_closed_without_raw_close_returns(
    execution_mode: str,
) -> None:
    panel = _panel()
    panel.raw_close_returns_1d = None

    with pytest.raises(ValueError, match="raw_close_returns_1d"):
        CrossSectionalDataset(
            panel,
            np.arange(panel.num_dates),
            lookback=2,
            execution_mode=execution_mode,
        )


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_day_trade"])
def test_real_mode_split_helper_preserves_every_settlement_session(
    execution_mode: str,
) -> None:
    panel = _panel()
    panel.tradable_mask[3] = False
    panel.returns_1d[3] = np.nan
    indices = np.arange(panel.num_dates, dtype=np.int64)
    dataset = CrossSectionalDataset(
        panel,
        indices,
        lookback=2,
        execution_mode=execution_mode,
    )

    np.testing.assert_array_equal(
        _split_valid_indices(panel, indices, 2, execution_mode),
        dataset.valid_indices,
    )
    assert 3 in dataset.valid_indices
