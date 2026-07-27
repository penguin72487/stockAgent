from __future__ import annotations

import copy

import numpy as np
import pytest
import torch

from stockagent.data.panel import (
    PANEL_CACHE_VERSION,
    PanelData,
    _ExternalFeatureArrays,
    _apply_external_rule_masks,
    _panel_from_cache_payload,
)
from stockagent.data.panel_cache import load_panel_cache_v2, save_panel_cache_v2
from stockagent.training.dataset import CrossSectionalDataset, collate_batch
from stockagent.training.trainer import _pad_windowed_training_split
from stockagent.training.windowed import dataset_to_windowed_tensors


def _carrying_panel(
    *,
    include_open_short_mask: bool = True,
) -> PanelData:
    rows = 5
    symbols = 2
    shape = (rows, symbols)
    dates = np.arange(
        np.datetime64("2024-01-02", "D"),
        np.datetime64("2024-01-02", "D") + rows,
    )
    closes = np.asarray(
        [
            [100.0, 200.0],
            [101.0, 201.0],
            [102.0, 202.0],
            [103.0, 203.0],
            [104.0, 204.0],
        ],
        dtype=np.float32,
    )
    can_sell = np.ones(shape, dtype=bool)
    can_sell[2, 0] = False
    can_short_close = np.ones(shape, dtype=bool)
    can_short_close[2] = [False, True]
    can_sell_open = np.ones(shape, dtype=bool)
    can_sell_open[2, 1] = False
    can_short_open = np.ones(shape, dtype=bool)
    can_short_open[2] = [True, False]
    capacity = np.full(shape, 100, dtype=np.int64)
    capacity[2] = [10, 20]
    return PanelData(
        dates=dates,
        symbols=["2330", "0050"],
        feature_names=["known_feature"],
        features=np.zeros((rows, symbols, 1), dtype=np.float32),
        returns_1d=np.zeros(shape, dtype=np.float32),
        tradable_mask=np.ones(shape, dtype=bool),
        alive_mask=np.ones(shape, dtype=bool),
        benchmark_returns=np.zeros((rows,), dtype=np.float32),
        close_prices=closes,
        daily_volumes=np.full(shape, 1_000.0, dtype=np.float32),
        can_buy_mask=np.ones(shape, dtype=bool),
        can_sell_mask=can_sell,
        can_short_open_mask=can_short_close,
        can_short_open_open_mask=(
            can_short_open if include_open_short_mask else None
        ),
        force_short_cover_mask=np.zeros(shape, dtype=bool),
        force_exit_mask=np.zeros(shape, dtype=bool),
        open_prices=closes - np.float32(0.5),
        intraday_returns=np.zeros(shape, dtype=np.float32),
        day_trade_can_buy_open_mask=np.ones(shape, dtype=bool),
        day_trade_can_sell_open_mask=can_sell_open,
        raw_close_returns_1d=np.zeros(shape, dtype=np.float32),
        corporate_action_avoidance_mask=np.zeros(shape, dtype=bool),
        unresolved_corporate_action_mask=np.zeros(shape, dtype=bool),
        short_capacity_shares=capacity,
    )


def _carrying_dataset(
    panel: PanelData,
    *,
    execution_mode: str = "tw_cash",
) -> CrossSectionalDataset:
    return CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates, dtype=np.int64),
        lookback=1,
        execution_mode=execution_mode,
    )


def test_panel_builds_distinct_open_and_close_short_masks() -> None:
    panel = _carrying_panel()
    panel = PanelData(
        dates=panel.dates[:3],
        symbols=["2330"],
        feature_names=panel.feature_names,
        features=panel.features[:3, :1],
        returns_1d=panel.returns_1d[:3, :1],
        tradable_mask=np.ones((3, 1), dtype=bool),
        alive_mask=np.ones((3, 1), dtype=bool),
        benchmark_returns=panel.benchmark_returns[:3],
        close_prices=np.asarray([[100.0], [90.0], [90.0]], dtype=np.float32),
        can_buy_mask=np.ones((3, 1), dtype=bool),
        can_sell_mask=np.ones((3, 1), dtype=bool),
        can_short_open_mask=np.ones((3, 1), dtype=bool),
        open_prices=np.asarray([[100.0], [95.0], [90.0]], dtype=np.float32),
        intraday_returns=np.zeros((3, 1), dtype=np.float32),
        day_trade_can_buy_open_mask=np.ones((3, 1), dtype=bool),
        day_trade_can_sell_open_mask=np.ones((3, 1), dtype=bool),
        raw_close_returns_1d=np.zeros((3, 1), dtype=np.float32),
        unresolved_corporate_action_mask=np.zeros((3, 1), dtype=bool),
    )
    rules = np.asarray(
        [
            [np.log(0.9), 1.0, 1_000.0],
            [np.log(0.9), 1.0, 2_000.0],
            [np.log(0.9), 1.0, 3_000.0],
        ],
        dtype=np.float64,
    )
    external = _ExternalFeatureArrays(
        feature_names=[],
        market_dates=np.empty((0,), dtype="datetime64[D]"),
        market_values=np.empty((0, 0), dtype=np.float32),
        by_symbol={},
        rule_names=[
            "_twpub_tpex_next_limit_down_ret",
            "_twpub_margin_short_evidence_next_session",
            "_twpub_short_capacity_shares_next_session",
        ],
        market_rule_values=np.empty((0, 3), dtype=np.float32),
        by_symbol_rules={"2330": (panel.dates, rules)},
        official_session_dates=panel.dates,
    )

    result = _apply_external_rule_masks(panel, external)

    # Session 1 opens above the 10% limit-down price but closes exactly at it.
    # The opening short is executable; the closing-auction short is not.
    assert not result.can_short_open_mask[1, 0]
    assert result.can_short_open_open_mask[1, 0]
    # Capacity is the prior official closing balance and is independent of the
    # later session-1 close-side execution outcome.
    assert result.short_capacity_shares[1, 0] == 1_000


@pytest.mark.parametrize("execution_mode", ["tw_cash", "tw_overnight"])
def test_carrying_dataset_keeps_open_and_close_short_decisions_independent(
    execution_mode: str,
) -> None:
    dataset = _carrying_dataset(
        _carrying_panel(),
        execution_mode=execution_mode,
    )
    row = int(np.flatnonzero(dataset.valid_indices == 2)[0])
    sample = dataset[row]

    assert sample["can_short_open_mask"].tolist() == [False, True]
    assert sample["can_short_open_open_mask"].tolist() == [True, False]
    assert sample["short_capacity_shares"].tolist() == [10, 20]
    # Session-2 capacity uses the completed session-1 close.
    assert sample["short_capacity_notional"].tolist() == [1_010.0, 4_020.0]


def test_old_cache_payload_cannot_infer_open_short_from_close_mask() -> None:
    panel = _carrying_panel(include_open_short_mask=False)
    payload = {
        "dates": panel.dates,
        "symbols": panel.symbols,
        "feature_names": panel.feature_names,
        "features": panel.features,
        "returns_1d": panel.returns_1d,
        "tradable_mask": panel.tradable_mask,
        "alive_mask": panel.alive_mask,
        "benchmark_returns": panel.benchmark_returns,
        "close_prices": panel.close_prices,
        "daily_volumes": panel.daily_volumes,
        "can_buy_mask": panel.can_buy_mask,
        "can_sell_mask": panel.can_sell_mask,
        "can_short_open_mask": panel.can_short_open_mask,
        "force_short_cover_mask": panel.force_short_cover_mask,
        "force_exit_mask": panel.force_exit_mask,
        "open_prices": panel.open_prices,
        "intraday_returns": panel.intraday_returns,
        "day_trade_can_buy_open_mask": panel.day_trade_can_buy_open_mask,
        "day_trade_can_sell_open_mask": panel.day_trade_can_sell_open_mask,
        "raw_close_returns_1d": panel.raw_close_returns_1d,
        "corporate_action_avoidance_mask": (
            panel.corporate_action_avoidance_mask
        ),
        "unresolved_corporate_action_mask": (
            panel.unresolved_corporate_action_mask
        ),
        "short_capacity_shares": panel.short_capacity_shares,
    }

    restored = _panel_from_cache_payload(payload)
    assert restored.can_short_open_open_mask is None
    dataset = _carrying_dataset(restored)
    assert dataset.can_short_open_mask_t.any()
    assert not dataset.can_short_open_open_mask_t.any()


def test_new_open_short_mask_round_trips_as_optional_cache_array(
    tmp_path,
) -> None:
    panel = _carrying_panel()
    save_panel_cache_v2(
        tmp_path,
        panel,
        source_hash="open-short-causality",
        backend_key="causal-open-short",
        version=PANEL_CACHE_VERSION,
    )

    restored = _panel_from_cache_payload(load_panel_cache_v2(tmp_path))
    np.testing.assert_array_equal(
        restored.can_short_open_open_mask,
        panel.can_short_open_open_mask,
    )


def test_carrying_capacity_is_invariant_to_current_close() -> None:
    baseline_panel = _carrying_panel()
    changed_panel = copy.deepcopy(baseline_panel)
    changed_panel.close_prices[2] = [777.0, 888.0]

    baseline = _carrying_dataset(baseline_panel)
    changed = _carrying_dataset(changed_panel)

    torch.testing.assert_close(
        changed.short_capacity_notional_t[2],
        baseline.short_capacity_notional_t[2],
    )
    torch.testing.assert_close(
        changed.can_short_open_mask_t[2],
        baseline.can_short_open_mask_t[2],
    )
    torch.testing.assert_close(
        changed.can_short_open_open_mask_t[2],
        baseline.can_short_open_open_mask_t[2],
    )
    assert baseline.short_capacity_notional_t[2].tolist() == [
        1_010.0,
        4_020.0,
    ]


def test_open_short_mask_reaches_collate_and_every_windowed_batch_path() -> None:
    dataset = _carrying_dataset(
        _carrying_panel(),
        execution_mode="tw_overnight",
    )
    split = dataset_to_windowed_tensors(dataset)
    expected_panel = dataset.can_short_open_open_mask_t

    assert torch.equal(split.can_short_open_open_mask, expected_panel)
    assert torch.equal(
        split.subset_symbols(torch.tensor([1])).can_short_open_open_mask,
        expected_panel[:, 1:2],
    )
    padded = split.pad_symbols(3)
    assert torch.equal(padded.can_short_open_open_mask[:, :2], expected_panel)
    assert not padded.can_short_open_open_mask[:, 2].any()

    rows = 2
    expected = expected_panel[split.valid_indices[:rows]]
    cpu = torch.device("cpu")
    batches = [
        split.batch_by_rows(0, rows, cpu, False),
        split.batch_metadata_by_rows(0, rows, cpu, False),
        split.batch_by_batch_indices(torch.arange(rows), cpu, False),
        split.batch_metadata_by_batch_indices(
            torch.arange(rows),
            cpu,
            False,
        ),
        split.panel_slab_batch_by_rows(0, rows, cpu, False),
    ]
    for batch in batches:
        assert batch is not None
        assert torch.equal(batch["can_short_open_open_mask"], expected)

    samples = [dataset[index] for index in range(rows)]
    dense = collate_batch(samples)
    padded_batch = collate_batch(samples[:1], batch_size=2)
    assert torch.equal(
        dense["can_short_open_open_mask"],
        torch.stack(
            [sample["can_short_open_open_mask"] for sample in samples]
        ),
    )
    assert not padded_batch["can_short_open_open_mask"][1].any()


def test_ragged_duplicate_padding_preserves_open_short_mask() -> None:
    split = dataset_to_windowed_tensors(
        _carrying_dataset(
            _carrying_panel(),
            execution_mode="tw_overnight",
        )
    )
    original_rows = len(split)
    padded = _pad_windowed_training_split(split, batch_size=3)

    assert len(padded) == 6
    assert original_rows == 4
    assert torch.equal(
        padded.can_short_open_open_mask,
        split.can_short_open_open_mask,
    )
    assert padded.valid_indices.tolist() == [
        *split.valid_indices.tolist(),
        int(split.valid_indices[-1]),
        int(split.valid_indices[-1]),
    ]
    assert padded.sample_mask.tolist() == [True, True, True, True, False, False]

    duplicate_batch = padded.batch_by_rows(
        original_rows,
        len(padded),
        torch.device("cpu"),
        non_blocking=False,
    )
    expected = split.can_short_open_open_mask[
        split.valid_indices[-1:]
    ].expand(2, -1)
    assert torch.equal(
        duplicate_batch["can_short_open_open_mask"],
        expected,
    )
