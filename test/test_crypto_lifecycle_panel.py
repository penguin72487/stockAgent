"""Dated announcement evidence survives native readers, cache and execution."""

from dataclasses import fields

import numpy as np
import polars as pl
import pytest
import torch

from stockagent.backtest.simulator import run_backtest_torch
from stockagent.data.crypto_lifecycle import ANNOUNCEMENT_COLUMNS, announced_exit_mask
from stockagent.data.panel import (
    _build_panel_from_symbol_arrays,
    _load_symbol_arrays_polars_lazy,
    _load_symbol_arrays_pyarrow,
    _save_panel_cache,
    _slice_symbol_arrays_start,
    load_panel_cache_v2_exact,
    slice_panel_start,
)
from stockagent.training.dataset import CrossSectionalDataset
from stockagent.training.windowed import dataset_to_windowed_tensors


LOADERS = [_load_symbol_arrays_pyarrow, _load_symbol_arrays_polars_lazy]


def _frame():
    dates = np.arange("2026-08-17", "2026-08-22", dtype="datetime64[D]")
    count = len(dates)
    return pl.DataFrame({
        "date": dates.astype(str), "open": [100.] * count,
        "max": [101.] * count, "min": [99.] * count, "close": [100.] * count,
        "execution_price": [100.] * count, "adjclose": [100.] * count,
        "Trading_Volume": [100.] * count, "execution_volume_equivalent": [100.] * count,
        "policy_tradable": [True] * count, "execution_available": [True] * count,
        # An unrelated pre-announcement missing label must not request an exit.
        "return_quarantined": [False, True, False, False, True],
        "bybit_perpetual_contract_version": [6] * count,
        "crypto_delisting_announced_date": ["2026-08-19"] * count,
        "crypto_delisting_time_utc": ["2026-08-21T09:00:00Z"] * count,
        "crypto_delisting_source_url": [
            "https://announcements.bybit.com/en/article/synthetic-unit-test/"
        ] * count,
    })


def _load(tmp_path, loader, frame=None):
    path = tmp_path / "VINEUSDT_features.parquet"
    (_frame() if frame is None else frame).write_parquet(path)
    return loader(path, tradable_mode="tradable", trading_volume_policy="required")


def test_native_readers_have_identical_announcement_masks_and_financial_arrays(tmp_path):
    # Metadata is constant per symbol, so native date sorting must not move its event.
    frame = _frame().reverse()
    left, right = [_load(tmp_path, loader, frame) for loader in LOADERS]
    for field in fields(left):
        lhs, rhs = getattr(left, field.name), getattr(right, field.name)
        if isinstance(lhs, np.ndarray):
            np.testing.assert_allclose(lhs, rhs, equal_nan=True) if lhs.dtype.kind == "f" else np.testing.assert_array_equal(lhs, rhs)
        else:
            assert lhs == rhs
    assert left.crypto_announced_exit_mask.tolist() == [False, False, False, True, True]
    assert left.tradable_mask.tolist() == [True, True, True, False, False]
    assert left.can_buy_mask.all() and left.can_sell_mask.all()
    assert left.alive_mask.all() and left.return_valuation_mask.all()
    assert np.isnan(left.returns_1d[1])
    assert not left.crypto_announced_exit_mask[1]
    assert np.isfinite(left.returns_1d[3])
    assert left.crypto_announced_exit_mask[3]


@pytest.mark.parametrize("loader", LOADERS)
def test_slicing_and_exact_cache_preserve_exit_flags_and_original_side_masks(tmp_path, loader):
    arrays = _load(tmp_path, loader)
    clipped = _slice_symbol_arrays_start(arrays, np.datetime64("2026-08-19", "ns"))
    assert clipped.crypto_announced_exit_mask.tolist() == [False, True, True]
    panel = _build_panel_from_symbol_arrays([arrays])
    sliced = slice_panel_start(panel, "2026-08-19")
    np.testing.assert_array_equal(sliced.force_exit_mask[:, 0], clipped.crypto_announced_exit_mask)
    cache = tmp_path / "derived_cache"
    _save_panel_cache(cache, sliced, "synthetic-announcement-source", "synthetic-reader")
    restored = load_panel_cache_v2_exact(cache, source_hash="synthetic-announcement-source",
                                          backend_key="synthetic-reader")
    assert restored is not None
    for name in ("force_exit_mask", "tradable_mask", "can_buy_mask", "can_sell_mask", "alive_mask"):
        np.testing.assert_array_equal(getattr(restored, name), getattr(sliced, name))
    assert restored.can_buy_mask.all() and restored.can_sell_mask.all()


@pytest.mark.parametrize("loader", LOADERS)
@pytest.mark.parametrize("missing", ANNOUNCEMENT_COLUMNS)
def test_partial_announcement_provenance_is_rejected_by_both_native_readers(tmp_path, loader, missing):
    with pytest.raises(ValueError, match="incomplete crypto announcement provenance"):
        _load(tmp_path, loader, _frame().drop(missing))


@pytest.mark.parametrize("field,value,match", [
    ("crypto_delisting_source_url", "https://example.com/en/article/test/", "official Bybit"),
    ("crypto_delisting_time_utc", "2026-08-21T09:00:00", "explicit UTC"),
    ("crypto_delisting_time_utc", "2026-08-19T09:00:00Z", "no daily decision"),
    ("bybit_perpetual_contract_version", 5, "Bybit perpetual daily source"),
])
def test_invalid_announcement_provenance_fails_closed(field, value, match):
    frame = _frame().with_columns(pl.lit(value).alias(field))
    with pytest.raises(ValueError, match=match):
        announced_exit_mask(frame["date"].to_numpy(), frame.to_dict(as_series=False))


@pytest.mark.parametrize("loader", LOADERS)
def test_absent_announcement_does_not_infer_exit_from_missing_label(tmp_path, loader):
    arrays = _load(tmp_path, loader, _frame().drop(*ANNOUNCEMENT_COLUMNS))
    assert arrays.crypto_announced_exit_mask is None
    assert np.isnan(arrays.returns_1d[1])
    assert arrays.tradable_mask.all()
    panel = _build_panel_from_symbol_arrays([arrays])
    assert not panel.force_exit_mask.any()


@pytest.mark.parametrize("loader", LOADERS)
@pytest.mark.parametrize("sign", [-1, 1])
def test_dataset_windowed_and_ledger_keep_announcement_exit_capacity_limited(tmp_path, loader, sign):
    panel = _build_panel_from_symbol_arrays([_load(tmp_path, loader)])
    # Publication day and first later midnight, both with independently finite labels.
    dataset = CrossSectionalDataset(panel, np.array([2, 3]), lookback=1,
                                   execution_mode="crypto_perpetual")
    split = dataset_to_windowed_tensors(dataset)
    batch = split.batch_by_rows(0, 2, device=torch.device("cpu"), non_blocking=False)
    assert batch["tradable_mask"][:, 0].tolist() == [True, False]
    assert batch["force_exit_mask"][:, 0].tolist() == [False, True]
    assert batch["can_buy_mask"].all() and batch["can_sell_mask"].all()
    result = run_backtest_torch(
        torch.tensor([[sign * .5], [-sign * .8]]), batch["future_log_returns"],
        batch["tradable_mask"], batch["benchmark"], buy_fee_rate=0., sell_fee_rate=0.,
        long_only=False, portfolio_activation="pre_normalized", execution_mode="crypto_perpetual",
        can_buy_mask=batch["can_buy_mask"], can_sell_mask=batch["can_sell_mask"],
        can_short_open_mask=batch["can_short_open_mask"], force_exit_mask=batch["force_exit_mask"],
        overnight_returns=batch["overnight_log_returns"], initial_weights=torch.tensor([sign * .5]),
        volume_limit_weights=torch.full((2, 1), .1),
    )
    torch.testing.assert_close(result.turnovers, torch.tensor([0., .1]))
    assert result.final_weights.item() == pytest.approx(sign * .4)
