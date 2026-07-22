from __future__ import annotations

from datetime import date, datetime, timedelta
import hashlib
import json

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import torch

from stockagent.data.panel import (
    DAY_TRADE_OPEN_GAP_FEATURE,
    PANEL_CACHE_VERSION,
    PanelData,
    _CorporateActionReference,
    _CorporateActionReferencePaths,
    _apply_corporate_action_avoidance_transitions,
    _article76_lunar_new_year_extra_business_days,
    _attach_raw_close_forward_returns,
    build_panel,
    load_cached_panel,
    _build_panel_from_symbol_arrays,
    _load_symbol_arrays_polars_lazy,
    _load_symbol_arrays_pyarrow,
    _load_exact_cash_entitlements,
    _load_corporate_action_reference,
    _panel_from_cache_payload,
)
from stockagent.data.panel_cache import load_panel_cache_v2, save_panel_cache_v2
from stockagent.training.dataset import CrossSectionalDataset, collate_batch


def _write_corporate_action_reference(
    public_dir,
    *,
    rows: list[dict[str, object]],
    receipt_sha256: str | None = None,
    requested_start_year: int = 2024,
    coverage_start_year: int | None = None,
) -> None:
    path = public_dir / "tw_corporate_action_reference.parquet"
    table = pa.Table.from_pylist(
        rows,
        schema=pa.schema(
            [
                ("date", pa.date32()),
                ("symbol", pa.large_string()),
                ("market", pa.large_string()),
                ("reference_price", pa.float64()),
                ("opening_reference_price", pa.float64()),
                ("previous_close", pa.float64()),
                ("event_type", pa.large_string()),
                ("source_url", pa.large_string()),
            ]
        ),
    )
    pq.write_table(table, path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    summary = {
        "baseline_established": True,
        "coverage_complete": True,
        "failure_count": 0,
        "schema_version": 3,
        "requested_start_year": requested_start_year,
        "coverage_start_year": (
            requested_start_year
            if coverage_start_year is None
            else coverage_start_year
        ),
        "end_date": f"{requested_start_year:04d}-12-31",
        "rows": len(rows),
        "output_receipt": {
            "path": str(path),
            "size": path.stat().st_size,
            "sha256": receipt_sha256 or digest,
        },
    }
    path.with_suffix(".summary.json").write_text(
        json.dumps(summary), encoding="utf-8"
    )


def test_corporate_action_reference_uses_cumulative_coverage_start(tmp_path) -> None:
    public_dir = tmp_path / "data_tw_public"
    public_dir.mkdir()
    _write_corporate_action_reference(
        public_dir,
        rows=[
            {
                "date": datetime(2025, 1, 2).date(),
                "symbol": "2330",
                "market": "twse",
                "reference_price": 100.0,
                "opening_reference_price": 100.0,
                "previous_close": 100.0,
                "event_type": "除息",
                "source_url": "https://example.invalid/official",
            }
        ],
        requested_start_year=2025,
        coverage_start_year=2000,
    )
    parquet = public_dir / "tw_corporate_action_reference.parquet"
    reference = _load_corporate_action_reference(
        _CorporateActionReferencePaths(
            parquet=parquet,
            summary=parquet.with_suffix(".summary.json"),
        )
    )

    assert reference is not None
    assert reference.coverage_start == np.datetime64("2000-01-01")


def _make_panel(*, include_day_trade_inputs: bool = True) -> PanelData:
    rows = 6
    symbols = 2
    dates = np.arange(rows).astype("datetime64[D]")
    features = np.zeros((rows, symbols, 1), dtype=np.float32)
    features[:, :, 0] = np.arange(rows, dtype=np.float32)[:, None]
    returns = np.full((rows, symbols), 0.01, dtype=np.float32)
    intraday = np.asarray(
        [[0.10 + row, -(0.10 + row)] for row in range(rows)],
        dtype=np.float32,
    )
    tradable = np.ones((rows, symbols), dtype=bool)
    tradable[2] = False
    eligible = np.ones((rows, symbols), dtype=bool)
    eligible[3] = False
    close = np.full((rows, symbols), 100.0, dtype=np.float32)
    open_px = np.full((rows, symbols), 90.0, dtype=np.float32)
    return PanelData(
        dates=dates,
        symbols=["2330", "0050"],
        feature_names=["date_code"],
        features=features,
        returns_1d=returns,
        tradable_mask=tradable,
        can_buy_mask=tradable.copy(),
        can_sell_mask=tradable.copy(),
        can_short_open_mask=tradable.copy(),
        alive_mask=np.ones_like(tradable),
        benchmark_returns=np.zeros((rows,), dtype=np.float32),
        close_prices=close,
        daily_volumes=np.full((rows, symbols), 1000.0, dtype=np.float32),
        open_prices=open_px if include_day_trade_inputs else None,
        intraday_returns=intraday if include_day_trade_inputs else None,
        day_trade_eligible_mask=eligible if include_day_trade_inputs else None,
        day_trade_can_short_open_mask=(
            eligible.copy() if include_day_trade_inputs else None
        ),
        day_trade_can_buy_open_mask=(
            eligible.copy() if include_day_trade_inputs else None
        ),
        day_trade_can_sell_open_mask=(
            np.flip(eligible, axis=1).copy() if include_day_trade_inputs else None
        ),
        raw_close_returns_1d=returns + np.float32(0.20),
        corporate_action_avoidance_mask=np.zeros_like(eligible),
        unresolved_corporate_action_mask=np.zeros_like(eligible),
    )


def test_cash_keeps_empty_exchange_sessions_while_naive_filter_is_unchanged() -> None:
    panel = _make_panel()
    indices = np.arange(panel.num_dates)

    naive = CrossSectionalDataset(panel, indices, lookback=2)
    cash = CrossSectionalDataset(
        panel,
        indices,
        lookback=2,
        execution_mode="tw_cash",
    )

    np.testing.assert_array_equal(naive.valid_indices, [1, 3, 4, 5])
    np.testing.assert_array_equal(cash.valid_indices, [2, 3, 4, 5])
    empty_session = cash[0]
    # The model's causal outer mask sees that both symbols were alive after the
    # completed t-1 session.  Current-session fill masks stay executor-only and
    # correctly block both sides on this empty exchange row.
    assert empty_session["tradable_mask"].all()
    assert not empty_session["can_buy_mask"].any()
    assert not empty_session["can_sell_mask"].any()
    assert empty_session["session_advance_mask"].item() is True
    torch.testing.assert_close(empty_session["x"][:, 0, 0], torch.tensor([0.0, 1.0]))
    torch.testing.assert_close(
        empty_session["future_log_returns"],
        torch.from_numpy(panel.raw_close_returns_1d[2]),
    )


def test_cash_raw_close_returns_carry_halt_mark_and_catch_up_once() -> None:
    panel = _make_panel(include_day_trade_inputs=False)
    closes = np.array([100.0, np.nan, np.nan, 110.0, 121.0, 121.0])
    panel.close_prices[:, 0] = closes

    result = _attach_raw_close_forward_returns(panel)

    # Valuation carry must never overwrite the raw execution quote panel.
    np.testing.assert_allclose(
        result.close_prices[:, 0],
        closes,
        equal_nan=True,
    )
    np.testing.assert_allclose(
        result.raw_close_returns_1d[:, 0],
        [0.0, 0.0, np.log(1.1), np.log(1.1), 0.0, 0.0],
        rtol=1e-6,
        atol=1e-7,
    )


def test_cash_single_row_panel_has_exact_terminal_self_mark() -> None:
    panel = PanelData(
        dates=np.array(["2024-01-02"], dtype="datetime64[D]"),
        symbols=["2330"],
        feature_names=["feature"],
        features=np.zeros((1, 1, 1), dtype=np.float32),
        returns_1d=np.full((1, 1), np.nan, dtype=np.float32),
        tradable_mask=np.ones((1, 1), dtype=np.bool_),
        alive_mask=np.ones((1, 1), dtype=np.bool_),
        benchmark_returns=np.zeros(1, dtype=np.float32),
        close_prices=np.array([[100.0]], dtype=np.float32),
        force_exit_mask=np.zeros((1, 1), dtype=np.bool_),
        unresolved_corporate_action_mask=np.zeros((1, 1), dtype=np.bool_),
    )

    result = _attach_raw_close_forward_returns(panel)

    np.testing.assert_array_equal(result.raw_close_returns_1d, [[0.0]])


def test_cash_raw_close_returns_reset_basis_after_corporate_action() -> None:
    panel = _make_panel(include_day_trade_inputs=False)
    panel.close_prices[:, 0] = [100.0, 100.0, 40.0, 44.0, 44.0, 44.0]
    assert panel.unresolved_corporate_action_mask is not None
    panel.unresolved_corporate_action_mask[1, 0] = True

    result = _attach_raw_close_forward_returns(panel)

    assert result.raw_close_returns_1d[0, 0] == pytest.approx(0.0)
    assert np.isnan(result.raw_close_returns_1d[1, 0])
    assert result.raw_close_returns_1d[2, 0] == pytest.approx(
        np.log(44.0 / 40.0), rel=1e-6
    )


def test_tw_cash_dataset_execution_inputs_ignore_same_session_label_and_volume() -> None:
    baseline_panel = _make_panel(include_day_trade_inputs=False)
    perturbed_panel = _make_panel(include_day_trade_inputs=False)
    target_session = 4
    # Missing completed-session volume means no demonstrated fill capacity.
    assert baseline_panel.daily_volumes is not None
    assert perturbed_panel.daily_volumes is not None
    baseline_panel.daily_volumes[target_session - 1, 0] = np.nan
    perturbed_panel.daily_volumes[target_session - 1, 0] = np.nan
    # These two quantities are known only after the target session.  They may
    # change the supervised label, but never the signal window, outer/side
    # masks, or causal participation reference used to size the order.
    assert perturbed_panel.raw_close_returns_1d is not None
    perturbed_panel.raw_close_returns_1d[target_session] = [7.0, -7.0]
    perturbed_panel.daily_volumes[target_session] = [1.0, 9_999_999.0]

    baseline = CrossSectionalDataset(
        baseline_panel,
        np.arange(baseline_panel.num_dates),
        lookback=2,
        execution_mode="tw_cash",
    )
    perturbed = CrossSectionalDataset(
        perturbed_panel,
        np.arange(perturbed_panel.num_dates),
        lookback=2,
        execution_mode="tw_cash",
    )
    sample_index = int(np.flatnonzero(baseline.valid_indices == target_session)[0])
    baseline_sample = baseline[sample_index]
    perturbed_sample = perturbed[sample_index]

    for key in (
        "x",
        "tradable_mask",
        "can_buy_mask",
        "can_sell_mask",
        "volume_notional",
    ):
        torch.testing.assert_close(baseline_sample[key], perturbed_sample[key])
    assert not torch.equal(
        baseline_sample["future_log_returns"],
        perturbed_sample["future_log_returns"],
    )
    assert baseline_sample["volume_notional"][0].item() == 0.0
    assert baseline_sample["volume_notional"][1].item() == pytest.approx(
        float(
            baseline_panel.daily_volumes[target_session - 1, 1]
            * baseline_panel.close_prices[target_session, 1]
        )
    )


def test_day_trade_uses_only_t_minus_one_features_and_same_day_return() -> None:
    panel = _make_panel()
    dataset = CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates),
        lookback=2,
        execution_mode="tw_day_trade",
    )

    # The first tradable target is t=2: two complete feature sessions t=0,1
    # are inside the split.  Session t=2 is retained despite an all-false mask.
    np.testing.assert_array_equal(dataset.valid_indices, [2, 3, 4, 5])
    first = dataset[0]
    torch.testing.assert_close(first["x"][:, 0, 0], torch.tensor([0.0, 1.0]))
    assert 2.0 not in first["x"][:, 0, 0].tolist()
    torch.testing.assert_close(
        first["future_log_returns"],
        torch.from_numpy(panel.intraday_returns[2]),
    )
    # The model sees only the open-time gate.  Today's deliberately false
    # close/full-day tradability must not leak into an opening decision.
    assert first["tradable_mask"].all()
    assert first["session_advance_mask"].item() is True
    torch.testing.assert_close(
        first["day_trade_can_buy_open_mask"],
        torch.from_numpy(panel.day_trade_can_buy_open_mask[2]),
    )
    torch.testing.assert_close(
        first["day_trade_can_sell_open_mask"],
        torch.from_numpy(panel.day_trade_can_sell_open_mask[2]),
    )

    # Eligibility is also a fail-closed legal gate, but never removes the
    # exchange session needed to advance T+2 queues.
    second = dataset[1]
    assert not second["day_trade_eligible_mask"].any()
    assert not second["can_buy_mask"].any()
    assert second["session_advance_mask"].item() is True


def test_day_trade_window_exposes_current_open_gap_without_current_session_row() -> None:
    panel = _make_panel()
    # Row r stores the gap observed at open[r+1].  For target t=2, row 1 is
    # therefore the one legal current-session input while all other channels
    # remain close-complete rows 0 and 1.
    gaps = np.arange(10.0, 16.0, dtype=np.float32)[:, None, None]
    gaps = np.broadcast_to(gaps, (panel.num_dates, panel.num_symbols, 1)).copy()
    panel.features = np.concatenate([panel.features, gaps], axis=2)
    panel.feature_names.append(DAY_TRADE_OPEN_GAP_FEATURE)
    panel.daily_volumes[2] = np.float32(999_999_999.0)
    panel.intraday_returns[2] = np.asarray([7.0, -7.0], dtype=np.float32)

    dataset = CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates),
        lookback=2,
        execution_mode="tw_day_trade",
    )
    first = dataset[0]

    torch.testing.assert_close(first["x"][:, 0, 0], torch.tensor([0.0, 1.0]))
    torch.testing.assert_close(first["x"][:, 0, 1], torch.tensor([10.0, 11.0]))
    assert 12.0 not in first["x"][:, 0, 1].tolist()


def test_day_trade_open_inputs_do_not_leak_close_label_or_full_day_volume() -> None:
    panel = _make_panel()
    panel.benchmark_returns[:] = np.arange(
        0.01,
        0.07,
        0.01,
        dtype=np.float32,
    )
    panel.daily_volumes[:, 0] = np.arange(10.0, 16.0, dtype=np.float32)
    panel.open_prices[:, 0] = np.arange(100.0, 106.0, dtype=np.float32)
    # Row 2 deliberately has no close-side tradability in _make_panel.  We also
    # remove one realized open fill while retaining legal eligibility: neither
    # execution outcome may alter the target-selection outer mask.
    panel.intraday_returns[2, 0] = np.nan
    panel.day_trade_can_buy_open_mask[2, 0] = False
    panel.day_trade_can_sell_open_mask[2, 0] = False
    panel.tradable_mask[2, 1] = True
    panel.can_buy_mask[2, 1] = True
    panel.can_sell_mask[2, 1] = True
    panel.can_short_open_mask[2, 1] = False
    panel.day_trade_can_short_open_mask[2, 1] = True
    panel.daily_volumes[1, 1] = np.nan

    dataset = CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates),
        lookback=2,
        execution_mode="tw_day_trade",
    )
    first = dataset[0]

    assert first["tradable_mask"][0].item() is True
    assert not first["day_trade_can_buy_open_mask"][0].item()
    assert not first["day_trade_can_sell_open_mask"][0].item()
    assert not first["can_buy_mask"][0].item()
    assert not first["can_sell_mask"][0].item()
    assert first["can_short_open_mask"][1].item() is True
    assert torch.isnan(first["future_log_returns"][0])
    # Opening liquidity uses completed session t-1 volume (11), valued at the
    # known t open (102), never eventual session-t volume (12).
    assert first["volume_notional"][0].item() == pytest.approx(11.0 * 102.0)
    assert first["volume_notional"][1].item() == pytest.approx(0.0)
    assert dataset.benchmark_t[2].item() == pytest.approx(
        float(panel.benchmark_returns[1])
    )
    assert dataset.benchmark_t[2].item() != pytest.approx(
        float(panel.intraday_returns[2, 1])
    )


def test_tw_cash_dataset_preserves_missing_return_for_active_state_validation() -> None:
    panel = _make_panel()
    panel.raw_close_returns_1d[2, 0] = np.nan
    dataset = CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates),
        lookback=2,
        execution_mode="tw_cash",
    )

    assert torch.isnan(dataset[0]["future_log_returns"][0])


def test_tw_cash_shorting_requires_capacity_and_preserves_forced_cover() -> None:
    panel = _make_panel()
    panel.short_capacity_shares = np.zeros_like(
        panel.tradable_mask,
        dtype=np.int64,
    )
    panel.short_capacity_shares[3, 0] = 7_000
    panel.short_margin_rate = np.full(
        panel.tradable_mask.shape,
        np.nan,
        dtype=np.float32,
    )
    panel.short_margin_rate[3, 0] = np.float32(1.3)
    panel.force_short_cover_mask = np.zeros_like(panel.tradable_mask, dtype=bool)
    panel.force_short_cover_mask[3, 1] = True

    dataset = CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates),
        lookback=2,
        execution_mode="tw_cash",
    )
    row3 = int(np.where(dataset.valid_indices == 3)[0][0])
    sample = dataset[row3]

    assert sample["can_short_open_mask"].tolist() == [True, False]
    assert sample["short_capacity_shares"].tolist() == [7_000, 0]
    assert sample["short_capacity_notional"].dtype == torch.float32
    assert sample["short_capacity_notional"].tolist() == [700_000.0, 0.0]
    assert sample["short_margin_rate"][0].item() == pytest.approx(1.3)
    assert sample["short_margin_rate"][1].item() == pytest.approx(0.9)
    assert sample["force_short_cover_mask"].tolist() == [False, True]

    padded = collate_batch([sample], batch_size=2)
    assert padded["short_capacity_shares"][1].tolist() == [0, 0]
    assert padded["short_capacity_notional"][1].tolist() == [0.0, 0.0]
    assert torch.isnan(padded["short_margin_rate"][1]).all()


def test_tw_cash_missing_margin_evidence_is_false_and_zero_not_can_sell() -> None:
    panel = _make_panel()
    panel.short_capacity_shares = None
    panel.short_margin_rate = None
    assert panel.can_sell_mask.any()

    dataset = CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates),
        lookback=2,
        execution_mode="tw_cash",
    )

    assert not dataset.can_short_open_mask_t.any()
    assert not dataset.short_capacity_shares_t.any()
    assert not dataset.short_capacity_notional_t.any()
    torch.testing.assert_close(
        dataset.short_margin_rate_t,
        torch.full_like(dataset.short_margin_rate_t, 0.9),
    )


def test_tw_cash_disabled_capacity_limit_preserves_eligibility_without_inventory() -> None:
    panel = _make_panel()
    panel.short_capacity_shares = None
    panel.short_margin_rate = None
    expected_eligibility = np.asarray(panel.can_short_open_mask) & np.asarray(
        panel.can_sell_mask
    )

    dataset = CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates),
        lookback=2,
        execution_mode="tw_cash",
        short_capacity_limit_enabled=False,
    )

    np.testing.assert_array_equal(
        dataset.can_short_open_mask_t.numpy(), expected_eligibility
    )
    np.testing.assert_array_equal(
        dataset.short_capacity_shares_t.numpy(),
        expected_eligibility.astype(np.int64),
    )


@pytest.mark.parametrize("bad_close", [np.nan, 0.0, -1.0, np.inf])
def test_tw_cash_short_capacity_notional_fails_closed_on_invalid_price(
    bad_close: float,
) -> None:
    panel = _make_panel()
    panel.short_capacity_shares = np.zeros_like(
        panel.tradable_mask,
        dtype=np.int64,
    )
    panel.short_capacity_shares[3, 0] = 7_000
    panel.close_prices[3, 0] = bad_close

    dataset = CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates),
        lookback=2,
        execution_mode="tw_cash",
    )

    assert dataset.short_capacity_shares_t[3, 0].item() == 7_000
    assert dataset.short_capacity_notional_t[3, 0].item() == 0.0


def test_tw_cash_short_capacity_notional_multiplies_exact_integer_shares() -> None:
    panel = _make_panel()
    exact_shares = 16_777_217
    panel.short_capacity_shares = np.zeros_like(
        panel.tradable_mask,
        dtype=np.int64,
    )
    panel.short_capacity_shares[3, 0] = exact_shares
    panel.close_prices[3, 0] = np.float32(3.0)

    dataset = CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates),
        lookback=2,
        execution_mode="tw_cash",
    )

    expected = np.float32(np.float64(exact_shares) * np.float64(3.0))
    assert dataset.short_capacity_notional_t[3, 0].item() == expected.item()
    assert expected.item() != (np.float32(exact_shares) * np.float32(3.0)).item()


def test_tw_cash_short_capacity_keeps_int64_oracle_above_float64_exact_range() -> None:
    panel = _make_panel()
    exact_shares = 2**53 + 1
    panel.short_capacity_shares = np.zeros_like(
        panel.tradable_mask,
        dtype=np.int64,
    )
    panel.short_capacity_shares[3, 0] = exact_shares

    dataset = CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates),
        lookback=2,
        execution_mode="tw_cash",
    )

    assert dataset.short_capacity_shares_t.dtype == torch.int64
    assert dataset.short_capacity_shares_t[3, 0].item() == exact_shares


def test_short_margin_rate_uses_non_contiguous_official_historical_floors() -> None:
    dates = np.asarray(
        [
            "2015-08-12",
            "2015-08-13",
            "2015-10-15",
            "2015-10-16",
            "2016-01-07",
            "2016-01-08",
            "2016-02-29",
            "2016-03-01",
        ],
        dtype="datetime64[D]",
    )
    rows = len(dates)
    shape = (rows, 1)
    raw_margin_rate = np.full(shape, np.nan, dtype=np.float32)
    raw_margin_rate[1, 0] = np.float32(1.25)
    raw_margin_rate[2, 0] = np.float32(0.80)
    panel = PanelData(
        dates=dates,
        symbols=["2330"],
        feature_names=["feature"],
        features=np.zeros((rows, 1, 1), dtype=np.float32),
        returns_1d=np.zeros(shape, dtype=np.float32),
        tradable_mask=np.ones(shape, dtype=bool),
        can_buy_mask=np.ones(shape, dtype=bool),
        can_sell_mask=np.ones(shape, dtype=bool),
        can_short_open_mask=np.ones(shape, dtype=bool),
        alive_mask=np.ones(shape, dtype=bool),
        benchmark_returns=np.zeros(rows, dtype=np.float32),
        close_prices=np.full(shape, 100.0, dtype=np.float32),
        raw_close_returns_1d=np.zeros(shape, dtype=np.float32),
        short_capacity_shares=np.full(shape, 1_000, dtype=np.int64),
        short_margin_rate=raw_margin_rate,
        unresolved_corporate_action_mask=np.zeros(shape, dtype=bool),
    )

    dataset = CrossSectionalDataset(
        panel,
        np.arange(rows),
        lookback=1,
        execution_mode="tw_cash",
    )

    torch.testing.assert_close(
        dataset.short_margin_rate_t[:, 0],
        torch.tensor([0.9, 1.25, 1.2, 0.9, 0.9, 1.2, 1.2, 0.9]),
    )
    assert torch.isfinite(dataset.short_margin_rate_t).all()


def test_day_trade_benchmark_uses_same_session_buy_and_hold_window() -> None:
    panel = _make_panel()
    panel.benchmark_returns[:] = np.asarray(
        [0.01, 0.02, 0.03, 0.04, 0.05, 0.06],
        dtype=np.float32,
    )
    dataset = CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates),
        lookback=2,
        execution_mode="tw_day_trade",
    )
    expected = np.asarray(
        [0.0, 0.01, 0.02, 0.03, 0.04, 0.05],
        dtype=np.float32,
    )

    np.testing.assert_allclose(dataset.benchmark_t.numpy(), expected)
    assert dataset[0]["benchmark"].item() == pytest.approx(float(expected[2]))
    assert dataset[0]["benchmark"].item() == pytest.approx(0.02)


@pytest.mark.parametrize(
    ("missing", "message"),
    [
        ("intraday_returns", "intraday_returns"),
        ("day_trade_eligible_mask", "point-in-time"),
        ("day_trade_can_buy_open_mask", "open-session"),
        ("day_trade_can_sell_open_mask", "open-session"),
    ],
)
def test_day_trade_fails_closed_without_required_historical_inputs(
    missing: str,
    message: str,
) -> None:
    panel = _make_panel()
    setattr(panel, missing, None)
    with pytest.raises(ValueError, match=message):
        CrossSectionalDataset(
            panel,
            np.arange(panel.num_dates),
            lookback=2,
            execution_mode="tw_day_trade",
        )


def test_collate_marks_padding_as_not_an_exchange_session() -> None:
    panel = _make_panel()
    dataset = CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates),
        lookback=2,
        execution_mode="tw_day_trade",
    )
    batch = collate_batch([dataset[0], dataset[1]], batch_size=3)

    torch.testing.assert_close(
        batch["session_advance_mask"], torch.tensor([True, True, False])
    )
    torch.testing.assert_close(
        batch["sample_mask"], torch.tensor([True, True, False])
    )
    assert not batch["day_trade_eligible_mask"][-1].any()
    assert not batch["day_trade_can_buy_open_mask"][-1].any()
    assert not batch["day_trade_can_sell_open_mask"][-1].any()
    assert not batch["unresolved_corporate_action_mask"][-1].any()


def _write_symbol_parquet(path) -> None:
    start = datetime(2024, 1, 2)
    table = pa.table(
        {
            "date": pa.array(
                [start + timedelta(days=offset) for offset in range(3)],
                type=pa.timestamp("ns"),
            ),
            "open": pa.array([100.0, 101.0, 102.0]),
            "max": pa.array([102.0, 103.0, 104.0]),
            "min": pa.array([99.0, 99.0, 101.0]),
            "close": pa.array([101.0, 100.0, 103.0]),
            "adjclose": pa.array([101.0, 100.0, 103.0]),
            "Trading_Volume": pa.array([1000.0, 1200.0, 900.0]),
            "day_trade_eligible": pa.array([1, 0, 1], type=pa.int8()),
        }
    )
    pq.write_table(table, path)


def test_pyarrow_polars_open_intraday_and_eligibility_parity(tmp_path) -> None:
    path = tmp_path / "2330_features.parquet"
    _write_symbol_parquet(path)

    arrow_arrays = _load_symbol_arrays_pyarrow(path)
    polars_arrays = _load_symbol_arrays_polars_lazy(path)
    arrow_panel = _build_panel_from_symbol_arrays([arrow_arrays])
    polars_panel = _build_panel_from_symbol_arrays([polars_arrays])

    expected_open = np.asarray([[100.0], [101.0], [102.0]], dtype=np.float32)
    expected_intraday = np.log(
        np.asarray([[101.0 / 100.0], [100.0 / 101.0], [103.0 / 102.0]])
    ).astype(np.float32)
    np.testing.assert_array_equal(arrow_panel.open_prices, expected_open)
    np.testing.assert_allclose(
        arrow_panel.intraday_returns,
        expected_intraday,
        rtol=1e-6,
        atol=1e-7,
    )
    np.testing.assert_array_equal(
        arrow_panel.day_trade_eligible_mask,
        np.asarray([[True], [False], [True]]),
    )
    np.testing.assert_array_equal(polars_panel.open_prices, arrow_panel.open_prices)
    np.testing.assert_allclose(
        polars_panel.intraday_returns,
        arrow_panel.intraday_returns,
        rtol=0.0,
        atol=0.0,
    )
    np.testing.assert_array_equal(
        polars_panel.day_trade_eligible_mask,
        arrow_panel.day_trade_eligible_mask,
    )


def test_execution_arrays_round_trip_panel_cache(tmp_path) -> None:
    source = tmp_path / "2330_features.parquet"
    source.write_bytes(b"source-placeholder")
    panel = _make_panel()
    panel.short_capacity_shares = np.arange(
        panel.num_dates * panel.num_symbols,
        dtype=np.int64,
    ).reshape(panel.num_dates, panel.num_symbols)
    panel.short_margin_rate = np.full(
        panel.tradable_mask.shape,
        1.3,
        dtype=np.float32,
    )
    save_panel_cache_v2(
        tmp_path,
        panel,
        source_hash="tw-execution-v1",
        backend_key="pyarrow|tw-execution-data",
        version=PANEL_CACHE_VERSION,
    )

    payload = load_panel_cache_v2(tmp_path, mmap_mode="r")
    restored = _panel_from_cache_payload(payload)
    np.testing.assert_array_equal(restored.open_prices, panel.open_prices)
    np.testing.assert_array_equal(restored.intraday_returns, panel.intraday_returns)
    np.testing.assert_array_equal(
        restored.day_trade_eligible_mask,
        panel.day_trade_eligible_mask,
    )
    np.testing.assert_array_equal(
        restored.day_trade_can_short_open_mask,
        panel.day_trade_can_short_open_mask,
    )
    np.testing.assert_array_equal(
        restored.day_trade_can_buy_open_mask,
        panel.day_trade_can_buy_open_mask,
    )
    np.testing.assert_array_equal(
        restored.day_trade_can_sell_open_mask,
        panel.day_trade_can_sell_open_mask,
    )
    np.testing.assert_array_equal(
        restored.raw_close_returns_1d,
        panel.raw_close_returns_1d,
    )
    np.testing.assert_array_equal(
        restored.corporate_action_avoidance_mask,
        panel.corporate_action_avoidance_mask,
    )
    np.testing.assert_array_equal(
        restored.unresolved_corporate_action_mask,
        panel.unresolved_corporate_action_mask,
    )
    np.testing.assert_array_equal(
        restored.short_capacity_shares,
        panel.short_capacity_shares,
    )
    np.testing.assert_array_equal(
        restored.short_margin_rate,
        panel.short_margin_rate,
    )


def test_cache_preserves_absent_eligibility_as_none(tmp_path) -> None:
    panel = _make_panel(include_day_trade_inputs=False)
    save_panel_cache_v2(
        tmp_path,
        panel,
        source_hash="no-eligibility",
        backend_key="pyarrow|no-eligibility",
        version=PANEL_CACHE_VERSION,
    )
    restored = _panel_from_cache_payload(load_panel_cache_v2(tmp_path))
    assert restored.open_prices is None
    assert restored.intraday_returns is None
    assert restored.day_trade_eligible_mask is None
    assert restored.day_trade_can_short_open_mask is None
    assert restored.day_trade_can_buy_open_mask is None
    assert restored.day_trade_can_sell_open_mask is None
    assert restored.short_capacity_shares is None
    assert restored.short_margin_rate is None


def test_official_actions_drive_cash_raw_returns_with_backend_parity(tmp_path) -> None:
    public_dir = tmp_path / "data_tw_public"
    stocks_dir = public_dir / "stocks"
    features_dir = public_dir / "features"
    stocks_dir.mkdir(parents=True)
    features_dir.mkdir()
    dates = [datetime(2024, 1, day) for day in (2, 3, 5, 8)]

    def write_symbol(symbol: str, selected: list[int], close: list[float], adj: list[float]) -> None:
        values = [dates[index] for index in selected]
        pq.write_table(
            pa.table(
                {
                    "date": pa.array(values, type=pa.timestamp("ns")),
                    "open": close,
                    "max": close,
                    "min": close,
                    "close": close,
                    "adjclose": adj,
                    "Trading_Volume": [1000.0] * len(close),
                }
            ),
            stocks_dir / f"{symbol}_features.parquet",
        )

    write_symbol("AAA", [0, 1, 2, 3], [10.0, 10.0, 40.0, 42.0], [10.0, 10.0, 10.0, 10.5])
    # BBB deliberately misses the 2024-01-03 exchange session.  Its 1/2 quote
    # must never connect directly to 1/5.
    write_symbol("BBB", [0, 2, 3], [20.0, 22.0, 23.0], [20.0, 22.0, 23.0])

    external_path = features_dir / "tw_public_stock_daily.parquet"
    pq.write_table(
        pa.table(
            {
                "date": pa.array(dates, type=pa.timestamp("ns")),
                "symbol": ["__MARKET__"] * len(dates),
                "_twpub_official_traded": [1.0] * len(dates),
            }
        ),
        external_path,
    )
    _write_corporate_action_reference(
        public_dir,
        rows=[
            {
                # A declared ex-date can become a whole-market closure.  It
                # must still resolve to the last executable session before it.
                "date": datetime(2024, 1, 4).date(),
                "symbol": "AAA",
                "market": "twse",
                "reference_price": 10.0,
                "opening_reference_price": 10.0,
                "previous_close": 10.0,
                "event_type": "除權",
                "source_url": "https://example.invalid/official",
            },
            {
                # The next weekday after the final quote must protect the
                # carried terminal holding without pulling a later event back.
                "date": datetime(2024, 1, 9).date(),
                "symbol": "BBB",
                "market": "twse",
                "reference_price": 23.0,
                "opening_reference_price": 23.0,
                "previous_close": 23.0,
                "event_type": "除息",
                "source_url": "https://example.invalid/official",
            },
            {
                # A later future event has unobserved business sessions before
                # it and therefore must not collapse onto the panel tail.
                "date": datetime(2024, 1, 12).date(),
                "symbol": "AAA",
                "market": "twse",
                "reference_price": 42.0,
                "opening_reference_price": 42.0,
                "previous_close": 42.0,
                "event_type": "除息",
                "source_url": "https://example.invalid/official",
            },
        ],
    )

    panels = []
    for backend in ("pyarrow", "polars_lazy"):
        panels.append(
            build_panel(
                stocks_dir,
                panel_backend=backend,
                panel_load_workers=0,
                external_feature_path=external_path,
                external_include_features=False,
                external_include_rules=True,
            )
        )
    arrow_panel, polars_panel = panels
    aaa = arrow_panel.symbols.index("AAA")
    bbb = arrow_panel.symbols.index("BBB")
    event_transition = int(
        np.flatnonzero(arrow_panel.dates.astype("datetime64[D]") == np.datetime64("2024-01-03"))[0]
    )
    assert arrow_panel.unresolved_corporate_action_mask[event_transition, aaa]
    assert arrow_panel.unresolved_corporate_action_mask[-1, bbb]
    assert int(arrow_panel.unresolved_corporate_action_mask.sum()) == 2
    # Exact raw-share accounting liquidates AAA before the unresolved action,
    # so its pre/post-action price bases must never be connected.  BBB's
    # internal missing official session is instead carried at the last close
    # and the catch-up change is recognized exactly once on resumption.
    assert np.isnan(arrow_panel.raw_close_returns_1d[event_transition, aaa])
    assert arrow_panel.returns_1d[event_transition, aaa] == pytest.approx(0.0)
    assert arrow_panel.raw_close_returns_1d[0, bbb] == pytest.approx(0.0)
    assert arrow_panel.raw_close_returns_1d[1, bbb] == pytest.approx(
        np.log(22.0 / 20.0), rel=1e-6
    )
    np.testing.assert_array_equal(
        polars_panel.unresolved_corporate_action_mask,
        arrow_panel.unresolved_corporate_action_mask,
    )
    np.testing.assert_allclose(
        polars_panel.raw_close_returns_1d,
        arrow_panel.raw_close_returns_1d,
        rtol=0.0,
        atol=0.0,
        equal_nan=True,
    )
    np.testing.assert_allclose(
        polars_panel.returns_1d,
        arrow_panel.returns_1d,
        rtol=0.0,
        atol=0.0,
        equal_nan=True,
    )

    restored = load_cached_panel(
        stocks_dir,
        panel_backend="polars_lazy",
        external_feature_path=external_path,
        external_include_features=False,
        external_include_rules=True,
    )
    assert restored is not None
    np.testing.assert_array_equal(
        restored.unresolved_corporate_action_mask,
        polars_panel.unresolved_corporate_action_mask,
    )
    np.testing.assert_allclose(
        restored.raw_close_returns_1d,
        polars_panel.raw_close_returns_1d,
        equal_nan=True,
    )


def test_corporate_action_tail_includes_closed_ex_date_before_next_session() -> None:
    dates = np.asarray(["2024-01-05", "2024-01-08"], dtype="datetime64[D]")
    rows, symbols = 2, 2
    mask = np.ones((rows, symbols), dtype=np.bool_)
    panel = PanelData(
        dates=dates,
        symbols=["TUE", "THU"],
        feature_names=["x"],
        features=np.zeros((rows, symbols, 1), dtype=np.float32),
        returns_1d=np.zeros((rows, symbols), dtype=np.float32),
        tradable_mask=mask,
        alive_mask=mask.copy(),
        benchmark_returns=np.zeros(rows, dtype=np.float32),
        close_prices=np.ones((rows, symbols), dtype=np.float32),
    )
    reference = _CorporateActionReference(
        event_dates_by_symbol={
            # Tuesday is a declared ex-date but a whole-market closure.  With
            # Wednesday as the next verified session, Monday is the final
            # executable close and must be protected.
            "TUE": np.asarray(["2024-01-09"], dtype="datetime64[ns]"),
            # Thursday has an intervening verified Wednesday session and must
            # not be pulled backward onto Monday's panel tail.
            "THU": np.asarray(["2024-01-11"], dtype="datetime64[ns]"),
        },
        coverage_start=np.datetime64("2024-01-01", "D"),
        coverage_end=np.datetime64("2024-01-31", "D"),
    )

    result = _apply_corporate_action_avoidance_transitions(
        panel,
        reference,
        official_session_dates=np.asarray(
            ["2024-01-05", "2024-01-08", "2024-01-10"],
            dtype="datetime64[D]",
        ),
    )

    assert result.unresolved_corporate_action_mask is not None
    assert result.unresolved_corporate_action_mask[-1, 0]
    assert not result.unresolved_corporate_action_mask[:, 1].any()
    assert int(result.unresolved_corporate_action_mask.sum()) == 1


def test_exact_cash_entitlement_replaces_avoidance_with_payment_claim() -> None:
    dates = np.asarray(
        ["2024-06-28", "2024-07-01", "2024-07-02", "2024-07-03"],
        dtype="datetime64[D]",
    )
    mask = np.ones((4, 1), dtype=np.bool_)
    panel = PanelData(
        dates=dates,
        symbols=["2330"],
        feature_names=["x"],
        features=np.zeros((4, 1, 1), dtype=np.float32),
        returns_1d=np.zeros((4, 1), dtype=np.float32),
        tradable_mask=mask,
        alive_mask=mask.copy(),
        benchmark_returns=np.zeros(4, dtype=np.float32),
        close_prices=np.asarray([[100.0], [90.0], [90.0], [90.0]], dtype=np.float32),
    )
    reference = _CorporateActionReference(
        event_dates_by_symbol={
            "2330": np.asarray(["2024-07-01"], dtype="datetime64[D]")
        },
        coverage_start=np.datetime64("2024-01-01", "D"),
        coverage_end=np.datetime64("2024-12-31", "D"),
        exact_cash_terms_by_symbol={
            "2330": (
                np.asarray(["2024-07-01"], dtype="datetime64[D]"),
                np.asarray([10.0], dtype=np.float64),
                np.asarray(["2024-07-03"], dtype="datetime64[D]"),
            )
        },
        exact_coverage_start=np.datetime64("2024-01-01", "D"),
        exact_coverage_end=np.datetime64("2024-12-31", "D"),
    )

    result = _apply_corporate_action_avoidance_transitions(panel, reference)

    assert result.corporate_action_avoidance_mask[0, 0]
    assert not result.unresolved_corporate_action_mask.any()
    assert result.cash_dividend_yield[0, 0] == pytest.approx(0.1)
    assert result.cash_dividend_payment_delay_sessions[0, 0] == 3
    assert int(np.count_nonzero(result.cash_dividend_yield)) == 1

    result = _attach_raw_close_forward_returns(result)
    assert result.raw_close_returns_1d[0, 0] == pytest.approx(np.log(0.9))


def test_avoid_mode_keeps_full_exact_action_exit_interval() -> None:
    dates = np.asarray(
        ["2024-06-28", "2024-07-01", "2024-07-02", "2024-07-03", "2024-07-05"],
        dtype="datetime64[D]",
    )
    mask = np.ones((dates.size, 1), dtype=np.bool_)
    can_sell = mask.copy()
    can_sell[2, 0] = False
    panel = PanelData(
        dates=dates,
        symbols=["2330"],
        feature_names=["x"],
        features=np.zeros((dates.size, 1, 1), dtype=np.float32),
        returns_1d=np.zeros((dates.size, 1), dtype=np.float32),
        tradable_mask=mask,
        can_buy_mask=mask.copy(),
        can_sell_mask=can_sell,
        alive_mask=mask.copy(),
        benchmark_returns=np.zeros(dates.size, dtype=np.float32),
        close_prices=np.full((dates.size, 1), 100.0, dtype=np.float32),
    )
    reference = _CorporateActionReference(
        event_dates_by_symbol={
            "2330": np.asarray(["2024-07-03"], dtype="datetime64[D]")
        },
        coverage_start=np.datetime64("2024-01-01", "D"),
        coverage_end=np.datetime64("2024-12-31", "D"),
        exact_cash_terms_by_symbol={
            "2330": (
                np.asarray(["2024-07-03"], dtype="datetime64[D]"),
                np.asarray([2.0], dtype=np.float64),
                np.asarray(["2024-07-05"], dtype="datetime64[D]"),
            )
        },
        exact_coverage_start=np.datetime64("2024-01-01", "D"),
        exact_coverage_end=np.datetime64("2024-12-31", "D"),
    )

    result = _attach_raw_close_forward_returns(
        _apply_corporate_action_avoidance_transitions(panel, reference)
    )
    assert np.flatnonzero(result.corporate_action_avoidance_mask[:, 0]).tolist() == [1, 2]
    assert not result.unresolved_corporate_action_mask.any()

    avoid = CrossSectionalDataset(
        result,
        np.arange(result.num_dates),
        lookback=1,
        execution_mode="tw_cash",
        tw_corporate_action_mode="avoid",
    )
    exact = CrossSectionalDataset(
        result,
        np.arange(result.num_dates),
        lookback=1,
        execution_mode="tw_cash",
        tw_corporate_action_mode="exact",
    )
    assert np.flatnonzero(
        avoid.unresolved_corporate_action_mask_t[:, 0].numpy()
    ).tolist() == [1, 2]
    assert not exact.unresolved_corporate_action_mask_t.any()
    assert exact.cash_dividend_yield_t[2, 0] == pytest.approx(0.02)


def test_exact_entitlement_loader_verifies_raw_mops_manifest(tmp_path) -> None:
    reference_path = tmp_path / "tw_corporate_action_reference.parquet"
    reference_path.write_bytes(b"receipt-bound reference")
    entitlement_path = tmp_path / "tw_corporate_action_entitlements.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "date": date(2024, 7, 1),
                    "symbol": "2330",
                    "handling": "exact_cash",
                    "cash_dividend_per_share": 10.0,
                    "cash_payment_date": date(2024, 7, 31),
                    "stop_transfer_start": date(2024, 7, 5),
                }
            ],
            schema=pa.schema(
                [
                    ("date", pa.date32()),
                    ("symbol", pa.large_string()),
                    ("handling", pa.large_string()),
                    ("cash_dividend_per_share", pa.float64()),
                    ("cash_payment_date", pa.date32()),
                    ("stop_transfer_start", pa.date32()),
                ]
            ),
        ),
        entitlement_path,
    )
    manifest_content = b'{"response_sha256":"official"}\n'
    manifest_sha = hashlib.sha256(manifest_content).hexdigest()
    manifest_path = (
        tmp_path
        / "raw"
        / "tw_corporate_action_entitlements"
        / "manifests"
        / f"{manifest_sha}.jsonl"
    )
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_bytes(manifest_content)
    entitlement_summary = entitlement_path.with_suffix(".summary.json")
    entitlement_summary.write_text(
        json.dumps(
            {
                "baseline_established": True,
                "coverage_complete": True,
                "failure_count": 0,
                "schema_version": 3,
                "coverage_start": "2024-01-01",
                "coverage_end": "2024-12-31",
                "rows": 1,
                "reference_rows": 1,
                "reference_receipt": {
                    "size": reference_path.stat().st_size,
                    "sha256": hashlib.sha256(reference_path.read_bytes()).hexdigest(),
                },
                "output_receipt": {
                    "size": entitlement_path.stat().st_size,
                    "sha256": hashlib.sha256(entitlement_path.read_bytes()).hexdigest(),
                },
                "raw_receipt_manifest": {
                    "relative_path": manifest_path.relative_to(tmp_path).as_posix(),
                    "size": manifest_path.stat().st_size,
                    "sha256": manifest_sha,
                    "entries": 1,
                },
            }
        ),
        encoding="utf-8",
    )
    paths = _CorporateActionReferencePaths(
        parquet=reference_path,
        summary=reference_path.with_suffix(".summary.json"),
        entitlements_parquet=entitlement_path,
        entitlements_summary=entitlement_summary,
    )

    terms, short_terms, start, end = _load_exact_cash_entitlements(paths)

    assert terms is not None and terms["2330"][1].tolist() == [10.0]
    assert short_terms is not None
    assert start == np.datetime64("2024-01-01")
    assert end == np.datetime64("2024-12-31")

    manifest_path.write_bytes(manifest_content + b"tamper\n")
    with pytest.raises(ValueError, match="manifest size mismatch"):
        _load_exact_cash_entitlements(paths)


def test_exact_cash_entitlement_applies_article76_short_deadline_and_ban() -> None:
    dates = np.asarray(
        [
            "2024-06-25",
            "2024-06-26",
            "2024-06-27",
            "2024-06-28",
            "2024-07-01",
            "2024-07-02",
            "2024-07-03",
            "2024-07-04",
            "2024-07-05",
            "2024-07-08",
        ],
        dtype="datetime64[D]",
    )
    mask = np.ones((dates.size, 1), dtype=np.bool_)
    panel = PanelData(
        dates=dates,
        symbols=["2330"],
        feature_names=["x"],
        features=np.zeros((dates.size, 1, 1), dtype=np.float32),
        returns_1d=np.zeros((dates.size, 1), dtype=np.float32),
        tradable_mask=mask,
        can_buy_mask=mask.copy(),
        can_sell_mask=mask.copy(),
        can_short_open_mask=mask.copy(),
        alive_mask=mask.copy(),
        benchmark_returns=np.zeros(dates.size, dtype=np.float32),
        close_prices=np.full((dates.size, 1), 100.0, dtype=np.float32),
    )
    reference = _CorporateActionReference(
        event_dates_by_symbol={
            "2330": np.asarray(["2024-07-03"], dtype="datetime64[D]")
        },
        coverage_start=np.datetime64("2024-01-01", "D"),
        coverage_end=np.datetime64("2024-12-31", "D"),
        exact_cash_terms_by_symbol={
            "2330": (
                np.asarray(["2024-07-03"], dtype="datetime64[D]"),
                np.asarray([10.0]),
                np.asarray(["2024-07-08"], dtype="datetime64[D]"),
            )
        },
        exact_coverage_start=np.datetime64("2024-01-01", "D"),
        exact_coverage_end=np.datetime64("2024-12-31", "D"),
        margin_short_stop_transfer_by_symbol={
            "2330": (
                np.asarray(["2024-07-03"], dtype="datetime64[D]"),
                np.asarray(["2024-07-05"], dtype="datetime64[D]"),
            )
        },
    )

    result = _apply_corporate_action_avoidance_transitions(panel, reference)

    assert result.unresolved_corporate_action_mask is not None
    assert not result.unresolved_corporate_action_mask.any()
    assert np.flatnonzero(result.force_short_cover_mask[:, 0]).tolist() == [2]
    assert np.flatnonzero(~result.can_short_open_mask[:, 0]).tolist() == [2, 3, 4, 5]
    assert result.cash_dividend_yield[5, 0] == pytest.approx(0.1)
    assert result.cash_dividend_payment_delay_sessions[5, 0] == 4


def test_article76_early_cover_blocks_reopen_before_statutory_deadline() -> None:
    dates = np.asarray(
        [
            "2024-06-25",
            "2024-06-26",
            "2024-06-27",
            "2024-06-28",
            "2024-07-01",
            "2024-07-02",
            "2024-07-03",
            "2024-07-04",
            "2024-07-05",
            "2024-07-08",
        ],
        dtype="datetime64[D]",
    )
    mask = np.ones((dates.size, 1), dtype=np.bool_)
    can_buy = mask.copy()
    # The legal deadline is row 2, but that session has no executable buy side.
    can_buy[2, 0] = False
    panel = PanelData(
        dates=dates,
        symbols=["2330"],
        feature_names=["x"],
        features=np.zeros((dates.size, 1, 1), dtype=np.float32),
        returns_1d=np.zeros((dates.size, 1), dtype=np.float32),
        tradable_mask=mask,
        can_buy_mask=can_buy,
        can_sell_mask=mask.copy(),
        can_short_open_mask=mask.copy(),
        alive_mask=mask.copy(),
        benchmark_returns=np.zeros(dates.size, dtype=np.float32),
        close_prices=np.full((dates.size, 1), 100.0, dtype=np.float32),
    )
    reference = _CorporateActionReference(
        event_dates_by_symbol={},
        coverage_start=np.datetime64("2024-01-01", "D"),
        coverage_end=np.datetime64("2024-12-31", "D"),
        exact_cash_terms_by_symbol={},
        exact_coverage_start=np.datetime64("2024-01-01", "D"),
        exact_coverage_end=np.datetime64("2024-12-31", "D"),
        margin_short_stop_transfer_by_symbol={
            "2330": (
                np.asarray(["2024-07-03"], dtype="datetime64[D]"),
                np.asarray(["2024-07-05"], dtype="datetime64[D]"),
            )
        },
    )

    result = _apply_corporate_action_avoidance_transitions(panel, reference)

    assert np.flatnonzero(result.force_short_cover_mask[:, 0]).tolist() == [1]
    assert np.flatnonzero(~result.can_short_open_mask[:, 0]).tolist() == [1, 2, 3, 4, 5]


def test_article76_impossible_cover_is_not_silently_dropped() -> None:
    dates = np.asarray(
        [
            "2024-06-25",
            "2024-06-26",
            "2024-06-27",
            "2024-06-28",
            "2024-07-01",
            "2024-07-02",
            "2024-07-03",
            "2024-07-04",
            "2024-07-05",
        ],
        dtype="datetime64[D]",
    )
    mask = np.ones((dates.size, 1), dtype=np.bool_)
    panel = PanelData(
        dates=dates,
        symbols=["2330"],
        feature_names=["x"],
        features=np.zeros((dates.size, 1, 1), dtype=np.float32),
        returns_1d=np.zeros((dates.size, 1), dtype=np.float32),
        tradable_mask=mask,
        can_buy_mask=np.zeros_like(mask),
        can_sell_mask=mask.copy(),
        can_short_open_mask=mask.copy(),
        alive_mask=mask.copy(),
        benchmark_returns=np.zeros(dates.size, dtype=np.float32),
        close_prices=np.full((dates.size, 1), 100.0, dtype=np.float32),
    )
    reference = _CorporateActionReference(
        event_dates_by_symbol={},
        coverage_start=np.datetime64("2024-01-01", "D"),
        coverage_end=np.datetime64("2024-12-31", "D"),
        exact_cash_terms_by_symbol={},
        exact_coverage_start=np.datetime64("2024-01-01", "D"),
        exact_coverage_end=np.datetime64("2024-12-31", "D"),
        margin_short_stop_transfer_by_symbol={
            "2330": (
                np.asarray(["2024-07-03"], dtype="datetime64[D]"),
                np.asarray(["2024-07-05"], dtype="datetime64[D]"),
            )
        },
    )

    result = _apply_corporate_action_avoidance_transitions(panel, reference)

    # stop-transfer insertion is row 8, so the ordinary deadline is row 2.
    assert np.flatnonzero(result.force_short_cover_mask[:, 0]).tolist() == [2]
    assert np.flatnonzero(~result.can_short_open_mask[:, 0]).tolist() == [2, 3, 4, 5]


def test_missing_stop_transfer_uses_conservative_t2_short_fallback() -> None:
    dates = np.asarray(
        [
            "2024-06-25",
            "2024-06-26",
            "2024-06-27",
            "2024-06-28",
            "2024-07-01",
            "2024-07-02",
            "2024-07-03",
            "2024-07-04",
        ],
        dtype="datetime64[D]",
    )
    mask = np.ones((dates.size, 1), dtype=np.bool_)
    panel = PanelData(
        dates=dates,
        symbols=["0050"],
        feature_names=["x"],
        features=np.zeros((dates.size, 1, 1), dtype=np.float32),
        returns_1d=np.zeros((dates.size, 1), dtype=np.float32),
        tradable_mask=mask,
        can_buy_mask=mask.copy(),
        can_sell_mask=mask.copy(),
        can_short_open_mask=mask.copy(),
        alive_mask=mask.copy(),
        benchmark_returns=np.zeros(dates.size, dtype=np.float32),
        close_prices=np.full((dates.size, 1), 100.0, dtype=np.float32),
    )
    reference = _CorporateActionReference(
        event_dates_by_symbol={
            "0050": np.asarray(["2024-07-03"], dtype="datetime64[D]")
        },
        coverage_start=np.datetime64("2024-01-01", "D"),
        coverage_end=np.datetime64("2024-12-31", "D"),
        exact_cash_terms_by_symbol={},
        exact_coverage_start=np.datetime64("2024-01-01", "D"),
        exact_coverage_end=np.datetime64("2024-12-31", "D"),
        margin_short_stop_transfer_by_symbol={},
    )

    result = _apply_corporate_action_avoidance_transitions(panel, reference)

    # The last cum-right close is row 5.  Without issuer stop-transfer terms,
    # the safe ordinary T+2 Article 76 deadline is conservatively row 2.
    assert np.flatnonzero(result.force_short_cover_mask[:, 0]).tolist() == [2]
    assert np.flatnonzero(~result.can_short_open_mask[:, 0]).tolist() == [2, 3, 4, 5]
    assert np.flatnonzero(result.unresolved_corporate_action_mask[:, 0]).tolist() == [5]


@pytest.mark.parametrize(
    ("stop_transfer_date", "expected_extra"),
    [
        # 2024-02-06 and 2024-02-07 were the two settlement-only days
        # following the final pre-Lunar-New-Year trade on 2024-02-05.
        ("2024-02-06", 0),
        ("2024-02-07", 1),
        ("2024-02-08", 2),
        ("2024-02-15", 2),
        ("2024-02-16", 1),
        ("2024-02-19", 0),
    ],
)
def test_article76_lunar_new_year_business_day_exception(
    stop_transfer_date: str,
    expected_extra: int,
) -> None:
    sessions = np.asarray(
        [
            "2024-01-29",
            "2024-01-30",
            "2024-01-31",
            "2024-02-01",
            "2024-02-02",
            "2024-02-05",
            "2024-02-15",
            "2024-02-16",
            "2024-02-19",
        ],
        dtype="datetime64[D]",
    )

    assert (
        _article76_lunar_new_year_extra_business_days(
            sessions, np.datetime64(stop_transfer_date)
        )
        == expected_extra
    )


def test_exact_cash_article76_lunar_new_year_deadline_uses_settlement_days() -> None:
    dates = np.asarray(
        [
            "2024-01-29",
            "2024-01-30",
            "2024-01-31",
            "2024-02-01",
            "2024-02-02",
            "2024-02-05",
            "2024-02-15",
            "2024-02-16",
            "2024-02-19",
        ],
        dtype="datetime64[D]",
    )
    mask = np.ones((dates.size, 1), dtype=np.bool_)
    panel = PanelData(
        dates=dates,
        symbols=["2330"],
        feature_names=["x"],
        features=np.zeros((dates.size, 1, 1), dtype=np.float32),
        returns_1d=np.zeros((dates.size, 1), dtype=np.float32),
        tradable_mask=mask,
        can_buy_mask=mask.copy(),
        can_sell_mask=mask.copy(),
        can_short_open_mask=mask.copy(),
        alive_mask=mask.copy(),
        benchmark_returns=np.zeros(dates.size, dtype=np.float32),
        close_prices=np.full((dates.size, 1), 100.0, dtype=np.float32),
    )
    reference = _CorporateActionReference(
        event_dates_by_symbol={},
        coverage_start=np.datetime64("2024-01-01", "D"),
        coverage_end=np.datetime64("2024-12-31", "D"),
        exact_cash_terms_by_symbol={},
        exact_coverage_start=np.datetime64("2024-01-01", "D"),
        exact_coverage_end=np.datetime64("2024-12-31", "D"),
        margin_short_stop_transfer_by_symbol={
            "2330": (
                np.asarray(["2024-02-08"], dtype="datetime64[D]"),
                np.asarray(["2024-02-08"], dtype="datetime64[D]"),
            )
        },
    )

    result = _apply_corporate_action_avoidance_transitions(panel, reference)

    # Two settlement-only days count toward the six business days, so the
    # deadline is Jan-31 instead of Jan-29 under the ordinary trading-day rule.
    assert np.flatnonzero(result.force_short_cover_mask[:, 0]).tolist() == [2]
    assert np.flatnonzero(~result.can_short_open_mask[:, 0]).tolist() == [2, 3, 4, 5]


def test_unresolved_action_moves_liquidation_to_last_two_sided_close() -> None:
    dates = np.asarray(
        ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
        dtype="datetime64[D]",
    )
    mask = np.ones((4, 1), dtype=np.bool_)
    panel = PanelData(
        dates=dates,
        symbols=["2330"],
        feature_names=["x"],
        features=np.zeros((4, 1, 1), dtype=np.float32),
        returns_1d=np.zeros((4, 1), dtype=np.float32),
        tradable_mask=mask,
        can_buy_mask=np.asarray([[True], [True], [False], [True]]),
        can_sell_mask=np.asarray([[True], [True], [False], [True]]),
        alive_mask=mask.copy(),
        benchmark_returns=np.zeros(4, dtype=np.float32),
        close_prices=np.full((4, 1), 100.0, dtype=np.float32),
    )
    reference = _CorporateActionReference(
        event_dates_by_symbol={
            "2330": np.asarray(["2024-01-05"], dtype="datetime64[D]")
        },
        coverage_start=np.datetime64("2024-01-01", "D"),
        coverage_end=np.datetime64("2024-12-31", "D"),
    )

    result = _apply_corporate_action_avoidance_transitions(panel, reference)

    assert result.unresolved_corporate_action_mask[:, 0].tolist() == [
        False,
        True,
        True,
        False,
    ]


def test_dataset_exact_mode_carries_cash_entitlement_terms() -> None:
    panel = _make_panel()
    panel.cash_dividend_yield = np.zeros_like(panel.returns_1d)
    panel.cash_dividend_yield[3, 0] = 0.05
    panel.cash_dividend_payment_delay_sessions = np.zeros_like(
        panel.returns_1d, dtype=np.int64
    )
    panel.cash_dividend_payment_delay_sessions[3, 0] = 2
    panel.corporate_action_avoidance_mask[3, 0] = True
    dataset = CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates),
        lookback=2,
        execution_mode="tw_cash",
        tw_corporate_action_mode="exact",
    )

    event_sample = dataset[
        int(np.flatnonzero(dataset.valid_indices == 3)[0])
    ]
    assert event_sample["cash_dividend_yield"][0] == pytest.approx(0.05)
    assert event_sample["cash_dividend_payment_delay_sessions"][0] == 2

    avoid = CrossSectionalDataset(
        panel,
        np.arange(panel.num_dates),
        lookback=2,
        execution_mode="tw_cash",
        tw_corporate_action_mode="avoid",
    )
    avoid_sample = avoid[int(np.flatnonzero(avoid.valid_indices == 3)[0])]
    assert "cash_dividend_yield" not in avoid_sample
    assert avoid_sample["unresolved_corporate_action_mask"][0]


def test_corporate_action_reference_receipt_tamper_fails_closed(tmp_path) -> None:
    public_dir = tmp_path / "data_tw_public"
    features_dir = public_dir / "features"
    stocks_dir = public_dir / "stocks"
    features_dir.mkdir(parents=True)
    stocks_dir.mkdir()
    external_path = features_dir / "tw_public_stock_daily.parquet"
    pq.write_table(
        pa.table(
            {
                "date": pa.array([datetime(2024, 1, 2)], type=pa.timestamp("ns")),
                "symbol": ["__MARKET__"],
                "_twpub_official_traded": [1.0],
            }
        ),
        external_path,
    )
    _write_corporate_action_reference(
        public_dir,
        rows=[
            {
                "date": datetime(2024, 1, 2).date(),
                "symbol": "AAA",
                "market": "twse",
                "reference_price": 10.0,
                "opening_reference_price": None,
                "previous_close": None,
                "event_type": "除息",
                "source_url": "https://example.invalid/official",
            }
        ],
        receipt_sha256="0" * 64,
    )

    with pytest.raises(ValueError, match="SHA-256"):
        build_panel(
            stocks_dir,
            panel_backend="pyarrow",
            external_feature_path=external_path,
            external_include_features=False,
            external_include_rules=True,
        )
