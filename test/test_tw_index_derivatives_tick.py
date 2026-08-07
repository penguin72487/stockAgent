from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
import json
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl
import pytest
import torch

from stockagent.backtest.tw_execution import normalize_execution_mode
from stockagent.backtest.tw_index_derivatives_tick import (
    execute_option_tick_target,
    initialize_option_tick_state,
    option_target_weights_from_logits,
    run_tw_index_derivatives_tick_bidask_continuous,
    run_tw_index_derivatives_tick_continuous,
    txo_short_initial_margin_per_contract,
)
from stockagent.config import load_config
from stockagent.data.tw_index_derivatives_tick import (
    TICK_FEATURE_COLUMNS,
    OPTION_TICK_FEATURE_COLUMNS,
    IndexDerivativesTickDay,
    IndexDerivativesTickDataset,
    TickFeatureNormalizer,
    attach_captured_bidask_execution,
    build_index_derivatives_tick_dataset,
    build_tick_day_frame,
    load_index_derivatives_tick_dataset,
    taifex_front_month,
    taifex_option_expiry,
)
from downloader.stream_shioaji_tw_microstructure import BOOK_SCHEMA, PartWriter
from stockagent.training.index_derivatives_tick import (
    _load_tick_checkpoint,
    _tick_backtest_result,
    _tick_checkpoint_state,
    _tick_experiment_manifest,
    _run_day,
    build_tick_walk_forward_folds,
)
from stockagent.training.trainer import _save_group_checkpoint


TAIPEI = ZoneInfo("Asia/Taipei")


def test_tick_reporting_adapter_preserves_native_return_contract() -> None:
    rows = [
        {
            "date": "2026-08-05",
            "log_return": 0.01,
            "turnover": 1.5,
        },
        {
            "date": "2026-08-06",
            "log_return": -0.02,
            "turnover": 2.5,
        },
    ]

    result, dates = _tick_backtest_result(
        rows,
        execution_mode="tw_index_options_tick_long",
        symbols=2,
    )

    np.testing.assert_array_equal(
        dates,
        np.asarray(["2026-08-05", "2026-08-06"], dtype="datetime64[D]"),
    )
    np.testing.assert_allclose(result.strategy_returns, [0.01, -0.02])
    np.testing.assert_allclose(result.benchmark_returns, [0.0, 0.0])
    np.testing.assert_allclose(result.turnovers, [1.5, 2.5])
    np.testing.assert_allclose(result.weights_history, np.zeros((2, 2)))
    assert result.execution_mode == "tw_index_options_tick_long"


def _event(second: int) -> datetime:
    return datetime(2026, 8, 6, 8, 45, tzinfo=TAIPEI) + timedelta(seconds=second)


def _tx_frame() -> pl.DataFrame:
    seconds = list(range(12))
    return pl.DataFrame(
        {
            "event_ts": [_event(value) for value in seconds],
            "session": ["day"] * len(seconds),
            "delivery_month_week": ["202608"] * len(seconds),
            "price": [20_000.0 + value for value in seconds],
            "matched_quantity": [1] * len(seconds),
            "source_row_number": list(range(1, len(seconds) + 1)),
        }
    )


def _txo_frame(*, include_future: bool) -> pl.DataFrame:
    seconds = [second for second in range(12) for _ in range(2)]
    rights = [right for _ in range(12) for right in ("C", "P")]
    prices = [
        100.0 + second + (5.0 if right == "P" else 0.0)
        for second in range(12)
        for right in ("C", "P")
    ]
    quantities = [1.0] * len(seconds)
    if include_future:
        prices[seconds.index(8)] = 250.0
        quantities[seconds.index(8)] = 7.0
    return pl.DataFrame(
        {
            "event_ts": [_event(value) for value in seconds],
            "session": ["day"] * len(seconds),
            "delivery_month_week": ["202608F1"] * len(seconds),
            "strike_price": [20_000.0] * len(seconds),
            "option_right": rights,
            "price": prices,
            "matched_quantity_equivalent": quantities,
        }
    )


def _option_depth(
    *,
    call_bid: float = 99.0,
    call_ask: float = 101.0,
    put_bid: float = 119.0,
    put_ask: float = 121.0,
    volume: float = 100.0,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    bids = torch.tensor(
        [
            [call_bid - level for level in range(5)],
            [put_bid - level for level in range(5)],
        ]
    )
    asks = torch.tensor(
        [
            [call_ask + level for level in range(5)],
            [put_ask + level for level in range(5)],
        ]
    )
    volumes = torch.full((2, 5), float(volume))
    return bids, volumes, asks, volumes.clone()


def test_front_month_rolls_only_after_third_wednesday() -> None:
    assert taifex_front_month(date(2026, 7, 15)) == "202607"
    assert taifex_front_month(date(2026, 7, 16)) == "202608"
    assert taifex_front_month(date(2026, 12, 17)) == "202701"


def test_option_expiry_resolves_monthly_wednesday_and_friday_series() -> None:
    assert taifex_option_expiry("202608") == date(2026, 8, 19)
    assert taifex_option_expiry("202608W1") == date(2026, 8, 5)
    assert taifex_option_expiry("202608F1") == date(2026, 8, 7)


def test_completed_second_features_do_not_read_future_option_trades() -> None:
    kwargs = {
        "trading_date": date(2026, 8, 6),
        "decision_interval_seconds": 1,
        "rolling_window_seconds": 1,
        "warmup_seconds": 1,
    }
    baseline = build_tick_day_frame(
        _tx_frame(), _txo_frame(include_future=False), **kwargs
    )
    with_future = build_tick_day_frame(
        _tx_frame(), _txo_frame(include_future=True), **kwargs
    )
    cutoff = _event(8)
    columns = ["event_ts", *TICK_FEATURE_COLUMNS]
    assert (
        baseline.filter(pl.col("event_ts") < cutoff)
        .select(columns)
        .equals(with_future.filter(pl.col("event_ts") < cutoff).select(columns))
    )
    assert bool(
        (
            with_future["execution_event_ts"].cast(pl.Int64)
            > with_future["event_ts"].cast(pl.Int64)
        ).all()
    )
    execution_ns = with_future["execution_event_ts"].cast(pl.Int64).to_numpy()
    assert np.all(execution_ns[1:] > execution_ns[:-1])
    option_rows = with_future.filter(pl.col("option_executable"))
    for name in ("call_execution_event_ts", "put_execution_event_ts"):
        option_execution_ns = option_rows[name].cast(pl.Int64).to_numpy()
        decision_ns = option_rows["event_ts"].cast(pl.Int64).to_numpy()
        assert np.all(option_execution_ns > decision_ns)
        assert np.all(option_execution_ns[1:] > option_execution_ns[:-1])
    assert bool(with_future["is_terminal"][-1])
    assert with_future["interval_log_return"][-1] == 0.0


def _captured_book_rows(decisions: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    sequence = 0
    for row_index, event_ts in enumerate(decisions["event_ts"]):
        receive_ns = int(event_ts.timestamp() * 1_000_000_000) + 1_300_000_000
        for code, best_bid, best_ask in (
            ("TXFQ", 20_000.0 + row_index, 20_002.0 + row_index),
            ("CALLQ", 100.0 + row_index, 102.0 + row_index),
            ("PUTQ", 105.0 + row_index, 107.0 + row_index),
        ):
            sequence += 1
            payload: dict[str, object] = {
                "event_seq": sequence,
                "worker_index": 0,
                "exchange": "TAIFEX",
                "code": code,
                "trade_date": event_ts.date(),
                "exchange_ts_ns": receive_ns - 1_000_000,
                "receive_ts_ns": receive_ns,
                "receive_monotonic_ns": receive_ns,
                "suspend": False,
                "simtrade": False,
                "intraday_odd": False,
            }
            for level in range(1, 6):
                payload[f"bid_price_{level}"] = best_bid - level + 1
                payload[f"bid_volume_{level}"] = 10
                payload[f"ask_price_{level}"] = best_ask + level - 1
                payload[f"ask_volume_{level}"] = 10
            rows.append(payload)
    return pl.from_dicts(rows)


def test_captured_bidask_attaches_first_latency_eligible_book_without_fallback() -> (
    None
):
    decisions = build_tick_day_frame(
        _tx_frame(),
        _txo_frame(include_future=False),
        trading_date=date(2026, 8, 6),
        decision_interval_seconds=1,
        rolling_window_seconds=1,
        warmup_seconds=1,
    )
    metadata = [
        {
            "security_type": "FUT",
            "code": "TXFQ",
            "delivery_month": "202608",
        },
        {
            "security_type": "OPT",
            "code": "CALLQ",
            "delivery_date": "2026-08-07",
            "strike_price": 20_000.0,
            "option_right": "C",
        },
        {
            "security_type": "OPT",
            "code": "PUTQ",
            "delivery_date": "2026-08-07",
            "strike_price": 20_000.0,
            "option_right": "P",
        },
    ]
    books = _captured_book_rows(decisions)
    attached = attach_captured_bidask_execution(
        decisions,
        books,
        metadata,
        execution_latency_ms=250.0,
        execution_max_wait_ms=500.0,
        max_transport_delay_ms=2_000.0,
    )
    assert attached["tx_book_valid"].all()
    assert attached["option_executable"].all()
    decision_ns = np.asarray(
        [int(value.timestamp() * 1_000_000_000) for value in attached["event_ts"]]
    )
    assert np.all(
        attached["call_execution_receive_ts_ns"].to_numpy()
        >= decision_ns + 1_250_000_000
    )
    assert attached["call_ask_price_1"][0] == pytest.approx(102.0)
    with pytest.raises(ValueError, match="no events for put"):
        attach_captured_bidask_execution(
            decisions,
            books.filter(pl.col("code") != "PUTQ"),
            metadata,
        )


def test_bidask_dataset_build_and_load_preserve_depth_receipts(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    output_root = tmp_path / "dataset"
    capture_root = tmp_path / "capture"
    trade_date = date(2026, 8, 6)
    tx_path = raw_root / "tx" / f"trading_date={trade_date}" / "transactions.parquet"
    txo_path = raw_root / "txo" / f"trading_date={trade_date}" / "transactions.parquet"
    tx_path.parent.mkdir(parents=True)
    txo_path.parent.mkdir(parents=True)
    _tx_frame().write_parquet(tx_path)
    _txo_frame(include_future=False).write_parquet(txo_path)
    (raw_root / "manifest.json").write_text(
        json.dumps({"status": "complete", "trading_dates": [str(trade_date)]}),
        encoding="utf-8",
    )
    decisions = build_tick_day_frame(
        _tx_frame(),
        _txo_frame(include_future=False),
        trading_date=trade_date,
        decision_interval_seconds=1,
        rolling_window_seconds=1,
        warmup_seconds=1,
    )
    writer = PartWriter(
        capture_root,
        "book_events",
        BOOK_SCHEMA,
        worker_index=0,
        capture_id="bidask_test",
        flush_rows=1,
        flush_seconds=60.0,
    )
    for row in _captured_book_rows(decisions).iter_rows(named=True):
        writer.append(row)
    writer.maybe_flush(force=True)
    metadata = [
        {"security_type": "FUT", "code": "TXFQ", "delivery_month": "202608"},
        {
            "security_type": "OPT",
            "code": "CALLQ",
            "delivery_date": "2026-08-07",
            "strike_price": 20_000.0,
            "option_right": "C",
        },
        {
            "security_type": "OPT",
            "code": "PUTQ",
            "delivery_date": "2026-08-07",
            "strike_price": 20_000.0,
            "option_right": "P",
        },
    ]
    manifest_path = (
        capture_root / "manifests" / f"trade_date={trade_date}" / "worker=00.json"
    )
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 3,
                "source": "shioaji_taifex_tick_bidask_v1",
                "status": "complete",
                "capture_id": "bidask_test",
                "worker_index": 0,
                "dropped_events": 0,
                "book_parts": writer.total_parts,
                "contract_metadata": metadata,
            }
        ),
        encoding="utf-8",
    )
    manifest = build_index_derivatives_tick_dataset(
        raw_root,
        output_root,
        execution_price_source="shioaji_bidask",
        bidask_capture_root=capture_root,
        execution_latency_ms=250.0,
        execution_max_wait_ms=500.0,
        max_transport_delay_ms=2_000.0,
        decision_interval_seconds=1,
        rolling_window_seconds=1,
        warmup_seconds=1,
    )
    assert manifest["execution_price_source"] == "shioaji_bidask"
    assert manifest["summary"]["dates"] == 1
    assert manifest["partitions"][0]["capture_receipt"]["capture_id"] == "bidask_test"
    dataset = load_index_derivatives_tick_dataset(output_root)
    normalizer = dataset.fit_normalizer([0], option_mode=True)
    day = dataset.load_day(0, normalizer=normalizer, option_mode=True)
    assert day.option_execution_bid_prices is not None
    assert day.option_execution_ask_volumes is not None
    assert day.option_execution_bid_prices.shape[1:] == (2, 5)


def test_tick_backtest_carries_state_and_forces_final_flat() -> None:
    exposure = torch.tensor([0.5, -0.5, 0.2], requires_grad=True)
    result = run_tw_index_derivatives_tick_continuous(
        exposure,
        torch.log(torch.tensor([1.01, 0.99, 1.0])),
        torch.tensor([100.0, 100.0, 100.0]),
        torch.ones(3, dtype=torch.bool),
        fee_rate_per_side=0.001,
        slippage_points_per_side=0.0,
        force_final_flat=True,
    )
    torch.testing.assert_close(result.executed_exposure, torch.tensor([0.5, -0.5, 0.0]))
    torch.testing.assert_close(result.turnovers, torch.tensor([0.5, 1.0, 0.5]))
    torch.testing.assert_close(
        result.strategy_returns,
        torch.tensor([0.0045, 0.0040, -0.0005]),
        atol=1e-7,
        rtol=1e-5,
    )
    assert result.final_exposure.item() == 0.0
    (-torch.log1p(result.strategy_returns).sum()).backward()
    assert exposure.grad is not None
    assert torch.isfinite(exposure.grad).all()


def test_tick_backtest_keeps_position_when_next_trade_is_unavailable() -> None:
    result = run_tw_index_derivatives_tick_continuous(
        torch.tensor([0.5, -0.5]),
        torch.zeros(2),
        torch.tensor([100.0, float("nan")]),
        torch.ones(2, dtype=torch.bool),
        fee_rate_per_side=0.0,
        slippage_points_per_side=0.0,
    )
    torch.testing.assert_close(result.executed_exposure, torch.tensor([0.5, 0.5]))
    torch.testing.assert_close(result.turnovers, torch.tensor([0.5, 0.0]))


def test_futures_bidask_executor_buys_ask_sells_bid_and_caps_depth() -> None:
    exposure = torch.tensor([1.0, 0.0], requires_grad=True)
    bid_prices = torch.tensor(
        [[99.0, 98.0, 97.0, 96.0, 95.0], [109.0, 108.0, 107.0, 106.0, 105.0]]
    )
    ask_prices = torch.tensor(
        [[101.0, 102.0, 103.0, 104.0, 105.0], [111.0, 112.0, 113.0, 114.0, 115.0]]
    )
    volumes = torch.tensor([[0.1] * 5, [100.0] * 5])
    result = run_tw_index_derivatives_tick_bidask_continuous(
        exposure,
        bid_prices,
        volumes,
        ask_prices,
        volumes,
        torch.stack([bid_prices[1], bid_prices[1]]),
        torch.stack([ask_prices[1], ask_prices[1]]),
        torch.ones(2, dtype=torch.bool),
        initial_equity=10_000.0,
        contract_multiplier=1.0,
        fee_rate_per_side=0.0,
        force_final_flat=True,
    )
    # Only 0.5 contracts exist across the first five ask levels, so a full
    # unit exposure cannot be fabricated.
    assert result.executed_exposure[0].item() == pytest.approx(0.005, rel=1e-5)
    assert result.cost_returns[0].item() > 0.0
    assert result.final_exposure.item() == pytest.approx(0.0, abs=1e-7)
    (-torch.log1p(result.strategy_returns).sum()).backward()
    assert exposure.grad is not None


def test_option_bidask_executor_sweeps_ask_and_reports_unfilled_quantity() -> None:
    initial = initialize_option_tick_state(initial_equity=100_000.0, device="cpu")
    bids, bid_volumes, asks, ask_volumes = _option_depth(volume=0.1)
    step = execute_option_tick_target(
        initial,
        target_weights=torch.tensor([0.8, 0.0]),
        tradable_mask=torch.ones(2, dtype=torch.bool),
        bid_prices=bids,
        bid_volumes=bid_volumes,
        ask_prices=asks,
        ask_volumes=ask_volumes,
        next_bid_prices=bids,
        next_ask_prices=asks,
        underlying_price=20_000.0,
        strike_price=20_000.0,
        allow_option_short=False,
        contract_multiplier=50.0,
        fixed_fee_per_contract_per_side_twd=0.0,
        transaction_tax_rate=0.0,
        slippage_points_per_side=0.0,
    )
    assert step.state.contracts[0].item() == pytest.approx(0.5)
    assert step.unfilled_contracts.item() > 0.0
    # Five levels 101..105 at 0.1 contracts each are paid, not the midpoint.
    assert step.premium_cash_flow.item() == pytest.approx(
        -0.1 * sum(range(101, 106)) * 50.0
    )


def test_option_action_allocator_reuses_long_and_short_cash_policy() -> None:
    logits = torch.tensor([[1.0, -1.0], [-1.0, 1.0]])
    mask = torch.ones_like(logits, dtype=torch.bool)
    long_weights = option_target_weights_from_logits(
        logits,
        mask,
        allow_option_short=False,
        maximum_capital_fraction=0.8,
    )
    short_weights = option_target_weights_from_logits(
        logits,
        mask,
        allow_option_short=True,
        maximum_capital_fraction=0.8,
    )
    assert bool((long_weights >= 0.0).all())
    assert bool((short_weights[0] > 0.0).any())
    assert bool((short_weights[0] < 0.0).any())
    assert bool((short_weights[1] > 0.0).any())
    assert bool((short_weights[1] < 0.0).any())
    assert bool((long_weights.sum(dim=1) <= 0.8).all())
    assert bool((short_weights.abs().sum(dim=1) <= 0.8).all())


def test_option_naked_call_and_put_margin_uses_official_single_leg_formula() -> None:
    margin = txo_short_initial_margin_per_contract(
        torch.tensor([100.0, 120.0]),
        underlying_price=20_000.0,
        strike_price=20_100.0,
    )
    torch.testing.assert_close(margin, torch.tensor([169_000.0, 175_000.0]))


def test_option_executor_enforces_permission_tax_margin_and_forced_flat() -> None:
    initial = initialize_option_tick_state(initial_equity=1_000_000.0, device="cpu")
    bids, bid_volumes, asks, ask_volumes = _option_depth()
    long_step = execute_option_tick_target(
        initial,
        target_weights=torch.tensor([-0.4, 0.4]),
        tradable_mask=torch.ones(2, dtype=torch.bool),
        bid_prices=bids,
        bid_volumes=bid_volumes,
        ask_prices=asks,
        ask_volumes=ask_volumes,
        next_bid_prices=bids,
        next_ask_prices=asks,
        underlying_price=20_000.0,
        strike_price=20_000.0,
        allow_option_short=False,
        fixed_fee_per_contract_per_side_twd=0.0,
        slippage_points_per_side=0.0,
    )
    assert long_step.state.contracts[0].item() == pytest.approx(0.0)
    assert long_step.state.contracts[1].item() > 0.0
    assert long_step.transaction_tax.item() > 0.0

    short_target = torch.tensor([-0.4, -0.4], requires_grad=True)
    short_step = execute_option_tick_target(
        initial,
        target_weights=short_target,
        tradable_mask=torch.ones(2, dtype=torch.bool),
        bid_prices=bids,
        bid_volumes=bid_volumes,
        ask_prices=asks,
        ask_volumes=ask_volumes,
        next_bid_prices=bids - 2.0,
        next_ask_prices=asks - 2.0,
        underlying_price=20_000.0,
        strike_price=20_000.0,
        allow_option_short=True,
        fixed_fee_per_contract_per_side_twd=0.0,
        slippage_points_per_side=0.0,
    )
    assert bool((short_step.state.contracts < 0.0).all())
    assert short_step.initial_margin_required.item() <= 800_000.0 + 1.0
    assert short_step.transaction_tax.item() > 0.0
    (-torch.log1p(short_step.net_return)).backward()
    assert short_target.grad is not None
    assert torch.isfinite(short_target.grad).all()

    close_step = execute_option_tick_target(
        short_step.state.detached(),
        target_weights=torch.ones(2),
        tradable_mask=torch.ones(2, dtype=torch.bool),
        bid_prices=bids - 2.0,
        bid_volumes=bid_volumes,
        ask_prices=asks - 2.0,
        ask_volumes=ask_volumes,
        next_bid_prices=bids - 2.0,
        next_ask_prices=asks - 2.0,
        underlying_price=20_000.0,
        strike_price=20_000.0,
        allow_option_short=True,
        fixed_fee_per_contract_per_side_twd=0.0,
        slippage_points_per_side=0.0,
        force_flat=True,
    )
    torch.testing.assert_close(close_step.state.contracts, torch.zeros(2))
    assert close_step.transaction_tax.item() > 0.0


def test_option_ruin_is_absorbing() -> None:
    initial = initialize_option_tick_state(initial_equity=1_000_000.0, device="cpu")
    bids, bid_volumes, asks, ask_volumes = _option_depth(
        put_bid=99.0,
        put_ask=101.0,
    )
    next_bids = bids.clone()
    next_asks = asks.clone()
    next_bids[0] = torch.tensor([9_998.0, 9_997.0, 9_996.0, 9_995.0, 9_994.0])
    next_asks[0] = torch.tensor([10_000.0, 10_001.0, 10_002.0, 10_003.0, 10_004.0])
    ruined = execute_option_tick_target(
        initial,
        target_weights=torch.tensor([-0.8, 0.0]),
        tradable_mask=torch.ones(2, dtype=torch.bool),
        bid_prices=bids,
        bid_volumes=bid_volumes,
        ask_prices=asks,
        ask_volumes=ask_volumes,
        next_bid_prices=next_bids,
        next_ask_prices=next_asks,
        underlying_price=20_000.0,
        strike_price=20_000.0,
        allow_option_short=True,
        fixed_fee_per_contract_per_side_twd=0.0,
        transaction_tax_rate=0.0,
        slippage_points_per_side=0.0,
    )
    assert not ruined.state.alive.item()
    torch.testing.assert_close(ruined.state.contracts, torch.zeros(2))
    still_ruined = execute_option_tick_target(
        ruined.state,
        target_weights=torch.tensor([0.8, 0.0]),
        tradable_mask=torch.ones(2, dtype=torch.bool),
        bid_prices=next_bids,
        bid_volumes=bid_volumes,
        ask_prices=next_asks,
        ask_volumes=ask_volumes,
        next_bid_prices=next_bids,
        next_ask_prices=next_asks,
        underlying_price=20_000.0,
        strike_price=20_000.0,
        allow_option_short=True,
        fixed_fee_per_contract_per_side_twd=0.0,
        transaction_tax_rate=0.0,
        slippage_points_per_side=0.0,
    )
    assert not still_ruined.state.alive.item()
    torch.testing.assert_close(still_ruined.state.contracts, torch.zeros(2))


def test_tick_walk_forward_uses_only_past_dates_for_training() -> None:
    folds = build_tick_walk_forward_folds(
        35, min_train_days=20, val_days=5, test_days=5
    )
    assert len(folds) == 2
    np.testing.assert_array_equal(folds[0].train_indices, np.arange(20))
    np.testing.assert_array_equal(folds[0].val_indices, np.arange(20, 25))
    np.testing.assert_array_equal(folds[0].test_indices, np.arange(25, 30))
    np.testing.assert_array_equal(folds[1].train_indices, np.arange(25))


@pytest.mark.parametrize(
    ("config_name", "option_mode"),
    [
        ("tw_index_derivatives_tick.yaml", False),
        ("tw_index_options_tick_long.yaml", True),
        ("tw_index_options_tick_short.yaml", True),
    ],
)
def test_shared_tick_runner_consumes_bidask_depth(
    config_name: str,
    option_mode: bool,
) -> None:
    root = Path(__file__).resolve().parents[1]
    config = load_config(root / "configs/markets" / config_name)
    config.training.lookback = 2
    rows = 4
    symbols = 2 if option_mode else 1
    feature_count = (
        len(OPTION_TICK_FEATURE_COLUMNS) if option_mode else len(TICK_FEATURE_COLUMNS)
    )
    bid = np.broadcast_to(
        np.asarray([100.0, 99.0, 98.0, 97.0, 96.0], dtype=np.float32),
        (rows, symbols, 5),
    ).copy()
    ask = np.broadcast_to(
        np.asarray([102.0, 103.0, 104.0, 105.0, 106.0], dtype=np.float32),
        (rows, symbols, 5),
    ).copy()
    volume = np.full((rows, symbols, 5), 100.0, dtype=np.float32)
    day = IndexDerivativesTickDay(
        trading_date=np.datetime64("2026-08-06"),
        event_ts=np.arange(rows),
        execution_event_ts=np.arange(rows) + 1,
        tx_contract_month="202608",
        features=np.zeros((rows, symbols, feature_count), dtype=np.float32),
        interval_log_returns=np.zeros(rows, dtype=np.float32),
        execution_prices=np.full(rows, 20_000.0, dtype=np.float32),
        tradable_mask=np.ones((rows, symbols), dtype=bool)
        if option_mode
        else np.ones(rows, dtype=bool),
        terminal_mask=np.asarray([False, False, False, True]),
        execution_bid_prices=None if option_mode else bid[:, 0],
        execution_bid_volumes=None if option_mode else volume[:, 0],
        execution_ask_prices=None if option_mode else ask[:, 0],
        execution_ask_volumes=None if option_mode else volume[:, 0],
        next_execution_bid_prices=None if option_mode else bid[:, 0],
        next_execution_ask_prices=None if option_mode else ask[:, 0],
        option_series="202608F1" if option_mode else None,
        option_expiry_date=np.datetime64("2026-08-07") if option_mode else None,
        option_strike=20_000.0 if option_mode else None,
        underlying_execution_prices=(
            np.full(rows, 20_000.0, dtype=np.float32) if option_mode else None
        ),
        option_execution_bid_prices=bid if option_mode else None,
        option_execution_bid_volumes=volume if option_mode else None,
        option_execution_ask_prices=ask if option_mode else None,
        option_execution_ask_volumes=volume if option_mode else None,
        option_next_execution_bid_prices=bid if option_mode else None,
        option_next_execution_ask_prices=ask if option_mode else None,
    )

    class FixedModel(torch.nn.Module):
        def forward(
            self,
            features: torch.Tensor,
            mask: torch.Tensor,
            *,
            return_aux: bool,
        ) -> torch.Tensor:
            del return_aux
            return torch.where(
                mask,
                torch.ones_like(mask, dtype=features.dtype) * 0.25,
                torch.zeros_like(mask, dtype=features.dtype),
            )

    result = _run_day(
        model=FixedModel(),
        day=day,
        config=config,
        device=torch.device("cpu"),
        amp_dtype=None,
        batch_size=2,
        optimizer=None,
        scheduler=None,
    )
    assert result["decisions"] == pytest.approx(3.0)
    assert np.isfinite(float(result["net_return"]))


def test_tick_mode_config_and_alias_are_registered() -> None:
    root = Path(__file__).resolve().parents[1]
    config = load_config(root / "configs/markets/tw_index_derivatives_tick.yaml")
    assert normalize_execution_mode("台指期選逐筆") == "tw_index_derivatives_tick"
    assert config.trading.execution_mode == "tw_index_derivatives_tick"
    assert config.trading.frequency == "tick"
    assert config.training.model_name == "cross_sectional_index_futures"
    assert config.data.index_derivatives_tick_min_train_days == 20
    assert config.training.transformer_base_portfolio.attention_mode == "temporal_only"
    assert config.training.multitask_loss.return_rank_ic_weight == pytest.approx(0.0)
    long_config = load_config(root / "configs/markets/tw_index_options_tick_long.yaml")
    short_config = load_config(
        root / "configs/markets/tw_index_options_tick_short.yaml"
    )
    assert normalize_execution_mode("台指選逐筆買方") == "tw_index_options_tick_long"
    assert normalize_execution_mode("台指選逐筆可空賣") == "tw_index_options_tick_short"
    assert long_config.trading.long_only
    assert not short_config.trading.long_only
    assert long_config.training.model_name == "transformer_base_portfolio"
    assert short_config.training.model_name == "transformer_base_portfolio"
    assert long_config.training.transformer_base_portfolio.portfolio_mode == "long_only"
    assert (
        short_config.training.transformer_base_portfolio.portfolio_mode == "long_short"
    )


def test_tick_checkpoint_uses_canonical_envelope_and_namespaced_state(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    config = load_config(root / "configs/markets/tw_index_derivatives_tick.yaml")
    config.environment.device = "cpu"
    dataset = IndexDerivativesTickDataset(
        root=tmp_path,
        manifest={"source_manifest_sha256": "dataset-sha256"},
        dates=np.asarray(["2026-01-02", "2026-01-03"], dtype="datetime64[D]"),
        partition_paths=(),
        verify_partition_sha256=False,
    )
    normalizer = TickFeatureNormalizer(
        mean=np.zeros(len(TICK_FEATURE_COLUMNS), dtype=np.float32),
        scale=np.ones(len(TICK_FEATURE_COLUMNS), dtype=np.float32),
        counts=np.ones(len(TICK_FEATURE_COLUMNS), dtype=np.int64),
    )
    model = torch.nn.Linear(3, 1)
    original = {
        name: value.detach().clone() for name, value in model.state_dict().items()
    }
    optimizer = torch.optim.AdamW(model.parameters(), lr=1.0e-3)
    scaler = torch.amp.GradScaler("cuda", enabled=False)
    checkpoint_path = tmp_path / "train_20260102-20260103/checkpoint_last.pt"

    _save_group_checkpoint(
        checkpoint_path,
        train_years=[2026],
        epoch=2,
        model=model,
        optimizer=optimizer,
        scaler=scaler,
        experiment_manifest=_tick_experiment_manifest(dataset, config),
        effective_train_batch_size=int(config.training.batch_size_train),
        extra_payload={
            "best_val_loss": 0.25,
            "fold_id": 1,
            "tw_index_derivatives_tick_state": _tick_checkpoint_state(
                dataset=dataset,
                normalizer=normalizer,
                config=config,
            ),
        },
        no_improve_epochs=1,
        early_stop_patience=3,
        check_finite=True,
    )
    payload = torch.load(checkpoint_path, map_location="cpu", weights_only=True)
    assert "model_state_dict" in payload
    assert "optimizer_state_dict" in payload
    assert "scaler_state_dict" in payload
    assert "rng_state" in payload
    assert "tw_index_derivatives_tick_state" in payload
    assert "normalizer_mean" not in payload

    with torch.no_grad():
        for parameter in model.parameters():
            parameter.zero_()
    restored = _load_tick_checkpoint(
        checkpoint_path,
        model=model,
        optimizer=optimizer,
        scaler=scaler,
        scheduler=None,
        dataset=dataset,
        config=config,
    )
    for name, value in model.state_dict().items():
        torch.testing.assert_close(value, original[name])
    np.testing.assert_allclose(restored["normalizer_mean"], normalizer.mean)
    assert restored["epoch"] == 2
    assert restored["best_val_loss"] == pytest.approx(0.25)
