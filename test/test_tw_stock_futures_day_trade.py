from __future__ import annotations

from datetime import date
import hashlib
import json
import warnings
from pathlib import Path

import numpy as np
import polars as pl
import pytest
import torch
import yaml

from scripts.run_ablation_experiments import _build_configs, _experiment_rows
from scripts.build_tw_stock_futures_0900_entries import (
    _first_strictly_later_entries,
)
from stockagent.backtest.simulator import run_backtest_torch
from stockagent.backtest.tw_execution import normalize_execution_mode
from stockagent.config import load_config
from stockagent.data.panel import PanelData
from stockagent.data.tw_futures_portfolio_daily import (
    TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION,
)
from stockagent.data.tw_stock_futures_day_trade import (
    ENTRY_PRICE_SOURCE_DAILY_SESSION_OPEN_0900_PROXY,
    _load_0900_entries,
    _with_0900_execution_costs,
    _with_execution_costs,
    _with_integer_execution_contract,
    attach_stock_futures_day_trade_daily,
    select_causal_front_stock_futures,
    select_causal_front_stock_futures_candidates,
)
from stockagent.backtest.tw_stock_futures_day_trade import (
    run_tw_stock_futures_day_trade_integer_torch,
)
from stockagent.models.transformer_base_portfolio import (
    action_channels_for_execution_mode,
)
from stockagent.models.normalization import masked_l1_projection_weights
from stockagent.training.checkpoint_contract import _trading_checkpoint_contract


def _candidate(
    *,
    day: date,
    product: str,
    underlying: str = "2330",
    prior_volume: int,
    current_volume: int,
    prior_open_interest: int = 100,
    tenor_date: date = date(2026, 9, 16),
    open_price: float = 100.0,
    close_price: float = 110.0,
    known_previous: bool = True,
) -> dict[str, object]:
    return {
        "date": day,
        "product": product,
        "physical_contract": f"{product}:202609",
        "underlying_symbol": underlying,
        "asset_class": "stock_future",
        "tenor_rank": 1,
        "tenor_sort_date": tenor_date,
        "open": open_price,
        "close": close_price,
        "volume": current_volume,
        "previous_volume": prior_volume,
        "previous_open_interest": prior_open_interest,
        "source_row_observed": True,
        "same_contract_as_previous_session": known_previous,
        "executable": True,
        "contract_multiplier": 2_000.0,
        "fixed_fee_research_supported": True,
    }


def test_causal_front_selection_is_unique_and_ignores_current_volume() -> None:
    day = date(2026, 8, 10)
    frame = pl.DataFrame(
        [
            _candidate(
                day=day,
                product="AAA",
                prior_volume=10,
                current_volume=10_000,
            ),
            _candidate(
                day=day,
                product="BBB",
                prior_volume=20,
                current_volume=1,
            ),
            _candidate(
                day=day,
                product="CCC",
                underlying="2317",
                prior_volume=100,
                current_volume=100,
                known_previous=False,
            ),
        ]
    )
    selected = select_causal_front_stock_futures(frame)
    assert selected.height == 1
    assert selected.row(0, named=True)["product"] == "BBB"
    assert (
        selected.group_by("date", "underlying_symbol").len()["len"].max()
        == 1
    )


def test_integer_candidate_pool_keeps_standard_and_mini_nearby_contracts() -> None:
    day = date(2026, 8, 10)
    standard_low_liquidity = _candidate(
        day=day,
        product="CDF",
        prior_volume=10,
        current_volume=10_000,
    )
    standard_high_liquidity = _candidate(
        day=day,
        product="CDF2",
        prior_volume=20,
        current_volume=1,
    )
    mini = _candidate(
        day=day,
        product="QF",
        prior_volume=100,
        current_volume=100,
    )
    mini["contract_multiplier"] = 100.0
    selected = select_causal_front_stock_futures_candidates(
        pl.DataFrame([standard_low_liquidity, standard_high_liquidity, mini])
    )

    assert selected.height == 2
    assert selected["product"].to_list() == ["CDF2", "QF"]
    assert selected["candidate_slot"].to_list() == [0, 1]
    assert selected["contract_multiplier"].to_list() == [2_000.0, 100.0]


def test_integer_execution_contract_embeds_exact_fee_tax_reserve_and_capacity() -> None:
    selected = select_causal_front_stock_futures_candidates(
        pl.DataFrame(
            [
                _candidate(
                    day=date(2026, 8, 10),
                    product="CDF",
                    prior_volume=5,
                    current_volume=10,
                    open_price=100.0,
                    close_price=110.0,
                ),
                {
                    **_candidate(
                        day=date(2026, 8, 10),
                        product="QF",
                        prior_volume=9,
                        current_volume=10,
                        open_price=100.0,
                        close_price=110.0,
                    ),
                    "contract_multiplier": 100.0,
                },
            ]
        )
    )
    costed = _with_integer_execution_contract(
        selected,
        fee_per_contract_per_side_twd=40.0,
        max_volume_participation=0.5,
    ).sort("candidate_slot")

    # 2026 tax is 2e-5 per side and rounded half-up per contract.
    assert costed["reserved_open_cash_twd"].to_list() == pytest.approx(
        [200_088.0, 10_080.0]
    )
    assert costed["maximum_contracts"].to_list() == [2.0, 4.0]
    assert costed["long_net_simple_return"][0] == pytest.approx(
        (20_000.0 - 88.0) / 200_000.0
    )
    assert costed["short_net_simple_return"][1] == pytest.approx(
        (-1_000.0 - 80.0) / 10_000.0
    )


def _integer_execution_tensor(
    rows: int,
    symbols: int,
) -> torch.Tensor:
    execution = torch.full((rows, symbols, 2, 5), float("nan"))
    # Standard: TWD 200k notional, TWD 80 reserved/actual cost, +10% gross.
    execution[:, :, 0, :] = torch.tensor(
        [0.0996, -0.1004, 200_000.0, 200_080.0, 100.0]
    )
    # Mini: TWD 10k notional, same fixed cost, +10% gross.
    execution[:, :, 1, :] = torch.tensor(
        [0.092, -0.108, 10_000.0, 10_080.0, 100.0]
    )
    return execution


def test_integer_ledger_jointly_packs_standard_and_mini_and_updates_equity() -> None:
    weights = torch.tensor([[0.25], [0.25]], requires_grad=True)
    execution = _integer_execution_tensor(2, 1)
    result = run_tw_stock_futures_day_trade_integer_torch(
        weights,
        execution,
        initial_capital=1_000_000.0,
    )

    assert result.contract_quantities_history is not None
    assert result.contract_quantities_history.dtype == torch.int64
    torch.testing.assert_close(
        result.contract_quantities_history[0], torch.tensor([[1, 4]])
    )
    first_pnl = 1 * (0.0996 * 200_000.0) + 4 * (0.092 * 10_000.0)
    first_equity = 1_000_000.0 + first_pnl
    torch.testing.assert_close(
        result.equity_scale_history[0],
        torch.tensor(first_equity / 1_000_000.0),
    )
    torch.testing.assert_close(
        result.strategy_returns[0],
        torch.log(torch.tensor(first_equity / 1_000_000.0)),
    )
    # The second day's quantity is sized from the first day's actual closing
    # equity, proving that capital recurrence is not a fixed 10M denominator.
    assert int(result.contract_quantities_history[1].abs().sum()) >= 5
    result.strategy_returns.sum().backward()
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()


def test_integer_ledger_never_reallocates_failed_name_or_borrows_for_one_contract() -> None:
    weights = torch.tensor([[0.50, 0.50]], requires_grad=True)
    execution = _integer_execution_tensor(1, 2)
    execution[0, 0] = float("nan")
    # The second stock keeps only its own 50% cash sleeve.  It may not absorb
    # the failed first stock's half of capital.
    result = run_tw_stock_futures_day_trade_integer_torch(
        weights,
        execution,
        initial_capital=100_000.0,
    )
    assert result.contract_quantities_history is not None
    torch.testing.assert_close(
        result.contract_quantities_history[0],
        torch.tensor([[0, 0], [0, 4]]),
    )
    torch.testing.assert_close(
        result.weights_history,
        torch.tensor([[0.0, 0.4]]),
    )

    too_small = run_tw_stock_futures_day_trade_integer_torch(
        torch.tensor([[1.0]]),
        _integer_execution_tensor(1, 1),
        initial_capital=10_000.0,
    )
    assert torch.equal(
        too_small.contract_quantities_history,
        torch.zeros((1, 1, 2), dtype=torch.int64),
    )
    assert torch.equal(too_small.strategy_returns, torch.zeros(1))


def test_canonical_backtest_routes_integer_mode_and_carries_equity_scale() -> None:
    weights = torch.tensor([[0.25], [0.25]], requires_grad=True)
    result = run_backtest_torch(
        weights,
        torch.zeros((2, 1)),
        torch.ones((2, 1), dtype=torch.bool),
        torch.zeros(2),
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        portfolio_activation="pre_normalized",
        execution_mode="tw_stock_futures_day_trade_0900_integer",
        can_buy_mask=torch.ones((2, 1), dtype=torch.bool),
        can_sell_mask=torch.ones((2, 1), dtype=torch.bool),
        overnight_returns=_integer_execution_tensor(2, 1),
        day_trade_execution_initial_capital=1_000_000.0,
    )
    assert result.execution_mode == "tw_stock_futures_day_trade_0900_integer"
    assert result.equity_scale_history is not None
    assert result.final_equity_scale is not None
    assert float(result.final_equity_scale.detach()) > 1.0
    torch.testing.assert_close(result.weights_history[0], torch.tensor([0.24]))
    result.strategy_returns.sum().backward()
    assert weights.grad is not None

def test_round_trip_cost_uses_historical_tax_and_whole_twd_rounding() -> None:
    selected = pl.DataFrame(
        [
            _candidate(
                day=date(2013, 3, 29),
                product="AAA",
                prior_volume=10,
                current_volume=10,
            ),
            _candidate(
                day=date(2013, 4, 1),
                product="AAA",
                prior_volume=10,
                current_volume=10,
            ),
        ]
    )
    costed = _with_execution_costs(
        selected,
        fee_per_contract_per_side_twd=40.0,
    ).sort("date")

    # Before 2013-04-01: taxes are round(100*2000*4e-5)=8 and
    # round(110*2000*4e-5)=9.  There are also two TWD 40 commissions.
    assert costed["round_trip_cost_rate_per_open_notional"][0] == pytest.approx(
        97.0 / 200_000.0
    )
    # From 2013-04-01 the tax rate is 2e-5: 4 TWD at entry and exit.
    assert costed["round_trip_cost_rate_per_open_notional"][1] == pytest.approx(
        88.0 / 200_000.0
    )


def test_0900_daily_proxy_uses_daily_source_without_sidecar(tmp_path: Path) -> None:
    dates = [date(2026, 8, 10), date(2026, 8, 11)]
    data_path = tmp_path / "continuous_daily.parquet"
    pl.DataFrame(
        [
            _candidate(
                day=day,
                product="AAA",
                prior_volume=10,
                current_volume=10,
                open_price=100.0,
                close_price=110.0,
            )
            for day in dates
        ]
    ).write_parquet(data_path)
    digest = hashlib.sha256(data_path.read_bytes()).hexdigest()
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "contract_version": int(
                    TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION
                ),
                "outputs": {"continuous_daily": {"sha256": digest}},
            }
        ),
        encoding="utf-8",
    )
    shape = (2, 1)
    panel = PanelData(
        dates=np.asarray(dates, dtype="datetime64[D]"),
        symbols=["2330"],
        feature_names=["f0"],
        features=np.zeros((*shape, 1), dtype=np.float32),
        returns_1d=np.zeros(shape, dtype=np.float32),
        tradable_mask=np.ones(shape, dtype=bool),
        alive_mask=np.ones(shape, dtype=bool),
        benchmark_returns=np.zeros(2, dtype=np.float32),
        close_prices=np.ones(shape, dtype=np.float32),
    )

    attach_stock_futures_day_trade_daily(
        panel,
        data_path,
        fee_per_contract_per_side_twd=40.0,
        entry_price_source=ENTRY_PRICE_SOURCE_DAILY_SESSION_OPEN_0900_PROXY,
    )
    daily = panel.stock_futures_day_trade_daily
    assert daily is not None
    assert daily.entry_clock == (
        "taifex_day_session_open_0845_daily_proxy_for_0900_decision"
    )
    assert daily.entry_source_path == str(data_path)
    assert daily.entry_manifest_path == str(tmp_path / "manifest.json")
    np.testing.assert_allclose(
        daily.intraday_log_returns[:, 0],
        np.log(1.1),
        rtol=1.0e-6,
    )
    assert daily.policy_eligible_mask[:, 0].all()
    assert daily.executable_mask[:, 0].all()


def test_integer_attachment_aligns_two_candidate_slots_without_changing_stock_axis(
    tmp_path: Path,
) -> None:
    dates = [date(2026, 8, 10), date(2026, 8, 11)]
    rows: list[dict[str, object]] = []
    for day in dates:
        rows.append(
            _candidate(
                day=day,
                product="CDF",
                prior_volume=10,
                current_volume=10,
            )
        )
        mini = _candidate(
            day=day,
            product="QF",
            prior_volume=20,
            current_volume=20,
        )
        mini["contract_multiplier"] = 100.0
        rows.append(mini)
    data_path = tmp_path / "continuous_daily.parquet"
    pl.DataFrame(rows).write_parquet(data_path)
    digest = hashlib.sha256(data_path.read_bytes()).hexdigest()
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "contract_version": int(
                    TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION
                ),
                "outputs": {"continuous_daily": {"sha256": digest}},
            }
        ),
        encoding="utf-8",
    )
    shape = (2, 2)
    panel = PanelData(
        dates=np.asarray(dates, dtype="datetime64[D]"),
        symbols=["2330", "2317"],
        feature_names=["f0"],
        features=np.zeros((*shape, 1), dtype=np.float32),
        returns_1d=np.zeros(shape, dtype=np.float32),
        tradable_mask=np.ones(shape, dtype=bool),
        alive_mask=np.ones(shape, dtype=bool),
        benchmark_returns=np.zeros(2, dtype=np.float32),
        close_prices=np.ones(shape, dtype=np.float32),
    )
    attach_stock_futures_day_trade_daily(
        panel,
        data_path,
        fee_per_contract_per_side_twd=40.0,
        entry_price_source=ENTRY_PRICE_SOURCE_DAILY_SESSION_OPEN_0900_PROXY,
        integer_contracts=True,
        max_volume_participation=0.5,
    )
    daily = panel.stock_futures_day_trade_daily
    assert daily is not None
    assert daily.integer_candidate_execution is not None
    assert daily.integer_candidate_execution.shape == (2, 2, 2, 5)
    assert daily.policy_eligible_mask[:, 0].all()
    assert not daily.policy_eligible_mask[:, 1].any()
    assert np.isfinite(daily.integer_candidate_execution[:, 0]).all()
    assert np.isnan(daily.integer_candidate_execution[:, 1]).all()
    np.testing.assert_array_equal(
        daily.integer_candidate_execution[:, 0, :, 4],
        np.asarray([[5.0, 10.0], [5.0, 10.0]], dtype=np.float32),
    )


def test_0900_entry_uses_only_strictly_post_decision_trade_and_never_daily_open() -> None:
    selected = pl.DataFrame(
        [
            _candidate(
                day=date(2026, 8, 10),
                product="AAA",
                prior_volume=10,
                current_volume=10,
                open_price=100.0,
                close_price=110.0,
            ),
            _candidate(
                day=date(2026, 8, 10),
                product="BBB",
                underlying="2317",
                prior_volume=10,
                current_volume=10,
                open_price=50.0,
                close_price=55.0,
            ),
        ]
    ).with_columns(
        pl.when(pl.col("product") == "AAA")
        .then(False)
        .otherwise(pl.col("executable"))
        .alias("executable"),
        pl.when(pl.col("product") == "AAA")
        .then(float("nan"))
        .otherwise(pl.col("open"))
        .alias("open"),
    )
    entries = pl.DataFrame(
        {
            "date": [date(2026, 8, 10)],
            "physical_contract": ["AAA:202609"],
            "entry_time_hhmmss": [90001],
            "entry_price": [102.0],
            "matched_quantity": [2.0],
            "entry_source_row_observed": [True],
            "source_file_sha256": ["a" * 64],
        }
    )
    costed = _with_0900_execution_costs(
        selected,
        entries,
        fee_per_contract_per_side_twd=40.0,
    ).sort("underlying_symbol")
    aaa = costed.filter(pl.col("underlying_symbol") == "2330").row(0, named=True)
    bbb = costed.filter(pl.col("underlying_symbol") == "2317").row(0, named=True)
    assert aaa["round_trip_executable"]
    assert aaa["intraday_log_return"] == pytest.approx(np.log(110.0 / 102.0))
    assert aaa["intraday_log_return"] != pytest.approx(np.log(110.0 / 100.0))
    assert not bbb["round_trip_executable"]
    assert bbb["intraday_log_return"] is None


def test_0900_builder_excludes_decision_second_and_unselected_contracts() -> None:
    transactions = pl.DataFrame(
        {
            "trading_date": [date(2026, 8, 10)] * 4,
            "event_date": [date(2026, 8, 10)] * 4,
            "event_time": ["090000", "090001", "090002", "090001"],
            "event_ts": [
                "2026-08-10T09:00:00",
                "2026-08-10T09:00:01",
                "2026-08-10T09:00:02",
                "2026-08-10T09:00:01",
            ],
            "session": ["day"] * 4,
            "product": ["AAA", "AAA", "AAA", "BBB"],
            "delivery_month_week": ["202609"] * 4,
            "price": [99.0, 101.0, 102.0, 77.0],
            "matched_quantity": [1, 2, 3, 4],
            "source_row_number": [1, 2, 3, 4],
            "source_sha256": ["a" * 64] * 4,
        }
    ).with_columns(
        pl.col("event_ts").str.strptime(pl.Datetime("us"), strict=True)
    )
    selected = pl.DataFrame(
        {
            "date": [date(2026, 8, 10)],
            "physical_contract": ["AAA:202609"],
        }
    )
    entries = _first_strictly_later_entries(transactions, selected)
    assert entries.height == 1
    row = entries.row(0, named=True)
    assert row["entry_time_hhmmss"] == 90001
    assert row["entry_price"] == pytest.approx(101.0)
    assert row["physical_contract"] == "AAA:202609"


def test_0900_entry_manifest_requires_complete_panel_coverage(tmp_path: Path) -> None:
    path = tmp_path / "entry_0900.parquet"
    pl.DataFrame(
        {
            "date": [date(2026, 8, 10)],
            "physical_contract": ["AAA:202609"],
            "entry_time_hhmmss": [90001],
            "entry_price": [102.0],
            "matched_quantity": [2.0],
            "source_row_observed": [True],
            "source_file_sha256": ["a" * 64],
        }
    ).write_parquet(path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = {
        "dataset": "taifex_stock_futures_0900_entry_v1",
        "contract_version": 1,
        "status": "complete",
        "timezone": "Asia/Taipei",
        "decision_time": "09:00:00",
        "entry_rule": (
            "first_strictly_later_public_trade_row_through_09:00:59"
        ),
        "coverage": {
            "start": "2026-08-10",
            "end": "2026-08-10",
            "missing_trading_dates": [],
        },
        "outputs": {"entry_0900": {"sha256": digest}},
    }
    (tmp_path / "manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    dates = np.asarray(["2026-08-10"], dtype="datetime64[D]")
    entries, manifest_path = _load_0900_entries(path, panel_dates=dates)
    assert entries.height == 1
    assert manifest_path == tmp_path / "manifest.json"

    manifest["coverage"]["start"] = "2026-08-11"
    (tmp_path / "manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="does not span the complete panel"):
        _load_0900_entries(path, panel_dates=dates)


def test_active_count_scaled_l1_projection_can_hold_cash() -> None:
    logits = torch.tensor([[0.2, -0.1, 0.3, 100.0]])
    mask = torch.tensor([[True, True, True, False]])
    weights = masked_l1_projection_weights(
        logits,
        mask,
        long_only=False,
        scale_by_active_count=True,
    )
    torch.testing.assert_close(
        weights,
        torch.tensor([[0.2 / 3.0, -0.1 / 3.0, 0.3 / 3.0, 0.0]]),
    )
    assert weights.abs().sum() < 1.0


@pytest.mark.parametrize(
    "execution_mode",
    ["tw_stock_futures_day_trade", "tw_stock_futures_day_trade_0900"],
)
def test_tensor_execution_masks_before_allocation_and_never_reallocates_nonfills(
    execution_mode: str,
) -> None:
    raw = torch.tensor([[1.0, 1.0, 100.0]], requires_grad=True)
    policy_has_future = torch.tensor([[True, True, False]])
    current_executable = torch.tensor([[True, False, False]])
    returns = torch.tensor([[np.log1p(0.10), float("nan"), float("nan")]])
    costs = torch.tensor([[0.01, float("nan"), float("nan")]])

    result = run_backtest_torch(
        raw,
        returns,
        policy_has_future,
        torch.zeros(1),
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        portfolio_activation="identity",
        execution_mode=execution_mode,
        can_buy_mask=current_executable,
        can_sell_mask=current_executable,
        overnight_returns=costs,
        volume_limit_weights=torch.tensor([[0.80, 0.80, 0.80]]),
    )

    # Policy normalization first assigns 0.5/0.5 to the two known futures.
    # The first name keeps its 0.5 target, the second fails execution, and neither
    # missing capacity is reallocated.
    torch.testing.assert_close(
        result.weights_history,
        torch.tensor([[0.50, 0.0, 0.0]]),
    )
    torch.testing.assert_close(
        result.strategy_returns,
        torch.log1p(torch.tensor([0.50 * (0.10 - 0.01)])),
    )
    torch.testing.assert_close(result.turnovers, torch.tensor([1.0]))
    assert torch.equal(result.final_weights, torch.zeros(3))
    assert bool(result.final_alive)

    # This CPU-only gradient assertion must remain usable on hosts where the
    # NVIDIA driver is temporarily unavailable even though torch was built
    # with CUDA.  The strict CUDA environment check is tested separately.
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=r"CUDA initialization: CUDA unknown error.*",
            category=UserWarning,
        )
        result.strategy_returns.sum().backward()
    assert raw.grad is not None
    assert raw.grad[0, 0] != 0.0
    # The known-but-unfilled future remains in the causal policy denominator,
    # so it correctly receives a learning signal.  The no-futures stock does
    # not participate in either allocation or gradients.
    assert raw.grad[0, 1] != 0.0
    assert raw.grad[0, 2] == 0.0


def test_no_known_futures_means_zero_output_and_zero_trade() -> None:
    raw = torch.tensor([[2.0, -3.0]], requires_grad=True)
    no_future = torch.zeros_like(raw, dtype=torch.bool)
    result = run_backtest_torch(
        raw,
        torch.full_like(raw, float("nan")),
        no_future,
        torch.zeros(1),
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=False,
        portfolio_activation="identity",
        execution_mode="tw_stock_futures_day_trade",
        can_buy_mask=no_future,
        can_sell_mask=no_future,
        overnight_returns=torch.full_like(raw, float("nan")),
        volume_limit_weights=torch.zeros_like(raw),
    )
    assert torch.equal(result.weights_history, torch.zeros_like(raw))
    assert torch.equal(result.strategy_returns, torch.zeros(1))
    assert torch.equal(result.turnovers, torch.zeros(1))


def test_formal_config_keeps_complete_feature_abi_and_1000_epochs() -> None:
    config = load_config(
        "configs/markets/"
        "tw_stock_futures_day_trade_full_features_multi_basis_projection_l1_capital10m.yaml"
    )
    all_futures = load_config(
        "configs/markets/"
        "tw_stock_context_all_futures_portfolio_multi_basis_projection_l1.yaml"
    )
    assert normalize_execution_mode("個股期貨當沖") == (
        "tw_stock_futures_day_trade"
    )
    assert action_channels_for_execution_mode(
        "tw_stock_futures_day_trade"
    ) == ("target",)
    assert config.data.parquet_root == "data_tw_public/stocks"
    assert config.data.panel_start_date == "2014-01-01"
    assert not config.data.day_trade_open_feature
    assert config.data.feature_include == all_futures.data.feature_include
    assert config.data.feature_exclude == all_futures.data.feature_exclude
    assert config.data.feature_exclude == ["next_session_open_gap_logret"]
    assert len(config.data.feature_include) == 99
    assert len(
        [
            feature
            for feature in config.data.feature_include
            if feature not in config.data.feature_exclude
        ]
    ) == 98
    assert "twpub_pe_log" in config.data.feature_include
    assert "twpub_margin_balance_log" in config.data.feature_include
    assert "twpub_foreign_net_buy_flow" in config.data.feature_include
    assert "twpub_material_event_count_log" in config.data.feature_include
    assert "twpub_cbc_m2_yoy" in config.data.feature_include
    assert "twpub_taifex_tx_open_interest_log" in config.data.feature_include
    assert config.training.model_name == "financial_transformer"
    assert config.training.epochs == 1000
    assert config.trading.execution_mode == "tw_stock_futures_day_trade"
    assert config.trading.max_volume_participation == pytest.approx(0.5)
    assert config.trading.buy_fee_rate == 0.0
    assert config.trading.sell_fee_rate == 0.0
    assert config.runner.output_dir.endswith(
        "tw_stock_futures_day_trade_full_stock_context_full_features_"
        "multi_basis_projection_l1_capital10m_v2"
    )


def test_0900_daily_proxy_baseline_is_default_clock_complete_feature_successor() -> None:
    config = load_config(
        "configs/markets/"
        "tw_stock_futures_day_trade_0900_full_features_multi_basis_projection_l1_cash_capital10m.yaml"
    )
    legacy = load_config(
        "configs/markets/"
        "tw_stock_futures_day_trade_full_features_multi_basis_projection_l1_capital10m.yaml"
    )
    assert normalize_execution_mode("個股期貨9點當沖") == (
        "tw_stock_futures_day_trade_0900"
    )
    assert action_channels_for_execution_mode(
        "tw_stock_futures_day_trade_0900"
    ) == ("target",)
    assert config.trading.execution_mode == "tw_stock_futures_day_trade_0900"
    assert config.trading.tw_stock_futures_day_trade_0900_entry_source == (
        "daily_session_open_proxy"
    )
    assert config.trading.tw_stock_futures_day_trade_0900_data_path == ""
    assert config.data.day_trade_open_feature
    assert config.data.feature_exclude == []
    assert config.data.feature_include == legacy.data.feature_include
    assert len(config.data.feature_include) == 99
    assert config.training.model_name == "financial_transformer"
    assert config.training.epochs == 1000
    assert not config.training.financial_transformer.center_long_short_logits
    assert (
        config.training.financial_transformer.projection_l1_scale_by_active_count
    )
    assert config.training.financial_transformer.portfolio_output_mode == (
        "projection_l1"
    )
    assert config.trading.portfolio_activation == "pre_normalized"
    assert config.training.loss_portfolio_activation == "pre_normalized"
    assert config.runner.output_dir.endswith(
        "tw_stock_futures_day_trade_0900_daily_open_proxy_full_stock_context_"
        "full_features_multi_basis_projection_l1_cash_capital10m_v4"
    )
    assert config.runner.output_dir != legacy.runner.output_dir
    new_contract = _trading_checkpoint_contract(config)
    old_contract = _trading_checkpoint_contract(legacy)
    assert new_contract != old_contract
    new_futures = new_contract["taiwan_stock_futures_day_trade"]
    old_futures = old_contract["taiwan_stock_futures_day_trade"]
    assert new_futures["decision_clock"] == (
        "observed_cash_open_at_090000_asia_taipei"
    )
    assert new_futures["execution_clock"] == (
        "taifex_day_session_open_0845_to_close_daily_proxy_for_0900_decision"
    )
    assert new_futures["entry_price_source"] == "daily_session_open_proxy"
    assert new_futures["entry_fallback"] == (
        "none_declared_source_is_primary"
    )
    assert new_futures["execution_causality"] == (
        "counterfactual_daily_open_proxy_entry_precedes_decision_by_15_minutes_"
        "not_live_executable"
    )
    assert old_futures["execution_clock"] == "taifex_day_session_open_to_close"


def test_0900_integer_baseline_preserves_features_and_breaks_checkpoint_contract() -> None:
    integer = load_config(
        "configs/markets/"
        "tw_stock_futures_day_trade_0900_integer_full_features_multi_basis_projection_l1_cash_capital10m.yaml"
    )
    continuous = load_config(
        "configs/markets/"
        "tw_stock_futures_day_trade_0900_full_features_multi_basis_projection_l1_cash_capital10m.yaml"
    )
    assert normalize_execution_mode("個股期貨9點整數口當沖") == (
        "tw_stock_futures_day_trade_0900_integer"
    )
    assert action_channels_for_execution_mode(
        "tw_stock_futures_day_trade_0900_integer"
    ) == ("target",)
    assert integer.data.feature_include == continuous.data.feature_include
    assert integer.data.feature_exclude == continuous.data.feature_exclude == []
    assert integer.training.epochs == 1000
    assert integer.trading.tw_stock_futures_day_trade_fee_twd == 40.0
    assert integer.trading.tw_stock_futures_day_trade_initial_capital == 10_000_000.0
    assert integer.trading.volume_participation_equity == 10_000_000.0
    assert integer.runner.output_dir != continuous.runner.output_dir
    integer_contract = _trading_checkpoint_contract(integer)[
        "taiwan_stock_futures_day_trade"
    ]
    continuous_contract = _trading_checkpoint_contract(continuous)[
        "taiwan_stock_futures_day_trade"
    ]
    assert integer_contract != continuous_contract
    assert integer_contract["candidate_multipliers"] == [2_000.0, 100.0]
    assert integer_contract["quantity_contract"] == (
        "signed_q_j_int64_whole_contracts_only"
    )
    assert integer_contract["accounting"] == (
        "exact_forward_integer_contract_fully_collateralized_recurrent_equity"
    )


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"data": {"day_trade_open_feature": False}}, "open_feature=true"),
        (
            {"training": {"financial_transformer": {"center_long_short_logits": True}}},
            "must not de-mean logits",
        ),
        (
            {
                "training": {
                    "financial_transformer": {
                        "projection_l1_scale_by_active_count": False
                    }
                }
            },
            "projection_l1_scale_by_active_count=true",
        ),
        (
            {
                "trading": {
                    "tw_stock_futures_day_trade_0900_entry_source": "unknown"
                }
            },
            "entry_source to be one of",
        ),
        (
            {
                "trading": {
                    "tw_stock_futures_day_trade_0900_entry_source": (
                        "post_0900_trade_sidecar"
                    )
                }
            },
            "requires a receipt-backed",
        ),
    ],
)
def test_0900_baseline_assumptions_fail_closed(
    tmp_path: Path,
    override: dict[str, object],
    message: str,
) -> None:
    baseline = Path(
        "configs/markets/"
        "tw_stock_futures_day_trade_0900_full_features_multi_basis_projection_l1_cash_capital10m.yaml"
    ).resolve()
    payload: dict[str, object] = {"base_config": str(baseline), **override}
    path = tmp_path / "invalid.yaml"
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    with pytest.raises(ValueError, match=message):
        load_config(path)


def test_fop_substitution_ablation_is_a_fair_two_arm_contract(
    tmp_path: Path,
) -> None:
    spec_path = Path(
        "configs/ablations/tw_stock_futures_substitution_day_trade_v1.yaml"
    ).resolve()
    spec, experiments = _experiment_rows(spec_path)
    assert [row["name"] for row in experiments] == [
        "cash_stock_day_trade_no_open_control",
        "nearby_stock_futures_day_trade",
    ]
    assert spec["expected_fold_count"] == 12
    assert spec["runtime"]["parallel_jobs"] == 1

    runs = _build_configs(spec_path, spec, experiments, tmp_path)
    raw = {
        run["name"]: yaml.safe_load(run["config_path"].read_text(encoding="utf-8"))
        for run in runs
    }
    loaded = {
        run["name"]: load_config(run["config_path"])
        for run in runs
    }
    control = loaded["cash_stock_day_trade_no_open_control"]
    treatment = loaded["nearby_stock_futures_day_trade"]

    for config in (control, treatment):
        assert config.data.parquet_root == "data_tw_public/stocks"
        assert config.data.panel_start_date == "2014-01-01"
        assert config.data.benchmark_name == "universe_average_return"
        assert not config.data.day_trade_open_feature
        assert "next_session_open_gap_logret" in config.data.feature_exclude
        assert config.training.model_name == "financial_transformer"
        assert config.training.lookback == 32
        assert config.training.epochs == 1000
        assert config.training.loss_type == "log_utility"
        assert config.training.multi_gpu_strategy == "distributed_data_parallel"
        assert config.trading.long_only is False
        assert config.trading.volume_participation_equity == pytest.approx(
            10_000_000.0
        )

    assert control.data.feature_include == treatment.data.feature_include
    assert control.data.feature_exclude == treatment.data.feature_exclude
    assert control.training.financial_transformer == (
        treatment.training.financial_transformer
    )
    assert control.trading.execution_mode == "tw_day_trade"
    assert control.trading.buy_fee_rate == pytest.approx(0.000855)
    assert control.trading.sell_fee_rate == pytest.approx(0.003855)
    assert control.trading.tw_day_trade_unlimited_margin_conversion is True
    assert treatment.trading.execution_mode == "tw_stock_futures_day_trade"
    assert treatment.trading.buy_fee_rate == 0.0
    assert treatment.trading.sell_fee_rate == 0.0
    assert treatment.trading.tw_day_trade_unlimited_margin_conversion is False

    # Generated roots are fresh and arm-specific; no checkpoint can cross the
    # incompatible stock/futures execution boundary.
    assert raw["cash_stock_day_trade_no_open_control"]["runner"]["output_dir"] != (
        raw["nearby_stock_futures_day_trade"]["runner"]["output_dir"]
    )
