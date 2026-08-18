from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import torch

from stockagent.backtest.simulator import run_backtest_torch
from stockagent.backtest.tw_futures_portfolio import (
    run_tw_futures_portfolio_continuous_numpy,
)
from stockagent.config import load_config
from stockagent.data.tw_futures_portfolio_daily import (
    build_continuous_daily,
    build_product_master,
)
from stockagent.models.transformer_base_portfolio import (
    action_channels_for_execution_mode,
)


def _source_row(
    day: str,
    product: str,
    contract: str,
    open_price: float,
    close_price: float,
) -> dict[str, object]:
    return {
        "date": day,
        "product": product,
        "contract": contract,
        "series_type": "monthly",
        "session": "一般",
        "session_reported": True,
        "open": open_price,
        "high": max(open_price, close_price),
        "low": min(open_price, close_price),
        "close": close_price,
        "volume": 10,
        "settlement": close_price,
        "open_interest": 100,
        "last_bid": close_price - 1.0,
        "last_ask": close_price + 1.0,
        "historical_high": None,
        "historical_low": None,
        "suspension_status": None,
        "spread_order_volume": 0,
        "source_file": "fixture.csv",
        "source_sha256": "0" * 64,
    }


def _write_official_codes(path: Path, rows: list[tuple[str, str]]) -> None:
    pl.DataFrame(rows, schema=["code", "product_name"], orient="row").write_csv(path)


def test_expiry_month_slot_keeps_far_contract_until_its_own_expiry(tmp_path: Path) -> None:
    source = tmp_path / "all.parquet"
    products = tmp_path / "products.csv"
    stocks = tmp_path / "stocks.csv"
    official = tmp_path / "official.csv"
    pl.DataFrame(
        [
            _source_row("2026-08-10", "TX", "202608", 100.0, 104.0),
            _source_row("2026-08-11", "TX", "202608", 110.0, 120.0),
            _source_row("2026-08-10", "TX", "202609", 210.0, 211.0),
            _source_row("2026-08-12", "TX", "202609", 220.0, 221.0),
            _source_row("2026-08-10", "CDF", "202608", 50.0, 51.0),
            _source_row("2026-08-11", "CDF", "202608", 52.0, 53.0),
            _source_row("2026-08-12", "CDF", "202609", 60.0, 61.0),
        ]
    ).with_columns(pl.col("date").str.to_date()).write_parquet(source)
    pl.DataFrame(
        {
            "root": ["TXF", "CDF"],
            "product_name": ["臺股期貨", "台積電期貨"],
            "raw_root_name": ["臺股期貨", "台積電期貨"],
            "listed_contracts": [2, 2],
            "continuous_r1": ["TXFR1", "CDFR1"],
            "continuous_r2": ["TXFR2", "CDFR2"],
            "listed_codes_json": ["[]", "[]"],
        }
    ).write_csv(products)
    pl.DataFrame(
        {
            "code": ["2330"],
            "name": ["台積電"],
            "market": ["twse"],
            "security_type": ["stock"],
            "source": ["fixture"],
        }
    ).write_csv(stocks)
    _write_official_codes(official, [("TX", "臺股期貨"), ("CDF", "台積電期貨")])

    daily, master = build_continuous_daily(source, products, stocks, official)
    assert set(master["official_product"].to_list()) == {"TX", "CDF"}
    tx_aug = daily.filter(pl.col("symbol") == "TX_M08_L1").sort("date")
    assert tx_aug["contract"].to_list() == ["202608", "202608"]
    assert np.isclose(tx_aug["holding_log_return"][0], np.log(110.0 / 100.0))
    assert np.isclose(tx_aug["holding_log_return"][1], np.log(120.0 / 110.0))
    assert tx_aug["must_liquidate"].to_list() == [False, True]
    assert tx_aug["liquidation_reason"][1] == "last_trade_date"

    # September remains M09 on both sides of the August expiry.  It does not
    # migrate from R2 to R1 and can therefore stay open as a far-month holding.
    tx_sep = daily.filter(pl.col("symbol") == "TX_M09_L1").sort("date")
    assert tx_sep["contract"].to_list() == ["202609", "202609", "202609"]
    assert tx_sep["source_row_observed"].to_list() == [True, False, True]
    assert tx_sep["executable"].to_list() == [True, False, True]
    assert tx_sep["must_liquidate"].to_list() == [False, False, True]
    assert np.isclose(tx_sep["holding_log_return"][0], np.log(211.0 / 210.0))
    assert np.isclose(tx_sep["holding_log_return"][1], np.log(220.0 / 211.0))


def test_same_delivery_month_overlap_gets_stable_lanes(tmp_path: Path) -> None:
    source = tmp_path / "all.parquet"
    products = tmp_path / "products.csv"
    stocks = tmp_path / "stocks.csv"
    official = tmp_path / "official.csv"
    pl.DataFrame(
        [
            _source_row("2026-08-10", "SPF", "202609", 100.0, 101.0),
            _source_row("2026-08-10", "SPF", "202709", 200.0, 201.0),
            _source_row("2026-08-11", "SPF", "202609", 102.0, 103.0),
            _source_row("2026-08-11", "SPF", "202709", 202.0, 203.0),
            _source_row("2026-08-12", "SPF", "202709", 204.0, 205.0),
        ]
    ).with_columns(pl.col("date").str.to_date()).write_parquet(source)
    pl.DataFrame(
        {
            "root": ["SPF"],
            "product_name": ["S&P500期貨"],
            "raw_root_name": ["S&P500期貨"],
            "listed_contracts": [2],
            "continuous_r1": ["SPFR1"],
            "continuous_r2": ["SPFR2"],
            "listed_codes_json": ["[]"],
        }
    ).write_csv(products)
    pl.DataFrame(
        {
            "code": ["2330"],
            "name": ["台積電"],
            "market": ["twse"],
            "security_type": ["stock"],
            "source": ["fixture"],
        }
    ).write_csv(stocks)
    _write_official_codes(official, [("SPF", "S&P500期貨")])

    daily, _ = build_continuous_daily(source, products, stocks, official)
    older = daily.filter(pl.col("contract") == "202609")
    farther = daily.filter(pl.col("contract") == "202709").sort("date")
    assert older["symbol"].unique().to_list() == ["SPF_M09_L1"]
    assert farther["symbol"].unique().to_list() == ["SPF_M09_L2"]
    assert farther["must_liquidate"].to_list() == [False, False, True]


def test_historical_product_master_uses_official_codes_and_explicit_scope(
    tmp_path: Path,
) -> None:
    products = tmp_path / "products.csv"
    stocks = tmp_path / "stocks.csv"
    official = tmp_path / "official.csv"
    pl.DataFrame(
        {
            "root": ["TXF"],
            "product_name": ["臺股期貨"],
        }
    ).write_csv(products)
    pl.DataFrame(
        {
            "code": ["2104"],
            "name": ["國際中橡"],
            "security_type": ["stock"],
        }
    ).write_csv(stocks)
    _write_official_codes(
        official,
        [("FJF", "中橡期貨"), ("I5F", "印度50期貨"), ("CPF", "原油期貨")],
    )

    master = build_product_master(
        products,
        stocks,
        official,
        source_products=["FJF", "I5F", "CPF"],
    )
    assert master["official_product"].to_list() == ["FJF", "I5F"]
    fjf = master.filter(pl.col("official_product") == "FJF").row(0, named=True)
    assert fjf["underlying_symbol"] == "2104"
    assert fjf["asset_class"] == "stock_future"
    assert fjf["listing_scope"] == "historical"
    i5f = master.filter(pl.col("official_product") == "I5F").row(0, named=True)
    assert i5f["asset_class"] == "index_future"
    assert i5f["region"] == "foreign"


def test_historical_product_master_does_not_require_current_broker_catalogue(
    tmp_path: Path,
) -> None:
    missing_products = tmp_path / "missing-products.csv"
    stocks = tmp_path / "stocks.csv"
    official = tmp_path / "official.csv"
    pl.DataFrame(
        {
            "code": ["2104", "2384"],
            "name": ["國際中橡", "勝華"],
            "security_type": ["stock", "stock"],
        }
    ).write_csv(stocks)
    _write_official_codes(
        official,
        [("DT1", "勝華期貨"), ("FJF", "中橡期貨"), ("TX", "臺股期貨")],
    )

    master = build_product_master(
        missing_products,
        stocks,
        official,
        source_products=["DT1", "FJF", "TX"],
    )

    assert master["official_product"].to_list() == ["DT1", "FJF", "TX"]
    assert master["underlying_symbol"].to_list() == ["2384", "2104", None]
    assert master["shioaji_roots"].to_list() == ["", "", ""]
    assert master["listing_scope"].to_list() == [
        "historical",
        "historical",
        "historical",
    ]
    assert master["fixed_fee_research_supported"].to_list() == [False, True, True]
    assert master["contract_multiplier"].to_list() == [None, 2000.0, 200.0]
    assert master["sinopac_network_fee_group"].to_list() == [None, "stock", "large"]


def test_futures_portfolio_uses_one_signed_target_action_channel() -> None:
    assert action_channels_for_execution_mode("tw_futures_portfolio_day") == (
        "target",
    )


def test_tensor_ledger_closes_after_return_and_matches_numpy() -> None:
    weights = torch.tensor([[1.0], [1.0]], requires_grad=True)
    returns = torch.log1p(torch.tensor([[0.10], [0.20]]))
    tradable = torch.ones_like(weights, dtype=torch.bool)
    liquidate = torch.tensor([[False], [True]])
    benchmark = torch.zeros(2)
    fee_rates = torch.tensor([[0.01], [0.02]])
    result = run_backtest_torch(
        weights,
        returns,
        tradable,
        benchmark,
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=True,
        portfolio_activation="identity",
        execution_mode="tw_futures_portfolio_day",
        can_buy_mask=tradable,
        can_sell_mask=tradable,
        force_exit_mask=liquidate,
        overnight_returns=fee_rates,
    )
    expected = run_tw_futures_portfolio_continuous_numpy(
        np.ones((2, 1), dtype=np.float32),
        returns.detach().numpy(),
        tradable.numpy(),
        liquidate.numpy(),
        fee_rate_per_open_notional=fee_rates.numpy(),
    )
    np.testing.assert_allclose(
        result.strategy_returns.detach().numpy(), expected.strategy_returns, rtol=1e-6
    )
    assert torch.equal(result.final_weights, torch.zeros_like(result.final_weights))
    result.strategy_returns.sum().backward()
    assert weights.grad is not None
    assert torch.isfinite(weights.grad).all()


def test_current_open_execution_mask_does_not_leak_or_ruin_account() -> None:
    weights = torch.tensor([[1.0], [1.0]], requires_grad=True)
    returns = torch.tensor([[float("nan")], [0.10]])
    prior_known = torch.ones_like(weights, dtype=torch.bool)
    current_executable = torch.tensor([[False], [True]])
    result = run_backtest_torch(
        weights,
        returns,
        prior_known,
        torch.zeros(2),
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=True,
        portfolio_activation="identity",
        execution_mode="tw_futures_portfolio_day",
        can_buy_mask=current_executable,
        can_sell_mask=current_executable,
        force_exit_mask=torch.tensor([[False], [True]]),
        overnight_returns=torch.zeros_like(weights),
    )
    # The model was allowed to request the contract from prior information,
    # but the executor rejects row 0 because the current open is unavailable.
    # This is a non-fill, not a fabricated flat holding or absorbing default.
    torch.testing.assert_close(result.strategy_returns, torch.tensor([0.0, 0.10]))
    assert bool(result.final_alive)
    assert torch.equal(result.final_weights, torch.zeros_like(result.final_weights))


def test_existing_position_is_carried_across_zero_transaction_day() -> None:
    weights = torch.tensor([[1.0], [0.0], [1.0]])
    returns = torch.tensor([[0.0], [0.05], [0.0]])
    known_from_prior_day = torch.ones_like(weights, dtype=torch.bool)
    current_executable = torch.tensor([[True], [False], [True]])
    result = run_backtest_torch(
        weights,
        returns,
        known_from_prior_day,
        torch.zeros(3),
        buy_fee_rate=0.0,
        sell_fee_rate=0.0,
        long_only=True,
        portfolio_activation="identity",
        execution_mode="tw_futures_portfolio_day",
        can_buy_mask=current_executable,
        can_sell_mask=current_executable,
        force_exit_mask=torch.tensor([[False], [False], [True]]),
        overnight_returns=torch.zeros_like(weights),
    )
    # Row 1 cannot trade, so its zero target cannot flatten the existing lot.
    # The position remains valued and earns the bridge return until trading
    # resumes; the physical contract is then closed by the expiry flag.
    torch.testing.assert_close(
        result.strategy_returns, torch.tensor([0.0, 0.05, 0.0])
    )
    assert result.weights_history[1, 0] > 0.0
    assert bool(result.final_alive)
    assert torch.equal(result.final_weights, torch.zeros_like(result.final_weights))


def test_new_training_config_uses_canonical_daily_trainer() -> None:
    config = load_config("configs/markets/tw_futures_portfolio_day.yaml")
    assert config.trading.execution_mode == "tw_futures_portfolio_day"
    assert config.training.model_name == "transformer_base_portfolio"
    assert config.training.loss_type == "log_utility"
    assert config.training.epochs == 1000
    assert config.data.use_tw_public_features
    assert not config.data.use_tw_public_rules
    assert config.trading.buy_fee_rate == 0.0
    assert config.trading.sell_fee_rate == 0.0
    assert config.trading.tw_futures_portfolio_fee_large_twd == 60.0
    assert config.trading.tw_futures_portfolio_fee_standard_twd == 24.0
    assert config.trading.tw_futures_portfolio_fee_stock_twd == 40.0
    assert config.trading.tw_futures_portfolio_fee_micro_twd == 16.0
    assert "taifex_portfolio_daily_v3" in config.trading.tw_futures_portfolio_data_path
