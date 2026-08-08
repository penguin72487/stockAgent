from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from scripts.backtest_taifex_opening_straddle_hold_to_expiry import (
    _assert_accounting,
    _build_cycle_results,
    _build_daily_results,
    _plan_cycles,
)


def _source_row(
    trading_date: str,
    series: str,
    *,
    strike: float = 20_000.0,
    call_open: float = 100.0,
    put_open: float = 90.0,
) -> dict[str, object]:
    return {
        "date": trading_date,
        "option_series": series,
        "strike": strike,
        "tx_open": 20_020.0,
        "opening_abs_moneyness_points": 20.0,
        "call_open": call_open,
        "put_open": put_open,
        "call_source_file": "source.csv",
        "call_source_sha256": "a" * 64,
        "put_source_file": "source.csv",
        "put_source_sha256": "a" * 64,
    }


def test_cycle_keeps_contract_until_expiry_then_enters_next_session() -> None:
    source = pd.DataFrame(
        [
            _source_row("2025-01-02", "202501F1"),
            _source_row("2025-01-03", "202501F1", strike=20_100.0),
            _source_row("2025-01-06", "202501W2", strike=20_200.0),
            _source_row("2025-01-07", "202501W2", strike=20_300.0),
            _source_row("2025-01-08", "202501W2", strike=20_400.0),
        ]
    )

    settlements = {
        (date(2025, 1, 3), "202501F1"): {"settlement_price": 20_050.0},
        (date(2025, 1, 8), "202501W2"): {"settlement_price": 20_450.0},
    }
    plans, skipped = _plan_cycles(source, settlements)

    assert skipped.empty
    assert plans["entry_date"].tolist() == [date(2025, 1, 2), date(2025, 1, 6)]
    assert plans["expiry_date"].tolist() == [date(2025, 1, 3), date(2025, 1, 8)]
    assert plans["strike"].tolist() == [20_000.0, 20_200.0]


def test_cycle_uses_official_holiday_shifted_expiry_not_calendar_guess() -> None:
    source = pd.DataFrame(
        [
            _source_row("2013-04-30", "201305W1", strike=8_200.0),
            _source_row("2013-05-02", "201305W1", strike=8_250.0),
            _source_row("2013-05-03", "201305W2", strike=8_300.0),
            _source_row("2013-05-08", "201305W2", strike=8_350.0),
        ]
    )
    settlements = {
        (date(2013, 5, 2), "201305W1"): {"settlement_price": 8_230.0},
        (date(2013, 5, 8), "201305W2"): {"settlement_price": 8_350.0},
    }

    plans, skipped = _plan_cycles(source, settlements)

    assert skipped.empty
    assert plans.iloc[0]["entry_date"] == date(2013, 4, 30)
    assert plans.iloc[0]["calendar_expiry_date"] == date(2013, 5, 1)
    assert plans.iloc[0]["expiry_date"] == date(2013, 5, 2)
    assert plans.iloc[0]["official_minus_calendar_expiry_days"] == 1


def test_cycle_charges_opening_fees_and_statutory_taxes_then_finishes_flat() -> None:
    plans = pd.DataFrame(
        [
            {
                "entry_date": date(2025, 1, 2),
                "expiry_date": date(2025, 1, 3),
                "calendar_expiry_date": date(2025, 1, 3),
                "option_series": "202501F1",
                "strike": 20_000.0,
                "tx_open": 20_020.0,
                "opening_abs_moneyness_points": 20.0,
                "call_entry_price_points": 100.0,
                "put_entry_price_points": 90.0,
                "holding_sessions": 2,
                "call_entry_source_file": "entry.csv",
                "call_entry_source_sha256": "a" * 64,
                "put_entry_source_file": "entry.csv",
                "put_entry_source_sha256": "a" * 64,
            }
        ]
    )
    terminal_rows = {
        (date(2025, 1, 3), "202501F1", 20_000.0, "C"): {
            "close": 130.0,
            "source_file": "expiry.csv",
            "source_sha256": "b" * 64,
        },
        (date(2025, 1, 3), "202501F1", 20_000.0, "P"): {
            "close": 80.0,
            "source_file": "expiry.csv",
            "source_sha256": "b" * 64,
        },
    }

    cycles, trades, excluded = _build_cycle_results(
        plans,
        terminal_rows,
        fee_per_contract_side_twd=22.0,
        official_settlements={
            (date(2025, 1, 3), "202501F1"): {
                "settlement_price": 20_210.0,
                "source_file": "settlement.parquet",
                "source_sha256": "c" * 64,
            }
        },
    )
    _assert_accounting(cycles, trades)

    assert excluded.empty
    assert cycles.iloc[0]["gross_pnl_twd"] == pytest.approx(1_000.0)
    assert cycles.iloc[0]["fee_twd"] == pytest.approx(44.0)
    assert cycles.iloc[0]["entry_transaction_tax_twd"] == pytest.approx(10.0)
    assert cycles.iloc[0]["settlement_transaction_tax_twd"] == pytest.approx(
        20.0
    )
    assert cycles.iloc[0]["transaction_tax_twd"] == pytest.approx(30.0)
    assert cycles.iloc[0]["net_after_fee_twd"] == pytest.approx(956.0)
    assert cycles.iloc[0]["net_pnl_twd"] == pytest.approx(926.0)
    assert cycles.iloc[0]["required_capital_twd"] == pytest.approx(9_554.0)
    assert cycles.iloc[0]["final_call_contracts"] == 0
    assert cycles.iloc[0]["final_put_contracts"] == 0
    assert trades["fixed_fee_twd"].sum() == pytest.approx(44.0)
    assert trades["transaction_tax_twd"].sum() == pytest.approx(30.0)
    assert trades.groupby("option_right")["delta_contracts"].sum().eq(0).all()


def test_official_settlement_replaces_expiry_last_trade_proxy() -> None:
    plans = pd.DataFrame(
        [
            {
                "entry_date": date(2025, 11, 3),
                "expiry_date": date(2025, 11, 5),
                "calendar_expiry_date": date(2025, 11, 5),
                "option_series": "202511W1",
                "strike": 28_400.0,
                "tx_open": 28_410.0,
                "opening_abs_moneyness_points": 10.0,
                "call_entry_price_points": 200.0,
                "put_entry_price_points": 150.0,
                "holding_sessions": 3,
                "call_entry_source_file": "entry.csv",
                "call_entry_source_sha256": "a" * 64,
                "put_entry_source_file": "entry.csv",
                "put_entry_source_sha256": "a" * 64,
            }
        ]
    )
    terminal_rows = {
        (date(2025, 11, 5), "202511W1", 28_400.0, "C"): {
            "close": 0.1,
            "source_file": "expiry.csv",
            "source_sha256": "b" * 64,
        },
        (date(2025, 11, 5), "202511W1", 28_400.0, "P"): {
            "close": 650.0,
            "source_file": "expiry.csv",
            "source_sha256": "b" * 64,
        },
    }
    settlements = {
        (date(2025, 11, 5), "202511W1"): {
            "settlement_price": 27_752.0,
            "source_file": "settlement.parquet",
            "source_sha256": "c" * 64,
        }
    }

    cycles, trades, excluded = _build_cycle_results(
        plans,
        terminal_rows,
        fee_per_contract_side_twd=22.0,
        official_settlements=settlements,
    )
    _assert_accounting(cycles, trades)

    assert excluded.empty
    assert cycles.iloc[0]["terminal_value_source"] == (
        "official_taifex_final_settlement_price"
    )
    assert cycles.iloc[0]["call_terminal_price_points"] == 0.0
    assert cycles.iloc[0]["put_terminal_price_points"] == 648.0
    assert cycles.iloc[0]["cash_settlement_option_right"] == "P"
    assert cycles.iloc[0]["entry_transaction_tax_twd"] == pytest.approx(18.0)
    assert cycles.iloc[0]["settlement_transaction_tax_twd"] == pytest.approx(
        28.0
    )
    assert cycles.iloc[0]["proxy_minus_settlement_terminal_twd"] == pytest.approx(
        105.0
    )
    assert set(trades.loc[trades["delta_contracts"].eq(-1), "reason"]) == {
        "official_expiry_cash_settlement"
    }


def test_daily_mark_to_market_reconciles_to_taxed_cycle_pnl() -> None:
    plans = pd.DataFrame(
        [
            {
                "entry_date": date(2025, 1, 2),
                "expiry_date": date(2025, 1, 3),
                "calendar_expiry_date": date(2025, 1, 3),
                "option_series": "202501F1",
                "strike": 20_000.0,
                "tx_open": 20_020.0,
                "opening_abs_moneyness_points": 20.0,
                "call_entry_price_points": 100.0,
                "put_entry_price_points": 90.0,
                "holding_sessions": 2,
                "call_entry_source_file": "entry.csv",
                "call_entry_source_sha256": "a" * 64,
                "put_entry_source_file": "entry.csv",
                "put_entry_source_sha256": "a" * 64,
            }
        ]
    )
    contract_rows = {
        (date(2025, 1, 2), "202501F1", 20_000.0, "C"): {
            "settlement": 120.0,
            "close": 121.0,
        },
        (date(2025, 1, 2), "202501F1", 20_000.0, "P"): {
            "settlement": 80.0,
            "close": 79.0,
        },
        (date(2025, 1, 3), "202501F1", 20_000.0, "C"): {
            "close": 130.0,
            "source_file": "expiry.csv",
            "source_sha256": "b" * 64,
        },
        (date(2025, 1, 3), "202501F1", 20_000.0, "P"): {
            "close": 80.0,
            "source_file": "expiry.csv",
            "source_sha256": "b" * 64,
        },
    }
    settlements = {
        (date(2025, 1, 3), "202501F1"): {
            "settlement_price": 20_210.0,
            "source_file": "settlement.parquet",
            "source_sha256": "c" * 64,
        }
    }
    cycles, _, _ = _build_cycle_results(
        plans,
        contract_rows,
        fee_per_contract_side_twd=22.0,
        official_settlements=settlements,
    )

    daily, metrics = _build_daily_results(
        cycles,
        contract_rows,
        [date(2025, 1, 2), date(2025, 1, 3)],
        capital_base_twd=9_554.0,
    )

    assert daily["net_after_fee_and_tax_twd"].tolist() == pytest.approx(
        [446.0, 480.0]
    )
    assert daily["net_after_fee_and_tax_twd"].sum() == pytest.approx(926.0)
    assert metrics["total_net_pnl_twd"] == pytest.approx(926.0)
    assert metrics["official_daily_settlement_leg_marks"] == 2


def test_short_straddle_reuses_cycle_and_daily_ledgers_without_fake_capital_return() -> None:
    plans = pd.DataFrame(
        [
            {
                "entry_date": date(2025, 1, 2),
                "expiry_date": date(2025, 1, 3),
                "calendar_expiry_date": date(2025, 1, 3),
                "option_series": "202501F1",
                "strike": 20_000.0,
                "tx_open": 20_020.0,
                "opening_abs_moneyness_points": 20.0,
                "call_entry_price_points": 100.0,
                "put_entry_price_points": 90.0,
                "holding_sessions": 2,
                "call_entry_source_file": "entry.csv",
                "call_entry_source_sha256": "a" * 64,
                "put_entry_source_file": "entry.csv",
                "put_entry_source_sha256": "a" * 64,
            }
        ]
    )
    contract_rows = {
        (date(2025, 1, 2), "202501F1", 20_000.0, "C"): {
            "settlement": 120.0,
            "close": 121.0,
        },
        (date(2025, 1, 2), "202501F1", 20_000.0, "P"): {
            "settlement": 80.0,
            "close": 79.0,
        },
        (date(2025, 1, 3), "202501F1", 20_000.0, "C"): {
            "close": 130.0,
            "source_file": "expiry.csv",
            "source_sha256": "b" * 64,
        },
        (date(2025, 1, 3), "202501F1", 20_000.0, "P"): {
            "close": 80.0,
            "source_file": "expiry.csv",
            "source_sha256": "b" * 64,
        },
    }
    settlements = {
        (date(2025, 1, 3), "202501F1"): {
            "settlement_price": 20_210.0,
            "source_file": "settlement.parquet",
            "source_sha256": "c" * 64,
        }
    }
    long_cycles, _, _ = _build_cycle_results(
        plans,
        contract_rows,
        fee_per_contract_side_twd=22.0,
        position_side="long",
        official_settlements=settlements,
    )
    short_cycles, short_trades, excluded = _build_cycle_results(
        plans,
        contract_rows,
        fee_per_contract_side_twd=22.0,
        position_side="short",
        official_settlements=settlements,
    )
    _assert_accounting(short_cycles, short_trades)

    assert excluded.empty
    assert short_cycles.iloc[0]["gross_pnl_twd"] == pytest.approx(-1_000.0)
    assert short_cycles.iloc[0]["gross_pnl_twd"] == pytest.approx(
        -long_cycles.iloc[0]["gross_pnl_twd"]
    )
    assert short_cycles.iloc[0]["fee_twd"] == pytest.approx(44.0)
    assert short_cycles.iloc[0]["transaction_tax_twd"] == pytest.approx(30.0)
    assert short_cycles.iloc[0]["net_pnl_twd"] == pytest.approx(-1_074.0)
    assert pd.isna(short_cycles.iloc[0]["required_capital_twd"])
    assert set(short_trades.loc[short_trades["reason"].eq("open_atm_straddle"), "delta_contracts"]) == {-1}
    assert set(
        short_trades.loc[
            short_trades["reason"].eq("official_expiry_cash_settlement"),
            "delta_contracts",
        ]
    ) == {1}

    short_daily, short_metrics = _build_daily_results(
        short_cycles,
        contract_rows,
        [date(2025, 1, 2), date(2025, 1, 3)],
        capital_base_twd=None,
    )
    assert short_daily["net_after_fee_and_tax_twd"].tolist() == pytest.approx(
        [-554.0, -520.0]
    )
    assert short_daily["net_after_fee_and_tax_twd"].sum() == pytest.approx(
        -1_074.0
    )
    assert short_metrics["capital_base_twd"] is None
    assert short_metrics["annualized_sharpe"] is None
