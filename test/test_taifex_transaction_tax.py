from __future__ import annotations

from datetime import date

import pytest

from stockagent.research.taifex_transaction_tax import (
    option_cash_settlement_transaction_tax_twd,
    option_premium_transaction_tax_twd,
    round_taifex_tax_twd,
    stock_index_futures_tax_rate,
)


def test_taifex_tax_rounds_each_contract_half_up_to_twd() -> None:
    assert round_taifex_tax_twd(2.49) == 2.0
    assert round_taifex_tax_twd(2.50) == 3.0
    assert option_premium_transaction_tax_twd(
        50.0,
        multiplier_twd_per_point=50.0,
    ) == 3.0


def test_taifex_published_txo_tax_example() -> None:
    assert option_premium_transaction_tax_twd(
        30.5,
        multiplier_twd_per_point=50.0,
    ) == 2.0
    assert option_cash_settlement_transaction_tax_twd(
        7_808.0,
        settlement_date=date(2025, 1, 1),
        multiplier_twd_per_point=50.0,
    ) == 8.0


def test_stock_index_futures_tax_schedule_changes_on_2013_04_01() -> None:
    assert stock_index_futures_tax_rate(date(2013, 3, 31)) == pytest.approx(
        0.00004
    )
    assert stock_index_futures_tax_rate(date(2013, 4, 1)) == pytest.approx(
        0.00002
    )


def test_option_cash_settlement_uses_index_contract_amount() -> None:
    assert option_cash_settlement_transaction_tax_twd(
        17_123.4,
        settlement_date=date(2012, 12, 19),
        multiplier_twd_per_point=50.0,
    ) == 34.0
    assert option_cash_settlement_transaction_tax_twd(
        17_123.4,
        settlement_date=date(2025, 12, 3),
        multiplier_twd_per_point=50.0,
    ) == 17.0


def test_unverified_early_tax_schedule_fails_closed() -> None:
    with pytest.raises(ValueError, match="not verified"):
        stock_index_futures_tax_rate(date(2008, 10, 5))
