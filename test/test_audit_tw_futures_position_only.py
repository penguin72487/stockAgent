import polars as pl
import pytest

from scripts.audit_tw_futures_position_only import candidates, release_rows


def _row(date, physical, *, volume=0, executable=False, oi=0, mark=0, terminal=False):
    return {"product": "CJ2", "date": date, "physical_contract": physical,
            "official_volume": volume, "outright_volume": volume, "executable": executable,
            "open_interest": oi, "valuation_settlement": mark, "cash_settlement": terminal,
            "corporate_identity_candidate_only": True}


def test_position_without_new_trade_remains_an_account_observation():
    universe = {"contract": "tw_futures_universe_release_scope_v1", "products": [
        {"product": "CJ2", "positive_daily_rows": 0},
        {"product": "CV1", "positive_daily_rows": None},
        {"product": "TX", "positive_daily_rows": 4}]}
    assert [r["product"] for r in candidates(universe)] == ["CJ2", "CV1"]
    frame = pl.DataFrame([_row("2011-11-07", "CJ2-201112", oi=6, mark=18.45),
                          _row("2011-12-21", "CJ2-201112", oi=6, mark=16.0, terminal=True)])
    result = release_rows(frame, ["CJ2"])[0]
    assert result["positive_outright_rows"] == result["executable_rows"] == 0
    assert result["positive_open_interest_rows"] == result["positive_valuation_rows"] == 2
    assert result["cash_settlement_rows"] == 1
    assert result["candidate_identity_rows"] == 2


def test_ambiguous_physical_key_and_absent_product_are_rejected():
    frame = pl.DataFrame([_row("2011-11-07", "CJ2-201112"),
                          _row("2011-11-07", "CJ2-201112")])
    with pytest.raises(ValueError, match="ambiguous"):
        release_rows(frame, ["CJ2"])
    with pytest.raises(ValueError, match="no accounting rows"):
        release_rows(frame, ["CJ2", "EB1"])
