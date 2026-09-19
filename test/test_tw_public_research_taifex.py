from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from stockagent.data.tw_public_research_taifex import (
    SOURCE_COLUMNS,
    TX_COLUMNS,
    TX_FINAL_COLUMN,
    build_taifex_research_features,
)


def test_taifex_research_uses_next_session_and_monthly_tx_only(tmp_path: Path) -> None:
    sessions = [date(2014, 5, 15), date(2014, 5, 16), date(2014, 5, 19)]
    base = tmp_path / "base.parquet"
    pl.DataFrame({
        "date": [day for day in sessions for _ in range(2)],
        "symbol": ["__MARKET__", "2330"] * len(sessions),
        "existing_raw": [1.0] * (len(sessions) * 2),
    }).write_parquet(base)

    put_call = tmp_path / "put_call.parquet"
    pl.DataFrame({
        "date": sessions[:2],
        "available_date": sessions[1:],
        "published_after_close": [True, True],
        "availability_rule": ["next_receipt_verified_taifex_session"] * 2,
        "put_volume": [25.0, 50.0],
        "call_volume": [100.0, 100.0],
        "put_open_interest": [50.0, 75.0],
        "call_open_interest": [100.0, 100.0],
        "put_call_volume_ratio_pct": [25.0, 50.0],
        "put_call_open_interest_ratio_pct": [50.0, 75.0],
    }).write_parquet(put_call)

    futures = tmp_path / "futures.parquet"
    pl.DataFrame({
        "date": [sessions[0], sessions[0], sessions[1], sessions[1], sessions[2]],
        "product": ["TX"] * 5,
        "session": ["一般"] * 5,
        "contract": ["201405", "201406", "201405", "201406", "201406"],
        "volume": [10, 20, 30, 40, 50],
        "open_interest": [100, 200, 300, 400, 500],
        "settlement": [0.0, 9000.0, 0.0, 9010.0, 9020.0],
    }).write_parquet(futures)

    final = tmp_path / "final.parquet"
    pl.DataFrame({
        "settlement_date": [sessions[0], sessions[0]],
        "product": ["TX", "TX"],
        "contract": ["201405", "201405W3"],
        "final_settlement_price": [8990.0, 8999.0],
    }).write_parquet(final)

    output = tmp_path / "research.parquet"
    kwargs = dict(
        base_path=base, history_path=put_call, futures_path=futures,
        final_settlement_path=final, output_path=output,
    )
    receipt = build_taifex_research_features(**kwargs)
    result = pl.read_parquet(output).sort("date", "symbol")
    first_market = result.filter(
        (pl.col("date") == sessions[0]) & (pl.col("symbol") == "__MARKET__")
    )
    second_market = result.filter(
        (pl.col("date") == sessions[1]) & (pl.col("symbol") == "__MARKET__")
    )
    second_stock = result.filter(
        (pl.col("date") == sessions[1]) & (pl.col("symbol") == "2330")
    )
    assert first_market.get_column(SOURCE_COLUMNS["put_volume"]).to_list() == [None]
    assert second_market.get_column(SOURCE_COLUMNS["put_volume"]).to_list() == [25.0]
    assert second_market.get_column(TX_COLUMNS[0]).to_list() == [30.0]
    assert second_market.get_column(TX_COLUMNS[1]).to_list() == [300.0]
    assert second_market.get_column(TX_COLUMNS[2]).to_list() == [9000.0]
    assert second_market.get_column(TX_FINAL_COLUMN).to_list() == [8990.0]
    assert second_stock.get_column(TX_FINAL_COLUMN).to_list() == [None]
    assert receipt["feature_columns"] == [*SOURCE_COLUMNS.values(), *TX_COLUMNS, TX_FINAL_COLUMN]
    inode = output.stat().st_ino
    assert build_taifex_research_features(**kwargs)["reused"] is True
    assert output.stat().st_ino == inode
