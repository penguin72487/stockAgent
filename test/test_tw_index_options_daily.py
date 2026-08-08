from __future__ import annotations

import csv
from pathlib import Path
import zipfile

from scripts.backtest_taifex_classic_opening_straddle_daily import (
    _assert_accounting,
    _build_daily,
)
from stockagent.data.tw_index_futures import (
    build_taifex_index_futures_day_session,
)
from stockagent.data.tw_index_options_daily import (
    build_taifex_monthly_atm_straddles,
    build_taifex_weekly_atm_straddles,
    load_taifex_monthly_atm_straddles,
    load_taifex_weekly_atm_straddles,
)


_FUTURES_HEADER = [
    "交易日期",
    "契約",
    "到期月份(週別)",
    "開盤價",
    "最高價",
    "最低價",
    "收盤價",
    "結算價",
    "成交量",
    "未沖銷契約數",
    "最後最佳買價",
    "最後最佳賣價",
    "歷史最高價",
    "歷史最低價",
    "是否因訊息面暫停交易",
    "交易時段",
    "價差對單式委託成交量",
]
_OPTION_HEADER = [
    "交易日期",
    "契約",
    "到期月份(週別)",
    "履約價",
    "買賣權",
    "開盤價",
    "最高價",
    "最低價",
    "收盤價",
    "結算價",
    "成交量",
    "未沖銷契約數",
    "最後最佳買價",
    "最後最佳賣價",
    "歷史最高價",
    "歷史最低價",
    "是否因訊息面暫停交易",
    "交易時段",
]


def _write(path: Path, header: list[str], rows: list[list[object]]) -> None:
    with path.open("w", encoding="cp950", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def _future_row(
    trading_date: str,
    contract: str,
    open_price: float,
    *,
    session: str = "一般",
) -> list[object]:
    return [
        trading_date,
        "TX",
        contract,
        open_price,
        open_price + 100,
        open_price - 100,
        open_price + 20,
        open_price + 20,
        100,
        0,
        open_price + 19,
        open_price + 21,
        open_price + 100,
        open_price - 100,
        "",
        session,
        0,
    ]


def _option_row(
    trading_date: str,
    series: str,
    strike: int,
    right: str,
    open_price: object,
    close_price: object,
    *,
    session: str = "一般",
    volume: int = 10,
) -> list[object]:
    numeric = float(open_price) if open_price not in ("", "-") else 0.0
    return [
        trading_date,
        "TXO",
        series,
        strike,
        right,
        open_price,
        numeric + 10,
        max(0.1, numeric - 10),
        close_price,
        close_price,
        volume,
        0,
        close_price,
        close_price,
        numeric + 10,
        max(0.1, numeric - 10),
        "",
        session,
    ]


def _futures(tmp_path: Path) -> Path:
    source = tmp_path / "futures.csv"
    _write(
        source,
        _FUTURES_HEADER,
        [
            _future_row("2025/01/02", "202501", 20120),
            _future_row("2025/01/03", "202501", 20220),
            _future_row("2025/01/06", "202501", 20320),
        ],
    )
    return build_taifex_index_futures_day_session(
        [source],
        tmp_path / "futures.parquet",
        products=("TX",),
    )


def test_monthly_atm_uses_tx_open_and_never_falls_back_for_liquidity(
    tmp_path: Path,
) -> None:
    futures = _futures(tmp_path)
    source = tmp_path / "options.csv"
    rows: list[list[object]] = []
    for strike in (20000, 20200, 20400):
        rows.extend(
            [
                _option_row("2025/01/02", "202501", strike, "買權", 100, 110),
                _option_row("2025/01/02", "202501", strike, "賣權", 90, 80),
            ]
        )
    rows.extend(
        [
            _option_row("2025/01/02", "202501W1", 20100, "買權", 99, 109),
            _option_row("2025/01/02", "202501W1", 20100, "賣權", 89, 79),
            _option_row(
                "2025/01/02", "202501", 20200, "買權", 999, 999, session="盤後"
            ),
            _option_row("2025/01/03", "202501", 20200, "買權", 100, 120),
            _option_row("2025/01/03", "202501", 20200, "賣權", "-", 80),
            _option_row("2025/01/03", "202501", 20400, "買權", 90, 100),
            _option_row("2025/01/03", "202501", 20400, "賣權", 95, 85),
            _option_row("2025/01/06", "202501", 20400, "買權", 80, 120),
            _option_row("2025/01/06", "202501", 20400, "賣權", 85, 60),
        ]
    )
    _write(source, _OPTION_HEADER, rows)

    output = build_taifex_monthly_atm_straddles(
        [source], futures, tmp_path / "options.parquet"
    )
    frame = load_taifex_monthly_atm_straddles(output).to_pandas()

    first = frame.loc[frame["date"].astype(str) == "2025-01-02"].iloc[0]
    assert first["option_series"] == "202501"
    assert first["strike"] == 20200
    assert first["call_open"] == 100
    assert bool(first["executable"])

    second = frame.loc[frame["date"].astype(str) == "2025-01-03"].iloc[0]
    assert second["strike"] == 20200
    assert not bool(second["executable"])
    assert "missing_put_open" in second["exclusion_reason"]


def test_legacy_cp950_zip_and_fixed_fee_accounting(tmp_path: Path) -> None:
    futures = _futures(tmp_path)
    source = tmp_path / "legacy.csv"
    session_index = _OPTION_HEADER.index("交易時段")
    header = [value for index, value in enumerate(_OPTION_HEADER) if index != session_index]
    rows = [
        _option_row("2025/01/02", "202501", 20200, "買權", 100, 130),
        _option_row("2025/01/02", "202501", 20200, "賣權", 90, 70),
    ]
    legacy_rows = [
        [value for index, value in enumerate(row) if index != session_index]
        for row in rows
    ]
    _write(source, header, legacy_rows)
    archive = tmp_path / "annual.zip"
    with zipfile.ZipFile(archive, "w") as handle:
        handle.write(source, arcname="Daily_2001_01.csv")

    output = build_taifex_monthly_atm_straddles(
        [archive], futures, tmp_path / "options.parquet"
    )
    source_frame = load_taifex_monthly_atm_straddles(output).to_pandas()
    daily = _build_daily(source_frame, fee_per_contract_side_twd=22.0)
    _assert_accounting(daily, fee=22.0)

    assert len(daily) == 1
    assert daily.iloc[0]["gross_pnl_twd"] == 500.0
    assert daily.iloc[0]["fee_twd"] == 88.0
    assert daily.iloc[0]["net_pnl_twd"] == 412.0


def test_short_straddle_mirrors_gross_pnl_and_finishes_flat(tmp_path: Path) -> None:
    futures = _futures(tmp_path)
    source = tmp_path / "short.csv"
    _write(
        source,
        _OPTION_HEADER,
        [
            _option_row("2025/01/02", "202501", 20200, "買權", 100, 130),
            _option_row("2025/01/02", "202501", 20200, "賣權", 90, 70),
        ],
    )
    output = build_taifex_monthly_atm_straddles(
        [source], futures, tmp_path / "options.parquet"
    )
    source_frame = load_taifex_monthly_atm_straddles(output).to_pandas()
    long_daily = _build_daily(
        source_frame,
        fee_per_contract_side_twd=22.0,
        position_side="long",
    )
    short_daily = _build_daily(
        source_frame,
        fee_per_contract_side_twd=22.0,
        position_side="short",
    )
    _assert_accounting(short_daily, fee=22.0, position_side="short")

    assert short_daily.iloc[0]["entry_option_cashflow_twd"] == 9_500.0
    assert short_daily.iloc[0]["exit_option_cashflow_twd"] == -10_000.0
    assert short_daily.iloc[0]["gross_pnl_twd"] == -500.0
    assert short_daily.iloc[0]["fee_twd"] == 88.0
    assert short_daily.iloc[0]["net_pnl_twd"] == -588.0
    assert short_daily.iloc[0]["final_call_contracts"] == 0
    assert short_daily.iloc[0]["final_put_contracts"] == 0
    assert (
        short_daily.iloc[0]["gross_pnl_twd"]
        == -long_daily.iloc[0]["gross_pnl_twd"]
    )


def test_missing_option_partition_retains_tx_audit_fields(tmp_path: Path) -> None:
    futures = _futures(tmp_path)
    source = tmp_path / "options.csv"
    _write(
        source,
        _OPTION_HEADER,
        [
            _option_row("2025/01/02", "202501", 20200, "買權", 100, 110),
            _option_row("2025/01/02", "202501", 20200, "賣權", 90, 80),
            _option_row("2025/01/06", "202501", 20400, "買權", 80, 120),
            _option_row("2025/01/06", "202501", 20400, "賣權", 85, 60),
        ],
    )
    output = build_taifex_monthly_atm_straddles(
        [source], futures, tmp_path / "options.parquet"
    )
    frame = load_taifex_monthly_atm_straddles(output).to_pandas()
    missing = frame.loc[frame["date"].astype(str) == "2025-01-03"].iloc[0]
    assert missing["exclusion_reason"] == "missing_txo_daily_partition"
    assert missing["tx_contract_month"] == "202501"
    assert missing["tx_open"] == 20220
    assert not bool(missing["executable"])


def test_weekly_selects_nearest_expiry_before_price_or_liquidity(
    tmp_path: Path,
) -> None:
    futures = _futures(tmp_path)
    source = tmp_path / "weekly.csv"
    rows = [
        _option_row("2025/01/02", "202501W1", 20200, "買權", 100, 110),
        _option_row("2025/01/02", "202501W1", 20200, "賣權", 90, 80),
        _option_row(
            "2025/01/02", "202501W2", 20100, "買權", 10, 50, volume=10000
        ),
        _option_row(
            "2025/01/02", "202501W2", 20100, "賣權", 10, 50, volume=10000
        ),
        _option_row("2025/01/03", "202501W1", 20200, "買權", 100, 120),
        _option_row("2025/01/03", "202501W1", 20200, "賣權", "-", 80),
        _option_row("2025/01/03", "202501W2", 20200, "買權", 90, 100),
        _option_row("2025/01/03", "202501W2", 20200, "賣權", 95, 85),
    ]
    _write(source, _OPTION_HEADER, rows)

    output = build_taifex_weekly_atm_straddles(
        [source], futures, tmp_path / "weekly.parquet"
    )
    frame = load_taifex_weekly_atm_straddles(output).to_pandas()

    first = frame.loc[frame["date"].astype(str) == "2025-01-02"].iloc[0]
    assert first["option_series"] == "202501W1"
    assert first["strike"] == 20200
    assert bool(first["executable"])

    second = frame.loc[frame["date"].astype(str) == "2025-01-03"].iloc[0]
    assert second["option_series"] == "202501W1"
    assert not bool(second["executable"])
    assert "missing_put_open" in second["exclusion_reason"]


def test_weekly_selects_friday_before_later_wednesday(tmp_path: Path) -> None:
    futures = _futures(tmp_path)
    source = tmp_path / "wednesday_friday.csv"
    rows = [
        _option_row("2025/01/02", "202501F1", 20200, "買權", 100, 110),
        _option_row("2025/01/02", "202501F1", 20200, "賣權", 90, 80),
        _option_row("2025/01/02", "202501W2", 20200, "買權", 10, 50),
        _option_row("2025/01/02", "202501W2", 20200, "賣權", 10, 50),
    ]
    _write(source, _OPTION_HEADER, rows)

    output = build_taifex_weekly_atm_straddles(
        [source], futures, tmp_path / "weekly.parquet"
    )
    frame = load_taifex_weekly_atm_straddles(output).to_pandas()

    selected = frame.loc[frame["date"].astype(str) == "2025-01-02"].iloc[0]
    assert selected["option_series"] == "202501F1"
    assert selected["strike"] == 20200
