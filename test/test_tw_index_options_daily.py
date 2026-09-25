from __future__ import annotations

import csv
from datetime import date
import math
from pathlib import Path
import zipfile

import pyarrow.parquet as pq

from scripts.download_taifex_option_daily_history import (
    _builder_fingerprint,
    _futures_contract_open_by_date,
    _futures_open_by_date,
    _merge_full_chain_shards,
    _prepare_atm_source_projections,
    _prepare_full_chain_shards,
)
from scripts.taifex_daily_download_common import sha256_path

from scripts.backtest_taifex_classic_opening_straddle_daily import (
    _assert_accounting,
    _build_daily,
)
from stockagent.data.tw_index_futures import (
    build_taifex_index_futures_day_session,
)
from stockagent.data.tw_index_options_daily import (
    _read_txo_rows,
    build_taifex_opening_atm_straddles,
    build_taifex_monthly_atm_straddles,
    build_taifex_option_full_chain,
    build_taifex_weekly_atm_straddles,
    iter_taifex_option_daily_rows,
    load_taifex_monthly_atm_straddles,
    load_taifex_weekly_atm_straddles,
)


def test_atm_source_cache_matches_direct_and_invalidates_affected_futures(tmp_path: Path) -> None:
    futures = _futures(tmp_path)
    sources = []
    for suffix, day, strike in (
        ("02", "2025/01/02", 20200),
        ("03", "2025/01/03", 20300),
    ):
        source = tmp_path / f"option_{suffix}.csv"
        _write(source, _OPTION_HEADER, [
            _option_row(day, "202501", strike, "買權", 100, 110),
            _option_row(day, "202501", strike, "賣權", 90, 80),
        ])
        sources.append(source)
    manifest = [
        {"path": str(source), "sha256": sha256_path(source)}
        for source in sources
    ]
    kwargs = {
        "scope": "monthly",
        "tx_by_date": _futures_contract_open_by_date(futures),
        "builder_fingerprint": _builder_fingerprint(),
    }
    cache = tmp_path / "atm_cache"
    projections, rebuilt = _prepare_atm_source_projections(manifest, cache, **kwargs)
    assert rebuilt == 2
    direct = build_taifex_opening_atm_straddles(
        sources, futures, tmp_path / "direct.parquet", series_scope="monthly"
    )
    cached = build_taifex_opening_atm_straddles(
        sources, futures, tmp_path / "cached.parquet", series_scope="monthly",
        source_projections=projections,
    )
    assert sha256_path(cached) == sha256_path(direct)
    assert pq.read_table(cached).num_rows == 2

    reused, rebuilt = _prepare_atm_source_projections(manifest, cache, **kwargs)
    assert rebuilt == 0
    assert reused == projections
    cache_file = next((cache / "monthly").glob("*.json"))
    cache_file.write_text("{corrupt", encoding="utf-8")
    _repaired, rebuilt = _prepare_atm_source_projections(manifest, cache, **kwargs)
    assert rebuilt == 1

    changed_source = tmp_path / "changed_futures.csv"
    _write(changed_source, _FUTURES_HEADER, [
        _future_row("2025/01/02", "202501", 20120),
        _future_row("2025/01/03", "202501", 20420),
        _future_row("2025/01/06", "202501", 30320),
    ])
    changed_futures = build_taifex_index_futures_day_session(
        [changed_source], tmp_path / "changed_futures.parquet", products=("TX",)
    )
    changed_kwargs = {
        **kwargs,
        "tx_by_date": _futures_contract_open_by_date(changed_futures),
    }
    changed, rebuilt = _prepare_atm_source_projections(
        manifest, cache, **changed_kwargs
    )
    assert rebuilt == 1  # Only Jan 3 changed; Jan 6 has no option receipt.
    changed_cached = build_taifex_opening_atm_straddles(
        sources, changed_futures, tmp_path / "changed_cached.parquet",
        series_scope="monthly", source_projections=changed,
    )
    changed_direct = build_taifex_opening_atm_straddles(
        sources, changed_futures, tmp_path / "changed_direct.parquet",
        series_scope="monthly",
    )
    assert sha256_path(changed_cached) == sha256_path(changed_direct)

    _write(sources[0], _OPTION_HEADER, [
        _option_row("2025/01/02", "202501", 20200, "買權", 110, 120),
        _option_row("2025/01/02", "202501", 20200, "賣權", 90, 80),
    ])
    changed_manifest = [
        {"path": str(source), "sha256": sha256_path(source)}
        for source in sources
    ]
    changed, rebuilt = _prepare_atm_source_projections(
        changed_manifest, cache, **changed_kwargs
    )
    assert rebuilt == 1
    changed_cached = build_taifex_opening_atm_straddles(
        sources, changed_futures, tmp_path / "source_changed_cached.parquet",
        series_scope="monthly", source_projections=changed,
    )
    changed_direct = build_taifex_opening_atm_straddles(
        sources, changed_futures, tmp_path / "source_changed_direct.parquet",
        series_scope="monthly",
    )
    assert sha256_path(changed_cached) == sha256_path(changed_direct)


def test_weekly_atm_cache_preserves_monthly_only_gap_reason(tmp_path: Path) -> None:
    futures = _futures(tmp_path)
    sources = []
    for suffix, day, series in (
        ("02", "2025/01/02", "202501W1"),
        ("03", "2025/01/03", "202501"),
        ("06", "2025/01/06", "202501W2"),
    ):
        source = tmp_path / f"weekly_{suffix}.csv"
        _write(source, _OPTION_HEADER, [
            _option_row(day, series, 20200, "買權", 100, 110),
            _option_row(day, series, 20200, "賣權", 90, 80),
        ])
        sources.append(source)
    manifest = [
        {"path": str(source), "sha256": sha256_path(source)}
        for source in sources
    ]
    projected, rebuilt = _prepare_atm_source_projections(
        manifest, tmp_path / "cache", scope="weekly",
        tx_by_date=_futures_contract_open_by_date(futures),
        builder_fingerprint=_builder_fingerprint(),
    )
    assert rebuilt == 3
    direct = build_taifex_opening_atm_straddles(
        sources, futures, tmp_path / "weekly_direct.parquet", series_scope="weekly"
    )
    cached = build_taifex_opening_atm_straddles(
        sources, futures, tmp_path / "weekly_cached.parquet", series_scope="weekly",
        source_projections=projected,
    )
    assert sha256_path(cached) == sha256_path(direct)
    frame = pq.read_table(cached).to_pandas()
    gap = frame.loc[frame["date"].astype(str) == "2025-01-03"].iloc[0]
    assert gap["exclusion_reason"] == "no_weekly_txo_listing"


def test_full_chain_shards_match_direct_build_and_rebuild_only_changed_futures_dates(
    tmp_path: Path,
) -> None:
    futures = _futures(tmp_path)
    sources = []
    for day, strike in (("2025/01/02", 20200), ("2025/01/03", 20300)):
        source = tmp_path / f"{day[-2:]}.csv"
        _write(source, _OPTION_HEADER, [
            _option_row(day, "202501", strike, "買權", 100, 110),
            _option_row(day, "202501", strike, "賣權", 90, 80),
        ])
        sources.append(source)
    receipt_manifest = [
        {"path": str(source), "sha256": sha256_path(source)}
        for source in sources
    ]
    fingerprint = _builder_fingerprint()
    output_root = tmp_path / "normalized"
    cache_root = tmp_path / "node_cache"
    opens = _futures_open_by_date(futures)
    shards, rebuilt = _prepare_full_chain_shards(
        receipt_manifest, futures, cache_root,
        scope="monthly", tx_open_by_date=opens,
        builder_fingerprint=fingerprint,
    )
    assert rebuilt == 2
    merged = _merge_full_chain_shards(
        shards, output_root / "monthly_full_chain.parquet", scope="monthly"
    )
    direct = build_taifex_option_full_chain(
        sources, futures, tmp_path / "direct.parquet", series_scope="monthly"
    )
    assert pq.read_table(merged).equals(pq.read_table(direct), check_metadata=True)
    reused, rebuilt = _prepare_full_chain_shards(
        receipt_manifest, futures, cache_root,
        scope="monthly", tx_open_by_date=opens,
        builder_fingerprint=fingerprint,
    )
    assert rebuilt == 0
    assert reused == shards

    Path(str(reused[0]["shard_path"])).write_bytes(b"corrupt")
    repaired, rebuilt = _prepare_full_chain_shards(
        receipt_manifest, futures, cache_root,
        scope="monthly", tx_open_by_date=opens,
        builder_fingerprint=fingerprint,
    )
    assert rebuilt == 1
    assert pq.read_table(_merge_full_chain_shards(
        repaired, output_root / "repaired.parquet", scope="monthly"
    )).equals(pq.read_table(direct), check_metadata=True)

    changed_source = tmp_path / "changed_futures.csv"
    _write(changed_source, _FUTURES_HEADER, [
        _future_row("2025/01/02", "202501", 20120),
        _future_row("2025/01/03", "202501", 20420),
        _future_row("2025/01/06", "202501", 30320),
    ])
    changed_futures = build_taifex_index_futures_day_session(
        [changed_source], tmp_path / "changed_futures.parquet", products=("TX",)
    )
    changed_shards, rebuilt = _prepare_full_chain_shards(
        receipt_manifest, changed_futures, cache_root,
        scope="monthly", tx_open_by_date=_futures_open_by_date(changed_futures),
        builder_fingerprint=fingerprint,
    )
    assert rebuilt == 1  # Jan 6 is outside both receipts; only Jan 3 changed.
    changed_merged = _merge_full_chain_shards(
        changed_shards, output_root / "changed.parquet", scope="monthly"
    )
    changed_direct = build_taifex_option_full_chain(
        sources, changed_futures, tmp_path / "changed_direct.parquet",
        series_scope="monthly",
    )
    assert pq.read_table(changed_merged).equals(
        pq.read_table(changed_direct), check_metadata=True
    )


def test_full_chain_shard_empty_scope_and_overlap_fail_closed(tmp_path: Path) -> None:
    futures = _futures(tmp_path)
    sources = []
    for name in ("first", "second"):
        source = tmp_path / f"{name}.csv"
        _write(source, _OPTION_HEADER, [
            _option_row("2025/01/02", "202501", 20200, "買權", 100, 110),
        ])
        sources.append(source)
    manifest = [
        {"path": str(source), "sha256": sha256_path(source)}
        for source in sources
    ]
    kwargs = {
        "tx_open_by_date": _futures_open_by_date(futures),
        "builder_fingerprint": _builder_fingerprint(),
    }
    monthly, rebuilt = _prepare_full_chain_shards(
        manifest, futures, tmp_path / "node_cache", scope="monthly", **kwargs
    )
    assert rebuilt == 2
    import pytest

    with pytest.raises(ValueError, match="overlapping full-chain receipts"):
        _merge_full_chain_shards(monthly, tmp_path / "overlap.parquet", scope="monthly")
    weekly, rebuilt = _prepare_full_chain_shards(
        manifest, futures, tmp_path / "node_cache", scope="weekly", **kwargs
    )
    assert rebuilt == 2
    with pytest.raises(ValueError, match="no normalized TXO weekly"):
        _merge_full_chain_shards(weekly[:1], tmp_path / "empty.parquet", scope="weekly")


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


def test_option_csv_reader_preserves_duplicate_header_and_short_row(
    tmp_path: Path,
) -> None:
    source = tmp_path / "options.csv"
    header = [
        "交易日期", "契約", "到期月份(週別)", "履約價", "買賣權",
        "開盤價", "收盤價", "結算價", "成交量",
        "最後最佳買價", "最後最佳買價", "最後最佳賣價",
    ]
    _write(
        source,
        header,
        [
            ["2025/01/02", "TXO", "202501", 20000, "買權", 100, 110,
             105, 3, 1, 2, 3, "ignored extra field"],
            [],
            ["2025/01/03", "TXO", "202501", 20000, "賣權", 90, 80,
             85, 2],
        ],
    )
    by_date, all_dates = _read_txo_rows(source, series_scope="monthly")

    assert all_dates == {date(2025, 1, 2), date(2025, 1, 3)}
    assert by_date[date(2025, 1, 2)][("202501", 20000.0, "C")].last_bid == 2.0
    short = by_date[date(2025, 1, 3)][("202501", 20000.0, "P")]
    assert math.isnan(short.last_bid)
    assert math.isnan(short.last_ask)


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


def test_surface_iterator_reads_monthly_and_weekly_in_one_parser_pass(
    tmp_path: Path,
) -> None:
    source = tmp_path / "surface.csv"
    rows = [
        _option_row("2025/01/02", "202501", 20200, "買權", 100, 110),
        _option_row("2025/01/02", "202501", 20200, "賣權", 90, 80),
        _option_row("2025/01/02", "202501W1", 20200, "買權", 50, 60),
        _option_row("2025/01/02", "202501W1", 20200, "賣權", 40, 30),
        _option_row("2025/01/03", "202501W1", 20300, "買權", 45, 55),
    ]
    _write(source, _OPTION_HEADER, rows)

    selected = list(
        iter_taifex_option_daily_rows(
            [source],
            trading_dates={date(2025, 1, 2)},
        )
    )

    assert len(selected) == 4
    assert {row["series_scope"] for row in selected} == {"monthly", "weekly"}
    assert {row["option_series"] for row in selected} == {"202501", "202501W1"}
