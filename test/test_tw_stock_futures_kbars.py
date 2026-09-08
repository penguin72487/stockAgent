from dataclasses import replace
from datetime import date, datetime, timedelta
import json
import sys

import numpy as np
import polars as pl
import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from downloader.download_shioaji_historical_market_data import (
    HistoryContract, RECEIPT_SCHEMA_VERSION, SOURCE, _kbar_paths, _write_inventory,
)
from scripts import build_tw_stock_futures_0900_entries as builder
from stockagent.data.tw_stock_futures_day_trade import select_causal_front_stock_futures_candidates
from stockagent.data.tw_stock_futures_kbars import KBAR_SOURCE, normalize_futures_kbars
from stockagent.data.tw_stock_futures_minute import load_futures_minute_tape
from stockagent.data.tw_futures_portfolio_daily import TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION
from test_tw_stock_futures_day_trade import _candidate


def kbars():
    times = [datetime(2026, 9, 3, h, m) for h, m in [(8, 46), (8, 47), (13, 20), (13, 25), (13, 30), (13, 31)]]
    return pl.DataFrame({
        "ts": pl.Series(times).cast(pl.Datetime("ns")).cast(pl.Int64),
        "trading_date": [date(2026, 9, 3)] * 6,
        "query_contract": ["CDFI6"] * 6, "security_type": ["FUT"] * 6,
        "Open": [100., 999., 103., 106., 107., 888.],
        "High": [102., 999., 103., 106., 107., 888.],
        "Low": [100., 999., 103., 106., 107., 888.],
        "Close": [102., 999., 103., 106., 107., 888.],
        "Volume": [4, 2, 2, 2, 2, 2],
        "Amount": [404., 1998., 206., 212., 214., 1776.],
    })


def test_kbars_preserve_completed_minute_boundary_vwap_and_contract_volume():
    frame = normalize_futures_kbars(kbars(), code="CDFI6", physical_contract="CDF:202609", source_sha256="a" * 64)
    assert frame["minute"].to_list() == [526, 800, 805, 810]
    assert frame["vwap"].to_list() == [101, 103, 106, 107]
    assert frame["volume"].to_list() == [4, 2, 2, 2]  # No stock-lot conversion or B/S division.


@pytest.mark.parametrize("case", ["wrong_amount_scale", "fractional_volume", "tick_timestamp", "duplicate", "cash_stock", "null"])
def test_invalid_kbars_are_rejected(case):
    frame = kbars()
    if case == "wrong_amount_scale":
        frame = frame.with_columns(pl.col("Amount") * 2000)
    elif case == "fractional_volume":
        frame = frame.with_columns(pl.col("Volume") + 0.5)
    elif case == "tick_timestamp":
        frame = frame.with_columns(pl.col("ts") + 1)
    elif case == "duplicate":
        frame = pl.concat([frame, frame.head(1)])
    elif case == "cash_stock":
        frame = frame.with_columns(pl.lit("STK").alias("security_type"))
    else:
        frame = frame.with_columns(pl.lit(None).alias("Amount"))
    with pytest.raises(ValueError):
        normalize_futures_kbars(frame, code="CDFI6", physical_contract="CDF:202609", source_sha256="a" * 64)


@pytest.mark.parametrize("case", ["valid", "check_only", "missing", "corrupt", "partial_session", "unmapped_alias", "zero_event_volume", "overlap_identical", "overlap_changed", "overlap_empty"])
def test_existing_builder_accepts_only_verified_kbars_without_tick_dependencies(tmp_path, monkeypatch, case):
    import downloader.download_shioaji_historical_market_data as collector

    root = tmp_path / "source"
    output = tmp_path / "output"
    day = date(2026, 9, 3)
    daily_frame = pl.DataFrame([_candidate(day=day, product="CDF", prior_volume=100,
                                         current_volume=0 if case == "overlap_empty" else 200)])
    daily = tmp_path / "daily" / "continuous_daily.parquet"
    atomic_write_parquet(daily, daily_frame)
    daily_digest = sha256_file(daily)
    atomic_write_json(daily.with_name("manifest.json"), {
        "contract_version": TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION,
        "outputs": {"continuous_daily": {"sha256": daily_digest}},
    })
    row = HistoryContract(collection="exact_futures", priority=2, security_type="FUT",
                          asset_class="futures", code="CDFI6", root="CDF", name="TSMC",
                          exchange="TAIFEX", begin_date=day, end_date=day,
                          delivery_date=date(2026, 9, 16), delivery_month="202609")
    if case == "unmapped_alias":
        row = replace(row, code="CDFR1")
    if case.startswith("overlap_"):
        row = replace(row, end_date=day + timedelta(days=1))
    _write_inventory(root, [row], completed_session=day)
    data_path, receipt_path = _kbar_paths(root, row, day, day)
    raw = kbars()
    if case == "zero_event_volume":
        # There are genuine trades outside the required event minutes; no fills remain valid.
        raw = raw.filter(pl.col("Open") == 999)
    if case == "overlap_empty":
        raw = raw.with_columns(pl.lit(0).alias("Volume"), pl.lit(0.).alias("Amount"))
    if case != "missing":
        atomic_write_parquet(data_path, raw)
        atomic_write_json(receipt_path, {
            "schema_version": RECEIPT_SCHEMA_VERSION, "source": SOURCE, "method": "kbars",
            "status": "complete", "contract": row.code, "security_type": "FUT",
            "start": str(day), "end": str(day), "rows": raw.height,
            "observed_trading_dates": [str(day)], "sha256": sha256_file(data_path),
            "finished_at_utc": "2026-09-03T04:00:00Z" if case == "partial_session" else "2026-09-03T06:00:00Z",
        })
    if case == "corrupt":
        data_path.write_bytes(b"corrupt")
    if case.startswith("overlap_"):
        duplicate_data, duplicate_receipt = _kbar_paths(root, row, day, row.end_date)
        duplicate = raw
        if case == "overlap_changed":
            # Change a traded minute outside the execution tape too: the two
            # complete source observations of this day must still agree.
            duplicate = raw.with_columns(
                pl.when(pl.col("Open") == 999).then(pl.col("Volume") * 2)
                .otherwise(pl.col("Volume")).alias("Volume"),
                pl.when(pl.col("Open") == 999).then(pl.col("Amount") * 2)
                .otherwise(pl.col("Amount")).alias("Amount"),
            )
        atomic_write_parquet(duplicate_data, duplicate)
        receipt = json.loads(receipt_path.read_text())
        receipt.update(end=str(row.end_date), sha256=sha256_file(duplicate_data),
                       finished_at_utc="2026-09-04T06:00:00Z")
        if case == "overlap_empty":
            receipt.update(status="source_empty", rows=0, observed_trading_dates=[])
        atomic_write_json(duplicate_receipt, receipt)
    def forbidden(*args, **kwargs):
        pytest.fail("KBar path must not inspect transaction archives or ticks")
    monkeypatch.setattr(builder, "_archive_inventory", forbidden)
    monkeypatch.setattr(builder, "_parse_zip", forbidden)
    monkeypatch.setattr(collector, "_tick_paths", forbidden)
    argv = ["builder", "--minute-root", str(root), "--execution-policy", "scheduled_0846",
            "--daily-data-path", str(daily), "--output-dir", str(output), "--start-date", str(day)]
    if case == "check_only":
        argv.append("--check-only")
    monkeypatch.setattr(sys, "argv", argv)
    if case in {"corrupt", "partial_session", "overlap_changed"}:
        with pytest.raises(ValueError):
            builder.main()
        assert not output.exists()
        return
    result = builder.main()
    if case in {"missing", "unmapped_alias"}:
        assert result == 2 and not (output / "minutes.parquet").exists()
        return
    assert result == 0
    if case == "check_only":
        assert not output.exists()
        return
    path = output / "minutes.parquet"
    selected = select_causal_front_stock_futures_candidates(daily_frame)
    tape, receipt = load_futures_minute_tape(path, selected, np.array([str(day)], dtype="datetime64[D]"),
                                            ("2330",), daily_sha256=daily_digest, fee=40, participation=0.5)
    assert receipt["source_kind"] == KBAR_SOURCE
    assert tape.shape == (1, 1, 2, 63)
    if case in {"zero_event_volume", "overlap_empty"}:
        assert not tape[..., 3:].any()
    else:
        assert tape[0, 0, 0, 3] == 101 and tape[0, 0, 0, 7] == 2
        # A matching date alone cannot prove a different physical contract was queried.
        selected = selected.with_columns(pl.lit("CDF:202610").alias("physical_contract"))
        with pytest.raises(ValueError, match="selected contract-days"):
            load_futures_minute_tape(path, selected, np.array([str(day)], dtype="datetime64[D]"),
                                    ("2330",), daily_sha256=daily_digest, fee=40, participation=0.5)
