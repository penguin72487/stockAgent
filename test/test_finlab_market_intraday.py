from datetime import UTC, date, datetime
import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import polars as pl
from finlab.exceptions import DataError

from scripts.download_finlab_market_intraday import (
    _eligible, _open_index, _record, _summarize, _target_count,
    annotate_daily_close_coverage, load_universe, session_days, sync_market,
)
from scripts.download_finlab_history import safe_stem


def _fixture(root: Path) -> None:
    stocks = root / "stocks"
    stocks.mkdir(parents=True)
    pl.DataFrame({
        "code": ["2330", "2867"], "name": ["TSMC", "Former"],
        "market": ["twse", "twse"], "security_type": ["stock", "stock"],
        "source": ["official", "official"],
    }).write_csv(stocks / "symbols.csv")
    for symbol in ("2330", "2867"):
        pl.DataFrame({"date": [date(2026, 8, 19), date(2026, 9, 24)]
                      if symbol == "2330" else [date(2026, 8, 19)]}).write_parquet(
            stocks / f"{symbol}_features.parquet"
        )
    pl.DataFrame({"公司代號": ["2330"], "date": ["2026-09-24"]}).write_parquet(
        root / "twse_listed_company_basic.parquet")
    pl.DataFrame({"SecuritiesCompanyCode": ["9999"], "date": ["2026-09-24"]}).write_parquet(
        root / "tpex_basic_company.parquet")
    pl.DataFrame({"symbol": ["2867"], "date": ["2026-09-01"]}).write_parquet(
        root / "twse_delisted_company.parquet")
    pl.DataFrame({"symbol": ["9998"], "date": ["2020-01-01"]}).write_parquet(
        root / "tpex_delisted_company.parquet")
    pl.DataFrame({"date": ["2026-08-19", "2026-09-24"]}).write_parquet(
        root / "twse_market_index.parquet")


def test_official_current_snapshot_does_not_relabel_former_symbol_as_current(tmp_path):
    _fixture(tmp_path)
    universe = load_universe(tmp_path, start=date(2026, 8, 19))
    by_symbol = {item["symbol"]: item for item in universe}
    days = session_days(tmp_path, start=date(2026, 8, 19), end=date(2026, 9, 24))
    assert by_symbol["2867"]["current"] is False
    assert _eligible(by_symbol["2867"], date(2026, 8, 19))
    assert not _eligible(by_symbol["2867"], date(2026, 9, 24))
    assert _target_count(by_symbol["2867"], days) == 1
    assert by_symbol["9998"]["first_local_daily_date"] is None
    assert _target_count(by_symbol["9998"], days) == 0


def test_pre_2009_sessions_come_from_official_twse_and_tpex_daily_rows(tmp_path):
    _fixture(tmp_path)
    pl.DataFrame({"date": ["2004-02-11"]}).write_parquet(
        tmp_path / "twse_daily_ohlcv.parquet"
    )
    pl.DataFrame({"date": ["2003-08-01"]}).write_parquet(
        tmp_path / "tpex_daily_ohlcv.parquet"
    )
    days = session_days(tmp_path, start=date(2003, 8, 1), end=date(2026, 9, 24))
    assert days == [date(2003, 8, 1), date(2004, 2, 11),
                    date(2026, 8, 19), date(2026, 9, 24)]


def test_market_sync_counts_per_symbol_receipts_without_catalog_example_limit(tmp_path):
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    output.mkdir()
    calls = []

    def fetch(root, key, day, *, now):
        calls.append((key, day))
        return {"status": "downloaded_unverified_for_pit", "rows": 17,
                "parquet_size_bytes": 1234}

    with patch("scripts.download_finlab_market_intraday.derive_partition", return_value={
        "status": "derived_unverified_for_pit", "rows": 5, "parquet_size_bytes": 500,
    }), patch("scripts.download_finlab_market_intraday.credential_available", return_value=True), patch(
        "scripts.download_finlab_market_intraday.quota_room_mb", return_value=(1000.0, 5000.0)
    ):
        summary = sync_market(output, public, start=date(2026, 8, 19),
                              end=date(2026, 9, 24), limit=2,
                              reserve_mb=50, minimum_free_gb=0,
                              now=datetime(2026, 9, 25, tzinfo=UTC), fetch=fetch)
    assert all(key.startswith("tw_tick:") for key, _ in calls)
    assert calls[0] == ("tw_tick:2330", date(2026, 9, 24))
    assert summary["universe_symbols"] == 3
    assert summary["former_symbols"] == 2
    assert summary["by_kind"]["tw_minute"]["receipted_partitions"] == 2
    assert summary["by_kind"]["tw_tick"]["receipted_partitions"] == 2
    assert summary["by_kind"]["tw_minute"]["source_kind"] == "derived_from_tw_tick"
    assert next(row for row in summary["symbols"] if row["key"] == "tw_minute:2867")["target_partitions"] == 1
    assert summary["completion_eta"] is None


def test_real_market_path_calls_only_tick_api_and_writes_derived_minute(tmp_path):
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    output.mkdir()
    tick = pd.DataFrame({
        "stock_id": ["2330", "2330"],
        "trade_date": ["2026-09-24", "2026-09-24"],
        "timestamp": pd.to_datetime(["2026-09-24 09:00:01+08:00",
                                     "2026-09-24 09:00:02+08:00"]),
        "sequence": [0, 1], "close": [100.0, 101.0],
        "volume": [1, 2], "session": ["regular", "regular"],
    })
    with patch("scripts.download_finlab_market_intraday.credential_available", return_value=True), patch(
        "scripts.download_finlab_market_intraday.quota_room_mb", return_value=(1000.0, 5000.0)
    ), patch("finlab.data.get", return_value=tick) as get:
        summary = sync_market(output, public, start=date(2026, 9, 24),
                              end=date(2026, 9, 24), limit=1,
                              reserve_mb=50, minimum_free_gb=0,
                              now=datetime(2026, 9, 25, tzinfo=UTC))
    assert get.call_count == 1
    assert get.call_args.args[0] == "tw_tick:2330"
    assert summary["by_kind"]["tw_tick"]["receipted_partitions"] == 1
    assert summary["by_kind"]["tw_minute"]["receipted_partitions"] == 1
    assert summary["by_kind"]["tw_minute"]["rows"] == 1
    assert list((output / "intraday/derived_minute/receipts").glob("*/*.json"))


def test_new_exchange_session_keeps_older_backfill_cursor(tmp_path):
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    output.mkdir()
    with patch("scripts.download_finlab_market_intraday.credential_available", return_value=True), patch(
        "scripts.download_finlab_market_intraday.quota_room_mb", return_value=(0.0, 5000.0)
    ):
        sync_market(output, public, start=date(2026, 8, 19),
                    end=date(2026, 8, 19), limit=1, reserve_mb=50,
                    minimum_free_gb=0, now=datetime(2026, 9, 25, tzinfo=UTC))
        first = json.loads((output / "intraday/market_cursor.json").read_text())
        sync_market(output, public, start=date(2026, 8, 19),
                    end=date(2026, 9, 24), limit=1, reserve_mb=50,
                    minimum_free_gb=0, now=datetime(2026, 9, 25, tzinfo=UTC))
    second = json.loads((output / "intraday/market_cursor.json").read_text())
    assert first["oldest"] == second["oldest"]
    assert second["newest"] == first["newest"] + 3
    assert [row["day"] for row in second["recent_queue"]] == ["2026-09-24", "2026-08-19"]


def test_not_ready_partition_retries_after_next_quota_cycle(tmp_path):
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    output.mkdir()
    calls = []

    def fetch(root, key, day, *, now):
        calls.append((key, day))
        if len(calls) == 1:
            raise DataError("not_ready: partition not published")
        return {"status": "downloaded_unverified_for_pit", "rows": 2,
                "parquet_size_bytes": 100}

    with patch("scripts.download_finlab_market_intraday.credential_available", return_value=True), patch(
        "scripts.download_finlab_market_intraday.quota_room_mb", return_value=(1000.0, 5000.0)
    ):
        sync_market(output, public, start=date(2026, 8, 19),
                    end=date(2026, 9, 24), limit=1, reserve_mb=50,
                    minimum_free_gb=0, now=datetime(2026, 9, 25, tzinfo=UTC), fetch=fetch)
        sync_market(output, public, start=date(2026, 8, 19),
                    end=date(2026, 9, 24), limit=1, reserve_mb=50,
                    minimum_free_gb=0, now=datetime(2026, 9, 26, tzinfo=UTC), fetch=fetch)
    assert calls == [("tw_tick:2330", date(2026, 9, 24))] * 2


def test_former_stock_gets_early_probe_before_full_history_frontier(tmp_path):
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    output = tmp_path / "finlab"
    output.mkdir()
    calls = []

    def fetch(root, key, day, *, now):
        calls.append((key, day))
        return {"status": "downloaded_unverified_for_pit", "rows": 1,
                "parquet_size_bytes": 100}

    with patch("scripts.download_finlab_market_intraday.credential_available", return_value=True), patch(
        "scripts.download_finlab_market_intraday.quota_room_mb", return_value=(1000.0, 5000.0)
    ):
        sync_market(output, public, start=date(2026, 8, 19),
                    end=date(2026, 9, 24), limit=3, reserve_mb=50,
                    minimum_free_gb=0, now=datetime(2026, 9, 25, tzinfo=UTC), fetch=fetch)
    assert calls[2] == ("tw_tick:2867", date(2026, 8, 19))


def test_daily_close_counts_nonnull_symbols_and_local_fallback_separately(tmp_path):
    public = tmp_path / "public"
    public.mkdir()
    _fixture(public)
    universe = load_universe(public, start=date(2026, 8, 19))
    output = tmp_path / "finlab"
    (output / "receipts").mkdir(parents=True)
    (output / "datasets").mkdir()
    pl.DataFrame({"source_index": ["2026-08-19", "2026-09-24"],
                  "2330": [1.0, 2.0], "2867": [5.0, None]}).write_parquet(
        output / "datasets/close.parquet"
    )
    (output / "receipts" / f"{safe_stem('price:收盤價')}.json").write_text(json.dumps({
        "dataset": "price:收盤價", "status": "downloaded_unverified_for_pit",
        "parquet_path": "datasets/close.parquet",
    }))
    coverage = annotate_daily_close_coverage(output, universe)
    assert coverage["symbols_with_values"] == 2
    assert coverage["former_with_values"] == 1
    assert coverage["missing_both_sources"] == 1
    assert {item["symbol"]: item["finlab_daily_close_rows"] for item in universe} == {
        "2330": 2, "2867": 1, "9998": 0,
    }
