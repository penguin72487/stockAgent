from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3

import polars as pl
import pytest

from stockagent.live import tick_inventory as ticks


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_finlab_counts_nonempty_days_once_across_symbols_and_ignores_pending(tmp_path):
    path = tmp_path / "data_finlab/intraday/market_index.sqlite3"
    path.parent.mkdir(parents=True)
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE partitions(kind,symbol,trade_date,status,rows,parquet_bytes)")
        conn.executemany("INSERT INTO partitions VALUES (?,?,?,?,?,?)", [
            ("tw_tick", "2330", "2026-05-15", "downloaded_unverified_for_pit", 3, 100),
            ("tw_tick", "2317", "2026-05-15", "downloaded_unverified_for_pit", 2, 80),
            ("tw_tick", "2330", "2026-05-18", "downloaded_unverified_for_pit", 4, 110),
            ("tw_tick", "2330", "2026-10-26", "partition_not_ready", 0, 0),
            ("tw_minute", "2330", "2020-01-01", "downloaded_unverified_for_pit", 99, 500),
        ])
    row = ticks._finlab(tmp_path, ticks._Sources({}))
    assert (row["count"], row["days"], row["symbols"], row["partitions"]) == (9, 2, 2, 3)
    assert (row["first"], row["last"]) == ("2026-05-15", "2026-05-18")
    assert row["statistics_state"] == "receipt_index"


def test_checkpointed_readonly_database_fallback_never_ignores_wal_or_mutation(tmp_path, monkeypatch):
    path = tmp_path / "index.sqlite3"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE sample(value)")
        conn.execute("INSERT INTO sample VALUES (7)")
    connect = sqlite3.connect
    calls = []
    mutate = [False]
    def sandbox_connect(database, **kwargs):
        calls.append(database)
        if "immutable=1" not in database:
            raise sqlite3.OperationalError("unable to open database file")
        if mutate[0]:
            with connect(path) as writer:
                writer.execute("INSERT INTO sample VALUES (9)")
        return connect(database, **kwargs)
    monkeypatch.setattr(ticks.sqlite3, "connect", sandbox_connect)
    assert ticks._database(path, "SELECT value FROM sample") == [(7,)]
    assert len(calls) == 2
    assert not path.with_name(path.name + "-shm").exists()
    wal = path.with_name(path.name + "-wal")
    wal.write_bytes(b"active WAL must never be ignored")
    with pytest.raises(sqlite3.OperationalError, match="unable to open"):
        ticks._database(path, "SELECT value FROM sample")
    wal.unlink()
    mutate[0] = True
    with pytest.raises(sqlite3.OperationalError, match="metadata changed"):
        ticks._database(path, "SELECT value FROM sample")


def test_finmind_keeps_providers_and_event_dates_separate_and_empty_is_not_data(tmp_path):
    path = tmp_path / "data_finmind/complement/queue.sqlite3"
    path.parent.mkdir(parents=True)
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE tasks(dataset,partition,state,rows,first_data_date,last_data_date,bytes)")
        conn.executemany("INSERT INTO tasks VALUES (?,?,?,?,?,?,?)", [
            ("TaiwanFuturesTick", "2011-01-03", "complete", 5, "2011-01-03", "2011-01-03", 100),
            ("TaiwanFuturesTick", "2011-01-04", "complete", 7, "2011-01-04", "2011-01-04", 200),
            ("TaiwanFuturesTick", "2026-10-23", "empty", 0, None, None, 0),
            ("TaiwanFuturesTick", "2026-10-24", "failed", 99, "2026-10-24", "2026-10-24", 900),
            ("TaiwanFutOptTickInfo", "catalog", "complete", 10000, "2000-01-01", "2026-10-24", 999),
        ])
    rows = {row["id"]: row for row in ticks._finmind(tmp_path, ticks._Sources({}))}
    assert len(rows) == 4
    row = rows["finmind:TaiwanFuturesTick"]
    assert (row["count"], row["days"], row["first"], row["last"]) == (12, 2, "2011-01-03", "2011-01-04")
    assert "事件日期" in row["date_basis"]
    assert row["symbols"] is None
    assert rows["finmind:TaiwanStockPriceTick"]["count"] == 0
    missing = ticks._finmind(tmp_path / "missing", ticks._Sources({}))[0]
    assert missing["count"] is None and missing["days"] is None


def test_footer_inventory_retains_old_partitions_outside_rolling_manifest_and_reports_corruption(tmp_path):
    root = tmp_path / "data_tw_index_derivatives_ticks/tx"
    for day in ("2026-06-25", "2026-10-22"):
        path = root / f"trading_date={day}/transactions.parquet"
        path.parent.mkdir(parents=True)
        pl.DataFrame({"event_ts": [datetime.fromisoformat(day + "T09:00:01")], "price": [1]}).write_parquet(path)
    write_json(root / "manifest.json", {"partitions": [{"date": "2026-10-22", "rows": 1}]})
    cache = ticks._Sources({})
    row = ticks._official(tmp_path, cache)[0]
    assert (row["count"], row["days"], row["first"], row["last"]) == (2, 2, "2026-06-25", "2026-10-22")
    assert row["first_event"].startswith("2026-06-25")
    bad = root / "trading_date=2026-10-23/transactions.parquet"
    bad.parent.mkdir(parents=True)
    bad.write_bytes(b"corrupt")
    row = ticks._official(tmp_path, ticks._Sources({}))[0]
    assert row["count"] is None and row["known_count"] == 2
    assert row["days"] is None and row["statistics_state"] == "partial"


def test_continuous_dates_use_nonempty_directories_not_requested_range(tmp_path):
    root = tmp_path / "data_tw_futures/shioaji_history/JWFR2"
    write_json(root / "manifest.json", {
        "dataset": "shioaji_continuous_futures_historical_ticks_v1", "rows": 12,
        "complete_trading_dates": 2, "history_start": "2020-03-02", "history_end": "2026-10-23",
    })
    for day in ("2020-03-25", "2026-09-14"):
        (root / "ticks" / f"trading_date={day}").mkdir(parents=True)
    summary, contracts = ticks._continuous(tmp_path, ticks._Sources({}))
    assert len(contracts) == 1
    assert (summary["days"], summary["first"], summary["last"]) == (2, "2020-03-25", "2026-09-14")
    (root / "ticks/trading_date=2026-09-15").mkdir()
    _, contracts = ticks._continuous(tmp_path, ticks._Sources({}))
    assert contracts[0]["days"] is None and contracts[0]["statistics_state"] == "partial"


def test_capture_session_mirrors_zero_future_and_book_grains_are_distinct(tmp_path):
    root = tmp_path / "data_tw_index_derivatives_ticks/shioaji_fop_captures/manifests"
    base = {"capture_id": "original", "worker_index": 0, "tick_rows_written": 5,
            "book_rows_written": 8, "book_1s_rows_written": 3, "dropped_events": 0}
    write_json(root / "trade_date=2026-10-22/session=day/worker=0.json", base)
    write_json(root / "trade_date=2026-10-22/worker=0.json", {**base, "tick_rows_written": 999})
    write_json(root / "trade_date=2026-10-26/session=night/worker=0.json", {
        **base, "capture_id": "future", "tick_rows_written": 0, "book_rows_written": 0, "book_1s_rows_written": 0,
    })
    rows = ticks._captures(tmp_path, "data_tw_index_derivatives_ticks/shioaji_fop_captures",
                           "期權自行錄製", "taiwan_derivatives", ticks._Sources({}))
    assert [(r["kind"], r["count"], r["days"]) for r in rows] == [
        ("trade", 5, 1), ("book", 8, 1), ("book_snapshot", 3, 1)]
    assert all(r["last"] == "2026-10-22" for r in rows)
    (root / "trade_date=2026-10-27/worker=0.json").parent.mkdir()
    (root / "trade_date=2026-10-27/worker=0.json").write_text("[]")
    rows = ticks._captures(tmp_path, "data_tw_index_derivatives_ticks/shioaji_fop_captures",
                           "期權自行錄製", "taiwan_derivatives", ticks._Sources({}))
    assert rows[0]["count"] is None and rows[0]["known_count"] == 5


def test_source_cache_reuses_identity_but_invalidates_wal_and_rejects_bad_metadata(tmp_path):
    path, wal = tmp_path / "index", tmp_path / "index-wal"
    path.write_bytes(b"index")
    first = ticks._Sources({})
    assert first.read("db", [path, wal], lambda: 12) == 12
    second = ticks._Sources({"version": ticks.VERSION, "sources": first.current})
    def forbidden():
        raise AssertionError("unchanged source must not be read again")
    assert second.read("db", [path, wal], forbidden) == 12
    wal.write_bytes(b"new transaction")
    assert second.read("db", [path, wal], lambda: 15) == 15
    broken = ticks._Sources({"version": ticks.VERSION, "sources": []})
    assert broken.read("broken", [path], lambda: ticks._json(path)) is None
    assert broken.errors == 1


def test_unindexed_capture_pieces_include_old_capture_and_missing_manifest_dates_without_double_counting(tmp_path):
    relative = "data_tw_microstructure/captures"
    root = tmp_path / relative
    write_json(root / "manifests/trade_date=2026-07-20/worker=0.json", {
        "capture_id": "current", "worker_index": 0, "tick_rows_written": 5,
        "book_rows_written": 0, "book_1s_rows_written": 0, "dropped_events": 0})
    def piece(day, capture, rows):
        path = root / f"ticks/trade_date={day}/hour=09/capture={capture}-worker=0-part=1-100.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"price": list(range(rows))}).write_parquet(path)
    piece("2026-07-20", "current", 5)
    piece("2026-07-20", "old", 3)
    piece("2026-07-23", "interrupted", 7)
    first = ticks._Sources({})
    rows = ticks._captures(tmp_path, relative, "台股自行錄製", "taiwan_equity", first)
    assert rows[0]["count"] == 5
    extra = next(row for row in rows if row["statistics_state"] == "unattributed_footers")
    assert extra["count"] == 10 and extra["days"] == 2 and extra["partitions"] == 2
    assert extra["last"] == "2026-07-23"
    second = ticks._Sources({"version": ticks.VERSION, "sources": first.current})
    again = ticks._captures(tmp_path, relative, "台股自行錄製", "taiwan_equity", second)
    assert again == rows and second.hits >= 3 and second.footer_reads == 0


def test_cold_footer_audit_resumes_within_budget_and_compacts_finished_hours(tmp_path, monkeypatch):
    import stockagent.live.data_monitor_inventory as footer

    relative = "data_tw_microstructure/captures"
    hour = tmp_path / relative / "ticks/trade_date=2026-07-23/hour=09"
    hour.mkdir(parents=True)
    for index in range(3):
        (hour / f"capture=old-worker=0-part={index}-100.parquet").write_bytes(b"fixture")
    clock = [0.]
    monkeypatch.setattr(ticks.time, "monotonic", lambda: clock[0])
    def slow_footer(path):
        clock[0] += .6
        return 1
    monkeypatch.setattr(footer, "parquet_footer_count", slow_footer)
    previous = {}
    seen = []
    for _ in range(4):
        cache = ticks._Sources(previous, audit_seconds=.5)
        rows = ticks._captures(tmp_path, relative, "台股自行錄製", "taiwan_equity", cache)
        extra = rows[-1]
        seen.append(extra["known_count"])
        assert cache.footer_reads <= 1
        previous = {"version": ticks.VERSION, "sources": cache.current}
    assert seen == [1, 2, 3, 3]
    assert extra["count"] == 3 and extra["days"] == 1
    assert cache.audit_pending == 0
    assert all("members" not in str(value["value"]) for value in cache.current.values())


def test_summary_and_pages_keep_filter_intersection_null_and_generation(tmp_path):
    payload, _ = ticks.build_tick_inventory(tmp_path, now=datetime(2026, 10, 25, tzinfo=UTC))
    assert payload["collection"]["provider_calls"] == 0
    assert "datasets" not in ticks.tick_summary(payload)
    assert ticks.tick_page({})["state"] == "waiting_snapshot"
    page = ticks.tick_page(payload, provider="FinMind", market="taiwan_derivatives", kind="trade", search="Futures", limit=1)
    assert page["matching_total"] == 2 and page["has_more"]
    assert page["rows"][0]["count"] is None
    assert page["revision"] == payload["revision"]
    assert all("signature" not in str(row) for row in payload["groups"])
    with pytest.raises(ValueError):
        ticks.tick_page(payload, limit=101)


def test_extended_footer_audit_uses_spawn_with_exact_counts_and_releases_workers(tmp_path):
    paths = [tmp_path / "valid.parquet", tmp_path / "broken.parquet"]
    pl.DataFrame({"price": [2, 3, 4]}).write_parquet(paths[0])
    paths[1].write_bytes(b"broken")
    cache = ticks._Sources({}, audit_seconds=10)
    try:
        assert cache.footer_counts(paths) == [3, None]
        assert cache.footer_pool._mp_context.get_start_method() == "spawn"
    finally:
        cache.close()
    assert not cache.footer_pool._processes


def test_date_projection_avoids_quote_price_key_and_migrates_cached_rows():
    row = {"id": "example", "provider": "FinMind", "market_category": "taiwan_equity",
           "kind": "trade", "first": "2018-12-07", "last": "2026-10-06"}
    payload = {"version": ticks.VERSION, "groups": [row], "datasets": [row]}
    for projected in (ticks.tick_summary(payload)["groups"][0], ticks.tick_page(payload)["rows"][0]):
        assert projected["earliest_date"] == "2018-12-07"
        assert projected["latest_date"] == "2026-10-06"
        assert "first" not in projected and "last" not in projected
