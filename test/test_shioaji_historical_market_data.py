from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from contextlib import nullcontext

import polars as pl
import pytest

from downloader.download_shioaji_historical_market_data import (
    RECEIPT_SCHEMA_VERSION,
    SOURCE,
    HistoryContract,
    _atomic_write_json,
    _history_contract,
    _kbar_frame,
    _kbar_paths,
    _tick_paths,
    _valid_receipt,
    _write_inventory,
    build_tasks,
    discover_contracts,
    load_inventory,
    observed_tick_dates,
    select_latest_option_infos,
)


def _option(
    code: str,
    root: str,
    expiry: date,
    strike: float,
    right: str,
) -> SimpleNamespace:
    values = {
        "code": code,
        "root": root,
        "delivery_date": expiry,
        "underlying_code": "IX0001",
        "strike_price": strike,
        "option_right": right,
    }
    return SimpleNamespace(dict=lambda: values)


def _row(
    *,
    collection: str,
    priority: int,
    code: str,
    security_type: str = "OPT",
    begin: date = date(2026, 8, 20),
    end: date = date(2026, 8, 28),
) -> HistoryContract:
    return HistoryContract(
        collection=collection,
        priority=priority,
        security_type=security_type,
        asset_class="index" if security_type == "IND" else "options",
        code=code,
        root="TX1" if collection == "latest_weekly_option" else "TXO",
        name=code,
        exchange="TAIFEX" if security_type != "IND" else "TSE",
        begin_date=begin,
        end_date=end,
    )


def _ns(value: datetime) -> int:
    return int((value - datetime(1970, 1, 1)).total_seconds() * 1_000_000_000)


def test_select_latest_weekly_and_monthly_keeps_every_strike_and_right() -> None:
    completed = date(2026, 8, 28)
    infos = [
        _option("W1C", "TX1", date(2026, 9, 2), 46000, "C"),
        _option("W1P", "TX1", date(2026, 9, 2), 46000, "P"),
        _option("W2C", "TX2", date(2026, 9, 9), 46000, "C"),
        _option("F1C", "TXU", date(2026, 9, 4), 46000, "C"),
        _option("M1C", "TXO", date(2026, 9, 16), 45000, "C"),
        _option("M1P", "TXO", date(2026, 9, 16), 45000, "P"),
        _option("M2C", "TXO", date(2026, 10, 21), 45000, "C"),
        _option("OLD", "TXY", completed, 45000, "C"),
    ]

    weekly, monthly = select_latest_option_infos(
        infos, completed_session=completed
    )

    assert {item.dict()["code"] for item in weekly} == {"W1C", "W1P"}
    assert {item.dict()["code"] for item in monthly} == {"M1C", "M1P"}


def test_all_option_catalog_covers_roots_series_and_preserves_source_identity(tmp_path: Path) -> None:
    from downloader.download_shioaji_historical_market_data import ALL_OPTION_SCOPE

    def info(code, root, expiry, underlying, kind, ccy, strike, right):
        values = {
            "code": code, "root": root, "security_type": "OPT", "exchange": "TAIFEX",
            "begin_date": date(2026, 8, 1), "delivery_date": expiry,
            "last_trading_date": expiry, "delivery_month": expiry.strftime("%Y%m"),
            "underlying_code": underlying, "underlying_kind": kind,
            "quote_ccy": ccy, "spec_kind": "source-option-kind",
            "strike_price": strike, "option_right": right,
        }
        return SimpleNamespace(dict=lambda: values)

    chains = {
        "TX1": [info("W1C", "TX1", date(2026, 9, 9), "IX0001", "I", "TWD", 100, "C")],
        "TXO": [info("M1C", "TXO", date(2026, 9, 16), "IX0001", "I", "TWD", 100, "C"),
                info("M2P", "TXO", date(2026, 10, 21), "IX0001", "I", "TWD", 110, "P")],
        "TEO": [info("TEOC", "TEO", date(2026, 9, 16), "IX0028", "I", "TWD", 200, "C")],
        "ETF": [info("ETFP", "ETF", date(2026, 9, 16), "0050", "S", "TWD", 50, "P")],
        "GOLD": [info("GOLDC", "GOLD", date(2026, 9, 16), "GOLD", "C", "USD", 300, "C")],
    }
    calls = []
    def options(root):
        calls.append(root)
        return chains[root]
    contracts = SimpleNamespace(option_roots=lambda: [(root, root) for root in chains],
                                options=options, futures_roots=lambda: [], list=lambda _: [])
    api = SimpleNamespace(contracts=contracts)
    completed = date(2026, 9, 8)

    legacy = discover_contracts(api, completed_session=completed)
    assert {row.code for row in legacy} == {"W1C", "M1C"}
    assert set(calls) == {"TX1", "TXO"}
    calls.clear()
    expanded = discover_contracts(api, completed_session=completed, option_scope=ALL_OPTION_SCOPE)
    assert set(calls) == set(chains)
    assert {row.code for row in expanded} == {"W1C", "M1C", "M2P", "TEOC", "ETFP", "GOLDC"}
    assert {row.collection for row in expanded} == {"all_options"}
    manifest = _write_inventory(tmp_path, expanded, completed_session=completed, option_scope=ALL_OPTION_SCOPE)
    assert manifest["by_collection"]["all_options"] == 6
    assert len(manifest["option_scope_fingerprint"]) == 64
    rows = {row.code: row for row in load_inventory(tmp_path)}
    assert (rows["ETFP"].underlying_code, rows["ETFP"].underlying_kind, rows["ETFP"].quote_ccy) == ("0050", "S", "TWD")
    assert (rows["GOLDC"].underlying_kind, rows["GOLDC"].quote_ccy, rows["GOLDC"].strike_price) == ("C", "USD", 300)
    assert rows["M2P"].option_right == "P"


def test_option_inventory_scopes_cannot_be_silently_mixed(tmp_path: Path, monkeypatch) -> None:
    import sys
    import downloader.download_shioaji_historical_market_data as collector

    row = _row(collection="latest_monthly_option", priority=1, code="OLD")
    manifest = _write_inventory(tmp_path, [row], completed_session=date(2026, 8, 28))
    original = (tmp_path / "inventory/contracts.parquet").read_bytes()
    with pytest.raises(ValueError, match="incompatible"):
        _write_inventory(tmp_path, [row], completed_session=date(2026, 8, 28), option_scope="all_available")
    monkeypatch.setattr(sys, "argv", ["collector", "--dry-run", "--option-scope", "all_available",
                                      "--output-dir", str(tmp_path)])
    with pytest.raises(ValueError, match="incompatible"):
        collector.main()
    assert (tmp_path / "inventory/contracts.parquet").read_bytes() == original
    # Historical manifests without a scope marker mean the original TXO-only ABI.
    for key in ("option_scope", "option_scope_version", "option_scope_fingerprint"):
        manifest.pop(key)
    _atomic_write_json(tmp_path / "inventory/manifest.json", manifest)
    collector._validate_inventory_scope(tmp_path, "latest_txo")
    with pytest.raises(ValueError, match="incompatible"):
        collector._validate_inventory_scope(tmp_path, "all_available")


def test_all_option_query_bound_does_not_apply_futures_history_floor() -> None:
    values = {"code": "OLDOPT", "root": "ABC", "security_type": "OPT",
              "begin_date": date(2012, 4, 1), "last_trading_date": date(2012, 5, 16),
              "delivery_date": date(2012, 5, 16), "strike_price": 100, "option_right": "C"}
    row = _history_contract(SimpleNamespace(dict=lambda: values), collection="all_options",
                            completed_session=date(2012, 5, 15))
    assert row is not None
    assert (row.begin_date, row.end_date) == (date(2012, 4, 1), date(2012, 5, 15))


def test_all_option_catalog_failure_does_not_rewrite_inventory(tmp_path: Path) -> None:
    contracts = SimpleNamespace(option_roots=lambda: ["TXO", "TEO"],
                                options=lambda root: [] if root == "TXO" else (_ for _ in ()).throw(OSError("bad root")),
                                futures_roots=lambda: [], list=lambda _: [])
    with pytest.raises(RuntimeError, match="inventory preserved"):
        discover_contracts(SimpleNamespace(contracts=contracts), completed_session=date(2026, 9, 8),
                           option_scope="all_available")
    assert not (tmp_path / "inventory/contracts.parquet").exists()


def test_native_last_trade_date_is_not_clipped_to_completed_session_or_delivery(tmp_path: Path) -> None:
    values = {"code": "I5FI9", "root": "I5F", "security_type": "FUT",
        "begin_date": date(2019, 1, 1), "last_trading_date": date(2026, 9, 24),
        "delivery_date": date(2026, 9, 25), "delivery_month": "202609"}
    info = SimpleNamespace(dict=lambda: values)
    row = _history_contract(info, collection="exact_futures", completed_session=date(2026, 9, 23))
    assert row is not None
    assert row.begin_date == date(2020, 3, 22)
    assert row.source_begin_date == date(2019, 1, 1)
    assert row.end_date == date(2026, 9, 23)
    assert row.last_trading_date == date(2026, 9, 24)
    assert row.delivery_date == date(2026, 9, 25)
    manifest = _write_inventory(tmp_path, [row], completed_session=date(2026, 9, 23))
    current = pl.read_parquet(tmp_path / "inventory/current_contracts.parquet")
    assert manifest["schema_version"] == 2
    assert current.item(0, "last_trading_date") == date(2026, 9, 24)
    assert current.item(0, "source_begin_date") == date(2019, 1, 1)
    assert current.item(0, "last_observed_at_utc") == manifest["generated_at_utc"]
    assert load_inventory(tmp_path, current_only=True)[0].last_trading_date == date(2026, 9, 24)


def test_old_inventory_never_invents_native_lifecycle_dates(tmp_path: Path) -> None:
    old = _row(collection="latest_monthly_option", priority=1, code="OLD")
    current = _row(collection="latest_weekly_option", priority=0, code="NEW")
    _write_inventory(tmp_path, [old], completed_session=date(2026, 8, 28))
    directory = tmp_path / "inventory"
    # Simulate a v1 retained inventory without the newly recorded fields.
    path = directory / "contracts.parquet"
    pl.read_parquet(path).drop("source_begin_date", "last_trading_date").write_parquet(path)
    assert load_inventory(tmp_path)[0].last_trading_date is None
    _write_inventory(tmp_path, [current], completed_session=date(2026, 8, 28))
    rows = {row.code: row for row in load_inventory(tmp_path)}
    assert set(rows) == {"OLD", "NEW"}
    assert rows["OLD"].last_trading_date is None
    assert rows["OLD"].source_begin_date is None
    assert rows["NEW"].last_trading_date is None
    assert rows["OLD"].active_selection is False


def test_new_contract_is_catalogued_without_querying_uncompleted_history(tmp_path: Path) -> None:
    values = {"code": "PK1K6", "root": "PK1", "security_type": "FUT",
        "begin_date": date(2026, 10, 26), "last_trading_date": date(2026, 11, 18),
        "delivery_date": date(2026, 11, 18), "delivery_month": "202611"}
    row = _history_contract(SimpleNamespace(dict=lambda: values), collection="exact_futures",
                            completed_session=date(2026, 10, 22))
    assert row is not None
    assert row.begin_date == date(2026, 10, 26) and row.end_date == date(2026, 10, 22)
    assert build_tasks(tmp_path, [row], chunk_days=29) == []
    manifest = _write_inventory(tmp_path, [row], completed_session=date(2026, 10, 22))
    current = pl.read_parquet(tmp_path / "inventory/current_contracts.parquet")
    assert manifest["current_contracts_without_completed_history"] == 1
    assert current.item(0, "active_selection") is True
    assert current.item(0, "has_completed_history") is False
    assert current.item(0, "source_begin_date") == date(2026, 10, 26)
    assert load_inventory(tmp_path, current_only=True)[0] == row

    # Source metadata with an impossible lifetime is still rejected.
    values["last_trading_date"] = date(2026, 10, 25)
    assert _history_contract(SimpleNamespace(dict=lambda: values), collection="exact_futures",
                             completed_session=date(2026, 10, 22)) is None


def test_fop_kbars_exclude_next_trading_date_partial_night_session() -> None:
    row = _row(
        collection="latest_weekly_option",
        priority=0,
        code="TX146000I6",
    )
    payload = SimpleNamespace(
        dict=lambda: {
            "ts": [
                _ns(datetime(2026, 8, 28, 13, 0)),
                _ns(datetime(2026, 8, 28, 15, 0)),
            ],
            "Open": [1.0, 2.0],
            "High": [1.0, 2.0],
            "Low": [1.0, 2.0],
            "Close": [1.0, 2.0],
            "Volume": [1, 1],
            "Amount": [1.0, 2.0],
        }
    )

    frame, trading_dates = _kbar_frame(payload, row=row)

    assert frame.height == 1
    assert trading_dates == [date(2026, 8, 28)]
    assert frame.item(0, "Close") == 1.0


def test_tick_targets_derive_only_from_verified_kbar_receipts(tmp_path: Path) -> None:
    row = _row(
        collection="latest_weekly_option",
        priority=0,
        code="WEEK",
    )
    data_path, receipt_path = _kbar_paths(
        tmp_path, row, row.begin_date, row.end_date
    )
    data_path.parent.mkdir(parents=True)
    pl.DataFrame({"ts": [1]}).write_parquet(data_path)
    _atomic_write_json(
        receipt_path,
        {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "source": SOURCE,
            "status": "complete",
            "method": "kbars",
            "contract": row.code,
            "rows": 1,
            "start": row.begin_date.isoformat(),
            "end": row.end_date.isoformat(),
            "observed_trading_dates": ["2026-08-27", "2026-08-28"],
            "sha256": __import__("hashlib").sha256(data_path.read_bytes()).hexdigest(),
        },
    )

    assert observed_tick_dates(tmp_path, row, chunk_days=29) == [
        date(2026, 8, 27),
        date(2026, 8, 28),
    ]
    tasks = build_tasks(tmp_path, [row], chunk_days=29)
    assert [(task.method, task.start) for task in tasks] == [
        ("ticks", date(2026, 8, 28)),
        ("ticks", date(2026, 8, 27)),
    ]


def test_task_order_is_collection_priority_then_newest(tmp_path: Path) -> None:
    weekly = _row(
        collection="latest_weekly_option",
        priority=0,
        code="WEEK",
    )
    monthly = _row(
        collection="latest_monthly_option",
        priority=1,
        code="MONTH",
        begin=date(2026, 7, 1),
    )

    tasks = build_tasks(tmp_path, [monthly, weekly], chunk_days=29)

    assert tasks[0].contract.code == "WEEK"
    monthly_ends = [task.end for task in tasks if task.contract.code == "MONTH"]
    assert monthly_ends == sorted(monthly_ends, reverse=True)


def test_source_empty_receipt_is_terminal_without_fake_parquet(tmp_path: Path) -> None:
    row = _row(
        collection="indices",
        priority=3,
        code="IX0001",
        security_type="IND",
        begin=date(2020, 3, 2),
        end=date(2020, 3, 2),
    )
    data_path, receipt_path = _kbar_paths(
        tmp_path, row, row.begin_date, row.end_date
    )
    _atomic_write_json(
        receipt_path,
        {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "source": SOURCE,
            "status": "source_empty",
            "method": "kbars",
            "contract": row.code,
            "start": row.begin_date.isoformat(),
            "end": row.end_date.isoformat(),
            "rows": 0,
            "observed_trading_dates": [],
        },
    )

    assert _valid_receipt(
        receipt_path, data_path, method="kbars", code=row.code
    ) is not None
    assert build_tasks(tmp_path, [row], chunk_days=29) == []
    assert not data_path.exists()


def test_corrupt_tick_artifact_invalidates_receipt(tmp_path: Path) -> None:
    row = _row(
        collection="latest_weekly_option",
        priority=0,
        code="WEEK",
    )
    data_path, receipt_path = _tick_paths(tmp_path, row, date(2026, 8, 28))
    data_path.parent.mkdir(parents=True)
    data_path.write_bytes(b"not parquet")
    _atomic_write_json(
        receipt_path,
        {
            "schema_version": RECEIPT_SCHEMA_VERSION,
            "source": SOURCE,
            "status": "complete",
            "method": "ticks",
            "contract": row.code,
            "sha256": "wrong",
        },
    )

    assert _valid_receipt(
        receipt_path, data_path, method="ticks", code=row.code
    ) is None


def test_kbars_only_never_reads_or_schedules_ticks_and_preserves_full_summary(tmp_path, monkeypatch):
    import downloader.download_shioaji_historical_market_data as collector

    row = _row(collection="exact_futures", priority=2, code="CDFI6", security_type="FUT",
               begin=date(2026, 8, 28), end=date(2026, 8, 28))
    def forbidden(*args, **kwargs):
        raise AssertionError("KBar-only mode must never use tick data")
    monkeypatch.setattr(collector, "_tick_paths", forbidden)
    monkeypatch.setattr(collector, "observed_tick_dates", forbidden)
    monkeypatch.setattr(collector, "shioaji_query", lambda *a, **k: nullcontext(lambda _: None))
    payload = SimpleNamespace(dict=lambda: {
        "ts": [_ns(datetime(2026, 8, 28, 8, 46))],
        "Open": [100.], "High": [102.], "Low": [100.], "Close": [102.],
        "Volume": [2], "Amount": [202.],
    })
    api = SimpleNamespace(contracts=SimpleNamespace(get=lambda _: object()),
                          kbars=lambda **_: payload, ticks=forbidden)
    tasks = build_tasks(tmp_path, [row], chunk_days=29, kbars_only=True)
    assert [t.method for t in tasks] == ["kbars"]
    receipt, added = collector._query_task(
        api, tasks[0], output_root=tmp_path, timeout_ms=1000, retries=0,
        retry_backoff=0, rate_limiter=SimpleNamespace(wait=lambda: None), kbars_only=True,
    )
    assert receipt["rows"] == 1 and added == []
    assert build_tasks(tmp_path, [row], chunk_days=29, kbars_only=True) == []
    (tmp_path / "summary.json").write_text("existing complete tick summary")
    summary = collector._write_summary(tmp_path, [row], chunk_days=29, state="running",
                                       usage=None, progress_path=tmp_path / "progress_kbars.json", kbars_only=True)
    assert summary["state"] == "complete" and summary["methods"] == ["kbars"]
    assert summary["tick_dates"] == 0 and not summary["tick_target_universe_finalized"]
    assert (tmp_path / "summary.json").read_text() == "existing complete tick summary"
    assert (tmp_path / "summary_kbars.json").is_file()


def test_summary_actual_tick_days_are_a_union_not_contract_days(tmp_path, monkeypatch):
    import downloader.download_shioaji_historical_market_data as collector

    rows = [_row(collection="exact_futures", priority=2, code=code, security_type="FUT")
            for code in ("A", "B")]
    def valid(_receipt, _data, *, method, code):
        if method == "kbars":
            return {"status": "complete", "rows": 2, "size": 10,
                    "observed_trading_dates": ["2026-08-27", "2026-08-28"]}
        if "2026-08-27" in str(_receipt):
            return {"status": "source_empty", "rows": 0, "size": 0}
        return {"status": "complete", "rows": 5 if code == "A" else 4, "size": 20}
    monkeypatch.setattr(collector, "_valid_receipt", valid)
    result = collector._write_summary(tmp_path, rows, chunk_days=29, state="observed",
        usage=None, progress_path=tmp_path / "progress.json", persist=False)
    bucket = result["by_collection"]["exact_futures"]
    assert bucket["tick_dates"] == 4 and bucket["tick_rows"] == 9
    assert bucket["tick_data_days"] == 1 and bucket["tick_symbols_with_data"] == 2
    assert result["tick_data_days"] == 1
    assert bucket["tick_first_data_date"] == bucket["tick_last_data_date"] == "2026-08-28"
    assert bucket["kbar_data_days"] == 2 and bucket["kbar_symbols_with_data"] == 2
    assert bucket["kbar_first_data_date"] == "2026-08-27"
    assert bucket["kbar_last_data_date"] == "2026-08-28"


def test_summary_only_uses_original_writer_lock_and_never_logs_in(tmp_path, monkeypatch):
    import builtins
    import fcntl
    import sys
    import pytest
    import downloader.download_shioaji_historical_market_data as collector

    monkeypatch.setattr(sys, "argv", ["collector", "--summary-only", "--output-dir", str(tmp_path)])
    monkeypatch.setattr(collector, "load_inventory", lambda root: [
        _row(collection="exact_futures", priority=2, code="A", security_type="FUT")])
    real_import = builtins.__import__
    def no_login(name, *args, **kwargs):
        if name == "shioaji":
            raise AssertionError("metadata rebuild must not import or log in to the SDK")
        return real_import(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", no_login)
    with (tmp_path / "download.lock").open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            collector.main()
    assert collector.main() == 0
    assert (tmp_path / "summary.json").is_file()
