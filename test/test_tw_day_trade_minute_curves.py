from __future__ import annotations

from datetime import date, datetime, timedelta
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl
import pytest

from scripts.maintain_tw_day_trade_minute_curves import (
    _completed_scope,
    _inspect_strategy_price_provenance,
    _missing_endpoints_with_cache,
    _proven_price_state_from_curve_validation,
    _stock_calendar_source_state,
    _tx_benchmark_source_state,
    _validate_benchmarks,
)
from scripts.rebuild_tw_day_trade_minute_curves import (
    DEFAULT_LOCAL_MINUTE_ROOTS,
    MinutePriceStore,
    _benchmark_minute_row,
    _is_terminal_carry_cost,
    _ImmutableMinuteInventory,
    _ticks_to_minute_frame,
    fetch_missing_kbars,
    missing_accepted_endpoints,
    rebuild_benchmark_history,
    rebuild_strategy_marks,
    required_symbol_dates,
    validate_existing_strategy_marks,
    verified_manual_no_trade_pairs,
)


def test_zero_volume_price_only_curve_exception_is_legacy_v1_only():
    from stockagent.live.tw_day_trade_simulation import (
        LEGACY_REPLAY_FILL_CONTRACT_0901_FULL_COUNTERFACTUAL,
        REPLAY_FILL_CONTRACT_0901_FULL_COUNTERFACTUAL,
    )
    fill = {
        "position_id": "p", "symbol": "2330", "quantity": 1000,
        "purpose": "entry", "clock": "2026-10-28T09:01:00+08:00",
        "session_date": "2026-10-28", "fill_at": "2026-10-28T09:01:00+08:00",
        "simulation_only": True, "simulation_replay": True,
        "counterfactual_0901_price_fill": True, "synthetic_fill": False,
        "entry_price_method": "minute_close", "entry_price_source": "source",
        "price": 100.0,
    }
    legacy = _ImmutableMinuteInventory(
        {"p": {"symbol": "2330"}},
        [fill | {"fill_contract": LEGACY_REPLAY_FILL_CONTRACT_0901_FULL_COUNTERFACTUAL}],
        market="mode",
    )
    current = _ImmutableMinuteInventory(
        {"p": {"symbol": "2330"}},
        [fill | {"fill_contract": REPLAY_FILL_CONTRACT_0901_FULL_COUNTERFACTUAL}],
        market="mode",
    )
    fractional = _ImmutableMinuteInventory(
        {"p": {"symbol": "2330"}},
        [fill | {"fill_contract": LEGACY_REPLAY_FILL_CONTRACT_0901_FULL_COUNTERFACTUAL,
                 "fill_at": "2026-10-28T09:01:00.000000+08:00"}],
        market="mode",
    )
    assert legacy.full_0901_price_entries == {"p"}
    assert current.full_0901_price_entries == set()
    assert fractional.full_0901_price_entries == {"p"}


def test_minute_inventory_compares_instants_not_iso_string_formats():
    positions = {
        name: {"position_id": name, "symbol": name, "side": "long"}
        for name in ("same", "future", "offset")
    }
    base = {"purpose": "entry", "quantity": 1000, "price": 100.,
            "fee_and_tax_twd": 0.}
    events = [
        {**base, "position_id": "same", "symbol": "same",
         "clock": "2026-10-29T09:01:00.000000+08:00"},
        {**base, "position_id": "future", "symbol": "future",
         "clock": "2026-10-29T09:01:00.000001+08:00"},
        {**base, "position_id": "offset", "symbol": "offset",
         "clock": "2026-10-29T01:00:59.999999Z"},
    ]
    book = _ImmutableMinuteInventory(positions, events, market="a")
    prices, times = {"same": 105.}, {"same": "2026-10-29T09:01:00+08:00"}
    book.advance("2026-10-29T09:01:00+08:00", prices, times)
    assert set(book.positions) == {"same", "offset"}
    # A print at exactly the fill instant is not older just because the fill
    # includes fractional seconds.
    assert book.mark_price(book.positions["same"], prices, times) == 105.
    book.advance("2026-10-29T09:01:00.000001+08:00", prices, times)
    assert set(book.positions) == {"same", "future", "offset"}


def test_minute_inventory_retains_journal_order_for_equal_instants():
    position = {"position_id": "p", "symbol": "2330", "side": "long"}
    common = {"position_id": "p", "symbol": "2330", "quantity": 1000,
              "price": 100., "fee_and_tax_twd": 0.}
    book = _ImmutableMinuteInventory({"p": position}, [
        {**common, "purpose": "entry", "clock": "2026-10-29T09:01:00+08:00"},
        {**common, "purpose": "entry_completion", "clock": "2026-10-29T01:01:00Z"},
    ], market="a")
    book.advance("2026-10-29T09:01:00+08:00", {}, {})
    assert book.positions["p"]["signed_shares"] == 2000


def test_minute_inventory_rejects_unqualified_clock():
    position = {"position_id": "p", "symbol": "2330", "side": "long"}
    fill = {"position_id": "p", "symbol": "2330", "purpose": "entry",
            "quantity": 1000, "price": 100., "fee_and_tax_twd": 0.,
            "clock": "2026-10-29T09:01:00"}
    with pytest.raises(ValueError, match="timezone"):
        _ImmutableMinuteInventory({"p": position}, [fill], market="a")


def _native_same_second_exit_fixture():
    common = {"position_id": "p", "symbol": "2241", "market": "a",
              "session_date": "2026-11-05", "quantity": 1000,
              "simulation_only": True, "simulation_replay": False,
              "quote_at": "2026-11-05T13:00:54.302491+08:00"}
    entry = {**common, "purpose": "entry", "price": 35., "fee_and_tax_twd": 9.975,
             "clock": "2026-11-05T13:00:54.503061+08:00"}
    exit = {**common, "purpose": "stop_loss", "price": 34.9,
            "clock": "2026-11-05T13:00:54+08:00",
            "fill_at": "2026-11-05T13:00:54+08:00",
            "recorded_at": "2026-11-05T13:00:54+08:00",
            "entry_fee_allocated_twd": 9.975, "net_pnl_twd": -172.2715}
    position = {"position_id": "p", "symbol": "2241", "market": "a", "side": "long",
                "session_date": "2026-11-05", "signed_shares": 0,
                "entry_at": entry["clock"], "exit_at": exit["clock"],
                "last_exit_at": "2026-11-05T13:00:54.513326+08:00",
                "last_exit_quantity": 1000, "last_exit_price": 34.9,
                "last_exit_quote_at": common["quote_at"], "exit_reason": "stop_loss"}
    return position, entry, exit


def test_native_exit_clock_uses_unique_archived_precision_without_changing_journal():
    from copy import deepcopy
    position, entry, exit = _native_same_second_exit_fixture()
    originals = deepcopy((position, entry, exit))
    book = _ImmutableMinuteInventory({"p": position}, [entry, exit], market="a")
    book.advance("2026-11-05T13:00:54.503061+08:00", {}, {})
    assert book.positions["p"]["signed_shares"] == 1000
    book.advance("2026-11-05T13:00:54.513326+08:00", {}, {})
    assert book.positions == {} and book.realized == pytest.approx(-172.2715)
    assert len(book.fill_clock_resolutions) == 1
    assert book.fill_clock_resolutions[0]["original_journal_changed"] is False
    assert (position, entry, exit) == originals


@pytest.mark.parametrize("damage", ["quantity", "price", "quote", "purpose", "outside_second", "explicit_fraction", "duplicate"])
def test_native_exit_clock_refuses_unproven_or_ambiguous_precision(damage):
    position, entry, exit = _native_same_second_exit_fixture()
    if damage == "quantity": position["last_exit_quantity"] = 2000
    elif damage == "price": position["last_exit_price"] = 34.95
    elif damage == "quote": position["last_exit_quote_at"] = "other"
    elif damage == "purpose": position["exit_reason"] = "different"
    elif damage == "outside_second": position["last_exit_at"] = "2026-11-05T13:00:55.001000+08:00"
    elif damage == "explicit_fraction": exit["clock"] = "2026-11-05T13:00:54.000001+08:00"
    events = [entry, exit, dict(exit)] if damage == "duplicate" else [entry, exit]
    book = _ImmutableMinuteInventory({"p": position}, events, market="a")
    assert book.fill_clock_resolutions == []
    with pytest.raises(RuntimeError, match="exit precedes a proven entry"):
        book.advance("2026-11-05T13:01:00+08:00", {}, {})


def _cash_gap_fixture():
    from scripts.rebuild_tw_day_trade_minute_curves import recover_proven_cash_session_marks
    days = ["2026-10-29", "2026-10-30", "2026-11-02"]
    common = {"market": "mode", "initial_capital_twd": 1000.,
              "cumulative_realized_net_pnl_twd": 0., "open_net_liquidation_pnl_twd": 0.,
              "total_equity_twd": 1000., "open_position_count": 0}
    rows = [{**common, "session_date": d, "minute": f"{d}T{clock}+08:00",
             "recorded_at": f"{d}T{clock}:00+08:00"}
            for d, clock in [(days[0], "09:01"), (days[0], "13:30"),
                             (days[2], "09:01"), (days[2], "13:30")]]
    kwargs = {"state": {"modes": {"mode": {"initial_capital_twd": 1000.}}},
              "fill_rows": [], "activity_rows": [], "completed_sessions": days,
              "expected_markets": {"mode"},
              "reconstructed_at": datetime.fromisoformat("2026-11-10T06:00:00+08:00")}
    return recover_proven_cash_session_marks, rows, kwargs


def test_cash_gap_recovery_derives_cash_not_unobserved_market_trades():
    function, rows, kwargs = _cash_gap_fixture()
    result, proof = function(rows, {}, **kwargs)
    derived = result[len(rows):]
    assert len(derived) == proof["generated_rows"] == 270
    assert derived[0]["minute"] == "2026-10-30T09:01+08:00"
    assert derived[-1]["minute"] == "2026-10-30T13:30+08:00"
    assert {r["total_equity_twd"] for r in derived} == {1000.}
    assert all(r["open_position_count"] == 0 and not r["valuation_executable"] for r in derived)
    assert all(r["reconstructed_at"].startswith("2026-11-10") for r in derived)
    assert proof["source_price_or_execution_invented"] is False
    assert proof["sessions"][0]["market_zero_volume_claimed"] is False
    assert proof["sessions"][0]["missing_signal_replay_performed"] is False
    assert result[:len(rows)] == rows


def test_cash_gap_recovery_does_not_require_future_session_positions():
    function, rows, kwargs = _cash_gap_fixture()
    kwargs["fill_rows"] = [{"market": "mode", "session_date": "2026-11-03",
                           "fill_at": "2026-11-03T09:00:01+08:00",
                           "recorded_at": "2026-11-03T09:00:02+08:00",
                           "purpose": "entry", "position_id": "future-unrelated"}]
    result, proof = function(rows, {}, **kwargs)
    assert proof["generated_rows"] == 270
    assert result[:len(rows)] == rows
    assert kwargs["fill_rows"][0]["position_id"] == "future-unrelated"


def test_cash_gap_recovery_cannot_hide_past_activity_under_future_session_date():
    function, rows, kwargs = _cash_gap_fixture()
    kwargs["fill_rows"] = [{"market": "mode", "session_date": "2026-11-03",
                           "fill_at": "2026-11-02T09:00:01+08:00",
                           "recorded_at": "2026-11-02T09:00:02+08:00",
                           "purpose": "entry", "position_id": "misdated-entry"}]
    with pytest.raises(RuntimeError, match="no accepted position"):
        function(rows, {}, **kwargs)


@pytest.mark.parametrize("damage", ["activity", "fill", "wrong_anchor", "held_inventory", "partial_marks", "adjustment"])
def test_cash_gap_recovery_fails_closed_without_complete_account_proof(damage):
    function, rows, kwargs = _cash_gap_fixture()
    if damage == "activity": kwargs["activity_rows"] = [{"market": "mode", "session_date": "2026-10-30"}]
    elif damage == "fill": kwargs["fill_rows"] = [{"market": "mode", "session_date": "2026-10-30"}]
    elif damage == "wrong_anchor": rows[1]["cumulative_realized_net_pnl_twd"] = 10.
    elif damage == "held_inventory": rows[1]["open_position_count"] = 1
    elif damage == "partial_marks": rows.append({**rows[0], "session_date": "2026-10-30", "minute": "2026-10-30T09:30+08:00"})
    elif damage == "adjustment": kwargs["state"]["modes"]["mode"]["carry_cost_ledger"] = [{"amount_twd": 1}]
    with pytest.raises(RuntimeError):
        function(rows, {}, **kwargs)


def test_maintenance_preflight_import_does_not_load_training_runtime() -> None:
    command = (
        "import sys; import scripts.maintain_tw_day_trade_minute_curves; "
        "assert 'torch' not in sys.modules"
    )
    result = subprocess.run(
        [sys.executable, "-c", command],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


def test_minute_curve_and_replay_defaults_share_one_lightweight_declaration() -> None:
    from scripts.rebuild_tw_day_trade_minute_curves import DEFAULT_LOCAL_MINUTE_CACHE_ROOTS
    from scripts.rebuild_tw_day_trade_open_price_replay import DEFAULT_MINUTE_DATA_ROOTS
    from stockagent.live import minute_source_paths as paths

    assert DEFAULT_LOCAL_MINUTE_ROOTS is paths.DEFAULT_LOCAL_MINUTE_ROOTS
    assert DEFAULT_MINUTE_DATA_ROOTS is paths.DEFAULT_LOCAL_MINUTE_ROOTS
    assert DEFAULT_LOCAL_MINUTE_CACHE_ROOTS is paths.DEFAULT_LOCAL_MINUTE_CACHE_ROOTS


def _finmind_valuation_fixture(root, *, rows=None):
    day = "2026-01-02"
    rows = rows or [
        {"date": day, "minute": "09:00:00", "stock_id": "00883B", "open": 10.,
         "high": 11., "low": 10., "close": 11., "volume": 3.},
        {"date": day, "minute": "09:01:00", "stock_id": "00883B", "open": 12.,
         "high": 12., "low": 12., "close": 12., "volume": 0.},
        {"date": day, "minute": "13:30:00", "stock_id": "00883B", "open": 13.,
         "high": 13., "low": 13., "close": 13., "volume": 1.},
    ]
    obj = root / "parquet/TaiwanStockKBar/all/2026-01-02/object.parquet"
    obj.parent.mkdir(parents=True)
    pl.DataFrame(rows).write_parquet(obj)
    receipt = root / "receipts/TaiwanStockKBar/all/2026-01-02.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_text(json.dumps({"status": "complete", "dataset": "TaiwanStockKBar",
        "data_id": "", "partition": day, "rows": len(rows),
        "parquet_path": str(obj.relative_to(root)), "parquet_size_bytes": obj.stat().st_size,
        "sha256": hashlib.sha256(obj.read_bytes()).hexdigest()}))
    return obj, receipt


def test_finmind_same_generation_is_fully_validated_once_not_per_symbol(tmp_path, monkeypatch):
    import downloader.finmind_storage_objects as storage
    base = {"date": "2026-01-02", "minute": "09:00:00", "open": 10.,
            "high": 11., "low": 10., "close": 11., "volume": 1.}
    symbols = ["2330", "2317", "00883B"]
    _finmind_valuation_fixture(tmp_path, rows=[dict(base, stock_id=s) for s in symbols])
    original, calls = storage.validate_parquet, []

    def validate(*args):
        calls.append(args)
        return original(*args)

    monkeypatch.setattr(storage, "validate_parquet", validate)
    store = MinutePriceStore([], [], require_receipts=True, finmind_root=tmp_path)
    for symbol in symbols[:2]:
        assert store.prices(symbol, "2026-01-02")["2026-01-02T09:01+08:00"] == 11.
    assert len(calls) == 1
    assert store.finmind_read_metrics["validated_generation_reuses"] == 1
    receipt = tmp_path / "receipts/TaiwanStockKBar/all/2026-01-02.json"
    receipt.write_text(receipt.read_text() + " ")
    with pytest.raises(RuntimeError, match="changed during reconstruction"):
        store.prices(symbols[2], "2026-01-02")


def test_optional_opening_backup_is_batched_across_native_day(tmp_path, monkeypatch):
    import downloader.finmind_storage_objects as storage
    root, local = tmp_path / "finmind", tmp_path / "local"
    symbols = ["2330", "2317", "00883B"]
    _finmind_valuation_fixture(root, rows=[{
        "date": "2026-01-02", "minute": "09:00:00", "stock_id": s,
        "open": 10., "high": 11., "low": 10., "close": 11., "volume": 1.} for s in symbols])
    path = local / "trade_date=2026-01-02/data.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame({"symbol": symbols, "date": [date(2026, 1, 2)] * 3,
        "ts": [datetime(2026, 1, 2, 9, 2)] * 3, "Close": [12.] * 3, "Volume": [1.] * 3}).write_parquet(path)
    store = MinutePriceStore([local], [], require_receipts=True, finmind_root=root,
                             complete_opening_prices=True)
    monkeypatch.setattr(store, "_verified_replay_source", lambda *args: True)
    original, calls = storage.validate_parquet, []
    monkeypatch.setattr(storage, "validate_parquet", lambda *args: calls.append(args) or original(*args))
    store.prepare({s: {"2026-01-02"} for s in symbols})
    assert store.finmind_read_metrics["subset_reads"] == 1
    for symbol in symbols:
        prices = store.prices(symbol, "2026-01-02")
        assert prices["2026-01-02T09:01+08:00"] == 11.
        assert prices["2026-01-02T09:02+08:00"] == 12.
    assert len(calls) == 1 and store.finmind_read_metrics["subset_reads"] == 1


def test_first_observed_bar_keeps_actual_completed_clock_and_never_creates_0901(tmp_path):
    day = "2026-01-02"
    raw = [{"date": day, "minute": clock, "stock_id": "00883B", "open": price,
            "high": price, "low": price, "close": price, "volume": volume}
           for clock, price, volume in [("09:00:00", 10., 0.), ("09:02:00", 12., 2.),
                                         ("09:04:00", 13., 5.)]]
    obj, _ = _finmind_valuation_fixture(tmp_path, rows=raw)
    store = MinutePriceStore([], [], require_receipts=True, finmind_root=tmp_path)
    first = store.first_observed_bar("00883B", day)
    assert first["source_minute"] == day + "T09:03+08:00"
    assert first["price"] == 12. and len(first["source_pins"]) == 2
    assert day + "T09:01+08:00" not in store.prices("00883B", day)
    first["price"] = 0.
    assert store.first_observed_bar("00883B", day)["price"] == 12.
    obj.write_bytes(b"source changed")
    with pytest.raises(RuntimeError, match="changed during reconstruction"):
        store.assert_sources_unchanged()


def test_first_bar_valuation_rejects_unverified_or_execution_pinned_sources(tmp_path):
    for kwargs in ({}, {"require_receipts": True, "replay_symbol_sources": {}}):
        store = MinutePriceStore([], [], **kwargs)
        with pytest.raises(ValueError, match="verified valuation sources"):
            store.first_observed_bar("2330", "2026-01-02")
    with pytest.raises(RuntimeError, match="no verified observed first bar"):
        MinutePriceStore([], [], require_receipts=True).first_observed_bar("2330", "2026-01-02")


@pytest.mark.parametrize("zone", [None, "UTC", "America/New_York"])
def test_vector_minute_keys_equal_legacy_datetime_conversion(zone):
    import math
    base = datetime(2026, 1, 2, 1, 1, 59, 123456, tzinfo=ZoneInfo("UTC"))
    stamps = [base, base, None, base + timedelta(minutes=1), base + timedelta(minutes=2)]
    stamps = [(s.replace(tzinfo=None) if zone is None else s.astimezone(ZoneInfo(zone)))
              if s is not None else None for s in stamps]
    frame = pl.DataFrame({"date": [date(2026, 1, 2)] * 5,
        "ts": pl.Series(stamps, dtype=pl.Datetime("ns", zone)),
        "Close": [1., 2., 3., math.nan, 9.], "Volume": [1., 1., 1., 1., 0.]})
    expected = {}
    for row in frame.iter_rows(named=True):
        stamp, price = row["ts"], float(row["Close"])
        if row["Volume"] > 0 and isinstance(stamp, datetime) and math.isfinite(price) and price > 0:
            stamp = stamp.replace(tzinfo=TAIPEI) if stamp.tzinfo is None else stamp.astimezone(TAIPEI)
            expected[stamp.isoformat(timespec="minutes")] = price
    assert MinutePriceStore._frame_prices(frame, "2026-01-02") == expected


def test_vector_minute_keys_do_not_silently_accept_null_prices():
    frame = pl.DataFrame({"ts": [datetime(2026, 1, 2, 9, 1)],
                          "Close": pl.Series([None], dtype=pl.Float64)})
    with pytest.raises(TypeError):
        MinutePriceStore._frame_prices(frame, "2026-01-02")


@pytest.mark.parametrize("zone", [None, "UTC", "America/New_York"])
@pytest.mark.parametrize("with_volume", [False, True])
def test_batch_session_prices_equal_scalar_reader_with_ties_and_invalid_rows(zone, with_volume):
    stamps = [datetime(2026, 1, 2 + i // 5, 1, i % 5, tzinfo=ZoneInfo("UTC"))
              for i in range(10)]
    stamps[1] = stamps[0]
    stamps[6] = None
    stamps = [(s.replace(tzinfo=None) if zone is None else s.astimezone(ZoneInfo(zone)))
              if s is not None else None for s in stamps]
    frame = pl.DataFrame({"date": [date(2026, 1, 2)] * 5 + [date(2026, 1, 3)] * 5,
        "ts": pl.Series(stamps, dtype=pl.Datetime("ns", zone)),
        "Close": [1., 2., float("nan"), 0., float("inf"), 3., 4., -1., 5., 6.]})
    if with_volume:
        frame = frame.with_columns(pl.Series("Volume", [1., 2., 1., 1., 1., 0., 1., 1., float("nan"), -1.]))
    days = ["2026-01-03", "2026-01-02", "2026-01-05"]
    assert MinutePriceStore._frame_prices_by_session(frame, days) == {
        day: MinutePriceStore._frame_prices(frame, day) for day in days}


def test_batch_session_prices_keep_unusual_schema_compatibility_and_null_errors():
    frame = pl.DataFrame({"ts": [datetime(2026, 1, 2, 9, 1)], "Close": [10.]})
    days = ["2026-01-02", "2026-01-03"]
    assert MinutePriceStore._frame_prices_by_session(frame, days) == {
        day: MinutePriceStore._frame_prices(frame, day) for day in days}
    frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias("Close"))
    with pytest.raises(TypeError):
        MinutePriceStore._frame_prices_by_session(frame, days)


def test_local_finmind_valuation_fallback_retains_clock_and_never_assumes_shares(tmp_path):
    root = tmp_path / "finmind"
    obj, receipt = _finmind_valuation_fixture(root)
    store = MinutePriceStore([], [], require_receipts=True, finmind_root=root)
    store.prepare({"00883B": {"2026-01-02"}})
    assert store.prices("00883B", "2026-01-02") == {
        "2026-01-02T09:01+08:00": 11., "2026-01-02T13:30+08:00": 13.}
    proof = store.finmind_source_records["2026-01-02"]
    assert proof["execution_capacity_claimed"] is False
    assert proof["native_volume_use"] == "positive_trade_observation_only_not_shares_or_liquidity"
    store.assert_sources_unchanged()
    obj.write_bytes(b"tampered")
    with pytest.raises(RuntimeError, match="source changed"):
        store.assert_sources_unchanged()


def test_optional_opening_backup_fills_only_the_missing_minute(tmp_path, monkeypatch):
    root = tmp_path / "finmind"
    _finmind_valuation_fixture(root)
    day = "2026-01-02"
    later = {f"{day}T09:02+08:00": 99.}
    for enabled in (False, True):
        store = MinutePriceStore([tmp_path / "primary"], [], require_receipts=True,
            finmind_root=root, complete_opening_prices=enabled)
        monkeypatch.setattr(store, "_root_prices", lambda *_: dict(later))
        prices = store.prices("00883B", day)
        assert prices[f"{day}T09:02+08:00"] == 99.
        assert (f"{day}T09:01+08:00" in prices) is enabled
        assert f"{day}T13:30+08:00" not in prices
        if enabled:
            assert prices[f"{day}T09:01+08:00"] == 11.
            assert store.opening_backup_records[("00883B", day)]["valuation_only"] is True
            store.assert_sources_unchanged()


def test_optional_backup_does_not_replace_a_primary_opening_observation(tmp_path, monkeypatch):
    root = tmp_path / "finmind"
    _finmind_valuation_fixture(root)
    store = MinutePriceStore([tmp_path / "primary"], [], require_receipts=True,
        finmind_root=root, complete_opening_prices=True)
    monkeypatch.setattr(store, "_root_prices", lambda *_: {"2026-01-02T09:01+08:00": 10.})
    assert store.prices("00883B", "2026-01-02")["2026-01-02T09:01+08:00"] == 10.
    assert store.finmind_source_records == {} and store.opening_backup_records == {}


def test_empty_optional_provider_receipt_is_not_zero_trade_or_a_price(tmp_path, monkeypatch):
    root, day = tmp_path / "finmind", "2026-01-02"
    receipt = root / "receipts/TaiwanStockKBar/all" / (day + ".json")
    receipt.parent.mkdir(parents=True)
    receipt.write_text(json.dumps({"status": "observed_empty", "dataset": "TaiwanStockKBar",
                                  "data_id": "", "partition": day}))
    store = MinutePriceStore([tmp_path / "primary"], [], require_receipts=True,
        finmind_root=root, complete_opening_prices=True)
    later = {f"{day}T09:02+08:00": 99.}
    monkeypatch.setattr(store, "_root_prices", lambda *_: dict(later))
    assert store.prices("00883B", day) == later and store.opening_backup_records == {}
    store.assert_sources_unchanged()
    receipt.write_text(receipt.read_text() + "\n")
    with pytest.raises(RuntimeError, match="source changed"):
        store.assert_sources_unchanged()


def test_opening_backup_refuses_to_change_a_pinned_execution_source(tmp_path):
    with pytest.raises(ValueError, match="not an execution-pinned replay"):
        MinutePriceStore([], [], require_receipts=True, complete_opening_prices=True,
                         replay_symbol_sources={})


@pytest.mark.parametrize("broken", ["digest", "identity", "rows", "escape"])
def test_local_finmind_valuation_rejects_bad_receipt(tmp_path, broken):
    root = tmp_path / "finmind"
    _, path = _finmind_valuation_fixture(root)
    receipt = json.loads(path.read_text())
    if broken == "digest": receipt["sha256"] = "f" * 64
    elif broken == "identity": receipt["partition"] = "2026-01-05"
    elif broken == "rows": receipt["rows"] += 1
    else: receipt["parquet_path"] = "../outside.parquet"
    path.write_text(json.dumps(receipt))
    store = MinutePriceStore([], [], require_receipts=True, finmind_root=root)
    with pytest.raises(RuntimeError):
        store.prices("00883B", "2026-01-02")


def test_local_finmind_valuation_cannot_override_exact_execution_source_pins(tmp_path):
    root = tmp_path / "finmind"
    _finmind_valuation_fixture(root)
    store = MinutePriceStore([], [], require_receipts=True, finmind_root=root,
        replay_symbol_sources={("2026-01-02", "00883B"): [tmp_path / "required.parquet"]})
    assert store.prices("00883B", "2026-01-02") == {}
    assert store.finmind_source_records == {}


def test_public_history_prewarm_uses_only_local_read_only_gateway(monkeypatch) -> None:
    from scripts import maintain_tw_day_trade_minute_curves as maintenance

    observed = {}

    class Response:
        status = 200

        def getheader(self, name):
            assert name == "Content-Type"
            return "application/json; charset=utf-8"

        def read(self, maximum):
            assert maximum == 64 * 1024 * 1024 + 1
            return b'{"returned_points":317840}'

    class Connection:
        def __init__(self, host, port, *, timeout):
            observed["connection"] = (host, port, timeout)

        def request(self, method, path):
            observed["request"] = (method, path)

        def getresponse(self):
            return Response()

        def close(self):
            observed["closed"] = True

    monkeypatch.setenv("STOCKAGENT_PUBLIC_DASHBOARD_PORT", "8770")
    monkeypatch.setattr(maintenance, "HTTPConnection", Connection)
    receipt = maintenance._prewarm_public_history()
    assert receipt["status"] == "warmed"
    assert observed == {
        "connection": ("127.0.0.1", 8770, 20),
        "request": (
            "GET",
            "/tw-day-trade/api/history?range=all&resolution=1m&encoding=v2",
        ),
        "closed": True,
    }


def test_public_history_prewarm_failure_is_not_source_failure(monkeypatch) -> None:
    from scripts import maintain_tw_day_trade_minute_curves as maintenance

    monkeypatch.setenv("STOCKAGENT_PUBLIC_DASHBOARD_PORT", "0")
    assert maintenance._prewarm_public_history() == {
        "status": "not_warmed", "reason": "invalid_local_port"
    }

    class Connection:
        def __init__(self, host, port, *, timeout):
            pass

        def request(self, method, path):
            raise ConnectionRefusedError("gateway is down")

        def close(self):
            pass

    monkeypatch.setenv("STOCKAGENT_PUBLIC_DASHBOARD_PORT", "8770")
    monkeypatch.setattr(maintenance, "HTTPConnection", Connection)
    receipt = maintenance._prewarm_public_history()
    assert receipt["status"] == "not_warmed"
    assert receipt["reason"] == "ConnectionRefusedError"


def test_same_day_conversion_reversal_shares_terminal_accounting_clock() -> None:
    original = {"kind": "short_conversion_tax_and_handling"}
    reversal = {
        "kind": "manual_same_day_close_conversion_reversal",
        "reverses_cost_id": "original",
    }
    interest = {"kind": "margin_short_borrow_interest"}

    assert _is_terminal_carry_cost(original) is True
    assert _is_terminal_carry_cost(reversal) is True
    assert _is_terminal_carry_cost(interest) is False


def test_minute_repair_checks_maintenance_cache_before_api_fallback() -> None:
    assert DEFAULT_LOCAL_MINUTE_ROOTS[0] == Path(
        "artifacts/data_repair/tw_day_trade_minute_curve/maintenance/current/fetched_kbars"
    )


def test_replay_and_independent_curve_use_identical_minute_source_priority() -> None:
    from scripts.rebuild_tw_day_trade_open_price_replay import (
        DEFAULT_MINUTE_DATA_ROOTS,
    )

    assert DEFAULT_LOCAL_MINUTE_ROOTS == DEFAULT_MINUTE_DATA_ROOTS


def test_tx_benchmark_preflight_rejects_missing_or_source_empty_receipt(
    tmp_path: Path,
) -> None:
    state = _tx_benchmark_source_state(tmp_path / "TXFR1", "2026-09-15")
    assert state["ready"] is False
    assert state["status"] == "missing_or_invalid"

    receipts = tmp_path / "TXFR1" / "receipts"
    receipts.mkdir(parents=True)
    (receipts / "trading_date=2026-09-15.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source": "shioaji_txfr1_historical_ticks_v1",
                "contract": "TXFR1",
                "trading_date": "2026-09-15",
                "status": "source_empty",
                "session_finalized": True,
            }
        ),
        encoding="utf-8",
    )
    state = _tx_benchmark_source_state(tmp_path / "TXFR1", "2026-09-15")
    assert state["ready"] is False
    assert state["status"] == "source_empty"


def test_tx_benchmark_preflight_accepts_complete_retained_capture(
    tmp_path: Path, monkeypatch,
) -> None:
    from scripts import maintain_tw_day_trade_minute_curves as maintenance

    capture = tmp_path / "fop"
    manifests = capture / "manifests" / "trade_date=2026-09-23"
    manifests.mkdir(parents=True)
    (manifests / "worker=test.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        maintenance,
        "_tx_front_contract_metadata",
        lambda **_kwargs: ({"code": "TXFJ6"}, [{"sha256": "receipt"}]),
    )
    monkeypatch.setattr(
        maintenance,
        "_tx_day_books",
        lambda **_kwargs: pl.DataFrame(
            {
                "snapshot_ts_ns": [int(datetime(2026, 9, 23, 8, 45, tzinfo=ZoneInfo("Asia/Taipei")).timestamp() * 1e9)],
                "bid_price_1": [26000.0],
                "ask_price_1": [26001.0],
            }
        ),
    )
    state = _tx_benchmark_source_state(tmp_path / "missing-history", "2026-09-23", capture)
    assert state["ready"] is True
    assert state["source"] == "retained_shioaji_fop_book_1s"
    assert state["minutes"] == 300
    assert state["fresh_minutes"] == 1


def test_tx_benchmark_preflight_rejects_invalid_capture_without_history_fallback(
    tmp_path: Path, monkeypatch,
) -> None:
    from scripts import maintain_tw_day_trade_minute_curves as maintenance

    capture = tmp_path / "fop"
    manifests = capture / "manifests" / "trade_date=2026-09-23"
    manifests.mkdir(parents=True)
    (manifests / "worker=test.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        maintenance,
        "_tx_front_contract_metadata",
        lambda **_kwargs: (_ for _ in ()).throw(ValueError("invalid manifest")),
    )
    state = _tx_benchmark_source_state(tmp_path / "missing-history", "2026-09-23", capture)
    assert state["ready"] is False
    assert state["status"] == "invalid_capture"
    assert "invalid manifest" in state["reason"]


def test_stock_calendar_preflight_requires_verified_selected_session(
    tmp_path: Path, monkeypatch,
) -> None:
    from scripts import maintain_tw_day_trade_minute_curves as maintenance

    missing = _stock_calendar_source_state(tmp_path, "2026-09-22")
    assert missing["ready"] is False
    assert missing["status"] == "missing_or_invalid"

    monkeypatch.setattr(
        maintenance,
        "_validated_taiex_session_dates",
        lambda _root, _start, _end: ({date(2026, 9, 22)}, "verified-digest"),
    )
    ready = _stock_calendar_source_state(tmp_path, "2026-09-22")
    assert ready["ready"] is True
    assert ready["sha256"] == "verified-digest"


def test_verified_minute_source_cannot_change_before_publication(tmp_path: Path) -> None:
    path = tmp_path / "2026-09-01_2026-09-09.parquet"
    receipt = path.with_suffix(".receipt.json")
    path.write_bytes(b"original prices")
    receipt.write_text("{}")
    signature = tuple((p.stat().st_size, p.stat().st_mtime_ns, p.stat().st_ctime_ns)
                      for p in (path, receipt))
    store = MinutePriceStore(tmp_path, [], require_receipts=True)
    store._verified_chunk_dates[path] = (signature, {"2026-09-09"})
    store.assert_sources_unchanged()
    path.write_bytes(b"revised prices")
    with pytest.raises(RuntimeError, match="source changed"):
        store.assert_sources_unchanged()


def test_batch_chunk_lookup_resolves_source_root_once_and_fences_alias(tmp_path, monkeypatch):
    first, second, alias = tmp_path / "first", tmp_path / "second", tmp_path / "alias"
    first.mkdir()
    second.mkdir()
    alias.symlink_to(first, target_is_directory=True)
    original, calls = Path.resolve, []

    def resolve(path, *args, **kwargs):
        if path == alias:
            calls.append(path)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", resolve)
    store = MinutePriceStore([alias], [])
    for day in range(1, 29):
        assert store._chunk_paths(alias, "2330", f"2026-01-{day:02}") == ()
    assert len(calls) == 1
    store.assert_sources_unchanged()
    assert len(calls) == 2
    alias.unlink()
    alias.symlink_to(second, target_is_directory=True)
    with pytest.raises(RuntimeError, match="path alias changed during reconstruction"):
        store.assert_sources_unchanged()


def test_cached_chunk_path_identity_does_not_authorize_a_retargeted_file(tmp_path):
    first, second, alias = tmp_path / "first", tmp_path / "second", tmp_path / "alias"
    first.write_bytes(b"original")
    second.write_bytes(b"replacement")
    alias.symlink_to(first)
    store = MinutePriceStore([], [])
    assert store._source_path_identity(alias) == str(first)
    alias.unlink()
    alias.symlink_to(second)
    assert store._source_path_identity(alias) == str(first)
    with pytest.raises(RuntimeError, match="path alias changed during reconstruction"):
        store.assert_sources_unchanged()


def _batch_verified_chunk_fixture(tmp_path, monkeypatch):
    from downloader import download_shioaji_tw_minute_kbars as native
    root = tmp_path / "local"
    path = root / "minute_chunks/2330/2026-01-02_2026-01-04.parquet"
    path.parent.mkdir(parents=True)
    days = ["2026-01-02", "2026-01-03", "2026-01-04"]
    pl.DataFrame({"date": [date.fromisoformat(day) for day in days],
                  "ts": [datetime.fromisoformat(day + "T09:01") for day in days],
                  "Close": [10., 11., 12.], "Volume": [1., 2., 3.]}).write_parquet(path)
    path.with_suffix(".receipt.json").write_text(json.dumps({
        "returned_dates": days, "output_receipt": {"path": str(path)}}))
    validations = []
    monkeypatch.setattr(native, "minute_receipt_valid",
                        lambda *a, **k: validations.append((a, k)) or True)
    return MinutePriceStore([root], [], require_receipts=True), path, days, validations


def test_batch_chunk_metadata_checks_scale_with_files_not_requested_days(tmp_path, monkeypatch):
    from collections import Counter
    store, path, days, validations = _batch_verified_chunk_fixture(tmp_path, monkeypatch)
    signatures, original = Counter(), store._source_signature

    def signature(source):
        signatures[source] += 1
        return original(source)

    monkeypatch.setattr(store, "_source_signature", signature)
    store.prepare({"2330": set(days)})
    assert len(validations) == 1
    assert signatures == {path: 2, path.with_suffix(".receipt.json"): 2}
    assert store._batch_chunk_dates is None
    for day, expected in zip(days, [10., 11., 12.]):
        assert store.prices("2330", day) == {f"{day}T09:01+08:00": expected}


def test_batch_chunk_metadata_reuse_still_rejects_source_changed_while_decoding(tmp_path, monkeypatch):
    store, path, days, _validations = _batch_verified_chunk_fixture(tmp_path, monkeypatch)
    original = store._frame_prices_by_session

    def decode(frame, dates):
        values = original(frame, dates)
        path.write_bytes(path.read_bytes() + b"changed after verified read")
        return values

    monkeypatch.setattr(store, "_frame_prices_by_session", decode)
    with pytest.raises(RuntimeError, match="source changed during reconstruction"):
        store.prepare({"2330": set(days)})
    assert store._batch_chunk_dates is None


def test_margin_revaluation_uses_exact_replay_pinned_partition(
    tmp_path: Path,
) -> None:
    session_date = "2026-09-24"
    research = tmp_path / "research"
    partition = research / f"trade_date={session_date}" / "data.parquet"
    partition.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["2330"],
            "date": [date.fromisoformat(session_date)],
            "ts": [datetime(2026, 9, 24, 9, 2)],
            "Close": [1_250.0],
            "Volume": [10.0],
        }
    ).write_parquet(partition)
    digest = hashlib.sha256(partition.read_bytes()).hexdigest()
    store = MinutePriceStore(
        [research],
        [],
        require_receipts=True,
        replay_source_hashes={(session_date, str(partition)): digest},
    )

    assert store.prices("2330", session_date) == {
        "2026-09-24T09:02+08:00": 1_250.0
    }
    store.assert_sources_unchanged()
    partition.write_bytes(b"changed")
    with pytest.raises(RuntimeError, match="source changed"):
        store.assert_sources_unchanged()


def test_margin_revaluation_rejects_unpinned_research_partition(
    tmp_path: Path,
) -> None:
    research = tmp_path / "research"
    partition = research / "trade_date=2026-09-24" / "data.parquet"
    partition.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["2330"],
            "date": [date(2026, 9, 24)],
            "ts": [datetime(2026, 9, 24, 9, 2)],
            "Close": [1_250.0],
            "Volume": [10.0],
        }
    ).write_parquet(partition)
    store = MinutePriceStore([research], [], require_receipts=True)

    assert store.prices("2330", "2026-09-24") == {}


def test_prepared_replay_pair_filters_zero_volume_and_forbids_lower_source(
    tmp_path: Path,
) -> None:
    session_date = "2026-09-24"
    lower = tmp_path / "lower"
    chunk = lower / "minute_chunks" / "2330" / f"{session_date}_{session_date}.parquet"
    chunk.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "date": [date.fromisoformat(session_date)],
            "ts": [datetime(2026, 9, 24, 9, 2)],
            "Close": [999.0],
            "Volume": [100.0],
        }
    ).write_parquet(chunk)

    research = tmp_path / "research"
    partition = research / f"trade_date={session_date}" / "data.parquet"
    partition.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["2330", "2330"],
            "date": [date.fromisoformat(session_date)] * 2,
            "ts": [
                datetime(2026, 9, 24, 9, 1),
                datetime(2026, 9, 24, 9, 2),
            ],
            "Close": [100.0, 777.0],
            "Volume": [10.0, 0.0],
        }
    ).write_parquet(partition)
    digest = hashlib.sha256(partition.read_bytes()).hexdigest()
    store = MinutePriceStore(
        [lower, research],
        [],
        require_receipts=True,
        replay_source_hashes={(session_date, str(partition)): digest},
        replay_symbol_sources={
            (session_date, "2330"): {str(partition.resolve())}
        },
    )

    store.prepare({"2330": {session_date}})

    assert store.prices("2330", session_date) == {
        "2026-09-24T09:01+08:00": 100.0
    }


def test_prepare_does_not_read_a_lower_priority_duplicate_source(tmp_path: Path) -> None:
    first, second = tmp_path / "first", tmp_path / "second"
    _minute_file(first, "2330", [(datetime(2026, 8, 13, 9, 1), 102.0)])
    _minute_file(second, "2330", [(datetime(2026, 8, 13, 9, 1), 200.0)])
    for path in second.rglob("*.parquet"):
        path.write_bytes(b"This duplicate source must not be opened")
    store = MinutePriceStore([first, second], [])
    store.prepare({"2330": {"2026-08-13"}})
    assert store.prices("2330", "2026-08-13")["2026-08-13T09:01+08:00"] == 102.0
from stockagent.live.tw_day_trade_simulation import position_net_liquidation_pnl


def test_endpoint_preflight_finds_wholly_absent_market_session() -> None:
    rows = ({"session_date": "2026-09-03", "market": "present",
             "minute": f"2026-09-03T{clock}+08:00"}
            for clock in ("09:01", "13:30"))
    missing = missing_accepted_endpoints(
        rows, start=date(2026, 9, 3), end=date(2026, 9, 3),
        expected_sessions=["2026-09-03"], expected_markets={"present", "absent"},
    )
    assert missing == [{"session_date": "2026-09-03", "market": "absent",
                        "missing_clocks": ["09:01", "13:30"]}]


def test_rebuild_main_rejects_a_wholly_absent_expected_day_before_source_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import rebuild_tw_day_trade_minute_curves as rebuilder

    root = tmp_path / "ledger"
    root.mkdir()
    (root / "marks.jsonl").write_text("".join(
        json.dumps({"session_date": day, "market": "fixture",
                    "minute": f"{day}T{clock}+08:00"}) + "\n"
        for day in ("2026-01-02", "2026-01-05") for clock in ("09:01", "13:30")
    ), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", [
        "rebuild", "--state-dir", str(root), "--start-date", "2026-01-02",
        "--end-date", "2026-01-05", "--output-dir", str(tmp_path / "candidate"),
        "--expected-session-date", "2026-01-02", "--expected-session-date", "2026-01-03",
        "--expected-session-date", "2026-01-05", "--expected-market", "fixture",
        "--simulation",
    ])
    monkeypatch.setattr(rebuilder.MinutePriceStore, "prepare", lambda *_: pytest.fail("must not load prices"))
    with pytest.raises(RuntimeError, match="2026-01-03"):
        rebuilder.main()


def test_scoped_replayed_opening_repair_cannot_publish_directly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import rebuild_tw_day_trade_minute_curves as rebuilder

    monkeypatch.setattr(sys, "argv", [
        "rebuild", "--state-dir", str(tmp_path), "--start-date", "2026-01-02",
        "--end-date", "2026-01-02", "--output-dir", str(tmp_path / "candidate"),
        "--simulation", "--revalue-carried-marks", "--revalue-opening-marks",
        "--repair-replayed-opening-endpoint", "fixture:2026-01-02", "--publish",
    ])
    with pytest.raises(ValueError, match="direct publication is forbidden"):
        rebuilder.main()


def test_endpoint_preflight_reuses_only_unchanged_complete_marks(tmp_path: Path) -> None:
    day = "2026-09-03"
    marks = tmp_path / "marks.jsonl"
    marks.write_text("".join(
        json.dumps({"session_date": day, "market": "test", "minute": f"{day}T{clock}+08:00"}) + "\n"
        for clock in ("09:01", "13:30")
    ), encoding="utf-8")
    scope = {"completed": [day], "markets": {"test"}}
    missing, receipt = _missing_endpoints_with_cache(marks, previous_status=None, **scope)
    assert missing == []
    assert receipt["reused"] is False
    missing, reused = _missing_endpoints_with_cache(
        marks, previous_status={"accepted_endpoint_preflight": receipt}, **scope
    )
    assert missing == []
    assert reused["reused"] is True

    with marks.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"session_date": day, "market": "test", "minute": f"{day}T09:02+08:00"}) + "\n")
    missing, changed = _missing_endpoints_with_cache(
        marks, previous_status={"accepted_endpoint_preflight": receipt}, **scope
    )
    assert missing == []
    assert changed["reused"] is False
    missing, expanded = _missing_endpoints_with_cache(
        marks, completed=[day], markets={"test", "absent"},
        previous_status={"accepted_endpoint_preflight": changed},
    )
    assert missing == [{"session_date": day, "market": "absent", "missing_clocks": ["09:01", "13:30"]}]
    assert expanded["reused"] is False

    marks.write_text(json.dumps({"session_date": day, "market": "test", "minute": f"{day}T09:01+08:00"}) + "\n", encoding="utf-8")
    missing, _ = _missing_endpoints_with_cache(
        marks, previous_status={"accepted_endpoint_preflight": receipt}, **scope
    )
    assert missing == [{"session_date": day, "market": "test", "missing_clocks": ["13:30"]}]


def test_endpoint_preflight_rejects_ledger_change_during_scan(
    tmp_path: Path, monkeypatch,
) -> None:
    from scripts import maintain_tw_day_trade_minute_curves as maintenance

    day = "2026-09-03"
    marks = tmp_path / "marks.jsonl"
    marks.write_text(
        json.dumps({"session_date": day, "market": "test", "minute": f"{day}T09:01+08:00"}) + "\n",
        encoding="utf-8",
    )
    original = maintenance._iter_jsonl

    def changing_rows(path):
        yield from original(path)
        with marks.open("a", encoding="utf-8") as handle:
            handle.write("\n")

    monkeypatch.setattr(maintenance, "_iter_jsonl", changing_rows)
    with pytest.raises(RuntimeError, match="changed during accepted endpoint preflight"):
        _missing_endpoints_with_cache(
            marks, completed=[day], markets={"test"}, previous_status=None
        )


def test_maintenance_waits_before_subprocess_or_minute_data_loading(
    tmp_path: Path, monkeypatch,
) -> None:
    from scripts import maintain_tw_day_trade_minute_curves as maintenance

    (tmp_path / "marks.jsonl").write_text(
        json.dumps({"session_date": "2026-09-03", "market": "test",
                    "minute": "2026-09-03T09:01+08:00"}) + "\n",
        encoding="utf-8",
    )
    status = tmp_path / "status.json"
    monkeypatch.setattr(maintenance, "parse_args", lambda: SimpleNamespace(
        state_dir=tmp_path, output_root=tmp_path / "output", status_path=status,
        no_fetch=False,
    ))
    monkeypatch.setattr(maintenance, "_completed_scope", lambda _: (["2026-09-03"], {"test"}))

    def forbidden(*args, **kwargs):
        raise AssertionError("preflight must gate expensive validation and subprocesses")

    monkeypatch.setattr(maintenance, "_validate_current", forbidden)
    monkeypatch.setattr(maintenance.subprocess, "run", forbidden)
    maintenance.main()
    payload = json.loads(status.read_text())
    assert payload["status"] == "waiting_accepted_endpoints"
    assert payload["missing_endpoints"][0]["missing_clocks"] == ["13:30"]
    assert not (tmp_path / "output").exists()


def test_maintenance_no_fetch_still_protects_live_publication(
    tmp_path: Path, monkeypatch,
) -> None:
    from scripts import maintain_tw_day_trade_minute_curves as maintenance

    day = "2026-09-03"
    (tmp_path / "marks.jsonl").write_text(
        "".join(
            json.dumps({"session_date": day, "market": "test",
                        "minute": f"{day}T{clock}+08:00"}) + "\n"
            for clock in ("09:01", "13:30")
        ),
        encoding="utf-8",
    )
    status = tmp_path / "status.json"
    status.write_text(json.dumps({"completed_session_dates": [day]}), encoding="utf-8")
    monkeypatch.setattr(maintenance, "parse_args", lambda: SimpleNamespace(
        state_dir=tmp_path, output_root=tmp_path / "output", status_path=status,
        no_fetch=True,
    ))
    monkeypatch.setattr(maintenance, "_completed_scope", lambda _: ([day], {"test"}))
    monkeypatch.setattr(maintenance, "_tx_benchmark_source_state", lambda *a: {"ready": True})
    monkeypatch.setattr(maintenance, "_stock_calendar_source_state", lambda *a: {"ready": True, "sha256": "fixture"})
    monkeypatch.setattr(maintenance, "_official_session_scope_state", lambda *a: {"ready": True, "sha256": "fixture"})
    monkeypatch.setattr(maintenance, "_validate_current", lambda *a, **k: None)
    monkeypatch.setattr(maintenance, "_inspect_strategy_price_provenance", lambda *a, **k: {
        "unverified_opening_rows": 1,
        "unverified_interior_rows": 268,
    })
    monkeypatch.setattr(maintenance, "_validate_benchmarks", lambda *a, **k: None)
    monkeypatch.setattr(maintenance, "historical_query_is_protected", lambda _: True)
    monkeypatch.setattr(maintenance.subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(
        AssertionError("protected window must not start a rebuilder")
    ))

    maintenance.main()
    assert json.loads(status.read_text())["status"] == "waiting_live_priority_window"
    assert not (tmp_path / "output").exists()


def test_waiting_source_retry_never_skips_tx_preflight(tmp_path: Path, monkeypatch) -> None:
    from scripts import maintain_tw_day_trade_minute_curves as maintenance

    day = "2026-09-17"
    (tmp_path / "marks.jsonl").write_text(
        "".join(
            json.dumps({"session_date": day, "market": "test",
                        "minute": f"{day}T{clock}+08:00"}) + "\n"
            for clock in ("09:01", "13:30")
        ),
        encoding="utf-8",
    )
    status = tmp_path / "status.json"
    status.write_text(
        json.dumps({"status": "waiting_source", "completed_session_dates": [day]}),
        encoding="utf-8",
    )
    monkeypatch.setattr(maintenance, "parse_args", lambda: SimpleNamespace(
        state_dir=tmp_path, output_root=tmp_path / "output", status_path=status,
        no_fetch=False, tx_history_root=tmp_path / "TXFR1",
        fop_capture_root=tmp_path / "fop",
    ))
    monkeypatch.setattr(maintenance, "_completed_scope", lambda _: ([day], {"test"}))
    monkeypatch.setattr(maintenance, "_stock_calendar_source_state", lambda *a: {"ready": True, "sha256": "fixture"})
    monkeypatch.setattr(maintenance, "_official_session_scope_state", lambda *a: {"ready": True, "sha256": "fixture"})
    monkeypatch.setattr(maintenance.subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(
        AssertionError("unverified TX must not start a rebuilder")
    ))

    maintenance.main()
    assert json.loads(status.read_text())["status"] == "waiting_source"
    assert not (tmp_path / "output").exists()


def test_maintenance_waits_for_calendar_before_expensive_rebuild(
    tmp_path: Path, monkeypatch,
) -> None:
    from scripts import maintain_tw_day_trade_minute_curves as maintenance

    day = "2026-09-22"
    (tmp_path / "marks.jsonl").write_text(
        "".join(
            json.dumps({"session_date": day, "market": "test",
                        "minute": f"{day}T{clock}+08:00"}) + "\n"
            for clock in ("09:01", "13:30")
        ),
        encoding="utf-8",
    )
    status = tmp_path / "status.json"
    monkeypatch.setattr(maintenance, "parse_args", lambda: SimpleNamespace(
        state_dir=tmp_path, output_root=tmp_path / "output", status_path=status,
        no_fetch=False, calendar_root=tmp_path / "calendar",
    ))
    monkeypatch.setattr(maintenance, "_completed_scope", lambda _: ([day], {"test"}))
    monkeypatch.setattr(maintenance, "_tx_benchmark_source_state", lambda *a: {"ready": True})
    monkeypatch.setattr(maintenance.subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(
        AssertionError("missing official calendar must not start a rebuilder")
    ))

    maintenance.main()
    payload = json.loads(status.read_text())
    assert payload["status"] == "waiting_source"
    assert payload["failed_stage"] == "stock_session_calendar_preflight"
    assert not (tmp_path / "output").exists()


def test_calendar_scope_detects_a_wholly_unrecorded_official_session(monkeypatch, tmp_path: Path):
    from scripts import maintain_tw_day_trade_minute_curves as maintenance

    monkeypatch.setattr(maintenance, "_validated_taiex_session_dates", lambda *_: (
        [date(2026, 10, 29), date(2026, 10, 30), date(2026, 11, 2)], "calendar-sha"))
    result = maintenance._official_session_scope_state(tmp_path, ["2026-10-29", "2026-11-02"])
    assert result["ready"] is False
    assert result["missing_recorded_sessions"] == ["2026-10-30"]
    assert result["sha256"] == "calendar-sha"


def test_maintenance_does_not_treat_an_unrecorded_trading_day_as_complete(
    tmp_path: Path, monkeypatch,
) -> None:
    from scripts import maintain_tw_day_trade_minute_curves as maintenance

    days = ["2026-10-29", "2026-11-02"]
    (tmp_path / "marks.jsonl").write_text("".join(
        json.dumps({"session_date": day, "market": "test", "minute": f"{day}T{clock}+08:00"}) + "\n"
        for day in days for clock in ("09:01", "13:30")
    ), encoding="utf-8")
    status = tmp_path / "status.json"
    monkeypatch.setattr(maintenance, "parse_args", lambda: SimpleNamespace(
        state_dir=tmp_path, output_root=tmp_path / "output", status_path=status,
        no_fetch=True, calendar_root=tmp_path / "calendar",
    ))
    monkeypatch.setattr(maintenance, "_completed_scope", lambda _: (days, {"test"}))
    monkeypatch.setattr(maintenance, "_tx_benchmark_source_state", lambda *a: {"ready": True})
    monkeypatch.setattr(maintenance, "_stock_calendar_source_state", lambda *a: {"ready": True, "sha256": "calendar-sha"})
    monkeypatch.setattr(maintenance, "_validated_taiex_session_dates", lambda *_: (
        [date(2026, 10, 29), date(2026, 10, 30), date(2026, 11, 2)], "calendar-sha"))
    monkeypatch.setattr(maintenance, "_validate_current", lambda *a, **k: pytest.fail("must not validate a missing day"))
    monkeypatch.setattr(maintenance.subprocess, "run", lambda *a, **k: pytest.fail("must not launch a rebuild"))
    maintenance.main()
    payload = json.loads(status.read_text())
    assert payload["status"] == "waiting_unrecorded_official_sessions"
    assert payload["official_scope"]["missing_recorded_sessions"] == ["2026-10-30"]
    assert not (tmp_path / "output").exists()


def test_stock_benchmark_minute_uses_adjusted_units_without_adjusting_cost_basis() -> None:
    row = _benchmark_minute_row(
        {
            "entry_price": 100.0,
            "quantity": 1_000,
            "adjusted_quantity": 1_100.0,
            "initial_capital_twd": 100_000.0,
            "last_mark_price": 100.0,
            "daily_return_reference_price": 98.0,
        },
        minute=datetime(2026, 8, 13, 9, 1, tzinfo=ZoneInfo("Asia/Taipei")),
        price=100.0,
        fresh=True,
    )

    assert row["gross_pnl_twd"] == 10_000.0
    assert row["total_equity_twd"] == 110_000.0
    assert row["return_pct"] == pytest.approx(10.0)
    assert row["daily_return_pct"] == pytest.approx((100.0 / 98.0 - 1.0) * 100.0)


TAIPEI = ZoneInfo("Asia/Taipei")


def test_lossless_minute_history_keeps_every_point_beyond_chart_sample_limit(tmp_path: Path) -> None:
    from stockagent.live.tw_day_trade_dashboard import build_dashboard_history_snapshot
    from stockagent.live.public_dashboards import sanitize_tw_history

    rows = []
    for day_offset in range(10):
        start = datetime(2026, 8, 10, 9, 1, tzinfo=TAIPEI) + timedelta(days=day_offset)
        for offset in range(270):
            stamp = start + timedelta(minutes=offset)
            rows.append({
                "market": "test", "session_date": stamp.date().isoformat(),
                "minute": stamp.isoformat(timespec="minutes"),
                "initial_capital_twd": 10000.0,
                "total_equity_twd": 10000.0 + len(rows) + (offset % 3) * 5,
            })
    (tmp_path / "marks.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
    full = build_dashboard_history_snapshot(state_dir=tmp_path, range_key="all", resolution="1m")
    public = sanitize_tw_history(full)
    points = public["minute_series"][0]["points"]
    assert len(points) == 2700 == public["returned_points"] == public["raw_points_in_range"]
    assert public["downsampled"] is False
    assert points[0][1] == 0
    assert len({point[0] for point in points}) == 2700
    for source, point in zip(rows, points, strict=True):
        assert point[0] == int(datetime.fromisoformat(source["minute"]).timestamp() // 60)
        assert point[1] == pytest.approx((source["total_equity_twd"] / 10000 - 1) * 100)
    sampled = build_dashboard_history_snapshot(state_dir=tmp_path, range_key="all")
    assert sampled["downsampled"] is True
    assert len(sampled["history"]) <= 2000

    full_v2 = build_dashboard_history_snapshot(
        state_dir=tmp_path,
        range_key="all",
        resolution="1m",
        history_encoding="minute_columns_v2",
    )
    public_v2 = sanitize_tw_history(full_v2)
    encoded = public_v2["minute_series"][0]
    assert public_v2["history_encoding"] == "minute_columns_v2"
    assert len(public_v2["minute_axis"]) == 2700
    assert len(encoded["minute_indexes"]) == 2700
    assert encoded["minute_indexes"] == list(range(2700))
    assert encoded["return_pct"] == pytest.approx([point[1] for point in points])
    assert public_v2["returned_points"] == sum(
        len(series["minute_indexes"]) for series in public_v2["minute_series"]
    )


def test_public_minute_columns_refuse_non_numeric_embedded_data() -> None:
    from stockagent.live.public_dashboards import sanitize_tw_history, UnsafePublicDashboardPayload

    with pytest.raises(UnsafePublicDashboardPayload, match="minute point"):
        sanitize_tw_history({
            "simulation_only": True, "production_order_possible": False,
            "history_encoding": "minute_columns_v1",
            "minute_series": [{"series_id": "a", "points": [[1, 0, "secret", 0]]}],
        })

    with pytest.raises(UnsafePublicDashboardPayload, match="unequal minute columns"):
        sanitize_tw_history({
            "simulation_only": True, "production_order_possible": False,
            "returned_points": 2,
            "history_encoding": "minute_columns_v2",
            "minute_axis": [1, 2],
            "minute_series": [{
                "series_id": "a", "series_type": "strategy",
                "minute_indexes": [0, 1], "return_pct": [0.0],
                "cumulative_return_pct": [0.0, 1.0], "quality_flags": [0, 0],
            }],
        })


def test_existing_bracket_aware_strategy_marks_validate_without_rebuild() -> None:
    start = datetime(2026, 8, 13, 9, 1, tzinfo=TAIPEI)
    rows = []
    for market in ("a", "b", "c"):
        for offset in range(270):
            observed = start + timedelta(minutes=offset)
            rows.append(
                {
                    "session_date": "2026-08-13",
                    "market": market,
                    "minute": observed.isoformat(timespec="minutes"),
                    "valuation_stale": offset == 1,
                }
            )

    preserved, stats = validate_existing_strategy_marks(
        rows, start=date(2026, 8, 13), end=date(2026, 8, 13)
    )

    assert preserved == rows
    assert stats["generated_rows"] == 810
    assert stats["rows_with_carried_prices"] == 3
    assert stats["existing_bracket_aware_marks_preserved"] is True


def test_carry_minute_validation_preserves_cash_income_and_cost_identity():
    from stockagent.live.tw_day_trade_simulation import MARGIN_CARRY_CONTRACT
    start = datetime(2026, 8, 13, 9, 1, tzinfo=TAIPEI)
    rows = [{"session_date": "2026-08-13", "market": "a",
             "minute": (start + timedelta(minutes=i)).isoformat(timespec="minutes"),
             "margin_carry_contract": MARGIN_CARRY_CONTRACT,
             "initial_capital_twd": 10000., "cumulative_realized_net_pnl_twd": 100.,
             "open_net_liquidation_pnl_twd": -70., "cumulative_carry_cost_twd": 10.,
             "cumulative_corporate_action_net_twd": 50., "total_equity_twd": 10070.,
             "historical_minute_replay": True,
             "minute_valuation_contract": "right_labelled_historical_last_trade_mark_v1",
             "valuation_source": "fixture_kbar", "valuation_executable": False,
             "fresh_trade_notional_coverage_ratio": 1., "fresh_trade_position_count": 1,
             "last_trade_carried_position_count": 0, "missing_price_position_count": 0}
            for i in range(270)]
    assert validate_existing_strategy_marks(rows, start=start.date(), end=start.date())[0] == rows
    rows[42]["total_equity_twd"] += 1
    with pytest.raises(RuntimeError, match="NAV mismatch"):
        validate_existing_strategy_marks(rows, start=start.date(), end=start.date())
    rows[42]["total_equity_twd"] -= 1
    rows[42]["missing_price_position_count"] = 1
    with pytest.raises(RuntimeError, match="unsourced"):
        validate_existing_strategy_marks(rows, start=start.date(), end=start.date())


def test_existing_mark_validation_does_not_require_preserved_benchmarks() -> None:
    positions = {
        "2026-08-13": {
            "tw_day_trade": [{"symbol": "2317"}],
        }
    }

    required = required_symbol_dates(
        positions,
        ["2026-08-13"],
        include_stock_benchmarks=False,
    )

    assert required == {"2317": {"2026-08-13"}}


def test_manual_no_trade_pair_requires_complete_hash_verified_receipt(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state"
    raw = tmp_path / "raw"
    receipts = state / "settlement_receipts"
    day_source = raw / "tpex_daily_ohlcv/2026-09-10.json"
    prior_source = raw / "tpex_daily_ohlcv/2026-09-09.json"
    receipts.mkdir(parents=True)
    day_source.parent.mkdir(parents=True)
    day_source.write_text('{"date":"2026-09-10","rows":[]}', encoding="utf-8")
    prior_source.write_text('{"date":"2026-09-09","rows":[]}', encoding="utf-8")

    import hashlib

    def digest(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    receipt = {
        "status": "applied",
        "simulation_only": True,
        "contract": "user_authorized_last_traded_price_paper_settlement_v1",
        "session_date": "2026-09-10",
        "raw_root": str(raw),
        "last_traded_price_for": ["6680"],
        "sources": [
            {"path": str(day_source), "sha256": digest(day_source)},
            {"path": str(prior_source), "sha256": digest(prior_source)},
        ],
        "entries": [
            {
                "symbol": "6680",
                "price_basis": "last_available_trade",
                "price_date": "2026-09-09",
                "official_date": "2026-09-09",
                "source_sha256": digest(prior_source),
            }
        ],
    }
    receipt_path = receipts / "2026-09-10-test.json"
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    assert verified_manual_no_trade_pairs(state) == {
        ("6680", "2026-09-10"): "manual_last_trade_settlement:2026-09-10-test.json"
    }

    prior_source.write_text("damaged", encoding="utf-8")
    assert verified_manual_no_trade_pairs(state) == {}


def _write_maintenance_scope(
    root: Path,
    *,
    current_open_positions: int,
    closing_auction_settled: bool,
) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "rebuild_receipt.json").write_text(
        json.dumps(
            {
                "sessions": [
                    {
                        "session_date": "2026-08-31",
                        "close": {"status": "settled_official_close"},
                    },
                    {
                        "session_date": "2026-09-01",
                        "close": {"status": "current_session_open"},
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    modes = {}
    for market in ("a", "b", "c"):
        modes[market] = {
            "session_date": "2026-09-01",
            "open_position_count": current_open_positions,
            "positions": {
                "2330": {"signed_shares": 1_000 if current_open_positions else 0}
            },
            "closing_auction_settled_at": (
                "2026-09-01T13:30:01+08:00" if closing_auction_settled else None
            ),
            "engine_status": "active" if current_open_positions else "terminal",
        }
    (root / "state.json").write_text(json.dumps({"modes": modes}), encoding="utf-8")


def test_minute_curve_maintenance_defers_open_current_session(tmp_path: Path) -> None:
    _write_maintenance_scope(
        tmp_path,
        current_open_positions=1,
        closing_auction_settled=False,
    )

    sessions, markets = _completed_scope(tmp_path)

    assert sessions == ["2026-08-31"]
    assert markets == {"a", "b", "c"}


def test_minute_curve_maintenance_includes_flat_terminal_session(
    tmp_path: Path,
) -> None:
    _write_maintenance_scope(
        tmp_path,
        current_open_positions=0,
        closing_auction_settled=True,
    )

    sessions, markets = _completed_scope(tmp_path)

    assert sessions == ["2026-08-31", "2026-09-01"]
    assert markets == {"a", "b", "c"}


def test_completed_scope_retains_sessions_between_replay_and_current_state(tmp_path: Path) -> None:
    _write_maintenance_scope(tmp_path, current_open_positions=0, closing_auction_settled=True)
    state = json.loads((tmp_path / "state.json").read_text())
    for mode in state["modes"].values():
        mode["session_date"] = "2026-09-04"
    (tmp_path / "state.json").write_text(json.dumps(state))
    events = [
        {"event": "closing_auction_settled", "market": "a", "recorded_at": "2026-09-03T05:30:00Z"},
        {"event": "signal_registered", "market": "a", "recorded_at": "2026-09-02T09:00:00+08:00"},
        {"event": "closing_auction_settled", "market": "retired", "recorded_at": "2026-09-02T13:30:00+08:00"},
    ]
    (tmp_path / "events.jsonl").write_text("\n".join(json.dumps(x) for x in events))
    sessions, _ = _completed_scope(tmp_path)
    assert sessions == ["2026-08-31", "2026-09-03", "2026-09-04"]


def test_minute_curve_maintenance_requires_all_benchmark_minutes(
    tmp_path: Path,
) -> None:
    day = "2026-09-01"
    marks = []
    for benchmark_id, start_clock, points in (
        ("benchmark_0050", datetime(2026, 9, 1, 9, 0), 271),
        ("benchmark_2330", datetime(2026, 9, 1, 9, 0), 271),
        ("benchmark_tx_continuous", datetime(2026, 9, 1, 8, 45), 300),
    ):
        for offset in range(points):
            observed = start_clock + timedelta(minutes=offset)
            marks.append(
                {
                    "benchmark_id": benchmark_id,
                    "session_date": day,
                    "minute": observed.replace(tzinfo=TAIPEI).isoformat(
                        timespec="minutes"
                    ),
                }
            )
    (tmp_path / "benchmark_history.json").write_text(
        json.dumps({"marks": marks}), encoding="utf-8"
    )

    result = _validate_benchmarks(
        tmp_path,
        completed_session_dates=[day],
    )

    assert result["rows"]["benchmark_0050"] == 271
    assert result["rows"]["benchmark_tx_continuous"] == 300

    marks.pop()
    (tmp_path / "benchmark_history.json").write_text(
        json.dumps({"marks": marks}), encoding="utf-8"
    )
    with pytest.raises(RuntimeError, match="minute count"):
        _validate_benchmarks(tmp_path, completed_session_dates=[day])


def _minute_file(root: Path, symbol: str, rows: list[tuple[datetime, float]]) -> None:
    target = root / "minute_chunks" / symbol / "2026-08-13_2026-08-13.parquet"
    target.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "date": [date(2026, 8, 13)] * len(rows),
            "ts": [row[0].replace(tzinfo=None) for row in rows],
            "Close": [row[1] for row in rows],
        }
    ).write_parquet(target)


def _position() -> dict[str, object]:
    return {
        "position_id": "tw_day_trade:2026-08-13:2330",
        "symbol": "2330",
        "side": "long",
        "filled_shares": 1_000,
        "signed_shares": 0,
        "entry_price": 101.0,
        "sizing_open_price": 100.0,
        "entry_fee_twd": 30.0,
        "remaining_entry_fee_twd": 0.0,
        "buy_fee_rate": 0.001425,
        "sell_fee_rate": 0.002425,
        "commission_rebate_rate": 0.00114,
    }


def test_archived_position_uses_canonical_net_liquidation_math() -> None:
    position = _position()
    result = position_net_liquidation_pnl(
        position,
        102.0,
        signed_shares=1_000,
        remaining_entry_fee_twd=30.0,
    )
    assert result == pytest.approx(1_000.0 - 30.0 - 102_000.0 * 0.001285)


def test_tick_resample_is_right_labelled_and_keeps_close_auction() -> None:
    ticks = SimpleNamespace(
        ts=[
            int(np.datetime64("2026-08-13T09:00:10", "ns").astype(np.int64)),
            int(np.datetime64("2026-08-13T09:00:59", "ns").astype(np.int64)),
            int(np.datetime64("2026-08-13T13:30:00", "ns").astype(np.int64)),
        ],
        close=[100.0, 101.0, 102.0],
        volume=[1, 2, 3],
    )
    result = _ticks_to_minute_frame(ticks, symbol="2330", session_date="2026-08-13")
    assert result["ts"].to_list() == [
        datetime(2026, 8, 13, 9, 1),
        datetime(2026, 8, 13, 13, 30),
    ]
    assert result["Close"].to_list() == [101.0, 102.0]
    assert result["Volume"].to_list() == [3.0, 3.0]


def test_minute_store_uses_ordered_local_kbar_before_tick_cache(
    tmp_path: Path,
) -> None:
    preferred = tmp_path / "preferred"
    secondary = tmp_path / "secondary"
    tick_root = tmp_path / "ticks"
    minute = datetime(2026, 8, 13, 9, 1)
    _minute_file(preferred, "2330", [(minute, 101.0)])
    _minute_file(secondary, "2330", [(minute, 102.0)])
    tick_path = tick_root / "2330" / "2026-08-13.parquet"
    tick_path.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "date": [date(2026, 8, 13)],
            "ts": [minute],
            "Close": [103.0],
        }
    ).write_parquet(tick_path)

    required = {"2330": {"2026-08-13"}}
    store = MinutePriceStore([preferred, secondary], tick_root)
    store.prepare(required)
    prices = store.prices("2330", "2026-08-13")
    coverage = store.coverage(required)

    assert prices["2026-08-13T09:01+08:00"] == 101.0
    assert coverage["available_pairs"] == 1
    assert coverage["missing_pairs"] == 0
    assert coverage["source_pair_counts"] == {f"local_kbar:{preferred.resolve()}": 1}


def test_minute_store_reads_materialized_trade_date_partition(tmp_path: Path) -> None:
    research = tmp_path / "research"
    partition = research / "trade_date=2026-08-13" / "data.parquet"
    partition.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["0050", "2330"],
            "date": [date(2026, 8, 13)] * 2,
            "ts": [datetime(2026, 8, 13, 9, 1)] * 2,
            "Close": [100.0, 2_400.0],
        }
    ).write_parquet(partition)
    required = {"0050": {"2026-08-13"}, "2330": {"2026-08-13"}}
    store = MinutePriceStore([research], tmp_path / "ticks")

    store.prepare(required)

    assert store.prices("0050", "2026-08-13")["2026-08-13T09:01+08:00"] == 100.0
    assert store.prices("2330", "2026-08-13")["2026-08-13T09:01+08:00"] == 2_400.0
    assert store.coverage(required)["missing_pairs"] == 0


def test_fetch_missing_kbars_never_starts_collector_when_local_data_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    local = tmp_path / "local"
    _minute_file(
        local,
        "2330",
        [(datetime(2026, 8, 13, 9, 1), 2_400.0)],
    )
    required = {"2330": {"2026-08-13"}}
    store = MinutePriceStore([local], tmp_path / "ticks")
    store.prepare(required)

    def unexpected_run(*args: object, **kwargs: object) -> None:
        raise AssertionError("collector must not start for a locally covered pair")

    monkeypatch.setattr(
        "scripts.rebuild_tw_day_trade_minute_curves.subprocess.run",
        unexpected_run,
    )
    result = fetch_missing_kbars(
        store,
        required,
        output_root=tmp_path / "fetched",
        simulation=True,
        workers=1,
        requests_per_second=5.0,
        max_traffic_fraction=0.90,
    )

    assert result == {
        "api_process_started": False,
        "reason": "all_required_symbol_dates_found_locally",
        "requested_symbols": 0,
        "missing_before": 0,
        "missing_after": 0,
        "api_requests_started": 0,
    }


def test_fetch_missing_kbars_delegates_only_true_gap_to_canonical_collector(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetched = tmp_path / "fetched"
    required = {"2330": {"2026-08-13"}}
    store = MinutePriceStore([tmp_path / "local", fetched], tmp_path / "ticks")
    store.prepare(required)

    def fake_run(command: list[str], *, cwd: Path, check: bool) -> SimpleNamespace:
        assert cwd == Path(__file__).resolve().parents[1]
        assert check is False
        assert command[1].endswith("download_shioaji_tw_minute_kbars.py")
        assert command[command.index("--symbols") + 1] == "2330"
        _minute_file(
            fetched,
            "2330",
            [(datetime(2026, 8, 13, 9, 1), 2_400.0)],
        )
        (fetched / "download_summary.json").write_text(
            json.dumps(
                {
                    "api_requests_started_this_run": 1,
                    "stopped_for_traffic": False,
                    "stopped_for_market_hours": False,
                }
            ),
            encoding="utf-8",
        )
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(
        "scripts.rebuild_tw_day_trade_minute_curves.subprocess.run", fake_run
    )
    result = fetch_missing_kbars(
        store,
        required,
        output_root=fetched,
        simulation=True,
        workers=1,
        requests_per_second=5.0,
        max_traffic_fraction=0.90,
    )

    assert result["api_process_started"] is True
    assert result["requested_symbols"] == 1
    assert result["missing_before"] == 1
    assert result["missing_after"] == 0
    assert result["api_requests_started"] == 1


def test_fetch_missing_kbars_failure_names_the_remaining_pairs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetched = tmp_path / "fetched"
    required = {"5314": {"2026-09-11"}}
    store = MinutePriceStore([tmp_path / "local", fetched], tmp_path / "ticks")
    store.prepare(required)

    def fake_run(command: list[str], *, cwd: Path, check: bool) -> SimpleNamespace:
        fetched.mkdir(parents=True, exist_ok=True)
        (fetched / "download_summary.json").write_text(
            json.dumps(
                {
                    "api_requests_started_this_run": 1,
                    "stopped_for_traffic": False,
                    "stopped_for_market_hours": False,
                }
            ),
            encoding="utf-8",
        )
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(
        "scripts.rebuild_tw_day_trade_minute_curves.subprocess.run", fake_run
    )
    with pytest.raises(RuntimeError, match='"symbol": "5314"') as caught:
        fetch_missing_kbars(
            store,
            required,
            output_root=fetched,
            simulation=True,
            workers=1,
            requests_per_second=5.0,
            max_traffic_fraction=0.90,
        )

    assert '"session_date": "2026-09-11"' in str(caught.value)


def test_strategy_minute_rebuild_preserves_endpoints_and_discloses_carry(
    tmp_path: Path,
) -> None:
    kbar_root = tmp_path / "kbars"
    _minute_file(
        kbar_root,
        "2330",
        [(datetime(2026, 8, 13, 9, 2), 102.0)],
    )
    store = MinutePriceStore(kbar_root, tmp_path / "ticks")
    opening = {
        "minute": "2026-08-13T09:01+08:00",
        "recorded_at": "2026-08-13T09:01:00+08:00",
        "session_date": "2026-08-13",
        "market": "tw_day_trade",
        "initial_capital_twd": 10_000_000.0,
        "cumulative_realized_net_pnl_twd": 50.0,
        "open_net_liquidation_pnl_twd": -100.0,
        "total_equity_twd": 9_999_950.0,
        "open_position_count": 1,
        "stale_position_count": 0,
        "valuation_stale": False,
    }
    closing = {
        **opening,
        "minute": "2026-08-13T13:30+08:00",
        "recorded_at": "2026-08-13T13:30:00+08:00",
        "cumulative_realized_net_pnl_twd": 500.0,
        "open_net_liquidation_pnl_twd": 0.0,
        "total_equity_twd": 10_000_500.0,
        "open_position_count": 0,
    }
    rows, stats = rebuild_strategy_marks(
        [opening, closing],
        {"2026-08-13": {"tw_day_trade": [_position()]}},
        store,
        start=date(2026, 8, 13),
        end=date(2026, 8, 13),
    )
    assert len(rows) == 270
    assert rows[0] == opening
    assert rows[-1] == closing
    minute_0902 = rows[1]
    assert minute_0902["open_net_liquidation_pnl_twd"] == pytest.approx(
        position_net_liquidation_pnl(
            _position(),
            102.0,
            signed_shares=1_000,
            remaining_entry_fee_twd=30.0,
        )
    )
    assert minute_0902["fresh_trade_notional_coverage_ratio"] == 1.0
    assert rows[2]["last_trade_carried_position_count"] == 1
    assert rows[2]["valuation_stale"] is True
    assert stats["generated_rows"] == 270


def test_opening_revaluation_uses_same_close_and_fees_as_following_minute(tmp_path: Path) -> None:
    _minute_file(tmp_path / "kbars", "2330", [(datetime(2026, 8, 13, 9, 1), 102.0)])
    store = MinutePriceStore(tmp_path / "kbars", tmp_path / "ticks")
    opening = {"minute": "2026-08-13T09:01+08:00", "session_date": "2026-08-13",
               "market": "tw_day_trade", "initial_capital_twd": 10_000_000.0,
               "cumulative_realized_net_pnl_twd": 50.0,
               "total_equity_twd": 10_000_020.0, "open_position_count": 1}
    closing = {**opening, "minute": "2026-08-13T13:30+08:00", "open_position_count": 0}
    positions = {"2026-08-13": {"tw_day_trade": [_position()]}}
    rows, stats = rebuild_strategy_marks([opening, closing], positions, store,
        start=date(2026, 8, 13), end=date(2026, 8, 13), revalue_opening_marks=True)
    expected = 10_000_000 + 50 + 1_000 * (102 - 101) - 30 - 102_000 * (.002425 - .00114)
    assert rows[0]["total_equity_twd"] == pytest.approx(expected)
    assert rows[1]["total_equity_twd"] == rows[0]["total_equity_twd"]
    assert rows[0]["fresh_trade_position_count"] == 1
    assert rows[1]["last_trade_carried_position_count"] == 1
    assert rows[-1] == closing
    assert stats["opening_revaluations"][0]["before_equity_twd"] == 10_000_020
    missing_store = MinutePriceStore(tmp_path / "missing", tmp_path / "ticks")
    with pytest.raises(RuntimeError, match="missing completed 09:01"):
        rebuild_strategy_marks([opening, closing], positions, missing_store,
            start=date(2026, 8, 13), end=date(2026, 8, 13), revalue_opening_marks=True)


@pytest.mark.parametrize("invalid_field", [None, "fill_contract", "simulation_only",
                                         "synthetic_fill", "entry_price_source"])
def test_full_0901_price_carry_is_explicit_and_never_a_fresh_trade(tmp_path, invalid_field):
    from copy import deepcopy
    from stockagent.live.tw_day_trade_simulation import LEGACY_REPLAY_FILL_CONTRACT_0901_FULL_COUNTERFACTUAL

    day, market = "2026-08-13", "tw_day_trade"
    position = _position()
    opening = {"minute": f"{day}T09:01+08:00", "recorded_at": f"{day}T09:01:00+08:00",
               "session_date": day, "market": market, "initial_capital_twd": 10_000_000.0,
               "cumulative_realized_net_pnl_twd": 50.0, "open_position_count": 1,
               "total_equity_twd": 10_000_000.0}
    closing = {**opening, "minute": f"{day}T13:30+08:00",
               "recorded_at": f"{day}T13:30:00+08:00", "open_position_count": 0,
               "cumulative_realized_net_pnl_twd": 500.0, "total_equity_twd": 10_000_500.0}
    entry = {"position_id": position["position_id"], "market": market, "session_date": day,
             "symbol": "2330", "purpose": "entry", "quantity": 1000, "price": 101.0,
             "fee_and_tax_twd": 30.0, "fill_at": f"{day}T09:01:00+08:00",
             "recorded_at": "2026-10-21T15:00:00+08:00",
             "fill_contract": LEGACY_REPLAY_FILL_CONTRACT_0901_FULL_COUNTERFACTUAL,
             "simulation_only": True, "simulation_replay": True,
             "counterfactual_0901_price_fill": True, "synthetic_fill": False,
             "entry_price_method": "minute_close", "entry_price_source": "retained_0901_source_close"}
    if invalid_field:
        entry[invalid_field] = True if invalid_field == "synthetic_fill" else None
    fills = [entry, {**entry, "purpose": "exit", "fill_at": f"{day}T13:30:00+08:00",
                     "entry_fee_allocated_twd": 30.0, "net_pnl_twd": 450.0}]
    before = deepcopy(fills)
    _minute_file(tmp_path / "kbars", "2330", [(datetime(2026, 8, 13, 9, 2), 102.0)])
    store = MinutePriceStore(tmp_path / "kbars", tmp_path / "ticks")
    kwargs = dict(start=date.fromisoformat(day), end=date.fromisoformat(day),
                  fill_rows=fills, revalue_opening_marks=True)
    if invalid_field:
        with pytest.raises(RuntimeError, match="missing completed 09:01"):
            rebuild_strategy_marks([opening, closing], {day: {market: [position]}}, store, **kwargs)
        return
    rows, stats = rebuild_strategy_marks([opening, closing], {day: {market: [position]}}, store, **kwargs)
    assert len(rows) == 270
    assert rows[0]["total_equity_twd"] == pytest.approx(10_000_050 + position_net_liquidation_pnl(
        position, 101.0, signed_shares=1000, remaining_entry_fee_twd=30.0))
    assert rows[0]["valuation_stale"] is True
    assert rows[0]["fresh_trade_position_count"] == 0
    assert rows[0]["last_trade_carried_position_count"] == 1
    assert rows[1]["valuation_stale"] is False
    assert rows[1]["fresh_trade_position_count"] == 1
    assert rows[-1] == closing
    assert stats["opening_full_target_fill_price_carried_symbol_days"] == 1
    assert fills == before


@pytest.fixture
def clocked_flat_book():
    day, market = "2026-08-13", "tw_day_trade"
    first = {**_position(), "position_id": "first", "filled_shares": 2000,
             "entry_price": 101.0, "entry_fee_twd": 30.0}
    late = {**_position(), "position_id": "late", "symbol": "2317",
            "entry_price": 200.0, "entry_fee_twd": 5.0}

    def fill(identity, clock, purpose, quantity, price, **extra):
        return {"position_id": identity, "symbol": "2330" if identity == "first" else "2317",
                "market": market, "session_date": day, "purpose": purpose,
                "quantity": quantity, "price": price, "fill_at": f"{day}T{clock}+08:00",
                # Reconstruction time is not the execution clock.
                "recorded_at": "2026-10-21T15:00:00+08:00", **extra}

    fills = [fill("first", "09:01:00", "entry", 1000, 100.0, fee_and_tax_twd=10.0),
             fill("first", "09:03:00", "entry_completion", 1000, 102.0, fee_and_tax_twd=20.0),
             fill("late", "09:09:00", "entry", 1000, 200.0, fee_and_tax_twd=5.0),
             fill("first", "12:30:00", "exit", 1000, 102.0,
                  entry_fee_allocated_twd=15.0, net_pnl_twd=500.0),
             fill("first", "13:30:00", "exit", 1000, 103.0,
                  entry_fee_allocated_twd=15.0, net_pnl_twd=600.0),
             fill("late", "13:30:00", "exit", 1000, 201.0,
                  entry_fee_allocated_twd=5.0, net_pnl_twd=40.0)]
    opening = {"minute": f"{day}T09:01+08:00", "recorded_at": f"{day}T09:01:30+08:00",
               "session_date": day, "market": market, "initial_capital_twd": 10_000_000.0,
               "cumulative_realized_net_pnl_twd": 50.0, "open_position_count": 1,
               "total_equity_twd": 10_000_000.0}
    closing = {**opening, "minute": f"{day}T13:30+08:00",
               "recorded_at": f"{day}T13:30:00+08:00", "open_position_count": 0,
               "cumulative_realized_net_pnl_twd": 1190.0, "total_equity_twd": 10_001_190.0}

    class Store:
        def prices(self, symbol, session_date):
            assert session_date == day
            if symbol == "2317":
                # A late first entry cannot require a nonexistent 09:01 bar.
                return {f"{day}T09:09+08:00": 201.0}
            return {f"{day}T09:01+08:00": 102.0, f"{day}T09:03+08:00": 103.0}

    return [opening, closing], {day: {market: [first, late]}}, Store(), fills


def test_flat_minutes_replay_late_and_partial_entries_without_future_basis(clocked_flat_book):
    from copy import deepcopy
    endpoints, positions, store, fills = clocked_flat_book
    original = deepcopy((endpoints, positions, fills))
    # A prior-session fill outside the selected range must not reset the
    # cumulative realized baseline or demand that old position's archive.
    earlier = {**fills[0], "session_date": "2026-08-12", "position_id": "older"}
    rows, stats = rebuild_strategy_marks(endpoints, positions, store,
        start=date(2026, 8, 13), end=date(2026, 8, 13), fill_rows=[earlier, *fills],
        revalue_opening_marks=True)
    assert len(rows) == 270 and stats["generated_rows"] == 270
    by_clock = {row["minute"][11:16]: row for row in rows}
    first = positions["2026-08-13"]["tw_day_trade"][0]
    initial_net = position_net_liquidation_pnl({**first, "inventory_basis_price": 100.0},
        102.0, signed_shares=1000, remaining_entry_fee_twd=10.0)
    completed_net = position_net_liquidation_pnl({**first, "inventory_basis_price": 101.0},
        103.0, signed_shares=2000, remaining_entry_fee_twd=30.0)
    assert by_clock["09:01"]["open_net_liquidation_pnl_twd"] == pytest.approx(initial_net)
    assert by_clock["09:02"]["open_net_liquidation_pnl_twd"] == pytest.approx(initial_net)
    assert by_clock["09:03"]["open_net_liquidation_pnl_twd"] == pytest.approx(completed_net)
    assert by_clock["09:08"]["open_position_count"] == 1
    assert by_clock["09:09"]["open_position_count"] == 2
    assert by_clock["12:30"]["cumulative_realized_net_pnl_twd"] == 550.0
    assert by_clock["12:30"]["open_net_liquidation_pnl_twd"] == pytest.approx(
        position_net_liquidation_pnl({**first, "inventory_basis_price": 101.0},
            103.0, signed_shares=1000, remaining_entry_fee_twd=15.0)
        + position_net_liquidation_pnl(positions["2026-08-13"]["tw_day_trade"][1],
            201.0, signed_shares=1000, remaining_entry_fee_twd=5.0))
    assert rows[-1] == endpoints[-1]
    assert (endpoints, positions, fills) == original


def test_flat_opening_counts_fill_with_zero_fractional_seconds(clocked_flat_book):
    endpoints, positions, store, fills = clocked_flat_book
    endpoints[0]["recorded_at"] = "2026-08-13T09:01:00+08:00"
    fills[0]["fill_at"] = "2026-08-13T09:01:00.000000+08:00"
    fills[4]["fill_at"] = "2026-08-13T13:30:00.000000+08:00"
    fills[5]["fill_at"] = "2026-08-13T13:30:00.000000+08:00"
    rows, stats = rebuild_strategy_marks(
        endpoints, positions, store, fill_rows=fills,
        start=date(2026, 8, 13), end=date(2026, 8, 13),
        revalue_opening_marks=True,
    )
    assert len(rows) == 270 and stats["generated_rows"] == 270
    assert rows[0]["open_position_count"] == 1
    assert rows[-1] == endpoints[-1]


@pytest.mark.parametrize("endpoint,field,value,message", [
    (0, "open_position_count", 2, "position count mismatch"),
    (1, "open_position_count", 1, "closing fill-book accounting disagrees"),
    (1, "cumulative_realized_net_pnl_twd", 0.0, "closing fill-book accounting disagrees"),
])
def test_flat_fill_book_keeps_strict_endpoint_gates(clocked_flat_book, endpoint, field, value, message):
    endpoints, positions, store, fills = clocked_flat_book
    endpoints[endpoint][field] = value
    with pytest.raises(RuntimeError, match=message):
        rebuild_strategy_marks(endpoints, positions, store, fill_rows=fills,
            start=date(2026, 8, 13), end=date(2026, 8, 13), revalue_opening_marks=True)


def test_flat_opening_does_not_count_early_realized_fill_twice(clocked_flat_book):
    endpoints, positions, store, fills = clocked_flat_book
    fills.insert(1, {**fills[3], "fill_at": "2026-08-13T09:01:20+08:00",
                     "quantity": 500, "entry_fee_allocated_twd": 5.0, "net_pnl_twd": 40.0})
    fills[4] = {**fills[4], "quantity": 500, "entry_fee_allocated_twd": 10.0}
    # The accepted opening already contains this 40; prior realized is 10.
    # Later immutable exits total 1140, so the accepted close stays 1190.
    rows, _ = rebuild_strategy_marks(endpoints, positions, store, fill_rows=fills,
        start=date(2026, 8, 13), end=date(2026, 8, 13), revalue_opening_marks=True)
    assert rows[0]["cumulative_realized_net_pnl_twd"] == 50.0
    assert rows[1]["cumulative_realized_net_pnl_twd"] == 50.0
    assert rows[-1] == endpoints[-1]


def test_position_archive_cannot_resurrect_an_old_replay_without_entry_fill(tmp_path: Path) -> None:
    from scripts.rebuild_tw_day_trade_minute_curves import load_positions
    archive = tmp_path / "position_history/2026-08-13/tw_day_trade.json"
    archive.parent.mkdir(parents=True)
    archive.write_text(json.dumps({"positions": [_position()]}))
    (tmp_path / "state.json").write_text('{"modes":{}}')
    result = load_positions(tmp_path, start=date(2026, 8, 13), end=date(2026, 8, 13), fill_rows=[])
    assert result["2026-08-13"]["tw_day_trade"] == []
    assert archive.is_file()


def test_position_archive_reconciles_disclosed_entry_completion(tmp_path: Path) -> None:
    from scripts.rebuild_tw_day_trade_minute_curves import load_positions

    archive = tmp_path / "position_history/2026-08-13/tw_day_trade.json"
    archive.parent.mkdir(parents=True)
    position = {**_position(), "filled_shares": 35_000}
    archive.write_text(json.dumps({"positions": [position]}), encoding="utf-8")
    (tmp_path / "state.json").write_text('{"modes":{}}', encoding="utf-8")
    identity = {
        "session_date": "2026-08-13",
        "market": "tw_day_trade",
        "position_id": position["position_id"],
        "price": 101.0,
    }
    fills = [
        {**identity, "purpose": "entry", "quantity": 18_000},
        {**identity, "purpose": "entry_completion", "quantity": 17_000},
    ]
    loaded = load_positions(
        tmp_path, start=date(2026, 8, 13), end=date(2026, 8, 13),
        fill_rows=fills,
    )
    assert loaded["2026-08-13"]["tw_day_trade"] == [position]

    with pytest.raises(RuntimeError, match="position archive disagrees"):
        load_positions(
            tmp_path, start=date(2026, 8, 13), end=date(2026, 8, 13),
            fill_rows=fills[:1],
        )


def test_historical_benchmark_close_is_not_overwritten_by_old_live_quote(tmp_path: Path) -> None:
    from stockagent.live.tw_day_trade_dashboard import build_dashboard_history_snapshot
    first = {"benchmark_id": "benchmark_2330", "session_date": "2026-09-04",
             "minute": "2026-09-04T09:00+08:00", "initial_capital_twd": 100.0,
             "total_equity_twd": 110.0, "source": "official_daily_session_open"}
    last = {**first, "minute": "2026-09-04T13:30+08:00", "total_equity_twd": 121.0,
            "source": "official_daily_session_close"}
    (tmp_path / "benchmark_history.json").write_text(json.dumps({"schema_version": 1, "marks": [first, last]}))
    (tmp_path / "benchmark_marks.jsonl").write_text(json.dumps({**first, "total_equity_twd": 121.0}) + "\n")
    for dates in ({}, {"start_date": "2026-09-04", "end_date": "2026-09-04"}):
        history = build_dashboard_history_snapshot(state_dir=tmp_path, range_key="all", **dates)
        assert [row["return_pct"] for row in history["history"]] == pytest.approx([0.0, 10.0])


def test_strategy_minute_rebuild_uses_exit_ledger_for_missing_mark(
    tmp_path: Path,
) -> None:
    kbar_root = tmp_path / "kbars"
    _minute_file(
        kbar_root,
        "2330",
        [(datetime(2026, 8, 13, 9, 2), 102.0)],
    )
    store = MinutePriceStore(kbar_root, tmp_path / "ticks")
    opening = {
        "minute": "2026-08-13T09:01+08:00",
        "recorded_at": "2026-08-13T09:01:00+08:00",
        "session_date": "2026-08-13",
        "market": "tw_day_trade",
        "initial_capital_twd": 10_000_000.0,
        "cumulative_realized_net_pnl_twd": 50.0,
        "open_net_liquidation_pnl_twd": -100.0,
        "total_equity_twd": 9_999_950.0,
        "open_position_count": 1,
        "stale_position_count": 0,
        "valuation_stale": False,
    }
    closing = {
        **opening,
        "minute": "2026-08-13T13:30+08:00",
        "recorded_at": "2026-08-13T13:30:00+08:00",
        "cumulative_realized_net_pnl_twd": 450.0,
        "open_net_liquidation_pnl_twd": 0.0,
        "total_equity_twd": 10_000_450.0,
        "open_position_count": 0,
    }
    exit_fill = {
        "session_date": "2026-08-13",
        "market": "tw_day_trade",
        "position_id": "tw_day_trade:2026-08-13:2330",
        "purpose": "stop_loss",
        "fill_at": "2026-08-13T09:02:00+08:00",
        "quantity": 1_000,
        "entry_fee_allocated_twd": 30.0,
        "net_pnl_twd": 400.0,
    }

    rows, stats = rebuild_strategy_marks(
        [opening, closing],
        {"2026-08-13": {"tw_day_trade": [_position()]}},
        store,
        start=date(2026, 8, 13),
        end=date(2026, 8, 13),
        fill_rows=[exit_fill],
    )

    minute_0902 = rows[1]
    assert minute_0902["cumulative_realized_net_pnl_twd"] == 450.0
    assert minute_0902["open_net_liquidation_pnl_twd"] == 0.0
    assert minute_0902["open_position_count"] == 0
    assert minute_0902["total_equity_twd"] == 10_000_450.0
    assert rows[-1] == closing
    assert stats["inserted_missing_rows"] == 268
    assert stats["preserved_observed_rows"] == 2


def test_strategy_minute_rebuild_keeps_latest_observed_duplicate(
    tmp_path: Path,
) -> None:
    kbar_root = tmp_path / "kbars"
    _minute_file(
        kbar_root,
        "2330",
        [(datetime(2026, 8, 13, 9, 2), 102.0)],
    )
    store = MinutePriceStore(kbar_root, tmp_path / "ticks")
    base = {
        "session_date": "2026-08-13",
        "market": "tw_day_trade",
        "initial_capital_twd": 10_000_000.0,
        "cumulative_realized_net_pnl_twd": 0.0,
        "open_position_count": 1,
    }
    opening = {
        **base,
        "minute": "2026-08-13T09:01+08:00",
        "recorded_at": "2026-08-13T09:01:00+08:00",
    }
    older = {
        **base,
        "minute": "2026-08-13T09:02+08:00",
        "recorded_at": "2026-08-13T09:02:01+08:00",
        "total_equity_twd": 9_999_000.0,
    }
    latest = {
        **older,
        "recorded_at": "2026-08-13T09:02:59+08:00",
        "total_equity_twd": 10_001_000.0,
    }
    closing = {
        **base,
        "minute": "2026-08-13T13:30+08:00",
        "recorded_at": "2026-08-13T13:30:00+08:00",
        "open_position_count": 0,
    }

    rows, stats = rebuild_strategy_marks(
        [opening, older, latest, closing],
        {"2026-08-13": {"tw_day_trade": [_position()]}},
        store,
        start=date(2026, 8, 13),
        end=date(2026, 8, 13),
    )

    assert len(rows) == 270
    assert rows[1] == latest
    assert stats["duplicate_rows_removed"] == 1


def test_strategy_minute_repair_replaces_only_unverified_interior_marks(
    tmp_path: Path,
) -> None:
    kbar_root = tmp_path / "kbars"
    _minute_file(
        kbar_root,
        "2330",
        [
            (datetime(2026, 8, 13, 9, 2), 102.0),
            (datetime(2026, 8, 13, 9, 3), 103.0),
        ],
    )
    store = MinutePriceStore(kbar_root, tmp_path / "ticks")
    base = {
        "session_date": "2026-08-13",
        "market": "tw_day_trade",
        "initial_capital_twd": 10_000_000.0,
        "cumulative_realized_net_pnl_twd": 0.0,
        "open_position_count": 1,
    }
    opening = {
        **base,
        "minute": "2026-08-13T09:01+08:00",
        "recorded_at": "2026-08-13T09:01:00+08:00",
    }
    unverified = {
        **base,
        "minute": "2026-08-13T09:02+08:00",
        "recorded_at": "2026-08-13T09:02:00+08:00",
        "open_position_count": 0,
        "total_equity_twd": 1.0,
    }
    audited = {
        **base,
        "minute": "2026-08-13T09:03+08:00",
        "recorded_at": "2026-08-13T09:03:00+08:00",
        "total_equity_twd": 10_000_123.0,
        "historical_minute_replay": True,
        "minute_valuation_contract": "right_labelled_historical_last_trade_mark_v1",
        "valuation_source": "shioaji_historical_1m_close_with_last_trade_carry",
        "fresh_trade_notional_coverage_ratio": 1.0,
        "fresh_trade_position_count": 1,
        "last_trade_carried_position_count": 0,
        "missing_price_position_count": 0,
        "valuation_executable": False,
    }
    closing = {
        **base,
        "minute": "2026-08-13T13:30+08:00",
        "recorded_at": "2026-08-13T13:30:00+08:00",
        "open_position_count": 0,
    }

    rows, stats = rebuild_strategy_marks(
        [opening, unverified, audited, closing],
        {"2026-08-13": {"tw_day_trade": [_position()]}},
        store,
        start=date(2026, 8, 13),
        end=date(2026, 8, 13),
        repair_unverified_existing=True,
    )

    assert rows[0] == opening
    assert rows[1]["open_position_count"] == 1
    assert rows[1]["total_equity_twd"] != 1.0
    assert rows[1]["historical_minute_replay"] is True
    assert rows[2] == audited
    assert rows[-1] == closing
    assert stats["replaced_unverified_rows"] == 1
    assert stats["inserted_missing_rows"] == 266


def test_price_provenance_inspection_rejects_timestamp_only_minute(
    tmp_path: Path,
) -> None:
    base = datetime(2026, 8, 13, 9, 1, tzinfo=TAIPEI)
    rows = []
    for offset in range(270):
        row = {
            "session_date": "2026-08-13",
            "market": "tw_day_trade",
            "minute": (base + timedelta(minutes=offset)).isoformat(
                timespec="minutes"
            ),
            "historical_minute_replay": True,
            "minute_valuation_contract": "right_labelled_historical_last_trade_mark_v1",
            "valuation_source": "shioaji_historical_1m_close_with_last_trade_carry",
            "fresh_trade_notional_coverage_ratio": 1.0,
            "fresh_trade_position_count": 1,
            "last_trade_carried_position_count": 0,
            "missing_price_position_count": 0,
            "valuation_executable": False,
        }
        rows.append(row)
    rows[10].pop("valuation_source")
    (tmp_path / "marks.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )

    result = _inspect_strategy_price_provenance(
        tmp_path,
        completed_session_dates=["2026-08-13"],
        expected_markets={"tw_day_trade"},
    )

    assert result["expected_interior_rows"] == 268
    assert result["audited_interior_rows"] == 267
    assert result["unverified_interior_rows"] == 1
    assert result["expected_opening_rows"] == 1
    assert result["audited_opening_rows"] == 1
    assert result["unverified_opening_rows"] == 0


def test_price_provenance_inspection_rejects_unsourced_opening(tmp_path: Path) -> None:
    opening = {
        "session_date": "2026-08-13",
        "market": "tw_day_trade",
        "minute": "2026-08-13T09:01+08:00",
        "accepted_endpoint_accounting_verified": True,
    }
    (tmp_path / "marks.jsonl").write_text(json.dumps(opening) + "\n", encoding="utf-8")
    result = _inspect_strategy_price_provenance(
        tmp_path,
        completed_session_dates=["2026-08-13"],
        expected_markets={"tw_day_trade"},
    )
    assert result["unverified_opening_rows"] == 1
    assert result["audited_opening_rows"] == 0


def test_price_provenance_inspection_uses_latest_append_only_mark(tmp_path: Path) -> None:
    opening = {
        "session_date": "2026-08-13",
        "market": "tw_day_trade",
        "minute": "2026-08-13T09:01+08:00",
    }
    sourced = {
        **opening,
        "historical_minute_replay": True,
        "minute_valuation_contract": "right_labelled_historical_last_trade_mark_v1",
        "valuation_source": "retained_1m_close_from_immutable_carried_fill_and_action_book",
        "fresh_trade_notional_coverage_ratio": 1.0,
        "fresh_trade_position_count": 1,
        "last_trade_carried_position_count": 0,
        "missing_price_position_count": 0,
        "valuation_executable": False,
    }
    marks_path = tmp_path / "marks.jsonl"
    marks_path.write_text(
        "".join(json.dumps(row) + "\n" for row in (opening, sourced)),
        encoding="utf-8",
    )
    kwargs = {
        "completed_session_dates": ["2026-08-13"],
        "expected_markets": {"tw_day_trade"},
    }
    assert _inspect_strategy_price_provenance(tmp_path, **kwargs)["unverified_opening_rows"] == 0
    marks_path.write_text(
        "".join(json.dumps(row) + "\n" for row in (sourced, opening)),
        encoding="utf-8",
    )
    assert _inspect_strategy_price_provenance(tmp_path, **kwargs)["unverified_opening_rows"] == 1


def test_price_provenance_reuse_requires_hash_checked_opening_coverage() -> None:
    scope = {"completed_session_dates": ["2026-08-13"], "expected_markets": {"a"}}
    validated = {
        "required": True,
        "contract": "right_labelled_historical_last_trade_mark_v1",
        "opening_marks_revalued_at_completed_minute": True,
        "completed_session_dates": ["2026-08-13"],
        "points_per_session_mode": 270,
        "validated_rows": 270,
        "unverified_historical_interior_rows": 0,
    }
    reused = _proven_price_state_from_curve_validation(validated, **scope)
    assert reused is not None
    assert reused["audited_opening_rows"] == 1
    assert reused["audited_interior_rows"] == 268
    for patch in (
        {"opening_marks_revalued_at_completed_minute": False},
        {"unverified_historical_interior_rows": 1},
        {"validated_rows": 269},
        {"completed_session_dates": ["2026-08-14"]},
    ):
        assert _proven_price_state_from_curve_validation(
            {**validated, **patch}, **scope
        ) is None


@pytest.mark.parametrize("bad_points", [None, [1], [["a", 1]], [["b", "2026-08-13T09:01+08:00"]],
                                          [["a", "2026-08-13T09:02+08:00"]]])
def test_cost_acceptance_never_becomes_complete_market_price_proof(bad_points):
    scope = {"completed_session_dates": ["2026-08-13"], "expected_markets": {"a"}}
    proof = {"required": True, "contract": "right_labelled_historical_last_trade_mark_v1",
        "opening_marks_revalued_at_completed_minute": False,
        "completed_session_dates": ["2026-08-13"], "points_per_session_mode": 270,
        "validated_rows": 270, "unverified_historical_interior_rows": 0,
        "provisional_opening_cost_valuation": {
            "contract": "user_authorized_missing_0901_entry_cost_valuation_v1",
            "market_observation_verified": False, "financial_journals_changed": False,
            "verified_minute_points": 1, "points": [["a", "2026-08-13T09:01+08:00"]]}}
    if bad_points is not None:
        proof["provisional_opening_cost_valuation"]["points"] = bad_points
        assert _proven_price_state_from_curve_validation(proof, **scope) is None
    else:
        result = _proven_price_state_from_curve_validation(proof, **scope)
        assert result["audited_opening_rows"] == 0 and result["unverified_opening_rows"] == 1
        assert result["authorized_provisional_opening_rows"] == 1
        assert result["market_price_completeness_verified"] is False


def test_stock_benchmark_minute_rebuild_preserves_tx_marks(tmp_path: Path) -> None:
    kbar_root = tmp_path / "kbars"
    for symbol, price in (("0050", 101.0), ("2330", 2_401.0)):
        _minute_file(
            kbar_root,
            symbol,
            [(datetime(2026, 8, 13, 9, 1), price)],
        )
    store = MinutePriceStore(kbar_root, tmp_path / "ticks")

    def endpoint(
        benchmark_id: str, symbol: str, price: float, clock: str
    ) -> dict[str, object]:
        initial = price * 1_000
        return {
            "benchmark_id": benchmark_id,
            "symbol": symbol,
            "session_date": "2026-08-13",
            "minute": f"2026-08-13T{clock}+08:00",
            "recorded_at": f"2026-08-13T{clock}:00+08:00",
            "entry_price": price,
            "last_mark_price": price,
            "adjusted_quantity": 1_000.0,
            "quantity": 1_000,
            "initial_capital_twd": initial,
            "initial_fixed_fees_twd": initial * 0.000285,
            "liquidation_cost_twd": initial * 0.001285,
            "total_equity_twd": initial * (1.0 - 0.00157),
            "return_pct": -0.157,
            "valuation_stale": False,
        }

    stock_marks = []
    for benchmark_id, symbol, price in (
        ("benchmark_0050", "0050", 100.0),
        ("benchmark_2330", "2330", 2_400.0),
    ):
        stock_marks.extend(
            [
                endpoint(benchmark_id, symbol, price, "09:00"),
                endpoint(benchmark_id, symbol, price + 1.0, "13:30"),
            ]
        )
    tx = {
        "benchmark_id": "benchmark_tx_continuous",
        "session_date": "2026-08-13",
        "minute": "2026-08-13T08:45+08:00",
        "total_equity_twd": 1.0,
    }
    rebuilt, stats = rebuild_benchmark_history(
        {"marks": [tx, *stock_marks], "counts": {}},
        store,
        start=date(2026, 8, 13),
        end=date(2026, 8, 13),
    )
    assert len(rebuilt["marks"]) == 543
    assert tx in rebuilt["marks"]
    assert rebuilt["counts"] == {
        "marks": 543,
        "stock_marks": 542,
        "tx_minute_marks": 1,
    }
    assert stats["generated_stock_rows"] == 542


def test_history_rows_keep_minute_data_quality_fields(tmp_path: Path) -> None:
    from stockagent.live.tw_day_trade_dashboard import build_dashboard_history_snapshot

    row = {
        "minute": "2026-08-13T09:01+08:00",
        "recorded_at": "2026-08-13T09:01:00+08:00",
        "session_date": "2026-08-13",
        "market": "tw_day_trade",
        "initial_capital_twd": 10_000_000.0,
        "total_equity_twd": 10_001_000.0,
        "valuation_stale": True,
        "historical_minute_replay": True,
        "minute_valuation_contract": "right_labelled_historical_last_trade_mark_v1",
        "fresh_trade_position_count": 2,
        "last_trade_carried_position_count": 1,
        "missing_price_position_count": 0,
        "fresh_trade_notional_coverage_ratio": 0.75,
        "valuation_executable": False,
    }
    (tmp_path / "marks.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
    (tmp_path / "benchmark_history.json").write_text(
        json.dumps({"marks": [], "origins": {}}), encoding="utf-8"
    )
    payload = build_dashboard_history_snapshot(state_dir=tmp_path, range_key="all")
    history = payload["history"][0]
    assert history["historical_minute_replay"] is True
    assert history["fresh_trade_notional_coverage_ratio"] == 0.75
    assert history["last_trade_carried_position_count"] == 1
    assert history["valuation_executable"] is False


def test_fast_session_calendar_preserves_cash_days_without_reading_signal_prefix(tmp_path, monkeypatch):
    from stockagent.live import tw_day_trade_dashboard as dashboard
    def forbidden(*_args, **_kwargs):
        raise AssertionError("status dates must not parse or hash the signal ledger")
    monkeypatch.setattr(dashboard, "_ledger_session_index", forbidden)
    (tmp_path / "signals.jsonl").write_text("do not read this legacy stream\n")
    archive = tmp_path / "position_history/2026-11-06/mode.json"
    archive.parent.mkdir(parents=True)
    archive.write_text('{"positions":[]}')
    receipt = {"simulation_only": True, "production_order_possible": False,
        "sessions": [{"session_date": day} for day in ("2026-11-02", "2026-11-03", "2026-11-05")]}
    path = tmp_path / "rebuild_receipt.json"
    path.write_text(json.dumps(receipt))
    state = {"modes": {"mode": {"session_date": "2026-11-10"}}}
    def dates():
        return dashboard._available_session_dates(root=tmp_path, state=state,
            observed=datetime.fromisoformat("2026-11-10T09:00:00+08:00"),
            include_ledger_dates=False, include_benchmark_history_dates=False)
    assert dates() == ["2026-11-10", "2026-11-06", "2026-11-05", "2026-11-03", "2026-11-02"]
    receipt["sessions"].append({"session_date": "2026-11-09"})
    path.write_text(json.dumps(receipt))
    assert dates() == ["2026-11-10", "2026-11-09", "2026-11-06", "2026-11-05", "2026-11-03", "2026-11-02"]
