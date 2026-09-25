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
    _ticks_to_minute_frame,
    fetch_missing_kbars,
    missing_accepted_endpoints,
    rebuild_benchmark_history,
    rebuild_strategy_marks,
    required_symbol_dates,
    validate_existing_strategy_marks,
    verified_manual_no_trade_pairs,
)


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
    monkeypatch.setattr(maintenance, "_stock_calendar_source_state", lambda *a: {"ready": True})
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
