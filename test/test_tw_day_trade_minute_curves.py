from __future__ import annotations

from datetime import date, datetime
import json
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl
import pytest

from scripts.rebuild_tw_day_trade_minute_curves import (
    MinutePriceStore,
    _ticks_to_minute_frame,
    fetch_missing_kbars,
    rebuild_benchmark_history,
    rebuild_strategy_marks,
)
from stockagent.live.tw_day_trade_simulation import position_net_liquidation_pnl


TAIPEI = ZoneInfo("Asia/Taipei")


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
        max_traffic_fraction=0.75,
        traffic_reserve_mb=256.0,
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
        max_traffic_fraction=0.75,
        traffic_reserve_mb=256.0,
    )

    assert result["api_process_started"] is True
    assert result["requested_symbols"] == 1
    assert result["missing_before"] == 1
    assert result["missing_after"] == 0
    assert result["api_requests_started"] == 1


def test_strategy_minute_rebuild_preserves_endpoints_and_discloses_carry(
    tmp_path: Path,
) -> None:
    kbar_root = tmp_path / "kbars"
    _minute_file(
        kbar_root,
        "2330",
        [(datetime(2026, 8, 13, 9, 1), 102.0)],
    )
    store = MinutePriceStore(kbar_root, tmp_path / "ticks")
    opening = {
        "minute": "2026-08-13T09:00+08:00",
        "recorded_at": "2026-08-13T09:00:00+08:00",
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
    assert len(rows) == 271
    assert rows[0] == opening
    assert rows[-1] == closing
    minute_0901 = rows[1]
    assert minute_0901["open_net_liquidation_pnl_twd"] == pytest.approx(
        position_net_liquidation_pnl(
            _position(),
            102.0,
            signed_shares=1_000,
            remaining_entry_fee_twd=30.0,
        )
    )
    assert minute_0901["fresh_trade_notional_coverage_ratio"] == 1.0
    assert rows[2]["last_trade_carried_position_count"] == 1
    assert rows[2]["valuation_stale"] is True
    assert stats["generated_rows"] == 271


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
