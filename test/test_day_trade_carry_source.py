from types import SimpleNamespace
from datetime import date
import hashlib
import json

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import torch

from stockagent.data.tw_day_trade_carry_source import (
    build_prepared_day_trade_carry_source,
)


def _write_parquet(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(payload).write_parquet(path)


def _write_action_receipts(public, rows):
    reference = public / "tw_corporate_action_reference.parquet"
    reference.with_suffix(".summary.json").write_text("{}", encoding="utf-8")
    entitlement = public / "tw_corporate_action_entitlements.parquet"
    _write_parquet(entitlement, {
        "date": [row["date"] for row in rows],
        "symbol": [row["symbol"] for row in rows],
        "handling": [row.get("handling", "avoid") for row in rows],
        "cash_dividend_per_share": [row.get("cash") for row in rows],
        "cash_payment_date": [row.get("payment") for row in rows],
        "stop_transfer_start": [None for _row in rows],
    })
    raw = b'{"receipt":"test"}\n'
    raw_sha = hashlib.sha256(raw).hexdigest()
    raw_path = (
        public / "raw/tw_corporate_action_entitlements/manifests"
        / f"{raw_sha}.jsonl"
    )
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_bytes(raw)
    summary = {
        "schema_version": 4,
        "baseline_established": True,
        "coverage_complete": True,
        "failure_count": 0,
        "coverage_start": "2014-01-01",
        "coverage_end": "2030-12-31",
        "rows": len(rows),
        "reference_rows": len(rows),
        "raw_receipt_manifest": {
            "relative_path": str(raw_path.relative_to(public)),
            "sha256": raw_sha,
            "size": len(raw),
            "entries": 1,
        },
        "reference_receipt": {
            "size": reference.stat().st_size,
            "sha256": hashlib.sha256(reference.read_bytes()).hexdigest(),
        },
        "output_receipt": {
            "size": entitlement.stat().st_size,
            "sha256": hashlib.sha256(entitlement.read_bytes()).hexdigest(),
        },
    }
    entitlement.with_suffix(".summary.json").write_text(
        json.dumps(summary), encoding="utf-8"
    )


def test_physical_source_builds_once_and_loads_lazy_symbol_day_sessions(tmp_path):
    public = tmp_path / "public-release"
    _write_parquet(public / "features/tw_public_stock_daily.parquet", {"x": [1]})
    _write_parquet(
        public / "twse_daily_ohlcv.parquet",
        {
            "date": ["2019-01-02"], "證券代號": ["2330"],
            "開盤價": ["100"], "收盤價": ["100"],
        },
    )
    _write_parquet(
        public / "tpex_daily_ohlcv.parquet",
        {
            "date": ["2018-12-28", "2020-02-27"],
            "代號": ["6488", "6488"],
            "次日漲停價": ["55.0", "55.0"],
            "次日跌停價": ["45.0", "45.0"],
            "開盤": ["50", "50"], "收盤": ["50", "50"],
        },
    )
    _write_parquet(
        public / "tw_corporate_action_reference.parquet",
        {"date": [date(2019, 1, 2)], "symbol": ["2330"],
         "reference_price": [100.0]},
    )
    _write_action_receipts(
        public,
        [{"date": date(2019, 1, 2), "symbol": "2330"}],
    )
    minute = tmp_path / "minute-release"
    minute.mkdir()
    (minute / "manifest.json").write_text(
        '{"source":"shioaji_kbars_1m","research_ready":true,'
        '"status":"research_ready","partitions":["2020-03-02"]}',
        encoding="utf-8",
    )
    rows = []
    for symbol, price in (("2330", 100.0), ("6488", 50.0), ("0050", 120.0)):
        for minute_index in (1, 270):
            rows.append(
                {
                    "symbol": symbol, "minutes_from_open": minute_index,
                    "Open": price, "High": price, "Low": price,
                    "Close": price, "Amount": price * 2000.0,
                    "volume_shares": 2000.0,
                }
            )
    partition = minute / "trade_date=2020-03-02/data.parquet"
    partition.parent.mkdir()
    pq.write_table(pa.Table.from_pylist(rows), partition)
    non_session = minute / "trade_date=2020-03-01/data.parquet"
    non_session.parent.mkdir()
    pq.write_table(pa.Table.from_pylist(rows[:1]), non_session)
    (minute / "manifest.json").write_text(json.dumps({
        "source": "shioaji_kbars_1m", "research_ready": True,
        "status": "research_ready", "partitions": [{
            "status": "ok", "trade_date": "2020-03-02",
            "output": "trade_date=2020-03-02/data.parquet",
            "output_sha256": hashlib.sha256(partition.read_bytes()).hexdigest(),
        }, {
            "status": "ok", "trade_date": "2020-03-01",
            "output": "trade_date=2020-03-01/data.parquet",
            "output_sha256": hashlib.sha256(non_session.read_bytes()).hexdigest(),
            "rows": 1, "symbols": 1,
        }],
    }), encoding="utf-8")
    panel = SimpleNamespace(
        dates=np.asarray(["2019-01-02", "2020-03-02"], dtype="datetime64[D]"),
        symbols=["2330", "6488", "0050"],
        open_prices=np.asarray([[100.0, 50.0, 120.0], [100.0, 50.0, 120.0]]),
        close_prices=np.asarray([[100.0, 50.0, 120.0], [100.0, 50.0, 120.0]]),
        daily_volumes=np.asarray(
            [[542000.0, 542000.0, 542000.0], [5000.0, 5000.0, 5000.0]]
        ),
        corporate_action_avoidance_mask=np.zeros((2, 3), dtype=bool),
        unresolved_corporate_action_mask=np.zeros((2, 3), dtype=bool),
        force_exit_mask=np.zeros((2, 3), dtype=bool),
    )
    source = build_prepared_day_trade_carry_source(
        panel=panel, minute_root=minute,
        public_feature_path=public / "features/tw_public_stock_daily.parquet",
        cache_dir=tmp_path / "cache", allow_daily_proxy=True,
        daily_proxy_price_policy="official_open_close",
        corporate_action_mode="avoid",
    )
    assert not source.sessions and len(source) == 2
    first, second = source.rows([0, 1], [0, 1, 2], torch.device("cpu"))
    assert first.entry_price.tolist() == [100.0, 50.0, 120.0]
    assert first.exit_prices[:, -1, 0].tolist() == [100.0, 50.0, 120.0]
    assert second.entry_volume.tolist() == [2000.0, 2000.0, 5000.0 / 271.0]
    assert second.source_gap_mask.tolist() == [0.0, 0.0, 0.0]
    assert second.daily_proxy_mask.tolist() == [0.0, 0.0, 1.0]
    assert source.audit_receipt["minute_partition_scope"] == {
        "selected_panel_partitions": 1,
        "quarantined_non_panel_session_partitions": [{
            "trade_date": "2020-03-01",
            "output": "trade_date=2020-03-01/data.parquet",
            "output_sha256": hashlib.sha256(non_session.read_bytes()).hexdigest(),
            "rows": 1,
            "symbols": 1,
        }],
        "unselected_outside_panel_horizon_partitions": [],
    }
    # A second construction must validate/reuse the completed cache.
    again = build_prepared_day_trade_carry_source(
        panel=panel, minute_root=minute,
        public_feature_path=public / "features/tw_public_stock_daily.parquet",
        cache_dir=tmp_path / "cache", allow_daily_proxy=True,
        daily_proxy_price_policy="official_open_close",
        corporate_action_mode="avoid",
    )
    assert again.release_id == source.release_id


def test_physical_source_uses_daily_proxy_when_minute_volume_exceeds_day_bound(tmp_path):
    public = tmp_path / "public-release"
    _write_parquet(public / "features/tw_public_stock_daily.parquet", {"x": [1]})
    _write_parquet(
        public / "twse_daily_ohlcv.parquet",
        {
            "date": ["2020-02-27"], "證券代號": ["2330"],
            "開盤價": ["100"], "收盤價": ["100"],
        },
    )
    _write_parquet(
        public / "tpex_daily_ohlcv.parquet",
        {"date": ["2020-02-27"], "代號": ["6488"],
         "次日漲停價": ["55"], "次日跌停價": ["45"],
         "開盤": ["50"], "收盤": ["50"]},
    )
    _write_parquet(
        public / "tw_corporate_action_reference.parquet",
        {"date": [date(2020, 3, 2)], "symbol": ["2330"],
         "reference_price": [100.0]},
    )
    _write_action_receipts(
        public,
        [{"date": date(2020, 3, 2), "symbol": "2330"}],
    )
    minute = tmp_path / "minute-release"
    minute.mkdir()
    (minute / "manifest.json").write_text(
        '{"source":"shioaji_kbars_1m","research_ready":true,'
        '"status":"research_ready","partitions":["2020-03-02"]}',
        encoding="utf-8",
    )
    partition = minute / "trade_date=2020-03-02/data.parquet"
    partition.parent.mkdir()
    minute_rows = []
    for symbol, price in (("2330", 100.0), ("6488", 50.0)):
        for minute_index in (1, 270):
            minute_rows.append({
                "symbol": symbol, "minutes_from_open": minute_index,
                "Open": price, "High": price, "Low": price, "Close": price,
                "Amount": price * 1000.0, "volume_shares": 1000.0,
            })
    pq.write_table(pa.Table.from_pylist(minute_rows), partition)
    (minute / "manifest.json").write_text(json.dumps({
        "source": "shioaji_kbars_1m", "research_ready": True,
        "status": "research_ready", "partitions": [{
            "status": "ok", "trade_date": "2020-03-02",
            "output": "trade_date=2020-03-02/data.parquet",
            "output_sha256": hashlib.sha256(partition.read_bytes()).hexdigest(),
        }],
    }), encoding="utf-8")
    panel = SimpleNamespace(
        dates=np.asarray(["2020-03-02"], dtype="datetime64[D]"),
        symbols=["2330", "6488", "00836B"],
        open_prices=np.asarray([[100.0, 50.0, 35.77]], dtype=np.float32),
        close_prices=np.asarray([[100.0, 50.0, 35.59]], dtype=np.float32),
        daily_volumes=np.asarray([[2000.0, 1000.0, 107000.0]]),
        corporate_action_avoidance_mask=np.zeros((1, 3), dtype=bool),
        unresolved_corporate_action_mask=np.zeros((1, 3), dtype=bool),
        force_exit_mask=np.zeros((1, 3), dtype=bool),
    )
    source = build_prepared_day_trade_carry_source(
        panel=panel, minute_root=minute,
        public_feature_path=public / "features/tw_public_stock_daily.parquet",
        cache_dir=tmp_path / "cache", allow_daily_proxy=True,
        daily_proxy_price_policy="official_open_close", corporate_action_mode="avoid",
    )
    session = source.session_at(0)
    assert session.source_gap_mask.tolist() == [0.0, 0.0, 0.0]
    assert session.daily_proxy_mask.tolist() == [0.0, 1.0, 1.0]
    assert np.isfinite(session.exit_prices.numpy()[0]).any()
    assert session.exit_prices.numpy()[1, -1].tolist() == [50.0, 50.0]
    assert not np.isfinite(session.exit_prices.numpy()[1, :-1]).any()
    assert session.official_open[2].item() == 35.77
    assert session.entry_price[2].item() == 35.77
    assert session.exit_prices[2, -1].tolist() == [35.59, 35.59]
    precision = source.audit_receipt["daily_proxy_source_precision"]
    assert precision["validation"].startswith("preserve_panel_source_dtype")
    assert precision["normalized_open_cells"] >= 1
    assert precision["normalized_close_cells"] >= 1


def test_exact_cash_action_accounts_for_unliquidated_avoid_mode_residual(tmp_path):
    public = tmp_path / "public-release"
    _write_parquet(public / "features/tw_public_stock_daily.parquet", {"x": [1]})
    _write_parquet(
        public / "twse_daily_ohlcv.parquet",
        {
            "date": ["2020-03-19"], "證券代號": ["2330"],
            "開盤價": ["100"], "收盤價": ["100"],
        },
    )
    _write_parquet(
        public / "tpex_daily_ohlcv.parquet",
        {
            "date": ["2020-03-18", "2020-03-20"],
            "代號": ["00836B", "00836B"],
            "次日漲停價": ["42.68", "39.12"],
            "次日跌停價": ["34.84", "32.04"],
            "開盤": ["39.74", "--"], "收盤": ["39.71", "--"],
        },
    )
    _write_parquet(
        public / "tw_corporate_action_reference.parquet",
        {
            "date": [date(2020, 3, 19)], "symbol": ["00836B"],
            "reference_price": [38.71],
        },
    )
    _write_action_receipts(public, [{
        "date": date(2020, 3, 19), "symbol": "00836B",
        "handling": "exact_cash", "cash": 0.997,
        "payment": date(2020, 4, 28),
    }])
    minute = tmp_path / "minute-release"
    partition = minute / "trade_date=2020-03-19/data.parquet"
    partition.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([{
        "symbol": "00836B", "minutes_from_open": minute_index,
        "Open": 35.77, "High": 35.77, "Low": 35.59,
        "Close": 35.59, "Amount": 35.6 * 1000.0,
        "volume_shares": 1000.0,
    } for minute_index in (1, 270)]), partition)
    (minute / "manifest.json").write_text(json.dumps({
        "source": "shioaji_kbars_1m", "research_ready": True,
        "status": "research_ready", "partitions": [{
            "status": "ok", "trade_date": "2020-03-19",
            "output": "trade_date=2020-03-19/data.parquet",
            "output_sha256": hashlib.sha256(partition.read_bytes()).hexdigest(),
        }],
    }), encoding="utf-8")
    panel = SimpleNamespace(
        dates=np.asarray(
            ["2020-03-18", "2020-03-19", "2020-03-20"],
            dtype="datetime64[D]",
        ),
        symbols=["00836B"],
        open_prices=np.asarray([[39.74], [35.77], [np.nan]], dtype=np.float32),
        close_prices=np.asarray([[39.71], [35.59], [np.nan]], dtype=np.float32),
        daily_volumes=np.asarray([[113000.0], [107000.0], [0.0]]),
        corporate_action_avoidance_mask=np.asarray(
            [[True], [False], [False]]
        ),
        unresolved_corporate_action_mask=np.asarray(
            [[False], [False], [False]]
        ),
        force_exit_mask=np.zeros((3, 1), dtype=bool),
    )
    source = build_prepared_day_trade_carry_source(
        panel=panel, minute_root=minute,
        public_feature_path=public / "features/tw_public_stock_daily.parquet",
        cache_dir=tmp_path / "cache", allow_daily_proxy=True,
        daily_proxy_price_policy="official_open_close",
        corporate_action_mode="avoid",
    )
    event = source.session_at(1)
    assert event.source_gap_mask.tolist() == [0.0]
    assert event.action_mask.tolist() == [1.0]
    assert event.share_ratio.tolist() == [1.0]
    assert event.cash_per_old_share.tolist() == [0.997]
    assert event.payment_day.item() == date(2020, 4, 28).toordinal()
    assert source.audit_receipt["corporate_action_policy"]["mapped_events"] == 1
    no_trade = source.session_at(2)
    assert no_trade.halted.tolist() == [1.0]
    assert no_trade.source_gap_mask.tolist() == [0.0]
    assert np.isnan(no_trade.entry_price.item())
    assert no_trade.exit_capacity.sum().item() == 0.0
    assert no_trade.marks[:, 0].tolist() == [35.59]
    assert no_trade.marks[:, -1].tolist() == [35.59]
    assert source.audit_receipt["official_no_regular_execution"] == {
        "twse_symbol_days": 0,
        "tpex_symbol_days": 1,
        "total_symbol_days": 1,
        "valuation": "carry_previous_observable_regular_market_mark",
        "entry_exit_capacity": "zero",
        "absent_official_row": "not_classified_and_never_auto_filled",
    }
