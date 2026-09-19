from types import SimpleNamespace
from datetime import date
from dataclasses import fields, replace
import hashlib
import json

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import torch

from stockagent.backtest.tw_day_trade_carry import (
    compact_day_trade_carry_session,
)
from stockagent.data.tw_day_trade_carry_source import (
    PHYSICAL_SOURCE_RUN_RECEIPT_ENV,
    _exact_inventory_action_arrays,
    _share_replacement_arrays,
    build_prepared_day_trade_carry_source,
    compact_packed_day_trade_carry_session,
)
from stockagent.training.day_trade_carry_bridge import PreparedDayTradeCarryBatch


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
        "handling_reason": [
            row.get("handling_reason", "stock_or_subscription_action")
            for row in rows
        ],
        "reference_price": [row.get("reference_price", 100.0) for row in rows],
        "cash_dividend_per_share": [row.get("cash", 0.0) for row in rows],
        "cash_payment_date": [row.get("payment") for row in rows],
        "stock_dividend_ratio": [row.get("stock_ratio", 0.0) for row in rows],
        "stock_delivery_date": [row.get("stock_delivery") for row in rows],
        "stock_terms_complete": [
            row.get("stock_terms_complete", True) for row in rows
        ],
        "subscription_ratio": [
            row.get("subscription_ratio", 0.0) for row in rows
        ],
        "subscription_price": [
            row.get("subscription_price", 0.0) for row in rows
        ],
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
        "schema_version": 5,
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


def _write_share_replacement_receipts(public, rows=None):
    rows = rows or [{
        "symbol": "9999", "market": "twse",
        "resume_date": date(2029, 1, 2), "suspension_date": None,
        "new_shares_per_1000_old": None,
        "cash_return_per_old_share": None,
        "cash_payment_date": None,
        "cash_dividend_per_old_share": None,
        "subscription_shares_per_1000": None,
        "subscription_terms_present": None,
        "historical_halt_evidence": False, "executable_price": False,
        "contract": "exchange_share_replacement_reference_v1",
    }]
    output = public / "tw_share_replacement_reference.parquet"
    output.parent.mkdir(parents=True, exist_ok=True)
    pl.from_dicts(rows, infer_schema_length=None).write_parquet(output)
    output.with_suffix(".summary.json").write_text(json.dumps({
        "schema_version": 3,
        "coverage_start": "2014-01-01",
        "coverage_end": "2030-12-31",
        "source_download_complete": True,
        "failure_count": 0,
        "complete_all_markets": True,
        "lifecycle_catalog_complete": True,
        "covered_markets": ["twse", "tpex"],
        "rows": len(rows),
        "output_receipt": {
            "size": output.stat().st_size,
            "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
        },
    }), encoding="utf-8")


def test_share_replacement_maps_halt_conversion_and_unresolved_prefix(tmp_path):
    public = tmp_path / "public-release"
    _write_share_replacement_receipts(public, [{
        "symbol": "5493", "market": "tpex",
        "resume_date": date(2020, 10, 19),
        "suspension_date": date(2020, 10, 7),
        "new_shares_per_1000_old": 940.0,
        "cash_return_per_old_share": 0.6,
        "cash_payment_date": date(2020, 10, 23),
        "cash_dividend_per_old_share": None,
        "subscription_shares_per_1000": None,
        "subscription_terms_present": False,
        "historical_halt_evidence": True, "executable_price": False,
        "contract": "exchange_share_replacement_reference_v1",
    }, {
        "symbol": "9998", "market": "twse",
        "resume_date": date(2020, 10, 19), "suspension_date": None,
        "new_shares_per_1000_old": None,
        "cash_return_per_old_share": None, "cash_payment_date": None,
        "cash_dividend_per_old_share": None,
        "subscription_shares_per_1000": None,
        "subscription_terms_present": None,
        "historical_halt_evidence": False, "executable_price": False,
        "contract": "exchange_share_replacement_reference_v1",
    }])
    dates = np.asarray(
        ["2020-10-06", "2020-10-07", "2020-10-08", "2020-10-19"],
        dtype="datetime64[D]",
    )
    halted, action, ratio, cash, payment, delivery, block, counts = (
        _share_replacement_arrays(
            public_root=public, dates=dates, symbols=("5493", "9998")
        )
    )
    assert halted[:, 0].tolist() == [False, True, True, False]
    assert action[:, 0].tolist() == [False, False, False, True]
    assert ratio[-1, 0] == 0.94
    assert cash[-1, 0] == 0.6
    assert payment[-1, 0] == date(2020, 10, 23).toordinal()
    assert not delivery.any()
    assert block[:, 1].tolist() == [True, True, True, False]
    assert counts["mapped_exact_events"] == 1
    assert counts["unresolved_events"] == 1


def test_share_replacement_future_resume_never_masks_prior_history(tmp_path):
    public = tmp_path / "public-release"
    _write_share_replacement_receipts(public, [{
        "symbol": "5493", "market": "tpex",
        "resume_date": date(2020, 10, 19),
        "suspension_date": date(2020, 10, 7),
        "new_shares_per_1000_old": 940.0,
        "cash_return_per_old_share": 0.6,
        "cash_payment_date": date(2020, 10, 23),
        "cash_dividend_per_old_share": None,
        "subscription_shares_per_1000": None,
        "subscription_terms_present": False,
        "historical_halt_evidence": True, "executable_price": False,
        "contract": "exchange_share_replacement_reference_v1",
    }, {
        "symbol": "9998", "market": "twse",
        "resume_date": date(2020, 11, 2),
        "suspension_date": date(2020, 10, 30),
        "new_shares_per_1000_old": None,
        "cash_return_per_old_share": None, "cash_payment_date": None,
        "cash_dividend_per_old_share": None,
        "subscription_shares_per_1000": None,
        "subscription_terms_present": None,
        "historical_halt_evidence": True, "executable_price": False,
        "contract": "exchange_share_replacement_reference_v1",
    }])
    dates = np.asarray(
        ["2020-10-06", "2020-10-07", "2020-10-08"],
        dtype="datetime64[D]",
    )

    halted, action, _ratio, _cash, _payment, _delivery, block, counts = (
        _share_replacement_arrays(
            public_root=public, dates=dates, symbols=("5493", "9998")
        )
    )

    assert halted[:, 0].tolist() == [False, True, True]
    assert not halted[:, 1].any()
    assert not action.any()
    assert not block.any()
    assert counts["outside_horizon_events"] == 2
    assert counts["mapped_exact_events"] == 0
    assert counts["unresolved_events"] == 0


def test_share_replacement_closed_resume_maps_to_next_panel_session(tmp_path):
    public = tmp_path / "public-release"
    _write_share_replacement_receipts(public, [{
        "symbol": "2314", "market": "twse",
        "resume_date": date(2016, 9, 28),
        "suspension_date": date(2016, 9, 20),
        "new_shares_per_1000_old": 900.0,
        "cash_return_per_old_share": 1.0,
        "cash_payment_date": date(2016, 10, 5),
        "cash_dividend_per_old_share": None,
        "subscription_shares_per_1000": None,
        "subscription_terms_present": False,
        "historical_halt_evidence": True, "executable_price": False,
        "contract": "exchange_share_replacement_reference_v1",
    }])
    dates = np.asarray(
        ["2016-09-19", "2016-09-20", "2016-09-26", "2016-09-29"],
        dtype="datetime64[D]",
    )

    halted, action, ratio, cash, payment, _delivery, block, counts = (
        _share_replacement_arrays(
            public_root=public, dates=dates, symbols=("2314",)
        )
    )

    assert halted[:, 0].tolist() == [False, True, True, False]
    assert action[:, 0].tolist() == [False, False, False, True]
    assert ratio[-1, 0] == 0.9
    assert cash[-1, 0] == 1.0
    assert payment[-1, 0] == date(2016, 10, 5).toordinal()
    assert not block.any()
    assert counts["mapped_after_closed_date"] == 1


def test_exact_pending_stock_action_maps_ratio_and_delivery_without_price_proxy(
    tmp_path,
):
    public = tmp_path / "public-release"
    _write_parquet(
        public / "features/tw_public_stock_daily.parquet", {"x": [1]}
    )
    _write_parquet(
        public / "tw_corporate_action_reference.parquet",
        {
            "date": [date(2020, 8, 27)],
            "symbol": ["8941"],
            "reference_price": [36.09],
        },
    )
    _write_action_receipts(
        public,
        [
            {
                "date": date(2020, 8, 27),
                "symbol": "8941",
                "handling": "exact_inventory",
                "stock_ratio": 0.1,
                "stock_delivery": date(2020, 10, 14),
            }
        ],
    )

    mask, ratio, cash, payment, delivery, counts = (
        _exact_inventory_action_arrays(
            public_feature_path=public
            / "features/tw_public_stock_daily.parquet",
            dates=np.asarray(["2020-08-27"], dtype="datetime64[D]"),
            symbols=("8941",),
        )
    )

    assert mask.tolist() == [[True]]
    assert ratio.tolist() == [[1.1]]
    assert cash.tolist() == [[0.0]]
    assert payment.tolist() == [[0]]
    assert delivery.tolist() == [[date(2020, 10, 14).toordinal()]]
    assert counts["mapped_pending_stock_events"] == 1


def test_pure_subscription_right_maps_symmetric_official_reference_value(
    tmp_path,
):
    public = tmp_path / "public-release"
    _write_parquet(
        public / "features/tw_public_stock_daily.parquet", {"x": [1]}
    )
    _write_parquet(
        public / "tw_corporate_action_reference.parquet",
        {
            "date": [date(2020, 10, 23)],
            "symbol": ["6625"],
            "reference_price": [37.39],
        },
    )
    _write_action_receipts(
        public,
        [
            {
                "date": date(2020, 10, 23),
                "symbol": "6625",
                "handling": "avoid",
                "handling_reason": "stock_or_subscription_action",
                "reference_price": 37.39,
                "stock_terms_complete": True,
                "subscription_ratio": 0.113576395,
                "subscription_price": 30.0,
            }
        ],
    )

    mask, ratio, cash, payment, delivery, counts = (
        _exact_inventory_action_arrays(
            public_feature_path=public
            / "features/tw_public_stock_daily.parquet",
            dates=np.asarray(["2020-10-23"], dtype="datetime64[D]"),
            symbols=("6625",),
        )
    )

    expected = 0.113576395 * (37.39 - 30.0)
    assert mask.tolist() == [[True]]
    assert ratio.tolist() == [[1.0]]
    assert cash[0, 0] == pytest.approx(expected)
    assert payment.tolist() == [[date(2020, 10, 23).toordinal()]]
    assert delivery.tolist() == [[0]]
    assert counts["mapped_subscription_right_events"] == 1
    assert counts["mapped_zero_value_subscription_right_events"] == 0
    assert counts["_subscription_right_flat_indices"] == [0]


def test_incomplete_subscription_right_remains_unresolved(tmp_path):
    public = tmp_path / "public-release"
    _write_parquet(
        public / "features/tw_public_stock_daily.parquet", {"x": [1]}
    )
    _write_parquet(
        public / "tw_corporate_action_reference.parquet",
        {
            "date": [date(2020, 10, 23)],
            "symbol": ["6625"],
            "reference_price": [37.39],
        },
    )
    _write_action_receipts(
        public,
        [
            {
                "date": date(2020, 10, 23),
                "symbol": "6625",
                "handling": "avoid",
                "handling_reason": "stock_or_subscription_action",
                "reference_price": 37.39,
                "stock_terms_complete": False,
                "subscription_ratio": 0.113576395,
                "subscription_price": 30.0,
            }
        ],
    )

    mask, _ratio, _cash, _payment, _delivery, counts = (
        _exact_inventory_action_arrays(
            public_feature_path=public
            / "features/tw_public_stock_daily.parquet",
            dates=np.asarray(["2020-10-23"], dtype="datetime64[D]"),
            symbols=("6625",),
        )
    )

    assert not mask.any()
    assert counts["mapped_subscription_right_events"] == 0


def test_physical_source_builds_once_and_loads_lazy_symbol_day_sessions(
    tmp_path, monkeypatch,
):
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
        [{
            "date": date(2019, 1, 2),
            "symbol": "2330",
            "handling": "avoid",
            "handling_reason": "stock_or_subscription_action",
            "reference_price": 100.0,
            "stock_terms_complete": True,
            "subscription_ratio": 0.1,
            # A complete right may be worth exactly zero. It remains a
            # resolved source event, not an invalid sparse-cache record.
            "subscription_price": 100.0,
        }],
    )
    _write_share_replacement_receipts(public)
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
    first_packed = source.packed_session_loader(0)
    finite_flat = torch.nonzero(
        torch.isfinite(first.exit_prices.reshape(-1)), as_tuple=False
    ).flatten()
    torch.testing.assert_close(first_packed.exit_flat, finite_flat, rtol=0, atol=0)
    torch.testing.assert_close(
        first_packed.exit_price,
        first.exit_prices.reshape(-1)[finite_flat], rtol=0, atol=0,
    )
    torch.testing.assert_close(
        first_packed.exit_capacity,
        first.exit_capacity.reshape(-1)[finite_flat], rtol=0, atol=0,
    )
    assert second.entry_volume.tolist() == [2000.0, 2000.0, 5000.0 / 271.0]
    assert second.source_gap_mask.tolist() == [0.0, 0.0, 0.0]
    assert second.daily_proxy_mask.tolist() == [0.0, 0.0, 1.0]
    assert first.terminal_liquidation_price is None
    assert first.action_mask.tolist() == [1.0, 0.0, 0.0]
    assert first.share_ratio.tolist() == [1.0, 1.0, 1.0]
    assert first.cash_per_old_share.tolist() == [0.0, 0.0, 0.0]
    assert source.audit_receipt["corporate_action_policy"][
        "mapped_zero_value_subscription_right_events"
    ] == 1
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
    terminal_source = build_prepared_day_trade_carry_source(
        panel=panel,
        minute_root=minute,
        public_feature_path=public / "features/tw_public_stock_daily.parquet",
        cache_dir=tmp_path / "cache",
        allow_daily_proxy=True,
        daily_proxy_price_policy="official_open_close",
        corporate_action_mode="avoid",
        terminal_liquidation_unlimited_capacity=True,
        sparse_event_slots=1024,
    )
    terminal = terminal_source.session_at(1)
    assert terminal.terminal_liquidation_price.tolist() == [100.0, 50.0, 120.0]
    assert terminal_source.audit_receipt["terminal_liquidation_policy"] == (
        "official_close_unlimited_capacity_reduction_only"
    )
    sparse_capacity = terminal_source.audit_receipt["sparse_event_capacity"]
    assert sparse_capacity["configured_slots"] == 1024
    assert sparse_capacity["required_slots"] >= 6
    assert sparse_capacity["session_files_checked"] == 1
    with pytest.raises(ValueError, match="fixed compiled ABI before training"):
        build_prepared_day_trade_carry_source(
            panel=panel,
            minute_root=minute,
            public_feature_path=public / "features/tw_public_stock_daily.parquet",
            cache_dir=tmp_path / "cache",
            allow_daily_proxy=True,
            daily_proxy_price_policy="official_open_close",
            corporate_action_mode="avoid",
            terminal_liquidation_unlimited_capacity=True,
            sparse_event_slots=1,
        )
    terminal_packed = terminal_source.packed_session_loader(1)
    for row in (0, 1):
        direct = terminal_source.compact_session_loader(row)
        reference = compact_day_trade_carry_session(terminal_source.session_at(row))
        direct.validate_shape(3, torch.device("cpu"))
        for field in fields(direct):
            actual = getattr(direct, field.name)
            expected = getattr(reference, field.name)
            if isinstance(actual, torch.Tensor):
                torch.testing.assert_close(actual, expected, rtol=0, atol=0, equal_nan=True)
            else:
                assert actual == expected

    empty = replace(
        terminal_packed,
        exit_flat=terminal_packed.exit_flat[:0],
        exit_price=terminal_packed.exit_price[:0],
        exit_capacity=terminal_packed.exit_capacity[:0],
    )
    empty_compact = compact_packed_day_trade_carry_session(empty)
    empty_compact.validate_shape(3, torch.device("cpu"))
    assert empty_compact.symbol_event_ends.tolist() == [0, 0, 0]
    assert empty_compact.exit_capacity.tolist() == [0.0]
    if terminal_packed.exit_flat.numel() > 0:
        invalid = replace(
            terminal_packed,
            exit_flat=terminal_packed.exit_flat[:1].repeat(2),
            exit_price=terminal_packed.exit_price[:1].repeat(2),
            exit_capacity=terminal_packed.exit_capacity[:1].repeat(2),
        )
        with pytest.raises(ValueError, match="strictly ordered"):
            compact_packed_day_trade_carry_session(invalid)
    terminal_rebuilt = PreparedDayTradeCarryBatch.from_packed_sessions(
        (terminal_packed,), 1, event_compression=True
    ).sessions(torch.device("cpu"))[0]
    torch.testing.assert_close(
        terminal_rebuilt.terminal_liquidation_price,
        terminal.terminal_liquidation_price,
        rtol=0,
        atol=0,
    )
    # A child in the same orchestration run must reuse the parent's completed
    # byte verification only while the parent PID/file identities remain live.
    monkeypatch.setenv(
        PHYSICAL_SOURCE_RUN_RECEIPT_ENV,
        source.audit_receipt["run_verification_receipt"],
    )
    again = build_prepared_day_trade_carry_source(
        panel=panel, minute_root=minute,
        public_feature_path=public / "features/tw_public_stock_daily.parquet",
        cache_dir=tmp_path / "cache", allow_daily_proxy=True,
        daily_proxy_price_policy="official_open_close",
        corporate_action_mode="avoid",
    )
    assert again.release_id == source.release_id
    assert again.audit_receipt["run_verification_mode"] == "run_receipt"
    price_limit_caches = list(
        (tmp_path / "cache").glob("physical-price-limits-*/READY.json")
    )
    assert len(price_limit_caches) == 1


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
    _write_share_replacement_receipts(public)
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
    assert source.packed_session_loader is not None
    packed = source.packed_session_loader(0)
    rebuilt = PreparedDayTradeCarryBatch.from_packed_sessions(
        (packed,), 1, event_compression=True
    ).sessions(torch.device("cpu"))[0]
    for field in session.__dataclass_fields__:
        expected = getattr(session, field)
        actual = getattr(rebuilt, field)
        if isinstance(expected, torch.Tensor):
            torch.testing.assert_close(actual, expected, rtol=0, atol=0, equal_nan=True)
        else:
            assert actual == expected
    compact = source.compact_session_at(0)
    assert compact.uses_sparse_events
    assert compact.exit_prices.ndim == 1
    assert compact.exit_prices.numel() < session.exit_prices.numel()
    assert compact.marks.shape == (3, 1)
    assert compact.mark_path_valid.tolist() == [1.0, 1.0, 1.0]
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
    _write_share_replacement_receipts(public)
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
        "share_replacement_symbol_days": 0,
        "combined_total_symbol_days": 1,
        "valuation": "carry_previous_observable_regular_market_mark",
        "entry_exit_capacity": "zero",
        "absent_official_row": "not_classified_and_never_auto_filled",
    }
