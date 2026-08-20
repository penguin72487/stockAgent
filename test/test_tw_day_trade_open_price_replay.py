from __future__ import annotations

from datetime import date, datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from scripts import rebuild_tw_day_trade_open_price_replay as replay
from scripts import rebuild_tw_day_trade_benchmark_history as benchmark_replay
from scripts import backfill_tw_day_trade_open_signals as signal_backfill


TAIPEI = ZoneInfo("Asia/Taipei")


def _write_signal_candidate(
    root: Path,
    *,
    market: str,
    trading_date: date,
    generated_at: datetime,
    counterfactual: bool = False,
) -> object:
    signal_root = root / f"{trading_date.isoformat()} 09:00:00" / "signal"
    signal_root.mkdir(parents=True)
    input_path = root / "official_open.csv"
    input_path.write_text("symbol,price,open_price\n2330,1000,1000\n", encoding="utf-8")
    summary = {
        "market": market,
        "execution_mode": "tw_day_trade",
        "generated_at": generated_at.isoformat(),
        "live_session_open_feature_applied": True,
        "counterfactual_signal_regeneration": counterfactual,
        "simulation_only": counterfactual,
        "production_order_possible": False,
        "replay_effective_signal_at": datetime.combine(
            trading_date,
            datetime.min.time().replace(hour=9),
            tzinfo=TAIPEI,
        ).isoformat(),
        "counterfactual_open_provenance": {
            "source": "official_daily_session_open",
            "input_path": str(input_path),
            "input_sha256": "fixture-sha256",
        },
    }
    (signal_root / "summary.json").write_text(
        json.dumps(summary), encoding="utf-8"
    )
    pl.DataFrame(
        {"symbol": ["2330"], "target_weight": [0.5], "open_price": [1000.0]}
    ).write_parquet(signal_root / "target_weights.parquet")
    return type(
        "Spec",
        (),
        {"market": market, "signal_market": None, "live_output_dir": root},
    )()


def test_latest_valid_signal_accepts_explicit_counterfactual_contract(
    tmp_path: Path,
) -> None:
    trading_date = date(2026, 8, 13)
    spec = _write_signal_candidate(
        tmp_path,
        market="new_mode",
        trading_date=trading_date,
        generated_at=datetime(2026, 8, 20, 20, 0, tzinfo=TAIPEI),
        counterfactual=True,
    )

    generated_at, _summary_path, _weights_path, summary, rows = (
        replay._latest_valid_signal(spec, trading_date)
    )

    assert generated_at.date() == date(2026, 8, 20)
    assert summary["counterfactual_signal_regeneration"] is True
    assert rows[0]["symbol"] == "2330"


def test_latest_valid_signal_rejects_backdated_signal_without_contract(
    tmp_path: Path,
) -> None:
    trading_date = date(2026, 8, 13)
    spec = _write_signal_candidate(
        tmp_path,
        market="new_mode",
        trading_date=trading_date,
        generated_at=datetime(2026, 8, 20, 20, 0, tzinfo=TAIPEI),
        counterfactual=False,
    )

    with pytest.raises(FileNotFoundError, match="no open-feature-applied"):
        replay._latest_valid_signal(spec, trading_date)


def test_retained_twse_openapi_rule_receipt_uses_exact_parser() -> None:
    raw = json.dumps(
        [
            {
                "Date": "1150817",
                "Code": "2330",
                "Name": "台積電",
                "Suspension": "",
            }
        ],
        ensure_ascii=False,
    ).encode("utf-8")

    assert replay._retained_rule_response_kind(
        "twse_day_trade_eligibility", raw
    ) == "twse_day_trade_openapi_json"
    assert replay._retained_rule_response_kind(
        "tpex_day_trade_eligibility", raw
    ) == "json"


def test_replay_range_can_skip_weekend_non_sessions() -> None:
    assert replay._is_weekend(date(2026, 8, 15))
    assert replay._is_weekend(date(2026, 8, 16))
    assert not replay._is_weekend(date(2026, 8, 17))


def test_counterfactual_open_input_excludes_intraday_high_low_close() -> None:
    rows, missing_limits = signal_backfill._open_input_rows(
        official_rows={
            "2330": {
                "open": 1_000.0,
                "max": 1_050.0,
                "min": 990.0,
                "close": 1_040.0,
            }
        },
        limits={
            "2330": {
                "reference_price": 990.0,
                "upper_limit_price": 1_085.0,
                "lower_limit_price": 895.0,
            }
        },
    )

    assert missing_limits == 0
    assert rows == [
        {
            "symbol": "2330",
            "price": 1_000.0,
            "open_price": 1_000.0,
            "upper_limit_price": 1_085.0,
            "lower_limit_price": 895.0,
            "reference_price": 990.0,
        }
    ]
    assert not ({"high", "low", "close"} & set(rows[0]))


def test_previous_counterfactual_panel_date_uses_exchange_sessions(
    tmp_path: Path,
) -> None:
    twse_path = tmp_path / "twse.parquet"
    tpex_path = tmp_path / "tpex.parquet"
    pl.DataFrame({"date": ["2026-08-13", "2026-08-14"]}).write_parquet(
        twse_path
    )
    pl.DataFrame({"date": ["2026-08-13", "2026-08-14"]}).write_parquet(
        tpex_path
    )

    assert signal_backfill._previous_official_session_date(
        twse_path, tpex_path, date(2026, 8, 17)
    ) == date(2026, 8, 14)


def _retained_signal(
    tmp_path: Path,
    *,
    market: str,
    opening: float,
    price_source: str = "fixture",
) -> tuple[datetime, Path, Path, dict[str, object], list[dict[str, object]]]:
    root = tmp_path / market
    root.mkdir()
    summary_path = root / "summary.json"
    weights_path = root / "target_weights.parquet"
    summary = {
        "live_session_open_feature_applied": True,
        "price_source": price_source,
    }
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    pl.DataFrame(
        {"symbol": ["2330"], "target_weight": [0.5], "open_price": [opening]}
    ).write_parquet(weights_path)
    generated_at = datetime(2026, 8, 20, 9, 1, tzinfo=TAIPEI)
    return (
        generated_at,
        summary_path,
        weights_path,
        summary,
        pl.read_parquet(weights_path).to_dicts(),
    )


def test_retained_signal_opens_are_materialized_with_provenance(tmp_path: Path) -> None:
    selected = {
        "mode_a": _retained_signal(tmp_path, market="mode_a", opening=1_000.0),
        "mode_b": _retained_signal(tmp_path, market="mode_b", opening=1_000.0),
    }
    specs = [
        type("Spec", (), {"market": "mode_a"})(),
        type("Spec", (), {"market": "mode_b"})(),
    ]

    resolved, receipt = replay._reuse_retained_signal_open_map(
        specs=specs,
        selected=selected,
        state_dir=tmp_path / "state",
        trading_date=date(2026, 8, 20),
    )

    assert resolved == {"2330": 1_000.0}
    assert receipt["additional_shioaji_requests"] == 0
    assert receipt["cross_mode_open_conflicts"] == 0
    assert len(receipt["source_signal_evidence"]) == 2
    row = pl.read_parquet(receipt["path"]).row(0, named=True)
    assert row["source"] == "retained_live_signal_open_feature:fixture"
    assert sorted(row["markets"]) == ["mode_a", "mode_b"]


def test_retained_signal_open_conflict_fails_closed(tmp_path: Path) -> None:
    selected = {
        "mode_a": _retained_signal(tmp_path, market="mode_a", opening=1_000.0),
        "mode_b": _retained_signal(tmp_path, market="mode_b", opening=1_005.0),
    }
    specs = [
        type("Spec", (), {"market": "mode_a"})(),
        type("Spec", (), {"market": "mode_b"})(),
    ]

    with pytest.raises(ValueError, match="session-open conflict"):
        replay._reuse_retained_signal_open_map(
            specs=specs,
            selected=selected,
            state_dir=tmp_path / "state",
            trading_date=date(2026, 8, 20),
        )


def test_retained_official_mis_open_is_canonical_over_shioaji(tmp_path: Path) -> None:
    selected = {
        "mode_a": _retained_signal(
            tmp_path,
            market="mode_a",
            opening=995.0,
            price_source="shioaji:stock_snapshot",
        ),
        "mode_b": _retained_signal(
            tmp_path,
            market="mode_b",
            opening=1_000.0,
            price_source="twse_tpex:mis",
        ),
    }
    specs = [
        type("Spec", (), {"market": "mode_a"})(),
        type("Spec", (), {"market": "mode_b"})(),
    ]

    resolved, receipt = replay._reuse_retained_signal_open_map(
        specs=specs,
        selected=selected,
        state_dir=tmp_path / "state",
        trading_date=date(2026, 8, 20),
    )

    assert resolved == {"2330": 1_000.0}
    assert receipt["noncanonical_open_mismatch_count"] == 1
    mismatch = receipt["noncanonical_open_mismatches"][0]
    assert mismatch["canonical_source"] == "twse_tpex:mis"
    assert mismatch["noncanonical_source"] == "shioaji:stock_snapshot"


def _write_official_aggregate_fixtures(tmp_path: Path) -> tuple[Path, Path]:
    twse_path = tmp_path / "twse_daily_ohlcv.parquet"
    tpex_path = tmp_path / "tpex_daily_ohlcv.parquet"
    pl.DataFrame(
        {
            "date": ["2026-08-20"],
            "證券代號": ["2330"],
            "開盤價": ["1,200.00"],
            "最高價": ["1,215.00"],
            "最低價": ["1,190.00"],
            "收盤價": ["1,210.00"],
        }
    ).write_parquet(twse_path)
    pl.DataFrame(
        {
            "date": ["2026-08-20"],
            "代號": ["6488"],
            "開盤": ["900.00"],
            "最高": ["920.00"],
            "最低": ["895.00"],
            "收盤": ["910.00"],
        }
    ).write_parquet(tpex_path)
    return twse_path, tpex_path


def test_official_daily_row_falls_back_to_fresh_venue_aggregate(
    tmp_path: Path,
) -> None:
    twse_path, tpex_path = _write_official_aggregate_fixtures(tmp_path)
    stock_root = tmp_path / "stocks"
    stock_root.mkdir()
    replay._official_daily_row.cache_clear()
    replay._official_aggregate_daily_rows.cache_clear()

    row = replay._official_daily_row(
        stock_root,
        "2330",
        date(2026, 8, 20),
        twse_path,
        tpex_path,
    )

    assert row is not None
    assert row["open"] == 1_200.0
    assert row["close"] == 1_210.0
    assert row["_official_source"] == "twse_daily_ohlcv"


def test_benchmark_stock_row_uses_same_official_aggregate_fallback(
    tmp_path: Path,
) -> None:
    twse_path, tpex_path = _write_official_aggregate_fixtures(tmp_path)

    row = benchmark_replay._stock_daily_row(
        tmp_path / "missing_2330_features.parquet",
        date(2026, 8, 20),
        symbol="2330",
        twse_daily_ohlcv_path=twse_path,
        tpex_daily_ohlcv_path=tpex_path,
    )

    assert row == {
        "date": date(2026, 8, 20),
        "open": 1_200.0,
        "close": 1_210.0,
    }


def test_replay_rows_cannot_fall_back_to_noncanonical_signal_open() -> None:
    rows = [
        {"symbol": "2330", "open_price": 1_195.0, "target_weight": 0.5},
        {"symbol": "3531", "open_price": 40.0, "target_weight": -0.5},
    ]

    canonicalized = replay._canonicalize_signal_rows_for_replay(
        rows,
        {"2330": 1_200.0},
    )

    assert canonicalized[0]["open_price"] == 1_200.0
    assert canonicalized[0]["source_signal_open_price"] == 1_195.0
    assert canonicalized[1]["open_price"] is None
    assert canonicalized[1]["source_signal_open_price"] == 40.0
    assert rows[1]["open_price"] == 40.0


def test_tx_front_contract_comes_from_each_sessions_capture_manifest(
    tmp_path: Path,
) -> None:
    manifest_root = (
        tmp_path / "manifests" / "trade_date=2026-08-20"
    )
    manifest_root.mkdir(parents=True)
    (manifest_root / "worker=00.json").write_text(
        json.dumps(
            {
                "status": "complete",
                "contract_metadata": [
                    {
                        "logical_code": "TXFR1",
                        "code": "TXFI6",
                        "delivery_month": "202609",
                        "last_trading_date": "2026-09-16",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    metadata, receipts = benchmark_replay._tx_front_contract_metadata(
        capture_root=tmp_path,
        trading_date=date(2026, 8, 20),
    )

    assert metadata == {
        "code": "TXFI6",
        "delivery_month": "202609",
        "last_trading_date": "2026-09-16",
    }
    assert len(receipts) == 1
    assert len(receipts[0]["sha256"]) == 64
