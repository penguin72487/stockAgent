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
from scripts import run_tw_day_trade_simulation as live_runner
from stockagent.live.tw_day_trade_simulation import (
    ENTRY_FILL_POLICY_0901_MINUTE_VWAP,
    ENTRY_FILL_POLICY_CAUSAL_BOOK,
    ENTRY_FILL_POLICY_CAUSAL_BOOK_ELSE_OPEN_TICK,
    ENTRY_FILL_POLICY_OFFICIAL_OPEN_AT_0901,
)


TAIPEI = ZoneInfo("Asia/Taipei")


def test_benchmark_history_timestamp_only_rebuild_is_a_noop(tmp_path: Path) -> None:
    destination = tmp_path / "benchmark_history.json"
    original = {
        "schema_version": 1,
        "created_at": "2026-09-12T13:30:00+08:00",
        "marks": [{"benchmark_id": "benchmark_0050", "last_mark_price": 61.2}],
    }
    assert benchmark_replay._write_json_if_semantically_changed(
        destination,
        original,
        compact=True,
    ) is True
    before = destination.stat()
    before_bytes = destination.read_bytes()

    rebuilt = {**original, "created_at": "2026-09-13T13:30:00+08:00"}
    assert benchmark_replay._write_json_if_semantically_changed(
        destination,
        rebuilt,
        compact=True,
    ) is False

    assert destination.read_bytes() == before_bytes
    assert destination.stat().st_mtime_ns == before.st_mtime_ns


def test_benchmark_history_real_change_is_published(tmp_path: Path) -> None:
    destination = tmp_path / "benchmark_history.json"
    first = {"created_at": "first", "marks": [{"last_mark_price": 61.2}]}
    second = {"created_at": "second", "marks": [{"last_mark_price": 61.3}]}
    assert benchmark_replay._write_json_if_semantically_changed(destination, first)
    assert benchmark_replay._write_json_if_semantically_changed(destination, second)
    assert json.loads(destination.read_text(encoding="utf-8")) == second


def test_replay_checks_receipt_backed_repair_kbars_before_bulk_minute_data() -> None:
    roots = replay.DEFAULT_MINUTE_DATA_ROOTS

    assert roots[0] == Path(
        "artifacts/data_repair/tw_day_trade_minute_curve/maintenance/current/fetched_kbars"
    )
    assert roots[1] == Path(
        "artifacts/data_repair/tw_day_trade_minute_curve/kbars"
    )
    assert roots.index(Path("data_tw_minute/shioaji_1m")) > 1


def _complete_0901_query_receipt(*, resolved: int, requested: int) -> dict:
    return {
        "requested_symbols": requested,
        "queried_symbols": requested,
        "resolved_symbols": resolved,
        "source_empty_symbols": requested - resolved,
        "contract_missing_symbols": 0,
        "unqueried_symbols": 0,
        "error_counts": {},
        "stopped_for_traffic": False,
    }


def test_live_missed_opening_retries_source_empty_during_settle_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def fake_fetch(
        symbols,
        *,
        trading_date,
        max_traffic_fraction,
        progress_callback=None,
    ):
        calls.append(list(symbols))
        resolved_symbol = symbols[0]
        return (
            {
                resolved_symbol: {
                    "execution_price_0901": 100.0,
                    "source": "fixture_0901_vwap",
                }
            },
            _complete_0901_query_receipt(resolved=1, requested=len(symbols)),
        )

    monkeypatch.setattr(
        live_runner,
        "fetch_shioaji_historical_stock_0901_vwaps",
        fake_fetch,
    )
    first_prices, first_receipt = live_runner._resolve_missed_opening_prices(
        tmp_path,
        datetime(2026, 8, 13, 9, 1, 5, tzinfo=TAIPEI),
        {"1101", "2330"},
    )
    assert len(first_prices) == 1
    assert first_receipt["source_settling"] is True

    second_prices, second_receipt = live_runner._resolve_missed_opening_prices(
        tmp_path,
        datetime(2026, 8, 13, 9, 1, 30, tzinfo=TAIPEI),
        {"1101", "2330"},
    )
    assert calls == [["1101", "2330"], ["2330"]]
    assert set(second_prices) == {"1101", "2330"}
    assert second_receipt["source_settling"] is False


def test_live_missed_opening_finalizes_empty_source_after_settle_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        live_runner,
        "fetch_shioaji_historical_stock_0901_vwaps",
        lambda symbols, **_kwargs: (
            {},
            _complete_0901_query_receipt(resolved=0, requested=len(symbols)),
        ),
    )
    _prices, settling = live_runner._resolve_missed_opening_prices(
        tmp_path,
        datetime(2026, 8, 13, 9, 1, 5, tzinfo=TAIPEI),
        {"2330"},
    )
    assert settling["source_settling"] is True

    def unexpected_fetch(*_args, **_kwargs):
        raise AssertionError("terminal source-empty must not be queried forever")

    monkeypatch.setattr(
        live_runner,
        "fetch_shioaji_historical_stock_0901_vwaps",
        unexpected_fetch,
    )
    prices, finalized = live_runner._resolve_missed_opening_prices(
        tmp_path,
        datetime(2026, 8, 13, 9, 3, tzinfo=TAIPEI),
        {"2330"},
    )
    assert prices == {}
    assert finalized["source_settling"] is False
    assert finalized["unresolved_union_symbols"] == 1


def test_completed_0901_query_is_scoped_and_new_account_symbols_are_queried(tmp_path, monkeypatch):
    calls = []
    def fetch(symbols, **kwargs):
        calls.append(list(symbols))
        return {}, _complete_0901_query_receipt(resolved=0, requested=len(symbols))
    monkeypatch.setattr(live_runner, "fetch_shioaji_historical_stock_0901_vwaps", fetch)
    now = datetime(2026, 8, 13, 11, 0, tzinfo=TAIPEI)
    live_runner._resolve_missed_opening_prices(tmp_path, now, {"2330"})
    live_runner._resolve_missed_opening_prices(tmp_path, now, {"2330", "4905"})
    live_runner._resolve_missed_opening_prices(tmp_path, now, {"2330", "4905"})
    assert calls == [["2330"], ["4905"]]
    _, receipt = live_runner._load_missed_opening_prices(tmp_path, now)
    assert receipt["attempted_symbols"] == ["2330", "4905"]


def test_replay_candidate_retains_complete_benchmark_history(tmp_path: Path) -> None:
    source = tmp_path / "source" / "benchmark_history.json"
    source.parent.mkdir()
    payload = {
        "marks": [{"benchmark_id": "benchmark_0050"}],
        "origins": {
            "benchmark_0050": {},
            "benchmark_2330": {},
            "benchmark_tx_continuous": {},
        },
    }
    source.write_text(json.dumps(payload), encoding="utf-8")

    receipt = replay._retain_benchmark_history(source, tmp_path / "candidate")

    retained = tmp_path / "candidate" / "benchmark_history.json"
    assert retained.read_bytes() == source.read_bytes()
    assert receipt["marks"] == 1
    assert receipt["origins"] == sorted(payload["origins"])


def test_missing_retained_book_is_explicit_only_when_fallback_is_authorized(
    tmp_path: Path,
) -> None:
    day = date(2026, 8, 24)
    with pytest.raises(FileNotFoundError):
        replay._load_retained_historical_entry_books(
            historical_book_root=tmp_path,
            trading_date=day,
        )

    books, receipt = replay._load_retained_historical_entry_books(
        historical_book_root=tmp_path,
        trading_date=day,
        allow_missing=True,
    )
    assert books == {}
    assert receipt["source_missing"] is True
    assert receipt["fallback_authorized"] is True
    assert receipt["additional_shioaji_requests"] == 0


def test_open_only_replay_cannot_fabricate_best_bid_ask() -> None:
    spec = type(
        "Spec",
        (),
        {
            "market": "tw_day_trade",
            "entry_fill_policy": ENTRY_FILL_POLICY_CAUSAL_BOOK,
            "initial_capital_twd": 10_000_000.0,
            "lot_size": 1_000,
        },
    )()

    with pytest.raises(RuntimeError, match="no received best bid/ask"):
        replay._entry_quotes(
            [{"symbol": "2330", "target_weight": 0.1, "open_price": 1_000.0}],
            {
                "2330": {
                    "upper_limit_price": 1_100.0,
                    "lower_limit_price": 900.0,
                    "reference_price": 1_000.0,
                }
            },
            quote_at=datetime(2026, 8, 13, 9, 0, tzinfo=TAIPEI),
            spec=spec,
            canonical_open_by_symbol={"2330": 1_000.0},
            canonical_open_source="official_daily_session_open",
        )

    with pytest.raises(RuntimeError, match="open-only replay is retired"):
        replay._require_received_entry_book([spec])


def test_hybrid_entry_quotes_use_required_book_side_and_label_missing_side() -> None:
    spec = type(
        "Spec",
        (),
        {
            "market": "tw_day_trade",
            "entry_fill_policy": ENTRY_FILL_POLICY_CAUSAL_BOOK_ELSE_OPEN_TICK,
            "initial_capital_twd": 10_000_000.0,
            "lot_size": 1_000,
        },
    )()
    rows = [
        {"symbol": "2330", "target_weight": 0.5, "open_price": 1_000.0},
        {"symbol": "2317", "target_weight": -0.5, "open_price": 200.0},
    ]
    limits = {
        symbol: {
            "upper_limit_price": 1_100.0,
            "lower_limit_price": 900.0,
            "reference_price": 1_000.0,
        }
        for symbol in ("2330", "2317")
    }
    quotes, quality = replay._entry_quotes(
        rows,
        limits,
        quote_at=datetime(2026, 8, 13, 9, 0, tzinfo=TAIPEI),
        spec=spec,
        canonical_open_by_symbol={"2330": 1_000.0, "2317": 200.0},
        canonical_open_source="official_daily_session_open",
        historical_books={
            "2330": {
                "ask": 1_005.0,
                "ask_volume": 7.0,
                "ask_quote_at": "2026-08-13T09:00:07+08:00",
                "source": "shioaji:historical_stock_tick_best_quote",
            },
            "2317": {
                "ask": 201.0,
                "ask_volume": 10.0,
                "ask_quote_at": "2026-08-13T09:00:02+08:00",
                "source": "shioaji:historical_stock_tick_best_quote",
            },
        },
    )

    assert quotes["2330"]["ask"] == 1_005.0
    assert quotes["2330"]["ask_volume"] == 7.0
    assert quotes["2330"]["entry_price_is_synthetic_fallback"] is False
    assert quotes["2330"]["historical_source_quote_at"].startswith(
        "2026-08-13T09:00:07"
    )
    assert quotes["2317"]["bid"] is None
    assert quotes["2317"]["entry_price_is_synthetic_fallback"] is True
    assert quality["exact_best_quote_symbols"] == ["2330"]
    assert quality["adverse_tick_fallback_symbols"] == ["2317"]


def test_official_open_entry_quotes_do_not_require_or_fabricate_book() -> None:
    spec = type(
        "Spec",
        (),
        {
            "market": "tw_day_trade",
            "entry_fill_policy": ENTRY_FILL_POLICY_OFFICIAL_OPEN_AT_0901,
            "initial_capital_twd": 10_000_000.0,
            "lot_size": 1_000,
        },
    )()
    quotes, quality = replay._entry_quotes(
        [{"symbol": "2330", "target_weight": 0.1, "open_price": 1_000.0}],
        {
            "2330": {
                "upper_limit_price": 1_100.0,
                "lower_limit_price": 900.0,
                "reference_price": 1_000.0,
            }
        },
        quote_at=datetime(2026, 8, 13, 9, 1, tzinfo=TAIPEI),
        spec=spec,
        canonical_open_by_symbol={"2330": 1_000.0},
        canonical_open_source="official_daily_session_open",
    )

    assert quotes["2330"]["open"] == 1_000.0
    assert quotes["2330"]["bid"] is None
    assert quotes["2330"]["ask"] is None
    assert quotes["2330"]["entry_price_is_synthetic_fallback"] is False
    assert quality["adverse_tick_fallback_rows"] == 0


def test_0901_vwap_entry_quotes_keep_open_and_execution_prices_separate() -> None:
    spec = type(
        "Spec",
        (),
        {
            "market": "tw_day_trade",
            "entry_fill_policy": ENTRY_FILL_POLICY_0901_MINUTE_VWAP,
            "initial_capital_twd": 10_000_000.0,
            "lot_size": 1_000,
        },
    )()
    quotes, quality = replay._entry_quotes(
        [{"symbol": "2330", "target_weight": 0.1, "open_price": 1_000.0}],
        {
            "2330": {
                "upper_limit_price": 1_100.0,
                "lower_limit_price": 900.0,
                "reference_price": 1_000.0,
            }
        },
        quote_at=datetime(2026, 8, 13, 9, 1, tzinfo=TAIPEI),
        spec=spec,
        canonical_open_by_symbol={"2330": 1_000.0},
        canonical_open_source="official_daily_session_open",
        historical_books={
            "2330": {
                "execution_price_0901": 1_006.25,
                "source_window_end": "2026-08-13T09:00:59.500000+08:00",
                "source": (
                    "shioaji:historical_ticks_0900_090059_vwap_right_label_0901"
                ),
            }
        },
    )

    assert quotes["2330"]["open"] == 1_000.0
    assert quotes["2330"]["execution_price_0901"] == 1_006.25
    assert quotes["2330"]["last"] == 1_006.25
    assert quotes["2330"]["bid"] is None
    assert quotes["2330"]["ask"] is None
    assert quality["observed_0901_vwap_symbols"] == ["2330"]
    assert quality["missing_0901_vwap_symbols"] == []


def test_local_0901_vwap_loader_uses_amount_over_normalized_shares(
    tmp_path: Path,
) -> None:
    partition = tmp_path / "minute" / "trade_date=2026-08-13"
    partition.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["2330", "2330"],
            "date": [date(2026, 8, 13), date(2026, 8, 13)],
            "ts": [
                datetime(2026, 8, 13, 9, 1),
                datetime(2026, 8, 13, 9, 2),
            ],
            "minutes_from_open": [1, 2],
            "Amount": [100_750.0, 999_000.0],
            "volume_shares": [1_000.0, 1_000.0],
            "Low": [100.0, 999.0],
            "High": [102.0, 999.0],
        }
    ).write_parquet(partition / "data.parquet")

    rows, receipt = replay._local_0901_vwap_rows(
        minute_roots=(tmp_path / "minute",),
        symbols=["2330"],
        trading_date=date(2026, 8, 13),
    )

    assert rows["2330"]["execution_price_0901"] == 100.75
    assert rows["2330"]["execution_price_0901_method"] == "minute_vwap"
    assert rows["2330"]["quote_at"] == "2026-08-13T09:01:00+08:00"
    assert receipt["resolved_symbols"] == 1
    assert receipt["additional_shioaji_requests"] == 0


def test_local_0901_price_loader_uses_source_kbar_close_without_tick_or_vwap(
    tmp_path: Path,
) -> None:
    partition = tmp_path / "minute" / "trade_date=2026-08-13"
    partition.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["2330"],
            "date": [date(2026, 8, 13)],
            "ts": [datetime(2026, 8, 13, 9, 1)],
            "minutes_from_open": [1],
            "Open": [100.0],
            "High": [102.0],
            "Low": [99.0],
            "Close": [101.0],
            "Amount": [0.0],
            "volume_shares": [0.0],
        }
    ).write_parquet(partition / "data.parquet")

    rows, receipt = replay._local_0901_vwap_rows(
        minute_roots=(tmp_path / "minute",),
        symbols=["2330"],
        trading_date=date(2026, 8, 13),
    )

    assert rows["2330"]["execution_price_0901"] == 101.0
    assert rows["2330"]["execution_price_0901_method"] == "minute_close"
    assert rows["2330"]["tick_count_0901"] == 0
    assert rows["2330"]["valuation_price_0901"] is None
    assert receipt["price_method_counts"] == {"minute_close": 1}


def test_previous_official_session_ignores_malformed_legacy_date(
    tmp_path: Path,
) -> None:
    twse = tmp_path / "twse.parquet"
    tpex = tmp_path / "tpex.parquet"
    pl.DataFrame({"date": ["2014-12-;1", "2026-02-24", "2026-02-25"]}).write_parquet(
        twse
    )
    pl.DataFrame({"date": ["2026-02-23", "2026-02-24"]}).write_parquet(tpex)

    assert signal_backfill._previous_official_session_date(
        twse, tpex, date(2026, 2, 25)
    ) == date(2026, 2, 24)


def test_intraday_bar_loader_preserves_right_label_and_observed_vwap(
    tmp_path: Path,
) -> None:
    root = tmp_path / "minute"
    partition = root / "trade_date=2026-02-25"
    partition.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["2330"],
            "ts": [datetime(2026, 2, 25, 13, 24)],
            "Open": [100.0],
            "High": [102.0],
            "Low": [99.0],
            "Close": [101.0],
            "Amount": [100_500.0],
            "volume_shares": [1_000.0],
        }
    ).write_parquet(partition / "data.parquet")

    bars, receipt = replay._minute_bar_rows(
        (root,),
        trading_date=date(2026, 2, 25),
        symbols={"2330"},
    )

    row = bars["2330"]["2026-02-25T13:24+08:00"]
    assert row["vwap"] == 100.5
    assert row["high"] == 102.0
    assert row["low"] == 99.0
    assert receipt["resolved_symbols"] == 1
    assert receipt["missing_symbols"] == []


def test_intraday_zero_volume_padding_cannot_refresh_price_or_trigger_orders(tmp_path):
    root = tmp_path / "minute"
    partition = root / "trade_date=2026-09-07"
    partition.mkdir(parents=True)
    pl.DataFrame({"symbol": ["8342", "8342"],
        "ts": [datetime(2026, 9, 7, 9, 2), datetime(2026, 9, 7, 10, 1)],
        "Open": [87., 88.], "High": [87., 88.], "Low": [87., 88.], "Close": [87., 88.],
        "Amount": [0., 88000.], "volume_shares": [0., 1000.]}).write_parquet(partition / "data.parquet")
    bars, proof = replay._minute_bar_rows((root,), trading_date=date(2026, 9, 7), symbols={"8342"})
    assert list(bars["8342"]) == ["2026-09-07T10:01+08:00"]
    assert proof["ignored_zero_volume_rows"] == 1


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
            "input_sha256": replay._sha256(input_path),
        },
    }
    (signal_root / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
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


def test_replay_resolves_accepted_isolated_artifact_not_same_id_live_copy(tmp_path):
    day = date(2026, 8, 13)
    specs = []
    for name in ("accepted", "live"):
        spec = _write_signal_candidate(tmp_path / name, market="mode_a", trading_date=day,
            generated_at=datetime(2026, 8, 20, 20, 0, tzinfo=TAIPEI), counterfactual=True)
        path = next(spec.live_output_dir.rglob("summary.json"))
        data = json.loads(path.read_text()) | {"signal_id": "same-id", "version": name}
        path.write_text(json.dumps(data))
        specs.append(spec)
    accepted = next(specs[0].live_output_dir.rglob("summary.json"))
    row = {"market": "mode_a", "signal_id": "same-id", "summary_path": str(accepted),
           "weights_path": str(accepted.with_name("target_weights.parquet")),
           "summary_sha256": replay._sha256(accepted)}
    source = tmp_path / "ledger"
    nested = source / "account_receipts" / "mode_a"
    nested.mkdir(parents=True)
    (nested / "rebuild_receipt.json").write_text(json.dumps({"sessions": [
        {"session_date": str(day), "modes": [row]}]}))
    pins = replay._source_ledger_signal_artifacts(source, {(str(day), "mode_a"): "same-id"})
    proof = pins[(str(day), "mode_a")]
    result = replay._latest_valid_signal(specs[1], day, preferred_signal_id="same-id", pinned_artifact=proof)
    assert result[3]["version"] == "accepted"
    input_path = Path(proof["open_input_path"])
    original_input = input_path.read_bytes()
    input_path.write_bytes(original_input + b" ")
    with pytest.raises(ValueError, match="open input hash changed"):
        replay._latest_valid_signal(specs[1], day, preferred_signal_id="same-id", pinned_artifact=proof)
    with pytest.raises(ValueError, match="open input hash changed"):
        replay._source_ledger_signal_artifacts(source, {(str(day), "mode_a"): "same-id"})
    input_path.write_bytes(original_input)
    accepted.write_text(accepted.read_text() + " ")
    with pytest.raises(ValueError, match="hash changed"):
        replay._latest_valid_signal(specs[1], day, preferred_signal_id="same-id", pinned_artifact=proof)
    with pytest.raises(ValueError, match="hash changed"):
        replay._source_ledger_signal_artifacts(source, {(str(day), "mode_a"): "same-id"})
    # Explicit replacement removed the old pin; stale old artifacts do not
    # become an authority for a newly requested model.
    assert replay._source_ledger_signal_artifacts(source, {}) == {}


def test_declared_missing_signal_artifact_never_falls_back_to_live_directory(tmp_path):
    path = tmp_path / "missing" / "summary.json"
    (tmp_path / "rebuild_receipt.json").write_text(json.dumps({"sessions": [
        {"session_date": "2026-08-13", "modes": [{"market": "mode_a", "signal_id": "id",
         "summary_path": str(path), "weights_path": str(path.with_name("target_weights.parquet"))}]}]}))
    with pytest.raises(FileNotFoundError):
        replay._source_ledger_signal_artifacts(tmp_path, {("2026-08-13", "mode_a"): "id"})


def test_conflicting_accepted_artifacts_with_same_id_are_rejected(tmp_path):
    day = date(2026, 8, 13)
    rows = []
    for name in ("first", "second"):
        spec = _write_signal_candidate(tmp_path / name, market="mode_a", trading_date=day,
            generated_at=datetime(2026, 8, 20, 20, 0, tzinfo=TAIPEI), counterfactual=True)
        path = next(spec.live_output_dir.rglob("summary.json"))
        data = json.loads(path.read_text()) | {"signal_id": "id", "version": name}
        path.write_text(json.dumps(data))
        rows.append({"market": "mode_a", "signal_id": "id", "summary_path": str(path),
                     "weights_path": str(path.with_name("target_weights.parquet"))})
    (tmp_path / "rebuild_receipt.json").write_text(json.dumps({"sessions": [
        {"session_date": str(day), "modes": rows}]}))
    with pytest.raises(ValueError, match="conflicting accepted signal artifacts"):
        replay._source_ledger_signal_artifacts(tmp_path, {(str(day), "mode_a"): "id"})


def test_current_session_uses_state_artifact_when_no_history_receipt_exists(tmp_path):
    day = date(2026, 8, 13)
    spec = _write_signal_candidate(tmp_path / "live", market="mode_a", trading_date=day,
        generated_at=datetime(2026, 8, 13, 9, 0, tzinfo=TAIPEI))
    path = next(spec.live_output_dir.rglob("summary.json"))
    data = json.loads(path.read_text()) | {"signal_id": "current"}
    path.write_text(json.dumps(data))
    (tmp_path / "state.json").write_text(json.dumps({"modes": {"mode_a": {
        "session_date": str(day), "signal_id": "current", "signal_source_path": str(path),
        "target_weights_path": str(path.with_name("target_weights.parquet"))}}}))
    pins = replay._source_ledger_signal_artifacts(tmp_path, {(str(day), "mode_a"): "current"})
    result = replay._latest_valid_signal(spec, day, preferred_signal_id="current",
                                        pinned_artifact=pins[(str(day), "mode_a")])
    assert result[3]["signal_id"] == "current"


def test_realistic_replay_policy_preflight_rejects_unversioned_signal():
    spec = type("Spec", (), {"market": "mode_a", "uses_realistic_execution": True})()
    with pytest.raises(RuntimeError, match="exact-session policy mask"):
        replay._validate_replay_policy_summary(spec, date(2026, 8, 13), {})
    replay._validate_replay_policy_summary(spec, date(2026, 8, 13), {
        "day_trade_policy_mask_contract": "exact_session_eligibility_before_forward_v1"})


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

    assert (
        replay._retained_rule_response_kind("twse_day_trade_eligibility", raw)
        == "twse_day_trade_openapi_json"
    )
    assert (
        replay._retained_rule_response_kind("tpex_day_trade_eligibility", raw) == "json"
    )


def test_replay_range_can_skip_weekend_non_sessions() -> None:
    assert replay._is_weekend(date(2026, 8, 15))
    assert replay._is_weekend(date(2026, 8, 16))
    assert not replay._is_weekend(date(2026, 8, 17))


def test_source_ledger_pins_each_date_mode_signal_identity(tmp_path: Path) -> None:
    events = [
        {
            "event": "signal_registered",
            "recorded_at": "2026-08-13T09:00:00+08:00",
            "market": "mode_a",
            "signal_id": "original-a",
        },
        {
            "event": "signal_registered",
            "recorded_at": "2026-08-13T09:00:00+08:00",
            "market": "mode_b",
            "signal_id": "original-b",
        },
        {
            "event": "mark",
            "recorded_at": "2026-08-13T09:01:00+08:00",
            "market": "mode_a",
        },
    ]
    (tmp_path / "events.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in events),
        encoding="utf-8",
    )

    identities, provenance = replay._source_ledger_signal_ids(
        tmp_path,
        start_date=date(2026, 8, 13),
        end_date=date(2026, 8, 13),
    )

    assert identities == {
        ("2026-08-13", "mode_a"): "original-a",
        ("2026-08-13", "mode_b"): "original-b",
    }
    assert provenance["signal_registrations"] == 2
    assert len(provenance["sha256"]) == 64


def test_source_signal_pins_replace_only_the_selected_stable_market() -> None:
    expected = {
        ("2026-08-13", "stable"),
        ("2026-08-13", "control"),
        ("2026-08-14", "stable"),
        ("2026-08-14", "control"),
    }
    source = {
        key: f"old-{day}-{market}"
        for day, market in expected
        for key in [(day, market)]
    }

    pins, provenance = replay._resolve_source_signal_pins(
        source,
        expected_signal_keys=expected,
        known_markets={"stable", "control"},
        allowed_unpinned_markets=set(),
        replacement_signal_markets={"stable"},
    )

    assert set(pins) == {
        ("2026-08-13", "control"),
        ("2026-08-14", "control"),
    }
    assert provenance == {
        "allowed_unpinned_markets": [],
        "replacement_signal_markets": ["stable"],
        "replaced_pinned_signal_keys": 2,
        "discovered_signal_keys": 2,
        "pinned_signal_keys": 2,
    }


def test_source_signal_pins_fail_closed_for_unselected_missing_mode() -> None:
    expected = {
        ("2026-08-13", "stable"),
        ("2026-08-13", "control"),
    }

    with pytest.raises(ValueError, match="missing replay signal identities"):
        replay._resolve_source_signal_pins(
            {("2026-08-13", "stable"): "old-stable"},
            expected_signal_keys=expected,
            known_markets={"stable", "control"},
            allowed_unpinned_markets=set(),
            replacement_signal_markets={"stable"},
        )


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
    pl.DataFrame({"date": ["2026-08-13", "2026-08-14"]}).write_parquet(twse_path)
    pl.DataFrame({"date": ["2026-08-13", "2026-08-14"]}).write_parquet(tpex_path)

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
        "adjclose": 1_210.0,
        "open": 1_200.0,
        "close": 1_210.0,
    }


def test_benchmark_sessions_follow_own_official_sources_not_strategy_receipt(
    tmp_path: Path,
) -> None:
    stock_root = tmp_path / "stocks"
    stock_root.mkdir()
    for symbol in ("0050", "2330"):
        pl.DataFrame(
            {
                "date": [date(2026, 8, 26), date(2026, 8, 27)],
                "open": [100.0, 101.0],
                "close": [101.0, 102.0],
            }
        ).write_parquet(stock_root / f"{symbol}_features.parquet")

    sessions = benchmark_replay._completed_stock_benchmark_sessions(
        start=date(2026, 8, 26),
        end=date(2026, 8, 27),
        stock_parquet_root=stock_root,
        twse_daily_ohlcv_path=tmp_path / "unused_twse.parquet",
        tpex_daily_ohlcv_path=tmp_path / "unused_tpex.parquet",
    )

    assert sessions == [date(2026, 8, 26), date(2026, 8, 27)]


def test_benchmark_current_session_adjustment_uses_retained_reference_receipt() -> None:
    captured: dict[str, object] = {}

    class Engine:
        def _stock_total_return_adjustment(self, **kwargs):
            captured.update(kwargs)
            return 1.0, [], "official_reference_complete_with_current_session_reference"

    context = {
        "corporate_action_coverage_end": "2026-09-02",
        "current_session_reference_price": 108.45,
        "current_session_reference_source": "shioaji:stock_snapshot+prepared_limits",
        "previous_official_close": 108.45,
        "previous_official_close_date": "2026-09-01",
        "previous_official_close_source": "twse_official:0050_features.parquet",
    }

    factor, actions = benchmark_replay._required_stock_adjustment(
        Engine(),
        symbol="0050",
        entry_at=datetime(2026, 8, 20, 9, 0, tzinfo=TAIPEI),
        mark_date=date(2026, 9, 2),
        current_session_reference=context,
    )

    assert factor == 1.0
    assert actions == []
    assert captured["current_reference_price"] == 108.45
    assert captured["previous_close"] == 108.45
    assert captured["previous_close_date"] == "2026-09-01"


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
    manifest_root = tmp_path / "manifests" / "trade_date=2026-08-20"
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


def test_tx_history_minute_grid_is_complete_without_interpolation() -> None:
    day = date(2026, 2, 25)
    books = pl.DataFrame(
        {
            "event_ts": [
                datetime(2026, 2, 25, 8, 45, 1),
                datetime(2026, 2, 25, 8, 47, 1),
            ],
            "bid_price_1": [35_200.0, 35_210.0],
            "ask_price_1": [35_205.0, 35_215.0],
        }
    )

    minutes = benchmark_replay._tx_complete_minute_books(
        books,
        trading_date=day,
        timestamp_column="event_ts",
        epoch_utc=False,
    )

    assert len(minutes) == 300
    assert minutes[0][0].strftime("%H:%M") == "08:45"
    assert minutes[-1][0].strftime("%H:%M") == "13:44"
    assert minutes[1][1]["bid_price_1"] == 35_200.0
    assert minutes[1][2] is False
    assert minutes[2][1]["bid_price_1"] == 35_210.0
    assert benchmark_replay._tx_contract_code("202603") == "TXFC6"
