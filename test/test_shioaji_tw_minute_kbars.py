from __future__ import annotations

from datetime import date, datetime, timedelta
import hashlib
import json
import multiprocessing as mp
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from downloader.download_shioaji_tw_kbars import UniverseRow
from downloader.download_shioaji_tw_minute_kbars import (
    DEFAULT_REQUESTS_PER_SECOND,
    SHIOAJI_QUOTE_LIMIT_WINDOW_SECONDS,
    SharedRequestRateLimiter,
    SymbolResult,
    _write_run_summary,
    _write_market_hours_stop,
    completed_symbol_manifest_result,
    contract_for_stock_symbol,
    minute_chunk_paths,
    minute_receipt_valid,
    merge_retried_source_gap_chunk,
    unresolved_source_gap_dates_after_retry,
    provisional_publication_tail_dates,
    query_minute_chunk,
    restore_extended_tail_from_archived_manifest,
    select_universe,
    stock_contract_map,
    validate_minute_kbars,
)
from scripts.audit_shioaji_tw_minute_dataset import audit_frame
from scripts.build_shioaji_tw_minute_dataset import (
    MODEL_FEATURE_COLUMNS,
    _available_collection_symbols,
    _reject_subset_overwrite,
    _feature_statistics,
    _quarantine_stale_partitions,
    _split_official_session_rows,
    _validate_collection_gate,
    build_research_frame,
)


def test_provisional_tail_uses_official_calendar_not_weekday_guess(tmp_path: Path) -> None:
    reference = tmp_path / "2330.parquet"
    pl.DataFrame({
        "date": [date(2026, 9, 4)],
        "Trading_Volume": [1_000.0],
    }).write_parquet(reference)
    result = provisional_publication_tail_dates(
        reference,
        start=date(2026, 9, 4),
        end=date(2026, 9, 8),
        expected_dates={date(2026, 9, 4)},
        official_dates={date(2026, 9, 4), date(2026, 9, 7)},
    )
    assert result == {date(2026, 9, 7), date(2026, 9, 8)}


def test_research_builder_excludes_non_session_provider_rows() -> None:
    frame = pl.DataFrame(
        {
            "date": [date(2026, 9, 4), date(2026, 9, 5)],
            "symbol": ["2330", "2330"],
        }
    )
    accepted, rejected = _split_official_session_rows(
        frame, {date(2026, 9, 4)}
    )
    assert accepted["date"].to_list() == [date(2026, 9, 4)]
    assert rejected == {"2026-09-05": 1}


def test_research_builder_quarantines_stale_output_without_deleting(tmp_path: Path) -> None:
    output = tmp_path / "research_dataset"
    kept = output / "trade_date=2026-09-04"
    stale = output / "trade_date=2026-09-05"
    kept.mkdir(parents=True)
    stale.mkdir()
    (stale / "data.parquet").write_bytes(b"recoverable")

    moved = _quarantine_stale_partitions(output, {"2026-09-04"})

    assert kept.is_dir()
    assert not stale.exists()
    destination = Path(moved[0]["quarantine"])
    assert (destination / "data.parquet").read_bytes() == b"recoverable"
from stockagent.research.tw_minute_kbars import (
    MinuteKbarBacktestConfig,
    add_minute_strategy_scores,
    chronological_date_splits,
    run_minute_rebalance_backtest,
    run_minute_round_trip_backtest,
)


TRADE_DATE = date(2026, 7, 24)


def test_retry_keeps_old_broker_gap_when_current_public_daily_row_disappears() -> None:
    old_gap = date(2020, 10, 28)
    current_expected = date(2020, 10, 29)
    assert unresolved_source_gap_dates_after_retry(
        current_expected_dates={current_expected},
        retained_gap_dates={old_gap},
        returned_dates={current_expected},
    ) == ["2020-10-28"]
    assert unresolved_source_gap_dates_after_retry(
        current_expected_dates={current_expected},
        retained_gap_dates={old_gap},
        returned_dates={old_gap, current_expected},
    ) == []


def _acquire_rate_limit_slots(
    limiter: SharedRequestRateLimiter,
    count: int,
    output: object,
) -> None:
    for _ in range(count):
        output.put(limiter.acquire())


def _raw_minute_frame(
    *,
    next_open: float = 106.0,
    next_close: float = 107.0,
    session_close: float = 110.0,
    source_volume_multiplier: float = 10.0,
) -> pl.DataFrame:
    timestamps = [
        datetime(2026, 7, 24, 9, 1) + timedelta(minutes=index) for index in range(7)
    ]
    timestamps.append(datetime(2026, 7, 24, 13, 30))
    closes = [101.0, 102.0, 103.0, 104.0, 105.0, 106.0, next_close, session_close]
    opens = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, next_open, 109.0]
    highs = [max(a, b) + 1.0 for a, b in zip(opens, closes, strict=True)]
    lows = [min(a, b) - 1.0 for a, b in zip(opens, closes, strict=True)]
    return pl.DataFrame(
        {
            "ts": timestamps,
            "date": [TRADE_DATE] * len(timestamps),
            "symbol": ["2330"] * len(timestamps),
            "market": ["twse"] * len(timestamps),
            "Open": opens,
            "High": highs,
            "Low": lows,
            "Close": closes,
            "Volume": [100.0] * len(timestamps),
            # The default multiplier makes 100 raw units represent 1,000 shares;
            # keep notional inside every bar's causal low/high range.
            "Amount": [
                100.0 * source_volume_multiplier * ((high + low) / 2.0)
                for high, low in zip(highs, lows, strict=True)
            ],
            "contract_unit": [1_000.0] * len(timestamps),
        }
    )


def _research_frame(**kwargs: float) -> pl.DataFrame:
    return build_research_frame(_raw_minute_frame(**kwargs).lazy()).collect()


def test_minute_research_scores_exclude_verified_prelisting_market() -> None:
    source = _research_frame().with_columns(
        pl.lit("6716").alias("symbol"),
        pl.lit(date(2020, 3, 13)).alias("date"),
    )
    assert add_minute_strategy_scores(source).is_empty()
    listed = source.with_columns(pl.lit(date(2020, 3, 27)).alias("date"))
    assert add_minute_strategy_scores(listed).height == listed.height


def test_feature_statistics_square_integer_features_in_float64() -> None:
    frame = pl.DataFrame(
        {
            **{
                name: (
                    pl.Series([6, 132, 265], dtype=pl.Int16)
                    if name == "minutes_from_open"
                    else pl.Series([1.0, 2.0, 3.0], dtype=pl.Float64)
                )
                for name in MODEL_FEATURE_COLUMNS
            },
            "feature_valid": [True, True, True],
        }
    )

    statistics = _feature_statistics(frame)

    assert statistics["feature_sums"]["minutes_from_open"] == 403.0
    assert statistics["feature_sum_squares"]["minutes_from_open"] == 87_685.0


def _backtest_rows(
    *,
    first_valid: bool,
    first_close: float | None,
    include_second: bool = True,
) -> pl.DataFrame:
    rows = [
        {
            "date": TRADE_DATE,
            "ts": datetime(2026, 7, 24, 9, 30),
            "symbol": "2330",
            "minutes_from_open": 30,
            "feature_valid": True,
            "session_exit_valid": first_valid,
            "execution_open_next_1m": 100.0 if first_valid else None,
            "session_close": first_close,
            "future_volume_shares_next_1m": 10_000_000.0 if first_valid else None,
            "score_blend": 1.0,
        }
    ]
    if include_second:
        rows.append(
            {
                "date": TRADE_DATE,
                "ts": datetime(2026, 7, 24, 9, 30),
                "symbol": "0050",
                "minutes_from_open": 30,
                "feature_valid": True,
                "session_exit_valid": True,
                "execution_open_next_1m": 100.0,
                "session_close": 110.0,
                "future_volume_shares_next_1m": 10_000_000.0,
                "score_blend": 0.5,
            }
        )
    return pl.DataFrame(rows)


def _stateful_rows(
    *,
    invalid_top_rank: bool = False,
    second_2330_score: float = -0.5,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for minute, scores in (
        (6, {"2330": 1.0, "0050": 0.5}),
        (7, {"2330": second_2330_score, "0050": 1.0}),
    ):
        for symbol in ("2330", "0050"):
            valid = not (invalid_top_rank and minute == 6 and symbol == "2330")
            rows.append(
                {
                    "date": TRADE_DATE,
                    "ts": datetime(2026, 7, 24, 9, minute),
                    "symbol": symbol,
                    "minutes_from_open": minute,
                    "feature_valid": True,
                    "label_valid_1m": valid,
                    "execution_open_next_1m": 100.0 if valid else None,
                    "exit_close_next_1m": 100.0 if valid else None,
                    "future_volume_shares_next_1m": (10_000_000.0 if valid else None),
                    "session_close": 100.0,
                    "score_blend": scores[symbol],
                }
            )
    for symbol in ("2330", "0050"):
        rows.append(
            {
                "date": TRADE_DATE,
                "ts": datetime(2026, 7, 24, 13, 30),
                "symbol": symbol,
                "minutes_from_open": 270,
                "feature_valid": False,
                "label_valid_1m": False,
                "execution_open_next_1m": None,
                "exit_close_next_1m": None,
                "future_volume_shares_next_1m": None,
                "session_close": 100.0,
                "score_blend": None,
            }
        )
    return pl.DataFrame(rows)


def test_minute_chunk_paths_are_separate_from_daily_storage() -> None:
    data_path, receipt_path = minute_chunk_paths(
        Path("data_tw_minute/shioaji_1m"),
        "2330",
        date(2026, 7, 1),
        date(2026, 7, 24),
    )

    assert data_path == Path(
        "data_tw_minute/shioaji_1m/minute_chunks/2330/2026-07-01_2026-07-24.parquet"
    )
    assert receipt_path.name.endswith(".receipt.json")
    assert "daily_chunks" not in str(data_path)


def test_incomplete_extension_does_not_replace_terminal_catalog(
    tmp_path: Path,
) -> None:
    canonical_summary = tmp_path / "download_summary.json"
    canonical_report = tmp_path / "download_report.csv"
    canonical_summary.write_text('{"terminal": true}\n', encoding="utf-8")
    canonical_report.write_text("symbol,status\n2330,complete\n", encoding="utf-8")
    row = UniverseRow(
        symbol="2330",
        name="台積電",
        market="twse",
        security_type="stock",
        base_path=tmp_path / "2330.parquet",
    )
    result = SymbolResult(
        symbol="2330",
        status="partial",
        chunks_total=2,
        chunks_complete=1,
        source_minute_rows=0,
        daily_rows=0,
        first_date=None,
        last_date=None,
        output_path="",
        message="traffic guard",
    )
    output = _write_run_summary(
        tmp_path,
        args=SimpleNamespace(
            start_date="2020-03-02",
            end_date="2026-08-14",
            chunk_days=29,
            simulation=True,
            workers=5,
            requests_per_second=10.0,
        ),
        selected=[row],
        results=[result],
        traffic=(1_900_000_000, 2_147_483_648),
        stopped_for_traffic=True,
        stopped_for_market_hours=False,
        counters={
            "processed_chunks": 0,
            "queried_chunks": 0,
            "skipped_empty_chunks": 0,
        },
        rate={"total_requests": 0, "overall_rps": 0.0},
        fatal_error="",
    )
    assert output == tmp_path / "latest_run_summary.json"
    assert json.loads(canonical_summary.read_text())["terminal"] is True
    assert "2330,complete" in canonical_report.read_text()
    latest = json.loads(output.read_text())
    assert latest["published_terminal_catalog"] is False
    assert latest["partial_symbols"] == 1


def test_market_hours_preflight_stop_persists_zero_request_receipt(
    tmp_path: Path,
) -> None:
    row = UniverseRow(
        symbol="2330",
        name="台積電",
        market="twse",
        security_type="stock",
        base_path=tmp_path / "2330.parquet",
    )
    args = SimpleNamespace(
        start_date="2026-09-02",
        end_date="2026-09-02",
        chunk_days=1,
        simulation=True,
        workers=1,
        requests_per_second=5.0,
    )

    summary_path = _write_market_hours_stop(
        tmp_path,
        args=args,
        selected=[row],
        message="live-priority window",
    )

    summary = json.loads(summary_path.read_text())
    progress = json.loads((tmp_path / "progress.json").read_text())
    assert summary["stopped_for_market_hours"] is True
    assert summary["api_requests_started_this_run"] == 0
    assert summary["published_terminal_catalog"] is False
    assert progress["state"] == "stopped_for_market_hours"
    assert progress["api_requests_started_this_run"] == 0
    assert progress["stop_reason"] == "live-priority window"


def test_scheduled_yield_preserves_terminal_catalog_and_records_deadline(
    tmp_path: Path,
) -> None:
    canonical_summary = tmp_path / "download_summary.json"
    canonical_summary.write_text('{"end_date":"2026-09-21"}\n', encoding="utf-8")
    row = UniverseRow(
        symbol="2330", name="台積電", market="twse", security_type="stock",
        base_path=tmp_path / "2330.parquet",
    )
    from downloader.download_shioaji_tw_minute_kbars import _write_preflight_stop

    summary_path = _write_preflight_stop(
        tmp_path,
        args=SimpleNamespace(
            start_date="2020-03-02", end_date="2026-09-22", chunk_days=29,
            simulation=True, workers=3, requests_per_second=5.0,
            stop_at="2026-09-22T14:45:00+08:00",
        ),
        selected=[row],
        message="scheduled deadline reached",
        state="stopped_for_schedule",
    )
    summary = json.loads(summary_path.read_text())
    progress = json.loads((tmp_path / "progress.json").read_text())
    assert summary_path.name == "latest_run_summary.json"
    assert summary["stopped_for_schedule"] is True
    assert summary["scheduled_stop_at"] == "2026-09-22T14:45:00+08:00"
    assert summary["published_terminal_catalog"] is False
    assert summary["api_requests_started_this_run"] == 0
    assert progress["state"] == "stopped_for_schedule"
    assert json.loads(canonical_summary.read_text())["end_date"] == "2026-09-21"


def test_research_gate_allows_audited_source_gaps_but_rejects_failures(
    tmp_path: Path,
) -> None:
    path = tmp_path / "download_summary.json"
    base = {
        "schema_version": 1,
        "source": "shioaji_kbars_1m",
        "storage_frequency": "minute",
        "simulation": True,
        "fatal_error": None,
        "selected_symbols": 2,
        "reported_symbols": 2,
        "complete_symbols": 0,
        "complete_with_source_gap_symbols": 1,
        "contract_unavailable_symbols": 1,
        "failed_symbols": 0,
        "partial_symbols": 0,
        "resumable_collection_complete": True,
    }
    path.write_text(json.dumps(base), encoding="utf-8")

    result = _validate_collection_gate(
        path, selected_symbols=["0050"], subset_requested=False
    )
    assert result["complete_with_source_gap_symbols"] == 1

    base.update(
        complete_with_source_gap_symbols=0,
        failed_symbols=1,
        resumable_collection_complete=False,
    )
    path.write_text(json.dumps(base), encoding="utf-8")
    with pytest.raises(RuntimeError, match="not research-ready"):
        _validate_collection_gate(path, selected_symbols=[], subset_requested=False)


def test_research_build_uses_terminal_report_instead_of_stale_manifests(
    tmp_path: Path,
) -> None:
    report = tmp_path / "download_report.csv"
    report.write_text(
        "symbol,status\n0050,complete\n2330,complete_with_source_gaps\n"
        "4130,contract_unavailable\n",
        encoding="utf-8",
    )
    summary_path = tmp_path / "download_summary.json"
    payload = {
        "report_path": str(report),
        "selected_symbols": 3,
        "complete_symbols": 1,
        "complete_with_source_gap_symbols": 1,
        "contract_unavailable_symbols": 1,
    }

    assert _available_collection_symbols(summary_path, payload) == {"0050", "2330"}


def test_subset_build_cannot_overwrite_existing_full_market_partition(
    tmp_path: Path,
) -> None:
    partition = tmp_path / "trade_date=2026-02-25"
    partition.mkdir()
    (partition / "data.parquet").write_bytes(b"existing")
    with pytest.raises(RuntimeError, match="fresh isolated"):
        _reject_subset_overwrite(tmp_path, {"0056"})
    _reject_subset_overwrite(tmp_path, set())


def test_official_market_data_ceiling_is_fifty_per_ten_seconds() -> None:
    assert SHIOAJI_QUOTE_LIMIT_WINDOW_SECONDS == 10.0
    assert DEFAULT_REQUESTS_PER_SECOND == 5.0


def test_minute_kbar_validator_rejects_broken_price_and_notional() -> None:
    frame = pl.DataFrame({
        "symbol": ["0056"], "date": [date(2026, 2, 25)],
        "ts": [datetime(2026, 2, 25, 9, 1)],
        "Open": [10.0], "High": [9.0], "Low": [9.0], "Close": [10.0],
        "Volume": [1.0], "Amount": [0.0], "contract_unit": [1000.0],
    })
    with pytest.raises(RuntimeError, match="invalid_value_rows"):
        validate_minute_kbars(
            frame, symbol="0056", start=date(2026, 2, 25),
            end=date(2026, 2, 25),
        )


def test_gap_retry_merge_preserves_archived_bars_and_rejects_revisions() -> None:
    columns = {
        "symbol": ["0056"], "date": [date(2026, 2, 25)],
        "ts": [datetime(2026, 2, 25, 9, 3)],
        "Open": [10.0], "High": [10.0], "Low": [10.0], "Close": [10.0],
        "Volume": [1.0], "Amount": [10000.0], "market": ["twse"],
        "contract_unit": [1000.0],
    }
    archived = pl.DataFrame(columns)
    recovered = pl.DataFrame({
        **columns, "ts": [datetime(2026, 2, 25, 9, 1)],
    })
    merged = merge_retried_source_gap_chunk(archived, recovered)
    assert merged.height == 2
    assert merged["ts"].to_list() == [
        datetime(2026, 2, 25, 9, 1), datetime(2026, 2, 25, 9, 3),
    ]
    altered = archived.with_columns(pl.lit(11.0).alias("Close"))
    with pytest.raises(RuntimeError, match="conflicts"):
        merge_retried_source_gap_chunk(archived, altered)


def test_account_wide_rate_limiter_is_shared_across_processes() -> None:
    context = mp.get_context("spawn")
    output = context.Queue()
    # Scale the configured 50/10s boundary down to a fast 50/0.05s test while
    # preserving its 50-request sliding-window shape.
    limiter = SharedRequestRateLimiter(
        context,
        requests_per_second=1_000.0,
        max_requests=50,
        window_seconds=0.05,
    )
    processes = [
        context.Process(
            target=_acquire_rate_limit_slots,
            args=(limiter, 25, output),
        )
        for _ in range(4)
    ]

    for process in processes:
        process.start()
    starts = sorted(output.get(timeout=10.0) for _ in range(100))
    for process in processes:
        process.join(timeout=10.0)

    assert all(process.exitcode == 0 for process in processes)
    assert limiter.snapshot()["total_requests"] == 100
    assert all(
        later - earlier >= 0.05 - 1e-4
        for earlier, later in zip(starts[:50], starts[50:], strict=True)
    )


def test_full_market_universe_requires_explicit_scope() -> None:
    universe = [
        UniverseRow("0050", "元大台灣50", "twse", "etf", Path("0050.parquet")),
        UniverseRow("2330", "台積電", "twse", "stock", Path("2330.parquet")),
    ]

    selected = select_universe(
        universe,
        symbols="",
        universe_csv=None,
        all_symbols=True,
        max_symbols=0,
    )

    assert [row.symbol for row in selected] == ["0050", "2330"]
    with pytest.raises(ValueError, match="select exactly one"):
        select_universe(
            universe,
            symbols="",
            universe_csv=None,
            all_symbols=False,
            max_symbols=0,
        )


def test_contract_lookup_is_restricted_to_taiwan_stocks() -> None:
    contract = SimpleNamespace(
        code="2330",
        security_type=SimpleNamespace(value="STK"),
        exchange=SimpleNamespace(value="TSE"),
    )

    class FakeContracts:
        def __init__(self) -> None:
            self.list_calls: list[tuple[str, str]] = []

        def list(self, kind: str, *, region: str) -> list[SimpleNamespace]:
            self.list_calls.append((kind, region))
            return [contract]

        def info(self, value: SimpleNamespace) -> SimpleNamespace:
            assert value is contract
            return SimpleNamespace(unit=1000)

    api = SimpleNamespace(contracts=FakeContracts())
    contracts_by_code = stock_contract_map(api)
    row = UniverseRow("2330", "台積電", "twse", "stock", Path("2330_features.parquet"))

    resolved, unit, message = contract_for_stock_symbol(api, row, contracts_by_code)

    assert api.contracts.list_calls == [("STK", "TW")]
    assert resolved is contract
    assert unit == pytest.approx(1000.0)
    assert message == ""


def test_historical_identity_is_exact_and_not_live_eligibility(tmp_path: Path) -> None:
    from downloader.download_shioaji_tw_minute_kbars import historical_stock_identity, historical_stock_units
    source = tmp_path / "00883B_features.parquet"
    source.touch()
    row = UniverseRow("00883B", "中信ESG投資級債", "tpex", "etf", source)
    units = historical_stock_units(["00883B=1000"], [row])
    contract, unit, message = historical_stock_identity(row, units[row.symbol])
    assert (contract.code, contract.exchange, contract.security_type) == ("00883B", "OTC", "STK")
    assert unit == 1000 and message == "explicit_public_historical_identity_read_only_v1"
    assert contract_for_stock_symbol(SimpleNamespace(), row, {}) == (None, 0.0, "stock_contract_not_found")


@pytest.mark.parametrize("value", ["2330=1000", "00883B=nan", "00883B=0", "00883B=1.5", "00883B"])
def test_historical_unit_rejects_unselected_or_invalid_values(value: str) -> None:
    from downloader.download_shioaji_tw_minute_kbars import historical_stock_units
    row = UniverseRow("00883B", "ETF", "tpex", "etf", Path("source"))
    with pytest.raises(ValueError):
        historical_stock_units([value], [row])


def test_historical_unit_is_independently_checked_against_amount() -> None:
    from downloader.download_shioaji_tw_minute_kbars import validate_historical_unit_amount
    frame = pl.DataFrame({"Volume": [2.0], "Amount": [63000.0], "Low": [31.0], "High": [32.0]})
    validate_historical_unit_amount(frame, 1000)
    with pytest.raises(ValueError, match="unit/amount inconsistent"):
        validate_historical_unit_amount(frame, 100)
    with pytest.raises(ValueError, match="unit/amount inconsistent"):
        validate_historical_unit_amount(frame.with_columns(pl.lit(None).alias("Amount")), 1000)


@pytest.mark.parametrize("market,security_type,source_exists", [
    ("unknown", "stock", True), ("twse", "warrant", True), ("tpex", "etf", False),
])
def test_historical_identity_rejects_missing_public_reference(
    tmp_path: Path, market: str, security_type: str, source_exists: bool,
) -> None:
    from downloader.download_shioaji_tw_minute_kbars import historical_stock_identity
    path = tmp_path / "source.parquet"
    if source_exists:
        path.touch()
    with pytest.raises(ValueError, match="unverified historical stock identity"):
        historical_stock_identity(UniverseRow("00883B", "ETF", market, security_type, path), 1000)


def test_missing_kbars_tick_fallback_keeps_raw_evidence_and_time_units(tmp_path, monkeypatch):
    from contextlib import nullcontext
    import numpy as np
    from downloader import download_shioaji_tw_minute_kbars as collector
    day = date(2026, 2, 25)
    base = tmp_path / "3454.parquet"
    pl.DataFrame({"date": [day], "Trading_Volume": [3000.]}).write_parquet(base)
    row = UniverseRow("3454", "晶睿", "twse", "stock", base)
    raw = {"ts": [int(np.datetime64(f"{day}T09:00:59", "ns").astype(np.int64)),
                  int(np.datetime64(f"{day}T09:00:10", "ns").astype(np.int64))],
           "close": [102., 100.], "volume": [2, 1]}
    ticks = SimpleNamespace(**raw, dict=lambda: raw)
    class API:
        def kbars(self, **kwargs):
            raise RuntimeError("Data not found")
        def ticks(self, **kwargs):
            assert kwargs["date"] == str(day)
            return ticks
    monkeypatch.setattr(collector, "_taiwan_market_hours_now", lambda: False)
    traffic_checks = []
    monkeypatch.setattr(collector, "_check_traffic_budget", lambda api, *, max_fraction: traffic_checks.append(max_fraction))
    monkeypatch.setattr(collector, "shioaji_query", lambda *args, **kwargs: nullcontext(lambda _: None))
    slots = []
    frame, audit = collector.query_minute_chunk(
        API(), object(), row, contract_unit=1000, start=day, end=day, timeout_ms=10,
        retries=0, retry_backoff=0, expected_dates={day}, request_started=lambda: slots.append(1),
        tick_fallback_root=tmp_path / "raw", max_traffic_fraction=.25)
    assert frame["ts"].to_list() == [datetime(2026, 2, 25, 9, 1)]
    assert frame["Open"].item() == 100 and frame["Close"].item() == 102
    assert frame["Volume"].item() == 3 and frame["Amount"].item() == 304000
    assert audit["source_gap_dates"] == [] and audit["tick_fallback_queries"] == 1
    assert len(slots) == 2 and traffic_checks == [.25]
    path = tmp_path / "minute.parquet"
    output = collector._write_minute_parquet(frame, path)
    proof = {"schema_version": collector.RECEIPT_SCHEMA_VERSION, "source": collector.SOURCE_NAME,
             "storage_frequency": "minute", "symbol": "3454", "start_date": str(day), "end_date": str(day),
             "status": "ok", "rows": 1, "output_receipt": output, **audit}
    receipt = tmp_path / "minute.receipt.json"
    receipt.write_text(json.dumps(proof))
    assert collector.minute_receipt_valid(receipt, symbol="3454", start=day, end=day)
    Path(audit["raw_tick_sources"][0]["path"]).write_bytes(b"corrupt")
    assert not collector.minute_receipt_valid(receipt, symbol="3454", start=day, end=day)


def test_empty_kbars_recovers_observed_ticks_without_duplicate_day_query(tmp_path, monkeypatch):
    from contextlib import nullcontext
    import numpy as np
    from downloader import download_shioaji_tw_minute_kbars as collector

    day = date(2026, 2, 25)
    base = tmp_path / "3454.parquet"
    pl.DataFrame({"date": [day], "Trading_Volume": [3000.0]}).write_parquet(base)
    row = UniverseRow("3454", "晶睿", "twse", "stock", base)
    empty = {name: [] for name in ("ts", "Open", "High", "Low", "Close", "Volume", "Amount")}
    raw = {
        "ts": [int(np.datetime64(f"{day}T09:00:10", "ns").astype(np.int64))],
        "close": [100.0], "volume": [1],
    }

    class API:
        kbar_calls = 0

        def kbars(self, **kwargs):
            self.kbar_calls += 1
            return empty

        def ticks(self, **kwargs):
            return SimpleNamespace(**raw, dict=lambda: raw)

    monkeypatch.setattr(collector, "_taiwan_market_hours_now", lambda: False)
    monkeypatch.setattr(collector, "_check_traffic_budget", lambda *_a, **_k: None)
    monkeypatch.setattr(collector, "shioaji_query", lambda *_a, **_k: nullcontext(lambda _: None))
    api = API()
    slots = []
    frame, audit = collector.query_minute_chunk(
        api, object(), row, contract_unit=1000, start=day, end=day,
        timeout_ms=10, retries=0, retry_backoff=0, expected_dates={day},
        request_started=lambda: slots.append(1), tick_fallback_root=tmp_path / "raw",
    )
    assert api.kbar_calls == 1
    assert len(slots) == 2
    assert frame["ts"].to_list() == [datetime(2026, 2, 25, 9, 1)]
    assert audit["source_gap_dates"] == []
    assert audit["tick_fallback_queries"] == 1
    assert audit["underlying_data_method"] == "observed_ticks_aggregated_to_right_labelled_1m"


def test_source_gap_retry_queries_only_absent_days(monkeypatch):
    from downloader import download_shioaji_tw_minute_kbars as collector

    days = {date(2020, 3, 12), date(2020, 3, 18)}
    calls = []

    def fake_query(*_args, start, end, expected_dates, **_kwargs):
        calls.append((start, end, expected_dates))
        return pl.DataFrame(), {"source_gap_dates": [str(start)]}

    monkeypatch.setattr(collector, "query_minute_chunk", fake_query)
    row = UniverseRow("3454", "晶睿", "twse", "stock", Path("unused.parquet"))
    frame, audit = collector.query_source_gap_dates(
        object(), object(), row, contract_unit=1000, missing_dates=days,
        timeout_ms=10, retries=0, retry_backoff=0, request_started=None,
        tick_fallback_root=None, max_traffic_fraction=0.90,
    )
    assert frame.is_empty()
    assert calls == [(day, day, {day}) for day in sorted(days)]
    assert audit["source_gap_dates"] == [str(day) for day in sorted(days)]


def test_retried_raw_ticks_keep_prior_evidence(tmp_path):
    from downloader import download_shioaji_tw_minute_kbars as collector

    first = collector._write_immutable_raw_ticks(
        pl.DataFrame({"price": [100.0]}), tmp_path, symbol="3454",
        day=date(2026, 2, 25),
    )
    revised = collector._write_immutable_raw_ticks(
        pl.DataFrame({"price": [101.0]}), tmp_path, symbol="3454",
        day=date(2026, 2, 25),
    )
    assert first["path"] != revised["path"]
    assert Path(first["path"]).is_file()
    assert Path(revised["path"]).is_file()
    assert collector._sha256(Path(first["path"])) == first["sha256"]
    assert collector._sha256(Path(revised["path"])) == revised["sha256"]


def test_sealed_manifest_is_a_fast_restart_checkpoint(tmp_path: Path) -> None:
    row = UniverseRow("2330", "台積電", "twse", "stock", Path("2330_features.parquet"))
    chunks = [(date(2026, 7, 1), date(2026, 7, 24))]
    data_path, receipt_path = minute_chunk_paths(tmp_path, row.symbol, *chunks[0])
    data_path.parent.mkdir(parents=True)
    data_path.write_bytes(b"sealed parquet placeholder")
    receipt_path.write_text("{}\n", encoding="utf-8")
    manifest_path = tmp_path / "symbols" / "2330.manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source": "shioaji_kbars_1m",
                "storage_frequency": "minute",
                "simulation": True,
                "symbol": "2330",
                "requested_start": "2026-07-01",
                "requested_end": "2026-07-24",
                "minute_rows": 100,
                "sessions": 1,
                "first_date": "2026-07-24",
                "last_date": "2026-07-24",
                "chunks": [
                    {
                        "start_date": "2026-07-01",
                        "end_date": "2026-07-24",
                        "status": "ok",
                        "rows": 100,
                        "data_path": str(data_path),
                        "data_sha256": "already-verified",
                        "receipt_path": str(receipt_path),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = completed_symbol_manifest_result(
        tmp_path,
        row,
        chunks,
        requested_start=date(2026, 7, 1),
        requested_end=date(2026, 7, 24),
        simulation=True,
    )

    assert result is not None
    assert result.status == "complete"
    assert result.source_minute_rows == 100
    assert result.message == "resumed_from_sealed_manifest"
    assert (
        completed_symbol_manifest_result(
            tmp_path,
            row,
            chunks,
            requested_start=date(2026, 7, 1),
            requested_end=date(2026, 7, 25),
            simulation=True,
        )
        is None
    )


def test_sealed_manifest_reopens_when_official_session_advances(tmp_path: Path) -> None:
    row = UniverseRow("2330", "台積電", "twse", "stock", Path("2330_features.parquet"))
    chunks = [(date(2026, 7, 1), date(2026, 7, 25))]
    data_path, receipt_path = minute_chunk_paths(tmp_path, row.symbol, *chunks[0])
    data_path.parent.mkdir(parents=True)
    data_path.write_bytes(b"sealed parquet placeholder")
    receipt_path.write_text("{}\n", encoding="utf-8")
    manifest_path = tmp_path / "symbols" / "2330.manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source": "shioaji_kbars_1m",
                "storage_frequency": "minute",
                "simulation": True,
                "symbol": "2330",
                "requested_start": "2026-07-01",
                "requested_end": "2026-07-25",
                "minute_rows": 100,
                "sessions": 1,
                "first_date": "2026-07-24",
                "last_date": "2026-07-24",
                "terminal_coverage_dates": ["2026-07-24"],
                "chunks": [
                    {
                        "start_date": "2026-07-01",
                        "end_date": "2026-07-25",
                        "status": "ok",
                        "rows": 100,
                        "data_path": str(data_path),
                        "data_sha256": "already-verified",
                        "receipt_path": str(receipt_path),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    assert (
        completed_symbol_manifest_result(
            tmp_path,
            row,
            chunks,
            requested_start=date(2026, 7, 1),
            requested_end=date(2026, 7, 25),
            simulation=True,
            expected_dates={date(2026, 7, 24), date(2026, 7, 25)},
        )
        is None
    )


def test_minute_receipt_must_cover_newly_official_dates(tmp_path: Path) -> None:
    receipt_path = tmp_path / "2330.receipt.json"
    receipt_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source": "shioaji_kbars_1m",
                "storage_frequency": "minute",
                "simulation": True,
                "symbol": "2330",
                "start_date": "2026-07-24",
                "end_date": "2026-07-25",
                "status": "empty",
                "rows": 0,
                "returned_dates": [],
                "source_gap_dates": [],
            }
        ),
        encoding="utf-8",
    )

    assert minute_receipt_valid(
        receipt_path,
        symbol="2330",
        start=date(2026, 7, 24),
        end=date(2026, 7, 25),
        simulation=True,
    )
    assert not minute_receipt_valid(
        receipt_path,
        symbol="2330",
        start=date(2026, 7, 24),
        end=date(2026, 7, 25),
        simulation=True,
        required_dates={date(2026, 7, 25)},
    )


def test_minute_validation_rejects_out_of_session_timestamp() -> None:
    valid = _raw_minute_frame()
    audit = validate_minute_kbars(
        valid,
        symbol="2330",
        start=TRADE_DATE,
        end=TRADE_DATE,
    )
    assert audit["rows"] == 8
    assert audit["sessions"] == 1

    invalid = valid.with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.lit(datetime(2026, 7, 24, 9, 0)))
        .otherwise(pl.col("ts"))
        .alias("ts")
    )
    with pytest.raises(RuntimeError, match="out_of_session_rows"):
        validate_minute_kbars(
            invalid,
            symbol="2330",
            start=TRADE_DATE,
            end=TRADE_DATE,
        )


def test_query_drops_only_fully_zero_shioaji_placeholders() -> None:
    class FakeAPI:
        def kbars(self, **_: object) -> dict[str, list[object]]:
            return {
                "ts": [
                    datetime(2026, 7, 24, 9, 1),
                    datetime(2026, 7, 24, 9, 2),
                ],
                "Open": [100.0, 0.0],
                "High": [101.0, 0.0],
                "Low": [99.0, 0.0],
                "Close": [100.5, 0.0],
                "Volume": [10, 0],
                "Amount": [1_000_000.0, 0.0],
            }

    row = UniverseRow(
        "0051", "元大中型100", "twse", "etf", Path("0051_features.parquet")
    )
    frame, query_audit = query_minute_chunk(
        FakeAPI(),
        object(),
        row,
        contract_unit=1_000.0,
        start=TRADE_DATE,
        end=TRADE_DATE,
        timeout_ms=30_000,
        retries=0,
        retry_backoff=0.0,
        expected_dates={TRADE_DATE},
    )

    assert query_audit["zero_placeholder_rows_dropped"] == 1
    assert query_audit["negative_correction_rows_dropped"] == 0
    assert query_audit["out_of_session_rows_dropped"] == 0
    assert query_audit["outside_reference_date_rows_dropped"] == 0
    assert query_audit["source_gap_dates"] == []
    assert frame.height == 1
    assert frame["ts"][0] == datetime(2026, 7, 24, 9, 1)

    class PartiallyInvalidAPI:
        def kbars(self, **_: object) -> dict[str, list[object]]:
            return {
                "ts": [datetime(2026, 7, 24, 9, 1)],
                "Open": [0.0],
                "High": [101.0],
                "Low": [99.0],
                "Close": [100.5],
                "Volume": [10],
                "Amount": [1_000_000.0],
            }

    with pytest.raises(ValueError, match="invalid Kbar"):
        query_minute_chunk(
            PartiallyInvalidAPI(),
            object(),
            row,
            contract_unit=1_000.0,
            start=TRADE_DATE,
            end=TRADE_DATE,
            timeout_ms=30_000,
            retries=0,
            retry_backoff=0.0,
            expected_dates={TRADE_DATE},
        )


def test_query_audits_non_executable_shioaji_rows() -> None:
    class FakeAPI:
        def kbars(self, **_: object) -> dict[str, list[object]]:
            return {
                "ts": [
                    datetime(2026, 7, 24, 9, 1),
                    datetime(2026, 7, 24, 9, 2),
                    datetime(2026, 7, 24, 15, 0),
                    datetime(2026, 7, 24, 15, 0),
                ],
                "Open": [100.0, 101.0, 102.0, 102.0],
                "High": [101.0, 101.0, 102.0, 102.0],
                "Low": [99.0, 101.0, 102.0, 102.0],
                "Close": [100.5, 101.0, 102.0, 102.0],
                "Volume": [10, -2, 1, 1],
                "Amount": [1_000_000.0, -202_000.0, 102_000.0, 102_000.0],
            }

    row = UniverseRow(
        "0051", "元大中型100", "twse", "etf", Path("0051_features.parquet")
    )
    frame, query_audit = query_minute_chunk(
        FakeAPI(),
        object(),
        row,
        contract_unit=1_000.0,
        start=TRADE_DATE,
        end=TRADE_DATE,
        timeout_ms=30_000,
        retries=0,
        retry_backoff=0.0,
        expected_dates={TRADE_DATE},
    )

    assert frame["ts"].to_list() == [datetime(2026, 7, 24, 9, 1)]
    assert query_audit["negative_correction_rows_dropped"] == 1
    assert query_audit["out_of_session_rows_dropped"] == 2
    assert query_audit["outside_reference_date_rows_dropped"] == 0
    assert query_audit["source_gap_dates"] == []


def test_query_drops_one_sided_corrections_and_pre_lifecycle_rows() -> None:
    pre_lifecycle = TRADE_DATE - timedelta(days=1)

    class FakeAPI:
        def kbars(self, **_: object) -> dict[str, list[object]]:
            return {
                "ts": [
                    datetime.combine(pre_lifecycle, datetime.min.time()).replace(
                        hour=9, minute=1
                    ),
                    datetime.combine(pre_lifecycle, datetime.min.time()).replace(
                        hour=9, minute=1
                    ),
                    datetime(2026, 7, 24, 9, 1),
                    datetime(2026, 7, 24, 9, 2),
                    datetime(2026, 7, 24, 9, 3),
                ],
                "Open": [18.54, 18.10, 100.0, 0.0, 101.0],
                "High": [18.54, 18.10, 100.0, 0.0, 101.0],
                "Low": [18.54, 18.10, 100.0, 0.0, 101.0],
                "Close": [18.54, 18.10, 100.0, 0.0, 101.0],
                "Volume": [2_000, 1_000, 0, -107, 10],
                "Amount": [37_080_000.0, 18_100_000.0, -10.0, 0.0, 1_010_000.0],
            }

    row = UniverseRow("3597", "映興", "tpex", "stock", Path("3597_features.parquet"))
    frame, query_audit = query_minute_chunk(
        FakeAPI(),
        object(),
        row,
        contract_unit=1_000.0,
        start=pre_lifecycle,
        end=TRADE_DATE,
        timeout_ms=30_000,
        retries=0,
        retry_backoff=0.0,
        expected_dates={TRADE_DATE},
    )

    assert frame["ts"].to_list() == [datetime(2026, 7, 24, 9, 3)]
    assert query_audit["outside_reference_date_rows_dropped"] == 2
    assert query_audit["negative_correction_rows_dropped"] == 2
    assert query_audit["source_gap_dates"] == []


def test_query_retains_authorized_publication_tail_session() -> None:
    provisional_date = TRADE_DATE + timedelta(days=3)

    class FakeAPI:
        def kbars(self, **_: object) -> dict[str, list[object]]:
            return {
                "ts": [
                    datetime.combine(TRADE_DATE, datetime.min.time()).replace(
                        hour=9, minute=1
                    ),
                    datetime.combine(provisional_date, datetime.min.time()).replace(
                        hour=9, minute=1
                    ),
                ],
                "Open": [100.0, 101.0],
                "High": [100.0, 101.0],
                "Low": [100.0, 101.0],
                "Close": [100.0, 101.0],
                "Volume": [10, 20],
                "Amount": [1_000_000.0, 2_020_000.0],
            }

    row = UniverseRow(
        "0051", "元大中型100", "twse", "etf", Path("0051_features.parquet")
    )
    frame, query_audit = query_minute_chunk(
        FakeAPI(),
        object(),
        row,
        contract_unit=1_000.0,
        start=TRADE_DATE,
        end=provisional_date,
        timeout_ms=30_000,
        retries=0,
        retry_backoff=0.0,
        expected_dates={TRADE_DATE},
        provisional_dates={provisional_date},
    )

    assert frame["date"].to_list() == [TRADE_DATE, provisional_date]
    assert query_audit["outside_reference_date_rows_dropped"] == 0
    assert query_audit["source_gap_dates"] == []


def test_query_single_day_fallback_records_persistent_source_gap() -> None:
    missing_date = TRADE_DATE - timedelta(days=1)

    class FakeAPI:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []

        def kbars(self, **kwargs: object) -> dict[str, list[object]]:
            start = str(kwargs["start"])
            end = str(kwargs["end"])
            self.calls.append((start, end))
            if start == end == missing_date.isoformat():
                return {
                    "ts": [],
                    "Open": [],
                    "High": [],
                    "Low": [],
                    "Close": [],
                    "Volume": [],
                    "Amount": [],
                }
            return {
                "ts": [datetime(2026, 7, 24, 9, 1)],
                "Open": [100.0],
                "High": [101.0],
                "Low": [99.0],
                "Close": [100.5],
                "Volume": [10],
                "Amount": [1_000_000.0],
            }

    api = FakeAPI()
    request_starts: list[int] = []
    row = UniverseRow(
        "0051", "元大中型100", "twse", "etf", Path("0051_features.parquet")
    )
    frame, query_audit = query_minute_chunk(
        api,
        object(),
        row,
        contract_unit=1_000.0,
        start=missing_date,
        end=TRADE_DATE,
        timeout_ms=30_000,
        retries=0,
        retry_backoff=0.0,
        expected_dates={missing_date, TRADE_DATE},
        request_started=lambda: request_starts.append(len(request_starts) + 1),
    )

    assert frame.height == 1
    assert api.calls == [
        (missing_date.isoformat(), TRADE_DATE.isoformat()),
        (missing_date.isoformat(), missing_date.isoformat()),
    ]
    assert query_audit["single_day_fallback_queries"] == 1
    assert query_audit["source_gap_dates"] == [missing_date.isoformat()]
    assert request_starts == [1, 2]


@pytest.mark.parametrize("tick_backed", [False, True])
@pytest.mark.parametrize("extra_chunks", [0, 2])
def test_delisted_contract_extension_repacks_covered_archived_tail(
    tmp_path: Path, tick_backed: bool, extra_chunks: int,
) -> None:
    from datetime import timedelta
    base = tmp_path / "4130_features.parquet"
    pl.DataFrame({'date': [TRADE_DATE], 'Trading_Volume': [1000.]}).write_parquet(base)
    row = UniverseRow("4130", "健亞", "tpex", "stock", base)
    root = tmp_path / "minute"
    old_start = date(2026, 7, 9)
    old_end = date(2026, 7, 27)
    new_end = date(2026, 8, 4)
    old_path = root / "minute_chunks" / row.symbol / "archived.parquet"
    old_path.parent.mkdir(parents=True)
    frame = _raw_minute_frame().with_columns(pl.lit(row.symbol).alias("symbol"))
    frame.write_parquet(old_path)
    old_path.with_suffix(".receipt.json").write_text(json.dumps({
        "schema_version": 1, "source": "shioaji_kbars_1m", "storage_frequency": "minute",
        "simulation": True, "symbol": row.symbol, "start_date": str(old_start), "end_date": str(old_end),
        "status": "ok", "rows": frame.height,
        "output_receipt": {"path": str(old_path), "size": old_path.stat().st_size,
                           "sha256": hashlib.sha256(old_path.read_bytes()).hexdigest()},
    }))
    if tick_backed:
        raw = root / "raw_ticks.parquet"
        raw.write_bytes(b"retained tick test fixture")
        old_path.with_suffix(".receipt.json").write_text(json.dumps({
            "schema_version": 1, "source": "shioaji_kbars_1m", "storage_frequency": "minute",
            "simulation": True, "symbol": row.symbol, "start_date": str(old_start), "end_date": str(old_end),
            "status": "ok", "rows": frame.height,
            "underlying_data_method": "observed_ticks_aggregated_to_right_labelled_1m",
            "raw_tick_sources": [{"path": str(raw), "symbol": row.symbol, "size": raw.stat().st_size,
                                  "sha256": hashlib.sha256(raw.read_bytes()).hexdigest()}],
            "output_receipt": {"path": str(old_path), "size": old_path.stat().st_size,
                               "sha256": hashlib.sha256(old_path.read_bytes()).hexdigest()},
        }))
    manifest_path = root / "symbols" / f"{row.symbol}.manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "source": "shioaji_kbars_1m",
                "storage_frequency": "minute",
                "simulation": True,
                "symbol": row.symbol,
                "requested_start": old_start.isoformat(),
                "requested_end": old_end.isoformat(),
                "chunks": [
                    {
                        "start_date": old_start.isoformat(),
                        "end_date": old_end.isoformat(),
                        "status": "ok",
                        "data_path": str(old_path),
                        "data_sha256": hashlib.sha256(
                            old_path.read_bytes()
                        ).hexdigest(),
                        "source_gap_dates": [],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    chunks = [(old_start, new_end)]
    for _ in range(extra_chunks):
        chunks.append((chunks[-1][1] + timedelta(days=1), chunks[-1][1] + timedelta(days=29)))
    # A newly observed public trade outside the archive must not become an empty
    # bucket, even when the current contract directory no longer lists it.
    pl.DataFrame({'date': [TRADE_DATE, chunks[-1][1]], 'Trading_Volume': [1000., 1000.]}).write_parquet(base)
    assert not restore_extended_tail_from_archived_manifest(root, row, chunks,
        requested_start=old_start, requested_end=chunks[-1][1], simulation=True,
        expected_dates={TRADE_DATE, chunks[-1][1]})
    pl.DataFrame({'date': [TRADE_DATE], 'Trading_Volume': [1000.]}).write_parquet(base)
    restored = restore_extended_tail_from_archived_manifest(
        root,
        row,
        chunks,
        requested_start=old_start,
        requested_end=chunks[-1][1],
        simulation=True,
        expected_dates={TRADE_DATE},
    )

    assert restored
    receipt = json.loads(
        (
            root
            / "minute_chunks"
            / row.symbol
            / f"{old_start.isoformat()}_{new_end.isoformat()}.receipt.json"
        ).read_text()
    )
    assert receipt["status"] == "ok"
    assert receipt["query_performed"] is False
    assert receipt["query_skipped_reason"] == "archived_delisted_contract_tail_repacked"
    assert receipt["rows"] == frame.height
    for start, end in chunks[1:]:
        p = minute_chunk_paths(root, row.symbol, start, end)[1]
        assert minute_receipt_valid(p, symbol=row.symbol, start=start, end=end)
        proof = json.loads(p.read_text())
        assert proof['status'] == 'empty' and proof['output_receipt'] is None
        assert proof['returned_dates'] == [] and not proof['query_performed']
        assert proof['expected_reference_receipt']['sha256'] == hashlib.sha256(base.read_bytes()).hexdigest()
    if tick_backed:
        assert receipt["underlying_data_method"] == "observed_ticks_aggregated_to_right_labelled_1m"
        assert receipt["raw_tick_sources"][0]["path"] == str(raw)
        new_receipt = minute_chunk_paths(root, row.symbol, old_start, new_end)[1]
        assert minute_receipt_valid(new_receipt, symbol=row.symbol, start=old_start, end=new_end)
        raw.write_bytes(b"changed")
        assert not minute_receipt_valid(new_receipt, symbol=row.symbol, start=old_start, end=new_end)


def test_completed_bar_features_do_not_read_next_bar() -> None:
    baseline = _research_frame(next_open=106.0, next_close=107.0)
    changed_future = _research_frame(next_open=160.0, next_close=170.0)
    decision_ts = datetime(2026, 7, 24, 9, 6)

    baseline_row = baseline.filter(pl.col("ts") == decision_ts).row(0, named=True)
    changed_row = changed_future.filter(pl.col("ts") == decision_ts).row(0, named=True)

    assert baseline_row["feature_valid"]
    for column in MODEL_FEATURE_COLUMNS:
        assert changed_row[column] == pytest.approx(baseline_row[column])
    assert changed_row["execution_open_next_1m"] == pytest.approx(160.0)
    assert changed_row["exit_close_next_1m"] == pytest.approx(170.0)
    assert changed_row["long_gross_return_next_1m"] != pytest.approx(
        baseline_row["long_gross_return_next_1m"]
    )


def test_research_frame_infers_mixed_shioaji_volume_units_from_amount() -> None:
    raw = _raw_minute_frame(source_volume_multiplier=1_000.0).with_columns(
        pl.when(pl.int_range(pl.len()) == 1)
        .then(pl.col("Volume") * pl.col("contract_unit"))
        .otherwise(pl.col("Volume"))
        .alias("Volume")
    )
    result = build_research_frame(raw.lazy()).collect()

    assert result["source_volume_unit_valid"].to_list() == [True] * result.height
    assert result["source_volume_multiplier"].to_list() == [
        1_000.0,
        1.0,
        1_000.0,
        1_000.0,
        1_000.0,
        1_000.0,
        1_000.0,
        1_000.0,
    ]
    assert result["volume_shares"].to_list() == [100_000.0] * result.height


def test_dataset_audit_accepts_causal_synthetic_partition() -> None:
    result = audit_frame(_research_frame(), trade_date=TRADE_DATE)

    assert result["status"] == "ok"
    assert result["symbols"] == 1
    assert result["failures"] == {
        "duplicate_keys": 0,
        "out_of_session_rows": 0,
        "wrong_date_rows": 0,
        "raw_invalid_rows": 0,
        "invalid_volume_unit_rows": 0,
        "invalid_volume_notional_rows": 0,
        "invalid_volume_shares_rows": 0,
        "off_grid_price_values": 0,
        "unknown_price_security_rows": 0,
        "invalid_rows_with_labels": 0,
        "invalid_session_rows_with_labels": 0,
        "bad_label_alignment_rows": 0,
        "bad_label_value_rows": 0,
    }


def test_chronological_splits_never_shuffle_dates() -> None:
    dates = [TRADE_DATE - timedelta(days=index) for index in range(10)]
    splits = chronological_date_splits(dates)

    assert len(splits["train"]) == 6
    assert len(splits["validation"]) == 2
    assert len(splits["test"]) == 2
    assert max(splits["train"]) < min(splits["validation"])
    assert max(splits["validation"]) < min(splits["test"])


def test_default_mode_makes_a_stateful_decision_every_minute() -> None:
    config = MinuteKbarBacktestConfig(
        top_n=1,
        minimum_research_days=1,
        minimum_research_symbols=1,
    )
    result = run_minute_rebalance_backtest(
        _stateful_rows(),
        score_column="score_blend",
        config=config,
    )

    assert config.holding_mode == "minute_rebalance"
    assert config.first_decision_minute == 1
    assert result.summary["decisions"] == 2
    assert result.summary["timing_contract"].startswith(
        "every completed right-labelled bar"
    )
    minute_trades = result.trades.filter(pl.col("reason") == "minute_rebalance")
    assert minute_trades["side"].to_list() == ["buy", "sell", "buy"]
    assert minute_trades["symbol"].to_list() == ["2330", "2330", "0050"]
    assert result.trades["reason"][-1] == "forced_session_close"
    assert result.equity_curve["actual_gross_exposure"][-1] == pytest.approx(0.0)


def test_stateful_minute_selection_does_not_replace_invalid_top_rank() -> None:
    only_first_minute = _stateful_rows(invalid_top_rank=True).filter(
        pl.col("minutes_from_open").is_in([6, 270])
    )
    result = run_minute_rebalance_backtest(
        only_first_minute,
        score_column="score_blend",
        config=MinuteKbarBacktestConfig(
            top_n=1,
            first_decision_minute=6,
            last_decision_minute=6,
            minimum_research_days=1,
            minimum_research_symbols=1,
        ),
    )

    assert result.summary["decisions"] == 1
    assert result.summary["trades"] == 0
    assert result.equity_curve["selected_names"].to_list() == [1]
    assert result.equity_curve["filled_names"].to_list() == [0]


def test_minute_rank_hysteresis_keeps_existing_position() -> None:
    result = run_minute_rebalance_backtest(
        _stateful_rows(second_2330_score=0.5),
        score_column="score_blend",
        config=MinuteKbarBacktestConfig(
            top_n=1,
            selection_hysteresis_multiplier=2.0,
            minimum_research_days=1,
            minimum_research_symbols=1,
        ),
    )

    minute_trades = result.trades.filter(pl.col("reason") == "minute_rebalance")
    assert minute_trades["symbol"].unique().to_list() == ["2330"]
    assert "0050" not in minute_trades["symbol"].to_list()
    assert result.summary["decisions"] == 2


def test_invalid_top_rank_is_not_replaced_using_future_fill_data() -> None:
    result = run_minute_round_trip_backtest(
        _backtest_rows(first_valid=False, first_close=None),
        score_column="score_blend",
        config=MinuteKbarBacktestConfig(
            top_n=1,
            holding_mode="session_close",
            first_decision_minute=30,
            minimum_research_days=1,
            minimum_research_symbols=1,
        ),
    )

    assert result.summary["trades"] == 0
    assert result.summary["final_equity"] == pytest.approx(
        result.summary["initial_equity"]
    )
    assert result.equity_curve["selected_names"].to_list() == [1]
    assert result.equity_curve["filled_names"].to_list() == [0]


def test_flat_price_round_trip_is_negative_after_fees_and_slippage() -> None:
    result = run_minute_round_trip_backtest(
        _backtest_rows(
            first_valid=True,
            first_close=100.0,
            include_second=False,
        ),
        score_column="score_blend",
        config=MinuteKbarBacktestConfig(
            top_n=1,
            holding_mode="session_close",
            first_decision_minute=30,
            minimum_research_days=1,
            minimum_research_symbols=1,
        ),
    )

    assert result.summary["trades"] == 1
    assert result.trades["gross_return"][0] == pytest.approx(0.0)
    assert result.trades["net_return"][0] < 0.0
    assert result.summary["total_explicit_fees"] > 0.0
    assert result.summary["total_slippage_cost"] > 0.0
    assert result.summary["total_return"] < 0.0
