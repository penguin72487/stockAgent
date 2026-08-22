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
    SharedRequestRateLimiter,
    SymbolResult,
    _write_run_summary,
    completed_symbol_manifest_result,
    contract_for_stock_symbol,
    minute_chunk_paths,
    query_minute_chunk,
    restore_extended_tail_from_archived_manifest,
    select_universe,
    stock_contract_map,
    validate_minute_kbars,
)
from scripts.audit_shioaji_tw_minute_dataset import audit_frame
from scripts.build_shioaji_tw_minute_dataset import (
    MODEL_FEATURE_COLUMNS,
    _feature_statistics,
    _validate_collection_gate,
    build_research_frame,
)
from stockagent.research.tw_minute_kbars import (
    MinuteKbarBacktestConfig,
    chronological_date_splits,
    run_minute_rebalance_backtest,
    run_minute_round_trip_backtest,
)


TRADE_DATE = date(2026, 7, 24)


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
        _validate_collection_gate(
            path, selected_symbols=[], subset_requested=False
        )


def test_account_wide_rate_limiter_is_shared_across_processes() -> None:
    context = mp.get_context("spawn")
    output = context.Queue()
    # Scale the configured 50/5s boundary down to a fast 50/0.05s test while
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

    row = UniverseRow(
        "3597", "映興", "tpex", "stock", Path("3597_features.parquet")
    )
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


def test_delisted_contract_extension_repacks_covered_archived_tail(
    tmp_path: Path,
) -> None:
    row = UniverseRow(
        "4130", "健亞", "tpex", "stock", Path("4130_features.parquet")
    )
    root = tmp_path / "minute"
    old_start = date(2026, 7, 9)
    old_end = date(2026, 7, 27)
    new_end = date(2026, 8, 4)
    old_path = root / "minute_chunks" / row.symbol / "archived.parquet"
    old_path.parent.mkdir(parents=True)
    frame = _raw_minute_frame().with_columns(pl.lit(row.symbol).alias("symbol"))
    frame.write_parquet(old_path)
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
                        "data_sha256": hashlib.sha256(old_path.read_bytes()).hexdigest(),
                        "source_gap_dates": [],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    restored = restore_extended_tail_from_archived_manifest(
        root,
        row,
        [(old_start, new_end)],
        requested_start=old_start,
        requested_end=new_end,
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
