from __future__ import annotations

import io
import hashlib
import json
import math
import zipfile
from dataclasses import dataclass
from types import SimpleNamespace

import polars as pl
import pytest

from downloader import okx_historical_features as features


def _funding_prefix_frame() -> tuple[pl.DataFrame, int]:
    start = 1_767_225_600_000
    frame = pl.DataFrame({
        "date": [features._ms_to_date_string(start + i * 60_000) for i in range(6)],
        "okx_funding_realized_rate": [-0.0001] * 6,
        "okx_funding_interval_hours": [None, None, 2 / 60, 2 / 60, 2 / 60, 2 / 60],
        "okx_funding_age_hours": [0., 1 / 60, 0., 1 / 60, 2 / 60, 3 / 60],
    })
    return frame, start


@pytest.mark.parametrize("reverse", [False, True])
def test_funding_initial_event_interval_null_is_not_a_history_query_hole(reverse):
    frame, start = _funding_prefix_frame()
    if reverse:
        frame = frame.reverse()
    before = frame.clone()
    assert features._funding_history_start(frame, start) == start + 4 * 60_000
    assert frame.equals(before)  # No fill, rounding or invented interval in source values.


@pytest.mark.parametrize("problem", ["later_hole", "mixed_first_interval", "absent_age", "null_age", "negative_age", "nan_age", "nan_rate"])
def test_funding_repair_keeps_rewinding_unproven_or_later_metadata_holes(problem):
    frame, start = _funding_prefix_frame()
    if problem == "later_hole":
        frame = frame.with_columns(pl.Series("okx_funding_interval_hours", [None, None, 2 / 60, None, 2 / 60, 2 / 60]))
        expected = start + 3 * 60_000
    elif problem == "mixed_first_interval":
        frame = frame.with_columns(pl.Series("okx_funding_interval_hours", [None, 8., 2 / 60, 2 / 60, 2 / 60, 2 / 60]))
        expected = start
    elif problem == "absent_age":
        frame = frame.drop("okx_funding_age_hours")
        expected = start
    elif problem == "nan_rate":
        frame = frame.with_columns(pl.Series("okx_funding_realized_rate", [float("nan"), -0.0001, -0.0001, -0.0001, -0.0001, -0.0001]))
        expected = start
    else:
        ages = frame["okx_funding_age_hours"].to_list()
        ages[0] = {"null_age": None, "negative_age": -1., "nan_age": float("nan")}[problem]
        frame = frame.with_columns(pl.Series("okx_funding_age_hours", ages))
        expected = start
    assert features._funding_history_start(frame, start) == expected


def test_funding_start_without_history_or_all_null_rates_keeps_original_fallback():
    frame, start = _funding_prefix_frame()
    assert features._funding_history_start(frame.select("date"), start) == start
    frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias("okx_funding_realized_rate"))
    assert features._funding_history_start(frame, start) == start


@pytest.mark.parametrize("features_enabled", [False, True])
def test_targeted_quality_run_preserves_global_evidence(tmp_path, monkeypatch, features_enabled):
    from downloader import download_okx_perp_daily as collector

    output = tmp_path / "candles"
    output.mkdir()
    global_names = ["symbols.csv", "download_report.csv", "download_summary.json",
                    "download_receipt.json", "progress.json", "historical_feature_report.csv",
                    "okx_historical_feature_catalog.json", "historical_feature_summary.json"]
    for name in global_names:
        (output / name).write_bytes(b"preserved existing global evidence")
    records = [collector.SymbolRecord(code=f"{base}USDTSWAP", name=base, market="okx_perp",
        okx_symbol=f"{base}-USDT-SWAP", base_ccy=base, quote_ccy="USDT", settle_ccy="USDT",
        ct_type="linear", inst_family=f"{base}-USDT", uly=f"{base}-USDT", state="live",
        list_time=None) for base in ("BTC", "ETH")]
    monkeypatch.setattr(collector, "OkxClient", lambda **_: SimpleNamespace(limiter_activity=lambda: {}))
    monkeypatch.setattr(collector, "_fetch_swap_symbols", lambda *_a, **_k: records)
    seen, enriched = [], []

    def download(_client, record, folder, *_args, **_kwargs):
        seen.append(record.code)
        return collector.DownloadResult("crypto_okx_perp", record.code, record.okx_symbol,
            record.market, "skipped_up_to_date", 1, str(folder / f"{record.code}_features.parquet"))

    monkeypatch.setattr(collector, "_download_symbol_1m", download)
    if features_enabled:
        pl.DataFrame({"date": ["2026-01-01 00:00:00"]}).write_parquet(output / "BTCUSDTSWAP_features.parquet")

        def enrich(_client, selected, _output, **kwargs):
            for record in selected:
                enriched.append(record.code)
                for stage in features.FEATURE_STAGE_IDS:
                    kwargs["stage_progress_callback"](record.code, stage, "ok")
            return [features.HistoricalFeatureResult("BTCUSDTSWAP", "BTC-USDT-SWAP", "unchanged",
                1, str(output / "BTCUSDTSWAP_features.parquet"), False, "{}", "{}", "{}")]

        monkeypatch.setattr(collector, "run_historical_feature_downloads", enrich)
    monkeypatch.setattr(collector.sys, "argv", ["collector", "--output-dir", str(output),
        "--symbols", "BTC-USDT-SWAP", "--start-date", "2026-01-01", "--end-date", "2026-01-01",
        "--workers", "1"] + ([] if features_enabled else ["--skip-historical-features"]))
    collector.main()
    assert seen == ["BTCUSDTSWAP"]
    assert enriched == (["BTCUSDTSWAP"] if features_enabled else [])
    assert all((output / name).read_bytes() == b"preserved existing global evidence" for name in global_names)
    reports = list((output / "quality_repair_runs").glob("*/download_summary.json"))
    assert len(reports) == 1
    summary = json.loads(reports[0].read_text())
    assert summary["provider_scope_is_complete"] is False
    assert summary["requested_symbol_filter"] == ["BTC-USDT-SWAP"]


def test_enrichment_records_each_stage_time_even_when_source_fails(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = pl.DataFrame(
        {
            "date": ["2026-01-01 00:01:00"],
            "close": [100.0],
            "Trading_Volume": [1.0],
        }
    )
    monkeypatch.setattr(features, "_read_parquet", lambda _path: base)
    monkeypatch.setattr(features, "_add_derived_features", lambda frame: frame)
    monkeypatch.setattr(features, "_coverage_summary", lambda _frame: {})

    def unavailable(*_args, **_kwargs):
        raise RuntimeError("offline")

    for name in ("_fetch_array_history", "_fetch_object_history", "_fetch_rubik_history"):
        monkeypatch.setattr(features, name, unavailable)
    result = features.enrich_symbol_historical_features(
        object(),
        SimpleNamespace(code="BTC", okx_symbol="BTC-USDT-SWAP", inst_family="BTC-USDT"),
        tmp_path / "BTC_features.parquet",
        start_ms=1767225600000,
        end_ms=1767225660000,
        include_funding_archive=False,
    )

    elapsed = json.loads(result.stage_elapsed_seconds_json)
    assert set(features.FEATURE_STAGE_IDS) <= elapsed.keys()
    assert {"read_existing", "derive", "compare", "coverage"} <= elapsed.keys()
    assert all(value >= 0 for value in elapsed.values())
    assert result.total_elapsed_seconds >= 0
    assert result.status == "failed"
    assert set(json.loads(result.stage_status_json).values()) == {"failed"}
    summary = features.stage_latency_summary([result])
    assert summary["mark_price"]["samples"] == 1
    assert summary["mark_price"]["p95_seconds"] >= 0


def test_catalog_never_includes_snapshot_only_source() -> None:
    included = {
        "included",
        "included_existing",
        "included_limited_history",
    }
    for item in features.HISTORICAL_FEATURE_CATALOG:
        if item["download_status"] in included:
            assert "snapshot" not in item["category"]
            assert item["download_status"] != "excluded_snapshot"


def test_normalize_price_candles_keeps_only_completed_requested_rows() -> None:
    rows = [
        ["900000", "1", "3", "0.5", "2", "1"],
        ["1800000", "2", "4", "1", "3", "0"],
        ["2700000", "3", "5", "2", "4", "1"],
    ]

    frame = features._normalize_price_candles(
        rows,
        prefix="okx_mark",
        start_ms=900000,
        end_ms=1800000,
    )

    assert frame.height == 1
    assert frame["okx_mark_close"].to_list() == [2.0]


def test_funding_asof_uses_only_events_available_by_bar_close() -> None:
    base = pl.DataFrame(
        {
            "date": [
                "2026-01-01 00:18:00",
                "2026-01-01 00:19:00",
                "2026-01-01 00:20:00",
            ]
        }
    )
    event_ms = int(
        features.datetime(2026, 1, 1, 0, 20, tzinfo=features.timezone.utc).timestamp()
        * 1000
    )
    events = [
        {
            "ts": event_ms,
            "okx_funding_rate_at_settlement": 0.001,
            "okx_funding_realized_rate": 0.002,
            "okx_funding_interval_hours": 8.0,
            "okx_funding_formula_with_rate": 1.0,
            "okx_funding_method_current_period": 1.0,
        }
    ]

    materialized = features._materialize_funding_asof(
        base,
        events,
        start_ms=event_ms - 60 * 60 * 1000,
        end_ms=event_ms + 60 * 60 * 1000,
    )

    assert materialized["okx_funding_realized_rate"].to_list() == [
        None,
        pytest.approx(0.002),
        pytest.approx(0.002),
    ]
    assert materialized["okx_funding_age_hours"].to_list()[0] is None
    assert materialized["okx_funding_age_hours"].to_list()[1] == pytest.approx(
        0.0
    )


def test_derived_features_are_normalized_and_gap_aware() -> None:
    frame = pl.DataFrame(
        {
            "date": [
                "2026-01-01 00:00:00",
                "2026-01-01 00:05:00",
                "2026-01-01 01:00:00",
            ],
            "close": [100.0, 102.0, 110.0],
            "Trading_Volume": [10.0, 20.0, 30.0],
            "okx_mark_open": [100.0, 101.0, 109.0],
            "okx_mark_high": [102.0, 104.0, 111.0],
            "okx_mark_low": [99.0, 100.0, 108.0],
            "okx_mark_close": [101.0, 103.0, 110.0],
            "okx_index_open": [100.0, 100.5, 108.0],
            "okx_index_high": [101.0, 103.0, 110.0],
            "okx_index_low": [99.0, 100.0, 107.0],
            "okx_index_close": [100.5, 102.0, 109.0],
            "okx_open_interest_contracts": [1000.0, 1100.0, 1200.0],
            "okx_open_interest_usd": [100000.0, 120000.0, 130000.0],
            "okx_taker_sell_volume_contracts": [40.0, 25.0, 20.0],
            "okx_taker_buy_volume_contracts": [60.0, 75.0, 20.0],
        }
    )

    derived = features._add_derived_features(frame)

    assert derived["okx_taker_imbalance"].to_list() == pytest.approx(
        [0.2, 0.5, 0.0]
    )
    assert derived["okx_open_interest_usd_log_change_5m"][1] == pytest.approx(
        math.log(1.2)
    )
    assert derived["okx_open_interest_usd_log_change_5m"][2] is None
    assert derived["okx_mark_index_basis_log"][0] == pytest.approx(
        math.log(101.0 / 100.5)
    )


@dataclass
class _ArchiveClient:
    zip_bytes: bytes

    def get(self, path: str, params: dict) -> dict:
        assert path == features.MARKET_DATA_HISTORY_ENDPOINT
        return {
            "data": [
                {
                    "details": [
                        {
                            "groupDetails": [
                                {
                                    "filename": "BTC-USDT-SWAP-fundingrates-2026-01.zip",
                                    "url": "https://example.invalid/funding.zip",
                                }
                            ]
                        }
                    ]
                }
            ]
        }

    def get_bytes(self, url: str) -> bytes:
        assert url == "https://example.invalid/funding.zip"
        return self.zip_bytes


def test_funding_archive_parser_filters_symbol_and_range() -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "funding.csv",
            "instrument_name,funding_rate,funding_time\n"
            "BTC-USDT-SWAP,0.001,1767225600000\n"
            "ETH-USDT-SWAP,0.002,1767225600000\n",
        )
    client = _ArchiveClient(buffer.getvalue())

    rows = features._fetch_funding_archive(
        client,
        inst_family="BTC-USDT",
        okx_symbol="BTC-USDT-SWAP",
        start_ms=1767225500000,
        end_ms=1767225700000,
    )

    assert len(rows) == 1
    assert rows[0]["realizedRate"] == "0.001"


@pytest.mark.parametrize("months", [1, 10, 11, 20, 21])
def test_monthly_archive_queries_use_maximum_ten_inclusive_utc8_months(months):
    from datetime import datetime, timedelta, timezone
    tz = timezone(timedelta(hours=8))
    first = datetime(2025, 11, 1, tzinfo=tz)
    last = features._add_months(first, months - 1) + timedelta(days=20, hours=12)
    ranges = features._monthly_query_ranges(int((first + timedelta(hours=3)).timestamp() * 1000),
                                           int(last.timestamp() * 1000))
    assert len(ranges) == (months + 9) // 10
    assert ranges[0][0] == int(first.timestamp() * 1000)
    assert ranges[-1][1] == int(last.timestamp() * 1000)
    for index, (begin, end) in enumerate(ranges):
        a, b = (datetime.fromtimestamp(value / 1000, tz=tz) for value in (begin, end))
        assert 1 <= (b.year - a.year) * 12 + b.month - a.month + 1 <= 10
        assert a.day == 1 and a.hour == 0
        if index:
            assert begin == ranges[index - 1][1] + 1
    assert features._monthly_query_ranges(2, 1) == []


class _PricePages:
    def __init__(self, *, now_ms, recent_problem=None, history_problem=None):
        self.now_ms = now_ms
        self.calls = []
        self.recent_problem = recent_problem
        self.history_problem = history_problem

    def get(self, path, params):
        self.calls.append((path, dict(params)))
        problem = self.recent_problem if path == features.INDEX_PRICE_RECENT_ENDPOINT else self.history_problem
        if problem is not None:
            if isinstance(problem, Exception):
                raise problem
            return problem
        cursor = int(params["after"])
        first = cursor - features.CANDLE_INTERVAL_MS
        last = first - (int(params["limit"]) - 1) * features.CANDLE_INTERVAL_MS
        if path == features.INDEX_PRICE_RECENT_ENDPOINT:
            last = max(last, self.now_ms - features.INDEX_PRICE_RECENT_ENTRIES * features.CANDLE_INTERVAL_MS)
        return {"data": [
            [str(ts), "100", "110", "99", str(100 + (ts // 60000) % 7), "1"]
            for ts in range(first, last - 1, -features.CANDLE_INTERVAL_MS)
        ]}


def _price_fetch(client, *, minutes=1440, lag_minutes=1, prefer_recent=True):
    end = client.now_ms - lag_minutes * features.CANDLE_INTERVAL_MS
    start = end - (minutes - 1) * features.CANDLE_INTERVAL_MS
    audit = {}
    rows = features._fetch_array_history(
        client, features.INDEX_PRICE_HISTORY_ENDPOINT, {"instId": "BTC-USDT", "bar": "1m"},
        start_ms=start, end_ms=end, limit=100,
        recent_path=features.INDEX_PRICE_RECENT_ENDPOINT if prefer_recent else None,
        now_ms=client.now_ms, request_audit=audit,
    )
    return features._normalize_price_candles(rows, prefix="okx_index", start_ms=start, end_ms=end), audit


@pytest.mark.parametrize("minutes,lag", [(1, 1), (99, 1), (100, 1), (101, 1), (1440, 1), (2880, 1), (1440, 100), (100, 1440), (1440, 4000)])
def test_recent_index_routing_exactly_matches_history_across_retention_edge(minutes, lag):
    now_ms = 30_000 * features.CANDLE_INTERVAL_MS
    baseline = _PricePages(now_ms=now_ms)
    candidate = _PricePages(now_ms=now_ms)
    expected, _ = _price_fetch(baseline, minutes=minutes, lag_minutes=lag, prefer_recent=False)
    actual, audit = _price_fetch(candidate, minutes=minutes, lag_minutes=lag)
    assert actual.equals(expected)
    assert actual.height == minutes
    assert all(int(params["limit"]) == 100 for _, params in candidate.calls)
    assert len(baseline.calls) == len(candidate.calls)
    assert audit["fallback_reason"] is None
    if lag >= 1440:
        assert set(audit["page_calls"]) == {features.INDEX_PRICE_HISTORY_ENDPOINT}
    if minutes == 1440 and lag == 1:
        assert audit["page_calls"] == {
            features.INDEX_PRICE_RECENT_ENDPOINT: 13,
            features.INDEX_PRICE_HISTORY_ENDPOINT: 2,
        }
        assert sum(int(params["limit"]) for _, params in candidate.calls) == 1500


@pytest.mark.parametrize("problem", [
    ConnectionError("disconnected"),
    {"data": []},
    {"data": None},
    {"data": [[]]},
    {"data": [["1799940000", "1", "2", "1", "1", "1"]]},
])
def test_recent_failure_falls_back_at_identical_cursor_once_without_losing_history(problem):
    baseline = _PricePages(now_ms=1_800_000_000)
    candidate = _PricePages(now_ms=baseline.now_ms, recent_problem=problem)
    expected, _ = _price_fetch(baseline, minutes=1440, prefer_recent=False)
    actual, audit = _price_fetch(candidate, minutes=1440)
    assert actual.equals(expected)
    assert candidate.calls[0][1] == candidate.calls[1][1]
    assert candidate.calls[0][0] == features.INDEX_PRICE_RECENT_ENDPOINT
    assert all(path == features.INDEX_PRICE_HISTORY_ENDPOINT for path, _ in candidate.calls[1:])
    assert audit["page_calls"][features.INDEX_PRICE_RECENT_ENDPOINT] == 1
    assert audit["fallback_reason"] is not None


@pytest.mark.parametrize("kind", ["short", "gap", "duplicate", "unconfirmed", "missing_newest"])
def test_recent_incomplete_page_is_not_committed_before_history_fallback(kind):
    now_ms = 1_800_000_000
    client = _PricePages(now_ms=now_ms)
    valid = client.get(features.INDEX_PRICE_RECENT_ENDPOINT, {"after": str(now_ms), "limit": "100"})["data"]
    if kind == "short":
        valid = valid[:10]
    elif kind == "gap":
        valid[50][0] = str(int(valid[-1][0]) - 60_000)
    elif kind == "duplicate":
        valid[50] = list(valid[51])
    elif kind == "unconfirmed":
        valid[50][-1] = "0"
    else:
        valid = valid[1:]
    client.calls.clear()
    client.recent_problem = {"data": valid}
    expected, _ = _price_fetch(_PricePages(now_ms=now_ms), minutes=1440, prefer_recent=False)
    actual, audit = _price_fetch(client, minutes=1440)
    assert actual.equals(expected)
    assert client.calls[0][1] == client.calls[1][1]
    assert audit["fallback_reason"] == "recent_short_or_noncontiguous"


def test_recent_short_page_covering_entire_requested_window_does_not_fetch_history():
    now_ms = 1_800_000_000
    client = _PricePages(now_ms=now_ms)
    page = client.get(features.INDEX_PRICE_RECENT_ENDPOINT, {"after": str(now_ms), "limit": "100"})["data"][:5]
    client.calls.clear()
    client.recent_problem = {"data": page}
    frame, audit = _price_fetch(client, minutes=5)
    assert frame.height == 5
    assert audit["page_calls"] == {features.INDEX_PRICE_RECENT_ENDPOINT: 1}
    assert audit["fallback_reason"] is None


def test_exhausted_recent_and_history_failure_remains_failure():
    client = _PricePages(now_ms=1_800_000_000, recent_problem=ConnectionError("recent offline"), history_problem=ConnectionError("history offline"))
    with pytest.raises(ConnectionError, match="history offline"):
        _price_fetch(client)
    assert len(client.calls) == 2
    assert client.calls[0][1] == client.calls[1][1]


@pytest.mark.parametrize("pager", ["array", "object", "rubik"])
def test_all_historical_pagers_reject_repeated_page_instead_of_silent_success(pager):
    end = 1_800_000
    rows = [[str(end - offset * 60_000), "1", "2", "1", "1", "1"] for offset in range(2)]
    if pager == "object":
        rows = [{"fundingTime": row[0]} for row in rows]
    client = SimpleNamespace(get=lambda *_args: {"data": rows})
    fn = {
        "array": features._fetch_array_history,
        "object": features._fetch_object_history,
        "rubik": features._fetch_rubik_history,
    }[pager]
    extra = {"timestamp_field": "fundingTime"} if pager == "object" else {}
    with pytest.raises(ValueError, match="does not advance"):
        fn(client, "/history", {}, start_ms=end - 10 * 60_000, end_ms=end, limit=2, **extra)


@pytest.mark.parametrize("data", [None, "invalid", [[]], [False], [["invalid"]], [[True]], [[None]], [[-1]], [[1.25]]])
def test_history_page_rejects_malformed_rows_or_missing_timestamps(data):
    client = SimpleNamespace(get=lambda *_args: {"data": data})
    with pytest.raises(ValueError):
        features._history_page(client, "/history", {"limit": "100"}, cursor_ms=100)


def test_acquisition_fingerprint_is_stable_separate_from_unchanged_feature_abi():
    acquisition = features.feature_acquisition_payload()
    catalog = features.feature_catalog_payload()
    encoded = json.dumps(acquisition["contract"], sort_keys=True, separators=(",", ":")).encode()
    assert hashlib.sha256(encoded).hexdigest() == acquisition["fingerprint_sha256"]
    assert catalog["acquisition"] == acquisition
    assert acquisition["contract"]["version"] == 5
    assert acquisition["contract"]["feature_schema_version"] == catalog["schema_version"] == 1
    acquisition["contract"]["version"] = -1
    assert features.feature_acquisition_payload()["contract"]["version"] == 5


def test_oversized_history_page_retries_exact_request_without_committing_bad_rows():
    calls = []
    pages = iter([[["90"], ["80"], ["70"]], [["90"], ["80"]]])
    def get(path, params):
        calls.append((path, dict(params)))
        return {"data": next(pages)}
    rows, oldest = features._history_page(
        SimpleNamespace(get=get), features.OPEN_INTEREST_HISTORY_ENDPOINT,
        {"limit": "2", "end": "100", "begin": "10"}, cursor_ms=100,
    )
    assert rows == [["90"], ["80"]]
    assert oldest == 80
    assert len(calls) == 2 and calls[0] == calls[1]


def test_persistent_oversized_page_still_fails_closed_after_bounded_retry():
    calls = []
    def get(path, params):
        calls.append((path, dict(params)))
        return {"data": [["90"], ["80"], ["70"]]}
    with pytest.raises(ValueError, match="exceeds requested limit after exact request retry"):
        features._history_page(SimpleNamespace(get=get), "/history", {"limit": "2"}, cursor_ms=100)
    assert len(calls) == 2 and calls[0] == calls[1]


def test_full_enrichment_preserves_existing_prices_when_index_stage_fails(tmp_path, monkeypatch):
    end = 1767225660000
    base = pl.DataFrame({"date": ["2026-01-01 00:01:00"], "close": [100.0], "Trading_Volume": [1.0], "okx_index_close": [99.0]})
    monkeypatch.setattr(features, "_read_parquet", lambda _path: base)
    written = []
    monkeypatch.setattr(features, "_write_parquet", lambda frame, _path: written.append(frame))
    client = SimpleNamespace(get=lambda *_args: {"data": [[]]})
    result = features.enrich_symbol_historical_features(
        client, SimpleNamespace(code="BTC", okx_symbol="BTC-USDT-SWAP", inst_family="BTC-USDT"),
        tmp_path / "BTC_features.parquet", start_ms=end, end_ms=end, include_funding_archive=False,
    )
    assert result.status == "failed"
    assert json.loads(result.stage_status_json)["index_price"] == "failed"
    assert written[0]["okx_index_close"].to_list() == [99.0]
    assert "ValueError" in json.loads(result.errors_json)["index_price"]


def test_complete_eight_stage_enrichment_writes_exact_same_features_and_masks_for_hybrid(tmp_path, monkeypatch):
    now_ms = 30_000 * 60_000
    end_ms = now_ms - 60_000
    start_ms = now_ms - 1440 * 60_000

    class FullClient(_PricePages):
        def get(self, path, params):
            if path == features.FUNDING_RATE_HISTORY_ENDPOINT:
                self.calls.append((path, dict(params)))
                return {"data": [
                    {"fundingTime": str(ts), "fundingRate": "0.0001", "realizedRate": "0.00012", "formulaType": "withRate", "method": "current_period"}
                    for ts in (start_ms, start_ms - 8 * 60 * 60 * 1000)
                ]}
            if path.startswith("/api/v5/rubik/"):
                self.calls.append((path, dict(params)))
                cursor = (int(params["end"]) // 300_000) * 300_000
                floor = max(int(params["begin"]), cursor - (int(params["limit"]) - 1) * 300_000)
                values = ["1000", "100", "100000"] if path == features.OPEN_INTEREST_HISTORY_ENDPOINT else ["20", "30"] if path == features.TAKER_VOLUME_HISTORY_ENDPOINT else ["1.2"]
                return {"data": [[str(ts), *values] for ts in range(cursor, floor - 1, -300_000)]}
            return super().get(path, params)

    original_fetch = features._fetch_array_history
    prefer_recent = False

    def fixed_window_fetch(*args, **kwargs):
        kwargs["now_ms"] = now_ms
        if not prefer_recent:
            kwargs["recent_path"] = None
        return original_fetch(*args, **kwargs)

    monkeypatch.setattr(features, "_fetch_array_history", fixed_window_fetch)
    base = pl.DataFrame({
        "date": [features._ms_to_date_string(ts) for ts in range(start_ms, end_ms + 1, 60_000)],
        "close": [100.0] * 1440,
        "Trading_Volume": [20.0] * 1440,
    })
    frames, results = [], []
    record = SimpleNamespace(code="BTC", okx_symbol="BTC-USDT-SWAP", inst_family="BTC-USDT")
    for variant in ("history_only", "hybrid"):
        prefer_recent = variant == "hybrid"
        output = tmp_path / f"{variant}_features.parquet"
        features._write_parquet(base, output)
        result = features.enrich_symbol_historical_features(
            FullClient(now_ms=now_ms), record, output,
            start_ms=start_ms, end_ms=end_ms, include_funding_archive=False,
        )
        results.append(result)
        frames.append(features._read_parquet(output))
    assert frames[0].equals(frames[1])
    assert frames[1].height == 1440
    assert set(features.ALL_FEATURE_COLUMNS) <= set(frames[1].columns)
    assert all(result.status == "updated" for result in results)
    assert all(json.loads(result.errors_json) == {} for result in results)
    assert all(set(json.loads(result.stage_status_json).values()) == {"ok"} for result in results)
    assert json.loads(results[0].coverage_json) == json.loads(results[1].coverage_json)
    assert json.loads(results[0].index_acquisition_json)["page_calls"] == {features.INDEX_PRICE_HISTORY_ENDPOINT: 15}
    assert json.loads(results[1].index_acquisition_json)["page_calls"] == {features.INDEX_PRICE_RECENT_ENDPOINT: 13, features.INDEX_PRICE_HISTORY_ENDPOINT: 2}
    assert frames[1]["okx_index_close"].null_count() == 0
    assert frames[1]["okx_funding_realized_rate"].null_count() == 0
    assert frames[1]["okx_open_interest_usd"].drop_nulls().len() == 288
