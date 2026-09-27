from __future__ import annotations

from datetime import date
import csv
import gzip
from io import StringIO
import json
from pathlib import Path
import sys

import pandas as pd
import pyarrow.parquet as pq
import pytest

from scripts import download_taifex_public_history as collector

from scripts.download_taifex_public_history import (
    POSITIONING_SPECS,
    _next_session_map,
    _parse_positioning,
    _parse_put_call,
    _receipt_covers_request,
)


def _html_with_date(frame: pd.DataFrame) -> bytes:
    return (
        "<html><body><p>2026/09/01</p>"
        + frame.to_html(index=False)
        + "</body></html>"
    ).encode()


def test_put_call_ratio_keeps_trailing_comma_and_next_session_causality() -> None:
    content = (
        "日期,賣權成交量,買權成交量,買賣權成交量比率%,賣權未平倉量,"
        "買權未平倉量,買賣權未平倉量比率%\r\n"
        "2001/12/24,275,1145,24.02,90,683,13.18,\r\n"
    ).encode("cp950")
    frame = _parse_put_call(
        content,
        {date(2001, 12, 24): date(2001, 12, 25)},
    )

    assert len(frame) == 1
    assert frame.loc[0, "put_volume"] == 275
    assert frame.loc[0, "call_volume"] == 1145
    assert frame.loc[0, "available_date"].date() == date(2001, 12, 25)
    assert bool(frame.loc[0, "published_after_close"]) is True


def test_put_call_current_month_receipt_must_cover_latest_requested_date() -> None:
    receipt = {
        "request_payload": {
            "queryStartDate": "2026/09/01",
            "queryEndDate": "2026/09/01",
            "down_type": "1",
        }
    }

    assert _receipt_covers_request(receipt, dict(receipt["request_payload"])) is True
    assert _receipt_covers_request(
        receipt,
        {
            "queryStartDate": "2026/09/01",
            "queryEndDate": "2026/09/02",
            "down_type": "1",
        },
    ) is False


def test_institutional_parser_preserves_total_rows() -> None:
    rows = [
        ["1", "臺股期貨", "自營商", *range(1, 13)],
        ["期貨合計", "期貨合計", "期貨合計", *range(11, 23)],
    ]
    source = pd.DataFrame(rows)
    spec = next(item for item in POSITIONING_SPECS if item.name == "institutional_futures")
    frame = _parse_positioning(
        spec,
        _html_with_date(source),
        date(2026, 9, 1),
        _next_session_map([date(2026, 9, 1), date(2026, 9, 2)]),
    )

    assert frame["sequence"].tolist() == ["1", "期貨合計"]
    assert frame["participant_type"].tolist() == ["自營商", "期貨合計"]
    assert str(frame["trade_long_lots"].dtype) == "Int64"
    assert frame["available_date"].dt.date.tolist() == [
        date(2026, 9, 2),
        date(2026, 9, 2),
    ]


def test_large_trader_parser_splits_specific_corporate_values() -> None:
    source = pd.DataFrame(
        [[
            "臺股期貨(TX+MTX/4+TMF/20)",
            "所有 契約",
            "75,116  (75,116)",
            "64.3%  (64.3%)",
            "82,175  (81,277)",
            "70.4%  (69.6%)",
            "50,737  (50,737)",
            "43.5%  (43.5%)",
            "71,020  (71,020)",
            "60.8%  (60.8%)",
            "116742",
        ]]
    )
    spec = next(item for item in POSITIONING_SPECS if item.name == "large_trader_futures_tx")
    frame = _parse_positioning(
        spec,
        _html_with_date(source),
        date(2026, 9, 1),
        _next_session_map([date(2026, 9, 1), date(2026, 9, 2)]),
    )

    row = frame.iloc[0]
    assert row["expiry_bucket"] == "所有契約"
    assert row["buy_top10_positions"] == 82175
    assert row["buy_top10_specific_positions"] == 81277
    assert row["sell_top10_share_pct"] == 60.8
    assert row["market_open_interest"] == 116742


def test_large_trader_parser_accepts_spaces_inside_parentheses() -> None:
    source = pd.DataFrame(
        [["臺股期貨", "週契約", *(["0  (  0  )", "0%  (  0%  )"] * 4), "0"]]
    )
    spec = next(item for item in POSITIONING_SPECS if item.name == "large_trader_futures_tx")
    frame = _parse_positioning(
        spec,
        _html_with_date(source),
        date(2026, 9, 1),
        _next_session_map([date(2026, 9, 1), date(2026, 9, 2)]),
    )

    assert frame.loc[0, "buy_top5_positions"] == 0
    assert frame.loc[0, "buy_top5_specific_positions"] == 0


@pytest.mark.parametrize("spec", POSITIONING_SPECS)
def test_latest_positioning_report_does_not_require_next_session(spec) -> None:
    if spec.parser == "large_trader":
        source = pd.DataFrame(
            [["臺股期貨", "所有契約", *(["1 (1)", "1% (1%)"] * 4), "100"]]
        )
    else:
        labels = ["1", "臺股選擇權"]
        if spec.parser == "institutional_calls_puts":
            labels.append("買權")
        source = pd.DataFrame([[*labels, "自營商", *range(1, 13)]])
    frame = _parse_positioning(spec, _html_with_date(source), date(2026, 9, 1), {})
    assert len(frame) == 1
    assert frame["available_date"].isna().all()
    assert frame["published_after_close"].all()
    assert frame["availability_rule"].eq("next_receipt_verified_taifex_session").all()


def _save_test_shard(root: Path, day: date, available: date | None) -> dict:
    frame = collector._causal_columns(
        pd.DataFrame({"value": [100]}), day, {day: available} if available else {}
    )
    return collector._persist_response(
        root, dataset="institutional_futures", key=day.isoformat(),
        url=POSITIONING_SPECS[0].url, request_payload={"queryDate": day.isoformat()},
        content=f"official observation {day}".encode(), frame=frame,
        fetched_at="2026-09-01T10:00:00+00:00", headers={},
    )


def test_merge_fills_only_pending_availability_without_rewriting_receipts(tmp_path) -> None:
    known_day, latest_day, next_day = date(2026, 8, 31), date(2026, 9, 1), date(2026, 9, 2)
    known = _save_test_shard(tmp_path, known_day, latest_day)
    pending = _save_test_shard(tmp_path, latest_day, None)
    assert pq.read_schema(tmp_path / known["normalized_path"]).field("available_date").type == (
        pq.read_schema(tmp_path / pending["normalized_path"]).field("available_date").type
    )
    assert pending["availability_state"] == "waiting_verified_next_session"
    assert pending["availability_pending_rows"] == 1
    assert pending["completion_claim"] == "source_acquisition_not_full_pit_readiness"
    evidence = {
        path: path.read_bytes()
        for receipt in (known, pending)
        for path in (
            tmp_path / receipt["raw_path"], tmp_path / receipt["normalized_path"],
            collector._receipt_path(tmp_path, receipt["dataset"], receipt["request_key"]),
        )
    }

    first = collector._merge_dataset(tmp_path, "institutional_futures", {})
    assert first["availability_pending_rows"] == 1
    assert first["availability_aligned_rows"] == 1
    assert first["availability_state"] == "waiting_verified_next_session"
    # Even a conflicting map must not silently rewrite a date already certified
    # in its source shard.  Only a previously missing projection is filled.
    second = collector._merge_dataset(
        tmp_path, "institutional_futures", {known_day: next_day, latest_day: next_day}
    )
    frame = pq.read_table(tmp_path / second["output_path"]).to_pandas()
    assert frame["available_date"].dt.date.tolist() == [latest_day, next_day]
    assert second["availability_pending_rows"] == 0
    assert second["availability_state"] == "aligned"
    assert second["rows"] == 2
    assert all(path.read_bytes() == content for path, content in evidence.items())
    assert collector._valid_receipt(tmp_path, "institutional_futures", latest_day.isoformat())


def test_main_downloads_latest_session_and_reuses_completed_receipt(tmp_path, monkeypatch) -> None:
    calendar = tmp_path / "calendar.parquet"
    pd.DataFrame({"date": [pd.Timestamp("2026-09-01")]}).to_parquet(calendar)
    root = tmp_path / "output"
    spec = POSITIONING_SPECS[0]
    monkeypatch.setattr(collector, "POSITIONING_SPECS", (spec,))
    class NoopLimiter:
        def __init__(self, *args, **kwargs):
            pass

    monkeypatch.setattr(collector, "SharedRateLimiter", NoopLimiter)
    requested = []
    def post(session, limiter, url, payload, *, attempts):
        requested.append(payload["queryDate"])
        frame = pd.DataFrame([["1", "臺股期貨", "自營商", *range(1, 13)]])
        return _html_with_date(frame), "2026-09-01T10:00:00+00:00", {}

    monkeypatch.setattr(collector, "_post", post)
    monkeypatch.setattr(sys, "argv", [
        "download_taifex_public_history", "--output-dir", str(root),
        "--session-parquet", str(calendar), "--end-date", "2026-09-01",
        "--phase", "positioning",
    ])
    assert collector.main() == 0
    assert requested == ["2026/09/01"]
    manifest = json.loads((root / "manifest.json").read_text())
    assert manifest["status"] == "complete"
    assert manifest["availability_pending_rows"] == 1
    assert manifest["availability_state"] == "waiting_verified_next_session"
    assert manifest["datasets"][0]["last_date"] == "2026-09-01"
    assert manifest["completion_claim"] == "source_acquisition_not_full_pit_readiness"
    assert collector.main() == 0
    assert requested == ["2026/09/01"]
    pd.DataFrame({"date": pd.to_datetime(["2026-09-01", "2026-09-02"])}).to_parquet(calendar)
    assert collector.main() == 0
    assert requested == ["2026/09/01"]
    aligned = json.loads((root / "manifest.json").read_text())
    assert aligned["availability_pending_rows"] == 0
    assert aligned["availability_state"] == "aligned"
    assert aligned["datasets"][0]["last_date"] == "2026-09-01"


def test_put_call_trailing_observation_waits_for_calendar_only(tmp_path) -> None:
    day = date(2001, 12, 24)
    content = (
        "日期,賣權成交量,買權成交量,買賣權成交量比率%,賣權未平倉量,"
        "買權未平倉量,買賣權未平倉量比率%\r\n"
        "2001/12/24,275,1145,24.02,90,683,13.18,\r\n"
    ).encode("cp950")
    frame = _parse_put_call(content, {})
    collector._persist_response(
        tmp_path, dataset="put_call_ratio", key="2001-12", url=collector.PUT_CALL_URL,
        request_payload={}, content=content, frame=frame,
        fetched_at="2001-12-24T10:00:00+00:00", headers={},
    )
    result = collector._merge_dataset(tmp_path, "put_call_ratio", {day: date(2001, 12, 25)})
    output = pq.read_table(tmp_path / result["output_path"]).to_pandas()
    assert result["availability_pending_rows"] == 0
    assert output.loc[0, "available_date"].date() == date(2001, 12, 25)


def _range_csv(spec, days, *, missing_positions=False) -> bytes:
    options = spec.parser == "large_trader_options_csv"
    header = list(collector.LARGE_TRADER_CSV_HEADER)
    if options:
        header.insert(3, "買賣權")
    stream = StringIO()
    writer = csv.writer(stream)
    writer.writerow(header)
    for day in days:
        for category in ("0", "1"):
            row = [day.strftime("%Y/%m/%d"), " CA ", "南亞", "666666", category,
                   "-" if missing_positions else "4", "0", "4", "0", "4"]
            if options:
                row.insert(3, "買權")
            writer.writerow(row)
    writer.writerow([])
    writer.writerow(["月份類別格式: 666666為所有週到期契約合計，999999為所有契約合計。"])
    writer.writerow(["交易人類別格式： 0 為全體，1 為特定法人。"])
    writer.writerow(['-表無週契約；"0"表無未沖銷部位。'])
    return stream.getvalue().encode("cp950")


@pytest.mark.parametrize("spec", collector.LARGE_TRADER_RANGE_SPECS)
def test_range_csv_keeps_all_products_categories_raw_counts_and_missingness(spec) -> None:
    day = date(2026, 9, 1)
    frame = collector._parse_large_trader_csv(
        spec, _range_csv(spec, [day], missing_positions=True), day, day, {}
    )
    assert len(frame) == 2
    assert frame["product_code"].tolist() == ["CA", "CA"]
    assert set(frame["trader_category"]) == {"0", "1"}
    assert frame["buy_top5_positions"].isna().all()
    assert frame["sell_top5_positions"].eq(0).all()
    assert str(frame["market_open_interest"].dtype) == "Int64"
    assert frame["expiry_bucket"].eq("666666").all()
    assert frame["available_date"].isna().all()
    if "option_side" in frame:
        assert frame["option_side"].eq("買權").all()


def test_range_csv_rejects_html_truncation_wrong_dates_and_duplicate_grain() -> None:
    spec = collector.LARGE_TRADER_RANGE_SPECS[0]
    day = date(2026, 9, 1)
    with pytest.raises(collector.UnverifiedHistoryResponse, match="查無資料"):
        collector._parse_large_trader_csv(
            spec, '<html><script>alert("查無資料")</script></html>'.encode(), day, day, {}
        )
    with pytest.raises(ValueError, match="HTML"):
        collector._parse_large_trader_csv(spec, b"<!DOCTYPE html><body>maintenance</body>", day, day, {})
    with pytest.raises(ValueError, match="footer"):
        content = _range_csv(spec, [day]).decode("cp950").split("月份類別格式")[0].encode("cp950")
        collector._parse_large_trader_csv(spec, content, day, day, {})
    with pytest.raises(ValueError, match="outside"):
        collector._parse_large_trader_csv(spec, _range_csv(spec, [day]), date(2026, 9, 2), date(2026, 9, 2), {})
    with pytest.raises(ValueError, match="duplicate"):
        collector._parse_large_trader_csv(spec, _range_csv(spec, [day, day]), day, day, {})


def test_quarter_ranges_respect_three_calendar_month_limit_and_leap_day() -> None:
    assert list(collector._quarter_ranges(date(2023, 12, 20), date(2024, 4, 2))) == [
        (date(2023, 12, 20), date(2023, 12, 31)),
        (date(2024, 1, 1), date(2024, 3, 31)),
        (date(2024, 4, 1), date(2024, 4, 2)),
    ]


def test_range_history_reuses_observations_and_only_requests_incremental_sessions(tmp_path, monkeypatch) -> None:
    old = date(2004, 7, 1)
    days = [date(2026, 9, 1), date(2026, 9, 2)]
    calls = []
    def post(session, limiter, url, payload, *, attempts):
        calls.append((url, payload))
        spec = next(item for item in collector.LARGE_TRADER_RANGE_SPECS if item.url == url)
        start = date.fromisoformat(payload["queryStartDate"].replace("/", "-"))
        end = date.fromisoformat(payload["queryEndDate"].replace("/", "-"))
        content = ('<html><script>alert("查無資料")</script></html>'.encode() if start == old
                   else _range_csv(spec, [day for day in [*days, date(2026, 9, 3)] if start <= day <= end]))
        return content, "2026-09-02T10:00:00+00:00", {}
    monkeypatch.setattr(collector, "_post", post)
    def download(sessions, **kwargs):
        return collector._download_large_trader_ranges(
            tmp_path, None, None, _next_session_map(sessions), sessions,
            old, sessions[-1], 1, {}, **kwargs,
        )
    first = download([old, *days])
    assert len(calls) == 4
    assert all(item["status"] == "partial" and item["rows"] == 4 for item in first)
    assert all(item["missing_session_dates"] == [old.isoformat()] for item in first)
    statuses = [json.loads(path.read_text()) for path in (tmp_path / "range_status").glob("*/*/*.json")]
    missing_statuses = [item for item in statuses if item["status"] == "no_history_not_verified"]
    assert len(missing_statuses) == 2
    assert all((tmp_path / item["raw_path"]).is_file() for item in missing_statuses)
    assert all(not collector._receipt_path(tmp_path, item["dataset"], item["request_key"]).exists()
               for item in missing_statuses)
    second = download([old, *days])
    assert len(calls) == 4
    assert all(item["new_requests"] == 0 and item["deferred_session_count"] == 1 for item in second)
    third = download([old, *days, date(2026, 9, 3)])
    assert len(calls) == 6
    assert all(payload == {"queryStartDate": "2026/09/03", "queryEndDate": "2026/09/03"}
               for _, payload in calls[-2:])
    assert all(item["rows"] == 6 for item in third)
    download([old, *days, date(2026, 9, 3)], retry_unverified_hours=0)
    assert len(calls) == 8


def test_partial_csv_preserves_rows_but_missing_middle_session_is_not_complete(tmp_path, monkeypatch) -> None:
    spec = collector.LARGE_TRADER_RANGE_SPECS[0]
    monkeypatch.setattr(collector, "LARGE_TRADER_RANGE_SPECS", (spec,))
    days = [date(2026, 9, day) for day in (1, 2, 3)]
    calls = []
    def post(session, limiter, url, payload, *, attempts):
        calls.append(payload)
        selected = [days[0], days[2]] if len(calls) == 1 else [days[1]]
        return _range_csv(spec, selected), "2026-09-03T10:00:00+00:00", {}
    monkeypatch.setattr(collector, "_post", post)
    def download(**kwargs):
        return collector._download_large_trader_ranges(
            tmp_path, None, None, _next_session_map(days), days, days[0], days[-1], 1, {}, **kwargs
        )[0]
    first = download()
    assert first["status"] == "partial"
    assert first["rows"] == 4
    assert first["missing_session_dates"] == ["2026-09-02"]
    second = download(retry_unverified_hours=0)
    assert calls[-1] == {"queryStartDate": "2026/09/02", "queryEndDate": "2026/09/02"}
    assert second["status"] == "complete"
    assert second["rows"] == 6
    assert second["availability_pending_rows"] == 2
    assert second["product_universe_completeness"] == "not_independently_verified"


def test_range_failure_does_not_block_other_asset_class(tmp_path, monkeypatch) -> None:
    day = date(2026, 9, 1)
    def post(session, limiter, url, payload, *, attempts):
        if url == collector.LARGE_TRADER_RANGE_SPECS[0].url:
            raise TimeoutError("bounded request failed")
        return _range_csv(collector.LARGE_TRADER_RANGE_SPECS[1], [day]), "2026-09-01T10:00:00+00:00", {}
    monkeypatch.setattr(collector, "_post", post)
    summaries = collector._download_large_trader_ranges(
        tmp_path, None, None, {}, [day], day, day, 1, {},
    )
    assert summaries[0]["status"] == "partial"
    assert summaries[0]["rows"] == 0
    assert summaries[1]["status"] == "complete"
    assert summaries[1]["rows"] == 2


def test_range_cli_preserves_other_datasets_in_provider_manifest(tmp_path, monkeypatch) -> None:
    calendar = tmp_path / "calendar.parquet"
    pd.DataFrame({"date": [pd.Timestamp("2026-09-01")]}).to_parquet(calendar)
    root = tmp_path / "output"
    root.mkdir()
    old_summary = {"dataset": "institutional_futures", "status": "complete", "rows": 100}
    (root / "manifest.json").write_text(json.dumps({"datasets": [old_summary]}))
    monkeypatch.setattr(collector, "SharedRateLimiter", lambda *args, **kwargs: None)
    def post(session, limiter, url, payload, *, attempts):
        spec = next(item for item in collector.LARGE_TRADER_RANGE_SPECS if item.url == url)
        return _range_csv(spec, [date(2026, 9, 1)]), "2026-09-01T10:00:00+00:00", {}
    monkeypatch.setattr(collector, "_post", post)
    monkeypatch.setattr(sys, "argv", [
        "download_taifex_public_history", "--output-dir", str(root),
        "--session-parquet", str(calendar), "--end-date", "2026-09-01",
        "--large-trader-start", "2026-09-01", "--phase", "large-trader-range",
    ])
    assert collector.main() == 0
    manifest = json.loads((root / "manifest.json").read_text())
    assert old_summary in manifest["datasets"]
    assert manifest["status"] == "complete"
    assert manifest["availability_pending_rows"] == 4
    assert manifest["refreshed_datasets"] == [item.name for item in collector.LARGE_TRADER_RANGE_SPECS]


def test_range_merge_streams_and_rejects_overlapping_evidence_before_replace(tmp_path, monkeypatch) -> None:
    spec = collector.LARGE_TRADER_RANGE_SPECS[0]
    day, next_day = date(2026, 9, 1), date(2026, 9, 2)
    content = _range_csv(spec, [day])
    frame = collector._parse_large_trader_csv(spec, content, day, day, {})
    def persist(key):
        return collector._persist_response(
            tmp_path, dataset=spec.name, key=key, url=spec.url,
            request_payload={"queryStartDate": "2026/09/01", "queryEndDate": "2026/09/01"},
            content=content, frame=frame, fetched_at="2026-09-01T10:00:00+00:00", headers={},
            coverage={"observed_dates": [day.isoformat()]},
        )
    persist("2026-09-01_2026-09-01")
    def full_read_forbidden(*args, **kwargs):
        raise AssertionError("range merge must iterate Arrow batches, not load full Parquet tables")
    with monkeypatch.context() as patch:
        patch.setattr(collector.pq, "read_table", full_read_forbidden)
        result = collector._merge_dataset(tmp_path, spec.name, {day: next_day})
    target = tmp_path / result["output_path"]
    previous = target.read_bytes()
    assert result["merge_backend"] == "pyarrow_streaming_disjoint_observation_dates"
    assert result["availability_pending_rows"] == 0
    assert pq.read_table(target)["available_date"].to_pylist()[0].date() == next_day
    persist("2026-09-01_2026-09-02")
    with pytest.raises(RuntimeError, match="overlapping observed dates"):
        collector._merge_dataset(tmp_path, spec.name, {day: next_day})
    assert target.read_bytes() == previous


def _range_csv_with_absence_and_unknown(spec, day) -> bytes:
    records = list(csv.reader(StringIO(_range_csv(spec, [day]).decode("cp950"))))
    placeholder = [day.strftime("%Y/%m/%d"), "TX", "臺股期貨", "-", "-", *("-" for _ in range(5))]
    unknown = [day.strftime("%Y/%m/%d"), "CA", "南亞", "666666", "7", "1", "0", "1", "0", "4"]
    records[3:3] = [placeholder, placeholder, unknown]
    records[-1] = ['-表當日收盤後無週到期期貨契約；"0"表該契約於當日收盤後無未沖銷部位。']
    stream = StringIO()
    csv.writer(stream).writerows(records)
    return stream.getvalue().encode("cp950")


def test_range_parser_preserves_absence_rows_and_unknown_category_without_guessing() -> None:
    spec = collector.LARGE_TRADER_RANGE_SPECS[0]
    day = date(2026, 9, 1)
    frame = collector._parse_large_trader_csv(spec, _range_csv_with_absence_and_unknown(spec, day), day, day, {})
    assert len(frame) == 5
    assert frame["source_row_number"].nunique() == 5
    placeholders = frame[frame["row_quality"].eq("source_no_weekly_contract_placeholder")]
    assert len(placeholders) == 2
    assert placeholders["trader_category"].eq("-").all()
    assert placeholders["expiry_bucket"].eq("-").all()
    assert placeholders[list(collector.LARGE_TRADER_CSV_COLUMNS[5:])].isna().all().all()
    unknown = frame[frame["row_quality"].eq("unknown_trader_category")].iloc[0]
    assert unknown["trader_category"] == "7"
    assert unknown["buy_top5_positions"] == 1


def _save_failed_range_raw(root, spec, day, content, *, no_history=False):
    key = f"{day.isoformat()}_{day.isoformat()}"
    raw_path = root / "raw" / spec.name / str(day.year) / f"{key}_test.raw.gz"
    collector._atomic_write_bytes(raw_path, gzip.compress(content, mtime=0))
    status = {
        "dataset": spec.name, "request_key": key, "source_url": spec.url,
        "request_payload": {"queryStartDate": day.strftime("%Y/%m/%d"), "queryEndDate": day.strftime("%Y/%m/%d")},
        "status": "no_history_not_verified" if no_history else "failed",
        "error_type": "UnverifiedHistoryResponse" if no_history else "ValueError",
        "error": "official response says 查無資料" if no_history else "unknown large-trader category (official categories are 0 and 1)",
        "checked_at_utc": collector._utc_now(), "missing_session_dates": [day.isoformat()],
        "raw_path": collector._relative(raw_path, root), "raw_sha256": collector.sha256_path(raw_path),
    }
    collector._write_json(collector._range_status_path(root, spec.name, key), status)
    return status


def test_parser_upgrade_reuses_failed_raw_without_http_and_preserves_provenance(tmp_path, monkeypatch) -> None:
    spec = collector.LARGE_TRADER_RANGE_SPECS[0]
    monkeypatch.setattr(collector, "LARGE_TRADER_RANGE_SPECS", (spec,))
    day = date(2026, 9, 1)
    status = _save_failed_range_raw(tmp_path, spec, day, _range_csv_with_absence_and_unknown(spec, day))
    raw = tmp_path / status["raw_path"]
    previous = raw.read_bytes()
    monkeypatch.setattr(collector, "_post", lambda *args, **kwargs: pytest.fail("raw reparse must not call HTTP"))
    summaries = collector._download_large_trader_ranges(tmp_path, None, None, {}, [day], day, day, 1, {})
    summary = summaries[0]
    assert summary["rows"] == 5
    assert summary["new_requests"] == 0
    assert summary["offline_reparse_requests"] == 1
    assert summary["row_quality_counts"] == {"observed": 2, "source_no_weekly_contract_placeholder": 2, "unknown_trader_category": 1}
    assert summary["data_quality_state"] == "unmapped_source_categories"
    receipt = collector._valid_receipt(tmp_path, spec.name, status["request_key"])
    assert receipt["coverage"]["unknown_trader_categories"] == ["7"]
    assert receipt["coverage"]["recovery"]["original_fetch_time_evidence"] == "legacy_request_start_proxy"
    assert receipt["fetched_at_utc"] == status["checked_at_utc"]
    assert receipt["response_sha256"] == collector._sha256_bytes(gzip.decompress(previous))
    assert raw.read_bytes() == previous


def test_no_history_response_is_not_offline_replayed_and_can_be_refetched(tmp_path, monkeypatch) -> None:
    spec = collector.LARGE_TRADER_RANGE_SPECS[0]
    monkeypatch.setattr(collector, "LARGE_TRADER_RANGE_SPECS", (spec,))
    day = date(2026, 9, 1)
    status = _save_failed_range_raw(tmp_path, spec, day, '<html>查無資料</html>'.encode(), no_history=True)
    assert not collector._failed_range_is_reparseable(status)
    calls = []
    def post(*args, **kwargs):
        calls.append(True)
        return _range_csv(spec, [day]), collector._utc_now(), {}
    monkeypatch.setattr(collector, "_post", post)
    offline = collector._download_large_trader_ranges(
        tmp_path, None, None, {}, [day], day, day, 1, {}, reparse_failed_only=True,
    )[0]
    assert not calls
    assert offline["status"] == "partial"
    online = collector._download_large_trader_ranges(
        tmp_path, None, None, {}, [day], day, day, 1, {}, retry_unverified_hours=0,
    )[0]
    assert len(calls) == 1
    assert online["status"] == "complete"
    assert online["offline_reparse_requests"] == 0


def test_range_quality_schema_is_additive_for_old_verified_shards(tmp_path) -> None:
    spec = collector.LARGE_TRADER_RANGE_SPECS[0]
    days = [date(2026, 9, 1), date(2026, 9, 2)]
    for index, day in enumerate(days):
        content = _range_csv(spec, [day]) if index == 0 else _range_csv_with_absence_and_unknown(spec, day)
        frame = collector._parse_large_trader_csv(spec, content, day, day, {})
        if index == 0:
            frame = frame.drop(columns=["source_row_number", "row_quality"])
        collector._persist_response(
            tmp_path, dataset=spec.name, key=f"{day}_{day}", url=spec.url,
            request_payload={}, content=content, frame=frame, fetched_at=collector._utc_now(), headers={},
            coverage={"observed_dates": [day.isoformat()]},
        )
    summary = collector._merge_dataset(tmp_path, spec.name, {days[0]: days[1]})
    output = pq.read_table(tmp_path / summary["output_path"]).to_pandas()
    assert summary["rows"] == 7
    assert summary["row_quality_counts"]["observed"] == 4
    assert output.loc[output["date"].dt.date.eq(days[0]), "source_row_number"].isna().all()
    assert output.loc[output["date"].dt.date.eq(days[1]), "source_row_number"].notna().all()


def test_offline_range_cli_reports_partial_with_nonzero_exit(tmp_path, monkeypatch) -> None:
    calendar = tmp_path / "calendar.parquet"
    pd.DataFrame({"date": [pd.Timestamp("2026-09-01")]}).to_parquet(calendar)
    root = tmp_path / "output"
    monkeypatch.setattr(collector, "SharedRateLimiter", lambda *args, **kwargs: None)
    monkeypatch.setattr(collector, "_post", lambda *args, **kwargs: pytest.fail("offline CLI must not call HTTP"))
    monkeypatch.setattr(sys, "argv", [
        "download_taifex_public_history", "--output-dir", str(root),
        "--session-parquet", str(calendar), "--end-date", "2026-09-01",
        "--large-trader-start", "2026-09-01", "--phase", "large-trader-range",
        "--reparse-failed-ranges-only",
    ])
    assert collector.main() == 1
    assert json.loads((root / "manifest.json").read_text())["status"] == "partial"
    assert json.loads((root / "progress.json").read_text())["state"] == "partial"


def test_official_history_start_clamps_earlier_requests_without_false_gaps(tmp_path, monkeypatch) -> None:
    spec = collector.LARGE_TRADER_RANGE_SPECS[0]
    monkeypatch.setattr(collector, "LARGE_TRADER_RANGE_SPECS", (spec,))
    day = collector.LARGE_TRADER_HISTORY_START
    sessions = [date(1998, 7, 21), date(2004, 6, 30), day]
    _save_failed_range_raw(tmp_path, spec, sessions[0], b"<html>no history</html>", no_history=True)
    calls = []
    def post(session, limiter, url, payload, **kwargs):
        calls.append(payload)
        return _range_csv(spec, [day]), collector._utc_now(), {}
    monkeypatch.setattr(collector, "_post", post)
    summary = collector._download_large_trader_ranges(
        tmp_path, None, None, {}, sessions, sessions[0], day, 1, {}, retry_unverified_hours=0,
    )[0]
    assert calls == [{"queryStartDate": "2004/07/01", "queryEndDate": "2004/07/01"}]
    assert summary["requested_start_date"] == "1998-07-21"
    assert summary["effective_source_start_date"] == "2004-07-01"
    assert summary["source_not_supported"] == {
        "start_date": "1998-07-21", "end_date": "2004-06-30",
        "reason": "before_official_history_start", "verified_session_count": 2,
    }
    assert summary["status"] == "complete"
    assert summary["missing_session_count"] == 0
    assert summary["expected_session_count"] == 1
    assert collector.LARGE_TRADER_HISTORY_SOURCES[1] in summary["source_history_evidence_urls"]


def test_prelaunch_parser_waits_for_first_publication_not_observation_next_session() -> None:
    spec = collector.LARGE_TRADER_RANGE_SPECS[0]
    day = date(2004, 7, 1)
    calendar = {day: date(2004, 7, 2), date(2005, 1, 3): date(2005, 1, 4)}
    frame = collector._parse_large_trader_csv(spec, _range_csv(spec, [day]), day, day, calendar)
    assert frame["available_date"].dt.date.eq(date(2005, 1, 4)).all()
    assert frame["availability_rule"].eq(collector.LARGE_TRADER_LAUNCH_AVAILABILITY_RULE).all()
    pending = collector._parse_large_trader_csv(spec, _range_csv(spec, [day]), day, day, {day: date(2004, 7, 2)})
    assert pending["available_date"].isna().all()


@pytest.mark.parametrize("with_launch_calendar", [True, False])
def test_merge_corrects_prelaunch_dates_without_modifying_old_receipts_or_later_dates(tmp_path, with_launch_calendar) -> None:
    spec = collector.LARGE_TRADER_RANGE_SPECS[0]
    day = date(2004, 7, 1)
    content = _range_csv(spec, [day])
    frame = collector._parse_large_trader_csv(spec, content, day, day, {})
    # Reproduce old receipt-backed output: the first row is now disproven,
    # while the second had a later safe date that must not be moved earlier.
    frame["available_date"] = pd.Series(pd.to_datetime(["2004-07-02", "2005-01-05"]), dtype="datetime64[ms]")
    frame["availability_rule"] = "next_receipt_verified_taifex_session"
    receipt = collector._persist_response(
        tmp_path, dataset=spec.name, key=f"{day}_{day}", url=spec.url,
        request_payload={}, content=content, frame=frame, fetched_at=collector._utc_now(), headers={},
        coverage={"observed_dates": [day.isoformat()]},
    )
    evidence = {tmp_path / receipt[key]: (tmp_path / receipt[key]).read_bytes()
                for key in ("raw_path", "normalized_path")}
    receipt_path = collector._receipt_path(tmp_path, spec.name, receipt["request_key"])
    evidence[receipt_path] = receipt_path.read_bytes()
    calendar = {day: date(2004, 7, 2)}
    if with_launch_calendar:
        calendar[date(2005, 1, 3)] = date(2005, 1, 4)
    summary = collector._merge_dataset(tmp_path, spec.name, calendar)
    output = pq.read_table(tmp_path / summary["output_path"]).to_pandas()
    if with_launch_calendar:
        assert output.loc[0, "available_date"].date() == date(2005, 1, 4)
    else:
        assert pd.isna(output.loc[0, "available_date"])
    assert output.loc[1, "available_date"].date() == date(2005, 1, 5)
    assert output["availability_rule"].eq(collector.LARGE_TRADER_LAUNCH_AVAILABILITY_RULE).all()
    assert summary["source_rule_corrected_rows"] == 1
    assert summary["first_publication_floor_rows"] == 2
    assert summary["availability_alignment_version"] == 3
    assert all(path.read_bytes() == previous for path, previous in evidence.items())


@pytest.mark.parametrize("phase", ["put-call", "positioning", "large-trader-range", "all"])
@pytest.mark.parametrize("current_status,retained_status", [("complete", "partial"), ("partial", "complete")])
def test_phase_exit_uses_refreshed_scope_but_manifest_retains_global_health(
    tmp_path, monkeypatch, phase, current_status, retained_status,
) -> None:
    calendar = tmp_path / "calendar.parquet"
    pd.DataFrame({"date": [pd.Timestamp("2026-09-01")]}).to_parquet(calendar)
    root = tmp_path / "output"
    root.mkdir()
    retained_name = "large_trader_futures_all" if phase in {"put-call", "positioning"} else "previous_other_dataset"
    retained = {"dataset": retained_name, "status": retained_status, "rows": 100}
    (root / "manifest.json").write_text(json.dumps({"datasets": [retained]}))
    monkeypatch.setattr(collector, "SharedRateLimiter", lambda *args, **kwargs: None)
    monkeypatch.setattr(collector, "_download_put_call", lambda *args, **kwargs: None)
    monkeypatch.setattr(collector, "_download_positioning", lambda *args, **kwargs: None)
    def summary(dataset):
        return {"dataset": dataset, "status": current_status, "rows": 2, "availability_pending_rows": 0}
    monkeypatch.setattr(collector, "_merge_dataset", lambda root, dataset, next_sessions: summary(dataset))
    monkeypatch.setattr(collector, "_download_large_trader_ranges", lambda *args, **kwargs: [
        summary(spec.name) for spec in collector.LARGE_TRADER_RANGE_SPECS
    ])
    monkeypatch.setattr(sys, "argv", [
        "download_taifex_public_history", "--output-dir", str(root),
        "--session-parquet", str(calendar), "--end-date", "2026-09-01", "--phase", phase,
    ])
    expected_exit = 1 if phase == "all" or current_status == "partial" else 0
    assert collector.main() == expected_exit
    manifest = json.loads((root / "manifest.json").read_text())
    progress = json.loads((root / "progress.json").read_text())
    assert manifest["status"] == "partial"
    assert retained in manifest["datasets"]
    assert progress["state"] == "partial"
    assert manifest["phase_execution_scope"] == phase
    assert manifest["phase_execution_status"] == ("partial" if expected_exit else "complete")
    assert progress["phase_execution_status"] == manifest["phase_execution_status"]
