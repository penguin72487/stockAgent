from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import threading
from types import SimpleNamespace

import polars as pl
import pytest

from downloader import download_tw_public_data as twpub


def _historical_args(
    mode: str,
    *,
    end_date: str = "2024-01-04",
    require_taiex_session_calendar: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        mode=mode,
        start_date="earliest",
        end_date=end_date,
        include_weekends=False,
        max_dates=None,
        refresh=False,
        empty_recheck_days=0,
        daily_overlap_days=2,
        flush_every_dates=0,
        date_workers=2,
        progress=False,
        timeout=1,
        verify_ssl=True,
        retries=0,
        retry_backoff=0.0,
        skip_raw=True,
        sleep=0.0,
        resume=True,
        require_taiex_session_calendar=require_taiex_session_calendar,
    )


def _historical_spec() -> twpub.DatasetSpec:
    return twpub.DatasetSpec(
        name="sample_history",
        kind="historical_json_table",
        source="official-test",
        description="sample",
        tags=("test",),
        url_template="https://example.test/{date}",
        start_date="2024-01-02",
    )


def test_parse_json_table_payload_keeps_stock_codes_as_strings():
    spec = twpub.DatasetSpec(
        name="sample",
        kind="historical_json_table",
        source="TWSE",
        description="sample",
        tags=("test",),
    )
    payload = {
        "stat": "OK",
        "title": "sample",
        "fields": ["證券代號", "買進", "買進"],
        "data": [
            ["0050", "1,000", "2,000"],
            ["1101", "3", "4"],
        ],
    }

    frame = twpub._parse_json_table_payload(payload, spec, date(2024, 6, 3))

    assert frame["證券代號"].to_list() == ["0050", "1101"]
    assert "買進_2" in frame.columns
    assert frame.schema["證券代號"] == pl.String
    assert frame["date"].to_list() == ["2024-06-03", "2024-06-03"]


def test_download_modes_have_stable_legacy_aliases():
    assert twpub._canonical_mode("from-zero") == "rebuild"
    assert twpub._canonical_mode("full") == "repair"
    assert twpub._canonical_mode("daily-update") == "daily"


@pytest.mark.parametrize(
    "dataset,expected_version",
    [
        ("twse_daily_ohlcv", 7),
        ("twse_market_index", 7),
        ("twse_margin_balance", 5),
        ("twse_institutional_trades", 5),
        ("twse_daily_valuation", 5),
        ("tpex_daily_ohlcv", 12),
        ("tpex_margin_balance", 8),
        ("tpex_institutional_trades", 6),
        ("tpex_daily_valuation", 7),
    ],
)
def test_historical_parser_contract_is_versioned_per_dataset(
    dataset: str,
    expected_version: int,
):
    spec = twpub.DEFAULT_DATASETS[dataset]

    assert twpub._historical_parser_contract_version(spec) == expected_version


def test_unlisted_historical_dataset_uses_latest_parser_contract():
    assert (
        twpub._historical_parser_contract_version(_historical_spec())
        == twpub.HISTORICAL_PARSER_CONTRACT_VERSION
        == 11
    )


@pytest.mark.skipif(twpub.fcntl is None, reason="requires POSIX flock")
def test_historical_dataset_stage_lock_rejects_concurrent_writer(tmp_path: Path):
    spec = twpub.DEFAULT_DATASETS["tpex_daily_ohlcv"]

    with twpub._historical_dataset_lock(tmp_path, spec):
        with pytest.raises(RuntimeError, match="another process"):
            with twpub._historical_dataset_lock(tmp_path, spec):
                pass


def test_twse_mi_index_semantic_bumps_quarantine_only_affected_partials():
    def key_at_version(spec: twpub.DatasetSpec, version: int) -> str:
        payload = {
            "journal_schema_version": twpub.HISTORICAL_JOURNAL_SCHEMA_VERSION,
            "parser_contract_version": version,
            "spec": asdict(spec),
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()[:16]

    twse = twpub.DEFAULT_DATASETS["twse_daily_ohlcv"]
    twse_index = twpub.DEFAULT_DATASETS["twse_market_index"]
    twse_margin = twpub.DEFAULT_DATASETS["twse_margin_balance"]
    tpex = twpub.DEFAULT_DATASETS["tpex_daily_ohlcv"]

    assert key_at_version(twse, 5) == "74ab358560b05834"
    assert twpub._historical_resume_cache_key(twse) == key_at_version(twse, 7)
    assert twpub._historical_resume_cache_key(twse) != key_at_version(twse, 6)
    assert twpub._historical_resume_cache_key(twse_index) == key_at_version(
        twse_index, 7
    )
    assert twpub._historical_resume_cache_key(twse_index) != key_at_version(
        twse_index, 6
    )
    assert twpub._historical_resume_cache_key(twse_margin) == key_at_version(
        twse_margin, 5
    )
    assert twpub._historical_resume_cache_key(tpex) == key_at_version(tpex, 12)
    assert twpub._historical_resume_cache_key(tpex) != key_at_version(tpex, 11)


def test_repair_fetches_only_missing_weekdays(tmp_path: Path, monkeypatch):
    spec = _historical_spec()
    output_path = tmp_path / f"{spec.name}.parquet"
    pl.DataFrame(
        {"date": ["2024-01-02", "2024-01-04"], "value": ["old-2", "old-4"]}
    ).write_parquet(output_path)
    requested: list[date] = []

    def fake_download(spec, day, args, output_dir):
        requested.append(day)
        return twpub.HistoricalDateResult(
            day=day,
            url=f"https://example.test/{day}",
            frame=pl.DataFrame({"date": [day.isoformat()], "value": ["repaired"]}),
        )

    monkeypatch.setattr(twpub, "_download_historical_date", fake_download)
    result = twpub._download_historical(spec, _historical_args("repair"), tmp_path)

    assert requested == [date(2024, 1, 3)]
    assert result.status == "ok"
    assert result.coverage_complete is True
    assert result.missing_dates_before == 1
    assert pl.read_parquet(output_path).get_column("date").to_list() == [
        "2024-01-02",
        "2024-01-03",
        "2024-01-04",
    ]
    state = json.loads((tmp_path / "state" / f"{spec.name}.json").read_text())
    assert state["baseline_established"] is True
    assert state["checked_through"] == "2024-01-04"


def test_rebuild_keeps_previous_parquet_when_any_date_fails(tmp_path: Path, monkeypatch):
    spec = _historical_spec()
    output_path = tmp_path / f"{spec.name}.parquet"
    original = pl.DataFrame({"date": ["2024-01-02"], "value": ["production"]})
    original.write_parquet(output_path)

    def fake_download(spec, day, args, output_dir):
        if day == date(2024, 1, 3):
            return twpub.HistoricalDateResult(
                day=day,
                url="https://example.test/failure",
                frame=pl.DataFrame(),
                error="simulated failure",
            )
        return twpub.HistoricalDateResult(
            day=day,
            url=f"https://example.test/{day}",
            frame=pl.DataFrame({"date": [day.isoformat()], "value": ["staged"]}),
        )

    monkeypatch.setattr(twpub, "_download_historical_date", fake_download)
    result = twpub._download_historical(spec, _historical_args("rebuild"), tmp_path)

    assert result.status == "failed"
    assert result.failed_dates == 1
    assert pl.read_parquet(output_path).to_dicts() == original.to_dicts()
    assert list(tmp_path.glob("*.rebuild.parquet")) == []
    cache_key = twpub._historical_resume_cache_key(spec)
    partial_path = twpub._historical_partial_path(tmp_path, spec, cache_key)
    assert partial_path.exists()
    journal_path = twpub._historical_journal_path(tmp_path, spec)
    events = [json.loads(line) for line in journal_path.read_text().splitlines()]
    assert {event["status"] for event in events} == {"data", "failed"}


def test_rebuild_resume_retries_only_unresolved_dates(tmp_path: Path, monkeypatch):
    spec = _historical_spec()
    output_path = tmp_path / f"{spec.name}.parquet"
    original = pl.DataFrame({"date": ["2024-01-02"], "value": ["production"]})
    original.write_parquet(output_path)
    first_requested: list[date] = []

    def first_download(spec, day, args, output_dir):
        first_requested.append(day)
        if day == date(2024, 1, 3):
            return twpub.HistoricalDateResult(
                day=day,
                url=f"https://example.test/{day}",
                frame=pl.DataFrame(),
                error="temporary failure",
            )
        return twpub.HistoricalDateResult(
            day=day,
            url=f"https://example.test/{day}",
            frame=pl.DataFrame({"date": [day.isoformat()], "value": [f"first-{day.day}"]}),
        )

    monkeypatch.setattr(twpub, "_download_historical_date", first_download)
    first = twpub._download_historical(spec, _historical_args("rebuild"), tmp_path)

    assert first.status == "failed"
    assert first.coverage_complete is False
    assert first.missing_dates_after == 1
    assert sorted(first_requested) == [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
    ]
    assert pl.read_parquet(output_path).to_dicts() == original.to_dicts()

    second_requested: list[date] = []

    def second_download(spec, day, args, output_dir):
        second_requested.append(day)
        return twpub.HistoricalDateResult(
            day=day,
            url=f"https://example.test/{day}",
            frame=pl.DataFrame({"date": [day.isoformat()], "value": ["recovered"]}),
        )

    monkeypatch.setattr(twpub, "_download_historical_date", second_download)
    second = twpub._download_historical(spec, _historical_args("rebuild"), tmp_path)

    assert second_requested == [date(2024, 1, 3)]
    assert second.status == "ok"
    assert second.coverage_complete is True
    assert second.missing_dates_after == 0
    assert pl.read_parquet(output_path).get_column("date").to_list() == [
        "2024-01-02",
        "2024-01-03",
        "2024-01-04",
    ]
    state = json.loads((tmp_path / "state" / f"{spec.name}.json").read_text())
    assert state["failed_dates"] == {}
    assert state["replacement_promoted"] is True


def test_rebuild_resume_persists_confirmed_empty_dates(tmp_path: Path, monkeypatch):
    spec = _historical_spec()

    def first_download(spec, day, args, output_dir):
        if day == date(2024, 1, 3):
            return twpub.HistoricalDateResult(
                day=day,
                url=f"https://example.test/{day}",
                frame=pl.DataFrame(),
            )
        if day == date(2024, 1, 4):
            return twpub.HistoricalDateResult(
                day=day,
                url=f"https://example.test/{day}",
                frame=pl.DataFrame(),
                error="temporary failure",
            )
        return twpub.HistoricalDateResult(
            day=day,
            url=f"https://example.test/{day}",
            frame=pl.DataFrame({"date": [day.isoformat()], "value": ["data"]}),
        )

    monkeypatch.setattr(twpub, "_download_historical_date", first_download)
    first = twpub._download_historical(spec, _historical_args("rebuild"), tmp_path)
    assert first.status == "failed"

    requested: list[date] = []

    def second_download(spec, day, args, output_dir):
        requested.append(day)
        return twpub.HistoricalDateResult(
            day=day,
            url=f"https://example.test/{day}",
            frame=pl.DataFrame({"date": [day.isoformat()], "value": ["data"]}),
        )

    monkeypatch.setattr(twpub, "_download_historical_date", second_download)
    second = twpub._download_historical(spec, _historical_args("rebuild"), tmp_path)

    assert requested == [date(2024, 1, 4)]
    assert second.coverage_complete is True
    state = json.loads((tmp_path / "state" / f"{spec.name}.json").read_text())
    assert state["confirmed_empty_dates"] == ["2024-01-03"]


def test_rebuild_bootstraps_existing_atomic_raw_receipt(tmp_path: Path, monkeypatch):
    spec = _historical_spec()
    raw_dir = tmp_path / "raw" / spec.name
    raw_dir.mkdir(parents=True)
    (raw_dir / "2024-01-02.json").write_text(
        json.dumps(
            {
                "stat": "OK",
                "fields": ["value"],
                "data": [["from-raw"]],
            }
        ),
        encoding="utf-8",
    )
    requested: list[date] = []

    def fake_download(spec, day, args, output_dir):
        requested.append(day)
        return twpub.HistoricalDateResult(
            day=day,
            url=f"https://example.test/{day}",
            frame=pl.DataFrame({"date": [day.isoformat()], "value": ["network"]}),
        )

    monkeypatch.setattr(twpub, "_download_historical_date", fake_download)
    result = twpub._download_historical(spec, _historical_args("rebuild"), tmp_path)

    assert requested == [date(2024, 1, 3), date(2024, 1, 4)]
    assert result.coverage_complete is True
    frame = pl.read_parquet(tmp_path / f"{spec.name}.parquet")
    assert frame.filter(pl.col("date") == "2024-01-02")["value"].to_list() == [
        "from-raw"
    ]


def test_v7_rebuild_refetches_only_retitled_stale_mi_index_raw_receipt(
    tmp_path: Path,
    monkeypatch,
):
    spec = twpub.DatasetSpec(
        name="twse_market_index",
        kind="historical_json_table",
        source="TWSE",
        description="bounded market-index contract test",
        tags=("twse", "index"),
        url_template=(
            "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
            "?date={date}&type=IND&response=json"
        ),
        date_format="%Y%m%d",
        start_date="2024-01-02",
        table_mode="title_contains:指數",
    )
    raw_dir = tmp_path / "raw" / spec.name
    raw_dir.mkdir(parents=True)

    def raw_payload(day: date, *, top_level_date: str, close: str) -> str:
        return json.dumps(
            {
                "stat": "OK",
                "date": top_level_date,
                "params": {"date": top_level_date},
                "tables": [
                    {
                        "title": (
                            f"{day.year - 1911:03d}年{day.month:02d}月"
                            f"{day.day:02d}日 價格指數(臺灣證券交易所)"
                        ),
                        "fields": ["指數", "收盤指數"],
                        "data": [["發行量加權股價指數", close]],
                    }
                ],
            },
            ensure_ascii=False,
        )

    (raw_dir / "2024-01-02.json").write_text(
        raw_payload(date(2024, 1, 2), top_level_date="20240102", close="100.00"),
        encoding="utf-8",
    )
    (raw_dir / "2024-01-03.json").write_text(
        raw_payload(date(2024, 1, 3), top_level_date="20171218", close="999.00"),
        encoding="utf-8",
    )
    (raw_dir / "2024-01-04.json").write_text(
        raw_payload(date(2024, 1, 4), top_level_date="20240104", close="102.00"),
        encoding="utf-8",
    )
    requested: list[date] = []

    def fake_download(spec, day, args, output_dir):
        requested.append(day)
        return twpub.HistoricalDateResult(
            day=day,
            url=f"https://example.test/{day:%Y%m%d}",
            frame=pl.DataFrame(
                {
                    "date": [day.isoformat()],
                    "指數": ["發行量加權股價指數"],
                    "收盤指數": ["101.00"],
                }
            ),
        )

    monkeypatch.setattr(twpub, "_download_historical_date", fake_download)
    result = twpub._download_historical(
        spec,
        _historical_args("rebuild"),
        tmp_path,
    )

    assert requested == [date(2024, 1, 3)]
    assert result.coverage_complete is True
    output = pl.read_parquet(tmp_path / "twse_market_index.parquet").sort("date")
    assert output.select("date", "收盤指數").to_dicts() == [
        {"date": "2024-01-02", "收盤指數": "100.00"},
        {"date": "2024-01-03", "收盤指數": "101.00"},
        {"date": "2024-01-04", "收盤指數": "102.00"},
    ]


@pytest.mark.parametrize("tamper_receipt", [False, True])
def test_rebuild_reparses_only_verified_failed_receipt_after_contract_bump(
    tmp_path: Path,
    monkeypatch,
    tamper_receipt: bool,
):
    spec = _historical_spec()
    day = date(2024, 1, 2)
    content = json.dumps(
        {
            "stat": "OK",
            "fields": ["value"],
            "data": [["from-failed-receipt"]],
        }
    ).encode("utf-8")
    digest = hashlib.sha256(content).hexdigest()
    raw_path = twpub._write_immutable_raw(
        content,
        tmp_path / "raw_failures" / spec.name,
        spec.name,
        ".json",
        stem=f"{day.isoformat()}.{digest[:16]}",
    )
    old_cache = twpub.HistoricalResumeCache(
        cache_key="old-parser-contract",
        journal_path=twpub._historical_journal_path(tmp_path, spec),
        partial_path=tmp_path / "unused-old-partial.parquet",
        data_dates=set(),
        empty_dates=set(),
    )
    twpub._append_historical_journal_record(
        old_cache,
        spec,
        twpub.HistoricalDateResult(
            day=day,
            url="https://example.test/20240102?_=123",
            frame=pl.DataFrame(),
            raw_path=str(raw_path),
            error="rejected by old parser contract",
            http_status=200,
            content_type="application/json",
            content_length=len(content),
            body_sha256=digest,
            body_snippet="valid official payload",
            response_attempts=2,
        ),
        status="failed",
        source="network",
    )
    if tamper_receipt:
        raw_path.write_bytes(content + b" ")

    requested: list[date] = []

    def fake_download(spec, day, args, output_dir):
        requested.append(day)
        return twpub.HistoricalDateResult(
            day=day,
            url=f"https://example.test/{day:%Y%m%d}",
            frame=pl.DataFrame(
                {"date": [day.isoformat()], "value": ["network"]}
            ),
        )

    monkeypatch.setattr(twpub, "_download_historical_date", fake_download)
    result = twpub._download_historical(
        spec,
        _historical_args("rebuild"),
        tmp_path,
    )

    expected_requested = (
        [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
        if tamper_receipt
        else [date(2024, 1, 3), date(2024, 1, 4)]
    )
    assert requested == expected_requested
    assert result.coverage_complete is True
    frame = pl.read_parquet(tmp_path / f"{spec.name}.parquet")
    expected_value = "network" if tamper_receipt else "from-failed-receipt"
    assert frame.filter(pl.col("date") == day.isoformat())["value"].to_list() == [
        expected_value
    ]
    current_events = [
        json.loads(line)
        for line in old_cache.journal_path.read_text(encoding="utf-8").splitlines()
        if json.loads(line).get("cache_key")
        == twpub._historical_resume_cache_key(spec)
    ]
    reparsed = [
        event
        for event in current_events
        if event.get("source") == "raw_failure_reparse"
    ]
    if tamper_receipt:
        assert reparsed == []
    else:
        assert len(reparsed) == 1
        assert reparsed[0]["date"] == day.isoformat()
        assert reparsed[0]["raw_sha256"] == digest
        assert reparsed[0]["body_sha256"] == digest


def test_rebuild_max_dates_keeps_partial_without_publishing(tmp_path: Path, monkeypatch):
    spec = _historical_spec()
    output_path = tmp_path / f"{spec.name}.parquet"
    pl.DataFrame({"date": ["2024-01-02"], "value": ["production"]}).write_parquet(
        output_path
    )
    requested: list[date] = []

    def fake_download(spec, day, args, output_dir):
        requested.append(day)
        return twpub.HistoricalDateResult(
            day=day,
            url=f"https://example.test/{day}",
            frame=pl.DataFrame({"date": [day.isoformat()], "value": ["staged"]}),
        )

    monkeypatch.setattr(twpub, "_download_historical_date", fake_download)
    limited_args = _historical_args("rebuild")
    limited_args.max_dates = 1
    first = twpub._download_historical(spec, limited_args, tmp_path)

    assert requested == [date(2024, 1, 2)]
    assert first.status == "incomplete"
    assert first.missing_dates_after == 2
    assert pl.read_parquet(output_path)["value"].to_list() == ["production"]

    requested.clear()
    second = twpub._download_historical(spec, _historical_args("rebuild"), tmp_path)
    assert sorted(requested) == [date(2024, 1, 3), date(2024, 1, 4)]
    assert second.coverage_complete is True


def test_no_resume_deliberately_refetches_resolved_dates(tmp_path: Path, monkeypatch):
    spec = _historical_spec()
    requested: list[date] = []

    def fake_download(spec, day, args, output_dir):
        requested.append(day)
        return twpub.HistoricalDateResult(
            day=day,
            url=f"https://example.test/{day}",
            frame=pl.DataFrame({"date": [day.isoformat()], "value": ["data"]}),
        )

    monkeypatch.setattr(twpub, "_download_historical_date", fake_download)
    first = twpub._download_historical(spec, _historical_args("rebuild"), tmp_path)
    assert first.coverage_complete is True

    requested.clear()
    fresh_args = _historical_args("rebuild")
    fresh_args.resume = False
    second = twpub._download_historical(spec, fresh_args, tmp_path)
    assert sorted(requested) == [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
    ]
    assert second.coverage_complete is True


def test_historical_journal_tolerates_only_a_torn_final_line(tmp_path: Path):
    spec = _historical_spec()
    cache_key = twpub._historical_resume_cache_key(spec)
    path = twpub._historical_journal_path(tmp_path, spec)
    path.parent.mkdir(parents=True)
    valid = {
        "schema_version": twpub.HISTORICAL_JOURNAL_SCHEMA_VERSION,
        "cache_key": cache_key,
        "dataset": spec.name,
        "date": "2024-01-02",
        "status": "empty",
    }
    path.write_text(json.dumps(valid) + "\n{" , encoding="utf-8")
    latest = twpub._load_historical_journal_latest(path, spec, cache_key)
    assert latest[date(2024, 1, 2)]["status"] == "empty"

    path.write_text("{\n" + json.dumps(valid) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="non-terminal JSONL"):
        twpub._load_historical_journal_latest(path, spec, cache_key)

    path.write_text(json.dumps(valid) + "\n{\n", encoding="utf-8")
    with pytest.raises(ValueError, match="non-terminal JSONL"):
        twpub._load_historical_journal_latest(path, spec, cache_key)


def test_repair_does_not_accept_empty_response_for_suspicious_existing_date(
    tmp_path: Path, monkeypatch
):
    spec = twpub.DatasetSpec(
        name="twse_daily_ohlcv",
        kind="historical_json_table",
        source="TWSE",
        description="sample",
        tags=("test",),
        url_template="https://example.test/{date}",
        start_date="2024-01-02",
    )
    pl.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-02"],
            "證券代號": ["2330", "2330"],
            "成交股數": ["1,000", "1,000"],
            "開盤價": ["100", "100"],
            "最高價": ["101", "101"],
            "最低價": ["99", "99"],
            "收盤價": ["100", "100"],
        }
    ).write_parquet(tmp_path / "twse_daily_ohlcv.parquet")

    def empty_download(spec, day, args, output_dir):
        return twpub.HistoricalDateResult(
            day=day,
            url="https://example.test/empty",
            frame=pl.DataFrame(),
        )

    monkeypatch.setattr(twpub, "_download_historical_date", empty_download)
    result = twpub._download_historical(
        spec,
        _historical_args("repair", end_date="2024-01-02"),
        tmp_path,
    )

    assert result.status == "failed"
    assert result.coverage_complete is False
    assert result.failed_dates == 1


def test_daily_requires_a_completed_rebuild_or_repair(tmp_path: Path):
    spec = _historical_spec()
    pl.DataFrame({"date": ["2024-01-04"], "value": ["partial"]}).write_parquet(
        tmp_path / f"{spec.name}.parquet"
    )

    with pytest.raises(RuntimeError, match="complete baseline"):
        twpub._plan_historical_download(
            spec,
            _historical_args("daily", end_date="2024-01-05"),
            tmp_path,
        )


def test_daily_refreshes_overlap_and_appends_after_verified_baseline(tmp_path: Path):
    spec = _historical_spec()
    pl.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03", "2024-01-04"],
            "value": ["2", "3", "4"],
        }
    ).write_parquet(tmp_path / f"{spec.name}.parquet")
    state_path = tmp_path / "state" / f"{spec.name}.json"
    state_path.parent.mkdir()
    state_path.write_text(
        json.dumps(
            {
                "schema_version": twpub.COVERAGE_STATE_SCHEMA_VERSION,
                "dataset": spec.name,
                "baseline_established": True,
                "coverage_complete": True,
                "checked_through": "2024-01-04",
                "confirmed_empty_dates": [],
                "failed_dates": {},
            }
        ),
        encoding="utf-8",
    )

    plan = twpub._plan_historical_download(
        spec,
        _historical_args("daily", end_date="2024-01-05"),
        tmp_path,
    )

    assert plan.dates == [date(2024, 1, 4), date(2024, 1, 5)]


def test_repair_trusts_confirmed_historical_holidays(tmp_path: Path):
    spec = _historical_spec()
    pl.DataFrame(
        {"date": ["2024-01-02", "2024-01-04"], "value": ["2", "4"]}
    ).write_parquet(tmp_path / f"{spec.name}.parquet")
    state_path = tmp_path / "state" / f"{spec.name}.json"
    state_path.parent.mkdir()
    state_path.write_text(
        json.dumps(
            {
                "schema_version": twpub.COVERAGE_STATE_SCHEMA_VERSION,
                "dataset": spec.name,
                "confirmed_empty_dates": ["2024-01-03"],
                "failed_dates": {},
            }
        ),
        encoding="utf-8",
    )

    plan = twpub._plan_historical_download(spec, _historical_args("repair"), tmp_path)

    assert plan.dates == []


def test_official_status_distinguishes_no_data_from_server_errors():
    assert twpub._json_payload_status_error({"stat": "很抱歉，沒有符合條件的資料!"}) is None
    assert twpub._json_payload_status_error({"stat": "OK"}) is None
    assert twpub._json_payload_status_error({"stat": "系統忙碌，請稍後再試"}) == "系統忙碌，請稍後再試"


def test_historical_parser_requires_explicit_no_data_status_for_empty_payload():
    spec = _historical_spec()

    with pytest.raises(RuntimeError, match="parser produced no rows"):
        twpub._parse_historical_response_content(
            spec,
            date(2024, 1, 2),
            json.dumps({"stat": "OK", "tables": []}).encode(),
            "json",
        )

    frame, suffix = twpub._parse_historical_response_content(
        spec,
        date(2024, 1, 2),
        json.dumps({"stat": "很抱歉，沒有符合條件的資料!"}).encode(),
        "json",
    )
    assert frame.is_empty()
    assert suffix == ".json"

    structured_empty = {
        "stat": "OK",
        "date": "20240102",
        "tables": [
            {
                "title": "2024-01-02 sample",
                "fields": ["value"],
                "data": [],
            }
        ],
    }
    frame, _ = twpub._parse_historical_response_content(
        spec,
        date(2024, 1, 2),
        json.dumps(structured_empty).encode(),
        "json",
    )
    assert frame.is_empty()


def _tpex_margin_payload(day: date) -> dict[str, object]:
    return {
        "stat": "OK",
        "date": day.strftime("%Y%m%d"),
        "tables": [
            {
                "title": "上櫃股票融資融券餘額",
                "date": f"{day.year - 1911:02d}/{day:%m/%d}",
                "fields": list(
                    twpub.HISTORICAL_REQUIRED_COLUMNS["tpex_margin_balance"]
                ),
                "data": [
                    [
                        "4102",
                        "永日",
                        "100",
                        "1",
                        "2",
                        "99",
                        "10",
                        "0",
                        "1",
                        "9",
                    ]
                ],
            }
        ],
    }


def test_tpex_margin_known_archive_gap_accepts_only_explicit_empty_inside_range():
    spec = twpub.DEFAULT_DATASETS["tpex_margin_balance"]
    no_data = json.dumps(
        {"stat": "很抱歉，沒有符合條件的資料!"},
        ensure_ascii=False,
    ).encode()

    frame, suffix = twpub._parse_historical_response_content(
        spec,
        date(2007, 6, 1),
        no_data,
        "json",
    )
    assert frame.is_empty()
    assert suffix == ".json"

    with pytest.raises(
        twpub.HistoricalResponseError,
        match="validated open session",
    ):
        twpub._parse_historical_response_content(
            spec,
            date(2007, 5, 31),
            no_data,
            "json",
        )


def test_tpex_margin_known_archive_gap_keeps_nonempty_official_data():
    spec = twpub.DEFAULT_DATASETS["tpex_margin_balance"]
    day = date(2007, 6, 1)

    frame, _ = twpub._parse_historical_response_content(
        spec,
        day,
        json.dumps(
            _tpex_margin_payload(day),
            ensure_ascii=False,
        ).encode(),
        "json",
    )

    assert frame.height == 1
    assert frame.get_column("代號").to_list() == ["4102"]


def test_tpex_margin_gap_journals_immutable_receipt_and_resumes(
    tmp_path: Path,
    monkeypatch,
):
    spec = twpub.DEFAULT_DATASETS["tpex_margin_balance"]
    data_day = date(2007, 5, 31)
    gap_day = date(2007, 6, 1)
    expected_days = {data_day, gap_day}

    def plan_download(*_args, **_kwargs):
        return twpub.HistoricalDownloadPlan(
            start=data_day,
            end=gap_day,
            dates=sorted(expected_days),
            all_weekdays=set(expected_days),
            existing_dates=set(),
            confirmed_empty_dates=set(),
            suspicious_dates=set(),
            missing_before=set(expected_days),
            replace_output=True,
            state={},
        )

    class FakeResponse:
        status_code = 200
        headers = {"Content-Type": "application/json;charset=UTF-8"}

        def __init__(self, content: bytes):
            self.content = content

    no_data = json.dumps(
        {"stat": "很抱歉，沒有符合條件的資料!"},
        ensure_ascii=False,
    ).encode()

    def fake_get(url: str, **_kwargs):
        if "2007/06/01" in url:
            return FakeResponse(no_data)
        assert "2007/05/31" in url
        return FakeResponse(
            json.dumps(
                _tpex_margin_payload(data_day),
                ensure_ascii=False,
            ).encode()
        )

    monkeypatch.setattr(twpub, "_plan_historical_download", plan_download)
    monkeypatch.setattr(twpub, "_http_get", fake_get)
    args = _historical_args(
        "rebuild",
        end_date=gap_day.isoformat(),
        require_taiex_session_calendar=True,
    )

    first = twpub._download_historical(spec, args, tmp_path)

    assert first.status == "ok"
    assert first.coverage_complete is True
    assert first.source_unavailable_dates == 1
    receipt = tmp_path / "raw_empty" / spec.name / f"{gap_day}.json"
    assert receipt.read_bytes() == no_data
    events = [
        json.loads(line)
        for line in twpub._historical_journal_path(
            tmp_path,
            spec,
        ).read_text(encoding="utf-8").splitlines()
    ]
    empty_event = next(event for event in events if event["status"] == "empty")
    assert empty_event["date"] == gap_day.isoformat()
    assert empty_event["source_unavailable_reason"] == "official_endpoint_archive_gap"
    assert empty_event["raw_sha256"] == hashlib.sha256(no_data).hexdigest()
    assert empty_event["body_sha256"] == empty_event["raw_sha256"]
    state = json.loads(
        (tmp_path / "state" / f"{spec.name}.json").read_text(encoding="utf-8")
    )
    assert state["confirmed_source_unavailable_dates"] == [gap_day.isoformat()]
    assert state["confirmed_empty_date_accounting"] == {
        "other_confirmed_no_data": 0,
        "source_unavailable": 1,
        "total": 1,
    }
    assert state["source_unavailable_ranges"] == [
        {
            "confirmed_dates": 1,
            "end": gap_day.isoformat(),
            "expected_session_dates": 1,
            "reason": "official_endpoint_archive_gap",
            "start": gap_day.isoformat(),
        }
    ]

    monkeypatch.setattr(
        twpub,
        "_http_get",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("receipt-verified data and empty dates must resume")
        ),
    )
    second = twpub._download_historical(spec, args, tmp_path)

    assert second.status == "up_to_date"
    assert second.coverage_complete is True
    assert second.source_unavailable_dates == 1


def test_tpex_margin_gap_validates_stage_relative_receipt_path(
    tmp_path: Path,
    monkeypatch,
) -> None:
    spec = twpub.DEFAULT_DATASETS["tpex_margin_balance"]
    day = date(2007, 6, 1)
    monkeypatch.chdir(tmp_path)
    output_dir = Path("stage")
    receipt = output_dir / "raw_empty" / spec.name / f"{day}.json"
    receipt.parent.mkdir(parents=True)
    content = json.dumps(
        {"stat": "很抱歉，沒有符合條件的資料!"},
        ensure_ascii=False,
    ).encode()
    receipt.write_bytes(content)
    digest = hashlib.sha256(content).hexdigest()
    url, _ = twpub._historical_request_info(spec, day)
    result = twpub.HistoricalDateResult(
        day=day,
        url=url,
        frame=pl.DataFrame(),
        raw_path=str(receipt),
        http_status=200,
        content_type="application/json;charset=UTF-8",
        content_length=len(content),
        body_sha256=digest,
        body_snippet=content.decode(),
        source_unavailable_reason="official_endpoint_archive_gap",
    )

    assert twpub._source_unavailable_result_receipt_is_valid(
        output_dir,
        spec,
        result,
    )


def test_historical_parser_rejects_response_for_a_different_date():
    spec = _historical_spec()
    payload = {
        "stat": "OK",
        "date": "20260709",
        "title": "115年07月09日 sample",
        "fields": ["value"],
        "data": [["wrong-day"]],
    }

    with pytest.raises(RuntimeError, match="response date mismatch"):
        twpub._parse_historical_response_content(
            spec,
            date(2024, 1, 2),
            json.dumps(payload).encode(),
            "json",
        )


@pytest.mark.parametrize(
    "dataset,title,fields,row",
    [
        (
            "twse_daily_ohlcv",
            "104年01月30日 每日收盤行情(全部)",
            ["證券代號", "成交股數", "開盤價", "最高價", "最低價", "收盤價"],
            ["2330", "1000", "50", "51", "49", "50"],
        ),
        (
            "twse_market_index",
            "104年01月30日 價格指數(臺灣證券交易所)",
            ["指數", "收盤指數"],
            ["發行量加權股價指數", "9,361.91"],
        ),
    ],
)
def test_twse_mi_index_parser_rejects_retitled_stale_top_level_date(
    dataset: str,
    title: str,
    fields: list[str],
    row: list[str],
):
    payload = {
        "stat": "OK",
        "date": "20171218",
        "params": {"date": "20171218"},
        "tables": [{"title": title, "fields": fields, "data": [row]}],
    }

    with pytest.raises(RuntimeError, match="response date mismatch"):
        twpub._parse_historical_response_content(
            twpub.DEFAULT_DATASETS[dataset],
            date(2015, 1, 30),
            json.dumps(payload, ensure_ascii=False).encode(),
            "json",
        )


@pytest.mark.parametrize(
    "dataset,title,fields,row,identity_column",
    [
        (
            "twse_daily_ohlcv",
            "104年01月30日 每日收盤行情(全部)",
            ["證券代號", "成交股數", "開盤價", "最高價", "最低價", "收盤價"],
            ["2330", "1000", "50", "51", "49", "50"],
            "證券代號",
        ),
        (
            "twse_market_index",
            "104年01月30日 價格指數(臺灣證券交易所)",
            ["指數", "收盤指數"],
            ["發行量加權股價指數", "9,361.91"],
            "指數",
        ),
    ],
)
def test_twse_mi_index_parser_accepts_matching_title_and_top_level_date(
    dataset: str,
    title: str,
    fields: list[str],
    row: list[str],
    identity_column: str,
):
    payload = {
        "stat": "OK",
        "date": "20150130",
        "params": {"date": "20150130"},
        "tables": [{"title": title, "fields": fields, "data": [row]}],
    }

    frame, suffix = twpub._parse_historical_response_content(
        twpub.DEFAULT_DATASETS[dataset],
        date(2015, 1, 30),
        json.dumps(payload, ensure_ascii=False).encode(),
        "json",
    )

    assert suffix == ".json"
    assert frame.get_column(identity_column).to_list() == [row[0]]


def test_twse_mi_index_parser_rejects_stale_params_date_when_other_dates_match():
    payload = {
        "stat": "OK",
        "date": "20150130",
        "params": {"date": "20171218"},
        "tables": [
            {
                "title": "104年01月30日 價格指數(臺灣證券交易所)",
                "fields": ["指數", "收盤指數"],
                "data": [["發行量加權股價指數", "9,361.91"]],
            }
        ],
    }

    with pytest.raises(RuntimeError, match="response date mismatch"):
        twpub._parse_historical_response_content(
            twpub.DEFAULT_DATASETS["twse_market_index"],
            date(2015, 1, 30),
            json.dumps(payload, ensure_ascii=False).encode(),
            "json",
        )


def test_twse_market_index_retitled_stale_response_resets_session_and_uses_fallback_cachebuster(
    tmp_path: Path,
    monkeypatch,
):
    class FakeResponse:
        status_code = 200
        headers = {"Content-Type": "application/json"}

        def __init__(self, content: bytes):
            self.content = content

    def payload(top_level_date: str, close: str) -> bytes:
        return json.dumps(
            {
                "stat": "OK",
                "date": top_level_date,
                "params": {"date": top_level_date},
                "tables": [
                    {
                        "title": "104年01月30日 價格指數(臺灣證券交易所)",
                        "fields": ["指數", "收盤指數"],
                        "data": [["發行量加權股價指數", close]],
                    }
                ],
            },
            ensure_ascii=False,
        ).encode()

    responses = [
        FakeResponse(payload("20171218", "10,506.52")),
        FakeResponse(payload("20171218", "10,506.52")),
        FakeResponse(payload("20150130", "9,361.91")),
    ]
    requested_urls: list[str] = []
    discarded_sessions: list[bool] = []

    def fake_get(url: str, **kwargs):
        requested_urls.append(url)
        assert kwargs["retry_security_blocks"] is False
        return responses.pop(0)

    monkeypatch.setattr(twpub, "_http_get", fake_get)
    monkeypatch.setattr(
        twpub,
        "_discard_http_session",
        lambda: discarded_sessions.append(True),
    )
    monkeypatch.setattr(twpub.time, "time_ns", lambda: 123456789)
    args = _historical_args("rebuild", end_date="2015-01-30")
    args.retries = 1

    result = twpub._download_historical_date(
        twpub.DEFAULT_DATASETS["twse_market_index"],
        date(2015, 1, 30),
        args,
        tmp_path,
    )

    assert result.error is None
    assert result.frame.get_column("收盤指數").to_list() == ["9,361.91"]
    assert "/rwd/zh/afterTrading/MI_INDEX" in requested_urls[0]
    assert "/exchangeReport/MI_INDEX" in requested_urls[1]
    assert requested_urls[2] == f"{requested_urls[1]}&_=123456789"
    assert result.url == requested_urls[2]
    assert result.response_attempts == 3
    assert discarded_sessions == [True, True]
    assert responses == []


def test_historical_parser_rejects_wrong_selected_table_date_even_if_top_level_matches():
    spec = twpub.DEFAULT_DATASETS["twse_daily_ohlcv"]
    payload = {
        "stat": "OK",
        "date": "20050208",
        "tables": [
            {
                "title": "094年02月09日 每日收盤行情(全部)",
                "fields": ["證券代號", "成交股數", "開盤價", "最高價", "最低價", "收盤價"],
                "data": [["2330", "1000", "50", "51", "49", "50"]],
            }
        ],
    }

    with pytest.raises(RuntimeError, match="response date mismatch"):
        twpub._parse_historical_response_content(
            spec,
            date(2005, 2, 8),
            json.dumps(payload, ensure_ascii=False).encode(),
            "json",
        )


@pytest.mark.parametrize(
    "transient_content,content_type",
    [
        (b"", "application/json"),
        (b"<html><body>upstream busy</body></html>", "text/html"),
        (
            json.dumps(
                {
                    "stat": "OK",
                    "date": "20260709",
                    "fields": ["value"],
                    "data": [["wrong-day"]],
                }
            ).encode(),
            "application/json",
        ),
        (
            json.dumps({"stat": "系統忙碌，請稍後再試"}).encode(),
            "application/json",
        ),
    ],
)
def test_historical_date_retries_transient_http_200_parse_without_global_cooldown(
    tmp_path: Path,
    monkeypatch,
    transient_content: bytes,
    content_type: str,
):
    class FakeResponse:
        status_code = 200

        def __init__(self, content: bytes, response_content_type: str):
            self.content = content
            self.headers = {"Content-Type": response_content_type}

    class FakeLimiter:
        def defer(self, _seconds: float):
            raise AssertionError("HTTP 200 semantic retry must not defer every dataset")

    valid_content = json.dumps(
        {
            "stat": "OK",
            "date": "20240102",
            "fields": ["value"],
            "data": [["recovered"]],
        }
    ).encode()
    responses = [
        FakeResponse(transient_content, content_type),
        FakeResponse(valid_content, "application/json; charset=utf-8"),
    ]
    discarded_sessions: list[bool] = []
    sleeps: list[float] = []
    monkeypatch.setattr(twpub, "_http_get", lambda *args, **kwargs: responses.pop(0))
    monkeypatch.setattr(
        twpub,
        "_discard_http_session",
        lambda: discarded_sessions.append(True),
    )
    monkeypatch.setattr(twpub, "_global_tw_public_rate_limiter", lambda: FakeLimiter())
    monkeypatch.setattr(twpub.time, "sleep", lambda seconds: sleeps.append(seconds))
    args = _historical_args("rebuild", end_date="2024-01-02")
    args.retries = 1
    args.retry_backoff = 0.25

    result = twpub._download_historical_date(
        _historical_spec(),
        date(2024, 1, 2),
        args,
        tmp_path,
    )

    assert result.error is None
    assert result.frame.get_column("value").to_list() == ["recovered"]
    assert result.response_attempts == 2
    assert result.http_status == 200
    assert result.content_type == "application/json; charset=utf-8"
    assert result.body_sha256 == hashlib.sha256(valid_content).hexdigest()
    assert discarded_sessions == [True]
    assert sleeps == []
    assert responses == []


def test_successful_historical_http_200_keeps_worker_session(
    tmp_path: Path,
    monkeypatch,
):
    class FakeResponse:
        status_code = 200
        headers = {"Content-Type": "application/json"}
        content = json.dumps(
            {
                "stat": "OK",
                "date": "20240102",
                "fields": ["value"],
                "data": [["ok"]],
            }
        ).encode()

    monkeypatch.setattr(twpub, "_http_get", lambda *args, **kwargs: FakeResponse())
    monkeypatch.setattr(
        twpub,
        "_discard_http_session",
        lambda: (_ for _ in ()).throw(
            AssertionError("a validated response must retain the worker session")
        ),
    )

    result = twpub._download_historical_date(
        _historical_spec(),
        date(2024, 1, 2),
        _historical_args("rebuild", end_date="2024-01-02"),
        tmp_path,
    )

    assert result.error is None
    assert result.frame.get_column("value").to_list() == ["ok"]


def test_discard_http_session_closes_and_deletes_only_current_thread_session():
    class FakeSession:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    session = FakeSession()
    twpub._HTTP_LOCAL.session = session

    twpub._discard_http_session()

    assert session.closed is True
    assert not hasattr(twpub._HTTP_LOCAL, "session")


def test_parallel_http_200_semantic_retries_do_not_serialize_provider_schedule(
    tmp_path: Path,
    monkeypatch,
):
    class FakeResponse:
        status_code = 200
        headers = {"Content-Type": "application/json"}

        def __init__(self, content: bytes):
            self.content = content

    class FakeLimiter:
        def defer(self, _seconds: float):
            raise AssertionError("semantic retry must not impose a global cooldown")

    first_attempts = threading.Barrier(2)
    calls: dict[str, int] = {}
    calls_lock = threading.Lock()

    def fake_get(url: str, **_kwargs):
        requested_day = "20240102" if "20240102" in url else "20240103"
        with calls_lock:
            attempt = calls.get(requested_day, 0)
            calls[requested_day] = attempt + 1
        if attempt == 0:
            first_attempts.wait(timeout=2)
            return FakeResponse(b"<html>transient cache-poisoned response</html>")
        payload = {
            "stat": "OK",
            "date": requested_day,
            "fields": ["value"],
            "data": [[f"recovered-{requested_day}"]],
        }
        return FakeResponse(json.dumps(payload).encode())

    monkeypatch.setattr(twpub, "_http_get", fake_get)
    monkeypatch.setattr(twpub, "_global_tw_public_rate_limiter", lambda: FakeLimiter())
    monkeypatch.setattr(
        twpub.time,
        "sleep",
        lambda _seconds: (_ for _ in ()).throw(
            AssertionError("semantic retry must not sleep outside the limiter")
        ),
    )
    args = _historical_args("rebuild", end_date="2024-01-03")
    args.retries = 1
    args.retry_backoff = 10.0
    spec = _historical_spec()

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda day: twpub._download_historical_date(
                    spec,
                    day,
                    args,
                    tmp_path,
                ),
                [date(2024, 1, 2), date(2024, 1, 3)],
            )
        )

    assert [result.error for result in results] == [None, None]
    assert [result.response_attempts for result in results] == [2, 2]
    assert calls == {"20240102": 2, "20240103": 2}


def test_historical_date_does_not_retry_valid_structured_empty_response(
    tmp_path: Path,
    monkeypatch,
):
    class FakeResponse:
        status_code = 200
        headers = {"Content-Type": "application/json"}
        content = json.dumps(
            {
                "stat": "OK",
                "date": "20240102",
                "tables": [
                    {
                        "title": "2024-01-02 sample",
                        "fields": ["value"],
                        "data": [],
                    }
                ],
            }
        ).encode()

    class FakeLimiter:
        def defer(self, seconds: float):
            raise AssertionError("valid structured empty response must not trigger cooldown")

    calls = 0

    def fake_get(*args, **kwargs):
        nonlocal calls
        calls += 1
        return FakeResponse()

    monkeypatch.setattr(twpub, "_http_get", fake_get)
    monkeypatch.setattr(twpub, "_global_tw_public_rate_limiter", lambda: FakeLimiter())
    args = _historical_args("rebuild", end_date="2024-01-02")
    args.retries = 3

    result = twpub._download_historical_date(
        _historical_spec(),
        date(2024, 1, 2),
        args,
        tmp_path,
    )

    assert result.error is None
    assert result.frame.is_empty()
    assert result.response_attempts == 1
    assert calls == 1


def test_twse_cross_checked_empty_retries_cache_busted_and_keeps_failure_receipt(
    tmp_path: Path,
    monkeypatch,
):
    class FakeResponse:
        status_code = 200
        headers = {"Content-Type": "application/json"}

        def __init__(self, content: bytes):
            self.content = content

    def empty_payload(marker: str) -> bytes:
        return json.dumps(
            {
                "stat": "OK",
                "date": "20090202",
                "marker": marker,
                "tables": [
                    {
                        "title": "98年02月02日 價格指數",
                        "fields": ["指數", "收盤指數"],
                        "data": [],
                    }
                ],
            },
            ensure_ascii=False,
        ).encode()

    final_content = empty_payload("fallback-cache-busted")
    responses = [
        FakeResponse(empty_payload("primary")),
        FakeResponse(empty_payload("fallback")),
        FakeResponse(final_content),
    ]
    requested_urls: list[str] = []

    def fake_get(url: str, **kwargs):
        requested_urls.append(url)
        assert kwargs["retry_security_blocks"] is False
        return responses.pop(0)

    class FakeLimiter:
        def __init__(self):
            self.deferrals: list[float] = []

        def defer(self, seconds: float):
            self.deferrals.append(seconds)

    limiter = FakeLimiter()
    sleeps: list[float] = []
    monkeypatch.setattr(twpub, "_http_get", fake_get)
    monkeypatch.setattr(twpub, "_global_tw_public_rate_limiter", lambda: limiter)
    monkeypatch.setattr(twpub.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(twpub.time, "time_ns", lambda: 987654321)
    args = _historical_args(
        "rebuild",
        end_date="2009-02-02",
        require_taiex_session_calendar=True,
    )
    args.retries = 1
    args.retry_backoff = 0.25
    args.skip_raw = False
    spec = twpub.DEFAULT_DATASETS["twse_market_index"]

    result = twpub._download_historical_date(
        spec,
        date(2009, 2, 2),
        args,
        tmp_path,
    )

    assert result.error == (
        "official twse_market_index returned no rows on a verified open session"
    )
    assert result.frame.is_empty()
    assert result.response_attempts == 3
    assert "/rwd/zh/afterTrading/MI_INDEX" in requested_urls[0]
    assert "/exchangeReport/MI_INDEX" in requested_urls[1]
    assert requested_urls[2] == f"{requested_urls[1]}&_=987654321"
    assert result.url == requested_urls[2]
    assert limiter.deferrals == [0.25]
    assert sleeps == [0.25]
    assert responses == []
    assert result.raw_path is not None
    receipt = Path(result.raw_path)
    assert receipt.parent == tmp_path / "raw_failures" / spec.name
    assert receipt.read_bytes() == final_content
    assert result.body_sha256 == hashlib.sha256(final_content).hexdigest()


def test_final_parse_failure_keeps_auditable_journal_and_raw_receipt(
    tmp_path: Path,
    monkeypatch,
):
    class FakeResponse:
        status_code = 200
        headers = {"Content-Type": "text/html; charset=utf-8"}
        content = b"<html>temporary upstream response</html>"

    class FakeLimiter:
        def __init__(self):
            self.deferrals: list[float] = []

        def defer(self, seconds: float):
            self.deferrals.append(seconds)

    limiter = FakeLimiter()
    calls = 0

    def fake_get(*args, **kwargs):
        nonlocal calls
        calls += 1
        return FakeResponse()

    monkeypatch.setattr(twpub, "_http_get", fake_get)
    monkeypatch.setattr(twpub, "_global_tw_public_rate_limiter", lambda: limiter)
    monkeypatch.setattr(twpub.time, "sleep", lambda _seconds: None)
    args = _historical_args("rebuild", end_date="2024-01-02")
    args.retries = 1
    args.retry_backoff = 0.5
    args.skip_raw = False

    result = twpub._download_historical(_historical_spec(), args, tmp_path)

    assert result.status == "failed"
    assert result.failed_dates == 1
    assert calls == 2
    assert limiter.deferrals == []
    journal_path = twpub._historical_journal_path(tmp_path, _historical_spec())
    events = [json.loads(line) for line in journal_path.read_text().splitlines()]
    assert len(events) == 1
    event = events[0]
    assert event["status"] == "failed"
    assert event["http_status"] == 200
    assert event["content_type"] == "text/html; charset=utf-8"
    assert event["content_length"] == len(FakeResponse.content)
    assert event["body_sha256"] == hashlib.sha256(FakeResponse.content).hexdigest()
    assert event["body_snippet"] == "<html>temporary upstream response</html>"
    assert event["response_attempts"] == 2
    assert "not valid JSON" in event["error"]
    receipt = tmp_path / event["raw_path"]
    assert receipt == (
        tmp_path
        / "raw_failures"
        / _historical_spec().name
        / f"2024-01-02.{hashlib.sha256(FakeResponse.content).hexdigest()[:16]}.html"
    )
    assert receipt.read_bytes() == FakeResponse.content
    assert event["raw_size"] == len(FakeResponse.content)
    assert event["raw_sha256"] == event["body_sha256"]


def test_twse_market_index_uses_official_index_category_and_available_start():
    spec = twpub.DEFAULT_DATASETS["twse_market_index"]
    assert "type=IND" in str(spec.url_template)
    assert spec.start_date == "2009-01-05"


@pytest.mark.parametrize(
    "dataset,legacy_path",
    [
        ("twse_daily_ohlcv", "/exchangeReport/MI_INDEX"),
        ("twse_market_index", "/exchangeReport/MI_INDEX"),
        ("twse_daily_valuation", "/exchangeReport/BWIBBU_d"),
        ("twse_institutional_trades", "/fund/T86"),
        ("twse_margin_balance", "/exchangeReport/MI_MARGN"),
    ],
)
def test_twse_historical_datasets_have_distinct_official_fallbacks(
    dataset: str,
    legacy_path: str,
):
    spec = twpub.DEFAULT_DATASETS[dataset]
    primary = str(spec.url_template).format(date="20240102")

    fallback = twpub._historical_response_fallback_url(spec, primary)

    assert fallback is not None
    assert legacy_path in fallback
    assert fallback != primary


def test_twse_semantic_fallback_retry_uses_unique_cache_key(
    tmp_path: Path,
    monkeypatch,
):
    class FakeResponse:
        status_code = 200
        headers = {"Content-Type": "application/json"}

        def __init__(self, content: bytes):
            self.content = content

    class FakeLimiter:
        def defer(self, _seconds: float):
            return None

    wrong_day = json.dumps(
        {
            "stat": "OK",
            "date": "20171218",
            "fields": ["證券代號", "本益比"],
            "data": [["2330", "10"]],
        },
        ensure_ascii=False,
    ).encode()
    correct_day = json.dumps(
        {
            "stat": "OK",
            "date": "20240102",
            "fields": ["證券代號", "本益比"],
            "data": [["2330", "20"]],
        },
        ensure_ascii=False,
    ).encode()
    responses = [
        FakeResponse(wrong_day),
        FakeResponse(wrong_day),
        FakeResponse(correct_day),
    ]
    requested_urls: list[str] = []

    def fake_get(url: str, **kwargs):
        requested_urls.append(url)
        assert kwargs["retry_security_blocks"] is False
        return responses.pop(0)

    monkeypatch.setattr(twpub, "_http_get", fake_get)
    monkeypatch.setattr(twpub, "_global_tw_public_rate_limiter", lambda: FakeLimiter())
    monkeypatch.setattr(twpub.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(twpub.time, "time_ns", lambda: 123456789)
    args = _historical_args("rebuild", end_date="2024-01-02")
    args.retries = 1
    spec = twpub.DEFAULT_DATASETS["twse_daily_valuation"]

    result = twpub._download_historical_date(
        spec,
        date(2024, 1, 2),
        args,
        tmp_path,
    )

    assert result.error is None
    assert result.frame.height == 1
    assert "/rwd/zh/afterTrading/BWIBBU_d" in requested_urls[0]
    assert "/exchangeReport/BWIBBU_d" in requested_urls[1]
    assert requested_urls[2] == f"{requested_urls[1]}&_=123456789"
    assert result.url == requested_urls[2]
    assert result.response_attempts == 3
    assert responses == []


def test_twse_market_index_falls_back_to_exchange_report_after_primary_status_error(
    tmp_path: Path,
    monkeypatch,
):
    class FakeResponse:
        status_code = 200
        headers = {"Content-Type": "application/json"}

        def __init__(self, content: bytes):
            self.content = content

    class FakeLimiter:
        def __init__(self):
            self.deferrals: list[float] = []

        def defer(self, seconds: float):
            self.deferrals.append(seconds)

    primary_error = json.dumps(
        {"stat": "查詢日期大於今日，請重新查詢!"},
        ensure_ascii=False,
    ).encode()
    fallback_data = json.dumps(
        {
            "stat": "OK",
            "date": "20090202",
            "tables": [
                {
                    "title": "98年02月02日 價格指數",
                    "fields": ["指數", "收盤指數"],
                    "data": [["發行量加權股價指數", "4,247.97"]],
                },
                {
                    "title": "98年02月02日 每日收盤行情",
                    "fields": ["證券代號"],
                    "data": [["2330"]],
                },
            ],
        },
        ensure_ascii=False,
    ).encode()
    responses = [FakeResponse(primary_error), FakeResponse(fallback_data)]
    requested_urls: list[str] = []

    def fake_get(url: str, **kwargs):
        requested_urls.append(url)
        return responses.pop(0)

    limiter = FakeLimiter()
    monkeypatch.setattr(twpub, "_http_get", fake_get)
    monkeypatch.setattr(twpub, "_global_tw_public_rate_limiter", lambda: limiter)
    monkeypatch.setattr(twpub.time, "sleep", lambda _seconds: None)
    args = _historical_args("rebuild", end_date="2009-02-02")
    args.retries = 2
    args.retry_backoff = 0.2
    spec = twpub.DEFAULT_DATASETS["twse_market_index"]

    result = twpub._download_historical_date(
        spec,
        date(2009, 2, 2),
        args,
        tmp_path,
    )

    assert result.error is None
    assert result.frame.height == 1
    assert result.frame.get_column("指數").to_list() == ["發行量加權股價指數"]
    assert "type=IND" in requested_urls[0]
    assert "/exchangeReport/MI_INDEX" in requested_urls[1]
    assert "type=IND" in requested_urls[1]
    assert result.url == requested_urls[1]
    assert result.response_attempts == 2
    assert limiter.deferrals == []
    assert responses == []


def test_twse_market_index_cross_checks_structured_empty_before_accepting_it(
    tmp_path: Path,
    monkeypatch,
):
    class FakeResponse:
        status_code = 200
        headers = {"Content-Type": "application/json"}

        def __init__(self, content: bytes):
            self.content = content

    primary_empty = json.dumps(
        {
            "stat": "OK",
            "date": "20090202",
            "tables": [
                {
                    "title": "98年02月02日 價格指數",
                    "fields": ["指數", "收盤指數"],
                    "data": [],
                }
            ],
        },
        ensure_ascii=False,
    ).encode()
    fallback_data = json.dumps(
        {
            "stat": "OK",
            "date": "20090202",
            "tables": [
                {
                    "title": "98年02月02日 價格指數",
                    "fields": ["指數", "收盤指數"],
                    "data": [["發行量加權股價指數", "4,247.97"]],
                }
            ],
        },
        ensure_ascii=False,
    ).encode()
    responses = [FakeResponse(primary_empty), FakeResponse(fallback_data)]
    requested_urls: list[str] = []

    def fake_get(url: str, **kwargs):
        requested_urls.append(url)
        return responses.pop(0)

    monkeypatch.setattr(twpub, "_http_get", fake_get)
    args = _historical_args("rebuild", end_date="2009-02-02")
    args.retries = 0
    spec = twpub.DEFAULT_DATASETS["twse_market_index"]

    result = twpub._download_historical_date(
        spec,
        date(2009, 2, 2),
        args,
        tmp_path,
    )

    assert result.error is None
    assert result.frame.height == 1
    assert result.url == requested_urls[1]
    assert "/rwd/zh/afterTrading/MI_INDEX" in requested_urls[0]
    assert "/exchangeReport/MI_INDEX" in requested_urls[1]
    assert result.response_attempts == 2
    assert responses == []


def test_twse_daily_ohlcv_falls_back_to_exchange_report_after_primary_status_error(
    tmp_path: Path,
    monkeypatch,
):
    class FakeResponse:
        status_code = 200
        headers = {"Content-Type": "application/json"}

        def __init__(self, content: bytes):
            self.content = content

    primary_error = json.dumps(
        {"stat": "查詢日期小於93年2月11日，請重新查詢!"},
        ensure_ascii=False,
    ).encode()
    fallback_data = json.dumps(
        {
            "stat": "OK",
            "date": "20070425",
            "tables": [
                {
                    "title": "096年04月25日 每日收盤行情(全部)",
                    "fields": [
                        "證券代號",
                        "成交股數",
                        "開盤價",
                        "最高價",
                        "最低價",
                        "收盤價",
                    ],
                    "data": [["2330", "1000", "65", "66", "64", "65"]],
                }
            ],
        },
        ensure_ascii=False,
    ).encode()
    responses = [FakeResponse(primary_error), FakeResponse(fallback_data)]
    requested_urls: list[str] = []

    def fake_get(url: str, **kwargs):
        requested_urls.append(url)
        return responses.pop(0)

    class FakeLimiter:
        def defer(self, _seconds: float):
            return None

    monkeypatch.setattr(twpub, "_http_get", fake_get)
    monkeypatch.setattr(twpub, "_global_tw_public_rate_limiter", lambda: FakeLimiter())
    monkeypatch.setattr(twpub.time, "sleep", lambda _seconds: None)
    args = _historical_args("rebuild", end_date="2007-04-25")
    args.retries = 0
    spec = twpub.DEFAULT_DATASETS["twse_daily_ohlcv"]

    result = twpub._download_historical_date(
        spec,
        date(2007, 4, 25),
        args,
        tmp_path,
    )

    assert result.error is None
    assert result.frame.height == 1
    assert result.frame.get_column("證券代號").to_list() == ["2330"]
    assert "/rwd/zh/afterTrading/MI_INDEX" in requested_urls[0]
    assert "/exchangeReport/MI_INDEX" in requested_urls[1]
    assert result.url == requested_urls[1]
    assert result.response_attempts == 2
    assert responses == []


def test_http_retry_applies_provider_global_cooldown(monkeypatch):
    class FakeLimiter:
        def __init__(self):
            self.waits = 0
            self.deferrals: list[float] = []

        def wait(self):
            self.waits += 1

        def defer(self, seconds):
            self.deferrals.append(seconds)

    class FakeResponse:
        def __init__(self, status_code, headers=None):
            self.status_code = status_code
            self.headers = headers or {}

        def raise_for_status(self):
            if self.status_code >= 400:
                raise twpub.requests.HTTPError(str(self.status_code))

    class FakeSession:
        def __init__(self):
            self.responses = [
                FakeResponse(429, {"Retry-After": "0.25"}),
                FakeResponse(200),
            ]

        def get(self, *args, **kwargs):
            return self.responses.pop(0)

    limiter = FakeLimiter()
    session = FakeSession()
    sleeps: list[float] = []
    monkeypatch.setattr(twpub, "_global_tw_public_rate_limiter", lambda: limiter)
    monkeypatch.setattr(twpub, "_http_session", lambda: session)
    monkeypatch.setattr(twpub.time, "sleep", lambda seconds: sleeps.append(seconds))

    response = twpub._http_get(
        "https://example.test",
        timeout=1,
        verify_ssl=True,
        retries=1,
        retry_backoff=1.0,
    )

    assert response.status_code == 200
    assert response._stockagent_response_attempts == 2
    assert limiter.waits == 2
    assert limiter.deferrals == [0.25]
    assert sleeps == [0.25]


def test_historical_transport_failure_reports_all_internal_attempts(
    tmp_path: Path,
    monkeypatch,
):
    class FakeLimiter:
        def wait(self):
            return None

        def defer(self, _seconds):
            return None

    class FakeSession:
        def get(self, *args, **kwargs):
            raise twpub.requests.ConnectionError("offline")

    monkeypatch.setattr(twpub, "_global_tw_public_rate_limiter", lambda: FakeLimiter())
    monkeypatch.setattr(twpub, "_http_session", lambda: FakeSession())
    monkeypatch.setattr(twpub.time, "sleep", lambda _seconds: None)
    args = _historical_args("rebuild", end_date="2024-01-02")
    args.retries = 2

    result = twpub._download_historical_date(
        _historical_spec(),
        date(2024, 1, 2),
        args,
        tmp_path,
    )

    assert result.error == "offline"
    assert result.response_attempts == 3


def test_http_security_block_applies_long_provider_global_cooldown(monkeypatch):
    class FakeLimiter:
        def __init__(self):
            self.waits = 0
            self.deferrals: list[float] = []

        def wait(self):
            self.waits += 1

        def defer(self, seconds):
            self.deferrals.append(seconds)

    class FakeResponse:
        def __init__(
            self,
            status_code: int,
            content: bytes = b"",
            headers: dict[str, str] | None = None,
        ):
            self.status_code = status_code
            self.content = content
            self.headers = dict(headers or {})

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.responses = [
                FakeResponse(
                    307,
                    b"<html>FOR SECURITY REASONS, THIS PAGE CAN NOT BE ACCESSED.</html>",
                    {"Retry-After": "1"},
                ),
                FakeResponse(200, b"{}"),
            ]

        def get(self, *args, **kwargs):
            return self.responses.pop(0)

    limiter = FakeLimiter()
    session = FakeSession()
    sleeps: list[float] = []
    monkeypatch.setattr(twpub, "_global_tw_public_rate_limiter", lambda: limiter)
    monkeypatch.setattr(twpub, "_http_session", lambda: session)
    monkeypatch.setattr(twpub.time, "sleep", lambda seconds: sleeps.append(seconds))

    response = twpub._http_get(
        "https://example.test",
        timeout=1,
        verify_ssl=True,
        retries=1,
        retry_backoff=1.0,
    )

    assert response.status_code == 200
    assert limiter.waits == 2
    assert limiter.deferrals == [twpub.TW_PUBLIC_WAF_COOLDOWN_SECONDS]
    assert sleeps == [twpub.TW_PUBLIC_WAF_COOLDOWN_SECONDS]


def test_http_locationless_307_retries_as_throttling(monkeypatch):
    class FakeLimiter:
        def __init__(self):
            self.waits = 0
            self.deferrals: list[float] = []

        def wait(self):
            self.waits += 1

        def defer(self, seconds):
            self.deferrals.append(seconds)

    class FakeResponse:
        def __init__(self, status_code: int):
            self.status_code = status_code
            self.headers: dict[str, str] = {}
            self.content = b""
            self.closed = False

        def close(self):
            self.closed = True

        def raise_for_status(self):
            return None

    throttled = FakeResponse(307)
    success = FakeResponse(200)

    class FakeSession:
        responses = [throttled, success]

        def get(self, *args, **kwargs):
            return self.responses.pop(0)

    limiter = FakeLimiter()
    sleeps: list[float] = []
    monkeypatch.setattr(twpub, "_global_tw_public_rate_limiter", lambda: limiter)
    monkeypatch.setattr(twpub, "_http_session", lambda: FakeSession())
    monkeypatch.setattr(twpub.time, "sleep", lambda seconds: sleeps.append(seconds))

    response = twpub._http_get(
        "https://example.test",
        timeout=1,
        verify_ssl=True,
        retries=1,
        retry_backoff=1.0,
    )

    assert response is success
    assert throttled.closed is True
    assert limiter.waits == 2
    assert limiter.deferrals == [30.0]
    assert sleeps == [30.0]


def test_http_stream_enforces_total_wall_timeout(monkeypatch):
    class FakeLimiter:
        def wait(self):
            return None

        def defer(self, _seconds):
            return None

    class FakeResponse:
        status_code = 200
        headers: dict[str, str] = {}
        content = b""
        url = "https://example.test"

        def __init__(self):
            self.closed = False

        def iter_content(self, *, chunk_size):
            assert chunk_size == 1024 * 1024
            yield b"partial"

        def close(self):
            self.closed = True

    response = FakeResponse()

    class FakeSession:
        def get(self, *args, **kwargs):
            assert kwargs["stream"] is True
            assert kwargs["timeout"] == (1, 1)
            return response

    monotonic = iter([0.0, 1.1])
    monkeypatch.setattr(twpub, "_global_tw_public_rate_limiter", lambda: FakeLimiter())
    monkeypatch.setattr(twpub, "_http_session", lambda: FakeSession())
    monkeypatch.setattr(twpub.time, "monotonic", lambda: next(monotonic))

    with pytest.raises(twpub.requests.exceptions.Timeout, match="wall timeout"):
        twpub._http_get(
            "https://example.test",
            timeout=1,
            verify_ssl=True,
            retries=0,
        )

    assert response.closed is True


def test_waf_cooldown_honors_retry_after_when_it_is_longer():
    response = twpub.requests.Response()
    response.status_code = 403
    response.headers["Retry-After"] = "75"
    response._content = (
        b"<html>FOR SECURITY REASONS, THIS PAGE CAN NOT BE ACCESSED.</html>"
    )

    assert twpub._retry_delay_seconds(response, 0, 1.0) == 75.0


def test_http_security_block_can_switch_to_semantic_fallback_without_same_url_retry(
    monkeypatch,
):
    class FakeLimiter:
        def __init__(self):
            self.waits = 0
            self.deferrals: list[float] = []

        def wait(self):
            self.waits += 1

        def defer(self, seconds):
            self.deferrals.append(seconds)

    class FakeResponse:
        status_code = 307
        headers = {}
        content = b"<html>FOR SECURITY REASONS, THIS PAGE CAN NOT BE ACCESSED.</html>"

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.calls = 0

        def get(self, *args, **kwargs):
            self.calls += 1
            return FakeResponse()

    limiter = FakeLimiter()
    session = FakeSession()
    sleeps: list[float] = []
    monkeypatch.setattr(twpub, "_global_tw_public_rate_limiter", lambda: limiter)
    monkeypatch.setattr(twpub, "_http_session", lambda: session)
    monkeypatch.setattr(twpub.time, "sleep", lambda seconds: sleeps.append(seconds))

    response = twpub._http_get(
        "https://example.test",
        timeout=1,
        verify_ssl=True,
        retries=4,
        retry_backoff=1.0,
        retry_security_blocks=False,
    )

    assert response.status_code == 307
    assert session.calls == 1
    assert limiter.waits == 1
    assert limiter.deferrals == [twpub.TW_PUBLIC_WAF_COOLDOWN_SECONDS]
    assert sleeps == [twpub.TW_PUBLIC_WAF_COOLDOWN_SECONDS]


def test_historical_waf_switches_to_official_fallback_with_one_global_cooldown(
    tmp_path: Path,
    monkeypatch,
):
    class FakeLimiter:
        def __init__(self):
            self.waits = 0
            self.deferrals: list[float] = []

        def wait(self):
            self.waits += 1

        def defer(self, seconds):
            self.deferrals.append(seconds)

    class FakeResponse:
        headers = {"Content-Type": "application/json"}

        def __init__(self, status_code: int, content: bytes):
            self.status_code = status_code
            self.content = content

        def raise_for_status(self):
            if self.status_code >= 400:
                raise twpub.requests.HTTPError(str(self.status_code))

    correct = json.dumps(
        {
            "stat": "OK",
            "date": "20240102",
            "fields": ["證券代號", "本益比"],
            "data": [["2330", "20"]],
        },
        ensure_ascii=False,
    ).encode()

    class FakeSession:
        def __init__(self):
            self.responses = [
                FakeResponse(
                    307,
                    b"<html>FOR SECURITY REASONS, THIS PAGE CAN NOT BE ACCESSED.</html>",
                ),
                FakeResponse(200, correct),
            ]

        def get(self, *args, **kwargs):
            return self.responses.pop(0)

    limiter = FakeLimiter()
    session = FakeSession()
    discarded_sessions: list[bool] = []
    sleeps: list[float] = []
    monkeypatch.setattr(twpub, "_global_tw_public_rate_limiter", lambda: limiter)
    monkeypatch.setattr(twpub, "_http_session", lambda: session)
    monkeypatch.setattr(
        twpub,
        "_discard_http_session",
        lambda: discarded_sessions.append(True),
    )
    monkeypatch.setattr(twpub.time, "sleep", lambda seconds: sleeps.append(seconds))
    args = _historical_args("rebuild", end_date="2024-01-02")
    args.retries = 4
    spec = twpub.DEFAULT_DATASETS["twse_daily_valuation"]

    result = twpub._download_historical_date(
        spec,
        date(2024, 1, 2),
        args,
        tmp_path,
    )

    assert result.error is None
    assert result.frame.height == 1
    assert result.response_attempts == 2
    assert limiter.waits == 2
    assert limiter.deferrals == [twpub.TW_PUBLIC_WAF_COOLDOWN_SECONDS]
    assert sleeps == [twpub.TW_PUBLIC_WAF_COOLDOWN_SECONDS]
    assert discarded_sessions == [True]
    assert session.responses == []


def test_http_ssl_fallback_is_also_rate_limited(monkeypatch):
    class FakeLimiter:
        def __init__(self):
            self.waits = 0

        def wait(self):
            self.waits += 1

        def defer(self, seconds):
            raise AssertionError("unexpected defer")

    class FakeResponse:
        status_code = 200
        headers = {}

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.calls = 0

        def get(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise twpub.requests.exceptions.SSLError("certificate")
            assert kwargs["verify"] is False
            return FakeResponse()

    limiter = FakeLimiter()
    monkeypatch.setattr(twpub, "_global_tw_public_rate_limiter", lambda: limiter)
    monkeypatch.setattr(twpub, "_http_session", lambda: FakeSession())

    response = twpub._http_get(
        "https://example.test",
        timeout=1,
        verify_ssl=True,
        retries=0,
    )

    assert response.status_code == 200
    assert limiter.waits == 2


def test_table_mode_filters_json_tables_by_title():
    spec = twpub.DatasetSpec(
        name="sample",
        kind="historical_json_table",
        source="TWSE",
        description="sample",
        tags=("test",),
        table_mode="title_contains:每日收盤行情",
    )
    payload = {
        "stat": "OK",
        "tables": [
            {"title": "價格指數", "fields": ["指數"], "data": [["發行量加權股價指數"]]},
            {"title": "每日收盤行情", "fields": ["證券代號"], "data": [["2330"]]},
        ],
    }

    frame = twpub._parse_json_table_payload(payload, spec, date(2024, 6, 3))

    assert frame.height == 1
    assert frame["證券代號"].to_list() == ["2330"]
    assert frame["_table_title"].to_list() == ["每日收盤行情"]


def test_parse_csv_bytes_accepts_big5_and_dedupes_columns():
    raw = "日期,利率[%],利率[%]\n2002/5/2,2.269,2.270\n".encode("cp950")

    frame = twpub._parse_csv_bytes(raw)

    assert frame.columns == ["日期", "利率[%]", "利率[%]_2"]
    assert frame["利率[%]"].to_list() == ["2.269"]


def test_parse_xml_bytes_flattens_repeated_records():
    raw = b"""
    <Root>
      <Row><TIME_PERIOD>2024M01</TIME_PERIOD><VALUE>1.2</VALUE></Row>
      <Row><TIME_PERIOD>2024M02</TIME_PERIOD><VALUE>1.3</VALUE></Row>
    </Root>
    """

    frame = twpub._parse_xml_bytes(raw)

    assert frame.shape == (2, 2)
    assert frame["TIME_PERIOD"].to_list() == ["2024M01", "2024M02"]


def test_select_specs_accepts_tags_and_names():
    names = {spec.name for spec in twpub._select_specs(["macro", "twse_daily_ohlcv"])}

    assert "twse_daily_ohlcv" in names
    assert "cbc_overnight_rate" in names
    assert "dgbas_unemployment_rate" in names


def test_model_useful_group_is_curated_and_has_unique_dataset_names():
    specs = twpub._select_specs(["model_useful"])
    names = [spec.name for spec in specs]

    assert len(specs) == 117
    assert len(names) == len(set(names))
    assert all("model_useful" in spec.tags for spec in specs)
    assert not any("warrant" in spec.name or "bond" in spec.name or "gold" in spec.name for spec in specs)


def test_merge_frames_replaces_existing_dates():
    existing = pl.DataFrame(
        {
            "date": ["2024-06-03", "2024-06-04"],
            "value": ["old", "keep"],
        }
    )
    incoming = pl.DataFrame(
        {
            "date": ["2024-06-03"],
            "value": ["new"],
        }
    )

    merged = twpub._merge_frames(existing, incoming, refresh=False)

    assert merged.sort("date")["value"].to_list() == ["new", "keep"]


def test_snapshot_download_preserves_daily_vintages_and_immutable_raw(
    tmp_path: Path,
    monkeypatch,
):
    spec = twpub.DatasetSpec(
        name="sample_snapshot",
        kind="snapshot_url",
        source="official-test",
        description="sample",
        tags=("test", "snapshot"),
        url="https://example.test/snapshot",
    )
    args = SimpleNamespace(
        timeout=1,
        verify_ssl=True,
        retries=0,
        retry_backoff=0.0,
        skip_raw=False,
        refresh=False,
        mode="daily",
    )
    payloads = [
        b'[{"Code":"2330","value":"old"}]',
        b'[{"Code":"2330","value":"corrected"}]',
        b'[{"Code":"2330","value":"next"}]',
    ]

    class FakeResponse:
        def __init__(self, content: bytes):
            self.content = content
            self.headers = {"content-type": "application/json"}

    monkeypatch.setattr(
        twpub,
        "_http_get",
        lambda *args, **kwargs: FakeResponse(payloads.pop(0)),
    )
    snapshot_days = iter(("2024-06-03", "2024-06-03", "2024-06-04"))
    monkeypatch.setattr(twpub, "_snapshot_as_of_date", lambda: next(snapshot_days))

    twpub._download_snapshot_url(spec, args, tmp_path)
    twpub._download_snapshot_url(spec, args, tmp_path)
    twpub._download_snapshot_url(spec, args, tmp_path)

    frame = pl.read_parquet(tmp_path / "sample_snapshot.parquet").sort("_as_of_date")
    assert frame.select("_as_of_date", "date", "value").to_dicts() == [
        {
            "_as_of_date": "2024-06-03",
            "date": "2024-06-03",
            "value": "corrected",
        },
        {"_as_of_date": "2024-06-04", "date": "2024-06-04", "value": "next"},
    ]
    raw_paths = sorted((tmp_path / "raw" / "sample_snapshot").iterdir())
    assert len(raw_paths) == 3
    assert all(path.name.startswith("2024-06-0") for path in raw_paths)


def test_parse_twse_delisted_company_payload():
    payload = [{"DelistingDate": "114/07/24", "Company": "新光金", "Code": "2888"}]

    frame = twpub._twse_delisted_frame(payload)

    assert frame.to_dicts() == [{
        "date": "2025-07-24",
        "market": "twse",
        "symbol": "2888",
        "company_name": "新光金",
        "delisting_reason": "",
    }]


def test_parse_tpex_delisted_company_payload():
    payload = {
        "tables": [{
            "fields": ["股票代號", "公司名稱", "終止上櫃日期", "終止上櫃原因", "公司資料網址"],
            "data": [["6747", "亨泰光學股份有限公司", "114-12-04", "業務規則第15條之18", "https://example.test"]],
        }],
        "stat": "ok",
    }

    frame = twpub._tpex_delisted_frame(payload)

    assert frame["symbol"].to_list() == ["6747"]
    assert frame["date"].to_list() == ["2025-12-04"]
    assert frame["delisting_reason"].to_list() == ["業務規則第15條之18"]


def test_parse_tpex_legacy_daily_quotes_with_split_close_cells():
    raw_html = """
    <table>
      <tr><td>股票代號</td><td>證券名稱</td><td colspan=2>收盤價</td></tr>
      <tr><td>4205</td><td>恆義食品</td><td></td><td>18.1</td><td>△</td><td>0.1</td>
          <td>18</td><td>18.5</td><td>17.95</td><td>18.12</td><td>97,000</td>
          <td>1,757,700</td><td>24</td><td>18.1</td><td>18.2</td><td>60,086,250</td>
          <td>18.1</td><td>19.35</td><td>16.85</td></tr>
    </table>
    """

    frame = twpub._parse_tpex_daily_quotes_html(raw_html, date(2006, 12, 29))

    assert frame.select(["date", "代號", "收盤", "漲跌", "開盤", "最高", "最低", "成交股數"]).to_dicts() == [
        {
            "date": "2006-12-29",
            "代號": "4205",
            "收盤": "18.1",
            "漲跌": "+0.1",
            "開盤": "18",
            "最高": "18.5",
            "最低": "17.95",
            "成交股數": "97,000",
        }
    ]


def test_parse_tpex_2004_daily_quotes_without_close_spacer():
    raw_html = """
    <table>
      <tr><td>4205</td><td>恆義食品</td><td>16.1</td><td>▽</td><td>0.1</td>
          <td>16.2</td><td>16.2</td><td>16.1</td><td>16.11</td><td>11,000</td>
          <td>177,200</td><td>5</td><td>16.1</td><td>16.2</td><td>55,125,000</td>
          <td>16.1</td><td>17.2</td><td>15</td></tr>
    </table>
    """

    frame = twpub._parse_tpex_daily_quotes_html(raw_html, date(2004, 10, 28))

    assert frame.select(
        ["代號", "收盤", "漲跌", "開盤", "最高", "最低", "成交股數"]
    ).to_dicts() == [
        {
            "代號": "4205",
            "收盤": "16.1",
            "漲跌": "-0.1",
            "開盤": "16.2",
            "最高": "16.2",
            "最低": "16.1",
            "成交股數": "11,000",
        }
    ]


def test_parse_tpex_2004_corporate_action_with_blank_change_direction():
    raw_html = """
    <table>
      <tr><td>5006</td><td>高鋁金屬</td><td>9.15</td><td></td><td>除權</td>
          <td>9.65</td><td>9.75</td><td>9.15</td><td>9.5</td><td>3,569,000</td>
          <td>33,896,900</td><td>810</td><td>9.15</td><td>9.3</td><td>115,754,623</td>
          <td>9.15</td><td>9.75</td><td>8.55</td></tr>
    </table>
    """

    frame = twpub._parse_tpex_daily_quotes_html(raw_html, date(2004, 10, 28))

    assert frame.select(
        [
            "代號",
            "收盤",
            "漲跌",
            "開盤",
            "最高",
            "最低",
            "成交股數",
            "成交金額(元)",
            "成交筆數",
            "發行股數",
            "次日漲停價",
            "次日跌停價",
        ]
    ).to_dicts() == [
        {
            "代號": "5006",
            "收盤": "9.15",
            "漲跌": "除權",
            "開盤": "9.65",
            "最高": "9.75",
            "最低": "9.15",
            "成交股數": "3,569,000",
            "成交金額(元)": "33,896,900",
            "成交筆數": "810",
            "發行股數": "115,754,623",
            "次日漲停價": "9.75",
            "次日跌停價": "8.55",
        }
    ]


def test_parse_tpex_later_18_cell_daily_quote_with_status_spacer():
    raw_html = """
    <table>
      <tr><td>4205</td><td>恆義食品</td><td>⊕</td><td>18.1</td><td>+0.1</td>
          <td>18</td><td>18.5</td><td>17.95</td><td>18.12</td><td>97,000</td>
          <td>1,757,700</td><td>24</td><td>18.1</td><td>18.2</td><td>60,086,250</td>
          <td>18.1</td><td>19.35</td><td>16.85</td></tr>
    </table>
    """

    frame = twpub._parse_tpex_daily_quotes_html(raw_html, date(2006, 12, 29))

    assert frame.select(
        ["代號", "收盤", "漲跌", "開盤", "最高", "最低", "成交股數"]
    ).to_dicts() == [
        {
            "代號": "4205",
            "收盤": "18.1",
            "漲跌": "+0.1",
            "開盤": "18",
            "最高": "18.5",
            "最低": "17.95",
            "成交股數": "97,000",
        }
    ]


def test_parse_tpex_oracle_report_without_leading_spacer_and_strips_limit_glyphs():
    raw_html = """
    <table><tr>
      <td>4205<td><td>恆義食品<td><td>♁ 22.25<td><td>+1.45<td>
      <td>20.80<td><td>♁ 22.25<td><td>20.80<td><td>21.94<td><td>317,549<td>
      <td>6,965,764<td><td>128<td><td>22.00<td><td>22.25<td>
    <tr><td>next row
    """

    frame = twpub._parse_tpex_daily_quotes_html(raw_html, date(2004, 10, 27))

    assert frame.select(
        ["代號", "收盤", "漲跌", "開盤", "最高", "最低", "成交股數"]
    ).to_dicts() == [
        {
            "代號": "4205",
            "收盤": "22.25",
            "漲跌": "+1.45",
            "開盤": "20.80",
            "最高": "22.25",
            "最低": "20.80",
            "成交股數": "317,549",
        }
    ]


def test_parse_tpex_2003_oracle_report_with_unclosed_spacer_cells():
    raw_html = """
    <table><tr valign=top>
      <td colspan=2><td>3087<td><td>翔準<td><td>13.85<td><td>+0.00<td>
      <td>14.00<td><td>14.30<td><td>13.85<td><td>14.03<td><td>1,178,000<td>
      <td>16,521,950<td><td>472<td><td>13.85<td><td>13.90<td>
    <tr><td>next row
    """

    frame = twpub._parse_tpex_daily_quotes_html(raw_html, date(2003, 8, 1))

    assert frame.select(["代號", "收盤", "漲跌", "開盤", "最高", "最低", "成交股數"]).to_dicts() == [
        {
            "代號": "3087",
            "收盤": "13.85",
            "漲跌": "+0.00",
            "開盤": "14.00",
            "最高": "14.30",
            "最低": "13.85",
            "成交股數": "1,178,000",
        }
    ]


def test_tpex_daily_archive_keeps_valid_numbers_with_receipt_level_name_warning():
    spec = twpub.DEFAULT_DATASETS["tpex_daily_ohlcv"]
    raw = (
        "<html>資料日期：920801<tr><td>3087</td><td>".encode("cp950")
        + "\ufffd\ufffd".encode()
        + "</td><td>13.85</td><td>+0.00</td><td>14.00</td><td>14.30</td>"
        "<td>13.85</td><td>14.03</td><td>1,178,000</td><td>16,521,950</td>"
        "<td>472</td><td>13.85</td><td>13.90</td><td></td><td></td><td></td>"
        "<td></td></tr></html>".encode("cp950")
    )

    frame, _ = twpub._parse_historical_response_content(
        spec,
        date(2003, 8, 1),
        raw,
        "archive_html",
    )

    assert frame.get_column("收盤").to_list() == ["13.85"]
    assert frame.get_column("成交股數").to_list() == ["1,178,000"]
    assert frame.get_column("_name_decode_status").to_list() == [
        "official_receipt_name_bytes_unrecoverable"
    ]


def test_tpex_daily_lossy_receipt_marks_plausible_cp950_repaired_name_unusable():
    spec = twpub.DEFAULT_DATASETS["tpex_daily_ohlcv"]
    raw = (
        "<html>資料日期：930304<tr><td>5351</td><td>".encode("cp950")
        + bytes.fromhex("e0b1b3efbfbd")
        + "</td><td>13.85</td><td>+0.00</td><td>14.00</td><td>14.30</td>"
        "<td>13.85</td><td>14.03</td><td>1,178,000</td><td>16,521,950</td>"
        "<td>472</td><td>13.85</td><td>13.90</td><td></td><td></td><td></td>"
        "<td></td></tr></html>".encode("cp950")
    )

    frame, _ = twpub._parse_historical_response_content(
        spec,
        date(2004, 3, 4),
        raw,
        "archive_html",
    )

    assert frame.get_column("名稱").to_list() == ["鈺喉蕭"]
    assert frame.get_column("_name_decode_status").to_list() == [
        "official_receipt_name_bytes_unrecoverable"
    ]


def test_tpex_daily_archive_rejects_lossy_numeric_cell():
    spec = twpub.DEFAULT_DATASETS["tpex_daily_ohlcv"]
    raw = (
        "<html>資料日期：920801<tr><td>3087</td><td>翔準</td><td>13".encode(
            "cp950"
        )
        + b"\xff"
        + ".85</td><td>+0.00</td><td>14.00</td><td>14.30</td>"
        "<td>13.85</td><td>14.03</td><td>1,178,000</td><td>16,521,950</td>"
        "<td>472</td><td>13.85</td><td>13.90</td><td></td><td></td><td></td>"
        "<td></td></tr></html>".encode("cp950")
    )

    with pytest.raises(twpub.HistoricalResponseError, match="lossy decode damage"):
        twpub._parse_historical_response_content(
            spec,
            date(2003, 8, 1),
            raw,
            "archive_html",
        )


def test_tpex_daily_archive_rejects_lossy_symbol_even_when_other_rows_are_valid():
    spec = twpub.DEFAULT_DATASETS["tpex_daily_ohlcv"]
    damaged = (
        "<html>資料日期：920801<tr><td>30".encode("cp950")
        + b"\xff"
        + "7</td><td>翔準</td><td>13.85</td><td>+0.00</td><td>14.00</td>"
        "<td>14.30</td><td>13.85</td><td>14.03</td><td>1,178,000</td>"
        "<td>16,521,950</td><td>472</td><td>13.85</td><td>13.90</td>"
        "<td></td><td></td><td></td><td></td></tr>".encode("cp950")
    )
    valid = (
        "<tr><td>3088</td><td>艾訊</td><td>20.00</td><td>+0.10</td>"
        "<td>19.90</td><td>20.10</td><td>19.80</td><td>20.00</td>"
        "<td>1,000</td><td>20,000</td><td>10</td><td>19.90</td><td>20.00</td>"
        "<td></td><td></td><td></td><td></td></tr></html>"
    ).encode("cp950")

    with pytest.raises(twpub.HistoricalResponseError, match="malformed security code"):
        twpub._parse_historical_response_content(
            spec,
            date(2003, 8, 1),
            damaged + valid,
            "archive_html",
        )


def test_tpex_daily_archive_requires_an_official_declared_date():
    spec = twpub.DEFAULT_DATASETS["tpex_daily_ohlcv"]
    raw = (
        "<html><tr><td>3087</td><td>翔準</td><td>13.85</td><td>+0.00</td>"
        "<td>14.00</td><td>14.30</td><td>13.85</td><td>14.03</td>"
        "<td>1,178,000</td><td>16,521,950</td><td>472</td><td>13.85</td>"
        "<td>13.90</td><td></td><td></td><td></td><td></td></tr></html>"
    ).encode("cp950")

    with pytest.raises(twpub.HistoricalResponseError, match="declares no date"):
        twpub._parse_historical_response_content(
            spec,
            date(2003, 8, 1),
            raw,
            "archive_html",
        )


def test_tpex_daily_archive_accepts_labeled_compact_roc_date():
    spec = twpub.DEFAULT_DATASETS["tpex_daily_ohlcv"]
    raw = (
        "<html>資料日期：920801"
        "<tr><td>3087</td><td>翔準</td><td>13.85</td><td>+0.00</td>"
        "<td>14.00</td><td>14.30</td><td>13.85</td><td>14.03</td>"
        "<td>1,178,000</td><td>16,521,950</td><td>472</td><td>13.85</td>"
        "<td>13.90</td><td></td><td></td><td></td><td></td></tr></html>"
    ).encode("cp950")

    frame, suffix = twpub._parse_historical_response_content(
        spec,
        date(2003, 8, 1),
        raw,
        "archive_html",
    )

    assert suffix == ".html"
    assert frame.get_column("代號").to_list() == ["3087"]


def test_tpex_daily_archive_accepts_exact_styled_compact_roc_date_header():
    spec = twpub.DEFAULT_DATASETS["tpex_daily_ohlcv"]
    raw = (
        '<html><td width=71 colspan=5 rowspan=2 class="table-body-right">'
        "<fontStop><b><tt>930304</tt></b></fontStop>"
        "<tr><td>3087</td><td>翔準</td><td>13.85</td><td>+0.00</td>"
        "<td>14.00</td><td>14.30</td><td>13.85</td><td>14.03</td>"
        "<td>1,178,000</td><td>16,521,950</td><td>472</td><td>13.85</td>"
        "<td>13.90</td><td></td><td></td><td></td><td></td></tr></html>"
    ).encode("cp950")

    frame, suffix = twpub._parse_historical_response_content(
        spec,
        date(2004, 3, 4),
        raw,
        "archive_html",
    )

    assert suffix == ".html"
    assert frame.get_column("代號").to_list() == ["3087"]


def test_tpex_daily_styled_date_header_must_match_requested_date():
    spec = twpub.DEFAULT_DATASETS["tpex_daily_ohlcv"]
    raw = (
        '<html><td width="71" colspan="5" rowspan="2" class="table-body-right">'
        "<tt>930304</tt><tr><td>930304</td><td>測試</td><td>13.85</td>"
        "<td>0</td><td>13.85</td><td>13.85</td><td>13.85</td><td>13.85</td>"
        "<td>1</td><td>14</td><td>1</td><td>13.85</td><td>13.90</td>"
        "<td></td><td></td><td></td><td></td></tr></html>"
    ).encode("cp950")

    with pytest.raises(twpub.HistoricalResponseError, match="date mismatch"):
        twpub._parse_historical_response_content(
            spec,
            date(2004, 3, 5),
            raw,
            "archive_html",
        )


def test_tpex_daily_average_note_requires_exact_zero_trade_gate():
    spec = twpub.DEFAULT_DATASETS["tpex_daily_ohlcv"]
    valid = (
        "<html>資料日期：920801<tr><td>4801</td><td>測試</td><td>0</td><td>0</td>"
        "<td>0</td><td>0</td><td>0</td><td>註</td><td>0</td><td>0</td><td>0</td>"
        "<td>0</td><td>0</td><td></td><td></td><td></td><td></td></tr></html>"
    ).encode("cp950")

    frame, _ = twpub._parse_historical_response_content(
        spec,
        date(2003, 8, 1),
        valid,
        "archive_html",
    )
    assert frame.get_column("均價").to_list() == ["註"]

    invalid = valid.replace(
        "<td>註</td><td>0</td><td>0</td>".encode("cp950"),
        "<td>註</td><td>1</td><td>0</td>".encode("cp950"),
    )
    with pytest.raises(twpub.HistoricalResponseError, match="zero-trade gate"):
        twpub._parse_historical_response_content(
            spec,
            date(2003, 8, 1),
            invalid,
            "archive_html",
        )


def test_tpex_daily_lossy_average_note_is_canonicalized_only_under_zero_gate():
    frame = pl.DataFrame(
        {
            "date": ["2003-08-01"],
            "代號": ["4801"],
            "名稱": ["測�"],
            "收盤": ["0"],
            "漲跌": ["0"],
            "開盤": ["0"],
            "最高": ["0"],
            "最低": ["0"],
            "均價": ["嚙踝蕭"],
            "成交股數": ["0"],
            "成交金額(元)": ["0"],
            "成交筆數": ["0"],
            "最後買價": ["0"],
            "最後賣價": ["0"],
            "發行股數": [""],
            "次日參考價": [""],
            "次日漲停價": [""],
            "次日跌停價": [""],
            "_name_decode_status": ["official_receipt_name_bytes_unrecoverable"],
            "_table_title": ["上櫃股票每日收盤行情"],
        }
    )

    normalized = twpub._validate_tpex_daily_numeric_cells(
        frame,
        lossy_name_receipt=True,
    )
    assert normalized.get_column("均價").to_list() == ["註"]
    assert normalized.get_column("_name_decode_status").to_list() == [
        "official_receipt_name_bytes_unrecoverable"
    ]


def test_tpex_daily_archive_rejects_lossy_change_cell():
    spec = twpub.DEFAULT_DATASETS["tpex_daily_ohlcv"]
    raw = (
        "<html>資料日期：920801<tr><td>3087</td><td>翔準</td><td>13.85</td>".encode(
            "cp950"
        )
        + b"<td>\xff</td>"
        + "<td>14.00</td><td>14.30</td><td>13.85</td><td>14.03</td>"
        "<td>1,178,000</td><td>16,521,950</td><td>472</td><td>13.85</td>"
        "<td>13.90</td><td></td><td></td><td></td><td></td></tr></html>".encode(
            "cp950"
        )
    )

    with pytest.raises(twpub.HistoricalResponseError, match="lossy decode damage"):
        twpub._parse_historical_response_content(
            spec,
            date(2003, 8, 1),
            raw,
            "archive_html",
        )


@pytest.mark.parametrize(
    ("damaged", "recovered"),
    [
        ("嚙踝蕭嚙緞", "除權"),
        ("嚙踝蕭嚙踝蕭", "除息"),
        ("嚙踝蕭嚙緞嚙踝蕭", "除權息"),
    ],
)
def test_tpex_daily_recovers_only_evidenced_cp950_change_patterns(
    damaged: str,
    recovered: str,
):
    raw_html = (
        f"<tr><td>3087</td><td>測�</td><td>13.85</td><td>{damaged}</td>"
        "<td>14.00</td><td>14.30</td><td>13.85</td><td>14.03</td>"
        "<td>1,178,000</td><td>16,521,950</td><td>472</td><td>13.85</td>"
        "<td>13.90</td><td></td><td></td><td></td><td></td></tr>"
    )

    frame = twpub._parse_tpex_daily_quotes_html(
        raw_html,
        date(2004, 3, 4),
        lossy_name_receipt=True,
    )
    frame = twpub._validate_tpex_daily_numeric_cells(
        frame,
        lossy_name_receipt=True,
    )

    assert frame.get_column("漲跌").to_list() == [recovered]
    assert frame.get_column("_change_decode_status").to_list() == [
        twpub.TPEX_CHANGE_RECOVERY_STATUS
    ]


@pytest.mark.parametrize(
    ("damaged", "lossy_receipt"),
    [
        ("嚙踝蕭嚙緞", False),
        ("嚙踝蕭未知", True),
    ],
)
def test_tpex_daily_rejects_unevidenced_change_recovery(
    damaged: str,
    lossy_receipt: bool,
):
    raw_html = (
        f"<tr><td>3087</td><td>測試</td><td>13.85</td><td>{damaged}</td>"
        "<td>14.00</td><td>14.30</td><td>13.85</td><td>14.03</td>"
        "<td>1,178,000</td><td>16,521,950</td><td>472</td><td>13.85</td>"
        "<td>13.90</td><td></td><td></td><td></td><td></td></tr>"
    )
    frame = twpub._parse_tpex_daily_quotes_html(
        raw_html,
        date(2004, 3, 4),
        lossy_name_receipt=lossy_receipt,
    )

    with pytest.raises(twpub.HistoricalResponseError, match="lossy decode damage"):
        twpub._validate_tpex_daily_numeric_cells(
            frame,
            lossy_name_receipt=lossy_receipt,
        )


def test_tpex_feature_archive_rejects_invalid_big5_numeric_cell():
    spec = twpub.DEFAULT_DATASETS["tpex_daily_valuation"]
    raw = (
        "<html>上櫃股票個股本益比、股利率、股價淨值比 資料日期：920801"
        "<tr><td>3087</td><td>翔準</td><td>1".encode("cp950")
        + b"\xff"
        + "0</td><td>2.0</td><td>1.48</td></tr></html>".encode("cp950")
    )

    with pytest.raises(twpub.HistoricalResponseError, match="lossy Unicode"):
        twpub._parse_historical_response_content(
            spec,
            date(2003, 8, 1),
            raw,
            "tpex_valuation_archive_html",
        )


def test_tpex_legacy_zero_ohlc_sentinel_does_not_invalidate_the_whole_session(
    tmp_path: Path,
):
    spec = twpub.DEFAULT_DATASETS["tpex_daily_ohlcv"]
    path = tmp_path / "tpex_daily_ohlcv.parquet"
    pl.DataFrame(
        {
            "date": ["2003-09-12", "2003-09-12"],
            "代號": ["4522", "3087"],
            "成交股數": ["50", "135,300"],
            "開盤": [".00", "9.80"],
            "最高": [".00", "10.20"],
            "最低": [".00", "9.75"],
            "收盤": ["16.00", "9.95"],
        }
    ).write_parquet(path)

    counts, invalid_dates = twpub._existing_date_counts(path)
    suspicious, issues = twpub._suspicious_ohlcv_dates(path, spec, counts)

    assert invalid_dates == []
    assert suspicious == set()
    assert issues == []


def test_tpex_all_zero_regular_ohlc_with_verified_average_is_not_corrupt(
    tmp_path: Path,
):
    spec = twpub.DEFAULT_DATASETS["tpex_daily_ohlcv"]
    path = tmp_path / "tpex_daily_ohlcv.parquet"
    pl.DataFrame(
        {
            "date": ["2007-01-02", "2007-01-02"],
            "代號": ["4801", "3087"],
            "成交股數": ["240", "493,001"],
            "成交金額(元)": ["6,408", "4,847,290"],
            "均價": ["26.70", "9.83"],
            "開盤": ["0.00", "9.81"],
            "最高": ["0.00", "9.89"],
            "最低": ["0.00", "9.80"],
            "收盤": ["0.00", "9.81"],
        }
    ).write_parquet(path)

    counts, invalid_dates = twpub._existing_date_counts(path)
    suspicious, issues = twpub._suspicious_ohlcv_dates(path, spec, counts)

    assert invalid_dates == []
    assert suspicious == set()
    assert issues == []


def test_tpex_all_zero_ohlc_without_average_receipt_fails_closed(
    tmp_path: Path,
):
    spec = twpub.DEFAULT_DATASETS["tpex_daily_ohlcv"]
    path = tmp_path / "tpex_daily_ohlcv.parquet"
    pl.DataFrame(
        {
            "date": ["2007-01-02"],
            "代號": ["4801"],
            "成交股數": ["240"],
            "開盤": ["0.00"],
            "最高": ["0.00"],
            "最低": ["0.00"],
            "收盤": ["0.00"],
        }
    ).write_parquet(path)

    counts, _ = twpub._existing_date_counts(path)
    suspicious, issues = twpub._suspicious_ohlcv_dates(path, spec, counts)

    assert suspicious == {date(2007, 1, 2)}
    assert issues == ["invalid_ohlcv_dates=1"]


def test_tpex_historical_request_routes_each_official_archive_generation():
    spec = next(item for item in twpub.HISTORICAL_DAILY_DATASETS if item.name == "tpex_daily_ohlcv")

    archive_url, archive_kind = twpub._tpex_historical_request(date(2006, 12, 29), spec)
    legacy_url, legacy_kind = twpub._tpex_historical_request(date(2007, 7, 1), spec)
    current_url, current_kind = twpub._tpex_historical_request(date(2007, 7, 2), spec)

    assert archive_kind == "archive_html"
    assert archive_url.endswith("RSTA3104_951229.HTML")
    assert legacy_kind == "legacy_json_html"
    assert "dailyQuotesHis" in legacy_url
    assert current_kind == "json"
    assert "afterTrading/otc" in current_url


@pytest.mark.parametrize(
    "dataset,day,url_suffix,response_kind",
    [
        (
            "tpex_margin_balance",
            date(2003, 8, 1),
            "MARGIN_BALANCE/RSTA3106_920801.html",
            "tpex_margin_archive_html",
        ),
        (
            "tpex_daily_valuation",
            date(2006, 12, 29),
            "PERATIO_ANALYSIS/RSTA3103_951229.HTML",
            "tpex_valuation_archive_html",
        ),
        (
            "tpex_institutional_trades",
            date(2004, 6, 1),
            "DAILY_TRADE/BIGD930601S_N.html",
            "tpex_institutional_archive_html",
        ),
        (
            "tpex_institutional_trades",
            date(2007, 1, 2),
            "daily_trade/BIGD_960102S_N.html",
            "tpex_institutional_archive_html",
        ),
        (
            "tpex_institutional_trades",
            date(2007, 4, 20),
            "daily_trade/BIGD_960420S_N.html",
            "tpex_institutional_archive_html",
        ),
    ],
)
def test_tpex_feature_archives_have_exact_official_routes(
    dataset: str,
    day: date,
    url_suffix: str,
    response_kind: str,
):
    spec = twpub.DEFAULT_DATASETS[dataset]

    url, kind = twpub._tpex_historical_request(day, spec)

    assert url.endswith(url_suffix)
    assert kind == response_kind


def test_tpex_institutional_routes_cover_every_official_generation():
    spec = twpub.DEFAULT_DATASETS["tpex_institutional_trades"]

    middle_url, middle_kind = twpub._tpex_historical_request(
        date(2007, 4, 23),
        spec,
    )
    middle_last_url, middle_last_kind = twpub._tpex_historical_request(
        date(2014, 11, 28),
        spec,
    )
    current_url, current_kind = twpub._tpex_historical_request(
        date(2014, 12, 1),
        spec,
    )

    assert middle_kind == middle_last_kind == "json"
    assert "insti/dailyTradeHis" in middle_url
    assert "date=2007/04/23" in middle_url
    assert "cate=EW" in middle_url
    assert "insti/dailyTradeHis" in middle_last_url
    assert current_kind == "json"
    assert "insti/dailyTrade?" in current_url
    assert "sect=EW" in current_url


def _legacy_row(values: list[str], *, row_class: bool = False) -> str:
    row_attr = " class='table-body-right'" if row_class else ""
    cell_attr = "" if row_class else " class='table-body-right'"
    cells = "".join(f"<td{cell_attr}>{value}</td>" for value in values)
    return f"<tr{row_attr}>{cells}</tr>"


def test_parse_tpex_margin_archive_normalizes_both_legacy_layouts():
    spec = twpub.DEFAULT_DATASETS["tpex_margin_balance"]
    early = (
        "<html>融資融券餘額彙總表 資料日期：920801"
        + _legacy_row(
            [
                "4102",
                "永日",
                "745",
                "0",
                "11",
                "0",
                "734",
                "8,262",
                "0",
                "0",
                "0",
                "0",
                "0",
            ]
        )
        + "</html>"
    )
    late = (
        "<html>上櫃股票融資融券餘額 資料日期：951229"
        + _legacy_row(
            [
                "1333",
                "恩得利",
                "8,712",
                "277",
                "350",
                "0",
                "8,639",
                "(1,126)",
                "20,250",
                "47",
                "0",
                "12",
                "0",
                "35",
                "(1)",
                "1",
                "OX",
            ],
            row_class=True,
        )
        + "</html>"
    )

    early_frame, early_suffix = twpub._parse_historical_response_content(
        spec,
        date(2003, 8, 1),
        early.encode("cp950"),
        "tpex_margin_archive_html",
    )
    late_frame, late_suffix = twpub._parse_historical_response_content(
        spec,
        date(2006, 12, 29),
        late.encode("cp950"),
        "tpex_margin_archive_html",
    )

    assert early_suffix == late_suffix == ".html"
    assert early_frame.select(
        "代號",
        "資餘額",
        "資屬證金",
        "資限額",
        "券餘額",
        "備註",
    ).to_dicts() == [
        {
            "代號": "4102",
            "資餘額": "734",
            "資屬證金": "",
            "資限額": "8,262",
            "券餘額": "0",
            "備註": "",
        }
    ]
    assert late_frame.select(
        "代號",
        "資屬證金",
        "券屬證金",
        "資券相抵(張)",
        "備註",
    ).to_dicts() == [
        {
            "代號": "1333",
            "資屬證金": "1,126",
            "券屬證金": "1",
            "資券相抵(張)": "1",
            "備註": "OX",
        }
    ]


def test_parse_tpex_margin_archive_maps_2004_sixteen_cell_layout():
    spec = twpub.DEFAULT_DATASETS["tpex_margin_balance"]
    cells = [
        "1565",
        "精華光",
        "2,574",
        "73",
        "135",
        "0",
        "2,512",
        "(197)",
        "9,707",
        "67",
        "1",
        "6",
        "0",
        "62",
        "(1)",
        "",
    ]
    raw = (
        "<html>上櫃股票融資融券餘額"
        "<tr><td>單位：張</td>"
        "<td ALIGN=RIGHT VALIGN=CENTER COLSPAN='14'>940610</td></tr>"
        "<tr>"
        + "".join(f"<td>{value}</td>" for value in cells)
        + "</tr></html>"
    )

    frame, suffix = twpub._parse_historical_response_content(
        spec,
        date(2005, 6, 10),
        raw.encode("cp950"),
        "tpex_margin_archive_html",
    )

    assert suffix == ".html"
    assert frame.select(
        "代號",
        "資餘額",
        "資屬證金",
        "資限額",
        "券餘額",
        "券屬證金",
        "資券相抵(張)",
        "備註",
    ).to_dicts() == [
        {
            "代號": "1565",
            "資餘額": "2,512",
            "資屬證金": "197",
            "資限額": "9,707",
            "券餘額": "62",
            "券屬證金": "1",
            "資券相抵(張)": "",
            "備註": "",
        }
    ]


def test_parse_tpex_valuation_archive_maps_old_dividend_yield_schema():
    spec = twpub.DEFAULT_DATASETS["tpex_daily_valuation"]
    raw = (
        "<html>上櫃股票個股本益比、股利率、股價淨值比 資料日期：920801"
        + _legacy_row(["3087", "翔準", "NA", "0", "1.48"])
        + "</html>"
    )

    frame, suffix = twpub._parse_historical_response_content(
        spec,
        date(2003, 8, 1),
        raw.encode("cp950"),
        "tpex_valuation_archive_html",
    )

    assert suffix == ".html"
    assert frame.select(
        "股票代號",
        "公司名稱",
        "本益比",
        "每股股利",
        "股利年度",
        "殖利率(%)",
        "股價淨值比",
    ).to_dicts() == [
        {
            "股票代號": "3087",
            "公司名稱": "翔準",
            "本益比": "NA",
            "每股股利": "",
            "股利年度": "",
            "殖利率(%)": "0",
            "股價淨值比": "1.48",
        }
    ]


def test_parse_tpex_valuation_archive_accepts_labeled_roc_date():
    spec = twpub.DEFAULT_DATASETS["tpex_daily_valuation"]
    raw = (
        "<html>上櫃股票個股本益比、股利率、股價淨值比 "
        "交易日期:94年08月08日"
        + _legacy_row(["4205", "恆義食品", "9.2", "7.41", "1.14"])
        + "</html>"
    )

    frame, suffix = twpub._parse_historical_response_content(
        spec,
        date(2005, 8, 8),
        raw.encode("cp950"),
        "tpex_valuation_archive_html",
    )

    assert suffix == ".html"
    assert frame.select(
        "股票代號",
        "殖利率(%)",
        "股價淨值比",
    ).to_dicts() == [
        {
            "股票代號": "4205",
            "殖利率(%)": "7.41",
            "股價淨值比": "1.14",
        }
    ]


def test_parse_tpex_institutional_archive_maps_feature_columns():
    spec = twpub.DEFAULT_DATASETS["tpex_institutional_trades"]
    raw = (
        "<html>三大法人日交易資訊 交易日期:93年06月01日"
        + _legacy_row(
            [
                "5347",
                "世界",
                "212,000",
                "6,582,000",
                "-6,370,000",
                "4,510,000",
                "0",
                "4,510,000",
                "849,000",
                "698,000",
                "151,000",
                "-1,709,000",
            ]
        )
        + "</html>"
    )

    frame, suffix = twpub._parse_historical_response_content(
        spec,
        date(2004, 6, 1),
        raw.encode("cp950"),
        "tpex_institutional_archive_html",
    )

    assert suffix == ".html"
    assert frame.select(
        "代號",
        "外資及陸資淨買股數",
        "投信淨買股數",
        "自營商買股數",
        "自營商賣股數",
        "自營淨買股數",
        "三大法人買賣超股數",
    ).to_dicts() == [
        {
            "代號": "5347",
            "外資及陸資淨買股數": "-6,370,000",
            "投信淨買股數": "4,510,000",
            "自營商買股數": "849,000",
            "自營商賣股數": "698,000",
            "自營淨買股數": "151,000",
            "三大法人買賣超股數": "-1,709,000",
        }
    ]


def test_tpex_institutional_middle_json_normalizes_dealer_net_column():
    spec = twpub.DEFAULT_DATASETS["tpex_institutional_trades"]
    fields = [
        "代號",
        "名稱",
        "外資及陸資買股數",
        "外資及陸資賣股數",
        "外資及陸資淨買股數",
        "投信買進股數",
        "投信賣股數",
        "投信淨買股數",
        "自營商買股數",
        "自營商賣股數",
        "自營商淨買股數",
        "三大法人買賣超股數",
    ]
    payload = {
        "date": "20070423",
        "stat": "ok",
        "tables": [
            {
                "title": "三大法人買賣明細資訊",
                "date": "96/04/23",
                "fields": fields,
                "data": [["1565", "精華", *(["0"] * 10)]],
            }
        ],
    }

    frame, suffix = twpub._parse_historical_response_content(
        spec,
        date(2007, 4, 23),
        json.dumps(payload, ensure_ascii=False).encode(),
        "json",
    )

    assert suffix == ".json"
    assert "自營商淨買股數" not in frame.columns
    assert frame.get_column("自營淨買股數").to_list() == ["0"]


def test_tpex_institutional_grouped_json_restores_canonical_field_names():
    spec = twpub.DEFAULT_DATASETS["tpex_institutional_trades"]
    payload = {
        "date": "20260709",
        "stat": "ok",
        "tables": [
            {
                "title": "三大法人買賣明細資訊",
                "date": "115/07/09",
                "fields": list(twpub.TPEX_INSTITUTIONAL_GROUPED_SOURCE_FIELDS),
                "data": [
                    [
                        "006201",
                        "元大富櫃50",
                        "2,000",
                        "36,000",
                        "-34,000",
                        "0",
                        "0",
                        "0",
                        "2,000",
                        "36,000",
                        "-34,000",
                        "0",
                        "0",
                        "0",
                        "0",
                        "0",
                        "0",
                        "104,905",
                        "64,885",
                        "40,020",
                        "104,905",
                        "64,885",
                        "40,020",
                        "6,020",
                    ]
                ],
            }
        ],
    }

    frame, suffix = twpub._parse_historical_response_content(
        spec,
        date(2026, 7, 9),
        json.dumps(payload, ensure_ascii=False).encode(),
        "json",
    )

    assert suffix == ".json"
    assert frame.select(
        "外資及陸資淨買股數",
        "投信淨買股數",
        "自營淨買股數",
        "三大法人買賣超股數",
    ).to_dicts() == [
        {
            "外資及陸資淨買股數": "-34,000",
            "投信淨買股數": "0",
            "自營淨買股數": "40,020",
            "三大法人買賣超股數": "6,020",
        }
    ]


@pytest.mark.parametrize(
    "raw,error_pattern",
    [
        (
            "<html>上櫃股票個股本益比、股利率、股價淨值比 "
            "資料日期：920802"
            + _legacy_row(["3087", "翔準", "NA", "0", "1.48"])
            + "</html>",
            "response date mismatch",
        ),
        (
            "<html>網址並不存在 資料日期：920801"
            + _legacy_row(["3087", "翔準", "NA", "0", "1.48"])
            + "</html>",
            "title mismatch",
        ),
        (
            "<html>上櫃股票個股本益比、股利率、股價淨值比"
            + _legacy_row(["3087", "翔準", "NA", "0", "1.48"])
            + "</html>",
            "declares no date",
        ),
    ],
)
def test_tpex_archive_identity_is_fail_closed(raw: str, error_pattern: str):
    spec = twpub.DEFAULT_DATASETS["tpex_daily_valuation"]

    with pytest.raises(twpub.HistoricalResponseError, match=error_pattern):
        twpub._parse_historical_response_content(
            spec,
            date(2003, 8, 1),
            raw.encode("cp950"),
            "tpex_valuation_archive_html",
        )


@pytest.mark.parametrize("second_symbol", ["3087", "3088"])
def test_tpex_archive_rejects_duplicate_or_malformed_rows(second_symbol: str):
    spec = twpub.DEFAULT_DATASETS["tpex_daily_valuation"]
    first = _legacy_row(["3087", "翔準", "NA", "0", "1.48"])
    second_values = (
        [second_symbol, "艾訊", "10", "2.0", "1.2"]
        if second_symbol == "3087"
        else [second_symbol, "艾訊", "10", "2.0"]
    )
    raw = (
        "<html>上櫃股票個股本益比、股利率、股價淨值比 資料日期：920801"
        + first
        + _legacy_row(second_values)
        + "</html>"
    )

    expected = "duplicate symbol" if second_symbol == "3087" else "has 4 cells"
    with pytest.raises(twpub.HistoricalResponseError, match=expected):
        twpub._parse_historical_response_content(
            spec,
            date(2003, 8, 1),
            raw.encode("cp950"),
            "tpex_valuation_archive_html",
        )


def _write_validated_tpex_calendar(tmp_path: Path) -> None:
    pl.DataFrame(
        {
            "date": ["2003-08-01", "2003-08-04"],
            "代號": ["4102", "4102"],
            "成交股數": ["1,000", "2,000"],
            "開盤": ["10", "10"],
            "最高": ["11", "11"],
            "最低": ["9", "9"],
            "收盤": ["10", "10"],
        }
    ).write_parquet(tmp_path / "tpex_daily_ohlcv.parquet")
    state_dir = tmp_path / "state"
    state_dir.mkdir(exist_ok=True)
    (state_dir / "tpex_daily_ohlcv.json").write_text(
        json.dumps(
            {
                "schema_version": twpub.COVERAGE_STATE_SCHEMA_VERSION,
                "dataset": "tpex_daily_ohlcv",
                "baseline_established": True,
                "coverage_complete": True,
                "coverage_start": "2003-08-01",
                "coverage_end": "2003-08-05",
                "checked_through": "2003-08-05",
                "confirmed_empty_dates": ["2003-08-05"],
                "failed_dates": {},
            }
        ),
        encoding="utf-8",
    )


def _write_validated_taiex_calendar(
    output_dir: Path,
    dates: list[date],
    *,
    coverage_start: date,
    coverage_end: date,
) -> str:
    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = output_dir / "twse_taiex_ohlc.parquet"
    rows = len(dates)
    pl.DataFrame(
        {
            "date": dates,
            "opening_index": [10_000.0] * rows,
            "highest_index": [10_100.0] * rows,
            "lowest_index": [9_900.0] * rows,
            "closing_index": [10_050.0] * rows,
            "_dataset": ["twse_taiex_ohlc"] * rows,
            "_source": ["TWSE"] * rows,
            "_source_product": ["indicesReport/MI_5MINS_HIST"] * rows,
        }
    ).write_parquet(parquet_path)
    receipt_sha256 = twpub._file_sha256(parquet_path)
    (output_dir / "twse_taiex_ohlc.summary.json").write_text(
        json.dumps(
            {
                "schema_version": twpub.TAIEX_SESSION_CALENDAR_SUMMARY_SCHEMA_VERSION,
                "dataset": "twse_taiex_ohlc",
                "coverage_complete": True,
                "baseline_established": True,
                "replacement_promoted": True,
                "effective_start_date": coverage_start.isoformat(),
                "effective_end_date": coverage_end.isoformat(),
                "unresolved_month_count": 0,
                "failed_count": 0,
                "canonical_path": str(parquet_path),
                "output_rows": rows,
                "output_receipt": {
                    "path": parquet_path.name,
                    "size": parquet_path.stat().st_size,
                    "sha256": receipt_sha256,
                },
            }
        ),
        encoding="utf-8",
    )
    return receipt_sha256


def test_direct_cli_does_not_require_taiex_calendar_by_default(monkeypatch):
    monkeypatch.setattr("sys.argv", ["download_tw_public_data.py"])

    args = twpub.parse_args()

    assert args.require_taiex_session_calendar is False


def test_strict_plan_uses_receipt_verified_taiex_sessions_for_twse(tmp_path: Path):
    _write_validated_taiex_calendar(
        tmp_path,
        [date(2024, 1, 2), date(2024, 1, 4)],
        coverage_start=date(2024, 1, 1),
        coverage_end=date(2024, 1, 5),
    )
    spec = _historical_spec()
    args = _historical_args(
        "rebuild",
        end_date="2024-01-05",
        require_taiex_session_calendar=True,
    )

    plan = twpub._plan_historical_download(spec, args, tmp_path)

    assert plan.all_weekdays == {date(2024, 1, 2), date(2024, 1, 4)}
    assert plan.dates == [date(2024, 1, 2), date(2024, 1, 4)]
    assert plan.state["coverage_calendar_source"] == "twse_taiex_ohlc"
    assert plan.state["coverage_calendar_sha256"]


def test_strict_calendar_prunes_stale_non_session_failures_from_complete_state(
    tmp_path: Path,
    monkeypatch,
):
    _write_validated_taiex_calendar(
        tmp_path,
        [date(2024, 1, 2), date(2024, 1, 4)],
        coverage_start=date(2024, 1, 1),
        coverage_end=date(2024, 1, 5),
    )
    spec = _historical_spec()
    pl.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-04"],
            "value": ["session-2", "session-4"],
        }
    ).write_parquet(tmp_path / f"{spec.name}.parquet")
    state_dir = tmp_path / "state"
    state_dir.mkdir(exist_ok=True)
    (state_dir / f"{spec.name}.json").write_text(
        json.dumps(
            {
                "schema_version": twpub.COVERAGE_STATE_SCHEMA_VERSION,
                "dataset": spec.name,
                "baseline_established": True,
                "coverage_complete": False,
                "confirmed_empty_dates": [],
                "failed_dates": {
                    "2024-01-03": "legacy weekday false failure",
                    "2024-01-05": "legacy weekday false failure",
                    "not-a-date": "corrupt legacy state key",
                },
                "pruned_failed_dates_total": 7,
            }
        ),
        encoding="utf-8",
    )

    def unexpected_download(*args, **kwargs):
        raise AssertionError("non-session stale failures must not be requested")

    monkeypatch.setattr(twpub, "_download_historical_date", unexpected_download)
    result = twpub._download_historical(
        spec,
        _historical_args(
            "repair",
            end_date="2024-01-05",
            require_taiex_session_calendar=True,
        ),
        tmp_path,
    )

    assert result.status == "up_to_date"
    assert result.coverage_complete is True
    assert result.missing_dates_after == 0
    state = json.loads((state_dir / f"{spec.name}.json").read_text())
    assert state["failed_dates"] == {}
    assert state["last_pruned_failed_dates"] == 3
    assert state["last_pruned_failed_date_examples"] == [
        "2024-01-03",
        "2024-01-05",
        "not-a-date",
    ]
    assert state["pruned_failed_dates_total"] == 10


def test_strict_calendar_retains_unresolved_session_failure_in_state(
    tmp_path: Path,
    monkeypatch,
):
    _write_validated_taiex_calendar(
        tmp_path,
        [date(2024, 1, 2), date(2024, 1, 4)],
        coverage_start=date(2024, 1, 1),
        coverage_end=date(2024, 1, 5),
    )
    spec = _historical_spec()
    pl.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-04"],
            "value": ["session-2", "stale-session-4"],
        }
    ).write_parquet(tmp_path / f"{spec.name}.parquet")
    state_dir = tmp_path / "state"
    state_dir.mkdir(exist_ok=True)
    (state_dir / f"{spec.name}.json").write_text(
        json.dumps(
            {
                "schema_version": twpub.COVERAGE_STATE_SCHEMA_VERSION,
                "dataset": spec.name,
                "baseline_established": True,
                "coverage_complete": False,
                "confirmed_empty_dates": [],
                "failed_dates": {
                    "2024-01-03": "legacy weekday false failure",
                    "2024-01-04": "unresolved session failure",
                },
            }
        ),
        encoding="utf-8",
    )

    def failed_session_download(spec, day, args, output_dir):
        assert day == date(2024, 1, 4)
        return twpub.HistoricalDateResult(
            day=day,
            url=f"https://example.test/{day}",
            frame=pl.DataFrame(),
            error="still unavailable",
        )

    monkeypatch.setattr(twpub, "_download_historical_date", failed_session_download)
    result = twpub._download_historical(
        spec,
        _historical_args(
            "repair",
            end_date="2024-01-05",
            require_taiex_session_calendar=True,
        ),
        tmp_path,
    )

    assert result.status == "failed"
    assert result.coverage_complete is False
    assert result.missing_dates_after == 1
    state = json.loads((state_dir / f"{spec.name}.json").read_text())
    assert state["failed_dates"] == {"2024-01-04": "still unavailable"}
    assert state["last_pruned_failed_dates"] == 1
    assert state["last_pruned_failed_date_examples"] == ["2024-01-03"]


def test_strict_tpex_ohlcv_does_not_request_non_session_weekdays(tmp_path: Path):
    _write_validated_taiex_calendar(
        tmp_path,
        [date(2003, 8, 1), date(2003, 8, 4)],
        coverage_start=date(1999, 1, 5),
        coverage_end=date(2003, 8, 5),
    )
    spec = twpub.DEFAULT_DATASETS["tpex_daily_ohlcv"]
    args = _historical_args(
        "rebuild",
        end_date="2003-08-05",
        require_taiex_session_calendar=True,
    )

    plan = twpub._plan_historical_download(spec, args, tmp_path)

    assert plan.all_weekdays == {date(2003, 8, 1), date(2003, 8, 4)}
    assert date(2003, 8, 5) not in plan.dates
    assert plan.confirmed_empty_dates == set()


def test_strict_calendar_rejects_tampered_parquet_receipt(tmp_path: Path):
    _write_validated_taiex_calendar(
        tmp_path,
        [date(2024, 1, 2)],
        coverage_start=date(2024, 1, 1),
        coverage_end=date(2024, 1, 5),
    )
    with (tmp_path / "twse_taiex_ohlc.parquet").open("ab") as handle:
        handle.write(b"tampered")
    args = _historical_args(
        "rebuild",
        end_date="2024-01-05",
        require_taiex_session_calendar=True,
    )

    with pytest.raises(RuntimeError, match="size disagrees"):
        twpub._plan_historical_download(_historical_spec(), args, tmp_path)


def test_strict_calendar_hash_read_does_not_treat_atime_as_file_mutation(
    tmp_path: Path,
):
    _write_validated_taiex_calendar(
        tmp_path,
        [date(2024, 1, 2)],
        coverage_start=date(2024, 1, 1),
        coverage_end=date(2024, 1, 5),
    )
    parquet_path = tmp_path / "twse_taiex_ohlc.parquet"
    original = parquet_path.stat()
    os.utime(
        parquet_path,
        ns=(1_000_000_000, int(original.st_mtime_ns)),
    )
    args = _historical_args(
        "rebuild",
        end_date="2024-01-05",
        require_taiex_session_calendar=True,
    )

    plan = twpub._plan_historical_download(_historical_spec(), args, tmp_path)

    assert plan.all_weekdays == {date(2024, 1, 2)}


def test_strict_calendar_rejects_existing_non_session_canonical_date(tmp_path: Path):
    _write_validated_taiex_calendar(
        tmp_path,
        [date(2024, 1, 2), date(2024, 1, 4)],
        coverage_start=date(2024, 1, 1),
        coverage_end=date(2024, 1, 5),
    )
    spec = _historical_spec()
    pl.DataFrame(
        {
            "date": ["2024-01-03"],
            "value": ["unexpected holiday row"],
        }
    ).write_parquet(tmp_path / f"{spec.name}.parquet")
    args = _historical_args(
        "repair",
        end_date="2024-01-05",
        require_taiex_session_calendar=True,
    )

    with pytest.raises(RuntimeError, match="canonical dates disagree"):
        twpub._plan_historical_download(spec, args, tmp_path)


def test_strict_calendar_rejects_non_session_staged_partial_before_requests(
    tmp_path: Path,
):
    _write_validated_taiex_calendar(
        tmp_path,
        [date(2024, 1, 2), date(2024, 1, 4)],
        coverage_start=date(2024, 1, 1),
        coverage_end=date(2024, 1, 5),
    )
    spec = _historical_spec()
    args = _historical_args(
        "rebuild",
        end_date="2024-01-05",
        require_taiex_session_calendar=True,
    )
    plan = twpub._plan_historical_download(spec, args, tmp_path)
    partial_path = twpub._historical_partial_path(
        tmp_path,
        spec,
        twpub._historical_resume_cache_key(spec),
    )
    partial_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {"date": ["2024-01-03"], "value": ["unexpected holiday row"]}
    ).write_parquet(partial_path)

    with pytest.raises(RuntimeError, match="staged partial dates disagree"):
        twpub._prepare_historical_resume_cache(spec, args, tmp_path, plan)


def test_strict_calendar_never_records_session_empty_as_confirmed_empty(
    tmp_path: Path,
    monkeypatch,
):
    _write_validated_taiex_calendar(
        tmp_path,
        [date(2024, 1, 2), date(2024, 1, 4)],
        coverage_start=date(2024, 1, 1),
        coverage_end=date(2024, 1, 5),
    )
    spec = _historical_spec()
    args = _historical_args(
        "rebuild",
        end_date="2024-01-05",
        require_taiex_session_calendar=True,
    )

    def fake_download(spec, day, args, output_dir):
        frame = (
            pl.DataFrame({"date": [day.isoformat()], "value": ["ok"]})
            if day == date(2024, 1, 2)
            else pl.DataFrame()
        )
        return twpub.HistoricalDateResult(
            day=day,
            url=f"https://example.test/{day}",
            frame=frame,
            http_status=200,
        )

    monkeypatch.setattr(twpub, "_download_historical_date", fake_download)

    result = twpub._download_historical(spec, args, tmp_path)

    assert result.status == "failed"
    assert result.failed_dates == 1
    assert result.empty_dates == 0
    state = json.loads((tmp_path / "state" / f"{spec.name}.json").read_text())
    assert state["confirmed_empty_dates"] == []
    assert "2024-01-04" in state["failed_dates"]


def test_tpex_feature_plan_uses_only_validated_official_sessions(tmp_path: Path):
    _write_validated_tpex_calendar(tmp_path)
    spec = twpub.DEFAULT_DATASETS["tpex_margin_balance"]
    args = _historical_args("rebuild", end_date="2003-08-05")

    plan = twpub._plan_historical_download(spec, args, tmp_path)

    assert plan.all_weekdays == {date(2003, 8, 1), date(2003, 8, 4)}
    assert plan.dates == [date(2003, 8, 1), date(2003, 8, 4)]
    assert plan.state["coverage_calendar_source"] == "tpex_daily_ohlcv"


def test_strict_tpex_feature_calendar_is_bound_to_current_taiex_receipt(
    tmp_path: Path,
):
    receipt_sha256 = _write_validated_taiex_calendar(
        tmp_path,
        [date(2003, 8, 1), date(2003, 8, 4)],
        coverage_start=date(1999, 1, 5),
        coverage_end=date(2003, 8, 5),
    )
    _write_validated_tpex_calendar(tmp_path)
    state_path = tmp_path / "state" / "tpex_daily_ohlcv.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state.update(
        {
            "coverage_calendar_source": "twse_taiex_ohlc",
            "coverage_calendar_sha256": receipt_sha256,
        }
    )
    state_path.write_text(json.dumps(state), encoding="utf-8")
    spec = twpub.DEFAULT_DATASETS["tpex_margin_balance"]
    args = _historical_args(
        "rebuild",
        end_date="2003-08-05",
        require_taiex_session_calendar=True,
    )

    plan = twpub._plan_historical_download(spec, args, tmp_path)

    assert plan.all_weekdays == {date(2003, 8, 1), date(2003, 8, 4)}
    assert plan.state["coverage_calendar_source"] == "tpex_daily_ohlcv"
    assert plan.state["root_coverage_calendar_sha256"] == receipt_sha256


def test_strict_tpex_feature_calendar_rejects_stale_taiex_binding(tmp_path: Path):
    _write_validated_taiex_calendar(
        tmp_path,
        [date(2003, 8, 1), date(2003, 8, 4)],
        coverage_start=date(1999, 1, 5),
        coverage_end=date(2003, 8, 5),
    )
    _write_validated_tpex_calendar(tmp_path)
    state_path = tmp_path / "state" / "tpex_daily_ohlcv.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state.update(
        {
            "coverage_calendar_source": "twse_taiex_ohlc",
            "coverage_calendar_sha256": "0" * 64,
        }
    )
    state_path.write_text(json.dumps(state), encoding="utf-8")
    args = _historical_args(
        "rebuild",
        end_date="2003-08-05",
        require_taiex_session_calendar=True,
    )

    with pytest.raises(RuntimeError, match="receipt is stale"):
        twpub._plan_historical_download(
            twpub.DEFAULT_DATASETS["tpex_margin_balance"],
            args,
            tmp_path,
        )


def test_tpex_feature_plan_refuses_an_unverified_calendar(tmp_path: Path):
    spec = twpub.DEFAULT_DATASETS["tpex_margin_balance"]
    args = _historical_args("rebuild", end_date="2003-08-05")

    with pytest.raises(RuntimeError, match="completed tpex_daily_ohlcv baseline"):
        twpub._plan_historical_download(spec, args, tmp_path)


def test_tpex_dependency_scheduler_runs_calendar_phase_first(
    tmp_path: Path,
    monkeypatch,
):
    calls: list[list[str]] = []

    def fake_run_parallel_tasks(items, function, **kwargs):
        batch = list(items)
        calls.append([spec.name for spec in batch])
        return [
            twpub.DownloadResult(spec.name, "ok", 1, None, coverage_complete=True)
            for spec in batch
        ]

    monkeypatch.setattr(twpub, "run_parallel_tasks", fake_run_parallel_tasks)
    specs = [
        twpub.DEFAULT_DATASETS["tpex_margin_balance"],
        twpub.DEFAULT_DATASETS["twse_daily_ohlcv"],
        twpub.DEFAULT_DATASETS["tpex_daily_ohlcv"],
    ]
    args = SimpleNamespace(workers=2)

    twpub._run_selected_downloads(specs, args, tmp_path)

    assert calls == [
        ["twse_daily_ohlcv", "tpex_daily_ohlcv"],
        ["tpex_margin_balance"],
    ]
