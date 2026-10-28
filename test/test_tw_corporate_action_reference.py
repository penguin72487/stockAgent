from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib
from io import BytesIO
import json
from types import SimpleNamespace
from zipfile import ZIP_DEFLATED, ZipFile

import polars as pl
import pytest
from requests.exceptions import HTTPError, ReadTimeout

from downloader import download_tw_corporate_action_reference as reference
from downloader.download_tw_corporate_action_reference import (
    MonthlyDocumentIdentityError,
    _monthly_query_document_id,
    _parse_tpex_monthly_ods,
    _parse_tpex_disposition_payload,
    _payload_rows,
    _resolve_tpex_monthly_rows,
    _tpex_event_candidates,
    _write_immutable_raw,
)
from scripts.run_tw_public_0830_check import _receipt_dependency_errors


def test_tpex_disposition_query_retains_cross_year_notice_and_literal_no_data():
    payload = dict(stat='ok', date='20100101~20101231', tables=[dict(
        fields=['公布日期','證券代號','處置起訖時間','處置內容'], data=[
            ['98/12/31','3577','99/01/04~99/01/08','自99年01月04日起5個營業日'],
            ['99/01/05','','','本日無處置資料']])])
    rows, empty = _parse_tpex_disposition_payload(payload, start=date(2010,1,1), end=date(2010,12,31),
                                                source_sha256='a'*64)
    assert len(rows)==1 and rows[0]['date']=='2009-12-31'
    assert rows[0]['period_start']=='2010-01-04' and rows[0]['source_sha256']=='a'*64
    assert empty==['2010-01-05']
    for values in [['99/01/05','','','未知'], ['99/01/05','3577','','本日無處置資料'],
                   ['99/01/05','3577','98/01/04~98/01/08','5個營業日']]:
        invalid=dict(payload,tables=[dict(payload['tables'][0],data=[values])])
        with pytest.raises(ValueError):
            _parse_tpex_disposition_payload(invalid,start=date(2010,1,1),end=date(2010,12,31),source_sha256='a'*64)
    with pytest.raises(ValueError,match='requested query'):
        _parse_tpex_disposition_payload(dict(payload,date='20090101~20091231'),
            start=date(2010,1,1),end=date(2010,12,31),source_sha256='a'*64)


def _ods_bytes(rows: list[list[str]]) -> bytes:
    xml_rows = []
    for row in rows:
        cells = "".join(
            "<table:table-cell office:value-type='string'>"
            f"<text:p>{value}</text:p></table:table-cell>"
            for value in row
        )
        xml_rows.append(f"<table:table-row>{cells}</table:table-row>")
    content = (
        "<?xml version='1.0' encoding='utf-8'?>"
        "<office:document-content "
        "xmlns:office='urn:oasis:names:tc:opendocument:xmlns:office:1.0' "
        "xmlns:table='urn:oasis:names:tc:opendocument:xmlns:table:1.0' "
        "xmlns:text='urn:oasis:names:tc:opendocument:xmlns:text:1.0'>"
        "<office:body><office:spreadsheet><table:table>"
        + "".join(xml_rows)
        + "</table:table></office:spreadsheet></office:body></office:document-content>"
    ).encode()
    output = BytesIO()
    with ZipFile(output, "w", ZIP_DEFLATED) as archive:
        archive.writestr("content.xml", content)
    return output.getvalue()


def test_parse_twse_corporate_action_reference_payload() -> None:
    payload = {
        "stat": "OK",
        "fields": ["資料日期", "股票代號", "股票名稱", "除權息前收盤價", "除權息參考價", "權/息", "開盤競價基準"],
        "data": [["113年07月16日", "0050", "元大台灣50", "196.70", "195.70", "息", "195.70"]],
    }

    rows = _payload_rows(payload, market="twse", url="https://official.test")

    assert rows[0]["date"].isoformat() == "2024-07-16"
    assert rows[0]["symbol"] == "0050"
    assert rows[0]["reference_price"] == 195.7


def test_parse_tpex_historical_reference_payload() -> None:
    payload = {
        "tables": [
            {
                "fields": ["除權除息交易日", "股票代號", "股票名稱", "漲停價格", "除權參考價", "跌停價格", "開市交易基準價"],
                "data": [["95/01/10", "1333", "恩得利", "54.80", "49.99", "46.50", "51.30"]],
            }
        ]
    }

    rows = _payload_rows(payload, market="tpex", url="https://official.test")

    assert rows[0]["date"].isoformat() == "2006-01-10"
    assert rows[0]["reference_price"] == 49.99
    assert rows[0]["opening_reference_price"] == 51.3


def test_parse_tpex_monthly_query_and_right_ods() -> None:
    payload = {
        "stat": "ok",
        "tables": [
            {
                "title": "除權交易股票一覽表",
                "fields": ["日期", "下載XLS", "下載ODS"],
                "data": [
                    [
                        "93/09",
                        "/zh-tw/statistics/monthlyRptMktDl?doc=341",
                        "/zh-tw/statistics/monthlyRptMktDl?doc=341&isOds=Y",
                    ]
                ],
            }
        ],
    }

    document_id = _monthly_query_document_id(
        payload,
        year=2004,
        month=9,
        report_type=6,
    )
    rows = _parse_tpex_monthly_ods(
        _ods_bytes(
            [
                ["9月除權交易股票一覽表"],
                ["Ex-Right Stocks in Sep. 2004"],
                ["股票名稱", "日期", "除權前參考價", "除權後參考價"],
                ["創惟", "6", "22.7", "18.49"],
            ]
        ),
        year=2004,
        month=9,
        report_type=6,
        document_id=document_id,
        source_url="https://official.test/doc=341",
    )

    assert document_id == "341"
    assert rows == [
        {
            "date": date(2004, 9, 6),
            "report_type": 6,
            "event_type": "right",
            "report_name": "創惟",
            "previous_close": 22.7,
            "reference_price": 18.49,
            "document_id": "341",
            "document_row": 3,
            "source_url": "https://official.test/doc=341",
        }
    ]


def test_parse_tpex_monthly_dividend_uses_pre_and_post_reference_columns() -> None:
    rows = _parse_tpex_monthly_ods(
        _ods_bytes(
            [
                ["10月除息交易股票一覽表"],
                ["股票名稱", "日期", "除息金額", "除息前參考價", "除息後參考價"],
                ["琨詰科技", "12", "0.67", "13.7", "13.03"],
            ]
        ),
        year=2004,
        month=10,
        report_type=7,
        document_id="390",
        source_url="https://official.test/doc=390",
    )

    assert rows[0]["previous_close"] == 13.7
    assert rows[0]["reference_price"] == 13.03


@pytest.mark.parametrize(
    ("year", "month", "title"),
    [
        (2011, 1, "100年1月除權交易股票一覽表"),
        (2012, 4, "101年04月除權交易股票一覽表"),
    ],
)
def test_parse_tpex_monthly_accepts_exact_roc_year_title(
    year: int,
    month: int,
    title: str,
) -> None:
    rows = _parse_tpex_monthly_ods(
        _ods_bytes(
            [
                [title],
                ["股票名稱", "日期", "除權前參考價", "除權後參考價"],
                ["測試公司", "4", "24.90", "24.42"],
            ]
        ),
        year=year,
        month=month,
        report_type=6,
        document_id="1013",
        source_url="https://official.test/doc=1013",
    )

    assert rows[0]["date"] == date(year, month, 4)
    assert rows[0]["reference_price"] == 24.42


def test_parse_tpex_monthly_rejects_wrong_linked_workbook() -> None:
    content = _ods_bytes(
        [
            ["歷年上櫃股票統計"],
            ["年 度", "交易量", "成交值", "成交筆數"],
            ["78年 1989", "14", "913", "14"],
        ]
    )

    with pytest.raises(MonthlyDocumentIdentityError, match="expected title"):
        _parse_tpex_monthly_ods(
            content,
            year=2005,
            month=1,
            report_type=6,
            document_id="345",
            source_url="https://official.test/doc=345",
        )


def _candidate(
    event_date: date,
    symbol: str,
    previous_close: float,
    *,
    direct_reference: float | None = None,
) -> dict[str, object]:
    return {
        "date": event_date,
        "symbol": symbol,
        "name": "測試公司",
        "name_status": "",
        "event_type": "除權息",
        "previous_close": previous_close,
        "previous_daily_next_reference": direct_reference,
        "previous_source_url": "https://official.test/daily",
    }


def _monthly_row(
    event_date: date,
    previous_close: float,
    reference: float,
    *,
    report_type: int,
    document_row: int,
) -> dict[str, object]:
    return {
        "date": event_date,
        "report_type": report_type,
        "event_type": "right" if report_type == 6 else "dividend",
        "report_name": "測試公司",
        "previous_close": previous_close,
        "reference_price": reference,
        "document_id": str(300 + report_type),
        "document_row": document_row,
        "source_url": f"https://official.test/type={report_type}",
    }


def test_resolve_tpex_monthly_accepts_consistent_type6_type7_and_accounts_direct() -> None:
    event_date = date(2004, 9, 9)
    candidates = [
        _candidate(event_date, "6108", 30.0),
        _candidate(date(2004, 11, 3), "6217", 23.2, direct_reference=19.9),
    ]
    monthly_rows = [
        _monthly_row(event_date, 30.0, 26.61, report_type=6, document_row=8),
        _monthly_row(event_date, 30.0, 26.61, report_type=7, document_row=4),
        _monthly_row(date(2004, 11, 3), 23.2, 22.18, report_type=7, document_row=3),
    ]

    rows, stats = _resolve_tpex_monthly_rows(candidates, monthly_rows, [])

    assert {(row["symbol"], row["reference_price"]) for row in rows} == {
        ("6108", 26.61),
        ("6217", 19.9),
    }
    assert stats["monthly_report_keys"] == 1
    assert stats["previous_daily_next_reference_keys"] == 1
    assert stats["duplicate_consistent_monthly_keys"] == 1
    assert stats["unresolved_keys"] == 0
    assert stats["unresolved_months"] == []


def test_resolve_tpex_monthly_reports_every_unresolved_month() -> None:
    rows, stats = _resolve_tpex_monthly_rows(
        [
            _candidate(date(2012, 4, 26), "3303", 14.3),
            _candidate(date(2011, 1, 4), "8111", 24.9),
        ],
        [],
        [],
    )

    assert rows == []
    assert stats["unresolved_keys"] == 2
    assert stats["unresolved_months"] == ["2011-01", "2012-04"]


def test_tpex_event_candidates_can_skip_unmatchable_prior_close() -> None:
    frame = pl.DataFrame(
        {
            "date": ["2011-01-04", "2012-04-25", "2012-04-26"],
            "代號": ["8111", "3303", "3303"],
            "名稱": ["立碁", "岱稜", "岱稜"],
            "收盤": ["25.00", "14.30", "13.75"],
            "漲跌": ["除權", "+ 0.25", "除權"],
            "次日參考價": [None, None, None],
        }
    )

    candidates = _tpex_event_candidates(
        frame,
        start=date(2011, 1, 1),
        end=date(2012, 12, 31),
        require_complete_previous_close=False,
    )

    assert [(row["date"], row["symbol"]) for row in candidates] == [
        (date(2012, 4, 26), "3303")
    ]


def test_resolve_tpex_monthly_rejects_type6_type7_reference_conflict() -> None:
    event_date = date(2004, 9, 9)
    rows = [
        _monthly_row(event_date, 30.0, 26.61, report_type=6, document_row=8),
        _monthly_row(event_date, 30.0, 27.0, report_type=7, document_row=4),
    ]

    with pytest.raises(RuntimeError, match="conflicting TPEx type=6/7"):
        _resolve_tpex_monthly_rows(
            [_candidate(event_date, "6108", 30.0)],
            rows,
            [],
        )


def test_resolve_tpex_monthly_requires_name_unless_daily_receipt_is_unrecoverable() -> None:
    event_date = date(2004, 9, 9)
    candidate = _candidate(event_date, "6108", 30.0)
    row = _monthly_row(event_date, 30.0, 26.61, report_type=6, document_row=8)
    row["report_name"] = "另一家公司"

    with pytest.raises(RuntimeError, match="name does not match"):
        _resolve_tpex_monthly_rows([candidate], [row], [])

    candidate["name_status"] = "official_receipt_name_bytes_unrecoverable"
    resolved, stats = _resolve_tpex_monthly_rows([candidate], [row], [])

    assert resolved[0]["reference_price"] == 26.61
    assert stats["unrecoverable_daily_name_exception_keys"] == 1


def test_resolve_tpex_monthly_allows_only_one_character_official_name_truncation() -> None:
    event_date = date(2004, 9, 30)
    candidate = _candidate(event_date, "8087", 23.5)
    candidate["name"] = "華鎂光"
    row = _monthly_row(event_date, 23.5, 20.54, report_type=6, document_row=8)
    row["report_name"] = "華鎂"

    resolved, stats = _resolve_tpex_monthly_rows([candidate], [row], [])

    assert resolved[0]["reference_price"] == 20.54
    assert stats["normalized_name_alias_keys"] == 1


def test_write_immutable_monthly_raw_is_content_addressed(tmp_path) -> None:
    first = _write_immutable_raw(
        tmp_path,
        stem="type-6-doc-341",
        suffix=".ods",
        content=b"official-ods",
        source_url="https://official.test/doc=341",
    )
    second = _write_immutable_raw(
        tmp_path,
        stem="type-6-doc-341",
        suffix=".ods",
        content=b"revised-official-ods",
        source_url="https://official.test/doc=341",
    )

    assert first["path"] != second["path"]
    assert len(list(tmp_path.glob("*.ods"))) == 2


def test_annual_fetch_preserves_prior_generation_receipt_on_partial_failure(
    tmp_path, monkeypatch
) -> None:
    raw_dir = tmp_path / "raw" / "tw_corporate_action_reference"
    raw_dir.mkdir(parents=True)
    legacy = raw_dir / "twse_2026.json"
    original = b"previous verified official response"
    legacy.write_bytes(original)
    summary = tmp_path / "reference.summary.json"
    summary.write_text(json.dumps({"source_receipts": [{
        "path": str(legacy), "size": len(original),
        "sha256": hashlib.sha256(original).hexdigest(),
    }]}))
    payload = {
        "stat": "OK",
        "fields": ["資料日期", "股票代號", "除權息參考價"],
        "data": [["115年10月27日", "2330", "600.00"]],
    }
    content = json.dumps(payload, ensure_ascii=False).encode()

    def fetch(url, **kwargs):
        if "tpex" in url:
            raise TimeoutError("another required source failed")
        return SimpleNamespace(url=url, content=content, json=lambda: payload)

    monkeypatch.setattr(reference, "_http_get", fetch)
    args = SimpleNamespace(output_dir=tmp_path, timeout=1, retries=0, skip_raw=False)
    success = reference._fetch_year("twse", 2026, date(2026, 10, 27), args)
    failure = reference._fetch_year("tpex", 2026, date(2026, 10, 27), args)

    assert success.error is None and failure.error is not None
    assert legacy.read_bytes() == original
    assert success.raw_path != str(legacy)
    assert success.raw_receipt["sha256"] == hashlib.sha256(content).hexdigest()
    assert _receipt_dependency_errors(summary, live_root=tmp_path, keys=("source_receipts",)) == []


def test_annual_fetch_revisions_remain_distinct_and_skip_raw_is_respected(
    tmp_path, monkeypatch
) -> None:
    payload = {
        "stat": "OK",
        "fields": ["資料日期", "股票代號", "除權息參考價"],
        "data": [["115年10月27日", "2330", "600.00"]],
    }
    contents = [b"first official bytes", b"revised official bytes", b"not retained"]
    monkeypatch.setattr(reference, "_http_get", lambda url, **kwargs:
        SimpleNamespace(url=url, content=contents.pop(0), json=lambda: payload))
    args = SimpleNamespace(output_dir=tmp_path, timeout=1, retries=0, skip_raw=False)
    first = reference._fetch_year("twse", 2026, date(2026, 10, 27), args)
    second = reference._fetch_year("twse", 2026, date(2026, 10, 27), args)
    args.skip_raw = True
    third = reference._fetch_year("twse", 2026, date(2026, 10, 27), args)

    assert first.error is second.error is third.error is None
    assert first.raw_path != second.raw_path
    assert reference.Path(first.raw_path).read_bytes() == b"first official bytes"
    assert reference.Path(second.raw_path).read_bytes() == b"revised official bytes"
    assert third.raw_path is None and third.raw_receipt is None


def _annual_daily_fixture(tmp_path, monkeypatch):
    payload = {
        "stat": "OK", "strDate": "20260101", "endDate": "20261027",
        "fields": ["資料日期", "股票代號", "除權息參考價"],
        "data": [["115年10月27日", "2330", "600.00"]],
    }
    content = json.dumps(payload, ensure_ascii=False).encode()
    monkeypatch.setattr(reference, "_http_get", lambda url, **kwargs:
        SimpleNamespace(url=url, content=content, json=lambda: payload))
    args = SimpleNamespace(output_dir=tmp_path, timeout=1, retries=0, skip_raw=False, mode="daily")
    original = reference._fetch_year("twse", 2026, date(2026, 10, 27), args)
    assert original.error is None
    path = reference._annual_cache_path(
        tmp_path / "raw" / "tw_corporate_action_reference", "twse", 2026, date(2026, 10, 27)
    )
    assert path.is_file()
    return args, original, path


def test_annual_daily_retry_reuses_only_recent_verified_official_success(tmp_path, monkeypatch):
    args, original, _ = _annual_daily_fixture(tmp_path, monkeypatch)
    def timeout(*args, **kwargs):
        raise ReadTimeout("upstream is slow")
    monkeypatch.setattr(reference, "_http_get", timeout)

    recovered = reference._fetch_year("twse", 2026, date(2026, 10, 27), args)

    assert recovered.error is None
    assert recovered.rows == original.rows
    assert recovered.raw_receipt["sha256"] == original.raw_receipt["sha256"]
    assert recovered.raw_receipt["observed_at_utc"] == original.raw_receipt["observed_at_utc"]
    assert recovered.raw_receipt["retry_cache_reused"] is True
    assert recovered.raw_receipt["retry_failure_type"] == "ReadTimeout"


@pytest.mark.parametrize("defect", ["expired", "future", "digest", "end_date", "source_url", "content"])
def test_annual_daily_retry_rejects_stale_foreign_or_corrupt_cache(tmp_path, monkeypatch, defect):
    args, original, path = _annual_daily_fixture(tmp_path, monkeypatch)
    entry = json.loads(path.read_text())
    if defect == "expired":
        entry["observed_at_utc"] = (datetime.now(timezone.utc) - timedelta(seconds=301)).isoformat()
    elif defect == "future":
        entry["observed_at_utc"] = (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat()
    elif defect == "digest":
        entry["raw_receipt"]["sha256"] = "f" * 64
    elif defect == "end_date":
        entry["end_date"] = "2026-10-26"
    elif defect == "source_url":
        entry["raw_receipt"]["source_url"] = "https://example.invalid/data"
    else:
        reference.Path(original.raw_path).write_bytes(b"corrupted")
    path.write_text(json.dumps(entry))
    def timeout(*args, **kwargs):
        raise ReadTimeout("upstream is slow")
    monkeypatch.setattr(reference, "_http_get", timeout)

    recovered = reference._fetch_year("twse", 2026, date(2026, 10, 27), args)

    assert recovered.error and "ReadTimeout" in recovered.error
    assert recovered.raw_receipt is None


@pytest.mark.parametrize("failure", [HTTPError("quota or access rejected"), ValueError("bad response")])
def test_annual_retry_never_hides_permission_or_semantic_failures(tmp_path, monkeypatch, failure):
    args, _, _ = _annual_daily_fixture(tmp_path, monkeypatch)
    def fail(*args, **kwargs):
        raise failure
    monkeypatch.setattr(reference, "_http_get", fail)
    result = reference._fetch_year("twse", 2026, date(2026, 10, 27), args)
    assert result.error and type(failure).__name__ in result.error
    assert result.raw_receipt is None


def test_annual_retry_is_daily_only_and_rejects_foreign_query_bounds(tmp_path, monkeypatch):
    args, _, _ = _annual_daily_fixture(tmp_path, monkeypatch)
    def timeout(*args, **kwargs):
        raise ReadTimeout("upstream is slow")
    monkeypatch.setattr(reference, "_http_get", timeout)
    args.mode = "repair"
    assert reference._fetch_year("twse", 2026, date(2026, 10, 27), args).error
    payload = {"stat": "OK", "strDate": "20250101", "endDate": "20251231", "fields": [], "data": []}
    monkeypatch.setattr(reference, "_http_get", lambda url, **kwargs:
        SimpleNamespace(url=url, content=json.dumps(payload).encode(), json=lambda: payload))
    result = reference._fetch_year("twse", 2026, date(2026, 10, 27), args)
    assert result.error and "bounds differ" in result.error


def test_annual_twse_uses_second_official_host_after_transport_timeout(tmp_path, monkeypatch):
    payload = {
        "stat": "OK", "strDate": "20260101", "endDate": "20261027",
        "fields": ["資料日期", "股票代號", "除權息參考價"],
        "data": [["115年10月27日", "2330", "600.00"]],
    }
    content = json.dumps(payload, ensure_ascii=False).encode()
    calls = []
    def fetch(url, **kwargs):
        calls.append((url, kwargs["retries"]))
        if url == reference.TWSE_URL:
            raise ReadTimeout("primary timed out")
        return SimpleNamespace(url=url, content=content, json=lambda: payload,
                               _stockagent_response_attempts=2)
    monkeypatch.setattr(reference, "_http_get", fetch)
    args = SimpleNamespace(output_dir=tmp_path, timeout=1, retries=2, skip_raw=False, mode="daily")

    result = reference._fetch_year("twse", 2026, date(2026, 10, 27), args)

    assert result.error is None
    assert calls == [(reference.TWSE_URL, 0), (reference.TWSE_ALTERNATE_URL, 2)]
    assert result.url == reference.TWSE_ALTERNATE_URL
    assert [row.get("error_type") for row in result.raw_receipt["transport_attempts"]] == ["ReadTimeout", None]
    assert result.http_attempt_count == 3


def test_annual_partial_retry_reuses_only_original_successes_across_attempts(tmp_path, monkeypatch):
    args = SimpleNamespace(output_dir=tmp_path, timeout=1, retries=0, skip_raw=False, mode="daily")
    stage = {"attempt": 1}
    def fetch(url, **kwargs):
        start = kwargs["params"]["startDate"]
        year = int(start[:4])
        if (stage["attempt"] == 1 and year == 2026) or (stage["attempt"] == 2 and year == 2025):
            raise ReadTimeout("one annual source is temporarily unavailable")
        end = "20251231" if year == 2025 else "20261027"
        payload = {"stat": "OK", "strDate": f"{year}0101", "endDate": end,
            "fields": ["資料日期", "股票代號", "除權息參考價"],
            "data": [[f"{year - 1911}年10月27日", "2330", "600.00"]]}
        return SimpleNamespace(url=url, content=json.dumps(payload, ensure_ascii=False).encode(), json=lambda: payload)
    monkeypatch.setattr(reference, "_http_get", fetch)

    first_success = reference._fetch_year("twse", 2025, date(2026, 10, 27), args)
    first_failure = reference._fetch_year("twse", 2026, date(2026, 10, 27), args)
    stage["attempt"] = 2
    second_recovered = reference._fetch_year("twse", 2025, date(2026, 10, 27), args)
    second_success = reference._fetch_year("twse", 2026, date(2026, 10, 27), args)

    assert first_success.error is None and first_failure.error
    assert second_recovered.error is None and second_success.error is None
    assert second_recovered.raw_receipt["retry_cache_reused"] is True
    assert second_recovered.raw_receipt["sha256"] == first_success.raw_receipt["sha256"]
    assert second_recovered.raw_receipt["transport_attempts"] == first_success.raw_receipt["transport_attempts"]
    assert [attempt["error_type"] for attempt in second_recovered.raw_receipt["retry_transport_attempts"]] == ["ReadTimeout", "ReadTimeout"]
    assert second_recovered.http_attempt_count == 2
    assert second_success.raw_receipt["retry_cache_reused"] is False


def test_monthly_request_count_includes_actual_transport_retries(tmp_path, monkeypatch):
    args = SimpleNamespace(output_dir=tmp_path, timeout=1, retries=2, skip_raw=True)

    def fetch(url, **kwargs):
        count = 2 if url == reference.TPEX_MONTHLY_REPORT_URL else 3
        return SimpleNamespace(url=url, content=b"official", json=lambda: {},
                               _stockagent_response_attempts=count)

    monkeypatch.setattr(reference, "_http_get", fetch)
    monkeypatch.setattr(reference, "_monthly_query_document_id", lambda *a, **k: "document-id")
    monkeypatch.setattr(reference, "_parse_tpex_monthly_ods", lambda *a, **k: [{"symbol": "2330"}])
    result = reference._fetch_tpex_monthly_document(2026, 10, 1, args)
    assert result.status == "parsed"
    assert result.http_attempt_count == 5
