from __future__ import annotations

from datetime import date
from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from downloader.download_tw_corporate_action_reference import (
    MonthlyDocumentIdentityError,
    _monthly_query_document_id,
    _parse_tpex_monthly_ods,
    _payload_rows,
    _resolve_tpex_monthly_rows,
    _write_immutable_raw,
)


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
