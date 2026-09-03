from __future__ import annotations

from scripts.download_taifex_openapi_catalog import (
    _json_rows,
    _parse_structured_content,
    _slug,
)


def test_openapi_slug_is_stable_and_filesystem_safe() -> None:
    assert _slug("/DailyOptionsDelta") == "daily_options_delta"
    assert _slug("/va01") == "va01"


def test_openapi_rows_preserve_nested_values_as_json() -> None:
    rows = _json_rows([{"Date": "2026/09/01", "nested": {"b": 2, "a": 1}}])
    assert rows == [
        {"Date": "2026/09/01", "nested": '{"a":1,"b":2}'}
    ]


def test_openapi_parser_accepts_octet_stream_csv_with_utf8_bom() -> None:
    content = "\ufeff日期,商品代碼\n2026/09/01,TX\n".encode()
    payload, payload_format = _parse_structured_content(
        content, "application/octet-stream"
    )

    assert payload_format == "csv"
    assert payload == [{"日期": "2026/09/01", "商品代碼": "TX"}]
