from __future__ import annotations

from datetime import date
import hashlib
import json

import polars as pl
import pytest

from downloader.download_tw_corporate_action_entitlements import (
    BulkDividendKey,
    DetailKey,
    ListingKey,
    _collapse_bulk_event_rows,
    _mops_throttle_response,
    _record_raw_receipt_request,
    _requested_listing_keys,
    _reset_raw_receipt_requests,
    _verified_reference_receipt,
    _write_content_addressed_receipt_manifest,
    parse_mops_detail,
    parse_mops_bulk_dividends,
    parse_mops_listing,
)


def test_mops_http_200_throttle_body_is_not_an_official_receipt() -> None:
    assert _mops_throttle_response(
        "Overrun - 查詢過於頻繁,請稍後再試!! Too many query requests".encode(
            "utf-8"
        )
    )
    assert not _mops_throttle_response(b"<html>issuer disclosure</html>")


def test_listing_workload_adds_prior_year_only_for_first_quarter() -> None:
    events = pl.DataFrame(
        {
            "market": ["twse", "twse"],
            "symbol": ["2330", "2330"],
            "date": [date(2024, 1, 3), date(2024, 7, 3)],
        }
    )

    assert _requested_listing_keys(events) == [
        ListingKey(market="twse", symbol="2330", roc_year=112),
        ListingKey(market="twse", symbol="2330", roc_year=113),
    ]


def test_parse_mops_bulk_dividend_extracts_exact_cash_terms() -> None:
    cells = [
        "2330",
        "台積電",
        "112年度",
        "113/07/09",
        "",
        "",
        "",
        "10.0",
        "0.5",
        "",
        "113/07/03",
        "113/07/31",
        "0",
        "0",
        "0",
        "5,000,000,000",
        "113/06/20",
        "15:30:00",
        "新台幣10.0000元",
    ]
    html = (
        "<html><table>"
        "<tr><th>公司代號</th><th>現金股利發放日</th></tr>"
        "<tr>"
        + "".join(f"<td>{value}</td>" for value in cells)
        + "</tr></table></html>"
    ).encode("utf-8")

    rows = parse_mops_bulk_dividends(
        html, key=BulkDividendKey(market="twse", roc_year=113)
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["date"] == date(2024, 7, 3)
    assert row["record_date"] == date(2024, 7, 9)
    assert row["cash_dividend_per_share"] == pytest.approx(10.5)
    assert row["cash_payment_date"] == date(2024, 7, 31)
    assert row["stock_dividend_ratio"] == 0.0
    assert row["subscription_ratio"] == 0.0
    assert row["stop_transfer_start"] is None


@pytest.mark.parametrize(
    ("cells", "expected_cash", "expected_ex_date", "expected_payment"),
    [
        (
            [
                "2330", "台積電", "93年", "94/07/10", "", "",
                "", "", "", "", "", "2.0", "0.25",
                "94/07/04", "94/08/01", "", "", "", "", "",
                "94/06/20", "09:30:00", "新台幣10.0000元",
            ],
            2.25,
            date(2005, 7, 4),
            date(2005, 8, 1),
        ),
        (
            [
                "2330", "台積電", "104年", "105/07/10", "", "",
                "", "3.0", "0.5", "", "105/07/04", "105/08/01",
                "", "", "", "105/06/20", "09:30:00",
                "新台幣10.0000元",
            ],
            3.5,
            date(2016, 7, 4),
            date(2016, 8, 1),
        ),
    ],
)
def test_parse_mops_bulk_dividend_supports_historical_layouts(
    cells: list[str],
    expected_cash: float,
    expected_ex_date: date,
    expected_payment: date,
) -> None:
    html = (
        "<html><table><tr><th>公司代號</th><th>現金股利發放日</th></tr><tr>"
        + "".join(f"<td>{value}</td>" for value in cells)
        + "</tr></table></html>"
    ).encode("utf-8")

    rows = parse_mops_bulk_dividends(
        html, key=BulkDividendKey(market="twse", roc_year=94)
    )

    assert len(rows) == 1
    assert rows[0]["date"] == expected_ex_date
    assert rows[0]["cash_dividend_per_share"] == pytest.approx(expected_cash)
    assert rows[0]["cash_payment_date"] == expected_payment


def test_parse_mops_bulk_dividend_keeps_latest_official_correction() -> None:
    base = [
        "7610", "聯友金屬-創", "112年", "113/07/19", "", "", "",
        "0.1", "", "", "113/07/11", "113/08/08", "", "", "",
        "30,000,000", "113/06/28", "10:20:25", "新台幣10.0000元",
    ]
    corrected = list(base)
    corrected[7] = "0.2"
    corrected[17] = "11:46:40"
    html = (
        "<html><table><tr><th>公司代號</th><th>現金股利發放日</th></tr>"
        + "".join(
            "<tr>" + "".join(f"<td>{value}</td>" for value in row) + "</tr>"
            for row in (base, corrected)
        )
        + "</table></html>"
    ).encode("utf-8")

    rows = parse_mops_bulk_dividends(
        html, key=BulkDividendKey(market="twse", roc_year=113)
    )

    assert len(rows) == 1
    assert rows[0]["cash_dividend_per_share"] == pytest.approx(0.2)


def test_parse_mops_bulk_dividend_ignores_old_preferred_pseudo_symbol() -> None:
    common = [
        "8084", "巨虹", "94年", "95/08/29", "", "", "", "", "", "",
        "0", "0.2", "", "95/08/23", "95/09/20", "", "", "", "", "",
        "95/08/08", "16:13:12", "無面額",
    ]
    preferred = list(common)
    preferred[0] = "8084*"
    preferred[1] = "特別股*"
    html = (
        "<html><table><tr><th>公司代號</th><th>現金股利發放日</th></tr>"
        + "".join(
            "<tr>" + "".join(f"<td>{value}</td>" for value in row) + "</tr>"
            for row in (common, preferred)
        )
        + "</table></html>"
    ).encode("utf-8")

    rows = parse_mops_bulk_dividends(
        html, key=BulkDividendKey(market="tpex", roc_year=95)
    )

    assert len(rows) == 1
    assert rows[0]["symbol"] == "8084"


def test_bulk_cross_year_correction_uses_latest_announcement() -> None:
    original = {
        "date": date(2021, 3, 11),
        "symbol": "3680",
        "market": "tpex",
        "announcement_date": date(2020, 12, 25),
        "record_date": date(2021, 3, 17),
        "cash_dividend_per_share": 1.5,
        "cash_payment_date": date(2021, 4, 9),
        "stock_dividend_ratio": 0.0,
        "subscription_ratio": 0.0,
        "stock_terms_complete": True,
    }
    correction = {
        **original,
        "announcement_date": date(2021, 2, 19),
        "cash_dividend_per_share": 1.36147979,
    }

    rows = _collapse_bulk_event_rows([original, correction])

    assert len(rows) == 1
    assert rows[0]["cash_dividend_per_share"] == pytest.approx(1.36147979)


def test_multiple_action_kinds_on_one_ex_date_fail_closed() -> None:
    cash = {
        "date": date(2010, 7, 1),
        "symbol": "6224",
        "market": "twse",
        "announcement_date": date(2010, 6, 15),
        "record_date": date(2010, 7, 9),
        "cash_dividend_per_share": 2.48643,
        "cash_payment_date": None,
        "stock_dividend_ratio": 0.0,
        "subscription_ratio": 0.0,
        "stock_terms_complete": True,
    }
    subscription = {
        **cash,
        "cash_dividend_per_share": 0.0,
        "subscription_ratio": 0.06196397887,
    }

    rows = _collapse_bulk_event_rows([cash, subscription])

    assert len(rows) == 1
    assert rows[0]["cash_dividend_per_share"] == pytest.approx(2.48643)
    assert rows[0]["subscription_ratio"] > 0.0
    assert rows[0]["stock_terms_complete"] is False


def test_parse_mops_listing_deduplicates_detail_identity() -> None:
    onclick = (
        'DATE1.value="20240620";document.t108sb22_fm1.SEQ_NO.value="1";'
        'document.t108sb22_fm1.COMP.value="2330"'
    )
    content = f"<html><button onclick='{onclick}'>detail</button>{onclick}</html>".encode()

    rows = parse_mops_listing(
        content,
        key=ListingKey(market="twse", symbol="2330", roc_year=113),
    )

    assert rows == [
        DetailKey(
            market="twse",
            symbol="2330",
            announcement_date=date(2024, 6, 20),
            sequence=1,
        )
    ]


def test_parse_mops_detail_extracts_exact_cash_payment_terms() -> None:
    html = """
    <html><body>
      <table>
        <tr><td>公司代號</td><td>2330</td></tr>
        <tr><td>四、股票停止過戶起訖日期：</td><td>113年07月05日至113年07月09日</td></tr>
        <tr><td>（八）權利分派基準日：</td><td>113年07月09日</td></tr>
        <tr><td>除權/除息交易日：</td><td>113年07月03日</td></tr>
        <tr><td>＊現金股利發放日：</td><td>113年07月31日</td></tr>
        <tr><td>※除息--普通股：每壹股配發現金(股利) 10.50000000 元</td></tr>
        <tr><td>※除權--普通股：每壹股配發股票(股利) 0 元</td></tr>
      </table>
    </body></html>
    """.encode("utf-8")
    key = DetailKey(
        market="twse",
        symbol="2330",
        announcement_date=date(2024, 6, 20),
        sequence=1,
    )

    row = parse_mops_detail(html, key=key)

    assert row["date"] == date(2024, 7, 3)
    assert row["record_date"] == date(2024, 7, 9)
    assert row["stop_transfer_start"] == date(2024, 7, 5)
    assert row["stop_transfer_end"] == date(2024, 7, 9)
    assert row["cash_dividend_per_share"] == pytest.approx(10.5)
    assert row["cash_payment_date"] == date(2024, 7, 31)
    assert row["stock_dividend_ratio"] == 0.0
    assert row["subscription_ratio"] == 0.0
    assert row["stock_terms_complete"] is True


def test_parse_mops_detail_keeps_missing_payment_date_for_avoidance() -> None:
    html = """
    <html><body><table>
      <tr><td>公司代號</td><td>2330</td></tr>
      <tr><td>除權/除息交易日：</td><td>113年07月03日</td></tr>
      <tr><td>※除息--普通股：每壹股配發現金(股利) 10 元</td></tr>
    </table></body></html>
    """.encode("utf-8")

    row = parse_mops_detail(
        html,
        key=DetailKey(
            market="twse",
            symbol="2330",
            announcement_date=date(2024, 6, 20),
            sequence=1,
        ),
    )

    assert row["cash_dividend_per_share"] == pytest.approx(10.0)
    assert row["cash_payment_date"] is None


def test_entitlement_input_rejects_tampered_reference(tmp_path) -> None:
    path = tmp_path / "tw_corporate_action_reference.parquet"
    pl.DataFrame(
        {
            "date": [date(2024, 7, 3)],
            "symbol": ["2330"],
            "reference_price": [990.0],
            "event_type": ["息"],
        }
    ).write_parquet(path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    path.with_suffix(".summary.json").write_text(
        json.dumps(
            {
                "baseline_established": True,
                "coverage_complete": True,
                "failure_count": 0,
                "schema_version": 3,
                "rows": 1,
                "output_receipt": {
                    "size": path.stat().st_size,
                    "sha256": digest,
                },
            }
        ),
        encoding="utf-8",
    )

    receipt = _verified_reference_receipt(path)
    assert receipt["rows"] == 1

    path.write_bytes(path.read_bytes() + b"tamper")
    with pytest.raises(ValueError, match="size receipt mismatch"):
        _verified_reference_receipt(path)


def test_raw_receipt_manifest_binds_request_and_response(tmp_path) -> None:
    output_dir = tmp_path / "public"
    raw_root = output_dir / "raw" / "tw_corporate_action_entitlements"
    receipt_path = raw_root / "lists" / "twse-2330-113-v1.html"
    receipt_path.parent.mkdir(parents=True)
    receipt_path.write_bytes(b"<html>official response</html>")
    request_data = {
        "TYPEK": "sii",
        "co_id": "2330",
        "year": "113",
    }
    _reset_raw_receipt_requests()
    _record_raw_receipt_request(
        receipt_path,
        url="https://example.test/mops",
        data=request_data,
        content=receipt_path.read_bytes(),
    )

    receipt = _write_content_addressed_receipt_manifest(
        output_dir=output_dir,
        raw_root=raw_root,
    )

    manifest_path = output_dir / receipt["relative_path"]
    assert manifest_path.stem == receipt["sha256"]
    assert receipt["entries"] == 1
    row = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert row["path"] == (
        "raw/tw_corporate_action_entitlements/lists/twse-2330-113-v1.html"
    )
    assert row["request"]["data"] == request_data
    assert row["response_sha256"] == hashlib.sha256(
        receipt_path.read_bytes()
    ).hexdigest()

    receipt_path.write_bytes(b"<html>tampered after parse</html>")
    with pytest.raises(RuntimeError, match="changed after it was parsed"):
        _write_content_addressed_receipt_manifest(
            output_dir=output_dir,
            raw_root=raw_root,
        )
