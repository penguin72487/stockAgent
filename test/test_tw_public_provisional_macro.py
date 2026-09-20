from datetime import date, timedelta
import json
from pathlib import Path

import polars as pl

from downloader.download_tw_cbc_overnight_pages import parse_page
from downloader.download_tw_mof_macro_release_dates import (
    parse_listing as parse_mof_listing, parse_tax_pdf_text_release_date,
    parse_next_tax_release_schedule_text,
)
from downloader.download_tw_mof_trade_release_values import parse_trade_pdf_text
from stockagent.data.tw_public_provisional_macro import FEATURES, build_provisional_macro_events


def _source(root: Path, name: str, rows: list[dict]) -> None:
    enriched = [{**row, "_downloaded_at_utc": "2026-09-17T10:00:00+00:00",
                 "_url": f"https://official.example/{name}"} for row in rows]
    pl.DataFrame(enriched).write_parquet(root / f"{name}.parquet")


def test_cbc_overnight_page_parser_checks_schema_and_page_count() -> None:
    body = b"""
    <section class="lp"><table class="rwd-table"><tr><th>\xe6\xa8\x99\xe9\xa1\x8c(\xe9\xa1\xaf\xe7\xa4\xba\xe8\xb3\x87\xe6\x96\x99\xe6\x97\xa5\xe6\x9c\x9f)</th><th>\xe5\x88\xa9\xe7\x8e\x87</th></tr>
    <tr><td>2024/05/10</td><td>0.817</td></tr></table></section>
    <p>\xe5\x85\xb110\xe7\xad\x86\xe8\xb3\x87\xe6\x96\x99\xef\xbc\x8c\xe7\xac\xac1/2\xe9\xa0\x81</p>
    """
    rows, pages, total = parse_page(body)
    assert pages == 2
    assert total == 10
    assert rows == [{"subject_date": "2024-05-10", "rate_pct": 0.817}]


def test_mof_release_listing_uses_post_date_not_subject_month() -> None:
    body = """<div class='application'><table class='table-list'><thead><tr>
    <th>序號</th><th>標題</th><th>發布日期</th></tr></thead><tbody>
    <tr><td>1</td><td data-title='標題：'><a href='/singlehtml/id?cntId=abc'>
    113年4月海關進出口貿易初步統計</a></td>
    <td data-title='發布日期：'>2024-05-09</td></tr>
    </tbody></table></div><div id='pages'><p>(總共 1 頁，1 筆資料)</p></div>""".encode()
    rows, pages, advertised, listed = parse_mof_listing(body, "trade")
    assert (pages, advertised, listed) == (1, 1, 1)
    assert rows[0]["period"] == "2024-04"
    assert rows[0]["published_on"] == "2024-05-09"


def test_mof_press_pdf_rounded_ntd_units_are_explicit() -> None:
    text = """115 年 8 月海關進出口貿易初步統計
    按美元計算（億美元）
    出口 824.0
    按新臺幣計算（億元）
    出口 26,558 9,157 52.6
    進口 19,372 6,966 56.2
    出超 7,186 2,191 43.9
    """
    assert parse_trade_pdf_text(text, period="2026-08") == {
        "exports": 2_655_800_000, "imports": 1_937_200_000,
        "balance": 718_600_000,
    }


def test_mof_tax_original_pdf_release_date_checks_subject_period() -> None:
    text = """107 年 4 月全國賦稅收入初步統計
    財政部新聞稿
    107 年 5 月 10 日發布"""
    assert parse_tax_pdf_text_release_date(text, "2018-04") == date(2018, 5, 10)
    assert parse_next_tax_release_schedule_text(
        "下次發布日期：107 年 9 月 11 日下午 4 時", next_period="2018-08"
    ) == (date(2018, 9, 11), "16:00:00")


def test_provisional_macro_recovers_values_without_promoting_pit(tmp_path: Path) -> None:
    sessions = [date(2024, 5, 1) + timedelta(days=i) for i in range(62)]
    sessions = [day for day in sessions if day.weekday() < 5]
    pl.DataFrame({"date": sessions}).write_parquet(tmp_path / "twse_taiex_ohlc.parquet")
    _source(tmp_path, "cbc_usdtwd_closing_rate", [
        {"日期": "20240510", "NTD/USD": "32"},
        {"日期": "20240513", "NTD/USD": "33"},
    ])
    _source(tmp_path, "cbc_overnight_rate", [
        {"日期": "2024/5/10", "利率[%]": "0.8"},
        {"日期": "2024/5/10", "利率[%]": "0.7"},
        {"日期": "2024/5/13", "利率[%]": "0.9"},
    ])
    _source(tmp_path, "cbc_money_aggregates", [{
        "期間": "2024M04", "貨幣總計數-Ｍ１Ｂ-原始值": "100",
        "貨幣總計數-Ｍ２-原始值": "200",
    }])
    _source(tmp_path, "dgbas_cpi_basic", [{
        "Item": "總指數(指數基期：民國110年=100)", "TYPE": "原始值",
        "TIME_PERIOD": "2024M04", "Item_VALUE": "105.2",
    }])
    _source(tmp_path, "dgbas_gdp_expenditure_sa", [{
        "Item": "金額(新臺幣百萬元)、當期價格_1.國內生產毛額:2--3合計",
        "TYPE": "原始值", "TIME_PERIOD": "2024Q1", "Item_VALUE": "1000000",
    }])
    _source(tmp_path, "mof_customs_trade", [{
        "年度": "113", "月份": "4", "出口總值(新臺幣千元)": "100",
        "進口總值(新臺幣千元)": "90", "出入超(新臺幣千元)": "10",
    }])
    _source(tmp_path, "mof_tax_revenue", [
        {"稅目別": "113年 4月", "總計": "100", "證券交易稅": "20",
         "期貨交易稅": "2", "營業稅": "25"},
        {"稅目別": "113年 5月", "總計": "90", "證券交易稅": "-2",
         "期貨交易稅": "1", "營業稅": "-5"},
    ])
    pl.DataFrame([{"period": "2024-04", "published_on": "2024-05-10",
                   "release_url": "https://cbc.example/1",
                   "published_time_precision": "official_date_only"}]).write_parquet(
        tmp_path / "cbc_money_release_vintages.parquet"
    )
    pl.DataFrame([
        {"source": "cpi", "period": "2024-04", "published_on": "2024-05-10",
         "release_url": "https://dgbas.example/cpi", "published_time_precision": "official_document_time",
         "published_clock_taipei": "08:30:00"},
        {"source": "gdp", "period": "2024-Q1", "published_on": "2024-05-10",
         "release_url": "https://dgbas.example/gdp", "published_time_precision": "official_document_time",
         "published_clock_taipei": "16:00:00"},
    ]).write_parquet(tmp_path / "dgbas_release_vintages.parquet")
    supplemental = tmp_path / "supplemental"
    supplemental.mkdir()
    pl.DataFrame([{"subject_date": "2024-05-10", "rate_pct": 0.8,
                   "status": "ok", "page_url": "https://www.cbc.gov.tw/tw/lp-641-1.html",
                   "observed_at_utc": "2026-09-17T11:00:00+00:00"}]).write_parquet(
        supplemental / "cbc_overnight_official_pages.parquet"
    )
    pl.DataFrame([
        {"series": "trade", "period": "2024-04", "published_on": "2024-05-09",
         "release_url": "https://www.mof.gov.tw/singlehtml/trade"},
        {"series": "tax", "period": "2024-04", "published_on": "2024-05-13",
         "release_url": "https://www.mof.gov.tw/singlehtml/tax"},
    ]).write_parquet(supplemental / "mof_macro_release_dates.parquet")
    pl.DataFrame([{
        "period": "2024-05", "exports": 100_000_000.0,
        "imports": 90_000_000.0, "balance": 10_000_000.0,
        "precision_twd_thousand": 100_000,
        "pdf_url": "https://service.mof.gov.tw/example.pdf",
        "observed_at_utc": "2026-09-17T12:00:00+00:00",
    }]).write_parquet(supplemental / "mof_trade_release_values.parquet")
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "mof_macro_release_dates.json").write_text(json.dumps({
        "tax_pdf_fallback": {"scheduled_inferences": [{
            "period": "2024-05", "scheduled_on": "2024-06-11",
            "scheduled_clock_taipei": "16:00:00",
            "evidence_url": "https://service.mof.gov.tw/prior.pdf",
        }]}
    }), encoding="utf-8")

    frame, summary = build_provisional_macro_events(tmp_path)
    assert set(frame.get_column("feature")) == set(FEATURES)
    assert summary["ambiguous_bulk_keys"] == []
    assert summary["strict_pit_eligible_rows"] == 0
    assert not frame.get_column("strict_pit_eligible").any()
    by_feature = {feature: frame.filter(pl.col("feature") == feature)
                  for feature in FEATURES}
    assert by_feature["twpub_usdtwd_log"].filter(pl.col("subject_period") == "2024-05-10").item(0, "effective_session") == date(2024, 5, 13)
    assert by_feature["twpub_dgbas_cpi_log"].item(0, "effective_session") == date(2024, 5, 10)
    assert by_feature["twpub_dgbas_gdp_log"].item(0, "effective_session") == date(2024, 5, 13)
    assert by_feature["twpub_cbc_overnight_rate"].filter(pl.col("subject_period") == "2024-05-10").item(0, "source_dataset") == "cbc_overnight_official_pages"
    assert by_feature["twpub_cbc_m1b_log"].item(0, "publication_time_basis") == "official_release_date_inferred_clock"
    assert by_feature["twpub_mof_export_log"].item(0, "published_at_taipei").startswith("2024-05-09")
    assert by_feature["twpub_mof_tax_total_log"].item(0, "published_at_taipei").startswith("2024-05-13")
    rounded = by_feature["twpub_mof_export_log"].filter(pl.col("subject_period") == "2024-05")
    assert rounded.item(0, "source_dataset") == "mof_trade_release_values"
    assert rounded.item(0, "source_value_precision") == "nearest_100000_twd_thousand"
    assert rounded.item(0, "strict_pit_eligible") is False
    negative_tax = by_feature["twpub_mof_business_tax_log"].filter(pl.col("subject_period") == "2024-05")
    assert negative_tax.item(0, "source_value") == -5
    assert negative_tax.item(0, "feature_value") is None
    assert negative_tax.item(0, "transform_status") == "outside_existing_log_domain"
    raw_negative_tax = by_feature["twpub_mof_business_tax_raw"].filter(
        pl.col("subject_period") == "2024-05"
    )
    assert raw_negative_tax.item(0, "feature_value") == -5
    assert raw_negative_tax.item(0, "transform_status") == "raw"
    assert raw_negative_tax.item(0, "strict_pit_eligible") is False
    assert negative_tax.item(0, "published_at_taipei").startswith("2024-06-11")
    assert negative_tax.item(0, "publication_time_basis") == "estimated_official_advance_schedule"
    assert summary["feature_coverage"]["twpub_mof_business_tax_log"]["raw_value_only_rows"] == 1
