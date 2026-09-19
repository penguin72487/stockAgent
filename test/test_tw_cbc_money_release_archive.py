from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from downloader.download_tw_cbc_money_release_archive import parse_detail, parse_listing
from stockagent.data.tw_public_features import _build_cbc_monthly_macro_features


def test_listing_accepts_original_roc_and_chinese_periods() -> None:
    body = """<html><body>
    <li><time>2026-08-24</time><a href="/tw/cp-302-123-ABC-1.html">115年7月金融情況</a></li>
    <li><time>2004-04-26</time><a href="/tw/cp-302-124-ABC-1.html">新聞發布第064號(九十三年三月金融情況)</a></li>
    <li><time>2026-08-24</time><a href="/tw/cp-302-125-ABC-1.html">金融情況政策說明</a></li>
    <p>共7832筆資料，第1/392頁</p>
    </body></html>""".encode()
    rows, pages = parse_listing(body)
    assert pages == 392
    assert [row["period"] for row in rows] == ["2026-07", "2004-03"]


def test_undated_early_headline_requires_matching_body_period() -> None:
    listing = """<li><time>2000-01-25</time><a href="/tw/cp-302-23076-460B9-1.html">金融情況</a></li>
    <p>第1/1頁</p>""".encode()
    rows, _ = parse_listing(listing)
    assert rows[0]["period"] == "1999-12"
    detail = """<h2 class="title">金融情況</h2>
    <div class="publish_time"><time>2000-01-25</time></div>
    <section class="cp">金 融 情 況 民國八十八年十二月 貨幣供給額
    十二月日平均貨幣供給額Ｍ1A、Ｍ1B及Ｍ2年增率分別為7.87％、14.08％及7.08％。
    </section>""".encode()
    assert parse_detail(detail, rows[0]) == ({"m1b_yoy_pct": 14.08, "m2_yoy_pct": 7.08}, None)
    with pytest.raises(ValueError, match="periods disagree"):
        parse_detail(detail.replace("八十八年十二月".encode(), "八十八年十一月".encode()), rows[0])


def test_detail_extracts_original_current_period_yoy() -> None:
    listed = {"title": "115年7月金融情況", "published_on": "2026-08-24",
              "release_url": "https://www.cbc.gov.tw/tw/cp-302-123-ABC-1.html"}
    body = """<h2 class="title">115年7月金融情況</h2>
    <div class="publish_time"><time>2026-08-24</time></div>
    <section class="cp"><p>貨幣總計數 本年7月日平均貨幣總計數M1B及M2年增率
    分別下降為7.34%及7.42%，主要係資金淨匯出。</p></section>""".encode()
    assert parse_detail(body, listed) == ({"m1b_yoy_pct": 7.34, "m2_yoy_pct": 7.42}, None)
    with pytest.raises(ValueError, match="date mismatch"):
        parse_detail(body.replace(b"2026-08-24", b"2026-08-25"), listed)


def test_detail_missing_values_stays_raw_only() -> None:
    listed = {"title": "115年7月金融情況", "published_on": "2026-08-24",
              "release_url": "https://www.cbc.gov.tw/tw/cp-302-123-ABC-1.html"}
    body = """<h2 class="title">115年7月金融情況</h2>
    <div class="publish_time"><time>2026-08-24</time></div>
    <section class="cp">數值請見附件。</section>""".encode()
    assert parse_detail(body, listed) == ({}, "headline_yoy_not_found")


def test_cumulative_average_is_not_misread_as_current_month() -> None:
    listed = {"title": "115年7月金融情況", "published_on": "2026-08-24",
              "release_url": "https://www.cbc.gov.tw/tw/cp-302-123-ABC-1.html"}
    body = """<h2 class="title">115年7月金融情況</h2>
    <div class="publish_time"><time>2026-08-24</time></div>
    <section class="cp"><p>貨幣總計數 M1B及M2月增率分別為0.1%及0.2%。
    累計本年M1B及M2平均年增率分別為3.45%及4.50%。</p></section>""".encode()
    assert parse_detail(body, listed) == ({}, "headline_yoy_not_found")


def test_old_three_aggregate_sentence_does_not_shift_m1a_into_m1b() -> None:
    listed = {"title": "新聞發布第063號(民國94年3月金融情況)",
              "published_on": "2005-04-25",
              "release_url": "https://www.cbc.gov.tw/tw/cp-302-21671-6AD11-1.html"}
    body = """<h2 class="title">新聞發布第063號(民國94年3月金融情況)</h2>
    <div class="publish_time"><time>2005-04-25</time></div>
    <section class="cp"><p>貨幣總計數 3月日平均貨幣總計數Ｍ1A、Ｍ1B及Ｍ2
    年增率分別為10.36％、8.24％及5.95％。</p></section>""".encode()
    assert parse_detail(body, listed) == ({"m1b_yoy_pct": 8.24, "m2_yoy_pct": 5.95}, None)


@pytest.mark.parametrize(
    ("paragraph", "expected"),
    [
        ("貨幣供給額 二月日平均貨幣供給額Ｍ1A、Ｍ1B及Ｍ2年增率分別為負7.60％、負4.15％及5.87％。",
         {"m1b_yoy_pct": -4.15, "m2_yoy_pct": 5.87}),
        ("貨幣總計數 4月日平均貨幣總計數M1B及M2月增率分別為3.77%及0.83%；"
         "年增率分別為9.50%及6.78%。累計本年平均年增率分別為4.85%及6.60%。",
         {"m1b_yoy_pct": 9.50, "m2_yoy_pct": 6.78}),
        ("貨幣總計數 105年11月日平均貨幣總計數M1B、M2月增率分別為0.38%及0.37%。"
         "M1B年增率上升為6.56%；M2雖受資金變化影響，年增率僅略降至3.96%。",
         {"m1b_yoy_pct": 6.56, "m2_yoy_pct": 3.96}),
        ("貨幣總計數 M1B及M2月增率皆為0.11%；M1B及M2年增率則由上月之"
         "18.57%及9.12%，分別下降為18.23%及8.91%。累計本年平均年增率"
         "分別為18.20%及8.96%。",
         {"m1b_yoy_pct": 18.23, "m2_yoy_pct": 8.91}),
        ("貨幣總計數 M1B及M2月增率分別為-0.21%及0.40%；M1B及M2年增率"
         "分別略升為4.94%及5.11%。累計本年平均年增率分別為3.45%及4.50%。",
         {"m1b_yoy_pct": 4.94, "m2_yoy_pct": 5.11}),
        ("貨幣總計數 M1B及M2月增率分別為0.23%及0.37%；M1B年增率回升至"
         "5.00%，M2年增率則微降至6.04%。累計平均年增率分別為4.67%及5.86%。",
         {"m1b_yoy_pct": 5.00, "m2_yoy_pct": 6.04}),
    ],
)
def test_historical_press_release_formats(paragraph: str, expected: dict[str, float]) -> None:
    listed = {"title": "民國90年2月金融情況", "published_on": "2001-03-26",
              "release_url": "https://www.cbc.gov.tw/tw/cp-302-22137-13F0C-1.html"}
    body = (f'<h2 class="title">{listed["title"]}</h2>'
            f'<div class="publish_time"><time>{listed["published_on"]}</time></div>'
            f'<section class="cp">{paragraph}</section>').encode()
    assert parse_detail(body, listed) == (expected, None)


def test_original_release_enters_first_verified_session_after_posting(tmp_path: Path) -> None:
    pl.DataFrame({"date": [date(2026, 8, 24), date(2026, 8, 25)]}).write_parquet(
        tmp_path / "twse_taiex_ohlc.parquet"
    )
    pl.DataFrame({
        "period": ["2026-07", "2026-07"],
        "published_on": ["2026-08-24", "2026-08-24"],
        "release_url": ["https://www.cbc.gov.tw/tw/cp-302-123-ABC-1.html"] * 2,
        "metric": ["m1b_yoy_pct", "m2_yoy_pct"],
        "value_pct": [7.34, 7.42],
        "value_evidence": ["original_press_release_text"] * 2,
        "html_sha256": ["a" * 64] * 2,
    }).write_parquet(tmp_path / "cbc_money_release_vintages.parquet")
    result = _build_cbc_monthly_macro_features(tmp_path, market_symbol="__MARKET__")
    result = result.filter(pl.col("twpub_cbc_m1b_yoy").is_not_null()).sort("date")
    assert result.get_column("date").to_list() == [date(2026, 8, 25)]
    assert result.get_column("twpub_cbc_m1b_yoy").to_list() == pytest.approx([0.0734])
    assert result.get_column("twpub_cbc_m2_yoy").to_list() == pytest.approx([0.0742])
    assert result.get_column("twpub_cbc_m1b_log").null_count() == 1
