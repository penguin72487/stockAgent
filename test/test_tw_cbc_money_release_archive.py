from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path
import sys

import polars as pl
import pytest

from downloader.download_tw_cbc_money_release_archive import parse_detail, parse_listing
from downloader import download_tw_cbc_money_release_archive as money
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


def _money_listing_html(page: int, *, total_rows: int = 121, total_pages: int = 3,
                        raw_rows: int | None = None, money_rows: bool = True) -> bytes:
    count = min(60, total_rows - (page - 1) * 60) if raw_rows is None else raw_rows
    items = []
    for index in range(count):
        title = f"115年{7 - page}月金融情況" if index == 0 and money_rows else "一般新聞"
        items.append(
            f'<li><time>2026-07-01</time><a href="/tw/cp-302-{page}-{index}-1.html">'
            f'{title}</a></li>'
        )
    return ("".join(items) + f'<div class="total">共{total_rows}筆資料，第{page}/{total_pages}頁</div>'
            + '<select id="PageSize"><option value="60" selected>60</option></select>').encode()


@pytest.fixture
def money_listing_source(monkeypatch):
    bodies = {page: _money_listing_html(page) for page in (1, 2, 3)}
    requests = []

    def fetch(url, _limiter):
        requests.append(url)
        for page in (1, 2, 3):
            if url == money.LIST_URL.format(page=page):
                return bodies[page]
            if url == money.BASE + f"/tw/cp-302-{page}-0-1.html":
                return (
                    f'<h2 class="title">115年{7 - page}月金融情況</h2>'
                    '<div class="publish_time"><time>2026-07-01</time></div>'
                    '<section class="cp">貨幣總計數 M1B及M2年增率分別為1.00%及2.00%。</section>'
                ).encode()
        raise AssertionError(f"unexpected fixture request: {url}")

    monkeypatch.setattr(money, "_fetch", fetch)
    return bodies, requests


def test_full_listing_proves_raw_rows_not_filtered_money_count(tmp_path, money_listing_source):
    bodies, _requests = money_listing_source
    bodies[2] = _money_listing_html(2, money_rows=False)
    result = money.collect(tmp_path, workers=1, refresh_recent=0)
    assert result["complete"] is True
    assert result["registered_releases"] == result["saved_releases"] == 2
    assert [receipt["raw_rows"] for receipt in result["listing_receipts"]] == [60, 60, 1]
    assert [receipt["release_rows"] for receipt in result["listing_receipts"]] == [1, 0, 1]
    assert all(receipt["page_size"] == 60 and receipt["total_rows"] == 121
               and receipt["total_pages"] == 3 for receipt in result["listing_receipts"])


@pytest.mark.parametrize("damage", ["repeated_first_page", "zero_page", "zero_total", "excessive_pages"])
def test_first_acquisition_cannot_publish_incomplete_listing(
    tmp_path, monkeypatch, money_listing_source, damage,
):
    bodies, requests = money_listing_source
    if damage == "repeated_first_page":
        bodies[2] = bodies[3] = bodies[1]
    elif damage == "zero_page":
        bodies[1] = bodies[1].replace("第1/3頁".encode(), "第0/3頁".encode())
    elif damage == "zero_total":
        bodies[1] = bodies[1].replace("共121筆資料".encode(), "共0筆資料".encode())
    else:
        bodies[1] = _money_listing_html(1, total_rows=60001, total_pages=1001)
    monkeypatch.setattr(sys, "argv", [
        "money", "--output-dir", str(tmp_path), "--workers", "1", "--refresh-recent", "0",
    ])
    with pytest.raises(ValueError):
        money.main()
    assert all("/lp-302-" in url for url in requests)
    assert not (tmp_path / f"{money.OUTPUT_NAME}.parquet").exists()
    failed = json.loads((tmp_path / "state" / f"{money.OUTPUT_NAME}.json").read_bytes())
    assert failed["status"] == "degraded" and failed["complete"] is False
    assert "resume_checkpoint" not in failed


@pytest.mark.parametrize("damage", [
    "first_wrong_page", "second_wrong_page", "duplicate_page", "short_first",
    "short_second", "short_last", "extra_last", "changed_total_rows", "missing_total",
    "missing_page_size", "wrong_page_size", "contradictory_page_count", "upstream_error",
])
def test_invalid_listing_preserves_archive_and_checkpoint_before_details(
    tmp_path, monkeypatch, money_listing_source, damage,
):
    bodies, requests = money_listing_source
    good = money.collect(tmp_path, workers=1, refresh_recent=0)
    parquet = tmp_path / f"{money.OUTPUT_NAME}.parquet"
    original = parquet.read_bytes()
    if damage == "first_wrong_page":
        bodies[1] = bodies[1].replace("第1/3頁".encode(), "第2/3頁".encode())
    elif damage == "second_wrong_page":
        bodies[2] = bodies[2].replace("第2/3頁".encode(), "第3/3頁".encode())
    elif damage == "duplicate_page":
        bodies[2] = bodies[1]
    elif damage in {"short_first", "short_second", "short_last", "extra_last"}:
        page = 1 if damage == "short_first" else 2 if damage == "short_second" else 3
        count = 59 if page < 3 else 0 if damage == "short_last" else 2
        bodies[page] = _money_listing_html(page, raw_rows=count)
    elif damage == "changed_total_rows":
        bodies[2] = _money_listing_html(2, total_rows=122)
    elif damage == "missing_total":
        bodies[1] = bodies[1].replace("共121筆資料，".encode(), b"")
    elif damage == "missing_page_size":
        bodies[1] = bodies[1].replace(b" selected", b"")
    elif damage == "wrong_page_size":
        bodies[1] = bodies[1].replace(b'value="60"', b'value="20"')
    elif damage == "contradictory_page_count":
        bodies[1] = _money_listing_html(1, total_pages=4)
    else:
        bodies[2] = b"<html>This web server can't be reached</html>"
    requests.clear()
    monkeypatch.setattr(sys, "argv", [
        "money", "--output-dir", str(tmp_path), "--workers", "1", "--refresh-recent", "0",
    ])
    expected = money.SourceAccessBlocked if damage == "upstream_error" else ValueError
    with pytest.raises(expected):
        money.main()
    assert requests and all("/lp-302-" in url for url in requests)
    assert parquet.read_bytes() == original
    failed = json.loads((tmp_path / "state" / f"{money.OUTPUT_NAME}.json").read_bytes())
    assert failed["status"] == "degraded" and failed["complete"] is False
    assert failed["resume_checkpoint"]["completed_state"] == good


@pytest.mark.parametrize("damage", [None, "wrong_page", "short_prefix", "changed_total", "upstream_error"])
def test_recent_prefix_is_validated_without_claiming_new_full_scan(
    tmp_path, monkeypatch, money_listing_source, damage,
):
    bodies, requests = money_listing_source
    good = money.collect(tmp_path, workers=1, refresh_recent=0)
    parquet = tmp_path / f"{money.OUTPUT_NAME}.parquet"
    original = parquet.read_bytes()
    if damage == "wrong_page":
        bodies[2] = bodies[1]
    elif damage == "short_prefix":
        bodies[2] = _money_listing_html(2, raw_rows=1)
    elif damage == "changed_total":
        bodies[2] = _money_listing_html(2, total_rows=122)
    elif damage == "upstream_error":
        bodies[1] = b"<html>This web server can't be reached</html>"
    requests.clear()
    monkeypatch.setattr(sys, "argv", [
        "money", "--output-dir", str(tmp_path), "--workers", "1", "--refresh-recent", "0",
        "--recent-pages", "2",
    ])
    if damage is None:
        money.main()
    else:
        expected = money.SourceAccessBlocked if damage == "upstream_error" else ValueError
        with pytest.raises(expected):
            money.main()
    assert money.LIST_URL.format(page=3) not in requests
    assert all("/lp-302-" in url for url in requests)
    assert parquet.read_bytes() == original
    state = json.loads((tmp_path / "state" / f"{money.OUTPUT_NAME}.json").read_bytes())
    if damage is None:
        assert state["complete"] is True and state["scan_scope"] == "recent_pages"
        assert state["scanned_pages"] == 2 and state["index_total_pages"] == 3
        assert state["last_full_index_scan_at_utc"] == good["last_full_index_scan_at_utc"]
        assert state["listing_receipts"][2] == good["listing_receipts"][2]
    else:
        assert state["status"] == "degraded" and state["complete"] is False
        assert state["resume_checkpoint"]["completed_state"] == good


def test_legacy_twenty_row_cache_is_rejected_without_online_fallback(
    tmp_path, monkeypatch, money_listing_source,
):
    _bodies, requests = money_listing_source
    good = money.collect(tmp_path, workers=1, refresh_recent=0)
    parquet = tmp_path / f"{money.OUTPUT_NAME}.parquet"
    original = parquet.read_bytes()
    legacy = _money_listing_html(1, total_rows=41, raw_rows=20).replace(
        b'value="60"', b'value="20"',
    )
    monkeypatch.setattr(money, "_cached", lambda *_args, **_kwargs: legacy)
    monkeypatch.setattr(sys, "argv", [
        "money", "--output-dir", str(tmp_path), "--workers", "1", "--cached-list-pages",
    ])
    requests.clear()
    with pytest.raises(ValueError, match="page size"):
        money.main()
    assert requests == []
    assert parquet.read_bytes() == original
    failed = json.loads((tmp_path / "state" / f"{money.OUTPUT_NAME}.json").read_bytes())
    assert failed["status"] == "degraded" and failed["complete"] is False
    assert failed["resume_checkpoint"]["completed_state"] == good


@pytest.mark.parametrize("damage", [None, "wrong_tail_page", "missing_tail_rows"])
def test_legacy_resume_listing_bytes_need_completeness_beyond_matching_sha(
    tmp_path, monkeypatch, money_listing_source, damage,
):
    _bodies, requests = money_listing_source
    good = money.collect(tmp_path, workers=1, refresh_recent=0)
    parquet = tmp_path / f"{money.OUTPUT_NAME}.parquet"
    original = parquet.read_bytes()
    # Model a pre-fix successful receipt: its exact hash is valid, but it has
    # no new metadata fields and can pin an intrinsically wrong historical page.
    for receipt in good["listing_receipts"]:
        for key in ("raw_rows", "total_rows", "total_pages", "page_size"):
            receipt.pop(key)
    tail = good["listing_receipts"][2]
    path = Path(tail["path"])
    if damage == "wrong_tail_page":
        path.write_bytes(path.read_bytes().replace("第3/3頁".encode(), "第2/3頁".encode()))
    elif damage == "missing_tail_rows":
        path.write_bytes(_money_listing_html(3, raw_rows=0))
    tail["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    state_path = tmp_path / "state" / f"{money.OUTPUT_NAME}.json"
    state_path.write_text(json.dumps(good))
    monkeypatch.setattr(sys, "argv", [
        "money", "--output-dir", str(tmp_path), "--workers", "1", "--refresh-recent", "0",
        "--recent-pages", "2",
    ])
    requests.clear()
    if damage is None:
        money.main()
        assert requests == [money.LIST_URL.format(page=page) for page in (1, 2)]
        assert json.loads(state_path.read_bytes())["complete"] is True
    else:
        with pytest.raises(ValueError):
            money.main()
        assert requests == []
        failed = json.loads(state_path.read_bytes())
        assert failed["status"] == "degraded" and failed["complete"] is False
        assert failed["resume_checkpoint"]["completed_state"] == good
    assert parquet.read_bytes() == original


def test_resume_listing_pages_allow_valid_mixed_generations(tmp_path, money_listing_source):
    bodies, requests = money_listing_source
    good = money.collect(tmp_path, workers=1, refresh_recent=0)
    original = (tmp_path / f"{money.OUTPUT_NAME}.parquet").read_bytes()
    # The two newly observed pages have a larger total; the pinned older tail
    # still proves its own last-page count from the earlier full-index scan.
    for page in (1, 2):
        bodies[page] = _money_listing_html(page, total_rows=122)
    first = money.collect(tmp_path, workers=1, recent_pages=2, refresh_recent=0)
    assert [receipt["total_rows"] for receipt in first["listing_receipts"]] == [122, 122, 121]
    requests.clear()
    resumed = money.collect(tmp_path, workers=1, recent_pages=2, refresh_recent=0)
    assert requests == [money.LIST_URL.format(page=page) for page in (1, 2)]
    assert resumed["complete"] is True and resumed["scan_scope"] == "recent_pages"
    assert resumed["last_full_index_scan_at_utc"] == good["last_full_index_scan_at_utc"]
    assert resumed["listing_receipts"][2] == good["listing_receipts"][2]
    assert (tmp_path / f"{money.OUTPUT_NAME}.parquet").read_bytes() == original
