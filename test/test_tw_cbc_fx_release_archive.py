from __future__ import annotations

from contextlib import nullcontext
import hashlib
from pathlib import Path
import sys

import pytest
import requests

from downloader import download_tw_cbc_fx_release_archive as archive
from downloader.download_tw_cbc_fx_release_archive import _chinese_int, _period, parse_detail, parse_listing


def test_cli_resolves_relative_output_root_before_writing_receipts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["cbc", "--output-dir", "dataset"])
    seen: list[Path] = []
    monkeypatch.setattr(archive, "_writer_lock", lambda root: nullcontext())
    def fake_collect(root: Path, **_kwargs: object) -> dict[str, object]:
        seen.append(root)
        return {"failures": [], "saved_releases": 1}
    monkeypatch.setattr(archive, "collect", fake_collect)
    archive.main()
    assert seen == [(tmp_path / "dataset").resolve()]
    assert "saved_releases" in capsys.readouterr().out


def test_period_accepts_roc_arabic_and_chinese_titles() -> None:
    assert _period("115年8月底外匯存底") == "2026-08"
    assert _period("新聞發布第112號(九十三年六月底外匯存底)") == "2004-06"
    assert _period("新聞發布第21號(九十年一月外匯存底)") == "2001-01"
    assert _period("新聞發布第002號(九十二年底外匯存底)") == "2003-12"
    assert _period("外匯存底政策說明") is None
    assert _chinese_int("一千零八十") == 1080


def test_listing_filters_non_period_press_items() -> None:
    content = """<html><body>
      <li><time>2026-09-04</time><a href="/tw/cp-302-123-ABC-1.html">115年8月底外匯存底</a></li>
      <li><time>2026-09-03</time><a href="/tw/cp-302-124-ABC-1.html">外匯存底政策說明</a></li>
      <p>共7832筆資料，第1/392頁</p>
    </body></html>""".encode()
    rows, pages = parse_listing(content)
    assert pages == 392
    assert len(rows) == 1
    assert rows[0]["period"] == "2026-08"
    assert rows[0]["published_on"] == "2026-09-04"


def test_official_sixty_row_listing_keeps_legacy_cache_separate(tmp_path: Path) -> None:
    directory = tmp_path / "raw" / archive.OUTPUT_NAME / "list"
    directory.mkdir(parents=True)
    legacy = b"legacy twenty-row page"
    old_prefix = "page-0001"
    archive._save_raw(directory, old_prefix, legacy)
    assert archive.LIST_URL.format(page=1).endswith("/lp-302-1-1-60.html")
    assert archive._listing_layout(tmp_path, cached_only=False) == (
        archive.LIST_URL, 60, "page-0060-{page:04d}"
    )
    assert archive._listing_layout(tmp_path, cached_only=True) == (
        archive.LEGACY_LIST_URL, 20, "page-{page:04d}"
    )

    new_prefix = "page-0060-0001"
    archive._save_raw(directory, new_prefix, b"new sixty-row page")
    assert archive._listing_layout(tmp_path, cached_only=True) == (
        archive.LIST_URL, 60, "page-0060-{page:04d}"
    )
    assert archive._cached(directory, old_prefix) == legacy
    assert archive._cached(directory, new_prefix) == b"new sixty-row page"


def _sixty_row_listing_fixture(page: int, *, first_page_rows: int = 60) -> bytes:
    month = 9 - page
    filler = (
        ''.join(f'<li><time>2026-01-01</time><a href="/tw/cp-302-{index}-ABC-1.html">'
                '一般新聞</a></li>' for index in range(2, first_page_rows + 1))
        if page == 1 else ''
    )
    return (
        f'<li><time>2026-09-0{page}</time>'
        f'<a href="/tw/cp-302-{page}-ABC-1.html">115年{month}月底外匯存底</a></li>'
        f'{filler}<div class="total">共61筆資料，第{page}/2頁</div>'
        '<select id="PageSize"><option value="60" selected>60</option></select>'
    ).encode()


def test_collect_records_sixty_row_page_receipts_without_mixing_legacy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:

    requested: list[str] = []

    def fetch(url: str, _limiter: object) -> bytes:
        requested.append(url)
        return _sixty_row_listing_fixture(1 if "-1-60.html" in url else 2)

    monkeypatch.setattr(archive, "_fetch", fetch)
    monkeypatch.setattr(
        archive, "_collect_one",
        lambda row, *_args, **_kwargs: {**row, "metric": "fx_reserves_usd_100m", "value": 1.0},
    )
    monkeypatch.setattr(
        archive, "write_release_rows_if_changed",
        lambda *_args, **_kwargs: ("verified-sha", True),
    )
    summary = archive.collect(tmp_path, workers=1)
    assert summary["complete"] is True
    assert summary["registered_releases"] == 2
    assert summary["listing_page_size"] == 60
    assert summary["listing_pages"] == 2
    assert set(summary["stage_seconds"]) == {
        "listing_discovery", "detail_collection", "parquet_proof_write"
    }
    assert requested == [archive.LIST_URL.format(page=1), archive.LIST_URL.format(page=2)]
    assert [Path(row["path"]).name.startswith("page-0060-")
            for row in summary["listing_receipts"]] == [True, True]
    assert [row["raw_rows"] for row in summary["listing_receipts"]] == [60, 1]


@pytest.mark.parametrize("wrong_page,first_page_rows,error", [
    (True, 60, "page identity"),
    (False, 59, "59 of 60 rows"),
])
def test_listing_page_coverage_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    wrong_page: bool, first_page_rows: int, error: str,
) -> None:
    def fetch(url: str, _limiter: object) -> bytes:
        page = 1 if "-1-60.html" in url or wrong_page else 2
        return _sixty_row_listing_fixture(page, first_page_rows=first_page_rows)

    monkeypatch.setattr(archive, "_fetch", fetch)
    with pytest.raises(ValueError, match=error):
        archive.collect(tmp_path, workers=1)


def test_partial_sixty_row_cache_does_not_fall_back_to_legacy_pages(tmp_path: Path) -> None:
    directory = tmp_path / "raw" / archive.OUTPUT_NAME / "list"
    directory.mkdir(parents=True)
    body = "<p>共61筆資料，第1/2頁</p>".encode()
    archive._save_raw(directory, "page-0060-0001", body)
    archive._save_raw(directory, "page-0001", body)
    archive._save_raw(directory, "page-0002", "<p>共61筆資料，第2/2頁</p>".encode())
    with pytest.raises(FileNotFoundError, match="page 2"):
        archive.collect(tmp_path, workers=1, offline_cache_only=True)


def test_http_200_proxy_error_does_not_poison_cached_listing(tmp_path: Path) -> None:
    good = ('<li><time>2026-09-04</time><a href="/tw/cp-302-123-ABC-1.html">'
            '115年8月底外匯存底</a></li><p>第1/2頁</p>').encode()
    bad = b"<html><title>Error</title>This web server can't be reached</html>"
    directory = tmp_path / "list"
    directory.mkdir()
    good_path = directory / f"page-0001-{hashlib.sha256(good).hexdigest()[:16]}.html"
    bad_path = directory / f"page-0001-{hashlib.sha256(bad).hexdigest()[:16]}.html"
    good_path.write_bytes(good)
    bad_path.write_bytes(bad)
    assert archive._cached(directory, "page-0001", listing=True) == good
    with pytest.raises(archive.SourceAccessBlocked, match="upstream error page"):
        parse_listing(bad)


def test_fetch_retries_transient_redirect_without_following_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    url = archive.LIST_URL.format(page=164)

    def response(status: int, *, body: bytes = b"", location: str | None = None) -> requests.Response:
        result = requests.Response()
        result.status_code = status
        result.url = url
        result._content = body
        result._content_consumed = True
        if location is not None:
            result.headers["Location"] = location
        return result

    class Session:
        def __init__(self) -> None:
            self.responses = [
                response(302, location="https://other.example/blocked"),
                response(200, body=b"<html>official listing</html>"),
            ]
            self.urls: list[str] = []

        def get(self, requested: str, **kwargs: object) -> requests.Response:
            self.urls.append(requested)
            assert kwargs["allow_redirects"] is False
            return self.responses.pop(0)

    class Limiter:
        def __init__(self) -> None:
            self.waits = 0
            self.deferrals = 0

        def wait(self) -> None:
            self.waits += 1

        def defer(self, _seconds: float) -> None:
            self.deferrals += 1

    session = Session()
    limiter = Limiter()
    monkeypatch.setattr(archive, "_session", lambda: session)
    assert archive._fetch(url, limiter) == b"<html>official listing</html>"
    assert session.urls == [url, url]
    assert limiter.waits == 2
    assert limiter.deferrals == 1


def test_fetch_rejects_persistent_redirect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    url = archive.LIST_URL.format(page=164)

    class Session:
        def get(self, _url: str, **_kwargs: object) -> requests.Response:
            result = requests.Response()
            result.status_code = 302
            result.url = url
            result._content_consumed = True
            result.headers["Location"] = "https://other.example/blocked"
            return result

    class Limiter:
        def wait(self) -> None:
            pass

        def defer(self, _seconds: float) -> None:
            pass

    monkeypatch.setattr(archive, "_session", Session)
    with pytest.raises(ValueError, match="unexpected CBC redirect after retries"):
        archive._fetch(url, Limiter())


def test_detail_uses_original_amount_and_checks_release_date() -> None:
    listed = {"title": "115年8月底外匯存底", "period": "2026-08", "published_on": "2026-09-04",
              "release_url": "https://www.cbc.gov.tw/tw/cp-302-123-ABC-1.html"}
    content = """<h2 class="title">115年8月底外匯存底</h2>
    <div class="publish_time"><time>2026-09-04</time></div>
    <section class="cp"><p>115年8月底我國外匯存底金額為6,019.04億美元，
    較上月底增加76.33億美元。</p></section>""".encode()
    assert parse_detail(content, listed) == ("2026-08", 6019.04, None)
    with pytest.raises(ValueError, match="date mismatch"):
        parse_detail(content.replace(b"2026-09-04", b"2026-09-05"), listed)


def test_detail_does_not_invent_value() -> None:
    listed = {"title": "115年8月底外匯存底", "period": "2026-08", "published_on": "2026-09-04",
              "release_url": "https://www.cbc.gov.tw/tw/cp-302-123-ABC-1.html"}
    content = """<h2 class="title">115年8月底外匯存底</h2>
    <div class="publish_time"><time>2026-09-04</time></div>
    <section class="cp">完整數據請見附件</section>""".encode()
    assert parse_detail(content, listed) == ("2026-08", None, "headline_amount_not_found")


def test_detail_fixes_wrong_listing_period_from_original_article() -> None:
    listed = {
        "title": "新聞發布第201號(九十年八月底外匯存底)",
        "period": "2001-08", "published_on": "2001-10-08",
        "release_url": "https://www.cbc.gov.tw/tw/cp-302-22219-7BD21-1.html",
    }
    content = """<h2 class="title">新聞發布第201號(九十年八月底外匯存底)</h2>
    <div class="publish_time"><time>2001-10-08</time></div>
    <section class="cp">90年9月底外匯存底計1,152.00億美元</section>""".encode()
    assert parse_detail(content, listed) == ("2001-09", 1152.0, None)


def test_old_chinese_amount_is_original_not_bulk_revision() -> None:
    listed = {
        "title": "新聞發布第21號(九十年一月外匯存底)",
        "period": "2001-01", "published_on": "2001-02-07",
        "release_url": "https://www.cbc.gov.tw/tw/cp-302-22315-FB06B-1.html",
    }
    content = """<h2 class="title">新聞發布第21號(九十年一月外匯存底)</h2>
    <div class="publish_time"><time>2001-02-07</time></div>
    <section class="cp">九十年一月底外匯存底計一千零八十億五千六百萬美元。</section>""".encode()
    assert parse_detail(content, listed) == ("2001-01", 1080.56, None)
