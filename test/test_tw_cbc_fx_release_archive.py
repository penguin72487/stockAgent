from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
import sys

import pytest

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
