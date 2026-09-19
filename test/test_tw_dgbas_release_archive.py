from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
import sys

import pytest

from downloader import download_tw_dgbas_release_archive as archive


def test_cli_resolves_relative_output_root_before_writing_receipts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["dgbas", "--output-dir", "dataset"])
    seen: list[Path] = []
    monkeypatch.setattr(archive, "_writer_lock", lambda root: nullcontext())
    def fake_collect(root: Path, **_kwargs: object) -> dict[str, object]:
        seen.append(root)
        return {"failures": [], "saved_releases": 1}
    monkeypatch.setattr(archive, "collect", fake_collect)
    archive.main()
    assert seen == [(tmp_path / "dataset").resolve()]
    assert "saved_releases" in capsys.readouterr().out


def _listing(source: archive.Source, title: str, date: str = "115-09-08") -> bytes:
    return (
        '<table><tr><td data-title="標題">'
        f'<a href="News_Content.aspx?n=2668&s=236675">{title}</a></td>'
        f'<td data-title="發布日期">{date}</td></tr></table>'
    ).encode()


def _detail(title: str, posted: str = "115-09-08") -> bytes:
    return (
        f'<div id="CCMS_Content"><h3>{title}</h3>'
        '<a href="https://ws.dgbas.gov.tw/public/report.pdf">PDF</a>'
        '<a href="https://example.test/unsafe.pdf">unsafe</a></div>'
        f'<div><span>張貼日期：{posted}</span></div>'
    ).encode()


def test_cpi_release_listing_and_exact_headline_value() -> None:
    source = archive.SOURCE_BY_NAME["cpi"]
    title = "115年8月消費者物價指數(CPI)年增率跌1.25％"
    rows = archive.parse_listing(source, _listing(source, title))
    assert len(rows) == 1
    assert rows[0]["period"] == "2026-08"
    assert rows[0]["published_on"] == "2026-09-08"
    assert archive._headline_value(source, title) == ("cpi_yoy_pct", -1.25)
    assert archive.parse_detail(_detail(title), rows[0]) == [
        "https://ws.dgbas.gov.tw/public/report.pdf"
    ]


def test_future_schedule_is_not_a_historical_release() -> None:
    source = archive.SOURCE_BY_NAME["cpi"]
    title = "預訂於100年2月發布所得層級別消費者物價指數"
    assert archive.parse_listing(source, _listing(source, title)) == []
    unemployment = archive.SOURCE_BY_NAME["unemployment"]
    rescheduled = "99年1月人力資源調查與98年12月薪資統計結果提前於99年2月22日下午4時發布"
    assert archive.parse_listing(unemployment, _listing(unemployment, rescheduled)) == []


def test_legacy_direct_document_is_kept_as_release_evidence() -> None:
    source = archive.SOURCE_BY_NAME["cpi"]
    title = "93年1月台灣地區物價變動概況"
    body = (
        '<tr><td data-title="標題"><a href="https://ws.dgbas.gov.tw/Download.ashx?icon=.doc">'
        f'{title}</a></td><td data-title="發布日期">93-02-05</td></tr>'
    ).encode()
    row = archive.parse_listing(source, body)[0]
    assert row["release_kind"] == "direct_attachment"
    assert row["period"] == "2004-01"
    assert archive._attachment_suffix(row["release_url"]) == ".doc"


def test_article_date_must_match_official_index() -> None:
    source = archive.SOURCE_BY_NAME["cpi"]
    title = "115年8月消費者物價指數(CPI)年增率漲2.04％"
    row = archive.parse_listing(source, _listing(source, title))[0]
    with pytest.raises(ValueError, match="date mismatch"):
        archive.parse_detail(_detail(title, "115-09-09"), row)


def test_archived_page_checksum_is_verified(tmp_path: Path) -> None:
    directory = tmp_path / "release"
    body = _detail("115年8月消費者物價指數(CPI)年增率漲2.04％")
    digest, path = archive._save_raw(directory, "detail", ".html", body)
    assert len(digest) == 64
    assert archive._cached_detail(directory) == body
    Path(path).write_bytes(b"corrupt")
    with pytest.raises(RuntimeError, match="checksum"):
        archive._cached_detail(directory)


def test_other_headline_metrics_do_not_use_forecasts() -> None:
    unemployment = archive.SOURCE_BY_NAME["unemployment"]
    gdp = archive.SOURCE_BY_NAME["gdp"]
    assert archive._headline_value(
        unemployment, "115年7月就業人數為1,165.1萬人，失業率為3.39%，季調失業率為3.33%"
    ) == ("unemployment_rate_pct", 3.39)
    assert archive._headline_value(
        gdp, "115年第2季經濟成長率saar為5.70％，yoy為12.93％；預測全年11.05％"
    ) == ("gdp_yoy_pct", 12.93)
    assert archive._headline_value(
        gdp, "101年第1季預測經濟成長率為3.38％"
    ) == (None, None)


@pytest.mark.parametrize(
    ("title", "expected"),
    [
        ("100年第4季概估統計經濟成長1.90％，全年成長4.03％；101年預測成長3.91％", 1.90),
        ("100年第4季經濟成長率初步統計為1.89％，全年成長4.04％", 1.89),
        ("101年第1季經濟成長率概估統計為0.36％，預測全年成長3.38％", 0.36),
        ("101年第2季經濟成長率為-0.18％，預測101及102年成長1.66％", -0.18),
    ],
)
def test_early_gdp_original_headline_growth_is_yoy(title: str, expected: float) -> None:
    assert archive._headline_value(archive.SOURCE_BY_NAME["gdp"], title) == (
        "gdp_yoy_pct", expected
    )


def test_cpi_headline_without_direction_is_still_an_original_value() -> None:
    source = archive.SOURCE_BY_NAME["cpi"]
    assert archive._headline_value(
        source, "101年2月消費者物價指數(CPI)年增率0.25％，躉售物價指數年增率1.92％"
    ) == ("cpi_yoy_pct", 0.25)
    assert archive._headline_value(
        source, "101年2月消費者物價指數(CPI)年增率-0.25％"
    ) == ("cpi_yoy_pct", -0.25)


def test_original_cpi_pdf_text_gives_value_and_exact_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    text = (
        "中華民國93 年2 月5 日下午4 時發布，並透過網際網路同步發送\n"
        "一、1 月份消費者物價總指數(CPI)為100.50，較上月漲0.79％，"
        "與上年同月比較，微漲0.01％。\n二、躉售物價總指數(WPI)較上年同月漲2.39％"
    )
    class Page:
        def extract_text(self) -> str:
            return text
    class Reader:
        def __init__(self, path: Path) -> None:
            self.pages = [Page()]
    monkeypatch.setattr(archive, "PdfReader", Reader)
    assert archive._pdf_cpi_evidence(Path("original.pdf"), "2004-02-05") == (
        0.01, "16:00:00", None
    )
    value, clock, warning = archive._pdf_cpi_evidence(Path("original.pdf"), "2004-02-06")
    assert value is None and clock is None and "differs" in str(warning)


def test_original_gdp_pdf_clock_overrides_schedule(monkeypatch: pytest.MonkeyPatch) -> None:
    class Page:
        def extract_text(self) -> str:
            return "中華民國101年5月25日16時30分發布，並透過網際網路同步發送"
    class Reader:
        def __init__(self, path: Path) -> None:
            self.pages = [Page()]
    monkeypatch.setattr(archive, "PdfReader", Reader)
    text, clock, warning = archive._pdf_first_page_clock(
        Path("original.pdf"), "2012-05-25"
    )
    assert text and clock == "16:30:00" and warning is None


def test_unemployment_original_pdf_requires_subject_month_and_unadjusted_rate() -> None:
    text = (
        "中華民國94年2月25日發布。94年2月人力資源調查統計結果訂於3月22日發布。"
        "94年1月臺灣地區人力資源調查統計結果：季調失業率為4.19%。"
        "1月失業人數41萬9千人，失業率為4.06%，經調整季節變動因素後之失業率為4.19%。"
    )
    assert archive._pdf_unemployment_value(text, "2005-01") == pytest.approx(4.06)
    assert archive._pdf_unemployment_value(text, "2005-02") is None
    half_year = "94年6月暨上半年臺灣地區人力資源調查統計結果；失業率為4.22%。"
    assert archive._pdf_unemployment_value(half_year, "2005-06") == pytest.approx(4.22)
    unusual = "94年11月人力資源調查統計結果；失業率下降至4%以下，為3.94%。"
    assert archive._pdf_unemployment_value(unusual, "2005-11") == pytest.approx(3.94)
