from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from downloader.download_tw_mof_release_archive import (
    _official_url,
    _period_gaps,
    _verified_previous,
    select_body_pdf_url,
    select_original_pdf_url,
)


def test_original_pdf_selection_prefers_full_attachment_and_upgrades_official_http() -> None:
    detail = b'''<a title="\xe6\x96\xb0\xe8\x81\x9e\xe7\xa8\xbf\xe6\x9c\xac\xe6\x96\x87 PDF\xe6\xaa\x94" href="https://service.mof.gov.tw/news.pdf">body</a>
<a title="\xe6\x9c\xac\xe6\x96\x87\xe5\x8f\x8a\xe9\x99\x84\xe8\xa1\xa8 PDF\xe6\xaa\x94" href="http://service.mof.gov.tw/full.pdf">full</a>'''
    assert select_original_pdf_url(detail) == "https://service.mof.gov.tw/full.pdf"


def test_original_pdf_selection_rejects_ambiguous_and_foreign_links() -> None:
    with pytest.raises(ValueError, match="ambiguous"):
        select_original_pdf_url(
            '<a title="本文及附表" href="https://service.mof.gov.tw/a.pdf"></a>'
            '<a title="本文及附表" href="https://service.mof.gov.tw/b.pdf"></a>'.encode()
        )
    with pytest.raises(ValueError, match="out-of-scope"):
        _official_url("https://example.org/fake.pdf", pdf=True)


def test_legacy_mof_download_link_is_kept_on_official_host() -> None:
    detail = '<a title="本文及附表 PDF檔" href="/download/pub60824">PDF</a>'.encode()
    assert select_original_pdf_url(detail) == "https://www.mof.gov.tw/download/pub60824"


def test_separate_body_pdf_can_verify_a_tables_first_release() -> None:
    detail = ('<a title="新聞稿本文 PDF檔" href="/download/pub61287"></a>'
              '<a title="新聞稿本文及附表 PDF檔" href="/download/pub61288"></a>').encode()
    assert select_body_pdf_url(detail) == "https://www.mof.gov.tw/download/pub61287"


def test_month_gap_is_measured_without_inventing_releases() -> None:
    assert _period_gaps(["2018-11", "2019-01"]) == ["2018-12"]


def test_receipt_reuse_requires_exact_original_bytes(tmp_path: Path) -> None:
    pdf = tmp_path / "original.pdf"
    pdf.write_bytes(b"bytes")
    row = {
        "pdf_raw_path": str(pdf), "pdf_bytes": 5,
        "pdf_sha256": hashlib.sha256(b"bytes").hexdigest(),
        "detail_raw_path": None,
    }
    assert _verified_previous(row)
    pdf.write_bytes(b"other")
    assert not _verified_previous(row)
