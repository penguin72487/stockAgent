from datetime import date
import io
import json
import zipfile

import pytest

from downloader.artifact_io import sha256_file
from scripts import download_taifex_recent_index_derivatives_ticks as recent


DAY = date(2026, 10, 27)
MEMBER = "Flx_Daily_2026_10_27.csv"
HEADER = "成交日期,商品代號,到期日,成交時間,成交價格,成交數量(B+S),近月價格,遠月價格\r\n"
ROW = "20261027,MXFFX,20261028,084501,25000,2,-,-\r\n"
URL = ("https://www.taifex.com.tw/file/taifex/Dailydownload/"
       "DailydownloadCSVFlx/Daily_2026_10_27.zip")


def zipped(text, member=MEMBER):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member, text.encode("cp950"))
    return output.getvalue()


def test_header_only_customized_zip_is_valid_source_not_fabricated_trade():
    payload = zipped(HEADER)
    with pytest.raises(ValueError, match="implausibly small"):
        recent._validate_zip_payload(payload, expected_member=MEMBER)
    assert recent._customized_tick_source_readback(payload, day=DAY, member=MEMBER) == {
        "rows": 0, "products": [], "status": "header_only_observed"}


def test_customized_tick_keeps_eight_digit_expiry_and_date_identity():
    assert recent._customized_tick_source_readback(zipped(HEADER + ROW), day=DAY, member=MEMBER) == {
        "rows": 1, "products": ["MXFFX"], "status": "rows_observed"}
    with pytest.raises(ValueError, match="identity"):
        recent._customized_tick_source_readback(zipped(HEADER + ROW.replace("20261027", "20261026")),
                                                day=DAY, member=MEMBER)
    with pytest.raises(ValueError, match="header"):
        recent._customized_tick_source_readback(zipped(HEADER.replace("到期日", "月份") + ROW),
                                                day=DAY, member=MEMBER)


def test_recent_source_receipt_reuse_and_tamper_guard(tmp_path, monkeypatch):
    payload = zipped(HEADER)
    monkeypatch.setattr(recent, "_fetch_bytes", lambda *a, **k: (URL.encode(), {}))
    monkeypatch.setattr(recent, "_latest_completed_listing_date", lambda observed: DAY)
    calls = []

    def fake_download(url, target, **kwargs):
        calls.append(url)
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            target.write_bytes(payload)
        return {"path": str(target), "url": url, "bytes": target.stat().st_size,
                "sha256": sha256_file(target), "last_modified": None, "reused": len(calls) > 1}

    monkeypatch.setattr(recent, "_download_zip", fake_download)
    kwargs = dict(days=1, attempts=1, timeout=1, transport=None)
    manifest = recent._collect_customized_recent(tmp_path, **kwargs)
    assert json.loads(manifest.read_text())["total_rows"] == 0
    recent._collect_customized_recent(tmp_path, **kwargs)
    target = tmp_path / "raw/customized_futures/Daily_2026_10_27.zip"
    target.write_bytes(zipped(HEADER + ROW))
    with pytest.raises(ValueError, match="receipt mismatch"):
        recent._collect_customized_recent(tmp_path, **kwargs)
