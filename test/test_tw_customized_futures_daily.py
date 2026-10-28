from datetime import date
import json

import pytest

from downloader.artifact_io import sha256_file
from scripts import download_tw_index_futures_day_session as downloader
from stockagent.data.tw_customized_futures_daily import iter_customized_daily_rows


HEADER = "交易日期,契約,到期日,開盤價,最高價,最低價,收盤價,成交量,結算價\r\n"
ROW = "2024/01/23,MXFFX,20240125,17825,17850,17825,17850,6,17864,\r\n"
START, END = date(2024, 1, 22), date(2024, 1, 31)


def write_source(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content.encode("cp950"))


def read(path, **kwargs):
    return list(iter_customized_daily_rows(path, expected_sha256=sha256_file(path),
                                          start=START, end=END, **kwargs))


def test_retains_raw_row_and_distinguishes_expiry_from_month(tmp_path):
    path = tmp_path / "source.csv"
    write_source(path, HEADER + ROW)
    row, = read(path)
    assert row["product"] == "MXFFX"
    assert row["trading_date"] == date(2024, 1, 23)
    assert row["expiry_date"] == date(2024, 1, 25)
    assert row["raw"]["結算價"] == "17864"
    assert row["raw"]["成交量"] == "6"
    assert row["source_line"] == 2


def test_header_only_is_an_empty_response_not_missing_or_synthetic(tmp_path):
    path = tmp_path / "empty.csv"
    write_source(path, HEADER)
    assert read(path) == []
    summary = downloader._customized_source_readback(path, start=START, end=END)
    assert summary["status"] == "header_only_observed"
    assert summary["rows"] == summary["positive_volume_rows"] == 0


@pytest.mark.parametrize("row", [
    ROW.replace("20240125", "20241325"),
    ROW.replace("2024/01/23", "2024/02/01"),
    ROW.replace("MXFFX", ""),
    ROW.rstrip("\r\n") + "unexpected\r\n",
])
def test_malformed_source_identity_or_extra_values_fail_closed(tmp_path, row):
    path = tmp_path / "bad.csv"
    write_source(path, HEADER + row)
    with pytest.raises(ValueError):
        read(path)


def test_digest_is_required_and_rechecked_at_exhaustion(tmp_path):
    path = tmp_path / "source.csv"
    write_source(path, HEADER + ROW)
    iterator = iter_customized_daily_rows(path, expected_sha256=sha256_file(path), start=START, end=END)
    next(iterator)
    write_source(path, HEADER + ROW + ROW)
    with pytest.raises(ValueError, match="changed while reading"):
        list(iterator)
    with pytest.raises(ValueError, match="digest mismatch"):
        list(iter_customized_daily_rows(path, expected_sha256="a" * 64, start=START, end=END))


def test_download_receipt_resume_and_tamper_guard(tmp_path, monkeypatch):
    calls = []

    def fake_download(url, payload, target, **kwargs):
        calls.append((url, payload))
        write_source(target, HEADER + ROW)
        return target

    monkeypatch.setattr(downloader, "download_taifex_attachment", fake_download)
    root = tmp_path / "customized"
    kwargs = dict(start=START, end=END, attempts=1, request_interval=0)
    manifest_path = downloader._download_customized_history(root, **kwargs)
    manifest = json.loads(manifest_path.read_text())
    assert manifest["rows"] == manifest["positive_volume_rows"] == 1
    assert calls[0][1]["commodity_id"] == "all"
    downloader._download_customized_history(root, **kwargs)
    assert len(calls) == 1
    target = root / "raw" / "2024-01-22_2024-01-31_all.csv"
    write_source(target, HEADER)
    with pytest.raises(ValueError, match="receipt mismatch"):
        downloader._download_customized_history(root, **kwargs)


def test_unreceipted_interrupted_response_is_preserved_and_refetched(tmp_path, monkeypatch):
    root = tmp_path / "customized"
    target = root / "raw" / "2024-01-22_2024-01-31_all.csv"
    write_source(target, HEADER)
    monkeypatch.setattr(downloader, "download_taifex_attachment", lambda u, p, t, **k: write_source(t, HEADER + ROW))
    manifest_path = downloader._download_customized_history(root, start=START, end=END, attempts=1, request_interval=0)
    assert json.loads(manifest_path.read_text())["rows"] == 1
    assert len(list((root / "quarantined").glob("*.raw"))) == 1
