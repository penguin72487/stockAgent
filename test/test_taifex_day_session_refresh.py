from datetime import date, datetime
import hashlib
import os
from pathlib import Path

import pytest

from scripts import download_tw_index_futures_day_session as daily


def test_preclose_raw_range_must_be_refetched_after_all_futures_close(tmp_path: Path) -> None:
    target = tmp_path / "2026-09-01_2026-09-17_all.csv"
    target.write_bytes(b"early source")
    early = datetime(2026, 9, 17, 14, 45, tzinfo=daily.TAIPEI)
    os.utime(target, (early.timestamp(), early.timestamp()))
    assert not daily._needs_postclose_refresh(
        target, date(2026, 9, 17),
        now=datetime(2026, 9, 17, 15, 0, tzinfo=daily.TAIPEI),
    )
    assert daily._needs_postclose_refresh(
        target, date(2026, 9, 17),
        now=datetime(2026, 9, 17, 16, 30, tzinfo=daily.TAIPEI),
    )


def test_postclose_refresh_preserves_changed_early_raw_bytes(
    tmp_path: Path, monkeypatch,
) -> None:
    target = tmp_path / "raw" / "ranges" / "2026-09-01_2026-09-17_all.csv"
    target.parent.mkdir(parents=True)
    early = "交易日期,契約,到期月份(週別),交易時段,成交量,收盤價,結算價\n2026/09/17,TX,202609,一般,1,100,100\n".encode("cp950")
    final = "交易日期,契約,到期月份(週別),交易時段,成交量,收盤價,結算價\n2026/09/17,TX,202609,一般,2,101,101\n".encode("cp950")
    target.write_bytes(early)

    def fake_download(_payload, destination, **_kwargs):
        destination.write_bytes(final)
        return destination

    monkeypatch.setattr(daily, "_download", fake_download)
    daily._refresh_postclose_range({}, target, attempts=1, request_interval=0.0)

    archive = (
        tmp_path / "raw" / "superseded"
        / f"{target.stem}.{hashlib.sha256(early).hexdigest()}.raw"
    )
    assert target.read_bytes() == final
    assert archive.read_bytes() == early


def test_postclose_refresh_rejects_date_regression_without_replacing_source(
    tmp_path: Path, monkeypatch,
) -> None:
    target = tmp_path / "raw" / "ranges" / "2026-09-01_2026-09-17_all.csv"
    target.parent.mkdir(parents=True)
    header = "交易日期,契約,到期月份(週別),交易時段,成交量,收盤價,結算價\n"
    early = (header + "2026/09/16,TX,202609,一般,1,100,100\n"
             + "2026/09/17,TX,202609,一般,1,101,101\n").encode("cp950")
    regressed = (header + "2026/09/16,TX,202609,一般,1,100,100\n").encode("cp950")
    target.write_bytes(early)

    def fake_download(_payload, destination, **_kwargs):
        destination.write_bytes(regressed)
        return destination

    monkeypatch.setattr(daily, "_download", fake_download)
    with pytest.raises(RuntimeError, match="loses previously observed"):
        daily._refresh_postclose_range({}, target, attempts=1, request_interval=0.0)
    assert target.read_bytes() == early


def test_postclose_refresh_rejects_lost_index_session_on_retained_date(
    tmp_path: Path, monkeypatch,
) -> None:
    target = tmp_path / "raw" / "ranges" / "2026-09-01_2026-09-17_all.csv"
    target.parent.mkdir(parents=True)
    header = "交易日期,契約,到期月份(週別),交易時段,成交量,收盤價,結算價\n"
    early = (header
             + "2026/09/17,TX,202609,一般,1,100,100\n"
             + "2026/09/17,GDF,202609,一般,1,200,200\n").encode("cp950")
    regressed = (header
                 + "2026/09/17,GDF,202609,一般,1,200,200\n").encode("cp950")
    target.write_bytes(early)

    def fake_download(_payload, destination, **_kwargs):
        destination.write_bytes(regressed)
        return destination

    monkeypatch.setattr(daily, "_download", fake_download)
    with pytest.raises(RuntimeError, match="loses previously observed index day sessions"):
        daily._refresh_postclose_range({}, target, attempts=1, request_interval=0.0)
    assert target.read_bytes() == early
