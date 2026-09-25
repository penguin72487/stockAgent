from __future__ import annotations

from datetime import date, datetime
import importlib.util
import io
import json
from pathlib import Path
from zoneinfo import ZoneInfo
import zipfile

import pytest


SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "download_taifex_recent_index_derivatives_ticks.py"
)
SPEC = importlib.util.spec_from_file_location(
    "download_taifex_recent_index_derivatives_ticks", SCRIPT
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_extracts_latest_common_taifex_dates() -> None:
    futures_page = b"""
    https://www.taifex.com.tw/file/taifex/Dailydownload/DailydownloadCSV/Daily_2026_08_05.zip
    https://www.taifex.com.tw/file/taifex/Dailydownload/DailydownloadCSV/Daily_2026_08_06.zip
    """
    options_page = b"""
    https://www.taifex.com.tw/file/taifex/Dailydownload/OptionsDailydownloadCSV/OptionsDaily_2026_08_04.zip
    https://www.taifex.com.tw/file/taifex/Dailydownload/OptionsDailydownloadCSV/OptionsDaily_2026_08_05.zip
    https://www.taifex.com.tw/file/taifex/Dailydownload/OptionsDailydownloadCSV/OptionsDaily_2026_08_06.zip
    """

    futures = MODULE._extract_downloads(futures_page, pattern=MODULE.FUTURES_URL_RE)
    options = MODULE._extract_downloads(options_page, pattern=MODULE.OPTIONS_URL_RE)

    assert MODULE._selected_common_dates(futures, options, count=2) == [
        date(2026, 8, 5),
        date(2026, 8, 6),
    ]


def test_recent_listing_cutoff_rejects_unfinished_and_future_dates() -> None:
    taipei = ZoneInfo("Asia/Taipei")
    assert MODULE._latest_completed_listing_date(
        datetime(2026, 9, 25, 5, 24, tzinfo=taipei)
    ) == date(2026, 9, 24)
    assert MODULE._latest_completed_listing_date(
        datetime(2026, 9, 25, 17, 0, tzinfo=taipei)
    ) == date(2026, 9, 25)


def test_recent_rolling_gap_reuses_only_hash_verified_prior_window(
    tmp_path: Path,
) -> None:
    root = tmp_path.resolve()
    days = [date(2026, 9, 23), date(2026, 9, 24)]
    raw_downloads: list[dict[str, object]] = []
    partitions: list[dict[str, object]] = []
    listing_pages: dict[str, dict[str, object]] = {}
    for kind in ("futures", "options"):
        page_path = root / "raw" / "listing_pages" / f"{kind}.html"
        page_path.parent.mkdir(parents=True, exist_ok=True)
        page_path.write_bytes(b"official prior listing")
        listing_pages[kind] = {
            "path": str(page_path),
            "bytes": page_path.stat().st_size,
            "sha256": MODULE._sha256_path(page_path),
        }
        for day in days:
            compact = day.strftime("%Y_%m_%d")
            stem = f"Daily_{compact}" if kind == "futures" else f"OptionsDaily_{compact}"
            raw_path = root / "raw" / kind / f"{stem}.zip"
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(raw_path, "w") as archive:
                archive.writestr(f"{stem}.csv", b"column_a,column_b\n" + b"1,2\n" * 30)
            raw_sha = MODULE._sha256_path(raw_path)
            raw_downloads.append({
                "kind": kind,
                "trading_date": day.isoformat(),
                "path": str(raw_path),
                "bytes": raw_path.stat().st_size,
                "sha256": raw_sha,
            })
            parquet_path, receipt_path = MODULE._partition_paths(root, kind, day)
            parquet_path.parent.mkdir(parents=True, exist_ok=True)
            parquet_path.write_bytes(b"verified partition bytes")
            receipt = {
                "parser_contract_version": MODULE.PARSER_CONTRACT_VERSION,
                "kind": kind,
                "product": "TX" if kind == "futures" else "TXO",
                "trading_date": day.isoformat(),
                "source_path": str(raw_path),
                "source_sha256": raw_sha,
                "output_path": str(parquet_path),
                "output_bytes": parquet_path.stat().st_size,
                "output_sha256": MODULE._sha256_path(parquet_path),
            }
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
            partitions.append({**receipt, "reused": False})
    manifest = {
        "status": "complete",
        "parser_contract_version": MODULE.PARSER_CONTRACT_VERSION,
        "requested_days": 2,
        "date_start": days[0].isoformat(),
        "date_end": days[-1].isoformat(),
        "trading_dates": [day.isoformat() for day in days],
        "raw_downloads": raw_downloads,
        "partitions": partitions,
        "listing_pages": listing_pages,
    }
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    ready, reason, digest = MODULE._reuse_previous_complete_window(
        root, common_dates=days[1:], requested_days=2
    )
    assert ready and reason == "verified_previous_30_day_window" and digest
    assert MODULE._reuse_previous_complete_window(
        root, common_dates=days, requested_days=2
    )[0] is False
    raw_path = Path(str(raw_downloads[0]["path"]))
    raw_path.write_bytes(raw_path.read_bytes() + b"tampered")
    assert MODULE._reuse_previous_complete_window(
        root, common_dates=days[1:], requested_days=2
    )[:2] == (False, "prior_raw_hash_mismatch")


def test_options_preserve_side_rows_and_pair_matched_quantity() -> None:
    source = io.StringIO(
        "成交日期,商品代號,履約價格,到期月份(週別),買賣權別,成交時間,成交價格,成交數量(B or S),開盤集合競價\n"
        "20260805,TXO,44000,202608W1,C,150000,123,3,*\n"
        "20260805,TXO,44000,202608W1,C,150000,123,3,*\n"
        "20260806,CBO,21,202609,P,090000,1.99,2,\n"
    )

    frame = MODULE._parse_options_rows(
        source,
        trading_date=date(2026, 8, 6),
        source_file="OptionsDaily_2026_08_06.zip",
        source_sha256="a" * 64,
    )

    assert frame.height == 2
    assert frame["product"].unique().to_list() == ["TXO"]
    assert frame["session"].unique().to_list() == ["night"]
    assert frame["reported_side_quantity"].sum() == 6
    assert frame["matched_quantity_equivalent"].sum() == 3.0
    assert frame["event_ts"].dtype.time_zone == "Asia/Taipei"


def test_futures_convert_b_plus_s_quantity_to_matched_contracts() -> None:
    source = io.StringIO(
        "成交日期,商品代號,到期月份(週別),成交時間,成交價格,成交數量(B+S),近月價格,遠月價格,開盤集合競價\n"
        "20260806,TX,202608,084500,44580,8,-,-,*\n"
        "20260806,MTX,202608,084500,44580,8,-,-,*\n"
    )

    frame = MODULE._parse_futures_rows(
        source,
        trading_date=date(2026, 8, 6),
        source_file="Daily_2026_08_06.zip",
        source_sha256="b" * 64,
    )

    assert frame.height == 1
    assert frame["product"].to_list() == ["TX"]
    assert frame["reported_b_plus_s_quantity"].to_list() == [8]
    assert frame["matched_quantity"].to_list() == [4]
    assert frame["session"].to_list() == ["day"]


def test_futures_preserve_zero_and_negative_calendar_spread_prices() -> None:
    source = io.StringIO(
        "成交日期,商品代號,到期月份(週別),成交時間,成交價格,成交數量(B+S),近月價格,遠月價格,開盤集合競價\n"
        "20260819,TX,202608/202609,102057,0,20,44577,44577,\n"
        "20260819,TX,202608/202609,102058,-1,8,44580,44579,\n"
    )

    frame = MODULE._parse_futures_rows(
        source,
        trading_date=date(2026, 8, 19),
        source_file="Daily_2026_08_19.zip",
        source_sha256="b" * 64,
    )

    assert frame["price"].to_list() == [0.0, -1.0]
    assert frame["matched_quantity"].to_list() == [10, 4]


def test_futures_still_reject_non_positive_outright_price() -> None:
    source = io.StringIO(
        "成交日期,商品代號,到期月份(週別),成交時間,成交價格,成交數量(B+S),近月價格,遠月價格,開盤集合競價\n"
        "20260819,TX,202608,102057,0,20,-,-,\n"
    )

    with pytest.raises(ValueError, match="non-positive price"):
        MODULE._parse_futures_rows(
            source,
            trading_date=date(2026, 8, 19),
            source_file="Daily_2026_08_19.zip",
            source_sha256="b" * 64,
        )


def test_options_fail_closed_on_unpaired_side_quantity() -> None:
    source = io.StringIO(
        "成交日期,商品代號,履約價格,到期月份(週別),買賣權別,成交時間,成交價格,成交數量(B or S),開盤集合競價\n"
        "20260806,TXO,44000,202608W1,C,090000,123,3,\n"
    )

    with pytest.raises(ValueError, match="unpaired B-or-S quantities"):
        MODULE._parse_options_rows(
            source,
            trading_date=date(2026, 8, 6),
            source_file="OptionsDaily_2026_08_06.zip",
            source_sha256="a" * 64,
        )


def test_futures_fail_closed_on_odd_b_plus_s_quantity() -> None:
    source = io.StringIO(
        "成交日期,商品代號,到期月份(週別),成交時間,成交價格,成交數量(B+S),近月價格,遠月價格,開盤集合競價\n"
        "20260806,TX,202608,084500,44580,7,-,-,*\n"
    )

    with pytest.raises(ValueError, match="positive and even"):
        MODULE._parse_futures_rows(
            source,
            trading_date=date(2026, 8, 6),
            source_file="Daily_2026_08_06.zip",
            source_sha256="b" * 64,
        )
