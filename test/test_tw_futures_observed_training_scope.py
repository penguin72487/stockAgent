from datetime import date
import csv
import hashlib
import json
from pathlib import Path
from zipfile import ZipFile

import pytest

from scripts.audit_tw_futures_observed_training_scope import audit


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _fixture(tmp_path: Path, additional: str = "") -> dict:
    day = date(2026, 10, 29)
    archive = tmp_path / "Daily_2026_10_29.zip"
    text = (
        "日期,商品,契約,時間,價格,數量,近月,遠月,開盤\r\n"
        "20261028,ABC,202612,150001,100,2,-,-, \r\n"
        "20261029,DEF,202611,090001,200,4,-,-, \r\n"
        "20261029,EXT,202611,090002,300,2,-,-, \r\n"
        "20261029,NEW,202611,090003,400,2,-,-, \r\n"
        "20261029,ABC,202611/202612,090004,-2,2,-,-, \r\n"
        + additional
    )
    with ZipFile(archive, "w") as zipped:
        zipped.writestr("Daily_2026_10_29.csv", text.encode("cp950"))
    url = ("https://www.taifex.com.tw/file/taifex/Dailydownload/"
           "DailydownloadCSV/Daily_2026_10_29.zip")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "dataset": "taifex_recent_tx_txo_trade_by_trade", "status": "complete",
        "raw_downloads": [{"kind": "futures", "trading_date": str(day),
                           "path": str(archive), "url": url,
                           "bytes": archive.stat().st_size, "sha256": _sha(archive)}],
    }))
    scope = tmp_path / "scope.csv"
    _write_csv(scope, ["product", "selected_lifetimes"], [
        {"product": "ABC", "selected_lifetimes": 1},
        {"product": "DEF", "selected_lifetimes": 0},
    ])
    catalog = tmp_path / "catalog.csv"
    _write_csv(catalog, ["product", "product_name", "asset_class", "region", "settlement_currency"], [
        {"product": "ABC", "product_name": "a", "asset_class": "stock_future", "region": "domestic", "settlement_currency": "TWD"},
        {"product": "DEF", "product_name": "b", "asset_class": "etf_future", "region": "domestic", "settlement_currency": "TWD"},
        {"product": "EXT", "product_name": "c", "asset_class": "commodity_future", "region": "foreign", "settlement_currency": "USD"},
    ])
    return {"manifest": manifest, "day": day, "scope": scope,
            "scope_sha256": _sha(scope), "catalog": catalog}


def test_observed_scope_does_not_promote_unknown_currency_or_unselected_lifetimes(tmp_path):
    result = audit(**_fixture(tmp_path))
    assert result["selected_training_products"] == 1
    assert result["observed_outright_products"] == 4
    assert result["observed_in_training_scope"] == 1
    assert result["known_twd_settlement_outside_training_scope"] == ["DEF"]
    assert result["unknown_to_historical_catalog"] == ["NEW"]
    assert result["source_event_dates"] == ["20261028", "20261029"]
    assert result["spread_rows_excluded"] == 1


def test_copied_scope_and_native_archive_are_both_hash_bound(tmp_path):
    args = _fixture(tmp_path)
    args["scope"].write_text(args["scope"].read_text() + "\n")
    with pytest.raises(ValueError, match="training scope differs"):
        audit(**args)
    args = _fixture(tmp_path)
    with (tmp_path / "Daily_2026_10_29.zip").open("ab") as stream:
        stream.write(b"changed")
    with pytest.raises(ValueError, match="archive differs"):
        audit(**args)


@pytest.mark.parametrize("row", [
    "20261029,ABC,202611,090005,nan,2,-,-, \r\n",
    "20261029,ABC,202611,090005,100,3,-,-, \r\n",
    "20261101,ABC,202611,090005,100,2,-,-, \r\n",
])
def test_rejects_invalid_native_trade_instead_of_counting_it(tmp_path, row):
    with pytest.raises(ValueError):
        audit(**_fixture(tmp_path, row))
