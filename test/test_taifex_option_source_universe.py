import csv
from datetime import date
import hashlib
import json
from pathlib import Path
import zipfile

import pytest
import polars as pl

from scripts.audit_taifex_option_source_universe import _csv_column_names, audit
from scripts.audit_taifex_option_tick_opening_scope import _official_counts
from scripts.audit_taifex_option_contract_events import _compare_multisets
from scripts.audit_taifex_option_daily_tick_coverage import official_product_days, receipt_status


def _inputs(tmp_path: Path):
    source_dir = tmp_path / "annual"
    source_dir.mkdir()
    archive = source_dir / "2011_opt.zip"
    # Two dated sessions and an unreported session remain separate. A zero or
    # missing volume does not prove an execution, even if a source row exists.
    body = (
        "交易日期,契約,成交量,交易時段\n"
        "2011/1/3,ADO,3,一般\n"
        "2011/1/3,ADO,2,盤後\n"
        "2011/1/3,ADO,0,一般\n"
        "2011/1/4,ADO,1,\n"
        "2011/1/5,TXO,-,一般\n"
    )
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("2011.csv", body.encode("cp950"))
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"receipts": [{
        "path": str(archive), "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
    }]}), encoding="utf-8")
    catalog = tmp_path / "catalog.csv"
    with catalog.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerows([["code", "product_name"], ["ADO", "台積電選擇權"], ["TXO", "臺指選擇權"]])
    return source_dir, manifest, catalog


def test_daily_observation_is_not_execution_and_sessions_remain_distinct(tmp_path):
    source_dir, manifest, catalog = _inputs(tmp_path)
    rows, summary = audit(source_dir, manifest, catalog, 2011, 2011)
    by_code = {row["product"]: row for row in rows}
    assert by_code["ADO"]["positive_volume_days"] == 2
    assert by_code["ADO"]["positive_day_session_dates"] == 1
    assert by_code["ADO"]["positive_night_session_dates"] == 1
    assert by_code["ADO"]["positive_unreported_session_dates"] == 1
    assert by_code["TXO"]["positive_volume_days"] == 0
    assert by_code["TXO"]["invalid_volume_rows"] == 1
    assert summary["products_with_positive_volume"] == 1
    assert by_code["ADO"]["currency"] == "unverified"


def test_rejects_changed_source_before_publication(tmp_path):
    source_dir, manifest, catalog = _inputs(tmp_path)
    manifest.write_text(json.dumps({"receipts": [{
        "path": str(source_dir / "2011_opt.zip"), "sha256": "0" * 64,
    }]}), encoding="utf-8")
    with pytest.raises(ValueError, match="SHA mismatch"):
        audit(source_dir, manifest, catalog, 2011, 2011)


def test_unlabelled_trailing_empty_column_is_not_a_missing_transaction():
    from io import BytesIO

    binary = BytesIO("交易日期,契約,成交量,交易時段\n2018/7/2,CAO,3,一般,\n".encode("cp950"))
    names, adjusted = _csv_column_names(binary, "annual.csv")
    assert adjusted is True
    assert names[-1] == "__unlabelled_4"
    assert binary.tell() == 0

    invalid = BytesIO("交易日期,契約\n2018/7/2,CAO,unknown\n".encode("cp950"))
    with pytest.raises(ValueError, match="unlabelled nonempty"):
        _csv_column_names(invalid, "annual.csv")


def test_tick_sample_distinguishes_calendar_night_and_decision_windows(tmp_path):
    archive = tmp_path / "options.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("options.csv", (
            "header\n--- separator\n"
            "20261022,TXO,1,202610,P,090000,1,1\n"
            "20261027,TXO,1,202610,P,084500,1,1\n"
            "20261027,TXO,1,202610,P,084500,1,1\n"
            "20261027,CAO,1,202610,P,092723,1,1\n"
            "20261027,CAO,1,202610,P,134500,1,1\n"
        ).encode("ascii"))
    counts = _official_counts(archive, date(2026, 10, 27))
    assert counts["0845_0905"]["TXO"] == [2, "084500"]
    assert "CAO" not in counts["0845_0905"]
    assert counts["day_0845_1345"]["CAO"] == [1, "092723"]


def test_event_reconciliation_compares_side_row_multiplicity():
    columns = ["product", "price", "volume"]
    official = pl.DataFrame({"product": ["CCO", "CCO", "TGO"], "price": [1.0, 1.0, 2.0], "volume": [1, 1, 3]})
    vendor = pl.DataFrame({"product": ["CCO", "TGO", "TGO"], "price": [1.0, 2.0, 3.0], "volume": [1, 3, 2]})
    missing, extra, missing_examples, extra_examples = _compare_multisets(official, vendor, columns)
    assert missing == extra == 1
    assert missing_examples[0]["product"] == "CCO"
    assert extra_examples[0]["price"] == 3.0
    assert _compare_multisets(official, official, columns)[:2] == (0, 0)


def test_annual_positive_product_dates_keep_session_identity(tmp_path):
    source_dir, manifest, _ = _inputs(tmp_path)
    days, sources, _ = official_product_days(source_dir, manifest, 2011, 2011)
    assert days[date(2011, 1, 3)]["day"] == {"ADO"}
    assert days[date(2011, 1, 3)]["night"] == {"ADO"}
    assert days[date(2011, 1, 4)]["unreported"] == {"ADO"}
    assert date(2011, 1, 5) not in days
    assert len(sources) == 1


def test_tick_receipt_checks_identity_and_payload_hash(tmp_path):
    day = date(2011, 1, 3)
    root = tmp_path
    source = root / "parquet/option/sample.parquet"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"retained source")
    receipt = root / "receipts/TaiwanOptionTick/all/2011-01-03.json"
    receipt.parent.mkdir(parents=True)
    body = {"dataset": "TaiwanOptionTick", "data_id": "", "partition": str(day),
            "status": "complete", "rows": 2, "parquet_path": str(source.relative_to(root)),
            "parquet_size_bytes": source.stat().st_size, "sha256": hashlib.sha256(source.read_bytes()).hexdigest()}
    receipt.write_text(json.dumps(body), encoding="utf-8")
    assert receipt_status(day, root, verify_payload_sha=True)["status"] == "complete_payload_sha_verified"
    source.write_bytes(b"modified source")
    assert receipt_status(day, root, verify_payload_sha=True)["status"] == "complete_payload_sha_mismatch"
    body["partition"] = "2011-01-04"
    receipt.write_text(json.dumps(body), encoding="utf-8")
    assert receipt_status(day, root, verify_payload_sha=True)["status"] == "receipt_identity_mismatch"
