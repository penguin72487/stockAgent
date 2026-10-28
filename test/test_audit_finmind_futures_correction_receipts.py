import hashlib
import json

import pytest

from scripts.audit_finmind_futures_correction_receipts import inspect_receipt


@pytest.mark.parametrize(
    ("damage", "expected"),
    [
        (None, "fetched_after_notice_sha256_verified"),
        ("missing_receipt", "receipt_missing"),
        ("old_fetch", "fetch_not_proven_after_notice"),
        ("naive_time", "receipt_identity_or_status_invalid"),
        ("wrong_dataset", "receipt_identity_or_status_invalid"),
        ("wrong_status", "receipt_identity_or_status_invalid"),
        ("missing_parquet", "parquet_not_materialized"),
        ("bad_hash", "parquet_sha256_mismatch"),
        ("escaping_path", "invalid_parquet_path"),
        ("malformed_json", "receipt_unreadable"),
        ("not_object", "receipt_identity_or_status_invalid"),
    ],
)
def test_inspect_receipt_requires_post_notice_matching_payload(tmp_path, damage, expected):
    dataset, day = "TaiwanFuturesTick", "2018-02-13"
    receipt = tmp_path / "receipts" / dataset / "all" / f"{day}.json"
    receipt.parent.mkdir(parents=True)
    parquet = tmp_path / "parquet" / "data.parquet"
    parquet.parent.mkdir()
    parquet.write_bytes(b"retained source payload")
    item = {
        "dataset": dataset, "partition": day, "data_id": "", "status": "complete",
        "fetched_at_utc": "2026-09-21T00:00:00+00:00", "rows": 42,
        "sha256": hashlib.sha256(parquet.read_bytes()).hexdigest(),
        "parquet_path": "parquet/data.parquet",
    }
    if damage == "old_fetch":
        item["fetched_at_utc"] = "2026-09-20T23:59:59+00:00"
    elif damage == "naive_time":
        item["fetched_at_utc"] = "2026-09-21T00:00:00"
    elif damage == "wrong_dataset":
        item["dataset"] = "TaiwanFuturesKBar"
    elif damage == "wrong_status":
        item["status"] = "pending"
    elif damage == "missing_parquet":
        parquet.unlink()
    elif damage == "bad_hash":
        item["sha256"] = "0" * 64
    elif damage == "escaping_path":
        item["parquet_path"] = "../outside.parquet"
    elif damage == "not_object":
        item = []
    if damage != "missing_receipt":
        receipt.write_text("{" if damage == "malformed_json" else json.dumps(item))
    result = inspect_receipt(tmp_path, dataset, day, "2026-09-20")
    assert result["state"] == expected
