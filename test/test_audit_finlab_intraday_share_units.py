from datetime import date, datetime
import hashlib
import json

import polars as pl
import pytest

from scripts import audit_finlab_intraday_share_units as module


def _fixture(tmp_path, defect):
    root, reference_root = tmp_path / "finlab", tmp_path / "shioaji"
    day = date(2026, 9, 24)
    ref_path = reference_root / "minute_chunks/2330/2026-09-24_2026-09-24.receipt.json"
    ref_path.parent.mkdir(parents=True)
    ref_object = ref_path.with_suffix("").with_suffix(".parquet")
    pl.DataFrame({
        "symbol": ["2330"], "date": [day], "ts": [datetime(2026, 9, 24, 9, 1)],
        "Open": [100.], "High": [100.], "Low": [100.], "Close": [100.],
        "Volume": [2.], "Amount": [200_000.],
    }).write_parquet(ref_object)
    ref = {
        "status": "ok", "symbol": "2330", "security_type": "stock", "market": "twse",
        "contract_unit": 1000., "rows": 1, "underlying_data_method": "provider_kbars",
        "output_receipt": {"size": ref_object.stat().st_size,
                           "sha256": hashlib.sha256(ref_object.read_bytes()).hexdigest()},
    }
    ref_path.write_text(json.dumps(ref), encoding="utf-8")
    evidence = module._reference("2330", day, reference_root)
    assert evidence is not None
    revision = module.reference_revision("2330", day, reference_root)
    shares = [1000, 1000, None]
    if defect == "nonregular":
        shares[2] = 5000
    if defect == "partial_null":
        shares[1] = None
    frame = pl.DataFrame({"volume": [1, 1, 5], "volume_shares": shares,
                          "session": ["regular", "regular", "after_hours"]})
    target = root / "intraday/objects/tick.parquet"
    target.parent.mkdir(parents=True)
    frame.write_parquet(target)
    receipt = {
        "dataset": "tw_tick:2330", "trade_date": str(day), "fields": frame.columns,
        "rows": 3, "parquet_path": str(target.relative_to(root)),
        "parquet_size_bytes": target.stat().st_size,
        "sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
        "canonical_volume_scope": "regular_session_only", "canonical_volume_unit": "shares",
        "volume_unit_contract_version": 2, "publication_time_status": "not_verified_for_training",
        "volume_multiplier": 1000, "volume_unit_reference": evidence,
        "unit_reference_revision": revision,
    }
    if defect == "hash":
        receipt["sha256"] = "0" * 64
    if defect == "reference_not_direct":
        ref["underlying_data_method"] = "observed_ticks_aggregated_to_right_labelled_1m"
        ref_path.write_text(json.dumps(ref), encoding="utf-8")
    receipt_path = root / "intraday/receipts/tw_tick-2330/2026-09-24.json"
    receipt_path.parent.mkdir(parents=True)
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    return root, reference_root


def test_share_audit_accepts_complete_direct_evidence(tmp_path):
    root, reference_root = _fixture(tmp_path, None)
    result = module.audit(root, reference_root, expected_receipts=1)
    assert result["passed"], result["errors"]
    assert result["families"]["tw_tick"]["nonregular_rows"] == 1


@pytest.mark.parametrize("defect, message", [
    ("hash", "SHA-256 mismatch"),
    ("nonregular", "nonregular session"),
    ("partial_null", "regular-session shares"),
    ("reference_not_direct", "direct KBar evidence"),
])
def test_share_audit_fails_closed_on_broken_evidence(tmp_path, defect, message):
    root, reference_root = _fixture(tmp_path, defect)
    result = module.audit(root, reference_root, expected_receipts=1)
    assert not result["passed"]
    assert any(message in row["error"] for row in result["errors"])
