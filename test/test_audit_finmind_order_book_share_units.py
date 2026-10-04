import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import audit_finmind_order_book_share_units as module


def _fixture(tmp_path, defect=None):
    day = "2026-09-24"
    source = tmp_path / "market_intraday" / module.DATASET / "year=2026" / f"date={day}.parquet"
    source.parent.mkdir(parents=True)
    original = pa.table({"TotalBuyVolume": [2], "TotalSellVolume": [3], "TotalDealVolume": [1],
                         "TotalDealMoney": [0.00000005], "TotalBuyOrder": [4]})
    pq.write_table(original, source)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    old = {"dataset": module.DATASET, "date": day, "rows": 1, "status": "partial",
           "observed_grain": "1m", "fetched_at_utc": "2026-09-26T00:00:00+00:00",
           "parquet_path": str(source.relative_to(tmp_path)), "sha256": digest,
           "parquet_size_bytes": source.stat().st_size}
    version_root = tmp_path / "versions" / module.DATASET / day
    version_root.mkdir(parents=True)
    archive, old_receipt = version_root / f"{digest}.parquet", version_root / f"{digest}.json"
    archive.write_bytes(source.read_bytes())
    old_receipt.write_text(json.dumps(old), encoding="utf-8")
    normalized = original.append_column("TotalBuyVolume_shares", pa.array([2000], pa.int64()))
    normalized = normalized.append_column("TotalSellVolume_shares", pa.array([3000], pa.int64()))
    normalized = normalized.append_column("TotalDealVolume_shares", pa.array([1000], pa.int64()))
    normalized = normalized.append_column("TotalDealMoney_twd", pa.array([0.05], pa.float64()))
    if defect == "shares":
        normalized = normalized.set_column(normalized.schema.get_field_index("TotalBuyVolume_shares"),
                                           "TotalBuyVolume_shares", pa.array([2_000_000], pa.int64()))
    if defect == "raw":
        normalized = normalized.set_column(normalized.schema.get_field_index("TotalBuyOrder"),
                                           "TotalBuyOrder", pa.array([5], pa.int64()))
    pq.write_table(normalized, source)
    current = {**old, "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
               "parquet_size_bytes": source.stat().st_size,
               "unit_normalization_previous_source": {
                   "sha256": digest, "parquet_path": str(archive.relative_to(tmp_path)),
                   "receipt_path": str(old_receipt.relative_to(tmp_path))},
               "volume_units": {"contract_version": 1, "normalization_valid": True,
                   "invalid_rows": 0, "invalid_fields": {}, "market_scope": "twse_regular_trading_only",
                   "multipliers": {name: value[1] for name, value in module.MULTIPLIERS.items()}}}
    if defect == "fetched_at":
        current["fetched_at_utc"] = "2026-09-27T00:00:00+00:00"
    if defect in {"recovery", "recovery_wrong_sha"}:
        current["unit_normalization_recovery"] = {
            "reason": "interrupted_parquet_receipt_commit", "canonical_table_equal": True,
            "reconstructed_from_sha256": digest,
            "verified_new_sha256": current["sha256"] if defect == "recovery" else "0" * 64,
        }
    path = tmp_path / "receipts" / module.DATASET / f"{day}.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(current), encoding="utf-8")


@pytest.mark.parametrize("recovery", [None, "recovery"])
def test_order_book_audit_preserves_fractional_twd_and_partial_status(tmp_path, recovery):
    _fixture(tmp_path, recovery)
    result = module.audit(tmp_path, expected_receipts=1)
    assert result["passed"], result["errors"]
    assert result["preserved_status_counts"] == {"partial": 1}


@pytest.mark.parametrize("defect,message", [
    ("shares", "share quantity mismatch"),
    ("raw", "changed original provider"),
    ("fetched_at", "changed original receipt"),
    ("recovery_wrong_sha", "recovery proof"),
])
def test_order_book_audit_rejects_rehashed_incorrect_migrations(tmp_path, defect, message):
    _fixture(tmp_path, defect)
    result = module.audit(tmp_path, expected_receipts=1)
    assert not result["passed"]
    assert any(message in error["error"] for error in result["errors"])
