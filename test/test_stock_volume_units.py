import polars as pl
import json
from datetime import datetime

from downloader.stock_volume_units import with_stock_share_volume


def test_amount_price_evidence_must_have_one_multiplier_and_whole_shares():
    frame = pl.DataFrame({
        "Volume": [1., 1000., 1., .0001, 0., .001],
        "Amount": [100_000., 100_000., 10_000., 10., 0., 1.],
        "Low": [100., 100., 10., 100., 100., 1.],
        "High": [100., 100., 100., 100., 100., 10.],
        "contract_unit": [1000.] * 6,
    })
    normalized = with_stock_share_volume(frame, tolerance=.001)
    assert normalized["volume_shares"].to_list() == [1000., 1000., None, None, 0., None]
    assert normalized["source_volume_multiplier"].to_list()[:3] == [1000., 1., None]
    assert normalized["volume_unit_proof"].to_list()[2:4] == ["unresolved", "unresolved"]
    assert frame["Volume"].equals(normalized["Volume"])


def test_projected_audit_reports_unsafe_existing_shares_without_rewriting(tmp_path):
    from scripts.audit_shioaji_tw_minute_dataset import audit_volume_units

    root = tmp_path / "dataset"
    part = root / "trade_date=2026-09-24" / "data.parquet"
    part.parent.mkdir(parents=True)
    pl.DataFrame({
        "ts": [datetime(2026, 9, 24, 9, 1)], "symbol": ["2330"],
        "Volume": [1.], "Amount": [10_000.], "Low": [10.], "High": [100.],
        "contract_unit": [1000.], "volume_shares": [1000.],
    }).write_parquet(part)
    original = part.read_bytes()
    (root / "manifest.json").write_text(json.dumps({
        "partitions": [{"trade_date": "2026-09-24"}],
    }))
    report = audit_volume_units(root, tmp_path / "audit.json")
    assert report["status"] == "needs_repair"
    assert report["unsafe_stored_share_rows"] == 1
    assert report["examples"][0]["volume_shares"] is None
    assert part.read_bytes() == original
