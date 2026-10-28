import hashlib
import json
from datetime import date

import polars as pl
import pytest

from scripts.audit_tw_futures_holiday_terminal_sources import audit


def test_official_holiday_readback_checks_source_bytes_and_distinguishes_matches(tmp_path):
    target = date(2012, 1, 19)
    receipts = []
    for kind in ("index_futures", "stock_etf_futures", "commodity_futures", "interest_rate_futures"):
        raw = (b"<td>2012/1/19</td>" if kind == "stock_etf_futures" else b"<td>2012/1/18</td>")
        path = tmp_path / f"{kind}.html"
        path.write_bytes(raw)
        receipts.append({"kind": kind, "period": "2012-01" if kind == "stock_etf_futures" else "2012",
                         "path": str(path), "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)})
    normalized = tmp_path / "source.parquet"
    pl.DataFrame({"settlement_date": [target], "product": ["ABC"], "contract": ["201202"],
                  "source_kind": ["official_stock_etf_futures_html"],
                  "reported_date_role": ["final_settlement_day"],
                  "settlement_method": ["cash_settlement"]}).write_parquet(normalized)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "schema_version": 2, "status": "complete", "requested_start_date": "2012-01-01",
        "requested_end_date": "2012-12-31", "receipts": receipts,
        "outputs": {"futures_final_settlement_history": {
            "sha256": hashlib.sha256(normalized.read_bytes()).hexdigest(),
            "bytes": normalized.stat().st_size, "rows": 1}},
    }))
    result = audit(manifest, normalized, [target])
    assert [item["kind"] for item in result["raw_date_matches"]] == ["stock_etf_futures"]
    assert result["normalized_date_matches"][0]["product"] == "ABC"
    assert result["financial_or_training_adopted"] is False
    assert any("broker" in text for text in result["scope_limits"])

    # A formerly matching document cannot be silently replaced by an empty one.
    (tmp_path / "stock_etf_futures.html").write_bytes(b"<td>2012/1/18</td>")
    with pytest.raises(ValueError, match="official raw source changed"):
        audit(manifest, normalized, [target])
    with pytest.raises(ValueError, match="inside the retained source horizon"):
        audit(manifest, normalized, [date(2011, 1, 31)])
