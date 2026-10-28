import csv
import hashlib
import json

import pytest

from downloader.artifact_io import sha256_file
from scripts.audit_tw_futures_current_catalog import audit
from stockagent.data.tw_stock_futures_catalog import SOURCE_URL, rows_digest


def _sources(tmp_path, current, old):
    raw = b"official catalog fixture"
    digest = hashlib.sha256(raw).hexdigest()
    (tmp_path / "stock_futures_catalog_sources").mkdir()
    (tmp_path / "stock_futures_catalog_sources" / (digest + ".html")).write_bytes(raw)
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"schema_version": 1, "source_url": SOURCE_URL,
        "scope": "latest_catalog_not_signal_date", "complete": True,
        "row_count": len(current), "rows": current, "rows_sha256": rows_digest(current),
        "source_sha256": digest, "retrieved_at_utc": "2026-10-27T22:00:00+00:00"}))
    previous = tmp_path / "previous.csv"
    with previous.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["product", "underlying_symbol", "asset_class"])
        writer.writeheader()
        writer.writerows(old)
    receipt = tmp_path / "previous.json"
    receipt.write_text(json.dumps({"contract": "current_official_family_catalog_membership_readback_v1",
                                   "output": {"sha256": sha256_file(previous)}}))
    return catalog, previous, receipt


def test_catalog_comparison_detects_a_reused_code_and_excludes_non_equity(tmp_path):
    current = [{"product": "AA", "symbol": "2330", "has_futures": True},
               {"product": "BB", "symbol": "0050", "has_futures": True}]
    old = [{"product": "AAF", "underlying_symbol": "2330", "asset_class": "stock_future"},
           {"product": "BBF", "underlying_symbol": "0060", "asset_class": "etf_future"},
           {"product": "TX", "underlying_symbol": "", "asset_class": "index_future"}]
    paths = _sources(tmp_path, current, old)
    output = tmp_path / "result.json"
    result = audit(*paths, output)
    assert result["same_family_and_underlying_count"] == 1
    assert result["current_family_not_in_previous"] == [{"product": "BBF", "underlying_symbol": "0050"}]
    assert result["previous_family_not_in_current"] == [{"product": "BBF", "underlying_symbol": "0060"}]
    assert result["current_membership_is_not_historical_eligibility"] is True
    with pytest.raises(FileExistsError):
        audit(*paths, output)


def test_catalog_comparison_rejects_changed_official_body(tmp_path):
    current = [{"product": "AA", "symbol": "2330", "has_futures": True}]
    old = [{"product": "AAF", "underlying_symbol": "2330", "asset_class": "stock_future"}]
    paths = _sources(tmp_path, current, old)
    raw = next((tmp_path / "stock_futures_catalog_sources").iterdir())
    raw.write_text("changed")
    with pytest.raises(ValueError, match="raw body"):
        audit(*paths, tmp_path / "result.json")
    assert not (tmp_path / "result.json").exists()
