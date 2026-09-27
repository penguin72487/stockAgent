from datetime import UTC, datetime
import hashlib
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from downloader import release_archive_io as receipts
from downloader.download_tw_cbc_money_release_archive import LIST_URL, OUTPUT_NAME
from downloader.release_archive_io import read_release_resume_state, write_release_state
from scripts.audit_tw_official_release_archives import audit_one
from scripts.audit_tw_public_data_layer import audit_release_vintage_contract
from stockagent.live.data_monitor_dashboard import _tw_public_sources


@pytest.mark.parametrize("oversized", [False, True])
def test_resume_proof_does_not_green_current_dashboard_audit_or_training(
    tmp_path: Path, monkeypatch, oversized,
):
    if oversized:
        monkeypatch.setattr(receipts, "MAX_RELEASE_STATE_BYTES", 4096)
    root = tmp_path / "data_tw_public"
    listing = root / "raw" / OUTPUT_NAME / "list" / "listing.html"
    detail = root / "raw" / OUTPUT_NAME / "detail" / "article.html"
    for path in (listing, detail):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"test original bytes")
    raw_hash = hashlib.sha256(detail.read_bytes()).hexdigest()
    parquet = root / f"{OUTPUT_NAME}.parquet"
    pl.DataFrame([
        {"release_url": "https://www.cbc.gov.tw/tw/cp-302-1-test.html",
         "metric": metric, "period": "2013-01", "published_on": "2013-02-25",
         "value_pct": 2.0, "html_path": str(detail), "html_sha256": raw_hash}
        for metric in ("m1b_yoy_pct", "m2_yoy_pct")
    ]).write_parquet(parquet)
    completed = {
        "dataset": OUTPUT_NAME, "status": "complete", "complete": True,
        "value_history_complete": True, "value_releases": 1, "failures": [], "missing_periods": [],
        "earliest_period": "2013-01", "latest_period": "2013-01",
        "registered_releases": 1, "saved_releases": 1, "scan_scope": "full_index",
        "generated_at_utc": "2026-09-25T00:00:00+00:00",
        "parquet_sha256": hashlib.sha256(parquet.read_bytes()).hexdigest(),
        "listing_receipts": [{"page": 1, "url": LIST_URL.format(page=1),
                              "path": str(listing), "sha256": raw_hash}],
    }
    config = SimpleNamespace(data=SimpleNamespace(
        feature_include=["twpub_cbc_m1b_yoy"], feature_exclude=[], feature_zero_fill=[],
        panel_start_date="2014-01-01",
    ))
    write_release_state(root, OUTPUT_NAME, completed)
    # Positive controls ensure these checks are capable of accepting the source.
    assert audit_one(root, OUTPUT_NAME)["coverage_complete"] is True
    assert audit_one(root, OUTPUT_NAME)["integrity_ok"] is True
    assert audit_release_vintage_contract(root, config) == []
    before = next(row for row in _tw_public_sources(tmp_path, now=datetime.now(UTC))
                  if row["id"] == f"tw-public:{OUTPUT_NAME}")
    assert before["status"] == "complete"

    write_release_state(root, OUTPUT_NAME, {
        "dataset": OUTPUT_NAME, "status": "degraded", "complete": False,
        "error": "SourceAccessBlocked" + ("x" * 4096 if oversized else ""),
        "generated_at_utc": "2026-09-26T00:00:00+00:00",
    })
    assert read_release_resume_state(root, OUTPUT_NAME) == completed
    after = next(row for row in _tw_public_sources(tmp_path, now=datetime.now(UTC))
                 if row["id"] == f"tw-public:{OUTPUT_NAME}")
    assert after["status"] == "degraded"
    assert after["publishable"] is False
    assert audit_one(root, OUTPUT_NAME)["coverage_complete"] is False
    findings = audit_release_vintage_contract(root, config)
    assert len(findings) == 1
    assert findings[0].code == "incomplete_release_vintage"
