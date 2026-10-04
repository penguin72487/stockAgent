import hashlib
import json

import polars as pl

from scripts.shioaji_minute_backfill_state import (
    frontier_state,
    may_reuse_materialization,
)


def test_frontier_checks_exact_symbols_and_reuse_requires_unchanged_rows(tmp_path):
    source = tmp_path / "source"
    stock = tmp_path / "stocks"
    research = tmp_path / "research"
    audit_path = tmp_path / "audit.json"
    for path in (source, stock, research):
        path.mkdir()
    (source / "symbols").mkdir()
    (source / "download_summary.json").write_text(json.dumps({
        "start_date": "2020-03-02", "end_date": "2026-09-22",
        "selected_symbols": 2, "reported_symbols": 2,
        "resumable_collection_complete": True,
    }))
    report_path = source / "download_report.csv"
    pl.DataFrame({
        "symbol": ["1111", "2222"],
        "status": ["complete", "contract_unavailable"],
        "source_minute_rows": [100, 200],
    }).write_csv(report_path)
    source_manifest = source / "symbols" / "1111.manifest.json"
    source_manifest.write_text(json.dumps({
        "requested_start": "2020-03-02",
        "requested_end": "2026-09-22",
        "source_gap_dates": [],
        "terminal_coverage_dates": ["2026-09-22"],
        "chunks": [{
            "start_date": "2026-09-22", "end_date": "2026-09-22",
            "rows": 100, "data_sha256": "aaa", "source_gap_dates": [],
            "underlying_data_method": "provider_kbars",
        }],
    }))
    symbols_path = stock / "symbols.csv"
    pl.DataFrame({
        "code": ["1111", "2222"],
        "security_type": ["stock", "etf"],
    }).write_csv(symbols_path)
    ready, rows, fingerprint = frontier_state(
        source_root=source, stock_root=stock, target_date="2026-09-22",
    )
    assert (ready, rows) == (True, 300)
    assert len(fingerprint) == 64
    manifest_bytes = json.dumps({
        "research_ready": True,
        "download_start_date": "2020-03-02",
        "download_end_date": "2026-09-22",
        "source_fingerprint_sha256": fingerprint,
    }).encode()
    (research / "manifest.json").write_bytes(manifest_bytes)
    audit_path.write_text(json.dumps({
        "status": "research_ready", "last_date": "2026-09-22",
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
    }))
    def reusable():
        return may_reuse_materialization(
            source_root=source, research_root=research, audit_path=audit_path,
            target_date="2026-09-22", frontier_was_ready=True,
            source_rows_before=rows, source_fingerprint_before=fingerprint,
        )
    assert reusable()
    old_manifest = json.loads(manifest_bytes)
    old_manifest.pop("source_fingerprint_sha256")
    (research / "manifest.json").write_text(json.dumps(old_manifest))
    assert not reusable()  # Old manifests have no proof they match this source.
    (research / "manifest.json").write_bytes(manifest_bytes)
    changed = json.loads(source_manifest.read_text())
    changed["chunks"][0]["data_sha256"] = "bbb"
    source_manifest.write_text(json.dumps(changed))
    assert not reusable()  # same row count, different K-bar bytes
    changed["chunks"][0]["data_sha256"] = "aaa"
    source_manifest.write_text(json.dumps(changed))
    pl.DataFrame({
        "symbol": ["1111", "2222"],
        "status": ["complete", "contract_unavailable"],
        "source_minute_rows": [101, 200],
    }).write_csv(report_path)
    assert not reusable()
    pl.DataFrame({
        "code": ["1111", "3333"],
        "security_type": ["stock", "etf"],
    }).write_csv(symbols_path)
    assert frontier_state(
        source_root=source, stock_root=stock, target_date="2026-09-22",
    )[0] is False
