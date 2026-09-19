from __future__ import annotations

import hashlib
import json
from pathlib import Path

import polars as pl

from scripts.audit_tw_official_release_archives import audit_one


def test_release_audit_separates_hash_integrity_from_history_completeness(tmp_path: Path) -> None:
    name = "cbc_fx_reserve_release_vintages"
    raw = tmp_path / "raw"
    raw.mkdir()
    listing = raw / "list.html"
    article = raw / "article.html"
    listing.write_bytes(b"official listing")
    article.write_bytes(b"official article")
    pl.DataFrame({
        "period": ["2024-08"], "metric": ["fx_reserves_usd_100m"],
        "html_path": [str(article)],
        "html_sha256": [hashlib.sha256(article.read_bytes()).hexdigest()],
    }).write_parquet(tmp_path / f"{name}.parquet")
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / f"{name}.json").write_text(json.dumps({
        "status": "degraded", "complete": False, "saved_releases": 1,
        "registered_releases": 2, "headline_values": 1,
        "failures": [{"release_url": "missing"}],
        "listing_receipts": [{"path": str(listing),
                              "sha256": hashlib.sha256(listing.read_bytes()).hexdigest()}],
        "parquet_sha256": hashlib.sha256((tmp_path / f"{name}.parquet").read_bytes()).hexdigest(),
    }), encoding="utf-8")
    result = audit_one(tmp_path, name)
    assert result["integrity_ok"] is True
    assert result["coverage_complete"] is False
    assert result["verified_files"] == 3

    article.write_bytes(b"tampered")
    result = audit_one(tmp_path, name)
    assert result["integrity_ok"] is False
    assert any("SHA-256 differs" in error for error in result["errors"])


def test_release_audit_rejects_false_complete_state(tmp_path: Path) -> None:
    name = "cbc_fx_reserve_release_vintages"
    raw = tmp_path / "raw"
    raw.mkdir()
    listing = raw / "list.html"
    article = raw / "article.html"
    listing.write_bytes(b"official listing")
    article.write_bytes(b"official article")
    parquet = tmp_path / f"{name}.parquet"
    pl.DataFrame({
        "period": ["2024-08"], "metric": ["fx_reserves_usd_100m"],
        "value": [None], "html_path": [str(article)],
        "html_sha256": [hashlib.sha256(article.read_bytes()).hexdigest()],
    }).write_parquet(parquet)
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / f"{name}.json").write_text(json.dumps({
        "status": "complete", "complete": True, "saved_releases": 1,
        "registered_releases": 2, "headline_values": 1, "failures": [],
        "missing_periods": ["2024-07"],
        "listing_receipts": [{"path": str(listing),
                              "sha256": hashlib.sha256(listing.read_bytes()).hexdigest()}],
        "parquet_sha256": hashlib.sha256(parquet.read_bytes()).hexdigest(),
    }), encoding="utf-8")

    result = audit_one(tmp_path, name)
    assert result["coverage_complete"] is False
    assert result["integrity_ok"] is False
    assert any("uncollected registered" in error for error in result["errors"])
    assert any("missing periods" in error for error in result["errors"])
    assert any("without a usable value" in error for error in result["errors"])


def test_cbc_release_audit_independently_checks_month_sequence(tmp_path: Path) -> None:
    name = "cbc_fx_reserve_release_vintages"
    listing = tmp_path / "listing.html"
    article = tmp_path / "article.html"
    listing.write_bytes(b"listing")
    article.write_bytes(b"article")
    parquet = tmp_path / f"{name}.parquet"
    pl.DataFrame({
        "period": ["2024-01", "2024-03"],
        "metric": ["fx_reserves_usd_100m"] * 2,
        "value": [100.0, 101.0],
        "html_path": [str(article)] * 2,
        "html_sha256": [hashlib.sha256(article.read_bytes()).hexdigest()] * 2,
    }).write_parquet(parquet)
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / f"{name}.json").write_text(json.dumps({
        "status": "complete", "complete": True,
        "saved_releases": 2, "registered_releases": 2,
        "headline_values": 2, "distinct_periods": 2,
        "earliest_period": "2024-01", "latest_period": "2024-03",
        "failures": [], "missing_periods": [],
        "listing_receipts": [{"path": str(listing),
                              "sha256": hashlib.sha256(listing.read_bytes()).hexdigest()}],
        "parquet_sha256": hashlib.sha256(parquet.read_bytes()).hexdigest(),
    }), encoding="utf-8")

    result = audit_one(tmp_path, name)
    assert result["coverage_complete"] is False
    assert any("missing intervening months" in error for error in result["errors"])


def test_release_audit_relocates_hash_pinned_raw_receipts_without_following_live(
    tmp_path: Path,
) -> None:
    name = "cbc_fx_reserve_release_vintages"
    release = tmp_path / "release"
    raw = release / "raw" / name
    raw.mkdir(parents=True)
    listing = raw / "list.html"
    article = raw / "article.html"
    listing.write_bytes(b"published listing")
    article.write_bytes(b"published article")
    old_root = tmp_path / "live"
    old_raw = old_root / "raw" / name
    old_raw.mkdir(parents=True)
    (old_raw / "list.html").write_bytes(b"later changed listing")
    (old_raw / "article.html").write_bytes(b"later changed article")
    parquet = release / f"{name}.parquet"
    pl.DataFrame({
        "period": ["2024-08"], "metric": ["fx_reserves_usd_100m"],
        "value": [100.0], "html_path": [str(old_raw / "article.html")],
        "html_sha256": [hashlib.sha256(article.read_bytes()).hexdigest()],
    }).write_parquet(parquet)
    (release / "state").mkdir()
    state_path = release / "state" / f"{name}.json"
    state = {
        "status": "complete", "complete": True,
        "saved_releases": 1, "registered_releases": 1,
        "headline_values": 1, "failures": [], "missing_periods": [],
        "listing_receipts": [{
            "path": str(old_raw / "list.html"),
            "sha256": hashlib.sha256(listing.read_bytes()).hexdigest(),
        }],
        "parquet_sha256": hashlib.sha256(parquet.read_bytes()).hexdigest(),
    }
    state_path.write_text(json.dumps(state), encoding="utf-8")

    result = audit_one(release, name)
    assert result["integrity_ok"] is True
    assert result["coverage_complete"] is True
    assert result["verified_files"] == 3

    state["listing_receipts"][0]["path"] = str(old_raw / ".." / name / "list.html")
    state_path.write_text(json.dumps(state), encoding="utf-8")
    result = audit_one(release, name)
    assert result["integrity_ok"] is False
    assert any("parent traversal" in error for error in result["errors"])


def test_dgbas_complete_empty_period_groups_are_not_missing(tmp_path: Path) -> None:
    name = "dgbas_release_vintages"
    raw = tmp_path / "raw" / name
    raw.mkdir(parents=True)
    listing = raw / "listing.html"
    article = raw / "article.html"
    listing.write_bytes(b"listing")
    article.write_bytes(b"article")
    parquet = tmp_path / f"{name}.parquet"
    pl.DataFrame({
        "period": ["2024-08"], "metric": ["cpi_yoy_pct"],
        "release_kind": ["article"], "html_path": [str(article)],
        "html_sha256": [hashlib.sha256(article.read_bytes()).hexdigest()],
        "attachment_receipts": ["[]"],
    }).write_parquet(parquet)
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / f"{name}.json").write_text(json.dumps({
        "status": "complete", "complete": True, "saved_releases": 1,
        "registered_releases": 1, "failures": [],
        "missing_periods": {"cpi": [], "unemployment": [], "gdp": []},
        "listing_receipts": [{"path": str(listing),
                              "sha256": hashlib.sha256(listing.read_bytes()).hexdigest()}],
        "parquet_sha256": hashlib.sha256(parquet.read_bytes()).hexdigest(),
    }), encoding="utf-8")
    result = audit_one(tmp_path, name)
    assert result["integrity_ok"] is True
    assert result["coverage_complete"] is True


def test_cbc_money_audit_checks_pair_and_missing_month_claim(tmp_path: Path) -> None:
    name = "cbc_money_release_vintages"
    listing = tmp_path / "listing.html"
    article = tmp_path / "article.html"
    listing.write_bytes(b"listing")
    article.write_bytes(b"article")
    parquet = tmp_path / f"{name}.parquet"
    rows = [
        {"period": period, "metric": metric, "value_pct": 4.0,
         "published_on": period + "-24",
         "release_url": f"https://www.cbc.gov.tw/{period}",
         "html_path": str(article),
         "html_sha256": hashlib.sha256(article.read_bytes()).hexdigest()}
        for period in ("2024-01", "2024-03")
        for metric in ("m1b_yoy_pct", "m2_yoy_pct")
    ]
    pl.DataFrame(rows).write_parquet(parquet)
    (tmp_path / "state").mkdir()
    state_path = tmp_path / "state" / f"{name}.json"
    state = {
        "status": "complete", "complete": True, "saved_releases": 2,
        "registered_releases": 2, "value_releases": 2,
        "value_history_complete": True,
        "earliest_period": "2024-01", "latest_period": "2024-03",
        "missing_periods": [], "failures": [],
        "listing_receipts": [{"path": str(listing),
                              "sha256": hashlib.sha256(listing.read_bytes()).hexdigest()}],
        "parquet_sha256": hashlib.sha256(parquet.read_bytes()).hexdigest(),
    }
    state_path.write_text(json.dumps(state), encoding="utf-8")
    result = audit_one(tmp_path, name)
    assert result["integrity_ok"] is False
    assert any("missing_periods differs" in error for error in result["errors"])
    state["value_history_complete"] = False
    state["missing_periods"] = ["2024-02"]
    state_path.write_text(json.dumps(state), encoding="utf-8")
    result = audit_one(tmp_path, name)
    assert result["integrity_ok"] is True
    assert result["coverage_complete"] is True
