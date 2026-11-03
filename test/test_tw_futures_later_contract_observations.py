from datetime import date
import json

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from scripts.audit_tw_futures_later_contract_observations import audit, match_later_observations


def _row(day, physical, *, product="AA", delivery="202601", volume=1, observed=True,
         expiry=None, boundary=None, candidate=True):
    return {"date": date.fromisoformat(day), "physical_contract": physical,
            "product": product, "contract": delivery, "asset_class": "stock_future",
            "official_expiry": expiry, "source_row_observed": observed,
            "official_row_observed": observed, "official_source_sha256": "a" * 64,
            "outright_volume": volume, "accounting_only_date": False,
            "corporate_identity_candidate_only": candidate, "corporate_boundary": boundary,
            "lifetime_status": "candidate"}


def test_later_trade_is_strict_and_does_not_cross_physical_identity():
    source = pl.DataFrame([
        _row("2026-01-02", "AA:202601#g0", boundary=date(2026, 1, 6)),
        _row("2026-01-03", "AA:202601#g1"),  # different generation
        _row("2026-01-04", "AA:202601#g0", volume=0),
        _row("2026-01-05", "AA:202601#g0", observed=False),
        _row("2026-01-06", "AA:202601#g0"),
    ])
    gap = pl.DataFrame([{"date": date(2026, 1, 2), "physical_contract": "AA:202601#g0",
                         "product": "AA", "delivery_month": "202601"}])
    row = match_later_observations(gap, source).row(0, named=True)
    assert row["later_date"] == date(2026, 1, 6)
    assert row["later_observed"] and not row["later_before_boundary"]
    assert row["identity_candidate_only"] and row["expiry_missing"]


def test_missing_later_trade_and_mismatched_quarantine_identity():
    source = pl.DataFrame([_row("2026-01-02", "AA:202601#g0")])
    gap = pl.DataFrame([{"date": date(2026, 1, 2), "physical_contract": "AA:202601#g0",
                         "product": "AA", "delivery_month": "202601"}])
    row = match_later_observations(gap, source).row(0, named=True)
    assert row["later_date"] is None and not row["later_observed"]
    with pytest.raises(ValueError, match="identity"):
        match_later_observations(gap.with_columns(pl.lit("BB").alias("product")), source)
    with pytest.raises(ValueError, match="identity"):
        match_later_observations(gap.with_columns(pl.lit(None, dtype=pl.String).alias("delivery_month")), source)


def test_duplicate_source_days_are_rejected():
    row = _row("2026-01-02", "AA:202601#g0")
    gap = pl.DataFrame([{"date": date(2026, 1, 2), "physical_contract": "AA:202601#g0",
                         "product": "AA", "delivery_month": "202601"}])
    with pytest.raises(ValueError, match="duplicate"):
        match_later_observations(gap, pl.DataFrame([row, row]))


def test_source_pins_and_report_are_serializable(tmp_path):
    daily = tmp_path / "daily.parquet"
    pl.DataFrame([_row("2026-01-02", "AA:202601#g0"),
                  _row("2026-01-03", "AA:202601#g0")]).write_parquet(daily)
    quarantine = tmp_path / "quarantine.json"
    quarantine.write_text(json.dumps({"contract": "tw_futures_unknown_entry_quarantine_v1",
        "source_daily_sha256": sha256_file(daily), "contract_days": [{"date": "2026-01-02",
        "physical_contract": "AA:202601#g0", "product": "AA", "delivery_month": "202601"}]}))
    report = audit(quarantine, daily, quarantine_sha=sha256_file(quarantine))
    assert report["later_observed"] == 1
    assert report["examples"][0]["later_date"] == "2026-01-03"
    assert report["training_ready"] is False
    json.dumps(report)
    with pytest.raises(ValueError, match="quarantine source"):
        audit(quarantine, daily, quarantine_sha="wrong")
