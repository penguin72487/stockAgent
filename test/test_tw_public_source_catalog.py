from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from scripts.audit_tw_public_source_catalog import inventory


def test_catalog_keeps_presence_distinct_from_coverage(tmp_path: Path) -> None:
    dataset = "tpex_margin_balance"
    pl.DataFrame({"date": ["2007-05-31"]}).write_parquet(
        tmp_path / f"{dataset}.parquet"
    )
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / f"{dataset}.json").write_text(
        json.dumps({
            "coverage_complete": True,
            "missing_dates_after": 0,
            "confirmed_source_unavailable_dates": ["2007-06-01"],
        }),
        encoding="utf-8",
    )

    rows = inventory(tmp_path)
    row = next(item for item in rows if item["dataset"] == dataset)
    assert row["present"] is True
    assert row["rows"] == 1
    assert row["state_coverage_complete"] is True
    assert row["state_source_unavailable_dates"] == 1
    assert row["historical_completeness"] == "requires_receipt_and_calendar_audit"
    assert any(not item["present"] for item in rows)
    fund = next(item for item in rows if item["dataset"] == "sitca_domestic_fund_basic")
    assert fund["history_mode"] == "current_snapshot_archive_from_first_capture"
    assert fund["historical_completeness"] == "not_a_historical_archive"


def test_catalog_registers_original_release_archives_separately(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    name = "cbc_fx_reserve_release_vintages"
    pl.DataFrame({"period": ["2024-08"]}).write_parquet(tmp_path / f"{name}.parquet")
    (state_dir / f"{name}.json").write_text(
        json.dumps({"complete": False, "missing_periods": ["2024-07"],
                    "failures": [{"release_url": "https://www.cbc.gov.tw/tw/cp-302-1.html"}]}),
        encoding="utf-8",
    )
    row = next(item for item in inventory(tmp_path) if item["dataset"] == name)
    assert row["history_mode"] == "dated_original_release_archive"
    assert row["present"] is True
    assert row["state_coverage_complete"] is False
    assert row["state_missing_dates_after"] == 1
    assert row["state_failed_releases"] == 1
    assert row["state_source_unavailable_dates"] == 0
