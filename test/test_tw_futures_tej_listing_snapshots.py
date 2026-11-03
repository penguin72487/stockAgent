from datetime import date

import polars as pl
import pytest

from scripts.audit_tw_futures_tej_listing_snapshots import (
    TABLE_ID,
    compare_first_observation,
    snapshot_rows,
)


def sample():
    receipt = {
        "provider": "tej_smart_wizard", "table_id": TABLE_ID,
        "task_id": "task1", "observed_at_utc": "2026-10-02T18:13:46Z",
        "exported_rows": 1, "source_values_retained": True,
        "date_axis": "observed_source_snapshot_no_historical_date_claim",
        "row_grain": "native_company_current_snapshot_not_historical_observation",
    }
    header = ["Future Code", "Listed Date", "Expiration Moon", "Trade Date_End"]
    cells = ["CCF201104 contract", "2011/02/17", "2011/04", "2011/04/20"]
    raw = {"task_id": "task1", "table": "Future Attribute",
           "capture_method": "native_msaa_preview_full", "cells": [header, cells]}
    frame = pl.DataFrame({**dict(zip(header, [[value] for value in cells], strict=True)),
        "_query_symbol": ["CCF201104"], "_snapshot_observed_at_utc": [receipt["observed_at_utc"]]})
    return receipt, raw, frame


def test_vendor_grid_and_parquet_must_match_before_counting():
    receipt, raw, frame = sample()
    rows = snapshot_rows(receipt, raw, frame)
    assert rows[0]["listed"] == date(2011, 2, 17)
    corrupted = frame.with_columns(pl.lit("2011/02/16").alias("Listed Date"))
    with pytest.raises(ValueError, match="preview and parquet differ"):
        snapshot_rows(receipt, raw, corrupted)


def test_explicit_empty_vendor_response_requires_owned_dialog_and_no_rows():
    receipt, raw, frame = sample()
    empty = {"capture_method": "owned_vendor_empty_dialog",
             "source_outcome": "explicit_empty_scope",
             "empty_dialog_ownership_contract": "unique_same_thread_ownerless_modal_v1",
             "empty_acknowledgement_contract": "normal_ok_control_notification_no_foreground_v1"}
    receipt.update(empty, exported_rows=0)
    raw.update(empty)
    assert snapshot_rows(receipt, raw, frame.head(0)) == []
    receipt["empty_dialog_ownership_contract"] = "exact_owned_modal_v1"
    raw["empty_dialog_ownership_contract"] = "exact_owned_modal_v1"
    assert snapshot_rows(receipt, raw, frame.head(0)) == []
    with pytest.raises(ValueError, match="unverified empty"):
        snapshot_rows(receipt, raw, frame)
    raw["empty_dialog_ownership_contract"] = "unknown"
    with pytest.raises(ValueError, match="unverified empty"):
        snapshot_rows(receipt, raw, frame.head(0))


def test_vendor_snapshot_cannot_override_conflict_or_missing_dates():
    candidate = {"contract": "201104", "date": date(2011, 2, 17)}
    base = {"month": "2011/04", "listed": date(2011, 2, 17)}
    assert compare_first_observation(candidate, [base]) == "listed_on_first_observation"
    assert compare_first_observation(candidate, [dict(base, listed=date(2011, 2, 16))]) == "listed_before_first_observation"
    assert compare_first_observation(candidate, [dict(base, listed=date(2011, 2, 18))]) == "listed_after_first_observation_conflict"
    assert compare_first_observation(candidate, [base, dict(base, listed=date(2011, 2, 18))]) == "vendor_snapshot_conflict"
    assert compare_first_observation(candidate, [dict(base, listed=None)]) == "vendor_date_or_month_unknown"
    assert compare_first_observation(candidate, [dict(base, month="2011/05")]) == "vendor_date_or_month_unknown"
    assert compare_first_observation(candidate, []) == "vendor_symbol_not_observed"
