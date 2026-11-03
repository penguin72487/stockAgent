import json

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from scripts.audit_tw_futures_gap_kbar_windows import audit, classify_window


def bar(**overrides):
    return {"open": 40.0, "high": 41.0, "low": 39.0, "close": 40.5, "volume": 10, **overrides}


def test_double_sided_kbar_volume_and_vwap_range_are_checked():
    tick = {"entry_quote": 40.2, "entry_volume": 5.0}
    assert classify_window(tick, [bar()]) == "volume_and_vwap_range_match"
    assert classify_window(tick, [bar(volume=12)]) == "tick_bar_disagree"
    assert classify_window({**tick, "entry_quote": 42.0}, [bar()]) == "tick_bar_disagree"


def test_missing_or_ambiguous_bar_is_not_a_zero_trade():
    tick = {"entry_quote": 40.2, "entry_volume": 5.0}
    assert classify_window(tick, None) == "bar_source_unavailable"
    assert classify_window(None, []) == "neither_observed"
    assert classify_window(tick, []) == "tick_only"
    assert classify_window(None, [bar()]) == "bar_only"
    assert classify_window(tick, [bar(), bar()]) == "bar_identity_ambiguous"


def test_invalid_ohlc_and_odd_or_nonpositive_native_volume_are_not_accepted():
    for value in (bar(high=38.0), bar(volume=9), bar(volume=0), bar(low=float("nan"))):
        assert classify_window(None, [value]) == "bar_invalid"


def test_source_bound_window_audit_and_product_identity(tmp_path):
    gap = tmp_path / "gap.json"
    day = "2020-01-02"
    gap.write_text(json.dumps({"contract": "tw_futures_unknown_entry_quarantine_v1", "contract_days": [
        {"date": day, "physical_contract": "A:202001", "product": "A", "delivery_month": "202001"},
        {"date": day, "physical_contract": "B:202001", "product": "B", "delivery_month": "202001"},
    ]}))
    candidates = tmp_path / "candidates.parquet"
    pl.DataFrame([{"date": day, "physical_contract": "A:202001", "product": "A", "contract": "202001",
                   "decision_time": "08:45", "window_minutes": 1, "entry_quote": 40., "entry_volume": 5.}]
                 ).write_parquet(candidates)
    root = tmp_path / "provider"
    payload = root / "parquet" / "sample.parquet"
    payload.parent.mkdir(parents=True)
    pl.DataFrame([{"date": day, "futures_id": "A", "contract_date": "202001", "minute": "08:46:00", **bar()}]
                 ).write_parquet(payload)
    receipt = root / "receipts" / "TaiwanFuturesKBar" / "all" / (day + ".json")
    receipt.parent.mkdir(parents=True)
    receipt.write_text(json.dumps({"dataset": "TaiwanFuturesKBar", "data_id": "", "partition": day,
        "status": "complete", "parquet_path": "parquet/sample.parquet", "parquet_size_bytes": payload.stat().st_size,
        "sha256": sha256_file(payload), "rows": 1}))
    kwargs = dict(quarantine_sha=sha256_file(gap), candidates_sha=sha256_file(candidates))
    result = audit(gap, candidates, root, **kwargs)
    assert result["provider_date_states"] == {"receipt_verified": 1}
    assert {r["state"]: r["contract_days"] for r in result["counts_by_clock_and_state"] if r["clock"] == "08:45"} == {
        "neither_observed": 1, "volume_and_vwap_range_match": 1}
    pl.DataFrame([{"date": day, "physical_contract": "A:202001", "product": "B", "contract": "202001",
                   "decision_time": "08:45", "window_minutes": 1, "entry_quote": 40., "entry_volume": 5.}]
                 ).write_parquet(candidates)
    with pytest.raises(ValueError, match="product/month"):
        audit(gap, candidates, root, quarantine_sha=kwargs["quarantine_sha"], candidates_sha=sha256_file(candidates))
