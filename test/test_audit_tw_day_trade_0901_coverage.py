import json
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from scripts import audit_tw_day_trade_0901_coverage as coverage


def _write_json(path, payload):
    path.write_text(json.dumps(payload), encoding="utf-8")


@pytest.fixture
def candidate(tmp_path):
    signals = tmp_path / "signals"
    signals.mkdir()
    weights = signals / "target_weights.parquet"
    pl.DataFrame({"symbol": ["0050", "1101", "2330", "9999"],
                  "target_weight": [0.1, -0.2, 0.3, 0.0]}).write_parquet(weights)
    summary = signals / "summary.json"
    identity = {"checkpoint_sha256": "ab" * 32}
    _write_json(summary, {
        "counterfactual_official_open_session": "2026-01-02",
        "counterfactual_artifact_proofs": {
            "recipe_identity": identity, "weights_sha256": coverage._sha256(weights),
        },
    })
    receipt = tmp_path / "receipt.json"
    _write_json(receipt, {
        "status": "source_hashes_unchanged_candidate_not_yet_accepted",
        "candidate_accepted": False, "simulation_only": True,
        "production_order_possible": False, "market": "model", "recipe_identity": identity,
        "sessions": [{"session_date": "2026-01-02", "summary_path": str(summary),
                      "summary_sha256": coverage._sha256(summary), "weights_path": str(weights)}],
    })
    source_audit = tmp_path / "audit.json"
    _write_json(source_audit, {
        "contract": "isolated_counterfactual_signals_source_audit_v1", "status": "source_gaps_remain",
        "market": "model", "receipt_sha256": coverage._sha256(receipt),
        "sessions": 1, "start_date": "2026-01-02", "end_date": "2026-01-02",
        "nonzero_targets": 3, "ledger_modified": False, "publication_authorized": False,
    })
    return {"receipt_path": receipt, "source_audit_path": source_audit,
            "signal_root": signals, "minute_roots": (tmp_path,), "year": 2026}


def test_classifies_local_observations_without_assuming_fills_or_zero_trades(candidate, monkeypatch):
    def local(**kwargs):
        assert kwargs["symbols"] == ["0050", "1101", "2330"]
        return {
            "0050": {"observed_volume_known_0901": True, "tick_volume_units_0901": 1000,
                     "execution_price_0901": 60.0},
            "1101": {"observed_volume_known_0901": False, "tick_volume_units_0901": 0.0,
                     "execution_price_0901": 50.0},
        }, {"error_counts": {"SourceError": 1}, "source_counts": {"local": 2}}
    monkeypatch.setattr(coverage, "_local_0901_vwap_rows", local)
    result = coverage.audit(**candidate)
    assert result["totals"] == {"nonzero_targets": 3, "observed_positive_volume_candidates": 1,
        "missing_or_nonpositive_volume_candidates": 1, "no_local_price_observation": 1}
    assert result["sessions_detail"][0]["observed_positive_volume_candidates"] == ["0050"]
    assert {x["symbol"] for x in result["unresolved_target_days"]} == {"1101", "2330"}
    assert result["missing_means_no_trade"] is False
    assert result["positive_volume_proves_fill"] is False
    assert result["source_bytes_independently_verified"] is False
    assert result["publication_authorized"] is False
    assert result["ledger_modified"] is False


def test_rejects_changed_weights(candidate):
    path = candidate["signal_root"] / "target_weights.parquet"
    pl.DataFrame({"symbol": ["0050"], "target_weight": [0.5]}).write_parquet(path)
    with pytest.raises(ValueError, match="weights or counterfactual identity"):
        coverage.audit(**candidate)


def test_rejects_wrong_year(candidate):
    candidate["year"] = 2025
    with pytest.raises(ValueError, match="session calendar"):
        coverage.audit(**candidate)


def test_rejects_unbound_source_audit(candidate):
    path = candidate["source_audit_path"]
    audit = json.loads(path.read_text())
    audit["market"] = "other"
    _write_json(path, audit)
    with pytest.raises(ValueError, match="private source audit"):
        coverage.audit(**candidate)


def test_rejects_source_audit_mutation_during_scan(candidate, monkeypatch):
    def local(**kwargs):
        path = candidate["source_audit_path"]
        path.write_text(path.read_text() + " ", encoding="utf-8")
        return {}, {}
    monkeypatch.setattr(coverage, "_local_0901_vwap_rows", local)
    with pytest.raises(ValueError, match="changed during local coverage scan"):
        coverage.audit(**candidate)


def test_rejects_weights_outside_candidate_root(candidate):
    row = {"summary_path": str(candidate["signal_root"] / "summary.json"),
           "summary_sha256": coverage._sha256(candidate["signal_root"] / "summary.json"),
           "weights_path": str(candidate["receipt_path"]), "session_date": "2026-01-02"}
    with pytest.raises(ValueError, match="outside isolated root"):
        coverage._load_targets(row, signal_root=candidate["signal_root"], identity={})


@pytest.mark.parametrize("minute,expected", [
    (0, "opening_bar_requires_execution_validation"),
    (2, "source_has_no_0900_left_label_bar"),
])
def test_finlab_local_minute_is_hash_bound_and_left_labelled(tmp_path, minute, expected):
    from scripts.download_finlab_intraday import _partition_paths

    root = tmp_path / "finlab"
    objects = root / "intraday" / "objects"
    objects.mkdir(parents=True)
    obj = objects / "bar.parquet"
    pl.DataFrame({"stock_id": ["0050"], "trade_date": ["2026-01-02"],
        "timestamp": [datetime(2026, 1, 2, 9, minute, tzinfo=ZoneInfo("Asia/Taipei"))],
        "session": ["regular"], "open": [10.0], "high": [11.0], "low": [9.0],
        "close": [10.0], "volume": [1.0], "volume_shares": [1000]}).write_parquet(obj)
    day = date(2026, 1, 2)
    receipt_path = _partition_paths(root, "tw_minute:0050", day)[0]
    receipt_path.parent.mkdir(parents=True)
    _write_json(receipt_path, {"dataset": "tw_minute:0050", "trade_date": str(day),
        "status": "downloaded_unverified_for_pit", "source_grain": "stock_trade_day_left_labelled_minute",
        "canonical_volume_unit": "shares", "parquet_path": str(obj.relative_to(root)),
        "parquet_size_bytes": obj.stat().st_size, "sha256": coverage._sha256(obj)})
    result = coverage._finlab_observation(root, symbol="0050", day=day)
    assert result["status"] == expected
    assert result["exchange_no_trade_proven"] is False
    assert result["object_sha256"] == coverage._sha256(obj)
    if minute == 0:
        assert result["positive_volume_candidate"] is True
    # A modified object is no longer an accepted stored receipt.
    with obj.open("ab") as handle:
        handle.write(b"changed")
    assert coverage._finlab_observation(root, symbol="0050", day=day)["status"] == (
        "no_verified_local_finlab_receipt")


def test_priority_plan_contains_only_exact_unique_missing_native_receipts(tmp_path):
    result = {"contract": "counterfactual_local_0901_coverage_triage_v1",
        "unresolved_target_days": [
            {"symbol": "0050", "session_date": "2026-01-02", "finlab": {"status": "no_verified_local_finlab_receipt"}},
            {"symbol": "0050", "session_date": "2026-01-02", "finlab": {"status": "no_verified_local_finlab_receipt"}},
            {"symbol": "2330", "session_date": "2026-01-02", "finlab": {"status": "source_has_no_0900_left_label_bar"}},
        ]}
    path = tmp_path / "plan.json"
    assert coverage.write_finlab_priority_plan(result=result, output=path) == 1
    plan = json.loads(path.read_text())
    assert plan["items"] == [{"dataset": "tw_minute:0050", "partition": "2026-01-02"}]
    assert plan["provider"] == "finlab"
