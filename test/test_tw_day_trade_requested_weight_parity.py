from __future__ import annotations

import hashlib
import json

import numpy as np
import polars as pl
import pytest

from scripts.audit_tw_day_trade_requested_weight_parity import _write_new_json, audit


def _fixture(tmp_path, rows=None):
    original = tmp_path / "original.npz"
    np.savez(original, dates=np.array(["2026-01-02", "2026-01-05"], dtype="datetime64[D]"),
             carry_universe=np.array(["0050", "2330"]),
             requested_weights_history=np.array([[0.2, -0.1], [0.0, 0.3]]))
    signal = tmp_path / "signal"
    signal.mkdir()
    weights = signal / "target_weights.parquet"
    pl.DataFrame(rows or {"symbol": ["0050", "2330", "0099"],
                          "model_weight": [0.2, -0.1, 0.0],
                          "target_weight": [0.25, -0.1, 0.1]}).write_parquet(weights)
    summary = signal / "summary.json"
    summary.write_text(json.dumps({"signal_id": "s1", "simulation_only": True,
        "production_order_possible": False, "counterfactual_signal_regeneration": True,
        "counterfactual_artifact_proofs": {"weights_sha256": hashlib.sha256(weights.read_bytes()).hexdigest()}}))
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps({"status": "source_hashes_unchanged_candidate_not_yet_accepted",
        "sessions": [{"session_date": "2026-01-02", "signal_id": "s1",
                      "summary_path": str(summary), "weights_path": str(weights),
                      "summary_sha256": hashlib.sha256(summary.read_bytes()).hexdigest()}]}))
    return receipt, original, summary, weights


def test_parity_is_axis_aware_and_never_claims_fills_or_publication(tmp_path):
    receipt, original, _, _ = _fixture(tmp_path)
    result = audit(receipt, original)
    assert result["compared_sessions"] == 1
    assert result["original_sessions_without_candidate"] == ["2026-01-05"]
    assert result["comparisons"]["model_weight"]["sessions_with_numeric_differences"] == 0
    assert result["comparisons"]["target_weight"]["sessions_with_numeric_differences"] == 1
    assert result["comparisons"]["target_weight"]["sessions_with_membership_differences"] == 1
    assert result["sessions"][0]["comparisons"]["target_weight"]["candidate_nonzero_outside_original_universe"] == ["0099"]
    assert result["sessions"][0]["comparisons"]["target_weight"]["l1_weight_difference"] == pytest.approx(0.15)
    assert result["publication_authorized"] is False
    assert result["execution_or_performance_equivalence_proven"] is False


def test_changed_candidate_weights_fail_closed(tmp_path):
    receipt, original, _, weights = _fixture(tmp_path)
    weights.write_bytes(weights.read_bytes() + b"changed")
    with pytest.raises(ValueError, match="weight hash changed"):
        audit(receipt, original)


def test_duplicate_candidate_symbol_rejected(tmp_path):
    receipt, original, _, _ = _fixture(tmp_path, {"symbol": ["0050", "0050"],
        "model_weight": [0.1, 0.2], "target_weight": [0.1, 0.2]})
    with pytest.raises(ValueError, match="duplicate or invalid candidate symbol"):
        audit(receipt, original)


def test_incomplete_receipt_and_negative_epsilon_rejected(tmp_path):
    receipt, original, _, _ = _fixture(tmp_path)
    value = json.loads(receipt.read_text())
    value["status"] = "building_source_not_yet_verified"
    receipt.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="incomplete"):
        audit(receipt, original)
    with pytest.raises(ValueError, match="epsilon"):
        audit(receipt, original, epsilon=-1)


def test_receipt_writer_never_overwrites_existing_file(tmp_path):
    output = tmp_path / "private" / "audit.json"
    _write_new_json(output, {"run": 1})
    with pytest.raises(FileExistsError):
        _write_new_json(output, {"run": 2})
    assert json.loads(output.read_text()) == {"run": 1}
