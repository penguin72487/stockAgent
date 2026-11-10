from datetime import date
import json

import polars as pl
import pytest

from scripts.audit_tw_day_trade_counterfactual_signals import audit, write_limit_scopes
from scripts.backfill_tw_day_trade_open_signals import _sha256


def _candidate(tmp_path, *, missing_limit=False):
    day = "2026-01-02"
    source = tmp_path / "snapshot"
    source.mkdir()
    twse, tpex, features = (source / name for name in ("twse.parquet", "tpex.parquet", "features.parquet"))
    pl.DataFrame({"date": [day], "證券代號": ["0050"], "開盤價": ["100"],
                  "最高價": ["101"], "最低價": ["99"], "收盤價": ["100"],
                  "成交股數": ["1000"]}).write_parquet(twse)
    pl.DataFrame({"date": ["2026-01-05"], "代號": ["6488"], "開盤": ["900"],
                  "最高": ["900"], "最低": ["900"], "收盤": ["900"]}).write_parquet(tpex)
    features.write_bytes(b"feature-source")
    snapshot = {"status": "verified", "source_publication_claim": False, "sources": {}}
    for name, path in (("twse", twse), ("tpex", tpex), ("features", features)):
        snapshot["sources"][name] = {"retained": str(path), "sha256": _sha256(path), "bytes": path.stat().st_size}
    snapshot_path = source / "receipt.json"
    snapshot_path.write_text(json.dumps(snapshot))
    limit_root = tmp_path / "limits"
    limit_root.mkdir()
    limits_path = limit_root / f"{day}.parquet"
    symbol = "OTHER" if missing_limit else "0050"
    pl.DataFrame({"symbol": [symbol], "reference_price": [100.], "upper_limit_price": [110.],
                  "lower_limit_price": [90.]}).write_parquet(limits_path)
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    input_path = input_root / "open.csv"
    pl.DataFrame({"symbol": ["0050"], "price": [100.], "open_price": [100.],
                  "reference_price": [None if missing_limit else 100.],
                  "upper_limit_price": [None if missing_limit else 110.],
                  "lower_limit_price": [None if missing_limit else 90.]}).write_csv(input_path)
    signal_root = tmp_path / "signals"
    signal = signal_root / day / "id"
    signal.mkdir(parents=True)
    weights = signal / "target_weights.parquet"
    pl.DataFrame({"symbol": ["0050"], "target_weight": [0.5], "open_price": [100.]}).write_parquet(weights)
    identity = {"checkpoint_sha256": "fixture"}
    provenance = {"input_path": str(input_path), "input_sha256": _sha256(input_path),
                  "price_limit_path": str(limits_path), "price_limit_sha256": _sha256(limits_path),
                  "feature_cutoff_date": "2025-12-31", "missing_price_limit_rows": int(missing_limit)}
    summary = {"signal_id": "id", "generated_at": "2026-11-09T04:00:00+08:00",
               "simulation_only": True, "production_order_possible": False,
               "counterfactual_signal_regeneration": True, "replay_effective_signal_at": day + "T09:00:00+08:00",
               "counterfactual_open_clock_known": False, "counterfactual_historical_revision_history_proven": False,
               "opening_live_source_coverage_proven": False, "price_timestamp": None,
               "counterfactual_open_provenance": provenance,
               "counterfactual_artifact_proofs": {"recipe_identity": identity, "weights_sha256": _sha256(weights)}}
    summary_path = signal / "summary.json"
    summary_path.write_text(json.dumps(summary))
    receipt = {"status": "source_hashes_unchanged_candidate_not_yet_accepted", "simulation_only": True,
               "production_order_possible": False, "candidate_accepted": False, "market": "test",
               "recipe_identity": identity, "immutable_source_snapshot_receipt": str(snapshot_path),
               "source_hashes": {"twse_daily_ohlcv_sha256": _sha256(twse),
                                 "tpex_daily_ohlcv_sha256": _sha256(tpex), "feature_path_sha256": _sha256(features)},
               "sessions": [{"session_date": day, "previous_panel_date": "2025-12-31", "signal_id": "id",
                             "summary_path": str(summary_path), "summary_sha256": _sha256(summary_path),
                             "weights_path": str(weights)}]}
    receipt_path = input_root / "batch.json"
    receipt_path.write_text(json.dumps(receipt))
    return {"receipt_path": receipt_path, "signal_root": signal_root, "input_root": input_root,
            "expected_sessions": [date.fromisoformat(day)], "limit_root": limit_root}, weights


def test_audit_binds_original_sources_and_never_claims_deployment(tmp_path):
    args, _ = _candidate(tmp_path)
    result = audit(**args)
    assert result["status"] == "source_inputs_audited_not_accepted"
    assert result["nonzero_targets"] == 1
    assert not result["targets_without_valid_limits"]
    assert result["historical_first_print_time_proven"] is False
    assert result["historical_revision_sequence_proven"] is False
    assert result["publication_authorized"] is False


def test_audit_keeps_missing_limit_as_gap_not_zero_trade(tmp_path):
    args, _ = _candidate(tmp_path, missing_limit=True)
    result = audit(**args)
    assert result["status"] == "source_gaps_remain"
    assert result["targets_without_valid_limits"] == [{"date": "2026-01-02", "symbol": "0050"}]
    audit_path = args["input_root"] / "audit.json"
    audit_path.write_text(json.dumps(result))
    paths = write_limit_scopes(result=result, audit_path=audit_path, directory=args["input_root"] / "scopes")
    scope = json.loads(paths[0].read_text())
    assert scope["minute_symbols"] == ["0050"]
    assert scope["signal_source_audit_sha256"] == _sha256(audit_path)
    assert scope["publication_authorized"] is False
    with pytest.raises(ValueError, match="must be new"):
        write_limit_scopes(result=result, audit_path=audit_path, directory=args["input_root"] / "scopes")


def test_audit_rejects_mutated_weights_and_wrong_calendar(tmp_path):
    args, weights = _candidate(tmp_path)
    with pytest.raises(ValueError, match="official completed calendar"):
        audit(**{**args, "expected_sessions": [date(2026, 1, 5)]})
    weights.write_bytes(b"mutated")
    with pytest.raises(ValueError, match="exact artifact proof"):
        audit(**args)
