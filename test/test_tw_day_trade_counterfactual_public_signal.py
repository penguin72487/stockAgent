from datetime import datetime
import hashlib
import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from scripts import build_tw_day_trade_counterfactual_public_signal as public
from stockagent.live.tw_day_trade_completed_replay import verify_completed_open_sources
from test_tw_day_trade_completed_replay import DAY, _sources


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write(path, payload):
    path.write_text(json.dumps(payload, ensure_ascii=False))


@pytest.fixture
def candidate(tmp_path, monkeypatch):
    now = datetime(2026, 11, 9, 15, tzinfo=ZoneInfo("Asia/Taipei"))
    raw, prices = _sources(tmp_path)
    checkpoint = tmp_path / "checkpoint_best.pt"
    checkpoint.write_bytes(b"selected model")
    config = tmp_path / "candidate.yaml"
    config.write_text("fixture")
    monkeypatch.setattr(public, "load_market_config", lambda _: SimpleNamespace(
        enabled=False, day_trade_simulation_enabled=True, checkpoint_path=str(checkpoint),
        day_trade_rule_data_dir=str(tmp_path), market="2m"))
    proof = verify_completed_open_sources(session=DAY, observed_at=now, prices_csv=prices, raw_root=raw)
    weights = tmp_path / "target_weights.parquet"
    pl.DataFrame({"symbol": ["2330", "6488", "1101"], "name": ["甲", "乙", "丙"],
        "target_weight": [0.1, -0.2, 0.0], "tradable": [True, True, False],
        "can_buy": [True, True, False], "can_sell": [True, True, False]}).write_parquet(weights)
    signal_id = f"2m-counterfactual-official-open-{DAY}"
    summary = tmp_path / "summary.json"
    _write(summary, {"generated_at": "2026-11-09T14:45:00+08:00", "market": "2m",
        "signal_id": signal_id, "simulation_only": True, "production_order_possible": False,
        "counterfactual_signal_regeneration": True, "live_session_open_feature_applied": True,
        "replay_effective_signal_at": "2026-11-09T09:00:00+08:00", "checkpoint_path": str(checkpoint),
        "checkpoint_fingerprint": "af0ff5dd49c1", "counterfactual_open_provenance": {
            "completed_same_day_source_evidence": proof}, "counterfactual_artifact_proofs": {
            "weights_sha256": _sha(weights), "recipe_identity": {"checkpoint_sha256": _sha(checkpoint)}}})
    artifact = {"market": "2m", "signal_id": signal_id, "session_date": str(DAY)}
    for name, path in (("summary", summary), ("weights", weights), ("open_input", prices)):
        artifact[f"{name}_path"], artifact[f"{name}_sha256"] = str(path), _sha(path)
    preflight = tmp_path / "preflight.json"
    _write(preflight, {"contract": "exact_selected_signal_preflight_v1", "status": "accepted_sources_only",
        "simulation_only": True, "production_order_possible": False, "execution_or_minute_coverage_accepted": False,
        "expected_signals": 1, "accepted_signals": 1, "failures": [], "artifacts": [artifact]})
    receipt = tmp_path / "receipt.json"
    _write(receipt, {"status": "source_hashes_unchanged_candidate_not_yet_accepted", "market": "2m",
        "simulation_only": True, "production_order_possible": False, "sessions": [{
            "session_date": str(DAY), "summary_sha256": _sha(summary), "signal_id": signal_id}]})
    args = {"candidate_config": config, "preflight": preflight, "backfill_receipt": receipt,
            "checkpoint_sha256": _sha(checkpoint), "now": now}
    return args, weights, raw, summary


def test_projection_is_ordered_and_never_exposes_live_or_private_data(candidate, tmp_path):
    args, _, _, _ = candidate
    result = public.build(**args)
    assert result["counterfactual"] is True and result["live_signal"] is False
    assert result["broker_fill"] is False and result["execution_and_minute_coverage_verified"] is False
    assert result["signal_count"] == 2
    assert [row["symbol"] for row in result["signals"]] == ["6488", "2330"]
    assert str(tmp_path) not in json.dumps(result)
    assert "open_price" not in result["signals"][0]


def test_projection_rejects_mutated_weights_and_sources(candidate):
    args, weights, _, _ = candidate
    weights.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="preflight source changed"):
        public.build(**args)


def test_projection_rejects_a_future_publication(candidate):
    args, _, _, _ = candidate
    early = args["now"].replace(hour=14, minute=40)
    with pytest.raises(ValueError, match="generation clock"):
        public.build(**(args | {"now": early}))


def test_projection_rechecks_original_official_data(candidate):
    args, _, raw, _ = candidate
    path = raw / "twse_daily_ohlcv" / f"{DAY}.json"
    path.write_text(path.read_text() + " ")
    with pytest.raises(ValueError, match="retained bytes are unavailable"):
        public.build(**args)


def test_projection_recovers_only_byte_identical_retained_official_source(candidate):
    args, _, raw, _ = candidate
    original = raw / "twse_daily_ohlcv" / f"{DAY}.json"
    digest = _sha(original)
    retained = raw / ".close_evidence" / digest / "twse_daily_ohlcv" / f"{DAY}.json"
    retained.parent.mkdir(parents=True)
    retained.write_bytes(original.read_bytes())
    original.write_text(original.read_text() + " ")
    assert public.build(**args)["signal_count"] == 2
    retained.write_text(retained.read_text() + " ")
    with pytest.raises(ValueError, match="retained bytes are unavailable"):
        public.build(**args)
