from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace

import pytest

from stockagent.live.feature_source_contract import (
    LiveFeatureContractError,
    inspect_live_feature_source,
    require_live_feature_source,
)


def test_ordinary_live_modes_do_not_read_a_factorized_manifest(monkeypatch):
    from pathlib import Path

    monkeypatch.setattr(Path, "read_bytes", lambda _path: pytest.fail("ordinary modes must not read factorized sources"))
    data = SimpleNamespace(factorized_feature_manifest="")
    assert inspect_live_feature_source(data) == {"required": False, "ready": True, "blockers": []}
    require_live_feature_source(data)


def test_missing_factorized_manifest_is_not_a_six_feature_fallback(tmp_path):
    data = SimpleNamespace(factorized_feature_manifest=str(tmp_path / "missing.json"), feature_include=["open_raw", "close_raw"])
    with pytest.raises(LiveFeatureContractError) as raised:
        require_live_feature_source(data)
    codes = {row["code"] for row in raised.value.evidence["blockers"]}
    assert codes == {"factorized_manifest_missing"}
    assert raised.value.evidence["base_features"] == 2


def test_research_source_retains_restrictions_and_original_values(tmp_path):
    manifest = {
        "contract": "tw_factorized_nullable_panel_values_lag1_v2", "status": "complete",
        "research_only": True, "historical_point_in_time": False, "live_eligible": False,
        "use_restriction": "owner_private_noncommercial_vastai1T_training_no_public_redistribution",
        "logical_model_channels": 14726, "dates": ["2014-01-06", "2026-10-02"],
    }
    path = tmp_path / "factorized_manifest.json"
    path.write_text(json.dumps(manifest))
    before = path.read_bytes()
    evidence = inspect_live_feature_source(SimpleNamespace(factorized_feature_manifest=str(path)))
    assert evidence["manifest_sha256"] == hashlib.sha256(before).hexdigest()
    assert path.read_bytes() == before
    assert evidence["last_date"] == "2026-10-02"
    assert evidence["logical_model_channels"] == 14726
    assert evidence["member_hashes_verified"] is False
    assert evidence["checkpoint_input_parity_proven"] is False
    assert {row["code"] for row in evidence["blockers"]} == {
        "factorized_paper_projection_unverified",
    }
    assert evidence["raw_source_publication_requested"] is False
    assert {row["code"] for row in evidence["notices"]} == {
        "factorized_research_not_live_pit", "factorized_private_raw_source",
    }


@pytest.mark.parametrize("raw", ["invalid", "[]", "null", "1", "\u00ff"])
def test_invalid_manifest_is_a_visible_contract_failure(tmp_path, raw):
    path = tmp_path / "factorized_manifest.json"
    path.write_text(raw)
    with pytest.raises(LiveFeatureContractError) as raised:
        require_live_feature_source(SimpleNamespace(factorized_feature_manifest=str(path)))
    assert "factorized_manifest_invalid" in {row["code"] for row in raised.value.evidence["blockers"]}


def test_changing_admission_flags_alone_does_not_supply_the_live_adapter(tmp_path):
    path = tmp_path / "factorized_manifest.json"
    path.write_text(json.dumps({
        "contract": "tw_factorized_nullable_panel_values_lag1_v2", "status": "complete",
        "live_eligible": True, "research_only": False, "historical_point_in_time": True,
    }))
    with pytest.raises(LiveFeatureContractError) as raised:
        require_live_feature_source(SimpleNamespace(factorized_feature_manifest=str(path)))
    assert raised.value.evidence["blockers"][0]["code"] == "factorized_paper_projection_unverified"


def test_live_panel_refuses_before_cache_or_source_guard(monkeypatch, tmp_path):
    from stockagent.live import signal_engine

    monkeypatch.setattr(signal_engine, "live_tw_public_source_read_guard", lambda _root: pytest.fail("must fail before acquiring the live source lock"))
    monkeypatch.setattr(signal_engine, "_build_panel_from_stable_source", lambda *_args, **_kwargs: pytest.fail("must never build a six-column panel"))
    config = SimpleNamespace(data=SimpleNamespace(factorized_feature_manifest=str(tmp_path / "missing.json")))
    with pytest.raises(LiveFeatureContractError):
        signal_engine._build_panel(config, live_tail=True)


@pytest.mark.parametrize("prefetch", [False, True])
def test_signal_and_prefetch_refuse_before_runtime_or_quote_io(monkeypatch, tmp_path, prefetch):
    from stockagent.live import signal_engine

    config = SimpleNamespace(data=SimpleNamespace(factorized_feature_manifest=str(tmp_path / "missing.json")))
    monkeypatch.setattr(signal_engine, "load_config", lambda _path: config)
    monkeypatch.setattr(signal_engine, "configure_inference_runtime", lambda _config: pytest.fail("must not touch model/runtime globals"))
    monkeypatch.setattr(signal_engine, "_resolve_checkpoint", lambda *_args: pytest.fail("must fail before loading a checkpoint or quotes"))
    with pytest.raises(LiveFeatureContractError):
        if prefetch:
            signal_engine.prefetch_live_signal_prices(config_path="unused")
        else:
            signal_engine.generate_live_signal(config_path="unused", _panel_override=object())


def test_independent_fold11_candidate_is_exact_manual_paper_mode():
    from stockagent.config import load_config
    from stockagent.live.market_config import load_market_config

    candidate = load_market_config("services/discord_bot/markets/tw_day_trade_factorized_scale_separated_cash.yaml")
    expected_root = "artifacts/markets/tw_day_trade_factorized_values_20261006_no_basis_flat_bf16_tf32_b128_scale_separated_cash_v1/training-bf16"
    assert candidate.market == "tw_day_trade_factorized_scale_separated_cash"
    assert candidate.day_trade_simulation_enabled is True
    assert candidate.day_trade_strict_intraday is True
    assert candidate.day_trade_terminal_liquidation_unlimited_capacity is True
    assert candidate.model_auto_deploy is False
    assert candidate.output_dir == expected_root
    assert candidate.fold_id == 11
    assert candidate.checkpoint_path == expected_root + "/fold_11/checkpoint_best.pt"
    assert candidate.model_scoped_live_output is True
    config = load_config(candidate.config_path)
    assert config.training.financial_transformer.portfolio_output_mode == "score_entmax_scale_separated_cash"
    assert config.training.financial_transformer.temporal_basis_families == []
    assert config.training.financial_transformer.feature_svd_components == 0
    assert config.training.financial_transformer.feature_bottleneck_dim == 0
    assert config.data.factorized_feature_manifest


def test_private_signal_feature_policy_is_cached_and_shown_for_every_row(tmp_path, monkeypatch):
    from stockagent.live import tw_day_trade_dashboard as dashboard

    path = tmp_path / "summary.json"
    path.write_text(json.dumps({"source_publication_contract": {"feature_proxy_publication_allowed": False},
                               "model_explanation": {"all_feature_drivers": [{"feature": "private", "value": 123}]}}))
    dashboard._SIGNAL_FEATURE_SUMMARY_CACHE.clear()
    assert dashboard._read_signal_feature_drivers(path) == []
    monkeypatch.setattr(dashboard, "_object", lambda _path: pytest.fail("policy read must reuse the source-signature cache"))
    record = dashboard._read_signal_feature_record(path)
    assert record == {"drivers": [], "feature_proxy_publication_allowed": False,
                      "publication_reason": "model_derived_signals_only"}
    rows = [{"signal_id": "model-signal", "market": "new", "session_date": "2026-02-25",
             "symbol": symbol, "signal_source_path": str(path)} for symbol in ("2330", "2317")]
    records = dashboard._lookup_signal_feature_drivers(tmp_path, {}, rows)
    assert records["model-signal"] == record
    assert all(records[dashboard._signal_row_key(row)] == record for row in rows)


def test_research_caveat_is_retained_without_publishing_feature_values(tmp_path):
    from stockagent.live import tw_day_trade_dashboard as dashboard

    path = tmp_path / "summary.json"
    path.write_text(json.dumps({"source_publication_contract": {
        "feature_proxy_publication_allowed": False, "research_only": True,
        "historical_point_in_time_proven": False},
        "model_explanation": {"all_feature_drivers": [{"feature": "private", "value": 123}]}}))
    record = dashboard._read_signal_feature_record(path)
    assert record["drivers"] == []
    assert record["research_only"] is True
    assert record["historical_point_in_time_proven"] is False
