"""Causal futures inputs must stay explicit and checkpoint-incompatible."""
from pathlib import Path

import pytest
import yaml

from stockagent.config import load_config
from stockagent.training.checkpoint_contract import build_checkpoint_manifest
from stockagent.training.trainer import _validate_checkpoint_manifest
from test_tw_stock_context_futures_portfolio import _stock_panel


BASE = Path("configs/markets/tw_futures_v8_general.yaml").resolve()
CAUSAL = Path("configs/markets/tw_futures_v8_general_causal.yaml").resolve()
TRADABLE = Path("configs/markets/tw_futures_v8_general_tradable.yaml").resolve()


def test_causal_config_keeps_full_training_and_execution_contract():
    old = load_config(BASE)
    new = load_config(CAUSAL)
    assert new.data.tw_futures_denomination_context_basis == "prior_settlement"
    assert not new.data.tw_futures_current_open_feature
    for cfg in (new.training.financial_transformer, new.training.transformer_base_portfolio):
        assert cfg.futures_denomination_aware_output
        assert cfg.futures_feature_rms_normalization
        assert cfg.futures_denomination_hard_projection is False
    assert new.trading == old.trading
    assert new.training.epochs == old.training.epochs == 1000
    assert new.training.batch_size_train == old.training.batch_size_train
    assert new.training.early_stopping_no_improve_ratio == old.training.early_stopping_no_improve_ratio
    assert not old.training.financial_transformer.futures_feature_rms_normalization


def test_prior_capacity_experiment_changes_only_the_policy_feasibility_contract():
    old = load_config(BASE)
    new = load_config(TRADABLE)
    assert new.data.tw_futures_require_prior_capacity
    assert new.training == old.training
    assert new.trading == old.trading
    assert not new.training.financial_transformer.futures_denomination_aware_output
    assert not new.training.financial_transformer.futures_feature_rms_normalization
    manifest = build_checkpoint_manifest(_stock_panel(), new, include_data_content=False)
    baseline = build_checkpoint_manifest(_stock_panel(), old, include_data_content=False)
    contract = manifest["contracts"]["trading"]["taiwan_stock_context_futures_portfolio"]
    assert contract["cross_domain_contract_version"] == 8
    assert "positive_prior_volume_capacity" in contract["policy_mask"]
    assert manifest["fingerprints"]["model"] == baseline["fingerprints"]["model"]
    assert manifest["fingerprints"]["trading"] != baseline["fingerprints"]["trading"]


def test_prior_capacity_cannot_resume_the_old_action_universe(tmp_path):
    old = build_checkpoint_manifest(_stock_panel(), load_config(BASE), include_data_content=False)
    new = build_checkpoint_manifest(_stock_panel(), load_config(TRADABLE), include_data_content=False)
    with pytest.raises(RuntimeError, match="semantic fingerprint mismatch"):
        _validate_checkpoint_manifest({"experiment_manifest": old}, new,
            checkpoint_path=tmp_path / "old_universe.pt", scope="resume")
    _validate_checkpoint_manifest({"experiment_manifest": new}, new,
        checkpoint_path=tmp_path / "prior_capacity.pt", scope="resume")


def test_causal_inputs_change_manifest_and_preserve_executor_clock():
    config = load_config(CAUSAL)
    new = build_checkpoint_manifest(_stock_panel(), config, include_data_content=False)
    old = build_checkpoint_manifest(_stock_panel(), load_config(BASE), include_data_content=False)
    detail = new["contracts"]["trading"]["taiwan_stock_context_futures_portfolio"]
    assert detail["cross_domain_contract_version"] == 7
    assert "prior" in detail["denomination_clock"]
    assert detail["candidate_feature_columns"][-1] == "prior_one_contract_cash_requirement_twd"
    assert detail["integer_training_forward"] == "exact_integer_account_v3"
    assert new["fingerprints"]["model"] != old["fingerprints"]["model"]
    assert new["fingerprints"]["trading"] != old["fingerprints"]["trading"]


@pytest.mark.parametrize("field,value", [
    ("causal_feature_min_active_dates", 31),
    ("causal_feature_scale_epsilon", 0.003),
])
def test_futures_only_rms_retains_shared_fit_parameters_in_fingerprint(field, value):
    config = load_config(CAUSAL)
    model = config.training.financial_transformer
    model.causal_feature_rms_normalization = False
    before = build_checkpoint_manifest(_stock_panel(), config, include_data_content=False)
    setattr(model, field, value)
    after = build_checkpoint_manifest(_stock_panel(), config, include_data_content=False)
    assert before["fingerprints"]["model"] != after["fingerprints"]["model"]


@pytest.mark.parametrize("override", [
    {"data": {"tw_futures_current_open_feature": True}},
    {"data": {"tw_futures_denomination_context_basis": "current_open"}},
    {"trading": {"tw_futures_portfolio_integer_contracts": False}},
])
def test_unsafe_causal_combinations_fail_before_training(tmp_path, override):
    path = tmp_path / "invalid.yaml"
    path.write_text(yaml.safe_dump({"base_config": str(CAUSAL), **override}))
    with pytest.raises(ValueError):
        load_config(path)
