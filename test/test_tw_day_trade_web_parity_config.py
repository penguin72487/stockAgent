"""Deployment recipes must share semantics, not only similar YAML labels."""
from dataclasses import asdict, replace
from pathlib import Path

import pytest
import yaml

from stockagent.config import load_config
from stockagent.training.checkpoint_contract import (
    _active_model_config,
    _trading_checkpoint_contract,
)


ROOT = Path(__file__).resolve().parents[1]
TRAIN = ROOT / 'configs/deployments/tw_day_trade_v8_web_parity_v7.yaml'
INFER = ROOT / 'configs/deployments/tw_day_trade_v8_web_parity_v7_inference.yaml'


def test_training_and_web_recipes_have_identical_model_and_execution_contracts():
    train, infer = load_config(TRAIN), load_config(INFER)
    assert _active_model_config(train) == _active_model_config(infer)
    assert asdict(train.trading) == asdict(infer.trading)
    assert _trading_checkpoint_contract(train) == _trading_checkpoint_contract(infer)
    assert train.training.lookback == infer.training.lookback
    assert train.training.batch_size_train == 32
    assert not train.training.day_trade_sparse_events
    assert not train.training.day_trade_training_annual_episodes
    assert not train.training.day_trade_sub_lot_recovery
    assert train.runner.output_dir == infer.runner.output_dir
    assert str(train.runner.output_dir) == 'artifacts/markets/tw_day_trade_v8_web_parity_v7'


def test_new_entry_policy_is_fingerprinted_without_changing_legacy_contract():
    train = load_config(TRAIN)
    legacy = replace(train, trading=replace(train.trading,
        tw_day_trade_entry_remainder_policy='first_minute_only'))
    new_contract = _trading_checkpoint_contract(train)
    old_contract = _trading_checkpoint_contract(legacy)
    key = 'entry_remainder_policy'
    assert new_contract['taiwan_execution'][key] == 'frozen_target_until_1320'
    assert key not in old_contract['taiwan_execution']
    del new_contract['taiwan_execution'][key]
    assert new_contract == old_contract


def test_unlimited_close_keeps_minute_sweep_and_new_checkpoint_contract():
    config = load_config(ROOT / 'configs/deployments/tw_day_trade_v8_web_close_unlimited_v8.yaml')
    assert config.trading.tw_day_trade_terminal_liquidation_unlimited_capacity
    assert config.trading.tw_day_trade_entry_remainder_policy == 'frozen_target_until_1320'
    assert config.trading.max_volume_participation == .5
    assert config.trading.tw_day_trade_subscription_right_policy == 'reject_held'
    assert not config.training.day_trade_sparse_events
    assert not config.training.day_trade_training_annual_episodes
    assert _trading_checkpoint_contract(config)['taiwan_execution']['subscription_right_policy'] == 'reject_held'
    assert _trading_checkpoint_contract(config) != _trading_checkpoint_contract(load_config(TRAIN))


@pytest.mark.parametrize('override', [
    {'trading': {'max_volume_participation': .25}},
    {'trading': {'tw_day_trade_unlimited_margin_conversion': False}},
    {'training': {'day_trade_sparse_events': True}},
    {'training': {'day_trade_training_annual_episodes': True}},
])
def test_sweep_recipe_rejects_incompatible_execution_assumptions(tmp_path, override):
    path = tmp_path / 'invalid.yaml'
    path.write_text(yaml.safe_dump({'base_config': str(TRAIN), **override}), encoding='utf-8')
    with pytest.raises(ValueError):
        load_config(path)
