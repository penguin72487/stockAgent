"""The requested recipe must not silently select the legacy flat-stock ledger."""

from pathlib import Path

import numpy as np

from stockagent.config import _load_raw_config, load_config
from stockagent.data.walkforward import build_expanding_year_folds
from stockagent.training.trainer import (
    _ExecutionRuntime,
    _canonical_tensor_day_trade_report_label,
    _mode_artifact_contract_for_config,
)


ROOT = Path(__file__).resolve().parents[1]
CANDIDATE = ROOT / 'configs/markets/tw_day_trade_attention_layernorm_paper_parity_candidate.yaml'
SOURCE = ROOT / 'configs/markets/tw_day_trade_1m_hybrid_v12_attention_full_then_last_layernorm.yaml'


def test_candidate_retains_selected_architecture_and_annual_ownership():
    candidate = _load_raw_config(CANDIDATE)
    source = _load_raw_config(SOURCE)
    for field in ('model_name', 'lookback', 'financial_transformer',
                  'transformer_base_portfolio', 'loss_type'):
        assert candidate['training'][field] == source['training'][field]
    assert candidate['environment']['amp_dtype'] == 'bf16'
    wf = candidate['walk_forward']
    for field in ('min_train_years', 'val_years', 'require_future_test_year',
                  'require_contiguous_years', 'lookback_context'):
        assert wf[field] == source['walk_forward'][field]
    assert candidate['data']['panel_start_date'] == source['data']['panel_start_date'] == '2014-01-06'
    assert wf['expected_first_year'] == source['walk_forward']['expected_first_year'] == 2014
    years = np.arange(2014, 2027)
    dates = np.array([f'{year}-01-01' for year in years], dtype='datetime64[D]')
    folds = build_expanding_year_folds(
        dates, wf['min_train_years'], wf['val_years'], wf['require_future_test_year'])
    assert len(folds) == 11
    assert folds[0].train_years == [2014]
    assert folds[0].val_years == [2015]
    assert folds[0].test_years == list(range(2016, 2027))
    assert folds[-1].train_years == list(range(2014, 2025))
    assert folds[-1].val_years == [2025]
    assert folds[-1].test_years == [2026]


def test_candidate_preserves_requested_minute_capacity_and_carry():
    candidate = _load_raw_config(CANDIDATE)
    assert candidate['data']['day_trade_minute_execution_allow_daily_proxy'] is True
    assert candidate['data']['day_trade_minute_execution_daily_proxy_price_policy'] == 'official_open_close'
    assert candidate['trading']['max_volume_participation'] == 0.5
    assert candidate['trading']['volume_participation_equity'] == 10000000.0
    assert candidate['trading']['tw_day_trade_unlimited_margin_conversion'] is True
    assert candidate['trading']['tw_short_capacity_limit_enabled'] is False
    assert candidate['runner']['resume'] is True
    assert candidate['runner']['output_dir'] != _load_raw_config(SOURCE)['runner']['output_dir']
    assert candidate['training']['pretrained_initialization_root'] == _load_raw_config(SOURCE)['training']['pretrained_initialization_root']
    assert candidate['training']['pretrained_initialization_symbol_adapter'] == 'permutation_invariant_superset'
    assert candidate['training']['multi_gpu_strategy'] == 'distributed_data_parallel'
    resolved = load_config(CANDIDATE)
    assert resolved.trading.tw_day_trade_unlimited_margin_conversion is True
    assert resolved.data.day_trade_minute_execution_daily_proxy_price_policy == 'official_open_close'


def test_unmentioned_recipe_fields_cannot_drift_from_named_artifact_baseline():
    candidate, source = _load_raw_config(CANDIDATE), _load_raw_config(SOURCE)
    def leaves(value, prefix=''):
        if not isinstance(value, dict):
            return {prefix: value}
        return {name: leaf for key, child in value.items()
                for name, leaf in leaves(child, f'{prefix}.{key}' if prefix else key).items()}
    wanted, actual = leaves(source), leaves(candidate)
    changed = {key for key in wanted if key not in actual or wanted[key] != actual[key]}
    assert changed == {
        'experiment_name', 'runner.output_dir', 'data.panel_cache_root',
        'data.day_trade_minute_execution_cache_dir',
        'trading.tw_day_trade_unlimited_margin_conversion',
    }
    # The new price option did not exist in the artifact recipe; do not add
    # unrelated alternatives to the inherited baseline.
    added = {key: actual[key] for key in actual.keys() - wanted.keys()}
    assert added == {
        'data.day_trade_minute_execution_daily_proxy_price_policy': 'official_open_close',
        'training.pretrained_initialization_symbol_adapter': 'permutation_invariant_superset',
    }


def test_vast_candidate_pins_release_and_keeps_writes_outside_materialization():
    path = ROOT / 'configs/deployments/tw_day_trade_attention_parity_vastai1t_candidate.yaml'
    remote = _load_raw_config(path)
    generic = _load_raw_config(CANDIDATE)
    assert remote['training'] == generic['training']
    assert remote['trading'] == generic['trading']
    assert remote['walk_forward'] == generic['walk_forward']
    assert remote['data']['feature_include'] == generic['data']['feature_include']
    for key in ('parquet_root', 'tw_public_feature_path', 'day_trade_minute_execution_root'):
        value = remote['data'][key]
        assert value.startswith('/srv/stockagent-packed-materialized/')
        assert '/current/' not in value and '/latest/' not in value
    assert '248d0869d7d53be4' in remote['data']['parquet_root']
    assert '248d0869d7d53be4' in remote['data']['tw_public_feature_path']
    assert '09bedd96a2f68c39' in remote['data']['day_trade_minute_execution_root']
    for key in ('panel_cache_root', 'day_trade_minute_execution_cache_dir'):
        assert remote['data'][key].startswith('/root/stockAgent/artifacts/')
    assert remote['environment']['cpu_threads'] <= 53
    assert remote['runner']['require_cuda'] is True
    resolved = load_config(path)
    assert resolved.runner.require_cuda is True


def test_vast_integration_workspace_keeps_contract_and_measured_batch32_overrides():
    old = _load_raw_config(ROOT / 'configs/deployments/tw_day_trade_attention_parity_vastai1t_candidate.yaml')
    path = ROOT / 'configs/deployments/tw_day_trade_attention_training_vastai1t.yaml'
    new = _load_raw_config(path)
    assert new['trading'] == old['trading']
    assert new['walk_forward'] == old['walk_forward']
    assert new['environment'] == old['environment']
    assert new['training']['pretrained_initialization_root'] == '/root/stockAgent/' + old['training']['pretrained_initialization_root']
    new['training']['pretrained_initialization_root'] = old['training']['pretrained_initialization_root']
    assert new['training']['batch_size_train'] == 32
    assert new['training']['batch_size_eval'] == 16
    assert new['training']['day_trade_event_compression'] is True
    assert new['training']['day_trade_sparse_events'] is False
    assert new['training']['day_trade_sparse_event_slots'] == 262144
    for key in ('batch_size_train', 'batch_size_eval'):
        new['training'][key] = old['training'][key]
    for key in (
        'day_trade_event_compression',
        'day_trade_sparse_events',
        'day_trade_sparse_event_slots',
    ):
        new['training'].pop(key)
    assert new['training'] == old['training']
    for key in ('panel_cache_root', 'day_trade_minute_execution_cache_dir'):
        assert new['data'][key].startswith('/root/stockAgent/artifacts/')
        new['data'][key] = old['data'][key]
    assert new['data'] == old['data']
    assert new['runner']['output_dir'].startswith('/root/stockAgent/artifacts/')
    resolved = load_config(path)
    assert resolved.training.pretrained_initialization_root.startswith('/root/stockAgent/')


def test_candidate_metadata_names_the_physical_fifo_account_not_legacy_tplus():
    config = load_config(CANDIDATE)
    contract = _mode_artifact_contract_for_config(config)
    assert contract['recurrent_state_scope'] == (
        'cross_session_physical_fifo_margin_inventory_and_dated_cash_claims'
    )
    assert contract['terminal_policy'] == (
        'marked_physical_margin_inventory_after_1330_or_absorbing_default'
    )
    assert contract['mode_details']['execution_variant'] == (
        'exact_board_lot_minute_physical_fifo_v1'
    )
    assert contract['mode_details']['legacy_tplus_cash_queue'] == 'not_applicable'


def test_candidate_console_label_names_the_physical_fifo_account():
    runtime = _ExecutionRuntime(
        mode='tw_day_trade',
        buy_fee_rates=None,
        sell_fee_rates=None,
        lot_sizes=None,
        settlement_lag_sessions=2,
        day_trade_unlimited_margin_conversion=True,
    )
    assert _canonical_tensor_day_trade_report_label(runtime) == (
        'physical-fifo-margin-carry'
    )
