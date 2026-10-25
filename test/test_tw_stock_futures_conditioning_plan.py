"""Keep a paired optimizer study within its admitted futures research contract."""
from dataclasses import asdict
from pathlib import Path

import torch
import yaml

from scripts import run_ablation_experiments as ablation
from stockagent.config import _load_raw_config, load_config
from stockagent.models.futures_account_policy import (
    ACCOUNT_COEFFICIENT_START, ACCOUNT_COEFFICIENT_END,
)


SPEC = Path("configs/ablations/tw_stock_futures_conditioning_20261023_v1.yaml")
HISTORY2013 = Path("configs/ablations/tw_stock_futures_history2013_20261023_v1.yaml")
SPEED = Path("configs/ablations/tw_stock_futures_projection_speed_20261023_v1.yaml")


def test_projection_abba_changes_only_declared_precision_and_projection_axes(tmp_path):
    spec, cases = ablation._experiment_rows(SPEED)
    assert [r['name'] for r in cases] == [
        f'clock{clock}__{trial}' for clock in ('0845', '0900')
        for trial in ('a1', 'b1', 'b2', 'a2')
    ]
    raw_base = _load_raw_config(spec['base_config'])
    baseline = asdict(load_config(spec['base_config']))
    flags = ('candle_projection_fp32', 'factorized_compact_projection', 'factorized_sparse_projection')
    for row in cases:
        raw = ablation._deep_merge(ablation._deep_merge(raw_base, row['overrides']),
                                  spec['matrix']['fixed_overrides'])
        p = tmp_path / (row['name'] + '.yaml')
        p.write_text(yaml.safe_dump(raw))
        candidate = asdict(load_config(p))
        selected = row['name'].split('__')[1].startswith('b')
        assert all(candidate['training']['financial_transformer'][key] == selected for key in flags)
        assert candidate['training']['epochs'] == 4
        assert candidate['training']['early_stopping_no_improve_ratio'] == 1.
        for key in ('epochs', 'early_stopping_no_improve_ratio'):
            candidate['training'][key] = baseline['training'][key]
        for model in ('financial_transformer', 'executable_portfolio_transformer'):
            for key in flags:
                candidate['training'][model][key] = baseline['training'][model][key]
        assert candidate['training'] == baseline['training']
        for key in ('data', 'environment', 'evaluation', 'walk_forward', 'runner'):
            assert candidate[key] == baseline[key]


def test_complete_history_horizon_is_independent_and_keeps_financial_mechanisms(tmp_path):
    spec, cases = ablation._experiment_rows(HISTORY2013)
    assert [c['name'] for c in cases] == [
        'clock__0845', 'clock__0900', 'clock0845__notional',
        'clock0845__muon', 'clock0900__notional', 'clock0900__muon',
    ]
    base = _load_raw_config(spec['base_config'])
    reference = asdict(load_config(
        'configs/markets/tw_stock_futures_carry_0845_history2011_20261021_v5.yaml'))
    for case in cases:
        raw = ablation._deep_merge(ablation._deep_merge(base, case['overrides']),
                                  spec['matrix']['fixed_overrides'])
        path = tmp_path / (case['name'] + '.yaml')
        path.write_text(yaml.safe_dump(raw))
        candidate = asdict(load_config(path))
        # Full source context is built once before the canonical clock adapter
        # crops it. Changing panel_start_date would break the feature ABI.
        assert candidate['data'] == reference['data']
        assert candidate['walk_forward']['expected_first_year'] == 2013
        assert candidate['walk_forward']['split_start_year'] == 2013
        candidate['walk_forward'].update(expected_first_year=2011, split_start_year=2011)
        assert candidate['walk_forward'] == reference['walk_forward']
        clock = '0900' if '0900' in case['name'] else '0845'
        for key in ('tw_futures_portfolio_entry_clock_path',
                    'tw_futures_portfolio_opening_reference_path'):
            assert f'history2013_20261023_v1/clocks/{clock}/' in candidate['trading'][key]
            candidate['trading'][key] = reference['trading'][key]
        assert candidate['trading']['tw_futures_portfolio_decision_time'] == (
            '09:00' if clock == '0900' else '08:45')
        candidate['trading']['tw_futures_portfolio_decision_time'] = '08:45'
        assert candidate['trading'] == reference['trading']
        variant = case['name'].split('__')[1]
        assert candidate['training']['optimizer_name'] == ('muon' if variant == 'muon' else 'adamw')
        assert candidate['training']['financial_transformer']['futures_notional_score_coordinates'] == (
            variant == 'notional')
        for key in ('optimizer_name', 'optimizer_options'):
            candidate['training'][key] = reference['training'][key]
        for model in ('financial_transformer', 'executable_portfolio_transformer'):
            candidate['training'][model]['futures_notional_score_coordinates'] = False
        assert candidate['training'] == reference['training']
        assert candidate['runner']['resume'] is False
        assert candidate['runner']['require_cuda'] is True
        assert candidate['runner']['isolate_train_folds'] is False
        for key in ('evaluation', 'environment'):
            assert candidate[key] == reference[key]


def test_paired_cases_preserve_every_unselected_training_and_account_field(tmp_path):
    spec, cases = ablation._experiment_rows(SPEC)
    assert [row["name"] for row in cases] == [
        f"clock{clock}__{variant}"
        for clock in ("0845", "0900")
        for variant in ("notional", "adamw_lr3e5", "muon", "notional_lr3e5", "notional_muon")
    ]
    assert spec["fold_selection"] == "last"
    assert spec["runtime"]["parallel_jobs"] == 1
    base = _load_raw_config(spec["base_config"])
    reference = asdict(load_config(spec["base_config"]))
    for case in cases:
        raw = ablation._deep_merge(base, case["overrides"])
        raw = ablation._deep_merge(raw, spec["matrix"]["fixed_overrides"])
        path = tmp_path / (case["name"] + ".yaml")
        path.write_text(yaml.safe_dump(raw))
        candidate = asdict(load_config(path))
        clock, variant = case["name"].split("__")
        assert candidate["training"]["optimizer_name"] == ("muon" if "muon" in variant else "adamw")
        assert candidate["training"]["learning_rate"] == (3e-5 if "lr3e5" in variant else 1e-5)
        assert candidate["training"]["financial_transformer"]["futures_notional_score_coordinates"] == ("notional" in variant)
        for key in ("optimizer_name", "optimizer_options", "learning_rate"):
            candidate["training"][key] = reference["training"][key]
        candidate["training"]["financial_transformer"]["futures_notional_score_coordinates"] = False
        # The config loader mirrors active FinancialTransformer fields into
        # this inactive derived model config. It is not another experiment.
        candidate["training"]["executable_portfolio_transformer"]["futures_notional_score_coordinates"] = False
        assert candidate["training"] == reference["training"]
        if clock == "clock0900":
            assert candidate["trading"]["tw_futures_portfolio_decision_time"] == "09:00"
            for key in ("tw_futures_portfolio_entry_clock_path", "tw_futures_portfolio_opening_reference_path"):
                assert "/0900/" in candidate["trading"][key]
            for key in ("tw_futures_portfolio_decision_time", "tw_futures_portfolio_entry_clock_path", "tw_futures_portfolio_opening_reference_path"):
                candidate["trading"][key] = reference["trading"][key]
        assert candidate["trading"] == reference["trading"]
        for group in ("data", "evaluation", "environment", "walk_forward", "runner"):
            assert candidate[group] == reference[group]


def test_notional_coordinates_scale_causal_logits_and_response_coefficients_together():
    from test_futures_causal_account_policy import actor
    from stockagent.data.tw_stock_context_futures_portfolio import TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS

    torch.manual_seed(41)
    model = actor(True)
    # Exercise a learned response too; a zero head would hide a missing scale.
    with torch.no_grad():
        model.futures_account_response_head.weight.fill_(.03)
    width = len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS)
    features = torch.zeros(2, 1936, width + 5)
    features[:, :2, 1:18] = torch.randn(2, 2, 17)
    features[..., width - 1] = 1004
    features[..., width:] = torch.tensor([100., .1, .75, .02, 1.])
    mask = torch.zeros(2, 1936, dtype=torch.bool)
    mask[:, :2] = True
    stock, stock_mask = torch.randn(2, 4, 2, 3), torch.ones(2, 2, dtype=torch.bool)
    context = dict(candidate_features=features, candidate_mask=mask)
    old = model(stock, stock_mask, portfolio_context=context)
    model.futures_notional_score_coordinates = True
    new = model(stock, stock_mask, portfolio_context=context)
    torch.testing.assert_close(new[..., 1], old[..., 1] * .1, rtol=1e-6, atol=1e-7)
    coefficients = slice(ACCOUNT_COEFFICIENT_START, ACCOUNT_COEFFICIENT_END)
    torch.testing.assert_close(new[..., coefficients], old[..., coefficients] * .1, rtol=1e-6, atol=1e-7)
    assert new[:, 2:, 0].count_nonzero() == 0
    assert new[:, :2, coefficients].abs().sum() > 0
    new[:, :2, 0].sum().backward()
    assert torch.isfinite(model.futures_action_head.weight.grad).all()
    assert model.futures_action_head.weight.grad.abs().sum() > 0
