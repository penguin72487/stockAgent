from dataclasses import asdict, replace
import json
from pathlib import Path
from unittest.mock import Mock

import pytest
import torch

from scripts import run_tw_scale_cash_annual_ablations as entry
from scripts.check_tw_scale_cash_ablation_context import assess_calendar
from scripts.run_ablation_experiments import _build_configs, _experiment_rows
from stockagent.config import load_config
from stockagent.models.factory import build_model


def _flatten(value, prefix=''):
    if isinstance(value, dict):
        result = {}
        for key, child in value.items():
            result.update(_flatten(child, f'{prefix}.{key}' if prefix else key))
        return result
    return {prefix: value}


@pytest.fixture(scope='module')
def dated_manifest(tmp_path_factory):
    import numpy as np
    context = np.arange('2014-01-06', '2015-01-01', dtype='datetime64[D]')
    context = context[np.is_busday(context)][:246]
    targets = np.arange('2015-01-05', '2026-10-01', dtype='datetime64[D]')
    dates = np.concatenate([context, targets[np.is_busday(targets)]])
    path = tmp_path_factory.mktemp('calendar') / 'manifest.json'
    path.write_text(json.dumps({'dates': dates.astype(str).tolist()}))
    return path


@pytest.fixture(scope='module')
def cases(tmp_path_factory, dated_manifest):
    spec, rows = _experiment_rows(entry.SPEC)
    spec['matrix']['base_overrides'] = {'data': {'factorized_feature_manifest': str(dated_manifest)}}
    root = tmp_path_factory.mktemp('annual-ablations')
    return _build_configs(entry.SPEC, spec, rows, root)


@pytest.fixture(scope='module')
def baseline(tmp_path_factory, dated_manifest):
    spec, _ = _experiment_rows(entry.SPEC)
    spec['matrix']['base_overrides'] = {'data': {'factorized_feature_manifest': str(dated_manifest)}}
    root = tmp_path_factory.mktemp('ablation-engineering-control')
    row = {'name': 'baseline', 'description': 'engineering config only', 'overrides': {}}
    return load_config(_build_configs(entry.SPEC, spec, [row], root)[0]['config_path'])


def test_list_never_prepares_or_launches(monkeypatch, capsys):
    run = Mock(side_effect=AssertionError('must not launch'))
    monkeypatch.setattr(entry.subprocess, 'run', run)
    monkeypatch.setattr(entry, 'LAUNCH', Path('/does/not/exist'))
    assert entry.main(['--list']) == 0
    assert 'choices=27' in capsys.readouterr().out
    run.assert_not_called()


@pytest.mark.parametrize('live_status,frozen_status,expected_calls', [(1, 0, 1), (0, 1, 2), (0, 0, 2)])
def test_operational_cuda_preflight_keeps_frozen_trainer_and_stops_on_failure(
    monkeypatch, capsys, live_status, frozen_status, expected_calls,
):
    code = Path('/immutable/trainer')
    env = {'CUDA_VISIBLE_DEVICES': '0,1'}
    run = Mock(side_effect=[Mock(returncode=live_status), Mock(returncode=frozen_status)])
    monkeypatch.setattr(entry.subprocess, 'run', run)
    assert entry._training_preflight(code, env) == (live_status or frozen_status)
    assert run.call_count == expected_calls
    first = run.call_args_list[0]
    assert first.args[0][1] == str(entry.ROOT / 'scripts/check_environment.py')
    assert first.args[0][-4:] == ['--require-cuda', '--minimum-cuda-devices', '2', '--strict']
    assert first.kwargs == {'cwd': code, 'env': env}
    if expected_calls == 2:
        assert run.call_args_list[1].args[0][1] == str(code / 'scripts/check_environment.py')
    if live_status or frozen_status:
        assert '尚未啟動訓練' in capsys.readouterr().out


def test_repair_catalog_keeps_exact_same_selected_experiments():
    legacy, old_rows = _experiment_rows(entry.SPEC)
    repaired, new_rows = _experiment_rows(entry.REPAIR_SPEC)
    assert old_rows == new_rows
    assert legacy['matrix'] == repaired['matrix']
    assert legacy['fold_selection'] == repaired['fold_selection'] == 'last'
    assert repaired['output_root'].endswith('_v6')
    assert legacy['output_root'] != repaired['output_root']


def test_repair_list_never_prepares_or_launches(monkeypatch, capsys):
    run = Mock(side_effect=AssertionError('must not launch'))
    monkeypatch.setattr(entry.subprocess, 'run', run)
    assert entry.main(['--numerical-repair', '--list']) == 0
    assert 'choices=27' in capsys.readouterr().out
    run.assert_not_called()


def test_numerical_source_rejects_a_different_parent(tmp_path):
    path = tmp_path / 'binding.json'
    path.write_text(json.dumps({'contract': 'tw_ablation_numerical_source_v2',
                               'parent_training_source_sha256': 'wrong',
                               'parent_reporting_source_sha256': 'report'}))
    with pytest.raises(ValueError, match='different immutable parent'):
        entry._numerical_source({'source_sha256': 'train'}, {'source_sha256': 'report'}, path)


@pytest.mark.parametrize('bad', [
    {'head_compile_backend':'aot_eager'}, {'head_fullgraph_pass':False},
    {'head_captured_adjoint_pass':False}, {'world_size':1},
    {'complete_fold_lifecycle_pass':False}, {'canonical_resume_pass':False},
])
def test_numerical_source_requires_inductor_and_real_dual_ddp_acceptance(tmp_path,monkeypatch,bad):
    import stockagent.runtime_identity as identity
    monkeypatch.setattr(identity,'verify_source_release',lambda *a:{'source_sha256':'numeric'})
    monkeypatch.setattr(identity,'verify_release_bundles',lambda *a:None)
    allowed=['stockagent/models/factorized_input.py','stockagent/models/transformer_base_portfolio.py',
             'stockagent/training/checkpoint_contract.py','stockagent/training/trainer.py']
    parent=tmp_path/'parent.json';parent.write_text(json.dumps({'code':{'files':{n:'old' for n in allowed}}}))
    child=tmp_path/'release.json';child.write_text(json.dumps({'code':{'files':{n:'new' for n in allowed}}}))
    proof=tmp_path/'proof.json'
    proof.write_text(json.dumps({'state':'accepted_narrow_head_inductor_dual_ddp_v2',
        'source_sha256':'numeric','world_size':2,'complete_fold_lifecycle_pass':True,
        'canonical_resume_pass':True,'head_compile_backend':'inductor','head_fullgraph_pass':True,
        'head_captured_adjoint_pass':True,**bad}))
    binding=tmp_path/'binding.json'
    binding.write_text(json.dumps({'contract':'tw_ablation_numerical_source_v2',
        'parent_training_source_sha256':'train','parent_reporting_source_sha256':'report',
        'source_sha256':'numeric','source_receipt':str(child),'acceptance_receipt':str(proof)}))
    with pytest.raises(ValueError,match='dual-DDP lifecycle/resume acceptance'):
        entry._numerical_source({'source_sha256':'train'},
            {'source_sha256':'report','source_receipt':str(parent)},binding)


@pytest.mark.parametrize('raw', ['', ',', 'baseline,', ',baseline', 'baseline,,ffn__gelu'])
def test_reject_empty_selection(raw):
    with pytest.raises(ValueError, match='non-empty'):
        entry._selection(raw)


def test_unknown_choice_rejected_before_remote_operations(monkeypatch):
    run = Mock(side_effect=AssertionError('must not launch'))
    monkeypatch.setattr(entry.subprocess, 'run', run)
    with pytest.raises(ValueError, match='unknown experiments'):
        entry.main(['--only', 'unknown'])
    run.assert_not_called()


@pytest.mark.parametrize('name', ['baseline', 'cross_context__temporal_only',
                                 'lookback__d64', 'batch__b512', 'pooling__mean', 'compile__off'])
def test_unselected_experiments_rejected(name):
    with pytest.raises(ValueError, match='unknown experiments'):
        entry.main(['--only', name])


def test_exact_selected_order_and_no_extra_control():
    _, rows = _experiment_rows(entry.SPEC)
    names = [row['name'] for row in rows]
    assert len(names) == 27
    assert names[:5] == ['pooling__attention', 'lookback__d256', 'embedding__d16',
                         'embedding__d64', 'embedding__d128']
    assert names[5:13] == ['cross_context__no_latent', 'cross_context__no_market',
                           'time_position__off', 'rope__off', 'qk_norm__off',
                           'input_rms__off', 'norm__rmsnorm', 'ffn__gelu']
    assert len([name for name in names if name.startswith('output__')]) == 12
    assert names[-2:] == ['annual_reset__off', 'sub_lot_gradient__on']
    assert 'baseline' not in names
    _, subset = _experiment_rows(entry.SPEC, {'embedding__d128', 'pooling__attention'})
    assert [row['name'] for row in subset] == ['pooling__attention', 'embedding__d128']


def test_run_selected_list_never_launches(monkeypatch, capsys):
    run = Mock(side_effect=AssertionError('must not launch'))
    monkeypatch.setattr(entry.subprocess, 'run', run)
    assert entry.main(['--run-selected', '--list']) == 0
    assert 'choices=27' in capsys.readouterr().out
    run.assert_not_called()


def test_command_leaves_per_case_fold_to_canonical_scheduler(cases):
    command = entry._command(Path('/frozen'), Path('/spec.yaml'), {'baseline', 'ffn__gelu'}, dry_run=True)
    assert '--start-fold' not in command
    for flag, value in [('--max-folds', '1'), ('--parallel-jobs', '1'),
                        ('--multi-gpu-strategy', 'distributed_data_parallel')]:
        assert command[command.index(flag) + 1] == value
    assert '--dry-run' in command and '--no-auto-resume' in command
    assert '--profile-timing' not in command
    assert command[1] == '/frozen/scripts/run_ablation_experiments.py'
    adapter = entry.RUNNER.read_text()
    assert '--resume' in adapter and '--no-profile-timing' in adapter
    assert '--no-retrain-completed-folds' in adapter


def test_contract_binding_preserves_existing_bytes(tmp_path):
    path = tmp_path / 'contract.json'
    entry._bind_contract(path, {'source': 'a'})
    original = path.read_bytes()
    entry._bind_contract(path, {'source': 'a'})
    with pytest.raises(ValueError, match='incompatible'):
        entry._bind_contract(path, {'source': 'b'})
    assert path.read_bytes() == original


def test_every_variant_has_effect_and_preserves_source_and_execution(cases, baseline):
    names = [row['name'] for row in cases]
    assert len(names) == len(set(names))
    base_values = _flatten(asdict(baseline))
    normalized = set()
    for row in cases:
        c = load_config(row['config_path'])
        expected_fold = 10 if row['name'] == 'lookback__d256' else 11
        assert c.runner.start_fold == expected_fold
        assert row['start_fold'] == expected_fold
        assert c.runner.resume and c.runner.require_cuda
        assert c.training.multi_gpu_strategy == 'distributed_data_parallel'
        assert c.training.pretrained_initialization_root is None
        assert not c.training.day_trade_sparse_events
        assert not c.training.debug_timing_sync
        assert asdict(c.data) == asdict(baseline.data)
        assert asdict(c.trading) == asdict(baseline.trading)
        assert asdict(c.walk_forward) == asdict(baseline.walk_forward)
        assert c.walk_forward.year_boundary_mode == 'lookback_shifted'
        assert c.walk_forward.split_start_year == 2014
        assert c.walk_forward.lookback_context == 'panel_history'
        assert c.training.financial_transformer.feature_svd_components == 0
        assert c.training.financial_transformer.feature_bottleneck_dim == 0
        assert not c.training.financial_transformer.temporal_basis_families
        if row['name'] != 'annual_reset__off':
            assert c.training.day_trade_training_annual_episodes
        actual = _flatten(asdict(c))
        changes = {key: value for key, value in actual.items()
                   if key not in {'experiment_name', 'runner.output_dir'} and value != base_values[key]}
        assert changes, row['name']
        signature = repr(sorted(changes.items()))
        assert signature not in normalized, row['name']
        normalized.add(signature)


def test_all_models_construct_at_full_feature_abi_without_gpu_jobs(cases):
    # Meta-device constructor validation is engineering only, not GPU/DDP,
    # execution equivalence, VRAM admission or a training success claim.
    for row in cases:
        c = load_config(row['config_path'])
        with torch.device('meta'):
            model = build_model(config=c, lookback=c.training.lookback,
                                num_features=14726, num_symbols=2330)
        assert model.num_features == 14726, row['name']
        if row['name'] == 'cross_context__no_latent':
            assert model.attention_mode == 'market_token'
        elif row['name'] == 'cross_context__no_market':
            assert model.attention_mode == 'latent_only'
        elif row['name'] == 'pooling__attention':
            assert model.temporal_query_mode == 'full_then_last'
            assert model.temporal_pooling == 'attention'
            assert c.training.lookback == 32 and c.training.financial_transformer.d_model == 32
        elif row['name'] == 'lookback__d256':
            assert c.training.lookback == 256
            assert model.temporal_query_mode == 'last_only' and model.temporal_pooling == 'last'


def test_environment_keeps_baseline_precision_and_owner(monkeypatch):
    monkeypatch.setenv('PYTORCH_CUDA_ALLOC_CONF', 'legacy')
    monkeypatch.setenv('STOCKAGENT_RETRAIN_COMPLETED_FOLDS', '1')
    env = entry._environment({'source_receipt': '/fixed/release.json', 'transform_cache': '/shared/cache'},
                             {'transport_block_rows': 16}, Path('/fixed/code'))
    assert env['CUDA_VISIBLE_DEVICES'] == '0,1'
    assert env['STOCKAGENT_DAY_TRADE_TRANSPORT_BLOCK_ROWS'] == '16'
    assert env['STOCKAGENT_DAY_TRADE_CARRY_FLAT_TERMINAL'] == '1'
    assert env['STOCKAGENT_ABLATION_FROZEN_RUNNER'] == '/fixed/code/coda_runner.sh'
    assert env['STOCKAGENT_RETRAIN_COMPLETED_FOLDS'] == '0'
    assert 'PYTORCH_CUDA_ALLOC_CONF' not in env


def test_only_source_fixed_selectors_are_in_fixed_overrides():
    spec, rows = _experiment_rows(entry.SPEC)
    fixed = _flatten(spec['matrix']['fixed_overrides'])
    for row in rows:
        overlap = set(fixed).intersection(_flatten(row['overrides']))
        assert not overlap, (row['name'], overlap)


def test_output_aliases_not_counted_as_independent_experiments(cases, baseline):
    names = {row['name'] for row in cases}
    assert len(names) == 27
    assert 'output__cash_entmax' not in names and 'output__activation_l1' not in names
    assert 'output__l1' in names and 'output__score_cash' in names
    c = baseline
    with torch.device('meta'):
        model = build_model(config=c, lookback=c.training.lookback, num_features=14726, num_symbols=2330)
    for left, right in [('l1', 'activation_l1'), ('cash_entmax15', 'score_entmax_cash')]:
        def transform(mode):
            scores = torch.tensor([[0.0, 0.2, -0.7, 2.0], [0.01, -0.5, 0.0, 0.3]], requires_grad=True)
            weights, _ = model._postprocess_flat_target_logits(scores,
                torch.tensor([[True, True, True, True], [True, False, True, True]]),
                output_mode=mode, portfolio_activation='pre_normalized', return_parts=False)
            grad, = torch.autograd.grad((weights * torch.tensor([[.1, .2, -.3, .5], [.4, .5, .6, .7]])).sum(), scores)
            return weights, grad
        output_l, grad_l = transform(left)
        output_r, grad_r = transform(right)
        torch.testing.assert_close(output_l, output_r, rtol=0, atol=0)
        torch.testing.assert_close(grad_l, grad_r, rtol=0, atol=0)


def test_all_variants_shift_years_once_and_borrow_prior_context(baseline, dated_manifest):
    from stockagent.data.walkforward import build_expanding_year_folds
    from stockagent.training.trainer import (
        _split_valid_indices, _validate_annual_day_trade_training_boundaries,
    )
    dates = json.loads(dated_manifest.read_text())['dates']
    def assess(config):
        return assess_calendar(config, dates, build_folds=build_expanding_year_folds,
            split_indices=_split_valid_indices, validate_annual=_validate_annual_day_trade_training_boundaries)
    short = assess(baseline)
    assert short['state'] == 'accepted_calendar_context'
    assert short['context_sessions_before_first_target'] == 32
    assert short['fold_ids'] == [11]
    assert short['train_years'] == list(range(2014, 2025))
    assert short['val_years'] == [2025] and short['test_years'] == [2026]
    long = replace(baseline, training=replace(baseline.training, lookback=256),
                   runner=replace(baseline.runner, start_fold=10))
    proof = assess(long)
    assert proof['state'] == 'accepted_calendar_context'
    assert proof['context_sessions_before_first_target'] == 256
    assert not proof['missing_target_dates']
    assert proof['first_target'] == '2015-01-19'
    assert proof['first_valid_target'] == '2015-01-19'
    assert proof['period_contract']['offset_sessions'] == 256
    assert proof['annual_account_validation']['contract'] == 'observed_session_shifted_fresh_capital_v1'
    assert proof['test_years'] == [2025]
    assert proof['fold_ids'] == [10]
    # Synthetic weekdays are not the production TW calendar; the last shifted
    # period can start in late 2025 but must still own the latest 2026 sessions.
    assert proof['actual_intervals']['test']['last'].startswith('2026-')
    with pytest.raises(ValueError, match='not the last canonical fold=11'):
        assess(replace(baseline, runner=replace(baseline.runner, start_fold=10)))


@pytest.mark.parametrize('args', [[], ['--run-selected'], ['--dry-run']])
def test_default_full_queue_and_blocked_calendar_stop_before_gpu_work(tmp_path, monkeypatch, args):
    code = tmp_path / 'release/build-source'
    for name in ('scripts/run_ablation_experiments.py', 'scripts/check_environment.py', 'coda_runner.sh'):
        path = code / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('# engineering fixture\n')
    proof_path = tmp_path / 'acceptance.json'
    proof_path.write_text(json.dumps({'feature_manifest_sha256': 'feature', 'transport_block_rows': 16}))
    launch_path = tmp_path / 'launch.json'
    launch_path.write_text(json.dumps({'contract': 'accepted_annual_scale_cash_launch_v1',
        'state': 'ready', 'source_receipt': str(code.parent / 'release.json'),
        'acceptance_receipt': str(proof_path), 'config': '/accepted/config.yaml',
        'transform_cache': '/shared/cache', 'source_sha256': 'source',
        'config_sha256': 'config', 'source_manifest_sha256': 'input'}))
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        if 'check_tw_scale_cash_ablation_context.py' in command[1]:
            report = Path(command[command.index('--report') + 1])
            report.write_text(json.dumps({'feature_manifest_sha256': 'feature',
                'baseline_reference': {'state': 'verified_report_only_reference',
                    'rows': [{'fold_id': 10}], 'new_baseline_training': False}}))
            return Mock(returncode=2)
        return Mock(returncode=0)
    monkeypatch.setattr(entry, 'ROOT', tmp_path)
    monkeypatch.setattr(entry, 'LAUNCH', launch_path)
    monkeypatch.setattr(entry, '_shifted_source', lambda launch, path: (
        {'source_receipt': str(code.parent / 'release.json'), 'source_sha256': 'shifted'}, code))
    monkeypatch.setattr(entry, '_reporting_source', lambda source, path: (
        {'source_receipt': '/report/release.json', 'source_sha256': 'report'}, code))
    monkeypatch.setattr(entry.subprocess, 'run', run)
    assert entry.main(args) == 2
    assert len(calls) == 3  # launch check-only, config dry-run, calendar check
    assert '--check-only' in calls[0] and '--dry-run' in calls[1]
    assert not any('--require-cuda' in command for command in calls)
    assert not any('manage_gpu_jobs.py' in part for command in calls for part in command)
    selected = calls[1][calls[1].index('--only') + 1].split(',')
    assert len(selected) == 27
    assert not list(tmp_path.glob('artifacts/markets/**/baseline/summary.json'))


def test_source_binding_rejects_unrelated_amendments(tmp_path, monkeypatch):
    import stockagent.runtime_identity as identity
    files = {name: 'before' for name in ['stockagent/training/checkpoint_contract.py',
        'stockagent/training/day_trade_carry_bridge.py', 'stockagent/training/trainer.py',
        'scripts/run_ablation_experiments.py']}
    parent = tmp_path / 'parent.json'
    parent.write_text(json.dumps({'code': {'files': files}}))
    child = tmp_path / 'new/release.json'
    child.parent.mkdir()
    child.write_text(json.dumps({'code': {'files': {**{name: 'after' for name in files},
                                                  'train.py': 'unrelated'}}}))
    binding = tmp_path / 'binding.json'
    binding.write_text(json.dumps({'contract': 'tw_last_fold_source_v1',
        'source_receipt': str(child), 'source_sha256': 'shifted', 'parent_source_sha256': 'parent'}))
    monkeypatch.setattr(identity, 'verify_source_release', lambda *a: {'source_sha256': 'shifted'})
    monkeypatch.setattr(identity, 'verify_release_bundles', lambda *a: {})
    with pytest.raises(ValueError, match='unapproved source files'):
        entry._shifted_source({'source_receipt': str(parent), 'source_sha256': 'parent'}, binding)


@pytest.mark.parametrize('extra', [False, True])
def test_reporting_release_changes_only_plotter_and_scheduler(tmp_path, monkeypatch, extra):
    import stockagent.runtime_identity as identity
    parent_files = {'scripts/run_ablation_experiments.py': 'a',
                    'scripts/plot_ablation_analysis.py': 'b', 'stockagent/training/trainer.py': 'stable'}
    parent = tmp_path / 'training.json'
    parent.write_text(json.dumps({'code': {'files': parent_files}}))
    child_files = {**parent_files, 'scripts/run_ablation_experiments.py': 'c',
                   'scripts/plot_ablation_analysis.py': 'd'}
    if extra:
        child_files['stockagent/training/trainer.py'] = 'not-report-only'
    child = tmp_path / 'reports/release.json'
    child.parent.mkdir()
    child.write_text(json.dumps({'code': {'files': child_files}}))
    binding = tmp_path / 'binding.json'
    binding.write_text(json.dumps({'contract': 'tw_ablation_reporting_source_v1',
        'source_receipt': str(child), 'source_sha256': 'reports', 'parent_source_sha256': 'training'}))
    monkeypatch.setattr(identity, 'verify_source_release', lambda *a: {'source_sha256': 'reports'})
    monkeypatch.setattr(identity, 'verify_release_bundles', lambda *a: {})
    training = {'source_receipt': str(parent), 'source_sha256': 'training'}
    if extra:
        with pytest.raises(ValueError, match='optimizer/core files'):
            entry._reporting_source(training, binding)
    else:
        report, code = entry._reporting_source(training, binding)
        assert report['parent_source_sha256'] == 'training'
        assert code == child.parent / 'build-source'


def test_report_overlay_does_not_change_any_generated_training_config(tmp_path, cases, dated_manifest):
    spec, rows = _experiment_rows(entry.SPEC)
    spec['matrix']['base_overrides'] = {'data': {'factorized_feature_manifest': str(dated_manifest)}}
    root = tmp_path / 'same-output'
    before = _build_configs(entry.SPEC, spec, rows, root)
    saved = {row['name']: row['config_path'].read_bytes() for row in before}
    spec['postprocess_plots'] = entry._report_plot_specs(tmp_path / 'ops')
    after = _build_configs(entry.SPEC, spec, rows, root)
    assert {row['name']: row['config_path'].read_bytes() for row in after} == saved
    assert len(after) == 27


def test_plots_only_preserves_training_owner_and_skips_generation_and_gpu(tmp_path, monkeypatch):
    training_code = tmp_path / 'training/build-source'
    report_code = tmp_path / 'reports/build-source'
    for name in ('scripts/run_ablation_experiments.py', 'scripts/check_environment.py', 'coda_runner.sh'):
        path = training_code / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('# engineering fixture\n')
    proof = tmp_path / 'accepted.json'
    proof.write_text(json.dumps({'feature_manifest_sha256': 'features', 'transport_block_rows': 16}))
    launch = tmp_path / 'launch.json'
    launch.write_text(json.dumps({'contract': 'accepted_annual_scale_cash_launch_v1', 'state': 'ready',
        'source_receipt': '/original/release.json', 'acceptance_receipt': str(proof),
        'config': '/accepted/config.yaml', 'transform_cache': '/shared/cache',
        'source_sha256': 'original', 'config_sha256': 'config', 'source_manifest_sha256': 'input'}))
    runtime = tmp_path / 'artifacts/operations/training_launches' / Path(_experiment_rows(entry.SPEC)[0]['output_root']).name
    runtime.mkdir(parents=True)
    (runtime / 'calendar-context.json').write_text(json.dumps({'state': 'accepted_calendar_context',
        'feature_manifest_sha256': 'features'}))
    calls = []
    def run(command, **kwargs):
        calls.append((command, kwargs))
        assert '--dry-run' not in command
        assert '--require-cuda' not in command
        return Mock(returncode=0)
    monkeypatch.setattr(entry, 'ROOT', tmp_path)
    monkeypatch.setattr(entry, 'LAUNCH', launch)
    monkeypatch.setattr(entry, '_shifted_source', lambda *a: (
        {'source_receipt': '/training/release.json', 'source_sha256': 'training'}, training_code))
    monkeypatch.setattr(entry, '_reporting_source', lambda *a: (
        {'source_receipt': '/reports/release.json', 'source_sha256': 'reports'}, report_code))
    monkeypatch.setattr(entry.subprocess, 'run', run)
    assert entry.main(['--plots-only']) == 0
    assert len(calls) == 2  # check-only and actual reporting, no train/config generation
    command, kwargs = calls[-1]
    assert '--plots-only' in command
    assert command[1] == str(report_code / 'scripts/run_ablation_experiments.py')
    assert kwargs['env']['STOCKAGENT_CODE_RELEASE_RECEIPT'] == '/training/release.json'
    assert kwargs['env']['STOCKAGENT_ABLATION_FROZEN_RUNNER'] == str(training_code / 'coda_runner.sh')
    assert kwargs['env']['STOCKAGENT_ABLATION_REPORT_SOURCE_RECEIPT'] == '/reports/release.json'
    assert not list(tmp_path.glob('artifacts/markets/**/generated_configs/*.yaml'))
