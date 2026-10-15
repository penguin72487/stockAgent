from dataclasses import asdict
import hashlib
import json
from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml

from scripts import run_ablation_experiments as queue
from scripts import run_tw_scale_cash_annual_ablations as entry
from stockagent.config import load_config
from test_tw_scale_cash_annual_ablations import dated_manifest


@pytest.fixture
def optimized(tmp_path, dated_manifest):
    spec, rows = queue._experiment_rows(entry.OPTIMIZED_SPEC)
    spec['matrix']['base_overrides'] = {'data': {'factorized_feature_manifest': str(dated_manifest)}}
    runs = queue._build_configs(entry.OPTIMIZED_SPEC, spec, rows, tmp_path / 'output')
    return spec, runs


def test_optimized_catalog_preserves_research_variants_and_adds_baseline_first(optimized):
    spec, runs = optimized
    old_spec, old_rows = queue._experiment_rows(entry.REPAIR_SPEC)
    _, rows = queue._experiment_rows(entry.OPTIMIZED_SPEC)
    assert rows[1:] == old_rows
    assert len(rows) == 28 and rows[0]['name'] == 'baseline'
    settings = yaml.safe_load((entry.ROOT / spec['optimization_recipe']).read_text())['full_fold_candidates'][0]
    shared = settings['overrides']['training']['financial_transformer']
    for run in runs:
        config = load_config(run['config_path'])
        model = asdict(config.training.financial_transformer)
        assert {key: model[key] for key in shared} == shared
        assert config.training.epochs == 1000
        assert config.training.batch_size_train == 256
        assert config.runner.start_fold == queue._last_fold_id(config)
    models = {run['name']: load_config(run['config_path']) for run in runs}
    assert models['embedding__d128'].training.financial_transformer.d_model == 128
    assert models['lookback__d256'].training.lookback == 256
    assert models['baseline'].training.financial_transformer.d_model == 32
    assert spec['matrix']['dimensions'] == old_spec['matrix']['dimensions']


def test_optimized_list_and_command_keep_single_owner_and_continue(monkeypatch, capsys):
    run = Mock(side_effect=AssertionError('list cannot launch'))
    monkeypatch.setattr(entry.subprocess, 'run', run)
    assert entry.main(['--optimized', '--list']) == 0
    assert 'choices=28' in capsys.readouterr().out
    run.assert_not_called()
    command = entry._command(Path('/frozen'), Path('/spec'), {'baseline'}, dry_run=False,
                             python='/private/bin/python', continue_on_failure=True)
    assert command[0] == '/private/bin/python'
    assert '--stop-on-fail' not in command and '--no-auto-resume' in command
    assert command[command.index('--parallel-jobs') + 1] == '1'
    assert command[command.index('--max-folds') + 1] == '1'


def test_oom_retry_preserves_dimensions_and_previous_checkpoint_and_resumes_selected_attempt(optimized):
    spec, runs = optimized
    original = next(r for r in runs if r['name'] == 'embedding__d128')
    base = load_config(original['config_path'])
    policy = spec['runtime']['capacity_retry']
    first = queue._capacity_run(original, policy)
    checkpoint = first['output_dir'] / 'checkpoint_last.pt'
    checkpoint.write_bytes(b'preserved old optimizer')
    log = first['output_dir'] / 'ablation_worker.log'
    log.write_text('torch.OutOfMemoryError: CUDA out of memory')
    smaller = queue._capacity_run(first, policy, oom_log=log)
    changed = load_config(smaller['config_path'])
    assert changed.training.batch_size_train == 128
    assert changed.training.financial_transformer.d_model == 128
    assert changed.training.lookback == base.training.lookback
    assert asdict(changed.data) == asdict(base.data)
    assert asdict(changed.trading) == asdict(base.trading)
    assert changed.training.financial_transformer.kernel_softmax_backend == 'quack'
    assert changed.training.day_trade_optimizer_step_per_trajectory == base.training.day_trade_optimizer_step_per_trajectory
    assert checkpoint.read_bytes() == b'preserved old optimizer'
    assert smaller['output_dir'] != first['output_dir']
    resumed = queue._capacity_run(original, policy)
    assert resumed['config_path'] == smaller['config_path']
    assert original['output_dir'].resolve() == smaller['output_dir']
    assert hashlib.sha256(log.read_bytes()).hexdigest() == json.loads(
        (original['output_dir'].parent / '_capacity' / original['name'] / 'selection.json').read_text()
    )['attempts'][1]['oom_log_sha256']
    smaller['config_path'].write_text(smaller['config_path'].read_text() + '\n')
    with pytest.raises(ValueError, match='config changed'):
        queue._capacity_run(original, policy)


def test_non_oom_cannot_change_capacity_and_retry_is_bounded(optimized):
    spec, runs = optimized
    policy = {**spec['runtime']['capacity_retry'], 'max_retries': 0}
    first = queue._capacity_run(runs[0], policy)
    log = first['output_dir'] / 'failure.log'
    log.write_text('non-finite gradient after full-trajectory accumulation')
    with pytest.raises(ValueError, match='only after CUDA OOM'):
        queue._capacity_run(first, policy, oom_log=log)
    log.write_text('CUDA out of memory')
    assert queue._capacity_run(first, policy, oom_log=log) is None


def test_speed_uses_epoch3_max_rank_and_ignores_partial_row(tmp_path):
    group = tmp_path / 'train_2014-2024'
    group.mkdir()
    (group / 'epoch_curve.jsonl').write_text('\n'.join([
        json.dumps({'epoch': 1, 'epoch_max_rank_s': 500}),
        json.dumps({'epoch': 2, 'epoch_max_rank_s': 300}),
        json.dumps({'epoch': 3, 'epoch_max_rank_s': 80, 'epoch_total_s': 1}),
        json.dumps({'epoch': 4, 'epoch_max_rank_s': 90}),
        json.dumps({'epoch': 5, 'epoch_total_s': 2}), '{"epoch":6,']))
    timing = queue._steady_timing(tmp_path)
    assert timing['steady_epoch_max_rank_median_s'] == 85
    assert [r['epoch'] for r in timing['steady_samples']] == [3, 4]


def test_queue_retries_only_oom_then_continues_after_other_failure(optimized, tmp_path, monkeypatch):
    spec, runs = optimized
    selected = [next(r for r in runs if r['name'] == name)
                for name in ('baseline', 'embedding__d128', 'lookback__d256')]
    spec['postprocess_plots'] = []
    spec['output_root'] = str(tmp_path / 'output')
    catalog = tmp_path / 'catalog.yaml'
    catalog.write_text(yaml.safe_dump(spec))
    monkeypatch.setattr(queue, '_build_configs', lambda *args: selected)

    class Worker:
        launched = []

        def __init__(self, command, **kwargs):
            config = load_config(command[command.index('-c') + 1])
            self.pid = 95000 + len(self.launched)
            self.returncode = 1 if len(self.launched) in (0, 2) else 0
            if len(self.launched) == 0:
                kwargs['stdout'].write('CUDA out of memory\n')
            elif len(self.launched) == 2:
                kwargs['stdout'].write('non-finite gradient after full-trajectory accumulation\n')
            self.launched.append((config.experiment_name, config.training.batch_size_train))

        def poll(self):
            return self.returncode

    monkeypatch.setattr(queue.subprocess, 'Popen', Worker)
    monkeypatch.setattr(queue.sys, 'argv', ['queue', '--spec', str(catalog), '--runner', '/bin/true',
                                          '--max-folds', '1', '--no-auto-resume'])
    with pytest.raises(SystemExit) as result:
        queue.main()
    assert result.value.code == 1  # Failures remain visible despite completing the queue.
    assert [b for _, b in Worker.launched] == [256, 128, 256, 256]
    summary = json.loads((tmp_path / 'output' / 'summary.json').read_text())
    assert [row['status'] for row in summary] == ['succeeded', 'failed', 'succeeded']
    assert summary[0]['attempts'] == 2
    assert summary[1]['attempts'] == 1
