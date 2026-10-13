"""Resource admission reaches the canonical complete-fold boundary unchanged."""
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest
import yaml


@pytest.mark.parametrize('limits,expected', [([], (1.5, .96)),
    (['--min-vram-headroom-gib', '.125', '--max-peak-vram-fraction', '.999'], (.125, .999))])
def test_trial_records_and_forwards_declared_vram_policy(tmp_path, monkeypatch, limits, expected):
    from scripts import run_frontier_training_trial as trial
    base = tmp_path / 'config.yaml'
    base.write_text(yaml.safe_dump({'runner': {}, 'training': {'seed': 42}}))
    acceptance = tmp_path / 'acceptance.json'
    acceptance.write_text('{}')
    (tmp_path / 'runtime-launch.json').write_text(json.dumps({'acceptance_receipt': str(acceptance)}))
    code = tmp_path / 'code'
    code.mkdir()
    output = tmp_path / 'result'
    monkeypatch.setattr(trial, 'verify_source_release', lambda *a: {'source_sha256': 'fixture'})
    monkeypatch.setattr(trial, 'verify_release_bundles', lambda *a: None)
    monkeypatch.setattr(trial, 'runtime_identity', lambda: {})
    monkeypatch.setattr(trial, '_environment', lambda *a: {})
    monkeypatch.setattr(trial, '_wait_for_idle_gpus', lambda **k: None)
    monkeypatch.setattr(trial, '_gpu_lease_command', lambda command, *a, **k: command)
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(trial.subprocess, 'run', run)
    monkeypatch.setattr(sys, 'argv', ['run_frontier_training_trial.py', '--code-root', str(code),
        '--source-receipt', str(tmp_path / 'release.json'), '--baseline-config', str(base),
        '--output', str(output), '--name', 'resource-policy', *limits])
    assert trial.main() == 0
    benchmark = calls[1]
    assert Path(benchmark[1]).name == 'benchmark_tw_day_trade_batch_sizes.py'
    assert float(benchmark[benchmark.index('--min-vram-headroom-gib') + 1]) == expected[0]
    assert float(benchmark[benchmark.index('--max-peak-vram-fraction') + 1]) == expected[1]
    assert '--require-cuda' in calls[0] and '--strict' in calls[0]
    spec = json.loads((output / 'run-spec.json').read_text())
    assert spec['vram_admission_policy'] == {'min_headroom_gib': expected[0], 'max_peak_fraction': expected[1]}
    assert spec['formal_training_modified'] is False


@pytest.mark.parametrize('flag,value', [('--min-vram-headroom-gib', '-1'),
    ('--min-vram-headroom-gib', 'nan'), ('--max-peak-vram-fraction', '1.01'),
    ('--max-peak-vram-fraction', '0')])
def test_invalid_resource_policy_rejected_before_source_or_cuda(tmp_path, monkeypatch, flag, value):
    from scripts import run_frontier_training_trial as trial
    def forbidden(*args):
        pytest.fail('invalid policy reached source or CUDA admission')
    monkeypatch.setattr(trial, 'verify_source_release', forbidden)
    monkeypatch.setattr(sys, 'argv', ['run_frontier_training_trial.py', '--code-root', str(tmp_path),
        '--source-receipt', str(tmp_path / 'release.json'), '--baseline-config', str(tmp_path / 'config.yaml'),
        '--output', str(tmp_path / 'result'), '--name', 'invalid-resource-policy', flag, value])
    with pytest.raises(SystemExit, match='2'):
        trial.main()
