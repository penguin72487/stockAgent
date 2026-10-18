"""A recipe must not inherit another variant's experiment settings."""
from pathlib import Path
import subprocess
import sys

import pytest
import yaml

from scripts import run_frontier_fold_queue as queue


def test_each_recipe_binds_its_full_effective_config(tmp_path, monkeypatch):
    constant = tmp_path/'constant.yaml'
    linear = tmp_path/'linear.yaml'
    constant.write_text('mode: constant\n')
    linear.write_text('mode: linear\n')
    spec = tmp_path/'queue.yaml'
    spec.write_text(yaml.safe_dump({'full_fold_candidates': [
        {'name': 'constant', 'overrides': {}},
        {'name': 'linear', 'baseline_config': str(linear), 'overrides': {}},
    ]}))
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(queue.subprocess, 'run', run)
    monkeypatch.setattr(sys, 'argv', ['queue', '--spec', str(spec),
        '--baseline-config', str(constant), '--output', str(tmp_path/'results')])
    assert queue.main() == 0
    configs = [Path(cmd[cmd.index('--baseline-config')+1]) for cmd in commands]
    assert configs == [constant, linear]
    assert [yaml.safe_load(p.read_text())['mode'] for p in configs] == ['constant', 'linear']


def test_missing_recipe_config_fails_before_launch(tmp_path, monkeypatch):
    default = tmp_path/'default.yaml'
    default.write_text('{}\n')
    spec = tmp_path/'queue.yaml'
    spec.write_text(yaml.safe_dump({'full_fold_candidates': [
        {'name': 'broken', 'baseline_config': str(tmp_path/'missing.yaml'), 'overrides': {}},
    ]}))
    monkeypatch.setattr(sys, 'argv', ['queue', '--spec', str(spec),
        '--baseline-config', str(default), '--output', str(tmp_path/'results')])
    with pytest.raises(ValueError, match='baseline config is missing'):
        queue.main()
