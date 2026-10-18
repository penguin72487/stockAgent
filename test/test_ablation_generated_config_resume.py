"""Repreparing a selected case must preserve its capacity-resume identity."""
from pathlib import Path

import pytest
import yaml

from scripts.run_ablation_experiments import _build_configs, _capacity_run


@pytest.mark.parametrize('historical_suffixes', [1, 3])
def test_self_based_case_preparation_preserves_selected_attempt(tmp_path, historical_suffixes):
    repo = Path(__file__).resolve().parents[1]
    row = dict(name='baseline', overrides={}, description='selected case')
    spec = dict(base_config=str(repo/'configs/experiment_baseline.yaml'))
    run = _build_configs(tmp_path/'spec.yaml', spec, [row], tmp_path)[0]
    path = run['config_path']
    raw = yaml.safe_load(path.read_text())
    raw['experiment_name'] = 'existing-policy'+'-ablation-baseline'*historical_suffixes
    path.write_text(yaml.safe_dump(raw, sort_keys=False, allow_unicode=True))
    selected = _capacity_run(run, {})
    original = path.read_bytes()
    selection = tmp_path/'_capacity/baseline/selection.json'
    receipt = selection.read_bytes()

    # Preparation may run once for review and again for execution or recovery.
    formal = dict(base_config=str(path))
    for _ in range(3):
        resumed = _build_configs(tmp_path/'formal.yaml', formal, [row], tmp_path)[0]
        assert path.read_bytes() == original
        attempt = _capacity_run(resumed, {})
        assert attempt['config_path'] == selected['config_path']
        assert attempt['output_dir'] == selected['output_dir']
        assert selection.read_bytes() == receipt

    # Stabilizing the name must never authorize incompatible training changes.
    changed = {**row, 'overrides': {'training': {'learning_rate': 0.123}}}
    incompatible = _build_configs(tmp_path/'formal.yaml', formal, [changed], tmp_path)[0]
    with pytest.raises(ValueError, match='different experiment config'):
        _capacity_run(incompatible, {})
    assert selection.read_bytes() == receipt
