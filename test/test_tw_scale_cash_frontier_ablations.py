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


def test_frontier_recipes_follow_baseline_and_preserve_original_catalog(tmp_path, dated_manifest):
    spec, rows = queue._experiment_rows(entry.FRONTIER_SPEC)
    _, old = queue._experiment_rows(entry.OPTIMIZED_SPEC)
    assert rows[0] == old[0] and rows[8:] == old[1:]
    assert [r['name'] for r in rows[:8]] == ['baseline', 'optimizer__muon', 'optimizer__normuon',
        'optimizer__dion3', 'optimizer__soap', 'optimizer__ademamix',
        'optimizer__muon_ademamix', 'optimizer__muon_lr2']
    assert len(rows) == 35
    screen = yaml.safe_load((entry.ROOT / 'configs/benchmarks/frontier_training_techniques_20261014.yaml').read_text())
    recipes = {r['name']: r['overrides'] for r in screen['trials']}
    for row in rows[1:8]:
        assert row['overrides'] == recipes[row['name'].removeprefix('optimizer__')]
    spec['matrix']['base_overrides'] = {'data': {'factorized_feature_manifest': str(dated_manifest)}}
    runs = queue._build_configs(entry.FRONTIER_SPEC, spec, rows, tmp_path / 'output')
    baseline = load_config(runs[0]['config_path'])
    for run in runs[1:8]:
        config = load_config(run['config_path'])
        for section in ('data', 'walk_forward', 'trading', 'evaluation'):
            assert asdict(getattr(config, section)) == asdict(getattr(baseline, section))
        expected, actual = asdict(baseline.training), asdict(config.training)
        for key in ('optimizer_name', 'optimizer_options'):
            expected.pop(key); actual.pop(key)
        assert actual == expected
        assert config.training.epochs == 1000 and config.runner.start_fold == baseline.runner.start_fold
    models = {r['name']: load_config(r['config_path']) for r in runs}
    assert models['embedding__d128'].training.financial_transformer.d_model == 128
    assert models['lookback__d256'].training.lookback == 256
    assert not models['baseline'].training.optimizer_options
    assert spec['output_root'] != queue._load_ablation_spec(entry.OPTIMIZED_SPEC)['output_root']


def test_frontier_selection_keeps_catalog_order_and_list_never_launches(monkeypatch, capsys):
    run = Mock(side_effect=AssertionError('listing must not launch training'))
    monkeypatch.setattr(entry.subprocess, 'run', run)
    assert entry.main(['--frontier', '--list', '--only', 'optimizer__soap,baseline,optimizer__muon']) == 0
    output = capsys.readouterr().out
    assert output.index('baseline') < output.index('optimizer__muon') < output.index('optimizer__soap')
    run.assert_not_called()


def test_frontier_binding_requires_validated_runtime_and_exact_dependencies(tmp_path, monkeypatch):
    import stockagent.runtime_identity as identity
    def write(name, value):
        path = tmp_path / name
        path.write_text(json.dumps(value))
        return path
    old = write('old.json', {'code': {'files': {'stockagent/model.py': 'same', 'scripts/queue.py': 'old'}}})
    source = write('source.json', {'code': {'files': {'stockagent/model.py': 'same', 'scripts/queue.py': 'new'}}})
    validated = write('validated.json', {'code': {'files': {'stockagent/model.py': 'same', 'scripts/queue.py': 'tested'}}})
    validation = write('validation.json', {'source_sha256': 'tested', 'torch_version': '2.11',
        'optimizer_tests': 20, 'optimizer_tests_returncode': 0, 'strict_cuda_returncode': 0})
    deps = tmp_path / 'deps'; deps.mkdir()
    provenance = deps / 'direct_url.json'; provenance.write_text('pinned')
    sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
    binding = {'contract': 'tw_ablation_frontier_source_v1', 'parent_optimized_source_sha256': 'parent',
        'source_receipt': str(source), 'source_sha256': 'new', 'amendment_files': ['scripts/queue.py'],
        'optimizer_validation_receipt': str(validation), 'optimizer_validation_sha256': sha(validation),
        'validated_optimizer_source_sha256': 'tested', 'validated_optimizer_source_receipt': str(validated),
        'dependency_root': str(deps), 'dependency_files': {'direct_url.json': sha(provenance)}}
    path = write('binding.json', binding)
    parent = {'source_sha256': 'parent', 'source_receipt': str(old), 'kernel_versions': {'torch': '2.11'}}
    monkeypatch.setattr(identity, 'verify_source_release', lambda *args: {'source_sha256': 'new'})
    monkeypatch.setattr(identity, 'verify_release_bundles', lambda *args: None)
    observed, code = entry._frontier_source(parent, path)
    assert observed['source_sha256'] == 'new' and code == source.parent / 'build-source'
    source.write_text(json.dumps({'code': {'files': {'stockagent/model.py': 'wrong', 'scripts/queue.py': 'new'}}}))
    binding['amendment_files'] = ['scripts/queue.py', 'stockagent/model.py']; write('binding.json', binding)
    with pytest.raises(ValueError, match='differs from validated'):
        entry._frontier_source(parent, path)
    source.write_text(json.dumps({'code': {'files': {'stockagent/model.py': 'same', 'scripts/queue.py': 'new'}}}))
    binding['amendment_files'] = ['scripts/queue.py']; write('binding.json', binding)
    provenance.write_text('changed')
    with pytest.raises(ValueError, match='dependency changed'):
        entry._frontier_source(parent, path)


def test_prepend_dimensions_is_fail_closed_and_reuses_inherited_dimensions(tmp_path):
    bad = tmp_path / 'bad.yaml'
    bad.write_text(yaml.safe_dump({'base_spec': str(entry.OPTIMIZED_SPEC),
        'matrix': {'prepend_dimensions': {'invalid': 'mapping'}}}))
    with pytest.raises(ValueError, match='prepend_dimensions must be a list'):
        queue._experiment_rows(bad)
