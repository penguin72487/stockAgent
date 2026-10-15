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


def test_structural_catalog_preserves_all_35_research_cases_and_architectures(tmp_path, dated_manifest):
    spec, rows = queue._experiment_rows(entry.STRUCTURAL_SPEC)
    _, old = queue._experiment_rows(entry.FRONTIER_SPEC)
    assert [r['name'] for r in rows] == [r['name'] for r in old]
    assert [r for r in rows if r['name'] != 'annual_reset__off'] == [r for r in old if r['name'] != 'annual_reset__off']
    assert len(rows) == 35 and rows[0]['name'] == 'baseline'
    spec['matrix']['base_overrides'].setdefault('data', {})['factorized_feature_manifest'] = str(dated_manifest)
    runs = queue._build_configs(entry.STRUCTURAL_SPEC, spec, rows, tmp_path / 'output')
    models = {run['name']: load_config(run['config_path']) for run in runs}
    for name, config in models.items():
        assert config.training.day_trade_annual_episode_sharding == (name != 'annual_reset__off')
        assert config.training.day_trade_annual_episode_world_size == 2
        assert config.training.ddp_broadcast_buffers == (name == 'annual_reset__off')
        assert config.training.ddp_trajectory_gradient_sync == ('batch' if name == 'annual_reset__off' else 'trajectory')
        assert config.training.epochs == 1000
    assert models['baseline'].training.financial_transformer.d_model == 32
    assert models['baseline'].training.lookback == 32
    assert models['embedding__d128'].training.financial_transformer.d_model == 128
    assert models['lookback__d256'].training.lookback == 256
    assert spec['runtime']['capacity_retry']['minimum_train_batch'] == 16
    assert spec['output_root'].endswith('_v9')


def test_structural_listing_never_launches_training(monkeypatch, capsys):
    launch = Mock(side_effect=AssertionError('listing must not train'))
    monkeypatch.setattr(entry.subprocess, 'run', launch)
    assert entry.main(['--structural', '--list']) == 0
    assert 'choices=35' in capsys.readouterr().out
    launch.assert_not_called()


def test_resident_catalog_changes_storage_only_and_preserves_every_case(tmp_path, dated_manifest):
    spec, rows = queue._experiment_rows(entry.RESIDENT_SPEC)
    _, previous = queue._experiment_rows(entry.STRUCTURAL_SPEC)
    assert rows == previous
    spec['matrix']['base_overrides'].setdefault('data', {})['factorized_feature_manifest'] = str(dated_manifest)
    runs = queue._build_configs(entry.RESIDENT_SPEC, spec, rows, tmp_path / 'resident')
    old_spec, old_rows = queue._experiment_rows(entry.STRUCTURAL_SPEC)
    old_spec['matrix']['base_overrides'].setdefault('data', {})['factorized_feature_manifest'] = str(dated_manifest)
    controls = queue._build_configs(entry.STRUCTURAL_SPEC, old_spec, old_rows, tmp_path / 'old')
    assert len(runs) == 35 and [r['name'] for r in runs[:2]] == ['baseline', 'optimizer__muon']
    for run, control in zip(runs, controls, strict=True):
        actual, expected = load_config(run['config_path']), load_config(control['config_path'])
        assert actual.training.financial_transformer.factorized_sparse_projection
        assert actual.training.financial_transformer.factorized_sparse_cache_bytes == 20*1024**3
        assert actual.training.batch_size_train == 128
        assert expected.training.batch_size_train == 256
        for section in ('data','walk_forward','trading','evaluation'):
            assert asdict(getattr(actual,section)) == asdict(getattr(expected,section))
        a, b = asdict(actual.training), asdict(expected.training)
        a.pop('batch_size_train')
        b.pop('batch_size_train')
        for section in ('financial_transformer','executable_portfolio_transformer'):
            for key in ('factorized_sparse_projection','factorized_sparse_cache_bytes'):
                a[section].pop(key)
                b[section].pop(key)
        assert a == b
    assert spec['output_root'].endswith('_v10')
    assert spec['runtime']['capacity_retry'] == old_spec['runtime']['capacity_retry']


def test_resident_list_does_not_start_training(monkeypatch, capsys):
    launch = Mock(side_effect=AssertionError('listing must not train'))
    monkeypatch.setattr(entry.subprocess, 'run', launch)
    assert entry.main(['--resident-sparse','--list']) == 0
    assert 'choices=35' in capsys.readouterr().out
    launch.assert_not_called()


def test_ledger_catalog_keeps_dimensions_and_original_35_cases(tmp_path, dated_manifest):
    spec, rows = queue._experiment_rows(entry.LEDGER_SPEC)
    old_spec, old_rows = queue._experiment_rows(entry.RESIDENT_SPEC)
    assert rows == old_rows and len(rows) == 35
    for value in (spec,old_spec):
        value['matrix']['base_overrides'].setdefault('data',{})['factorized_feature_manifest']=str(dated_manifest)
    runs=queue._build_configs(entry.LEDGER_SPEC,spec,rows,tmp_path/'ledger')
    controls=queue._build_configs(entry.RESIDENT_SPEC,old_spec,old_rows,tmp_path/'resident')
    for run,control in zip(runs,controls,strict=True):
        a,b=load_config(run['config_path']),load_config(control['config_path'])
        for section in ('data','walk_forward','trading','training','evaluation'):
            assert asdict(getattr(a,section))==asdict(getattr(b,section))
    assert spec['output_root'].endswith('_v12')
    recipe=yaml.safe_load((entry.ROOT/spec['optimization_recipe']).read_text())['full_fold_candidates'][0]
    runtime=recipe['runtime_overrides']
    assert runtime['STOCKAGENT_DAY_TRADE_GRAPH_FUSED_STEP']==1
    assert runtime['STOCKAGENT_DAY_TRADE_DEVICE_PREFETCH']==1
    assert runtime['STOCKAGENT_DAY_TRADE_DEVICE_PREFETCH_ROWS']==32
    assert spec['runtime']['capacity_retry']==old_spec['runtime']['capacity_retry']
    launch=Mock(side_effect=AssertionError('listing must not train'))
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(entry.subprocess,'run',launch)
        assert entry.main(['--ledger-graph','--list'])==0
    launch.assert_not_called()


def test_ledger_binding_rejects_missing_financial_or_source_adjoint_evidence(tmp_path,monkeypatch):
    path=tmp_path/'mechanism.json'
    proof=dict(contract='ledger_graph_mechanism_validation_v1',source_sha256='ledger',
        strict_cuda_returncode=0,mechanism_tests_returncode=0,
        canonical_financial_forward_and_adjoint_pass=True,failed_certificate_replays_canonical=True,
        checkpoint_resume_contract_pass=True,prepared_transport_bitwise_pass=True,
        normalized_csr_disk_cache_adjoint_pass=True,
        explicit_graph_contract=dict(version='canonical_terminal_flat_buffered_bounded_adjoint_v2',adjoint='saved'))
    path.write_text(json.dumps(proof))
    binding=dict(source_sha256='ledger',mechanism_validation_receipt=str(path),
                 mechanism_validation_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    measured=Mock(return_value=(binding,tmp_path/'code'))
    monkeypatch.setattr(entry,'_structural_source',measured)
    assert entry._ledger_source({'source_sha256':'resident'},tmp_path/'binding.json')[0]==binding
    assert measured.call_args.kwargs==dict(binding_contract='tw_ablation_ledger_graph_source_v1',
                                          parent_key='parent_resident_source_sha256')
    proof['explicit_graph_contract']['step']='canonical_whole_session_compiled_v1'
    path.write_text(json.dumps(proof))
    binding['mechanism_validation_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError,match='financial/source/adjoint'):
        entry._ledger_source({'source_sha256':'resident'},tmp_path/'binding.json')
    binding['explicit_graph_contract']=dict(proof['explicit_graph_contract'])
    with pytest.raises(ValueError,match='financial/source/adjoint'):
        entry._ledger_source({'source_sha256':'resident'},tmp_path/'binding.json')
    proof['bounded_device_prefetch_bitwise_stream_pass']=True
    path.write_text(json.dumps(proof))
    binding['mechanism_validation_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
    assert entry._ledger_source({'source_sha256':'resident'},tmp_path/'binding.json')[0]==binding
    binding['explicit_graph_contract']={**binding['explicit_graph_contract'],'step':'unverified'}
    with pytest.raises(ValueError,match='unsupported compiled step'):
        entry._ledger_source({'source_sha256':'resident'},tmp_path/'binding.json')
    binding['explicit_graph_contract']=dict(proof['explicit_graph_contract'])
    proof['prepared_transport_bitwise_pass']=False
    path.write_text(json.dumps(proof))
    binding['mechanism_validation_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError,match='financial/source/adjoint'):
        entry._ledger_source({'source_sha256':'resident'},tmp_path/'binding.json')


def test_resident_binding_requires_mechanism_and_unchanged_financial_owners(tmp_path, monkeypatch):
    path = tmp_path/'mechanism.json'
    proof = {'source_sha256':'resident','strict_cuda_returncode':0,
        'mechanism_tests_returncode':0,'mechanism_tests':32,
        'projection_contract':'full_schema_csr_affine_stable_full_rows_gpu_resident_v2',
        'all_parameter_adjoint_pass':True,'financial_owner_files_unchanged':True}
    path.write_text(json.dumps(proof))
    binding = {'source_sha256':'resident','mechanism_validation_receipt':str(path),
        'mechanism_validation_sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
    measured = Mock(return_value=(binding,tmp_path/'build-source'))
    monkeypatch.setattr(entry,'_structural_source',measured)
    assert entry._resident_source({'source_sha256':'v9'},tmp_path/'binding.json')[0] == binding
    assert measured.call_args.kwargs == {'binding_contract':'tw_ablation_resident_source_v1',
        'parent_key':'parent_structural_source_sha256'}
    proof['financial_owner_files_unchanged'] = False
    path.write_text(json.dumps(proof))
    binding['mechanism_validation_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError,match='mechanism/adjoint'):
        entry._resident_source({'source_sha256':'v9'},tmp_path/'binding.json')
    path.write_text('{}')
    with pytest.raises(ValueError,match='validation changed'):
        entry._resident_source({'source_sha256':'v9'},tmp_path/'binding.json')


def test_structural_binding_rejects_unmeasured_runtime_or_false_admission(tmp_path, monkeypatch):
    import stockagent.runtime_identity as identity
    def write(name, value):
        path = tmp_path / name
        path.write_text(json.dumps(value))
        return path
    sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
    old = write('old.json', {'code': {'files': {'stockagent/ledger.py': 'old'}}})
    source = write('source.json', {'code': {'files': {'stockagent/ledger.py': 'new'}}})
    tested = write('tested.json', {'code': {'files': {'stockagent/ledger.py': 'new'}}})
    trials = []
    for index, arm in enumerate(['A', 'B', 'B', 'A']):
        result = write(f'result-{index}.json', {'ok': True, 'lifecycle_ok': True,
            'timing_scope': 'maximum_rank', 'annual_ownership_validation': {'ok': True}})
        trials.append({'arm': arm, 'source_sha256': 'tested',
                       'result': str(result), 'result_sha256': sha(result)})
    proof = {'contract': 'frontier_full_fold_abba_acceptance_v1', 'order': ['A','B','B','A'],
             'selected_fold': 11, 'world_size': 2, 'trials': trials}
    acceptance = write('acceptance.json', proof)
    binding = {'contract': 'tw_ablation_structural_source_v1', 'parent_frontier_source_sha256': 'parent',
        'source_sha256': 'new', 'source_receipt': str(source), 'amendment_files': ['stockagent/ledger.py'],
        'performance_source_sha256': 'tested', 'performance_source_receipt': str(tested),
        'performance_acceptance_receipt': str(acceptance), 'performance_acceptance_sha256': sha(acceptance)}
    path = write('binding.json', binding)
    parent = {'source_sha256': 'parent', 'source_receipt': str(old)}
    monkeypatch.setattr(identity, 'verify_source_release', lambda *a: {'source_sha256': 'new'})
    monkeypatch.setattr(identity, 'verify_release_bundles', lambda *a: None)
    assert entry._structural_source(parent, path)[0]['source_sha256'] == 'new'
    tested.write_text(json.dumps({'code': {'files': {'stockagent/ledger.py': 'different'}}}))
    with pytest.raises(ValueError, match='differs from measured source'):
        entry._structural_source(parent, path)
    tested.write_text(source.read_text())
    result = write('result-1.json', {'ok': True, 'lifecycle_ok': True,
        'timing_scope': 'maximum_rank', 'annual_ownership_validation': {'ok': False}})
    proof['trials'][1]['result_sha256'] = sha(result)
    acceptance = write('acceptance.json', proof)
    binding['performance_acceptance_sha256'] = sha(acceptance)
    write('binding.json', binding)
    with pytest.raises(ValueError, match='failed lifecycle or ownership'):
        entry._structural_source(parent, path)


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
