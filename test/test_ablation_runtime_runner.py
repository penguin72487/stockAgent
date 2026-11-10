import json

import pytest

from scripts.run_ablation_runtime_runner import case_for_config, environment_for_case, main


def context(root):
    def env(prefetch):
        return dict(STOCKAGENT_ANNUAL_BATCH_DEVICE_CACHE_BYTES='536870912',
            STOCKAGENT_DAY_TRADE_DEVICE_PREFETCH=prefetch,
            STOCKAGENT_DAY_TRADE_DEVICE_PREFETCH_ROWS='32')
    return dict(contract='ablation_case_transport_environment_v1', output_root=str(root),
        cases=['temporal_layers__one','baseline'], first_case='temporal_layers__one',
        first_environment=env('1'), default_environment=env('0'))


@pytest.mark.parametrize('relative', ['generated_configs/temporal_layers__one.yaml',
    '_capacity/temporal_layers__one/attempt-00/config.yaml',
    '_capacity/temporal_layers__one/attempt-03/config.yaml'])
def test_first_case_remains_scoped_across_capacity_attempts(tmp_path, relative):
    path=tmp_path/relative;path.parent.mkdir(parents=True);path.touch()
    assert case_for_config(path,tmp_path,context(tmp_path)['cases'])=='temporal_layers__one'


def test_nonpriority_case_preserves_other_runtime_and_disables_prefetch(tmp_path):
    result=environment_for_case(context(tmp_path),'baseline',{'OTHER':'kept',
        'STOCKAGENT_DAY_TRADE_DEVICE_PREFETCH':'1'})
    assert result['OTHER']=='kept' and result['STOCKAGENT_DAY_TRADE_DEVICE_PREFETCH']=='0'


def test_changed_capacity_config_uses_the_default_transport_profile(tmp_path):
    payload=context(tmp_path);payload['first_config_sha256']='measured-config'
    assert environment_for_case(payload,'temporal_layers__one',{},
        config_sha256='new-capacity-config')['STOCKAGENT_DAY_TRADE_DEVICE_PREFETCH']=='0'


@pytest.mark.parametrize('relative', ['generated_configs/unknown.yaml','unmanaged/config.yaml'])
def test_unknown_configs_are_rejected(tmp_path,relative):
    path=tmp_path/relative;path.parent.mkdir(parents=True);path.touch()
    with pytest.raises(ValueError):case_for_config(path,tmp_path,context(tmp_path)['cases'])


def test_runner_forwards_original_training_arguments_and_records_scope(tmp_path,monkeypatch):
    root=tmp_path/'results';root.mkdir()
    config=root/'generated_configs/baseline.yaml';config.parent.mkdir();config.write_text('{}')
    runner=tmp_path/'coda_runner.sh';runner.write_text('#!/bin/bash\n')
    payload=context(root);payload.update(canonical_runner=str(runner),receipt_root=str(tmp_path/'receipts'))
    path=tmp_path/'context.json';path.write_text(json.dumps(payload))
    observed=[]
    monkeypatch.setattr('os.execvpe',lambda executable,argv,env:observed.append((executable,argv,env)))
    forwarded=['-c',str(config),'--','--fold-order','descending','--start-fold','1','--max-folds','10']
    main(['--context',str(path),'--',*forwarded])
    assert observed[0][0]==str(runner)
    assert observed[0][1]==[str(runner),*forwarded]
    assert observed[0][2]['STOCKAGENT_DAY_TRADE_DEVICE_PREFETCH']=='0'
    assert len(list((tmp_path/'receipts/baseline').glob('*.json')))==1
