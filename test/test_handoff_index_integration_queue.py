import json
from types import SimpleNamespace

import pytest

from scripts import run_handoff_index_integration_queue as queue
from test_handoff_index_d_queue import done


def fixed_source(dataset):
    value = done(dataset)
    value['D_publication'].update(new_manifest_sha256='c'*64, new_snapshot_id=dataset+'-private')
    return value


def comparison(dataset):
    return {'contract': queue.planner.CONTRACT, 'dataset': dataset, 'state': 'metadata_compared',
            'private_manifest_sha256': 'c'*64, 'private_snapshot_id': dataset+'-private',
            'public_manifest_sha256': 'd'*64, 'catalog_sha256': 'e'*64,
            'source_payload_read': False, 'semantic_merge_complete': False}


def configure(monkeypatch):
    monkeypatch.setattr(queue.source_cli, 'ENROLLED', {'a-source': ('a', [], False),
                                                       'b-source': ('b', [], False)})
    return {'resolve': lambda *_a, **_kw: SimpleNamespace(manifest_sha256='d'*64),
            'policy': lambda _name: ((), 'e'*64)}


def test_private_only_source_does_not_retry_public_integration(tmp_path, monkeypatch):
    dataset = 'public-economic-history'
    monkeypatch.setattr(queue.source_cli, 'ENROLLED', {'source': (dataset, [], False)})
    (tmp_path/(dataset+'.status.json')).write_text(json.dumps(fixed_source(dataset)))
    def no_public_policy(*_args, **_kwargs):
        raise AssertionError('private-only data has no public integration')
    rows = queue.run_once(tmp_path, now=lambda: 1000, code_sha='x',
                          resolve=no_public_policy, policy=no_public_policy)
    assert rows[0]['state'] == 'not_applicable_private_only'


def test_pinned_direct_public_coverage_needs_no_private_integration(tmp_path, monkeypatch):
    dataset = 'tw-minute-source-workspace'
    monkeypatch.setattr(queue.source_cli, 'ENROLLED', {'source': (dataset, [], False)})
    monkeypatch.setattr(queue.d_queue, '_done', lambda _value, name:name == dataset)
    def no_extra_integration(*_args, **_kwargs):
        raise AssertionError('the pinned public D snapshot already covers this source')
    rows = queue.run_once(tmp_path, now=lambda:1000, code_sha='x',
                          resolve=no_extra_integration, policy=no_extra_integration)
    assert rows[0]['state'] == 'completed'


def test_comparison_queue_requires_fixed_generation_and_retries_changed_head(tmp_path, monkeypatch):
    config = configure(monkeypatch)
    (tmp_path/'a.status.json').write_text(json.dumps(fixed_source('a')))
    (tmp_path/'a.diff-status.json').write_text(json.dumps(comparison('a')))
    rows = queue.run_once(tmp_path, now=lambda: 1000, code_sha='x', **config)
    assert [row['state'] for row in rows] == ['completed', 'waiting_input']
    def runner(command, on_heartbeat, timeout):
        assert command[-2:] == ['--dataset', 'a']
        on_heartbeat()
        updated = comparison('a')
        updated['public_manifest_sha256'] = 'f'*64
        (tmp_path/'a.diff-status.json').write_text(json.dumps(updated))
        return SimpleNamespace(returncode=0, stderr='')
    config['resolve'] = lambda *_a, **_kw: SimpleNamespace(manifest_sha256='f'*64)
    rows = queue.run_once(tmp_path, now=lambda: 1001, runner=runner, code_sha='x', **config)
    assert rows[0]['state'] == 'completed'


def test_comparison_queue_failure_does_not_block_another_dataset(tmp_path, monkeypatch):
    config = configure(monkeypatch)
    for name in ('a', 'b'):
        (tmp_path/(name+'.status.json')).write_text(json.dumps(fixed_source(name)))
    failed = lambda *_a, **_kw: SimpleNamespace(returncode=1, stderr='temporary owner busy')
    rows = queue.run_once(tmp_path, now=lambda: 1000, runner=failed, code_sha='x', **config)
    assert rows[0]['state'] == 'retrying'
    def runner(command, **_kw):
        assert command[-1] == 'b'
        (tmp_path/'b.diff-status.json').write_text(json.dumps(comparison('b')))
        return SimpleNamespace(returncode=0, stderr='')
    rows = queue.run_once(tmp_path, now=lambda: 1001, runner=runner, code_sha='x', **config)
    assert [row['state'] for row in rows] == ['retrying', 'completed']


def test_expired_busy_retry_cannot_starve_another_dataset(tmp_path, monkeypatch):
    config = configure(monkeypatch)
    for name in ('a', 'b'):
        (tmp_path/(name+'.status.json')).write_text(json.dumps(fixed_source(name)))
    failed = lambda *_a, **_kw: SimpleNamespace(returncode=1, stderr='temporary owner busy')
    rows = queue.run_once(tmp_path, now=lambda: 1000, runner=failed, code_sha='x', **config)
    assert rows[0]['state'] == 'retrying' and rows[0]['retry_after_epoch'] == 1015
    def runner(command, **_kw):
        assert command[-1] == 'b'
        (tmp_path/'b.diff-status.json').write_text(json.dumps(comparison('b')))
        return SimpleNamespace(returncode=0, stderr='')
    rows = queue.run_once(tmp_path, now=lambda: 1060, runner=runner, code_sha='x', **config)
    assert [row['state'] for row in rows] == ['waiting', 'completed']


def test_forex_semantic_work_uses_the_existing_queue_and_retries(tmp_path, monkeypatch):
    name = 'forex-frankfurter'
    monkeypatch.setattr(queue.source_cli, 'ENROLLED', {'fx-source': (name, [], False)})
    (tmp_path/(name+'.status.json')).write_text(json.dumps(fixed_source(name)))
    (tmp_path/(name+'.diff-status.json')).write_text(json.dumps(comparison(name)))
    config = {'resolve': lambda *_a, **_kw: SimpleNamespace(manifest_sha256='d'*64),
              'policy': lambda _name: ((), 'e'*64)}
    monkeypatch.setattr(queue, '_forex_audits_ready', lambda *_: True)
    monkeypatch.setattr(queue, '_forex_done', lambda *_a, **_kw: False)
    def failed(command, **_kw):
        assert command[-1].endswith('integrate_frankfurter_fixed_generations.py')
        return SimpleNamespace(returncode=1, stderr='temporary owner busy')
    rows = queue.run_once(tmp_path, now=lambda: 1000, runner=failed, code_sha='x', **config)
    assert rows[0]['state'] == 'retrying' and rows[0]['error'] == 'owner_busy'
    assert rows[0]['retry_after_epoch'] == 1015
    rows = queue.run_once(tmp_path, now=lambda: 1001, runner=failed, code_sha='x', **config)
    assert rows[0]['state'] == 'retrying'
    rows = queue.run_once(tmp_path, now=lambda: 1016, runner=failed, code_sha='x', **config)
    assert rows[0]['state'] == 'retrying' and rows[0]['retry_after_epoch'] == 1031
    monkeypatch.setattr(queue, '_forex_done', lambda *_a, **_kw: True)
    rows = queue.run_once(tmp_path, now=lambda: 1017, runner=failed, code_sha='x', **config)
    assert rows[0]['state'] == 'completed'


def test_forex_completion_survives_only_a_fixed_source_repack(tmp_path):
    root = tmp_path/'migration/handoff-index-archive-v1'
    base = root/'forex-frankfurter'
    base.mkdir(parents=True)
    source = fixed_source('forex-frankfurter')
    receipt = {'contract': 'stockagent.frankfurter_fixed_generation_integration.v1',
        'state': 'public_generation_integrated', 'private_original_manifest_sha256': 'c'*64,
        'new_public_manifest_sha256': 'd'*64, 'public_only_rows': 0, 'conflicting_overlap_rows': 0}
    (base/'frankfurter-semantic-integration-v1.json').write_text(json.dumps(receipt))
    resolve = lambda *_a, **_kw: SimpleNamespace(manifest_sha256='d'*64)
    assert queue._forex_done(root, source, resolve=resolve)
    new = 'f'*64
    resolve = lambda *_a, **_kw: SimpleNamespace(manifest_sha256=new)
    assert not queue._forex_done(root, source, resolve=resolve)
    repack = root.parent/'source-local-exact-repack/forex-frankfurter'/(new+'.json')
    repack.parent.mkdir(parents=True)
    repack.write_text(json.dumps({'contract': 'stockagent.source_local_exact_repack.v1',
        'dataset': 'forex-frankfurter', 'old_manifest_sha256': 'd'*64, 'new_manifest_sha256': new,
        'state': 'new_D_head_committed_from_exact_E_source'}))
    assert queue._forex_done(root, source, resolve=resolve)


def test_archive_semantic_queue_runs_controls_then_promotion(tmp_path, monkeypatch):
    name = 'binance-public-archive'
    monkeypatch.setattr(queue.source_cli, 'ENROLLED', {'archive-source': (name, [], False)})
    (tmp_path/(name+'.status.json')).write_text(json.dumps(fixed_source(name)))
    (tmp_path/(name+'.diff-status.json')).write_text(json.dumps(comparison(name)))
    config = {'resolve': lambda *_a, **_kw: SimpleNamespace(manifest_sha256='d'*64),
              'policy': lambda _name: ((), 'e'*64)}
    flags = {'rows': True, 'controls': False, 'promotion': False}
    monkeypatch.setattr(queue, '_archive_needs_audit', lambda *_: True)
    monkeypatch.setattr(queue, '_archive_audit_done', lambda *_: flags['rows'])
    monkeypatch.setattr(queue, '_archive_controls_done', lambda *_: flags['controls'])
    monkeypatch.setattr(queue, '_archive_promotion_done', lambda *_a, **_kw: flags['promotion'])
    def runner(command, **_kwargs):
        if command[-1].endswith('audit_binance_archive_controls.py'):
            flags['controls'] = True
        elif command[-1].endswith('promote_binance_archive_semantic.py'):
            flags['promotion'] = True
        else:
            raise AssertionError(command)
        return SimpleNamespace(returncode=0, stderr='')
    first = queue.run_once(tmp_path, now=lambda: 1000, runner=runner, code_sha='x', **config)
    assert first[0]['state'] == 'completed' and first[0]['work'] == 'archive_controls'
    second = queue.run_once(tmp_path, now=lambda: 1001, runner=runner, code_sha='x', **config)
    assert second[0]['state'] == 'completed' and second[0]['work'] == 'archive_promotion'


def test_archive_public_only_paths_stop_impossible_promotion_without_starving_queue(tmp_path, monkeypatch):
    name = 'binance-public-archive'
    monkeypatch.setattr(queue.source_cli, 'ENROLLED', {'archive-source': (name, [], False)})
    (tmp_path/(name+'.status.json')).write_text(json.dumps(fixed_source(name)))
    plan = comparison(name)
    plan['counters'] = {'public_only': 3}
    (tmp_path/(name+'.diff-status.json')).write_text(json.dumps(plan))
    monkeypatch.setattr(queue, '_archive_needs_audit', lambda *_: True)
    monkeypatch.setattr(queue, '_archive_audit_done', lambda *_: True)
    monkeypatch.setattr(queue, '_archive_controls_done', lambda *_: True)
    monkeypatch.setattr(queue, '_archive_promotion_done', lambda *_a, **_kw: False)
    def no_promotion(*_args, **_kwargs):
        raise AssertionError('the fixed-private promotion cannot keep public-only paths')
    rows = queue.run_once(tmp_path, now=lambda: 1000, runner=no_promotion, code_sha='x',
        resolve=lambda *_a, **_kw: SimpleNamespace(manifest_sha256='d'*64),
        policy=lambda _name: ((), 'e'*64))
    assert rows[0]['state'] == 'blocked_semantic_merge' and rows[0]['work'] == 'archive_union_required'
    # A later changed public head is re-planned rather than hidden by this block.
    rows = queue.run_once(tmp_path, now=lambda: 1001, runner=lambda *_a, **_kw:
        SimpleNamespace(returncode=1, stderr='owner busy'), code_sha='x',
        resolve=lambda *_a, **_kw: SimpleNamespace(manifest_sha256='f'*64),
        policy=lambda _name: ((), 'e'*64))
    assert rows[0].get('work') != 'archive_union_required'
    assert rows[0]['state'] == 'retrying'


@pytest.mark.parametrize('name', ['binance', 'yahoo-market', 'legacy-parquet', 'free-public-context', 'forex-pepperstone'])
def test_latest_fixed_selection_uses_existing_queue_and_is_idempotent(tmp_path, monkeypatch, name):
    monkeypatch.setattr(queue.source_cli, 'ENROLLED', {'source': (name, [], False)})
    (tmp_path/(name+'.status.json')).write_text(json.dumps(fixed_source(name)))
    (tmp_path/(name+'.diff-status.json')).write_text(json.dumps(comparison(name)))
    config = {'resolve': lambda *_a, **_kw: SimpleNamespace(manifest_sha256='d'*64),
              'policy': lambda _name: ((), 'e'*64)}
    flags = {'completed': False}
    monkeypatch.setattr(queue, '_latest_enabled', lambda *_: True)
    monkeypatch.setattr(queue, '_latest_public_binding_matches', lambda *_: True)
    monkeypatch.setattr(queue, '_latest_promotion_done', lambda *_a, **_kw: flags['completed'])
    def runner(command, **_kwargs):
        assert command[1].endswith('promote_latest_fixed_generation.py') and command[-2:] == ['--dataset', name]
        flags['completed'] = True
        return SimpleNamespace(returncode=0, stderr='')
    first = queue.run_once(tmp_path, now=lambda: 1000, runner=runner, code_sha='x', **config)
    assert first[0]['state'] == 'completed' and first[0]['work'] == 'latest_fixed_promotion'
    def no_repeat(*_args, **_kwargs):
        raise AssertionError('a committed generation must not be promoted again')
    second = queue.run_once(tmp_path, now=lambda: 1001, runner=no_repeat, code_sha='x', **config)
    assert second[0]['state'] == 'completed' and 'work' not in second[0]


def test_latest_selection_never_reselects_old_fixed_source_after_public_head_advances(tmp_path, monkeypatch):
    name = 'binance'
    monkeypatch.setattr(queue.source_cli, 'ENROLLED', {'source': (name, [], False)})
    source = fixed_source(name)
    (tmp_path/(name+'.status.json')).write_text(json.dumps(source))
    current = comparison(name); current['public_manifest_sha256'] = 'f'*64
    (tmp_path/(name+'.diff-status.json')).write_text(json.dumps(current))
    base = tmp_path/name; base.mkdir()
    (base/('latest-promotion-'+'a'*64+'.json')).write_text(json.dumps({
        'contract': 'stockagent.latest_fixed_link_promotion.v1', 'state': 'committed',
        'new_manifest_sha256': 'd'*64, 'private_manifest_sha256': 'c'*64,
        'changed_member_values_compared': False, 'old_historical_value_coverage': 'not_compared'}))
    resolve = lambda *_a, **_kw: SimpleNamespace(manifest_sha256='f'*64)
    assert not queue._latest_promotion_done(tmp_path, name, source, resolve=resolve)
    assert queue._latest_promotion_done(tmp_path, name, source, resolve=resolve, allow_advanced=True)
    monkeypatch.setattr(queue, '_latest_enabled', lambda *_: True)
    rows = queue.run_once(tmp_path, now=lambda: 1000, code_sha='x', resolve=resolve,
        policy=lambda _: ((), 'e'*64), runner=lambda *_a, **_kw: (_ for _ in ()).throw(
            AssertionError('a prior fixed source must not replace an advanced public head')))
    assert rows[0]['state'] == 'completed' and 'work' not in rows[0]


def test_latest_selection_yields_when_original_public_binding_has_changed(tmp_path, monkeypatch):
    name = 'okx'
    monkeypatch.setattr(queue.source_cli, 'ENROLLED', {'source': (name, [], False)})
    (tmp_path/(name+'.status.json')).write_text(json.dumps(fixed_source(name)))
    (tmp_path/(name+'.diff-status.json')).write_text(json.dumps(comparison(name)))
    monkeypatch.setattr(queue, '_latest_enabled', lambda *_: True)
    monkeypatch.setattr(queue, '_latest_public_binding_matches', lambda *_: False)
    monkeypatch.setattr(queue, '_latest_promotion_done', lambda *_a, **_kw: False)
    rows = queue.run_once(tmp_path, now=lambda: 1000, code_sha='x',
        resolve=lambda *_a, **_kw: SimpleNamespace(manifest_sha256='d'*64),
        policy=lambda _: ((), 'e'*64), runner=lambda *_a, **_kw: (_ for _ in ()).throw(
            AssertionError('changed public generation must not be replaced')))
    assert rows[0]['state'] == 'waiting_generation_changed' and 'work' not in rows[0]
