from contextlib import nullcontext
import hashlib
import json
from pathlib import Path

import pytest

from stockagent.data_sync import packed_snapshots as packed, semantic_link_promotion as promotion
from stockagent.data_sync import semantic_link_retirement as retirement, stream_repack, superseded_repack_history
from stockagent.data_sync.desync_snapshots import SnapshotError
from test_semantic_link_promotion import build_latest_candidate, candidate, latest_candidate, latest_with_shared_object  # noqa: F401


@pytest.fixture
def ready(candidate, tmp_path, monkeypatch):
    cold, private, old, new, options = candidate
    promoted = promotion.promote(cold, private, **options)
    fixed = {**options['plan'], 'comparison_scope': 'fixed_public_snapshot',
             'public_snapshot_id': old.manifest['snapshot_id']}
    fixed['identity_sha256'] = stream_repack._identity(fixed)
    old_ref = old.manifest_path.relative_to(cold).as_posix()
    shared = old.manifest['archive']['inventory']['relpath']
    monkeypatch.setattr(retirement.r, '_boundary', lambda *_a, **_kw: nullcontext())
    monkeypatch.setattr(retirement.old_retirement, 'catalog_index', lambda *_a, **_kw: ({}, {}, 'catalog'))
    monkeypatch.setattr(retirement, '_private_catalog_generation', lambda *_: 'same')
    monkeypatch.setattr(retirement.r, 'catalog_references', lambda _c, _root, _m, targets, _g:
        {value: {old_ref, 'manifests/other/retained.json'} if value == shared else {old_ref} for value in targets})
    monkeypatch.setattr(retirement.r, 'fresh_manifest_references', lambda *_: set())
    monkeypatch.setattr(retirement.r, 'include_other_authority_repack_pins', lambda _c, _m, known: known)
    monkeypatch.setattr(retirement.r, '_histories', lambda *_: [])
    monkeypatch.setattr(retirement.packed_retention, '_local_protected_snapshot_ids', lambda *_a, **_kw: (set(), {}, []))
    monkeypatch.setattr(retirement, 'process_references_many', lambda *_: [])
    kwargs = {'promotion': promoted, 'plan': fixed, 'rows': options['rows'], 'controls': options['controls'],
        'catalog_sha': options['catalog_sha'], 'journal_path': tmp_path/'E/retirement.json',
        'catalog': tmp_path/'catalog', 'observation': tmp_path/'observation', 'cache': tmp_path/'cache',
        'materialized': tmp_path/'materialized', 'retention_state': tmp_path/'retention'}
    return cold, private, old, new, shared, kwargs


def test_retirement_dry_run_and_apply_preserve_new_head_and_shared_objects(ready):
    cold, private, old, new, shared, kwargs = ready
    dry = retirement.retire(cold, private, apply=False, **kwargs)
    assert dry['state'] == 'planned' and old.manifest_path.exists()
    assert shared not in {item['relative'] for item in dry['plan']['items']}
    result = retirement.retire(cold, private, apply=True, **kwargs)
    assert result['state'] == 'completed' and not old.manifest_path.exists()
    assert (cold/shared).exists(), 'another manifest still refers to this object'
    current = packed.resolve_latest_packed(cold, 'binance-public-archive', require_objects=True)
    assert current.manifest_sha256 == kwargs['promotion']['new_manifest_sha256']
    assert retirement.retire(cold, private, apply=True, **kwargs)['state'] == 'completed'


@pytest.mark.parametrize('defect', [None, 'retirement', 'worklist', 'manifest', 'old_returned', 'missing_object'])
def test_semantic_successor_requires_exact_completed_handoff(ready, tmp_path, defect):
    cold, private, old, _new, _shared, kwargs = ready
    promoted = kwargs['promotion']
    identity = promoted['comparison_identity_sha256']
    migration = tmp_path/'workspace/migration'
    stage = migration/'stream-source-repack-v1/binance-public-archive'/('a'*64)
    stage.mkdir(parents=True)
    evidence = migration/'handoff-index-archive-v1/binance-public-archive'
    diff_dir = evidence/'fixed/integration-diff-v1'
    diff_dir.mkdir(parents=True)
    payload = b'exact worklist'
    plan = {**kwargs['plan'], 'worklist': {'name': 'worklist.gz', 'sha256': hashlib.sha256(payload).hexdigest()}}
    plan['identity_sha256'] = stream_repack._identity(plan)
    kwargs['plan'] = plan
    kwargs['journal_path'] = evidence/f'semantic-retirement-{identity}.json'
    outcome = retirement.retire(cold, private, apply=True, **kwargs)
    assert outcome['state'] == 'completed'
    promoted['retirement'] = {'state': 'completed', 'apply': True, 'dataset': 'binance-public-archive',
        'old_manifest_sha256': old.manifest_sha256, 'new_manifest_sha256': promoted['new_manifest_sha256']}
    (evidence/f'semantic-promotion-{identity}.json').write_text(json.dumps(promoted))
    (evidence/f'row-audit-{identity}.json').write_text(json.dumps(kwargs['rows']))
    (evidence/f'control-audit-{identity}.json').write_text(json.dumps(kwargs['controls']))
    (diff_dir/f"diff-plan-{plan['identity_sha256']}.json").write_text(json.dumps(plan))
    (diff_dir/'worklist.gz').write_bytes(payload)
    if defect == 'retirement':
        path = kwargs['journal_path']
        journal = json.loads(path.read_text())
        journal['removed_count'] -= 1
        path.write_text(json.dumps(journal))
    elif defect == 'worklist':
        (diff_dir/'worklist.gz').write_bytes(b'changed')
    elif defect == 'manifest':
        path = evidence/f'semantic-promotion-{identity}.json'
        altered = json.loads(path.read_text())
        altered['manifest']['metadata']['changed'] = True
        path.write_text(json.dumps(altered))
    elif defect == 'old_returned':
        old.manifest_path.write_text(json.dumps(old.manifest))
    elif defect == 'missing_object':
        (cold/promoted['manifest']['archive']['objects'][0]['relpath']).unlink()
    if defect is None:
        result = superseded_repack_history._semantic_successor(
            cold, stage, 'binance-public-archive', old.manifest_sha256, old.manifest['snapshot_id'])
        assert result['manifest_sha256'] == promoted['new_manifest_sha256']
        assert result['snapshot_id'] == promoted['new_snapshot_id']
    else:
        with pytest.raises((SnapshotError, OSError, ValueError)):
            superseded_repack_history._semantic_successor(
                cold, stage, 'binance-public-archive', old.manifest_sha256, old.manifest['snapshot_id'])


def test_retirement_refuses_a_research_pin_before_unlink(ready, monkeypatch):
    cold, private, old, new, shared, kwargs = ready
    monkeypatch.setattr(retirement.packed_retention, '_local_protected_snapshot_ids',
                        lambda *_a, **_kw: ({old.manifest['snapshot_id']}, {}, []))
    with pytest.raises(SnapshotError, match='pinned'):
        retirement.retire(cold, private, apply=True, **kwargs)
    assert old.manifest_path.exists() and not kwargs['journal_path'].exists()


def test_latest_retirement_is_explicit_about_uncompared_values_and_preserves_pins(latest_candidate, tmp_path, monkeypatch):
    cold, private, old, new, options = latest_candidate
    promoted = promotion.promote(cold, private, **options)
    old_ref = old.manifest_path.relative_to(cold).as_posix()
    monkeypatch.setattr(retirement.r, '_boundary', lambda *_a, **_kw: nullcontext())
    monkeypatch.setattr(retirement.old_retirement, 'catalog_index', lambda *_a, **_kw: ({}, {}, 'catalog'))
    monkeypatch.setattr(retirement, '_private_catalog_generation', lambda *_: 'same')
    monkeypatch.setattr(retirement.r, 'catalog_references', lambda _c, _root, _m, targets, _g:
                        {value: {old_ref} for value in targets})
    monkeypatch.setattr(retirement.r, 'fresh_manifest_references', lambda *_: set())
    monkeypatch.setattr(retirement.r, 'include_other_authority_repack_pins', lambda _c, _m, known: known)
    monkeypatch.setattr(retirement.r, '_histories', lambda *_: [])
    monkeypatch.setattr(retirement.packed_retention, '_local_protected_snapshot_ids', lambda *_a, **_kw: (set(), {}, []))
    monkeypatch.setattr(retirement, 'process_references_many', lambda *_: [])
    kwargs = {'promotion': promoted, 'plan': options['plan'], 'rows': {}, 'controls': {},
        'latest_policy': options['latest_policy'], 'catalog_sha': 'c'*64,
        'journal_path': tmp_path/'E/retirement.json', 'catalog': tmp_path/'catalog',
        'observation': tmp_path/'observation', 'cache': tmp_path/'cache',
        'materialized': tmp_path/'materialized', 'retention_state': tmp_path/'retention'}
    dry = retirement.retire(cold, private, apply=False, **kwargs)
    assert dry['contract'] == retirement.LATEST_CONTRACT and dry['old_historical_value_coverage'] == 'not_compared'
    assert old.manifest_path.exists()
    monkeypatch.setattr(retirement.packed_retention, '_local_protected_snapshot_ids',
                        lambda *_a, **_kw: ({old.manifest['snapshot_id']}, {}, []))
    with pytest.raises(SnapshotError, match='pinned'):
        retirement.retire(cold, private, apply=True, **kwargs)
    assert old.manifest_path.exists()
    monkeypatch.setattr(retirement.packed_retention, '_local_protected_snapshot_ids', lambda *_a, **_kw: (set(), {}, []))
    result = retirement.retire(cold, private, apply=True, **kwargs)
    assert result['state'] == 'completed' and result['changed_member_values_compared'] is False
    assert not old.manifest_path.exists()
    current = packed.resolve_latest_packed(cold, 'binance', require_objects=True)
    assert current.manifest_sha256 == promoted['new_manifest_sha256']


def test_yahoo_latest_retirement_preserves_research_pins_and_new_generation(tmp_path, monkeypatch):
    cold, private, old, _new, options = build_latest_candidate(tmp_path, monkeypatch, 'yahoo-market')
    promoted = promotion.promote(cold, private, **options)
    old_ref = old.manifest_path.relative_to(cold).as_posix()
    monkeypatch.setattr(retirement.r, '_boundary', lambda *_a, **_kw: nullcontext())
    monkeypatch.setattr(retirement.old_retirement, 'catalog_index', lambda *_a, **_kw: ({}, {}, 'catalog'))
    monkeypatch.setattr(retirement, '_private_catalog_generation', lambda *_: 'same')
    monkeypatch.setattr(retirement.r, 'catalog_references', lambda _c, _root, _m, targets, _g:
                        {value: {old_ref} for value in targets})
    monkeypatch.setattr(retirement.r, 'fresh_manifest_references', lambda *_: set())
    monkeypatch.setattr(retirement.r, 'include_other_authority_repack_pins', lambda _c, _m, known: known)
    monkeypatch.setattr(retirement.r, '_histories', lambda *_: [])
    monkeypatch.setattr(retirement, 'process_references_many', lambda *_: [])
    monkeypatch.setattr(retirement.packed_retention, '_local_protected_snapshot_ids',
                        lambda *_a, **_kw: ({old.manifest['snapshot_id']}, {}, []))
    kwargs = {'promotion':promoted, 'plan':options['plan'], 'rows':{}, 'controls':{},
        'latest_policy':options['latest_policy'], 'catalog_sha':options['catalog_sha'],
        'journal_path':tmp_path/'E/retirement.json', 'catalog':tmp_path/'catalog',
        'observation':tmp_path/'observation', 'cache':tmp_path/'cache',
        'materialized':tmp_path/'materialized', 'retention_state':tmp_path/'retention'}
    with pytest.raises(SnapshotError, match='pinned'):
        retirement.retire(cold, private, apply=True, **kwargs)
    assert old.manifest_path.exists()
    monkeypatch.setattr(retirement.packed_retention, '_local_protected_snapshot_ids',
                        lambda *_a, **_kw: (set(), {}, []))
    result = retirement.retire(cold, private, apply=True, **kwargs)
    assert result['state'] == 'completed' and result['old_historical_value_coverage'] == 'not_compared'
    assert not old.manifest_path.exists()
    current = packed.resolve_latest_packed(cold, 'yahoo-market', require_objects=True)
    assert current.manifest_sha256 == promoted['new_manifest_sha256']
    assert packed.read_packed_files(cold, current, names=['rates'], maximum_bytes=100,
                                    maximum_object_bytes=1024**2)['rates'] == b'newer but different values'


def test_latest_retirement_keeps_a_shared_existing_public_object(latest_with_shared_object, tmp_path, monkeypatch):
    cold, private, old, new, options = latest_with_shared_object
    promoted = promotion.promote(cold, private, **options)
    shared, = [item['sha256'] for item in promoted['linked'] if item.get('mode') == 'existing_public_same_sha']
    descriptor, = [item for item in new.manifest['archive']['objects'] if item['sha256'] == shared]
    old_ref = old.manifest_path.relative_to(cold).as_posix()
    monkeypatch.setattr(retirement.r, '_boundary', lambda *_a, **_kw: nullcontext())
    monkeypatch.setattr(retirement.old_retirement, 'catalog_index', lambda *_a, **_kw: ({}, {}, 'catalog'))
    monkeypatch.setattr(retirement, '_private_catalog_generation', lambda *_: 'same')
    monkeypatch.setattr(retirement.r, 'catalog_references', lambda _c, _root, _m, targets, _g:
        {value: {old_ref, 'manifests/okx/new.json'} if value == descriptor['relpath'] else {old_ref}
         for value in targets})
    monkeypatch.setattr(retirement.r, 'fresh_manifest_references', lambda *_: set())
    monkeypatch.setattr(retirement.r, 'include_other_authority_repack_pins', lambda _c, _m, known: known)
    monkeypatch.setattr(retirement.r, '_histories', lambda *_: [])
    monkeypatch.setattr(retirement.packed_retention, '_local_protected_snapshot_ids', lambda *_a, **_kw: (set(), {}, []))
    monkeypatch.setattr(retirement, 'process_references_many', lambda *_: [])
    result = retirement.retire(cold, private, promotion=promoted, plan=options['plan'], rows={}, controls={},
        latest_policy=options['latest_policy'], catalog_sha='c'*64, journal_path=tmp_path/'E/retirement.json',
        catalog=tmp_path/'catalog', observation=tmp_path/'observation', cache=tmp_path/'cache',
        materialized=tmp_path/'materialized', retention_state=tmp_path/'retention', apply=True)
    assert result['state'] == 'completed' and not old.manifest_path.exists()
    assert (cold/descriptor['relpath']).is_file() and (private/descriptor['relpath']).is_file()
    assert descriptor['relpath'] not in {item['relative'] for item in result['plan']['items']}
    assert packed.resolve_latest_packed(cold, 'okx', require_objects=True).manifest_sha256 == promoted['new_manifest_sha256']
