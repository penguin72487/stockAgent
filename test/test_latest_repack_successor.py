from contextlib import nullcontext
import gzip
import hashlib
import json

import pytest

from stockagent.data_sync import latest_promotion_evidence as latest_evidence
from stockagent.data_sync import dataset_migration, dataset_version_layout, packed_snapshots as packed
from stockagent.data_sync import semantic_link_promotion as promotion, semantic_link_retirement as retirement
from stockagent.data_sync import stream_repack
from stockagent.data_sync.latest_repack_successor import latest_fixed_successor
from stockagent.data_sync.desync_snapshots import SnapshotError
from test_semantic_link_promotion import build_latest_candidate


@pytest.mark.parametrize('defect', [None, 'advanced_head', 'retirement', 'worklist', 'policy', 'old_returned', 'missing_object'])
def test_latest_successor_requires_completed_fixed_chain(tmp_path, monkeypatch, defect):
    cold, private, old, _new, options = build_latest_candidate(tmp_path, monkeypatch, 'okx')
    plan = options['plan']
    plan['counters']['private_only'] = 0
    payload = gzip.compress((json.dumps({'path': 'rates', 'decision': 'conflict', 'category': 'parquet',
        'public': {'kind': 'file'}, 'private': {'kind': 'file'}}) + '\n').encode())
    plan['worklist'] = {'name': 'worklist.gz', 'sha256': hashlib.sha256(payload).hexdigest()}
    plan['identity_sha256'] = stream_repack._identity(plan)
    promoted = promotion.promote(cold, private, **options)
    identity = promoted['comparison_identity_sha256']
    migration = tmp_path / 'workspace/migration'
    stage = migration / 'stream-source-repack-v1/okx' / ('a' * 64)
    stage.mkdir(parents=True)
    evidence = migration / 'handoff-index-archive-v1/okx'
    diff = evidence / 'fixed/integration-diff-v1'
    diff.mkdir(parents=True)
    (diff / f'diff-plan-{identity}.json').write_text(json.dumps(plan))
    (diff / 'worklist.gz').write_bytes(payload)
    policy_path = tmp_path / 'policy.json'
    policy_path.write_text(json.dumps(options['latest_policy']))
    monkeypatch.setattr(latest_evidence, 'POLICY', policy_path)
    monkeypatch.setattr(dataset_migration, 'private_history_root', lambda *_a, **_kw: private)
    monkeypatch.setattr(dataset_version_layout, 'version_policy', lambda *_a, **_kw: {})

    old_ref = old.manifest_path.relative_to(cold).as_posix()
    monkeypatch.setattr(retirement.r, '_boundary', lambda *_a, **_kw: nullcontext())
    monkeypatch.setattr(retirement.old_retirement, 'catalog_index', lambda *_a, **_kw: ({}, {}, 'catalog'))
    monkeypatch.setattr(retirement, '_private_catalog_generation', lambda *_a: 'same')
    monkeypatch.setattr(retirement.r, 'catalog_references', lambda _c, _root, _m, targets, _g:
                        {value: {old_ref} for value in targets})
    monkeypatch.setattr(retirement.r, 'fresh_manifest_references', lambda *_a: set())
    monkeypatch.setattr(retirement.r, 'include_other_authority_repack_pins', lambda _c, _m, known: known)
    monkeypatch.setattr(retirement.r, '_histories', lambda *_a: [])
    monkeypatch.setattr(retirement.packed_retention, '_local_protected_snapshot_ids',
                        lambda *_a, **_kw: (set(), {}, []))
    monkeypatch.setattr(retirement, 'process_references_many', lambda *_a: [])
    journal_path = evidence / f'latest-retirement-{identity}.json'
    result = retirement.retire(cold, private, promotion=promoted, plan=plan, rows={}, controls={},
        latest_policy=options['latest_policy'], catalog_sha=options['catalog_sha'], journal_path=journal_path,
        catalog=tmp_path/'catalog', observation=tmp_path/'observation', cache=tmp_path/'cache',
        materialized=tmp_path/'materialized', retention_state=tmp_path/'retention', apply=True)
    assert result['state'] == 'completed'
    promoted.update(old_sources_deleted=True, retirement={
        'state': 'completed', 'apply': True, 'dataset': 'okx', 'old_manifest_sha256': old.manifest_sha256,
        'new_manifest_sha256': promoted['new_manifest_sha256'], 'changed_member_values_compared': False,
        'old_historical_value_coverage': 'not_compared', 'uncompared_changed_members': 1})
    promotion_path = evidence / f'latest-promotion-{identity}.json'
    promotion_path.write_text(json.dumps(promoted))
    if defect == 'advanced_head':
        current_source = tmp_path / 'newer-live-source'
        current_source.mkdir()
        (current_source / 'rates').write_bytes(b'newly published source')
        current = packed.publish_packed_snapshot(cold, 'okx', current_source, node_id='test')
    elif defect == 'retirement':
        journal = json.loads(journal_path.read_text())
        journal['removed_count'] -= 1
        journal_path.write_text(json.dumps(journal))
    elif defect == 'worklist':
        (diff / 'worklist.gz').write_bytes(b'changed')
    elif defect == 'policy':
        changed = options['latest_policy'].copy()
        changed['allow_uncompared_historical_values'] = False
        policy_path.write_text(json.dumps(changed))
    elif defect == 'old_returned':
        old.manifest_path.write_text(json.dumps(old.manifest))
    elif defect == 'missing_object':
        (cold / promoted['manifest']['archive']['objects'][0]['relpath']).unlink()
    if defect in (None, 'advanced_head'):
        checked = latest_fixed_successor(cold, stage, 'okx', old.manifest_sha256, old.manifest['snapshot_id'])
        assert checked['manifest_sha256'] == promoted['new_manifest_sha256']
        assert checked['snapshot_id'] == promoted['new_snapshot_id']
        assert checked['old_historical_value_coverage'] == 'not_compared'
        assert checked['uncompared_changed_members'] == 1
        if defect == 'advanced_head':
            assert packed.resolve_latest_packed(cold, 'okx').manifest_sha256 == current.manifest_sha256
    else:
        with pytest.raises((SnapshotError, OSError, ValueError)):
            latest_fixed_successor(cold, stage, 'okx', old.manifest_sha256, old.manifest['snapshot_id'])
