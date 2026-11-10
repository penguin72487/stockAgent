import copy
import json
import os
from pathlib import Path

import pytest

from stockagent.data_sync import packed_snapshots as packed, semantic_link_promotion as promotion, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def candidate(tmp_path, monkeypatch):
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    source1, source2 = tmp_path/'old-source', tmp_path/'new-source'
    source1.mkdir()
    source2.mkdir()
    (source1/'rates').write_bytes(b'old')
    (source2/'rates').write_bytes(b'old plus new')
    cold, private = tmp_path/'cold', tmp_path/'cold/.local-state/private-history'
    old = packed.publish_packed_snapshot(cold, 'binance-public-archive', source1, node_id='test')
    new = packed.publish_packed_snapshot(private, 'binance-public-archive', source2, node_id='test',
        metadata={'source_mode': 'retained_handoff_C_index', 'migration_source_identity_sha256': '9'*64})
    objects = [new.manifest['archive']['inventory'], *new.manifest['archive']['objects']]
    prepared = {'contract': stream_repack.CONTRACT+'.prepared', 'dataset': 'binance-public-archive',
        'source_mode': 'retained_handoff_C_index', 'old_manifest_sha256': '9'*64,
        'source_member_sha_streamed': True, 'destination_independent_readback': False,
        'files': new.manifest['source']['files'],
        'logical_bytes': new.manifest['source']['logical_bytes'],
        'inventory': {'sha256': objects[0]['sha256'], 'bytes': objects[0]['bytes']},
        'packages': [{'sha256': x['sha256'], 'bytes': x['bytes']} for x in objects[1:]]}
    prepared['identity_sha256'] = stream_repack._identity(prepared)
    install = {'contract': stream_repack.CONTRACT+'.install', 'old_manifest_sha256': '9'*64,
        'prepared_sha256': prepared['identity_sha256'], 'new_manifest_sha256': new.manifest_sha256,
        'installed': [{'sha256': item['sha256'], 'D_signature': stream_repack._signature(private/item['relpath'])}
                      for item in objects]}
    plan = {'contract': 'stockagent.index_generation_diff.v1', 'dataset': 'binance-public-archive',
        'public_manifest_sha256': old.manifest_sha256, 'private_manifest_sha256': new.manifest_sha256,
        'private_snapshot_id': new.manifest['snapshot_id'], 'excluded_subtrees': [],
        'counters': {'public_only': 0, 'private_excluded': 0, 'unexpected_public_excluded': 0, 'conflict': 8},
        'conflict_categories': {'parquet': 3, 'json': 4, 'operational_state': 1}}
    plan['identity_sha256'] = stream_repack._identity(plan)
    shared = {'state': 'compared', 'comparison_identity_sha256': plan['identity_sha256'],
        'public_manifest_sha256': old.manifest_sha256, 'private_manifest_sha256': new.manifest_sha256,
        'catalog_sha256': 'c'*64, 'source_mutated': False}
    rows = {**shared, 'contract': 'stockagent.binance_archive_generation_audit.v1',
        'files_total': 3, 'files_completed': 3, 'stream_scope': {'files': 3,
            'original_member_sha256_verified': True, 'independent_recovery_verified': False},
        'categories': {'candles': {'files': 1, 'old_only_rows': 0, 'overlap_rows_with_any_change': 0,
                                    'overlap_rows_with_price_change': 0},
                       'plan': {'files': 1, 'old_only_rows': 0},
                       'lifecycle': {'files': 1, 'old_only_rows': 0}}}
    controls = {**shared, 'contract': 'stockagent.binance_archive_control_audit.v1',
        'files_total': 5, 'files_completed': 5, 'stream_scope': {'files': 5,
            'original_member_sha256_verified': True, 'independent_recovery_verified': False},
        'changed_json_fields': {key: [] for key in ('capacity_receipt.json', 'download_summary.json',
                                                    'plan_summary.json', 'progress.json')},
        'state_comparison': {'old_only_rows': 0, 'completed_regressions_same_etag': 0,
                             'hash_conflicts_same_etag': 0}}
    entry = {'dataset': 'binance-public-archive', 'source': str(source2), 'role': 'training',
             'publish': True, 'excluded_subtrees': []}
    stage = tmp_path/'E'
    stage.mkdir()
    options = {'dataset': 'binance-public-archive', 'old_sha': old.manifest_sha256,
        'private_id': new.manifest['snapshot_id'], 'private_sha': new.manifest_sha256,
        'prepared': prepared, 'install': install, 'plan': plan, 'rows': rows, 'controls': controls,
        'catalog_sha': 'c'*64, 'catalog_entry': entry, 'journal_path': stage/'journal.json',
        'check_processes': lambda _paths: [], 'notify': False}
    return cold, private, old, new, options


def test_semantic_promotion_links_objects_once_and_publishes_fixed_head(candidate):
    cold, private, old, newer, options = candidate
    result = promotion.promote(cold, private, **options)
    current = packed.resolve_latest_packed(cold, 'binance-public-archive', require_objects=True)
    assert result['state'] == 'committed' and current.manifest_sha256 == result['new_manifest_sha256']
    assert result['payload_bytes_copied'] == 0 and result['independent_D_readback'] is False
    for item in [newer.manifest['archive']['inventory'], *newer.manifest['archive']['objects']]:
        assert os.path.samefile(cold/item['relpath'], private/item['relpath'])
    assert packed.read_packed_files(cold, current, names=['rates'], maximum_bytes=100,
                                    maximum_object_bytes=1024**2)['rates'] == b'old plus new'
    assert promotion.promote(cold, private, **options)['new_manifest_sha256'] == current.manifest_sha256


def test_semantic_promotion_never_links_when_old_rows_or_state_would_be_lost(candidate):
    cold, private, old, newer, options = candidate
    bad = copy.deepcopy(options)
    bad['rows']['categories']['candles']['old_only_rows'] = 1
    with pytest.raises(SnapshotError, match='row-level merge'):
        promotion.promote(cold, private, **bad)
    assert packed.resolve_latest_packed(cold, 'binance-public-archive').manifest_sha256 == old.manifest_sha256
    assert not options['journal_path'].exists()


def test_semantic_promotion_resumes_link_after_interruption(candidate, monkeypatch):
    cold, private, old, newer, options = candidate
    original = promotion.os.link
    called = 0
    def crash(*args, **kwargs):
        nonlocal called
        original(*args, **kwargs)
        called += 1
        if called == 1:
            raise OSError('simulated interruption after link')
    monkeypatch.setattr(promotion.os, 'link', crash)
    with pytest.raises(OSError, match='interruption'):
        promotion.promote(cold, private, **options)
    journal = json.loads(options['journal_path'].read_bytes())
    assert journal['pending'] and journal['state'] == 'linking'
    assert packed.resolve_latest_packed(cold, 'binance-public-archive').manifest_sha256 == old.manifest_sha256
    monkeypatch.setattr(promotion.os, 'link', original)
    result = promotion.promote(cold, private, **options)
    assert result['state'] == 'committed' and len(result['linked']) == len(newer.manifest['archive']['objects'])+1


def test_semantic_promotion_recovers_when_head_commit_precedes_receipt(candidate, monkeypatch):
    cold, private, old, newer, options = candidate
    original = promotion.atomic_write_json
    def crash(path, value, **kwargs):
        if Path(path) == options['journal_path'] and value.get('state') == 'committed':
            raise OSError('simulated crash after head commit')
        return original(path, value, **kwargs)
    monkeypatch.setattr(promotion, 'atomic_write_json', crash)
    with pytest.raises(OSError, match='crash after head'):
        promotion.promote(cold, private, **options)
    assert json.loads(options['journal_path'].read_bytes())['state'] == 'head_pending'
    assert packed.resolve_latest_packed(cold, 'binance-public-archive').manifest_sha256 != old.manifest_sha256
    monkeypatch.setattr(promotion, 'atomic_write_json', original)
    assert promotion.promote(cold, private, **options)['state'] == 'committed'


def build_latest_candidate(tmp_path, monkeypatch, dataset='binance', *, shared=False, identical=False,
                           include_lock=False):
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    old_source, new_source = tmp_path/'old-source', tmp_path/'new-source'
    old_source.mkdir(); new_source.mkdir()
    (old_source/'rates').write_bytes(b'old observed values')
    (new_source/'rates').write_bytes(b'old observed values' if identical else b'newer but different values')
    if include_lock:
        (old_source/'.download.lock').write_bytes(b'lock')
        (new_source/'.download.lock').write_bytes(b'lock')
    if shared:
        (old_source/'shared').write_bytes(b'identical encoded object')
        (new_source/'shared').write_bytes(b'identical encoded object')
    cold, private = tmp_path/'cold', tmp_path/'cold/.local-state/private-history'
    old = packed.publish_packed_snapshot(cold, dataset, old_source, node_id='test', loose_file_threshold_bytes=1)
    new = packed.publish_packed_snapshot(private, dataset, new_source, node_id='test', loose_file_threshold_bytes=1,
        metadata={'source_mode': 'retained_handoff_C_index', 'migration_source_identity_sha256': '9'*64})
    objects = [new.manifest['archive']['inventory'], *new.manifest['archive']['objects']]
    prepared = {'contract': stream_repack.CONTRACT+'.prepared', 'dataset': dataset,
        'source_mode': 'retained_handoff_C_index', 'old_manifest_sha256': '9'*64,
        'source_member_sha_streamed': True, 'destination_independent_readback': False,
        'files': new.manifest['source']['files'], 'logical_bytes': new.manifest['source']['logical_bytes'],
        'inventory': {'sha256': objects[0]['sha256'], 'bytes': objects[0]['bytes']},
        'packages': [{'sha256': item['sha256'], 'bytes': item['bytes']} for item in objects[1:]]}
    prepared['identity_sha256'] = stream_repack._identity(prepared)
    install = {'contract': stream_repack.CONTRACT+'.install', 'old_manifest_sha256': '9'*64,
        'prepared_sha256': prepared['identity_sha256'], 'new_manifest_sha256': new.manifest_sha256,
        'installed': [{'sha256': item['sha256'], 'D_signature': stream_repack._signature(private/item['relpath'])}
                      for item in objects]}
    plan = {'contract': 'stockagent.index_generation_diff.v1', 'dataset': dataset,
        'state': 'metadata_compared', 'source_payload_read': False, 'semantic_merge_complete': False,
        'public_manifest_sha256': old.manifest_sha256, 'public_snapshot_id': old.manifest['snapshot_id'],
        'private_manifest_sha256': new.manifest_sha256, 'private_snapshot_id': new.manifest['snapshot_id'],
        'excluded_subtrees': [], 'counters': {'public_only': 0, 'private_excluded': 0,
            'unexpected_public_excluded': 0, 'conflict': 1}, 'conflict_categories': {'parquet': 1}}
    if identical:
        plan['counters'].update(equal=2 if include_lock else 1, private_only=0, conflict=0,
                                conflict_bytes_public=0, conflict_bytes_private=0)
        plan['conflict_categories'] = {}
    plan['identity_sha256'] = stream_repack._identity(plan)
    policy = {'contract': 'stockagent.fast_latest_generation_migration.v1', 'datasets': [dataset],
              'allow_uncompared_historical_values': True, 'generations': {dataset: {
                  'public_manifest_sha256': old.manifest_sha256, 'private_manifest_sha256': new.manifest_sha256}}}
    stage = tmp_path/'E'; stage.mkdir()
    options = {'dataset': dataset, 'old_sha': old.manifest_sha256, 'private_id': new.manifest['snapshot_id'],
        'private_sha': new.manifest_sha256, 'prepared': prepared, 'install': install, 'plan': plan,
        'rows': {}, 'controls': {}, 'catalog_sha': 'c'*64,
        'catalog_entry': {'dataset': dataset, 'source': str(new_source), 'role': 'training', 'publish': True},
        'journal_path': stage/'latest.json', 'check_processes': lambda _: [], 'notify': False,
        'latest_policy': policy}
    return cold, private, old, new, options


@pytest.fixture
def latest_candidate(tmp_path, monkeypatch):
    return build_latest_candidate(tmp_path, monkeypatch)


@pytest.fixture
def latest_with_shared_object(tmp_path, monkeypatch):
    return build_latest_candidate(tmp_path, monkeypatch, 'okx', shared=True)


def test_latest_fixed_promotion_preserves_truth_about_uncompared_values(latest_candidate):
    cold, private, old, newer, options = latest_candidate
    result = promotion.promote(cold, private, **options)
    current = packed.resolve_latest_packed(cold, 'binance', require_objects=True)
    assert result['contract'] == promotion.LATEST_CONTRACT and result['state'] == 'committed'
    assert result['uncompared_changed_members'] == 1 and result['changed_member_values_compared'] is False
    assert current.manifest['metadata']['old_historical_value_coverage'] == 'not_compared'
    assert packed.read_packed_files(cold, current, names=['rates'], maximum_bytes=100,
                                    maximum_object_bytes=1024**2)['rates'] == b'newer but different values'
    assert old.manifest_path.exists()  # Promotion is separate from the guarded old-version retirement.
    assert promotion.promote(cold, private, **options)['new_manifest_sha256'] == current.manifest_sha256


@pytest.mark.parametrize('dataset', ['yahoo-market', 'forex-pepperstone'])
def test_market_latest_selection_uses_same_bound_generation_contract(tmp_path, monkeypatch, dataset):
    cold, private, old, newer, options = build_latest_candidate(tmp_path, monkeypatch, dataset)
    result = promotion.promote(cold, private, **options)
    assert result['state'] == 'committed' and result['dataset'] == dataset
    assert result['old_historical_value_coverage'] == 'not_compared'
    assert old.manifest_path.exists()
    assert packed.resolve_latest_packed(cold, dataset).manifest_sha256 == result['new_manifest_sha256']


def test_bybit_latest_selection_preserves_only_its_reviewed_empty_cache_exclusion(tmp_path, monkeypatch):
    cold, private, old, newer, options = build_latest_candidate(tmp_path, monkeypatch, 'bybit')
    exclusion = ['perpetual_daily/panel_cache_v2']
    options['plan']['excluded_subtrees'] = exclusion
    options['catalog_entry']['excluded_subtrees'] = exclusion
    options['plan']['identity_sha256'] = stream_repack._identity(options['plan'])
    result = promotion.promote(cold, private, **options)
    current = packed.resolve_latest_packed(cold, 'bybit', require_objects=True)
    assert result['state'] == 'committed' and result['old_sources_deleted'] is False
    assert result['uncompared_changed_members'] == 1 and result['changed_member_values_compared'] is False
    assert current.manifest['source']['excluded_subtrees'] == exclusion
    assert promotion.public_source_matches_fixed(current.manifest['source'], newer.manifest['source'], options['plan'])
    assert old.manifest_path.exists()


@pytest.mark.parametrize('defect', ['excluded_member', 'different_exclusion'])
def test_bybit_latest_selection_rejects_unaccounted_or_unreviewed_exclusions(tmp_path, monkeypatch, defect):
    cold, private, old, _newer, options = build_latest_candidate(tmp_path, monkeypatch, 'bybit')
    options['plan']['excluded_subtrees'] = (['perpetual_daily/panel_cache_v2'] if defect == 'excluded_member'
                                             else ['some_other_cache'])
    options['catalog_entry']['excluded_subtrees'] = options['plan']['excluded_subtrees']
    if defect == 'excluded_member':
        options['plan']['counters']['private_excluded'] = 1
    options['plan']['identity_sha256'] = stream_repack._identity(options['plan'])
    with pytest.raises(SnapshotError):
        promotion.promote(cold, private, **options)
    assert not options['journal_path'].exists()
    assert packed.resolve_latest_packed(cold, 'bybit').manifest_sha256 == old.manifest_sha256


@pytest.mark.parametrize('dataset', ['legacy-parquet', 'free-public-context'])
def test_equal_only_selection_rechecks_both_sealed_inventories(tmp_path, monkeypatch, dataset):
    cold, private, old, newer, options = build_latest_candidate(tmp_path, monkeypatch, dataset, identical=True)
    result = promotion.promote(cold, private, **options)
    current = packed.resolve_latest_packed(cold, dataset, require_objects=True)
    assert result['state'] == 'committed' and result['uncompared_changed_members'] == 0
    assert current.manifest_sha256 == result['new_manifest_sha256']
    assert packed.read_packed_files(cold, current, names=['rates'], maximum_bytes=100,
                                    maximum_object_bytes=1024**2)['rates'] == b'old observed values'
    assert old.manifest_path.exists()  # Separate guarded retirement owns old versions.


@pytest.mark.parametrize('dataset', ['legacy-parquet', 'free-public-context'])
def test_equal_only_rejects_a_false_equal_plan(tmp_path, monkeypatch, dataset):
    cold, private, old, newer, options = build_latest_candidate(tmp_path, monkeypatch, dataset)
    options['plan']['counters'].update(equal=1, private_only=0, conflict=0,
                                       conflict_bytes_public=0, conflict_bytes_private=0)
    options['plan']['conflict_categories'] = {}
    options['plan']['identity_sha256'] = stream_repack._identity(options['plan'])
    with pytest.raises(SnapshotError, match='equal-only inventory row differs'):
        promotion.promote(cold, private, **options)
    assert not options['journal_path'].exists()
    assert packed.resolve_latest_packed(cold, dataset).manifest_sha256 == old.manifest_sha256


def test_equal_public_generation_preserves_its_lock_exclusion(tmp_path, monkeypatch):
    from stockagent.data_sync import semantic_link_retirement
    cold, private, old, newer, options = build_latest_candidate(
        tmp_path, monkeypatch, 'free-public-context', identical=True)
    options['plan']['excluded_subtrees'] = ['.download.lock']
    options['catalog_entry']['excluded_subtrees'] = ['.download.lock']
    options['plan']['identity_sha256'] = stream_repack._identity(options['plan'])
    result = promotion.promote(cold, private, **options)
    current = packed.resolve_latest_packed(cold, 'free-public-context')
    assert current.manifest['source']['excluded_subtrees'] == ['.download.lock']
    assert newer.manifest['source']['excluded_subtrees'] == []
    checked = semantic_link_retirement._check_replacement(cold, private, result, options['plan'])
    assert checked.manifest_sha256 == current.manifest_sha256
    tampered = dict(current.manifest['source'])
    tampered['logical_bytes'] += 1
    assert not promotion.public_source_matches_fixed(tampered, newer.manifest['source'], options['plan'])
    tampered = dict(current.manifest['source'], excluded_subtrees=[])
    assert not promotion.public_source_matches_fixed(tampered, newer.manifest['source'], options['plan'])


def test_equal_selection_rejects_a_present_excluded_control_file(tmp_path, monkeypatch):
    cold, private, old, newer, options = build_latest_candidate(
        tmp_path, monkeypatch, 'free-public-context', identical=True, include_lock=True)
    options['plan']['excluded_subtrees'] = ['.download.lock']
    options['catalog_entry']['excluded_subtrees'] = ['.download.lock']
    options['plan']['identity_sha256'] = stream_repack._identity(options['plan'])
    with pytest.raises(SnapshotError, match='excluded path'):
        promotion.promote(cold, private, **options)
    assert packed.resolve_latest_packed(cold, 'free-public-context').manifest_sha256 == old.manifest_sha256


@pytest.mark.parametrize('counter', ['conflict', 'private_only', 'public_only', 'private_excluded',
                                     'unexpected_public_excluded', 'conflict_bytes_public', 'conflict_bytes_private'])
def test_equal_only_refuses_any_unaccounted_source_difference(tmp_path, monkeypatch, counter):
    cold, private, old, newer, options = build_latest_candidate(tmp_path, monkeypatch, 'legacy-parquet', identical=True)
    options['plan']['counters'][counter] = 1
    if counter == 'conflict':
        options['plan']['conflict_categories'] = {'parquet': 1}
    options['plan']['identity_sha256'] = stream_repack._identity(options['plan'])
    with pytest.raises(SnapshotError):
        promotion.promote(cold, private, **options)
    assert not options['journal_path'].exists()
    assert packed.resolve_latest_packed(cold, 'legacy-parquet').manifest_sha256 == old.manifest_sha256


def test_latest_promotion_reuses_an_existing_public_object_and_records_remaining_duplicate(latest_with_shared_object):
    cold, private, old, newer, options = latest_with_shared_object
    preview = promotion.promote(cold, private, **options, dry_run=True)
    assert preview['state'] == 'preflight_only' and preview['reused_existing_objects'] == 1
    assert preview['duplicate_encoded_bytes_pending'] == len(b'identical encoded object')
    assert not options['journal_path'].exists()
    assert packed.resolve_latest_packed(cold, 'okx').manifest_sha256 == old.manifest_sha256
    result = promotion.promote(cold, private, **options)
    shared, = [item for item in result['linked'] if item.get('mode') == 'existing_public_same_sha']
    descriptor, = [item for item in newer.manifest['archive']['objects'] if item['sha256'] == shared['sha256']]
    assert descriptor['bytes'] == len(b'identical encoded object')
    assert not os.path.samefile(cold/descriptor['relpath'], private/descriptor['relpath'])
    assert result['duplicate_encoded_bytes_pending'] == descriptor['bytes']
    current = packed.resolve_latest_packed(cold, 'okx', require_objects=True)
    assert current.manifest['metadata']['shared_physical_object_names'] is False
    assert current.manifest['metadata']['duplicate_encoded_bytes_pending'] == descriptor['bytes']
    assert promotion.promote(cold, private, **options)['state'] == 'committed'


@pytest.mark.parametrize('defect', ['policy', 'public_only', 'wrong_scope', 'binding'])
def test_latest_fixed_promotion_refuses_unapproved_or_missing_original_paths(latest_candidate, defect):
    cold, private, old, newer, options = latest_candidate
    if defect == 'policy': options['latest_policy']['allow_uncompared_historical_values'] = False
    if defect == 'public_only': options['plan']['counters']['public_only'] = 1
    if defect == 'wrong_scope': options['plan']['excluded_subtrees'] = ['private']
    if defect == 'binding': options['latest_policy']['generations']['binance']['private_manifest_sha256'] = '0'*64
    options['plan']['identity_sha256'] = stream_repack._identity(options['plan'])
    with pytest.raises(SnapshotError):
        promotion.promote(cold, private, **options)
    assert packed.resolve_latest_packed(cold, 'binance').manifest_sha256 == old.manifest_sha256
    assert not options['journal_path'].exists()
