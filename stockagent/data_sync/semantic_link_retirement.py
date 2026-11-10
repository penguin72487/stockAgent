"""Retire one semantically superseded release through the existing GC owners."""
from __future__ import annotations

import os
from pathlib import Path
import time

from scripts import retire_staged_cold_objects as old_retirement
from stockagent.data_sync import packed_retention, packed_snapshots as p, repack_retirement as r, stream_repack as s
from stockagent.data_sync.backup_stream import _private_catalog_generation
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.materialized_cache import process_references_many
from stockagent.data_sync.semantic_link_promotion import (CONTRACT as PROMOTION, LATEST_CONTRACT as LATEST_PROMOTION,
    FAST_LATEST_DATASETS, _linked_generation_matches, public_source_matches_fixed,
    validate_binance_evidence, validate_latest_fixed_evidence)

CONTRACT = 'stockagent.semantic_link_retirement.v1'
LATEST_CONTRACT = 'stockagent.latest_fixed_link_retirement.v1'


def _check_replacement(cold, private, promotion, plan, *, require_current_head=True):
    if type(require_current_head) is not bool:
        raise SnapshotError('semantic replacement head requirement is invalid')
    name = promotion['dataset']
    newer = p.resolve_packed_snapshot_id(cold, name, promotion['new_snapshot_id'], require_objects=True)
    original = p.resolve_packed_snapshot_id(private, name, promotion['private_snapshot_id'], require_objects=False)
    meta = newer.manifest.get('metadata', {})
    latest = promotion.get('contract') == LATEST_PROMOTION
    keys = (('latest_replaced_original_manifest_sha256', 'latest_fixed_private_manifest_sha256',
             'latest_comparison_identity_sha256') if latest else
            ('semantic_superset_original_manifest_sha256', 'semantic_superset_private_manifest_sha256',
             'semantic_comparison_identity_sha256'))
    if (newer.manifest_sha256 != promotion['new_manifest_sha256']
            or original.manifest_sha256 != promotion['private_manifest_sha256']
            or meta.get(keys[0]) != promotion['old_manifest_sha256']
            or meta.get(keys[1]) != promotion['private_manifest_sha256']
            or meta.get(keys[2]) != promotion['comparison_identity_sha256']
            or latest and (meta.get('selection_contract') != LATEST_PROMOTION
                           or meta.get('changed_member_values_compared') is not False
                           or meta.get('old_historical_value_coverage') != 'not_compared'
                           or meta.get('uncompared_changed_members') != plan.get('counters', {}).get('conflict'))
            or not public_source_matches_fixed(newer.manifest['source'], original.manifest['source'], plan)
            or plan.get('private_manifest_sha256') != original.manifest_sha256
            or require_current_head and p.resolve_latest_packed(cold, name, require_objects=False).manifest_sha256 != newer.manifest_sha256):
        raise SnapshotError('semantic replacement source or public head differs')
    expected = [original.manifest['archive']['inventory'], *original.manifest['archive']['objects']]
    actual = [newer.manifest['archive']['inventory'], *newer.manifest['archive']['objects']]
    if actual != expected:
        raise SnapshotError('semantic replacement does not use the exact linked source objects')
    linked = {item.get('sha256'): item for item in promotion.get('linked', []) if isinstance(item, dict)}
    if len(linked) != len(expected) or set(linked) != {item['sha256'] for item in expected}:
        raise SnapshotError('semantic replacement link journal is incomplete')
    for item in expected:
        record = linked[item['sha256']]
        if (record.get('mode') == 'existing_public_same_sha' and not latest
                or not _linked_generation_matches(private, cold, item, record)):
            raise SnapshotError('semantic replacement linked object changed')
    return newer


def retire(cold: Path, private: Path, *, promotion: dict, plan: dict, rows: dict, controls: dict,
           catalog_sha: str, journal_path: Path, catalog: Path, observation: Path, cache: Path,
           materialized: Path, retention_state: Path, extra_locks=(), guard=lambda: None,
           apply=False, latest_policy=None) -> dict:
    """Dry-run or unlink only old, unpinned, unreferenced exact D names."""
    cold, private, journal_path = Path(cold).absolute(), Path(private).absolute(), Path(journal_path).absolute()
    latest = latest_policy is not None
    contract = LATEST_CONTRACT if latest else CONTRACT
    if (promotion.get('contract') != (LATEST_PROMOTION if latest else PROMOTION)
            or promotion.get('state') != 'committed'
            or promotion.get('dataset') not in (FAST_LATEST_DATASETS if latest else {'binance-public-archive'})
            or journal_path.is_symlink()
            or p._paths_overlap(cold, journal_path) or journal_path.parent.resolve(strict=True) != journal_path.parent):
        raise SnapshotError('semantic retirement authority or promotion differs')
    if latest:
        validate_latest_fixed_evidence(plan, latest_policy)
        if (promotion.get('dataset') != plan.get('dataset')
                or promotion.get('comparison_identity_sha256') != plan.get('identity_sha256')
                or promotion.get('changed_member_values_compared') is not False
                or promotion.get('old_historical_value_coverage') != 'not_compared'):
            raise SnapshotError('latest fixed retirement selection evidence differs')
    else:
        validate_binance_evidence(plan, rows, controls, catalog_sha=catalog_sha,
                                  audit_identity=promotion.get('comparison_identity_sha256'))
    name = promotion['dataset']
    old_sha = p._validate_hash(promotion.get('old_manifest_sha256'), 'semantic old SHA')
    new_sha = p._validate_hash(promotion.get('new_manifest_sha256'), 'semantic new SHA')
    old_id = p.validate_slug(plan.get('public_snapshot_id'), 'semantic old snapshot')
    new_id = p.validate_slug(promotion.get('new_snapshot_id'), 'semantic new snapshot')
    if ((not latest and plan.get('comparison_scope') != 'fixed_public_snapshot')
            or plan.get('public_manifest_sha256') != old_sha
            or plan.get('private_manifest_sha256') != promotion.get('private_manifest_sha256')
            or plan.get('private_snapshot_id') != promotion.get('private_snapshot_id')):
        raise SnapshotError('semantic retirement fixed comparison differs')
    old_relative = f'manifests/{name}/{old_id}.json'
    journal = r.load_journal(journal_path) if journal_path.is_file() else None
    if journal is not None:
        if (journal.get('contract') != contract or journal.get('dataset') != name
                or journal.get('old_manifest_sha256') != old_sha or journal.get('new_manifest_sha256') != new_sha
                or journal.get('comparison_identity_sha256') != plan['identity_sha256']
                or journal.get('plan_sha256') != s._identity(journal.get('plan', {}))
                or journal.get('state') not in {'planned', 'removing', 'completed'}):
            raise SnapshotError('semantic retirement journal identity differs')
        if journal['state'] == 'completed':
            return journal
        old_refs = set(journal['plan']['old_refs'])
    else:
        old = p.resolve_packed_snapshot_id(cold, name, old_id, require_objects=False)
        if old.manifest_sha256 != old_sha:
            raise SnapshotError('semantic retirement old manifest differs')
        old_refs = r._refs(old.manifest)
    with r._boundary(cold, extra_locks, attempts=1):
        guard()
        r._require_not_current_head(cold, name, old_sha, old_id)
        _check_replacement(cold, private, promotion, plan)
    metadata, _, catalog_identity = old_retirement.catalog_index(catalog, observation, set(), cache_path=cache)
    generation = _private_catalog_generation(catalog)
    references = r.catalog_references(catalog, cold, metadata, old_refs, generation)
    with r._boundary(cold, extra_locks, attempts=1):
        guard()
        if _private_catalog_generation(catalog) != generation:
            raise SnapshotError('semantic retirement catalog changed')
        known = r.fresh_manifest_references(cold, metadata, old_refs, references)
        _check_replacement(cold, private, promotion, plan)
        r._require_not_current_head(cold, name, old_sha, old_id)
        known.update((old_id, new_id))
        known = r.include_other_authority_repack_pins(cold, materialized, known)
        protected, _, errors = packed_retention._local_protected_snapshot_ids(materialized, known,
            research_pin_root=retention_state/'research-pins',
            research_pin_marker=retention_state/packed_retention.RESEARCH_PIN_MARKER)
        if errors or old_id in protected:
            raise SnapshotError('semantic retirement original release is pinned or protection is unknown')
        histories = r._histories(cold, name, old_sha, old_id)
        deletable = sorted(ref for ref, owners in references.items() if not (owners-{old_relative}))
        if journal is None:
            items = []
            for kind, relative in ([('history', value) for value in histories] + [('manifest', old_relative)]
                                   + [('object', value) for value in deletable]):
                path = r._regular(cold, relative, allow_hardlinks=kind == 'object')
                items.append({'kind': kind, 'relative': relative, 'signature': s._signature(path),
                    'bytes': path.stat().st_size,
                    'allocated_bytes': path.stat().st_blocks*512 if path.stat().st_nlink == 1 else 0})
            retirement_plan = {'old_snapshot_id': old_id, 'new_snapshot_id': new_id, 'old_refs': sorted(old_refs),
                               'items': items, 'catalog_identity_sha256': catalog_identity}
            journal = {'contract': contract, 'dataset': name, 'old_manifest_sha256': old_sha,
                'new_manifest_sha256': new_sha, 'comparison_identity_sha256': plan['identity_sha256'],
                'plan': retirement_plan, 'plan_sha256': s._identity(retirement_plan), 'removed': [],
                'state': 'planned', 'independent_D_readback': False, 'NAS_accepted': False}
            if latest:
                journal.update(changed_member_values_compared=False, old_historical_value_coverage='not_compared',
                               uncompared_changed_members=plan['counters']['conflict'])
        else:
            planned = {item['relative'] for item in journal['plan']['items'] if item['kind'] == 'object'}
            if not planned.issubset(deletable):
                raise SnapshotError('semantic retirement references changed during retry')
        if process_references_many([cold/item['relative'] for item in journal['plan']['items']
                                    if os.path.lexists(cold/item['relative'])]):
            raise SnapshotError('semantic retirement original release is in use')
        if not apply:
            return {**journal, 'apply': False, 'reclaimable_bytes': sum(
                item['bytes'] for item in journal['plan']['items'] if item['kind'] == 'object')}
        r.journalled_unlink(cold, journal_path, journal)
        journal.update(state='completed', completed_at_epoch=time.time(),
            deleted_object_bytes=sum(item['bytes'] for item in journal['plan']['items'] if item['kind'] == 'object'),
            estimated_allocated_bytes_reclaimed=sum(item.get('allocated_bytes', 0)
                for item in journal['plan']['items'] if item['kind'] == 'object'),
            deleted_object_count=sum(item['kind'] == 'object' for item in journal['plan']['items']))
        r.save_journal(journal_path, journal)
        return journal
