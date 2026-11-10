"""Retire superseded, unpinned versions of a dataset already repacked on D.

The user elected not to retain unused incremental versions during migration.
This removes no current head, pinned study or object used by any other visible
manifest. Removed historical contents are not claimed to be independently
recoverable; the exact retired version and object identities remain journalled.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import time

from stockagent.data_sync import packed_retention, packed_snapshots as p, repack_retirement as r, stream_repack
from stockagent.data_sync.backup_stream import _private_catalog_generation
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.materialized_cache import process_references_many
from scripts.retire_staged_cold_objects import catalog_index

CONTRACT = 'stockagent.superseded_repack_history.v1'
# One owner pass amortizes the catalog and historical-head census across a
# useful batch. Reference occurrences remain separately bounded below.
MAX_VERSIONS = 128
MAX_REFERENCE_OCCURRENCES = 20000
MAX_WITNESS_MANIFESTS = 4
MAX_WITNESS_CANDIDATES = 16


def _shared_witnesses(cold: Path, dataset: str, targets: set[str], candidates: list[dict],
                      paths: list[Path]) -> tuple[dict[str, set[str]], list[dict]]:
    """Find positive evidence that objects remain referenced by other versions.

    These are only witnesses for *retention*. An uncovered object still needs
    the complete catalog graph before it may be removed. Every selected witness
    is rechecked under the original exclusive publication owner before unlink.
    """
    excluded = {row['relative'] for row in candidates}
    references: dict[str, set[str]] = {}
    witnesses = []
    inspected = 0
    for path in paths:
        relative = path.relative_to(cold).as_posix()
        if relative in excluded:
            continue
        inspected += 1
        if inspected > MAX_WITNESS_CANDIDATES:
            break
        try:
            regular = r._regular(cold, relative)
            before = stream_repack._signature(regular)
            manifest = r._read_manifest(cold, relative)
            if (stream_repack._signature(regular) != before or manifest['dataset'] != dataset
                    or manifest['snapshot_id'] != path.stem):
                continue
            covered = (targets - references.keys()) & r._refs(manifest)
            if not covered:
                continue
            witnesses.append({'relative': relative, 'signature': before, 'covered_count': len(covered)})
            for ref in covered:
                references[ref] = {relative}
            if len(references) == len(targets) or len(witnesses) >= MAX_WITNESS_MANIFESTS:
                break
        except (OSError, ValueError, TypeError, KeyError, SnapshotError):
            # Invalid or changing hints never remove the full graph fallback.
            continue
    return references, witnesses


def _recheck_shared_witnesses(cold: Path, witnesses: list[dict]) -> None:
    for witness in witnesses:
        try:
            path = r._regular(cold, witness['relative'])
            if stream_repack._signature(path) != witness['signature']:
                raise SnapshotError('shared-reference witness changed before retirement')
        except (OSError, ValueError, KeyError) as error:
            raise SnapshotError('shared-reference witness unavailable before retirement') from error


def _heads(cold: Path, dataset: str) -> set[str]:
    root = cold/'heads'/dataset
    if root.is_symlink() or not root.is_dir():
        raise SnapshotError('history retirement head root unavailable')
    result = set()
    for path in root.glob('*.json'):
        raw = r._regular(cold, path.relative_to(cold).as_posix()).read_bytes()
        head = json.loads(raw)
        if head.get('dataset') != dataset or not isinstance(head.get('snapshot_id'), str):
            raise SnapshotError('history retirement head identity differs')
        result.add(head['snapshot_id'])
    if not result:
        raise SnapshotError('history retirement has no current head')
    return result


def _sole_current_head(cold: Path, dataset: str):
    """Observe an empty history without claiming an absent anchor's successor.

    The caller holds the normal publication/backup/replication owners. A
    single valid head and its one manifest prove there is no other manifest
    to retire at this instant; they do not prove value or recovery lineage.
    """
    manifests = cold/'manifests'/dataset
    heads = cold/'heads'/dataset
    if (manifests.is_symlink() or not manifests.is_dir()
            or heads.is_symlink() or not heads.is_dir()):
        raise SnapshotError('history manifest or head namespace unavailable')
    paths = list(manifests.glob('*.json'))
    head_paths = list(heads.glob('*.json'))
    if len(paths) != 1 or len(head_paths) != 1:
        return None
    r._regular(cold, paths[0].relative_to(cold).as_posix())
    r._regular(cold, head_paths[0].relative_to(cold).as_posix())
    resolved = p.resolve_latest_packed(cold, dataset, require_objects=False)
    if (resolved.manifest_path != paths[0] or resolved.head_path != head_paths[0]
            or resolved.manifest.get('snapshot_id') != paths[0].stem):
        raise SnapshotError('sole history head differs from its manifest')
    return {'snapshot_id': resolved.manifest['snapshot_id'],
            'manifest_sha256': resolved.manifest_sha256}


def _protect(materialized, state, known, *, cold=None):
    def load(ids):
        return packed_retention._local_protected_snapshot_ids(materialized, ids,
            research_pin_root=state/'research-pins', research_pin_marker=state/packed_retention.RESEARCH_PIN_MARKER)
    if cold is not None:
        known = r.include_other_authority_repack_pins(cold, materialized, known)
    protected, _, errors = load(known)
    if errors and cold is not None:
        # A concurrent source build can pin a newer current snapshot that is
        # not in the last backup catalog yet. Resolve names from the live
        # manifest namespaces before treating that pin as unknown. The final
        # unlink pass repeats the protection check under the publication lock.
        current = set(known)
        with os.scandir(cold/'manifests') as datasets:
            for dataset in datasets:
                if not dataset.is_dir(follow_symlinks=False):
                    raise SnapshotError('unexpected manifest namespace while checking pins')
                with os.scandir(dataset.path) as manifests:
                    for entry in manifests:
                        if entry.name.endswith('.json'):
                            if not entry.is_file(follow_symlinks=False):
                                raise SnapshotError('invalid manifest name while checking pins')
                            current.add(p.validate_slug(entry.name[:-5], 'manifest snapshot'))
        # A private or training pin may have appeared after the first
        # cross-authority census. Resolve it again using the same exact-pin
        # contract; an unresolvable pin remains an error on the second load.
        current = r.include_other_authority_repack_pins(cold, materialized, current)
        protected, _, errors = load(current)
    if errors:
        raise SnapshotError('history retirement protection metadata unknown')
    return protected


def _active_journal(stage: Path, *, prepared_only=False):
    active = []
    prefix = 'prepared-history-retirement-' if prepared_only else 'history-retirement-'
    for path in stage.glob(prefix+'*.json'):
        if path.is_symlink() or not path.is_file():
            raise SnapshotError('history retirement journal redirected')
        summary = json.loads(path.read_text())
        if not isinstance(summary, dict) or summary.get('state') != 'completed':
            active.append((path, r.load_journal(path)))
    if len(active) > 1:
        raise SnapshotError('multiple unfinished history retirements require reconciliation')
    return active[0] if active else (None, None)


def _semantic_successor(cold: Path, stage: Path, dataset: str, old_sha: str, old_id: str):
    """Follow Binance's one completed, exact semantic-retirement handoff.

    This does not establish historical recovery or replace current-head/pin
    checks. It only supplies a live anchor when the original repack result was
    itself retired by the previously authorized semantic promotion.
    """
    if dataset != 'binance-public-archive':
        return None
    from stockagent.data_sync.packed_member_stream import read_stable_bytes
    from stockagent.data_sync.semantic_link_promotion import CONTRACT as PROMOTION, validate_binance_evidence
    from stockagent.data_sync.semantic_link_retirement import CONTRACT as RETIREMENT

    root = stage.parents[2]/'handoff-index-archive-v1'/dataset
    if not root.exists():
        return None
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('semantic successor evidence root redirected')
    paths = list(root.glob('semantic-promotion-*.json'))
    if len(paths) > 128:
        raise SnapshotError('semantic successor evidence exceeds bound')
    candidates = []
    for path in paths:
        value = json.loads(read_stable_bytes(path, maximum=1024**2))
        if isinstance(value, dict) and value.get('old_manifest_sha256') == old_sha:
            candidates.append((path, value))
    if not candidates:
        return None
    if len(candidates) != 1:
        raise SnapshotError('semantic successor has ambiguous promotion evidence')
    path, promotion = candidates[0]
    identity = p._validate_hash(promotion.get('comparison_identity_sha256'), 'semantic comparison SHA')
    new_sha = p._validate_hash(promotion.get('new_manifest_sha256'), 'semantic successor SHA')
    new_id = p.validate_slug(promotion.get('new_snapshot_id'), 'semantic successor snapshot')
    private_sha = p._validate_hash(promotion.get('private_manifest_sha256'), 'semantic private SHA')
    summary = promotion.get('retirement')
    if (path.name != 'semantic-promotion-'+identity+'.json'
            or promotion.get('contract') != PROMOTION or promotion.get('state') != 'committed'
            or promotion.get('dataset') != dataset or not isinstance(summary, dict)
            or summary.get('state') != 'completed' or summary.get('apply') is not True
            or summary.get('dataset') != dataset or summary.get('old_manifest_sha256') != old_sha
            or summary.get('new_manifest_sha256') != new_sha):
        raise SnapshotError('semantic successor promotion or retirement summary differs')
    journal_path = root/('semantic-retirement-'+identity+'.json')
    before = stream_repack._signature(journal_path)
    journal = r.load_journal(journal_path)
    if stream_repack._signature(journal_path) != before:
        raise SnapshotError('semantic successor retirement journal changed')
    plan = journal.get('plan')
    retired_comparison = p._validate_hash(journal.get('comparison_identity_sha256'), 'retirement comparison SHA')
    items = plan.get('items') if isinstance(plan, dict) else None
    removed = journal.get('removed')
    old_relative = f'manifests/{dataset}/{old_id}.json'
    if (journal.get('contract') != RETIREMENT or journal.get('state') != 'completed'
            or journal.get('dataset') != dataset or journal.get('old_manifest_sha256') != old_sha
            or journal.get('new_manifest_sha256') != new_sha
            or not isinstance(plan, dict) or journal.get('plan_sha256') != stream_repack._identity(plan)
            or not isinstance(items, list) or any(not isinstance(item, dict) for item in items)
            or not isinstance(removed, list)
            or removed != [item.get('relative') for item in items]
            or plan.get('old_snapshot_id') != old_id or plan.get('new_snapshot_id') != new_id
            or not any(item.get('kind') == 'manifest' and item.get('relative') == old_relative for item in items)
            or os.path.lexists(cold/old_relative)):
        raise SnapshotError('semantic successor does not prove the exact prior manifest was retired')
    # Equivalent regenerated diff plans can have a distinct identity from the
    # row/control audit. Revalidate the exact plan used by retirement against
    # that original audit and its fixed, hashed worklist.
    plans = list(root.glob('*/integration-diff-v1/diff-plan-'+retired_comparison+'.json'))
    if len(plans) != 1:
        raise SnapshotError('semantic successor retirement comparison is missing or ambiguous')
    diff = json.loads(read_stable_bytes(plans[0], maximum=1024**2))
    rows = json.loads(read_stable_bytes(root/('row-audit-'+identity+'.json'), maximum=4*1024**2))
    controls = json.loads(read_stable_bytes(root/('control-audit-'+identity+'.json'), maximum=1024**2))
    if (not isinstance(diff, dict) or diff.get('identity_sha256') != retired_comparison
            or stream_repack._identity(diff) != retired_comparison
            or diff.get('public_manifest_sha256') != old_sha
            or diff.get('private_manifest_sha256') != private_sha
            or not isinstance(diff.get('worklist'), dict)):
        raise SnapshotError('semantic successor comparison identity differs')
    worklist = diff['worklist'].get('name')
    if not isinstance(worklist, str) or Path(worklist).name != worklist:
        raise SnapshotError('semantic successor worklist name differs')
    payload = read_stable_bytes(plans[0].parent/worklist, maximum=8*1024**2)
    if hashlib.sha256(payload).hexdigest() != diff['worklist'].get('sha256'):
        raise SnapshotError('semantic successor worklist SHA differs')
    validate_binance_evidence(diff, rows, controls, catalog_sha=rows.get('catalog_sha256'),
                              audit_identity=identity)
    newer = p.resolve_packed_snapshot_id(cold, dataset, new_id, require_objects=True)
    meta = newer.manifest.get('metadata', {})
    if (newer.manifest_sha256 != new_sha or newer.manifest != promotion.get('manifest')
            or meta.get('semantic_superset_original_manifest_sha256') != old_sha
            or meta.get('semantic_superset_private_manifest_sha256') != private_sha
            or meta.get('semantic_comparison_identity_sha256') != identity):
        raise SnapshotError('semantic successor live manifest differs from its fixed promotion')
    return {'manifest_sha256': new_sha, 'snapshot_id': new_id, 'comparison_identity_sha256': identity}


def _replacement_successor(cold: Path, stage: Path, dataset: str, old_sha: str, old_id: str):
    """Keep semantic and explicitly unverified latest-selection proofs distinct."""
    semantic = _semantic_successor(cold, stage, dataset, old_sha, old_id)
    if semantic is not None:
        return 'semantic_successor', semantic
    from stockagent.data_sync.latest_repack_successor import latest_fixed_successor
    latest = latest_fixed_successor(cold, stage, dataset, old_sha, old_id)
    if latest is not None:
        return 'latest_fixed_successor', latest
    return None, None


def retire(cold: Path, stage: Path, receipt: dict, *, catalog: Path, observation: Path,
           cache: Path, materialized: Path, retention_state: Path,
           extra_locks=(), guard=lambda: None, apply=False, prepared_only=False):
    cold, stage = cold.absolute(), stage.absolute()
    dataset = p.validate_slug(receipt.get('dataset'), 'dataset')
    old_sha = p._validate_hash(receipt.get('old_manifest_sha256'), 'old manifest SHA')
    if stage.is_symlink() or stage.resolve(strict=True) != stage:
        raise SnapshotError('history retirement stage redirected')
    if prepared_only:
        if (receipt.get('contract') != 'stockagent.source_local_prepared_history.v1'
                or receipt.get('state') != 'source_hashed_E_packages_prepared'):
            raise SnapshotError('history retirement needs a complete E preparation')
        prepared = stream_repack._read_prepared(stage, dataset, old_sha)
        new_sha = old_sha
        new_id = p.validate_slug(prepared.get('old_snapshot_id'), 'prepared snapshot')
    else:
        new_sha = p._validate_hash(receipt.get('new_manifest_sha256'), 'new manifest SHA')
        new_id = p.validate_slug(receipt.get('new_snapshot_id'), 'new snapshot')
        if (receipt.get('contract') != 'stockagent.source_local_exact_repack.v1'
                or receipt.get('state') != 'new_D_head_committed_from_exact_E_source'
                or receipt.get('source_mode') != 'D_streamed_to_E_dataset_pack'
                or receipt.get('old_release_removed') is not True):
            raise SnapshotError('history retirement needs a completed direct repack retirement')
        prepared = stream_repack._prepared_metadata(stage, dataset, old_sha)
    journal_path, journal = _active_journal(stage, prepared_only=prepared_only)
    # Do not rescan the catalog and candidate manifests on every timer tick
    # when a canonical owner already prevents the eventual unlink. This is a
    # nonblocking probe; the later boundary still rechecks every lock and pin.
    original_new_sha, original_new_id = new_sha, new_id
    successor_kind, successor = None, None
    with r._boundary(cold, extra_locks, attempts=1):
        guard()
        expected_manifest = cold/'manifests'/dataset/(original_new_id+'.json')
        if not prepared_only and not os.path.lexists(expected_manifest):
            successor_kind, successor = _replacement_successor(cold, stage, dataset, original_new_sha, original_new_id)
            if successor is None:
                if journal is None and (current := _sole_current_head(cold, dataset)) is not None:
                    return {'contract': CONTRACT, 'state': 'no_unpinned_history', 'dataset': dataset,
                            'versions_removed': 0, 'object_bytes_removed': 0, 'protected_versions': 0,
                            'original_anchor_missing_without_successor': True,
                            'observed_current_head': current}
                raise SnapshotError('replacement release missing without a verified fixed successor')
            new_sha, new_id = successor['manifest_sha256'], successor['snapshot_id']
            if journal is not None and journal.get('new_manifest_sha256') != new_sha:
                raise SnapshotError('unfinished history journal belongs to another replacement')
    manifests_root = cold/'manifests'/dataset
    if manifests_root.is_symlink() or not manifests_root.is_dir():
        raise SnapshotError('history manifest namespace unavailable')
    paths = None
    if journal is None:
        paths = sorted(path for path in manifests_root.glob('*.json') if path.stem != new_id)
        if len(paths) > 10000:
            raise SnapshotError('history release count exceeds one bounded pass')
        if not paths and new_id in _heads(cold, dataset):
            # There is no other manifest to retire. This observation causes
            # no unlink and needs no multi-GiB reference graph; the original
            # owner will retry when another history becomes due.
            return {'contract': CONTRACT, 'state': 'no_unpinned_history', 'dataset': dataset,
                    'versions_removed': 0, 'object_bytes_removed': 0, 'protected_versions': 0,
                    'effective_new_manifest_sha256': new_sha, 'effective_new_snapshot_id': new_id,
                    **({successor_kind: successor} if successor_kind else {})}
    metadata, _, catalog_identity = catalog_index(catalog, observation, set(), cache_path=cache)
    generation = _private_catalog_generation(catalog)
    if journal is None:
        known = {Path(rel).stem for rel in metadata} | {path.stem for path in paths} | {new_id}
        protected = _protect(materialized, retention_state, known, cold=cold) | _heads(cold, dataset) | {new_id}
        candidates = []
        reference_occurrences = 0
        for path in paths:
            if path.stem in protected:
                continue
            rel = path.relative_to(cold).as_posix()
            before = stream_repack._signature(r._regular(cold, rel))
            raw = path.read_bytes()
            value = json.loads(raw)
            p._validate_manifest(value)
            if (stream_repack._signature(path) != before or value['dataset'] != dataset
                    or value['snapshot_id'] != path.stem):
                raise SnapshotError('history manifest changed or has wrong identity')
            refs = [value['archive']['inventory'], *value['archive']['objects']]
            if candidates and reference_occurrences + len(refs) > MAX_REFERENCE_OCCURRENCES:
                break
            candidates.append({'relative': rel, 'snapshot_id': path.stem,
                'sha256': hashlib.sha256(raw).hexdigest(), 'signature': before,
                'refs': [dict(relative=ref['relpath'], sha256=ref['sha256'], bytes=ref['bytes'])
                         for ref in refs]})
            reference_occurrences += len(refs)
            if len(candidates) >= MAX_VERSIONS:
                break
        if not candidates:
            return {'contract': CONTRACT, 'state': 'no_unpinned_history', 'dataset': dataset,
                    'versions_removed': 0, 'object_bytes_removed': 0,
                    'protected_versions': len([path for path in paths if path.stem in protected]),
                    'effective_new_manifest_sha256': new_sha, 'effective_new_snapshot_id': new_id,
                    **({successor_kind: successor} if successor_kind else {})}
        old_refs = {row['relative'] for candidate in candidates for row in candidate['refs']}
    else:
        if (not isinstance(journal, dict) or journal.get('contract') != CONTRACT
                or journal.get('dataset') != dataset or journal.get('new_manifest_sha256') != new_sha
                or successor_kind is not None and journal.get(successor_kind) != successor
                or not isinstance(journal.get('plan'), dict)
                or journal.get('plan_sha256') != stream_repack._identity(journal['plan'])
                or journal.get('state') != 'removing' or not isinstance(journal.get('removed'), list)):
            raise SnapshotError('history retirement journal identity differs')
        candidates = journal['plan']['versions']
        old_refs = set(journal['plan']['old_refs'])
    # A live, non-retiring manifest is sufficient positive proof that an
    # object must be retained. Scan the expensive catalog graph only for the
    # objects for which no such witness was found. Witnesses do not authorize
    # any deletion and are rechecked under the final publication owner.
    witness_paths = paths
    if witness_paths is None:
        witness_paths = sorted(path for path in manifests_root.glob('*.json') if path.stem != new_id)
        if len(witness_paths) > 10000:
            raise SnapshotError('history release count exceeds one bounded pass')
    witnessed, witnesses = _shared_witnesses(cold, dataset, old_refs, candidates, witness_paths)
    unwitnessed = old_refs - witnessed.keys()
    with r._boundary(cold, extra_locks, attempts=60):
        guard()
    # Even for an empty target set this checks catalog completeness or reuses
    # the existing generation-bound graph cache; an incomplete catalog must
    # never be mistaken for permission to remove a manifest.
    references = r.catalog_references(catalog, cold, metadata, unwitnessed, generation,
        reference_cache=cache.parent/'catalog-target-reference-cache-v1.json', catalog_identity=catalog_identity)
    references.update(witnessed)
    with r._boundary(cold, extra_locks, attempts=60):
        guard()
        if _private_catalog_generation(catalog) != generation:
            raise SnapshotError('history retirement catalog changed')
        known = r.fresh_manifest_references(cold, metadata, old_refs, references)
        _recheck_shared_witnesses(cold, witnesses)
        known.update(x['snapshot_id'] for x in candidates)
        known.add(new_id)
        if prepared_only:
            # Only older versions may go: the current D head stays readable
            # throughout E preparation and its subsequent D installation.
            prepared = stream_repack._read_prepared(stage, dataset, old_sha)
            current = p.resolve_latest_packed(cold, dataset, require_objects=False)
            if (current.manifest_sha256 != old_sha or current.manifest['snapshot_id'] != new_id
                    or current.manifest['source']['files'] != prepared['files']
                    or current.manifest['source']['logical_bytes'] != prepared['logical_bytes']):
                raise SnapshotError('prepared history current D head changed')
        elif successor is None:
            r.check_replacement(cold, stage, dataset, old_sha, new_sha, new_id, prepared)
        elif _replacement_successor(cold, stage, dataset, original_new_sha, original_new_id) != (successor_kind, successor):
            raise SnapshotError('fixed successor changed before history retirement')
        protected = _protect(materialized, retention_state, known, cold=cold) | _heads(cold, dataset) | {new_id}
        if any(row['snapshot_id'] in protected for row in candidates):
            raise SnapshotError('history candidate gained a current head or research pin')
        retiring = {row['relative'] for row in candidates}
        deletable = {ref for ref, owners in references.items() if not (owners-retiring)}
        selected_histories = r._histories_many(cold, dataset,
            [(version['sha256'], version['snapshot_id']) for version in candidates])
        if journal is None:
            items = []
            for version in candidates:
                path = r._regular(cold, version['relative'])
                if stream_repack._signature(path) != version['signature']:
                    raise SnapshotError('history manifest changed before planning')
            for relative in selected_histories:
                path = r._regular(cold, relative)
                items.append({'kind': 'history', 'relative': relative, 'signature': stream_repack._signature(path),
                              'bytes': path.stat().st_size})
            for version in candidates:
                path = r._regular(cold, version['relative'])
                items.append({'kind': 'manifest', 'relative': version['relative'],
                              'signature': version['signature'], 'bytes': path.stat().st_size})
            for relative in sorted(deletable):
                if not os.path.lexists(cold/relative):
                    continue  # Historical gap; its descriptor is retained in the journal.
                path = r._regular(cold, relative, allow_hardlinks=True)
                items.append({'kind': 'object', 'relative': relative,
                              'signature': stream_repack._signature(path), 'bytes': path.stat().st_size,
                              'allocated_bytes': path.stat().st_blocks*512 if path.stat().st_nlink == 1 else 0})
            plan = {'versions': candidates, 'old_refs': sorted(old_refs), 'items': items,
                    'catalog_identity_sha256': catalog_identity,
                    'reference_witnesses': witnesses, 'catalog_graph_target_count': len(unwitnessed)}
            identity = stream_repack._identity(plan)
            prefix = 'prepared-history-retirement-' if prepared_only else 'history-retirement-'
            journal_path = stage/(prefix+identity+'.json')
            journal = {'contract': CONTRACT, 'dataset': dataset, 'new_manifest_sha256': new_sha,
                       'plan': plan, 'plan_sha256': identity, 'removed': [], 'state': 'planned',
                       'historical_versions_independently_recovered': False, 'NAS_accepted': False}
            if successor is not None:
                journal.update({successor_kind: successor, 'original_new_manifest_sha256': original_new_sha,
                                'original_new_snapshot_id': original_new_id})
        else:
            plan = journal['plan']
            names = [item['relative'] for item in plan['items']]
            if (len(set(names)) != len(names) or len(set(journal['removed'])) != len(journal['removed'])
                    or not set(journal['removed']).issubset(names)
                    or (journal.get('pending') is not None and not isinstance(journal.get('pending'), (str, list)))
                    or not {item['relative'] for item in plan['items'] if item['kind'] == 'object'}.issubset(deletable)):
                raise SnapshotError('history retirement references or progress changed')
            expected_histories = {item['relative'] for item in plan['items'] if item['kind'] == 'history'}
            if set(selected_histories) - expected_histories:
                raise SnapshotError('new historical head appeared during retirement')
        if process_references_many([cold/item['relative'] for item in plan['items']]):
            raise SnapshotError('history candidate is used by a running process')
        if not apply:
            return {**journal, 'apply': False, 'object_bytes': sum(item['bytes'] for item in plan['items']
                                                                   if item['kind'] == 'object')}
        for relative in journal['removed']:
            if os.path.lexists(cold/relative):
                raise SnapshotError('retired history name unexpectedly returned')
        r.journalled_unlink(cold, journal_path, journal)
        journal.update(state='completed', completed_at_epoch=time.time(),
                       versions_removed=len(candidates),
                       object_bytes_removed=sum(item['bytes'] for item in plan['items'] if item['kind'] == 'object'),
                       estimated_allocated_bytes_reclaimed=sum(item.get('allocated_bytes', 0) for item in plan['items']
                                                               if item['kind'] == 'object'))
        r.save_journal(journal_path, journal)
        return journal
