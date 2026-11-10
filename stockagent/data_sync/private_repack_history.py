"""Retire unpinned private historical versions after an exact source repack.

This is local-only and never exports private data. Every remaining manifest,
current head, research pin and live reader is checked before journalled unlink.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import time
from contextlib import ExitStack, closing, contextmanager

from stockagent.data_sync import packed_snapshots as p, private_handoff_retirement as private, repack_retirement as r
from stockagent.data_sync import stream_repack as s, superseded_repack_history as history
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.materialized_cache import process_references_many

CONTRACT = 'stockagent.private_stream_repack_history.v1'
CURRENT_CONTRACT = 'stockagent.current_and_research_history.v1'
MAX_APPENDED_MANIFESTS = 128
MAX_APPENDED_MANIFEST_BYTES = 32*1024**2


@contextmanager
def _path_inventory(cold, manifest, *, guard):
    """Coverage needs only path/type, not repeated hashes and chunk extents."""
    from stockagent.data_sync.packed_metadata import MetadataRows
    with MetadataRows(dict, dict, memory_rows=p.METADATA_MEMORY_ROWS) as rows:
        def projection():
            with closing(p._iter_inventory(cold, manifest)) as source:
                for number, row in enumerate(source):
                    if number % 8192 == 0:
                        guard()
                    yield {'path':row['path'], 'kind':row.get('kind')}
        rows.extend(projection())
        yield rows


def _include_appended_references(cold, before, after, references, *, guard):
    """Accept bounded immutable publications under the final original owner.

    A new manifest must be decoded for fresh object/dependency references.
    Changed or removed old manifests still reject the plan. Thus normal
    append-only publication need not restart a long metadata census, while
    no unseen reference can authorize deletion.
    """
    if any(after.get(name) != signature for name, signature in before.items()):
        raise SnapshotError('private history namespace changed during reference census')
    added = sorted(set(after)-set(before))
    if (len(added) > MAX_APPENDED_MANIFESTS
            or sum(after[name][3] for name in added) > MAX_APPENDED_MANIFEST_BYTES):
        raise SnapshotError('private history append delta exceeds bounded reference recheck')
    targets = set(references)
    for relative in added:
        guard()
        manifest = r._read_manifest(cold, relative)
        private.check_dependency_metadata(manifest)
        if s._signature(cold/relative) != after[relative]:
            raise SnapshotError('appended history manifest changed during reference recheck')
        for ref in r._refs(manifest) & targets:
            references[ref].add(relative)
    return len(added)


def retire(cold: Path, stage: Path, receipt: dict, *, public_index: Path, materialized: Path,
           retention_state: Path, extra_locks=(), guard=lambda: None, apply=False,
           process_check=process_references_many, prepared_only=False, current_head_only=False):
    cold, stage = cold.absolute(), stage.absolute()
    dataset = p.validate_slug(receipt.get('dataset'), 'dataset')
    old_sha = p._validate_hash(receipt.get('old_manifest_sha256'), 'old manifest SHA')
    if (not current_head_only and (dataset not in s.PRIVATE_REPACK_DATASETS or receipt.get('private') is not True)) \
            or stage.is_symlink() or stage.resolve(strict=True) != stage:
        raise SnapshotError('private history requires an enrolled private stage')
    contract = CURRENT_CONTRACT if current_head_only else CONTRACT
    if current_head_only:
        if (prepared_only or receipt.get('contract') != CURRENT_CONTRACT
                or receipt.get('state') != 'validated_current_inventory'
                or receipt.get('inventory_metadata_validated') is not True):
            raise SnapshotError('ordinary history requires its explicit current-inventory contract')
        new_sha = old_sha
        new_id = p.validate_slug(receipt.get('snapshot_id'), 'current snapshot')
        prepared = None
    elif prepared_only:
        if (receipt.get('contract') != 'stockagent.source_local_prepared_history.v1'
                or receipt.get('state') != 'source_hashed_E_packages_prepared'):
            raise SnapshotError('private history requires a complete E preparation')
        prepared = s._read_prepared(stage, dataset, old_sha)
        new_sha = old_sha
        new_id = p.validate_slug(prepared.get('old_snapshot_id'), 'prepared snapshot')
    else:
        new_sha = p._validate_hash(receipt.get('new_manifest_sha256'), 'new manifest SHA')
        new_id = p.validate_slug(receipt.get('new_snapshot_id'), 'new snapshot')
        from stockagent.data_sync import private_subset_reuse as subset
        if (receipt.get('contract') != 'stockagent.source_local_exact_repack.v1'
                or receipt.get('state') != 'new_D_head_committed_from_exact_E_source'
                or receipt.get('source_mode') not in {'D_streamed_to_E_dataset_pack', subset.SOURCE_MODE}
                or (receipt.get('source_mode') == subset.SOURCE_MODE and dataset not in subset.REVIEWED_PAIRS)
                or receipt.get('old_release_removed') is not True):
            raise SnapshotError('private history requires its exact committed and retired stream release')
        prepared = s._prepared_metadata(stage, dataset, old_sha)
    unfinished = []
    journal_prefix = ('ordinary-history-retirement-' if current_head_only else
                      'private-prepared-history-retirement-' if prepared_only else 'private-history-retirement-')
    for path in stage.glob(journal_prefix+'*.json'):
        if path.is_symlink() or not path.is_file():
            raise SnapshotError('private history journal redirected')
        value = r.load_journal(path)
        if value.get('state') != 'completed':
            unfinished.append((path, value))
    if len(unfinished) > 1:
        raise SnapshotError('multiple unfinished private history retirements')
    journal_path, journal = unfinished[0] if unfinished else (None, None)
    if journal is not None and (journal.get('contract') != contract or journal.get('dataset') != dataset
            or journal.get('new_manifest_sha256') != new_sha or not isinstance(journal.get('plan'), dict)
            or journal.get('plan_sha256') != s._identity(journal['plan'])
            or journal.get('state') not in {'planned', 'removing'}):
        raise SnapshotError('private history journal identity differs')
    if journal is None and not current_head_only:
        current_source = p.resolve_latest_packed(cold, dataset, require_objects=False)
        if not s.private_repack_source_allowed(dataset, current_source.manifest):
            raise SnapshotError('private history belongs to an excluded source owner')

    with r._boundary(cold, extra_locks):
        guard()
        census = private._manifest_census(cold)
    dependency_pins = private._source_link_protections(cold, census, dataset)
    known = {Path(name).stem for name in census} | {new_id}
    if current_head_only:
        public = cold.parent.parent if cold.name == 'private-history' and cold.parent.name == '.local-state' else cold
        other = public if cold != public else public/'.local-state/private-history'
        if other.is_dir():
            known.update(Path(name).stem for name in private._manifest_census(other))
    else:
        known.update(private._public_known(public_index))
    if journal is not None:
        known.update(row['snapshot_id'] for row in journal['plan']['versions'])
    protected = (history._protect(materialized, retention_state, known, cold=cold)
                 | history._heads(cold, dataset) | {new_id} | set(dependency_pins))
    if journal is None:
        candidates, occurrences = [], 0
        coverage_protected = 0
        manifest_prefix = f'manifests/{dataset}/'
        with ExitStack() as metadata:
            current_rows = None
            if current_head_only:
                anchor = p.resolve_packed_snapshot_id(cold, dataset, new_id, require_objects=False)
                if anchor.manifest_sha256 != new_sha:
                    raise SnapshotError('ordinary archive fixed anchor changed before planning')
                current_rows = metadata.enter_context(_path_inventory(cold, anchor.manifest, guard=guard))
            for relative in sorted(name for name in census if name.startswith(manifest_prefix)):
                guard()
                path = cold/relative
                if path.stem in protected:
                    continue
                raw = r._regular(cold, relative).read_bytes()
                manifest = json.loads(raw)
                p._validate_manifest(manifest)
                if s._signature(path) != census[relative] or manifest['dataset'] != dataset or manifest['snapshot_id'] != path.stem:
                    raise SnapshotError('private history manifest changed during planning')
                if current_rows is not None:
                    # A latest head may be an incomplete historical capture.
                    # Never remove the only recorded file or symlink merely
                    # because that old version was not used in research.
                    covered = True
                    with closing(p._iter_inventory(cold, manifest)) as old_rows:
                        for number, old_row in enumerate(old_rows):
                            if number % 8192 == 0:
                                guard()
                            if old_row['kind'] == 'directory':
                                continue
                            replacement = current_rows.lookup(old_row['path'])
                            if replacement is None or replacement['kind'] != old_row['kind']:
                                covered = False
                                break
                    if not covered:
                        coverage_protected += 1
                        continue
                refs = sorted(r._refs(manifest))
                if candidates and occurrences+len(refs) > history.MAX_REFERENCE_OCCURRENCES:
                    break
                candidates.append({'relative': relative, 'snapshot_id': path.stem,
                                   'sha256': hashlib.sha256(raw).hexdigest(),
                                   'signature': census[relative], 'refs': refs})
                occurrences += len(refs)
                if len(candidates) >= history.MAX_VERSIONS:
                    break
        if not candidates:
            return {'contract': contract, 'dataset': dataset,
                    'state': 'retained_unique_history' if coverage_protected else 'no_unpinned_history',
                    'versions_removed': 0, 'object_bytes_removed': 0,
                    'coverage_protected_versions': coverage_protected,
                    'protected_versions': sum(Path(name).stem in protected and Path(name).stem != new_id
                                              for name in census if name.startswith(manifest_prefix))}
        targets = {ref for candidate in candidates for ref in candidate['refs']}
    else:
        candidates, targets = journal['plan']['versions'], set(journal['plan']['old_refs'])
    references = {ref: set() for ref in targets}
    for relative, before in census.items():
        guard()
        manifest = r._read_manifest(cold, relative)
        private.check_dependency_metadata(manifest)
        if s._signature(cold/relative) != before:
            raise SnapshotError('private history manifest changed during reference census')
        for ref in r._refs(manifest) & targets:
            references[ref].add(relative)

    with r._boundary(cold, extra_locks):
        guard()
        current_census = private._manifest_census(cold)
        appended = _include_appended_references(cold, census, current_census, references, guard=guard)
        census = current_census
        known.update(Path(name).stem for name in census)
        # Pin enrollment uses the same publication owner and may have completed
        # while the unlocked reference census was running.
        dependency_pins = private._source_link_protections(cold, census, dataset)
        if current_head_only:
            # A newly published head does not invalidate this immutable,
            # validated anchor. Keep the anchor as well as every actual head;
            # the reconciled census includes the new head's fresh references.
            anchor = p.resolve_packed_snapshot_id(cold, dataset, new_id, require_objects=True)
            if (anchor.manifest_sha256 != new_sha
                    or anchor.manifest['archive']['inventory']['sha256'] != receipt.get('inventory_sha256')):
                raise SnapshotError('ordinary archive fixed anchor changed during planning')
        elif prepared_only:
            prepared = s._read_prepared(stage, dataset, old_sha)
            current = p.resolve_latest_packed(cold, dataset, require_objects=False)
            if (current.manifest_sha256 != old_sha or current.manifest['snapshot_id'] != new_id
                    or current.manifest['source']['files'] != prepared['files']
                    or current.manifest['source']['logical_bytes'] != prepared['logical_bytes']):
                raise SnapshotError('private prepared history current D head changed')
        else:
            r.check_replacement(cold, stage, dataset, old_sha, new_sha, new_id, prepared)
        known.update(row['snapshot_id'] for row in candidates)
        protected = (history._protect(materialized, retention_state, known, cold=cold)
                     | history._heads(cold, dataset) | {new_id} | set(dependency_pins))
        if any(row['snapshot_id'] in protected for row in candidates):
            raise SnapshotError('private history candidate gained a head or research pin')
        retiring = {row['relative'] for row in candidates}
        deletable = {ref for ref, owners in references.items() if not (owners-retiring)}
        histories = r._histories_many(cold, dataset, [(row['sha256'], row['snapshot_id']) for row in candidates])
        if journal is None:
            items = []
            for candidate in candidates:
                if not s._signature_matches(r._regular(cold, candidate['relative']), candidate['signature']):
                    raise SnapshotError('private history candidate changed before unlink planning')
            for kind, relative in ([('history', value) for value in histories]
                                   + [('manifest', row['relative']) for row in candidates]
                                   + [('object', value) for value in sorted(deletable)]):
                if not os.path.lexists(cold/relative):
                    from stockagent.data_sync.packed_object_placement import retirement_record
                    if kind != 'object' or retirement_record(cold, relative) is None:
                        continue
                item = r.plan_member(cold, kind, relative)
                if kind != 'object':
                    item['allocated_bytes'] = 0
                items.append(item)
            plan = {'versions': candidates, 'old_refs': sorted(targets), 'items': items}
            identity = s._identity(plan)
            journal_path = stage/(journal_prefix+identity+'.json')
            journal = {'contract': contract, 'dataset': dataset, 'new_manifest_sha256': new_sha,
                       'plan': plan, 'plan_sha256': identity, 'removed': [], 'state': 'planned',
                       'historical_versions_independently_recovered': False, 'NAS_accepted': False}
            if current_head_only:
                journal['retention_fence'] = 'fixed-validated-anchor-and-all-current-heads-v2'
        else:
            plan = journal['plan']
            if (not {item['relative'] for item in plan['items'] if item['kind'] == 'object'}.issubset(deletable)
                    or set(histories) - {item['relative'] for item in plan['items'] if item['kind'] == 'history'}):
                raise SnapshotError('private history references changed during retry')
        if process_check([r.member_path(cold, item, missing_ok=True) for item in plan['items']
                          if os.path.lexists(r.member_path(cold, item, missing_ok=True))]):
            raise SnapshotError('private history candidate is used by a running process')
        if not apply:
            return {**journal, 'apply': False}
        for relative in journal['removed']:
            if os.path.lexists(cold/relative):
                raise SnapshotError('retired private history name unexpectedly returned')
        r.journalled_unlink(cold, journal_path, journal)
        journal.update(state='completed', completed_at_epoch=time.time(), versions_removed=len(candidates),
                       appended_manifests_reconciled=appended,
                       object_bytes_removed=sum(item['bytes'] for item in plan['items'] if item['kind'] == 'object'),
                       estimated_allocated_bytes_reclaimed=sum(item['allocated_bytes'] for item in plan['items'] if 'placement' not in item),
                       estimated_E_allocated_bytes_reclaimed=sum(item['allocated_bytes'] for item in plan['items'] if 'placement' in item))
        r.save_journal(journal_path, journal)
        return journal
