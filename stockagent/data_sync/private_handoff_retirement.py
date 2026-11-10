"""Retire only the superseded private D encoding of a sealed C-to-E handoff."""
from __future__ import annotations

import hashlib
import itertools
import json
import os
from pathlib import Path
import re
import time
from types import SimpleNamespace

from stockagent.data_sync import packed_retention, packed_snapshots as p, repack_retirement as r, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.materialized_cache import process_references_many

CONTRACT = 'stockagent.private_handoff_D_retirement.v1'


def check_dependency_metadata(manifest: dict) -> None:
    """Do not retire a generation if a new reference contract is unknown.

    Existing private publishers use the SQLite and source-link contracts below;
    public bootstrap releases describe provenance rather than a private source
    dependency. The owner checks the actual fixed dependencies separately.
    """
    metadata = manifest.get('metadata', {})
    if not isinstance(metadata, dict):
        raise SnapshotError('private manifest metadata is invalid')
    recognized = {'consistent_sqlite_bindings', 'private_source_link_bindings',
                  'version_dependencies_sha256'}
    for key in metadata:
        if not isinstance(key, str):
            raise SnapshotError('private manifest metadata key is invalid')
        if re.search(r'bind|dependenc|source.link|linked.source', key, re.IGNORECASE) and key not in recognized:
            raise SnapshotError('unsupported private dependency metadata: '+key)


def _manifest_census(cold: Path):
    root = cold/'manifests'
    if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
        raise SnapshotError('private D manifest root unavailable or redirected')
    result = {}
    with os.scandir(root) as datasets:
        for dataset in datasets:
            if not dataset.is_dir(follow_symlinks=False):
                raise SnapshotError('unexpected private D manifest namespace entry')
            with os.scandir(dataset.path) as members:
                for member in members:
                    if not member.name.endswith('.json'):
                        continue
                    if not member.is_file(follow_symlinks=False):
                        raise SnapshotError('private D manifest redirected')
                    path = Path(member.path)
                    relative = path.relative_to(cold).as_posix()
                    result[relative] = stream_repack._signature(path)
                    if len(result) > 100000:
                        raise SnapshotError('private D manifest census exceeds bound')
    return result


def _public_known(cache: Path):
    if cache.is_symlink() or not cache.is_file() or cache.stat().st_size > 128*1024**2:
        raise SnapshotError('public snapshot protection index unavailable')
    value = json.loads(cache.read_text())
    body = {key: item for key, item in value.items() if key != 'identity_sha256'}
    digest = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':'),
                                       ensure_ascii=False).encode()).hexdigest()
    if (value.get('identity_sha256') != digest or not isinstance(value.get('manifests'), dict)):
        raise SnapshotError('public snapshot protection index identity differs')
    return {Path(name).stem for name in value['manifests'] if isinstance(name, str)}


def _retained_source_pin_protections(cold: Path, dataset: str, *, source_root=None, pin_root=None,
                                     expected_cold=None) -> dict[str, str]:
    """Protect the original retained FRED pin, including while its manifest is missing."""
    from stockagent.data_sync import retained_source as retained
    expected_cold = Path(expected_cold or retained.DEFAULT_COLD).absolute()
    if dataset != 'fred-crypto-macro' or cold.absolute() != expected_cold:
        return {}
    source = Path(source_root or Path(__file__).resolve().parents[2]/'data_fred_crypto_macro').absolute()
    marker = source/retained.BINDING
    binding = json.loads(retained._read_binding(marker)[0]) if os.path.lexists(marker) else None
    if binding is not None and (not isinstance(binding, dict)
            or binding.get('contract') != retained.CONTRACT or binding.get('dataset') != dataset
            or binding.get('source') != str(source) or binding.get('cold_root') != str(expected_cold)):
        raise SnapshotError('retained FRED protection binding differs')
    root = Path(pin_root or retained.DEFAULT_PIN_ROOT).absolute()
    directory = root/'pins/retained-sources'
    if os.path.lexists(directory) and (directory.is_symlink() or not directory.is_dir()
                                        or directory.resolve(strict=True) != directory):
        raise SnapshotError('retained FRED protection directory is redirected')
    pins = sorted(itertools.islice(directory.glob(f'{dataset}-*.pin.json'), 4097)) if directory.is_dir() else []
    if len(pins) > 4096:
        raise SnapshotError('retained FRED protection pin count exceeds its bound')
    result = {}
    for pin in pins:
        match = re.fullmatch(re.escape(dataset)+r'-([a-f0-9]{64})\.pin\.json', pin.name)
        if not match or retained._pin_path(root, dataset, match.group(1)) != pin:
            raise SnapshotError('retained FRED protection pin name differs')
        digest = match.group(1)
        value = json.loads(retained._read_binding(pin)[0])
        manifest = value.get('manifest') if isinstance(value, dict) else None
        if (not isinstance(value, dict) or set(value) != {'schema_version', 'resolved_at', 'manifest_sha256', 'manifest'}
                or type(value.get('schema_version')) is not int or value['schema_version'] != 1
                or value.get('manifest_sha256') != digest or not isinstance(manifest, dict)
                or hashlib.sha256(p._canonical_json_bytes(manifest)).hexdigest() != digest):
            raise SnapshotError('retained FRED protection pin differs')
        p._validate_manifest(manifest)
        snapshot = p.validate_slug(manifest['snapshot_id'], 'retained FRED snapshot')
        if manifest['dataset'] != dataset or snapshot in result and result[snapshot] != digest:
            raise SnapshotError('retained FRED protection identity differs')
        if binding is not None and binding.get('manifest_sha256') == digest:
            if (binding.get('snapshot_id') != snapshot
                    or binding.get('inventory_sha256') != manifest['archive']['inventory']['sha256']):
                raise SnapshotError('retained FRED protection identity differs')
        result[snapshot] = digest
    if binding is not None:
        snapshot = p.validate_slug(binding.get('snapshot_id'), 'retained FRED snapshot')
        digest = p._validate_hash(binding.get('manifest_sha256'), 'retained FRED manifest')
        if result.get(snapshot) != digest:
            raise SnapshotError('retained FRED protection binding has no matching pin')
    return result


def _source_link_protections(cold: Path, census: dict, dataset: str) -> dict[str, str]:
    """Keep exact private futures dependencies while any saved manifest uses them.

    The full source manifest census is already fenced by the owner before and
    after planning. Only the small dependent manifest namespace needs decoding;
    contents and archives are never read for this reference check.
    """
    from stockagent.data_sync import private_source_links as links

    result = _retained_source_pin_protections(cold, dataset)
    for snapshot, digest in _pending_preparation_protections(cold, dataset).items():
        if snapshot in result and result[snapshot] != digest:
            raise SnapshotError('private preparation protection identities conflict')
        result[snapshot] = digest
    parent, sep, suffix = dataset.rpartition('-state-')
    is_state = bool(sep and re.fullmatch('[a-f0-9]{12}', suffix))
    if dataset != links.TARGET_DATASET and not is_state:
        return result
    prefixes = set()
    if dataset == links.TARGET_DATASET or is_state and parent == links.TARGET_DATASET:
        prefixes.add('manifests/local-tw-futures/')
    if is_state:
        prefixes.add(f'manifests/{parent}/')
    for relative, signature in census.items():
        if not any(relative.startswith(prefix) for prefix in prefixes):
            continue
        manifest = r._read_manifest(cold, relative)
        check_dependency_metadata(manifest)
        if stream_repack._signature(cold/relative) != signature:
            raise SnapshotError('private source-link manifest changed during reference census')
        dependencies = list(links.source_link_dependencies(links.manifest_source_links(SimpleNamespace(manifest=manifest))))
        raw = manifest.get('metadata', {}).get('consistent_sqlite_bindings')
        if is_state and manifest['dataset'] == parent and raw is not None:
            states = json.loads(raw)
            if not isinstance(states, list) or len(states) > 100000:
                raise SnapshotError('private SQLite dependency list is invalid')
            for state in states:
                source = state.get('source_relative') if isinstance(state, dict) else None
                if (not isinstance(source, str) or p._safe_relative_path(source, 'SQLite source').as_posix() != source
                        or state.get('dataset') != parent+'-state-'+hashlib.sha256(source.encode()).hexdigest()[:12]
                        or any(source+suffix not in manifest['source'].get('excluded_subtrees', [])
                               for suffix in ('', '-wal', '-shm', '-journal'))):
                    raise SnapshotError('private SQLite dependency is invalid')
                dependencies.append(state)
        for row in dependencies:
            if row['dataset'] != dataset:
                continue
            snapshot_id = p.validate_slug(row['snapshot_id'], 'private dependency snapshot')
            digest = p._validate_hash(row['manifest_sha256'], 'private dependency manifest SHA')
            if snapshot_id in result and result[snapshot_id] != digest:
                raise SnapshotError('private source-link dependency has conflicting identities')
            target = f'manifests/{dataset}/{snapshot_id}.json'
            if target not in census:
                raise SnapshotError('private source-link dependency manifest is missing')
            fixed = p.resolve_packed_snapshot_id(cold, dataset, snapshot_id, require_objects=False)
            if fixed.manifest_sha256 != digest or stream_repack._signature(cold/target) != census[target]:
                raise SnapshotError('private source-link dependency manifest changed or differs')
            result[snapshot_id] = digest
    return result


def _pending_preparation_protections(cold: Path, dataset: str, *,
        control=Path('/var/lib/stockagent/lakehouse-control/dataset-versions'),
        expected_cold=Path('/srv/stockagent-packed/.local-state/private-history')) -> dict[str, str]:
    """Protect exact SQLite side releases before their parent manifest commits."""
    from stockagent.data_sync.packed_member_stream import read_stable_bytes
    match = re.fullmatch(r'(.+)-state-([a-f0-9]{12})', dataset)
    if match is None or cold.absolute() != Path(expected_cold).absolute():
        return {}
    parent = p.validate_slug(match.group(1), 'preparation parent')
    control = Path(control).absolute()
    pointer = read_stable_bytes(control/'current.json', maximum=128*1024)
    current = json.loads(pointer)
    if not isinstance(current, dict):
        raise SnapshotError('private preparation owner pointer differs')
    directory = Path(current.get('operation_directory', ''))
    if (current.get('contract') != 'all_catalog_incremental_migration_v1'
            or directory.parent != control/'operations'
            or re.fullmatch('[a-f0-9]{16}-[a-f0-9]{32}', directory.name) is None
            or directory.is_symlink() or directory.resolve(strict=True) != directory):
        raise SnapshotError('private preparation owner pointer differs')
    path = directory/'preparations'/(parent+'.json')
    if not os.path.lexists(path):
        return {}
    value = json.loads(read_stable_bytes(path, maximum=32*1024**2))
    states = value.get('sqlite_states') if isinstance(value, dict) else None
    if (not isinstance(value, dict) or value.get('contract') != 'all_catalog_incremental_migration_v1' or value.get('dataset') != parent
            or not isinstance(states, list) or len(states) > 4096):
        raise SnapshotError('private preparation dependency metadata differs')
    result = {}
    for row in states:
        if not isinstance(row, dict):
            raise SnapshotError('private preparation dependency row differs')
        if row.get('dataset') != dataset or 'snapshot_id' not in row:
            continue
        relative = p._safe_relative_path(row.get('source_relative'), 'SQLite source').as_posix()
        if parent+'-state-'+hashlib.sha256(relative.encode()).hexdigest()[:12] != dataset:
            raise SnapshotError('private preparation dependency source differs')
        snapshot = p.validate_slug(row.get('snapshot_id'), 'prepared SQLite snapshot')
        digest = p._validate_hash(row.get('manifest_sha256'), 'prepared SQLite manifest SHA')
        if snapshot in result:
            raise SnapshotError('private preparation dependency is duplicated')
        result[snapshot] = digest
    if read_stable_bytes(control/'current.json', maximum=128*1024) != pointer:
        raise SnapshotError('private preparation owner changed during dependency read')
    return result


def retire(cold: Path, stage: Path, receipt: dict, *, public_index: Path, materialized: Path,
           retention_state: Path, extra_locks=(), guard=lambda: None, apply=False,
           process_check=process_references_many):
    cold, stage = cold.absolute(), stage.absolute()
    dataset = p.validate_slug(receipt.get('dataset'), 'dataset')
    old_sha = p._validate_hash(receipt.get('old_manifest_sha256'), 'old manifest SHA')
    new_sha = p._validate_hash(receipt.get('new_manifest_sha256'), 'new manifest SHA')
    new_id = p.validate_slug(receipt.get('new_snapshot_id'), 'new snapshot')
    mode = receipt.get('source_mode')
    from stockagent.data_sync import private_subset_reuse as subset
    if (receipt.get('contract') != 'stockagent.source_local_exact_repack.v1'
            or receipt.get('state') != 'new_D_head_committed_from_exact_E_source'
            or mode not in {'retained_handoff_C_original', 'D_streamed_to_E_dataset_pack', subset.SOURCE_MODE}
            or (mode in {'D_streamed_to_E_dataset_pack', subset.SOURCE_MODE}
                and dataset not in stream_repack.PRIVATE_REPACK_DATASETS)
            or (mode == subset.SOURCE_MODE and dataset not in subset.REVIEWED_PAIRS)
            or receipt.get('private') is not True or stage.is_symlink() or stage.resolve(strict=True) != stage):
        raise SnapshotError('private handoff publication receipt differs')
    prepared = stream_repack._prepared_metadata(stage, dataset, old_sha)
    if ((mode == 'retained_handoff_C_original' and prepared.get('source_mode') != mode)
            or (mode == 'D_streamed_to_E_dataset_pack' and prepared.get('source_mode') is not None)
            or (mode == subset.SOURCE_MODE and (prepared.get('source_mode') != mode
                or prepared.get('donor_dataset') != subset.REVIEWED_PAIRS[dataset]
                or receipt.get('donor_manifest_sha256') != prepared.get('donor_manifest_sha256')))):
        raise SnapshotError('private handoff preparation mode differs')
    old_id = p.validate_slug(prepared['old_snapshot_id'], 'old snapshot')
    old_relative = f'manifests/{dataset}/{old_id}.json'
    journal_path = stage/'private-D-retirement.json'
    if journal_path.is_symlink():
        raise SnapshotError('private D retirement journal redirected')
    journal = r.load_journal(journal_path) if journal_path.is_file() else None
    if journal is not None:
        if (journal.get('contract') != CONTRACT or journal.get('dataset') != dataset
                or journal.get('old_manifest_sha256') != old_sha or journal.get('new_manifest_sha256') != new_sha
                or journal.get('prepared_sha256') != prepared['identity_sha256']
                or not isinstance(journal.get('plan'), dict)
                or journal.get('plan_sha256') != stream_repack._identity(journal['plan'])
                or journal.get('state') not in ('planned', 'removing', 'completed')):
            raise SnapshotError('private D retirement journal identity differs')
        if journal['state'] == 'completed':
            return journal
        old_refs = set(journal['plan']['old_refs'])
    else:
        old = p.resolve_packed_snapshot_id(cold, dataset, old_id, require_objects=False)
        if old.manifest_sha256 != old_sha:
            raise SnapshotError('private old D release differs from the sealed source')
        if mode in {'D_streamed_to_E_dataset_pack', subset.SOURCE_MODE} and not stream_repack.private_repack_source_allowed(dataset, old.manifest):
            raise SnapshotError('private old D release belongs to an excluded source owner')
        old_refs = r._refs(old.manifest)

    # Snapshot the small metadata namespace, then decode it outside the owner
    # lock; a final signature census below detects concurrent publication.
    with r._boundary(cold, extra_locks):
        guard()
        census = _manifest_census(cold)
    dependency_pins = _source_link_protections(cold, census, dataset)
    references = {ref: set() for ref in old_refs}
    for relative, signature in census.items():
        manifest = r._read_manifest(cold, relative)
        check_dependency_metadata(manifest)
        if stream_repack._signature(cold/relative) != signature:
            raise SnapshotError('private D manifest changed during reference census')
        for ref in r._refs(manifest) & old_refs:
            references[ref].add(relative)
    known = {Path(name).stem for name in census} | _public_known(public_index) | {old_id, new_id}
    known = r.include_other_authority_repack_pins(cold, materialized, known)

    with r._boundary(cold, extra_locks):
        guard()
        if _manifest_census(cold) != census:
            raise SnapshotError('private D manifest namespace changed during reference census')
        # A retained-source pin can be created while the long reference scan
        # runs outside this owner. Re-read it under the final deletion lock.
        dependency_pins = _source_link_protections(cold, census, dataset)
        r.check_replacement(cold, stage, dataset, old_sha, new_sha, new_id, prepared)
        head_root = cold/'heads'/dataset
        if head_root.is_symlink() or not head_root.is_dir():
            raise SnapshotError('private D head namespace unavailable')
        heads = list(head_root.glob('*.json'))
        if not heads:
            raise SnapshotError('private D head namespace empty')
        for path in heads:
            head = json.loads(r._regular(cold, path.relative_to(cold).as_posix()).read_text())
            if head.get('dataset') != dataset or head.get('manifest_sha256') == old_sha or head.get('snapshot_id') == old_id:
                raise SnapshotError('old private D release is still a head or head identity differs')
        protected, _, errors = packed_retention._local_protected_snapshot_ids(materialized, known,
            research_pin_root=retention_state/'research-pins',
            research_pin_marker=retention_state/packed_retention.RESEARCH_PIN_MARKER)
        if errors or old_id in protected or old_id in dependency_pins:
            raise SnapshotError('private D release is pinned or protection metadata is unknown')
        histories = r._histories(cold, dataset, old_sha, old_id)
        deletable = sorted(ref for ref, owners in references.items() if not (owners-{old_relative}))
        if journal is None:
            # The same user's E-first transaction can already have removed
            # only exact exclusive private objects to make D installation fit.
            # Its durable journal is the sole authority for those absences.
            from stockagent.data_sync import staged_head_capacity_relief as relief
            prior = relief.load(stage, dataset, old_sha, prepared)
            pre_retired = set()
            if prior is not None:
                if prior.get('private') is not True or prior['state'] != 'completed':
                    raise SnapshotError('private current-head relief is not completed')
                pre_retired = set(prior['removed'])
                if (not pre_retired.issubset(old_refs)
                        or set(prior['plan'].get('old_refs', [])) != old_refs):
                    raise SnapshotError('private current-head relief references differ')
                if any(not os.path.lexists(cold/ref) and references.get(ref, set())-{old_relative}
                       for ref in pre_retired):
                    raise SnapshotError('a removed private object gained another manifest reference')
            items = []
            already_removed = []
            for kind, relative in ([('history', rel) for rel in histories]
                                   + [('manifest', old_relative)] + [('object', ref) for ref in deletable]):
                if kind == 'object' and not os.path.lexists(cold/relative) and relative in pre_retired:
                    already_removed.append(relative)
                    continue
                items.append(r.plan_member(cold, kind, relative))
            plan = {'old_snapshot_id': old_id, 'new_snapshot_id': new_id, 'old_refs': sorted(old_refs),
                    'items': items, 'private_manifest_count': len(census),
                    'already_removed_by_E_first_relief': already_removed}
            journal = {'contract': CONTRACT, 'dataset': dataset, 'old_manifest_sha256': old_sha,
                       'new_manifest_sha256': new_sha, 'prepared_sha256': prepared['identity_sha256'],
                       'plan': plan, 'plan_sha256': stream_repack._identity(plan), 'removed': [],
                       'state': 'planned', 'independent_D_readback': False, 'NAS_accepted': False}
        else:
            plan = journal['plan']
            objects = {item['relative'] for item in plan['items'] if item['kind'] == 'object'}
            planned_histories = {item['relative'] for item in plan['items'] if item['kind'] == 'history'}
            if not objects.issubset(deletable) or not set(histories).issubset(planned_histories):
                raise SnapshotError('private D object or historical head gained another reference')
        if process_check([r.member_path(cold, item, missing_ok=True) for item in plan['items']
                          if os.path.lexists(r.member_path(cold, item, missing_ok=True))]):
            raise SnapshotError('private old D release is in use')
        if not apply:
            return {**journal, 'apply': False, 'reclaimable_bytes': sum(
                item['bytes'] for item in plan['items'] if item['kind'] == 'object')}
        r.journalled_unlink(cold, journal_path, journal)
        journal.update(state='completed', completed_at_epoch=time.time(),
            deleted_object_bytes=sum(item['bytes'] for item in plan['items'] if item['kind'] == 'object' and 'placement' not in item),
            estimated_allocated_bytes_reclaimed=sum(item['allocated_bytes'] for item in plan['items'] if item['kind'] == 'object' and 'placement' not in item),
            deleted_object_count=sum(item['kind'] == 'object' and 'placement' not in item for item in plan['items']),
            estimated_E_allocated_bytes_reclaimed=sum(item['allocated_bytes'] for item in plan['items'] if 'placement' in item))
        r.save_journal(journal_path, journal)
        return journal
