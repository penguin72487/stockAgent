"""Promote a proven public superset by linking existing immutable D objects.

The source and destination stay under their original publication owners. The
new names share physical bytes; this is not a copy, NAS backup, payload reread,
or permission to retire either previous manifest. Link and head steps have a
durable intent so a crash cannot make an unrelated existing object acceptable.
"""
from __future__ import annotations

from contextlib import ExitStack
import copy
import itertools
import json
import os
from pathlib import Path
import time

from stockagent.data_sync import packed_snapshots as p, stream_repack as s
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json, _fsync_directory
from stockagent.data_sync.materialized_cache import process_references_many

CONTRACT = 'stockagent.semantic_link_promotion.v1'
LATEST_CONTRACT = 'stockagent.latest_fixed_link_promotion.v1'
EXACT_EQUAL_DATASETS = frozenset({'legacy-parquet', 'free-public-context'})
FAST_LATEST_DATASETS = frozenset({'binance', 'bybit', 'okx', 'yahoo-market', 'forex-pepperstone'}) | EXACT_EQUAL_DATASETS


def _allowed_latest_exclusions(dataset: str, value) -> bool:
    return (value == [] or dataset == 'free-public-context' and value == ['.download.lock']
            or dataset == 'bybit' and value == ['perpetual_daily/panel_cache_v2'])


def validate_latest_fixed_evidence(plan: dict, policy: dict) -> None:
    """Bind the user's fast latest-generation selection; do not claim equality."""
    if not isinstance(plan, dict):
        raise SnapshotError('latest fixed selection plan is invalid')
    generations = policy.get('generations') if isinstance(policy, dict) else None
    binding = generations.get(plan.get('dataset')) if isinstance(generations, dict) else None
    if (not isinstance(policy, dict)
            or policy.get('contract') != 'stockagent.fast_latest_generation_migration.v1'
            or policy.get('allow_uncompared_historical_values') is not True
            or not isinstance(policy.get('datasets'), list)
            or plan.get('dataset') not in policy['datasets']
            or plan.get('dataset') not in FAST_LATEST_DATASETS
            or not isinstance(binding, dict)
            or binding.get('public_manifest_sha256') != plan.get('public_manifest_sha256')
            or binding.get('private_manifest_sha256') != plan.get('private_manifest_sha256')
            or plan.get('contract') != 'stockagent.index_generation_diff.v1'
            or plan.get('identity_sha256') != s._identity(plan)
            or plan.get('state') != 'metadata_compared'
            or plan.get('source_payload_read') is not False
            or plan.get('semantic_merge_complete') is not False
            or not _allowed_latest_exclusions(plan.get('dataset'), plan.get('excluded_subtrees'))):
        raise SnapshotError('latest fixed selection policy, identity or source scope differs')
    counts = plan.get('counters')
    categories = plan.get('conflict_categories')
    if (not isinstance(counts, dict) or not isinstance(categories, dict)
            or any(counts.get(key) != 0 for key in ('public_only', 'private_excluded', 'unexpected_public_excluded'))
            or type(counts.get('conflict')) is not int or counts['conflict'] < 0
            or any(type(value) is not int or value < 0 for value in categories.values())
            or sum(categories.values()) != counts['conflict']):
        raise SnapshotError('latest fixed selection has unaccounted public paths')
    if plan.get('dataset') in EXACT_EQUAL_DATASETS and (
            type(counts.get('equal')) is not int or counts['equal'] <= 0
            or any(counts.get(key) != 0 for key in
                   ('conflict', 'public_only', 'private_only', 'private_excluded',
                    'unexpected_public_excluded', 'conflict_bytes_public', 'conflict_bytes_private'))
            or categories):
        raise SnapshotError('equal-only selection requires identical complete inventories')


def validate_binance_evidence(plan: dict, rows: dict, controls: dict, *, catalog_sha: str,
                              audit_identity: str | None = None) -> None:
    """Require every differing file class to prove a fixed public superset."""
    identity = plan.get('identity_sha256')
    if (plan.get('contract') != 'stockagent.index_generation_diff.v1'
            or identity != s._identity(plan) or plan.get('dataset') != 'binance-public-archive'
            or plan.get('excluded_subtrees') != []):
        raise SnapshotError('semantic promotion comparison identity or scope differs')
    expected_audit = p._validate_hash(audit_identity, 'audit comparison identity') if audit_identity is not None else identity
    counts = plan.get('counters', {})
    if (counts.get('public_only') != 0 or counts.get('private_excluded') != 0
            or counts.get('unexpected_public_excluded') != 0):
        raise SnapshotError('semantic promotion has unaccounted original paths')
    for value, contract in ((rows, 'stockagent.binance_archive_generation_audit.v1'),
                            (controls, 'stockagent.binance_archive_control_audit.v1')):
        proof = value.get('stream_scope')
        if (value.get('contract') != contract or value.get('state') != 'compared'
                or value.get('comparison_identity_sha256') != expected_audit
                or value.get('public_manifest_sha256') != plan.get('public_manifest_sha256')
                or value.get('private_manifest_sha256') != plan.get('private_manifest_sha256')
                or value.get('catalog_sha256') != catalog_sha or value.get('source_mutated') is not False
                or not isinstance(proof, dict) or proof.get('original_member_sha256_verified') is not True
                or proof.get('independent_recovery_verified') is not False):
            raise SnapshotError('semantic promotion source audit identity differs')
    categories = rows.get('categories', {})
    candle, archive_plan, lifecycle = (categories.get(key, {}) for key in ('candles', 'plan', 'lifecycle'))
    parquet = plan.get('conflict_categories', {}).get('parquet')
    if (type(parquet) is not int or parquet < 2
            or plan.get('conflict_categories') != {'parquet': parquet, 'json': 4, 'operational_state': 1}
            or counts.get('conflict') != parquet+5 or rows.get('files_total') != parquet
            or rows.get('files_completed') != parquet or rows.get('stream_scope', {}).get('files') != parquet
            or candle.get('files') != parquet-2 or archive_plan.get('files') != 1 or lifecycle.get('files') != 1
            or any(value.get('old_only_rows') != 0 for value in (candle, archive_plan, lifecycle))
            or candle.get('overlap_rows_with_any_change') != 0
            or candle.get('overlap_rows_with_price_change') != 0):
        raise SnapshotError('semantic promotion needs a row-level merge')
    state = controls.get('state_comparison', {})
    if (controls.get('files_total') != 5 or controls.get('files_completed') != 5
            or controls.get('stream_scope', {}).get('files') != 5
            or set(controls.get('changed_json_fields', {})) != {'capacity_receipt.json', 'download_summary.json',
                                                                'plan_summary.json', 'progress.json'}
            or any(state.get(key) != 0 for key in ('old_only_rows', 'completed_regressions_same_etag',
                                                   'hash_conflicts_same_etag'))):
        raise SnapshotError('semantic promotion needs a downloader-state merge')


def _same_original_after_link(before: list, after: list) -> bool:
    # Creating exactly one hardlink changes ctime and link count, not the
    # immutable inode, mode, byte length or mtime. Mounted D is guarded by the
    # caller; Linux dev numbers may change after a WSL remount.
    return (isinstance(before, list) and isinstance(after, list) and len(before) == len(after) == 7
            and before[1:5] == after[1:5] and before[6] == 1 and after[6] == 2)


def _linked_generation_matches(private, cold, item, linked):
    """Verify a hardlink or an existing public object with the same fixed SHA."""
    origin, target = private/item['relpath'], cold/item['relpath']
    if origin.is_symlink() or target.is_symlink():
        return False
    if linked.get('mode') == 'existing_public_same_sha':
        return (s._signature_matches(origin, linked.get('source_signature'))
                and s._signature_matches(target, linked.get('target_signature')))
    return (os.path.samefile(origin, target)
            and s._signature_matches(origin, linked.get('source_signature')))


def _verify_equal_inventories(cold: Path, public, private: Path, fixed, expected: int,
                              excluded_subtrees=()) -> int:
    """Compare every sealed inventory row, ignoring only its encoding location.

    The canonical inventory reader checks the compressed inventory SHA. This
    does not reread archive payloads or claim independent data recovery.
    """
    count = 0
    for old, new in itertools.zip_longest(p._iter_inventory(cold, public.manifest),
                                           p._iter_inventory(private, fixed.manifest)):
        if not isinstance(old, dict) or not isinstance(new, dict):
            raise SnapshotError('equal-only inventories have different lengths')
        left = {key: value for key, value in old.items() if key != 'storage'}
        right = {key: value for key, value in new.items() if key != 'storage'}
        kind = left.get('kind')
        if (left != right or kind not in {'file', 'directory', 'symlink'}
                or type(left.get('mode')) is not int):
            raise SnapshotError('equal-only inventory row differs')
        if any(left['path'] == root or left['path'].startswith(root+'/') for root in excluded_subtrees):
            raise SnapshotError('equal-only inventory contains an excluded path')
        if kind == 'file':
            if type(left.get('size')) is not int or left['size'] < 0:
                raise SnapshotError('equal-only inventory file size differs')
            p._validate_hash(left.get('sha256'), 'equal-only inventory file SHA')
        count += 1
    if count != expected or count == 0:
        raise SnapshotError('equal-only inventory count differs from its comparison')
    return count


def public_source_matches_fixed(public: dict, fixed: dict, plan: dict) -> bool:
    """Only an explicitly reviewed public source exclusion may differ."""
    expected = dict(fixed)
    if plan.get('dataset') in EXACT_EQUAL_DATASETS | {'bybit'}:
        exclusions = plan.get('excluded_subtrees')
        if not _allowed_latest_exclusions(plan['dataset'], exclusions):
            return False
        expected['excluded_subtrees'] = exclusions
    return public == expected


def promote(cold: Path, private: Path, *, dataset: str, old_sha: str, private_id: str,
            private_sha: str, prepared: dict, install: dict, plan: dict, rows: dict, controls: dict,
            catalog_sha: str,
            catalog_entry: dict, journal_path: Path, guard=lambda: None,
            check_processes=process_references_many, notify=True, latest_policy=None, dry_run=False) -> dict:
    """Publish fixed private objects as a public head, without copying payloads."""
    from scripts.publish_data_releases import catalog_publication_identity

    cold, private, journal_path = Path(cold).absolute(), Path(private).absolute(), Path(journal_path).absolute()
    dataset = p.validate_slug(dataset, 'semantic dataset')
    if type(dry_run) is not bool:
        raise SnapshotError('promotion dry-run selection is invalid')
    latest = latest_policy is not None
    contract = LATEST_CONTRACT if latest else CONTRACT
    if latest:
        validate_latest_fixed_evidence(plan, latest_policy)
        if plan.get('dataset') != dataset:
            raise SnapshotError('latest fixed selection belongs to another dataset')
    elif dataset != 'binance-public-archive':
        raise SnapshotError('semantic link promotion is enrolled only for Binance public archive')
    old_sha = p._validate_hash(old_sha, 'old public manifest')
    private_sha = p._validate_hash(private_sha, 'fixed private manifest')
    if not latest:
        validate_binance_evidence(plan, rows, controls, catalog_sha=catalog_sha)
    comparison_id = p._validate_hash(plan.get('identity_sha256'), 'comparison identity')
    private_id = p.validate_slug(private_id, 'fixed private snapshot')
    if (p._paths_overlap(cold, journal_path) or journal_path.is_symlink()
            or not journal_path.parent.is_dir() or journal_path.parent.resolve(strict=True) != journal_path.parent
            or catalog_entry.get('dataset') != dataset or catalog_entry.get('publish') is not True
            or catalog_entry.get('excluded_subtrees', []) != (plan.get('excluded_subtrees') if latest else [])
            or plan.get('public_manifest_sha256') != old_sha or plan.get('private_manifest_sha256') != private_sha
            or plan.get('private_snapshot_id') != private_id):
        raise SnapshotError('semantic promotion authority or catalog scope differs')
    if cold.stat().st_dev != private.stat().st_dev:
        raise SnapshotError('semantic object links require one guarded physical filesystem')
    source = p.resolve_packed_snapshot_id(private, dataset, private_id, require_objects=False)
    if (source.manifest_sha256 != private_sha or source.manifest['source'].get('excluded_subtrees') != []
            or source.manifest.get('metadata', {}).get('source_mode') != 'retained_handoff_C_index'
            or prepared.get('contract') != s.CONTRACT+'.prepared' or prepared.get('dataset') != dataset
            or prepared.get('identity_sha256') != s._identity(prepared)
            or prepared.get('source_mode') != 'retained_handoff_C_index'
            or source.manifest['metadata'].get('migration_source_identity_sha256') != prepared.get('old_manifest_sha256')
            or prepared.get('source_member_sha_streamed') is not True
            or prepared.get('destination_independent_readback') is not False
            or install.get('contract') != s.CONTRACT+'.install'
            or install.get('old_manifest_sha256') != prepared.get('old_manifest_sha256')
            or install.get('prepared_sha256') != prepared.get('identity_sha256')
            or install.get('new_manifest_sha256') != private_sha):
        raise SnapshotError('semantic promotion fixed source or sealed installation differs')
    objects = [source.manifest['archive']['inventory'], *source.manifest['archive']['objects']]
    descriptors = {item['sha256']: item for item in objects}
    installed_rows = install.get('installed', [])
    installed = {item.get('sha256'): item for item in installed_rows if isinstance(item, dict)}
    prepared_rows = [*prepared.get('packages', []), prepared.get('inventory', {})]
    sealed = {item.get('sha256'): item for item in prepared_rows if isinstance(item, dict)}
    if (len(descriptors) != len(objects) or len(installed) != len(installed_rows)
            or len(sealed) != len(prepared_rows) or set(installed) != set(descriptors) or set(sealed) != set(descriptors)
            or any(descriptors[sha]['bytes'] != sealed[sha].get('bytes') for sha in descriptors)):
        raise SnapshotError('semantic promotion installation object set differs')
    if (source.manifest['source']['files'] != prepared.get('files')
            or source.manifest['source']['logical_bytes'] != prepared.get('logical_bytes')):
        raise SnapshotError('semantic promotion source counts differ from its sealed build')
    journal = json.loads(journal_path.read_bytes()) if journal_path.is_file() else {}
    if journal and (journal.get('contract') != contract or journal.get('dataset') != dataset
            or journal.get('old_manifest_sha256') != old_sha or journal.get('private_manifest_sha256') != private_sha
            or journal.get('comparison_identity_sha256') != comparison_id):
        raise SnapshotError('semantic promotion journal identity differs')
    if dry_run and journal:
        raise SnapshotError('dry run cannot resume an existing promotion journal')

    private_locks, public_locks = private/'.local-state/locks', cold/'.local-state/locks'
    with ExitStack() as locks:
        # All locks are nonblocking. Keep the existing cross-authority order.
        for lock, shared in ((private_locks/'publish-retention-global.lock', True),
                             (public_locks/'publish-retention-global.lock', True),
                             (private_locks/f'publish-{dataset}.lock', False),
                             (public_locks/f'publish-{dataset}.lock', False)):
            locks.enter_context(p._try_publication_lock(lock, shared=shared))
        guard()
        current = p.resolve_latest_packed(cold, dataset, require_objects=False)
        if current.manifest_sha256 != old_sha:
            if (journal.get('state') in {'head_pending', 'committed'}
                    and current.manifest_sha256 == journal.get('new_manifest_sha256')
                    and current.manifest == journal.get('manifest')):
                linked = {item['sha256']: item for item in journal.get('linked', [])}
                if (set(linked) != set(descriptors) or len(linked) != len(journal.get('linked', []))
                        or any(not _linked_generation_matches(private, cold, item, linked[item['sha256']])
                               for item in objects)):
                    raise SnapshotError('committed semantic promotion object generation differs')
                p.resolve_latest_packed(cold, dataset, require_objects=True)
                journal.update(state='committed', new_snapshot_id=current.manifest['snapshot_id'],
                               linked_object_count=len(linked), payload_bytes_copied=0,
                               observed_at_epoch=time.time())
                atomic_write_json(journal_path, journal, durable=True)
                if notify:
                    p._notify_packed_publication(cold, dataset,
                        new_object_paths=tuple(item['relpath'] for item in objects),
                        new_manifest_paths=(current.manifest_path.relative_to(cold).as_posix(),), defer_scan=True)
                return journal
            raise SnapshotError('semantic promotion public head changed')
        fixed = p.resolve_packed_snapshot_id(private, dataset, private_id, require_objects=False)
        if fixed.manifest_sha256 != private_sha:
            raise SnapshotError('semantic promotion private manifest changed')
        if latest and dataset in EXACT_EQUAL_DATASETS:
            _verify_equal_inventories(cold, current, private, fixed, plan['counters']['equal'],
                                      plan['excluded_subtrees'])
        source_paths = [private/item['relpath'] for item in objects]
        if check_processes(source_paths):
            raise SnapshotError('semantic promotion source objects are in use')
        old_objects = {item['sha256']: item for item in
                       [current.manifest['archive']['inventory'], *current.manifest['archive']['objects']]}
        if not journal:
            reused = []
            for item in objects:
                origin, target = private/item['relpath'], cold/item['relpath']
                prior = old_objects.get(item['sha256'])
                reusable = (latest and prior is not None and prior.get('relpath') == item['relpath']
                            and prior.get('bytes') == item['bytes'] and not target.is_symlink()
                            and target.is_file() and target.stat().st_size == item['bytes'])
                if (not s._signature_matches(origin, installed[item['sha256']].get('D_signature'))
                        or os.path.lexists(target) and not reusable):
                    raise SnapshotError('semantic promotion source generation or target differs')
                if reusable:
                    reused.append(item)
            if dry_run:
                return {'contract': contract, 'dataset': dataset, 'state': 'preflight_only',
                    'old_manifest_sha256': old_sha, 'private_manifest_sha256': private_sha,
                    'object_count': len(objects), 'reused_existing_objects': len(reused),
                    'duplicate_encoded_bytes_pending': sum(item['bytes'] for item in reused
                        if not os.path.samefile(private/item['relpath'], cold/item['relpath'])),
                    'changed_member_values_compared': False if latest else True,
                    'old_historical_value_coverage': 'not_compared' if latest else 'semantic_audit',
                    'payload_bytes_copied': 0, 'head_changed': False, 'source_deleted': False}
            node = p._resolve_node_id(cold, None)
            now = time.time_ns()
            stamp = p.next_hlc(p._observed_stamps(cold, dataset, now_ns=now,
                max_clock_skew_seconds=p.DEFAULT_MAX_CLOCK_SKEW_SECONDS, require_objects=False), node_id=node, now_ns=now)
            seconds, nanos = divmod(stamp.physical_ns, 1_000_000_000)
            snapshot_id = p.validate_slug(f"{dataset[:40]}-{time.strftime('%Y%m%dT%H%M%S', time.gmtime(seconds))}"
                f"{nanos:09d}Z-l{stamp.logical}-{node[:40]}-{source.manifest['archive']['inventory']['sha256'][:16]}", 'snapshot')
            manifest = copy.deepcopy(source.manifest)
            manifest.update(snapshot_id=snapshot_id, published_at=p._utc_iso_from_ns(stamp.physical_ns),
                            hlc=stamp.to_dict(), publisher={'node_id': node})
            if latest and dataset in EXACT_EQUAL_DATASETS | {'bybit'}:
                # The sealed C handoff excluded writer-control locks without
                # listing them as source data. Preserve the public catalog's
                # explicit exclusion after proving neither inventory has it.
                manifest['source']['excluded_subtrees'] = plan['excluded_subtrees']
            manifest['archive']['base_snapshot_id'] = current.manifest['snapshot_id']
            duplicate_bytes = sum(item['bytes'] for item in objects
                                  if latest and item['sha256'] in old_objects
                                  and os.path.lexists(cold/item['relpath'])
                                  and not os.path.samefile(private/item['relpath'], cold/item['relpath']))
            manifest['metadata'] = {'role': str(catalog_entry['role']), 'catalog_schema': '1',
                'catalog_publication_contract': 'catalog_entry_publication_v1',
                'catalog_entry_sha256': catalog_publication_identity(catalog_entry),
                'shared_physical_object_names': duplicate_bytes == 0, 'independent_D_readback': False, 'NAS_accepted': False}
            if latest:
                manifest['metadata'].update(latest_replaced_original_manifest_sha256=old_sha,
                    latest_fixed_private_manifest_sha256=private_sha, latest_comparison_identity_sha256=comparison_id,
                    selection_contract=LATEST_CONTRACT, changed_member_values_compared=False,
                    old_historical_value_coverage='not_compared', uncompared_changed_members=plan['counters']['conflict'],
                    duplicate_encoded_bytes_pending=duplicate_bytes)
            else:
                manifest['metadata'].update(semantic_superset_original_manifest_sha256=old_sha,
                    semantic_superset_private_manifest_sha256=private_sha,
                    semantic_comparison_identity_sha256=comparison_id)
            p._validate_manifest(manifest)
            journal = {'contract': contract, 'dataset': dataset, 'state': 'linking',
                'old_manifest_sha256': old_sha, 'private_manifest_sha256': private_sha,
                'private_snapshot_id': private_id, 'comparison_identity_sha256': comparison_id,
                'manifest': manifest, 'linked': [], 'independent_D_readback': False,
                'NAS_accepted': False, 'old_sources_deleted': False}
            if latest:
                journal.update(changed_member_values_compared=False, old_historical_value_coverage='not_compared',
                               uncompared_changed_members=plan['counters']['conflict'],
                               duplicate_encoded_bytes_pending=duplicate_bytes)
            atomic_write_json(journal_path, journal, durable=True)
        manifest = journal.get('manifest')
        p._validate_manifest(manifest)
        keys = (('latest_replaced_original_manifest_sha256', 'latest_fixed_private_manifest_sha256',
                 'latest_comparison_identity_sha256') if latest else
                ('semantic_superset_original_manifest_sha256', 'semantic_superset_private_manifest_sha256',
                 'semantic_comparison_identity_sha256'))
        if (manifest['dataset'] != dataset or manifest['metadata'].get(keys[0]) != old_sha
                or manifest['metadata'].get(keys[1]) != private_sha
                or manifest['metadata'].get(keys[2]) != comparison_id
                or latest and (manifest['metadata'].get('selection_contract') != LATEST_CONTRACT
                               or manifest['metadata'].get('changed_member_values_compared') is not False
                               or manifest['metadata'].get('old_historical_value_coverage') != 'not_compared')):
            raise SnapshotError('semantic promotion planned manifest differs')
        linked = {item['sha256']: item for item in journal.get('linked', [])}
        if len(linked) != len(journal.get('linked', [])) or not set(linked).issubset(descriptors):
            raise SnapshotError('semantic promotion link journal is invalid')
        for item in objects:
            sha, rel = item['sha256'], item['relpath']
            origin, target = private/rel, cold/rel
            p._ensure_shared_packed_directory(cold, target.parent)
            if sha in linked:
                if not _linked_generation_matches(private, cold, item, linked[sha]):
                    raise SnapshotError('semantic promotion linked object changed')
                continue
            before = installed[sha]['D_signature']
            if os.path.lexists(target):
                prior = old_objects.get(sha)
                reusable = (latest and prior is not None and prior.get('relpath') == item['relpath']
                            and prior.get('bytes') == item['bytes'] and not target.is_symlink()
                            and target.is_file() and target.stat().st_size == item['bytes']
                            and s._signature_matches(origin, before))
                if reusable:
                    linked[sha] = {'sha256': sha, 'mode': 'existing_public_same_sha',
                                   'source_signature': s._signature(origin), 'target_signature': s._signature(target)}
                    journal['linked'] = list(linked.values())
                    atomic_write_json(journal_path, journal, durable=True)
                    continue
                if (journal.get('pending') != sha or target.is_symlink() or not os.path.samefile(origin, target)
                        or not _same_original_after_link(before, s._signature(origin))):
                    raise SnapshotError('semantic promotion unjournalled target exists')
            else:
                if not s._signature_matches(origin, before):
                    raise SnapshotError('semantic promotion source object changed')
                journal['pending'] = sha
                atomic_write_json(journal_path, journal, durable=True)
                os.link(origin, target, follow_symlinks=False)
                _fsync_directory(target.parent)
                if not os.path.samefile(origin, target) or not _same_original_after_link(before, s._signature(origin)):
                    raise SnapshotError('semantic promotion hardlink generation differs')
            linked[sha] = {'sha256': sha, 'source_signature': s._signature(origin)}
            journal['linked'] = list(linked.values())
            journal.pop('pending', None)
            atomic_write_json(journal_path, journal, durable=True)
        if any(not _linked_generation_matches(private, cold, item, linked[item['sha256']]) for item in objects):
            raise SnapshotError('semantic promotion object generation changed before commit')
        guard()
        if p.resolve_latest_packed(cold, dataset, require_objects=False).manifest_sha256 != old_sha:
            raise SnapshotError('semantic promotion old head changed before commit')
        rel = Path('manifests')/dataset/(manifest['snapshot_id']+'.json')
        p._ensure_shared_packed_directory(cold, (cold/rel).parent)
        new_sha = p.write_immutable_json(cold/rel, manifest)
        journal.update(state='head_pending', new_manifest_sha256=new_sha)
        atomic_write_json(journal_path, journal, durable=True)
        node = manifest['publisher']['node_id']
        head_path = cold/'heads'/dataset/(node+'.json')
        p._ensure_shared_packed_directory(cold, head_path.parent)
        p._archive_d_primary_head(cold, head_path)
        head = {'schema_version': p.PACKED_HEAD_SCHEMA_VERSION, 'dataset': dataset, 'node_id': node,
                'snapshot_id': manifest['snapshot_id'], 'hlc': manifest['hlc'],
                'updated_at': manifest['published_at'], 'manifest_relpath': rel.as_posix(),
                'manifest_sha256': new_sha}
        atomic_write_json(head_path, head, durable=True)
        if p.resolve_latest_packed(cold, dataset, require_objects=True).manifest_sha256 != new_sha:
            raise SnapshotError('semantic promotion committed head is not current')
        journal.update(state='committed', new_manifest_sha256=new_sha, new_snapshot_id=manifest['snapshot_id'],
                       linked_object_count=len(linked), payload_bytes_copied=0, observed_at_epoch=time.time())
        atomic_write_json(journal_path, journal, durable=True)
    if notify:
        p._notify_packed_publication(cold, dataset,
            new_object_paths=tuple(item['relpath'] for item in objects),
            new_manifest_paths=(rel.as_posix(),), defer_scan=True)
    return journal
