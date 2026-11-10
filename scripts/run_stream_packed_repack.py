#!/usr/bin/env python3
"""Stream one fixed D release into E dataset packs, then publish them on D."""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from functools import lru_cache
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync import packed_snapshots as packed, retirement_turn, source_repack_bloat, staged_head_capacity_relief, stream_repack, workspace_volume
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.completed_forward_capacity import reserve_capacity
from stockagent.data_sync.dataset_migration import private_history_root
from stockagent.data_sync.dataset_version_layout import version_policy
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.packed_member_stream import read_stable_bytes
from scripts.run_bounded_storage_maintenance import TradingPriorityGuard

COLD = Path('/srv/stockagent-packed')
D_INSTALL_RESERVE = 32 * 1024**3
# The user released the old backup allocation for these exact migrations.
# Each scoped physical floor is rechecked; all other generations keep 32 GiB.
_ONE_TIME_D_RESERVE = {
    # The user released the old backup reserve for the E-first migration.
    # This exact generation has its only complete local source on E while a
    # previously started D writer finishes. Keep an 8-GiB physical floor and
    # recheck it for every native write; no other dataset inherits this value.
    ('binance', 'ce0f8747ca4da7b6f18184711a702d2a67e0fd81549f4384fc86e56aa77e28b8', False): 8 * 1024**3,
    ('binance-public-archive', '5288d22ed33fc707497278c7ce218b56b67a35b98277b7cab69542f01445cf35', False): 12 * 1024**3,
    ('local-tw-index-derivatives-ticks', '5e015fa56b850933a460a020095e837d8953e8307f9824afe1709a43fdcc0a21', True): 12 * 1024**3,
}

# A retired Syncthing peer left this exact head behind when it was removed
# from the packed folder.  Other lab203 datasets or generations are not
# covered by the historical de-enrollment receipt.
_RETIRED_LAB203_DATASET = 'stockagent-migration-core'
_RETIRED_LAB203_MANIFEST = 'd2c97c13d2b9e04a30a2f2ac119f815f6a7d5939bd83fb3e795c20346f9cce56'
_RETIRED_LAB203_PREPARED = '840c362960d470632bb20fbb4596dea25f61a930ef144993861628e77eba065e'
_RETIRED_LAB203_RECEIPT = Path('/var/lib/stockagent-peer-retirement/lab203-retirement.json')


def install_reserve_bytes(dataset: str, expected_sha: str, *, private: bool) -> int:
    return _ONE_TIME_D_RESERVE.get((dataset, expected_sha, private), D_INSTALL_RESERVE)


def _source_head_nodes(cold: Path, dataset: str) -> dict[str, str]:
    """Read the bounded, validated publisher-head namespace."""
    root = cold/'heads'/packed.validate_slug(dataset, 'dataset')
    if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
        raise SnapshotError('source head namespace unavailable or redirected')
    heads = list(root.glob('*.json'))
    if not heads or len(heads) > 1024:
        raise SnapshotError('source head namespace is empty or exceeds its bound')
    result = {}
    for path in heads:
        if path.is_symlink() or not path.is_file():
            raise SnapshotError('source head is redirected')
        head = json.loads(read_stable_bytes(path, maximum=65536))
        if not isinstance(head, dict) or head.get('dataset') != dataset:
            raise SnapshotError('source head identity differs')
        node = packed.validate_slug(str(head.get('node_id', '')), 'head node')
        digest = packed._validate_hash(head.get('manifest_sha256'), 'head manifest SHA')
        if path.name != node+'.json':
            raise SnapshotError('source head filename differs from publisher')
        result[node] = digest
    return result


def foreign_source_head(cold: Path, dataset: str, expected_sha: str, publisher: str) -> bool:
    """Detect an old current head the new publisher cannot replace itself."""
    return any(digest == expected_sha and node != publisher
               for node, digest in _source_head_nodes(cold, dataset).items())


@lru_cache(maxsize=2)
def _handoff_timer_active(_time_bucket: int) -> bool:
    try:
        return subprocess.run(['systemctl', 'is-active', '--quiet', 'stockagent-foreign-head-handoff.timer'],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def _retired_lab203_successor_ready(cold: Path, dataset: str, expected: str,
                                    publisher: str, prepared: dict, *, with_successor=False) -> bool:
    """Allow only the fixed source-hashed successor of the retired peer.

    This observes the current sharing topology; failure or ambiguity keeps
    the original wait. It never removes the foreign head or its objects.
    """
    if (dataset != _RETIRED_LAB203_DATASET or expected != _RETIRED_LAB203_MANIFEST
            or publisher != 'penguin' or prepared.get('identity_sha256') != _RETIRED_LAB203_PREPARED
            or prepared.get('source_member_sha_streamed') is not True
            or prepared.get('packed_format') is not None):
        return False
    try:
        from scripts import retire_lab203_syncthing as retired
        receipt = json.loads(read_stable_bytes(_RETIRED_LAB203_RECEIPT, maximum=65536))
        if (not isinstance(receipt, dict) or receipt.get('action') != 'retired'
                or receipt.get('device_id') != retired.LAB203_DEVICE_ID
                or receipt.get('cold_dataset') != dataset
                or receipt.get('cold_manifest_sha256') != expected
                or receipt.get('cold_snapshot_id') != retired.LAB203_SNAPSHOT_ID
                or prepared.get('old_snapshot_id') != retired.LAB203_SNAPSHOT_ID):
            return False
        heads = _source_head_nodes(cold, dataset)
        if (not with_successor and heads != {'lab203': expected}
                or with_successor and (set(heads) != {'lab203', 'penguin'}
                                       or heads['lab203'] != expected)):
            return False
        base, headers = retired._api()
        config = retired._get(base, headers, '/rest/config', timeout=2)
        if not isinstance(config, dict):
            return False
        devices, folders = config.get('devices'), config.get('folders')
        if (not isinstance(devices, list) or not isinstance(folders, list)
                or any(not isinstance(item, dict) for item in devices + folders)):
            return False
        packed_folders = [folder for folder in folders if folder.get('id') == retired.PACKED_FOLDER_ID]
        if len(packed_folders) != 1:
            return False
        folder = packed_folders[0]
        participants = folder.get('devices')
        if (folder.get('paused') is not False or folder.get('type') != 'sendreceive'
                or not isinstance(participants, list)
                or any(not isinstance(item, dict) or not isinstance(item.get('deviceID'), str)
                       for item in participants)):
            return False
        penguins = [item.get('deviceID') for item in devices if item.get('name') == 'penguin']
        if len(penguins) != 1 or not isinstance(penguins[0], str):
            return False
        packed_ids = {item['deviceID'] for item in participants}
        lab_ids = {retired.LAB203_DEVICE_ID} | {
            item['deviceID'] for item in devices
            if item.get('name') == 'lab203' and isinstance(item.get('deviceID'), str)}
        return penguins[0] in packed_ids and not (lab_ids & packed_ids)
    except (OSError, ValueError, KeyError, TypeError, SnapshotError):
        return False


def require_retired_lab203_before_commit(cold: Path, stage: Path, dataset: str, expected: str,
                                         publisher: str, prepared: dict) -> None:
    """Never strand an already-started E-only recovery if peer topology changes."""
    if (dataset != _RETIRED_LAB203_DATASET or expected != _RETIRED_LAB203_MANIFEST
            or publisher != 'penguin' or prepared.get('identity_sha256') != _RETIRED_LAB203_PREPARED
            or prepared.get('source_member_sha_streamed') is not True
            or prepared.get('packed_format') is not None):
        raise SnapshotError('retired lab203 fixed recovery identity changed before commit')
    relief = staged_head_capacity_relief.load(stage, dataset, expected, prepared)
    started = relief is not None and (relief.get('removed_count', 0) > 0 or relief.get('pending'))
    if started:
        if relief.get('state') != 'completed' or _source_head_nodes(cold, dataset) != {'lab203': expected}:
            raise SnapshotError('retired lab203 E-only recovery head or journal changed')
        return
    if not _retired_lab203_successor_ready(cold, dataset, expected, publisher, prepared):
        raise SnapshotError('retired lab203 packed topology or fixed head changed before commit')


def foreign_handoff_successor_ready(cold: Path, dataset: str, expected: str,
                                    publisher: str, prepared: dict) -> bool:
    """Admit a fitting D successor for a specifically enrolled handoff.

    This is a scheduling gate, never permission to remove old data. The writer
    independently checks sealed input, capacity and current head under its
    owners; the separate handoff and retirement owners check the peer and pins.
    """
    if (publisher != 'penguin' or prepared.get('source_member_sha_streamed') is not True
            or prepared.get('packed_format') == 'content-alias-v3'):
        return False
    if _retired_lab203_successor_ready(cold, dataset, expected, publisher, prepared):
        return True
    return (_handoff_timer_active(int(time.monotonic() // 30))
            and _source_head_nodes(cold, dataset) == {'vastai1T': expected})


def defer_foreign_head_install(cold: Path, stage: Path, dataset: str, expected: str,
                               publisher: str, prepared: dict, journal: dict) -> bool:
    """Never interrupt an existing install or an E-only recovery transaction."""
    if journal:
        return False  # The caller already validated the exact install journal.
    relief = staged_head_capacity_relief.load(stage, dataset, expected, prepared)
    if relief is not None and (relief.get('removed_count', 0) > 0 or relief.get('pending')):
        return False
    if foreign_handoff_successor_ready(cold, dataset, expected, publisher, prepared):
        return False
    return foreign_source_head(cold, dataset, expected, publisher)


def cleanup_completed_stage(cold: Path, stage: Path, receipt: dict, *, guard=lambda: None) -> dict:
    """Withdraw a temporary E view before removing its fixed stage members."""
    from stockagent.data_sync import temporary_e_authority
    if os.path.lexists(stage/temporary_e_authority.MARKER):
        receipt['E_temporary_authority_retirement'] = temporary_e_authority.retire(
            cold, stage, receipt, guard=guard)
    else:
        prior = stage/'temporary-authority-retirement.json'
        if os.path.lexists(prior):
            if prior.is_symlink() or not prior.is_file():
                raise SnapshotError('temporary E authority retirement receipt is redirected')
            recorded = json.loads(prior.read_text())
            if (not isinstance(recorded, dict)
                    or recorded.get('contract') != temporary_e_authority.CONTRACT+'.retirement'
                    or recorded.get('state') != 'retired'
                    or recorded.get('dataset') != receipt.get('dataset')
                    or recorded.get('old_manifest_sha256') != receipt.get('old_manifest_sha256')
                    or recorded.get('new_manifest_sha256') != receipt.get('new_manifest_sha256')):
                raise SnapshotError('temporary E authority retirement receipt differs')
            receipt['E_temporary_authority_retirement'] = recorded
    return stream_repack.cleanup_stage(cold, stage, receipt, guard=guard)


def private_publisher(cold: Path, dataset: str, latest) -> str:
    """Replace the same private head; never publish a second private owner."""
    if not stream_repack.private_repack_source_allowed(dataset, latest.manifest):
        raise SnapshotError('private streaming dataset belongs to an excluded source owner')
    from stockagent.data_sync.private_handoff_retirement import check_dependency_metadata
    check_dependency_metadata(latest.manifest)
    node = packed.validate_slug(latest.manifest.get('publisher', {}).get('node_id'), 'private publisher')
    head_root = cold/'heads'/dataset
    if head_root.is_symlink() or not head_root.is_dir() or head_root.resolve(strict=True) != head_root:
        raise SnapshotError('private head namespace unavailable')
    heads = list(head_root.glob('*.json'))
    if len(heads) != 1 or heads[0].name != node+'.json' or heads[0].is_symlink():
        raise SnapshotError('private dataset does not have one fixed publisher head')
    head = json.loads(heads[0].read_text())
    if (head.get('dataset') != dataset or head.get('node_id') != node
            or head.get('snapshot_id') != latest.manifest['snapshot_id']
            or head.get('manifest_sha256') != latest.manifest_sha256):
        raise SnapshotError('private publisher head does not match the selected release')
    return node


def remaining_install_bytes(cold: Path, prepared: dict, journal: dict) -> int:
    """Count only bytes not already committed by this exact transaction."""
    items = [*prepared['packages'], prepared['inventory']]
    if not journal:
        return sum(item['bytes'] for item in items)
    if (journal.get('contract') != stream_repack.CONTRACT+'.install'
            or journal.get('dataset') != prepared['dataset']
            or journal.get('old_manifest_sha256') != prepared['old_manifest_sha256']
            or journal.get('prepared_sha256') != prepared['identity_sha256']
            or not isinstance(journal.get('installed'), list)):
        raise SnapshotError('streaming install journal differs from prepared stage')
    installed = {item['sha256']: item for item in journal['installed']}
    if len(installed) != len(journal['installed']):
        raise SnapshotError('streaming install journal has duplicate objects')
    remaining = 0
    for item in items:
        digest = item['sha256']
        suffix = '.jsonl.gz' if item is prepared['inventory'] else '.zip'
        kind = 'inventories' if item is prepared['inventory'] else 'packs'
        target = cold.joinpath(*packed._object_relpath(kind, digest, suffix).parts)
        known = installed.pop(digest, None)
        if known is not None:
            if target.is_symlink() or not target.is_file() or not stream_repack._signature_matches(target, known.get('D_signature')):
                raise SnapshotError('installed D generation changed during retry')
            continue
        pending = journal.get('pending')
        if (isinstance(pending, dict) and pending.get('sha256') == digest
                and pending.get('target_absent_before') is True and os.path.lexists(target)):
            if target.is_symlink() or not target.is_file() or target.stat().st_size != item['bytes']:
                raise SnapshotError('interrupted D install has an invalid final name')
            continue
        remaining += item['bytes']
    if installed:
        raise SnapshotError('streaming install journal includes an unknown object')
    return remaining


def stage_growth_budget(stage: Path, dataset: str, expected: str, logical: int,
                        files: int, largest_old_pack: int, *, already_committed: bool) -> int:
    """Reserve only future E growth; sealed checkpoint packs already occupy E."""
    if already_committed:
        return 1024**3
    if (stage/'prepared.json').is_file():
        stream_repack._read_prepared(stage, dataset, expected)
        return 1024**3
    completed_bytes = completed_files = 0
    if (stage/'checkpoint.sqlite').is_file():
        checkpoint = stream_repack._Checkpoint(stage, dataset, expected)
        try:
            completed_bytes = sum(item['logical_bytes'] for item in checkpoint.packages)
            completed_files = sum(item['file_count'] for item in checkpoint.packages)
        finally:
            checkpoint.close()
        if completed_bytes > logical or completed_files > files:
            raise SnapshotError('streaming checkpoint exceeds fixed source totals')
    return (logical-completed_bytes) + largest_old_pack + (files-completed_files)*1024 + 1024**3


def main() -> int:
    started = time.monotonic()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--expected-manifest-sha256', required=True)
    parser.add_argument('--progress-file', type=Path)
    parser.add_argument('--private-source', action='store_true',
                        help='repack an explicitly enrolled local-only private D dataset')
    args = parser.parse_args()
    dataset = packed.validate_slug(args.dataset, 'dataset')
    expected = packed._validate_hash(args.expected_manifest_sha256, 'original manifest SHA')
    d_reserve = install_reserve_bytes(dataset, expected, private=args.private_source)
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    workspace = Path(config['mount_point'])
    _check_d_primary_mount(COLD)
    cold = private_history_root(COLD, version_policy(ROOT), initialize=False) if args.private_source else COLD
    if args.private_source and dataset not in stream_repack.PRIVATE_REPACK_DATASETS:
        raise SnapshotError('private streaming dataset is not enrolled')
    root = workspace_volume.path('migration', create=True)/'stream-source-repack-v1'
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('streaming migration root is redirected')
    stage = root/dataset/expected
    resumed = stage.exists() or stage.is_symlink()
    progress_file = args.progress_file
    permitted_progress = {root/'queue-progress.json', root/('prepared-install-'+dataset+'.progress.json')}
    if progress_file is not None and progress_file.absolute() not in permitted_progress:
        raise SnapshotError('streaming progress must use its enrolled E location')

    priority_guard = TradingPriorityGuard()
    def guard():
        priority_guard.check()
        workspace_volume.check(c=config)
        _check_d_primary_mount(COLD)
        if args.private_source and private_history_root(COLD, version_policy(ROOT), initialize=False) != cold:
            raise SnapshotError('private streaming D authority changed')

    latest = packed.resolve_latest_packed(cold, dataset, require_objects=False)
    node_id = private_publisher(cold, dataset, latest) if args.private_source else 'penguin'
    old_sha = latest.manifest_sha256
    if old_sha != expected:
        # The stream owner itself reconciles its exact post-head crash; other
        # head changes must not start another bulk read or overwrite history.
        journal = stage/'install.json'
        if (not journal.is_file() or journal.is_symlink()
                or json.loads(journal.read_text()).get('new_manifest_sha256') != old_sha):
            raise SnapshotError('streaming repack fixed head changed')
    manifest = latest.manifest
    logical = int(manifest['source']['logical_bytes'])
    files = int(manifest['source']['files'])
    if old_sha == expected:
        held = source_repack_bloat.auto_holds(workspace)
        if (dataset, expected) in held:
            source_repack_bloat.ensure_stream_intent(workspace, dataset, expected)
            raise SnapshotError('duplicate source content queued for E dedup candidate')
        if not resumed:
            try:
                # The existing dataset publisher lock prevents a second local
                # stream owner from creating this stage during the census.
                with packed._try_publication_lock(cold/'.local-state/locks'/f'publish-{dataset}.lock'):
                    result = source_repack_bloat.preflight(workspace, cold, dataset, expected, manifest,
                        guard=guard, admit=lambda count: workspace_volume.admit(count, c=config),
                        private=args.private_source)
            except packed.PackedPublicationBusy:
                raise
            except (OSError, ValueError, TypeError, KeyError, sqlite3.Error, SnapshotError) as error:
                # An already committed hold must never fall through to a
                # normal write after an interrupted intent creation.
                if (dataset, expected) in source_repack_bloat.auto_holds(workspace):
                    raise SnapshotError('duplicate source content queued for E dedup candidate') from error
                print('dedup metadata preflight deferred: '+type(error).__name__, file=sys.stderr)
                result = None
            if result is not None:
                raise SnapshotError('duplicate source content queued for E dedup candidate')
    largest_old_pack = max((int(item['bytes']) for item in manifest['archive']['objects']
                            if item['kind'] == 'pack'), default=0)
    # One new E archive set and at most one native read scratch; metadata and
    # ZIP descriptors get bounded headroom. The canonical writer rechecks D.
    budget = stage_growth_budget(stage, dataset, expected, logical, files, largest_old_pack,
                                 already_committed=old_sha != expected)
    controls = workspace/'migration/provider-state-temporary-20261103/capacity-leases'
    last_report = 0.0
    def report(event):
        nonlocal last_report
        priority_guard.check()
        if progress_file is None:
            return
        if (time.monotonic() - last_report < 3 and event.get('stage') in
                {'streaming_source_into_E_packs', 'loading_source_inventory', 'validating_source_inventory',
                 'indexing_source_inventory', 'writing_E_inventory', 'validating_D_inventory'}):
            return
        last_report = time.monotonic()
        atomic_write_json(progress_file, {'contract': stream_repack.CONTRACT+'.progress',
            'dataset': dataset, 'expected_manifest_sha256': expected, 'state': 'running',
            'files_total': files, 'logical_bytes_total': logical, 'observed_at_epoch': time.time(),
            'progress': event})
    turn_root = workspace/'migration/source-local-exact-repack'
    turn_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    install_turn = ExitStack()
    def admit_install(prepared, journal):
        if (not args.private_source and defer_foreign_head_install(
                cold, stage, dataset, expected, node_id, prepared, journal)):
            report({'stage': 'waiting_D_foreign_head', 'files_completed': prepared['files'],
                    'logical_bytes_processed': prepared['logical_bytes']})
            raise SnapshotError('another publisher current head prevents old-source retirement; E prepared')
        relief = staged_head_capacity_relief.load(stage, dataset, expected, prepared)
        if relief is None or not (relief.get('removed_count', 0) > 0 or relief.get('pending')):
            # The original global and physical-write locks remain the actual
            # authority. A pressured retirement owner can request a brief turn;
            # E-only recovery must never wait behind this scheduling hint.
            install_turn.enter_context(retirement_turn.turn(turn_root, retirement=False))
        remaining = remaining_install_bytes(cold, prepared, journal)
        available = shutil.disk_usage(COLD).free
        if available < remaining + d_reserve:
            report({'stage': 'waiting_D_capacity', 'encoded_bytes_remaining': remaining,
                    'D_free_bytes': available, 'D_reserve_bytes': d_reserve})
            raise SnapshotError('D needs space for this dataset plus its physical write reserve '
                                f'(E prepared; remaining={remaining}; free={available})')
    def admit_next(item):
        available = shutil.disk_usage(COLD).free
        if available < item['bytes'] + d_reserve:
            report({'stage': 'waiting_D_capacity', 'encoded_bytes_remaining': item['bytes'],
                    'D_free_bytes': available, 'D_reserve_bytes': d_reserve})
            raise SnapshotError('D reserve reached during install; completed E packs and D journal retained')
    with install_turn, reserve_capacity(controls, key=expected, maximum_growth_bytes=budget,
            capacity_bytes=2**63-1, minimum_free_bytes=config['minimum_free_bytes']) as lease:
        workspace_volume.admit(budget+lease['other_reserved_growth_bytes'], c=config)
        report({'stage': 'streaming_source_into_E_packs', 'files_completed': 0, 'logical_bytes_processed': 0})
        native_reserve = ({'native_write_reserve_bytes': d_reserve}
                          if d_reserve != D_INSTALL_RESERVE else {})
        def check_retired_lab203_before_commit(prepared):
            require_retired_lab203_before_commit(cold, stage, dataset, expected, node_id, prepared)
        commit_guard = ({'before_head_commit': check_retired_lab203_before_commit}
                        if not args.private_source and dataset == _RETIRED_LAB203_DATASET
                        and expected == _RETIRED_LAB203_MANIFEST else {})
        result = stream_repack.repack(cold, stage, dataset, expected, node_id=node_id,
                                      native=True, guard=guard, progress=report, admit_install=admit_install,
                                      admit_next=admit_next,
                                      install_lock=workspace/'migration/cold-install-write-owner.lock',
                                      materialized_pin_root=(Path('/srv/stockagent-packed-materialized')
                                          if COLD == Path('/srv/stockagent-packed') else None),
                                      **native_reserve, **commit_guard)
    receipt_root = workspace/'migration/source-local-exact-repack'/dataset
    receipt_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
        'state': 'new_D_head_committed_from_exact_E_source', 'dataset': dataset,
        'old_manifest_sha256': expected, 'new_snapshot_id': result['snapshot_id'],
        'new_manifest_sha256': result['new_manifest_sha256'], 'files': result['files'],
        'logical_bytes': result['logical_bytes'], 'new_object_count': result['objects'],
        'pack_layout': 'source-dataset-v1', 'old_release_removed': False,
        'independent_D_readback': False, 'NAS_accepted': False,
        'source_mode': 'D_streamed_to_E_dataset_pack', 'private': args.private_source,
        'observed_at_epoch': time.time()}
    prepared = stream_repack._prepared_metadata(stage, dataset, expected)
    if prepared.get('packed_format') == 'content-alias-v3':
        receipt.update(packed_format='content-alias-v3', unique_content_sha_streamed=True,
                       all_original_occurrences_reread=False,
                       candidate_sha256=prepared['candidate_identity_sha256'])
    # The E copies are temporary after D publication. Keep the exact journal
    # and receipt; the queue retries cleanup if interrupted before its receipt.
    cleanup = cleanup_completed_stage(cold, stage, receipt, guard=guard)
    receipt['E_stage_cleanup'] = cleanup
    receipt['resumed_existing_stage'] = resumed
    receipt['single_run_duration_seconds'] = None if resumed else round(time.monotonic()-started, 3)
    atomic_write_json(receipt_root/(result['new_manifest_sha256']+'.json'), receipt, durable=True)
    print(json.dumps(receipt, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
