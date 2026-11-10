#!/usr/bin/env python3
"""Archive the explicitly reviewed offline replay pairs to D, then retire C names."""
from __future__ import annotations

from contextlib import ExitStack
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.run_bounded_storage_maintenance import TradingPriorityGuard
from scripts.run_stream_packed_repack import COLD, D_INSTALL_RESERVE, remaining_install_bytes, stage_growth_budget
from scripts.retire_local_offline_artifacts import COMMON_OWNER
from stockagent.data_sync import (handoff_index_archive, index_archive_publish, offline_replay_pairs,
    packed_snapshots as packed, stream_repack, workspace_relocation, workspace_source_handoff, workspace_volume)
from stockagent.data_sync.artifact_consumers import artifact_service_references
from stockagent.data_sync.artifact_maintenance import artifact_process_references_many
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.completed_forward_capacity import reserve_capacity
from stockagent.data_sync.dataset_migration import private_history_root
from stockagent.data_sync.dataset_version_layout import version_policy
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.remote_legacy_return import active_configuration_references_many
from stockagent.data_sync.offline_replay_policy import CONTRACT, ENROLLED, completed

SOURCE_ROOT = ROOT/'service_data/live'
MIRROR_ROOT = Path('/srv/stockagent-artifacts-hot/live')


def _owner(stack: ExitStack, path: Path):
    descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
    stream = stack.enter_context(os.fdopen(descriptor, 'r+b'))
    info = os.fstat(stream.fileno())
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_nlink != 1:
        raise SnapshotError('canonical artifact mutation owner differs')
    fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _consumer_guard(source: Path, mirror: Path) -> None:
    paths = (source, mirror)
    if artifact_process_references_many(paths, SOURCE_ROOT):
        raise BlockingIOError('offline replay has an active process consumer')
    if any(active_configuration_references_many(paths, ROOT).values()):
        raise BlockingIOError('offline replay has an active research or recovery reference')
    if any(artifact_service_references(paths, ROOT).values()):
        raise BlockingIOError('offline replay has an active service reference')


def _root_identity(path: Path) -> list[int]:
    info = path.lstat()
    if path.is_symlink() or not stat.S_ISDIR(info.st_mode) or path.resolve(strict=True) != path:
        raise SnapshotError('enrolled offline replay root is unavailable')
    return [info.st_dev, info.st_ino]


def _load(path: Path) -> dict:
    if path.is_symlink():
        raise SnapshotError('offline replay status is redirected')
    if not path.exists():
        return {}
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict) or value.get('contract') != CONTRACT:
        raise SnapshotError('offline replay status contract differs')
    return value


def _fixed_publication(private: Path, stage: Path, dataset: str, identity: str,
                       prepared: dict, expected_manifest: str) -> None:
    """Bind C retirement to the committed head and the installed D generations."""
    journal_path = stage/'install.json'
    if journal_path.is_symlink() or not journal_path.is_file():
        raise SnapshotError('offline replay D installation journal is missing')
    journal = json.loads(journal_path.read_bytes())
    current = packed.resolve_latest_packed(private, dataset)
    if (journal.get('contract') != stream_repack.CONTRACT+'.install' or journal.get('state') != 'head_pending'
            or journal.get('dataset') != dataset or journal.get('old_manifest_sha256') != identity
            or journal.get('prepared_sha256') != prepared['identity_sha256']
            or journal.get('new_manifest_sha256') != expected_manifest
            or current.manifest_sha256 != expected_manifest
            or current.manifest['metadata'].get('migration_source_identity_sha256') != identity
            or journal.get('snapshot_id') != current.manifest['snapshot_id']):
        raise SnapshotError('offline replay D archive head or journal changed')
    payloads = [*prepared['packages'], prepared['inventory']]
    expected = {item['sha256'] for item in payloads}
    installed_rows = journal.get('installed')
    if not isinstance(installed_rows, list):
        raise SnapshotError('offline replay D installed objects are missing')
    installed = {item.get('sha256'): item for item in installed_rows if isinstance(item, dict)}
    refs = {item['sha256']: item['relpath'] for item in
            [*current.manifest['archive']['objects'], current.manifest['archive']['inventory']]}
    if len(installed) != len(installed_rows) or set(installed) != expected or set(refs) != expected:
        raise SnapshotError('offline replay D installed object set differs')
    for digest, row in installed.items():
        target = packed._path_under(private, refs[digest], 'offline replay D object')
        if target.is_symlink() or not target.is_file() or not stream_repack._signature_matches(target, row.get('D_signature')):
            raise SnapshotError('offline replay D installed object generation changed')


def migrate(dataset: str, workspace: Path, private: Path, config: dict) -> dict:
    if dataset not in ENROLLED:
        raise SnapshotError('offline replay pair is not enrolled')
    source, mirror = SOURCE_ROOT/ENROLLED[dataset], MIRROR_ROOT/ENROLLED[dataset]
    root = workspace/'migration/offline-replay-pairs-v1'/dataset
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('offline replay stage root redirected')
    status_path = root/'status.json'
    status = _load(status_path)
    if status and (status.get('dataset') != dataset or status.get('source') != str(source)
                   or status.get('mirror') != str(mirror)):
        raise SnapshotError('offline replay enrollment changed')
    if status.get('state') == 'completed':
        if not completed(status, dataset):
            raise SnapshotError('offline replay completed receipt is incomplete')
        return status
    base = {'contract': CONTRACT, 'dataset': dataset, 'source': str(source), 'mirror': str(mirror),
            'independent_D_readback': False, 'NAS_accepted': False}
    status = {**base, **status}
    last = 0.0

    def save(**items):
        if items.get('state') not in (None, 'waiting_retry'):
            status.pop('error', None)
            status.pop('error_type', None)
        status.update(items, observed_at_epoch=time.time())
        atomic_write_json(status_path, status, durable=True)

    def progress(event):
        nonlocal last
        now = time.monotonic()
        if now-last < 2 and event.get('stage') in {'hashing_source', 'streaming_source_into_E_packs',
                                                   'streaming_D_object', 'retiring_C_pair'}:
            return
        last = now
        save(state='running', progress=event)

    priority = TradingPriorityGuard()
    guard_after = 0.0
    def guard():
        nonlocal guard_after
        priority.check()
        now = time.monotonic()
        if now < guard_after:
            return
        workspace_volume.check(c=config)
        _check_d_primary_mount(COLD)
        if private_history_root(COLD, version_policy(ROOT), initialize=False) != private:
            raise SnapshotError('offline replay private D authority changed')
        guard_after = now+5

    guard()
    if 'source_root' in status or 'mirror_root' in status:
        offline_replay_pairs.root_rebind(source, mirror, status.get('source_root'), status.get('mirror_root'))
    else:
        _root_identity(source)
        _root_identity(mirror)
    with ExitStack() as stack:
        lock = stack.enter_context(workspace_source_handoff._lock_supplemental_file(source/'.download.lock'))
        lock_inode = os.fstat(lock.fileno()).st_ino
        if status.get('source_lock_inode', lock_inode) != lock_inode:
            raise SnapshotError('offline replay original lock changed')
        if 'source_root' not in status:
            _consumer_guard(source, mirror)
            summary = offline_replay_pairs.check_pair(source, mirror)
            save(state='indexing', source_root=_root_identity(source), mirror_root=_root_identity(mirror),
                 source_lock_inode=lock_inode, **summary)
        evidence, destination = root/'source-index', root/'empty-destination'
        if not status.get('source_identity'):
            _consumer_guard(source, mirror)
            offline_replay_pairs.check_pair(source, mirror)
            index = offline_replay_pairs.hash_index(source, destination, evidence, guard=guard, progress=progress)
            offline_replay_pairs.check_pair(source, mirror)
            identity_body = {'contract': CONTRACT+'.source', 'source': str(source), 'mirror': str(mirror),
                'source_root': status['source_root'], 'mirror_root': status['mirror_root'],
                'index_sha256': workspace_relocation.digest(index, no_follow=True)}
            identity = hashlib.sha256(json.dumps(identity_body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            save(state='indexed', source_identity=identity, index_sha256=identity_body['index_sha256'])
        else:
            index, identity = evidence/'files.sqlite', status['source_identity']
            packed._validate_hash(identity, 'offline replay fixed source identity')
            if workspace_relocation.digest(index, no_follow=True) != status.get('index_sha256'):
                raise SnapshotError('offline replay fixed index changed')
        stage = root/'archive'/identity
        if not status.get('D_archive_committed'):
            _consumer_guard(source, mirror)
            offline_replay_pairs.check_pair(source, mirror)
            budget = stage_growth_budget(stage, dataset, identity, status['logical_bytes'],
                                         status['files']+1, 0, already_committed=False)
            controls = workspace/'migration/provider-state-temporary-20261103/capacity-leases'
            controls.mkdir(mode=0o700, parents=True, exist_ok=True)
            if controls.is_symlink() or controls.resolve(strict=True) != controls:
                raise SnapshotError('offline replay capacity control is redirected')
            with reserve_capacity(controls, key=identity, maximum_growth_bytes=budget,
                                  capacity_bytes=2**63-1, minimum_free_bytes=config['minimum_free_bytes']) as lease:
                workspace_volume.admit(budget+lease['other_reserved_growth_bytes'], c=config)
                prepared = handoff_index_archive.prepare(source, stage, dataset, identity, index,
                    original_inode=status['source_root'][1], original_lock_inode=lock_inode,
                    preserved_paths=['.download.lock'], guard=guard, progress=progress,
                    large_member_deflate_workers=4)
            offline_replay_pairs.check_pair(source, mirror)
            _consumer_guard(source, mirror)
            save(state='E_archive_sealed', prepared_sha256=prepared['identity_sha256'],
                 encoded_bytes=sum(item['bytes'] for item in prepared['packages']))
            def admit_install(record, journal):
                if shutil.disk_usage(COLD).free < remaining_install_bytes(private, record, journal)+D_INSTALL_RESERVE:
                    raise SnapshotError('D free space is below offline replay install and reserve')
            def admit_next(item):
                if shutil.disk_usage(COLD).free < item['bytes']+D_INSTALL_RESERVE:
                    raise SnapshotError('D reserve reached during offline replay install')
            result = index_archive_publish.publish(private, stage, dataset, identity, index, source.name,
                guard=guard, progress=progress, admit_install=admit_install, admit_next=admit_next,
                install_lock=workspace/'migration/cold-install-write-owner.lock')
            save(state='D_archive_committed', D_archive_committed=True,
                 D_manifest_sha256=result['new_manifest_sha256'], D_snapshot_id=result['snapshot_id'])
        prepared = (stream_repack._prepared_metadata(stage, dataset, identity)
                    if status.get('C_payload_retired') is True
                    else stream_repack._read_prepared(stage, dataset, identity))
        if prepared['identity_sha256'] != status['prepared_sha256']:
            raise SnapshotError('offline replay sealed E preparation differs')
        binding = {'source_root': status['source_root'], 'mirror_root': status['mirror_root'],
                   'prepared_sha256': prepared['identity_sha256'], 'manifest_sha256': status['D_manifest_sha256']}
        if status.get('C_payload_retired') is not True:
            with ExitStack() as retire_stack:
                _owner(retire_stack, COMMON_OWNER)
                retire_stack.enter_context(packed._try_publication_lock(private/'.local-state/locks/publish-retention-global.lock', shared=True))
                retire_stack.enter_context(packed._try_publication_lock(private/'.local-state/locks'/f'publish-{dataset}.lock'))
                def retirement_guard():
                    guard()
                    _fixed_publication(private, stage, dataset, identity, prepared, binding['manifest_sha256'])
                result = offline_replay_pairs.retire_pair(source, mirror, index, stage/'paired-retirement.sqlite', binding,
                    guard=retirement_guard, consumer_guard=lambda: _consumer_guard(source, mirror), progress=progress)
            save(state='C_payload_retired', C_payload_retired=True, C_retirement=result)
        else:
            result = status.get('C_retirement')
            if not isinstance(result, dict) or result.get('state') != 'paired_payload_retired':
                raise SnapshotError('offline replay C retirement receipt differs')
        receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
            'state': 'new_D_head_committed_from_exact_E_source', 'dataset': dataset,
            'old_manifest_sha256': identity, 'new_manifest_sha256': status['D_manifest_sha256'],
            'new_snapshot_id': status['D_snapshot_id']}
        cleanup = stream_repack.cleanup_stage(private, stage, receipt, guard=guard)
        save(state='completed', C_retirement=result, E_archive_cleanup=cleanup)
    return status


def main() -> int:
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    workspace = Path(config['mount_point'])
    queue = workspace/'migration/offline-replay-pairs-v1'
    queue.mkdir(mode=0o700, parents=True, exist_ok=True)
    if queue.is_symlink() or queue.resolve(strict=True) != queue:
        raise SnapshotError('offline replay queue is redirected')
    descriptor = os.open(queue/'owner.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        pending = [dataset for dataset in ENROLLED
                   if not completed(_load(queue/dataset/'status.json'), dataset)]
        if not pending:
            return 0
        _check_d_primary_mount(COLD)
        private = private_history_root(COLD, version_policy(ROOT), initialize=False)
        failures = 0
        for dataset in pending:
            try:
                result = migrate(dataset, workspace, private, config)
                print(json.dumps({'dataset': dataset, 'state': result['state']}, ensure_ascii=False), flush=True)
            except (OSError, ValueError, SnapshotError) as error:
                path = queue/dataset/'status.json'
                value = _load(path)
                if not value:
                    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                    value = {'contract': CONTRACT, 'dataset': dataset,
                             'source': str(SOURCE_ROOT/ENROLLED[dataset]),
                             'mirror': str(MIRROR_ROOT/ENROLLED[dataset]),
                             'independent_D_readback': False, 'NAS_accepted': False}
                value.update(state='waiting_retry', error_type=type(error).__name__, error=str(error)[:1000],
                             observed_at_epoch=time.time())
                atomic_write_json(path, value, durable=True)
                print(f'{dataset}: {type(error).__name__}: {error}', file=sys.stderr, flush=True)
                failures += 1
        return 1 if failures else 0
    finally:
        os.close(descriptor)


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
