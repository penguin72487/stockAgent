#!/usr/bin/env python3
"""Repair fixed reviewed futures aliases without deleting original payload.

Expired names pointed outside the data root at vanished RAM worktrees. Preserve
their exact link bytes/inodes and a portable source provenance document; never
pretend their missing target payload has been recovered. All raw data and
existing build directories remain under their original names.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack, contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.artifact_maintenance import artifact_process_references_many
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.dataset_migration import _operation_owner
from stockagent.data_sync.desync_snapshots import (
    SnapshotError, atomic_write_bytes, atomic_write_json, rename_without_overwrite,
)
from stockagent.data_sync.packed_snapshots import PackedPublicationBusy, _try_publication_lock
from stockagent.data_sync.remote_legacy_return import active_configuration_references_many

CONTRACT = 'futures_dangling_code_alias_relocation_v1'
VERSIONS = (128, 153, 157, 165, 175)
MIGRATION_ROOT = Path('/var/lib/stockagent/lakehouse-control/dataset-versions')
COLD = Path('/srv/stockagent-packed')
LEASE_ROOT = 'artifacts/operations/futures-universe725-repair-20261025-v1'
VALIDATION_ROOT = 'data_tw_index_futures/preparation_sources/universe725_repair_20261025_v3'
EXPIRED_LEASES = {
    **{f'{VALIDATION_ROOT}/financial_source_validation_v{v}':
       (f'/dev/shm/stockagent-universe725-source-validation-v{v}', f'local-source-validation-v{v}-lease.json',
        'bounded_reconstructible_financial_source_validation_v1') for v in (100, 115)},
    **{f'{VALIDATION_ROOT}/source_rule_validation_v{v}':
       (f'/dev/shm/stockagent-universe725-rule-validation-v{v}', f'source-rule-validation-v{v}-lease.json', None)
       for v in (107, 108)},
    **{f'{VALIDATION_ROOT}/source_rule_validation_v{v}':
       (f'/dev/shm/stockagent-universe725-corporate-source-v{v}', f'corporate-source-v{v}-lease.json',
        'bounded_reconstructible_corporate_source_v1') for v in (120, 132, 138, 147, 149, 155, 167, 170)},
    **{f'{VALIDATION_ROOT}/source_amendment_validation_v{v}':
       (f'/dev/shm/stockagent-universe725-corporate-amendment-v{v}', f'corporate-amendment-v{v}-lease.json',
        'bounded_reconstructible_corporate_amendment_v1') for v in (119, 131)},
    f'{VALIDATION_ROOT}/corporate_gpu_ocr_v117':
       ('/dev/shm/stockagent-universe725-corporate-ocr-v117', 'unbound-original-ocr-v117-lease.json',
        'bounded_reconstructible_original_ocr_v1'),
}
CHECKPOINT = 'all_twd/clocks/bulk_readback_checkpoints/7fea43af255048f32e160a0f20fba03dd73d0d416268f771c7af64107b2df151'
PREPARED = 'data_tw_index_futures/training_prepared/tw_futures_universe765_20261027_v1'
PORTABLE_PATH, PORTABLE_TARGET = f'{PREPARED}/execution_all_twd_v8q/{CHECKPOINT}', f'{PREPARED}/execution_all_twd_v8/{CHECKPOINT}'


def _real_directory(path: Path) -> None:
    if path.resolve(strict=True) != path or not path.is_dir():
        raise SnapshotError('repair parent is missing or redirected')


def _make_real_directories(path: Path) -> None:
    """Never create repair state through a redirected ancestor."""
    missing = []
    cursor = path
    while not os.path.lexists(cursor):
        missing.append(cursor)
        cursor = cursor.parent
    _real_directory(cursor)
    for directory in reversed(missing):
        directory.mkdir(mode=0o700, exist_ok=True)
        _real_directory(directory)


def _signature(path: Path) -> dict:
    info = path.lstat()
    return {key: getattr(info, 'st_' + key) for key in
            ('dev', 'ino', 'mode', 'size', 'mtime_ns', 'ctime_ns', 'uid', 'gid', 'nlink')}


def _current() -> dict:
    _real_directory(MIGRATION_ROOT)
    path = MIGRATION_ROOT / 'current.json'
    if path.is_symlink():
        raise SnapshotError('migration pointer is redirected')
    current = json.loads(path.read_bytes())
    operation = Path(current['operation_directory'])
    if (operation.parent != MIGRATION_ROOT / 'operations'
            or not re.fullmatch(r'[a-f0-9-]{16,96}', operation.name)):
        raise SnapshotError('migration operation is outside its original owner')
    _real_directory(operation)
    return current


def _references(repo: Path, paths: list[Path]) -> list[str]:
    # Resolving a dangling link alone loses its logical preparation-suite
    # ancestor. Keep that suite in the check as well as the physical target.
    parents = [path.parent for path in paths if path.is_relative_to(repo / 'data_tw_index_futures/preparation_sources')]
    paths = list(dict.fromkeys([*paths, *parents]))
    references = artifact_process_references_many(paths, repo / 'data_tw_index_futures')
    for values in active_configuration_references_many(paths, repo).values():
        references.extend(values)
    return references


def _allowed(repo: Path, scope: str) -> dict[str, str]:
    if scope == 'code-builds':
        return {f'data_tw_index_futures/build_inputs/universe725_recovery_20261025_v1-v{v}':
                f'/dev/shm/stockagent-universe725-source-build-v{v}' for v in VERSIONS}
    if scope == 'expired-leases':
        return {**{name: value[0] for name, value in EXPIRED_LEASES.items()}, PORTABLE_PATH: str(repo / PORTABLE_TARGET)}
    raise SnapshotError('unreviewed source alias scope')


def _lease_evidence(repo: Path, relative: str) -> dict:
    physical, name, contract = EXPIRED_LEASES[relative]
    path = repo / LEASE_ROOT / name
    _real_directory(path.parent)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 64 * 1024:
        raise SnapshotError('expired source view needs its original bounded regular lease')
    raw = path.read_bytes()
    value = json.loads(raw)
    logical = value.get('logical_root', value.get('logical'))
    logical = Path(logical) if isinstance(logical, str) else Path('/unbound')
    if not logical.is_absolute():
        logical = repo / logical
    boot = value.get('boot_id')
    if (value.get('contract') != contract or logical != repo / relative
            or value.get('physical_root', value.get('physical')) != physical
            or not (value.get('volatile_reconstructible') is True
                    or value.get('volatile_reconstructible_candidate') is True)
            or contract is None and not (value.get('volatile_reconstructible') is True
                                          and value.get('source_originals_untouched') is True)
            or value.get('source_originals_deleted') is True
            or not isinstance(boot, str) or not re.fullmatch(r'[0-9a-f-]{36}', boot)
            or boot == Path('/proc/sys/kernel/random/boot_id').read_text().strip()):
        raise SnapshotError('source lease identity, old boot or volatile-view contract differs')
    return {'path': str(path.relative_to(repo)), 'sha256': hashlib.sha256(raw).hexdigest(),
            'original_utf8': raw.decode('utf-8')}


def _portable_binding(repo: Path) -> dict:
    target = repo / PORTABLE_TARGET
    _real_directory(target)
    manifest = target / 'manifest.json'
    if manifest.is_symlink() or not manifest.is_file():
        raise SnapshotError('portable checkpoint alias has no original manifest')
    relative = os.path.relpath(target, (repo / PORTABLE_PATH).parent)
    from stockagent.data_sync.packed_snapshots import _validate_symlink_target
    _validate_symlink_target(str(Path(PORTABLE_PATH).relative_to('data_tw_index_futures')), relative)
    return {'replacement_target': relative, 'target_signature': _signature(target),
            'target_manifest_sha256': hashlib.sha256(manifest.read_bytes()).hexdigest()}


def inventory(repo: Path, scope: str = 'code-builds') -> dict:
    repo = repo.absolute()
    parent = repo / 'data_tw_index_futures/build_inputs'
    _real_directory(parent)
    current = _current()
    rows = []
    for relative, expected in _allowed(repo, scope).items():
        path = repo / relative
        if not os.path.lexists(path):
            continue
        portable = scope == 'expired-leases' and relative == PORTABLE_PATH
        _real_directory(path.parent)
        if not path.is_symlink() or os.readlink(path) != expected or (not portable and os.path.lexists(expected)):
            raise SnapshotError('reviewed alias changed or its target is present; preserve for inspection')
        row = {'path': str(path.relative_to(repo)), 'target': expected,
                     'target_sha256': hashlib.sha256(os.fsencode(expected)).hexdigest(),
                     'signature': _signature(path)}
        if scope == 'expired-leases':
            row.update(_portable_binding(repo) if portable else {'lease_evidence': _lease_evidence(repo, relative)})
        rows.append(row)
    if not rows:
        raise SnapshotError('no reviewed dangling aliases; do not create an empty repair')
    return {'contract': CONTRACT, 'scope': scope, 'observed_at_epoch': time.time(),
            'repo': str(repo), 'operation_directory': current['operation_directory'],
            'migration_plan_sha256': current['plan_sha256'], 'aliases': rows,
            'blockers': _references(repo, [repo / row['path'] for row in rows]),
            'missing_target_payload_recovered': False, 'source_payload_deletion_authorized': False}


@contextmanager
def original_owners(plan: dict):
    current = _current()
    if (current['operation_directory'] != plan['operation_directory']
            or current['plan_sha256'] != plan['migration_plan_sha256']):
        raise SnapshotError('original migration identity changed after inventory')
    _check_d_primary_mount(COLD)
    operation = Path(current['operation_directory'])
    with _operation_owner(operation), ExitStack() as locks:
        again = _current()
        if any(again[key] != current[key] for key in ('operation_directory', 'plan_sha256')):
            raise SnapshotError('migration pointer changed while acquiring its owner')
        progress = json.loads((operation / 'progress.json').read_bytes())
        for name in ('tw-index-futures', 'local-tw-index-futures'):
            row = progress['datasets'][name]
            if row['state'] not in ('pending', 'waiting_retry') or row.get('snapshot_id'):
                raise PackedPublicationBusy('original source has a committed or unfinished transaction')
        for cold in (COLD, COLD / '.local-state/private-history'):
            _real_directory(cold)
            locks.enter_context(_try_publication_lock(
                cold / '.local-state/locks/publish-retention-global.lock', shared=True))
        for cold, name in ((COLD, 'tw-index-futures'),
                           (COLD / '.local-state/private-history', 'local-tw-index-futures')):
            locks.enter_context(_try_publication_lock(cold / '.local-state/locks' / f'publish-{name}.lock'))
        yield


def _sync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _matches(path: Path, row: dict, *, renamed: bool) -> bool:
    current = _signature(path)
    expected = row['signature']
    fields = set(expected) - ({'ctime_ns'} if renamed else set())
    return (stat.S_ISLNK(current['mode']) and all(current[key] == expected[key] for key in fields)
            and os.readlink(path) == row['target'])


def apply(repo: Path, raw_inventory: bytes, expected_sha256: str, receipt: Path) -> dict:
    digest = hashlib.sha256(raw_inventory).hexdigest()
    if digest != expected_sha256:
        raise SnapshotError('repair requires the exact reviewed inventory SHA-256')
    plan = json.loads(raw_inventory)
    repo = repo.absolute()
    scope = plan.get('scope', 'code-builds')
    allowed = _allowed(repo, scope)
    if (plan.get('contract') != CONTRACT or plan.get('repo') != str(repo)
            or plan.get('blockers') or not isinstance(plan.get('aliases'), list)
            or not 1 <= len(plan['aliases']) <= len(allowed)):
        raise SnapshotError('invalid or blocked source-link repair inventory')
    parent = repo / 'data_tw_index_futures/build_inputs'
    _real_directory(parent)
    if len({r.get('path') for r in plan['aliases']}) != len(plan['aliases']) or any(
            allowed.get(row.get('path')) != row.get('target') for row in plan['aliases']):
        raise SnapshotError('repair inventory contains an unreviewed alias')
    signature_fields = {'dev', 'ino', 'mode', 'size', 'mtime_ns', 'ctime_ns', 'uid', 'gid', 'nlink'}
    if any(not isinstance(row.get('signature'), dict) or set(row['signature']) != signature_fields
           or any(type(value) is not int for value in row['signature'].values())
           or row.get('target_sha256') != hashlib.sha256(os.fsencode(row['target'])).hexdigest()
           for row in plan['aliases']):
        raise SnapshotError('repair inventory lacks complete original link metadata')
    for row in plan['aliases']:
        _real_directory((repo / row['path']).parent)
        portable = scope == 'expired-leases' and row['path'] == PORTABLE_PATH
        if portable:
            if any(row.get(key) != value for key, value in _portable_binding(repo).items()):
                raise SnapshotError('portable alias target differs from its original fixed checkpoint')
        elif row.get('replacement_target') is not None:
            raise SnapshotError('unreviewed portable replacement')
        elif scope == 'expired-leases' and row.get('lease_evidence') != _lease_evidence(repo, row['path']):
            raise SnapshotError('original volatile source lease changed since inventory')
    receipt = receipt.absolute()
    if not receipt.is_relative_to(repo / 'artifacts/operations'):
        raise SnapshotError('repair receipts must remain outside data under original operations')
    _make_real_directories(receipt)
    quarantine = repo / 'service_data/futures-source-link-repair' / digest / 'quarantine'
    _make_real_directories(quarantine)
    if quarantine.stat().st_dev != parent.stat().st_dev:
        raise SnapshotError('source alias relocation must preserve its inode on one filesystem')
    provenance = repo / 'data_tw_index_futures/state/source_link_repairs' / (digest + '.json')
    document = {'contract': CONTRACT, 'inventory_sha256': digest, 'aliases': plan['aliases'],
        'role': 'retired_ephemeral_code_build_aliases', 'missing_target_payload': 'absent_not_reconstructed',
        'source_payload_files_changed': 0, 'original_link_names_preserved_in_private_quarantine': True}
    if scope == 'expired-leases':
        document.update(role='expired_volatile_validation_views_and_portable_checkpoint_alias',
                        expired_view_payload_recovered=False, historical_validation_outputs_verified=False)
    provenance_bytes = (json.dumps(document, sort_keys=True, indent=2) + '\n').encode()
    with original_owners(plan):
        references = _references(repo, [path for row in plan['aliases'] for path in
            (repo / row['path'], quarantine / Path(row['path']).name)])
        if references:
            raise SnapshotError('reviewed aliases have current process/configuration/recovery references')
        # Reconcile the complete set before the first rename, including a
        # previous interrupted transaction. Never overwrite either name.
        for row in plan['aliases']:
            source, moved = repo / row['path'], quarantine / Path(row['path']).name
            source_present, moved_present = os.path.lexists(source), os.path.lexists(moved)
            replacement = row.get('replacement_target')
            replaced = (replacement is not None and moved_present and source.is_symlink()
                        and os.readlink(source) == replacement and source.resolve() == Path(row['target']))
            resumable_gap = replacement is not None and moved_present and not source_present
            if ((source_present == moved_present and not replaced)
                    or (not replacement and os.path.lexists(row['target']))
                    or not _matches(moved if moved_present else source, row, renamed=moved_present)
                    or replacement is not None and not (replaced or resumable_gap or source_present and not moved_present)):
                raise SnapshotError('alias identity/target changed; preserve both names and the journal')
        _make_real_directories(provenance.parent)
        if os.path.lexists(provenance):
            if provenance.is_symlink() or provenance.read_bytes() != provenance_bytes:
                raise SnapshotError('source provenance differs from this fixed repair')
        else:
            atomic_write_bytes(provenance, provenance_bytes, mode=0o600)
        # Persist the new control directory names before retiring any old
        # alias name. The actual rename retains its original link inode.
        for directory in (provenance.parent.parent, *quarantine.parents):
            if directory == repo:
                break
            _sync_directory(directory)
        result = {'contract': CONTRACT, 'inventory_sha256': digest, 'state': 'moving',
                  'moved': [], 'deleted_payload_files': 0, 'reclaimed_bytes': 0,
                  'source_provenance': str(provenance), 'missing_target_payload_recovered': False}
        atomic_write_json(receipt / 'intent.json', {**result, 'plan': plan})
        for row in plan['aliases']:
            source, moved = repo / row['path'], quarantine / Path(row['path']).name
            replacement = row.get('replacement_target')
            if not os.path.lexists(moved):
                if ((not replacement and os.path.lexists(row['target']))
                        or not _matches(source, row, renamed=False)):
                    raise SnapshotError('alias changed immediately before relocation')
                rename_without_overwrite(source, moved)
                _sync_directory(source.parent)
                _sync_directory(quarantine)
            if replacement is not None and not os.path.lexists(source):
                source.symlink_to(replacement, target_is_directory=True)
                _sync_directory(source.parent)
            source_valid = (source.is_symlink() and os.readlink(source) == replacement
                            and source.resolve() == Path(row['target'])) if replacement else not os.path.lexists(source)
            if not source_valid or not _matches(moved, row, renamed=True):
                raise SnapshotError('relocated alias failed exact metadata/link readback')
            result['moved'].append({'path': row['path'], 'quarantine': str(moved),
                                    'signature': _signature(moved), 'target': os.readlink(moved)})
            if replacement is not None:
                if any(row.get(key) != value for key, value in _portable_binding(repo).items()):
                    raise SnapshotError('portable target changed during the alias repair')
                result['moved'][-1]['replacement_target'] = replacement
            atomic_write_json(receipt / 'journal.json', result)
        if provenance.read_bytes() != provenance_bytes:
            raise SnapshotError('source provenance changed before completing relocation')
        result.update(state='preserved_and_relocated', observed_at_epoch=time.time(),
                      cold_reconstruction_verified=False, source_retirement=False)
        atomic_write_json(receipt / 'result.json', result)
        return result


def main() -> int:
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--expected-inventory-sha256')
    parser.add_argument('--receipt-directory', type=Path)
    parser.add_argument('--scope', choices=('code-builds', 'expired-leases'), default='code-builds')
    parser.add_argument('--wait-seconds', type=float, default=0,
                        help='Bounded wait for the unchanged original owner, without holding its locks')
    args = parser.parse_args()
    if not 0 <= args.wait_seconds <= 300:
        parser.error('owner wait must be between 0 and 300 seconds')
    if not args.apply:
        if args.inventory.exists() or args.inventory.is_symlink():
            parser.error('preserve the original inventory; use a new path')
        result = inventory(ROOT, args.scope)
        atomic_write_json(args.inventory, result)
        print(json.dumps({'aliases': len(result['aliases']), 'blockers': len(result['blockers']),
                         'inventory_sha256': hashlib.sha256(args.inventory.read_bytes()).hexdigest()}))
        return 0
    if not args.expected_inventory_sha256 or not args.receipt_directory or args.inventory.is_symlink():
        parser.error('apply requires a regular fixed inventory, its SHA-256 and original receipt directory')
    raw = args.inventory.read_bytes()
    deadline = time.monotonic() + args.wait_seconds
    while True:
        try:
            result = apply(ROOT, raw, args.expected_inventory_sha256, args.receipt_directory)
            break
        except PackedPublicationBusy:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise
            time.sleep(min(1, remaining))
    print(json.dumps({key: result[key] for key in ('state', 'deleted_payload_files', 'reclaimed_bytes')}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
