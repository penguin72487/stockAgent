"""Temporary, explicitly enrolled provider placement on the existing E volume.

This preserves canonical collector paths and owners. It does not authorize cold
retirement, hide an unavailable volume with a C fallback, or change source data.
The small node-local registry survives a restart; payload and copy proofs use E.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from stockagent.data_sync import workspace_volume as volume
from stockagent.data_sync.desync_snapshots import SnapshotError

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = volume.ENROLLMENT.parent / 'provider-placement.json'
CONTRACT = 'temporary_provider_e_workspace_v1'
PROVIDERS = ('finlab', 'finmind', 'tej')
GENERATION = 'provider-state-temporary-20261103'
MIGRATION_FENCE = Path('/run/stockagent-provider-migration.active')


def load():
    # A reboot can leave the enrolled service-control tree at its recorded
    # destination while the legacy bind mount is absent.  An empty new legacy
    # directory is not proof that this host was never enrolled: treating it as
    # such would let a provider write to the C placeholder instead of its E
    # namespace.  Keep the original registry and owner untouched until the
    # control roots have been reconciled by their placement owner.
    if not REGISTRY.exists():
        retained = ROOT / 'service_data/stockagent/workspace-volume/provider-placement.json'
        if REGISTRY.is_symlink() or retained.exists() or retained.is_symlink():
            raise SnapshotError('provider placement registry unavailable at the canonical control root')
        return {'contract': CONTRACT, 'providers': {}}
    info = REGISTRY.lstat()
    if REGISTRY.is_symlink() or info.st_uid != os.geteuid() or info.st_size > 1024**2:
        raise SnapshotError('provider placement registry identity differs')
    value = json.loads(REGISTRY.read_bytes())
    if (value.get('contract') != CONTRACT or not isinstance(value.get('providers'), dict)
            or set(value['providers']) - set(PROVIDERS)):
        raise SnapshotError('provider placement registry contract differs')
    c = volume.configuration()
    for name, entry in value['providers'].items():
        if (entry.get('alias') != str(ROOT / ('data_' + name))
                or entry.get('destination') != str(Path(c['mount_point']) / 'downloads' / GENERATION / name / 'data')
                or entry.get('backup') != str(ROOT / ('.data_' + name + '.pre-e-20261103'))
                or entry.get('evidence') != str(Path(c['mount_point']) / 'migration' / GENERATION / name)
                or entry.get('filesystem_uuid') != json.loads(volume.ENROLLMENT.read_bytes())['filesystem_uuid']):
            raise SnapshotError('provider placement paths or E UUID differ')
        for child in entry.get('external_subtrees', []):
            relative = Path(child['relative'])
            if relative.is_absolute() or '..' in relative.parts or str(relative) != child['relative']:
                raise SnapshotError('provider external subtree leaves its enrolled root')
            expected = (Path(c['mount_point']) / volume.FINLAB_FUTURES_ROLE
                        if name == 'finlab' and child['relative'] == 'intraday/objects/futures-workspace-v1'
                        and child['readonly'] is False else Path(entry['backup']) / relative)
            if (child['source'] != str(expected) or type(child['readonly']) is not bool
                    or (not child['readonly'] and name != 'finlab')):
                raise SnapshotError('provider external subtree identity differs')
    return value


def exact_mount(target, source):
    """Recognize bind mounts on the SAME device, where Path.is_mount is false."""
    try:
        return (target.resolve(strict=True) == target and source.resolve(strict=True) == source
                and target.samefile(source) and volume._mount_info(target)['target'] == str(target))
    except (OSError, SnapshotError):
        return False


def check_entry(name, entry, *, allow_cutover=False):
    if entry.get('state') not in ({'active', 'cutover'} if allow_cutover else {'active'}):
        raise SnapshotError('provider workspace migration has not completed')
    volume.check()
    alias, destination = Path(entry['alias']), Path(entry['destination'])
    if not exact_mount(alias, destination):
        raise SnapshotError('provider E alias is missing or differs; C fallback refused')
    proof = Path(entry['evidence']) / 'copy-result.json'
    from stockagent.data_sync.workspace_relocation import digest
    if digest(proof) != entry.get('copy_result_sha256'):
        raise SnapshotError('provider full-copy proof changed')
    for child in entry.get('external_subtrees', []):
        target = alias / child['relative']
        if not exact_mount(target, Path(child['source'])):
            raise SnapshotError('provider enrolled child alias is missing or differs')
        if child['readonly'] and not (os.statvfs(target).f_flag & os.ST_RDONLY):
            raise SnapshotError('unused provider subtree must remain read-only')
    return {'provider': name, 'state': 'active', 'physical_volume': 'E:',
            'alias': str(alias), 'destination': str(destination)}


def check_path(path):
    path = path.absolute()
    # Ordinary non-provider and unenrolled hosts keep the existing contract.
    names = [name for name in PROVIDERS if path.is_relative_to(ROOT / ('data_' + name))]
    if not names:
        return None
    value = load()
    if value['providers'] and MIGRATION_FENCE.exists():
        raise SnapshotError('provider owners wait for the complete E migration handoff')
    if names[0] not in value['providers']:
        return None
    return check_entry(names[0], value['providers'][names[0]])
