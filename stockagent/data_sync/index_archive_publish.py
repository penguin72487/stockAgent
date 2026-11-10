"""Commit a sealed C-index archive as a local-only D source snapshot.

The private D head is independent of the public current dataset. This moves
the exact archived source generation to D; it does not merge conflicting rows
or assert that either generation contains the other.
"""
from __future__ import annotations

from contextlib import ExitStack, closing
import hashlib
import json
import os
from pathlib import Path
import time

from stockagent.data_sync import handoff_source_repack as handoff, packed_snapshots as p
from stockagent.data_sync import stream_repack as s, workspace_relocation as relocation
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

CONTRACT = 'stockagent.index_source_private_D_publication.v1'


class ExistingPrivateHead(SnapshotError):
    """An existing private head needs the original owner's comparison."""


def source_summary(index: Path, prepared: dict, root_name: str) -> dict:
    """Reconstruct the canonical path census without reading source payloads."""
    if (not isinstance(root_name, str) or not root_name or '/' in root_name or root_name in ('.', '..')
            or not s._generation_matches(index, prepared.get('source_index_signature'), relocation.signature(index))):
        raise SnapshotError('source index generation or root name differs')
    children = {'.': {'directory': [], 'file': []}}
    files = logical = directories = 0
    with closing(handoff._index_connection(index)) as db:
        for name, kind, size in db.execute('SELECT path,kind,bytes FROM files ORDER BY path'):
            relative = p._safe_relative_path(name, 'source index path')
            if relative.as_posix() != name or kind not in {'directory', 'file'}:
                raise SnapshotError('source index namespace differs')
            parent = relative.parent.as_posix()
            children.setdefault(parent, {'directory': [], 'file': []})[kind].append((name, size))
            if kind == 'directory':
                children.setdefault(name, {'directory': [], 'file': []})
                directories += 1
            else:
                if type(size) is not int or size < 0:
                    raise SnapshotError('source index file size differs')
                files += 1
                logical += size
    if files != prepared['files'] or logical != prepared['logical_bytes']:
        raise SnapshotError('source index counts differ from the sealed E archive')
    digest, visited, stack = hashlib.sha256(), set(), ['.']
    while stack:
        parent = stack.pop()
        if parent in visited:
            raise SnapshotError('source index directory cycle')
        visited.add(parent)
        entries = children[parent]
        dirs, members = sorted(entries['directory']), sorted(entries['file'])
        for kind, values in ((b'D', dirs), (b'F', members)):
            for name, size in values:
                digest.update(kind+b'\0'+name.encode('utf-8', errors='surrogateescape')+b'\0')
                digest.update(str(size if kind == b'F' else 0).encode('ascii')+b'\0\n')
        stack.extend(name for name, _ in reversed(dirs))
    if visited != set(children):
        raise SnapshotError('source index includes an unparented namespace')
    return {'snapshot_root_name': root_name, 'excluded_subtrees': [],
            'selection': {'maximum_file_bytes': None, 'symlinks': 'included'},
            'files': files, 'directories': directories, 'symlinks': 0, 'logical_bytes': logical,
            'omitted_files_above_maximum': 0, 'omitted_bytes_above_maximum': 0,
            'portable_fingerprint_sha256': digest.hexdigest()}


def publish(cold: Path, stage: Path, dataset: str, identity: str, index: Path, root_name: str, *,
            node_id=None, guard=lambda: None, progress=None,
            admit_install=lambda _prepared, _journal: None, admit_next=lambda _item: None,
            install_lock=None):
    cold, stage, index = (Path(path).absolute() for path in (cold, stage, index))
    dataset = p.validate_slug(dataset, 'dataset')
    identity = p._validate_hash(identity, 'sealed source identity')
    node_id = p._resolve_node_id(cold, node_id)
    if p._paths_overlap(cold, stage) or stage.is_symlink() or stage.resolve(strict=True) != stage:
        raise SnapshotError('source archive stage is redirected or overlaps D')
    prepared = s._read_prepared(stage, dataset, identity)
    if prepared.get('source_mode') != 'retained_handoff_C_index':
        raise SnapshotError('sealed E archive is not a source-index generation')
    source = source_summary(index, prepared, root_name)
    journal_path = stage/'install.json'
    if journal_path.is_symlink():
        raise SnapshotError('source archive install journal redirected')
    journal = json.loads(journal_path.read_text()) if journal_path.is_file() else {}
    if not journal:
        journal = {'contract': s.CONTRACT+'.install', 'dataset': dataset,
                   'old_manifest_sha256': identity, 'prepared_sha256': prepared['identity_sha256'],
                   'state': 'installing', 'installed': []}
    if (journal.get('contract') != s.CONTRACT+'.install' or journal.get('dataset') != dataset
            or journal.get('old_manifest_sha256') != identity
            or journal.get('prepared_sha256') != prepared['identity_sha256']
            or journal.get('state') not in {'installing', 'manifest_planned', 'head_pending'}
            or not isinstance(journal.get('installed'), list)):
        raise SnapshotError('source archive install journal identity differs')
    guard()
    locks = cold/'.local-state/locks'
    with ExitStack() as stack:
        stack.enter_context(p._try_publication_lock(locks/'publish-retention-global.lock', shared=True))
        stack.enter_context(p._try_publication_lock(locks/f'publish-{dataset}.lock'))
        if install_lock is not None:
            stack.enter_context(p._try_publication_lock(Path(install_lock)))
            guard()
        head_root = cold/'heads'/dataset
        if head_root.is_symlink():
            raise SnapshotError('private D source head namespace redirected')
        heads = list(head_root.glob('*.json')) if head_root.is_dir() else []
        if heads:
            if len(heads) != 1 or heads[0].name != node_id+'.json' or journal.get('state') != 'head_pending':
                raise ExistingPrivateHead('private D dataset already has a different current owner')
            current = json.loads(heads[0].read_text())
            if current.get('manifest_sha256') != journal.get('new_manifest_sha256'):
                raise SnapshotError('private D source head differs from the install journal')
        admit_install(prepared, journal if journal_path.exists() else {})
        if not journal_path.exists():
            atomic_write_json(journal_path, journal, durable=True)
        items = [*prepared['packages'], prepared['inventory']]
        installed = {item['sha256']: item for item in journal['installed']}
        if len(installed) != len(journal['installed']):
            raise SnapshotError('source archive install contains duplicate objects')
        completed = 0
        total = sum(item['bytes'] for item in items)
        if progress is not None:
            progress({'stage': 'installing_D_objects', 'objects_completed': 0,
                      'objects_total': len(items), 'encoded_bytes_installed': 0,
                      'encoded_bytes_total': total})
        for number, item in enumerate(items, 1):
            digest = item['sha256']
            inventory = item is prepared['inventory']
            target = cold.joinpath(*p._object_relpath('inventories' if inventory else 'packs', digest,
                                                      '.jsonl.gz' if inventory else '.zip').parts)
            origin = s._safe_stage_file(stage, item['name'])
            if not s._signature_matches(origin, item['E_signature']):
                raise SnapshotError('sealed E archive changed before D install')
            known = installed.get(digest)
            if known is not None:
                if target.is_symlink() or not target.is_file() or not s._signature_matches(target, known.get('D_signature')):
                    raise SnapshotError('installed D source object generation differs')
            else:
                guard()
                pending = journal.get('pending')
                if (isinstance(pending, dict) and pending.get('sha256') == digest
                        and pending.get('target_absent_before') is True and os.path.lexists(target)):
                    if target.is_symlink() or not target.is_file() or target.stat().st_size != item['bytes']:
                        raise SnapshotError('interrupted private D source object differs')
                else:
                    admit_next(item)
                    journal['pending'] = {'sha256': digest, 'target_absent_before': not os.path.lexists(target)}
                    atomic_write_json(journal_path, journal, durable=True)
                    def on_copy(sent):
                        if progress is not None:
                            progress({'stage': 'streaming_D_object', 'objects_completed': number-1,
                                'objects_total': len(items), 'encoded_bytes_installed': completed,
                                'encoded_bytes_streamed': completed+sent, 'encoded_bytes_total': total})
                    p._install_immutable_object(cold, origin, target, expected_sha256=digest,
                                                retain_source=True, on_copy=on_copy)
                installed[digest] = {'sha256': digest, 'D_signature': s._signature(target)}
                journal['installed'] = list(installed.values())
                journal.pop('pending', None)
                atomic_write_json(journal_path, journal, durable=True)
            completed += item['bytes']
            if progress is not None:
                progress({'stage': 'installing_D_objects', 'objects_completed': number,
                          'objects_total': len(items), 'encoded_bytes_installed': completed,
                          'encoded_bytes_total': total})
        if set(installed) != {item['sha256'] for item in items}:
            raise SnapshotError('private D source install contains unknown objects')
        if journal['state'] == 'installing':
            template = {'schema_version': p.PACKED_SNAPSHOT_SCHEMA_VERSION,
                        'snapshot_id': prepared['old_snapshot_id'], 'archive': {'format': p.PACKED_ARCHIVE_FORMAT}}
            archive = s._archive(prepared, template)
            archive.pop('base_snapshot_id', None)
            now = time.time_ns()
            stamp = p.next_hlc(p._observed_stamps(cold, dataset, now_ns=now,
                max_clock_skew_seconds=p.DEFAULT_MAX_CLOCK_SKEW_SECONDS, require_objects=False), node_id=node_id, now_ns=now)
            seconds, nanos = divmod(stamp.physical_ns, 1_000_000_000)
            snapshot_id = p.validate_slug(f"{dataset[:40]}-{time.strftime('%Y%m%dT%H%M%S', time.gmtime(seconds))}"
                f"{nanos:09d}Z-l{stamp.logical}-{node_id[:40]}-{archive['inventory']['sha256'][:16]}", 'snapshot_id')
            manifest = {'schema_version': p.PACKED_SNAPSHOT_SCHEMA_VERSION, 'dataset': dataset,
                        'snapshot_id': snapshot_id, 'published_at': p._utc_iso_from_ns(stamp.physical_ns),
                        'hlc': stamp.to_dict(), 'publisher': {'node_id': node_id}, 'source': source,
                        'archive': archive, 'metadata': {'migration_source_identity_sha256': identity,
                            'source_mode': 'retained_handoff_C_index', 'public_dataset_merged': False,
                            'independent_D_readback': False, 'NAS_accepted': False}}
            p._validate_manifest(manifest)
            journal.update(state='manifest_planned', manifest=manifest)
            atomic_write_json(journal_path, journal, durable=True)
        manifest = journal['manifest']
        p._validate_manifest(manifest)
        if (manifest.get('dataset') != dataset or manifest.get('source') != source
                or manifest.get('metadata', {}).get('migration_source_identity_sha256') != identity):
            raise SnapshotError('planned private D source manifest differs')
        s.validate_new_inventory(cold, manifest, progress=progress)
        if progress is not None:
            progress({'stage': 'committing_D_head', 'objects_completed': len(items),
                      'objects_total': len(items), 'encoded_bytes_installed': total,
                      'encoded_bytes_total': total})
        snapshot_id = p.validate_slug(manifest['snapshot_id'], 'snapshot_id')
        manifest_rel = Path('manifests')/dataset/(snapshot_id+'.json')
        p._ensure_shared_packed_directory(cold, (cold/manifest_rel).parent)
        manifest_sha = p.write_immutable_json(cold/manifest_rel, manifest)
        if journal['state'] == 'head_pending' and (journal.get('new_manifest_sha256') != manifest_sha
                                                   or journal.get('snapshot_id') != snapshot_id):
            raise SnapshotError('planned private D head differs from its journal')
        journal.update(state='head_pending', new_manifest_sha256=manifest_sha, snapshot_id=snapshot_id)
        atomic_write_json(journal_path, journal, durable=True)
        head = {'schema_version': p.PACKED_HEAD_SCHEMA_VERSION, 'dataset': dataset, 'node_id': node_id,
                'snapshot_id': snapshot_id, 'hlc': manifest['hlc'], 'updated_at': manifest['published_at'],
                'manifest_relpath': manifest_rel.as_posix(), 'manifest_sha256': manifest_sha}
        head_path = cold/'heads'/dataset/(node_id+'.json')
        p._ensure_shared_packed_directory(cold, head_path.parent)
        guard()
        if head_path.exists():
            if head_path.is_symlink() or json.loads(head_path.read_text()) != head:
                raise SnapshotError('existing private D source head differs')
        else:
            atomic_write_json(head_path, head, durable=True)
    return {'state': 'published', 'dataset': dataset, 'source_identity': identity,
            'new_manifest_sha256': manifest_sha, 'snapshot_id': snapshot_id,
            'files': prepared['files'], 'logical_bytes': prepared['logical_bytes'],
            'objects': len(prepared['packages']), 'public_dataset_merged': False}
