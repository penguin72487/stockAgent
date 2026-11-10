"""Prepare an exact same-root private inventory using existing donor objects.

This module only prepares metadata on E. It does not publish a head, remove an
object, change an existing release, or claim that payloads were re-read. The
caller must keep both fixed generations under their original owner locks.
"""
from __future__ import annotations

from collections import defaultdict
import hashlib
import json
import os
from pathlib import Path
import re
import uuid

from stockagent.data_sync import packed_snapshots as p, stream_repack as s
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

CONTRACT = 'stockagent.private_same_root_subset_reuse.v1'
SOURCE_MODE = 'private_same_root_inventory_reuse'
FORMAT = 'same-root-subset-v1'
# Exact source-root pairs; other provider owners remain independent. Each
# execution also fixes both heads and compares every target inventory row.
REVIEWED_PAIRS = {
    'local-bybit': 'bybit',
    'local-forex-frankfurter': 'forex-frankfurter',
    'local-taifex-public-history': 'taifex-public-workspace',
}


def incompatibility_path(workspace: Path, dataset: str, old_sha: str, donor_sha: str) -> Path:
    if dataset not in REVIEWED_PAIRS:
        raise SnapshotError('same-root subset pair is not enrolled')
    p._validate_hash(old_sha, 'target manifest SHA')
    p._validate_hash(donor_sha, 'donor manifest SHA')
    return Path(workspace)/'migration/private-subset-incompatible-v1'/dataset/(old_sha+'-'+donor_sha+'.json')


def incompatible(workspace: Path, dataset: str, old_sha: str, donor_sha: str) -> bool:
    path = incompatibility_path(workspace, dataset, old_sha, donor_sha)
    if not os.path.lexists(path):
        return False
    if path.is_symlink() or not path.is_file() or path.resolve(strict=True) != path:
        raise SnapshotError('same-root incompatibility evidence redirected')
    value = _stable_document(path, limit=16384)
    if (value.get('contract') != CONTRACT+'.incompatible' or value.get('identity_sha256') != s._identity(value)
            or value.get('dataset') != dataset or value.get('old_manifest_sha256') != old_sha
            or value.get('donor_manifest_sha256') != donor_sha
            or value.get('reason') not in {'missing_path', 'different_metadata'}):
        raise SnapshotError('same-root incompatibility evidence differs')
    return True


def record_incompatibility(stage: Path, dataset: str, old_sha: str, donor_sha: str,
                           intent: dict, pin, reason: str, donor_pin=None) -> bool:
    """Close only our empty comparison attempt, then cache its fixed mismatch.

    Never move or delete a prepared inventory, install journal, or unknown
    stage member. Caller holds the original publication locks and current head.
    """
    stage = Path(stage).absolute()
    if (reason not in {'missing_path', 'different_metadata'} or stage.name != old_sha
            or stage.parent.name != dataset or stage.parent.parent.name != 'stream-source-repack-v1'
            or stage.parent.parent.parent.name != 'migration' or stage.is_symlink()
            or stage.resolve(strict=True) != stage):
        return False
    if {entry.name for entry in stage.iterdir()} != {'intent.json'}:
        return False
    intent_path = stage/'intent.json'
    if intent_path.is_symlink() or _stable_document(intent_path, limit=16384) != intent:
        raise SnapshotError('same-root incompatible stage intent changed')
    workspace = stage.parents[3]
    path = incompatibility_path(workspace, dataset, old_sha, donor_sha)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.parent.is_symlink() or path.parent.resolve(strict=True) != path.parent:
        raise SnapshotError('same-root incompatibility destination redirected')
    s._clear_repack_pin(donor_pin)
    s._clear_repack_pin(pin)
    intent_path.unlink()
    stage.rmdir()
    p._fsync_directory(stage.parent)
    value = {'contract':CONTRACT+'.incompatible', 'dataset':dataset,
             'old_manifest_sha256':old_sha, 'donor_manifest_sha256':donor_sha,
             'reason':reason, 'source_deleted':False, 'payload_read':False}
    value['identity_sha256'] = s._identity(value)
    if os.path.lexists(path):
        if _stable_document(path, limit=16384) != value:
            raise SnapshotError('same-root incompatibility record differs')
    else:
        atomic_write_json(path, value, durable=True)
    return True


def clear_scoped_donor_pin(pin_root: Path | None, donor: str, donor_sha: str, stage: Path) -> None:
    if pin_root is None:
        return
    path = Path(pin_root)/'.stream-repack-pins'/donor/s._repack_pin_name(donor_sha, stage, scoped=True)
    if not os.path.lexists(path):
        return
    if path.is_symlink() or not path.is_file():
        raise SnapshotError('same-root donor pin redirected')
    value = _stable_document(path, limit=16384)
    if (value.get('contract') != s.CONTRACT+'.source-pin' or value.get('dataset') != donor
            or value.get('manifest_sha256') != donor_sha or value.get('stage') != str(stage)
            or not isinstance(value.get('manifest'), dict)):
        raise SnapshotError('same-root donor pin identity differs')
    p.validate_slug(value['manifest'].get('snapshot_id'), 'donor snapshot')
    s._clear_repack_pin((path, value))


def supersede_uninstalled_stage(stage: Path, old_intent: dict, new_intent: dict,
                                old_snapshot_id: str, pin_root: Path | None) -> None:
    """Preserve a superseded E-only attempt so a changed donor can retry.

    The caller holds both fixed dataset locks and the private publication
    owner, and has confirmed the original target is still the current head.
    No installed transaction or unexpected stage member is moved.
    """
    stage = Path(stage).absolute()
    name, old_sha = new_intent['dataset'], new_intent['old_manifest_sha256']
    previous_donor = p._validate_hash(old_intent.get('subset_donor_manifest_sha256'), 'old donor SHA')
    if (stage.name != old_sha or stage.parent.name != name
            or stage.parent.parent.name != 'stream-source-repack-v1'
            or stage.parent.parent.parent.name != 'migration' or stage.is_symlink()
            or stage.resolve(strict=True) != stage
            or old_intent != {'contract':s.CONTRACT+'.intent', 'dataset':name,
                'old_manifest_sha256':old_sha, 'subset_donor':REVIEWED_PAIRS.get(name),
                'subset_donor_manifest_sha256':previous_donor}
            or _stable_document(stage/'intent.json', limit=16384) != old_intent):
        raise SnapshotError('superseded subset stage identity differs')
    allowed = re.compile(r'(?:intent\.json|prepared\.json|subset-prepared\.json|'
                         r'inventory-[a-f0-9]{64}\.jsonl\.gz|subset-inventory-[a-f0-9]{32}\.partial)\Z')
    for path in stage.iterdir():
        if allowed.fullmatch(path.name) is None or path.is_symlink() or not path.is_file():
            raise SnapshotError('superseded subset stage has installed or unknown content')
    if (stage/'prepared.json').exists():
        previous = s._prepared_metadata(stage, name, old_sha)
        if (previous.get('packed_format') != FORMAT
                or previous.get('donor_manifest_sha256') != previous_donor):
            raise SnapshotError('superseded subset prepared identity differs')
    workspace = stage.parents[3]
    directory = workspace/'migration/private-subset-superseded-v1'/name/old_sha
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    if directory.is_symlink() or directory.resolve(strict=True) != directory:
        raise SnapshotError('superseded subset destination redirected')
    if pin_root is not None:
        pin_path = Path(pin_root)/'.stream-repack-pins'/name/s._repack_pin_name(old_sha, stage)
        if os.path.lexists(pin_path):
            expected = {'contract':s.CONTRACT+'.source-pin', 'dataset':name,
                        'manifest':{'snapshot_id':p.validate_slug(old_snapshot_id, 'old snapshot')},
                        'manifest_sha256':old_sha, 'stage':str(stage)}
            s._clear_repack_pin((pin_path, expected))
        clear_scoped_donor_pin(pin_root, old_intent['subset_donor'], previous_donor, stage)
    destination = directory/(previous_donor+'-'+uuid.uuid4().hex)
    os.rename(stage, destination)
    p._fsync_directory(directory)
    p._fsync_directory(stage.parent)
    value = {'contract':CONTRACT+'.superseded', 'dataset':name, 'old_manifest_sha256':old_sha,
             'previous_donor_manifest_sha256':previous_donor,
             'new_donor_manifest_sha256':new_intent['subset_donor_manifest_sha256'],
             'original_target_head_preserved':True, 'source_deleted':False, 'D_install_started':False}
    value['identity_sha256'] = s._identity(value)
    atomic_write_json(destination/'superseded.json', value, durable=True)
    stage.mkdir(mode=0o700)
    atomic_write_json(stage/'intent.json', new_intent, durable=True)


def _stable_document(path: Path, *, limit=32*1024**2) -> dict:
    from stockagent.data_sync.packed_member_stream import read_stable_bytes
    value = json.loads(read_stable_bytes(path, maximum=limit))
    if not isinstance(value, dict):
        raise SnapshotError('subset reuse document is not an object')
    return value


def _committed_link_proof(cold: Path, workspace: Path, donor, refs, installed) -> dict:
    """Follow the original committed cross-authority hardlink transaction.

    The caller holds both original publication authorities. A different inode,
    content generation, uncommitted link or unknown journal never qualifies.
    """
    from stockagent.data_sync import semantic_link_promotion as promotion
    name = donor.manifest['dataset']
    primary = Path(cold).parent.parent
    if (name != 'bybit' or Path(cold) != primary/'.local-state/private-history'):
        raise SnapshotError('donor installed object generation changed')
    public = p.resolve_latest_packed(primary, name, require_objects=True)
    meta = public.manifest.get('metadata', {})
    comparison = p._validate_hash(meta.get('latest_comparison_identity_sha256'), 'link comparison SHA')
    if (meta.get('selection_contract') != promotion.LATEST_CONTRACT
            or meta.get('latest_fixed_private_manifest_sha256') != donor.manifest_sha256):
        raise SnapshotError('donor installed object generation changed')
    path = Path(workspace)/'migration/handoff-index-archive-v1'/name/('latest-promotion-'+comparison+'.json')
    from stockagent.data_sync.packed_member_stream import read_stable_bytes
    raw = read_stable_bytes(path, maximum=32*1024**2)
    journal = json.loads(raw)
    links = journal.get('linked')
    if (journal.get('contract') != promotion.LATEST_CONTRACT or journal.get('state') != 'committed'
            or journal.get('dataset') != name or journal.get('comparison_identity_sha256') != comparison
            or journal.get('private_manifest_sha256') != donor.manifest_sha256
            or journal.get('private_snapshot_id') != donor.manifest['snapshot_id']
            or journal.get('new_manifest_sha256') != public.manifest_sha256
            or journal.get('new_snapshot_id') != public.manifest['snapshot_id']
            or journal.get('manifest') != public.manifest or not isinstance(links, list)):
        raise SnapshotError('donor link promotion identity differs')
    linked = {row.get('sha256'): row for row in links if isinstance(row, dict)}
    if len(linked) != len(links) or set(linked) != {row['sha256'] for row in refs}:
        raise SnapshotError('donor link promotion object set differs')
    for row in refs:
        link = linked[row['sha256']]
        if (not promotion._same_original_after_link(installed[row['sha256']].get('D_signature'),
                                                    link.get('source_signature'))
                or not promotion._linked_generation_matches(Path(cold), primary, row, link)):
            raise SnapshotError('donor installed object generation changed')
    return {'comparison_sha256':comparison, 'journal_sha256':hashlib.sha256(raw).hexdigest(),
            'public_manifest_sha256':public.manifest_sha256}


def retained_handoff_donor_proof(cold: Path, workspace: Path, donor, *, layout_confirmed: bool) -> dict:
    """Bind a donor to its original source-hashed E build and installed D inodes."""
    name, manifest = donor.manifest['dataset'], donor.manifest
    metadata = manifest.get('metadata', {})
    identity = p._validate_hash(metadata.get('migration_source_identity_sha256'), 'donor source identity')
    if (layout_confirmed is not True or metadata.get('source_mode') != 'retained_handoff_C_index'
            or metadata.get('public_dataset_merged') is not False):
        raise SnapshotError('donor is not a source-hashed private handoff')
    stage = Path(workspace)/'migration/handoff-index-archive-v1'/name/identity
    if stage.is_symlink() or stage.resolve(strict=True) != stage:
        raise SnapshotError('donor handoff stage is missing or redirected')
    prepared = s._prepared_metadata(stage, name, identity)
    from stockagent.data_sync.packed_member_stream import read_stable_bytes
    raw = read_stable_bytes(stage/'install.json', maximum=32*1024**2)
    journal = json.loads(raw)
    if (prepared.get('source_mode') != 'retained_handoff_C_index'
            or prepared.get('source_member_sha_streamed') is not True
            or prepared.get('files') != manifest['source']['files']
            or prepared.get('logical_bytes') != manifest['source']['logical_bytes']
            or journal.get('contract') != s.CONTRACT+'.install'
            or journal.get('state') != 'head_pending' or journal.get('dataset') != name
            or journal.get('old_manifest_sha256') != identity
            or journal.get('prepared_sha256') != prepared['identity_sha256']
            or journal.get('new_manifest_sha256') != donor.manifest_sha256
            or journal.get('snapshot_id') != manifest['snapshot_id']
            or not isinstance(journal.get('installed'), list)):
        raise SnapshotError('donor source-hashed installation identity differs')
    refs = [*manifest['archive']['objects'], manifest['archive']['inventory']]
    installed = {row['sha256']: row for row in journal['installed']}
    expected = {row['sha256'] for row in [*prepared['packages'], prepared['inventory']]}
    if (len(installed) != len(journal['installed']) or expected != {row['sha256'] for row in refs}
            or set(installed) != expected):
        raise SnapshotError('donor source-hashed installation members differ')
    mismatched = False
    for row in refs:
        path = p._path_under(Path(cold), row['relpath'], 'donor object')
        if path.is_symlink() or not path.is_file() or path.stat().st_size != row['bytes']:
            raise SnapshotError('donor installed object generation changed')
        mismatched |= not s._signature_matches(path, installed[row['sha256']].get('D_signature'))
    linked_proof = _committed_link_proof(cold, workspace, donor, refs, installed) if mismatched else None
    proof = {'contract': CONTRACT+'.donor-proof', 'dataset': name,
        'manifest_sha256': donor.manifest_sha256, 'snapshot_id': manifest['snapshot_id'],
        'source_identity_sha256': identity, 'prepared_sha256': prepared['identity_sha256'],
        'install_journal_sha256': hashlib.sha256(raw).hexdigest(),
        **({'committed_link_proof':linked_proof} if linked_proof is not None else {}),
        'source_member_sha_streamed': True, 'source_layout_confirmed': True,
        'source_payload_reread': False, 'independent_D_readback': False}
    proof['identity_sha256'] = s._identity(proof)
    return proof


def _comparison_rows(cold: Path, original: dict, donor: dict, usage: dict, counts: dict,
                     guard=lambda: None, progress=None):
    """Merge sorted paths and retain the target's metadata with donor storage."""
    others = iter(p._iter_inventory(cold, donor))
    right = next(others, None)
    for number, left in enumerate(p._iter_inventory(cold, original), 1):
        if number % 8192 == 0:
            guard()
            if progress is not None:
                progress({'stage':'comparing_fixed_inventories', 'items_completed':number-1,
                          'items_total':sum(original['source'][k] for k in ('files','directories','symlinks'))})
        while right is not None and right['path'] < left['path']:
            right = next(others, None)
        if right is None or right['path'] != left['path']:
            raise SnapshotError('fixed donor does not cover every target path')
        own = {key: value for key, value in left.items() if key != 'storage'}
        candidate = {key: value for key, value in right.items() if key != 'storage'}
        if own != candidate:
            raise SnapshotError('fixed donor target metadata or source SHA differs')
        result = dict(left)
        if left['kind'] == 'file':
            storage = right.get('storage')
            if not isinstance(storage, dict) or storage.get('kind') not in {'pack', 'blob', 'chunks'}:
                raise SnapshotError('fixed donor has unsupported storage')
            if storage['kind'] == 'pack' and storage.get('member') != left['path']:
                raise SnapshotError('donor pack member does not retain the target path')
            if storage['kind'] == 'chunks' and original['schema_version'] != p.CHUNKED_SNAPSHOT_SCHEMA_VERSION:
                raise SnapshotError('target reader contract does not support donor chunks')
            per_file = defaultdict(int)
            for digest, size in p._storage_extents(storage, left['size']):
                per_file[digest] += size
            for digest, size in per_file.items():
                usage[digest]['file_count'] += 1
                usage[digest]['logical_bytes'] += size
            result['storage'] = dict(storage)
            counts['files'] += 1
            counts['logical_bytes'] += left['size']
        counts['items'] += 1
        entry = p._entry_from_record(result)
        if entry.inventory_row() != result:
            raise SnapshotError('subset reuse cannot preserve unknown inventory fields')
        yield entry
        right = next(others, None)


def prepare(cold: Path, stage: Path, original, donor, *, guard=lambda: None, donor_proof=None,
            progress=None) -> dict:
    """Seal a fixed E inventory; no data payload or original is removed."""
    cold, stage = Path(cold).absolute(), Path(stage).absolute()
    target, candidate = original.manifest, donor.manifest
    if (target['dataset'] == candidate['dataset']
            or target.get('schema_version') not in {p.PACKED_SNAPSHOT_SCHEMA_VERSION, p.CHUNKED_SNAPSHOT_SCHEMA_VERSION}
            or candidate.get('schema_version') not in {p.PACKED_SNAPSHOT_SCHEMA_VERSION, p.CHUNKED_SNAPSHOT_SCHEMA_VERSION}
            or not target.get('source', {}).get('snapshot_root_name')
            or target['source']['snapshot_root_name'] != candidate.get('source', {}).get('snapshot_root_name')
            or candidate.get('archive', {}).get('new_pack_layout') != p.SOURCE_DATASET_PACK_LAYOUT):
        raise SnapshotError('subset reuse requires different fixed datasets from the same source root')
    if stage.is_symlink() or stage.resolve(strict=True) != stage or not stage.is_dir():
        raise SnapshotError('subset reuse stage is unavailable or redirected')
    guard()
    def report_validation(label):
        def report(event):
            guard()
            if progress is not None:
                progress({'stage':label, 'items_completed':event['completed_items'],
                          'items_total':event['total_items']})
        return report
    p._validate_inventory(target, p._iter_inventory(cold, target), retain_object_members=False,
                          progress=report_validation('checking_target_inventory'))
    p._validate_inventory(candidate, p._iter_inventory(cold, candidate), retain_object_members=False,
                          progress=report_validation('checking_donor_inventory'))
    usage = defaultdict(lambda: {'file_count': 0, 'logical_bytes': 0})
    counts = {'items': 0, 'files': 0, 'logical_bytes': 0}
    temporary = stage/('subset-inventory-'+uuid.uuid4().hex+'.partial')
    try:
        digest = p._write_inventory(temporary, _comparison_rows(cold, target, candidate, usage, counts, guard, progress))
        if (counts['files'] != target['source']['files']
                or counts['logical_bytes'] != target['source']['logical_bytes']
                or counts['items'] != sum(target['source'][key] for key in ('files', 'directories', 'symlinks'))):
            raise SnapshotError('subset reuse target counts differ')
        final = stage/('inventory-'+digest+'.jsonl.gz')
        if os.path.lexists(final):
            if final.is_symlink() or not final.is_file() or p.sha256_file(final) != digest:
                raise SnapshotError('existing subset inventory differs')
            temporary.unlink()
        else:
            os.rename(temporary, final)
            p._fsync_directory(stage)
        descriptors = {obj['sha256']: obj for obj in candidate['archive']['objects']}
        objects = []
        for sha, used in sorted(usage.items()):
            if sha not in descriptors:
                raise SnapshotError('subset reuse references an unknown donor object')
            obj = dict(descriptors[sha])
            stored_count = int(obj.get('stored_file_count', obj['file_count']))
            stored_bytes = int(obj.get('stored_logical_bytes', obj['logical_bytes']))
            obj.update(file_count=used['file_count'], logical_bytes=used['logical_bytes'])
            if obj['kind'] == 'pack' and (obj['file_count'] != stored_count or obj['logical_bytes'] != stored_bytes
                                          or obj.get('member_selection') == 'subset'):
                obj.update(member_selection='subset', stored_file_count=stored_count, stored_logical_bytes=stored_bytes)
            objects.append(obj)
        objects.sort(key=lambda item: (item['kind'], item['sha256']))
        size = final.stat().st_size
        archive = {**target['archive'], 'inventory': {'bytes': size, 'format': p.INVENTORY_FORMAT,
            'relpath': p._object_relpath('inventories', digest, '.jsonl.gz').as_posix(), 'sha256': digest},
            'objects': objects, 'object_count': len(objects),
            'stored_bytes': sum(item['bytes'] for item in objects)+size,
            'new_pack_layout': p.SOURCE_DATASET_PACK_LAYOUT,
            'new_pack_target_bytes': candidate['archive'].get('new_pack_target_bytes', s.TARGET_BYTES),
            'base_snapshot_id': target['snapshot_id'], 'reused_files': 0,
            'external_reused_files': counts['files'], 'changed_files': 0,
            'new_object_count': 1, 'new_stored_bytes': size}
        proposal = {**target, 'archive': archive}
        p._validate_inventory(proposal, p._iter_inventory(stage, {'archive': {'inventory': {
            'relpath': final.name, 'sha256': digest, 'bytes': size}}}), retain_object_members=False)
        value = {'contract': CONTRACT+'.prepared', 'dataset': target['dataset'],
            'old_snapshot_id': target['snapshot_id'], 'old_manifest_sha256': original.manifest_sha256,
            'donor_dataset': candidate['dataset'], 'donor_snapshot_id': candidate['snapshot_id'],
            'donor_manifest_sha256': donor.manifest_sha256, 'source_root_name': target['source']['snapshot_root_name'],
            'donor_proof': donor_proof,
            'source_inventory_sha256': target['archive']['inventory']['sha256'],
            'donor_inventory_sha256': candidate['archive']['inventory']['sha256'],
            'files': counts['files'], 'logical_bytes': counts['logical_bytes'], 'matched_items': counts['items'],
            'inventory': {'name': final.name, 'sha256': digest, 'bytes': size, 'E_signature': s._signature(final)},
            'archive': archive, 'source_payload_reread': False, 'destination_independent_readback': False,
            'NAS_accepted': False}
        value['identity_sha256'] = s._identity(value)
        atomic_write_json(stage/'subset-prepared.json', value, durable=True)
        return value
    except BaseException:
        if temporary.is_file() and not temporary.is_symlink():
            temporary.unlink()
        raise


def seal_stream_prepared(stage: Path, candidate: dict) -> dict:
    """Bind the comparison to the canonical repack journal contract.

    The record is metadata-only: it must never claim a fresh payload read.
    """
    proof = candidate.get('donor_proof')
    if (candidate.get('contract') != CONTRACT+'.prepared'
            or candidate.get('identity_sha256') != s._identity(candidate)
            or candidate.get('source_payload_reread') is not False
            or candidate.get('destination_independent_readback') is not False
            or candidate.get('NAS_accepted') is not False
            or REVIEWED_PAIRS.get(candidate.get('dataset')) != candidate.get('donor_dataset')
            or not isinstance(proof, dict) or proof.get('contract') != CONTRACT+'.donor-proof'
            or proof.get('identity_sha256') != s._identity(proof)
            or proof.get('dataset') != candidate.get('donor_dataset')
            or proof.get('manifest_sha256') != candidate.get('donor_manifest_sha256')
            or proof.get('source_member_sha_streamed') is not True
            or proof.get('source_layout_confirmed') is not True):
        raise SnapshotError('same-root subset candidate identity differs')
    body = {'contract': s.CONTRACT+'.prepared', 'dataset': candidate['dataset'],
        'old_manifest_sha256': candidate['old_manifest_sha256'],
        'old_snapshot_id': candidate['old_snapshot_id'], 'packages': [],
        'inventory': candidate['inventory'], 'files': candidate['files'],
        'logical_bytes': candidate['logical_bytes'], 'packed_format': FORMAT,
        'source_mode': SOURCE_MODE, 'candidate_identity_sha256': candidate['identity_sha256'],
        'donor_dataset': candidate['donor_dataset'], 'donor_manifest_sha256': candidate['donor_manifest_sha256'],
        'source_inventory_sha_compared': True, 'donor_source_layout_confirmed': True,
        'source_member_sha_streamed': False, 'destination_independent_readback': False,
        'raw_extraction_tree_created': False, 'archive': candidate['archive']}
    body['identity_sha256'] = s._identity(body)
    path = stage/'prepared.json'
    if path.exists():
        if path.is_symlink() or _stable_document(path) != body:
            raise SnapshotError('same-root subset prepared generation changed')
    else:
        atomic_write_json(path, body, durable=True)
    return body
