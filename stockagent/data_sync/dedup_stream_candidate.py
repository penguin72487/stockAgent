"""Build an isolated content-deduplicated candidate from a fixed packed head.

This candidate alone never authorizes source retirement or a D-head switch.
Its inventory can reference one ZIP member from multiple original paths.
Adoption creates a separately bound v3 prepared record after the versioned
metadata contract succeeds; it does not itself change a head or delete data.
"""
from __future__ import annotations

from contextlib import ExitStack, closing
import json
import os
from pathlib import Path
import sqlite3
import stat
import time
import uuid

from stockagent.data_sync import packed_dedup_plan as plan, packed_snapshots as p, stream_repack as s
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.packed_metadata import MetadataRows

CONTRACT = 'stockagent.source_local_content_dedup_candidate.v1'
# Bump this whenever the v3 inventory validation contract changes. The cache
# records only a successful metadata pass for one immutable candidate identity.
ADOPTION_VALIDATION = CONTRACT+'.inventory-validation-v1'


class _NoOutput:
    def upsert(self, _entry):
        pass  # The complete logical inventory is streamed once after sealing.


def _plan(index: Path, dataset: str, expected_sha: str, manifest: dict):
    index = Path(index).absolute()
    if (index.is_symlink() or index.resolve(strict=True) != index or not index.is_file()
            or index.stat().st_nlink != 1):
        raise SnapshotError('dedup candidate plan index is redirected or shared')
    signature = s._signature(index)
    with closing(sqlite3.connect(index.as_uri()+'?mode=ro', uri=True)) as db:
        row = db.execute("SELECT value FROM meta WHERE key='summary'").fetchone()
        if row is None:
            raise SnapshotError('dedup candidate plan summary is missing')
        summary = json.loads(row[0])
        if (not isinstance(summary, dict) or summary.get('contract') != plan.CONTRACT
                or summary.get('dataset') != dataset or summary.get('old_manifest_sha256') != expected_sha
                or summary.get('inventory_sha256') != manifest['archive']['inventory']['sha256']
                or summary.get('files') != manifest['source']['files']
                or summary.get('logical_bytes') != manifest['source']['logical_bytes']
                or summary.get('inventory_metadata_validated') is not True
                or summary.get('source_payload_reread') is not False
                or summary.get('retirement_authorized') is not False):
            raise SnapshotError('dedup candidate plan identity differs')
        result = {}
        for digest, size, raw_path, occurrences in db.execute(
                'SELECT sha256,size,representative_path,occurrences FROM contents ORDER BY sha256'):
            p._validate_hash(digest, 'candidate content SHA')
            if (type(size) is not int or size < 0 or not isinstance(raw_path, bytes)
                    or type(occurrences) is not int or occurrences < 1):
                raise SnapshotError('dedup candidate plan entry is invalid')
            path = raw_path.decode('utf-8', errors='surrogatepass')
            if p._safe_relative_path(path, 'candidate representative').as_posix() != path:
                raise SnapshotError('dedup candidate representative path is invalid')
            result[digest] = (size, path, occurrences)
            if len(result) > plan.MAX_UNIQUE_CONTENTS:
                raise SnapshotError('dedup candidate plan exceeds its unique-item bound')
    if (len(result) != summary.get('unique_contents')
            or sum(item[0] for item in result.values()) != summary.get('unique_content_logical_bytes')
            or sum(item[2] for item in result.values()) != summary.get('files')
            or not s._signature_matches(index, signature)):
        raise SnapshotError('dedup candidate plan changed or its counts differ')
    return summary, result, signature


def read_sealed(stage: Path, dataset: str, expected: str) -> dict:
    """Check bounded metadata and file generations of a sealed candidate."""
    stage = Path(stage).absolute()
    path = stage/'candidate.json'
    if stage.is_symlink() or stage.resolve(strict=True) != stage or path.is_symlink() or not path.is_file():
        raise SnapshotError('dedup candidate record is redirected')
    value = json.loads(path.read_text())
    if (not isinstance(value, dict) or value.get('contract') != CONTRACT+'.candidate'
            or value.get('identity_sha256') != s._identity(value) or value.get('dataset') != dataset
            or value.get('old_manifest_sha256') != expected or value.get('publishable') is not False
            or value.get('retirement_authorized') is not False or value.get('unique_content_sha_streamed') is not True
            or value.get('all_original_occurrences_reread') is not False
            or not isinstance(value.get('packages'), list)
            or not isinstance(value.get('inventory'), dict)):
        raise SnapshotError('dedup candidate record differs')
    for item in value['packages']:
        if (not isinstance(item, dict) or item.get('name') != 'pack-'+str(item.get('sha256'))+'.zip'
                or any(type(item.get(field)) is not int or item[field] < 0 for field in
                       ('bytes', 'file_count', 'logical_bytes', 'stored_file_count', 'stored_logical_bytes'))
                or item['stored_file_count'] > item['file_count']):
            raise SnapshotError('dedup candidate package descriptor differs')
        p._validate_hash(item['sha256'], 'candidate pack SHA')
    inventory = value['inventory']
    if (inventory.get('name') != 'inventory-'+str(inventory.get('sha256'))+'.jsonl.gz'
            or type(inventory.get('bytes')) is not int or inventory['bytes'] < 0):
        raise SnapshotError('dedup candidate inventory descriptor differs')
    p._validate_hash(inventory['sha256'], 'candidate inventory SHA')
    if (sum(item['file_count'] for item in value['packages']) != value.get('files')
            or sum(item['logical_bytes'] for item in value['packages']) != value.get('logical_bytes')
            or sum(item['stored_file_count'] for item in value['packages']) != value.get('unique_contents')
            or sum(item['stored_logical_bytes'] for item in value['packages']) !=
               value.get('unique_content_logical_bytes')):
        raise SnapshotError('dedup candidate package counts differ')
    for item in [*value['packages'], value['inventory']]:
        if not s._signature_matches(s._safe_stage_file(stage, item['name']), item['E_signature']):
            raise SnapshotError('sealed dedup candidate generation changed')
    return value


def prepare_stream_adoption(cold: Path, stage: Path, dataset: str, expected: str, *,
                            progress=None, guard=lambda: None) -> dict:
    """Bind a sealed candidate to the original streaming installer's v3 ABI.

    One ordered metadata pass validates every logical path and its alias;
    payload bytes are not reread and no additional restored copy is created.
    The fixed source head and all E file generations are checked again while
    holding the dataset and retention owners before the record is committed.
    """
    cold, stage = Path(cold).absolute(), Path(stage).absolute()
    dataset = p.validate_slug(dataset, 'dataset')
    expected = p._validate_hash(expected, 'original manifest SHA')
    if (p._paths_overlap(cold, stage) or stage.is_symlink()
            or stage.resolve(strict=True) != stage or stat.S_IMODE(stage.stat().st_mode) != 0o700):
        raise SnapshotError('content-alias adoption stage is redirected or not private')
    prepared_path = stage/'prepared.json'
    if os.path.lexists(prepared_path):
        return s._read_prepared(stage, dataset, expected)
    candidate = read_sealed(stage, dataset, expected)
    old = p.resolve_latest_packed(cold, dataset, require_objects=False)
    if (old.manifest_sha256 != expected
            or candidate['old_inventory_sha256'] != old.manifest['archive']['inventory']['sha256']
            or candidate['files'] != old.manifest['source']['files']
            or candidate['logical_bytes'] != old.manifest['source']['logical_bytes']):
        raise SnapshotError('content-alias adoption original identity or counts differ')
    body = {'contract': s.CONTRACT+'.prepared', 'dataset': dataset,
            'old_manifest_sha256': expected, 'old_snapshot_id': old.manifest['snapshot_id'],
            'old_inventory_sha256': candidate['old_inventory_sha256'],
            'packages': candidate['packages'], 'inventory': candidate['inventory'],
            'files': candidate['files'], 'logical_bytes': candidate['logical_bytes'],
            'packed_format': 'content-alias-v3',
            'candidate_identity_sha256': candidate['identity_sha256'],
            'source_member_sha_streamed': False, 'unique_content_sha_streamed': True,
            'all_original_occurrences_reread': False, 'destination_independent_readback': False,
            'raw_extraction_tree_created': False}
    archive = s._archive(body, old.manifest)
    by_sha = {item['sha256']: item['name'] for item in candidate['packages']}
    if len(by_sha) != len(candidate['packages']):
        raise SnapshotError('content-alias candidate repeats a package SHA')
    for item in archive['objects']:
        item['relpath'] = by_sha[item['sha256']]
    archive['inventory']['relpath'] = candidate['inventory']['name']
    manifest = {**old.manifest, 'schema_version': p.ALIAS_SNAPSHOT_SCHEMA_VERSION, 'archive': archive}
    def report(event):
        guard()
        if progress is not None:
            progress(event)
    validation_path = stage/'adoption-validation.json'
    validation = {'contract': ADOPTION_VALIDATION, 'dataset': dataset,
                  'old_manifest_sha256': expected, 'candidate_sha256': candidate['identity_sha256'],
                  'old_inventory_sha256': candidate['old_inventory_sha256'],
                  'files': candidate['files'], 'logical_bytes': candidate['logical_bytes'],
                  'unique_contents': candidate['unique_contents'],
                  'unique_content_logical_bytes': candidate['unique_content_logical_bytes'],
                  'source_payload_reread': False, 'destination_independent_readback': False}
    validation['identity_sha256'] = s._identity(validation)
    cached = False
    if os.path.lexists(validation_path):
        if (validation_path.is_symlink() or not validation_path.is_file()
                or validation_path.stat().st_size > 16384):
            raise SnapshotError('content-alias validation record is redirected or oversized')
        try:
            cached = json.loads(validation_path.read_text()) == validation
        except (OSError, ValueError):
            cached = False
    guard()
    if not cached:
        result = p._validate_inventory(manifest, p._iter_inventory(stage, manifest),
                                       retain_object_members=False, progress=report)
        if (result['unique_content_files'] != candidate['unique_contents']
                or result['unique_content_logical_bytes'] != candidate['unique_content_logical_bytes']):
            raise SnapshotError('content-alias candidate unique member totals differ')
        # Confirm all sealed file generations before recording the reusable
        # pass. The fixed head and files are checked again under the owner
        # below; this record alone never publishes or authorizes retirement.
        if read_sealed(stage, dataset, expected) != candidate:
            raise SnapshotError('content-alias candidate changed during metadata validation')
        atomic_write_json(validation_path, validation, durable=True)
    locks = cold/'.local-state/locks'
    with p._try_publication_lock(locks/'publish-retention-global.lock', shared=True), \
            p._try_publication_lock(locks/f'publish-{dataset}.lock'):
        guard()
        if (p.resolve_latest_packed(cold, dataset, require_objects=False).manifest_sha256 != expected
                or read_sealed(stage, dataset, expected) != candidate):
            raise SnapshotError('content-alias candidate changed during metadata validation')
        checkpoint = stage/'checkpoint.sqlite'
        if os.path.lexists(checkpoint):
            checkpoint = s._safe_stage_file(stage, 'checkpoint.sqlite')
            if any(os.path.lexists(stage/name) for name in ('checkpoint.sqlite-wal', 'checkpoint.sqlite-shm')):
                raise SnapshotError('content-alias candidate checkpoint is still active')
            body['resume_index'] = {'name': checkpoint.name, 'bytes': checkpoint.stat().st_size,
                                    'E_signature': s._signature(checkpoint)}
        candidate_intent = {'contract': CONTRACT+'.intent', 'dataset': dataset, 'old_manifest_sha256': expected}
        stream_intent = {'contract': s.CONTRACT+'.intent', 'dataset': dataset, 'old_manifest_sha256': expected}
        intent_path, original_path = stage/'intent.json', stage/'candidate-intent.json'
        if intent_path.is_symlink() or not intent_path.is_file():
            raise SnapshotError('content-alias candidate intent is missing or redirected')
        intent = json.loads(intent_path.read_text())
        if intent == candidate_intent:
            if os.path.lexists(original_path) and (original_path.is_symlink()
                    or json.loads(original_path.read_text()) != candidate_intent):
                raise SnapshotError('content-alias saved candidate intent differs')
            atomic_write_json(original_path, candidate_intent, durable=True)
            atomic_write_json(intent_path, stream_intent, durable=True)
        elif (intent != stream_intent or original_path.is_symlink() or not original_path.is_file()
                or json.loads(original_path.read_text()) != candidate_intent):
            raise SnapshotError('content-alias candidate intent differs')
        body.update(candidate_inventory_metadata_validated=True, observed_at_epoch=time.time())
        body['identity_sha256'] = s._identity(body)
        atomic_write_json(prepared_path, body, durable=True)
    return s._read_prepared(stage, dataset, expected)


def prepare(cold: Path, resolved, stage: Path, index: Path, *, native=False, progress=None,
            guard=lambda: None) -> dict:
    """Prepare an E-only candidate; the caller owns the original source lock.

    The input plan is rechecked against every row of the fixed inventory. Each
    distinct content representative is streamed and checked against its source
    SHA once. Other occurrences use the fixed inventory's SHA and size; they
    are not individually reread, and that distinction remains in the result.
    """
    cold, stage, index = Path(cold).absolute(), Path(stage).absolute(), Path(index).absolute()
    manifest = resolved.manifest
    dataset = p.validate_slug(manifest['dataset'], 'dataset')
    expected = p._validate_hash(resolved.manifest_sha256, 'original manifest SHA')
    if p._paths_overlap(cold, stage) or stage.is_symlink():
        raise SnapshotError('dedup candidate stage must be outside the cold store')
    stage.mkdir(mode=0o700, parents=True, exist_ok=True)
    if stage.resolve(strict=True) != stage or stat.S_IMODE(stage.stat().st_mode) != 0o700:
        raise SnapshotError('dedup candidate stage is redirected or not private')
    intent = {'contract': CONTRACT+'.intent', 'dataset': dataset, 'old_manifest_sha256': expected}
    intent_path = stage/'intent.json'
    if os.path.lexists(intent_path):
        if intent_path.is_symlink() or json.loads(intent_path.read_text()) != intent:
            raise SnapshotError('dedup candidate stage belongs to another transaction')
    else:
        if any(stage.iterdir()):
            raise SnapshotError('dedup candidate stage is not empty')
        atomic_write_json(intent_path, intent, durable=True)
    guard()
    if p.resolve_latest_packed(cold, dataset, require_objects=False).manifest_sha256 != expected:
        raise SnapshotError('dedup candidate original head changed')
    summary, contents, plan_signature = _plan(index, dataset, expected, manifest)
    candidate_path = stage/'candidate.json'
    if os.path.lexists(candidate_path):
        return read_sealed(stage, dataset, expected)
    with ExitStack() as stack:
        checkpoint = s._Checkpoint(stage, dataset, expected)
        stack.callback(checkpoint.close)
        s._reset_unfinished(stage, (item['name'] for item in checkpoint.packages))
        def inventory_progress(event):
            guard()
            if progress is not None:
                progress({'stage': 'loading_source_inventory' if event['stage'] == 'load-packed-inventory'
                          else 'validating_source_inventory',
                          'items_completed': event['completed_items'], 'items_total': event['total_items']})
        original = stack.enter_context(p._open_inventory(cold, manifest, progress=inventory_progress))
        p._validate_inventory(manifest, original, retain_object_members=False, progress=inventory_progress)
        first, paths = {}, set()
        counts = {}
        for number, row in enumerate(original, 1):
            if row['kind'] != 'file':
                continue
            digest, size, path = row['sha256'], row['size'], row['path']
            expected_content = contents.get(digest)
            if expected_content is None or expected_content[0] != size:
                raise SnapshotError('dedup plan differs from fixed inventory content')
            counts[digest] = counts.get(digest, 0)+1
            if digest not in first:
                first[digest] = path
                if path != expected_content[1]:
                    raise SnapshotError('dedup plan representative differs from fixed inventory')
                paths.add(path)
            if number % 8192 == 0:
                guard()
                if progress is not None:
                    progress({'stage': 'checking_dedup_plan', 'items_completed': number,
                              'items_total': len(original)})
        if (len(first) != len(contents) or len(paths) != len(contents)
                or any(counts.get(digest) != row[2] for digest, row in contents.items())):
            raise SnapshotError('dedup plan occurrence counts differ from fixed inventory')
        saved = {raw.decode('utf-8', errors='surrogatepass'): (digest, size)
                 for raw, digest, size in checkpoint.db.execute('SELECT path,digest,size FROM members')}
        rep_sizes = {row[1]: row[0] for row in contents.values()}
        if any(path not in paths or size != rep_sizes[path] for path, (_pack, size) in saved.items()):
            raise SnapshotError('dedup checkpoint does not match planned representatives')
        def packing_progress(event):
            guard()
            if progress is not None:
                progress({**event, 'unique_contents_total': summary['unique_contents'],
                          'unique_bytes_total': summary['unique_content_logical_bytes']})
        builder = s._PackBuilder(stage, _NoOutput(), checkpoint=checkpoint, progress=packing_progress)
        try:
            from stockagent.data_sync import dedup_native_chunks
            chunked = dedup_native_chunks.stream(cold, manifest, original, selected_paths=paths,
                completed_paths=set(saved), consume=builder.consume, guard=guard, native=native)
            from stockagent.data_sync import dedup_native_packs
            packed = dedup_native_packs.stream(cold, manifest, original, selected_paths=paths,
                completed_paths=set(saved), consume=builder.consume, guard=guard, native=native,
                on_fallback=(lambda error: progress({'stage': 'selective_native_fallback',
                                                    'error_type': error}) if progress is not None else None))
            finished = set(saved) | chunked | packed
            if not finished.issubset(paths):
                raise SnapshotError('dedup selective readers returned an unknown representative')
            if finished != paths:
                s._stream_inputs(cold, manifest, original, builder.consume, native=native,
                                 completed=lambda path: path not in paths or path in finished)
            physical_packages = builder.close()
        except BaseException:
            builder.abort()
            raise
        if (builder.files != len(contents) or builder.bytes != summary['unique_content_logical_bytes']):
            raise SnapshotError('dedup candidate did not stream every unique representative')
        saved = {raw.decode('utf-8', errors='surrogatepass'): (digest, size)
                 for raw, digest, size in checkpoint.db.execute('SELECT path,digest,size FROM members')}
        if set(saved) != paths:
            raise SnapshotError('dedup candidate checkpoint is incomplete')
        counts_by_package = {item['sha256']: [0, 0] for item in physical_packages}
        output = stack.enter_context(MetadataRows(p._entry_record, p._entry_from_record,
                                                memory_rows=p.METADATA_MEMORY_ROWS))
        for number, row in enumerate(original, 1):
            entry = p._entry_from_record(row)
            if entry.kind == 'file':
                representative = contents[entry.sha256][1]
                package, size = saved[representative]
                if size != entry.size or package not in counts_by_package:
                    raise SnapshotError('dedup candidate pack mapping differs')
                entry.storage = {'kind': 'pack', 'member': representative, 'object_sha256': package}
                counts_by_package[package][0] += 1
                counts_by_package[package][1] += size
            output.append(entry)
            if number % 8192 == 0:
                guard()
                if progress is not None:
                    progress({'stage': 'writing_candidate_inventory', 'items_completed': number,
                              'items_total': len(original)})
        temporary = stage/('inventory-'+uuid.uuid4().hex+'.partial')
        def compression_progress(completed):
            guard()
            if progress is not None:
                progress({'stage': 'compressing_candidate_inventory', 'items_completed': completed,
                          'items_total': len(original)})
        inventory_sha = p._write_inventory(temporary, output, progress=compression_progress)
        inventory = stage/('inventory-'+inventory_sha+'.jsonl.gz')
        os.rename(temporary, inventory)
        p._fsync_directory(stage)
        packages = []
        for item in physical_packages:
            count, logical = counts_by_package[item['sha256']]
            packages.append({**item, 'stored_file_count': item['file_count'],
                             'stored_logical_bytes': item['logical_bytes'],
                             'file_count': count, 'logical_bytes': logical})
        if (sum(item['file_count'] for item in packages) != summary['files']
                or sum(item['logical_bytes'] for item in packages) != summary['logical_bytes']
                or not s._signature_matches(index, plan_signature)):
            raise SnapshotError('dedup candidate logical counts or plan changed')
        if p.resolve_latest_packed(cold, dataset, require_objects=False).manifest_sha256 != expected:
            raise SnapshotError('dedup candidate original head changed during preparation')
        body = {'contract': CONTRACT+'.candidate', 'dataset': dataset, 'old_manifest_sha256': expected,
                'old_inventory_sha256': summary['inventory_sha256'], 'packages': packages,
                'inventory': {'sha256': inventory_sha, 'name': inventory.name,
                              'bytes': inventory.stat().st_size, 'E_signature': s._signature(inventory)},
                'files': summary['files'], 'logical_bytes': summary['logical_bytes'],
                'unique_contents': summary['unique_contents'],
                'unique_content_logical_bytes': summary['unique_content_logical_bytes'],
                'unique_content_sha_streamed': True, 'all_original_occurrences_reread': False,
                'publishable': False, 'retirement_authorized': False,
                'observed_at_epoch': time.time()}
        body['identity_sha256'] = s._identity(body)
        atomic_write_json(candidate_path, body, durable=True)
        return body
