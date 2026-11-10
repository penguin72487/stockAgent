"""Retire one superseded D encoding after a source-hashed streaming repack.

The user opted out of backup and independent payload readback for this
migration.  Shared objects, current heads, active readers and research pins
remain protected.  Each exact unlink is journalled for interruption recovery.
"""
from __future__ import annotations

from contextlib import ExitStack, closing, contextmanager
from concurrent.futures import ThreadPoolExecutor
import hashlib
import itertools
import json
import os
from pathlib import Path
import re
import stat
import time

from scripts import retire_staged_cold_objects as old_retirement
from stockagent.data_sync import packed_retention, packed_snapshots as p, stream_repack
from stockagent.data_sync.backup_stream import _private_catalog_generation
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.materialized_cache import process_references_many
from stockagent.data_sync.packed_backup import same_file_signature, signature

CONTRACT = 'stockagent.source_local_repack_retirement.v1'
MANIFEST_STAT_WORKERS = 8
MANIFEST_STAT_BATCH = 512


@contextmanager
def _boundary(cold: Path, extra_locks, *, attempts=1):
    """Retry briefly without ever retaining a partial set of owner locks."""
    last = 'publication'
    for attempt in range(attempts):
        stack = ExitStack()
        try:
            for lock in extra_locks:
                last = lock.name
                stack.enter_context(old_retirement._existing_lock(lock))
            # All acquisitions are nonblocking and any partial set is
            # discarded. Check the long-lived replication/backup owners
            # first so their busy retry loop does not repeatedly seize the
            # global publication inode and starve an unrelated E build.
            last = 'publication'
            stack.enter_context(p._try_publication_lock(cold/'.local-state/locks/publish-retention-global.lock'))
        except (BlockingIOError, p.PackedPublicationBusy):
            stack.close()
            if attempt+1 == attempts:
                raise SnapshotError('retirement owner busy: '+last) from None
            time.sleep(1)
            continue
        try:
            yield
        finally:
            stack.close()
        return


def _read_manifest(cold: Path, relative: str) -> dict:
    path = p._path_under(cold, relative, 'manifest relpath')
    before = signature(path)
    if path.is_symlink() or not path.is_file():
        raise SnapshotError('retirement manifest is missing or redirected')
    value = json.loads(path.read_bytes())
    if signature(path) != before:
        raise SnapshotError('retirement manifest changed during read')
    p._validate_manifest(value)
    if path.parent.name != value['dataset'] or path.stem != value['snapshot_id']:
        raise SnapshotError('retirement manifest path differs from identity')
    return value


def _refs(manifest: dict) -> set[str]:
    return {row['relpath'] for row in [manifest['archive']['inventory'], *manifest['archive']['objects']]}


def include_other_authority_repack_pins(cold: Path, materialized: Path, known: set[str]) -> set[str]:
    """Recognize exact public/private pins without weakening unknown-pin checks.

    Both authorities share the materialization pin namespace. A valid private
    in-flight build or training pin must not block an unrelated retirement just
    because the other authority's cached manifest census has not caught up.
    """
    known = set(known)
    public = cold.parent.parent if cold.name == 'private-history' and cold.parent.name == '.local-state' else cold
    roots = (public, public/'.local-state/private-history')
    pin_root = materialized/'.stream-repack-pins'
    if pin_root.exists() or pin_root.is_symlink():
        if pin_root.is_symlink() or not pin_root.is_dir() or pin_root.resolve(strict=True) != pin_root:
            raise SnapshotError('streaming pin namespace redirected')
        pins = list(pin_root.glob('*/*.pin.json'))
    else:
        pins = []
    if len(pins) > 4096:
        raise SnapshotError('streaming pin namespace exceeds bound')
    for pin in pins:
        try:
            if pin.is_symlink() or pin.parent.is_symlink() or not pin.is_file() or pin.stat().st_size > 16384:
                continue
            value = json.loads(pin.read_text())
            if not isinstance(value, dict) or value.get('contract') != stream_repack.CONTRACT+'.source-pin':
                continue
            manifest = value.get('manifest')
            if not isinstance(manifest, dict):
                continue
            dataset = p.validate_slug(value.get('dataset'), 'pin dataset')
            snapshot = p.validate_slug(manifest.get('snapshot_id'), 'pin snapshot')
            digest = p._validate_hash(value.get('manifest_sha256'), 'pin manifest SHA')
            stage = value.get('stage')
            unscoped_name = digest+'.pin.json'
            scoped_name = (stream_repack._repack_pin_name(digest, Path(stage), scoped=True)
                           if isinstance(stage, str) and stage else None)
            if (pin.parent.name != dataset or pin.name not in {unscoped_name, scoped_name}
                    or snapshot in known):
                continue
            for root in roots:
                relative = f'manifests/{dataset}/{snapshot}.json'
                candidate = root/relative
                if not candidate.exists() or candidate.is_symlink():
                    continue
                _regular(root, relative)
                resolved = p.resolve_packed_snapshot_id(root, dataset, snapshot, require_objects=False)
                if resolved.manifest_sha256 == digest:
                    known.add(snapshot)
                    break
        except (OSError, ValueError, TypeError, KeyError, SnapshotError):
            # The ordinary protection reader will reject a malformed or
            # unresolvable pin instead of treating it as absent.
            continue
    training = materialized/'.training-pins'
    if training.exists() or training.is_symlink():
        if training.is_symlink() or not training.is_dir() or training.resolve(strict=True) != training:
            raise SnapshotError('training pin namespace redirected')
        pins = list(training.glob('*/*.pin.json'))
        if len(pins) > 4096:
            raise SnapshotError('training pin namespace exceeds bound')
        for pin in pins:
            try:
                if pin.is_symlink() or pin.parent.is_symlink() or not pin.is_file() or pin.stat().st_size > p.MAX_PACKED_PIN_BYTES:
                    continue
                preview = json.loads(pin.read_bytes())
                manifest = preview.get('manifest') if isinstance(preview, dict) else None
                if not isinstance(manifest, dict):
                    continue
                dataset = p.validate_slug(manifest.get('dataset'), 'training pin dataset')
                snapshot = p.validate_slug(manifest.get('snapshot_id'), 'training pin snapshot')
                if pin.name != dataset+'.pin.json' or snapshot in known:
                    continue
                for root in roots:
                    candidate = root/'manifests'/dataset/(snapshot+'.json')
                    if not candidate.exists() or candidate.is_symlink():
                        continue
                    try:
                        _regular(root, candidate.relative_to(root).as_posix())
                        fixed = p.resolve_packed_pin(root, pin, expected_dataset=dataset)
                    except (OSError, ValueError, TypeError, KeyError, SnapshotError):
                        continue
                    if fixed.manifest['snapshot_id'] == snapshot:
                        known.add(snapshot)
                        break
            except (OSError, ValueError, TypeError, KeyError, SnapshotError):
                # The original protection reader still rejects an unresolved
                # active pin; never convert a bad pin into permission to delete.
                continue
    return known


CATALOG_TARGET_CACHE_CONTRACT = 'stockagent.retirement_catalog_target_references.v1'
MAX_TARGET_CACHE_BYTES = 64 * 1024**2


def cached_catalog_references(path: Path, catalog: Path, metadata: dict, targets: set[str],
                              generation, catalog_identity) -> tuple[dict, set] | None:
    """Use only an intact complete scan bound to this catalog and target set."""
    from stockagent.data_sync.packed_member_stream import read_stable_bytes
    try:
        if path.is_symlink() or not path.is_file():
            return None
        value = json.loads(read_stable_bytes(path, maximum=MAX_TARGET_CACHE_BYTES))
        references, seen_rows = value.get('references'), value.get('seen_manifests')
        if (value.get('contract') != CATALOG_TARGET_CACHE_CONTRACT
                or not old_retirement._same_catalog_generation(catalog, value.get('catalog_generation'), generation)
                or value.get('catalog_identity_sha256') != catalog_identity
                or value.get('identity_sha256') != stream_repack._identity(value)
                or not isinstance(references, dict) or not targets.issubset(references)
                or not isinstance(seen_rows, list) or len(seen_rows) != len(set(seen_rows))
                or any(not isinstance(owner, str) or owner not in metadata for owner in seen_rows)):
            return None
        seen = set(seen_rows)
        result = {}
        for target in targets:
            owners = references[target]
            if (not isinstance(owners, list) or len(owners) != len(set(owners))
                    or any(not isinstance(owner, str) or owner not in seen for owner in owners)):
                return None
            result[target] = set(owners)
        return result, seen
    except (OSError, ValueError, TypeError, KeyError, SnapshotError):
        return None


def build_catalog_reference_cache(catalog: Path, cold: Path, metadata: dict, targets: set[str],
                                  generation, catalog_identity, path: Path) -> dict:
    """Scan the original graph once for several exact pending retirements."""
    if not targets or len(targets) > 100000:
        raise SnapshotError('catalog target cache scope is empty or exceeds its bound')
    refs, seen = _scan_catalog_named_references(catalog, metadata, targets, generation)
    if sum(len(owners) for owners in refs.values()) > 1000000:
        raise SnapshotError('catalog target cache reference count exceeds its bound')
    value = {'contract': CATALOG_TARGET_CACHE_CONTRACT, 'catalog_generation': generation,
             'catalog_identity_sha256': catalog_identity,
             'references': {target: sorted(refs[target]) for target in sorted(targets)},
             'seen_manifests': sorted(seen),
             'observed_at_epoch': time.time()}
    value['identity_sha256'] = stream_repack._identity(value)
    encoded = json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
    if len(encoded) > MAX_TARGET_CACHE_BYTES:
        raise SnapshotError('catalog target cache exceeds its metadata bound')
    if _private_catalog_generation(catalog) != generation:
        raise SnapshotError('catalog generation changed before target cache commit')
    atomic_write_json(path, value, durable=True)
    if _private_catalog_generation(catalog) != generation:
        raise SnapshotError('catalog generation changed during target cache commit')
    return {'targets': len(targets), 'reference_occurrences': sum(len(owners) for owners in refs.values()),
            'cache_bytes': len(encoded)}


def _scan_catalog_named_references(catalog: Path, metadata: dict, targets: set[str], generation):
    import ijson  # Only the large-catalog scan needs the streaming parser.
    if _private_catalog_generation(catalog) != generation:
        raise SnapshotError('catalog generation changed before reference scan')
    references = {target: set() for target in targets}
    seen, current, relevant = set(), None, set()
    errors = excluded = 0
    with catalog.open('rb') as stream:
        for prefix, event, value in ijson.parse(stream):
            if prefix == 'releases.item' and event == 'start_map':
                current, relevant = None, set()
            elif prefix == 'releases.item.manifest_relative' and event == 'string':
                current = value
            elif prefix == 'releases.item.required_file_keys.item' and event == 'string':
                relative = value.rsplit('@', 1)[0]
                if relative in targets:
                    relevant.add(relative)
            elif prefix == 'releases.item' and event == 'end_map':
                if not isinstance(current, str) or current in seen or current not in metadata:
                    raise SnapshotError('catalog release graph has invalid manifest identities')
                seen.add(current)
                for relative in relevant:
                    references[relative].add(current)
            elif prefix == 'metadata_errors.item' and event in ('start_map', 'string'):
                errors += 1
            elif prefix == 'excluded_manifests.item' and event == 'string':
                excluded += 1
    if errors or excluded or _private_catalog_generation(catalog) != generation:
        raise SnapshotError('catalog graph is incomplete or changed')
    return references, seen


def catalog_references(catalog: Path, cold: Path, metadata: dict, targets: set[str], generation,
                       *, reference_cache: Path | None = None, catalog_identity=None) -> dict:
    """Use the fixed graph; always revisit omitted and subsequently changed manifests."""
    if _private_catalog_generation(catalog) != generation:
        raise SnapshotError('catalog generation changed before reference scan')
    cached = (cached_catalog_references(reference_cache, catalog, metadata, targets, generation, catalog_identity)
              if reference_cache is not None else None)
    if cached is None:
        references, seen = _scan_catalog_named_references(catalog, metadata, targets, generation)
    else:
        references, seen = cached
    if _private_catalog_generation(catalog) != generation:
        raise SnapshotError('catalog generation changed after reference scan')
    # The catalog intentionally excludes abandoned/test releases from its
    # retained graph. Their still-present manifests may nevertheless share an
    # object, so account for them rather than assuming they are disposable.
    for relative in set(metadata) - seen:
        path = cold/relative
        if not path.exists() and not path.is_symlink():
            continue  # A name already retired since the fixed catalog.
        if not same_file_signature(metadata[relative], signature(path)):
            continue  # A fresh final scan checks changed/new manifests.
        for ref in _refs(_read_manifest(cold, relative)) & targets:
            references[ref].add(relative)
    return references


def fresh_manifest_references(cold: Path, metadata: dict, targets: set[str], references: dict) -> set[str]:
    known, current_paths = set(), set()
    def inspect(entry):
        # One no-follow metadata lookup covers both regular-file validation
        # and the same five fields used by the existing signature contract.
        info = entry.stat(follow_symlinks=False)
        if not stat.S_ISREG(info.st_mode):
            raise SnapshotError('cold manifest is not a regular file')
        return Path(entry.path), (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)

    def manifest_entries():
        with os.scandir(cold/'manifests') as datasets:
            for dataset in datasets:
                if not dataset.is_dir(follow_symlinks=False):
                    raise SnapshotError('unexpected cold manifest namespace member')
                with os.scandir(dataset.path) as manifests:
                    for entry in manifests:
                        if entry.name.endswith('.json'):
                            yield entry

    # DrvFs metadata calls are independent but have significant per-call
    # latency. Keep the scan under the original publication lock, bound both
    # threads and memory, and merge results in the original directory order.
    with ThreadPoolExecutor(max_workers=MANIFEST_STAT_WORKERS, thread_name_prefix='cold-manifest') as pool, \
            closing(manifest_entries()) as entries:
        while batch := tuple(itertools.islice(entries, MANIFEST_STAT_BATCH)):
            for path, observed in pool.map(inspect, batch):
                relative = path.relative_to(cold).as_posix()
                current_paths.add(relative)
                known.add(path.stem)
                if same_file_signature(metadata.get(relative), observed):
                    continue
                for ref in _refs(_read_manifest(cold, relative)) & targets:
                    references[ref].add(relative)
    # Previously retired catalog versions no longer own objects. This census
    # runs under the publication lock, so their absent names are authoritative.
    for owners in references.values():
        owners.intersection_update(current_paths)
    return known


def _regular(cold: Path, relative: str, *, allow_hardlinks=False) -> Path:
    path = p._path_under(cold, relative, 'retirement member')
    if path.is_symlink() or not path.is_file():
        raise SnapshotError('retirement member is missing or redirected: '+relative)
    if path.stat().st_nlink != 1 and not allow_hardlinks:
        raise SnapshotError('retirement member has multiple hardlinks: '+relative)
    return path


def member_path(cold: Path, item: dict, *, missing_ok=False) -> Path:
    if 'placement' in item:
        from stockagent.data_sync.packed_object_placement import retirement_path
        return retirement_path(cold, item['placement'], missing_ok=missing_ok)[2]
    return cold/item['relative']


def plan_member(cold: Path, kind: str, relative: str) -> dict:
    """Bind local physical placement without weakening reference eligibility."""
    from stockagent.data_sync.packed_object_placement import retirement_record
    located = retirement_record(cold, relative) if kind == 'object' else None
    if located is not None:
        from stockagent.data_sync.packed_object_placement import retirement_path
        path = retirement_path(cold, located)[2]
    else:
        path = _regular(cold, relative, allow_hardlinks=kind == 'object')
    info = path.stat()
    return {'kind': kind, 'relative': relative, 'signature': stream_repack._signature(path),
            'bytes': info.st_size, 'allocated_bytes': info.st_blocks*512 if info.st_nlink == 1 else 0,
            **({'placement': located} if located is not None else {})}


def check_replacement(cold: Path, stage: Path, dataset: str, old_sha: str,
                      new_sha: str, new_id: str, prepared: dict):
    replacement = p.resolve_packed_snapshot_id(cold, dataset, new_id, require_objects=False)
    if (replacement.manifest_sha256 != new_sha
            or replacement.manifest.get('metadata', {}).get('physical_repack_original_manifest_sha256') != old_sha
            or replacement.manifest['source']['files'] != prepared['files']
            or replacement.manifest['source']['logical_bytes'] != prepared['logical_bytes']):
        raise SnapshotError('replacement release differs from source-hashed build')
    install = json.loads((stage/'install.json').read_text())
    installed = {row['sha256']: row for row in install.get('installed', [])}
    new_refs = [replacement.manifest['archive']['inventory'], *replacement.manifest['archive']['objects']]
    if (install.get('new_manifest_sha256') != new_sha
            or install.get('prepared_sha256') != prepared['identity_sha256']
            or set(installed) != {row['sha256'] for row in new_refs}):
        raise SnapshotError('replacement installation journal differs')
    for row in new_refs:
        # A completed cross-authority promotion may deliberately share this
        # immutable object by hardlink. Its exact post-link signature is
        # recorded by the sharing transaction in the original install journal.
        target = p._read_object_path(cold, row['relpath'])
        if target.is_symlink() or not target.is_file():
            raise SnapshotError('replacement placed object is missing or redirected')
        if not stream_repack._signature_matches(target, installed[row['sha256']]['D_signature']):
            raise SnapshotError('replacement object generation changed')
    return replacement


def _histories_many(cold: Path, dataset: str, versions) -> list[str]:
    """Find all selected old heads in one namespace scan, not one per version."""
    pairs = list(versions)
    if not pairs:
        return []
    by_sha, by_snapshot = {}, {}
    for digest, snapshot in pairs:
        p._validate_hash(digest, 'historical manifest SHA')
        p.validate_slug(snapshot, 'historical snapshot')
        if ((digest in by_sha and by_sha[digest] != snapshot)
                or (snapshot in by_snapshot and by_snapshot[snapshot] != digest)):
            raise SnapshotError('historical retirement identities conflict')
        by_sha[digest], by_snapshot[snapshot] = snapshot, digest
    root = cold/'head-history/heads'/dataset
    if not root.exists():
        return []
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('historical head root redirected')
    matched = []
    for path in root.glob('*/*.json'):
        if path.is_symlink() or path.parent.is_symlink() or not path.is_file():
            raise SnapshotError('historical head is redirected')
        raw = path.read_bytes()
        head = json.loads(raw)
        digest, snapshot = head.get('manifest_sha256'), head.get('snapshot_id')
        if ((isinstance(digest, str) and digest in by_sha)
                or (isinstance(snapshot, str) and snapshot in by_snapshot)):
            if (not isinstance(digest, str) or not isinstance(snapshot, str)
                    or head.get('dataset') != dataset or by_sha.get(digest) != snapshot
                    or by_snapshot.get(snapshot) != digest or hashlib.sha256(raw).hexdigest() != path.stem):
                raise SnapshotError('historical head identity differs')
            matched.append(path.relative_to(cold).as_posix())
    return sorted(matched)


def _histories(cold: Path, dataset: str, old_sha: str, snapshot: str) -> list[str]:
    return _histories_many(cold, dataset, [(old_sha, snapshot)])


def _require_not_current_head(cold: Path, dataset: str, old_sha: str, old_id: str) -> None:
    root = cold/'heads'/dataset
    if root.is_symlink() or not root.is_dir():
        raise SnapshotError('current head namespace is missing or redirected')
    heads = list(root.glob('*.json'))
    if not heads:
        raise SnapshotError('current head namespace is empty')
    for path in heads:
        head = json.loads(_regular(cold, path.relative_to(cold).as_posix()).read_text())
        if head.get('dataset') != dataset:
            raise SnapshotError('current head dataset identity differs')
        if head.get('manifest_sha256') == old_sha or head.get('snapshot_id') == old_id:
            raise SnapshotError('old release is still a current head')


def load_journal(path: Path) -> dict:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise SnapshotError('retirement journal is not an object')
    if 'plan' not in value:
        name = value.get('plan_ref')
        if not isinstance(name, str) or not re.fullmatch(r'retirement-plan-[a-f0-9]{64}\.json', name):
            raise SnapshotError('retirement plan reference is invalid')
        plan_path = path.parent/name
        if plan_path.is_symlink() or not plan_path.is_file():
            raise SnapshotError('retirement plan sidecar is missing or redirected')
        plan = json.loads(plan_path.read_text())
        if not isinstance(plan, dict) or stream_repack._identity(plan) != value.get('plan_sha256'):
            raise SnapshotError('retirement plan sidecar identity differs')
        value['plan'] = plan
        count = value.get('removed_count')
        if type(count) is not int or count < 0 or count > len(plan['items']):
            raise SnapshotError('retirement journal cursor differs')
        value['removed'] = [item['relative'] for item in plan['items'][:count]]
    return value


def save_journal(path: Path, journal: dict):
    """Write the large immutable plan once; update only a small progress cursor."""
    plan = journal['plan']
    digest = journal['plan_sha256']
    name = 'retirement-plan-'+digest+'.json'
    plan_path = path.parent/name
    if not journal.get('_plan_prepared'):
        if stream_repack._identity(plan) != digest:
            raise SnapshotError('retirement plan identity changed')
        if not os.path.lexists(plan_path):
            atomic_write_json(plan_path, plan, durable=True)
        elif (plan_path.is_symlink() or not plan_path.is_file()
              or stream_repack._identity(json.loads(plan_path.read_text())) != digest):
            raise SnapshotError('retirement plan sidecar changed or redirected')
        journal['_plan_prepared'] = True
    count = len(journal['removed'])
    if count > len(plan['items']) or (count and journal['removed'][-1] != plan['items'][count-1]['relative']):
        raise SnapshotError('retirement cursor is not an exact plan prefix')
    body = {key: value for key, value in journal.items() if key not in ('plan', 'removed', '_plan_prepared')}
    body.update(plan_ref=name, removed_count=len(journal['removed']))
    atomic_write_json(path, body, durable=True)


def journalled_unlink(cold: Path, journal_path: Path, journal: dict, *, batch_size=64, guard=lambda: None,
                      parallel_objects=1):
    """Durably intent bounded exact batches; recover a partly unlinked batch."""
    items = journal['plan']['items']
    names = [item['relative'] for item in items]
    removed = journal.get('removed')
    pending = journal.get('pending')
    pending = [pending] if isinstance(pending, str) else pending
    if (type(parallel_objects) is not int or not 1 <= parallel_objects <= 16
            or not isinstance(removed, list) or len(set(names)) != len(names)
            or len(set(removed)) != len(removed) or not set(removed).issubset(names)
            or (pending is not None and (not isinstance(pending, list) or not pending
                 or len(pending) > batch_size or len(set(pending)) != len(pending)))):
        raise SnapshotError('retirement journal progress differs from its plan')
    removed_set = set(removed)
    remaining = [item for item in items if item['relative'] not in removed_set]
    if pending is not None and pending != [item['relative'] for item in remaining[:len(pending)]]:
        raise SnapshotError('retirement pending batch is not the next exact sequence')
    journal['state'] = 'removing'
    save_journal(journal_path, journal)
    while remaining:
        guard()
        existing_intent = pending is not None
        batch = remaining[:len(pending)] if existing_intent else remaining[:batch_size]
        def check(item):
            path = (member_path(cold, item) if 'placement' in item else
                    _regular(cold, item['relative'], allow_hardlinks=item.get('kind') == 'object'))
            if not stream_repack._signature_matches(path, item['signature']):
                raise SnapshotError('retirement member changed before batch intent')

        def remove(item):
            path = member_path(cold, item, missing_ok=True)
            if os.path.lexists(path):
                if 'placement' not in item:
                    path = _regular(cold, item['relative'], allow_hardlinks=item.get('kind') == 'object')
                if not stream_repack._signature_matches(path, item['signature']):
                    raise SnapshotError('retirement member changed before unlink')
                try:
                    path.unlink()
                except PermissionError:
                    if item.get('kind') != 'object':
                        raise
                    return item['relative']
            if 'placement' in item:
                from stockagent.data_sync.packed_object_placement import retirement_path
                locations, record, _path = retirement_path(cold, item['placement'], missing_ok=True)
                locations.mark_retired(record, journal['plan_sha256'])
            return None

        # Only object-only batches may run out of order. Histories and
        # manifests remain serial and cannot be skipped ahead of objects.
        parallel = parallel_objects > 1 and all(item.get('kind') == 'object' for item in batch)
        if parallel:
            with ThreadPoolExecutor(max_workers=parallel_objects, thread_name_prefix='retire-object') as pool:
                if not existing_intent:
                    list(pool.map(check, batch))
                    pending = [item['relative'] for item in batch]
                    journal['pending'] = pending
                    save_journal(journal_path, journal)
                busy_objects = [value for value in pool.map(remove, batch) if value is not None]
        else:
            if not existing_intent:
                for item in batch:
                    check(item)
                pending = [item['relative'] for item in batch]
                journal['pending'] = pending
                save_journal(journal_path, journal)
            busy_objects = [value for item in batch if (value := remove(item)) is not None]
        for parent in sorted({member_path(cold, item, missing_ok=True).parent for item in batch}):
            p._fsync_directory(parent)
        if busy_objects:
            # Keep the whole durable pending batch. A retry treats successfully
            # unlinked names as absent and rechecks each remaining generation.
            raise SnapshotError(f'{len(busy_objects)} retirement object(s) temporarily unavailable')
        journal['removed'].extend(pending)
        journal.pop('pending', None)
        save_journal(journal_path, journal)
        remaining = remaining[len(batch):]
        pending = None
    # DrvFs change notifications can miss retired names. Queue their precise
    # paths through the existing Syncthing owner before this transaction may
    # be marked complete. A retry requeues an interrupted final notification;
    # a scan acknowledgement is not peer delivery or restore evidence.
    from stockagent.data_sync import syncthing_scan
    if (cold.absolute() == syncthing_scan.CANONICAL_ROOT
            and (cold/syncthing_scan.D_PRIMARY_MARKER).is_file() and journal['removed']):
        syncthing_scan.queue_after_publish(cold, journal['dataset'],
                                           new_object_paths=tuple(journal['removed']))
    return journal


def retire(cold: Path, stage: Path, receipt: dict, *, catalog: Path, observation: Path,
           cache: Path, materialized: Path, retention_state: Path,
           extra_locks=(), guard=lambda: None, apply=False) -> dict:
    """Dry-run or journal and remove one exact old encoding's unused D names."""
    cold, stage = cold.absolute(), stage.absolute()
    dataset = p.validate_slug(receipt.get('dataset'), 'dataset')
    old_sha = p._validate_hash(receipt.get('old_manifest_sha256'), 'old SHA')
    new_sha = p._validate_hash(receipt.get('new_manifest_sha256'), 'new SHA')
    new_id = p.validate_slug(receipt.get('new_snapshot_id'), 'new snapshot')
    if (receipt.get('contract') != 'stockagent.source_local_exact_repack.v1'
            or receipt.get('state') != 'new_D_head_committed_from_exact_E_source'
            or receipt.get('source_mode') != 'D_streamed_to_E_dataset_pack'
            or stage.is_symlink() or stage.resolve(strict=True) != stage):
        raise SnapshotError('retirement receipt or stage is not an enrolled streaming repack')
    prepared = stream_repack._prepared_metadata(stage, dataset, old_sha)
    old_id = p.validate_slug(prepared['old_snapshot_id'], 'old snapshot')
    old_relative = f'manifests/{dataset}/{old_id}.json'
    journal_path = stage/'retirement.json'
    if journal_path.is_symlink():
        raise SnapshotError('retirement journal redirected')
    journal = load_journal(journal_path) if journal_path.is_file() else None
    if journal is not None:
        if (not isinstance(journal, dict) or not isinstance(journal.get('plan'), dict)
                or journal.get('contract') != CONTRACT or journal.get('dataset') != dataset
                or journal.get('old_manifest_sha256') != old_sha or journal.get('new_manifest_sha256') != new_sha
                or journal.get('prepared_sha256') != prepared['identity_sha256']
                or journal.get('plan_sha256') != stream_repack._identity(journal['plan'])
                or not isinstance(journal.get('removed'), list)):
            raise SnapshotError('retirement journal identity differs')
        items = journal['plan'].get('items')
        if (not isinstance(items, list) or not all(isinstance(row, dict) and isinstance(row.get('relative'), str)
                                                  for row in items)):
            raise SnapshotError('retirement journal members are invalid')
        names = [row['relative'] for row in items]
        if (len(set(names)) != len(names) or len(set(journal['removed'])) != len(journal['removed'])
                or not set(journal['removed']).issubset(names)
                or (journal.get('pending') is not None and not isinstance(journal.get('pending'), (str, list)))
                or journal.get('state') not in ('planned', 'removing', 'completed')):
            raise SnapshotError('retirement journal progress differs from its plan')
        if journal.get('state') == 'completed':
            return journal
        old_refs = set(journal['plan']['old_refs'])
    else:
        old = p.resolve_packed_snapshot_id(cold, dataset, old_id, require_objects=False)
        if old.manifest_sha256 != old_sha:
            raise SnapshotError('old release manifest differs from source-hashed build')
        old_refs = _refs(old.manifest)
    # Do not make every queued candidate wait a minute for a publisher that
    # already holds the shared owner. Yield immediately so unrelated private
    # work can run; the scheduler retries this exact candidate. The final
    # boundary below still reacquires all owners before any journal or unlink.
    with _boundary(cold, extra_locks, attempts=1):
        guard()
        # Another publisher may still own the old head. Check that cheap
        # blocker before reading the large catalog, then repeat at unlink.
        _require_not_current_head(cold, dataset, old_sha, old_id)
        # This is only an early *negative* decision: a pin that names this
        # exact old version lets us yield without a full reference census.
        # Unrelated or unreadable pin metadata can never authorize removal;
        # the complete protection/error check below remains mandatory.
        early_protected, _, _ = packed_retention._local_protected_snapshot_ids(
            materialized, {old_id}, research_pin_root=retention_state/'research-pins',
            research_pin_marker=retention_state/packed_retention.RESEARCH_PIN_MARKER)
        if old_id in early_protected:
            raise SnapshotError('old release is pinned')
    metadata, _, catalog_identity = old_retirement.catalog_index(catalog, observation, set(), cache_path=cache)
    generation = _private_catalog_generation(catalog)
    references = catalog_references(catalog, cold, metadata, old_refs, generation,
        reference_cache=cache.parent/'catalog-target-reference-cache-v1.json', catalog_identity=catalog_identity)
    with _boundary(cold, extra_locks, attempts=60):
        guard()
        if _private_catalog_generation(catalog) != generation:
            raise SnapshotError('catalog changed during retirement')
        known = fresh_manifest_references(cold, metadata, old_refs, references)
        check_replacement(cold, stage, dataset, old_sha, new_sha, new_id, prepared)
        _require_not_current_head(cold, dataset, old_sha, old_id)
        known.update((old_id, new_id))
        known = include_other_authority_repack_pins(cold, materialized, known)
        protected, _, errors = packed_retention._local_protected_snapshot_ids(materialized, known,
            research_pin_root=retention_state/'research-pins',
            research_pin_marker=retention_state/packed_retention.RESEARCH_PIN_MARKER)
        if errors or old_id in protected:
            raise SnapshotError('old release is pinned or local protection metadata is unknown')
        histories = _histories(cold, dataset, old_sha, old_id)
        deletable = sorted(ref for ref, owners in references.items() if not (owners-{old_relative}))
        if journal is None:
            # A user-directed E-first cutover may have already journalled
            # deletion of exact exclusive old objects to make room for this
            # now-committed D replacement. Preserve that proof and never
            # mistake any other missing object for an authorized removal.
            from stockagent.data_sync import staged_head_capacity_relief
            prior = staged_head_capacity_relief.load(stage, dataset, old_sha, prepared)
            pre_retired = set()
            if prior is not None:
                if prior['state'] != 'completed':
                    raise SnapshotError('current-head relief is not completed')
                pre_retired = set(prior['removed'])
                if (not pre_retired.issubset(old_refs)
                        or set(prior['plan'].get('old_refs', [])) != old_refs):
                    raise SnapshotError('current-head relief references differ from old release')
                if any(not os.path.lexists(cold/ref) and references.get(ref, set())-{old_relative}
                       for ref in pre_retired):
                    raise SnapshotError('a removed old object gained another manifest reference')
            items = []
            already_removed = []
            for kind, relative in ([('history', rel) for rel in histories]
                                   + [('manifest', old_relative)] + [('object', rel) for rel in deletable]):
                if kind == 'object' and not os.path.lexists(cold/relative) and relative in pre_retired:
                    already_removed.append(relative)
                    continue
                items.append(plan_member(cold, kind, relative))
            plan = {'old_snapshot_id': old_id, 'new_snapshot_id': new_id, 'old_refs': sorted(old_refs),
                    'items': items, 'already_removed_by_E_first_relief': already_removed,
                    'catalog_identity_sha256': catalog_identity}
            journal = {'contract': CONTRACT, 'dataset': dataset, 'old_manifest_sha256': old_sha,
                       'new_manifest_sha256': new_sha, 'prepared_sha256': prepared['identity_sha256'],
                       'plan': plan, 'plan_sha256': stream_repack._identity(plan), 'removed': [],
                       'state': 'planned', 'independent_D_readback': False, 'NAS_accepted': False}
        else:
            plan = journal['plan']
            planned_objects = {item['relative'] for item in plan['items'] if item['kind'] == 'object'}
            planned_histories = {item['relative'] for item in plan['items'] if item['kind'] == 'history'}
            if not planned_objects.issubset(deletable) or not set(histories).issubset(planned_histories):
                raise SnapshotError('references changed during retirement retry')
        if process_references_many([member_path(cold, item, missing_ok=True) for item in plan['items']]):
            raise SnapshotError('old release is used by a running process')
        if not apply:
            return {**journal, 'apply': False, 'reclaimable_bytes': sum(i['bytes'] for i in plan['items']
                                                                      if i['kind'] == 'object')}
        for relative in journal['removed']:
            if os.path.lexists(cold/relative):
                raise SnapshotError('retired name unexpectedly returned')
        journalled_unlink(cold, journal_path, journal)
        journal.update(state='completed', completed_at_epoch=time.time(),
            deleted_object_bytes=sum(i['bytes'] for i in plan['items'] if i['kind'] == 'object' and 'placement' not in i),
            estimated_allocated_bytes_reclaimed=sum(i.get('allocated_bytes', 0) for i in plan['items'] if i['kind'] == 'object' and 'placement' not in i),
            deleted_object_count=sum(i['kind'] == 'object' and 'placement' not in i for i in plan['items']),
            estimated_E_allocated_bytes_reclaimed=sum(i.get('allocated_bytes', 0) for i in plan['items'] if 'placement' in i))
        save_journal(journal_path, journal)
        return journal
