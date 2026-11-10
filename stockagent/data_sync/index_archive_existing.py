"""Reuse an already complete private D generation for one reviewed C handoff.

Only immutable inventory metadata is compared. No packed data payload is read
back, and no existing D head is modified by this operation.
"""
from __future__ import annotations

from contextlib import ExitStack
import gzip
import hashlib
import json
import os
from itertools import zip_longest
from pathlib import Path
import time

from stockagent.data_sync import index_generation_diff as diff, packed_snapshots as p
from stockagent.data_sync import stream_repack as s, workspace_relocation as relocation
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.packed_member_stream import _signature

CONTRACT = 'stockagent.index_archive_existing_match.v1'
REVIEWED = {'public-economic-history': ('data_public_economic',
    ('.download.lock', '.bea.lock', '.census.lock', '.moi.lock')),
    'news-fulltext-bodies-workspace': ('bodies', ('.download.lock',)),
    'tw-microstructure-train-workspace': ('hft_dataset', ('.download.lock',)),
    'tw-minute-train-workspace': ('research_dataset', ('.download.lock',)),
    'tw-minute-source-workspace': ('shioaji_1m', ('.download.lock',)),
    'tw-futures-universe725-workspace': ('tw_futures_universe725_20261025_v2', ('.download.lock',)),
    'tw-futures-universe765-workspace': ('tw_futures_universe765_20261027_v1', ('.download.lock',))}
# This source is already part of the original private parent dataset. Its
# snapshot must remain pinned until that dependency is explicitly replaced.
SUBTREE_COVERAGES = {'news-fulltext-bodies-workspace':
                     ('news-fulltext-local', 'data_news_fulltext', 'bodies/'),
                     'tw-microstructure-train-workspace': ('tw-microstructure-train', 'hft_dataset', ''),
                     'tw-minute-train-workspace': ('tw-minute-train', 'research_dataset', ''),
                     'tw-minute-source-workspace': ('tw-minute-source-cold', 'shioaji_1m', ''),
                     'tw-futures-universe725-workspace': ('tw-index-futures', 'data_tw_index_futures',
                         'training_prepared/tw_futures_universe725_20261025_v2/'),
                     'tw-futures-universe765-workspace': ('tw-index-futures', 'data_tw_index_futures',
                         'training_prepared/tw_futures_universe765_20261027_v1/')}
PUBLIC_COVERAGE_DATASETS = frozenset({'tw-microstructure-train-workspace', 'tw-minute-train-workspace',
                                      'tw-minute-source-workspace', 'tw-futures-universe725-workspace',
                                      'tw-futures-universe765-workspace'})


def _covering_root(cold, dataset):
    if dataset not in PUBLIC_COVERAGE_DATASETS:
        return cold
    public = cold.parent.parent
    if cold != public/'.local-state/private-history' or not (public/'.stockagent-d-primary').is_file():
        raise SnapshotError('public D covering authority differs')
    return public


def _materialized_root():
    repo = Path(__file__).resolve().parents[2]
    config = json.loads((repo/'configs/data_sync/packed_retention.json').read_bytes())
    root = Path(config['materialized_root'])
    if not root.is_absolute():
        raise SnapshotError('existing D coverage pin root is not absolute')
    return root


def _coverage_pin(proof, stage):
    dataset = proof['covering_dataset']
    digest, snapshot = proof['manifest_sha256'], proof['snapshot_id']
    root = _materialized_root()
    pin_format = proof.get('pin_format')
    if pin_format not in (None, 'stage-v1'):
        raise SnapshotError('existing D coverage pin format differs')
    path = root/'.stream-repack-pins'/dataset/s._repack_pin_name(digest, stage, scoped=pin_format == 'stage-v1')
    value = {'contract': s.CONTRACT+'.source-pin', 'dataset': dataset,
             'manifest': {'snapshot_id': snapshot}, 'manifest_sha256': digest, 'stage': str(stage)}
    s._check_repack_pin((path, value))
    return path


def receipt_matches(receipt, prepared_sha256) -> bool:
    if not isinstance(receipt, dict) or receipt.get('state') != 'existing_D_head_matches_source_inventory':
        return False
    proof = receipt.get('existing_match')
    dataset = receipt.get('dataset')
    scope = REVIEWED.get(dataset) if isinstance(dataset, str) else None
    coverage = SUBTREE_COVERAGES.get(dataset)
    valid = (isinstance(proof, dict) and scope is not None and proof.get('contract') == CONTRACT
            and proof.get('identity_sha256') == s._identity(proof)
            and proof.get('dataset') == receipt.get('dataset')
            and proof.get('source_identity') == receipt.get('old_manifest_sha256')
            and proof.get('prepared_sha256') == prepared_sha256
            and proof.get('snapshot_id') == receipt.get('new_snapshot_id')
            and proof.get('manifest_sha256') == receipt.get('new_manifest_sha256')
            and proof.get('root_name') == scope[0] and proof.get('excluded_coordination_paths') == list(scope[1])
            and proof.get('files') == receipt.get('files') and proof.get('logical_bytes') == receipt.get('logical_bytes')
            and proof.get('source_payload_read') is False and proof.get('independent_D_readback') is False
            and proof.get('NAS_accepted') is False
            and (coverage is None or (proof.get('covering_dataset') == coverage[0]
                 and proof.get('covering_root_name') == coverage[1]
                 and proof.get('coverage_prefix') == coverage[2]
                 and (dataset not in PUBLIC_COVERAGE_DATASETS or proof.get('covering_authority') == 'public')
                 and proof.get('covering_snapshot_pinned') is True)))
    if not valid:
        return False
    if coverage:
        try:
            _coverage_pin(proof, Path(proof['pin_stage']))
        except (OSError, KeyError, ValueError, TypeError, SnapshotError):
            return False
    return True


def _locked_head(cold: Path, dataset: str):
    root = cold/'heads'/dataset
    if root.is_symlink() or not root.is_dir():
        raise SnapshotError('existing private D head is unavailable')
    heads = list(root.glob('*.json'))
    node_id = p._resolve_node_id(cold)
    if len(heads) != 1 or heads[0].is_symlink() or heads[0].name != node_id+'.json':
        raise SnapshotError('existing private D head has a different owner')
    return p.resolve_latest_packed(cold, dataset, require_objects=True)


def _locks(cold: Path, dataset: str, install_lock, *, covering_dataset=None, covering_root=None):
    stack = ExitStack()
    try:
        covering_root = cold if covering_root is None else Path(covering_root)
        for owner in sorted({cold, covering_root}, key=str):
            stack.enter_context(p._try_publication_lock(owner/'.local-state/locks/publish-retention-global.lock', shared=True))
        scopes = {(cold, dataset)}
        if covering_dataset is not None:
            scopes.add((covering_root, covering_dataset))
        for owner, name in sorted(scopes, key=lambda item: (str(item[0]), item[1])):
            stack.enter_context(p._try_publication_lock(owner/f'.local-state/locks/publish-{name}.lock'))
        if install_lock is not None:
            stack.enter_context(p._try_publication_lock(Path(install_lock)))
        return stack
    except BaseException:
        stack.close()
        raise


def _scope(stage, cold, dataset, identity, root_name):
    cold, stage = Path(cold).absolute(), Path(stage).absolute()
    if (dataset not in REVIEWED or REVIEWED[dataset][0] != root_name
            or p._paths_overlap(cold, stage) or stage.is_symlink()
            or stage.resolve(strict=True) != stage):
        raise SnapshotError('existing private D reuse is outside its reviewed source scope')
    prepared = s._prepared_metadata(stage, dataset, identity)
    excluded = REVIEWED[dataset][1]
    expected_supplemental = dataset != 'tw-minute-train-workspace'
    if (prepared.get('source_mode') != 'retained_handoff_C_index'
            or tuple(prepared.get('preserved_paths', ())) != excluded
            or prepared.get('supplemental_root_lock') is not expected_supplemental):
        raise SnapshotError('existing private D reuse protected paths differ')
    return cold, stage, prepared, excluded


def _source_inventory(path: Path, expected_sha: str, expected_bytes: int,
                      excluded: tuple[str, ...], *, guard=lambda: None):
    """Stream the sealed compressed inventory with bounded memory.

    Verify its complete compressed SHA and inode before decoding, then fence
    the same open inode and pathname through the final comparison. No payload
    package is opened and no deletion occurs while this generator runs.
    """
    p._validate_hash(expected_sha, 'fixed E inventory SHA')
    if type(expected_bytes) is not int or not 0 < expected_bytes <= 2 * 1024**3:
        raise SnapshotError('fixed E inventory length is invalid')
    before = _signature(path)
    if before[3] != expected_bytes:
        raise SnapshotError('fixed E source inventory length differs')
    previous = ''
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as handle:
            opened = os.fstat(handle.fileno())
            signature = lambda st: (st.st_dev, st.st_ino, st.st_mode, st.st_size,
                                    st.st_mtime_ns, st.st_ctime_ns, st.st_nlink)
            if signature(opened) != before:
                raise SnapshotError('fixed E source inventory changed before opening')
            digest = hashlib.sha256()
            while block := handle.read(1024 * 1024):
                digest.update(block)
                guard()
            if digest.hexdigest() != expected_sha or signature(os.fstat(handle.fileno())) != before:
                raise SnapshotError('fixed E source inventory SHA or generation differs')
            handle.seek(0)
            with gzip.open(handle, 'rt', encoding='utf-8') as stream:
                for number, line in enumerate(stream, 1):
                    row = json.loads(line)
                    if not isinstance(row, dict):
                        raise SnapshotError('fixed E source inventory row is not an object')
                    name = p._safe_relative_path(row.get('path', ''), 'fixed E inventory path').as_posix()
                    if name <= previous:
                        raise SnapshotError('fixed E source inventory is not uniquely ordered')
                    previous = name
                    if number % 8192 == 0:
                        guard()
                    if name not in excluded:
                        yield row
            if signature(os.fstat(handle.fileno())) != before or _signature(path) != before:
                raise SnapshotError('fixed E source inventory changed during comparison')
    except (OSError, UnicodeDecodeError, ValueError, TypeError) as error:
        raise SnapshotError('fixed E source inventory is malformed') from error


def match(cold, stage, dataset, identity, index, root_name, *, guard=lambda: None, install_lock=None,
          progress=None):
    cold, stage, prepared, excluded = _scope(stage, cold, dataset, identity, root_name)
    s._read_prepared(stage, dataset, identity)
    index = Path(index)
    if not s._generation_matches(index, prepared.get('source_index_signature'), relocation.signature(index)):
        raise SnapshotError('fixed source index changed before existing D comparison')
    inventory = s._safe_stage_file(stage, prepared['inventory']['name'])
    coverage = SUBTREE_COVERAGES.get(dataset)
    covering_dataset = coverage[0] if coverage else dataset
    covering_root = _covering_root(cold, dataset)
    with _locks(cold, dataset, install_lock, covering_dataset=covering_dataset, covering_root=covering_root):
        guard()
        resolved = _locked_head(covering_root, covering_dataset)
        source = resolved.manifest.get('source', {})
        if (source.get('snapshot_root_name') != (coverage[1] if coverage else root_name)
                or (not coverage and set(source.get('excluded_subtrees', ())) != set(excluded))
                or source.get('omitted_files_above_maximum', 0) != 0
                or source.get('omitted_uncommitted_staging_files', 0) != 0):
            raise SnapshotError('existing private D head has a different source scope')
        def covered_rows():
            last_scan_report = time.monotonic()
            for scanned, row in enumerate(p._iter_inventory(covering_root, resolved.manifest), 1):
                if scanned % 8192 == 0:
                    # The parent can contain millions of names before this
                    # subtree. Keep the trading-window guard and observation
                    # alive even before the first matched child row.
                    guard()
                    if (coverage and not row['path'].startswith(coverage[2]) and progress is not None
                            and time.monotonic()-last_scan_report >= 2):
                        last_scan_report = time.monotonic()
                        progress({'stage':'scanning_D_parent_inventory', 'inventory_items_completed':scanned})
                if coverage:
                    if not row['path'].startswith(coverage[2]):
                        continue
                    row = {**row, 'path': row['path'][len(coverage[2]):]}
                    if row['path'] in excluded:
                        continue
                yield row
        matched = files = logical = 0
        source_rows = _source_inventory(inventory, prepared['inventory']['sha256'],
                                        prepared['inventory']['bytes'], excluded, guard=guard)
        last_progress = time.monotonic()
        for left, right in zip_longest(covered_rows(), source_rows):
            if left is None or right is None or left['path'] != right['path'] or not diff._same(left, right):
                raise SnapshotError('existing private D inventory does not exactly cover the E source')
            matched += 1
            if right.get('kind') == 'file':
                files += 1
                logical += right['size']
            if progress is not None and matched % 8192 == 0 and time.monotonic()-last_progress >= 2:
                last_progress = time.monotonic()
                progress({'stage': 'comparing_D_existing_inventory', 'inventory_items_completed': matched,
                          'files_completed': files, 'files_total': prepared['files'],
                          'logical_bytes_processed': logical, 'logical_bytes_total': prepared['logical_bytes']})
        if not coverage and (files != source.get('files') or logical != source.get('logical_bytes')):
            raise SnapshotError('existing private D source counts differ')
        if coverage:
            import sqlite3
            with sqlite3.connect(f'file:{index}?mode=ro', uri=True) as db:
                excluded_files = db.execute(
                    'SELECT count(*),coalesce(sum(bytes),0) FROM files WHERE kind=\'file\' AND path IN ('
                    + ','.join('?' for _ in excluded) + ')', excluded).fetchone()
            if files != prepared['files']-excluded_files[0] or logical != prepared['logical_bytes']-excluded_files[1]:
                raise SnapshotError('existing D subtree does not cover the sealed source counts')
            if not s._generation_matches(index, prepared.get('source_index_signature'), relocation.signature(index)):
                raise SnapshotError('fixed source index changed during existing D comparison')
        guard()
        proof = {'contract': CONTRACT, 'dataset': dataset, 'source_identity': identity,
            'prepared_sha256': prepared['identity_sha256'], 'snapshot_id': resolved.manifest['snapshot_id'],
            'manifest_sha256': resolved.manifest_sha256, 'root_name': root_name,
            'excluded_coordination_paths': list(excluded), 'matched_items': matched,
            'files': files, 'logical_bytes': logical, 'source_payload_read': False,
            'independent_D_readback': False, 'NAS_accepted': False}
        if coverage:
            # Multiple source subtrees may rely on the same immutable parent.
            # Each keeps its own pin without replacing another stage's proof.
            s._repack_pin(_materialized_root(), covering_dataset, resolved.manifest_sha256,
                          resolved.manifest['snapshot_id'], stage, scoped=True)
            proof.update(covering_dataset=covering_dataset, covering_root_name=coverage[1],
                         coverage_prefix=coverage[2], covering_snapshot_pinned=True, pin_stage=str(stage),
                         pin_format='stage-v1')
            if dataset in PUBLIC_COVERAGE_DATASETS:
                proof['covering_authority'] = 'public'
        proof['identity_sha256'] = s._identity(proof)
        atomic_write_json(stage/'existing-D-match.json', proof, durable=True)
        return proof


def cached_covering_match(cold, stage, dataset, identity, index, root_name, *, guard=lambda: None,
                          install_lock=None):
    """Reuse a previously completed comparison only while every binding holds.

    The original D/E metadata comparison and pin are still required. This
    check does not open payloads or claim a new independent readback.
    """
    if dataset not in SUBTREE_COVERAGES:
        return None
    cold, stage, prepared, _excluded = _scope(stage, cold, dataset, identity, root_name)
    path = stage/'existing-D-match.json'
    if not path.exists() and not path.is_symlink():
        return None
    from stockagent.data_sync.packed_member_stream import read_stable_bytes
    if path.is_symlink():
        raise SnapshotError('cached D coverage comparison is redirected')
    try:
        proof = json.loads(read_stable_bytes(path, maximum=128*1024))
    except (OSError, ValueError, TypeError) as error:
        raise SnapshotError('cached D coverage comparison is malformed') from error
    if not isinstance(proof, dict):
        raise SnapshotError('cached D coverage comparison is malformed')
    receipt = {'state':'existing_D_head_matches_source_inventory', 'dataset':dataset,
        'old_manifest_sha256':identity, 'new_snapshot_id':proof.get('snapshot_id'),
        'new_manifest_sha256':proof.get('manifest_sha256'), 'files':proof.get('files'),
        'logical_bytes':proof.get('logical_bytes'), 'existing_match':proof}
    if proof.get('pin_stage') != str(stage) or not receipt_matches(receipt, prepared.get('identity_sha256')):
        raise SnapshotError('cached D coverage comparison identity or pin differs')
    index = Path(index)
    covering_dataset = SUBTREE_COVERAGES[dataset][0]
    covering_root = _covering_root(cold, dataset)
    with _locks(cold, dataset, install_lock, covering_dataset=covering_dataset, covering_root=covering_root):
        guard()
        current_prepared = s._read_prepared(stage, dataset, identity)
        if (current_prepared.get('identity_sha256') != prepared.get('identity_sha256')
                or not s._generation_matches(index, prepared.get('source_index_signature'), relocation.signature(index))
                or not receipt_matches(receipt, prepared['identity_sha256'])):
            raise SnapshotError('cached D coverage source generation or pin changed')
        resolved = _locked_head(covering_root, covering_dataset)
        if (resolved.manifest_sha256 != proof.get('manifest_sha256')
                or resolved.manifest.get('snapshot_id') != proof.get('snapshot_id')):
            return None
        guard()
        return proof


def restore_covering_pin(cold, stage, dataset, identity, root_name, proof, *, guard=lambda: None, install_lock=None):
    """Restore a lost pin only while the same verified parent is still current."""
    cold, stage, prepared, excluded = _scope(stage, cold, dataset, identity, root_name)
    coverage = SUBTREE_COVERAGES.get(dataset)
    if (not coverage or not isinstance(proof, dict) or proof.get('contract') != CONTRACT
            or proof.get('identity_sha256') != s._identity(proof) or proof.get('dataset') != dataset
            or proof.get('source_identity') != identity or proof.get('prepared_sha256') != prepared['identity_sha256']
            or proof.get('root_name') != root_name or proof.get('excluded_coordination_paths') != list(excluded)
            or proof.get('covering_dataset') != coverage[0] or proof.get('covering_root_name') != coverage[1]
            or proof.get('coverage_prefix') != coverage[2] or proof.get('pin_stage') != str(stage)
            or (dataset in PUBLIC_COVERAGE_DATASETS and proof.get('covering_authority') != 'public')
            or proof.get('covering_snapshot_pinned') is not True):
        raise SnapshotError('existing D subtree pin proof differs')
    covering_root = _covering_root(cold, dataset)
    with _locks(cold, dataset, install_lock, covering_dataset=coverage[0], covering_root=covering_root):
        guard()
        resolved = _locked_head(covering_root, coverage[0])
        if (resolved.manifest_sha256 != proof['manifest_sha256']
                or resolved.manifest['snapshot_id'] != proof['snapshot_id']):
            raise SnapshotError('covering D head changed before pin recovery')
        pin_format = proof.get('pin_format')
        if pin_format not in (None, 'stage-v1'):
            raise SnapshotError('existing D subtree pin format differs')
        s._repack_pin(_materialized_root(), coverage[0], proof['manifest_sha256'], proof['snapshot_id'], stage,
                      scoped=pin_format == 'stage-v1')
        _coverage_pin(proof, stage)


def cleanup(cold, stage, dataset, identity, root_name, proof, *, guard=lambda: None, install_lock=None):
    cold, stage, prepared, excluded = _scope(stage, cold, dataset, identity, root_name)
    if not isinstance(proof, dict):
        raise SnapshotError('existing D match proof is missing')
    path = stage/'existing-D-match.json'
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 16384:
        raise SnapshotError('existing D match proof is unavailable')
    saved = json.loads(path.read_bytes())
    if (saved != proof or proof.get('contract') != CONTRACT or proof.get('identity_sha256') != s._identity(proof)
            or proof.get('dataset') != dataset or proof.get('source_identity') != identity
            or proof.get('prepared_sha256') != prepared['identity_sha256'] or proof.get('root_name') != root_name
            or proof.get('excluded_coordination_paths') != list(excluded)
            or proof.get('source_payload_read') is not False or proof.get('independent_D_readback') is not False):
        raise SnapshotError('existing D match proof differs')
    if os.path.lexists(stage/'temporary-authority.json'):
        raise SnapshotError('temporary E authority must be retired before stage cleanup')
    members = [*prepared['packages'], prepared['inventory']]
    if isinstance(prepared.get('resume_index'), dict):
        members.append(prepared['resume_index'])
    names = {row['name'] for row in members}
    journal_path = stage/'existing-D-cleanup.json'
    coverage = SUBTREE_COVERAGES.get(dataset)
    covering_dataset = coverage[0] if coverage else dataset
    if coverage and (proof.get('covering_dataset') != covering_dataset
            or proof.get('covering_root_name') != coverage[1] or proof.get('coverage_prefix') != coverage[2]
            or (dataset in PUBLIC_COVERAGE_DATASETS and proof.get('covering_authority') != 'public')
            or proof.get('covering_snapshot_pinned') is not True or proof.get('pin_stage') != str(stage)):
        raise SnapshotError('existing D subtree coverage proof differs')
    covering_root = _covering_root(cold, dataset)
    with _locks(cold, dataset, install_lock, covering_dataset=covering_dataset, covering_root=covering_root):
        guard()
        resolved = _locked_head(covering_root, covering_dataset)
        if (resolved.manifest_sha256 != proof['manifest_sha256']
                or resolved.manifest['snapshot_id'] != proof['snapshot_id']):
            raise SnapshotError('existing private D head changed before E cleanup')
        if coverage:
            _coverage_pin(proof, stage)
        if journal_path.exists() or journal_path.is_symlink():
            if journal_path.is_symlink() or not journal_path.is_file():
                raise SnapshotError('existing D cleanup journal is redirected')
            journal = json.loads(journal_path.read_bytes())
        else:
            journal = {'contract': CONTRACT+'.cleanup', 'proof_sha256': proof['identity_sha256'],
                       'state': 'removing', 'removed': []}
            atomic_write_json(journal_path, journal, durable=True)
        if (journal.get('contract') != CONTRACT+'.cleanup' or journal.get('proof_sha256') != proof['identity_sha256']
                or journal.get('state') not in {'removing', 'completed'} or not isinstance(journal.get('removed'), list)
                or len(set(journal['removed'])) != len(journal['removed'])
                or not set(journal['removed']).issubset(names) or journal.get('pending') not in (None, *names)):
            raise SnapshotError('existing D cleanup journal differs')
        for item in members:
            name, target = item['name'], stage/item['name']
            if name in journal['removed']:
                if os.path.lexists(target):
                    raise SnapshotError('removed E stage member unexpectedly returned')
                continue
            if not os.path.lexists(target):
                if journal.get('pending') != name:
                    raise SnapshotError('E stage member disappeared without cleanup intent')
            else:
                source = s._safe_stage_file(stage, name)
                if not s._signature_matches(source, item.get('E_signature')):
                    raise SnapshotError('E stage member changed before reuse cleanup')
                journal['pending'] = name
                atomic_write_json(journal_path, journal, durable=True)
                source.unlink()
                p._fsync_directory(stage)
            journal['removed'].append(name)
            journal.pop('pending', None)
            atomic_write_json(journal_path, journal, durable=True)
        journal.update(state='completed', completed_at_epoch=time.time())
        atomic_write_json(journal_path, journal, durable=True)
        return {'state': 'completed', 'files_removed': len(members),
                'bytes_removed': sum(item['bytes'] for item in members), 'independent_D_readback': False}
