"""Route a fixed, duplicate-heavy source to the existing E dedup owner.

The census reads only the original packed inventory and writes a disposable
SQLite plan.  It neither reads payloads nor changes a D head.  An auto hold is
an exact scheduling decision, not a recovery proof or deletion authorization.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import time

from stockagent.data_sync import packed_dedup_plan, packed_snapshots as p, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

CONTRACT = 'stockagent.source_repack_auto_dedup_hold.v1'
MIN_DUPLICATE_BYTES = 128 * 1024**2
# Logical occurrences and unique contents are different cardinalities. A
# dataset with millions of repeated paths must not skip a bounded unique-SHA
# census just because its path count exceeds the unique-content memory limit.
MAX_FILES = 50_000_000


def _identity(value: dict) -> str:
    body = {key: item for key, item in value.items() if key != 'identity_sha256'}
    return hashlib.sha256(p._canonical_json_bytes(body)).hexdigest()


def _directory(workspace: Path, relative: str) -> Path:
    root = Path(workspace).absolute()
    if root.is_symlink() or root.resolve(strict=True) != root or not root.is_dir():
        raise SnapshotError('auto dedup workspace is redirected')
    path = Path(relative)
    if path.is_absolute() or '..' in path.parts:
        raise SnapshotError('auto dedup metadata directory is outside the workspace')
    directory = root
    for part in path.parts:
        directory = directory/part
        if directory.is_symlink():
            raise SnapshotError('auto dedup metadata directory is redirected')
        directory.mkdir(mode=0o700, exist_ok=True)
        if directory.resolve(strict=True) != directory or not directory.is_dir():
            raise SnapshotError('auto dedup metadata directory is redirected')
    return directory


def ensure_stream_intent(workspace: Path, dataset: str, digest: str) -> Path:
    """Resume the tiny metadata step if interrupted after writing the hold."""
    dataset, digest = p.validate_slug(dataset, 'dataset'), p._validate_hash(digest, 'original manifest SHA')
    parent = _directory(workspace, 'migration/stream-source-repack-v1/'+dataset)
    stage = parent/digest
    if stage.is_symlink():
        raise SnapshotError('auto dedup stream stage is redirected')
    stage.mkdir(mode=0o700, exist_ok=True)
    if stage.resolve(strict=True) != stage or stat.S_IMODE(stage.stat().st_mode) != 0o700:
        raise SnapshotError('auto dedup stream stage is not private')
    path = stage/'intent.json'
    intent = {'contract': stream_repack.CONTRACT+'.intent', 'dataset': dataset, 'old_manifest_sha256': digest}
    if os.path.lexists(path):
        if path.is_symlink() or not path.is_file() or json.loads(path.read_text()) != intent:
            raise SnapshotError('auto dedup stream intent differs')
    else:
        if any(stage.iterdir()):
            raise SnapshotError('auto dedup stream stage is not empty')
        atomic_write_json(path, intent, durable=True)
    return stage


def _summary(index: Path, dataset: str, digest: str, manifest: dict) -> dict:
    if (index.is_symlink() or not index.is_file() or index.resolve(strict=True) != index
            or not stat.S_ISREG(index.lstat().st_mode) or index.stat().st_nlink != 1):
        raise SnapshotError('auto dedup plan is redirected')
    with sqlite3.connect(index.as_uri()+'?mode=ro', uri=True, timeout=0) as db:
        row = db.execute("SELECT value FROM meta WHERE key='summary'").fetchone()
        counts = db.execute('SELECT COUNT(*),COALESCE(SUM(size),0),COALESCE(SUM(occurrences),0),'
                            'COALESCE(SUM(size*occurrences),0) FROM contents').fetchone()
    if row is None or not isinstance(row[0], str) or len(row[0]) > 1024**2:
        raise SnapshotError('auto dedup plan summary is missing')
    value = json.loads(row[0])
    if (not isinstance(value, dict) or value.get('contract') != packed_dedup_plan.CONTRACT
            or value.get('dataset') != dataset or value.get('old_manifest_sha256') != digest
            or value.get('inventory_sha256') != manifest['archive']['inventory']['sha256']
            or value.get('files') != manifest['source']['files']
            or value.get('logical_bytes') != manifest['source']['logical_bytes']
            or counts != (value.get('unique_contents'), value.get('unique_content_logical_bytes'),
                          value.get('files'), value.get('logical_bytes'))
            or type(value.get('duplicate_logical_bytes')) is not int
            or value['duplicate_logical_bytes'] != value['logical_bytes']-value['unique_content_logical_bytes']
            or value.get('inventory_metadata_validated') is not True
            or value.get('source_payload_reread') is not False):
        raise SnapshotError('auto dedup plan does not match its fixed source')
    return value


def _census(plans: Path, cold: Path, dataset: str, digest: str, manifest: dict, *, guard) -> dict | None:
    """Observe a bounded census and avoid repeating a proven memory-limit miss."""
    index = plans/(dataset+'-'+digest+'.sqlite')
    status = plans/(dataset+'-'+digest+'.status.json')
    fields = Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()
    binding = {'contract': CONTRACT+'.census-v1', 'dataset': dataset,
               'old_manifest_sha256': digest,
               'inventory_sha256': manifest['archive']['inventory']['sha256'],
               'max_unique_contents': packed_dedup_plan.MAX_UNIQUE_CONTENTS,
               'planner_sha256': hashlib.sha256(Path(packed_dedup_plan.__file__).read_bytes()).hexdigest(),
               'source_payload_reread': False, 'D_head_changed': False, 'retirement_authorized': False}
    if status.is_symlink():
        raise SnapshotError('dedup census observation is redirected')
    if status.is_file() and status.stat().st_size <= 16384:
        prior = json.loads(status.read_text())
        if (isinstance(prior, dict) and prior.get('state') == 'unique_limit'
                and all(prior.get(key) == value for key, value in binding.items())):
            return None
    owner = {'pid': os.getpid(), 'start_ticks': int(fields[19]),
             'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}
    def report(event, *, state='running'):
        guard()
        atomic_write_json(status, {**binding, 'state': state, 'owner_identity': owner,
            'observed_at_epoch': time.time(), 'progress': event}, durable=True)
    if not os.path.lexists(index):
        report({'files_completed': 0, 'files_total': manifest['source']['files']})
        try:
            packed_dedup_plan.build_plan(cold, dataset, digest, plans, guard=guard,
                progress=report, max_unique=packed_dedup_plan.MAX_UNIQUE_CONTENTS)
        except SnapshotError as error:
            if str(error) != 'content census unique-item memory bound reached':
                try:
                    report({'error_type': type(error).__name__}, state='failed')
                except (OSError, ValueError, SnapshotError):
                    pass
                raise
            report({'files_total': manifest['source']['files']}, state='unique_limit')
            return None
        except FileExistsError:
            pass  # A same-identity planner committed first; validate below.
        except Exception as error:
            try:
                report({'error_type': type(error).__name__}, state='failed')
            except (OSError, ValueError, SnapshotError):
                pass
            raise
    summary = _summary(index, dataset, digest, manifest)
    report({key: summary[key] for key in ('files', 'logical_bytes', 'unique_contents',
                                         'unique_content_logical_bytes', 'duplicate_logical_bytes')}, state='ready')
    return summary


def preflight(workspace: Path, cold: Path, dataset: str, digest: str, manifest: dict, *,
              guard=lambda: None, admit=lambda _bytes: None, private=False,
              allow_sealed=False) -> dict | None:
    """Enroll repeated content, including an unchanged capacity-blocked E build.

    An already sealed normal encoding may be superseded only before any D
    install or temporary-authority transaction begins. Its exact preparation
    remains bound to the hold and is preserved by the later stage handoff.
    This function itself neither replaces nor removes that encoding.
    """
    workspace, cold = Path(workspace).absolute(), Path(cold).absolute()
    dataset, digest = p.validate_slug(dataset, 'dataset'), p._validate_hash(digest, 'original manifest SHA')
    if (type(private) is not bool or type(allow_sealed) is not bool
            or p._paths_overlap(workspace, cold) or manifest.get('dataset') != dataset
            or private and (cold.name != 'private-history' or cold.parent.name != '.local-state'
                            or not stream_repack.private_repack_source_allowed(dataset, manifest))):
        raise SnapshotError('auto dedup source scope differs')
    source, archive = manifest['source'], manifest['archive']
    if archive.get('format') == p.ALIAS_ARCHIVE_FORMAT:
        return None  # It already represents equal contents by aliases.
    files, logical = source['files'], source['logical_bytes']
    if type(files) is not int or type(logical) is not int or not 0 < files <= MAX_FILES or logical < 0:
        return None
    encoded = sum(item['bytes'] for item in archive['objects']) + archive['inventory']['bytes']
    # A fixed inventory already encoded much less than its logical size can
    # contain shared content. Check only plausible material savings; the plan
    # then establishes actual repeated SHA/size entries before holding work.
    if logical < encoded + MIN_DUPLICATE_BYTES or logical*20 < encoded*21:
        return None
    stage = workspace/'migration/stream-source-repack-v1'/dataset/digest
    sealed_sha = None
    if os.path.lexists(stage):
        if not allow_sealed:
            return None  # Never redirect an already-started normal transaction.
        prepared = stream_repack._read_prepared(stage, dataset, digest)
        if (prepared.get('packed_format') is not None
                or prepared.get('files') != files or prepared.get('logical_bytes') != logical
                or prepared.get('old_inventory_sha256', archive['inventory']['sha256']) != archive['inventory']['sha256']
                or any(os.path.lexists(stage/name) for name in
                       ('install.json', 'temporary-authority.json', 'current-head-capacity-relief.json'))):
            return None
        # Avoid another encoding when the sealed build is already compact.
        new_encoded = sum(item['bytes'] for item in prepared['packages']) + prepared['inventory']['bytes']
        if new_encoded <= encoded + max(MIN_DUPLICATE_BYTES, encoded//4):
            return None
        sealed_sha = prepared['identity_sha256']
    elif allow_sealed:
        return None
    admit(min(1024**3, 64*1024**2 + files*1024))
    plans = _directory(workspace, 'migration/source-local-exact-repack/dedup-plans')
    guard()
    summary = _census(plans, cold, dataset, digest, manifest, guard=guard)
    if summary is None:
        return None
    duplicate = summary['duplicate_logical_bytes']
    if duplicate < max(MIN_DUPLICATE_BYTES, logical//10):
        return None
    if p.resolve_latest_packed(cold, dataset, require_objects=False).manifest_sha256 != digest:
        raise SnapshotError('auto dedup source head changed')
    holds = _directory(workspace, 'migration/source-local-exact-repack/auto-dedup-holds-v1')
    value = {'contract': CONTRACT, 'dataset': dataset, 'expected_manifest_sha256': digest,
             'inventory_sha256': summary['inventory_sha256'], 'files': files, 'logical_bytes': logical,
             'duplicate_logical_bytes': duplicate, 'duplicate_path_count': summary['duplicate_path_count'],
             'old_encoded_bytes': encoded, 'reason': 'source_duplicate_bloat',
             'source_payload_reread': False, 'D_head_changed': False, 'retirement_authorized': False,
             'observed_at_epoch': time.time()}
    if private:
        value['private'] = True
    if sealed_sha is not None:
        value['sealed_source_prepared_sha256'] = sealed_sha
    value['identity_sha256'] = _identity(value)
    atomic_write_json(holds/(dataset+'-'+digest+'.json'), value, durable=True)
    ensure_stream_intent(workspace, dataset, digest)
    return value


def auto_holds(workspace: Path) -> set[tuple[str, str]]:
    """Read exact auto holds; a damaged record holds only its named source."""
    root = Path(workspace).absolute()/'migration/source-local-exact-repack/auto-dedup-holds-v1'
    if not os.path.lexists(root):
        return set()
    if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
        raise SnapshotError('auto dedup hold directory is redirected')
    paths = sorted(root.glob('*.json'))
    if len(paths) > 4096:
        raise SnapshotError('auto dedup hold directory exceeds its bound')
    result = set()
    for path in paths:
        match = re.fullmatch(r'([a-z0-9][a-z0-9-]{0,127})-([a-f0-9]{64})\.json', path.name)
        if not match:
            continue
        dataset, digest = match.groups()
        try:
            if path.is_symlink() or not path.is_file() or path.stat().st_size > 16384:
                raise SnapshotError('auto dedup hold record is redirected')
            value = json.loads(path.read_text())
            if (not isinstance(value, dict) or value.get('contract') != CONTRACT
                    or value.get('identity_sha256') != _identity(value)
                    or value.get('dataset') != dataset or value.get('expected_manifest_sha256') != digest
                    or value.get('reason') != 'source_duplicate_bloat'
                    or type(value.get('private', False)) is not bool):
                raise SnapshotError('auto dedup hold identity differs')
            stage = Path(workspace)/'migration/stream-source-repack-v1'/dataset/digest
            try:
                prepared = stream_repack._read_prepared(stage, dataset, digest)
                stream_repack.require_content_alias_install(stage, dataset, digest, prepared)
            except (OSError, ValueError, TypeError, KeyError, SnapshotError):
                pass
            else:
                if prepared.get('packed_format') == 'content-alias-v3':
                    continue  # The exact replacement is activated for the original owner.
        except (OSError, ValueError, TypeError, KeyError, SnapshotError):
            pass  # An unreadable record never releases this exact source.
        result.add((dataset, digest))
    return result


def auto_hold_rows(workspace: Path) -> list[dict]:
    """Keep valid planned generations visible even when discovery rotates."""
    workspace = Path(workspace).absolute()
    root = workspace/'migration/source-local-exact-repack/auto-dedup-holds-v1'
    if not os.path.lexists(root):
        return []
    if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
        raise SnapshotError('auto dedup hold directory is redirected')
    paths = sorted(root.glob('*.json'))
    if len(paths) > 4096:
        raise SnapshotError('auto dedup hold directory exceeds its bound')
    rows = []
    for path in paths:
        match = re.fullmatch(r'([a-z0-9][a-z0-9-]{0,127})-([a-f0-9]{64})\.json', path.name)
        if not match:
            continue
        dataset, digest = match.groups()
        try:
            if path.is_symlink() or not path.is_file() or path.stat().st_size > 16384:
                continue
            value = json.loads(path.read_text())
            if (not isinstance(value, dict) or value.get('contract') != CONTRACT
                    or value.get('identity_sha256') != _identity(value)
                    or value.get('dataset') != dataset or value.get('expected_manifest_sha256') != digest
                    or value.get('reason') != 'source_duplicate_bloat'
                    or type(value.get('private', False)) is not bool
                    or any(type(value.get(key)) is not int or value[key] < 0 for key in ('files', 'logical_bytes'))):
                continue
            rows.append({'dataset': dataset, 'expected_manifest_sha256': digest,
                         'origin': 'private_cold' if value.get('private') is True else 'cold',
                         'source': str(workspace/'migration/stream-source-repack-v1'/dataset/digest),
                         'files': value['files'], 'logical_bytes': value['logical_bytes']})
        except (OSError, ValueError, TypeError, KeyError):
            continue
    return rows
