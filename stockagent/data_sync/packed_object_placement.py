"""Local immutable-object placement behind unchanged packed manifest paths.

The operator can keep service archives on E while D retains cold datasets and
portable metadata. Locations are local control state, never a synchronized
command. A registered E generation is mandatory: a missing or changed E file
must not silently fall back to another copy. This is not a recovery receipt.
"""
from __future__ import annotations

from contextlib import closing, nullcontext
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
import time

from stockagent.data_sync.desync_snapshots import SnapshotError, _safe_relative_path, _fsync_directory, atomic_write_json

CONTRACT = 'stockagent.packed_object_placement.v1'
POLICY = Path('/etc/stockagent/packed-object-placement.json')
AUTHORITY = Path('/srv/stockagent-packed')
WORKSPACE = Path('/srv/stockagent-workspace')
STATE = Path('/var/lib/stockagent/packed-object-placement')
ROOT = Path(__file__).resolve().parents[2]
READER_FILES = (
    'stockagent/data_sync/packed_object_placement.py', 'scripts/run_packed_object_placement.py',
    'scripts/stage_d_cold_relocation.py',
    'stockagent/data_sync/packed_snapshots.py', 'stockagent/data_sync/packed_alias_v3.py',
    'stockagent/data_sync/packed_backup.py', 'stockagent/data_sync/offhost_backup.py',
    'stockagent/data_sync/packed_verification_sources.py', 'stockagent/data_sync/packed_member_stream.py',
    'stockagent/data_sync/packed_verification.py', 'stockagent/data_sync/windows_chunk_hash.py',
    'stockagent/data_sync/dedup_native_chunks.py', 'stockagent/data_sync/dedup_native_packs.py',
    'stockagent/data_sync/stream_repack.py', 'stockagent/data_sync/repack_retirement.py',
    'stockagent/data_sync/private_handoff_retirement.py', 'stockagent/data_sync/private_repack_history.py',
)
OBJECT = re.compile(r'objects/(?:blobs/[0-9a-f]{2}/[0-9a-f]{64}\.blob|'
                    r'packs/[0-9a-f]{2}/[0-9a-f]{64}\.zip|'
                    r'inventories/[0-9a-f]{2}/[0-9a-f]{64}\.jsonl\.gz)\Z')
_LOCAL = threading.local()
_POLICY_CACHE = None
_MOUNT_CACHE = None


def is_archive_path(path: Path) -> bool:
    return Path(path).is_relative_to(WORKSPACE/'archives')


def reader_code_identity() -> str:
    files = {name: hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in READER_FILES}
    return hashlib.sha256(json.dumps(files, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def require_reader_rollout(state: Path) -> dict:
    value = regular_document(state/'readers-ready.json')
    if (value.get('contract') != CONTRACT+'.reader-rollout'
            or value.get('reader_code_sha256') != reader_code_identity()
            or value.get('local_reader_tests_passed') is not True
            or not re.fullmatch('[a-f0-9]{64}', str(value.get('loaded_worker_code_identity', '')))):
        raise SnapshotError('placed archive reader rollout is missing or changed')
    return value


def signature(path: Path) -> list[int]:
    value = path.lstat()
    if not stat.S_ISREG(value.st_mode) or path.is_symlink():
        raise SnapshotError('placed immutable object is not regular')
    return [value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns,
            value.st_ctime_ns, value.st_mode, value.st_nlink]


def regular_document(path: Path, *, limit=65536) -> dict:
    if path.is_symlink():
        raise SnapshotError('placement control redirected')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid()
                or before.st_mode & 0o022 or before.st_size > limit):
            raise SnapshotError('placement control ownership or size differs')
        raw = stream.read(limit+1)
        after = os.fstat(stream.fileno())
    fields = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
    if any(getattr(before, key) != getattr(after, key) or
           getattr(after, key) != getattr(path.lstat(), key) for key in fields):
        raise SnapshotError('placement control changed')
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise SnapshotError('placement control must be an object')
    return value


def load_policy(path: Path | None = None) -> dict | None:
    global _POLICY_CACHE
    path = POLICY if path is None else path
    if not os.path.lexists(path):
        return None
    info = path.lstat()
    key = (str(path), info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
    if _POLICY_CACHE is not None and _POLICY_CACHE[0] == key:
        return _POLICY_CACHE[1]
    value = regular_document(path)
    if (value.get('contract') != CONTRACT or value.get('authority_node_id') != 'penguin'
            or value.get('cold_root') != str(AUTHORITY)
            or value.get('workspace_root') != str(WORKSPACE)
            or value.get('state_root') != str(STATE)
            or value.get('default_tier') != 'D' or value.get('enabled') is not True):
        raise SnapshotError('unsupported placement policy')
    names = value.get('warm_private_datasets')
    if (not isinstance(names, list) or not names or len(names) > 128 or len(set(names)) != len(names)
            or any(not isinstance(name, str) or re.fullmatch(r'[a-z0-9][a-z0-9-]{0,127}', name) is None
                   for name in names)
            or type(value.get('maximum_wave_bytes')) is not int
            or not 64*1024**2 <= value['maximum_wave_bytes'] <= 16*1024**3
            or type(value.get('reserve_bytes')) is not int or value['reserve_bytes'] < 64*1024**3):
        raise SnapshotError('invalid placement cohort or capacity limits')
    _POLICY_CACHE = key, value
    return value


class Locations:
    """One SQLite control index, rather than one small ticket per CAS object."""
    def __init__(self, state: Path, workspace: Path, authority: Path):
        self.state, self.workspace, self.authority = map(Path, (state, workspace, authority))
        self.database = self.state/'locations.sqlite'

    def initialize(self):
        self.state.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.state.resolve(strict=True) != self.state:
            raise SnapshotError('placement state root redirected')
        with closing(sqlite3.connect(self.database)) as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('PRAGMA synchronous=FULL')
            db.execute('CREATE TABLE IF NOT EXISTS objects ('
                       'authority TEXT NOT NULL, relative TEXT NOT NULL, sha256 TEXT NOT NULL,'
                       'bytes INTEGER NOT NULL, physical_relative TEXT NOT NULL,'
                       'E_signature TEXT NOT NULL, D_signature TEXT NOT NULL,'
                       'dataset TEXT NOT NULL, source_manifest TEXT NOT NULL, state TEXT NOT NULL,'
                       'reclaimed_bytes INTEGER, updated_at REAL NOT NULL, retired_by TEXT,'
                       'PRIMARY KEY(authority,relative), UNIQUE(physical_relative))')
            db.commit()
        self.database.chmod(0o600)

    def _reader(self):
        if not self.database.exists():
            return None
        info = self.database.lstat()
        if (not stat.S_ISREG(info.st_mode) or self.database.is_symlink()
                or info.st_uid != os.geteuid() or info.st_mode & 0o022
                or self.state.resolve(strict=True) != self.state):
            raise SnapshotError('placement database is not private local control')
        key = (str(self.database), info.st_dev, info.st_ino)
        cached = getattr(_LOCAL, 'placement_reader', None)
        if cached is None or cached[0] != key:
            if cached is not None:
                cached[1].close()
            db = sqlite3.connect(self.database.as_uri()+'?mode=ro', uri=True, isolation_level=None)
            db.row_factory = sqlite3.Row
            _LOCAL.placement_reader = key, db
        return _LOCAL.placement_reader[1]

    def get(self, authority: str, relative: str) -> dict | None:
        reader = self._reader()
        if reader is None:
            return None
        row = reader.execute('SELECT * FROM objects WHERE authority=? AND relative=?',
                             (authority, relative)).fetchone()
        return dict(row) if row is not None else None

    def physical(self, row: dict) -> Path:
        relative = _safe_relative_path(row['physical_relative'], 'placed object path')
        if relative.parts[0] != 'archives':
            raise SnapshotError('placed object is outside E archives')
        target = self.workspace.joinpath(*relative.parts)
        if (self.workspace.resolve(strict=True) != self.workspace or
                target.parent.resolve(strict=True) != target.parent):
            raise SnapshotError('placed E object namespace redirected')
        expected = json.loads(row['E_signature'])
        if (not isinstance(expected, list) or len(expected) != 7
                or any(type(n) is not int or n < 0 for n in expected)):
            raise SnapshotError('placed E generation is malformed')
        actual = signature(target)
        # Linux block-device numbers can change at reboot. The enrolled E
        # mount and persistent inode/clock identity still must agree.
        if (actual[0] != self.workspace.stat().st_dev or actual[1:] != expected[1:]
                or actual[2] != row['bytes']):
            raise SnapshotError('placed E object generation changed')
        return target

    def resolve(self, cold: Path, relative: str) -> Path:
        cold = Path(cold)
        logical = cold.joinpath(*_safe_relative_path(relative, 'object relpath').parts)
        roots = {self.authority: 'public', self.authority/'.local-state/private-history': 'private'}
        if cold not in roots or OBJECT.fullmatch(relative) is None:
            return logical
        row = self.get(roots[cold], relative)
        if row is None or row['state'] == 'E_retired':
            return logical
        if row['state'] not in {'E_ready', 'D_unlink_pending', 'E_authoritative'}:
            raise SnapshotError('placed object state is not readable')
        return self.physical(row)

    def register(self, *, authority: str, relative: str, digest: str, size: int,
                 physical: Path, dataset: str, source_manifest: str, original_signature: list):
        if (authority not in {'public', 'private'} or OBJECT.fullmatch(relative) is None
                or re.fullmatch('[a-f0-9]{64}', digest) is None
                or digest not in Path(relative).name or type(size) is not int or size < 0):
            raise SnapshotError('invalid placement object identity')
        record = dict(authority=authority, relative=relative, sha256=digest, bytes=size,
                      physical_relative=physical.relative_to(self.workspace).as_posix(),
                      E_signature=json.dumps(signature(physical)), D_signature=json.dumps(original_signature),
                      dataset=dataset, source_manifest=source_manifest, state='E_ready',
                      reclaimed_bytes=0, updated_at=time.time(), retired_by=None)
        self.physical(record)
        self.initialize()
        existing = self.get(authority, relative)
        if existing is not None:
            if existing['state'] == 'E_retired':
                with closing(sqlite3.connect(self.database)) as db:
                    db.execute('PRAGMA synchronous=FULL')
                    columns = [key for key in record if key not in ('authority', 'relative')]
                    changed = db.execute('UPDATE objects SET '+','.join(key+'=?' for key in columns)+
                        " WHERE authority=? AND relative=? AND state='E_retired'",
                        (*[record[key] for key in columns], authority, relative))
                    if changed.rowcount != 1:
                        raise SnapshotError('retired object placement changed during reuse')
                    db.commit()
                return record
            keys = ('sha256', 'bytes', 'physical_relative', 'E_signature', 'D_signature', 'source_manifest')
            if any(existing[key] != record[key] for key in keys):
                raise SnapshotError('placement belongs to another exact transaction')
            return existing
        with closing(sqlite3.connect(self.database)) as db:
            db.execute('PRAGMA synchronous=FULL')
            columns = list(record)
            db.execute('INSERT INTO objects ('+','.join(columns)+') VALUES ('+','.join('?' for _ in columns)+')',
                       tuple(record.values()))
            db.commit()
        return record

    def update(self, row: dict, state: str, *, reclaimed_bytes=None):
        if state not in {'D_unlink_pending', 'E_authoritative'}:
            raise SnapshotError('invalid placement transition')
        with closing(sqlite3.connect(self.database)) as db:
            db.execute('PRAGMA synchronous=FULL')
            result = db.execute('UPDATE objects SET state=?,reclaimed_bytes=?,updated_at=? '
                'WHERE authority=? AND relative=? AND E_signature=? AND state=?',
                (state, reclaimed_bytes, time.time(), row['authority'], row['relative'],
                 row['E_signature'], row['state']))
            if result.rowcount != 1:
                raise SnapshotError('placement transaction changed during transition')
            db.commit()
        return self.get(row['authority'], row['relative'])

    def pending(self, dataset: str) -> list[dict]:
        reader = self._reader()
        if reader is None:
            return []
        return [dict(row) for row in reader.execute(
            "SELECT * FROM objects WHERE dataset=? AND state IN ('E_ready','D_unlink_pending') ORDER BY updated_at LIMIT 512",
            (dataset,))]

    def totals(self) -> dict:
        reader = self._reader()
        rows = [] if reader is None else reader.execute(
            'SELECT dataset,state,COUNT(*) AS files,SUM(bytes) AS bytes,'
            'SUM(reclaimed_bytes) AS retired_D_allocated_bytes FROM objects GROUP BY dataset,state').fetchall()
        return {'contract': CONTRACT, 'observed_at_epoch': time.time(), 'datasets': [dict(row) for row in rows],
                'NAS_accepted': False, 'independent_readback': False}

    def mark_retired(self, row: dict, plan_sha: str):
        """Called only by the original reference/pin/process retirement owner."""
        if re.fullmatch('[a-f0-9]{64}', plan_sha) is None:
            raise SnapshotError('placed object retirement needs a fixed original plan')
        if row['state'] == 'E_retired':
            if row['retired_by'] != plan_sha:
                raise SnapshotError('placed object was retired by another plan')
            return
        with closing(sqlite3.connect(self.database)) as db:
            db.execute('PRAGMA synchronous=FULL')
            changed = db.execute("UPDATE objects SET state='E_retired',retired_by=?,updated_at=? "
                'WHERE authority=? AND relative=? AND E_signature=? AND state=?',
                (plan_sha, time.time(), row['authority'], row['relative'], row['E_signature'], row['state']))
            if changed.rowcount != 1:
                raise SnapshotError('placed object retirement generation changed')
            db.commit()


def finish_handoff(locations: Locations, row: dict, *, guard=lambda: None,
                   process_check=lambda _paths: [], before_unlink=lambda: None) -> dict:
    """Resume the exact handoff after its full source-hashed E copy is durable."""
    locations.physical(row)
    source_root = locations.authority
    if row['authority'] == 'private':
        source_root = source_root/'.local-state/private-history'
    source = source_root/row['relative']
    if row['state'] == 'E_authoritative':
        return row
    guard()
    if not os.path.lexists(source):
        # A prior durable unlink intent can survive a crash; the lost final
        # disk-allocation observation is left unknown, never counted twice.
        if row['state'] != 'D_unlink_pending':
            raise SnapshotError('D source absent without this handoff intent')
        return locations.update(row, 'E_authoritative', reclaimed_bytes=None)
    before = source.lstat()
    if signature(source) != json.loads(row['D_signature']) or before.st_nlink != 1:
        raise SnapshotError('D source changed or shared before tier handoff')
    if process_check([source]):
        raise SnapshotError('D source has an active reader; tier handoff deferred')
    if row['state'] == 'E_ready':
        row = locations.update(row, 'D_unlink_pending', reclaimed_bytes=None)
    before_unlink()
    guard()
    locations.physical(row)
    if signature(source) != json.loads(row['D_signature']):
        raise SnapshotError('D source changed after durable tier intent')
    source.unlink()
    _fsync_directory(source.parent)
    return locations.update(row, 'E_authoritative', reclaimed_bytes=before.st_blocks*512)


def relocate(locations: Locations, cold: Path, dataset: str, manifest_sha: str, objects: list[dict], *,
             guard=lambda: None, process_check=lambda _paths: [],
             copier=None, progress=lambda _event: None, handoff_owner=nullcontext) -> dict:
    """One bounded immutable wave; callers hold original publication owners.

    Each object is hashed during its single sequential source-to-E pass.
    No decompression or destination reread is performed. A crash after
    routing resumes only its exact E generation before removing any D name.
    """
    if (cold != locations.authority/'.local-state/private-history'
            or re.fullmatch('[a-z0-9][a-z0-9-]{0,127}', dataset) is None
            or re.fullmatch('[a-f0-9]{64}', manifest_sha) is None or len(objects) > 512):
        raise SnapshotError('invalid private archive placement wave')
    locations.initialize()
    unfinished = locations.pending(dataset)
    if unfinished:
        with handoff_owner():
            for row in unfinished:
                finish_handoff(locations, row, guard=guard, process_check=process_check)
    from scripts import stage_d_cold_relocation as staging
    from scripts.retire_staged_cold_objects import _identity, closed_E_handoff
    from stockagent.data_sync.packed_backup import signature as source_signature
    plans = locations.state/'waves'/dataset
    plans.mkdir(mode=0o700, parents=True, exist_ok=True)
    if plans.resolve(strict=True) != plans:
        raise SnapshotError('placement wave controls redirected')
    pending = []
    for path in sorted(plans.glob('*.json')):
        plan = regular_document(path, limit=2*1024**2)
        if not (plans/(path.stem+'.done')).exists():
            if (plan.get('identity_sha256') != path.stem or _identity(plan) != path.stem
                    or plan.get('dataset') != dataset or plan.get('candidate_scope') != CONTRACT):
                raise SnapshotError('interrupted placement wave identity differs')
            pending.append(plan)
    selected = []
    for item in ([] if pending else objects):
        relative, digest, size = item['relpath'], item['sha256'], item['bytes']
        if (OBJECT.fullmatch(relative) is None or '/inventories/' in relative
                or digest not in Path(relative).name or type(size) is not int or size < 0):
            raise SnapshotError('unsupported archive placement member')
        existing = locations.get('private', relative)
        if existing is not None and existing['state'] != 'E_retired':
            locations.physical(existing)
            continue
        guard()
        source = cold/relative
        before = signature(source)
        if (before[2] != size or before[-1] != 1 or source.parent.resolve(strict=True) != source.parent):
            raise SnapshotError('archive source is shared, redirected or changed')
        selected.append({'relative': relative, 'sha256': digest, 'bytes': size,
                         'signature': list(source_signature(source)), 'placement_signature': before})
    if selected and process_check([cold/item['relative'] for item in selected]):
        raise SnapshotError('archive source is in use')
    if not pending and selected:
        plan = {'contract': staging.CONTRACT, 'candidate_scope': CONTRACT,
                'dataset': dataset, 'source_manifest_sha256': manifest_sha,
                'objects': selected, 'files': len(selected), 'bytes': sum(x['bytes'] for x in selected),
                'deletion_authorized': False}
        plan['identity_sha256'] = _identity(plan)
        atomic_write_json(plans/(plan['identity_sha256']+'.json'), plan, durable=True)
        pending.append(plan)
    completed = total = 0
    # One existing exact wave is resumed before another generation is planned.
    for plan in pending[:1]:
        guard()
        destination = (locations.workspace/'archives'/dataset/plan['source_manifest_sha256']/
                       plan['identity_sha256'])
        def checkpoint(files, count):
            guard()
            progress({'stage': 'copying_old_D_archive_to_E', 'completed_objects': files,
                      'completed_bytes': count, 'total_bytes': plan['bytes'], 'total_objects': plan['files'],
                      'wave_identity': plan['identity_sha256']})
        result = (copier or staging.stage_fast)(plan, cold=cold, destination=destination,
            progress=checkpoint, resume=destination.exists())
        verified = closed_E_handoff(plan, result, destination)
        with handoff_owner():
            guard()
            if process_check([cold/item['relative'] for item in verified
                              if (cold/item['relative']).exists()]):
                raise SnapshotError('archive source has an active reader after copy')
            for item in verified:
                guard()
                source = cold/item['relative']
                existing = locations.get('private', item['relative'])
                row = (existing if existing is not None and existing['state'] != 'E_retired' else locations.register(
                    authority='private', relative=item['relative'], digest=item['sha256'], size=item['bytes'],
                    physical=destination/'cold'/item['relative'], dataset=dataset,
                    source_manifest=plan['source_manifest_sha256'], original_signature=item['placement_signature']))
                finish_handoff(locations, row, guard=guard)
                completed += 1
                total += item['bytes']
                progress({'stage': 'E_archive_registered_D_name_retired', 'completed_objects': completed,
                          'completed_bytes': total, 'total_bytes': plan['bytes'], 'total_objects': plan['files'],
                          'wave_identity': plan['identity_sha256']})
        atomic_write_json(plans/(plan['identity_sha256']+'.done'), {
            'contract': CONTRACT+'.wave-complete', 'plan_identity_sha256': plan['identity_sha256'],
            'completed_at_epoch': time.time(), 'files': completed, 'bytes': total,
            'source_hashed_while_copying': True, 'independent_readback': False}, durable=True)
    return {**locations.totals(), 'wave_files': completed, 'wave_bytes': total}


def resolve_read(cold: Path, relative: str) -> Path:
    logical = Path(cold).joinpath(*_safe_relative_path(relative, 'object relpath').parts)
    if Path(cold) not in {AUTHORITY, AUTHORITY/'.local-state/private-history'}:
        return logical
    # No enrollment means exactly the old behavior, with one cached dentry.
    if not POLICY.exists():
        return logical
    policy = load_policy()
    if policy is None:
        return logical
    locations = Locations(Path(policy['state_root']), Path(policy['workspace_root']), Path(policy['cold_root']))
    result = locations.resolve(cold, relative)
    if result != logical:
        global _MOUNT_CACHE
        root = locations.workspace.stat()
        key = root.st_dev, root.st_ino
        if _MOUNT_CACHE is None or _MOUNT_CACHE[0] != key or time.monotonic()-_MOUNT_CACHE[1] > 10:
            from stockagent.data_sync import workspace_volume
            workspace_volume.check()
            _MOUNT_CACHE = key, time.monotonic()
    return result


def has_placed_objects(cold: Path, objects) -> bool:
    """A cheap routing hint; actual readers still validate every generation."""
    if cold not in {AUTHORITY, AUTHORITY/'.local-state/private-history'} or not POLICY.exists():
        return False
    policy = load_policy()
    locations = Locations(Path(policy['state_root']), Path(policy['workspace_root']), AUTHORITY)
    reader = locations._reader()
    if reader is None:
        return False
    authority = 'public' if cold == AUTHORITY else 'private'
    placed = {row[0] for row in reader.execute("SELECT relative FROM objects WHERE authority=? AND state!='E_retired'", (authority,))}
    return any(item['relpath'] in placed for item in objects)


def enrolled_locations(cold: Path) -> Locations | None:
    if cold not in {AUTHORITY, AUTHORITY/'.local-state/private-history'} or not POLICY.exists():
        return None
    policy = load_policy()
    return Locations(Path(policy['state_root']), Path(policy['workspace_root']), AUTHORITY)


def retirement_record(cold: Path, relative: str) -> dict | None:
    locations = enrolled_locations(cold)
    if locations is None:
        return None
    authority = 'public' if cold == locations.authority else 'private'
    row = locations.get(authority, relative)
    if row is None or row['state'] == 'E_retired':
        return None
    if row['state'] != 'E_authoritative':
        raise SnapshotError('placed archive handoff must finish before retirement')
    locations.physical(row)
    keys = ('authority', 'relative', 'sha256', 'bytes', 'physical_relative', 'E_signature', 'source_manifest')
    return {key: row[key] for key in keys}


def retirement_path(cold: Path, record: dict, *, missing_ok=False) -> tuple[Locations, dict, Path]:
    locations = enrolled_locations(cold)
    if locations is None:
        raise SnapshotError('placed object retirement enrollment missing')
    row = locations.get(record['authority'], record['relative'])
    if row is None or any(row.get(key) != value for key, value in record.items()):
        raise SnapshotError('placed object retirement differs from the original fixed plan')
    path = locations.workspace/_safe_relative_path(row['physical_relative'], 'retired E object')
    if path.parent.resolve(strict=True) != path.parent:
        raise SnapshotError('placed object retirement namespace redirected')
    if os.path.lexists(path):
        locations.physical(row)
    elif not missing_ok:
        raise SnapshotError('placed object missing without retirement intent')
    return locations, row, path


def matches_original_generation(path: Path, expected: list) -> bool:
    """Bind an original installed D inode to its source-hashed E relocation."""
    if not is_archive_path(path) or not POLICY.exists():
        return False
    locations = enrolled_locations(AUTHORITY)
    if locations is None:
        return False
    reader = locations._reader()
    if reader is None:
        return False
    row = reader.execute('SELECT * FROM objects WHERE physical_relative=?',
                         (path.relative_to(locations.workspace).as_posix(),)).fetchone()
    if row is None or row['state'] not in {'E_ready', 'D_unlink_pending', 'E_authoritative'}:
        return False
    row = dict(row)
    locations.physical(row)
    old = json.loads(row['D_signature'])
    original = [old[0], old[1], old[5], old[2], old[3], old[4], old[6]]
    if expected == original:
        return True
    if isinstance(expected, list) and len(expected) == 7 and expected[1:] == original[1:]:
        from stockagent.data_sync.stream_repack import _trusted_device_renumbering
        logical_root = locations.authority if row['authority'] == 'public' else locations.authority/'.local-state/private-history'
        return _trusted_device_renumbering(logical_root/row['relative'], locations.authority.stat().st_dev)
    return False
