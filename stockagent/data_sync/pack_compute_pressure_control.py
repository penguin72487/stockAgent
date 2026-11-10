"""Relieve Vast scratch pressure using the original spool and D retention owners.

Called only while the caller owns the spool lock. The D source must belong to
an independently reconstructed fixed release and is freshly hashed under the
original retention lease; a cache-hit staging proof alone is insufficient. The remote object is a
reconstructible, private compute cache, never an accepted source or training
result. No primary, NAS, or Syncthing object is deleted here.
"""
from __future__ import annotations

import base64
from datetime import datetime, time as wall_time
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import shlex
import stat
import subprocess
import uuid
from zoneinfo import ZoneInfo

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync import pack_compute_cache, pack_compute_pressure, packed_verification
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.packed_snapshots import (
    _native_blob_hash_batches, _try_publication_lock, resolve_packed_snapshot_id,
)
from stockagent.data_sync.windows_cold_io import hash_many

SCOPES = {
    '/srv/stockagent-packed/.local-state/private-history': 'local-tw-index-futures',
    '/srv/stockagent-packed': 'tw-index-futures',
}
VERSION_CONTROL = Path('/var/lib/stockagent/lakehouse-control/dataset-versions')


def _outside_opening() -> bool:
    from scripts.check_outside_tw_opening_resource_window import evaluate
    root = Path(__file__).resolve().parents[2] / 'data_tw_public'
    return bool(evaluate(datetime.now(ZoneInfo('Asia/Taipei')),
        protected_until=wall_time(9, 10), official_calendar_root=root)['allowed'])


def _fixed_release(scope, cold):
    """Resolve the owner's independently reconstructed, immutable D release."""
    root = VERSION_CONTROL
    current_path = root / 'current.json'
    current = json.loads(current_path.read_bytes())
    operation = Path(current['operation_directory'])
    if (current.get('contract') != 'all_catalog_incremental_migration_v1'
            or current.get('state') != 'verified' or operation.is_symlink()
            or operation.parent != root / 'operations' or operation.resolve() != operation):
        raise ValueError('fixed D recovery operation is not verified')
    progress = json.loads((operation / 'progress.json').read_bytes())
    row = progress.get('datasets', {}).get(scope['dataset'], {})
    if (progress.get('contract') != current['contract'] or progress.get('state') != 'verified'
            or progress.get('plan_sha256') != current.get('plan_sha256')
            or row.get('state') != 'verified'):
        raise ValueError('fixed D recovery progress is not verified')
    recovery = operation / 'recovery' / (scope['dataset'] + '.json')
    if Path(row.get('recovery_receipt', '')) != recovery or recovery.is_symlink():
        raise ValueError('fixed D recovery receipt identity differs')
    proof = json.loads(recovery.read_bytes())
    if (proof.get('contract') != current['contract']
            or proof.get('dataset') != scope['dataset']
            or proof.get('full_original_file_reconstruction_verified') is not True
            or proof.get('snapshot_id') != row.get('snapshot_id')
            or proof.get('manifest_sha256') != row.get('manifest_sha256')):
        raise ValueError('fixed D recovery proof is incomplete')
    fixed = resolve_packed_snapshot_id(cold, scope['dataset'], row['snapshot_id'], require_objects=False)
    if fixed.manifest_sha256 != row['manifest_sha256']:
        raise ValueError('fixed D manifest changed since recovery')
    objects = {}
    for item in fixed.manifest['archive']['objects']:
        if item.get('kind') != 'pack':
            continue
        digest = item['sha256']
        if item.get('relpath') != f'objects/packs/{digest[:2]}/{digest}.zip':
            raise ValueError('fixed D manifest has a noncanonical pack path')
        objects[digest] = item['bytes']
    return {'snapshot_id': row['snapshot_id'], 'manifest_sha256': fixed.manifest_sha256,
            'operation_directory': str(operation), 'objects': objects}


def _request(scope, current, completed, remote, reserve, available, release):
    """Select bounded cache pairs whose original is in the fixed D release."""
    rows = []
    for digest, value in completed.items():
        info = current.get(digest)
        if (info is None or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or value.get('payload_cache_retired') is True
                or value.get('object_sha256') != digest
                or value.get('object_bytes') != info.st_size
                or release['objects'].get(digest) != info.st_size
                or value.get('source_generation') != list(pack_compute_cache.signature(info))
                or value.get('worker_sha256') != packed_verification.SOURCE_SHA256
                or value.get('cache_worker_sha256') != pack_compute_cache.SOURCE_SHA256
                or value.get('inventory_bound') is not False
                or value.get('source_retirement') is not False or value.get('acceptance') is not False
                or type(value.get('computed_at_unix')) not in (int, float)
                or not math.isfinite(value['computed_at_unix'])):
            continue
        fields = ('object_sha256', 'object_bytes', 'worker_sha256',
                  'cache_worker_sha256', 'computed_at_unix', 'source_generation')
        expected = {key: value[key] for key in fields}
        expected.update(source_retirement=False)
        rows.append({'item': {'kind': 'pack', 'sha256': digest, 'bytes': info.st_size},
                     'expected': expected})
    rows.sort(key=lambda row: (-row['item']['bytes'], row['expected']['computed_at_unix']))
    if type(available) is not int or available < 0:
        raise ValueError('remote free space is not observed')
    target = reserve + 8 * 1024**3
    needed = max(0, target - available)
    selected, total = [], 0
    for row in rows:
        if len(selected) >= 128 or total >= needed + needed // 10:
            break
        if total + row['item']['bytes'] > 16 * 1024**3:
            continue
        selected.append(row)
        total += row['item']['bytes']
    return {'plan': uuid.uuid4().hex, 'boot_id': remote.boot_id, 'reserve_bytes': reserve,
            'target_free_bytes': target, 'candidates': selected}


def _remote_program(repo, reserve):
    """Send the exact pinned implementation; do not modify the Vast checkout."""
    worker = Path(packed_verification.__file__).read_bytes()
    cache = Path(pack_compute_cache.__file__).read_bytes()
    pressure = Path(pack_compute_pressure.__file__).read_bytes()
    if (hashlib.sha256(worker).hexdigest() != packed_verification.SOURCE_SHA256
            or hashlib.sha256(cache).hexdigest() != pack_compute_cache.SOURCE_SHA256
            or hashlib.sha256(pressure).hexdigest() != pack_compute_pressure.SOURCE_SHA256):
        raise ValueError('compute implementation changed after import')
    encoded = [base64.b64encode(value).decode('ascii') for value in (worker, cache, pressure)]
    root = str(PurePosixPath(str(repo)) / 'service_data/pack_compute_cache')
    return (
        "import base64,hashlib,json,signal,sys,types\n"
        "def deadline(*a): raise TimeoutError('bounded pressure run expired')\n"
        "signal.signal(signal.SIGALRM,deadline);signal.alarm(60)\n"
        f"w=base64.b64decode({encoded[0]!r}); n={{'__name__':'fixed_pack_library','WORKER_SHA256':hashlib.sha256(w).hexdigest()}};exec(compile(w,'<fixed-pack-library>','exec'),n)\n"
        f"c=base64.b64decode({encoded[1]!r}); q={{'__name__':'fixed_cache_library','CACHE_WORKER_SHA256':hashlib.sha256(c).hexdigest()}};exec(compile(c,'<fixed-cache-library>','exec'),q)\n"
        f"p=base64.b64decode({encoded[2]!r}); r={{'__name__':'fixed_pressure_library','PRESSURE_WORKER_SHA256':hashlib.sha256(p).hexdigest()}};exec(compile(p,'<fixed-pressure-library>','exec'),r)\n"
        f"owner=q['ComputeCache']({root!r},types.SimpleNamespace(**n),{reserve!r})\n"
        "print(json.dumps(r['relieve'](owner,json.load(sys.stdin)),separators=(',',':')),flush=True)\n"
    )


def retry_vast_syncthing_scan(options):
    """Retry only the enrolled local index folder after a disk-free error.

    A scan request is not convergence or backup acceptance. Credentials stay
    on the remote host and the original Syncthing owner remains responsible.
    """
    program = r'''import json,os,urllib.request
key=os.environ.get('STGUIAPIKEY') or os.environ.get('OPEN_BUTTON_TOKEN')
if not key: print(json.dumps({'state':'failed'}));raise SystemExit
base='http://127.0.0.1:18384/rest/db/'
headers={'X-API-Key':key}
try:
 with urllib.request.urlopen(urllib.request.Request(base+'status?folder=stockagent-packed',headers=headers),timeout=5) as response:
  status=json.load(response)
 if status.get('state')=='error' and 'insufficient space on disk for database' in str(status.get('error','')).lower():
  with urllib.request.urlopen(urllib.request.Request(base+'scan?folder=stockagent-packed',headers=headers,data=b'',method='POST'),timeout=5) as response:
   print(json.dumps({'state':'requested' if response.status==200 else 'failed'}))
 elif status.get('state') in {'idle','syncing','scanning','scan-waiting'}:
  print(json.dumps({'state':'not_required'}))
 else: print(json.dumps({'state':'failed'}))
except Exception: print(json.dumps({'state':'failed'}))
'''
    command = ('if [ -r /etc/environment ]; then set -a; . /etc/environment; set +a; fi; '
               'python3 -c ' + shlex.quote(program))
    try:
        result = subprocess.run(['ssh', '-T', '-F', options['ssh_profile'], 'stockagent-vast', command],
            capture_output=True, text=True, timeout=15, check=True)
        value = json.loads(result.stdout)
        return value['state'] if isinstance(value, dict) and value.get('state') in {'requested', 'not_required'} else 'failed'
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError):
        return 'failed'


def relieve(scope, current, completed, options, remote, state_root: Path, *, available, progress=lambda _: None):
    if SCOPES.get(scope.get('cold_root')) != scope.get('dataset'):
        raise ValueError('pressure source is not an enrolled immutable D scope')
    if not _outside_opening():
        return {'state': 'waiting_for_tw_opening', 'removed': []}
    reserve = options.get('remote_spool_reserve_bytes', 32 * 1024**3)
    if type(reserve) is not int or reserve < 1024**3:
        raise ValueError('invalid existing remote reserve')
    state_root = Path(state_root)
    if state_root.is_symlink() or state_root.resolve() != state_root or not state_root.is_dir():
        raise ValueError('spool receipt root is redirected')
    marker = state_root / ('pressure-' + scope['dataset'] + '.status.json')
    if marker.is_symlink():
        raise ValueError('pressure status is redirected')
    if marker.exists() and json.loads(marker.read_bytes()).get('state') in {'prepared', 'outcome_unknown'}:
        return {'state': 'blocked_unknown_prior_result', 'removed': []}
    cold = Path(scope['cold_root'])
    authority = Path('/srv/stockagent-packed')
    _check_d_primary_mount(authority)
    owner = cold / '.local-state/locks/publish-retention-global.lock'
    if not owner.is_file() or owner.is_symlink():
        raise ValueError('original D retention owner is unavailable')
    with _try_publication_lock(owner, shared=True):
        release = _fixed_release(scope, cold)
        request = _request(scope, current, completed, remote, reserve, available, release)
        candidates = request['candidates']
        if not candidates:
            return {'state': 'no_proven_candidates', 'removed': []}
        # A staged cache hit did not re-read D.  Check every selected member's
        # complete original bytes under the retention lease before any unlink.
        members = []
        for row in candidates:
            digest = row['item']['sha256']
            path = cold / 'objects/packs' / digest[:2] / (digest + '.zip')
            if list(pack_compute_cache.signature(path.lstat())) != row['expected']['source_generation']:
                raise ValueError('D source changed before cache pressure relief')
            members.append((path, row['item']['bytes']))
        total_bytes = sum(size for _, size in members)
        checked = 0
        progress({'phase': 'verify_fixed_d_originals', 'done_bytes': 0, 'total_bytes': total_bytes,
                  'plan': request['plan']})
        # Keep each native HDD pass small enough to report progress and yield
        # at batch boundaries; file bytes remain strictly sequential per pass.
        batches, group, size = [], [], 0
        for path, count in members:
            if group and size + count > 4 * 1024**3:
                batches.extend(_native_blob_hash_batches(group))
                group, size = [], 0
            group.append((path, count))
            size += count
        if group:
            batches.extend(_native_blob_hash_batches(group))
        if sum(len(batch) for batch in batches) != len(members):
            raise ValueError('D original exceeds bounded native full-hash policy')
        for batch in batches:
            if not _outside_opening():
                return {'state': 'waiting_for_tw_opening', 'removed': []}
            hashes = hash_many(batch)
            for path in batch:
                if hashes.get(path) != path.stem:
                    raise ValueError('D original full SHA differs from fixed manifest')
                checked += path.stat().st_size
            progress({'phase': 'verify_fixed_d_originals', 'done_bytes': checked, 'total_bytes': total_bytes,
                      'plan': request['plan']})
        if _fixed_release(scope, cold)['manifest_sha256'] != release['manifest_sha256']:
            raise ValueError('fixed D recovery changed during verification')
        for row in candidates:
            digest = row['item']['sha256']
            path = cold / 'objects/packs' / digest[:2] / (digest + '.zip')
            if list(pack_compute_cache.signature(path.lstat())) != row['expected']['source_generation']:
                raise ValueError('D source changed after full hash and before cache relief')
            row['expected'].update(source_verified=True,
                source_manifest_sha256=release['manifest_sha256'],
                source_snapshot_id=release['snapshot_id'])
        receipt = state_root / ('pressure-' + request['plan'] + '.json')
        atomic_write_json(receipt, {'contract': 'stockagent.vast_pack_pressure.v1',
            'state': 'prepared', 'dataset': scope['dataset'], 'request': request,
            'fixed_recovery': {key: release[key] for key in ('snapshot_id', 'manifest_sha256', 'operation_directory')},
            'pressure_worker_sha256': pack_compute_pressure.SOURCE_SHA256,
            'source_retirement': False, 'nas_acceptance': False}, durable=True)
        atomic_write_json(marker, {'state': 'prepared', 'plan': request['plan'],
            'receipt': str(receipt), 'dataset': scope['dataset']}, durable=True)
        progress({'phase': 'reclaim_remote_cache', 'done_bytes': checked, 'total_bytes': total_bytes,
                  'plan': request['plan']})
        program = _remote_program(remote.repo, reserve)
        command = 'python3 -c ' + shlex.quote(program)
        try:
            process = subprocess.run(['ssh', '-T', '-F', options['ssh_profile'], 'stockagent-vast', command],
                input=json.dumps(request), text=True, capture_output=True, timeout=75, check=True)
            result = json.loads(process.stdout)
            allowed = {row['item']['sha256']: row['item']['bytes'] for row in candidates}
            removed = result.get('removed')
            if (not isinstance(removed, list) or result.get('source_retirement') is not False
                    or result.get('nas_acceptance') is not False or len(removed) > len(allowed)
                    or len({row.get('object_sha256') for row in removed}) != len(removed)
                    or any(not isinstance(row, dict) or allowed.get(row.get('object_sha256')) != row.get('object_bytes')
                           or type(row.get('released_allocated_bytes')) is not int
                           or row['released_allocated_bytes'] < 0 for row in removed)):
                raise ValueError('remote pressure result differs from the fixed plan')
            _check_d_primary_mount(authority)
            for row in candidates:
                digest = row['item']['sha256']
                path = cold / 'objects/packs' / digest[:2] / (digest + '.zip')
                if list(pack_compute_cache.signature(path.lstat())) != row['expected']['source_generation']:
                    raise ValueError('D source changed during cache pressure relief')
            atomic_write_json(receipt, {'contract': 'stockagent.vast_pack_pressure.v1',
                'state': 'recorded', 'dataset': scope['dataset'], 'request': request,
                'fixed_recovery': {key: release[key] for key in ('snapshot_id', 'manifest_sha256', 'operation_directory')},
                'pressure_worker_sha256': pack_compute_pressure.SOURCE_SHA256,
                'result': result, 'source_retirement': False, 'nas_acceptance': False}, durable=True)
            atomic_write_json(marker, {'state': 'recorded', 'plan': request['plan'],
                'receipt': str(receipt), 'dataset': scope['dataset']}, durable=True)
            return result
        except BaseException as error:
            # Remote per-object prepared/removed journals survive a disconnect;
            # the local receipt explicitly refuses to count unknown outcomes.
            atomic_write_json(receipt, {'contract': 'stockagent.vast_pack_pressure.v1',
                'state': 'outcome_unknown', 'dataset': scope['dataset'], 'request': request,
                'fixed_recovery': {key: release[key] for key in ('snapshot_id', 'manifest_sha256', 'operation_directory')},
                'pressure_worker_sha256': pack_compute_pressure.SOURCE_SHA256,
                'error_type': type(error).__name__, 'source_retirement': False,
                'nas_acceptance': False}, durable=True)
            atomic_write_json(marker, {'state': 'outcome_unknown', 'plan': request['plan'],
                'receipt': str(receipt), 'dataset': scope['dataset']}, durable=True)
            raise
