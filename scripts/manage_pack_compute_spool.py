#!/usr/bin/env python3
"""Stage newly sealed D packs to the enrolled private Vast compute cache.

This read-only compute queue does not publish, change a head, adopt an inventory
or retire a source. Timestamp discovery only prioritizes speculative work;
every result is bound to full SHA and the final owner still checks its inventory.
"""
from __future__ import annotations

import argparse
import base64
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import ExitStack
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import stat
import sys
import subprocess
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from stockagent.data_sync.packed_verification import RemotePacks, configuration, SOURCE_SHA256 as VERIFIER_SHA
from stockagent.data_sync.pack_compute_cache import MAX_OBJECT_BYTES, signature, SOURCE_SHA256 as CACHE_SHA

STATE = ROOT / 'service_data/pack_compute_spool'
CONTRACT = 'stockagent.pack_compute_spool.v1'
ENROLLED_SOURCES = {
    '/srv/stockagent-packed/.local-state/private-history': 'local-tw-index-futures',
    '/srv/stockagent-packed': 'tw-index-futures',
}


class RemoteCapacityWait(Exception):
    """A disposable remote cache miss cannot be staged below its reserve."""


def remote_spool_available(options, remote):
    """One read-only observation per scope; the remote writer remains authority."""
    profile, repo = options.get('ssh_profile'), getattr(remote, 'repo', None)
    if not options.get('remote_spool') or not isinstance(profile, str) or not repo:
        return None
    program = 'import os,sys;s=os.statvfs(sys.argv[1]);print(s.f_bavail*s.f_frsize)'
    cache_root = PurePosixPath(str(repo)) / 'service_data/pack_compute_cache'
    command = 'python3 -c ' + shlex.quote(program) + ' ' + shlex.quote(str(cache_root))
    try:
        result = subprocess.run(['ssh', '-T', '-F', profile, 'stockagent-vast', command],
            capture_output=True, text=True, check=True, timeout=10)
        value = result.stdout.strip()
        return int(value) if value.isascii() and value.isdigit() and len(value) <= 20 else None
    except (OSError, ValueError, subprocess.SubprocessError):
        return None  # Original remote physical admission still applies.


def now():
    return datetime.now(timezone.utc).isoformat()


class NativeUpload:
    """One lazy native reader; upload overlaps other lanes' remote decoding."""
    def __init__(self, path, size, lock, remote_available=None, remote_reserve=None):
        self.path, self.size, self.lock = path, size, lock
        self.remote_available, self.remote_reserve = remote_available, remote_reserve
        self.stack = None
        self.started = self.finished = None
        self.bytes_read, self.eof = 0, False

    def seek(self, offset):
        if offset != 0 or self.stack is not None:
            raise ValueError('native upload supports exactly one full forward read')
        if (type(self.remote_available) is int and type(self.remote_reserve) is int
                and self.remote_available < self.remote_reserve + self.size):
            raise RemoteCapacityWait('remote disposable cache reserve is unavailable')
        from stockagent.data_sync.windows_cold_io import binary_reader
        self.lock.acquire()
        self.started = time.monotonic()
        self.stack = ExitStack()
        try:
            # The remote RPC deadline cannot interrupt source.seek/read. Bound
            # native pipe inactivity too, including a stalled WSL launch that
            # produces neither bytes nor EOF. Only this read-only child aborts.
            self.stream = self.stack.enter_context(binary_reader(
                self.path, expected_bytes=self.size, read_idle_timeout=120))
        except BaseException:
            self.abort(); raise

    def read(self, count):
        block = self.stream.read(count)
        self.bytes_read += len(block)
        self.eof = not block
        return block

    def _finish(self, *, aborted):
        if self.stack is not None:
            stack, self.stack = self.stack, None
            try:
                if aborted:
                    # Inject cancellation into the native reader's context so
                    # it terminates an unread producer instead of waiting for
                    # a child blocked on a full, no-longer-consumed pipe.
                    stack.__exit__(RuntimeError, RuntimeError('native upload cancelled'), None)
                else:
                    stack.close()  # exact EOF, child exit, mount and generation
            finally:
                self.finished = time.monotonic()
                self.lock.release()

    def abort(self):
        self._finish(aborted=True)

    def close(self):
        if self.stack is None:
            return
        if not self.eof or self.bytes_read != self.size:
            self.abort()
            raise ValueError('native upload did not consume exact source and EOF')
        self._finish(aborted=False)


def candidates(cold, since):
    """Only the CAS pack namespace, never a source-data recursive census."""
    root = cold / 'objects/packs'
    if root.is_symlink() or root.resolve() != root:
        raise ValueError('compute source object namespace is redirected')
    result = []
    if str(cold) in ENROLLED_SOURCES:
        from stockagent.data_sync.windows_cold_io import POWERSHELL, _windows_process_environment
        # Enumerate metadata once on NTFS, rather than one WSL/P9 stat for
        # every historical pack. Time is only a discovery hint; selected names
        # still get canonical lstat, full bytes and generation gates below.
        script = r'''$ErrorActionPreference='Stop'
$cfg=[Console]::In.ReadLine() | ConvertFrom-Json
$root=[string]$cfg.root
if($root -notin @('D:\stockagent-cold-primary\packed\.local-state\private-history\objects\packs',
                 'D:\stockagent-cold-primary\packed\objects\packs')){throw 'unenrolled CAS namespace'}
$since=[DateTimeOffset]::FromUnixTimeSeconds([long]$cfg.since).UtcDateTime
$rows=New-Object 'Collections.Generic.List[string]'
foreach($d in ([IO.DirectoryInfo]::new($root)).EnumerateDirectories()) {
 if($d.Name -notmatch '^[a-f0-9]{2}$' -or ($d.Attributes -band [IO.FileAttributes]::ReparsePoint)){continue}
 foreach($f in $d.EnumerateFiles('*.zip')) {
  if($f.Name -match '^[a-f0-9]{64}\.zip$' -and $f.LastWriteTimeUtc -ge $since -and -not ($f.Attributes -band [IO.FileAttributes]::ReparsePoint)){
   $rows.Add($f.Name.Substring(0,64))
   if($rows.Count -gt 65536){throw 'bounded speculative metadata exceeded'}
  }
 }
}
ConvertTo-Json -InputObject @($rows.ToArray()) -Compress
'''
        command = [POWERSHELL, '-NoLogo', '-NoProfile', '-NonInteractive', '-EncodedCommand',
                   base64.b64encode(script.encode('utf-16le')).decode()]
        from stockagent.data_sync.windows_cold_io import windows_path
        value = subprocess.run(command, input=json.dumps({'since': int(since), 'root': windows_path(root)})+'\n',
            text=True, capture_output=True, timeout=60, check=True, env=_windows_process_environment())
        names = json.loads(value.stdout)
        if not isinstance(names, list) or len(names) > 65536 or any(not isinstance(n,str) or re.fullmatch('[a-f0-9]{64}', n) is None for n in names):
            raise ValueError('native speculative object discovery differs')
        paths = [root/name[:2]/(name+'.zip') for name in names]
        with ThreadPoolExecutor(max_workers=16) as pool:
            infos = list(pool.map(lambda p: p.lstat(), paths))
        return sorted([(p,i) for p,i in zip(paths,infos) if stat.S_ISREG(i.st_mode)
                      and 0 < i.st_size <= MAX_OBJECT_BYTES], key=lambda row:(row[1].st_mtime_ns,row[0].name))
    for prefix in sorted(root.iterdir()):
        if re.fullmatch('[0-9a-f]{2}', prefix.name) is None or not prefix.is_dir() or prefix.is_symlink():
            continue
        for path in prefix.iterdir():
            if re.fullmatch('[0-9a-f]{64}\\.zip', path.name) is None or path.stem[:2] != prefix.name:
                continue
            info = path.lstat()
            if (stat.S_ISREG(info.st_mode) and info.st_mtime >= since
                    and 0 < info.st_size <= MAX_OBJECT_BYTES):
                result.append((path, info))
    return sorted(result, key=lambda row: (row[1].st_mtime_ns, row[0].name))


def enrolled_sources(cfg):
    if (cfg.get('contract') != CONTRACT or cfg.get('enabled') is not True
            or type(cfg.get('pressure_recovery_enabled', False)) is not bool
            or type(cfg.get('maximum_jobs_per_cycle')) is not int
            or not 1 <= cfg['maximum_jobs_per_cycle'] <= 256):
        raise ValueError('invalid explicitly enrolled private compute scope')
    extra = cfg.get('additional_sources', [])
    if not isinstance(extra, list) or len(extra) > 1:
        raise ValueError('invalid additional compute sources')
    scopes = [{key: cfg[key] for key in ('cold_root', 'dataset', 'installed_since_utc')}, *extra]
    seen = set()
    for scope in scopes:
        if (not isinstance(scope, dict)
                or set(scope) != {'cold_root', 'dataset', 'installed_since_utc'}
                or ENROLLED_SOURCES.get(scope['cold_root']) != scope['dataset']
                or scope['cold_root'] in seen):
            raise ValueError('unenrolled or duplicate compute source')
        stamp = datetime.fromisoformat(scope['installed_since_utc'])
        if stamp.tzinfo is None:
            raise ValueError('compute source requires an explicit clock offset')
        seen.add(scope['cold_root'])
    return scopes


def scope_context(scope):
    return hashlib.sha256(json.dumps({'contract': CONTRACT, **scope}, sort_keys=True).encode()).hexdigest()


def run_scope(scope, cfg, *, progress):
    cold = Path(scope['cold_root'])
    since = max(datetime.fromisoformat(scope['installed_since_utc']).timestamp(), time.time()-7*86400)
    options = configuration(scope['dataset'], Path(cfg.get('verification_policy', '/etc/stockagent/pack-verification.json')))
    if options is None or options.get('remote_spool') is not True:
        raise ValueError('original verifier has not enrolled disposable disk staging')
    context = scope_context(scope)
    state_path = STATE / ('scope-' + context + '.json')
    if state_path.is_symlink():
        raise ValueError('compute checkpoint is redirected')
    previous = json.loads(state_path.read_bytes()) if state_path.exists() and not state_path.is_symlink() else {}
    # Retain the original v1 checkpoint. Source additions cannot relabel it.
    legacy = STATE / 'state.json'
    if not previous and legacy.is_file() and not legacy.is_symlink():
        original = json.loads(legacy.read_bytes())
        if original.get('context_sha256') == context:
            previous = original
    remote = RemotePacks(options, context)
    completed = previous.get('objects', {}) if previous.get('remote_boot_id') == remote.boot_id and previous.get('context_sha256') == context else {}
    discovery_started = time.monotonic()
    try:
        jobs = candidates(cold, since)
    except BaseException:
        remote.close()
        raise
    current = {path.stem: info for path, info in jobs}
    completed = {key: value for key, value in completed.items()
                 if (key in current and value.get('source_generation') == list(signature(current[key]))
                     and value.get('worker_sha256') == VERIFIER_SHA
                     and value.get('cache_worker_sha256') == CACHE_SHA)}
    previous_retries = previous.get('retry_failures')
    previous_retries = previous_retries if isinstance(previous_retries, dict) else {}
    retries = {key: value for key, value in previous_retries.items()
               if (key in current and key not in completed and isinstance(value, dict)
                   and value.get('source_generation') == list(signature(current[key]))
                   and type(value.get('retry_after_epoch')) in (int, float)
                   and math.isfinite(value['retry_after_epoch'])
                   and value['retry_after_epoch'] <= time.time() + 1020
                   and type(value.get('attempts')) is int and 0 < value['attempts'] <= 1000)}
    pending = [(path, info) for path, info in jobs if path.stem not in completed]
    pending_jobs = [(path, info) for path, info in pending
                    if path.stem not in retries or retries[path.stem]['retry_after_epoch'] <= time.time()]
    need_capacity = bool(pending) or previous.get('pressure_sync_state') == 'failed'
    available = remote_spool_available(options, remote) if need_capacity else None
    reserve = options.get('remote_spool_reserve_bytes', 32*1024**3)
    report = {'contract': CONTRACT, 'context_sha256': context, 'dataset': scope['dataset'], 'remote_boot_id': remote.boot_id,
              'objects': completed, 'source_retirement': False, 'nas_acceptance': False,
              'total_items': len(jobs), 'total_bytes': sum(info.st_size for _, info in jobs),
              'workers': options['remote_workers'], 'source_readers': 1, 'active_jobs': 0,
              'computed_this_turn': 0,
              'failures': {}, 'retry_failures': retries, 'state': 'running',
              'remote_spool_available_bytes': available, 'remote_spool_reserve_bytes': reserve,
              'capacity_wait': False}
    for metric in ('bytes', 'items'):
        prior = previous.get('pressure_reclaimed_total_' + metric,
                             previous.get('pressure_reclaimed_' + metric, 0))
        report['pressure_reclaimed_total_' + metric] = prior if type(prior) is int and prior >= 0 else 0
    if previous.get('pressure_sync_state') in {'requested', 'not_required', 'failed'}:
        report['pressure_sync_state'] = previous['pressure_sync_state']
    report['discovery_seconds'] = time.monotonic()-discovery_started
    report['inventory_observed_at_utc'] = now()
    def save():
        report.update(observed_at_utc=now(), completed_items=len(completed),
                      completed_bytes=sum(v['object_bytes'] for v in completed.values()))
        atomic_write_json(state_path, report, durable=True)
        progress({**{k:v for k,v in report.items() if k not in {'objects', 'failures', 'retry_failures', 'cleanup'}},
                  'failed_items': len(report['failures']),
                  'retry_deferred_items': sum(item['retry_after_epoch'] > time.time() for item in retries.values())})
    # Recover only disposable Vast compute-cache pairs whose original D bytes
    # are freshly checked against an independently reconstructed fixed release.
    # Keep this on the original spool owner; unknown remote outcomes block any
    # further automatic reclamation rather than pretending an unlink succeeded.
    scan_attempted = False
    if (cfg.get('pressure_recovery_enabled') is True and type(available) is int
            and available < reserve and pending and completed):
        from stockagent.data_sync.pack_compute_pressure_control import relieve
        from stockagent.data_sync.pack_compute_pressure import SOURCE_SHA256 as PRESSURE_SHA
        retry_at = previous.get('pressure_retry_after_epoch')
        deferred = (type(retry_at) in (int, float) and math.isfinite(retry_at)
                    and time.time() < retry_at <= time.time() + 300
                    and previous.get('pressure_retry_worker_sha256') == PRESSURE_SHA)
        def pressure_progress(value):
            report['pressure_state'] = value['phase']
            report['pressure_done_bytes'] = value['done_bytes']
            report['pressure_total_bytes'] = value['total_bytes']
            report['pressure_plan'] = value.get('plan')
            save()
        if deferred:
            report['pressure_state'] = previous.get('pressure_state', 'waiting_retry')
            report['pressure_retry_after_epoch'] = retry_at
            report['pressure_retry_worker_sha256'] = PRESSURE_SHA
            report['capacity_wait'] = True
        else:
            try:
                result = relieve(scope, current, completed, options, remote, STATE,
                                 available=available, progress=pressure_progress)
                report['pressure_state'] = result['state']
                removed = result.get('removed', [])
                for row in removed:
                    completed[row['object_sha256']]['payload_cache_retired'] = True
                report['pressure_reclaimed_bytes'] = sum(row['released_allocated_bytes'] for row in removed)
                report['pressure_reclaimed_items'] = len(removed)
                report['pressure_reclaimed_total_bytes'] += report['pressure_reclaimed_bytes']
                report['pressure_reclaimed_total_items'] += report['pressure_reclaimed_items']
                available = remote_spool_available(options, remote)
                report['remote_spool_available_bytes'] = available
                report['capacity_wait'] = type(available) is int and available < reserve
                if removed and type(available) is int and available >= 16 * 1024**3:
                    from stockagent.data_sync.pack_compute_pressure_control import retry_vast_syncthing_scan
                    report['pressure_sync_state'] = retry_vast_syncthing_scan(options)
                    scan_attempted = True
                if not removed and report['capacity_wait'] and result['state'] != 'waiting_for_tw_opening':
                    report['pressure_retry_after_epoch'] = time.time() + 300
                    report['pressure_retry_worker_sha256'] = PRESSURE_SHA
            except Exception as error:
                report['pressure_state'] = 'blocked_error'
                report['pressure_error_type'] = type(error).__name__
                report['capacity_wait'] = True
                report['pressure_retry_after_epoch'] = time.time() + 300
                report['pressure_retry_worker_sha256'] = PRESSURE_SHA
        save()
    if (cfg.get('pressure_recovery_enabled') is True and not scan_attempted
            and report.get('pressure_sync_state') == 'failed'
            and type(available) is int and available >= 16 * 1024**3):
        from stockagent.data_sync.pack_compute_pressure_control import retry_vast_syncthing_scan
        report['pressure_sync_state'] = retry_vast_syncthing_scan(options)
        save()
    io_lock = threading.Lock()
    def compute(path, before):
        item = {'kind': 'pack', 'sha256': path.stem, 'bytes': before.st_size}
        source = NativeUpload(path, before.st_size, io_lock, available, reserve)
        began = time.monotonic()
        try: result = remote.stage(source, item, on_payload_sent=source.close)
        finally: source.abort()
        if signature(path.lstat()) != signature(before):
            raise ValueError('closed D pack generation changed during staging')
        finished = time.monotonic()
        return {**result, 'source_generation': list(signature(before)), 'computed_at_utc': now(),
                'complete_roundtrip_seconds': finished-began,
                'native_read_upload_seconds': source.finished-source.started if source.finished else 0,
                'after_upload_seconds': finished-source.finished if source.finished else 0,
                'acceptance': False, 'source_retirement': False}
    save()
    try:
        with ThreadPoolExecutor(max_workers=options['remote_workers'], thread_name_prefix='stage-closed-pack') as pool:
            iterator = iter(pending_jobs[:cfg['maximum_jobs_per_cycle']])
            active = {}
            circuit_open = False
            def dispatch():
                if circuit_open:
                    return
                for path, before in iterator:
                    active[pool.submit(compute, path, before)] = path.stem
                    return
            for _ in range(options['remote_workers']): dispatch()
            while active:
                report['active_jobs'] = len(active)
                save()
                done, _ = wait(active, timeout=10, return_when=FIRST_COMPLETED)
                for future in done:
                    digest = active.pop(future)
                    try:
                        completed[digest] = future.result()
                        retries.pop(digest, None)
                        report['computed_this_turn'] += 1
                    except Exception as exc:
                        report['failures'][digest] = type(exc).__name__
                        report['capacity_wait'] = report['capacity_wait'] or isinstance(exc, RemoteCapacityWait)
                        prior = retries.get(digest, {})
                        attempts = min(1000, int(prior.get('attempts', 0)) + 1)
                        retries[digest] = {'source_generation': list(signature(current[digest])),
                            'attempts': attempts, 'error_type': type(exc).__name__,
                            'retry_after_epoch': time.time() + min(900, 60 * 2**min(attempts - 1, 4))}
                        # RemotePacks returns aborted sessions to its shared
                        # queue to unblock waiters. They cannot service later
                        # jobs in this turn; keep the original checkpoint and
                        # let the next timer create fresh sessions instead.
                        if any(getattr(session, 'closed', False) for session in getattr(remote, 'sessions', ())):
                            circuit_open = True
                    dispatch()
                save()
        report['active_jobs'] = 0
        report['state'] = ('blocked' if report['failures'] or (retries and not pending_jobs) else 'queued'
                           if len(completed) < len(jobs) else 'waiting_for_new_sealed_packs')
    finally:
        report['cleanup'] = remote.close()
        save()


def aggregate_status(reports):
    rows = list(reports.values())
    state = ('running' if any(row['state'] == 'running' for row in rows) else
             'blocked' if any(row['state'] == 'blocked' for row in rows) else
             'queued' if any(row['state'] == 'queued' for row in rows) else
             'waiting_for_new_sealed_packs')
    result = {'contract': CONTRACT, 'state': state, 'observed_at_utc': now(),
              'scopes': rows, 'source_retirement': False, 'nas_acceptance': False}
    for key in ('completed_items', 'total_items', 'completed_bytes', 'total_bytes', 'active_jobs', 'failed_items'):
        result[key] = sum(row.get(key, 0) for row in rows if type(row.get(key, 0)) is int)
    return result


def run_sources(cfg, scopes):
    reports = {}
    previous_path = STATE / 'status.json'
    if previous_path.is_symlink():
        raise ValueError('compute status is redirected')
    previous = json.loads(previous_path.read_bytes()) if previous_path.exists() else {}
    for scope in scopes:
        context = scope_context(scope)
        prior = next((row for row in previous.get('scopes', [previous])
                      if row.get('context_sha256') == context), {})
        reports[context] = {**prior, 'context_sha256': context, 'dataset': scope['dataset'],
                            'state': 'queued', 'active_jobs': 0}
    def save(report):
        reports[report['context_sha256']] = report
        atomic_write_json(STATE/'status.json', aggregate_status(reports), durable=False)
    for scope in scopes:
        try:
            run_scope(scope, cfg, progress=save)
        except Exception as error:
            # Preserve this scope's checkpoint and failure while serving its
            # independently enrolled neighbor through the same queue owner.
            save({**reports[scope_context(scope)], 'state': 'blocked', 'active_jobs': 0,
                  'observed_at_utc': now(), 'error_type': type(error).__name__})
    return aggregate_status(reports)


def drain_sources(load_configuration):
    """Rotate bounded source turns immediately while real ready work advances.

    The minute timer is for discovery/blocked retries, not an idle gap in a
    nonempty queue. Reload explicit enrollment at each complete turn. Failure
    or no progress yields to that same timer rather than spinning or forcing
    another owner. Each remote request keeps its established deadline.
    """
    while True:
        cfg = load_configuration()
        report = run_sources(cfg, enrolled_sources(cfg))
        if (not any(row['state'] == 'queued' for row in report['scopes'])
                or not any(row.get('computed_this_turn', 0) for row in report['scopes'])):
            return report


def load_configuration(path):
    info = path.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077):
        raise ValueError('spool enrollment must be owned private metadata')
    cfg = json.loads(path.read_bytes())
    enrolled_sources(cfg)
    return cfg


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('/etc/stockagent/pack-compute-spool.json'))
    args = parser.parse_args()
    load_configuration(args.config)
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    _check_d_primary_mount(Path('/srv/stockagent-packed'))
    if STATE.is_symlink() or STATE.resolve() != STATE:
        raise ValueError('local compute queue is redirected')
    STATE.mkdir(mode=0o700, parents=True, exist_ok=True)
    if STATE.stat().st_mode & 0o077:
        raise ValueError('local compute queue must be private')
    lockfd = os.open(STATE/'owner.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(lockfd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        print(json.dumps(drain_sources(lambda: load_configuration(args.config)), ensure_ascii=False))
    finally:
        os.close(lockfd)


if __name__ == '__main__':
    main()
