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
import os
from pathlib import Path
import re
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


def now():
    return datetime.now(timezone.utc).isoformat()


class NativeUpload:
    """One lazy native reader; upload overlaps other lanes' remote decoding."""
    def __init__(self, path, size, lock):
        self.path, self.size, self.lock = path, size, lock
        self.stack = None
        self.started = self.finished = None

    def seek(self, offset):
        if offset != 0 or self.stack is not None:
            raise ValueError('native upload supports exactly one full forward read')
        from stockagent.data_sync.windows_cold_io import binary_reader
        self.lock.acquire()
        self.started = time.monotonic()
        self.stack = ExitStack()
        try:
            self.stream = self.stack.enter_context(binary_reader(self.path, expected_bytes=self.size))
        except BaseException:
            self.close(); raise

    def read(self, count):
        return self.stream.read(count)

    def close(self):
        if self.stack is not None:
            stack, self.stack = self.stack, None
            try: stack.close()  # exact EOF, child exit, mount and generation
            finally:
                self.finished = time.monotonic()
                self.lock.release()


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
    pending_jobs = [(path, info) for path, info in jobs if path.stem not in completed]
    report = {'contract': CONTRACT, 'context_sha256': context, 'dataset': scope['dataset'], 'remote_boot_id': remote.boot_id,
              'objects': completed, 'source_retirement': False, 'nas_acceptance': False,
              'total_items': len(jobs), 'total_bytes': sum(info.st_size for _, info in jobs),
              'workers': options['remote_workers'], 'source_readers': 1, 'active_jobs': 0,
              'computed_this_turn': 0,
              'failures': {}, 'state': 'running'}
    report['discovery_seconds'] = time.monotonic()-discovery_started
    report['inventory_observed_at_utc'] = now()
    def save():
        report.update(observed_at_utc=now(), completed_items=len(completed),
                      completed_bytes=sum(v['object_bytes'] for v in completed.values()))
        atomic_write_json(state_path, report, durable=True)
        progress({**{k:v for k,v in report.items() if k not in {'objects', 'failures', 'cleanup'}},
                  'failed_items': len(report['failures'])})
    io_lock = threading.Lock()
    def compute(path, before):
        item = {'kind': 'pack', 'sha256': path.stem, 'bytes': before.st_size}
        source = NativeUpload(path, before.st_size, io_lock)
        began = time.monotonic()
        try: result = remote.stage(source, item, on_payload_sent=source.close)
        finally: source.close()
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
            def dispatch():
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
                        report['computed_this_turn'] += 1
                    except Exception as exc: report['failures'][digest] = type(exc).__name__
                    dispatch()
                save()
        report['active_jobs'] = 0
        report['state'] = ('blocked' if report['failures'] else 'queued'
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
