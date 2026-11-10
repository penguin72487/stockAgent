"""Exact pack reconstruction shared by local and private Vast compute lanes.

The canonical verifier still owns inventory, object generations and acceptance.
Workers return compact ordered SHA commitments, never an authorization to unlink
source data. The remote side runs this same fixed standard-library program.
"""
from __future__ import annotations

import base64
from collections import deque
import configparser
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import re
import queue
import selectors
import shlex
import stat
import subprocess
import signal
import sys
import threading
import time
import uuid
import zipfile

CONTRACT = 'exact_pack_reconstruction_lanes_v1'
POLICY = Path('/etc/stockagent/pack-verification.json')
SOURCE_SHA256 = (hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
                 if '__file__' in globals() else globals().get('WORKER_SHA256'))


def commitment(rows):
    """Unambiguous ordered framing; use actual hashes for reconstructed leaves."""
    result = hashlib.sha256()
    for row in rows:
        value = json.dumps(row, ensure_ascii=True, separators=(',', ':')).encode()
        result.update(len(value).to_bytes(8, 'big'))
        result.update(value)
    return result.hexdigest()


def expected_result(item, rows):
    return {'contract': CONTRACT, 'object_sha256': item['sha256'],
            'object_bytes': item['bytes'], 'files': len(rows),
            'logical_bytes': sum(row[1] for row in rows),
            'originals_sha256': commitment(rows)}


def inspect_pack(source, item, rows):
    """Full object SHA, all stored CRCs and every selected original's SHA/size."""
    # An early sealed-object computation has no published inventory yet. Its
    # commitment is useful only after the original owner binds the exact rows.
    unbound = rows is None
    wanted = None if unbound else expected_result(item, rows)
    names = None if unbound else [row[0] for row in rows]
    if names is not None and (len(set(names)) != len(names) or names != sorted(names)):
        raise ValueError('pack expectations are not unique and ordered')
    digest, count = hashlib.sha256(), 0
    source.seek(0)
    while block := source.read(8 * 1024**2):
        count += len(block)
        digest.update(block)
    if count != item['bytes'] or digest.hexdigest() != item['sha256']:
        raise ValueError('pack object SHA or size differs')
    source.seek(0)
    actual_rows = []
    current = iter(rows or ())
    row = next(current, None)
    with zipfile.ZipFile(source) as archive:
        stored = archive.namelist()
        if len(set(stored)) != len(stored):
            raise ValueError('duplicate stored pack member')
        if unbound:
            if (stored != sorted(stored) or len(stored) > 32768
                    or sum(info.file_size for info in archive.infolist()) > 128*1024**3):
                raise ValueError('unbound pack members are not ordered or bounded')
        elif item.get('member_selection') == 'subset':
            selected = iter(names)
            name = next(selected, None)
            for stored_name in stored:
                if stored_name == name:
                    name = next(selected, None)
            if name is not None:
                raise ValueError('pack does not contain the ordered inventory subset')
        elif stored != names:
            raise ValueError('pack members differ from inventory')
        if not unbound and len(stored) != int(item.get('stored_file_count', item['file_count'])):
            raise ValueError('stored pack member count differs')
        for name in stored:
            selected = unbound or (row is not None and row[0] == name)
            digest, size = hashlib.sha256(), 0
            # Reading every member through EOF checks CRC, including old
            # members outside a current subset. No extraction or second decode.
            with archive.open(name) as member:
                while block := member.read(8 * 1024**2):
                    if selected:
                        digest.update(block)
                        size += len(block)
            if selected:
                actual_rows.append([name, size, digest.hexdigest()])
                if not unbound:
                    row = next(current, None)
    if row is not None:
        raise ValueError('incomplete original pack reconstruction')
    actual = expected_result(item, actual_rows)
    if not unbound and actual != wanted:
        raise ValueError('original pack SHA or size differs')
    return {**actual, 'stored_members_crc_verified': len(stored),
            'worker_sha256': SOURCE_SHA256, 'inventory_bound': not unbound}


def _memory_headroom():
    values = {line.split(':')[0]: int(line.split()[1])*1024
              for line in Path('/proc/meminfo').read_text().splitlines()}
    available = values['MemAvailable']
    maximum, current = Path('/sys/fs/cgroup/memory.max'), Path('/sys/fs/cgroup/memory.current')
    if maximum.is_file() and current.is_file() and maximum.read_text().strip() != 'max':
        available = min(available, int(maximum.read_text())-int(current.read_text()))
    return max(0, available)


def _read_blocking(stream, count):
    result = bytearray()
    while len(result) < count:
        block = stream.read(min(count-len(result), 8*1024**2))
        if not block:
            raise EOFError('incomplete verification frame')
        result.extend(block)
    return bytes(result)


def _write_frame(stream, value):
    body = json.dumps(value, separators=(',', ':')).encode()
    stream.write(len(body).to_bytes(8, 'big'))
    stream.write(body)
    stream.flush()


def _remote_stream():
    """A fixed stdlib compute RPC over SSH, with no payload file or new port."""
    incoming, outgoing = sys.stdin.buffer, sys.stdout.buffer
    boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    identity = {'contract': CONTRACT, 'boot_id': boot, 'worker_sha256': SOURCE_SHA256}
    _write_frame(outgoing, {**identity, 'state': 'ready', 'memory_headroom_bytes': _memory_headroom()})
    manifest = None
    while True:
        size = int.from_bytes(_read_blocking(incoming, 8), 'big')
        if not 0 < size <= 64*1024**2:
            raise ValueError('verification request is not bounded metadata')
        request = json.loads(_read_blocking(incoming, size))
        if request.get('boot_id') != boot or request.get('worker_sha256') != SOURCE_SHA256:
            raise ValueError('verification stream identity differs')
        if request.get('mode') == 'stop':
            _write_frame(outgoing, {**identity, 'state': 'memory_worker_closed'})
            return
        if manifest is None:
            manifest = request['manifest_sha256']
        if manifest != request.get('manifest_sha256'):
            raise ValueError('verification stream changed its fixed manifest')
        item, rows = request['item'], request['rows']
        if (request.get('mode') != 'verify' or item.get('kind') != 'pack'
                or type(item.get('bytes')) is not int or item['bytes'] < 0
                or re.fullmatch(r'[0-9a-f]{32}', request['job_id']) is None
                or not 1 <= request['timeout_seconds'] <= 1800):
            raise ValueError('invalid exact pack request')
        if _memory_headroom() < 4*item['bytes'] + len(rows)*1024 + 128*1024**2:
            raise ValueError('remote verification memory unavailable')
        # This alarm belongs only to this disposable CPU worker. It bounds
        # truncated sends/decodes after a client disappears; no service quota.
        signal.setitimer(signal.ITIMER_REAL, request['timeout_seconds'])
        try:
            with io.BytesIO() as payload:
                remaining = item['bytes']
                while remaining:
                    block = incoming.read(min(remaining, 8*1024**2))
                    if not block:
                        raise EOFError('incomplete exact pack payload')
                    payload.write(block)
                    remaining -= len(block)
                started = time.monotonic()
                result = inspect_pack(payload, item, rows)
            # Drop the only payload buffer before returning or reading a job.
            _write_frame(outgoing, {**result, **identity, 'job_id': request['job_id'],
                         'manifest_sha256': manifest, 'payload_storage': 'anonymous_memory',
                         'verification_seconds': time.monotonic()-started})
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)


def _pipe_io(fd, *, data=None, count=None, deadline):
    """Deadline-aware nonblocking pipes; a dead peer cannot block a pool lane."""
    sending = data is not None
    target = memoryview(data) if sending else bytearray()
    offset = 0
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(fd, selectors.EVENT_WRITE if sending else selectors.EVENT_READ)
            while offset < (len(target) if sending else count):
                remaining = deadline-time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    raise TimeoutError('private pack verification deadline expired')
                try:
                    if sending:
                        offset += os.write(fd, target[offset:offset+1024**2])
                    else:
                        block = os.read(fd, min(count-offset, 1024**2))
                        if not block:
                            raise EOFError('verification worker closed before its receipt')
                        target.extend(block)
                        offset += len(block)
                except BlockingIOError:
                    continue
    finally:
        if sending:
            target.release()  # Even a retained timeout traceback cannot pin BytesIO.
    return None if sending else bytes(target)


class StreamSession:
    def __init__(self, command, timeout):
        self.timeout = timeout
        self.closed = False
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, bufsize=0)
        for stream in (self.process.stdin, self.process.stdout):
            os.set_blocking(stream.fileno(), False)
        try:
            self.identity = self.read(time.monotonic()+timeout)
            if (not isinstance(self.identity, dict) or self.identity.get('state') != 'ready' or self.identity.get('worker_sha256') != SOURCE_SHA256
                    or self.identity.get('contract') != CONTRACT):
                raise ValueError('remote verification handshake differs')
        except BaseException:
            self.abort()
            raise

    def read(self, deadline):
        fd = self.process.stdout.fileno()
        size = int.from_bytes(_pipe_io(fd, count=8, deadline=deadline), 'big')
        if not 0 < size <= 64*1024:
            raise ValueError('verification receipt exceeds its bounded contract')
        value = json.loads(_pipe_io(fd, count=size, deadline=deadline))
        if not isinstance(value, dict):
            raise ValueError('verification receipt is not an object')
        return value

    def request(self, value, source=None, *, on_payload_sent=None):
        deadline = time.monotonic()+self.timeout
        body = json.dumps(value, separators=(',', ':')).encode()
        if len(body) > 64*1024**2:
            raise ValueError('verification control exceeds its bounded frame')
        fd = self.process.stdin.fileno()
        _pipe_io(fd, data=len(body).to_bytes(8, 'big')+body, deadline=deadline)
        if source is not None:
            source.seek(0)
            if isinstance(source, io.BytesIO):
                with source.getbuffer() as payload:
                    if len(payload) != value['item']['bytes']:
                        raise ValueError('verified source buffer length differs')
                    _pipe_io(fd, data=payload, deadline=deadline)
            else:
                total = 0
                while block := source.read(8*1024**2):
                    total += len(block)
                    _pipe_io(fd, data=block, deadline=deadline)
                if total != value['item']['bytes']:
                    raise ValueError('verified source stream length differs')
        if on_payload_sent is not None:
            on_payload_sent()  # Finish native EOF/generation gates before reply.
        return self.read(deadline)

    def abort(self):
        if self.closed:
            return
        self.closed = True
        if self.process.poll() is None:
            self.process.kill()
        self.process.communicate()

    def close(self):
        if self.closed:
            return {'state': 'worker_aborted'}
        result = self.request({'mode': 'stop', 'boot_id': self.identity['boot_id'], 'worker_sha256': SOURCE_SHA256})
        if result.get('state') != 'memory_worker_closed' or result.get('boot_id') != self.identity['boot_id']:
            raise ValueError('memory worker shutdown receipt differs')
        self.process.stdin.close()
        self.process.wait(timeout=5)
        if self.process.returncode:
            raise ValueError('memory worker exited unsuccessfully')
        self.process.stdout.close()
        self.process.stderr.close()
        self.closed = True
        return result


def configuration(dataset, path=POLICY):
    if not path.exists() and not path.is_symlink():
        return None
    value = path.lstat()
    if (not stat.S_ISREG(value.st_mode) or value.st_uid != os.geteuid()
            or value.st_mode & 0o077):
        raise ValueError('pack verification policy must be private and owned')
    raw = path.read_bytes()
    data = json.loads(raw)
    if data.get('schema_version') != 1 or type(data.get('enabled')) is not bool:
        raise ValueError('invalid pack verification policy')
    if (not isinstance(data.get('datasets'), list)
            or any(not isinstance(name, str) for name in data['datasets'])
            or type(data['local_workers']) is not int or data['local_workers'] < 1
            or type(data['remote_workers']) is not int or not 1 <= data['remote_workers'] <= 32
            or type(data.get('remote_spool', False)) is not bool
            or type(data.get('remote_spool_reserve_bytes', 32*1024**3)) is not int
            or data.get('remote_spool_reserve_bytes', 32*1024**3) < 1024**3
            or not 30 <= data['timeout_seconds'] <= 1800
            or any(type(data.get(key)) not in (int, float) or data[key] <= 0 for key in
                   ('initial_local_bytes_per_second', 'initial_remote_bytes_per_second'))):
        raise ValueError('invalid pack verification admission')
    if not data['enabled'] or dataset not in data['datasets']:
        return None
    return {**data, 'policy_sha256': hashlib.sha256(raw).hexdigest(), 'policy_path': str(path)}


class RemotePacks:
    """Private memory RPC on the enrolled endpoint; cold transport stays separate."""
    def __init__(self, options, manifest_sha256):
        from stockagent.data_sync.rclone_edge_transport import configuration as transport_configuration
        from stockagent.remote_ssh import validate_remote_absolute
        self.options = options
        self.transport = transport_configuration(Path(options['transport_policy']))
        if self.transport is None:
            raise ValueError('enrolled SFTP transport unavailable')
        profile = Path(options['ssh_profile'])
        if (profile.is_symlink() or not profile.is_file() or profile.stat().st_mode & 0o077
                or profile.stat().st_uid != os.geteuid()):
            raise ValueError('verification SSH profile must be private')
        connection = configparser.ConfigParser(interpolation=None)
        connection.read(self.transport['rclone_config'])
        sftp = connection[self.transport['remote_name']]
        resolved = subprocess.run(['ssh', '-G', '-F', str(profile), 'stockagent-vast'],
                                  capture_output=True, text=True, check=True, timeout=15)
        ssh = dict(line.split(' ', 1) for line in resolved.stdout.splitlines() if ' ' in line)
        if (ssh.get('hostname') != sftp['host'] or ssh.get('user') != sftp['user']
                or ssh.get('port') != sftp['port'] or ssh.get('identityfile') != sftp['key_file']
                or ssh.get('userknownhostsfile') != sftp['known_hosts_file']
                or ssh.get('stricthostkeychecking') not in ('yes', 'true')):
            raise ValueError('verification SSH and enrolled SFTP endpoints differ')
        self.source = Path(__file__).read_bytes()
        if hashlib.sha256(self.source).hexdigest() != SOURCE_SHA256:
            raise ValueError('pack worker changed after import')
        repo = validate_remote_absolute(self.transport['remote_repo_root'], 'remote repository')
        self.root = None  # No remote payload or scratch pathname is created.
        self.repo, self.boot_id, self.disabled = repo, None, threading.Event()
        self.manifest_sha256 = manifest_sha256
        encoded = base64.b64encode(self.source).decode('ascii')
        bootstrap = ("import base64,hashlib; s=base64.b64decode("+repr(encoded)+
                     "); exec(compile(s,'<fixed-pack-worker>','exec'),"
                     "{'__name__':'__main__','WORKER_SHA256':hashlib.sha256(s).hexdigest()})")
        self.spool = bool(options.get('remote_spool', False))
        if self.spool:
            from stockagent.data_sync import pack_compute_cache
            bootstrap = pack_compute_cache.bootstrap(self.source, self.repo,
                options.get('remote_spool_reserve_bytes', 32*1024**3))
        command = ('cd '+shlex.quote(self.repo)+' && source scripts/runtime_env.sh && '
                   'run_fintech_python -c '+shlex.quote(bootstrap))
        self.sessions = []
        self.available = queue.Queue()
        try:
            for _ in range(options['remote_workers']):
                session = StreamSession(['ssh', '-T', '-F', options['ssh_profile'], 'stockagent-vast', command],
                                        min(15, options['timeout_seconds']))
                session.timeout = options['timeout_seconds']
                self.sessions.append(session)
                if self.boot_id is None:
                    self.boot_id = session.identity['boot_id']
                if session.identity.get('boot_id') != self.boot_id:
                    raise ValueError('verification worker boots differ')
                if self.spool and session.identity.get('cache_worker_sha256') != pack_compute_cache.SOURCE_SHA256:
                    raise ValueError('remote temporary cache implementation differs')
                self.available.put(session)
        except BaseException:
            for session in self.sessions:
                session.abort()
            raise

    def verify(self, source, item, rows):
        job = uuid.uuid4().hex
        args = {'mode': 'verify', 'job_id': job, 'item': item, 'rows': rows,
                'manifest_sha256': self.manifest_sha256, 'boot_id': self.boot_id,
                'worker_sha256': SOURCE_SHA256, 'timeout_seconds': self.options['timeout_seconds']}
        started = time.monotonic()
        try:
            session = self.available.get(timeout=self.options['timeout_seconds'])
        except queue.Empty as exc:
            raise TimeoutError('private verification worker unavailable') from exc
        try:
            result = session.request({**args, 'mode': 'cached_verify'}) if getattr(self, 'spool', False) else None
            if result is None or result.get('state') == 'cache_miss':
                if source is None:
                    raise ValueError('remote sealed object cache miss')
                result = session.request(args, source)
            expected = expected_result(item, rows)
            if (any(result.get(k) != v for k, v in expected.items())
                    or result.get('worker_sha256') != SOURCE_SHA256
                    or result.get('boot_id') != self.boot_id
                    or result.get('manifest_sha256') != self.manifest_sha256
                    or result.get('job_id') != job
                    or (getattr(self, 'spool', False) and result.get('inventory_bound') is not True)
                    or result.get('stored_members_crc_verified') != int(item.get('stored_file_count', item['file_count']))):
                raise ValueError('remote reconstruction commitment differs')
            return {**result, 'roundtrip_seconds': time.monotonic()-started,
                    'execution_node': 'vast',
                    'remote_files_created': result.get('remote_files_created', 0)}
        except BaseException:
            session.abort()
            raise
        finally:
            self.available.put(session)

    def stage(self, source, item, *, on_payload_sent=None):
        """Speculative closed-object compute; never a recovery/retirement proof."""
        if not self.spool:
            raise ValueError('remote staging is not explicitly enrolled')
        session = self.available.get(timeout=self.options['timeout_seconds'])
        try:
            args = {'mode': 'stage', 'job_id': uuid.uuid4().hex, 'item': item,
                    'manifest_sha256': self.manifest_sha256, 'boot_id': self.boot_id,
                    'worker_sha256': SOURCE_SHA256, 'timeout_seconds': self.options['timeout_seconds']}
            result = session.request({**args, 'mode': 'stage_lookup'})
            if result.get('state') == 'cache_miss':
                result = session.request(args, source, on_payload_sent=on_payload_sent)
            if (result.get('object_sha256') != item['sha256'] or result.get('object_bytes') != item['bytes']
                    or result.get('worker_sha256') != SOURCE_SHA256 or result.get('boot_id') != self.boot_id
                    or result.get('job_id') != args['job_id'] or result.get('inventory_bound') is not False):
                raise ValueError('unbound remote computation identity differs')
            return result
        except BaseException:
            session.abort()
            raise
        finally:
            self.available.put(session)

    def close(self):
        results = []
        for session in self.sessions:
            try:
                results.append(session.close())
            except (OSError, ValueError, EOFError, TimeoutError, subprocess.SubprocessError) as exc:
                session.abort()
                results.append({'state': 'worker_aborted', 'reason': type(exc).__name__})
        return {'state': 'memory_workers_finished', 'workers': results,
                'payload_storage': 'private_disposable_disk' if self.spool else 'anonymous_memory'}


def verify_pack_jobs(sync_root, resolved, entries, options, *, native, progress=None):
    """Work stealing with one in-flight job per lane; no ordered-head blocking.

    SQLite inventory reads stay on the calling thread. D buffer fills serialize;
    decode/hash and encrypted memory RPC overlap after a full buffer SHA. Workers
    never retain a raw tree, invoke a publisher or write an acceptance receipt.
    """
    from stockagent.data_sync import packed_snapshots as packed
    from stockagent.data_sync.packed_verification_sources import PackSources
    from downloader.artifact_io import atomic_write_json

    packs = [item for item in resolved.manifest['archive']['objects'] if item['kind'] == 'pack']
    if not packs:
        return {}, {'contract': CONTRACT, 'jobs': 0, 'nodes': {}}
    evidence = Path(options['evidence_directory'])
    evidence.mkdir(mode=0o700, parents=True, exist_ok=False)
    started = time.monotonic()
    io_lock = threading.Lock()
    sources = PackSources(sync_root, packs, native=native)
    remote = None
    remote_error = None
    try:
        remote = RemotePacks(options, resolved.manifest_sha256)
    except (OSError, ValueError, EOFError, RuntimeError, subprocess.SubprocessError) as exc:
        remote_error = type(exc).__name__
    jobs = deque(packs)
    proofs, completed_files, completed_bytes = {}, 0, 0
    lanes = ['local'] * options['local_workers'] + (['vast'] * options['remote_workers'] if remote else [])

    def report(pending=()):
        if progress is not None:
            progress({'stage': 'parallel-pack-reconstruction',
                      'completed_items': completed_files,
                      'total_items': int(resolved.manifest['source']['files']),
                      'completed_bytes': completed_bytes,
                      'total_bytes': int(resolved.manifest['source']['logical_bytes']),
                      'verified_objects': len(proofs),
                      'total_objects': len(resolved.manifest['archive']['objects']),
                      'verified_object_bytes': sum(p['object_bytes'] for p in proofs.values()),
                      'total_object_bytes': sum(p['bytes'] for p in resolved.manifest['archive']['objects']),
                      'pack_jobs_completed': len(proofs), 'pack_jobs_total': len(packs),
                      'pending_local_packs': sum(lane == 'local' for lane, _ in pending),
                      'pending_remote_packs': sum(lane == 'vast' for lane, _ in pending),
                      'worker_nodes': {node: sum(p['execution_node'] == node for p in proofs.values())
                                       for node in set(lanes)}, 'acceptance': False})

    def execute(item, rows, lane):
        path = packed._read_object_path(sync_root, item['relpath'])
        info = path.lstat()
        before = (packed._source_stat(info), info.st_nlink)
        begin = time.monotonic()
        failure = None
        with contextlib.ExitStack() as stack:
            # Only the fill is serialized. Generation fences remain alive
            # until that exact worker finishes; no new global inode/owner lock.
            io_lock.acquire()
            try:
                source = stack.enter_context(sources.open(item))
                if isinstance(source, Path):
                    # Low-memory/SSD fallback seeks D; keep its original
                    # single-reader behavior and do not upload from D again.
                    source = stack.enter_context(source.open('rb'))
                    if lane == 'vast' and remote is not None and getattr(remote, 'spool', False):
                        # Large objects need not fit the bridge RAM buffer. A
                        # staged remote computation still requires a fresh full
                        # SHA of D's authority plus unchanged generations here.
                        from stockagent.data_sync.windows_cold_io import hash_file
                        if hash_file(path) != item['sha256']:
                            raise packed.SnapshotError('native cold pack SHA differs')
                        try:
                            result = remote.verify(None, item, rows)
                        except (OSError, ValueError, EOFError, RuntimeError, subprocess.SubprocessError) as exc:
                            failure = type(exc).__name__
                            remote.disabled.set()
                            result = inspect_pack(source, item, rows)
                            lane = 'local'
                    else:
                        result = inspect_pack(source, item, rows)
                        lane = 'local'
                else:
                    result = None
            finally:
                io_lock.release()
            if result is None:
                if lane == 'vast' and not remote.disabled.is_set():
                    try:
                        result = remote.verify(source, item, rows)
                    except (OSError, ValueError, EOFError, RuntimeError, subprocess.SubprocessError) as exc:
                        failure = type(exc).__name__
                        remote.disabled.set()
                if result is None:
                    result = inspect_pack(source, item, rows)
                    lane = 'local'
        info = path.lstat()
        if (packed._source_stat(info), info.st_nlink) != before:
            raise packed.SnapshotError('cold pack generation changed during compute delegation')
        return {**result, 'execution_node': lane, 'remote_failure': failure,
                'complete_seconds': time.monotonic()-begin}

    with ThreadPoolExecutor(max_workers=len(lanes), thread_name_prefix='exact-pack') as executor:
        pending = {}
        rates = {'local': options.get('initial_local_bytes_per_second', 25*1024**2),
                 'vast': options.get('initial_remote_bytes_per_second', 8*1024**2)}
        def dispatch(lane):
            if not jobs:
                return
            # Stop handing out remote work before the tail. A busy slower
            # remote lane must not become the last whole-batch dependency.
            if lane == 'vast' and len(packs) > len(lanes):
                local_remaining = sum(row['bytes'] for row in jobs) - jobs[0]['bytes']
                if local_remaining/rates['local'] < jobs[0]['bytes']/rates['vast']:
                    return
            item = jobs.popleft()
            rows = [[row['path'], row['size'], row['sha256']]
                    for row in entries.object_rows(item['sha256'])]
            pending[executor.submit(execute, item, rows, lane)] = (lane, item)
        try:
            for lane in lanes:
                dispatch(lane)
            report(pending.values())
            while pending:
                done, _ = wait(pending, timeout=5, return_when=FIRST_COMPLETED)
                if not done:
                    report(pending.values())
                for future in done:
                    lane, item = pending.pop(future)
                    proof = future.result()
                    measured = item['bytes']/max(proof['complete_seconds'], 0.001)
                    rates[proof['execution_node']] = 0.5*rates[proof['execution_node']]+0.5*measured
                    proofs[item['sha256']] = proof
                    completed_files += proof['files']
                    completed_bytes += proof['logical_bytes']
                    atomic_write_json(evidence/(item['sha256']+'.json'), {
                        **proof, 'manifest_sha256': resolved.manifest_sha256,
                        'acceptance': False, 'source_retirement': False}, durable=False)
                    dispatch(lane)
                    report(pending.values())
        finally:
            for future in pending:
                future.cancel()
            # Join before closing the remote session or inventory. A failed
            # future cannot leave background work using a disposed buffer.
            executor.shutdown(wait=True, cancel_futures=True)
            sources.close()
            if remote is not None:
                try:
                    cleanup = remote.close()
                except (OSError, ValueError, EOFError, RuntimeError, subprocess.SubprocessError) as exc:
                    cleanup = {'state': 'retained_session', 'reason': type(exc).__name__}
                atomic_write_json(evidence/'remote-session.json', {
                    'remote_root': remote.root, 'boot_id': remote.boot_id,
                    'cleanup': cleanup, 'acceptance': False}, durable=False)
    nodes = {node: sum(row['execution_node'] == node for row in proofs.values()) for node in set(lanes)}
    summary = {'contract': CONTRACT, 'jobs': len(proofs), 'nodes': nodes,
               'complete_seconds': time.monotonic()-started, 'worker_sha256': SOURCE_SHA256,
               'remote_initialization_failure': remote_error, 'policy_sha256': options.get('policy_sha256'),
               'transport': 'ssh-framed-memory-v1',
               'source_read_execution': sources.summary(),
               'evidence_directory': str(evidence)}
    if hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != SOURCE_SHA256:
        raise ValueError('pack verifier changed during reconstruction')
    if options.get('policy_path') and hashlib.sha256(Path(options['policy_path']).read_bytes()).hexdigest() != options['policy_sha256']:
        raise ValueError('pack verification policy changed during reconstruction')
    atomic_write_json(evidence/'summary.json', summary, durable=False)
    return proofs, summary


if __name__ == '__main__':
    _remote_stream()
