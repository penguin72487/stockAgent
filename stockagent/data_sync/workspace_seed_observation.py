"""Bounded, non-authoritative progress for the existing rsync seed owner.

The rsync client reports its file-list position on SIGVTALRM (rsync >= 3.2).
This observer never copies, verifies, moves, or removes a source file.  Its
receipt is telemetry only; the canonical full readback still follows the seed.
"""
from __future__ import annotations

import ctypes
import json
import os
from pathlib import Path
import re
import signal
import sqlite3
import stat
import subprocess
import time

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync.storage_progress import _staging_cgroup_path, _staging_owner_pid, metadata

CONTRACT = 'stockagent.workspace_rsync_seed_observation.v1'
RECEIPT = 'seed-rsync-observation.json'
CHECKPOINT_CONTRACT = 'stockagent.workspace_seed_previous_checkpoint.v1'
CHECKPOINT_RECEIPT = 'seed-previous-checkpoint.json'
RSYNC_ARGS = ['rsync', '--archive', '--hard-links', '--no-inc-recursive', '--numeric-ids',
              '--ignore-existing', '--no-devices', '--no-specials', '--']
REPORT = re.compile(r'(?P<bytes>[0-9][0-9,]*)\s+\d+%\s+.*?\(xfr#\d+,\s*to-chk=(?P<left>\d+)/(?P<total>\d+)\)')


def parse_report(message: object, *, entries: int, logical_bytes: int) -> dict | None:
    if isinstance(message, list):
        try:
            message = bytes(message).decode('utf-8')
        except (ValueError, UnicodeError):
            return None
    if not isinstance(message, str) or len(message) > 4096:
        return None
    match = REPORT.search(message)
    if not match:
        return None
    left, total = int(match['left']), int(match['total'])
    transferred = int(match['bytes'].replace(',', ''))
    # rsync's list includes the source root, while the fixed census does not.
    if total != entries + 1 or not 0 <= left <= total or not 0 <= transferred <= logical_bytes:
        return None
    return {'entries_checked': total - left, 'entries_total': total,
            'transferred_bytes': transferred}


def process_identity(pid: int) -> tuple[int, str]:
    fields = (Path('/proc') / str(pid) / 'stat').read_text().rsplit(')', 1)[1].split()
    return int(fields[1]), fields[19]


def owner_for(name: str) -> int | None:
    path = Path('/var/lib/stockagent/workspace-volume') / (name + '.lock')
    if path.is_symlink():
        return None
    info = path.stat()
    return _staging_owner_pid(Path('/proc/locks').read_text(),
                              (os.major(info.st_dev), os.minor(info.st_dev)), info.st_ino)


def find_client(owner: int, source: Path, destination: Path) -> tuple[int, str, str] | None:
    expected = [*RSYNC_ARGS, str(source) + '/', str(destination) + '/']
    matches = []
    for proc in Path('/proc').iterdir():
        if not proc.name.isdecimal():
            continue
        try:
            parent, started = process_identity(int(proc.name))
            if parent != owner:
                continue
            args = [part.decode('utf-8') for part in (proc / 'cmdline').read_bytes().split(b'\0') if part]
            group = _staging_cgroup_path((proc / 'cgroup').read_text().strip())
            if (args == expected and os.readlink(proc / 'exe') == '/usr/bin/rsync'
                    and group is not None):
                matches.append((int(proc.name), started, group.rsplit('/', 1)[-1]))
        except (OSError, ValueError, IndexError, UnicodeError):
            continue
    return matches[0] if len(matches) == 1 else None


def journal_sample(pid: int, unit: str, *, entries: int, logical_bytes: int,
                   earliest: float, now: float) -> dict | None:
    output = subprocess.run(['journalctl', '-u', unit, f'_PID={pid}', '--since=-6 minutes',
                             '-n', '32', '-o', 'json', '--no-pager'],
                            capture_output=True, text=True, timeout=10, check=True).stdout
    if len(output) > 1024 * 1024:
        return None
    valid = []
    for line in output.splitlines():
        try:
            item = json.loads(line)
            stamp = int(item['__REALTIME_TIMESTAMP']) / 1_000_000
            if (item.get('_PID') != str(pid) or item.get('_SYSTEMD_UNIT') != unit
                    or not earliest <= stamp <= now + 5 or now - stamp > 300):
                continue
            parsed = parse_report(item.get('MESSAGE'), entries=entries, logical_bytes=logical_bytes)
            if parsed:
                valid.append({**parsed, 'observed_at_epoch': stamp})
        except (ValueError, TypeError, KeyError, json.JSONDecodeError):
            continue
    return max(valid, key=lambda row: row['observed_at_epoch']) if valid else None


def request_report(pid: int, owner: int, started: str, name: str) -> bool:
    """Signal the pinned child only if owner, generation and handler still match."""
    lib = ctypes.CDLL(None, use_errno=True)
    try:
        open_pidfd, send_signal = lib.pidfd_open, lib.pidfd_send_signal
    except AttributeError:
        return False
    open_pidfd.argtypes = [ctypes.c_int, ctypes.c_uint]
    send_signal.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint]
    fd = open_pidfd(pid, 0)
    if fd < 0:
        return False
    try:
        if process_identity(pid) != (owner, started) or owner_for(name) != owner:
            return False
        status = (Path('/proc') / str(pid) / 'status').read_text()
        caught = re.search(r'^SigCgt:\s*([0-9a-fA-F]+)$', status, re.MULTILINE)
        if not caught or not int(caught[1], 16) & (1 << (signal.SIGVTALRM - 1)):
            return False
        return send_signal(fd, signal.SIGVTALRM, None, 0) == 0
    except (OSError, ValueError, IndexError):
        return False
    finally:
        os.close(fd)


def boot_id() -> str:
    return Path('/proc/sys/kernel/random/boot_id').read_text().strip()


def bound_checkpoint(name: str, evidence: Path, intent: dict, *, now: float) -> dict | None:
    """A historical copy checkpoint, bound to this boot and original owner.

    It is never a fresh readback, a completion claim or an ETA input.
    """
    value = metadata(evidence / CHECKPOINT_RECEIPT)
    try:
        stamp = value.get('observed_at_epoch')
        files, size = value.get('verified_files'), value.get('verified_bytes')
        total_files, total_bytes = intent.get('files'), intent.get('logical_bytes')
        if (value.get('contract') != CHECKPOINT_CONTRACT or value.get('name') != name
                or value.get('intent_started_at_epoch') != intent.get('started_at_epoch')
                or value.get('boot_id') != boot_id() or type(stamp) not in (int, float)
                or not 0 <= stamp <= now + 5
                or type(files) is not int or type(total_files) is not int or not 0 <= files <= total_files
                or type(size) is not int or type(total_bytes) is not int or not 0 <= size <= total_bytes):
            return None
        owner, pid = value['owner_pid'], value['client_pid']
        if (type(owner) is not int or type(pid) is not int or owner_for(name) != owner
                or process_identity(owner)[1] != value.get('owner_start_ticks')
                or process_identity(pid) != (owner, value.get('client_start_ticks'))):
            return None
        return value
    except (OSError, ValueError, IndexError, TypeError, KeyError):
        return None


def capture_checkpoint(name: str, evidence: Path, intent: dict, *, owner: int,
                       owner_start: str, pid: int, started: str, now: float) -> bool:
    if bound_checkpoint(name, evidence, intent, now=now):
        return True  # One bounded SQLite read per owner/client generation.
    index = evidence / 'files.sqlite'
    try:
        if index.is_symlink() or evidence.resolve(strict=True) != evidence:
            return False
        before = index.stat()
        if not stat.S_ISREG(before.st_mode):
            return False
        deadline = time.monotonic() + 12
        connection = sqlite3.connect(index.as_uri() + '?mode=ro', uri=True, timeout=2)
        try:
            connection.execute('PRAGMA query_only=ON')
            connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 10000)
            files, size = connection.execute(
                "SELECT count(*), coalesce(sum(bytes),0) FROM files WHERE kind='file' AND state='verified'"
            ).fetchone()
        finally:
            connection.close()
        after = index.stat()
        identity = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
        if (identity(before) != identity(after) or owner_for(name) != owner
                or process_identity(owner)[1] != owner_start or process_identity(pid) != (owner, started)
                or metadata(evidence / 'progress.json').get('state') != 'seeding_hardlinked_copy'
                or type(files) is not int or not 0 <= files <= intent['files']
                or type(size) is not int or not 0 <= size <= intent['logical_bytes']):
            return False
        atomic_write_json(evidence / CHECKPOINT_RECEIPT, {'contract': CHECKPOINT_CONTRACT,
            'name': name, 'intent_started_at_epoch': intent['started_at_epoch'], 'boot_id': boot_id(),
            'owner_pid': owner, 'owner_start_ticks': owner_start,
            'client_pid': pid, 'client_start_ticks': started,
            'verified_files': files, 'verified_bytes': size, 'observed_at_epoch': time.time()})
        return True
    except (OSError, ValueError, TypeError, KeyError, IndexError, sqlite3.Error):
        return False


def observe(item: dict, workspace: Path, *, now: float | None = None) -> str:
    from scripts import run_workspace_source_staging as queue
    now = time.time() if now is None else now
    source, destination, evidence, _ = queue.paths(item, workspace)
    intent, progress = metadata(evidence / 'intent.json'), metadata(evidence / 'progress.json')
    expected_contract = 'workspace_scoped_relocation_v1' if item.get('preserve_symlinks') else 'workspace_exact_relocation_v1'
    entries, logical, start = intent.get('entries'), intent.get('logical_bytes'), intent.get('started_at_epoch')
    if (not item.get('seed_hardlinks') or intent.get('contract') != expected_contract
            or intent.get('state') != 'indexed' or intent.get('source') != str(source)
            or intent.get('destination') != str(destination) or progress.get('state') != 'seeding_hardlinked_copy'
            or type(entries) is not int or entries <= 0 or type(logical) is not int or logical < 0
            or type(start) not in (int, float) or start > now):
        return 'not_seeding'
    try:
        version = subprocess.check_output(['/usr/bin/rsync', '--version'], text=True, timeout=5).splitlines()[0]
        match = re.search(r'\bversion\s+(\d+)\.(\d+)\.(\d+)', version)
        if not match or tuple(map(int, match.groups())) < (3, 2, 0):
            return 'unsupported_rsync'
        owner = owner_for(item['name'])
        client = find_client(owner, source, destination) if owner else None
        if not client:
            return 'no_owned_client'
        pid, started, unit = client
        owner_start = process_identity(owner)[1]
        # This is a single read of the durable checkpoint, not fresh source or
        # destination validation. Failure never blocks the original copier.
        capture_checkpoint(item['name'], evidence, intent, owner=owner, owner_start=owner_start,
                           pid=pid, started=started, now=now)
        # Ask first, so a normal small-file workload can expose this minute's
        # sample rather than always lagging one full timer period. A large
        # active file may not emit until later; the previous sample remains
        # explicitly timestamped and is never refreshed by this request.
        requested = request_report(pid, owner, started, item['name'])
        if requested:
            time.sleep(0.25)
        sample = journal_sample(pid, unit, entries=entries, logical_bytes=logical, earliest=start, now=now)
        if sample and owner_for(item['name']) == owner and process_identity(pid) == (owner, started):
            # Do not expose paths, filenames, journal text, or claim validation.
            atomic_write_json(evidence / RECEIPT, {'contract': CONTRACT, 'name': item['name'],
                'intent_started_at_epoch': start, 'owner_pid': owner, 'owner_start_ticks': owner_start,
                'client_pid': pid, 'client_start_ticks': started, **sample})
        return 'sampled_and_requested' if sample and requested else 'requested' if requested else 'not_requested'
    except (OSError, ValueError, IndexError, subprocess.SubprocessError):
        return 'observation_unavailable'


def bound_sample(name: str, evidence: Path, intent: dict, *, now: float) -> dict | None:
    value = metadata(evidence / RECEIPT)
    observed, entries, checked = value.get('observed_at_epoch'), value.get('entries_total'), value.get('entries_checked')
    try:
        if (value.get('contract') != CONTRACT or value.get('name') != name
                or value.get('intent_started_at_epoch') != intent.get('started_at_epoch')
                or type(observed) not in (int, float) or not now - 300 <= observed <= now + 5
                or type(entries) is not int or entries != intent.get('entries', -2) + 1
                or type(checked) is not int or not 0 <= checked <= entries
                or type(value.get('transferred_bytes')) is not int
                or not 0 <= value['transferred_bytes'] <= intent.get('logical_bytes', -1)):
            return None
        owner, pid = value['owner_pid'], value['client_pid']
        if (type(owner) is not int or type(pid) is not int or owner_for(name) != owner
                or process_identity(owner)[1] != value.get('owner_start_ticks')
                or process_identity(pid) != (owner, value.get('client_start_ticks'))):
            return None
        return value
    except (OSError, ValueError, IndexError, TypeError, KeyError):
        return None
