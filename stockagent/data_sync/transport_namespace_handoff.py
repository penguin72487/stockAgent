"""Hand off unreachable legacy sender mounts without restarting their services.

Called only by the original placement transaction under its three writer locks.
Normal umount retains the kernel's busy-reference check. No force/lazy detach,
process stop, source deletion or change to the parent mount's propagation.
"""
import argparse
from contextlib import contextmanager
import ctypes
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from stockagent.data_sync import transport_placement as placement

CONTRACT = 'stockagent.transport_namespace_handoff.v1'


def write_intent(path, value):
    atomic_write_bytes(path, json.dumps(value).encode() + b'\n', durable=True, mode=0o600)


def mounts(raw):
    result = []
    for line in raw.splitlines():
        left, right = line.split(' - ', 1)
        fields, fs = left.split(), right.split()
        decode = lambda s: re.sub(r'\\([0-7]{3})', lambda m: chr(int(m[1], 8)), s)
        result.append({'id': fields[0], 'parent': fields[1], 'root': decode(fields[3]),
                       'target': decode(fields[4]), 'flags': fields[5].split(','),
                       'propagation': fields[6:], 'filesystem': fs[0], 'source': fs[1]})
    return result


def exited_only(process):
    """Recognize only a stable zombie with no live tasks or mount namespaces.

    A zombie retains a /proc entry until its parent reaps it, but it no longer
    owns files or a mount namespace. A zombie group leader may still have live
    threads, so check every task twice and fail closed on unreadable/racing
    identities. A live process with inaccessible namespace stays blocking.
    """
    def identity(path):
        tail = (path / 'stat').read_text().rsplit(') ', 1)[1].split()
        if len(tail) <= 19 or tail[0] != 'Z':
            raise ValueError('task is not a zombie')
        return int(tail[19])  # /proc/<pid>/stat field 22: starttime

    def tasks():
        entries = sorted((process / 'task').iterdir())
        if not entries or len(entries) > 4096 or any(not p.name.isdecimal() for p in entries):
            raise ValueError('incomplete task census')
        result = {}
        for entry in entries:
            result[entry.name] = identity(entry)
            try:
                os.readlink(entry / 'ns/mnt')
            except FileNotFoundError:
                pass
            else:
                raise ValueError('task still exposes a mount namespace')
        return result

    try:
        first = identity(process)
        observed = tasks()
        return identity(process) == first and tasks() == observed
    except (OSError, ValueError, IndexError):
        return False


def namespaces(proc=Path('/proc')):
    """Inspect each live namespace once; an unreadable live process defers work."""
    result = {}
    for process in proc.iterdir():
        if not process.name.isdecimal():
            continue
        try:
            identity = os.readlink(process / 'ns/mnt')
            if identity not in result:
                result[identity] = (process, mounts((process / 'mountinfo').read_text()))
        except FileNotFoundError:
            if process.exists() and not exited_only(process) and process.exists():
                raise BlockingIOError('live mount namespace inspection incomplete')
        except (OSError, ValueError, IndexError) as error:
            if process.exists():
                raise BlockingIOError('live mount namespace inspection incomplete') from error
    return result


def legacy(row):
    return (row['filesystem'] == '9p' and row['source'].upper().startswith('D:')
            and row['root'] in {'/' + placement.D_BACKING.name,
                               '/' + placement.D_BACKING.name + '.relocated-to-E-v1'})


def exclusive_mount(row, identity, observed):
    """Only singleton peer groups are eligible; never change propagation roots."""
    if any(not item.startswith('shared:') for item in row['propagation']):
        return False
    groups = set(row['propagation'])
    if not groups:
        return True
    return all(ns == identity and other['id'] == row['id']
               for ns, (_, rows) in observed.items() for other in rows
               if groups.intersection(other['propagation']))


def eligible(identity, observed):
    process, rows = observed[identity]
    old = [row for row in rows if legacy(row)]
    if len(old) != 1 or old[0]['target'] != str(placement.CANONICAL):
        raise BlockingIOError('legacy sender has unexpected mount aliases')
    row = old[0]
    parent = next((item for item in rows if item['id'] == row['parent']), None)
    if (parent is None or not exclusive_mount(parent, identity, observed)
            or not exclusive_mount(row, identity, observed)
            or any(item['parent'] == row['id'] for item in rows)):
        raise BlockingIOError('sender mount has peers, propagation dependencies or submounts')
    try:
        (process / 'root' / str(placement.CANONICAL).lstrip('/')).stat()
    except FileNotFoundError:
        return row
    raise BlockingIOError('reachable legacy sender requires its normal consumer handoff')


def check_resume_parent(identity, expected, observed):
    rows = observed[identity][1]
    parent_candidates = [row for row in rows if row['target'] != str(placement.CANONICAL)
                         and placement.CANONICAL.is_relative_to(row['target'])]
    parent = max(parent_candidates, key=lambda row: len(row['target']), default=None)
    if (parent is None or parent['id'] != expected['parent']
            or not exclusive_mount(parent, identity, observed)
            or any(row['target'].startswith(str(placement.CANONICAL) + '/') for row in rows)):
        raise BlockingIOError('sender parent changed during the prepared handoff')


def key(path):
    info = path.stat()
    return [info.st_dev, info.st_ino]


def tree_key(descriptor):
    info = os.fstat(descriptor)
    return [info.st_dev, info.st_ino]


class MountAttributes(ctypes.Structure):
    _fields_ = [(field, ctypes.c_uint64) for field in ('attr_set', 'attr_clr', 'propagation', 'userns_fd')]


def mount_api(name, args, types):
    function = getattr(ctypes.CDLL(None, use_errno=True), name)
    function.argtypes, function.restype = types, ctypes.c_int
    result = function(*args)
    if result == -1:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), name)
    return result


@contextmanager
def prepared_tree(source, flags):
    """Export a detached E mount, with permissions applied before attachment.

    Older service namespaces do not contain the E filesystem at all. An old
    MS_BIND via /proc/1/root fails EINVAL there; a detached mount FD is the
    kernel API for crossing this boundary, without changing parent mounts.
    Constants are from Linux uapi/linux/mount.h; use libc, not syscall numbers.
    """
    bits = {'ro': 1, 'nosuid': 2, 'nodev': 4, 'noexec': 8,
            'noatime': 16, 'strictatime': 32, 'nodiratime': 128}
    if set(flags) - (set(bits) | {'rw', 'relatime'}):
        raise BlockingIOError('sender has unsupported VFS flags; preserve its old mount')
    fd = mount_api('open_tree', (-100, os.fsencode(source), 1 | os.O_CLOEXEC),
                   (ctypes.c_int, ctypes.c_char_p, ctypes.c_uint))
    try:
        attrs = MountAttributes(sum(bits.get(flag, 0) for flag in flags), 0x70, 1 << 18, 0)
        mount_api('mount_setattr', (fd, b'', 0x1000, ctypes.byref(attrs), ctypes.sizeof(attrs)),
                  (ctypes.c_int, ctypes.c_char_p, ctypes.c_uint,
                   ctypes.POINTER(MountAttributes), ctypes.c_size_t))
        yield fd
    finally:
        os.close(fd)


def attach_tree(descriptor, target):
    mount_api('move_mount', (descriptor, b'', -100, os.fsencode(target), 4),
              (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint))


def execute(argv):
    # Never force or lazily detach a busy mount. Errors remain private evidence.
    result = subprocess.run(argv, capture_output=True, text=True, timeout=10)
    if result.returncode:
        raise subprocess.CalledProcessError(result.returncode, argv, output=result.stdout, stderr=result.stderr)


def handoff_one(plan_path, tree_fd):
    """Runs in the pinned original namespace, with a durable PID-1-owned intent."""
    plan = json.loads(plan_path.read_bytes())
    identity = os.readlink('/proc/self/ns/mnt')
    initial = os.readlink('/proc/1/ns/mnt')
    source = Path('/proc/1/root') / plan['destination'].lstrip('/')
    canonical = placement.CANONICAL
    if (plan.get('contract') != CONTRACT or identity != plan.get('namespace') or identity == initial
            or plan.get('canonical') != str(canonical)
            or plan.get('boot_id') != Path('/proc/1/root/proc/sys/kernel/random/boot_id').read_text().strip()
            or key(source) != plan.get('destination_key')
            or tree_key(tree_fd) != plan.get('destination_key')
            or key(Path('/proc/1/root') / str(canonical).lstrip('/')) != plan['destination_key']):
        raise ValueError('namespace or enrolled E identity changed')
    expected = plan['mount']
    if not legacy(expected) or expected['target'] != str(canonical):
        raise ValueError('intent is not an exact legacy sender mount')

    def save(phase):
        plan.update(phase=phase, observed_at_epoch=time.time())
        write_intent(plan_path, plan)

    rows = mounts(Path('/proc/self/mountinfo').read_text())
    current = [row for row in rows if row['target'] == str(canonical)]
    if len(current) > 1:
        raise BlockingIOError('stacked sender mounts need explicit owner review')
    if current and current[0]['filesystem'] == 'ext4' and key(canonical) == plan['destination_key']:
        pass  # Resume after bind/remount but before the last receipt.
    elif current:
        if current != [expected]:
            raise BlockingIOError('legacy sender mount generation changed')
        fresh = namespaces()
        if eligible(identity, fresh) != expected:
            raise BlockingIOError('legacy sender eligibility changed')
        save('prepared')
        execute(['umount', '--internal-only', '--no-canonicalize', str(canonical)])
        save('old_mount_removed')
    elif plan.get('phase') not in {'prepared', 'old_mount_removed', 'E_bound', 'alias_verified'}:
        raise BlockingIOError('unmounted sender lacks a prepared handoff')

    if not current or current[0]['filesystem'] != 'ext4':
        # The old mount was already unreachable. This restores the same name;
        # there is no payload movement and no writable directory construction.
        check_resume_parent(identity, expected, namespaces())
        attach_tree(tree_fd, canonical)
        save('E_bound')
    if expected['propagation']:
        execute(['mount', '--make-shared', str(canonical)])
    final = mounts(Path('/proc/self/mountinfo').read_text())
    aliases = [row for row in final if row['target'] == str(canonical)]
    if (len(aliases) != 1 or aliases[0]['filesystem'] != 'ext4'
            or not set(expected['flags']).issubset(aliases[0]['flags'])
            or any(legacy(row) for row in final) or key(canonical) != plan['destination_key']
            or key(source) != plan['destination_key']):
        raise ValueError('E sender alias or original permissions not verified')
    save('alias_verified')
    return {'namespace': identity, 'state': 'alias_verified'}


def run(evidence, destination):
    """Original owner calls this under its existing sender/publication locks."""
    if os.readlink('/proc/self/ns/mnt') != os.readlink('/proc/1/ns/mnt'):
        raise ValueError('sender placement must run in the PID 1 namespace')
    placement.check_transport(physical=True)
    evidence = evidence.absolute()
    observed = namespaces()
    boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    plans = evidence / 'namespace-handoff'
    plans.mkdir(mode=0o700, exist_ok=True)
    plans = plans / boot
    plans.mkdir(mode=0o700, exist_ok=True)
    results = []
    own = os.readlink('/proc/self/ns/mnt')
    for identity, (process, rows) in observed.items():
        if identity == own:
            continue
        plan_path = plans / (identity.removeprefix('mnt:[').removesuffix(']') + '.json')
        pending = json.loads(plan_path.read_bytes()) if plan_path.exists() else None
        if not any(legacy(row) for row in rows) and (not pending or pending.get('phase') == 'alias_verified'):
            continue
        try:
            with (process / 'ns/mnt').open('rb') as pinned:
                if os.readlink(process / 'ns/mnt') != identity or os.fstat(pinned.fileno()).st_ino != int(plan_path.stem):
                    raise BlockingIOError('namespace changed while pinning')
                if pending is None:
                    row = eligible(identity, observed)
                    plan = {'contract': CONTRACT, 'phase': 'planned', 'namespace': identity, 'boot_id': boot,
                            'canonical': str(placement.CANONICAL), 'destination': str(destination),
                            'destination_key': key(destination), 'mount': row}
                    write_intent(plan_path, plan)
                # The FD pins the namespace; PID reuse cannot redirect nsenter.
                child_plan = Path('/proc/1/root') / str(plan_path).lstrip('/')
                # ProtectHome hides /root in some service namespaces. Reach
                # the already selected runtime and source through PID 1;
                # keep its interpreter/stdlib ABI, with no second environment.
                pid1 = '/proc/1/root'
                env = {**os.environ, 'PYTHONHOME': pid1 + sys.prefix,
                       'PYTHONPATH': pid1 + str(placement.CONFIG.parents[2]),
                       'PYTHONDONTWRITEBYTECODE': '1'}
                plan = json.loads(plan_path.read_bytes())
                with prepared_tree(destination, plan['mount']['flags']) as tree_fd:
                    result = subprocess.run(['nsenter', f'--mount=/proc/self/fd/{pinned.fileno()}', '--',
                                             pid1 + sys.executable, '-m', __name__, '--plan', str(child_plan),
                                             '--tree-fd', str(tree_fd)], env=env,
                                            pass_fds=(pinned.fileno(), tree_fd), capture_output=True, text=True)
                if result.returncode:
                    (plan_path.with_suffix('.log')).write_text(result.stderr[-4096:])
                    results.append({'namespace': identity, 'state': 'waiting_original_mount_handoff'})
                else:
                    results.append(json.loads(result.stdout))
                placement.check_transport(physical=True)
        except (BlockingIOError, FileNotFoundError):
            results.append({'namespace': identity, 'state': 'waiting_original_mount_handoff'})
    value = {'contract': CONTRACT, 'observed_at_epoch': time.time(), 'namespaces': results,
             'handed_off': sum(row['state'] == 'alias_verified' for row in results),
             'waiting': sum(row['state'] != 'alias_verified' for row in results),
             'services_restarted': 0, 'source_names_deleted': 0}
    atomic_write_json(evidence / 'namespace-handoff-status.json', value)
    return value


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--tree-fd', type=int, required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(handoff_one(args.plan, args.tree_fd)))
    except subprocess.CalledProcessError as error:
        print(error.stderr[-2000:], file=sys.stderr)
        raise
