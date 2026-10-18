"""Compare complete immutable verification with atomic no-symlink resolution.

Only this isolated process replaces ``safe``. Every original payload SHA, exact
file-set, manifest and READY check remains. This is not publication, transport,
remote recovery, or permission to adopt the candidate in another owner.
"""
from __future__ import annotations

import argparse
import ast
import ctypes
from datetime import UTC, datetime
import errno
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import time
from unittest.mock import patch

from downloader.artifact_io import atomic_write_json, atomic_write_text
from scripts.benchmark_lakehouse_source_wave import limits, match_native_limits
from scripts.verify_lakehouse_workflow_history import owner_identity
from stockagent.control import lakehouse
from stockagent.data_sync import immutable_replication as immutable
from stockagent.runtime_identity import identity_sha256


class OpenHow(ctypes.Structure):
    _fields_ = [('flags', ctypes.c_uint64), ('mode', ctypes.c_uint64), ('resolve', ctypes.c_uint64)]


def native_syscall():
    # Linux x86-64 and asm-generic tables define openat2=437. Unknown ABIs use
    # the original ancestor checks, never a guessed syscall or weaker guard.
    if (platform.system() != 'Linux' or platform.machine() not in {'x86_64', 'aarch64', 'riscv64'}
            or not hasattr(os, 'O_PATH') or ctypes.sizeof(ctypes.c_void_p) != 8):
        return None
    call = ctypes.CDLL(None, use_errno=True).syscall
    call.restype = ctypes.c_long
    return call


OPENAT2 = native_syscall()


def checked_no_symlink(path):
    """True only after one successful complete kernel path resolution.

    O_NOFOLLOW must NOT accompany O_PATH: that combination can return a final
    symlink FD despite RESOLVE_NO_SYMLINKS. Errors other than ELOOP retain the
    complete old guard, including its missing-payload metadata semantics.
    """
    global OPENAT2
    call = OPENAT2  # Another thread may disable future probes during this call.
    if call is None:
        return False
    how = OpenHow(os.O_PATH | os.O_CLOEXEC, 0, 0x04)
    fd = call(ctypes.c_long(437), ctypes.c_int(-100), ctypes.c_char_p(os.fsencode(path)),
              ctypes.byref(how), ctypes.c_size_t(ctypes.sizeof(how)))
    if fd >= 0:
        os.close(fd)
        return True
    error = ctypes.get_errno()
    if error == errno.ELOOP:
        raise ValueError('immutable member path is redirected')
    if error in {errno.ENOSYS, errno.EINVAL, errno.EPERM}:
        OPENAT2 = None
    return False


def candidate_safe(source, checker=checked_no_symlink):
    tree = ast.parse(source)
    selected = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'safe']
    if len(selected) != 1:
        raise ValueError('one original canonical safe function required')
    function = selected[0]
    # After adoption the current canonical guard already has this wrapper.
    # Replace only that exact known probe, preserving the original fallback;
    # do not nest two probes or silently unwrap an unrelated safety predicate.
    for node in function.body:
        if not isinstance(node, ast.If) or not isinstance(node.test, ast.BoolOp):
            continue
        expected = ast.parse('not _checked_no_symlink(path)', mode='eval').body
        if (isinstance(node.test.op, ast.And) and len(node.test.values) == 2
                and ast.dump(node.test.values[0]) == ast.dump(expected)):
            node.test = node.test.values[1]
    guards = [node for node in function.body if isinstance(node, ast.If)
              and isinstance(node.test, ast.Call) and isinstance(node.test.func, ast.Name)
              and node.test.func.id == 'any']
    if len(guards) != 1:
        raise ValueError('original complete ancestor guard changed')
    guards[0].test = ast.BoolOp(op=ast.And(), values=[
        ast.UnaryOp(op=ast.Not(), operand=ast.Call(func=ast.Name(id='_checked', ctx=ast.Load()),
                                                 args=[ast.Name(id='path', ctx=ast.Load())], keywords=[])),
        guards[0].test])
    scope = ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[]))
    namespace = {**vars(immutable), '_checked': checker}
    exec(compile(scope, '<isolated-atomic-safe>', 'exec'), namespace)
    return namespace['safe']


def signature(path):
    value = path.stat()
    return [getattr(value, key) for key in ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')]


def prepare(c, root, snapshot):
    journal = json.loads((Path(c['state_root']) / f'export-{snapshot}.json').read_bytes())
    identity = journal['delivery_identity_sha256']
    release = Path(c['lake_root']) / 'releases' / ('lake-' + identity)
    if (not immutable.HASH.fullmatch(identity) or journal['state'] not in {'publication_prepared', 'published'}
            or journal['delivery_root'] != str(release)):
        raise ValueError('exact original enrolled immutable release required')
    owner = owner_identity()
    before = signature(release / 'manifest.json')
    manifest = immutable.read_manifest(release)
    if (manifest['identity_sha256'] != identity or before != signature(release / 'manifest.json')
            or owner != owner_identity()):
        raise ValueError('source manifest or native owner changed')
    source = Path(immutable.__file__).read_text()
    root.mkdir(mode=0o700)
    atomic_write_text(root / 'original_immutable_replication.py', source, mode=0o600, durable=True)
    proof = {'state': 'accepted_fixed_immutable_verification_input', 'snapshot_id': snapshot,
             'release': str(release), 'manifest_identity_sha256': identity,
             'manifest_signature': before, 'native_owner': owner,
             'original_source_sha256': hashlib.sha256(source.encode()).hexdigest(),
             'comparator_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
             'complete_files': len(manifest['files']),
             'complete_bytes': sum(row['bytes'] for row in manifest['files'].values())}
    atomic_write_json(root / 'inputs.json', proof, durable=True)
    return proof


def run_case(c, root, label, output):
    proof = json.loads((root / 'inputs.json').read_bytes())
    source = (root / 'original_immutable_replication.py').read_text()
    release = Path(proof['release'])
    owner, before_limits = owner_identity(), limits()
    if (proof['state'] != 'accepted_fixed_immutable_verification_input'
            or release.parent != Path(c['lake_root']) / 'releases'
            or proof['native_owner'] != owner
            or source != Path(immutable.__file__).read_text()
            or hashlib.sha256(source.encode()).hexdigest() != proof['original_source_sha256']
            or hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != proof['comparator_sha256']
            or signature(release / 'manifest.json') != proof['manifest_signature']):
        raise ValueError('original source, owner, comparator or fixed input changed')
    checks = {'atomic_successes': 0, 'original_guard_fallbacks': 0}
    def checked(path):
        result = checked_no_symlink(path)
        checks['atomic_successes' if result else 'original_guard_fallbacks'] += 1
        return result
    selected = immutable.safe if label == 'baseline' else candidate_safe(source, checked)
    wall, cpu = time.perf_counter(), time.process_time()
    with patch.object(immutable, 'safe', selected):
        manifest = immutable.verify(release)
    elapsed, spent = time.perf_counter()-wall, time.process_time()-cpu
    if (manifest['identity_sha256'] != proof['manifest_identity_sha256']
            or len(manifest['files']) != proof['complete_files']
            or sum(row['bytes'] for row in manifest['files'].values()) != proof['complete_bytes']
            or signature(release / 'manifest.json') != proof['manifest_signature']
            or owner != owner_identity() or before_limits != limits()
            or source != Path(immutable.__file__).read_text()):
        raise ValueError('complete verification identity or owner boundary changed')
    result = {'state': 'accepted_complete_immutable_verification', 'label': label,
              'observed_at_utc': datetime.now(UTC).isoformat(), 'wall_seconds': elapsed,
              'cpu_seconds': spent, 'complete_manifest_result_sha256': identity_sha256(manifest),
              'manifest_identity_sha256': manifest['identity_sha256'],
              'complete_files': len(manifest['files']), 'complete_bytes': proof['complete_bytes'],
              'path_resolution_checks': checks, 'resource_limits': before_limits,
              'native_owner': owner, 'fresh_process_peak_rss_bytes':
                  resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
              'source_writes': 0, 'provider_calls': 0, 'service_controls': 0,
              'caveats': ['Complete original verify, not export/replication or NAS recovery.',
                          'Only an isolated process changes safe; native code is untouched.']}
    atomic_write_json(output, result, durable=True)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--prepare', action='store_true')
    parser.add_argument('--snapshot', type=int)
    parser.add_argument('--label', choices=('baseline', 'candidate'))
    parser.add_argument('--output', type=Path)
    args = parser.parse_args(argv)
    if not args.root.resolve().is_relative_to(Path('artifacts/benchmarks').resolve()):
        parser.error('private benchmark root required')
    if args.prepare:
        if args.root.exists() or args.snapshot is None:
            parser.error('preparation needs a fresh root and an exact enrolled snapshot')
    elif (args.label is None or args.output is None or args.output.exists()
          or not args.output.resolve().is_relative_to(args.root.resolve())):
        parser.error('use a fresh case receipt below its input root')
    match_native_limits()
    c = lakehouse.configuration()
    lakehouse.guard(c)
    result = prepare(c, args.root, args.snapshot) if args.prepare else run_case(c, args.root, args.label, args.output)
    print(json.dumps({key: result[key] for key in
                     ('state', 'label', 'wall_seconds', 'cpu_seconds', 'complete_files') if key in result}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
