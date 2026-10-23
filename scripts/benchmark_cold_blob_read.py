#!/usr/bin/env python3
"""Compare guarded D blob recovery through DrvFs and native Windows pipes.

Default mode reads real immutable objects with full SHA and readback. Optional
publication mode owns isolated generated D fixtures and removes only those
after exact new/old restore; never production data, mounts or OS caches.
"""
from __future__ import annotations
import argparse
import ast
from datetime import UTC, datetime
import hashlib
import inspect
import json
import os
from pathlib import Path
import resource
import shutil
import statistics
import sys
import tempfile
import time
import uuid
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json, sha256_file
from stockagent.data_sync.windows_cold_io import admit_workspace, binary_reader


def atomic_control_reference_reader():
    """Retain the former allocation request, changing no observation fences.

    This reference is used only on generated private fixtures. Replacing one
    checked AST argument avoids a second implementation of source validation.
    Both variants retain current JSON/FD/size checks and the nonblocking open.
    """
    from stockagent.data_sync import packed_snapshots as packed
    tree = ast.parse(inspect.getsource(packed._read_atomic_json_control))
    reads = [node for node in ast.walk(tree) if isinstance(node, ast.Assign)
             and any(isinstance(target, ast.Name) and target.id == 'raw' for target in node.targets)]
    if (len(reads) != 1 or ast.unparse(reads[0].value) != 'stream.read(before.st_size + 1)'):
        raise SnapshotError('observed-size reference no longer matches its single read boundary')
    reads[0].value.args = [ast.parse('64 * 1024**2 + 1', mode='eval').body]
    namespace = dict(packed.__dict__)
    exec(compile(ast.fix_missing_locations(tree), '<retained-control-read-bound>', 'exec'), namespace)
    return namespace['_read_atomic_json_control']


def atomic_control_workflow_trial(observed_size: bool, *, control_files: int) -> dict:
    """Complete failed/retry/no-op/corrected publication and both-version recovery."""
    from stockagent.data_sync import packed_snapshots as packed
    primary = Path('/srv/stockagent-packed')
    _check_d_primary_mount(primary)
    admit_workspace(Path('/var/tmp'), 256 * 1024**2 + control_files * 32768)
    local = Path(tempfile.mkdtemp(prefix='stockagent-control-read-', dir='/var/tmp'))
    private = primary / '.local-state' / ('control-read-measurement-' + uuid.uuid4().hex)
    private.mkdir(mode=0o700)
    private_identity = private.lstat()
    source = local / 'data_finmind'
    # Use the same private marker/ignore/owner policy as the real collector.
    # The entire authority here is an owned, generated D fixture, not an alias
    # into the production private history or an exemption from its admission.
    from stockagent.data_sync.dataset_migration import private_history_root
    packed.initialize_packed_layout(private, node_id='fixture')
    cold = private_history_root(private, {'private_history': {
        'enabled': True, 'relative_root': '.local-state/private-history',
        'shared_export': False, 'nas_export': False, 'source_retirement': False}})
    receipts = source / 'sponsor/receipts/TaiwanStockPrice/all'
    receipts.mkdir(parents=True, mode=0o700)
    def write_control(index, value):
        path = receipts / f'{index:08}.json'
        path.write_bytes(json.dumps({'observation': index, 'value': value, 'missing': None,
            'volume_unit': 'shares', 'source': 'generated-private-measurement'},
            sort_keys=True, separators=(',', ':')).encode())
        path.chmod(0o600)
        os.utime(path, ns=(1700000000000000000, 1700000000000000000))
    for index in range(control_files):
        write_control(index, -index)
    def originals():
        return {path.relative_to(source).as_posix(): sha256_file(path)
                for path in source.rglob('*') if path.is_file()}
    initial = originals()
    reader = packed._read_atomic_json_control if observed_size else atomic_control_reference_reader()
    source_calls, phases = 0, {}
    options = dict(node_id='fixture', file_chunk_bytes=8 * 1024**2, pack_buckets=2,
        private_observation_cohort=True, private_atomic_json_controls=True,
        batch_directory_fsync=True)
    def interrupted_guard():
        nonlocal source_calls
        source_calls += 1
        if source_calls == 2:
            raise SnapshotError('deliberate generated control baseline interruption')
    def measured(name, action):
        phase = time.monotonic()
        result = action()
        phases[name] = time.monotonic() - phase
        return result
    def readback(target, release, expected):
        restored = packed.fetch_packed_snapshot(cold, target, release)
        actual = {path.relative_to(restored).as_posix(): sha256_file(path)
                  for path in restored.rglob('*') if path.is_file()
                  and path.relative_to(restored).as_posix() in expected}
        if actual != expected:
            raise SnapshotError('generated full control original readback differs')
        # Detect unexpected source members as well, without counting READY or
        # the materializer's own hidden marker as original observations.
        manifest_paths = {row['path'] for row in packed._load_inventory(cold, release.manifest)
                          if row['kind'] == 'file'}
        if manifest_paths != set(expected):
            raise SnapshotError('generated complete source inventory differs')
    started, cpu = time.monotonic(), cpu_seconds()
    with patch.object(packed, '_read_atomic_json_control', reader):
        phase = time.monotonic()
        try:
            packed.publish_packed_snapshot(cold, 'controls', source,
                source_guard=interrupted_guard, **options)
        except SnapshotError as error:
            if str(error) != 'deliberate generated control baseline interruption':
                raise
        else:
            raise SnapshotError('generated control interruption was not exercised')
        phases['failed_baseline_seconds'] = time.monotonic() - phase
        if list((cold / 'heads/controls').glob('*.json')):
            raise SnapshotError('failed control fixture committed a head')
        first = measured('retry_publication_seconds',
            lambda: packed.publish_packed_snapshot(cold, 'controls', source, **options))
        first_proof = measured('baseline_full_verify_seconds',
            lambda: packed.verify_packed_snapshot(cold, first, reconstruct_all=True))
        measured('baseline_full_restore_seconds', lambda: readback(local / 'first', first, initial))
        unchanged = measured('unchanged_publication_seconds',
            lambda: packed.publish_packed_snapshot(cold, 'controls', source, **options))
        if unchanged.manifest_sha256 != first.manifest_sha256:
            raise SnapshotError('unchanged generated controls advanced a timestamp-only head')
        write_control(0, -2.5)
        (receipts / '00000001.json').unlink()
        write_control(control_files, -9000)
        revised = originals()
        second = measured('incremental_publication_seconds',
            lambda: packed.publish_packed_snapshot(cold, 'controls', source, **options))
        second_proof = measured('incremental_full_verify_seconds',
            lambda: packed.verify_packed_snapshot(cold, second, reconstruct_all=True))
        measured('incremental_full_restore_seconds', lambda: readback(local / 'second', second, revised))
        measured('old_version_full_restore_seconds', lambda: readback(local / 'old', first, initial))
        if (first_proof['independently_reconstructed_files'] != control_files
                or second_proof['independently_reconstructed_files'] != control_files):
            raise SnapshotError('complete control count, NULL or raw value differs')
    elapsed, spent = time.monotonic() - started, cpu_seconds() - cpu
    if (private.resolve() != private or private.lstat().st_ino != private_identity.st_ino
            or private.lstat().st_uid != os.geteuid() or private.lstat().st_mode & 0o777 != 0o700):
        raise SnapshotError('owned generated control fixture changed; retain it')
    _check_d_primary_mount(primary)
    cleanup = time.monotonic()
    shutil.rmtree(private)
    shutil.rmtree(local)
    return {'profile': 'observed_size' if observed_size else 'maximum_bound',
        'complete_seconds': elapsed, 'posix_cpu_seconds_excluding_windows_processes': spent,
        **phases, 'inventory_sha256': [r.manifest['archive']['inventory']['sha256'] for r in (first, second)],
        'source_original_sha256': [initial, revised], 'control_files': control_files,
        'first_files_reconstructed': first_proof['independently_reconstructed_files'],
        'second_files_reconstructed': second_proof['independently_reconstructed_files'],
        'old_version_full_original_readback': True, 'unchanged_head_reused': True,
        'generated_fixture_cleanup_complete': True, 'fixture_cleanup_seconds': time.monotonic() - cleanup,
        'production_source_writes': 0, 'production_cold_object_deletions': 0}


def chunk_reuse_workflow_trial(native_enabled: bool, *, native_recovery: bool = False,
                               pipeline_comparison: bool = False) -> dict:
    """Generated D fixture: failed baseline -> retry -> revision -> both restores.

    The temporary authority exception exists only in this bounded benchmark;
    production still admits solely canonical public/private D roots. No real
    source, enrolled dataset, primary CAS or retained version is removed.
    """
    from stockagent.data_sync import packed_snapshots as packed
    from stockagent.data_sync.windows_cold_io import hash_many
    primary = Path('/srv/stockagent-packed')
    _check_d_primary_mount(primary)
    admit_workspace(Path('/var/tmp'), 32 * 1024**3 + 1024**3)
    local = Path(tempfile.mkdtemp(prefix='stockagent-chunk-reuse-', dir='/var/tmp'))
    private = primary / '.local-state' / ('chunk-reuse-measurement-' + uuid.uuid4().hex)
    private.mkdir(mode=0o700)
    private_identity = private.lstat()
    source, cold = local / 'source', private / 'cold'
    source.mkdir(mode=0o700)
    large_files = 12 if pipeline_comparison else 1
    for file_index in range(large_files):
        path = source / ('large.bin' if file_index == 0 else f'large-{file_index:02}.bin')
        with path.open('xb') as stream:
            for index in range(2 if pipeline_comparison else 16):
                token = f'{file_index}:{index}' if pipeline_comparison else str(index)
                stream.write(hashlib.sha256(token.encode()).digest() * (8 * 1024**2 // 32))
            stream.flush()
            os.fsync(stream.fileno())
    raw = source / 'large.bin'
    for index in range(64):
        (source / f'{index:04}.json').write_bytes(
            json.dumps({'value': -index, 'missing': None, 'unit': 'shares'}).encode())
    for member in source.iterdir():
        member.chmod(0o600)
        os.utime(member, ns=(1700000000000000000, 1700000000000000000))
    initial_sha = sha256_file(raw)
    initial_originals = {path.name: sha256_file(path) for path in source.iterdir()}
    calls, source_calls = [], 0
    def authority(root):
        if root != cold or private.resolve() != private or private.lstat().st_ino != private_identity.st_ino:
            raise SnapshotError('generated D fixture identity changed')
        _check_d_primary_mount(primary)
    def measured_hash(paths):
        calls.append(len(paths))
        return hash_many(paths)
    def interrupted_source_guard():
        nonlocal source_calls
        source_calls += 1
        if source_calls == 2:
            raise SnapshotError('deliberate generated baseline interruption')
    options = dict(node_id='fixture', file_chunk_bytes=8 * 1024**2, pack_buckets=2,
                   loose_file_threshold_bytes=8 * 1024**2, batch_directory_fsync=True)
    started, cpu = time.monotonic(), cpu_seconds()
    with patch.object(packed, '_check_native_pack_read_authority', authority), \
            patch('stockagent.data_sync.windows_cold_io.hash_many', measured_hash):
        try:
            packed.publish_packed_snapshot(cold, 'fixture', source,
                source_guard=interrupted_source_guard, **options)
        except SnapshotError as error:
            if str(error) != 'deliberate generated baseline interruption':
                raise
        else:
            raise SnapshotError('the generated interruption did not occur')
        if list((cold / 'heads/fixture').glob('*.json')):
            raise SnapshotError('an interrupted fixture committed a head')
        retry_started = time.monotonic()
        first = packed.publish_packed_snapshot(cold, 'fixture', source,
            d_primary_native_chunk_reuse=native_enabled or pipeline_comparison,
            d_primary_native_chunk_reuse_pipeline=pipeline_comparison and native_enabled, **options)
        retry_seconds = time.monotonic() - retry_started
        first_proof = packed.verify_packed_snapshot(cold, first, reconstruct_all=True,
            d_primary_native_chunk_reads=(native_enabled or pipeline_comparison) and native_recovery)
        restored = packed.fetch_packed_snapshot(cold, local / 'first', first)
        if {path.name: sha256_file(path) for path in restored.iterdir() if path.is_file()
            and path.name in initial_originals} != initial_originals:
            raise SnapshotError('generated first-original recovery differs')
        with raw.open('r+b') as stream:
            stream.write(b'exact revised raw value')
            stream.flush()
            os.fsync(stream.fileno())
        (source / '0000.json').unlink()
        (source / 'new.json').write_bytes(b'{"value":-2.5,"missing":null,"unit":"shares"}')
        for member in source.iterdir():
            member.chmod(0o600)
            os.utime(member, ns=(1700000001000000000, 1700000001000000000))
        revised_sha = sha256_file(raw)
        revised_originals = {path.name: sha256_file(path) for path in source.iterdir()}
        second = packed.publish_packed_snapshot(cold, 'fixture', source,
            d_primary_native_chunk_reuse=native_enabled or pipeline_comparison,
            d_primary_native_chunk_reuse_pipeline=pipeline_comparison and native_enabled, **options)
        second_proof = packed.verify_packed_snapshot(cold, second, reconstruct_all=True,
            d_primary_native_chunk_reads=(native_enabled or pipeline_comparison) and native_recovery)
        now = packed.fetch_packed_snapshot(cold, local / 'second', second)
        old = packed.fetch_packed_snapshot(cold, local / 'old', first)
        if (sha256_file(now / 'large.bin') != revised_sha or sha256_file(old / 'large.bin') != initial_sha
                or (now / '0000.json').exists() or not (old / '0000.json').is_file()
                or {name: sha256_file(now / name) for name in revised_originals} != revised_originals
                or {name: sha256_file(old / name) for name in initial_originals} != initial_originals
                or (now / 'new.json').read_bytes() != (source / 'new.json').read_bytes()):
            raise SnapshotError('generated incremental/old-version recovery differs')
    wall, spent = time.monotonic() - started, cpu_seconds() - cpu
    profile = ('pipeline' if native_enabled else 'file_batch') if pipeline_comparison else (
        'batch' if native_enabled else 'drvfs')
    result = {'profile': profile, 'complete_seconds': wall,
        'posix_cpu_seconds_excluding_windows_processes': spent, 'retry_publication_seconds': retry_seconds,
        'inventory_sha256': [r.manifest['archive']['inventory']['sha256'] for r in (first, second)],
        'original_sha256': [initial_sha, revised_sha], 'native_hash_batch_member_counts': calls,
        'native_full_original_chunk_recovery': (native_enabled or pipeline_comparison) and native_recovery,
        'native_existing_chunk_pipeline': pipeline_comparison and native_enabled,
        'source_original_sha256': [initial_originals, revised_originals],
        'first_files_reconstructed': first_proof['independently_reconstructed_files'],
        'second_files_reconstructed': second_proof['independently_reconstructed_files'],
        'old_version_full_original_readback': True, 'uncommitted_attempt_retained_until_verified_retry': True,
        'production_source_writes': 0, 'production_cold_object_deletions': 0}
    authority(cold)
    if private.lstat().st_uid != os.geteuid() or private.lstat().st_mode & 0o777 != 0o700:
        raise SnapshotError('generated fixture is no longer private; retain it')
    # Exact owned generated fixtures only, after complete new/old readback.
    shutil.rmtree(private)
    shutil.rmtree(local)
    result['generated_fixture_cleanup_complete'] = True
    return result


def signature(path):
    s = path.lstat()
    return [s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns, s.st_mode, s.st_nlink]


def cpu_seconds():
    return sum(resource.getrusage(w).ru_utime + resource.getrusage(w).ru_stime
               for w in (resource.RUSAGE_SELF, resource.RUSAGE_CHILDREN))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument('--hash-batch-only', action='store_true',
                        help='Compare full native SHA per process versus bounded batching; no copies')
    parser.add_argument('--hash-reference', choices=('individual', 'drvfs'), default='individual',
                        help='Reference for the read-only hash batch ABBA; defaults preserve prior trials')
    parser.add_argument('--publication-workflow', action='store_true',
                        help='ABBA of complete generated D publication/recovery, including uncommitted retry')
    parser.add_argument('--native-recovery', action='store_true',
                        help='Include the separately selected full-SHA native chunk recovery')
    parser.add_argument('--native-limits', action='store_true',
                        help='Match the original storage worker CPU/RAM/nice/affinity on this engineering unit only')
    parser.add_argument('--chunk-pipeline', action='store_true',
                        help='Compare existing per-file native reuse with cross-file bounded pipeline; 12 x 16 MiB originals')
    parser.add_argument('--atomic-control-workflow', action='store_true',
                        help='Complete generated D JSON-control ABBA: maximum versus observed-size reads')
    parser.add_argument('--control-files', type=int, default=8192,
                        help='Number of generated JSON controls for that workflow (32..32768)')
    args = parser.parse_args()
    if args.output.exists() or not 1 <= args.rounds <= 5:
        parser.error("fresh receipt and 1-5 rounds required")
    os.umask(0o077)
    limits, owner = None, None
    if args.native_recovery and not args.publication_workflow:
        parser.error('native recovery requires the complete generated workflow')
    if args.chunk_pipeline and not (args.publication_workflow and args.native_recovery):
        parser.error('chunk pipeline comparison requires publication and full native recovery')
    if args.atomic_control_workflow and (args.publication_workflow or args.hash_batch_only
                                      or args.native_recovery or args.chunk_pipeline
                                      or not 32 <= args.control_files <= 32768):
        parser.error('choose the isolated atomic-control workload and a bounded file count')
    if args.native_limits:
        from scripts.benchmark_lakehouse_source_wave import match_native_limits
        from scripts.verify_lakehouse_workflow_history import owner_identity
        limits, owner = match_native_limits(), owner_identity()
    if args.atomic_control_workflow:
        code_paths = (Path(__file__), ROOT / 'stockagent/data_sync/packed_snapshots.py',
                      ROOT / 'stockagent/data_sync/packed_metadata.py')
        code = {str(path): sha256_file(path) for path in code_paths}
        rows = []
        for enabled in (False, True, True, False):
            row = atomic_control_workflow_trial(enabled, control_files=args.control_files)
            rows.append(row)
            print(json.dumps({key: value for key, value in row.items()
                              if key != 'source_original_sha256'}), flush=True)
        if (len({tuple(row['inventory_sha256']) for row in rows}) != 1
                or any(row['source_original_sha256'] != rows[0]['source_original_sha256'] for row in rows)
                or code != {str(path): sha256_file(path) for path in code_paths}):
            raise SnapshotError('complete control workflow code/inventory/original parity changed')
        if args.native_limits:
            from scripts.benchmark_lakehouse_source_signature import limits as observed_limits
            if limits != observed_limits() or owner_identity() != owner:
                raise SnapshotError('native owner/resource bounds changed during complete control ABBA')
        metrics = ('complete_seconds', 'retry_publication_seconds', 'unchanged_publication_seconds',
                   'incremental_publication_seconds', 'posix_cpu_seconds_excluding_windows_processes')
        means = {profile: {metric: statistics.mean(row[metric] for row in rows if row['profile'] == profile)
                           for metric in metrics} for profile in ('maximum_bound', 'observed_size')}
        atomic_write_json(args.output, {'state': 'accepted_generated_D_atomic_control_workflow_abba',
            'observed_at_utc': datetime.now(UTC).isoformat(), 'runs': rows, 'means': means,
            'code_sha256': code, 'scope': 'generated private JSON controls on D; not all-source or NAS acceptance',
            'reference_difference': 'only stream.read argument; same complete source/FD/JSON/recovery gates',
            'native_resource_limits': limits, 'native_owner': owner, 'os_cache_flushed': False,
            'provider_calls': 0, 'production_source_writes': 0, 'production_cold_object_deletions': 0}, durable=True)
        return
    if args.publication_workflow:
        if args.hash_batch_only:
            parser.error('choose either a complete generated workflow or read-only real-object hashes')
        code_paths = (Path(__file__), ROOT / 'stockagent/data_sync/packed_snapshots.py',
                      ROOT / 'stockagent/data_sync/windows_cold_io.py', ROOT / 'scripts/windows_cold_binary_io.ps1')
        code = {str(path): sha256_file(path) for path in code_paths}
        rows = []
        for enabled in (False, True, True, False):
            row = chunk_reuse_workflow_trial(enabled, native_recovery=args.native_recovery,
                pipeline_comparison=args.chunk_pipeline)
            rows.append(row)
            print(json.dumps(row), flush=True)
        if (len({tuple(row['inventory_sha256']) for row in rows}) != 1
                or len({tuple(row['original_sha256']) for row in rows}) != 1
                or code != {str(path): sha256_file(path) for path in code_paths}):
            raise SnapshotError('complete D workflow code/inventory/original parity changed')
        means = {profile: {metric: statistics.mean(row[metric] for row in rows if row['profile'] == profile)
                 for metric in ('complete_seconds', 'retry_publication_seconds',
                                'posix_cpu_seconds_excluding_windows_processes')}
                 for profile in (('file_batch', 'pipeline') if args.chunk_pipeline else ('drvfs', 'batch'))}
        if args.native_limits:
            from scripts.benchmark_lakehouse_source_signature import limits as observed_limits
            if limits != observed_limits() or owner_identity() != owner:
                raise SnapshotError('native owner/resource bounds changed during complete ABBA')
        atomic_write_json(args.output, {'state': 'accepted_generated_D_chunk_reuse_workflow_abba',
            'observed_at_utc': datetime.now(UTC).isoformat(),
            'runs': rows, 'means': means, 'code_sha256': code,
            'scope': ('generated 12 x 16 MiB plus 64 small files on D; per-file vs cross-file pipeline'
                      if args.chunk_pipeline else 'generated 128 MiB plus 64 small files on D')
                     + ', not production/NAS/all-source acceptance',
            'cross_file_pipeline_comparison': args.chunk_pipeline,
            'provider_calls': 0, 'production_source_writes': 0, 'production_cold_object_deletions': 0,
            'native_resource_limits': limits, 'native_owner': owner,
            'windows_cpu_measured': False, 'os_cache_flushed': False}, durable=True)
        return
    cold = Path("/srv/stockagent-packed")
    _check_d_primary_mount(cold)
    samples = []
    for parent in sorted((cold / "objects/blobs").iterdir()):
        for path in sorted(parent.iterdir()):
            if path.is_symlink() or path.suffix != ".blob":
                continue
            info = path.stat()
            if 8 * 1024**2 <= info.st_size <= 32 * 1024**2:
                samples.append({"path": path, "signature": signature(path), "sha256": path.stem})
            if len(samples) == (8 if args.hash_batch_only else 3):
                break
        if len(samples) == (8 if args.hash_batch_only else 3):
            break
    if len(samples) != (8 if args.hash_batch_only else 3):
        raise SnapshotError("bounded real cold objects were not found")
    total = sum(row["signature"][2] for row in samples)
    if args.hash_batch_only:
        from stockagent.data_sync.windows_cold_io import hash_file, hash_many
        rows=[]
        reference = args.hash_reference
        for turn in range(args.rounds):
            for profile in ((reference,'batch') if turn%2==0 else ('batch',reference)):
                if any(signature(s['path']) != s['signature'] for s in samples):
                    raise SnapshotError('immutable measurement object changed')
                started, cpu = time.monotonic(), cpu_seconds()
                hashes=(hash_many([s['path'] for s in samples]) if profile=='batch' else
                        {s['path']:(sha256_file(s['path']) if profile=='drvfs' else hash_file(s['path']))
                         for s in samples})
                if any(hashes[s['path']] != s['sha256'] or signature(s['path']) != s['signature'] for s in samples):
                    raise SnapshotError('native measurement full SHA or signature differs')
                row={'profile':profile,'round':turn+1,'bytes':total,'seconds':time.monotonic()-started,
                     'cpu_seconds':cpu_seconds()-cpu,
                     'every_full_sha256_verified':True}
                rows.append(row);print(json.dumps(row),flush=True)
        medians={p:statistics.median(r['seconds'] for r in rows if r['profile']==p) for p in (reference,'batch')}
        atomic_write_json(args.output,{'scope':'bounded real cold blob full SHA; no reconstruction/NAS multiplier',
            'samples':[{**r,'path':str(r['path'])} for r in samples],'runs':rows,'medians':medians,
            'authoritative_cold_changed':False,'scratch_created':False,'os_cache_flushed':False})
        return
    admit_workspace(Path("/var/lib"), 32 * 1024**3 + 2 * total)
    private = Path(tempfile.mkdtemp(prefix="stockagent-native-cold-read-", dir="/var/lib"))
    rows = []
    for turn in range(args.rounds):
        for profile in (("drvfs", "native") if turn % 2 == 0 else ("native", "drvfs")):
            started, cpu = time.monotonic(), cpu_seconds()
            for index, sample in enumerate(samples):
                path = sample["path"]
                if signature(path) != sample["signature"]:
                    raise SnapshotError("immutable D sample changed; scratch retained")
                target = private / f"{turn}-{profile}-{index}.blob"
                digest, count = hashlib.sha256(), 0
                reader = binary_reader(path) if profile == "native" else path.open("rb")
                with reader as source, target.open("xb") as output:
                    while block := source.read(4 * 1024**2):
                        output.write(block)
                        digest.update(block)
                        count += len(block)
                    output.flush()
                    os.fsync(output.fileno())
                if (count != sample["signature"][2] or digest.hexdigest() != sample["sha256"]
                        or sha256_file(target) != sample["sha256"] or signature(path) != sample["signature"]):
                    raise SnapshotError("D recovery full SHA/signature differs; scratch retained")
                target.unlink()
            duration = time.monotonic() - started
            row = {"profile": profile, "round": turn + 1, "logical_bytes": total,
                   "complete_workflow_seconds": duration, "cpu_seconds": cpu_seconds() - cpu,
                   "verified_MiB_per_second": total / duration / 1024**2,
                   "full_original_and_restored_sha_verified": True}
            rows.append(row)
            print(json.dumps(row), flush=True)
    if any(private.iterdir()):
        raise SnapshotError("unknown measurement scratch retained")
    private.rmdir()
    medians = {p: statistics.median(r["complete_workflow_seconds"] for r in rows if r["profile"] == p)
               for p in ("drvfs", "native")}
    value = {"state": "real_d_blob_recovery_measured", "samples": [
        {**r, "path": str(r["path"])} for r in samples], "runs": rows, "medians": medians,
        "native_speedup": medians["drvfs"] / medians["native"],
        "scope": "bounded blob copy/fsync/SHA; not a full release or NAS workflow",
        "os_cache_flushed": False, "authoritative_cold_changed": False, "scratch_removed": True}
    atomic_write_json(args.output, value)
    print(json.dumps({"medians": medians, "native_speedup": value["native_speedup"]}))


if __name__ == "__main__":
    main()
