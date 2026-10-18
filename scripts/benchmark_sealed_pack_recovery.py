"""ABBA complete canonical publication/recovery after a real child-process exit.

Only generated, owned /var/tmp fixtures are touched. Includes the abandoned
attempt, retry, full original SHA/CRC, durable readback, incremental revision,
and independent old-version restore. Not a D/NAS or all-source speed claim.
"""
from __future__ import annotations

import argparse
from contextlib import nullcontext
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import resource
import statistics
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync import dataset_migration as migration
from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync.packing_checkpoint import SealedPackHints

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = 'sealed_pack_retry_complete_pipeline_abba_v1'


def identity():
    names = (Path(__file__), Path(packed.__file__), Path(migration.__file__),
             ROOT / 'stockagent/data_sync/packing_checkpoint.py',
             ROOT / 'stockagent/data_sync/packed_metadata.py')
    return {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in names}


def digest_tree(path):
    digest = hashlib.sha256()
    for member in sorted(path.iterdir()):
        digest.update(member.name.encode() + b'\0')
        with member.open('rb') as stream:
            while block := stream.read(8 * 1024**2):
                digest.update(block)
    return digest.hexdigest()


def inputs(base, enabled):
    source, parent = base / 'source', base / 'cold'
    policy = {'private_history': {'enabled': True, 'relative_root': '.local-state/private-history',
        'shared_export': False, 'nas_export': False, 'source_retirement': False}}
    cold = migration.private_history_root(parent, policy)
    index = SealedPackHints(base / 'journal/sealed-pack-checkpoints/example', cold, source, 'example') if enabled else None
    return cold, source, index


def publish(base, enabled):
    cold, source, index = inputs(base, enabled)
    selected = packed.publish_packed_snapshot(cold, 'example', source, node_id='fixture',
        pack_buckets=1, file_chunk_bytes=1024**2, batch_directory_fsync=True,
        private_observation_cohort=True, sealed_pack_hints=index)
    return selected, index


def admit_fixture(base):
    info = base.lstat()
    if (base.resolve() != base or base.parent != Path('/var/tmp')
            or not base.name.startswith('stockagent-sealed-pack-benchmark-')
            or info.st_uid != os.geteuid() or info.st_mode & 0o777 != 0o700):
        raise ValueError('only the exact owned private benchmark fixture is permitted')


def trial(files, enabled):
    with tempfile.TemporaryDirectory(prefix='stockagent-sealed-pack-benchmark-', dir='/var/tmp') as tmp:
        base = Path(tmp)
        source = base / 'source'
        source.mkdir()
        for number in range(files):
            raw = json.dumps({'value': -number, 'missing': None, 'unit': 'shares',
                'payload': hashlib.sha256(str(number).encode()).hexdigest() * 128}).encode()
            (source / f'{number:06}.json').write_bytes(raw)
        packed.initialize_packed_layout(base / 'cold', node_id='fixture')
        cold, _, _ = inputs(base, enabled)
        expected = digest_tree(source)
        cpu = time.process_time()
        children_cpu = resource.getrusage(resource.RUSAGE_CHILDREN)
        stamp = time.perf_counter()
        child = subprocess.run([sys.executable, '-m', 'scripts.benchmark_sealed_pack_recovery',
            '--crash-child', str(base), '--files', str(files), *(['--enabled'] if enabled else [])],
            capture_output=True, text=True, timeout=600)
        if child.returncode != 88 or list((cold / 'heads/example').glob('*.json')):
            raise ValueError('the fixed deliberate exit was not an uncommitted attempt')
        original_writer = packed._write_pack
        writes = []
        def observed(root, entries, target, **options):
            writes.append(len(entries))
            return original_writer(root, entries, target, **options)
        packed._write_pack = observed
        try:
            first, index = publish(base, enabled)
        finally:
            packed._write_pack = original_writer
        retry_files_written = sum(writes)
        first_proof = packed.verify_packed_snapshot(cold, first, reconstruct_all=True)
        restored = packed.fetch_packed_snapshot(cold, base / 'first-readback', first)
        if digest_tree(restored) != expected:
            raise ValueError('all original source bytes differ after retry')
        inventories = [first.manifest['archive']['inventory']['sha256']]
        if index is not None:
            retirement = migration._retire_owned_scratch(base / 'journal', 'example', {
                'sealed_pack_hint_files': list(index.receipts.values())})
            if retirement['state'] != 'retired' or list(index.root.glob('*.json')):
                raise ValueError('verified disposable checkpoint metadata was not retired')
        (source / '000000.json').write_bytes(b'revised current value,-2.5,NULL\n')
        (source / '000001.json').unlink()
        (source / 'new.json').write_bytes(b'new published observation\n')
        second, _ = publish(base, enabled)
        second_proof = packed.verify_packed_snapshot(cold, second, reconstruct_all=True)
        now = packed.fetch_packed_snapshot(cold, base / 'second-readback', second)
        if digest_tree(now) != digest_tree(source):
            raise ValueError('incremental all-file readback differs')
        old = packed.fetch_packed_snapshot(cold, base / 'old-readback', first)
        if digest_tree(old) != expected:
            raise ValueError('the old fixed original version was damaged')
        inventories.append(second.manifest['archive']['inventory']['sha256'])
        child_usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        spent = time.process_time() - cpu + child_usage.ru_utime + child_usage.ru_stime - children_cpu.ru_utime - children_cpu.ru_stime
        return {'variant': 'checkpoint' if enabled else 'reference',
            'complete_wall_seconds': time.perf_counter() - stamp, 'complete_cpu_seconds': spent,
            'peak_process_rss_bytes': max(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss, child_usage.ru_maxrss) * 1024,
            'source_files_per_version': files, 'retry_pack_member_writes': retry_files_written,
            'retry_full_source_hash_and_pack_recovery': True,
            'first_files_independently_reconstructed': first_proof['independently_reconstructed_files'],
            'second_files_independently_reconstructed': second_proof['independently_reconstructed_files'],
            'old_version_all_original_readback': True, 'inventory_sha256': inventories,
            'hint_statistics': index.statistics if index else None,
            'deliberate_crash_exit': child.returncode, 'production_source_payloads_deleted': 0,
            'generated_fixture_deletions': 1}


def mutable_trial(files, enabled):
    """Full v3 upgrade -> publish/retry -> cold readback -> owned cleanup.

    The writer replaces its tail with IDENTICAL bytes after sealing the pack.
    Both variants must recover the exact same values. The reference retains
    the old full control recensus and ordinary payload-stat rejection; its
    real one-second bounded retry delay remains in complete wall time.
    """
    with tempfile.TemporaryDirectory(prefix='stockagent-sealed-pack-benchmark-', dir='/var/tmp') as tmp:
        base = Path(tmp)
        source, cold = base / 'data_bybit', base / 'cold'
        (base / 'configs').mkdir()
        configuration = json.loads((ROOT / 'configs/columnar_storage.json').read_text())
        configuration['source_versioning']['roots'] = {}
        atomic_write_json(base / 'configs/columnar_storage.json', configuration)
        (source / 'raw').mkdir(parents=True)
        for number in range(files):
            (source / f'raw/{number:06}.bin').write_bytes(
                json.dumps({'value': -number, 'missing': None, 'unit': 'shares'}).encode() * 64)
        mutable = source / '1m/_hot_tail/EXAMPLE.parquet'
        mutable.parent.mkdir(parents=True)
        mutable.write_bytes(b'fixed original raw values=-7,NULL,shares\n' * 512)
        entry = {'dataset': 'example', 'source': str(source), 'publish': False, 'role': 'source',
                 'note': 'owned generated fixture', 'pack_buckets': 1, 'loose_threshold_mib': 8,
                 'file_chunk_bytes': 8 * 1024**2, 'reader_contract': 'packed-v2-chunks'}
        args = SimpleNamespace(sync_root=cold, migration_state=base / 'journal', node_id='fixture',
            retry_seconds=1, maximum_passes=1, watch=False, recover_missing_base_objects=False,
            source_lock_fd=None, retire_staging=True)
        packed.initialize_packed_layout(cold, node_id='fixture')
        private = migration.private_history_root(cold, configuration['source_versioning'])
        with patch.object(migration, '_mutable_published_locations', lambda *_: {}):
            prepared = migration._private_preparation(entry, args, source, private,
                                                      configuration['source_versioning'])
        prepared['control_scope'] = 'recursive_mutable_controls_v3'
        atomic_write_json(migration._preparation_path(args, 'example'), prepared)
        intent_path = Path(prepared['controls_staging']).with_suffix('.intent.json')
        intent = json.loads(intent_path.read_text())
        atomic_write_json(intent_path, {**intent, 'contract': 'recursive_mutable_controls_v3'})

        def tree(root):
            return {p.relative_to(root).as_posix(): packed.sha256_file(p)
                    for p in sorted(root.rglob('*')) if p.is_file()}
        expected = tree(source)
        native_write, native_freeze = packed._write_pack, migration._freeze_controls
        writes, replaced = [], []
        def writer(root, entries, target, **options):
            result = native_write(root, entries, target, **options)
            writes.append(len(entries))
            if not replaced:
                replacement = mutable.with_name('.same-observation')
                replacement.write_bytes(mutable.read_bytes())
                replacement.replace(mutable)
                replaced.append(True)
            return result
        def old_controls(*argv, **options):
            return native_freeze(*argv, **{**options, 'upgrade_from': None})
        started, cpu_started = time.perf_counter(), time.process_time()
        with patch.object(packed, '_write_pack', writer), \
                (nullcontext() if enabled else patch.object(migration, '_mutable_published_locations', lambda *_: {})), \
                (nullcontext() if enabled else patch.object(migration, '_freeze_controls', old_controls)):
            result = migration.migrate_catalog(base, args, [entry])
            if not enabled:
                if result['datasets']['example']['state'] != 'waiting_retry':
                    raise ValueError('reference failed to exercise the mutable-payload rejection')
                time.sleep(1.05)  # Real retry deadline; no journal edit or fake clock.
                result = migration.migrate_catalog(base, args, [entry])
        if result['state'] != 'verified' or not replaced:
            raise ValueError('complete mutable-cohort publication did not verify')
        selected = packed.resolve_latest_packed(private, 'example')
        readback = packed.fetch_packed_snapshot(private, base / 'restored', selected)
        if tree(readback) != expected or tree(source) != expected:
            raise ValueError('frozen/full-retry versions differ from original values')
        row = result['datasets']['example']
        proof = json.loads(Path(row['recovery_receipt']).read_text())
        if (proof['independently_reconstructed_files'] != files + 1
                or row['owned_scratch_retirement']['state'] != 'retired'):
            raise ValueError('original-file recovery or owned cleanup was skipped')
        return {'variant': 'checkpoint' if enabled else 'reference',
            'complete_wall_seconds': time.perf_counter() - started,
            'complete_cpu_seconds': time.process_time() - cpu_started,
            'peak_process_rss_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
            'retry_pack_member_writes': sum(writes), 'attempts': row['attempts'],
            'inventory_sha256': [selected.manifest['archive']['inventory']['sha256']],
            'full_original_readback': True, 'owned_scratch_retired': True,
            'reference_retry_delay_seconds': 1.05 if not enabled else 0,
            'production_source_payloads_deleted': 0, 'generated_fixture_deletions': 1}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--files', type=int, default=2048)
    parser.add_argument('--trial', choices=('reference', 'checkpoint'))
    parser.add_argument('--crash-child', type=Path)
    parser.add_argument('--enabled', action='store_true')
    parser.add_argument('--native-limits', action='store_true')
    parser.add_argument('--mutable-cohort', action='store_true',
                        help='measure complete v3 control upgrade and an atomic mutable-output rewrite')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args(argv)
    if not 32 <= args.files <= 16384 or args.files % 8:
        parser.error('use a bounded multiple-of-eight fixture (32..16384 files)')
    packed.MAX_PACK_MEMBERS = args.files // 8
    if args.crash_child:
        admit_fixture(args.crash_child)
        original = packed._write_pack
        count = 0
        def crash(root, entries, target, **options):
            nonlocal count
            count += 1
            if count == 8:
                target.write_bytes(b'UNCOMMITTED partial ZIP; never a sealed hint')
                os._exit(88)  # Real process termination: bypass ExitStack/cleanup.
            return original(root, entries, target, **options)
        packed._write_pack = crash
        publish(args.crash_child, args.enabled)
        raise ValueError('deliberate mid-publication exit was not exercised')
    if args.trial:
        function = mutable_trial if args.mutable_cohort else trial
        print(json.dumps(function(args.files, args.trial == 'checkpoint')))
        return 0
    if (args.output is None or args.output.exists() or args.output.is_symlink()
            or not args.output.resolve().is_relative_to((ROOT / 'artifacts/benchmarks').resolve())):
        parser.error('a fresh private benchmark receipt is required')
    code = identity()
    limits, owner = None, None
    if args.native_limits:
        from scripts.benchmark_lakehouse_source_wave import match_native_limits
        from scripts.verify_lakehouse_workflow_history import owner_identity
        limits, owner = match_native_limits(), owner_identity()
    results = []
    for variant in ('reference', 'checkpoint', 'checkpoint', 'reference'):
        result = subprocess.run([sys.executable, '-m', 'scripts.benchmark_sealed_pack_recovery',
            '--files', str(args.files), '--trial', variant,
            *(['--mutable-cohort'] if args.mutable_cohort else [])],
            check=True, capture_output=True, text=True, timeout=1200)
        row = json.loads(result.stdout)
        results.append(row)
        print(json.dumps({'trial': len(results), **row}), flush=True)
    if code != identity() or len({tuple(r['inventory_sha256']) for r in results}) != 1:
        raise ValueError('fixed code/inventory parity failed')
    if args.native_limits:
        from scripts.benchmark_lakehouse_source_signature import limits as observed_limits
        if limits != observed_limits() or owner != owner_identity():
            raise ValueError('native owner or resource limits changed')
    means = {variant: {metric: statistics.mean(row[metric] for row in results if row['variant'] == variant)
        for metric in ('complete_wall_seconds', 'complete_cpu_seconds', 'peak_process_rss_bytes', 'retry_pack_member_writes')}
        for variant in ('reference', 'checkpoint')}
    proof = {'contract': ('mutable_cohort_complete_retry_abba_v1' if args.mutable_cohort else CONTRACT),
        'state': 'accepted_complete_mutable_retry_abba' if args.mutable_cohort else 'accepted_complete_crash_retry_abba',
        'observed_at_utc': datetime.now(UTC).isoformat(), 'trials': results, 'means': means,
        'code_sha256': code, 'native_resource_limits': limits, 'production_migration_claim': False,
        'scope': 'owned local SSD generated fixture, not D/NAS/all providers',
        'provider_calls': 0, 'production_source_writes': 0, 'production_cold_writes': 0}
    atomic_write_json(args.output, proof, durable=True)
    print(json.dumps({'state': proof['state'], 'means': means}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
