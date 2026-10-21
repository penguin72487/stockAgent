"""Read-only real-input benchmark of complete incremental-version round trips.

Run as a module with runtime_env.sh. Payloads are copied into a private temporary
fixture; the production sources, cold authority and services are never changed.
The reference emulates the former pre-hash + chunk-write source passes. Both
variants retain full cold object/member/original SHA checks and restoration.
"""
from __future__ import annotations

import argparse
import ast
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import resource
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from unittest.mock import patch

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync.desync_snapshots import sha256_file


def signature(path):
    info = path.lstat()
    return tuple(getattr(info, field) for field in
                 ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns"))


def source_proof(path):
    before = signature(path)
    if path.is_symlink() or not path.is_file() or not 8 * 1024**2 < before[2] <= 256 * 1024**2:
        raise ValueError("use a real closed 8..256 MiB source file")
    digest = sha256_file(path)
    if signature(path) != before:
        raise ValueError("benchmark input changed")
    return {"path": str(path), "sha256": digest, "bytes": before[2], "signature": list(before)}


@contextmanager
def former_source_passes(enabled, previous, counts):
    original = packed._store_chunked_source
    def two_pass(root, source, entry, **options):
        if entry.sha256 is None:
            entry.sha256 = packed._hash_source_entry(source, entry)
            counts["source_pre_hash_passes"] += 1
            counts["source_pre_hash_bytes"] += entry.size
            prior = previous.get(entry.path)
            if prior and prior["sha256"] == entry.sha256 and prior["size"] == entry.size:
                entry.storage = dict(prior["storage"])
                return
        original(root, source, entry, **options)
    with patch.object(packed, "_store_chunked_source", two_pass if enabled else original):
        yield


@contextmanager
def recovery_fusion(enabled):
    """Keep the former complete two-pass verifier as a measurement oracle."""
    validate = packed._validate_inventory
    def reference_inventory(*args, **options):
        result = validate(*args, **options)
        return {**result, 'chunk_only_objects': set()}
    with patch.object(packed, '_validate_inventory', validate if enabled else reference_inventory):
        yield


@contextmanager
def native_stream_fixture(cold, *, batched, counts, portable_reference=False):
    """Only this admitted, disposable D fixture uses the private native hooks.

    The reference is the retained per-extent reader, not a skipped integrity
    check. Both run full chunk/original SHA, object verification and restoration.
    Production exact-root admission is unchanged; every native I/O still guards
    the enrolled D volume, and batches add post-read signature/authority checks.
    """
    from stockagent.data_sync import windows_cold_io as native
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    verify, materialize = packed._verify_packed_inventory, packed._materialize_regular_files
    chunks, launch = packed._iter_chunked_file, native._launch
    def admit(root):
        if root != cold or root.resolve() != root:
            raise ValueError('native stream fixture escaped its owned cold root')
        _check_d_primary_mount(Path('/srv/stockagent-packed'))
    def verify_native(root, *args, **options):
        admit(root)
        return verify(root, *args, **{**options, 'd_primary_native_blob_reads': batched or not portable_reference})
    def materialize_native(root, *args, **options):
        admit(root)
        return materialize(root, *args, **{**options, 'd_primary_native_blob_reads': batched or not portable_reference})
    def stream_chunks(root, objects, row, *, native=False, verified_extent=None):
        admit(root)
        if batched or portable_reference:
            yield from chunks(root, objects, row, native=batched, verified_extent=verified_extent)
        else:
            for digest, size in packed._storage_extents(row['storage'], int(row['size'])):
                path = packed._path_under(root, objects[digest]['relpath'], 'fixture chunk relpath')
                yield from packed._iter_single_chunk(path, digest, size, native=True)
                if verified_extent is not None:
                    verified_extent(digest)
    def measured_launch(configs, **options):
        if all(c['mode'] == 'read' for c in configs):
            counts['native_read_processes'] += 1
            counts['native_read_members'] += len(configs)
        return launch(configs, **options)
    with patch.object(packed, '_verify_packed_inventory', verify_native), \
            patch.object(packed, '_materialize_regular_files', materialize_native), \
            patch.object(packed, '_iter_chunked_file', stream_chunks), \
            patch.object(native, '_launch', measured_launch):
        yield


def run_trial(inputs, proofs, *, reference, fused_recovery=None, native_stream_batch=None, cold_parent=None,
              portable_stream_reference=False):
    started = time.perf_counter()
    cpu_started = time.process_time()
    phases, inventories, previous = [], [], {}
    counts = {"source_pre_hash_passes": 0, "source_pre_hash_bytes": 0,
              'native_read_processes': 0, 'native_read_members': 0}
    with tempfile.TemporaryDirectory(prefix="stockagent-version-benchmark-", dir='/var/tmp') as temporary, ExitStack() as fixtures:
        base = Path(temporary)
        source, cold = base / "source", base / "cold"
        source.mkdir()
        if native_stream_batch is not None:
            if cold_parent is None:
                raise ValueError('native stream trial needs an admitted isolated cold parent')
            cold = Path(fixtures.enter_context(tempfile.TemporaryDirectory(
                prefix='stockagent-stream-benchmark-', dir=cold_parent)))
            fixtures.enter_context(native_stream_fixture(cold, batched=native_stream_batch, counts=counts,
                                                         portable_reference=portable_stream_reference))
        (source / "units.json").write_text(json.dumps({"code": "0050", "value": -7, "missing": None,
                                                        "volume_unit": "shares"}))
        selected = []
        with former_source_passes(reference, previous, counts):
            for index, original in enumerate(inputs):
                shutil.copyfile(original, source / "source.parquet")
                if sha256_file(source / "source.parquet") != proofs[index]["sha256"]:
                    raise ValueError("private fixture copy differs from its source")
                stamp = time.perf_counter()
                release = packed.publish_packed_snapshot(cold, "benchmark", source, node_id="test",
                    loose_file_threshold_bytes=8 * 1024**2, file_chunk_bytes=8 * 1024**2,
                    pack_buckets=2, batch_directory_fsync=True)
                publish_seconds = time.perf_counter() - stamp
                inventories.append(release.manifest['archive']['inventory']['sha256'])
                stamp = time.perf_counter()
                if fused_recovery is None:
                    proof = packed.verify_packed_snapshot(cold, release,
                                                          reconstruct_paths=["source.parquet", "units.json"])
                else:
                    with recovery_fusion(fused_recovery):
                        proof = packed.verify_packed_snapshot(cold, release, reconstruct_all=True)
                verify_seconds = time.perf_counter() - stamp
                stamp = time.perf_counter()
                restored = packed.fetch_packed_snapshot(cold, base / "restored", release)
                if (sha256_file(restored / "source.parquet") != proofs[index]["sha256"]
                        or json.loads((restored / "units.json").read_text())["missing"] is not None
                        or proof["independently_reconstructed_files"] != 2):
                    raise ValueError("complete cold restoration failed")
                phases.append({"stage": "baseline" if index == 0 else "incremental",
                    "publish_seconds": publish_seconds, "full_verify_seconds": verify_seconds,
                    "restore_and_readback_seconds": time.perf_counter() - stamp,
                    "new_stored_bytes": release.manifest["archive"]["new_stored_bytes"],
                    "logical_bytes": release.manifest["source"]["logical_bytes"],
                    **({'chunk_object_hash_fusion': proof.get('chunk_object_hash_fusion')}
                       if fused_recovery is not None else {})})
                previous.clear()
                previous.update({row["path"]: row for row in packed._load_inventory(cold, release.manifest)
                                 if row["kind"] == "file"})
                selected.append((release, proofs[index]["sha256"]))
            stamp = time.perf_counter()
            noop = packed.publish_packed_snapshot(cold, "benchmark", source, node_id="test",
                loose_file_threshold_bytes=8 * 1024**2, file_chunk_bytes=8 * 1024**2,
                pack_buckets=2, batch_directory_fsync=True)
            if noop.manifest_sha256 != selected[-1][0].manifest_sha256:
                raise ValueError("unchanged version advanced its semantic head")
            phases.append({"stage": "semantic_noop", "seconds": time.perf_counter() - stamp})
        # The old snapshot is still independently recoverable after updates.
        first, expected = selected[0]
        old = packed.fetch_packed_snapshot(cold, base / "old-restored", first)
        if sha256_file(old / "source.parquet") != expected:
            raise ValueError("incremental update damaged an earlier version")
    variant = ('fused_chunk_recovery' if fused_recovery else 'two_pass_chunk_recovery') if fused_recovery is not None else (
        'former_two_pass' if reference else 'single_pass')
    if native_stream_batch is not None:
        variant = 'batched_native_stream' if native_stream_batch else (
            'portable_chunk_stream' if portable_stream_reference else 'per_extent_native_stream')
    return {"variant": variant,
            "complete_wall_seconds": time.perf_counter() - started,
            "complete_cpu_seconds": time.process_time() - cpu_started,
            "phases": phases, 'inventory_sha256': inventories,
            **counts, "full_roundtrip_verified": True}


def evaluate_cdc(inputs):
    results = []
    for chunking, maximum in (("fixed-v1", 8 * 1024**2), ("fastcdc-v1", 8 * 1024**2),
                              ("fastcdc-v1", 1024**2)):
        started, objects, total, chunks = time.perf_counter(), {}, 0, 0
        for path in inputs:
            with path.open("rb") as stream:
                for block in packed._source_blocks(stream, maximum, chunking):
                    total += len(block)
                    chunks += 1
                    objects[hashlib.sha256(block).hexdigest()] = len(block)
        results.append({"policy": chunking, "maximum_bytes": maximum, "logical_bytes": total,
            "unique_chunk_bytes": sum(objects.values()), "reused_bytes": total - sum(objects.values()),
            "chunks": chunks, "seconds": time.perf_counter() - started})
    return results


def native_chunk_trial(reference_path, reference_sha, candidate_path, candidate_sha, variant):
    """Generated D fixture: existing CAS -> publish -> complete new/old readback.

    The seeded CAS is fixture setup, representing already downloaded chunks.
    Every target publication, cold verification, durable fetch, byte readback,
    no-op, revision and old-version readback remains in complete wall time.
    """
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    primary = Path('/srv/stockagent-packed')
    _check_d_primary_mount(primary)
    parent = primary / '.local-state'
    if parent.resolve() != parent or shutil.disk_usage(parent).free < 32 * 1024**3:
        raise ValueError('native chunk fixture needs the guarded D free-space reserve')
    selected_path, selected_sha = ((reference_path, reference_sha) if variant == 'portable'
                                  else (candidate_path, candidate_sha))
    raw = selected_path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != selected_sha:
        raise ValueError('retained chunk writer identity changed')
    nodes = [node for node in ast.parse(raw).body
             if isinstance(node, ast.FunctionDef) and node.name == '_store_chunked_source']
    if len(nodes) != 1:
        raise ValueError('one retained chunk writer is required')
    namespace = dict(packed.__dict__)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(selected_path), 'exec'), namespace)
    store = namespace['_store_chunked_source']
    with tempfile.TemporaryDirectory(prefix='stockagent-chunk-reuse-benchmark-', dir='/var/tmp') as hot_tmp, \
            tempfile.TemporaryDirectory(prefix='stockagent-chunk-reuse-benchmark-', dir=parent) as cold_tmp:
        hot, cold = Path(hot_tmp), Path(cold_tmp)
        source = hot / 'source'; source.mkdir()
        for number in range(2):
            (source / f'{number}.bin').write_bytes(hashlib.shake_256(str(number).encode()).digest(32 * 1024**2))
        (source / 'units.json').write_bytes(b'{"raw":-7,"missing":null,"unit":"shares"}\n')
        def tree(root):
            return {p.name: sha256_file(p) for p in sorted(root.iterdir()) if p.is_file()}
        def publish(dataset):
            return packed.publish_packed_snapshot(cold, dataset, source, node_id='fixture',
                pack_buckets=2, loose_file_threshold_bytes=1024**2, file_chunk_bytes=8 * 1024**2,
                batch_directory_fsync=True)
        stamp = time.perf_counter()
        publish('donor')
        setup_seconds = time.perf_counter() - stamp
        original = tree(source)
        phases, inventories, versions = [], [], []
        # Only this owned, admitted D fixture relaxes the exact root matcher.
        # The native adapter still checks every original D/member guard.
        def fixture_native(root):
            if root != cold or root.resolve() != root:
                raise ValueError('native fixture escaped its owned cold directory')
            _check_d_primary_mount(primary)
            return True
        started, cpu = time.perf_counter(), time.process_time()
        # Experimental functions live only in this admitted fixture namespace.
        # In particular, a rejected candidate need not remain in production.
        namespace['_native_chunk_reuse_hashes'] = fixture_native
        with patch.object(packed, '_store_chunked_source', store):
            for version in range(2):
                phase = {'stage': 'initial_existing_cas' if version == 0 else 'incremental_revision'}
                stamp = time.perf_counter()
                selected = publish('subject')
                phase['publish_seconds'] = time.perf_counter() - stamp
                stamp = time.perf_counter()
                proof = packed.verify_packed_snapshot(cold, selected, reconstruct_all=True)
                restored = packed.fetch_packed_snapshot(cold, hot / f'restored-{version}', selected)
                if proof['independently_reconstructed_files'] != 3 or tree(restored) != tree(source):
                    raise ValueError('native chunk complete original reconstruction/readback differs')
                phase['full_recovery_materialization_readback_seconds'] = time.perf_counter() - stamp
                phase['new_stored_bytes'] = selected.manifest['archive']['new_stored_bytes']
                phase['logical_bytes'] = selected.manifest['source']['logical_bytes']
                inventories.append(selected.manifest['archive']['inventory']['sha256'])
                versions.append(selected)
                phases.append(phase)
                if version == 0:
                    stamp = time.perf_counter()
                    if publish('subject').manifest_sha256 != selected.manifest_sha256:
                        raise ValueError('semantic no-op advanced the accepted head')
                    phases.append({'stage': 'semantic_noop', 'seconds': time.perf_counter() - stamp})
                    with (source / '0.bin').open('r+b') as stream:
                        stream.write(b'changed original revision')
                        stream.seek(0, 2)
                        stream.write(b'incremental tail')
            stamp = time.perf_counter()
            old = packed.fetch_packed_snapshot(cold, hot / 'old-restored', versions[0])
            if tree(old) != original:
                raise ValueError('incremental revision damaged the previous complete version')
            phases.append({'stage': 'old_version_full_restore_and_readback', 'seconds': time.perf_counter() - stamp})
        wall, spent = time.perf_counter() - started, time.process_time() - cpu
        result = {'variant': variant, 'complete_wall_seconds': wall, 'complete_cpu_seconds': spent,
            'peak_rss_kib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            'fixture_setup_seconds': setup_seconds, 'source_files': 3, 'source_payload_bytes': 64 * 1024**2,
            'phases': phases, 'inventory_sha256': inventories, 'original_tree': original,
            'full_originals_and_old_version_verified': True}
    result['owned_generated_fixture_removed'] = True
    return result


def native_chunk_benchmark(args):
    from scripts.benchmark_lakehouse_source_wave import match_native_limits
    from scripts.verify_lakehouse_workflow_history import owner_identity
    from scripts.benchmark_lakehouse_source_signature import limits
    observed, owner = match_native_limits(), owner_identity()
    identity = {str(path): sha256_file(path) for path in
                (Path(__file__), Path(packed.__file__), args.reference, args.candidate)}
    trials = []
    for variant in ('portable', 'native', 'native', 'portable'):
        child = subprocess.run([sys.executable, '-m', 'scripts.benchmark_dataset_version_pipeline',
            '--native-reused-chunks', '--native-trial', variant, '--reference', str(args.reference),
            '--reference-sha256', args.reference_sha256, '--candidate', str(args.candidate),
            '--candidate-sha256', args.candidate_sha256],
            check=True, capture_output=True, text=True, timeout=1200)
        trials.append(json.loads(child.stdout))
        print(json.dumps({'trial': len(trials), **trials[-1]}), flush=True)
    if (identity != {path: sha256_file(Path(path)) for path in identity}
            or limits() != observed or owner_identity() != owner
            or len({tuple(row['inventory_sha256']) for row in trials}) != 1
            or len({json.dumps(row['original_tree'], sort_keys=True) for row in trials}) != 1):
        raise ValueError('native chunk code/resource/owner/original-byte parity differs')
    means = {variant: {metric: statistics.mean(row[metric] for row in trials if row['variant'] == variant)
        for metric in ('complete_wall_seconds', 'complete_cpu_seconds', 'peak_rss_kib')}
        for variant in ('portable', 'native')}
    result = {'contract': 'native_chunk_reuse_complete_pipeline_abba_v1', 'state': 'verified',
        'observed_at_utc': datetime.now(UTC).isoformat(), 'trials': trials, 'means': means,
        'code_sha256': identity, 'native_resource_limits': observed, 'production_source_writes': 0,
        'production_cold_writes': 0, 'provider_calls': 0, 'hardware_only_limit_claim': False,
        'scope': 'owned 64MiB existing-CAS D fixture, complete initial/noop/revision/full new and old readback'}
    atomic_write_json(args.output, result, durable=True)
    print(json.dumps({'state': result['state'], 'means': means}))
    return 0


def inline_control_trial(*, inline, count=256, cold_parent=None):
    """Compare two safe designs, including durable new/old version readback.

    Eager capture is a design reference, not the failing legacy production
    pipeline. Both designs seal identical generated bytes and retain all gates.
    """
    if type(count) is not int or not 1 <= count <= 4096:
        raise ValueError('use a bounded generated receipt fixture')
    started, cpu = time.perf_counter(), time.process_time()
    phases, inventories, versions = [], [], []
    copies = 0
    with tempfile.TemporaryDirectory(prefix='stockagent-inline-controls-', dir='/var/tmp') as hot_tmp, \
            tempfile.TemporaryDirectory(prefix='stockagent-inline-controls-', dir=cold_parent) as cold_tmp:
        hot, outer = Path(hot_tmp), Path(cold_tmp)
        source = hot / 'data_finmind'
        receipts = source / 'complement/receipts/fixture/all'
        receipts.mkdir(parents=True)
        names = [f'complement/receipts/fixture/all/{number:06d}.json' for number in range(count)]
        for number, name in enumerate(names):
            (source / name).write_bytes(json.dumps({'value': -number, 'missing': None,
                'unit': 'shares', 'payload': 'stable raw bytes ' * 8}, sort_keys=True).encode())
        cold = outer / '.local-state/private-history'
        cold.mkdir(parents=True, mode=0o700)
        atomic_write_json(cold / 'private-history.json',
                          {'shared_export': False, 'nas_export': False, 'source_retirement': False}, durable=True)
        def tree(root):
            return {p.relative_to(root).as_posix(): sha256_file(p)
                    for p in sorted(root.rglob('*')) if p.is_file()}
        first_tree = tree(source)
        def publish(revision):
            nonlocal copies
            overrides = {}
            if not inline:
                for name in names:
                    captured = hot / f'capture-{revision}' / name
                    entry = packed._SourceEntry(path=name, kind='file', mode=0o644,
                                                atomic_json_control=True)
                    atomic_write_bytes(captured, packed._read_atomic_json_control(source, entry), durable=True)
                    overrides[name] = captured
                    copies += 1
            return packed.publish_packed_snapshot(cold, 'inline-controls', source, node_id='fixture',
                pack_buckets=4, batch_directory_fsync=True, file_chunk_bytes=8 * 1024**2,
                private_observation_cohort=True, private_atomic_json_controls=inline,
                file_overrides=overrides)
        for revision in range(2):
            stamp = time.perf_counter()
            selected = publish(revision)
            publication_seconds = time.perf_counter() - stamp
            stamp = time.perf_counter()
            proof = packed.verify_packed_snapshot(cold, selected, reconstruct_all=True)
            restored = packed.fetch_packed_snapshot(cold, hot / f'restored-{revision}', selected)
            if proof['independently_reconstructed_files'] != count or tree(restored) != tree(source):
                raise ValueError('inline receipt complete roundtrip differs')
            phases.append({'stage': 'baseline' if revision == 0 else 'incremental',
                'publish_seconds': publication_seconds,
                'full_verify_restore_readback_seconds': time.perf_counter() - stamp})
            inventories.append(selected.manifest['archive']['inventory']['sha256'])
            versions.append(selected)
            if revision == 0:
                stamp = time.perf_counter()
                if publish('noop').manifest_sha256 != selected.manifest_sha256:
                    raise ValueError('unchanged control cohort advanced its accepted head')
                phases.append({'stage': 'semantic_noop', 'seconds': time.perf_counter() - stamp})
                atomic_write_bytes(source / names[0], b'{"value":-7000,"missing":null,"unit":"shares"}', durable=True)
        stamp = time.perf_counter()
        old = packed.fetch_packed_snapshot(cold, hot / 'old-restored', versions[0])
        if tree(old) != first_tree:
            raise ValueError('incremental control revision damaged the old version')
        phases.append({'stage': 'old_version_full_restore_readback', 'seconds': time.perf_counter() - stamp})
    return {'variant': 'inline' if inline else 'eager_copy_reference', 'phases': phases,
        'complete_wall_seconds': time.perf_counter() - started,
        'complete_cpu_seconds': time.process_time() - cpu, 'inventory_sha256': inventories,
        'loose_control_copies_created': copies, 'source_files': count,
        'full_originals_and_old_version_verified': True, 'owned_generated_fixture_removed': True}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", action="append", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--evaluate-cdc", action="store_true")
    parser.add_argument('--native-reused-chunks', action='store_true')
    parser.add_argument('--native-inline-controls', action='store_true')
    parser.add_argument('--native-stream-batch', action='store_true',
                        help='Compare exact native per-extent versus bounded batched reads on an isolated D fixture')
    parser.add_argument('--native-stream-portable-reference', action='store_true',
                        help='Use the actual migration portable chunk reader as the native batch reference')
    parser.add_argument('--chunk-recovery-fusion', action='store_true',
                        help='Compare complete two-pass versus fused full chunk/original verification')
    parser.add_argument('--native-trial', choices=('portable', 'native'))
    parser.add_argument('--reference', type=Path)
    parser.add_argument('--reference-sha256')
    parser.add_argument('--candidate', type=Path)
    parser.add_argument('--candidate-sha256')
    args = parser.parse_args(argv)
    if args.native_stream_portable_reference and not args.native_stream_batch:
        parser.error('portable stream reference requires --native-stream-batch')
    if args.native_stream_batch and (args.evaluate_cdc or args.native_reused_chunks or args.native_inline_controls
            or args.chunk_recovery_fusion or args.native_trial or args.reference
            or args.reference_sha256 or args.candidate or args.candidate_sha256):
        parser.error('native stream batch requires only real source revisions and a fresh output')
    if args.chunk_recovery_fusion and (args.evaluate_cdc or args.native_reused_chunks
            or args.native_inline_controls or args.native_trial or args.reference
            or args.reference_sha256 or args.candidate or args.candidate_sha256):
        parser.error('chunk recovery fusion requires only source revisions and a fresh output')
    if args.native_inline_controls:
        if (args.source or args.evaluate_cdc or args.native_reused_chunks or args.native_trial
                or args.reference or args.reference_sha256 or args.candidate or args.candidate_sha256
                or args.output is None or args.output.exists() or args.output.is_symlink()
                or not args.output.absolute().is_relative_to(Path('artifacts/benchmarks').absolute())):
            parser.error('inline controls require a fresh benchmark output and no other mode')
        from stockagent.data_sync.cold_primary import _check_d_primary_mount
        primary = Path('/srv/stockagent-packed')
        _check_d_primary_mount(primary)
        parent = primary / '.local-state'
        if parent.resolve() != parent or shutil.disk_usage(parent).free < 32 * 1024**3:
            raise ValueError('inline fixture needs the guarded D free-space reserve')
        identity = {str(path): sha256_file(path) for path in (Path(__file__), Path(packed.__file__))}
        trials = [inline_control_trial(inline=variant, cold_parent=parent)
                  for variant in (False, True, True, False)]
        if (identity != {path: sha256_file(Path(path)) for path in identity}
                or len({tuple(row['inventory_sha256']) for row in trials}) != 1):
            raise ValueError('inline benchmark code or complete original inventories differ')
        means = {variant: statistics.mean(row['complete_wall_seconds'] for row in trials
                    if row['variant'] == variant) for variant in ('eager_copy_reference', 'inline')}
        result = {'contract': 'native_inline_control_complete_pipeline_abba_v1', 'state': 'verified',
            'observed_at_utc': datetime.now(UTC).isoformat(), 'trials': trials,
            'mean_complete_seconds': means, 'code_sha256': identity,
            'production_source_writes': 0, 'production_cold_writes': 0, 'provider_calls': 0,
            'hardware_only_limit_claim': False,
            'scope': '256 generated receipts on guarded D; eager-copy alternative versus inline; not production migration speed'}
        atomic_write_json(args.output, result, durable=True)
        print(json.dumps({'state': result['state'], 'mean_complete_seconds': means}))
        return 0
    if args.native_reused_chunks:
        if (args.source or args.evaluate_cdc or args.reference is None or not args.reference_sha256
                or args.candidate is None or not args.candidate_sha256):
            parser.error('native reuse requires fixed reference/candidate identities and no source/CDC override')
        if args.native_trial:
            print(json.dumps(native_chunk_trial(args.reference, args.reference_sha256,
                args.candidate, args.candidate_sha256, args.native_trial)))
            return 0
        if (args.output is None or args.output.exists() or args.output.is_symlink()
                or not args.output.absolute().is_relative_to(Path('artifacts/benchmarks').absolute())):
            parser.error('a fresh private benchmark output is required')
        return native_chunk_benchmark(args)
    if args.native_trial or args.reference or args.reference_sha256 or args.candidate or args.candidate_sha256:
        parser.error('native reference/trial options require --native-reused-chunks')
    if args.output is None or args.source is None:
        parser.error('source revisions and a fresh output are required')
    if not 2 <= len(args.source) <= 4 or args.output.exists() or args.output.is_symlink():
        parser.error("use 2..4 closed real input revisions and a new output receipt")
    inputs = [path.absolute() for path in args.source]
    proofs = [source_proof(path) for path in inputs]
    from stockagent.data_sync import windows_cold_io as native_io
    code_paths = (Path(__file__), Path(packed.__file__), Path(native_io.__file__), native_io.SCRIPT)
    code_identity = {str(path): sha256_file(path) for path in code_paths}
    if args.native_stream_batch:
        from stockagent.data_sync.cold_primary import _check_d_primary_mount
        from scripts.benchmark_lakehouse_source_wave import match_native_limits
        from scripts.verify_lakehouse_workflow_history import owner_identity
        from scripts.benchmark_lakehouse_source_signature import limits
        primary = Path('/srv/stockagent-packed')
        _check_d_primary_mount(primary)
        parent = primary / '.local-state'
        if parent.resolve() != parent or shutil.disk_usage(parent).free < 32 * 1024**3:
            raise ValueError('native stream fixture needs the guarded D free-space reserve')
        observed, owner = match_native_limits(), owner_identity()
        trials = [run_trial(inputs, proofs, reference=False, fused_recovery=True,
                    native_stream_batch=variant, cold_parent=parent,
                    portable_stream_reference=args.native_stream_portable_reference)
                  for variant in (False, True, True, False)]
        if (limits() != observed or owner_identity() != owner
                or code_identity != {str(path): sha256_file(path) for path in code_paths}
                or len({tuple(row['inventory_sha256']) for row in trials}) != 1
                or any(list(signature(path)) != proof['signature'] or sha256_file(path) != proof['sha256']
                       for path, proof in zip(inputs, proofs, strict=True))):
            raise ValueError('native stream complete code/resource/owner/input/inventory parity differs')
        reference_variant = 'portable_chunk_stream' if args.native_stream_portable_reference else 'per_extent_native_stream'
        means = {variant: statistics.mean(row['complete_wall_seconds'] for row in trials
                    if row['variant'] == variant) for variant in (reference_variant, 'batched_native_stream')}
        result = {'contract': 'native_stream_complete_pipeline_abba_v1', 'state': 'verified',
            'observed_at_utc': datetime.now(UTC).isoformat(), 'sources': proofs, 'trials': trials,
            'mean_complete_seconds': means,
            'reference_variant': reference_variant,
            'complete_speed_ratio': means[reference_variant] / means['batched_native_stream'],
            'code_sha256': code_identity, 'native_resource_limits': observed,
            'production_source_writes': 0, 'production_cold_writes': 0, 'provider_calls': 0,
            'complete_inventory_parity_verified': True, 'hardware_only_limit_claim': False,
            'scope': 'isolated guarded D fixture; input copies, baseline/update/noop, full SHA, durable new/old restore, readback and fixture removal'}
        atomic_write_json(args.output, result, durable=True)
        print(json.dumps({key: result[key] for key in ('state', 'mean_complete_seconds', 'complete_speed_ratio')}))
        return 0
    trials = ([run_trial(inputs, proofs, reference=False, fused_recovery=variant)
               for variant in (False, True, True, False)] if args.chunk_recovery_fusion else
              [run_trial(inputs, proofs, reference=variant) for variant in (True, False, False, True)])
    cdc = evaluate_cdc(inputs) if args.evaluate_cdc else None
    for path, proof in zip(inputs, proofs, strict=True):
        if list(signature(path)) != proof["signature"] or sha256_file(path) != proof["sha256"]:
            raise ValueError("production input changed during benchmark; retain unaccepted results")
    if code_identity != {str(path): sha256_file(path) for path in code_paths}:
        raise ValueError('benchmark code changed; retain unaccepted results')
    if args.chunk_recovery_fusion:
        if len({tuple(row['inventory_sha256']) for row in trials}) != 1:
            raise ValueError('two-pass and fused complete original inventories differ')
        means = {variant: statistics.mean(row['complete_wall_seconds'] for row in trials
                    if row['variant'] == variant) for variant in ('two_pass_chunk_recovery', 'fused_chunk_recovery')}
        phases = {variant: statistics.mean(sum(phase.get('full_verify_seconds', 0) for phase in row['phases'])
                    for row in trials if row['variant'] == variant) for variant in means}
        result = {'contract': 'full_chunk_recovery_complete_pipeline_abba_v1', 'state': 'verified',
            'observed_at_utc': datetime.now(UTC).isoformat(), 'sources': proofs, 'trials': trials,
            'mean_complete_seconds': means, 'mean_full_verify_seconds': phases,
            'complete_speed_ratio': means['two_pass_chunk_recovery'] / means['fused_chunk_recovery'],
            'code_sha256': code_identity, 'source_payload_writes': 0, 'production_cold_writes': 0,
            'provider_calls': 0, 'complete_inventory_parity_verified': True, 'hardware_only_limit_claim': False,
            'caveats': ['Warm local SSD fixture, not production D/NAS throughput or cold-cache proof.',
                        'ABBA retains input copy, baseline, update, no-op, full verification, durable new/old restore and cleanup.']}
        atomic_write_json(args.output, result, durable=True)
        print(json.dumps({key: result[key] for key in
                         ('state', 'mean_complete_seconds', 'mean_full_verify_seconds', 'complete_speed_ratio')}))
        return 0
    reference = statistics.mean(t["complete_wall_seconds"] for t in trials if t["variant"] == "former_two_pass")
    candidate = statistics.mean(t["complete_wall_seconds"] for t in trials if t["variant"] == "single_pass")
    result = {"contract": "dataset_version_complete_pipeline_benchmark_v1", "state": "verified",
        "observed_at_utc": datetime.now(UTC).isoformat(), "sources": proofs, "trials": trials,
        "mean_complete_seconds": {"former_two_pass": reference, "single_pass": candidate},
        "complete_speed_ratio": reference / candidate, "max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "cdc": cdc, "source_payload_writes": 0, "production_cold_writes": 0, "provider_calls": 0,
        "caveats": ["Warm local SSD fixture, not an end-to-end D/NAS throughput claim.",
                    "Four A/B/B/A trials retain copying, hashing, publication, complete verification, old-version restore and cleanup.",
                    "Raw Parquet chunk reuse is measured, not inferred from synthetic insertion tests."]}
    atomic_write_json(args.output, result, durable=True)
    print(json.dumps({key: result[key] for key in ("state", "mean_complete_seconds", "complete_speed_ratio", "cdc")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
