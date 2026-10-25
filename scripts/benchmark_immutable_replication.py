"""Compare full immutable copy/retry/resume with fixed real sealed inputs.

The source is an existing D release and remains read-only. Each ABBA trial has
its own private destination; only that probe's READY is removed to exercise
resume. All native SHA, exact-set, conflict and post-copy checks run unchanged.
This measures replication, not the complete catalog/migration/NAS workflow.
"""
from __future__ import annotations

import argparse
import ast
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import resource
import shutil
import statistics
import time
from types import ModuleType

from downloader.artifact_io import atomic_write_json, atomic_write_text
from scripts.benchmark_lakehouse_source_wave import limits, match_native_limits
from stockagent.control import lakehouse
from stockagent.data_sync import immutable_replication as immutable


def load_module(source, label):
    result = ModuleType('immutable_replication_' + label)
    exec(compile(source, '<preserved-immutable-' + label + '>', 'exec'), result.__dict__)
    return result


def validate_scope(reference, candidate):
    def remainder(source):
        tree = ast.parse(source)
        tree.body = [node for node in tree.body if not (
            isinstance(node, ast.FunctionDef)
            and node.name in {'verify', '_verified_inventory', 'replicate'})]
        return ast.dump(tree, include_attributes=False)
    if remainder(reference) != remainder(candidate):
        raise ValueError('unrelated changes invalidate the replication comparator')


def trial(active, label, source, destination, rclone, expected):
    inventories = []
    original = active.inventory

    def measured(root):
        start, cpu = time.perf_counter(), time.process_time()
        value = original(root)
        inventories.append({'role': 'source' if root == source else 'destination',
            'wall_seconds': time.perf_counter() - start,
            'cpu_seconds': time.process_time() - cpu,
            'files': len(value), 'bytes': sum(row['bytes'] for row in value.values())})
        return value

    active.inventory = measured
    wall, cpu = time.perf_counter(), time.process_time()
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    proofs = []
    try:
        for case in ('fresh_copy', 'retry_noop', 'resume_missing_ready'):
            if case == 'resume_missing_ready':
                # Explicit fault injection on this trial's owned probe only.
                immutable.safe(destination, 'READY').unlink()
            proof = active.replicate(source, destination, rclone)
            semantic = {key: value for key, value in proof.items()
                        if key not in {'complete_workflow_seconds', 'accepted_at_utc'}}
            if (proof['complete_files'] != len(expected)
                    or proof['complete_bytes'] != sum(row['bytes'] for row in expected.values())
                    or not all(proof[key] is True for key in (
                        'all_files_sha256_verified', 'exact_file_set_verified', 'source_unchanged_verified'))):
                raise ValueError('full replication acceptance or file coverage differs')
            proofs.append({'case': case, 'proof': proof, 'semantic': semantic})
        # Extra independent final readback is included in the full stopwatch.
        if immutable.verify(destination) != immutable.verify(source):
            raise ValueError('independent destination/source verification differs')
    finally:
        active.inventory = original
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    return {'label': label, 'wall_seconds': time.perf_counter() - wall,
            'cpu_seconds': time.process_time() - cpu,
            'child_cpu_seconds': (after.ru_utime + after.ru_stime - children.ru_utime - children.ru_stime),
            'resource_limits': limits(), 'inventories': inventories, 'cases': proofs,
            'source_inventory_passes': sum(row['role'] == 'source' for row in inventories),
            'native_inventory_bytes': sum(row['bytes'] for row in inventories),
            'independent_full_source_destination_readback': True}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--reference', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    output = args.output.absolute()
    if (output.exists() or output.is_symlink()
            or not output.resolve().is_relative_to(Path('artifacts/operations').resolve())):
        parser.error('a fresh private operations directory is required')
    native_limits = match_native_limits()
    c = lakehouse.configuration()
    lakehouse.guard(c)
    source = args.source.absolute()
    manifest = immutable.read_manifest(source)
    if (source.parent != Path(c['lake_root']) / 'releases'
            or source.name != 'lake-' + manifest['identity_sha256']):
        raise ValueError('use one exact existing sealed primary D release')
    reference = args.reference.read_text()
    candidate_path = Path(immutable.__file__)
    candidate = candidate_path.read_text()
    validate_scope(reference, candidate)
    logical = sum(row['bytes'] for row in manifest['files'].values())
    if shutil.disk_usage(output.parent).free < 25 * 1024**3 + 4 * (logical + 16 * 1024**2):
        raise ValueError('capacity admission preserves 25GiB plus all four probe destinations')
    output.mkdir(mode=0o700)
    atomic_write_text(output / 'reference.py', reference, mode=0o600, durable=True)
    atomic_write_text(output / 'candidate.py', candidate, mode=0o600, durable=True)
    expected = immutable._verified_inventory(source)[1]
    atomic_write_json(output / 'input-inventory.json', expected, durable=True)
    fixture = {'source': str(source), 'delivery_identity_sha256': manifest['identity_sha256'],
        'complete_files': len(expected), 'complete_bytes': sum(row['bytes'] for row in expected.values()),
        'reference_sha256': hashlib.sha256(reference.encode()).hexdigest(),
        'candidate_sha256': hashlib.sha256(candidate.encode()).hexdigest(),
        'resource_limits': native_limits, 'rclone': str(Path(c['binaries']) / 'bin/rclone')}
    atomic_write_json(output / 'fixture.json', fixture, durable=True)
    variants = {name: load_module(text, name) for name, text in (
        ('baseline', reference), ('candidate', candidate))}
    rows = []
    for number, label in enumerate(('baseline', 'candidate', 'candidate', 'baseline')):
        root = output / f'trial-{number}-{label}'
        root.mkdir(mode=0o700)
        row = trial(variants[label], label, source, root / 'payload',
                    Path(c['binaries']) / 'bin/rclone', expected)
        rows.append(row)
        atomic_write_json(output / f'trial-{number}.json', row, durable=True)
        print(json.dumps({key: row[key] for key in (
            'label', 'wall_seconds', 'cpu_seconds', 'source_inventory_passes', 'native_inventory_bytes')}), flush=True)
    if immutable._verified_inventory(source)[1] != expected:
        raise ValueError('fixed source changed across the complete ABBA workflow')
    if candidate_path.read_text() != candidate or args.reference.read_text() != reference:
        raise ValueError('implementation changed across the comparison')
    semantic = [item['semantic'] for row in rows for item in row['cases']]
    if any(value != semantic[0] for value in semantic) or any(row['resource_limits'] != native_limits for row in rows):
        raise ValueError('semantic output or resource parity differs')
    means = {label: {key: statistics.mean(row[key] for row in rows if row['label'] == label)
                    for key in ('wall_seconds', 'cpu_seconds', 'child_cpu_seconds', 'native_inventory_bytes')}
             for label in variants}
    result = {'state': 'accepted_fixed_input_replication_abba',
        'observed_at_utc': datetime.now(UTC).isoformat(), 'fixture': fixture, 'means': means,
        'trials': rows, 'all_twelve_acceptance_semantics_identical': True,
        'independent_source_destination_full_readback': True,
        'whole_replication_workflow_improved': means['candidate']['wall_seconds'] < means['baseline']['wall_seconds'],
        'source_writes': 0, 'production_catalog_writes': 0, 'production_service_controls': 0,
        'probe_ready_fault_injections': 4, 'probe_payload_retired': False,
        'scope': 'Fixed D release to private SSD; full copy, retry, missing-READY resume and readback. '
                 'Not complete export, migration, NAS acceptance, cold-cache throughput or a hardware limit.'}
    atomic_write_json(output / 'measurement.json', result, durable=True)
    print(json.dumps({key: result[key] for key in ('state', 'whole_replication_workflow_improved', 'means')}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
