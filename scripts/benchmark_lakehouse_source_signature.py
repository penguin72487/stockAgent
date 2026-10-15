"""Whole idle-register replay on every native metadata path, never a publisher.

The fresh reuse gate is an explicit engineering fixture. It does NOT assert
that current source heads match a committed catalog or replace a full capture.
Both variants run the original register function, real mount guards, real
READ_ONLY DuckLake queries, private status publication and durable readback.
Only the four repeated stats in its signature expression differ.
"""
from __future__ import annotations

import argparse
import ast
from contextlib import ExitStack
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import statistics
import time
from types import ModuleType
from unittest.mock import patch

from downloader.artifact_io import atomic_write_json, atomic_write_text
from stockagent.control import lakehouse
from stockagent.data_sync import backup_stream
from stockagent.data_sync.testing_disposition import DEFAULT_POLICY
from stockagent.runtime_identity import identity_sha256


ONE_STAT_SOURCE = '''
def _source_metadata_signature(paths, include_orphans):
    entries = []
    for path in paths:
        observed = path.stat()
        entries.append((str(path), observed.st_ino, observed.st_size,
                        observed.st_mtime_ns, observed.st_ctime_ns))
    return identity_sha256([include_orphans, entries])
'''


def candidate_source(source):
    """Change exactly the signature lambda, preserving the original workflow."""
    tree = ast.parse(source)
    register = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'register')
    assignments = [node for node in ast.walk(register) if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == 'source_signature' for target in node.targets)]
    if len(assignments) != 1 or not isinstance(assignments[0].value, ast.Lambda):
        raise ValueError('preserved original signature boundary differs')
    if ast.unparse(assignments[0].value).count('.stat()') != 4:
        raise ValueError('reference does not contain the original four stat calls')
    assignments[0].value = ast.parse('lambda: _source_metadata_signature(paths, include_orphans)', mode='eval').body
    tree.body.insert(tree.body.index(register), ast.parse(ONE_STAT_SOURCE).body[0])
    return ast.unparse(ast.fix_missing_locations(tree)) + '\n'


def module(source, label):
    result = ModuleType('lakehouse_signature_' + label)
    exec(compile(source, '<preserved-lakehouse-' + label + '>', 'exec'), result.__dict__)
    return result


def metadata_paths(c):
    cold = Path(c['cold_root'])
    paths = [Path('configs/data_sync/packed_datasets.json'),
             Path('configs/data_sync/backup_history_disposition_20261004.json'),
             *sorted((cold / 'heads').glob('*/*.json')),
             *sorted((cold / 'manifests').glob('*/*.json'))]
    if DEFAULT_POLICY.exists():
        paths.append(DEFAULT_POLICY)
    return paths


def one_stat_signature(paths, include_orphans, observations=None):
    def record(value):
        if observations is not None:
            observations.append(value)
        return identity_sha256(value)

    namespace = {'identity_sha256': record}
    exec(ONE_STAT_SOURCE, namespace)
    return namespace['_source_metadata_signature'](paths, include_orphans)


def limits():
    group = next(line.partition('::')[2] for line in Path('/proc/self/cgroup').read_text().splitlines() if line.startswith('0::'))
    root = Path('/sys/fs/cgroup') / group.lstrip('/')
    return {'nice': os.getpriority(os.PRIO_PROCESS, 0),
            'cpu_affinity': sorted(os.sched_getaffinity(0)),
            **{name: (root / name).read_text().strip() for name in ('cpu.max', 'memory.high', 'memory.max')}}


def run_trial(active, c, root, initial_status, token, include_orphans, clock):
    root.mkdir(mode=0o700)
    trial_config = {**c, 'state_root': str(root)}
    atomic_write_json(root / 'catalog-status.json', initial_status)
    atomic_write_json(root / 'catalog-input-signature.json', {
        'registry_contract': 2, 'input_signature_sha256': token,
        'full_capture_epoch': time.time(), 'full_capture_observed_at_utc': clock.isoformat()})
    connections, observed_tokens, observed_inputs = [], [], []

    def recorded_identity(value):
        # Observe the hash of the real complete native metadata pass. Do not
        # substitute recorded stat fields or manufacture a matching token.
        result = identity_sha256(value)
        observed_tokens.append(result)
        observed_inputs.append(value)
        return result

    def read_only(config, **kwargs):
        if kwargs != {'read_only': True} or config != trial_config:
            raise ValueError('engineering replay must never create a writable connection')
        connections.append(True)
        return lakehouse.connect(c, read_only=True)

    def forbidden_capture(*args, **kwargs):
        atomic_write_json(root / 'native-source-signature-input.json', observed_inputs, durable=True)
        atomic_write_json(root / 'rejection.json', {
            'state': 'rejected_changed_idle_inputs',
            'criterion': ('native_metadata_token_changed' if observed_tokens != [token]
                          else 'native_catalog_reuse_not_proven'),
            'expected_source_token': token, 'native_observed_source_tokens': observed_tokens,
            'native_readonly_connection_calls': len(connections),
            'fixture_snapshot_id': initial_status['snapshot_id'],
            'fixture_release_count': initial_status['release_count'],
            'full_capture_calls': 0, 'source_writes': 0, 'production_catalog_writes': 0,
        }, durable=True)
        raise ValueError('reuse precondition changed; full source capture is outside this idle replay')

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock.astimezone(tz)

    resource_before = limits()
    start, cpu = time.perf_counter(), time.process_time()
    with ExitStack() as stack:
        stack.enter_context(patch.object(active, 'datetime', Clock))
        stack.enter_context(patch.object(active, 'connect', read_only))
        stack.enter_context(patch.object(active, 'identity_sha256', recorded_identity))
        stack.enter_context(patch.object(backup_stream, 'capture_catalog', forbidden_capture))
        result = active.register(trial_config)
        if (result.get('reuse_scope') != 'unchanged source metadata; hourly full inventory'
                or connections != [True] or observed_tokens != [token]):
            raise ValueError('original complete idle-register workflow was not exercised')
        published = json.loads((root / 'catalog-status.json').read_bytes())
        if published != result:
            raise ValueError('complete private status publication differs')
        atomic_write_json(root / 'durable-readback.json', published, durable=True)
        readback = json.loads((root / 'durable-readback.json').read_bytes())
        if readback != result:
            raise ValueError('complete durable result differs')
    elapsed, spent = time.perf_counter() - start, time.process_time() - cpu
    if limits() != resource_before:
        raise ValueError('resource limits changed during measured workflow')
    # Preserve the already observed tuples outside the timed publication
    # quantum. This diagnoses a changed head/inode/clock without another scan.
    atomic_write_json(root / 'native-source-signature-input.json', observed_inputs, durable=True)
    expected = {key: value for key, value in result.items() if key != 'complete_workflow_seconds'}
    return {'wall_seconds': elapsed, 'cpu_seconds': spent, 'semantic_output_sha256': identity_sha256(expected),
            'complete_output': result, 'resource_limits': resource_before,
            'readonly_connections': len(connections), 'durable_complete_readback': True,
            'native_complete_source_signature_sha256': observed_tokens[0], 'native_signature_passes': len(observed_tokens),
            'full_capture_calls': 0, 'source_writes': 0, 'production_catalog_writes': 0}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-root', required=True, type=Path)
    parser.add_argument('--reference', type=Path, help='explicitly preserved local original code, not received peer code')
    parser.add_argument('--reference-sha256', help='mandatory exact SHA256 for a preserved reference')
    args = parser.parse_args(argv)
    output = args.output_root.absolute()
    if output.exists() or not output.resolve().is_relative_to(Path('artifacts/benchmarks').resolve()):
        parser.error('fresh private root under artifacts/benchmarks required')
    # This is HDD metadata I/O, not a reason to compete with opening preparation.
    from downloader.finmind_scheduling import protected_stock_opening
    if protected_stock_opening(datetime.now(UTC)):
        raise ValueError('defer engineering metadata benchmark until opening protection ends')
    if bool(args.reference) != bool(args.reference_sha256):
        parser.error('a preserved reference needs its exact SHA256, and vice versa')
    c = lakehouse.configuration()
    lakehouse.guard(c)
    output.mkdir(mode=0o700, parents=True)
    original_path = Path('stockagent/control/lakehouse.py')
    reference_path = args.reference or original_path
    original = reference_path.read_text()
    if args.reference and hashlib.sha256(original.encode()).hexdigest() != args.reference_sha256:
        raise ValueError('preserved original code SHA256 differs')
    code_paths = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                  for path in (reference_path, original_path, Path(__file__))}
    candidate = candidate_source(original)
    atomic_write_text(output / 'original_lakehouse.py', original, mode=0o600, durable=True)
    atomic_write_text(output / 'candidate_lakehouse.py', candidate, mode=0o600, durable=True)
    variants = {label: module(source, label) for label, source in (('baseline', original), ('candidate', candidate))}
    initial_status = json.loads((Path(c['state_root']) / 'catalog-status.json').read_bytes())
    if initial_status.get('unavailable_release_count') != 0:
        raise ValueError('do not manufacture reuse eligibility for unavailable releases')
    con = lakehouse.connect(c, read_only=True)
    try:
        snapshot = con.execute("SELECT max(snapshot_id) FROM ducklake_snapshots('lake')").fetchone()[0]
        rows = con.execute('SELECT count(*) FROM lake.dataset_releases').fetchone()[0]
    finally:
        con.close()
    if (snapshot, rows) != (initial_status['snapshot_id'], initial_status['release_count']):
        raise ValueError('native status and independent readonly catalog disagree')
    # Reject a publication still between SQL commit and status publication
    # before issuing a complete HDD metadata inventory, not after it.
    paths = metadata_paths(c)
    include_orphans = c.get('include_unreferenced_cold_objects', False)
    if type(include_orphans) is not bool:
        raise ValueError('native source scope is not explicit')
    source_observations = []
    token = one_stat_signature(paths, include_orphans, source_observations)
    atomic_write_json(output / 'fixture-source-signature-input.json', source_observations, durable=True)
    clock = datetime.now(UTC)
    atomic_write_json(output / 'fixture.json', {
        'origin': 'explicit fresh reuse-gate replay; not proof current source matches committed catalog',
        'metadata_paths': [str(path) for path in paths], 'metadata_paths_count': len(paths),
        'source_signature_sha256': token, 'snapshot_id': snapshot, 'release_count': rows,
        'clock': clock.isoformat(), 'include_orphans': include_orphans,
        'reference_code_path': str(reference_path), 'code_sha256': code_paths,
        'original_sha256': hashlib.sha256(original.encode()).hexdigest(),
        'candidate_sha256': hashlib.sha256(candidate.encode()).hexdigest()}, durable=True)
    warmups = []
    for label, active in variants.items():
        warmup = run_trial(active, c, output / ('warmup-' + label), initial_status, token, include_orphans, clock)
        warmups.append({'label': label, **warmup})
    atomic_write_json(output / 'warmups.json', warmups, durable=True)
    trials = []
    for number, label in enumerate(('baseline', 'candidate', 'candidate', 'baseline')):
        trial = run_trial(variants[label], c, output / f'trial-{number}-{label}', initial_status, token, include_orphans, clock)
        trial.update(label=label)
        trials.append(trial)
        atomic_write_json(output / f'trial-{number}.json', trial, durable=True)
        print(json.dumps({key: trial[key] for key in ('label', 'wall_seconds', 'cpu_seconds')}), flush=True)
    # Every warmup/trial already performed and verified the native full path
    # enumeration/stat/hash. A second external scan between trials adds HDD
    # work without a new acceptance boundary: the next native pass checks it.
    # Close the final pass's boundary independently, retaining membership and
    # every inode/size/mtime/ctime field, without freezing production writers.
    if metadata_paths(c) != paths or one_stat_signature(paths, include_orphans) != token:
        raise ValueError('complete live metadata input set changed; do not accept comparison')
    equal = len({trial['semantic_output_sha256'] for trial in trials}) == 1
    stable = (all(hashlib.sha256(Path(path).read_bytes()).hexdigest() == sha for path, sha in code_paths.items())
              and all(trial['resource_limits'] == trials[0]['resource_limits'] for trial in trials))
    means = {label: {key: statistics.mean(trial[key] for trial in trials if trial['label'] == label)
                     for key in ('wall_seconds', 'cpu_seconds')} for label in variants}
    improved = all(means['candidate'][key] < means['baseline'][key] for key in ('wall_seconds', 'cpu_seconds'))
    result = {'schema_version': 1, 'state': 'measured_bounded_idle_register_replay',
              'observed_at_utc': datetime.now(UTC).isoformat(), 'exact_semantic_output_parity': equal,
              'source_code_and_resources_stable': stable, 'whole_idle_replay_improved': improved,
              'metadata_paths': len(paths), 'means': means, 'trials': trials,
              'warmup_and_trial_native_signatures_verified': all(
                  row['native_complete_source_signature_sha256'] == token and row['native_signature_passes'] == 1
                  for row in [*warmups, *trials]),
              'independent_final_full_source_signature_verified': True,
              'code_sha256': code_paths,
              'fixture_origin': 'fresh reuse eligibility is an explicit private engineering fixture',
              'caveats': ['Not a full capture/register/backup throughput benchmark.',
                          'Native source-to-catalog equivalence, loaded code and full recapture need independent acceptance.',
                          'Only the original stopwatch differs; observed clock is replayed without excluding fields.'],
              'provider_calls': 0, 'production_catalog_writes': 0, 'source_writes': 0, 'service_controls': 0}
    atomic_write_json(output / 'measurement.json', result, durable=True)
    if not equal or not stable:
        raise ValueError('complete idle replay parity or stability not established')
    print(json.dumps({key: result[key] for key in ('state', 'whole_idle_replay_improved', 'metadata_paths', 'means')}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
