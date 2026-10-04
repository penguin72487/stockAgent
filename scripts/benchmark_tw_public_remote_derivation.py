#!/usr/bin/env python3
"""Measure full canonical rebuilds on this actual remote node, then apply a winner.

Every trial retains source checks, summary, output and parity evidence. The
search is sequential; it does not reserve a GPU, change a builder, drop caches,
publish a view or delete data. Historical timings only seed new measurements.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from stockagent.remote_build import candidate_threads, choose_measured, digest, local_affinities, observe_node
from stockagent.runtime_identity import runtime_identity, verify_source_release


def sha(path: Path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def stop_owned(process):
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()


def benchmark(args):
    if socket.gethostname() == 'penguin':
        raise ValueError('full feature build measurements belong on the remote node')
    code, source = args.code_root.resolve(strict=True), args.source_root.resolve(strict=True)
    output = args.output.resolve()
    if output.exists() or output.is_relative_to(code) or output.is_relative_to(source):
        raise ValueError('benchmark needs a new output outside immutable code/source')
    profile = observe_node(source, output.parent)
    if profile['limits']['memory_headroom_bytes'] < args.memory_budget_gib * 1024**3:
        raise ValueError('declared benchmark RAM exceeds current cgroup/host headroom')
    threads = candidate_threads(profile) if args.threads is None else args.threads
    if not threads or len(set(threads)) != len(threads) or any(
        type(value) is not int or not 1 <= value <= profile['limits']['cpu_thread_trial_cap'] for value in threads
    ):
        raise ValueError('candidate threads exceed actual affinity/cgroup budget')
    if shutil.disk_usage(output.parent).free < (len(threads) + 12) * 2 * 1024**3:
        raise ValueError('insufficient private scratch for retained full-build trials')
    oracle = json.loads(args.reference.read_bytes())
    if oracle['state'] != 'accepted' or oracle['snapshot_id'] != args.snapshot_id or oracle['end_date'] != args.end_date:
        raise ValueError('parity reference does not match the requested full workload')
    expected = oracle['builds'][0]
    code_proof = verify_source_release(args.code_receipt, code)
    native = runtime_identity()
    if native['sha256'] != oracle['runtime']['sha256']:
        raise ValueError('benchmark/reference native runtimes differ')
    context = {'code_receipt_sha256': sha(args.code_receipt), 'source_sha256': code_proof['source_sha256'],
               'runtime_sha256': native['sha256'], 'snapshot_id': args.snapshot_id,
               'manifest_sha256': oracle['manifest_sha256'], 'end_date': args.end_date,
               'builder': 'canonical_tw_public_full'}
    output.mkdir(mode=0o700)
    atomic_write_json(output / 'node-before.json', profile)
    started = time.perf_counter()
    runs = []
    base = {'affinity': None, 'memory_budget_bytes': args.memory_budget_gib * 1024**3}
    driver = code / 'scripts/verify_tw_public_remote_derivation.py'

    def run_one(parameters, phase, selection=None):
        before = observe_node(source, output)
        if before['machine_sha256'] != profile['machine_sha256']:
            raise ValueError('node conditions changed; new measurements are required')
        name = f'{len(runs)+1:03d}-{phase}-{digest(parameters)[:12]}'
        directory = output / name
        log = output / (name + '.log')
        argv = [sys.executable, str(driver), '--code-root', str(code), '--code-receipt', str(args.code_receipt),
                '--packed-root', str(args.packed_root), '--source-root', str(source),
                '--snapshot-id', args.snapshot_id, '--end-date', args.end_date, '--output', str(directory),
                '--build-count', '1', '--memory-budget-gib', str(args.memory_budget_gib),
                '--polars-threads', str(parameters['polars_threads']), '--arrow-threads', str(parameters['arrow_threads'])]
        if selection is not None:
            argv += ['--tuning-receipt', str(selection)]
        elif parameters['affinity'] is not None:
            taskset = shutil.which('taskset')
            if taskset is None:
                raise ValueError('taskset is required for NUMA affinity trials')
            argv = [taskset, '--cpu-list', ','.join(map(str, parameters['affinity'])), *argv]
        trial_started = time.perf_counter()
        with log.open('xb') as stream:
            process = subprocess.Popen(argv, stdout=stream, stderr=subprocess.STDOUT, cwd=code, start_new_session=True)
            try:
                exit_code = process.wait(timeout=args.trial_timeout_seconds)
            except subprocess.TimeoutExpired:
                stop_owned(process)
                exit_code = 124
            finally:
                stop_owned(process)
        elapsed = time.perf_counter() - trial_started
        record = {'index': len(runs)+1, 'phase': phase, 'parameters': parameters,
                  'state': 'rejected', 'exit_code': exit_code, 'complete_wall_seconds': elapsed,
                  'log': str(log), 'log_sha256': sha(log), 'node_before': before,
                  'exact_output_parity': False, 'resource_budget_met': False}
        receipt_path = directory / 'acceptance.json'
        if exit_code == 0 and receipt_path.is_file():
            receipt = json.loads(receipt_path.read_bytes())
            actual = receipt['builds'][0]
            fields = ('rows', 'stock_rows', 'market_rows', 'features', 'schema_sha256',
                      'source_receipts', 'symbol_universe_receipt')
            parity = all(actual[key] == expected[key] for key in fields) and (
                actual['output_receipt']['sha256'] == expected['output_receipt']['sha256'])
            if receipt['measurement_context'] != context or not receipt['source_and_code_unchanged']:
                raise ValueError('candidate code/source/runtime changed')
            eligible = receipt['peak_memory_budget_met'] and receipt['memory_budget_bytes'] == parameters['memory_budget_bytes']
            pools = receipt['effective_polars_threads'] == parameters['polars_threads'] and receipt['effective_arrow_threads'] == parameters['arrow_threads']
            affinity = receipt['node_profile_after']['identity']['affinity']
            eligible = eligible and pools and (parameters['affinity'] is None or affinity == parameters['affinity'])
            record.update(state='accepted' if parity and eligible else 'rejected',
                          exact_output_parity=parity, resource_budget_met=eligible,
                          peak_rss_bytes=receipt['process_peak_rss_bytes'],
                          process_cpu_seconds=receipt['process_cpu_seconds'],
                          canonical_verifier_wall_seconds=receipt['wall_seconds'],
                          receipt=str(receipt_path), receipt_sha256=sha(receipt_path),
                          output_sha256=actual['output_receipt']['sha256'],
                          output_rows=actual['rows'], stages=actual['stage_elapsed_seconds'])
        runs.append(record)
        atomic_write_json(output / 'progress.json', {'state': 'running', 'runs': runs})
        print(json.dumps({key: record[key] for key in
                          ('index', 'phase', 'parameters', 'state', 'complete_wall_seconds', 'exit_code')}), flush=True)
        return record

    # Start near the historical seed, then cover every hardware-admitted pool.
    for value in sorted(threads, key=lambda value: (abs(value - 8), value)):
        run_one({**base, 'polars_threads': value, 'arrow_threads': value}, 'pool-screen')
    selected = choose_measured(runs)['selected']['parameters']
    arrow_candidates = {max(1, selected['arrow_threads']//2), 8, profile['limits']['cpu_worker_budget']}
    for value in sorted(arrow_candidates):
        parameters = {**selected, 'arrow_threads': value}
        if value <= profile['limits']['cpu_worker_budget'] and not any(run['parameters'] == parameters for run in runs):
            run_one(parameters, 'arrow-screen')
    selected = choose_measured(runs)['selected']['parameters']
    for node in local_affinities(profile):
        if len(node['cpus']) >= max(selected['polars_threads'], selected['arrow_threads']):
            run_one({**selected, 'affinity': node['cpus']}, 'numa-screen')
    finalists = [row['parameters'] for row in choose_measured(runs)['ranked'][:2]]
    # Screening sample plus two balanced repeat samples for each finalist.
    for parameters in finalists + list(reversed(finalists)):
        run_one(parameters, 'finalist-repeat')
    ranking = choose_measured(runs, minimum_samples=3)
    selection = {'schema_version': 1, 'state': 'accepted', 'observed_at_utc': datetime.now(timezone.utc).isoformat(),
                 'machine_sha256': profile['machine_sha256'], 'context': context, **ranking,
                 'expected_output_sha256': expected['output_receipt']['sha256'],
                 'reference_receipt_sha256': sha(args.reference),
                 'selected_measurements': [{key: run[key] for key in
                     ('receipt', 'receipt_sha256', 'complete_wall_seconds')}
                     for run in runs if run['state'] == 'accepted' and run['parameters'] == ranking['selected']['parameters']],
                 'profile': profile, 'scope': 'This exact node/code/source/runtime and full workload; warmed source reads, fastest repeated mean, no global default'}
    candidate = output / 'selection-candidate.json'
    atomic_write_json(candidate, selection)
    confirmation = run_one(ranking['selected']['parameters'], 'selected-application', candidate)
    if confirmation['state'] != 'accepted':
        raise ValueError('selected settings failed their actual application')
    if verify_source_release(args.code_receipt, code) != code_proof or runtime_identity() != native:
        raise ValueError('benchmark code/native runtime changed')
    selection['confirmation'] = {key: confirmation[key] for key in ('receipt', 'receipt_sha256', 'complete_wall_seconds')}
    atomic_write_json(output / 'selection.json', selection)
    result = {'schema_version': 1, 'state': 'accepted', 'runs': runs,
              'selection': str(output / 'selection.json'), 'selection_sha256': sha(output / 'selection.json'),
              'context': context, 'machine_sha256': profile['machine_sha256'], 'node_before': profile,
              'node_after': observe_node(source, output), 'total_wall_seconds': time.perf_counter()-started,
              'selected': ranking['selected'], 'all_accepted_outputs_match_exact_reference': True,
              'rejected_candidates_retained': sum(run['state'] != 'accepted' for run in runs),
              'claim': ranking['claim'], 'gpu_or_financial_algorithm_changed': False,
              'source_or_previous_artifacts_deleted': False, 'production_promoted': False}
    atomic_write_json(output / 'acceptance.json', result)
    print(json.dumps({key: result[key] for key in ('state', 'selected', 'total_wall_seconds')}), flush=True)
    return result


def main():
    def interrupted(signum, _frame):
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('code-root', 'code-receipt', 'packed-root', 'source-root', 'reference', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--snapshot-id', required=True)
    parser.add_argument('--end-date', required=True)
    parser.add_argument('--memory-budget-gib', type=int, choices=(64, 128, 256), default=64)
    parser.add_argument('--threads', type=int, nargs='+', help='Explicit seed candidates; all require actual node admission')
    parser.add_argument('--trial-timeout-seconds', type=int, default=600)
    args = parser.parse_args()
    if args.trial_timeout_seconds < 1:
        parser.error('trial timeout must be positive')
    benchmark(args)


if __name__ == '__main__':
    main()
