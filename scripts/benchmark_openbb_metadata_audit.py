"""Replay the complete canonical file audit on bounded, copied real files.

The sample is intentionally stratified within 10,000 successful tasks. It is
not representative provider throughput, nor proof that the full archive is
complete. No source manifest, provider, or live audit process is changed.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import sqlite3
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file  # noqa: E402
from scripts import monitor_openbb_archive as monitor  # noqa: E402


def identity(path: Path) -> tuple[int, ...]:
    stamp = path.stat()
    return stamp.st_dev, stamp.st_ino, stamp.st_size, stamp.st_mtime_ns, stamp.st_ctime_ns


def prepare(source: Path, fixture: Path, baseline_revision: str) -> dict:
    if fixture.exists():
        raise ValueError('preserve previous fixture; choose a new path')
    fixture.mkdir(parents=True, mode=0o700)
    if shutil.disk_usage(fixture).free < 30 * 2**30:
        raise ValueError('insufficient private fixture space')
    original = subprocess.run(['git', 'show', f'{baseline_revision}:scripts/monitor_openbb_archive.py'],
                              check=True, capture_output=True, cwd=ROOT).stdout
    (fixture / 'reference.py').write_bytes(original)
    with monitor._open_read_only(source / '_state/openbb_archive.sqlite3') as conn:
        plan = monitor._active_plan_token(conn)
        schema = conn.execute("SELECT sql FROM sqlite_master WHERE name='tasks' AND type='table'").fetchone()[0]
        candidates = [dict(row) for row in conn.execute(
            "SELECT * FROM tasks WHERE active=1 AND plan_token=? AND status='success' LIMIT 10000", (plan,))]
    if not candidates:
        raise ValueError('no successful real samples')
    # Include small files as well as decode-heavy tables; do not average this
    # deliberately heavy sample into a whole-service speedup claim.
    selected = {row['task_id']: row for row in candidates[:32]}
    selected.update({row['task_id']: row for row in sorted(candidates, key=lambda row: row['rows'], reverse=True)[:8]})
    state = fixture / '_state'
    state.mkdir(mode=0o700)
    files, sources, total_bytes = {}, [], 0
    with sqlite3.connect(state / 'openbb_archive.sqlite3') as shadow:
        shadow.execute(schema)
        for row in selected.values():
            path = Path(row['output_path'])
            stamp, digest = identity(path), sha256_file(path)
            total_bytes += stamp[2]
            if total_bytes > 512 * 2**20:
                raise ValueError('sample byte budget exceeded')
            target = fixture / 'data' / f"{row['task_id']}.parquet"
            target.parent.mkdir(mode=0o700, exist_ok=True)
            shutil.copyfile(path, target)
            if sha256_file(target) != digest or identity(path) != stamp:
                raise ValueError('source changed during bounded copy')
            sources.append({'path': str(path), 'identity': stamp, 'sha256': digest,
                            'rows': row['rows'], 'endpoint': row['endpoint']})
            files[str(target.relative_to(fixture))] = digest
            row['output_path'] = str(target.resolve())
            names = list(row)
            shadow.execute(f"INSERT INTO tasks ({','.join(names)}) VALUES ({','.join('?' for _ in names)})",
                           [row[name] for name in names])
    files['reference.py'] = sha256_file(fixture / 'reference.py')
    files['_state/openbb_archive.sqlite3'] = sha256_file(state / 'openbb_archive.sqlite3')
    result = {'schema_version': 1, 'observed_at_utc': datetime.now(UTC).isoformat(), 'plan': plan,
              'candidate_count': len(candidates), 'sample_files': len(selected), 'sample_bytes': total_bytes,
              'baseline_revision': baseline_revision, 'files': files, 'sources': sources}
    atomic_write_json(fixture / 'manifest.json', result)
    return result


def worker(fixture: Path, variant: str) -> dict:
    manifest = json.loads((fixture / 'manifest.json').read_bytes())
    module = monitor
    if variant == 'baseline':
        spec = importlib.util.spec_from_file_location('_openbb_audit_reference', fixture / 'reference.py')
        module = importlib.util.module_from_spec(spec)
        module.__file__ = str(ROOT / 'scripts/monitor_openbb_archive.py')
        spec.loader.exec_module(module)
    connection = module._open_read_only(fixture / '_state/openbb_archive.sqlite3')
    try:
        started, cpu = time.perf_counter(), time.process_time()
        result = module._audit_success_files(connection, output_dir=fixture,
                                            plan_token=manifest['plan'], show_progress=False)
        elapsed, cpu = time.perf_counter() - started, time.process_time() - cpu
    finally:
        connection.close()
    if not result['passed'] or result['checked_files'] != manifest['sample_files']:
        raise ValueError('canonical copied-file audit failed')
    # Only the preexisting stopwatch is nonsemantic. Keep all counts, samples,
    # schema, integrity, provenance, owner-index size and acceptance fields.
    result.pop('ownership_index_build_seconds', None)
    result.pop('ownership_scan_seconds', None)
    encoded = json.dumps(result, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()
    peak = next(int(row.split()[1]) * 1024 for row in Path('/proc/self/status').read_text().splitlines()
                if row.startswith('VmHWM:'))
    return {'variant': variant, 'elapsed_seconds': elapsed, 'cpu_seconds': cpu,
            'peak_rss_bytes': peak, 'output_sha256': hashlib.sha256(encoded).hexdigest(), 'audit': result}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--source', type=Path, default=ROOT / 'data_openBB')
    parser.add_argument('--baseline-revision', default='aacc15783907ccb83093dbd8ae78f0745d927a8d')
    parser.add_argument('--prepare-only', action='store_true')
    parser.add_argument('--worker', choices=('baseline', 'candidate'))
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if args.prepare_only:
        receipt = prepare(args.source, args.fixture, args.baseline_revision)
        print(json.dumps({key: receipt[key] for key in ('sample_files', 'sample_bytes', 'candidate_count')}))
        return
    if args.worker:
        print(json.dumps(worker(args.fixture, args.worker)))
        return
    if args.output is None:
        parser.error('--output required for paired audit')
    manifest = json.loads((args.fixture / 'manifest.json').read_bytes())
    code_sha = sha256_file(ROOT / 'scripts/monitor_openbb_archive.py')
    trials = []
    for variant in ('baseline', 'candidate', 'candidate', 'baseline', 'baseline', 'candidate'):
        completed = subprocess.run([sys.executable, str(Path(__file__).resolve()), '--fixture',
                                    str(args.fixture), '--worker', variant], check=True, capture_output=True,
                                   text=True, cwd=ROOT, timeout=180)
        trial = json.loads(completed.stdout)
        trials.append(trial)
        print(json.dumps({key: value for key, value in trial.items() if key != 'audit'}), flush=True)
    if len({row['output_sha256'] for row in trials}) != 1:
        raise ValueError('complete bounded audit semantic parity failed')
    if any(sha256_file(args.fixture / name) != digest for name, digest in manifest['files'].items()):
        raise ValueError('private fixture changed')
    if any(identity(Path(row['path'])) != tuple(row['identity']) or sha256_file(Path(row['path'])) != row['sha256']
           for row in manifest['sources']):
        raise ValueError('source samples changed')
    if code_sha != sha256_file(ROOT / 'scripts/monitor_openbb_archive.py'):
        raise ValueError('implementation changed during trials')
    medians = {variant: {key: statistics.median(row[key] for row in trials if row['variant'] == variant)
                         for key in ('elapsed_seconds', 'cpu_seconds', 'peak_rss_bytes')}
               for variant in ('baseline', 'candidate')}
    atomic_write_json(args.output, {'state': 'accepted_bounded_audit_replay',
                                   'scope': 'full_canonical_file_audit_on_stratified_copied_sample_not_whole_archive',
                                   'exact_semantic_output_parity': True, 'trials': trials, 'medians': medians,
                                   'source_manifest': str(args.fixture / 'manifest.json'), 'code_sha256': code_sha,
                                   'provider_calls': 0, 'production_writes': 0,
                                   'limits': ['nonexclusive_host_load', 'os_cache_not_flushed', 'deliberately_decode_heavy_sample']})


if __name__ == '__main__':
    main()
