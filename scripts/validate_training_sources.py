"""Named source gates used by the existing canonical release publisher."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.data_sync.desync_snapshots import atomic_write_json, sha256_file, SnapshotError
from stockagent.data_sync.source_validation import audit_stock_minute_source, verify_minute_release_receipt
from stockagent.data_sync.source_validation import signature
from stockagent.data_sync.source_validation import validator_identity, read_object
from stockagent.data_sync.minute_catalog_validation import validate_stock_minute_catalog


def tree_signature(root):
    """Catch source bytes/receipts changing after validation, even with old mtimes.

    This is a local native-stat guard, not the content/SHA transport proof. The
    publisher still validates its complete inventory before committing a head.
    """
    digest = hashlib.sha256()
    selected = root.resolve(strict=True)
    for directory, dirs, names in os.walk(root, followlinks=False):
        dirs.sort()
        for name in dirs:
            if (Path(directory)/name).is_symlink():
                raise SnapshotError('validation cannot skip a symlinked source directory')
        for name in sorted(names):
            path = Path(directory)/name
            # os.walk never followed a directory symlink. Resolve only actual
            # file aliases, not every ancestor of hundreds of thousands of
            # ordinary files; native dev/ino/ctime still guard atomic changes.
            if path.is_symlink() and not path.resolve(strict=True).is_relative_to(selected):
                raise SnapshotError(f'validation member escapes source: {path}')
            digest.update(json.dumps([str(path.relative_to(root)), signature(path)],
                                      separators=(',', ':')).encode())
            digest.update(b'\n')
    return digest.hexdigest()


def _root(entry, repo_root):
    raw = Path(entry['source'])
    return raw if raw.is_absolute() else repo_root / raw


def _identity(entry, repo_root):
    root = _root(entry, repo_root)
    kind = entry['validation_gate']['kind']
    if kind == 'stock_minute_source':
        from scripts.shioaji_minute_backfill_state import source_fingerprint
        data = {'summary': sha256_file(root / 'download_summary.json'),
                'report': sha256_file(root / 'download_report.csv'),
                'members': source_fingerprint(root)}
    elif kind == 'stock_minute_dataset':
        data = {'manifest': sha256_file(root / 'manifest.json')}
        raw = Path(entry['validation_gate']['calendar_root'])
        calendar = raw if raw.is_absolute() else repo_root/raw
        data['official_calendar'] = {name: sha256_file(calendar/name) for name in (
            'twse_taiex_ohlc.parquet', 'twse_taiex_ohlc.summary.json')}
    else:
        raise SnapshotError(f'unknown source validation gate: {kind}')
    data['native_tree_signature'] = tree_signature(root)
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()


def verify_unchanged(entry, proof, *, repo_root=ROOT):
    if _identity(entry, repo_root) != proof['identity']:
        raise SnapshotError('validated source identity changed before publication')


def _validator_dependencies(kind):
    """Hash implementations without importing their numerical dependencies."""
    names = ['scripts/validate_training_sources.py', 'scripts/runtime_env.sh',
             'stockagent/data_sync/source_validation.py',
             'stockagent/data_sync/minute_catalog_validation.py']
    if kind == 'stock_minute_source':
        names.extend(['downloader/download_shioaji_tw_minute_kbars.py',
                      'scripts/shioaji_minute_backfill_state.py',
                      'downloader/stock_volume_units.py'])
    elif kind == 'stock_minute_dataset':
        names.extend(['scripts/audit_shioaji_tw_minute_dataset.py',
            'scripts/build_shioaji_tw_minute_dataset.py',
            'stockagent/data/tw_price_rules.py',
            'stockagent/data/tw_exchange_price_classification.py',
            'downloader/stock_volume_units.py', 'downloader/download_tw_public_data.py'])
    else:
        raise SnapshotError(f'unknown source validation gate: {kind}')
    return [ROOT / name for name in names]


def _semantic_audit(entry, repo_root, state):
    """Run the original auditors, never a reduced storage-role substitute."""
    root, gate = _root(entry, repo_root), entry['validation_gate']
    if gate['kind'] == 'stock_minute_source':
        result = audit_stock_minute_source(root, state / 'files.sqlite3')
        result['terminal_catalog'] = validate_stock_minute_catalog(root, state / 'catalog.sqlite3')
        return result
    if gate['kind'] != 'stock_minute_dataset':
        raise SnapshotError(f"unknown source validation gate: {gate['kind']}")
    raw = Path(gate['calendar_root'])
    calendar = raw if raw.is_absolute() else repo_root / raw
    audit_path = state / 'semantic_audit.json'
    try:
        subprocess.run([sys.executable, str(ROOT / 'scripts/audit_shioaji_tw_minute_dataset.py'),
            '--dataset-root', str(root), '--all-partitions', '--calendar-root', str(calendar),
            '--verification-cache', str(state / 'files.sqlite3'), '--output', str(audit_path)],
            check=True, cwd=repo_root, stdout=sys.stderr)
    except subprocess.CalledProcessError as exc:
        raise SnapshotError(f'source semantic validation failed: {entry["dataset"]}') from exc
    return verify_minute_release_receipt(root, audit_path)


def _audit_with_runtime(entry, repo_root, state):
    if all(importlib.util.find_spec(name) is not None for name in ('numpy', 'polars')):
        return _semantic_audit(entry, repo_root, state)
    # The locked storage role deliberately excludes acquisition/ML packages.
    # Only its fixed semantic auditor uses the existing native data runtime;
    # the storage process keeps its own runtime, source lock and before/after
    # identity guards. No dependency installation, provider call or training.
    environment = dict(os.environ)
    for name in ('PYTHON_BIN', 'FINTECH_ENV_PATH'):
        environment.pop(name, None)
    command = ['bash', '-c', 'source "$1/scripts/runtime_env.sh"; '
        'run_fintech_python "$1/scripts/validate_training_sources.py" '
        '--audit-entry-stdin --repo-root "$2"', 'source-semantic-audit', str(ROOT), str(repo_root)]
    try:
        completed = subprocess.run(command, input=json.dumps(entry), text=True,
            stdout=subprocess.PIPE, check=True, cwd=repo_root, env=environment)
    except subprocess.CalledProcessError as exc:
        raise SnapshotError(f'canonical data-runtime semantic audit failed: {entry["dataset"]}; '
                            f'exit={exc.returncode}; source remains unaccepted') from exc
    # Audit progress goes to stderr in the fixed child; stdout is one result.
    value = json.loads(completed.stdout)
    if (not isinstance(value, dict) or value.get('contract') != 'source_semantic_audit_result_v1'
            or value.get('dataset') != entry['dataset'] or not isinstance(value.get('result'), dict)):
        raise SnapshotError('semantic auditor returned an unrelated or incomplete result')
    return value['result']


def validate_catalog_source(entry, *, repo_root=ROOT):
    gate = entry['validation_gate']
    before = _identity(entry, repo_root)
    state = repo_root / 'artifacts/operations/source_validation' / entry['dataset']
    state.mkdir(parents=True, exist_ok=True)
    validator_sha = validator_identity(*_validator_dependencies(gate['kind']))
    latest = state/'latest.json'
    if latest.is_file():
        previous = read_object(latest)
        if (previous.get('identity') == before and previous.get('validator_sha256') == validator_sha
                and previous.get('gate') == gate):
            # Exact native tree identity and unchanged validator contracts make
            # the previously full semantic proof reusable. Not an mtime-only
            # shortcut; publisher still checks cold content and its inventory.
            verify_unchanged(entry, previous, repo_root=repo_root)
            return previous
    result = _audit_with_runtime(entry, repo_root, state)
    proof = {'contract': 'catalog_source_semantic_validation_v1', 'identity': before,
             'validator_sha256': validator_sha, 'gate': gate,
             'dataset': entry['dataset'], 'result': result}
    verify_unchanged(entry, proof, repo_root=repo_root)
    atomic_write_json(state / 'latest.json', proof)
    return proof


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('dataset', nargs='?')
    parser.add_argument('--audit-entry-stdin', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--repo-root', type=Path, default=ROOT, help=argparse.SUPPRESS)
    parser.add_argument('--catalog', type=Path, default=ROOT/'configs/data_sync/packed_datasets.json')
    args = parser.parse_args()
    if args.audit_entry_stdin:
        from contextlib import redirect_stdout
        entry = json.load(sys.stdin)
        state = args.repo_root / 'artifacts/operations/source_validation' / entry['dataset']
        state.mkdir(parents=True, exist_ok=True)
        with redirect_stdout(sys.stderr):
            result = _semantic_audit(entry, args.repo_root, state)
        print(json.dumps({'contract': 'source_semantic_audit_result_v1',
                          'dataset': entry['dataset'], 'result': result}))
        return
    if not args.dataset:
        parser.error('dataset is required')
    entries = json.loads(args.catalog.read_text())['datasets']
    entry = next(x for x in entries if x['dataset'] == args.dataset)
    result = validate_catalog_source(entry)
    print(json.dumps({'dataset': args.dataset, 'identity': result['identity'],
                      'status': result['result']['status']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
