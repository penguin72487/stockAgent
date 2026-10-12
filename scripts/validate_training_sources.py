"""Named source gates used by the existing canonical release publisher."""
from __future__ import annotations

import argparse
import hashlib
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


def validate_catalog_source(entry, *, repo_root=ROOT):
    gate = entry['validation_gate']
    root = _root(entry, repo_root)
    before = _identity(entry, repo_root)
    state = repo_root / 'artifacts/operations/source_validation' / entry['dataset']
    state.mkdir(parents=True, exist_ok=True)
    dependencies = [Path(__file__), Path(sys.modules[audit_stock_minute_source.__module__].__file__),
                    Path(sys.modules[validate_stock_minute_catalog.__module__].__file__)]
    if gate['kind'] == 'stock_minute_source':
        from downloader import download_shioaji_tw_minute_kbars as collector
        dependencies.append(Path(collector.__file__))
    else:
        from scripts import audit_shioaji_tw_minute_dataset as auditor
        from scripts import build_shioaji_tw_minute_dataset as builder
        from stockagent.data import tw_price_rules, tw_exchange_price_classification
        from downloader import stock_volume_units
        from downloader import download_tw_public_data
        dependencies.extend(Path(module.__file__) for module in (
            auditor, builder, tw_price_rules, tw_exchange_price_classification, stock_volume_units,
            download_tw_public_data))
    validator_sha = validator_identity(*dependencies)
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
    if gate['kind'] == 'stock_minute_source':
        result = audit_stock_minute_source(root, state / 'files.sqlite3')
        result['terminal_catalog'] = validate_stock_minute_catalog(root, state/'catalog.sqlite3')
    elif gate['kind'] == 'stock_minute_dataset':
        raw = Path(gate['calendar_root'])
        calendar = raw if raw.is_absolute() else repo_root / raw
        audit_path = state / 'semantic_audit.json'
        try:
            subprocess.run([sys.executable, str(repo_root / 'scripts/audit_shioaji_tw_minute_dataset.py'),
                '--dataset-root', str(root), '--all-partitions', '--calendar-root', str(calendar),
                '--verification-cache', str(state / 'files.sqlite3'), '--output', str(audit_path)],
                check=True, cwd=repo_root)
        except subprocess.CalledProcessError as exc:
            raise SnapshotError(f'source semantic validation failed: {entry["dataset"]}') from exc
        result = verify_minute_release_receipt(root, audit_path)
    else:
        raise SnapshotError(f"unknown source validation gate: {gate['kind']}")
    proof = {'contract': 'catalog_source_semantic_validation_v1', 'identity': before,
             'validator_sha256': validator_sha, 'gate': gate,
             'dataset': entry['dataset'], 'result': result}
    verify_unchanged(entry, proof, repo_root=repo_root)
    atomic_write_json(state / 'latest.json', proof)
    return proof


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('dataset')
    parser.add_argument('--catalog', type=Path, default=ROOT/'configs/data_sync/packed_datasets.json')
    args = parser.parse_args()
    entries = json.loads(args.catalog.read_text())['datasets']
    entry = next(x for x in entries if x['dataset'] == args.dataset)
    result = validate_catalog_source(entry)
    print(json.dumps({'dataset': args.dataset, 'identity': result['identity'],
                      'status': result['result']['status']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
