#!/usr/bin/env python3
"""Compare the final official archives and finish a bounded cross-source audit."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import time
import zipfile

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.build_tw_option_cross_source_references import snapshot_receipt
from scripts.run_tw_option_cross_source_recent import run as run_cross_source
from stockagent.data.tw_futures_official_ticks import (pin_official_option_tick_inventory,
                                                       pin_official_tick_inventory)


def csv_member_hash(path: Path, expected_member: str) -> dict:
    digest = hashlib.sha256()
    with zipfile.ZipFile(path) as archive:
        if archive.namelist() != [expected_member]:
            raise ValueError('archive member differs from the official contract')
        info = archive.getinfo(expected_member)
        with archive.open(info) as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(chunk)
    return {'csv_sha256': digest.hexdigest(), 'csv_bytes': info.file_size}


def compare(preliminary: Path, official_head: Path, output: Path) -> dict:
    preliminary, official_head, output = (p.resolve() for p in (preliminary, official_head, output))
    if output.exists() or output.is_symlink():
        raise FileExistsError('preserve previous source comparison')
    preliminary_sha = sha256_file(preliminary)
    doc = json.loads(preliminary.read_bytes())
    if (doc.get('contract') != 'taifex_precompletion_source_snapshot_v1'
            or doc.get('status') != 'observed_provisional' or doc.get('complete_trading_day') is not False
            or doc.get('training_ready') is not False):
        raise ValueError('a pinned provisional source snapshot is required')
    day = date.fromisoformat(doc['trading_date'])
    head_sha = sha256_file(official_head)
    frozen = Path(snapshot_receipt({'receipt_path': str(official_head), 'receipt_sha256': head_sha},
                                   official_head.parent)['receipt_path'])
    results = {}
    for kind, pin in (('futures', pin_official_tick_inventory), ('options', pin_official_option_tick_inventory)):
        final = pin(frozen, [day])[0]
        early = doc['raw_downloads'][kind]
        early_path, final_path = Path(early['path']), Path(final['path'])
        if (early_path.is_symlink() or final_path.is_symlink() or sha256_file(early_path) != early['sha256']
                or sha256_file(final_path) != final['sha256'] or final_path.stat().st_size != final['bytes']):
            raise ValueError('provisional or final archive hash differs')
        prefix = 'Daily' if kind == 'futures' else 'OptionsDaily'
        member = f"{prefix}_{day.strftime('%Y_%m_%d')}.csv"
        first, last = csv_member_hash(early_path, member), csv_member_hash(final_path, member)
        results[kind] = {'provisional_archive_sha256': early['sha256'],
                         'completed_archive_sha256': final['sha256'],
                         'provisional_csv': first, 'completed_csv': last,
                         'archive_bytes_identical': early['sha256'] == final['sha256'],
                         'csv_contents_identical': first == last}
        if sha256_file(early_path) != early['sha256'] or sha256_file(final_path) != final['sha256']:
            raise ValueError('source archives changed during comparison')
    if sha256_file(preliminary) != preliminary_sha or sha256_file(frozen) != head_sha:
        raise ValueError('source receipts changed during comparison')
    result = {'contract': 'taifex_precompletion_vs_completed_source_v1',
              'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'trading_date': day.isoformat(),
              'preliminary_manifest': str(preliminary), 'preliminary_manifest_sha256': preliminary_sha,
              'completed_manifest_snapshot': str(frozen), 'completed_manifest_sha256': head_sha,
              'sources': results, 'implementation_sha256': sha256_file(Path(__file__)), 'training_ready': False,
              'limitations': ['Identical CSV content is not proof of every market trade or execution.',
                              'Source changes require separate contract and clock reconciliation.']}
    atomic_write_json(output, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('parent-run', 'preliminary', 'official', 'plan', 'compare-output', 'data-root', 'report-root'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--max-wait-seconds', type=int, default=86400)
    args = parser.parse_args()
    if not 1 <= args.max_wait_seconds <= 7 * 86400:
        raise ValueError('a finite wait of at most seven days is required')
    pins = {str(p.resolve()): sha256_file(p) for p in (
        Path(__file__), Path('scripts/run_tw_option_cross_source_recent.py'), args.preliminary, args.plan)}
    deadline = time.monotonic() + args.max_wait_seconds
    print(json.dumps({'state': 'waiting_for_canonical_completed_daily_sources',
                      'provider_calls_by_waiter': 0}), flush=True)
    while time.monotonic() < deadline:
        state = json.loads(args.parent_run.read_bytes()).get('state')
        if state == 'succeeded':
            if any(sha256_file(Path(p)) != h for p, h in pins.items()):
                raise ValueError('captured inputs or implementation changed during wait')
            result = compare(args.preliminary, args.official, args.compare_output)
            print(json.dumps({'state': 'provisional_and_completed_sources_compared',
                              'output': str(args.compare_output), 'sources': result['sources']},
                             ensure_ascii=False), flush=True)
            summary = run_cross_source(Path('data_finmind/complement'), args.plan, args.official,
                                       args.data_root, args.report_root, args.report_root / 'summary.json',
                                       ['2026-10-28'])
            if len(summary['results']) != 1 or summary['results'][0]['state'] not in {'built_verified', 'reused_verified'}:
                raise ValueError('the requested cross-source day did not pass its independent readback')
            print(json.dumps({'state': 'cross_source_day_verified', 'summary': str(args.report_root / 'summary.json'),
                              'training_ready': False}, ensure_ascii=False), flush=True)
            return
        if state not in {'queued', 'running'}:
            raise ValueError('canonical source workflow ended without success: ' + str(state))
        time.sleep(min(30, max(0, deadline - time.monotonic())))
    raise TimeoutError('completed official source was not ready before the finite wait expired')


if __name__ == '__main__':
    main()
