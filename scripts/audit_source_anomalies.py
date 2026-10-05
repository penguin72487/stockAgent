#!/usr/bin/env python3
"""Inventory current sources, audit native grains, emit finite owner worklists.

No provider/network calls, credentials, training builds, source mutation, or TEJ
inspection. Durable checkpoints allow a large scan to resume without repeating
unchanged files. Footer evidence is never presented as full semantic validation.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import UTC, datetime
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_text, sha256_file
from downloader.parquet_integrity import parquet_receipt_error
from downloader.source_anomalies import CONTRACT, file_binding, footer_profile, market_profile
from stockagent.live.data_monitor_inventory import _selected_files


def read(path: Path):
    return json.loads(path.read_text()) if path.is_file() else {}


def csv_file(path: Path, rows: list[dict]):
    fields = list(dict.fromkeys(key for row in rows for key in row))
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    writer.writerows({key: json.dumps(value, ensure_ascii=False) if isinstance(value, (list, dict)) else value
                     for key, value in row.items()} for row in rows)
    atomic_write_text(path, output.getvalue())


def excluded(value: str) -> bool:
    return "tej" in value.lower()


def discover(root: Path) -> tuple[dict[Path, dict], list[dict]]:
    """Use canonical physical families, current licensed heads, extra source roots.

    Archives, retries and derived tables remain visible but are not dispatched
    as duplicate acquisitions. FinMind current task heads are audited by its
    existing receipt sweeper; they are not expanded into another million-file
    recursive directory traversal here.
    """
    grouped = _selected_files(root)
    entries: dict[Path, dict] = {}
    families = []
    for family, files in grouped.items():
        if excluded(family):
            continue
        if family == "physical:finlab:downloaded-datasets":
            families.append({"family": family, "files": len(files),
                             "audit_scope": "historical_objects_retained; current_receipt_heads_below"})
            continue
        families.append({"family": family, "files": len(files), "audit_scope": "registered_physical_family"})
        for path in files:
            if excluded(str(path.relative_to(root))):
                continue
            entries.setdefault(path, {"families": [], "owner": "source_specific_review"})["families"].append(family)
    finlab = root / "data_finlab"
    for receipt_path in sorted((finlab / "receipts").glob("*.json")):
        try:
            receipt = read(receipt_path)
        except (OSError, ValueError):
            entries[receipt_path] = {"families": ["finlab:current-heads"], "owner": "FinLab",
                                    "discovery_error": "unreadable_receipt"}
            continue
        if not isinstance(receipt, dict):
            entries[receipt_path] = {"families": ["finlab:current-heads"], "owner": "FinLab",
                                    "discovery_error": "invalid_receipt_identity"}
            continue
        relative = Path(str(receipt.get("parquet_path") or ""))
        path = finlab / relative
        if (not receipt.get("parquet_path") or relative.is_absolute() or ".." in relative.parts
                or not receipt.get("dataset") or path.is_symlink()
                or not path.resolve().is_relative_to(finlab.resolve())):
            entries[receipt_path] = {"families": ["finlab:current-heads"], "owner": "FinLab",
                "dataset": receipt.get("dataset"), "discovery_error": "invalid_or_unsafe_receipt_reference",
                "receipt_sha256": sha256_file(receipt_path)}
            continue
        entries[path] = {"families": ["finlab:current-heads"], "owner": "FinLab",
                         "dataset": receipt["dataset"], "receipt": receipt,
                         "receipt_sha256": sha256_file(receipt_path), "source_root": str(finlab)}
    # These source roots are absent from the shared monitor's physical patterns.
    for name in ("data_public_economic", "data_keyed_public_catalogs", "data_crypto_reference",
                 "data_forex_frankfurter/official_v2", "data_dune_crypto/normalized"):
        folder = root / name
        paths = sorted(folder.rglob("*.parquet")) if folder.is_dir() else []
        families.append({"family": name, "files": len(paths), "audit_scope": "additional_source_tables"})
        for path in paths:
            entries.setdefault(path, {"families": [], "owner": "source_specific_review"})["families"].append(name)
    # Queue candidates are not stored observations. A WHERE status='success'
    # table scan can walk a 19 GB planning DB. Discover actual output files,
    # then let the canonical priority-provider audit verify receipt task IDs.
    folder = root / 'data_openBB/compact_l1'
    paths = sorted(p for p in folder.glob('**/segments/*.parquet') if '_stale' not in p.parts) if folder.is_dir() else []
    families.append({'family': 'openbb:current-l1-segments', 'files': len(paths),
                     'audit_scope': 'current_compacted_source_segments; not_raw_fragment_completeness_proof'})
    for path in paths:
        entries.setdefault(path, {'families': ['openbb:current-l1-segments'], 'owner': 'OpenBB'})
    families.append({'family': 'openbb:raw-query-fragments', 'files': None,
                     'audit_scope': 'separate_streaming_archive_audit; never_materialize_millions_of_paths'})
    return entries, families


def scan(root: Path, out: Path, args) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    snapshot_path = root / "artifacts/live/data_monitor/public_status.json"
    snapshot_bytes = snapshot_path.read_bytes()
    snapshot = json.loads(snapshot_bytes)
    registry = [s for s in snapshot['sources'] if not excluded(s.get('id', '')) and not excluded(s.get('provider', ''))]
    source_rows = [{"id": s['id'], "provider": s.get('provider'), "title": s.get('title'),
                    "registry_alias": s.get('registry_alias'), "state": s.get('operation_state'),
                    "stored_record_evidence": s.get('record_stats', {}).get('state'),
                    "source_status_is_not_quality_proof": True} for s in registry]
    csv_file(out / 'sources.csv', source_rows)
    atomic_write_json(out / 'progress.json', {'contract': CONTRACT, 'state': 'discovering_physical_sources',
        'TEJ_excluded': True, 'network_calls': 0, 'observed_at_utc': datetime.now(UTC).isoformat()})
    entries, families = discover(root)
    csv_file(out / 'physical_families.csv', families)
    configuration = {"contract": CONTRACT, "end_date": args.end_date,
                     "implementation": sha256_file(ROOT / 'downloader/source_anomalies.py')}
    # The DB is diagnostic evidence only; source databases are always read-only.
    database = sqlite3.connect(out / 'scan.sqlite3')
    database.execute('CREATE TABLE IF NOT EXISTS files (path TEXT PRIMARY KEY,binding TEXT NOT NULL,context TEXT NOT NULL,result TEXT NOT NULL)')
    started = time.monotonic()
    last_progress, completed, reused = started, 0, 0
    counts = Counter()
    for path, spec in sorted(entries.items(), key=lambda item: (
            not any(f in {'group:binance', 'group:okx', 'group:bybit'} for f in item[1]['families']), str(item[0]))):
        crypto = any(f in {'group:binance', 'group:okx', 'group:bybit'} for f in spec['families'])
        context = hashlib.sha256(json.dumps({**configuration,
            "profile": "crypto_values" if crypto and args.mode == 'values' else "footer",
            "receipt_sha256": spec.get('receipt_sha256'), "reported_rows": spec.get('reported_rows')},
            sort_keys=True).encode()).hexdigest()
        try:
            binding = json.dumps(file_binding(path, logical=crypto), sort_keys=True)
            previous = database.execute('SELECT result FROM files WHERE path=? AND binding=? AND context=?',
                                        (str(path), binding, context)).fetchone()
            if previous is None and not (crypto and args.mode == 'values'):
                # Upgrading the candle scanner does not invalidate an unchanged
                # v1 footer proof. Bind receipt identity as well as file clocks;
                # failed/racing checks and full-value profiles are never reused.
                candidate = database.execute('SELECT result FROM files WHERE path=? AND binding=?',
                                             (str(path), binding)).fetchone()
                if candidate:
                    cached = json.loads(candidate[0])
                    if (cached.get('contract') == CONTRACT and cached.get('validation') ==
                            'all_row_group_footers; exact_scan_of_infinite_extrema_columns'
                            and cached.get('status') in {'footer_checked', 'invalid_values', 'receipt_integrity_failure'}
                            and cached.get('receipt_sha256') == spec.get('receipt_sha256')
                            and cached.get('reported_rows') == spec.get('reported_rows')):
                        previous = candidate
            if previous and not json.loads(previous[0]).get('unfinished_or_future_rows'):
                result = json.loads(previous[0]); reused += 1
                database.execute('UPDATE files SET context=? WHERE path=?', (context, str(path)))
            else:
                if spec.get('discovery_error'):
                    result = {"path": str(path), "status": "receipt_integrity_failure",
                              "receipt_error": spec['discovery_error']}
                else:
                    result = market_profile(path, args, crypto_1m=True) if crypto and args.mode == 'values' else footer_profile(path)
                if 'receipt' in spec:
                    result['receipt_error'] = parquet_receipt_error(Path(spec['source_root']), spec['receipt'])
                    if result['receipt_error']:
                        result['status'] = 'receipt_integrity_failure'
                if spec.get('reported_rows') is not None and result.get('rows') != spec['reported_rows']:
                    result.update(status='receipt_row_count_mismatch', reported_rows=spec['reported_rows'])
                result.update(**{k: v for k, v in spec.items() if k not in {'receipt', 'source_root'}})
                database.execute('INSERT OR REPLACE INTO files VALUES (?,?,?,?)',
                                 (str(path), binding, context, json.dumps(result, ensure_ascii=False)))
        except Exception as exc:
            preserved = any(part in {'quarantine', '_quarantine', '_stale'} for part in path.parts)
            result = {"path": str(path), "status": "archived_invalid_preserved" if preserved else "unreadable_or_unverified", "error_type": type(exc).__name__,
                      "families": spec['families'], "owner": spec['owner'], "dataset": spec.get('dataset')}
            # No exception contents, private URLs or response bodies in summaries.
            database.execute('INSERT OR REPLACE INTO files VALUES (?,?,?,?)',
                             (str(path), '', context, json.dumps(result, ensure_ascii=False)))
        counts[result['status']] += 1
        completed += 1
        now = time.monotonic()
        if now - last_progress >= 20 or completed == len(entries):
            database.commit()
            atomic_write_json(out / 'progress.json', {"contract": CONTRACT, "observed_at_utc": datetime.now(UTC).isoformat(),
                "state": "scanning", "completed_files": completed, "total_files": len(entries),
                "reused_unchanged_files": reused, "states": dict(counts), "elapsed_seconds": now - started,
                "network_calls": 0, "TEJ_excluded": True})
            print(f'[source-quality] files={completed}/{len(entries)} states={dict(counts)}', flush=True)
            last_progress = now
    database.commit()
    results = [json.loads(row[0]) for path in entries for row in database.execute('SELECT result FROM files WHERE path=?', (str(path),))]
    database.close()
    abnormal = [r for r in results if r['status'] not in {'footer_checked', 'values_checked'}]
    csv_file(out / 'issues.csv', abnormal)
    csv_file(out / 'files.csv', [{k: v for k, v in r.items() if k not in {'file_binding', 'repair_windows_ms'}} for r in results])
    current_finlab = [r for r in abnormal if r.get('owner') == 'FinLab' and r.get('dataset') and r['status'] != 'changed_during_check']
    work = [{"source": "FinLab", "dataset": r['dataset'], "reason": "source_integrity_repair", "priority": 1,
             "unresolved_recheck_keys": r.get('infinite_values') or 1,
             "evidence_status": "current_receipt_or_stored_value_failure"} for r in current_finlab]
    csv_file(out / 'provider_priority_worklist.csv', work)
    by_family = defaultdict(Counter)
    for result in results:
        for family in result['families']:
            by_family[family][result['status']] += 1
    summary = {"contract": CONTRACT, "created_at_utc": datetime.now(UTC).isoformat(),
        "registry_sha256": hashlib.sha256(snapshot_bytes).hexdigest(), "snapshot_at_utc": snapshot.get('generated_at_utc'),
        "registered_sources_excluding_TEJ": len(registry), "excluded_TEJ_registry_rows": len(snapshot['sources']) - len(registry),
        "unique_current_physical_files": len(entries), "states": dict(counts),
        "families": {k: dict(v) for k, v in sorted(by_family.items())}, "mode": args.mode,
        "elapsed_seconds": time.monotonic() - started, "network_calls": 0, "TEJ_excluded": True,
        "priority_dataset_requests": len(work), "all_source_values_or_history_complete": False,
        "limits": ["Footers do not prove OHLC geometry, primary-key uniqueness or missing-calendar completeness",
                   "Continuous candle missing instants need finite upstream confirmation",
                   "FinMind current receipts are checked by the existing independent full sweeps",
                   "Document/tick/native sparse grains require their producer-specific contracts",
                   "Archived and derived representations are not separate acquisition obligations"],
        "outputs": {p.name: {"sha256": sha256_file(p), "bytes": p.stat().st_size}
                    for p in out.glob('*.csv')}}
    atomic_write_json(out / 'audit_manifest.json', summary)
    atomic_write_json(out / 'progress.json', {**summary, "state": "audit_finished_not_download_complete"})
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--mode', choices=('footers', 'values'), default='values')
    parser.add_argument('--end-date', default='today')
    parser.add_argument('--stale-max-lag-days', type=int, default=14)
    parser.add_argument('--daily-gap-days', type=int, default=10)
    parser.add_argument('--intraday-gap-multiple', type=float, default=4)
    args = parser.parse_args()
    result = scan(args.root.resolve(), args.output_dir, args)
    print(json.dumps({k: v for k, v in result.items() if k not in {'families', 'outputs'}}, ensure_ascii=False))


if __name__ == '__main__':
    main()
