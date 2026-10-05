#!/usr/bin/env python3
"""Explain every inventoried provider/source state without resetting queues."""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json, atomic_write_text
from downloader.finmind_correction_plans import resolve_owner
from downloader.parquet_integrity import parquet_receipt_error
from scripts.prepare_tw_day_trade_feature_catalog import inspect_task, sha256, write_csv
from stockagent.data.finlab_acquisition_contract import safe_stem
from scripts.download_finlab_history import verified_source_empty


def read_csv(path):
    with path.open(encoding='utf-8-sig', newline='') as stream:
        return list(csv.DictReader(stream))


def report(catalog: Path, out: Path):
    if out.exists(): raise FileExistsError('use an immutable provider audit directory')
    out.mkdir(parents=True)
    states = []
    for row in read_csv(catalog / 'feature_candidates.csv'):
        reason, action = row['reason'], 'existing_value_needs_semantic_clock_or_unit_adapter'
        if row['admission'] == 'not_model_input':
            reason, action = 'identifier_text_or_metadata_not_numeric_feature', 'do_not_request_as_numeric_gap'
        elif row['admission'] == 'missing_values':
            reason, action = 'no_numeric_value_in_inspected_representation_not_provider_corruption_proof', 'compare_native_source_or_applicability'
            if row['field'] == 'twpub_mof_business_tax_log':
                reason, action = 'legacy_derived_null_but_native_signed_business_tax_already_available', 'use_existing_tw_native_macro_raw_quantity'
            elif 'forex' in row['dataset_id'] and row['field'] == 'Trading_Volume':
                reason, action = 'no_exchange_wide_spot_fx_volume_at_this_provider', 'not_applicable_keep_null'
        elif row['admission'] == 'incomplete_source':
            reason, action = 'inventory_or_concurrent_read_unverified', 'recheck_exact_current_receipt_before_retry'
        states.append({**row, 'gap_classification': reason, 'next_gap_action': action})
    rechecks = []
    for old in read_csv(catalog / 'source_failures.csv'):
        if old['provider'] != 'FinMind':
            rechecks.append({**old, 'current_state': 'needs_owner_source_recheck'}); continue
        base = ROOT / 'data_finmind' / old['lane']
        with sqlite3.connect(f'file:{base / "queue.sqlite3"}?mode=ro', uri=True) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute('SELECT dataset,data_id,partition,rows,receipt_path,state FROM tasks WHERE dataset=? AND data_id=? AND partition=?',
                               (old['dataset'], old['data_id'], old['partition'])).fetchone()
        if row is None:
            rechecks.append({**old, 'current_state': 'task_identity_not_found'}); continue
        current = inspect_task((base, dict(row))); current.pop('stats', None)
        receipt = json.loads(Path(current['receipt_path']).read_text())
        independent = parquet_receipt_error(base, receipt)
        rechecks.append({'original_scan': old, 'current_receipt': current, 'full_content_error': independent,
                        'current_state': 'concurrent_refresh_now_verified' if not current.get('error') and independent is None else 'actual_current_integrity_failure'})
    ownership = []
    for row in read_csv(catalog / 'finmind_partition_states.csv'):
        if not row['dataset'].startswith('Taiwan'): continue
        owner = resolve_owner(row['dataset'])
        row = {**row, 'canonical_owner': owner}
        if row['lane'] != owner:
            row['gap_classification'] = 'non_owner_or_optional_duplicate_history; not_primary_gap_proof'
        elif row['state'] == 'complete':
            row['gap_classification'] = 'downloaded_partition_not_every_issuer_account_completeness'
        elif row['state'] in {'not_observation_date', 'non_session', 'calendar_wait'}:
            row['gap_classification'] = 'normal_calendar_or_release_frequency'
        elif row['state'] == 'observed_empty':
            row['gap_classification'] = 'provider_empty_with_receipt; verify_documented_start_or_release_due'
        else:
            row['gap_classification'] = 'owner_pending_or_retry; existing_worker_preserved'
        ownership.append(row)
    source_empty = []
    for key in ['dividend_otc:權息', 'management_change_events:變更交易開始日']:
        root = ROOT / 'data_finlab'
        attempt_path = root / 'attempts' / (safe_stem(key) + '.json')
        attempt = json.loads(attempt_path.read_text()) if attempt_path.exists() else {}
        source_empty.append({'dataset': key, 'status': attempt.get('status'),
            'verified_provider_all_null': verified_source_empty(key, root, attempt),
            'attempted_at_utc': attempt.get('attempted_at_utc'),
            'next_action': 'existing_source_empty_cooldown; never_fill_false_zero'})
    write_csv(out / 'all_source_field_states.csv', states)
    write_csv(out / 'finmind_canonical_owner_states.csv', ownership)
    write_csv(out / 'finlab_source_empty.csv', source_empty)
    atomic_write_json(out / 'exact_failure_rechecks.json', rechecks)
    issues = [r for r in rechecks if r['current_state'] != 'concurrent_refresh_now_verified']
    verified_ids = {r['original_scan']['lane'] + ':' + r['original_scan']['dataset'] for r in rechecks
                    if r['current_state'] == 'concurrent_refresh_now_verified'}
    for row in states:
        if row['dataset_id'] in verified_ids and row['admission'] == 'incomplete_source':
            row.update(gap_classification='inventory_refresh_race_now_exact_receipt_verified',
                       next_gap_action='retain_original_scan; no_duplicate_download_required')
    for row in ownership:
        if row['lane'] != row['canonical_owner']:
            continue
        if row['state'] == 'deprecated_query_shape':
            row['gap_classification'] = 'obsolete_query_not_a_numeric_null_repair; canonical_replacement_only'
        elif row['state'] == 'outside_documented_range':
            row['gap_classification'] = 'outside_documented_provider_history; not_due_repair'
        row['automatic_missing_value_priority'] = False
    # Persist the resolved DTO, retaining the old admission and original failures.
    write_csv(out / 'all_source_field_states.csv', states)
    write_csv(out / 'finmind_canonical_owner_states.csv', ownership)
    summary = {'created_at_utc': datetime.now(UTC).isoformat(),
        'catalog_sha256': sha256(catalog / 'catalog_summary.json'), 'all_source_field_rows': len(states),
        'canonical_tw_queue_states': len(ownership), 'original_scan_failures': len(rechecks),
        'currently_unverified_or_failed_sources': len(issues), 'network_requests': 0,
        'classes': dict(Counter(r['gap_classification'] for r in states)),
        'all_provider_history_complete': False}
    atomic_write_json(out / 'manifest.json', summary)
    atomic_write_text(out / 'index.md', '# Provider 缺值狀態與當前收據\n\n'
        f"- 全來源欄位表示逐項分類：{len(states):,} 筆。[完整清單](all_source_field_states.csv)。\n"
        f"- 原掃描異常 {len(rechecks)} 筆；重新核對當前筆數及完整 SHA 後尚未通過 {len(issues)} 筆。[原始與重查證據](exact_failure_rechecks.json)。\n"
        '- [FinMind 唯一下載所有權與狀態](finmind_canonical_owner_states.csv)：Complement 未做完的重複歷史不等於 Sponsor 主來源缺值。\n'
        '- [FinLab 官方全 NULL 回應](finlab_source_empty.csv)：保留證據與正常重試，不補假零。\n'
        '- 不把未接線、經濟上無定義、日曆非觀測日、尚未公布、optional 科目和來源損壞合併成一個「缺資料」數。\n')
    return summary


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--catalog', type=Path, required=True); p.add_argument('--output-dir', type=Path, required=True)
    a = p.parse_args(); print(json.dumps(report(a.catalog, a.output_dir), ensure_ascii=False, indent=2))


if __name__ == '__main__': main()
