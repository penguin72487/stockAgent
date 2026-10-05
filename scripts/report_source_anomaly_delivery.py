#!/usr/bin/env python3
"""Reconcile quality, expected-grain and dispatch evidence without fake completion."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import UTC, datetime
import json
from pathlib import Path
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.quality_priority import load_plan
from scripts.audit_source_anomalies import csv_file, excluded


def read(path):
    return json.loads(path.read_text()) if path.is_file() else {}


def provider_scope(provider: str) -> tuple[str, str]:
    """A related audit is not a claim that every endpoint of a brand was checked."""
    name = provider.lower()
    if 'finlab' in name:
        return 'current_head_receipt_footer_and_nonfinite_rechecks', 'finlab-workload.json;recheck-acceptance/nonfinite_feature_verification.json'
    if 'finmind' in name:
        return '451598_current_positive_receipt_integrity_not_all_values', 'finmind-sponsor-integrity.json;finmind-complement-integrity.json;finmind-workload/inventory.csv'
    if 'shioaji' in name or '永豐' in name:
        return 'stock_positive_volume_symbol_days_and_current_minute_partitions_not_all_raw_ticks', 'shioaji-history/summary.json;tw-minute-coverage/summary.json'
    if provider in {'Binance', 'Bybit', 'OKX'}:
        return 'every_canonical_perpetual_1m_OHLCV_and_key_not_every_feature_or_endpoint', 'current-scan/audit_manifest.json;source-window-proof/summary.json'
    if 'yahoo' in name or 'yfinance' in name:
        return 'canonical_OHLCV_baseline_and_finite_reconfirmed_repair_intents', 'ohlcv-baseline/data_quality_summary.json;all-market-priority-receipt.json'
    if 'taifex' in name or 'futures exchange' in name:
        return 'inventory_receipts_rule_extraction_backlog_not_all_document_semantics', 'taifex/taifex_public_inventory.json'
    if any(n in name for n in ('openbb', 'bea', 'census', 'moi', 'frankfurter')):
        return 'current_footers_and_declared_macro_receipts_not_all_observation_keys', 'current-scan/audit_manifest.json;macro/verification_20261005T031217Z.json'
    if any(n in name for n in ('twse', 'tpex', 'mops', 'tdcc', '中央銀行', '主計', '財政部', '台股公開')):
        return 'registered_footers;stock_native_values_and_9_daily_source_calendars_only', 'current-scan/audit_manifest.json;tw-native/summary.json;tw-raw/summary.json'
    return 'registry_and_discovered_current_footers_only;endpoint_semantics_unverified', 'current-scan/audit_manifest.json'


def empty_minute_day_classification(audit: Path) -> dict:
    """Trace an empty training day to broker availability before issuing calls."""
    minute_path = audit / 'tw-minute-coverage/pair_coverage.parquet'
    if not minute_path.is_file():
        return {'state': 'not_audited'}
    frame = pl.read_parquet(minute_path, columns=[
        'symbol', 'trade_date', 'reference_status', 'observed_active_minutes',
        'full_grid_physical_minutes', 'full_grid_observed_trade_minutes'])
    frame = frame.filter(pl.col('observed_active_minutes') == 0)
    with (audit / 'shioaji-history/missing_symbol_days.csv').open(newline='') as stream:
        source_outcomes = {(r['symbol'], r['trade_date']): r['category'] for r in csv.DictReader(stream)}
    rows = []
    for row in frame.to_dicts():
        category = ('minute_only_unverified_reference' if row['reference_status'] != 'official_positive_volume'
                    else source_outcomes.get((row['symbol'], str(row['trade_date'])),
                                            'source_day_returned_scope_or_view_needs_reconciliation'))
        rows.append({**row, 'broker_source_category': category,
                     'not_proven_missing_regular_trade_execution': True})
    csv_file(audit / 'tw-minute-empty-day-classification.csv', rows)
    return {'state': 'classified', 'empty_regular_session_symbol_days': len(rows),
            'by_broker_source_category': dict(Counter(r['broker_source_category'] for r in rows)),
            'source_empty_not_verified_missing_trades': True,
            'sha256': sha256_file(audit / 'tw-minute-empty-day-classification.csv')}


def build(root: Path, audit: Path, *, bybit_after_run: Path | None = None) -> dict:
    manifest = read(audit / 'current-scan/audit_manifest.json')
    if not manifest.get('TEJ_excluded'):
        raise ValueError('TEJ exclusion not verified')
    with (audit / 'registry.csv').open(newline='') as stream:
        registry = [r for r in csv.DictReader(stream) if not excluded(r['provider']) and not excluded(r['id'])]
    by_provider = defaultdict(list)
    for row in registry:
        by_provider[row['provider']].append(row)
    provider_rows = []
    for provider, rows in sorted(by_provider.items()):
        scope, evidence = provider_scope(provider)
        provider_rows.append({
            'provider': provider, 'registered_endpoints_or_references': len(rows),
            'registry_aliases': sum(r.get('registry_alias') == 'True' for r in rows),
            'declared_work_classes': dict(Counter(r['work_class'] for r in rows)),
            'declared_operation_states_not_quality_proof': dict(Counter(r['operation_state'] for r in rows)),
            'actual_quality_proof_scope': scope, 'evidence_under_audit_root': evidence,
            'all_provider_history_and_values_verified': False,
            'no_uniform_theoretical_rows_assumed': True,
        })
    csv_file(audit / 'provider_quality_coverage.csv', provider_rows)
    acceptance = read(audit / 'recheck-acceptance/summary.json')
    window = read(audit / 'source-window-proof/summary.json')
    tw = read(audit / 'tw-native/summary.json')
    raw = read(audit / 'tw-raw/summary.json')
    minute = read(audit / 'tw-minute-coverage/summary.json')
    shioaji = read(audit / 'shioaji-history/summary.json')
    macro = read(audit / 'macro/verification_20261005T031217Z.json')
    taifex = read(audit / 'taifex/taifex_public_inventory.json')
    finlab = read(audit / 'finlab-workload.json')
    finmind = [read(audit / f'finmind-{owner}-integrity.json') for owner in ('sponsor', 'complement')]
    if window and window.get('evidence_sha256') != sha256_file(audit / 'source-window-proof/source_window_evidence.json'):
        raise ValueError('source-window evidence changed')
    priorities = []
    queues = []
    for relative in ('data_binance/1m', 'data_bybit/1m', 'data_okx/1m',
                     'data_yahoo/us_stocks', 'data_yahoo/forex', 'data_yahoo/crypto'):
        plan = load_plan(root / relative)
        states = dict(Counter(r.get('state') for r in plan['requests']))
        manifest_path = root / relative / 'symbols.csv'
        with manifest_path.open(newline='') as stream:
            codes = {row['code'] for row in csv.DictReader(stream)}
        queues.append({'owner_root': relative, 'requests': len(plan['requests']), 'states': states,
                       'present_in_current_native_manifest': sum(r['code'] in codes for r in plan['requests']),
                       'manifest_presence_is_not_automatic_schedule_guarantee': True})
        for row in plan['requests']:
            priorities.append({
                'owner': relative, 'key': row['code'], 'reason': row['reason'],
                'priority_after_due_refresh': 1, 'state': row['state'], 'attempts': row['attempts'],
                'evidence_sha256': row['evidence_sha256'],
                'present_in_current_native_manifest': row['code'] in codes,
                'native_dispatch': ('legacy_grain_review_no_1m_overwrite' if row['state'] == 'legacy_grain_review'
                                    else 'existing_historical_collector_when_owner_scope_selected'),
                'source_checked_is_not_gap_fill': True,
            })
    features = read(audit / 'recheck-acceptance/nonfinite_feature_verification.json')
    for item in features.get('finlab_current_receipt_heads', []):
        priorities.append({'owner': 'FinLab', 'key': item['dataset'], 'reason': 'nonfinite_values_domain_review',
            'priority_after_due_refresh': 1, 'state': 'upstream_rechecked_nonfinite_values_remain',
            'remaining_nonfinite_cells': item['infinite_values'],
            'native_dispatch': 'existing_FinLab_priority_dataset_intent_consumed',
            'evidence_sha256': sha256_file(audit / 'recheck-acceptance/nonfinite_feature_verification.json')})
    priorities.append({'owner': 'Shioaji', 'key': 'positive_volume_stock_symbol_days',
        'reason': 'previous_source_empty_requires_bounded_recheck', 'priority_after_due_refresh': 1,
        'remaining_symbol_days': shioaji.get('counts', {}).get('source_gap'),
        'state': 'existing_afterclose_completed_target_session_retry',
        'native_dispatch': 'run_shioaji_minute_full_backfill.sh --retry-source-gaps --fallback-missing-kbars-to-ticks',
        'market_open_new_login_or_forced_restart': False})
    for item in features.get('binance_non_price_features', []):
        priorities.append({'owner': 'Binance feature normalizer', 'key': Path(item['path']).name,
            'reason': 'nonfinite_ratio_or_log_requires_denominator_domain_review',
            'remaining_nonfinite_cells': item['infinite_values'],
            'state': 'not_a_candle_download_repair', 'native_dispatch': 'producer_specific_review_not_new_venue'})
    for item in macro.get('economic', []):
        for dataset in item['datasets']:
            if dataset.get('rejected_rows') or dataset.get('invalid_transaction_dates'):
                priorities.append({'owner': item['provider'], 'key': dataset['dataset'],
                    'reason': 'source_CSV_or_transaction_date_invalid',
                    'state': 'raw_and_rejected_rows_preserved_no_ambiguous_field_or_date_guess',
                    'rejected_rows': dataset.get('rejected_rows', 0),
                    'invalid_transaction_dates': dataset.get('invalid_transaction_dates', 0),
                    'native_dispatch': 'existing_economic_source_revision_refresh;semantic_repair_needs_source_evidence'})
    parsing = taifex.get('summary', {}).get('rule_archive_quality', {}).get('parsing_status_counts', {})
    priorities.append({'owner': 'TAIFEX rule parser', 'key': 'already_downloaded_documents',
        'reason': 'structured_extraction_backlog_not_missing_source_bytes',
        'pending_ocr_documents': parsing.get('pending_ocr'), 'failed_parser_documents': parsing.get('failed'),
        'state': 'remaining_local_extraction_work_not_started_by_this_audit',
        'native_dispatch': 'existing_rule_parser;no_new_download_or_GPU_job_created'})
    with (audit / 'current-scan/issues.csv').open(newline='') as stream:
        for issue in csv.DictReader(stream):
            path = Path(issue['path'])
            if path.parent == root / 'data_parquet' and issue['status'] == 'unreadable_or_unverified':
                priorities.append({'owner': 'legacy_stock_projection', 'key': path.name,
                    'reason': 'legacy_unreadable_file_with_healthy_authoritative_TW_source',
                    'state': 'ABI_and_consumer_review_before_local_reconstruction',
                    'native_dispatch': 'no_duplicate_provider_fetch;no_active_training_artifact_overwrite'})
    csv_file(audit / 'priority_repair_worklist.csv', priorities)
    performance = {'state': 'not_measured_in_this_report'}
    if bybit_after_run is not None:
        comparison = read(bybit_after_run / 'progress.json')
        compare_summary = read(bybit_after_run / 'download_summary.json')
        codes = set(compare_summary.get('requested_symbol_filter') or [])
        old_plan = load_plan(root / 'data_bybit/1m')
        old_attempts = {r['code']: r['history'][0]['request_pages'] for r in old_plan['requests']
                        if r['code'] in codes and r.get('history')}
        before_pages = sum(old_attempts.values())
        after_pages = comparison.get('telemetry_counts', {}).get('request_pages')
        if codes and codes == set(old_attempts) and before_pages and after_pages is not None:
            performance = {
                'scope': sorted(codes), 'before_kline_pages': before_pages, 'after_kline_pages': after_pages,
                'saved_request_fraction': 1 - after_pages / before_pages,
                'before_per_symbol_receipt_pages': old_attempts,
                'before_intent_receipt_sha256': sha256_file(root / 'data_bybit/1m/quality_priority_requests.json'),
                'after_progress_sha256': sha256_file(bybit_after_run / 'progress.json'),
                'throughput_or_all_provider_completion_not_extrapolated': True,
                'evidence': str(bybit_after_run.resolve().relative_to(root.resolve())),
            }
    result = {
        'contract': 'source_quality_delivery_v1', 'TEJ_excluded': True,
        'observed_at_utc': datetime.now(UTC).isoformat(),
        'registry_snapshot_at_utc': manifest.get('snapshot_at_utc'),
        'registered_sources_excluding_TEJ': len(registry), 'provider_labels': len(provider_rows),
        'unique_current_physical_files': manifest['unique_current_physical_files'],
        'physical_audit_states': manifest['states'],
        'TW_native_quote_audit': tw.get('quotes'), 'TW_daily_source_calendars': tw.get('historical_source_tables'),
        'TW_raw_classification': raw, 'TW_minute_current_partition_audit': minute,
        'TW_minute_empty_day_source_reconciliation': empty_minute_day_classification(audit),
        'Shioaji_symbol_day_classification': shioaji.get('counts'),
        'FinMind_positive_current_receipts': {'checked': sum(r.get('checked', 0) for r in finmind),
            'failed': sum(r.get('failed', 0) for r in finmind), 'not_full_numeric_or_historical_proof': True},
        'FinLab_nonfinite_current_heads': features.get('finlab_current_receipt_heads'),
        'FinLab_runtime_dispatch_checks': finlab.get('checks'),
        'native_source_gap_window_confirmation': window,
        'after_collector_logical_validation': acceptance,
        'source_quality_queues': queues, 'repair_worklist_rows_not_missing_observation_total': len(priorities),
        'Bybit_sparse_head_performance': performance,
        'macro_rejects_and_receipt_scope': [{k: v for k, v in r.items() if k != 'datasets'}
                                         for r in macro.get('economic', [])],
        'TAIFEX_rule_extraction_not_download_backlog': taifex.get('summary', {}).get('rule_archive_quality'),
        'all_source_history_values_or_field_semantics_complete': False,
        'limits': [
            'Current registered sources were inventoried; raw OpenBB millions of fragments were not all read',
            'Footer and receipt checks do not prove every primary key, economic domain or sparse-feature applicability',
            'TW minute clock-grid absences include no trades, auctions, lifecycle and unpublished source outcomes',
            'Source-only invalid observations require source evidence, not invented prices, dates or another venue',
            'Failed source rechecks cap at 3; unchanged upstream anomalies remain visible, not falsely complete',
            'No TEJ collector, source bytes, quota or completeness was inspected or changed',
        ],
        'preference_applied': 'canonical_owners;summary_first;explicit_proof_scope;affected_scope_repair',
        'evidence': {},
    }
    for rel in ('current-scan/audit_manifest.json', 'tw-native/summary.json', 'tw-raw/summary.json',
                'tw-minute-coverage/summary.json', 'shioaji-history/summary.json', 'recheck-acceptance/summary.json',
                'source-window-proof/summary.json', 'finmind-sponsor-integrity.json',
                'finmind-complement-integrity.json', 'provider_quality_coverage.csv', 'priority_repair_worklist.csv'):
        path = audit / rel
        result['evidence'][rel] = {'sha256': sha256_file(path)} if path.is_file() else {'state': 'not_available'}
    atomic_write_json(audit / 'delivery_summary.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--audit-dir', type=Path, required=True)
    parser.add_argument('--bybit-after-run', type=Path)
    args = parser.parse_args()
    result = build(args.root, args.audit_dir, bybit_after_run=args.bybit_after_run)
    print(json.dumps({k: result[k] for k in ('TEJ_excluded', 'registered_sources_excluding_TEJ',
        'provider_labels', 'unique_current_physical_files', 'repair_worklist_rows_not_missing_observation_total',
        'all_source_history_values_or_field_semantics_complete')}, ensure_ascii=False))


if __name__ == '__main__':
    main()
