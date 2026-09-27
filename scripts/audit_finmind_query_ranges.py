"""Inventory configured FinMind query scopes using local evidence only.

No provider requests, queue migrations, or Parquet reads. Queue aggregates and
status/receipt claims are observations, not independent content verification or
proof of complete historical universes. The only writes are new report files.
"""

from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime
import io
import json
from pathlib import Path
import sqlite3
from typing import Any

from downloader import download_finmind_complement as complement
from downloader import download_finmind_free as free
from downloader import download_finmind_sponsor as sponsor
from downloader import finmind_batching as batching
from downloader.artifact_io import atomic_write_json, atomic_write_text
from downloader.finmind_observation_dates import PERIOD_DATASETS
from downloader.finmind_scheduling import PRODUCT_HISTORY_STARTS


CATALOG_URL = 'https://finmind.github.io/llms-full.txt'
COUNT_STATES = ('pending', 'complete', 'observed_empty', 'failed')
FAILED_STATES = frozenset({'failed', 'blocked', 'not_entitled', 'invalid_request', 'partial'})
SETTLEMENT_EVIDENCE = 'artifacts/data_quality/finmind_max_ranges_20260927T024357385865Z.json'
SETTLEMENT_URLS = ['https://finmind.github.io/tutor/TaiwanMarket/Derivative/',
                   'https://github.com/FinMind/FinMind-MCP/blob/master/knowledge/datasets.md',
                   'https://github.com/FinMind/FinMind/blob/master/tests/data/test_data_loader.py']


def _contract(shape: str, first: str | None, max_span: str, **extra: Any) -> dict[str, Any]:
    return {'query_shape': shape, 'configured_first_date': first, 'max_span': max_span,
            'provider_maximum_span': None, 'provider_limit_basis': 'not_documented_not_unlimited',
            'source_urls': [CATALOG_URL], **extra}


def registry() -> dict[str, dict[str, Any]]:
    """One row per provider dataset; owners are aliases, never extra endpoints."""
    result: dict[str, dict[str, Any]] = {}

    def add(dataset: str, owner: str, contract: dict[str, Any]) -> None:
        row = result.setdefault(dataset, {'dataset': dataset, 'owners': [], 'owner_contracts': {},
                                          'disabled': False})
        row['owners'].append(owner)
        row['owner_contracts'][owner] = contract

    for spec in sponsor.SOURCES:
        first = spec.first_date.isoformat() if spec.first_date else None
        if spec.dataset in PRODUCT_HISTORY_STARTS:
            contract = _contract('deprecated_no_id_query_shape', first, 'no_requests_legacy_alias_only',
                                 legacy_alias=True, delegated_to_owner='complement',
                                 source_status='delegated_to_complement_product_history',
                                 excluded_queue_state='deprecated_query_shape',
                                 source_urls=SETTLEMENT_URLS,
                                 contract_evidence=SETTLEMENT_EVIDENCE,
                                 contract_uncertainty='official_mcp_omits_id_requirement_but_same_span_no_id_probe_was_empty',
                                 no_id_history_verified_usable=False)
        elif spec.dataset == complement.WIDE_INSTITUTIONAL:
            contract = _contract('derived_no_api', first, 'verified_long_parent_partition')
        elif spec.dataset in batching.RANGE_CONTRACTS:
            range_spec = batching.RANGE_CONTRACTS[spec.dataset]
            contract = _contract('whole_market_date_range', first, 'all_due_contiguous_pending_periods',
                                 calendar_partition_limit=range_spec.max_partitions,
                                 decoded_response_byte_limit=range_spec.max_response_bytes,
                                 response_row_limit=range_spec.max_response_rows,
                                 source_urls=[range_spec.documentation_url],
                                 range_contract_version=batching.BATCH_CONTRACT_VERSION)
        elif spec.grain == 'snapshot':
            contract = _contract('snapshot', first, 'one_current_snapshot')
        elif spec.dataset in PERIOD_DATASETS:
            contract = _contract('whole_market_period_anchor', first, 'one_observation_date',
                                 pre_2014_non_anchor_exclusion_verified=False,
                                 source_urls=['https://finmind.github.io/tutor/TaiwanMarket/Fundamental/'])
        elif spec.grain == 'day':
            contract = _contract('whole_market_day', first, 'one_observation_date')
        else:
            contract = _contract('unverified_whole_market_date_range', first,
                                 f'configured_one_{spec.grain}_not_provider_limit')
        contract['storage_grain'] = spec.grain
        add(spec.dataset, 'sponsor', contract)

    id_starts = {'TaiwanExchangeRate': '2006-01-01',
                 'TaiwanFuturesDealerTradingVolumeDaily': '2021-04-01',
                 'TaiwanOptionDealerTradingVolumeDaily': '2021-04-01',
                 'TaiwanStockCapitalReductionReferencePrice': '2011-01-01'}
    for dataset in complement.ALL_DATASETS:
        if dataset in PRODUCT_HISTORY_STARTS:
            contract = _contract('per_product_history', PRODUCT_HISTORY_STARTS[dataset].isoformat(),
                                 'one_product_documented_start_through_today_inclusive',
                                 first_date_basis='documented_floor_not_first_returned_event',
                                 historical_universe_verified_complete=False,
                                 configured_universe_source='futures_or_options_master_all_known_product_ids',
                                 query_contract_basis='explicit_product_full_span_probe_verified',
                                 with_id_documented_examples=['TX' if dataset.startswith('TaiwanFutures') else 'TXO'],
                                 request_end_inclusive=True, open_ended_history_request=False,
                                 source_urls=SETTLEMENT_URLS, contract_evidence=SETTLEMENT_EVIDENCE,
                                 source_date_semantics='expiry_date',
                                 decoded_response_byte_limit=complement.BULK_MAX_RESPONSE_BYTES,
                                 response_row_limit=complement.BULK_MAX_RESPONSE_ROWS)
        elif dataset == complement.WIDE_INSTITUTIONAL:
            contract = _contract('derived_no_api', None, 'verified_long_parent_partition')
        elif dataset in complement.SNAPSHOTS:
            contract = _contract('snapshot', None, 'one_current_snapshot')
        elif dataset in complement.GLOBAL_START_YEAR:
            first = f'{complement.GLOBAL_START_YEAR[dataset]}-01-01'
            bulk = dataset in complement.BULK_GLOBAL_HISTORY
            contract = _contract('global_date_range' if bulk else 'global_annual_partition', first,
                                 'all_due_contiguous_periods' if bulk else 'configured_one_year_not_provider_limit',
                                 first_date_basis='configured_request_floor_not_verified_provider_earliest',
                                 open_ended_history_request=False,
                                 calendar_partition_limit=None if bulk else 1,
                                 decoded_response_byte_limit=complement.BULK_MAX_RESPONSE_BYTES if bulk else None,
                                 response_row_limit=complement.BULK_MAX_RESPONSE_ROWS if bulk else None,
                                 response_end_semantics=('next_day_midnight_then_client_half_open_filter'
                                                         if dataset == 'GoldPrice' and bulk else
                                                         'inclusive_observation_date'))
        else:
            contract = _contract('per_id_date_range', id_starts.get(dataset, '1900-01-01'),
                                 'one_id_configured_start_through_latest',
                                 first_date_basis='configured_request_floor_not_verified_provider_earliest',
                                 historical_universe_verified_complete=False,
                                 open_ended_history_request=True)
            if dataset in complement.FIXED_ID_HISTORY:
                contract['configured_ids'] = list(complement.FIXED_ID_HISTORY[dataset])
                contract['configured_id_count'] = len(complement.FIXED_ID_HISTORY[dataset])
                contract['universe_scope'] = 'configured_fixed_ids_not_all_possible_provider_ids'
            elif dataset == 'TaiwanExchangeRate':
                contract['configured_ids'] = list(complement.CURRENCIES)
                contract['configured_id_count'] = len(complement.CURRENCIES)
        add(dataset, 'complement', contract)

    for dataset in free.SESSION_DATASETS:
        add(dataset, 'free', _contract('whole_market_day', str(free.HISTORY_START), 'one_observation_date'))
    for dataset in (free.CALENDAR_DATASET, free.MASTER_DATASET):
        add(dataset, 'free', _contract('calendar_snapshot' if dataset == free.CALENDAR_DATASET else 'snapshot',
                                       None, 'one_provider_table'))
    for dataset, reason in sponsor.UNSCHEDULED.items():
        if 'snapshot' in reason:
            shape, span = 'unscheduled_live_snapshot', 'current_snapshot_not_history'
        elif 'sponsorpro_bulk' in reason:
            shape, span = 'unscheduled_sponsorpro_day_object', 'one_day_object_requires_separate_tier'
        elif 'per_day' in reason:
            shape, span = 'unscheduled_per_id_day', 'one_id_one_day'
        else:
            shape, span = 'unscheduled_per_id_or_special_endpoint', 'not_verified_in_current_worker'
        add(dataset, 'unscheduled', _contract(shape, None, span, unscheduled_reason=reason))
    add('TaiwanStockNews', 'disabled_policy', _contract('disabled_news', None, 'not_requested_explicitly_disabled',
                                                      disabled_reason='news_explicitly_disabled'))
    result['TaiwanStockNews']['disabled'] = True
    for row in result.values():
        preferred = (('complement', 'sponsor', 'free', 'unscheduled', 'disabled_policy')
                     if row['dataset'] in PRODUCT_HISTORY_STARTS else
                     ('sponsor', 'complement', 'free', 'unscheduled', 'disabled_policy'))
        owner = next(owner for owner in preferred if owner in row['owners'])
        row['primary_owner'] = owner
        row.update({key: row['owner_contracts'][owner][key]
                    for key in ('query_shape', 'configured_first_date', 'max_span')})
        row['source_urls'] = sorted({url for contract in row['owner_contracts'].values()
                                     for url in contract['source_urls']})
    return result


def _json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _unknown(basis: str) -> dict[str, Any]:
    return {'observation_basis': basis, 'state_counts': None, 'pending_due': None,
            'first_data_date': None, 'last_data_date': None, 'rows': None,
            'known_data_id_count': None, 'failed_or_blocked': None, 'excluded_deprecated': None,
            **{key: None for key in COUNT_STATES}}


def _queue_observations(path: Path, now: datetime) -> tuple[dict[str, Any], dict[str, Any]]:
    if not path.is_file():
        return {}, {'path': str(path), 'state': 'missing_queue'}
    values: dict[str, Any] = {}
    try:
        with sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=5) as conn:
            conn.execute('PRAGMA query_only=ON')
            for dataset, state, count, due in conn.execute(
                "SELECT dataset,state,count(*),sum(CASE WHEN state='pending' AND "
                "(next_attempt_at_utc IS NULL OR next_attempt_at_utc<=?) THEN 1 ELSE 0 END) "
                "FROM tasks GROUP BY dataset,state", (now.isoformat(),),
            ):
                item = values.setdefault(dataset, {**_unknown('readonly_queue_aggregate'),
                                                    'state_counts': {}, 'pending_due': 0})
                item['state_counts'][state] = count
                item['pending_due'] += due
            for dataset, first, last, rows, ids, first_partition, last_partition in conn.execute(
                "SELECT dataset,min(CASE WHEN rows>0 THEN first_data_date END),"
                "max(CASE WHEN rows>0 THEN last_data_date END),sum(rows),"
                "count(DISTINCT CASE WHEN data_id!='' THEN data_id END),min(partition),max(partition) "
                "FROM tasks GROUP BY dataset"
            ):
                values[dataset].update(first_data_date=first, last_data_date=last, rows=rows,
                                       known_data_id_count=ids, first_partition=first_partition,
                                       last_partition=last_partition)
            for item in values.values():
                item.update({key: item['state_counts'].get(key, 0) for key in COUNT_STATES})
                item['failed_or_blocked'] = sum(item['state_counts'].get(key, 0) for key in FAILED_STATES)
                item['excluded_deprecated'] = item['state_counts'].get('deprecated_query_shape', 0)
            policies = {}
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if 'request_batch_limits' in tables:
                policies['learned_batch_limits'] = dict(conn.execute('SELECT dataset,max_partitions FROM request_batch_limits'))
            if 'request_batch_policy' in tables:
                policies['disabled_batching'] = dict(conn.execute('SELECT dataset,error_code FROM request_batch_policy'))
            if 'complement_year_batch_policy' in tables:
                policies['learned_year_batch_limits'] = dict(conn.execute(
                    'SELECT dataset,max_partitions FROM complement_year_batch_policy'))
        return values, {'path': str(path), 'state': 'observed', **policies}
    except sqlite3.Error as error:
        return {}, {'path': str(path), 'state': 'queue_unreadable', 'error_type': type(error).__name__}


def _free_observation(root: Path, dataset: str, status: dict[str, Any]) -> dict[str, Any]:
    item = _unknown('missing_status_or_receipt')
    series = status.get('series', {}).get(dataset)
    if isinstance(series, dict):
        total, complete = series.get('total'), series.get('complete')
        pending = total - complete if isinstance(total, int) and isinstance(complete, int) else None
        item.update(observation_basis='reported_free_status_not_revalidated',
                    state_counts={'complete': complete, 'pending': pending},
                    pending=pending, complete=complete, rows=series.get('rows'),
                    first_data_date=series.get('first_complete_date'), last_data_date=series.get('last_complete_date'))
    elif dataset == free.CALENDAR_DATASET:
        receipt = _json(root / 'calendar.json')
        dates = receipt.get('dates')
        if receipt.get('source_dataset') == dataset and isinstance(dates, list) and dates:
            item.update(observation_basis='reported_calendar_snapshot_includes_future_dates_not_revalidated',
                        state_counts={'complete': 1}, complete=1, rows=len(dates),
                        first_data_date=min(dates), last_data_date=max(dates),
                        receipt_observed_at_utc=receipt.get('observed_at_utc'))
    elif dataset == free.MASTER_DATASET:
        paths = sorted((root / 'receipts' / dataset).glob('*.json'))
        receipt = _json(paths[-1]) if paths else {}
        if receipt.get('dataset') == dataset:
            state = receipt.get('status')
            item.update(observation_basis='latest_master_receipt_not_revalidated',
                        state_counts={state: 1} if isinstance(state, str) else None,
                        complete=1 if state == 'complete' else None, rows=receipt.get('rows'),
                        first_data_date=receipt.get('source_first_date'),
                        last_data_date=receipt.get('source_last_date'),
                        receipt_observed_at_utc=receipt.get('fetched_at_utc'),
                        receipt_path=str(paths[-1]))
    item['status_observed_at_utc'] = status.get('observed_at_utc')
    return item


def _lower_bound(row: dict[str, Any]) -> tuple[int | None, str]:
    """Only a conditional floor for observed pending work; never a global ETA."""
    shape, pending = row['query_shape'], row['pending']
    if shape == 'derived_no_api':
        return 0, 'local_derivation_zero_direct_calls_parent_acquisition_excluded'
    if shape in {'per_id_date_range', 'per_product_history'}:
        return None, 'historical_identifier_universe_not_proven_complete'
    if pending is None:
        return None, 'pending_work_unknown_missing_or_unseeded_local_evidence'
    if shape.startswith('unverified'):
        return None, 'whole_market_query_contract_unverified'
    if pending == 0:
        return 0, 'no_pending_tasks_observed_not_a_completeness_or_refresh_claim'
    if shape in {'whole_market_date_range', 'global_date_range'}:
        return 1, 'conditional_at_least_one_call_pending_holes_resources_cooldowns_can_require_more'
    if shape in {'snapshot', 'calendar_snapshot'}:
        return 1, 'one_snapshot_for_observed_pending_work_refreshes_excluded'
    return None, 'query_direction_or_historical_contract_unproven_do_not_equate_tasks_and_requests'


def build_inventory(root: Path, now: datetime | None = None) -> dict[str, Any]:
    now = (now or datetime.now(UTC)).astimezone(UTC)
    rows = registry()
    statuses = {owner: _json(root / owner / 'status.json') for owner in ('sponsor', 'complement')}
    statuses['free'] = _json(root / 'status.json')
    observations, queue_states = {}, {}
    for owner in ('sponsor', 'complement'):
        observations[owner], queue_states[owner] = _queue_observations(root / owner / 'queue.sqlite3', now)
    delegated = set(statuses['complement'].get('delegated_to_sponsor', []))
    for dataset, row in rows.items():
        row['owner_observations'] = {}
        for owner in row['owners']:
            if owner in observations:
                item = dict(observations[owner].get(dataset, _unknown(queue_states[owner]['state']
                                    if queue_states[owner]['state'] != 'observed' else 'dataset_not_seeded')))
                item['status_observed_at_utc'] = statuses[owner].get('observed_at_utc')
                item['delegated_to_sponsor'] = owner == 'complement' and dataset in delegated
                item['delegated_to_complement_product_history'] = (
                    owner == 'sponsor' and dataset in PRODUCT_HISTORY_STARTS)
            elif owner == 'free':
                item = _free_observation(root, dataset, statuses['free'])
            else:
                item = _unknown('explicitly_disabled' if row['disabled'] else 'not_scheduled')
            row['owner_observations'][owner] = item
        primary = row['owner_observations'][row['primary_owner']]
        row.update({key: primary.get(key) for key in COUNT_STATES})
        row.update({key: primary.get(key) for key in ('pending_due', 'failed_or_blocked', 'excluded_deprecated')})
        row['observation_basis'] = primary['observation_basis']
        row['observed_first_date'] = primary.get('first_data_date')
        row['observed_last_date'] = primary.get('last_data_date')
        row['observed_rows'] = primary.get('rows')
        row['known_data_id_count'] = primary.get('known_data_id_count')
        row['count_basis'] = 'primary_owner_only_alias_counts_not_added'
        row['remaining_lower_bound_requests'], row['lowerbound_basis'] = _lower_bound(row)
        row['lower_bound_scope'] = 'conditional_observed_pending_acquisition_only_excludes_refresh_retry_inflight'
        if row['query_shape'] in {'whole_market_day', 'whole_market_period_anchor'} and 'complement' in row['owners']:
            row['conditional_per_id_alternative'] = {
                'formula': 'min(required_missing_dates_or_periods, sum(per_id_required_range_requests)+universe_and_overlap_validation_calls)',
                'requires': 'verified_complete_historical_universe_and_exact_coverage_reconciliation',
                'historical_universe_verified_complete': False,
                'is_proven_minimum': False,
            }
    alias_count = len(sponsor.SOURCES) + len(complement.ALL_DATASETS) + len(free.SESSION_DATASETS) + 2 + len(sponsor.UNSCHEDULED)
    migrated_alias_count = len(set(PRODUCT_HISTORY_STARTS) & set(complement.ALL_DATASETS))
    return {
        'schema_version': 1, 'observed_at_utc': now.isoformat(), 'api_requests': 0,
        'production_queue_writes': 0, 'parquet_scans': 0,
        'global_minimum_requests': None,
        'global_minimum_basis': 'not_proven_contracts_universes_overlap_and_refresh_work_differ',
        'coverage': {'unique_dataset_count': len(rows), 'current_catalog_alias_count': alias_count,
                     'legacy_catalog_alias_count': alias_count - migrated_alias_count,
                     'legacy_catalog_basis': 'before_two_settlement_complement_owner_aliases_were_added',
                     'product_history_migration_alias_count': migrated_alias_count,
                     'legacy_aliases_are_unique_datasets': False, 'explicit_disabled_news_rows': 1,
                     'runtime_range_contract_count': len(batching.RANGE_CONTRACTS),
                     'scope': 'configured_registry_union_plus_unscheduled_and_disabled_news_not_all_provider_products'},
        'queue_observations': queue_states,
        'status_observations': {owner: {'state': value.get('state'), 'observed_at_utc': value.get('observed_at_utc')}
                                for owner, value in statuses.items()},
        'candidate_universe': statuses['complement'].get('candidate_universe'),
        'evidence_limits': ['Queue/status claims were not revalidated against Parquet bytes.',
                            'Cross-owner observations are not one atomic snapshot.',
                            'Complete and observed_empty states do not prove source completeness or PIT.',
                            'Partition counts are not HTTP counts after batching or delegation.'],
        'datasets': [rows[dataset] for dataset in sorted(rows)],
    }


def write_inventory(report: dict[str, Any], output_dir: Path) -> dict[str, str]:
    stamp = datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')
    base = output_dir / f'finmind_query_ranges_{stamp}'
    fields = ('dataset', 'owners', 'primary_owner', 'disabled', 'query_shape', 'configured_first_date',
              'observed_first_date', 'observed_last_date', 'observed_rows', 'known_data_id_count',
              *COUNT_STATES, 'max_span',
              'pending_due', 'failed_or_blocked', 'excluded_deprecated', 'observation_basis',
              'remaining_lower_bound_requests', 'lowerbound_basis', 'lower_bound_scope',
              'count_basis', 'owner_contracts', 'owner_observations', 'source_urls')
    buffer = io.StringIO(newline='')
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    for row in report['datasets']:
        writer.writerow({key: (json.dumps(row.get(key), ensure_ascii=False, sort_keys=True)
                               if isinstance(row.get(key), (dict, list)) else row.get(key)) for key in fields})
    atomic_write_text(base.with_suffix('.csv'), buffer.getvalue(), durable=True)
    atomic_write_json(base.with_suffix('.json'), report)
    return {'json': str(base.with_suffix('.json')), 'csv': str(base.with_suffix('.csv'))}


def main(argv: list[str] | None = None) -> int:
    repo = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=repo / 'data_finmind')
    parser.add_argument('--output-dir', type=Path, default=repo / 'artifacts' / 'data_quality')
    args = parser.parse_args(argv)
    report = build_inventory(args.root)
    paths = write_inventory(report, args.output_dir)
    print(json.dumps({'reports': paths, **report['coverage'], 'api_requests': 0,
                      'global_minimum_requests': None}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
