#!/usr/bin/env python3
"""Independently read back pre-night reference candidates with PyArrow.

Raw times are checked without the original Polars reducers, including all
positive prints for the product/month on both calendar days. This is not a
second provider, a historical delivery certificate, or a training admission.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
import gzip
import hashlib
import json
from pathlib import Path
import struct

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(4 * 1024**2), b''):
            value.update(block)
    return value.hexdigest()


def raw_extrema(path: Path, requested: set[tuple[str, str]]) -> dict:
    result = {}
    parquet = pq.ParquetFile(path)
    # Discard unrelated products/months in Arrow before making Python objects.
    # Membership of the two fields is only a conservative prefilter: the
    # original tuple check below still decides which physical contracts count.
    # Fall back to the original row loop for unusual schemas/keys so the
    # optimisation cannot change matching or financial reduction semantics.
    fields = parquet.schema_arrow
    string_key_schema = all(
        (pa.types.is_string(fields.field(name).type)
         or pa.types.is_large_string(fields.field(name).type))
        for name in ('futures_id', 'contract_date')
    )
    string_keys = all(isinstance(product, str) and isinstance(contract, str)
                      for product, contract in requested)
    if not requested:
        return result
    if string_key_schema and string_keys:
        products = pa.array(sorted({product for product, _ in requested}),
                            type=fields.field('futures_id').type)
        contracts = pa.array(sorted({contract for _, contract in requested}),
                             type=fields.field('contract_date').type)
    else:
        products = contracts = None
    for batch in parquet.iter_batches(columns=['date', 'futures_id', 'contract_date', 'price', 'volume']):
        if products is not None:
            selected = pc.and_(pc.is_in(batch.column('futures_id'), value_set=products),
                               pc.is_in(batch.column('contract_date'), value_set=contracts))
            batch = batch.filter(selected)
        columns = batch.to_pydict()
        for time, product, contract, price, volume in zip(
                columns['date'], columns['futures_id'], columns['contract_date'],
                columns['price'], columns['volume'], strict=True):
            key = product, contract
            if key not in requested or price is None or volume is None or price <= 0 or volume <= 0:
                continue
            if not isinstance(time, str) or len(time) != 19:
                raise ValueError('raw event timestamp is not an exact second')
            encoded = struct.pack('!f', price)
            row = result.setdefault(key, {'first': time, 'last': time, 'last_prices': set()})
            row['first'] = min(row['first'], time)
            if time > row['last']:
                row['last'], row['last_prices'] = time, set()
            if time == row['last']:
                row['last_prices'].add(encoded)
    return result


def check_candidates(candidates, coverage, final_coverage, originals, classification=None, *, weekend=False,
                     zero_afterhours=None, early_holiday_sessions=None):
    failures = []
    for row in candidates:
        day, previous, physical = row['date'], row['_previous_date'], row['physical_contract']
        key = row['product'], row['contract']
        reasons = []
        if zero_afterhours is not None:
            date_ok = (day.isoformat() >= '2017-05-15'
                       and (day, *key) in zero_afterhours
                       and zero_afterhours[(day, *key)] == row.get('night_zero_source_sha256'))
        elif classification is None:
            date_ok = day.isoformat() < '2017-05-15'
        else:
            current_class = classification.get((day, physical))
            previous_class = classification.get((previous, physical))
            cutoffs = {'stock_future': '2024-01-22', 'etf_future': '2022-06-27'}
            date_ok = (current_class == previous_class == row.get('asset_class')
                       and current_class in cutoffs and '2017-05-15' <= day.isoformat() < cutoffs[current_class])
        if early_holiday_sessions is not None:
            previous_ok = early_holiday_sessions.get(day) == previous
        else:
            previous_ok = (day.weekday() == 0 and day.isoformat() >= '2019-01-07'
                           and previous == day - timedelta(days=3)) if weekend else previous == day - timedelta(days=1)
        if (not date_ok or not previous_ok
                or row['reference_trade_ts'].date() != previous
                or row['reference_available_by'] > row['decision_ts']):
            reasons.append('date_or_availability')
        for check_day in (previous, day):
            for book in (coverage, final_coverage):
                evidence = book.get((check_day, physical))
                if (evidence is None or evidence['verified'] is not True or evidence['block_volume'] != 0
                        or evidence['source_file_sha256'] != originals[check_day]['sha256']):
                    reasons.append('coverage_or_source')
                elif evidence['product'] != row['product'] or evidence['contract'] != row['contract']:
                    reasons.append('physical_identity')
        prior = originals[previous]['rows'].get(key)
        if (not prior or prior['last'] != row['reference_trade_ts'].strftime('%Y-%m-%d %H:%M:%S')
                or len(prior['last_prices']) != 1
                or struct.pack('!f', row['reference_quote']) not in prior['last_prices']
                or originals[previous]['sha256'] != row['reference_source_sha256']):
            reasons.append('prior_last_print')
        today = originals[day]['rows'].get(key)
        if today:
            first = datetime.strptime(today['first'], '%Y-%m-%d %H:%M:%S')
            first_available = first.replace(second=0, microsecond=0) + timedelta(minutes=1)
            if first_available <= row['decision_ts']:
                reasons.append('current_trade_available')
        if reasons:
            failures.append({'date': day.isoformat(), 'physical_contract': physical,
                             'decision_ts': row['decision_ts'].isoformat(), 'reasons': sorted(set(reasons))})
    return failures


def build(manifest_path: Path, report: Path) -> dict:
    if report.exists() or report.is_symlink():
        raise ValueError('independent readback report already exists')
    manifest_path = manifest_path.resolve()
    manifest_sha = digest(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    contract = manifest.get('contract')
    early_holidays = contract in ('tw_futures_early_holiday_prenight_candidate_v1',
                                  'tw_futures_early_holiday_prenight_candidate_v2')
    weekend = contract in ('tw_futures_equity_weekend_candidate_v1',
                           'tw_futures_zero_afterhours_weekend_candidate_v1')
    zero_night = contract in ('tw_futures_zero_afterhours_adjacent_candidate_v1',
                              'tw_futures_zero_afterhours_weekend_candidate_v1')
    extended = contract in ('tw_futures_equity_day_only_adjacent_candidate_v1',
                            'tw_futures_equity_weekend_candidate_v1')
    if (contract not in ('tw_futures_prenight_adjacent_day_candidate_v1',
                         'tw_futures_early_holiday_prenight_candidate_v1',
                         'tw_futures_early_holiday_prenight_candidate_v2',
                         'tw_futures_equity_day_only_adjacent_candidate_v1',
                         'tw_futures_equity_weekend_candidate_v1',
                         'tw_futures_zero_afterhours_adjacent_candidate_v1',
                         'tw_futures_zero_afterhours_weekend_candidate_v1')
            or manifest.get('training_ready') is not False):
        raise ValueError('unsupported or unexpectedly admitted candidate')
    payload = Path(manifest['data_path'])
    if digest(payload) != manifest['data_sha256']:
        raise ValueError('candidate payload differs from its manifest')
    pins = manifest['sources']
    extra_pins = [pins['daily_classification'], pins['equity_notice']] if extended else []
    if weekend:
        extra_pins.append(pins['weekend_notice'])
    if zero_night:
        extra_pins.extend(pins['zero_afterhours_inputs'])
    if early_holidays:
        extra_pins.extend(pins['early_holiday_originals'])
        extra_pins.append(pins['early_holiday_implementation'])
    for pin in (pins['checkpoint_manifest'], pins['coverage'], pins['final_coverage'],
                pins['exchange_notice'], *extra_pins):
        if digest(Path(pin['path'])) != pin['sha256']:
            raise ValueError('candidate source pin differs')
    if extended:
        if pins['equity_notice']['sha256'] != 'e32b3482fee476e7d26585e59392d716cc27de49bceaeca80a41555adbcc1d06':
            raise ValueError('equity notice is not the reviewed official publication')
    if extended or zero_night or early_holidays:
        if (len(pins['references']) != 2
                or pins['references'][0]['pair_identity'] != pins['references'][1]['pair_identity']):
            raise ValueError('opening references do not share a paired identity')
        for ref in pins['references']:
            for key in ('data', 'manifest', 'entries'):
                if digest(Path(ref[key]['path'])) != ref[key]['sha256']:
                    raise ValueError('paired reference source pin differs')
            entry = json.loads(Path(ref['entries']['path']).read_text())
            doc = json.loads(Path(ref['manifest']['path']).read_text())
            if (entry['source_daily_sha256'] != doc['source_daily_sha256']
                    or doc['data_sha256'] != ref['data']['sha256']
                    or doc['pair_identity'] != entry['pair_identity']
                    or doc['pair_identity'] != ref['pair_identity']):
                raise ValueError('reference is not the paired financial release')
            if extended and doc['source_daily_sha256'] != pins['daily_classification']['sha256']:
                raise ValueError('classification is not the paired financial release')
    if weekend and pins['weekend_notice']['sha256'] != '0de9f1708ba06e4d1a8ce81ce7850c30686c5147d4fb899642613f3dec795d9f':
        raise ValueError('weekend notice is not the reviewed official publication')
    holiday_sessions = None
    if early_holidays:
        reviewed = {
            ('2010-12-16', '2011-01-28', '2011-02-08',
             '6d22d15520a7456e14845f7e493b2c19026c8828bcc58ce0897657894bd1c2fa'),
            ('2011-12-02', '2012-01-18', '2012-01-30',
             'df9937f008ab3fc5037d7b68cde9a8b1d89e7fd7108d045205896a11f320266b'),
        }
        if contract == 'tw_futures_early_holiday_prenight_candidate_v2':
            reviewed.update({
                ('2013-01-25', '2013-02-06', '2013-02-18',
                 'f8ecdfaecd66b510772caaa4a20be313239dbc13696be2f22b25cae57da7e8c0'),
                ('2014-01-09', '2014-01-27', '2014-02-05',
                 'a0da401cfd53157b518dc3872d64ee9e5cc3e103d106498f5c27198a7baecdb4'),
                ('2015-01-14', '2015-02-13', '2015-02-24',
                 '020cd365c8b1c03e701dccf7efe1b921494aff20d5d38134da9e9e3ab33c7e0d'),
                ('2016-01-08', '2016-02-03', '2016-02-15',
                 '0ee9702e367ea0cd8f9f9f1a5d0b8b1603c127a837a3eaf01d430c6285b52f51'),
                ('2017-01-03', '2017-01-24', '2017-02-02',
                 'cfcaedb2ec9204ed4e5f2bb18d67e06b1c9722215e7620ea4dac7922a48843b8'),
            })
        originals = pins['early_holiday_originals']
        observed = {(item['published_date'], item['last_session'], item['reopening_session'],
                     item['content_sha256']) for item in originals}
        if len(originals) != len(reviewed) or observed != reviewed:
            raise ValueError('early holiday notices differ from the independently reviewed originals')
        for item in originals:
            if hashlib.sha256(gzip.decompress(Path(item['path']).read_bytes())).hexdigest() != item['content_sha256']:
                raise ValueError('early holiday original content differs')
        holiday_sessions = {date.fromisoformat(reopening): date.fromisoformat(previous)
                            for _, previous, reopening, _ in reviewed}
    zero_afterhours = None
    if zero_night:
        input_pins = pins['zero_afterhours_inputs']
        zero_doc = json.loads(Path(input_pins[0]['path']).read_text())
        if (zero_doc.get('contract') != 'taifex_explicit_zero_afterhours_source_evidence_v1'
                or zero_doc.get('training_ready') is not False):
            raise ValueError('unsupported explicit zero-night source')
        expected = {(zero_doc['data_path'], zero_doc['data_sha256']),
                    (zero_doc['source_manifest'], zero_doc['source_manifest_sha256']),
                    (zero_doc['source_data'], zero_doc['source_data_sha256']),
                    *((item['path'], item['sha256']) for item in zero_doc['originals'])}
        if {(pin['path'], pin['sha256']) for pin in input_pins[1:]} != expected:
            raise ValueError('explicit zero-night pins differ from their source manifest')
        rows = pq.read_table(zero_doc['data_path'], columns=['date', 'product', 'contract', 'source_sha256']).to_pylist()
        zero_afterhours = {(r['date'], r['product'], r['contract']): r['source_sha256'] for r in rows}
        if len(rows) != len(zero_afterhours) or len(rows) != zero_doc['rows']:
            raise ValueError('explicit zero-night source keys or count differ')
    checkpoint = json.loads(Path(pins['checkpoint_manifest']['path']).read_text())
    inventory = {item['date']: item for item in checkpoint['identity']['objects']}
    candidates = pq.read_table(payload).to_pylist()
    if len(candidates) != manifest['candidate_rows']:
        raise ValueError('candidate row count differs')
    requested = defaultdict(set)
    for row in candidates:
        for day in (row['date'], row['_previous_date']):
            requested[day].add((row['product'], row['contract']))
    originals = {}
    for index, (day, products) in enumerate(sorted(requested.items()), 1):
        obj = inventory[day.isoformat()]
        if obj.get('source_kind'):
            raise ValueError('independent readback only accepts pinned whole-market objects')
        raw_path, receipt = Path(obj['path']), Path(obj['receipt_path'])
        if digest(raw_path) != obj['sha256'] or digest(receipt) != obj['receipt_sha256']:
            raise ValueError(f'raw source or receipt changed for {day}')
        originals[day] = {'sha256': obj['sha256'], 'rows': raw_extrema(raw_path, products)}
        if digest(raw_path) != obj['sha256'] or digest(receipt) != obj['receipt_sha256']:
            raise ValueError(f'raw source or receipt changed during readback for {day}')
        if index % 100 == 0 or index == len(requested):
            print(json.dumps({'stage': 'independent_raw_readback', 'dates_checked': index,
                              'dates_total': len(requested)}), flush=True)
    days = sorted(requested)
    columns = ['date', 'physical_contract', 'product', 'contract', 'verified', 'block_volume', 'source_file_sha256']
    def read_coverage(pin):
        table = pq.read_table(pin['path'], columns=columns, filters=[('date', 'in', days)])
        rows = table.to_pylist()
        result = {(row['date'], row['physical_contract']): row for row in rows}
        if len(rows) != len(result):
            raise ValueError('duplicate physical coverage rows')
        return result
    classification = None
    if extended:
        daily = pq.read_table(pins['daily_classification']['path'],
                              columns=['date', 'physical_contract', 'asset_class'],
                              filters=[('date', 'in', days)]).to_pylist()
        classification = {(row['date'], row['physical_contract']): row['asset_class'] for row in daily}
        if len(daily) != len(classification):
            raise ValueError('duplicate dated physical classifications')
    failures = check_candidates(candidates, read_coverage(pins['coverage']),
                                read_coverage(pins['final_coverage']), originals, classification, weekend=weekend,
                                zero_afterhours=zero_afterhours, early_holiday_sessions=holiday_sessions)
    if digest(manifest_path) != manifest_sha or digest(payload) != manifest['data_sha256']:
        raise ValueError('candidate changed during independent readback')
    for pin in (pins['checkpoint_manifest'], pins['coverage'], pins['final_coverage'],
                pins['exchange_notice'], *extra_pins):
        if digest(Path(pin['path'])) != pin['sha256']:
            raise ValueError('candidate source changed during independent readback')
    result = {'contract': 'tw_futures_prenight_independent_readback_v1',
              'created_at_utc': datetime.now(timezone.utc).isoformat(),
              'candidate_manifest_path': str(manifest_path), 'candidate_manifest_sha256': manifest_sha,
              'candidate_sha256': digest(payload), 'implementation_sha256': digest(Path(__file__)),
              'candidate_rows': len(candidates), 'raw_dates': len(requested),
              'failure_count': len(failures), 'failures': failures[:100],
              'accepted': not failures, 'training_ready': False,
              'limitations': ['Same vendor originals, not a second provider.',
                              'Readback does not prove historical receive time or remove other training gates.']}
    atomic_write_json(report, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    result = build(args.manifest, args.report)
    print(json.dumps({key: result[key] for key in ('candidate_rows', 'raw_dates', 'failure_count', 'accepted')}), flush=True)
    if not result['accepted']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
