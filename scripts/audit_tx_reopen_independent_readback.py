#!/usr/bin/env python3
"""Independently read back a retained-TX tick audit and official block arithmetic."""
from __future__ import annotations

import argparse
from collections import defaultdict
import csv
from datetime import UTC, datetime
import hashlib
import io
import json
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def readback(audit_path: Path, projection: Path, block_manifest: Path,
             samples: list[str]) -> dict:
    audit = json.loads(audit_path.read_text())
    if audit.get('contract') != 'tx_retained_finmind_contract_day_clock_audit_v2':
        raise ValueError('expected the official-block-corrected audit')
    for path in (projection, block_manifest):
        if audit['inputs_sha256'].get(str(path)) != digest(path):
            raise ValueError('pinned audit input changed')
    source = json.loads(block_manifest.read_text())
    if source.get('source') != 'taifex_official_block_volume_inventory_v1':
        raise ValueError('expected dated official block inventory')
    blocks = defaultdict(int)
    for item in source['sources']:
        raw = Path(item['path']).read_bytes()
        if (item['url'] != 'https://www.taifex.com.tw/cht/3/dlProductOrderDown'
                or hashlib.sha256(raw).hexdigest() != item['sha256']):
            raise ValueError('official block original changed')
        for row in csv.DictReader(io.StringIO(raw.decode('cp950'))):
            if row['交易時段'].strip() != '一般' or row['契約'].strip() != 'TX':
                continue
            day = row['交易日期'].strip().replace('/', '-')
            if not item['source_first_date'] <= day <= item['source_last_date']:
                raise ValueError('official block row is outside its pinned interval')
            blocks[(day, row['到期月份(週別)'].strip())] += int(row['成交數量'])
    daily = pq.read_table(projection).to_pylist()
    lookup = {(str(row['date']), row['contract']): row for row in daily}
    if len(lookup) != len(daily):
        raise ValueError('duplicate official physical contract-day')
    rows = audit['rows']
    if len(rows) != audit['counts']['target_contract_days']:
        raise ValueError('audit count mismatch')
    for row in rows:
        key = (row['date'], row['contract_month'])
        daily_row = lookup[key]
        block = blocks[key]
        if (block != row['official_block_volume']
                or daily_row['outright_volume'] - block != row['official_regular_outright_volume']
                or (row['observed_matched_contracts'] == daily_row['outright_volume'] - block)
                != row['volume_matches_official_outright']):
            raise ValueError(f'block/regular-book arithmetic mismatch {key}')
    by_day = {r['date']: r for r in rows}
    objects = {r['date']: r for r in audit['source_objects']}
    if len(by_day) != len(rows) or not set(samples) <= set(by_day):
        raise ValueError('missing or duplicate sample session')
    checked = []
    for day in samples:
        row, obj = by_day[day], objects[day]
        path, receipt = Path(obj['path']), Path(obj['receipt_path'])
        if digest(path) != obj['sha256'] or digest(receipt) != obj['receipt_sha256']:
            raise ValueError('sample original or receipt changed')
        table = pq.read_table(path, columns=['date', 'futures_id', 'contract_date', 'price', 'volume'])
        mask = pc.and_(pc.equal(table['futures_id'], 'TX'),
                       pc.equal(table['contract_date'], row['contract_month']))
        table = table.filter(mask)
        start, end = [day + ' ' + clock for clock in row['session_envelope']]
        records = [(stamp, index, price, volume)
                   for index, (stamp, price, volume) in enumerate(zip(
                       table['date'].to_pylist(), table['price'].to_pylist(), table['volume'].to_pylist()))
                   if start <= stamp <= end]
        records.sort(key=lambda r: (r[0], r[1]))
        if not records or any(vol <= 0 or vol % 2 for _, _, _, vol in records):
            raise ValueError('sample has invalid or no regular prints')
        if (len(records) != row['observed_source_rows']
                or sum(r[3] for r in records) // 2 != row['observed_matched_contracts']):
            raise ValueError('sample native quantity differs')
        for probe in row['entry_probes']:
            later = [r for r in records if r[0] > day + ' ' + probe['after']]
            if not later:
                if probe['observed']:
                    raise ValueError('sample missing advertised probe')
                continue
            earliest = later[0][0]
            same = [r for r in later if r[0] == earliest]
            if (probe['timestamp'] != earliest or probe['source_rows_at_second'] != len(same)
                    or probe['distinct_prices'] != sorted({float(r[2]) for r in same})
                    or probe['matched_contracts_at_second'] != sum(r[3] for r in same) // 2):
                raise ValueError('sample timestamp probe differs')
        checked.append({'date': day, 'contract_month': row['contract_month'],
                        'source_rows': len(records), 'raw_sha256': obj['sha256']})
    return {'contract': 'tx_reopen_independent_block_and_sample_tick_readback_v1',
            'observed_at_utc': datetime.now(UTC).isoformat(),
            'audit_sha256': digest(audit_path), 'daily_projection_sha256': digest(projection),
            'block_manifest_sha256': digest(block_manifest),
            'all_contract_days_block_and_quantity_arithmetic_checked': len(rows),
            'sample_native_pyarrow_readback': checked,
            'historical_source_delivery_clock_verified': False, 'broker_fill_verified': False,
            'benchmark_admitted': False, 'training_admitted': False,
            'interpretation': 'Independent reader and arithmetic, same retained source objects; not a second data vendor.'}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--audit', type=Path, required=True)
    p.add_argument('--projection', type=Path, required=True)
    p.add_argument('--block-manifest', type=Path, required=True)
    p.add_argument('--sample', action='append', required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    if args.output.exists():
        raise FileExistsError('choose a new output file')
    result = readback(args.audit, args.projection, args.block_manifest, args.sample)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
