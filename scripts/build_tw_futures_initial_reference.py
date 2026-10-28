#!/usr/bin/env python3
"""Bind retained TEJ exchange references to verified new physical generations.

This source builder performs no provider requests and does not modify the
download queue, execution observations or an accepted financial release.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, UTC
import json
from pathlib import Path
import shutil
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import polars as pl
from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_futures_execution_terms import bind_product_specifications
from stockagent.data.tw_futures_initial_reference import (
    CONTRACT, COLLECTION_CONTRACT, LISTING_CONTRACT, LISTING_REASON, REISSUE_REASON,
    EVENT_BIRTH_CONTRACT, EVENT_COLLECTION_CONTRACT, EVENT_BIRTH_REASONS,
    KEYS, NATIVE_FIELDS, SCHEMA, STANDARD_SHARE_UNITS,
    native_reissue_reference, native_listing_reference, native_event_birth_reference,
)
from stockagent.data.tw_futures_margin_release import read_bound_output
from stockagent.data_sync.artifact_dedup import ImmutableSourceBatch, link_immutable_source

TABLE_ID = '117392c87504f94e71a04d01'


def build(tej: Path, financial: Path, source: Path, definition_root: Path, output: Path,
          *, include_new_listings: bool = False, include_event_births: bool = False) -> dict:
    if include_event_births and not include_new_listings:
        raise ValueError('Event births require the explicit listing collection scope')
    tej, financial, source, definition_root, output = [
        p.resolve() for p in (tej, financial, source, definition_root, output)]
    if output.exists():
        raise FileExistsError('Preserve the previous source candidate and use a new output')
    if shutil.disk_usage(output.parent).free < 8*1024**3:
        raise RuntimeError('Insufficient source-materialization reserve')
    pins = {}

    def pin(path: Path, expected: str | None = None):
        if path.is_symlink() or not path.is_file():
            raise ValueError('A source must be an ordinary retained file')
        digest = sha256_file(path)
        if expected is not None and expected != digest:
            raise ValueError(f'Source SHA mismatch: {path}')
        pins[str(path)] = digest
        return digest

    checkpoint = json.loads((financial/'accounting_checkpoint.json').read_text())
    pin(financial/'accounting_checkpoint.json')
    pin(source/'source_manifest.json', checkpoint['inputs']['source_manifest_sha256'])
    for name in ('frame.parquet', 'contract_day_blockers.parquet'):
        pin(financial/name, checkpoint['outputs'][name])
    first = pl.scan_parquet(financial/'frame.parquet').select(
        *KEYS, 'product', 'contract', 'previous_symbol_date').filter(pl.col('previous_symbol_date').is_null())
    flags = pl.scan_parquet(financial/'contract_day_blockers.parquet').select(
        *KEYS, 'is_warmup', 'missing_product_specification')
    first = first.join(flags, on=KEYS, validate='1:1').filter(
        pl.col('is_warmup') & ~pl.col('missing_product_specification')).collect()
    spec_path = source/'specifications/specifications.parquet'
    specifications, _ = read_bound_output(spec_path)
    pin(spec_path)
    pin(spec_path.with_name('manifest.json'))
    specs = bind_product_specifications(first, specifications)
    first = first.join(specs.select('date', 'product', 'spec_contract_multiplier', 'spec_opening_time'),
        on=['date', 'product'], validate='m:1').filter(
            pl.col('spec_contract_multiplier').is_in(STANDARD_SHARE_UNITS))
    first_rows = {(r['date'], r['product'], r['contract']): r for r in first.iter_rows(named=True)}
    if len(first_rows) != first.height:
        raise ValueError('Ambiguous new generation at a native product/month boundary')

    definition_path = definition_root/'native_adjustment_reference_definition.json'
    definition = json.loads(definition_path.read_text())
    dm = json.loads((definition_root/'manifest.json').read_text())
    if (definition.get('dataset') != 'TWN/AFUTRSTK' or definition.get('native_reference_field') != 'ex_p'
            or definition.get('historical_value_definition_verified') is not True
            or definition.get('actual_historical_delivery_clock_verified') is not False
            or definition.get('field', [None]*6)[-1] != '為交易所公佈，調整生效日當天的開盤參考價。'):
        raise ValueError('Exact native exchange-opening-reference definition required')
    pin(definition_root/'manifest.json')
    definition_members = []
    for name, digest in dm['files'].items():
        p = definition_root/name
        pin(p, digest)
        definition_members.append(p)
    import gzip
    for item in dm['sources']:
        p = definition_root/item['path']
        pin(p, item['raw_sha256'])
        import hashlib
        if hashlib.sha256(gzip.decompress(p.read_bytes())).hexdigest() != item['content_sha256']:
            raise ValueError('Archived primary definition content SHA mismatch')
        definition_members.append(p)
    if definition['documentation_content_sha256'] not in {x['content_sha256'] for x in dm['sources']}:
        raise ValueError('Opening-reference definition does not bind the archived primary page')

    with sqlite3.connect((tej/'queue.sqlite3').as_uri()+'?mode=ro', uri=True) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        table = dict(db.execute('SELECT * FROM tables WHERE table_id=?', (TABLE_ID,)).fetchone())
        if json.loads(table['fields_json']) != NATIVE_FIELDS:
            raise ValueError('Native adjustment field identity changed')
        tasks = [dict(r) for r in db.execute('SELECT task_id,receipt_path,actual_rows FROM tasks '
            "WHERE table_id=? AND state='complete' AND actual_rows>0 ORDER BY task_id", (TABLE_ID,))]
    rows, used, scanned, counts = [], {}, [], Counter()
    for index, task in enumerate(tasks):
        rp = tej/task['receipt_path']
        receipt = json.loads(rp.read_text())
        if (receipt['task_id'] != task['task_id'] or receipt['table_id'] != TABLE_ID
                or receipt.get('source_values_retained') is not True or receipt.get('training_auto_injected') is not False):
            raise ValueError('Native receipt lacks the exact retained source owner')
        raw, pq = tej/receipt['raw_path'], tej/receipt['parquet_path']
        lineage = dict(source_task_id=task['task_id'], source_raw_sha256=pin(raw, receipt['raw_sha256']),
            source_parquet_sha256=pin(pq, receipt['parquet_sha256']), source_receipt_sha256=pin(rp))
        scanned.append(dict(**lineage, raw_path=str(raw), parquet_path=str(pq), receipt_path=str(rp)))
        data = pl.read_parquet(pq)
        if data.height != task['actual_rows']:
            raise ValueError('Native exported-row count differs from the canonical queue')
        counts['native_rows'] += data.height
        for native in data.iter_rows(named=True):
            normalized = native_reissue_reference(native)
            reference_contract = CONTRACT
            if normalized is None and include_new_listings:
                normalized = native_listing_reference(native)
                reference_contract = LISTING_CONTRACT
            if normalized is None and include_event_births:
                normalized = native_event_birth_reference(native)
                reference_contract = EVENT_BIRTH_CONTRACT
            if normalized is None:
                counts['other_event_or_absent_reference'] += 1
                continue
            birth = first_rows.get(tuple(normalized[c] for c in ('date', 'product', 'contract')))
            if birth is None:
                counts['not_a_verified_empty_standard_generation'] += 1
                continue
            if normalized['reference_contract_multiplier'] != birth['spec_contract_multiplier']:
                raise ValueError('Native standard units differ from their dated product specification')
            normalized.update(physical_contract=birth['physical_contract'], reference_contract=reference_contract,
                market_reference_asof=f"{birth['date']}T{birth['spec_opening_time']}+08:00",
                definition_content_sha256=definition['documentation_content_sha256'], **lineage)
            rows.append(normalized)
            used[task['task_id']] = (raw, pq, rp)
        if (index+1) % 1000 == 0:
            print(json.dumps(dict(stage='retained_exports_verified', exports=index+1, candidates=len(rows))), flush=True)
    result = pl.DataFrame(rows, schema=SCHEMA)
    values = [c for c in SCHEMA if not c.startswith('source_')]
    unique = result.select(values).unique()
    if unique.select(KEYS).is_duplicated().any():
        raise ValueError('Conflicting native initial references; retain both and review')
    result = result.sort('source_task_id').unique(subset=KEYS, keep='first').sort(KEYS)
    if not result.height:
        raise ValueError('No source-bound new-generation reference found')
    output.mkdir()
    atomic_write_parquet(output/'initial_references.parquet', result)
    atomic_write_parquet(output/'completed_source_inventory.parquet', pl.DataFrame(scanned))
    originals = {}
    # The immutable object owner copies producer bytes before linking evidence;
    # it never hard-links mutable downloader files into an accepted source.
    with ImmutableSourceBatch() as batch:
        for paths in used.values():
            for p in paths:
                digest = pins[str(p)]
                relative = 'sources/native/'+digest+p.suffix
                if relative not in originals:
                    link_immutable_source(p, output/relative, output.parent/'.rule-source-objects', digest,
                        batch=batch)
                    originals[relative] = dict(path=relative, sha256=digest)
        for p in [definition_root/'manifest.json', *definition_members]:
            relative = 'sources/definition/'+str(p.relative_to(definition_root))
            digest = pins[str(p)]
            link_immutable_source(p, output/relative, output.parent/'.rule-source-objects', digest,
                batch=batch)
            originals[relative] = dict(path=relative, sha256=digest)
    for name, digest in pins.items():
        if sha256_file(Path(name)) != digest:
            raise ValueError('Source changed during native-reference normalization')
    event_contracts = {CONTRACT: REISSUE_REASON, LISTING_CONTRACT: LISTING_REASON}
    if include_event_births:
        event_contracts[EVENT_BIRTH_CONTRACT] = list(EVENT_BIRTH_REASONS)
    manifest = dict(contract=(EVENT_COLLECTION_CONTRACT if include_event_births
                             else COLLECTION_CONTRACT if include_new_listings else CONTRACT),
        **(dict(reference_event_contracts=event_contracts) if include_new_listings else {}),
        reference_value_definition_verified=True,
        reference_units_bound_to_dated_specifications=True,
        snapshot_transport_clock_backdated=False, reference_is_execution_price=False,
        market_asof_contract='exchange_defined_regular_opening_reference_v1',
        actual_historical_tej_delivery_clock_verified=False, provider_requests=0,
        source_exports_verified=len(tasks), selected_source_exports=len(used), native_rows=counts['native_rows'],
        normalized_rows=result.height, products=result['product'].n_unique(), exclusions=dict(counts),
        sources=list(originals.values()), input_pins=pins,
        outputs={n: dict(sha256=sha256_file(output/n)) for n in
            ('initial_references.parquet', 'completed_source_inventory.parquet')},
        financial_or_training_adopted=False, observed_at_utc=datetime.now(UTC).isoformat())
    atomic_write_json(output/'manifest.json', manifest)
    return {k: v for k,v in manifest.items() if k not in ('sources','input_pins')}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tej-root', type=Path, required=True)
    parser.add_argument('--financial-baseline', type=Path, required=True)
    parser.add_argument('--source-release', type=Path, required=True)
    parser.add_argument('--definition-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--include-new-listings', action='store_true',
        help='Also bind populated native listing references under their distinct event contract')
    parser.add_argument('--include-event-births', action='store_true',
        help='Also bind populated native event references to proved-empty standard generations')
    args = parser.parse_args()
    print(json.dumps(build(args.tej_root, args.financial_baseline, args.source_release,
        args.definition_root, args.output, include_new_listings=args.include_new_listings,
        include_event_births=args.include_event_births)), flush=True)


if __name__ == '__main__':
    main()
