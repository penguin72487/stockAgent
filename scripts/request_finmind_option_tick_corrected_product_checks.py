#!/usr/bin/env python3
"""Register diverse product/day checks from corrected option source audits.

The selection remains a bounded priority request through the existing native
owner. No prices are manufactured or admitted into a training dataset.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import UTC, datetime
import json
from pathlib import Path

from downloader.artifact_io import sha256_file
from downloader.finmind_source_conflict_rechecks import CONTRACT_BATCHES, load_option_contract_conflicts
from scripts.request_finmind_option_tick_product_checks import CONTRACT, DATASET, evidence, register_evidenced
from scripts.run_finmind_option_conflict_wait import corrected_contract_readbacks


def register_corrected(root: Path, readback_paths: list[Path], output: Path,
                       now: datetime, max_items: int = 64) -> dict:
    root = root.resolve()
    if not 1 <= max_items <= 64 or not readback_paths:
        raise ValueError('one to 64 checks and corrected audit evidence are required')
    readbacks, pins = corrected_contract_readbacks(root, readback_paths)
    pins[str(Path(__file__).resolve())] = sha256_file(Path(__file__))
    choices = defaultdict(list)
    for sidecar in sorted((root / CONTRACT_BATCHES).glob('*.json')):
        loaded = load_option_contract_conflicts(root, now, sidecar=sidecar)
        if not loaded:
            continue
        for old in loaded[0]['requests']:
            day = old['start_date']
            new = readbacks.get(day)
            if (not new or not new['verified'] or new['receipt'] != old['original_receipt_sha256']
                    or new['parquet'] != old['original_parquet_sha256']):
                continue
            prior = {tuple(key) for key in old['expected_source_contracts']}
            if not new['keys'].issubset(prior):
                raise ValueError('corrected option keys differ from the original source audit')
            receipt = root / 'receipts' / DATASET / 'all' / f'{day}.json'
            if sha256_file(receipt) != old['original_receipt_sha256']:
                continue  # A fresh whole-market object requires a fresh audit.
            product_keys = defaultdict(list)
            for key in new['keys']:
                product_keys[key[0]].append(key)
            for product, keys in product_keys.items():
                choices[product].append((day, len(keys)))
    # First cover as many distinct products as possible, choosing the day with
    # the most audited missing keys per product; subsequent rounds cover days.
    for candidates in choices.values():
        candidates.sort(key=lambda item: (-item[1], item[0]))
    selected = []
    offset = 0
    while len(selected) < max_items:
        candidates = [(product, rows[offset]) for product, rows in sorted(choices.items())
                      if len(rows) > offset]
        if not candidates:
            break
        for product, (day, _) in candidates:
            selected.append((day, product))
            if len(selected) == max_items:
                break
        offset += 1
    if not selected:
        raise ValueError('no comparable corrected product/day evidence')
    checked = evidence(root, selected, now)
    for item in checked:
        corrected = readbacks[item['partition']]
        original = {tuple(key) for key in item['audited_missing_contracts']}
        keys = sorted(key for key in corrected['keys'] if key[0] == item['data_id'])
        if (not corrected['verified'] or corrected['receipt'] != item['original_receipt_sha256']
                or corrected['parquet'] != item['original_parquet_sha256']
                or not keys or not set(keys).issubset(original)):
            raise ValueError('corrected product audit changed before registration')
        item['audited_missing_contracts'] = [list(key) for key in keys]
    result = register_evidenced(root, checked, now, output, dataset=DATASET, contract=CONTRACT,
        reason='Compare corrected official option contract gaps against native per-product tick data; preserve original source receipts.',
        additional_evidence_pins=pins)
    result.update(distinct_products=len({product for _, product in selected}),
                  audited_missing_contract_keys=sum(len(row['audited_missing_contracts']) for row in checked))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/complement'))
    parser.add_argument('--corrected-contract-readback', type=Path, action='append', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--max-items', type=int, default=64)
    args = parser.parse_args()
    result = register_corrected(args.root, args.corrected_contract_readback, args.output, datetime.now(UTC), args.max_items)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
