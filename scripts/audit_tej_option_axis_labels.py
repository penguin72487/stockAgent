#!/usr/bin/env python3
"""Profile a retained TEJ options menu; never infer market data or tradeability."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import UTC, datetime
import json
from pathlib import Path
import re

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.tej_company_axis_snapshot import iter_snapshot, verify_snapshot

REPO = Path(__file__).resolve().parents[1]
CODE = re.compile(r'(?P<prefix>[A-Z0-9]{3})(?P<year>20[0-9]{2})'
                  r'(?P<month>0[1-9]|1[0-2])(?P<week>[WF][1-5])?'
                  r'(?P<side>[CP])(?P<strike>[0-9]+(?:\.[0-9]+)?)')
DESCRIPTION = re.compile(r'(?P<underlying>.*?)(?P<side>買權|賣權)\s+'
                         r'(?P<series>20[0-9]{4}(?:[WF][1-5])?)\s+'
                         r'(?P<strike>[0-9]+(?:\.[0-9]+)?)')


def parse_label(label: str) -> dict | None:
    if label.count('==>') != 1:
        return None
    code, description = label.split('==>')
    match = CODE.fullmatch(code)
    if not match:
        return None
    part = match.groupdict()
    desc = DESCRIPTION.fullmatch(description)
    if (not desc or desc['side'] != ('買權' if part['side'] == 'C' else '賣權')
            or desc['series'] != part['year'] + part['month'] + (part['week'] or '')
            or desc['strike'] != part['strike']):
        return None
    return {'code': code, **part, 'description_prefix': desc['underlying']}


def profile(labels) -> dict:
    kinds, years, sides = Counter(), Counter(), Counter()
    prefixes = defaultdict(Counter)
    noncontract = []
    codes = set()
    duplicates = 0
    count = empty_underlying = 0
    for label in labels:
        count += 1
        code = label.split('==>', 1)[0]
        duplicates += code in codes
        codes.add(code)
        part = parse_label(label)
        if part is None:
            noncontract.append(label)
            continue
        kind = {'W': 'week_w', 'F': 'week_f'}.get((part['week'] or '')[:1], 'monthly')
        kinds[kind] += 1
        years[part['year']] += 1
        sides[part['side']] += 1
        empty_underlying += not part['description_prefix']
        prefixes[part['prefix']][kind] += 1
    return {'labels': count, 'candidate_contract_labels': sum(kinds.values()),
            'candidate_kinds': dict(sorted(kinds.items())), 'label_years': dict(sorted(years.items())),
            'candidate_sides': dict(sorted(sides.items())), 'distinct_candidate_prefixes': len(prefixes),
            'candidate_prefixes': {k: dict(sorted(v.items())) for k, v in sorted(prefixes.items())},
            'candidate_empty_underlying_descriptions': empty_underlying,
            'noncontract_or_unparsed_labels': noncontract,
            'duplicate_code_strings': duplicates}


def audit(receipt_path: Path, output: Path) -> dict:
    if output.exists() or output.is_symlink() or not output.resolve().is_relative_to(REPO / 'artifacts/markets'):
        raise ValueError('new research report path required')
    receipt_path = receipt_path.resolve(strict=True)
    receipt = json.loads(receipt_path.read_bytes())
    manifest = Path(receipt['snapshot_manifest_path'])
    binary = Path(receipt['snapshot_path'])
    if (receipt.get('contract') != 'tej_large_company_axis_independent_readback_v1'
            or receipt.get('market_observations_obtained') is not False
            or receipt.get('full_axis_admitted_to_plan') is not False
            or receipt.get('training_input_admitted') is not False
            or not manifest.resolve().is_relative_to(REPO / 'data_tej/raw')
            or not binary.resolve().is_relative_to(REPO / 'data_tej/raw')
            or sha256_file(manifest) != receipt['snapshot_manifest_sha256']):
        raise ValueError('exact source-only independent receipt required')
    proof = verify_snapshot(binary)
    if proof != receipt['source_proof'] or proof['duplicate_exact_labels']:
        raise ValueError('independent source proof differs or exact labels are ambiguous')
    stats = profile(iter_snapshot(binary))
    if stats['labels'] != proof['count'] or stats['duplicate_code_strings']:
        raise ValueError('source label profile is incomplete or codes are ambiguous')
    if sha256_file(binary) != proof['sha256'] or sha256_file(manifest) != receipt['snapshot_manifest_sha256']:
        raise ValueError('source changed during profiling')
    result = {'contract': 'tej_option_source_menu_label_profile_v1',
              'observed_at_utc': datetime.now(UTC).isoformat(),
              'source_receipt_path': str(receipt_path), 'source_receipt_sha256': sha256_file(receipt_path),
              'source_snapshot_path': str(binary), 'source_snapshot_sha256': proof['sha256'],
              'source_table': receipt['table'], 'stats': stats,
              'implementation_sha256': sha256_file(Path(__file__)),
              'provider_queries_by_audit': 0, 'market_observations_obtained': False,
              'full_axis_admitted_to_plan': False, 'training_input_admitted': False,
              'limitations': ['Candidate parsing describes only present display labels and is not an official crosswalk.',
                  'The all-option menu includes years outside the selected table name; table coverage is unproven.',
                  'A prefix is not proof of a physical contract generation, listed product, currency or category.',
                  'Labels do not establish trading dates, prices, data rights, execution or point-in-time availability.']}
    output.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(output, result)
    if json.loads(output.read_bytes()) != result:
        raise ValueError('profile readback differs')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.receipt, args.output)
    stats = result['stats']
    print(json.dumps({'report': str(args.output), 'labels': stats['labels'],
                      'candidate_kinds': stats['candidate_kinds'],
                      'other_labels': len(stats['noncontract_or_unparsed_labels']),
                      'training_input_admitted': False}, ensure_ascii=False))


if __name__ == '__main__':
    main()
