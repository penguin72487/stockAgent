#!/usr/bin/env python3
"""Pair a frozen full-history ablation matrix with partial-clock controls.

This only creates a draft experiment specification. It does not start a
worker, certify the financial sources, or change the benchmark contract.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from pathlib import Path
import sys

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_bytes


def paired_spec(parent: dict, *, partial_clock_root: Path, output_root: Path) -> dict:
    result = deepcopy(parent)
    dimensions = result.get('matrix', {}).get('dimensions', [])
    if (len(dimensions) != 1 or dimensions[0].get('name') != 'all_twd'
            or len(dimensions[0].get('variants', [])) != 8
            or result.get('expected_fold_count') != 14):
        raise ValueError('expected the reviewed 14-fold eight-variant full-history parent')
    paired = []
    for variant in dimensions[0]['variants']:
        name = variant['name']
        parts = name.split('__')
        if len(parts) < 3 or parts[0] not in {'clock0845', 'clock0900'} or parts[1] not in {'1m', '300m'}:
            raise ValueError('parent variant does not name a reviewed clock and window')
        strict = deepcopy(variant)
        strict['name'] = 'strict__' + name
        partial = deepcopy(variant)
        partial['name'] = 'partial__' + name
        partial['description'] = str(partial.get('description', '')) + ' Incomplete-source observed prints; research only.'
        clock = parts[0][5:]
        folder = partial_clock_root / ('entry_window_300m' if parts[1] == '300m' else '') / clock
        trading = partial.setdefault('overrides', {}).setdefault('trading', {})
        trading['tw_futures_portfolio_entry_clock_path'] = str(folder / 'entries.parquet')
        trading['tw_futures_portfolio_opening_reference_path'] = str(folder / 'opening_reference.parquet')
        paired.extend((strict, partial))
    if len({row['name'] for row in paired}) != len(paired):
        raise ValueError('paired variant names are not unique')
    dimensions[0]['variants'] = paired
    result['output_root'] = str(output_root)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('parent-spec', 'partial-clock-root', 'output-root', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError('draft specification must not overwrite an existing file')
    parent = yaml.safe_load(args.parent_spec.read_text())
    result = paired_spec(parent, partial_clock_root=args.partial_clock_root, output_root=args.output_root)
    atomic_write_bytes(args.output, yaml.safe_dump(result, allow_unicode=True, sort_keys=False).encode())
    print(f"draft spec: {args.output}; variants={len(result['matrix']['dimensions'][0]['variants'])}; no jobs started")


if __name__ == '__main__':
    main()
