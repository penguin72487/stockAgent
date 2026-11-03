#!/usr/bin/env python3
"""Project a pinned TX daily release for a retrospective tick-source audit.

The projection is research evidence only; it is not an execution, historical
information-availability, or benchmark admission receipt.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from downloader.artifact_io import sha256_file  # noqa: E402

COLUMNS = [
    'date', 'product', 'contract', 'official_expiry', 'official_volume',
    'outright_volume', 'official_open', 'official_high', 'official_low',
    'official_close', 'official_source_sha256', 'official_row_observed',
    'source_row_observed', 'accounting_only_date',
]


def project(source: Path, expected_sha256: str, start: str, end: str) -> pl.DataFrame:
    if sha256_file(source) != expected_sha256:
        raise ValueError('input daily release differs from its pinned digest')
    frame = (pl.scan_parquet(source)
             .filter((pl.col('product') == 'TX') & ~pl.col('accounting_only_date')
                     & pl.col('date').is_between(pl.lit(start).str.to_date(),
                                                 pl.lit(end).str.to_date()))
             .select(COLUMNS).sort('date', 'contract').collect())
    if not frame.height or frame.select(pl.struct(['date', 'contract']).is_duplicated().any()).item():
        raise ValueError('TX daily release has no rows or ambiguous physical contract-days')
    if sha256_file(source) != expected_sha256:
        raise ValueError('input daily release changed during projection')
    return frame


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--daily', required=True, type=Path)
    parser.add_argument('--daily-sha256', required=True)
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('choose a new output directory')
    result = project(args.daily, args.daily_sha256, args.start, args.end)
    args.output.mkdir(parents=True)
    table = args.output / 'tx_contract_days.parquet'
    result.write_parquet(table)
    manifest = {
        'contract': 'tx_contract_day_research_audit_projection_v1',
        'observed_at_utc': datetime.now(UTC).isoformat(),
        'source_path': str(args.daily), 'source_sha256': args.daily_sha256,
        'projection_sha256': sha256_file(table), 'projection_rows': result.height,
        'script_sha256': sha256_file(Path(__file__)),
        'start_date': args.start, 'end_date': args.end,
        'research_only': True, 'source_pit_verified': False,
        'benchmark_admitted': False, 'training_admitted': False,
    }
    (args.output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps(manifest))


if __name__ == '__main__':
    main()
