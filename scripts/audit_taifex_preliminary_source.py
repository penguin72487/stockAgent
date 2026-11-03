#!/usr/bin/env python3
"""Independently read a finite preliminary TAIFEX source capture."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timezone
import json
from pathlib import Path
import zipfile

import polars as pl
import pyarrow as pa
import pyarrow.csv as arrow_csv

from downloader.artifact_io import atomic_write_json, sha256_file


def audit(manifest: Path) -> dict:
    manifest = manifest.resolve()
    manifest_sha = sha256_file(manifest)
    doc = json.loads(manifest.read_bytes())
    if (doc.get('contract') != 'taifex_precompletion_source_snapshot_v1'
            or doc.get('status') != 'observed_provisional' or doc.get('complete_trading_day') is not False
            or doc.get('training_ready') is not False):
        raise ValueError('a source-only preliminary snapshot is required')
    day = date.fromisoformat(doc['trading_date'])
    columns = ['date', 'product', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7', 'c8']
    results = {}
    for kind in ('futures', 'options'):
        page, source = doc['listing_pages'][kind], doc['raw_downloads'][kind]
        page_path, path = Path(page['path']), Path(source['path'])
        if (page_path.is_symlink() or path.is_symlink() or sha256_file(page_path) != page['sha256']
                or sha256_file(path) != source['sha256'] or path.stat().st_size != source['bytes']):
            raise ValueError('preliminary archive or listing hash differs')
        prefix = 'Daily' if kind == 'futures' else 'OptionsDaily'
        member = f"{prefix}_{day.strftime('%Y_%m_%d')}.csv"
        counts, products = Counter(), set()
        with zipfile.ZipFile(path) as archive:
            if archive.namelist() != [member]:
                raise ValueError('unexpected preliminary ZIP member')
            with archive.open(member) as stream:
                reader = arrow_csv.open_csv(stream,
                    read_options=arrow_csv.ReadOptions(encoding='cp950', column_names=columns,
                                                       skip_rows=2 if kind == 'options' else 1,
                                                       block_size=2 * 1024 * 1024),
                    convert_options=arrow_csv.ConvertOptions(column_types={col: pa.string() for col in columns}))
                try:
                    for batch in reader:
                        frame = pl.from_arrow(batch).select(pl.col('date').str.strip_chars(),
                                                            pl.col('product').str.strip_chars())
                        if frame.filter(pl.col('date').is_null() | pl.col('product').is_null()
                                        | ~pl.col('date').str.contains(r'^\d{8}$')
                                        | (pl.col('product') == '')).height:
                            raise ValueError('invalid preliminary source row identity')
                        for raw_day, count in frame.group_by('date').len().iter_rows():
                            observed = date(int(raw_day[:4]), int(raw_day[4:6]), int(raw_day[6:8]))
                            counts[observed.isoformat()] += count
                        products.update(frame['product'].unique().to_list())
                finally:
                    reader.close()
        expected = doc['independent_readback'][kind]
        actual = {'source_rows': sum(counts.values()), 'physical_date_rows': dict(sorted(counts.items())),
                  'source_product_count': len(products)}
        if actual != expected:
            raise ValueError('independent Arrow row count or source scope differs')
        results[kind] = {**actual, 'archive_sha256': source['sha256'], 'listing_sha256': page['sha256']}
    if sha256_file(manifest) != manifest_sha or any(
            sha256_file(Path(doc['raw_downloads'][kind]['path'])) != doc['raw_downloads'][kind]['sha256']
            or sha256_file(Path(doc['listing_pages'][kind]['path'])) != doc['listing_pages'][kind]['sha256']
            for kind in ('futures', 'options')):
        raise ValueError('preliminary capture changed during readback')
    return {'contract': 'taifex_precompletion_source_independent_readback_v1',
            'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'trading_date': day.isoformat(),
            'capture_manifest': str(manifest), 'capture_manifest_sha256': manifest_sha,
            'captured_at': doc['captured_at'], 'source_status': doc['status'], 'sources': results,
            'implementation_sha256': sha256_file(Path(__file__)), 'training_ready': False,
            'limitations': ['Source may be revised before the canonical 17:00 completion cutoff.',
                'These are raw row counts, not independent fills or validated executable prices.',
                'The canonical completed-day receipt has not been established by this audit.']}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError('preserve earlier source evidence')
    result = audit(args.manifest)
    atomic_write_json(args.output, result)
    print(json.dumps({'output': str(args.output), 'sources': result['sources'],
                      'training_ready': False}, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
