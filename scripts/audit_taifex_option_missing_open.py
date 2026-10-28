#!/usr/bin/env python3
"""Classify option-product presence mismatches using official opening prices.

TAIFEX volume includes negotiated block trades that are not in FinMind ticks.
An official nonzero opening price supplies stronger ordinary-market evidence:
block trades do not determine the reported opening. This still does not supply
the time of the trade, an executable quote, historical PIT, or a fill.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import UTC, datetime
import hashlib
import io
import json
import math
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.csv as arrow_csv

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, sha256_file
from scripts.audit_taifex_option_source_universe import _csv_column_names, _session
from stockagent.data.tw_index_futures import iter_taifex_daily_csv_streams


def audit(coverage_path: Path, presence_path: Path) -> tuple[list[dict], dict]:
    presence_raw = presence_path.read_bytes()
    presence = json.loads(presence_raw)
    if presence.get('contract') != 'taifex_option_daily_vs_finmind_product_presence_v1':
        raise ValueError('incompatible product-presence audit')
    source_csv = Path(presence['day_inventory'])
    source_raw = source_csv.read_bytes()
    if hashlib.sha256(source_raw).hexdigest() != presence['day_inventory_sha256']:
        raise ValueError('product-presence inventory changed')
    coverage_raw = coverage_path.read_bytes()
    if hashlib.sha256(coverage_raw).hexdigest() != presence['coverage_summary_sha256']:
        raise ValueError('coverage summary changed')
    coverage = json.loads(coverage_raw)
    candidates = set()
    for row in csv.DictReader(io.StringIO(source_raw.decode('utf-8'), newline='')):
        if row['source_status'] == 'product_readback_verified':
            candidates.update((row['date'], product) for product in row['missing_day_products'].split('|') if product)
    observations = {key: [] for key in candidates}
    cols = ['交易日期', '契約', '到期月份(週別)', '履約價', '買賣權', '開盤價', '成交量', '交易時段']
    inputs = []
    for year in sorted({day[:4] for day, _ in candidates}):
        files = [source for source in coverage['annual_sources'] if Path(source['path']).name == f'{year}_opt.zip']
        if len(files) != 1:
            raise ValueError('missing or ambiguous annual source')
        source = Path(files[0]['path'])
        if sha256_file(source) != files[0]['sha256']:
            raise ValueError('annual source changed')
        year_pairs = {(day, product) for day, product in candidates if day.startswith(year)}
        candidate_days = {day.replace('-', '/') for day, _ in year_pairs}
        candidate_products = {product for _, product in year_pairs}
        for stream, member, digest in iter_taifex_daily_csv_streams(source):
            if digest != files[0]['sha256']:
                raise ValueError('annual source digest mismatch')
            names, _ = _csv_column_names(stream.buffer, member)
            reader = arrow_csv.open_csv(
                stream.buffer, read_options=arrow_csv.ReadOptions(encoding='cp950', column_names=names,
                                                                 skip_rows=1, block_size=2 * 1024 * 1024),
                convert_options=arrow_csv.ConvertOptions(include_columns=cols, include_missing_columns=True,
                                                         column_types={name: pa.string() for name in cols}),
            )
            try:
                for batch in reader:
                    frame = pl.from_arrow(batch).select(pl.all().fill_null('').str.strip_chars()).filter(
                        pl.col('交易日期').is_in(candidate_days) & pl.col('契約').is_in(candidate_products)
                        & (pl.col('成交量').str.replace_all(',', '').cast(pl.Float64, strict=False) > 0))
                    for item in frame.to_dicts():
                        key = (item['交易日期'].replace('/', '-'), item['契約'])
                        if key in observations and _session(item['交易時段']) == 'day':
                            observations[key].append(item)
            finally:
                reader.close()
        if sha256_file(source) != files[0]['sha256']:
            raise ValueError('annual source changed during read')
        inputs.append({'path': str(source.resolve()), 'sha256': files[0]['sha256']})
    rows = []
    for (day, product), items in sorted(observations.items()):
        # Keep the original decimal string and only use numeric conversion to
        # distinguish a reported positive opening from '-', empty or zero.
        opened = []
        for item in items:
            try:
                value = float(item['開盤價'].replace(',', ''))
                if math.isfinite(value) and value > 0:
                    opened.append(item)
            except ValueError:
                pass
        example = opened[0] if opened else None
        rows.append({'date': day, 'product': product, 'official_positive_rows': len(items),
                     'official_positive_open_rows': len(opened),
                     'classification': 'official_open_missing_from_object' if opened else 'daily_volume_only_unresolved',
                     'example_contract': example['到期月份(週別)'] if example else '',
                     'example_strike': example['履約價'] if example else '',
                     'example_right': example['買賣權'] if example else '',
                     'example_open_price': example['開盤價'] if example else '',
                     'example_daily_volume_including_block': example['成交量'] if example else ''})
    if coverage_path.read_bytes() != coverage_raw or presence_path.read_bytes() != presence_raw or source_csv.read_bytes() != source_raw:
        raise ValueError('audit input changed during read')
    stats = []
    for year in sorted({row['date'][:4] for row in rows}):
        year_rows = [row for row in rows if row['date'].startswith(year)]
        classes = Counter(row['classification'] for row in year_rows)
        stats.append({'year': int(year), 'candidate_product_days': len(year_rows),
                      'ordinary_open_product_days': classes['official_open_missing_from_object'],
                      'ordinary_open_dates': len({row['date'] for row in year_rows if row['classification'] == 'official_open_missing_from_object'}),
                      'volume_only_unresolved_product_days': classes['daily_volume_only_unresolved']})
    return rows, {
        'contract': 'taifex_option_missing_open_product_presence_v1',
        'observed_at_utc': datetime.now(UTC).isoformat(), 'inputs': inputs,
        'coverage_sha256': hashlib.sha256(coverage_raw).hexdigest(),
        'presence_summary_sha256': hashlib.sha256(presence_raw).hexdigest(),
        'presence_inventory_sha256': hashlib.sha256(source_raw).hexdigest(),
        'audit_script_sha256': sha256_file(Path(__file__)), 'by_year': stats,
        'rules_source': 'https://www.taifex.com.tw/cht/4/bTIntroduction',
        'limitations': ['The daily opening is offline corroboration only, not an intraday model feature or clock-specific fill.',
                        'A positive ordinary opening and a missing product in a whole-day object indicate a source mismatch, not its root cause.',
                        'Other absent products can be block-only; do not impute executions or declare an unobserved trade.'],
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--coverage-summary', type=Path, required=True)
    p.add_argument('--presence-summary', type=Path, required=True)
    p.add_argument('--output-dir', type=Path, required=True)
    args = p.parse_args()
    rows, summary = audit(args.coverage_summary, args.presence_summary)
    if not rows:
        raise ValueError('no product-presence candidates')
    buffer = io.StringIO(newline='')
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    data = buffer.getvalue().encode()
    output = args.output_dir / 'option_missing_product_open_evidence.csv'
    atomic_write_bytes(output, data)
    summary.update(inventory=str(output.resolve()), inventory_sha256=hashlib.sha256(data).hexdigest())
    atomic_write_json(args.output_dir / 'option_missing_product_open_evidence.summary.json', summary)
    print(json.dumps({'output': str(output), 'by_year': summary['by_year']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
