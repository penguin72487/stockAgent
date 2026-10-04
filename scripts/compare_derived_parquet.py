#!/usr/bin/env python3
"""Compare common logical columns in bounded batches; never infer resume safety."""
from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.compute as pc  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402


def logical_equal(a, b):
    equal = pc.fill_null(pc.equal(a, b), False)
    equal = pc.or_(equal, pc.and_(pc.is_null(a), pc.is_null(b)))
    if pa.types.is_floating(a.type):
        both_nan = pc.and_(pc.fill_null(pc.is_nan(a), False), pc.fill_null(pc.is_nan(b), False))
        equal = pc.or_(equal, both_nan)
    return equal


def bounded_batches(file, columns, end_date, symbols=None):
    for batch in file.iter_batches(columns=columns, batch_size=16384, use_threads=False):
        if end_date is not None:
            dates = batch.column(columns.index('date'))
            if not pa.types.is_date(dates.type):
                raise ValueError('completed-session cutoff requires an explicit Arrow date field')
            batch = pc.filter(batch, pc.less_equal(dates, pa.scalar(end_date, type=dates.type)))
        if symbols is not None:
            values = batch.column(columns.index('symbol'))
            batch = pc.filter(batch, pc.is_in(values, value_set=pa.array(sorted(symbols), type=values.type)))
        if batch.num_rows:
            yield batch


def compare(left: Path, right: Path, *, end_date=None, symbols=None):
    before = {str(p): (p.stat().st_ino, p.stat().st_size, p.stat().st_mtime_ns, p.stat().st_ctime_ns)
              for p in (left, right)}
    started = time.perf_counter()
    a, b = pq.ParquetFile(left), pq.ParquetFile(right)
    sa, sb = a.schema_arrow, b.schema_arrow
    columns = [n for n in sa.names if n in sb.names]
    if not {'date', 'symbol'} <= set(columns):
        raise ValueError('comparison needs canonical date/symbol keys')
    changed_types = {n: [str(sa.field(n).type), str(sb.field(n).type)] for n in columns
                     if sa.field(n).type != sb.field(n).type}
    if changed_types:
        raise ValueError('common column types differ; no normalization may hide schema drift')
    ia, ib = bounded_batches(a, columns, end_date, symbols), bounded_batches(b, columns, end_date, symbols)
    ca, cb, oa, ob, rows = None, None, 0, 0, 0
    differences = dict.fromkeys(columns, 0)
    exhausted_equally = True
    while True:
        if ca is None or oa == ca.num_rows:
            ca, oa = next(ia, None), 0
        if cb is None or ob == cb.num_rows:
            cb, ob = next(ib, None), 0
        if ca is None or cb is None:
            exhausted_equally = ca is None and cb is None
            break
        count = min(ca.num_rows-oa, cb.num_rows-ob)
        x, y = ca.slice(oa, count), cb.slice(ob, count)
        for n in ('date', 'symbol'):
            if not x.column(columns.index(n)).equals(y.column(columns.index(n))):
                raise ValueError('row keys/order differ; common values must not be compared by position')
        for i, name in enumerate(columns):
            equal = logical_equal(x.column(i), y.column(i))
            differences[name] += pc.sum(pc.cast(pc.invert(equal), pa.int64())).as_py() or 0
        rows += count; oa += count; ob += count
    after = {str(p): (p.stat().st_ino, p.stat().st_size, p.stat().st_mtime_ns, p.stat().st_ctime_ns)
             for p in (left, right)}
    if before != after:
        raise ValueError('input changed during logical comparison')
    return {'schema_version': 1, 'state': 'compared', 'left': str(left), 'right': str(right),
            'left_total_rows': a.metadata.num_rows, 'right_total_rows': b.metadata.num_rows,
            'inclusive_end_date': end_date.isoformat() if end_date else None,
            'selected_symbol_count': len(symbols) if symbols is not None else None,
            'compared_rows': rows, 'common_columns': columns,
            'added_right_columns': [n for n in sb.names if n not in sa.names],
            'removed_right_columns': [n for n in sa.names if n not in sb.names],
            'different_cells_by_column': {n: v for n, v in differences.items() if v},
            'both_completed_same_scope': exhausted_equally,
            'logical_common_columns_equal': exhausted_equally and not any(differences.values()),
            'inputs_stable': True, 'wall_seconds': time.perf_counter()-started,
            'checkpoint_resume_compatibility_verified': False,
            'scope': 'aligned common-column logical values, NULL and NaN distinguished; no source ABI/resume or PIT proof'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('left', type=Path)
    parser.add_argument('right', type=Path)
    parser.add_argument('--end-date', type=date.fromisoformat)
    parser.add_argument('--symbols-json', type=Path, help='explicit allowed symbol list; no inferred intersection')
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists() or args.output.resolve() in {args.left.resolve(), args.right.resolve()}:
        raise ValueError('comparison receipt requires a new path; inputs and existing evidence cannot be overwritten')
    pa.set_cpu_count(2)
    symbols = None
    if args.symbols_json:
        values = json.loads(args.symbols_json.read_text())
        if (not isinstance(values, list) or not values or len(values) > 10000
                or any(not isinstance(v, str) or not v for v in values) or len(values) != len(set(values))):
            raise ValueError('selected symbols must be an explicit unique nonempty string list')
        symbols = set(values)
    result = compare(args.left, args.right, end_date=args.end_date, symbols=symbols)
    atomic_write_json(args.output, result)
    print(json.dumps({k: result[k] for k in ('state', 'compared_rows', 'logical_common_columns_equal',
                                           'different_cells_by_column', 'wall_seconds')}), flush=True)


if __name__ == '__main__':
    main()
