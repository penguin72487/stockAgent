#!/usr/bin/env python3
"""Bounded, source-pinned language comparison; never selects a production backend.

Run through scripts/runtime_env.sh. Native kernels live under scripts/benchmarks;
the output directory owns snapshots, binaries, raw samples and provenance.
"""
from __future__ import annotations

import argparse
import ctypes
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import resource
import shutil
import statistics
import subprocess
import sys
import time

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def sha(path: Path) -> str:
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def atomic_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    temporary.replace(path)


def verify_arrays(actual, expected) -> dict:
    """No floating tolerance: preserve NaN locations, finite bits and signed zero."""
    if len(actual) != len(expected):
        raise AssertionError('output arity differs')
    hashes = []
    for index, (got, want) in enumerate(zip(actual, expected)):
        got, want = np.asarray(got), np.asarray(want)
        if got.shape != want.shape or got.dtype != want.dtype or got.dtype != np.float64:
            raise AssertionError(f'output {index} shape/dtype differs')
        missing = np.isnan(want)
        if not np.array_equal(np.isnan(got), missing):
            raise AssertionError(f'output {index} NaN mask differs')
        valid = ~missing
        if not np.array_equal(got[valid].view(np.uint64), want[valid].view(np.uint64)):
            count = np.count_nonzero(got[valid].view(np.uint64) != want[valid].view(np.uint64))
            raise AssertionError(f'output {index} differs in {count} non-NaN bit patterns')
        canonical = np.ascontiguousarray(got).copy()
        canonical[missing] = np.nan  # Only NaN payload/sign are outside the numeric contract.
        hashes.append(hashlib.sha256(canonical.tobytes()).hexdigest())
    return {'finite_and_infinite_bits_equal': True, 'nan_masks_equal': True,
            'nan_payloads_compared': False, 'canonical_output_sha256': hashes}


def moments_loop(values: np.ndarray, window: int):
    n = len(values)
    sums, squares, counts = np.empty(n), np.empty(n), np.empty(n, dtype=np.int64)
    total, square, count = 0., 0., 0
    for i in range(n):
        raw = float(values[i])
        finite = math.isfinite(raw)
        value = raw if finite else 0.
        count += int(finite)
        total += value
        square += value * value
        counts[i], sums[i], squares[i] = count, total, square
    means, stds = np.empty(n), np.empty(n)
    for i in range(n):
        count = int(counts[i]) - (int(counts[i - window]) if i >= window else 0)
        total = float(sums[i]) - (float(sums[i - window]) if i >= window else 0.)
        square = float(squares[i]) - (float(squares[i - window]) if i >= window else 0.)
        mean = total / count if count else 0.
        second = square / count if count else 0.
        variance = second - mean * mean
        means[i], stds[i] = mean, math.sqrt(variance if variance > 0. else 0.)
    return means, stds


def moments_prefix_once(values: np.ndarray, window: int):
    """Candidate: reuse exact prefix arrays instead of computing each twice."""
    valid = np.isfinite(values)
    masked = np.where(valid, values, 0.)
    counts = np.cumsum(valid.astype(np.int64))
    sums, squares = np.cumsum(masked), np.cumsum(np.square(masked))
    if window < len(values):
        counts[window:] = counts[window:] - counts[:-window]
        sums[window:] = sums[window:] - sums[:-window]
        squares[window:] = squares[window:] - squares[:-window]
    means = np.divide(sums, counts, out=np.zeros_like(sums), where=counts > 0)
    second = np.divide(squares, counts, out=np.zeros_like(squares), where=counts > 0)
    return means, np.sqrt(np.maximum(second - np.square(means), 0.))


def cpu_seconds() -> float:
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    return time.process_time() + children.ru_utime + children.ru_stime


def measure(function, *, target_cpu_s: float):
    wall, cpu, iterations = time.perf_counter(), cpu_seconds(), 0
    while True:
        output = function()
        iterations += 1
        if cpu_seconds() - cpu >= target_cpu_s or iterations >= 2048:
            break
    return output, {'wall_s': (time.perf_counter() - wall) / iterations,
                    'cpu_s': (cpu_seconds() - cpu) / iterations, 'iterations': iterations}


def balanced_order(names: list[str], round_index: int) -> list[str]:
    # Every adjacent pair is A..Z / Z..A, limiting systematic warm-cache drift.
    return names if round_index % 2 == 0 else list(reversed(names))


def summarize(samples: list[dict]) -> dict:
    return {field: {'median': statistics.median(row[field] for row in samples),
                    'min': min(row[field] for row in samples),
                    'max': max(row[field] for row in samples)}
            for field in ('wall_s', 'cpu_s')}


def main() -> int:
    main_start = time.perf_counter()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--stock-root', type=Path, default=Path('data_tw_public/stocks'))
    parser.add_argument('--stock-files', type=int, default=64)
    parser.add_argument('--returns-source', type=Path, default=Path('data_okx/BTCUSDTSWAP_features.parquet'))
    parser.add_argument('--query-source', type=Path, default=Path('data_tw_index_futures/all_futures_daily_sessions.parquet'))
    parser.add_argument('--rustc', default=shutil.which('rustc'))
    parser.add_argument('--cxx', default=shutil.which('g++'))
    parser.add_argument('--node', default=shutil.which('node'))
    parser.add_argument('--repeats', type=int, default=4)
    parser.add_argument('--target-cpu-ms', type=float, default=40.)
    parser.add_argument('--window', type=int, default=32)
    parser.add_argument('--threads', type=int, default=1)
    args = parser.parse_args()
    if min(args.stock_files, args.repeats, args.window, args.threads, args.target_cpu_ms) <= 0:
        parser.error('counts, window, thread budget and timing target must be positive')
    out = args.output_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)
    if (out / 'result.json').exists():
        parser.error('result.json exists; use a new output directory to preserve evidence')
    os.environ['POLARS_MAX_THREADS'] = str(args.threads)
    import numba as nb
    import polars as pl
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    from stockagent.data import panel_numba
    from stockagent.data.tw_all_futures import _REGULAR_SESSION_ALIASES, _rolling_moments
    from stockagent.data.tw_price_rules import (
        TW_TICK_RULE_2005_EFFECTIVE_ORDINAL as tick_cutoff,
        TW_LIMIT_10_PERCENT_EFFECTIVE_ORDINAL as ratio_cutoff,
        limit_price_numpy,
    )
    from stockagent.remote_build import observe_node
    nb.set_num_threads(args.threads)
    pa.set_cpu_count(args.threads)
    pa.set_io_thread_count(args.threads)
    event_path = out / 'events.jsonl'

    def event(message):
        row = {'at_utc': datetime.now(timezone.utc).isoformat(), 'message': message}
        with event_path.open('a') as stream:
            stream.write(json.dumps(row, ensure_ascii=False) + '\n')
        print(message, flush=True)

    start = main_start
    sources = []

    def source_bytes(path):
        path = path.resolve(strict=True)
        before = path.stat()
        body = path.read_bytes()
        after = path.stat()
        identity = lambda stat: (stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
        if identity(before) != identity(after) or len(body) != before.st_size:
            raise RuntimeError(f'source changed while snapshotting: {path}')
        sources.append({'path': str(path), 'size': len(body), 'mtime_ns': before.st_mtime_ns,
                        'sha256': hashlib.sha256(body).hexdigest()})
        return body

    event('Snapshotting real source data, preserving provenance')
    stock_paths = [p for p in sorted(args.stock_root.glob('*_features.parquet'))
                   if len(p.stem.split('_')[0]) == 4 and p.stem.split('_')[0].isdigit()][:args.stock_files]
    if len(stock_paths) != args.stock_files:
        raise RuntimeError('not enough regular-stock source files')
    prices, dates = [], []
    for path in stock_paths:
        table = pq.read_table(pa.BufferReader(source_bytes(path)), columns=['date', 'close'])
        prices.append(table['close'].to_numpy().astype(np.float64))
        dates.append(table['date'].to_numpy().astype('datetime64[D]').astype(np.int64))
    prices, dates = np.concatenate(prices), np.concatenate(dates)
    if not len(prices):
        raise ValueError('empty stock input')
    pq.write_table(pa.table({'value': prices, 'date_ordinal': dates}), out / 'prices.parquet')
    return_column = 'okx_mark_log_return_15m'
    return_table = pq.read_table(pa.BufferReader(source_bytes(args.returns_source)), columns=['date', return_column])
    times = np.asarray(return_table['date'].to_pylist(), dtype='datetime64[ns]')
    if np.isnat(times).any() or np.any(times[1:] < times[:-1]):
        raise ValueError('return source must have observed, chronological timestamps')
    values = np.ascontiguousarray(return_table[return_column].to_numpy(), dtype=np.float64)
    if not len(values):
        raise ValueError('empty rolling input')
    pq.write_table(pa.table({'value': values}), out / 'returns.parquet')
    (out / 'query.parquet').write_bytes(source_bytes(args.query_source))
    atomic_json(out / 'sources.json', {'sources': sources})
    code_paths = [Path(__file__), ROOT / 'stockagent/data/panel_numba.py',
                  ROOT / 'stockagent/data/tw_price_rules.py', ROOT / 'stockagent/data/tw_all_futures.py',
                  *sorted((ROOT / 'scripts/benchmarks').glob('language_kernels.*'))]
    code = {str(path.relative_to(ROOT)): sha(path) for path in code_paths}
    profile = observe_node(out / 'prices.parquet', out)
    result = {'schema_version': 1, 'state': 'running',
              'scope': 'bounded CPU kernels, pinned Parquet workflows and call boundaries; no full training/deployment claim',
              'created_at_utc': datetime.now(timezone.utc).isoformat(),
              'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
              'code_sha256': code, 'node_before': profile,
              'runtime': {'python': sys.version, 'numpy': np.__version__, 'numba': nb.__version__,
                          'pyarrow': pa.__version__, 'polars': pl.__version__,
                          'numba_threads': nb.get_num_threads(), 'arrow_threads': pa.cpu_count(),
                          'arrow_io_threads': pa.io_thread_count(), 'polars_threads': pl.thread_pool_size()},
              'inputs': {'stock_files': len(stock_paths), 'price_rows': len(prices), 'rolling_rows': len(values),
                         'window': args.window, 'return_column': return_column,
                         'first_timestamp': str(times[0]), 'last_timestamp': str(times[-1]),
                         'snapshots': {name: sha(out / name) for name in ('prices.parquet', 'returns.parquet', 'query.parquet')}},
              'builds': [], 'kernels': {}, 'workflows': {}, 'queries': {}, 'boundary': {}}

    def python_limits(price, day, ratio=1.10):
        array = np.empty(len(price))
        tick = panel_numba._tw_tick_size_scalar.py_func
        dated_ratio = panel_numba._tw_dated_limit_ratio.py_func
        for i in range(len(price)):
            theoretical = float(price[i]) * dated_ratio(ratio, int(day[i]))
            if not math.isfinite(theoretical) or theoretical <= 0.:
                array[i] = np.nan
                continue
            step = tick(theoretical, int(day[i]))
            rounded = (math.ceil(theoretical / step - 1e-12) if ratio < 1.
                       else math.floor(theoretical / step + 1e-12)) * step
            array[i] = math.floor(rounded * 100. + .5) / 100.
        return (array,)

    compiled_moments = nb.njit(cache=True, fastmath=False)(moments_loop)
    candidates = {
        'limits': {
            'python': python_limits,
            'numpy': lambda p, d, r=1.10: (limit_price_numpy(p, r, d.astype('datetime64[D]')),),
            'numba': lambda p, d, r=1.10: (panel_numba.tw_limit_price(p, r, d.astype('datetime64[D]')),),
        },
        'moments': {
            'python': lambda p, d, r=1.10: moments_loop(p, args.window),
            'numpy': lambda p, d, r=1.10: tuple(a.ravel() for a in _rolling_moments(p[:, None], np.isfinite(p[:, None]), args.window)),
            'numpy_prefix_once': lambda p, d, r=1.10: moments_prefix_once(p, args.window),
            'numba': lambda p, d, r=1.10: compiled_moments(p, args.window),
        },
    }
    native = {}
    pointer_float = np.ctypeslib.ndpointer(dtype=np.float64, ndim=1, flags='C_CONTIGUOUS')
    pointer_int = np.ctypeslib.ndpointer(dtype=np.int64, ndim=1, flags='C_CONTIGUOUS')
    for language, compiler, extension in [('cpp', args.cxx, 'cpp'), ('rust', args.rustc, 'rs')]:
        if not compiler:
            result['builds'].append({'language': language, 'state': 'unavailable'})
            continue
        # rustc may be a rustup proxy: preserve argv[0], including the symlink name.
        compiler = str(Path(compiler).absolute())
        library_path = out / f'{language}.so'
        source = ROOT / f'scripts/benchmarks/language_kernels.{extension}'
        command = ([compiler, '-O3', '-march=native', '-std=c++17', '-shared', '-fPIC',
                    '-ffp-contract=off', '-fno-fast-math', '-fno-math-errno', str(source), '-o', str(library_path)] if language == 'cpp' else
                   [compiler, '--edition=2021', '--crate-type=cdylib', '-C', 'opt-level=3',
                    '-C', 'target-cpu=native', '-C', 'codegen-units=1', str(source), '-o', str(library_path)])
        event(f'Building {language} candidate')
        began, cpu = time.perf_counter(), cpu_seconds()
        built = subprocess.run(command, capture_output=True, text=True, timeout=180)
        build_wall, build_cpu = time.perf_counter() - began, cpu_seconds() - cpu
        (out / f'{language}_build.log').write_text(built.stdout + built.stderr)
        if built.returncode:
            raise RuntimeError(f'{language} compilation failed; see {out / (language + "_build.log")}')
        result['builds'].append({'language': language, 'state': 'built', 'command': command,
            'compiler': subprocess.check_output([compiler, '--version'], text=True).splitlines()[0],
            'wall_s': build_wall, 'cpu_s': build_cpu, 'binary_sha256': sha(library_path)})
        library = ctypes.CDLL(str(library_path))
        library.limits.argtypes = [pointer_float, pointer_int, pointer_float, ctypes.c_size_t,
                                    ctypes.c_int64, ctypes.c_int64, ctypes.c_double]
        library.limits.restype = None
        library.moments.argtypes = [pointer_float, pointer_float, pointer_float, ctypes.c_size_t, ctypes.c_size_t]
        library.moments.restype = None
        native[language] = library

        def native_limits(p, d, r=1.10, library=library):
            output = np.empty(len(p))
            library.limits(p, d, output, len(p), tick_cutoff, ratio_cutoff, r)
            return (output,)

        def native_moments(p, d, r=1.10, library=library):
            mean, std = np.empty(len(p)), np.empty(len(p))
            library.moments(p, mean, std, len(p), args.window)
            return mean, std

        candidates['limits'][language] = native_limits
        candidates['moments'][language] = native_moments

    def node_call(kind, p, d, *, repeats=0, ratio=1.10):
        # This bridge is deliberately timed in full workflows, including startup/copies.
        p.tofile(out / 'node_input.bin')
        if kind == 'limits':
            d.tofile(out / 'node_dates.bin')
        command = [args.node, str(ROOT / 'scripts/benchmarks/language_kernels.mjs'), kind,
                   str(out / 'node_input.bin'), str(out / 'node_dates.bin'), str(out / 'node_output.bin'),
                   str(out / 'node_meta.json'), str(tick_cutoff), str(ratio_cutoff), str(args.window), str(repeats), str(ratio)]
        subprocess.run(command, check=True, capture_output=True, text=True, timeout=180)
        output = np.fromfile(out / 'node_output.bin', dtype=np.float64)
        parts = (output,) if kind == 'limits' else tuple(np.split(output, 2))
        return parts, json.loads((out / 'node_meta.json').read_text())

    result['jit_setup'] = {}
    for name, function in [('numba_limits', lambda: candidates['limits']['numba'](prices[:8], dates[:8])),
                           ('numba_moments', lambda: compiled_moments(values[:8], args.window))]:
        began, cpu = time.perf_counter(), cpu_seconds()
        function()
        result['jit_setup'][name] = {'wall_s': time.perf_counter() - began, 'cpu_s': cpu_seconds() - cpu,
            'scope': 'first invocation in this process; existing persistent Numba disk cache retained'}
    # Independent edge corpus is not appended to historical observations.
    edge_prices = np.array([0., -0., -1., np.nan, np.inf, -np.inf, .01, 4.999, 5.,
                           9.999, 10., 14.999, 15., 49.999, 50., 99.999, 100., 149.999, 150., 499.999, 500., 999.999, 1000., 524.])
    for day in [tick_cutoff - 1, tick_cutoff, ratio_cutoff - 1, ratio_cutoff]:
        edge_dates = np.full(len(edge_prices), day, dtype=np.int64)
        for ratio in [.90, 1.10]:
            expected = candidates['limits']['numba'](edge_prices, edge_dates, ratio)
            for function in candidates['limits'].values():
                verify_arrays(function(edge_prices, edge_dates, ratio), expected)
            if args.node:
                verify_arrays(node_call('limits', edge_prices, edge_dates, ratio=ratio)[0], expected)
    for edge in [np.array([], dtype=np.float64), np.array([np.nan]), np.array([1., np.nan, -1., 0., np.inf]),
                 np.tile(np.array([.1, -.1, np.nan, .2, -.2]), args.window + 1)]:
        expected = candidates['moments']['numpy'](edge, None)
        for function in candidates['moments'].values():
            verify_arrays(function(edge, None), expected)
        if args.node:
            verify_arrays(node_call('moments', edge, None)[0], expected)
    result['edge_checks'] = {'limit_cases_per_backend': len(edge_prices) * 8,
                              'rolling_lengths': [0, 1, 5, 5 * (args.window + 1)],
                              'contract': 'canonical bit equality except NaN payload; independent edge corpus'}
    event('Canonical historical-rule, invalid-value and rolling-boundary checks passed')

    data = {'limits': (prices, dates), 'moments': (values, None)}
    oracles = {}
    for kind, (p, d) in data.items():
        oracle = candidates[kind]['numba' if kind == 'limits' else 'numpy'](p, d)
        oracles[kind] = oracle
        records = {}
        for name, function in candidates[kind].items():
            began = time.perf_counter()
            output = function(p, d)
            first = time.perf_counter() - began
            records[name] = {'first_actual_shape_call_s': first, 'parity': verify_arrays(output, oracle), 'samples': []}
        for round_index in range(args.repeats):
            event(f'{kind}: resident-kernel round {round_index + 1}/{args.repeats}')
            for name in balanced_order(list(records), round_index):
                output, sample = measure(lambda: candidates[kind][name](p, d), target_cpu_s=args.target_cpu_ms / 1000.)
                verify_arrays(output, oracle)
                records[name]['samples'].append({'round': round_index, **sample})
        if args.node:
            output, metadata = node_call(kind, p, d, repeats=args.repeats)
            records['javascript'] = {'samples': metadata['samples'], 'node_metadata': metadata,
                                     'parity': verify_arrays(output, oracle),
                                     'scope': 'resident Node kernel; separate process, no IPC/startup in these samples'}
        for record in records.values():
            record['summary'] = summarize(record['samples'])
        result['kernels'][kind] = records
        atomic_json(out / 'partial.json', result)

    def workflow(kind, name):
        wall, cpu, stage_start = time.perf_counter(), cpu_seconds(), time.perf_counter()
        table = pq.read_table(out / ('prices.parquet' if kind == 'limits' else 'returns.parquet'))
        p = np.ascontiguousarray(table['value'].to_numpy(), dtype=np.float64)
        d = np.ascontiguousarray(table['date_ordinal'].to_numpy(), dtype=np.int64) if kind == 'limits' else None
        stages = {'read_decode_s': time.perf_counter() - stage_start}
        stage_start = time.perf_counter()
        output = node_call(kind, p, d)[0] if name == 'javascript' else candidates[kind][name](p, d)
        stages['compute_adapter_s'] = time.perf_counter() - stage_start
        stage_start = time.perf_counter()
        path = out / f'{kind}_{name}_output.parquet'
        pq.write_table(pa.table({f'out{i}': array for i, array in enumerate(output)}), path, compression='zstd')
        with path.open('rb') as stream:
            os.fsync(stream.fileno())
        stages['write_fsync_s'] = time.perf_counter() - stage_start
        stage_start = time.perf_counter()
        readback = pq.read_table(path)
        verify_arrays(tuple(readback[f'out{i}'].to_numpy() for i in range(len(output))), oracles[kind])
        stages['readback_verify_s'] = time.perf_counter() - stage_start
        return {'wall_s': time.perf_counter() - wall, 'cpu_s': cpu_seconds() - cpu, **stages}

    for kind in data:
        names = [*candidates[kind], *(['javascript'] if args.node else [])]
        records = {name: {'samples': []} for name in names}
        for round_index in range(args.repeats):
            event(f'{kind}: complete read/compute/write/fsync/readback round {round_index + 1}/{args.repeats}')
            for name in balanced_order(names, round_index):
                records[name]['samples'].append({'round': round_index, **workflow(kind, name)})
        for name, record in records.items():
            record['summary'] = summarize(record['samples'])
            record['stage_median_s'] = {field: statistics.median(row[field] for row in record['samples'])
                                       for field in ('read_decode_s', 'compute_adapter_s', 'write_fsync_s', 'readback_verify_s')}
            record['output_file_sha256'] = sha(out / f'{kind}_{name}_output.parquet')
        result['workflows'][kind] = records
        atomic_json(out / 'partial.json', result)

    event('Measuring Arrow C++ / Polars Rust Parquet workflows')
    query_path = out / 'query.parquet'
    columns = ['date', 'product', 'contract', 'session', 'open', 'high', 'low', 'close', 'volume']
    start_date = datetime(2020, 1, 1).date()
    query_schema = pa.schema([pq.read_schema(query_path).field(name) for name in columns])
    # Raw official sessions retain Chinese labels; share the canonical alias set.
    sessions = sorted(_REGULAR_SESSION_ALIASES)
    filters = [('date', '>=', start_date), ('product', '==', 'TX'), ('session', 'in', sessions)]

    def query(engine):
        if engine == 'arrow_full':
            table = pq.read_table(query_path)
            mask = pc.and_(pc.and_(pc.greater_equal(table['date'], pa.scalar(start_date)),
                                  pc.equal(table['product'], 'TX')), pc.is_in(table['session'], value_set=pa.array(sessions)))
            table = table.filter(mask).select(columns)
        elif engine == 'arrow_projected':
            table = pq.read_table(query_path, columns=columns, filters=filters)
        else:
            table = (pl.scan_parquet(query_path).filter((pl.col('date') >= start_date) &
                (pl.col('product') == 'TX') & pl.col('session').is_in(sessions)).select(columns).collect().to_arrow())
        return table.cast(query_schema).sort_by([(name, 'ascending') for name in ['date', 'product', 'contract', 'session']])

    query_expected = query('arrow_full')
    query_records = {name: {'samples': []} for name in ('arrow_full', 'arrow_projected', 'polars_lazy')}
    if not query_expected.num_rows:
        raise ValueError('empty Parquet query is not a useful benchmark')
    for round_index in range(args.repeats):
        for name in balanced_order(list(query_records), round_index):
            wall, cpu = time.perf_counter(), cpu_seconds()
            table = query(name)
            read_compute = time.perf_counter() - wall
            path = out / f'query_{name}_output.parquet'
            pq.write_table(table, path, compression='zstd')
            with path.open('rb') as stream:
                os.fsync(stream.fileno())
            if not pq.read_table(path).equals(query_expected, check_metadata=False):
                raise AssertionError(f'{name} query readback differs from full Arrow oracle')
            query_records[name]['samples'].append({'round': round_index, 'wall_s': time.perf_counter() - wall,
                'cpu_s': cpu_seconds() - cpu, 'read_filter_sort_s': read_compute})
        event(f'Parquet query round {round_index + 1}/{args.repeats} passed')
    for record in query_records.values():
        record['summary'] = summarize(record['samples'])
        record['output_rows'] = query_expected.num_rows
        record['exact_table_parity'] = True
    result['queries'] = {'source_rows': pq.read_metadata(query_path).num_rows,
                         'columns': columns, 'filters': [str(item) for item in filters], 'engines': query_records}

    # Same C ABI and output, varying only call granularity. No separate process per row.
    count = min(4096, len(prices))
    for name in native:
        function = candidates['limits'][name]
        bulk_expected = function(prices[:count], dates[:count])

        def per_row():
            output = np.empty(count)
            for i in range(count):
                output[i] = function(prices[i:i + 1], dates[i:i + 1])[0][0]
            return (output,)

        records = {'bulk': [], 'per_row': []}
        for round_index in range(args.repeats):
            for mode in balanced_order(list(records), round_index):
                function_call = per_row if mode == 'per_row' else lambda: function(prices[:count], dates[:count])
                output, sample = measure(function_call, target_cpu_s=args.target_cpu_ms / 1000.)
                verify_arrays(output, bulk_expected)
                records[mode].append(sample)
        result['boundary'][name] = {'rows': count,
            **{mode: {'samples': samples, 'summary': summarize(samples)} for mode, samples in records.items()}}
    event('Bulk versus per-row native calls passed exact output checks')
    for name, expected_sha in result['inputs']['snapshots'].items():
        if sha(out / name) != expected_sha:
            raise RuntimeError(f'input snapshot changed: {name}')
    if {str(path.relative_to(ROOT)): sha(path) for path in code_paths} != code:
        raise RuntimeError('benchmark/canonical source changed during measurement')
    result['node_after'] = observe_node(out / 'prices.parquet', out)
    result['parent_process_cumulative_peak_rss_kib'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    result['complete_benchmark_wall_s'] = time.perf_counter() - start
    result['state'] = 'accepted_bounded_language_benchmark'
    result['limitations'] = [
        'Concurrent host services; wall-time ranges and CPU time are both retained. No exclusive-host or disk-cold claim.',
        'Core samples include output allocations; C++/Rust use bulk ctypes, NumPy/Numba use Python adapters.',
        'JavaScript core excludes its IPC/startup; full workflow includes a fresh Node process and binary copies.',
        'All source decode/write/readback checks are in bounded workflows, not a full historical panel/training fold.',
        'No full ledger, autograd, GPU kernel, model quality, remote-node speed or production deployment acceptance.',
        'Installed Rust toolchain acquisition is separate from recorded per-candidate compilation.',
    ]
    atomic_json(out / 'result.json', result)
    event(f'Accepted bounded measurements: {out / "result.json"}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
