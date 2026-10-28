"""Reuse FRED's existing typed service view, never create a second raw cache.

The summary seals the normalizer, Parquet, exact window receipts and original
encoded content. A changed source/control generation falls back to raw parsing
before use, or fails the collector transaction if it changes after use.
"""
from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import struct
import sys
from types import CodeType

import polars as pl

CONTRACT = 'fred_closed_projection_v1'
MAX_FRAME_BYTES = 64 * 1024**2
MAX_FRAME_ROWS = 1_000_000


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _identity(path: Path) -> tuple:
    path = path.absolute()
    info = path.lstat()
    if (not stat.S_ISREG(info.st_mode) or path.resolve(strict=True) != path
            or info.st_nlink != 1 or info.st_uid != os.geteuid()):
        raise ValueError('projection source must be an owned regular original')
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
            info.st_ctime_ns, info.st_mode, info.st_nlink)


def _read(path: Path, maximum_bytes: int) -> tuple[bytes, tuple]:
    before = _identity(path)
    if before[2] > maximum_bytes:
        raise ValueError('projection input exceeds its read admission')
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    with os.fdopen(descriptor, 'rb') as stream:
        held = os.fstat(stream.fileno())
        if (held.st_dev, held.st_ino) != before[:2]:
            raise ValueError('projection input changed before opening')
        raw = stream.read(before[2] + 1)
    if len(raw) != before[2] or _identity(path) != before:
        raise ValueError('projection input changed during reading')
    return raw, before


def _code_constant(value):
    if isinstance(value, CodeType):
        names = ('co_argcount', 'co_posonlyargcount', 'co_kwonlyargcount', 'co_nlocals',
            'co_stacksize', 'co_flags', 'co_code', 'co_consts', 'co_names', 'co_varnames',
            'co_freevars', 'co_cellvars', 'co_exceptiontable', 'co_name', 'co_qualname')
        return ['code', {name: _code_constant(getattr(value, name)) for name in names}]
    if value is None or type(value) in (bool, int, str):
        return [type(value).__name__, value]
    if value is Ellipsis:
        return ['ellipsis']
    if type(value) is bytes:
        return ['bytes', value.hex()]
    if type(value) is float:
        return ['float64', struct.pack('>d', value).hex()]
    if type(value) is complex:
        return ['complex', _code_constant(value.real), _code_constant(value.imag)]
    if type(value) is tuple:
        return ['tuple', [_code_constant(item) for item in value]]
    if type(value) is frozenset:
        return ['frozenset', sorted((_code_constant(item) for item in value),
            key=lambda item: json.dumps(item, sort_keys=True, ensure_ascii=False))]
    if type(value) is slice:
        return ['slice', *(_code_constant(item) for item in (value.start, value.stop, value.step))]
    raise ValueError('unsupported loaded normalizer code constant')


def normalizer_identity(functions, *, parameters: dict) -> str:
    # Canonical loaded-code fields ignore transient reference counts, filename
    # and hash-seed ordering. Never hash marshal's object-sharing encoding.
    owned = (_sha, _identity, _read, normalizer_identity, _rows_sha, _key, _raw_path,
        _groups, ClosedProjection.load.__func__, ClosedProjection.lookup,
        ClosedProjection.check_generation, build_manifest, _code_constant)
    document = {'contract': CONTRACT, 'parameters': parameters,
        'code_identity_contract': 'python_loaded_code_fields_v1',
        'loaded_code': {fn.__qualname__: _code_constant(fn.__code__) for fn in (*functions, *owned)},
        'polars_version': pl.__version__, 'python_version': sys.version,
        'maximum_frame_bytes': MAX_FRAME_BYTES, 'maximum_frame_rows': MAX_FRAME_ROWS}
    return _sha(json.dumps(document, sort_keys=True).encode())


def _rows_sha(rows: list[dict]) -> str:
    # Preserve all scalar fields and negative/zero values. Reject non-finite
    # projections; the original parser remains the fallback for those inputs.
    encoded = sorted(json.dumps(row, ensure_ascii=False, sort_keys=True,
        separators=(',', ':'), allow_nan=False).encode() for row in rows)
    return _sha(b'\n'.join(encoded))


def _key(window) -> str:
    return f'{window.series_id}:{window.realtime_start}:{window.realtime_end}'


def _raw_path(output: Path, name: str) -> Path:
    relative = Path(name)
    if (not name.startswith('raw/') or relative.is_absolute() or '..' in relative.parts
            or relative.as_posix() != name):
        raise ValueError('projection raw path is not a canonical source-relative name')
    return output / relative


def _groups(frame: pl.DataFrame) -> dict:
    return {key: part.to_dicts() for key, part in frame.partition_by(
        ['series_id', 'raw_sha256'], as_dict=True, maintain_order=True).items()}


class ClosedProjection:
    def __init__(self, output, records, groups, guards):
        self.output, self.records, self.groups, self.guards = output, records, groups, guards
        self.reused_windows = 0

    @classmethod
    def load(cls, output: Path, *, identity: str, schema) -> ClosedProjection | None:
        try:
            summary_raw, _ = _read(output / 'download_summary.json', 2 * 1024**2)
            summary = json.loads(summary_raw)
            contract = summary.get('closed_projection', {})
            records = contract.get('windows')
            if (summary.get('state') != 'complete' or contract.get('contract') != CONTRACT
                    or contract.get('normalizer_identity') != identity or not isinstance(records, dict)
                    or len(records) > 4096):
                return None
            path = output / 'observations.parquet'
            raw, before = _read(path, MAX_FRAME_BYTES)
            if _sha(raw) != summary['output_sha256']:
                return None
            frame = pl.read_parquet(io.BytesIO(raw), n_rows=MAX_FRAME_ROWS + 1)
            if frame.schema != schema or frame.height != summary['rows'] or frame.height > MAX_FRAME_ROWS:
                return None
            return cls(output, records, _groups(frame), {path: before})
        except (OSError, ValueError, TypeError, KeyError, pl.exceptions.PolarsError):
            return None

    def lookup(self, window, receipt_raw: bytes, *, retained_raw=None) -> dict | None:
        try:
            record = self.records.get(_key(window))
            if not isinstance(record, dict) or _sha(receipt_raw) != record['receipt_sha256']:
                return None
            receipt_path = self.output / 'receipts' / window.series_id / f'{window.realtime_start}_{window.realtime_end}.json'
            current, receipt_before = _read(receipt_path, 1024**2)
            if current != receipt_raw:
                return None
            rows = self.groups.get((window.series_id, record['raw_sha256']), [])
            if len(rows) != record['rows'] or _rows_sha(rows) != record['rows_sha256']:
                return None
            path = _raw_path(self.output, record['raw_path'])
            if path.exists() or path.is_symlink():
                before = _identity(path)
                if list(before) != record['raw_signature']:
                    return None
                self.guards[path] = before
            elif (retained_raw is None or retained_raw.encoded_identity(record['raw_path'])
                    != (record['encoded_bytes'], record['encoded_sha256'])):
                return None
            self.guards[receipt_path] = receipt_before
            self.reused_windows += 1
            return {**record, 'rows': rows}
        except (OSError, ValueError, TypeError, KeyError):
            return None

    def check_generation(self):
        if any(_identity(path) != before for path, before in self.guards.items()):
            raise ValueError('FRED projection source/control generation changed during collection')


def build_manifest(output: Path, results, frame: pl.DataFrame, *, identity: str,
                   end_date: str, retained_raw=None) -> dict:
    groups, records = _groups(frame), {}
    for result in results:
        if result.window.realtime_end >= end_date or result.status == 'failed':
            continue
        try:
            rows = groups.get((result.window.series_id, result.raw_sha256), [])
            rows_sha = _rows_sha(rows)
            if len(rows) != len(result.rows) or rows_sha != _rows_sha(result.rows):
                continue  # Overlapping/deduplicated windows have no exact reusable projection.
            receipt_path = output / 'receipts' / result.window.series_id / f'{result.window.realtime_start}_{result.window.realtime_end}.json'
            receipt_raw, _ = _read(receipt_path, 1024**2)
            receipt = json.loads(receipt_raw)
            if (receipt['raw_sha256'] != result.raw_sha256
                    or receipt['rows'] != len(result.rows)):
                continue
            name = str(receipt['raw_path'])
            path = _raw_path(output, name)
            if path.exists() or path.is_symlink():
                encoded, before = _read(path, MAX_FRAME_BYTES)
                if _sha(gzip.decompress(encoded)) != result.raw_sha256:
                    continue
                encoded_identity = len(encoded), _sha(encoded)
            else:
                encoded_identity = retained_raw.encoded_identity(name) if retained_raw is not None else None
                before = None
                if encoded_identity is None:
                    continue
            records[_key(result.window)] = {'raw_path': name, 'raw_sha256': result.raw_sha256,
                'receipt_sha256': _sha(receipt_raw), 'rows': len(rows), 'rows_sha256': rows_sha,
                'encoded_bytes': encoded_identity[0], 'encoded_sha256': encoded_identity[1],
                'raw_signature': list(before) if before is not None else None,
                'retrieved_at_utc': result.retrieved_at_utc, 'point_in_time_gap': result.point_in_time_gap}
        except (OSError, ValueError, TypeError, KeyError, gzip.BadGzipFile, EOFError):
            continue  # Optional projection admission never changes the canonical raw result.
    return {'contract': CONTRACT, 'normalizer_identity': identity, 'windows': records}
