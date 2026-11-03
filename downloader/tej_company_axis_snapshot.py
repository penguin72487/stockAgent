"""Independent reader for the TEJ bridge's staged, read-only company menu.

The binary is a source-menu observation, not an admitted trading universe,
company/date plan, or evidence that any market observation exists. The native
writer makes two ordered reads before the caller can publish a manifest.
"""
from __future__ import annotations

from collections.abc import Iterator
import hashlib
import os
from pathlib import Path
import stat
import struct

MAGIC = 'stockagent_tej_company_axis_binary_v1'
MAX_COUNT = 2_000_000
MAX_BYTES = 256 * 1024 * 1024
MAX_LABEL_BYTES = 16_384  # Native bridge bounds labels at 4,096 UTF-16 units.


def _read_exact(stream, size: int) -> bytes:
    data = stream.read(size)
    if len(data) != size:
        raise ValueError('Truncated TEJ company-axis snapshot')
    return data


def _read_dotnet_string(stream, *, maximum: int = MAX_LABEL_BYTES) -> str:
    # BinaryWriter's non-negative 7-bit encoded Int32 UTF-8 byte count.
    length = 0
    for shift in range(0, 35, 7):
        value = _read_exact(stream, 1)[0]
        if shift == 28 and value > 7:
            raise ValueError('Invalid .NET string length')
        length |= (value & 0x7f) << shift
        if not value & 0x80:
            if shift and value == 0:
                raise ValueError('Non-canonical .NET string length')
            break
    else:
        raise ValueError('Invalid .NET string length')
    if length > maximum:
        raise ValueError('TEJ company-axis string exceeds bound')
    try:
        return _read_exact(stream, length).decode('utf-8', errors='strict')
    except UnicodeDecodeError as exc:
        raise ValueError('Invalid TEJ company-axis UTF-8') from exc


def _labels(stream) -> Iterator[str]:
    if _read_dotnet_string(stream, maximum=128) != MAGIC:
        raise ValueError('Unexpected TEJ snapshot contract')
    count = struct.unpack('<q', _read_exact(stream, 8))[0]
    if not 1 <= count <= MAX_COUNT:
        raise ValueError('TEJ snapshot count outside bound')
    for _ in range(count):
        label = _read_dotnet_string(stream)
        # Python lengths count Unicode scalar values; encoding to UTF-16
        # reproduces the native control's bound including surrogate pairs.
        if not label or len(label.encode('utf-16-le')) // 2 > 4096:
            raise ValueError('TEJ company-axis label outside native bound')
        yield label
    if stream.read(1):
        raise ValueError('TEJ snapshot has trailing bytes')


def _open_bounded(path: Path):
    if path.is_symlink() or not path.is_file() or not 0 < path.stat().st_size <= MAX_BYTES:
        raise ValueError('Bounded regular TEJ snapshot required')
    stream = path.open('rb')
    current = os.fstat(stream.fileno())
    if not stat.S_ISREG(current.st_mode) or not 0 < current.st_size <= MAX_BYTES:
        stream.close()
        raise ValueError('Bounded regular TEJ snapshot required')
    return stream


def iter_snapshot(path: Path) -> Iterator[str]:
    """Validate framing and yield exact display labels in source order."""
    with _open_bounded(Path(path)) as stream:
        yield from _labels(stream)


def verify_snapshot(path: Path) -> dict:
    """Count exact duplicate labels; never silently discard or rewrite them."""
    class DigestReader:
        def __init__(self, stream):
            self.stream, self.digest, self.bytes = stream, hashlib.sha256(), 0

        def read(self, size):
            data = self.stream.read(size)
            self.digest.update(data)
            self.bytes += len(data)
            return data

    seen: set[str] = set()
    count = duplicates = 0
    with _open_bounded(Path(path)) as stream:
        reader = DigestReader(stream)
        for label in _labels(reader):
            count += 1
            if label in seen:
                duplicates += 1
            else:
                seen.add(label)
        if reader.bytes != os.fstat(stream.fileno()).st_size:
            raise ValueError('TEJ snapshot size changed during verification')
    return {'count': count, 'unique_exact_labels': len(seen),
            'duplicate_exact_labels': duplicates, 'sha256': reader.digest.hexdigest(),
            'bytes': reader.bytes, 'full_axis_admitted': False}
