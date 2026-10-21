"""Bounded receipt I/O only; JSON, SQLite and replay stay on the owner thread."""

from collections import deque
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from pathlib import PosixPath


RECEIPT_CHUNK = 128
RECEIPT_CHUNK_BYTES = 1 << 20
RECEIPT_PROBE_STRIDE = 16
RECEIPT_PREFETCH_MAX_CHUNKS = 16
ADAPTIVE_RECEIPT_STRATEGY_VERSION = 2
RECEIPT_PREFIX_IO_VERSION = 2


def receipt_parent_matches(path, directory):
    """Exact lexical parent equality, without allocating another POSIX Path.

    This does not resolve symlinks or prove a file valid. Concrete standard
    POSIX paths have case-sensitive component equality; anchors and the empty
    path are their own parent. Other flavours/subclasses retain their original
    parent/equality implementation, including Windows case folding.
    """
    if type(path) is PosixPath and type(directory) is PosixPath:
        parts = path.parts
        return (parts[:-1] if path.name else parts) == directory.parts
    return path.parent == directory


def adaptive_receipt_readers(probe):
    """Overlap clearly wait-bound reads, not the warmed CPU-bound path.

    Sample one complete 128-input chunk per 2048 synchronous receipts. After
    each bounded prefetch slice the owner joins readers and samples again,
    so neither a warm prefix nor a cold prefix fixes the remaining I/O mode.
    Descheduling is included in wall wait, not attributed to disk alone.
    """
    return (2 if probe['receipts'] == RECEIPT_CHUNK and probe['wall_seconds'] >= .02
        and probe['cpu_seconds'] <= .65 * probe['wall_seconds'] else 0)


def _chunks(paths):
    remaining = iter(paths)
    while chunk := list(islice(remaining, RECEIPT_CHUNK)):
        yield chunk


def _read_prefix(path, limit):
    """Read the same bounded bytes without a per-file BufferedReader.

    Raw reads may be short without EOF. Only an empty read ends the input;
    later errors propagate instead of accepting a successfully read prefix.
    Context-managed FileIO preserves path/open/close failures and subclasses.
    """
    with path.open('rb', buffering=0) as stream:
        first = stream.read(limit)
        remaining = limit - len(first)
        if not remaining or not first:
            return first
        parts = [first]
        while remaining:
            part = stream.read(remaining)
            if part == b'':
                break
            parts.append(part)
            remaining -= len(part)
        return first if len(parts) == 1 else b''.join(parts)


def _read_chunk(paths, root=None):
    """One future per SQL-sized batch, not one future per tiny receipt.

    A batch owns at most one MiB of raw input. Oversized inputs are read in
    full by the original owner, never truncated or considered valid by size.
    Preserve a read exception at its path so earlier receipts still replay
    in the original order before that exception is raised on the owner.
    """
    result, remaining = [], RECEIPT_CHUNK_BYTES
    receipt_directory = root / 'receipts' if root is not None else None
    for path in paths:
        raw = None
        if remaining:
            try:
                if root is not None and not receipt_parent_matches(path, receipt_directory):
                    path.relative_to(root)  # Guard explicit paths before any speculative I/O.
                candidate = _read_prefix(path, remaining + 1)
            except Exception as error:
                result.append((path, None, error))
                result.extend((later, None, None) for later in paths[len(result):])
                return result
            if len(candidate) <= remaining:
                raw = candidate
                remaining -= len(candidate)
            else:
                remaining = 0
        result.append((path, raw, None))
    return result


def receipt_input_chunks(paths, *, readers=0, root=None):
    """Yield ordered inputs with at most two read-only batches in flight.

    Zero readers retains synchronous original reads. Close this iterator on
    owner errors: pending work is cancelled and all running readers joined.
    Paths are consumed incrementally, not submitted as an unbounded map.
    """
    if type(readers) is not int or not 0 <= readers <= 2:
        raise ValueError('bounded_batched_receipt_readers_required')
    chunks = _chunks(paths)
    if not readers:
        for chunk in chunks:
            yield [(path, None, None) for path in chunk]
        return
    pool = ThreadPoolExecutor(max_workers=readers, thread_name_prefix='news-receipt-batch')
    pending = deque()
    try:
        for _ in range(readers):
            chunk = next(chunks, None)
            if chunk is not None:
                pending.append(pool.submit(_read_chunk, chunk, root) if root is not None
                    else pool.submit(_read_chunk, chunk))
        while pending:
            result = pending.popleft().result()
            following = next(chunks, None)
            if following is not None:
                pending.append(pool.submit(_read_chunk, following, root) if root is not None
                    else pool.submit(_read_chunk, following))
            yield result
    finally:
        for future in pending:
            future.cancel()
        pool.shutdown(wait=True, cancel_futures=True)
