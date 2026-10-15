"""Cooperative ingestion of immutable news metadata, with one queue writer.

A checkpoint and its bounded group of original4,096-row updates commit together. The
receipt head is published only after every row has been imported. A stopped
owner resumes from the durable cursor; ordinary HTTP dispatch can run between
chunks. No source requests, secondary SQLite writer or artificial source clock.
"""
from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import time

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import sha256_file

METADATA_INGEST_CONTRACT_VERSION = 2
METADATA_BATCH_ROWS = 4096  # Preserve the original duplicate/title boundary.
METADATA_COMMIT_MAX_BATCHES = 4
METADATA_COMMIT_SOFT_SECONDS = 0.75
METADATA_SLICE_SECONDS = 0.05
METADATA_SLICE_INTERVAL_SECONDS = 1.0
METADATA_TEMPORARY_CACHE_KIB = 8192  # Full real-receipt measurement, not permanent RAM.


def _borrow_cache(queue, reader):
    borrowers = getattr(queue, '_metadata_cache_borrowers', None)
    if borrowers is None:
        borrowers = queue._metadata_cache_borrowers = set()
    if reader in borrowers:
        return
    if not borrowers:
        original = queue.db.execute('PRAGMA cache_size').fetchone()[0]
        queue._metadata_cache_original = original
        kib = abs(original) if original < 0 else original * queue.db.execute('PRAGMA page_size').fetchone()[0] / 1024
        # Respect a deliberately larger caller cache. Only borrow a measured,
        # small working set when the original cache is smaller.
        if kib < METADATA_TEMPORARY_CACHE_KIB:
            queue.db.execute(f'PRAGMA cache_size=-{METADATA_TEMPORARY_CACHE_KIB}')
    borrowers.add(reader)


def _release_cache(queue, reader):
    borrowers = getattr(queue, '_metadata_cache_borrowers', set())
    if reader not in borrowers:
        return
    borrowers.remove(reader)
    if not borrowers:
        queue.db.execute(f'PRAGMA cache_size={int(queue._metadata_cache_original)}')
        del queue._metadata_cache_original


def _signature(path):
    stat = path.stat()
    return stat.st_mtime_ns, stat.st_size


class ReceiptReader:
    def __init__(self, queue, root, receipt_path, provider, link_column):
        from downloader.news_fulltext_queue import source_path, utc_now
        self.queue = queue
        self.path = str(receipt_path.resolve())
        self.signature = _signature(receipt_path)
        receipt = json.loads(receipt_path.read_text())
        self.parquet = source_path(root, receipt['parquet_path'])
        self.parquet_signature = _signature(self.parquet)
        self.origin_key = self.path + ':' + receipt['sha256']
        # Revalidate the full immutable file on each new reader, including
        # restart. A resumed in-memory reader keeps only one Arrow batch.
        if sha256_file(self.parquet) != receipt['sha256']:
            raise ValueError('metadata_sha256_mismatch')
        self.file = pq.ParquetFile(self.parquet)
        if (self.file.metadata.num_rows != receipt['rows']
                or link_column not in self.file.schema_arrow.names):
            self.close()
            raise ValueError('metadata_schema_or_rows_mismatch')
        self.expected_rows = receipt['rows']
        self.link_column = link_column
        columns = [c for c in (link_column, 'title', 'date') if c in self.file.schema_arrow.names]
        with queue.db:
            origin = queue.db.execute('SELECT id FROM origins WHERE origin_key=?', (self.origin_key,)).fetchone()
            if origin:
                self.origin_id = origin[0]
                progress = queue.db.execute('SELECT processed_rows,expected_rows,link_column FROM metadata_ingest_progress '
                                            'WHERE origin_id=?', (self.origin_id,)).fetchone()
                if not progress:
                    self.close()
                    raise ValueError('complete_receipt_reader_not_required')
                self.processed_rows = progress[0]
                if (progress[1:] != (self.expected_rows, link_column)
                        or not 0 <= self.processed_rows <= self.expected_rows
                        or self.processed_rows != self.expected_rows and self.processed_rows % METADATA_BATCH_ROWS):
                    self.close()
                    raise ValueError('metadata_cursor_or_scope_mismatch')
                queue.db.execute("UPDATE metadata_ingest_progress SET state='pending' WHERE origin_id=?", (self.origin_id,))
            else:
                cursor = queue.db.execute('INSERT INTO origins(origin_key,provider,receipt_path,sha256,rows,imported_at_utc,parquet_path) '
                                          'VALUES (?,?,?,?,?,?,?)', (self.origin_key, provider, self.path, receipt['sha256'],
                                                                    0, utc_now(), str(self.parquet)))
                self.origin_id, self.processed_rows = cursor.lastrowid, 0
                queue.db.execute('INSERT INTO metadata_ingest_progress VALUES (?,?,?,?,?)',
                                 (self.origin_id, self.expected_rows, 0, link_column, 'pending'))
            # Keep old partial versions as evidence, never call them complete.
            queue.db.execute("UPDATE metadata_ingest_progress SET state='superseded' WHERE origin_id IN "
                             '(SELECT id FROM origins WHERE receipt_path=? AND id!=?)', (self.path, self.origin_id))
        self.batches = self.file.iter_batches(batch_size=METADATA_BATCH_ROWS, columns=columns)
        skipped = 0
        while skipped < self.processed_rows:
            batch = next(self.batches, None)
            if batch is None:
                self.close()
                raise ValueError('metadata_cursor_exceeds_original_batches')
            skipped += batch.num_rows
        if skipped != self.processed_rows:
            self.close()
            raise ValueError('metadata_cursor_not_on_original_batch_boundary')

    def unchanged(self):
        return (_signature(Path(self.path)) == self.signature
                and _signature(self.parquet) == self.parquet_signature)

    def close(self):
        if getattr(self, 'file', None) is not None:
            self.file.close()
        _release_cache(self.queue, self)


def close_readers(queue):
    for reader in getattr(queue, '_metadata_receipt_readers', {}).values():
        reader.close()
    queue._metadata_receipt_readers = {}


def ingest_receipt(queue, root, receipt_path, *, provider, link_column, maximum_wall_seconds=None, stop=None):
    """Legacy bool result; ``metadata_receipt_pending`` distinguishes a yield.

    No deadline means complete the receipt as before. With a deadline the
    indivisible unit is one short SQLite transaction: at most4 original Arrow
    batches, or the .75s cooperative write window. Check stop between logical
    batches. The deadline never promises to interrupt a batch, SQL or fsync.
    """
    from downloader.news_fulltext_queue import utc_now
    queue.metadata_receipt_pending = False
    if stop is not None and stop.is_set():
        queue.metadata_receipt_pending = True
        return False
    deadline = time.monotonic() + maximum_wall_seconds if maximum_wall_seconds is not None else None
    readers = getattr(queue, '_metadata_receipt_readers', None)
    if readers is None:
        readers = queue._metadata_receipt_readers = {}
    path = str(receipt_path.resolve())
    reader = readers.get(path)
    try:
        signature = _signature(receipt_path)
        previous = queue.db.execute('SELECT mtime_ns,bytes FROM receipt_heads WHERE path=?', (path,)).fetchone()
        if previous and tuple(previous) == signature:
            return False
        if reader and not reader.unchanged():
            reader.close()
            readers.pop(path)
            reader = None
        if reader is None:
            receipt = json.loads(receipt_path.read_text())
            origin_key = path + ':' + receipt['sha256']
            origin = queue.db.execute('SELECT id FROM origins WHERE origin_key=?', (origin_key,)).fetchone()
            if origin and not queue.db.execute('SELECT 1 FROM metadata_ingest_progress WHERE origin_id=?', (origin[0],)).fetchone():
                with queue.db:
                    queue.db.execute('INSERT OR REPLACE INTO receipt_heads VALUES (?,?,?)', (path, *signature))
                return False
            reader = readers[path] = ReceiptReader(queue, root, receipt_path, provider, link_column)
        while True:
            if stop is not None and stop.is_set() or deadline is not None and time.monotonic() >= deadline:
                queue.metadata_receipt_pending = True
                return False
            if not reader.unchanged():
                reader.close()
                readers.pop(path)
                queue.metadata_receipt_pending = True
                return False
            group_started = time.monotonic()
            processed, invalid, batches, completed = reader.processed_rows, 0, 0, False
            with queue.db:
                while (batches < METADATA_COMMIT_MAX_BATCHES
                       and time.monotonic() - group_started < METADATA_COMMIT_SOFT_SECONDS
                       and (stop is None or not stop.is_set())):
                    batch = next(reader.batches, None)
                    if batch is None:
                        if processed != reader.expected_rows:
                            raise ValueError('metadata_processed_rows_mismatch')
                        completed = True
                        break
                    rows = [{'url': r.get(link_column), 'title': r.get('title'), 'date': r.get('date')}
                            for r in batch.to_pylist()]
                    _borrow_cache(queue, reader)
                    invalid += queue.add_articles(rows, origin_id=reader.origin_id)[1]
                    processed += batch.num_rows
                    batches += 1
                if batches:
                    queue.db.execute('UPDATE origins SET rows=?,invalid_urls=invalid_urls+? WHERE id=?',
                                     (processed, invalid, reader.origin_id))
                    queue.db.execute('UPDATE metadata_ingest_progress SET processed_rows=? WHERE origin_id=?',
                                     (processed, reader.origin_id))
                if completed:
                    queue.db.execute('DELETE FROM metadata_ingest_progress WHERE origin_id=?', (reader.origin_id,))
                    queue.db.execute('DELETE FROM origin_errors WHERE receipt_path=?', (path,))
                    queue.db.execute('INSERT OR REPLACE INTO receipt_heads VALUES (?,?,?)', (path, *reader.signature))
            queue.metrics.count(provider, 'metadata_imported_rows', processed - reader.processed_rows)
            queue.metrics.count(provider, 'metadata_imported_batches', batches)
            queue.metrics.count(provider, 'metadata_imported_commit_groups', int(batches > 0))
            reader.processed_rows = processed
            if completed:
                reader.close()
                readers.pop(path)
                return True
    except (OSError, ValueError, KeyError, pa.ArrowException) as exc:
        if reader:
            reader.close()
            readers.pop(path, None)
        with queue.db:
            queue.db.execute('INSERT OR REPLACE INTO origin_errors VALUES (?,?,?)',
                             (path, type(exc).__name__ + ':' + str(exc)[:160], utc_now()))
            queue.db.execute("UPDATE metadata_ingest_progress SET state='error' WHERE origin_id IN "
                             '(SELECT id FROM origins WHERE receipt_path=?) AND state=\'pending\' ', (path,))
        return False
    except BaseException:
        # The Arrow iterator moved before SQL completed. Discard it after a
        # rollback so a same-owner retry also uses the durable row cursor.
        if reader:
            reader.close()
            readers.pop(path, None)
        raise


class LocalInventory:
    def __init__(self, queue, finlab_root, finmind_root):
        self.queue, self.finmind_root = queue, finmind_root
        self.imported, self.checked, self.current = 0, 0, None
        self.last_status = time.monotonic()
        self.finlab = iter(sorted((finlab_root / 'receipts').glob('tw_news*.json')))
        self.finlab_root = finlab_root
        self.phase = 'finlab'
        self.finmind = []
        source_db = finmind_root / 'queue.sqlite3'
        if source_db.is_file():
            source = sqlite3.connect(f'file:{source_db.resolve()}?mode=ro', uri=True, timeout=10)
            try:
                self.finmind = source.execute("SELECT receipt_path FROM tasks WHERE dataset='TaiwanStockNews' "
                    "AND state='complete' AND receipt_path IS NOT NULL ORDER BY partition DESC").fetchall()
            finally:
                source.close()
            with queue.db:
                queue.gauge('latest_inventory_finmind_receipts', len(self.finmind))
                queue.gauge('latest_inventory_finmind_receipts_checked', 0)
        self.finmind_paths = iter(self.finmind)

    def advance(self, *, maximum_wall_seconds=None, stop=None):
        from downloader.news_fulltext_queue import source_path, utc_now
        deadline = time.monotonic() + maximum_wall_seconds if maximum_wall_seconds is not None else None
        imported = 0
        while stop is None or not stop.is_set():
            if deadline is not None and time.monotonic() >= deadline:
                break
            if self.current is None:
                if self.phase == 'finlab':
                    path = next(self.finlab, None)
                    if path is None:
                        self.phase = 'finmind'
                        continue
                    self.current = (self.finlab_root, path, 'finlab', 'url')
                else:
                    relative = next(self.finmind_paths, None)
                    if relative is None:
                        return {'imported': imported, 'complete': True}
                    try:
                        path = source_path(self.finmind_root, relative[0])
                    except ValueError:
                        with self.queue.db:
                            self.queue.db.execute('INSERT OR REPLACE INTO origin_errors VALUES (?,?,?)',
                                (str(self.finmind_root / relative[0]), 'missing_accepted_receipt', utc_now()))
                        self.checked += 1
                        continue
                    self.current = (self.finmind_root, path, 'finmind', 'link')
            root, path, provider, column = self.current
            remaining = max(0, deadline - time.monotonic()) if deadline is not None else None
            imported += int(self.queue.ingest_receipt(root, path, provider=provider, link_column=column,
                                                     maximum_wall_seconds=remaining, stop=stop))
            if getattr(self.queue, 'metadata_receipt_pending', False):
                break
            self.current = None
            if self.phase == 'finmind':
                self.checked += 1
                now = time.monotonic()
                if self.checked % 100 == 0 or self.checked == len(self.finmind) or now - self.last_status >= 15:
                    with self.queue.db:
                        self.queue.gauge('latest_inventory_finmind_receipts_checked', self.checked)
                    if self.checked == len(self.finmind) or now - self.last_status >= 15:
                        with self.queue.metrics.measure('metadata_inventory_status'):
                            self.queue.status(activity='inventorying')
                        self.last_status = time.monotonic()
        return {'imported': imported, 'complete': False}


def ingest_local_slice(queue, finlab_root, finmind_root, *, maximum_wall_seconds=None, stop=None):
    inventory = getattr(queue, '_metadata_local_inventory', None)
    if inventory is None:
        inventory = queue._metadata_local_inventory = LocalInventory(queue, finlab_root, finmind_root)
    result = inventory.advance(maximum_wall_seconds=maximum_wall_seconds, stop=stop)
    queue.metadata_inventory_pending = not result['complete']
    if result['complete']:
        queue._metadata_local_inventory = None
    return result
