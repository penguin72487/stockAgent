"""Cooperative ingestion of immutable news metadata, with one queue writer.

A checkpoint and its bounded group of original4,096-row updates commit together. The
receipt head is published only after every row has been imported. A stopped
owner resumes from the durable cursor; ordinary HTTP dispatch can run between
chunks. No source requests, secondary SQLite writer or artificial source clock.
"""
from __future__ import annotations

import fnmatch
import hashlib
import json
import os
from collections import Counter
from pathlib import Path
import sqlite3
import stat
import time
import zlib

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import sha256_file

METADATA_INGEST_CONTRACT_VERSION = 7
METADATA_BATCH_ROWS = 4096  # Preserve the original duplicate/title boundary.
METADATA_COMMIT_MAX_BATCHES = 4
METADATA_COMMIT_SOFT_SECONDS = 0.75
METADATA_SLICE_SECONDS = 0.05
METADATA_SLICE_INTERVAL_SECONDS = 1.0
METADATA_TEMPORARY_CACHE_KIB = 8192  # Full real-receipt measurement, not permanent RAM.
METADATA_RECEIPT_IDENTITY_CACHE_LIMIT = 32768
METADATA_FAILED_RECEIPT_RETRY_MAX_SECONDS = 30.0
METADATA_FINMIND_FULL_RESCAN_SECONDS = 180.0
ALIAS_CENSUS_PROOF_CONTRACT = 'metadata_receipt_alias_census_proof_v1'


def _alias_census_proof(census, finmind_root, pending, provisional):
    """Serialize the actual reference observation, without looking at files again."""
    references = sorted([provider, logical, target, list(signature) if signature is not None else None]
                        for (provider, logical), (target, signature) in census['second'].items())
    observed_targets = {reference[2] for reference in references}
    candidates = {old for old, _ in pending.values()} | set(provisional)
    document = {
        'contract': ALIAS_CENSUS_PROOF_CONTRACT,
        'finlab_catalog': list(census['catalog'][0]),
        'finmind_catalog': list(census['catalog'][1]),
        'finmind_root': str(finmind_root.absolute()),
        'references': references,
        'pending': sorted([provider, logical, old, new] for (provider, logical), (old, new) in pending.items()),
        'provisional_targets': sorted(provisional),
        'retired_targets': sorted(candidates - observed_targets),
    }
    raw = json.dumps(document, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode('utf-8')
    return hashlib.sha256(raw).hexdigest(), raw


def read_alias_census_proof(db, digest):
    """Read and validate a stored census; absent legacy evidence is unknown.

    This validates the retained record, not publisher data or an atomic view of
    the independent producer filesystem and databases.
    """
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                  "AND name='metadata_receipt_alias_census_proofs'").fetchone() is None:
        return None
    row = db.execute('SELECT version,raw_bytes,payload_zlib FROM metadata_receipt_alias_census_proofs '
                     'WHERE census_sha256=?', (digest,)).fetchone()
    if row is None:
        return None
    try:
        version, raw_size, compressed = row
        if version != 1 or not isinstance(raw_size, int) or raw_size < 0:
            raise ValueError
        decoder = zlib.decompressobj()
        raw = decoder.decompress(compressed, raw_size + 1)
        if (len(raw) != raw_size or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail
                or hashlib.sha256(raw).hexdigest() != digest):
            raise ValueError
        document = json.loads(raw)
        if document['contract'] != ALIAS_CENSUS_PROOF_CONTRACT:
            raise ValueError
        lab, mind, root = document['finlab_catalog'], document['finmind_catalog'], document['finmind_root']
        if (not isinstance(lab, list) or not isinstance(mind, list) or not isinstance(root, str)
                or not all(isinstance(path, str) and path for path in lab + mind)
                or lab != sorted(lab) or mind != sorted(mind)):
            raise ValueError
        expected = {('finlab', path) for path in lab}
        expected.update(('finmind', str((Path(root) / path).absolute())) for path in mind)
        references = {}
        for provider, logical, target, signature in document['references']:
            key = (provider, logical)
            if (key in references or provider not in ('finlab', 'finmind')
                    or not isinstance(logical, str) or not isinstance(target, str) or not target):
                raise ValueError
            if signature is not None and (not isinstance(signature, list) or len(signature) != 5
                                          or not all(type(value) is int for value in signature)):
                raise ValueError
            references[key] = (target, signature)
        if set(references) != expected:
            raise ValueError
        candidates = set(document['provisional_targets'])
        if not all(isinstance(target, str) and target for target in candidates):
            raise ValueError
        pending_keys = set()
        for provider, logical, old, new in document['pending']:
            key = (provider, logical)
            if (key in pending_keys or not isinstance(old, str) or not old or not isinstance(new, str)
                    or key not in references or references[key][0] != new or references[key][1] is None):
                raise ValueError
            pending_keys.add(key)
            candidates.add(old)
        if not pending_keys or document['retired_targets'] != sorted(
                candidates - {target for target, _ in references.values()}):
            raise ValueError
        return document
    except (KeyError, TypeError, ValueError, UnicodeDecodeError, zlib.error) as exc:
        raise ValueError('alias_census_proof_invalid') from exc


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
    # Producers atomically replace receipts. Size and mtime alone are not a
    # generation identifier (a copy can preserve both). ctime also detects
    # in-place writes followed by restoring mtime on ordinary local filesystems.
    return stat.st_mtime_ns, stat.st_size, stat.st_dev, stat.st_ino, stat.st_ctime_ns


def _scope(root, provider, link_column):
    # Resolve symlink changes with one stat instead of walking every component
    # on each inventory pass. Directory ctime changes when its contents change,
    # but that does not change the root's identity or the receipt's scope.
    stat = root.stat()
    return str(root), stat.st_dev, stat.st_ino, provider, link_column


def _publish_receipt_head(queue, path, signature):
    # Called inside the same SQLite transaction as completion or legacy reuse.
    queue.db.execute('INSERT OR REPLACE INTO receipt_heads VALUES (?,?,?)', (path, *signature[:2]))


def _publish_current_origin(queue, path, origin_id):
    from downloader.news_fulltext_queue import utc_now
    queue.db.execute(
        'INSERT INTO metadata_current_origins(receipt_path,origin_id,selected_at_utc) VALUES (?,?,?) '
        'ON CONFLICT(receipt_path) DO UPDATE SET origin_id=excluded.origin_id, '
        'selected_at_utc=excluded.selected_at_utc WHERE origin_id!=excluded.origin_id',
        (path, origin_id, utc_now()))


def _clear_verified_alias_error(queue, alias, resolved):
    """Clear only a path-resolution error for an alias that now verifies.

    Origins retain the resolved source identity. A previously broken symlink
    was recorded under its lexical name; completing the target alone would
    otherwise leave that old error on every subsequent status snapshot.
    """
    if alias == resolved:
        return
    try:
        if str(Path(alias).resolve()) != resolved:
            return
    except (OSError, RuntimeError, ValueError):
        return
    row = queue.db.execute('SELECT error_code FROM origin_errors WHERE receipt_path=?', (alias,)).fetchone()
    if row and (row[0] == 'missing_accepted_receipt' or row[0].startswith((
            'finmind_receipt_path:', 'receipt_path_resolution:', 'RuntimeError:'))):
        with queue.db:
            queue.db.execute('DELETE FROM origin_errors WHERE receipt_path=? AND error_code=?', (alias, row[0]))


def _completed_origin_needs_selection(queue, path, origin_id):
    current = queue.db.execute('SELECT origin_id FROM metadata_current_origins WHERE receipt_path=?',
                               (path,)).fetchone()
    if current:
        return current[0] != origin_id
    # Legacy queues infer the highest complete id. Only persist an explicit
    # selection when that inference would otherwise pick a different version.
    return queue.db.execute(
        'SELECT 1 FROM origins o LEFT JOIN metadata_ingest_progress p ON p.origin_id=o.id '
        'WHERE o.receipt_path=? AND o.id>? AND (p.origin_id IS NULL OR p.state=\'complete\') LIMIT 1',
        (path, origin_id)).fetchone() is not None


def _remember_receipt_identity(cache, path, identity):
    cache[path] = identity
    if len(cache) > METADATA_RECEIPT_IDENTITY_CACHE_LIMIT:
        cache.pop(next(iter(cache)))


def _read_receipt(path):
    receipt = json.loads(path.read_text())
    if (not isinstance(receipt, dict)
            or not isinstance(receipt.get('sha256'), str)
            or len(receipt['sha256']) != 64
            or any(char not in '0123456789abcdef' for char in receipt['sha256'])
            or type(receipt.get('rows')) is not int or receipt['rows'] < 0
            or not isinstance(receipt.get('parquet_path'), str) or not receipt['parquet_path']):
        raise ValueError('invalid_metadata_receipt')
    return receipt


class _MetadataSourceChanged(Exception):
    """The source moved during a cooperative unit; roll it back and retry."""


class ReceiptReader:
    def __init__(self, queue, root, receipt_path, provider, link_column):
        from downloader.news_fulltext_queue import source_path, utc_now
        self.priority_hint = None
        self.queue = queue
        self.scope = _scope(root, provider, link_column)
        self.path = str(receipt_path.resolve())
        self.signature = _signature(receipt_path)
        receipt = _read_receipt(receipt_path)
        self.parquet = source_path(root, receipt['parquet_path'])
        self.parquet_signature = _signature(self.parquet)
        self.origin_key = self.path + ':' + receipt['sha256']
        # Revalidate the full immutable file on each new reader, including
        # restart. A resumed in-memory reader keeps only one Arrow batch.
        checksum = sha256_file(self.parquet)
        if not self.unchanged():
            raise _MetadataSourceChanged
        if checksum != receipt['sha256']:
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
            queue.db.execute("UPDATE metadata_ingest_progress SET state='superseded' WHERE state!='complete' AND origin_id IN "
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
        if provider == 'finlab' and receipt.get('dataset') == 'tw_news_cnyes':
            # Optional background URL comparison only. The original ordered
            # import and its exact association counts remain authoritative.
            from downloader.news_fulltext_finlab_priority import FinlabCnyesPriority
            try:
                self.priority_hint = FinlabCnyesPriority.start(self, root, receipt, provider)
            except (OSError, ValueError, TypeError, sqlite3.OperationalError, RuntimeError):
                queue.metrics.count('cnyes', 'finlab_delta_hint_unavailable')

    def unchanged(self):
        try:
            return (_signature(Path(self.path)) == self.signature
                    and _signature(self.parquet) == self.parquet_signature)
        except OSError:
            return False

    def close(self):
        if self.priority_hint is not None:
            self.priority_hint.close()
            self.priority_hint = None
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
    identities = getattr(queue, '_metadata_receipt_identities', None)
    if identities is None:
        identities = queue._metadata_receipt_identities = {}
    alias = str(receipt_path.absolute())
    try:
        path = str(receipt_path.resolve())
    except (OSError, RuntimeError) as exc:
        # pathlib can report a symlink loop as RuntimeError before the regular
        # per-receipt error handler is reached. Preserve the original path so
        # a later inventory can retry it after local repair.
        path = alias
        reader = readers.pop(path, None)
        if reader:
            reader.close()
        with queue.db:
            queue.db.execute('INSERT OR REPLACE INTO origin_errors VALUES (?,?,?)',
                             (path, 'receipt_path_resolution:' + type(exc).__name__ + ':' + str(exc)[:160], utc_now()))
            queue.db.execute("UPDATE metadata_ingest_progress SET state='error' WHERE origin_id IN "
                             '(SELECT id FROM origins WHERE receipt_path=?) AND state=\'pending\' ', (path,))
        return False
    # All subsequent operations use the target whose identity is recorded in
    # origins, even if a receipt symlink is retargeted during this call.
    receipt_path = Path(path)
    reader = readers.get(path)
    try:
        signature = _signature(receipt_path)
        scope = _scope(root, provider, link_column)
        identity = (*signature, *scope)
        previous = identities.get(path)
        if previous == identity:
            _clear_verified_alias_error(queue, alias, path)
            return False
        if previous and tuple(previous[5:]) != scope:
            identities.pop(path, None)
            raise ValueError('metadata_receipt_scope_mismatch')
        identities.pop(path, None)
        if reader and reader.scope != scope:
            raise ValueError('metadata_receipt_scope_mismatch')
        if reader and not reader.unchanged():
            reader.close()
            readers.pop(path)
            reader = None
        if reader is None:
            receipt = _read_receipt(receipt_path)
            origin_key = path + ':' + receipt['sha256']
            origin = queue.db.execute(
                'SELECT o.id,o.provider,o.rows,o.parquet_path,p.origin_id,h.mtime_ns,h.bytes,e.receipt_path,p.state '
                'FROM origins o LEFT JOIN metadata_ingest_progress p ON p.origin_id=o.id '
                'LEFT JOIN receipt_heads h ON h.path=o.receipt_path '
                'LEFT JOIN origin_errors e ON e.receipt_path=o.receipt_path WHERE o.origin_key=?',
                (origin_key,)).fetchone()
            if origin and (origin[4] is None or origin[8] == 'complete'):
                if origin[1] != provider or type(receipt.get('rows')) is not int or origin[2] != receipt['rows']:
                    raise ValueError('metadata_completed_receipt_scope_mismatch')
                from downloader.news_fulltext_queue import source_path
                parquet = source_path(root, receipt['parquet_path'])
                if str(parquet) != origin[3]:
                    parquet_signature = _signature(parquet)
                    checksum = sha256_file(parquet)
                    if _signature(parquet) != parquet_signature:
                        raise _MetadataSourceChanged
                    if checksum != receipt['sha256']:
                        raise ValueError('metadata_sha256_mismatch')
                if _signature(receipt_path) != signature:
                    raise _MetadataSourceChanged
                needs_selection = _completed_origin_needs_selection(queue, path, origin[0])
                partial = queue.db.execute(
                    'SELECT 1 FROM metadata_ingest_progress p JOIN origins o ON o.id=p.origin_id '
                    "WHERE o.receipt_path=? AND o.id!=? AND p.state IN ('pending','error') LIMIT 1",
                    (path, origin[0])).fetchone()
                if tuple(origin[5:7]) != signature[:2] or origin[7] is not None or needs_selection or partial:
                    with queue.db:
                        if _signature(receipt_path) != signature:
                            raise _MetadataSourceChanged
                        _publish_receipt_head(queue, path, signature)
                        queue.db.execute('DELETE FROM origin_errors WHERE receipt_path=?', (path,))
                        if needs_selection:
                            _publish_current_origin(queue, path, origin[0])
                        if partial:
                            queue.db.execute(
                                "UPDATE metadata_ingest_progress SET state='superseded' WHERE state IN ('pending','error') "
                                'AND origin_id IN (SELECT id FROM origins WHERE receipt_path=? AND id!=?)',
                                (path, origin[0]))
                        if _signature(receipt_path) != signature:
                            raise _MetadataSourceChanged
                _remember_receipt_identity(identities, path, identity)
                _clear_verified_alias_error(queue, alias, path)
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
            if reader.priority_hint is not None:
                reader.priority_hint.poll()
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
                    _publish_receipt_head(queue, path, reader.signature)
                    _publish_current_origin(queue, path, reader.origin_id)
                if not reader.unchanged():
                    raise _MetadataSourceChanged
            if batches:
                # This is a hint about a committed transaction, not a claim
                # that the receipt or any source is complete. The resident
                # owner can immediately offer another slice when no response
                # needs a handoff; a failed or rolled-back batch cannot spin it.
                queue.metadata_inventory_made_progress = True
            queue.metrics.count(provider, 'metadata_imported_rows', processed - reader.processed_rows)
            queue.metrics.count(provider, 'metadata_imported_batches', batches)
            queue.metrics.count(provider, 'metadata_imported_commit_groups', int(batches > 0))
            reader.processed_rows = processed
            if completed:
                _remember_receipt_identity(identities, path, (*reader.signature, *scope))
                reader.close()
                readers.pop(path)
                _clear_verified_alias_error(queue, alias, path)
                return True
    except _MetadataSourceChanged:
        identities.pop(path, None)
        if reader:
            reader.close()
            readers.pop(path, None)
        queue.metadata_receipt_pending = True
        return False
    except (OSError, ValueError, KeyError, pa.ArrowException) as exc:
        identities.pop(path, None)
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
        self.finlab_root = finlab_root
        self.finlab_dir = finlab_root / 'receipts'
        self.finlab_error_path = str(self.finlab_dir.absolute())
        self.finlab = None
        self.finlab_retry_at = 0.0
        self.finlab_failed = bool(queue.db.execute(
            "SELECT 1 FROM origin_errors WHERE receipt_path=? AND substr(error_code,1,22)='finlab_source_catalog:'",
            (self.finlab_error_path,)).fetchone())
        self.phase = 'finlab'
        self.finmind = None
        self.finmind_paths = None
        self.finmind_done = False
        self.finmind_retry_at = 0.0
        self.finmind_invalid_paths = 0
        self.finmind_completed_snapshot = None
        self.finmind_full_rescan_at = 0.0
        self.finmind_retry_paths = set()
        self.finmind_validation_retries = {}
        self.finmind_replaying_failures = False
        self.current_finmind_relative = None
        self.finmind_db = finmind_root / 'queue.sqlite3'
        self.finmind_error_path = str(self.finmind_db.absolute())
        self.finmind_failed = bool(queue.db.execute(
            "SELECT 1 FROM origin_errors WHERE receipt_path=? AND substr(error_code,1,18)='finmind_source_db:'",
            (self.finmind_error_path,)).fetchone())
        self.alias_heads = {(provider, logical): target for provider, logical, target in queue.db.execute(
            'SELECT provider,logical_path,target_path FROM metadata_receipt_alias_heads')}
        self.alias_pending = {(provider, logical): (old, new) for provider, logical, old, new in queue.db.execute(
            'SELECT provider,logical_path,old_target,new_target FROM metadata_receipt_alias_pending')}
        self.retired_targets = {row[0] for row in queue.db.execute(
            'SELECT receipt_path FROM metadata_retired_receipt_targets')}
        self.alias_census = None
        self.alias_retry_at = 0.0

    def _observe_alias(self, provider, logical, target):
        """Persist a known retarget before admitting any rows from its target."""
        from downloader.news_fulltext_queue import utc_now
        key = (provider, logical)
        pending = self.alias_pending.get(key)
        previous = self.alias_heads.get(key, pending[0] if pending else None)
        if previous is None and target and logical != target:
            # A previously ordinary receipt can become a symlink. Older
            # symlinks for which no logical name was recorded cannot be
            # reconstructed from the physical origin alone.
            if self.queue.db.execute('SELECT 1 FROM origins WHERE provider=? AND receipt_path=? LIMIT 1',
                                     (provider, logical)).fetchone():
                previous = logical
                with self.queue.db:
                    self.queue.db.execute('INSERT OR IGNORE INTO metadata_receipt_alias_heads VALUES (?,?,?,?)',
                                          (provider, logical, previous, utc_now()))
                self.alias_heads[key] = previous
        # A newly discovered reference to a retired physical target must be
        # verified before it can rejoin the selected set. Keep the old target
        # as the evidence anchor without inventing a verified alias head.
        reactivation = previous is None and bool(target) and target in self.retired_targets
        if reactivation:
            previous = target
        if previous is None or (previous == target and pending is None and not reactivation):
            return
        old = pending[0] if pending else previous
        if pending and pending[1] == target:
            return
        with self.queue.db:
            self.queue.db.execute(
                'INSERT INTO metadata_receipt_alias_pending VALUES (?,?,?,?,?) '
                'ON CONFLICT(provider,logical_path) DO UPDATE SET new_target=excluded.new_target, '
                'observed_at_utc=excluded.observed_at_utc', (provider, logical, old, target, utc_now()))
            if target and not self.queue.db.execute('SELECT 1 FROM origins WHERE receipt_path=? LIMIT 1',
                                                    (target,)).fetchone():
                self.queue.db.execute('INSERT OR IGNORE INTO metadata_receipt_alias_provisional VALUES (?)',
                                      (target,))
        self.alias_pending[key] = (old, target)
        self.alias_census = None

    def _verified_alias(self, provider, logical, target):
        """Remember only successfully ingested aliases; never erase origins."""
        from downloader.news_fulltext_queue import utc_now
        key = (provider, logical)
        if key not in self.alias_heads and logical != target:
            with self.queue.db:
                self.queue.db.execute('INSERT OR IGNORE INTO metadata_receipt_alias_heads VALUES (?,?,?,?)',
                                      (provider, logical, target, utc_now()))
            self.alias_heads[key] = target
        if target in self.retired_targets and not self.alias_pending:
            with self.queue.db:
                self.queue.db.execute('DELETE FROM metadata_retired_receipt_targets WHERE receipt_path=?', (target,))
            self.retired_targets.remove(target)

    def _alias_catalog(self):
        """Read exact catalog membership; missing known sources are not empty."""
        try:
            with os.scandir(self.finlab_dir) as entries:
                lab = sorted(str((self.finlab_dir / entry.name).absolute()) for entry in entries
                             if fnmatch.fnmatchcase(entry.name, 'tw_news*.json'))
        except FileNotFoundError:
            if self.finlab_root.exists() or self.queue.db.execute(
                    "SELECT 1 FROM origins WHERE provider='finlab' LIMIT 1").fetchone():
                raise
            lab = []
        try:
            source_stat = self.finmind_db.stat()
        except FileNotFoundError:
            if self.finmind_root.exists() or self.queue.db.execute(
                    "SELECT 1 FROM origins WHERE provider='finmind' LIMIT 1").fetchone():
                raise
            mind = []
        else:
            if not stat.S_ISREG(source_stat.st_mode):
                raise ValueError('finmind_source_db_not_regular')
            try:
                source = sqlite3.connect(self.finmind_db.resolve().as_uri() + '?mode=ro', uri=True, timeout=0)
                try:
                    mind = [row[0] for row in source.execute(
                        "SELECT receipt_path FROM tasks WHERE dataset='TaiwanStockNews' AND state='complete'")]
                finally:
                    source.close()
            except sqlite3.Error as exc:
                raise ValueError('finmind_source_catalog_unavailable') from exc
        if any(not isinstance(path, str) or not path or '\0' in path for path in mind):
            raise ValueError('invalid_finmind_receipt_catalog')
        # This census asks which physical targets are referenced. A task's
        # generation is not membership and must not starve a census while
        # an unrelated source updates. Duplicate paths still count as members.
        mind.sort()
        entries = [('finlab', path, None) for path in lab]
        entries += [('finmind', str((self.finmind_root / path).absolute()), path) for path in mind]
        return (tuple(lab), tuple(mind)), entries

    def _reconcile_aliases(self, deadline, stop):
        """Two cooperative path passes and stable catalogs before retirement."""
        from downloader.news_fulltext_queue import source_path, utc_now
        if not self.alias_pending:
            return True
        finalized = False
        try:
            if self.alias_census is None:
                if time.monotonic() < self.alias_retry_at:
                    return False
                catalog, entries = self._alias_catalog()
                self.alias_census = {'catalog': catalog, 'entries': entries, 'pass': 0,
                                     'index': 0, 'first': {}, 'second': {}}
            census = self.alias_census
            while census['pass'] < 2:
                while census['index'] < len(census['entries']):
                    if ((deadline is not None and time.monotonic() >= deadline)
                            or (stop is not None and stop.is_set())):
                        return False
                    provider, logical, relative = census['entries'][census['index']]
                    target = (source_path(self.finmind_root, relative) if provider == 'finmind'
                              else Path(logical).resolve(strict=True))
                    # source_path already checked FinMind's target is a file.
                    if provider == 'finlab' and not target.is_file():
                        raise ValueError('receipt_not_regular')
                    key = (provider, logical)
                    # Only a changed alias needs an immutable-generation
                    # check here. For other members, the resolved target is
                    # enough to establish whether an old target is referenced.
                    value = (str(target), _signature(target) if key in self.alias_pending else None)
                    observed = census['first' if census['pass'] == 0 else 'second']
                    if key in observed and observed[key] != value:
                        raise ValueError('receipt_alias_changed')
                    observed[key] = value
                    census['index'] += 1
                if self._alias_catalog()[0] != census['catalog']:
                    raise ValueError('receipt_catalog_changed')
                census['pass'] += 1
                census['index'] = 0
            if census['first'] != census['second']:
                raise ValueError('receipt_alias_changed')
            identities = getattr(self.queue, '_metadata_receipt_identities', {})
            for key, (_, target) in self.alias_pending.items():
                observed = census['second'].get(key)
                if (not target or not observed or observed[0] != target
                        or tuple(identities.get(target, ())[:5]) != observed[1]):
                    raise ValueError('receipt_alias_not_verified')
            observed_targets = {value[0] for value in census['second'].values()}
            provisional = {row[0] for row in self.queue.db.execute(
                'SELECT receipt_path FROM metadata_receipt_alias_provisional')}
            candidates = {old for old, _ in self.alias_pending.values()} | provisional
            # Encoding and compression need no SQLite write lock. They occur
            # only on an alias change, not on ordinary inventory passes.
            digest, raw = _alias_census_proof(census, self.finmind_root, self.alias_pending, provisional)
            compressed = zlib.compress(raw, 3)
            with self.queue.db:
                # No filesystem/SQLite shared transaction exists. Recheck the
                # changed aliases as close to committing the census as possible.
                for key, (_, target) in self.alias_pending.items():
                    provider, logical = key
                    current = (source_path(self.finmind_root, os.path.relpath(logical, self.finmind_root))
                               if provider == 'finmind' else Path(logical).resolve(strict=True))
                    if str(current) != target or _signature(current) != census['second'][key][1]:
                        raise ValueError('receipt_alias_changed_before_commit')
                if provisional != {row[0] for row in self.queue.db.execute(
                        'SELECT receipt_path FROM metadata_receipt_alias_provisional')}:
                    raise ValueError('receipt_alias_provisional_changed_before_commit')
                self.queue.db.execute(
                    'INSERT OR IGNORE INTO metadata_receipt_alias_census_proofs '
                    '(census_sha256,version,raw_bytes,payload_zlib,recorded_at_utc) VALUES (?,?,?,?,?)',
                    (digest, 1, len(raw), compressed, utc_now()))
                stored = self.queue.db.execute(
                    'SELECT version,raw_bytes,payload_zlib FROM metadata_receipt_alias_census_proofs '
                    'WHERE census_sha256=?', (digest,)).fetchone()
                # Identical bytes need no redundant decompression. A reused
                # digest with different bytes must independently validate;
                # corrupt evidence is never overwritten to hide the error.
                if stored is None or (tuple(stored) != (1, len(raw), compressed)
                                      and read_alias_census_proof(self.queue.db, digest) != json.loads(raw)):
                    raise ValueError('alias_census_proof_conflict')
                for target in candidates - observed_targets:
                    self.queue.db.execute('INSERT OR REPLACE INTO metadata_retired_receipt_targets VALUES (?,?,?)',
                                          (target, utc_now(), digest))
                    # A partial target no longer referenced by either catalog
                    # is retained as a resumable cursor, not active work.
                    self.queue.db.execute(
                        "UPDATE metadata_ingest_progress SET state='superseded' WHERE state IN ('pending','error') "
                        'AND origin_id IN (SELECT id FROM origins WHERE receipt_path=?)', (target,))
                for target in observed_targets & self.retired_targets:
                    self.queue.db.execute('DELETE FROM metadata_retired_receipt_targets WHERE receipt_path=?', (target,))
                for (provider, logical), (old, target) in self.alias_pending.items():
                    self.queue.db.execute('INSERT OR REPLACE INTO metadata_receipt_alias_heads VALUES (?,?,?,?)',
                                          (provider, logical, target, utc_now()))
                    self.queue.db.execute('INSERT INTO metadata_receipt_alias_events '
                        '(provider,logical_path,old_target,new_target,observed_at_utc,census_sha256) VALUES (?,?,?,?,?,?)',
                        (provider, logical, old, target, utc_now(), digest))
                self.queue.db.execute('DELETE FROM metadata_receipt_alias_pending')
                self.queue.db.execute('DELETE FROM metadata_receipt_alias_provisional')
            finalized = True
            self.retired_targets = (self.retired_targets | (candidates - observed_targets)) - observed_targets
            self.alias_heads.update({key: value[1] for key, value in self.alias_pending.items()})
            self.alias_pending.clear()
            self.alias_census = None
            readers = getattr(self.queue, '_metadata_receipt_readers', {})
            for target in candidates - observed_targets:
                reader = readers.pop(target, None)
                if reader:
                    reader.close()
            return True
        except (OSError, RuntimeError, ValueError):
            if finalized:
                raise
            # Preserve the pending record and source data; a fresh pass will
            # retry after the producer or mount becomes stable again.
            self.alias_census = None
            self.alias_retry_at = time.monotonic() + METADATA_SLICE_INTERVAL_SECONDS
            self.finlab = self.finmind = self.finmind_paths = None
            self.finmind_done = False
            self.finlab_retry_at = self.finmind_retry_at = 0.0
            self.phase = 'finlab'
            return False

    def _record_catalog_error(self, path, prefix, exc):
        from downloader.news_fulltext_queue import utc_now
        error = prefix + type(exc).__name__ + ':' + str(exc)[:160]
        with self.queue.db:
            self.queue.db.execute(
                'INSERT INTO origin_errors VALUES (?,?,?) ON CONFLICT(receipt_path) DO UPDATE SET '
                'error_code=excluded.error_code,checked_at_utc=excluded.checked_at_utc '
                'WHERE origin_errors.error_code!=excluded.error_code',
                (path, error, utc_now()))

    def _finlab_unavailable(self, exc):
        self._record_catalog_error(self.finlab_error_path, 'finlab_source_catalog:', exc)
        self.finlab_failed = True
        self.finlab_retry_at = time.monotonic() + METADATA_SLICE_INTERVAL_SECONDS
        return False

    def _load_finlab(self):
        """Discover a real directory, distinguishing a missing source from empty."""
        try:
            # Path.glob may suppress directory I/O errors as an empty result.
            # Keep the old direct-child pattern and lexical receipt order.
            with os.scandir(self.finlab_dir) as entries:
                paths = sorted(self.finlab_dir / entry.name for entry in entries
                               if fnmatch.fnmatchcase(entry.name, 'tw_news*.json'))
        except FileNotFoundError as exc:
            previously_imported = self.queue.db.execute(
                "SELECT 1 FROM origins WHERE provider='finlab' LIMIT 1").fetchone()
            if self.finlab_failed or self.finlab_root.exists() or previously_imported:
                return self._finlab_unavailable(exc)
            paths = []
        except (OSError, RuntimeError, ValueError) as exc:
            return self._finlab_unavailable(exc)
        if self.finlab_failed:
            with self.queue.db:
                self.queue.db.execute(
                    "DELETE FROM origin_errors WHERE receipt_path=? AND substr(error_code,1,22)='finlab_source_catalog:'",
                    (self.finlab_error_path,))
        self.finlab = iter(paths)
        self.finlab_failed = False
        self.finlab_retry_at = 0.0
        return True

    def _finmind_unavailable(self, exc):
        self._record_catalog_error(self.finmind_error_path, 'finmind_source_db:', exc)
        self.finmind_failed = True
        return False

    def _finmind_failure_fingerprint(self, path):
        """Observe both receipt and data identity without hashing data again."""
        from downloader.news_fulltext_queue import source_path
        try:
            receipt_signature = _signature(path)
        except OSError:
            return None
        try:
            receipt = _read_receipt(path)
            parquet = source_path(self.finmind_root, receipt['parquet_path'])
            artifact = (str(parquet), _signature(parquet))
        except (OSError, RuntimeError, ValueError, KeyError) as exc:
            artifact = (type(exc).__name__, getattr(exc, 'errno', None))
        return receipt_signature, artifact

    def _finmind_checked(self):
        if self.finmind_replaying_failures:
            return
        self.checked += 1
        now = time.monotonic()
        # The disk-blocked resident already publishes its own heartbeat.
        coalesced = getattr(self.queue, '_coalesce_inventory_heartbeats', False)
        status_due = self.checked == len(self.finmind) or now - self.last_status >= 15
        if self.checked % 100 == 0 or self.checked == len(self.finmind) or (status_due and not coalesced):
            with self.queue.db:
                self.queue.gauge('latest_inventory_finmind_receipts_checked', self.checked)
            if status_due and not coalesced:
                with self.queue.metrics.measure('metadata_inventory_status'):
                    self.queue.status(activity='inventorying')
                self.last_status = time.monotonic()
        if coalesced:
            self.last_status = time.monotonic()

    def _load_finmind(self):
        """Read one source catalog without blocking unrelated source work.

        The owner retries a failed catalog as pending work. Its queue and the
        already committed FinLab batches remain authoritative. A truly absent,
        never-failed optional FinMind catalog retains the old empty behavior.
        """
        try:
            source_stat = self.finmind_db.stat()
        except FileNotFoundError as exc:
            # An absent optional source is not an error until its root is
            # present or the owner has already imported its receipts. A
            # lost mount/catalog must not silently become an empty success.
            # The owner's own SQLite errors must still propagate as such.
            previously_imported = self.queue.db.execute(
                "SELECT 1 FROM origins WHERE provider='finmind' LIMIT 1").fetchone()
            if self.finmind_failed or self.finmind_root.exists() or previously_imported:
                return self._finmind_unavailable(exc)
            source_stat = None
        except (OSError, RuntimeError, ValueError) as exc:
            return self._finmind_unavailable(exc)
        try:
            if source_stat is None:
                paths = []
            else:
                if not stat.S_ISREG(source_stat.st_mode):
                    raise ValueError('finmind_source_db_not_regular')
                # as_uri escapes #, ?, spaces and other URI characters. A busy
                # producer is retried by the owner; do not block every source
                # for SQLite's former ten-second busy timeout.
                source = sqlite3.connect(self.finmind_db.resolve().as_uri() + '?mode=ro', uri=True, timeout=0)
                try:
                    # A completed task with no usable receipt is unresolved,
                    # not an absent task. Keep it in the catalog for status
                    # and retry while letting valid siblings proceed.
                    columns = {column[1] for column in source.execute('PRAGMA table_info(tasks)')}
                    # The producer updates last_attempt_at_utc whenever it
                    # commits a new receipt, even if its path stays the same.
                    # Older catalog fixtures without that column still get a
                    # periodic full receipt rescan.
                    generation = 'last_attempt_at_utc' if 'last_attempt_at_utc' in columns else 'NULL'
                    paths = source.execute(f"SELECT receipt_path,{generation} FROM tasks WHERE dataset='TaiwanStockNews' "
                        "AND state='complete' ORDER BY partition DESC").fetchall()
                finally:
                    source.close()
        except (OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
            return self._finmind_unavailable(exc)
        snapshot = tuple(paths)
        valid = [row for row in paths if isinstance(row[0], str) and row[0] and '\0' not in row[0]]
        invalid = len(paths) - len(valid)
        previous = self.finmind_completed_snapshot
        incremental = bool(invalid and previous is not None and time.monotonic() < self.finmind_full_rescan_at)
        repeat = bool(incremental and snapshot == previous)
        if invalid:
            # Do not include source values in the error. An empty, NULL or
            # non-text path cannot safely identify a local receipt.
            self._record_catalog_error(self.finmind_error_path, 'finmind_source_db:',
                                       ValueError(f'{invalid} completed task(s) have invalid receipt paths'))
        if repeat:
            # Previously verified receipts need no filesystem scan or database
            # rewrite just because another row is malformed. Retry only paths
            # that could not be verified during the last pass.
            work = [row for row in valid if row[0] in self.finmind_retry_paths]
            self.checked = len(paths)
        elif incremental:
            # A catalog update normally changes just one producer receipt.
            # Retain the original ordering of changed and failed entries and
            # avoid stat'ing thousands of unchanged receipts each second.
            old = Counter(previous)
            work = []
            valid_names = {row[0] for row in valid}
            self.finmind_retry_paths.intersection_update(valid_names)
            self.finmind_validation_retries = {key: value for key, value in self.finmind_validation_retries.items()
                                               if key in valid_names}
            for row in valid:
                seen = old[row] > 0
                if seen:
                    old[row] -= 1
                if not seen or row[0] in self.finmind_retry_paths:
                    work.append(row)
            self.checked = len(paths) - len(work)
        else:
            work = valid
            self.checked = invalid
            self.finmind_retry_paths.clear()
            self.finmind_validation_retries.clear()
            self.finmind_full_rescan_at = time.monotonic() + METADATA_FINMIND_FULL_RESCAN_SECONDS
        with self.queue.db:
            if source_stat is not None and not repeat:
                self.queue.gauge('latest_inventory_finmind_receipts', len(paths))
                self.queue.gauge('latest_inventory_finmind_receipts_checked', self.checked)
            if self.finmind_failed and not invalid:
                self.queue.db.execute(
                    "DELETE FROM origin_errors WHERE receipt_path=? AND substr(error_code,1,18)='finmind_source_db:'",
                    (self.finmind_error_path,))
        self.finmind, self.finmind_paths = paths, iter(work)
        self.finmind_invalid_paths = invalid
        self.finmind_replaying_failures = repeat
        self.finmind_failed = bool(invalid)
        return True

    def advance(self, *, maximum_wall_seconds=None, stop=None):
        from downloader.news_fulltext_queue import source_path
        deadline = time.monotonic() + maximum_wall_seconds if maximum_wall_seconds is not None else None
        imported = 0
        while stop is None or not stop.is_set():
            if deadline is not None and time.monotonic() >= deadline:
                break
            if self.current is None:
                # After an unavailable FinLab catalog, retry it between
                # FinMind receipts without restarting the FinMind iterator.
                if (self.phase == 'finmind' and self.finlab is None
                        and time.monotonic() >= self.finlab_retry_at):
                    self.phase = 'finlab'
                if self.phase == 'finlab':
                    if self.finlab is None:
                        if time.monotonic() < self.finlab_retry_at or not self._load_finlab():
                            if self.finmind_done:
                                return {'imported': imported, 'complete': False}
                            self.phase = 'finmind'
                            continue
                    path = next(self.finlab, None)
                    if path is None:
                        if self.finmind_done:
                            return {'imported': imported, 'complete': self._reconcile_aliases(deadline, stop)}
                        self.phase = 'finmind'
                        continue
                    self.current = (self.finlab_root, path, 'finlab', 'url')
                else:
                    if self.finmind_paths is None:
                        if time.monotonic() < self.finmind_retry_at or not self._load_finmind():
                            if self.finlab is None:
                                self.phase = 'finlab'
                            return {'imported': imported, 'complete': False}
                    relative = next(self.finmind_paths, None)
                    if relative is None:
                        if self.finmind_invalid_paths:
                            self.finmind_completed_snapshot = tuple(self.finmind)
                            self.finmind_paths = None
                            self.finmind_retry_at = time.monotonic() + METADATA_SLICE_INTERVAL_SECONDS
                            # Recheck high-priority FinLab between catalog
                            # retries; a broken FinMind row cannot freeze it.
                            self.finlab = None
                            self.finlab_retry_at = 0.0
                            self.phase = 'finlab'
                            return {'imported': imported, 'complete': False}
                        self.finmind_done = True
                        if self.finlab is None:
                            self.phase = 'finlab'
                            return {'imported': imported, 'complete': False}
                        return {'imported': imported, 'complete': self._reconcile_aliases(deadline, stop)}
                    try:
                        path = source_path(self.finmind_root, relative[0])
                    except (ValueError, OSError) as exc:
                        error_path = str(self.finmind_root / relative[0])
                        self._observe_alias('finmind', str((self.finmind_root / relative[0]).absolute()), '')
                        self._record_catalog_error(error_path, 'finmind_receipt_path:', exc)
                        getattr(self.queue, '_metadata_receipt_identities', {}).pop(error_path, None)
                        self.finmind_retry_paths.add(relative[0])
                        self._finmind_checked()
                        continue
                    retry = self.finmind_validation_retries.get(relative[0])
                    if retry:
                        fingerprint = self._finmind_failure_fingerprint(path)
                        if (fingerprint is not None and fingerprint == retry[0]
                                and time.monotonic() < retry[1]):
                            self._finmind_checked()
                            continue
                    self.current_finmind_relative = relative[0]
                    self.current = (self.finmind_root, path, 'finmind', 'link')
            root, path, provider, column = self.current
            logical = (str((self.finmind_root / self.current_finmind_relative).absolute())
                       if provider == 'finmind' else str(path.absolute()))
            try:
                # FinMind's source_path already checked containment and
                # resolved the physical file for this iteration.
                target = str(path) if provider == 'finmind' else str(path.resolve())
            except (OSError, RuntimeError, ValueError):
                target = ''
            self._observe_alias(provider, logical, target)
            remaining = max(0, deadline - time.monotonic()) if deadline is not None else None
            imported += int(self.queue.ingest_receipt(root, path, provider=provider, link_column=column,
                                                     maximum_wall_seconds=remaining, stop=stop))
            if getattr(self.queue, 'metadata_receipt_pending', False):
                break
            self.current = None
            may_need_alias_update = (logical != target or (provider, logical) in self.alias_heads
                                     or target in self.retired_targets)
            if (may_need_alias_update and target
                    and target in getattr(self.queue, '_metadata_receipt_identities', {})):
                try:
                    still_target = str(Path(logical).resolve())
                except (OSError, RuntimeError, ValueError):
                    still_target = ''
                if still_target == target:
                    self._verified_alias(provider, logical, target)
                else:
                    self._observe_alias(provider, logical, still_target)
            if self.phase == 'finmind':
                if str(path) in getattr(self.queue, '_metadata_receipt_identities', {}):
                    _clear_verified_alias_error(self.queue,
                        str(self.finmind_root / self.current_finmind_relative), str(path))
                    self.finmind_retry_paths.discard(self.current_finmind_relative)
                    self.finmind_validation_retries.pop(self.current_finmind_relative, None)
                else:
                    self.finmind_retry_paths.add(self.current_finmind_relative)
                    fingerprint = self._finmind_failure_fingerprint(path)
                    previous = self.finmind_validation_retries.get(self.current_finmind_relative)
                    delay = (min(previous[2] * 2, METADATA_FAILED_RECEIPT_RETRY_MAX_SECONDS)
                             if previous and fingerprint is not None and fingerprint == previous[0]
                             else METADATA_SLICE_INTERVAL_SECONDS)
                    self.finmind_validation_retries[self.current_finmind_relative] = (
                        fingerprint, time.monotonic() + delay, delay)
                self.current_finmind_relative = None
                self._finmind_checked()
        return {'imported': imported, 'complete': False}


def ingest_local_slice(queue, finlab_root, finmind_root, *, maximum_wall_seconds=None, stop=None):
    queue.metadata_inventory_made_progress = False
    inventory = getattr(queue, '_metadata_local_inventory', None)
    if inventory is None:
        inventory = queue._metadata_local_inventory = LocalInventory(queue, finlab_root, finmind_root)
    result = inventory.advance(maximum_wall_seconds=maximum_wall_seconds, stop=stop)
    queue.metadata_inventory_pending = not result['complete']
    if result['complete']:
        queue._metadata_local_inventory = None
    return result
