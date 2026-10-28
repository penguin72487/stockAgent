"""Persist verified derivative metadata, invalidated by file and receipt identity.

These are local filesystem mutation proofs, not payload checksums. Every reuse
still checks the regular file's device/inode/size/mtime/ctime twice; absent,
expired or malformed evidence requires the canonical footer audit. Deep audits
remain independent and never consume this cache.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time

import pyarrow as pa
from stockagent import private_json

POLICY = 'openbb_l1_file_identity_metadata_proof_v1'
TABLE = 'l1_derivative_metadata_proofs_v1'
FULL_AUDIT_INTERVAL_NS = 24 * 60 * 60 * 1_000_000_000
_DDL = f'''CREATE TABLE IF NOT EXISTS {TABLE} (
    segment_id TEXT PRIMARY KEY, body BLOB NOT NULL, sha256 TEXT NOT NULL
)'''


class MetadataProofCache:
    """One canonical compactor attempt; only changed proofs are written."""

    # One sequential read avoids a primary-key B-tree lookup for every segment.
    # The pool belongs to this audit only and is released by finish(). It does
    # not replace either fresh filesystem observation or any proof predicate.
    columns = join = ''

    def __init__(self, connection: sqlite3.Connection, *, contract: str,
                 force_full: bool = False, now_ns: int | None = None):
        self.connection = connection
        self.contract = [POLICY, pa.__version__, contract]
        self.force_full = force_full
        self.now_ns = time.time_ns() if now_ns is None else now_ns
        self.pending = []
        self.hits = self.misses = self.written = self.deleted = 0
        with connection:
            connection.execute(_DDL)
            actual = connection.execute(
                "SELECT sql FROM sqlite_schema WHERE type='table' AND name=?", (TABLE,),
            ).fetchone()
            normalize = lambda value: ' '.join(value.replace('IF NOT EXISTS ', '').split())
            if actual is None or normalize(str(actual[0])) != normalize(_DDL):
                raise RuntimeError('OpenBB derivative metadata proof table schema changed')
        self.pool = {} if force_full else {
            row[0]: (row[1], row[2]) for row in connection.execute(
                f'SELECT segment_id,body,sha256 FROM {TABLE}')
        }
        self.proof_entries_read = len(self.pool)

    @staticmethod
    def manifest_contract(row):
        return [str(row['segment_id']), str(row['endpoint']), str(row['output_path']),
                int(row['output_rows']), int(row['output_bytes']), str(row['schema_fingerprint'])]

    def reuse(self, row, signature, *, physical_required: bool):
        """Return (physical_identity,) on a verified hit, otherwise None."""
        body, digest = self.pool.get(row['segment_id'], (None, None))
        if not self.force_full and type(body) is bytes and type(digest) is str:
            try:
                if hashlib.sha256(body).hexdigest() == digest:
                    record = private_json.loads(body)
                    if (type(record) is list and len(record) == 7
                            and record[0] == self.contract
                            and record[1] == self.manifest_contract(row)
                            and record[2] == list(signature)
                            and type(record[3]) is int
                            and 0 <= self.now_ns - record[3] < FULL_AUDIT_INTERVAL_NS
                            and type(record[4]) is bool
                            and (record[4] or not physical_required)
                            and (record[5] is None or (type(record[5]) is str
                                 and len(record[5]) == 64
                                 and all(c in '0123456789abcdef' for c in record[5])))
                            and record[6] == 'validated'):
                        self.hits += 1
                        return (record[5],)
            except (ValueError, TypeError, OverflowError):
                pass
        self.misses += 1
        return None

    def record(self, row, signature, *, physical_checked: bool, physical_identity):
        body = json.dumps([self.contract, self.manifest_contract(row), list(signature),
                           self.now_ns, physical_checked, physical_identity, 'validated'],
                          separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
        self.pending.append((str(row['segment_id']), body, hashlib.sha256(body).hexdigest()))

    def finish(self, stale_ids):
        # Cache publication follows the completed audit. An interrupted scan
        # cannot turn a partial observation into a completed proof generation.
        with self.connection:
            self.connection.executemany(
                f'INSERT INTO {TABLE}(segment_id,body,sha256) VALUES(?,?,?) '
                'ON CONFLICT(segment_id) DO UPDATE SET body=excluded.body,sha256=excluded.sha256',
                self.pending,
            )
            self.written = len(self.pending)
            self.pending.clear()
            before = self.connection.total_changes
            self.connection.executemany(f'DELETE FROM {TABLE} WHERE segment_id=?',
                                        ((str(key),) for key in stale_ids))
            self.deleted = self.connection.total_changes - before
        self.pool.clear()

    def snapshot(self):
        return dict(policy=POLICY, hits=self.hits, misses=self.misses,
                    proofs_written=self.written, proofs_deleted=self.deleted,
                    force_full=self.force_full, full_audit_interval_ns=FULL_AUDIT_INTERVAL_NS,
                    proof_entries_read=self.proof_entries_read,
                    proof_lookup='one_sequential_pool_released_after_audit',
                    invalidation='manifest contract + runtime policy + regular file dev/inode/size/mtime/ctime',
                    scope='Local OS-visible mutation; not a payload checksum or bit-rot audit')
