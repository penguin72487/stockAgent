"""Generation-bound proof of an already validated complete DuckDB catalog.

No source DDL or task-writer hooks. The canonical archive_meta transaction
stores the witnessed catalog only after persisted SQL validation. Reuse still
requires the exact regular DB identity, publisher/reader contract and age;
the caller independently verifies every current view input and reader policy.
This covers OS-visible mutation, not silent media corruption.
"""
from __future__ import annotations

import hashlib
import json
import os
import stat
import time

from stockagent import private_json

KEY = 'l1_view_publication_proof_v1'
POLICY = 'openbb_complete_view_publication_identity_v1'
INTERVAL_NS = 24*60*60*1_000_000_000


def identity(path):
    # An external writer can change a DuckDB WAL while the main inode is still
    # unchanged. Only a checkpointed, closed generation can witness its SQL.
    try:
        os.stat(os.fspath(path)+'.wal')
    except FileNotFoundError:
        pass
    else:
        raise ValueError('view catalog has an uncheckpointed WAL')
    value = os.stat(path)
    if not stat.S_ISREG(value.st_mode):
        raise ValueError('view catalog is not a regular file')
    return [value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns,value.st_ctime_ns]


def _encode(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)


class ViewPublicationProof:
    def __init__(self, connection, path, contract, *, now_ns=None):
        self.connection, self.path, self.contract = connection, path, contract
        self.now_ns = time.time_ns() if now_ns is None else now_ns
        self.observation = dict(policy=POLICY,state='not_reused',proofs_written=0,
                                scope='OS-visible file mutation; not payload checksums or media corruption')

    def read(self):
        try:
            row = self.connection.execute('SELECT value FROM archive_meta WHERE key=?',(KEY,)).fetchone()
            envelope = private_json.loads(row[0]) if row else None
            if type(envelope) is not dict or set(envelope)!={'body','sha256'}:
                return None
            body = envelope['body']
            if type(body) is not str or hashlib.sha256(body.encode()).hexdigest()!=envelope['sha256']:
                return None
            proof = private_json.loads(body)
            if (type(proof) is not list or len(proof)!=7 or proof[0]!=POLICY
                    or proof[1]!=os.path.abspath(self.path) or proof[2]!=self.contract
                    or type(proof[3]) is not int or not 0<=self.now_ns-proof[3]<INTERVAL_NS
                    or proof[4]!=identity(self.path) or type(proof[5]) is not list
                    or any(type(row) is not list or len(row)!=13 for row in proof[5])
                    or proof[6]!='persisted_sql_and_complete_catalog_validated'):
                return None
            self.observation['state']='identity_proof_validated'
            return proof[4], [tuple(row) for row in proof[5]]
        except (OSError,ValueError,TypeError,OverflowError,KeyError):
            return None

    def record(self, rows, *, expected_identity):
        before = identity(self.path)
        if before != list(expected_identity):
            raise RuntimeError('L1 view catalog changed after complete validation')
        body = _encode([POLICY,os.path.abspath(self.path),self.contract,self.now_ns,before,rows,
                        'persisted_sql_and_complete_catalog_validated'])
        envelope = _encode(dict(body=body,sha256=hashlib.sha256(body.encode()).hexdigest()))
        with self.connection:
            if identity(self.path)!=before:
                raise RuntimeError('L1 view catalog changed while recording publication proof')
            columns = {row[1] for row in self.connection.execute('PRAGMA table_info(archive_meta)')}
            if 'updated_at' in columns:
                self.connection.execute('INSERT INTO archive_meta(key,value,updated_at) VALUES(?,?,CURRENT_TIMESTAMP) '
                    'ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at',(KEY,envelope))
            else:
                self.connection.execute('INSERT INTO archive_meta(key,value) VALUES(?,?) '
                    'ON CONFLICT(key) DO UPDATE SET value=excluded.value',(KEY,envelope))
        self.observation.update(state='complete_validation_recorded',proofs_written=1)
