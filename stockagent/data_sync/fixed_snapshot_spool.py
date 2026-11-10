"""Bounded E scratch for comparing two immutable packed source generations.

The canonical packed reader verifies selected source members before these
bytes are admitted. SQLite scratch is resumable and has no authority to
publish, retire or delete either source generation.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat

from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_snapshots import _safe_relative_path


class FixedSnapshotSpool:
    CONTRACT = 'stockagent.fixed_snapshot_comparison_spool.v1'

    def __init__(self, path: Path, binding: dict, records: dict, *, maximum_file_bytes: int,
                 maximum_side_bytes: int):
        self.path = Path(path).absolute()
        self.records = records
        self.maximum_file_bytes = maximum_file_bytes
        self.maximum_side_bytes = maximum_side_bytes
        if (self.path.parent.is_symlink() or self.path.parent.resolve(strict=True) != self.path.parent
                or not isinstance(binding, dict) or not isinstance(records, dict) or not 0 < len(records) <= 4096
                or type(maximum_file_bytes) is not int or not 0 < maximum_file_bytes <= 256*1024**2
                or type(maximum_side_bytes) is not int or not 0 < maximum_side_bytes <= 4*1024**3):
            raise SnapshotError('fixed comparison scratch scope differs')
        expected = {}
        for name, row in records.items():
            if not isinstance(name, str) or _safe_relative_path(name, 'fixed comparison member').as_posix() != name:
                raise SnapshotError('fixed comparison member path differs')
            for side in ('public', 'private'):
                member = row.get(side) if isinstance(row, dict) else None
                size = member.get('size') if isinstance(member, dict) else None
                digest = member.get('sha256') if isinstance(member, dict) else None
                if (type(size) is not int or not 0 <= size <= maximum_file_bytes
                        or not isinstance(digest, str) or re.fullmatch('[0-9a-f]{64}', digest) is None):
                    raise SnapshotError('fixed comparison member binding differs')
                expected[(side, name)] = (size, digest)
        if any(sum(size for (which, _), (size, _) in expected.items() if which == side) > maximum_side_bytes
               for side in ('public', 'private')):
            raise SnapshotError('fixed comparison scratch exceeds its byte bound')
        self.expected = expected
        self.binding = {'contract': self.CONTRACT, **binding,
            'records_sha256': hashlib.sha256(json.dumps(
                sorted((side, name, size, digest) for (side, name), (size, digest) in expected.items()),
                separators=(',', ':')).encode()).hexdigest()}
        if os.path.lexists(self.path):
            info = self.path.lstat()
            if self.path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_nlink != 1:
                raise SnapshotError('fixed comparison scratch is redirected or shared')
        else:
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            os.close(descriptor)
        self.db = sqlite3.connect(self.path, timeout=1)
        try:
            self.db.execute('PRAGMA journal_mode=WAL')
            self.db.execute('PRAGMA synchronous=NORMAL')
            self.db.execute('PRAGMA cache_size=-32768')
            self.db.execute('CREATE TABLE IF NOT EXISTS meta (name TEXT PRIMARY KEY,value TEXT NOT NULL)')
            self.db.execute('CREATE TABLE IF NOT EXISTS members (side TEXT NOT NULL,name TEXT NOT NULL, '
                'size INTEGER NOT NULL,sha TEXT NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(side,name))')
            saved = self.db.execute("SELECT value FROM meta WHERE name='binding'").fetchone()
            if saved is None:
                if self.db.execute('SELECT count(*) FROM members').fetchone()[0]:
                    raise SnapshotError('unbound fixed comparison scratch has members')
                self.db.execute('INSERT INTO meta VALUES (?,?)', ('binding', json.dumps(self.binding, sort_keys=True)))
                self.db.commit()
            elif json.loads(saved[0]) != self.binding:
                raise SnapshotError('fixed comparison scratch generation differs')
            existing = self.db.execute('SELECT side,name,size,sha,length(payload) FROM members').fetchall()
            for side, name, size, digest, actual_size in existing:
                if self.expected.get((side, name)) != (size, digest) or actual_size != size:
                    raise SnapshotError('fixed comparison scratch contains an unbound member')
        except BaseException:
            self.db.close()
            raise

    def missing(self, side: str) -> set[str]:
        if side not in {'public', 'private'}:
            raise SnapshotError('unknown fixed comparison side')
        existing = {row[0] for row in self.db.execute('SELECT name FROM members WHERE side=?', (side,))}
        return {name for which, name in self.expected if which == side} - existing

    def put(self, side: str, name: str, value: bytes) -> None:
        expected = self.expected.get((side, name))
        if (expected is None or not isinstance(value, bytes) or len(value) != expected[0]
                or hashlib.sha256(value).hexdigest() != expected[1]):
            raise SnapshotError('streamed fixed member differs from the worklist')
        self.db.execute('INSERT INTO members VALUES (?,?,?,?,?)', (side, name, len(value), expected[1], value))

    def commit(self) -> None:
        self.db.commit()
        self.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')

    def get(self, side: str, name: str) -> bytes:
        row = self.db.execute('SELECT size,sha,payload FROM members WHERE side=? AND name=?', (side, name)).fetchone()
        expected = self.expected.get((side, name))
        if (row is None or expected is None or (row[0], row[1]) != expected or len(row[2]) != row[0]
                or hashlib.sha256(row[2]).hexdigest() != row[1]):
            raise SnapshotError('fixed comparison scratch content changed')
        return row[2]

    def close(self) -> None:
        if self.db is not None:
            self.db.close()
            self.db = None

    def remove(self) -> None:
        """Remove only the known transient database and its SQLite sidecars."""
        self.commit()
        self.close()
        for path in (self.path, self.path.with_name(self.path.name+'-wal'), self.path.with_name(self.path.name+'-shm')):
            if not os.path.lexists(path):
                continue
            info = path.lstat()
            if path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_nlink != 1:
                raise SnapshotError('fixed comparison scratch changed before cleanup')
            path.unlink()
        descriptor = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def populate_pair(spool: FixedSnapshotSpool, records: dict, *, sides, stream, guard,
                  progress=lambda _event: None, maximum_file_bytes: int, maximum_side_bytes: int) -> dict:
    """Stream each fixed snapshot once in packed order; resume completed E members."""
    proofs = {}
    if [item[0] for item in sides] != ['private', 'public']:
        raise SnapshotError('fixed comparison source order differs')
    for side, root, snapshot in sides:
        missing = spool.missing(side)
        total = sum(records[name][side]['size'] for name in records)
        completed = sum(records[name][side]['size'] for name in records if name not in missing)
        streamed = 0
        progress({'stage': 'reading_fixed_'+side, 'stage_files_total': len(records),
                  'stage_files_completed': len(records)-len(missing),
                  'stage_bytes_total': total, 'stage_bytes_completed': completed})
        def consume(name, data):
            nonlocal completed, streamed
            guard()
            spool.put(side, name, data)
            streamed += 1
            completed += len(data)
            if streamed % 16 == 0:
                spool.commit()
            progress({'stage': 'reading_fixed_'+side, 'stage_files_total': len(records),
                      'stage_files_completed': len(records)-len(missing)+streamed,
                      'stage_bytes_total': total, 'stage_bytes_completed': completed})
        proof = (stream(root, snapshot, names=missing, consume=consume,
                        maximum_file_bytes=maximum_file_bytes, maximum_total_bytes=maximum_side_bytes)
                 if missing else {'files': 0, 'logical_bytes': 0})
        if missing and (not isinstance(proof, dict) or proof.get('files') != len(missing)
                or proof.get('logical_bytes') != sum(records[name][side]['size'] for name in missing)
                or proof.get('original_member_sha256_verified') is not True
                or proof.get('independent_recovery_verified') is not False):
            raise SnapshotError('fixed comparison stream proof is incomplete')
        spool.commit()
        if spool.missing(side):
            raise SnapshotError('fixed comparison scratch is incomplete')
        proofs[side] = {'files': len(records), 'logical_bytes': total,
                        'original_member_sha256_verified': True,
                        'whole_encoded_object_sha256_verified': False,
                        'independent_recovery_verified': False,
                        'packed_files_streamed_this_attempt': proof.get('files', 0),
                        'reused_scratch_files': len(records)-len(missing)}
    return proofs
