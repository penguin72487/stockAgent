"""Atomically hand one sealed offline repair tree to its E working copy.

The source-index archive and retirement journal are the existing owners.  This
adapter preserves the historical pathname as a symlink to the fixed E copy;
it never touches the live ledger or claims a destination readback/NAS backup.
"""
from __future__ import annotations

from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import stat
import time

from stockagent.data_sync import dedup_stage_handoff, handoff_source_repack, stream_repack
from stockagent.data_sync import workspace_relocation as relocation
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.materialized_cache import process_references_many

CONTRACT = 'stockagent.offline_repair_e_handoff.v1'


def check_staged_namespace(destination: Path, index: Path) -> None:
    """Recheck the exact staged names and saved E inodes, without payload reads."""
    if (destination.is_symlink() or destination.resolve(strict=True) != destination
            or index.is_symlink() or not index.is_file()):
        raise SnapshotError('offline repair E source or index redirected')
    with closing(sqlite3.connect(f'file:{index}?mode=ro', uri=True)) as db:
        expected = db.execute('SELECT count(*) FROM files').fetchone()[0]
        seen = 0
        for name, kind, signature in relocation.walk(destination):
            row = db.execute('SELECT f.kind,f.state,s.target_sig FROM files f '
                'LEFT JOIN stream_files s ON f.path=s.path WHERE f.path=? COLLATE BINARY', (name,)).fetchone()
            if row is None or row[0] != kind:
                raise SnapshotError('offline repair E namespace differs from its fixed index')
            if kind == 'file' and (row[1] != 'verified' or row[2] is None
                                   or signature != json.loads(row[2])):
                raise SnapshotError('offline repair E file generation differs from staging')
            seen += 1
        if seen != expected:
            raise SnapshotError('offline repair E namespace is incomplete')


def handoff(source: Path, destination: Path, index: Path, stage: Path, dataset: str, identity: str, *,
            original_inode: int, lock_inode: int, guard=lambda: None,
            process_check=process_references_many, progress=None, after_exchange=None) -> dict:
    """Exchange the original pathname, then journal exact C unlinks.

    The caller owns the original artifact mutation lock, the candidate's
    supplemental lock and E capacity/mount checks. A crash at either side of
    the same-filesystem exchange is reconciled from its fixed journal.
    """
    source, destination, index, stage = (Path(x).absolute() for x in (source, destination, index, stage))
    prepared = stream_repack._read_prepared(stage, dataset, identity)
    if (prepared.get('source_mode') != 'retained_handoff_C_index'
            or prepared.get('source_root_inode') != original_inode
            or prepared.get('preserved_paths') != ['.download.lock']
            or prepared.get('supplemental_root_lock') is not True
            or not stream_repack._generation_matches(index, prepared.get('source_index_signature'),
                                                     relocation.signature(index))):
        raise SnapshotError('offline repair archive does not bind its fixed source')
    if (source.parent.resolve(strict=True) != source.parent or destination.resolve(strict=True) != destination
            or source == destination or source in destination.parents or destination in source.parents):
        raise SnapshotError('offline repair handoff roots differ')
    check_staged_namespace(destination, index)
    dest_info = destination.stat()
    preserved = source.parent/('.'+source.name+'.e-handoff-'+identity[:16])
    journal = stage/'C-offline-handoff.json'
    binding = {'contract': CONTRACT, 'source': str(source), 'destination': str(destination),
        'preserved': str(preserved), 'dataset': dataset, 'source_identity': identity,
        'prepared_sha256': prepared['identity_sha256'], 'original_inode': original_inode,
        'lock_inode': lock_inode, 'destination_inode': dest_info.st_ino,
        'destination_device': dest_info.st_dev}
    if journal.is_symlink():
        raise SnapshotError('offline repair handoff journal redirected')
    if journal.exists():
        value = json.loads(journal.read_bytes())
        if (not isinstance(value, dict) or any(value.get(key) != val for key, val in binding.items()
                                               if key != 'destination_device')
                or not stream_repack._generation_matches(destination,
                    [value.get('destination_device'), value.get('destination_inode')],
                    [dest_info.st_dev, dest_info.st_ino])
                or value.get('state') not in {'planned', 'exchanged', 'C_payload_retired'}):
            raise SnapshotError('offline repair handoff journal differs')
    else:
        if (source.is_symlink() or not source.is_dir() or source.stat().st_ino != original_inode
                or os.path.lexists(preserved)):
            raise SnapshotError('offline repair original or staging name differs')
        if process_check([source]):
            raise BlockingIOError('offline repair C source has an active process reference')
        value = {**binding, 'state': 'planned', 'observed_at_epoch': time.time()}
        atomic_write_json(journal, value, durable=True)
    guard()
    if not source.is_symlink():
        if source.stat().st_ino != original_inode or value['state'] != 'planned':
            raise SnapshotError('offline repair C original changed before exchange')
        if not os.path.lexists(preserved):
            os.symlink(str(destination), preserved)
        if not preserved.is_symlink() or os.readlink(preserved) != str(destination):
            raise SnapshotError('offline repair exchange link differs')
        if process_check([source]):
            raise BlockingIOError('offline repair C source has an active process reference')
        dedup_stage_handoff._exchange(source, preserved)
        if after_exchange is not None:
            after_exchange()
    if (not source.is_symlink() or os.readlink(source) != str(destination)
            or preserved.is_symlink() or not preserved.is_dir() or preserved.stat().st_ino != original_inode):
        raise SnapshotError('offline repair exchange did not preserve both fixed names')
    if value['state'] == 'C_payload_retired':
        return value
    value.update(state='exchanged', observed_at_epoch=time.time())
    atomic_write_json(journal, value, durable=True)

    def retire_guard():
        guard()
        if (not source.is_symlink() or os.readlink(source) != str(destination)
                or not stream_repack._generation_matches(destination,
                    [value['destination_device'], value['destination_inode']],
                    [destination.stat().st_dev, destination.stat().st_ino])):
            raise SnapshotError('offline repair E working path changed before C retirement')
        if process_check([preserved]):
            raise BlockingIOError('offline repair retained C source has an active process reference')

    result = handoff_source_repack.retire(preserved, stage, dataset, identity, index,
        original_inode=original_inode, original_lock_inode=lock_inode, guard=retire_guard,
        process_check=process_check, progress=progress, planning_progress=progress)
    value.update(state='C_payload_retired', C_retirement=result, C_payload_deleted=True,
                 destination_independent_readback=False, NAS_accepted=False, observed_at_epoch=time.time())
    atomic_write_json(journal, value, durable=True)
    return value
