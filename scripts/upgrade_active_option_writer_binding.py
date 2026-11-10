#!/usr/bin/env python3
"""Bind the reviewed TXO settlement writer update to an active raw handoff."""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync import workspace_source_handoff as handoff
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json, write_immutable_json
from stockagent.data_sync.packed_member_stream import read_stable_bytes

WRITER = 'scripts/download_taifex_final_settlement_history.py'
CONTRACT = 'stockagent.active_option_raw_writer_upgrade.v1'


def _identity(value: dict) -> str:
    body = {key: item for key, item in value.items() if key != 'identity_sha256'}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def _read_binding(path: Path) -> dict:
    if path.is_symlink() or not path.is_file() or path.stat().st_uid != os.geteuid() \
            or path.stat().st_mode & 0o022:
        raise SnapshotError('option handoff binding is missing or not operator-owned')
    value = json.loads(read_stable_bytes(path, maximum=8192))
    if not isinstance(value, dict) or value.get('identity_sha256') != _identity(value):
        raise SnapshotError('option handoff binding identity differs')
    return value


def upgrade(previous_identity: str, old_writer_sha: str, new_writer_sha: str, *, apply=False) -> dict:
    """Change exactly one writer SHA, preserving the original binding under the owner."""
    if type(apply) is not bool or any(not isinstance(s, str) or not re.fullmatch(r'[0-9a-f]{64}', s)
                                      for s in (previous_identity, old_writer_sha, new_writer_sha)):
        raise ValueError('upgrade requires exact SHA-256 identities')
    if old_writer_sha == new_writer_sha:
        raise ValueError('writer upgrade must change its exact code identity')
    if not (handoff.binance_handoff_window_allowed(for_start=True)
            and handoff.options_raw_handoff_window_allowed(for_start=True)):
        raise BlockingIOError('trading priority window')
    name = handoff.OPTIONS_RAW_NAME
    with handoff.selected_source(name), handoff.lock_file(handoff.CONTROL/'owner.lock'):
        journal = handoff.read_journal()
        if not journal or journal.get('state') != 'active':
            raise SnapshotError('option source handoff is not active')
        scope = handoff._scope()
        destination = handoff._assert_retained(journal)
        with ExitStack() as locks:
            handoff._lock_writer_roots(locks, scope.backup, destination)
            if (not handoff.provider_workspace.exact_mount(scope.source, destination)
                    or not handoff.binance_handoff_window_allowed(for_start=True)
                    or not handoff.options_raw_handoff_window_allowed(for_start=True)):
                raise SnapshotError('option source mount or trading window changed')
            marker = handoff.CONTROL/(handoff.RETAINED_LOCK_SOURCES[name]['label']+'-enrollment.json')
            prior_path = marker.with_name(marker.name+'.prior-'+previous_identity+'.json')
            current = _read_binding(marker)
            candidate = handoff.retained_enrollment_value(name)
            if current.get('identity_sha256') == candidate.get('identity_sha256'):
                previous = _read_binding(prior_path)
                already = True
            else:
                previous, already = current, False
            old_hashes, new_hashes = previous.get('writer_guard_sha256'), candidate.get('writer_guard_sha256')
            if (previous.get('identity_sha256') != previous_identity
                    or not isinstance(old_hashes, dict) or not isinstance(new_hashes, dict)
                    or old_hashes.get(WRITER) != old_writer_sha or new_hashes.get(WRITER) != new_writer_sha
                    or {k:v for k,v in old_hashes.items() if k != WRITER} !=
                       {k:v for k,v in new_hashes.items() if k != WRITER}
                    or {k:v for k,v in previous.items() if k not in {'identity_sha256','writer_guard_sha256'}} !=
                       {k:v for k,v in candidate.items() if k not in {'identity_sha256','writer_guard_sha256'}}):
                raise SnapshotError('option writer upgrade exceeds the reviewed change')
            result = {'contract': CONTRACT, 'state': 'already_upgraded' if already else 'ready',
                      'previous_identity_sha256': previous_identity,
                      'new_identity_sha256': candidate['identity_sha256'],
                      'old_writer_sha256': old_writer_sha, 'new_writer_sha256': new_writer_sha,
                      'source_files_deleted': 0}
            if apply:
                if prior_path.is_symlink():
                    raise SnapshotError('prior option handoff binding is redirected')
                if prior_path.exists():
                    if _read_binding(prior_path) != previous:
                        raise SnapshotError('prior option handoff binding differs')
                else:
                    write_immutable_json(prior_path, previous)
                if not already:
                    atomic_write_json(marker, candidate, durable=True)
                handoff._validate_retained_enrollment(name)
                result.update(state='already_upgraded' if already else 'upgraded', observed_at_epoch=time.time())
                atomic_write_json(marker.with_name(marker.name+'.writer-upgrade.json'), result, durable=True)
            return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--previous-identity', required=True)
    parser.add_argument('--old-writer-sha', required=True)
    parser.add_argument('--new-writer-sha', required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    print(json.dumps(upgrade(args.previous_identity, args.old_writer_sha, args.new_writer_sha,
                             apply=args.apply), ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
