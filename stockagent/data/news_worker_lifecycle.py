"""Bounded local owner evidence, separate from persisted article counts.

Only the CLI holding the canonical queue lock publishes it. Readers verify
the Linux PID generation; a fresh file alone never establishes liveness.
This module has no collector, credential, database or network imports.
"""
from datetime import UTC, datetime
import json
import math
from pathlib import Path
import re
import time

from stockagent.data.finlab_acquisition_contract import process_owner, process_owner_alive


CONTRACT_VERSION = 1
FIRST_SNAPSHOT_TIMING_CONTRACT_VERSION = 1
MAX_BYTES = 4096
PHASES = frozenset({'queue_initialization', 'acceptance_recovery', 'policy_reconciliation',
    'awaiting_first_snapshot', 'snapshot_published', 'closed', 'failed'})


class NewsWorkerLifecycle:
    def __init__(self, root):
        self.path = Path(root) / 'owner_lifecycle.json'
        self.record = None
        self.last_write = 0.
        self.write_errors = 0
        self.started_monotonic = None

    def started(self, owner_id):
        # Called only AFTER flock succeeded. A rejected second owner must not
        # overwrite the real owner's initialization or shutdown evidence.
        self.started_monotonic = time.monotonic()
        self.record = {'contract_version': CONTRACT_VERSION, 'owner_id': owner_id,
            'process_owner': process_owner(), 'started_at_utc': datetime.now(UTC).isoformat(),
            'state': 'initializing'}
        self.phase('queue_initialization')

    def phase(self, phase, *, receipts_decoded=None, throttle=False):
        if self.record is None:
            return
        if phase not in PHASES:
            raise ValueError('unknown_news_worker_phase')
        tick = time.monotonic()
        if throttle and tick - self.last_write < 1:
            return
        self.last_write = tick
        self.record.update(phase=phase, updated_at_utc=datetime.now(UTC).isoformat())
        if receipts_decoded is not None:
            self.record['receipts_decoded'] = receipts_decoded
        # Telemetry is not an article acceptance gate. A disk/permission error
        # must not discard a completed body or trigger another publisher call.
        from downloader.artifact_io import atomic_write_json
        try:
            atomic_write_json(self.path, self.record, durable=False, compact=True)
        except OSError:
            self.write_errors += 1

    def snapshot_written(self):
        if self.record is not None:
            # Called after durable status publication. Preserve the FIRST
            # snapshot, not a later observer's readback or recurring heartbeat.
            if 'first_snapshot_at_utc' not in self.record:
                self.record.update(
                    first_snapshot_timing_contract_version=FIRST_SNAPSHOT_TIMING_CONTRACT_VERSION,
                    first_snapshot_at_utc=datetime.now(UTC).isoformat(),
                    initialization_wall_seconds=max(0., time.monotonic() - self.started_monotonic))
            self.record['state'] = 'ready'
            self.phase('snapshot_published')

    def finish(self, *, failed=False):
        if self.record is not None:
            self.record['state'] = 'failed' if failed else 'closed'
            self.phase('failed' if failed else 'closed')


def _first_snapshot_timing(value, started, observed):
    """Legacy/malformed timing is unknown; never infer it from a heartbeat."""
    try:
        version = value.get('first_snapshot_timing_contract_version')
        seconds = value.get('initialization_wall_seconds')
        if (type(version) is not int or version != FIRST_SNAPSHOT_TIMING_CONTRACT_VERSION
                or value['state'] == 'initializing' or type(seconds) not in {int, float}
                or not math.isfinite(seconds) or seconds < 0):
            return None, None
        first = datetime.fromisoformat(value['first_snapshot_at_utc'])
        if first.tzinfo is None or not started <= first <= observed:
            return None, None
        return first.isoformat(), seconds
    except (KeyError, ValueError, TypeError, OverflowError):
        return None, None


def read_news_worker_lifecycle(root, *, snapshot_owner_id, snapshot_at, now):
    """Project safe shape/clock facts, never PID, owner token or arbitrary text."""
    unavailable = {'contract_version': CONTRACT_VERSION, 'available': False}
    try:
        with (Path(root) / 'owner_lifecycle.json').open('rb') as stream:
            raw = stream.read(MAX_BYTES + 1)
        if len(raw) > MAX_BYTES:
            return unavailable
        value = json.loads(raw)
        if (not isinstance(value, dict) or type(value.get('contract_version')) is not int
                or value['contract_version'] != CONTRACT_VERSION
                or not isinstance(value.get('owner_id'), str) or not re.fullmatch('[0-9a-f]{32}', value['owner_id'])
                or value.get('phase') not in PHASES or value.get('state') not in {'initializing', 'ready', 'closed', 'failed'}):
            return unavailable
        allowed_phases = {'initializing': PHASES - {'snapshot_published', 'closed', 'failed'},
            'ready': {'snapshot_published'}, 'closed': {'closed'}, 'failed': {'failed'}}
        if value['phase'] not in allowed_phases[value['state']]:
            return unavailable
        started = datetime.fromisoformat(value['started_at_utc'])
        observed = datetime.fromisoformat(value['updated_at_utc'])
        if (started.tzinfo is None or observed.tzinfo is None or started > observed
                or (observed - now).total_seconds() > 5):
            return unavailable
        matching = value['owner_id'] == snapshot_owner_id
        supersedes = bool(not matching and (snapshot_at is None or started >= snapshot_at))
        alive = process_owner_alive(value.get('process_owner'), started_at_utc=started.isoformat(), now=now)
        count = value.get('receipts_decoded')
        first_snapshot, initialization = _first_snapshot_timing(value, started, observed)
        return {'contract_version': CONTRACT_VERSION, 'available': True,
            'process_alive': alive, 'state': value['state'], 'phase': value['phase'],
            'started_at_utc': started.isoformat(), 'observed_at_utc': observed.isoformat(),
            'age_seconds': max(0., (now - observed).total_seconds()),
            'elapsed_seconds': max(0., (now - started).total_seconds()),
            'first_snapshot_at_utc': first_snapshot,
            'initialization_wall_seconds': initialization,
            'snapshot_owner_matches': matching, 'supersedes_snapshot': supersedes,
            'receipts_decoded': count if type(count) is int and count >= 0 else None}
    except (OSError, ValueError, UnicodeError, KeyError, TypeError, OverflowError):
        return unavailable
