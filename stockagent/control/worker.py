"""Execute the one opt-in read-only pilot through its existing canonical owner."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import shutil
import socket
import threading
import time
from typing import Any

from downloader.artifact_io import atomic_write_json
from stockagent.control.contracts import integer
from stockagent.control.postgres import ControlStore, LeaseLost
from stockagent.runtime_identity import runtime_identity, verify_source_release


def node_observation(root: Path, *, cpu_slots: int = 2, memory_bytes: int = 512 * 1024 * 1024) -> dict[str, Any]:
    integer(cpu_slots, 'node CPU slots', minimum=1, maximum=1024)
    integer(memory_bytes, 'node memory budget', minimum=1, maximum=2**60)
    root = root.resolve(strict=True)
    info = {}
    for line in Path('/proc/meminfo').read_text().splitlines():
        name, value = line.split(':', 1)
        if name == 'MemAvailable':
            info[name] = int(value.split()[0]) * 1024
    if not info.get('MemAvailable'):
        raise ValueError('node needs a current MemAvailable observation')
    cpus = len(os.sched_getaffinity(0))
    free = shutil.disk_usage(root).free
    return {'cpu_slots': min(cpu_slots, cpus), 'memory_bytes': min(memory_bytes, info['MemAvailable']),
            'scratch_bytes': free, 'observation': {
                'host': socket.gethostname(), 'observed_at_utc': datetime.now(timezone.utc).isoformat(),
                'cpu_affinity_count': cpus, 'memory_available_bytes': info['MemAvailable'],
                'scratch_free_bytes': free, 'scratch_root': str(root),
                'cpu_budget_scope': 'declared control-worker slots, not host CPU idle measurement',
                'gpu_admission': 'not_supported_by_read_only_pilot; canonical_GPU_manager_owns_it'}}


def execute_one(dsn: str, *, schema: str, node_id: str, worker_id: str,
                receipt: Path, root: Path, output: Path, lease_seconds: int = 60,
                work_key: str | None = None) -> dict[str, Any]:
    integer(lease_seconds, 'lease', minimum=3, maximum=3600)
    with ControlStore(dsn, schema=schema) as store:
        store.register_node(node_id, **node_observation(root), ttl_seconds=max(60, lease_seconds*2))
        claim = store.claim(node_id, worker_id, lease_seconds=lease_seconds,
                            job_keys=(work_key,) if work_key is not None else None)
        if claim is None:
            return {'state': 'no_eligible_work', 'node_id': node_id}
        stopped = threading.Event()
        heartbeat_errors = []

        def renew():
            try:
                with ControlStore(dsn, schema=schema) as heartbeats:
                    while not stopped.wait(lease_seconds / 3):
                        heartbeats.heartbeat(claim, lease_seconds=lease_seconds)
                        heartbeats.register_node(node_id, **node_observation(root), ttl_seconds=max(60, lease_seconds*2))
            except Exception as error:
                heartbeat_errors.append(type(error).__name__)

        heartbeat = threading.Thread(target=renew, name='control-lease-heartbeat', daemon=True)
        heartbeat.start()
        started = time.perf_counter()
        outcome = {'schema_version': 1, 'work_key': claim.spec.key,
                   'identity_sha256': claim.spec.identity_sha256, 'attempt': claim.attempt,
                   'lease_token': claim.token, 'node_id': node_id, 'worker_id': worker_id,
                   'observed_at_utc': datetime.now(timezone.utc).isoformat()}
        try:
            if hashlib.sha256(receipt.read_bytes()).hexdigest() != claim.spec.inputs['receipt_sha256']:
                raise ValueError('local receipt differs from immutable work input')
            result = verify_source_release(receipt, root)
            if result['source_sha256'] != claim.spec.inputs['source_sha256']:
                raise ValueError('verified source differs from immutable work input')
            result['worker_runtime'] = runtime_identity()
            result['wall_seconds'] = time.perf_counter() - started
            if heartbeat_errors:
                raise LeaseLost('heartbeat could not prove continuing lease ownership')
            store.finish(claim, result)
            outcome.update(state='succeeded', result=result)
        except Exception as error:
            # The canonical owner may include an internal path in an error.
            # Keep only the error type in shared state; private receipts retain its bounded reason.
            outcome.update(error_type=type(error).__name__, reason=str(error)[:1024])
            try:
                state = store.fail(claim, type(error).__name__)
                outcome.update(state='attempt_failed', next_work_state=state)
            except LeaseLost:
                outcome.update(state='lease_lost', next_work_state='not_modified')
        finally:
            stopped.set()
            heartbeat.join(timeout=12)
        outcome['wall_seconds'] = time.perf_counter() - started
        atomic_write_json(output / f'{claim.spec.key}.attempt-{claim.attempt}.{claim.token}.json', outcome)
        return outcome
