"""Replay both existing persistent storage histories; never execute activities.

Use the locked lakehouse control role. Fetch history from pinned native run IDs,
preserve every event privately, and replay through the production SDK sandbox.
No worker, enrollment, signal, reset, source publication or provider requests.
"""
from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path
import subprocess
from unittest.mock import patch

from downloader.artifact_io import atomic_write_json, atomic_write_text
from scripts.manage_lakehouse import worker_code_identity
from scripts.rollout_lakehouse_source_policy import IDS, OWNER
from stockagent.control.lakehouse import configuration, guard


def owner_identity():
    # Keep the locked storage role independent of FinMind/requests/CUDA imports.
    raw = subprocess.run(['systemctl', 'show', OWNER, '-p', 'ActiveState', '-p', 'InvocationID', '-p', 'MainPID'],
                         capture_output=True, text=True, check=True, timeout=5).stdout
    return dict(line.split('=', 1) for line in raw.splitlines() if '=' in line)


def validate_events(history):
    identifiers = [event.event_id for event in history.events]
    if not identifiers or identifiers != list(range(1, len(identifiers) + 1)):
        raise ValueError('history must preserve every event in original order')


async def replay(c, root):
    from temporalio.client import Client, WorkflowHistory
    from temporalio.worker import Replayer
    from stockagent.control import storage_workflow as sw

    client = await Client.connect(c['temporal_endpoint'], namespace=c['temporal_namespace'])
    results = []
    # A replayer consumes recorded activity results, never a live activity
    # executor. A bounded executor prevents diagnostic thread oversubscription.
    with ThreadPoolExecutor(max_workers=2) as executor:
        replayer = Replayer(workflows=[sw.StorageLifecycle, sw.SourceReplicationLifecycle],
                            workflow_task_executor=executor, namespace=c['temporal_namespace'])
        for name in IDS:
            description = await client.get_workflow_handle(name).describe()
            if description.status.name != 'RUNNING':
                raise ValueError('canonical persistent workflow is not running')
            pinned = client.get_workflow_handle(name, run_id=description.run_id)
            history = await pinned.fetch_history(rpc_timeout=timedelta(seconds=30))
            validate_events(history)
            path = root / (name + '.history.json')
            raw = history.to_json()
            atomic_write_text(path, raw, mode=0o600, durable=True)
            observed = path.read_text()
            if observed != raw:
                raise ValueError('private complete history readback differs')
            restored = WorkflowHistory.from_json(name, observed)
            validate_events(restored)
            with patch.object(sw, 'configuration', side_effect=AssertionError('replay invoked an activity')):
                result = await replayer.replay_workflow(restored, raise_on_replay_failure=True)
            if result.replay_failure is not None:
                raise ValueError('persistent history replay failed')
            current = await pinned.describe()
            if current.run_id != description.run_id or current.status.name != 'RUNNING':
                raise ValueError('pinned persistent workflow changed during replay')
            results.append({'workflow_id': name, 'run_id': description.run_id,
                            'first_run_id': description.raw_description.workflow_execution_info.first_run_id,
                            'events': len(history.events), 'history_path': str(path),
                            'history_sha256': hashlib.sha256(raw.encode()).hexdigest(),
                            'state': 'accepted_complete_history_replay'})
    return results


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-root', required=True, type=Path)
    args = parser.parse_args(argv)
    root = args.output_root.absolute()
    if root.exists() or not root.resolve().is_relative_to(Path('artifacts/operations').resolve()):
        parser.error('a fresh private root under artifacts/operations is required')
    c = configuration()
    guard(c)
    root.mkdir(mode=0o700, parents=True)
    owner, code = owner_identity(), worker_code_identity()
    verifier_sha = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    if owner.get('ActiveState') != 'active':
        raise ValueError('native storage owner is not active')
    results = asyncio.run(replay(c, root))
    if (owner_identity() != owner or worker_code_identity() != code
            or hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != verifier_sha):
        raise ValueError('native owner or code changed during replay')
    result = {'schema_version': 1, 'state': 'accepted_complete_persistent_history_replay',
              'observed_at_utc': datetime.now(UTC).isoformat(), 'native_owner': owner,
              'worker_code_identity_sha256': code, 'verifier_sha256': verifier_sha,
              'histories': results, 'activity_executions': 0, 'workflow_starts_or_resets': 0,
              'source_writes': 0, 'provider_calls': 0, 'service_controls': 0,
              'caveats': ['History replay verifies deterministic workflow compatibility, not source completeness.',
                          'Current histories are pinned per run, not all historical continuations or a crash recovery trial.',
                          'Loaded native code and publication/backup acceptance require separate readback.']}
    atomic_write_json(root / 'acceptance.json', result, durable=True)
    print(json.dumps(result))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
