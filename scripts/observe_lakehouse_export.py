#!/usr/bin/env python3
"""Observe the existing catalog export; never start or change a workflow."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import os
from pathlib import Path

from downloader.artifact_io import atomic_write_json

CONTRACT = 'stockagent.catalog_export_observation.v1'
OUTPUT = Path('/var/lib/stockagent/lakehouse-control/catalog-export-observation.json')
WORKFLOW = 'stockagent-storage-lifecycle-v1'


def _utc_field(value, name):
    if not value.HasField(name):
        return None
    return getattr(value, name).ToDatetime(tzinfo=timezone.utc).isoformat()


def project(phase, pending, *, now, run_id):
    """Expose fixed codes, not failure text, paths, credentials or payloads."""
    phase = phase if isinstance(phase, dict) else {}
    rows = [row for row in pending if row.activity_type.name == 'export_version']
    if len(rows) > 1:
        raise ValueError('more than one canonical catalog export is pending')
    if not isinstance(run_id, str) or not run_id:
        raise ValueError('canonical workflow run identity is missing')
    result = {'contract': CONTRACT, 'read_only': True, 'observed_at_utc': now, 'workflow_run_id': run_id,
              'phase': 'exporting' if phase.get('state') == 'exporting_version' else 'other',
              'snapshot_id': phase.get('snapshot_id') if type(phase.get('snapshot_id')) is int else None,
              'activity': None}
    if rows:
        row = rows[0]
        states = {1: 'scheduled', 2: 'started', 3: 'cancel_requested'}
        if row.state not in states or not row.activity_id or type(row.attempt) is not int or row.attempt < 1:
            raise ValueError('catalog export activity identity is incomplete')
        message = row.last_failure.message if row.HasField('last_failure') else ''
        failure = ('native_command_rejected' if 'Native batch failed (native-command-rejected' in message
                   else 'other' if message else None)
        result['activity'] = {'id': row.activity_id, 'state': states[row.state], 'attempt': row.attempt,
                              'failure': failure,
                              'failure_at_utc': _utc_field(row, 'last_attempt_complete_time') if failure else None,
                              'started_at_utc': _utc_field(row, 'last_started_time')}
    return result


async def observe():
    from temporalio.client import Client
    from stockagent.control.lakehouse import configuration
    config = configuration()
    client = await Client.connect(config['temporal_endpoint'], namespace=config['temporal_namespace'])
    handle = client.get_workflow_handle(WORKFLOW)
    description = await handle.describe()
    if description.status.name != 'RUNNING':
        raise ValueError('canonical lifecycle workflow is not running')
    phase = await handle.query('status')
    return project(phase, description.raw_description.pending_activities,
                   now=datetime.now(timezone.utc).isoformat(), run_id=description.run_id)


def main():
    os.umask(0o077)
    value = asyncio.run(asyncio.wait_for(observe(), timeout=10))
    atomic_write_json(OUTPUT, value, durable=False)


if __name__ == '__main__':
    main()
