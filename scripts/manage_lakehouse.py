#!/usr/bin/env python3
"""Fixed lakehouse control actions; no arbitrary command dispatch."""
import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from datetime import timedelta
import json
from pathlib import Path
import sys
import signal

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.control.lakehouse import configuration, register, export, provision_extensions
from downloader.artifact_io import atomic_write_json
from stockagent.runtime_identity import identity_sha256
from stockagent.data_sync.immutable_replication import digest


def worker_code_identity():
    paths = ('stockagent/control/lakehouse.py', 'stockagent/control/storage_workflow.py',
             'stockagent/data_sync/immutable_replication.py', 'stockagent/data_sync/offhost_backup.py',
             'stockagent/data_sync/immutable_transport_cache.py',
             'stockagent/data_sync/nas_coverage.py',
             'stockagent/data_sync/windows_cold_io.py', 'scripts/windows_cold_binary_io.ps1',
             'scripts/manage_lakehouse.py', 'scripts/run_lakehouse_control.sh', 'scripts/runtime_env.sh')
    return identity_sha256({p: digest(ROOT / p) for p in paths})


async def temporal(action, c):
    from temporalio.client import Client
    from temporalio.exceptions import WorkflowAlreadyStartedError
    from temporalio.worker import Worker
    from temporalio.api.workflowservice.v1 import RegisterNamespaceRequest
    from temporalio.service import RPCError, RPCStatusCode
    from google.protobuf.duration_pb2 import Duration
    from stockagent.control.storage_workflow import StorageLifecycle, SourceReplicationLifecycle, register_versions, export_version, check_nas_acceptance, stage_source_replication
    client = await Client.connect(c["temporal_endpoint"], namespace=c["temporal_namespace"])
    if action == "enroll":
        try:
            await client.workflow_service.register_namespace(RegisterNamespaceRequest(namespace=c["temporal_namespace"],
                workflow_execution_retention_period=Duration(seconds=30 * 86400)))
        except RPCError as error:
            if error.status != RPCStatusCode.ALREADY_EXISTS:
                raise
        try:
            handle = await client.start_workflow(StorageLifecycle.run, {}, id="stockagent-storage-lifecycle-v1", task_queue=c["task_queue"])
        except WorkflowAlreadyStartedError:
            handle = client.get_workflow_handle("stockagent-storage-lifecycle-v1")
        try:
            await client.start_workflow(SourceReplicationLifecycle.run, {}, id="stockagent-source-replication-v1", task_queue=c["task_queue"])
        except WorkflowAlreadyStartedError:
            pass
        return {"state": "enrolled", "workflow_id": handle.id, "source_replication_workflow_id": "stockagent-source-replication-v1"}
    if action == "worker":
        stop = asyncio.Event()
        for signum in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_running_loop().add_signal_handler(signum, stop.set)
        # Catalog publication and raw transport have separate canonical locks.
        # HDD cache retirement must not starve version/ACK control activities.
        with ThreadPoolExecutor(max_workers=2) as executor:
            async with Worker(client, task_queue=c["task_queue"], workflows=[StorageLifecycle, SourceReplicationLifecycle],
                              activities=[register_versions, export_version, check_nas_acceptance, stage_source_replication],
                              activity_executor=executor, max_concurrent_activities=2,
                              graceful_shutdown_timeout=timedelta(seconds=45)):
                atomic_write_json(Path(c["state_root"]) / "worker-ready.json", {"state": "polling", "namespace": c["temporal_namespace"],
                    "task_queue": c["task_queue"], "code_identity_sha256": worker_code_identity(),
                    "observed_at_utc": datetime.now(timezone.utc).isoformat()})
                await stop.wait()
        return {"state": "stopped_after_graceful_worker_shutdown"}
    if not await client.service_client.check_health():
        raise RuntimeError("Temporal frontend health check failed")
    handle = client.get_workflow_handle("stockagent-storage-lifecycle-v1")
    description = await handle.describe()
    phase = await handle.query(StorageLifecycle.status)
    source = client.get_workflow_handle("stockagent-source-replication-v1")
    source_description = await source.describe()
    return {"state": "running", "workflow_id": handle.id, "run_id": description.run_id,
            "status": description.status.name, "phase": phase,
            "source_replication_workflow": {"workflow_id": source.id, "run_id": source_description.run_id,
                                            "status": source_description.status.name},
            "worker_code_identity_sha256": worker_code_identity(),
            "observed_at_utc": datetime.now(timezone.utc).isoformat()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("extensions", "register", "export", "enroll", "worker", "status"))
    parser.add_argument("--snapshot", type=int)
    args = parser.parse_args()
    c = configuration()
    if args.action == "extensions":
        provision_extensions(c)
        result = {"state": "extensions_installed"}
    elif args.action == "register":
        result = register(c)
    elif args.action == "export":
        if args.snapshot is None:
            raise ValueError("export needs an exact DuckLake snapshot")
        result = export(c, args.snapshot)
    else:
        result = asyncio.run(temporal(args.action, c))
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
