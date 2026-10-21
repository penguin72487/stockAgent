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
             'stockagent/data_sync/packed_snapshots.py', 'stockagent/data_sync/packed_backup.py',
             'stockagent/data_sync/cold_primary.py',
             'stockagent/data_sync/packed_metadata.py',
             'stockagent/data_sync/syncthing_scan.py',
             'stockagent/data_sync/materialized_cache.py',
             'stockagent/data_sync/cache_capacity.py',
             'stockagent/data_sync/completed_forward_capacity.py',
             'stockagent/data_sync/recovery_queue.py',
             'configs/data_sync/cache_capacity.json',
             'stockagent/data_sync/packing_checkpoint.py',
             'stockagent/data_sync/dataset_migration.py', 'stockagent/data_sync/dataset_version_layout.py',
             'stockagent/data_sync/private_source_links.py',
             'scripts/publish_data_releases.py', 'configs/data_sync/packed_datasets.json',
             'scripts/finlab_release_gate.py', 'stockagent/data/finlab_acquisition_contract.py',
             'scripts/validate_training_sources.py',
             'scripts/shioaji_minute_backfill_state.py',
             'scripts/compact_openbb_l1.py', 'scripts/download_taifex_rule_history.py',
             'downloader/ohlcv_hot_tail.py',
             'stockagent/data_sync/source_validation.py',
             'stockagent/data_sync/minute_catalog_validation.py',
             'scripts/audit_shioaji_tw_minute_dataset.py',
             'scripts/build_shioaji_tw_minute_dataset.py',
             'downloader/download_shioaji_tw_minute_kbars.py',
             'stockagent/data/tw_price_rules.py',
             'stockagent/data/tw_exchange_price_classification.py',
             'downloader/stock_volume_units.py', 'downloader/download_tw_public_data.py',
             'configs/columnar_storage.json',
             'stockagent/data_sync/desync_snapshots.py',
             'stockagent/data_sync/nas_coverage.py',
             'stockagent/data_sync/backup_stream.py', 'stockagent/data_sync/testing_disposition.py',
             'stockagent/data_sync/windows_cold_io.py', 'scripts/windows_cold_binary_io.ps1',
             'scripts/manage_lakehouse.py', 'scripts/run_lakehouse_control.sh', 'scripts/runtime_env.sh',
             'configs/training_rescue.json')
    code = {p: digest(ROOT / p) for p in paths}
    from stockagent.data_sync.testing_disposition import DEFAULT_POLICY
    if DEFAULT_POLICY.exists():
        code[str(DEFAULT_POLICY.relative_to(ROOT))] = digest(DEFAULT_POLICY)
    return identity_sha256(code)


async def temporal(action, c):
    from temporalio.client import Client
    from temporalio.exceptions import WorkflowAlreadyStartedError
    from temporalio.worker import Worker
    from temporalio.api.workflowservice.v1 import RegisterNamespaceRequest
    from temporalio.service import RPCError, RPCStatusCode
    from google.protobuf.duration_pb2 import Duration
    from stockagent.control.storage_workflow import (StorageLifecycle, SourceReplicationLifecycle,
        DatasetVersionLifecycle, register_versions, export_version, check_nas_acceptance,
        stage_source_replication, advance_dataset_versions)
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
        try:
            await client.start_workflow(DatasetVersionLifecycle.run, {}, id="stockagent-dataset-versions-v1", task_queue=c["task_queue"])
        except WorkflowAlreadyStartedError:
            pass
        return {"state": "enrolled", "workflow_id": handle.id, "source_replication_workflow_id": "stockagent-source-replication-v1",
                "dataset_version_workflow_id": "stockagent-dataset-versions-v1"}
    if action == "worker":
        # Initialize pybind/native libraries once on the main thread before
        # workflow sandboxes and concurrent activity imports exist. A failed
        # first import can otherwise leave DuckDB's ExplainType registry half
        # initialized and poison every subsequent activity in this process.
        import duckdb  # noqa: F401
        import pyarrow  # noqa: F401
        stop = asyncio.Event()
        for signum in (signal.SIGTERM, signal.SIGINT):
            asyncio.get_running_loop().add_signal_handler(signum, stop.set)
        # Catalog publication and raw transport have separate canonical locks.
        # HDD cache retirement must not starve version/ACK control activities.
        with ThreadPoolExecutor(max_workers=3) as executor:
            async with Worker(client, task_queue=c["task_queue"], workflows=[StorageLifecycle, SourceReplicationLifecycle, DatasetVersionLifecycle],
                              activities=[register_versions, export_version, check_nas_acceptance, stage_source_replication, advance_dataset_versions],
                              activity_executor=executor, max_concurrent_activities=3,
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
    versions = client.get_workflow_handle("stockagent-dataset-versions-v1")
    try:
        version_description = await versions.describe()
        version_status = {"workflow_id": versions.id, "run_id": version_description.run_id,
                          "status": version_description.status.name, "phase": await versions.query(DatasetVersionLifecycle.status)}
    except RPCError as error:
        if error.status != RPCStatusCode.NOT_FOUND:
            raise
        version_status = {"workflow_id": versions.id, "status": "NOT_ENROLLED"}
    return {"state": "running", "workflow_id": handle.id, "run_id": description.run_id,
            "status": description.status.name, "phase": phase,
            "source_replication_workflow": {"workflow_id": source.id, "run_id": source_description.run_id,
                                            "status": source_description.status.name},
            "dataset_version_workflow": version_status,
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
