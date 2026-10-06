"""Durable production storage lifecycle. Activities own all external I/O."""
from datetime import timedelta
from temporalio import activity, workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from contextlib import contextmanager
    import contextvars
    import threading
    from stockagent.control.lakehouse import configuration, register, export, acceptance, stage_source_wave, retire_accepted_transport
    from downloader.artifact_io import atomic_write_json
    from pathlib import Path


@contextmanager
def activity_lease():
    """Heartbeat during bounded I/O so a dead worker is reclaimed promptly."""
    stopped = threading.Event()
    context = contextvars.copy_context()
    def beat():
        while not stopped.wait(10):
            activity.heartbeat('canonical owner remains active; durable journal owns recovery')
    thread = threading.Thread(target=context.run, args=(beat,), daemon=True, name='storage-activity-heartbeat')
    activity.heartbeat('canonical storage activity lease')
    thread.start()
    try:
        yield
    finally:
        stopped.set()
        thread.join(timeout=1)


@activity.defn
def register_versions() -> dict:
    activity.heartbeat("capturing authoritative source catalog")
    with activity_lease():
        result = register(configuration())
    activity.heartbeat(result["snapshot_id"])
    return result


@activity.defn
def export_version(snapshot_id: int) -> dict:
    activity.heartbeat("exporting fixed snapshot")
    with activity_lease():
        result = export(configuration(), snapshot_id)
    # No host paths or credentials enter Temporal history.
    return {k: result[k] for k in ("snapshot_id", "delivery_identity_sha256", "state")}


@activity.defn
def check_nas_acceptance(delivery_id: str) -> dict:
    c = configuration()
    with activity_lease():
        result = acceptance(c, delivery_id)
        if result['state'] == 'nas_archive_file_recovery_verified':
            result['transport_retirement'] = retire_accepted_transport(c, delivery_id, result)
    atomic_write_json(Path(c["state_root"]) / "replication-status.json", result)
    return result


@activity.defn
def stage_source_replication() -> dict:
    activity.heartbeat("enrolling immutable source-object wave")
    with activity_lease():
        return stage_source_wave(configuration())


@workflow.defn
class SourceReplicationLifecycle:
    def __init__(self):
        self.phase = {"state": "created"}

    @workflow.query
    def status(self) -> dict:
        return self.phase

    @workflow.run
    async def run(self, request: dict) -> None:
        retry = RetryPolicy(initial_interval=timedelta(seconds=5), maximum_interval=timedelta(minutes=2))
        for _ in range(60):
            result = await workflow.execute_activity(stage_source_replication, start_to_close_timeout=timedelta(hours=2),
                                                     heartbeat_timeout=timedelta(seconds=60), retry_policy=retry)
            self.phase = result
            if result["state"] == "waiting_nas_archive_acceptance":
                await workflow.sleep(timedelta(seconds=30))
            else:
                await workflow.sleep(timedelta(seconds=60))
        workflow.continue_as_new({})


@workflow.defn
class StorageLifecycle:
    def __init__(self):
        self.phase = {"state": "created"}

    @workflow.query
    def status(self) -> dict:
        return self.phase

    @workflow.run
    async def run(self, request: dict) -> None:
        retry = RetryPolicy(initial_interval=timedelta(seconds=5), maximum_interval=timedelta(minutes=2))
        last = request.get("last_snapshot", -1)
        pending_export = request.get('pending_export')
        for _ in range(60):
            self.phase = {"state": "registering_dataset_versions"}
            catalog = ({'snapshot_id': pending_export['snapshot_id']} if pending_export else
                await workflow.execute_activity(register_versions, start_to_close_timeout=timedelta(minutes=20), heartbeat_timeout=timedelta(seconds=60), retry_policy=retry))
            if catalog["snapshot_id"] != last:
                self.phase = {"state": "exporting_version", "snapshot_id": catalog["snapshot_id"]}
                exported = pending_export or await workflow.execute_activity(export_version, catalog["snapshot_id"], start_to_close_timeout=timedelta(minutes=20), heartbeat_timeout=timedelta(seconds=60), retry_policy=retry)
                pending_export = None
                if exported['state'] == 'catalog_advanced':
                    self.phase = {'state': 'recapturing_advanced_catalog', 'snapshot_id': exported['snapshot_id']}
                    continue
                self.phase = {**exported, "state": "waiting_nas_archive_acceptance"}
                while True:
                    proof = await workflow.execute_activity(check_nas_acceptance, exported["delivery_identity_sha256"],
                        start_to_close_timeout=timedelta(minutes=2), heartbeat_timeout=timedelta(seconds=60), retry_policy=retry)
                    if proof["state"] == "nas_archive_file_recovery_verified":
                        self.phase = proof
                        break
                    if (workflow.info().is_continue_as_new_suggested()
                            or workflow.info().get_current_history_length() >= 5000):
                        # Preserve this exact delivery during a multi-day NAS
                        # outage; never recapture moving latest on continuation.
                        workflow.continue_as_new({'last_snapshot': last, 'pending_export': exported})
                    await workflow.sleep(timedelta(seconds=30))
                last = catalog["snapshot_id"]
            self.phase = {"state": "monitoring_increments", "snapshot_id": last}
            await workflow.sleep(timedelta(seconds=60))
        workflow.continue_as_new({"last_snapshot": last})
