"""Durable production storage lifecycle. Activities own all external I/O."""
from datetime import datetime, timedelta, timezone
from temporalio import activity, workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from contextlib import contextmanager
    import contextvars
    import fcntl
    import os
    import stat
    import threading
    import time
    from stockagent.control import lakehouse as _lakehouse
    from stockagent.control.lakehouse import CatalogColdCapacity, CatalogOwnerBusy, configuration, register, export, acceptance, catalog_enrollment, stage_source_wave, retire_accepted_transport
    from downloader.artifact_io import atomic_write_json
    from pathlib import Path
    from stockagent.data_sync.dataset_migration import advance_catalog_versions

    # A live Temporal sandbox can reload this module while the exclusive
    # worker still holds the previous lakehouse module. Keep import and replay
    # compatible until the original owner reaches its safe handoff boundary.
    class _OlderWorkerHasNoTradingPriority(Exception):
        pass
    CatalogTradingPriority = getattr(_lakehouse, 'CatalogTradingPriority', _OlderWorkerHasNoTradingPriority)
    _migration_pause_requested = getattr(_lakehouse, '_migration_pause_requested', lambda _state: False)


@contextmanager
def activity_lease(*, on_heartbeat=None):
    """Fence the entire activity while retaining its original owner locks."""
    stopped = threading.Event()
    context = contextvars.copy_context()
    def beat():
        last_observation = time.monotonic()
        while not stopped.wait(10):
            activity.heartbeat('canonical owner remains active; durable journal owns recovery')
            if on_heartbeat is not None and time.monotonic() - last_observation >= 60:
                on_heartbeat()
                last_observation = time.monotonic()
    thread = threading.Thread(target=context.run, args=(beat,), daemon=True, name='storage-activity-heartbeat')
    activity.heartbeat('canonical storage activity lease')
    thread.start()
    try:
        root = Path(configuration()['state_root'])
        if root.is_symlink() or root.resolve(strict=True) != root:
            raise ValueError('storage activity lease root is redirected')
        descriptor = os.open(root / 'activity-lease.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, 'r+b') as lease:
            if not stat.S_ISREG(os.fstat(lease.fileno()).st_mode):
                raise ValueError('storage activity lease is not a regular file')
            while True:
                try:
                    fcntl.flock(lease.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if activity.is_cancelled() or activity.is_worker_shutdown():
                        raise InterruptedError('storage worker stopped while waiting for its activity lease')
                    time.sleep(0.25)
            yield
    finally:
        stopped.set()
        thread.join(timeout=1)


def _catalog_activity_observation(config, state, *, reason=None):
    """Publish liveness, never a synthetic catalog progress or acceptance."""
    try:
        root = Path(config['state_root'])
        if root.is_symlink() or root.resolve(strict=True) != root:
            return
        value = {'contract': 'stockagent.catalog_activity_observation.v1', 'state': state,
                 'observed_at_utc': datetime.now(timezone.utc).isoformat(),
                 'reason': reason, 'catalog_acceptance_claimed': False}
        atomic_write_json(root/'catalog-activity-observation.json', value, durable=False)
    except (OSError, ValueError, KeyError, TypeError):
        pass  # Observation failure must not change the original catalog owner.


@activity.defn
def register_versions() -> dict:
    activity.heartbeat("capturing authoritative source catalog")
    config = configuration()
    try:
        with activity_lease(on_heartbeat=lambda: _catalog_activity_observation(config, 'running')):
            _catalog_activity_observation(config, 'running')
            result = register(config)
    except CatalogTradingPriority:
        # Keep the last committed registry while day trading has priority;
        # this shares the existing replay-safe waiting transition.
        _catalog_activity_observation(config, 'waiting', reason='tw_day_trade_priority_window')
        return {'state': 'waiting_catalog_owner', 'reason': 'tw_day_trade_priority_window',
                'retry_after_seconds': 60}
    except CatalogOwnerBusy:
        # Another canonical activity owns this exact lock. This is a normal
        # scheduling wait, not a failed source or permission to read latest.
        _catalog_activity_observation(config, 'waiting', reason='catalog_owner_busy')
        return {'state': 'waiting_catalog_owner', 'retry_after_seconds': 15}
    except CatalogColdCapacity:
        # The guarded D lake cannot accept its Parquet registry while full.
        # Keep the committed snapshot and retry without creating a new one.
        _catalog_activity_observation(config, 'waiting', reason='catalog_cold_capacity')
        return {'state': 'waiting_cold_physical_capacity', 'retry_after_seconds': 60}
    except Exception:
        _catalog_activity_observation(config, 'blocked', reason='catalog_refresh_failed')
        raise
    _catalog_activity_observation(config, 'waiting', reason='catalog_cycle_complete')
    activity.heartbeat(result["snapshot_id"])
    return result


@contextmanager
def observe_catalog_export(config: dict, snapshot_id: int):
    """Publish bounded, attempt-bound counters without owning or accepting work."""
    from stockagent.data_sync.work_progress import observe_work
    if not activity.in_activity():
        yield
        return
    info = activity.info()
    destination = Path(config['state_root']) / 'catalog-export-progress.json'
    last = 0.0

    def record(row):
        nonlocal last
        now = time.monotonic()
        done, total = row.get('completed_items'), row.get('total_items')
        finished = type(done) is int and type(total) is int and total > 0 and done == total
        if now - last < 5 and not finished:
            return
        last = now
        value = {'contract': 'stockagent.catalog_export_progress.v1',
                 'snapshot_id': snapshot_id, 'activity_id': info.activity_id,
                 'workflow_run_id': info.workflow_run_id, 'attempt': info.attempt,
                 'observed_at_utc': datetime.now(timezone.utc).isoformat(),
                 **row}
        try:
            atomic_write_json(destination, value, durable=False)
        except OSError:
            # An observational receipt must never cancel a valid export.
            pass

    with observe_work(record):
        yield


@activity.defn
def export_version(snapshot_id: int, long_timeout: bool = False) -> dict:
    if type(long_timeout) is not bool:
        raise ValueError('catalog export timeout selection must be boolean')
    if not long_timeout:
        # An old recorded activity keeps its original 20-minute timeout even
        # after the worker is upgraded. Ask the workflow to schedule the same
        # snapshot again with the new bound, without starting another copy.
        return {'snapshot_id': snapshot_id, 'delivery_identity_sha256': None,
                'state': 'reschedule_with_long_timeout'}
    c = configuration()
    if _migration_pause_requested(Path(c['state_root'])):
        # User selected migration first, with no additional backup as a gate.
        # Preserve the committed registry and let this fixed workflow retry.
        return {'snapshot_id': snapshot_id, 'delivery_identity_sha256': None,
                'state': 'waiting_user_migration'}
    activity.heartbeat("exporting fixed snapshot")
    with activity_lease():
        # Keep this wait inside the activity lease so no unpinned snapshot or
        # source is touched while the existing catalog owner is active.
        def wait_for_owner() -> bool:
            if activity.is_cancelled() or activity.is_worker_shutdown():
                return False
            time.sleep(5)
            return not (activity.is_cancelled() or activity.is_worker_shutdown())
        with observe_catalog_export(c, snapshot_id):
            result = export(c, snapshot_id, on_owner_busy=wait_for_owner)
    # No host paths or credentials enter Temporal history.
    return {k: result[k] for k in ("snapshot_id", "delivery_identity_sha256", "state")}


@activity.defn
def check_nas_acceptance(delivery_id: str) -> dict:
    c = configuration()
    with activity_lease():
        enrolled = catalog_enrollment(c, delivery_id)
        result = acceptance(c, delivery_id, **({'enrolled_catalog': enrolled} if enrolled is not None else {}))
        if result['state'] == 'nas_archive_file_recovery_verified':
            atomic_write_json(Path(c['state_root']) / ('catalog-acceptance-'+delivery_id+'.json'), result, durable=True)
            result['transport_retirement'] = {'state': 'delegated_to_transport_gc'}
    atomic_write_json(Path(c["state_root"]) / "replication-status.json", result)
    return result


@activity.defn
def stage_source_replication() -> dict:
    activity.heartbeat("enrolling immutable source-object wave")
    with activity_lease():
        return stage_source_wave(configuration())


@activity.defn
def advance_dataset_versions() -> dict:
    activity.heartbeat("resuming catalog-backed incremental dataset versions")
    with activity_lease():
        return advance_catalog_versions(Path(__file__).resolve().parents[2], configuration(),
            stop_requested=lambda: activity.is_cancelled() or activity.is_worker_shutdown())


@workflow.defn
class DatasetVersionLifecycle:
    def __init__(self):
        self.phase = {"state": "created"}

    @workflow.query
    def status(self) -> dict:
        return self.phase

    @workflow.run
    async def run(self, request: dict) -> None:
        retry = RetryPolicy(initial_interval=timedelta(seconds=10), maximum_interval=timedelta(minutes=2))
        for _ in range(60):
            self.phase = await workflow.execute_activity(advance_dataset_versions,
                start_to_close_timeout=timedelta(hours=24), heartbeat_timeout=timedelta(seconds=60),
                retry_policy=retry)
            delay = dataset_version_poll_seconds(self.phase)
            if delay:
                await workflow.sleep(timedelta(seconds=delay))
        workflow.continue_as_new({})


def dataset_version_poll_seconds(phase: dict) -> int:
    """Keep old recorded timers; new results admit ready sources immediately."""
    if phase['state'] == 'waiting_next_daily_cycle':
        return 600
    delay = phase.get('next_poll_seconds', 30)
    if type(delay) is not int or not 0 <= delay <= 300:
        raise ValueError('dataset version polling delay is outside its bounded contract')
    return delay


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
            long_capture = workflow.patched('source-replication-long-catalog-capture-v1')
            result = await workflow.execute_activity(stage_source_replication,
                                                     start_to_close_timeout=timedelta(hours=24 if long_capture else 2),
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
        # While backup is explicitly deferred, keep the already committed
        # snapshot without rescanning the entire D catalog every minute.
        pending_snapshot = request.get('pending_snapshot')
        for _ in range(60):
            self.phase = {"state": "registering_dataset_versions"}
            if pending_export:
                catalog = {'snapshot_id': pending_export['snapshot_id']}
            elif pending_snapshot is not None:
                catalog = {'snapshot_id': pending_snapshot}
            else:
                long_capture = workflow.patched('storage-long-catalog-capture-v1')
                catalog = await workflow.execute_activity(register_versions,
                    start_to_close_timeout=timedelta(hours=24) if long_capture else timedelta(minutes=20),
                    heartbeat_timeout=timedelta(seconds=60), retry_policy=retry)
            if catalog.get('state') in {'waiting_catalog_owner', 'waiting_cold_physical_capacity'}:
                self.phase = catalog
                await workflow.sleep(timedelta(seconds=catalog['retry_after_seconds']))
                continue
            if catalog["snapshot_id"] != last:
                self.phase = {"state": "exporting_version", "snapshot_id": catalog["snapshot_id"]}
                if pending_export:
                    exported = pending_export
                else:
                    # Full catalog exports can exceed 20 minutes even while
                    # heartbeats and the original owner remain healthy. Keep
                    # old history commands unchanged during replay; new
                    # exports have the same 24-hour bound as registration.
                    long_export = workflow.patched('storage-long-catalog-export-v1')
                    if long_export:
                        exported = await workflow.execute_activity(
                            export_version, args=[catalog["snapshot_id"], True],
                            start_to_close_timeout=timedelta(hours=24),
                            heartbeat_timeout=timedelta(seconds=60), retry_policy=retry)
                    else:
                        exported = await workflow.execute_activity(
                            export_version, catalog["snapshot_id"],
                            start_to_close_timeout=timedelta(minutes=20),
                            heartbeat_timeout=timedelta(seconds=60), retry_policy=retry)
                    if exported['state'] == 'reschedule_with_long_timeout':
                        if not workflow.patched('storage-reschedule-short-catalog-export-v1'):
                            raise ValueError('short export reschedule requires the recorded workflow patch')
                        if exported['snapshot_id'] != catalog['snapshot_id']:
                            raise ValueError('short export reschedule changed the fixed snapshot')
                        exported = await workflow.execute_activity(
                            export_version, args=[catalog["snapshot_id"], True],
                            start_to_close_timeout=timedelta(hours=24),
                            heartbeat_timeout=timedelta(seconds=60), retry_policy=retry)
                pending_export = None
                if exported['state'] == 'waiting_user_migration':
                    self.phase = {'state': 'waiting_user_migration', 'snapshot_id': exported['snapshot_id']}
                    if workflow.patched('storage-reuse-paused-snapshot-v1'):
                        pending_snapshot = exported['snapshot_id']
                    await workflow.sleep(timedelta(seconds=60))
                    continue
                pending_snapshot = None
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
        workflow.continue_as_new({"last_snapshot": last,
            **({'pending_snapshot': pending_snapshot} if pending_snapshot is not None else {})})
