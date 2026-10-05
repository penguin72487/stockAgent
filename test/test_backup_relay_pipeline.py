"""Real encrypted local Restic, simulated CIFS/Windows admission, no live NAS."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import threading
import time

import pytest

from scripts.backup_delivery_receipt import DISPATCH, atomic_public, read_json, signed, validate_ack
from scripts.verify_backup_delivery import CONTRACT
from stockagent.data_sync import backup_relay_pipeline as module
from stockagent.data_sync.offhost_backup import ResticBackup, private_json
from stockagent.runtime_identity import runtime_identity


@pytest.fixture
def relay(tmp_path, monkeypatch):
    binary = os.environ.get("STOCKAGENT_TEST_RESTIC_BINARY") or shutil.which("restic")
    if not binary:
        pytest.skip("provide the accepted role Restic binary")
    nas = tmp_path / "nas"
    user = nas / "students/user"
    user.mkdir(parents=True)
    password = tmp_path / "password.private"
    password.write_text("test-only-" + os.urandom(16).hex())
    password.chmod(0o600)
    runtime = tmp_path / "runtime-lock.json"
    private_json(runtime, runtime_identity())
    profile = tmp_path / "nas.private.json"
    private_json(profile, {"schema_version": 1, "automatic_pruning": False, "reserve_bytes": 1024,
        "nas": {"host": "test-nas", "share": "Lab203", "mount_point": str(nas),
                "relative_directory": "students/user", "repository_directory": "repository"}})
    backend = ResticBackup(Path(binary), str(user / "repository"), password, cache=tmp_path / "init-cache")
    repository = backend.initialize()["id"]
    ingress, receipts, state, scratch = [tmp_path / name for name in ("ingress", "receipts", "state", "scratch")]
    for p in (ingress, receipts, state, scratch):
        p.mkdir(mode=0o700)
    lock = tmp_path / "existing-owner.lock"
    lock.touch()
    c = {"schema_version": 1, "producer_device_id": "test-source", "receiver_device_id": "test-relay",
        "repository_id": repository, "ingress_root": str(ingress), "receipt_root": str(receipts),
        "state_root": str(state), "scratch_root": str(scratch), "owner_lock": str(lock),
        "nas_configuration": str(profile), "password_file": str(password), "restic": str(Path(binary).absolute()),
        "runtime_lock": str(runtime), "minimum_free_bytes": 1024, "maximum_batch_bytes": 16*1024**2, "readiness_max_age_seconds": 900,
        "maximum_scratch_bytes": 128*1024**2, "maximum_jobs_per_cycle": 4, "job_workers": 4,
        "backup_workers": 2, "restore_workers": 2, "verify_workers": 2, "retry_delays_seconds": [30,120,600],
        "windows_distribution": "", "powershell": str(Path("/bin/true").resolve()), "installed_handoff_identity_sha256": "1"*64}
    monkeypatch.setattr("stockagent.data_sync.nas_target.mounted_volume", lambda _: (7,"cifs","//test-nas/Lab203"))
    monkeypatch.setattr(module, "physical_free_bytes", lambda _: shutil.disk_usage(tmp_path).free)
    return c, backend


def delivery(c, number, *, size=1024**2):
    raw = os.urandom(size)
    digest = hashlib.sha256(raw).hexdigest()
    relative = f"cold/objects/blobs/{digest[:2]}/{digest}.blob"
    envelope = signed({"contract": CONTRACT, "source_plan_identity_sha256": "2"*64,
        "files": [{"relative":relative,"sha256":digest,"bytes":len(raw)}],
        "producer_node_id":"penguin", "source_scope":"closed test source", "selection":"incremental_object_wave",
        "all_history_verified":False,"unpublished_sources_included":False})
    key = envelope["identity_sha256"]
    root = Path(c["ingress_root"]) / ("batch-"+key)
    (root / relative).parent.mkdir(parents=True)
    (root / relative).write_bytes(raw)
    private_json(root / "backup-envelope.json", envelope)
    (root / "READY").write_text(key+"\n")
    dispatch = signed({"contract": DISPATCH, **{k:c[k] for k in module.PAIRED},
        "batch_relative":root.name,"envelope_identity_sha256":key,
        "envelope_file_sha256":hashlib.sha256((root/"backup-envelope.json").read_bytes()).hexdigest(),
        "complete_files":3,"complete_bytes":len(raw)+(root/"backup-envelope.json").stat().st_size+(root/"READY").stat().st_size,
        "catalog_identity_sha256":"2"*64,"delivery_kind":"incremental_cold_objects", "complete_release_in_this_delivery":False})
    atomic_public(Path(c["ingress_root"])/"tools/dispatch"/(key+".json"),dispatch)
    return dispatch, root


def test_real_restic_parallel_cohort_ack_and_restart(relay, monkeypatch):
    c, backend = relay
    entries = [delivery(c,n) for n in range(4)]
    real = ResticBackup.run
    counts, active, peak = {}, set(), 0
    guard = threading.Lock()

    def traced(self, arguments, **kwargs):
        nonlocal peak
        operation = arguments[0]
        with guard:
            counts[operation] = counts.get(operation,0)+1
            if operation == "check":
                assert not active, "exclusive check must wait for all backup/restore children"
            if operation in ("backup","restore"):
                active.add(threading.current_thread().name)
                peak = max(peak,len(active))
        try:
            return real(self,arguments,**kwargs)
        finally:
            with guard:
                active.discard(threading.current_thread().name)

    monkeypatch.setattr(ResticBackup,"run",traced)
    status = module.run_cycle(c)
    assert peak >= 2 and counts["check"] == 1 and counts["backup"] == 4 and counts["restore"] == 4
    for dispatch,_ in entries:
        ack = read_json(Path(c["receipt_root"])/("acceptance-"+dispatch["envelope_identity_sha256"]+".json"))
        validate_ack(ack,dispatch)
        journal = read_json(Path(c["state_root"])/(dispatch["envelope_identity_sha256"]+".json"))
        assert journal["success_scratch_removed"] is True
    assert not list(Path(c["scratch_root"]).iterdir())
    before = counts.copy()
    module.run_cycle(c)
    assert counts.get("backup") == before["backup"] and counts.get("restore") == before["restore"]
    assert counts["check"] == before["check"]
    assert status["parallelism"]["job_workers"] == 4


def test_corrupt_batch_requeues_without_blocking_other_batches(relay):
    c,_ = relay
    bad,batch = delivery(c,0)
    good,_ = delivery(c,1)
    payload = next((batch/"cold").rglob("*.blob"))
    payload.write_bytes(b"x"*payload.stat().st_size)
    status = module.run_cycle(c)
    states = {j["envelope_identity_sha256"]:j["state"] for j in status["jobs"]}
    assert states[bad["envelope_identity_sha256"]] == "retry_wait"
    assert states[good["envelope_identity_sha256"]] == "accepted"
    assert not (Path(c["receipt_root"])/("acceptance-"+bad["envelope_identity_sha256"]+".json")).exists()


def test_repository_check_failure_retains_then_reuses_verified_restore(relay,monkeypatch):
    c,_ = relay
    dispatch,_ = delivery(c,0)
    real = ResticBackup.run
    counts = {}

    def failing(self,arguments,**kwargs):
        counts[arguments[0]] = counts.get(arguments[0],0)+1
        if arguments[0] == "check" and counts["check"] == 1:
            raise module.SnapshotError("simulated transient repository check failure")
        return real(self,arguments,**kwargs)

    monkeypatch.setattr(ResticBackup,"run",failing)
    first = module.run_cycle(c)
    assert first["jobs"][0]["state"] == "retry_wait"
    key = dispatch["envelope_identity_sha256"]
    path = Path(c["state_root"])/(key+".json")
    item = read_json(path)
    snapshot = item["snapshot_id"]
    item["next_attempt_epoch"] = 0
    private_json(path,item)
    second = module.run_cycle(c)
    assert second["jobs"][0]["state"] == "accepted"
    assert counts["backup"] == 1 and counts["restore"] == 1 and counts["check"] == 2
    assert read_json(Path(c["receipt_root"])/("acceptance-"+key+".json"))["snapshot_id"] == snapshot


def test_partial_tagged_snapshot_is_rejected_then_replaced_by_complete_backup(relay):
    c,backend=relay
    dispatch,batch=delivery(c,0)
    key=dispatch["envelope_identity_sha256"]
    # Real incomplete source snapshot, tagged like a pre-crash attempt. Restic
    # itself exits zero because its exclusion is intentional; source SHA fails.
    result=backend.run(["backup","--json","--exclude","*.blob","--tag",key,
        "--tag",dispatch["identity_sha256"],str(batch)])
    partial=[json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    partial=next(v["snapshot_id"] for v in partial if v.get("message_type") == "summary")
    journal=Path(c["state_root"])/(key+".json")
    private_json(journal,{"attempts":1,"last_attempt_epoch":0,"next_attempt_epoch":0,"state":"backup"})
    failed=module.run_cycle(c)
    assert failed["jobs"][0]["state"] == "retry_wait"
    item=read_json(journal)
    assert item["rejected_snapshot_ids"] == [partial] and "snapshot_id" not in item
    assert all(Path(p).is_dir() for p in item["retained_failed_outputs"])
    item["next_attempt_epoch"]=0
    private_json(journal,item)
    accepted=module.run_cycle(c)
    assert accepted["jobs"][0]["state"] == "accepted"
    ack=read_json(Path(c["receipt_root"])/("acceptance-"+key+".json"))
    assert ack["snapshot_id"] != partial
    assert all(Path(p).is_dir() for p in read_json(journal)["retained_failed_outputs"])


def test_owner_collision_never_publishes_acceptance(relay):
    import fcntl
    c,_ = relay
    delivery(c,0)
    with Path(c["owner_lock"]).open("a") as owner:
        fcntl.flock(owner,fcntl.LOCK_EX|fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            module.run_cycle(c)
    assert not list(Path(c["receipt_root"]).glob("acceptance-*.json"))


def test_capacity_and_unknown_fields_fail_closed(relay):
    c,_ = relay
    unsafe = deepcopy(c)
    unsafe["command"] = "execute remote data"
    with pytest.raises(ValueError):
        module.run_cycle(unsafe)
    dispatch,_ = delivery(c,0)
    small = deepcopy(c)
    small["maximum_scratch_bytes"] = dispatch["complete_bytes"]-1
    status = module.run_cycle(small)
    assert status["jobs"][0]["state"] == "queued"


def test_real_local_measurement_uses_isolated_repository_and_all_acceptance_gates(relay):
    from scripts.benchmark_lab203_backup_pipeline import benchmark
    c,backend = relay
    for number in range(4):
        delivery(c,number)
    evidence = Path(c["state_root"]).parent / "actual-measurement"
    result = benchmark(c,evidence)
    assert result["production_repository_modified"] is False
    assert len(result["samples"]) == 8
    assert all(r["all_four_fixed_snapshot_full_sha_verified"] for r in result["samples"])
    assert result["selected_job_workers"] in (2,4)
    assert result["fresh_repository_each_trial"] is True
    assert len({r["fresh_repository_id"] for r in result["samples"]}) == 8
    assert json.loads(backend.run(["snapshots","--json"]).stdout) == []
    assert not list(Path(c["receipt_root"]).glob("acceptance-*.json"))


def test_parallel_status_rejects_private_fields_and_unpinned_code(relay):
    c,_ = relay
    transport=Path(c["ingress_root"])
    receipts=Path(c["receipt_root"])
    status = module.run_cycle(c)
    manifest=signed({"contract":"frozen_backup_receiver_handoff_v1","fixed_code":"v8"})
    atomic_public(transport/"tools/continuous-backup-20261004-v8/handoff-manifest.json",manifest)
    # Installed code identity must equal this exact source-pinned package.
    with pytest.raises(ValueError):
        module.receiver_status(c,transport,receipts)
    status["installed_handoff_identity_sha256"]=manifest["identity_sha256"]
    status=signed({k:v for k,v in status.items() if k != "identity_sha256"})
    atomic_public(receipts/"pipeline-status.json",status,replace=True)
    assert module.receiver_status(c,transport,receipts)["state"] == "parallel_receiver_active"
    # Delivery of a new frozen version is not remote activation. Continue to
    # recognize the exact installed v8 and report an upgrade independently.
    future=signed({"contract":"frozen_backup_receiver_handoff_v1","fixed_code":"v10"})
    atomic_public(transport/"tools/continuous-backup-20261004-v10/handoff-manifest.json",future)
    observed=module.receiver_status(c,transport,receipts)
    assert observed["state"] == "parallel_receiver_active" and observed["upgrade_pending"] is True
    assert observed["installed_package_version"] == "v8" and observed["available_package_version"] == "v10"
    status["private_password"]="must never return"
    status=signed({k:v for k,v in status.items() if k != "identity_sha256"})
    atomic_public(receipts/"pipeline-status.json",status,replace=True)
    with pytest.raises(ValueError):
        module.receiver_status(c,transport,receipts)


def test_upgrade_reuses_known_retry_journals_without_replacing_previous_configuration(relay):
    from scripts.install_lab203_backup_pipeline import previous_parallel_configuration
    c,_=relay
    ingress=Path(c["ingress_root"])
    old=deepcopy(c)
    pins={key:c[key] for key in module.PAIRED}
    manifest=signed({"contract":"frozen_backup_receiver_handoff_v1","fixed_code":"v8",**pins})
    atomic_public(ingress/"tools/continuous-backup-20261004-v8/handoff-manifest.json",manifest)
    previous=deepcopy(c)
    previous.update(state_root=str(Path(c["state_root"]).parent/"pipeline-state-v8"),
        scratch_root=str(Path(c["scratch_root"]).parent/"lab203-backup-pipeline-scratch-v8"),
        installed_handoff_identity_sha256=manifest["identity_sha256"])
    journal=Path(previous["state_root"])/("a"*64+".json")
    private_json(journal,{"snapshot_id":"b"*64,"state":"retry_wait"})
    path=Path(c["state_root"]).parent/"previous-pipeline.json"
    private_json(path,previous)
    before=path.read_bytes(),journal.read_bytes()
    accepted=previous_parallel_configuration(path,old,pins,ingress)
    assert accepted["state_root"] == previous["state_root"]
    assert accepted["scratch_root"] == previous["scratch_root"]
    assert (path.read_bytes(),journal.read_bytes()) == before
    for key in ("owner_lock","runtime_lock","state_root","scratch_root"):
        bad=deepcopy(previous)
        bad[key]=str(Path(bad[key]).parent/"different-authority")
        private_json(path,bad)
        with pytest.raises(ValueError):
            previous_parallel_configuration(path,old,pins,ingress)
    bad=deepcopy(previous)
    bad["installed_handoff_identity_sha256"]="d"*64
    private_json(path,bad)
    with pytest.raises(ValueError):
        previous_parallel_configuration(path,old,pins,ingress)
    assert journal.read_bytes() == before[1]


def test_failed_receipt_publication_requeues_only_its_job(relay,monkeypatch):
    c,_=relay
    bad,_=delivery(c,0)
    good,_=delivery(c,1)
    actual=module.publish
    def publish_one(batch,dispatch,*args,**kwargs):
        if dispatch["envelope_identity_sha256"] == bad["envelope_identity_sha256"]:
            raise OSError("simulated transient public receipt write failure")
        return actual(batch,dispatch,*args,**kwargs)
    monkeypatch.setattr(module,"publish",publish_one)
    status=module.run_cycle(c)
    states={j["envelope_identity_sha256"]:j["state"] for j in status["jobs"]}
    assert states[bad["envelope_identity_sha256"]] == "retry_wait"
    assert states[good["envelope_identity_sha256"]] == "accepted"


def test_accepted_scratch_cleanup_resumes_even_after_ingress_is_retired(relay,monkeypatch):
    c,_=relay
    dispatch,batch=delivery(c,0)
    monkeypatch.setattr(module,"process_references_many",lambda _: {"busy-test":True})
    assert module.run_cycle(c)["jobs"][0]["state"] == "accepted"
    key=dispatch["envelope_identity_sha256"]
    assert Path(read_json(Path(c["state_root"])/(key+".json"))["output"]).is_dir()
    shutil.rmtree(batch)  # Simulate separately authorized source transport retirement.
    monkeypatch.setattr(module,"process_references_many",lambda _: {})
    module.run_cycle(c)
    assert read_json(Path(c["state_root"])/(key+".json"))["success_scratch_removed"] is True


def test_source_transfer_nas_restore_ack_pipeline_overlaps_real_jobs(relay,monkeypatch):
    """Synthetic canonical source, simulated transport, real encrypted Restic."""
    from concurrent.futures import ThreadPoolExecutor
    from stockagent.data_sync import backup_stream as source_module
    from stockagent.data_sync.backup_stream import BackupStream
    from stockagent.data_sync.packed_snapshots import initialize_packed_layout,publish_packed_snapshot
    c,_=relay
    base=Path(c["state_root"]).parent
    cold,source=base/"cold-source",base/"loose-source"
    initialize_packed_layout(cold,node_id="penguin")
    source.mkdir()
    for n in range(4):
        (source/f"real-test-{n}.bin").write_bytes(os.urandom(1024**2))
    publish_packed_snapshot(cold,"test-data",source,loose_file_threshold_bytes=1024,pack_buckets=2)
    catalog=base/"source-publication.json"
    catalog.write_text(json.dumps({"datasets":[{"dataset":"test-data","publish":True}]}))
    transport=base/"source-transport"
    transport.mkdir()
    (transport/".stfolder").mkdir()
    (transport/".stignore").write_text("(?d).staging\n(?d).staging/**\n")
    configuration={"schema_version":1, **{k:c[k] for k in module.PAIRED}, "cold_root":str(cold),
        "publication_catalog":str(catalog),"transport_root":str(transport),"receipt_root":str(base/"source-receipts"),
        "state_root":str(base/"source-state"),"required_mounts":{},"require_transport_mount":False,
        "maximum_batch_bytes":2*1024**2,"maximum_batch_files":2,"maximum_pending_bytes":8*1024**2,
        "maximum_pending_deliveries":4,"maximum_retained_transport_bytes":64*1024**2,
        "reserve_bytes":1024,"readiness_max_age_seconds":900,"automatic_pruning":False,
        "automatic_batch_deletion":False,"source_cleanup_authorized":False,
        "pipeline":{"maximum_waves_per_cycle":4,"copy_workers":2,"verify_workers":2,"retry_delays_seconds":[30,120,600]}}
    queue=BackupStream(configuration)
    queue.receipts.mkdir()
    module.run_cycle(c)
    atomic_public(queue.receipts/"readiness.json",read_json(Path(c["receipt_root"])/"readiness.json"))
    nas_started=threading.Event()
    source_done=threading.Event()
    guard=threading.Lock()
    active=set()
    overlap=[]
    actual_restic=ResticBackup.run
    def traced(self,args,**kwargs):
        operation=args[0]
        if operation == "backup":
            with guard: active.add(threading.current_thread().name)
            nas_started.set()
        try:
            return actual_restic(self,args,**kwargs)
        finally:
            if operation == "backup":
                with guard: active.discard(threading.current_thread().name)
    monkeypatch.setattr(ResticBackup,"run",traced)
    actual_export=source_module.export_incremental_delivery
    exports=0
    def exporting(*args,**kwargs):
        nonlocal exports
        exports+=1
        if exports == 2:
            assert nas_started.wait(15),"receiver must start without waiting for source's entire cycle"
            with guard: overlap.append(bool(active))
        return actual_export(*args,**kwargs)
    monkeypatch.setattr(source_module,"export_incremental_delivery",exporting)
    def transfer(ledger):
        for item in ledger["deliveries"].values():
            dispatch=item["dispatch"]
            destination=Path(c["ingress_root"])/dispatch["batch_relative"]
            if not destination.exists():
                shutil.copytree(queue.transport/dispatch["batch_relative"],destination)
            atomic_public(Path(c["ingress_root"])/"tools/dispatch"/(dispatch["envelope_identity_sha256"]+".json"),dispatch,replace=True)
    monkeypatch.setattr(queue,"scan_transport",transfer)
    def receiving():
        deadline=time.monotonic()+45
        while time.monotonic() < deadline:
            module.run_cycle(c)
            for path in Path(c["receipt_root"]).glob("acceptance-*.json"):
                atomic_public(queue.receipts/path.name,read_json(path),replace=True)
            if source_done.is_set():
                keys=set(queue.load_ledger()["deliveries"])
                returned={p.name.removeprefix("acceptance-").removesuffix(".json") for p in queue.receipts.glob("acceptance-*.json")}
                if keys and keys <= returned:
                    return
            time.sleep(0.02)
        raise AssertionError("pipeline failed to return all exact acceptance receipts")
    with ThreadPoolExecutor(max_workers=1) as pool:
        receiver=pool.submit(receiving)
        queue.cycle()
        source_done.set()
        receiver.result(timeout=50)
    inspected=queue.cycle(publish=False)
    assert overlap == [True]
    assert len(queue.load_ledger()["deliveries"]) >= 2
    assert inspected["pending_delivery_count"] == 0
    assert all(item.get("acceptance") for item in queue.load_ledger()["deliveries"].values())
