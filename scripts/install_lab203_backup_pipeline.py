#!/usr/bin/env python3
"""One local v10 upgrade: measure this NAS, then replace the service driver.

Reuse the installed v7 Mamba role, local credentials, timer, semantic post-hook
and owner. Keep the previous worker and unit definitions for rollback.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT))

from scripts.backup_delivery_receipt import HASH, read_json, valid_signature
from scripts.verify_backup_delivery import verify
from scripts.benchmark_lab203_backup_pipeline import benchmark
from stockagent.data_sync.backup_relay_pipeline import PAIRED, validate_configuration
from stockagent.data_sync.desync_snapshots import atomic_write_bytes
from stockagent.data_sync.offhost_backup import private_file, private_json
from stockagent.runtime_identity import runtime_identity, validate_runtime_lock


def execute(argv, timeout=120):
    result = subprocess.run(list(map(str,argv)), capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError("local pipeline installation command failed; retain private evidence")
    return result.stdout.decode("utf-8-sig")


def dropin(code):
    path = str(code / "scripts/run_lab203_backup_pipeline.sh")
    if any(c in path for c in '\n\r%"\\'):
        raise ValueError("fixed installed service path contains unsupported systemd syntax")
    return ("[Service]\n# One controller owns bounded concurrent jobs; v7 semantic post-hook stays installed.\n"
            "ExecStart=\n" + f'ExecStart=/bin/bash "{path}"\n')


def timer_dropin():
    return ("[Timer]\n# Run the same owner promptly after completion, including requeued jobs.\n"
            "OnCalendar=\nOnBootSec=\nOnUnitInactiveSec=\nOnBootSec=30s\nOnUnitInactiveSec=15s\nRandomizedDelaySec=0\nAccuracySec=1s\n")


def previous_parallel_configuration(path,old,pins,ingress):
    """Adopt only known installed journals; retain its configuration untouched."""
    if not path.exists():
        return None
    private_file(path)
    previous=read_json(path)
    validate_configuration(previous)
    authoritative=(*PAIRED,"owner_lock","receipt_root","nas_configuration","password_file","restic",
                   "runtime_lock","windows_distribution","powershell")
    if any(previous[k] != old[k] for k in authoritative) or any(previous[k] != pins[k] for k in PAIRED):
        raise ValueError("previous pipeline differs from the existing owner/runtime/NAS configuration")
    if Path(previous["ingress_root"]) != ingress:
        raise ValueError("previous pipeline uses another ingress authority")
    installed=None
    for version in ("v8","v9"):
        manifest_path=ingress / ("tools/continuous-backup-20261004-"+version+"/handoff-manifest.json")
        if manifest_path.is_file():
            manifest=read_json(manifest_path)
            valid_signature(manifest,"frozen_backup_receiver_handoff_v1")
            if any(manifest[k] != pins[k] for k in PAIRED):
                raise ValueError("previous frozen package belongs to another paired deployment")
            if manifest["identity_sha256"] == previous["installed_handoff_identity_sha256"]:
                installed=version
    if installed is None:
        raise ValueError("previous installed pipeline has no known frozen identity")
    expected_state=Path(old["state_root"]).parent / ("pipeline-state-"+installed)
    expected_scratch=Path(old["scratch_root"]).parent / ("lab203-backup-pipeline-scratch-"+installed)
    if Path(previous["state_root"]) != expected_state or Path(previous["scratch_root"]) != expected_scratch:
        raise ValueError("previous retry journal or scratch root is outside its installed namespace")
    return previous


def install(args):
    if os.geteuid() != 0:
        raise ValueError("upgrade locally as the existing lab203 root owner")
    started = time.perf_counter()
    pins = read_json(args.bootstrap)
    valid_signature(pins, "fixed_automatic_backup_bootstrap_v1")
    if (pins["package_relative"] != "tools/continuous-backup-20261004-v10"
            or pins["one_time_local_installation_required"] is not True
            or pins["receiver_hook_deployed"] is not False):
        raise ValueError("use the fixed v10 parallel pipeline bootstrap")
    package = args.bootstrap.parent.parent / pins["package_relative"]
    if package != ROOT or any(p.is_symlink() for p in (package,*package.parents)):
        raise ValueError("invoke the installer from its fixed closed v10 package")
    verified = verify(package, pins["envelope_identity_sha256"], workers=4)
    manifest_path = package / "handoff-manifest.json"
    if hashlib.sha256(manifest_path.read_bytes()).hexdigest() != pins["handoff_manifest_sha256"]:
        raise ValueError("v10 raw handoff manifest differs from its source pin")
    manifest = read_json(manifest_path)
    valid_signature(manifest,"frozen_backup_receiver_handoff_v1")
    if manifest["private_credentials_included"] is not False or manifest["automatic_execution_from_syncthing"] is not False:
        raise ValueError("upgrade must keep credentials private and install fixed local code")
    old_configuration = Path("/etc/lab203-backup/recovery-queue.json")
    private_file(old_configuration)
    old = read_json(old_configuration)
    for key in ("producer_device_id","receiver_device_id","repository_id"):
        if old[key] != pins[key] or old[key] != manifest[key]:
            raise ValueError("v10 identities differ from the existing local deployment")
    if validate_runtime_lock(read_json(Path(old["runtime_lock"])),runtime_identity()):
        raise ValueError("run this installer using the already accepted v7 Mamba role")
    previous=previous_parallel_configuration(Path("/etc/lab203-backup/backup-pipeline.json"),old,pins,
        Path("/srv/lab203-backup/ingress"))
    code = Path("/opt/lab203-backup/backup-pipeline-code-v10")
    configuration = Path("/etc/lab203-backup/backup-pipeline-v10.json")
    environment = configuration.with_suffix(".env")
    service = Path("/etc/systemd/system/lab203-backup.service.d/zz-parallel-pipeline-v10.conf")
    timer = Path("/etc/systemd/system/lab203-backup.timer.d/zz-parallel-pipeline-v10.conf")
    state = Path(previous["state_root"]) if previous else Path("/srv/lab203-backup/pipeline-state-v10")
    scratch = Path(previous["scratch_root"]) if previous else Path("/var/lib/lab203-backup-pipeline-scratch-v10")
    fresh=(code,configuration,environment,service,timer,args.evidence)
    if previous is None:
        fresh=(*fresh,state,scratch)
    for p in fresh:
        if p.exists() or any(q.is_symlink() for q in (p,*p.parents)):
            raise ValueError("retain any previous parallel installation/evidence and inspect it")
    if not (Path(sys.prefix) / "conda-meta").is_dir():
        raise ValueError("reuse the actual installed Mamba role, not system Python")
    execute(["systemctl","is-enabled","lab203-backup.timer"])
    execute(["systemctl","is-active","lab203-backup.timer"])
    before = execute(["systemctl","cat","lab203-backup.service","lab203-backup.timer"])
    if "run_lab203_recovery_queue.sh" not in before:
        raise ValueError("preserve the already installed v7 semantic recovery hook")
    args.evidence.mkdir(mode=0o700,parents=True)
    atomic_write_bytes(args.evidence / "service-before.private.txt",before.encode(),mode=0o600)
    c = {"schema_version":1, **{k:old[k] for k in ("producer_device_id","receiver_device_id","repository_id",
        "receipt_root","owner_lock","nas_configuration","password_file","restic","runtime_lock","windows_distribution","powershell")},
        "ingress_root":"/srv/lab203-backup/ingress", "state_root":str(state),"scratch_root":str(scratch),
        "minimum_free_bytes":old["minimum_free_bytes"],"maximum_batch_bytes":8*1024**3,
        "maximum_scratch_bytes":64*1024**3,"maximum_jobs_per_cycle":4,"readiness_max_age_seconds":900,
        "job_workers":4,"backup_workers":2,"restore_workers":2,"verify_workers":4,
        "retry_delays_seconds":[30,120,600],"installed_handoff_identity_sha256":manifest["identity_sha256"]}
    validate_configuration(c)
    private_json(args.evidence / "candidate.private.json",c)
    # Pause only the timer; let an already running real batch finish normally.
    execute(["systemctl","stop","lab203-backup.timer"])
    activated = False
    try:
        deadline = time.monotonic()+8*3600
        while execute(["systemctl","show","lab203-backup.service","--property=ActiveState","--value"]).strip() in {"active","activating","deactivating"}:
            if time.monotonic() >= deadline:
                raise TimeoutError("existing backup has not finished; preserve its owner")
            time.sleep(5)
        measurements = benchmark(c,args.evidence / "actual-nas-measurement")
        c.update(job_workers=measurements["selected_job_workers"],backup_workers=measurements["selected_backup_workers"],
            restore_workers=measurements["selected_restore_workers"])
        shutil.copytree(package,code)
        verify(code,pins["envelope_identity_sha256"],workers=4)
        private_json(configuration,c)
        body = "FINTECH_ENV_PATH="+shlex.quote(sys.prefix)+"\n"
        atomic_write_bytes(environment,body.encode(),mode=0o600)
        for p in (service,timer):
            p.parent.mkdir(mode=0o755,parents=True,exist_ok=True)
        atomic_write_bytes(service,dropin(code).encode(),mode=0o644)
        atomic_write_bytes(timer,timer_dropin().encode(),mode=0o644)
        execute(["systemd-analyze","verify","/etc/systemd/system/lab203-backup.service","/etc/systemd/system/lab203-backup.timer"])
        execute(["systemctl","daemon-reload"])
        loaded = execute(["systemctl","show","lab203-backup.service","--property=ExecStart,ExecStartPost","--value"])
        if str(code / "scripts/run_lab203_backup_pipeline.sh") not in loaded or "run_lab203_recovery_queue.sh" not in loaded:
            raise ValueError("loaded service lost its fixed pipeline or semantic recovery hook")
        execute(["systemctl","start","--no-block","lab203-backup.service"])
        activated = True
    except Exception:
        # Keep package, benchmark and journals as evidence. Remove only the two
        # exact newly written drop-ins; restore the old driver, never its data.
        for path,expected in ((service,dropin(code)),(timer,timer_dropin())):
            if path.is_file() and not path.is_symlink() and path.read_text() == expected:
                path.unlink()
        execute(["systemctl","daemon-reload"])
        raise
    finally:
        execute(["systemctl","start","lab203-backup.timer"])
    result = {"state":"locally_measured_parallel_driver_activated", "driver_activated":activated,
        "existing_worker_preserved":True,"existing_owner_reused":True,"semantic_post_hook_preserved":True,
        "previous_parallel_configuration_preserved":previous is not None,"previous_retry_journals_reused":previous is not None,
        "mamba_role_reused":True,"measured_policy":{k:c[k] for k in ("job_workers","backup_workers","restore_workers","verify_workers")},
        "frozen_package_files_verified":verified["files_verified"],"handoff_identity_sha256":manifest["identity_sha256"],
        "private_credentials_published":False,"automatic_pruning":False,"automatic_ingress_deletion":False,
        "complete_installation_seconds":time.perf_counter()-started,"production_acceptance_requires_returned_pipeline_status":True}
    private_json(args.evidence / "installation.json",result)
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap",type=Path,default=Path("/srv/lab203-backup/ingress/tools/automatic-backup-bootstrap-v10.json"))
    parser.add_argument("--evidence",type=Path,default=Path("/var/log/lab203-backup/parallel-v10-"+time.strftime("%Y%m%dT%H%M%S")))
    args=parser.parse_args()
    os.umask(0o077)
    try:
        print(json.dumps(install(args)))
    except Exception as error:
        print(json.dumps({"state":"installation_failed","error_type":type(error).__name__}))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
