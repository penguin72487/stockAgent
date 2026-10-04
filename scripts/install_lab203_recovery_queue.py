#!/usr/bin/env python3
"""One-time local installation; keep the current lab203 worker and timer intact.

Run only on lab203 after verifying the frozen handoff with the already trusted
source verifier. This installer adds one fixed ExecStartPost, never a command
interpreter, remote login, another backup owner or NAS pruning.
"""
from __future__ import annotations

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
# A synced closed package must remain byte-for-byte unchanged during bootstrap.
# The main script itself is not cached; disable caches before package imports.
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT))

from scripts.backup_delivery_receipt import read_json, valid_signature  # noqa: E402
from scripts.verify_backup_delivery import verify  # noqa: E402
from stockagent.data_sync.desync_snapshots import atomic_write_bytes  # noqa: E402
from stockagent.data_sync.nas_target import NasTarget  # noqa: E402
from stockagent.data_sync.offhost_backup import private_file, private_json  # noqa: E402
from stockagent.data_sync.recovery_queue import physical_free_bytes, validate_configuration  # noqa: E402
from stockagent.runtime_identity import runtime_identity, validate_runtime_lock  # noqa: E402


def execute(argv, *, cwd=None, timeout=120, umask=0o077):
    result = subprocess.run(list(map(str, argv)), cwd=cwd, capture_output=True, text=True,
                            timeout=timeout, umask=umask)
    if result.returncode:
        raise RuntimeError("local recovery installation command failed; private output was not published")
    return result.stdout


def dropin(code_root: Path) -> str:
    # A fixed installed path; no command or path is taken from a sync request.
    path = str(code_root / "scripts/run_lab203_recovery_queue.sh")
    if any(c in path for c in '\n\r%"\\'):
        raise ValueError("installed hook path contains unsupported systemd syntax")
    return ("[Service]\n# Recovery failures have their own receipts; continue normal file backup.\n"
            f'ExecStartPost=-/bin/bash "{path}"\n')


def service_files(exclude: Path | None = None) -> dict:
    result = {}
    for name in ("lab203-backup.service", "lab203-backup.timer"):
        unit = Path("/etc/systemd/system") / name
        for path in [unit, *sorted(unit.with_name(name + ".d").glob("*.conf"))]:
            if path == exclude:
                continue
            if not path.is_file() or path.is_symlink():
                raise ValueError("existing backup unit is missing or redirected")
            result[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def bootstrap_pins(path: Path) -> dict:
    value = read_json(path)
    valid_signature(value, "fixed_automatic_backup_bootstrap_v1", fields={"contract", "producer_device_id",
        "receiver_device_id", "repository_id", "package_relative", "envelope_identity_sha256",
        "handoff_manifest_sha256", "one_time_local_installation_required", "receiver_hook_deployed", "identity_sha256"})
    if (value["package_relative"] != "tools/continuous-backup-20261004-v7"
            or value["one_time_local_installation_required"] is not True or value["receiver_hook_deployed"] is not False):
        raise ValueError("use the explicit fixed v7 one-time bootstrap")
    from scripts.backup_delivery_receipt import HASH
    if any(not HASH.fullmatch(value[k]) for k in ("envelope_identity_sha256", "handoff_manifest_sha256", "repository_id")):
        raise ValueError("bootstrap lacks full pinned identities")
    return value


def install(args) -> dict:
    started = time.perf_counter()
    if os.geteuid() != 0 or "microsoft" not in Path("/proc/sys/kernel/osrelease").read_text().lower():
        raise ValueError("install this hook locally on lab203 WSL as the existing root owner")
    if args.evidence.exists():
        raise ValueError("preserve previous installation evidence")
    native_before = runtime_identity()
    package = args.package.absolute()
    verified = verify(package, args.envelope_identity)
    manifest = read_json(package / "handoff-manifest.json")
    valid_signature(manifest, "frozen_backup_receiver_handoff_v1")
    if hashlib.sha256((package / "handoff-manifest.json").read_bytes()).hexdigest() != args.manifest_sha256:
        raise ValueError("frozen package raw manifest differs from the independently pinned handoff")
    if manifest["replace_existing_worker"] is not False or manifest["private_credentials_included"] is not False:
        raise ValueError("preserve the existing worker and private NAS configuration")
    profile = read_json(args.profile)
    if any(profile[k] != manifest[k] for k in ("producer_device_id", "receiver_device_id", "repository_id")):
        raise ValueError("local paired profile differs from the source handoff")
    role = args.role_prefix.absolute()
    code = Path("/opt/lab203-backup/recovery-code-v7")
    config = Path("/etc/lab203-backup/recovery-queue.json")
    environment = config.with_suffix(".env")
    hook = Path("/etc/systemd/system/lab203-backup.service.d/recovery-queue.conf")
    lock = Path("/opt/lab203-backup/receipts/recovery-runtime-lock-v7.json")
    state = Path("/srv/lab203-backup/recovery-state")
    scratch = Path("/var/lib/lab203-recovery-scratch")
    for path in (role, code, config, environment, hook, lock, state, scratch, args.evidence):
        if any(p.is_symlink() for p in (path, *path.parents)):
            raise ValueError("local installation path is redirected")
    if any(path.exists() for path in (code, config, environment, hook, lock, state, scratch)):
        raise ValueError("preserve an existing recovery installation; inspect instead of overwriting")
    private_file(args.password_file)
    private_file(args.nas_configuration)
    NasTarget(args.nas_configuration).check()
    if not Path("/srv/lab203-backup/state/owner.lock").is_file():
        raise ValueError("existing lab203 backup owner is missing")
    before = service_files()
    execute(["systemctl", "is-enabled", "lab203-backup.timer"])
    execute(["systemctl", "is-active", "lab203-backup.timer"])
    args.evidence.mkdir(mode=0o700, parents=True)
    private_json(args.evidence / "service-before.json", before)
    # The published Linux explicit lock owns this NEW PG/Python role only.
    if not role.exists():
        if not args.mamba:
            raise ValueError("provide the existing Miniforge Mamba executable for a fresh recovery prefix")
        execute([args.mamba, "create", "-y", "-p", role, "--file",
                 package / "configs/environments/locks/control-recovery-linux-64-20261003.explicit.txt"],
                timeout=1800, umask=0o022)
    if not (role / "conda-meta").is_dir():
        raise ValueError("use an isolated Mamba recovery environment")
    execute(["runuser", "-u", "nobody", "--", role / "bin/initdb", "--version"])
    shutil.copytree(package, code)
    verify(code, args.envelope_identity)
    body = execute([role / "bin/python", "-c", "import json,psycopg; from stockagent.runtime_identity "
                    "import runtime_identity; print(json.dumps(runtime_identity()))"], cwd=code)
    observed_runtime = json.loads(body)
    private_json(lock, observed_runtime)
    state.mkdir(mode=0o700)
    scratch.mkdir(mode=0o755)
    scratch.chmod(0o755)  # Only this new root, for the private nobody-owned PG cluster.
    c = {"schema_version": 1, **{k: profile[k] for k in ("producer_device_id", "receiver_device_id", "repository_id")},
        "request_root": "/srv/lab203-backup/ingress/tools/recovery-requests",
        "receipt_root": "/srv/lab203-backup/receipts", "state_root": str(state), "scratch_root": str(scratch),
        "owner_lock": "/srv/lab203-backup/state/owner.lock", "nas_configuration": str(args.nas_configuration.absolute()),
        "password_file": str(args.password_file.absolute()), "restic": str(args.restic.absolute()),
        "pg_bin": str(role / "bin"), "runtime_lock": str(lock), "minimum_free_bytes": 64 * 1024**3,
        "maximum_restore_bytes": 4 * 1024**3, "maximum_plans_per_cycle": 1,
        "retry_delays_seconds": [300, 900, 3600], "installed_handoff_identity_sha256": manifest["identity_sha256"],
        "windows_distribution": args.distribution,
        "powershell": "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe"}
    validate_configuration(c)
    if physical_free_bytes(c) < c["minimum_free_bytes"] + c["maximum_restore_bytes"]:
        raise ValueError("Windows VHDX backing drive lacks the required real free space")
    private_json(config, c)
    env = "FINTECH_ENV_PATH=" + shlex.quote(str(role)) + "\nSTOCKAGENT_RECOVERY_QUEUE_CONFIGURATION=" + shlex.quote(str(config)) + "\n"
    atomic_write_bytes(environment, env.encode(), mode=0o600)
    hook.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
    atomic_write_bytes(hook, dropin(code).encode(), mode=0o644)
    execute(["systemd-analyze", "verify", "/etc/systemd/system/lab203-backup.service"])
    execute(["systemctl", "daemon-reload"])
    after = service_files(exclude=hook)
    if before != after:
        raise ValueError("existing backup worker/timer changed; inspect the retained service evidence")
    post = execute(["systemctl", "show", "lab203-backup.service", "--property=ExecStartPost", "--value"])
    if str(code / "scripts/run_lab203_recovery_queue.sh") not in post:
        raise ValueError("fixed recovery hook is absent from the loaded service")
    execute(["systemctl", "is-enabled", "lab203-backup.timer"])
    execute(["systemctl", "is-active", "lab203-backup.timer"])
    if validate_runtime_lock(native_before, runtime_identity()):
        raise ValueError("native backup role changed during isolated recovery installation")
    result = {"state": "fixed_recovery_hook_installed", "existing_worker_preserved": True,
        "existing_timer_preserved": True, "backup_owner_reused": True, "native_runtime_unchanged": True,
        "frozen_package_files_verified": verified["files_verified"],
        "handoff_identity_sha256": manifest["identity_sha256"], "private_credentials_published": False,
        "automatic_pruning": False, "nas_semantic_recovery_completed": False,
        "complete_installation_seconds": time.perf_counter() - started}
    private_json(args.evidence / "installation.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, default=Path("/srv/lab203-backup/ingress/tools/continuous-backup-20261004-v7"))
    parser.add_argument("--bootstrap", type=Path)
    parser.add_argument("--envelope-identity")
    parser.add_argument("--manifest-sha256")
    parser.add_argument("--profile", type=Path, default=ROOT / "configs/data_sync/backup_receipts.json")
    parser.add_argument("--nas-configuration", type=Path, required=True)
    parser.add_argument("--password-file", type=Path, required=True)
    parser.add_argument("--restic", type=Path, required=True)
    parser.add_argument("--role-prefix", type=Path, required=True)
    parser.add_argument("--mamba", type=Path)
    parser.add_argument("--distribution", default="Ubuntu")
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        if args.bootstrap:
            pins = bootstrap_pins(args.bootstrap)
            for option, field in (("envelope_identity", "envelope_identity_sha256"), ("manifest_sha256", "handoff_manifest_sha256")):
                if getattr(args, option) is not None and getattr(args, option) != pins[field]:
                    raise ValueError("explicit pins differ from the paired source bootstrap")
                setattr(args, option, pins[field])
        if not args.envelope_identity or not args.manifest_sha256:
            raise ValueError("use the synchronized paired bootstrap or independent explicit pins")
        print(json.dumps(install(args)))
    except Exception as error:
        print(json.dumps({"state": "installation_failed", "error_type": type(error).__name__}))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
