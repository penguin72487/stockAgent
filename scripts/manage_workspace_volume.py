#!/usr/bin/env python3
"""Create/mount the E data VHD without moving services or touching D cold data."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync.workspace_volume import (
    CONFIG, ENROLLMENT, MARKER, FINLAB_FUTURES_ALIAS, FINLAB_FUTURES_CONTRACT,
    FINLAB_FUTURES_MARKER, FINLAB_FUTURES_ROLE, _mount_info, admit, check,
    check_finlab_futures, configuration, marker, native_identity, wsl_command,
)
from stockagent.data_sync.desync_snapshots import SnapshotError


def initial_resize(c):
    """A running service VM must never be shut down to grow a data disk."""
    wsl_command(c, ["--terminate", c["distribution"]], timeout=60)
    try:
        result = wsl_command(c, ["--manage", c["distribution"], "--resize", "3500GB"], timeout=180)
    except SnapshotError as error:
        if "WSL_E_DISTRO_NOT_STOPPED" not in str(error):
            raise
        result = {"state": "resize_deferred_until_service_vm_quiet", "target": "3500GB",
                  "reason": "WSL_E_DISTRO_NOT_STOPPED", "service_vm_shutdown_performed": False}
    atomic_write_json(ENROLLMENT.parent / "initial-resize.json", result)


def rootfs(c, target):
    """Small local system binaries with their exact ELF dependencies; no keys."""
    with tempfile.TemporaryDirectory(prefix="stockagent-data-rootfs-") as td:
        root = Path(td)
        for d in ("bin", "usr/bin", "etc", "root", "tmp", "proc", "sys", "dev",
                  "mnt/wsl", "workspace", "opt/stockagent"):
            (root / d).mkdir(parents=True, exist_ok=True)
        binaries = ["/bin/bash", "/usr/bin/mount", "/usr/bin/mkdir", "/usr/bin/sleep",
                    "/usr/bin/findmnt", "/usr/bin/stat", "/usr/bin/cat", "/usr/bin/sync"]
        members = set(binaries)
        for binary in binaries:
            output = subprocess.run(["ldd", binary], capture_output=True, text=True, check=True).stdout
            for line in output.splitlines():
                for word in line.split():
                    if word.startswith("/"):
                        members.add(word)
        manifest = {}
        for name in sorted(members):
            source = Path(name)
            dest = root / name.lstrip("/")
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, dest, follow_symlinks=True)
            manifest[name] = hashlib.sha256(dest.read_bytes()).hexdigest()
        (root / "bin/sh").symlink_to("bash")
        (root / "etc/passwd").write_text("root:x:0:0:root:/root:/bin/bash\n")
        (root / "etc/group").write_text("root:x:0:\n")
        (root / "etc/os-release").write_text('NAME="StockAgent data volume"\nID=stockagent-data\n')
        (root / "etc/wsl.conf").write_text(
            "[boot]\nsystemd=false\n[user]\ndefault=root\n[automount]\nenabled=false\n"
            "mountFsTab=false\n[interop]\nenabled=false\nappendWindowsPath=false\n")
        (root / "workspace" / MARKER).write_text(json.dumps(marker(c), sort_keys=True) + "\n")
        (root / "opt/stockagent/share.sh").write_text(
            "#!/bin/bash\nset -euo pipefail\n"
            "export PATH=/usr/bin:/bin\n"
            "mkdir -p /mnt/wsl/stockagent-workspace\n"
            "if ! findmnt -n --mountpoint /mnt/wsl/stockagent-workspace >/dev/null; then\n"
            "  mount --bind /workspace /mnt/wsl/stockagent-workspace\nfi\n"
            "[[ $(stat -c '%d:%i' /workspace) == $(stat -c '%d:%i' /mnt/wsl/stockagent-workspace) ]]\n")
        with tarfile.open(target, "x") as archive:
            for item in sorted(root.iterdir()):
                archive.add(item, arcname=item.name)
        return manifest


def install(c):
    value = native_identity(c)
    state = ENROLLMENT.parent
    destination = Path("/mnt/e/StockAgent/workspace")
    archive = destination.parent / "data-rootfs-v1.tar"
    if value.get("registration"):
        if ENROLLMENT.exists():
            raise ValueError("enrolled data distribution: use mount, never reimport")
        bootstrap = json.loads((state / "bootstrap.json").read_bytes())
        if (bootstrap.get("marker") != marker(c)
                or bootstrap.get("archive_sha256") != hashlib.sha256(archive.read_bytes()).hexdigest()):
            raise ValueError("interrupted import has no exact bootstrap identity")
        # Only this new, unenrolled data instance can be stopped for its initial
        # resize. Never shut down WSL or touch the running C service distro.
        initial_resize(c)
        return mount(c, enrolling=True)
    if value["free_bytes"] < c["minimum_free_bytes"] + 2 * 1024**3:
        raise ValueError("E lacks initial headroom")
    if destination.exists():
        raise ValueError("preserve existing E workspace installation")
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    members = rootfs(c, archive)
    atomic_write_json(state / "bootstrap.json", {"contract": "local_data_rootfs_v1", "members": members,
        "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(), "marker": marker(c)})
    wsl_command(c, ["--import", c["distribution"], c["install_location"],
                   "E:\\StockAgent\\data-rootfs-v1.tar", "--version", "2"], timeout=180)
    # Resize only the newly created, still-empty data VHD, never the C distro.
    initial_resize(c)
    return mount(c, enrolling=True)


def mount(c, *, enrolling=False):
    identity = native_identity(c)
    if not identity.get("registration"):
        raise ValueError("data distribution has not been installed")
    wsl_command(c, ["--distribution", c["distribution"], "--user", "root", "--exec",
                   "/bin/bash", "/opt/stockagent/share.sh"], timeout=90)
    shared, target = Path(c["shared_root"]), Path(c["mount_point"])
    if (shared.is_symlink() or not shared.is_mount() or _mount_info(shared)["fstype"] != "ext4"
            or json.loads((shared / MARKER).read_bytes()) != marker(c)):
        raise ValueError("E data instance failed to expose its exact ext4 workspace")
    info = _mount_info(shared)
    if enrolling:
        if ENROLLMENT.exists() or not info.get("uuid"):
            raise ValueError("preserve enrollment or reject missing filesystem UUID")
        atomic_write_json(ENROLLMENT, {"marker": marker(c), "filesystem_uuid": info["uuid"]})
    elif not ENROLLMENT.exists():
        raise ValueError("E workspace has no completed enrollment")
    if target.is_symlink():
        raise ValueError("workspace target is redirected")
    target.mkdir(mode=0o700, exist_ok=True)
    if not target.is_mount():
        if any(target.iterdir()):
            raise ValueError("refuse to hide C data beneath E workspace")
        subprocess.run(["mount", "--bind", str(shared), str(target)], check=True)
    result = check(physical=True, c=c)
    for role in ("downloads", "increments", "build", "verify", "restore", "transport", "migration"):
        child = target / role
        if child.is_symlink():
            raise ValueError("redirected workspace role")
        child.mkdir(mode=0o700, exist_ok=True)
    return result


def mount_finlab_futures(c):
    """Attach a new empty E namespace; never move or hide old source files."""
    admit(c=c)
    source = Path(c["mount_point"]) / FINLAB_FUTURES_ROLE
    target = FINLAB_FUTURES_ALIAS
    for parent in (source.parent, target.parent):
        if parent.resolve(strict=True) != parent or not parent.is_dir():
            raise ValueError("FinLab futures workspace parent is absent or redirected")
    expected = {"contract": FINLAB_FUTURES_CONTRACT, "volume_id": c["volume_id"],
                "source": str(source), "target": str(target)}
    if not source.exists():
        source.mkdir(mode=0o700)
        atomic_write_json(source / FINLAB_FUTURES_MARKER, expected)
    if source.is_symlink() or not source.is_dir():
        raise ValueError("FinLab futures workspace source is redirected")
    identity = source / FINLAB_FUTURES_MARKER
    if identity.is_symlink() or not identity.is_file() or json.loads(identity.read_bytes()) != expected:
        raise ValueError("preserve unrecognized FinLab futures workspace")
    if target.is_symlink():
        raise ValueError("FinLab futures workspace alias is redirected")
    target.mkdir(mode=0o700, exist_ok=True)
    if not target.is_mount():
        if any(target.iterdir()):
            raise ValueError("refuse to hide existing FinLab futures objects")
        subprocess.run(["mount", "--bind", str(source), str(target)], check=True)
    return check_finlab_futures(physical=True, c=c)


def main():
    if os.readlink("/proc/self/ns/mnt") != os.readlink("/proc/1/ns/mnt"):
        os.execvp("nsenter", ["nsenter", "--mount=/proc/1/ns/mnt", "--", sys.executable,
                              str(Path(__file__).resolve()), *sys.argv[1:]])
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("install", "mount", "check", "install-services",
                                            "mount-finlab-futures", "check-finlab-futures",
                                            "install-finlab-futures-service"))
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("use the original local root storage owner")
    c = configuration()
    ENROLLMENT.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (ENROLLMENT.parent / "owner.lock").open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.command == 'install-finlab-futures-service':
            result = check(physical=True, c=c)
            repo = Path(__file__).resolve().parents[1]
            name = 'stockagent-finlab-futures-workspace.service'
            template = repo / 'deploy/systemd' / (name + '.in')
            target = Path('/etc/systemd/system') / name
            text = template.read_text().replace('__REPO_ROOT__', str(repo))
            if target.exists() and target.read_text() != text:
                raise ValueError('preserve nonmatching FinLab workspace service unit')
            target.write_text(text)
            subprocess.run(['systemctl', 'daemon-reload'], check=True)
            fcntl.flock(owner, fcntl.LOCK_UN)
            subprocess.run(['systemctl', 'enable', '--now', name], check=True)
            result = check_finlab_futures(physical=True, c=c)
        elif args.command == 'install-services':
            result=check(physical=True,c=c)
            repo=Path(__file__).resolve().parents[1]
            for name in ('stockagent-e-workspace-mount.service', 'stockagent-transport-relocation.service',
                         'stockagent-transport-relocation.timer'):
                template=repo/'deploy/systemd'/(name+'.in')
                target=Path('/etc/systemd/system')/name
                text=template.read_text().replace('__REPO_ROOT__',str(repo))
                if target.exists() and target.read_text()!=text:
                    raise ValueError('preserve existing nonmatching workspace service unit')
                target.write_text(text)
            # Install ordering before cutover: a reboot between the E config
            # commit and alias mount must not run the sender before its disk.
            dropin=Path('/etc/systemd/system/stockagent-backup-transport-mount.service.d/e-workspace.conf')
            text=(repo/'deploy/systemd/stockagent-backup-transport-e-workspace.conf.in').read_text()
            if dropin.exists() and dropin.read_text()!=text:
                raise ValueError('preserve nonmatching transport boot ordering')
            dropin.parent.mkdir(parents=True,exist_ok=True)
            dropin.write_text(text)
            subprocess.run(['systemctl','daemon-reload'],check=True)
            fcntl.flock(owner,fcntl.LOCK_UN)
            subprocess.run(['systemctl','enable','--now','stockagent-e-workspace-mount.service'],check=True)
            subprocess.run(['systemctl','enable','--now','stockagent-transport-relocation.timer'],check=True)
        else:
            result = (install(c) if args.command == "install" else mount(c) if args.command == "mount"
                      else mount_finlab_futures(c) if args.command == "mount-finlab-futures"
                      else check_finlab_futures(physical=True, c=c) if args.command == "check-finlab-futures"
                      else check(physical=True, c=c))
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
