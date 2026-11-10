#!/usr/bin/env python3
"""Audit and atomically relocate enrolled mutable service directories.

No payload is copied, hashed, re-encoded or deleted. Existing open descriptors,
locks and legacy paths keep referring to the original inode. Initial control
placement requires idle owners. Reboot replay restores only enrolled unchanged
binds, preserving target readers and refusing referenced or populated placeholders.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import time

from scripts.relocate_root_backup_alias import exchange, exchange_names, _syncdir
from stockagent.data_sync.desync_snapshots import atomic_write_json
from stockagent.service_layout import canonical_service_path, service_moves

CONTRACT = "stockagent.service_storage_placement.v1"


def reject_nested_mounts(source: Path) -> None:
    # Mount topology is the authoritative boundary, including same-filesystem
    # file/directory bind mounts that a per-file st_dev check cannot detect.
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        field = line.split()[4]
        mount = Path(re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), field))
        if mount == source or source in mount.parents:
            raise ValueError("nested filesystem requires separate placement enrollment")


def audit_tree(source: Path, *, deadline_seconds: float = 30) -> dict:
    """Metadata only: refuse relative links whose meaning a move would change."""
    started = time.monotonic()
    root = source.lstat()
    if not stat.S_ISDIR(root.st_mode) or source.resolve() != source:
        raise ValueError("source must be a real unredirected service directory")
    reject_nested_mounts(source)
    entries, links, stack = 0, [], [source]
    while stack:
        if time.monotonic() - started > deadline_seconds:
            raise TimeoutError("service metadata audit deadline; original paths retained")
        with os.scandir(stack.pop()) as children:
            for child in children:
                entries += 1
                if entries % 1024 == 0 and time.monotonic() - started > deadline_seconds:
                    raise TimeoutError("service metadata audit deadline; original paths retained")
                # Native directory entry types avoid one stat syscall for every
                # immutable regular file. No payload is being copied/deleted;
                # the original directory inode itself is atomically renamed.
                if child.is_symlink():
                    target = os.readlink(child.path)
                    if not os.path.isabs(target):
                        lexical = Path(os.path.normpath(os.path.join(os.path.dirname(child.path), target)))
                        if not lexical.is_relative_to(source):
                            raise ValueError("relative link escapes service root; preserve original placement")
                    links.append({"path": str(Path(child.path).relative_to(source)), "target": target})
                elif child.is_dir(follow_symlinks=False):
                    stack.append(Path(child.path))
                elif not child.is_file(follow_symlinks=False):
                    raise ValueError("special service node requires owner-specific migration")
    reject_nested_mounts(source)
    if not os.path.samestat(source.stat(), root):
        raise ValueError("service root identity changed during metadata audit")
    return {"device": root.st_dev, "inode": root.st_ino, "entries": entries,
            "links": sorted(links, key=lambda row: row["path"]),
            "elapsed_seconds": round(time.monotonic() - started, 3)}


def owner_gate(source: Path, root: Path, *, boot_handoff: bool = False) -> str | None:
    if source in {root / "artifacts/operations", Path("/var/lib/stockagent")}:
        if boot_handoff:
            active = subprocess.run(["systemctl", "list-units", "--type=service",
                "--state=active,activating,deactivating", "--plain", "--no-legend", "--no-pager"],
                capture_output=True, text=True, check=True, timeout=10)
            if any(line.split()[0].startswith("stockagent-")
                   and line.split()[0] != "stockagent-service-layout.service"
                   for line in active.stdout.splitlines() if line.split()):
                return "service_owners_running_wait_for_next_boot"
            from stockagent.data_sync.materialized_cache import process_references_many
            target = service_moves(root)[source]
            if process_references_many([source, target], limit=1, include_current_process=True):
                return "control_process_reference"
            return None
        from stockagent.data_sync.storage_monitor import local_census_priority_reason
        reason = local_census_priority_reason({"id": "penguin-wsl"}, root)
        if reason:
            return reason
        # These trees include exact signed restore plans and live controller
        # locks. Switching them is an offline owner handoff, never a timer race.
        return "control_owner_handoff_required"
    return None


def native_control_audit(source: Path) -> dict:
    """Placement proof for an unchanged native subtree, not a copy/retirement.

    The old native bind path remains the controller's read/write interface,
    preserving relative links, sockets and every original file/lock inode.
    Traversing millions of untouched descendants would add no content proof to
    the atomic directory exchange. Payload recovery/retirement stays separate.
    """
    info = source.lstat()
    if not stat.S_ISDIR(info.st_mode) or source.resolve() != source:
        raise ValueError("control source is redirected")
    reject_nested_mounts(source)
    return {"device": info.st_dev, "inode": info.st_ino,
            "entries": None, "descendants_scanned": False,
            "proof": "unchanged_native_subtree_inode_with_original_bind_interface",
            "relative_link_interface": str(source), "payload_recovery_claim": False}


def restore_bind(source: Path, target: Path, row: dict, receipt: Path) -> None:
    """Re-establish a declared native compatibility mount after reboot.

    Only the journal's original target and empty mountpoint inodes are
    accepted. No arbitrary preexisting directory is mounted over or removed.
    """
    binding = row["binding"]
    if (target.is_symlink() or target.resolve() != target
            or target.stat().st_dev != source.parent.stat().st_dev
            or target.stat().st_ino != row["audit"]["inode"]):
        raise ValueError("control bind target identity differs")
    if source.is_dir() and not source.is_symlink() and os.path.samestat(source.stat(), target.stat()):
        row["state"] = "already_placed"
        return
    slot = Path(binding["slot"])
    if source.is_symlink():
        if (os.readlink(source) != str(target) or not slot.is_dir() or slot.is_symlink()
                or slot.stat().st_ino != binding["mountpoint_inode"] or any(slot.iterdir())):
            raise ValueError("interrupted control bind does not match its mountpoint intent")
        exchange_names(source, slot)
        _syncdir(source.parent)
        _syncdir(slot.parent)
    if (source.is_symlink() or not source.is_dir() or any(source.iterdir())
            or source.stat().st_ino != binding["mountpoint_inode"]
            or not slot.is_symlink() or os.readlink(slot) != str(target)):
        raise ValueError("control compatibility mountpoint identity differs")
    try:
        subprocess.run(["mount", "--bind", str(target), str(source)], check=True,
                       capture_output=True, text=True, timeout=10)
        if not os.path.samestat(source.stat(), target.stat()):
            raise RuntimeError("control bind readback differs")
    except Exception:
        # A timeout can follow a completed mount. Never undo a live mount or
        # guess its outcome; successful inode readback is the narrow criterion.
        if os.path.samestat(source.stat(), target.stat()):
            row["state"] = "placed"
            atomic_write_json(receipt, row)
            return
        # Restore both original directory and legacy name without a gap. Only
        # our exact private self-link is removed; no payload name is unlinked.
        exchange_names(source, slot)
        exchange_names(source, target)
        if (source.stat().st_ino != row["audit"]["inode"] or not target.is_symlink()
                or os.readlink(target) != str(target)):
            raise RuntimeError("control rollback identity differs; preserve all names")
        target.unlink()
        _syncdir(source.parent)
        _syncdir(target.parent)
        row["state"] = "rolled_back_bind_unavailable"
        atomic_write_json(receipt, row)
        return
    row["state"] = "placed"
    atomic_write_json(receipt, row)


def enrolled_bind_gate(source: Path, target: Path, row: dict, root: Path, *,
                       boot_handoff: bool) -> str | None:
    """Replay an unchanged enrolled bind without waiting for unrelated owners.

    Readers of the original target keep their exact inodes. Only references to
    an unmounted compatibility name prevent replay; initial placement retains
    the stricter, offline owner gate.
    """
    if (target.is_symlink() or target.resolve() != target
            or target.stat().st_dev != source.parent.stat().st_dev
            or target.stat().st_ino != row["audit"]["inode"]):
        raise ValueError("control bind target identity differs")
    if source.is_dir() and not source.is_symlink() and os.path.samestat(source.stat(), target.stat()):
        return None
    if not boot_handoff:
        return owner_gate(source, root, boot_handoff=False)
    # Never conceal new state created in an unmounted placeholder. An explicit
    # recovery can preserve those names separately, as in the 2026-11-04 audit.
    if source.is_dir() and not source.is_symlink():
        if source.stat().st_ino != row["binding"]["mountpoint_inode"] or any(source.iterdir()):
            return "control_mountpoint_changed_requires_recovery"
    from stockagent.data_sync.materialized_cache import process_references_many
    if process_references_many([source], limit=1, include_current_process=True):
        return "control_mountpoint_process_reference"
    return None


def required_bind_failures(root: Path, areas: list[str]) -> list[str]:
    """Bounded startup readback, including when retirement holds its lock.

    Optional first-time organization may defer. A previously enrolled bind
    must actually expose its original target before dependent services start.
    """
    mappings = {new.name: (old, new) for old, new in service_moves(root).items()}
    failures = []
    for area in areas:
        source, target = mappings[area]
        receipt = root / "service_data" / (".placement-" + area + ".json")
        try:
            if not receipt.exists() and not receipt.is_symlink():
                continue
            if receipt.is_symlink() or receipt.stat().st_size > 1024**2:
                raise ValueError("invalid placement receipt")
            row = json.loads(receipt.read_bytes())
            if not row.get("binding"):
                continue
            if (row.get("source") != str(source) or row.get("target") != str(target)
                    or target.is_symlink() or target.resolve() != target
                    or target.stat().st_ino != row["audit"]["inode"]
                    or target.stat().st_dev != source.parent.stat().st_dev
                    or source.is_symlink() or not os.path.samestat(source.stat(), target.stat())):
                raise ValueError("enrolled compatibility mount is not restored")
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            failures.append(area)
    return failures


@contextmanager
def placement_lease(root: Path):
    directory = root / "service_data"
    directory.mkdir(mode=0o700, exist_ok=True)
    if directory.resolve() != directory:
        raise ValueError("service_data must be a real local directory")
    path = directory / ".placement.lock"
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield directory
    finally:
        os.close(fd)


def organize(root: Path, areas: list[str], *, apply: bool = False, boot_handoff: bool = False) -> dict:
    root = root.resolve(strict=True)
    mappings = {new.name: (old, new) for old, new in service_moves(root).items()}
    if not areas or len(set(areas)) != len(areas) or not set(areas) <= mappings.keys():
        raise ValueError("select distinct enrolled service areas")
    report = {"contract": CONTRACT, "apply": apply, "rows": [],
              "observed_at_utc": datetime.now(timezone.utc).isoformat(),
              "payload_bytes_copied": 0, "source_files_deleted": 0,
              "reclaimed_bytes": 0, "legacy_paths_preserved": True}
    with placement_lease(root) as directory:
        journal = directory / "placement.json"
        if journal.is_symlink():
            raise ValueError("placement journal must not be redirected")
        for area in areas:
            source, target = mappings[area]
            row = {"area": area, "source": str(source), "target": str(target)}
            report["rows"].append(row)
            area_receipt = directory / (".placement-" + area + ".json")
            if area_receipt.exists():
                if area_receipt.is_symlink() or area_receipt.stat().st_size > 1024**2:
                    raise ValueError("control placement receipt is redirected or oversized")
                previous = json.loads(area_receipt.read_bytes())
                if previous.get("binding") and target.exists():
                    if (previous.get("source") != str(source) or previous.get("target") != str(target)
                            or previous["binding"].get("slot") != str(directory / ".compatibility" / area)):
                        raise ValueError("control placement receipt scope differs")
                    reason = enrolled_bind_gate(source, target, previous, root,
                                                boot_handoff=boot_handoff)
                    if reason:
                        row.update(state="deferred", reason=reason)
                        continue
                    row.update(previous)
                    if apply:
                        restore_bind(source, target, row, area_receipt)
                    else:
                        row["state"] = "planned_bind_restore"
                    continue
            if source.is_symlink():
                if canonical_service_path(source, root) != target:
                    raise ValueError("compatibility alias differs")
                info = target.stat()
                row.update(state="already_placed", device=info.st_dev, inode=info.st_ino)
                continue
            if not source.exists():
                row["state"] = "absent_no_payload_created"
                continue
            if target.exists() or target.is_symlink():
                raise FileExistsError("independent destination exists; do not merge service state")
            reason = owner_gate(source, root, boot_handoff=boot_handoff)
            if reason:
                row.update(state="deferred", reason=reason)
                continue
            try:
                row["audit"] = (native_control_audit(source) if area in {"operations", "stockagent"}
                                else audit_tree(source))
            except (TimeoutError, ValueError) as exc:
                if not boot_handoff:
                    raise
                row.update(state="deferred", reason="metadata_audit_requires_owner_review",
                           detail=str(exc))
                continue
            row["state"] = "planned"
            if apply:
                control = area in {"operations", "stockagent"}
                if control:
                    slots = directory / ".compatibility"
                    slots.mkdir(mode=0o700, exist_ok=True)
                    if slots.resolve() != slots:
                        raise ValueError("control mountpoint staging is redirected")
                    slot = slots / area
                    if slot.exists() or slot.is_symlink():
                        if (not area_receipt.exists() or slot.is_symlink() or not slot.is_dir()
                                or any(slot.iterdir()) or previous.get("state") not in
                                {"planned", "rolled_back_bind_unavailable"}
                                or slot.stat().st_ino != previous["binding"]["mountpoint_inode"]):
                            raise FileExistsError("undeclared control mountpoint staging")
                    else:
                        slot.mkdir(mode=stat.S_IMODE(source.stat().st_mode))
                        _syncdir(slots)
                    row["binding"] = {"kind": "native_bind_compatibility", "slot": str(slot),
                                      "mountpoint_inode": slot.stat().st_ino}
                    atomic_write_json(area_receipt, row)
                # Persist intent before the atomic exchange. Recovery recognizes
                # an already completed exchange by exact alias and directory ID.
                atomic_write_json(journal, report)
                identity = row["audit"]
                row["exchange"] = exchange(source, target, source_parent=source.parent,
                    target_parent=directory, expected_directory_id=(identity["device"], identity["inode"]))
                if canonical_service_path(source, root) != target:
                    raise RuntimeError("placement readback failed; keep both names for inspection")
                row["state"] = "placed"
                if control:
                    atomic_write_json(area_receipt, row)
                    restore_bind(source, target, row, area_receipt)
                if row["state"] == "placed":
                    atomic_write_json(directory / "layout_generation.json", {
                        "contract": "stockagent.service_layout_generation.v1",
                        "changed_at_utc": datetime.now(timezone.utc).isoformat()})
                atomic_write_json(journal, report)
        if apply:
            atomic_write_json(journal, report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--areas", nargs="+", default=["live", "cache", "discord_bot", "stockagent-live", "operations", "stockagent"])
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--boot-handoff", action="store_true",
                        help="hand off control roots only before all service owners start")
    parser.add_argument("--report", type=Path, help="write the metadata-only operation receipt")
    args = parser.parse_args()
    # The original scheduler inode serializes placement with retirement. It is
    # never replaced or unlocked on behalf of a different process.
    lock = Path("/run/lock/stockagent-wsl-retirement-wave-scheduler.lock")
    fd = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            # Placement is optional while the original owner holds this lock.
            # Preserve its paths and let services keep using their current
            # owner; a namespace tidy-up must not block the trading service.
            value = {"contract": CONTRACT, "state": "deferred",
                     "reason": "original_retirement_owner_active", "rows": [],
                     "payload_bytes_copied": 0, "source_files_deleted": 0,
                     "reclaimed_bytes": 0}
        else:
            value = organize(args.root, args.areas, apply=args.apply, boot_handoff=args.boot_handoff)
        failures = (required_bind_failures(args.root.resolve(strict=True), args.areas)
                    if args.apply and args.boot_handoff else [])
        if args.apply and args.boot_handoff:
            value["required_bind_failures"] = failures
            value["startup_control_paths_ready"] = not failures
        if args.report:
            if args.report.is_symlink() or args.report.parent.resolve() != args.report.parent.absolute():
                raise ValueError("placement receipt path is redirected")
            args.report.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_json(args.report, value)
        print(json.dumps(value, indent=2))
        return 75 if failures else 0
    finally:
        os.close(fd)


if __name__ == "__main__":
    raise SystemExit(main())
